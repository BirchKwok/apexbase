# Scan And Physical Execution Architecture

This page describes the storage/query boundary introduced in ApexBase 1.33 and
extended in 1.34 with batched overlay streams and bounded parallel scan/fold.
It is an internal contract for contributors; SQL users keep using the same
`ApexClient.execute(...)` API.

## Why The Boundary Exists

Fast paths are useful when they optimize a reusable physical operation. They
become difficult to extend when storage methods encode a complete benchmark
query. ApexBase therefore separates four decisions:

1. SQL parsing and semantic validation stay in `ApexExecutor`.
2. The executor translates supported predicates into a storage-level request.
3. `TableStorageBackend` chooses the base or overlay-aware scan lane.
4. Physical operators consume a `Morsel` and its selection vector.

Unsupported expressions return to the general SQL evaluator. The scan layer
does not weaken SQL semantics to force a query through the optimized path.

## Core Protocol

The protocol is defined in `apexbase/src/storage/scan.rs`:

| Type | Responsibility |
| --- | --- |
| `ScanRequest` | Borrowed projection and an optional typed predicate tree |
| `ScanPredicate` | One physical leaf: comparison, range, `IN` list, or null check |
| `ScanBound` | Inclusive or exclusive numeric boundary |
| `ColumnView` | Immutable Arrow array plus field metadata |
| `SelectionVector` | Either all rows or explicit `u32` row positions |
| `Morsel` | Column views, physical row offset/count, and current selection |
| `BatchMorselStream` | Row-group-sized morsels over a stable persisted read view |
| `BatchMorselOutcome` | `Morsel` or `Unsupported` (caller must use the single-shot path) |

The single-shot request emits one morsel. The batch stream emits one
row-group-sized morsel per call, each with the complete typed predicate
already applied, so operators can consume batches incrementally with bounded
memory. The row-offset and selection contract intentionally allows a later
scheduler to split scans into parallel morsels without changing downstream
operator inputs.

## Lane Selection

`TableStorageBackend::scan()` first validates projection and predicate columns,
then chooses one of two correctness-equivalent lanes.

### Persisted base lane

When no delta, pending delta, pending V4 rows, or in-memory overlay exists:

- string equality is preferred as the candidate mmap predicate;
- otherwise zone-map estimates choose the narrowest numeric range;
- candidate row IDs are materialized selectively when they cover at most 75%
  of the table;
- dense candidates use a full projected read to avoid expensive random gather;
- every predicate is still reapplied to the resulting Arrow views, preserving
  strict bounds and conjunction semantics.

The candidate is a physical access decision, not a semantic shortcut.

### Overlay-aware lane

If any delta or in-memory overlay is visible, the backend uses the
authoritative merged `read_columns_to_arrow()` path. The same `Morsel` and
selection evaluator then feed downstream operators, so appends, updates, and
deletes cannot disappear behind a base-file-only optimization.

## First Vertical Pipeline

The first executor slice covers:

```text
conjunctive WHERE
    -> projected storage scan
    -> selection materialization
    -> GROUP BY / aggregates
    -> HAVING
    -> ordered TopK
    -> LIMIT / OFFSET
```

The executor removes `WHERE` only after the scan protocol has consumed the
complete conjunction. `HAVING` stays attached to the grouped statement and is
therefore evaluated before TopK and limit processing.

Supported predicate forms currently include parentheses, `AND`, `OR`,
`BETWEEN`, `IN`, `IS [NOT] NULL`, string equality, numeric equality, and
numeric `<`, `<=`, `>`, `>=` in either literal/column order, including
negative literals (parsed as a unary minus over a literal). Arbitrary
expressions, unsupported Arrow types, and numeric bounds that cannot be
represented exactly fall back to the general evaluator.

## Batched Physical Pipeline (R3)

`TableStorageBackend::scan_batches()` exposes the stable row-group stream in
two correctness-equivalent lanes:

- **clean** — the read view is the persisted V4 file alone (no delta file, no
  pending DeltaStore cells, no pending V4 rows, and no in-memory table); rows
  stream directly from the mmap row groups;
- **overlay** (v1.34.0) — `AppendedRows` and/or `PendingCells` are visible;
  base row groups still pass through zero-copy, each batch is patched by `_id`
  from one DeltaStore snapshot taken when the stream is created, and appended
  rows arrive as a tail batch.

The batch lane returns `None` — and the caller falls back to the single-shot
`scan()` for the whole request — for an in-memory table, unflushed V4 rows, a
legacy non-V4 file, or a projection/predicate type the typed protocol cannot
evaluate.

Each batch is one row group of active rows (deletion vectors applied) with the
projected columns, and the complete typed predicate is re-evaluated on every
batch, so per-batch selections keep the exact `Morsel::select` semantics and
concatenated batches reproduce the single-shot row order. A batch whose column
types the typed protocol cannot evaluate reports `Unsupported`, which also
falls the whole request back to the single-shot path.

The stream snapshots the footer and the mmap `Arc` at creation, so the file
view is stable for the whole stream even if a later write replaces the file.
Before reading a row group, the stream may skip it when the conservative
zone-map proof shows every row of the group outside the predicate: missing
zone maps, non-numeric columns, lossy int-to-float coercions, `NotEq`, and
`IsNull` never skip; `AND` skips when either side is provably disjoint and
`OR` only when both sides are. Zone maps cover pre-deletion data, which can
only enlarge the true range, so a proven-empty zone stays empty after deletes.

The first consumer is the serial batched
Filter -> GROUP BY -> HAVING -> TopK executor slice
(`query/executor/batch_group.rs`), which consumes row-group-sized batches into
an incremental group state, keeping scan memory bounded by one row group plus
the group map regardless of table size. Aggregate semantics mirror the
single-batch kernel (COUNT is the group row count, SUM/MIN/MAX skip NULLs,
AVG divides the sum by the group row count, NULL group keys form one group).
`APEX_BATCH_SCAN=0` disables the batched slice for A/B diagnostics.

Routing note: the batched slice is only reached through
`try_scan_group_pipeline`. The legacy fused fast kernels dispatch earlier and
keep the shapes they own; most importantly the fused single-key kernel takes
any single dictionary-key `GROUP BY` with at most one value aggregate,
independent of `APEX_BATCH_SCAN`. Multi-key groups, extra value aggregates,
and predicates outside the fused lane budget are what reach the batched
slice.

## Cache And Summary Rules

Two related summaries reduce fixed overhead without changing visibility:

- validated string cardinality is cached per epoch-checked backend and cleared
  by normal read-cache invalidation after local mutations;
- Python analytical-result cache tokens skip overlay inspection for external
  file-only SQL, while current-table SQL receives a clean-overlay check and
  table epoch in one Rust call.

External files remain guarded by resolved path, size, and nanosecond mtime.
Cross-client table changes remain guarded by the shared table epoch.

## Extension Rules

When adding another operator or predicate:

1. extend the physical protocol only for semantics the storage layer can
   represent exactly;
2. keep SQL AST types out of `storage/scan.rs`;
3. preserve a general-evaluator fallback;
4. test base and delta/overlay visibility;
5. test NULLs, strict/inclusive bounds, unsupported types, and invalid input;
6. add a same-machine performance metric for the new shared path;
7. do not add a query-specific storage API when the operation can compose from
   scan, selection, aggregation, ordering, and materialization.

## Current Limits

- Zone-map pruning over `OR` is conservative: a row group is skipped only
  when both sides are provably disjoint.
- Numeric scan bounds use `f64`, so wide integer literals deliberately fall
  back when exact round-tripping is impossible.
- Selection materialization currently uses Arrow `take`; late materialization
  can move further downstream in a later phase.
- The batch stream is serial by default. v1.34.0 adds a bounded parallel
  row-group scan/fold over disjoint row-group ranges (`scan_batches_ranges`),
  gated by a process-wide worker-token budget of
  `min(available_parallelism - 1, 8)`: fewer than two free tokens falls back to
  the serial stream, and readers auto-enable parallel workers when the
  calibrated serial prediction exceeds the cost threshold and parallel history
  is still faster. `APEX_PARALLEL_SCAN=0`/`1` forces serial and `N >= 2` forces
  `N` workers. See [Resource Ownership](RESOURCE_OWNERSHIP.md) for the token
  pool and the visibility states that keep a request on the serial path.

These are explicit fallback boundaries, not silent semantic differences.
