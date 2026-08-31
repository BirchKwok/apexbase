# Scan And Physical Execution Architecture

This page describes the storage/query boundary introduced in ApexBase 1.33.
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
| `ScanRequest` | Borrowed projection and a conjunction of physical predicates |
| `ScanPredicate` | Numeric range or string equality; multiple entries mean `AND` |
| `ScanBound` | Inclusive or exclusive numeric boundary |
| `ColumnView` | Immutable Arrow array plus field metadata |
| `SelectionVector` | Either all rows or explicit `u32` row positions |
| `Morsel` | Column views, physical row offset/count, and current selection |

The current implementation emits one morsel per request. The row-offset and
selection contract intentionally allows a later scheduler to split scans into
parallel morsels without changing downstream operator inputs.

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

Supported predicate forms currently include parentheses, `AND`, `BETWEEN`,
string equality, numeric equality, and numeric `<`, `<=`, `>`, `>=` in either
literal/column order. `OR`, arbitrary expressions, unsupported Arrow types,
and numeric bounds that cannot be represented exactly fall back to the general
evaluator.

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

- Predicate lists are conjunctive; there is no boolean expression tree yet.
- Numeric scan bounds use `f64`, so wide integer literals deliberately fall
  back when exact round-tripping is impossible.
- Selection materialization currently uses Arrow `take`; late materialization
  can move further downstream in a later phase.
- One request currently yields one morsel; parallel scheduling is future work.

These are explicit fallback boundaries, not silent semantic differences.
