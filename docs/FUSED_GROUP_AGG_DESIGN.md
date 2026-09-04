# Fused Filter + GROUP BY Design (Boolean 6.4x 专项)

Status: complete (2026-09, Phases 1-3 merged into the v1.33.x working tree)

## Problem

No-cache benchmark metric `Boolean Filter+GROUP+HAVING+TopK`:

```sql
SELECT city, COUNT(*) AS n, AVG(score) AS av
FROM default
WHERE (city IN ('Beijing','Shanghai','Guangzhou') OR age IN (33,44,55))
  AND score >= 40
GROUP BY city
HAVING COUNT(*) > 100
ORDER BY n DESC, city
LIMIT 5
```

ApexBase 37.2 ms vs DuckDB 5.83 ms (6.4x slower) — the only structural
weakness on the 103-metric fair scoreboard.

## Verified cost breakdown (1M rows, no cache)

Stage costs measured on the generic pipeline for this query:

| Stage | Cost | Notes |
| --- | --- | --- |
| Candidate lane (`city IN` scan) | ~7 ms | already dict-key level: per-row u16 index + flag test |
| OR/AND index merge | ~3 ms | O(n) two-pointer merge (fixed in v1.33.0 dev) |
| Gather (300K rows × 3 cols) | ~14 ms | `extract_rows_by_indices_to_arrow`; city decoded per row into a new StringArray; primitives use `Vec<Option<T>>` (16 B/row) |
| Morsel predicate re-eval | ~4 ms | generic `In` leaf does per-row string compare per value on the `StringDictionary` view |
| GROUP BY city (192K rows) | ~1 ms | `execute_group_by` dispatch, not a bottleneck |

The pipeline reads the same data 2–3 times (candidate lane, gather, re-eval)
and re-evaluates the same predicate twice. That redundancy is the structural
gap vs DuckDB's single fused vectorized scan.

## Design principles

1. **One physical protocol.** Dictionary keys (u16 row ids) + primitive
   column lanes are the shared representation for candidate lanes, morsel
   re-evaluation, and the fused aggregation kernel. New filter shapes are
   expressed as small predicate trees over these lanes and route to the
   fused path automatically when capability-gated, otherwise to the improved
   generic pipeline. No per-metric `try_*` branching on SQL text.
2. **Capability-gated fusion.** The fused kernel accepts a bounded predicate
   grammar (leaves: numeric range, numeric IN, string IN/Eq on a
   dictionary-encoded column; structure: AND/OR/NOT). Anything outside the
   grammar falls back to the generic pipeline — correctness first.
3. **Post-aggregate tail processing.** HAVING / ORDER BY / LIMIT / OFFSET run
   on the aggregated result (≤ dictionary size rows), reusing existing
   `apply_order_by_topk` / `apply_limit_offset` / HAVING evaluation.

## Phase 1 — generic pipeline quick wins

Benefits every filtered query, not just the benchmark shape.

### P1.1 Mixed-projection dictionary gather

`read_columns_by_indices_to_arrow` today: `try_dict_indexed_read` requires
*all* projected columns to be dictionary-covered, so a mixed projection
(`city, age, score`) falls back to full indexed extraction, which decodes
every selected city string and copies it into a fresh `StringArray`.

Change: per-column dispatch in the V4 indexed path:

- string column with a usable global dict cache (no nulls, ≤4096 distinct,
  group_ids length matches active rows) → `DictionaryArray` with keys
  gathered from the cached u16 ids (2 B/row);
- every other column → existing parallel indexed extraction for that column.

Result batch may mix `DictionaryArray` and primitive columns. Downstream
operators already accept both (`ColumnView::StringDictionary`,
`execute_group_by` dict dispatches).

### P1.2 Dictionary-level IN/Eq in morsel re-evaluation

`ResolvedPredicate::In` on a `StringDictionary` view currently resolves each
row's string and compares against every IN value. Add a `DictIn` leaf:
resolve matching dictionary keys once (O(distinct)), then per row do a u32
key load + bitset test. Same for `StringEq` on a dictionary view
(`DictEq`, single key compare). Falls back to the generic leaf when the view
is not a dictionary or the value set is empty.

### P1.3 Primitive gather layout

In `extract_rows_by_indices_to_arrow`, replace `Vec<Option<i64>>` /
`Vec<Option<f64>>` accumulators (16 B/row incl. tag + zero fill) with
primitive buffers + validity buffers (8 B/row), built directly into Arrow
`Int64Array` / `Float64Array` with `NullBuffer`.

Expected Phase-1 total: 37.2 ms → ~30 ms. The fused path (Phase 2) is the
real win; Phase 1 raises the floor for all fallback shapes.

## Phase 2 — fused predicate-lane aggregation kernel

Generalize the existing fused path
(`try_fast_numeric_filter_group_by` +
`OnDemandStorage::execute_between_group_agg_cached(_mmap)`) instead of adding
a parallel mechanism:

### Kernel

Single streaming pass over row groups (existing RCIX plumbing):

- group column: dictionary-encoded (global dict cache provides per-row u16
  ids + distinct strings; ≤4096 distinct);
- up to 2 additional numeric lanes (Int64/UInt*/Float64, plain or bitpack /
  float-dict encodings as already decoded by the kernel);
- predicate tree over lanes:
  - leaf `num_range(col, lo, hi)` with the existing epsilon/ceil/floor
    strict-bound semantics;
  - leaf `num_in(col, sorted_values)` (binary search, ≤16 values);
  - leaf `dict_in(group_col, keys)` / `dict_eq(group_col, key)` (bitset over
    u16 ids, computed once from distinct strings);
  - structure: AND / OR (NOT only around a leaf).
- per row: `pass = eval(tree)` → `counts[key] += pass`, `sums[key] += pass*agg[row]`.

Aggregates: COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col) over one
numeric column (same set the kernel already supports; MIN/MAX added via
per-group running min/max when enabled by gate).

### Gate (in `try_fast_numeric_filter_group_by`, generalized)

Accept when:

- single dictionary-encoded group column, ≤4096 distinct, no nulls in
  group/filter/aggregate columns;
- WHERE is a tree of the supported leaves over ≤2 non-group numeric columns
  plus the group column itself (string IN/Eq);
- SELECT = group column + COUNT(*) / SUM / AVG / MIN / MAX over ≤1 numeric
  column (existing column-shape check);
- no joins, no DISTINCT, no windows, no delta/pending state;
- HAVING: optional, references only the aggregates in SELECT (post-filter);
- ORDER BY / LIMIT / OFFSET: applied post-aggregation (existing helpers).

Anything unparseable returns `None` → existing generic pipeline.

### Expected result

1M-row single pass: ~18 MB sequential column reads (u16 ids + 2×8 B lanes) +
predicate eval + L1-resident accumulation into ≤10 group slots.
Target 8–12 ms (≤2x DuckDB), i.e. a 3–4x improvement over the 37.2 ms
generic pipeline.

## Phase 3 — LUT truth table + fast lane decode

Phase 2's per-row predicate-tree evaluation is still the dominant cost once
the data path is lean. Phase 3 removes both remaining costs: per-row tree
evaluation (truth table) and per-row-group lane materialization (in-place
decode).

### P3.1 LUT truth-table fast path

When the predicate has **≤3 lane leaves** (dictionary leaves are folded to
group-id masks at compile time), the whole AND/OR/NOT tree is precomputed
into a truth table over `(comparison bits, group id)`:

- `lut_prog = try_build_lut_program(predicate, num_groups)`: for each of the
  ≤3 lane leaves a comparison bit is defined (range or IN membership), and
  the compiled LUT maps `(bits << stride_shift) | group_id → 0/1`, where
  `stride = next_power_of_two(num_groups)`.
- Size bound: `2^k × stride ≤ 8 × 4096 = 32 KB` (k ≤ 3, groups ≤ 4096) —
  L1/L2 resident, one lookup per row.
- Per row: k straight-line comparisons (no branches over the tree), one LUT
  byte load, one scatter into the group accumulator.
- The row loop is monomorphized per per-leaf mode tuple via const-generic
  `lut_loop_{0,1,2,3}` (84 instantiations); IN comparisons with N ≤ 8 are
  unrolled (first four direct, remainder a tiny loop) because a
  runtime-length vectorized loop emits heavy reduction code.
- `all_none` (no row can match) returns without scanning.
- Predicates with >3 lane leaves fall back to the Phase 2 generic per-slot
  plan — same kernel, same buffers, unchanged behavior.

### P3.2 In-place lane views (no per-RG materialization)

`decode_fused_lane` returns a view instead of a copied buffer:

- **PLAIN, 8-byte aligned** → zero-copy `&[i64]` / `&[f64]` over the mmap
  body (existing behavior).
- **PLAIN, misaligned** → raw in-place byte view (`FusedLaneView::Raw*`);
  the LUT loop reads 8-byte slots with `ptr::read_unaligned`, which is a
  native unaligned load on ARM64. No per-RG memcpy, no allocation. The
  generic (fallback) path materializes such lanes into scratch buffers.
- **BITPACK int** → two-word shift-merge decode (`bitpack_fill` in
  `mod.rs`; each value spans at most two u64 words, short per-bit tail for
  the final partial word) written **directly into the caller buffer** —
  the generic decode path no longer allocates an intermediate per-RG Vec.
  The shared `bitpack_decode_i64` uses the same core, so all bitpack
  consumers benefit.

### Measured (1M rows, no cache, probe query)

| Stage | Before P3 | After P3 |
| --- | --- | --- |
| Row loop (k=2, count+avg) | 6.1 ms | 4.2 ms |
| Lane decode (age BITPACK + score PLAIN) | 4.5 ms | ~1.3 ms |
| Total `Boolean Filter+GROUP+HAVING+TopK` (probe, no cache) | 10.8 ms | 5.5 ms |

Public benchmark (no cache, 1M rows): the metric moved from 37.2 ms
(6.4x slower than DuckDB) to 5.55 ms vs DuckDB 5.76 ms — now the winner.
Tabular fair scoreboard: 103/103 (was 101/103).

## Testing and acceptance (per phase, per AGENTS.md)

- Rust unit tests: kernel vs generic pipeline on random predicate trees
  (fuzzed leaves/structure), null-free gates, boundary epsilon cases,
  HAVING/ORDER/LIMIT tails; P1.1/P1.2 mixed-batch round trips.
- Python differential tests in `test/test_query_architecture_contracts.py`:
  random WHERE/GROUP/HAVING/ORDER/LIMIT shapes, fused result must match the
  generic pipeline row-for-row.
- Benchmark: the existing `Boolean Filter+GROUP+HAVING+TopK` metric covers
  the fused gate; P1.1/P1.2 are covered by the existing filtering metrics.
- Acceptance per phase: `maturin develop --release`, full pytest, full
  cargo test, public benchmark (default + `--no-result-cache`) vs
  `benchmarks/latest_public_baseline.json`, canary gate, and `--mode full`
  local perf guard before release.

## Risk notes

- Epsilon/strict-bound semantics of the numeric range leaf must be reused
  verbatim from the existing kernel (no re-derivation).
- `group_ids` (u16) validity: the gate must re-validate dictionary cache
  length against active rows (deletes) exactly as `try_dict_indexed_read`
  does.
- Mixed DictionaryArray batches change batch schema for some queries; the
  Python layer and any code that assumes `Utf8` for string columns must be
  audited (Arrow consumers in `into_record_batch` and result materialization).
