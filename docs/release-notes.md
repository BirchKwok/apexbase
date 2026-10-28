# Release Notes

This page summarizes the changes introduced in each ApexBase release, grouped by functional area.


## [v1.38.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.38.0)
*2026-10-28*

[Compare with v1.37.0](https://github.com/BirchKwok/ApexBase/compare/v1.37.0...v1.38.0)

> **V5 architecture preview (second release).** v1.38.0 continues the V5 storage
> and execution preview started in v1.37.0. The on-disk format, V4 read
> compatibility and write-exclusion rules are unchanged; this release fixes
> three defects reported against v1.37.0 and closes the read-path gaps they
> exposed. Treat the format as a preview: read
> [V5 Preview Status](#v5-preview-status) before pointing older builds at new
> files, exactly as for v1.37.0.

### Highlights

This release fixes the three defects in the v1.37.0 issue report — the
`SELECT t.* … JOIN` projection failure, the SQL point-lookup latency gap and the
read degradation a predicate `UPDATE` used to leave behind — and keeps the
DeltaStore overlay from pushing point/index reads onto the merged-scan lanes.
The data format, Python/Rust APIs and upgrade path are unchanged.

### Correctness

- Fix `SELECT t.* … JOIN` losing every non-key column. `required_columns()`
  returns `None` for a star projection ("read all columns"), but the general
  hash-join route collapsed that `None` into an empty column list and only
  appended the JOIN keys, so both sides were read with just the key columns.
  `SELECT t.*, p.name … LEFT JOIN` therefore failed with
  `Projected column 'p.name' does not exist`, and the forms that did not error
  (`SELECT t.* FROM t JOIN p`, `t.*, p.*`, `t.*, 'x'`) silently returned only
  the key columns. The star flag is now preserved, only explicit column lists
  are extended with the ON keys, and `COUNT(*)`-style joins keep their old
  minimal-column behaviour. `test/test_join_star_projection.py` pins the
  reported shape plus qualified, unqualified, literal, expression, INNER/RIGHT
  and comma-join variants and the unaffected controls. The parser expands
  `alias.*` as a plain star, so a star result may still carry the other side's
  columns; that pre-existing projection-scoping behaviour is unchanged.

### Read Path Performance

- Parameterized point lookups no longer pay for text-keyed caches. The Python
  client computed result-cache eligibility even when the cache was disabled,
  bound `?` parameters with a per-character state machine, and then handed the
  Rust classifier a fresh SQL text on every call, so each `WHERE _id = ?`
  lookup paid three separate caches that could not hit. The disabled-cache case
  short-circuits, a literal-free binding fast path replaces the state machine,
  and a stable per-statement shape cache feeds `SELECT <projection> FROM <table>
  WHERE _id = ?` straight to the native point-read bindings. On the issue
  report's own harness (50k rows, `params=`, projected columns) the lookup went
  from **0.0124 ms (5.7× native, 4.9× SQLite) to 0.0025 ms (1.15× native,
  0.82× SQLite)**.
- Point reads and secondary-index lookups stay on their fast lanes while a
  DeltaStore cell overlay is visible. A predicate `UPDATE` records cell
  overrides in `<table>.apex.deltastore`; every read lane used to stand down and
  hand the query to the merged executor until `REINDEX` merged the sidecar.
  `retrieve()` and the SQL `_id` fast path now read the base row through the
  same RCIX lanes and apply the pending overrides (deleted rows return empty);
  appended rows and unflushed buffers still take the merged path.
  `index_accelerated_read` now stays eligible when the overlay is cells only:
  the postings' candidates are unioned with the rows whose pending update
  touches a predicate column, the residual `WHERE` re-evaluates the merged
  values, covering (index-only) scans are disabled under an overlay, and
  DeltaStore deletions are filtered from the candidates. On the report's 17-21×
  point-read and 20-190× index-lookup degradations, the same measurements now
  show **~1.6× (native retrieve), ~1.0× (SQL `_id`) and ~1.1-3.4× (index
  lookups)**, with results byte-identical to the post-`REINDEX` answers.
- The overlay merge and the index LIMIT path stop materializing work they do
  not need. `DeltaMerger::merge` used to rebuild every column of every batch
  whenever a DeltaStore was visible, even if no row of the batch was touched; it
  now probes the overlay against the batch ids (small batches), returns the
  base batch zero-copy when untouched, and reuses unchanged columns when no row
  is deleted. Index reads under an overlay also read candidates in id order in
  bounded chunks and stop once `LIMIT`/`OFFSET` are satisfied instead of
  materializing every candidate. On a 200k-row table the projected indexed
  `WHERE age = 42 LIMIT 100` read after a one-row `UPDATE` fell from
  **~11-15× to ~2-3×** the clean read, and the equal-shape point/aggregate
  metrics in the canary are 91-99% lower than on `origin/main`.

### Benchmarks And Tests

- New regression tests: `test/test_join_star_projection.py` (17 cases) for the
  JOIN star projection, and `test/test_overlay_read_after_update.py` for
  correctness plus `perf`-marked guards of the post-`UPDATE` point, index and
  indexed-LIMIT reads (the pre-fix shapes fail the guards by 11-28×).
- New canary metrics so the overlay lanes are measured on every commit:
  `Point lookup (native, cell overlay)`, `Point lookup (SQL, cell overlay)`,
  `Index eq (cell overlay)`, `Index eq + LIMIT (cell overlay)` and
  `Projected point lookup (params)`. The canary runs the current benchmark
  script against both wheels, so the base/current comparison shows the overlay
  fixes directly: **-92.0% / -93.7% point reads, -99.2% index equality, -92.0% index
  equality + LIMIT, and -78.7% for the parameterized projected lookup**.
- The public 1,000,000-row scoreboard completes all 103 tabular metrics plus
  6/6 exact-vector and 6/6 quantized-vector comparisons, winning **102/103**
  tabular against the best of SQLite/DuckDB (the single loss is `UNION ALL
  (ordered)` at 1.05×, one of the set-operation shapes the project documents as
  load-sensitive) and **6/6 + 6/6** vector comparisons.
- Against the previous v1.36.2 snapshot, this single-run comparison flags eight
  metrics — `Aggregation (5 funcs)` +20.7%, `COUNT(*)` +29.0%, `COUNT(DISTINCT
  category)` +27.5%, `Filter (name = 'user_5000')` +46.9%, `GROUP BY category (10
  groups)` +32.8%, `INTERSECT (ordered)` +71.0%, `Retrieve Many (SQL, 100 IDs)`
  +23.3% and `UNION ALL (ordered)` +238.4% — the small/set-operation shapes
  whose run-to-run variance the project already records (two runs of one tree
  can flag a different set). The authoritative same-machine gate against
  `origin/main` passed all 109 public metrics and every dedicated phase, and the
  point-lookup metrics improved (`Point Lookup (SQL by ID) -2.2%`), so these are
  recorded as baseline noise rather than engine regressions; the refreshed
  baseline now starts from this V5-generation run instead of the v1.36.2
  snapshot.

### Validation And Performance

- Release build, the complete serial Python suite (**2,475 passed, 0 failed,
  0 skipped**, verified from the JUnit XML the release workflow consumes) and
  the complete Rust suites in every release configuration: **755 unit + 6
  documentation tests** with default features, **755 + 6** core-only
  (`--no-default-features`), **761 + 6** with `server` and **759 + 6** with
  `flight`.
- Same-machine canary gate against `origin/main`: **84 metrics checked, all
  passed**; the only metric flagged by the first run
  (`Float partial range COUNT`, +139%) re-measured at +0.18% on the repeat,
  confirming the GC-stall noise documented for that shape in v1.37.0.
- Same-machine full gate against `origin/main`: **passed (exit 0)** across all
  13 phases — public scoreboard (**109 metrics**), Q/s (14), quantized (8),
  index (4), parallel (4), wide-table (20), string-zone (4), HTAP (10),
  hybrid (12), PG-wire (7), hot-shape (15) and mixed (2).
- Windows release readiness was audited before tagging because the release
  workflow builds and tests on `windows-latest`: every Unix-only syscall is
  behind `cfg(unix)` / `cfg(target_os = "macos")` / `cfg(target_os = "linux")`
  with a Windows or portable fallback (`flock` via `fs2`,
  `read_exact_at`/`seek_read`, `sync_all`, the deferred-delete metadata path,
  the Unix-only WAL fault-injection helpers that are `cfg(all(test, unix))`,
  and Linux-only `MAP_POPULATE`/`MADV_SEQUENTIAL`), Windows has dedicated
  `FILE_FLAG_SEQUENTIAL_SCAN` and prefault paths, and no Unix-only dependency
  was added. This release's changes are portable Rust plus stdlib Python, and
  the new tests run in well under a second each. (A local
  `--target x86_64-pc-windows-msvc` check reaches only the C dependencies —
  mimalloc/zstd need the Windows toolchain — so the Windows jobs remain the
  authoritative check.)
- Refresh `benchmarks/latest_public_baseline.json` from this release's
  1,000,000-row run (kept at `benchmarks/results/v1.38.0-public.json`) so public
  comparisons are made against the V5 generation rather than the v1.36.2
  snapshot.

### Upgrade Notes

- **No API, SQL or format change** from v1.37.0: existing V4 and V5-preview
  files open unchanged, and no migration step is needed.
- A predicate `UPDATE` no longer requires a `REINDEX` to restore read latency;
  `REINDEX` keeps its existing meaning (rebuild postings, materialize the
  sidecar).
- `SELECT alias.*` combined with a JOIN now returns the joined tables' columns
  instead of failing or dropping them; it may carry the other side's columns as
  before for comma joins.
- Update the Rust crate and Python package version metadata to 1.38.0.


## [v1.37.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.37.0)
*2026-10-11*

[Compare with v1.36.2](https://github.com/BirchKwok/ApexBase/compare/v1.36.2...v1.37.0)

> **V5 architecture preview.** v1.37.0 is the first release of the **V5 storage
> and execution architecture**, published as a **preview**: the new layout, the
> new read lanes and the new write-exclusion rules are complete and validated on
> this tree, but the format is still allowed to change before V5 is declared
> stable. Read [V5 Preview Status](#v5-preview-status) before pointing existing
> data or older builds at it.

### Highlights

v1.37.0 is the largest change since the V4 row-group format landed. The storage
layer moves to **V5**: a file may declare format version `5` and carry V5-only
column encodings, tables reserve `_id` ranges against a shared on-disk
high-water mark so several processes can write one table without handing out the
same row id, and every public write path now takes the same cross-process
`flock`. The read side grows the lanes that the V4 profile showed were missing:
row-group row directories give single-row point reads direct column decoding, an
executor index lane serves planner-selected equality and range lookups
(covering-case scans never touch the table), full-text `MATCH()` selections
materialize only the rows they hit, and `ORDER BY <distance>(col, literal)
LIMIT k` runs as a bounded TopK over the requested columns only. On top of that
the release finishes the out-of-core work: file-backed query paths stream
instead of materializing, aggregation and join operators are admitted against a
query memory budget, and the same-machine gate now measures peak RSS as well as
latency. Grouped aggregation, join and vector semantics also pick up a batch of
silent-wrong-answer fixes — see the correctness sections below.

The V5 preview keeps **V4 compatibility in one direction**: this build reads V4
files, and V4 data written by this build (for example with
`APEX_WIDTH_FIDELITY=0`) stays byte-compatible, but a pre-V5 build must not open
a version-5 file.

### V5 Preview Status

- **What is stable.** The V5 write path (width fidelity, cross-process write
  exclusion and row-id reservation), the format-version declaration, and every
  new read lane are implemented, covered by tests on both the Rust and Python
  sides, and pass the complete validation chain recorded under
  [Validation And Performance](#validation-and-performance)
- **What is still open.** The `.apex` format version is declared per file and can
  gain further V5-only encodings in later releases; a file written today may
  therefore not be readable by a *future* V5 build that changes an encoding, and
  older (V4-era) builds cannot read a version-5 file at all. Point releases
  before V5 is declared stable may still adjust the encoding set
- **Opting out.** Set `APEX_WIDTH_FIDELITY=0` (or call the width-fidelity API)
  to keep writing the V4 layout and the version-4 declaration; V5-only features
  then stay disabled and the file remains readable by older builds
- **Reading the declaration.** The format version is the byte at offset 8 of the
  `.apex` header; `4` is the V4 row-group layout and `5` marks a table that uses
  a V5-only column encoding
- **No migration step.** Opening an existing V4 file works as before; a table
  only starts declaring version `5` once a write uses a V5-only encoding

### V5 Storage Format And Dual-Writer Safety

- Add the `Float32` width-fidelity encoding: a `Float32` column is stored at its
  declared four bytes per value (`FLAG_NARROW_FLOAT32` in the header flags plus
  `COL_ENCODING_PLAIN_F32` per column) instead of being promoted to eight bytes,
  and the table then declares format version `5`. Measured on the `wf` gate
  phase: **−12.5% bytes on disk**, **−32% first-touch page faults** on a
  `Float32` projection, and **−36%** on a range filter, with point reads and full
  scans unchanged
- Reject unknown format versions explicitly: opening a file whose header version
  is above the supported maximum now fails with
  `unsupported ApexBase format version N (this build supports up to 5)` instead
  of decoding unknown encodings
- Route **every** write path through one `<table>.apex.lock` `flock`
  (`storage/cross_process_lock.rs`): the single-row (`store_one`) and columnar
  (`store_columnar`) fast paths previously published through
  `Database::write_typed*`, which took only the in-process lock, so two processes
  writing one table could each read the same base and let the last publish drop
  the other's rows. The guard is re-entrant per thread and keeps the historical
  30-second contention budget with 100 µs → 5 ms backoff
- Reserve `_id` ranges against the shared high-water mark in the table sidecar
  (`epoch::reserve_row_ids`): ids may be skipped but never repeated, and the
  single-process sequence stays contiguous because the reservation minimum is
  that same next id
- Keep the per-table sidecar (`<table>.apex.lock`) authoritative for both the
  published generation and the row-id high-water mark, and document the naming
  rule as the single one that the SQL path, the Python bindings and the epoch
  tracker share
- Tolerate a concurrent publish in the table open path: a header read before a
  writer appended a row group and a footer read after it describe two different
  layouts, so the footer failed to parse (`V4 footer: schema overflow`) or failed
  the id/layout check and surfaced as a spurious error from `store()` — the
  cross-process writer test hit it as `OSError: V4 footer: schema overflow`. Both
  open paths now re-read the header, length and footer until they agree, with a
  bounded backoff; the read-side lane already did this by returning `None`
- Bounds-check the V4 footer and WAL readers so a corrupt or truncated file is an
  error instead of a panic: `FooterV4::from_bytes` computes the schema end with a
  checked add (a corrupt length wrapped `pos + schema_len` past the bound check
  and then panicked on a slice whose start was past its end), and
  `WalRecord::from_bytes_versioned` checks the 13-byte header, the body and the
  CRC (a record a crash left half-written aborted the process from
  `SELECT COUNT(*)` via `fast_row_count`). Both add Rust regression tests that cut
  a real record at every byte offset

### Read Lanes

- **Row-group row directories (RCIX) for point reads**: `retrieve_rcix` /
  `retrieve_rcix_projected` (through `read_row_by_id_to_arrow` and the Python
  bindings' `try_pk_read`) binary-search the row-group directory for the id and
  decode only the requested columns, deriving `_id` instead of reading the ID
  column. Compressed row groups carry the same per-column directory because the
  write path records column offsets before compression, so an id that falls in a
  compressed or older-encoded row group is served by the decode lane against the
  cached decompressed body, using the same `decode_point_column_value`
- **Point reads no longer fall off the fast lane**: `point_read_lane_available(id)`
  is now the single decision point separating "row does not exist" from "lane
  unavailable" (`retrieve_rcix*` returns `None` for both), including in the
  bindings' `try_pk_read`. A persistent table that had taken one `UPDATE` used to
  turn `_id = X` into a full table scan (**0.002 ms → 61.8 ms**, a 1470× cliff in
  the earlier profile); the fixed path returns **0.042 ms**, and compressed
  tables no longer re-parse and re-plan the statement on every lookup
- **Executor index lane**: `ApexExecutor::try_index_accelerated_read` serves
  planner-selected `OltpIndexLookup` single-column and composite equality and
  range predicates, and a covering lookup runs `try_index_only_scan` without
  reading the table at all. It yields to the scan lane whenever
  `indexes_stale(path)` or `backend.requires_merged_read()` holds, because
  persistent postings only describe persisted row groups and would silently drop
  overlay rows
- **Full-text row-selection pushdown**: a `WHERE` whose top-level `AND` contains
  one `MATCH()` now materializes only the matching rows
  (`try_fts_row_selection`). Without a deletion vector the resolved `_id` bitmap
  is mapped to physical positions (row-group directory `positions_for_ids`, else
  a scan of the `_id` column) and only the required columns are read, so vector
  and blob columns come back whole; with a deletion vector the path falls back to
  the id-based scalar reader. `OR`/`NOT` forms and non-clean overlays keep the
  existing lane
- **Distance TopK fusion**: `ORDER BY <distance>(col, [literal]) [ASC] LIMIT k`
  (including `SELECT <expr> AS alias … ORDER BY alias`) runs as a bounded TopK
  instead of a full sort. A clean, uncompressed table scores zero-copy over mmap
  (`order_by_distance_topk_direct`); a selective predicate first resolves
  candidate positions (`filtered_distance_topk`); everything else keeps the
  allocation-free SIMD distance plus bounded heap path folded inline. All three
  read only the columns the statement needs, and `MATCH()` forms are served by
  the FTS lane first
- **Staged-read correctness rules** are documented in `READ_PATH_CAPABILITIES.md`:
  the shared tail re-evaluates `WHERE`, so `read_matching_rows_for_tail` must
  keep predicate columns while early-return callers use the pruned
  `read_matching_rows_adaptive`; candidate positions are physical, so lanes that
  use them require a clean view with `active_row_count() == base_row_count()`;
  and the two `None` meanings of a point read must go through
  `point_read_lane_available`

### Query Execution And Correctness

- **Visible row space**: reads compose the visible rows as
  `[persisted base][.delta rows][buffered tail]` through
  `OnDemandStorage::visible_read_segments()`, and every lane keys off it. Earlier
  code that assumed the write buffer was the whole table could silently drop base
  rows after `spill_pending_v4_rows_to_delta()`; the first-value cache, string
  and index lanes now refuse the shapes they cannot serve
- **Grouped aggregation**: restore the deleted-row lane, keep `HAVING`-injected
  value aggregates on real pointers (a fused grouping kernel used to write
  through a null pointer and crash with `SIGSEGV`), keep NULL and `0`/`0.0` in
  separate groups (the `u64` identity hash merged them), group by expression
  columns and by join keys, and stop the two-key `GROUP BY u.id, o.id` form from
  emitting the first key twice — the emitter now prefers qualified names and
  matches at most one group key per projected column
- **Joins**: keep right-table non-key columns reachable after a swapped RIGHT
  JOIN with and without an appended `ON` predicate (`SELECT o.id`, `SELECT u.id`
  now return the correct side, previously an error or a silently wrong column);
  give counting joins a direct-index histogram so a 1M-row unfiltered count join
  drops from ~128 ms to ~15 ms and the join family's spread narrows from 150× to
  about 1.1–15×; and fix outer-join counting (unmatched preserved rows count 1,
  NULL keys never match)
- **Expression column names**: never strip a qualifier by splitting on the last
  `.`, which cut into floating-point literals and made `ORDER BY abs(score - 2.5)
  DESC` silently keep scan order and `GROUP BY ABS(score - 1.5)` collapse to a
  single group; use exact-name resolution first and the identifier-aware
  `strip_column_qualifier` otherwise
- **Deletes stayed visible through `ORDER BY … LIMIT k`**: a persisted deletion
  vector rewrites the header's row count to the surviving rows, so
  `active_row_count() == row_count()` even though the deleted rows are still
  stored *before* the survivors. The ORDER BY late-materialization lane read the
  sort column as the active view but addressed the chosen rows by physical
  position, so `DELETE FROM t WHERE k < 1000` + flush followed by
  `SELECT k, s FROM t ORDER BY s ASC LIMIT 5` answered with the deleted
  `u0000000..u0000004`; the filtered scatter path did the same and panicked a
  `take` once an index reached the active length
  (`index out of bounds: the len is 19000 but the index is 19000`). A nullable
  sort column is what routes the statement onto those lanes — the dictionary and
  plain string top-k lanes decline on NULLs. `TableStorageBackend` now exposes
  `has_persisted_deletions()` (asking the storage for the vector instead of
  comparing row counts), the lane reads winners back by `_id` in that state, and
  the scatter shortcut is skipped so the indexed reader answers
- **`ORDER BY … LIMIT n OFFSET m` applied the offset twice** on the top-k lane
  that reads only the k chosen rows: its index list already skipped `m` and the
  caller applied the page again, so `ORDER BY <plain string> LIMIT 10 OFFSET 7`
  returned rows 14..24. `compute_topk_indices_generic` and the inlined streaming
  heaps now return exactly the first `k` positions and no longer take an offset
  argument, which removes the double application and the ambiguous convention
  that allowed it
- **Encoding tag**: keep the one-byte encoding tag in the slice handed to
  `read_column_encoded`, whose dispatch reads `bytes[0]` — a caller that skipped
  the null bitmap and the tag read `count=216172782113783808` from a BITPACK
  column and reported truncation
- **Python writes**: reject out-of-range integers and unsupported Python objects
  with an explicit error instead of silently storing `NULL` or an empty string,
  and route all-scalar write batches (`INSERT`/`UPDATE`/`DELETE`/DDL/`COPY`)
  through the scalar result path so the first write of a session no longer pays
  the one-time `import pyarrow` cost (measured 43–51 ms, 33 ms of it the C
  extensions) before answering in 0.2–2.2 ms

### Out-Of-Core And Memory Budgets

- Stream file-backed query paths instead of materializing them: peak RSS for the
  out-of-core import shape falls from **1435 MB to 106 MB** and the incremental
  footprint from **+1397 MB to +39 MB**
- Admit sorting and materializing joins against the query memory budget, and make
  the storage-layer numeric grouping path report a clear error instead of
  succeeding silently on 5,000,000 groups; peak RSS for that shape drops from
  **1307.3 MB to 271.8 MB**
- Window all six out-of-core vector lanes (single and batch `f32`/`f16` plus the
  two binary encodings); a 1 MB budget yields 4.785 ms (single `f32`), 2.970 ms
  (single `f16`), 7.555 ms (batch `f16`) and 11.319 ms (batch `f32`) with results
  bit-identical to the unbounded run
- Cap the scan buffer only when asked: `APEX_SCAN_BUFFER_MB` bounds a
  whole-column scan buffer (default unlimited), and combine it with the existing
  dictionary cap to keep large scans bounded
- Add measured peak RSS to the gate (`--memory`, 103 RSS deltas) so a footprint
  regression fails the release check the same way a latency regression does

### Validation And Performance

- Stop panicking on a WAL record a crash left half-written:
  `WalRecord::from_bytes_versioned` sliced the 13-byte header, the body and the
  trailing CRC without checking the buffer length, so a truncated record aborted
  the process (`range end index 9 out of range for slice of length 7` at
  `storage/incremental.rs:410`) instead of ending the valid prefix that
  `read_all_with_offsets` scans. A record that cannot be parsed completely now
  returns `InvalidData`, and the length read from the file is bounds-checked with
  a checked add so a corrupt value cannot wrap into a slice inside the buffer.
  This was reachable from `SELECT COUNT(*)` (through `fast_row_count`) on any
  table whose WAL ended mid-record, and the release workflow's Linux Python 3.10
  job hit it in `test_kill_during_commit_is_all_or_nothing`, which kills the
  writer during commit. A Rust regression test now cuts a real record at every
  byte offset and asserts an error rather than a panic, plus a corrupt length, and
  that the complete record still round-trips
- `ORDER BY <plain string column> LIMIT k` declines its dedicated mmap lane on
  Windows (`scan_top_k_plain_string_indices_mmap` returns `None` there under a
  compile-time platform check): every Windows job of the release workflow aborted
  the process inside that walk while ordering 300k unique strings
  (`test_a_high_cardinality_string_orders_correctly_in_both_directions`), while
  the same test passes on Linux and macOS with the same binary layout. The walk's
  slices are all bounds-checked, so this is a platform-specific fault that has not
  been explained yet; Windows therefore takes the general path, which is slower
  but returns the same values, and the lane stays active everywhere else
- Stop asserting that the out-of-core importer's table file was observed
  *growing*: on a fast runner the 600k-row import finishes before the second poll,
  so `size > earliest` never held and the test failed on the macOS Python 3.9 job.
  What the parent can actually prove is that a non-empty table file existed while
  the importer was still running — a fully buffering implementation shows no file
  at all during the run — so that is what the test asserts now, with a 10 ms poll
  interval
- Read the PG wire server's resident set on Windows through
  `GetProcessMemoryInfo` instead of `ps`, which Windows does not ship:
  `server_rss_mb` returned `None` there, so the large-result memory assertion
  failed on every Windows job. The probe now answers on every platform the release
  workflow tests, and the assertion (well below the ~48 MB a fully materializing
  server added) is unchanged
- Give the out-of-core CSV scan assertion Windows working-set headroom (1200 MB
  there, 100 MB elsewhere, still below the +1397 MB the materializing path added)
  and write its CSV fixture in batched byte chunks instead of one encode per row,
  so the assertion sees less allocation noise
- Widen the bit-packed covering-aggregate cost band from 2x to 4x: both sides are
  single-shot medians on a shared runner, and a release run failed it at 2.4x
  while the per-row fallback it exists to catch was ~7.8x on the same shape
- Allow the saturated-statement timing comparison the same 10 ms the server's wait
  budget allows, because both sides are wall-clock samples taken on a busy
  machine; the rejection must still be shorter than the statement that held the
  slot
- Release build, the complete serial Python suite (**2,343 passed, 0 failed,
  0 skipped**), and the complete Rust suites pass on this tree: **731 unit +
  6 documentation tests** with the default features and again in the core-only
  (`--no-default-features`) configuration, **737 + 6** with `server` and
  **735 + 6** with `flight`
- Fix a release-blocking compile error in the core-only configuration: the
  compressed-batch test called the Python-only projected batch lane, so
  `cargo test --no-default-features --release` failed with two errors and would
  have blocked the `rust-test` job and therefore every wheel build. The test now
  gates only the projected-lane assertions, and the core-only suite builds and
  passes
- The public 1,000,000-row benchmark completed with ApexBase winning **102/103**
  tabular, **6/6** exact-vector and **6/6** shared quantized-vector comparisons,
  with no functional gap; the one tabular loss is
  `NOT filter (age NOT BETWEEN, name NOT LIKE)` at 3.49 ms against DuckDB's
  3.19 ms (1.1×), the load-sensitive file-I/O shape that a single public run also
  flagged in v1.36.2 and that the interleaved gate has repeatedly placed inside
  threshold
- Refresh `benchmarks/latest_public_baseline.json` from this release's full
  1,000,000-row run (the report is kept at
  `benchmarks/results/v1.37.0-public.json`) so public comparisons are made
  against the V5 generation instead of the v1.36.1 snapshot
- The full-mode same-machine gate against `origin/main` (v1.36.2) passes
  **10 of 11 phases** with a single marked metric — `CROSS JOIN COUNT`, the
  long-standing jitter of a path that has no join key, was reproduced negative by
  isolated A/B runs, and shares no code with this release's changes; it is
  reported here rather than described as a pass
- The remaining full-mode phases all pass: `htap` 10/10, `hybrid` 12/12, `qps`
  12/12, `quant` 8/8, `wf` 20/20, `wide` 30/30, `idx` 4/4, `par` 4/4, `zs` 4/4
  and `pgwire` 7/7
- Quantized recall stays above its per-codec floors (float16 1.000, bfloat16
  1.000, int8 0.975, uint8 0.985) and the vector lanes are checked to return
  exactly the brute-force top-k row sets

### Upgrade Notes

- **V5 preview**: this is a preview of the V5 architecture. Read
  [V5 Preview Status](#v5-preview-status) first if the data must stay readable by
  older builds
- **Format compatibility is one-way.** This build reads V4 files. V4-era builds
  must not open a version-5 file: they do not know the new encodings and would
  misread values instead of failing. Set `APEX_WIDTH_FIDELITY=0` to keep writing
  the V4 layout and the version-4 declaration when a file has to be handed to an
  older build
- **No API change and no migration step** from v1.36.2; existing V4 tables open
  unchanged and only start declaring version `5` once a write uses a V5-only
  encoding
- Concurrency gain: several processes may now write one table through any public
  write path, with `_id` ranges reserved from the shared high-water mark
- Update the Rust crate and Python package version metadata to 1.37.0


## [v1.36.2](https://github.com/BirchKwok/ApexBase/releases/tag/v1.36.2)
*2026-09-24*

[Compare with v1.36.1](https://github.com/BirchKwok/ApexBase/compare/v1.36.1...v1.36.2)

### Highlights

v1.36.2 is a compiler-hygiene and baseline-maintenance release. The crate now
compiles with zero warnings under every feature configuration the release
workflow builds — core, `server`, `flight`, the default `python` set, and the
full `python` + `server` + `flight` set that the wheels ship — which removed
roughly 2,500 lines of unreachable code without touching any reachable path, the
`.apex` file format, or the public Python, Rust, PostgreSQL-wire, and Flight
entry points. It also refreshes the checked-in public performance baseline to
the released v1.36.1 so that future comparisons are made against the current
generation instead of the stale v1.33.0 snapshot.

### Compiler Warning Cleanup

- Remove every unused item the compiler reported: 51 dead functions, constants, wrapper types, struct fields and helper methods; 65 unused imports; 47 unused bindings; 10 unneeded `mut`; 9 dead assignments; and 2 unreachable `match` arms, including a shadowed `SqlExpr::InSubquery` arm that duplicated an earlier pattern
- Delete the vestigial `update_by_id_cell_cache`: it was initialized, cleared and retained but never inserted into, so the per-client `DashMap` and its no-op maintenance calls only cost memory
- Keep the two file-handle fields in `MappedCatalog` / `MappedSchemas` with an explicit `#[allow(dead_code)]` and a comment, because they anchor the mapped file for the mapping's lifetime
- Gate the five helpers that only in-crate unit tests call (`cached_len`, `QueryMemoryBudget::limit`/`used`, `ScanBound::exclusive`, `PreparedCommit::txn_id`, `insert_typed_to_delta`) behind `#[cfg(test)]` instead of leaving them compiled into the shipping library
- Make `flush_out_rg!` return the flushed row count and assign it only where the counter is read, removing the dead store the final invocation performed while keeping the row-group flush boundary identical
- Gate the Python-only `Database` wrappers (`cached_backend`, `cache_backend`, `open_backend`, `open_insert_backend`, `create_backend`, `temp_dir`, `invalidate_query_cache`, and the FTS `wait_fts_backfill` / `has_fts_backfill` / `fts_manager` / `register_fts_manager` / `unregister_fts_manager` / `fts_backfill` / `enable_fts_config` / `read_fts_config` helpers) and the two mmap projection helpers behind `#[cfg(feature = "python")]`, and the executor's `wait_fts_backfills_for_dir` / `unregister_fts_manager` / `has_fts_backfill` / `cache_backend_pub` behind `#[cfg(feature = "python")]` (plus `test` for the two the Rust unit tests drive), so the core-only configuration the release workflow builds is warning-free too
- Silence the remaining non-unused diagnostics: explicit `{reg:v}` formatting for the aarch64 `fcvtl`/`fcvtl2` operands, an explicit `#[pyo3(signature = (where_clause, limit=None))]` for `_query_arrow_ffi`, `'_`-annotated lock guards, upper-case const-generic parameters in the fused LUT loops, `pub(crate)` / `pub(in crate::query::executor)` on the types the corresponding helpers expose, and removal of the `[profile.release.package.apexbase-server]` override that never matched a package

### Performance Baseline

- Regenerate `benchmarks/latest_public_baseline.json` from the released v1.36.1 tag (commit `9d365cb`), built locally with the same toolchain and `target-cpu=native` flags, keeping the comparison on one machine and one dependency set; the previous v1.33.0 snapshot is retained under `local-perf-results/`

### Validation And Performance

- `cargo check --release --all-targets` reports zero warnings and zero errors for every configuration the release workflow builds: core (`--no-default-features`), `server`, `flight`, the default `python` set, and the full `python` + `server` + `flight` set
- Release build, the complete serial Python suite (1,894 passed), and the complete Rust suites passed on this tree in both the default-feature and the core-only (`--no-default-features --release`) configuration (630 unit + 6 documentation tests each)
- The public 1,000,000-row benchmark completed with ApexBase winning 103/103 tabular, 6/6 exact-vector, and 6/6 shared quantized-vector comparisons; the previously load-sensitive `NOT filter (age NOT BETWEEN, name NOT LIKE)` measured 2.08 ms against DuckDB's 3.03 ms
- The same-machine canary against `origin/main` passed 64/64; the full-mode same-machine gate passed 109/109 metrics with zero regressions, together with its Q/s, quantized-index, index and parallel-scan phases
- The two JSON-read metrics that a single-shot public run flagged as slower were re-checked by the interleaved multi-sample gate, which placed them at +8.85% and +14.29% (inside threshold) with overlapping sample ranges, confirming run-to-run file-I/O noise rather than a regression

### Upgrade Notes

- No `.apex` file-format change, no API change, and no migration from v1.36.1
- Update the Rust crate and Python package version metadata to 1.36.2


## [v1.36.1](https://github.com/BirchKwok/ApexBase/releases/tag/v1.36.1)
*2026-09-23*

[Compare with v1.36.0](https://github.com/BirchKwok/ApexBase/compare/v1.36.0...v1.36.1)

### Highlights

v1.36.1 is a correctness follow-up to v1.36.0. It closes the race in which a
reader materializing a table `CREATE TABLE` had registered but not yet written
could replace the rows a concurrent writer was publishing, restores a typed-write
regression test that the v1.36.0 work dropped, and reaps the rewrite scratch files
that the new atomic publish path can leave behind after a crash. The V4 file
format and the public Python, Rust, PostgreSQL-wire, and Flight entry points are
unchanged.

### Lazy Table Materialization

- Materialize a registered lazy table only while its path is still free, and do it under the same lock that serializes base-file publication, so an empty materialized file can never replace rows a writer published in the meantime; when the file is already there it is adopted instead of overwritten
- Persist the catalog constraints of a materialized table under the table write lock, but only when the calling mutation does not already hold it

### Write Path Housekeeping

- Reap `<stem>.apex.<pid>.<seq>.tmp` rewrite scratch files after a crashed or killed writer: once per directory per process when a table is opened (a directory listing per open is not affordable on the temporary-table and cross-database read paths) and unconditionally when a table is dropped, which the fixed `TABLE_FILE_SUFFIXES` list could never match
- Drop the unused `read_lock` / `with_read_lock` / `with_write_lock` helpers and document the read contract they claimed instead: reads stay lock-free and correct through atomic publication, the per-table epoch that invalidates stale cached backends, and bounded retries around the publish window

### Tests

- Restore the Rust test `write_typed_columns_slow_then_fast_path` (slow schema-inferring write, then the V4 append fast path, with both row groups verified) that the v1.36.0 work removed, and remove its leftover duplicate `#[test]` attribute
- Add `materialize_adopts_an_existing_table_file`, which proves a published row survives materialization, and `scratch_sweep_removes_only_stale_rewrite_files`, which covers the stale-file threshold and the names the sweep must not touch
- The concurrent reader/writer coverage added in v1.36.0 (`test_engine_defect_regressions.py`, `test_write_scenarios.py`) is the Python-side guard for the materialization race; it passed 20/20 runs under CPU load

### Validation And Performance

- Release build, the complete serial Python suite, and the complete Rust suites (630 unit + 6 documentation tests for the core feature set) passed on this tree
- The public 1,000,000-row benchmark completed with ApexBase winning 103/103 tabular, 6/6 exact-vector, and 6/6 shared quantized-vector comparisons
- The same-machine canary against `origin/main` passed 64/64; the full-mode same-machine gate reported 102/103 metrics inside the threshold, and the single flagged metric (`ORDER BY expression (LENGTH)`, +16.08%) was reproduced as machine noise: its fastest sample equals the base's, and a back-to-back A/B on that exact query measured 1.748 ms against 1.745 ms

### Upgrade Notes

- No `.apex` file-format change, no API change, and no migration from v1.36.0
- Update the Rust crate and Python package version metadata to 1.36.1

---


## [v1.36.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.36.0)
*2026-09-23*

[Compare with v1.35.0](https://github.com/BirchKwok/ApexBase/compare/v1.35.0...v1.36.0)

### Highlights

v1.36.0 fixes the engine defects that surfaced while building the new
end-to-end example suite. Concurrent writers no longer lose rows, a `replace()`
followed by `delete()` no longer leaves a duplicated or unreachable row, a
value a write does not supply reads as `NULL` on every read path instead of the
column type's default, outer-join anti-joins and aggregate-free `GROUP BY`
return the documented rows, and window functions become general expressions
that can be nested inside scalar functions and arithmetic. The release also
ships a 17-scenario Python and 9-example Rust usage-example suite. The V4 file
format and the public Python, Rust, PostgreSQL-wire, and Flight entry points
are unchanged.

### Window Functions As Expressions

- Parse a window call anywhere an expression is allowed, not only as a bare projection: `ROUND(SUM(amt) OVER (PARTITION BY g), 2)`, `SUM(amt) OVER (PARTITION BY g) / 2`, `CAST(SUM(x) OVER (PARTITION BY g) AS INT)`; the executor computes the window and then finishes the surrounding expression
- Parse explicit `ROWS` / `RANGE` frames, including the `BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, `UNBOUNDED PRECEDING`, `n PRECEDING`, and `n FOLLOWING` endpoints, and reject a frame the executor does not compute with `Unsupported window frame for <FUNC>: ...` instead of silently answering with a whole-partition value
- Keep the v1.35.0 window surface working unchanged: `PARTITION BY`, `ORDER BY ... ASC|DESC NULLS FIRST|LAST`, the ranking functions, `LAG` / `LEAD`, `FIRST_VALUE` / `LAST_VALUE` / `NTH_VALUE` / `NTILE`, and the cumulative `SUM` / `RUNNING_SUM`
- Window support is still SELECT-list only: a window in `WHERE`, `HAVING`, `ON`, or `ORDER BY` fails with `Unsupported expression type ...`, two windows in one projection item are not executed yet, and a window combined with an aggregate in the same query currently returns the bare window value because that statement rebuild drops the projection wrapper

### Query Correctness

- Run aggregate-free `GROUP BY`: `SELECT cat FROM t GROUP BY cat` returns one row per group instead of one row per input row
- Stop pushing `IS NULL` conjuncts below the null-producing side of an outer join, so `... LEFT JOIN ... WHERE r.col IS NULL` returns the anti-join rows only; null-rejecting predicates (`IS NOT NULL`, comparisons, `LIKE`, `IN`, `BETWEEN`) still push down
- Return a named one-column batch for the join `COUNT(*)` fast path so a result alias is preserved
- Disable the bounded `ROW_NUMBER()` derived-table pushdown that could panic inside `arrow::compute::take`; the window is computed over the full input and the outer predicate filters afterwards, with retained row positions bounds-checked
- Keep in-memory `__cte_*` batches away from the cached-backend probe, so a CTE that reaches a join fast path no longer fails with `failed to open table '.../__cte_...'`

### Write Path Durability And Concurrency

- Hold a per-table write lock across the whole read-modify-write **including the flush** in every mutation entry point (`write`, `delete`, `delete_one`, `replace`, the column DDL operations, and both typed write routes), so two writers on one table can no longer interleave and lose each other's rows
- Publish base files under a per-path rewrite lock and a unique scratch name (`<stem>.apex.<pid>.<seq>.tmp`) instead of the shared `<table>.apex.tmp` that a concurrent rewrite or table open could consume
- Publish every base file, including a table's first write, through that scratch file and `rename` it into place, so a concurrent reader can never map an empty or half-written table: the `Empty file` write failures and the SIGBUS they caused on Linux are gone, and a transiently empty file is retried instead of reported
- Tag each cached backend with the table epoch its data was loaded from, and tag the executor's cache entry with the epoch observed before the file was opened, so a reader that fetches a backend just before a concurrent write commits can no longer pin a view that is missing the committed rows (`COUNT(*)` now agrees with a scan of the same table)
- Apply deferred deletes before an append derives its footer from disk, and publish deletion vectors under the rewrite lock, so a deferred delete cannot be undone by a concurrent append
- Record file length and mtime in the deferred-delete snapshot and discard it when either changed, instead of writing a stale snapshot over a newer file
- Serialize first-time table materialization, re-check the backend cache under that lock, and retry an open that lands between footer and header publication (8 attempts, 50 µs backoff) instead of surfacing `Corrupt Apex file: invalid footer offset` or `No such file or directory`
- Never reuse an mmap-only read backend for a write, and record the real V4 format flag in the schema cache so a V4 table is not routed down the legacy delta append
- Record the rows a delta-file compaction brings into memory, so the following save is not skipped and the merged rows are neither lost nor their ids reused
- Keep `next_id` raise-only against the delta high-water mark, so an INSERT after reopen cannot overwrite a delta row
- Write row groups with ascending ids and binary-search groups that are not implicitly contiguous, so a row re-appended by `replace()` or a group with gaps after deletes stays reachable
- Stop `append_row_group` from pushing persisted rows into the pending id buffer, and key the "base is loaded" decision on `v4_base_loaded` instead of the first buffered id
- Do not count a delete twice when the row is already deleted in the delta overlay

### NULL And Type Coercion

- A value a write does not supply now reads as SQL `NULL` on every path (full scan, projection, point lookup, `retrieve()`), not `0` / `0.0` / `false` / `''` / empty bytes
- Widen an integer value written into a `FLOAT` / `DOUBLE` column on the typed, columnar, delta, and row-wise insert routes (`1` stores `1.0`)
- Promote a columnar column whose integer-typed list contains a float to `Float64`; `[1, 2.5, 3]` used to be stored as `[1, 0, 3]`
- Store an uncoercible value as `NULL` (1.5 into an `INT` column, `"1.5"` into a `DOUBLE` column, `1` into a `BOOL` column) instead of the type's default
- Mark the rows padded by a later column addition as `NULL`, and read a row group written before a column existed as `NULL`
- Return the real count from `count_rows()` / `row_count()` on `:memory:` tables, which reported `0`

### Python And Embedded API Behavior

- Publish the single-row memtable before any other write route runs, so `store({...})` followed by `store([...])` no longer drops the single row
- Persist the memtable and the pending overlay and invalidate the table before SQL DML, so an `INSERT` no longer reuses an `_id` held by a buffered row
- Fall through to the general delete/replace path when the DeltaStore overlay declines an id, so `delete()` / `replace()` on a `replace()`d row performs the operation (and returns `True`) instead of returning `False` and leaving the row in place
- Skip the overlay fast path for process-local (`:memory:`) tables in `delete`, `delete_batch`, and `replace`
- Serve an `_id` projection over an empty result instead of raising `RuntimeError("Projected column '_id' does not exist")`
- `Table::retrieve` falls back to the query path while rows are buffered or when the table has a vector column, so a live row is no longer reported absent
- `Table::execute` publishes a concurrent writer's buffered rows before running a query, and `Table::flush` is a no-op when nothing is dirty or pending instead of rewriting the file from a possibly stale backend
- `ResultSet::scalar()` returns the count for a one-cell `Data` batch, and `arrow_value_at` decodes a `FixedSizeList` vector column into `Value::FixedList`
- Decline the typed delta encoding for a row whose value type it cannot represent, instead of storing the type's default

### Usage Examples

- Add 17 runnable Python scenarios (`usage-examples/Python/py01` ... `py17`) and 9 Rust examples (`usage-examples/Rust/r01` ... `r09`) covering analytics, HTAP transactions, SQLite migration, data quality, DataFrame interop, window functions, full-text search, vector search, hybrid retrieval, RAG, and a multi-source lake
- Index them in `usage-examples/README.md` with the verified SQL capability boundaries, three defect postmortems with measurements, and the exact run and verification loops
- Register the Rust examples as `[[example]]` targets so `cargo run --example r01_config_store --no-default-features` works from the repository root

### Documentation

- FTS: `ALTER FTS INDEX ... DISABLE` makes full-text reads unavailable (`MATCH()`, `FUZZY_MATCH()`, `FTS_SCORE()`, `search_text()`) instead of answering from a snapshot that is stale by definition
- Rust embedded API: document that `execute` synchronizes with a writer only while it still has buffered rows and re-checks under the table lock, and that `retrieve` / `retrieve_many` account for buffered rows before reporting a miss

### Validation And Performance

- Release build completed on this tree (Rust release profile and the CPython 3.12 wheel); the complete serial Python suite and the complete Rust suites for the default and `flight` feature sets were run against it before the release commit
- The frozen tree passed the same-machine base/current canary against `origin/main`: 64/64 metrics, no regression (`local-perf-results/20260923-000901/`)
- The full same-machine comparison in this cycle (1,000,000 rows, 109 metrics) and the extended index, parallel-scan, QPS, and quantized-vector comparisons passed against the same base (`local-perf-results/20260922-141339/`)
- Regression coverage added: 74 Python cases in `test/test_engine_defect_regressions.py` (33) and `test/test_write_scenarios.py` (41), plus 26 Rust tests across `apexbase/src/embedded/mod.rs`, `apexbase/src/query/sql_parser.rs`, and `apexbase/src/storage/on_demand/tests.rs`

### Upgrade Notes

- No `.apex` file-format change and no migration: `FORMAT_VERSION_V4` is still 4, and row groups written by this release remain readable by v1.35
- A value a write does not supply is now `NULL` rather than `0` / `0.0` / `false` / `''`; this changes `WHERE col = 0`, `COUNT(col)`, aggregates, and point lookups on data written from now on
- `delete()` returns `False` for a row that is already deleted (previously `True`), while `active_row_count()` becomes correct
- `SELECT cat FROM t GROUP BY cat` now returns one row per group, and `SELECT * FROM t GROUP BY x` is routed to the grouped engine
- A Rust caller matching `ApexResult` sees `Data` instead of `Scalar` for the join `COUNT(*)` fast path, and the column is named after the alias
- A bare `rows` or `range` used as an implicit result alias directly after a window specification is now parsed as a frame keyword; write `AS rows` / `AS range`
- Base-file rewrites now publish from `<stem>.apex.<pid>.<seq>.tmp`; a crash can leave such a scratch file behind, and dropping a table does not reap it
- Update the Rust crate and Python package version metadata to 1.36.0

---


## [v1.35.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.35.0)
*2026-09-21*

[Compare with v1.34.0](https://github.com/BirchKwok/ApexBase/compare/v1.34.0...v1.35.0)

### Highlights

v1.35.0 is a correctness and documentation release. It closes the remaining
process-local (`:memory:`) gaps in full-text search and table maintenance so an
in-memory database behaves like a filesystem database, turns three silent
failure modes into explicit errors or results, and refreshes the user,
reference, and internals documentation against the v1.34.0 runtime. The V4 file
format and the public Python, Rust, PostgreSQL-wire, and Flight entry points are
unchanged.

### In-Memory FTS Parity

- Report the FTS mmap lane as unavailable when a table has no persisted V4 base instead of returning an empty result, so a back-fill falls through to the generic Arrow read instead of silently indexing zero rows
- Mark process-local FTS engines as needing their first rebuild, so an `init_fts()` issued after writes back-fills the rows that already exist
- Mirror the process-local FTS configuration into the Python client, so SQL `CREATE` / `ALTER` / `DROP FTS INDEX` and the Python search API agree on enabled tables and `MATCH()` resolves on `:memory:` databases

### SQL Surface

- Accept `CREATE FTS INDEX ... WITH (lazy_load=..., cache_size=...)`; the tokenizer emits `Token::With`, so the previous `Identifier("WITH")` check never matched and the whole statement failed to parse
- Run `ANALYZE` and `REINDEX` on `:memory:` tables, keeping statistics and rebuilt postings in process memory instead of addressing a path that does not exist
- Keep the stale-index marker for `:memory:` tables in a process-local set, so `REINDEX`, commit-time index maintenance, and the stale-index read gate work everywhere; an `apexbase_memory:` path is not a legal Windows filename

### Explicit Failure Modes And Result Shape

- Reject unknown `add_column` type names with `ValueError` listing the supported names instead of silently creating a `String` column, matching `create_table()`
- Stop counting the hidden internal `_id` column in `ResultView.shape`, so `shape[1]` always equals `len(result.columns)` unless the result is requested with `show_internal_id=True`
- Register only process-local tables in the storage engine's in-memory registry; a filesystem path created with an explicit schema object is no longer cached there
- Route the FTS configuration accessors through the `Database` façade, keeping the bindings free of direct executor calls

### Documentation

- Add the read-path capability matrix, the resource-ownership inventory, and the fused-kernel design note to the documentation navigation and index
- Record the v1.34.0 public benchmark snapshot and label the retained v1.30 tables as historical
- Correct FTS options, durability levels, column and result semantics, vector SQL syntax, SQL type aliases, and method signatures against the built package
- Document the transaction commit-outcome contract, WAL durability, `REINDEX` recovery, `EXPLAIN ANALYZE` route/divergence/feedback, the per-query aggregation memory budget, and the parallel-scan worker budget
- Document the measured rationale for keeping the fused `GROUP BY` kernel as the default route in `docs/FUSED_GROUP_AGG_DESIGN.md`

### Validation And Performance

- Release build and complete serial Python suite passed: 1,820 tests
- Complete Rust suites passed: 602 unit + 6 documentation tests for default features, and 606 unit + 6 documentation tests with Flight
- Public 1-million-row benchmark completed with ApexBase winning 103/103 tabular, 6/6 exact-vector and 6/6 shared quantized-vector comparisons on the release runtime
- Same-machine canary passed 64/64
- Every fix carries Rust unit tests and Python regression tests, including a new `test/test_memory_table_sql_parity.py` suite
- The retained historical public-baseline comparison is cross-date (macOS 27.0 against the 26.6.2 baseline) and flags a small set of bimodal microbenchmarks; the same-machine comparison remains the regression decision

### Upgrade Notes

- No public API or `.apex` file-format migration is required
- `add_column` now raises `ValueError` for an unrecognized type name; callers that relied on the implicit `String` fallback must pass an explicit type
- `ResultView.shape` no longer includes the internal `_id` column; use `get_ids()` or `show_internal_id=True` when the row id is needed
- `:memory:` FTS indexes back-fill in either creation order, and SQL FTS DDL is visible to the Python search API
- `ANALYZE` and `REINDEX` on `:memory:` tables keep their state in process memory and write no files
- Update the Rust crate and Python package version metadata to 1.35.0

---


## [v1.34.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.34.0)
*2026-09-20*

[Compare with v1.33.1](https://github.com/BirchKwok/ApexBase/compare/v1.33.1...v1.34.0)

### Highlights

v1.34.0 completes the planned reliability and resource-control stages of the
September architecture program. It gives transaction failures an explicit
outcome contract, extends WAL recovery through UPDATE and index failures,
streams supported base-plus-delta scans and Flight results in bounded batches,
and introduces per-query aggregation memory budgets. The release keeps the V4
format and public Python, Rust, PostgreSQL-wire, and Flight entry points
compatible.

### Transaction And Recovery Semantics

- Classify commit errors as `not_committed`, `unknown`, or `committed`, retaining the transaction ID, original I/O error kind and source so callers can distinguish a safe new transaction from a result that must be reconciled after reopen
- Propagate Safe/Max durability through WAL commit, reject durable UPDATE shapes before the commit point when they cannot be recovered, and replay recoverable transactional UPDATE records after restart
- Apply multi-table transaction work in deterministic table order and document the crash boundary as per-table convergence rather than claiming database-wide crash atomicity
- Mark persistent secondary indexes stale before committed table changes, refuse stale postings in both planning and execution, and rebuild safely through `REINDEX`
- Preserve visibility publication and cache invalidation when post-commit watermark maintenance fails, preventing a logically committed transaction from being replayed

### Batched And Parallel Query Execution

- Add a serial row-group pipeline for supported single-table `Filter -> GROUP BY -> HAVING -> TopK` queries, consuming selection vectors directly without gathering a temporary compact batch
- Stream persisted base rows together with delta inserts, cell updates and deletes through a stable overlay snapshot, while retaining exact single-batch fallback for unsupported types and unflushed in-memory state
- Add bounded parallel row-group scan/fold execution with caller-thread cancellation, per-query worker limits and explicit 2/4/8-thread performance coverage
- Carry executable index access details in the query plan and report the physical route through `EXPLAIN ANALYZE`, including plan/execution divergence when correctness requires fallback
- Persist explicit `EXPLAIN ANALYZE` cost feedback per table, invalidate it after schema changes, and ignore sidecars from incompatible OS, architecture or parallelism environments

### Resource Ownership And Flight Streaming

- Enforce a configurable per-query byte budget for supported aggregation state across serial and parallel workers, returning a clear out-of-memory error and releasing reservations on success, cancellation, failure and fallback
- Bound planner feedback, statistics and CTE caches, including RAII cleanup for failed CTE evaluation, without adding a global query-path lock
- Propagate session context and cancellation through the scheduler and bound its work queue
- Stream supported Flight projections through a capacity-two channel with slow-consumer backpressure and disconnect cancellation; split materialized fallbacks into 65,536-row delivery chunks
- Cache schema discovery with a fixed capacity and avoid a second full execution for supported streaming queries

### Read-Path Correctness And Maintainability

- Make `OverlayState` the single source for read-lane visibility gates and publish a capability matrix covering supported types, overlay states and exact fallback targets
- Fix merged reads with persisted delete vectors so physical base positions, active-row windows and delta patches remain in the same row space
- Fix repeated flush after in-memory V4 appends by refreshing stale footer metadata without re-entering the footer write lock while a read lock is held
- Split SELECT implementation blocks by responsibility while preserving one executor module and its existing dependency direction
- Persist same-machine performance-gate provenance, including actual source inventories, binary patches, Cargo locks, wheel digests and raw comparison exit states

### Validation And Performance

- Release build and complete serial Python suite passed: 1,811 tests
- Complete Rust suites passed: 596 unit + 6 documentation tests for default features, and 600 unit + 6 documentation tests with Flight
- Public 1-million-row benchmark completed with ApexBase winning 103/103 tabular, 6/6 exact-vector and 6/6 shared quantized-vector comparisons
- Same-machine canary passed 64/64 after the required five-sample confirmation; the original `4d85ed2` architecture-closeout base also passed 64/64
- Same-machine full gate passed all 135 checks: 109 main, 10 Q/s, 8 quantized-vector, 4 index and 4 parallel metrics
- The retained historical public-baseline comparison flagged five cross-date metrics on a different macOS version; each corresponding metric passed the same-machine full comparison, and the original nonzero historical comparison remains recorded

### Upgrade Notes

- No public API or `.apex` file-format migration is required
- Persisted plan-feedback sidecars from schema versions before v3, or from a different environment fingerprint, are ignored and recalibrated on the next `EXPLAIN ANALYZE`
- An `unknown` commit outcome must be reconciled by reopening and checking authoritative state; applications must not blindly replay its DML
- Multi-table durable transactions converge deterministically per table after a crash but do not provide database-wide crash atomicity
- Update the Rust crate and Python package version metadata to 1.34.0

---


## [v1.33.1](https://github.com/BirchKwok/ApexBase/releases/tag/v1.33.1)
*2026-09-05*

[Compare with v1.33.0](https://github.com/BirchKwok/ApexBase/compare/v1.33.0...v1.33.1)

### Highlights

v1.33.1 completes the first fused filtered-grouped execution kernel introduced
by the 1.33 scan architecture and changes the public cross-engine benchmark to
an explicit no-result-cache contract. On the retained 1-million-row run,
`Boolean Filter+GROUP+HAVING+TopK` completes in 3.98 ms in ApexBase versus
5.68 ms in DuckDB; an independent repeat measured 4.02 ms versus 5.39 ms.

### Fused Boolean Grouped Execution

- Compile supported Boolean predicate trees into reusable per-slot programs for `AND`, `OR`, `NOT`, numeric comparisons, `BETWEEN`, `IN`, and dictionary-string equality
- Evaluate compact BITPACK integer predicates through per-row-group truth tables and reusable decode buffers, avoiding full-width temporary materialization
- Fuse predicate evaluation, grouping, aggregate updates, `HAVING`, ordering, and TopK while preserving NULL, sentinel-dictionary, and strict integer comparison semantics
- Keep unsupported expressions and over-budget truth tables on the general executor so the optimization never weakens SQL correctness

### Catalog And Lifecycle Safety

- Make the table registry authoritative for same-name CREATE and CTAS decisions, preserving registered table files on `AlreadyExists`
- Reap only unregistered orphan files before same-name recreation and defer DROP file unlinking with fingerprint checks so a newly recreated file cannot be deleted later
- Drain pending table deletions during client lifecycle cleanup and avoid unnecessary buffered-write and FTS filesystem work on ordinary table drops

### Benchmark Contract And Results

- Disable the ApexBase result cache for every public SQLite/DuckDB benchmark client instead of applying a one-off exception to a single metric
- Record `apex_result_cache: false` in benchmark JSON and retain `--no-result-cache` as a compatibility no-op because no-cache is now unconditional
- Reset `benchmarks/latest_public_baseline.json` to the new no-cache methodology; cached historical results are not compared as performance regressions
- Complete all 103 tabular metrics, 6 exact-vector metrics, and 8 quantized-vector rows; ApexBase won all 115 rows with a direct competitor in the retained run
- Add benchmark contract tests and Rust/Python correctness coverage for Boolean trees, LUT limits, BITPACK decoding, catalog-safe CTAS, and deferred deletion

### Validation

- Release build completed successfully before the final metadata and documentation-only edits
- Full Python suite: 1,739 passed
- Full Rust suite: 505 unit tests and 6 documentation tests passed
- Two complete public no-cache benchmark runs both recorded 103/103 tabular, 6/6 exact-vector, and 6/6 shared quantized-vector wins
- Per release instruction, no additional local base/current gate was run after the benchmark methodology changed globally

### Upgrade Notes

- No public API migration is required
- No `.apex` file-format migration is required
- Public benchmark results from earlier cached methodology are historical only and are not directly comparable to the v1.33.1 no-cache baseline
- Update the Rust crate and Python package version metadata to 1.33.1

---


## [v1.33.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.33.0)
*2026-09-01*

[Compare with v1.32.0](https://github.com/BirchKwok/ApexBase/compare/v1.32.0...v1.33.0)

### Highlights

v1.33.0 establishes a shared physical scan boundary between SQL execution and
storage. Instead of adding another query-shaped fast path for each benchmark,
the executor can now compose projection, predicate selection, grouping,
`HAVING`, ordering, and TopK over one storage contract. The first vertical
slice covers filtered grouped analytics on both persisted base data and
write-after-load delta state.

### Scan And Physical Execution

- Introduce `ScanRequest`, `ScanPredicate`, `ScanBound`, immutable Arrow-backed `ColumnView`, `SelectionVector`, and `Morsel` in a SQL-independent storage module
- Resolve predicate columns and Arrow types once per morsel, then evaluate supported numeric ranges and string equality in one conjunctive pass
- Preserve NULL behavior and support Int64, Float64, UTF-8, Large UTF-8, and UInt32-keyed dictionary strings in the shared selection evaluator
- Materialize selected rows only at the operator boundary; an all-row selection keeps the original Arrow arrays without an unnecessary `take`
- Carry physical row offset and row count in each morsel so future parallel scan scheduling can reuse the protocol without changing operator inputs

### Base And Delta Lane Planning

- Route base-only V4 tables through a selective mmap lane that prefers string equality, otherwise choosing the lowest zone-map-estimated numeric range instead of depending on SQL predicate order
- Gather projected rows when the selected set is at most 75% of the table; use a full projected read for dense selections to avoid expensive random materialization
- Reapply the complete predicate conjunction after candidate selection so a candidate access path never replaces residual SQL semantics
- Route delta files, pending deltas, pending V4 rows, and in-memory overlay state through the authoritative merged read before applying the same morsel/selection contract

### Filtered Grouped Pipeline

- Add the first complete vertical physical pipeline for conjunctive `WHERE -> GROUP BY -> HAVING -> ordered TopK -> LIMIT/OFFSET`
- Keep `HAVING` attached until after aggregation and before TopK, fixing the architectural ambiguity of legacy fused paths that did not own the complete semantic sequence
- Translate parentheses, `AND`, `BETWEEN`, string equality, numeric equality, and numeric `<`, `<=`, `>`, `>=`, including reversed literal/column forms
- Project only columns required by filters, grouping, aggregates, `HAVING`, and ordering before entering the shared scan path
- Retain the general SQL evaluator for joins, distinct/grouping expressions, OR trees, unsupported Arrow types, and other shapes outside this first protocol slice

### Correctness And Compatibility

- Preserve exact Int64/UInt64 comparison behavior by rejecting numeric bounds that cannot round-trip through the scan protocol's `f64` representation
- Preserve strict versus inclusive bound semantics, NULL exclusion, qualified/quoted column cleanup, delta visibility, and deterministic `HAVING`/TopK ordering
- Keep the public SQL and Python APIs unchanged; existing `.apex` V4 files remain compatible and no storage-format migration is required
- Keep unsupported queries on the existing general path rather than weakening semantics or returning approximate results

### Cache And Fixed-Cost Improvements

- Cache exact string cardinality summaries inside the epoch-checked `TableStorageBackend`, avoiding repeated filesystem metadata and global dictionary validation for warm `COUNT(DISTINCT string_column)` queries
- Clear cardinality summaries through the normal read-cache invalidation path after local mutations; externally changed tables receive a newly validated backend
- Skip table-overlay inspection for external-file-only analytical result-cache hits, which remain guarded by resolved path, file size, and nanosecond mtime
- Combine the clean-overlay check and current-table epoch lookup into one Rust/Python boundary call for cached table analytics while retaining cross-client write visibility

### Tests, Benchmarks, And Documentation

- Add Rust unit coverage for strict/inclusive numeric bounds, NULL handling, dictionary-string predicates, and base/delta `HAVING`-before-TopK behavior
- Add Python end-to-end coverage with the result cache disabled, rotating bound parameters, committed delta inserts/updates, wide-integer fallback, and stable cache-generation tokens
- Extend the same-machine canary with `Uncached delta Filter+GROUP+HAVING+TopK`, a parameter-rotating write-after-load metric that exercises the complete new path
- Document the protocol, lane selection, extension rules, fallback boundaries, cache visibility rules, and future parallel-morsel direction in the new Scan & Physical Execution architecture guide
- Update the Rust crate and Python package version metadata to 1.33.0

### Upgrade Notes

- No public API migration is required
- No `.apex` file-format migration is required
- Applications relying on exact wide-integer predicates keep the same results; such predicates intentionally use the general evaluator when the shared scan bound cannot represent them exactly
- Contributors extending scan predicates or operators should follow the protocol and fallback rules in `docs/SCAN_EXECUTION_ARCHITECTURE.md` and add corresponding base/current performance coverage

---


## [v1.32.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.32.0)
*2026-08-29*

[Compare with v1.31.1](https://github.com/BirchKwok/ApexBase/compare/v1.31.1...v1.32.0)

- Add a true process-local in-memory backend for `ApexClient(":memory:")`, with the same table, SQL, DDL, DML, index, and full-text query paths as disk-backed databases while avoiding database, catalog, WAL, delta, and index files
- Add the process-local `apexbase.execute()` convenience API, backed by a lazily initialized shared in-memory connection for small embedded SQL workflows
- Preserve in-memory read-your-write behavior across table creation, schema changes, inserts, updates, deletes, truncation, table listing, and database cleanup
- Add direct file fast paths for CSV/TSV reads, including `COUNT(*)`, bounded `LIMIT`/`OFFSET`, scalar aggregates, filtered aggregation, and string or numeric `GROUP BY` queries
- Reduce query-planning and scan overhead for `GROUP BY`, `HAVING`, join aggregation, dictionary-backed projections, null checks, and mmap TopK paths while retaining generic fallbacks for unsupported shapes
- Add bounded analytical result caching with lazy Python-dictionary or Arrow representations, file-generation and table-epoch invalidation, transaction/volatile-query protection, and write-visible invalidation for in-memory tables
- Harden dictionary-indexed projections so mixed, nullable, or out-of-range dictionary data safely falls back to the general reader instead of constructing invalid arrays
- Make Lance conversion safe for source columns containing dotted names by producing unique top-level field names
- Add cross-engine default-execute and in-memory comparisons, expanded CSV/file-read and cache-invalidation regression coverage, and a recovered full TFTP OLAP benchmark covering native, in-memory, and raw-file workloads
- Update the Rust crate and Python package version metadata to 1.32.0

---


## [v1.31.1](https://github.com/BirchKwok/ApexBase/releases/tag/v1.31.1)
*2026-08-23*

[Compare with v1.31.0](https://github.com/BirchKwok/ApexBase/compare/v1.31.0...v1.31.1)

- Reduce full serial pytest runtime by reusing the read-only 20,000-row ApexBase, SQLite, and DuckDB parity dataset across all 44 cross-engine assertions instead of rebuilding it for every test
- Keep the cross-engine parity suite compatible with SQLite versions before 3.46 while retaining dedicated ApexBase coverage for numeric literals with digit separators
- Preserve the full Python 3.9 Windows parity and memory suites by supplying standard SQLite math functions when the bundled SQLite omits them and by using correctly typed 64-bit Windows process handles for RSS measurement
- Keep sqliteai-vector tests active on Python builds whose standard-library SQLite disables extension loading by falling back to an extension-capable APSW connection
- Fuse the two predicate-side dictionary scans used by UNION, INTERSECT, and EXCEPT into one mmap pass with separate side counts, improving distinct and multiplicity-preserving set operations without query-specific cases
- Parallelize cached dictionary-filtered numeric aggregation by row group for all string equality filters, reducing long-tail latency across COUNT, SUM, AVG, MIN, and MAX combinations
- Remove all pytest ignore filters from the repository and release workflow so the memory comparison tests run as part of the complete suite
- Install DuckDB, Lance, and sqliteai-vector in the release test matrix and fail the release job if pytest reports any skipped test
- Run the complete Rust test suite for both tag-triggered and manually dispatched releases instead of substituting `cargo check` during manual backfills
- Add release-workflow regression tests that prevent test selectors, missing optional test dependencies, skipped pytest cases, or manual-dispatch Rust-test bypasses from returning
- Update the Rust crate and Python package version metadata to 1.31.1

---


## [v1.31.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.31.0)
*2026-08-22*

[Compare with v1.30.0](https://github.com/BirchKwok/ApexBase/compare/v1.30.0...v1.31.0)

- Accelerate compressed-vector TopK distance scans by scoring encoded rows directly, precomputing query-side state once, and retaining only a fixed-capacity candidate set instead of decoding every vector
- Add explicit AArch64 NEON and x86_64 AVX2/FMA kernels with runtime feature detection and a portable scalar fallback for supported hosts
- Optimize all stored precisions: Float16, BFloat16, Int8, UInt8, 1Bit, and deterministic TurboQuant 2/3/4-bit, including batch-query execution and exact source-vector reranking
- Add zero-copy mmap access for eligible single-row-group quantized scans, avoiding intermediate encoded-column materialization on the hot path
- Integrate quantized-vector measurements into `benchmarks/bench_vs_sqlite_duckdb.py`, covering all eight compressed precisions alongside exact ApexBase, sqlite-vector, and DuckDB vector metrics
- Standardize public quantized-vector measurements on 1,000,000 vectors with 128 dimensions and retain latency, recall, exact-rescore, storage, and cross-engine comparison data in the public report
- Expand local same-machine performance guards so full acceptance includes the 1M x 128D quantized-vector phase for all stored precisions
- Reduce GROUP BY and HAVING planning overhead for every key type, key count, aggregate combination, and predicate shape by borrowing the parsed statement unless HAVING truly requires an implicit aggregate column
- Accelerate multi-column `SELECT DISTINCT` for any bounded low-cardinality dictionary-key combination with an exact mixed-radix bitmap, while retaining the generic path for high-cardinality and mixed-type projections
- Reduce lazy-table `ALTER TABLE ADD COLUMN` metadata overhead by validating and replacing the schema under one catalog lock and one registry scan
- Refresh the performance and vector-quantization documentation with measured 1M x 128D results, SIMD architecture coverage, approximate-search semantics, and reproducible benchmark commands
- Update the Rust crate and Python package version metadata to 1.31.0

---


## [v1.30.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.30.0)
*2026-08-21*

[Compare with v1.29.0](https://github.com/BirchKwok/ApexBase/compare/v1.29.0...v1.30.0)

- Add native vector storage and search for Float32, Float16, BFloat16, Int8, UInt8, 1Bit, and deterministic TurboQuant-MSE-style 2/3/4-bit codecs, including SQL type aliases, persistence, Arrow projection, mmap reads, and single/batch TopK execution
- Add system-maintained quantized accelerator columns with streaming row-group backfill, automatic maintenance on append and source-vector replacement, persisted source/target dependency metadata, and rejection of direct writes that could make the projection stale
- Preserve the authoritative Float32, Float16, or BFloat16 source column for exact reranking: `topk_distance(..., accelerator=..., candidate_k=..., rescore=True)` performs a compressed coarse scan and reads only the selected source rows for exact-distance rescore
- Add `create_quantized_column()` and safe `drop_quantized_column()` lifecycle APIs; accelerators can be physically removed without deleting their source, while dropping a source with active dependants or using the quantized-drop API on an ordinary column is rejected
- Add Rust and Python regression coverage for all nine codecs, aliases, invalid/ragged inputs, persistence, compressed row groups, backfill, append/replace synchronization, rescore, recall trends, and safe source/accelerator deletion
- Add a dedicated vector-quantization benchmark and public API memory cases for accelerator creation/deletion, plus a complete quantization design and usage guide
- Let same-machine performance reports compare different ApexBase release versions while continuing to require identical runtime dependencies and build toolchains
- Update the Rust crate and Python package version metadata to 1.30.0

---


## [v1.29.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.29.0)
*2026-08-08*

[Compare with v1.28.0](https://github.com/BirchKwok/ApexBase/compare/v1.28.0...v1.29.0)

- Introduce a streaming Row-Group rewrite engine for V4 storage: delta-file compaction, compressed-row-group deletes, and `DROP COLUMN` now merge/rewrite Row Group by Row Group via mmap into a temporary file followed by an atomic rename, so peak memory is O(largest Row Group + delta payloads) instead of the whole table — tables larger than physical memory can be compacted and restructured without OOM
- Make `ALTER TABLE ADD COLUMN` footer-only on mmap-only V4 tables: only the footer schema is updated, read paths synthesize all-NULL values for rows already on disk, and later compactions materialize the column; `DROP COLUMN` and `ADD COLUMN` materialize any pending delta first
- Chunk appends by `row_group_size` so a single large batch is split into multiple Row Groups, bounding per-RG buffers and preserving zone-map granularity regardless of batch size
- Upgrade in-memory `String`/`Binary`/`StringDict` column offsets from u32 to u64, eliminating silent truncation for columns larger than 4 GiB while keeping the on-disk format (u32 offsets per Row Group) unchanged and fully compatible with existing files
- Build `store()`/`store_columnar()` columns directly from borrowed Python buffers (`&str` / `&[u8]`) through a new `write_typed_columns` path, removing per-element `String`/`Vec` allocation and the typed intermediate copy; FTS string retention now happens only when FTS is enabled
- Fix `_id` leakage and missing-column padding in mmap read paths (RCIX extraction, indexed row reads, and filtered limit scans) that were exposed by footer-only ADD COLUMN, and align column projection on numeric-range filter fast paths
- Expand regression coverage with Rust and Python tests for streaming compaction across many row groups, footer-only ADD COLUMN, streaming DROP COLUMN, compressed deletes, append chunking, u64 offsets, and the borrowed-buffer write path, and refresh the storage architecture documentation
- Update the Rust crate and Python package version metadata to 1.29.0

---


## [v1.28.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.28.0)
*2026-08-06*

[Compare with v1.27.0](https://github.com/BirchKwok/ApexBase/compare/v1.27.0...v1.28.0)

- Fix `create_table` silently rebuilding an existing table: table names are now managed by a per-database metadata registry (binary `.apex_tables` catalog), so a fresh process calling `create_table` on an existing table raises `Table already exists` instead of wiping data; `CREATE`/`DROP`/`ALTER` and cross-process creation are serialized by an exclusive catalog lock
- Fix `WHERE` + `ORDER BY ... DESC LIMIT` returning rows only from the first matching row group: filtered scans now read the full matching set before global top-k sorting, and non-projected `ORDER BY` columns (including `_id`) are read so sorting is always applied
- Unify internal `_id` visibility: explicitly projecting `_id` returns it consistently before and after flush, independent of which cached read path handles the query
- Add a native batch numeric UPDATE path used by `execute_batch` for `UPDATE ... SET <numeric col> = <literal> WHERE _id = N`, reducing a 10,000-row backfill from roughly 28-40 seconds to about 0.1 seconds, and expose a projected mmap row-read API on the Rust crate
- Add general SQL parameter binding to the Python client: positional `?`, named `:name`/`@name`/`$name`, IN-list expansion, string escaping, and arity/type validation, while keeping the TopK vector FFI fast path
- Introduce a binary, memory-mapped table catalog with per-entry CRC tamper detection, generation-based snapshot caching, and legacy `*.apex` backfill; the registry is the authoritative source of table names across processes
- Optimize core query routing: SQL classification results are cached in the core, and primary-key point/batch reads execute through a single combined FFI call on direct mmap readers, improving point lookups and projected ID batch reads
- Add cross-engine table-operation benchmarks (CREATE, DROP, CREATE+DROP, LIST, ALTER) and canary coverage, plus regression tests for the catalog, ORDER BY correctness, parameter binding, and batch updates
- Update the Rust crate and Python package version metadata to 1.28.0

---

## [v1.27.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.27.0)
*2026-08-01*

[Compare with v1.26.0](https://github.com/BirchKwok/ApexBase/compare/v1.26.0...v1.27.0)

- Optimize Arrow record batch processing for large, variable-length result sets, improving throughput for wide string and binary projections and reducing unnecessary materialization in Arrow conversion paths
- Add end-to-end support for large UTF-8 and binary values in Arrow-backed result batches, preserving correctness across `to_arrow()`, `to_record_batches()`, `CREATE TABLE AS SELECT`, and `INSERT ... SELECT`
- Improve JOIN and subquery execution so TopK-style lookups prune blob-heavy rows more effectively and keep qualified filters aligned with expected row semantics
- Introduce shared, mmap-backed table-epoch tracking so logical write invalidation is visible across processes and cached reads are invalidated more reliably after table mutations
- Strengthen on-demand storage projection and scan behavior for lazy column reads, including more precise pruning of unused columns and better handling of blob-aware reads
- Expand regression coverage for TopK parser/binding safety, join-filter behavior, blob projection, DML validation, and null/empty-value semantics
- Update the Rust crate and Python package version metadata to 1.27.0

---

## [v1.26.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.26.0)
*2026-07-29*

[Compare with v1.25.0](https://github.com/BirchKwok/ApexBase/compare/v1.25.0...v1.26.0)

- Harden V4 `.apex` file validation at open time: verify header/footer offsets, schema column counts, visible row counts, and row-group bounds, returning a clean corruption error for truncated or malformed files instead of risking a Rust panic
- Strengthen SQL DDL/DML validation and read-your-write consistency across `INSERT`, `UPDATE`, `DELETE`, schema changes, cached reads, numeric range statistics, and zone maps, including clearer errors for invalid types, column arity, and unsafe schema mutations
- Preserve SQL null semantics across expressions, grouping, aggregates, Arrow/Pandas conversion, temporary CSV tables, and projected row reads
- Improve vector `topk_distance` execution by validating query vectors and dimensions, applying filters before distance computation, and avoiding unnecessary reads of wide or unused BLOB columns in TopK JOINs
- Add binary parameter binding for single-query `topk_distance` calls, eliminating vector text interpolation in the Python API
- Extend full-text search with Boolean `AND`/`OR`/`NOT` expressions, configurable fuzzy matching, safer index reconfiguration, and richer index status information
- Add `ApexClient.execute_batch_parallel` for independent read-only SQL statements while retaining ordered `execute_batch` semantics for scripts
- Improve BLOB projection, `CREATE TABLE AS SELECT`, `INSERT ... SELECT`, Lance vector/temporal round-trips, correlated text subqueries, and process-safe concurrent writes
- Expand regression and performance coverage for storage corruption handling, DML and null edge cases, FTS, TopK JOINs with BLOB data, parallel batch execution, and Arrow batch-result APIs
- Update the Rust crate and Python package version metadata to 1.26.0

---

## [v1.25.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.25.0)
*2026-07-26*

[Compare with v1.24.0](https://github.com/BirchKwok/ApexBase/compare/v1.24.0...v1.25.0)

- Fix `REPLACE(...)` parsing in general expressions while preserving `SELECT * REPLACE (...)` projection syntax
- Add SQL scientific-notation numeric literals and move single-query vector TopK parameters to binary FFI instead of text interpolation
- Execute expression equality JOIN keys with hash join instead of Cartesian materialization, avoiding Arrow offset overflow on large joins
- Add `error`, `skip`, and `warn` malformed-row policies to CSV table functions, COPY, and temporary-table registration
- Align `metric="cosine"` with documented cosine-distance semantics; older releases could return the least similar rows for this alias
- Fix DELETE performance after UPDATE and append-only `.delta` growth by reusing file-derived delta caches across table epoch changes and avoiding full `.delta` rescans on the DELETE path
- Introduce a `Database` / `Session` façade so Python, Embedded, Server, and Flight query orchestration share one architecture boundary
- Unify table epoch cache invalidation so each logical write bumps the epoch once at the outermost scope, with merged delta reads that do not compact on the read path
- Split aggregation, DML, mmap scan, and Python bindings into domain modules while keeping parent files as thin assembly layers under architecture contract tests
- Move performance acceptance to local same-machine base/current guards, including canary and full runners, pytest runtime checks, and architecture contract coverage

---

## [v1.24.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.24.0)
*2026-07-17*

[Compare with v1.23.0](https://github.com/BirchKwok/ApexBase/compare/v1.23.0...v1.24.0)

- Add streaming, fixed-size RecordBatch imports for CSV and Parquet temporary tables, preserving rows and schemas across batch boundaries while keeping large file materialization memory-efficient
- Add direct Parquet `COUNT(*)` execution and numeric range-filtered `GROUP BY` aggregate fast paths to avoid unnecessary row materialization on common analytical queries
- Improve mmap range scans, numeric range `LIMIT` caching, aggregate WAL handling, and on-demand read/write paths for lower allocation and faster repeated queries
- Compact medium-cardinality string dictionaries using capacity derived from expected unique values, reducing memory overhead during ingestion and temporary-table workloads
- Correct schema-only V4 table writes and strengthen Arrow conversion, DML, blob, and storage handling across empty and incrementally populated tables
- Add an out-of-core CSV/Parquet benchmark against DuckDB covering direct file analysis, disk-backed materialization, repeated queries, peak RSS, and storage size
- Refresh the public ApexBase/SQLite/DuckDB benchmark scoreboard and performance documentation, including updated OLAP, OLTP, and vector metric accounting
- Expand Python regression coverage for cross-batch CSV/Parquet imports, file table functions, temporary tables, numeric range caching, SQL execution, and benchmark profile consistency
- Update the Rust crate and Python package version metadata to 1.24.0

---

## [v1.23.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.23.0)
*2026-07-15*

[Compare with v1.22.0](https://github.com/BirchKwok/ApexBase/compare/v1.22.0...v1.23.0)

- Replace the external `nanofts` crate with ApexFTS, an in-repo Rust full-text engine using `.afts` snapshots and checksummed `.afts.wal`
- Split ApexFTS into analyzer, engine, index, query, and storage modules while preserving Arrow zero-copy indexing paths such as `add_documents_arrow_str`
- Own the process allocator via `mimalloc` instead of inheriting one from the former FTS dependency
- Rebuild FTS from `.apex` table data when only legacy `.nfts` files remain; quarantine corrupt ApexFTS snapshots instead of opening them as valid data
- Improve query DDL/DML/select execution, SQL parsing, Python bindings, and FTS documentation and regression coverage
- Update the Rust crate and Python package version metadata to 1.23.0

---

## [v1.22.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.22.0)
*2026-07-14*

[Compare with v1.21.0](https://github.com/BirchKwok/ApexBase/compare/v1.21.0...v1.22.0)

- Add Lance dataset import/export helpers: `ApexClient.from_lance`, `ApexClient.to_lance`, and `ResultView.to_lance`
- Route Lance interoperability through Arrow tables for a lean in-process handoff while preserving ApexBase and Lance on-disk formats
- Add cost-based SELECT planning with executable candidate details, cardinality feedback, residual-predicate preservation, bounded join planning, and richer `EXPLAIN` timing and cost visibility
- Strengthen index and statistics correctness with generation-aware sidecars, typed composite keys, composite-prefix and range access, AND intersection, OR union, covering-index costs, Zone Map costs, and controlled row-id materialization
- Expand Hive-style user behavior benchmark coverage with 360-degree, complex, most-complex, and syntax-torture workloads plus DuckDB and SQLite equivalents
- Improve aggregation, DML, expression, join, window, mmap, and embedded execution paths for complex SQL and indexed workloads, with broader regression coverage for edge cases
- Refine memory-efficient public APIs and add Python and Rust memory benchmarks for internal and embedded access paths
- Expand the public benchmark scoreboard with 18 common OLTP microbenchmarks covering direct row counts, point reads, missing-row lookups, small projected reads, single-row writes, update/delete-by-id paths, and read-your-write checks
- Expand the public OLAP benchmark scoreboard with category/grouped ordering, HAVING, ascending TopK, distinct-count, JSON group-by, and filtered city aggregation metrics
- Reuse global string dictionary caches for filtered string aggregations so predicates such as `city = 'Beijing'` can aggregate numeric columns without reparsing the filter column
- Refresh README branding and performance snapshot documentation
- Update the Rust crate and Python package version metadata to 1.22.0

---

## [v1.21.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.21.0)
*2026-07-09*

[Compare with v1.20.1](https://github.com/BirchKwok/ApexBase/compare/v1.20.1...v1.21.0)

- Add Lance-like `BLOB` / `LARGE_BINARY` column support with descriptor-backed storage for inline, packed sidecar, and dedicated sidecar payloads
- Keep blob payloads lazy by storing compact descriptors in `.apex` column data and materializing Arrow `LargeBinary` values only when blob columns are projected
- Add Python helpers for single and batch payload access: `read_blob`, `read_blobs`, `read_blob_range`, `read_blob_ranges`, `read_blob_descriptor`, `read_blob_info`, and `read_blob_infos`
- Extend the Rust engine, SQL parser, Arrow conversion layer, Python bindings, WAL path, mmap path, and on-demand storage pipeline to handle `Blob` values end to end
- Add blob-focused performance coverage with `benchmarks/bench_blob_lance.py`, comparing ApexBase blob write/read/projection behavior against Lance Blob API
- Update the Rust crate and Python package version metadata to 1.21.0

---

## [v1.20.1](https://github.com/BirchKwok/ApexBase/releases/tag/v1.20.1)
*2026-06-30*

[Compare with v1.20.0](https://github.com/BirchKwok/ApexBase/compare/v1.20.0...v1.20.1)

- Add dynamic time-based column defaults in `CREATE TABLE`, including `DEFAULT CURRENT_DATE`, `DEFAULT CURRENT_TIMESTAMP`, `DEFAULT NOW`, and `DEFAULT UNIX_TIMESTAMP()`
- Add row-independent DEFAULT expressions, including arithmetic such as `DEFAULT (60 * 60)`, scalar functions such as `DEFAULT LOWER('ACTIVE')`, and typed casts such as `DEFAULT CAST('2026-01-02' AS DATE)`
- Add `INSERT DEFAULT VALUES` and `VALUES(DEFAULT, ...)` support so rows can explicitly use declared column defaults
- Apply defaults during `INSERT` for omitted columns and explicit `DEFAULT` values, with type-aware output for DATE, TIMESTAMP, string, integer, and floating-point columns
- Persist literal, expression-folded, and dynamic default definitions in on-demand table schemas so constraints survive save/reopen cycles
- Store SQL DATE and TIMESTAMP expression values through table and incremental storage paths by mapping them to their numeric backing representation
- Reject DEFAULT expressions that reference table columns or subqueries, keeping defaults row-independent and deterministic except for the supported time functions
- Add Rust and Python regression coverage for dynamic DEFAULT functions, constant-expression folding, cast defaults, `INSERT DEFAULT VALUES`, `VALUES(DEFAULT, ...)`, and invalid column-reference defaults
- Update Rust crate and Python package version metadata to 1.20.1

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Query Engine</td><td>4</td></tr>
<tr><td>Storage Engine</td><td>2</td></tr>
<tr><td>Tests</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v1.20.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.20.0)
*2026-06-23*

[Compare with v1.19.1](https://github.com/BirchKwok/ApexBase/compare/v1.19.1...v1.20.0)

- Add broad Hive complex SQL support across parser and executor for CTE-heavy ETL workloads, `INSERT OVERWRITE ... PARTITION`, Hive-style `LATERAL VIEW`, `EXPLODE`/`POSEXPLODE`, `STACK`, and richer expression handling
- Add optimized JSON projection fusion for repeated `GET_JSON_OBJECT` extraction and Hive array-splitting patterns
- Extend query signature detection and fast paths for complex Hive-style query shapes
- Improve aggregation, joins, SELECT, DDL, window execution, Python client, embedded API, and bindings to handle the new SQL coverage
- Add reproducible Hive complex SQL benchmark suite comparing ApexBase and DuckDB, including three large reference SQL workloads
- Add SQLite-equivalent SQL benchmark coverage for the same three Hive workload shapes, with ApexBase, DuckDB, and SQLite result-set equality checks across 100K, 500K, and 1M behavior rows
- Add comprehensive Hive complex SQL, CTE/EXPLAIN/INSERT SELECT, and storage architecture regression tests
- Add coding-agent prerequisite documentation in `precondition.md`
- Update version metadata to 1.20.0
- Enhance build and release workflows with tag-driven scheduled/manual builds, GitHub Release generation, release-note extraction by tag, historical pure-Python version releases, and Rust historical release backfills
- Simplify docs publishing/release-note generation flow and remove generated comments from release notes
- Simplify installation docs by removing Tool installation and GitHub Pages deployment sections

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>2</td></tr>
<tr><td>Project Config</td><td>3</td></tr>
<tr><td>Documentation</td><td>3</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Rust Embedded API</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>9</td></tr>
<tr><td>Storage Engine</td><td>1</td></tr>
<tr><td>Benchmarks</td><td>4</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Tests</td><td>3</td></tr>
</tbody>
</table></details>

---

## [v1.19.1](https://github.com/BirchKwok/ApexBase/releases/tag/v1.19.1)
*2026-06-11*

[Compare with v1.19.0](https://github.com/BirchKwok/ApexBase/compare/v1.19.0...v1.19.1)

- Fix macOS arm64 SIGSEGV crash on ApexStorage init via lazy imports (pyarrow, pandas, polars no longer loaded at import time)
- Fix weakref deadlock by switching `threading.Lock` to `threading.RLock` in `_InstanceRegistry`
- Fix CI pytest hangs in cross-process memtable tests
- Fix numeric range mmap scan offset handling in Python bindings
- Remove global warm-query GROUP BY / ORDER BY top-k static cache infrastructure from query executor
- Add pandas 3.x compatibility fixes in `to_pandas()`
- Rename `invalidate_query_caches` to `invalidate_read_caches` across storage backend
- Add `test_import_stability.py` regression tests and pytest `--timeout=120` config

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>1</td></tr>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>2</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>1</td></tr>
<tr><td>Storage Engine</td><td>1</td></tr>
<tr><td>Benchmarks</td><td>4</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Tests</td><td>3</td></tr>
</tbody>
</table></details>

---

## [v1.19.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.19.0)
*2026-05-29*

[Compare with v1.18.0](https://github.com/BirchKwok/ApexBase/compare/v1.18.0...v1.19.0)

- Set up comprehensive MkDocs Material documentation site with GitHub Actions deployment
- Add FLOAT16 vector column type with f16→f32 byte decoding and ndarray/list conversion in Python bindings
- FTS index now backfills rows written through the Python `store()` API
- Async FTS backfill for tables with >100K rows (background thread, non-blocking)
- FTS backfill reads string columns directly from mmap for zero-copy performance
- Add new documentation pages: concepts, installation, performance, user guides
- Add HTAP roadmap document
- Replace large inline README section with cross-reference to new docs site

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>1</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>23</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>6</td></tr>
<tr><td>Storage Engine</td><td>5</td></tr>
<tr><td>Tests</td><td>4</td></tr>
</tbody>
</table></details>

---

## [v1.18.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.18.0)
*2026-05-20*

[Compare with v1.17.0](https://github.com/BirchKwok/ApexBase/compare/v1.17.0...v1.18.0)

- Add LIMIT and OFFSET support in SQL queries (parser, executor, Python client)
- Improve index building for streaming batches with batch-size limit
- Add mmap_scan numeric range and string equality filter support with LIMIT/row offset
- Add Python client fast-path cache for projected string equality with LIMIT/OFFSET
- Add `retrieve_projected_by_string_eq_limit` for direct mmap-backed projected scans in Python bindings
- Enhance embedded API `register_temp_table` with LIMIT/OFFSET support and temp directory cleanup

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Rust Embedded API</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>4</td></tr>
<tr><td>Storage Engine</td><td>2</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Tests</td><td>2</td></tr>
</tbody>
</table></details>

---

## [v1.17.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.17.0)
*2026-05-06*

[Compare with v1.16.0](https://github.com/BirchKwok/ApexBase/compare/v1.16.0...v1.17.0)

- Add temporary table support: `register_temp_table` and `drop_temp_table` for CSV, JSON, and Parquet files
- Temp tables materialize into native .apex format, auto-cleaned on DB drop
- Add window function execution module (`window.rs`)
- Enhance DuckDB result materialization compatibility (`to_arrow_table` instead of `to_arrow`)
- Improve aggregation, join, and SELECT execution paths with temp table awareness
- Add comprehensive temp table tests (CSV, JSON, SQL query access, cleanup, persistence)

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>4</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Rust Embedded API</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>10</td></tr>
<tr><td>Storage Engine</td><td>1</td></tr>
<tr><td>Benchmarks</td><td>1</td></tr>
<tr><td>Tests</td><td>2</td></tr>
</tbody>
</table></details>

---

## [v1.16.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.16.0)
*2026-05-03*

[Compare with v1.15.0](https://github.com/BirchKwok/ApexBase/compare/v1.15.0...v1.16.0)

- Add COPY TO export support: tables exportable to CSV, TSV, JSON, NDJSON with configurable options
- Add JSON mutation functions: JSON_SET, JSON_INSERT, JSON_REPLACE, JSON_REMOVE
- Add view-aware SQL routing in Python client for persisted views
- Add comprehensive test suite for SQL view, COPY, and JSON operations
- Optimize expression evaluator with string-based LIKE, GLOB, and REGEXP improvements

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Query Engine</td><td>6</td></tr>
<tr><td>Benchmarks</td><td>1</td></tr>
<tr><td>Tests</td><td>7</td></tr>
</tbody>
</table></details>

---

## [v1.15.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.15.0)
*2026-05-02*

[Compare with v1.14.0](https://github.com/BirchKwok/ApexBase/compare/v1.14.0...v1.15.0)

- WAL-backed transaction durability: commits write TxnBegin/TxnCommit to per-table WAL with optional fsync
- Adaptive row group size for on-demand storage with narrow/wide table tests
- Add query signature fast paths: projected point lookup, ID batch, full scan, string/numeric range filter
- Zone map-based string filtering to skip irrelevant row groups during equality scans
- Single-pass filtered string aggregation on mmap tables
- Delta string index caching and row count caching for repeated read performance
- Separate write_file and delta_file handles with atomic sync-pending bitmask tracking
- Add `build.rs` for macOS Python library rpath linking
- Remove old test infrastructure (`run_tests.py`, `test/README`)

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>1</td></tr>
<tr><td>Other</td><td>5</td></tr>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>4</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Data Layer</td><td>5</td></tr>
<tr><td>Rust Embedded API</td><td>1</td></tr>
<tr><td>Arrow Flight gRPC</td><td>2</td></tr>
<tr><td>Full-Text Search</td><td>1</td></tr>
<tr><td>Library Core</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>4</td></tr>
<tr><td>Query Engine</td><td>18</td></tr>
<tr><td>Scaling & Sharding</td><td>5</td></tr>
<tr><td>PostgreSQL Wire Protocol</td><td>3</td></tr>
<tr><td>Storage Engine</td><td>24</td></tr>
<tr><td>Table Catalog</td><td>5</td></tr>
<tr><td>Transaction Manager</td><td>4</td></tr>
<tr><td>Benchmarks</td><td>1</td></tr>
<tr><td>Build System</td><td>1</td></tr>
<tr><td>Tests</td><td>20</td></tr>
</tbody>
</table></details>

---

## [v1.14.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.14.0)
*2026-03-27*

[Compare with v1.13.0](https://github.com/BirchKwok/ApexBase/compare/v1.13.0...v1.14.0)

- Add parallel multi-predicate mmap scan with zone map pruning and rayon-based predicate evaluation
- Add parallel column extraction via rayon for 2+ columns and 500+ rows
- Add column projection support to skip unrequested columns during Arrow batch construction
- Add Binary column type support (ColBuf::Bin, ColBuf::FixedVec, BinaryArray output)
- Fix col=value filter pushdown logic with type-specific handling
- Add 3 new benchmarks (numeric IN, OR cross-column, numeric OR)

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Query Engine</td><td>2</td></tr>
<tr><td>Storage Engine</td><td>3</td></tr>
<tr><td>Benchmarks</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v1.13.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.13.0)
*2026-03-26*

[Compare with v1.12.0](https://github.com/BirchKwok/ApexBase/compare/v1.12.0...v1.13.0)

- Fix `ApexClient` with `drop_if_exists=True` to always create fresh storage instead of reusing shared storage

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v1.12.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.12.0)
*2026-03-26*

[Compare with v1.11.0](https://github.com/BirchKwok/ApexBase/compare/v1.11.0...v1.12.0)

- Add backtick-quoted identifier parsing (`identifier`, Hive/MySQL style) to SQL parser
- Add unit tests for backtick parsing: SELECT, WHERE, ORDER BY, and unterminated identifier error
- Update docs to document quoted identifier syntax

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>4</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Query Engine</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v1.11.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.11.0)
*2026-03-18*

[Compare with v1.10.0](https://github.com/BirchKwok/ApexBase/compare/v1.10.0...v1.11.0)

- Add x86_64 AVX2+FMA SIMD kernels for L2, L1, Linf, inner_product, cosine distance functions
- Improve Windows mmap prefault with 3-tier strategy (single-threaded, rayon-parallel, PrefetchVirtualMemory)
- Add Windows atomic rename retries with backoff, engine cache invalidation before writes
- Increase Windows WAL buffer size from 64KB to 512KB
- Remove `benchmarks/stress_test.py`

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Query Engine</td><td>1</td></tr>
<tr><td>Storage Engine</td><td>3</td></tr>
<tr><td>Benchmarks</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v1.10.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.10.0)
*2026-03-18*

[Compare with v1.9.0](https://github.com/BirchKwok/ApexBase/compare/v1.9.0...v1.10.0)

- Drop V3 file format compatibility — all operations require V4 footer
- Replace heap-based GROUP BY with HashMap accumulator supporting SUM/COUNT/AVG
- Add `read_fixed_scattered_optimized` with row-group batching for FixedList/Float16List scatter reads
- Add `bench_filter_group_order.py` and `bench_group_by_detail.py` benchmark scripts
- Rename internal V3-specific constants (MAGIC_V3→MAGIC, HEADER_SIZE_V3→HEADER_SIZE, etc.)

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>4</td></tr>
<tr><td>Storage Engine</td><td>11</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Benchmarks</td><td>2</td></tr>
</tbody>
</table></details>

---

## [v1.9.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.9.0)
*2026-03-17*

[Compare with v1.8.0](https://github.com/BirchKwok/ApexBase/compare/v1.8.0...v1.9.0)

- Add Rust Embedded API module (`Database`/`Table` structs) for pure-Rust usage without Python
- Add query signature/caching system for deduplicating identical queries
- Add concurrent query scheduler for parallel batch execution
- Add vectorized hash join implementation
- Add concurrent storage access with shared table handles and multi-client support
- Add FLOAT16_VECTOR column type with f16-quantized storage and all distance metrics
- Add `batch_topk_distance` Python API for batched multi-query vector search
- Add `execute_batch` Python API for parallel SQL execution via scheduler
- Add schema evolution: `add_column`/`drop_column` on existing tables via Rust Embedded API
- Add new docs: RUST_EMBEDDED_API.md, FLOAT16_VECTOR_GUIDE.md, ENGINEERING_GUIDELINES.md

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>1</td></tr>
<tr><td>Other</td><td>4</td></tr>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>6</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Data Layer</td><td>1</td></tr>
<tr><td>Rust Embedded API</td><td>1</td></tr>
<tr><td>Library Core</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>14</td></tr>
<tr><td>PostgreSQL Wire Protocol</td><td>1</td></tr>
<tr><td>Storage Engine</td><td>14</td></tr>
<tr><td>Benchmarks</td><td>13</td></tr>
<tr><td>Tests</td><td>5</td></tr>
</tbody>
</table></details>

---

## [v1.8.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.8.0)
*2026-02-25*

[Compare with v1.7.0](https://github.com/BirchKwok/ApexBase/compare/v1.7.0...v1.8.0)

- Add `vector_ops.rs` module implementing 6 vector distance functions (L2, L1, Linf, cosine, inner_product, array_distance)
- Add TopK vector search via `ORDER BY array_distance(col, [query]) LIMIT k`
- Add SQL INSERT with vector array literals and string literal auto-coercion
- Add Python `topk_distance()` API with 6 distance metrics, custom column names, and numpy support
- Add `SQL explode_rename(topk_distance(...))` for SQL-based top-k with JOIN
- Add `set_compression`/`get_compression` API (LZ4, Zstd)
- Add `batch_replace` API for bulk row replacement by ID
- Add `optimize()` method to compact storage and `get_column_dtype` for schema introspection
- Add new benchmarks: bench_vector.py, bench_vs_polars.py, bench_no_cache.py, bench_simsimd.py
- Add `test_vector_ops.py` with 1000+ lines of distance tests cross-validated against numpy

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Other</td><td>2</td></tr>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>3</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Data Layer</td><td>2</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>12</td></tr>
<tr><td>PostgreSQL Wire Protocol</td><td>1</td></tr>
<tr><td>Storage Engine</td><td>12</td></tr>
<tr><td>Benchmarks</td><td>5</td></tr>
<tr><td>Tests</td><td>5</td></tr>
</tbody>
</table></details>

---

## [v1.7.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.7.0)
*2026-02-23*

[Compare with v1.6.0](https://github.com/BirchKwok/ApexBase/compare/v1.6.0...v1.7.0)

- Add full-text search (FTS) module with `CREATE FTS INDEX` SQL syntax and `search_text()` Python API
- Rewrite window function engine: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, SUM/AVG windows
- Fix NULL semantics: LAG/LEAD return NULL at partition boundaries, COUNT(col) excludes NULLs
- Add set operations: UNION (dedup), UNION ALL, INTERSECT, EXCEPT with multi-column variants
- Add subquery support: scalar subqueries, IN/NOT IN, correlated EXISTS
- Add multiple CTE support with chaining and CTE+window function combinations
- Add CASE expressions with WHEN/THEN/ELSE, including CASE in GROUP BY
- Add comprehensive test suites: test_comprehensive_coverage.py, test_sql_edge_cases.py
- Add bench_fts.rs example for FTS in Rust

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Full-Text Search</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>11</td></tr>
<tr><td>PostgreSQL Wire Protocol</td><td>1</td></tr>
<tr><td>Storage Engine</td><td>8</td></tr>
<tr><td>Benchmarks</td><td>1</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Tests</td><td>4</td></tr>
</tbody>
</table></details>

---

## [v1.6.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.6.0)
*2026-02-21*

[Compare with v1.5.0](https://github.com/BirchKwok/ApexBase/compare/v1.5.0...v1.6.0)

- Add in-place UPDATE execution via `scan_and_update_inplace` — writes directly to disk without full rewrite
- Rewrite delta store from log-based to map-based (`HashMap`) preventing unbounded log growth
- Add `delta_batch_update_rows` for multiple cell-level updates in a single lock acquisition
- Add `scan_numeric_range_mmap_with_ids` — combined WHERE column scan + ID retrieval in one pass
- Add pending delta application (`apply_pending_deltas_in_place`) for save_v4() baking
- Add DELETE + window function + FTS benchmarks
- Add warm no-gc benchmark mode for measuring without GC interference

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Query Engine</td><td>2</td></tr>
<tr><td>PostgreSQL Wire Protocol</td><td>1</td></tr>
<tr><td>Storage Engine</td><td>6</td></tr>
<tr><td>Benchmarks</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v1.5.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.5.0)
*2026-02-20*

[Compare with v1.4.0](https://github.com/BirchKwok/ApexBase/compare/v1.4.0...v1.5.0)

- Add `open_for_read_with_file` to save 2 syscalls by reusing pre-opened File handle
- Add fast path for SELECT COUNT(*) — bypasses SQL parser, reads directly from `active_row_count` atomic
- Add batch Arrow column → Python list converter (`arrow_col_to_pylist`) eliminating per-element dispatch
- Refactor LRU cache with AtomicU64 for lock-free access time tracking
- Add regex pre-compilation in Python client for faster SQL parsing
- Merge File::open + metadata into a single syscall in `get_cached_backend`
- Add 8 new benchmarks: full scan→pandas, multi-GROUP BY, LIKE, multi-condition, multi-ORDER BY, COUNT DISTINCT, IN filter, UPDATE

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>1</td></tr>
<tr><td>PostgreSQL Wire Protocol</td><td>1</td></tr>
<tr><td>Storage Engine</td><td>3</td></tr>
<tr><td>Benchmarks</td><td>2</td></tr>
</tbody>
</table></details>

---

## [v1.4.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.4.0)
*2026-02-20*

[Compare with v1.3.0](https://github.com/BirchKwok/ApexBase/compare/v1.3.0...v1.4.0)

- Add FTS auto-sync on INSERT: newly inserted rows automatically indexed
- Add FTS auto-sync on DELETE: deleted rows removed from FTS indexes
- Add CREATE FTS INDEX backfill: existing rows indexed on creation
- Add ALTER FTS INDEX ... ENABLE backfill support
- Add cross-database SHOW FTS INDEXES with enabled/disabled status
- Remove roadmap/planning docs (HTAP_GAP_ANALYSIS.md, HTAP_ROADMAP.md, P0_IMPLEMENTATION_PLAN.md)

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>7</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Query Engine</td><td>4</td></tr>
<tr><td>PostgreSQL Wire Protocol</td><td>1</td></tr>
<tr><td>Tests</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v1.3.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.3.0)
*2026-02-20*

[Compare with v1.2.0](https://github.com/BirchKwok/ApexBase/compare/v1.2.0...v1.3.0)

- Add SQL-native full-text search DDL: CREATE/DROP/ALTER FTS INDEX, SHOW FTS INDEXES
- Add FTS query predicates: MATCH('query') and FUZZY_MATCH('query') in WHERE clauses
- Add FTS_GUIDE.md documentation (478 lines) covering architecture, SQL usage, and configuration
- Upgrade nanofts to 0.5.0 with zero-copy Arrow indexing (~3.3M docs/s throughput)
- Add global FtsManager registry and fts_config.json persistence

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>5</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Full-Text Search</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>4</td></tr>
<tr><td>PostgreSQL Wire Protocol</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v1.2.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.2.0)
*2026-02-20*

[Compare with v1.1.0](https://github.com/BirchKwok/ApexBase/compare/v1.1.0...v1.2.0)

- Add multi-database support with isolated subdirectories, `use_database()`, and cross-database SQL
- Add Arrow Flight gRPC server with zero-copy columnar data transfer
- Add unified `apexbase-serve` CLI launching both PG Wire and Flight servers
- Add bench_flight.py and bench_pg_wire.py comparing server protocol performance
- Add mmap-based aggregation path, per-instance backend caching, SQL parse caching
- Add TCP_NODELAY on server sockets and per-connection USE/\c database switching
- Add comprehensive multi-database test suite (496 lines)

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>3</td></tr>
<tr><td>Python Package</td><td>3</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Arrow Flight gRPC</td><td>2</td></tr>
<tr><td>Library Core</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>3</td></tr>
<tr><td>Query Engine</td><td>7</td></tr>
<tr><td>PostgreSQL Wire Protocol</td><td>4</td></tr>
<tr><td>Storage Engine</td><td>8</td></tr>
<tr><td>Benchmarks</td><td>5</td></tr>
<tr><td>Tests</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v1.1.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.1.0)
*2026-02-10*

[Compare with v1.0.0](https://github.com/BirchKwok/ApexBase/compare/v1.0.0...v1.1.0)

- Add Extended Query Handler for prepared statement support
- Add SQL comment stripping (-- and /* */) before statement type detection
- Add proper PostgreSQL command tags (INSERT oid+rows, DELETE rows, etc.)
- Add pg_catalog support for pg_tables, pg_stat_user_tables
- Improve DROP TABLE with cleanup of WAL, delta, deltastore files
- Add line comment (--) support to SQL parser

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>3</td></tr>
<tr><td>PostgreSQL Wire Protocol</td><td>3</td></tr>
</tbody>
</table></details>

---

## [v1.0.0](https://github.com/BirchKwok/ApexBase/releases/tag/v1.0.0)
*2026-02-10*

[Compare with v0.6.0](https://github.com/BirchKwok/ApexBase/compare/v0.6.0...v1.0.0)

- Release v1.0 — first stable release
- Add PostgreSQL wire protocol server with DBeaver/psql/DataGrip/pgAdmin/Navicat support
- Add pg_catalog compatibility layer (pg_namespace, pg_database, pg_class, pg_attribute, information_schema)
- Add Arrow type→PostgreSQL type mapping and FieldInfo generation
- Major README rewrite with HTAP feature list (transactions, MVCC, indexing, window functions, 70+ built-in functions)

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Python Package</td><td>2</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Library Core</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>2</td></tr>
<tr><td>Query Engine</td><td>1</td></tr>
<tr><td>PostgreSQL Wire Protocol</td><td>4</td></tr>
</tbody>
</table></details>

---

## [v0.6.0](https://github.com/BirchKwok/ApexBase/releases/tag/v0.6.0)
*2026-02-09*

[Compare with v0.5.0](https://github.com/BirchKwok/ApexBase/compare/v0.5.0...v0.6.0)

- Split monolithic executor.rs (~11554 lines) into modular submodules (aggregation, ddl, dml, expressions, joins, select, window, tests)
- Split monolithic on_demand.rs (~10072 lines) into modular submodules (agg_wal, arrow_io, header, mmap_scan, read_write, storage_core, tests, types)
- Add window function support (ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE)
- Add CTE (WITH ... AS), EXPLAIN/ANALYZE query plans, UNION/UNION ALL support
- Add concurrency stress test, constraint tests, DuckDB memory comparison tests

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>1</td></tr>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>3</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Data Layer</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>13</td></tr>
<tr><td>Storage Engine</td><td>18</td></tr>
<tr><td>Transaction Manager</td><td>2</td></tr>
<tr><td>Benchmarks</td><td>1</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Tests</td><td>9</td></tr>
</tbody>
</table></details>

---

## [v0.5.0](https://github.com/BirchKwok/ApexBase/releases/tag/v0.5.0)
*2026-02-07*

[Compare with v0.4.2](https://github.com/BirchKwok/ApexBase/compare/v0.4.2...v0.5.0)

- Add query planner with OLTP (index-based) and OLAP (vectorized) routing
- Add indexing subsystem: B-Tree, hash index, index manager with CREATE/DROP/REINDEX SQL
- Add transaction support: transaction context, manager, OCC conflict detection
- Add MVCC engine: snapshot isolation, row versioning, garbage collection
- Add DeltaStore cell-level updates with merge-commit compaction
- Add global storage engine registry with LRU cache
- Add HTAP architecture support with scaling module (partition, shard, node, router)
- Rewrite on_demand.rs (~4000→9939 lines) with full columnar storage, LZ4/Zstd compression, WAL durability

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>8</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Library Core</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>5</td></tr>
<tr><td>Scaling & Sharding</td><td>5</td></tr>
<tr><td>Storage Engine</td><td>17</td></tr>
<tr><td>Transaction Manager</td><td>4</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Benchmarks</td><td>1</td></tr>
<tr><td>Tests</td><td>23</td></tr>
</tbody>
</table></details>

---

## [v0.4.2](https://github.com/BirchKwok/ApexBase/releases/tag/v0.4.2)
*2026-02-02*

[Compare with v0.4.1](https://github.com/BirchKwok/ApexBase/compare/v0.4.1...v0.4.2)

- Add persistent Row Group Bloom Filters for fast row group skipping
- Add DirectCountAgg for fast counting aggregation using direct array indexing
- Remove DuckDB comparison benchmarks and legacy test scripts
- Improve on_demand storage with per-column dictionary caching and SIMD filter scanning

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Other</td><td>13</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>2</td></tr>
<tr><td>Query Engine</td><td>2</td></tr>
<tr><td>Storage Engine</td><td>4</td></tr>
</tbody>
</table></details>

---

## [v0.4.1](https://github.com/BirchKwok/ApexBase/releases/tag/v0.4.1)
*2026-02-01*

[Compare with v0.4.0](https://github.com/BirchKwok/ApexBase/compare/v0.4.0...v0.4.1)

- Add comprehensive documentation: API_REFERENCE.md (865 lines), EXAMPLES.md, QUICK_START.md
- Add benchmark suite comparing ApexBase vs DuckDB
- Add bloom filter-based row group skipping in storage engine
- Add `list_databases()` method to Python client

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Documentation</td><td>5</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>2</td></tr>
<tr><td>Storage Engine</td><td>2</td></tr>
<tr><td>Other</td><td>6</td></tr>
<tr><td>Tests</td><td>3</td></tr>
</tbody>
</table></details>

---

## [v0.4.0](https://github.com/BirchKwok/ApexBase/releases/tag/v0.4.0)
*2026-02-01*

[Compare with v0.3.0](https://github.com/BirchKwok/ApexBase/compare/v0.3.0...v0.4.0)

- Add adaptive multi-column filter strategy with smart column ordering
- Add SIMD-accelerated take/gather operations (AVX2 on x86, NEON on ARM)
- Add fast path for Complex (Filter+Group+Order) queries
- Add `delete(where=)` support to Python client
- Refactor __init__.py (~1412→157 lines) by delegating to client.py

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>2</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>2</td></tr>
<tr><td>Query Engine</td><td>5</td></tr>
<tr><td>Storage Engine</td><td>2</td></tr>
<tr><td>Tests</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v0.3.0](https://github.com/BirchKwok/ApexBase/releases/tag/v0.3.0)
*2026-01-29*

[Compare with v0.2.3](https://github.com/BirchKwok/ApexBase/compare/v0.2.3...v0.3.0)

- Add DML support (INSERT, DELETE, UPDATE, TRUNCATE) to SQL executor
- Add DDL support (CREATE/DROP/ALTER TABLE) with schema management
- Add comprehensive DDL/DML test suite (~1957 lines)
- Add performance optimization fast paths for string/numeric/multi-condition filters
- Translate all documentation from Chinese to English

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>2</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>1</td></tr>
<tr><td>Query Engine</td><td>4</td></tr>
<tr><td>Storage Engine</td><td>4</td></tr>
<tr><td>Tests</td><td>2</td></tr>
</tbody>
</table></details>

---

## [v0.2.3](https://github.com/BirchKwok/ApexBase/releases/tag/v0.2.3)
*2026-01-27*

[Compare with v0.2.2](https://github.com/BirchKwok/ApexBase/compare/v0.2.2...v0.2.3)

- Add multi-platform wheel builds (Windows/macOS/Linux) for Python 3.9-3.13
- Refactor GitHub Actions: separate Linux manylinux build job

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>1</td></tr>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>2</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v0.2.2](https://github.com/BirchKwok/ApexBase/releases/tag/v0.2.2)
*2026-01-27*

[Compare with v0.2.1](https://github.com/BirchKwok/ApexBase/compare/v0.2.1...v0.2.2)

- Add maturin multi-platform wheel builds with Python interpreter path detection
- Remove old technical design docs
- Improve README with installation, usage, and benchmarks

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>1</td></tr>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>5</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v0.2.1](https://github.com/BirchKwok/ApexBase/releases/tag/v0.2.1)
*2026-01-27*

[Compare with v0.2.0](https://github.com/BirchKwok/ApexBase/compare/v0.2.0...v0.2.1)

- Version bump and minor CI fixes

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>1</td></tr>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v0.2.0](https://github.com/BirchKwok/ApexBase/releases/tag/v0.2.0)
*2026-01-27*

[Compare with v0.1.0](https://github.com/BirchKwok/ApexBase/compare/v0.1.0...v0.2.0)

- Complete core rewrite from Python to Rust with PyO3 bindings
- Add Rust-native columnar storage engine with .apex file format
- Add Rust-native SQL query executor with vectorized Arrow processing and Cranelift JIT
- Add Arrow IPC zero-copy data bridge between Rust and Python
- Add comprehensive test suite (~12000 lines)
- Replace DuckDB storage with custom columnar storage engine

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>1</td></tr>
<tr><td>Other</td><td>17</td></tr>
<tr><td>Project Config</td><td>2</td></tr>
<tr><td>Documentation</td><td>5</td></tr>
<tr><td>Python Package</td><td>1</td></tr>
<tr><td>Python Client</td><td>1</td></tr>
<tr><td>Data Layer</td><td>5</td></tr>
<tr><td>Full-Text Search</td><td>1</td></tr>
<tr><td>Library Core</td><td>1</td></tr>
<tr><td>Python Bindings</td><td>2</td></tr>
<tr><td>Query Engine</td><td>7</td></tr>
<tr><td>Storage Engine</td><td>4</td></tr>
<tr><td>Table Catalog</td><td>5</td></tr>
<tr><td>Tests</td><td>17</td></tr>
</tbody>
</table></details>

---

## [v0.1.0](https://github.com/BirchKwok/ApexBase/releases/tag/v0.1.0)
*2025-08-02*

[Compare with v0.0.2](https://github.com/BirchKwok/ApexBase/compare/v0.0.2...v0.1.0)

- Replace SQLite storage with DuckDB-based storage engine
- Add full-text search module with index creation, fuzzy matching, and snippets
- Rewrite ApexClient with caching, auto-switch, list_tables, close methods
- Add ResultView with to_dict/to_pandas/to_arrow converters
- Add comprehensive test suite (test_apex_client.py, 622 lines)

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>Other</td><td>14</td></tr>
<tr><td>Documentation</td><td>1</td></tr>
<tr><td>Project Config</td><td>1</td></tr>
<tr><td>Tests</td><td>4</td></tr>
</tbody>
</table></details>

---

## [v0.0.2](https://github.com/BirchKwok/ApexBase/releases/tag/v0.0.2)
*2025-01-25*

[Compare with v0.0.1](https://github.com/BirchKwok/ApexBase/compare/v0.0.1...v0.0.2)

- Add package metadata: Apache-2.0 license, PyPI classifiers for Python 3.9–3.13
- Fix GitHub Actions CI/CD pipeline for PyPI publishing

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>1</td></tr>
<tr><td>Other</td><td>1</td></tr>
<tr><td>Project Config</td><td>1</td></tr>
</tbody>
</table></details>

---

## [v0.0.1](https://github.com/BirchKwok/ApexBase/releases/tag/v0.0.1)
*2025-01-25*

- Initial release — project bootstrap
- ApexClient Python class for database management (CRUD operations)
- SQLite-based Storage with batch operations and LimitedDict LRU cache
- Query class with SQL-like filter parsing (WHERE, ORDER BY, LIMIT, BETWEEN, LIKE, IN)
- SQLParser/SQLGenerator modules for expression handling
- GitHub Actions CI/CD pipeline setup

<details>
<summary><b>Changed files by module</b></summary>

<table>
<thead>
<tr><th>Module</th><th>Files changed</th></tr>
</thead>
<tbody>
<tr><td>CI/CD</td><td>1</td></tr>
</tbody>
</table></details>

---
