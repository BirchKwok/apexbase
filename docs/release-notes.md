# Release Notes

This page summarizes the changes introduced in each ApexBase release, grouped by functional area.


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
