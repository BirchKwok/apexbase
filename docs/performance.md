# Performance

This page tracks the latest verified local benchmark snapshot rather than an old best-case run. Benchmarks are meant to be reproducible, not magical; always rerun them on your own workload and hardware.

## Latest Verified Snapshot

- **Snapshot date / commit**: 2026-08-07, `08af3b2` on `main`
- **System**: macOS 26.6, Apple arm (10 cores), 32 GB RAM
- **Stack**: Python 3.12.2, ApexBase 1.28.0, SQLite 3.46.0, DuckDB 1.1.3, PyArrow 23.0.1
- **Dataset**: 1,000,000 rows x 5 columns (`name`, `age`, `score`, `city`, `category`)
- **Vector dataset**: 1,000,000 vectors x dim=128, `k=10`, batch size 10 queries
- **Method**: 2 warmup iterations + 5 timed iterations
- **Layout**: the default benchmark entrypoint tracks the README public scoreboard: 77 fair cross-engine tabular metrics (45 OLAP + 32 OLTP, including the five table-operation metrics added in v1.28.0) plus six competitive vector metrics. Apex-only vector metrics and other extended diagnostics live in `benchmarks/bench_vs_sqlite_duckdb_extended.py`.
- **Fairness rule**: only the default fair OLAP/OLTP cross-engine tables count toward the win/loss summary. Vector similarity uses a separate dataset and its own ApexBase-vs-DuckDB scoreboard.

## Scoreboard

| Scope | Metrics | Apex wins | Ties | Slower |
| --- | ---: | ---: | ---: | ---: |
| Default fair (OLAP + OLTP) | 77 | 75 | 0 | 2 |
| OLAP fair | 45 | 45 | 0 | 0 |
| OLTP fair | 32 | 30 | 0 | 2 |
| Vector similarity (ApexBase vs DuckDB) | 6 | 6 | 0 | 0 |

Total verified: **83 metrics** (77 tabular + 6 vector). The two slower tabular
metrics are `Table DROP` and `Table CREATE+DROP cycle` against SQLite's
empty-table operations; ApexBase remains faster than DuckDB on both (see
[Table Ops](#table-ops-new-in-v1280)).

Stock SQLite is not ranked in the vector table because the built-in `sqlite3`
used here has no native vector distance/top-k functions in this harness.

## Representative OLAP Gaps

| Metric | ApexBase | SQLite | DuckDB | Gap to best other |
| --- | ---: | ---: | ---: | --- |
| COUNT(*) | 0.087 ms | 7.83 ms | 0.528 ms | 6.1x faster vs DuckDB |
| SELECT * LIMIT 100 (warm cache) | 0.071 ms | 0.120 ms | 0.235 ms | 1.7x faster vs SQLite |
| Filtered LIMIT 100 (age>30) | 0.016 ms | 0.122 ms | 0.415 ms | 7.6x faster vs SQLite |
| GROUP BY city (10 groups) | 1.52 ms | 355.31 ms | 2.92 ms | 1.9x faster vs DuckDB |
| Temp Table (CSV) Query (filter+agg) | 0.429 ms | N/A | 0.668 ms | 1.6x faster vs DuckDB |

## Representative OLTP Gaps

| Metric | ApexBase | SQLite | DuckDB | Gap to best other |
| --- | ---: | ---: | ---: | --- |
| Bulk Insert (N rows; default fair) | 225.19 ms | 1.01 s | 178.57 s | 4.5x faster vs SQLite |
| Point Lookup (SQL by ID) | 2.92 us | 4.38 us | 3.09 ms | 1.5x faster vs SQLite |
| Retrieve Many (SQL, 100 IDs) | 0.195 ms | 0.435 ms | 5.49 ms | 2.2x faster vs SQLite |
| FTS Index Build (name,city,category) | 1.50 ms | 1.51 s | 1.32 s | 880x faster vs DuckDB |
| FTS Search ('Electronics') | 5.38 ms | 29.49 ms | 23.06 ms | 4.3x faster vs DuckDB |

## Table Ops (new in v1.28.0)

v1.28.0 added five cross-engine table-operation metrics to the fair OLTP
scoreboard: `Table CREATE (1 col)`, `Table DROP`, `Table CREATE+DROP cycle`,
`List tables (10)`, and `ALTER TABLE ADD COLUMN`. ApexBase wins 3 of 5; the two
losses are against SQLite's minimal empty-table DDL path, while ApexBase stays
ahead of DuckDB on every one of them.

| Metric | ApexBase | SQLite | DuckDB | Gap |
| --- | ---: | ---: | ---: | --- |
| Table CREATE (1 col) | 0.012 ms | 0.037 ms | 0.139 ms | 3.1x faster vs SQLite |
| Table DROP | 0.049 ms | 0.030 ms | 0.142 ms | 1.6x slower vs SQLite; 2.9x faster vs DuckDB |
| Table CREATE+DROP cycle | 0.107 ms | 0.069 ms | 0.346 ms | 1.5x slower vs SQLite; 3.2x faster vs DuckDB |
| List tables (10) | 5.96 us | 0.031 ms | 2.71 ms | 5.2x faster vs SQLite |
| ALTER TABLE ADD COLUMN | 0.057 ms | 0.074 ms | 0.128 ms | 1.3x faster vs SQLite |

## Representative Vector Gaps

SQLite is excluded here because stock `sqlite3` in this harness has no native vector distance/top-k support.

Single-query rows compare one materialized TopK result from each engine. Batch rows compare ApexBase's `batch_topk_distance()` with ten DuckDB single-query SQL calls over the same deterministic query batch; every DuckDB result is materialized before the next query runs.

| Metric | ApexBase | DuckDB | Gap to DuckDB |
| --- | ---: | ---: | ---: |
| TopK L2 | 7.73 ms | 32.50 ms | 4.2x faster |
| TopK Cosine | 7.87 ms | 43.32 ms | 5.5x faster |
| TopK Dot | 7.61 ms | 30.39 ms | 4.0x faster |
| Batch TopK L2 (10 queries) | 59.37 ms | 357.23 ms | 6.0x faster |
| Batch TopK Cosine (10 queries) | 70.25 ms | 480.35 ms | 6.8x faster |
| Batch TopK Dot (10 queries) | 59.35 ms | 475.29 ms | 8.0x faster |

## Hot-Path Latency Snapshot

The tables below come from the extended diagnostics profile
(`benchmarks/bench_vs_sqlite_duckdb_extended.py`) and are not part of the fair
scoreboard. They answer a different question: how fast is the already-loaded
hot path, and what happens when durability or transaction semantics are made
explicit?

Snapshot: 200,000 rows, 2 warmup iterations + 3 timed iterations, median
per-call hot-path latency, recorded 2026-08-07.

### Default Microbenchmarks

| Metric | ApexBase | SQLite | DuckDB | Gap to best other |
| --- | ---: | ---: | ---: | --- |
| COUNT(*) (direct API) | 4.0 us | 1.259 ms | 0.145 ms | 36x faster vs DuckDB |
| Point lookup (projected SQL) | 3.0 us | 3.0 us | 1.691 ms | tied with SQLite |
| Retrieve 100 IDs (projected SQL) | 0.034 ms | 0.112 ms | 4.532 ms | 3.3x faster vs SQLite |
| Insert 1 row (default fair) | 0.012 ms | 0.015 ms | 0.275 ms | 1.3x faster vs SQLite |
| UPDATE by ID | 3.0 us | 5.0 us | 0.491 ms | 1.7x faster vs SQLite |
| DELETE missing ID | 0.4 us | 4.0 us | 0.385 ms | 10x faster vs SQLite |

### Durable Fair Microbenchmarks

| Metric | ApexBase | SQLite | DuckDB | Gap to best other |
| --- | ---: | ---: | ---: | --- |
| Insert 1 row (durable fair) | 0.167 ms | 0.121 ms | 33.185 ms | 1.4x slower vs SQLite; 199x faster vs DuckDB |
| UPDATE by ID (durable fair) | 4.0 us | 7.0 us | 4.479 ms | 1.8x faster vs SQLite |

### Transaction Fair Microbenchmarks

| Metric | ApexBase | SQLite | DuckDB | Gap to best other |
| --- | ---: | ---: | ---: | --- |
| TXN empty (BEGIN+COMMIT; durable sync) | 2.0 us | 5.0 us | 0.177 ms | 2.5x faster vs SQLite |
| TXN read COUNT(*) (COMMIT; durable sync) | 0.023 ms | 1.345 ms | 0.320 ms | 13.9x faster vs DuckDB |
| TXN backlog string miss (COMMIT; 1500 preseed; durable sync) | 0.063 ms | 9.088 ms | 0.412 ms | 6.5x faster vs DuckDB |
| TXN backlog COUNT(*) (COMMIT; 1500 preseed; durable sync) | 0.024 ms | 1.459 ms | 0.312 ms | 13.0x faster vs DuckDB |
| TXN backlog INSERT+read-own-name (COMMIT; 1500 preseed; durable sync) | 0.374 ms | 9.929 ms | 36.439 ms | 26.5x faster vs SQLite |

## OLAP Throughput (Q/s)

The Q/s harness (mixed read profile: `COUNT(*)`, two `GROUP BY` scans, and a
filtered `LIMIT 100`, materialized to Python rows) currently fails on the
v1.28.0 build with `RuntimeError: Projected column 'city' does not exist`. The
failure is reproducible in the full extended benchmark flow at 200,000 and
50,000 rows: after a second `ApexClient` instance runs in the same process
(the materialization section), the first client's `GROUP BY` projection loses
its columns. SQLite and DuckDB Q/s values are unaffected:

| Engine | Single thread Q/s | 4 threads Q/s |
| --- | ---: | ---: |
| SQLite | 33.3 | 114.9 |
| DuckDB | 883.0 | 2,461.6 |

Until the projection bug is fixed, ApexBase Q/s cannot be reported from this
harness. The earlier published throughput snapshot (ApexBase ~123,700 Q/s
single-threaded) belongs to a previous release and should not be read as
current. This is tracked in the performance benchmark scripts; see
`benchmarks/bench_vs_sqlite_duckdb.py` (`run_qps_benchmark`) and the regression
test added with the fix.

## OLTP Write Visibility

ApexBase exposes two fast single-row append paths, and the benchmark keeps them out of the fair scoreboard because their visibility rules are Apex-specific:

- **Memtable OLTP** is the default fast single-row path for schema-stable `store({...})` calls with `durability="fast"`. The writing client can read the row immediately, managed clients in the same Python process share the storage instance, and `flush()` / `close()` persists pending rows.
- **Buffered OLTP** is explicit: call `begin_buffered_writes()`, issue many single-row `store({...})` calls, then call `flush_buffered_writes()` or `end_buffered_writes(flush=True)`. Buffered rows are not visible until flushed.

That separation is deliberate: the fair tables compare committed cross-engine behavior, while Apex-only write modes remain visible as diagnostics instead of being mixed into the competitive summary.

## Reproduce

Use the same command as the snapshot above:

```bash
python benchmarks/bench_vs_sqlite_duckdb.py
```

Add `--skip-vector` if you want a tabular-only rerun without the separate vector module.
Run `python benchmarks/bench_vs_sqlite_duckdb_extended.py --rows 200000 --warmup 2 --iterations 3` for the file-format, materialization, Q/s, microbenchmark, durable, transaction, buffered/memtable, and full vector diagnostics. Add `--output path.json` to keep a machine-readable report of any run.

### Out-of-core file comparison

Use the focused harness to compare ApexBase and DuckDB against the exact same
generated CSV or Parquet source:

```bash
python benchmarks/bench_out_of_core_import.py --rows 1000000 --format csv
python benchmarks/bench_out_of_core_import.py --rows 1000000 --format parquet
```

Each engine runs in an isolated process. The report separates direct file query
time, disk-backed table materialization time, repeated native-table query time,
incremental peak RSS, and storage size. Results and filtered row counts are
cross-checked before ratios are printed. DuckDB defaults to a `1GB` memory limit
and an explicit spill directory; change it with `--memory-limit 512MB`. Increase
`--rows` until the generated source exceeds physical memory for a true
out-of-core stress run. Ratios are reported as ApexBase divided by DuckDB, so a
value below `1.0x` is better for ApexBase.

On the same Apple Silicon development machine, the focused 1,000,000-row
Parquet run (21 measured queries after 5 warmups) produced this verification
snapshot:

| Metric | ApexBase | DuckDB | ApexBase / DuckDB |
| --- | ---: | ---: | ---: |
| Direct filtered Parquet count | 1.35 ms | 9.75 ms | 0.138x |
| Disk-backed materialization | 0.131 s | 0.278 s | 0.469x |
| Filter + GROUP BY + COUNT/AVG | 1.175 ms | 3.862 ms | 0.304x |
| Incremental peak RSS | 77.8 MB | 126.9 MB | 0.613x |
| Native storage size | 9.5 MiB | 6.5 MiB | 1.454x |

The native storage result first decreased from 33.9 MiB to 27.4 MiB by
selecting string dictionary encoding from its actual serialized size instead
of a periodic cardinality sample. One-, two-, or four-byte string dictionary
indices reduced it further to 22.6 MiB. Temp materializations now omit the
physical `_id` array when a row group's IDs are contiguous, reconstructing IDs
from `min_id` only in `.apex_tmp` files; this keeps mutable-table storage and
DML paths on their established format. Lossless low-cardinality `Float64`
dictionaries then reduce this workload to 9.5 MiB. The float encoding is used
only for at least 32K rows when sampling shows useful repetition and its
serialized size is below 70% of plain storage; high-cardinality data retains
the original encoding. The fused range-filter aggregation evaluates each
dictionary value once and scans compact row indices without materializing a
full float vector. Nullable inputs and more complex SQL shapes deliberately
fall back to the general executor.

Blob storage has a focused Lance comparison harness:

```bash
python benchmarks/bench_blob_lance.py --rows 200 --reads 200 --iterations 3
```

The script measures write throughput, non-blob projection scans, descriptor metadata reads, random full blob reads, random range reads, and projected blob materialization. It uses Lance Blob helpers when the installed Lance package exposes them, and reports unavailable or fallback modes explicitly.

For a larger stress run, increase `--rows` to `1000000`.
