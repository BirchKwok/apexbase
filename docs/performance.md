# Performance

This page records the retained public cross-engine benchmark for ApexBase
v2.0.0. It is a reproducible snapshot, not a universal claim: rerun the suite
on your own hardware and workload before drawing conclusions.

v2.1.0 is a correctness release. Its same-machine base/current gate shows no
regressions on the same toolchain; compared with this v2.0.0 snapshot (built with
rustc 1.92) a new-stable codegen shift is visible on a few shapes — the base
revision built with the newer toolchain shows the same shift, so it is a
toolchain effect, not a v2.1.0 regression. The baseline below remains the
reference for cross-engine comparisons.

## v2.0.0 public snapshot

- **Date / source**: 2026-11-03, v2.0.0 release tree (commit `426424a`)
- **Baseline protocol**: median of three same-machine runs
  (`benchmarks/latest_public_baseline.json`, `baseline_meta.samples = 3`)
- **System**: macOS 27.0.1, Apple arm64 (10 cores), 32 GB RAM
- **Runtime stack**: Python 3.12.2, ApexBase 2.0.0, SQLite 3.46.0, DuckDB
  1.1.3, PyArrow 23.0.1, sqlite-vector 1.0.0
- **Build**: rustc 1.92.0, maturin 1.9.1, release profile
- **Tabular dataset**: 1,000,000 rows × 5 columns
- **Vector dataset**: 1,000,000 Float32 vectors × 128 dimensions, `k=10`,
  10 exact batch queries
- **Quantized module**: 1,000,000 rows, 20 queries, `candidate_k=100`,
  seed `20260821`
- **Method**: ApexBase result cache disabled, 2 warmup iterations + 5 timed
  iterations, materialized results
- **Retained report**: `benchmarks/latest_public_baseline.json`

The suite completes all **119 named rows**: 105 tabular, 6 exact-vector, and
8 quantized precision rows. Every row with a direct competitor is won —
**117/117 comparable**: 105/105 tabular, 6/6 exact vector, and 6/6 quantized
codecs shared with sqlite-vector. Float16 and BFloat16 are ApexBase-only
quantized formats and are excluded from the comparable total.

| Scope | Metrics | Apex wins | Ties | Slower |
| --- | ---: | ---: | ---: | ---: |
| OLAP fair | 105 | 105 | 0 | 0 |
| Exact vector similarity | 6 | 6 | 0 | 0 |
| Quantized vector, shared codecs | 6 | 6 | 0 | 0 |
| **Comparable total** | **117** | **117** | **0** | **0** |

### Workload scoreboard

| Workload | Metrics | Apex wins | Apex vs best competitor |
| --- | ---: | ---: | ---: |
| Load & Index | 2 | 2 | 0.09× (11.1× faster) |
| Point & Limited Reads | 17 | 17 | 0.19× (5.3× faster) |
| Filtering | 11 | 11 | 0.46× (2.2× faster) |
| Aggregation | 14 | 14 | 0.19× (5.3× faster) |
| Ordering, Window, View | 9 | 9 | 0.04× (25.0× faster) |
| Full Materialization | 3 | 3 | 0.26× (3.8× faster) |
| Joins | 5 | 5 | 0.21× (4.8× faster) |
| Set Operations | 6 | 6 | 0.22× (4.5× faster) |
| Subqueries & CTE | 4 | 4 | 0.11× (9.1× faster) |
| Expression Evaluation | 5 | 5 | 0.34× (2.9× faster) |
| File Scan | 9 | 9 | 0.34× (2.9× faster) |
| DML | 13 | 13 | 0.05× (20.0× faster) |
| Search | 1 | 1 | 0.17× (5.9× faster) |
| Table Ops | 5 | 5 | 0.51× (2.0× faster) |
| Other | 1 | 1 | 0.68× (1.5× faster) |

`Apex vs best competitor` is the ratio of total workload time to the fastest
competitor total (geometric mean for per-query comparisons); lower is better.

### Representative medians

| Metric | ApexBase | SQLite | DuckDB | ApexBase vs best |
| --- | ---: | ---: | ---: | ---: |
| `COUNT(*)` | 0.080 ms | 8.453 ms | 0.552 ms | 6.9× faster |
| Point lookup (SQL by `_id`) | 0.004 ms | 0.004 ms | 4.002 ms | 1.3× faster |
| Filter (`name = 'user_5000'`) | 0.194 ms | 46.635 ms | 1.644 ms | 8.5× faster |
| Filter (age BETWEEN 25 AND 35) | 67.899 ms | 280.567 ms | 174.607 ms | 2.6× faster |
| Multi-condition filter | 187.608 ms | 604.410 ms | 364.692 ms | 1.9× faster |
| `GROUP BY` city (10 groups) | 0.652 ms | 362.494 ms | 3.972 ms | 6.1× faster |
| `GROUP BY` + `HAVING` | 0.682 ms | 362.404 ms | 3.813 ms | 5.6× faster |
| `COUNT(DISTINCT city)` | 0.220 ms | 91.969 ms | 4.414 ms | 20.0× faster |
| Window `ROW_NUMBER` by city | 0.494 ms | 553.034 ms | 50.933 ms | 103.2× faster |
| `ORDER BY score LIMIT 100` | 1.994 ms | 55.580 ms | 5.682 ms | 2.8× faster |
| Projection full scan (3 cols) | 225.311 ms | 885.678 ms | 699.396 ms | 3.1× faster |
| `SELECT *` → pandas | 23.456 ms | 1,396.460 ms | 219.166 ms | 9.3× faster |
| LEFT JOIN count | 0.823 ms | 156.612 ms | 4.346 ms | 5.3× faster |
| Bulk insert (1M rows) | 245.573 ms | 1,001.223 ms | 165,728.633 ms | 4.1× faster |
| FTS index build (3 fields) | 1.823 ms | 1,637.500 ms | 1,229.940 ms | 674.5× faster |

All timings are medians in milliseconds from the retained baseline. Results are
materialized; ApexBase's result cache is disabled for every measurement.

### Exact vector search

1,000,000 Float32 vectors × 128 dimensions, `k=10`, compared against SQLite +
sqlite-vector and DuckDB. All six rows matched brute-force exact TopK row sets.

| Metric | ApexBase | SQLite + sqlite-vector | DuckDB |
| --- | ---: | ---: | ---: |
| TopK L2 | 5.643 ms | 144.645 ms | 36.805 ms |
| TopK Cosine | 5.750 ms | 169.105 ms | 49.544 ms |
| TopK Dot | 5.474 ms | 137.335 ms | 39.775 ms |
| Batch TopK L2 (10 queries) | 50.036 ms | 1,403.238 ms | 352.702 ms |
| Batch TopK Cosine (10 queries) | 55.469 ms | 1,683.128 ms | 441.690 ms |
| Batch TopK Dot (10 queries) | 50.293 ms | 1,361.057 ms | 398.597 ms |

### Quantized vector retrieval

The quantized module keeps the source Float32 vector as the authoritative
column, scans a compressed accelerator for candidates, and optionally rescans
the source vector for exact reranking. Twenty queries, `k=10`,
`candidate_k=100`, batch-amortized latency per query.

| Codec | Apex quantized | Apex recall | Apex exact rescore | Rescore recall | SQLite quantized | SQLite recall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| INT8 | 1.428 ms | 0.975 | 5.615 ms | 1.000 | 8.671 ms | 0.945 |
| UINT8 | 3.600 ms | 0.985 | 8.395 ms | 1.000 | 8.660 ms | 0.960 |
| 1-bit | 0.582 ms | 0.140 | 2.195 ms | 0.415 | 1.995 ms | 0.100 |
| TurboQuant 2-bit | 5.795 ms | 0.450 | 7.863 ms | 0.870 | 27.249 ms | 0.535 |
| TurboQuant 3-bit | 20.431 ms | 0.595 | 24.357 ms | 0.990 | 66.844 ms | 0.725 |
| TurboQuant 4-bit | 12.563 ms | 0.760 | 16.934 ms | 1.000 | 52.058 ms | 0.840 |
| Float16 | 2.502 ms | 1.000 | 3.288 ms | 1.000 | — | — |
| BFloat16 | 12.556 ms | 1.000 | 22.120 ms | 1.000 | — | — |

Recall is overlap with each engine's own exact Float32 top-10. ApexBase's exact
reranking recovered 1.000 recall for INT8, UINT8, TurboQuant 4-bit, Float16,
and BFloat16 on this dataset. See the
[Vector Quantization Guide](VECTOR_QUANTIZATION_GUIDE.md) for how to choose a
codec.

### 10M-row scale checks

The release-scale harness (not part of the 1M public scoreboard) records:

| Workload at 10M rows | Result |
| --- | ---: |
| `COUNT(*)` | 0.043 ms |
| Low-cardinality `GROUP BY` | 7.1 ms |
| High-cardinality `GROUP BY` (10M groups, 1 GB budget) | 2.6 s |
| Same query with a 256 MB budget | Spilled to disk, exact result |
| IVF recall@10, `nprobe=16` | 0.97 at ~11 ms median |
| `ORDER BY ... LIMIT` | 83.8 ms (DuckDB 50.5 ms) |

The last row is a known performance follow-up rather than a regression in the
release gate.

## Reproduce

Run the same default public profile:

```bash
python benchmarks/bench_vs_sqlite_duckdb.py \
  --output benchmarks/results/public.json
```

Use `--skip-vector` for a tabular-only run or `--skip-quantized-vector` to keep
exact-vector metrics while omitting the compressed module. The unified command
accepts `--rows`, `--warmup`, `--iterations`, `--vector-rows`, `--vector-dim`,
and the `--quantized-vector-*` options when a different workload is required.

`benchmarks/latest_public_baseline.json` is the maintained baseline. Rebuild it
from at least three same-machine runs so a single noisy run cannot move it:

```bash
python benchmarks/rebuild_public_baseline.py \
  --input run1.json run2.json run3.json \
  --output benchmarks/latest_public_baseline.json
```

The quantized benchmark requires the optional `sqliteai-vector` Python package
and a Python build linked against a compatible SQLite version.

## Methodology and limits

- Results are workload-sensitive. A lower row count, a different Python build,
  or a different CPU changes absolute latency; compare shapes and ratios rather
  than single numbers.
- The public suite is a cross-engine scoreboard, not ApexBase's own regression
  gate. Regressions are caught by the same-machine base/current guard in
  `benchmarks/run_local_perf_guard.py` and the canary in
  `benchmarks/bench_perf_canary.py`.
- SQLite rows use the `sqlite-vector` extension for ranked vector metrics; without
  the extension, those rows are reported as unavailable rather than estimated.
- All reports are retained as JSON, including config, system, build, and
  dependency metadata, so a result can be audited after the fact.

## History

v1.x snapshots (v1.35.0, v1.34.0, v1.30.0) remain in the
[release notes](release-notes.md) and the git history. This page is kept in sync
with `benchmarks/latest_public_baseline.json`; when the maintained baseline is
rebuilt, the snapshot section above is updated with it.
