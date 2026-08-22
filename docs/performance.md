# Performance

This page records the latest complete public cross-engine benchmark. It is a
reproducible snapshot, not a universal claim: rerun the suite on your own
hardware and workload.

## v1.30.0 Public Snapshot

- **Date / source**: 2026-08-21, `d8b27a4` plus the v1.30.0 release workspace
- **System**: macOS 26.6.2, Apple arm64 (10 cores), 32 GB RAM
- **Tabular stack**: Python 3.12.2, ApexBase 1.30.0, SQLite 3.46.0, DuckDB 1.1.3, PyArrow 23.0.1
- **Vector supplement stack**: Python 3.13.3, ApexBase 1.30.0, SQLite 3.53.3 + sqlite-vector 1.0.0 (NEON), DuckDB 1.1.3, PyArrow 23.0.1
- **Tabular dataset**: 1,000,000 rows x 5 columns
- **Vector dataset**: 1,000,000 Float32 vectors x 128 dimensions, `k=10`, 10 batch queries
- **Method**: 2 warmup iterations + 5 timed iterations, materialized results
- **Reports**: `benchmarks/results/v1.30.0-vector-quantization-public-final.json`, `benchmarks/results/v1.30.0-sqlite-vector-exact.json`, and `benchmarks/results/v1.30.0-sqlite-vector-quantization.json`

The public suite completed all **108/108 metrics**. ApexBase won **106/108**:
all 70 OLAP metrics, 30 of 32 OLTP metrics, and all 6 vector metrics. The only
two slower results were `Table DROP` and `Table CREATE+DROP cycle` against
SQLite's minimal empty-table DDL path; ApexBase remained faster than DuckDB on
both.

| Scope | Metrics | Apex wins | Ties | Slower |
| --- | ---: | ---: | ---: | ---: |
| OLAP fair | 70 | 70 | 0 | 0 |
| OLTP fair | 32 | 30 | 0 | 2 |
| Exact vector similarity | 6 | 6 | 0 | 0 |
| **Total** | **108** | **106** | **0** | **2** |

The six public vector rows use exact Float32 scans. SQLite is represented by
the [sqlite-vector](https://github.com/sqliteai/sqlite-vector) extension's
`vector_full_scan()` rather than by SQLite core, which has no native vector
distance operator. Quantized scans remain a separate diagnostic because their
recall and storage contracts differ from exact Float32 search.

## All 108 Metrics

### OLAP Fair Metrics (70)

| Metric | ApexBase | SQLite | DuckDB |
| --- | ---: | ---: | ---: |
| COUNT(*) | 0.075 ms | 8.035 ms | 0.519 ms |
| SELECT * LIMIT 100 (cold reopen) | 0.110 ms | 0.140 ms | 0.602 ms |
| SELECT * LIMIT 100 (warm cache) | 0.076 ms | 0.121 ms | 0.232 ms |
| SELECT * LIMIT 10K (cold reopen) | 4.461 ms | 13.626 ms | 8.879 ms |
| SELECT * LIMIT 10K (warm cache) | 4.303 ms | 12.919 ms | 8.590 ms |
| Projection full scan (3 cols) | 238.617 ms | 922.001 ms | 698.914 ms |
| Filtered LIMIT 100 (age>30) | 0.017 ms | 0.141 ms | 0.290 ms |
| LIMIT 100 OFFSET 10K | 0.049 ms | 0.240 ms | 0.301 ms |
| Filter (name = 'user_5000') | 0.162 ms | 46.726 ms | 1.727 ms |
| Filter (age BETWEEN 25 AND 35) | 70.793 ms | 288.977 ms | 165.045 ms |
| GROUP BY city (10 groups) | 1.507 ms | 364.261 ms | 4.032 ms |
| GROUP BY category (10 groups) | 1.511 ms | 365.684 ms | 3.358 ms |
| GROUP BY city ORDER BY count | 0.969 ms | 279.311 ms | 3.416 ms |
| GROUP BY category ORDER BY count | 1.010 ms | 281.478 ms | 3.149 ms |
| GROUP BY + HAVING | 1.558 ms | 360.756 ms | 3.857 ms |
| GROUP BY category + HAVING | 1.556 ms | 356.575 ms | 3.596 ms |
| Persistent VIEW select | 0.910 ms | 363.288 ms | 2.582 ms |
| ORDER BY score LIMIT 100 | 2.260 ms | 55.744 ms | 5.601 ms |
| ORDER BY score ASC LIMIT 100 | 1.981 ms | 55.992 ms | 4.788 ms |
| Aggregation (5 funcs) | 0.235 ms | 88.328 ms | 1.219 ms |
| Filtered aggregation (category) | 0.434 ms | 61.952 ms | 1.174 ms |
| Filtered aggregation (city) | 0.425 ms | 60.457 ms | 1.123 ms |
| COUNT WHERE category | 0.247 ms | 57.858 ms | 0.841 ms |
| Complex (Filter+Group+Order) | 1.544 ms | 167.413 ms | 3.037 ms |
| SELECT * -> pandas (full scan) | 27.275 ms | 1.43 s | 217.250 ms |
| GROUP BY city,category (100 grp) | 1.164 ms | 681.439 ms | 5.467 ms |
| LIKE filter (name LIKE user_1%) | 43.767 ms | 210.222 ms | 101.863 ms |
| Multi-cond (age>30 AND score>50) | 195.705 ms | 619.532 ms | 373.821 ms |
| ORDER BY city,score DESC LIMIT100 | 3.412 ms | 76.123 ms | 7.217 ms |
| COUNT(DISTINCT city) | 0.195 ms | 92.387 ms | 4.259 ms |
| COUNT(DISTINCT category) | 0.192 ms | 94.236 ms | 4.832 ms |
| IN filter (city IN 3 cities) | 125.114 ms | 514.088 ms | 285.915 ms |
| Numeric IN (age IN 9 values) | 61.760 ms | 280.557 ms | 144.735 ms |
| OR cross-col (age=25 OR city=BJ) | 71.940 ms | 226.453 ms | 112.400 ms |
| Numeric OR (age=20\|30\|40\|50) | 31.485 ms | 150.690 ms | 67.100 ms |
| Window ROW_NUMBER PARTITION BY city | 0.742 ms | 518.455 ms | 46.636 ms |
| JOIN GROUP BY ORDER LIMIT | 5.160 ms | 392.784 ms | 7.876 ms |
| LEFT JOIN COUNT | 0.806 ms | 161.447 ms | 3.966 ms |
| LEFT JOIN extra ON predicate | 0.812 ms | 145.464 ms | 3.527 ms |
| FULL OUTER JOIN (bounded) | 0.728 ms | 1.246 ms | 1.771 ms |
| CROSS JOIN COUNT | 0.598 ms | 48.583 ms | 1.596 ms |
| UNION ALL (ordered) | 1.468 ms | 81.119 ms | 3.709 ms |
| UNION DISTINCT (ordered) | 2.644 ms | 84.339 ms | 3.140 ms |
| INTERSECT (ordered) | 7.846 ms | 177.454 ms | 8.191 ms |
| EXCEPT (ordered) | 2.882 ms | 175.378 ms | 7.385 ms |
| IN subquery COUNT | 0.673 ms | 101.201 ms | 3.414 ms |
| EXISTS subquery COUNT | 5.185 ms | 399.828 ms | 23.765 ms |
| Derived table GROUP BY | 1.118 ms | 279.524 ms | 4.106 ms |
| CTE with AVG filter | 1.419 ms | 364.752 ms | 4.786 ms |
| CASE aggregate GROUP BY | 1.360 ms | 315.547 ms | 5.727 ms |
| String functions (UPPER/LENGTH/SUBSTR/CONCAT/TRIM) | 0.935 ms | 1.919 ms | 1.663 ms |
| Numeric functions (ROUND/ABS/FLOOR/CEIL/MOD) | 0.736 ms | 1.344 ms | 1.273 ms |
| COALESCE/NULLIF filter | 0.381 ms | 137.111 ms | 2.116 ms |
| NOT filter (age NOT BETWEEN, name NOT LIKE) | 2.062 ms | 63.308 ms | 3.607 ms |
| Deep offset (LIMIT 100 OFFSET 100K) | 47.073 ms | 255.639 ms | 198.049 ms |
| ORDER BY expression (LENGTH) | 2.230 ms | 70.825 ms | 5.375 ms |
| Window SUM OVER (running) | 0.575 ms | 452.769 ms | 42.017 ms |
| Window RANK (partitioned) | 0.561 ms | 476.528 ms | 43.014 ms |
| Window LAG (partitioned) | 0.574 ms | 441.836 ms | 35.582 ms |
| DISTINCT (city, category) | 4.235 ms | 635.642 ms | 5.785 ms |
| GROUP BY 2 cols + HAVING | 1.145 ms | 481.822 ms | 5.116 ms |
| CSV Read + COUNT(*) | 13.659 ms | N/A | 46.370 ms |
| CSV Read + Filter + GROUP BY | 27.802 ms | N/A | 51.477 ms |
| CSV Read + Full Scan LIMIT 1000 | 14.786 ms | N/A | 22.633 ms |
| JSON Read + COUNT(*) | 4.652 ms | N/A | 66.710 ms |
| JSON Read + Filter | 8.690 ms | N/A | 82.001 ms |
| JSON Read + GROUP BY category | 56.599 ms | N/A | 89.853 ms |
| Temp Table (CSV) Query (filter+agg) | 0.464 ms | N/A | 0.719 ms |
| JSON Read + ORDER BY LIMIT 100 | 48.435 ms | N/A | 103.954 ms |
| CSV Read + ORDER BY LIMIT 100 | 18.192 ms | N/A | 57.492 ms |

### OLTP Fair Metrics (32)

| Metric | ApexBase | SQLite | DuckDB |
| --- | ---: | ---: | ---: |
| Bulk Insert (N rows; default fair) | 238.079 ms | 1.04 s | 185.68 s |
| Point Lookup (SQL by ID) | 2.71 us | 4.31 us | 2.847 ms |
| Retrieve Many (SQL, 100 IDs) | 0.197 ms | 0.335 ms | 4.835 ms |
| COUNT(*) (direct API) | 6.19 us | 7.895 ms | 0.304 ms |
| Point lookup (projected SQL) | 2.44 us | 3.32 us | 2.523 ms |
| Point lookup (direct full row) | 2.05 us | 4.29 us | 2.613 ms |
| Missing ID lookup | 2.16 us | 2.63 us | 2.918 ms |
| Retrieve 10 IDs (projected SQL) | 7.88 us | 0.015 ms | 3.759 ms |
| Retrieve 100 IDs (projected SQL) | 0.036 ms | 0.107 ms | 5.151 ms |
| SELECT 3 cols LIMIT 100 | 0.045 ms | 0.078 ms | 0.176 ms |
| String equality (projected) | 0.049 ms | 47.056 ms | 1.462 ms |
| City filter LIMIT 100 | 0.020 ms | 0.109 ms | 0.216 ms |
| Insert 1 row (default fair) | 0.011 ms | 0.015 ms | 0.319 ms |
| Insert+Read own row | 0.014 ms | 0.019 ms | 4.150 ms |
| Insert+COUNT visible | 0.017 ms | 8.230 ms | 0.678 ms |
| UPDATE by ID | 3.04 us | 4.34 us | 0.891 ms |
| UPDATE missing ID | 3.01 us | 3.90 us | 0.907 ms |
| UPDATE+Read by ID | 5.11 us | 7.32 us | 1.852 ms |
| Replace row by ID | 0.62 us | 4.44 us | 1.007 ms |
| Insert+DELETE by ID | 0.023 ms | 0.027 ms | 1.385 ms |
| DELETE missing ID | 0.41 us | 3.97 us | 0.684 ms |
| Insert 1K rows (default fair) | 0.682 ms | 1.702 ms | 185.861 ms |
| UPDATE rows (age=25; idempotent) | 4.144 ms | 43.291 ms | 15.605 ms |
| Store+DELETE 1K (combined) | 1.076 ms | 40.499 ms | 184.709 ms |
| DELETE 1K (pure delete; setup rows) | 0.187 ms | 39.612 ms | 0.385 ms |
| FTS Index Build (name,city,category) | 1.546 ms | 1.57 s | 1.15 s |
| FTS Search ('Electronics') | 5.437 ms | 30.056 ms | 23.242 ms |
| Table CREATE (1 col) | 0.016 ms | 0.040 ms | 0.116 ms |
| Table DROP | 0.056 ms | 0.031 ms | 0.098 ms |
| Table CREATE+DROP cycle | 0.110 ms | 0.068 ms | 0.276 ms |
| List tables (10) | 0.011 ms | 0.020 ms | 2.846 ms |
| ALTER TABLE ADD COLUMN | 0.067 ms | 0.082 ms | 0.107 ms |

### Exact Vector Similarity (6)

| Metric | ApexBase | SQLite + sqlite-vector | DuckDB |
| --- | ---: | ---: | ---: |
| TopK L2 | 7.951 ms | 123.397 ms | 31.644 ms |
| TopK Cosine | 7.843 ms | 131.655 ms | 37.821 ms |
| TopK Dot | 8.420 ms | 120.771 ms | 32.322 ms |
| Batch TopK L2 (10 queries) | 47.107 ms | 1,225.980 ms | 320.179 ms |
| Batch TopK Cosine (10 queries) | 51.042 ms | 1,317.794 ms | 377.865 ms |
| Batch TopK Dot (10 queries) | 47.494 ms | 1,203.704 ms | 316.817 ms |

All vector rows matched the brute-force exact top-k row sets.

## Quantized L2 Distance Snapshot

This separate snapshot uses 100,000 identical normally distributed Float32
vectors with 128 dimensions, 20 query vectors, `k=10`, two warmups, and five
timed iterations. Latency is the median per query. Recall is overlap with each
engine's own exact Float32 top-10. sqlite-vector quantized data was preloaded,
and ApexBase exact reranking used `candidate_k=100`.

The common comparison covers the six modes supported by both engines. The
codec names describe each engine's implementation, not a shared binary format.

| Codec | ApexBase quantized | Apex recall | Apex exact-rescore | Rescore recall | sqlite-vector quantized | SQLite recall | SQLite preload |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| INT8 | 7.992 ms | 0.995 | 9.239 ms | 1.000 | 0.936 ms | 0.985 | 12.97 MiB |
| UINT8 | 8.708 ms | 0.995 | 9.886 ms | 1.000 | 0.927 ms | 0.975 | 12.97 MiB |
| 1-bit | 15.473 ms | 0.250 | 16.839 ms | 0.670 | 0.206 ms | 0.145 | 2.29 MiB |
| TurboQuant 2-bit | 89.180 ms | 0.440 | 90.461 ms | 0.935 | 2.802 ms | 0.540 | 4.20 MiB |
| TurboQuant 3-bit | 95.410 ms | 0.650 | 96.949 ms | 1.000 | 7.357 ms | 0.745 | 5.72 MiB |
| TurboQuant 4-bit | 101.204 ms | 0.775 | 102.594 ms | 1.000 | 5.355 ms | 0.860 | 7.25 MiB |

sqlite-vector does not expose Float16 or BFloat16 through
`vector_quantize_scan()`. ApexBase's additional derived-column results were:

| ApexBase-only codec | Quantized L2 | Recall@10 | Exact-rescore | Rescore recall |
| --- | ---: | ---: | ---: | ---: |
| Float16 | 0.826 ms | 1.000 | 2.204 ms | 1.000 |
| BFloat16 | 8.113 ms | 1.000 | 9.492 ms | 1.000 |

For context, the exact Float32 scans in this 100K snapshot were approximately
1.05-1.33 ms/query for ApexBase and 22.11-22.40 ms/query for sqlite-vector.
Compression therefore did not imply a latency win on every ApexBase codec in
this workload. The JSON report also retains build time, database size, and
sqlite-vector's estimated preloaded representation size for every mode.

## Reproduce

Run the same default public profile:

```bash
python benchmarks/bench_vs_sqlite_duckdb.py \
  --output benchmarks/results/public.json
```

Use `--skip-vector` for a tabular-only run. Use
`benchmarks/bench_vector_quantization.py` to measure compressed storage,
candidate recall, exact rescore, and sqlite-vector quantized scans separately
from this public exact-scan scoreboard:

```bash
python benchmarks/bench_vector_quantization.py \
  --rows 100000 --dim 128 --queries 20 \
  --warmup 2 --iterations 5 \
  --output benchmarks/results/quantization.json
```

The quantization benchmark requires the optional `sqliteai-vector` Python
package and a Python build linked against a compatible SQLite version.
