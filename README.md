<p>
  <img align="left" src="docs/img/apexbase-wordmark.svg" width="360" alt="ApexBase">
  <br>
  <br>
  <br>
</p>
<br clear="left">

[![PyPI](https://img.shields.io/pypi/v/apexbase.svg)](https://pypi.org/project/apexbase/)
[![Python](https://img.shields.io/pypi/pyversions/apexbase.svg)](https://pypi.org/project/apexbase/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**ApexBase is a high-performance embedded HTAP database with a Rust core and a Python-first API.**

**Install it, choose persistent `.apex` files or a true process-local in-memory database, run analytical SQL, import/export DataFrames, and optionally expose persistent data through PostgreSQL Wire or Arrow Flight. No separate database service is required.**

## Why ApexBase

| What you need | What ApexBase gives you |
| --- | --- |
| **Fast local analytics** | Columnar storage, shared scan/selection operators, vectorized execution, SQL aggregations, joins, CTEs, windows, and indexes |
| **Low-friction Python workflows** | `ApexClient`, `apexbase.execute`, Pandas / Polars / PyArrow conversion, file table functions, and local or in-memory storage |
| **One engine for mixed workloads** | HTAP design: fast writes, point lookups, analytical scans, transactions, and MVCC |
| **Search built in** | Full-text search, vector TopK, Float16/BFloat16/Int8/UInt8/1Bit/TurboQuant storage, and exact reranking from a retained source vector |
| **Tool compatibility** | PostgreSQL Wire for database clients and Arrow Flight for fast columnar transfer |

## Install

```bash
pip install apexbase
```

Build from source:

```bash
python -m pip install maturin
maturin develop --release
```

## 30-Second Example: FTS + SQL + Vector Search In One Local File

```python
from apexbase import ApexClient

with ApexClient("./rag-data") as client:
    client.execute("""
        CREATE TABLE articles (
            title TEXT,
            body TEXT,
            category TEXT,
            views INT,
            embedding FLOAT16_VECTOR
        )
    """)
    client.use_table("articles")

    client.store([
        {
            "title": "Rust-powered local analytics",
            "body": "A columnar embedded database for fast SQL and search.",
            "category": "database",
            "views": 4200,
            "embedding": [0.10, 0.82, 0.20],
        },
        {
            "title": "Hybrid retrieval for RAG",
            "body": "Combine full-text recall, SQL filters, and semantic vector ranking.",
            "category": "ai",
            "views": 6100,
            "embedding": [0.16, 0.74, 0.58],
        },
        {
            "title": "SQLite migration notes",
            "body": "Move local applications to an analytical embedded store.",
            "category": "database",
            "views": 2600,
            "embedding": [0.80, 0.12, 0.10],
        },
    ])

    client.execute("CREATE FTS INDEX ON articles(title, body)")

    # FTS recall + structured SQL guardrails + pgvector-style semantic rerank.
    df = client.execute("""
        SELECT
            title,
            category,
            views,
            cosine_distance(embedding, [0.12, 0.78, 0.25]) AS semantic_dist
        FROM articles
        WHERE MATCH('database')
          AND category = 'database'
          AND views > 3000
        ORDER BY semantic_dist
        LIMIT 5
    """).to_pandas()

    print(df)
```

**ApexBase gives you pgvector-style semantic search, SQL filters, and full-text search in the same embedded database file.** It is the kind of stack you would otherwise assemble from SQLite/DuckDB + FTS + pgvector, but without a server process or a separate search/vector service; results still convert directly to Pandas, Polars, or Arrow.

For scratch work, tests, and short-lived analytics, use the same API without
creating database, catalog, WAL, delta, or index files:

```python
from apexbase import ApexClient

with ApexClient(":memory:") as client:
    client.execute("CREATE TABLE metrics (name TEXT, value DOUBLE)")
    client.execute("INSERT INTO metrics VALUES ('latency_ms', 2.4)")
    print(client.execute("SELECT AVG(value) AS average FROM metrics").scalar())
```

For the smallest SQL-only scripts, `apexbase.execute(...)` uses one lazily
created process-local in-memory connection shared by later module-level calls.

## Performance At A Glance

The latest retained complete public snapshot uses **1,000,000 tabular rows**
and **1,000,000 vectors x 128 dimensions** on Apple arm64 with Python 3.12.

| Area | Snapshot |
| --- | --- |
| **Public coverage** | 102 tabular metrics, 6 exact-vector metrics, and 8 ApexBase quantized-vector precision rows |
| **Comparable results** | ApexBase wins 111 / 114 rows with a direct competitor in the retained complete snapshot |
| **Exact vector search** | All 6 single/batch Float32 TopK rows beat the compared engines and match brute-force exact top-k row sets |
| **Reproducibility** | Fixed data sizes, 2 warmups, 5 timed iterations, dependency metadata, and a retained JSON report |

Benchmarks are workload-sensitive. The default benchmark command tracks this public scoreboard; extended diagnostics live in `benchmarks/bench_vs_sqlite_duckdb_extended.py`. See the full reproducible setup in the [Performance documentation](https://birchkwok.github.io/apexbase/latest/performance/).

Starting with 1.33, filtered grouped queries can use a shared physical scan
protocol across persisted base data and delta/overlay state. The protocol
keeps predicate selection, grouping, `HAVING`, and ordered TopK as composable
operators with a generic fallback for unsupported or inexact predicate forms.
See [Scan & Physical Execution](https://birchkwok.github.io/apexbase/latest/SCAN_EXECUTION_ARCHITECTURE/).

## Documentation

**Start here:** <https://birchkwok.github.io/apexbase/>

| Goal | Page |
| --- | --- |
| **Get running quickly** | [Installation](https://birchkwok.github.io/apexbase/latest/installation/) and [Quick Start](https://birchkwok.github.io/apexbase/latest/QUICK_START/) |
| **Understand the model** | [Core Concepts](https://birchkwok.github.io/apexbase/latest/concepts/) |
| **Use the Python API** | [Python Client Guide](https://birchkwok.github.io/apexbase/latest/user-guide/python-client/) and [API Reference](https://birchkwok.github.io/apexbase/latest/API_REFERENCE/) |
| **Write SQL** | [SQL Guide](https://birchkwok.github.io/apexbase/latest/user-guide/sql/) |
| **Import files and DataFrames** | [Data Import](https://birchkwok.github.io/apexbase/latest/user-guide/data-import/) |
| **Use database tools or Arrow clients** | [Server Protocols](https://birchkwok.github.io/apexbase/latest/user-guide/server-protocols/) |
| **Search text or vectors** | [Full-Text Search](https://birchkwok.github.io/apexbase/latest/FTS_GUIDE/), [Float16 Vectors](https://birchkwok.github.io/apexbase/latest/FLOAT16_VECTOR_GUIDE/), and [Vector Quantization](https://birchkwok.github.io/apexbase/latest/VECTOR_QUANTIZATION_GUIDE/) |
| **Embed from Rust** | [Rust Embedded API](https://birchkwok.github.io/apexbase/latest/RUST_EMBEDDED_API/) |

## Interfaces

```bash
# Embedded Python
python -c "from apexbase import ApexClient; print(ApexClient)"

# PostgreSQL Wire + Arrow Flight together
apexbase-serve --dir ./data

# Individual protocol servers
apexbase-server --dir ./data --port 5432
apexbase-flight --dir ./data --port 50051
```

## Lance Interop

```python
from apexbase import ApexClient

with ApexClient("./data") as client:
    client.use_table("articles")
    client.to_lance("./articles.lance")

with ApexClient("./imported") as client:
    client.from_lance("./articles.lance", table_name="articles")
```

Lance conversion uses Arrow tables as the handoff path. This keeps the in-process conversion lean and Arrow-native, while each format still writes its own on-disk layout.

## License

Apache-2.0
