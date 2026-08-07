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

**Install it, write local `.apex` table files, run analytical SQL, import/export DataFrames, and optionally expose the same data through PostgreSQL Wire or Arrow Flight. No separate database service is required.**

## Why ApexBase

| What you need | What ApexBase gives you |
| --- | --- |
| **Fast local analytics** | Columnar storage, vectorized execution, SQL aggregations, joins, CTEs, windows, and indexes |
| **Low-friction Python workflows** | `ApexClient`, Pandas / Polars / PyArrow conversion, file table functions, and simple local persistence |
| **One engine for mixed workloads** | HTAP design: fast writes, point lookups, analytical scans, transactions, and MVCC |
| **Search built in** | Full-text search, fuzzy matching, vector TopK search, and float16 embedding storage |
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

## Performance At A Glance

Latest local snapshot (2026-08-07): **ApexBase 1.28.0**, 1M-row tabular dataset, 1M-vector dataset, Apple arm, Python 3.12.

| Area | Snapshot |
| --- | --- |
| **Fair OLAP + OLTP comparison** | **77 public tabular metrics** tracked; ApexBase wins **75 / 77** in the benchmark harness (the 2 losses are `Table DROP` / `Table CREATE+DROP cycle` vs SQLite) |
| **GROUP BY city** | **1.9x faster** than DuckDB in the representative snapshot |
| **FTS search** | **5.5x faster** than SQLite in the representative snapshot |
| **Batch vector TopK cosine** | **6.8x faster** than DuckDB in the representative snapshot |

Benchmarks are workload-sensitive. The default benchmark command tracks this public scoreboard; extended diagnostics live in `benchmarks/bench_vs_sqlite_duckdb_extended.py`. See the full reproducible setup in the [Performance documentation](https://birchkwok.github.io/apexbase/latest/performance/).

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
| **Search text or vectors** | [Full-Text Search](https://birchkwok.github.io/apexbase/latest/FTS_GUIDE/) and [Float16 Vectors](https://birchkwok.github.io/apexbase/latest/FLOAT16_VECTOR_GUIDE/) |
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
