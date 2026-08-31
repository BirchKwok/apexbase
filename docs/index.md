# ApexBase

**ApexBase is a high-performance HTAP embedded database with a Rust core and a Python API.**

Use it when you want a local database that can ingest records quickly, run analytical SQL, interoperate with DataFrames, and expose the same data through embedded Python, PostgreSQL Wire, or Arrow Flight.

```bash
pip install apexbase
```

Need an ephemeral database instead? `ApexClient(":memory:")` runs the same
table, SQL, DDL, DML, index, and full-text paths without creating database
files. For tiny SQL-only workflows, call the process-local
`apexbase.execute(...)` convenience API.

```python
from apexbase import ApexClient

with ApexClient("./data") as client:
    client.create_table("users")
    client.store([
        {"name": "Alice", "age": 30, "city": "Beijing"},
        {"name": "Bob", "age": 25, "city": "Shanghai"},
    ])

    df = client.execute("""
        SELECT city, COUNT(*) AS users
        FROM users
        GROUP BY city
        ORDER BY users DESC
    """).to_pandas()
```

## Start Here

<div class="grid cards" markdown>

- **Install ApexBase**

    Set up the Python package, build from source, or run the docs locally.

    [Installation](installation.md)

- **Run your first query**

    Create a table, insert rows, run SQL, and convert results to a DataFrame.

    [Quick Start](QUICK_START.md)

- **Understand the model**

    Learn how directories, databases, tables, schemas, durability, and results fit together.

    [Core Concepts](concepts.md)

- **Pick an interface**

    Use direct Python for embedded apps, PostgreSQL Wire for tools, or Arrow Flight for columnar transfer.

    [Server Protocols](user-guide/server-protocols.md)

</div>

## Documentation Layers

| Layer | Best for | Pages |
| --- | --- | --- |
| Getting started | First-time setup and mental model | [Installation](installation.md), [Quick Start](QUICK_START.md), [Core Concepts](concepts.md) |
| User guide | Building an application with ApexBase | [Python Client](user-guide/python-client.md), [SQL Guide](user-guide/sql.md), [Data Import](user-guide/data-import.md) |
| Reference | Exact API and type details | [Python API](API_REFERENCE.md), [Rust Embedded API](RUST_EMBEDDED_API.md) |
| Performance | Reproducible benchmark snapshots | [Performance](performance.md) |
| Query execution internals | Shared scan, selection, delta visibility, and physical operators | [Scan & Physical Execution](SCAN_EXECUTION_ARCHITECTURE.md) |
| Feature guides | Deep dives into specialized capabilities | [Full-Text Search](FTS_GUIDE.md), [Float16 Vectors](FLOAT16_VECTOR_GUIDE.md), [Vector Quantization](VECTOR_QUANTIZATION_GUIDE.md) |
| Internals | Contributors and maintainers | [Storage Architecture](STORAGE_ARCHITECTURE.md), [Scan & Physical Execution](SCAN_EXECUTION_ARCHITECTURE.md), [Engineering Guidelines](ENGINEERING_GUIDELINES.md), [HTAP Roadmap](HTAP_ROADMAP.md) |

## What ApexBase Is Good At

- Embedded HTAP workloads where a single local database must handle writes and analytical reads.
- Python data workflows that need Pandas, Polars, and PyArrow interoperability.
- SQL-first local analytics without running a separate database server.
- Vector search and full-text search in the same storage engine as structured data.
- Tool integration through PostgreSQL-compatible clients and Arrow Flight consumers.

## Interface Summary

| Interface | Use when | Entry point |
| --- | --- | --- |
| Python API | You are embedding persistent or in-memory ApexBase in a Python app or notebook | `ApexClient(path)` or `ApexClient(":memory:")` |
| Default SQL API | You want a shared process-local connection with no setup | `from apexbase import execute` |
| Rust API | You want direct Rust integration without Python | `apexbase::embedded::ApexDB` |
| PostgreSQL Wire | You want DBeaver, DataGrip, psql, BI tools, or libpq clients | `apexbase-server` or `apexbase-serve` |
| Arrow Flight | You need fast columnar result streaming | `apexbase-flight` or `apexbase-serve` |

## Local Documentation

```bash
python -m pip install -r docs/requirements.txt
python -m mkdocs serve
```

The GitHub Pages workflow builds this same MkDocs site on pull requests and publishes versioned documentation with `mike`. The version selector in the header can switch back to older docs after multiple versions have been deployed.
