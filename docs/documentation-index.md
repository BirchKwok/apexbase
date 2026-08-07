# ApexBase Documentation Index

This directory is the source for the ApexBase documentation site. The site is built with MkDocs Material from the repository root:

```bash
python -m pip install -r docs/requirements.txt
python -m mkdocs serve
```

## Recommended Reading Order

| Step | Page | What you get |
| --- | --- | --- |
| 1 | [Home](index.md) | Project overview and navigation by audience |
| 2 | [Installation](installation.md) | Package install, source build, local docs, GitHub Pages deployment |
| 3 | [Quick Start](QUICK_START.md) | First database, table, inserts, SQL, and DataFrame conversion |
| 4 | [Core Concepts](concepts.md) | Databases, tables, schemas, results, durability, and interfaces |
| 5 | [Python Client Guide](user-guide/python-client.md) | Application-oriented Python workflows |
| 6 | [SQL Guide](user-guide/sql.md) | DDL, DML, analytics, transactions, table functions, search |
| 7 | [Data Import](user-guide/data-import.md) | Records, batches, DataFrames, files, and temporary tables |
| 8 | [Server Protocols](user-guide/server-protocols.md) | PostgreSQL Wire and Arrow Flight |

## Reference And Deep Dives

| Page | Audience |
| --- | --- |
| [Python API Reference](API_REFERENCE.md) | Python users who need exact method signatures |
| [Rust Embedded API](RUST_EMBEDDED_API.md) | Rust users embedding ApexBase directly |
| [Performance](performance.md) | Users comparing ApexBase against SQLite, DuckDB, and vector workloads |
| [Usage Examples](EXAMPLES.md) | Users looking for task-oriented snippets |
| [Full-Text Search Guide](FTS_GUIDE.md) | Users building text search workflows |
| [Float16 Vector Guide](FLOAT16_VECTOR_GUIDE.md) | Users storing and querying embeddings |
| [Storage Architecture](STORAGE_ARCHITECTURE.md) | Contributors and maintainers |
| [Engineering Guidelines](ENGINEERING_GUIDELINES.md) | Contributors changing query or storage paths |
| [HTAP Roadmap](HTAP_ROADMAP.md) | High-level project direction |

## Server Quick Reference

| CLI | Default | Description |
| --- | --- | --- |
| `apexbase-serve` | `pg=5432`, `flight=50051` | Start both servers simultaneously |
| `apexbase-server` | `5432` | PostgreSQL Wire only |
| `apexbase-flight` | `50051` | Arrow Flight gRPC only |

## Documentation Deployment

GitHub Pages deployment is handled by `.github/workflows/docs.yml`.

- Pull requests run `python -m mkdocs build --strict`.
- Manual `workflow_dispatch` runs deploy the current package version with the `latest` alias.
- `v*` tags deploy the tag version and keep historical documentation available.
- The site URL is configured as `https://birchkwok.github.io/ApexBase/`.

In GitHub repository settings, use **Pages -> Build and deployment -> Source -> Deploy from a branch**, with branch **`gh-pages`** and folder **`/(root)`**.
