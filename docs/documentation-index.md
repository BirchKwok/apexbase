# ApexBase Documentation Index

This directory is the source for the ApexBase documentation site. The site is
built with MkDocs Material from the repository root:

```bash
python -m pip install -r docs/requirements.txt
python -m mkdocs serve
```

## Recommended reading order

| Step | Page | What you get |
| --- | --- | --- |
| 1 | [Home](index.md) | Product overview and navigation by audience |
| 2 | [What's New in 2.0](whats-new-2.0.md) | V5 GA features, compatibility rules, migration, known limits |
| 3 | [Installation](installation.md) | Package install, source build, local docs |
| 4 | [Quickstart](QUICK_START.md) | Agent memory, analytics SQL, in-memory mode, durability |
| 5 | [Core Concepts](concepts.md) | Databases, tables, schemas, types, durability, execution model |
| 6 | [Agent Memory & RAG](user-guide/agent-memory.md) | Tool-calling memory store with hybrid retrieval |
| 7 | [Python Client](user-guide/python-client.md) | Application-oriented Python workflows |
| 8 | [SQL Guide](user-guide/sql.md) | DDL, DML, analytics, transactions, table functions, search |
| 9 | [Data Import](user-guide/data-import.md) | Records, batches, DataFrames, files, temporary tables |
| 10 | [Server Protocols](user-guide/server-protocols.md) | PostgreSQL Wire and Arrow Flight |

## Reference and deep dives

| Page | Audience |
| --- | --- |
| [Python API Reference](API_REFERENCE.md) | Python users who need exact method signatures |
| [Rust Embedded API](RUST_EMBEDDED_API.md) | Rust users embedding ApexBase directly |
| [Usage Examples](EXAMPLES.md) | Task-oriented snippets for every major workflow |
| [Performance](performance.md) | Reproducible SQLite/DuckDB/vector benchmark snapshot |
| [Full-Text Search Guide](FTS_GUIDE.md) | BM25 search, `MATCH()`, fuzzy search, hybrid retrieval |
| [Float16 Vector Guide](FLOAT16_VECTOR_GUIDE.md) | Storing and querying embeddings in half precision |
| [Vector Quantization Guide](VECTOR_QUANTIZATION_GUIDE.md) | Compressed candidates with exact reranking |
| [Storage Architecture](STORAGE_ARCHITECTURE.md) | Contributors and maintainers |
| [Scan & Physical Execution](SCAN_EXECUTION_ARCHITECTURE.md) | Contributors extending filters, grouping, ordering, or scan scheduling |
| [Read-Path Capabilities](READ_PATH_CAPABILITIES.md) | Contributors touching read visibility, merged reads, or scan fallbacks |
| [Resource Ownership](RESOURCE_OWNERSHIP.md) | Contributors changing caches, session state, or memory limits |
| [Fused Filter + GROUP BY Design](FUSED_GROUP_AGG_DESIGN.md) | Contributors working on the filtered-grouped execution kernel |
| [Engineering Guidelines](ENGINEERING_GUIDELINES.md) | Contributors changing query or storage paths |
| [Query Optimizer Roadmap](QUERY_OPTIMIZER_ROADMAP.md) | Contributors tracking planner work |
| [HTAP Roadmap](HTAP_ROADMAP.md) | High-level project direction |

## Server quick reference

| CLI | Default | Description |
| --- | --- | --- |
| `apexbase-serve` | `pg=5432`, `flight=50051` | Start both servers simultaneously |
| `apexbase-server` | `5432` | PostgreSQL Wire only |
| `apexbase-flight` | `50051` | Arrow Flight gRPC only |

## Documentation deployment

Build and validate the site locally before opening a pull request:

```bash
python -m pip install -r docs/requirements.txt
python -m mkdocs build --strict
python -m mkdocs serve
```

GitHub Pages deployment is handled by `.github/workflows/docs.yml`.

- Pull requests run `python -m mkdocs build --strict`; the strict flag fails the
  build on missing link targets and broken navigation entries.
- Manual `workflow_dispatch` runs deploy the current package version with the
  `latest` alias.
- `v*` tags deploy the tag version and keep historical documentation available.
- The site URL is configured as `https://birchkwok.github.io/apexbase/`.

In GitHub repository settings, use **Pages → Build and deployment → Source →
Deploy from a branch**, with branch **`gh-pages`** and folder **`/(root)`**.
