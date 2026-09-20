# HTAP Roadmap

This roadmap is a reader-friendly snapshot of where ApexBase is today and what the project is optimizing for next. It is intentionally high level; implementation details live in issues, tests, and engineering documents.

## Current Focus

ApexBase is focused on being a fast embedded HTAP engine with:

- Single-file table storage.
- Rust execution core and Python-first ergonomics.
- Analytical SQL over local data.
- Efficient DataFrame and Arrow interoperability.
- PostgreSQL Wire and Arrow Flight access for external tools.
- Full-text search and vector search as built-in capabilities.

## Implemented Foundations

| Area | Status |
| --- | --- |
| Columnar storage | V4 row-group storage with `.apex` table files |
| Python API | `ApexClient`, `ResultView`, DataFrame import/export |
| SQL | DDL, DML, joins, CTEs, subqueries, windows, set operations |
| Multi-database | Named databases and `database.table` SQL references |
| Transactions | BEGIN / COMMIT / ROLLBACK, savepoints, optimistic concurrency, and a commit-outcome contract (`not_committed` / `unknown` / `committed`) with WAL recovery for INSERT, UPDATE, and DELETE |
| Indexing | B-Tree and Hash indexes, stale-posting detection, and `REINDEX` recovery |
| Full-text search | SQL-native FTS index management and `MATCH()` predicates |
| Vector search | TopK distance APIs, SQL integration, and stored quantized accelerators with exact rescore |
| Protocols | PostgreSQL Wire and Arrow Flight servers, including streaming Flight results with backpressure |
| Query execution | Shared scan protocol, serial and parallel row-group pipeline, per-query aggregation memory budget |
| Rust embedding | Native Rust API for embedded use |

## Near-Term Documentation Goals

- Keep the first-run path short and accurate.
- Split user-facing concepts from API reference material.
- Add focused guides for SQL, import paths, and protocol deployment.
- Keep contributor notes separate from application-user documentation.
- Build and publish docs automatically through GitHub Pages.

## Product Direction

| Theme | Direction |
| --- | --- |
| Embedded reliability | Keep local persistence predictable and easy to reason about |
| Query performance | Preserve fast paths for common analytical and point lookup patterns |
| Interoperability | Make Arrow, Pandas, Polars, PostgreSQL clients, and Rust all feel native |
| Operational simplicity | Keep the default mode serverless while offering protocol servers when needed |
| Specialized search | Continue integrating structured SQL with text and vector search |

## Contributor Entry Points

- [Storage Architecture](STORAGE_ARCHITECTURE.md) explains the storage stack.
- [Engineering Guidelines](ENGINEERING_GUIDELINES.md) records query-dispatch and fast-path rules.
- [Rust Embedded API](RUST_EMBEDDED_API.md) documents native integration points.
- Tests under `test/` are the most concrete source of supported behavior.
