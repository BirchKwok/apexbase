# Core Concepts

This page gives the mental model for ApexBase. Snippets assume
`from apexbase import ApexClient`.

!!! info "What's new in v2.0"
    v2.0.0 makes V5 GA: format 5, logical types, bounded execution, and
    unified routing. See [What's New in 2.0](whats-new-2.0.md).

## Database Root

An `ApexClient` opens a root directory that holds the default database and any
named databases. Each table is stored as a `.apex` file.

```python
from apexbase import ApexClient

with ApexClient("./data") as client:
    client.create_table("users")
    client.store({"name": "Alice"})
```

## In-Memory Mode

Pass `":memory:"` for a true process-local backend, not a temporary directory:

```python
with ApexClient(":memory:") as client:
    client.create_table("scratch")
    client.store({"value": 42})
    print(client.execute("SELECT value FROM scratch").scalar())
```

Tables use the normal SQL, DDL, DML, index, FTS, and result paths, but no
database, catalog, WAL, delta, or index files are created; contents disappear
when the client closes and are not shared.

## Databases

The default database maps to the root directory; named databases live in
subdirectories. Select one with `client.use_database("analytics")`, list them
with `client.list_databases()`, and reference another database in SQL as
`database.table`.

## Tables

Tables are explicit: create or select one before calling table-scoped methods.
Every row has an internal `_id` hidden unless you request it explicitly or pass
`show_internal_id=True`.

```python
with ApexClient(":memory:") as client:
    client.create_table("users")
    client.use_table("users")
```

## Schemas

ApexBase can infer column types from the first write, or you can declare a
schema for a clear contract and faster bulk loading:

```python
with ApexClient(":memory:") as client:
    client.create_table("orders", schema={
        "order_id": "int64", "customer": "string",
        "total": "float64", "paid": "bool",
    })
```

Declaring the schema is required for logical types and API-written time values.

## Logical Types (V5)

Format-5 tables can declare logical types. The type is recorded in the catalog,
and `describe_table()` reports the parameterized form.

| Family | Declared as | Python values |
| --- | --- | --- |
| UUID / Decimal | `uuid`, `decimal(p,s)` | 16-byte `bytes`; decimal also accepts `Decimal`/`int`/`str` on write |
| Time | `timestamp`, `timestamptz`, `date`, `time`, `interval` | `datetime`, `date`, `time`, `timedelta` |
| List / Extension | `list<element>`, `extension<codec>` | Columnar lists; codec-encoded values |

SQL string literals are the portable way to write time-family values:

```python
with ApexClient("./data") as client:
    client.execute("""
        CREATE TABLE events (id BIGINT, label TEXT, created TIMESTAMP, day DATE)
    """)
    client.use_table("events")
    client.execute("""
        INSERT INTO events
        VALUES (1, 'deploy', '2026-11-03 09:30:00', '2026-11-03')
    """)
    print(client.describe_table("events"))
    print(client.execute("SELECT * FROM events").to_dict())
```

Time, list, and extension values decode to native Python values; `to_arrow()`
and `to_polars()` keep typed columns, and `APEX_LOGICAL_TYPES=0` refuses new
typed columns.

!!! note "Typed writes"
    API `store()` accepts Python `datetime`/`date` objects on filesystem-backed
    tables after the column type is declared. On `:memory:`, write time values
    through SQL literals; the SQL form runs in both modes.

## Records And Columns

ApexBase accepts row-oriented dictionaries, lists of dictionaries, and
columnar dictionaries; columnar data is usually fastest for bulk ingest.

```python
with ApexClient(":memory:") as client:
    client.create_table("people", schema={"name": "string", "age": "int64"})
    client.store({"name": "Alice", "age": 30})
    client.store([{"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}])
    client.store({"name": ["Diana", "Eve"], "age": [28, 41]})
    print(client.execute("SELECT COUNT(*) AS rows FROM people").scalar())
```

## ResultView

Queries return a `ResultView` that converts to native rows or columnar formats:

```python
with ApexClient(":memory:") as client:
    client.create_table("users")
    client.store({"name": "Alice"})
    result = client.execute("SELECT * FROM users")
    rows = result.to_dict()
    pandas_df = result.to_pandas()
    polars_df = result.to_polars()
    arrow_table = result.to_arrow()
```

`execute()` materializes its result; for large outputs use
`client.execute_stream()` (see [Bounded Execution](#bounded-execution)).

## Query Execution And Visibility

SQL semantics do not depend on whether rows live in the persisted base file or
in delta/overlay state. Supported conjunctive filters become a shared physical
scan request; the backend uses selective mmap access or an authoritative merged
read, then applies grouping, `HAVING`, ordering, and limits in SQL order.
Inexact forms fall back to the general evaluator, never an approximate result.
See [Scan & Physical Execution](SCAN_EXECUTION_ARCHITECTURE.md).

## Transactions And Statement Routing

A statement string with several statements is one transaction: if any statement
fails, earlier statements in the call are rolled back. On a filesystem database,
`BEGIN`, `COMMIT`, and `ROLLBACK` control a transaction across calls; buffered
DML is applied on commit or discarded on rollback.

```python
with ApexClient("./data") as client:
    client.execute("CREATE TABLE orders (id BIGINT, total DOUBLE)")
    client.use_table("orders")
    client.execute("BEGIN; INSERT INTO orders VALUES (1, 39.9); COMMIT;")
    client.execute("BEGIN")
    client.execute("INSERT INTO orders VALUES (2, 9.9)")
    client.execute("ROLLBACK")
    print(client.execute("SELECT id FROM orders ORDER BY id").to_dict())
```

PG wire treats a multi-statement simple-query message without explicit control
as an implicit transaction, so a failed statement rolls back the whole message;
`BEGIN`/`COMMIT`/`ROLLBACK` are per-connection state. See
[SQL Guide](user-guide/sql.md#transactions).

## Bounded Execution

Operator state for `GROUP BY`, `ORDER BY`, `DISTINCT`, and joins is checked
against a per-query budget and spills to disk when needed, with an identical
result; `execute()` still materializes its result in memory.

| Variable | Default | Effect |
| --- | --- | --- |
| `APEX_QUERY_MEMORY_MB` | `1024` | Per-statement operator budget in MiB; `0` disables it |
| `APEX_PROCESS_BUDGET_MB` | unset (disabled) | Process-wide admission budget in MiB |
| `APEX_SPILL_DIR` | system temp `apex_spill/` | Directory for out-of-core run files |

```python
import os

os.environ["APEX_QUERY_MEMORY_MB"] = "256"
os.environ.update(APEX_PROCESS_BUDGET_MB="1024",
                  APEX_SPILL_DIR="/tmp/apex-spill")

with ApexClient(":memory:") as client:
    client.execute("CREATE TABLE events (id BIGINT, payload TEXT)")
    client.use_table("events")
    client.store({"id": list(range(5)), "payload": [f"row-{i}" for i in range(5)]})
    for batch in client.execute_stream("SELECT id, payload FROM events"):
        print(batch.num_rows, batch.column("id").to_pylist())
```

Statements that cannot be admitted are rejected explicitly with
`[APEX_ADMISSION_QUEUE_FULL]` or `[APEX_ADMISSION_TIMEOUT]`; nothing waits
unboundedly or drops silently. See
[Process Memory Admission](API_REFERENCE.md#process-memory-admission-v5-s10),
[Out-of-Core Spill](API_REFERENCE.md#out-of-core-spill-v5-s11), and
[Streaming Results](API_REFERENCE.md#streaming-results-v5-s12).

## Hybrid Retrieval And Agent Memory

Full-text search (BM25) and vector search run in the same process and query
path. `hybrid_search()` fuses a vector TopK branch with a BM25 branch on `_id`.
For metadata filters, use the SQL hybrid pattern (`WHERE MATCH(...) AND
metadata_predicate`) or filter the recalled rows in Python.

```python
with ApexClient("./data") as client:
    client.create_table("notes", schema={
        "slug": "string", "body": "string", "embedding": "float32_vector",
    })
    client.store({
        "slug": ["intro", "install", "search"],
        "body": ["ApexBase stores data in one local file per table.",
                 "Install ApexBase with pip and create a client.",
                 "Full-text search and vector search run in one process."],
        "embedding": [[1.0, 0.0, 0.0], [0.9, 0.1, 0.0], [0.0, 1.0, 0.0]],
    })
    client.init_fts(index_fields=["body"])
    print(client.hybrid_search(
        [1.0, 0.0, 0.0], "install local file", k=2, vector_col="embedding"
    ).to_dict())
```

This is the agent-memory shape: one local database root for conversation
chunks, metadata, and embeddings; embeddings come from your application's
model, and no server or LLM runs in the database. See [Agent Memory & RAG](user-guide/agent-memory.md).

## Multi-Process Writes

Several processes can write one table. A per-table lock serializes writers, and
each writer reserves `_id` ranges from a shared on-disk high-water mark in the
table sidecar, so ids stay unique across processes. See [Storage Architecture](STORAGE_ARCHITECTURE.md).

## FTS Reconciliation

FTS persistence reconciles its snapshot and WAL generation with the table when
the index opens, and a stale index is rejected explicitly instead of answering
from partial coverage. The `.apex` table remains the source of truth, so
`init_fts()` or `CREATE FTS INDEX` back-fills it. See [Full-Text Search Guide](FTS_GUIDE.md).

## Process-Local Default Connection

The module-level `apexbase.execute()` helper lazily creates one shared
in-memory client inside the current Python process:

```python
import apexbase

apexbase.execute("CREATE TABLE counters (name TEXT, value INT)")
apexbase.execute("INSERT INTO counters VALUES ('jobs', 3)")
print(apexbase.execute("SELECT value FROM counters WHERE name = ?",
                       params=["jobs"]).scalar())
```

Later module-level calls see the same tables. Use an explicit `ApexClient` for
lifecycle control and isolation.

## Durability And Bounded Caches

Durability is configured when opening the client: `fast` leaves writes in the
OS page cache with no fsync, `safe` fsyncs on `flush()`/`close()`, and `max`
fsyncs every write.

```python
with ApexClient(":memory:", durability="safe") as client:
    client.create_table("audit_log")
    client.store({"ok": True})
```

Per-process caches are bounded for a predictable memory ceiling: parsed SQL
statements, planner feedback (256 query shapes per table across at most 256
tables), table statistics, CTE batches, dictionary/null caches, and the
client-side result cache (64 results, 4096 rows / 8 MiB each). Caches are
invalidated by epoch and file mtime. See [Resource Ownership](RESOURCE_OWNERSHIP.md).

## Interfaces And Next Steps

The engine is reachable through the Python API, Rust embedded API, PostgreSQL
Wire, and Arrow Flight. Continue with [Quick Start](QUICK_START.md),
[SQL Guide](user-guide/sql.md), or [Agent Memory & RAG](user-guide/agent-memory.md).
