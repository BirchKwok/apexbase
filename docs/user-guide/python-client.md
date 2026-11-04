# Python Client Guide

The Python client is the main ApexBase interface for embedded applications,
scripts, notebooks, and agents. It opens a directory (or an in-memory database),
runs SQL, writes rows and DataFrames, and reads results into Arrow, Pandas, or
Polars.

## Client Lifecycle

Use the client as a context manager so file handles and temporary tables are
released reliably:

```python
from apexbase import ApexClient

with ApexClient("./data", durability="safe") as client:
    client.create_table("events")
    client.store({"kind": "signup", "user_id": 1})
```

`create_clean()` drops and recreates the storage under a path, which is handy for
tests; `:memory:` is an isolated in-memory database with no sidecar files:

```python
with ApexClient.create_clean("./tmp_data") as client:
    client.create_table("scratch")

with ApexClient(":memory:") as client:
    client.execute("CREATE TABLE scratch (value INT)")
```

Cross-process features such as `hybrid_search()` need a file-backed database, not
`:memory:`. For a shared process-local session, the module-level helpers
(`apexbase.execute(...)`) behave the same way with one implicit client.

## Database And Table Selection

```python
client.use_database("analytics")
client.create_table("events")
client.use_table("events")

client.use(database="analytics", table="events")
```

`use(...)` switches context and creates the table if it is missing; named
databases are subdirectories under the client root.

## Writing Data

```python
client.create_table("users", {"name": "string", "age": "int64"})

client.store({"name": "Alice", "age": 30})
client.store([{"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}])
client.store({"name": ["Diana", "Eve"], "age": [29, 31]})
```

The columnar dict is the fastest pure-Python ingest, and the explicit schema
skips type inference on the first write.

DataFrame importers select or create the target table:

```python
import pandas as pd
import polars as pl
import pyarrow as pa

client.from_pandas(pd.DataFrame({"name": ["Alice"], "age": [30]}), table_name="users")
client.from_polars(pl.DataFrame({"name": ["Bob"], "age": [25]}), table_name="users")
client.from_pyarrow(pa.table({"name": ["Charlie"], "age": [35]}), table_name="users")
```

## Blob Payloads

Declare large byte columns as `blob` or `large_binary` for descriptor-backed
storage:

```python
client.create_table("files", {"name": "string", "payload": "blob"})
client.store({
    "name": ["small.bin", "video.bin"],
    "payload": [b"small", b"\0" * (5 * 1024 * 1024)],
})

info = client.read_blob_info("payload", 2)
chunk = client.read_blob_range("payload", 2, offset=0, length=65536)
data = client.read_blob("payload", 2)
```

Payload bytes load only when you project the blob column or use a helper.

## Querying

Use `execute()` for SQL:

```python
client.use_table("users")
result = client.execute("""
    SELECT age, COUNT(*) AS users
    FROM users
    GROUP BY age
    ORDER BY users DESC
""")
```

Use `query()` for table-scoped filtering and record helpers for known ids:

```python
result = client.query(where_clause="age >= 30", limit=100)
row = client.retrieve(1)
rows = client.retrieve_many([1, 2, 3])
all_rows = client.retrieve_all()
```

Deterministic analytical `SELECT` results may be cached per client; writes and
schema changes invalidate entries, and volatile queries are not reused.

## Working With Results

`ResultView` is a lightweight handle; convert it to the format you need:

```python
client.use_table("users")
result = client.execute("SELECT * FROM users")
print(result.shape)      # (rows, columns)
print(result.columns)    # ['name', 'age']; show_internal_id=True adds '_id'
rows = result.to_dict()
df = result.to_pandas()
pl_df = result.to_polars()
arrow_table = result.to_arrow()
```

Arrow conversion is the cheapest path for columnar downstream code.

## Typed Projection

`describe_table()` reports declared storage types, including parameterised
logical types:

```python
import decimal

client.create_table("ledger", {"id": "int64", "amount": "decimal(18,2)"})
client.store({"id": [1], "amount": [decimal.Decimal("19.99")]})
client.use_table("ledger")
print(client.describe_table())   # {'id': 'int64', 'amount': 'decimal(18,2)'}

result = client.execute("SELECT * FROM ledger")
arrow_table = result.to_arrow(declared_types=client.describe_table())
polars_df = result.to_polars(declared_types=client.describe_table())
```

Without `declared_types`, decimal and UUID payloads stay as 16-byte values; with
it, Arrow gets `decimal128(18, 2)` and Polars gets `Decimal(18, 2)`, and the same
holds for time-family and `list<T>` columns.

## Batch Execution

`execute_batch()` runs ordered SQL with read-after-write visibility and coalesces
consecutive `UPDATE ... WHERE _id = N` statements:

```python
client.use_table("users")
results = client.execute_batch([
    "INSERT INTO users (name, age) VALUES ('Alice', 30)",
    "UPDATE users SET age = 31 WHERE _id = 1",
    "SELECT COUNT(*) AS users FROM users",
])
```

`execute_batch_parallel()` runs independent read-only queries concurrently:

```python
stats = client.execute_batch_parallel([
    "SELECT city, COUNT(*) FROM users GROUP BY city",
    "SELECT AVG(age) FROM users",
])
```

Only `SELECT` / `WITH` / `EXPLAIN` are accepted.

## Transactions

A statement string with several statements is one transaction; control statements
in the same call apply to the whole batch:

```python
client.use_table("users")
client.execute("""
    BEGIN;
    UPDATE users SET age = 31 WHERE _id = 1;
    ROLLBACK;
""")
```

A client-side `BEGIN` covers later batches until you commit or roll back:

```python
client.execute("BEGIN")
client.execute("INSERT INTO users (name, age) VALUES ('Dana', 41)")
client.execute("COMMIT")
```

An error inside a transaction discards buffered DML and returns the connection
to auto-commit; a failed `COMMIT` reports a machine-readable outcome and
transaction id. See the [SQL Guide](sql.md#transactions) for recovery rules.

## Streaming Results

`execute_stream()` returns an iterator of `pyarrow.RecordBatch` objects instead
of materializing the whole result:

```python
for batch in client.execute_stream("SELECT name, age FROM users"):
    print(batch.num_rows, "rows")   # one row group at a time
```

Plain projections keep one batch resident. `ORDER BY`, `GROUP BY`, and joins are
materialized by their spill operators as a single batch. Errors surface as
`RuntimeError`; `close()` stops early.

## Vector Search And Hybrid Retrieval

```python
import numpy as np

client.create_table("docs", {"body": "text", "embedding": "float32_vector"})
client.use_table("docs")
client.store({"body": ["a", "b"], "embedding": np.random.rand(2, 8).astype(np.float32)})

query = np.random.rand(8).astype(np.float32)
hits = client.topk_distance("embedding", query, k=5, metric="cosine")
rows = client.retrieve_many(hits.get_ids().tolist())
```

Run many queries in one call with `batch_topk_distance()`:

```python
queries = np.random.rand(200, 8).astype(np.float32)
pairs = client.batch_topk_distance("embedding", queries, k=10)
# shape (200, 10, 2); pairs[i, :, 0] are ids, pairs[i, :, 1] distances
```

Fuse vector and BM25 ranking on a persistent database:

```python
client.init_fts()   # or CREATE FTS INDEX ON docs(body) on first use
fused = client.hybrid_search(
    query, "deployment rollback",
    k=10, vector_col="embedding", metric="cosine", fusion="rrf",
)
```

Build a persisted IVF index for large tables and query with `nprobe=`:

```python
client.create_vector_index("embedding", nlist=64, iterations=10, metric="cosine")
info = client.vector_index_info()   # dim/nlist/rows/metric/stale
approx = client.topk_distance("embedding", query, k=10, metric="cosine", nprobe=16)
client.drop_vector_index()
```

Rebuild when `info["stale"]` is true. See [Agent Memory & RAG](agent-memory.md)
for the full hybrid recipe.

## Quantized Vector Retrieval

```python
client.create_quantized_column("embedding", "embedding_tq4", "turboquant4")
hits = client.topk_distance(
    "embedding", query, k=20,
    accelerator="embedding_tq4", candidate_k=160, rescore=True,
)
client.drop_quantized_column("embedding_tq4")
```

Inserts update the accelerator automatically; direct accelerator writes are
rejected. See the [Vector Quantization Guide](../VECTOR_QUANTIZATION_GUIDE.md).

## Updating And Deleting

```python
client.use_table("users")
client.replace(1, {"name": "Alice", "age": 31})
client.delete(where="age < 18")
client.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
client.execute("DELETE FROM users WHERE age < 18")
```

Prefer `replace()` or `delete()` on tables with a vector column: in 2.0.0 a SQL
`UPDATE` on such a table disables the direct vector scan used by
`hybrid_search()`. See [Agent Memory & RAG](agent-memory.md#recency-and-scope).

## Schema Changes

```python
client.use_table("users")
client.add_column("email", "string")
client.rename_column("email", "contact_email")
client.drop_column("contact_email")
```

Schema changes invalidate cached results and index postings for the table;
rebuild secondary or FTS indexes when needed.

## Durability And Flushing

```python
client = ApexClient("./data", durability="safe")
client.flush()
```

| Level | Behaviour | Use for |
| --- | --- | --- |
| `fast` | Highest throughput, no per-write fsync | Scratch data, reproducible imports |
| `safe` | Balanced persistence | Application data and agent memory |
| `max` | Fsync on every write | Data you cannot replay |

## Memory Budgets And Admission

| Variable | Default | Effect |
| --- | --- | --- |
| `APEX_QUERY_MEMORY_MB` | `1024` | Per-query operator budget; `ORDER BY`/`GROUP BY`/`DISTINCT`/`JOIN` spill when exceeded |
| `APEX_PROCESS_BUDGET_MB` | disabled | Process-wide reservation budget with a bounded queue |
| `APEX_SPILL_DIR` | temp `apex_spill/` | Root directory for out-of-core run files |

Set them before the client opens. The per-query budget is always in effect;
process-wide admission is off until `APEX_PROCESS_BUDGET_MB` is set. Statements
that do not fit wait in a bounded queue; a full queue or timeout is rejected
with `[APEX_ADMISSION_QUEUE_FULL]` or `[APEX_ADMISSION_TIMEOUT]` instead of OOM.

```python
client._storage._admission_stats()
# (used_bytes, waiting, admitted, rejected, timed_out, budget_bytes)
```

## Where To Go Next

- [SQL Guide](sql.md) and [Data Import](data-import.md) for SQL and bulk loads.
- [Agent Memory & RAG](agent-memory.md) for hybrid retrieval.
- [Python API Reference](../API_REFERENCE.md) for every method signature.
