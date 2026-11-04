# Quickstart

Go from `pip install` to a working local database in about five minutes. You
will build a persistent agent-memory store with hybrid retrieval, run
analytical SQL over a DataFrame, and learn the knobs that keep an embedded
database healthy in production.

ApexBase is an embedded engine: there is no server to start and no connection
string to manage. A database is a directory or a single `:memory:` client.

## 1. Install

```bash
pip install apexbase
```

Verify the native extension loads:

```bash
python - <<'PY'
import apexbase
from apexbase import ApexClient
print("apexbase", apexbase.__version__)
with ApexClient(":memory:") as db:
    db.execute("CREATE TABLE smoke (ok BOOL)")
    db.execute("INSERT INTO smoke VALUES (true)")
    print(db.execute("SELECT COUNT(*) FROM smoke").scalar())
PY
```

Building from source needs a Rust toolchain and `maturin`; see
[Installation](installation.md) for platform requirements and the release-build
workflow. Upgrading from v1? Read the
[2.0 migration notes](whats-new-2.0.md) first.

## 2. Give your agent a memory

Agent memory has three jobs: write a memory, recall the relevant ones, and stay
inside a bounded memory and latency budget. ApexBase does all three in one
engine: BM25 recall, vector ranking, SQL metadata, and persistence.

```python
from datetime import datetime, timezone

from apexbase import ApexClient


class AgentMemory:
    def __init__(self, path="./agent-memory"):
        self.db = ApexClient(path)
        if "memories" not in self.db.list_tables():
            self.db.execute("""
                CREATE TABLE memories (
                    content TEXT,
                    source TEXT,
                    created_at TEXT,
                    embedding FLOAT16_VECTOR
                )
            """)
            self.db.execute("CREATE FTS INDEX ON memories(content)")
        self.db.use_table("memories")

    def remember(self, content, embedding, source="chat", created_at=None):
        self.db.store([{
            "content": content,
            "source": source,
            "created_at": created_at or datetime.now(timezone.utc).isoformat(),
            "embedding": embedding,
        }])

    def recall(self, text, embedding, k=5, source=None):
        # Native fusion: BM25 recall + cosine ranking over one table.
        hits = self.db.hybrid_search(
            embedding, text, k=k * 4,
            vector_col="embedding", metric="cosine",
        )
        rows = self.db.retrieve_many([int(i) for i in hits.get_ids()]).to_dict()
        if source is not None:
            rows = [row for row in rows if row["source"] == source]
        return rows[:k]


memory = AgentMemory()
memory.remember("User prefers dark mode.", [0.10, 0.82, 0.20], source="profile")
memory.remember("Deploys run Fridays at 17:00 UTC.", [0.30, 0.40, 0.85], source="ops")

for row in memory.recall("deploy schedule", [0.28, 0.42, 0.84], k=2):
    print("-", row["content"])
```

What each piece does:

- `FLOAT16_VECTOR` stores embeddings at half precision; you can switch to
  `FLOAT32_VECTOR` when exact fidelity matters more than size.
- `CREATE FTS INDEX` gives BM25 recall over `content`.
- `hybrid_search()` fuses the BM25 and vector branches with reciprocal rank
  fusion. Use `fusion="weighted"` and `alpha` when you need to tune the blend.
- The table is a normal `.apex` file. Reopen the same path in the next process
  and the memory is still there.
- The API is model-agnostic: your embedding model produces the vector, and any
  function-calling framework can wrap `recall()` as a tool.

Expose recall to a model with a small JSON schema:

```python
RECALL_TOOL = {
    "type": "function",
    "function": {
        "name": "recall_memory",
        "description": "Retrieve memories relevant to the current task.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "k": {"type": "integer", "default": 5},
            },
            "required": ["query"],
        },
    },
}
```

Read the [Agent Memory & RAG guide](user-guide/agent-memory.md) for session
scoping, forgetting, recency filters, and the SQL hybrid pattern.

## 3. Analyze data with SQL and DataFrames

Import a DataFrame, run analytical SQL, and take the result back as Pandas.
The same code path works for Polars (`to_polars()`) and Arrow (`to_arrow()`).

```python
import pandas as pd

from apexbase import ApexClient

events = pd.DataFrame({
    "event": ["signup", "click", "purchase", "click", "signup"],
    "value": [0.0, 2.5, 29.9, 1.2, 0.0],
    "day": ["2026-01-01", "2026-01-02", "2026-01-02", "2026-01-03", "2026-01-04"],
})

with ApexClient("./analytics") as db:
    db.from_pandas(events, table_name="events")
    df = db.execute("""
        SELECT event, COUNT(*) AS n, ROUND(AVG(value), 2) AS avg_value
        FROM events
        WHERE day >= '2026-01-02'
        GROUP BY event
        ORDER BY n DESC
    """).to_pandas()

print(df)
```

For file-based analysis without an import step:

```python
db.execute("SELECT city, COUNT(*) FROM read_parquet('orders.parquet') GROUP BY city")
db.execute("SELECT * FROM read_csv('events.csv') LIMIT 10")
```

See [Data Import](user-guide/data-import.md) for CSV, JSON, Parquet, Lance,
Pandas, Polars, and PyArrow workflows.

## 4. Use in-memory mode

`ApexClient(":memory:")` is a true process-local database, not a temporary
directory. It creates no database, catalog, WAL, delta, or index files, and its
contents disappear when the client closes:

```python
from apexbase import ApexClient

with ApexClient(":memory:") as db:
    db.execute("CREATE TABLE events (kind TEXT, duration_ms DOUBLE)")
    db.execute("INSERT INTO events VALUES (?, ?)", params=["query", 1.8])
    print(db.execute("SELECT AVG(duration_ms) FROM events").scalar())
```

For short SQL-only scripts, the module-level helper shares one lazily created
in-memory connection in the current process:

```python
import apexbase

apexbase.execute("CREATE TABLE flags (name TEXT, enabled BOOL)")
apexbase.execute("INSERT INTO flags VALUES ('cache', true)")
print(apexbase.execute("SELECT * FROM flags").to_dict())
```

Use a filesystem path when data must survive the process.

## 5. Tune durability and memory

Choose durability when opening the client:

| Mode | Best for | Behavior |
| --- | --- | --- |
| `fast` (default) | Analytics, scratch data, rebuildable memory | Writes stay in the OS page cache; no fsync |
| `safe` | Application data and agent memory | Fsync on explicit `flush()` / `close()` |
| `max` | Highest crash safety | Fsync every write |

```python
with ApexClient("./agent-memory", durability="safe") as db:
    db.use_table("memories")
    db.store({"content": "durable memory",
              "source": "chat",
              "created_at": "2026-11-04T10:00:00Z",
              "embedding": [0.1, 0.2, 0.3]})
    db.flush()
```

For long-running processes and multi-agent hosts, set the memory bounds once:

```bash
export APEX_QUERY_MEMORY_MB=256      # per-query operator budget (default 1024)
export APEX_PROCESS_BUDGET_MB=1024   # process admission ceiling (default: off)
export APEX_SPILL_DIR=/var/tmp/apex-spill  # out-of-core ORDER BY/GROUP BY/JOIN
```

When a result may be large, stream Arrow batches instead of materializing it:

```python
with ApexClient("./agent-memory") as db:
    db.use_table("memories")
    for batch in db.execute_stream("SELECT content FROM memories"):
        handle(batch)  # pyarrow.RecordBatch, one row group at a time
```

Multi-statement SQL is one transaction: statements run in order and a failure
or `ROLLBACK` discards the batch. `BEGIN` / `COMMIT` / `ROLLBACK` also work as
their own statements, and PostgreSQL Wire applies the same routing to
multi-statement messages.

## 6. Next steps

- [Agent Memory & RAG](user-guide/agent-memory.md): the complete agent-memory workflow.
- [Python Client](user-guide/python-client.md): lifecycle, streaming, transactions, typed projection.
- [SQL Guide](user-guide/sql.md): joins, CTEs, windows, set operations, parameters.
- [Core Concepts](concepts.md): databases, tables, durability, and result views.
- [Performance](performance.md): reproduce the public benchmark on your machine.
- [API Reference](API_REFERENCE.md): exact method signatures.
