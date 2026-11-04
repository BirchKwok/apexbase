# Agent Memory & RAG

An agent needs memory that outlives a session: notes, summaries, preferences,
incident context. ApexBase keeps that memory in one local file and ranks it with
SQL, BM25, and vector similarity in one engine. This page builds a tested memory
store, wraps recall as an LLM tool, and bounds memory use.

## Why A Local Memory Engine

A typical agent stack glues together a relational store, an FTS engine, a vector
service, and application code that keeps them consistent. ApexBase collapses
that stack into one embedded engine over one file.

| Concern | Glue stack | ApexBase |
| --- | --- | --- |
| Storage | database + vector service | one `.apex` file, V5 format |
| Text ranking | external BM25 service | `CREATE FTS INDEX`, `MATCH()` |
| Vector ranking | external ANN service | vector column + `topk_distance` |
| Consistency | copy jobs, re-embedding | row writes update every index |
| Operations | server, ports, credentials | embedded library |

Back up or move one directory; the agent links the library, and PG Wire and
Flight are opt-in. One commit boundary covers rows, BM25, and vectors, so
`BEGIN ... ROLLBACK` discards a memory write consistently. Query budgets and
spill cap operator state, and results convert to Arrow, Pandas, and Polars.

## Build A Memory Store

```python
import numpy as np
from datetime import datetime, timezone
from apexbase import ApexClient


def toy_embed(text, dim=8):
    vector = np.zeros(dim, dtype=np.float32)
    for i in range(len(text) - 1):
        vector[(ord(text[i]) * 31 + ord(text[i + 1])) % dim] += 1.0
    norm = np.linalg.norm(vector)
    return vector / norm if norm else vector


class MemoryStore:
    def __init__(self, path, embed=toy_embed):
        self.db = ApexClient(path, durability="safe")
        if "memories" not in self.db.list_tables():   # idempotent open
            self.db.create_table("memories", {
                "content": "text", "source": "text",
                "created_at": "text", "embedding": "float16_vector",
            })
            self.db.execute("CREATE FTS INDEX ON memories(content)")
        self.db.use_table("memories")
        self._embed = embed

    def remember(self, content, source="manual"):
        self.db.store([{
            "content": content, "source": source,
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "embedding": self._embed(content),
        }])

    def recall(self, query, k=5, source=None):
        hits = self.db.hybrid_search(
            self._embed(query), query, k=k * 4,   # over-fetch for filtering
            vector_col="embedding", metric="cosine",
        )
        rows = self.db.retrieve_many([int(i) for i in hits.get_ids()]).to_dict()
        memories = [{key: row[key] for key in ("content", "source", "created_at")}
                    for row in rows]
        if source is not None:
            memories = [m for m in memories if m["source"] == source]
        return memories[:k]


mem = MemoryStore("./agent-memory")
mem.remember("Blue-green rollback restores the previous release", source="runbook")
mem.remember("Postgres pool exhausted during incident 42", source="incident/42")
mem.remember("Retry policy caps backoff at 30 seconds", source="runbook")

for memory in mem.recall("how do we roll back a deploy?", k=2):
    print(memory["content"], "|", memory["source"])
```

```text
Blue-green rollback restores the previous release | runbook
Retry policy caps backoff at 30 seconds | runbook
```

What the class does:

- **Persistent, idempotent open.** `durability="safe"` fsyncs writes an agent
  must not lose; the `list_tables()` check creates the schema and FTS index only
  when the file is new, and FTS reconciles its generation with the table on
  reopen.
- **Schema.** `content`/`source` are text, `created_at` is ISO-8601 text, and
  `embedding` is `float16_vector` (half the disk of `float32_vector`).
- **`remember()`.** One `store([{...}])` row write updates FTS and the vector
  column together, so recall never sees a half-indexed row.
- **`recall()`.** `hybrid_search` fuses a cosine `topk_distance` branch and a
  BM25 branch on `_id`, then `retrieve_many()` fetches the rows. `k * 4`
  over-fetches so a source filter still has candidates.

`toy_embed` is a deterministic placeholder; replace it with your model's
function and keep a fixed dimension.

!!! note "Use a persistent database"
    In 2.0.0 `hybrid_search` needs the persistent vector-scan lane; an
    `ApexClient(":memory:")` client raises
    `RuntimeError: topk_distance(raw=True): column 'embedding' is not served by
    the direct vector scan`.

## Recency And Scope

`created_at` is ISO-8601 text with a UTC offset, so `ORDER BY` sorts
chronologically as long as every writer uses the same offset.

```python
recent = mem.db.execute(
    "SELECT content, source, created_at FROM memories "
    "ORDER BY created_at DESC LIMIT 10").to_dict()
scoped = mem.db.execute(
    "SELECT content, created_at FROM memories WHERE source = ? "
    "ORDER BY created_at DESC", params=["runbook"]).to_dict()
```

`replace()` rewrites the whole row, so carry the fields you keep; `delete()`
removes by internal id.

```python
row = mem.db.retrieve(2)          # internal id from retrieve()/get_ids()
mem.db.replace(2, {"content": "redacted", "source": row["source"],
                   "created_at": row["created_at"],  # keep the recency key
                   "embedding": mem._embed("redacted")})
mem.db.execute("DELETE FROM memories WHERE created_at < ?",
               params=["2026-01-01T00:00:00+00:00"])
mem.db.delete(id=3)
```

!!! warning "Known issue: avoid SQL UPDATE on vector tables"
    On 2.0.0, a SQL `UPDATE` on a table with a vector column permanently
    disables the direct vector scan used by `hybrid_search()` (and
    `topk_distance(raw=True)`), even after `flush()` or reopen. Use `replace()`
    or `delete()` plus a fresh `store()` for edits and forgetting. Non-raw
    `topk_distance()` and SQL `cosine_distance()` keep working after an UPDATE.

For retention, delete by `created_at`; no index rebuild is needed.

## Expose Recall As A Tool

Declare the tool once and keep the database boundary in your handler. No LLM
call happens here: your runtime parses the model's tool call and invokes
`dispatch_tool()`.

```python
import json

RECALL_TOOL = {
    "name": "recall_memory",
    "description": "Search the agent's long-term memory for relevant notes.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string"},
            "k": {"type": "integer", "minimum": 1, "maximum": 10, "default": 5},
            "source": {"type": "string"},
        },
        "required": ["query"],
    },
}


def dispatch_tool(mem, name, arguments):
    if name != "recall_memory":
        raise ValueError(f"unknown tool: {name}")
    args = json.loads(arguments)
    memories = mem.recall(args["query"], k=args.get("k", 5), source=args.get("source"))
    return json.dumps({"memories": memories})

dispatch_tool(mem, "recall_memory", '{"query": "rollback", "k": 1}')
# -> {"memories": [{"content": "Blue-green rollback ...", "source": "runbook"}]}
```

Serve `RECALL_TOOL` with your model's tool definitions, validate `arguments` as
JSON, and return one JSON object per call. `source` scopes memory per user or
session without a second table.

## Stream Large Tool Outputs

Do not materialize tool results that leave memory. `execute_stream()` yields
`pyarrow.RecordBatch` objects, one row group at a time:

```python
import os

os.environ["APEX_QUERY_MEMORY_MB"] = "256"     # per-query operator budget
os.environ["APEX_PROCESS_BUDGET_MB"] = "1024"  # process admission budget
os.environ["APEX_SPILL_DIR"] = "/var/tmp/apex-spill"

from apexbase import ApexClient

db = ApexClient("./agent-memory", durability="safe")
db.use_table("memories")
for batch in db.execute_stream("SELECT _id, content FROM memories"):
    for row in batch.to_pylist():
        print(row["content"])   # hand one memory at a time to the model layer
db.close()
```

| Variable | Default | Effect |
| --- | --- | --- |
| `APEX_QUERY_MEMORY_MB` | `1024` | Per-query operator budget; `ORDER BY`/`GROUP BY`/`DISTINCT`/`JOIN` spill instead of failing |
| `APEX_PROCESS_BUDGET_MB` | disabled | Process-wide admission budget with a bounded queue and explicit `[APEX_ADMISSION_*]` rejections |
| `APEX_SPILL_DIR` | temp `apex_spill/` | Root directory for out-of-core run files |

Set these before the client opens. Streaming covers plain projections;
`ORDER BY`, `GROUP BY`, and joins are delivered by their spill operators as one
batch. Prefer `execute_stream()` over `execute()` for large results.

## Hybrid SQL For Structured Filters

When a filter belongs in SQL (hard scope, date range, join), rank directly.
`MATCH()` takes a string literal; `cosine_distance()` takes an inline array
literal, so build it from Python:

```python
query = toy_embed("how do we roll back a deploy?")
vector_literal = "[" + ", ".join(f"{value:.6f}" for value in query) + "]"

rows = mem.db.execute(f"""
    SELECT content, source,
           cosine_distance(embedding, {vector_literal}) AS distance
    FROM memories
    WHERE MATCH('rollback') AND source = 'runbook'
    ORDER BY distance LIMIT 5
""").to_dict()
```

The predicate constrains scope, BM25 selects text candidates, and
`cosine_distance()` orders the survivors. Bound lists expand into several
arguments, so inline the vector literal as shown.

!!! warning "Known issue: zero text candidates break cosine_distance"
    On 2.0.0, `MATCH()` combined with `cosine_distance()` raises
    `RuntimeError: array_distance: FixedSizeList dim 0 != query dim 8` when
    `MATCH()` returns no rows: the empty candidate set reaches the distance
    kernel as a zero-dimension list. Prefer `hybrid_search` for recall, or
    guard the SQL path with `search_text()`:

```python
if len(mem.db.search_text("rollback")) == 0:
    rows = []
else:
    rows = mem.db.execute(f"""
        SELECT content, source,
               cosine_distance(embedding, {vector_literal}) AS distance
        FROM memories
        WHERE MATCH('rollback') AND source = 'runbook'
        ORDER BY distance LIMIT 5
    """).to_dict()
```

## Choose A Retrieval Mode

| Mode | Use when | API |
| --- | --- | --- |
| FTS only | Exact terms, names, error codes, quoted fragments | `search_text()`, `MATCH()`, `FTS_SCORE()` |
| Vector only | Paraphrases with no shared terms | `topk_distance()`, `cosine_distance()` |
| Hybrid | Free-form agent notes where terms and meaning both matter | `hybrid_search()` |

Start hybrid for recall, then measure. A hard keyword filter usually fits SQL
with `MATCH()`; a purely semantic query is cheaper with vector-only. For large
corpora, build an IVF index and pass `nprobe=` to `topk_distance()`; rebuild
when `vector_index_info()["stale"]` is true.

## Where To Go Next

- [Full-Text Search Guide](../FTS_GUIDE.md) for `MATCH()` and FTS lifecycle.
- [Vector Quantization Guide](../VECTOR_QUANTIZATION_GUIDE.md) and
  [Float16 Vector Guide](../FLOAT16_VECTOR_GUIDE.md) for vector storage.
- [Python Client](python-client.md) and [SQL Guide](sql.md) for the APIs above.
- [Performance](../performance.md) for measured ingest and search numbers.
- [Python API Reference](../API_REFERENCE.md#hybrid-search) for `hybrid_search`.
