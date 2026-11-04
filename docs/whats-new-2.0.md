# What's new in ApexBase 2.0

ApexBase 2.0.0 is the **V5 GA** release. It promotes the V5 storage and
execution architecture to a stable format, completes the logical type system,
closes the remaining SQL semantics gaps, and makes memory usage predictable for
long-running applications and agents.

If you are upgrading from v1.x, read the [migration guide](#upgrade-guide)
before opening production files.

## At a glance

| Area | What changed in 2.0 |
| --- | --- |
| **Storage** | On-disk format 5 is stable; V5 builds read V4 files; compressed tables keep one frame per column; `_id` ranges are reserved across processes |
| **Types** | `uuid`, `decimal(p,s)`, `timestamp`/`timestamptz`/`date`/`time`/`interval`, `list<T>`, and explicit `extension<codec>` columns |
| **Execution** | Per-query memory budget, process admission with a bounded queue, out-of-core spill, and `execute_stream()` Arrow batches |
| **SQL** | `DISTINCT *` on visible columns, deterministic duplicate-alias handling, fixed `RIGHT JOIN` ordering, uniform transaction and `COPY` routing |
| **Search** | FTS generations reconciled on open; stale indexes rejected; native `hybrid_search()` fusion; persisted IVF index |
| **Performance** | 105/105 tabular and 6/6 exact-vector comparison rows won in the retained public baseline; 10M-row scale checks recorded |

## Stable storage format 5

Format version 5 is fixed for the 2.0 release line. It is a per-file
declaration in the `.apex` header; a table starts declaring version 5 when a
V5-only encoding is first written.

| Compatibility | Behavior |
| --- | --- |
| Read V4 files with a V5 build | Supported, unchanged |
| Read format-5 files with a pre-V5 build | **Not supported**: those builds do not know the new encodings |
| Write the V4 layout from a V5 build | `APEX_WIDTH_FIDELITY=0` keeps the older layout and version declaration |
| Compressed tables read by older builds | `APEX_CHUNK_INDEX=0` keeps the flat frame expected by pre-V5 readers |
| SQL migration step | None; a V4 file opens as before |

Format 5 adds an optional logical-type schema section and a compressed frame per
column for chunked tables, so a column-selective scan decodes only the columns it
touches. Multi-process writers reserve `_id` ranges from a shared on-disk
high-water mark: ids may be skipped, but they are never repeated.

## The logical type system

New columns carry declared types all the way through storage, SQL, and Arrow:

```python
from decimal import Decimal
from apexbase import ApexClient

with ApexClient("./typed") as client:
    client.create_table("records", schema={
        "id": "uuid",
        "amount": "decimal(38,2)",
        "created": "timestamptz",
        "tags": "list<string>",
    })
    print(client.describe_table())
    # {'id': 'uuid', 'amount': 'decimal(38,2)',
    #  'created': 'timestamptz', 'tags': 'list<string>'}
```

| Type | Storage | Notes |
| --- | --- | --- |
| `uuid` | 16 bytes | Projected as `bytes` by default |
| `decimal(p,s)` / `decimal128` | 16-byte `Decimal128` payload | `decimal.Decimal`, `int`, and exact strings accepted on write; typed Arrow/Polars projection keeps the payload |
| `timestamp` / `timestamptz` / `date` / `time` / `interval` | 8 bytes | Presented as Python `datetime` / `date` / `time` / `timedelta` on every read path |
| `list<element>` | Variable-length list | Columnar writes; Arrow `ListArray` projection; predicates/aggregates over lists are explicit errors |
| `extension<codec>` | Encoded binary | Codec registered in Python; version recorded in the schema |

SQL `DECIMAL(p,s)` is enabled by default in V5: `INSERT` takes exact
string/integer literals, `SUM`/`MIN`/`MAX` return the unscaled payload, and
`AVG` returns a scaled float. `APEX_SQL_DECIMAL=0` restores the legacy text
representation if you need the old behavior.

### Extension codecs are explicit

`json` and `bytes` are built in. Everything else — including `pickle` — must be
registered deliberately, so a data file never executes code just because it was
opened:

```python
import json
from apexbase import register_extension_codec

register_extension_codec(
    "json",
    lambda value: json.dumps(value).encode(),
    lambda payload: json.loads(payload),
)

with ApexClient("./extensions") as client:
    client.create_table("events", {
        "id": "int64",
        "payload": "extension<json>",
    })
    client.use_table("events")
    client.store({"id": [1], "payload": [{"kind": "tool", "ok": True}]})
    print(client.execute("SELECT * FROM events").to_dict())
```

Unregistered codecs, codec failures, and version mismatches raise explicit
errors; there is no silent fallback to raw bytes.

## Bounded execution

The V5 executor has a hard memory story instead of "it depends on the data":

| Control | Default | What it bounds |
| --- | --- | --- |
| `APEX_QUERY_MEMORY_MB` | `1024` | Per-query operator state (aggregation, sort, join, distinct) |
| `APEX_PROCESS_BUDGET_MB` | unset/`0` (off) | Process-wide admission ceiling with a bounded queue |
| `APEX_QUERY_ADMISSION_MB` | `16` | Reservation for ordinary point/short/scan statements |
| `APEX_QUERY_QUEUE_MAX` | `64` | Maximum statements waiting for an admission slot |
| `APEX_QUERY_QUEUE_WAIT_MS` | `5000` | Per-statement admission wait before an explicit timeout |
| `APEX_SPILL_DIR` | system temp | Root directory for out-of-core run files |
| `APEX_SPILL_TTL_SECS` | `3600` | Cleanup age for crash-leftover spill files |

`ORDER BY`, `GROUP BY`, `SELECT DISTINCT`, and equi-`JOIN` fall back to disk
when their tracked state exceeds the budget; the result is identical to the
in-memory path. A statement that cannot be admitted is rejected explicitly with
`[APEX_ADMISSION_QUEUE_FULL]` or `[APEX_ADMISSION_TIMEOUT]` in the error message
— never a silent drop.

### Streaming results

`execute()` still materializes its result. When the output itself is too large,
stream it:

```python
with ApexClient("./events") as client:
    client.use_table("events")
    for batch in client.execute_stream("SELECT id, payload FROM events"):
        consume(batch)  # pyarrow.RecordBatch, one row group at a time
```

Plain projections stream one row-group batch at a time. Shapes that must be
materialized internally (`ORDER BY`, `GROUP BY`, `JOIN`) are still bounded by the
spill operators and delivered as a single batch. The same streaming path powers
PostgreSQL Wire reads and Arrow Flight.

## SQL semantics and routing

Three semantic changes are intentional migration notes:

| Change | New behavior |
| --- | --- |
| `SELECT DISTINCT *` | Deduplicates on visible columns only; the internal `_id` is not part of the key |
| Duplicate projection aliases | Made unique deterministically: `SELECT a AS x, b AS x` returns `x`, `x_1` instead of overwriting |
| `ORDER BY <alias>` | Resolves projection aliases before sorting, including `RIGHT JOIN` paths; NULLs sort last ascending, first descending |

Statement routing is now uniform across transports:

- A multi-statement `execute()` call is one transaction; a failure or
  `ROLLBACK` discards the batch.
- PostgreSQL Wire simple-query batches are implicit transactions with
  PostgreSQL semantics; `BEGIN`/`COMMIT`/`ROLLBACK` are per-connection state.
- `COPY <new_table> FROM '<file>'` works without a selected table and makes the
  imported table the current one.
- `USE` inside a multi-statement batch or transaction is rejected explicitly.

```python
with ApexClient("./tx") as client:
    client.execute("CREATE TABLE events (id INT, payload TEXT)")
    client.use_table("events")
    client.execute("""
        BEGIN;
        INSERT INTO events (id, payload) VALUES (1, 'first');
        INSERT INTO events (id, payload) VALUES (2, 'second');
        COMMIT;
    """)
    print(client.execute("SELECT COUNT(*) FROM events").scalar())  # 2
```

## Safer retrieval

- **FTS generations are reconciled on open.** The snapshot and WAL carry source
  generations; an index that cannot be verified against the table is rejected
  explicitly instead of answering from partial coverage. `ALTER FTS INDEX
  ENABLE` also becomes searchable before it returns.
- **Atomic index replacement** works on Windows as well as Unix, and every open
  index is flushed on client close.
- **Hybrid retrieval is native.** `hybrid_search()` fuses a vector TopK branch
  with a BM25 branch using reciprocal rank fusion or a weighted blend, without
  moving data to another service.
- **A persisted IVF index** (`create_vector_index`) supports approximate vector
  search with `topk_distance(..., nprobe=...)`; `vector_index_info()["stale"]`
  tells you when the table changed since the build.

The [Agent Memory & RAG guide](user-guide/agent-memory.md) shows the hybrid
pattern end to end; [Full-Text Search](FTS_GUIDE.md),
[Float16 Vectors](FLOAT16_VECTOR_GUIDE.md), and
[Vector Quantization](VECTOR_QUANTIZATION_GUIDE.md) cover the retrieval
building blocks.

## Performance

The retained public baseline is the median of three same-machine runs:
1,000,000 tabular rows, 1,000,000 × 128-dim Float32 vectors, 2 warmups, 5 timed
iterations, result cache disabled, Apple arm64. ApexBase won **105/105** tabular
and **6/6** exact-vector rows; the quantized module won all **6/6** codecs shared
with sqlite-vector.

The 10M-row scale harness records `COUNT(*)` at 0.043 ms, low-cardinality
`GROUP BY` at 7.1 ms, a high-cardinality 10M-group `GROUP BY` at 2.6 s under a
1 GB budget (a 256 MB re-run validated spill with an exact result), and IVF
recall@10 = 0.97 at ~11 ms median with `nprobe=16`.

See [Performance](performance.md) for the full scoreboard, environment, and
reproduction commands.

## Upgrade guide

1. **Back up the directory** before upgrading, as with any storage format
   change.
2. **Install 2.0.0** and open an existing database. V4 files open unchanged; no
   SQL or table rewrite is required.
3. **Keep older readers working** by writing the V4 layout with
   `APEX_WIDTH_FIDELITY=0` and, for compressed tables, `APEX_CHUNK_INDEX=0`.
4. **Upgrade readers first for typed files.** Files using the logical-type
   section require a 2.0.0-or-newer reader; older V5 preview builds do not know
   the parameterised encodings.
5. **Review semantics-sensitive queries** against the `DISTINCT *`, alias, and
   `RIGHT JOIN` changes above.
6. **Set memory bounds** for long-running services and agents:
   `APEX_QUERY_MEMORY_MB`, `APEX_PROCESS_BUDGET_MB`, and `APEX_SPILL_DIR`.

## Known limits

- `ORDER BY ... LIMIT` over 10M rows is slower than DuckDB in the scale harness
  (83.8 ms vs 50.5 ms at 10M); it is tracked as a performance follow-up.
- The 10M-row SQLite comparison runs on a 1M subset because of SQLite import
  time; DuckDB is compared at the full 10M rows.
- `execute()` materializes its result (O(output)); use `execute_stream()` when
  that is too large.
- Predicates, aggregates, `ORDER BY`, and `GROUP BY` over `extension<codec>` or
  `list<T>` columns are outside the V5 query surface and raise explicit errors.

The complete V4 → V5 compatibility rules and the historical v1.x notes remain in
the [release notes](release-notes.md); the storage layout is documented in
[Storage Architecture](STORAGE_ARCHITECTURE.md).
