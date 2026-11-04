# SQL Guide

ApexBase supports a practical SQL dialect for embedded HTAP workloads: DDL, DML,
analytical `SELECT`, joins, CTEs, set operations, transactions, table functions,
full-text search, and vector search.

## Running SQL

```python
from apexbase import ApexClient

with ApexClient("./data", durability="safe") as client:
    client.execute("CREATE TABLE IF NOT EXISTS users (name STRING, age INT)")
    client.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    result = client.execute("SELECT * FROM users WHERE age >= 18")
```

A statement string with several statements runs as one transaction.

## DDL

```sql
CREATE TABLE IF NOT EXISTS users;
ALTER TABLE users ADD COLUMN name STRING;
ALTER TABLE users ADD COLUMN age INT;
DROP TABLE IF EXISTS old_users;
```

Use qualified names for named databases: `CREATE TABLE analytics.events;` and
`DROP TABLE IF EXISTS analytics.old_events;`.

`CREATE TABLE` accepts logical types: `DECIMAL(p,s)`/`NUMERIC(p,s)`,
`TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, `TIME`, `INTERVAL`, `UUID`, `LIST<T>`, and
the vector types. `DECIMAL` stores a 16-byte payload and keeps precision and scale; time values
round-trip as Python `datetime`/`date`/`time`/`timedelta`:

```sql
CREATE TABLE ledger (id INT, amount DECIMAL(18,2), created TIMESTAMP);
INSERT INTO ledger (id, amount, created)
VALUES (1, '19.99', '2026-11-04 09:30:00');
```

See [Logical Types](../API_REFERENCE.md#table-management) for the full type list.

## DML

```sql
INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25);
UPDATE users SET age = 31 WHERE name = 'Alice';
DELETE FROM users WHERE age < 18;
```

`UPDATE ... SET` accepts literals and bound parameters; complex right-hand-side
expressions are not supported yet. `UPDATE` on a table with a vector column
disables the direct vector scan used by `hybrid_search()`; see
[Agent Memory & RAG](agent-memory.md#recency-and-scope).

## Parameter Binding

Pass `params` to `execute()` instead of interpolating strings. Positional `?`
placeholders consume values from a list/tuple; named `:name`, `@name`, or
`$name` placeholders read from a dict. A list/tuple value expands to a
comma-separated list for `IN (?)`.

```python
client.execute("SELECT * FROM users WHERE age > ?", params=[30])   # positional
client.execute(
    "INSERT INTO events (kind, ts) VALUES (:kind, :ts)",
    params={"kind": "click", "ts": "2026-08-07 12:00:00"},
)                                                                   # named
client.execute("SELECT * FROM users WHERE city IN (?)",
               params=[["Beijing", "Shanghai"]])                    # IN-list
```

Binding works for SELECT, DML, DDL, and transaction statements and keeps TopK on
its native FFI path. Wrong arity raises `ValueError`; an unsupported value type
raises `TypeError`.

## Analytical Queries

```sql
SELECT city, COUNT(*) AS users, AVG(age) AS avg_age
FROM users
WHERE age BETWEEN 18 AND 65
GROUP BY city
HAVING users > 10
ORDER BY avg_age DESC
LIMIT 20;
```

## Joins

```sql
SELECT u.name, e.event, e.ts
FROM users u
JOIN events e ON u.id = e.user_id
WHERE e.event = 'purchase';
```

Supported join forms include `INNER`, `LEFT`, `RIGHT`, `FULL`, and `CROSS`.

## CTEs And Subqueries

```sql
WITH active_users AS (
    SELECT user_id, COUNT(*) AS events
    FROM events
    GROUP BY user_id
)
SELECT u.name, a.events
FROM users u
JOIN active_users a ON u.id = a.user_id
WHERE a.events > 5;
```

## Window Functions

```sql
SELECT
    user_id,
    ts,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts) AS event_rank,
    COUNT(*) OVER (PARTITION BY user_id) AS user_events
FROM events;
```

## Set Operations

```sql
SELECT user_id FROM web_events
UNION
SELECT user_id FROM mobile_events;
```

`UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT` are supported.

## V5 Semantics Notes

Three v2.0.0 changes can alter results of queries written for older releases:

- `SELECT DISTINCT *` deduplicates on visible columns only; `_id` is not part
  of the key (`show_internal_id=True` still returns one representative `_id`).
- Duplicate projection aliases are made unique: `SELECT a AS x, b AS x` yields
  columns `x`, `x_1`.
- `ORDER BY <alias>` resolves projection aliases, including the RIGHT JOIN path.
  NULLs sort last for ascending and first for descending (Arrow default).

```sql
SELECT DISTINCT * FROM dup;            -- dedupes (a, b, c), not _id
SELECT a AS x, b AS x FROM dup;        -- columns: x, x_1
SELECT v FROM nul ORDER BY v ASC;      -- NULLs last
```

Use explicit distinct aliases when output names are part of a contract.

## Indexes

```sql
CREATE INDEX idx_order_user ON orders (user_id);
DROP INDEX idx_order_user ON orders;
DROP INDEX IF EXISTS idx_order_user ON orders;

ANALYZE orders;   -- refresh the planner statistics for this table
REINDEX orders;   -- rebuild persistent secondary-index postings
```

`DROP INDEX` requires the `ON table` clause. Secondary-index DDL, `ANALYZE`, and
`REINDEX` operate on the selected table. Persisted postings are marked stale
before a committed table change and are not used until `REINDEX` rebuilds them.

## Transactions

```sql
BEGIN;
INSERT INTO orders (order_id, total) VALUES (1001, 39.9);
SAVEPOINT before_adjustment;
UPDATE orders SET total = 35.9 WHERE order_id = 1001;
RELEASE before_adjustment;
COMMIT;
```

Use `ROLLBACK` to cancel a transaction, or `ROLLBACK TO savepoint_name` to undo
part of one. A multi-statement string in one `execute()` call is a single
transaction, and a client-side `BEGIN` covers later batches. Statements route
the same way inside and outside transactions, so buffered DML obeys the same
commit boundary. PG Wire adds implicit transactions for multi-statement simple
queries; see [Server Protocols](server-protocols.md#postgresql-wire).

### Commit Failures

If `COMMIT` fails, the error carries a machine-readable outcome and the
transaction id, for example:

```text
commit_outcome=unknown txn_id=42: <original I/O error>
```

| Outcome | Meaning | Safe next step |
| --- | --- | --- |
| `not_committed` | Nothing was applied. | Retry as a new transaction. |
| `unknown` | A marker or data write may have taken effect. | Reopen, inspect state, reconcile; never blind-replay. |
| `committed` | Data, indexes, and publication completed. | Treat the work as durable. |

Multi-table transactions apply work in deterministic table order. After a crash,
recovery converges each table independently, not database-wide.

## COPY

```sql
COPY users TO 'users.csv' (FORMAT CSV, HEADER);
COPY users TO 'users.parquet' (FORMAT PARQUET);

COPY people FROM 'people.csv' (FORMAT CSV, HEADER);
```

`COPY <table> TO` requires a selected table. `COPY <new_table> FROM` creates and
publishes the table without a selected table, and the target becomes the selected
table for later unqualified statements. In 2.0.0, `COPY ... FROM` is supported
for CSV; import Parquet with `read_parquet()` or `from_pyarrow()` instead, and
see [Data Import](data-import.md#parquet-and-lance).

## File Table Functions

Query files directly:

```sql
SELECT city, COUNT(*) AS rows
FROM read_csv('events.csv')
GROUP BY city;
```

Supported functions are `read_csv(path)`, `read_parquet(path)`, and
`read_json(path)`. For repeated queries, register the file as a temporary table
from Python; see [Data Import](data-import.md).

## Full-Text Search

```sql
CREATE FTS INDEX ON articles(title, content);

SELECT title
FROM articles
WHERE MATCH('rust database');
```

`MATCH()` takes one string literal and requires all query terms. `FTS_SCORE()`
projects or orders by BM25 relevance; `FUZZY_MATCH()` tolerates typos:

```sql
SELECT title, FTS_SCORE() AS relevance
FROM articles WHERE MATCH('database') ORDER BY relevance DESC;

SELECT title FROM articles WHERE FUZZY_MATCH('databse');
```

See the [Full-Text Search Guide](../FTS_GUIDE.md) for configuration and
lifecycle.

## Vector Search

`topk_distance(column, [query...], k, 'metric')` is a whole-column TopK
expression; wrap it in `explode_rename` to turn the `(id, distance)` pairs into
rows:

```sql
SELECT explode_rename(
    topk_distance(embedding, [0.1, 0.2, 0.3], 10, 'cosine'),
    '_id', 'dist'
)
FROM items;
```

A bound parameter works for the query vector from Python
(`params=[query_vector]`). Order existing rows by distance with `cosine_distance()` when you do not need a
TopK heap; its query vector must be an inline array literal:

```sql
SELECT id, cosine_distance(embedding, [0.1, 0.2, 0.3]) AS dist
FROM items ORDER BY dist LIMIT 10;
```

Metrics: `'l2'`/`'euclidean'`, `'l2_squared'`, `'l1'`, `'linf'`,
`'cosine'`/`'cosine_distance'`, and `'dot'`/`'inner_product'`. Vector columns can
be `FLOAT32_VECTOR`, `FLOAT16_VECTOR`, `BFLOAT16_VECTOR`, `INT8_VECTOR`,
`UINT8_VECTOR`, `BIT1_VECTOR`, or `TURBOQUANT2/3/4_VECTOR`; for the
source-plus-accelerator layout, see the
[Vector Quantization Guide](../VECTOR_QUANTIZATION_GUIDE.md).

!!! warning "Known issue: MATCH() plus cosine_distance()"
    Combining `MATCH()` with `cosine_distance()` raises
    `RuntimeError: array_distance: FixedSizeList dim 0 != query dim N` when the
    text branch returns no rows. Use `hybrid_search()` for combined ranking, or
    guard with `search_text()` first — see
    [Agent Memory & RAG](agent-memory.md#hybrid-sql-for-structured-filters).

## Explain

```sql
EXPLAIN SELECT * FROM users WHERE age > 30;
EXPLAIN ANALYZE SELECT city, COUNT(*) FROM users GROUP BY city;
```

`EXPLAIN` reports the candidate and chosen plan, including the index access spec
when one is executable. `EXPLAIN ANALYZE` also reports `Actual Path`,
`Actual Rows`, `Actual Time`, `Plan Divergence` (when execution left the planned
route), and `Feedback Recorded`. It persists per-shape cost and time feedback in
`<table>.plan_feedback`; feedback is ignored when the sidecar schema version or
OS/architecture/parallelism fingerprint differs, and dropped on schema changes.

## Execution Limits

Aggregation state for supported `GROUP BY` kernels is charged against a
per-query byte budget. Set `APEX_QUERY_MEMORY_MB` to the budget in MiB (`0`
disables it, default `1024`). `ORDER BY`, `GROUP BY`, `DISTINCT`, and equi-join
operators spill to `APEX_SPILL_DIR` when they exceed the budget, so the query
completes with an identical result instead of failing. `APEX_PROCESS_BUDGET_MB`
adds process-wide admission with a bounded queue and explicit
`[APEX_ADMISSION_*]` errors.

`execute()` materializes its result. Use `client.execute_stream()` for large
`SELECT` results: it yields `pyarrow.RecordBatch` objects, streaming plain
projections one row group at a time and spill-bounded shapes as one batch.

Batched scans may use up to `min(CPU - 1, 8)` row-group workers across all queries
in a process. `APEX_PARALLEL_SCAN` overrides this: `0`/`1` forces serial, `N >= 2`
forces `N` workers.

## Where To Go Next

- [Python Client](python-client.md) and [Data Import](data-import.md) for the
  Python API and bulk loads.
- [Agent Memory & RAG](agent-memory.md) for hybrid retrieval, and the
  [FTS Guide](../FTS_GUIDE.md) / [Vector Quantization Guide](../VECTOR_QUANTIZATION_GUIDE.md).
