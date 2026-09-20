# SQL Guide

ApexBase supports a practical SQL dialect for embedded HTAP workloads: DDL, DML, analytical SELECT queries, joins, CTEs, set operations, transactions, table functions, full-text search, and vector search.

## Running SQL

```python
from apexbase import ApexClient

with ApexClient("./data") as client:
    client.execute("CREATE TABLE IF NOT EXISTS users")
    client.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    result = client.execute("SELECT * FROM users WHERE age >= 18")
```

## DDL

```sql
CREATE TABLE IF NOT EXISTS users;
ALTER TABLE users ADD COLUMN name STRING;
ALTER TABLE users ADD COLUMN age INT;
DROP TABLE IF EXISTS old_users;
```

Use qualified names for named databases:

```sql
CREATE TABLE analytics.events;
DROP TABLE IF EXISTS analytics.old_events;
```

## DML

```sql
INSERT INTO users (name, age) VALUES ('Alice', 30);
INSERT INTO users (name, age) VALUES ('Bob', 25), ('Charlie', 35);

UPDATE users SET age = 31 WHERE name = 'Alice';
DELETE FROM users WHERE age < 18;
```

## Parameter Binding

Pass `params` to `execute()` instead of interpolating strings. Positional `?`
placeholders consume values from a list/tuple; named `:name`, `@name`, or
`$name` placeholders read from a dict. A list/tuple value expands to a
comma-separated list for `IN (?)`.

```python
# Positional
client.execute("SELECT * FROM users WHERE age > ?", params=[30])

# Named
client.execute(
    "INSERT INTO events (kind, ts) VALUES (:kind, :ts)",
    params={"kind": "click", "ts": "2026-08-07 12:00:00"},
)

# IN-list expansion
client.execute("SELECT * FROM users WHERE city IN (?)", params=[["Beijing", "Shanghai"]])
```

Binding works for SELECT, DML, DDL, and transaction statements, and keeps the
TopK vector query on its native FFI fast path. Wrong placeholder arity raises
`ValueError`, and an unsupported value type raises `TypeError`.

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

```sql
SELECT *
FROM users
WHERE id IN (SELECT user_id FROM events WHERE event = 'signup');
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

SELECT user_id FROM paid_users
EXCEPT
SELECT user_id FROM refunded_users;
```

`UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT` are supported.

## Indexes

```sql
CREATE INDEX idx_order_user ON orders (user_id);
DROP INDEX idx_order_user ON orders;
DROP INDEX IF EXISTS idx_order_user ON orders;

ANALYZE orders;   -- refresh the planner statistics for this table
REINDEX orders;   -- rebuild persistent secondary-index postings
```

`DROP INDEX` requires the `ON table` clause. Secondary-index DDL, `ANALYZE`, and
`REINDEX` operate on the selected table, so call `use_table(name)` first (or use
the `ON table` form where the statement accepts it).

ApexBase marks persisted index postings stale before a committed table change
and refuses to use them in planning or execution until `REINDEX` rebuilds them,
so a stale index degrades to a correct scan instead of returning wrong rows.

## Transactions

```sql
BEGIN;
INSERT INTO orders (order_id, total) VALUES (1001, 39.9);
SAVEPOINT before_adjustment;
UPDATE orders SET total = 35.9 WHERE order_id = 1001;
RELEASE before_adjustment;
COMMIT;
```

Use `ROLLBACK` to cancel a transaction, or `ROLLBACK TO savepoint_name` to undo part of one.

### Commit Failures

If `COMMIT` fails, the error carries a machine-readable outcome and the
transaction id, for example:

```text
commit_outcome=unknown txn_id=42: <original I/O error>
```

| Outcome | Meaning | Safe next step |
| --- | --- | --- |
| `not_committed` | No commit marker or data application was attempted. | Retry the work as a new transaction. |
| `unknown` | A marker or data write may have taken effect, or the transaction is gone and its outcome is unavailable. | Reopen the database, inspect the authoritative state, and reconcile; never blind-replay the DML. |
| `committed` | Data, indexes, and transaction publication completed; only post-commit maintenance failed. | Treat the work as durable. |

Multi-table transactions apply their work in deterministic table order. After a
crash, recovery converges each table independently: the contract is per-table
convergence, not database-wide atomicity.

## File Table Functions

Query files directly:

```sql
SELECT city, COUNT(*) AS rows
FROM read_csv('events.csv')
GROUP BY city;
```

Supported functions:

- `read_csv(path)`
- `read_parquet(path)`
- `read_json(path)`

For repeated queries over a file, register it as a temporary table from Python. See [Data Import](data-import.md).

## Full-Text Search

```sql
CREATE FTS INDEX ON articles(title, content);

SELECT title
FROM articles
WHERE MATCH('rust database');
```

For fuzzy matching, lifecycle commands, and configuration, see the [Full-Text Search Guide](../FTS_GUIDE.md).

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

The supported metrics are `'l2'`, `'cosine'`, and `'dot'`. The same expression
accepts a bound parameter for the query vector from Python:

```python
client.execute(
    "SELECT explode_rename(topk_distance(embedding, ?, 10, 'cosine'), '_id', 'dist') FROM items",
    params=[query_vector],
)
```

Vector columns can be declared as `FLOAT32_VECTOR`, `FLOAT16_VECTOR`,
`BFLOAT16_VECTOR`, `INT8_VECTOR`, `UINT8_VECTOR`, `BIT1_VECTOR`, or
`TURBOQUANT2_VECTOR` / `TURBOQUANT3_VECTOR` / `TURBOQUANT4_VECTOR`.
Standalone quantized columns are searchable but cannot provide an exact source
for reranking. For the recommended source-plus-accelerator layout, lifecycle
rules, and Python rescore API, see the
[Vector Quantization Guide](../VECTOR_QUANTIZATION_GUIDE.md). Float16 SIMD
details remain in the [Float16 Vector Guide](../FLOAT16_VECTOR_GUIDE.md).

## Explain

```sql
EXPLAIN SELECT * FROM users WHERE age > 30;
EXPLAIN ANALYZE SELECT city, COUNT(*) FROM users GROUP BY city;
```

`EXPLAIN` prints the candidate and chosen plan, including the index access spec
when one is executable. `EXPLAIN ANALYZE` also reports what actually ran:

- `Actual Path` — the physical route that executed;
- `Actual Rows` and `Actual Time` — measured result size and duration;
- `Plan Divergence` — a note when execution had to leave the planned route (for
  example, a planned index route that was unavailable at execution time);
- `Feedback Recorded` — whether this run's cost/time feedback was written to
  the table sidecar.

`EXPLAIN ANALYZE` persists per-shape cost and time feedback in the table
sidecar `<table>.plan_feedback`. Feedback is ignored when its sidecar schema
version or the recorded OS/architecture/parallelism fingerprint differs, and it
is dropped on schema changes.

Use `EXPLAIN` when you are checking whether a query is taking a fast path, using an index, or falling back to the full planner.

## Execution Limits

Aggregation state for supported `GROUP BY` kernels is charged against a
per-query byte budget. Set `APEX_QUERY_MEMORY_MB` to the budget in MiB (`0`
disables the budget, default `1024`). Exceeding it fails the query with an
out-of-memory error instead of letting the process exhaust memory; reservations
are released on success, cancellation, failure, and fallback.

Supported batched scans may use parallel row-group workers. At most
`min(CPU - 1, 8)` workers run across all queries in a process, and parallel
execution is selected only when the calibrated serial cost is at least 2 ms and
parallel history is still faster. `APEX_PARALLEL_SCAN` is a diagnostic
override: `0`/`1` forces serial, and `N >= 2` forces `N` workers.
