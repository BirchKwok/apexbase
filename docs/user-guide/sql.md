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
TopK vector query on its native FFI fast path. Arity and type mismatches raise
`ValueError`.

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

```sql
SELECT *
FROM explode_rename(
    topk_distance('items', 'embedding', '[0.1, 0.2, 0.3]', 10, 'cosine'),
    '_id,dist'
);
```

For Python batch search and float16 vector storage, see the [Float16 Vector Guide](../FLOAT16_VECTOR_GUIDE.md).

## Explain

```sql
EXPLAIN SELECT * FROM users WHERE age > 30;
EXPLAIN ANALYZE SELECT city, COUNT(*) FROM users GROUP BY city;
```

Use `EXPLAIN` when you are checking whether a query is taking a fast path, using an index, or falling back to the full planner.
