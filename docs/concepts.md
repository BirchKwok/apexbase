# Core Concepts

This page gives you the mental model for ApexBase before you dive into the API reference.

## Database Root

An `ApexClient` opens a root directory:

```python
from apexbase import ApexClient

client = ApexClient("./data")
```

The root directory contains the default database and any named databases. ApexBase stores each table as a `.apex` file.

For an ephemeral database, pass the special path `":memory:"`:

```python
client = ApexClient(":memory:")
```

This is a true process-local storage backend, not a temporary directory.
Tables still use the normal SQL, DDL, DML, index, FTS, schema, and result
paths, but no database, catalog, WAL, delta, blob-sidecar, or index files are
created. Its contents disappear when the client is closed and are not shared
with another independently created in-memory client.

## Databases

ApexBase supports multiple isolated databases under one root directory. The default database maps to the root directory. Named databases live in subdirectories.

```python
client.use_database("analytics")
client.use(database="analytics", table="events")

print(client.list_databases())
print(client.current_database)
```

SQL can refer to another database with `database.table` syntax:

```python
client.execute("""
    SELECT u.name, e.event
    FROM default.users u
    JOIN analytics.events e ON u.id = e.user_id
""")
```

## Tables

Tables are explicit. Create or select a table before using table-scoped methods such as `store()`, `retrieve_all()`, and `list_fields()`.

```python
client.create_table("users")
client.use_table("users")
```

You can also create tables with SQL:

```python
client.execute("CREATE TABLE IF NOT EXISTS users")
```

## Schemas

You may let ApexBase infer column types from the first write, or provide a schema up front for clearer contracts and faster bulk loading.

```python
client.create_table("orders", schema={
    "order_id": "int64",
    "customer": "string",
    "total": "float64",
    "paid": "bool",
})
```

## Records And Columns

ApexBase accepts row-oriented dictionaries, lists of dictionaries, and columnar dictionaries. For bulk ingest, columnar data is usually the fastest path.

```python
client.store({"name": "Alice", "age": 30})

client.store([
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35},
])

client.store({
    "name": ["Diana", "Eve"],
    "age": [28, 41],
})
```

Every stored row has an internal `_id`. SQL hides `_id` unless you request it explicitly.

## ResultView

Queries return a `ResultView`, which can convert to Python-native rows or columnar DataFrame formats.

```python
result = client.execute("SELECT * FROM users")

rows = result.to_dict()
pandas_df = result.to_pandas()
polars_df = result.to_polars()
arrow_table = result.to_arrow()
```

Use `ResultView` when you want to move smoothly between SQL, Python lists, Pandas, Polars, and PyArrow.

## Process-Local Default Connection

The module-level `apexbase.execute()` helper lazily creates one shared
in-memory client inside the current Python process:

```python
import apexbase

apexbase.execute("CREATE TABLE counters (name TEXT, value INT)")
apexbase.execute("INSERT INTO counters VALUES ('jobs', 3)")
value = apexbase.execute(
    "SELECT value FROM counters WHERE name = ?", ["jobs"]
).scalar()
```

Later module-level calls see the same tables. Use an explicit `ApexClient`
when you need lifecycle control, isolation, persistence, or more than one
database connection.

## Durability

Durability is configured when opening the client:

| Mode | Best for | Behavior |
| --- | --- | --- |
| `fast` | Local analytics, scratch data, benchmarks | Prioritizes throughput |
| `safe` | Application data with balanced speed and safety | Synchronous writes |
| `max` | Highest crash safety | Fsync on each write |

```python
client = ApexClient("./data", durability="safe")
```

## Interfaces

The same storage engine can be reached through several interfaces:

- Python API for embedded applications and notebooks.
- Rust embedded API for native Rust applications.
- PostgreSQL Wire server for SQL clients and database tools.
- Arrow Flight server for high-throughput columnar result streaming.

Start with the Python API unless you already know you need a wire protocol or Rust integration.
