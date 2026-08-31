# ApexBase API Reference

Complete API reference for ApexBase Python SDK.

## Table of Contents

1. [ApexClient](#apexclient) - Main client class
2. [Module-Level Execute](#module-level-execute) - Shared process-local SQL
3. [ResultView](#resultview) - Query results
4. [Constants](#constants) - Module constants
5. [File Reading Table Functions](#file-reading-table-functions) - read_csv / read_parquet / read_json
6. [Temporary Tables from Files](#temporary-tables-from-files) - register_temp_table / drop_temp_table
7. [Set Operations](#set-operations) - UNION / INTERSECT / EXCEPT
8. [Vector Search](#vector-search) - topk_distance / batch_topk_distance / SQL explode_rename

---

## ApexClient

The main entry point for ApexBase operations.

### Constructor

```python
ApexClient(
    dirpath: str = None,
    batch_size: int = 1000,
    drop_if_exists: bool = False,
    enable_cache: bool = True,
    cache_size: int = 10000,
    prefer_arrow_format: bool = True,
    durability: Literal['fast', 'safe', 'max'] = 'fast',
    _auto_manage: bool = True
)
```

**Parameters:**

- `dirpath`: Data directory path (default: current directory)
- `dirpath=":memory:"`: Create an isolated process-local database without filesystem persistence
- `batch_size`: Batch size for bulk operations
- `drop_if_exists`: If True, delete existing data on open
- `enable_cache`: Enable query result caching
- `cache_size`: Maximum cache entries
- `prefer_arrow_format`: Prefer Arrow format for internal transfers
- `durability`: Persistence level - 'fast' (async), 'safe' (sync), 'max' (fsync every write)

**Example:**
```python
from apexbase import ApexClient

# Basic usage
client = ApexClient("./data")

# With durability options
client = ApexClient("./data", durability="safe")

# Clean start (drop existing)
client = ApexClient.create_clean("./data")

# True in-memory database (no temporary directory or sidecar files)
client = ApexClient(":memory:")
```

---

## Module-Level Execute

```python
apexbase.execute(
    sql: str,
    params=None,
    *,
    show_internal_id: bool = None,
) -> ResultView
```

Execute SQL on a lazily initialized, process-local default in-memory
connection. Later calls in the same process reuse that connection and see its
tables. It creates no database, catalog, WAL, delta, blob-sidecar, or index
files.

`params` supports the same positional and named binding rules as
`ApexClient.execute()`. Use an explicit `ApexClient(":memory:")` when you need
an isolated lifecycle rather than the module-wide session.

```python
import apexbase

apexbase.execute("CREATE TABLE kv (key TEXT, value INT)")
apexbase.execute("INSERT INTO kv VALUES (?, ?)", ["answer", 42])
answer = apexbase.execute(
    "SELECT value FROM kv WHERE key = :key", {"key": "answer"}
).scalar()
```

---

### Database Management

ApexBase supports multiple isolated databases within a single root directory. Each named database is stored as a subdirectory; the `'default'` database maps to the root directory (backward-compatible).

#### use_database
```python
use_database(database: str = 'default') -> ApexClient
```
Switch to a named database. Creates the database subdirectory if it does not exist. Resets the current table to `None`.

**Parameters:**

- `database`: Database name. `'default'` (or `''`) maps to the root directory.

**Returns:** `self` (for method chaining)

**Examples:**
```python
# Switch to analytics database
client.use_database("analytics")

# Switch back to default (root-level tables)
client.use_database("default")

# Method chaining
client.use_database("hr").create_table("employees")
```

---

#### use
```python
use(database: str = 'default', table: str = None) -> ApexClient
```
Switch to a named database and optionally select or create a table in one call. If `table` is specified and does not exist it is created automatically.

**Parameters:**

- `database`: Database name (default = root-level).
- `table`: Table name to select. If `None`, only the database is switched.

**Returns:** `self` (for method chaining)

**Examples:**
```python
# Switch database only
client.use(database="analytics")

# Switch database and select an existing table
client.use(database="analytics", table="events")

# Switch database and auto-create table if missing
client.use(database="new_db", table="new_table")
client.store({"key": "value"})
```

---

#### list_databases
```python
list_databases() -> List[str]
```
Return a sorted list of all available databases. `'default'` is always included.

**Example:**
```python
dbs = client.list_databases()
print(dbs)  # ['analytics', 'default', 'hr']
```

---

#### current_database
```python
current_database: str  # Property
```
Return the name of the currently active database. Returns `'default'` when operating on root-level tables.

**Example:**
```python
client.use_database("analytics")
print(client.current_database)  # 'analytics'
```

---

### Cross-Database SQL

All SQL operations support the standard `database.table` qualified name syntax. The active database context only affects unqualified table references; qualified references always resolve to the correct database regardless of context.

**Supported operations:**
```python
# SELECT across databases
client.execute("SELECT * FROM default.users")
client.execute("SELECT * FROM analytics.events WHERE cnt > 10")

# JOIN across databases
client.execute("""
    SELECT u.name, e.event
    FROM default.users u
    JOIN analytics.events e ON u.id = e.user_id
""")

# INSERT into a different database
client.execute("INSERT INTO analytics.events (name, cnt) VALUES ('click', 1)")

# UPDATE in a different database
client.execute("UPDATE default.users SET age = 31 WHERE name = 'Alice'")

# DELETE from a different database
client.execute("DELETE FROM default.users WHERE age < 18")

# DDL across databases
client.execute("CREATE TABLE analytics.summary (total INT)")
client.execute("DROP TABLE IF EXISTS analytics.old_table")
```

---

### Table Management

#### create_table
```python
create_table(table_name: str, schema: dict = None) -> None
```
Create a new table, optionally with a pre-defined schema.

**Parameters:**

- `table_name`: Name of the table to create.
- `schema`: Optional dict mapping column names to type strings. Pre-defining schema avoids type inference on the first insert, providing a performance benefit for bulk loading.

**Supported types:** `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`, `float32`, `float64`, `bool`, `string`, `binary`, `blob`, `large_binary`

**Examples:**
```python
# Without schema
client.create_table("users")

# With pre-defined schema
client.create_table("orders", schema={
    "order_id": "int64",
    "product": "string",
    "price": "float64",
    "paid": "bool"
})
```

#### drop_table
```python
drop_table(table_name: str) -> None
```
Drop a table and all its data.

**Example:**
```python
client.drop_table("old_table")
```

#### use_table
```python
use_table(table_name: str) -> None
```
Switch to a different table for subsequent operations.

**Example:**
```python
client.use_table("users")
```

#### list_tables
```python
list_tables() -> List[str]
```
Return list of all table names.

**Example:**
```python
tables = client.list_tables()
print(tables)  # ['users', 'orders']
```

#### current_table
```python
current_table: Optional[str]  # Property
```
Get the name of the currently active table. Returns `None` if no table is selected.

**Example:**
```python
print(client.current_table)  # 'users'
```

---

### Data Storage

#### store
```python
store(data) -> None
```
Store data in the active table. Requires a table to be selected via `create_table()` or `use_table()` first. Accepts multiple formats:

- Single dict: `{"name": "Alice", "age": 30}`
- List of dicts: `[{"name": "A"}, {"name": "B"}]`
- Dict of columns: `{"name": ["A", "B"], "age": [20, 30]}`
- pandas DataFrame
- polars DataFrame  
- PyArrow Table

**Examples:**
```python
# Single record
client.store({"name": "Alice", "age": 30})

# Multiple records
client.store([
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35}
])

# Columnar format (fastest for bulk)
client.store({
    "name": ["David", "Eve"],
    "age": [28, 32]
})
```

#### from_pandas
```python
from_pandas(df: pd.DataFrame, table_name: str = None) -> ApexClient
```
Import data from pandas DataFrame. Returns self for chaining.

**Parameters:**

- `df`: pandas DataFrame to import
- `table_name`: Optional. If provided, auto-creates/selects the table before importing.

**Example:**
```python
import pandas as pd
df = pd.DataFrame({"name": ["A", "B"], "age": [20, 30]})
client.from_pandas(df, table_name="users")
```

#### from_polars
```python
from_polars(df: pl.DataFrame, table_name: str = None) -> ApexClient
```
Import data from polars DataFrame. Returns self for chaining.

**Parameters:**

- `df`: polars DataFrame to import
- `table_name`: Optional. If provided, auto-creates/selects the table before importing.

**Example:**
```python
import polars as pl
df = pl.DataFrame({"name": ["A", "B"], "age": [20, 30]})
client.from_polars(df, table_name="users")
```

#### from_pyarrow
```python
from_pyarrow(table: pa.Table, table_name: str = None) -> ApexClient
```
Import data from PyArrow Table. Returns self for chaining.

**Parameters:**

- `table`: PyArrow Table to import
- `table_name`: Optional. If provided, auto-creates/selects the table before importing.

**Example:**
```python
import pyarrow as pa
table = pa.table({"name": ["A", "B"], "age": [20, 30]})
client.from_pyarrow(table, table_name="users")
```

#### store_durable_one
```python
store_durable_one(data: dict) -> None
```
Persist one schema-stable row immediately when the durable fast path applies.
Falls back to `store()` plus `flush()` for unsupported cases, so the API
remains correct when the optimized delta path is unavailable.

**Example:**
```python
client.store_durable_one({"name": "Alice", "age": 30})
```

#### from_lance
```python
from_lance(
    uri,
    table_name: str = None,
    columns: Optional[List[str]] = None,
    filter=None,
    limit: Optional[int] = None,
    batch_size: Optional[int] = None,
    **dataset_options
) -> ApexClient
```
Import a Lance dataset into the current or named ApexBase table. Data is read
through Lance's Arrow path into ApexBase's columnar write path. `uri` may be a
Lance dataset URI/path or an object exposing `to_batches`. Extra keyword
arguments are forwarded to `lance.dataset`. Returns self for chaining.

**Example:**
```python
client.from_lance("my_lance_dir", table_name="items")
```

#### to_lance
```python
to_lance(
    uri,
    sql: str = None,
    mode: str = "create",
    show_internal_id: bool = False,
    **write_options
)
```
Export the current table or a SQL result to a Lance dataset. When `sql` is
omitted, exports `SELECT *` from the current table. `mode` is the Lance write
mode (default `"create"`); `show_internal_id` controls whether `_id` appears
when exporting a SQL result. Extra keyword arguments are forwarded to the Lance
writer. Returns the written dataset URI/path.

**Example:**
```python
client.to_lance("export_lance_dir")
client.to_lance("export_lance_dir", sql="SELECT name, age FROM users WHERE age > 18")
```

---

### Data Retrieval

#### execute
```python
execute(sql: str, show_internal_id: bool = None, params=None) -> ResultView
```
Execute SQL query and return results.

**Parameters:**

- `sql`: SQL statement (SELECT, INSERT, etc.)
- `show_internal_id`: If True, include _id column in results
- `params`: Optional bound parameters. Positional `?` placeholders use a list/tuple; named `:name`, `@name`, or `$name` placeholders use a dict. A list/tuple value expands to a comma-separated list for `IN (?)`.

**Example:**
```python
# Basic query (use your table name in FROM clause)
results = client.execute("SELECT * FROM users WHERE age > 25")

# Aggregation
results = client.execute("SELECT COUNT(*), AVG(age) FROM users")
count = results.scalar()

# With ordering and limits
results = client.execute("SELECT name, age FROM users ORDER BY age DESC LIMIT 10")

# Parameter binding
results = client.execute("SELECT * FROM users WHERE age > ?", params=[25])
results = client.execute(
    "SELECT * FROM users WHERE city = :city AND age >= :min_age",
    params={"city": "Beijing", "min_age": 18},
)
results = client.execute(
    "SELECT * FROM users WHERE city IN (?)", params=[["Beijing", "Shanghai"]]
)
```

Parameter binding works for SELECT, DML, DDL, and transaction statements. The
bound TopK vector query keeps its native FFI fast path.

#### execute_batch
```python
execute_batch(queries: List[str]) -> List[ResultView]
```
Execute a SQL script in list order with read-after-write visibility. Consecutive
simple `UPDATE ... SET <numeric column> = <literal> WHERE _id = N` statements
against the same table and column are coalesced into one native batch write;
all other statements run sequentially.

**Example:**
```python
results = client.execute_batch([
    "INSERT INTO users (name, age) VALUES ('Alice', 30)",
    "INSERT INTO users (name, age) VALUES ('Bob', 25)",
    "UPDATE users SET age = 31 WHERE _id = 1",
])
```

#### execute_batch_parallel
```python
execute_batch_parallel(queries: List[str]) -> List[ResultView]
```
Execute independent read-only SQL statements concurrently. Statements that may
mutate state are rejected, so callers cannot accidentally depend on
nondeterministic write ordering. Use `execute_batch` for ordered scripts that
must preserve statement order.

**Example:**
```python
results = client.execute_batch_parallel([
    "SELECT city, COUNT(*) FROM users GROUP BY city",
    "SELECT AVG(age) FROM users",
    "SELECT * FROM events ORDER BY ts DESC LIMIT 100",
])
```

#### query
```python
query(
    sql: str = None,
    where_clause: str = None,
    limit: int = None
) -> ResultView
```
Query with WHERE expression (backward compatibility).

**Example:**
```python
# WHERE expression only
results = client.query("age > 25")
results = client.query("name LIKE 'A%'")

# With limit
results = client.query(where_clause="city = 'NYC'", limit=100)
```

#### retrieve
```python
retrieve(id_: int) -> Optional[dict]
```
Get a single record by its internal _id.

**Example:**
```python
record = client.retrieve(1)
print(record)  # {'_id': 1, 'name': 'Alice', 'age': 30}
```

#### Blob helpers
```python
read_blob(column: str, id_: int) -> Optional[bytes]
read_blobs(column: str, ids: List[int]) -> List[Optional[bytes]]
read_blob_range(column: str, id_: int, offset: int = 0, length: int = None) -> Optional[bytes]
read_blob_ranges(column: str, ids: List[int], offsets: List[int], length: int = None) -> List[Optional[bytes]]
read_blob_descriptor(column: str, id_: int) -> Optional[bytes]
read_blob_info(column: str, id_: int) -> Optional[dict]
read_blob_infos(column: str, ids: List[int]) -> List[Optional[dict]]
```

`blob` / `large_binary` columns store compact descriptors in the main table and keep payload bytes in inline, packed sidecar, or dedicated sidecar storage. Use these helpers for point reads, metadata reads, and range reads without projecting the whole blob column through SQL.

**Example:**
```python
client.create_table("files", {"name": "string", "payload": "blob"})
client.store({"name": "clip.bin", "payload": b"...large bytes..."})
info = client.read_blob_info("payload", 1)
chunk = client.read_blob_range("payload", 1, offset=1024, length=4096)
```

#### retrieve_many
```python
retrieve_many(ids: List[int]) -> ResultView
```
Get multiple records by their internal _ids.

**Example:**
```python
results = client.retrieve_many([1, 2, 3, 6])
df = results.to_pandas()
```

#### retrieve_all
```python
retrieve_all() -> ResultView
```
Get all records from the current table.

**Example:**
```python
results = client.retrieve_all()
print(len(results))  # Total row count
```

#### count_rows
```python
count_rows(table_name: str = None) -> int
```
Count rows in a table.

**Example:**
```python
count = client.count_rows()
count = client.count_rows("users")  # Specific table
```

---

### Data Modification

#### replace
```python
replace(id_: int, data: dict) -> bool
```
Replace a record by _id.

**Example:**
```python
success = client.replace(0, {"name": "Alice", "age": 31})
```

#### batch_replace
```python
batch_replace(data_dict: Dict[int, dict]) -> List[int]
```
Batch replace multiple records.

**Example:**
```python
updated = client.batch_replace({
    0: {"name": "Alice", "age": 31},
    1: {"name": "Bob", "age": 26}
})
```

#### delete
```python
delete(id: Optional[Union[int, List[int]]] = None, where: Optional[str] = None) -> Union[bool, int]
```
Delete record(s) by `_id` or by a `WHERE` expression. Deleting by `id` returns a
bool indicating success; deleting by `where` returns the number of deleted rows.
At least one argument is required (safety protection against accidental full
deletes).

**Example:**
```python
# Single delete
client.delete(id=5)

# Batch delete
client.delete(id=[1, 2, 3])

# Conditional delete
client.delete(where="age < 18")
```

---

### Column Operations

#### add_column
```python
add_column(column_name: str, column_type: str) -> None
```
Add a new column to the current table.

**Types:** `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`, `Float64`, `String`, `Bool`

**Example:**
```python
client.add_column("email", "String")
client.add_column("score", "Float64")
```

#### drop_column
```python
drop_column(column_name: str) -> None
```
Drop a column from the current table. Cannot drop _id column.

**Example:**
```python
client.drop_column("temp_field")
```

#### rename_column
```python
rename_column(old_column_name: str, new_column_name: str) -> None
```
Rename a column. Cannot rename _id column.

**Example:**
```python
client.rename_column("email", "email_address")
```

#### get_column_dtype
```python
get_column_dtype(column_name: str) -> str
```
Get the data type of a column.

**Example:**
```python
dtype = client.get_column_dtype("age")  # 'Int64'
```

#### list_fields
```python
list_fields() -> List[str]
```
List all column names in the current table.

**Example:**
```python
fields = client.list_fields()
print(fields)  # ['_id', 'name', 'age', 'city']
```

---

### Full-Text Search

FTS is implemented natively in Rust and available through all interfaces (Python API, PG Wire, Arrow Flight). The recommended way to manage and query FTS indexes is via SQL.

#### FTS SQL DDL and Query Reference

| Statement | Description |
|-----------|-------------|
| `CREATE FTS INDEX ON table (col1, col2)` | Create FTS index on specified columns |
| `CREATE FTS INDEX ON table` | Create FTS index on all string columns |
| `CREATE FTS INDEX ON table WITH (opt=val)` | Create with options |
| `DROP FTS INDEX ON table` | Drop index and delete files |
| `ALTER FTS INDEX ON table DISABLE` | Suspend indexing, keep files |
| `ALTER FTS INDEX ON table ENABLE` | Resume indexing and back-fill any missed rows |
| `SHOW FTS INDEXES` | List FTS-configured tables across all databases |
| `WHERE MATCH('query')` | Exact full-text search |
| `WHERE FUZZY_MATCH('query')` | Fuzzy / typo-tolerant search |
| `FTS_SCORE()` | BM25 score for the statement's unique `MATCH()` query |
| `FTS_SCORE('query')` | BM25 score for an explicit query |

**`CREATE FTS INDEX`**
```sql
CREATE FTS INDEX ON table_name [(col1, col2, ...)] [WITH (lazy_load=bool, cache_size=N)]
```

- `(col1, col2)` — optional column list; omit to index all string columns
- `lazy_load` — mmap the v3 term directory and decode postings on demand (default `false`)
- `cache_size` — maximum decoded postings retained in lazy mode (default `10000`)

```python
client.execute("CREATE FTS INDEX ON articles (title, content)")
client.execute("CREATE FTS INDEX ON logs WITH (lazy_load=true, cache_size=50000)")
```

**`DROP FTS INDEX`**
```sql
DROP FTS INDEX ON table_name
```
Removes the index entry and deletes the ApexFTS `.afts` snapshot and WAL from disk.

**`ALTER FTS INDEX ... DISABLE`**
```sql
ALTER FTS INDEX ON table_name DISABLE
```
Suspends FTS write-sync (INSERT / DELETE no longer update the index). Index files are kept on disk. Use `ALTER FTS INDEX ... ENABLE` to resume.

**`ALTER FTS INDEX ... ENABLE`**
```sql
ALTER FTS INDEX ON table_name ENABLE
```
Resumes FTS write-sync and **back-fills all rows** currently in the table, including any rows inserted while FTS was disabled.

**`SHOW FTS INDEXES`**
```sql
SHOW FTS INDEXES
```
Returns a result set with columns: `database`, `table`, `enabled`, `fields`, `lazy_load`, `cache_size`. Lists indexes across the root directory and all named sub-databases.

```python
result = client.execute("SHOW FTS INDEXES")
df = result.to_pandas()
#   database     table  enabled          fields  lazy_load  cache_size
#    default  articles     True  title, content      False       10000
```

**`MATCH('query')`**

Used in `WHERE` clauses to filter rows whose indexed text contains all query terms.

```python
# Simple search
client.execute("SELECT * FROM articles WHERE MATCH('python tutorial')")

# Combined with other conditions
client.execute("""
    SELECT title, content FROM articles
    WHERE MATCH('machine learning') AND year >= 2023
    ORDER BY _id DESC
    LIMIT 10
""")

# Aggregations
client.execute("SELECT COUNT(*) FROM articles WHERE MATCH('rust')")
```

**`FTS_SCORE([query])`**

Projects or orders by BM25 relevance. With no argument it binds to the unique `MATCH()` query in the statement; otherwise pass one string literal explicitly.

```python
client.execute("""
    SELECT title, FTS_SCORE() AS relevance
    FROM articles
    WHERE MATCH('database')
    ORDER BY relevance DESC
""")
```

**`FUZZY_MATCH('query')`**

Like `MATCH()` but tolerates typos and spelling variations.

```python
client.execute("SELECT * FROM articles WHERE FUZZY_MATCH('pytohn')")   # matches 'python'
client.execute("SELECT * FROM articles WHERE FUZZY_MATCH('databse')")  # matches 'database'
```

> **Note:** `MATCH()` / `FUZZY_MATCH()` require a FTS index to exist for the queried table. Use `CREATE FTS INDEX ON table` first. The SQL interface works over all transports (Python, PG Wire, Arrow Flight).

---

#### Python API

#### init_fts
```python
init_fts(
    table_name: str = None,
    index_fields: Optional[List[str]] = None,
    lazy_load: bool = False,
    cache_size: int = 10000
) -> ApexClient
```
Initialize full-text search for a table.

**Parameters:**

- `table_name`: Table to index (default: current table)
- `index_fields`: Fields to index (None = all string fields)
- `lazy_load`: Load index on first search
- `cache_size`: FTS cache size

**Example:**
```python
client.init_fts(index_fields=["title", "content"])
client.init_fts(index_fields=["name"], lazy_load=True)
```

#### search_text
```python
search_text(query: str, table_name: str = None) -> np.ndarray
```
Search for documents containing query terms. Returns array of _ids.

**Example:**
```python
ids = client.search_text("database")
print(ids)  # array([0, 5, 10])
```

IDs are ordered by BM25 relevance. A query wrapped in double quotes performs an exact phrase search:

```python
ids = client.search_text('"distributed database"')
```

#### search_text_with_scores
```python
search_text_with_scores(
    query: str,
    table_name: str = None,
    limit: int = 1000
) -> List[Tuple[int, float]]
```
Return BM25-ranked document IDs together with their scores.

```python
hits = client.search_text_with_scores("database", limit=20)
```

#### fuzzy_search_text
```python
fuzzy_search_text(
    query: str,
    min_results: int = 1,
    table_name: str = None
) -> np.ndarray
```
Fuzzy search tolerating typos. Returns array of _ids.

**Example:**
```python
ids = client.fuzzy_search_text("databse")  # Matches "database"
```

#### search_and_retrieve
```python
search_and_retrieve(
    query: str,
    table_name: str = None,
    limit: Optional[int] = None,
    offset: int = 0
) -> ResultView
```
Search and return full records.

**Example:**
```python
results = client.search_and_retrieve("python")
results = client.search_and_retrieve("python", limit=10, offset=20)
```

#### search_and_retrieve_top
```python
search_and_retrieve_top(
    query: str,
    n: int = 100,
    table_name: str = None
) -> ResultView
```
Return top N search results.

**Example:**
```python
results = client.search_and_retrieve_top("important", n=5)
```

#### get_fts_stats
```python
get_fts_stats(table_name: str = None) -> Dict
```
Get FTS statistics.

**Example:**
```python
stats = client.get_fts_stats()
print(stats)  # {'fts_enabled': True, 'doc_count': 1000, 'term_count': 5000}
```

#### set_fts_fuzzy_config
```python
set_fts_fuzzy_config(
    threshold: float = 0.7,
    max_distance: int = 2,
    max_candidates: int = 20,
    table_name: str = None
) -> None
```
Configure fuzzy matching parameters for the table's FTS engine. `threshold` is
the similarity threshold in `[0, 1]`, `max_distance` is the maximum edit
distance, and `max_candidates` limits the fuzzy candidate terms.

**Example:**
```python
client.set_fts_fuzzy_config(threshold=0.75, max_distance=1)
```

#### compact_fts_index
```python
compact_fts_index(table_name: str = None) -> None
```
Compact the FTS index for a table, merging pending WAL updates into the
snapshot.

**Example:**
```python
client.compact_fts_index()
```

#### warmup_fts_terms
```python
warmup_fts_terms(terms: List[str], table_name: str = None) -> int
```
Warm FTS caches for the given terms. Returns the number of terms warmed, or `0`
if FTS is not initialized.

**Example:**
```python
warmed = client.warmup_fts_terms(["python", "database", "rust"])
```

#### disable_fts
```python
disable_fts(table_name: str = None) -> ApexClient
```
Disable FTS (keeps index files).

**Example:**
```python
client.disable_fts()
```

#### drop_fts
```python
drop_fts(table_name: str = None) -> ApexClient
```
Disable FTS and delete index files.

**Example:**
```python
client.drop_fts()
```

---

### Utility Methods

#### flush
```python
flush() -> None
```
Flush all pending writes to disk.

**Example:**
```python
client.flush()
```

#### set_auto_flush
```python
set_auto_flush(rows: int = 0, bytes: int = 0) -> None
```
Set auto-flush thresholds.

**Example:**
```python
client.set_auto_flush(rows=1000)  # Flush every 1000 rows
client.set_auto_flush(bytes=1024*1024)  # Flush every 1MB
```

#### get_auto_flush
```python
get_auto_flush() -> tuple
```
Get current auto-flush configuration.

**Example:**
```python
rows, bytes = client.get_auto_flush()
```

#### set_compression
```python
set_compression(compression: str) -> bool
```
Set compression type for the current table. Only effective on empty tables
(`row_count == 0`); ignored if the table already contains data. The setting
persists across restarts. Valid values are `"none"`, `"lz4"`, and `"zstd"`.
Returns `True` if applied, `False` for a non-empty no-op.

**Example:**
```python
client.set_compression("zstd")
```

#### get_compression
```python
get_compression() -> str
```
Get the current compression type for the current table (`"none"`, `"lz4"`, or
`"zstd"`).

**Example:**
```python
compression = client.get_compression()
```

#### begin_buffered_writes
```python
begin_buffered_writes(flush_rows: int = 0) -> None
```
Enable explicit client-local buffered single-row writes. Rows become
durable/visible after `flush_buffered_writes()`, `flush()`, or `close()`.
Trades immediate visibility for lower per-row Python overhead in OLTP-style
append bursts. `flush_rows` auto-flushes after that many buffered rows
(`0` disables).

**Example:**
```python
client.begin_buffered_writes(flush_rows=1000)
```

#### end_buffered_writes
```python
end_buffered_writes(flush: bool = True) -> None
```
Disable buffered writes. With `flush=True` (default), pending rows are flushed
first; with `flush=False` they are discarded.

#### flush_buffered_writes
```python
flush_buffered_writes() -> int
```
Flush pending buffered single-row writes. Returns the number of rows flushed.

**Example:**
```python
flushed = client.flush_buffered_writes()
```

#### buffered_write_count
```python
buffered_write_count() -> int
```
Return the number of pending client-local buffered rows.

#### optimize
```python
optimize() -> None
```
Best-effort optimization hook; currently flushes pending writes.

#### flush_cache
```python
flush_cache() -> None
```
Alias for `flush()` (legacy name).

#### estimate_memory_bytes
```python
estimate_memory_bytes() -> int
```
Estimate current memory usage in bytes.

**Example:**
```python
mem_bytes = client.estimate_memory_bytes()
print(f"Using {mem_bytes / 1024 / 1024:.1f} MB")
```

#### close
```python
close() -> None
```
Close the client and release resources.

**Example:**
```python
client.close()
```

---

## ResultView

Container for query results with multiple output formats.

### Conversion Methods

#### to_pandas
```python
to_pandas(zero_copy: bool = True) -> pd.DataFrame
```
Convert to pandas DataFrame.

**Parameters:**

- `zero_copy`: Use ArrowDtype for zero-copy (pandas 2.0+)

**Example:**
```python
df = results.to_pandas()
df = results.to_pandas(zero_copy=False)  # Traditional NumPy types
```

#### to_polars
```python
to_polars() -> pl.DataFrame
```
Convert to polars DataFrame.

**Example:**
```python
df = results.to_polars()
```

#### to_arrow
```python
to_arrow() -> pa.Table
```
Convert to PyArrow Table.

**Example:**
```python
table = results.to_arrow()
```

#### to_dict
```python
to_dict() -> List[dict]
```
Convert to list of dictionaries.

**Example:**
```python
records = results.to_dict()
for record in records:
    print(record["name"])
```

### Access Methods

#### scalar
```python
scalar() -> Any
```
Get single scalar value (for aggregate queries).

**Example:**
```python
count = client.execute("SELECT COUNT(*) FROM users").scalar()
```

#### first
```python
first() -> Optional[dict]
```
Get first row as dictionary.

**Example:**
```python
row = results.first()
```

#### get_ids
```python
get_ids(return_list: bool = False) -> Union[np.ndarray, List[int]]
```
Get internal _ids from results.

**Example:**
```python
ids = results.get_ids()  # numpy array (default)
ids = results.get_ids(return_list=True)  # Python list
```

### Properties

#### shape
```python
shape: tuple  # (rows, columns)
```

#### columns
```python
columns: List[str]
```

### Sequence Interface

```python
# Length
len(results)

# Iteration
for row in results:
    print(row)

# Indexing
first = results[0]
second = results[1]
```

---

## Constants

### Module Constants

```python
from apexbase import (
    __version__,      # Package version
    FTS_AVAILABLE,    # True (FTS always available)
    ARROW_AVAILABLE,  # True if pyarrow installed
    POLARS_AVAILABLE, # True if polars installed
    DurabilityLevel,  # Type hint: Literal['fast', 'safe', 'max']
)
```

---

## SQL Support

ApexBase supports standard SQL for querying.

### Quoted Identifiers

When a column name collides with a SQL reserved keyword (e.g. `order`, `group`, `select`, `table`), wrap it in **backticks** (Hive/MySQL style) or **double quotes** (SQL standard) so the parser treats it as an identifier instead of a keyword.

| Style | Syntax | Example |
|-------|--------|---------|
| Backtick (Hive/MySQL) | `` `column` `` | `` SELECT `order`, `group` FROM t `` |
| Double-quote (SQL standard) | `"column"` | `SELECT "order", "group" FROM t` |

Both styles produce identical results — use whichever you prefer.

**Examples:**

```sql
-- Backtick style
SELECT `order`, `group`, `select` FROM orders WHERE `order` > 100 ORDER BY `order` DESC

-- Double-quote style
SELECT "order", "group", "select" FROM orders WHERE "order" > 100 ORDER BY "order" DESC

-- Mixed (both styles in one query)
SELECT `order`, "group" FROM orders

-- Works in all clauses: SELECT, WHERE, ORDER BY, GROUP BY, HAVING, INSERT, etc.
INSERT INTO t (`order`, `group`) VALUES (1, 'A')
SELECT `group`, COUNT(*) FROM t GROUP BY `group` HAVING COUNT(*) > 5
```

> **Tip:** Quoting is only needed when the column name is a reserved keyword. Regular column names like `name`, `age`, `city` do not need quoting.

### Parameter Binding

Pass `params` to `execute()` to bind values instead of interpolating strings.
Positional `?` placeholders consume values from a list/tuple; named
`:name`, `@name`, or `$name` placeholders read from a dict. A list/tuple value
expands to a comma-separated list for `IN (?)`.

```python
# Positional
client.execute("SELECT * FROM users WHERE age > ? AND city = ?", params=[30, "Beijing"])

# Named
client.execute(
    "INSERT INTO events (kind, ts) VALUES (:kind, :ts)",
    params={"kind": "click", "ts": "2026-08-07 12:00:00"},
)

# IN-list expansion
client.execute("SELECT * FROM users WHERE city IN (?)", params=[["Beijing", "Shanghai"]])

# Bound TopK vector query (keeps the native FFI fast path)
client.execute(
    "SELECT explode_rename(topk_distance(embedding, ?, 10, 'cosine'), '_id', 'dist') FROM items",
    params=[query_vector],
)
```

Arity and type mismatches raise `ValueError` instead of silently producing a
wrong query. Placeholders inside string/identifier literals and comments are
not substituted.

### SELECT
```sql
SELECT * FROM table
SELECT col1, col2 FROM table
SELECT col1 AS alias FROM table
SELECT DISTINCT col1 FROM table
SELECT * FROM table WHERE condition
SELECT * FROM table ORDER BY col DESC
SELECT * FROM table LIMIT 100
SELECT * FROM table LIMIT 100 OFFSET 10
SELECT * FROM table ORDER BY col LIMIT 100
```

### Aggregate Functions
```sql
SELECT COUNT(*) FROM table
SELECT COUNT(DISTINCT col) FROM table
SELECT SUM(col), AVG(col), MAX(col), MIN(col) FROM table
```

### WHERE Clauses
```sql
WHERE col = value
WHERE col > value
WHERE col LIKE 'pattern%'
WHERE col IN (1, 2, 3)
WHERE col BETWEEN 10 AND 20
WHERE col IS NULL
WHERE col IS NOT NULL
WHERE condition1 AND condition2
WHERE condition1 OR condition2
```

### GROUP BY / HAVING
```sql
SELECT category, COUNT(*), AVG(price) 
FROM products 
GROUP BY category

SELECT category, COUNT(*) 
FROM products 
GROUP BY category 
HAVING COUNT(*) > 10
```

For supported conjunctive numeric/string filters, ApexBase can route the scan
through a shared base/delta-aware physical pipeline before grouping. `HAVING`
is evaluated after aggregation and before ordered TopK/`LIMIT`. Queries outside
the physical protocol—including wide integer bounds that cannot be represented
exactly—retain the general SQL execution path with the same public semantics.

See [Scan & Physical Execution](SCAN_EXECUTION_ARCHITECTURE.md) for contributor
details and current fallback boundaries.

### JOINs
```sql
SELECT * FROM table1 JOIN table2 ON table1.id = table2.id
```

---

## File Reading Table Functions

ApexBase provides SQL table functions that read external files directly in a `FROM` clause without importing data into a table first. All three functions return a result compatible with the full SQL engine — you can apply `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, `JOIN`, and `UNION` on top of them.

### read_csv

```sql
SELECT * FROM read_csv('path/to/file.csv')
SELECT * FROM read_csv('path/to/file.csv', header=true, delimiter=',')
SELECT * FROM read_csv('path/to/file.csv', on_bad_lines='skip')
```

**Parameters:**

| Option | Default | Description |
|--------|---------|-------------|
| `header` | `true` | Whether the first row is a header. Set to `false` / `0` if the file has no header row. |
| `delimiter` / `delim` / `sep` | `,` | Field delimiter character. Use `'\t'` for TSV files. |
| `on_bad_lines` | `error` | Malformed-row policy: `error`, `skip`, or `warn`. |
| `ignore_errors` | `false` | Compatibility alias: `true` is equivalent to `on_bad_lines='skip'`. |

**Schema inference**: types are inferred automatically from the first 100 data rows (Int64, Float64, Bool, or String).

**Examples:**

```python
# Read a comma-delimited CSV with header row
result = client.execute("SELECT * FROM read_csv('/data/sales.csv')")

# Tab-separated values (TSV)
result = client.execute("SELECT * FROM read_csv('/data/data.tsv', delimiter='\t')")

# No header row
result = client.execute("SELECT * FROM read_csv('/data/raw.csv', header=false)")

# Full SQL on top of the file
result = client.execute("""
    SELECT city, COUNT(*) AS cnt, AVG(price)
    FROM read_csv('/data/orders.csv')
    WHERE price > 100
    GROUP BY city
    ORDER BY cnt DESC
    LIMIT 10
""")

# Convert directly to DataFrame
df = client.execute("SELECT * FROM read_csv('/data/large.csv')").to_pandas()
```

### read_parquet

```sql
SELECT * FROM read_parquet('path/to/file.parquet')
```

No options — schema is read directly from the Parquet file's metadata.

**Examples:**

```python
# Read a Parquet file
result = client.execute("SELECT * FROM read_parquet('/data/events.parquet')")

# Projection and filter
result = client.execute("""
    SELECT user_id, SUM(amount) AS total
    FROM read_parquet('/data/transactions.parquet')
    WHERE category = 'food'
    GROUP BY user_id
""")

# Zero-copy to Arrow
table = client.execute("SELECT * FROM read_parquet('/data/wide.parquet')").to_arrow()
```

### read_json

```sql
SELECT * FROM read_json('path/to/file.json')
```

Handles two formats automatically:

- **NDJSON / JSON Lines** — one JSON object per line (`.json`, `.jsonl`, `.ndjson`)
- **pandas column-oriented JSON** — output of `df.to_json(orient='columns')` or `orient='split'`

No options — format is detected automatically.

**Examples:**

```python
# Read NDJSON (one JSON object per line)
result = client.execute("SELECT * FROM read_json('/data/logs.ndjson')")

# Read pandas-exported JSON
result = client.execute("SELECT * FROM read_json('/data/export.json')")

# Apply aggregation
result = client.execute("""
    SELECT status, COUNT(*) AS cnt
    FROM read_json('/data/events.json')
    GROUP BY status
    ORDER BY cnt DESC
""")
```

### Joining file reads with tables

```python
# JOIN a file read with a stored table
result = client.execute("""
    SELECT u.name, f.score
    FROM users u
    JOIN read_csv('/data/scores.csv') f ON u.id = f.user_id
    WHERE f.score > 90
""")

# UNION a file with a table
result = client.execute("""
    SELECT name, city FROM users
    UNION ALL
    SELECT name, city FROM read_csv('/data/new_users.csv')
""")
```

### Performance notes

- All three functions use `mmap` for zero-copy file access.
- CSV and JSON files are parsed in parallel (one chunk per CPU core via Rayon).
- Parquet files use parallel column decoding with shared metadata (zero re-parse overhead).
- Benchmarked against Polars on 1M rows: CSV 0.95×, NDJSON 0.93×, Parquet 1.33× (Arrow output).

---

## Temporary Tables from Files

Register external data files (CSV, JSON, Parquet) as temporary native tables. The file is parsed once and materialized into ApexBase's mmap-backed `.apex` format, stored in a `.apex_tmp/` subdirectory. Subsequent queries bypass file parsing entirely — leveraging zone maps, bloom filters, and zero-copy mmap reads for **order-of-magnitude speedups** over repeated `read_csv()` / `read_json()` / `read_parquet()` calls. Temp tables are automatically cleaned up on client close.

### `register_temp_table(name, file_path, on_bad_lines="error")`

```python
client.register_temp_table(name: str, file_path: str, on_bad_lines: str = "error")
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Name for the temporary table |
| `file_path` | `str` | Path to the data file |
| `on_bad_lines` | `str` | CSV-only malformed-row policy: `error`, `skip`, or `warn` |

**Supported formats** (auto-detected by file extension):

| Extension | Format | Parser |
|-----------|--------|--------|
| `.csv`, `.tsv` | CSV / TSV | Parallel mmap CSV parser |
| `.json`, `.ndjson`, `.jsonl` | JSON / NDJSON | Parallel mmap JSON parser |
| `.parquet` | Parquet | Parallel column decode with shared metadata |

**Examples:**

```python
# CSV — most common use case
client.register_temp_table("sales", "/data/sales.csv")

# Import a dirty CSV while retaining every valid row
client.register_temp_table(
    "catalog", "/data/catalog.csv", on_bad_lines="skip"
)

# JSON / NDJSON
client.register_temp_table("logs", "/data/events.ndjson")

# Parquet
client.register_temp_table("users_parquet", "/data/users.parquet")

# Query the temp table like any persistent table
result = client.execute("SELECT * FROM sales WHERE amount > 1000")
count = client.execute("SELECT COUNT(*) FROM sales WHERE category = 'Electronics'").scalar()

# Full SQL — JOINs, aggregations, window functions all work
top = client.execute("""
    SELECT city, SUM(amount) AS total
    FROM sales
    GROUP BY city
    ORDER BY total DESC
    LIMIT 10
""")
```

### `drop_temp_table(name)`

```python
client.drop_temp_table(name: str)
```

Explicitly drop a temp table before client close. Temp tables are also cleaned up automatically when the client is closed or garbage-collected.

```python
client.drop_temp_table("sales")
```

### `CREATE TEMP TABLE` (SQL syntax)

```python
client.execute("CREATE TEMP TABLE invoices AS SELECT * FROM read_csv('/data/invoices.csv')")
```

This SQL syntax produces the same result as `register_temp_table()` — the table is created in the temp directory and queries resolve to it automatically.

### Performance & memory characteristics

| Aspect | `read_csv()` (direct) | `register_temp_table()` (temp) |
|--------|----------------------|-------------------------------|
| First query | Parse CSV → Arrow batch | Parse CSV → write `.apex` → read `.apex` |
| Subsequent queries | Re-parse CSV every time | Read from mmap-backed `.apex` (zero parse) |
| Filtered queries | Full scan every time | Zone maps skip irrelevant row groups |
| Point lookups | Linear scan | Bloom filter acceleration |
| Memory usage | Full batch in RAM | mmap — near-zero RAM (data on disk) |
| Aggregate speed | Re-parse + aggregate | mmap + SIMD aggregate on columnar data |

For workloads that query the same file multiple times, `register_temp_table()` provides **10–100× speedup** on subsequent queries compared to repeated `read_csv()` / `read_json()`.

---

## Set Operations

SQL set operations combine result sets from two or more `SELECT` statements. Both sides must return the same number of columns.

### UNION

Combines rows from both sides and **removes duplicate rows**.

```sql
SELECT col FROM table_a
UNION
SELECT col FROM table_b
```

### UNION ALL

Combines rows from both sides and **keeps all rows**, including duplicates.

```sql
SELECT col FROM table_a
UNION ALL
SELECT col FROM table_b
```

### INTERSECT

Returns only rows that appear in **both** result sets (deduplicated).

```sql
SELECT col FROM table_a
INTERSECT
SELECT col FROM table_b
```

### EXCEPT

Returns rows from the **left** side that do **not** appear in the right side (deduplicated).

```sql
SELECT col FROM table_a
EXCEPT
SELECT col FROM table_b
```

### Ordering and limiting set operation results

Append `ORDER BY`, `LIMIT`, and `OFFSET` after the final set operation; they apply to the combined result:

```sql
SELECT val FROM a
UNION
SELECT val FROM b
ORDER BY val ASC
LIMIT 100
```

### Examples

```python
# UNION: unique names across two tables
client.execute("""
    SELECT name FROM customers
    UNION
    SELECT name FROM leads
    ORDER BY name
""")

# UNION ALL: all rows from both tables (duplicates kept)
client.execute("""
    SELECT name, city FROM domestic_users
    UNION ALL
    SELECT name, city FROM international_users
""")

# INTERSECT: users who appear in both the orders and wishlist tables
client.execute("""
    SELECT user_id FROM orders
    INTERSECT
    SELECT user_id FROM wishlist
""")

# EXCEPT: users who placed orders but have no open support tickets
client.execute("""
    SELECT user_id FROM orders
    EXCEPT
    SELECT user_id FROM support_tickets WHERE status = 'open'
""")

# Works with read_csv too
client.execute("""
    SELECT email FROM users
    EXCEPT
    SELECT email FROM read_csv('/data/unsubscribed.csv')
""")
```

### Set operation summary

| Operation | Duplicates | Rows returned |
|-----------|-----------|---------------|
| `UNION` | removed | left ∪ right (unique) |
| `UNION ALL` | kept | all rows from both sides |
| `INTERSECT` | removed | left ∩ right |
| `EXCEPT` | removed | left \ right |

---

## Vector Search

ApexBase has a built-in vector similarity search engine implemented entirely in Rust with SIMD-accelerated distance kernels and an OS-level mmap scan buffer that is populated once and reused across queries. Both single-query and batch modes are available through dedicated Python methods and through a SQL extension syntax.

### Vector column storage

Vectors are stored as **FixedList** columns when inserted as numpy arrays or numeric Python lists/tuples. For embedding-heavy tables, declare the column as `FLOAT16_VECTOR` to store half-precision values while keeping the same SQL distance functions.

```python
import numpy as np
from apexbase import ApexClient

client = ApexClient("./vecdb")
client.create_table("items")

# numpy arrays → FixedList column
client.store({
    "label": ["a", "b", "c"],
    "vec":   [np.random.rand(128).astype(np.float32) for _ in range(3)],
})

# Python list/tuple vectors are also accepted
client.store({"label": "d", "vec": [0.1, 0.2, 0.3]})
```

---

### Float16 Vector Storage (`FLOAT16_VECTOR`)

ApexBase supports **half-precision (float16) vector columns** for memory-efficient embedding storage.  Each element is stored as a 16-bit IEEE 754 half-precision float, halving the memory and I/O footprint compared to float32 vectors.

#### Declaring a float16 column

```sql
CREATE TABLE embeddings (id TEXT, vec FLOAT16_VECTOR)
```

Accepted type name aliases: `FLOAT16_VECTOR`, `FLOAT16VECTOR`, `F16_VECTOR`.

#### Inserting float16 vectors

Use batch store for best throughput. Pass vectors as `numpy` arrays or numeric Python lists — any numeric dtype is accepted; the storage layer converts to float16 automatically.

```python
import numpy as np
from apexbase import ApexClient

client = ApexClient("./vecdb")
client.execute("CREATE TABLE embeddings (label TEXT, vec FLOAT16_VECTOR)")
client.use_table("embeddings")

# float32 source data — auto-quantized to f16 on write
vecs = np.random.rand(1000, 128).astype(np.float32)
client.store([{"label": str(i), "vec": vecs[i]} for i in range(len(vecs))])

# Python lists also work, including single-record writes
client.store({"label": "queryable", "vec": [0.1, 0.2, 0.3, 0.4]})

# float16 source data — stored directly
vecs_f16 = vecs.astype(np.float16)
client.store([{"label": str(i), "vec": vecs_f16[i]} for i in range(len(vecs_f16))])
```

#### Querying float16 vectors

All `topk_distance`, `batch_topk_distance`, and SQL distance functions work transparently on `FLOAT16_VECTOR` columns.  The query vector is always provided in float32/float64 — no special handling needed.

```python
query = np.random.rand(128).astype(np.float32)

# TopK search
results = client.topk_distance("vec", query, k=10, metric="l2")

# SQL distance functions also work
client.execute("""
    SELECT label, array_distance(vec, [0.1, 0.2, 0.3, 0.4]) AS dist
    FROM embeddings
    ORDER BY dist
    LIMIT 5
""")
```

#### SIMD acceleration

Float16 distance kernels are hardware-accelerated on supported CPUs:

| Architecture | Feature | Kernels |
|---|---|---|
| `aarch64` (Apple M-series, AWS Graviton) | `fp16` NEON | L2, Dot, Cosine, L1, L∞ — FCVTL/FCVTL2 |
| `x86_64` | `f16c` + `AVX2` | L2, Dot, Cosine, L1, L∞ — `_cvtph_ps` |
| All others | scalar fallback | all metrics |

CPU feature detection is automatic at runtime — no build flags or environment variables required.  On Apple Silicon (M1/M2/M3/M4), f16 kernels are typically **≥2× faster** than equivalent float32 kernels.

#### Quantization error

Float16 has ~3 decimal digits of precision (machine epsilon ≈ 9.77 × 10⁻⁴).  For unit vectors or typical embedding ranges [−1, 1], the relative distance error is under 0.2%.

```python
import numpy as np

def f16_quantize(v):
    return v.astype(np.float16).astype(np.float32)

vec = np.random.rand(128).astype(np.float32)
q   = np.random.rand(128).astype(np.float32)

exact    = float(np.sqrt(np.sum((vec - q) ** 2)))
f16_dist = float(np.sqrt(np.sum((f16_quantize(vec) - q) ** 2)))
print(f"relative error: {abs(exact - f16_dist) / exact:.2e}")  # typically < 2e-3
```

---

### Quantized Vector Storage and Exact Rescore

ApexBase supports `FLOAT32_VECTOR`, `FLOAT16_VECTOR`, `BFLOAT16_VECTOR`,
`INT8_VECTOR`, `UINT8_VECTOR`, `BIT1_VECTOR`, and
`TURBOQUANT2_VECTOR` / `TURBOQUANT3_VECTOR` / `TURBOQUANT4_VECTOR`.
For retrieval systems, keep the source vector and create a separate stored
accelerator so the compressed scan can be followed by source-vector reranking:

```python
client.create_quantized_column(
    source="vec",
    target="vec_tq4",
    codec="turboquant4",
)

results = client.topk_distance(
    "vec",
    query,
    k=10,
    accelerator="vec_tq4",
    candidate_k=80,
    rescore=True,
)

# Physically removes vec_tq4 and its dependency metadata; vec is preserved.
client.drop_quantized_column("vec_tq4")
```

The accelerator is backfilled for existing rows and automatically regenerated
for later inserts and source replacements. Direct accelerator writes are
rejected. A source column cannot be dropped while an accelerator depends on it,
and `drop_quantized_column()` rejects ordinary, non-derived columns.

See the [Vector Quantization Guide](VECTOR_QUANTIZATION_GUIDE.md) for codec sizes,
tradeoffs, standalone quantized columns, and lifecycle details.

---

### topk_distance

```python
topk_distance(
    col: str,
    query,
    k: int = 10,
    metric: str = 'l2',
    id_col: str = '_id',
    dist_col: str = 'dist',
    accelerator: str | None = None,
    candidate_k: int | None = None,
    rescore: bool = True,
) -> ResultView
```

Heap-based nearest-neighbour search: O(n log k), significantly faster than `ORDER BY distance LIMIT k` for large tables.

**Parameters:**

- `col`: Name of the vector column to search (FixedList or Binary).
- `query`: Query vector — list, tuple, or numpy array of floats.
- `k`: Number of nearest neighbours to return (default `10`).
- `metric`: Distance metric (see table below).
- `id_col`: Column name for the returned row IDs (default `'_id'`).
- `dist_col`: Column name for the returned distances (default `'dist'`).
- `accelerator`: Optional system-maintained quantized column for candidate generation.
- `candidate_k`: Approximate candidate count before reranking; defaults to `max(8 * k, 64)` and must be at least `k`.
- `rescore`: If `True`, recompute candidate distances from `col`; if `False`, return the accelerator's approximate TopK.

**Supported metrics:**

| `metric` value | Aliases | Formula |
|---|---|---|
| `'l2'` | `'euclidean'` | √Σ(aᵢ−bᵢ)² |
| `'l2_squared'` | — | Σ(aᵢ−bᵢ)² |
| `'l1'` | `'manhattan'` | Σ|aᵢ−bᵢ| |
| `'linf'` | `'chebyshev'` | max|aᵢ−bᵢ| |
| `'cosine'` | `'cosine_distance'` | 1 − (a·b)/(‖a‖‖b‖) |
| `'dot'` | `'inner_product'` | −(a·b) (negated for min-heap) |

**Returns:** `ResultView` with `id_col` (Int64) and `dist_col` (Float64) columns, sorted nearest first.

**Example:**

```python
import numpy as np

query = np.random.rand(128).astype(np.float32)

# L2 (default)
results = client.topk_distance('vec', query, k=10)
df = results.to_pandas()
# df columns: _id (int64), dist (float64)

# Cosine distance, custom column names
results = client.topk_distance('vec', query, k=5, metric='cosine',
                                id_col='item_id', dist_col='cosine_dist')

# Join back to the original table to retrieve full records
top_ids = results.get_ids()                    # numpy array of _id values
records  = client.retrieve_many(top_ids.tolist())

# Or use a SQL subquery
client.execute("""
    SELECT items.label, items.vec
    FROM items
    WHERE _id IN (
        SELECT _id FROM (
            SELECT explode_rename(topk_distance(vec, [0.1, 0.2, 0.3], 5, 'l2'), '_id', 'dist')
            FROM items
        )
    )
""")
```

---

### batch_topk_distance

```python
batch_topk_distance(
    col: str,
    queries,
    k: int = 10,
    metric: str = 'l2',
) -> numpy.ndarray
```

Batch nearest-neighbour search — N query vectors in a single Rust call.

**Why use this instead of calling `topk_distance` N times:**

- The mmap float buffer (`scan_buf`) is populated **once** regardless of N.
- All N queries run in **parallel** via Rayon (outer parallelism over queries).
- The `_id` column is read only once.

**Parameters:**

- `col`: Name of the vector column (FixedList or Binary).
- `queries`: `(N, D)` numpy array or array-like of query vectors (float32 or float64). A 1-D array is treated as a single query (N=1).
- `k`: Number of nearest neighbours per query (default `10`).
- `metric`: Distance metric — same values accepted as `topk_distance`.

**Returns:** `numpy.ndarray` of shape `(N, k, 2)`, dtype `float64`.

- `result[i, j, 0]` — `_id` of the j-th nearest neighbour for query i (cast to `int64` as needed).
- `result[i, j, 1]` — corresponding distance.
- Each row is sorted ascending by distance.
- Entries padded with `(-1, inf)` when fewer than k neighbours exist.

**Example:**

```python
import numpy as np

N, D = 100, 128
queries = np.random.rand(N, D).astype(np.float32)

result = client.batch_topk_distance('vec', queries, k=10)
# result.shape == (100, 10, 2)

ids   = result[:, :, 0].astype(np.int64)   # shape (100, 10)
dists = result[:, :, 1]                     # shape (100, 10)

# Nearest neighbour for each query
nearest_id   = ids[:, 0]    # shape (100,)
nearest_dist = dists[:, 0]  # shape (100,)

# Cosine similarity batch search
result_cos = client.batch_topk_distance('vec', queries, k=5, metric='cosine')
```

---

### SQL: `explode_rename(topk_distance(...))`

Vector search is also available as a pure SQL expression. This is the form used internally by `topk_distance()` and is useful when composing larger SQL queries.

**Syntax:**

```sql
SELECT explode_rename(
    topk_distance(col, [q1, q2, ..., qD], k, 'metric'),
    'id_column_name',
    'dist_column_name'
)
FROM table_name
```

- `col` — vector column name.
- `[q1, q2, ..., qD]` — query vector as an array literal (float values).
- `k` — integer number of results.
- `'metric'` — distance metric string.
- The two string arguments to `explode_rename` name the output columns.

`explode_rename` "explodes" the TopK pairs returned by `topk_distance` into k rows with two named columns.

**Examples:**

```python
# Basic: top 10 by L2 distance
results = client.execute("""
    SELECT explode_rename(topk_distance(vec, [0.1, 0.2, 0.3], 10, 'l2'), '_id', 'dist')
    FROM items
""")
df = results.to_pandas()
# df: _id (int64), dist (float64), 10 rows sorted nearest first

# Cosine distance with custom column names
results = client.execute("""
    SELECT explode_rename(
        topk_distance(vec, [1.0, 0.0, 0.0], 5, 'cosine'),
        'item_id', 'cosine_dist'
    )
    FROM items
""")

# Dot product (inner product) search
results = client.execute("""
    SELECT explode_rename(topk_distance(vec, [0.5, 0.5, 0.5], 20, 'dot'), '_id', 'score')
    FROM embeddings
""")
```

> **Note:** The SQL `topk_distance` / `explode_rename` syntax requires the array literal `[...]` form for the query vector. To use dynamic vectors from Python, use the `topk_distance()` method instead, which handles the formatting automatically.

---

### Vector Search Performance

Benchmark: 1M rows × dim=128, k=10, release build, warm mmap scan buffer.

| Metric | ApexBase | DuckDB | Speedup |
|---|---|---|---|
| L2 | ~12ms | ~47ms | **3.8× faster** |
| Cosine | ~13ms | ~42ms | **3.1× faster** |
| Dot | ~13ms | ~36ms | **2.8× faster** |

All three metrics use a single scan of the mmap float buffer; distance computation is SIMD-accelerated.

---

### INSERT
```sql
INSERT INTO table (col1, col2) VALUES (1, 'a')
INSERT INTO table VALUES (1, 'a', 3.14)
INSERT INTO table (col1, col2) VALUES (1, 'a'), (2, 'b'), (3, 'c')
```

### DDL (Data Definition Language)

ApexBase supports full SQL DDL operations.

#### CREATE TABLE
```sql
CREATE TABLE table_name
CREATE TABLE IF NOT EXISTS table_name
```

#### ALTER TABLE
```sql
-- Add column
ALTER TABLE table_name ADD COLUMN column_name DATA_TYPE

-- Rename column  
ALTER TABLE table_name RENAME COLUMN old_name TO new_name

-- Drop column
ALTER TABLE table_name DROP COLUMN column_name
```

#### DROP TABLE
```sql
DROP TABLE table_name
DROP TABLE IF EXISTS table_name
```

#### Supported Data Types

| Type | Aliases | Description |
|------|---------|-------------|
| `STRING` | `VARCHAR`, `TEXT` | String/text data |
| `INT` | `INTEGER`, `INT32`, `INT64` | Integer numbers |
| `FLOAT` | `DOUBLE`, `FLOAT64` | Floating point numbers |
| `BOOL` | `BOOLEAN` | Boolean values |
| `FLOAT16_VECTOR` | `FLOAT16VECTOR`, `F16_VECTOR` | Half-precision float vector (SIMD-accelerated TopK) |

### Examples

```python
# Create table via SQL
client.execute("CREATE TABLE IF NOT EXISTS users")

# Add columns via SQL
client.execute("ALTER TABLE users ADD COLUMN name STRING")
client.execute("ALTER TABLE users ADD COLUMN age INT")

# Insert data via SQL
client.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
results = client.execute("SELECT * FROM users WHERE age > 25")

# Modify schema via SQL
client.execute("ALTER TABLE users RENAME COLUMN name TO full_name")
client.execute("ALTER TABLE users DROP COLUMN age")

# Drop table via SQL
client.execute("DROP TABLE IF EXISTS users")
```

#### Multi-Statement SQL

You can execute multiple SQL statements in a single call by separating them with semicolons:

```python
# Execute multiple statements at once
client.execute("""
    CREATE TABLE IF NOT EXISTS products;
    ALTER TABLE products ADD COLUMN name STRING;
    ALTER TABLE products ADD COLUMN price FLOAT;
    INSERT INTO products (name, price) VALUES ('Laptop', 999.99)
""")

# Multiple INSERT statements
client.execute("""
    INSERT INTO products (name, price) VALUES ('Mouse', 29.99);
    INSERT INTO products (name, price) VALUES ('Keyboard', 79.99);
    INSERT INTO products (name, price) VALUES ('Monitor', 299.99)
""")

# The result of the last statement is returned
results = client.execute("""
    CREATE TABLE IF NOT EXISTS temp;
    INSERT INTO temp (name) VALUES ('test');
    SELECT * FROM temp
""")
```

**Multi-Statement SQL Rules:**

- Statements are separated by semicolons (`;`)
- Semicolons inside string literals are handled correctly
- Statements execute sequentially in order
- The result of the last SELECT statement is returned
- DDL statements (CREATE, ALTER, DROP, INSERT) return empty results

---
