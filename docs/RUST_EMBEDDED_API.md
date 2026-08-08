# ApexBase Embedded Rust API

Use ApexBase as a high-performance embedded database directly from Rust — no Python, no FFI overhead.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Types](#core-types)
  - [Value](#value)
  - [ColumnType](#columntype)
  - [DataType](#datatype)
- [Database Operations — ApexDB](#database-operations-apexdb)
- [Table Operations — Table](#table-operations-table)
  - [Write Operations](#write-operations)
  - [Read Operations](#read-operations)
  - [SQL Execution](#sql-execution)
  - [Schema Management](#schema-management)
  - [Maintenance](#maintenance)
- [ResultSet](#resultset)
- [Multi-Database Support](#multi-database-support)
- [Durability Levels](#durability-levels)
- [Transactions via SQL](#transactions-via-sql)
- [Indexes](#indexes)
- [Full-Text Search](#full-text-search)
- [Vector Search](#vector-search)
- [Temporary Tables](#temporary-tables)
- [Concurrency & Thread Safety](#concurrency-thread-safety)
- [Public Helper Functions](#public-helper-functions)
- [Error Handling](#error-handling)
- [Complete API Reference](#complete-api-reference)
- [Running the Example](#running-the-example)
- [Performance Notes](#performance-notes)

---

## Installation

### Path dependency (local development)

```toml
[dependencies]
apexbase = { path = "path/to/ApexBase", default-features = false }
```

### Git dependency

```toml
[dependencies]
apexbase = { git = "https://github.com/BirchKwok/ApexBase.git", default-features = false }
```

Disabling the `python` default feature drops the PyO3 / numpy dependencies and reduces compile time:

```toml
# Pure embedded Rust library — no Python bindings compiled
apexbase = { ..., default-features = false }
```

If you also need the PG Wire or Arrow Flight servers:

```toml
apexbase = { ..., default-features = false, features = ["server"] }
apexbase = { ..., default-features = false, features = ["flight"] }
```

---

## Quick Start

```rust
use apexbase::embedded::{ApexDB, Row};
use apexbase::data::Value;
use std::collections::HashMap;

fn main() -> apexbase::Result<()> {
    // Open (or create) a database directory
    let db = ApexDB::open("./my_database")?;

    // Create a table
    let table = db.create_table("users")?;

    // Insert a record
    let id = table.insert([
        ("name".to_string(),  Value::String("Alice".to_string())),
        ("age".to_string(),   Value::Int64(30)),
        ("score".to_string(), Value::Float64(92.5)),
    ].into_iter().collect())?;

    println!("Inserted _id = {id}");

    // SQL query → Arrow RecordBatch
    let rs = table.execute("SELECT * FROM users WHERE age > 25")?;
    let batch = rs.to_record_batch()?;
    println!("{} rows", batch.num_rows());

    // Or convert to Vec<HashMap<String, Value>>
    let rs = table.execute("SELECT name, age FROM users ORDER BY age")?;
    for row in rs.to_rows()? {
        println!("{:?}", row.get("name"));
    }

    Ok(())
}
```

---

## Core Types

| Type | Description |
|------|-------------|
| `ApexDB` | Top-level database handle. Cheaply `Clone`-able (`Arc` internally). |
| `ApexDBBuilder` | Builder for `ApexDB` with durability and drop-if-exists options. |
| `Table` | Table-scoped operations handle. Cheaply `Clone`-able. |
| `ResultSet` | Result of a SQL query or DML statement. |
| `Row` | `HashMap<String, Value>` — a single database row. |
| `Value` | Enum for all column values (see below). |
| `DurabilityLevel` | `Fast` / `Safe` / `Max` — controls fsync behavior. |
| `ColumnType` | Column type enum used in `create_table_with_schema`. |
| `DataType` | Column type enum used in `add_column` / `schema()`. |

### Value

`apexbase::data::Value` — the universal value type for row data:

| Variant | Rust type | SQL type |
|---------|-----------|----------|
| `Value::Int64(i64)` | `i64` | `INT64` / `INTEGER` |
| `Value::Float64(f64)` | `f64` | `FLOAT64` / `DOUBLE` |
| `Value::String(String)` | `String` | `STRING` / `TEXT` / `VARCHAR` |
| `Value::Bool(bool)` | `bool` | `BOOL` / `BOOLEAN` |
| `Value::Binary(Vec<u8>)` | `Vec<u8>` | `BINARY` / `VARBINARY` |
| `Value::Blob(Vec<u8>)` | `Vec<u8>` | `BLOB` / `LARGE_BINARY` |
| `Value::FixedList(Vec<u8>)` | raw LE f32 bytes | `FIXEDLIST` (vector embedding) |
| `Value::Null` | — | `NULL` |

```rust
use apexbase::data::Value;

let v_int   = Value::Int64(42);
let v_float = Value::Float64(3.14);
let v_str   = Value::String("hello".to_string());
let v_bool  = Value::Bool(true);
let v_bytes = Value::Binary(vec![0u8, 1, 2, 3]);
let v_blob  = Value::Blob(vec![0u8; 128 * 1024]);
let v_null  = Value::Null;
```

### ColumnType

`apexbase::storage::on_demand::ColumnType` — used when pre-defining a schema with `create_table_with_schema`:

| Variant | Description |
|---------|-------------|
| `ColumnType::Int64` | 64-bit signed integer |
| `ColumnType::Float64` | 64-bit IEEE 754 float |
| `ColumnType::String` | UTF-8 string (plain or dict-encoded on disk) |
| `ColumnType::Bool` | Boolean (bit-packed) |
| `ColumnType::Binary` | Arbitrary byte array |
| `ColumnType::Blob` | Large byte object stored through inline/sidecar descriptors |
| `ColumnType::FixedList` | Fixed-size float32 vector (embedding storage) |

### DataType

`apexbase::data::DataType` — used in schema introspection (`schema()`, `column_type()`) and column management (`add_column`):

| Variant | Description |
|---------|-------------|
| `DataType::Int64` | 64-bit integer |
| `DataType::Float64` | 64-bit float |
| `DataType::String` | UTF-8 string |
| `DataType::Bool` | Boolean |
| `DataType::Binary` | Byte array |
| `DataType::Blob` | Large byte object |

---

## Database Operations — ApexDB

### Opening a Database

```rust
use apexbase::embedded::ApexDB;

// Default: Fast durability, no drop — creates the directory if absent
let db = ApexDB::open("./data")?;

// Builder pattern for full control
use apexbase::storage::DurabilityLevel;

let db = ApexDB::builder("./data")
    .durability(DurabilityLevel::Safe)
    .drop_if_exists(true)   // wipe all existing .apex files first
    .build()?;
```

### Table DDL

```rust
// Create an empty table (schema inferred from first insert)
let table = db.create_table("events")?;

// Create with a predefined schema — guarantees column order, avoids inference
use apexbase::storage::on_demand::ColumnType;

let table = db.create_table_with_schema("orders", &[
    ("order_id".to_string(), ColumnType::Int64),
    ("product".to_string(),  ColumnType::String),
    ("price".to_string(),    ColumnType::Float64),
    ("shipped".to_string(),  ColumnType::Bool),
])?;

// Open an existing table
let table = db.table("events")?;

// Drop a table (deletes .apex + companion files)
db.drop_table("events")?;

// List all tables in the current database (sorted)
let names: Vec<String> = db.list_tables();

// Current base directory path
let dir = db.base_dir();
```

### Database-level SQL

```rust
// Execute SQL without a specific table context (cross-table queries, DDL, etc.)
let rs = db.execute("SELECT COUNT(*) FROM users")?;
let rs = db.execute("CREATE INDEX idx_age ON users (age)")?;
let rs = db.execute("SELECT u.name, o.product FROM users u JOIN orders o ON u._id = o.user_id")?;
```

### Cache Invalidation

```rust
// Invalidate all engine caches (useful after external writes to the same directory)
db.invalidate_cache();
```

---

## Table Operations — Table

### Write Operations

```rust
use apexbase::embedded::Row;
use apexbase::data::Value;
use std::collections::HashMap;

// ── Insert ────────────────────────────────────────────────────────────────────

// Single record → returns assigned _id (u64, auto-increment)
let mut rec: Row = HashMap::new();
rec.insert("name".to_string(), Value::String("Bob".to_string()));
rec.insert("age".to_string(),  Value::Int64(25));
let id: u64 = table.insert(rec)?;

// Using iterator collect shorthand
let id = table.insert([
    ("name".to_string(), Value::String("Carol".to_string())),
    ("age".to_string(),  Value::Int64(32)),
].into_iter().collect())?;

// Batch insert (most efficient for many rows) → Vec<u64> of _ids
let records: Vec<Row> = (0..1000).map(|i| {
    [
        ("n".to_string(), Value::Int64(i)),
        ("v".to_string(), Value::Float64(i as f64 * 1.5)),
    ].into_iter().collect()
}).collect();
let ids: Vec<u64> = table.insert_batch(&records)?;

// Insert from Arrow RecordBatch (fastest for Arrow-native pipelines)
use arrow::record_batch::RecordBatch;
let ids: Vec<u64> = table.insert_arrow(&batch)?;

// ── Delete ────────────────────────────────────────────────────────────────────

// Delete by _id → true if the row existed
let existed: bool = table.delete(id)?;

// Delete multiple rows → returns count deleted
let deleted: usize = table.delete_batch(&[1, 2, 3, 4, 5])?;

// SQL DELETE with WHERE clause
table.execute("DELETE FROM users WHERE age < 18")?;

// ── Replace (overwrite whole row) ────────────────────────────────────────────

let mut updated: Row = HashMap::new();
updated.insert("name".to_string(),  Value::String("Bob 2.0".to_string()));
updated.insert("age".to_string(),   Value::Int64(26));
let existed: bool = table.replace(id, updated)?;
```

### Read Operations

```rust
// Point lookup by _id → Option<Row>
let row: Option<Row> = table.retrieve(id)?;
if let Some(r) = row {
    println!("{:?}", r.get("name"));
}

// Batch lookup by _ids → Arrow RecordBatch (V4 mmap fast-path)
let batch: RecordBatch = table.retrieve_many(&[1, 2, 3])?;

// Row count — O(1) for V4-format tables (reads footer metadata only)
let n: u64 = table.count()?;

// Check if a row exists
let exists: bool = table.exists(id)?;

// Absolute path to the .apex file
let path: &Path = table.path();
```

### SQL Execution

The full SQL engine (JIT, mmap fast-paths, zone-map pruning) is available on `Table::execute`.  
The table is the default context — use its name unqualified in SQL.

```rust
// SELECT with filter
let rs = table.execute("SELECT name, score FROM users WHERE score > 80")?;

// Aggregation
let rs = table.execute(
    "SELECT city, AVG(score) AS avg_score FROM users GROUP BY city HAVING COUNT(*) > 2"
)?;

// ORDER BY + LIMIT
let rs = table.execute("SELECT * FROM users ORDER BY score DESC LIMIT 10")?;

// Subqueries
let rs = table.execute(
    "SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)"
)?;

// CTEs
let rs = table.execute(
    "WITH seniors AS (SELECT * FROM users WHERE age >= 30)
     SELECT city, COUNT(*) FROM seniors GROUP BY city"
)?;

// Window functions
let rs = table.execute(
    "SELECT name, ROW_NUMBER() OVER (PARTITION BY city ORDER BY score DESC) AS rn FROM users"
)?;

// DML: UPDATE
table.execute("UPDATE users SET active = true WHERE last_login > '2024-01-01'")?;

// DML: INSERT ... ON CONFLICT (upsert)
table.execute(
    "INSERT INTO users (name, age) VALUES ('Alice', 31)
     ON CONFLICT (name) DO UPDATE SET age = 31"
)?;

// DDL: CREATE TABLE AS
table.execute("CREATE TABLE seniors AS SELECT * FROM users WHERE age >= 30")?;

// EXPLAIN / EXPLAIN ANALYZE
let plan = table.execute("EXPLAIN SELECT * FROM users WHERE age > 25")?;

// File reading directly in SQL (no import step)
let rs = table.execute(
    "SELECT city, COUNT(*) FROM read_csv('/data/users.csv') GROUP BY city LIMIT 10"
)?;
```

### Schema Management

```rust
use apexbase::data::DataType;

// Schema as (column_name, DataType) pairs — preserves column order
let schema: Vec<(String, DataType)> = table.schema()?;
for (col, dtype) in &schema {
    println!("{col}: {dtype:?}");
}

// Column names only
let cols: Vec<String> = table.columns()?;

// Type of a specific column
let dtype: Option<DataType> = table.column_type("age")?;

// Add a new column — all existing rows are set to NULL
table.add_column("active", DataType::Bool)?;

// Drop a column
table.drop_column("temporary_col")?;

// Rename a column
table.rename_column("old_name", "new_name")?;
```

### Maintenance

```rust
// Flush in-memory writes to disk (important for Safe/Max durability)
table.flush()?;
```

---

## ResultSet

`ResultSet` is the return type of every `execute()` call. It holds either an Arrow `RecordBatch`, a scalar `i64`, or an empty result with schema.

```rust
let rs = table.execute("SELECT * FROM users WHERE age > 25")?;

// Number of result rows
println!("{} rows", rs.num_rows());

// Column names in result order
println!("{:?}", rs.columns());   // ["name", "age", "score", ...]

// Check for empty result
if rs.is_empty() { return Ok(()); }

// Convert to Arrow RecordBatch — zero-copy for the Data variant
let batch: RecordBatch = rs.to_record_batch()?;

// Convert to Vec<Row> (HashMap<String, Value>)
let rs = table.execute("SELECT * FROM users LIMIT 10")?;
let rows: Vec<Row> = rs.to_rows()?;
for row in &rows {
    println!("name={:?}  age={:?}", row.get("name"), row.get("age"));
}

// Scalar result — for COUNT(*), SUM, etc.
let rs = table.execute("SELECT COUNT(*) FROM users")?;
if let Some(count) = rs.scalar() {
    println!("count = {count}");
}
```

### ResultSet Methods

| Method | Return type | Description |
|--------|-------------|-------------|
| `to_record_batch()` | `Result<RecordBatch>` | Convert to Arrow `RecordBatch` (zero-copy for `Data` variant) |
| `to_rows()` | `Result<Vec<Row>>` | Convert to `Vec<HashMap<String, Value>>` |
| `num_rows()` | `usize` | Number of result rows |
| `columns()` | `Vec<String>` | Column names in result order |
| `scalar()` | `Option<i64>` | For scalar results (COUNT, SUM, …) |
| `is_empty()` | `bool` | `true` when result has 0 rows |

---

## Multi-Database Support

A single `ApexDB` handle can manage multiple isolated databases (sub-directories). Each named database has its own set of `.apex` files.

```rust
// Switch to a named sub-database (creates the sub-directory if needed)
db.use_database("production")?;
let prod_users = db.create_table("users")?;

// Switch to another named database
db.use_database("staging")?;
let stage_users = db.table("users")?;   // different file from prod

// Cross-database SQL — standard db.table syntax
let rs = db.execute("SELECT * FROM production.users")?;
let rs = db.execute(
    "SELECT u.name, e.event
     FROM production.users u
     JOIN analytics.events e ON u._id = e.user_id"
)?;

// Revert to root database
db.use_database("")?;
```

---

## Durability Levels

| Level | `fsync` | Use Case |
|-------|---------|----------|
| `Fast` (default) | Never | Bulk import, analytics, reconstructible data |
| `Safe` | On `flush()` | Most production write workloads |
| `Max` | Every write | Financial records, critical ledger data |

```rust
use apexbase::storage::DurabilityLevel;

let db = ApexDB::builder("./data")
    .durability(DurabilityLevel::Safe)
    .build()?;

// Insert then explicitly flush to disk
let id = table.insert(row)?;
table.flush()?;    // ensures data survives a process crash
```

---

## Transactions via SQL

ApexBase supports OCC-based transactions with SAVEPOINT / ROLLBACK TO:

```rust
// Begin a transaction
table.execute("BEGIN")?;

table.execute("INSERT INTO users (name, age) VALUES ('Tx1', 20)")?;

// Savepoint
table.execute("SAVEPOINT sp1")?;
table.execute("INSERT INTO users (name, age) VALUES ('Tx2', 21)")?;

// Partial rollback — undo Tx2 only
table.execute("ROLLBACK TO sp1")?;

// Commit — persists Tx1 only
table.execute("COMMIT")?;

// Or rollback everything
table.execute("BEGIN")?;
table.execute("INSERT INTO users (name, age) VALUES ('Abandoned', 99)")?;
table.execute("ROLLBACK")?;
```

Transactions apply to the table context of the `Table` handle. Use `db.execute()` for multi-table transactions.

---

## Indexes

```rust
// B-tree index for range queries and equality lookups
table.execute("CREATE INDEX idx_age ON users (age)")?;

// Unique index for upsert / deduplication
table.execute("CREATE UNIQUE INDEX idx_name ON users (name)")?;

// Queries automatically use indexes when the optimizer detects them
let rs = table.execute("SELECT * FROM users WHERE age = 30")?;   // index scan
let rs = table.execute("SELECT * FROM users WHERE age BETWEEN 20 AND 40")?;  // range scan

// Drop an index
table.execute("DROP INDEX idx_age ON users")?;

// Rebuild all indexes on a table
table.execute("REINDEX users")?;
```

---

## Full-Text Search

```rust
// Create a FTS index on one or more string columns
table.execute("CREATE FTS INDEX ON articles (title, content)")?;

// Search with exact matching
let rs = table.execute("SELECT * FROM articles WHERE MATCH('rust programming')")?;

// Fuzzy search — tolerates typos
let rs = table.execute(
    "SELECT title FROM articles WHERE FUZZY_MATCH('databse')"
)?;

// Combine FTS with regular predicates
let rs = table.execute(
    "SELECT * FROM articles
     WHERE MATCH('machine learning') AND published_at > '2024-01-01'
     ORDER BY _id DESC LIMIT 20"
)?;

// FTS also works in aggregations
let rs = table.execute("SELECT COUNT(*) FROM articles WHERE MATCH('deep learning')")?;

// Manage the index
table.execute("SHOW FTS INDEXES")?;
table.execute("ALTER FTS INDEX ON articles DISABLE")?;  // suspend, keep files
table.execute("DROP FTS INDEX ON articles")?;           // remove index + files
```

---

## Vector Search

ApexBase provides SIMD-accelerated (NEON / AVX2) nearest-neighbour search. Vectors are stored as `FixedList` columns.

```rust
use apexbase::data::Value;
use std::collections::HashMap;

// Store vectors — encode float32 values as raw LE bytes
fn encode_f32_vec(floats: &[f32]) -> Vec<u8> {
    floats.iter().flat_map(|f| f.to_le_bytes()).collect()
}

// Create a table with a vector column
let items = db.create_table_with_schema("items", &[
    ("label".to_string(), ColumnType::String),
    ("vec".to_string(),   ColumnType::FixedList),
])?;

// Insert records with 4-dimensional float32 vectors
for (label, v) in [("a", [0.1f32, 0.2, 0.3, 0.4]),
                   ("b", [0.9, 0.8, 0.7, 0.6])] {
    let mut r = HashMap::new();
    r.insert("label".to_string(), Value::String(label.to_string()));
    r.insert("vec".to_string(),   Value::FixedList(encode_f32_vec(&v)));
    items.insert(r)?;
}

// SQL TopK — array literal syntax
let rs = items.execute(
    "SELECT explode_rename(topk_distance(vec, [0.1, 0.2, 0.3, 0.4], 5, 'l2'), '_id', 'dist')
     FROM items"
)?;
let rows = rs.to_rows()?;
for row in &rows {
    println!("_id={:?}  dist={:?}", row.get("_id"), row.get("dist"));
}

// SQL vector distance functions
let rs = items.execute(
    "SELECT label, array_distance(vec, [0.1, 0.2, 0.3, 0.4]) AS dist
     FROM items ORDER BY dist LIMIT 5"
)?;
```

**Supported distance metrics:** `'l2'` / `'euclidean'`, `'l2_squared'`, `'l1'` / `'manhattan'`, `'linf'` / `'chebyshev'`, `'cosine'` / `'cosine_distance'`, `'dot'` / `'inner_product'`

---

## Temporary Tables

Register external data files (CSV, JSON, Parquet) as temporary native tables. The file is parsed once and materialized into ApexBase's mmap-backed `.apex` format stored in a `.apex_tmp/` subdirectory. Subsequent queries bypass file parsing entirely.

```rust
use apexbase::embedded::ApexDB;

let db = ApexDB::open("./data")?;

// Register a CSV file as a temp table
db.register_temp_table("orders", "/data/orders.csv")?;

// Query it — zone maps, bloom filters, zero-copy mmap reads
let rs = db.execute("SELECT city, COUNT(*) FROM orders GROUP BY city")?;
let batch = rs.to_record_batch()?;
println!("rows: {}", batch.num_rows());

// Full SQL works: WHERE, JOIN, GROUP BY, window functions, etc.
let rs = db.execute("
    SELECT o.city, SUM(o.amount) AS total
    FROM orders o
    WHERE o.amount > 100
    GROUP BY o.city
    ORDER BY total DESC
    LIMIT 10
")?;

// Drop when done
db.drop_temp_table("orders")?;
```

**Supported formats:** CSV (.csv/.tsv), JSON (.json/.ndjson/.jsonl), Parquet (.parquet) — auto-detected by file extension.

**Performance:** Temp tables use memory-mapped I/O (near-zero RAM), zone maps for filter pushdown, and bloom filters for point lookups. For workloads that query the same file multiple times, `register_temp_table()` provides order-of-magnitude speedups over repeated `read_csv()` / `read_json()` calls.

**Cleanup:** Temp tables are automatically removed from disk when the `ApexDB` instance is dropped.

```rust
fn register_temp_table(&self, name: &str, file_path: &str) -> Result<()>
fn drop_temp_table(&self, name: &str) -> Result<()>
```

---

## Concurrency & Thread Safety

Both `ApexDB` and `Table` are `Clone + Send + Sync`. The underlying storage engine uses fine-grained `RwLock`s, and Rayon powers parallel aggregation and vector scans.

```rust
use std::sync::Arc;
use std::thread;

let db = Arc::new(ApexDB::open("./data")?);
let table = Arc::new(db.create_table("events")?);

let handles: Vec<_> = (0..4).map(|thread_id| {
    let t = Arc::clone(&table);
    thread::spawn(move || {
        // Concurrent reads are fully parallel (shared mmap)
        let rs = t.execute("SELECT COUNT(*) FROM events").unwrap();
        println!("thread {thread_id}: count = {:?}", rs.scalar());
    })
}).collect();

for h in handles { h.join().unwrap(); }
```

**Notes:**
- `ApexDB::clone()` and `Table::clone()` are `O(1)` — they share the same `Arc<DbInner>`.
- Reads (`execute`, `retrieve`, `retrieve_many`, `count`) are lock-free on V4 mmap-only tables.
- Writes are serialized per-table via an internal write lock in the storage engine.
- `use_database()` modifies shared state; avoid calling it from multiple threads concurrently.

---

## Public Helper Functions

Two public functions are exported from `apexbase::embedded`:

```rust
use apexbase::embedded::{record_batch_to_rows, arrow_value_at};
use arrow::array::ArrayRef;

// Convert a full RecordBatch to Vec<Row>
let rows: Vec<Row> = record_batch_to_rows(&batch)?;

// Extract a single Value at a specific row index from any ArrayRef
let val: Value = arrow_value_at(&column_ref, row_index);
```

**`record_batch_to_rows(batch: &RecordBatch) → Result<Vec<Row>>`**  
Iterates over all columns and rows, calling `arrow_value_at` for each cell.

**`arrow_value_at(arr: &ArrayRef, row: usize) → Value`**  
Handles: `Int8/16/32/64`, `UInt8/16/32/64`, `Float32/64`, `Boolean`, `Utf8`, `LargeUtf8`, `Binary`, `LargeBinary`. Large Arrow binary values are represented as `Value::Blob`; regular Arrow binary values are represented as `Value::Binary`. Returns `Value::Null` for null entries or unrecognized types.

---

## Error Handling

All methods return `apexbase::Result<T>` which is `Result<T, ApexError>`.

```rust
use apexbase::ApexError;

match db.table("nonexistent") {
    Err(ApexError::TableNotFound(name)) => eprintln!("No table: {name}"),
    Err(ApexError::TableExists(name))   => eprintln!("Already exists: {name}"),
    Err(ApexError::Io(e))               => eprintln!("IO error: {e}"),
    Err(e)                              => eprintln!("Other: {e}"),
    Ok(table)                           => { /* use table */ }
}

// Use ? for ergonomic propagation
fn process(db: &ApexDB) -> apexbase::Result<()> {
    let table = db.table("orders")?;
    let rs = table.execute("SELECT * FROM orders WHERE price > 100")?;
    let rows = rs.to_rows()?;
    println!("{} expensive orders", rows.len());
    Ok(())
}
```

---

## Complete API Reference

### ApexDB

| Method | Return type | Description |
|--------|-------------|-------------|
| `ApexDB::open(path)` | `Result<ApexDB>` | Open/create database with `Fast` durability |
| `ApexDB::builder(path)` | `ApexDBBuilder` | Return builder for custom options |
| `create_table(name)` | `Result<Table>` | Create new empty table |
| `create_table_with_schema(name, schema)` | `Result<Table>` | Create table with predefined schema |
| `table(name)` | `Result<Table>` | Open existing table |
| `drop_table(name)` | `Result<()>` | Delete table + companion files |
| `list_tables()` | `Vec<String>` | Sorted list of all tables in current db dir |
| `execute(sql)` | `Result<ResultSet>` | Database-level SQL (no default table context) |
| `use_database(name)` | `Result<()>` | Switch to named sub-database (`""` = root) |
| `base_dir()` | `PathBuf` | Current base directory path |
| `invalidate_cache()` | `()` | Evict all engine caches for current directory |
| `register_temp_table(name, file_path)` | `Result<()>` | Parse a CSV/JSON/Parquet file and register as a native temp table |
| `drop_temp_table(name)` | `Result<()>` | Drop a temp table |

### ApexDBBuilder

| Method | Return type | Description |
|--------|-------------|-------------|
| `durability(level)` | `ApexDBBuilder` | Set durability level (default: `Fast`) |
| `drop_if_exists(flag)` | `ApexDBBuilder` | If `true`, wipe existing `.apex` files on open |
| `build()` | `Result<ApexDB>` | Build and open the database |

### Table

#### Write

| Method | Return type | Description |
|--------|-------------|-------------|
| `insert(row)` | `Result<u64>` | Insert one record; returns assigned `_id` |
| `insert_batch(rows)` | `Result<Vec<u64>>` | Batch insert; returns all assigned `_id`s |
| `insert_arrow(batch)` | `Result<Vec<u64>>` | Insert Arrow `RecordBatch`; fastest for Arrow data |
| `replace(id, row)` | `Result<bool>` | Overwrite row by `_id`; `true` if existed |
| `delete(id)` | `Result<bool>` | Delete row by `_id`; `true` if existed |
| `delete_batch(ids)` | `Result<usize>` | Delete multiple rows; returns count deleted |

#### Read

| Method | Return type | Description |
|--------|-------------|-------------|
| `retrieve(id)` | `Result<Option<Row>>` | Point lookup by `_id` |
| `retrieve_many(ids)` | `Result<RecordBatch>` | Batch lookup by `_id`s (V4 mmap fast-path) |
| `count()` | `Result<u64>` | Active row count (O(1) for V4 tables) |
| `exists(id)` | `Result<bool>` | Check if row with `_id` exists |
| `path()` | `&Path` | Absolute path to the `.apex` file |

#### SQL

| Method | Return type | Description |
|--------|-------------|-------------|
| `execute(sql)` | `Result<ResultSet>` | Run SQL with this table as default context |

#### Schema

| Method | Return type | Description |
|--------|-------------|-------------|
| `schema()` | `Result<Vec<(String, DataType)>>` | Full schema in column order |
| `columns()` | `Result<Vec<String>>` | Column names in schema order |
| `column_type(name)` | `Result<Option<DataType>>` | Type of a specific column |
| `add_column(name, dtype)` | `Result<()>` | Add new column (existing rows → NULL) |
| `drop_column(name)` | `Result<()>` | Drop a column |
| `rename_column(old, new)` | `Result<()>` | Rename a column |

#### Maintenance

| Method | Return type | Description |
|--------|-------------|-------------|
| `flush()` | `Result<()>` | Flush in-memory writes to disk |

---

## Running the Example

```bash
cargo run --example embedded --no-default-features
```

The example at `examples/embedded.rs` demonstrates all 16 steps:
- Opening a database with the builder
- Creating a table with predefined schema
- Inserting individual rows
- SQL queries (filter, aggregate, GROUP BY)
- Bulk insert (1 000 rows)
- Delete and replace by `_id`
- `retrieve_many` (Arrow batch read)
- Schema introspection (`columns()`)
- Add / drop columns
- Multi-table operations
- Database-level SQL (`db.execute`)

---

## Performance Notes

- **Point lookups** (`retrieve`) — O(log n) via V4 RCIX index, ~24 µs warm.
- **Batch reads** (`retrieve_many`) — single footer lock + one mmap slice per row-group via V4 mmap fast-path.
- **Bulk insert** (`insert_batch`) — routes through `Database::write` (StorageEngine smart routing): V4 tables append a new Row Group via the insert backend; legacy non-V4 tables use delta appends when the schema matches exactly.
- **Arrow insert** (`insert_arrow`) — bypasses `HashMap` construction; preferred for Arrow-native pipelines.
- **SQL queries** — same Arrow-native JIT engine as the Python API: Cranelift JIT, vectorized SIMD filters, zone-map pruning, mmap on-demand scans.
- **Count** (`count()`) — O(1) for V4 tables — reads only the footer metadata.
- **Concurrency** — reads are parallel on V4 mmap-only tables (no lock contention); writes are serialized per table.
- **Vector search** — SIMD-accelerated (NEON fp16 on ARM, AVX2+F16C on x86_64); 3–4× faster than DuckDB at 1M rows × dim=128.
