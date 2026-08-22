# ApexBase Usage Examples

Comprehensive examples covering 100% of the ApexBase Python API.

## Table of Contents

1. [Basic Operations](#basic-operations)
2. [Table Management](#table-management)
3. [SQL DDL Operations](#sql-ddl-operations)
4. [Data Import](#data-import)
5. [Querying Data](#querying-data)
6. [File Reading Table Functions](#file-reading-table-functions)
7. [Temporary Tables from Files](#temporary-tables-from-files)
8. [Set Operations](#set-operations)
9. [Vector Search](#vector-search)
10. [Column Operations](#column-operations)
11. [Full-Text Search](#full-text-search)
12. [Data Modification](#data-modification)
13. [Utility Methods](#utility-methods)
14. [Advanced Usage](#advanced-usage)

---

## Basic Operations

### Initialization

```python
from apexbase import ApexClient

# Basic initialization
client = ApexClient("./data")

# With options
client = ApexClient(
    dirpath="./data",
    durability="safe",        # "fast" | "safe" | "max"
    batch_size=1000,
    cache_size=10000
)

# Create clean instance (deletes existing data)
client = ApexClient.create_clean("./fresh_data")
```

### Storing Data

```python
# Create a table first (required before any data operations)
client.create_table("users")

# Single record
client.store({"name": "Alice", "age": 30})

# Multiple records
client.store([
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35}
])

# Columnar format (fastest for bulk)
client.store({
    "name": ["David", "Eve", "Frank"],
    "age": [28, 32, 40],
    "city": ["NYC", "LA", "Chicago"]
})
```

### Closing

```python
# Explicit close
client.close()

# Context manager (recommended)
with ApexClient("./data") as client:
    client.create_table("mydata")
    client.store({"key": "value"})
    # Auto-closes on exit
```

---

## Table Management

ApexBase requires explicit table creation before any data operations. Each table is stored as a separate `.apex` file.

```python
client = ApexClient("./data")

# Create tables (the last created table becomes the active table)
client.create_table("users")

# Create table with pre-defined schema
client.create_table("orders", schema={
    "order_id": "int64",
    "product": "string",
    "price": "float64"
})

# List tables
tables = client.list_tables()
print(tables)  # ['users', 'orders']

# Switch table
client.use_table("users")
print(client.current_table)  # 'users'

# Store in the active table
client.store({"username": "alice", "email": "alice@example.com"})

# Reopen an existing database
client2 = ApexClient("./data")
client2.use_table("users")  # Select an existing table

# Drop table (active table resets to None)
client.drop_table("orders")

client.close()
```

---

## SQL DDL Operations

ApexBase supports full SQL DDL (Data Definition Language) operations:

### CREATE TABLE

```python
client = ApexClient("./data")

# Create a new table (becomes the active table)
client.execute("CREATE TABLE employees")

# Create table only if it doesn't exist
client.execute("CREATE TABLE IF NOT EXISTS departments")

# Verify tables were created
print(client.list_tables())  # ['employees', 'departments']
```

### INSERT

```python
# Insert single row with column names
client.execute("INSERT INTO employees (name, age, department) VALUES ('Alice', 30, 'Engineering')")

# Insert multiple rows in one statement
client.execute("""
    INSERT INTO employees (name, age, department) 
    VALUES 
        ('Charlie', 35, 'Marketing'),
        ('David', 28, 'Engineering')
""")

# Query the data
results = client.execute("SELECT * FROM employees")
print(f"Inserted {len(results)} records")
```

### ALTER TABLE

```python
# Add a column with type
client.execute("ALTER TABLE employees ADD COLUMN email STRING")
client.execute("ALTER TABLE employees ADD COLUMN salary FLOAT")

# Rename a column
client.execute("ALTER TABLE employees RENAME COLUMN email TO email_address")

# Drop a column
client.execute("ALTER TABLE employees DROP COLUMN email_address")
```

### DROP TABLE

```python
# Drop a table
client.execute("DROP TABLE departments")

# Drop only if exists (avoids error if table doesn't exist)
client.execute("DROP TABLE IF EXISTS temp_table")

# Verify department table was dropped
print(client.list_tables())  # ['employees']
```

### Complete DDL Workflow

```python
client = ApexClient("./data")

# 1. Create tables via SQL
client.execute("CREATE TABLE IF NOT EXISTS products")
client.execute("CREATE TABLE IF NOT EXISTS categories")

# 2. Add columns via SQL
client.execute("ALTER TABLE categories ADD COLUMN name STRING")
client.execute("ALTER TABLE categories ADD COLUMN description STRING")
client.execute("ALTER TABLE products ADD COLUMN name STRING")
client.execute("ALTER TABLE products ADD COLUMN price FLOAT")
client.execute("ALTER TABLE products ADD COLUMN category_id INT")

# 3. Insert data via SQL
client.execute("INSERT INTO categories (name, description) VALUES ('Electronics', 'Gadgets')")
client.execute("""
    INSERT INTO products (name, price, category_id) 
    VALUES 
        ('Laptop', 999.99, 1),
        ('Smartphone', 699.99, 1)
""")

# 4. Query the data
products = client.execute("SELECT * FROM products")
print(products.to_pandas())

# 5. Clean up via SQL
client.execute("DROP TABLE IF EXISTS products")
client.execute("DROP TABLE IF EXISTS categories")

client.close()
```

### Supported DDL Syntax

| Statement | Description | Example |
|-----------|-------------|---------|
| `CREATE TABLE [IF NOT EXISTS] name` | Create new table | `CREATE TABLE users` |
| `INSERT INTO ... VALUES ...` | Insert single row | `INSERT INTO users (name) VALUES ('Alice')` |
| `INSERT INTO ... VALUES (...), (...)` | Insert multiple rows | `INSERT INTO users (name) VALUES ('A'), ('B')` |
| `ALTER TABLE ... ADD COLUMN ...` | Add column | `ALTER TABLE users ADD COLUMN age INT` |
| `ALTER TABLE ... RENAME COLUMN ...` | Rename column | `ALTER TABLE users RENAME COLUMN age TO years` |
| `ALTER TABLE ... DROP COLUMN ...` | Drop column | `ALTER TABLE users DROP COLUMN age` |
| `DROP TABLE [IF EXISTS] name` | Drop table | `DROP TABLE users` |

### Supported Data Types in DDL

| Type | Aliases | Description |
|------|---------|-------------|
| `STRING` | `VARCHAR`, `TEXT` | Variable-length text |
| `INT` | `INTEGER`, `INT32`, `INT64` | Integer numbers |
| `FLOAT` | `DOUBLE`, `FLOAT64` | Floating-point numbers |
| `BOOL` | `BOOLEAN` | True/false values |

### Multi-Statement SQL

Execute multiple SQL statements separated by semicolons:

```python
client = ApexClient("./data")

# Setup entire schema in one call
client.execute("""
    CREATE TABLE IF NOT EXISTS products;
    ALTER TABLE products ADD COLUMN name STRING;
    ALTER TABLE products ADD COLUMN price FLOAT;
    ALTER TABLE products ADD COLUMN category STRING;
    INSERT INTO products (name, price, category) VALUES ('Laptop', 999.99, 'Electronics')
""")

# Insert multiple rows efficiently
client.execute("""
    INSERT INTO products (name, price, category) VALUES ('Mouse', 29.99, 'Electronics');
    INSERT INTO products (name, price, category) VALUES ('Keyboard', 79.99, 'Electronics');
    INSERT INTO products (name, price, category) VALUES ('Monitor', 299.99, 'Electronics');
    INSERT INTO products (name, price, category) VALUES ('Desk', 199.99, 'Furniture')
""")

# Query and analyze
results = client.execute("""
    SELECT category, COUNT(*) as count, AVG(price) as avg_price 
    FROM products 
    GROUP BY category
""")
print(results.to_pandas())

# Clean up multiple tables
client.execute("""
    DROP TABLE IF EXISTS products;
    DROP TABLE IF EXISTS temp_table
""")

client.close()
```

**Notes on Multi-Statement SQL:**

- Statements are separated by semicolons (`;`)
- Semicolons inside string literals are handled correctly
- Statements execute sequentially
- The result of the last statement is returned
- Useful for schema setup and batch operations

---

## Data Import

### From Pandas

```python
import pandas as pd

client = ApexClient("./data")

df = pd.DataFrame({
    "product": ["A", "B", "C"],
    "price": [10.5, 20.0, 15.0],
    "quantity": [100, 200, 150]
})

# table_name auto-creates and selects the table
client.from_pandas(df, table_name="products")
```

### From Polars

```python
import polars as pl

client = ApexClient("./data")

df = pl.DataFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "score": [85, 92, 78]
})

client.from_polars(df, table_name="scores")
```

### From PyArrow

```python
import pyarrow as pa

client = ApexClient("./data")

table = pa.table({
    "id": [1, 2, 3],
    "value": ["a", "b", "c"]
})

client.from_pyarrow(table, table_name="items")
```

---

## Querying Data

### SQL Queries

```python
client = ApexClient("./data")
client.create_table("metrics")

# Insert test data
for i in range(100):
    client.store({"id": i, "value": i * 10, "category": f"cat_{i % 5}"})

# Basic SELECT (use your table name in FROM clause)
results = client.execute("SELECT * FROM metrics")
print(f"Total rows: {len(results)}")

# WHERE clause
results = client.execute("SELECT * FROM metrics WHERE value > 500")

# ORDER BY with LIMIT
results = client.execute("""
    SELECT * FROM metrics
    WHERE category = 'cat_1'
    ORDER BY value DESC
    LIMIT 10
""")

# Aggregation
results = client.execute("""
    SELECT 
        COUNT(*) as total,
        AVG(value) as avg_value,
        MAX(value) as max_value,
        MIN(value) as min_value
    FROM metrics
""")
print(results.first())

# GROUP BY
results = client.execute("""
    SELECT category, COUNT(*), AVG(value)
    FROM metrics
    GROUP BY category
""")
for row in results:
    print(row)

# Get single scalar value
count = client.execute("SELECT COUNT(*) FROM metrics").scalar()
print(f"Count: {count}")

# Count rows shortcut
count = client.count_rows()
count = client.count_rows("users")  # Specific table

client.close()
```

### Quoted Identifiers (Reserved Keyword Column Names)

```python
client = ApexClient("./data")
client.create_table("events")

# Store data with columns named after SQL keywords
client.store([
    {"order": 1, "group": "A", "select": 100},
    {"order": 2, "group": "B", "select": 200},
    {"order": 3, "group": "A", "select": 150},
])

# Use backticks (Hive/MySQL style) to quote reserved keywords
results = client.execute("SELECT `order`, `group` FROM events WHERE `order` > 1")
print(results.to_pandas())

# Double quotes (SQL standard) also work
results = client.execute('SELECT "order", "group" FROM events ORDER BY "order" DESC')

# GROUP BY with quoted column
results = client.execute("""
    SELECT `group`, COUNT(*), AVG(`select`)
    FROM events
    GROUP BY `group`
""")
for row in results:
    print(row)

# INSERT with quoted column names
client.execute("INSERT INTO events (`order`, `group`, `select`) VALUES (4, 'C', 300)")

client.close()
```

### Using query() Method

```python
client = ApexClient("./data")
client.use_table("users")  # Select existing table

# Simple WHERE expression (uses the active table)
results = client.query("age > 25")
results = client.query("name LIKE 'A%'")

# With limit
results = client.query("age > 25", limit=100)

# Using where_clause parameter
results = client.query(where_clause="city = 'NYC'", limit=50)

client.close()
```

### Retrieve by ID

```python
client = ApexClient("./data")
client.use_table("users")

# Single record
record = client.retrieve(1)
print(record)  # {'_id': 1, 'name': 'Alice', ...}

# Multiple records
results = client.retrieve_many([1, 6, 11, 16])
df = results.to_pandas()

# All records
results = client.retrieve_all()
print(f"Shape: {results.shape}")

client.close()
```

### ResultView Operations

```python
results = client.execute("SELECT * FROM users WHERE age > 25")

# Convert formats
df = results.to_pandas()
pl_df = results.to_polars()
arrow_table = results.to_arrow()
dicts = results.to_dict()

# Properties
print(results.shape)      # (rows, columns)
print(results.columns)    # ['_id', 'name', 'age', ...]
print(len(results))       # row count

# Get IDs
ids = results.get_ids()           # numpy array
ids = results.get_ids(return_list=True)  # Python list

# Get single values
first = results.first()
scalar = client.execute("SELECT COUNT(*) FROM users").scalar()

# Iteration
for row in results:
    print(row["name"])

# Indexing
row = results[0]
```

---

## Column Operations

```python
client = ApexClient("./data")
client.create_table("people")
client.store({"name": "Alice", "age": 30})

# Add column
client.add_column("email", "String")
client.add_column("score", "Float64")

# Rename column
client.rename_column("email", "email_address")

# Get column type
dtype = client.get_column_dtype("age")
print(f"age column type: {dtype}")  # Int64

# List all fields
fields = client.list_fields()
print(fields)  # ['_id', 'name', 'age', 'email_address', 'score']

# Drop column
client.drop_column("score")

client.close()
```

---

## Full-Text Search

FTS is available via two interfaces. The SQL interface (recommended) works over Python, PG Wire, and Arrow Flight. The Python API provides direct programmatic access.

> See [FTS_GUIDE.md](FTS_GUIDE.md) for the complete reference.

### SQL Interface (Recommended)

```python
client = ApexClient("./data")
client.create_table("articles")

# 1. Create the FTS index via SQL DDL
client.execute("CREATE FTS INDEX ON articles (title, content)")

# 2. Insert data — rows are indexed automatically
client.store([
    {"title": "Python Tutorial",     "content": "Learn Python programming"},
    {"title": "Rust Guide",          "content": "Systems programming with Rust"},
    {"title": "Database Design",     "content": "Designing efficient databases"},
    {"title": "Machine Learning",    "content": "Deep learning with PyTorch"},
])

# 3. MATCH — all query terms must appear
results = client.execute("SELECT * FROM articles WHERE MATCH('python')")
print(results.to_pandas())
#    _id            title                    content
# 0    0  Python Tutorial  Learn Python programming

# 4. FUZZY_MATCH — tolerates typos
results = client.execute("SELECT * FROM articles WHERE FUZZY_MATCH('progaming')")

# 5. Combine FTS with other predicates
results = client.execute("""
    SELECT title FROM articles
    WHERE MATCH('programming') AND _id > 0
    ORDER BY _id DESC LIMIT 5
""")

# 6. FTS + aggregation
n = client.execute("SELECT COUNT(*) FROM articles WHERE MATCH('python')").scalar()
print(f"Python articles: {n}")

# 7. Manage indexes
client.execute("SHOW FTS INDEXES")                       # lists all databases
client.execute("ALTER FTS INDEX ON articles DISABLE")    # suspend, keep files
client.execute("ALTER FTS INDEX ON articles ENABLE")     # resume + back-fill missed rows
client.execute("DROP FTS INDEX ON articles")             # remove + delete files

client.close()
```

### FTS with Options

```python
# Large index: lazy loading + bigger cache
client.execute("""
    CREATE FTS INDEX ON logs (message, source)
    WITH (lazy_load=true, cache_size=100000)
""")

# Cross-interface: after init, PG Wire and Arrow Flight can use MATCH() too
# (No extra configuration needed — FTS registry is global in the Rust executor)
```

### Python API (Alternative)

```python
client = ApexClient("./data")
client.create_table("docs")

client.store([
    {"title": "Python Tutorial", "content": "Learn Python programming"},
    {"title": "Rust Guide",      "content": "Systems programming with Rust"},
    {"title": "Database Design", "content": "Designing efficient databases"},
])

# Initialize FTS (also registers with global SQL executor)
client.init_fts(index_fields=["title", "content"])

# Search — returns numpy array of _ids
ids = client.search_text("Python")
print(f"Found {len(ids)} documents")

# Search and retrieve full records
results = client.search_and_retrieve("programming")
for row in results:
    print(f"Title: {row['title']}")

# Top N results
top_results = client.search_and_retrieve_top("database", n=5)

# Fuzzy search (tolerates typos)
ids = client.fuzzy_search_text("progamming")  # Note typo

# Stats
stats = client.get_fts_stats()
print(f"Documents: {stats['doc_count']}, Terms: {stats['term_count']}")

client.disable_fts()   # suspend (keep files)
client.drop_fts()      # remove (delete files)

client.close()
```

### FTS with Lazy Loading

```python
client = ApexClient("./data")
client.use_table("docs")

# Initialize with lazy loading
client.init_fts(
    index_fields=["content"],
    lazy_load=True,      # Index loaded on first search
    cache_size=50000     # FTS cache size
)

# First search will load the index
results = client.search_and_retrieve("keyword")

client.close()
```

---

## Data Modification

### Replace Records

```python
client = ApexClient("./data")
client.create_table("people")

# Insert test data
client.store({"name": "Alice", "age": 30})

# Replace by ID
success = client.replace(0, {"name": "Alice Smith", "age": 31})

# Batch replace
updated_ids = client.batch_replace({
    0: {"name": "Alice Updated", "age": 32},
    1: {"name": "Bob Updated", "age": 26}
})

client.close()
```

### Delete Records

```python
client = ApexClient("./data")
client.use_table("people")

# Delete single record
client.delete(5)

# Delete multiple records
client.delete([1, 2, 3, 4, 5])

client.close()
```

---

## Utility Methods

```python
client = ApexClient("./data")
client.use_table("users")

# Flush data to disk
client.flush()

# Flush cache (alias for flush)
client.flush_cache()

# Set auto-flush thresholds
client.set_auto_flush(rows=1000)        # Flush every 1000 rows
client.set_auto_flush(bytes=1024*1024)  # Flush every 1MB
client.set_auto_flush(rows=500, bytes=512*1024)

# Get auto-flush config
rows, bytes = client.get_auto_flush()

# Estimate memory usage
mem_bytes = client.estimate_memory_bytes()
print(f"Memory: {mem_bytes / 1024 / 1024:.2f} MB")

# Optimize storage
client.optimize()  # Currently same as flush

client.close()
```

---

## Advanced Usage

### Multi-Table Operations

```python
client = ApexClient("./data")

# Create multiple tables
client.create_table("users")
client.create_table("products")
client.create_table("orders")

# Work with users
client.use_table("users")
client.store([
    {"user_id": 1, "name": "Alice"},
    {"user_id": 2, "name": "Bob"}
])

# Work with products
client.use_table("products")
client.store([
    {"product_id": 1, "name": "Laptop", "price": 999.99},
    {"product_id": 2, "name": "Mouse", "price": 29.99}
])

# Query across tables
users = client.execute("SELECT * FROM users")
client.use_table("products")
products = client.execute("SELECT * FROM products")

# Get row counts for all tables
for table in client.list_tables():
    count = client.count_rows(table)
    print(f"{table}: {count} rows")

client.close()
```

### Durability Options

```python
# Fast - async writes, best performance (default)
client = ApexClient("./data", durability="fast")

# Safe - sync writes, data safety
client = ApexClient("./data", durability="safe")

# Max - fsync every write, maximum durability
client = ApexClient("./data", durability="max")
```

### Working with Large Datasets

```python
# Use columnar storage for bulk inserts
client = ApexClient("./data")
client.create_table("benchmark")

# Generate large dataset
import numpy as np
n = 1_000_000

data = {
    "id": range(n),
    "value": np.random.randn(n),
    "category": np.random.choice(["A", "B", "C"], n)
}

# Columnar insert is much faster
client.store(data)

# Query with limit
results = client.execute("SELECT * FROM benchmark LIMIT 100")

client.close()
```

### Complete Workflow Example

```python
from apexbase import ApexClient
import pandas as pd

# Initialize with safe durability
with ApexClient("./analytics", durability="safe") as client:
    # Create table
    client.create_table("sales")
    
    # Import data from pandas
    df = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=365),
        "product": np.random.choice(["A", "B", "C"], 365),
        "quantity": np.random.randint(1, 100, 365),
        "revenue": np.random.randn(365) * 100 + 500
    })
    client.from_pandas(df)
    
    # Add computed column
    client.add_column("region", "String")
    
    # Query analytics
    monthly = client.execute("""
        SELECT 
            strftime('%Y-%m', date) as month,
            SUM(revenue) as total_revenue,
            AVG(quantity) as avg_quantity
        FROM sales
        GROUP BY month
        ORDER BY month
    """)
    
    print(monthly.to_pandas())
    
    # Initialize FTS for product search
    client.init_fts(index_fields=["product"])
    
    # Search products
    results = client.search_and_retrieve("A")
    print(f"Found {len(results)} records")
    
    # Cleanup
    client.drop_fts()
    
# Client automatically closed
```

---

## File Reading Table Functions

Read external files directly inside a SQL `FROM` clause — no data import or table creation needed. The full SQL engine (WHERE, GROUP BY, ORDER BY, LIMIT, JOIN, UNION) applies to the result.

### read_csv

```python
from apexbase import ApexClient
import tempfile, os

client = ApexClient(tempfile.mkdtemp())

# Minimal — auto-infers comma delimiter and header row
df = client.execute("SELECT * FROM read_csv('/path/to/file.csv')").to_pandas()

# Tab-separated values
df = client.execute("SELECT * FROM read_csv('/path/to/file.tsv', delimiter='\t')").to_pandas()

# No header row (columns named col0, col1, ...)
df = client.execute("SELECT * FROM read_csv('/path/to/raw.csv', header=false)").to_pandas()

# Filter and aggregate on the file
result = client.execute("""
    SELECT region, COUNT(*) AS orders, SUM(revenue) AS total_rev
    FROM read_csv('/data/sales_2024.csv')
    WHERE revenue > 0
    GROUP BY region
    ORDER BY total_rev DESC
    LIMIT 20
""")
for row in result:
    print(row)

# Read into Arrow Table (zero-copy)
table = client.execute("SELECT * FROM read_csv('/data/large.csv')").to_arrow()

# Read into polars DataFrame
import polars as pl
df = client.execute("SELECT * FROM read_csv('/data/data.csv')").to_polars()
```

### read_parquet

```python
# Schema is taken from the Parquet file's own metadata
df = client.execute("SELECT * FROM read_parquet('/data/events.parquet')").to_pandas()

# Projection (only read needed columns)
df = client.execute("""
    SELECT user_id, event_type, ts
    FROM read_parquet('/data/events.parquet')
    WHERE event_type = 'purchase'
""").to_pandas()

# Aggregate directly
result = client.execute("""
    SELECT event_type, COUNT(*) AS cnt, AVG(amount) AS avg_amount
    FROM read_parquet('/data/transactions.parquet')
    GROUP BY event_type
    ORDER BY cnt DESC
""")

# To Arrow — zero-copy from mmap
import pyarrow as pa
table = client.execute("SELECT * FROM read_parquet('/data/wide.parquet')").to_arrow()
```

### read_json

```python
# NDJSON (one JSON object per line — .json, .jsonl, .ndjson)
df = client.execute("SELECT * FROM read_json('/data/logs.ndjson')").to_pandas()

# pandas-exported JSON (df.to_json(orient='columns') or orient='split')
df = client.execute("SELECT * FROM read_json('/data/export.json')").to_pandas()

# Filter and aggregate
result = client.execute("""
    SELECT level, COUNT(*) AS cnt
    FROM read_json('/data/app_logs.json')
    WHERE level IN ('ERROR', 'WARN')
    GROUP BY level
    ORDER BY cnt DESC
""")
```

### Combining file reads with tables and set operations

```python
client.create_table("users")
client.store([{"id": 1, "email": "alice@example.com"}, {"id": 2, "email": "bob@example.com"}])

# JOIN: enrich stored users with scores from a file
result = client.execute("""
    SELECT u.email, s.score
    FROM users u
    JOIN read_csv('/data/scores.csv') s ON u.id = s.user_id
    WHERE s.score >= 80
    ORDER BY s.score DESC
""")

# UNION ALL: merge stored table with new rows from a file
result = client.execute("""
    SELECT email FROM users
    UNION ALL
    SELECT email FROM read_csv('/data/new_signups.csv')
""")

# EXCEPT: remove unsubscribed addresses from the list
result = client.execute("""
    SELECT email FROM users
    EXCEPT
    SELECT email FROM read_csv('/data/unsubscribed.csv')
""")
```

**read_csv options summary:**

| Option | Default | Aliases | Description |
|--------|---------|---------|-------------|
| `header` | `true` | — | `false` or `0` to treat first row as data |
| `delimiter` | `,` | `delim`, `sep` | Single-character field separator |

---

## Vector Search

ApexBase provides SIMD-accelerated nearest-neighbour search over vector columns. The engine scans an OS-level mmap float buffer that is populated once and reused, delivering 3–4× speedups over DuckDB at 1M rows.

### Storing vectors

```python
import numpy as np
from apexbase import ApexClient

client = ApexClient("./vecdb")
client.create_table("items")

# Bulk insert — numpy arrays become FixedList columns (optimal path)
N, D = 100_000, 128
vecs = np.random.rand(N, D).astype(np.float32)
client.store({
    "label": [f"item_{i}" for i in range(N)],
    "vec":   list(vecs),   # list of 1-D numpy arrays
})

# Single record
client.store({"label": "query_item", "vec": np.array([0.1] * D, dtype=np.float32)})

# Python list/tuple vectors also work
client.store({"label": "list_item", "vec": [0.1, 0.2, 0.3]})
```

### Quantized candidate retrieval with exact reranking

Keep the authoritative vector and add a separate compressed accelerator. The
accelerator generates a wider candidate set; ApexBase then recomputes distances
from `vec` and returns the exact order within that set.

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

# Removes the compressed column; vec and ordinary exact search remain intact.
client.drop_quantized_column("vec_tq4")
```

Supported codecs are `float32`, `float16`, `bfloat16`, `int8`, `uint8`,
`bit1`, `turboquant2`, `turboquant3`, and `turboquant4`. See the
[Vector Quantization Guide](VECTOR_QUANTIZATION_GUIDE.md) for storage sizes,
recall tradeoffs, automatic maintenance, and deletion constraints.

### `topk_distance` — single-query search

Returns the k nearest neighbours for one query vector as a `ResultView`.

```python
query = np.random.rand(D).astype(np.float32)

# Default: L2 distance, top 10
results = client.topk_distance('vec', query, k=10)
df = results.to_pandas()
print(df.columns.tolist())   # ['_id', 'dist']
print(df.head())
#    _id      dist
# 0   42  0.312...
# 1   17  0.318...
# ...

# Cosine distance
results = client.topk_distance('vec', query, k=5, metric='cosine')

# Dot product (inner product) — maximise similarity
results = client.topk_distance('vec', query, k=10, metric='dot')

# Custom output column names
results = client.topk_distance(
    'vec', query, k=10, metric='l2',
    id_col='item_id', dist_col='l2_dist',
)
df = results.to_pandas()
print(df.columns.tolist())  # ['item_id', 'l2_dist']
```

**Retrieve full records for the top-k results:**

```python
results  = client.topk_distance('vec', query, k=10)
top_ids  = results.get_ids()                    # numpy int64 array
records  = client.retrieve_many(top_ids.tolist())  # ResultView with all columns
print(records.to_pandas())
```

**All supported metrics:**

| `metric` | Aliases | Notes |
|---|---|---|
| `'l2'` | `'euclidean'` | Euclidean distance (default) |
| `'l2_squared'` | — | Squared L2 (avoids sqrt, same ranking) |
| `'l1'` | `'manhattan'` | Sum of absolute differences |
| `'linf'` | `'chebyshev'` | Maximum absolute difference |
| `'cosine'` | `'cosine_distance'` | 1 − cosine similarity |
| `'dot'` | `'inner_product'` | Negated dot product (min = max similarity) |

### `batch_topk_distance` — N queries in one call

Significantly faster than N sequential `topk_distance` calls when the scan buffer is warm.

```python
import numpy as np

N_queries = 200
queries = np.random.rand(N_queries, D).astype(np.float32)

# Returns ndarray of shape (N_queries, k, 2)
result = client.batch_topk_distance('vec', queries, k=10)
print(result.shape)   # (200, 10, 2)

# Extract IDs and distances
ids   = result[:, :, 0].astype(np.int64)  # shape (200, 10)
dists = result[:, :, 1]                   # shape (200, 10)

# Best (nearest) neighbour for each query
nearest_id   = ids[:, 0]    # shape (200,)
nearest_dist = dists[:, 0]  # shape (200,)

# Batch cosine search
result_cos = client.batch_topk_distance('vec', queries, k=5, metric='cosine')

# Single-query convenience: 1-D array is accepted as N=1
result_single = client.batch_topk_distance('vec', queries[0], k=10)
print(result_single.shape)  # (1, 10, 2)
```

**Result layout:**

```
result[i, j, 0]  →  _id  of the j-th nearest neighbour for query i
result[i, j, 1]  →  dist of the j-th nearest neighbour for query i
```

Rows padded with `(-1, inf)` when fewer than k neighbours exist.

### SQL: `explode_rename(topk_distance(...))`

Vector search is also available as a SQL expression. The Python `topk_distance()` method builds this SQL internally; you can write it directly for more complex compositions.

```python
# Basic SQL vector search
results = client.execute("""
    SELECT explode_rename(topk_distance(vec, [0.1, 0.2, 0.3], 10, 'l2'), '_id', 'dist')
    FROM items
""")
df = results.to_pandas()
# 10 rows: _id (int64), dist (float64), sorted nearest first

# Cosine search with custom names
results = client.execute("""
    SELECT explode_rename(
        topk_distance(vec, [1.0, 0.0, 0.0], 5, 'cosine'),
        'item_id', 'cosine_dist'
    )
    FROM items
""")

# Dot product search
results = client.execute("""
    SELECT explode_rename(topk_distance(vec, [0.5, 0.5, 0.5], 20, 'dot'), '_id', 'score')
    FROM embeddings
""")
```

**SQL syntax summary:**

```sql
SELECT explode_rename(
    topk_distance(col, [q1, q2, ..., qD], k, 'metric'),
    'id_column_name',
    'dist_column_name'
)
FROM table_name
```

- The query vector must be an inline array literal `[f1, f2, ...]`.
- For dynamic Python query vectors, prefer `client.topk_distance()` which formats the literal automatically.

### End-to-end example: embedding search

```python
import numpy as np
from apexbase import ApexClient

DIM = 64
client = ApexClient("./embed_db")
client.create_table("docs")

# Insert documents with embeddings
docs = [
    {"title": "Introduction to Rust",   "vec": np.random.rand(DIM).astype(np.float32)},
    {"title": "Python Data Science",    "vec": np.random.rand(DIM).astype(np.float32)},
    {"title": "Deep Learning Basics",   "vec": np.random.rand(DIM).astype(np.float32)},
    {"title": "Vector Database Guide",  "vec": np.random.rand(DIM).astype(np.float32)},
]
client.store(docs)

# Query: find the 3 most similar documents
query_vec = np.random.rand(DIM).astype(np.float32)
top = client.topk_distance('vec', query_vec, k=3, metric='cosine')

# Retrieve full records
top_ids  = top.get_ids()
results  = client.retrieve_many(top_ids.tolist())
for row in results:
    print(row['title'], row['_id'])

# Batch: multiple query vectors at once
queries = np.random.rand(10, DIM).astype(np.float32)
batch   = client.batch_topk_distance('vec', queries, k=3, metric='cosine')
ids   = batch[:, :, 0].astype(np.int64)   # (10, 3)
dists = batch[:, :, 1]                    # (10, 3)
print("Top-3 IDs for each query:")
print(ids)
```

---

## Temporary Tables from Files

Register CSV, JSON, or Parquet files as native temp tables. The file is parsed once; all subsequent queries use the mmap-backed `.apex` format (zone maps, bloom filters, zero-copy reads).

### Register a CSV file

```python
# Register — parses and materializes as native .apex
client.register_temp_table("sales", "/data/sales.csv")

# Query — no parsing overhead on subsequent calls
monthly = client.execute("""
    SELECT category, SUM(amount) AS total
    FROM sales
    WHERE amount > 100
    GROUP BY category
    ORDER BY total DESC
    LIMIT 10
""")

# JOIN with a persistent table
result = client.execute("""
    SELECT s.*, u.name
    FROM sales s
    JOIN users u ON s.user_id = u._id
    WHERE s.city = 'Beijing'
""")
```

### Register JSON / NDJSON

```python
client.register_temp_table("logs", "/data/events.ndjson")

# Aggregate over the temp table
counts = client.execute("""
    SELECT event_type, COUNT(*) AS cnt
    FROM logs
    GROUP BY event_type
""").to_dict()
```

### Register Parquet

```python
client.register_temp_table("users", "/data/users.parquet")

count = client.execute("SELECT COUNT(*) FROM users WHERE age > 30").scalar()
```

### Drop a temp table

```python
client.drop_temp_table("sales")
# Temp tables are also auto-cleaned when client is closed
```

### CREATE TEMP TABLE (SQL syntax)

```python
client.execute("CREATE TEMP TABLE invoices AS SELECT * FROM read_csv('/data/invoices.csv')")
```

### Performance comparison

For workloads that query the same file repeatedly, `register_temp_table()` provides dramatic speedups:

```python
import time

# Slow: re-parses CSV every time
t0 = time.perf_counter()
for _ in range(100):
    client.execute("SELECT COUNT(*) FROM read_csv('/data/big.csv')")
print(f"read_csv repeated: {(time.perf_counter()-t0)*1000:.0f}ms")

# Fast: parse once, query native .apex
client.register_temp_table("big", "/data/big.csv")
t0 = time.perf_counter()
for _ in range(100):
    client.execute("SELECT COUNT(*) FROM big")
print(f"temp table: {(time.perf_counter()-t0)*1000:.0f}ms")

client.drop_temp_table("big")
```

---

## Set Operations

Set operations combine result sets from two `SELECT` statements. Both sides must produce the same number of columns.

### UNION — deduplicated

```python
client.execute("CREATE TABLE a (val INT)")
client.execute("INSERT INTO a VALUES (1),(2),(3),(4)")
client.execute("CREATE TABLE b (val INT)")
client.execute("INSERT INTO b VALUES (2),(3),(5),(6)")

# UNION removes duplicates — result: [1, 2, 3, 4, 5, 6]
result = client.execute("""
    SELECT val FROM a
    UNION
    SELECT val FROM b
    ORDER BY val
""")
assert [r['val'] for r in result] == [1, 2, 3, 4, 5, 6]
```

### UNION ALL — all rows including duplicates

```python
# UNION ALL keeps duplicates — result: 8 rows (2, 3 appear twice)
result = client.execute("""
    SELECT val FROM a
    UNION ALL
    SELECT val FROM b
    ORDER BY val
""")
assert len(result) == 8
```

### INTERSECT — rows in both sides

```python
# INTERSECT returns rows present in both a and b — result: [2, 3]
result = client.execute("""
    SELECT val FROM a
    INTERSECT
    SELECT val FROM b
    ORDER BY val
""")
assert [r['val'] for r in result] == [2, 3]
```

### EXCEPT — rows only in left side

```python
# EXCEPT returns rows in a that are NOT in b — result: [1, 4]
result = client.execute("""
    SELECT val FROM a
    EXCEPT
    SELECT val FROM b
    ORDER BY val
""")
assert [r['val'] for r in result] == [1, 4]
```

### Practical examples

```python
# Find customers who both placed an order AND have a wishlist entry
result = client.execute("""
    SELECT user_id FROM orders
    INTERSECT
    SELECT user_id FROM wishlist
""")

# Find customers who ordered but have NO open support ticket
result = client.execute("""
    SELECT user_id FROM orders
    EXCEPT
    SELECT user_id FROM support_tickets WHERE status = 'open'
""")

# Combine users from two separate databases
result = client.execute("""
    SELECT name, email FROM default.users
    UNION
    SELECT name, email FROM analytics.trial_users
    ORDER BY name
""")

# Set operations work with read_csv too
result = client.execute("""
    SELECT email FROM subscribers
    EXCEPT
    SELECT email FROM read_csv('/data/bounced_emails.csv')
""")
```

### Quick reference

| Operation | Duplicates | Returns |
|-----------|-----------|------|
| `UNION` | removed | rows in left **or** right |
| `UNION ALL` | kept | all rows from both sides |
| `INTERSECT` | removed | rows in left **and** right |
| `EXCEPT` | removed | rows in left but **not** right |

---

## API Summary

### ApexClient Methods

**Initialization:** `__init__`, `create_clean`, `close`

**Table Management:** `use_table`, `create_table`, `drop_table`, `list_tables`, `current_table`

**Data Storage:** `store`, `from_pandas(df, table_name=)`, `from_polars(df, table_name=)`, `from_pyarrow(table, table_name=)`

**Query:** `execute`, `query`, `retrieve`, `retrieve_many`, `retrieve_all`, `count_rows`

**Vector Search:** `topk_distance(col, query, k=10, metric='l2', id_col='_id', dist_col='dist')`, `batch_topk_distance(col, queries, k=10, metric='l2')`

**Modification:** `replace`, `batch_replace`, `delete`

**Columns:** `add_column`, `drop_column`, `rename_column`, `get_column_dtype`, `list_fields`

**FTS:** `init_fts`, `search_text`, `fuzzy_search_text`, `search_and_retrieve`, `search_and_retrieve_top`, `get_fts_stats`, `disable_fts`, `drop_fts`

**Utility:** `flush`, `flush_cache`, `set_auto_flush`, `get_auto_flush`, `estimate_memory_bytes`, `optimize`

### ResultView Methods

**Conversion:** `to_pandas`, `to_polars`, `to_arrow`, `to_dict`

**Access:** `scalar`, `first`, `get_ids`, `__len__`, `__iter__`, `__getitem__`

**Properties:** `shape`, `columns`

---

*This documentation covers 100% of the public ApexBase Python API.*
