# Quick Start Guide

Get started with ApexBase in 5 minutes.

## Installation

```bash
pip install apexbase
```

Or from source:
```bash
git clone https://github.com/BirchKwok/ApexBase.git
pip install maturin
maturin develop --release
```

## Basic Example

```python
from apexbase import ApexClient

# 1. Create client
client = ApexClient("./my_data")

# 2. Create a table (required before any data operations)
client.create_table("users")

# 3. Store data
client.store({"name": "Alice", "age": 30, "city": "NYC"})

# 4. Query
results = client.execute("SELECT * FROM users WHERE age > 25")

# 5. Use results
df = results.to_pandas()
print(df)

# 6. Close
client.close()
```

## Working with Tables

ApexBase requires explicit table creation before any data operations. Each table is stored as a separate `.apex` file.

```python
client = ApexClient("./data")

# Create tables (the last created table becomes the active table)
client.create_table("users")
client.store({"name": "Bob", "email": "bob@example.com"})

# Create table with pre-defined schema (avoids type inference on first insert)
client.create_table("orders", schema={
    "user_id": "int64",
    "amount": "float64",
    "product": "string"
})
client.store({"user_id": 0, "amount": 100.0, "product": "Widget"})

# Switch between tables
client.use_table("users")

# List tables
print(client.list_tables())  # ['users', 'orders']

# Reopen an existing database
client2 = ApexClient("./data")
client2.use_table("users")  # Select an existing table

client.close()
```

## SQL DDL (Data Definition Language)

ApexBase supports full SQL DDL operations:

```python
client = ApexClient("./data")

# CREATE TABLE
client.execute("CREATE TABLE employees")
client.execute("CREATE TABLE IF NOT EXISTS departments")  # No error if exists

# ALTER TABLE
client.execute("ALTER TABLE employees ADD COLUMN name STRING")
client.execute("ALTER TABLE employees ADD COLUMN age INT")

# INSERT
client.execute("INSERT INTO employees (name, age) VALUES ('Alice', 30)")
client.execute("INSERT INTO employees (name, age) VALUES ('Bob', 25), ('Charlie', 35)")

# Query
results = client.execute("SELECT * FROM employees WHERE age > 25")

# DROP TABLE
client.execute("DROP TABLE employees")
client.execute("DROP TABLE IF EXISTS departments")  # No error if not exists

# Check tables
print(client.list_tables())

client.close()
```

### Multi-Statement SQL

Execute multiple SQL statements in a single call using semicolons:

```python
client = ApexClient("./data")

# Execute multiple DDL statements at once
client.execute("""
    CREATE TABLE IF NOT EXISTS products;
    ALTER TABLE products ADD COLUMN name STRING;
    ALTER TABLE products ADD COLUMN price FLOAT;
    INSERT INTO products (name, price) VALUES ('Laptop', 999.99)
""")

# Execute multiple INSERT statements
client.execute("""
    INSERT INTO products (name, price) VALUES ('Mouse', 29.99);
    INSERT INTO products (name, price) VALUES ('Keyboard', 79.99);
    INSERT INTO products (name, price) VALUES ('Monitor', 299.99)
""")

# Query results
results = client.execute("SELECT * FROM products ORDER BY price DESC")
print(results.to_pandas())

client.close()
```

**Supported DDL / DML Statements:**
- `CREATE TABLE [IF NOT EXISTS] table_name`
- `ALTER TABLE ... ADD COLUMN column_name TYPE`
- `INSERT INTO ... VALUES ...`
- `DROP TABLE [IF EXISTS] table_name`

**Multi-Statement SQL:**
- Separate statements with semicolons (`;`)
- Statements are executed sequentially
- The result of the last statement is returned

## Bulk Data Import

```python
import pandas as pd

client = ApexClient("./data")

# from_pandas with table_name auto-creates and selects the table
df = pd.DataFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
})
client.from_pandas(df, table_name="users")

# From columnar dict (fastest, requires active table)
client.store({
    "product": ["A", "B", "C"],
    "price": [10.5, 20.0, 15.0],
    "quantity": [100, 200, 150]
})

client.close()
```

## SQL Queries

```python
client = ApexClient("./data")
client.create_table("metrics")

# Insert test data
for i in range(100):
    client.store({"id": i, "value": i * 10})

# Basic query (use your table name in FROM clause)
results = client.execute("SELECT * FROM metrics WHERE value > 500")

# Aggregation
scalar = client.execute("SELECT COUNT(*) FROM users").scalar()
avg = client.execute("SELECT AVG(value) FROM metrics").scalar()

# GROUP BY
results = client.execute("""
    SELECT category, COUNT(*), AVG(price)
    FROM products
    GROUP BY category
""")

# ORDER BY with LIMIT
results = client.execute("""
    SELECT * FROM metrics
    ORDER BY value DESC
    LIMIT 10
""")

client.close()
```

### Quoted Identifiers

If a column name is a SQL reserved keyword (e.g. `order`, `group`), wrap it in backticks or double quotes:

```python
# Backtick style (Hive/MySQL)
results = client.execute("SELECT `order`, `group` FROM t WHERE `order` > 10")

# Double-quote style (SQL standard)
results = client.execute('SELECT "order", "group" FROM t WHERE "order" > 10')
```

## Full-Text Search

```python
client = ApexClient("./data")
client.create_table("docs")

# Add documents
client.store([
    {"title": "Python Guide", "content": "Learn Python programming"},
    {"title": "Rust Tutorial", "content": "Systems programming with Rust"},
    {"title": "Database Design", "content": "Designing efficient databases"}
])

# Initialize FTS
client.init_fts(index_fields=["title", "content"])

# Search
ids = client.search_text("Python")
print(f"Found {len(ids)} documents")

# Search and retrieve records
results = client.search_and_retrieve("programming")
for row in results:
    print(row["title"])

# Fuzzy search (handles typos)
ids = client.fuzzy_search_text("progamming")  # Note the typo

client.close()
```

## Column Operations

```python
client = ApexClient("./data")
client.create_table("people")
client.store({"name": "Alice", "age": 30})

# Add column
client.add_column("email", "String")

# Rename column
client.rename_column("email", "contact_email")

# Get column type
dtype = client.get_column_dtype("age")
print(f"age is {dtype}")  # Int64

# Drop column
client.drop_column("contact_email")

# List fields
fields = client.list_fields()
print(fields)  # ['_id', 'name', 'age']

client.close()
```

## Context Manager (Recommended)

```python
# Automatic cleanup
with ApexClient("./data") as client:
    client.create_table("mydata")
    client.store({"key": "value"})
    results = client.execute("SELECT * FROM mydata")
    df = results.to_pandas()
    # Client automatically closed

# With clean slate
with ApexClient.create_clean("./fresh_data") as client:
    client.create_table("mydata")
    client.store({"fresh": "start"})
```

## Durability Options

```python
# Fast (default) - async writes, best performance
client = ApexClient("./data", durability="fast")

# Safe - sync writes, data safety
client = ApexClient("./data", durability="safe")

# Max - fsync every write, maximum durability
client = ApexClient("./data", durability="max")
```

## ResultView Operations

```python
results = client.execute("SELECT * FROM users")

# Different formats
df = results.to_pandas()
pl_df = results.to_polars()
arrow = results.to_arrow()
dicts = results.to_dict()

# Properties
print(results.shape)      # (rows, columns)
print(results.columns)    # ['_id', 'name', 'age']
print(len(results))       # row count

# Access
first = results.first()
ids = results.get_ids()   # numpy array

# Iteration
for row in results:
    print(row)

# Indexing
row = results[0]
```

## Next Steps

- [API Reference](API_REFERENCE.md) - Complete API documentation
- [Examples](EXAMPLES.md) - More usage examples
- [Core Concepts](concepts.md) - Mental model for databases, tables, durability, and results
- [Project README](https://github.com/BirchKwok/ApexBase#readme) - Repository overview
