# Data Import

ApexBase moves data in and out of Python without a custom ETL job: records,
columnar batches, DataFrames, files, Parquet, and Lance datasets all land in the
same table format.

## Choose An Import Path

| Source | Best method |
| --- | --- |
| Python records | `client.store(dict)` or `client.store(list[dict])` |
| Large Python batches | `client.store(columnar_dict)` |
| Pandas / Polars / Arrow | `client.from_pandas()` / `from_polars()` / `from_pyarrow()` |
| Embeddings for RAG | `store()` with a NumPy `(n, dim)` array in a columnar dict |
| CSV / JSON / Parquet one-off query | SQL `read_csv()`, `read_json()`, `read_parquet()` |
| CSV / JSON / Parquet repeated queries | `register_temp_table()` |
| Parquet export | SQL `COPY <table> TO ... (FORMAT PARQUET)` |
| Lance | `client.from_lance()` / `client.to_lance()` |

## Records And Columnar Batches

```python
client.create_table("events")
client.store({
    "user_id": [1, 2, 3],
    "event": ["signup", "click", "purchase"],
    "value": [0.0, 0.0, 29.9],
})
```

The columnar dict is the fastest pure-Python ingest; an explicit schema avoids
type inference on the first write
(`create_table("users", {"name": "string", "age": "int64"})`).

## DataFrames

```python
import pandas as pd
import polars as pl
import pyarrow as pa

client.from_pandas(pd.DataFrame({"name": ["Alice"], "age": [30]}), table_name="users")
client.from_polars(pl.DataFrame({"name": ["Bob"], "age": [25]}), table_name="users")
client.from_pyarrow(pa.table({"name": ["Charlie"], "age": [35]}), table_name="users")
```

`table_name` selects or creates the target table.

## Bulk Embeddings For RAG

Pass the embedding matrix as one NumPy array inside a columnar dict.
`float16_vector` halves the disk cost; `float32_vector` avoids the conversion:

```python
import numpy as np

dim = 768
vectors = np.random.rand(50_000, dim).astype(np.float32)

client.create_table("chunks", {"content": "text", "embedding": "float16_vector"})
client.use_table("chunks")
client.store({
    "content": [f"chunk {i}" for i in range(50_000)],
    "embedding": vectors,
})
client.execute("CREATE FTS INDEX ON chunks(content)")
```

Create the FTS index after the bulk load so it covers every chunk without
incremental updates. A Pandas frame whose embedding column holds Python lists
also imports (`from_pandas(df, table_name="chunks")`), but the columnar NumPy
route avoids building 50,000 Python lists. Mixed embedding dimensions are
rejected instead of truncated.

## Query Files Directly

```python
result = client.execute("""
    SELECT city, COUNT(*) AS rows
    FROM read_csv('events.csv')
    GROUP BY city
    ORDER BY rows DESC
""")
```

`read_parquet('orders.parquet')` and `read_json('events.ndjson')` work the same
way. File functions can join stored tables:
`JOIN read_csv('events.csv') f ON u.id = f.user_id`.

## Register Temporary Tables

For repeated queries, parse a file once into a temporary table:

```python
client.register_temp_table("events_file", "events.csv")

client.execute("""
    SELECT event, COUNT(*) AS rows
    FROM events_file
    GROUP BY event
""")

client.drop_temp_table("events_file")
```

Temporary tables are cleaned up when the client closes.

## Parquet And Lance

Export with `COPY ... TO`; import Parquet with `read_parquet()`,
`from_pyarrow()`, or a temporary table:

```python
import pyarrow.parquet as pq

client.execute("COPY users TO 'users.parquet' (FORMAT PARQUET)")
client.from_pyarrow(pq.read_table("users.parquet"), table_name="users_imported")

client.register_temp_table("users_file", "users.parquet")
client.execute("SELECT COUNT(*) FROM users_file")
client.drop_temp_table("users_file")
```

In 2.0.0, `COPY ... FROM` supports CSV; use the routes above for Parquet. For
CSV round trips, `COPY users TO 'users.csv' (FORMAT CSV, HEADER)` and
`COPY users_imported FROM 'users.csv' (FORMAT CSV, HEADER)`.

Lance datasets both ways:

```python
client.use_table("users")   # to_lance() exports the current selection
client.to_lance("users.lance")
client.from_lance("users.lance", table_name="users_from_lance")
```

`from_lance(uri, columns=..., filter=..., limit=...)` pushes those options into
Lance's Arrow reader.

## Performance Tips

- Create tables with explicit schemas for large repeated imports.
- Prefer columnar dicts, NumPy arrays, or DataFrame import over row dictionaries.
- Load embeddings as one `(n, dim)` array, then create the FTS index.
- Use `register_temp_table()` when querying the same file many times.
- Use `durability="fast"` for scratch imports and `"safe"`/`"max"` for app data.
- Import in batches sized to your memory budget.

## Where To Go Next

- [Python Client](python-client.md) for client lifecycle and typed projection.
- [SQL Guide](sql.md) for `read_csv()`/`read_parquet()` in queries.
- [Agent Memory & RAG](agent-memory.md) for the retrieval side of a bulk load.
- [Python API Reference](../API_REFERENCE.md) for `from_lance`, `to_lance`, and
  `register_temp_table` signatures.
