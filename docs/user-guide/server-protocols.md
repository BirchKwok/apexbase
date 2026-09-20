# Server Protocols

ApexBase is embedded by default, but it can also expose the same data over PostgreSQL Wire and Arrow Flight.

## Which Protocol Should I Use?

| Interface | Best for | Command |
| --- | --- | --- |
| Python API | Embedded apps, scripts, notebooks | `ApexClient("./data")` |
| PostgreSQL Wire | DBeaver, DataGrip, psql, pgAdmin, BI tools, libpq clients | `apexbase-server` |
| Arrow Flight | Fast columnar transfer into Arrow-native systems | `apexbase-flight` |
| Combined server | Running both protocols from one process | `apexbase-serve` |

## Start Both Servers

```bash
apexbase-serve --dir ./data
```

Default ports:

| Protocol | Port |
| --- | --- |
| PostgreSQL Wire | `5432` |
| Arrow Flight | `50051` |

Custom ports:

```bash
apexbase-serve --dir ./data --pg-port 15432 --flight-port 50052
```

Disable one protocol:

```bash
apexbase-serve --dir ./data --no-flight
apexbase-serve --dir ./data --no-pg
```

## PostgreSQL Wire

Start only the PostgreSQL-compatible server:

```bash
apexbase-server --dir ./data --host 127.0.0.1 --port 5432
```

Connect with `psql`:

```bash
psql -h 127.0.0.1 -p 5432 -d apexbase
```

Connect with Python:

```python
import psycopg2

conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="apexbase")
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM users")
print(cur.fetchall())
```

Authentication is currently disabled for the local server; any credentials supplied by clients are accepted.

## Arrow Flight

Start only the Flight server:

```bash
apexbase-flight --dir ./data --host 127.0.0.1 --port 50051
```

Query with PyArrow Flight:

```python
import pyarrow.flight as fl

client = fl.connect("grpc://127.0.0.1:50051")
table = client.do_get(fl.Ticket(b"SELECT * FROM users LIMIT 100")).read_all()
df = table.to_pandas()
```

Arrow Flight is the best option when result sets are large and the consumer can work with Arrow tables.

Supported single-table projections stream from the executor through a bounded
channel (capacity two), so a slow consumer applies backpressure to the server
instead of making it buffer the whole result. Results that must be materialized
first are delivered in 65,536-row chunks. A client that disconnects cancels the
in-flight query.

## Operational Notes

- Use the same `--dir` that your embedded Python or Rust app uses.
- Run one writer-heavy workload at a time unless your application has tested its concurrency pattern.
- Prefer binding to `127.0.0.1` for local tools.
- Put the server behind your own network controls if exposing it beyond the machine.
- Both `apexbase-server` and `apexbase-flight` accept `--host` (default `127.0.0.1`) in addition to `--dir` and `--port`.
