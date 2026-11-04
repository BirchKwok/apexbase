# Server Protocols

ApexBase is embedded by default; the same data directory can also be exposed over
PostgreSQL Wire and Arrow Flight when an external tool needs to attach.

## Which Protocol Should I Use?

| Interface | Best for | Command |
| --- | --- | --- |
| Python API | Embedded apps, scripts, notebooks, agents | `ApexClient("./data")` |
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

Custom ports, or one protocol only:

```bash
apexbase-serve --dir ./data --pg-port 15432 --flight-port 50052
apexbase-serve --dir ./data --no-flight
apexbase-serve --dir ./data --no-pg
```

## PostgreSQL Wire

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

Authentication is disabled for the local server in 2.0.0: any credentials a
client supplies are accepted.

### Transactions And Routing

Every transport answers through the same statement classifier: DML/DDL/transaction
statements return a command tag and everything else streams rows.

- **Simple query** — a statement batch that is not a single auto-commit statement
  runs as one transaction. A multi-statement message without explicit control is
  an **implicit transaction**: if any statement fails, earlier statements in that
  message are rolled back.
- **Session state** — `BEGIN`/`COMMIT`/`ROLLBACK` are per connection and work
  across messages. An error inside a transaction rolls it back and returns the
  connection to auto-commit.
- **Explicit errors** — `BEGIN` inside an open transaction is an error, and a
  `ROLLBACK` outside one is an error. `USE`/`\c` inside a multi-statement batch or
  a transaction is rejected; issue it as its own simple query.

### COPY Routing

`COPY <new_table> FROM '<file>'` answers without a selected table: it creates and
publishes the table and makes it the connection's selected table.
`COPY <table> TO '<file>'` requires a known or selected table. File paths resolve
against the server process working directory, so use absolute paths or start the
server from the directory that holds the files.

```sql
COPY people FROM '/data/people.csv' (FORMAT CSV, HEADER);
COPY people TO '/data/people_out.csv' (FORMAT CSV, HEADER);
```

## Arrow Flight

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

Flight is the best option when result sets are large and the consumer works with
Arrow. Single-table projections stream through a bounded channel (capacity two),
so a slow consumer applies backpressure to the server instead of making it buffer
the whole result; materialized results are delivered in 65,536-row chunks. A
client that disconnects cancels the in-flight query.

```python
reader = client.do_get(fl.Ticket(b"SELECT id, payload FROM events"))
for chunk in reader:
    print(chunk.data.num_rows, "rows")   # pyarrow.RecordBatch chunks
```

## Concurrency And Writers

The servers handle concurrent statements through the engine's process admission
path: with `APEX_PROCESS_BUDGET_MB` set, statements reserve memory and wait in a
bounded queue instead of exhausting the machine. Run one writer-heavy workload at
a time unless you have tested your own pattern; multi-process writes reserve
`_id` ranges from an on-disk high-water mark, so ids stay unique across processes.

Long-running queries share the `APEX_QUERY_MEMORY_MB` budget and `APEX_SPILL_DIR`
of embedded clients. Set the environment when starting the server.

## Security

The local server does not authenticate or encrypt connections in 2.0.0. Bind to
`127.0.0.1` unless you need remote access, and put authentication and TLS in
front of it (an SSH tunnel or trusted proxy) before exposing it. Treat the data
directory as the security boundary: anyone who can start a server on it can read
and write the data.

## Operational Notes

- Use the same `--dir` that your embedded Python or Rust app uses.
- `apexbase-server` and `apexbase-flight` accept `--host` (default `127.0.0.1`)
  plus `--dir` and `--port`; `apexbase-serve` uses `--pg-port`/`--flight-port`.
- A slow Flight consumer throttles the server by design; do not add an unbounded
  queue in front of it.
- Check `client._storage._admission_stats()` in embedded code and
  `[APEX_ADMISSION_*]` errors in server logs when statements are rejected.

## Where To Go Next

- [Python Client](python-client.md) for the embedded API.
- [SQL Guide](sql.md#transactions) for transaction and commit semantics.
- [Performance](../performance.md) for measured numbers.
