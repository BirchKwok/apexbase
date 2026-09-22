# ApexBase real-world usage scenarios

This directory collects **ready-to-run** ApexBase usage scenarios covering both the
Python and Rust APIs. Every scenario is a **self-contained, runnable single file**:
it builds deterministic synthetic data, needs no network, is repeatable, and ships
with detailed comments and self-checking assertions.

> Location note: this directory is separate from the repository's original `examples/`.
> That directory is kept as the Rust benchmark fixture (two of its files are imported
> directly by pytest) and serves a different purpose from the usage scenarios in this
> document. Do not confuse the two.

## Directory layout

```
usage-examples/
├── README.md          # this file: index + capability boundaries + known defects
├── Python/            # 17 Python scenarios
│   ├── _demo_env.py   # shared helper module (not a scenario; imported by the examples)
│   └── pyNN_*.py
└── Rust/              # 9 Rust scenarios
    └── rNN_*.rs
```

The Python examples are all run from the `usage-examples/Python` directory so that
`_demo_env.py` can be imported directly. Every scenario writes its data files into its
own `_out/` subdirectory, so they never interfere with each other.

## Python scenarios (17)

How to run (from the `usage-examples/Python` directory):

```bash
python py01_parquet_sales_analytics.py
```

| # | File | Scenario | Main capabilities demonstrated |
|---|---|---|---|
| 01 | `py01_parquet_sales_analytics.py` | Local Parquet sales-detail analysis | `read_parquet`, `register_temp_table`, CTEs, window ranking, self-join share |
| 02 | `py02_metrics_timeseries.py` | Application metrics and time-series aggregation | Time bucketing, aggregation by service, deviation detection, approximate quantiles |
| 03 | `py03_transaction_ledger.py` | Transaction ledger and reconciliation | Point reads, `retrieve`/`replace`/`delete`, debit-credit balance checks, transactions |
| 04 | `py04_log_ingestion_dedup.py` | Log ingestion, dedup and level rollup | `CASE` severity classes, `DISTINCT`, pattern search, error rate |
| 05 | `py05_sqlite_migration.py` | Migrate from SQLite and reconcile | Cross-checking result consistency, DataFrame import/export |
| 06 | `py06_data_quality_gate.py` | Pre-release data-quality gate | Assertion-style `NOT NULL`/uniqueness/range/referential-integrity checks |
| 07 | `py07_dataframe_interop.py` | Pandas/Polars/PyArrow interop | `from_*` / `to_*` round-trip consistency |
| 08 | `py08_retention_cost_windows.py` | Retention and cost attribution | `ROW_NUMBER`/`RANK` TopN, self-join cumulative totals and shares |
| 09 | `py09_window_functions_tour.py` | Window-function tour | `ROW_NUMBER`/`RANK`/`DENSE_RANK`/`LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTILE`, aggregate windows |
| 10 | `py10_rag_knowledge_base.py` | Local RAG knowledge base | Document ingestion, FTS recall, vector reranking, context and prompt assembly |
| 11 | `py11_hybrid_retrieval.py` | Hybrid retrieval (FTS + SQL + vector) | Recall/filter/rerank in one SQL statement, with recall-based measurement |
| 12 | `py12_vector_quantization.py` | Vector quantization and exact reranking | `create_quantized_column`, `accelerator`, `candidate_k`, recall@k |
| 13 | `py13_batch_vector_search.py` | Batch vector search | `batch_topk_distance` for many queries in one call |
| 14 | `py14_vector_metrics.py` | Vector distance-metric comparison | Ordering properties of l2 / l2_squared / cosine / dot / l1 / linf |
| 15 | `py15_data_lake_foundation.py` | Hybrid-storage data-lake foundation | Partitioned Parquet + materialized hot copy, cold/hot comparison |
| 16 | `py16_lake_multisource_join.py` | Multi-source federation on the lake | Parquet + CSV + NDJSON joined and aggregated in one SQL query |
| 17 | `py17_embedding_ingestion.py` | Embedding bulk ingestion and incremental updates | Columnar fast writes, quantized accelerator column, incremental append and single-row updates |

## Rust scenarios (9)

How to run (from the repository root):

```bash
cargo run --example r01_config_store --no-default-features
```

These examples are registered as `[[example]]` entries in `Cargo.toml`, so
`cargo run --example <name>` works.

| # | Name | File | Scenario | Main capabilities demonstrated |
|---|---|---|---|---|
| R1 | `r01_config_store` | `r01_config_store.rs` | Embedded configuration center / feature flags | `ApexDB::builder`, durability levels, schema, CRUD, multi-database isolation |
| R2 | `r02_concurrent_writes` | `r02_concurrent_writes.rs` | Concurrent reads and transactions | Single-writer/multi-reader concurrency, transaction atomicity, parallel writes to separate tables |
| R3 | `r03_batch_etl_csv` | `r03_batch_etl_csv.rs` | CSV -> column-store batch ETL | `register_temp_table`, cleaning and dedup, `insert_arrow` |
| R4 | `r04_fulltext_search` | `r04_fulltext_search.rs` | In-process full-text search | FTS index, `MATCH`, `FUZZY_MATCH`, index operations |
| R5 | `r05_vector_search` | `r05_vector_search.rs` | Vector similarity search in a service | `FixedList` encoding, `topk_distance`, fetch-back, distance functions |
| R6 | `r06_shared_file_analytics` | `r06_shared_file_analytics.rs` | Cross-database read-only analytics | Multiple databases, cross-database SQL/JOIN, multi-threaded reads |
| R7 | `r07_rag_pipeline` | `r07_rag_pipeline.rs` | Pure-Rust local RAG pipeline | Chunking, embedding, dual-path recall, RRF fusion, prompt assembly |
| R8 | `r08_hybrid_retrieval` | `r08_hybrid_retrieval.rs` | Hybrid-retrieval quality comparison | FTS/vector/hybrid recall and precision comparison |
| R9 | `r09_embedded_data_lake` | `r09_embedded_data_lake.rs` | Data-lake foundation | Multi-source registration, federated JOIN, hot-data materialization, cold/hot reconciliation |

R7/R8 use a **deterministic "simulated embedding" function** (character/word-bag
hashing + normalization) instead of a real model, so the full retrieval chain is
reproducible without downloading anything; to wire in a real project, replace that
function with your ONNX/candle/remote embedding call and leave the rest unchanged.

## Verified SQL capability boundaries

The conclusions below were all **measured** against the current version of this
repository. The example code is written to the supported forms and documents these
boundaries in the relevant files.

### Supported window functions

**Ranking / offset**: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`,
`FIRST_VALUE()`, `LAST_VALUE()`, `NTILE()`, with `OVER (PARTITION BY ... ORDER BY ...)`.

**Aggregates**: `SUM()` / `AVG()` / `COUNT()` with `OVER (...)` **compose** — they work
as a top-level projection column *and* can be wrapped by a function, used in arithmetic,
and carry a `ROWS` frame:

```sql
-- All of these work:
SELECT rep, amt, SUM(amt) OVER (PARTITION BY g) AS group_total FROM sales
SELECT ROUND(SUM(amt) OVER (PARTITION BY g), 2) AS group_total FROM sales
SELECT SUM(amt) OVER (PARTITION BY g) / 2 AS half_total FROM sales
SELECT x, SUM(x) OVER (
    PARTITION BY g ORDER BY k
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS running_total FROM sales
```

Three caveats remain:

* A window in `ORDER BY` is still a parse error. Compute the window in a
  subquery/CTE and order by the resulting column instead. (A window nested in a
  scalar function such as `ROUND(...)` or `CAST(...)`, including combined with
  arithmetic, is supported.)
* Only the cumulative frame `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
  (equivalently, `ORDER BY k` with no frame) is implemented. A **bounded** frame such
  as `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` is now **rejected with an error**
  rather than silently ignored — it used to return the whole-partition value, which
  looked plausible but was wrong. For a bounded moving window, use a self-join with an
  explicit range condition instead (see the py08 / py09 examples).
* `LAST_VALUE()` also ignores an explicit frame and returns the **whole partition's**
  last value.

Beyond windows: `GROUP BY` accepts expressions (for example `GROUP BY UPPER(cat)`) and
aliases, aggregate-free `GROUP BY` (with only projection columns) now groups correctly,
and `ORDER BY` may reference a column that is not in `SELECT`. `IN` still requires a
column on its left side, and `||` string concatenation is still a parse error.

### Unsupported capabilities and workarounds

| Unsupported | Symptom | Workaround |
|---|---|---|
| Bounded `ROWS n PRECEDING` window frames | Frame silently ignored; whole-partition result (`COUNT` can panic) | Self-join with an explicit range condition |
| `DATE_TRUNC` / `strftime` / `DATE()` / `EXTRACT` | `Unsupported function` | Materialize the time bucket as a **string column** at write time (for example `2024-03`) and group/range-filter as text |
| `\|\|` string concatenation | Parse error | `CONCAT(a, b)` / `CONCAT_WS(sep, ...)` |
| `MIN/MAX` on a string column | Meaningless value rather than the string (numeric columns are fine) | Choose among string values in the application layer, or restructure as `GROUP BY` |
| `array_distance(vec, q, 'metric')` | `requires exactly 2 arguments` | `array_distance(vec, q)` for L2; use `cosine_distance(vec, q)` for other metrics |
| A table function in a JOIN | Parse error | Take the TopK `_id`s first, then re-query with `WHERE _id IN (...)` |
| `IN` with an expression on its left | `IN requires column on left side` | Rewrite as several `OR` equality conditions |
| A window inside `CAST(...)` or in `ORDER BY` | `Unsupported expression type` / parse error | Compute the window in a subquery/CTE, then cast/order outside it |

### Behavioural differences worth knowing

1. **Vector distance functions must inline an array literal**:
   `cosine_distance(emb, [0.1,0.2,0.3])`. Binding a list through a `?` placeholder
   expands it into several scalar arguments and errors. On the Python side,
   `_demo_env.vector_literal(vec)` generates the literal.
2. **After `register_temp_table` the Python client must call `use_table(name)`** before
   querying, otherwise it reports `No table selected`; temp tables also **do not appear
   in `list_tables()`**. (The Rust `db.execute()` path can query a temp table by name
   directly.)
3. **FTS is not available for `:memory:`** (it reports `File not open`), and search terms
   should be at least 2 characters.
4. **`client.current_table` is a property** (not a method); `count_rows()` is a method.
5. **Rust `FixedList` vectors must be written as raw little-endian f32 bytes**:
   `Value::FixedList(encode_f32_vec(&v))`; they read back as
   `FixedSizeList<Float32>` (not a flat `Float32Array`).
6. **A Rust embedded transaction must complete within a single `execute()` call**
   (`BEGIN; ...; COMMIT` as one statement string). Splitting it across
   `execute("BEGIN")` ... `execute("COMMIT")` fails; the r02 example shows the pattern.
7. **`ALTER FTS INDEX ... DISABLE` deliberately makes `MATCH()` unavailable** until the
   index is `ENABLE`d again. This is by design, not a defect — while an index is
   disabled, filtered reads fail on purpose.

## Known defects (please read before using)

The defects below were **reproduced by measurement** while writing these examples, not
inferred. They affect how you write against the Rust embedded API, so they are recorded
explicitly here. Fixed entries are kept, with their root cause, because they shape how
the examples are written.

### 1. `Table::replace()` followed by `Table::delete()` — FIXED

Calling `replace()` and then `delete()` used to duplicate rows: the delete did not
remove the target row, the row count kept growing, and the deleted row was still
visible to `retrieve()`.

Measured at the time (5 rows -> replace(id=1) -> delete(id=2)):

| Operation | `count()` | Correct |
|---|---:|---:|
| Insert 5 rows | 5 | 5 |
| `replace` only | 5 | 5 |
| `delete` only | 4 | 4 |
| **`replace` then `delete`** | **9** | **4** |

A further delete magnified it again (9 -> 17), and `retrieve(id=2)` still returned the
original value.

**Root cause.** `OnDemandStorage` has to decide how many of its buffered rows are *new*
and how many are simply the base table held in memory; `save()` then persists only the
new ones. It inferred that state from "the buffer does not start at `_id` 1" instead of
asking its own `v4_base_loaded` flag. A loaded base does not have to start at `_id` 1 —
`replace()` re-appends the row it rewrote, so the base can begin with any ID. Once that
happened:

* the whole loaded base was reported as pending, so the flush spilled the table into the
  delta sidecar **on top of** the base copy already on disk (that is the 5 -> 9 jump);
* the append-only spill skipped its "a persisted row is deleted" guard for the same
  reason, so a later `replace()` published the new row while leaving the old copy active.

**Fix.** Both decisions now use `v4_base_loaded` (`pending_v4_in_memory_rows` and
`spill_pending_v4_rows_to_delta`), so the spill is only taken for genuinely appended rows
and any delete of a persisted row goes through a rewrite or an in-place deletion-vector
update.

Two follow-on problems in the same area were found while verifying it and are fixed too:

* **Rewrites could leave a row group with unsorted IDs.** `save_v4()` wrote the buffered
  rows in buffer order, and a re-appended `replace()` row landed at the end. Point lookups
  pick a row group by `[min_id, max_id]` and then locate the row with an `id - min_id`
  guess plus a binary search over the group's ID section, so an unsorted group made rows
  unreachable — including rows that had nothing to do with the replace. `save_v4()` now
  keeps each group's IDs ascending (a replaced row returns to its ID position).
* **Point lookups also gave up on sparse IDs.** After a rewrite that removed rows, the
  direct guess overshoots the group (`id - min_id >= row_count`) and the lookup returned
  "no such row" without searching. `retrieve_rcix` / `retrieve_rcix_projected` now fall
  back to searching the ID section, which is what the Python point-read path already did.
* **`retrieve()` could not see rows that were still buffered.** A base-file miss on a
  table whose newest rows live in the delta sidecar, the DeltaStore overlay or the
  in-memory append buffer is now answered through the query path instead of being
  reported as absent. (This one is not specific to `replace()`: a single-row `insert()`
  appends to the delta sidecar and was invisible to `retrieve()` as well.)

Measured after the fix: `5 rows -> replace(id=1) -> delete(id=2)` counts 4, the deleted
row is gone, the rewritten value is visible through `retrieve()`, `retrieve_many()` and
SQL, and repeated replace/delete cycles keep the count exact. `cargo test` pins it down
with `embedded::tests::test_replace_then_delete_keeps_row_counts` and
`embedded::tests::test_retrieve_sees_rows_from_every_write_path`; the Python side is
covered by `test/test_engine_defect_regressions.py::TestReplaceDeleteInteraction`.


### 2. Concurrent same-table writes — FIXED

Previously, when several threads called `insert_batch()` on the **same table**
concurrently, all but one failed with `No such file or directory`, and the survivors
lost rows (one run wrote 400 rows but counted 611).

The root cause was that the V4 store keeps a private in-memory column buffer per
backend and persists it by rewriting or appending the shared `.apex` file, so two
writers interleaved their read-modify-write. Writes are now serialized per table for
the whole mutation, which is what the Rust API documentation already promised
("writes are serialized per-table via an internal write lock"). Concurrent **reads**
remain fully parallel.

Measured after the fix: 4 threads x 25 batches x 20 rows → 0 errors, 2000 written,
2000 counted; 4 threads x 40 batches x 25 rows → 4000 written, 4000 counted;
8 threads x 50 batches x 50 rows → 0 errors, 20000 written, 20000 counted. `r02`
now demonstrates genuine concurrent writes to one table.

**Reader/writer interleaving — FIXED.** Running a writer while other threads read the
*same* table used to fail intermittently with `No such file or directory`, and when it
did the row count diverged from what was written (`written=3000 count=3025`).

The failing path was **not** the base `.apex` file. Every base-file rewrite publishes its
result by writing `<table>.apex.tmp` and `rename`-ing it over the table, and that scratch
name was **shared by every participant**:

* `save_v4` — the full rewrite,
* `stream_rewrite_v4` — streaming compaction and the delete rewrite,
* `rewrite_v4_drop_columns` — the column-drop DDL rewrite,
* the "stale `.tmp` cleanup" that `OnDemandStorage::open_with_durability` runs on *every*
  backend open.

Letting two of those overlap explains both reported symptoms with one cause. The loser's
`rename` finds its scratch file already consumed and fails with `No such file or
directory` (the loser is not always the reader), while the winner can publish a snapshot
that a later save had already superseded — the row-count divergence.

Two things allowed the overlap:

1. `Table::execute()` flushes rows that a writer is still holding in memory so the query
   can see them. The check-then-flush ran **without** the table write lock, so a reader
   starting a query could begin a base-file rewrite in the middle of a writer's
   read-modify-write.
2. Nothing serialized base-file replacement. The per-table write lock only covers
   `StorageEngine::write`, and the flush runs *inside* that section — so it cannot reuse
   the same lock (it is not reentrant).

The fix has three parts:

* every rewrite writes to a **private scratch file** (`<table>.apex.<pid>.<seq>.tmp`) and
  renames *that*, so no two participants can consume each other's scratch file — this also
  stops the stale-`.tmp` cleanup from unlinking a file that is actively being written;
* a **per-table rewrite lock** (`storage::table_save_lock::rewrite_lock`) serializes
  base-file replacement, so an older snapshot can no longer be published over a newer one;
* `Table::execute()` takes the table write lock and **re-checks** before flushing, so a
  reader's flush becomes a no-op once a writer has persisted the rows. Queries that find
  nothing pending never touch the lock.

Concurrent **reads** remain fully parallel: a query only takes the lock when a writer has
left rows in memory, which is exactly the case that must not race.

Measured after the fix: the reported shape (4 writer threads x 40 batches x 25 rows plus 3
reader threads) and smaller/larger variants, 50+ trials — 0 failures and `written == count
== expected` every time, with roughly 1000 reader queries per trial. The same harness failed
essentially every trial before the fix (the smallest variant used, 2 writers x 10 batches x
10 rows plus 2 readers, failed 6 of 6). `cargo test` pins it down:
`embedded::tests::test_concurrent_reads_and_writes_to_same_table` fails on the old code and
passes on the new one.

**What the earlier investigation ruled out.** Nine local variants were tried and reverted
before the scratch-file collision was found; they are recorded here so they are not
repeated:

| # | Approach | Outcome |
|---|---|---|
| 1 | Retry the path open (bounded, with backoff) | Still failed: the scratch file is absent for as long as the other participant holds it |
| 2 | Defer `invalidate()` while a write is in flight | Reduced the rate, did not close it |
| 3 | `try_clone()` on the write handle, drop the original | Writer-side row loss fixed, but 6 append-path tests broke (the clone was read-mode) |
| 4 | `try_clone()`, keep the original alive | Same 6 tests broke |
| 5 | `try_clone()` + `sync_all()` before dropping | Same 6 tests broke |
| 6 | Split by caller: append clears the handle, rewrite keeps it | Same 6 tests broke |
| 7 | Open read+write while the read handle is held (no clone) | 611 tests green, race unfixed |
| 8 | Reuse the backend's own read handle for the flush | 611 tests green, race unfixed |
| 9 | Unify `get_insert_backend` / `get_read_backend` ownership | 611 tests green, race unfixed |

All nine changed the write/flush path itself; none of them touched who *else* may hold the
scratch file, which is where the defect actually lived.

`r02` now runs its reader threads genuinely interleaved with its writers and asserts that
every writer succeeded and every row is present exactly once.

### 3. `Value::Null` lost on single-row writes — FIXED

Writing a row whose column is `Value::Null` used to store the column default, so the
value read back as `String("")` / `Int64(0)` / `Float64(0.0)` / `Bool(false)` depending on
the column type. It was not limited to one API: a single-row write into a table that
already had a file was affected in both the Rust embedded and the Python clients, while
the first write (and batch writes) looked correct — which made it look like a NULL
handling issue in the row API instead of a routing issue in the engine.

**Root cause.** `StorageEngine::classify_write` sends a V4 table to the V4 write path and
only legacy tables to the append-only delta file. The delta encoding carries **no null
bitmap** and only scalar int/float/string/bool columns. Two things broke that split:

1. Backend materialization recorded `is_v4: false` in the schema cache regardless of the
   file's real format, so the very next single-row write to a V4 table was classified as
   "legacy delta" and encoded through the delta path — NULLs became defaults, and
   vector/blob columns were dropped entirely (a single-row insert into a vector table
   failed outright with `all columns in a record batch must have the same length`).
2. `OnDemandStorage::append_row_group` (used by the columnar fast path) extended the
   pending `ids` buffer with rows it had written straight to the file, without extending
   `columns` / `nulls`. The three arrays are one parallel buffer, so the next in-memory
   insert recorded its null bit at an absolute index while its column data sat at a
   relative one, and the following flush sliced the wrong bytes out of both.

**Fix.** The schema cache now records `backend.storage.is_v4_format()`, and
`append_row_group` no longer touches the pending buffers (it still publishes the ID range,
active count and next ID). With writes routed correctly, the V4 flush's existing
"declines on NULLs" guard takes over and persists null bitmaps through a rewrite.

Two related point-read gaps were found while verifying this and are fixed too:

* `retrieve()` reported "no such row" for **every row of a table with a vector column**,
  because the V4 point lookup only decodes scalar columns. A base-file miss is now only
  treated as authoritative when the point lookup can decode the whole schema.
* A base miss was already answered through the query path for buffered rows; that fallback
  now also covers non-decodable schemas.

Measured after the fix: `NULL` survives the first write, a single-row write into a
materialized table, a batch write and a write after a batch (both clients); a vector table
keeps its vectors through the same route and its rows are retrievable and searchable.
`cargo test` pins it down with `embedded::tests::test_null_survives_every_write_route` and
`embedded::tests::test_retrieve_finds_rows_in_vector_tables`; the Python side is covered
by `test/test_engine_defect_regressions.py::TestWriteRoutesKeepValues`.

## Reproducing and verifying

```bash
# Python: run every scenario
cd usage-examples/Python
for f in py*.py; do python "$f" >/dev/null || echo "FAILED: $f"; done

# Rust: run every scenario (from the repository root)
for ex in r01_config_store r02_concurrent_writes r03_batch_etl_csv \
          r04_fulltext_search r05_vector_search r06_shared_file_analytics \
          r07_rag_pipeline r08_hybrid_retrieval r09_embedded_data_lake; do
  cargo run -q --example "$ex" --no-default-features >/dev/null || echo "FAILED: $ex"
done
```

Every example that references a relative path creates its output directory based on
**the script's own location**, so it runs correctly no matter what the current working
directory is.

## Environment requirements

- Python 3.9+ (this repository is verified on 3.12) with `apexbase`, `numpy`, `pandas`,
  `polars` and `pyarrow` installed.
- Rust (this repository is verified on 1.92); the examples use `--no-default-features`,
  so they compile and run without the Python bindings.
- No external database, model or network access is required.
