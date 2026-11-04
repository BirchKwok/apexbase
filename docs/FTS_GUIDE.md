# Full-Text Search (FTS) Guide

ApexBase includes **ApexFTS**, its native Rust full-text storage engine, directly in the SQL executor. FTS is a **first-class SQL feature** — indexes are created with DDL statements and queried with standard `WHERE` predicates, making FTS available through every interface: Python API, PostgreSQL Wire, and Arrow Flight. No external NanoFTS package is required.

---

## Table of Contents

1. [Why FTS in the same engine](#why-fts-in-the-same-engine)
2. [Architecture](#architecture)
3. [Quick Start](#quick-start)
4. [DDL Reference](#ddl-reference)
   - [CREATE FTS INDEX](#create-fts-index)
   - [DROP FTS INDEX](#drop-fts-index)
   - [ALTER FTS INDEX DISABLE / ENABLE](#alter-fts-index-disable-enable)
   - [SHOW FTS INDEXES](#show-fts-indexes)
5. [Query Reference](#query-reference)
   - [MATCH()](#match)
   - [FTS_SCORE()](#fts_score)
   - [FUZZY_MATCH()](#fuzzy_match)
6. [Combining FTS with SQL](#combining-fts-with-sql)
7. [Python API](#python-api)
8. [PG Wire and Arrow Flight](#pg-wire-and-arrow-flight)
9. [Lifecycle and Storage](#lifecycle-and-storage)
10. [Configuration Options](#configuration-options)
11. [Performance Tips](#performance-tips)
12. [Known Constraints](#known-constraints)

---

## Why FTS in the same engine

Agent and RAG pipelines need two kinds of recall over the same records. BM25
recall is precise for names, identifiers, quoted spans and rare terms; vector
recall is semantic. ApexBase serves both from one table:

- **BM25 recall** through ApexFTS: `MATCH()`, `FUZZY_MATCH()` and `FTS_SCORE()`
  are SQL predicates executed in Rust beside the data.
- **SQL filters compose directly.** Because `MATCH()` is a `WHERE` predicate, you
  can add tenant IDs, date ranges, categories and aggregations in the same
  statement instead of post-filtering a separate search service.
- **Vector fusion** through `hybrid_search()`: one call fuses a BM25 branch and a
  `topk_distance()` branch on `_id` using reciprocal rank fusion (`rrf`) or
  normalized weights (`weighted`), so your retriever gets one ranked list.
- **One database directory, no external search service.** The index lives under
  `{dir}/fts_indexes/` next to the table data and is served by the same executor
  over Python, PG Wire and Arrow Flight.

```python
import numpy as np
from apexbase import ApexClient

client = ApexClient("./rag_db")   # use a filesystem directory for hybrid retrieval
client.execute("CREATE TABLE docs (title TEXT, body TEXT, vec FLOAT32_VECTOR)")
client.execute("CREATE FTS INDEX ON docs (title, body)")

vecs = np.random.rand(3, 4).astype(np.float32)
client.store({
    "title": ["Agent memory", "Rust guide", "Python tutorial"],
    "body": ["retrieval over long-term agent memory",
             "systems programming", "learn python"],
    "vec": [vecs[i] for i in range(3)],
})

hits = client.hybrid_search(
    np.random.rand(4).astype(np.float32),   # query embedding
    "agent memory",                          # BM25 query
    k=3,
    vector_col="vec",
    fusion="rrf",
)
print(hits.to_pandas())    # _id, score — one fused ranked list
client.close()
```

See [Hybrid Search](API_REFERENCE.md#hybrid-search) for the full signature and
[Agent memory](user-guide/agent-memory.md) for the end-to-end retrieval workflow.

---

## Architecture

```
                    ┌─────────────────────────────────────┐
                    │         SQL Executor (Rust)          │
                    │                                      │
  Python API  ───►  │  parse SQL → detect MATCH() →       │
  PG Wire     ───►  │  look up FtsManager →               │
  Arrow Flight ───► │  search → Roaring bitmap → filter   │
                    │                                      │
                    └────────────┬────────────────────────┘
                                 │
                    ┌────────────▼────────────────────────┐
                    │  Global FTS Registry                 │
                    │  key: base_dir → Arc<FtsManager>    │
                    │  FtsManager: table → FtsEngine      │
                    └────────────┬────────────────────────┘
                                 │
                    ┌────────────▼────────────────────────┐
                    │  Disk: {dir}/fts_indexes/{table}.afts│
                    │        + {table}.afts.wal            │
                    │        + {table}.afts.meta           │
                    │         {dir}/fts_config.json        │
                    └─────────────────────────────────────┘
```

Key design points:

- FTS state is stored in a **global Rust registry** keyed by database directory, so PG Wire and Arrow Flight connections share the same FTS engines as Python API calls.
- `MATCH('query')` in a `WHERE` clause is resolved once to an `Arc<RoaringTreemap>`; Arrow batches test `_id` membership directly without materializing an `IN` list.
- The configuration is persisted in `fts_config.json` alongside the `.apex` data files, and is re-loaded automatically on process restart.
- Postings use 64-bit Roaring bitmaps. Packed per-document token streams provide BM25 term frequency, document length, and phrase positions without duplicating the term dictionary.
- Updates and deletes are recorded in a checksummed WAL and compacted into an atomically replaced v3 snapshot; the snapshot's `.afts.meta` records the source-table generation used to reject stale indexes on open (v2.0.0).
- With `lazy_load=true`, the fixed-width term directory and posting blobs remain mmap-backed; posting bitmaps are decoded on first access into a bounded cache.
- Text is NFKC-normalized and Unicode-lowercased. Unicode word boundaries cover Latin, Cyrillic, Arabic and other spaced scripts; n-grams cover Han, Hiragana, Katakana and Hangul.

---

## Quick Start

```python
from apexbase import ApexClient

client = ApexClient("./data")
client.create_table("articles")

# 1. Create the FTS index (DDL)
client.execute("CREATE FTS INDEX ON articles (title, content)")

# 2. Insert data — rows are indexed automatically on each store()
client.store([
    {"title": "Rust programming language", "content": "Rust is fast and safe"},
    {"title": "Python tutorial",           "content": "Python is easy to learn"},
    {"title": "Machine learning basics",   "content": "Deep learning with PyTorch"},
])

# 3. Query using MATCH() in WHERE
results = client.execute("SELECT * FROM articles WHERE MATCH('rust')")
print(results.to_pandas())
#                    title                    content
# 0  Rust programming language  Rust is fast and safe

# 4. Fuzzy search — tolerates typos
results = client.execute("SELECT * FROM articles WHERE FUZZY_MATCH('pytohn')")
print(results.to_pandas())
#             title                    content
# 0  Python tutorial  Python is easy to learn

client.close()
```

---

## DDL Reference

### CREATE FTS INDEX

```sql
CREATE FTS INDEX ON table_name
    [(col1 [, col2, ...])]
    [WITH (option = value [, ...])]
```

**Effect:**

- Registers the table as FTS-enabled: a filesystem database writes
  `fts_config.json`, a `:memory:` database keeps the same document in a
  process-local registry.
- Creates (or opens) the ApexFTS engine for the table under `{dir}/fts_indexes/{table}.afts` and replays `{table}.afts.wal`. Process-local tables keep the engine in memory instead.
- **Existing rows are back-filled automatically** — all rows already in the table are indexed immediately. The status message reports the number of rows indexed.
- New documents stored via `store()` / `INSERT` are indexed automatically on every write.

**Column list (optional)**

If omitted, all string columns are indexed. Specify columns to reduce index size and improve precision:

```sql
-- Index specific columns
CREATE FTS INDEX ON articles (title, content)

-- Index all string columns (no column list)
CREATE FTS INDEX ON articles
```

**Options (WITH clause)**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `lazy_load` | bool | `false` | mmap the v3 term directory and decode postings on first access |
| `cache_size` | int | `10000` | Maximum decoded posting bitmaps retained in lazy mode |

```sql
CREATE FTS INDEX ON logs WITH (lazy_load=true, cache_size=50000)
CREATE FTS INDEX ON articles (title) WITH (cache_size=100000)
```

The same options are available through `client.init_fts()`:

```python
# Equivalent to CREATE FTS INDEX ON articles (title, content)
client.init_fts(index_fields=["title", "content"])

# Retain up to 200,000 decoded posting bitmaps
client.init_fts(cache_size=200000)

# Keep the term directory and postings mmap-backed across reopen
client.init_fts(index_fields=["subject", "body"], lazy_load=True)
```

**Examples:**

```python
# Index title + content
client.execute("CREATE FTS INDEX ON articles (title, content)")

# Index every string column
client.execute("CREATE FTS INDEX ON wiki")

# Explicit options through SQL...
client.execute("CREATE FTS INDEX ON logs WITH (lazy_load=true, cache_size=50000)")

# ...or through the Python API
client.init_fts(lazy_load=True)
```

Both routes work on filesystem and `:memory:` databases, and both back-fill
rows that already exist. After SQL DDL, the Python search API sees the same
enabled state, so `search_text()` and `MATCH()` can be mixed freely.

---

### DROP FTS INDEX

```sql
DROP FTS INDEX ON table_name
```

**Effect:**

- Removes the table entry from `fts_config.json`.
- Deletes the `.afts` snapshot and WAL from `{dir}/fts_indexes/`, together with legacy `.nfts` files when present.
- Removes the in-memory engine from the global registry.

```python
client.execute("DROP FTS INDEX ON articles")
```

---

### ALTER FTS INDEX DISABLE / ENABLE

**Disable:**
```sql
ALTER FTS INDEX ON table_name DISABLE
```

**Effect:**

- Sets `enabled = false` in `fts_config.json`.
- **Does not** delete index files.
- While disabled, SQL `INSERT` / `DELETE` writes are **not** synced to the FTS index.
- **Full-text reads are unavailable while disabled**: `MATCH()`,
  `FUZZY_MATCH()`, `FTS_SCORE()` and `search_text()` fail for this table instead
  of answering from the retained index. The retained snapshot is stale by
  definition (writes made while disabled were not indexed), so failing loudly is
  safer than returning incomplete results. Re-enable the index to read again.
- Useful when temporarily suspending FTS to avoid write overhead during a large bulk load.

**Enable:**
```sql
ALTER FTS INDEX ON table_name ENABLE
```

**Effect:**

- Sets `enabled = true` in `fts_config.json`.
- **Back-fills all rows** currently in the table into the FTS index (idempotent — re-indexing already-indexed rows is safe).
- Rows inserted while FTS was disabled are caught up automatically during the enable step.
- Returns a status message with the number of rows indexed.

```python
# Disable while doing a large bulk import
client.execute("ALTER FTS INDEX ON articles DISABLE")

# ... bulk import — rows are NOT synced to FTS during this window ...

# Re-enable: back-fills all rows, including those inserted while disabled
client.execute("ALTER FTS INDEX ON articles ENABLE")
```

---

### SHOW FTS INDEXES

```sql
SHOW FTS INDEXES
```

Returns a result set describing all FTS-configured tables across **all databases** rooted at the server directory.

**Result columns:**

| Column | Type | Description |
|--------|------|-------------|
| `database` | string | Database name (`default` for the root directory, or the sub-database name) |
| `table` | string | Table name |
| `enabled` | bool | Whether FTS is currently active |
| `fields` | string | Indexed columns (comma-separated, or `(all string cols)`) |
| `lazy_load` | bool | Whether the term directory is mmapped and postings are decoded on first access |
| `cache_size` | int | Bound on decoded posting bitmaps retained in lazy mode |

```python
df = client.execute("SHOW FTS INDEXES").to_pandas()
print(df)
#   database     table  enabled          fields  lazy_load  cache_size
# 0  default  articles     True  title, content      False       10000
# 1  default      wiki     True  (all string cols)   False      200000
# 2   shopdb  products     True            name      False       10000
```

---

## Query Reference

### MATCH()

```sql
WHERE MATCH('query text')
```

Performs an exact full-text search. Returns all rows where the indexed text contains **all** of the query terms. Multi-word queries require all words to appear (AND semantics).

```sql
-- Single term
SELECT * FROM articles WHERE MATCH('python')

-- Multi-term (all terms must appear)
SELECT * FROM articles WHERE MATCH('machine learning')

-- Exact phrase: terms must be adjacent and in this order
SELECT * FROM articles WHERE MATCH('"machine learning"')

-- Chinese / CJK supported
SELECT * FROM articles WHERE MATCH('人工智能')
```

**Return behaviour:** The executor retains matches as a compressed bitmap and evaluates `_id` membership directly — zero rows are returned if there are no matches. SQL rows follow normal query ordering; add `ORDER BY` when deterministic SQL order is required.

---

### FTS_SCORE()

`FTS_SCORE()` exposes the BM25 relevance of the unique `MATCH()` query in the same statement:

```sql
SELECT title, FTS_SCORE() AS relevance
FROM articles
WHERE MATCH('embedded database')
ORDER BY relevance DESC
LIMIT 20;
```

An explicit query is also supported: `ORDER BY FTS_SCORE('embedded database') DESC`. Without an argument, the statement must contain exactly one non-fuzzy `MATCH()` query.

### FUZZY_MATCH()

```sql
WHERE FUZZY_MATCH('query text')
```

Fuzzy full-text search that tolerates spelling errors and typos. Uses edit-distance based matching.

```sql
-- Typo: 'pytohn' → matches 'python'
SELECT * FROM articles WHERE FUZZY_MATCH('pytohn')

-- Typo: 'databse' → matches 'database'
SELECT * FROM articles WHERE FUZZY_MATCH('databse')
```

> **When to use:** Prefer `MATCH()` for known-correct queries (faster). Use `FUZZY_MATCH()` for user-typed search inputs where typos are expected.

---

## Combining FTS with SQL

`MATCH()` / `FUZZY_MATCH()` are standard `WHERE` predicates and compose with all other SQL features:

```python
# FTS + date filter + ORDER BY + LIMIT
client.execute("""
    SELECT title, author, published_at
    FROM articles
    WHERE MATCH('neural network') AND published_at >= '2023-01-01'
    ORDER BY published_at DESC
    LIMIT 20
""")

# FTS + aggregation
client.execute("""
    SELECT author, COUNT(*) AS article_count
    FROM articles
    WHERE MATCH('deep learning')
    GROUP BY author
    ORDER BY article_count DESC
""")

# FTS in a CTE
client.execute("""
    WITH matched AS (
        SELECT * FROM articles WHERE MATCH('rust async')
    )
    SELECT author, COUNT(*) FROM matched GROUP BY author
""")

# COUNT
n = client.execute("SELECT COUNT(*) FROM articles WHERE MATCH('python')").scalar()

# Combined fuzzy + exact
client.execute("""
    SELECT * FROM articles
    WHERE FUZZY_MATCH('machne lerning') AND category = 'AI'
""")
```

### Hybrid retrieval with vectors

FTS and vector search can also be combined in one statement: `MATCH()` narrows
the candidate rows, and a `topk_distance()` subquery supplies semantic recall.
The derived table needs an alias:

```python
# docs: a table with title/body text and a vec vector column (see the hybrid example above)
results = client.execute("""
    SELECT title
    FROM docs
    WHERE MATCH('agent memory')
      AND _id IN (
          SELECT t._id FROM (
              SELECT explode_rename(
                  topk_distance(vec, [0.1, 0.2, 0.3, 0.4], 50, 'l2'),
                  '_id', 'dist'
              )
              FROM docs
          ) t
      )
    LIMIT 10
""")
```

For a single ranked list instead of a filter intersection, use the
`hybrid_search()` Python API shown in
[Why FTS in the same engine](#why-fts-in-the-same-engine); it fuses the two
branches with RRF by default. See [Hybrid Search](API_REFERENCE.md#hybrid-search)
for `fusion` and `alpha`, [Float16 Vectors](FLOAT16_VECTOR_GUIDE.md) for
half-precision embeddings, and [Vector Quantization](VECTOR_QUANTIZATION_GUIDE.md)
for compressed candidate generation before exact reranking.

---

## Python API

The Python API provides direct FTS access without going through SQL. It is useful for programmatic control and when you need the raw ID arrays.

### init_fts

```python
client.init_fts(
    table_name: str = None,
    index_fields: Optional[List[str]] = None,
    lazy_load: bool = False,
    cache_size: int = 10000
) -> ApexClient
```

Initialize FTS for the current (or specified) table. Equivalent to `CREATE FTS INDEX ON table`.

> **Note:** `init_fts()` also registers the FTS engine with the global Rust registry, so subsequent SQL queries via PG Wire / Arrow Flight can use `MATCH()` on the same index.

```python
client.use_table("articles")
client.init_fts(index_fields=["title", "content"])
```

### search_text

```python
ids = client.search_text("query", table_name=None)  # → np.ndarray[int64]
```

Results are ordered by BM25 relevance. Wrap the query in double quotes for an exact phrase:

```python
ids = client.search_text('"distributed database"')
```

### search_text_with_scores

```python
hits = client.search_text_with_scores("query", limit=1000)
# [(document_id, bm25_score), ...]
```

### fuzzy_search_text

```python
ids = client.fuzzy_search_text("query", min_results=1, table_name=None)  # → np.ndarray[int64]
```

### search_and_retrieve / search_and_retrieve_top

```python
results = client.search_and_retrieve("query", limit=100, offset=0)     # → ResultView
top5    = client.search_and_retrieve_top("query", n=5)                  # → ResultView
```

### Lifecycle

```python
client.disable_fts()    # suspend (keep files)
client.drop_fts()       # drop (delete files)
stats = client.get_fts_stats()   # {'fts_enabled': True, 'doc_count': N, ...}
client.compact_fts_index()       # compact the on-disk snapshot + WAL
```

### Fuzzy Configuration And Warmup

```python
client.set_fts_fuzzy_config(
    threshold=0.7,          # minimum similarity in [0, 1]
    max_distance=2,         # maximum edit distance
    max_candidates=20,      # candidate terms examined per query term
    table_name=None,
)

# Pre-decode postings for terms you expect to search
loaded = client.warmup_fts_terms(["rust", "database"], table_name=None)
```

The same fuzzy knobs are also accepted per call by `FUZZY_MATCH(...)` and
`fuzzy_search_text(...)`.

---

## PG Wire and Arrow Flight

Because FTS is implemented entirely in the Rust SQL executor, `MATCH()` and `FUZZY_MATCH()` work transparently over all server protocols:

```bash
# Start the combined server
apexbase-serve --dir /path/to/data
```

```python
# Python API — create the index
import apexbase
c = apexbase.ApexClient("/path/to/data")
c.create_table("articles")
c.execute("CREATE FTS INDEX ON articles (title, content)")
c.store([...])   # rows auto-indexed
c.close()
```

```python
# psycopg2 via PG Wire — query using MATCH()
import psycopg2
conn = psycopg2.connect(host="localhost", port=5432, dbname="default")
cur = conn.cursor()
cur.execute("SELECT * FROM articles WHERE MATCH('rust')")
rows = cur.fetchall()
```

```python
# Arrow Flight — query using MATCH()
import pyarrow.flight as flight
client = flight.connect("grpc://localhost:50051")
info = client.get_flight_info(flight.FlightDescriptor.for_command(
    b"SELECT title FROM articles WHERE MATCH('python')"
))
reader = client.do_get(info.endpoints[0].ticket)
table = reader.read_all()
```

No extra configuration required — the FTS registry is global within the server process.

---

## Lifecycle and Storage

```
{database_dir}/
├── articles.apex          ← table data
├── fts_config.json        ← FTS configuration (shared with Python API)
└── fts_indexes/
    ├── articles.afts      ← ApexFTS atomic snapshot
    ├── articles.afts.wal  ← checksummed write-ahead log
    └── articles.afts.meta ← source-generation metadata used by v2.0.0 reconciliation
```

When an enabled table has only a legacy `.nfts` file, ApexBase treats the `.apex` table as the source of truth and rebuilds an `.afts` snapshot on first initialization. Unknown or corrupt ApexFTS versions are quarantined instead of being opened as valid data.

**`fts_config.json` format:**

```json
{
  "articles": {
    "enabled": true,
    "index_fields": ["title", "content"],
    "config": { "lazy_load": false, "cache_size": 10000 }
  },
  "wiki": {
    "enabled": true,
    "index_fields": null,
    "config": { "lazy_load": true, "cache_size": 200000 }
  }
}
```

This file is written by both the Rust DDL handlers (`CREATE FTS INDEX`) and the Python `init_fts()` / `drop_fts()` methods, ensuring consistent state across interfaces.

!!! note "v2.0.0 (V5 GA): generation reconciliation and stale-index rejection"
    On open, ApexBase reconciles the recovered `.afts` snapshot/WAL generation
    with its source table (row-count and table-generation checks). If the index
    cannot account for the table — a lost WAL tail, a writer that bypassed FTS,
    or a snapshot without source-generation metadata — the index is quarantined
    (renamed to `{table}.afts.stale`) and SQL full-text queries fail with an
    explicit error instead of answering from partial coverage:

    ```
    FTS index for table 'docs' is stale and was quarantined: <reason>;
    rebuild it with init_fts() before querying
    ```

    Rebuild the index to restore full-text reads, then run your query again.
    This is a safety property, not a retryable transient error: never silently
    accept a stale index in retrieval code.

---

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `lazy_load` | `false` | Keep the v3 term directory and postings mmap-backed. |
| `cache_size` | `10000` | Maximum decoded posting bitmaps retained in lazy mode. |

## Performance Tips

1. **Index only the columns you search.** Specifying `(title, content)` instead of indexing all string columns reduces index size and improves write throughput.

2. **Use `MATCH()` for known-correct queries.** `FUZZY_MATCH()` is slower due to edit-distance computation — reserve it for user search boxes.

3. **Combine with secondary indexes.** FTS supplies a compressed `_id` bitmap which the executor filters efficiently. Add a B-Tree index on high-selectivity non-FTS columns to speed up compound predicates:
   ```sql
   CREATE INDEX idx_cat ON articles (category);
   SELECT * FROM articles WHERE MATCH('python') AND category = 'tutorial';
   ```

4. **Rows are searchable immediately.** `store()` / `INSERT` index each document before they return, so a following `MATCH()` sees the new rows. Call `client.flush()` after a bulk load if you want the storage and WAL persisted now rather than on close.

5. **Compact after write-heavy periods.** `client.compact_fts_index()` merges snapshot, WAL, updates, and tombstones into one atomic `.afts` snapshot.

---

## Known Constraints

- `FTS_SCORE()` currently scores exact `MATCH()` queries; fuzzy edit-distance candidates do not expose BM25 scores through SQL.
- `hybrid_search()` needs an initialized FTS index and a filesystem database — its vector branch uses the direct mmap vector scan, so it does not run on a `:memory:` client (pure SQL and FTS work there).
- For tables you query with `hybrid_search()`, change rows with `store()` / `replace()` / `DELETE`; after a SQL `UPDATE` the direct vector scan can reject the column (`topk_distance(raw=True): column '...' is not served by the direct vector scan`) and does not recover in that session. Non-raw `topk_distance()` and SQL distance functions keep working.
- Legacy v2 snapshots use the eager compatibility loader and upgrade to v3 on the next compaction.
