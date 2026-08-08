# ApexBase Storage Architecture

## Overview

ApexBase uses a unified `StorageEngine` as the single entry point for all
storage operations, with a `Database`/`Session` façade on top that Python
bindings, the embedded Rust API, the PostgreSQL Wire server, and the Arrow
Flight server all call. SQL execution is owned by `ApexExecutor`, which the
`Database` façade routes to; storage internals never call the SQL executor.

The on-disk format is the V4 row-group columnar format (`.apex`). Legacy V3
files were dropped in v1.10.0 and are rejected on open; the `APEXV3` magic is
retained in the header for compatibility with readers that check it.

This document defines the architecture and engineering guidelines for future
development.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Python Client (client.py)                       │
│                store() / retrieve() / execute() / ...               │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│              PyO3 Bindings (python/bindings/)                       │
│  wrapper.rs (ApexStorageImpl) + read / write / sql / arrow / blob   │
│                                                                     │
│  Responsibilities:                                                  │
│  - File locking (acquire_write_lock / acquire_read_lock)            │
│  - FTS index coordination                                           │
│  - Local cached_backends per ApexStorageImpl instance               │
│  - Type conversion (Python ↔ Rust)                                  │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ All storage ops via Database façade
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                Database / Session façade (database.rs)              │
│  - open_backend / cached_backend / create_backend / invalidate      │
│  - write / write_typed → StorageEngine + index notification         │
│  - execute / query / txn → ApexExecutor                             │
└──────────────┬──────────────────────────────────┬───────────────────┘
               │                                   │ SQL / DML / DDL
               ▼                                   ▼
┌────────────────────────────────────┐  ┌─────────────────────────────┐
│   StorageEngine (engine.rs)        │  │  ApexExecutor               │
│   [SINGLETON]                      │  │  (query/executor/)          │
│                                    │  │  - SQL planning + fast paths│
│  Core Responsibilities:            │  │  - QuerySignature classifier│
│  - Smart write routing             │  │  - Index / stats / FTS      │
│    (classify_write)                │  │    coordination             │
│  - LRU backend cache (64 entries)  │  └─────────────────────────────┘
│  - Epoch-checked cache invalidation│
│  - Delta compaction coordination   │
└──────────────┬─────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              TableStorageBackend (backend.rs)                       │
│                                                                     │
│  Low-level operations:                                              │
│  - insert_rows() / insert_rows_to_delta()                           │
│  - delete() / replace() / update                                    │
│  - add_column() / drop_column() / rename_column()                   │
│  - save() / compact() / open_for_insert / open_for_delete           │
└──────────────┬──────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                OnDemandStorage (on_demand/)                         │
│                                                                     │
│  - V4 columnar file I/O (read_write.rs, header.rs, storage_core.rs) │
│  - Memory-mapped scans (mmap_scan/{predicate,projection,groupby,    │
│    topk,vector,statistics}.rs)                                      │
│  - DeltaStore (.apex.delta) + compaction                            │
│  - Blob sidecars (<table>.blobs/)                                   │
│  - Stats sidecar (<table>.stats)                                    │
│  - Aggregation WAL (agg_wal.rs)                                     │
└─────────────────────────────────────────────────────────────────────┘
```

Supporting subsystems (not shown above, all under `apexbase/src/storage/`):

| Module | Role |
|--------|------|
| `epoch.rs` | Generation counters; caches are only trusted when `epoch + mtime` match |
| `mvcc/` | Snapshot / version store / GC used by transactions |
| `index/` | B-Tree and hash secondary indexes (`IndexManager`) |
| `delta/` | DeltaStore: update log + delete bitmap, merge logic |
| `incremental.rs` | Append-only WAL (`.apex.wal`) for incremental writes |
| `table_catalog.rs` | `.apex_tables` / `.apex_schemas` memory-mapped registries |
| `bloom.rs`, `concurrent.rs` | Filter helpers and concurrent primitives |

## StorageEngine API Reference

### Write Operations

| Method | Description | Write Mode |
|--------|-------------|------------|
| `write(path, rows, durability)` | Smart write routing | Auto (delta/full) |
| `write_one(path, row, durability)` | Single row write | Auto |
| `write_typed(path, columns..., durability)` | Typed columnar write | V4 append / full |

`query()` and `retrieve()` are no longer `StorageEngine` methods: SQL goes
through `Database::execute` / `Database::query`, and point retrieval through
the embedded API / Python bindings.

### Read Operations

| Method | Description |
|--------|-------------|
| `exists(path, id)` | Check if record exists |
| `row_count(path)` | Total row count (base + delta) |
| `active_row_count(path)` | Excluding deleted rows |

### Delete Operations

| Method | Description |
|--------|-------------|
| `delete(path, ids, durability)` | Delete multiple by IDs |
| `delete_one(path, id, durability)` | Delete single record |

### Schema Operations

| Method | Description |
|--------|-------------|
| `create_table(path, durability)` | Create new table |
| `create_table_with_schema(path, durability, schema)` | Create with explicit schema |
| `get_schema(path)` | Get table schema |
| `add_column(path, name, dtype, durability)` | Add column |
| `drop_column(path, name, durability)` | Remove column |
| `rename_column(path, old, new, durability)` | Rename column |
| `list_columns(path)` | List all columns |
| `get_column_type(path, name)` | Get column type |

### Cache Management

| Method | Description |
|--------|-------------|
| `invalidate(path)` | Invalidate single table cache |
| `invalidate_dir(dir)` | Invalidate all tables in directory |
| `get_read_backend(path)` | Cached read backend (epoch-checked) |
| `get_write_backend(path, durability)` | Write backend, compacts delta first |
| `get_insert_backend(path, durability)` | Append backend reused for incremental inserts |

## Smart Write Routing

Write routing is decided by `StorageEngine::classify_write()`, which combines
the legacy delta check with V4 detection in a single pass and uses an
epoch-checked schema cache to avoid file I/O on the fast path:

```rust
// engine.rs — classify_write(table_path, rows) -> (use_delta, is_v4)
// 1. Table must exist and be non-empty (file size >= 256 bytes)
// 2. V4 files ALWAYS take the full-write path:
//      get_insert_backend() + insert_rows() + save()
//      save() appends a new Row Group (no full rewrite, no delta file)
// 3. Non-V4 legacy files with an exact column match use the delta path:
//      insert_rows_to_delta() appends to .apex.delta
```

### Write Mode Selection

| Condition | Write Mode | Reason |
|-----------|------------|--------|
| New table | Full | Need to create file structure |
| Empty table | Full | Need to establish schema |
| V4 table (any schema) | **Full (Row Group append)** | Default format; `save()` appends via `append_row_group` |
| Legacy non-V4, new/missing columns | Full | Delta doesn't support schema evolution |
| Legacy non-V4, same columns | Delta | Memory-efficient append-only |

Notes:

- The Python `store()` / `store_columnar()` path calls `Database::write_typed`,
  which uses the `append_row_group` fast path when the V4 table exists with a
  matching schema — incremental inserts avoid rewriting the base file.
- SQL `INSERT` inside a transaction goes through
  `ApexExecutor` → `try_apply_txn_insert_delta()` → `insert_rows_to_delta()`
  and appends to `.apex.delta` directly (guarded: no constraints, no secondary
  indexes, no FTS on the table).
- V3 files are rejected on open (`Unsupported legacy file format (V3)`), so the
  non-V4 delta branch above is a legacy code path, not a supported on-disk
  format.

## Engineering Guidelines

### 1. Always Use Database Façade / StorageEngine

**DO:**

```rust
// In bindings
crate::Database::write(&table_path, &rows, durability)?;

// Storage-only call sites (engine-internal code)
let engine = crate::storage::engine::engine();
engine.write(&table_path, &rows, durability)?;
```

**DON'T:**

```rust
// Direct backend access - AVOID
let backend = TableStorageBackend::open(&table_path)?;
backend.insert_rows(&rows)?;
backend.save()?;
```

### 2. Lock Ordering

Always acquire locks in this order to prevent deadlocks:
1. File lock (`acquire_write_lock` / `acquire_read_lock`)
2. StorageEngine operation
3. Release file lock
4. Invalidate local caches

```rust
// Correct pattern
let lock_file = Self::acquire_write_lock(&table_path)?;
let result = crate::Database::write(&table_path, &rows, durability);
Self::release_lock(lock_file);
self.invalidate_backend(&table_name);
```

### 3. Cache Invalidation

- `StorageEngine` invalidates its own backend/schema caches internally.
- The `Database` façade invalidates per-instance `cached_backends` and the
  global engine cache after writes.
- Python bindings invalidate their `cached_backends` map after writes
  (`invalidate_backend`).
- Do NOT reach into `ApexExecutor` caches from storage code — the executor
  coordinates its own index/stats/FTS invalidation through `Database`.

### 4. Error Handling

All StorageEngine methods return `io::Result<T>`. Convert to PyErr at the
bindings layer:

```rust
engine.write(&table_path, &rows, durability)
    .map_err(|e| PyIOError::new_err(e.to_string()))?;
```

### 5. Durability Levels

| Level | fsync Behavior | Use Case |
|-------|----------------|----------|
| `Fast` | No fsync | Development, testing |
| `Safe` | fsync on save/flush | Production default |
| `Max` | fsync on every write | Critical data |

The append-only WAL (`.apex.wal`, `storage/incremental.rs`) is a separate
incremental-write mechanism and is not tied to the `Max` durability level.

## File Format

### V4 Row Group Format (.apex) — Default

V4 is the current default save format; legacy V3 compatibility was dropped in
v1.10.0 (2026-03-18). Data is split into adaptive-size Row Groups (default
65,536 rows; 131,072 for very narrow rows, 32,768 for wide rows), each
self-contained with IDs, deletion vector, and per-column data.

```
┌─────────────────────────────────────┐
│ Header (256 bytes)                   │
│ - Magic "APEXV3\0\0" (retained),     │
│   version=4, flags, row_count,       │
│   column_count, row_group_size,      │
│   schema_offset, column_index_offset,│
│   id_offset, checksum                │
├─────────────────────────────────────┤
│ Schema Block                         │
├─────────────────────────────────────┤
│ Column Index (32 bytes per column)   │
├─────────────────────────────────────┤
│ ID Column (contiguous u64 array)     │
├─────────────────────────────────────┤
│ Row Group 0                          │
│ ┌─────────────────────────────────┐ │
│ │ RG Header (32B): magic "APXG",  │ │
│ │ row_count, col_count, min_id,   │ │
│ │ max_id, flags (LZ4/ZSTD/NONE)   │ │
│ │ IDs (u64, contiguous-encoded)   │ │
│ │ Deletion vector (bitmap)        │ │
│ │ Col 0: null bitmap + data       │ │
│ │ Col 1: null bitmap + data       │ │
│ │ ...                             │ │
│ └─────────────────────────────────┘ │
├─────────────────────────────────────┤
│ Row Group 1 ...                      │
├─────────────────────────────────────┤
│ V4 Footer                            │
│ - Schema                            │
│ - Vec<RowGroupMeta> (40B each)      │
│ - Optional zone maps (ZMAP)         │
│ - Optional RCIX offsets (per-RG     │
│   per-column body offsets)          │
│ - footer_size + magic "APXFOOT\0"   │
└─────────────────────────────────────┘
```

**Key design decisions:**

- String columns are dict-encoded on disk for low-cardinality data (transparent
  to the read path); in-memory state always uses plain `String`.
- Row groups can be LZ4- or ZSTD-compressed (flag bits in the RG header);
  default is no compression for maximum read performance.
- Appends are chunked by `row_group_size`: a single large batch is split into
  multiple Row Groups, so per-RG buffers and later streaming rewrites stay
  bounded regardless of batch size.
- Zone maps (`ZMAP`, numeric min/max per RG) let scans skip row groups that
  cannot match a filter; the RCIX section enables O(1) direct seeks for
  cold-start `SELECT * LIMIT N`.
- A statistics sidecar `<table>.stats` (magic `APEXSTAT`) stores per-column
  null counts / ranges / histograms for the query optimizer and is invalidated
  with each DDL/DML/compaction generation.
- `save_v4()` pre-filters deleted rows, writes clean data, sets in-memory state
  directly (no disk reload), and appends new Row Groups for incremental writes.
- The Python `store()` / `store_columnar()` path builds Row Group columns
  directly from borrowed Python buffers (`&str` / `&[u8]`, no per-element
  `String`/`Vec` allocation) and passes them to
  `StorageEngine::write_typed_columns`, which appends without re-copying into
  a typed intermediate layer. Peak memory for a string-heavy batch drops from
  ~2× the data to ~1× (the caller's input plus one column buffer).
- In-memory string/binary columns use u64 offsets; the on-disk format keeps u32
  offsets per Row Group (blocks stay far below 4 GiB), so columns larger than
  4 GiB never silently truncate in memory. The disk format is unchanged.
- V3 files are **not** auto-converted: opening one fails with
  `Unsupported legacy file format (V3). Please re-create the table.`

### Blob Sidecar Storage

`Blob` columns follow a Lance-like layout. The main table stores only a
descriptor, so scans that do not project the blob column avoid loading large
payloads.

- Inline blobs up to 64KB are embedded directly in the descriptor.
- Packed blobs from 64KB to 4MB are appended to `<table>.blobs/packed.blob`.
- Dedicated blobs larger than 4MB are written as separate files under
  `<table>.blobs/objects/`.
- Descriptors include length and checksum metadata. Point reads can fetch
  descriptor metadata, full bytes, or byte ranges without forcing unrelated
  columns to load.
- Projected blob reads return Arrow `LargeBinary`; scans that omit blob columns
  keep using descriptor-only column data.

### Delta File (.apex.delta)

`.apex.delta` persists the DeltaStore: an update log (`(row, col, new_value)`)
plus a delete bitmap, serialized with bincode. It is used for:

- pending deletes and row updates applied on top of the base V4 file;
- SQL transactional INSERT batches appended via `insert_rows_to_delta()`;
- legacy non-V4 delta appends.

### Compaction

Delta files are compacted into the base file when:
- Delta size > 10MB (`DELTA_COMPACT_SIZE`)
- Delta rows > 100,000 (`DELTA_COMPACT_ROWS`)
- Before opening a write backend when a delta exists (`open_for_compact`)

Compaction merges delta into the V4 base file and clears the delta store.
For mmap-only V4 backends (the production path) this is a **streaming
RG-by-RG rewrite** (`compact_streaming_v4`): each Row Group is parsed from the
mmap, merged with the DeltaStore updates/deletes and the appended delta rows,
and written to a fresh `.apex.tmp` before an atomic rename. Peak memory is
O(largest Row Group + delta payloads), so tables larger than physical memory
can be compacted. The legacy in-memory merge path remains for backends that
already materialized the base.

## Schema Changes (bounded memory)

- **ADD COLUMN** on mmap-only V4 tables is footer-only: only the footer schema
  is updated, no data is rewritten. Read paths synthesize all-NULL values for
  rows already on disk; later compactions materialize the column.
- **DROP COLUMN** runs a streaming RG-by-RG rewrite
  (`rewrite_v4_drop_columns`) that physically removes the column without
  loading the table. Any pending delta is compacted first.
- **RENAME COLUMN** updates the footer schema in place.

## Deletes (bounded memory)

- Uncompressed Row Groups: deletion vectors are updated in place (O(row groups)
  writes) via `save_delete_only`.
- Compressed Row Groups (deletion vectors live inside the compressed body): the
  delete triggers a streaming rewrite (`rewrite_v4_active_rows`) that drops the
  deleted rows RG-by-RG via mmap instead of loading the whole table.

## Table Catalog (`.apex_tables`)

Since v1.28.0, each database directory owns a memory-mapped binary table
registry, `.apex_tables`, that is the authoritative source of table names
across processes. It replaces the earlier behavior where `create_table` on an
existing table could silently rebuild it: a fresh process calling
`create_table` on an existing table now raises `Table already exists`.

- **Layout**: 32-byte header with magic `APXTBL02` and format version 2,
  followed by fixed-size slots (name length + 128-byte name + CRC32), default
  capacity 1024.
- **Concurrency**: all mutations run under an exclusive advisory lock
  (`.apex_tables.lock`) and update the mapped region in place, so CREATE/DROP
  no longer pay a full file rewrite per DDL.
- **Integrity**: each slot carries its own CRC32, so accidental corruption or
  manual edits are detected and rejected.
- **Readers**: take an optimistic generation snapshot, verify CRCs, and retry
  if the generation changed. A snapshot cache keyed by generation plus file
  mtime guards against external rewrites.
- **Migration**: legacy databases without a catalog, or with the earlier
  one-shot binary/JSON formats, are backfilled/migrated on first access.

Lazy schema information is kept in a second memory-mapped registry,
`.apex_schemas` (magic `APXSCM01`, version 1, 256 KiB region), used by the
on-demand storage layer to avoid parsing full `.apex` headers for schema-only
reads.

## Concurrency and MVCC

- **Epochs**: every cache entry records the table epoch at insertion time;
  readers re-check `epoch + mtime` before trusting a cached backend. All
  storage mutations take a `logical_write` epoch guard.
- **MVCC**: transaction reads use `Snapshot` / `SnapshotManager`; row versions
  live in a `VersionStore` and are reclaimed by the `GarbageCollector`.
- **Locks**: table file locks (advisory) serialize write paths; the catalog
  uses its own `.apex_tables.lock`; writes are serialized per table while V4
  mmap-only reads run in parallel.

## Adding New Operations

When adding a new storage operation:

1. **Add the method to `StorageEngine`** (`engine.rs`)
   - Handle cache invalidation
   - Use appropriate backend method
   - Return `io::Result<T>`

2. **Expose it through the `Database` façade** (`database.rs`) if bindings or
   embedded callers need it; keep SQL-facing behavior in `ApexExecutor`.

3. **Update the bindings** (`python/bindings/`) to use the new façade method
   - Acquire file lock
   - Call the façade/engine method
   - Release lock
   - Invalidate local `cached_backends`

4. **Write tests** covering:
   - Normal operation
   - Edge cases (empty data, non-existent table)
   - Concurrent access

5. **Update this document** if the operation changes the data flow

## Testing

Run all tests to verify storage operations:

```bash
maturin develop --release
pytest
cargo test
```

All Python and Rust tests must pass; see `ENGINEERING_GUIDELINES.md` for the
full validation sequence.
