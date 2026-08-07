# ApexBase Storage Architecture

## Overview

ApexBase uses a unified `StorageEngine` as the single entry point for all storage operations. This document defines the architecture and engineering guidelines for future development.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     Python Client (client.py)                   │
│                store() / retrieve() / execute() / ...           │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Python Bindings (bindings.rs)                │
│           ApexStorageImpl - PyO3 wrapper for Python             │
│                                                                 │
│  Responsibilities:                                              │
│  - File locking (acquire_write_lock / acquire_read_lock)        │
│  - FTS indexing coordination                                    │
│  - Local backend cache invalidation                             │
│  - Type conversion (Python ↔ Rust)                              │
└─────────────────────────────┬───────────────────────────────────┘
                              │ All storage ops via engine()
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                 StorageEngine (engine.rs) [SINGLETON]           │
│                                                                 │
│  Core Responsibilities:                                         │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ 1. Smart Write Routing                                  │    │
│  │    - Delta write: existing table + same columns         │    │
│  │    - Full write: new table / schema change / partial    │    │
│  └─────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ 2. Unified Cache Management                             │    │
│  │    - LRU eviction (MAX_CACHE_ENTRIES = 64)              │    │
│  │    - Auto-invalidation on write                         │    │
│  │    - Delta compaction on read                           │    │
│  └─────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ 3. Cache Invalidation                                   │    │
│  │    - invalidate(path) - single table                    │    │
│  │    - invalidate_dir(dir) - all tables in directory      │    │
│  │    - ApexExecutor cache sync                            │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              TableStorageBackend (backend.rs)                   │
│                                                                 │
│  Low-level operations:                                          │
│  - insert_rows() / insert_rows_to_delta()                       │
│  - delete() / replace()                                         │
│  - add_column() / drop_column() / rename_column()               │
│  - save() / compact()                                           │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│               OnDemandStorage (on_demand.rs)                    │
│                                                                 │
│  File format operations:                                        │
│  - Columnar file I/O (.apex format)                             │
│  - Delta file I/O (.apex.delta format)                          │
│  - Memory-mapped reads                                          │
│  - On-demand column loading                                     │
└─────────────────────────────────────────────────────────────────┘
```

## StorageEngine API Reference

### Write Operations

| Method | Description | Write Mode |
|--------|-------------|------------|
| `write(path, rows, durability)` | Smart write routing | Auto (delta/full) |
| `write_one(path, row, durability)` | Single row write | Auto |
| `write_typed(path, columns, durability)` | Columnar write | Full |

### Read Operations

| Method | Description |
|--------|-------------|
| `query(sql, base_dir, table_name)` | Execute SQL query |
| `retrieve(path, base_dir, table_name, id)` | Get single record |
| `exists(path, id)` | Check if record exists |
| `row_count(path)` | Total row count |
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

## Smart Write Routing

```rust
// Decision logic in StorageEngine::should_use_delta()
fn should_use_delta(table_path, rows) -> bool {
    // 1. Table must exist
    if !table_path.exists() { return false; }
    
    // 2. Table must have existing data
    if backend.row_count() == 0 { return false; }
    
    // 3. Columns must match exactly (no new, no missing)
    let schema_cols = backend.get_schema().columns();
    let data_cols = rows.columns();
    schema_cols == data_cols
}
```

### Write Mode Selection

| Condition | Write Mode | Reason |
|-----------|------------|--------|
| New table | Full | Need to create file structure |
| Empty table | Full | Need to establish schema |
| New columns | Full | Delta doesn't support schema evolution |
| Missing columns | Full | Preserve NULL semantics (not default 0) |
| Same columns | **Delta** | Memory efficient, append-only |

## Engineering Guidelines

### 1. Always Use StorageEngine

**DO:**
```rust
// In bindings.rs
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
let result = engine.write(&table_path, &rows, durability);
Self::release_lock(lock_file);
self.invalidate_backend(&table_name);
```

### 3. Cache Invalidation

- `StorageEngine` handles its own cache invalidation internally
- `bindings.rs` should invalidate its local `cached_backends` after writes
- Do NOT call `ApexExecutor::invalidate_cache_for_path()` directly - `StorageEngine` does this

### 4. Error Handling

All StorageEngine methods return `io::Result<T>`. Convert to PyErr at the bindings layer:

```rust
engine.write(&table_path, &rows, durability)
    .map_err(|e| PyIOError::new_err(e.to_string()))?;
```

### 5. Durability Levels

| Level | fsync Behavior | Use Case |
|-------|----------------|----------|
| `Fast` | No fsync | Development, testing |
| `Safe` | fsync on save | Production default |
| `Max` | fsync + WAL | Critical data |

## File Format

### V4 Row Group Format (.apex) — Default

V4 is the default save format since Feb 2026. Data is split into 64K-row Row Groups,
each self-contained with IDs, deletion vector, and per-column data.

```
┌─────────────────────────────────────┐
│ Header (256 bytes)                   │
│ - Magic "APEXV3", version=4         │
│ - row_count, column_count           │
│ - footer_offset, row_group_count    │
├─────────────────────────────────────┤
│ Row Group 0                          │
│ ┌─────────────────────────────────┐ │
│ │ RG Header (32B): magic, counts  │ │
│ │ IDs (u64 array)                 │ │
│ │ Deletion vector (bitmap)        │ │
│ │ Col 0: null bitmap + data       │ │
│ │ Col 1: null bitmap + data       │ │
│ │ ...                             │ │
│ └─────────────────────────────────┘ │
├─────────────────────────────────────┤
│ Row Group 1 ...                      │
├─────────────────────────────────────┤
│ V4 Footer                            │
│ - Schema (with dict-encoded types)   │
│ - Vec<RowGroupMeta> (40B each)       │
│ - footer_size + magic                │
└─────────────────────────────────────┘
```

**Key design decisions:**
- String columns are dict-encoded on disk for low-cardinality data (transparent to read path)
- In-memory state always uses plain `String` columns (dict encoding is disk-only)
- `Blob` columns store compact descriptors in the `.apex` column data and materialize bytes lazily from sidecar files only when the blob column is projected
- `save_v4()` pre-filters deleted rows, writes clean data, sets in-memory state directly (no disk reload)
- Legacy V3 files are auto-detected and read correctly; first save converts to V4

### Blob Sidecar Storage

`Blob` columns follow a Lance-like layout. The main table stores only a descriptor, so scans that do not project the blob column avoid loading large payloads.

- Inline blobs up to 64KB are embedded directly in the descriptor.
- Packed blobs from 64KB to 4MB are appended to `<table>.blobs/packed.blob`.
- Dedicated blobs larger than 4MB are written as separate files under `<table>.blobs/objects/`.
- Descriptors include length and checksum metadata. Point reads can fetch descriptor metadata, full bytes, or byte ranges without forcing unrelated columns to load.
- Projected blob reads return Arrow `LargeBinary`; scans that omit blob columns keep using descriptor-only column data.

### Delta File (.apex.delta)

```
┌─────────────────────────────────────┐
│ Delta Header                         │
├─────────────────────────────────────┤
│ Appended rows (bincode)              │
└─────────────────────────────────────┘
```

### Compaction

Delta files are compacted into the base file when:
- Delta size > 10MB (`DELTA_COMPACT_SIZE`)
- Delta rows > 100,000 (`DELTA_COMPACT_ROWS`)
- On read (transparent to caller)

Compaction loads all data in-memory, merges delta, and saves as V4.

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

## Adding New Operations

When adding a new storage operation:

1. **Add method to `StorageEngine`** (`engine.rs`)
   - Handle cache invalidation
   - Use appropriate backend method
   - Return `io::Result<T>`

2. **Update `bindings.rs`** to use the new engine method
   - Acquire file lock
   - Call engine method
   - Release lock
   - Invalidate local cache

3. **Write tests** covering:
   - Normal operation
   - Edge cases (empty data, non-existent table)
   - Concurrent access

4. **Update this document** if the operation changes the data flow

## Testing

Run all tests to verify storage operations:

```bash
maturin develop --release
pytest
```

All Python and Rust tests must pass; see `ENGINEERING_GUIDELINES.md` for the
full validation sequence.
