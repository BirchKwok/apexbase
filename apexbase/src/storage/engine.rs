//! StorageEngine - Unified storage interface
//!
//! This module provides a single entry point for all storage operations,
//! handling complexity like caching, delta writes, and data merging internally.
//!
//! # Architecture
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │            Python Bindings                   │
//! │  store() / retrieve() / execute() / ...     │
//! └─────────────────┬───────────────────────────┘
//!                   │ Simple API
//!                   ▼
//! ┌─────────────────────────────────────────────┐
//! │           StorageEngine                      │
//! │  - Unified cache management                  │
//! │  - Automatic write routing (full/delta)     │
//! │  - Automatic read merging (base+delta)      │
//! └─────────────────┬───────────────────────────┘
//!                   │
//!                   ▼
//! ┌─────────────────────────────────────────────┐
//! │     Low-level storage (OnDemandStorage)     │
//! └─────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use ahash::AHashMap;
use once_cell::sync::Lazy;
use parking_lot::RwLock;

use super::backend::TableStorageBackend;
use super::on_demand::ColumnType;
use super::DurabilityLevel;
use crate::data::Value;

// ============================================================================
// Configuration
// ============================================================================

/// Maximum number of cached table backends
const MAX_CACHE_ENTRIES: usize = 64;

/// Delta file size threshold for auto-compaction (10MB)
const DELTA_COMPACT_SIZE: u64 = 10 * 1024 * 1024;

/// Delta row count threshold for auto-compaction
const DELTA_COMPACT_ROWS: usize = 100_000;

/// Decode a Binary ColumnData (offsets + data) into per-row `Vec<u8>` values.
fn binary_column_to_values(offsets: &[u64], data: &[u8], row_count: usize) -> Vec<Vec<u8>> {
    let mut values = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let start = offsets.get(i).copied().unwrap_or(0) as usize;
        let end = offsets.get(i + 1).copied().unwrap_or(data.len() as u64) as usize;
        values.push(if start <= end && end <= data.len() {
            data[start..end].to_vec()
        } else {
            Vec::new()
        });
    }
    values
}

fn materialize_vector_derivations(
    columns: &mut HashMap<String, super::on_demand::ColumnData>,
    null_positions: &mut HashMap<String, Vec<bool>>,
    schema: &[(String, ColumnType)],
    derivations: &[super::on_demand::VectorDerivation],
) -> io::Result<()> {
    use super::on_demand::ColumnData;

    for derivation in derivations {
        if columns.contains_key(&derivation.target) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "quantized vector column '{}' is system-maintained and cannot be written directly",
                    derivation.target
                ),
            ));
        }
        let target_type = schema
            .iter()
            .find(|(name, _)| name == &derivation.target)
            .map(|(_, dtype)| *dtype)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "quantized vector derivation target '{}' is missing from schema",
                        derivation.target
                    ),
                )
            })?;
        let codec = target_type.vector_codec().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("column '{}' has no vector codec", derivation.target),
            )
        })?;
        let source = columns.get(&derivation.source).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write must include source vector column '{}' required by '{}'",
                    derivation.source, derivation.target
                ),
            )
        })?;

        let (source_bytes, dim, source_codec) = match source {
            ColumnData::FixedList { data, dim } => (
                data.as_slice(),
                *dim as usize,
                crate::compute::vector_quantization::VectorCodec::Float32,
            ),
            ColumnData::Float16List { data, dim } => (
                data.as_slice(),
                *dim as usize,
                crate::compute::vector_quantization::VectorCodec::Float16,
            ),
            ColumnData::QuantizedList { data, dim, codec } => {
                (data.as_slice(), *dim as usize, *codec)
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("source vector column '{}' is not a vector", derivation.source),
                ))
            }
        };
        let source_stride = source_codec.row_width(dim)?;
        if dim == 0 || source_stride == 0 || source_bytes.len() % source_stride != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("source vector column '{}' has invalid row width", derivation.source),
            ));
        }

        let row_count = source_bytes.len() / source_stride;
        let mut data = Vec::with_capacity(row_count * codec.row_width(dim)?);
        let mut values = vec![0.0f32; dim];
        for row in source_bytes.chunks_exact(source_stride) {
            crate::compute::vector_quantization::decode_vector(
                source_codec,
                row,
                dim,
                &mut values,
            )?;
            crate::compute::vector_quantization::encode_vector(codec, &values, &mut data)?;
        }
        let derived = if target_type == ColumnType::Float16List {
            ColumnData::Float16List { data, dim: dim as u32 }
        } else {
            ColumnData::QuantizedList {
                data,
                dim: dim as u32,
                codec,
            }
        };
        columns.insert(derivation.target.clone(), derived);
        if let Some(source_nulls) = null_positions.get(&derivation.source).cloned() {
            null_positions.insert(derivation.target.clone(), source_nulls);
        }
    }
    Ok(())
}

// ============================================================================
// Cache Entry
// ============================================================================

/// Cached table entry with metadata for LRU eviction
struct CacheEntry {
    /// The storage backend
    backend: Arc<TableStorageBackend>,
    /// File modification time when cached
    modified_time: SystemTime,
    /// Last access time for LRU eviction
    last_access: Instant,
    /// Whether there's a pending delta file
    has_delta: bool,
    /// Table epoch observed when this backend was cached.
    epoch: u64,
}

/// Lightweight schema cache entry (for fast should_use_delta checks)
struct SchemaCache {
    /// Column names in schema order
    columns: std::collections::HashSet<String>,
    /// Row count (0 means empty table, use full write)
    row_count: u64,
    /// File modification time when cached
    modified_time: SystemTime,
    /// Whether this is a V4 format file (cached to avoid repeated header reads)
    is_v4: bool,
    /// Table epoch observed when this schema was cached.
    epoch: u64,
}

struct InsertCacheEntry {
    backend: Arc<TableStorageBackend>,
    epoch: u64,
}

// ============================================================================
// Global Engine Instance
// ============================================================================

/// Global storage engine instance (singleton pattern)
static ENGINE: Lazy<StorageEngine> = Lazy::new(StorageEngine::new);

// ============================================================================
// StorageEngine
// ============================================================================

/// Unified storage engine that handles all read/write operations
///
/// # Features
/// - **Unified caching**: Single cache for all table backends
/// - **Automatic write routing**: Chooses full write or delta write based on conditions
/// - **Automatic read merging**: Transparently merges base and delta data
/// - **Simple API**: Hides all complexity from callers
pub struct StorageEngine {
    /// Unified cache for all table backends
    cache: RwLock<AHashMap<PathBuf, CacheEntry>>,
    /// Lightweight schema cache for fast should_use_delta checks
    schema_cache: RwLock<AHashMap<PathBuf, SchemaCache>>,
    /// Cache for insert-mode backends (for delta writes) - avoids repeated file I/O
    insert_cache: RwLock<AHashMap<PathBuf, InsertCacheEntry>>,
    /// Authoritative process-local tables. Unlike the bounded disk caches,
    /// these entries are pinned until their memory database is dropped.
    memory_tables: RwLock<AHashMap<PathBuf, Arc<TableStorageBackend>>>,
}

impl StorageEngine {
    /// Create a new storage engine
    fn new() -> Self {
        Self {
            cache: RwLock::new(AHashMap::with_capacity(MAX_CACHE_ENTRIES)),
            schema_cache: RwLock::new(AHashMap::with_capacity(MAX_CACHE_ENTRIES * 2)),
            insert_cache: RwLock::new(AHashMap::with_capacity(MAX_CACHE_ENTRIES)),
            memory_tables: RwLock::new(AHashMap::new()),
        }
    }

    /// Get the global engine instance
    pub fn global() -> &'static StorageEngine {
        &ENGINE
    }

    // ========================================================================
    // Cache Management
    // ========================================================================

    /// Evict least recently used entries if cache is full
    fn evict_lru_if_needed(cache: &mut AHashMap<PathBuf, CacheEntry>) {
        while cache.len() >= MAX_CACHE_ENTRIES {
            // Find LRU entry
            let lru_key = cache
                .iter()
                .min_by_key(|(_, entry)| entry.last_access)
                .map(|(k, _)| k.clone());

            if let Some(key) = lru_key {
                cache.remove(&key);
            } else {
                break;
            }
        }
    }

    /// Check if delta file exists for a table
    fn has_delta_file(table_path: &Path) -> bool {
        let delta_path = Self::delta_path(table_path);
        delta_path.exists()
    }

    /// Get delta file path for a table
    fn delta_path(table_path: &Path) -> PathBuf {
        let mut delta = table_path.to_path_buf();
        let name = delta.file_name().unwrap_or_default().to_string_lossy();
        delta.set_file_name(format!("{}.delta", name));
        delta
    }

    /// Get file modification time
    fn get_modified_time(path: &Path) -> SystemTime {
        std::fs::metadata(path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH)
    }

    /// Invalidate cache for a specific table
    pub fn invalidate(&self, table_path: &Path) {
        // Use path directly without canonicalize for speed (already absolute in most cases)
        self.cache.write().remove(table_path);
        self.insert_cache.write().remove(table_path);
        self.schema_cache.write().remove(table_path);
    }

    /// Invalidate read/query caches after an append while keeping the insert backend warm.
    ///
    /// The insert backend owns the just-updated footer/next-id state, so dropping it after
    /// every small append defeats the write fast path. Read caches still need to be cleared
    /// so subsequent queries see the newly appended row group.
    #[inline]
    fn invalidate_after_append(&self, table_path: &Path) {
        self.cache.write().remove(table_path);
        self.schema_cache.write().remove(table_path);
    }

    /// Invalidate all caches under a directory
    pub fn invalidate_dir(&self, dir: &Path) {
        // Use path directly without canonicalize for speed
        self.cache.write().retain(|path, _| !path.starts_with(dir));
        self.schema_cache
            .write()
            .retain(|path, _| !path.starts_with(dir));
        self.insert_cache
            .write()
            .retain(|path, _| !path.starts_with(dir));
    }

    #[inline]
    pub fn memory_backend(&self, table_path: &Path) -> Option<Arc<TableStorageBackend>> {
        self.memory_tables.read().get(table_path).cloned()
    }

    #[inline]
    pub fn table_exists(&self, table_path: &Path) -> bool {
        table_path.exists() || self.memory_tables.read().contains_key(table_path)
    }

    pub fn list_memory_tables(&self, base_dir: &Path) -> Vec<String> {
        let mut tables: Vec<String> = self
            .memory_tables
            .read()
            .keys()
            .filter(|path| path.parent() == Some(base_dir))
            .filter_map(|path| path.file_stem()?.to_str().map(str::to_owned))
            .collect();
        tables.sort_unstable();
        tables
    }

    pub fn drop_memory_table(&self, table_path: &Path) -> bool {
        self.cache.write().remove(table_path);
        self.insert_cache.write().remove(table_path);
        self.schema_cache.write().remove(table_path);
        self.memory_tables.write().remove(table_path).is_some()
    }

    pub fn drop_memory_database(&self, base_dir: &Path) {
        self.invalidate_dir(base_dir);
        self.memory_tables
            .write()
            .retain(|path, _| !path.starts_with(base_dir));
    }

    // ========================================================================
    // Backend Access
    // ========================================================================

    /// Get or create a backend for writing (loads existing data)
    ///
    /// This method:
    /// 1. Checks cache for existing backend
    /// 2. If cached and fresh, returns it
    /// 3. If delta exists, compacts it first
    /// 4. Opens fresh backend and caches it
    pub fn get_write_backend(
        &self,
        table_path: &Path,
        durability: DurabilityLevel,
    ) -> io::Result<Arc<TableStorageBackend>> {
        if let Some(backend) = self.memory_backend(table_path) {
            return Ok(backend);
        }
        // Use path directly - avoid expensive canonicalize
        let cache_key = table_path.to_path_buf();
        let epoch = crate::storage::epoch::current(table_path);
        let has_delta = Self::has_delta_file(table_path);
        let modified = Self::get_modified_time(table_path);

        // Check cache first (only if no delta pending)
        if !has_delta {
            let mut cache = self.cache.write();
            if let Some(entry) = cache.get_mut(&cache_key) {
                if entry.epoch == epoch && entry.modified_time >= modified && !entry.has_delta {
                    entry.last_access = Instant::now();
                    return Ok(entry.backend.clone());
                }
            }
        }

        // If delta exists, compact it first using lightweight open (metadata only)
        if has_delta {
            let storage = TableStorageBackend::open_for_compact(table_path)?;
            storage.compact()?;
            // Invalidate after compaction
            self.cache.write().remove(&cache_key);
            self.schema_cache.write().remove(&cache_key);
        }

        // Open fresh backend
        let backend = if table_path.exists() {
            TableStorageBackend::open_for_write_with_durability(table_path, durability)?
        } else {
            // Lazy table: materialize with the schema recorded in the table
            // catalog when CREATE deferred the per-table file write.
            crate::storage::table_catalog::materialize_table_backend(
                table_path,
                durability,
            )?
        };

        let backend = Arc::new(backend);
        let new_modified = Self::get_modified_time(table_path);

        // Cache the backend and update schema cache
        {
            let mut cache = self.cache.write();
            Self::evict_lru_if_needed(&mut cache);
            cache.insert(
                cache_key.clone(),
                CacheEntry {
                    backend: backend.clone(),
                    modified_time: new_modified,
                    last_access: Instant::now(),
                    has_delta: false,
                    epoch,
                },
            );
        }

        // Update schema cache
        {
            let schema_cols: std::collections::HashSet<String> = backend
                .get_schema()
                .into_iter()
                .map(|(name, _)| name)
                .collect();
            let row_count = backend.row_count();
            let mut schema_cache = self.schema_cache.write();
            schema_cache.insert(
                cache_key,
                SchemaCache {
                    columns: schema_cols,
                    row_count,
                    modified_time: new_modified,
                    is_v4: false,
                    epoch,
                },
            );
        }

        Ok(backend)
    }

    /// Get backend for read-only operations (may use cached version)
    pub fn get_read_backend(&self, table_path: &Path) -> io::Result<Arc<TableStorageBackend>> {
        if let Some(backend) = self.memory_backend(table_path) {
            return Ok(backend);
        }
        // Use path directly - avoid expensive canonicalize
        let cache_key = table_path.to_path_buf();
        let epoch = crate::storage::epoch::current(table_path);
        let modified = Self::get_modified_time(table_path);

        // Check cache
        {
            let mut cache = self.cache.write();
            if let Some(entry) = cache.get_mut(&cache_key) {
                if entry.epoch == epoch && entry.modified_time >= modified {
                    entry.last_access = Instant::now();
                    return Ok(entry.backend.clone());
                }
            }
        }

        // Open fresh backend (read-only mode). A registered table whose file
        // was not yet materialized is created empty (with its catalog schema);
        // unregistered paths keep failing with NotFound instead of creating
        // stray files.
        let backend = if table_path.exists() {
            Arc::new(TableStorageBackend::open(table_path)?)
        } else if crate::storage::table_catalog::file_exists_or_registered(table_path)? {
            Arc::new(crate::storage::table_catalog::materialize_table_backend(
                table_path,
                DurabilityLevel::Fast,
            )?)
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{}' does not exist", table_path.display()),
            ));
        };
        let new_modified = Self::get_modified_time(table_path);

        // Cache it
        {
            let mut cache = self.cache.write();
            Self::evict_lru_if_needed(&mut cache);
            cache.insert(
                cache_key.clone(),
                CacheEntry {
                    backend: backend.clone(),
                    modified_time: new_modified,
                    last_access: Instant::now(),
                    has_delta: false,
                    epoch,
                },
            );
        }

        // Update schema cache
        {
            let schema_cols: std::collections::HashSet<String> = backend
                .get_schema()
                .into_iter()
                .map(|(name, _)| name)
                .collect();
            let row_count = backend.row_count();
            let mut schema_cache = self.schema_cache.write();
            schema_cache.insert(
                cache_key,
                SchemaCache {
                    columns: schema_cols,
                    row_count,
                    modified_time: new_modified,
                    is_v4: false,
                    epoch,
                },
            );
        }

        Ok(backend)
    }

    /// Get or create a cached insert backend (for delta writes)
    /// This avoids repeated file I/O when doing many small writes
    fn get_insert_backend(
        &self,
        table_path: &Path,
        durability: DurabilityLevel,
    ) -> io::Result<Arc<TableStorageBackend>> {
        if let Some(backend) = self.memory_backend(table_path) {
            return Ok(backend);
        }
        let cache_key = table_path.to_path_buf();
        let epoch = crate::storage::epoch::current(table_path);

        // Check insert cache first
        {
            let cache = self.insert_cache.read();
            if let Some(entry) = cache.get(&cache_key) {
                if entry.epoch == epoch {
                    return Ok(Arc::clone(&entry.backend));
                }
            }
        }

        // Open new insert backend
        let backend = Arc::new(TableStorageBackend::open_for_insert_with_durability(
            table_path, durability,
        )?);

        // Cache it
        {
            let mut cache = self.insert_cache.write();
            // Evict if too many entries
            if cache.len() >= MAX_CACHE_ENTRIES {
                if let Some(key) = cache.keys().next().cloned() {
                    cache.remove(&key);
                }
            }
            cache.insert(
                cache_key,
                InsertCacheEntry {
                    backend: Arc::clone(&backend),
                    epoch,
                },
            );
        }

        Ok(backend)
    }

    // ========================================================================
    // Write Operations
    // ========================================================================

    /// Write rows to a table with smart routing
    ///
    /// Automatically chooses the optimal write strategy:
    /// - **Delta write**: When table exists with data AND columns match exactly
    /// - **Full write**: For new tables, schema evolution, or partial columns
    pub fn write(
        &self,
        table_path: &Path,
        rows: &[HashMap<String, Value>],
        durability: DurabilityLevel,
    ) -> io::Result<Vec<u64>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let epoch_write = crate::storage::epoch::logical_write(table_path);

        // Stored vector accelerators are derived state. Expand row-oriented
        // writes before routing so the delta/full-write classifier sees the
        // physical target columns and callers cannot write stale accelerators.
        let mut expanded_rows = None;
        if self.table_exists(table_path) {
            let derivations = self.get_read_backend(table_path)?.storage.vector_derivations();
            if !derivations.is_empty() {
                let mut expanded = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut output = row.clone();
                    for derivation in &derivations {
                        if row.contains_key(&derivation.target) {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!(
                                    "quantized vector column '{}' is system-maintained and cannot be written directly",
                                    derivation.target
                                ),
                            ));
                        }
                        let derived = match row.get(&derivation.source) {
                            Some(Value::FixedList(bytes)) => Value::FixedList(bytes.clone()),
                            Some(Value::Null) => Value::Null,
                            Some(_) => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!(
                                        "source vector column '{}' must be a Float32 vector",
                                        derivation.source
                                    ),
                                ))
                            }
                            None => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!(
                                        "write must include source vector column '{}' required by '{}'",
                                        derivation.source, derivation.target
                                    ),
                                ))
                            }
                        };
                        output.insert(derivation.target.clone(), derived);
                    }
                    expanded.push(output);
                }
                expanded_rows = Some(expanded);
            }
        }
        let rows = expanded_rows.as_deref().unwrap_or(rows);

        // Determine write strategy and V4 status in one pass (avoids double is_v4_file)
        let (use_delta, is_v4) = self.classify_write(table_path, rows);

        let ids = if use_delta {
            // Delta write: memory efficient, schema unchanged
            // Use cached insert backend to avoid repeated file I/O
            let backend = self.get_insert_backend(table_path, durability)?;
            let ids = backend.insert_rows_to_delta(rows)?;

            // For delta writes, only invalidate backend cache (not schema cache)
            // Schema doesn't change, so schema cache remains valid
            self.cache.write().remove(table_path);
            self.invalidate_after_append(table_path);

            ids
        } else {
            // Full write: for new tables, schema evolution, or partial columns
            // V4 files: use insert backend (metadata-only) since save() uses
            // append_row_group. open_for_write loads all data which is empty
            // for V4 mmap-only, causing data loss.
            let backend = if is_v4 {
                self.get_insert_backend(table_path, durability)?
            } else {
                self.get_write_backend(table_path, durability)?
            };
            let ids = backend.insert_rows(rows)?;
            backend.save()?;

            // Full invalidation for schema changes
            self.invalidate(table_path);

            ids
        };

        epoch_write.commit();
        drop(epoch_write);
        if let Some(entry) = self.insert_cache.write().get_mut(table_path) {
            entry.epoch = crate::storage::epoch::current(table_path);
        }
        Ok(ids)
    }

    /// Check if a file is V4 format by reading the header version field.
    #[inline]
    fn is_v4_file(table_path: &Path) -> bool {
        if crate::storage::is_memory_path(table_path) {
            return false;
        }
        // Header: 8-byte magic + 4-byte version
        let mut buf = [0u8; 12];
        if let Ok(mut f) = std::fs::File::open(table_path) {
            use std::io::Read;
            if f.read_exact(&mut buf).is_ok() {
                let version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
                return version >= 4;
            }
        }
        false
    }

    /// Classify write operation: returns (use_delta, is_v4)
    ///
    /// OPTIMIZED: Combines should_use_delta + is_v4_file into a single pass.
    /// Uses schema cache (with is_v4 field) to avoid repeated file header reads.
    #[inline]
    fn classify_write(&self, table_path: &Path, rows: &[HashMap<String, Value>]) -> (bool, bool) {
        if rows.is_empty() {
            return (false, false);
        }
        if crate::storage::is_memory_path(table_path) {
            return (false, false);
        }

        // Single metadata call for existence, size, and modified time
        let meta = match std::fs::metadata(table_path) {
            Ok(m) => m,
            Err(_) => return (false, false), // File doesn't exist
        };

        // Fast path: check file size (empty files should use full write)
        if meta.len() < 256 {
            return (false, false);
        }

        let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let cache_key = table_path.to_path_buf();
        let epoch = crate::storage::epoch::current(table_path);

        // Try schema cache first (FAST PATH — avoids file I/O entirely)
        {
            let schema_cache = self.schema_cache.read();
            if let Some(cached) = schema_cache.get(&cache_key) {
                if cached.epoch == epoch && cached.modified_time >= modified {
                    // V4 files always use full write (append_row_group)
                    if cached.is_v4 {
                        return (false, true);
                    }
                    // Non-V4 with data: check schema match for delta
                    if cached.row_count > 0 {
                        let use_delta = rows.iter().all(|row| {
                            row.len() == cached.columns.len()
                                && row.keys().all(|name| cached.columns.contains(name))
                        });
                        return (use_delta, false);
                    }
                    return (false, false);
                }
            }
        }

        // Cache miss — read V4 status from file header (single file open)
        let is_v4 = Self::is_v4_file(table_path);

        // V4 files: always full write, update cache with is_v4 flag
        if is_v4 {
            // Lightweight cache update (just V4 flag, no full schema load)
            let mut schema_cache = self.schema_cache.write();
            schema_cache.insert(
                cache_key,
                SchemaCache {
                    columns: std::collections::HashSet::new(),
                    row_count: 0,
                    modified_time: modified,
                    is_v4: true,
                    epoch,
                },
            );
            return (false, true);
        }

        // Non-V4 cache miss: open backend for schema check (SLOW PATH)
        let backend = match TableStorageBackend::open(table_path) {
            Ok(b) => b,
            Err(_) => return (false, false),
        };

        let row_count = backend.row_count();
        if row_count == 0 {
            return (false, false);
        }

        let schema_cols: std::collections::HashSet<_> = backend
            .get_schema()
            .into_iter()
            .map(|(name, _)| name)
            .collect();

        // Update schema cache
        {
            let mut schema_cache = self.schema_cache.write();
            schema_cache.insert(
                cache_key,
                SchemaCache {
                    columns: schema_cols.clone(),
                    row_count,
                    modified_time: modified,
                    is_v4: false,
                    epoch,
                },
            );
        }

        let use_delta = rows.iter().all(|row| {
            row.len() == schema_cols.len() && row.keys().all(|name| schema_cols.contains(name))
        });
        (use_delta, false)
    }

    /// Write a single row to a table
    pub fn write_one(
        &self,
        table_path: &Path,
        row: HashMap<String, Value>,
        durability: DurabilityLevel,
    ) -> io::Result<u64> {
        let ids = self.write(table_path, &[row], durability)?;
        Ok(ids.into_iter().next().unwrap_or(0))
    }

    // ========================================================================
    // Read Operations
    // ========================================================================

    /// Check if a record with given ID exists
    pub fn exists(&self, table_path: &Path, id: u64) -> io::Result<bool> {
        let backend = self.get_read_backend(table_path)?;
        Ok(backend.exists(id))
    }

    /// Get row count for a table
    pub fn row_count(&self, table_path: &Path) -> io::Result<u64> {
        let backend = self.get_read_backend(table_path)?;
        Ok(backend.row_count())
    }

    // ========================================================================
    // Delete Operations
    // ========================================================================

    /// Delete records matching a filter
    pub fn delete(
        &self,
        table_path: &Path,
        ids: &[u64],
        durability: DurabilityLevel,
    ) -> io::Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        let epoch_write = crate::storage::epoch::logical_write(table_path);

        // Invalidate before delete
        self.invalidate(table_path);

        let backend = self.get_write_backend(table_path, durability)?;

        let mut deleted = 0;
        for &id in ids {
            if backend.delete(id) {
                deleted += 1;
            }
        }

        if deleted > 0 {
            backend.save()?;
        }

        // Invalidate after delete
        self.invalidate(table_path);

        if deleted > 0 {
            epoch_write.commit();
        }
        Ok(deleted)
    }

    // ========================================================================
    // Schema Operations
    // ========================================================================

    /// Get schema for a table
    pub fn get_schema(
        &self,
        table_path: &Path,
    ) -> io::Result<Vec<(String, crate::data::DataType)>> {
        let backend = self.get_read_backend(table_path)?;
        Ok(backend.get_schema())
    }

    /// Create a new table, optionally with a pre-defined schema.
    /// Pre-defining schema avoids schema inference on the first insert.
    pub fn create_table(&self, table_path: &Path, durability: DurabilityLevel) -> io::Result<()> {
        let epoch_write = crate::storage::epoch::logical_write(table_path);
        let backend = Arc::new(TableStorageBackend::create_with_durability(
            table_path,
            durability,
        )?);
        if crate::storage::is_memory_path(table_path) {
            self.memory_tables
                .write()
                .insert(table_path.to_path_buf(), backend);
        }
        epoch_write.commit();
        Ok(())
    }

    /// Create a new table with a pre-defined schema.
    /// Columns and null vectors are pre-allocated with correct types so
    /// insert_typed() hits the fast path immediately on the first insert.
    pub fn create_table_with_schema(
        &self,
        table_path: &Path,
        durability: DurabilityLevel,
        schema_cols: &[(String, ColumnType)],
    ) -> io::Result<()> {
        let epoch_write = crate::storage::epoch::logical_write(table_path);
        let backend = Arc::new(TableStorageBackend::create_with_schema_and_durability(
            table_path,
            durability,
            schema_cols,
        )?);
        if crate::storage::is_memory_path(table_path) {
            self.memory_tables
                .write()
                .insert(table_path.to_path_buf(), backend);
        }
        epoch_write.commit();
        Ok(())
    }

    pub fn create_table_with_schema_object(
        &self,
        table_path: &Path,
        durability: DurabilityLevel,
        schema: crate::storage::OnDemandSchema,
    ) -> io::Result<()> {
        let backend = Arc::new(
            TableStorageBackend::create_with_schema_object_and_durability(
                table_path, durability, schema,
            )?,
        );
        self.memory_tables
            .write()
            .insert(table_path.to_path_buf(), backend);
        Ok(())
    }

    /// Delete a single record by ID
    pub fn delete_one(
        &self,
        table_path: &Path,
        id: u64,
        durability: DurabilityLevel,
    ) -> io::Result<bool> {
        let epoch_write = crate::storage::epoch::logical_write(table_path);
        self.invalidate(table_path);
        let backend = self.get_write_backend(table_path, durability)?;
        let result = backend.delete(id);
        if result {
            backend.save()?;
        }
        self.invalidate(table_path);
        if result {
            epoch_write.commit();
        }
        Ok(result)
    }

    /// Replace a record by ID
    pub fn replace(
        &self,
        table_path: &Path,
        id: u64,
        fields: &HashMap<String, Value>,
        durability: DurabilityLevel,
    ) -> io::Result<bool> {
        let epoch_write = crate::storage::epoch::logical_write(table_path);
        self.invalidate(table_path);
        let backend = self.get_write_backend(table_path, durability)?;
        let derivations = backend.storage.vector_derivations();
        let mut expanded_fields = None;
        if !derivations.is_empty() {
            let mut expanded = fields.clone();
            for derivation in &derivations {
                if fields.contains_key(&derivation.target) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "quantized vector column '{}' is system-maintained and cannot be written directly",
                            derivation.target
                        ),
                    ));
                }
                if let Some(Value::FixedList(bytes)) = fields.get(&derivation.source) {
                    expanded.insert(
                        derivation.target.clone(),
                        Value::FixedList(bytes.clone()),
                    );
                }
            }
            expanded_fields = Some(expanded);
        }
        let result = backend.replace(id, expanded_fields.as_ref().unwrap_or(fields))?;
        if result {
            backend.save()?;
        }
        self.invalidate(table_path);
        if result {
            epoch_write.commit();
        }
        Ok(result)
    }

    /// Get active row count (excluding deleted)
    pub fn active_row_count(&self, table_path: &Path) -> io::Result<u64> {
        let backend = self.get_read_backend(table_path)?;
        Ok(backend.active_row_count())
    }

    /// Fast path: Get base table row count only (no delta scan)
    /// Use this for COUNT(*) without WHERE clause - O(1) lock-free read
    pub fn base_row_count(&self, table_path: &Path) -> io::Result<u64> {
        let backend = self.get_read_backend(table_path)?;
        Ok(backend.base_row_count())
    }

    // ========================================================================
    // Schema Modification Operations
    // ========================================================================

    /// Add a column to the table
    pub fn add_column(
        &self,
        table_path: &Path,
        column_name: &str,
        dtype: crate::data::DataType,
        durability: DurabilityLevel,
    ) -> io::Result<()> {
        let epoch_write = crate::storage::epoch::logical_write(table_path);
        self.invalidate(table_path);
        if Self::is_v4_file(table_path) {
            // V4: schema-only DDL on the metadata insert backend. save() updates
            // the footer schema in place (no data load, no rewrite); reads
            // synthesize all-NULL values for rows already on disk.
            let backend = self.get_insert_backend(table_path, durability)?;
            if Self::has_delta_file(table_path) {
                // Materialize pending delta rows first so the schema change and
                // subsequent reads see one coherent column layout.
                backend.storage.compact()?;
            }
            backend.add_column(column_name, dtype)?;
            backend.save()?;
            self.invalidate(table_path);
            epoch_write.commit();
            return Ok(());
        }
        let backend = self.get_write_backend(table_path, durability)?;
        backend.add_column(column_name, dtype)?;
        backend.save()?;
        self.invalidate(table_path);
        epoch_write.commit();
        Ok(())
    }

    /// Add a system-maintained stored vector projection.
    ///
    /// Existing rows are backfilled Row Group by Row Group and published by an
    /// atomic file replacement; empty tables need only a footer schema update.
    pub fn add_quantized_vector_column(
        &self,
        table_path: &Path,
        source_column: &str,
        target_column: &str,
        target_dtype: crate::data::DataType,
        durability: DurabilityLevel,
    ) -> io::Result<()> {
        use crate::storage::on_demand::ColumnType;

        let target_type = ColumnType::from_data_type(target_dtype);
        if !matches!(
            target_type,
            ColumnType::Float16List
                | ColumnType::BFloat16List
                | ColumnType::Int8Vector
                | ColumnType::UInt8Vector
                | ColumnType::Bit1Vector
                | ColumnType::TurboQuant2Vector
                | ColumnType::TurboQuant3Vector
                | ColumnType::TurboQuant4Vector
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "quantized target must use Float16, BFloat16, Int8, UInt8, 1Bit, or TurboQuant 2/3/4-bit",
            ));
        }

        crate::storage::table_catalog::ensure_table_file(table_path, durability)?;
        let epoch_write = crate::storage::epoch::logical_write(table_path);
        self.invalidate(table_path);
        let backend = self.get_insert_backend(table_path, durability)?;
        let schema = backend.storage.get_schema();
        let source_type = schema
            .iter()
            .find(|(name, _)| name == source_column)
            .map(|(_, dtype)| *dtype)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("source vector column '{}' not found", source_column),
                )
            })?;
        if !matches!(
            source_type,
            ColumnType::FixedList | ColumnType::Float16List | ColumnType::BFloat16List
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "column '{}' is not a rescore-capable vector source",
                    source_column
                ),
            ));
        }
        if schema.iter().any(|(name, _)| name == target_column) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Column '{}' already exists", target_column),
            ));
        }
        if Self::has_delta_file(table_path) {
            backend.storage.compact()?;
        }
        let has_rows = backend.storage.row_count() != 0;
        let result = (|| -> io::Result<()> {
            backend.add_column(target_column, target_dtype)?;
            backend.storage.register_vector_derivation(
                source_column,
                target_column,
                crate::compute::vector_quantization::TURBOQUANT_CODEC_VERSION as u16,
            )?;
            if has_rows {
                backend.storage.rewrite_v4_drop_columns()?;
            } else {
                backend.save()?;
            }
            Ok(())
        })();
        self.invalidate(table_path);
        result?;
        epoch_write.commit();
        Ok(())
    }

    /// Drop a column from the table
    pub fn drop_column(
        &self,
        table_path: &Path,
        column_name: &str,
        durability: DurabilityLevel,
    ) -> io::Result<()> {
        let epoch_write = crate::storage::epoch::logical_write(table_path);
        self.invalidate(table_path);
        if Self::is_v4_file(table_path) {
            // V4: logical drop in schema, then a streaming RG-by-RG rewrite
            // that physically removes the column without loading the table.
            let backend = self.get_insert_backend(table_path, durability)?;
            if Self::has_delta_file(table_path) {
                // Materialize pending delta rows before the physical rewrite.
                backend.storage.compact()?;
            }
            backend.drop_column(column_name)?;
            backend.storage.rewrite_v4_drop_columns()?;
            self.invalidate(table_path);
            epoch_write.commit();
            return Ok(());
        }
        let backend = self.get_write_backend(table_path, durability)?;
        backend.drop_column(column_name)?;
        backend.save()?;
        self.invalidate(table_path);
        epoch_write.commit();
        Ok(())
    }

    /// Drop only a registered system-maintained vector accelerator.
    pub fn drop_quantized_vector_column(
        &self,
        table_path: &Path,
        target_column: &str,
        durability: DurabilityLevel,
    ) -> io::Result<()> {
        let is_target = self
            .get_read_backend(table_path)?
            .storage
            .vector_derivations()
            .iter()
            .any(|derivation| derivation.target == target_column);
        if !is_target {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "column '{}' is not a registered quantized accelerator",
                    target_column
                ),
            ));
        }
        self.drop_column(table_path, target_column, durability)
    }

    /// Rename a column
    pub fn rename_column(
        &self,
        table_path: &Path,
        old_name: &str,
        new_name: &str,
        durability: DurabilityLevel,
    ) -> io::Result<()> {
        let epoch_write = crate::storage::epoch::logical_write(table_path);
        self.invalidate(table_path);
        if Self::is_v4_file(table_path) {
            // V4: modify schema in-memory then update footer only (no data reload)
            let backend = self.get_insert_backend(table_path, durability)?;
            backend.rename_column(old_name, new_name)?;
            backend.storage.update_v4_footer_schema()?;
            self.invalidate(table_path);
            epoch_write.commit();
            return Ok(());
        }
        let backend = self.get_write_backend(table_path, durability)?;
        backend.rename_column(old_name, new_name)?;
        backend.save()?;
        self.invalidate(table_path);
        epoch_write.commit();
        Ok(())
    }

    /// List all columns
    pub fn list_columns(&self, table_path: &Path) -> io::Result<Vec<String>> {
        let backend = self.get_read_backend(table_path)?;
        Ok(backend.list_columns())
    }

    /// Get column type
    pub fn get_column_type(
        &self,
        table_path: &Path,
        column_name: &str,
    ) -> io::Result<Option<crate::data::DataType>> {
        let backend = self.get_read_backend(table_path)?;
        Ok(backend.get_column_type(column_name))
    }

    /// Write typed columns (for store_columnar)
    /// OPTIMIZED: Uses V4 append_row_group for small inserts into existing tables
    /// to avoid rewriting the entire file (50x+ speedup for incremental inserts)
    pub fn write_typed(
        &self,
        table_path: &Path,
        mut int_columns: HashMap<String, Vec<i64>>,
        mut float_columns: HashMap<String, Vec<f64>>,
        string_columns: HashMap<String, Vec<String>>,
        mut binary_columns: HashMap<String, Vec<Vec<u8>>>,
        fixedlist_columns: HashMap<String, Vec<Vec<u8>>>,
        bool_columns: HashMap<String, Vec<bool>>,
        null_positions: HashMap<String, Vec<bool>>,
        durability: DurabilityLevel,
    ) -> io::Result<Vec<u64>> {
        use crate::storage::on_demand::{ColumnData, ColumnType};
        let epoch_write = crate::storage::epoch::logical_write(table_path);

        // Determine row count from first non-empty column
        let row_count = int_columns
            .values()
            .next()
            .map(|v| v.len())
            .or_else(|| float_columns.values().next().map(|v| v.len()))
            .or_else(|| string_columns.values().next().map(|v| v.len()))
            .or_else(|| bool_columns.values().next().map(|v| v.len()))
            .or_else(|| binary_columns.values().next().map(|v| v.len()))
            .or_else(|| fixedlist_columns.values().next().map(|v| v.len()))
            .unwrap_or(0);
        if row_count == 0 {
            return Ok(Vec::new());
        }

        // FAST PATH: V4 append for existing tables with matching schema
        // Check if file exists, is V4, and schema matches
        if row_count > 0 && self.table_exists(table_path) {
            if let Ok(meta) = std::fs::metadata(table_path) {
                if meta.len() >= 256 {
                    // Reuse the insert backend so repeated small appends avoid reopening
                    // and reparsing the V4 footer on every call.
                    if let Ok(backend) = self.get_insert_backend(table_path, durability) {
                        let storage = &backend.storage;
                        let schema = storage.get_schema();
                        let header = storage.header_info();
                        let is_v4 = header.0 > 0; // footer_offset > 0 means V4

                        // A schema-only V4 file may contain an empty bootstrap row group.
                        // The first write must go through save_v4() so that bootstrap metadata
                        // is replaced instead of retained ahead of appended data row groups.
                        if is_v4 && !schema.is_empty() && storage.row_count() > 0 {
                            // Check column match
                            let schema_cols: std::collections::HashSet<String> =
                                schema.iter().map(|(name, _)| name.clone()).collect();
                            let mut data_cols = std::collections::HashSet::new();
                            for k in int_columns.keys() {
                                data_cols.insert(k.clone());
                            }
                            for k in float_columns.keys() {
                                data_cols.insert(k.clone());
                            }
                            for k in string_columns.keys() {
                                data_cols.insert(k.clone());
                            }
                            for k in bool_columns.keys() {
                                data_cols.insert(k.clone());
                            }
                            for k in binary_columns.keys() {
                                data_cols.insert(k.clone());
                            }
                            for k in fixedlist_columns.keys() {
                                data_cols.insert(k.clone());
                            }

                            if schema_cols == data_cols {
                                // Build ColumnData + null bitmaps in schema order
                                let mut new_columns: Vec<ColumnData> =
                                    Vec::with_capacity(schema.len());
                                let mut new_nulls: Vec<Vec<u8>> = Vec::with_capacity(schema.len());
                                let mut moved_int_columns = Vec::new();
                                let mut moved_float_columns = Vec::new();

                                // Allocate IDs
                                let start_id = storage.next_id_value();
                                let ids: Vec<u64> =
                                    (start_id..start_id + row_count as u64).collect();

                                for (schema_idx, (col_name, col_type)) in schema.iter().enumerate()
                                {
                                    // Build null bitmap
                                    let null_bitmap =
                                        if let Some(null_vec) = null_positions.get(col_name) {
                                            let mut bitmap = vec![0u8; (row_count + 7) / 8];
                                            for (i, &is_null) in null_vec.iter().enumerate() {
                                                if is_null {
                                                    bitmap[i / 8] |= 1 << (i % 8);
                                                }
                                            }
                                            if bitmap.iter().any(|&b| b != 0) {
                                                bitmap
                                            } else {
                                                Vec::new()
                                            }
                                        } else {
                                            Vec::new()
                                        };
                                    new_nulls.push(null_bitmap);

                                    match col_type {
                                        ColumnType::Int64
                                        | ColumnType::Int32
                                        | ColumnType::Int16
                                        | ColumnType::Int8
                                        | ColumnType::UInt8
                                        | ColumnType::UInt16
                                        | ColumnType::UInt32
                                        | ColumnType::UInt64
                                        | ColumnType::Timestamp
                                        | ColumnType::Date => {
                                            let vals =
                                                if let Some(vals) = int_columns.remove(col_name) {
                                                    moved_int_columns.push(schema_idx);
                                                    vals
                                                } else {
                                                    vec![0; row_count]
                                                };
                                            new_columns.push(ColumnData::Int64(vals));
                                        }
                                        ColumnType::Float64 | ColumnType::Float32 => {
                                            let vals = if let Some(vals) =
                                                float_columns.remove(col_name)
                                            {
                                                moved_float_columns.push(schema_idx);
                                                vals
                                            } else {
                                                vec![0.0; row_count]
                                            };
                                            new_columns.push(ColumnData::Float64(vals));
                                        }
                                        ColumnType::String
                                        | ColumnType::StringDict
                                        | ColumnType::Null => {
                                            if let Some(vals) = string_columns.get(col_name) {
                                                let mut offsets =
                                                    Vec::with_capacity(vals.len() + 1);
                                                let mut data = Vec::new();
                                                   offsets.push(0u64);
                                                for s in vals {
                                                    data.extend_from_slice(s.as_bytes());
                                                    offsets.push(data.len() as u64);
                                                }
                                                new_columns
                                                    .push(ColumnData::String { offsets, data });
                                            } else {
                                                let offsets = vec![0u64; row_count + 1];
                                                new_columns.push(ColumnData::String {
                                                    offsets,
                                                    data: Vec::new(),
                                                });
                                            }
                                        }
                                        ColumnType::Bool => {
                                            if let Some(vals) = bool_columns.get(col_name) {
                                                let byte_count = (vals.len() + 7) / 8;
                                                let mut packed = vec![0u8; byte_count];
                                                for (i, &v) in vals.iter().enumerate() {
                                                    if v {
                                                        packed[i / 8] |= 1 << (i % 8);
                                                    }
                                                }
                                                new_columns.push(ColumnData::Bool {
                                                    data: packed,
                                                    len: vals.len(),
                                                });
                                            } else {
                                                new_columns.push(ColumnData::Bool {
                                                    data: vec![0u8; (row_count + 7) / 8],
                                                    len: row_count,
                                                });
                                            }
                                        }
                                        ColumnType::Binary | ColumnType::Blob => {
                                            if let Some(vals) = binary_columns.get(col_name) {
                                                let mut offsets =
                                                    Vec::with_capacity(vals.len() + 1);
                                                let mut data = Vec::new();
                                                offsets.push(0u64);
                                                if *col_type == ColumnType::Blob {
                                                    for desc in storage.write_blob_values(vals)? {
                                                        data.extend_from_slice(&desc);
                                                        offsets.push(data.len() as u64);
                                                    }
                                                } else {
                                                    for b in vals {
                                                        data.extend_from_slice(b);
                                                        offsets.push(data.len() as u64);
                                                    }
                                                }
                                                new_columns
                                                    .push(ColumnData::Binary { offsets, data });
                                            } else {
                                                let offsets = vec![0u64; row_count + 1];
                                                new_columns.push(ColumnData::Binary {
                                                    offsets,
                                                    data: Vec::new(),
                                                });
                                            }
                                        }
                                        ColumnType::FixedList => {
                                            if let Some(vals) = fixedlist_columns.get(col_name) {
                                                let dim = vals
                                                    .iter()
                                                    .find(|b| !b.is_empty())
                                                    .map(|b| b.len() / 4)
                                                    .unwrap_or(0)
                                                    as u32;
                                                let mut data: Vec<u8> = Vec::with_capacity(
                                                    row_count * dim as usize * 4,
                                                );
                                                for b in vals {
                                                    data.extend_from_slice(b);
                                                }
                                                new_columns
                                                    .push(ColumnData::FixedList { data, dim });
                                            } else {
                                                new_columns.push(ColumnData::FixedList {
                                                    data: Vec::new(),
                                                    dim: 0,
                                                });
                                            }
                                        }
                                        ColumnType::Float16List => {
                                            if let Some(vals) = fixedlist_columns.get(col_name) {
                                                let dim = vals
                                                    .iter()
                                                    .find(|b| !b.is_empty())
                                                    .map(|b| b.len() / 4)
                                                    .unwrap_or(0)
                                                    as u32;
                                                let mut data: Vec<u8> = Vec::with_capacity(
                                                    row_count * dim as usize * 2,
                                                );
                                                for b in vals {
                                                    for chunk in b.chunks_exact(4) {
                                                        let f = f32::from_le_bytes(
                                                            chunk.try_into().unwrap(),
                                                        );
                                                        let h =
                                                            crate::storage::on_demand::f32_to_f16(
                                                                f,
                                                            );
                                                        data.extend_from_slice(&h.to_le_bytes());
                                                    }
                                                }
                                                new_columns
                                                    .push(ColumnData::Float16List { data, dim });
                                            } else {
                                                new_columns.push(ColumnData::Float16List {
                                                    data: Vec::new(),
                                                    dim: 0,
                                                });
                                            }
                                        }
                                        dtype @ (ColumnType::BFloat16List
                                        | ColumnType::Int8Vector
                                        | ColumnType::UInt8Vector
                                        | ColumnType::Bit1Vector
                                        | ColumnType::TurboQuant2Vector
                                        | ColumnType::TurboQuant3Vector
                                        | ColumnType::TurboQuant4Vector) => {
                                            let codec = dtype.vector_codec().unwrap();
                                            if let Some(vals) = fixedlist_columns.get(col_name) {
                                                let dim = vals.iter().find(|row| !row.is_empty())
                                                    .map(|row| row.len() / 4).unwrap_or(0) as u32;
                                                let mut data = Vec::with_capacity(
                                                    row_count * codec.row_width(dim as usize)?,
                                                );
                                                for row in vals {
                                                    let values = row.chunks_exact(4).map(|bytes| {
                                                        f32::from_le_bytes(bytes.try_into().unwrap())
                                                    }).collect::<Vec<_>>();
                                                    crate::compute::vector_quantization::encode_vector(
                                                        codec, &values, &mut data,
                                                    )?;
                                                }
                                                new_columns.push(ColumnData::QuantizedList { data, dim, codec });
                                            } else {
                                                new_columns.push(ColumnData::QuantizedList { data: Vec::new(), dim: 0, codec });
                                            }
                                        }
                                    }
                                }

                                // Append row group and return
                                match storage.append_row_group(&ids, &new_columns, &new_nulls) {
                                    Ok(()) => {
                                        self.invalidate_after_append(table_path);
                                        epoch_write.commit();
                                        drop(epoch_write);
                                        if let Some(entry) =
                                            self.insert_cache.write().get_mut(table_path)
                                        {
                                            entry.epoch =
                                                crate::storage::epoch::current(table_path);
                                        }
                                        return Ok(ids);
                                    }
                                    Err(_) => {
                                        // Recover moved inputs before falling through to the
                                        // full-write path. Append failures are rare, so keep
                                        // recovery off the successful hot path.
                                        for schema_idx in moved_int_columns {
                                            if let ColumnData::Int64(values) =
                                                &mut new_columns[schema_idx]
                                            {
                                                int_columns.insert(
                                                    schema[schema_idx].0.clone(),
                                                    std::mem::take(values),
                                                );
                                            }
                                        }
                                        for schema_idx in moved_float_columns {
                                            if let ColumnData::Float64(values) =
                                                &mut new_columns[schema_idx]
                                            {
                                                float_columns.insert(
                                                    schema[schema_idx].0.clone(),
                                                    std::mem::take(values),
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // SLOW PATH: Full write (for new tables, schema changes, etc.)
        self.invalidate(table_path);
        let backend = self.get_write_backend(table_path, durability)?;
        let mut blob_columns: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        for (name, col_type) in backend.storage.get_schema() {
            if col_type == ColumnType::Blob {
                if let Some(values) = binary_columns.remove(&name) {
                    blob_columns.insert(name, backend.storage.write_blob_values(&values)?);
                }
            }
        }
        let ids = backend.insert_typed_with_nulls_full_with_blobs(
            int_columns,
            float_columns,
            string_columns,
            binary_columns,
            fixedlist_columns,
            blob_columns,
            bool_columns,
            null_positions,
        )?;
        backend.save()?;
        self.invalidate(table_path);
        epoch_write.commit();
        drop(epoch_write);
        if let Some(entry) = self.insert_cache.write().get_mut(table_path) {
            entry.epoch = crate::storage::epoch::current(table_path);
        }
        Ok(ids)
    }

    /// Write pre-built columnar data (for store_columnar).
    ///
    /// Same semantics as `write_typed`, but accepts columns that are already
    /// materialized as `ColumnData` so the Python binding can build them
    /// directly from borrowed `&str` / `&[u8]` buffers. This avoids the
    /// per-element `Vec<String>` / `Vec<Vec<u8>>` intermediate layer and the
    /// second copy into the Row Group buffers — peak memory for a string-heavy
    /// batch drops from ~2× the data to ~1×.
    pub fn write_typed_columns(
        &self,
        table_path: &Path,
        mut columns: HashMap<String, crate::storage::on_demand::ColumnData>,
        mut null_positions: HashMap<String, Vec<bool>>,
        durability: DurabilityLevel,
    ) -> io::Result<Vec<u64>> {
        use crate::storage::on_demand::{ColumnData, ColumnType};
        let epoch_write = crate::storage::epoch::logical_write(table_path);

        let row_count = columns.values().map(|c| c.len()).max().unwrap_or(0);
        if row_count == 0 {
            return Ok(Vec::new());
        }

        if self.table_exists(table_path) {
            let backend = self.get_insert_backend(table_path, durability)?;
            let derivations = backend.storage.vector_derivations();
            if !derivations.is_empty() {
                let schema = backend.storage.get_schema();
                materialize_vector_derivations(
                    &mut columns,
                    &mut null_positions,
                    &schema,
                    &derivations,
                )?;
            }
        }

        // FAST PATH: V4 append for existing tables with matching schema
        if table_path.exists() {
            if let Ok(meta) = std::fs::metadata(table_path) {
                if meta.len() >= 256 {
                    if let Ok(backend) = self.get_insert_backend(table_path, durability) {
                        let storage = &backend.storage;
                        let schema = storage.get_schema();
                        let header = storage.header_info();
                        let is_v4 = header.0 > 0;

                        if is_v4 && !schema.is_empty() && storage.row_count() > 0 {
                            let schema_cols: std::collections::HashSet<String> =
                                schema.iter().map(|(name, _)| name.clone()).collect();
                            let data_cols: std::collections::HashSet<String> =
                                columns.keys().cloned().collect();

                            if schema_cols == data_cols {
                                // Assemble ColumnData in schema order. A column is
                                // accepted only when its variant matches the schema
                                // type (same rule as write_typed's typed buckets);
                                // otherwise it is padded with the type default.
                                let mut new_columns: Vec<ColumnData> =
                                    Vec::with_capacity(schema.len());
                                let mut new_nulls: Vec<Vec<u8>> =
                                    Vec::with_capacity(schema.len());

                                let start_id = storage.next_id_value();
                                let ids: Vec<u64> =
                                    (start_id..start_id + row_count as u64).collect();

                                for (col_name, col_type) in schema.iter() {
                                    let null_bitmap =
                                        if let Some(null_vec) = null_positions.get(col_name) {
                                            let mut bitmap = vec![0u8; (row_count + 7) / 8];
                                            for (i, &is_null) in null_vec.iter().enumerate() {
                                                if is_null {
                                                    bitmap[i / 8] |= 1 << (i % 8);
                                                }
                                            }
                                            if bitmap.iter().any(|&b| b != 0) {
                                                bitmap
                                            } else {
                                                Vec::new()
                                            }
                                        } else {
                                            Vec::new()
                                        };
                                    new_nulls.push(null_bitmap);

                                    let col = columns.remove(col_name);
                                    match col_type {
                                        ColumnType::Int64
                                        | ColumnType::Int32
                                        | ColumnType::Int16
                                        | ColumnType::Int8
                                        | ColumnType::UInt8
                                        | ColumnType::UInt16
                                        | ColumnType::UInt32
                                        | ColumnType::UInt64
                                        | ColumnType::Timestamp
                                        | ColumnType::Date => {
                                            let vals = match col {
                                                Some(ColumnData::Int64(v)) => v,
                                                _ => vec![0; row_count],
                                            };
                                            new_columns.push(ColumnData::Int64(vals));
                                        }
                                        ColumnType::Float64 | ColumnType::Float32 => {
                                            let vals = match col {
                                                Some(ColumnData::Float64(v)) => v,
                                                _ => vec![0.0; row_count],
                                            };
                                            new_columns.push(ColumnData::Float64(vals));
                                        }
                                        ColumnType::String
                                        | ColumnType::StringDict
                                        | ColumnType::Null => match col {
                                            Some(ColumnData::String { offsets, data }) => {
                                                new_columns.push(ColumnData::String {
                                                    offsets,
                                                    data,
                                                });
                                            }
                                            _ => {
                                                new_columns.push(ColumnData::String {
                                                    offsets: vec![0u64; row_count + 1],
                                                    data: Vec::new(),
                                                });
                                            }
                                        },
                                        ColumnType::Bool => {
                                            let packed = match col {
                                                Some(ColumnData::Bool { data, len }) => {
                                                    debug_assert_eq!(len, row_count);
                                                    data
                                                }
                                                _ => vec![0u8; (row_count + 7) / 8],
                                            };
                                            new_columns.push(ColumnData::Bool {
                                                data: packed,
                                                len: row_count,
                                            });
                                        }
                                        ColumnType::Binary | ColumnType::Blob => match col {
                                            Some(ColumnData::Binary { offsets, data }) => {
                                                if *col_type == ColumnType::Blob {
                                                    // Blob schema: values are raw bytes in the
                                                    // binary column; write sidecar descriptors.
                                                    let values = binary_column_to_values(
                                                        &offsets,
                                                        &data,
                                                        row_count,
                                                    );
                                                    let descriptors =
                                                        storage.write_blob_values(&values)?;
                                                    let mut desc_offsets =
                                                        Vec::with_capacity(descriptors.len() + 1);
                                                    let mut desc_data = Vec::new();
                                                    desc_offsets.push(0u64);
                                                    for d in &descriptors {
                                                        desc_data.extend_from_slice(d);
                                                        desc_offsets.push(desc_data.len() as u64);
                                                    }
                                                    new_columns.push(ColumnData::Binary {
                                                        offsets: desc_offsets,
                                                        data: desc_data,
                                                    });
                                                } else {
                                                    new_columns.push(ColumnData::Binary {
                                                        offsets,
                                                        data,
                                                    });
                                                }
                                            }
                                            _ => {
                                                new_columns.push(ColumnData::Binary {
                                                    offsets: vec![0u64; row_count + 1],
                                                    data: Vec::new(),
                                                });
                                            }
                                        },
                                        ColumnType::FixedList => {
                                            let col = match col {
                                                Some(ColumnData::FixedList { data, dim }) => {
                                                    ColumnData::FixedList { data, dim }
                                                }
                                                _ => ColumnData::FixedList {
                                                    data: Vec::new(),
                                                    dim: 0,
                                                },
                                            };
                                            new_columns.push(col);
                                        }
                                        ColumnType::Float16List => {
                                            let col = match col {
                                                Some(ColumnData::Float16List { data, dim }) => {
                                                    ColumnData::Float16List { data, dim }
                                                }
                                                // FixedList columns carry f32 bytes (the
                                                // Python binding's numpy path); convert to
                                                // f16 exactly like write_typed's typed path.
                                                Some(ColumnData::FixedList { data, dim }) => {
                                                    let mut f16_data =
                                                        Vec::with_capacity(data.len() / 2);
                                                    for chunk in data.chunks_exact(4) {
                                                        let f = f32::from_le_bytes(
                                                            chunk.try_into().unwrap(),
                                                        );
                                                        f16_data.extend_from_slice(
                                                            &crate::storage::on_demand::f32_to_f16(
                                                                f,
                                                            )
                                                            .to_le_bytes(),
                                                        );
                                                    }
                                                    ColumnData::Float16List {
                                                        data: f16_data,
                                                        dim,
                                                    }
                                                }
                                                _ => ColumnData::Float16List {
                                                    data: Vec::new(),
                                                    dim: 0,
                                                },
                                            };
                                            new_columns.push(col);
                                        }
                                        dtype @ (ColumnType::BFloat16List
                                        | ColumnType::Int8Vector
                                        | ColumnType::UInt8Vector
                                        | ColumnType::Bit1Vector
                                        | ColumnType::TurboQuant2Vector
                                        | ColumnType::TurboQuant3Vector
                                        | ColumnType::TurboQuant4Vector) => {
                                            let codec = dtype.vector_codec().unwrap();
                                            let encoded = match col {
                                                Some(ColumnData::QuantizedList { data, dim, codec: actual })
                                                    if actual == codec => ColumnData::QuantizedList { data, dim, codec },
                                                Some(ColumnData::FixedList { data, dim }) => {
                                                    let mut encoded = Vec::new();
                                                    for row in data.chunks_exact(dim as usize * 4) {
                                                        let values = row.chunks_exact(4).map(|bytes| {
                                                            f32::from_le_bytes(bytes.try_into().unwrap())
                                                        }).collect::<Vec<_>>();
                                                        crate::compute::vector_quantization::encode_vector(codec, &values, &mut encoded)?;
                                                    }
                                                    ColumnData::QuantizedList { data: encoded, dim, codec }
                                                }
                                                _ => ColumnData::QuantizedList { data: Vec::new(), dim: 0, codec },
                                            };
                                            new_columns.push(encoded);
                                        }
                                    }
                                }

                                match storage.append_row_group(&ids, &new_columns, &new_nulls) {
                                    Ok(()) => {
                                        self.invalidate_after_append(table_path);
                                        epoch_write.commit();
                                        drop(epoch_write);
                                        if let Some(entry) =
                                            self.insert_cache.write().get_mut(table_path)
                                        {
                                            entry.epoch =
                                                crate::storage::epoch::current(table_path);
                                        }
                                        return Ok(ids);
                                    }
                                    Err(_) => {
                                        return Err(io::Error::new(
                                            io::ErrorKind::Other,
                                            "append_row_group failed",
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // SLOW PATH: convert pre-built columns back to the typed maps and let
        // write_typed handle new tables / schema evolution / non-V4 files.
        let mut int_columns: HashMap<String, Vec<i64>> = HashMap::new();
        let mut float_columns: HashMap<String, Vec<f64>> = HashMap::new();
        let mut string_columns: HashMap<String, Vec<String>> = HashMap::new();
        let mut binary_columns: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut fixedlist_columns: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut bool_columns: HashMap<String, Vec<bool>> = HashMap::new();
        for (name, col) in columns {
            match col {
                ColumnData::Int64(v) => {
                    int_columns.insert(name, v);
                }
                ColumnData::Float64(v) => {
                    float_columns.insert(name, v);
                }
                ColumnData::String { offsets, data } => {
                    let mut vals = Vec::with_capacity(offsets.len().saturating_sub(1));
                    for i in 0..offsets.len().saturating_sub(1) {
                        let start = offsets[i] as usize;
                        let end = offsets[i + 1] as usize;
                        vals.push(String::from_utf8_lossy(&data[start..end]).to_string());
                    }
                    string_columns.insert(name, vals);
                }
                ColumnData::StringDict {
                    indices,
                    dict_offsets,
                    dict_data,
                } => {
                    let mut vals = Vec::with_capacity(indices.len());
                    for &idx in &indices {
                        if idx == 0 || (idx as usize) >= dict_offsets.len() {
                            vals.push(String::new());
                        } else {
                            let start = dict_offsets[(idx - 1) as usize] as usize;
                            let end = dict_offsets[idx as usize] as usize;
                            vals.push(
                                String::from_utf8_lossy(&dict_data[start..end]).to_string(),
                            );
                        }
                    }
                    string_columns.insert(name, vals);
                }
                ColumnData::Binary { offsets, data } => {
                    let mut vals = Vec::with_capacity(offsets.len().saturating_sub(1));
                    for i in 0..offsets.len().saturating_sub(1) {
                        let start = offsets[i] as usize;
                        let end = offsets[i + 1] as usize;
                        vals.push(data[start..end].to_vec());
                    }
                    binary_columns.insert(name, vals);
                }
                ColumnData::Bool { data, len } => {
                    let mut vals = Vec::with_capacity(len);
                    for i in 0..len {
                        vals.push((data[i / 8] >> (i % 8)) & 1 == 1);
                    }
                    bool_columns.insert(name, vals);
                }
                ColumnData::FixedList { data, dim } => {
                    let row_bytes = dim as usize * 4;
                    let mut vals = Vec::with_capacity(if row_bytes == 0 {
                        0
                    } else {
                        data.len() / row_bytes
                    });
                    for chunk in data.chunks(row_bytes.max(1)) {
                        vals.push(chunk.to_vec());
                    }
                    fixedlist_columns.insert(name, vals);
                }
                ColumnData::Float16List { data, dim } => {
                    // Convert f16 bytes back to f32 bytes for the typed path.
                    let row_bytes = dim as usize * 2;
                    let mut vals = Vec::with_capacity(if row_bytes == 0 {
                        0
                    } else {
                        data.len() / row_bytes
                    });
                    for chunk in data.chunks(row_bytes.max(1)) {
                        let mut f32_row = Vec::with_capacity(chunk.len() * 2);
                        for pair in chunk.chunks_exact(2) {
                            let h = u16::from_le_bytes(pair.try_into().unwrap());
                            f32_row.extend_from_slice(
                                &crate::storage::on_demand::f16_to_f32(h).to_le_bytes(),
                            );
                        }
                        vals.push(f32_row);
                    }
                    fixedlist_columns.insert(name, vals);
                }
                ColumnData::QuantizedList { data, dim, codec } => {
                    let stride = codec.row_width(dim as usize)?;
                    let mut vals = Vec::with_capacity(if stride == 0 { 0 } else { data.len() / stride });
                    let mut decoded = vec![0.0f32; dim as usize];
                    for row in data.chunks_exact(stride) {
                        crate::compute::vector_quantization::decode_vector(codec, row, dim as usize, &mut decoded)?;
                        let mut bytes = Vec::with_capacity(dim as usize * 4);
                        for &value in &decoded { bytes.extend_from_slice(&value.to_le_bytes()); }
                        vals.push(bytes);
                    }
                    fixedlist_columns.insert(name, vals);
                }
            }
        }

        self.write_typed(
            table_path,
            int_columns,
            float_columns,
            string_columns,
            binary_columns,
            fixedlist_columns,
            bool_columns,
            null_positions,
            durability,
        )
    }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Get the global storage engine
pub fn engine() -> &'static StorageEngine {
    StorageEngine::global()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn write_typed_columns_slow_then_fast_path() {
        use crate::storage::on_demand::{ColumnData, ColumnType};

        let dir = tempdir().unwrap();
        let table_path = dir.path().join("typed_cols.apex");
        let engine = engine();

        // First write: new table -> slow path (schema inferred from data).
        let mut cols: HashMap<String, ColumnData> = HashMap::new();
        cols.insert(
            "name".to_string(),
            ColumnData::String {
                offsets: vec![0u64, 3, 6],
                data: b"abcdef".to_vec(),
            },
        );
        cols.insert("score".to_string(), ColumnData::Int64(vec![1, 2]));
        let ids = engine
            .write_typed_columns(&table_path, cols, HashMap::new(), DurabilityLevel::Fast)
            .unwrap();
        assert_eq!(ids, vec![1, 2]);

        // Second write: V4 append fast path with matching schema.
        let mut cols2: HashMap<String, ColumnData> = HashMap::new();
        cols2.insert(
            "name".to_string(),
            ColumnData::String {
                offsets: vec![0u64, 2, 4],
                data: b"xyzw".to_vec(),
            },
        );
        cols2.insert("score".to_string(), ColumnData::Int64(vec![3, 4]));
        let ids2 = engine
            .write_typed_columns(&table_path, cols2, HashMap::new(), DurabilityLevel::Fast)
            .unwrap();
        assert_eq!(ids2, vec![3, 4]);

        // Verify both row groups are readable with correct values.
        let backend = engine.get_read_backend(&table_path).unwrap();
        assert_eq!(backend.row_count(), 4);
        let batch = backend
            .read_columns_to_arrow(Some(&["name", "score"]), 0, None)
            .unwrap();
        assert_eq!(batch.num_rows(), 4);
        use arrow::array::Array;
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "abc");
        assert_eq!(names.value(1), "def");
        assert_eq!(names.value(2), "xy");
        assert_eq!(names.value(3), "zw");
        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(scores.values(), &[1, 2, 3, 4]);
    }

    #[test]
    fn write_typed_columns_converts_fixedlist_to_float16_schema() {
        use crate::storage::on_demand::{ColumnData, ColumnType};

        let dir = tempdir().unwrap();
        let table_path = dir.path().join("f16_cols.apex");
        let engine = engine();
        engine
            .create_table_with_schema(
                &table_path,
                DurabilityLevel::Fast,
                &[("vec".to_string(), ColumnType::Float16List)],
            )
            .unwrap();

        let f32_row = |a: f32, b: f32, c: f32, d: f32| -> Vec<u8> {
            let mut out = Vec::with_capacity(16);
            for v in [a, b, c, d] {
                out.extend_from_slice(&v.to_le_bytes());
            }
            out
        };

        // First write: empty table -> slow path (FixedList -> typed -> f16).
        let mut cols: HashMap<String, ColumnData> = HashMap::new();
        cols.insert(
            "vec".to_string(),
            ColumnData::FixedList {
                data: [
                    f32_row(1.0, 0.0, 0.0, 0.0),
                    f32_row(0.0, 1.0, 0.0, 0.0),
                ]
                .concat(),
                dim: 4,
            },
        );
        engine
            .write_typed_columns(&table_path, cols, HashMap::new(), DurabilityLevel::Fast)
            .unwrap();

        // Second write: V4 append fast path must convert FixedList -> Float16List.
        let mut cols2: HashMap<String, ColumnData> = HashMap::new();
        cols2.insert(
            "vec".to_string(),
            ColumnData::FixedList {
                data: [
                    f32_row(0.0, 0.0, 1.0, 0.0),
                    f32_row(0.0, 0.0, 0.0, 1.0),
                ]
                .concat(),
                dim: 4,
            },
        );
        engine
            .write_typed_columns(&table_path, cols2, HashMap::new(), DurabilityLevel::Fast)
            .unwrap();

        let backend = engine.get_read_backend(&table_path).unwrap();
        let batch = backend
            .read_columns_to_arrow(Some(&["vec"]), 0, None)
            .unwrap();
        assert_eq!(batch.num_rows(), 4);
        let vec_col = batch.column_by_name("vec").unwrap();
        assert_eq!(vec_col.len(), 4);
    }

    #[test]
    fn test_engine_write_read() {
        let dir = tempdir().unwrap();
        let table_path = dir.path().join("test.apex");

        let engine = StorageEngine::global();

        // Write some rows
        let mut row1 = HashMap::new();
        row1.insert("name".to_string(), Value::String("Alice".to_string()));
        row1.insert("age".to_string(), Value::Int64(30));

        let mut row2 = HashMap::new();
        row2.insert("name".to_string(), Value::String("Bob".to_string()));
        row2.insert("age".to_string(), Value::Int64(25));

        let ids = engine
            .write(&table_path, &[row1, row2], DurabilityLevel::Fast)
            .unwrap();
        assert_eq!(ids.len(), 2);

        // Check row count
        let count = engine.row_count(&table_path).unwrap();
        assert_eq!(count, 2);

        // Check exists — use actual returned IDs (not hardcoded 1/2)
        // because StorageEngine::global() is shared and next_id may not start at 1
        assert!(engine.exists(&table_path, ids[0]).unwrap());
        assert!(engine.exists(&table_path, ids[1]).unwrap());
        assert!(!engine.exists(&table_path, 999).unwrap());
    }

    #[test]
    fn memory_table_keeps_rows_without_creating_files() {
        let table_path = PathBuf::from("apexbase_memory:test/rows.apex");
        let engine = StorageEngine::global();
        engine.drop_memory_database(Path::new("apexbase_memory:test"));
        engine
            .create_table_with_schema(
                &table_path,
                DurabilityLevel::Fast,
                &[("value".to_string(), ColumnType::Int64)],
            )
            .unwrap();

        let rows = [HashMap::from([("value".to_string(), Value::Int64(42))])];
        let ids = engine
            .write(&table_path, &rows, DurabilityLevel::Fast)
            .unwrap();

        assert_eq!(ids, vec![crate::storage::FIRST_ROW_ID]);
        assert_eq!(engine.row_count(&table_path).unwrap(), 1);
        assert!(!table_path.exists());
        engine.drop_memory_database(Path::new("apexbase_memory:test"));
    }

    #[test]
    fn memory_table_runs_sql_without_files() {
        let base_dir = PathBuf::from("apexbase_memory:sql");
        let default_path = base_dir.join("apexbase.apex");
        let table_path = base_dir.join("items.apex");
        let engine = StorageEngine::global();
        engine.drop_memory_database(&base_dir);

        crate::Database::execute(
            "CREATE TABLE items (value BIGINT, label TEXT)",
            &base_dir,
            &default_path,
        )
        .unwrap();
        crate::Database::execute(
            "INSERT INTO items (value, label) VALUES (7, 'seven')",
            &base_dir,
            &default_path,
        )
        .unwrap();
        let result = crate::Database::execute(
            "SELECT value, label FROM items",
            &base_dir,
            &default_path,
        )
        .unwrap()
        .to_record_batch()
        .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert!(!table_path.exists());
        engine.drop_memory_database(&base_dir);
    }

    #[test]
    fn test_typed_write_replaces_schema_only_bootstrap_before_append() {
        use arrow::array::{Int64Array, StringArray};

        let dir = tempdir().unwrap();
        let table_path = dir.path().join("streamed.apex");
        let engine = StorageEngine::global();
        engine
            .create_table_with_schema(
                &table_path,
                DurabilityLevel::Fast,
                &[
                    ("value".to_string(), ColumnType::Int64),
                    ("category".to_string(), ColumnType::String),
                ],
            )
            .unwrap();

        for (values, categories) in [
            (vec![1, 2, 3], vec!["a", "b", "c"]),
            (vec![4, 5], vec!["d", "e"]),
        ] {
            engine
                .write_typed(
                    &table_path,
                    HashMap::from([("value".to_string(), values)]),
                    HashMap::new(),
                    HashMap::from([(
                        "category".to_string(),
                        categories.into_iter().map(str::to_string).collect(),
                    )]),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    DurabilityLevel::Fast,
                )
                .unwrap();
        }

        let backend = TableStorageBackend::open(&table_path).unwrap();
        let batch = backend.read_columns_to_arrow(None, 0, None).unwrap();
        assert_eq!(batch.num_rows(), 5);
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let categories = batch
            .column_by_name("category")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.values(), &[1, 2, 3, 4, 5]);
        assert_eq!(categories.value(4), "e");
    }
}
