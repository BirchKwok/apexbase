//! Index Manager - Manages all indexes for a table
//!
//! Handles index lifecycle (create, drop, rebuild) and query optimization
//! by selecting the best index for a given query predicate.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::btree::{BTreeIndex, IndexKey};
use super::hash_index::HashIndex;
use crate::data::{DataType, Value};

// ============================================================================
// Index Type
// ============================================================================

/// Type of index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    /// B-Tree index: good for range queries and ordered access
    BTree,
    /// Hash index: optimal for equality lookups
    Hash,
}

impl IndexType {
    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "btree" | "b-tree" | "b_tree" => Some(IndexType::BTree),
            "hash" => Some(IndexType::Hash),
            _ => None,
        }
    }
}

// ============================================================================
// Index Metadata
// ============================================================================

/// Metadata for a single index (persisted in catalog)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMeta {
    /// Index name (unique within a table)
    pub name: String,
    /// Column name the index is built on (first column for composite)
    pub column_name: String,
    /// Index type
    pub index_type: IndexType,
    /// Whether it enforces uniqueness
    pub unique: bool,
    /// Column data type (type of first column for composite)
    pub data_type: DataType,
    /// Creation timestamp
    pub created_at: i64,
    /// All columns in the index (for composite indexes).
    /// Empty means single-column (backward compat with old serialized catalogs).
    #[serde(default)]
    pub columns: Vec<String>,
    /// Composite key encoding. Zero denotes the legacy NUL-joined format;
    /// version one is the typed tuple representation.
    #[serde(default)]
    pub key_format_version: u8,
}

#[derive(Serialize, Deserialize)]
struct LegacyIndexMeta {
    name: String,
    column_name: String,
    index_type: IndexType,
    unique: bool,
    data_type: DataType,
    created_at: i64,
    #[serde(default)]
    columns: Vec<String>,
}

impl IndexMeta {
    /// Get the effective column list (handles backward compat)
    pub fn effective_columns(&self) -> Vec<&str> {
        if self.columns.is_empty() {
            vec![&self.column_name]
        } else {
            self.columns.iter().map(|s| s.as_str()).collect()
        }
    }

    /// Whether this is a composite (multi-column) index
    pub fn is_composite(&self) -> bool {
        self.columns.len() > 1
    }
}

const TYPED_TUPLE_VERSION: u8 = 1;

/// Build a versioned typed tuple without string conversion or separators.
fn composite_key(columns: &[String], values: &HashMap<String, Value>) -> Option<IndexKey> {
    if columns.len() == 1 {
        values.get(&columns[0]).map(|v| IndexKey::from_value(v))
    } else {
        let mut parts: Vec<IndexKey> = Vec::with_capacity(columns.len());
        for col in columns {
            match values.get(col) {
                Some(v) => parts.push(IndexKey::from_value(v)),
                None => return None, // Missing column value
            }
        }
        Some(IndexKey::Tuple {
            version: TYPED_TUPLE_VERSION,
            values: parts,
        })
    }
}

// ============================================================================
// Index Instance (runtime)
// ============================================================================

/// A runtime index instance (either BTree or Hash)
enum IndexInstance {
    BTree(BTreeIndex),
    Hash(HashIndex),
}

impl IndexInstance {
    fn insert(&mut self, key: IndexKey, row_id: u64) -> io::Result<()> {
        match self {
            IndexInstance::BTree(idx) => idx.insert(key, row_id),
            IndexInstance::Hash(idx) => idx.insert(key, row_id),
        }
    }

    fn remove(&mut self, key: &IndexKey, row_id: u64) -> bool {
        match self {
            IndexInstance::BTree(idx) => idx.remove(key, row_id),
            IndexInstance::Hash(idx) => idx.remove(key, row_id),
        }
    }

    fn get(&self, key: &IndexKey) -> Option<&[u64]> {
        match self {
            IndexInstance::BTree(idx) => idx.get(key),
            IndexInstance::Hash(idx) => idx.get(key),
        }
    }

    fn tuple_prefix(&self, prefix: &[IndexKey]) -> Vec<u64> {
        match self {
            IndexInstance::BTree(index) => index.tuple_prefix(prefix, None),
            IndexInstance::Hash(index) => index.tuple_prefix(prefix),
        }
    }

    fn save(&mut self) -> io::Result<()> {
        match self {
            IndexInstance::BTree(idx) => idx.save(),
            IndexInstance::Hash(idx) => idx.save(),
        }
    }

    fn clear(&mut self) {
        match self {
            IndexInstance::BTree(idx) => idx.clear(),
            IndexInstance::Hash(idx) => idx.clear(),
        }
    }

    fn len(&self) -> u64 {
        match self {
            IndexInstance::BTree(idx) => idx.len(),
            IndexInstance::Hash(idx) => idx.len(),
        }
    }
}

// ============================================================================
// Query Hint (what the planner tells us)
// ============================================================================

/// A predicate hint from the query planner for index selection
#[derive(Debug, Clone)]
pub enum PredicateHint {
    /// Equality: col = value
    Eq(Value),
    /// Range: col BETWEEN low AND high
    Range { low: Value, high: Value },
    /// Greater than: col > value
    Gt(Value),
    /// Greater than or equal: col >= value
    Gte(Value),
    /// Less than: col < value
    Lt(Value),
    /// Less than or equal: col <= value
    Lte(Value),
    /// IN list: col IN (v1, v2, ...)
    In(Vec<Value>),
}

/// Result of an index lookup
#[derive(Debug)]
pub struct IndexLookupResult {
    /// Row IDs that match the predicate
    pub row_ids: Vec<u64>,
    /// Whether this is an exact result (no further filtering needed)
    pub exact: bool,
}

// ============================================================================
// Index Manager
// ============================================================================

/// Manages all indexes for a single table
///
/// Responsibilities:
/// - Create / drop / rebuild indexes
/// - Route queries to the best index
/// - Keep indexes in sync with data changes
/// - Persist index catalog
pub struct IndexManager {
    in_memory: bool,
    /// Table name
    table_name: String,
    /// Base directory for index files
    base_dir: PathBuf,
    /// Index catalog: index_name → metadata
    catalog: HashMap<String, IndexMeta>,
    /// Runtime index instances: index_name → instance
    instances: HashMap<String, IndexInstance>,
    /// Column → index name mapping for fast lookup
    column_index_map: HashMap<String, Vec<String>>,
    /// Whether catalog has been modified
    dirty: bool,
}

impl IndexManager {
    /// Create a new index manager for a table
    pub fn new(table_name: &str, base_dir: &Path) -> Self {
        let idx_dir = base_dir.join("indexes");
        Self {
            in_memory: crate::storage::is_memory_path(base_dir),
            table_name: table_name.to_string(),
            base_dir: idx_dir,
            catalog: HashMap::new(),
            instances: HashMap::new(),
            column_index_map: HashMap::new(),
            dirty: false,
        }
    }

    /// Load existing index catalog from disk
    pub fn load(table_name: &str, base_dir: &Path) -> io::Result<Self> {
        if crate::storage::is_memory_path(base_dir) {
            return Ok(Self::new(table_name, base_dir));
        }
        let idx_dir = base_dir.join("indexes");
        let catalog_path = idx_dir.join(format!("{}.idxcat", table_name));

        if !catalog_path.exists() {
            return Ok(Self::new(table_name, base_dir));
        }

        let data = std::fs::read(&catalog_path)?;
        let catalog: HashMap<String, IndexMeta> = match bincode::deserialize(&data) {
            Ok(catalog) => catalog,
            Err(current_error) => {
                let legacy: HashMap<String, LegacyIndexMeta> = bincode::deserialize(&data)
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, current_error.to_string())
                    })?;
                legacy
                    .into_iter()
                    .map(|(name, meta)| {
                        (
                            name,
                            IndexMeta {
                                name: meta.name,
                                column_name: meta.column_name,
                                index_type: meta.index_type,
                                unique: meta.unique,
                                data_type: meta.data_type,
                                created_at: meta.created_at,
                                columns: meta.columns,
                                key_format_version: 0,
                            },
                        )
                    })
                    .collect()
            }
        };

        let mut mgr = Self {
            in_memory: false,
            table_name: table_name.to_string(),
            base_dir: idx_dir,
            catalog: catalog.clone(),
            instances: HashMap::new(),
            column_index_map: HashMap::new(),
            dirty: false,
        };

        // Build column→index mapping.  Composite indexes are registered for
        // every indexed column so the planner can discover a usable prefix;
        // lookup itself still requires the complete composite key.
        for (name, meta) in &catalog {
            for column in meta.effective_columns() {
                mgr.column_index_map
                    .entry(column.to_string())
                    .or_insert_with(Vec::new)
                    .push(name.clone());
            }
        }

        // Lazily load index instances (only when needed)
        Ok(mgr)
    }

    /// Table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<&IndexMeta> {
        self.catalog.values().collect()
    }

    /// Get index metadata by name
    pub fn get_index_meta(&self, name: &str) -> Option<&IndexMeta> {
        self.catalog.get(name)
    }

    /// Check if a column has any index
    pub fn has_index_on(&self, column_name: &str) -> bool {
        self.column_index_map.contains_key(column_name)
    }

    /// Check if a column has a single-column index that can be probed without
    /// values from other columns.  Composite indexes must be planned through
    /// `lookup_composite` instead of being treated as single-column indexes.
    pub fn has_single_column_index_on(&self, column_name: &str) -> bool {
        self.column_index_map
            .get(column_name)
            .into_iter()
            .flatten()
            .any(|name| {
                self.catalog
                    .get(name)
                    .map(|meta| !meta.is_composite())
                    .unwrap_or(false)
            })
    }

    pub fn has_single_column_btree_on(&self, column_name: &str) -> bool {
        self.column_index_map
            .get(column_name)
            .into_iter()
            .flatten()
            .any(|name| {
                self.catalog.get(name).map_or(false, |meta| {
                    !meta.is_composite() && meta.index_type == IndexType::BTree
                })
            })
    }

    /// Check whether an index has exactly the supplied composite key order.
    pub fn has_composite_index(&self, columns: &[String]) -> bool {
        columns.len() > 1
            && self.catalog.values().any(|meta| {
                meta.is_composite()
                    && meta
                        .effective_columns()
                        .iter()
                        .map(|column| *column)
                        .eq(columns.iter().map(|column| column.as_str()))
            })
    }

    /// Check whether an index type can satisfy an equality or range lookup.
    pub fn has_usable_index_for_predicate(&self, column_name: &str, is_range: bool) -> bool {
        self.column_index_map
            .get(column_name)
            .into_iter()
            .flatten()
            .any(|name| {
                self.catalog.get(name).map_or(false, |meta| {
                    !meta.is_composite() && (!is_range || meta.index_type == IndexType::BTree)
                })
            })
    }

    /// Returns true when this table has no indexes at all — used as a fast CBO bypass.
    #[inline]
    pub fn catalog_is_empty(&self) -> bool {
        self.catalog.is_empty()
    }

    // ========================================================================
    // Index Lifecycle
    // ========================================================================

    /// Create a new single-column index
    pub fn create_index(
        &mut self,
        name: &str,
        column_name: &str,
        index_type: IndexType,
        unique: bool,
        data_type: DataType,
    ) -> io::Result<()> {
        self.create_index_multi(
            name,
            &[column_name.to_string()],
            index_type,
            unique,
            data_type,
        )
    }

    /// Create a new index (supports single or multi-column)
    pub fn create_index_multi(
        &mut self,
        name: &str,
        columns: &[String],
        index_type: IndexType,
        unique: bool,
        data_type: DataType,
    ) -> io::Result<()> {
        if columns.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Index requires at least one column",
            ));
        }
        if self.catalog.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Index '{}' already exists", name),
            ));
        }

        // Create index directory if needed
        if !self.in_memory {
            std::fs::create_dir_all(&self.base_dir)?;
        }

        let first_col = &columns[0];
        let meta = IndexMeta {
            name: name.to_string(),
            column_name: first_col.clone(),
            index_type,
            unique,
            data_type,
            created_at: chrono::Utc::now().timestamp(),
            columns: columns.to_vec(),
            key_format_version: if columns.len() > 1 {
                TYPED_TUPLE_VERSION
            } else {
                0
            },
        };

        // Create the runtime instance
        let index_path = self.index_file_path(name, index_type);
        let instance = match index_type {
            IndexType::BTree => {
                IndexInstance::BTree(BTreeIndex::with_path(first_col, unique, index_path))
            }
            IndexType::Hash => {
                IndexInstance::Hash(HashIndex::with_path(first_col, unique, index_path))
            }
        };

        // Register: map each column to this index
        for col in columns {
            self.column_index_map
                .entry(col.clone())
                .or_insert_with(Vec::new)
                .push(name.to_string());
        }
        self.catalog.insert(name.to_string(), meta);
        self.instances.insert(name.to_string(), instance);
        self.dirty = true;

        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&mut self, name: &str) -> io::Result<()> {
        let meta = self.catalog.get(name).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Index '{}' not found", name),
            )
        })?;

        // Remove from every column mapping.  Composite indexes are registered
        // under all their columns, not only the leading column.
        for column in meta.effective_columns() {
            if let Some(names) = self.column_index_map.get_mut(column) {
                names.retain(|n| n != name);
                if names.is_empty() {
                    self.column_index_map.remove(column);
                }
            }
        }
        self.catalog.remove(name);

        // Remove runtime instance
        self.instances.remove(name);

        // Remove index file
        let index_path = self.index_file_path(name, meta.index_type);
        let _ = std::fs::remove_file(&index_path);

        self.dirty = true;
        Ok(())
    }

    // ========================================================================
    // Data Maintenance (keep indexes in sync)
    // ========================================================================

    /// Notify that a row was inserted
    pub fn on_insert(
        &mut self,
        row_id: u64,
        column_values: &HashMap<String, Value>,
    ) -> io::Result<()> {
        // Collect unique index names that need updating
        let mut seen_indexes: std::collections::HashSet<String> = std::collections::HashSet::new();
        for col_name in column_values.keys() {
            if let Some(index_names) = self.column_index_map.get(col_name).cloned() {
                for idx_name in index_names {
                    seen_indexes.insert(idx_name);
                }
            }
        }
        for idx_name in &seen_indexes {
            let cols = self
                .catalog
                .get(idx_name)
                .map(|m| {
                    m.effective_columns()
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if let Some(key) = composite_key(&cols, column_values) {
                let instance = self.ensure_loaded(idx_name)?;
                instance.insert(key, row_id)?;
            }
        }
        Ok(())
    }

    /// Notify that a row was deleted
    pub fn on_delete(&mut self, row_id: u64, column_values: &HashMap<String, Value>) {
        let mut seen_indexes: std::collections::HashSet<String> = std::collections::HashSet::new();
        for col_name in column_values.keys() {
            if let Some(index_names) = self.column_index_map.get(col_name).cloned() {
                for idx_name in index_names {
                    seen_indexes.insert(idx_name);
                }
            }
        }
        for idx_name in &seen_indexes {
            let cols = self
                .catalog
                .get(idx_name)
                .map(|m| {
                    m.effective_columns()
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if let Some(key) = composite_key(&cols, column_values) {
                if let Some(instance) = self.instances.get_mut(idx_name.as_str()) {
                    instance.remove(&key, row_id);
                }
            }
        }
    }

    /// Notify that a row was updated
    pub fn on_update(
        &mut self,
        row_id: u64,
        old_values: &HashMap<String, Value>,
        new_values: &HashMap<String, Value>,
    ) -> io::Result<()> {
        // Update each affected index as a complete key.  Building a scalar
        // key for a composite index silently leaves stale entries behind.
        let index_names: Vec<String> = self.catalog.keys().cloned().collect();
        for idx_name in index_names {
            let columns: Vec<String> = self
                .catalog
                .get(&idx_name)
                .map(|meta| {
                    meta.effective_columns()
                        .iter()
                        .map(|column| column.to_string())
                        .collect()
                })
                .unwrap_or_default();
            let changed = columns.iter().any(|column| {
                old_values
                    .get(column)
                    .zip(new_values.get(column))
                    .map(|(old, new)| old != new)
                    .unwrap_or(false)
            });
            if !changed {
                continue;
            }
            let old_key = composite_key(&columns, old_values);
            let new_key = composite_key(&columns, new_values);
            if let Some(instance) = self.instances.get_mut(&idx_name) {
                if let Some(key) = old_key.as_ref() {
                    instance.remove(key, row_id);
                }
                if let Some(key) = new_key {
                    instance.insert(key, row_id)?;
                }
            }
        }
        Ok(())
    }

    // ========================================================================
    // Query Optimization
    // ========================================================================

    /// Try to use an index for a predicate on a column
    /// Returns row IDs if an index can satisfy the predicate, None otherwise
    pub fn lookup(
        &mut self,
        column_name: &str,
        predicate: &PredicateHint,
    ) -> io::Result<Option<IndexLookupResult>> {
        let index_names = match self.column_index_map.get(column_name) {
            Some(names) => names.clone(),
            None => return Ok(None),
        };

        // Find the best index for this predicate
        let best_idx_name = self.select_best_index(&index_names, predicate);
        if best_idx_name.is_none() {
            return Ok(None);
        }
        let idx_name = best_idx_name.unwrap();

        let instance = self.ensure_loaded(&idx_name)?;

        let row_ids = match predicate {
            PredicateHint::Eq(val) => {
                let key = IndexKey::from_value(val);
                instance
                    .get(&key)
                    .map(|ids| ids.to_vec())
                    .unwrap_or_default()
            }
            PredicateHint::Range { low, high } => {
                match instance {
                    IndexInstance::BTree(bt) => {
                        let low_key = IndexKey::from_value(low);
                        let high_key = IndexKey::from_value(high);
                        bt.range_inclusive(&low_key, &high_key)
                    }
                    IndexInstance::Hash(_) => return Ok(None), // Hash can't do range
                }
            }
            PredicateHint::Gt(val) => match instance {
                IndexInstance::BTree(bt) => {
                    let key = IndexKey::from_value(val);
                    bt.greater_than(&key)
                }
                IndexInstance::Hash(_) => return Ok(None),
            },
            PredicateHint::Gte(val) => match instance {
                IndexInstance::BTree(bt) => {
                    let key = IndexKey::from_value(val);
                    bt.greater_than_or_equal(&key)
                }
                IndexInstance::Hash(_) => return Ok(None),
            },
            PredicateHint::Lt(val) => match instance {
                IndexInstance::BTree(bt) => {
                    let key = IndexKey::from_value(val);
                    bt.less_than(&key)
                }
                IndexInstance::Hash(_) => return Ok(None),
            },
            PredicateHint::Lte(val) => match instance {
                IndexInstance::BTree(bt) => {
                    let key = IndexKey::from_value(val);
                    bt.less_than_or_equal(&key)
                }
                IndexInstance::Hash(_) => return Ok(None),
            },
            PredicateHint::In(vals) => {
                let mut result = Vec::new();
                for val in vals {
                    let key = IndexKey::from_value(val);
                    if let Some(ids) = instance.get(&key) {
                        result.extend_from_slice(ids);
                    }
                }
                result
            }
        };

        Ok(Some(IndexLookupResult {
            row_ids,
            exact: true,
        }))
    }

    // ========================================================================
    // Persistence
    // ========================================================================

    /// Save catalog and all dirty indexes to disk
    pub fn save(&mut self) -> io::Result<()> {
        if self.in_memory {
            self.dirty = false;
            return Ok(());
        }
        if self.dirty {
            std::fs::create_dir_all(&self.base_dir)?;
            let catalog_path = self.base_dir.join(format!("{}.idxcat", self.table_name));
            let data = bincode::serialize(&self.catalog)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            std::fs::write(&catalog_path, &data)?;
            self.dirty = false;
        }

        // Save all dirty index instances
        for instance in self.instances.values_mut() {
            instance.save()?;
        }
        Ok(())
    }

    /// Rebuild all indexes from scratch (used after compaction)
    pub fn rebuild_all(&mut self) {
        for instance in self.instances.values_mut() {
            instance.clear();
        }
    }

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    /// Get the file path for an index
    fn index_file_path(&self, name: &str, index_type: IndexType) -> PathBuf {
        let ext = match index_type {
            IndexType::BTree => "btidx",
            IndexType::Hash => "hashidx",
        };
        self.base_dir
            .join(format!("{}_{}.{}", self.table_name, name, ext))
    }

    /// Ensure an index instance is loaded into memory
    fn ensure_loaded(&mut self, name: &str) -> io::Result<&mut IndexInstance> {
        if !self.instances.contains_key(name) {
            let meta = self
                .catalog
                .get(name)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("Index '{}' not found", name),
                    )
                })?
                .clone();
            let path = self.index_file_path(name, meta.index_type);
            let instance = if path.exists() {
                match meta.index_type {
                    IndexType::BTree => IndexInstance::BTree(BTreeIndex::load(&path)?),
                    IndexType::Hash => IndexInstance::Hash(HashIndex::load(&path)?),
                }
            } else {
                match meta.index_type {
                    IndexType::BTree => IndexInstance::BTree(BTreeIndex::with_path(
                        &meta.column_name,
                        meta.unique,
                        path,
                    )),
                    IndexType::Hash => IndexInstance::Hash(HashIndex::with_path(
                        &meta.column_name,
                        meta.unique,
                        path,
                    )),
                }
            };
            self.instances.insert(name.to_string(), instance);
        }
        Ok(self.instances.get_mut(name).unwrap())
    }

    /// Select the best index for a predicate
    fn select_best_index(
        &self,
        index_names: &[String],
        predicate: &PredicateHint,
    ) -> Option<String> {
        let usable: Vec<&String> = index_names
            .iter()
            .filter(|name| {
                self.catalog
                    .get(*name)
                    .map(|meta| !meta.is_composite())
                    .unwrap_or(false)
            })
            .collect();
        match predicate {
            PredicateHint::Eq(_) | PredicateHint::In(_) => {
                // Prefer hash index for equality, fall back to btree
                for name in &usable {
                    if let Some(meta) = self.catalog.get(*name) {
                        if meta.index_type == IndexType::Hash {
                            return Some((*name).clone());
                        }
                    }
                }
                // Fall back to any available index
                usable.first().map(|name| (*name).clone())
            }
            PredicateHint::Range { .. }
            | PredicateHint::Gt(_)
            | PredicateHint::Gte(_)
            | PredicateHint::Lt(_)
            | PredicateHint::Lte(_) => {
                // Only BTree supports range queries
                for name in &usable {
                    if let Some(meta) = self.catalog.get(*name) {
                        if meta.index_type == IndexType::BTree {
                            return Some((*name).clone());
                        }
                    }
                }
                None
            }
        }
    }

    /// Lookup a complete equality key for a composite index.
    ///
    /// Prefix and range scans need a typed tuple key and are intentionally not
    /// accepted here until the on-disk key encoding supports ordered tuples.
    pub fn lookup_composite(
        &mut self,
        columns: &[String],
        values: &HashMap<String, Value>,
    ) -> io::Result<Option<IndexLookupResult>> {
        if columns.len() < 2 || columns.iter().any(|column| !values.contains_key(column)) {
            return Ok(None);
        }

        let index_name = self
            .catalog
            .iter()
            .find(|(_, meta)| {
                meta.is_composite()
                    && meta
                        .effective_columns()
                        .iter()
                        .map(|c| *c)
                        .eq(columns.iter().map(|c| c.as_str()))
            })
            .map(|(name, _)| name.clone());
        let Some(index_name) = index_name else {
            return Ok(None);
        };

        if self.catalog[&index_name].key_format_version != TYPED_TUPLE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "composite index '{}' uses a legacy key format; rebuild the index",
                    index_name
                ),
            ));
        }

        let key = composite_key(columns, values).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "incomplete composite key")
        })?;
        let instance = self.ensure_loaded(&index_name)?;
        let row_ids = instance
            .get(&key)
            .map(|ids| ids.to_vec())
            .unwrap_or_else(|| match &key {
                IndexKey::Tuple { version: 1, values } => instance.tuple_prefix(values),
                _ => Vec::new(),
            });
        Ok(Some(IndexLookupResult {
            row_ids,
            exact: true,
        }))
    }

    /// Probe the longest leading equality prefix of a composite index. A
    /// BTree can additionally constrain the next key element to an inclusive
    /// range. Legacy composite indexes are rejected with a rebuild message.
    pub fn lookup_composite_prefix(
        &mut self,
        columns: &[String],
        equality_values: &HashMap<String, Value>,
        range: Option<(&String, &Value, &Value)>,
    ) -> io::Result<Option<IndexLookupResult>> {
        let index_name = self
            .catalog
            .iter()
            .find(|(_, meta)| {
                meta.is_composite()
                    && meta
                        .effective_columns()
                        .iter()
                        .map(|c| *c)
                        .eq(columns.iter().map(String::as_str))
            })
            .map(|(name, _)| name.clone());
        let Some(index_name) = index_name else {
            return Ok(None);
        };
        let meta = &self.catalog[&index_name];
        if meta.key_format_version != TYPED_TUPLE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "composite index '{}' uses a legacy key format; rebuild the index",
                    index_name
                ),
            ));
        }
        let mut prefix = Vec::new();
        for column in columns {
            match equality_values.get(column) {
                Some(value) => prefix.push(IndexKey::from_value(value)),
                None => break,
            }
        }
        if prefix.is_empty() || prefix.len() == columns.len() {
            return Ok(None);
        }
        let range_keys = match range {
            Some((column, low, high)) if columns.get(prefix.len()) == Some(column) => {
                Some((IndexKey::from_value(low), IndexKey::from_value(high)))
            }
            _ => None,
        };
        let instance = self.ensure_loaded(&index_name)?;
        let row_ids = match instance {
            IndexInstance::BTree(index) => {
                index.tuple_prefix(&prefix, range_keys.as_ref().map(|(low, high)| (low, high)))
            }
            IndexInstance::Hash(index) if range_keys.is_none() => index.tuple_prefix(&prefix),
            IndexInstance::Hash(_) => return Ok(None),
        };
        Ok(Some(IndexLookupResult {
            row_ids,
            exact: true,
        }))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_manager_create_and_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = IndexManager::new("test_table", dir.path());

        // Create a hash index on _id
        mgr.create_index("idx_id", "_id", IndexType::Hash, true, DataType::UInt64)
            .unwrap();

        // Insert some data
        let mut row = HashMap::new();
        row.insert("_id".to_string(), Value::UInt64(1));
        mgr.on_insert(0, &row).unwrap();

        row.insert("_id".to_string(), Value::UInt64(2));
        mgr.on_insert(1, &row).unwrap();

        // Lookup
        let result = mgr
            .lookup("_id", &PredicateHint::Eq(Value::UInt64(1)))
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().row_ids, vec![0]);
    }

    #[test]
    fn test_index_manager_btree_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = IndexManager::new("test_table", dir.path());

        mgr.create_index("idx_age", "age", IndexType::BTree, false, DataType::Int64)
            .unwrap();

        for i in 0..100 {
            let mut row = HashMap::new();
            row.insert("age".to_string(), Value::Int64(i));
            mgr.on_insert(i as u64, &row).unwrap();
        }

        let result = mgr
            .lookup(
                "age",
                &PredicateHint::Range {
                    low: Value::Int64(10),
                    high: Value::Int64(20),
                },
            )
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().row_ids.len(), 11);
    }

    #[test]
    fn test_index_manager_persistence() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut mgr = IndexManager::new("test_table", dir.path());
            mgr.create_index(
                "idx_name",
                "name",
                IndexType::BTree,
                false,
                DataType::String,
            )
            .unwrap();

            let mut row = HashMap::new();
            row.insert("name".to_string(), Value::String("alice".into()));
            mgr.on_insert(0, &row).unwrap();

            mgr.save().unwrap();
        }

        // Reload
        let mut mgr = IndexManager::load("test_table", dir.path()).unwrap();
        assert!(mgr.has_index_on("name"));

        let result = mgr
            .lookup("name", &PredicateHint::Eq(Value::String("alice".into())))
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().row_ids, vec![0]);
    }

    #[test]
    fn test_index_manager_drop() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = IndexManager::new("test_table", dir.path());
        mgr.create_index("idx_x", "x", IndexType::Hash, false, DataType::Int64)
            .unwrap();
        assert!(mgr.has_index_on("x"));

        mgr.drop_index("idx_x").unwrap();
        assert!(!mgr.has_index_on("x"));
    }

    #[test]
    fn test_composite_lookup_reload_and_drop_cleans_all_columns() {
        let dir = tempfile::tempdir().unwrap();
        let columns = vec!["city".to_string(), "age".to_string()];
        {
            let mut mgr = IndexManager::new("test_table", dir.path());
            mgr.create_index_multi(
                "idx_city_age",
                &columns,
                IndexType::Hash,
                false,
                DataType::String,
            )
            .unwrap();
            for (id, (city, age)) in [("NYC", 20i64), ("NYC", 30), ("LA", 20)]
                .into_iter()
                .enumerate()
            {
                let mut row = HashMap::new();
                row.insert("city".to_string(), Value::String(city.to_string()));
                row.insert("age".to_string(), Value::Int64(age));
                mgr.on_insert(id as u64, &row).unwrap();
            }
            mgr.save().unwrap();
        }

        let mut mgr = IndexManager::load("test_table", dir.path()).unwrap();
        assert!(mgr.has_index_on("city"));
        assert!(mgr.has_index_on("age"));
        let mut values = HashMap::new();
        values.insert("city".to_string(), Value::String("NYC".to_string()));
        values.insert("age".to_string(), Value::Int64(30));
        let result = mgr.lookup_composite(&columns, &values).unwrap().unwrap();
        assert_eq!(result.row_ids, vec![1]);

        mgr.drop_index("idx_city_age").unwrap();
        assert!(!mgr.has_index_on("city"));
        assert!(!mgr.has_index_on("age"));
    }

    #[test]
    fn typed_composite_keys_do_not_collide_on_embedded_nul() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = IndexManager::new("test_table", dir.path());
        let columns = vec!["left".to_string(), "right".to_string()];
        mgr.create_index_multi(
            "idx_pair",
            &columns,
            IndexType::Hash,
            false,
            DataType::String,
        )
        .unwrap();
        let first = HashMap::from([
            ("left".to_string(), Value::String("a\0b".to_string())),
            ("right".to_string(), Value::String("c".to_string())),
        ]);
        let second = HashMap::from([
            ("left".to_string(), Value::String("a".to_string())),
            ("right".to_string(), Value::String("b\0c".to_string())),
        ]);
        mgr.on_insert(10, &first).unwrap();
        mgr.on_insert(20, &second).unwrap();
        assert_eq!(
            mgr.lookup_composite(&columns, &first)
                .unwrap()
                .unwrap()
                .row_ids,
            vec![10]
        );
        assert_eq!(
            mgr.lookup_composite(&columns, &second)
                .unwrap()
                .unwrap()
                .row_ids,
            vec![20]
        );
    }

    #[test]
    fn composite_btree_supports_prefix_and_next_column_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = IndexManager::new("test_table", dir.path());
        let columns = vec!["city".to_string(), "age".to_string()];
        mgr.create_index_multi(
            "idx_city_age",
            &columns,
            IndexType::BTree,
            false,
            DataType::String,
        )
        .unwrap();
        for (row_id, city, age) in [
            (1, "NYC", 20),
            (2, "NYC", 30),
            (3, "NYC", 40),
            (4, "SF", 30),
        ] {
            mgr.on_insert(
                row_id,
                &HashMap::from([
                    ("city".to_string(), Value::String(city.to_string())),
                    ("age".to_string(), Value::Int64(age)),
                ]),
            )
            .unwrap();
        }
        let equality = HashMap::from([("city".to_string(), Value::String("NYC".to_string()))]);
        let prefix = mgr
            .lookup_composite_prefix(&columns, &equality, None)
            .unwrap()
            .unwrap();
        assert_eq!(prefix.row_ids, vec![1, 2, 3]);
        let range_column = "age".to_string();
        let low = Value::Int64(25);
        let high = Value::Int64(35);
        let ranged = mgr
            .lookup_composite_prefix(&columns, &equality, Some((&range_column, &low, &high)))
            .unwrap()
            .unwrap();
        assert_eq!(ranged.row_ids, vec![2]);
    }

    #[test]
    fn legacy_composite_catalog_requests_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let index_dir = dir.path().join("indexes");
        std::fs::create_dir_all(&index_dir).unwrap();
        let legacy = HashMap::from([(
            "idx_pair".to_string(),
            LegacyIndexMeta {
                name: "idx_pair".to_string(),
                column_name: "left".to_string(),
                index_type: IndexType::Hash,
                unique: false,
                data_type: DataType::String,
                created_at: 0,
                columns: vec!["left".to_string(), "right".to_string()],
            },
        )]);
        std::fs::write(
            index_dir.join("test_table.idxcat"),
            bincode::serialize(&legacy).unwrap(),
        )
        .unwrap();

        let mut mgr = IndexManager::load("test_table", dir.path()).unwrap();
        let error = mgr
            .lookup_composite(
                &["left".to_string(), "right".to_string()],
                &HashMap::from([
                    ("left".to_string(), Value::String("a".to_string())),
                    ("right".to_string(), Value::String("b".to_string())),
                ]),
            )
            .unwrap_err();
        assert!(error.to_string().contains("rebuild the index"));
    }
}
