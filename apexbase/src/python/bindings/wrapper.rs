//! PyO3 bindings - On-demand storage engine
//!
//! This module provides Python bindings that use on-demand storage directly,
//! enabling on-demand reading without loading entire tables into memory.

use crate::data::Value;
use crate::fts::FtsConfig;
use crate::fts::FtsManager;
use crate::query::{ApexResult, SqlParser};
use crate::storage::on_demand::{ColumnValue, SchemaStableValue};
use crate::storage::{DurabilityLevel, TableStorageBackend};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use fs2::FileExt;
use parking_lot::RwLock;
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[path = "arrow.rs"]
mod arrow_bridge;
#[path = "blob.rs"]
mod blob;
#[path = "read.rs"]
mod read;
#[path = "sql.rs"]
mod sql;
#[path = "write.rs"]
mod write;

#[derive(Clone)]
struct SchemaStableMemtableWriter {
    database: String,
    table_name: String,
    table_path: PathBuf,
    backend: Arc<TableStorageBackend>,
    schema: Vec<(String, crate::storage::on_demand::ColumnType)>,
}

/// Convert Python dict to HashMap<String, Value>
fn dict_to_values(dict: &Bound<'_, PyDict>) -> PyResult<HashMap<String, Value>> {
    let mut fields = HashMap::with_capacity(dict.len());

    for (key, value) in dict.iter() {
        let key: String = key.extract()?;
        if key == "_id" {
            continue;
        }
        let val = py_to_value(&value)?;
        fields.insert(key, val);
    }

    Ok(fields)
}

#[inline]
fn sort_and_dedupe_ids(ids: &[u64]) -> Vec<u64> {
    if ids.len() < 2 {
        return ids.to_vec();
    }

    let mut sorted_ids = ids.to_vec();
    sorted_ids.sort_unstable();
    sorted_ids.dedup();
    sorted_ids
}

#[inline]
fn next_up_f64_binding(value: f64) -> f64 {
    if value.is_nan() || value == f64::INFINITY {
        return value;
    }
    if value == 0.0 {
        return f64::from_bits(1);
    }
    let bits = value.to_bits();
    if value > 0.0 {
        f64::from_bits(bits + 1)
    } else {
        f64::from_bits(bits - 1)
    }
}

#[inline]
fn next_down_f64_binding(value: f64) -> f64 {
    -next_up_f64_binding(-value)
}

/// Parse aggregate expressions from SELECT clause: "SELECT COUNT(*) as cnt, AVG(score)"
/// Returns Vec of (function_name, optional_column_name, optional_alias)
fn parse_agg_select(sql: &str) -> Option<Vec<(String, Option<String>, Option<String>)>> {
    let upper = sql.to_ascii_uppercase();
    let select_start = upper.find("SELECT")? + 6;
    let from_pos = upper[select_start..].find(" FROM")?;
    let select_clause = &sql[select_start..select_start + from_pos];
    let mut result = Vec::new();
    for part in select_clause.split(',') {
        let part = part.trim();
        let part_upper = part.to_ascii_uppercase();
        if let Some(lp) = part_upper.find('(') {
            let func_name = part_upper[..lp].trim().to_string();
            if matches!(func_name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                // Find closing paren
                let after_lp = &part[lp + 1..];
                if let Some(rp) = after_lp.find(')') {
                    let inner = after_lp[..rp].trim();
                    let col = if inner == "*" || inner.is_empty() {
                        None
                    } else {
                        Some(inner.trim_matches('"').to_string())
                    };
                    // Check for alias after closing paren
                    let after_paren = after_lp[rp + 1..].trim();
                    let alias = if after_paren.to_ascii_uppercase().starts_with("AS ") {
                        Some(after_paren[3..].trim().trim_matches('"').to_string())
                    } else if !after_paren.is_empty() && !after_paren.starts_with(',') {
                        Some(after_paren.trim().trim_matches('"').to_string())
                    } else {
                        None
                    };
                    result.push((func_name, col, alias));
                }
            }
        }
    }
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Compute sum/min/max from an Arrow array (Int64 or Float64)
fn agg_array_stats(arr: &dyn arrow::array::Array) -> (f64, f64, f64, bool) {
    use arrow::array::{Float64Array, Int64Array};
    if let Some(ia) = arr.as_any().downcast_ref::<Int64Array>() {
        let sum: i64 = ia.iter().flatten().sum();
        let min = ia.iter().flatten().min().unwrap_or(i64::MAX);
        let max = ia.iter().flatten().max().unwrap_or(i64::MIN);
        (sum as f64, min as f64, max as f64, true)
    } else if let Some(fa) = arr.as_any().downcast_ref::<Float64Array>() {
        let sum: f64 = fa.iter().flatten().sum();
        let min = fa.iter().flatten().fold(f64::INFINITY, f64::min);
        let max = fa.iter().flatten().fold(f64::NEG_INFINITY, f64::max);
        (sum, min, max, false)
    } else {
        (0.0, 0.0, 0.0, false)
    }
}

/// Convert Python value to Value
fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    use pyo3::types::PyBytes;

    if obj.is_none() {
        return Ok(Value::Null);
    }

    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Value::Bool(b));
    }

    if let Ok(i) = obj.extract::<i64>() {
        return Ok(Value::Int64(i));
    }

    if let Ok(f) = obj.extract::<f64>() {
        return Ok(Value::Float64(f));
    }

    // Check for bytes BEFORE string (bytes can be extracted as string)
    if obj.is_instance_of::<PyBytes>() {
        if let Ok(bytes) = obj.extract::<Vec<u8>>() {
            return Ok(Value::Binary(bytes));
        }
    }

    // numpy ndarray (1-D float32 or float64) → FixedList (raw LE f32 bytes)
    // Checked BEFORE list/sequence to catch np.ndarray first.
    if obj
        .get_type()
        .name()
        .map(|n| n == "ndarray")
        .unwrap_or(false)
    {
        if let Ok(floats) = obj
            .call_method0("flatten")
            .and_then(|flat| flat.call_method1("astype", ("float32",)))
            .and_then(|f32arr| f32arr.call_method0("tobytes"))
            .and_then(|b| b.extract::<Vec<u8>>())
        {
            if floats.len() % 4 == 0 {
                return Ok(Value::FixedList(floats));
            }
        }
    }

    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::String(s));
    }

    // Python list/tuple of numbers → FixedList (raw LE f32 bytes), matching numpy vectors.
    if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
        if let Ok(values) = obj.extract::<Vec<f32>>() {
            let mut bytes = Vec::with_capacity(values.len() * 4);
            for value in values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            return Ok(Value::FixedList(bytes));
        }
    }

    if let Ok(bytes) = obj.extract::<Vec<u8>>() {
        return Ok(Value::Binary(bytes));
    }

    Ok(Value::Null)
}

fn py_to_column_value(obj: &Bound<'_, PyAny>) -> PyResult<ColumnValue> {
    use pyo3::types::PyBytes;

    if obj.is_none() {
        return Ok(ColumnValue::Null);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(ColumnValue::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(ColumnValue::Int64(i));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(ColumnValue::Float64(f));
    }
    if obj.is_instance_of::<PyBytes>() {
        if let Ok(bytes) = obj.extract::<Vec<u8>>() {
            return Ok(ColumnValue::Binary(bytes));
        }
    }
    if obj
        .get_type()
        .name()
        .map(|n| n == "ndarray")
        .unwrap_or(false)
    {
        if let Ok(floats) = obj
            .call_method0("flatten")
            .and_then(|flat| flat.call_method1("astype", ("float32",)))
            .and_then(|f32arr| f32arr.call_method0("tobytes"))
            .and_then(|b| b.extract::<Vec<u8>>())
        {
            if floats.len() % 4 == 0 {
                return Ok(ColumnValue::FixedList(floats));
            }
        }
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(ColumnValue::String(s));
    }
    if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
        if let Ok(values) = obj.extract::<Vec<f32>>() {
            let mut bytes = Vec::with_capacity(values.len() * 4);
            for value in values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            return Ok(ColumnValue::FixedList(bytes));
        }
    }
    Ok(ColumnValue::Null)
}

fn dict_to_column_values(dict: &Bound<'_, PyDict>) -> PyResult<HashMap<String, ColumnValue>> {
    let mut fields = HashMap::with_capacity(dict.len());

    for (key, value) in dict.iter() {
        let key: String = key.extract()?;
        if key == "_id" {
            continue;
        }
        fields.insert(key, py_to_column_value(&value)?);
    }

    Ok(fields)
}

/// Convert ColumnValue to Python object
#[allow(dead_code)]
fn column_value_to_py(py: Python<'_>, val: &ColumnValue) -> PyResult<PyObject> {
    match val {
        ColumnValue::Null => Ok(py.None()),
        ColumnValue::Bool(b) => Ok(b.into_py(py)),
        ColumnValue::Int64(i) => Ok(i.into_py(py)),
        ColumnValue::Float64(f) => Ok(f.into_py(py)),
        ColumnValue::String(s) => Ok(s.into_py(py)),
        ColumnValue::Binary(b) => Ok(b.clone().into_py(py)),
        ColumnValue::Blob(b) => Ok(b.clone().into_py(py)),
        ColumnValue::FixedList(b) => Ok(b.clone().into_py(py)),
    }
}

/// Convert Value to Python object
fn value_to_py(py: Python<'_>, val: &Value) -> PyResult<PyObject> {
    use pyo3::types::PyBytes;

    match val {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_py(py)),
        Value::Int8(i) => Ok((*i as i64).into_py(py)),
        Value::Int16(i) => Ok((*i as i64).into_py(py)),
        Value::Int32(i) => Ok((*i as i64).into_py(py)),
        Value::Int64(i) => Ok(i.into_py(py)),
        Value::UInt8(i) => Ok((*i as i64).into_py(py)),
        Value::UInt16(i) => Ok((*i as i64).into_py(py)),
        Value::UInt32(i) => Ok((*i as i64).into_py(py)),
        Value::UInt64(i) => Ok((*i as i64).into_py(py)),
        Value::Float32(f) => Ok((*f as f64).into_py(py)),
        Value::Float64(f) => Ok(f.into_py(py)),
        Value::String(s) => Ok(s.into_py(py)),
        Value::Binary(b) => Ok(PyBytes::new_bound(py, b).into()),
        Value::Blob(b) => Ok(PyBytes::new_bound(py, b).into()),
        Value::FixedList(b) => Ok(PyBytes::new_bound(py, b).into()),
        Value::Json(j) => Ok(j.to_string().into_py(py)),
        Value::Timestamp(t) => Ok(t.into_py(py)),
        Value::Date(d) => Ok(d.into_py(py)),
        Value::Array(arr) => {
            let list = PyList::empty_bound(py);
            for v in arr {
                list.append(value_to_py(py, v)?)?;
            }
            Ok(list.into())
        }
    }
}

fn values_to_columns_dict<'py>(
    py: Python<'py>,
    vals: &[(String, Value)],
) -> PyResult<Bound<'py, PyDict>> {
    let columns_dict = PyDict::new_bound(py);
    for (col_name, val) in vals {
        let pyval = value_to_py(py, val)?;
        columns_dict.set_item(col_name.as_str(), PyList::new_bound(py, [pyval]))?;
    }
    Ok(columns_dict)
}

fn mmap_batch_columns_to_pydict<'py>(
    py: Python<'py>,
    batch: crate::storage::on_demand::MmapBatchColumns,
    requested: Option<&[String]>,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    use crate::storage::on_demand::MmapBatchColumn;
    use pyo3::types::{PyBytes, PyList};

    let columns_dict = PyDict::new_bound(py);
    if batch.row_count == 0 {
        return Ok(Some(columns_dict));
    }

    let mut columns = batch.columns;
    let emit_column = |name: String, col: MmapBatchColumn| -> PyResult<()> {
        match col {
            MmapBatchColumn::I64(vals) => {
                columns_dict.set_item(name.as_str(), PyList::new_bound(py, vals))?;
            }
            MmapBatchColumn::F64(vals) => {
                columns_dict.set_item(name.as_str(), PyList::new_bound(py, vals))?;
            }
            MmapBatchColumn::Str(vals) => {
                columns_dict.set_item(name.as_str(), PyList::new_bound(py, vals))?;
            }
            MmapBatchColumn::Bool(vals) => {
                columns_dict.set_item(name.as_str(), PyList::new_bound(py, vals))?;
            }
            MmapBatchColumn::Bin(vals) => {
                let list = PyList::empty_bound(py);
                for val in vals {
                    match val {
                        Some(bytes) => list.append(PyBytes::new_bound(py, &bytes))?,
                        None => list.append(py.None())?,
                    }
                }
                columns_dict.set_item(name.as_str(), list)?;
            }
        }
        Ok(())
    };

    if let Some(requested) = requested {
        for requested_col in requested {
            let Some(pos) = columns.iter().position(|(name, _)| name == requested_col) else {
                return Ok(None);
            };
            let (name, col) = columns.swap_remove(pos);
            emit_column(name, col)?;
        }
    } else {
        for (name, col) in columns {
            emit_column(name, col)?;
        }
    }

    Ok(Some(columns_dict))
}

fn projected_values_to_columns_dict<'py>(
    py: Python<'py>,
    vals: &[(String, Value)],
    columns: &[String],
) -> PyResult<Option<Bound<'py, PyDict>>> {
    let columns_dict = PyDict::new_bound(py);
    if vals.len() >= columns.len()
        && columns.iter().enumerate().all(|(idx, requested_col)| {
            vals.get(idx)
                .map(|(col_name, _)| col_name == requested_col)
                .unwrap_or(false)
        })
    {
        for (requested_col, (_, val)) in columns.iter().zip(vals.iter()) {
            let pyval = value_to_py(py, val)?;
            columns_dict.set_item(requested_col.as_str(), PyList::new_bound(py, [pyval]))?;
        }
        return Ok(Some(columns_dict));
    }

    for requested_col in columns {
        let Some((_, val)) = vals.iter().find(|(col_name, _)| col_name == requested_col) else {
            return Ok(None);
        };
        let pyval = value_to_py(py, val)?;
        columns_dict.set_item(requested_col.as_str(), PyList::new_bound(py, [pyval]))?;
    }
    Ok(Some(columns_dict))
}

fn projected_values_to_row_dict<'py>(
    py: Python<'py>,
    vals: &[(String, Value)],
    columns: &[String],
) -> PyResult<Option<Bound<'py, PyDict>>> {
    let row = PyDict::new_bound(py);
    if vals.len() >= columns.len()
        && columns.iter().enumerate().all(|(idx, requested_col)| {
            vals.get(idx)
                .map(|(col_name, _)| col_name == requested_col)
                .unwrap_or(false)
        })
    {
        for (requested_col, (_, val)) in columns.iter().zip(vals.iter()) {
            row.set_item(requested_col.as_str(), value_to_py(py, val)?)?;
        }
        return Ok(Some(row));
    }

    for requested_col in columns {
        let Some((_, val)) = vals.iter().find(|(col_name, _)| col_name == requested_col) else {
            return Ok(None);
        };
        row.set_item(requested_col.as_str(), value_to_py(py, val)?)?;
    }
    Ok(Some(row))
}

#[derive(Clone, Copy)]
struct NumericUpdateCellCache {
    footer_offset: u64,
    null_byte_file_offset: u64,
    null_mask: u8,
    value_file_offset: u64,
}

/// Per-client backend cache validated by the storage-owned table epoch.
/// The epoch is shared through the table lock mapping, so the hot path is an
/// atomic load even when another process may commit the table.
type EpochBackendEntry = (Arc<TableStorageBackend>, u64);

struct EpochBackendCache {
    entries: DashMap<String, EpochBackendEntry>,
}

impl EpochBackendCache {
    fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    #[inline]
    fn get(&self, key: &str) -> Option<Arc<TableStorageBackend>> {
        let mut entry = self.entries.get_mut(key)?;
        let current = crate::storage::epoch::current(entry.0.path());
        if entry.1 != current {
            if entry.0.has_pending_deltas() || entry.0.pending_v4_in_memory_rows() > 0 {
                entry.1 = current;
            } else {
                drop(entry);
                self.entries.remove(key);
                return None;
            }
        }
        Some(Arc::clone(&entry.0))
    }

    #[inline]
    fn insert(&self, key: String, backend: Arc<TableStorageBackend>) -> Option<EpochBackendEntry> {
        let epoch = crate::storage::epoch::current(backend.path());
        self.entries.insert(key, (backend, epoch))
    }

    #[inline]
    fn remove(&self, key: &str) -> Option<(String, EpochBackendEntry)> {
        self.entries.remove(key)
    }

    #[inline]
    fn retain(&self, mut keep: impl FnMut(&String) -> bool) {
        self.entries.retain(|key, _| keep(key));
    }

    #[inline]
    fn clear(&self) {
        self.entries.clear();
    }

    fn all_backends(&self) -> Vec<Arc<TableStorageBackend>> {
        self.entries
            .iter()
            .map(|entry| Arc::clone(&entry.value().0))
            .collect()
    }
}

/// ApexStorage - On-demand columnar storage engine
///
/// This storage engine uses V4 format (.apex) for persistence and supports:
/// - On-demand column reading (only loads requested columns)
/// - On-demand row range reading (only loads requested rows)
/// - Soft delete with deleted bitmap
/// - Full SQL query support via ApexExecutor
/// - Cross-platform file locking for concurrent access safety
///   - Read operations use shared locks (multiple readers allowed)
///   - Write operations use exclusive locks (single writer)
/// - Multi-database support: named databases stored in subdirectories
#[pyclass(name = "ApexStorage")]
pub struct ApexStorageImpl {
    /// Root directory (top-level dir; contains both default tables and named-db subdirs)
    root_dir: PathBuf,
    /// Current database name. "" or "default" means root_dir (backward-compat default).
    /// Named databases (e.g. "analytics") reside at root_dir/analytics/.
    current_database: RwLock<String>,
    /// Current base directory = root_dir (default) or root_dir/db_name (named db).
    /// Updated atomically by use_database_().
    base_dir: RwLock<PathBuf>,
    /// Table paths (table_name -> path) - lazily populated
    table_paths: RwLock<HashMap<String, PathBuf>>,
    /// Whether table_paths has been fully scanned from directory
    tables_scanned: RwLock<bool>,
    /// Cached storage backends per table (table_name -> backend)
    /// Backends are opened once and reused for all operations
    /// Uses DashMap for lock-free concurrent reads
    cached_backends: EpochBackendCache,
    /// Verified `(table, column) -> ColumnType` entries for numeric `_id` update fast paths.
    update_by_id_numeric_cache: DashMap<String, (crate::storage::on_demand::ColumnType, u64)>,
    /// Verified `(table, column, id) -> physical cell offsets` entries for repeated numeric updates.
    update_by_id_cell_cache: DashMap<String, (NumericUpdateCellCache, u64)>,
    /// Exact full-row payloads for repeated idempotent `replace(id, row)` calls.
    replace_exact_row_cache: DashMap<String, (HashMap<String, Value>, u64)>,
    /// Tables written since the last flush; flush preopens their read backend
    /// so the first post-load SELECT does not pay mmap/footer open cost.
    flush_prewarm_tables: DashMap<String, PathBuf>,
    /// Current table name
    current_table: RwLock<String>,
    /// FTS Manager (optional) — Arc so it can be shared with the global SQL executor registry
    fts_manager: RwLock<Option<Arc<FtsManager>>>,
    /// FTS index field names per table
    fts_index_fields: RwLock<HashMap<String, Vec<String>>>,
    /// Durability level for ACID guarantees
    durability: DurabilityLevel,
    /// Current active transaction ID (None if not in a transaction)
    current_txn_id: RwLock<Option<u64>>,
    /// Auto-flush row threshold (struct-level so it survives backend cache invalidation)
    auto_flush_rows: RwLock<u64>,
    /// Auto-flush byte threshold (struct-level so it survives backend cache invalidation)
    auto_flush_bytes: RwLock<u64>,
    /// Temp directory for temporary tables (root_dir/.apex_tmp/)
    temp_dir: PathBuf,
    /// Narrow single-row memtable writer cache for schema-stable OLTP inserts.
    schema_stable_memtable_writer: RwLock<Option<SchemaStableMemtableWriter>>,
}

/// Internal Rust-only methods (not exposed to Python)
impl ApexStorageImpl {
    #[inline]
    fn backend_cache_key(table_path: &std::path::Path, table_name: &str) -> String {
        format!("{}\0{}", table_path.to_string_lossy(), table_name)
    }

    #[inline]
    fn insert_backend_cache_key(table_path: &std::path::Path, table_name: &str) -> String {
        format!("{}\0{}\0insert", table_path.to_string_lossy(), table_name)
    }

    fn try_insert_schema_stable_borrowed_row(
        row: &Bound<'_, PyDict>,
        schema: &[(String, crate::storage::on_demand::ColumnType)],
        backend: &TableStorageBackend,
    ) -> PyResult<Option<u64>> {
        let has_internal_id = row.get_item("_id")?.is_some();
        if row
            .len()
            .saturating_sub(if has_internal_id { 1 } else { 0 })
            != schema.len()
        {
            return Ok(None);
        }

        let mut py_values = Vec::with_capacity(schema.len());
        for (col_name, _) in schema {
            let Some(value) = row.get_item(col_name.as_str())? else {
                return Ok(None);
            };
            if value.is_none() {
                return Ok(None);
            }
            py_values.push(value);
        }

        let mut ordered_values = Vec::with_capacity(schema.len());
        for ((_, col_type), value) in schema.iter().zip(py_values.iter()) {
            use crate::storage::on_demand::ColumnType;
            match *col_type {
                ColumnType::Bool => {
                    let Ok(v) = value.extract::<bool>() else {
                        return Ok(None);
                    };
                    ordered_values.push(SchemaStableValue::Bool(v));
                }
                ColumnType::Int8
                | ColumnType::Int16
                | ColumnType::Int32
                | ColumnType::Int64
                | ColumnType::UInt8
                | ColumnType::UInt16
                | ColumnType::UInt32
                | ColumnType::UInt64
                | ColumnType::Timestamp
                | ColumnType::Date => {
                    let Ok(v) = value.extract::<i64>() else {
                        return Ok(None);
                    };
                    ordered_values.push(SchemaStableValue::Int64(v));
                }
                ColumnType::Float32 | ColumnType::Float64 => {
                    let v = match value.extract::<f64>() {
                        Ok(v) => v,
                        Err(_) => match value.extract::<i64>() {
                            Ok(v) => v as f64,
                            Err(_) => return Ok(None),
                        },
                    };
                    ordered_values.push(SchemaStableValue::Float64(v));
                }
                ColumnType::String => {
                    let Ok(v) = value.extract::<&str>() else {
                        return Ok(None);
                    };
                    ordered_values.push(SchemaStableValue::Str(v));
                }
                ColumnType::Binary => {
                    let Ok(v) = value.downcast::<PyBytes>() else {
                        return Ok(None);
                    };
                    ordered_values.push(SchemaStableValue::Binary(v.as_bytes()));
                }
                ColumnType::Blob
                | ColumnType::StringDict
                | ColumnType::FixedList
                | ColumnType::Float16List
                | ColumnType::Null => return Ok(None),
            }
        }

        let id = backend
            .insert_one_schema_stable_borrowed(&ordered_values)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(Some(id))
    }

    fn try_cached_schema_stable_memtable_insert(
        &self,
        row: &Bound<'_, PyDict>,
    ) -> PyResult<Option<Vec<i64>>> {
        let current_table = self.current_table.read();
        let current_database = self.current_database.read();
        let guard = self.schema_stable_memtable_writer.read();
        let writer = guard.as_ref().filter(|writer| {
            writer.table_name == *current_table && writer.database == *current_database
        });
        let Some(writer) = writer else {
            return Ok(None);
        };
        if writer.backend.has_pending_deltas() {
            return Ok(None);
        }

        let Some(id) =
            Self::try_insert_schema_stable_borrowed_row(row, &writer.schema, &writer.backend)?
        else {
            return Ok(None);
        };

        crate::Database::cache_backend(&writer.table_path, Arc::clone(&writer.backend));
        crate::query::planner::invalidate_table_stats(&writer.table_path.to_string_lossy());

        Ok(Some(vec![id as i64]))
    }

    #[inline]
    fn replace_row_cache_key(
        table_path: &std::path::Path,
        table_name: &str,
        row_id: u64,
    ) -> String {
        format!(
            "{}\0{}\0replace\0{}",
            table_path.to_string_lossy(),
            table_name,
            row_id
        )
    }

    /// Get the lock file path for a table
    #[inline]
    fn get_lock_path(table_path: &Path) -> PathBuf {
        table_path.with_extension("apex.lock")
    }

    /// Acquire a lock on the table (shared for read, exclusive for write).
    /// Uses retry with exponential backoff (100µs → 200µs → ... → 50ms max total wait).
    /// This avoids spurious "Database is locked" errors under concurrent load.
    fn acquire_lock(table_path: &Path, exclusive: bool) -> io::Result<File> {
        let lock_path = Self::get_lock_path(table_path);
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;

        let max_wait = std::time::Duration::from_secs(30);
        let mut backoff = std::time::Duration::from_micros(100);
        let start = std::time::Instant::now();

        loop {
            let result: io::Result<()> = if exclusive {
                lock_file.try_lock_exclusive()
            } else {
                lock_file
                    .try_lock_shared()
                    .map_err(|e| io::Error::new(io::ErrorKind::WouldBlock, e.to_string()))
            };

            match result {
                Ok(()) => return Ok(lock_file),
                Err(_) if start.elapsed() < max_wait => {
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(std::time::Duration::from_millis(5));
                }
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!(
                            "Database is locked (waited {}ms): {}",
                            start.elapsed().as_millis(),
                            e
                        ),
                    ));
                }
            }
        }
    }

    #[inline]
    fn acquire_read_lock(table_path: &Path) -> io::Result<File> {
        Self::acquire_lock(table_path, false)
    }

    #[inline]
    fn acquire_write_lock(table_path: &Path) -> io::Result<File> {
        Self::acquire_lock(table_path, true)
    }

    /// Release a lock (unlock and drop the file handle)
    #[inline]
    fn release_lock(lock_file: File) {
        let _ = lock_file.unlock();
        drop(lock_file);
    }

    /// Parse a Python dict {col_name: type_str} into Vec<(String, ColumnType)>
    fn parse_schema_dict(
        dict: &Bound<'_, PyDict>,
    ) -> PyResult<Vec<(String, crate::storage::on_demand::ColumnType)>> {
        use crate::storage::on_demand::ColumnType;
        let mut cols = Vec::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let col_name: String = key.extract()?;
            let type_str: String = value.extract()?;
            let ct = match type_str.to_lowercase().as_str() {
                "int8" | "i8" => ColumnType::Int8,
                "int16" | "i16" => ColumnType::Int16,
                "int32" | "i32" | "int" => ColumnType::Int32,
                "int64" | "i64" | "integer" => ColumnType::Int64,
                "uint8" | "u8" => ColumnType::UInt8,
                "uint16" | "u16" => ColumnType::UInt16,
                "uint32" | "u32" => ColumnType::UInt32,
                "uint64" | "u64" => ColumnType::UInt64,
                "float32" | "f32" | "float" => ColumnType::Float32,
                "float64" | "f64" | "double" => ColumnType::Float64,
                "bool" | "boolean" => ColumnType::Bool,
                "str" | "string" | "text" | "varchar" => ColumnType::String,
                "bytes" | "binary" => ColumnType::Binary,
                "blob" | "large_binary" | "largebinary" => ColumnType::Blob,
                "float16_vector" | "float16vector" | "f16_vector" => ColumnType::Float16List,
                "timestamp" | "datetime" => ColumnType::Timestamp,
                "date" => ColumnType::Date,
                _ => return Err(PyValueError::new_err(format!(
                    "Unknown column type '{}' for column '{}'. Supported: int8, int16, int32, int64, \
                     uint8, uint16, uint32, uint64, float32, float64, bool, string, binary, blob, \
                     float16_vector, timestamp, date",
                    type_str, col_name
                ))),
            };
            cols.push((col_name, ct));
        }
        Ok(cols)
    }

    /// Get the path for the current table
    #[inline]
    fn get_current_table_path(&self) -> PyResult<PathBuf> {
        let table_name = self.current_table.read().clone();
        if table_name.is_empty() {
            return Err(PyValueError::new_err(
                "No table selected. Call create_table() or use_table() first.",
            ));
        }
        let paths = self.table_paths.read();
        if let Some(p) = paths.get(&table_name) {
            return Ok(p.clone());
        }
        drop(paths);
        // Lazy: check disk using current base_dir
        let base_dir = self.current_base_dir();
        let p = base_dir.join(format!("{}.apex", table_name));
        let registered = crate::storage::table_catalog::resolve(&base_dir, &table_name)
            .map(|path| path.is_some())
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        if registered {
            self.table_paths
                .write()
                .insert(table_name.clone(), p.clone());
            return Ok(p);
        }
        Err(PyValueError::new_err(format!(
            "Table not found: {}",
            table_name
        )))
    }

    /// Get both table path and name in one lock acquisition (optimization)
    #[inline]
    fn get_current_table_info(&self) -> PyResult<(PathBuf, String)> {
        let table_name = self.current_table.read().clone();
        if table_name.is_empty() {
            return Err(PyValueError::new_err(
                "No table selected. Call create_table() or use_table() first.",
            ));
        }
        let path = {
            let paths = self.table_paths.read();
            paths.get(&table_name).cloned()
        };
        if let Some(p) = path {
            return Ok((p, table_name));
        }
        // Lazy: check disk using current base_dir
        let base_dir = self.current_base_dir();
        let p = base_dir.join(format!("{}.apex", table_name));
        let registered = crate::storage::table_catalog::resolve(&base_dir, &table_name)
            .map(|path| path.is_some())
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        if registered {
            self.table_paths
                .write()
                .insert(table_name.clone(), p.clone());
            return Ok((p, table_name));
        }
        Err(PyValueError::new_err(format!(
            "Table not found: {}",
            table_name
        )))
    }

    /// Resolve the table path for a query-signature fast path.
    #[inline]
    fn resolve_signature_table(
        &self,
        explicit_table: Option<&str>,
        default_table_name: &str,
        default_table_path: &Path,
        base_dir: &Path,
    ) -> (String, PathBuf) {
        let clean_name = match explicit_table {
            Some(name) => name.trim_matches('"').trim_matches('`'),
            None => default_table_name,
        };

        if clean_name.is_empty() {
            return (
                default_table_name.to_string(),
                default_table_path.to_path_buf(),
            );
        }

        if let Some(dot_pos) = clean_name.find('.') {
            let db_name = clean_name[..dot_pos].trim();
            let tbl_name = clean_name[dot_pos + 1..].trim();
            let safe_tbl: String = tbl_name
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c == '_' || c == '-' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            let safe_tbl = if safe_tbl.len() > 200 {
                &safe_tbl[..200]
            } else {
                &safe_tbl
            };

            let db_dir = if db_name.is_empty() || db_name.eq_ignore_ascii_case("default") {
                self.root_dir.clone()
            } else {
                self.root_dir.join(db_name)
            };
            return (
                clean_name.to_string(),
                db_dir.join(format!("{}.apex", safe_tbl)),
            );
        }

        if clean_name.eq_ignore_ascii_case("default") || clean_name == default_table_name {
            return (clean_name.to_string(), default_table_path.to_path_buf());
        }

        let safe_name: String = clean_name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let safe_name = if safe_name.len() > 200 {
            &safe_name[..200]
        } else {
            &safe_name
        };
        (
            clean_name.to_string(),
            base_dir.join(format!("{}.apex", safe_name)),
        )
    }

    /// Get or create cached backend for current table
    /// Uses open_for_write to ensure existing data is loaded for write operations
    /// Get backend for INSERT operations - memory efficient!
    /// Uses open_for_insert which doesn't load existing column data.
    /// Data is written to delta file and merged on read.
    fn get_backend_for_insert(&self, py: Python<'_>) -> PyResult<Arc<TableStorageBackend>> {
        let table_name = self.current_table.read().clone();
        let table_path = self.get_current_table_path()?;
        let cache_key = Self::insert_backend_cache_key(&table_path, &table_name);

        // Check if backend is already cached (lock-free read)
        if let Some(entry) = self.cached_backends.get(&cache_key) {
            return Ok(entry.clone());
        }

        // Create new backend with open_for_insert (memory efficient)
        let backend = py
            .allow_threads(|| {
                if table_path.exists() {
                    crate::Database::open_insert_backend(&table_path, self.durability)
                } else {
                    crate::Database::create_backend(&table_path, self.durability)
                }
            })
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        self.cached_backends.insert(cache_key, backend.clone());

        Ok(backend)
    }

    /// Get a mmap/read backend suitable for fast overlay writes.
    fn get_backend_for_overlay(
        &self,
        py: Python<'_>,
        table_path: &Path,
        table_name: &str,
    ) -> PyResult<Arc<TableStorageBackend>> {
        let cache_key = Self::backend_cache_key(table_path, table_name);
        if let Some(entry) = self.cached_backends.get(&cache_key) {
            return Ok(entry.clone());
        }

        if let Ok(backend) = py.allow_threads(|| crate::Database::cached_backend(table_path)) {
            self.cached_backends.insert(cache_key, Arc::clone(&backend));
            return Ok(backend);
        }

        let backend = py
            .allow_threads(|| {
                if table_path.exists() {
                    crate::Database::open_backend(table_path, self.durability)
                } else {
                    crate::Database::create_backend(table_path, self.durability)
                }
            })
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        self.cached_backends.insert(cache_key, Arc::clone(&backend));
        crate::Database::cache_backend(table_path, Arc::clone(&backend));
        Ok(backend)
    }

    fn table_has_secondary_indexes(
        &self,
        py: Python<'_>,
        table_path: &Path,
        table_name: &str,
    ) -> bool {
        let base_dir = table_path
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .to_path_buf();
        py.allow_threads(|| {
            let catalog_path = base_dir
                .join("indexes")
                .join(format!("{}.idxcat", table_name));
            if !catalog_path.exists() {
                return false;
            }
            crate::storage::index::IndexManager::load(table_name, &base_dir)
                .map(|mgr| !mgr.catalog_is_empty())
                // A catalog that cannot be decoded must not allow an indexed
                // table onto a write path that bypasses index maintenance.
                .unwrap_or(true)
        })
    }

    #[inline]
    fn py_value_matches_exact(obj: &Bound<'_, PyAny>, stored: &Value) -> PyResult<bool> {
        use pyo3::types::PyBytes;

        if obj.is_none() {
            return Ok(matches!(stored, Value::Null));
        }

        if let Ok(value) = obj.extract::<bool>() {
            return Ok(matches!(stored, Value::Bool(current) if *current == value));
        }

        if let Ok(value) = obj.extract::<i64>() {
            return Ok(matches!(stored, Value::Int64(current) if *current == value));
        }

        if let Ok(value) = obj.extract::<f64>() {
            return Ok(matches!(stored, Value::Float64(current) if *current == value));
        }

        if obj.is_instance_of::<PyBytes>() {
            if let Ok(value) = obj.extract::<Vec<u8>>() {
                return Ok(matches!(stored, Value::Binary(current) if *current == value));
            }
        }

        if obj
            .get_type()
            .name()
            .map(|name| name == "ndarray")
            .unwrap_or(false)
        {
            if let Ok(value) = obj
                .call_method0("flatten")
                .and_then(|flat| flat.call_method1("astype", ("float32",)))
                .and_then(|f32arr| f32arr.call_method0("tobytes"))
                .and_then(|bytes| bytes.extract::<Vec<u8>>())
            {
                if value.len() % 4 == 0 {
                    return Ok(matches!(stored, Value::FixedList(current) if *current == value));
                }
            }
            return Ok(false);
        }

        if let Ok(value) = obj.extract::<String>() {
            return Ok(matches!(stored, Value::String(current) if *current == value));
        }

        if let Ok(value) = obj.extract::<Vec<u8>>() {
            return Ok(matches!(stored, Value::Binary(current) if *current == value));
        }

        Ok(matches!(stored, Value::Null))
    }

    fn py_dict_matches_exact_fields(
        data: &Bound<'_, PyDict>,
        fields: &HashMap<String, Value>,
    ) -> PyResult<bool> {
        let dict_len = data.len();
        if dict_len != fields.len() {
            if dict_len != fields.len() + 1 || data.get_item("_id").ok().flatten().is_none() {
                return Ok(false);
            }
        }

        for (name, stored) in fields {
            let Some(value) = data.get_item(name).ok().flatten() else {
                return Ok(false);
            };
            if !Self::py_value_matches_exact(&value, stored)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn row_matches_exact_py_dict(
        &self,
        backend: &TableStorageBackend,
        row_id: u64,
        data: &Bound<'_, PyDict>,
    ) -> PyResult<Option<bool>> {
        let schema = backend.storage.get_schema();
        if schema.is_empty() {
            return Ok(None);
        }

        let mut field_count = 0usize;
        for (key, _) in data.iter() {
            let key: String = key.extract()?;
            if key == "_id" {
                continue;
            }
            if !schema.iter().any(|(name, _)| name == &key) {
                return Ok(None);
            }
            field_count += 1;
        }
        if field_count != schema.len() {
            return Ok(None);
        }

        {
            let delta = backend.storage.delta_store();
            if delta.is_deleted(row_id) {
                return Ok(Some(false));
            }
            if let Some(updates) = delta.get_row_updates(row_id) {
                if schema.iter().all(|(name, _)| updates.contains_key(name)) {
                    for (name, _) in &schema {
                        let Some(value) = data.get_item(name).ok().flatten() else {
                            return Ok(None);
                        };
                        let Some(record) = updates.get(name) else {
                            return Ok(Some(false));
                        };
                        if !Self::py_value_matches_exact(&value, &record.new_value)? {
                            return Ok(Some(false));
                        }
                    }
                    return Ok(Some(true));
                }
            }
        }

        let mut current_row: HashMap<String, Value> = backend
            .storage
            .retrieve_rcix(row_id)
            .ok()
            .flatten()
            .or_else(|| backend.storage.read_row_by_id_values(row_id).ok().flatten())
            .map(|vals| vals.into_iter().collect())
            .unwrap_or_default();

        if current_row.is_empty() {
            return Ok(Some(false));
        }

        {
            let delta = backend.storage.delta_store();
            if let Some(updates) = delta.get_row_updates(row_id) {
                for (col_name, record) in updates {
                    current_row.insert(col_name.clone(), record.new_value.clone());
                }
            }
        }

        for (name, _) in &schema {
            let Some(value) = data.get_item(name).ok().flatten() else {
                return Ok(None);
            };
            let Some(current) = current_row.get(name) else {
                return Ok(Some(false));
            };
            if !Self::py_value_matches_exact(&value, current)? {
                return Ok(Some(false));
            }
        }

        Ok(Some(true))
    }

    /// Return `Some(true)` when the current stored row is already identical to
    /// the provided full-row payload. Returns `None` when we cannot cheaply
    /// determine equality (for example partial-row replacements).
    fn row_matches_exact_fields(
        &self,
        backend: &TableStorageBackend,
        row_id: u64,
        fields: &HashMap<String, Value>,
    ) -> PyResult<Option<bool>> {
        let schema = backend.storage.get_schema();
        if schema.is_empty()
            || schema.len() != fields.len()
            || schema.iter().any(|(name, _)| !fields.contains_key(name))
        {
            return Ok(None);
        }

        {
            let delta = backend.storage.delta_store();
            if delta.is_deleted(row_id) {
                return Ok(Some(false));
            }
            if let Some(updates) = delta.get_row_updates(row_id) {
                if schema.iter().all(|(name, _)| updates.contains_key(name)) {
                    return Ok(Some(schema.iter().all(|(name, _)| {
                        updates.get(name).map(|record| &record.new_value) == fields.get(name)
                    })));
                }
            }
        }

        let mut current_row: HashMap<String, Value> = backend
            .storage
            .retrieve_rcix(row_id)
            .ok()
            .flatten()
            .or_else(|| backend.storage.read_row_by_id_values(row_id).ok().flatten())
            .map(|vals| vals.into_iter().collect())
            .unwrap_or_default();

        if current_row.is_empty() {
            return Ok(Some(false));
        }

        {
            let delta = backend.storage.delta_store();
            if let Some(updates) = delta.get_row_updates(row_id) {
                for (col_name, record) in updates {
                    current_row.insert(col_name.clone(), record.new_value.clone());
                }
            }
        }

        Ok(Some(schema.iter().all(|(name, _)| {
            current_row.get(name) == fields.get(name)
        })))
    }

    fn persist_pending_overlay_for_table(
        &self,
        py: Python<'_>,
        table_path: &Path,
        table_name: &str,
    ) -> PyResult<()> {
        let mut backends: Vec<Arc<TableStorageBackend>> = Vec::new();
        let cache_key = Self::backend_cache_key(table_path, table_name);

        if let Some(entry) = self.cached_backends.get(&cache_key) {
            backends.push(entry);
        }
        if let Some(entry) = self.cached_backends.get(table_name) {
            let backend = entry;
            if !backends.iter().any(|b| Arc::ptr_eq(b, &backend)) {
                backends.push(backend);
            }
        }
        backends.retain(|backend| backend.has_pending_deltas());
        if backends.is_empty() {
            return Ok(());
        }

        py.allow_threads(|| -> PyResult<()> {
            for backend in backends {
                backend
                    .save_delta_store()
                    .map_err(|e| PyIOError::new_err(e.to_string()))?;
            }

            Ok(())
        })
    }

    /// Get backend for UPDATE/DELETE operations - loads all data into memory.
    /// This is required because save() rewrites the entire file.
    fn get_backend(&self) -> PyResult<Arc<TableStorageBackend>> {
        let table_name = self.current_table.read().clone();
        let table_path = self.get_current_table_path()?;
        let cache_key = Self::backend_cache_key(&table_path, &table_name);

        // Check if backend is already cached (lock-free read)
        if let Some(entry) = self.cached_backends.get(&cache_key) {
            return Ok(entry.clone());
        }

        // Create new backend with durability level and cache it
        // Use open_for_write to ensure existing column data is loaded
        // This is necessary because save() rewrites the entire file from in-memory columns
        let backend = if table_path.exists() {
            crate::Database::open_write_backend(&table_path, self.durability)
                .map_err(|e| PyIOError::new_err(e.to_string()))?
        } else {
            crate::Database::create_backend(&table_path, self.durability)
                .map_err(|e| PyIOError::new_err(e.to_string()))?
        };

        self.cached_backends.insert(cache_key, backend.clone());

        Ok(backend)
    }

    fn get_read_backend_cached(&self, py: Python<'_>) -> PyResult<Arc<TableStorageBackend>> {
        let (table_path, table_name) = self.get_current_table_info()?;
        let cache_key = Self::backend_cache_key(&table_path, &table_name);

        if let Some(entry) = self.cached_backends.get(&cache_key) {
            return Ok(entry.clone());
        }
        if let Ok(backend) = crate::Database::cached_backend(&table_path) {
            self.cached_backends.insert(cache_key, Arc::clone(&backend));
            return Ok(backend);
        }

        let backend = py
            .allow_threads(|| crate::Database::open_backend(&table_path, self.durability))
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        self.cached_backends.insert(cache_key, Arc::clone(&backend));
        crate::Database::cache_backend(&table_path, Arc::clone(&backend));
        Ok(backend)
    }

    fn blob_mode_to_str(mode: crate::storage::on_demand::BlobStorageMode) -> &'static str {
        match mode {
            crate::storage::on_demand::BlobStorageMode::Inline => "inline",
            crate::storage::on_demand::BlobStorageMode::Packed => "packed",
            crate::storage::on_demand::BlobStorageMode::Dedicated => "dedicated",
            crate::storage::on_demand::BlobStorageMode::External => "external",
        }
    }

    /// Invalidate cached backend for a table (used when table is dropped or modified externally)
    fn invalidate_backend(&self, table_name: &str) {
        self.cached_backends.remove(table_name);
        self.cached_backends
            .remove(&format!("{}_insert", table_name));
        let table_suffix = format!("\0{table_name}");
        let insert_suffix = format!("\0{table_name}\0insert");
        self.cached_backends
            .retain(|key| !(key.ends_with(&table_suffix) || key.ends_with(&insert_suffix)));
        let update_cache_marker = format!("\0{table_name}\0");
        let legacy_prefix = format!("{table_name}\0");
        self.update_by_id_numeric_cache.retain(|key, _| {
            !(key.starts_with(&legacy_prefix) || key.contains(&update_cache_marker))
        });
        self.update_by_id_cell_cache.retain(|key, _| {
            !(key.starts_with(&legacy_prefix) || key.contains(&update_cache_marker))
        });
        let replace_cache_marker = format!("\0{table_name}\0replace\0");
        self.replace_exact_row_cache
            .retain(|key, _| !key.contains(&replace_cache_marker));
        let clear_writer = self
            .schema_stable_memtable_writer
            .read()
            .as_ref()
            .map(|writer| writer.table_name == table_name)
            .unwrap_or(false);
        if clear_writer {
            *self.schema_stable_memtable_writer.write() = None;
        }
    }

    /// Release per-client cached backends before a statement replaces or
    /// removes a table file (TRUNCATE / INSERT OVERWRITE / DROP / ALTER).
    ///
    /// These backends hold open file mappings that the SQL executor's own
    /// cache invalidation cannot see. On Windows, truncating or rewriting a
    /// file while a user-mapped section is open fails with OS error 1224, so
    /// the per-client reference must be dropped BEFORE the DDL runs (the
    /// existing post-write invalidation is too late for file-replacing DDL).
    fn release_backends_for_file_replacing_sql(&self, sql: &str) {
        let s = sql.trim_start();
        let head = s.get(..16).unwrap_or(s).to_ascii_uppercase();
        let replaces_file = head.starts_with("TRUNCATE")
            || head.starts_with("INSERT OVERWRITE")
            || head.starts_with("DROP TABLE")
            || head.starts_with("ALTER TABLE")
            || (head.starts_with("BEGIN")
                && s.to_ascii_uppercase().contains("TRUNCATE"));
        if replaces_file {
            self.cached_backends.clear();
            self.flush_prewarm_tables.clear();
            *self.schema_stable_memtable_writer.write() = None;
        }
    }

    fn mark_flush_prewarm(&self, table_path: &Path, table_name: &str) {
        self.flush_prewarm_tables.insert(
            Self::backend_cache_key(table_path, table_name),
            table_path.to_path_buf(),
        );
    }

    fn prewarm_flushed_backend(&self, table_path: &Path, table_name: &str) {
        let cache_key = Self::backend_cache_key(table_path, table_name);
        if self.flush_prewarm_tables.remove(&cache_key).is_none() || !table_path.exists() {
            return;
        }
        if let Ok(backend) = crate::Database::open_backend(table_path, self.durability) {
            self.cached_backends.insert(cache_key, Arc::clone(&backend));
            crate::Database::cache_backend(table_path, backend);
        }
    }

    /// Return current base directory (root_dir for default db, root_dir/db for named db)
    #[inline]
    fn current_base_dir(&self) -> PathBuf {
        self.base_dir.read().clone()
    }
}

#[pymethods]
impl ApexStorageImpl {
    #[new]
    #[pyo3(signature = (path, drop_if_exists = false, durability = "fast"))]
    fn new(path: &str, drop_if_exists: bool, durability: &str) -> PyResult<Self> {
        // Parse durability level
        let durability_level = DurabilityLevel::from_str(durability).ok_or_else(|| {
            PyValueError::new_err(format!(
                "Invalid durability level '{}'. Must be 'fast', 'safe', or 'max'",
                durability
            ))
        })?;
        // Convert to absolute path to avoid issues with relative paths
        let path_obj = PathBuf::from(path);
        let abs_path = if path_obj.is_absolute() {
            path_obj
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(&path_obj)
        };
        let root_dir = abs_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));

        // Handle drop_if_exists
        if drop_if_exists {
            crate::Database::invalidate_dir(&root_dir);
            crate::Database::unregister_fts_manager(&root_dir);
            let _ = crate::storage::table_catalog::clear(&root_dir);
            crate::storage::on_demand::clear_pending_deletes_for_dir(&root_dir);

            // Remove all .apex files in the directory
            if let Ok(entries) = fs::read_dir(&root_dir) {
                for entry in entries.flatten() {
                    let p = entry.path();
                    if p.extension().map(|e| e == "apex").unwrap_or(false) {
                        let _ = fs::remove_file(&p);
                    }
                }
            }

            // Also remove FTS indexes
            let fts_dir = root_dir.join("fts_indexes");
            if fts_dir.exists() {
                let _ = fs::remove_dir_all(&fts_dir);
            }
        }

        // No default table - users must explicitly create or use a table
        // Existing .apex files in the directory are discovered lazily via use_table() or list_tables()

        let temp_dir = root_dir.join(".apex_tmp");
        let _ = fs::create_dir_all(&temp_dir);

        Ok(Self {
            root_dir: root_dir.clone(),
            current_database: RwLock::new(String::new()),
            base_dir: RwLock::new(root_dir),
            table_paths: RwLock::new(HashMap::new()),
            tables_scanned: RwLock::new(false),
            cached_backends: EpochBackendCache::new(),
            update_by_id_numeric_cache: DashMap::new(),
            update_by_id_cell_cache: DashMap::new(),
            replace_exact_row_cache: DashMap::new(),
            flush_prewarm_tables: DashMap::new(),
            current_table: RwLock::new(String::new()),
            fts_manager: RwLock::new(None::<Arc<FtsManager>>),
            fts_index_fields: RwLock::new(HashMap::new()),
            durability: durability_level,
            current_txn_id: RwLock::new(None),
            auto_flush_rows: RwLock::new(0),
            auto_flush_bytes: RwLock::new(0),
            temp_dir,
            schema_stable_memtable_writer: RwLock::new(None),
        })
    }

    // ========== Table Management ==========

    // ========== Multi-Database Operations ==========

    // ========== Retrieve Operations ==========

    // ========== Schema Operations ==========

    // ========== FTS Operations ==========
}

fn sampled_unique_count_i64(values: &[i64], n: usize) -> usize {
    let sample = n.min(512);
    let step = (n / sample.max(1)).max(1);
    let mut seen = ahash::AHashSet::with_capacity(sample);
    let mut index = 0usize;
    while index < n && seen.len() <= sample / 2 {
        seen.insert(values[index]);
        index = index.saturating_add(step);
    }
    seen.len()
}

fn sampled_unique_count_f64(values: &[f64], n: usize) -> usize {
    let sample = n.min(512);
    let step = (n / sample.max(1)).max(1);
    let mut seen = ahash::AHashSet::with_capacity(sample);
    let mut index = 0usize;
    while index < n && seen.len() <= sample / 2 {
        seen.insert(values[index].to_bits());
        index = index.saturating_add(step);
    }
    seen.len()
}

fn should_cache_repeated_numeric(sample_unique: usize, n: usize) -> bool {
    let sample = n.min(512);
    sample >= 64 && sample_unique.saturating_mul(4) <= sample
}

fn should_cache_repeated_strings(values: &arrow::array::StringArray, n: usize) -> bool {
    let sample = n.min(256);
    if sample < 32 {
        return false;
    }
    let step = (n / sample).max(1);
    let mut seen = ahash::AHashSet::with_capacity(sample);
    let mut index = 0usize;
    while index < n && seen.len() <= sample / 2 {
        seen.insert(values.value(index));
        index = index.saturating_add(step);
    }
    seen.len().saturating_mul(3) <= sample
}

fn arrow_col_to_pylist(py: Python<'_>, arr: &arrow::array::ArrayRef) -> PyResult<PyObject> {
    use arrow::array::*;
    use arrow::datatypes::DataType as ArrowDT;
    use pyo3::types::PyList;

    let n = arr.len();
    match arr.data_type() {
        ArrowDT::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            let has_nulls = a.null_count() > 0;
            if !has_nulls {
                let values = a.values();
                if should_cache_repeated_numeric(sampled_unique_count_i64(values, n), n) {
                    use pyo3::ffi;

                    let mut cache: ahash::AHashMap<i64, pyo3::PyObject> =
                        ahash::AHashMap::with_capacity(64);
                    let list_obj = unsafe {
                        let list_ptr = ffi::PyList_New(n as ffi::Py_ssize_t);
                        if list_ptr.is_null() {
                            return Err(pyo3::PyErr::fetch(py));
                        }
                        for i in 0..n {
                            let value = values[i];
                            let py_obj = match cache.get(&value) {
                                Some(obj) => obj.clone_ref(py),
                                None => {
                                    let obj = value.into_py(py);
                                    cache.insert(value, obj.clone_ref(py));
                                    obj
                                }
                            };
                            ffi::PyList_SET_ITEM(list_ptr, i as ffi::Py_ssize_t, py_obj.into_ptr());
                        }
                        pyo3::PyObject::from_owned_ptr(py, list_ptr)
                    };
                    Ok(list_obj.into())
                } else {
                    use pyo3::ffi;

                    let list_obj = unsafe {
                        let list_ptr = ffi::PyList_New(n as ffi::Py_ssize_t);
                        if list_ptr.is_null() {
                            return Err(pyo3::PyErr::fetch(py));
                        }
                        for (i, value) in values.iter().take(n).enumerate() {
                            let item = ffi::PyLong_FromLongLong(*value);
                            if item.is_null() {
                                ffi::Py_DECREF(list_ptr);
                                return Err(pyo3::PyErr::fetch(py));
                            }
                            ffi::PyList_SET_ITEM(list_ptr, i as ffi::Py_ssize_t, item);
                        }
                        pyo3::PyObject::from_owned_ptr(py, list_ptr)
                    };
                    Ok(list_obj.into())
                }
            } else {
                let list = PyList::empty_bound(py);
                for i in 0..n {
                    if a.is_null(i) {
                        list.append(py.None())?;
                    } else {
                        list.append(a.value(i))?;
                    }
                }
                Ok(list.into())
            }
        }
        ArrowDT::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            let has_nulls = a.null_count() > 0;
            if !has_nulls {
                let values = a.values();
                if should_cache_repeated_numeric(sampled_unique_count_f64(values, n), n) {
                    use pyo3::ffi;

                    let mut cache: ahash::AHashMap<u64, pyo3::PyObject> =
                        ahash::AHashMap::with_capacity(64);
                    let list_obj = unsafe {
                        let list_ptr = ffi::PyList_New(n as ffi::Py_ssize_t);
                        if list_ptr.is_null() {
                            return Err(pyo3::PyErr::fetch(py));
                        }
                        for i in 0..n {
                            let value = values[i];
                            let key = value.to_bits();
                            let py_obj = match cache.get(&key) {
                                Some(obj) => obj.clone_ref(py),
                                None => {
                                    let obj = value.into_py(py);
                                    cache.insert(key, obj.clone_ref(py));
                                    obj
                                }
                            };
                            ffi::PyList_SET_ITEM(list_ptr, i as ffi::Py_ssize_t, py_obj.into_ptr());
                        }
                        pyo3::PyObject::from_owned_ptr(py, list_ptr)
                    };
                    Ok(list_obj.into())
                } else {
                    use pyo3::ffi;

                    let list_obj = unsafe {
                        let list_ptr = ffi::PyList_New(n as ffi::Py_ssize_t);
                        if list_ptr.is_null() {
                            return Err(pyo3::PyErr::fetch(py));
                        }
                        for (i, value) in values.iter().take(n).enumerate() {
                            let item = ffi::PyFloat_FromDouble(*value);
                            if item.is_null() {
                                ffi::Py_DECREF(list_ptr);
                                return Err(pyo3::PyErr::fetch(py));
                            }
                            ffi::PyList_SET_ITEM(list_ptr, i as ffi::Py_ssize_t, item);
                        }
                        pyo3::PyObject::from_owned_ptr(py, list_ptr)
                    };
                    Ok(list_obj.into())
                }
            } else {
                let list = PyList::empty_bound(py);
                for i in 0..n {
                    if a.is_null(i) {
                        list.append(py.None())?;
                    } else {
                        list.append(a.value(i))?;
                    }
                }
                Ok(list.into())
            }
        }
        ArrowDT::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            if a.null_count() == 0 {
                if should_cache_repeated_strings(a, n) {
                    // Low-cardinality string columns benefit from interning and
                    // pre-sized list construction. High-cardinality columns like
                    // `name` are faster with the direct iterator path below.
                    use pyo3::ffi;

                    let mut cache: std::collections::HashMap<&str, pyo3::PyObject> =
                        std::collections::HashMap::with_capacity(32);
                    let list_obj = unsafe {
                        let list_ptr = ffi::PyList_New(n as ffi::Py_ssize_t);
                        if list_ptr.is_null() {
                            return Err(pyo3::PyErr::fetch(py));
                        }
                        for i in 0..n {
                            let s = a.value(i);
                            let py_obj: pyo3::PyObject = match cache.get(s) {
                                Some(o) => o.clone_ref(py),
                                None => {
                                    let o: pyo3::PyObject = s.into_py(py);
                                    cache.insert(s, o.clone_ref(py));
                                    o
                                }
                            };
                            ffi::PyList_SET_ITEM(list_ptr, i as ffi::Py_ssize_t, py_obj.into_ptr());
                        }
                        pyo3::PyObject::from_owned_ptr(py, list_ptr)
                    };
                    Ok(list_obj.into())
                } else {
                    use pyo3::ffi;
                    use std::ffi::c_char;

                    let list_obj = unsafe {
                        let list_ptr = ffi::PyList_New(n as ffi::Py_ssize_t);
                        if list_ptr.is_null() {
                            return Err(pyo3::PyErr::fetch(py));
                        }
                        for i in 0..n {
                            let s = a.value(i);
                            let item = ffi::PyUnicode_FromStringAndSize(
                                s.as_ptr() as *const c_char,
                                s.len() as ffi::Py_ssize_t,
                            );
                            if item.is_null() {
                                ffi::Py_DECREF(list_ptr);
                                return Err(pyo3::PyErr::fetch(py));
                            }
                            ffi::PyList_SET_ITEM(list_ptr, i as ffi::Py_ssize_t, item);
                        }
                        pyo3::PyObject::from_owned_ptr(py, list_ptr)
                    };
                    Ok(list_obj.into())
                }
            } else {
                let list = PyList::empty_bound(py);
                for i in 0..n {
                    if a.is_null(i) {
                        list.append(py.None())?;
                    } else {
                        let s = a.value(i);
                        if s == "\x00__NULL__\x00" {
                            list.append(py.None())?;
                        } else {
                            list.append(s)?;
                        }
                    }
                }
                Ok(list.into())
            }
        }
        ArrowDT::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            let list = PyList::empty_bound(py);
            for i in 0..n {
                if a.is_null(i) {
                    list.append(py.None())?;
                } else {
                    list.append(a.value(i))?;
                }
            }
            Ok(list.into())
        }
        _ => {
            // Fallback: per-element generic path
            let list = PyList::empty_bound(py);
            for i in 0..n {
                list.append(value_to_py(py, &arrow_value_at(arr, i))?)?;
            }
            Ok(list.into())
        }
    }
}

fn arrow_value_at(array: &arrow::array::ArrayRef, idx: usize) -> Value {
    use arrow::array::*;
    use arrow::datatypes::DataType as ArrowDataType;

    if array.is_null(idx) {
        return Value::Null;
    }

    match array.data_type() {
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Int64(arr.value(idx))
        }
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Int64(arr.value(idx) as i64)
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Value::Float64(arr.value(idx))
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let s = arr.value(idx);
            // Check for NULL marker
            if s == "\x00__NULL__\x00" {
                Value::Null
            } else {
                Value::String(s.to_string())
            }
        }
        ArrowDataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(arr.value(idx))
        }
        ArrowDataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::UInt64(arr.value(idx))
        }
        ArrowDataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Value::Binary(arr.value(idx).to_vec())
        }
        ArrowDataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Value::Blob(arr.value(idx).to_vec())
        }
        ArrowDataType::Dictionary(_, _) => {
            // Handle DictionaryArray<UInt32Type> with Utf8 values
            use arrow::datatypes::UInt32Type;
            if let Some(dict_arr) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
                if dict_arr.is_null(idx) {
                    Value::Null
                } else {
                    let key = dict_arr.keys().value(idx) as usize;
                    let values = dict_arr.values();
                    if let Some(str_values) = values.as_any().downcast_ref::<StringArray>() {
                        if key < str_values.len() {
                            let s = str_values.value(key);
                            if s == "\x00__NULL__\x00" {
                                Value::Null
                            } else {
                                Value::String(s.to_string())
                            }
                        } else {
                            Value::Null
                        }
                    } else {
                        Value::Null
                    }
                }
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}
