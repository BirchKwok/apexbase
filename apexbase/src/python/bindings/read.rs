//! PyO3 binding methods split by domain.

use super::*;
use arrow::array::StringArray;

#[pymethods]
impl ApexStorageImpl {
    fn _table_epoch(&self) -> PyResult<u64> {
        let table_path = self.get_current_table_path()?;
        Ok(crate::storage::epoch::current(&table_path))
    }

    fn row_count(&self, py: Python<'_>) -> PyResult<u64> {
        let table_path = self.get_current_table_path()?;
        // If file doesn't exist (e.g., after drop_if_exists), return 0
        if !py.allow_threads(|| table_path.exists()) {
            return Ok(0);
        }

        let table_name = self.current_table.read().clone();
        let cache_key = Self::backend_cache_key(&table_path, &table_name);
        if let Some(backend) = self.cached_backends.get(&cache_key) {
            return Ok(backend.active_row_count());
        }

        // No file lock needed — active_count is atomic and always consistent
        let count = py.allow_threads(|| {
            crate::Database::active_row_count(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })?;

        Ok(count)
    }

    fn count_rows(&self, py: Python<'_>) -> PyResult<u64> {
        self.row_count(py)
    }

    fn has_pending_memtable_rows(&self, py: Python<'_>) -> PyResult<bool> {
        let table_name = self.current_table.read().clone();
        if table_name.is_empty() {
            return Ok(false);
        }
        let table_path = self.get_current_table_path()?;
        let mut backends: Vec<Arc<TableStorageBackend>> = Vec::new();
        for cache_key in [
            Self::backend_cache_key(&table_path, &table_name),
            Self::insert_backend_cache_key(&table_path, &table_name),
        ] {
            if let Some(entry) = self.cached_backends.get(&cache_key) {
                let backend = entry;
                if !backends.iter().any(|cached| Arc::ptr_eq(cached, &backend)) {
                    backends.push(backend);
                }
            }
        }
        Ok(py.allow_threads(|| {
            backends
                .iter()
                .any(|backend| backend.pending_v4_in_memory_rows() > 0)
        }))
    }

    fn has_pending_overlay_writes(&self, py: Python<'_>) -> PyResult<bool> {
        let table_name = self.current_table.read().clone();
        if table_name.is_empty() {
            return Ok(false);
        }

        let table_path = self.get_current_table_path()?;
        let mut backends: Vec<Arc<TableStorageBackend>> = Vec::new();
        for cache_key in [
            Self::backend_cache_key(&table_path, &table_name),
            Self::insert_backend_cache_key(&table_path, &table_name),
        ] {
            if let Some(entry) = self.cached_backends.get(&cache_key) {
                let backend = entry;
                if !backends.iter().any(|cached| Arc::ptr_eq(cached, &backend)) {
                    backends.push(backend);
                }
            }
        }

        Ok(py.allow_threads(|| {
            backends.iter().any(|backend| {
                backend.is_dirty()
                    || backend.has_pending_deltas()
                    || backend.pending_v4_in_memory_rows() > 0
            })
        }))
    }

    fn resolve_table_path_for_count(&self, table_name: &str) -> PyResult<PathBuf> {
        let clean = table_name.trim_matches('"').trim_matches('`');
        // Check per-instance path cache first
        {
            let paths = self.table_paths.read();
            if let Some(p) = paths.get(clean) {
                return Ok(p.clone());
            }
        }
        let base_dir = self.current_base_dir();
        // Check temp dir first (temp tables shadow persistent)
        if let Some(temp_dir) = crate::Database::temp_dir() {
            let temp_path = temp_dir.join(format!("{}.apex", clean));
            if temp_path.exists() {
                let mut paths = self.table_paths.write();
                paths.insert(clean.to_string(), temp_path.clone());
                return Ok(temp_path);
            }
        }
        // Try base_dir
        let p = base_dir.join(format!("{}.apex", clean));
        if p.exists() {
            let mut paths = self.table_paths.write();
            paths.insert(clean.to_string(), p.clone());
            return Ok(p);
        }
        Err(PyValueError::new_err(format!("Table not found: {}", clean)))
    }

    fn fast_row_count_for(&self, py: Python<'_>, table_name: &str) -> PyResult<u64> {
        let table_path = self.resolve_table_path_for_count(table_name)?;
        let cache_key = Self::backend_cache_key(&table_path, table_name);

        // Per-instance backend cache (no stat(), no delta check)
        if let Some(backend) = self.cached_backends.get(&cache_key) {
            let backend = Arc::clone(&backend);
            return Ok(backend.active_row_count());
        }

        // Global cache fallback
        if let Ok(backend) = py.allow_threads(|| crate::Database::cached_backend(&table_path)) {
            self.cached_backends
                .insert(cache_key.clone(), Arc::clone(&backend));
            return Ok(backend.active_row_count());
        }

        // Engine fallback
        let count = py.allow_threads(|| {
            crate::Database::active_row_count(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })?;
        Ok(count)
    }

    fn fast_row_count(&self, py: Python<'_>) -> PyResult<u64> {
        let table_name = self.current_table.read().clone();
        if table_name.is_empty() {
            return Err(PyValueError::new_err(
                "No table selected. Call create_table() or use_table() first.",
            ));
        }
        self.fast_row_count_for(py, &table_name)
    }

    fn get_durability(&self) -> String {
        self.durability.as_str().to_string()
    }

    fn get_auto_flush(&self) -> PyResult<(u64, u64)> {
        Ok((*self.auto_flush_rows.read(), *self.auto_flush_bytes.read()))
    }

    fn estimate_memory_bytes(&self, py: Python<'_>) -> PyResult<u64> {
        let table_name = self.current_table.read().clone();
        let table_path = self.get_current_table_path()?;
        let cache_key = Self::backend_cache_key(&table_path, &table_name);
        if let Some(backend) = self.cached_backends.get(&cache_key) {
            let mem = backend.estimate_memory_bytes();
            if mem > 0 {
                return Ok(mem);
            }
        }
        // No in-memory data (flushed to disk): estimate from file size
        if let Ok(meta) = py.allow_threads(|| std::fs::metadata(&table_path)) {
            return Ok(meta.len());
        }
        Ok(0)
    }

    fn get_compression(&self, py: Python<'_>) -> PyResult<String> {
        use crate::storage::on_demand::OnDemandStorage;
        let table_path = self.get_current_table_path()?;
        py.allow_threads(|| {
            if table_path.exists() {
                let storage = OnDemandStorage::open_with_durability(&table_path, self.durability)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(storage.compression().as_str().to_string())
            } else {
                Ok("none".to_string())
            }
        })
    }

    fn retrieve(&self, py: Python<'_>, id: i64) -> PyResult<Option<PyObject>> {
        let table_path = self.get_current_table_path()?;

        if id < 0 {
            return Ok(None);
        }

        // ULTRA-FAST PATH: Direct V4 value read - no file lock, no Arrow, no GIL release
        // Skip allow_threads() for sub-0.1ms operations where GIL overhead dominates
        // Use per-instance cached_backends first: no stat() syscalls (~600µs saved vs get_cached_backend_pub).
        let table_name = self.current_table.read().clone();
        let cache_key = Self::backend_cache_key(&table_path, &table_name);
        let maybe_cached = { self.cached_backends.get(&cache_key).map(|v| Arc::clone(&v)) };
        let backend_opt: Option<Arc<TableStorageBackend>> = if let Some(b) = maybe_cached {
            Some(b)
        } else if let Ok(b) = crate::Database::cached_backend(&table_path) {
            // Populate per-instance cache so next call is zero-syscall
            self.cached_backends.insert(cache_key, Arc::clone(&b));
            Some(b)
        } else {
            None
        };
        if let Some(backend) = backend_opt {
            let id_u64 = id as u64;
            if id_u64 >= backend.next_id_value() && !backend.has_delta() {
                return Ok(None);
            }
            if backend.has_pending_deltas() {
                // Pending UPDATE overlays are applied by the Arrow executor fallback below.
            } else {
                if backend.pending_v4_in_memory_rows() > 0 {
                    let vals_result =
                        py.allow_threads(|| backend.storage.read_row_by_id_values(id_u64));
                    return match vals_result {
                        Ok(Some(vals)) => {
                            let dict = PyDict::new_bound(py);
                            for (k, v) in vals {
                                dict.set_item(k, value_to_py(py, &v)?)?;
                            }
                            Ok(Some(dict.into()))
                        }
                        Ok(None) => Ok(None),
                        Err(_) => Ok(None),
                    };
                }
                // Release GIL for all Rust computation; re-acquire only for PyDict construction.
                // retrieve_rcix: page-cached RCIX read, handles PLAIN/BITPACK/RLE/StringDict.
                let rcix_result = py.allow_threads(|| backend.storage.retrieve_rcix(id_u64));
                if let Ok(Some(vals)) = rcix_result {
                    let dict = PyDict::new_bound(py);
                    for (k, v) in vals {
                        dict.set_item(k, value_to_py(py, &v)?)?;
                    }
                    return Ok(Some(dict.into()));
                }
                // Fallback: may need to (re)create mmap after save_v4 invalidation
                let vals_result =
                    py.allow_threads(|| backend.storage.read_row_by_id_values(id_u64));
                if let Ok(Some(vals)) = vals_result {
                    let dict = PyDict::new_bound(py);
                    for (k, v) in vals {
                        dict.set_item(k, value_to_py(py, &v)?)?;
                    }
                    return Ok(Some(dict.into()));
                }
                // Arrow batch cache path: O(1) index lookup + batch.slice(idx, 1)
                let batch_result = py.allow_threads(|| backend.read_row_by_id_to_arrow(id_u64));
                if let Ok(Some(batch)) = batch_result {
                    if batch.num_rows() > 0 {
                        let dict = PyDict::new_bound(py);
                        let schema = batch.schema();
                        for col_idx in 0..batch.num_columns() {
                            let col_name = schema.field(col_idx).name();
                            let val = arrow_value_at(batch.column(col_idx), 0);
                            dict.set_item(col_name.as_str(), value_to_py(py, &val)?)?;
                        }
                        return Ok(Some(dict.into()));
                    }
                }
            }
        }

        let base_dir = self.current_base_dir();
        let root_dir = self.root_dir.clone();
        let table_name = self.current_table.read().clone();

        let result = py.allow_threads(|| -> PyResult<Option<HashMap<String, Value>>> {
            // FALLBACK: File lock + Arrow path for edge cases
            let lock_file = Self::acquire_read_lock(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            let result: PyResult<Option<HashMap<String, Value>>> = (|| {
                let sql = format!("SELECT * FROM \"{}\" WHERE _id = {}", table_name, id);
                let result = crate::Session::new(&base_dir, &table_path)
                    .with_root_dir(&root_dir)
                    .execute(&sql)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                let batch = result
                    .to_record_batch()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                if batch.num_rows() == 0 {
                    return Ok(None);
                }

                let mut row_data = HashMap::new();
                for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                    let val = arrow_value_at(batch.column(col_idx), 0);
                    row_data.insert(field.name().clone(), val);
                }
                Ok(Some(row_data))
            })();
            Self::release_lock(lock_file);
            result
        });

        let result = result?;

        match result {
            None => Ok(None),
            Some(row_data) => {
                let dict = PyDict::new_bound(py);
                for (k, v) in row_data {
                    dict.set_item(k, value_to_py(py, &v)?)?;
                }
                Ok(Some(dict.into()))
            }
        }
    }

    fn retrieve_projected(
        &self,
        py: Python<'_>,
        id: i64,
        columns: Vec<String>,
    ) -> PyResult<Option<PyObject>> {
        if id < 0 || columns.is_empty() {
            return Ok(None);
        }
        let table_name = self.current_table.read().clone();
        if table_name.is_empty() {
            return Err(PyValueError::new_err(
                "No table selected. Call create_table() or use_table() first.",
            ));
        }
        let backend_opt: Option<Arc<TableStorageBackend>> = {
            let legacy_cached = self
                .cached_backends
                .get(&table_name)
                .map(|v| Arc::clone(&v));
            if legacy_cached.is_some() {
                legacy_cached
            } else {
                let table_path = self.get_current_table_path().ok();
                if let Some(table_path) = table_path {
                    let cache_key = Self::backend_cache_key(&table_path, &table_name);
                    let keyed_cached = self.cached_backends.get(&cache_key).map(|v| Arc::clone(&v));
                    if let Some(backend) = keyed_cached {
                        self.cached_backends
                            .insert(table_name.clone(), Arc::clone(&backend));
                        Some(backend)
                    } else {
                        crate::Database::cached_backend(&table_path).ok().map(|b| {
                            self.cached_backends
                                .insert(cache_key.clone(), Arc::clone(&b));
                            self.cached_backends
                                .insert(table_name.clone(), Arc::clone(&b));
                            b
                        })
                    }
                } else {
                    None
                }
            }
        };

        let Some(backend) = backend_opt else {
            return Ok(None);
        };
        if backend.has_pending_deltas()
            || backend.has_delta()
            || backend.pending_v4_in_memory_rows() > 0
        {
            return Ok(None);
        }

        let requested_cols: Vec<&str> = columns.iter().map(String::as_str).collect();
        let vals_result = backend
            .storage
            .retrieve_rcix_projected(id as u64, &requested_cols)
            .ok()
            .flatten()
            .or_else(|| {
                backend
                    .storage
                    .read_row_by_id_values(id as u64)
                    .ok()
                    .flatten()
            });
        let Some(vals) = vals_result else {
            return Ok(None);
        };

        let out = PyDict::new_bound(py);
        let Some(columns_dict) = projected_values_to_columns_dict(py, &vals, &columns)? else {
            return Ok(None);
        };
        out.set_item("columns_dict", columns_dict)?;
        out.set_item("rows_affected", 0i64)?;
        Ok(Some(out.into()))
    }

    fn retrieve_first_by_string_eq_limit1(
        &self,
        py: Python<'_>,
        column: String,
        value: String,
    ) -> PyResult<Option<PyObject>> {
        if column.is_empty() {
            return Ok(None);
        }
        let (table_path, table_name) = self.get_current_table_info()?;
        let cache_key = Self::backend_cache_key(&table_path, &table_name);
        let backend_opt: Option<Arc<TableStorageBackend>> = self
            .cached_backends
            .get(&cache_key)
            .map(|v| Arc::clone(&v))
            .or_else(|| {
                crate::Database::cached_backend(&table_path).ok().map(|b| {
                    self.cached_backends
                        .insert(cache_key.clone(), Arc::clone(&b));
                    b
                })
            });

        let Some(backend) = backend_opt else {
            return Ok(None);
        };
        if backend.has_pending_deltas()
            || backend.has_delta()
            || backend.pending_v4_in_memory_rows() > 0
        {
            return Ok(None);
        }

        let Some(row_id) = backend
            .first_row_id_for_string_eq(&column, &value)
            .ok()
            .flatten()
        else {
            return Ok(None);
        };

        let vals_result = backend
            .storage
            .retrieve_rcix(row_id)
            .ok()
            .flatten()
            .or_else(|| backend.storage.read_row_by_id_values(row_id).ok().flatten());
        let Some(vals) = vals_result else {
            return Ok(None);
        };

        let out = PyDict::new_bound(py);
        let columns_dict = values_to_columns_dict(py, &vals)?;
        out.set_item("columns_dict", columns_dict)?;
        out.set_item("rows_affected", 0i64)?;
        Ok(Some(out.into()))
    }

    fn retrieve_projected_first_by_string_eq_limit1(
        &self,
        py: Python<'_>,
        filter_column: String,
        value: String,
        columns: Vec<String>,
    ) -> PyResult<Option<PyObject>> {
        if filter_column.is_empty() || columns.is_empty() {
            return Ok(None);
        }
        let (table_path, table_name) = self.get_current_table_info()?;
        let cache_key = Self::backend_cache_key(&table_path, &table_name);
        let backend_opt: Option<Arc<TableStorageBackend>> = self
            .cached_backends
            .get(&cache_key)
            .map(|v| Arc::clone(&v))
            .or_else(|| {
                crate::Database::cached_backend(&table_path).ok().map(|b| {
                    self.cached_backends
                        .insert(cache_key.clone(), Arc::clone(&b));
                    b
                })
            });

        let Some(backend) = backend_opt else {
            return Ok(None);
        };
        if backend.has_pending_deltas() || backend.pending_v4_in_memory_rows() > 0 {
            return Ok(None);
        }

        let Some(row_id) = backend
            .first_row_id_for_string_eq(&filter_column, &value)
            .ok()
            .flatten()
        else {
            return Ok(None);
        };

        let requested_cols: Vec<&str> = columns.iter().map(String::as_str).collect();
        let vals_result = backend
            .storage
            .retrieve_rcix_projected(row_id, &requested_cols)
            .ok()
            .flatten()
            .or_else(|| backend.storage.read_row_by_id_values(row_id).ok().flatten());
        let Some(vals) = vals_result else {
            return Ok(None);
        };

        let Some(columns_dict) = projected_values_to_columns_dict(py, &vals, &columns)? else {
            return Ok(None);
        };

        let out = PyDict::new_bound(py);
        out.set_item("columns_dict", columns_dict)?;
        out.set_item("rows_affected", 0i64)?;
        Ok(Some(out.into()))
    }

    fn retrieve_projected_by_string_eq_limit(
        &self,
        py: Python<'_>,
        filter_column: String,
        value: String,
        columns: Vec<String>,
        limit: usize,
        offset: usize,
    ) -> PyResult<Option<PyObject>> {
        if filter_column.is_empty() || columns.is_empty() {
            return Ok(None);
        }

        let (table_path, table_name) = self.get_current_table_info()?;
        let cache_key = Self::backend_cache_key(&table_path, &table_name);
        let backend_opt: Option<Arc<TableStorageBackend>> = self
            .cached_backends
            .get(&cache_key)
            .map(|v| Arc::clone(&v))
            .or_else(|| {
                crate::Database::cached_backend(&table_path).ok().map(|b| {
                    self.cached_backends
                        .insert(cache_key.clone(), Arc::clone(&b));
                    b
                })
            });

        let Some(backend) = backend_opt else {
            return Ok(None);
        };
        if backend.has_pending_deltas() || backend.pending_v4_in_memory_rows() > 0 {
            return Ok(None);
        }

        let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        let needed = offset.saturating_add(limit);
        if limit == 0 {
            let out = PyDict::new_bound(py);
            let columns_dict = PyDict::new_bound(py);
            for col_name in &columns {
                columns_dict.set_item(col_name.as_str(), PyList::empty_bound(py))?;
            }
            out.set_item("columns_dict", columns_dict)?;
            out.set_item("rows_affected", 0i64)?;
            return Ok(Some(out.into()));
        }

        let indices_result = py.allow_threads(|| -> io::Result<Option<Vec<usize>>> {
            let Some(indices) =
                backend.scan_string_filter_mmap(&filter_column, &value, Some(needed))?
            else {
                return Ok(None);
            };
            Ok(Some(indices.into_iter().skip(offset).take(limit).collect()))
        });

        let Some(final_indices) = indices_result.map_err(|e| PyIOError::new_err(e.to_string()))?
        else {
            return Ok(None);
        };

        if final_indices.is_empty() {
            let out = PyDict::new_bound(py);
            let columns_dict = PyDict::new_bound(py);
            for col_name in &columns {
                columns_dict.set_item(col_name.as_str(), PyList::empty_bound(py))?;
            }
            out.set_item("columns_dict", columns_dict)?;
            out.set_item("rows_affected", 0i64)?;
            return Ok(Some(out.into()));
        }

        let cols_result = py.allow_threads(|| {
            backend
                .storage
                .extract_rows_by_indices_mmap_columns(&final_indices, Some(col_refs.as_slice()))
        });
        if let Some(batch_cols) = cols_result.map_err(|e| PyIOError::new_err(e.to_string()))? {
            if let Some(columns_dict) =
                mmap_batch_columns_to_pydict(py, batch_cols, Some(&columns))?
            {
                let out = PyDict::new_bound(py);
                out.set_item("columns_dict", columns_dict)?;
                out.set_item("rows_affected", 0i64)?;
                return Ok(Some(out.into()));
            }
        }

        let batch_result = py.allow_threads(|| -> io::Result<Option<RecordBatch>> {
            if final_indices.is_empty() {
                backend
                    .read_columns_to_arrow(Some(col_refs.as_slice()), 0, Some(0))
                    .map(Some)
            } else {
                backend
                    .read_columns_by_indices_to_arrow(&final_indices, Some(col_refs.as_slice()))
                    .map(Some)
            }
        });

        let Some(batch) = batch_result.map_err(|e| PyIOError::new_err(e.to_string()))? else {
            return Ok(None);
        };

        let out = PyDict::new_bound(py);
        let columns_dict = PyDict::new_bound(py);
        if batch.num_rows() == 0 {
            for col_name in &columns {
                columns_dict.set_item(col_name.as_str(), PyList::empty_bound(py))?;
            }
        } else {
            let schema = batch.schema();
            for col_idx in 0..batch.num_columns() {
                let col_name = schema.field(col_idx).name();
                let arr = batch.column(col_idx);
                let col_list = arrow_col_to_pylist(py, arr)?;
                columns_dict.set_item(col_name, col_list)?;
            }
        }
        out.set_item("columns_dict", columns_dict)?;
        out.set_item("rows_affected", 0i64)?;
        Ok(Some(out.into()))
    }

    fn retrieve_by_numeric_range_limit(
        &self,
        py: Python<'_>,
        filter_column: String,
        op: String,
        value: f64,
        limit: usize,
        offset: usize,
    ) -> PyResult<Option<PyObject>> {
        if filter_column.is_empty() {
            return Ok(None);
        }

        let (low, high) = match op.as_str() {
            "=" => (value, value),
            ">" => (next_up_f64_binding(value), f64::INFINITY),
            ">=" => (value, f64::INFINITY),
            "<" => (f64::NEG_INFINITY, next_down_f64_binding(value)),
            "<=" => (f64::NEG_INFINITY, value),
            _ => return Ok(None),
        };

        let (table_path, table_name) = self.get_current_table_info()?;
        let cache_key = Self::backend_cache_key(&table_path, &table_name);
        let backend_opt: Option<Arc<TableStorageBackend>> = self
            .cached_backends
            .get(&cache_key)
            .map(|v| Arc::clone(&v))
            .or_else(|| {
                crate::Database::cached_backend(&table_path).ok().map(|b| {
                    self.cached_backends
                        .insert(cache_key.clone(), Arc::clone(&b));
                    b
                })
            });

        let Some(backend) = backend_opt else {
            return Ok(None);
        };
        if backend.has_pending_deltas()
            || backend.has_delta()
            || backend.pending_v4_in_memory_rows() > 0
        {
            return Ok(None);
        }

        let needed = offset.saturating_add(limit);
        let cols_result = py.allow_threads(
            || -> io::Result<Option<crate::storage::on_demand::MmapBatchColumns>> {
                let Some(mut indices) =
                    backend.scan_numeric_range_mmap(&filter_column, low, high, Some(needed))?
                else {
                    return Ok(None);
                };
                if offset == 0 {
                    indices.truncate(limit);
                    backend
                        .storage
                        .extract_rows_by_indices_mmap_columns(&indices, None)
                } else {
                    let final_indices: Vec<usize> =
                        indices.into_iter().skip(offset).take(limit).collect();
                    backend
                        .storage
                        .extract_rows_by_indices_mmap_columns(&final_indices, None)
                }
            },
        );

        let Some(batch_cols) = cols_result.map_err(|e| PyIOError::new_err(e.to_string()))? else {
            return Ok(None);
        };
        let Some(columns_dict) = mmap_batch_columns_to_pydict(py, batch_cols, None)? else {
            return Ok(None);
        };

        let out = PyDict::new_bound(py);
        out.set_item("columns_dict", columns_dict)?;
        out.set_item("rows_affected", 0i64)?;
        Ok(Some(out.into()))
    }

    fn execute_filtered_numeric_agg(
        &self,
        py: Python<'_>,
        sql: String,
        table: String,
        filter_column: String,
        op: String,
        value: f64,
    ) -> PyResult<Option<PyObject>> {
        if table.is_empty() || filter_column.is_empty() {
            return Ok(None);
        }

        let agg_exprs = match parse_agg_select(&sql) {
            Some(exprs) if !exprs.is_empty() => exprs,
            _ => return Ok(None),
        };

        let (low, high) = match op.as_str() {
            "=" => (value, value),
            ">" => (next_up_f64_binding(value), f64::INFINITY),
            ">=" => (value, f64::INFINITY),
            "<" => (f64::NEG_INFINITY, next_down_f64_binding(value)),
            "<=" => (f64::NEG_INFINITY, value),
            _ => return Ok(None),
        };

        let (default_table_path, default_table_name) = self.get_current_table_info()?;
        let base_dir = self.current_base_dir();
        let target_table = table.trim_matches('"').trim_matches('`').to_string();
        let target_path =
            if target_table.eq_ignore_ascii_case("default") || target_table == default_table_name {
                default_table_path
            } else {
                self.table_paths
                    .read()
                    .get(&target_table)
                    .cloned()
                    .unwrap_or_else(|| base_dir.join(format!("{}.apex", target_table)))
            };

        let cache_key = Self::backend_cache_key(&target_path, &target_table);
        let backend_opt: Option<Arc<TableStorageBackend>> = self
            .cached_backends
            .get(&cache_key)
            .map(|v| Arc::clone(&v))
            .or_else(|| {
                crate::Database::cached_backend(&target_path).ok().map(|b| {
                    self.cached_backends
                        .insert(cache_key.clone(), Arc::clone(&b));
                    b
                })
            });

        let Some(backend) = backend_opt else {
            return Ok(None);
        };
        if backend.has_pending_deltas()
            || backend.has_delta()
            || backend.pending_v4_in_memory_rows() > 0
        {
            return Ok(None);
        }

        let mut unique_cols: Vec<String> = Vec::new();
        for (func, col, _) in &agg_exprs {
            let is_count_star = func == "COUNT"
                && col
                    .as_ref()
                    .map(|c| {
                        c == "*"
                            || c.chars()
                                .next()
                                .map(|ch| ch.is_ascii_digit())
                                .unwrap_or(false)
                    })
                    .unwrap_or(true);
            if is_count_star {
                if !unique_cols.iter().any(|c| c == "*") {
                    unique_cols.push("*".to_string());
                }
            } else if let Some(c) = col {
                if !unique_cols.contains(c) {
                    unique_cols.push(c.clone());
                }
            }
        }
        if unique_cols.is_empty() {
            return Ok(None);
        }

        let col_refs: Vec<&str> = unique_cols.iter().map(String::as_str).collect();
        let agg_result = py.allow_threads(|| {
            backend.execute_filtered_numeric_agg_mmap(&filter_column, low, high, &col_refs)
        });
        let Some(stats) = agg_result.map_err(|e| PyIOError::new_err(e.to_string()))? else {
            return Ok(None);
        };

        let mut stat_map: std::collections::HashMap<&str, (i64, f64, f64, f64, bool)> =
            std::collections::HashMap::new();
        for (idx, col_name) in col_refs.iter().enumerate() {
            if idx < stats.len() {
                stat_map.insert(col_name, stats[idx]);
            }
        }
        let match_count = stat_map.get("*").map(|s| s.0).unwrap_or(0);

        let out = PyDict::new_bound(py);
        let columns_dict = PyDict::new_bound(py);
        for (func, col, alias) in &agg_exprs {
            let output_name = if let Some(a) = alias {
                a.clone()
            } else if let Some(c) = col {
                format!("{}({})", func, c)
            } else {
                format!("{}(*)", func)
            };

            match func.as_str() {
                "COUNT" => {
                    let count = if let Some(c) = col {
                        let is_count_star = c == "*"
                            || c.chars()
                                .next()
                                .map(|ch| ch.is_ascii_digit())
                                .unwrap_or(false);
                        if is_count_star {
                            match_count
                        } else {
                            stat_map.get(c.as_str()).map(|s| s.0).unwrap_or(0)
                        }
                    } else {
                        match_count
                    };
                    columns_dict.set_item(&output_name, PyList::new_bound(py, &[count]))?;
                }
                "SUM" | "AVG" | "MIN" | "MAX" => {
                    if let Some(c) = col {
                        let (count, sum, min_v, max_v, is_int) = stat_map
                            .get(c.as_str())
                            .copied()
                            .unwrap_or((0, 0.0, 0.0, 0.0, false));
                        match func.as_str() {
                            "SUM" => {
                                if is_int {
                                    columns_dict.set_item(
                                        &output_name,
                                        PyList::new_bound(py, &[sum as i64]),
                                    )?;
                                } else {
                                    columns_dict
                                        .set_item(&output_name, PyList::new_bound(py, &[sum]))?;
                                }
                            }
                            "AVG" => {
                                let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                                columns_dict
                                    .set_item(&output_name, PyList::new_bound(py, &[avg]))?;
                            }
                            "MIN" => {
                                if is_int {
                                    columns_dict.set_item(
                                        &output_name,
                                        PyList::new_bound(py, &[min_v as i64]),
                                    )?;
                                } else {
                                    columns_dict
                                        .set_item(&output_name, PyList::new_bound(py, &[min_v]))?;
                                }
                            }
                            "MAX" => {
                                if is_int {
                                    columns_dict.set_item(
                                        &output_name,
                                        PyList::new_bound(py, &[max_v as i64]),
                                    )?;
                                } else {
                                    columns_dict
                                        .set_item(&output_name, PyList::new_bound(py, &[max_v]))?;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => return Ok(None),
            }
        }
        out.set_item("columns_dict", columns_dict)?;
        out.set_item("rows_affected", 0i64)?;
        Ok(Some(out.into()))
    }

    fn retrieve_projected_row(
        &self,
        py: Python<'_>,
        id: i64,
        columns: Vec<String>,
    ) -> PyResult<Option<PyObject>> {
        if id < 0 || columns.is_empty() {
            return Ok(None);
        }
        let (table_path, table_name) = self.get_current_table_info()?;
        let cache_key = Self::backend_cache_key(&table_path, &table_name);
        let backend_opt: Option<Arc<TableStorageBackend>> = self
            .cached_backends
            .get(&cache_key)
            .map(|v| Arc::clone(&v))
            .or_else(|| {
                crate::Database::cached_backend(&table_path).ok().map(|b| {
                    self.cached_backends
                        .insert(cache_key.clone(), Arc::clone(&b));
                    b
                })
            });

        let Some(backend) = backend_opt else {
            return Ok(None);
        };
        if backend.has_pending_deltas() || backend.pending_v4_in_memory_rows() > 0 {
            return Ok(None);
        }

        let requested_cols: Vec<&str> = columns.iter().map(String::as_str).collect();
        let vals_result = backend
            .storage
            .retrieve_rcix_projected(id as u64, &requested_cols)
            .ok()
            .flatten()
            .or_else(|| {
                backend
                    .storage
                    .read_row_by_id_values(id as u64)
                    .ok()
                    .flatten()
            });
        let Some(vals) = vals_result else {
            return Ok(None);
        };

        let Some(row) = projected_values_to_row_dict(py, &vals, &columns)? else {
            return Ok(None);
        };
        Ok(Some(row.into()))
    }

    fn retrieve_many_projected(
        &self,
        py: Python<'_>,
        ids: Vec<i64>,
        columns: Vec<String>,
    ) -> PyResult<Option<PyObject>> {
        if ids.is_empty() || columns.is_empty() {
            return Ok(None);
        }
        let (table_path, table_name) = self.get_current_table_info()?;
        let backend_opt: Option<Arc<TableStorageBackend>> = self
            .cached_backends
            .get(&table_name)
            .map(|v| Arc::clone(&v))
            .or_else(|| {
                crate::Database::cached_backend(&table_path).ok().map(|b| {
                    self.cached_backends
                        .insert(table_name.clone(), Arc::clone(&b));
                    b
                })
            });

        let Some(backend) = backend_opt else {
            return Ok(None);
        };
        if backend.has_pending_deltas() || backend.has_delta() {
            return Ok(None);
        }

        let mut sorted_ids: Vec<u64> = ids
            .into_iter()
            .filter(|id| *id >= 0)
            .map(|id| id as u64)
            .collect();
        if sorted_ids.is_empty() {
            return Ok(None);
        }
        sorted_ids.sort_unstable();
        sorted_ids.dedup();

        if backend.storage.is_v4_format() && !backend.storage.has_v4_in_memory_data() {
            if let Ok(Some(batch_cols)) = backend.storage.retrieve_many_mmap_columns(&sorted_ids) {
                let row_count = batch_cols.row_count;
                if row_count != sorted_ids.len() {
                    return Ok(None);
                }
                let Some(columns_dict) =
                    mmap_batch_columns_to_pydict(py, batch_cols, Some(&columns))?
                else {
                    return Ok(None);
                };
                let out = PyDict::new_bound(py);
                out.set_item("columns_dict", columns_dict)?;
                out.set_item("rows_affected", row_count as i64)?;
                return Ok(Some(out.into()));
            }
        }

        let mut col_values: Vec<Vec<PyObject>> = (0..columns.len())
            .map(|_| Vec::with_capacity(sorted_ids.len()))
            .collect();
        let expected_rows = sorted_ids.len();
        let mut matched_rows = 0usize;

        for id in sorted_ids {
            let vals_result = backend
                .storage
                .retrieve_rcix(id)
                .ok()
                .flatten()
                .or_else(|| backend.storage.read_row_by_id_values(id).ok().flatten());
            let Some(vals) = vals_result else {
                continue;
            };

            let mut row_values: Vec<PyObject> = Vec::with_capacity(columns.len());
            for requested_col in &columns {
                let Some((_, val)) = vals.iter().find(|(col_name, _)| col_name == requested_col)
                else {
                    return Ok(None);
                };
                row_values.push(value_to_py(py, val)?);
            }
            for (idx, value) in row_values.into_iter().enumerate() {
                col_values[idx].push(value);
            }
            matched_rows += 1;
        }

        if matched_rows != expected_rows {
            return Ok(None);
        }

        let columns_dict = PyDict::new_bound(py);
        for (idx, requested_col) in columns.iter().enumerate() {
            columns_dict.set_item(
                requested_col.as_str(),
                PyList::new_bound(py, &col_values[idx]),
            )?;
        }

        let out = PyDict::new_bound(py);
        out.set_item("columns_dict", columns_dict)?;
        out.set_item("rows_affected", matched_rows as i64)?;
        Ok(Some(out.into()))
    }

    fn retrieve_many(&self, py: Python<'_>, ids: Vec<i64>) -> PyResult<PyObject> {
        use pyo3::types::PyDict;

        if ids.is_empty() {
            let out = PyDict::new_bound(py);
            out.set_item("columns_dict", PyDict::new_bound(py))?;
            out.set_item("rows_affected", 0i64)?;
            return Ok(out.into());
        }

        let (table_path, table_name) = self.get_current_table_info()?;

        // Try to get cached backend for direct storage access
        let maybe_cached = self
            .cached_backends
            .get(&table_name)
            .map(|v| Arc::clone(&v));
        let backend_opt: Option<Arc<TableStorageBackend>> = if let Some(b) = maybe_cached {
            Some(b)
        } else if let Ok(b) = crate::Database::cached_backend(&table_path) {
            self.cached_backends
                .insert(table_name.clone(), Arc::clone(&b));
            Some(b)
        } else {
            None
        };

        // Use direct storage batch read (one mmap pass per RG, no per-row lock overhead)
        if let Some(backend) = backend_opt {
            let ids_u64: Vec<u64> = ids.iter().map(|&id| id as u64).collect();

            // Fast path: direct mmap-to-Python columns — one footer lock + one mmap slice per RG.
            let batch_cols_opt = if !backend.has_pending_deltas()
                && !backend.has_delta()
                && backend.storage.is_v4_format()
                && !backend.storage.has_v4_in_memory_data()
            {
                backend
                    .storage
                    .retrieve_many_mmap_columns(&ids_u64)
                    .ok()
                    .flatten()
            } else {
                None
            };

            if let Some(batch_cols) = batch_cols_opt {
                let row_count = batch_cols.row_count;
                if row_count == ids_u64.len() {
                    let columns_dict = mmap_batch_columns_to_pydict(py, batch_cols, None)?
                        .unwrap_or_else(|| PyDict::new_bound(py));
                    let out = PyDict::new_bound(py);
                    out.set_item("columns_dict", columns_dict)?;
                    out.set_item("rows_affected", row_count as i64)?;
                    return Ok(out.into());
                }
            }

            // Fallback: per-row retrieve_rcix (non-RCIX RGs)
            let mut all_rows: Vec<Vec<(String, Value)>> = Vec::with_capacity(ids_u64.len());
            if !backend.has_pending_deltas() && !backend.has_delta() {
                for &id in &ids_u64 {
                    if let Ok(Some(row)) = backend.storage.retrieve_rcix(id) {
                        all_rows.push(row);
                    }
                }
            }

            if all_rows.len() != ids_u64.len() {
                let batch = py
                    .allow_threads(|| backend.read_rows_by_ids_to_arrow(&ids_u64))
                    .map_err(|e| PyIOError::new_err(e.to_string()))?;
                let columns_dict = PyDict::new_bound(py);
                let schema = batch.schema();
                for col_idx in 0..batch.num_columns() {
                    let col_name = schema.field(col_idx).name();
                    let col_list = arrow_col_to_pylist(py, batch.column(col_idx))?;
                    columns_dict.set_item(col_name, col_list)?;
                }
                let out = PyDict::new_bound(py);
                out.set_item("columns_dict", columns_dict)?;
                out.set_item("rows_affected", batch.num_rows() as i64)?;
                return Ok(out.into());
            }

            if all_rows.is_empty() {
                let out = PyDict::new_bound(py);
                out.set_item("columns_dict", PyDict::new_bound(py))?;
                out.set_item("rows_affected", 0i64)?;
                return Ok(out.into());
            }

            let num_rows = all_rows.len();
            let col_names: Vec<String> = all_rows[0].iter().map(|(n, _)| n.clone()).collect();
            let num_cols = col_names.len();

            let columns_dict = PyDict::new_bound(py);
            for col_idx in 0..num_cols {
                let col_name = &col_names[col_idx];
                let mut py_list: Vec<PyObject> = Vec::with_capacity(num_rows);
                for row in &all_rows {
                    let val = value_to_py(py, &row[col_idx].1)?;
                    py_list.push(val);
                }
                let py_list_bound = PyList::new_bound(py, &py_list);
                columns_dict.set_item(col_name.as_str(), py_list_bound)?;
            }

            let out = PyDict::new_bound(py);
            out.set_item("columns_dict", columns_dict)?;
            out.set_item("rows_affected", num_rows as i64)?;
            return Ok(out.into());
        }

        // Fallback: empty result
        let out = PyDict::new_bound(py);
        out.set_item("columns_dict", PyDict::new_bound(py))?;
        out.set_item("rows_affected", 0i64)?;
        Ok(out.into())
    }

    fn retrieve_all(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        let (table_path, table_name) = self.get_current_table_info()?;

        let rows = py.allow_threads(|| -> PyResult<Vec<HashMap<String, Value>>> {
            let sql = format!("SELECT * FROM {}", table_name);
            let sql = sql.as_str();
            let result = crate::Session::new(&table_path, &table_path)
                .execute(sql)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let batch = result
                .to_record_batch()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let mut rows = Vec::with_capacity(batch.num_rows());
            for row_idx in 0..batch.num_rows() {
                let mut row_data = HashMap::new();
                for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                    let val = arrow_value_at(batch.column(col_idx), row_idx);
                    row_data.insert(field.name().clone(), val);
                }
                rows.push(row_data);
            }

            Ok(rows)
        })?;

        let mut result = Vec::with_capacity(rows.len());
        for row_data in rows {
            let dict = PyDict::new_bound(py);
            for (k, v) in row_data {
                dict.set_item(k, value_to_py(py, &v)?)?;
            }
            result.push(dict.into());
        }

        Ok(result)
    }

    fn list_fields(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let table_path = self.get_current_table_path()?;

        py.allow_threads(|| -> PyResult<Vec<String>> {
            // Acquire shared read lock
            let lock_file = Self::acquire_read_lock(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            // Use StorageEngine for unified list_columns
            let result = crate::Database::columns(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()));

            Self::release_lock(lock_file);
            result
        })
    }

    fn get_column_dtype(&self, py: Python<'_>, column_name: &str) -> PyResult<Option<String>> {
        let table_path = self.get_current_table_path()?;

        let column_name = column_name.to_string();
        py.allow_threads(|| -> PyResult<Option<String>> {
            // Acquire shared read lock
            let lock_file = Self::acquire_read_lock(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            // Use StorageEngine for unified get_column_type
            let result = crate::Database::column_type(&table_path, &column_name)
                .map(|dtype| dtype.map(|dt| format!("{:?}", dt)))
                .map_err(|e| PyIOError::new_err(e.to_string()));

            Self::release_lock(lock_file);
            result
        })
    }

    #[pyo3(name = "_is_fts_enabled")]
    fn is_fts_enabled(&self) -> bool {
        self.fts_manager.read().is_some()
    }

    #[pyo3(name = "_get_fts_config")]
    fn get_fts_config(&self) -> Option<Vec<String>> {
        let table_name = self.current_table.read().clone();
        self.fts_index_fields.read().get(&table_name).cloned()
    }

    #[pyo3(signature = (query, limit=None))]
    fn search_text(
        &self,
        py: Python<'_>,
        query: &str,
        limit: Option<usize>,
    ) -> PyResult<Vec<(i64, f32)>> {
        let table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();
        let mgr = self.fts_manager.read();

        if let Some(m) = mgr.as_ref() {
            let engine = m
                .get_engine(&table_name)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            // Release GIL during search for better concurrency
            let results = py.allow_threads(|| {
                crate::Database::wait_fts_backfill(&base_dir, &table_name);
                engine
                    .search_ranked(query, limit.unwrap_or(100))
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })?;
            Ok(results
                .into_iter()
                .map(|hit| (hit.doc_id as i64, hit.score))
                .collect())
        } else {
            Err(PyRuntimeError::new_err("FTS not initialized"))
        }
    }

    #[pyo3(signature = (query, limit=None, min_results=1, max_distance=None))]
    fn fuzzy_search_text(
        &self,
        py: Python<'_>,
        query: &str,
        limit: Option<usize>,
        min_results: usize,
        max_distance: Option<usize>,
    ) -> PyResult<Vec<(i64, f32)>> {
        let table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();
        let mgr = self.fts_manager.read();

        if let Some(m) = mgr.as_ref() {
            let engine = m
                .get_engine(&table_name)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            // Release GIL during fuzzy search for better concurrency
            let ids: Vec<u64> = py.allow_threads(|| -> PyResult<Vec<u64>> {
                crate::Database::wait_fts_backfill(&base_dir, &table_name);
                if let Some(max_distance) = max_distance {
                    engine.set_fuzzy_config(0.7, max_distance, 20);
                }
                let result = engine
                    .fuzzy_search(query, min_results)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                // Convert result handle to Vec<u64>
                Ok(result
                    .page(0, limit.unwrap_or(100))
                    .into_iter()
                    .map(|id| id as u64)
                    .collect())
            })?;
            Ok(ids.into_iter().map(|id| (id as i64, 1.0f32)).collect())
        } else {
            Err(PyRuntimeError::new_err("FTS not initialized"))
        }
    }

    #[pyo3(signature = (query, limit=None, offset=0))]
    fn search_and_retrieve(
        &self,
        py: Python<'_>,
        query: &str,
        limit: Option<usize>,
        offset: usize,
    ) -> PyResult<PyObject> {
        let requested = limit.unwrap_or(100);
        let results = self.search_text(py, query, Some(offset.saturating_add(requested)))?;
        let ids: Vec<i64> = results
            .into_iter()
            .skip(offset)
            .take(requested)
            .map(|(id, _)| id)
            .collect();
        self.retrieve_many(py, ids)
    }

    #[pyo3(name = "_fts_warmup")]
    fn fts_warmup(&self, terms: Vec<String>) -> PyResult<usize> {
        let table_name = self.current_table.read().clone();
        if let Some(manager) = self.fts_manager.read().as_ref() {
            return manager
                .get_engine(&table_name)
                .map(|engine| engine.warmup_terms(&terms))
                .map_err(|e| PyRuntimeError::new_err(e.to_string()));
        }
        Ok(0)
    }

    fn get_fts_stats(&self, py: Python<'_>) -> PyResult<Option<(usize, usize)>> {
        let table_name = self.current_table.read().clone();
        let mgr = self.fts_manager.read().clone();

        if let Some(m) = mgr {
            Ok(py.allow_threads(|| {
                if let Ok(engine) = m.get_engine(&table_name) {
                    let stats = engine.stats();
                    let doc_count = stats.get("doc_count").copied().unwrap_or(0) as usize;
                    let term_count = stats.get("term_count").copied().unwrap_or(0) as usize;
                    Some((doc_count, term_count))
                } else {
                    None
                }
            }))
        } else {
            Ok(None)
        }
    }
}
