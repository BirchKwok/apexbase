//! PyO3 binding methods split by domain.

use super::*;
use crate::storage::ColumnData;

#[pymethods]
impl ApexStorageImpl {
    fn has_secondary_indexes(&self, py: Python<'_>) -> PyResult<bool> {
        let (table_path, table_name) = self.get_current_table_info()?;
        Ok(self.table_has_secondary_indexes(py, &table_path, &table_name))
    }

    fn store(&self, py: Python<'_>, data: &Bound<'_, PyDict>) -> PyResult<i64> {
        let fields = dict_to_values(data)?;
        let (table_path, table_name) = self.get_current_table_info()?;
        let _epoch_write = crate::storage::epoch::logical_write(&table_path);
        let durability = self.durability;
        self.persist_pending_overlay_for_table(py, &table_path, &table_name)?;

        let id = py.allow_threads(|| -> PyResult<i64> {
            // The in-process locks do not protect independent Python workers.
            // Every durability mode therefore participates in the same OS lock.
            let lock_file = Self::acquire_write_lock(&table_path)
                        .map_err(|e| PyIOError::new_err(e.to_string()))?;

            let result = crate::Database::write(&table_path, &[fields], durability)
                .map(|ids| ids.first().copied().unwrap_or(0) as i64)
                .map_err(|e| PyIOError::new_err(e.to_string()));

            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }

            result
        })?;

        // Invalidate local backend cache (StorageEngine handles its own cache)
        self.invalidate_backend(&table_name);
        self.mark_flush_prewarm(&table_path, &table_name);

        // Index in FTS if enabled
        self.index_for_fts(py, id, data)?;

        Ok(id)
    }

    fn store_batch(&self, py: Python<'_>, data: &Bound<'_, PyList>) -> PyResult<Vec<i64>> {
        let num_rows = data.len();
        if num_rows == 0 {
            return Ok(Vec::new());
        }

        // Collect all rows
        let mut rows: Vec<HashMap<String, Value>> = Vec::with_capacity(num_rows);
        for item in data.iter() {
            let dict = item.downcast::<PyDict>()?;
            let fields = dict_to_values(dict)?;
            rows.push(fields);
        }

        let (table_path, table_name) = self.get_current_table_info()?;
        let _epoch_write = crate::storage::epoch::logical_write(&table_path);
        let durability = self.durability;
        self.persist_pending_overlay_for_table(py, &table_path, &table_name)?;

        let ids = py.allow_threads(|| -> PyResult<Vec<u64>> {
            let lock_file = Self::acquire_write_lock(&table_path)
                        .map_err(|e| PyIOError::new_err(e.to_string()))?;

            let result = crate::Database::write(&table_path, &rows, durability)
                .map_err(|e| PyIOError::new_err(e.to_string()));

            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }

            result
        })?;

        // Invalidate local backend cache
        self.invalidate_backend(&table_name);
        self.mark_flush_prewarm(&table_path, &table_name);

        // Index in FTS if enabled (batch operation - only if FTS manager exists)
        // OPTIMIZED: Use add_documents_arrow_str (🥈 ~3.3M docs/s, zero-copy &str path)
        {
            let mgr = self.fts_manager.read();
            if mgr.is_some() {
                let table_name = self.current_table.read().clone();
                let base_dir = self.current_base_dir();
                let index_fields = self.fts_index_fields.read().get(&table_name).cloned();

                if let Some(m) = mgr.as_ref() {
                    if let Ok(engine) = m.get_engine(&table_name) {
                        // Determine which fields to index
                        let fields_to_index: Vec<String> = match &index_fields {
                            Some(fields) => fields.clone(),
                            None => {
                                // Auto-detect string fields from first document
                                let mut auto_fields = Vec::new();
                                if let Some(first_item) = data.iter().next() {
                                    if let Ok(dict) = first_item.downcast::<PyDict>() {
                                        for (key, value) in dict.iter() {
                                            if let Ok(key_str) = key.extract::<String>() {
                                                if key_str != "_id"
                                                    && value.extract::<String>().is_ok()
                                                {
                                                    auto_fields.push(key_str);
                                                }
                                            }
                                        }
                                    }
                                }
                                auto_fields
                            }
                        };

                        if !fields_to_index.is_empty() {
                            let num_docs = ids.len();
                            // Build columnar String data — direct per-field lookup, no per-doc HashMap
                            let mut columns: Vec<(String, Vec<String>)> = fields_to_index
                                .iter()
                                .map(|f| (f.clone(), Vec::with_capacity(num_docs)))
                                .collect();

                            for (i, item) in data.iter().enumerate() {
                                if i >= ids.len() {
                                    break;
                                }
                                if let Ok(dict) = item.downcast::<PyDict>() {
                                    for (field_idx, field_name) in
                                        fields_to_index.iter().enumerate()
                                    {
                                        let value = dict
                                            .get_item(field_name)
                                            .ok()
                                            .flatten()
                                            .and_then(|v| v.extract::<String>().ok())
                                            .unwrap_or_default();
                                        columns[field_idx].1.push(value);
                                    }
                                }
                            }

                            // 🥈 add_documents_arrow_str: zero-copy &str slices, ~3.3M docs/s
                            if !columns.is_empty() && !columns[0].1.is_empty() {
                                let doc_ids_u64: Vec<u64> = ids.clone();
                                let columns_ref: Vec<(String, Vec<&str>)> = columns
                                    .iter()
                                    .map(|(name, vals)| {
                                        (name.clone(), vals.iter().map(|s| s.as_str()).collect())
                                    })
                                    .collect();
                                let _ = py.allow_threads(|| {
                                    crate::Database::wait_fts_backfill(&base_dir, &table_name);
                                    engine.add_documents_arrow_str(&doc_ids_u64, columns_ref)
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(ids.into_iter().map(|id| id as i64).collect())
    }

    fn store_one(&self, py: Python<'_>, row: &Bound<'_, PyDict>) -> PyResult<Vec<i64>> {
        if row.is_empty() {
            return Ok(Vec::new());
        }

        let mut int_columns: HashMap<String, Vec<i64>> = HashMap::new();
        let mut float_columns: HashMap<String, Vec<f64>> = HashMap::new();
        let mut string_columns: HashMap<String, Vec<String>> = HashMap::new();
        let mut binary_columns_map: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut fixedlist_columns_map: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut bool_columns: HashMap<String, Vec<bool>> = HashMap::new();
        let mut null_positions: HashMap<String, Vec<bool>> = HashMap::new();

        for (key, value) in row.iter() {
            let col_name: String = key.extract()?;
            if col_name == "_id" {
                continue;
            }

            if value.is_none() {
                string_columns.insert(col_name.clone(), vec![String::new()]);
                null_positions.insert(col_name, vec![true]);
            } else if let Ok(v) = value.extract::<bool>() {
                bool_columns.insert(col_name.clone(), vec![v]);
                null_positions.insert(col_name, vec![false]);
            } else if let Ok(v) = value.extract::<i64>() {
                int_columns.insert(col_name.clone(), vec![v]);
                null_positions.insert(col_name, vec![false]);
            } else if let Ok(v) = value.extract::<f64>() {
                float_columns.insert(col_name.clone(), vec![v]);
                null_positions.insert(col_name, vec![false]);
            } else if let Ok(bytes) = value.extract::<Vec<u8>>() {
                binary_columns_map.insert(col_name.clone(), vec![bytes]);
                null_positions.insert(col_name, vec![false]);
            } else if value
                .get_type()
                .name()
                .map(|n| n == "ndarray")
                .unwrap_or(false)
            {
                if let Ok(bytes) = value
                    .call_method0("flatten")
                    .and_then(|flat| flat.call_method1("astype", ("float32",)))
                    .and_then(|f32arr| f32arr.call_method0("tobytes"))
                    .and_then(|b| b.extract::<Vec<u8>>())
                {
                    fixedlist_columns_map.insert(col_name.clone(), vec![bytes]);
                    null_positions.insert(col_name, vec![false]);
                } else {
                    string_columns.insert(
                        col_name.clone(),
                        vec![value.extract::<String>().unwrap_or_default()],
                    );
                    null_positions.insert(col_name, vec![false]);
                }
            } else {
                string_columns.insert(
                    col_name.clone(),
                    vec![value.extract::<String>().unwrap_or_default()],
                );
                null_positions.insert(col_name, vec![false]);
            }
        }

        if int_columns.is_empty()
            && float_columns.is_empty()
            && string_columns.is_empty()
            && binary_columns_map.is_empty()
            && fixedlist_columns_map.is_empty()
            && bool_columns.is_empty()
        {
            return Ok(Vec::new());
        }

        let (table_path, table_name) = self.get_current_table_info()?;
        let durability = self.durability;
        self.persist_pending_overlay_for_table(py, &table_path, &table_name)?;
        let result = py.allow_threads(|| {
            crate::Database::write_typed(
                &table_path,
                int_columns,
                float_columns,
                string_columns,
                binary_columns_map,
                fixedlist_columns_map,
                bool_columns,
                null_positions,
                durability,
            )
            .map_err(|e| PyIOError::new_err(e.to_string()))
        })?;

        self.invalidate_backend(&table_name);
        #[cfg(target_os = "windows")]
        crate::Database::invalidate(&table_path);

        Ok(result.into_iter().map(|id| id as i64).collect())
    }

    fn store_one_memtable(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyDict>,
    ) -> PyResult<Option<Vec<i64>>> {
        if row.is_empty() {
            return Ok(Some(Vec::new()));
        }
        if let Some(ids) = self.try_cached_schema_stable_memtable_insert(row)? {
            return Ok(Some(ids));
        }

        let (table_path, table_name) = self.get_current_table_info()?;
        if self.table_has_secondary_indexes(py, &table_path, &table_name) {
            return Ok(None);
        }

        self.persist_pending_overlay_for_table(py, &table_path, &table_name)?;
        let backend = self.get_backend_for_insert(py)?;
        if !backend.storage.is_v4_format() || backend.storage.has_constraints() {
            return Ok(None);
        }

        let schema = backend.storage.get_schema();
        if schema.is_empty() {
            return Ok(None);
        }
        if schema.iter().any(|(_, ty)| {
            matches!(
                ty,
                crate::storage::on_demand::ColumnType::FixedList
                    | crate::storage::on_demand::ColumnType::Float16List
                    | crate::storage::on_demand::ColumnType::BFloat16List
                    | crate::storage::on_demand::ColumnType::Int8Vector
                    | crate::storage::on_demand::ColumnType::UInt8Vector
                    | crate::storage::on_demand::ColumnType::Bit1Vector
                    | crate::storage::on_demand::ColumnType::TurboQuant2Vector
                    | crate::storage::on_demand::ColumnType::TurboQuant3Vector
                    | crate::storage::on_demand::ColumnType::TurboQuant4Vector
                    | crate::storage::on_demand::ColumnType::Null
            )
        }) {
            return Ok(None);
        }

        if let Some(id) = Self::try_insert_schema_stable_borrowed_row(row, &schema, &backend)? {
            let cache_key = Self::backend_cache_key(&table_path, &table_name);
            self.cached_backends.insert(cache_key, Arc::clone(&backend));
            crate::Database::cache_backend(&table_path, Arc::clone(&backend));
            crate::query::planner::invalidate_table_stats(&table_path.to_string_lossy());

            let database = self.current_database.read().clone();
            *self.schema_stable_memtable_writer.write() = Some(SchemaStableMemtableWriter {
                database,
                table_name: table_name.clone(),
                table_path: table_path.clone(),
                backend: Arc::clone(&backend),
                schema: schema.clone(),
            });

            return Ok(Some(vec![id as i64]));
        }

        let schema_len = schema.len();
        let mut int_columns: HashMap<String, Vec<i64>> = HashMap::new();
        let mut float_columns: HashMap<String, Vec<f64>> = HashMap::new();
        let mut string_columns: HashMap<String, Vec<String>> = HashMap::new();
        let mut binary_columns_map: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let fixedlist_columns_map: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut bool_columns: HashMap<String, Vec<bool>> = HashMap::new();
        let mut null_positions: HashMap<String, Vec<bool>> = HashMap::new();
        let mut field_count = 0usize;

        for (key, value) in row.iter() {
            let col_name: String = key.extract()?;
            if col_name == "_id" {
                continue;
            }
            field_count += 1;
            let Some((_, col_type)) = schema.iter().find(|(name, _)| name == &col_name) else {
                return Ok(None);
            };

            use crate::storage::on_demand::ColumnType;
            match *col_type {
                ColumnType::Bool => {
                    let v = if value.is_none() {
                        false
                    } else if let Ok(v) = value.extract::<bool>() {
                        v
                    } else {
                        return Ok(None);
                    };
                    bool_columns.insert(col_name.clone(), vec![v]);
                    null_positions.insert(col_name, vec![value.is_none()]);
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
                    let v = if value.is_none() {
                        0
                    } else if let Ok(v) = value.extract::<i64>() {
                        v
                    } else {
                        return Ok(None);
                    };
                    int_columns.insert(col_name.clone(), vec![v]);
                    null_positions.insert(col_name, vec![value.is_none()]);
                }
                ColumnType::Float32 | ColumnType::Float64 => {
                    let v = if value.is_none() {
                        0.0
                    } else if let Ok(v) = value.extract::<f64>() {
                        v
                    } else {
                        match value.extract::<i64>() {
                            Ok(v) => v as f64,
                            Err(_) => return Ok(None),
                        }
                    };
                    float_columns.insert(col_name.clone(), vec![v]);
                    null_positions.insert(col_name, vec![value.is_none()]);
                }
                ColumnType::String | ColumnType::StringDict => {
                    let v = if value.is_none() {
                        String::new()
                    } else if let Ok(v) = value.extract::<String>() {
                        v
                    } else {
                        return Ok(None);
                    };
                    string_columns.insert(col_name.clone(), vec![v]);
                    null_positions.insert(col_name, vec![value.is_none()]);
                }
                ColumnType::Binary => {
                    let v = if value.is_none() {
                        Vec::new()
                    } else if let Ok(v) = value.extract::<Vec<u8>>() {
                        v
                    } else {
                        return Ok(None);
                    };
                    binary_columns_map.insert(col_name.clone(), vec![v]);
                    null_positions.insert(col_name, vec![value.is_none()]);
                }
                ColumnType::Blob
                | ColumnType::FixedList
                | ColumnType::Float16List
                | ColumnType::BFloat16List
                | ColumnType::Int8Vector
                | ColumnType::UInt8Vector
                | ColumnType::Bit1Vector
                | ColumnType::TurboQuant2Vector
                | ColumnType::TurboQuant3Vector
                | ColumnType::TurboQuant4Vector
                | ColumnType::Null => {
                    return Ok(None);
                }
            }
        }

        if field_count != schema_len {
            return Ok(None);
        }

        let result = py.allow_threads(|| {
            backend
                .insert_typed_with_nulls_full(
                    int_columns,
                    float_columns,
                    string_columns,
                    binary_columns_map,
                    fixedlist_columns_map,
                    bool_columns,
                    null_positions,
                )
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })?;

        let cache_key = Self::backend_cache_key(&table_path, &table_name);
        self.cached_backends.insert(cache_key, Arc::clone(&backend));
        crate::Database::cache_backend(&table_path, Arc::clone(&backend));
        crate::query::planner::invalidate_table_stats(&table_path.to_string_lossy());

        Ok(Some(result.into_iter().map(|id| id as i64).collect()))
    }

    fn store_one_delta(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyDict>,
    ) -> PyResult<Option<Vec<i64>>> {
        if row.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let fields = dict_to_column_values(row)?;
        if fields.is_empty() {
            return Ok(Some(Vec::new()));
        }
        if fields.values().any(|value| {
            matches!(
                value,
                ColumnValue::Null
                    | ColumnValue::Binary(_)
                    | ColumnValue::Blob(_)
                    | ColumnValue::FixedList(_)
            )
        }) {
            return Ok(None);
        }

        let (table_path, table_name) = self.get_current_table_info()?;
        // Keep indexed tables on the existing path until index maintenance for
        // delta-only rows is fully covered.
        if self.table_has_secondary_indexes(py, &table_path, &table_name) {
            return Ok(None);
        }

        self.persist_pending_overlay_for_table(py, &table_path, &table_name)?;
        let backend = self.get_backend_for_insert(py)?;

        if !backend.storage.is_v4_format() || backend.storage.has_constraints() {
            return Ok(None);
        }

        let schema = backend.storage.get_schema();
        if schema.is_empty() {
            return Ok(None);
        }
        let schema_len = schema.len();
        if schema_len != fields.len()
            || fields
                .keys()
                .any(|name| !schema.iter().any(|(schema_name, _)| schema_name == name))
        {
            return Ok(None);
        }

        let durability = self.durability;
        let ids = py.allow_threads(|| -> PyResult<Vec<u64>> {
            let lock_file = Self::acquire_write_lock(&table_path)
                        .map_err(|e| PyIOError::new_err(e.to_string()))?;

            let result = backend
                .insert_column_rows_to_delta(&[fields])
                .map_err(|e| PyIOError::new_err(e.to_string()));

            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }

            result
        })?;
        // Keep the insert backend warm so repeated OLTP appends don't reopen
        // and rescan the delta file. Read/query caches must still be invalidated.
        self.cached_backends
            .remove(&Self::backend_cache_key(&table_path, &table_name));
        crate::Database::invalidate_query_cache(&table_path);
        crate::query::planner::invalidate_table_stats(&table_path.to_string_lossy());
        crate::Database::notify_indexes_after_write(&table_path, &ids);

        Ok(Some(ids.into_iter().map(|id| id as i64).collect()))
    }

    fn store_rows_delta(
        &self,
        py: Python<'_>,
        rows: &Bound<'_, PyList>,
    ) -> PyResult<Option<Vec<i64>>> {
        if rows.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let mut all_fields = Vec::with_capacity(rows.len());
        for item in rows.iter() {
            let row = item.downcast::<PyDict>()?;
            let fields = dict_to_column_values(row)?;
            if fields.is_empty() {
                return Ok(None);
            }
            if fields.values().any(|value| {
                matches!(
                    value,
                    ColumnValue::Null
                        | ColumnValue::Binary(_)
                        | ColumnValue::Blob(_)
                        | ColumnValue::FixedList(_)
                )
            }) {
                return Ok(None);
            }
            all_fields.push(fields);
        }

        let (table_path, table_name) = self.get_current_table_info()?;
        if self.table_has_secondary_indexes(py, &table_path, &table_name) {
            return Ok(None);
        }

        self.persist_pending_overlay_for_table(py, &table_path, &table_name)?;
        let backend = self.get_backend_for_insert(py)?;

        if !backend.storage.is_v4_format() || backend.storage.has_constraints() {
            return Ok(None);
        }

        let schema = backend.storage.get_schema();
        if schema.is_empty() {
            return Ok(None);
        }
        let schema_len = schema.len();
        for fields in &all_fields {
            if schema_len != fields.len()
                || fields
                    .keys()
                    .any(|name| !schema.iter().any(|(schema_name, _)| schema_name == name))
            {
                return Ok(None);
            }
        }

        let durability = self.durability;
        let ids = py.allow_threads(|| -> PyResult<Vec<u64>> {
            let lock_file = Self::acquire_write_lock(&table_path)
                        .map_err(|e| PyIOError::new_err(e.to_string()))?;

            let result = backend
                .insert_column_rows_to_delta(&all_fields)
                .map_err(|e| PyIOError::new_err(e.to_string()));

            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }

            result
        })?;
        self.cached_backends
            .remove(&Self::backend_cache_key(&table_path, &table_name));
        crate::Database::invalidate_query_cache(&table_path);
        crate::query::planner::invalidate_table_stats(&table_path.to_string_lossy());
        crate::Database::notify_indexes_after_write(&table_path, &ids);

        Ok(Some(ids.into_iter().map(|id| id as i64).collect()))
    }

    fn store_one_delta_durable(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyDict>,
    ) -> PyResult<Option<Vec<i64>>> {
        if row.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let fields = dict_to_column_values(row)?;
        if fields.is_empty() {
            return Ok(Some(Vec::new()));
        }
        if fields.values().any(|value| {
            matches!(
                value,
                ColumnValue::Null
                    | ColumnValue::Binary(_)
                    | ColumnValue::Blob(_)
                    | ColumnValue::FixedList(_)
            )
        }) {
            return Ok(None);
        }

        let (table_path, table_name) = self.get_current_table_info()?;
        if self.table_has_secondary_indexes(py, &table_path, &table_name) {
            return Ok(None);
        }

        self.persist_pending_overlay_for_table(py, &table_path, &table_name)?;
        let backend = self.get_backend_for_insert(py)?;

        if !backend.storage.is_v4_format() || backend.storage.has_constraints() {
            return Ok(None);
        }

        let schema = backend.storage.get_schema();
        if schema.is_empty() {
            return Ok(None);
        }
        let schema_len = schema.len();
        if schema_len != fields.len()
            || fields
                .keys()
                .any(|name| !schema.iter().any(|(schema_name, _)| schema_name == name))
        {
            return Ok(None);
        }

        let result = py.allow_threads(|| -> PyResult<Vec<u64>> {
            let ids = backend
                .insert_column_rows_to_delta(&[fields])
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            backend
                .sync()
                .map_err(|e| PyIOError::new_err(format!("Failed to durable-insert: {}", e)))?;
            Ok(ids)
        });

        let ids = result?;
        self.cached_backends
            .remove(&Self::backend_cache_key(&table_path, &table_name));
        crate::Database::invalidate_query_cache(&table_path);
        crate::query::planner::invalidate_table_stats(&table_path.to_string_lossy());

        Ok(Some(ids.into_iter().map(|id| id as i64).collect()))
    }

    fn store_columnar(&self, py: Python<'_>, columns: &Bound<'_, PyDict>) -> PyResult<Vec<i64>> {
        if columns.is_empty() {
            return Ok(Vec::new());
        }

        // First pass: validate all columns have the same length
        let mut col_lengths: Vec<(String, usize)> = Vec::new();
        for (key, value) in columns.iter() {
            let col_name: String = key.extract()?;
            if col_name == "_id" {
                continue;
            }

            let list = value.downcast::<PyList>().map_err(|_| {
                PyValueError::new_err(format!("Column '{}' must be a list", col_name))
            })?;
            col_lengths.push((col_name, list.len()));
        }

        if col_lengths.is_empty() {
            return Ok(Vec::new());
        }

        // Check all columns have same length
        let first_len = col_lengths[0].1;
        for (name, len) in &col_lengths {
            if *len != first_len {
                return Err(PyValueError::new_err(format!(
                    "All columns must have the same length: '{}' has {} rows, expected {}",
                    name, len, first_len
                )));
            }
        }

        let num_rows = first_len;
        if num_rows == 0 {
            return Ok(Vec::new());
        }

        // Build ColumnData directly from the Python buffers. Strings and bytes
        // are borrowed (&str / &[u8]) and copied once into the column data
        // buffer — no per-element String/Vec allocation, no typed intermediate
        // layer (see StorageEngine::write_typed_columns).
        let mut columns_map: HashMap<String, ColumnData> = HashMap::new();
        let mut null_positions: HashMap<String, Vec<bool>> = HashMap::new();

        for (key, value) in columns.iter() {
            let col_name: String = key.extract()?;
            if col_name == "_id" {
                continue;
            }

            let list = value.downcast::<PyList>().map_err(|_| {
                PyValueError::new_err(format!("Column '{}' must be a list", col_name))
            })?;

            let col_len = list.len();
            if col_len == 0 {
                continue;
            }

            // Detect type from first non-None element
            // NOTE: Check bool before int because in Python bool is a subclass of int
            // NOTE: Check bytes before string because PyBytes can also be extracted as str in some pyo3 versions
            let mut col_type: Option<&str> = None;
            for item in list.iter() {
                if !item.is_none() {
                    if item.extract::<bool>().is_ok()
                        && item.get_type().name().map_or(false, |n| n == "bool")
                    {
                        col_type = Some("bool");
                    } else if item.downcast::<pyo3::types::PyBytes>().is_ok() {
                        col_type = Some("bytes");
                    } else if item.get_type().name().map_or(false, |n| n == "ndarray") {
                        // Always use "fixedlist" for numpy arrays (any dtype).
                        // The fixedlist path calls .astype("float32").tobytes() which produces
                        // f32 bytes.  insert_typed_with_nulls_full then calls
                        // push_float16_list_from_f32() which does the single correct f32→f16
                        // conversion for Float16List columns.  Routing through "float16_vector"
                        // would produce f16 bytes here AND call push_float16_list_from_f32()
                        // again — causing a double conversion and garbled data.
                        col_type = Some("fixedlist");
                    } else if item
                        .downcast::<pyo3::types::PyList>()
                        .ok()
                        .and_then(|seq| seq.get_item(0).ok())
                        .map_or(false, |first| first.extract::<f64>().is_ok())
                    {
                        col_type = Some("fixedlist");
                    } else if item.extract::<i64>().is_ok() {
                        col_type = Some("int");
                    } else if item.extract::<f64>().is_ok() {
                        col_type = Some("float");
                    } else if item.extract::<&str>().is_ok() {
                        col_type = Some("string");
                    }
                    break;
                }
            }

            match col_type {
                Some("int") => {
                    let mut vals = Vec::with_capacity(col_len);
                    let mut nulls = Vec::with_capacity(col_len);
                    for item in list.iter() {
                        let is_null = item.is_none();
                        nulls.push(is_null);
                        vals.push(if is_null {
                            0
                        } else {
                            item.extract::<i64>().unwrap_or(0)
                        });
                    }
                    columns_map.insert(col_name.clone(), ColumnData::Int64(vals));
                    null_positions.insert(col_name, nulls);
                }
                Some("float") => {
                    let mut vals = Vec::with_capacity(col_len);
                    let mut nulls = Vec::with_capacity(col_len);
                    for item in list.iter() {
                        let is_null = item.is_none();
                        nulls.push(is_null);
                        vals.push(if is_null {
                            0.0
                        } else {
                            item.extract::<f64>().unwrap_or(0.0)
                        });
                    }
                    columns_map.insert(col_name.clone(), ColumnData::Float64(vals));
                    null_positions.insert(col_name, nulls);
                }
                Some("bool") => {
                    let mut vals = Vec::with_capacity(col_len);
                    let mut nulls = Vec::with_capacity(col_len);
                    for item in list.iter() {
                        let is_null = item.is_none();
                        nulls.push(is_null);
                        vals.push(if is_null {
                            false
                        } else {
                            item.extract::<bool>().unwrap_or(false)
                        });
                    }
                    let byte_count = (vals.len() + 7) / 8;
                    let mut packed = vec![0u8; byte_count];
                    for (i, &v) in vals.iter().enumerate() {
                        if v {
                            packed[i / 8] |= 1 << (i % 8);
                        }
                    }
                    columns_map.insert(
                        col_name.clone(),
                        ColumnData::Bool {
                            data: packed,
                            len: vals.len(),
                        },
                    );
                    null_positions.insert(col_name, nulls);
                }
                Some("bytes") => {
                    let mut offsets = Vec::with_capacity(col_len + 1);
                    let mut data = Vec::new();
                    let mut nulls = Vec::with_capacity(col_len);
                    offsets.push(0u64);
                    for item in list.iter() {
                        let is_null = item.is_none();
                        nulls.push(is_null);
                        if is_null {
                            offsets.push(data.len() as u64);
                        } else if let Ok(b) = item.downcast::<pyo3::types::PyBytes>() {
                            data.extend_from_slice(b.as_bytes());
                            offsets.push(data.len() as u64);
                        } else if let Ok(s) = item.extract::<Vec<u8>>() {
                            data.extend_from_slice(&s);
                            offsets.push(data.len() as u64);
                        } else {
                            offsets.push(data.len() as u64);
                        }
                    }
                    columns_map.insert(
                        col_name.clone(),
                        ColumnData::Binary { offsets, data },
                    );
                    null_positions.insert(col_name, nulls);
                }
                Some("fixedlist") => {
                    let mut rows: Vec<Vec<u8>> = Vec::with_capacity(col_len);
                    let mut nulls = Vec::with_capacity(col_len);
                    for item in list.iter() {
                        let is_null = item.is_none();
                        nulls.push(is_null);
                        if is_null {
                            rows.push(Vec::new());
                        } else if let Ok(bytes) = item
                            .call_method0("flatten")
                            .and_then(|flat| flat.call_method1("astype", ("float32",)))
                            .and_then(|f32arr| f32arr.call_method0("tobytes"))
                            .and_then(|b| b.extract::<Vec<u8>>())
                        {
                            if bytes.is_empty() || bytes.chunks_exact(4).any(|chunk| {
                                !f32::from_le_bytes(chunk.try_into().unwrap()).is_finite()
                            }) {
                                return Err(PyValueError::new_err(format!(
                                    "Vector column '{}' must contain non-empty finite vectors",
                                    col_name
                                )));
                            }
                            rows.push(bytes);
                        } else if let Ok(seq) = item.downcast::<pyo3::types::PyList>() {
                            if seq.is_empty() {
                                return Err(PyValueError::new_err(format!(
                                    "Vector column '{}' must contain non-empty vectors", col_name
                                )));
                            }
                            let mut bytes = Vec::with_capacity(seq.len() * 4);
                            for elem in seq.iter() {
                                let f = elem.extract::<f32>().map_err(|_| PyValueError::new_err(
                                    format!("Vector column '{}' contains a non-numeric value", col_name)
                                ))?;
                                if !f.is_finite() {
                                    return Err(PyValueError::new_err(format!(
                                        "Vector column '{}' must contain only finite values", col_name
                                    )));
                                }
                                bytes.extend_from_slice(&f.to_le_bytes());
                            }
                            rows.push(bytes);
                        } else {
                            rows.push(Vec::new());
                        }
                    }
                    let dim = rows
                        .iter()
                        .find(|b| !b.is_empty())
                        .map(|b| b.len() / 4)
                        .unwrap_or(0) as u32;
                    if dim == 0 {
                        return Err(PyValueError::new_err(format!(
                            "Vector column '{}' has no non-null vector dimension", col_name
                        )));
                    }
                    let mut data: Vec<u8> = Vec::with_capacity(col_len * dim as usize * 4);
                    for (row_index, b) in rows.iter().enumerate() {
                        if b.is_empty() {
                            data.resize(data.len() + dim as usize * 4, 0);
                        } else if b.len() == dim as usize * 4 {
                            data.extend_from_slice(b);
                        } else {
                            return Err(PyValueError::new_err(format!(
                                "Vector column '{}' row {} has dimension {}, expected {}",
                                col_name, row_index, b.len() / 4, dim
                            )));
                        }
                    }
                    columns_map.insert(col_name.clone(), ColumnData::FixedList { data, dim });
                    null_positions.insert(col_name, nulls);
                }
                Some("float16_vector") => {
                    let mut rows: Vec<Vec<u8>> = Vec::with_capacity(col_len);
                    let mut nulls = Vec::with_capacity(col_len);
                    for item in list.iter() {
                        let is_null = item.is_none();
                        nulls.push(is_null);
                        if is_null {
                            rows.push(Vec::new());
                        } else if let Ok(f32_bytes) = item
                            .call_method0("flatten")
                            .and_then(|flat| flat.call_method1("astype", ("float32",)))
                            .and_then(|f32arr| f32arr.call_method0("tobytes"))
                            .and_then(|b| b.extract::<Vec<u8>>())
                        {
                            let f16_bytes: Vec<u8> = f32_bytes
                                .chunks_exact(4)
                                .flat_map(|c| {
                                    let f = f32::from_le_bytes(c.try_into().unwrap());
                                    crate::storage::on_demand::f32_to_f16(f).to_le_bytes()
                                })
                                .collect();
                            rows.push(f16_bytes);
                        } else {
                            rows.push(Vec::new());
                        }
                    }
                    let dim = rows
                        .iter()
                        .find(|b| !b.is_empty())
                        .map(|b| b.len() / 2)
                        .unwrap_or(0) as u32;
                    let mut data: Vec<u8> = Vec::with_capacity(col_len * dim as usize * 2);
                    for b in &rows {
                        data.extend_from_slice(b);
                    }
                    columns_map.insert(col_name.clone(), ColumnData::Float16List { data, dim });
                    null_positions.insert(col_name, nulls);
                }
                Some("string") | None => {
                    let mut offsets = Vec::with_capacity(col_len + 1);
                    let mut data = Vec::new();
                    let mut nulls = Vec::with_capacity(col_len);
                    offsets.push(0u64);
                    for item in list.iter() {
                        let is_null = item.is_none();
                        nulls.push(is_null);
                        if is_null {
                            offsets.push(data.len() as u64);
                        } else if let Ok(s) = item.extract::<&str>() {
                            // Borrowed Python str buffer — single copy into `data`.
                            data.extend_from_slice(s.as_bytes());
                            offsets.push(data.len() as u64);
                        } else {
                            offsets.push(data.len() as u64);
                        }
                    }
                    columns_map.insert(
                        col_name.clone(),
                        ColumnData::String { offsets, data },
                    );
                    null_positions.insert(col_name, nulls);
                }
                _ => {}
            }
        }

        if num_rows == 0 {
            return Ok(Vec::new());
        }

        let (table_path, table_name) = self.get_current_table_info()?;
        let durability = self.durability;
        self.persist_pending_overlay_for_table(py, &table_path, &table_name)?;

        // FTS indexing needs the original string columns after the write. Keep
        // a copy only when FTS is enabled — the common case avoids cloning the
        // whole batch.
        let string_columns_for_fts: Option<HashMap<String, ColumnData>> =
            if self.fts_manager.read().is_some() {
                let mut m = HashMap::new();
                for (name, col) in columns_map.iter() {
                    if matches!(col, ColumnData::String { .. }) {
                        m.insert(name.clone(), col.clone());
                    }
                }
                Some(m)
            } else {
                None
            };

        let ids = py.allow_threads(|| -> PyResult<Vec<u64>> {
            // Skip file lock for 'fast' durability
            let lock_file = Self::acquire_write_lock(&table_path)
                        .map_err(|e| PyIOError::new_err(e.to_string()))?;

            let result = crate::Database::write_typed_columns(
                &table_path,
                columns_map,
                null_positions,
                durability,
            )
            .map_err(|e| PyIOError::new_err(e.to_string()));

            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }

            result
        })?;

        // Invalidate local backend cache
        self.invalidate_backend(&table_name);
        self.mark_flush_prewarm(&table_path, &table_name);
        // On Windows, engine.insert_cache holds a mmap'd backend after write_typed.
        // Clearing it ensures set_len() in subsequent transaction-commit delete paths succeeds
        // (ERROR_USER_MAPPED_FILE / os error 1224 is triggered when any mmap is open).
        #[cfg(target_os = "windows")]
        crate::Database::invalidate(&table_path);

        // Index in FTS if enabled - OPTIMIZED: Use add_documents_arrow_str (🥈 zero-copy &str path)
        if let Some(string_columns_for_fts) = &string_columns_for_fts {
            let mgr = self.fts_manager.read();
            if mgr.is_some() {
                let table_name = self.current_table.read().clone();
                let base_dir = self.current_base_dir();
                let index_fields = self.fts_index_fields.read().get(&table_name).cloned();

                if let Some(m) = mgr.as_ref() {
                    if let Ok(engine) = m.get_engine(&table_name) {
                        // Determine which string fields to index
                        let string_field_names: Vec<String> = match &index_fields {
                            Some(fields) => fields
                                .iter()
                                .cloned()
                                .filter(|f| string_columns_for_fts.contains_key(f))
                                .collect(),
                            None => string_columns_for_fts.keys().cloned().collect(),
                        };

                        if !string_field_names.is_empty() {
                            // Build owned String columns, then convert to &str for zero-copy call
                            let fts_columns: Vec<(String, Vec<String>)> = string_field_names
                                .iter()
                                .filter_map(|f| {
                                    string_columns_for_fts
                                        .get(f)
                                        .map(|col| {
                                            let vals = match col {
                                                ColumnData::String { offsets, data } => {
                                                    let mut vals =
                                                        Vec::with_capacity(offsets.len() - 1);
                                                    for i in 0..offsets.len().saturating_sub(1) {
                                                        let start = offsets[i] as usize;
                                                        let end = offsets[i + 1] as usize;
                                                        vals.push(String::from_utf8_lossy(
                                                            &data[start..end],
                                                        )
                                                        .to_string());
                                                    }
                                                    vals
                                                }
                                                _ => Vec::new(),
                                            };
                                            (f.clone(), vals)
                                        })
                                })
                                .collect();

                            // 🥈 add_documents_arrow_str: zero-copy &str slices, ~3.3M docs/s
                            if !fts_columns.is_empty() {
                                let doc_ids_u64: Vec<u64> = ids.clone();
                                let columns_ref: Vec<(String, Vec<&str>)> = fts_columns
                                    .iter()
                                    .map(|(name, vals)| {
                                        (name.clone(), vals.iter().map(|s| s.as_str()).collect())
                                    })
                                    .collect();
                                let _ = py.allow_threads(|| {
                                    crate::Database::wait_fts_backfill(&base_dir, &table_name);
                                    engine.add_documents_arrow_str(&doc_ids_u64, columns_ref)
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(ids.into_iter().map(|id| id as i64).collect())
    }

    fn index_for_fts(&self, py: Python<'_>, id: i64, data: &Bound<'_, PyDict>) -> PyResult<()> {
        let table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();
        let mgr = self.fts_manager.read().clone();

        if mgr.is_none() {
            return Ok(());
        }

        // Get index fields config
        let index_fields = self.fts_index_fields.read().get(&table_name).cloned();

        // Build fields map from dict
        let mut fields = HashMap::new();
        for (key, value) in data.iter() {
            let key_str: String = key.extract()?;
            if key_str == "_id" {
                continue;
            }

            // Check if this field should be indexed
            let should_index = match &index_fields {
                Some(idx_fields) => idx_fields.contains(&key_str),
                None => value.extract::<String>().is_ok(), // Index all string fields by default
            };

            if should_index {
                if let Ok(s) = value.extract::<String>() {
                    fields.insert(key_str, s);
                }
            }
        }

        if fields.is_empty() {
            return Ok(());
        }

        // Index the document via add_documents_arrow_texts (pre-joined text, zero-copy &str)
        if let Some(m) = mgr {
            let joined = fields.values().cloned().collect::<Vec<_>>().join(" ");
            let doc_id = id as u64;
            py.allow_threads(|| {
                crate::Database::wait_fts_backfill(&base_dir, &table_name);
                if let Ok(engine) = m.get_engine(&table_name) {
                    let doc_ids = [doc_id];
                    let texts = [joined.as_str()];
                    let _ = engine.add_documents_arrow_texts(&doc_ids, &texts);
                }
            });
        }

        Ok(())
    }

    fn delete(&self, py: Python<'_>, id: i64) -> PyResult<bool> {
        let (table_path, table_name) = self.get_current_table_info()?;
        let _epoch_write = crate::storage::epoch::logical_write(&table_path);
        let durability = self.durability;

        if id < 0 {
            return Ok(false);
        }

        let replace_cache_key = Self::replace_row_cache_key(&table_path, &table_name, id as u64);

        if durability == DurabilityLevel::Fast
            && !self.table_has_secondary_indexes(py, &table_path, &table_name)
        {
            let backend = self.get_backend_for_overlay(py, &table_path, &table_name)?;
            if !backend.storage.has_constraints() {
                if backend.delete_pending_v4_in_memory_row(id as u64) {
                    self.replace_exact_row_cache.remove(&replace_cache_key);
                    crate::Database::cache_backend(&table_path, Arc::clone(&backend));
                    crate::query::planner::invalidate_table_stats(&table_path.to_string_lossy());
                    return Ok(true);
                }

                let result = py.allow_threads(|| {
                    let deleted = backend
                        .delta_delete_row(id as u64)
                        .map_err(|e| PyIOError::new_err(e.to_string()))?;
                    if deleted {
                        backend
                            .save_delta_store()
                            .map_err(|e| PyIOError::new_err(e.to_string()))?;
                    }
                    Ok::<bool, PyErr>(deleted)
                })?;
                if result {
                    self.replace_exact_row_cache.remove(&replace_cache_key);
                    crate::Database::cache_backend(&table_path, Arc::clone(&backend));
                    crate::query::planner::invalidate_table_stats(&table_path.to_string_lossy());
                }
                return Ok(result);
            }
        }

        let result = py.allow_threads(|| -> PyResult<bool> {
            // Skip file lock for 'fast' durability
            let lock_file = Self::acquire_write_lock(&table_path)
                        .map_err(|e| PyIOError::new_err(e.to_string()))?;

            // Use StorageEngine for unified delete
            let result = crate::Database::delete_one(&table_path, id as u64, durability)
                .map_err(|e| PyIOError::new_err(e.to_string()));

            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }

            result
        })?;

        // Invalidate local backend cache
        self.invalidate_backend(&table_name);

        Ok(result)
    }

    fn delete_batch(&self, py: Python<'_>, ids: Vec<i64>) -> PyResult<bool> {
        // Empty list is a successful no-op
        if ids.is_empty() {
            return Ok(true);
        }

        let (table_path, table_name) = self.get_current_table_info()?;
        let _epoch_write = crate::storage::epoch::logical_write(&table_path);
        let durability = self.durability;

        if durability == DurabilityLevel::Fast
            && !self.table_has_secondary_indexes(py, &table_path, &table_name)
        {
            let backend = self.get_backend_for_overlay(py, &table_path, &table_name)?;
            if !backend.storage.has_constraints() {
                let deleted_ids = py.allow_threads(|| -> PyResult<Vec<u64>> {
                    let mut deleted_ids = Vec::new();
                    for id in &ids {
                        if *id < 0 {
                            continue;
                        }
                        let row_id = *id as u64;
                        if backend.delete_pending_v4_in_memory_row(row_id)
                            || backend
                                .delta_delete_row(row_id)
                                .map_err(|e| PyIOError::new_err(e.to_string()))?
                        {
                            deleted_ids.push(row_id);
                        }
                    }
                    if !deleted_ids.is_empty() {
                        backend
                            .save_delta_store()
                            .map_err(|e| PyIOError::new_err(e.to_string()))?;
                    }
                    Ok(deleted_ids)
                })?;
                for row_id in &deleted_ids {
                    self.replace_exact_row_cache
                        .remove(&Self::replace_row_cache_key(
                            &table_path,
                            &table_name,
                            *row_id,
                        ));
                }
                if !deleted_ids.is_empty() {
                    crate::Database::cache_backend(&table_path, Arc::clone(&backend));
                    crate::query::planner::invalidate_table_stats(&table_path.to_string_lossy());
                }
                return Ok(!deleted_ids.is_empty());
            }
        }

        let ids_u64: Vec<u64> = ids.into_iter().map(|id| id as u64).collect();
        let deleted = py.allow_threads(|| -> PyResult<usize> {
            // Skip file lock for 'fast' durability
            let lock_file = Self::acquire_write_lock(&table_path)
                        .map_err(|e| PyIOError::new_err(e.to_string()))?;

            // Use StorageEngine for unified delete
            let deleted = crate::Database::delete(&table_path, &ids_u64, durability)
                .map_err(|e| PyIOError::new_err(e.to_string()));

            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }

            deleted
        })?;

        // Invalidate local backend cache
        self.invalidate_backend(&table_name);

        Ok(deleted > 0)
    }

    fn delete_where(&self, py: Python<'_>, where_clause: &str) -> PyResult<i64> {
        let (table_path, table_name) = self.get_current_table_info()?;

        // Build DELETE SQL statement
        let sql = format!("DELETE FROM {} WHERE {}", table_name, where_clause);

        // Execute using ApexExecutor
        let base_dir = self.current_base_dir();
        let root_dir = self.root_dir.clone();
        let exec_result = py.allow_threads(|| {
            crate::Session::new(&base_dir, &table_path)
                .with_root_dir(&root_dir)
                .execute(&sql)
        });
        let result = exec_result.map_err(|e| PyIOError::new_err(e.to_string()))?;

        // Invalidate cached backend since data changed
        self.invalidate_backend(&table_name);
        // Invalidate StorageEngine cache so count_rows() sees updated state
        crate::Database::invalidate(&table_path);

        // Extract scalar result (number of deleted rows)
        match result {
            ApexResult::Scalar(count) => Ok(count),
            _ => Ok(0),
        }
    }

    fn delete_all(&self, py: Python<'_>) -> PyResult<i64> {
        let (table_path, table_name) = self.get_current_table_info()?;

        // Build DELETE SQL statement without WHERE
        let sql = format!("DELETE FROM {}", table_name);

        // Execute using ApexExecutor
        let base_dir = self.current_base_dir();
        let root_dir = self.root_dir.clone();
        let exec_result = py.allow_threads(|| {
            crate::Session::new(&base_dir, &table_path)
                .with_root_dir(&root_dir)
                .execute(&sql)
        });
        let result = exec_result.map_err(|e| PyIOError::new_err(e.to_string()))?;

        // Invalidate cached backend since data changed
        self.invalidate_backend(&table_name);
        // Invalidate StorageEngine cache so count_rows() sees updated state
        crate::Database::invalidate(&table_path);

        // Extract scalar result (number of deleted rows)
        match result {
            ApexResult::Scalar(count) => Ok(count),
            _ => Ok(0),
        }
    }

    fn update_numeric_by_id_inplace(
        &self,
        id: i64,
        column: String,
        value: f64,
    ) -> PyResult<Option<i64>> {
        if id < 0 || column == "_id" {
            return Ok(None);
        }

        let (table_path, table_name) = self.get_current_table_info()?;
        let _epoch_write = crate::storage::epoch::logical_write(&table_path);
        let Some((backend, col_type)) =
            self.numeric_update_target(&table_path, &table_name, &column)
        else {
            return Ok(None);
        };
        let replace_cache_key = Self::replace_row_cache_key(&table_path, &table_name, id as u64);

        let bytes = match col_type {
            crate::storage::on_demand::ColumnType::Float64
            | crate::storage::on_demand::ColumnType::Float32 => value.to_le_bytes(),
            crate::storage::on_demand::ColumnType::Int64
            | crate::storage::on_demand::ColumnType::Int32
            | crate::storage::on_demand::ColumnType::Int16
            | crate::storage::on_demand::ColumnType::Int8
            | crate::storage::on_demand::ColumnType::UInt8
            | crate::storage::on_demand::ColumnType::UInt16
            | crate::storage::on_demand::ColumnType::UInt32
            | crate::storage::on_demand::ColumnType::UInt64
            | crate::storage::on_demand::ColumnType::Timestamp
            | crate::storage::on_demand::ColumnType::Date => (value as i64).to_le_bytes(),
            _ => return Ok(None),
        };

        match backend.update_by_id_inplace(id as u64, &column, &bytes) {
            Ok(Some((n, physically_written))) => {
                if physically_written {
                    self.replace_exact_row_cache.remove(&replace_cache_key);
                    crate::Database::invalidate(&table_path);
                    crate::Database::invalidate_query_cache(&table_path);
                    crate::query::planner::invalidate_table_stats(&table_path.to_string_lossy());
                }
                Ok(Some(n))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(PyIOError::new_err(e.to_string())),
        }
    }

    /// Batch numeric update for many `_id` rows in a single pass.
    ///
    /// Uses the in-place mmap writer when the row group layout allows it, and
    /// otherwise records all rows in the DeltaStore with one persistence call,
    /// avoiding the per-statement SQL executor cost for bulk backfills.
    ///
    /// Returns `None` when the fast path cannot serve the batch as a whole
    /// (for example constraints, indexes, or a non-numeric column); callers
    /// must then fall back to per-statement execution. Otherwise returns one
    /// entry per input id: `Some(affected)` when handled, or `None` for an
    /// individual row that needs the general UPDATE path.
    fn update_numeric_by_id_batch(
        &self,
        ids: Vec<i64>,
        column: String,
        values: Vec<f64>,
    ) -> PyResult<Option<Vec<Option<i64>>>> {
        if ids.len() != values.len() {
            return Err(PyValueError::new_err(
                "ids and values must have the same length",
            ));
        }
        if ids.is_empty() {
            return Ok(Some(Vec::new()));
        }
        if column == "_id" || ids.iter().any(|&id| id < 0) {
            return Ok(None);
        }

        let (table_path, table_name) = self.get_current_table_info()?;
        let _epoch_write = crate::storage::epoch::logical_write(&table_path);
        let Some((backend, col_type)) =
            self.numeric_update_backend_and_type(&table_path, &table_name, &column)
        else {
            return Ok(None);
        };

        let mut affected = Vec::with_capacity(ids.len());
        let mut delta_values: Vec<(u64, &str, crate::data::Value)> = Vec::new();
        let mut delta_value_idxs: Vec<usize> = Vec::new();
        let mut any_written = false;
        let has_pending = backend.has_pending_deltas()
            || backend.pending_v4_in_memory_rows() > 0;
        for (idx, (&id, &value)) in ids.iter().zip(values.iter()).enumerate() {
            let bytes = match col_type {
                crate::storage::on_demand::ColumnType::Float64
                | crate::storage::on_demand::ColumnType::Float32 => value.to_le_bytes(),
                crate::storage::on_demand::ColumnType::Int64
                | crate::storage::on_demand::ColumnType::Int32
                | crate::storage::on_demand::ColumnType::Int16
                | crate::storage::on_demand::ColumnType::Int8
                | crate::storage::on_demand::ColumnType::UInt8
                | crate::storage::on_demand::ColumnType::UInt16
                | crate::storage::on_demand::ColumnType::UInt32
                | crate::storage::on_demand::ColumnType::UInt64
                | crate::storage::on_demand::ColumnType::Timestamp
                | crate::storage::on_demand::ColumnType::Date => (value as i64).to_le_bytes(),
                _ => return Ok(None),
            };
            let mut handled_inplace = false;
            if !has_pending {
                match backend.update_by_id_inplace(id as u64, &column, &bytes) {
                    Ok(Some((n, physically_written))) => {
                        if physically_written {
                            any_written = true;
                            self.replace_exact_row_cache.remove(
                                &Self::replace_row_cache_key(&table_path, &table_name, id as u64),
                            );
                        }
                        affected.push(Some(n));
                        handled_inplace = true;
                    }
                    Ok(None) => {}
                    Err(e) => return Err(PyIOError::new_err(e.to_string())),
                }
            }
            if handled_inplace {
                continue;
            }

            // Fall back to DeltaStore for this row (compressed row groups or an
            // existing delta overlay). The whole batch is persisted once below.
            let row_id = id as u64;
            if has_pending {
                let delta = backend.storage.delta_store();
                if delta.is_deleted(row_id) {
                    affected.push(Some(0));
                    continue;
                }
            }
            let exists = match backend.row_id_active_rcix(row_id)? {
                Some(exists) => exists,
                None => {
                    affected.push(None);
                    continue;
                }
            };
            if !exists {
                affected.push(Some(0));
                continue;
            }
            let value = match col_type {
                crate::storage::on_demand::ColumnType::Float64
                | crate::storage::on_demand::ColumnType::Float32 => crate::data::Value::Float64(value),
                _ => crate::data::Value::Int64(value as i64),
            };
            delta_values.push((row_id, column.as_str(), value));
            delta_value_idxs.push(idx);
            affected.push(Some(1));
        }

        if !delta_values.is_empty() {
            backend.delta_batch_update_rows(&delta_values);
            backend.save_delta_store()?;
            for &idx in &delta_value_idxs {
                let id = ids[idx];
                self.replace_exact_row_cache.remove(
                    &Self::replace_row_cache_key(&table_path, &table_name, id as u64),
                );
                any_written = true;
            }
        }

        if any_written {
            crate::Database::invalidate(&table_path);
            crate::Database::invalidate_query_cache(&table_path);
            crate::query::planner::invalidate_table_stats(&table_path.to_string_lossy());
        }
        Ok(Some(affected))
    }

    fn replace(&self, py: Python<'_>, id: i64, data: &Bound<'_, PyDict>) -> PyResult<bool> {
        if id < 0 {
            return Ok(false);
        }

        let table_path = self.get_current_table_path()?;
        let _epoch_write = crate::storage::epoch::logical_write(&table_path);
        let table_name = self.current_table.read().clone();
        let durability = self.durability;
        let replace_cache_key = Self::replace_row_cache_key(&table_path, &table_name, id as u64);

        let table_epoch = crate::storage::epoch::current(&table_path);
        let stale_replace =
            if let Some(entry) = self.replace_exact_row_cache.get(&replace_cache_key) {
                if entry.value().1 == table_epoch
                    && Self::py_dict_matches_exact_fields(data, &entry.value().0)?
                {
                    return Ok(true);
                }
                entry.value().1 != table_epoch
            } else {
                false
            };
        if stale_replace {
            self.replace_exact_row_cache.remove(&replace_cache_key);
        }

        if let Ok(backend) = self.get_backend_for_overlay(py, &table_path, &table_name) {
            if let Some(true) = self.row_matches_exact_py_dict(&backend, id as u64, data)? {
                return Ok(true);
            }
        }

        let fields = dict_to_values(data)?;

        if !fields.is_empty() {
            if let Ok(backend) = self.get_backend_for_overlay(py, &table_path, &table_name) {
                if let Some(true) = self.row_matches_exact_fields(&backend, id as u64, &fields)? {
                    return Ok(true);
                }
            }
        }

        if durability == DurabilityLevel::Fast
            && !fields.is_empty()
            && !self.table_has_secondary_indexes(py, &table_path, &table_name)
        {
            let backend = self.get_backend_for_overlay(py, &table_path, &table_name)?;
            if !backend.storage.has_constraints() {
                let schema = backend.storage.get_schema();
                let schema_cols: std::collections::HashSet<&str> =
                    schema.iter().map(|(name, _)| name.as_str()).collect();
                let schema_supported = schema.iter().all(|(_, ty)| {
                    use crate::storage::on_demand::ColumnType;
                    !matches!(
                        *ty,
                        ColumnType::Binary
                            | ColumnType::FixedList
                            | ColumnType::Float16List
                            | ColumnType::BFloat16List
                            | ColumnType::Int8Vector
                            | ColumnType::UInt8Vector
                            | ColumnType::Bit1Vector
                            | ColumnType::TurboQuant2Vector
                            | ColumnType::TurboQuant3Vector
                            | ColumnType::TurboQuant4Vector
                            | ColumnType::Null
                    )
                });
                let exact_schema = fields.len() == schema_cols.len()
                    && fields
                        .keys()
                        .all(|name| schema_cols.contains(name.as_str()));

                if schema_supported && exact_schema {
                    let result = py.allow_threads(|| {
                        backend
                            .delta_update_existing_row(id as u64, &fields)
                            .map_err(|e| PyIOError::new_err(e.to_string()))
                    })?;
                    if result {
                        self.replace_exact_row_cache.insert(
                            replace_cache_key.clone(),
                            (fields.clone(), crate::storage::epoch::current(&table_path)),
                        );
                        crate::Database::cache_backend(&table_path, Arc::clone(&backend));
                        crate::query::planner::invalidate_table_stats(
                            &table_path.to_string_lossy(),
                        );
                    } else {
                        self.replace_exact_row_cache.remove(&replace_cache_key);
                    }
                    return Ok(result);
                }
            }
        }

        // Use StorageEngine for unified replace
        let result = py.allow_threads(|| -> PyResult<bool> {
            let lock_file = Self::acquire_write_lock(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            let result = crate::Database::replace(&table_path, id as u64, &fields, durability)
                .map_err(|e| PyIOError::new_err(e.to_string()));
            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }
            result
        });

        // Invalidate local backend cache
        self.invalidate_backend(&table_name);

        if matches!(&result, Ok(true)) {
            self.replace_exact_row_cache.remove(&replace_cache_key);
        }

        result
    }

    fn add_column(&self, py: Python<'_>, column_name: &str, column_type: &str) -> PyResult<()> {
        let dtype = match column_type.to_lowercase().as_str() {
            "int" | "int64" | "i64" | "integer" => crate::data::DataType::Int64,
            "float" | "float64" | "f64" | "double" => crate::data::DataType::Float64,
            "bool" | "boolean" => crate::data::DataType::Bool,
            "str" | "string" | "text" => crate::data::DataType::String,
            "bytes" | "binary" => crate::data::DataType::Binary,
            "blob" | "large_binary" | "largebinary" => crate::data::DataType::Blob,
            "float16_vector" | "float16vector" | "f16_vector" => {
                crate::data::DataType::Float16Vector
            }
            "float32_vector" | "f32_vector" => crate::data::DataType::Float32Vector,
            "bfloat16_vector" | "bf16_vector" => crate::data::DataType::BFloat16Vector,
            "int8_vector" | "i8_vector" => crate::data::DataType::Int8Vector,
            "uint8_vector" | "u8_vector" => crate::data::DataType::UInt8Vector,
            "bit1_vector" | "binary1_vector" => crate::data::DataType::Bit1Vector,
            "turboquant2_vector" | "tq2_vector" => crate::data::DataType::TurboQuant2Vector,
            "turboquant3_vector" | "tq3_vector" => crate::data::DataType::TurboQuant3Vector,
            "turboquant4_vector" | "tq4_vector" => crate::data::DataType::TurboQuant4Vector,
            "timestamp" | "datetime" => crate::data::DataType::Timestamp,
            "date" => crate::data::DataType::Date,
            _ => crate::data::DataType::String,
        };

        let table_path = self.get_current_table_path()?;
        let table_name = self.current_table.read().clone();
        let durability = self.durability;

        // Invalidate local backend cache before operation
        self.invalidate_backend(&table_name);

        let column_name = column_name.to_string();
        let result = py.allow_threads(|| -> PyResult<()> {
            // Acquire exclusive write lock
            let lock_file = Self::acquire_write_lock(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            // Use StorageEngine for unified add_column
            let result = crate::Database::add_column(&table_path, &column_name, dtype, durability)
                .map_err(|e| PyIOError::new_err(e.to_string()));

            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }
            result
        });

        // Invalidate local backend cache after operation
        self.invalidate_backend(&table_name);

        result
    }

    fn create_quantized_column(
        &self,
        py: Python<'_>,
        source_column: &str,
        target_column: &str,
        codec: &str,
    ) -> PyResult<()> {
        let dtype = match codec.to_ascii_lowercase().replace(['-', '_'], "").as_str() {
            "float16" | "f16" => crate::data::DataType::Float16Vector,
            "bfloat16" | "bf16" => crate::data::DataType::BFloat16Vector,
            "int8" | "i8" => crate::data::DataType::Int8Vector,
            "uint8" | "u8" => crate::data::DataType::UInt8Vector,
            "1bit" | "bit1" | "binary1" => crate::data::DataType::Bit1Vector,
            "turboquant2" | "tq2" => crate::data::DataType::TurboQuant2Vector,
            "turboquant3" | "tq3" => crate::data::DataType::TurboQuant3Vector,
            "turboquant4" | "tq4" => crate::data::DataType::TurboQuant4Vector,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported vector codec '{}'. Expected float16, bfloat16, int8, uint8, 1bit, or turboquant2/3/4",
                    codec
                )))
            }
        };
        let table_path = self.get_current_table_path()?;
        let table_name = self.current_table.read().clone();
        let durability = self.durability;
        let source_column = source_column.to_string();
        let target_column = target_column.to_string();

        self.invalidate_backend(&table_name);
        let result = py.allow_threads(|| -> PyResult<()> {
            let lock_file = Self::acquire_write_lock(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            let result = crate::Database::add_quantized_vector_column(
                &table_path,
                &source_column,
                &target_column,
                dtype,
                durability,
            )
            .map_err(|e| PyIOError::new_err(e.to_string()));
            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }
            result
        });
        self.invalidate_backend(&table_name);
        result
    }

    fn drop_quantized_column(&self, py: Python<'_>, target_column: &str) -> PyResult<()> {
        let table_path = self.get_current_table_path()?;
        let table_name = self.current_table.read().clone();
        let durability = self.durability;
        let target_column = target_column.to_string();
        self.invalidate_backend(&table_name);
        let result = py.allow_threads(|| -> PyResult<()> {
            let lock_file = Self::acquire_write_lock(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            let result = crate::Database::drop_quantized_vector_column(
                &table_path,
                &target_column,
                durability,
            )
            .map_err(|e| PyIOError::new_err(e.to_string()));
            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }
            result
        });
        self.invalidate_backend(&table_name);
        result
    }

    fn drop_column(&self, py: Python<'_>, column_name: &str) -> PyResult<()> {
        let table_path = self.get_current_table_path()?;
        let table_name = self.current_table.read().clone();
        let durability = self.durability;

        // Invalidate local backend cache before operation
        self.invalidate_backend(&table_name);

        let column_name = column_name.to_string();
        let result = py.allow_threads(|| -> PyResult<()> {
            // Acquire exclusive write lock
            let lock_file = Self::acquire_write_lock(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            // Use StorageEngine for unified drop_column
            let result = crate::Database::drop_column(&table_path, &column_name, durability)
                .map_err(|e| PyIOError::new_err(e.to_string()));

            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }
            result
        });

        // Invalidate local backend cache after operation
        self.invalidate_backend(&table_name);

        result
    }

    fn rename_column(&self, py: Python<'_>, old_name: &str, new_name: &str) -> PyResult<()> {
        let table_path = self.get_current_table_path()?;
        let table_name = self.current_table.read().clone();
        let durability = self.durability;

        let old_name = old_name.to_string();
        let new_name = new_name.to_string();
        let result = py.allow_threads(|| -> PyResult<()> {
            // Acquire exclusive write lock
            let lock_file = Self::acquire_write_lock(&table_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            // Use StorageEngine for unified rename_column
            let result =
                crate::Database::rename_column(&table_path, &old_name, &new_name, durability)
                    .map_err(|e| PyIOError::new_err(e.to_string()));

            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }
            result
        });

        // Invalidate local backend cache
        self.invalidate_backend(&table_name);

        result
    }

    fn save(&self) -> PyResult<()> {
        // Storage auto-saves on each operation
        Ok(())
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let table_path = self.get_current_table_path()?;
        let table_name = self.current_table.read().clone();
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

        if backends.is_empty() {
            self.prewarm_flushed_backend(&table_path, &table_name);
            return Ok(());
        }

        let any_needs_save = py.allow_threads(|| -> PyResult<bool> {
            let mut actions: Vec<(Arc<TableStorageBackend>, bool)> = Vec::new();
            for backend in backends {
                let needs_save = backend.is_dirty()
                    || backend.has_pending_deltas()
                    || backend.pending_v4_in_memory_rows() > 0;
                let needs_sync = backend.storage.sync_pending();
                if needs_save || needs_sync {
                    actions.push((backend, needs_save));
                }
            }

            if actions.is_empty() {
                return Ok(false);
            }

            let any_needs_save = actions.iter().any(|(_, needs_save)| *needs_save);
            let lock_file = if any_needs_save {
                Self::acquire_write_lock(&table_path)
                    .map_err(|e| PyIOError::new_err(e.to_string()))?
            } else {
                None
            };

            let result: PyResult<()> = (|| {
                for (backend, needs_save) in actions {
                    if needs_save {
                        backend
                            .save()
                            .and_then(|_| backend.sync())
                            .map_err(|e| PyIOError::new_err(format!("Failed to flush: {}", e)))?;
                    } else {
                        backend
                            .sync()
                            .map_err(|e| PyIOError::new_err(format!("Failed to flush: {}", e)))?;
                    }
                }
                Ok(())
            })();

            if let Some(lf) = lock_file {
                Self::release_lock(lf);
            }
            result.map(|_| any_needs_save)
        })?;

        if any_needs_save {
            crate::Database::invalidate(&table_path);
            crate::Database::invalidate_query_cache(&table_path);
            crate::query::planner::invalidate_table_stats(&table_path.to_string_lossy());
            self.invalidate_backend(&table_name);
        }
        self.prewarm_flushed_backend(&table_path, &table_name);
        Ok(())
    }

    #[pyo3(signature = (rows = 0, bytes = 0))]
    fn set_auto_flush(&self, rows: u64, bytes: u64) -> PyResult<()> {
        // Persist at struct level so thresholds survive backend cache invalidation
        *self.auto_flush_rows.write() = rows;
        *self.auto_flush_bytes.write() = bytes;
        // Also apply to cached backend if present
        let table_name = self.current_table.read().clone();
        let table_path = self.get_current_table_path()?;
        let cache_key = Self::backend_cache_key(&table_path, &table_name);
        if let Some(backend) = self.cached_backends.get(&cache_key) {
            backend.set_auto_flush(rows, bytes);
        }
        Ok(())
    }

    fn set_compression(&self, py: Python<'_>, compression: &str) -> PyResult<bool> {
        use crate::storage::on_demand::{CompressionType, OnDemandStorage};
        let comp = CompressionType::from_str_opt(compression).ok_or_else(|| {
            PyValueError::new_err(format!(
                "Invalid compression type '{}'. Use 'none', 'lz4', or 'zstd'.",
                compression
            ))
        })?;
        let table_path = self.get_current_table_path()?;
        py.allow_threads(|| {
            let storage = if table_path.exists() {
                OnDemandStorage::open_with_durability(&table_path, self.durability)
            } else if crate::storage::table_catalog::file_exists_or_registered(&table_path)? {
                let backend = crate::storage::table_catalog::materialize_table_backend(
                    &table_path,
                    self.durability,
                )?;
                Ok(backend.storage)
            } else {
                OnDemandStorage::create_with_durability(&table_path, self.durability)
            }
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            storage
                .set_compression(comp)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let base_dir = self.current_base_dir();
        let pending_backends: Vec<Arc<TableStorageBackend>> = self
            .cached_backends
            .all_backends()
            .into_iter()
            .filter_map(|backend| {
                if backend.has_pending_deltas() || backend.pending_v4_in_memory_rows() > 0 {
                    Some(backend)
                } else {
                    None
                }
            })
            .collect();

        py.allow_threads(|| {
            crate::Database::unregister_fts_manager(&base_dir);
            for backend in pending_backends {
                if backend.has_pending_deltas() || backend.pending_v4_in_memory_rows() > 0 {
                    let _ = backend.save();
                }
            }
            if self.in_memory {
                crate::Database::drop_memory_database(&self.root_dir);
            }
        });

        // Clear per-instance cached backends (releases per-instance references)
        self.cached_backends.clear();
        self.update_by_id_numeric_cache.clear();
        self.update_by_id_cell_cache.clear();
        self.replace_exact_row_cache.clear();
        self.flush_prewarm_tables.clear();

        // Release the table catalog mapping(s) for this database so Windows
        // can rewrite or delete `.apex_tables` after the client closes
        // (OS error 1224 otherwise). The default database lives at root_dir;
        // a named database lives at base_dir.
        if !self.in_memory {
            crate::storage::table_catalog::release(&self.root_dir);
            if self.root_dir != base_dir {
                crate::storage::table_catalog::release(&base_dir);
            }
        }

        // Clean up temp tables
        if !self.in_memory {
            let _ = fs::remove_dir_all(&self.temp_dir);
        }

        // On Windows: release all mmaps so temp directories can be cleaned up.
        // On Unix: mmaps remain valid after atomic rename; keep STORAGE_CACHE alive
        // so the 50ms fast path in get_cached_backend skips stat() calls on next retrieve().
        #[cfg(target_os = "windows")]
        {
            crate::Database::invalidate_dir(&base_dir);
            crate::Database::invalidate_query_cache_for_dir(&base_dir);
        }
        Ok(())
    }

    #[pyo3(name = "_init_fts")]
    #[pyo3(signature = (index_fields=None, lazy_load=false, cache_size=10000))]
    fn init_fts(
        &self,
        py: Python<'_>,
        index_fields: Option<Vec<String>>,
        lazy_load: bool,
        cache_size: usize,
    ) -> PyResult<()> {
        let table_name = self.current_table.read().clone();

        // Record index field configuration
        if let Some(fields) = index_fields.clone() {
            self.fts_index_fields
                .write()
                .insert(table_name.clone(), fields);
        }

        // Ensure manager exists. SQL DDL may have already built and registered
        // a manager with populated engines; reuse it so Python-side config sync
        // does not replace a freshly backfilled FTS index with an empty one.
        if self.fts_manager.read().is_none() {
            let base_dir = self.current_base_dir();
            let manager = py.allow_threads(|| {
                if let Some(existing) = crate::Database::fts_manager(&base_dir) {
                    existing
                } else {
                    let fts_dir = base_dir.join("fts_indexes");
                    let config = FtsConfig {
                        lazy_load,
                        cache_size,
                        ..FtsConfig::default()
                    };
                    Arc::new(FtsManager::new(&fts_dir, config))
                }
            });
            py.allow_threads(|| {
                crate::Database::register_fts_manager(&base_dir, manager.clone());
            });
            *self.fts_manager.write() = Some(manager);
        } else {
            // Already initialized — ensure global registry is up to date
            let mgr_arc = self.fts_manager.read().clone();
            if let Some(m) = mgr_arc {
                let base_dir = self.current_base_dir();
                py.allow_threads(|| {
                    crate::Database::register_fts_manager(&base_dir, m);
                });
            }
        }

        // Touch/create engine for current table
        let mgr = self.fts_manager.read().clone();
        if let Some(m) = mgr {
            let config = FtsConfig {
                lazy_load,
                cache_size,
                ..FtsConfig::default()
            };
            let needs_rebuild = py.allow_threads(|| -> PyResult<bool> {
                m.configure_table(&table_name, config);
                m.get_engine(&table_name)
                    .map(|engine| engine.needs_rebuild())
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })?;
            if needs_rebuild {
                let base_dir = self.current_base_dir();
                let backfill_running =
                    py.allow_threads(|| crate::Database::has_fts_backfill(&base_dir, &table_name));
                if !backfill_running {
                    let fields = index_fields.clone();
                    py.allow_threads(|| {
                        crate::Database::fts_backfill(&base_dir, &table_name, fields.as_deref(), m)
                            .map_err(|e| PyIOError::new_err(e.to_string()))
                    })?;
                }
            }
        }

        Ok(())
    }

    #[pyo3(name = "_fts_remove_engine")]
    #[pyo3(signature = (delete_files=false))]
    fn fts_remove_engine(&self, py: Python<'_>, delete_files: bool) -> PyResult<()> {
        let table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();

        // Remove any cached index field configuration for this table
        self.fts_index_fields.write().remove(&table_name);

        let mgr = self.fts_manager.read().clone();
        py.allow_threads(|| -> PyResult<()> {
            crate::Database::wait_fts_backfill(&base_dir, &table_name);
            if let Some(m) = mgr {
                m.remove_engine(&table_name, delete_files)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            }
            Ok(())
        })?;

        Ok(())
    }

    #[pyo3(name = "_fts_index")]
    fn fts_index(&self, py: Python<'_>, id: i64, text: &str) -> PyResult<()> {
        let table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();
        let mgr = self.fts_manager.read();

        if let Some(m) = mgr.as_ref() {
            let engine = m
                .get_engine(&table_name)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let mut fields = HashMap::new();
            fields.insert("content".to_string(), text.to_string());
            // Release GIL during indexing operation
            py.allow_threads(|| {
                crate::Database::wait_fts_backfill(&base_dir, &table_name);
                engine
                    .add_document(id as u64, fields)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })?;
        }

        Ok(())
    }

    #[pyo3(name = "_fts_remove")]
    fn fts_remove(&self, py: Python<'_>, id: i64) -> PyResult<()> {
        let table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();
        let mgr = self.fts_manager.read();

        if let Some(m) = mgr.as_ref() {
            let engine = m
                .get_engine(&table_name)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            // Release GIL during remove operation
            py.allow_threads(|| {
                crate::Database::wait_fts_backfill(&base_dir, &table_name);
                engine
                    .remove_document(id as u64)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })?;
        }

        Ok(())
    }

    #[pyo3(name = "_fts_set_fuzzy_config")]
    fn fts_set_fuzzy_config(
        &self,
        threshold: f64,
        max_distance: usize,
        max_candidates: usize,
    ) -> PyResult<()> {
        let table_name = self.current_table.read().clone();
        if let Some(manager) = self.fts_manager.read().as_ref() {
            manager
                .get_engine(&table_name)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
                .set_fuzzy_config(threshold, max_distance, max_candidates);
        }
        Ok(())
    }

    #[pyo3(name = "_fts_compact")]
    fn fts_compact(&self, py: Python<'_>) -> PyResult<()> {
        let table_name = self.current_table.read().clone();
        if let Some(manager) = self.fts_manager.read().as_ref() {
            let engine = manager
                .get_engine(&table_name)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            py.allow_threads(|| {
                engine
                    .compact()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })?;
        }
        Ok(())
    }

    #[pyo3(name = "_fts_flush")]
    fn fts_flush(&self, py: Python<'_>) -> PyResult<()> {
        let table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();
        let mgr = self.fts_manager.read();

        if let Some(m) = mgr.as_ref() {
            let engine = m
                .get_engine(&table_name)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            // Release GIL during flush (I/O operation)
            py.allow_threads(|| {
                crate::Database::wait_fts_backfill(&base_dir, &table_name);
                engine
                    .flush()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })?;
        }

        Ok(())
    }

    #[pyo3(name = "_fts_index_columns")]
    fn fts_index_columns(
        &self,
        py: Python<'_>,
        ids: Vec<i64>,
        columns: HashMap<String, Vec<String>>,
    ) -> PyResult<usize> {
        if ids.is_empty() || columns.is_empty() {
            return Ok(0);
        }
        let table_name = self.current_table.read().clone();
        let mgr = self.fts_manager.read();
        if let Some(m) = mgr.as_ref() {
            if let Ok(engine) = m.get_engine(&table_name) {
                let count = ids.len();
                let doc_ids_u64: Vec<u64> = ids.iter().map(|&id| id as u64).collect();
                // Build owned Vec<String> columns then borrow as &str — zero extra copy
                let owned: Vec<(String, Vec<String>)> = columns.into_iter().collect();
                let columns_ref: Vec<(String, Vec<&str>)> = owned
                    .iter()
                    .map(|(name, vals)| (name.clone(), vals.iter().map(|s| s.as_str()).collect()))
                    .collect();
                py.allow_threads(|| {
                    let _ = engine.add_documents_arrow_str(&doc_ids_u64, columns_ref);
                });
                return Ok(count);
            }
        }
        Ok(0)
    }
}

/// Rust-only helpers shared by the numeric `_id` update fast paths.
impl ApexStorageImpl {
    /// Resolve the backend, numeric column type, and fast-path guards for a
    /// numeric `_id` update target. Pending DeltaStore rows are allowed; the
    /// callers decide whether in-place or DeltaStore writes are appropriate.
    fn numeric_update_backend_and_type(
        &self,
        table_path: &Path,
        table_name: &str,
        column: &str,
    ) -> Option<(Arc<TableStorageBackend>, crate::storage::on_demand::ColumnType)> {
        let backend_cache_key = Self::backend_cache_key(table_path, table_name);
        let cache_key = format!("{}\0{}", backend_cache_key, column);

        let backend_opt: Option<Arc<TableStorageBackend>> = self
            .cached_backends
            .get(&backend_cache_key)
            .map(|v| Arc::clone(&v))
            .or_else(|| {
                crate::Database::cached_backend(table_path).ok().map(|b| {
                    self.cached_backends
                        .insert(backend_cache_key.clone(), Arc::clone(&b));
                    b
                })
            })
            .or_else(|| crate::Database::open_backend(table_path, DurabilityLevel::Fast).ok());
        let backend = backend_opt?;

        let table_epoch = crate::storage::epoch::current(table_path);
        let cached_col_type = self
            .update_by_id_numeric_cache
            .get(&cache_key)
            .and_then(|entry| (entry.value().1 == table_epoch).then_some(entry.value().0));
        if cached_col_type.is_none() {
            self.update_by_id_numeric_cache.remove(&cache_key);
        }
        let col_type = if let Some(col_type) = cached_col_type {
            col_type
        } else {
            let base_dir = table_path
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .to_path_buf();
            if let Ok(index_mgr) = crate::storage::index::IndexManager::load(table_name, &base_dir)
            {
                if index_mgr
                    .list_indexes()
                    .iter()
                    .any(|meta| meta.effective_columns().iter().any(|c| *c == column))
                {
                    return None;
                }
            }
            if backend.storage.has_constraints() {
                return None;
            }
            let Some((_, col_type)) = backend
                .storage
                .get_schema()
                .into_iter()
                .find(|(name, _)| name == column)
            else {
                return None;
            };
            let is_numeric = matches!(
                col_type,
                crate::storage::on_demand::ColumnType::Float64
                    | crate::storage::on_demand::ColumnType::Float32
                    | crate::storage::on_demand::ColumnType::Int64
                    | crate::storage::on_demand::ColumnType::Int32
                    | crate::storage::on_demand::ColumnType::Int16
                    | crate::storage::on_demand::ColumnType::Int8
                    | crate::storage::on_demand::ColumnType::UInt8
                    | crate::storage::on_demand::ColumnType::UInt16
                    | crate::storage::on_demand::ColumnType::UInt32
                    | crate::storage::on_demand::ColumnType::UInt64
                    | crate::storage::on_demand::ColumnType::Timestamp
                    | crate::storage::on_demand::ColumnType::Date
            );
            if !is_numeric {
                return None;
            }
            self.update_by_id_numeric_cache
                .insert(cache_key, (col_type, table_epoch));
            col_type
        };
        Some((backend, col_type))
    }

    /// Resolve the backend and numeric column type for the in-place mmap fast
    /// path, which is only safe when no DeltaStore overlay is pending.
    fn numeric_update_target(
        &self,
        table_path: &Path,
        table_name: &str,
        column: &str,
    ) -> Option<(Arc<TableStorageBackend>, crate::storage::on_demand::ColumnType)> {
        let (backend, col_type) =
            self.numeric_update_backend_and_type(table_path, table_name, column)?;
        if backend.has_pending_deltas() || backend.pending_v4_in_memory_rows() > 0 {
            return None;
        }
        Some((backend, col_type))
    }
}
