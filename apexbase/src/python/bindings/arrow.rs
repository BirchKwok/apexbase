//! PyO3 binding methods split by domain.

use super::*;
use arrow::array::Array;

#[pymethods]
impl ApexStorageImpl {
    fn _execute_arrow_ffi(&self, py: Python<'_>, sql: &str) -> PyResult<(usize, usize)> {
        use crate::query::query_signature::{self, QuerySignature};
        use arrow::array::{Array, StructArray};
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

        let sql = sql.to_string();
        let sig = query_signature::classify(&sql);
        let is_write = matches!(&sig, QuerySignature::DmlWrite | QuerySignature::Ddl { .. });
        let table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();
        // Fall back to base_dir when no table selected (e.g. SELECT * FROM read_csv(...)).
        // Table-function queries don't use the default_table_path at all.
        let table_path = self
            .get_current_table_path()
            .unwrap_or_else(|_| base_dir.clone());
        // File-replacing DDL (TRUNCATE / DROP / ALTER / INSERT OVERWRITE) must
        // run with no per-client backend still mapping the table file, or
        // Windows rejects the file rewrite with OS error 1224.
        self.release_backends_for_file_replacing_sql(&sql);
        // Execute query in Rust thread pool
        let batch = py.allow_threads(|| -> PyResult<RecordBatch> {
            let result = crate::Session::new(&base_dir, &table_path)
                .with_root_dir(&self.root_dir)
                .with_temp_dir(&self.temp_dir)
                .execute_classified(&sql, &sig)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            result
                .to_record_batch()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })?;
        if is_write && !table_name.is_empty() {
            self.invalidate_backend(&table_name);
        }

        // Convert RecordBatch to StructArray for FFI export
        let struct_array: StructArray = batch.into();
        let array_data = struct_array.to_data();

        // Export to FFI
        let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&array_data)
            .map_err(|e| PyRuntimeError::new_err(format!("FFI export failed: {}", e)))?;

        // Leak the FFI structs to get stable pointers (caller must free via _free_arrow_ffi)
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as usize;
        let array_ptr = Box::into_raw(Box::new(ffi_array)) as usize;

        Ok((schema_ptr, array_ptr))
    }

    /// Execute a SELECT and materialize the result directly into a Python dict
    /// of column lists (bypassing the PyArrow FFI round-trip). This is much
    /// faster than `to_pylist()` for small result sets; the caller should only
    /// use it for bounded (LIMIT) queries.
    fn _execute_pylist(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        use crate::query::query_signature::{self, QuerySignature};
        use arrow::array::Array;

        let sql = sql.to_string();
        let sig = query_signature::classify(&sql);
        let is_write = matches!(&sig, QuerySignature::DmlWrite | QuerySignature::Ddl { .. });
        let table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();
        let table_path = self
            .get_current_table_path()
            .unwrap_or_else(|_| base_dir.clone());
        self.release_backends_for_file_replacing_sql(&sql);
        let batch = py.allow_threads(|| -> PyResult<RecordBatch> {
            let result = crate::Session::new(&base_dir, &table_path)
                .with_root_dir(&self.root_dir)
                .with_temp_dir(&self.temp_dir)
                .execute_classified(&sql, &sig)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            result
                .to_record_batch()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })?;
        if is_write && !table_name.is_empty() {
            self.invalidate_backend(&table_name);
        }

        let columns_dict = PyDict::new_bound(py);
        let schema = batch.schema();
        for col_idx in 0..batch.num_columns() {
            let col_name = schema.field(col_idx).name().to_string();
            let arr = batch.column(col_idx);
            let col_list = arrow_col_to_pylist(py, arr)?;
            columns_dict.set_item(col_name, col_list)?;
        }
        Ok(columns_dict.into())
    }

    fn _execute_like_ffi(&self, py: Python<'_>, sql: &str) -> PyResult<(usize, usize)> {
        use crate::query::query_signature::{self, QuerySignature};
        use arrow::array::{Array, StructArray};
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

        let sig = query_signature::classify(sql);
        let (table, col, pattern) = match sig {
            QuerySignature::LikeFilter {
                table,
                column,
                pattern,
            } => (table, column, pattern),
            _ => return Ok((0, 0)),
        };

        let default_table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();
        let default_table_path = if default_table_name.is_empty() {
            base_dir.clone()
        } else {
            self.table_paths
                .read()
                .get(&default_table_name)
                .cloned()
                .unwrap_or_else(|| base_dir.join(format!("{}.apex", default_table_name)))
        };
        let (_, table_path) = self.resolve_signature_table(
            table.as_deref(),
            &default_table_name,
            &default_table_path,
            &base_dir,
        );

        let batch = py.allow_threads(|| -> Option<arrow::record_batch::RecordBatch> {
            let backend = crate::Database::cached_backend(&table_path).ok()?;
            if backend.pending_v4_in_memory_rows() > 0 {
                return None;
            }
            backend
                .scan_like_and_extract_mmap(&col, &pattern, None)
                .ok()
                .flatten()
        });

        let batch = match batch {
            Some(b) if b.num_rows() > 0 => b,
            _ => return Ok((0, 0)),
        };

        // Export via Arrow C Data Interface (zero-copy)
        let struct_array: StructArray = batch.into();
        let array_data = struct_array.to_data();
        let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&array_data)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("FFI export: {}", e)))?;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as usize;
        let array_ptr = Box::into_raw(Box::new(ffi_array)) as usize;
        Ok((schema_ptr, array_ptr))
    }

    fn _free_arrow_ffi(&self, schema_ptr: usize, array_ptr: usize) -> PyResult<()> {
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

        if schema_ptr != 0 {
            unsafe {
                let _ = Box::from_raw(schema_ptr as *mut FFI_ArrowSchema);
            }
        }
        if array_ptr != 0 {
            unsafe {
                let _ = Box::from_raw(array_ptr as *mut FFI_ArrowArray);
            }
        }
        Ok(())
    }

    fn _execute_arrow_ipc(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        use crate::query::query_signature::{self, QuerySignature};
        use arrow::ipc::writer::StreamWriter;
        use pyo3::types::PyBytes;

        let sig = query_signature::classify(sql);
        let is_write = matches!(&sig, QuerySignature::DmlWrite | QuerySignature::Ddl { .. });
        let is_multi = matches!(&sig, QuerySignature::MultiStatement);

        // Single read of current_table — avoids double RwLock acquire in get_current_table_path()
        let table_name = self.current_table.read().clone();
        let table_path = if table_name.is_empty() {
            self.current_base_dir()
        } else {
            self.table_paths
                .read()
                .get(&table_name)
                .cloned()
                .unwrap_or_else(|| self.current_base_dir().join(format!("{}.apex", table_name)))
        };
        let base_dir = self.current_base_dir();
        // FAST PATH: SELECT * LIMIT N — build Arrow batch directly from V4
        if let QuerySignature::SimpleScanLimit {
            limit,
            offset,
            ref table,
        } = &sig
        {
            let (_, target_path) =
                self.resolve_signature_table(table.as_deref(), &table_name, &table_path, &base_dir);
            if let Ok(backend) = crate::Database::cached_backend(&target_path) {
                if backend.pending_v4_in_memory_rows() == 0 {
                    let batch_result = if *offset > 0 {
                        if backend.has_pending_deltas()
                            || backend.has_delta()
                            || backend.active_row_count() != backend.row_count()
                        {
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "simple scan offset fast path unavailable",
                            ))
                        } else {
                            let end = (*offset)
                                .saturating_add(*limit)
                                .min(backend.row_count() as usize);
                            let indices: Vec<usize> = (*offset..end).collect();
                            backend.read_columns_by_indices_to_arrow(&indices, None)
                        }
                    } else {
                        backend
                            .storage
                            .to_arrow_batch_with_limit(None, false, *limit)
                    };
                    if let Ok(batch) = batch_result {
                        if batch.num_rows() > 0 || batch.num_columns() > 0 {
                            let mut buf = Vec::with_capacity(batch.get_array_memory_size() + 256);
                            {
                                let mut writer =
                                    StreamWriter::try_new(&mut buf, batch.schema().as_ref())
                                        .map_err(|e| {
                                            PyRuntimeError::new_err(format!(
                                                "IPC writer error: {}",
                                                e
                                            ))
                                        })?;
                                writer.write(&batch).map_err(|e| {
                                    PyRuntimeError::new_err(format!("IPC write error: {}", e))
                                })?;
                                writer.finish().map_err(|e| {
                                    PyRuntimeError::new_err(format!("IPC finish error: {}", e))
                                })?;
                            }
                            return Ok(PyBytes::new_bound(py, &buf).into());
                        }
                    }
                }
            }
        }

        let sql = sql.to_string();
        let current_txn = *self.current_txn_id.read();
        // Same Windows-safe release as _execute_arrow_ffi: cached per-client
        // backends would keep the table file mapped during TRUNCATE/DROP/ALTER.
        self.release_backends_for_file_replacing_sql(&sql);

        let (batch, new_txn_id) = if is_multi {
            py.allow_threads(|| -> PyResult<(RecordBatch, Option<u64>)> {
                let stmts = crate::query::sql_parser::SqlParser::parse_multi(&sql)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                let (result, final_txn) = crate::Session::new(&base_dir, &table_path)
                    .with_root_dir(&self.root_dir)
                    .with_temp_dir(&self.temp_dir)
                    .execute_multi_with_txn(stmts, current_txn)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                let batch = result
                    .to_record_batch()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok((batch, final_txn))
            })?
        } else {
            let batch = py.allow_threads(|| -> PyResult<RecordBatch> {
                let result = crate::Session::new(&base_dir, &table_path)
                    .with_root_dir(&self.root_dir)
                    .with_temp_dir(&self.temp_dir)
                    .execute_classified(&sql, &sig)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                result
                    .to_record_batch()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })?;
            (batch, current_txn)
        };

        if is_multi && new_txn_id != current_txn {
            *self.current_txn_id.write() = new_txn_id;
        }

        // Serialize to IPC format
        let estimated_size = batch.get_array_memory_size() + 512;
        let mut buf = Vec::with_capacity(estimated_size);
        {
            let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
                .map_err(|e| PyRuntimeError::new_err(format!("IPC writer error: {}", e)))?;
            writer
                .write(&batch)
                .map_err(|e| PyRuntimeError::new_err(format!("IPC write error: {}", e)))?;
            writer
                .finish()
                .map_err(|e| PyRuntimeError::new_err(format!("IPC finish error: {}", e)))?;
        }

        // Invalidate cached backend AFTER write operations
        if (is_write || is_multi) && !table_name.is_empty() {
            self.invalidate_backend(&table_name);
        }

        // After DROP TABLE, remove from table_paths (uses pre-extracted DdlKind — no re-uppercase)
        if let QuerySignature::Ddl {
            kind: crate::query::query_signature::DdlKind::DropTable { ref name },
        } = &sig
        {
            self.table_paths.write().remove(name);
            self.invalidate_backend(name);
            if *self.current_table.read() == *name {
                *self.current_table.write() = String::new();
            }
        }

        // After CREATE TABLE, register the new table (uses pre-extracted DdlKind)
        if let QuerySignature::Ddl {
            kind: crate::query::query_signature::DdlKind::CreateTable { ref name },
        } = &sig
        {
            let tbl_path = self.current_base_dir().join(format!("{}.apex", name));
            self.table_paths.write().insert(name.clone(), tbl_path);
            *self.current_table.write() = name.clone();
        }

        Ok(PyBytes::new_bound(py, &buf).into())
    }

    fn _query_arrow_ffi(
        &self,
        py: Python<'_>,
        where_clause: &str,
        limit: Option<usize>,
    ) -> PyResult<(usize, usize)> {
        use arrow::array::{Array, StructArray};
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

        // Single read of current_table — avoids double RwLock acquire
        let table_name = self.current_table.read().clone();
        if table_name.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "No table selected. Call create_table() or use_table() first.",
            ));
        }
        let table_path = self
            .table_paths
            .read()
            .get(&table_name)
            .cloned()
            .unwrap_or_else(|| self.current_base_dir().join(format!("{}.apex", table_name)));
        let base_dir = self.current_base_dir();
        let where_clause = where_clause.to_string();

        // Build SQL from where clause using current table name
        let sql = if let Some(lim) = limit {
            if where_clause == "1=1" || where_clause.is_empty() {
                format!("SELECT * FROM \"{}\" LIMIT {}", table_name, lim)
            } else {
                format!(
                    "SELECT * FROM \"{}\" WHERE {} LIMIT {}",
                    table_name, where_clause, lim
                )
            }
        } else {
            if where_clause == "1=1" || where_clause.is_empty() {
                format!("SELECT * FROM \"{}\"", table_name)
            } else {
                format!("SELECT * FROM \"{}\" WHERE {}", table_name, where_clause)
            }
        };

        // Execute query
        let batch = py.allow_threads(|| -> PyResult<RecordBatch> {
            let result = crate::Session::new(&base_dir, &table_path)
                .with_root_dir(&self.root_dir)
                .execute(&sql)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            result
                .to_record_batch()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })?;

        // Empty result
        if batch.num_rows() == 0 {
            return Ok((0, 0));
        }

        // Convert to StructArray for FFI
        let struct_array: StructArray = batch.into();
        let array_data = struct_array.to_data();

        let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&array_data)
            .map_err(|e| PyRuntimeError::new_err(format!("FFI export failed: {}", e)))?;

        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as usize;
        let array_ptr = Box::into_raw(Box::new(ffi_array)) as usize;

        Ok((schema_ptr, array_ptr))
    }

    #[pyo3(name = "_topk_distance_ffi")]
    fn topk_distance_ffi(
        &self,
        py: Python<'_>,
        col: &str,
        query_bytes: &[u8],
        k: usize,
        metric: &str,
    ) -> PyResult<(usize, usize)> {
        use crate::compute::vector_ops::bytes_to_query_vec_f32;
        use arrow::array::{Array, StructArray};
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

        let query_f32 = bytes_to_query_vec_f32(query_bytes).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "_topk_distance_ffi: query_bytes must be raw little-endian float32 bytes",
            )
        })?;
        if query_f32.is_empty() || query_f32.iter().any(|value| !value.is_finite()) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "_topk_distance_ffi: query vector must be non-empty and contain only finite values",
            ));
        }

        let table_path = self
            .get_current_table_path()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        // Direct path — no SQL string formatting or parsing overhead.
        let col_owned = col.to_string();
        let metric_str = metric.to_string();
        let names = vec!["_id".to_string(), "dist".to_string()];

        let batch = py.allow_threads(|| -> PyResult<RecordBatch> {
            use crate::compute::vector_ops::{
                topk_heap_direct_parallel, DistanceComputer, DistanceMetric,
            };
            use arrow::array::{ArrayRef, BinaryArray, Float64Array, Int64Array};
            use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

            let metric_enum = DistanceMetric::from_str(&metric_str).ok_or_else(|| {
                PyRuntimeError::new_err(format!(
                    "_topk_distance_ffi: unknown metric '{}'",
                    metric_str
                ))
            })?;

            let backend = crate::Database::cached_backend(&table_path)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let id_field = Field::new(&names[0], ArrowDataType::Int64, false);
            let dist_field = Field::new(&names[1], ArrowDataType::Float64, false);
            let out_schema = std::sync::Arc::new(Schema::new(vec![id_field, dist_field]));

            let computer = DistanceComputer::new(metric_enum, query_f32.clone());

            // FAST PATH: zero-copy scan on OS mmap — no Arrow batch, no memcpy
            let direct_topk = backend
                .topk_fixedlist_direct(&col_owned, &computer, k)
                .ok()
                .flatten()
                .or_else(|| {
                    backend
                        .topk_binary_direct(&col_owned, &computer, k)
                        .ok()
                        .flatten()
                });
            if let Some(topk) = direct_topk {
                if topk.is_empty() {
                    return RecordBatch::try_new(
                        out_schema,
                        vec![
                            std::sync::Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
                            std::sync::Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
                        ],
                    )
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()));
                }
                // Read only the _id column (8MB) to map row indices → IDs
                let id_batch = backend
                    .read_columns_to_arrow(Some(&["_id"]), 0, None)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                let id_col = id_batch.column_by_name("_id");
                let ids: Vec<i64> = topk
                    .iter()
                    .map(|(row_idx, _)| {
                        id_col
                            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                            .map(|a| a.value(*row_idx))
                            .unwrap_or(*row_idx as i64)
                    })
                    .collect();
                let dists: Vec<f64> = topk.iter().map(|(_, d)| *d as f64).collect();
                return RecordBatch::try_new(
                    out_schema,
                    vec![
                        std::sync::Arc::new(Int64Array::from(ids)) as ArrayRef,
                        std::sync::Arc::new(Float64Array::from(dists)) as ArrayRef,
                    ],
                )
                .map_err(|e| PyRuntimeError::new_err(e.to_string()));
            }

            // FALLBACK: Arrow path for Binary columns / compressed RGs
            let needed: &[&str] = &[&col_owned, "_id"];
            let full_batch = backend
                .read_columns_to_arrow(Some(needed), 0, None)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            if full_batch.num_rows() == 0 {
                return RecordBatch::try_new(
                    out_schema,
                    vec![
                        std::sync::Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
                        std::sync::Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
                    ],
                )
                .map_err(|e| PyRuntimeError::new_err(e.to_string()));
            }

            let bin_col = full_batch.column_by_name(&col_owned).ok_or_else(|| {
                PyRuntimeError::new_err(format!("column '{}' not found", col_owned))
            })?;

            let topk = if let Some(fixed_arr) = bin_col
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeListArray>()
            {
                if fixed_arr.value_length() as usize != computer.query.len() {
                    return Err(PyRuntimeError::new_err(format!(
                        "topk_distance: query dimension {} does not match column dimension {}",
                        computer.query.len(),
                        fixed_arr.value_length()
                    )));
                }
                use crate::compute::vector_ops::topk_heap_direct_parallel_fixed;
                topk_heap_direct_parallel_fixed(fixed_arr, &computer, k)
            } else if let Some(bin_arr) = bin_col.as_any().downcast_ref::<BinaryArray>() {
                let expected = computer.query.len() * std::mem::size_of::<f32>();
                if let Some(actual) = (0..bin_arr.len())
                    .find(|&idx| !bin_arr.is_null(idx))
                    .map(|idx| bin_arr.value(idx).len())
                {
                    if actual != expected {
                        return Err(PyRuntimeError::new_err(format!(
                            "topk_distance: query dimension {} does not match column dimension {}",
                            computer.query.len(),
                            actual / std::mem::size_of::<f32>()
                        )));
                    }
                }
                topk_heap_direct_parallel(bin_arr, &computer, k)
            } else {
                return Err(PyRuntimeError::new_err(format!(
                    "column '{}' is not a vector column",
                    col_owned
                )));
            };

            let id_col = full_batch.column_by_name("_id");
            let ids: Vec<i64> = topk
                .iter()
                .map(|(row_idx, _)| {
                    if let Some(arr) = &id_col {
                        if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                            return a.value(*row_idx);
                        }
                    }
                    *row_idx as i64
                })
                .collect();
            let dists: Vec<f64> = topk.iter().map(|(_, d)| *d as f64).collect();

            RecordBatch::try_new(
                out_schema,
                vec![
                    std::sync::Arc::new(Int64Array::from(ids)) as ArrayRef,
                    std::sync::Arc::new(Float64Array::from(dists)) as ArrayRef,
                ],
            )
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })?;

        if batch.num_rows() == 0 {
            return Ok((0, 0));
        }

        let struct_array: StructArray = batch.into();
        let array_data = struct_array.to_data();
        let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&array_data)
            .map_err(|e| PyRuntimeError::new_err(format!("FFI export failed: {}", e)))?;

        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as usize;
        let array_ptr = Box::into_raw(Box::new(ffi_array)) as usize;
        Ok((schema_ptr, array_ptr))
    }

    #[pyo3(name = "_topk_rescore_ffi")]
    fn topk_rescore_ffi(
        &self,
        py: Python<'_>,
        source_col: &str,
        accelerator_col: &str,
        query_bytes: &[u8],
        k: usize,
        candidate_k: usize,
        metric: &str,
    ) -> PyResult<(usize, usize)> {
        use crate::compute::vector_ops::{bytes_to_query_vec_f32, DistanceComputer, DistanceMetric};
        use arrow::array::{ArrayRef, Float64Array, Int64Array, StructArray};
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

        if k == 0 || candidate_k < k {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "candidate_k must be greater than or equal to k",
            ));
        }
        let query = bytes_to_query_vec_f32(query_bytes).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "query_bytes must be raw little-endian float32 bytes",
            )
        })?;
        if query.is_empty() || query.iter().any(|value| !value.is_finite()) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "query vector must be non-empty and contain only finite values",
            ));
        }
        let metric = DistanceMetric::from_str(metric).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("unknown vector distance metric")
        })?;
        let table_path = self
            .get_current_table_path()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let source_col = source_col.to_string();
        let accelerator_col = accelerator_col.to_string();

        let batch = py.allow_threads(|| -> PyResult<RecordBatch> {
            let backend = crate::Database::cached_backend(&table_path)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let derivation = backend
                .storage
                .vector_derivations()
                .into_iter()
                .find(|item| item.target == accelerator_col)
                .ok_or_else(|| {
                    PyRuntimeError::new_err(format!(
                        "column '{}' is not a registered quantized accelerator",
                        accelerator_col
                    ))
                })?;
            if derivation.source != source_col {
                return Err(PyRuntimeError::new_err(format!(
                    "quantized column '{}' derives from '{}', not '{}'",
                    accelerator_col, derivation.source, source_col
                )));
            }
            if derivation.codec_version
                != crate::compute::vector_quantization::TURBOQUANT_CODEC_VERSION as u16
            {
                return Err(PyRuntimeError::new_err(format!(
                    "quantized column '{}' uses unsupported codec version {}",
                    accelerator_col, derivation.codec_version
                )));
            }
            if backend.has_pending_deltas() || backend.pending_v4_in_memory_rows() > 0 {
                return Err(PyRuntimeError::new_err(
                    "quantized rescore requires pending writes to be flushed",
                ));
            }

            let computer = DistanceComputer::new(metric, query);
            let candidates = backend
                .topk_fixedlist_direct(&accelerator_col, &computer, candidate_k)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let candidates = if let Some(candidates) = candidates {
                candidates
            } else {
                // Compressed/legacy row groups cannot expose a fixed-width mmap
                // slice. Preserve correctness by decoding the accelerator via
                // Arrow; uncompressed V4 tables stay on the bounded-memory path.
                let coarse_batch = backend
                    .read_columns_to_arrow(Some(&[&accelerator_col, "_id"]), 0, None)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                let coarse = coarse_batch
                    .column_by_name(&accelerator_col)
                    .and_then(|array| {
                        array
                            .as_any()
                            .downcast_ref::<arrow::array::FixedSizeListArray>()
                    })
                    .ok_or_else(|| {
                        PyRuntimeError::new_err(format!(
                            "quantized accelerator '{}' is not a fixed-size vector",
                            accelerator_col
                        ))
                    })?;
                if coarse.value_length() as usize != computer.query.len() {
                    return Err(PyRuntimeError::new_err(format!(
                        "query dimension {} does not match accelerator dimension {}",
                        computer.query.len(),
                        coarse.value_length()
                    )));
                }
                crate::compute::vector_ops::topk_heap_direct_parallel_fixed(
                    coarse,
                    &computer,
                    candidate_k,
                )
            };

            let out_schema = std::sync::Arc::new(Schema::new(vec![
                Field::new("_id", ArrowDataType::Int64, false),
                Field::new("dist", ArrowDataType::Float64, false),
            ]));
            if candidates.is_empty() {
                return RecordBatch::try_new(
                    out_schema,
                    vec![
                        std::sync::Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
                        std::sync::Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
                    ],
                )
                .map_err(|e| PyRuntimeError::new_err(e.to_string()));
            }

            let candidate_rows = candidates
                .iter()
                .map(|(row, _)| *row)
                .collect::<Vec<_>>();
            let exact_batch = backend
                .read_columns_by_indices_to_arrow(&candidate_rows, Some(&[&source_col, "_id"]))
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let source = exact_batch
                .column_by_name(&source_col)
                .and_then(|array| {
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::FixedSizeListArray>()
                })
                .ok_or_else(|| {
                    PyRuntimeError::new_err(format!(
                        "source column '{}' is not a fixed-size vector",
                        source_col
                    ))
                })?;
            if source.value_length() as usize != computer.query.len() {
                return Err(PyRuntimeError::new_err(format!(
                    "query dimension {} does not match source dimension {}",
                    computer.query.len(),
                    source.value_length()
                )));
            }
            let exact = crate::compute::vector_ops::topk_heap_direct_parallel_fixed(
                source,
                &computer,
                k,
            );
            let ids = exact_batch
                .column_by_name("_id")
                .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| PyRuntimeError::new_err("candidate ID column is missing"))?;
            let result_ids = exact
                .iter()
                .map(|(candidate_index, _)| ids.value(*candidate_index))
                .collect::<Vec<_>>();
            let distances = exact
                .iter()
                .map(|(_, distance)| *distance as f64)
                .collect::<Vec<_>>();
            RecordBatch::try_new(
                out_schema,
                vec![
                    std::sync::Arc::new(Int64Array::from(result_ids)) as ArrayRef,
                    std::sync::Arc::new(Float64Array::from(distances)) as ArrayRef,
                ],
            )
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })?;

        if batch.num_rows() == 0 {
            return Ok((0, 0));
        }
        let struct_array: StructArray = batch.into();
        let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&struct_array.to_data())
            .map_err(|e| PyRuntimeError::new_err(format!("FFI export failed: {}", e)))?;
        Ok((
            Box::into_raw(Box::new(ffi_schema)) as usize,
            Box::into_raw(Box::new(ffi_array)) as usize,
        ))
    }

    #[pyo3(name = "_batch_topk_ffi")]
    fn batch_topk_ffi(
        &self,
        py: Python<'_>,
        col: &str,
        queries_bytes: &[u8],
        n_queries: usize,
        k: usize,
        metric: &str,
    ) -> PyResult<PyObject> {
        use crate::compute::vector_ops::DistanceMetric;
        use arrow::array::Int64Array;
        use pyo3::types::PyBytes;

        if n_queries == 0 || k == 0 {
            let empty: Vec<u8> = vec![];
            return Ok(PyBytes::new_bound(py, &empty).into());
        }
        if queries_bytes.len() % 4 != 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "_batch_topk_ffi: queries_bytes length must be a multiple of 4",
            ));
        }
        let total_floats = queries_bytes.len() / 4;
        if total_floats % n_queries != 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "_batch_topk_ffi: queries_bytes length must be divisible by n_queries",
            ));
        }
        let dim = total_floats / n_queries;

        // Parse raw LE f32 bytes into Vec<f32>
        let queries_f32: Vec<f32> = queries_bytes
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        if dim == 0 || queries_f32.iter().any(|value| !value.is_finite()) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "_batch_topk_ffi: query vectors must be non-empty and contain only finite values",
            ));
        }

        let table_path = self
            .get_current_table_path()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let col_owned = col.to_string();
        let metric_str = metric.to_string();
        let n_q = n_queries;

        let (all_results, ids_map) =
            py.allow_threads(|| -> PyResult<(Vec<Vec<(usize, f32)>>, Vec<i64>)> {
                let metric_enum = DistanceMetric::from_str(&metric_str).ok_or_else(|| {
                    PyRuntimeError::new_err(format!(
                        "_batch_topk_ffi: unknown metric '{}'",
                        metric_str
                    ))
                })?;

                let backend = crate::Database::cached_backend(&table_path)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                // FAST PATH: mmap direct scan (FixedList → Binary fallback)
                let batch_results = backend
                    .batch_topk_fixedlist_direct(&col_owned, &queries_f32, n_q, k, metric_enum)
                    .ok()
                    .flatten()
                    .or_else(|| {
                        backend
                            .batch_topk_binary_direct(&col_owned, &queries_f32, n_q, k, metric_enum)
                            .ok()
                            .flatten()
                    });

                let all_results: Vec<Vec<(usize, f32)>> = if let Some(r) = batch_results {
                    r
                } else {
                    // FALLBACK: load Arrow batch, run batch topk on FixedSizeListArray / BinaryArray
                    use crate::compute::vector_ops::{
                        topk_heap_direct_parallel, topk_heap_direct_parallel_fixed,
                        DistanceComputer,
                    };
                    use arrow::array::{BinaryArray, FixedSizeListArray};

                    let needed: &[&str] = &[&col_owned, "_id"];
                    let full_batch = backend
                        .read_columns_to_arrow(Some(needed), 0, None)
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                    if full_batch.num_rows() == 0 {
                        return Ok((vec![vec![]; n_q], vec![]));
                    }

                    let bin_col = full_batch.column_by_name(&col_owned).ok_or_else(|| {
                        PyRuntimeError::new_err(format!("column '{}' not found", col_owned))
                    })?;
                    if let Some(fixed_arr) =
                        bin_col.as_any().downcast_ref::<FixedSizeListArray>()
                    {
                        if fixed_arr.value_length() as usize != dim {
                            return Err(PyRuntimeError::new_err(format!(
                                "topk_distance: query dimension {} does not match column dimension {}",
                                dim,
                                fixed_arr.value_length()
                            )));
                        }
                    } else if let Some(bin_arr) =
                        bin_col.as_any().downcast_ref::<BinaryArray>()
                    {
                        let expected = dim * std::mem::size_of::<f32>();
                        if let Some(actual) = (0..bin_arr.len())
                            .find(|&idx| !bin_arr.is_null(idx))
                            .map(|idx| bin_arr.value(idx).len())
                        {
                            if actual != expected {
                                return Err(PyRuntimeError::new_err(format!(
                                    "topk_distance: query dimension {} does not match column dimension {}",
                                    dim,
                                    actual / std::mem::size_of::<f32>()
                                )));
                            }
                        }
                    }

                    // Run N queries sequentially (Arrow fallback — uncommon path)
                    let mut results = Vec::with_capacity(n_q);
                    for qi in 0..n_q {
                        let q = queries_f32[qi * dim..(qi + 1) * dim].to_vec();
                        let computer = DistanceComputer::new(metric_enum, q);
                        let topk = if let Some(fixed_arr) =
                            bin_col.as_any().downcast_ref::<FixedSizeListArray>()
                        {
                            topk_heap_direct_parallel_fixed(fixed_arr, &computer, k)
                        } else if let Some(bin_arr) = bin_col.as_any().downcast_ref::<BinaryArray>()
                        {
                            topk_heap_direct_parallel(bin_arr, &computer, k)
                        } else {
                            return Err(PyRuntimeError::new_err(format!(
                                "column '{}' is not a vector column",
                                col_owned
                            )));
                        };
                        results.push(topk);
                    }

                    let id_col = full_batch.column_by_name("_id");
                    let n_rows = full_batch.num_rows();
                    let ids: Vec<i64> = (0..n_rows)
                        .map(|i| {
                            id_col
                                .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                                .map(|a| a.value(i))
                                .unwrap_or(i as i64)
                        })
                        .collect();
                    return Ok((results, ids));
                };

                // Read _id column once to map row_idx → _id
                let id_batch = backend
                    .read_columns_to_arrow(Some(&["_id"]), 0, None)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                let n_rows = id_batch.num_rows();
                let id_col = id_batch.column_by_name("_id");
                let ids: Vec<i64> = (0..n_rows)
                    .map(|i| {
                        id_col
                            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                            .map(|a| a.value(i))
                            .unwrap_or(i as i64)
                    })
                    .collect();

                Ok((all_results, ids))
            })?;

        // Encode results as flat f64 bytes: (N × K × 2), row-major
        // [i, j, 0] = id (as f64), [i, j, 1] = dist (as f64)
        // Pad with (-1.0, f64::INFINITY) when fewer than k neighbours found.
        let out_len = n_queries * k * 2;
        let mut out: Vec<u8> = Vec::with_capacity(out_len * 8);
        for qi in 0..n_queries {
            let row = if qi < all_results.len() {
                &all_results[qi]
            } else {
                &[][..]
            };
            for j in 0..k {
                let (id_f64, dist_f64) = if j < row.len() {
                    let (row_idx, dist) = row[j];
                    let id = if row_idx < ids_map.len() {
                        ids_map[row_idx]
                    } else {
                        row_idx as i64
                    };
                    (id as f64, dist as f64)
                } else {
                    (-1.0f64, f64::INFINITY)
                };
                out.extend_from_slice(&id_f64.to_le_bytes());
                out.extend_from_slice(&dist_f64.to_le_bytes());
            }
        }

        Ok(PyBytes::new_bound(py, &out).into())
    }
}
