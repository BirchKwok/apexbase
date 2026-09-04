//! PyO3 binding methods split by domain.

use super::*;

#[pymethods]
impl ApexStorageImpl {
    fn _query_route_family(&self, sql: &str) -> &'static str {
        use crate::query::query_signature::{self, QuerySignature};

        match query_signature::classify(sql) {
            QuerySignature::DmlWrite | QuerySignature::Ddl { .. } => "write",
            QuerySignature::Transaction => "transaction",
            QuerySignature::MultiStatement => "multi",
            QuerySignature::SessionCommand => "session",
            _ => "read",
        }
    }

    fn execute(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        use crate::query::query_signature::{self, QuerySignature};

        let sig = query_signature::classify(sql);
        let is_write = matches!(&sig, QuerySignature::DmlWrite | QuerySignature::Ddl { .. });

        // Single read of current_table — avoids 3x RwLock acquire + String clone
        let table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();
        let table_path = if table_name.is_empty() {
            base_dir.clone()
        } else {
            self.table_paths
                .read()
                .get(&table_name)
                .cloned()
                .unwrap_or_else(|| base_dir.join(format!("{}.apex", table_name)))
        };

        // ── PRIMARY-KEY READ FAST PATHS ──
        // Point (`WHERE _id = N`) and batch (`WHERE _id IN (...)`) lookups run
        // directly on the mmap column reader in a single call. These paths hold
        // the GIL (the reads are microseconds) and never parse the SQL again:
        // the signature came from `classify()` above.
        if let Some(result) =
            self.try_pk_read(py, &sig, &table_name, &table_path, &base_dir, false)?
        {
            return Ok(result);
        }

        // ── FAST PATH: SELECT ... WHERE string_col = 'value' LIMIT N [OFFSET M] ──
        if let QuerySignature::StringEqualityFilterLimit {
            ref table,
            column,
            value,
            limit,
            offset,
        } = &sig
        {
            let (target_table, target_path) =
                self.resolve_signature_table(table.as_deref(), &table_name, &table_path, &base_dir);
            let maybe_backend = self
                .cached_backends
                .get(&target_table)
                .map(|v| Arc::clone(&v))
                .or_else(|| {
                    crate::Database::cached_backend(&target_path).ok().map(|b| {
                        self.cached_backends
                            .insert(target_table.clone(), Arc::clone(&b));
                        b
                    })
                });

            if let Some(backend) = maybe_backend.filter(|backend| {
                matches!(
                    backend.get_column_type(column),
                    Some(crate::data::DataType::String)
                )
            }) {
                let can_use_limit_scan = !backend.has_pending_deltas()
                    || (backend.pending_delta_delete_count() == 0
                        && !backend.pending_delta_updates_column(column));
                if can_use_limit_scan {
                    if *limit == 1 && *offset == 0 {
                        let row_id_result =
                            py.allow_threads(|| backend.first_row_id_for_string_eq(column, value));
                        if let Ok(Some(row_id)) = row_id_result {
                            let vals_result = py
                                .allow_threads(|| backend.storage.retrieve_rcix(row_id))
                                .ok()
                                .flatten()
                                .or_else(|| {
                                    py.allow_threads(|| {
                                        backend.storage.read_row_by_id_values(row_id)
                                    })
                                    .ok()
                                    .flatten()
                                });
                            if let Some(vals) = vals_result {
                                let out = PyDict::new_bound(py);
                                let columns_dict = values_to_columns_dict(py, &vals)?;
                                out.set_item("columns_dict", columns_dict)?;
                                out.set_item("rows_affected", 0i64)?;
                                return Ok(out.into());
                            }
                        }
                    }

                    let batch_result = py.allow_threads(|| {
                        backend.read_columns_filtered_string_with_limit_to_arrow(
                            None, column, value, true, *limit, *offset,
                        )
                    });
                    if let Ok(batch) = batch_result {
                        let out = PyDict::new_bound(py);
                        let columns_dict = PyDict::new_bound(py);
                        let schema = batch.schema();
                        for col_idx in 0..batch.num_columns() {
                            let col_name = schema.field(col_idx).name();
                            let arr = batch.column(col_idx);
                            let col_list = arrow_col_to_pylist(py, arr)?;
                            columns_dict.set_item(col_name, col_list)?;
                        }
                        out.set_item("columns_dict", columns_dict)?;
                        out.set_item("rows_affected", 0i64)?;
                        return Ok(out.into());
                    }
                }
            }
        }

        // ── FAST PATH: projected string equality + LIMIT ──
        if let QuerySignature::ProjectedStringEqualityFilterLimit {
            ref table,
            columns,
            column,
            value,
            limit,
            offset,
        } = &sig
        {
            let (target_table, target_path) =
                self.resolve_signature_table(table.as_deref(), &table_name, &table_path, &base_dir);
            let maybe_backend = self
                .cached_backends
                .get(&target_table)
                .map(|v| Arc::clone(&v))
                .or_else(|| {
                    crate::Database::cached_backend(&target_path).ok().map(|b| {
                        self.cached_backends
                            .insert(target_table.clone(), Arc::clone(&b));
                        b
                    })
                });

            if let Some(backend) = maybe_backend.filter(|backend| {
                matches!(
                    backend.get_column_type(column),
                    Some(crate::data::DataType::String)
                )
            }) {
                let can_use_limit_scan = !backend.has_pending_deltas()
                    || (backend.pending_delta_delete_count() == 0
                        && !backend.pending_delta_updates_column(column));
                if can_use_limit_scan {
                    if *limit == 1 && *offset == 0 {
                        let row_id_result =
                            py.allow_threads(|| backend.first_row_id_for_string_eq(column, value));
                        if let Ok(Some(row_id)) = row_id_result {
                            let vals_result = py
                                .allow_threads(|| backend.storage.retrieve_rcix(row_id))
                                .ok()
                                .flatten()
                                .or_else(|| {
                                    py.allow_threads(|| {
                                        backend.storage.read_row_by_id_values(row_id)
                                    })
                                    .ok()
                                    .flatten()
                                });
                            if let Some(vals) = vals_result {
                                if let Some(columns_dict) =
                                    projected_values_to_columns_dict(py, &vals, columns)?
                                {
                                    let out = PyDict::new_bound(py);
                                    out.set_item("columns_dict", columns_dict)?;
                                    out.set_item("rows_affected", 0i64)?;
                                    return Ok(out.into());
                                }
                            }
                        }
                    }

                    let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
                    let batch_result = py.allow_threads(|| {
                        backend.read_columns_filtered_string_with_limit_to_arrow(
                            Some(col_refs.as_slice()),
                            column,
                            value,
                            true,
                            *limit,
                            *offset,
                        )
                    });
                    if let Ok(batch) = batch_result {
                        let out = PyDict::new_bound(py);
                        let columns_dict = PyDict::new_bound(py);
                        let schema = batch.schema();
                        for col_idx in 0..batch.num_columns() {
                            let col_name = schema.field(col_idx).name();
                            let arr = batch.column(col_idx);
                            let col_list = arrow_col_to_pylist(py, arr)?;
                            columns_dict.set_item(col_name, col_list)?;
                        }
                        out.set_item("columns_dict", columns_dict)?;
                        out.set_item("rows_affected", 0i64)?;
                        return Ok(out.into());
                    }
                }
            }
        }

        // ── FAST PATH: SELECT * ... WHERE numeric_col <op> value LIMIT N [OFFSET M] ──
        if let QuerySignature::NumericRangeFilterLimit {
            ref table,
            column,
            low,
            high,
            limit,
            offset,
        } = &sig
        {
            if self.current_txn_id.read().is_none() {
                let (target_table, target_path) = self.resolve_signature_table(
                    table.as_deref(),
                    &table_name,
                    &table_path,
                    &base_dir,
                );
                let maybe_backend = self
                    .cached_backends
                    .get(&target_table)
                    .map(|v| Arc::clone(&v))
                    .or_else(|| {
                        crate::Database::cached_backend(&target_path).ok().map(|b| {
                            self.cached_backends
                                .insert(target_table.clone(), Arc::clone(&b));
                            b
                        })
                    });

                if let Some(backend) = maybe_backend {
                    if !backend.has_pending_deltas() && !backend.has_delta() {
                        let needed = (*offset).saturating_add(*limit);
                        let cols_result = py.allow_threads(
                            || -> std::io::Result<
                                Option<crate::storage::on_demand::MmapBatchColumns>,
                            > {
                                let Some(indices) = backend.scan_numeric_range_mmap(
                                    column,
                                    *low,
                                    *high,
                                    Some(needed),
                                )?
                                else {
                                    return Ok(None);
                                };
                                let final_indices: Vec<usize> =
                                    indices.into_iter().skip(*offset).take(*limit).collect();
                                backend
                                    .storage
                                    .extract_rows_by_indices_mmap_columns(&final_indices, None)
                            },
                        );

                        if let Ok(Some(batch_cols)) = cols_result {
                            if let Some(columns_dict) =
                                mmap_batch_columns_to_pydict(py, batch_cols, None)?
                            {
                                let out = PyDict::new_bound(py);
                                out.set_item("columns_dict", columns_dict)?;
                                out.set_item("rows_affected", 0i64)?;
                                return Ok(out.into());
                            }
                        } else if let Ok(Some(final_indices)) =
                            py.allow_threads(|| -> std::io::Result<Option<Vec<usize>>> {
                                let Some(indices) = backend.scan_numeric_range_mmap(
                                    column,
                                    *low,
                                    *high,
                                    Some(needed),
                                )?
                                else {
                                    return Ok(None);
                                };
                                Ok(Some(
                                    indices.into_iter().skip(*offset).take(*limit).collect(),
                                ))
                            })
                        {
                            let batch_result =
                                py.allow_threads(|| -> std::io::Result<Option<RecordBatch>> {
                                    if final_indices.is_empty() {
                                        backend.read_columns_to_arrow(None, 0, Some(0)).map(Some)
                                    } else {
                                        backend
                                            .read_columns_by_indices_to_arrow(&final_indices, None)
                                            .map(Some)
                                    }
                                });

                            if let Ok(Some(batch)) = batch_result {
                                let out = PyDict::new_bound(py);
                                let columns_dict = PyDict::new_bound(py);
                                let schema = batch.schema();
                                for col_idx in 0..batch.num_columns() {
                                    let col_name = schema.field(col_idx).name();
                                    let arr = batch.column(col_idx);
                                    let col_list = arrow_col_to_pylist(py, arr)?;
                                    columns_dict.set_item(col_name, col_list)?;
                                }
                                out.set_item("columns_dict", columns_dict)?;
                                out.set_item("rows_affected", 0i64)?;
                                return Ok(out.into());
                            }
                        }
                    }
                }
            }
        }

        // ── FAST PATH: projected numeric comparison + LIMIT ──
        if let QuerySignature::ProjectedNumericRangeFilterLimit {
            ref table,
            columns,
            column,
            low,
            high,
            limit,
            offset,
        } = &sig
        {
            if self.current_txn_id.read().is_none() {
                let (target_table, target_path) = self.resolve_signature_table(
                    table.as_deref(),
                    &table_name,
                    &table_path,
                    &base_dir,
                );
                let maybe_backend = self
                    .cached_backends
                    .get(&target_table)
                    .map(|v| Arc::clone(&v))
                    .or_else(|| {
                        crate::Database::cached_backend(&target_path).ok().map(|b| {
                            self.cached_backends
                                .insert(target_table.clone(), Arc::clone(&b));
                            b
                        })
                    });

                if let Some(backend) = maybe_backend {
                    if !backend.has_pending_deltas() && !backend.has_delta() {
                        let needed = (*offset).saturating_add(*limit);
                        let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
                        let cols_result = py.allow_threads(
                            || -> std::io::Result<
                                Option<crate::storage::on_demand::MmapBatchColumns>,
                            > {
                                let Some(indices) = backend.scan_numeric_range_mmap(
                                    column,
                                    *low,
                                    *high,
                                    Some(needed),
                                )?
                                else {
                                    return Ok(None);
                                };
                                let final_indices: Vec<usize> =
                                    indices.into_iter().skip(*offset).take(*limit).collect();
                                backend.storage.extract_rows_by_indices_mmap_columns(
                                    &final_indices,
                                    Some(col_refs.as_slice()),
                                )
                            }
                        );

                        if let Ok(Some(batch_cols)) = cols_result {
                            if let Some(columns_dict) =
                                mmap_batch_columns_to_pydict(py, batch_cols, Some(columns))?
                            {
                                let out = PyDict::new_bound(py);
                                out.set_item("columns_dict", columns_dict)?;
                                out.set_item("rows_affected", 0i64)?;
                                return Ok(out.into());
                            }
                        } else if let Ok(Some(final_indices)) =
                            py.allow_threads(|| -> std::io::Result<Option<Vec<usize>>> {
                                let Some(indices) = backend.scan_numeric_range_mmap(
                                    column,
                                    *low,
                                    *high,
                                    Some(needed),
                                )?
                                else {
                                    return Ok(None);
                                };
                                Ok(Some(
                                    indices.into_iter().skip(*offset).take(*limit).collect(),
                                ))
                            })
                        {
                            let batch_result =
                                py.allow_threads(|| -> std::io::Result<Option<RecordBatch>> {
                                    if final_indices.is_empty() {
                                        backend
                                            .read_columns_to_arrow(
                                                Some(col_refs.as_slice()),
                                                0,
                                                Some(0),
                                            )
                                            .map(Some)
                                    } else {
                                        backend
                                            .read_columns_by_indices_to_arrow(
                                                &final_indices,
                                                Some(col_refs.as_slice()),
                                            )
                                            .map(Some)
                                    }
                                });

                            if let Ok(Some(batch)) = batch_result {
                                let out = PyDict::new_bound(py);
                                let columns_dict = PyDict::new_bound(py);
                                let schema = batch.schema();
                                for col_idx in 0..batch.num_columns() {
                                    let col_name = schema.field(col_idx).name();
                                    let arr = batch.column(col_idx);
                                    let col_list = arrow_col_to_pylist(py, arr)?;
                                    columns_dict.set_item(col_name, col_list)?;
                                }
                                out.set_item("columns_dict", columns_dict)?;
                                out.set_item("rows_affected", 0i64)?;
                                return Ok(out.into());
                            }
                        }
                    }
                }
            }
        }

        // ── FAST PATH: SELECT * LIMIT N — pread RCIX, returns columnar dict ──
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
                    let limit = *limit;
                    let offset = *offset;
                    let batch_result = py.allow_threads(|| {
                        if offset > 0 {
                            if backend.has_pending_deltas()
                                || backend.has_delta()
                                || backend.active_row_count() != backend.row_count()
                            {
                                Ok(None)
                            } else {
                                let end = offset
                                    .saturating_add(limit)
                                    .min(backend.row_count() as usize);
                                let indices: Vec<usize> = (offset..end).collect();
                                backend
                                    .read_columns_by_indices_to_arrow(&indices, None)
                                    .map(Some)
                            }
                        } else {
                            match backend.storage.get_or_load_footer() {
                                Ok(Some(footer)) => {
                                    let col_indices: Vec<usize> =
                                        (0..footer.schema.column_count()).collect();
                                    backend.storage.to_arrow_batch_pread_rcix(
                                        &col_indices,
                                        true,
                                        limit,
                                    )
                                }
                                _ => Ok(None),
                            }
                        }
                    });
                    if let Ok(Some(batch)) = batch_result {
                        if batch.num_rows() > 0 {
                            let out = PyDict::new_bound(py);
                            let columns_dict = PyDict::new_bound(py);
                            let schema = batch.schema();
                            for col_idx in 0..batch.num_columns() {
                                let col_name = schema.field(col_idx).name();
                                let arr = batch.column(col_idx);
                                let col_list = arrow_col_to_pylist(py, arr)?;
                                columns_dict.set_item(col_name, col_list)?;
                            }
                            out.set_item("columns_dict", columns_dict)?;
                            out.set_item("rows_affected", 0i64)?;
                            return Ok(out.into());
                        }
                    }
                }
            }
        }

        // ── FAST PATH: SELECT col1, col2 FROM table — projected full scan ──
        if let QuerySignature::ProjectedFullScan { ref table, columns } = &sig {
            if self.current_txn_id.read().is_none() {
                let (_, target_path) = self.resolve_signature_table(
                    table.as_deref(),
                    &table_name,
                    &table_path,
                    &base_dir,
                );
                if let Ok(backend) = crate::Database::cached_backend(&target_path) {
                    if backend.pending_v4_in_memory_rows() == 0 {
                        let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
                        let batch_result = py.allow_threads(|| {
                            backend.read_columns_to_arrow(Some(col_refs.as_slice()), 0, None)
                        });
                        if let Ok(batch) = batch_result {
                            if batch.num_rows() > 0 {
                                let out = PyDict::new_bound(py);
                                let columns_dict = PyDict::new_bound(py);
                                let schema = batch.schema();
                                for col_idx in 0..batch.num_columns() {
                                    let col_name = schema.field(col_idx).name();
                                    let arr = batch.column(col_idx);
                                    let col_list = arrow_col_to_pylist(py, arr)?;
                                    columns_dict.set_item(col_name, col_list)?;
                                }
                                out.set_item("columns_dict", columns_dict)?;
                                out.set_item("rows_affected", 0i64)?;
                                return Ok(out.into());
                            }
                        }
                    }
                }
            }
        }

        // ── FAST PATH: SELECT * WHERE col > N LIMIT M — numeric range filter ──
        if let QuerySignature::NumericRangeFilterLimit {
            ref table,
            column,
            low,
            high,
            limit,
            offset,
        } = &sig
        {
            if self.current_txn_id.read().is_none() {
                let (_, target_path) = self.resolve_signature_table(
                    table.as_deref(),
                    &table_name,
                    &table_path,
                    &base_dir,
                );
                if let Ok(backend) = crate::Database::cached_backend(&target_path) {
                    if !backend.has_pending_deltas()
                        && !backend.has_delta()
                        && backend.pending_v4_in_memory_rows() == 0
                    {
                        let needed = (*offset).saturating_add(*limit);
                        let cols_result = py.allow_threads(
                            || -> std::io::Result<
                                Option<crate::storage::on_demand::MmapBatchColumns>,
                            > {
                                let Some(indices) = backend.scan_numeric_range_mmap(
                                    column,
                                    *low,
                                    *high,
                                    Some(needed),
                                )?
                                else {
                                    return Ok(None);
                                };
                                let final_indices: Vec<usize> =
                                    indices.into_iter().skip(*offset).take(*limit).collect();
                                backend
                                    .storage
                                    .extract_rows_by_indices_mmap_columns(&final_indices, None)
                            }
                        );
                        if let Ok(Some(batch_cols)) = cols_result {
                            if let Some(columns_dict) =
                                mmap_batch_columns_to_pydict(py, batch_cols, None)?
                            {
                                let out = PyDict::new_bound(py);
                                out.set_item("columns_dict", columns_dict)?;
                                out.set_item("rows_affected", 0i64)?;
                                return Ok(out.into());
                            }
                        } else if let Ok(Some(indices)) = py.allow_threads(|| {
                            backend.scan_numeric_range_mmap(column, *low, *high, Some(needed))
                        }) {
                            let final_indices: Vec<usize> =
                                indices.into_iter().skip(*offset).take(*limit).collect();
                            let batch_result = py.allow_threads(|| {
                                if final_indices.is_empty() {
                                    backend.read_columns_to_arrow(None, 0, Some(0))
                                } else {
                                    backend.read_columns_by_indices_to_arrow(&final_indices, None)
                                }
                            });
                            if let Ok(batch) = batch_result {
                                if batch.num_rows() > 0 {
                                    let out = PyDict::new_bound(py);
                                    let columns_dict = PyDict::new_bound(py);
                                    let schema = batch.schema();
                                    for col_idx in 0..batch.num_columns() {
                                        let col_name = schema.field(col_idx).name();
                                        let arr = batch.column(col_idx);
                                        let col_list = arrow_col_to_pylist(py, arr)?;
                                        columns_dict.set_item(col_name, col_list)?;
                                    }
                                    out.set_item("columns_dict", columns_dict)?;
                                    out.set_item("rows_affected", 0i64)?;
                                    return Ok(out.into());
                                }
                            }
                        }
                    }
                }
            }
        }

        // ── FAST PATH: Filtered string equality aggregation (pre-parse) ──
        if let QuerySignature::FilteredStringAgg {
            ref table,
            ref filter_column,
            ref filter_value,
        } = &sig
        {
            if self.current_txn_id.read().is_none() {
                let (_, target_path) = self.resolve_signature_table(
                    table.as_deref(),
                    &table_name,
                    &table_path,
                    &base_dir,
                );
                if let Ok(backend) = crate::Database::cached_backend(&target_path) {
                    if backend.is_mmap_only()
                        && !backend.has_pending_deltas()
                        && !backend.has_delta()
                        && backend.pending_v4_in_memory_rows() == 0
                        && matches!(
                            backend.get_column_type(filter_column),
                            Some(crate::data::DataType::String)
                        )
                    {
                        let filter_col = filter_column.clone();
                        let filter_val = filter_value.clone();
                        // Parse aggregation expressions from SQL
                        if let Some(agg_exprs) = parse_agg_select(sql) {
                            // Collect unique columns needed by the storage fast path.
                            // Add "*" when COUNT(*) / COUNT(1) is present so the storage
                            // layer returns the true match count without an extra scan.
                            let mut unique_cols: Vec<String> = Vec::new();
                            for (func, col, _alias) in &agg_exprs {
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
                            let col_refs: Vec<&str> =
                                unique_cols.iter().map(|s| s.as_str()).collect();
                            // Single-pass: scan string filter + aggregate in one sequential pass
                            let agg_result = py.allow_threads(|| {
                                backend.execute_filtered_string_agg_mmap(
                                    &filter_col,
                                    &filter_val,
                                    &col_refs,
                                )
                            });
                            if let Ok(Some(stats)) = agg_result {
                                if stats.iter().all(|stat| stat.0 == 0) {
                                    // Fall through to the SQL executor. After REINDEX, this
                                    // pre-parse shortcut can observe a stale mmap dictionary
                                    // view even though the generic filter path is correct.
                                } else {
                                    // Build stat lookup: column name -> (count, sum, min, max, is_int)
                                    let mut stat_map: std::collections::HashMap<
                                        &str,
                                        (i64, f64, f64, f64, bool),
                                    > = std::collections::HashMap::new();
                                    for (i, col_name) in col_refs.iter().enumerate() {
                                        if i < stats.len() {
                                            stat_map.insert(col_name, stats[i]);
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
                                                        stat_map
                                                            .get(c.as_str())
                                                            .map(|s| s.0)
                                                            .unwrap_or(0)
                                                    }
                                                } else {
                                                    match_count
                                                };
                                                columns_dict.set_item(
                                                    &output_name,
                                                    PyList::new_bound(py, &[count]),
                                                )?;
                                            }
                                            "SUM" | "AVG" | "MIN" | "MAX" => {
                                                if let Some(c) = col {
                                                    let (count, sum, min_v, max_v, is_int) =
                                                        stat_map
                                                            .get(c.as_str())
                                                            .copied()
                                                            .unwrap_or((0, 0.0, 0.0, 0.0, false));
                                                    match func.as_str() {
                                                        "SUM" => {
                                                            if is_int {
                                                                columns_dict.set_item(
                                                                    &output_name,
                                                                    PyList::new_bound(
                                                                        py,
                                                                        &[sum as i64],
                                                                    ),
                                                                )?;
                                                            } else {
                                                                columns_dict.set_item(
                                                                    &output_name,
                                                                    PyList::new_bound(py, &[sum]),
                                                                )?;
                                                            }
                                                        }
                                                        "AVG" => {
                                                            let avg = if count > 0 {
                                                                sum / count as f64
                                                            } else {
                                                                0.0
                                                            };
                                                            columns_dict.set_item(
                                                                &output_name,
                                                                PyList::new_bound(py, &[avg]),
                                                            )?;
                                                        }
                                                        "MIN" => {
                                                            if is_int {
                                                                columns_dict.set_item(
                                                                    &output_name,
                                                                    PyList::new_bound(
                                                                        py,
                                                                        &[min_v as i64],
                                                                    ),
                                                                )?;
                                                            } else {
                                                                columns_dict.set_item(
                                                                    &output_name,
                                                                    PyList::new_bound(py, &[min_v]),
                                                                )?;
                                                            }
                                                        }
                                                        "MAX" => {
                                                            if is_int {
                                                                columns_dict.set_item(
                                                                    &output_name,
                                                                    PyList::new_bound(
                                                                        py,
                                                                        &[max_v as i64],
                                                                    ),
                                                                )?;
                                                            } else {
                                                                columns_dict.set_item(
                                                                    &output_name,
                                                                    PyList::new_bound(py, &[max_v]),
                                                                )?;
                                                            }
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    out.set_item("columns_dict", columns_dict)?;
                                    out.set_item("rows_affected", 0i64)?;
                                    return Ok(out.into());
                                }
                            }
                        }
                    }
                }
            }
        }

        // ── Transaction handling (single uppercase pass) ──
        let is_txn = matches!(&sig, QuerySignature::Transaction);
        let txn_upper = if is_txn {
            sql.trim().to_ascii_uppercase()
        } else {
            String::new()
        };
        let is_begin = is_txn && txn_upper.starts_with("BEGIN");
        let is_commit = is_txn && (txn_upper == "COMMIT" || txn_upper == "COMMIT;");
        let is_rollback = is_txn
            && (txn_upper == "ROLLBACK" || txn_upper == "ROLLBACK;")
            && !txn_upper.starts_with("ROLLBACK TO");
        let is_savepoint = is_txn && txn_upper.starts_with("SAVEPOINT ");
        let is_rollback_to = is_txn && txn_upper.starts_with("ROLLBACK TO");
        let is_release = is_txn && txn_upper.starts_with("RELEASE");

        let current_txn = *self.current_txn_id.read();
        let is_txn_dml = current_txn.is_some() && matches!(&sig, QuerySignature::DmlWrite);
        let is_txn_select = current_txn.is_some()
            && !is_write
            && !is_txn
            && matches!(
                &sig,
                QuerySignature::Complex
                    | QuerySignature::CountStar { .. }
                    | QuerySignature::PointLookup { .. }
                    | QuerySignature::ProjectedPointLookup { .. }
                    | QuerySignature::SimpleScanLimit { .. }
                    | QuerySignature::ProjectedScanLimit { .. }
                    | QuerySignature::IdBatchLookup { .. }
                    | QuerySignature::ProjectedIdBatchLookup { .. }
                    | QuerySignature::FullScan { .. }
                    | QuerySignature::ProjectedFullScan { .. }
                    | QuerySignature::StringEqualityFilter { .. }
                    | QuerySignature::StringEqualityFilterLimit { .. }
                    | QuerySignature::ProjectedStringEqualityFilter { .. }
                    | QuerySignature::ProjectedStringEqualityFilterLimit { .. }
                    | QuerySignature::NumericRangeFilterLimit { .. }
                    | QuerySignature::ProjectedNumericRangeFilterLimit { .. }
                    | QuerySignature::LikeFilter { .. }
                    | QuerySignature::FilteredStringAgg { .. }
                    | QuerySignature::TableFunction
            );

        let sql = sql.to_string();
        // File-replacing DDL must not run while a per-client cached backend
        // still maps the table file (Windows OS error 1224).
        self.release_backends_for_file_replacing_sql(&sql);
        // Return enum to avoid per-cell arrow_value_at inside allow_threads
        enum ExecOut {
            Scalar(String, i64), // key, value — for txn commands
            Batch(RecordBatch),  // data result — columnar conversion with GIL
            Empty,               // no result
        }

        let exec_out = py.allow_threads(|| -> PyResult<ExecOut> {
            let session = crate::Session::new(&base_dir, &table_path)
                .with_root_dir(&self.root_dir)
                .with_temp_dir(&self.temp_dir);
            if is_begin {
                let result = session
                    .execute_classified(&sql, &sig)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                if let ApexResult::Scalar(txn_id) = &result {
                    return Ok(ExecOut::Scalar("txn_id".to_string(), *txn_id));
                }
                return Ok(ExecOut::Empty);
            }

            if is_commit {
                if let Some(txn_id) = current_txn {
                    let result = session
                        .commit_txn(txn_id)
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                    if let ApexResult::Scalar(n) = &result {
                        return Ok(ExecOut::Scalar("rows_applied".to_string(), *n));
                    }
                }
                return Ok(ExecOut::Empty);
            }

            if is_rollback {
                if let Some(txn_id) = current_txn {
                    session
                        .rollback_txn(txn_id)
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                }
                return Ok(ExecOut::Empty);
            }

            if is_savepoint {
                if let Some(txn_id) = current_txn {
                    let name = sql
                        .trim()
                        .strip_prefix("SAVEPOINT ")
                        .or_else(|| sql.trim().strip_prefix("savepoint "))
                        .unwrap_or("")
                        .trim()
                        .trim_end_matches(';')
                        .to_string();
                    let mgr = crate::txn::txn_manager();
                    mgr.with_context(txn_id, |ctx| {
                        ctx.savepoint(&name);
                        Ok(())
                    })
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                }
                return Ok(ExecOut::Empty);
            }

            if is_rollback_to {
                if let Some(txn_id) = current_txn {
                    let rest = txn_upper.strip_prefix("ROLLBACK TO").unwrap_or("").trim();
                    let rest = rest
                        .strip_prefix("SAVEPOINT")
                        .unwrap_or(rest)
                        .trim()
                        .trim_end_matches(';');
                    let name_start = txn_upper.find(rest).unwrap_or(0);
                    let name = sql.trim()[name_start..]
                        .trim()
                        .trim_end_matches(';')
                        .to_string();
                    let mgr = crate::txn::txn_manager();
                    mgr.with_context(txn_id, |ctx| ctx.rollback_to_savepoint(&name))
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                }
                return Ok(ExecOut::Empty);
            }

            if is_release {
                if let Some(txn_id) = current_txn {
                    let rest = txn_upper.strip_prefix("RELEASE").unwrap_or("").trim();
                    let rest = rest
                        .strip_prefix("SAVEPOINT")
                        .unwrap_or(rest)
                        .trim()
                        .trim_end_matches(';');
                    let name_start = txn_upper.find(rest).unwrap_or(0);
                    let name = sql.trim()[name_start..]
                        .trim()
                        .trim_end_matches(';')
                        .to_string();
                    let mgr = crate::txn::txn_manager();
                    mgr.with_context(txn_id, |ctx| ctx.release_savepoint(&name))
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                }
                return Ok(ExecOut::Empty);
            }

            if is_txn_dml || is_txn_select {
                let txn_id = current_txn.unwrap();
                let parsed =
                    SqlParser::parse(&sql).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                let result = session
                    .execute_in_txn(txn_id, parsed)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                if let ApexResult::Scalar(n) = &result {
                    return Ok(ExecOut::Scalar("rows_buffered".to_string(), *n));
                }
                let batch = result
                    .to_record_batch()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                return Ok(ExecOut::Batch(batch));
            }

            // Normal execution (non-transaction writes, DDL, and fallback reads)
            let result = session
                .execute_classified(&sql, &sig)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            if let ApexResult::Scalar(n) = &result {
                return Ok(ExecOut::Scalar("rows_affected".to_string(), *n));
            }
            let batch = result
                .to_record_batch()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(ExecOut::Batch(batch))
        })?;
        // Update transaction state after execution
        if is_begin {
            if let ExecOut::Scalar(_, txn_id) = &exec_out {
                *self.current_txn_id.write() = Some(*txn_id as u64);
            }
        }
        if is_commit || is_rollback {
            *self.current_txn_id.write() = None;
            if !table_name.is_empty() {
                self.invalidate_backend(&table_name);
            }
        }

        // Build Python result — columnar conversion for batches (single downcast per column)
        let out = PyDict::new_bound(py);
        match exec_out {
            ExecOut::Batch(batch) => {
                let columns_dict = PyDict::new_bound(py);
                let schema = batch.schema();
                for col_idx in 0..batch.num_columns() {
                    let col_name = schema.field(col_idx).name();
                    let arr = batch.column(col_idx);
                    let col_list = arrow_col_to_pylist(py, arr)?;
                    columns_dict.set_item(col_name, col_list)?;
                }
                out.set_item("columns_dict", columns_dict)?;
            }
            ExecOut::Scalar(key, val) => {
                out.set_item("columns", PyList::new_bound(py, [&key]))?;
                let row = PyList::new_bound(py, [val.into_py(py)]);
                out.set_item("rows", PyList::new_bound(py, [row]))?;
            }
            ExecOut::Empty => {
                out.set_item("columns", PyList::empty_bound(py))?;
                out.set_item("rows", PyList::empty_bound(py))?;
            }
        }
        out.set_item("rows_affected", 0)?;

        // Invalidate cached backend AFTER write operations
        if is_write && !table_name.is_empty() {
            self.invalidate_backend(&table_name);
        }

        // After CREATE TABLE, register the new table and set it as current
        if let QuerySignature::Ddl {
            kind: crate::query::query_signature::DdlKind::CreateTable { ref name },
        } = &sig
        {
            let tbl_path = self.current_base_dir().join(format!("{}.apex", name));
            self.table_paths.write().insert(name.clone(), tbl_path);
            *self.current_table.write() = name.clone();
        } else if let QuerySignature::Ddl {
            kind: crate::query::query_signature::DdlKind::DropTable { ref name },
        } = &sig
        {
            // The executor already removed the registry entry and file; clear
            // the client-side cache so a stale path cannot resurrect the table.
            self.table_paths.write().remove(name);
            if *self.current_table.read() == *name {
                *self.current_table.write() = String::new();
            }
        }

        Ok(out.into())
    }

    fn execute_batch(&self, py: Python<'_>, queries: Vec<String>) -> PyResult<PyObject> {
        use arrow::ipc::writer::StreamWriter;

        let table_path = self
            .get_current_table_path()
            .unwrap_or_else(|_| self.current_base_dir());
        let base_dir = self.current_base_dir();
        let root_dir = self.root_dir.clone();
        let temp_dir = self.temp_dir.clone();

        // Execute queries in parallel using Rayon (releases GIL)
        let ipc_results: Vec<Result<Vec<u8>, String>> = py.allow_threads(|| {
            use rayon::prelude::*;

            queries
                .par_iter()
                .map(|sql| {
                    // Execute query in Rust thread pool
                    let result = crate::Session::new(&base_dir, &table_path)
                        .with_root_dir(&root_dir)
                        .with_temp_dir(&temp_dir)
                        .execute(sql);
                    let batch = match result {
                        Ok(r) => r.to_record_batch(),
                        Err(e) => return Err(e.to_string()),
                    };
                    let batch = match batch {
                        Ok(b) => b,
                        Err(e) => return Err(e.to_string()),
                    };

                    // Serialize to IPC format
                    let estimated_size = batch.get_array_memory_size() + 512;
                    let mut buf = Vec::with_capacity(estimated_size);
                    {
                        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
                            .map_err(|e| format!("IPC writer error: {}", e))?;
                        writer
                            .write(&batch)
                            .map_err(|e| format!("IPC write error: {}", e))?;
                        writer
                            .finish()
                            .map_err(|e| format!("IPC finish error: {}", e))?;
                    }
                    Ok(buf)
                })
                .collect()
        });

        // Build Python list of results
        let empty_slice: &[PyObject] = &[];
        let list = PyList::new_bound(py, empty_slice);

        for result in ipc_results {
            match result {
                Ok(buf) => {
                    let py_bytes = pyo3::types::PyBytes::new_bound(py, &buf);
                    list.append(py_bytes)?;
                }
                Err(e) => return Err(PyRuntimeError::new_err(e)),
            }
        }
        Ok(list.into())
    }

    fn use_table(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        // First check cache
        {
            let paths = self.table_paths.read();
            if paths.contains_key(name) {
                drop(paths);
                *self.current_table.write() = name.to_string();
                return Ok(());
            }
        }

        // Table not in cache - check if it exists on disk (lazy discovery)
        let base_dir = self.current_base_dir();
        let table_path = base_dir.join(format!("{}.apex", name));
        if self.in_memory
            && crate::Database::table_exists(&table_path)
        {
            self.table_paths
                .write()
                .insert(name.to_string(), table_path);
            *self.current_table.write() = name.to_string();
            return Ok(());
        }
        let found = py.allow_threads(|| {
            let path = crate::storage::table_catalog::resolve(&base_dir, name)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            Ok::<bool, PyErr>(path.is_some())
        })?;
        if found {
            // Add to cache
            self.table_paths
                .write()
                .insert(name.to_string(), table_path);
            *self.current_table.write() = name.to_string();
            return Ok(());
        }

        Err(PyValueError::new_err(format!("Table not found: {}", name)))
    }

    fn current_table(&self) -> String {
        self.current_table.read().clone()
    }

    #[pyo3(signature = (name, schema=None))]
    fn create_table(
        &self,
        py: Python<'_>,
        name: &str,
        schema: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let schema_cols = if let Some(schema_dict) = schema {
            Some(Self::parse_schema_dict(schema_dict)?)
        } else {
            None
        };

        let base_dir = self.current_base_dir();
        let table_path = base_dir.join(format!("{}.apex", name));
        if self.in_memory {
            if crate::Database::table_exists(&table_path) {
                return Err(PyValueError::new_err(format!(
                    "Table already exists: {}",
                    name
                )));
            }
            let result = if let Some(schema_cols) = schema_cols.as_ref() {
                crate::Database::create_table_with_schema(
                    &table_path,
                    self.durability,
                    schema_cols,
                )
            } else {
                crate::Database::create_table(&table_path, self.durability)
            };
            result.map_err(|e| PyIOError::new_err(format!("Failed to create table: {}", e)))?;
            self.table_paths
                .write()
                .insert(name.to_string(), table_path);
            *self.current_table.write() = name.to_string();
            return Ok(());
        }
        py.allow_threads(|| {
            // The per-database table registry is authoritative: it prevents a
            // fresh process from silently rebuilding a table that another
            // client created, and the registry lock closes the cross-process
            // create race (two clients cannot both pass the existence check).
            let registry_lock = crate::storage::table_catalog::lock(&base_dir)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            let registry = registry_lock
                .snapshot()
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            if registry.contains_key(name) {
                return Err(PyValueError::new_err(format!(
                    "Table already exists: {}",
                    name
                )));
            }
            // An unregistered file is an orphan (deferred deletion or crash);
            // the registry is authoritative, so reap it and reuse the name.
            if table_path.exists() {
                crate::storage::table_catalog::reap_table_files(&table_path);
            }

            // Lazy materialization: CREATE records the name (and schema) in the
            // catalog without paying a per-table file write. The `.apex` file
            // is created on first real access with the recorded schema.
            let result = if let Some(schema_cols) = schema_cols.as_ref() {
                let mut schema = crate::storage::OnDemandSchema::new();
                for (name, col_type) in schema_cols {
                    schema.add_column(name, *col_type);
                }
                registry_lock.insert(name)?;
                registry_lock
                    .set_schema(name, &schema.to_bytes())
                    .map_err(|e| {
                        let _ = registry_lock.remove(name);
                        e
                    })
            } else {
                registry_lock.insert(name)
            };
            match result {
                Ok(()) => {}
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::AlreadyExists {
                        return Err(PyValueError::new_err(format!(
                            "Table already exists: {}",
                            name
                        )));
                    }
                    return Err(PyIOError::new_err(format!(
                        "Failed to create table: {}",
                        e
                    )));
                }
            }
            Ok(())
        })?;

        // Remove any stale cache entry before inserting the fresh one.
        self.table_paths.write().remove(name);
        self.table_paths
            .write()
            .insert(name.to_string(), table_path);

        *self.current_table.write() = name.to_string();
        Ok(())
    }

    fn drop_table(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        // Invalidate cached backend first (releases file lock)
        self.invalidate_backend(name);

        let base_dir = self.current_base_dir();
        if self.in_memory {
            let path = base_dir.join(format!("{}.apex", name));
            if !crate::Database::drop_memory_table(&path) {
                return Err(PyValueError::new_err(format!("Table not found: {}", name)));
            }
            self.table_paths.write().remove(name);
            if *self.current_table.read() == name {
                *self.current_table.write() = String::new();
            }
            return Ok(());
        }
        let path = py.allow_threads(|| {
            let registry_lock = crate::storage::table_catalog::lock(&base_dir)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            let path = registry_lock
                .remove(name)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            let path = path.ok_or_else(|| PyValueError::new_err(format!("Table not found: {}", name)))?;
            registry_lock
                .remove_schema(name)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            if path.exists() {
                // Physical unlink is deferred; the file is reaped on
                // same-name recreation or client close.
                crate::storage::table_catalog::queue_file_deletions(&[&path]);
            }
            Ok::<PathBuf, PyErr>(path)
        })?;
        // Mirror the SQL DROP path: invalidate the process-wide caches and
        // bump the table epoch so cross-process result caches keyed on the
        // old generation are discarded.
        crate::Database::invalidate_dropped_table(&path);
        crate::storage::epoch::bump(&path);
        self.table_paths.write().remove(name);

        if *self.current_table.read() == name {
            *self.current_table.write() = String::new();
        }
        Ok(())
    }

    #[pyo3(signature = (name, file_path, on_bad_lines = "error"))]
    fn register_temp_table(
        &self,
        py: Python<'_>,
        name: &str,
        file_path: &str,
        on_bad_lines: &str,
    ) -> PyResult<()> {
        let temp_path = self.temp_dir.join(format!("{}.apex", name));
        let temp_dir = self.temp_dir.clone();
        let _ = py.allow_threads(|| fs::create_dir_all(&temp_dir));

        if py.allow_threads(|| temp_path.exists()) {
            return Err(PyValueError::new_err(format!(
                "Temp table '{}' already exists. Use drop_temp_table() first.",
                name
            )));
        }

        let fmt = {
            let lower = file_path.to_lowercase();
            if lower.ends_with(".csv") || lower.ends_with(".tsv") {
                "CSV"
            } else if lower.ends_with(".json")
                || lower.ends_with(".ndjson")
                || lower.ends_with(".jsonl")
            {
                "JSON"
            } else {
                "PARQUET"
            }
        };

        let base_dir = self.current_base_dir();
        let options = vec![("on_bad_lines".to_string(), on_bad_lines.to_string())];
        let result = py.allow_threads(|| {
            crate::Session::new(&base_dir, &base_dir)
                .with_temp_dir(&self.temp_dir)
                .copy_import(&temp_path, name, file_path, fmt, &options)
        });

        match result {
            Ok(_) => {
                self.table_paths.write().insert(name.to_string(), temp_path);
                Ok(())
            }
            Err(e) => {
                let _ = py.allow_threads(|| fs::remove_file(&temp_path));
                Err(PyIOError::new_err(format!(
                    "Failed to register temp table: {}",
                    e
                )))
            }
        }
    }

    fn drop_temp_table(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        if let Some(path) = self.table_paths.write().remove(name) {
            py.allow_threads(|| {
                let _ = fs::remove_file(&path);
                let _ = fs::remove_file(path.with_extension("apex.wal"));
                crate::Database::invalidate(&path);
            });
        }
        Ok(())
    }

    fn list_tables(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        // The per-database registry is authoritative; legacy databases without
        // metadata are backfilled from `.apex` files by the catalog module.
        let base_dir = self.current_base_dir();
        if self.in_memory {
            return Ok(crate::Database::list_memory_tables(&base_dir));
        }
        py.allow_threads(|| {
            crate::storage::table_catalog::list(&base_dir)
                .map(|mut tables| {
                    tables.sort();
                    tables
                })
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    #[pyo3(name = "use_database_")]
    fn use_database_(&self, py: Python<'_>, db_name: &str) -> PyResult<()> {
        let new_base_dir = if db_name.is_empty() || db_name.eq_ignore_ascii_case("default") {
            self.root_dir.clone()
        } else {
            let db_dir = self.root_dir.join(db_name);
            if self.in_memory {
                self.memory_databases.write().insert(db_name.to_string());
            } else {
                py.allow_threads(|| {
                    fs::create_dir_all(&db_dir).map_err(|e| {
                        PyIOError::new_err(format!("Cannot create database '{}': {}", db_name, e))
                    })
                })?;
            }
            db_dir
        };

        *self.current_database.write() = db_name.to_string();
        *self.base_dir.write() = new_base_dir;

        // Clear all per-database caches
        self.cached_backends.clear();
        self.update_by_id_numeric_cache.clear();
        self.update_by_id_cell_cache.clear();
        self.replace_exact_row_cache.clear();
        self.table_paths.write().clear();
        *self.tables_scanned.write() = false;
        *self.current_table.write() = String::new();

        Ok(())
    }

    #[pyo3(name = "current_database_")]
    fn current_database_(&self) -> String {
        self.current_database.read().clone()
    }

    #[pyo3(name = "list_databases_")]
    fn list_databases_(&self, py: Python<'_>) -> Vec<String> {
        if self.in_memory {
            let mut databases: Vec<String> = self.memory_databases.read().iter().cloned().collect();
            databases.sort_unstable();
            return databases;
        }
        let root_dir = self.root_dir.clone();
        py.allow_threads(|| {
            let mut dbs = vec!["default".to_string()];
            if let Ok(entries) = fs::read_dir(&root_dir) {
                for entry in entries.flatten() {
                    let p = entry.path();
                    if p.is_dir() {
                        if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                            // Skip hidden dirs and internal dirs
                            if !name.starts_with('.') && name != "fts_indexes" {
                                dbs.push(name.to_string());
                            }
                        }
                    }
                }
            }
            dbs.sort();
            dbs.dedup();
            dbs
        })
    }

    #[pyo3(signature = (where_clause, limit=None))]
    fn query(
        &self,
        py: Python<'_>,
        where_clause: &str,
        limit: Option<usize>,
    ) -> PyResult<Vec<PyObject>> {
        let (table_path, table_name) = self.get_current_table_info()?;

        let rows = py.allow_threads(|| -> PyResult<Vec<HashMap<String, Value>>> {
            let sql = if let Some(lim) = limit {
                format!(
                    "SELECT * FROM {} WHERE {} LIMIT {}",
                    table_name, where_clause, lim
                )
            } else {
                format!("SELECT * FROM {} WHERE {}", table_name, where_clause)
            };

            let result = crate::Session::new(&table_path, &table_path)
                .execute(&sql)
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
}

impl ApexStorageImpl {
    /// Combined primary-key read fast path.
    ///
    /// Classifies the SQL (cached in the core), resolves the cached backend,
    /// and returns the projected rows as a `columns_dict` — all in one call
    /// with no Python-side parsing. Returns `None` for every other signature
    /// or when pending/delta state requires the general executor.
    fn try_pk_read(
        &self,
        py: Python<'_>,
        sig: &crate::query::query_signature::QuerySignature,
        table_name: &str,
        table_path: &std::path::Path,
        base_dir: &std::path::Path,
        row_result: bool,
    ) -> PyResult<Option<PyObject>> {
        use crate::query::query_signature::QuerySignature;

        let (pk_ids, pk_id, pk_table, pk_columns) = match sig {
            QuerySignature::PointLookup { id, table } => {
                (vec![*id], Some(*id), table.as_deref(), None)
            }
            QuerySignature::ProjectedPointLookup {
                id,
                table,
                columns,
            } => (
                vec![*id],
                Some(*id),
                table.as_deref(),
                Some(columns.as_slice()),
            ),
            QuerySignature::IdBatchLookup { ids, table } => {
                (ids.clone(), None, table.as_deref(), None)
            }
            QuerySignature::ProjectedIdBatchLookup {
                ids,
                table,
                columns,
            } => (
                ids.clone(),
                None,
                table.as_deref(),
                Some(columns.as_slice()),
            ),
            _ => return Ok(None),
        };
        if pk_ids.is_empty() {
            return Ok(None);
        }

        let (target_table, target_path) = self.resolve_signature_table(
            pk_table,
            table_name,
            table_path,
            base_dir,
        );
        let maybe_backend = self
            .cached_backends
            .get(&target_table)
            .map(|v| Arc::clone(&v))
            .or_else(|| {
                crate::Database::cached_backend(&target_path).ok().map(|b| {
                    self.cached_backends
                        .insert(target_table.clone(), Arc::clone(&b));
                    b
                })
            });

        let Some(backend) = maybe_backend else {
            return Ok(None);
        };
        // The on-disk delta file is loaded into the in-memory DeltaStore when
        // the backend is opened, so two cheap guards are sufficient: no
        // pending delta overlay and no unflushed in-memory rows. File-stat
        // based guards are deliberately avoided on this hot path.
        if backend.has_pending_deltas() || backend.pending_v4_in_memory_rows() > 0 {
            return Ok(None);
        }

        if let Some(id) = pk_id {
            // Single-row point reads use the lean RCIX reader.
            if let Some(columns) = pk_columns {
                let projected_cols: Vec<&str> = columns.iter().map(String::as_str).collect();
                if let Ok(Some(vals)) = backend.storage.retrieve_rcix_projected(id, &projected_cols)
                {
                    let out = PyDict::new_bound(py);
                    if row_result {
                        if let Some(row) = projected_values_to_row_dict(py, &vals, columns)? {
                            out.set_item("row", row)?;
                        } else {
                            return Ok(None);
                        }
                    } else if let Some(columns_dict) =
                        projected_values_to_columns_dict(py, &vals, columns)?
                    {
                        out.set_item("columns_dict", columns_dict)?;
                    } else {
                        return Ok(None);
                    }
                    out.set_item("rows_affected", 0i64)?;
                    return Ok(Some(out.into()));
                }
            } else if let Ok(Some(vals)) = backend.storage.retrieve_rcix(id) {
                let out = PyDict::new_bound(py);
                if row_result {
                    let row = PyDict::new_bound(py);
                    for (col_name, val) in &vals {
                        row.set_item(col_name.as_str(), value_to_py(py, val)?)?;
                    }
                    out.set_item("row", row)?;
                } else {
                    let columns_dict = PyDict::new_bound(py);
                    for (col_name, val) in &vals {
                        let pyval = value_to_py(py, val)?;
                        columns_dict
                            .set_item(col_name.as_str(), PyList::new_bound(py, [pyval]))?;
                    }
                    out.set_item("columns_dict", columns_dict)?;
                }
                out.set_item("rows_affected", 0i64)?;
                return Ok(Some(out.into()));
            }
            return Ok(None);
        }

        // Multi-row `_id IN (...)` reads use the batch reader.
        let sorted_ids = sort_and_dedupe_ids(&pk_ids);
        let batch_result = if let Some(columns) = pk_columns {
            backend
                .storage
                .retrieve_many_mmap_columns_projected(&sorted_ids, columns)
        } else {
            backend.storage.retrieve_many_mmap_columns(&sorted_ids)
        };
        if let Ok(Some(batch_cols)) = batch_result {
            if batch_cols.row_count == sorted_ids.len() {
                if let Some(columns_dict) =
                    mmap_batch_columns_to_pydict(py, batch_cols, pk_columns)?
                {
                    let out = PyDict::new_bound(py);
                    out.set_item("columns_dict", columns_dict)?;
                    out.set_item("rows_affected", 0i64)?;
                    return Ok(Some(out.into()));
                }
            }
        }
        Ok(None)
    }

}

#[pymethods]
impl ApexStorageImpl {
    /// One FFI call that classifies and direct-reads primary-key lookups.
    /// Returns `None` when the general executor must handle the statement.
    #[pyo3(name = "execute_pk")]
    fn execute_pk(&self, py: Python<'_>, sql: &str) -> PyResult<Option<PyObject>> {
        let sig = crate::query::query_signature::classify(sql);
        let table_name = self.current_table.read().clone();
        let base_dir = self.current_base_dir();
        let table_path = if table_name.is_empty() {
            base_dir.clone()
        } else {
            self.table_paths
                .read()
                .get(&table_name)
                .cloned()
                .unwrap_or_else(|| base_dir.join(format!("{}.apex", table_name)))
        };
        self.try_pk_read(py, &sig, &table_name, &table_path, &base_dir, true)
    }

}
