use super::*;

impl ApexExecutor {
    pub(in crate::query::executor) fn execute_delete(
        storage_path: &Path,
        where_clause: Option<&SqlExpr>,
    ) -> io::Result<ApexResult> {
        let __t = std::time::Instant::now();
        if !crate::storage::is_memory_path(storage_path) {
            crate::storage::table_catalog::ensure_table_file(
                storage_path,
                crate::storage::DurabilityLevel::Fast,
            )?;
        }
        let _epoch_write = crate::storage::epoch::logical_write(storage_path);

        // Collect indexed column names for this table (for index maintenance)
        // Use effective_columns() to include ALL columns of composite indexes,
        // not just the first column (column_name), so composite_key() can find the full key.
        let indexed_cols: Vec<String> = {
            let (bd, tn) = base_dir_and_table(storage_path);
            let mgr = get_index_manager(&bd, &tn);
            let lock = mgr.lock();
            let mut cols: Vec<String> = lock
                .list_indexes()
                .iter()
                .flat_map(|m| m.effective_columns().into_iter().map(|s| s.to_string()))
                .collect();
            cols.sort();
            cols.dedup();
            cols
        };

        // FOREIGN KEY enforcement: check if any child tables reference this table
        // We need to scan sibling .apex files for FK constraints pointing here
        let (base_dir, this_table_name) = base_dir_and_table(storage_path);
        let fk_children: Vec<(String, String, String)> = {
            // (child_table_path, child_col, ref_col)
            let mut children = Vec::new();
            if let Ok(entries) = std::fs::read_dir(&base_dir) {
                for entry in entries.flatten() {
                    let p = entry.path();
                    if p.extension().map(|e| e == "apex").unwrap_or(false) && p != storage_path {
                        if let Ok(child_storage) = TableStorageBackend::open(&p) {
                            let child_schema = child_storage.get_schema();
                            for (col_name, _) in &child_schema {
                                let cons = child_storage.storage.get_column_constraints(col_name);
                                if let Some((ref rt, ref rc)) = cons.foreign_key {
                                    if rt == &this_table_name {
                                        children.push((
                                            p.to_string_lossy().to_string(),
                                            col_name.clone(),
                                            rc.clone(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            children
        };

        // For DELETE without WHERE, delete all rows (soft delete)
        if where_clause.is_none() {
            // FK enforcement: if deleting ALL rows, check child tables have no referencing rows
            if !fk_children.is_empty() {
                for (child_path, child_col, _ref_col) in &fk_children {
                    let child_storage =
                        TableStorageBackend::open(std::path::Path::new(child_path))?;
                    let child_batch = child_storage.read_columns_to_arrow(
                        Some(&[child_col.as_str()]),
                        0,
                        None,
                    )?;
                    if let Some(col_arr) = child_batch.column_by_name(child_col) {
                        // Check if any non-null FK values exist in child
                        for r in 0..col_arr.len() {
                            if !col_arr.is_null(r) {
                                let child_table = std::path::Path::new(child_path)
                                    .file_stem()
                                    .map(|s| s.to_string_lossy().to_string())
                                    .unwrap_or_default();
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!("FOREIGN KEY constraint violated: cannot delete from '{}' — referenced by '{}.{}'", this_table_name, child_table, child_col),
                                ));
                            }
                        }
                    }
                }
            }
            // Preserve genuinely in-memory state, but otherwise use the narrow
            // delete opener instead of populating the general read cache.
            let storage = crate::storage::engine::engine()
                .memory_backend(storage_path)
                .or_else(|| get_pending_cached_backend(storage_path))
                .map(Ok)
                .unwrap_or_else(|| {
                    TableStorageBackend::open_for_delete(storage_path).map(Arc::new)
                })?;
            let count = storage.active_row_count() as i64;

            // Read _id + indexed columns for index maintenance
            let mut read_cols: Vec<String> = vec!["_id".to_string()];
            read_cols.extend(indexed_cols.iter().cloned());
            read_cols.sort();
            read_cols.dedup();
            let col_refs: Vec<&str> = read_cols.iter().map(|s| s.as_str()).collect();
            let batch = storage.read_columns_to_arrow(Some(&col_refs), 0, None)?;

            let mut deleted_entries: Vec<(u64, std::collections::HashMap<String, Value>)> =
                Vec::new();
            let mut all_rids: Vec<u64> = Vec::new();
            if let Some(id_col) = batch.column_by_name("_id") {
                if let Some(id_arr) = id_col.as_any().downcast_ref::<UInt64Array>() {
                    for i in 0..id_arr.len() {
                        let rid = id_arr.value(i);
                        let mut vals = std::collections::HashMap::new();
                        for cn in &indexed_cols {
                            if let Some(c) = batch.column_by_name(cn) {
                                vals.insert(cn.clone(), Self::arrow_value_at_col(c, i));
                            }
                        }
                        deleted_entries.push((rid, vals));
                        all_rids.push(rid);
                    }
                } else if let Some(id_arr) = id_col.as_any().downcast_ref::<Int64Array>() {
                    for i in 0..id_arr.len() {
                        let rid = id_arr.value(i) as u64;
                        let mut vals = std::collections::HashMap::new();
                        for cn in &indexed_cols {
                            if let Some(c) = batch.column_by_name(cn) {
                                vals.insert(cn.clone(), Self::arrow_value_at_col(c, i));
                            }
                        }
                        deleted_entries.push((rid, vals));
                        all_rids.push(rid);
                    }
                }
            }
            if storage.has_delta() {
                storage.delta_delete_rows(&all_rids);
                storage.save_delta_store()?;
            } else {
                storage.delete_batch(&all_rids);
                storage.save_delete_only()?;
            }
            if let Err(error) = Self::notify_index_delete(storage_path, &deleted_entries) {
                flag_indexes_stale_after_failure(storage_path);
                return Err(error);
            }
            Self::notify_fts_delete(storage_path, &deleted_entries);
            invalidate_storage_cache(storage_path);
            crate::storage::engine::engine().invalidate(storage_path);
            invalidate_table_stats(&storage_path.to_string_lossy());
            return Ok(ApexResult::Scalar(count));
        }

        // Python flushes pending overlays before SQL writes, so the common path
        // uses the mmap-only delete opener. Embedded callers with true
        // in-memory rows retain their authoritative cached backend.
        let storage = crate::storage::engine::engine()
            .memory_backend(storage_path)
            .or_else(|| get_pending_cached_backend(storage_path))
            .map(Ok)
            .unwrap_or_else(|| TableStorageBackend::open_for_delete(storage_path).map(Arc::new))?;

        // ── Fast scan path: simple numeric/string predicate, no FK, no indexes ──
        // Numeric: delete_where_numeric_range_inplace — single pass, no id_to_idx HashMap.
        // String:  scan_string_filter_mmap → get_ids → delete_batch + save_delete_only.
        // `col >= a AND col < b` is one range on one column: without the
        // two-sided form the extraction fails and the statement materializes the
        // table (measured 11.6 ms at 1M rows against 1.4 ms for `col = v`).
        let numeric_range = where_clause.and_then(|w| {
            Self::extract_two_sided_same_col_range(w)
                .or_else(|| Self::extract_numeric_range_from_where(w))
        });
        // `_id = X` addresses the physical row identifier directly. The read side
        // resolves it through the row-group directory, but the delete path had no
        // branch for it — unlike UPDATE, which has had its `_id` branch all along —
        // so the numeric lane looked `_id` up in the schema, failed, and the
        // statement fell through to materializing every column of the table
        // (1M rows: 3.6 ms against 1.4 ms for the same shape on a user column).
        let id_equality = where_clause.and_then(Self::extract_id_equality);
        let predicate_delta_override = numeric_range
            .clone()
            .is_some_and(|(column, _, _)| storage.pending_delta_updates_column(&column));
        if fk_children.is_empty()
            && indexed_cols.is_empty()
            && !Self::table_fts_enabled(&base_dir, &this_table_name)
            && !predicate_delta_override
        {
            if let Some(row_id) = id_equality {
                if let Some(deleted) = Self::try_delete_by_id(storage.as_ref(), row_id)? {
                    if deleted > 0 {
                        invalidate_storage_cache(storage_path);
                        crate::storage::engine::engine().invalidate(storage_path);
                        invalidate_table_stats(&storage_path.to_string_lossy());
                    }
                    return Ok(ApexResult::Scalar(deleted));
                }
            }
            if let Some((col, low, high)) = numeric_range.clone() {

                let pending_ids = if storage.pending_v4_in_memory_rows() > 0 {
                    storage.pending_numeric_range_ids(&col, low, high)
                } else {
                    Vec::new()
                };
                if let Some(deleted) =
                    storage.delete_where_numeric_range_inplace(&col, low, high)?
                {
                    let delta_deleted = if storage.has_delta() {
                        let delta_ids = storage.delta_numeric_range_ids(&col, low, high)?;
                        storage.delta_delete_rows(&delta_ids)
                    } else {
                        0
                    };
                    let pending_deleted = pending_ids
                        .iter()
                        .filter(|&&id| storage.delete_pending_v4_in_memory_row(id))
                        .count();
                    if delta_deleted > 0 {
                        storage.save_delta_store()?;
                    }
                    let deleted = deleted + delta_deleted as i64 + pending_deleted as i64;
                    if deleted > 0 {
                        invalidate_storage_cache(storage_path);
                        crate::storage::engine::engine().invalidate(storage_path);
                        invalidate_table_stats(&storage_path.to_string_lossy());
                    }
                    return Ok(ApexResult::Scalar(deleted));
                }
                // Inplace path unavailable (non-PLAIN encoding) — scan for IDs then use
                // ID-based inplace delete (binary search, no 1M-entry id_to_idx HashMap build)
                if let Some(all_rids) = storage.scan_numeric_range_mmap_with_ids(&col, low, high)? {
                    let delta_deleted = if storage.has_delta() {
                        let delta_ids = storage.delta_numeric_range_ids(&col, low, high)?;
                        storage.delta_delete_rows(&delta_ids)
                    } else {
                        0
                    };
                    let pending_deleted = pending_ids
                        .iter()
                        .filter(|&&id| storage.delete_pending_v4_in_memory_row(id))
                        .count();
                    if delta_deleted > 0 {
                        storage.save_delta_store()?;
                    }
                    let deleted =
                        all_rids.len() as i64 + delta_deleted as i64 + pending_deleted as i64;
                    if deleted > 0 {
                        if !all_rids.is_empty()
                            && storage.delete_ids_inplace_v4(&all_rids)?.is_none()
                        {
                            // Compressed RGs: full in-memory fallback
                            storage.delete_batch(&all_rids);
                            storage.save_delete_only()?;
                        }
                        invalidate_storage_cache(storage_path);
                        crate::storage::engine::engine().invalidate(storage_path);
                        invalidate_table_stats(&storage_path.to_string_lossy());
                    }
                    return Ok(ApexResult::Scalar(deleted));
                }
            } else if let Some((col, val)) = Self::extract_string_equality(where_clause.unwrap()) {
                let pending_ids = if storage.pending_v4_in_memory_rows() > 0 {
                    storage.pending_string_equality_ids(&col, &val)
                } else {
                    Vec::new()
                };
                if let Some(indices) = storage.scan_string_filter_mmap(&col, &val, None)? {
                    let all_rids = storage.get_ids_for_global_indices_mmap(&indices)?;
                    let delta_deleted = if storage.has_delta() {
                        let delta_ids = storage.delta_string_equality_ids(&col, &val)?;
                        storage.delta_delete_rows(&delta_ids)
                    } else {
                        0
                    };
                    let pending_deleted = pending_ids
                        .iter()
                        .filter(|&&id| storage.delete_pending_v4_in_memory_row(id))
                        .count();
                    if delta_deleted > 0 {
                        storage.save_delta_store()?;
                    }
                    let deleted =
                        all_rids.len() as i64 + delta_deleted as i64 + pending_deleted as i64;
                    if deleted > 0 {
                        if !all_rids.is_empty()
                            && storage.delete_ids_inplace_v4(&all_rids)?.is_none()
                        {
                            storage.delete_batch(&all_rids);
                            storage.save_delete_only()?;
                        }
                        invalidate_storage_cache(storage_path);
                        crate::storage::engine::engine().invalidate(storage_path);
                        invalidate_table_stats(&storage_path.to_string_lossy());
                    }
                    return Ok(ApexResult::Scalar(deleted));
                }
            }
        }
        // ── End fast scan path ──

        // Extract column names from WHERE clause + indexed columns + FK-referenced columns
        let mut where_cols: Vec<String> = Vec::new();
        Self::collect_columns_from_expr(where_clause.unwrap(), &mut where_cols);
        where_cols.extend(indexed_cols.iter().cloned());
        // Include columns referenced by child FKs so we can check them
        for (_, _, ref_col) in &fk_children {
            if !where_cols.contains(ref_col) {
                where_cols.push(ref_col.clone());
            }
        }
        where_cols.sort();
        where_cols.dedup();

        // Always include _id for deletion
        if !where_cols.iter().any(|c| c == "_id") {
            where_cols.push("_id".to_string());
        }

        let col_refs: Vec<&str> = where_cols.iter().map(|s| s.as_str()).collect();
        let batch = storage.read_columns_to_arrow(Some(&col_refs), 0, None)?;
        let filter_mask = Self::evaluate_predicate(&batch, where_clause.unwrap())?;

        // FK enforcement: for rows being deleted, check no child references exist
        if !fk_children.is_empty() {
            for (child_path, child_col, ref_col) in &fk_children {
                // Get the parent column values being deleted
                let parent_arr = batch.column_by_name(ref_col);
                if parent_arr.is_none() {
                    continue;
                }
                let parent_arr = parent_arr.unwrap();

                // Load child FK column
                let child_storage = TableStorageBackend::open(std::path::Path::new(child_path))?;
                let child_batch =
                    child_storage.read_columns_to_arrow(Some(&[child_col.as_str()]), 0, None)?;
                let child_arr = child_batch.column_by_name(child_col);
                if child_arr.is_none() {
                    continue;
                }
                let child_arr = child_arr.unwrap();

                for i in 0..filter_mask.len() {
                    if !filter_mask.value(i) || parent_arr.is_null(i) {
                        continue;
                    }
                    let parent_val = Self::arrow_value_at_col(parent_arr, i);
                    // Check if any child row references this value
                    for cr in 0..child_arr.len() {
                        if child_arr.is_null(cr) {
                            continue;
                        }
                        let child_val = Self::arrow_value_at_col(child_arr, cr);
                        if parent_val == child_val {
                            let child_table = std::path::Path::new(child_path)
                                .file_stem()
                                .map(|s| s.to_string_lossy().to_string())
                                .unwrap_or_default();
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("FOREIGN KEY constraint violated: cannot delete from '{}' — value referenced by '{}.{}'", this_table_name, child_table, child_col),
                            ));
                        }
                    }
                }
            }
        }

        // Collect matching row IDs and index values for batch deletion
        let mut deleted = 0i64;
        let mut deleted_entries: Vec<(u64, std::collections::HashMap<String, Value>)> = Vec::new();
        let mut all_rids: Vec<u64> = Vec::new();
        if let Some(id_col) = batch.column_by_name("_id") {
            for i in 0..filter_mask.len() {
                if filter_mask.value(i) {
                    let rid = if let Some(id_arr) = id_col.as_any().downcast_ref::<UInt64Array>() {
                        Some(id_arr.value(i))
                    } else if let Some(id_arr) = id_col.as_any().downcast_ref::<Int64Array>() {
                        Some(id_arr.value(i) as u64)
                    } else {
                        None
                    };
                    if let Some(rid) = rid {
                        let mut vals = std::collections::HashMap::new();
                        for cn in &indexed_cols {
                            if let Some(c) = batch.column_by_name(cn) {
                                vals.insert(cn.clone(), Self::arrow_value_at_col(c, i));
                            }
                        }
                        deleted_entries.push((rid, vals));
                        all_rids.push(rid);
                        deleted += 1;
                    }
                }
            }
        }

        if deleted > 0 {
            // Fast path: no FK/index notifications — use mmap binary-search delete
            // (bypasses the id_to_idx HashMap build in delete_batch)
            let used_delta = storage.has_delta();
            if used_delta {
                storage.delta_delete_rows(&all_rids);
                storage.save_delta_store()?;
            }
            let used_inplace = if !used_delta && fk_children.is_empty() && indexed_cols.is_empty() {
                storage.delete_ids_inplace_v4(&all_rids)?.is_some()
            } else {
                false
            };
            if !used_delta && !used_inplace {
                storage.delete_batch(&all_rids);
                storage.save_delete_only()?;
            }
            if let Err(error) = Self::notify_index_delete(storage_path, &deleted_entries) {
                flag_indexes_stale_after_failure(storage_path);
                return Err(error);
            }
            Self::notify_fts_delete(storage_path, &deleted_entries);
            invalidate_storage_cache(storage_path);
            crate::storage::engine::engine().invalidate(storage_path);
            invalidate_table_stats(&storage_path.to_string_lossy());
        }

        Ok(ApexResult::Scalar(deleted))
    }

    /// `_id = <finite non-negative integer literal>` as a physical row id.
    ///
    /// Only the exact-equality form is accepted: `_id` is an unsigned row
    /// identifier, so a range, a fractional literal or a negative one is left to
    /// the general path's authoritative answer.
    fn extract_id_equality(expr: &SqlExpr) -> Option<u64> {
        let (column, low, high) = Self::extract_two_sided_same_col_range(expr)
            .or_else(|| Self::extract_numeric_range_from_where(expr))?;
        if column != "_id" || (low - high).abs() >= f64::EPSILON {
            return None;
        }
        if !low.is_finite() || low < 0.0 || low.fract() != 0.0 {
            return None;
        }
        Some(low as u64)
    }

    /// Delete one physical row id without materializing the table.
    ///
    /// Returns `Ok(None)` when the cheap lanes cannot answer, so the caller keeps
    /// the general path: the base file's row groups must be uncompressed, because
    /// the in-place deletion-vector writer declines on compressed bodies.
    fn try_delete_by_id(storage: &TableStorageBackend, row_id: u64) -> io::Result<Option<i64>> {
        // A row already deleted through the DeltaStore must stay a no-op: the base
        // file's deletion vector does not know about that overlay, so without this
        // guard running the same DELETE twice would report the row as deleted again.
        let already_deleted = storage.storage.delta_store().is_deleted(row_id);
        if already_deleted {
            return Ok(Some(0));
        }
        // The base row goes through the same in-place deletion vector the
        // numeric-range lane uses; `None` means the lane declined and nothing was
        // written, so the general path below can still answer in full.
        let base_deleted = match storage.delete_ids_inplace_v4(&[row_id])? {
            Some(n) => n,
            None => return Ok(None),
        };
        let mut deleted = base_deleted;
        // Rows appended after the base file live only in the row delta, and the
        // DeltaStore is what records their deletion.
        let mut delta_deleted = 0usize;
        if storage.has_delta() {
            delta_deleted = storage.delta_delete_rows(&[row_id]);
            deleted += delta_deleted as i64;
        }
        // Unflushed rows are never in the base file; this deletion is exact (it
        // reports whether the id was actually pending).
        if storage.pending_v4_in_memory_rows() > 0
            && storage.delete_pending_v4_in_memory_row(row_id)
        {
            deleted += 1;
        }
        if delta_deleted > 0 {
            storage.save_delta_store()?;
        }
        Ok(Some(deleted))
    }
}
