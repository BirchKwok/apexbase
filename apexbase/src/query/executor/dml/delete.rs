use super::*;

impl ApexExecutor {
    pub(in crate::query::executor) fn execute_delete(
        storage_path: &Path,
        where_clause: Option<&SqlExpr>,
    ) -> io::Result<ApexResult> {
        if !storage_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Table does not exist",
            ));
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
            let storage = get_pending_cached_backend(storage_path).unwrap_or(Arc::new(
                TableStorageBackend::open_for_delete(storage_path)?,
            ));
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
            Self::notify_index_delete(storage_path, &deleted_entries);
            Self::notify_fts_delete(storage_path, &deleted_entries);
            invalidate_storage_cache(storage_path);
            crate::storage::engine::engine().invalidate(storage_path);
            invalidate_table_stats(&storage_path.to_string_lossy());
            return Ok(ApexResult::Scalar(count));
        }

        // Python flushes pending overlays before SQL writes, so the common path
        // uses the mmap-only delete opener. Embedded callers with true
        // in-memory rows retain their authoritative cached backend.
        let storage = get_pending_cached_backend(storage_path).unwrap_or(Arc::new(
            TableStorageBackend::open_for_delete(storage_path)?,
        ));

        // ── Fast scan path: simple numeric/string predicate, no FK, no indexes ──
        // Numeric: delete_where_numeric_range_inplace — single pass, no id_to_idx HashMap.
        // String:  scan_string_filter_mmap → get_ids → delete_batch + save_delete_only.
        let predicate_delta_override = Self::extract_numeric_range_from_where(
            where_clause.expect("DELETE predicate checked above"),
        )
        .is_some_and(|(column, _, _)| storage.pending_delta_updates_column(&column));
        if fk_children.is_empty()
            && indexed_cols.is_empty()
            && !Self::table_fts_enabled(&base_dir, &this_table_name)
            && !predicate_delta_override
        {
            if let Some((col, low, high)) =
                Self::extract_numeric_range_from_where(where_clause.unwrap())
            {
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
            Self::notify_index_delete(storage_path, &deleted_entries);
            Self::notify_fts_delete(storage_path, &deleted_entries);
            invalidate_storage_cache(storage_path);
            crate::storage::engine::engine().invalidate(storage_path);
            invalidate_table_stats(&storage_path.to_string_lossy());
        }

        Ok(ApexResult::Scalar(deleted))
    }
}
