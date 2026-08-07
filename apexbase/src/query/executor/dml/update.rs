use super::*;

impl ApexExecutor {
    pub(in crate::query::executor) fn execute_update(
        storage_path: &Path,
        assignments: &[(String, SqlExpr)],
        where_clause: Option<&SqlExpr>,
    ) -> io::Result<ApexResult> {
        use std::collections::HashMap as StdHashMap;

        crate::storage::table_catalog::ensure_table_file(
            storage_path,
            crate::storage::DurabilityLevel::Fast,
        )?;
        let _epoch_write = crate::storage::epoch::logical_write(storage_path);

        // Invalidate cache before write
        invalidate_storage_cache(storage_path);

        // Collect indexed column names for index maintenance
        // Use effective_columns() to include ALL columns of composite indexes.
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

        // Use open (mmap-only) — avoids loading ALL columns into memory.
        // DeltaStore updates are applied lazily at read time (DeltaMerger) and
        // merged into the base file via apply_pending_deltas_in_place() on next
        // open_for_write (e.g., DELETE), which is safe because of Fix B.
        let storage = TableStorageBackend::open(storage_path)?;
        let schema_types: StdHashMap<String, crate::data::DataType> =
            storage.get_schema().into_iter().collect();
        let mut normalized_assignments = Vec::with_capacity(assignments.len());
        for (column, expr) in assignments {
            if column == "_id" {
                return Err(err_input("UPDATE cannot assign to internal _id"));
            }
            let target = schema_types.get(column).ok_or_else(|| {
                err_input(format!("UPDATE column '{}' does not exist", column))
            })?;
            let normalized = match expr {
                SqlExpr::Literal(value) => {
                    SqlExpr::Literal(Self::coerce_update_literal(value, *target, column)?)
                }
                other => other.clone(),
            };
            normalized_assignments.push((column.clone(), normalized));
        }
        let assignments = normalized_assignments.as_slice();

        let (fts_base_dir, fts_table_name) = base_dir_and_table(storage_path);
        let fts_cfg = Self::read_fts_config(&fts_base_dir);
        let fts_entry = fts_cfg
            .as_object()
            .and_then(|object| object.get(&fts_table_name));
        let fts_enabled = fts_entry
            .and_then(|entry| entry.get("enabled"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let fts_fields: Vec<String> = if fts_enabled {
            fts_entry
                .and_then(|entry| entry.get("index_fields"))
                .and_then(|value| value.as_array())
                .map(|fields| {
                    fields
                        .iter()
                        .filter_map(|field| field.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_else(|| {
                    storage
                        .get_schema()
                        .into_iter()
                        .filter(|(_, data_type)| matches!(data_type, crate::data::DataType::String))
                        .map(|(name, _)| name)
                        .collect()
                })
        } else {
            Vec::new()
        };

        // ── Mmap super-fast path ─────────────────────────────────────────────────
        // For: all-literal SET + simple numeric WHERE + no constraints + no indexes.
        // Uses scan_and_update_inplace: single pass per SET column, writes directly to
        // the base .apex file — no DeltaStore serialization, no Arrow batch, no bincode.
        // The storage helpers widen affected zone maps and invalidate exact
        // aggregate sidecars before mutating cells.  This keeps the mmap path
        // safe for both exact-ID and numeric-range updates without leaving a
        // large DeltaStore overlay that would penalize subsequent reads.
        if indexed_cols.is_empty()
            && !fts_enabled
            && !storage.storage.has_constraints()
            && !storage.has_delta()
            && storage.pending_v4_in_memory_rows() == 0
        {
            let all_lit = assignments
                .iter()
                .all(|(_, e)| matches!(e, SqlExpr::Literal(_)));
            if all_lit {
                if let Some((where_col, low, high)) =
                    where_clause.and_then(|w| Self::extract_numeric_range_from_where(w))
                {
                    // Safety: if WHERE column has delta-store overrides, skip fast path
                    let where_col_clean = {
                        let ds = storage.storage.delta_store();
                        !ds.all_updates()
                            .values()
                            .any(|m| m.contains_key(&where_col))
                    };
                    if where_col_clean {
                        if where_col == "_id"
                            && low.is_finite()
                            && high.is_finite()
                            && (low - high).abs() < f64::EPSILON
                            && low >= 0.0
                        {
                            let row_id = low as u64;
                            {
                                let delta = storage.storage.delta_store();
                                if delta.is_deleted(row_id) {
                                    return Ok(ApexResult::Scalar(0));
                                }
                            }
                            if let Some(row_exists) = storage.row_id_active_rcix(row_id)? {
                                if !row_exists {
                                    return Ok(ApexResult::Scalar(0));
                                }
                            }
                            if assignments.len() == 1 {
                                let (col_name, expr) = &assignments[0];
                                if col_name != "_id" {
                                    if let SqlExpr::Literal(v) = expr {
                                        let bytes_opt = match v {
                                            Value::Float64(f) => Some(f.to_le_bytes()),
                                            Value::Int64(i) => Some(i.to_le_bytes()),
                                            _ => None,
                                        };
                                        if let Some(bytes) = bytes_opt {
                                            if let Some((n, physically_written)) = storage
                                                .update_by_id_inplace(row_id, col_name, &bytes)?
                                            {
                                                if physically_written {
                                                    invalidate_storage_cache(storage_path);
                                                    invalidate_table_stats(
                                                        &storage_path.to_string_lossy(),
                                                    );
                                                }
                                                return Ok(ApexResult::Scalar(n));
                                            }
                                        }
                                    }
                                }
                            }

                            let mut delta_values: Vec<(u64, &str, Value)> =
                                Vec::with_capacity(assignments.len());
                            let mut can_delta_by_id = true;
                            for (col_name, expr) in assignments {
                                if col_name == "_id" {
                                    can_delta_by_id = false;
                                    break;
                                }
                                if let SqlExpr::Literal(v) = expr {
                                    delta_values.push((row_id, col_name.as_str(), v.clone()));
                                } else {
                                    can_delta_by_id = false;
                                    break;
                                }
                            }
                            if can_delta_by_id {
                                if let Some(row_exists) = storage.row_id_active_rcix(row_id)? {
                                    let updated = if row_exists { 1 } else { 0 };
                                    if updated > 0 {
                                        storage.delta_batch_update_rows(&delta_values);
                                        storage.save_delta_store()?;
                                    }
                                    invalidate_storage_cache(storage_path);
                                    invalidate_table_stats(&storage_path.to_string_lossy());
                                    return Ok(ApexResult::Scalar(updated));
                                }
                            }
                        }

                        // Try in-place write for each SET column
                        let mut all_inplace = true;
                        let mut inplace_count: i64 = 0;
                        for (col_name, expr) in assignments {
                            if let SqlExpr::Literal(v) = expr {
                                let bytes_opt = match v {
                                    Value::Float64(f) => Some(f.to_le_bytes()),
                                    Value::Int64(i) => Some(i.to_le_bytes()),
                                    _ => None,
                                };
                                if let Some(bytes) = bytes_opt {
                                    match storage.scan_and_update_inplace(
                                        &where_col, low, high, col_name, &bytes,
                                    )? {
                                        Some(n) => {
                                            inplace_count = n;
                                        }
                                        None => {
                                            all_inplace = false;
                                            break;
                                        }
                                    }
                                } else {
                                    all_inplace = false;
                                    break;
                                }
                            }
                        }
                        if all_inplace {
                            // In-place write succeeded — invalidate executor cache so next read rebuilds
                            invalidate_storage_cache(storage_path);
                            invalidate_table_stats(&storage_path.to_string_lossy());
                            return Ok(ApexResult::Scalar(inplace_count));
                        }
                    }
                }
            }
        }
        // ── End mmap super-fast path ─────────────────────────────────────────────

        // COLUMN PROJECTION: only read the columns we actually need.
        // For a simple UPDATE SET col=literal WHERE other_col=val, this is just
        // [_id, WHERE-col] instead of all columns — a significant I/O reduction.
        // Also, if DeltaStore has no entries for the projected columns (e.g. previous
        // UPDATEs only changed a different column), DeltaMerger is effectively a no-op.
        let has_constraints = storage.storage.has_constraints();
        let set_col_refs: Vec<String> = assignments
            .iter()
            .flat_map(|(_, expr)| Self::collect_column_refs(expr))
            .collect();
        let unique_pk_cols: Vec<String> = if has_constraints {
            let schema = storage.get_schema();
            schema
                .iter()
                .filter(|(name, _)| {
                    let c = storage.storage.get_column_constraints(name);
                    c.unique || c.primary_key
                })
                .map(|(name, _)| name.clone())
                .collect()
        } else {
            vec![]
        };
        let total_schema_cols = storage.get_schema().len() + 1; // +1 for _id
        let mut needed_cols: Vec<String> = vec!["_id".to_string()];
        if let Some(where_expr) = where_clause {
            for c in Self::collect_column_refs(where_expr) {
                if !needed_cols.iter().any(|x| x == &c) {
                    needed_cols.push(c);
                }
            }
        }
        for c in &set_col_refs {
            if !needed_cols.iter().any(|x| x == c) {
                needed_cols.push(c.clone());
            }
        }
        for c in &indexed_cols {
            if !needed_cols.iter().any(|x| x == c) {
                needed_cols.push(c.clone());
            }
        }
        for c in &fts_fields {
            if !needed_cols.iter().any(|x| x == c) {
                needed_cols.push(c.clone());
            }
        }
        for c in &unique_pk_cols {
            if !needed_cols.iter().any(|x| x == c) {
                needed_cols.push(c.clone());
            }
        }
        let batch = if needed_cols.len() < total_schema_cols {
            let refs: Vec<&str> = needed_cols.iter().map(|s| s.as_str()).collect();
            storage.read_columns_to_arrow(Some(&refs), 0, None)?
        } else {
            storage.read_columns_to_arrow(None, 0, None)?
        };

        let filter_mask = if let Some(where_expr) = where_clause {
            Self::evaluate_predicate(&batch, where_expr)?
        } else {
            BooleanArray::from(vec![true; batch.num_rows()])
        };

        // Collect row IDs and update data for matching rows.
        // Hoist column_by_name + downcast outside the hot loop to avoid O(N) repeated lookups.
        let mut updates: Vec<(u64, StdHashMap<String, Value>)> = Vec::new();
        let mut old_index_entries: Vec<(u64, StdHashMap<String, Value>)> = Vec::new();
        let mut new_index_entries: Vec<(u64, StdHashMap<String, Value>)> = Vec::new();
        let mut fts_updates: Vec<(u64, StdHashMap<String, String>)> = Vec::new();

        // Pre-extract _id array once (avoids column_by_name + downcast inside hot loop)
        enum IdArray<'a> {
            U64(&'a UInt64Array),
            I64(&'a Int64Array),
        }
        let id_array: Option<IdArray<'_>> = batch.column_by_name("_id").and_then(|c| {
            if let Some(a) = c.as_any().downcast_ref::<UInt64Array>() {
                Some(IdArray::U64(a))
            } else {
                c.as_any().downcast_ref::<Int64Array>().map(IdArray::I64)
            }
        });
        let id_array = match id_array {
            Some(a) => a,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "_id column not found",
                ))
            }
        };

        // Pre-evaluate assignments that are pure literals (SET col = literal) — avoids
        // per-row evaluate_expr_to_value calls for the common case.
        let all_literals = assignments
            .iter()
            .all(|(_, e)| matches!(e, SqlExpr::Literal(_)));
        let literal_update: Option<StdHashMap<String, Value>> = if all_literals {
            let mut m = StdHashMap::with_capacity(assignments.len());
            for (col_name, expr) in assignments {
                if let SqlExpr::Literal(v) = expr {
                    m.insert(col_name.clone(), v.clone());
                }
            }
            Some(m)
        } else {
            None
        };

        // Pre-extract index columns arrays to avoid repeated column_by_name in loop
        let indexed_col_arrays: Vec<(&str, &ArrayRef)> = if !indexed_cols.is_empty() {
            indexed_cols
                .iter()
                .filter_map(|cn| batch.column_by_name(cn).map(|c| (cn.as_str(), c)))
                .collect()
        } else {
            Vec::new()
        };

        // ── Fast path: all-literal SET + no constraints + no indexes ───────────
        // Avoids building per-row HashMap for each matched row.
        if let Some(ref lit) = literal_update {
            if indexed_col_arrays.is_empty() && !fts_enabled && !storage.storage.has_constraints() {
                // Collect matching row IDs (no HashMap allocation per row)
                let mut matched_ids: Vec<u64> = Vec::new();
                for i in 0..filter_mask.len() {
                    if filter_mask.value(i) {
                        let id = match &id_array {
                            IdArray::U64(a) => a.value(i),
                            IdArray::I64(a) => a.value(i) as u64,
                        };
                        matched_ids.push(id);
                    }
                }
                let updated = matched_ids.len() as i64;
                if updated > 0 {
                    // Build flat batch directly without per-row HashMap
                    let mut flat_batch: Vec<(u64, &str, Value)> =
                        Vec::with_capacity(matched_ids.len() * lit.len());
                    for id in &matched_ids {
                        for (col, val) in lit {
                            flat_batch.push((*id, col.as_str(), val.clone()));
                        }
                    }
                    storage.delta_batch_update_rows(&flat_batch);
                    storage.save_delta_store()?;
                }
                invalidate_storage_cache(storage_path);
                invalidate_table_stats(&storage_path.to_string_lossy());
                return Ok(ApexResult::Scalar(updated));
            }
        }
        // ── End fast path ───────────────────────────────────────────────────────

        for i in 0..filter_mask.len() {
            if !filter_mask.value(i) {
                continue;
            }

            let id = match &id_array {
                IdArray::U64(a) => a.value(i),
                IdArray::I64(a) => a.value(i) as u64,
            };

            // Evaluate assignment expressions to get new values
            let update_data: StdHashMap<String, Value> = if let Some(ref lit) = literal_update {
                lit.clone()
            } else {
                let mut m = StdHashMap::with_capacity(assignments.len());
                for (col_name, expr) in assignments {
                    let value = Self::evaluate_expr_to_value(&batch, expr, i)?;
                    m.insert(col_name.clone(), value);
                }
                m
            };

            // Index maintenance: collect old and new indexed values
            if !indexed_col_arrays.is_empty() {
                let mut old_vals = StdHashMap::with_capacity(indexed_col_arrays.len());
                let mut new_vals = StdHashMap::with_capacity(indexed_col_arrays.len());
                for (cn, c) in &indexed_col_arrays {
                    let old_val = Self::arrow_value_at_col(c, i);
                    let new_val = update_data
                        .get(*cn)
                        .cloned()
                        .unwrap_or_else(|| old_val.clone());
                    old_vals.insert(cn.to_string(), old_val);
                    new_vals.insert(cn.to_string(), new_val);
                }
                old_index_entries.push((id, old_vals));
                new_index_entries.push((id, new_vals));
            }

            if fts_enabled {
                let mut fields = StdHashMap::with_capacity(fts_fields.len());
                for field in &fts_fields {
                    let value = update_data.get(field).cloned().or_else(|| {
                        batch
                            .column_by_name(field)
                            .map(|column| Self::arrow_value_at_col(column, i))
                    });
                    if let Some(Value::String(value)) = value {
                        fields.insert(field.clone(), value);
                    }
                }
                fts_updates.push((id, fields));
            }

            updates.push((id, update_data));
        }

        let updated = updates.len() as i64;

        // Enforce constraints on updated values
        if updated > 0 && storage.storage.has_constraints() {
            for (_row_id, update_data) in &updates {
                for (col_name, value) in update_data {
                    let cons = storage.storage.get_column_constraints(col_name);
                    // NOT NULL check
                    if cons.not_null && matches!(value, Value::Null) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "NOT NULL constraint violated: column '{}' cannot be NULL",
                                col_name
                            ),
                        ));
                    }
                }
            }

            // UNIQUE / PK check: ensure new values don't conflict with existing or other updates
            let schema = storage.get_schema();
            let unique_cols: Vec<String> = schema
                .iter()
                .filter(|(name, _)| {
                    let c = storage.storage.get_column_constraints(name);
                    c.unique || c.primary_key
                })
                .map(|(name, _)| name.clone())
                .collect();

            for uc in &unique_cols {
                let constraint_kind = if storage.storage.get_column_constraints(uc).primary_key {
                    "PRIMARY KEY"
                } else {
                    "UNIQUE"
                };

                // Collect new values being set for this unique column
                let updated_row_ids: std::collections::HashSet<u64> = updates
                    .iter()
                    .filter(|(_, data)| data.contains_key(uc))
                    .map(|(id, _)| *id)
                    .collect();
                let new_vals: Vec<&Value> = updates
                    .iter()
                    .filter_map(|(_, data)| data.get(uc))
                    .collect();

                if new_vals.is_empty() {
                    continue;
                }

                // Check duplicates among new values
                {
                    let mut seen = std::collections::HashSet::new();
                    for v in &new_vals {
                        if !matches!(v, Value::Null) {
                            let key = format!("{:?}", v);
                            if !seen.insert(key) {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!(
                                        "{} constraint violated: duplicate value in column '{}'",
                                        constraint_kind, uc
                                    ),
                                ));
                            }
                        }
                    }
                }

                // Check against existing rows (excluding rows being updated)
                if let Some(existing_col) = batch.column_by_name(uc) {
                    use arrow::array::{
                        BooleanArray as BA, Float64Array as F64A, Int64Array as I64A,
                        StringArray as SA,
                    };
                    let id_col = batch.column_by_name("_id");
                    for new_val in &new_vals {
                        if matches!(new_val, Value::Null) {
                            continue;
                        }
                        for row in 0..existing_col.len() {
                            if existing_col.is_null(row) {
                                continue;
                            }
                            // Skip rows that are being updated (they'll have new values)
                            if let Some(idc) = &id_col {
                                let rid = idc
                                    .as_any()
                                    .downcast_ref::<UInt64Array>()
                                    .map(|a| a.value(row))
                                    .or_else(|| {
                                        idc.as_any()
                                            .downcast_ref::<Int64Array>()
                                            .map(|a| a.value(row) as u64)
                                    });
                                if let Some(rid) = rid {
                                    if updated_row_ids.contains(&rid) {
                                        continue;
                                    }
                                }
                            }
                            let matches = match new_val {
                                Value::Int64(v) => existing_col
                                    .as_any()
                                    .downcast_ref::<I64A>()
                                    .map(|a| a.value(row) == *v)
                                    .unwrap_or(false),
                                Value::String(v) => existing_col
                                    .as_any()
                                    .downcast_ref::<SA>()
                                    .map(|a| a.value(row) == v.as_str())
                                    .unwrap_or(false),
                                Value::Float64(v) => existing_col
                                    .as_any()
                                    .downcast_ref::<F64A>()
                                    .map(|a| (a.value(row) - v).abs() < f64::EPSILON)
                                    .unwrap_or(false),
                                Value::Bool(v) => existing_col
                                    .as_any()
                                    .downcast_ref::<BA>()
                                    .map(|a| a.value(row) == *v)
                                    .unwrap_or(false),
                                _ => false,
                            };
                            if matches {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!(
                                        "{} constraint violated: duplicate value in column '{}'",
                                        constraint_kind, uc
                                    ),
                                ));
                            }
                        }
                    }
                }
            }
        }

        // Enforce CHECK constraints on updated values
        if updated > 0 && storage.storage.has_constraints() {
            let schema = storage.get_schema();
            for (schema_col, _) in &schema {
                let cons = storage.storage.get_column_constraints(schema_col);
                if let Some(ref check_sql) = cons.check_expr_sql {
                    let check_expr = {
                        let parse_sql = format!("SELECT 1 FROM _dummy WHERE {}", check_sql);
                        match crate::query::sql_parser::SqlParser::parse(&parse_sql) {
                            Ok(crate::query::sql_parser::SqlStatement::Select(sel)) => {
                                sel.where_clause.clone()
                            }
                            _ => None,
                        }
                    };
                    if let Some(ref expr) = check_expr {
                        for (_row_id, update_data) in &updates {
                            // Build a 1-row batch from the update data
                            let mut fields = Vec::new();
                            let mut arrays: Vec<ArrayRef> = Vec::new();
                            for (cn, value) in update_data {
                                match value {
                                    Value::Int64(v) => {
                                        fields.push(Field::new(cn, ArrowDataType::Int64, true));
                                        arrays.push(Arc::new(Int64Array::from(vec![*v])));
                                    }
                                    Value::Float64(v) => {
                                        fields.push(Field::new(cn, ArrowDataType::Float64, true));
                                        arrays.push(Arc::new(Float64Array::from(vec![*v])));
                                    }
                                    Value::String(v) => {
                                        fields.push(Field::new(cn, ArrowDataType::Utf8, true));
                                        arrays.push(Arc::new(StringArray::from(vec![v.as_str()])));
                                    }
                                    Value::Bool(v) => {
                                        fields.push(Field::new(cn, ArrowDataType::Boolean, true));
                                        arrays.push(Arc::new(BooleanArray::from(vec![*v])));
                                    }
                                    _ => {}
                                }
                            }
                            if !fields.is_empty() {
                                let batch_schema = Arc::new(Schema::new(fields));
                                if let Ok(row_batch) = RecordBatch::try_new(batch_schema, arrays) {
                                    if let Ok(mask) = Self::evaluate_predicate(&row_batch, expr) {
                                        if mask.len() > 0 && !mask.value(0) {
                                            return Err(io::Error::new(
                                                io::ErrorKind::InvalidInput,
                                                format!(
                                                    "CHECK constraint violated: {} (column '{}')",
                                                    check_sql, schema_col
                                                ),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Enforce FOREIGN KEY constraints on updated values
        if updated > 0 && storage.storage.has_constraints() {
            let (base_dir, _) = base_dir_and_table(storage_path);
            let schema = storage.get_schema();
            for (schema_col, _) in &schema {
                let cons = storage.storage.get_column_constraints(schema_col);
                if let Some((ref ref_table, ref ref_column)) = cons.foreign_key {
                    // Collect updated values for this FK column
                    let fk_vals: Vec<&Value> = updates
                        .iter()
                        .filter_map(|(_, data)| data.get(schema_col))
                        .collect();
                    if fk_vals.is_empty() {
                        continue;
                    }

                    let ref_path = base_dir.join(format!("{}.apex", ref_table));
                    if !crate::storage::table_catalog::file_exists_or_registered(&ref_path)? {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!(
                                "FOREIGN KEY: referenced table '{}' does not exist",
                                ref_table
                            ),
                        ));
                    }
                    crate::storage::table_catalog::ensure_table_file(
                        &ref_path,
                        crate::storage::DurabilityLevel::Fast,
                    )?;
                    let ref_storage = TableStorageBackend::open(&ref_path)?;
                    let ref_batch =
                        ref_storage.read_columns_to_arrow(Some(&[ref_column.as_str()]), 0, None)?;
                    let ref_col_arr = ref_batch.column_by_name(ref_column);

                    for val in &fk_vals {
                        if matches!(val, Value::Null) {
                            continue;
                        }
                        let found = if let Some(ref_arr) = ref_col_arr {
                            let mut exists = false;
                            for r in 0..ref_arr.len() {
                                if ref_arr.is_null(r) {
                                    continue;
                                }
                                let matches = match val {
                                    Value::Int64(v) => ref_arr
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .map(|a| a.value(r) == *v)
                                        .unwrap_or(false),
                                    Value::Float64(v) => ref_arr
                                        .as_any()
                                        .downcast_ref::<Float64Array>()
                                        .map(|a| (a.value(r) - v).abs() < f64::EPSILON)
                                        .unwrap_or(false),
                                    Value::String(v) => ref_arr
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .map(|a| a.value(r) == v.as_str())
                                        .unwrap_or(false),
                                    Value::Bool(v) => ref_arr
                                        .as_any()
                                        .downcast_ref::<BooleanArray>()
                                        .map(|a| a.value(r) == *v)
                                        .unwrap_or(false),
                                    _ => false,
                                };
                                if matches {
                                    exists = true;
                                    break;
                                }
                            }
                            exists
                        } else {
                            false
                        };
                        if !found {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("FOREIGN KEY constraint violated: value in column '{}' not found in {}.{}", schema_col, ref_table, ref_column),
                            ));
                        }
                    }
                }
            }
        }

        // Record cell-level updates in DeltaStore in a single batch (one lock acquisition).
        {
            let mut batch: Vec<(u64, &str, Value)> =
                Vec::with_capacity(updates.iter().map(|(_, m)| m.len()).sum());
            for (row_id, update_data) in &updates {
                for (col, val) in update_data {
                    batch.push((*row_id, col.as_str(), val.clone()));
                }
            }
            storage.delta_batch_update_rows(&batch);
        }

        // Persist delta store to disk for crash safety.
        // No compact_deltas() needed: pending deltas are applied at read time via
        // DeltaMerger, and baked into the base file via apply_pending_deltas_in_place()
        // on the next open_for_write (e.g., DELETE). Safe because of Fix B.
        if updated > 0 {
            storage.save_delta_store()?;

            // Index maintenance: update indexed values
            if !indexed_cols.is_empty() {
                Self::notify_index_delete(storage_path, &old_index_entries);
                // Insert new indexed values directly
                let (base_dir, table_name) = base_dir_and_table(storage_path);
                let idx_mgr_arc = get_index_manager(&base_dir, &table_name);
                let mut idx_mgr = idx_mgr_arc.lock();
                if !idx_mgr.list_indexes().is_empty() {
                    for (row_id, col_vals) in &new_index_entries {
                        let _ = idx_mgr.on_insert(*row_id, col_vals);
                    }
                    let _ = idx_mgr.save();
                }
            }
            Self::notify_fts_update(storage_path, fts_updates);
        }

        // Invalidate cache after write
        invalidate_storage_cache(storage_path);
        invalidate_table_stats(&storage_path.to_string_lossy());

        Ok(ApexResult::Scalar(updated))
    }

    fn coerce_update_literal(
        value: &Value,
        target: crate::data::DataType,
        column: &str,
    ) -> io::Result<Value> {
        use crate::data::DataType;

        if matches!(value, Value::Null) {
            return Ok(Value::Null);
        }
        if matches!(target, DataType::Float32 | DataType::Float64 | DataType::Decimal) {
            return value.as_f64().map(Value::Float64).ok_or_else(|| {
                err_input(format!(
                    "Cannot assign {} to numeric column '{}'",
                    value.data_type(),
                    column
                ))
            });
        }
        if matches!(
            target,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ) {
            if let Some(integer) = value.as_i64() {
                return Ok(Value::Int64(integer));
            }
            if let Some(number) = value.as_f64() {
                if number.is_finite()
                    && number >= i64::MIN as f64
                    && number <= i64::MAX as f64
                {
                    return Ok(Value::Int64(number.trunc() as i64));
                }
            }
            return Err(err_input(format!(
                "Cannot safely assign {} to integer column '{}'",
                value.data_type(),
                column
            )));
        }
        match target {
            DataType::String => Ok(Value::String(value.to_string_value())),
            DataType::Bool => value
                .as_bool()
                .map(Value::Bool)
                .ok_or_else(|| err_input(format!("Column '{}' requires BOOL", column))),
            _ if value.data_type() == target => Ok(value.clone()),
            _ => Err(err_input(format!(
                "Cannot assign {} to {} column '{}'",
                value.data_type(),
                target,
                column
            ))),
        }
    }

    pub(in crate::query::executor) fn extract_numeric_range_from_where(expr: &SqlExpr) -> Option<(String, f64, f64)> {
        match expr {
            SqlExpr::Between {
                column,
                low,
                high,
                negated: false,
            } => {
                let lo = Self::literal_to_f64(low)?;
                let hi = Self::literal_to_f64(high)?;
                Some((column.trim_matches('"').to_string(), lo, hi))
            }
            SqlExpr::BinaryOp { left, op, right } => {
                let (col, val, flipped) = if let SqlExpr::Column(c) = left.as_ref() {
                    if let Some(v) = Self::literal_to_f64(right) {
                        (c.trim_matches('"').to_string(), v, false)
                    } else {
                        return None;
                    }
                } else if let SqlExpr::Column(c) = right.as_ref() {
                    if let Some(v) = Self::literal_to_f64(left) {
                        (c.trim_matches('"').to_string(), v, true)
                    } else {
                        return None;
                    }
                } else {
                    return None;
                };
                let inf = f64::INFINITY;
                let ninf = f64::NEG_INFINITY;
                let tiny = 0.5; // integer step for integer columns
                match op {
                    BinaryOperator::Eq => Some((col, val, val)),
                    BinaryOperator::Gt => {
                        if !flipped {
                            Some((col, val + tiny, inf))
                        } else {
                            Some((col, ninf, val - tiny))
                        }
                    }
                    BinaryOperator::Ge => {
                        if !flipped {
                            Some((col, val, inf))
                        } else {
                            Some((col, ninf, val))
                        }
                    }
                    BinaryOperator::Lt => {
                        if !flipped {
                            Some((col, ninf, val - tiny))
                        } else {
                            Some((col, val + tiny, inf))
                        }
                    }
                    BinaryOperator::Le => {
                        if !flipped {
                            Some((col, ninf, val))
                        } else {
                            Some((col, val, inf))
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub(in crate::query::executor) fn literal_to_f64(expr: &SqlExpr) -> Option<f64> {
        match expr {
            SqlExpr::Literal(Value::Int64(v)) => Some(*v as f64),
            SqlExpr::Literal(Value::Float64(v)) => Some(*v),
            _ => None,
        }
    }

    pub(in crate::query::executor) fn collect_column_refs(expr: &SqlExpr) -> Vec<String> {
        let mut refs = Vec::new();
        Self::collect_column_refs_inner(expr, &mut refs);
        refs
    }

    pub(in crate::query::executor) fn collect_column_refs_inner(expr: &SqlExpr, refs: &mut Vec<String>) {
        match expr {
            SqlExpr::Column(name) => refs.push(name.trim_matches('"').to_string()),
            SqlExpr::BinaryOp { left, right, .. } => {
                Self::collect_column_refs_inner(left, refs);
                Self::collect_column_refs_inner(right, refs);
            }
            SqlExpr::UnaryOp { expr, .. } => Self::collect_column_refs_inner(expr, refs),
            SqlExpr::Like { column, .. }
            | SqlExpr::Regexp { column, .. }
            | SqlExpr::In { column, .. }
            | SqlExpr::InSubquery { column, .. }
            | SqlExpr::IsNull { column, .. } => refs.push(column.trim_matches('"').to_string()),
            SqlExpr::Between {
                column, low, high, ..
            } => {
                refs.push(column.trim_matches('"').to_string());
                Self::collect_column_refs_inner(low, refs);
                Self::collect_column_refs_inner(high, refs);
            }
            SqlExpr::Case {
                when_then,
                else_expr,
            } => {
                for (cond, then) in when_then {
                    Self::collect_column_refs_inner(cond, refs);
                    Self::collect_column_refs_inner(then, refs);
                }
                if let Some(e) = else_expr {
                    Self::collect_column_refs_inner(e, refs);
                }
            }
            SqlExpr::Function { args, .. } => {
                for a in args {
                    Self::collect_column_refs_inner(a, refs);
                }
            }
            SqlExpr::Cast { expr, .. } | SqlExpr::Paren(expr) => {
                Self::collect_column_refs_inner(expr, refs);
            }
            SqlExpr::ArrayIndex { array, index } => {
                Self::collect_column_refs_inner(array, refs);
                Self::collect_column_refs_inner(index, refs);
            }
            _ => {}
        }
    }

    pub(in crate::query::executor) fn get_value_at(array: &ArrayRef, row: usize) -> Option<Value> {
        if array.is_null(row) {
            return Some(Value::Null);
        }
        if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
            Some(Value::Int64(arr.value(row)))
        } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
            Some(Value::Float64(arr.value(row)))
        } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            Some(Value::String(arr.value(row).to_string()))
        } else if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
            Some(Value::Bool(arr.value(row)))
        } else if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
            Some(Value::Int64(arr.value(row) as i64))
        } else {
            None
        }
    }

    pub(in crate::query::executor) fn evaluate_expr_to_value(
        batch: &RecordBatch,
        expr: &SqlExpr,
        row: usize,
    ) -> io::Result<Value> {
        match expr {
            SqlExpr::Literal(v) => Ok(v.clone()),
            SqlExpr::Column(name) => {
                let col_name = name.trim_matches('"');
                if let Some(col) = batch.column_by_name(col_name) {
                    if col.is_null(row) {
                        return Ok(Value::Null);
                    }
                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        Ok(Value::Int64(arr.value(row)))
                    } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                        Ok(Value::Float64(arr.value(row)))
                    } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        Ok(Value::String(arr.value(row).to_string()))
                    } else if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                        Ok(Value::Bool(arr.value(row)))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Column '{}' not found", col_name),
                    ))
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Complex expressions in UPDATE not yet supported",
            )),
        }
    }
}
