// Transaction, index, and pragma coordination; DML operations live in submodules.

use super::*;

#[path = "copy.rs"]
mod copy;
#[path = "delete.rs"]
mod delete;
#[path = "insert.rs"]
mod insert;
#[path = "update.rs"]
mod update;

#[derive(Clone, Copy)]
struct PushdownFilter {
    col_idx: usize,
    op: u8,
    op_eq: bool,
    val_f64: f64,
}

impl PushdownFilter {
    #[inline]
    pub(in crate::query::executor) fn matches(self, value: f64) -> bool {
        match self.op {
            b'>' if self.op_eq => value >= self.val_f64,
            b'<' if self.op_eq => value <= self.val_f64,
            b'>' => value > self.val_f64,
            b'<' => value < self.val_f64,
            b'=' => (value - self.val_f64).abs() < 1e-12,
            b'!' => (value - self.val_f64).abs() >= 1e-12,
            _ => true,
        }
    }
}

#[derive(Clone)]
struct JsonNumericFilter {
    key: Vec<u8>,
    op: BinaryOperator,
    flipped: bool,
    val_f64: f64,
}

struct DefaultEvalContext {
    now: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CsvBadLinePolicy {
    Error,
    Skip,
    Warn,
}

fn csv_bad_line_policy(options: &[(String, String)]) -> io::Result<CsvBadLinePolicy> {
    if options.iter().any(|(key, value)| {
        key.eq_ignore_ascii_case("ignore_errors")
            && matches!(value.to_ascii_lowercase().as_str(), "true" | "1")
    }) {
        return Ok(CsvBadLinePolicy::Skip);
    }
    let value = options
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("on_bad_lines"))
        .map(|(_, value)| value.to_ascii_lowercase())
        .unwrap_or_else(|| "error".to_string());
    match value.as_str() {
        "error" => Ok(CsvBadLinePolicy::Error),
        "skip" => Ok(CsvBadLinePolicy::Skip),
        "warn" => Ok(CsvBadLinePolicy::Warn),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "on_bad_lines must be 'error', 'skip', or 'warn'",
        )),
    }
}

#[inline]
fn csv_field_count(line: &[u8], delimiter: u8) -> usize {
    let mut fields = 1usize;
    let mut quoted = false;
    let mut index = 0usize;
    while index < line.len() {
        match line[index] {
            b'"' => {
                if quoted && index + 1 < line.len() && line[index + 1] == b'"' {
                    index += 1;
                } else {
                    quoted = !quoted;
                }
            }
            byte if byte == delimiter && !quoted => fields += 1,
            _ => {}
        }
        index += 1;
    }
    fields
}

impl DefaultEvalContext {
    pub(in crate::query::executor) fn new() -> Self {
        Self {
            now: chrono::Utc::now(),
        }
    }
}

fn parse_pushdown_filter(
    filter_str: &str,
    schema: &arrow::datatypes::Schema,
) -> Option<PushdownFilter> {
    let bytes = filter_str.as_bytes();
    let op_pos = bytes
        .iter()
        .position(|&b| b == b'>' || b == b'<' || b == b'=' || b == b'!')?;
    let col_name = &filter_str[..op_pos];
    let op = bytes[op_pos];
    let op_eq = op_pos + 1 < bytes.len() && bytes[op_pos + 1] == b'=';
    let val_start = if op_eq { op_pos + 2 } else { op_pos + 1 };
    let val_str = &filter_str[val_start..];
    let col_idx = schema.index_of(col_name).ok()?;
    let val_f64: f64 = if val_str.starts_with('\'') || val_str.starts_with('"') {
        return None;
    } else {
        val_str.parse().ok()?
    };
    Some(PushdownFilter {
        col_idx,
        op,
        op_eq,
        val_f64,
    })
}

#[inline]
fn csv_filter_match(field: &[u8], filter: &PushdownFilter) -> bool {
    if field.is_empty() {
        return false;
    }
    let val: Option<f64> = std::str::from_utf8(field).ok().and_then(|s| s.parse().ok());
    val.map(|value| filter.matches(value)).unwrap_or(false)
}

impl ApexExecutor {
    // ========== Transaction Execution Methods ==========

    pub(in crate::query::executor) fn execute_begin(read_only: bool) -> io::Result<ApexResult> {
        let mgr = crate::txn::txn_manager();
        let txn_id = if read_only {
            mgr.begin_read_only()
        } else {
            mgr.begin()
        };
        Ok(ApexResult::Scalar(txn_id as i64))
    }

    pub fn execute_commit_txn(
        txn_id: u64,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<ApexResult> {
        let mgr = crate::txn::txn_manager();

        // Commit validates OCC conflicts and returns buffered writes without
        // cloning them in the executor first.
        let writes = mgr.commit_with_writes(txn_id).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Transaction conflict: {}", e))
        })?;

        // Collect affected table paths
        let mut affected_tables: std::collections::HashSet<std::path::PathBuf> =
            std::collections::HashSet::new();
        for write in &writes {
            let table_name = write.table();
            let table_path = Self::resolve_table_path(table_name, base_dir, default_table_path);
            affected_tables.insert(table_path);
        }
        let _epoch_writes: Vec<_> = affected_tables
            .iter()
            .map(|path| crate::storage::epoch::logical_write(path))
            .collect();

        // Phase 1: Write TxnBegin to each affected table's WAL, if that table
        // is using a WAL-backed durability mode. Do not use get_cached_backend()
        // here: that read path compacts pending .delta files and turns small
        // transaction commits into full-table rewrites.
        let mut wal_backends: std::collections::HashMap<std::path::PathBuf, TableStorageBackend> =
            std::collections::HashMap::new();
        for table_path in &affected_tables {
            if let Ok(Some(backend)) = Self::open_txn_wal_backend(table_path) {
                let _ = backend.storage.wal_write_txn_begin(txn_id);
                wal_backends.insert(table_path.clone(), backend);
            }
        }

        // Phase 2 (P0-4): Write buffered DML to WAL with txn_id BEFORE applying to storage
        // This ensures crash recovery can replay committed transactions from WAL.
        for write in &writes {
            use crate::txn::context::TxnWrite;
            let table_name = write.table();
            let table_path = Self::resolve_table_path(table_name, base_dir, default_table_path);
            if let Some(backend) = wal_backends.get(&table_path) {
                match write {
                    TxnWrite::Insert { data, row_id, .. } => {
                        use crate::storage::on_demand::ColumnValue as CV;
                        let wal_data: std::collections::HashMap<String, CV> = data
                            .iter()
                            .map(|(k, v)| {
                                let cv = match v {
                                    Value::Null => CV::Null,
                                    Value::Bool(b) => CV::Bool(*b),
                                    Value::Int64(i) => CV::Int64(*i),
                                    Value::Int32(i) => CV::Int64(*i as i64),
                                    Value::Float64(f) => CV::Float64(*f),
                                    Value::String(s) => CV::String(s.clone()),
                                    Value::UInt64(u) => CV::Int64(*u as i64),
                                    _ => CV::Null,
                                };
                                (k.clone(), cv)
                            })
                            .collect();
                        let _ = backend
                            .storage
                            .wal_write_txn_insert(txn_id, *row_id, wal_data);
                    }
                    TxnWrite::Delete { row_id, .. } => {
                        let _ = backend.storage.wal_write_txn_delete(txn_id, *row_id);
                    }
                    TxnWrite::Update { .. } => {
                        // Updates are applied as delta store changes (not WAL-logged individually)
                    }
                }
            }
        }

        // Phase 3: Apply buffered writes to storage
        let applied = Self::apply_txn_writes(&writes, base_dir, default_table_path);

        // Phase 4: Write TxnCommit to each affected table's WAL (flush + optional sync)
        for table_path in &affected_tables {
            if let Some(backend) = wal_backends.get(table_path) {
                let _ = backend.storage.wal_write_txn_commit(txn_id);
            }
        }

        // Invalidate read caches for all affected tables.
        let engine = crate::storage::engine::engine();
        for table_path in &affected_tables {
            engine.invalidate(table_path);
            crate::storage::backend::invalidate_global_dict_cache(table_path);
        }

        // Index maintenance is coordinated above storage so StorageEngine never
        // depends on the query runtime. WAL-backed inserts can already be visible
        // to normal reads here, so use the committed transaction payload directly
        // instead of reopening the table and trying to identify its new rows.
        let mut inserted_rows = std::collections::HashMap::new();
        for write in &writes {
            if let crate::txn::context::TxnWrite::Insert {
                table,
                row_id,
                data,
            } = write
            {
                let table_path = Self::resolve_table_path(table, base_dir, default_table_path);
                inserted_rows
                    .entry(table_path)
                    .or_insert_with(Vec::new)
                    .push((*row_id, data));
            }
        }
        for (table_path, rows) in inserted_rows {
            let (index_base_dir, table_name) = base_dir_and_table(&table_path);
            let idx_mgr_arc = get_index_manager(&index_base_dir, &table_name);
            let mut idx_mgr = idx_mgr_arc.lock();
            if idx_mgr.list_indexes().is_empty() {
                continue;
            }
            for (row_id, values) in rows {
                idx_mgr.on_insert(row_id, values)?;
            }
            idx_mgr.save()?;
        }

        Ok(ApexResult::Scalar(applied))
    }

    pub(in crate::query::executor) fn open_txn_wal_backend(storage_path: &Path) -> io::Result<Option<TableStorageBackend>> {
        let wal_path = {
            let mut p = storage_path.to_path_buf();
            let ext = p
                .extension()
                .map(|e| format!("{}.wal", e.to_string_lossy()))
                .unwrap_or_else(|| "wal".to_string());
            p.set_extension(ext);
            p
        };
        if !wal_path.exists() {
            return Ok(None);
        }

        TableStorageBackend::open_for_insert_with_durability(
            storage_path,
            crate::storage::DurabilityLevel::Safe,
        )
        .map(Some)
    }

    pub(in crate::query::executor) fn apply_txn_writes(
        writes: &[crate::txn::context::TxnWrite],
        base_dir: &Path,
        default_table_path: &Path,
    ) -> i64 {
        use crate::txn::context::TxnWrite;

        let mut applied = 0i64;
        let mut idx = 0usize;
        while idx < writes.len() {
            match &writes[idx] {
                TxnWrite::Insert { table, data, .. } => {
                    let columns = Self::txn_insert_columns(data);
                    let mut values_list = Vec::new();
                    let mut row_maps = Vec::new();
                    let mut next_idx = idx;
                    while next_idx < writes.len() {
                        match &writes[next_idx] {
                            TxnWrite::Insert {
                                table: next_table,
                                data: next_data,
                                ..
                            } if next_table == table
                                && Self::txn_insert_columns(next_data) == columns =>
                            {
                                row_maps.push(next_data.clone());
                                values_list.push(
                                    columns
                                        .iter()
                                        .map(|c| next_data.get(c).cloned().unwrap_or(Value::Null))
                                        .collect::<Vec<_>>(),
                                );
                                next_idx += 1;
                            }
                            _ => break,
                        }
                    }

                    let table_path = Self::resolve_table_path(table, base_dir, default_table_path);
                    match Self::try_apply_txn_insert_delta(&table_path, &row_maps) {
                        Ok(Some(count)) => applied += count,
                        Ok(None) => {
                            match Self::execute_insert(&table_path, Some(&columns), &values_list) {
                                Ok(_) => applied += values_list.len() as i64,
                                Err(e) => {
                                    eprintln!("Warning: failed to apply txn insert batch: {}", e)
                                }
                            }
                        }
                        Err(e) => eprintln!("Warning: failed to apply txn insert delta: {}", e),
                    }
                    idx = next_idx;
                }
                _ => {
                    match Self::apply_txn_write(&writes[idx], base_dir, default_table_path) {
                        Ok(count) => applied += count,
                        Err(e) => eprintln!("Warning: failed to apply txn write: {}", e),
                    }
                    idx += 1;
                }
            }
        }
        applied
    }

    pub(in crate::query::executor) fn txn_insert_columns(data: &std::collections::HashMap<String, Value>) -> Vec<String> {
        let mut columns: Vec<String> = data.keys().cloned().collect();
        columns.sort_unstable();
        columns
    }

    pub(in crate::query::executor) fn try_apply_txn_insert_delta(
        storage_path: &Path,
        rows: &[std::collections::HashMap<String, Value>],
    ) -> io::Result<Option<i64>> {
        if rows.is_empty() {
            return Ok(Some(0));
        }
        if !storage_path.exists()
            && !crate::storage::table_catalog::file_exists_or_registered(storage_path)?
        {
            return Ok(Some(0));
        }
        crate::storage::table_catalog::ensure_table_file(
            storage_path,
            crate::storage::DurabilityLevel::Fast,
        )?;

        let storage = TableStorageBackend::open_for_insert(storage_path)?;
        if storage.storage.has_constraints() {
            return Ok(None);
        }

        let schema = storage.get_schema();
        let schema_cols: std::collections::HashSet<&str> =
            schema.iter().map(|(name, _)| name.as_str()).collect();
        for row in rows {
            if row.len() != schema_cols.len()
                || row
                    .keys()
                    .any(|name| name == "_id" || !schema_cols.contains(name.as_str()))
            {
                return Ok(None);
            }
        }

        let (base_dir, table_name) = base_dir_and_table(storage_path);
        {
            let idx_mgr_arc = get_index_manager(&base_dir, &table_name);
            let idx_mgr = idx_mgr_arc.lock();
            if !idx_mgr.list_indexes().is_empty() {
                return Ok(None);
            }
        }
        if Self::table_fts_enabled(&base_dir, &table_name) {
            return Ok(None);
        }

        let ids = storage.insert_rows_to_delta(rows)?;
        refresh_storage_cache_signature(storage_path);
        invalidate_table_stats(&storage_path.to_string_lossy());
        Ok(Some(ids.len() as i64))
    }

    pub(in crate::query::executor) fn table_fts_enabled(base_dir: &Path, table_name: &str) -> bool {
        let cfg = Self::read_fts_config(base_dir);
        cfg.as_object()
            .and_then(|o| o.get(table_name))
            .and_then(|entry| entry.get("enabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    pub(in crate::query::executor) fn apply_txn_write(
        write: &crate::txn::context::TxnWrite,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<i64> {
        use crate::txn::context::TxnWrite;
        match write {
            TxnWrite::Insert { table, data, .. } => {
                let table_path = Self::resolve_table_path(table, base_dir, default_table_path);
                let columns: Vec<String> = data.keys().cloned().collect();
                let values: Vec<Value> = columns.iter().map(|c| data[c].clone()).collect();
                let values_list = vec![values];
                Self::execute_insert(&table_path, Some(&columns), &values_list)?;
                Ok(1)
            }
            TxnWrite::Delete { table, row_id, .. } => {
                let table_path = Self::resolve_table_path(table, base_dir, default_table_path);
                let where_clause = SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::Column("_id".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(SqlExpr::Literal(Value::UInt64(*row_id))),
                };
                Self::execute_delete(&table_path, Some(&where_clause))?;
                Ok(1)
            }
            TxnWrite::Update {
                table,
                row_id,
                new_data,
                ..
            } => {
                let table_path = Self::resolve_table_path(table, base_dir, default_table_path);
                if let Some(count) =
                    Self::try_apply_txn_update_by_id_fast(&table_path, *row_id, new_data)?
                {
                    return Ok(count);
                }

                let assignments: Vec<(String, SqlExpr)> = new_data
                    .iter()
                    .map(|(col, val)| (col.clone(), SqlExpr::Literal(val.clone())))
                    .collect();
                let where_clause = SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::Column("_id".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(SqlExpr::Literal(Value::UInt64(*row_id))),
                };
                Self::execute_update(&table_path, &assignments, Some(&where_clause))?;
                Ok(1)
            }
        }
    }

    pub(in crate::query::executor) fn try_apply_txn_update_by_id_fast(
        storage_path: &Path,
        row_id: u64,
        new_data: &std::collections::HashMap<String, Value>,
    ) -> io::Result<Option<i64>> {
        if new_data.is_empty() || new_data.contains_key("_id") {
            return Ok(None);
        }

        crate::storage::table_catalog::ensure_table_file(
            storage_path,
            crate::storage::DurabilityLevel::Fast,
        )?;
        let storage = TableStorageBackend::open_for_insert(storage_path)?;
        if storage.storage.has_constraints() {
            return Ok(None);
        }

        let (base_dir, table_name) = base_dir_and_table(storage_path);
        {
            let idx_mgr_arc = get_index_manager(&base_dir, &table_name);
            let idx_mgr = idx_mgr_arc.lock();
            if !idx_mgr.list_indexes().is_empty() {
                return Ok(None);
            }
        }
        if Self::table_fts_enabled(&base_dir, &table_name) {
            return Ok(None);
        }

        match storage.row_id_active_rcix(row_id)? {
            Some(true) => {}
            Some(false) => return Ok(Some(0)),
            None => return Ok(None),
        }

        if new_data.len() == 1 {
            let (col_name, value) = new_data.iter().next().unwrap();
            let bytes_opt = match value {
                Value::Float64(f) => Some(f.to_le_bytes()),
                Value::Int64(i) => Some(i.to_le_bytes()),
                Value::Int32(i) => Some((*i as i64).to_le_bytes()),
                _ => None,
            };
            if let Some(bytes) = bytes_opt {
                if let Some((count, physically_written)) =
                    storage.update_by_id_inplace(row_id, col_name, &bytes)?
                {
                    if physically_written {
                        invalidate_storage_cache(storage_path);
                        invalidate_table_stats(&storage_path.to_string_lossy());
                    }
                    return Ok(Some(count));
                }
            }
        }

        let batch: Vec<(u64, &str, Value)> = new_data
            .iter()
            .map(|(col, val)| (row_id, col.as_str(), val.clone()))
            .collect();
        storage.delta_batch_update_rows(&batch);
        storage.save_delta_store()?;
        invalidate_storage_cache(storage_path);
        invalidate_table_stats(&storage_path.to_string_lossy());
        Ok(Some(1))
    }

    pub fn execute_rollback_txn(txn_id: u64) -> io::Result<ApexResult> {
        let mgr = crate::txn::txn_manager();
        mgr.rollback(txn_id)?;
        Ok(ApexResult::Scalar(0))
    }

    pub fn execute_in_txn(
        txn_id: u64,
        stmt: SqlStatement,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<ApexResult> {
        let mgr = crate::txn::txn_manager();

        // Statement-level rollback: create implicit savepoint for DML statements
        let is_dml = matches!(
            &stmt,
            SqlStatement::Insert { .. } | SqlStatement::Delete { .. } | SqlStatement::Update { .. }
        );
        let implicit_sp = "__stmt_savepoint__";
        if is_dml {
            let _ = mgr.with_context(txn_id, |ctx| {
                ctx.savepoint(implicit_sp);
                Ok(())
            });
        }

        let result = Self::execute_in_txn_inner(txn_id, stmt, base_dir, default_table_path, mgr);

        // On DML failure, rollback to implicit savepoint (undo this statement only)
        if is_dml {
            if result.is_err() {
                let _ = mgr.with_context(txn_id, |ctx| ctx.rollback_to_savepoint(implicit_sp));
            } else {
                // Success — release the implicit savepoint
                let _ = mgr.with_context(txn_id, |ctx| ctx.release_savepoint(implicit_sp));
            }
        }

        result
    }

    pub(in crate::query::executor) fn execute_in_txn_inner(
        txn_id: u64,
        stmt: SqlStatement,
        base_dir: &Path,
        default_table_path: &Path,
        mgr: &'static crate::txn::manager::TxnManager,
    ) -> io::Result<ApexResult> {
        match stmt {
            SqlStatement::Insert {
                table,
                columns,
                values,
            } => {
                let table_path = Self::resolve_table_path(&table, base_dir, default_table_path);
                // Reserve synthetic row IDs from the storage allocator so transactional
                // inserts follow the same 1-based monotonic sequence as committed rows.
                let (existing_inserts, cached_base_id) = mgr.with_context(txn_id, |ctx| {
                    Ok((ctx.pending_insert_count(&table), ctx.insert_base_id(&table)))
                })?;
                let mut storage = None;
                let base_root = if let Some(base_id) = cached_base_id {
                    base_id
                } else {
                    crate::storage::table_catalog::ensure_table_file(
                        &table_path,
                        crate::storage::DurabilityLevel::Fast,
                    )?;
                    let opened = TableStorageBackend::open_for_insert(&table_path)?;
                    let base_id = opened.next_id_value().max(crate::storage::FIRST_ROW_ID);
                    storage = Some(opened);
                    base_id
                };
                let remember_base_id = cached_base_id.is_none();
                let base_id = base_root + existing_inserts;
                let (col_names, resolved_values) =
                    if Self::is_insert_default_values(columns.as_deref(), &values) {
                        drop(storage.take());
                        Self::resolve_default_values_insert_for_path(&table_path)?
                    } else {
                        let col_names: Vec<String> = if let Some(cols) = &columns {
                            cols.clone()
                        } else {
                            crate::storage::table_catalog::ensure_table_file(
                                &table_path,
                                crate::storage::DurabilityLevel::Fast,
                            )?;
                            let opened = match storage.take() {
                                Some(opened) => opened,
                                None => TableStorageBackend::open_for_insert(&table_path)?,
                            };
                            let schema = opened.get_schema();
                            schema.iter().map(|(n, _)| n.clone()).collect()
                        };
                        let resolved_values = Self::resolve_insert_values_for_path(
                            &table_path,
                            columns.as_deref(),
                            &values,
                        )?;
                        (col_names, resolved_values)
                    };
                let mut pending_rows = Vec::with_capacity(values.len());
                for (ri, row_values) in resolved_values.iter().enumerate() {
                    let row_id = base_id + ri as u64;
                    let mut data = std::collections::HashMap::new();
                    for (i, val) in row_values.iter().enumerate() {
                        if i < col_names.len() {
                            data.insert(col_names[i].clone(), val.clone());
                        }
                    }
                    pending_rows.push((row_id, data));
                }
                let buffered = pending_rows.len() as i64;
                mgr.with_context(txn_id, |ctx| {
                    if remember_base_id {
                        ctx.remember_insert_base_id(&table, base_root);
                    }
                    for (row_id, data) in pending_rows {
                        ctx.buffer_insert(&table, row_id, data)?;
                    }
                    Ok(())
                })?;
                Ok(ApexResult::Scalar(buffered))
            }
            SqlStatement::Delete {
                table,
                where_clause,
            } => {
                let table_path = Self::resolve_table_path(&table, base_dir, default_table_path);
                crate::storage::table_catalog::ensure_table_file(
                    &table_path,
                    crate::storage::DurabilityLevel::Fast,
                )?;
                let storage = TableStorageBackend::open_for_insert(&table_path)?;
                let mut buffered = 0i64;

                if let Some(where_expr) = &where_clause {
                    if let Some(rid) = Self::extract_id_equality_filter(where_expr) {
                        if let Some(batch) = storage.read_row_by_id_to_arrow(rid)? {
                            let old_data = Self::row_value_map_from_batch(&batch, 0);
                            mgr.with_context(txn_id, |ctx| {
                                ctx.buffer_delete(&table, rid, old_data)
                            })?;
                            return Ok(ApexResult::Scalar(1));
                        }
                        return Ok(ApexResult::Scalar(0));
                    }
                }

                // Read ALL columns so we can capture old_data for VersionStore (snapshot isolation)
                let batch = storage.read_columns_to_arrow(None, 0, None)?;
                let col_names: Vec<String> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();

                let filter = if let Some(where_expr) = &where_clause {
                    Self::evaluate_predicate(&batch, where_expr)?
                } else {
                    BooleanArray::from(vec![true; batch.num_rows()])
                };

                for i in 0..filter.len() {
                    if filter.value(i) {
                        if let Some(id_col) = batch.column_by_name("_id") {
                            let rid = if let Some(a) = id_col.as_any().downcast_ref::<UInt64Array>()
                            {
                                Some(a.value(i))
                            } else if let Some(a) = id_col.as_any().downcast_ref::<Int64Array>() {
                                Some(a.value(i) as u64)
                            } else {
                                None
                            };
                            if let Some(rid) = rid {
                                // Capture old row data for VersionStore
                                let mut old_data = std::collections::HashMap::new();
                                for (ci, cn) in col_names.iter().enumerate() {
                                    if cn == "_id" {
                                        continue;
                                    }
                                    old_data.insert(
                                        cn.clone(),
                                        Self::arrow_value_at_col(batch.column(ci), i),
                                    );
                                }
                                mgr.with_context(txn_id, |ctx| {
                                    ctx.buffer_delete(&table, rid, old_data)
                                })?;
                                buffered += 1;
                            }
                        }
                    }
                }
                Ok(ApexResult::Scalar(buffered))
            }
            SqlStatement::Update {
                table,
                assignments,
                where_clause,
            } => {
                let table_path = Self::resolve_table_path(&table, base_dir, default_table_path);
                crate::storage::table_catalog::ensure_table_file(
                    &table_path,
                    crate::storage::DurabilityLevel::Fast,
                )?;
                let storage = TableStorageBackend::open_for_insert(&table_path)?;

                if let Some(where_expr) = &where_clause {
                    if let Some(rid) = Self::extract_id_equality_filter(where_expr) {
                        if let Some(batch) = storage.read_row_by_id_to_arrow(rid)? {
                            let old_data = Self::row_value_map_from_batch(&batch, 0);
                            let mut new_data = std::collections::HashMap::new();
                            for (col, expr) in &assignments {
                                if let SqlExpr::Literal(val) = expr {
                                    new_data.insert(col.clone(), val.clone());
                                }
                            }
                            mgr.with_context(txn_id, |ctx| {
                                ctx.buffer_update(&table, rid, old_data, new_data)
                            })?;
                            return Ok(ApexResult::Scalar(1));
                        }
                        return Ok(ApexResult::Scalar(0));
                    }
                }

                // Read ALL columns for old_data capture (snapshot isolation)
                let batch = storage.read_columns_to_arrow(None, 0, None)?;
                let col_names: Vec<String> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                let mut buffered = 0i64;

                let filter = if let Some(where_expr) = &where_clause {
                    Self::evaluate_predicate(&batch, where_expr)?
                } else {
                    BooleanArray::from(vec![true; batch.num_rows()])
                };

                for i in 0..filter.len() {
                    if filter.value(i) {
                        if let Some(id_col) = batch.column_by_name("_id") {
                            let rid = if let Some(a) = id_col.as_any().downcast_ref::<UInt64Array>()
                            {
                                Some(a.value(i))
                            } else if let Some(a) = id_col.as_any().downcast_ref::<Int64Array>() {
                                Some(a.value(i) as u64)
                            } else {
                                None
                            };
                            if let Some(rid) = rid {
                                // Capture old row data for VersionStore
                                let mut old_data = std::collections::HashMap::new();
                                for (ci, cn) in col_names.iter().enumerate() {
                                    if cn == "_id" {
                                        continue;
                                    }
                                    old_data.insert(
                                        cn.clone(),
                                        Self::arrow_value_at_col(batch.column(ci), i),
                                    );
                                }
                                let mut new_data = std::collections::HashMap::new();
                                for (col, expr) in &assignments {
                                    if let SqlExpr::Literal(val) = expr {
                                        new_data.insert(col.clone(), val.clone());
                                    }
                                }
                                mgr.with_context(txn_id, |ctx| {
                                    ctx.buffer_update(&table, rid, old_data, new_data)
                                })?;
                                buffered += 1;
                            }
                        }
                    }
                }
                Ok(ApexResult::Scalar(buffered))
            }
            SqlStatement::Select(ref select) => {
                // Extract table name from SELECT for overlay lookup
                let table_name = match &select.from {
                    Some(crate::query::sql_parser::FromItem::Table { table, .. }) => table.clone(),
                    _ => "default".to_string(),
                };

                // Execute SELECT normally against base storage
                let result = Self::execute_parsed_multi(stmt, base_dir, default_table_path)?;

                // Get transaction snapshot timestamp for MVCC visibility
                let snapshot_ts = mgr.get_snapshot_ts(txn_id)?;
                let version_store = mgr.get_version_store(&table_name);

                // Phase A: Overlay buffered writes from this transaction
                let writes = mgr.with_context(txn_id, |ctx| Ok(ctx.write_set().to_vec()))?;

                // Collect inserts and deletes for this table (own writes)
                let mut inserted_rows: Vec<(u64, &std::collections::HashMap<String, Value>)> =
                    Vec::new();
                let mut deleted_ids: std::collections::HashSet<u64> =
                    std::collections::HashSet::new();
                let mut updated_rows: Vec<(u64, &std::collections::HashMap<String, Value>)> =
                    Vec::new();
                for w in &writes {
                    use crate::txn::context::TxnWrite;
                    match w {
                        TxnWrite::Insert {
                            table,
                            row_id,
                            data,
                            ..
                        } if table == &table_name => {
                            inserted_rows.push((*row_id, data));
                        }
                        TxnWrite::Delete { table, row_id, .. } if table == &table_name => {
                            deleted_ids.insert(*row_id);
                        }
                        TxnWrite::Update {
                            table,
                            row_id,
                            new_data,
                            ..
                        } if table == &table_name => {
                            updated_rows.push((*row_id, new_data));
                        }
                        _ => {}
                    }
                }

                // Check if VersionStore has any entries (Phase B: cross-txn isolation)
                let has_versions = version_store.row_count() > 0;
                let has_own_writes = !inserted_rows.is_empty()
                    || !deleted_ids.is_empty()
                    || !updated_rows.is_empty();

                // If no version history and no own writes, return as-is (fast path)
                if !has_versions && !has_own_writes {
                    return Ok(result);
                }

                // Convert result to record batch and apply overlay
                let batch = result.to_record_batch()?;
                let schema = batch.schema();
                let num_cols = batch.num_columns();
                let col_names: Vec<String> =
                    schema.fields().iter().map(|f| f.name().clone()).collect();

                // Build row-level representation with MVCC snapshot filter + own-write overlay
                let mut rows: Vec<Vec<Value>> = Vec::new();
                for row_idx in 0..batch.num_rows() {
                    // Get row ID
                    let rid = if let Some(id_col) = batch.column_by_name("_id") {
                        if let Some(a) = id_col.as_any().downcast_ref::<UInt64Array>() {
                            Some(a.value(row_idx))
                        } else if let Some(a) = id_col.as_any().downcast_ref::<Int64Array>() {
                            Some(a.value(row_idx) as u64)
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    // Check own deletes first
                    if let Some(rid) = rid {
                        if deleted_ids.contains(&rid) {
                            continue; // Deleted by this transaction
                        }
                    }

                    // Phase B: MVCC snapshot isolation — check VersionStore
                    if has_versions {
                        if let Some(rid) = rid {
                            // Check if VersionStore has a version chain for this row
                            let visible = version_store.read(rid, snapshot_ts);
                            let exists_in_vs = version_store.row_count() > 0 && {
                                // Check if this specific row has a chain
                                version_store.exists(rid, u64::MAX - 2) || // exists at any ts
                                version_store.read_latest(rid).is_some() ||
                                version_store.read(rid, u64::MAX - 2).is_some()
                            };

                            if exists_in_vs {
                                // Row has version history — use snapshot visibility
                                if version_store.read(rid, snapshot_ts).is_none() {
                                    // Not visible at snapshot_ts:
                                    // Either inserted after snapshot or deleted before snapshot
                                    // Check if it's an insert after snapshot (begin_ts > snapshot_ts)
                                    let is_deleted = {
                                        let chains = version_store.chains_ref();
                                        chains
                                            .get(&rid)
                                            .map(|c| c.is_deleted_at(snapshot_ts))
                                            .unwrap_or(false)
                                    };
                                    if is_deleted {
                                        // Row was deleted before our snapshot — already gone
                                        continue;
                                    }
                                    // Row was inserted after our snapshot — invisible
                                    continue;
                                }
                                // Visible: check if we should use version data instead of storage data
                                if let Some(versioned_data) = visible {
                                    // Use versioned data (may differ from current storage)
                                    let mut row = Vec::with_capacity(num_cols);
                                    for cn in &col_names {
                                        if cn == "_id" {
                                            row.push(Value::UInt64(rid));
                                        } else if let Some(val) = versioned_data.get(cn) {
                                            row.push(val.clone());
                                        } else {
                                            // Column not in version data, use storage value
                                            let ci =
                                                col_names.iter().position(|n| n == cn).unwrap();
                                            row.push(Self::arrow_value_at_col(
                                                batch.column(ci),
                                                row_idx,
                                            ));
                                        }
                                    }
                                    // Apply own updates on top
                                    for (uid, new_data) in &updated_rows {
                                        if rid == *uid {
                                            for (col, val) in *new_data {
                                                if let Some(ci) =
                                                    col_names.iter().position(|n| n == col)
                                                {
                                                    row[ci] = val.clone();
                                                }
                                            }
                                        }
                                    }
                                    rows.push(row);
                                    continue;
                                }
                            }
                        }
                    }

                    // No version history for this row — use storage data as-is
                    let mut row = Vec::with_capacity(num_cols);
                    for col_idx in 0..num_cols {
                        row.push(Self::arrow_value_at_col(batch.column(col_idx), row_idx));
                    }

                    // Apply own updates
                    if !updated_rows.is_empty() {
                        if let Some(rid) = rid {
                            for (uid, new_data) in &updated_rows {
                                if rid == *uid {
                                    for (col, val) in *new_data {
                                        if let Some(ci) = col_names.iter().position(|n| n == col) {
                                            row[ci] = val.clone();
                                        }
                                    }
                                }
                            }
                        }
                    }

                    rows.push(row);
                }

                // Append buffered inserts (own writes)
                for (row_id, insert_data) in &inserted_rows {
                    let mut row = vec![Value::Null; num_cols];
                    if let Some(ci) = col_names.iter().position(|n| n == "_id") {
                        row[ci] = Value::Int64(*row_id as i64);
                    }
                    for (col, val) in *insert_data {
                        if let Some(ci) = col_names.iter().position(|n| n == col) {
                            row[ci] = val.clone();
                        }
                    }
                    rows.push(row);
                }

                // Convert back to RecordBatch
                Self::rows_to_apex_result(&col_names, &rows, &schema)
            }
            SqlStatement::Commit => Self::execute_commit_txn(txn_id, base_dir, default_table_path),
            SqlStatement::Rollback => Self::execute_rollback_txn(txn_id),
            SqlStatement::Savepoint { name } => {
                mgr.with_context(txn_id, |ctx| {
                    ctx.savepoint(&name);
                    Ok(())
                })?;
                Ok(ApexResult::Scalar(0))
            }
            SqlStatement::RollbackToSavepoint { name } => {
                mgr.with_context(txn_id, |ctx| ctx.rollback_to_savepoint(&name))?;
                Ok(ApexResult::Scalar(0))
            }
            SqlStatement::ReleaseSavepoint { name } => {
                mgr.with_context(txn_id, |ctx| ctx.release_savepoint(&name))?;
                Ok(ApexResult::Scalar(0))
            }
            other => Self::execute_parsed_multi(other, base_dir, default_table_path),
        }
    }

    // ========== Index DDL Execution Methods ==========

    pub(in crate::query::executor) fn execute_create_index(
        base_dir: &Path,
        default_table_path: &Path,
        name: &str,
        table: &str,
        columns: &[String],
        unique: bool,
        index_type_str: Option<&str>,
        if_not_exists: bool,
    ) -> io::Result<ApexResult> {
        use crate::storage::index::IndexType;

        if columns.is_empty() {
            return Err(err_input("CREATE INDEX requires at least one column"));
        }

        let idx_type = match index_type_str {
            Some("HASH") => IndexType::Hash,
            Some("BTREE") | Some("B-TREE") | Some("B_TREE") => IndexType::BTree,
            None => {
                // Default: HASH for equality-heavy columns, BTREE otherwise
                IndexType::Hash
            }
            Some(other) => {
                return Err(err_input(format!(
                    "Unknown index type: {}. Use HASH or BTREE",
                    other
                )))
            }
        };

        let table_path = Self::resolve_table_path(table, base_dir, default_table_path);
        if !crate::storage::table_catalog::file_exists_or_registered(&table_path)? {
            return Err(err_not_found(format!("Table '{}' does not exist", table)));
        }

        // Get the IndexManager for this table
        let idx_mgr_arc = get_index_manager(base_dir, table);
        let mut idx_mgr = idx_mgr_arc.lock();

        // Check if already exists
        if idx_mgr.get_index_meta(name).is_some() {
            if if_not_exists {
                return Ok(ApexResult::Scalar(0));
            }
            return Err(err_input(format!(
                "Index '{}' already exists on table '{}'",
                name, table
            )));
        }

        // Determine column data type from table schema (use first column's
        // type). A registered lazy table is materialized with its catalog
        // schema before the index build reads it.
        crate::storage::table_catalog::ensure_table_file(
            &table_path,
            crate::storage::DurabilityLevel::Fast,
        )?;
        let storage = TableStorageBackend::open(&table_path)?;
        let schema = storage.get_schema();
        let first_col = &columns[0];
        let data_type = schema
            .iter()
            .find(|(n, _)| n == first_col)
            .map(|(_, dt)| dt.clone())
            .ok_or_else(|| {
                err_not_found(format!(
                    "Column '{}' not found in table '{}'",
                    first_col, table
                ))
            })?;

        // Validate all columns exist
        for col in columns {
            if !schema.iter().any(|(n, _)| n == col) {
                return Err(err_not_found(format!(
                    "Column '{}' not found in table '{}'",
                    col, table
                )));
            }
        }

        // Create the index (supports single or multi-column)
        idx_mgr.create_index_multi(name, columns, idx_type, unique, data_type)?;

        // Build index from existing data
        let row_count = storage.row_count();
        if row_count > 0 {
            const INDEX_BUILD_BATCH_ROWS: usize = 65_536;

            let build_result = (|| -> io::Result<()> {
                let mut start_row = 0usize;
                let total_rows = row_count as usize;

                while start_row < total_rows {
                    let limit = (total_rows - start_row).min(INDEX_BUILD_BATCH_ROWS);
                    let batch = storage.read_columns_to_arrow(None, start_row, Some(limit))?;

                    if batch.num_rows() == 0 {
                        break;
                    }

                    let id_col = batch
                        .column_by_name("_id")
                        .ok_or_else(|| err_data("_id column not found"))?;

                    let id_arr = id_col.as_any().downcast_ref::<UInt64Array>();
                    let id_arr_i64 = id_col.as_any().downcast_ref::<Int64Array>();

                    for row in 0..batch.num_rows() {
                        let row_id: u64 = if let Some(arr) = id_arr {
                            arr.value(row)
                        } else if let Some(arr) = id_arr_i64 {
                            arr.value(row) as u64
                        } else {
                            (start_row + row) as u64
                        };

                        // Extract values from all indexed columns
                        let mut col_vals = std::collections::HashMap::with_capacity(columns.len());
                        for col in columns {
                            if let Some(data_col) = batch.column_by_name(col) {
                                col_vals
                                    .insert(col.clone(), Self::arrow_value_at_col(data_col, row));
                            }
                        }
                        idx_mgr.on_insert(row_id, &col_vals)?;
                    }

                    start_row += batch.num_rows();
                }

                Ok(())
            })();

            if let Err(err) = build_result {
                let _ = idx_mgr.drop_index(name);
                return Err(err);
            }
        }

        // Save index to disk
        idx_mgr.save()?;

        Ok(ApexResult::Scalar(row_count as i64))
    }

    pub(in crate::query::executor) fn execute_drop_index(
        base_dir: &Path,
        name: &str,
        table: &str,
        if_exists: bool,
    ) -> io::Result<ApexResult> {
        let idx_mgr_arc = get_index_manager(base_dir, table);
        let mut idx_mgr = idx_mgr_arc.lock();

        if idx_mgr.get_index_meta(name).is_none() {
            if if_exists {
                return Ok(ApexResult::Scalar(0));
            }
            return Err(err_not_found(format!(
                "Index '{}' not found on table '{}'",
                name, table
            )));
        }

        idx_mgr.drop_index(name)?;
        idx_mgr.save()?;

        // Invalidate index cache to reload on next access
        invalidate_index_cache(base_dir, table);

        Ok(ApexResult::Scalar(0))
    }

    pub(in crate::query::executor) fn rows_to_apex_result(
        col_names: &[String],
        rows: &[Vec<Value>],
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> io::Result<ApexResult> {
        use arrow::array::{
            ArrayBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
            UInt64Builder,
        };
        use arrow::datatypes::DataType;

        let num_rows = rows.len();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            match field.data_type() {
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(num_rows);
                    for row in rows {
                        match row.get(col_idx) {
                            Some(Value::Int64(v)) => builder.append_value(*v),
                            Some(Value::Int32(v)) => builder.append_value(*v as i64),
                            Some(Value::UInt64(v)) => builder.append_value(*v as i64),
                            Some(Value::Null) | None => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::UInt64 => {
                    let mut builder = UInt64Builder::with_capacity(num_rows);
                    for row in rows {
                        match row.get(col_idx) {
                            Some(Value::UInt64(v)) => builder.append_value(*v),
                            Some(Value::Int64(v)) => builder.append_value(*v as u64),
                            Some(Value::Null) | None => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(num_rows);
                    for row in rows {
                        match row.get(col_idx) {
                            Some(Value::Float64(v)) => builder.append_value(*v),
                            Some(Value::Float32(v)) => builder.append_value(*v as f64),
                            Some(Value::Null) | None => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                    for row in rows {
                        match row.get(col_idx) {
                            Some(Value::String(v)) => builder.append_value(v),
                            Some(Value::Null) | None => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Boolean => {
                    let mut builder = BooleanBuilder::with_capacity(num_rows);
                    for row in rows {
                        match row.get(col_idx) {
                            Some(Value::Bool(v)) => builder.append_value(*v),
                            Some(Value::Null) | None => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                _ => {
                    // Fallback: null array
                    let mut builder = StringBuilder::with_capacity(num_rows, 0);
                    for _ in 0..num_rows {
                        builder.append_null();
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
            }
        }

        // Make all fields nullable to avoid Arrow errors when overlay produces nulls
        let nullable_fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| Field::new(f.name(), f.data_type().clone(), true))
            .collect();
        let nullable_schema = Arc::new(Schema::new(nullable_fields));
        let batch = RecordBatch::try_new(nullable_schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(ApexResult::Data(batch))
    }

    pub(in crate::query::executor) fn arrow_value_at_col(col: &ArrayRef, row: usize) -> Value {
        use arrow::array::{
            BinaryArray, BooleanArray, DictionaryArray, Float32Array, Int16Array, Int32Array,
            Int8Array, LargeBinaryArray, LargeStringArray, UInt16Array, UInt32Array, UInt8Array,
        };
        use arrow::datatypes::{
            Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
            UInt8Type,
        };
        if col.is_null(row) {
            return Value::Null;
        }
        macro_rules! dictionary_string {
            ($key_type:ty) => {
                if let Some(arr) = col.as_any().downcast_ref::<DictionaryArray<$key_type>>() {
                    let key = arr.keys().value(row) as usize;
                    return arr
                        .values()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .filter(|values| key < values.len())
                        .map(|values| Value::String(values.value(key).to_string()))
                        .unwrap_or(Value::Null);
                }
            };
        }
        dictionary_string!(Int8Type);
        dictionary_string!(Int16Type);
        dictionary_string!(Int32Type);
        dictionary_string!(Int64Type);
        dictionary_string!(UInt8Type);
        dictionary_string!(UInt16Type);
        dictionary_string!(UInt32Type);
        dictionary_string!(UInt64Type);
        if let Some(arr) = col.as_any().downcast_ref::<Int8Array>() {
            Value::Int8(arr.value(row))
        } else if let Some(arr) = col.as_any().downcast_ref::<Int16Array>() {
            Value::Int16(arr.value(row))
        } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
            Value::Int32(arr.value(row))
        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            Value::Int64(arr.value(row))
        } else if let Some(arr) = col.as_any().downcast_ref::<UInt8Array>() {
            Value::UInt8(arr.value(row))
        } else if let Some(arr) = col.as_any().downcast_ref::<UInt16Array>() {
            Value::UInt16(arr.value(row))
        } else if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
            Value::UInt32(arr.value(row))
        } else if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
            Value::UInt64(arr.value(row))
        } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            Value::Float64(arr.value(row))
        } else if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
            Value::Float32(arr.value(row))
        } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            Value::String(arr.value(row).to_string())
        } else if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
            Value::String(arr.value(row).to_string())
        } else if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
            Value::Binary(arr.value(row).to_vec())
        } else if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
            Value::Binary(arr.value(row).to_vec())
        } else if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
            Value::Bool(arr.value(row))
        } else {
            // Projection/window readers may expose encoded Arrow types that
            // differ from the full-scan representation. Preserve index
            // correctness by decoding the scalar display form as a final,
            // cold-path fallback during index maintenance and ANALYZE.
            arrow::util::display::array_value_to_string(col.as_ref(), row)
                .ok()
                .map(|value| {
                    value
                        .parse::<i64>()
                        .map(Value::Int64)
                        .or_else(|_| value.parse::<f64>().map(Value::Float64))
                        .unwrap_or_else(|_| Value::String(value))
                })
                .unwrap_or(Value::Null)
        }
    }

    pub(in crate::query::executor) fn row_value_map_from_batch(
        batch: &RecordBatch,
        row: usize,
    ) -> std::collections::HashMap<String, Value> {
        let mut data = std::collections::HashMap::new();
        for (ci, field) in batch.schema().fields().iter().enumerate() {
            let name = field.name();
            if name == "_id" {
                continue;
            }
            data.insert(
                name.clone(),
                Self::arrow_value_at_col(batch.column(ci), row),
            );
        }
        data
    }

    // ========== Index Maintenance Helpers ==========

    pub fn notify_indexes_after_write(storage_path: &Path, inserted_ids: &[u64]) {
        if inserted_ids.is_empty() {
            return;
        }
        let (base_dir, table_name) = base_dir_and_table(storage_path);
        let idx_mgr_arc = get_index_manager(&base_dir, &table_name);
        let mut idx_mgr = idx_mgr_arc.lock();
        if idx_mgr.list_indexes().is_empty() {
            return;
        }

        let indexed_cols: Vec<String> = idx_mgr
            .list_indexes()
            .iter()
            .flat_map(|meta| meta.effective_columns().into_iter().map(str::to_owned))
            .collect();
        let mut col_names: Vec<String> = vec!["_id".to_string()];
        col_names.extend(indexed_cols.iter().cloned());
        col_names.sort();
        col_names.dedup();

        // Open a read backend to get inserted row data
        if let Ok(backend) = TableStorageBackend::open(storage_path) {
            let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
            if let Ok(batch) = backend.read_columns_to_arrow(Some(&col_refs), 0, None) {
                let id_col = batch.column_by_name("_id");
                let id_set: std::collections::HashSet<u64> = inserted_ids.iter().copied().collect();
                for row in 0..batch.num_rows() {
                    let row_id = if let Some(col) = id_col {
                        if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                            arr.value(row)
                        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                            arr.value(row) as u64
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    };

                    if !id_set.contains(&row_id) {
                        continue;
                    }

                    let mut col_vals = std::collections::HashMap::new();
                    for col_name in &indexed_cols {
                        if let Some(col) = batch.column_by_name(col_name) {
                            col_vals.insert(col_name.clone(), Self::arrow_value_at_col(col, row));
                        }
                    }
                    let _ = idx_mgr.on_insert(row_id, &col_vals);
                }
                let _ = idx_mgr.save();
            }
        }
    }

    pub fn notify_indexes_after_delete(
        storage_path: &Path,
        deleted_ids: &[u64],
        column_values: &[std::collections::HashMap<String, Value>],
    ) {
        if deleted_ids.is_empty() {
            return;
        }
        let (base_dir, table_name) = base_dir_and_table(storage_path);
        let idx_mgr_arc = get_index_manager(&base_dir, &table_name);
        let mut idx_mgr = idx_mgr_arc.lock();
        if idx_mgr.list_indexes().is_empty() {
            return;
        }
        for (i, &row_id) in deleted_ids.iter().enumerate() {
            if i < column_values.len() {
                idx_mgr.on_delete(row_id, &column_values[i]);
            }
        }
        let _ = idx_mgr.save();
    }

    pub(in crate::query::executor) fn notify_index_insert(
        storage_path: &Path,
        storage: &TableStorageBackend,
        start_row_count: u64,
    ) {
        let (base_dir, table_name) = base_dir_and_table(storage_path);
        let idx_mgr_arc = get_index_manager(&base_dir, &table_name);
        let mut idx_mgr = idx_mgr_arc.lock();
        if idx_mgr.list_indexes().is_empty() {
            return;
        }

        // Read _id + indexed columns for new rows
        let indexed_cols: Vec<String> = idx_mgr
            .list_indexes()
            .iter()
            .flat_map(|meta| meta.effective_columns().into_iter().map(str::to_owned))
            .collect();
        let mut col_names: Vec<String> = vec!["_id".to_string()];
        col_names.extend(indexed_cols.iter().cloned());
        col_names.sort();
        col_names.dedup();

        let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        let new_count = storage.row_count();
        if new_count <= start_row_count {
            return;
        }
        // Read all rows and only process new ones (rows after start_row_count)
        if let Ok(batch) = storage.read_columns_to_arrow(Some(&col_refs), 0, None) {
            let id_col = batch.column_by_name("_id");
            // Process rows from start_row_count onwards
            let start = start_row_count as usize;
            for row in start..batch.num_rows() {
                let row_id = if let Some(col) = id_col {
                    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                        arr.value(row)
                    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        arr.value(row) as u64
                    } else {
                        continue;
                    }
                } else {
                    continue;
                };

                let mut col_vals = std::collections::HashMap::new();
                for col_name in &indexed_cols {
                    if let Some(col) = batch.column_by_name(col_name) {
                        col_vals.insert(col_name.clone(), Self::arrow_value_at_col(col, row));
                    }
                }
                let _ = idx_mgr.on_insert(row_id, &col_vals);
            }
            let _ = idx_mgr.save();
        }
    }

    pub(in crate::query::executor) fn notify_index_delete(
        storage_path: &Path,
        deleted_entries: &[(u64, std::collections::HashMap<String, Value>)],
    ) {
        if deleted_entries.is_empty() {
            return;
        }
        let (base_dir, table_name) = base_dir_and_table(storage_path);
        let idx_mgr_arc = get_index_manager(&base_dir, &table_name);
        let mut idx_mgr = idx_mgr_arc.lock();
        if idx_mgr.list_indexes().is_empty() {
            return;
        }
        for (row_id, col_vals) in deleted_entries {
            idx_mgr.on_delete(*row_id, col_vals);
        }
        let _ = idx_mgr.save();
    }

    pub(in crate::query::executor) fn notify_fts_insert(storage_path: &Path, storage: &TableStorageBackend, start_row_count: u64) {
        use arrow::array::{Int64Array, StringArray, UInt64Array};

        let (base_dir, table_name) = base_dir_and_table(storage_path);

        // Check if FTS is enabled for this table via config
        let cfg = Self::read_fts_config(&base_dir);
        let table_cfg = cfg.as_object().and_then(|o| o.get(&table_name));
        let enabled = table_cfg
            .and_then(|e| e.get("enabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !enabled {
            return;
        }

        let fields: Option<Vec<String>> = table_cfg
            .and_then(|e| e.get("index_fields"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            });

        let lazy_load = table_cfg
            .and_then(|e| e.get("config"))
            .and_then(|c| c.get("lazy_load"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let cache_size = table_cfg
            .and_then(|e| e.get("config"))
            .and_then(|c| c.get("cache_size"))
            .and_then(|v| v.as_u64())
            .unwrap_or(10000) as usize;
        let fts_cfg = crate::fts::FtsConfig {
            lazy_load,
            cache_size,
            ..crate::fts::FtsConfig::default()
        };

        // Get or create FTS manager for this database
        let mgr_arc = {
            if let Some(m) = get_fts_manager(&base_dir) {
                m
            } else {
                let fts_dir = base_dir.join("fts_indexes");
                let m = std::sync::Arc::new(crate::fts::FtsManager::new(&fts_dir, fts_cfg.clone()));
                crate::query::executor::register_fts_manager(&base_dir, m.clone());
                m
            }
        };
        mgr_arc.configure_table(&table_name, fts_cfg);

        let engine = match mgr_arc.get_engine(&table_name) {
            Ok(e) => e,
            Err(_) => return,
        };
        crate::query::executor::wait_fts_backfill(&base_dir, &table_name);

        // Determine string columns to index
        let schema = storage.get_schema();
        let string_cols: Vec<String> = if let Some(f) = &fields {
            f.clone()
        } else {
            schema
                .iter()
                .filter(|(_, dt)| matches!(dt, crate::data::DataType::String))
                .map(|(n, _)| n.clone())
                .collect()
        };
        if string_cols.is_empty() {
            return;
        }

        let new_count = storage.row_count();
        if new_count <= start_row_count {
            return;
        }

        let mut col_names = vec!["_id".to_string()];
        col_names.extend(string_cols.iter().cloned());
        col_names.sort();
        col_names.dedup();
        let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

        let batch = match storage.read_columns_to_arrow(Some(&col_refs), 0, None) {
            Ok(b) => b,
            Err(_) => return,
        };

        let id_col = match batch.column_by_name("_id") {
            Some(c) => c,
            None => return,
        };

        let start = start_row_count as usize;
        let mut ids: Vec<u64> = Vec::new();
        let mut col_data: Vec<Vec<String>> = vec![Vec::new(); string_cols.len()];

        for row in start..batch.num_rows() {
            let rid = if let Some(arr) = id_col.as_any().downcast_ref::<UInt64Array>() {
                arr.value(row)
            } else if let Some(arr) = id_col.as_any().downcast_ref::<Int64Array>() {
                arr.value(row) as u64
            } else {
                continue;
            };
            ids.push(rid);
            for (ci, col_name) in string_cols.iter().enumerate() {
                let val = batch
                    .column_by_name(col_name)
                    .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                    .map(|arr| {
                        if arr.is_null(row) {
                            String::new()
                        } else {
                            arr.value(row).to_string()
                        }
                    })
                    .unwrap_or_default();
                col_data[ci].push(val);
            }
        }

        if ids.is_empty() {
            return;
        }

        let columns: Vec<(String, Vec<String>)> = string_cols.into_iter().zip(col_data).collect();
        let _ = engine.add_documents_columnar(ids, columns);
        let _ = engine.flush();
    }

    pub(in crate::query::executor) fn notify_fts_delete(
        storage_path: &Path,
        deleted_entries: &[(u64, std::collections::HashMap<String, Value>)],
    ) {
        if deleted_entries.is_empty() {
            return;
        }
        let (base_dir, table_name) = base_dir_and_table(storage_path);

        // Check if FTS is enabled for this table
        let cfg = Self::read_fts_config(&base_dir);
        let enabled = cfg
            .as_object()
            .and_then(|o| o.get(&table_name))
            .and_then(|e| e.get("enabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !enabled {
            return;
        }

        // Only sync if manager is already registered (avoid creating one just for a delete)
        let mgr_arc = match get_fts_manager(&base_dir) {
            Some(m) => m,
            None => return,
        };

        let engine = match mgr_arc.get_engine(&table_name) {
            Ok(e) => e,
            Err(_) => return,
        };
        crate::query::executor::wait_fts_backfill(&base_dir, &table_name);

        let ids: Vec<u64> = deleted_entries.iter().map(|(id, _)| *id).collect();
        let _ = engine.remove_documents(&ids);
        let _ = engine.flush();
    }

    pub(in crate::query::executor) fn notify_fts_update(
        storage_path: &Path,
        documents: Vec<(u64, std::collections::HashMap<String, String>)>,
    ) {
        if documents.is_empty() {
            return;
        }
        let (base_dir, table_name) = base_dir_and_table(storage_path);
        let cfg = Self::read_fts_config(&base_dir);
        let table_cfg = cfg.as_object().and_then(|o| o.get(&table_name));
        if !table_cfg
            .and_then(|e| e.get("enabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return;
        }
        let lazy_load = table_cfg
            .and_then(|e| e.get("config"))
            .and_then(|c| c.get("lazy_load"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let cache_size = table_cfg
            .and_then(|e| e.get("config"))
            .and_then(|c| c.get("cache_size"))
            .and_then(|v| v.as_u64())
            .unwrap_or(10_000) as usize;
        let fts_config = crate::fts::FtsConfig {
            lazy_load,
            cache_size,
            ..crate::fts::FtsConfig::default()
        };
        let manager = get_fts_manager(&base_dir).unwrap_or_else(|| {
            let manager = std::sync::Arc::new(crate::fts::FtsManager::new(
                base_dir.join("fts_indexes"),
                fts_config.clone(),
            ));
            crate::query::executor::register_fts_manager(&base_dir, manager.clone());
            manager
        });
        manager.configure_table(&table_name, fts_config);
        crate::query::executor::wait_fts_backfill(&base_dir, &table_name);
        if let Ok(engine) = manager.get_engine(&table_name) {
            if engine.add_documents(documents).is_ok() {
                let _ = engine.flush();
            }
        }
    }

    // ========== DML Execution Methods ==========

    #[inline]
    pub(in crate::query::executor) fn execute_analyze(storage_path: &Path, table_name: &str) -> io::Result<ApexResult> {
        crate::storage::table_catalog::ensure_table_file(
            storage_path,
            crate::storage::DurabilityLevel::Fast,
        )?;
        let storage = TableStorageBackend::open(storage_path)?;
        let batch = storage.read_columns_to_arrow(None, 0, None)?;
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        let mut col_names: Vec<String> = Vec::new();
        let mut col_types: Vec<String> = Vec::new();
        let mut ndv_vals: Vec<i64> = Vec::new();
        let mut null_counts: Vec<i64> = Vec::new();
        let mut min_vals: Vec<String> = Vec::new();
        let mut max_vals: Vec<String> = Vec::new();
        let mut row_counts: Vec<i64> = Vec::new();
        let mut col_stats_map: std::collections::HashMap<
            String,
            crate::query::planner::ColumnStats,
        > = std::collections::HashMap::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            col_names.push(field.name().clone());
            col_types.push(format!("{}", field.data_type()));
            row_counts.push(num_rows as i64);

            // Count nulls
            let nc = (0..num_rows).filter(|&i| col.is_null(i)).count() as i64;
            null_counts.push(nc);

            // Collect distinct values and typed numeric bounds.  Keep exact
            // NDV for normal tables, but sample very large columns so ANALYZE
            // does not allocate one string per row indefinitely.
            let mut distinct = std::collections::HashSet::new();
            let mut frequencies = std::collections::HashMap::<String, u64>::new();
            let mut min_s = String::new();
            let mut max_s = String::new();
            let mut numeric_min: Option<f64> = None;
            let mut numeric_max: Option<f64> = None;
            let mut first = true;
            let sample_limit = 100_000usize;
            let sample_stride = (num_rows / sample_limit).max(1);

            for i in 0..num_rows {
                if col.is_null(i) {
                    continue;
                }
                let value = Self::arrow_value_at_col(col, i);
                let val_str = value.to_string();
                if i % sample_stride == 0 {
                    distinct.insert(val_str.clone());
                    *frequencies.entry(val_str.clone()).or_default() += 1;
                }
                let numeric = match value {
                    Value::Int64(v) => Some(v as f64),
                    Value::UInt64(v) => Some(v as f64),
                    Value::Float64(v) => Some(v),
                    _ => None,
                };
                if let Some(value) = numeric {
                    numeric_min = Some(numeric_min.map_or(value, |min| min.min(value)));
                    numeric_max = Some(numeric_max.map_or(value, |max| max.max(value)));
                }
                if first {
                    min_s = val_str.clone();
                    max_s = val_str.clone();
                    first = false;
                } else {
                    if val_str < min_s {
                        min_s = val_str.clone();
                    }
                    if val_str > max_s {
                        max_s = val_str;
                    }
                }
            }
            let sampled_rows = ((num_rows + sample_stride - 1) / sample_stride).max(1);
            let estimated_ndv = if sample_stride == 1 {
                distinct.len() as u64
            } else {
                ((distinct.len() as f64 * num_rows as f64 / sampled_rows as f64).round() as u64)
                    .min(num_rows as u64)
            };
            let mut most_common_values: Vec<(String, u64)> = frequencies
                .into_iter()
                .map(|(value, count)| (value, count.saturating_mul(sample_stride as u64)))
                .collect();
            most_common_values.sort_unstable_by(|left, right| right.1.cmp(&left.1));
            most_common_values.truncate(16);
            ndv_vals.push(estimated_ndv as i64);
            min_vals.push(min_s.clone());
            max_vals.push(max_s.clone());

            let histogram = match (numeric_min, numeric_max) {
                (Some(min), Some(max)) if max > min => {
                    const BUCKETS: usize = 32;
                    let mut counts = vec![0u64; BUCKETS];
                    let width = (max - min) / BUCKETS as f64;
                    for i in 0..num_rows {
                        if col.is_null(i) {
                            continue;
                        }
                        let value = match Self::arrow_value_at_col(col, i) {
                            Value::Int64(v) => Some(v as f64),
                            Value::UInt64(v) => Some(v as f64),
                            Value::Float64(v) => Some(v),
                            _ => None,
                        };
                        if let Some(value) = value {
                            let bucket =
                                (((value - min) / width).floor() as usize).min(BUCKETS - 1);
                            counts[bucket] += 1;
                        }
                    }
                    counts
                        .into_iter()
                        .enumerate()
                        .map(
                            |(bucket, row_count)| crate::query::planner::HistogramBucket {
                                lower: min + width * bucket as f64,
                                upper: if bucket + 1 == BUCKETS {
                                    max
                                } else {
                                    min + width * (bucket + 1) as f64
                                },
                                row_count,
                            },
                        )
                        .collect()
                }
                _ => Vec::new(),
            };

            // Accumulate stats for CBO cache
            col_stats_map.insert(
                field.name().clone(),
                crate::query::planner::ColumnStats {
                    ndv: estimated_ndv,
                    null_count: nc as u64,
                    min_value: min_s,
                    max_value: max_s,
                    numeric_min,
                    numeric_max,
                    histogram,
                    most_common_values,
                },
            );
        }

        // Store stats in CBO cache for cost-based optimization
        let table_stats = crate::query::planner::TableStats {
            schema_version: 0,
            schema_generation: 0,
            data_generation: 0,
            row_count: num_rows as u64,
            columns: col_stats_map,
            collected_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            source_size: crate::query::planner::table_data_size(&storage_path.to_string_lossy()),
        };
        crate::query::planner::store_table_stats(&storage_path.to_string_lossy(), table_stats);

        // Build result as a RecordBatch
        let result_schema = Arc::new(Schema::new(vec![
            Field::new("column_name", arrow::datatypes::DataType::Utf8, false),
            Field::new("column_type", arrow::datatypes::DataType::Utf8, false),
            Field::new("row_count", arrow::datatypes::DataType::Int64, false),
            Field::new("null_count", arrow::datatypes::DataType::Int64, false),
            Field::new("ndv", arrow::datatypes::DataType::Int64, false),
            Field::new("min_value", arrow::datatypes::DataType::Utf8, true),
            Field::new("max_value", arrow::datatypes::DataType::Utf8, true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(
                col_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                col_types.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(row_counts)),
            Arc::new(Int64Array::from(null_counts)),
            Arc::new(Int64Array::from(ndv_vals)),
            Arc::new(StringArray::from(
                min_vals.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                max_vals.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
        ];
        let result_batch = RecordBatch::try_new(result_schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(ApexResult::Data(result_batch))
    }

    pub(in crate::query::executor) fn execute_reindex(
        base_dir: &Path,
        default_table_path: &Path,
        table: &str,
    ) -> io::Result<ApexResult> {
        let table_path = Self::resolve_table_path(table, base_dir, default_table_path);
        if !table_path.exists() {
            return Err(err_not_found(format!("Table '{}' does not exist", table)));
        }

        // REINDEX must rebuild from committed values, including txn append
        // sidecars and DeltaStore cell updates.
        Self::materialize_table_sidecars(&table_path)?;
        Self::invalidate_cache_for_path(&table_path);
        crate::storage::backend::invalidate_global_dict_cache(&table_path);

        let idx_mgr_arc = get_index_manager(base_dir, table);
        let mut idx_mgr = idx_mgr_arc.lock();
        let indexes = idx_mgr
            .list_indexes()
            .iter()
            .map(|m| {
                (
                    m.name.clone(),
                    m.effective_columns()
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();

        if indexes.is_empty() {
            return Ok(ApexResult::Scalar(0));
        }

        // Clear all index data
        idx_mgr.rebuild_all();

        // Re-read table data and rebuild
        let storage = TableStorageBackend::open(&table_path)?;
        let row_count = storage.row_count();
        if row_count > 0 {
            const INDEX_BUILD_BATCH_ROWS: usize = 65_536;
            let mut start_row = 0usize;
            let total_rows = row_count as usize;
            while start_row < total_rows {
                let limit = (total_rows - start_row).min(INDEX_BUILD_BATCH_ROWS);
                let batch = storage.read_columns_to_arrow(None, start_row, Some(limit))?;
                if batch.num_rows() == 0 {
                    break;
                }

                let id_col = batch
                    .column_by_name("_id")
                    .ok_or_else(|| err_data("_id column not found"))?;
                let id_arr = id_col.as_any().downcast_ref::<UInt64Array>();
                let id_arr_i64 = id_col.as_any().downcast_ref::<Int64Array>();

                for row in 0..batch.num_rows() {
                    let row_id: u64 = if let Some(arr) = id_arr {
                        arr.value(row)
                    } else if let Some(arr) = id_arr_i64 {
                        arr.value(row) as u64
                    } else {
                        (start_row + row) as u64
                    };

                    let mut col_vals =
                        std::collections::HashMap::with_capacity(batch.num_columns());
                    for (column_index, field) in batch.schema().fields().iter().enumerate() {
                        if field.name() != "_id" {
                            col_vals.insert(
                                field.name().clone(),
                                Self::arrow_value_at_col(batch.column(column_index), row),
                            );
                        }
                    }
                    idx_mgr.on_insert(row_id, &col_vals)?;
                }

                start_row += batch.num_rows();
            }
        }

        idx_mgr.save()?;
        drop(idx_mgr);
        invalidate_index_cache(base_dir, table);
        Self::invalidate_cache_for_path(&table_path);
        crate::storage::backend::invalidate_global_dict_cache(&table_path);
        Ok(ApexResult::Scalar(indexes.len() as i64))
    }

    pub(in crate::query::executor) fn execute_pragma(
        base_dir: &Path,
        default_table_path: &Path,
        name: &str,
        arg: Option<&str>,
    ) -> io::Result<ApexResult> {
        match name.to_lowercase().as_str() {
            "integrity_check" => {
                let table_name = arg.unwrap_or("default");
                let table_path = Self::resolve_table_path(table_name, base_dir, default_table_path);
                Self::pragma_integrity_check(&table_path, table_name)
            }
            "table_info" => {
                let table_name = arg
                    .ok_or_else(|| err_input("PRAGMA table_info requires a table name argument"))?;
                let table_path = Self::resolve_table_path(table_name, base_dir, default_table_path);
                Self::pragma_table_info(&table_path, table_name)
            }
            "version" => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "version",
                    arrow::datatypes::DataType::Utf8,
                    false,
                )]));
                let arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["ApexBase 1.0"]))];
                let batch = RecordBatch::try_new(schema, arrays)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                Ok(ApexResult::Data(batch))
            }
            "stats" => {
                let table_name = arg.unwrap_or("default");
                let table_path = Self::resolve_table_path(table_name, base_dir, default_table_path);
                let key = table_path.to_string_lossy().to_string();
                if let Some(stats) = crate::query::planner::get_table_stats(&key) {
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("table", arrow::datatypes::DataType::Utf8, false),
                        Field::new("row_count", arrow::datatypes::DataType::Int64, false),
                        Field::new("columns", arrow::datatypes::DataType::Int64, false),
                        Field::new("collected_at", arrow::datatypes::DataType::Int64, false),
                    ]));
                    let arrays: Vec<ArrayRef> = vec![
                        Arc::new(StringArray::from(vec![table_name])),
                        Arc::new(Int64Array::from(vec![stats.row_count as i64])),
                        Arc::new(Int64Array::from(vec![stats.columns.len() as i64])),
                        Arc::new(Int64Array::from(vec![stats.collected_at as i64])),
                    ];
                    let batch = RecordBatch::try_new(schema, arrays)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    Ok(ApexResult::Data(batch))
                } else {
                    Ok(ApexResult::Scalar(0))
                }
            }
            _ => Err(err_input(format!(
                "Unknown PRAGMA: {}. Supported: integrity_check, table_info, version, stats",
                name
            ))),
        }
    }

    pub(in crate::query::executor) fn pragma_integrity_check(table_path: &Path, table_name: &str) -> io::Result<ApexResult> {
        let mut checks: Vec<String> = Vec::new();
        let mut statuses: Vec<String> = Vec::new();

        // Check 1: File exists
        if !table_path.exists() {
            checks.push("file_exists".to_string());
            statuses.push(format!("FAIL: Table '{}' file not found", table_name));
            return Self::integrity_result(&checks, &statuses);
        }
        checks.push("file_exists".to_string());
        statuses.push("ok".to_string());

        // Check 2: File is readable and has valid header
        let file_meta = std::fs::metadata(table_path)?;
        let file_size = file_meta.len();
        if file_size < 128 {
            checks.push("header_valid".to_string());
            statuses.push(format!(
                "FAIL: File too small ({} bytes, minimum 128)",
                file_size
            ));
            return Self::integrity_result(&checks, &statuses);
        }
        checks.push("header_valid".to_string());
        statuses.push("ok".to_string());

        // Check 3: Can open storage
        match TableStorageBackend::open(table_path) {
            Ok(storage) => {
                checks.push("storage_open".to_string());
                statuses.push("ok".to_string());

                // Check 4: Schema readable
                let schema = storage.get_schema();
                checks.push("schema_valid".to_string());
                statuses.push(format!("ok ({} columns)", schema.len()));

                // Check 5: Row count consistent
                let row_count = storage.row_count();
                checks.push("row_count".to_string());
                statuses.push(format!("ok ({} rows)", row_count));

                // Check 6: Can read data
                match storage.read_columns_to_arrow(None, 0, Some(1)) {
                    Ok(batch) => {
                        checks.push("data_readable".to_string());
                        statuses.push(format!("ok ({} columns in batch)", batch.num_columns()));
                    }
                    Err(e) => {
                        checks.push("data_readable".to_string());
                        statuses.push(format!("FAIL: {}", e));
                    }
                }

                // Check 7: WAL file (if exists)
                let wal_path = table_path.with_extension("apex.wal");
                if wal_path.exists() {
                    match crate::storage::incremental::WalReader::open(&wal_path) {
                        Ok(mut reader) => match reader.read_all() {
                            Ok(records) => {
                                checks.push("wal_valid".to_string());
                                statuses.push(format!("ok ({} records)", records.len()));
                            }
                            Err(e) => {
                                checks.push("wal_valid".to_string());
                                statuses.push(format!("WARN: WAL read error: {}", e));
                            }
                        },
                        Err(e) => {
                            checks.push("wal_valid".to_string());
                            statuses.push(format!("WARN: WAL open error: {}", e));
                        }
                    }
                } else {
                    checks.push("wal_valid".to_string());
                    statuses.push("ok (no WAL file)".to_string());
                }

                // Check 8: Index files (if any)
                let (bd, tn) = base_dir_and_table(table_path);
                let idx_mgr_arc = get_index_manager(&bd, &tn);
                let idx_mgr = idx_mgr_arc.lock();
                let idx_list = idx_mgr.list_indexes();
                checks.push("indexes".to_string());
                statuses.push(format!("ok ({} indexes)", idx_list.len()));
            }
            Err(e) => {
                checks.push("storage_open".to_string());
                statuses.push(format!("FAIL: {}", e));
            }
        }

        Self::integrity_result(&checks, &statuses)
    }

    pub(in crate::query::executor) fn integrity_result(checks: &[String], statuses: &[String]) -> io::Result<ApexResult> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("check", arrow::datatypes::DataType::Utf8, false),
            Field::new("status", arrow::datatypes::DataType::Utf8, false),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(
                checks.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                statuses.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
        ];
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(ApexResult::Data(batch))
    }

    pub(in crate::query::executor) fn pragma_table_info(table_path: &Path, table_name: &str) -> io::Result<ApexResult> {
        if !table_path.exists() {
            return Err(err_not_found(format!(
                "Table '{}' does not exist",
                table_name
            )));
        }
        let storage = TableStorageBackend::open(table_path)?;
        let schema = storage.get_schema();

        let mut cids: Vec<i64> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut types: Vec<String> = Vec::new();

        for (idx, (col_name, col_type)) in schema.iter().enumerate() {
            cids.push(idx as i64);
            names.push(col_name.clone());
            types.push(format!("{:?}", col_type));
        }

        let result_schema = Arc::new(Schema::new(vec![
            Field::new("cid", arrow::datatypes::DataType::Int64, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
            Field::new("type", arrow::datatypes::DataType::Utf8, false),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(cids)),
            Arc::new(StringArray::from(
                names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                types.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
        ];
        let batch = RecordBatch::try_new(result_schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(ApexResult::Data(batch))
    }

    // =====================================================================
    // Data Import Table Functions: read_csv / read_json / read_parquet
    // =====================================================================

    const IMPORT_BATCH_ROWS: usize = 65_536;
}
