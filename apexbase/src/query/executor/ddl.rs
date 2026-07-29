// DDL execution: CREATE TABLE, DROP TABLE, ALTER TABLE, TRUNCATE, EXPLAIN, CTE

impl ApexExecutor {
    pub(super) fn execute_show_tables(base_dir: &Path) -> io::Result<ApexResult> {
        let mut tables = Vec::new();
        if base_dir.exists() {
            for entry in std::fs::read_dir(base_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().is_some_and(|ext| ext == "apex") {
                    if let Some(name) = path.file_stem().and_then(|name| name.to_str()) {
                        tables.push(name.to_string());
                    }
                }
            }
        }
        tables.sort_unstable();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_name",
            ArrowDataType::Utf8,
            false,
        )]));
        let values: ArrayRef = Arc::new(StringArray::from(tables));
        Ok(ApexResult::Data(
            RecordBatch::try_new(schema, vec![values])
                .map_err(|error| err_data(error.to_string()))?,
        ))
    }

    pub(super) fn execute_show_databases(base_dir: &Path) -> io::Result<ApexResult> {
        let mut databases = vec!["main".to_string()];
        if base_dir.exists() {
            for entry in std::fs::read_dir(base_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir()
                    && std::fs::read_dir(&path)?
                        .filter_map(Result::ok)
                        .any(|child| {
                            child.path().extension().is_some_and(|ext| ext == "apex")
                                || child.file_name() == "fts_config.json"
                        })
                {
                    if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                        databases.push(name.to_string());
                    }
                }
            }
        }
        databases.sort_unstable();
        databases.dedup();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "database_name",
            ArrowDataType::Utf8,
            false,
        )]));
        let values: ArrayRef = Arc::new(StringArray::from(databases));
        Ok(ApexResult::Data(
            RecordBatch::try_new(schema, vec![values])
                .map_err(|error| err_data(error.to_string()))?,
        ))
    }

    pub(super) fn execute_describe_table(
        base_dir: &Path,
        default_table_path: &Path,
        table: &str,
    ) -> io::Result<ApexResult> {
        let table_path = Self::resolve_table_path(table, base_dir, default_table_path);
        if !table_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' does not exist", table),
            ));
        }
        let backend = TableStorageBackend::open(&table_path)?;
        let schema_entries = backend.get_schema();
        let names: Vec<String> = schema_entries
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let types: Vec<String> = schema_entries
            .iter()
            .map(|(_, dtype)| format!("{:?}", dtype))
            .collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("column_name", ArrowDataType::Utf8, false),
            Field::new("data_type", ArrowDataType::Utf8, false),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
        ];
        Ok(ApexResult::Data(
            RecordBatch::try_new(schema, arrays)
                .map_err(|error| err_data(error.to_string()))?,
        ))
    }

    // ========== DDL Execution Methods ==========

    fn delta_path_for_table(table_path: &Path) -> PathBuf {
        let mut delta = table_path.to_path_buf();
        let name = delta.file_name().unwrap_or_default().to_string_lossy();
        delta.set_file_name(format!("{}.delta", name));
        delta
    }

    fn delta_meta_path_for_delta(delta_path: &Path) -> PathBuf {
        let mut meta = delta_path.to_path_buf();
        let name = meta.file_name().unwrap_or_default().to_string_lossy();
        meta.set_file_name(format!("{}.meta", name));
        meta
    }

    fn deltastore_path_for_table(table_path: &Path) -> PathBuf {
        let mut path = table_path.to_path_buf();
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        path.set_file_name(format!("{}.deltastore", name));
        path
    }

    fn remove_table_sidecars(table_path: &Path) {
        let delta_path = Self::delta_path_for_table(table_path);
        let deltastore_path = Self::deltastore_path_for_table(table_path);
        let mut deltastore_tmp = table_path.to_path_buf();
        let name = deltastore_tmp
            .file_name()
            .unwrap_or_default()
            .to_string_lossy();
        deltastore_tmp.set_file_name(format!("{}.deltastore.tmp", name));

        let _ = std::fs::remove_file(Self::delta_meta_path_for_delta(&delta_path));
        let _ = std::fs::remove_file(&delta_path);
        let _ = std::fs::remove_file(&deltastore_path);
        let _ = std::fs::remove_file(&deltastore_tmp);
    }

    fn materialize_table_sidecars(table_path: &Path) -> io::Result<()> {
        let delta_path = Self::delta_path_for_table(table_path);
        if delta_path.exists() {
            let storage = TableStorageBackend::open_for_compact(table_path)?;
            storage.compact()?;
            invalidate_storage_cache(table_path);
            crate::storage::engine::engine().invalidate(table_path);
        }

        let deltastore_path = Self::deltastore_path_for_table(table_path);
        if deltastore_path.exists() {
            let storage = TableStorageBackend::open_for_write(table_path)?;
            if storage.has_pending_deltas() {
                storage.save_full()?;
                invalidate_storage_cache(table_path);
                crate::storage::engine::engine().invalidate(table_path);
            }
        }

        Ok(())
    }

    /// Execute CREATE TABLE statement
    /// High-performance: O(1) - just creates file header
    fn execute_create_table(
        table_path: &Path,
        table: &str,
        columns: &[crate::query::sql_parser::ColumnDef],
        if_not_exists: bool,
    ) -> io::Result<ApexResult> {
        // Ensure parent directory exists (needed for named databases)
        if let Some(parent) = table_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if table_path.exists() {
            if if_not_exists {
                // Return success without error
                return Ok(ApexResult::Scalar(0));
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("Table '{}' already exists", table),
                ));
            }
        }
        let epoch_write = crate::storage::epoch::logical_write(table_path);

        // Create empty storage file with schema
        TableStorageBackend::create(&table_path)?;
        let storage = TableStorageBackend::open_for_write(&table_path)?;

        // Add columns to schema (if provided)
        for col_def in columns {
            storage.add_column(&col_def.name, col_def.data_type.clone())?;
        }

        // Set constraints on the underlying schema
        {
            use crate::query::sql_parser::{ColumnConstraintKind, DefaultValueFunction};
            use crate::storage::on_demand::ColumnConstraints;
            for col_def in columns {
                if !col_def.constraints.is_empty() {
                    let default_val = col_def.constraints.iter().find_map(|c| {
                        use crate::storage::on_demand::DefaultValue;
                        match c {
                            ColumnConstraintKind::Default(v) => Some(match v {
                                Value::Int64(n) => DefaultValue::Int64(*n),
                                Value::Float64(f) => DefaultValue::Float64(*f),
                                Value::String(s) => DefaultValue::String(s.clone()),
                                Value::Bool(b) => DefaultValue::Bool(*b),
                                Value::Date(d) => DefaultValue::Date(*d),
                                Value::Timestamp(t) => DefaultValue::Timestamp(*t),
                                _ => DefaultValue::Null,
                            }),
                            ColumnConstraintKind::DefaultFunction(func) => Some(match func {
                                DefaultValueFunction::CurrentDate => DefaultValue::CurrentDate,
                                DefaultValueFunction::CurrentTimestamp => {
                                    DefaultValue::CurrentTimestamp
                                }
                                DefaultValueFunction::UnixTimestamp => DefaultValue::UnixTimestamp,
                            }),
                            _ => None,
                        }
                    });
                    let check_sql = col_def.constraints.iter().find_map(|c| {
                        if let ColumnConstraintKind::Check(sql) = c {
                            Some(sql.clone())
                        } else {
                            None
                        }
                    });
                    let fk = col_def.constraints.iter().find_map(|c| {
                        if let ColumnConstraintKind::ForeignKey {
                            ref_table,
                            ref_column,
                        } = c
                        {
                            Some((ref_table.clone(), ref_column.clone()))
                        } else {
                            None
                        }
                    });
                    storage.storage.set_column_constraints(
                        &col_def.name,
                        ColumnConstraints {
                            not_null: col_def.constraints.contains(&ColumnConstraintKind::NotNull),
                            primary_key: col_def
                                .constraints
                                .contains(&ColumnConstraintKind::PrimaryKey),
                            unique: col_def.constraints.contains(&ColumnConstraintKind::Unique),
                            default_value: default_val,
                            check_expr_sql: check_sql,
                            foreign_key: fk,
                            autoincrement: col_def
                                .constraints
                                .contains(&ColumnConstraintKind::Autoincrement),
                        },
                    );
                }
            }
        }

        storage.save()?;
        epoch_write.commit();

        Ok(ApexResult::Scalar(0))
    }

    /// Execute DROP TABLE statement
    /// High-performance: O(1) - just deletes file
    fn execute_drop_table(
        table_path: &Path,
        table: &str,
        if_exists: bool,
    ) -> io::Result<ApexResult> {
        // Invalidate ALL caches to release file handles and mmaps
        invalidate_storage_cache(table_path);
        crate::storage::engine::engine().invalidate(table_path);

        if !table_path.exists() {
            if if_exists {
                return Ok(ApexResult::Scalar(0));
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table),
                ));
            }
        }
        let epoch_write = crate::storage::epoch::logical_write(table_path);

        std::fs::remove_file(table_path)?;
        invalidate_table_schema_stats(&table_path.to_string_lossy());

        // Clean up associated files (WAL, delta, deltastore) in same directory as table
        let parent_dir = table_path.parent().unwrap_or(table_path);
        let file_stem = table_path.file_name().unwrap_or_default().to_string_lossy();
        let cleanup_extensions = [
            format!("{}.wal", file_stem),
            format!("{}.delta", file_stem),
            format!("{}.deltastore", file_stem),
        ];
        for name in &cleanup_extensions {
            let path = parent_dir.join(name);
            if path.exists() {
                let _ = std::fs::remove_file(&path);
            }
        }
        epoch_write.commit();

        Ok(ApexResult::Scalar(0))
    }

    /// Execute ALTER TABLE statement
    fn execute_alter_table(
        table_path: &Path,
        table: &str,
        operation: &crate::query::sql_parser::AlterTableOp,
    ) -> io::Result<ApexResult> {
        use crate::query::sql_parser::AlterTableOp;

        if !table_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' does not exist", table),
            ));
        }
        let epoch_write = crate::storage::epoch::logical_write(table_path);

        // Invalidate all caches before write (executor + StorageEngine)
        invalidate_storage_cache(&table_path);
        crate::storage::engine::engine().invalidate(&table_path);

        // Schema rewrites must see all committed append-only rows and cell deltas.
        // Keep SELECT fast by avoiding auto-compact there; pay this cost only for DDL.
        Self::materialize_table_sidecars(table_path)?;

        // Note: ALTER TABLE operations need to preserve existing data, so we use open_for_write
        // which loads all column data. For true schema-only operations (like TRUNCATE),
        // we can use open_for_schema_change which only loads metadata.
        let storage = TableStorageBackend::open_for_write(&table_path)?;

        match operation {
            AlterTableOp::AddColumn { name, data_type } => {
                storage.add_column(name, data_type.clone())?;
            }
            AlterTableOp::DropColumn { name } => {
                storage.drop_column(name)?;
            }
            AlterTableOp::RenameColumn { old_name, new_name } => {
                storage.rename_column(old_name, new_name)?;
            }
        }

        storage.save()?;

        // Invalidate all caches after write to ensure subsequent reads get fresh data
        invalidate_storage_cache(&table_path);
        invalidate_table_schema_stats(&table_path.to_string_lossy());
        crate::storage::engine::engine().invalidate(&table_path);
        epoch_write.commit();

        Ok(ApexResult::Scalar(0))
    }

    /// Execute TRUNCATE TABLE statement
    /// High-performance: recreates empty file
    fn execute_truncate(storage_path: &Path) -> io::Result<ApexResult> {
        if !storage_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Table does not exist",
            ));
        }
        let epoch_write = crate::storage::epoch::logical_write(storage_path);

        // Invalidate caches before write
        invalidate_storage_cache(storage_path);
        // On Windows, engine insert_cache may hold mmaps that block file truncate (OS error 1224)
        crate::storage::engine::engine().invalidate(storage_path);
        Self::remove_table_sidecars(storage_path);

        // OPTIMIZATION: Use open_for_schema_change - only loads metadata, NOT column data
        let old_storage = TableStorageBackend::open_for_schema_change(storage_path)?;
        let schema = old_storage.get_schema();
        drop(old_storage);

        // Recreate empty file with same schema
        TableStorageBackend::create(storage_path)?;
        // Use open_for_schema_change for adding columns (schema only)
        let storage = TableStorageBackend::open_for_schema_change(storage_path)?;
        for (name, dtype) in &schema {
            storage.add_column(name, dtype.clone())?;
        }
        storage.save()?;
        Self::remove_table_sidecars(storage_path);

        // Invalidate cache after write to ensure subsequent reads get fresh data
        invalidate_storage_cache(storage_path);
        invalidate_table_stats(&storage_path.to_string_lossy());
        crate::storage::engine::engine().invalidate(storage_path);
        epoch_write.commit();

        Ok(ApexResult::Scalar(0))
    }

    // ========== EXPLAIN / CTE / INSERT SELECT ==========

    /// Execute EXPLAIN statement — returns a text description of the query plan
    fn execute_explain(
        stmt: SqlStatement,
        analyze: bool,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<ApexResult> {
        let mut plan_lines: Vec<String> = Vec::new();

        // Describe the statement type
        match &stmt {
            SqlStatement::Select(select) => {
                plan_lines.push("Query Plan:".to_string());
                // FROM
                if let Some(ref from) = select.from {
                    match from {
                        FromItem::Table { table, alias } => {
                            let alias_str = alias
                                .as_ref()
                                .map(|a| format!(" AS {}", a))
                                .unwrap_or_default();
                            plan_lines.push(format!("  Scan: table={}{}", table, alias_str));
                            let table_path =
                                Self::resolve_table_path(table, base_dir, default_table_path);
                            if table_path.exists() {
                                if let Ok(backend) = get_cached_backend(&table_path) {
                                    let row_count = backend.row_count();
                                    let schema = backend.get_schema();
                                    plan_lines.push(format!("    Rows: ~{}", row_count));
                                    plan_lines.push(format!("    Columns: {}", schema.len()));
                                }
                            }
                        }
                        FromItem::Subquery { alias, .. } => {
                            plan_lines.push(format!("  Scan: subquery AS {}", alias));
                        }
                        FromItem::TableFunction {
                            func, file, alias, ..
                        } => {
                            let alias_str = alias
                                .as_ref()
                                .map(|a| format!(" AS {}", a))
                                .unwrap_or_default();
                            plan_lines.push(format!("  Scan: {}('{}'){}", func, file, alias_str));
                        }
                        FromItem::TopkDistance {
                            col,
                            k,
                            metric,
                            alias,
                            ..
                        } => {
                            let alias_str = alias
                                .as_ref()
                                .map(|a| format!(" AS {}", a))
                                .unwrap_or_default();
                            plan_lines.push(format!(
                                "  Scan: topk_distance({}, k={}, metric='{}'){}",
                                col, k, metric, alias_str
                            ));
                        }
                        FromItem::DirectFile { file, alias } => {
                            let alias_str = alias
                                .as_ref()
                                .map(|a| format!(" AS {}", a))
                                .unwrap_or_default();
                            plan_lines.push(format!("  DirectFile: '{}'{}", file, alias_str));
                        }
                        FromItem::LateralExplode { .. }
                        | FromItem::LateralPosExplode { .. }
                        | FromItem::LateralStack { .. } => {
                            plan_lines.push("  Scan: lateral view".to_string());
                        }
                    }
                }

                // Show the physical decision, not only the SQL-shaped
                // operators above.  This makes EXPLAIN useful for verifying
                // CBO decisions and whether statistics were available.
                if let Some(FromItem::Table { .. }) = select.from.as_ref() {
                    let table_path =
                        Self::resolve_from_table_path(select, base_dir, default_table_path);
                    if let Ok(backend) = get_cached_backend(&table_path) {
                        let (table_base, table_name) = base_dir_and_table(&table_path);
                        let index_manager = get_index_manager(&table_base, &table_name);
                        let index_guard = index_manager.lock();
                        let plan = crate::query::planner::QueryPlanner::plan_select_details(
                            select,
                            Some(&*index_guard),
                            &table_path.to_string_lossy(),
                            Self::planner_context(&backend, select.where_clause.as_ref()),
                        );
                        plan_lines.push(format!(
                            "  Chosen Plan: {:?} (estimated_cost={:.3}, estimated_rows={:.1}, stats={})",
                            plan.strategy,
                            plan.cost.total,
                            plan.cost.output_rows,
                            if plan.stats_available { "available" } else { "default" }
                        ));
                        plan_lines.push(format!(
                            "    Planning Time: {:.3}ms",
                            plan.planning_time_micros as f64 / 1000.0
                        ));
                        if plan.feedback_applied {
                            plan_lines.push("    Feedback: applied".to_string());
                        }
                        for candidate in &plan.candidates {
                            plan_lines.push(format!(
                                "    Candidate: {} cost={:.3} rows={:.1}{}",
                                candidate.name,
                                candidate.cost.total,
                                candidate.cost.output_rows,
                                if candidate.strategy == plan.strategy {
                                    " [chosen]"
                                } else {
                                    ""
                                }
                            ));
                        }
                    }
                }
                // JOINs
                for join in &select.joins {
                    let jt = match join.join_type {
                        JoinType::Inner => "INNER JOIN",
                        JoinType::Left => "LEFT JOIN",
                        JoinType::Right => "RIGHT JOIN",
                        JoinType::Full => "FULL OUTER JOIN",
                        JoinType::Cross => "CROSS JOIN",
                        JoinType::Semi => "LEFT SEMI JOIN",
                        JoinType::Anti => "LEFT ANTI JOIN",
                    };
                    let table_name = match &join.right {
                        FromItem::Table { table, .. } => table.clone(),
                        FromItem::Subquery { alias, .. } => format!("(subquery) {}", alias),
                        FromItem::TableFunction { func, file, .. } => {
                            format!("{}('{}')", func, file)
                        }
                        FromItem::TopkDistance { col, k, metric, .. } => {
                            format!("topk_distance({}, k={}, metric='{}')", col, k, metric)
                        }
                        FromItem::DirectFile { file, .. } => format!("'{}'", file),
                        FromItem::LateralExplode { table_alias, .. } => {
                            format!("LATERAL VIEW EXPLODE AS {}", table_alias)
                        }
                        FromItem::LateralPosExplode { table_alias, .. } => {
                            format!("LATERAL VIEW POSEXPLODE AS {}", table_alias)
                        }
                        FromItem::LateralStack { table_alias, .. } => {
                            format!("LATERAL VIEW STACK AS {}", table_alias)
                        }
                    };
                    plan_lines.push(format!("  {}: {}", jt, table_name));
                }
                // WHERE
                if select.where_clause.is_some() {
                    plan_lines.push("  Filter: WHERE <predicate>".to_string());
                }
                // GROUP BY
                if !select.group_by.is_empty() {
                    plan_lines.push(format!("  GroupBy: {}", select.group_by.join(", ")));
                }
                // HAVING
                if select.having.is_some() {
                    plan_lines.push("  Filter: HAVING <predicate>".to_string());
                }
                // ORDER BY
                if !select.order_by.is_empty() {
                    let obs: Vec<String> = select
                        .order_by
                        .iter()
                        .map(|o| {
                            format!("{} {}", o.column, if o.descending { "DESC" } else { "ASC" })
                        })
                        .collect();
                    plan_lines.push(format!("  Sort: {}", obs.join(", ")));
                }
                // LIMIT/OFFSET
                if let Some(limit) = select.limit {
                    plan_lines.push(format!("  Limit: {}", limit));
                }
                if let Some(offset) = select.offset {
                    plan_lines.push(format!("  Offset: {}", offset));
                }
                // Projection
                let proj: Vec<String> = select
                    .columns
                    .iter()
                    .map(|c| match c {
                        SelectColumn::All => "*".to_string(),
                        SelectColumn::AllExclude(exclude) => {
                            format!("* EXCLUDE ({})", exclude.join(", "))
                        }
                        SelectColumn::AllReplace(reps) => {
                            let parts: Vec<String> =
                                reps.iter().map(|(_, col)| col.clone()).collect();
                            format!("* REPLACE ({})", parts.join(", "))
                        }
                        SelectColumn::Columns(pattern) => format!("COLUMNS('{}')", pattern),
                        SelectColumn::Column(name) => name.clone(),
                        SelectColumn::ColumnAlias { column, alias } => {
                            format!("{} AS {}", column, alias)
                        }
                        SelectColumn::Aggregate { func, column, .. } => {
                            format!("{}({})", func, column.as_deref().unwrap_or("*"))
                        }
                        SelectColumn::Expression { alias, .. } => {
                            alias.as_deref().unwrap_or("<expr>").to_string()
                        }
                        SelectColumn::WindowFunction { name, alias, .. } => {
                            alias.as_deref().unwrap_or(name.as_str()).to_string()
                        }
                    })
                    .collect();
                plan_lines.push(format!("  Output: {}", proj.join(", ")));
            }
            SqlStatement::Insert { table, values, .. } => {
                plan_lines.push("Query Plan:".to_string());
                plan_lines.push(format!("  Insert: table={}, rows={}", table, values.len()));
            }
            SqlStatement::InsertSelect { table, .. } => {
                plan_lines.push("Query Plan:".to_string());
                plan_lines.push(format!("  InsertSelect: table={}", table));
            }
            SqlStatement::CreateTableAs { table, .. } => {
                plan_lines.push("Query Plan:".to_string());
                plan_lines.push(format!("  CreateTableAs: table={}", table));
            }
            SqlStatement::Update { table, .. } => {
                plan_lines.push("Query Plan:".to_string());
                plan_lines.push(format!("  Update: table={}", table));
            }
            SqlStatement::Delete { table, .. } => {
                plan_lines.push("Query Plan:".to_string());
                plan_lines.push(format!("  Delete: table={}", table));
            }
            SqlStatement::Union(_) => {
                plan_lines.push("Query Plan:".to_string());
                plan_lines.push("  Union".to_string());
            }
            SqlStatement::Cte { name, .. } => {
                plan_lines.push("Query Plan:".to_string());
                plan_lines.push(format!("  CTE: {}", name));
            }
            other => {
                plan_lines.push(format!("Query Plan: {:?}", std::mem::discriminant(other)));
            }
        }

        // EXPLAIN ANALYZE: actually run the query and report timing
        if analyze {
            let start = std::time::Instant::now();
            let result = Self::execute_parsed_multi(stmt.clone(), base_dir, default_table_path)?;
            let elapsed = start.elapsed();
            plan_lines.push(format!(
                "  Execution Time: {:.3}ms",
                elapsed.as_secs_f64() * 1000.0
            ));
            plan_lines.push(format!(
                "  Actual Time: {:.3}ms",
                elapsed.as_secs_f64() * 1000.0
            ));
            if let Ok(batch) = result.to_record_batch() {
                plan_lines.push(format!("  Actual Rows: {}", batch.num_rows()));

                if let SqlStatement::Select(select) = &stmt {
                    let table_path =
                        Self::resolve_from_table_path(select, base_dir, default_table_path);
                    if table_path.exists() {
                        if let Ok(backend) = get_cached_backend(&table_path) {
                            let (table_base, table_name) = base_dir_and_table(&table_path);
                            let index_manager = get_index_manager(&table_base, &table_name);
                            let index_guard = index_manager.lock();
                            let plan = crate::query::planner::QueryPlanner::plan_select_details(
                                select,
                                Some(&*index_guard),
                                &table_path.to_string_lossy(),
                                Self::planner_context(&backend, select.where_clause.as_ref()),
                            );
                            crate::query::planner::record_plan_feedback(
                                &table_path.to_string_lossy(),
                                select,
                                &plan.strategy,
                                plan.cost.output_rows,
                                batch.num_rows() as f64,
                            );
                            plan_lines.push("  Feedback Recorded: yes".to_string());
                        }
                    }
                }
            }
        }

        // Return as a single-column RecordBatch with plan lines
        let plan_text = plan_lines.join("\n");
        let arr = arrow::array::StringArray::from(vec![plan_text.as_str()]);
        let schema = Schema::new(vec![Field::new("plan", ArrowDataType::Utf8, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arr)])
            .map_err(|e| err_data(e.to_string()))?;
        Ok(ApexResult::Data(batch))
    }

    /// Execute CTE (WITH name AS (...) main_query)
    /// Materializes the CTE body into a temp table and rewrites the main query to reference it.
    /// For recursive CTEs, implements iterative fixpoint: anchor UNION ALL recursive until no new rows.
    fn execute_cte(
        name: &str,
        column_aliases: &[String],
        body: SqlStatement,
        mut main: SqlStatement,
        recursive: bool,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<ApexResult> {
        let temp_path = base_dir.join(format!(
            "__cte_{}_{}_{}.apex",
            name,
            std::process::id(),
            CTE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));

        // Helper: materialize a RecordBatch into a temp table (create or append)
        let materialize_batch =
            |batch: &RecordBatch, path: &Path, create: bool| -> io::Result<()> {
                if create {
                    let backend = TableStorageBackend::create(path)?;
                    let schema = batch.schema();
                    for field in schema.fields() {
                        let col_type = match field.data_type() {
                            ArrowDataType::Int64 | ArrowDataType::UInt64 => {
                                crate::data::DataType::Int64
                            }
                            ArrowDataType::Float64 => crate::data::DataType::Float64,
                            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
                                crate::data::DataType::String
                            }
                            ArrowDataType::Boolean => crate::data::DataType::Bool,
                            _ => crate::data::DataType::String,
                        };
                        backend.add_column(field.name(), col_type)?;
                    }
                    if batch.num_rows() > 0 {
                        Self::insert_batch_into_backend(&backend, batch)?;
                    }
                    backend.save()?;
                } else if batch.num_rows() > 0 {
                    let backend = TableStorageBackend::open_for_write(path)?;
                    Self::insert_batch_into_backend(&backend, batch)?;
                    backend.save()?;
                }
                invalidate_storage_cache(path);
                Ok(())
            };

        if recursive {
            // Recursive CTE: body must be UNION ALL with anchor (left) and recursive part (right)
            let (anchor_stmt, recursive_stmt, _union_all) = match body {
                SqlStatement::Union(ref u) => ((*u.left).clone(), (*u.right).clone(), u.all),
                _ => {
                    // Not a UNION — treat as non-recursive fallback
                    let cte_result =
                        Self::execute_parsed_multi(body, base_dir, default_table_path)?;
                    let cte_batch = cte_result.to_record_batch().map_err(|e| {
                        err_data(format!("CTE body must return a result set: {}", e))
                    })?;
                    materialize_batch(&cte_batch, &temp_path, true)?;

                    let result = Self::execute_main_with_cte(
                        name,
                        main,
                        base_dir,
                        default_table_path,
                        &temp_path,
                    )?;
                    Self::cleanup_temp_table(&temp_path);
                    return result;
                }
            };

            // Step 1: Execute anchor query
            let anchor_result =
                Self::execute_parsed_multi(anchor_stmt, base_dir, default_table_path)?;
            let mut anchor_batch = anchor_result.to_record_batch().map_err(|e| {
                err_data(format!(
                    "Recursive CTE anchor must return a result set: {}",
                    e
                ))
            })?;

            // Apply column aliases if provided: WITH RECURSIVE fact(n, val) AS (...)
            if !column_aliases.is_empty() {
                anchor_batch = Self::remap_batch_columns(&anchor_batch, column_aliases)?;
            }

            if anchor_batch.num_rows() == 0 {
                // Empty anchor → materialize empty table, run main
                materialize_batch(&anchor_batch, &temp_path, true)?;
                let result = Self::execute_main_with_cte(
                    name,
                    main,
                    base_dir,
                    default_table_path,
                    &temp_path,
                )?;
                Self::cleanup_temp_table(&temp_path);
                return result;
            }

            // Record anchor column names (defines CTE schema — uses aliases if provided)
            let anchor_col_names: Vec<String> = anchor_batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();

            // Materialize anchor into temp table
            materialize_batch(&anchor_batch, &temp_path, true)?;

            // Step 2: Iterative fixpoint — execute recursive part until no new rows
            // Use a working table to hold last iteration's new rows (same schema as anchor)
            let working_path =
                base_dir.join(format!("__cte_{}_work_{}.apex", name, std::process::id()));
            materialize_batch(&anchor_batch, &working_path, true)?;

            const MAX_ITERATIONS: usize = 1000;
            for _iter in 0..MAX_ITERATIONS {
                // Execute recursive part with CTE name → working table
                let recursive_result = Self::execute_main_with_cte(
                    name,
                    recursive_stmt.clone(),
                    base_dir,
                    default_table_path,
                    &working_path,
                )?;
                let new_batch = match recursive_result {
                    Ok(r) => r.to_record_batch().ok(),
                    Err(_) => None,
                };

                let new_batch = match new_batch {
                    Some(b) if b.num_rows() > 0 => b,
                    _ => break, // No new rows — fixpoint reached
                };

                // Remap recursive result columns to anchor column names (by position)
                let remapped = Self::remap_batch_columns(&new_batch, &anchor_col_names)?;

                // Append remapped rows to the main CTE temp table
                materialize_batch(&remapped, &temp_path, false)?;

                // Replace working table with remapped rows for next iteration
                Self::cleanup_temp_table(&working_path);
                materialize_batch(&remapped, &working_path, true)?;
            }

            // Clean up working table
            Self::cleanup_temp_table(&working_path);

            // Step 3: Execute main query against accumulated CTE table
            let result =
                Self::execute_main_with_cte(name, main, base_dir, default_table_path, &temp_path)?;
            Self::cleanup_temp_table(&temp_path);
            result
        } else {
            let references = Self::visit_cte_references_in_statement(&mut main, name, None, None);
            if references == 0 {
                return Self::execute_parsed_multi(main, base_dir, default_table_path);
            }
            // A single-use CTE does not need a row-wise round trip through a
            // temporary Apex file. Keep shared CTEs materialized so repeated
            // references still execute the body only once.
            if column_aliases.is_empty()
                && matches!(&body, SqlStatement::Select(_) | SqlStatement::Union(_))
                && references == 1
            {
                let body = Self::prune_cte_body_for_references(name, body, &main);
                Self::visit_cte_references_in_statement(&mut main, name, Some(&body), None);
                return Self::execute_parsed_multi(main, base_dir, default_table_path);
            }

            // Shared non-recursive CTE: retain one immutable Arrow batch and let
            // every consumer clone its buffers at zero copy.
            let body = if column_aliases.is_empty() {
                Self::prune_cte_body_for_references(name, body, &main)
            } else {
                body
            };
            let cte_result = Self::execute_parsed_multi(body, base_dir, default_table_path)?;
            let mut cte_batch = cte_result
                .to_record_batch()
                .map_err(|e| err_data(format!("CTE body must return a result set: {}", e)))?;
            if !column_aliases.is_empty() {
                cte_batch = Self::remap_batch_columns(&cte_batch, column_aliases)?;
            }
            CTE_BATCH_CACHE.insert(temp_path.clone(), cte_batch);
            let result =
                Self::execute_main_with_cte(name, main, base_dir, default_table_path, &temp_path)?;
            CTE_BATCH_CACHE.remove(&temp_path);
            result
        }
    }

    fn prune_cte_body_for_references(
        cte_name: &str,
        body: SqlStatement,
        main: &SqlStatement,
    ) -> SqlStatement {
        let Some(needed) = Self::collect_cte_referenced_columns(main, cte_name) else {
            return body;
        };
        if needed.is_empty() {
            return body;
        }

        match body {
            SqlStatement::Select(mut select) => {
                if select.distinct || select.distinct_on.is_some() {
                    return SqlStatement::Select(select);
                }
                if select.columns.iter().any(|column| {
                    matches!(
                        column,
                        SelectColumn::All
                            | SelectColumn::AllExclude(_)
                            | SelectColumn::AllReplace(_)
                            | SelectColumn::Columns(_)
                    )
                }) {
                    return SqlStatement::Select(select);
                }

                let original_len = select.columns.len();
                let pruned_columns: Vec<SelectColumn> = select
                    .columns
                    .iter()
                    .filter(|column| {
                        Self::select_column_output_name(column)
                            .map(|name| needed.contains(&name.to_ascii_lowercase()))
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect();
                if pruned_columns.is_empty() || pruned_columns.len() == original_len {
                    return SqlStatement::Select(select);
                }
                select.columns = pruned_columns;
                SqlStatement::Select(select)
            }
            other => other,
        }
    }

    fn select_column_output_name(column: &SelectColumn) -> Option<String> {
        match column {
            SelectColumn::Column(name) => Some(Self::unqualified_name(name).to_string()),
            SelectColumn::ColumnAlias { alias, .. } => Some(alias.trim_matches('"').to_string()),
            SelectColumn::Aggregate {
                func,
                column,
                alias,
                ..
            } => alias.clone().or_else(|| {
                let inner = column.as_deref().unwrap_or("*");
                Some(format!("{}({})", func, inner))
            }),
            SelectColumn::Expression { alias, .. } => alias.clone(),
            SelectColumn::WindowFunction { alias, .. } => alias.clone(),
            _ => None,
        }
    }

    fn unqualified_name(name: &str) -> &str {
        let trimmed = name.trim_matches('"');
        trimmed
            .rsplit('.')
            .next()
            .unwrap_or(trimmed)
            .trim_matches('"')
    }

    fn collect_cte_referenced_columns(
        stmt: &SqlStatement,
        cte_name: &str,
    ) -> Option<HashSet<String>> {
        let mut aliases = HashSet::new();
        Self::collect_cte_aliases_in_statement(stmt, cte_name, &mut aliases);
        if aliases.is_empty() {
            return Some(HashSet::new());
        }

        let mut columns = HashSet::new();
        let mut requires_all = false;
        Self::collect_cte_columns_in_statement(stmt, &aliases, &mut columns, &mut requires_all);
        if requires_all {
            None
        } else {
            Some(columns)
        }
    }

    fn collect_cte_aliases_in_statement(
        stmt: &SqlStatement,
        cte_name: &str,
        aliases: &mut HashSet<String>,
    ) {
        match stmt {
            SqlStatement::Select(select) => {
                Self::collect_cte_aliases_in_select(select, cte_name, aliases)
            }
            SqlStatement::Union(union) => {
                Self::collect_cte_aliases_in_statement(&union.left, cte_name, aliases);
                Self::collect_cte_aliases_in_statement(&union.right, cte_name, aliases);
            }
            SqlStatement::Cte {
                name, body, main, ..
            } => {
                if !name.eq_ignore_ascii_case(cte_name) {
                    Self::collect_cte_aliases_in_statement(body, cte_name, aliases);
                    Self::collect_cte_aliases_in_statement(main, cte_name, aliases);
                }
            }
            SqlStatement::InsertSelect { query, .. }
            | SqlStatement::InsertOverwrite { query, .. }
            | SqlStatement::CreateTableAs { query, .. }
            | SqlStatement::Explain { stmt: query, .. } => {
                Self::collect_cte_aliases_in_statement(query, cte_name, aliases);
            }
            SqlStatement::HiveMultiInsert { inserts } => {
                for insert in inserts {
                    Self::collect_cte_aliases_in_statement(insert, cte_name, aliases);
                }
            }
            SqlStatement::CreateView { stmt, .. } => {
                Self::collect_cte_aliases_in_select(stmt, cte_name, aliases);
            }
            _ => {}
        }
    }

    fn collect_cte_aliases_in_select(
        select: &SelectStatement,
        cte_name: &str,
        aliases: &mut HashSet<String>,
    ) {
        if let Some(from) = &select.from {
            Self::collect_cte_aliases_in_from(from, cte_name, aliases);
        }
        for join in &select.joins {
            Self::collect_cte_aliases_in_from(&join.right, cte_name, aliases);
        }
    }

    fn collect_cte_aliases_in_from(from: &FromItem, cte_name: &str, aliases: &mut HashSet<String>) {
        match from {
            FromItem::Table { table, alias } if table.eq_ignore_ascii_case(cte_name) => {
                aliases.insert(
                    alias
                        .as_deref()
                        .unwrap_or(table)
                        .trim_matches('"')
                        .to_ascii_lowercase(),
                );
                aliases.insert(cte_name.to_ascii_lowercase());
            }
            FromItem::Subquery { stmt, .. } => {
                Self::collect_cte_aliases_in_statement(stmt, cte_name, aliases);
            }
            _ => {}
        }
    }

    fn collect_cte_columns_in_statement(
        stmt: &SqlStatement,
        aliases: &HashSet<String>,
        columns: &mut HashSet<String>,
        requires_all: &mut bool,
    ) {
        match stmt {
            SqlStatement::Select(select) => {
                Self::collect_cte_columns_in_select(select, aliases, columns, requires_all)
            }
            SqlStatement::Union(union) => {
                Self::collect_cte_columns_in_statement(&union.left, aliases, columns, requires_all);
                Self::collect_cte_columns_in_statement(
                    &union.right,
                    aliases,
                    columns,
                    requires_all,
                );
            }
            SqlStatement::Cte { body, main, .. } => {
                Self::collect_cte_columns_in_statement(body, aliases, columns, requires_all);
                Self::collect_cte_columns_in_statement(main, aliases, columns, requires_all);
            }
            SqlStatement::InsertSelect { query, .. }
            | SqlStatement::InsertOverwrite { query, .. }
            | SqlStatement::CreateTableAs { query, .. }
            | SqlStatement::Explain { stmt: query, .. } => {
                Self::collect_cte_columns_in_statement(query, aliases, columns, requires_all);
            }
            SqlStatement::HiveMultiInsert { inserts } => {
                for insert in inserts {
                    Self::collect_cte_columns_in_statement(insert, aliases, columns, requires_all);
                }
            }
            SqlStatement::CreateView { stmt, .. } => {
                Self::collect_cte_columns_in_select(stmt, aliases, columns, requires_all);
            }
            SqlStatement::Delete { where_clause, .. } => {
                if let Some(expr) = where_clause {
                    Self::collect_cte_columns_in_expr(expr, aliases, columns, requires_all, true);
                }
            }
            SqlStatement::Update {
                assignments,
                where_clause,
                ..
            } => {
                for (_, expr) in assignments {
                    Self::collect_cte_columns_in_expr(expr, aliases, columns, requires_all, true);
                }
                if let Some(expr) = where_clause {
                    Self::collect_cte_columns_in_expr(expr, aliases, columns, requires_all, true);
                }
            }
            _ => {}
        }
    }

    fn collect_cte_columns_in_select(
        select: &SelectStatement,
        aliases: &HashSet<String>,
        columns: &mut HashSet<String>,
        requires_all: &mut bool,
    ) {
        let unqualified_requires_all = Self::select_references_cte_alias(select, aliases);
        for column in &select.columns {
            match column {
                SelectColumn::All | SelectColumn::AllExclude(_) | SelectColumn::AllReplace(_) => {
                    if unqualified_requires_all {
                        *requires_all = true;
                    }
                }
                SelectColumn::Column(name) => {
                    Self::collect_cte_column_ref(
                        name,
                        aliases,
                        columns,
                        requires_all,
                        unqualified_requires_all,
                    );
                }
                SelectColumn::ColumnAlias { column, .. } => {
                    Self::collect_cte_column_ref(
                        column,
                        aliases,
                        columns,
                        requires_all,
                        unqualified_requires_all,
                    );
                }
                SelectColumn::Aggregate {
                    column: Some(column),
                    ..
                } if column != "*"
                    && !column
                        .chars()
                        .next()
                        .is_some_and(|ch| ch.is_ascii_digit()) =>
                {
                    Self::collect_cte_column_ref(
                        column,
                        aliases,
                        columns,
                        requires_all,
                        unqualified_requires_all,
                    );
                }
                SelectColumn::Expression { expr, .. } => {
                    Self::collect_cte_columns_in_expr(
                        expr,
                        aliases,
                        columns,
                        requires_all,
                        unqualified_requires_all,
                    );
                }
                SelectColumn::WindowFunction {
                    args,
                    partition_by,
                    order_by,
                    ..
                } => {
                    for arg in args {
                        Self::collect_cte_column_ref(
                            arg,
                            aliases,
                            columns,
                            requires_all,
                            unqualified_requires_all,
                        );
                    }
                    for partition in partition_by {
                        Self::collect_cte_column_ref(
                            partition,
                            aliases,
                            columns,
                            requires_all,
                            unqualified_requires_all,
                        );
                    }
                    for order in order_by {
                        Self::collect_cte_column_ref(
                            &order.column,
                            aliases,
                            columns,
                            requires_all,
                            unqualified_requires_all,
                        );
                        if let Some(expr) = &order.expr {
                            Self::collect_cte_columns_in_expr(
                                expr,
                                aliases,
                                columns,
                                requires_all,
                                unqualified_requires_all,
                            );
                        }
                    }
                }
                _ => {}
            }
        }

        if let Some(expr) = &select.where_clause {
            Self::collect_cte_columns_in_expr(
                expr,
                aliases,
                columns,
                requires_all,
                unqualified_requires_all,
            );
        }
        if let Some(expr) = &select.having {
            Self::collect_cte_columns_in_expr(
                expr,
                aliases,
                columns,
                requires_all,
                unqualified_requires_all,
            );
        }
        for expr in select.group_by_exprs.iter().flatten() {
            Self::collect_cte_columns_in_expr(
                expr,
                aliases,
                columns,
                requires_all,
                unqualified_requires_all,
            );
        }
        for group in &select.group_by {
            Self::collect_cte_column_ref(
                group,
                aliases,
                columns,
                requires_all,
                unqualified_requires_all,
            );
        }
        for order in &select.order_by {
            Self::collect_cte_column_ref(
                &order.column,
                aliases,
                columns,
                requires_all,
                unqualified_requires_all,
            );
            if let Some(expr) = &order.expr {
                Self::collect_cte_columns_in_expr(
                    expr,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
            }
        }
        for join in &select.joins {
            Self::collect_cte_columns_in_from(
                &join.right,
                aliases,
                columns,
                requires_all,
                unqualified_requires_all,
            );
            Self::collect_cte_columns_in_expr(
                &join.on,
                aliases,
                columns,
                requires_all,
                unqualified_requires_all,
            );
        }
        if let Some(from) = &select.from {
            Self::collect_cte_columns_in_from(
                from,
                aliases,
                columns,
                requires_all,
                unqualified_requires_all,
            );
        }
    }

    fn select_references_cte_alias(select: &SelectStatement, aliases: &HashSet<String>) -> bool {
        select
            .from
            .as_ref()
            .is_some_and(|from| Self::from_item_references_cte_alias(from, aliases))
            || select
                .joins
                .iter()
                .any(|join| Self::from_item_references_cte_alias(&join.right, aliases))
    }

    fn from_item_references_cte_alias(from: &FromItem, aliases: &HashSet<String>) -> bool {
        match from {
            FromItem::Table { table, .. } => {
                aliases.contains(&table.trim_matches('"').to_ascii_lowercase())
            }
            _ => false,
        }
    }

    fn collect_cte_columns_in_from(
        from: &FromItem,
        aliases: &HashSet<String>,
        columns: &mut HashSet<String>,
        requires_all: &mut bool,
        unqualified_requires_all: bool,
    ) {
        match from {
            FromItem::Subquery { stmt, .. } => {
                Self::collect_cte_columns_in_statement(stmt, aliases, columns, requires_all);
            }
            FromItem::LateralExplode { expr, .. } | FromItem::LateralPosExplode { expr, .. } => {
                Self::collect_cte_columns_in_expr(
                    expr,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
            }
            FromItem::LateralStack { values, .. } => {
                for expr in values {
                    Self::collect_cte_columns_in_expr(
                        expr,
                        aliases,
                        columns,
                        requires_all,
                        unqualified_requires_all,
                    );
                }
            }
            _ => {}
        }
    }

    fn collect_cte_columns_in_expr(
        expr: &SqlExpr,
        aliases: &HashSet<String>,
        columns: &mut HashSet<String>,
        requires_all: &mut bool,
        unqualified_requires_all: bool,
    ) {
        match expr {
            SqlExpr::Column(name) => Self::collect_cte_column_ref(
                name,
                aliases,
                columns,
                requires_all,
                unqualified_requires_all,
            ),
            SqlExpr::BinaryOp { left, right, .. } => {
                Self::collect_cte_columns_in_expr(
                    left,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
                Self::collect_cte_columns_in_expr(
                    right,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
            }
            SqlExpr::UnaryOp { expr, .. }
            | SqlExpr::Cast { expr, .. }
            | SqlExpr::Paren(expr)
            | SqlExpr::ExplodeRename { inner: expr, .. } => {
                Self::collect_cte_columns_in_expr(
                    expr,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
            }
            SqlExpr::InSubquery { column, stmt, .. } => {
                Self::collect_cte_column_ref(
                    column,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
                Self::collect_cte_columns_in_select(stmt, aliases, columns, requires_all);
            }
            SqlExpr::ExistsSubquery { stmt } | SqlExpr::ScalarSubquery { stmt } => {
                Self::collect_cte_columns_in_select(stmt, aliases, columns, requires_all);
            }
            SqlExpr::Case {
                when_then,
                else_expr,
            } => {
                for (when, then) in when_then {
                    Self::collect_cte_columns_in_expr(
                        when,
                        aliases,
                        columns,
                        requires_all,
                        unqualified_requires_all,
                    );
                    Self::collect_cte_columns_in_expr(
                        then,
                        aliases,
                        columns,
                        requires_all,
                        unqualified_requires_all,
                    );
                }
                if let Some(expr) = else_expr {
                    Self::collect_cte_columns_in_expr(
                        expr,
                        aliases,
                        columns,
                        requires_all,
                        unqualified_requires_all,
                    );
                }
            }
            SqlExpr::Between {
                column, low, high, ..
            } => {
                Self::collect_cte_column_ref(
                    column,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
                Self::collect_cte_columns_in_expr(
                    low,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
                Self::collect_cte_columns_in_expr(
                    high,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
            }
            SqlExpr::Like { column, .. }
            | SqlExpr::Regexp { column, .. }
            | SqlExpr::In { column, .. }
            | SqlExpr::IsNull { column, .. } => {
                Self::collect_cte_column_ref(
                    column,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
            }
            SqlExpr::Function { args, .. } => {
                for arg in args {
                    Self::collect_cte_columns_in_expr(
                        arg,
                        aliases,
                        columns,
                        requires_all,
                        unqualified_requires_all,
                    );
                }
            }
            SqlExpr::ArrayIndex { array, index } => {
                Self::collect_cte_columns_in_expr(
                    array,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
                Self::collect_cte_columns_in_expr(
                    index,
                    aliases,
                    columns,
                    requires_all,
                    unqualified_requires_all,
                );
            }
            _ => {}
        }
    }

    fn collect_cte_column_ref(
        reference: &str,
        aliases: &HashSet<String>,
        columns: &mut HashSet<String>,
        requires_all: &mut bool,
        unqualified_requires_all: bool,
    ) {
        let trimmed = reference.trim_matches('"');
        let Some((qualifier, column)) = trimmed.rsplit_once('.') else {
            if unqualified_requires_all {
                *requires_all = true;
            }
            return;
        };
        let qualifier = qualifier.trim_matches('"').to_ascii_lowercase();
        if aliases.contains(&qualifier) {
            columns.insert(Self::unqualified_name(column).to_ascii_lowercase());
        }
    }

    /// Helper: insert a RecordBatch into a TableStorageBackend
    fn insert_batch_into_backend(
        backend: &TableStorageBackend,
        batch: &RecordBatch,
    ) -> io::Result<()> {
        let schema = batch.schema();
        // Keep shared-CTE materialization memory bounded. A Hive EXPLODE can turn
        // one million source rows into several million intermediate rows; building
        // one HashMap vector for the entire batch would otherwise dominate memory.
        const CHUNK_ROWS: usize = 8192;
        for start in (0..batch.num_rows()).step_by(CHUNK_ROWS) {
            let end = (start + CHUNK_ROWS).min(batch.num_rows());
            let mut rows: Vec<std::collections::HashMap<String, crate::data::Value>> =
                Vec::with_capacity(end - start);
            for row_idx in start..end {
                let mut row = std::collections::HashMap::with_capacity(schema.fields().len());
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let col = batch.column(col_idx);
                    let val = Self::arrow_value_at_col(col, row_idx);
                    row.insert(field.name().clone(), val);
                }
                rows.push(row);
            }
            backend.insert_rows(&rows)?;
        }
        Ok(())
    }

    /// Helper: execute a statement with CTE name rewritten to reference temp table
    fn execute_main_with_cte(
        name: &str,
        main: SqlStatement,
        base_dir: &Path,
        default_table_path: &Path,
        temp_path: &Path,
    ) -> io::Result<io::Result<ApexResult>> {
        let temp_table_name = temp_path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        let mut rewritten = main;
        Self::visit_cte_references_in_statement(&mut rewritten, name, None, Some(&temp_table_name));
        Ok(Self::execute_parsed_multi(
            rewritten,
            base_dir,
            default_table_path,
        ))
    }

    /// Visit every visible CTE relation reference. With no replacement this
    /// only counts references; otherwise it rewrites them in the same pass.
    fn visit_cte_references_in_statement(
        stmt: &mut SqlStatement,
        cte_name: &str,
        inline_body: Option<&SqlStatement>,
        temp_table_name: Option<&str>,
    ) -> usize {
        match stmt {
            SqlStatement::Select(select) => {
                Self::visit_cte_references_in_select(select, cte_name, inline_body, temp_table_name)
            }
            SqlStatement::Union(union) => {
                Self::visit_cte_references_in_statement(
                    &mut union.left,
                    cte_name,
                    inline_body,
                    temp_table_name,
                ) + Self::visit_cte_references_in_statement(
                    &mut union.right,
                    cte_name,
                    inline_body,
                    temp_table_name,
                )
            }
            SqlStatement::Cte {
                name, body, main, ..
            } => {
                if name.eq_ignore_ascii_case(cte_name) {
                    0
                } else {
                    Self::visit_cte_references_in_statement(
                        body,
                        cte_name,
                        inline_body,
                        temp_table_name,
                    ) + Self::visit_cte_references_in_statement(
                        main,
                        cte_name,
                        inline_body,
                        temp_table_name,
                    )
                }
            }
            SqlStatement::InsertSelect { query, .. }
            | SqlStatement::InsertOverwrite { query, .. }
            | SqlStatement::CreateTableAs { query, .. }
            | SqlStatement::Explain { stmt: query, .. } => Self::visit_cte_references_in_statement(
                query,
                cte_name,
                inline_body,
                temp_table_name,
            ),
            SqlStatement::HiveMultiInsert { inserts } => inserts
                .iter_mut()
                .map(|insert| {
                    Self::visit_cte_references_in_statement(
                        insert,
                        cte_name,
                        inline_body,
                        temp_table_name,
                    )
                })
                .sum(),
            SqlStatement::CreateView { stmt, .. } => {
                Self::visit_cte_references_in_select(stmt, cte_name, inline_body, temp_table_name)
            }
            SqlStatement::Delete { where_clause, .. } => where_clause.as_mut().map_or(0, |expr| {
                Self::visit_cte_references_in_expr(expr, cte_name, inline_body, temp_table_name)
            }),
            SqlStatement::Update {
                assignments,
                where_clause,
                ..
            } => {
                assignments
                    .iter_mut()
                    .map(|(_, expr)| {
                        Self::visit_cte_references_in_expr(
                            expr,
                            cte_name,
                            inline_body,
                            temp_table_name,
                        )
                    })
                    .sum::<usize>()
                    + where_clause.as_mut().map_or(0, |expr| {
                        Self::visit_cte_references_in_expr(
                            expr,
                            cte_name,
                            inline_body,
                            temp_table_name,
                        )
                    })
            }
            SqlStatement::InsertOnConflict { do_update, .. } => {
                do_update.as_mut().map_or(0, |items| {
                    items
                        .iter_mut()
                        .map(|(_, expr)| {
                            Self::visit_cte_references_in_expr(
                                expr,
                                cte_name,
                                inline_body,
                                temp_table_name,
                            )
                        })
                        .sum()
                })
            }
            _ => 0,
        }
    }

    fn visit_cte_references_in_select(
        select: &mut SelectStatement,
        cte_name: &str,
        inline_body: Option<&SqlStatement>,
        temp_table_name: Option<&str>,
    ) -> usize {
        let mut count = select.from.as_mut().map_or(0, |from| {
            Self::visit_cte_references_in_from(from, cte_name, inline_body, temp_table_name)
        });
        for join in &mut select.joins {
            count += Self::visit_cte_references_in_from(
                &mut join.right,
                cte_name,
                inline_body,
                temp_table_name,
            );
            count += Self::visit_cte_references_in_expr(
                &mut join.on,
                cte_name,
                inline_body,
                temp_table_name,
            );
        }
        for column in &mut select.columns {
            match column {
                SelectColumn::Expression { expr, .. } => {
                    count += Self::visit_cte_references_in_expr(
                        expr,
                        cte_name,
                        inline_body,
                        temp_table_name,
                    );
                }
                SelectColumn::AllReplace(items) => {
                    for (expr, _) in items {
                        count += Self::visit_cte_references_in_expr(
                            expr,
                            cte_name,
                            inline_body,
                            temp_table_name,
                        );
                    }
                }
                _ => {}
            }
        }
        for expr in select.group_by_exprs.iter_mut().flatten() {
            count +=
                Self::visit_cte_references_in_expr(expr, cte_name, inline_body, temp_table_name);
        }
        for expr in [&mut select.where_clause, &mut select.having]
            .into_iter()
            .flatten()
        {
            count +=
                Self::visit_cte_references_in_expr(expr, cte_name, inline_body, temp_table_name);
        }
        for order in &mut select.order_by {
            if let Some(expr) = &mut order.expr {
                count += Self::visit_cte_references_in_expr(
                    expr,
                    cte_name,
                    inline_body,
                    temp_table_name,
                );
            }
        }
        count
    }

    fn visit_cte_references_in_from(
        from: &mut FromItem,
        cte_name: &str,
        inline_body: Option<&SqlStatement>,
        temp_table_name: Option<&str>,
    ) -> usize {
        match from {
            FromItem::Table { table, alias } if table.eq_ignore_ascii_case(cte_name) => {
                if inline_body.is_some() || temp_table_name.is_some() {
                    let alias = alias.clone().unwrap_or_else(|| cte_name.to_string());
                    *from = if let Some(body) = inline_body {
                        FromItem::Subquery {
                            stmt: Box::new(body.clone()),
                            alias,
                        }
                    } else {
                        FromItem::Table {
                            table: temp_table_name.unwrap().to_string(),
                            alias: Some(alias),
                        }
                    };
                }
                1
            }
            FromItem::Subquery { stmt, .. } => Self::visit_cte_references_in_statement(
                stmt,
                cte_name,
                inline_body,
                temp_table_name,
            ),
            _ => 0,
        }
    }

    fn visit_cte_references_in_expr(
        expr: &mut SqlExpr,
        cte_name: &str,
        inline_body: Option<&SqlStatement>,
        temp_table_name: Option<&str>,
    ) -> usize {
        match expr {
            SqlExpr::BinaryOp { left, right, .. } => {
                Self::visit_cte_references_in_expr(left, cte_name, inline_body, temp_table_name)
                    + Self::visit_cte_references_in_expr(
                        right,
                        cte_name,
                        inline_body,
                        temp_table_name,
                    )
            }
            SqlExpr::UnaryOp { expr, .. }
            | SqlExpr::Cast { expr, .. }
            | SqlExpr::Paren(expr)
            | SqlExpr::ExplodeRename { inner: expr, .. } => {
                Self::visit_cte_references_in_expr(expr, cte_name, inline_body, temp_table_name)
            }
            SqlExpr::InSubquery { stmt, .. }
            | SqlExpr::ExistsSubquery { stmt }
            | SqlExpr::ScalarSubquery { stmt } => {
                Self::visit_cte_references_in_select(stmt, cte_name, inline_body, temp_table_name)
            }
            SqlExpr::Case {
                when_then,
                else_expr,
            } => {
                let branches = when_then
                    .iter_mut()
                    .map(|(when, then)| {
                        Self::visit_cte_references_in_expr(
                            when,
                            cte_name,
                            inline_body,
                            temp_table_name,
                        ) + Self::visit_cte_references_in_expr(
                            then,
                            cte_name,
                            inline_body,
                            temp_table_name,
                        )
                    })
                    .sum::<usize>();
                branches
                    + else_expr.as_mut().map_or(0, |expr| {
                        Self::visit_cte_references_in_expr(
                            expr,
                            cte_name,
                            inline_body,
                            temp_table_name,
                        )
                    })
            }
            SqlExpr::Between { low, high, .. }
            | SqlExpr::ArrayIndex {
                array: low,
                index: high,
            } => {
                Self::visit_cte_references_in_expr(low, cte_name, inline_body, temp_table_name)
                    + Self::visit_cte_references_in_expr(
                        high,
                        cte_name,
                        inline_body,
                        temp_table_name,
                    )
            }
            SqlExpr::Function { args, .. } => args
                .iter_mut()
                .map(|expr| {
                    Self::visit_cte_references_in_expr(expr, cte_name, inline_body, temp_table_name)
                })
                .sum(),
            _ => 0,
        }
    }

    /// Helper: remap a RecordBatch's column names by position to match target names.
    /// Used in recursive CTEs where the recursive part may have different column names than the anchor.
    fn remap_batch_columns(
        batch: &RecordBatch,
        target_names: &[String],
    ) -> io::Result<RecordBatch> {
        let num_cols = batch.num_columns().min(target_names.len());
        let batch_schema = batch.schema();
        let mut fields = Vec::with_capacity(num_cols);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_cols);
        for i in 0..num_cols {
            let old_field = batch_schema.field(i);
            fields.push(Field::new(
                &target_names[i],
                old_field.data_type().clone(),
                old_field.is_nullable(),
            ));
            arrays.push(batch.column(i).clone());
        }
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| err_data(format!("Failed to remap CTE columns: {}", e)))
    }

    /// Helper: clean up temp CTE files
    fn cleanup_temp_table(path: &Path) {
        CTE_BATCH_CACHE.remove(path);
        let delta_path = Self::delta_path_for_table(path);
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("apex.wal"));
        let _ = std::fs::remove_file(path.with_extension("apex.lock"));
        let _ = std::fs::remove_file(Self::delta_meta_path_for_delta(&delta_path));
        let _ = std::fs::remove_file(&delta_path);
        let _ = std::fs::remove_file(path.with_extension("apex.deltastore"));
        invalidate_storage_cache(path);
    }

    /// Execute INSERT ... SELECT statement
    fn execute_insert_select(
        target_path: &Path,
        columns: Option<&[String]>,
        query: SqlStatement,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<ApexResult> {
        // Step 1: Execute the SELECT query
        let select_result = Self::execute_parsed_multi(query, base_dir, default_table_path)?;
        let select_batch = select_result.to_record_batch().map_err(|e| {
            err_data(format!(
                "INSERT SELECT query must return a result set: {}",
                e
            ))
        })?;

        if select_batch.num_rows() == 0 {
            return Ok(ApexResult::Scalar(0));
        }

        // Step 2: map source expressions to target columns strictly by
        // position.  Source aliases never become new target schema columns.
        let schema = select_batch.schema();
        let source_indices = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| (field.name() != "_id").then_some(index))
            .collect::<Vec<_>>();
        let target_names = if let Some(cols) = columns {
            cols.to_vec()
        } else {
            TableStorageBackend::open(target_path)?
                .get_schema()
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>()
        };
        if source_indices.len() != target_names.len() {
            return Err(err_input(format!(
                "INSERT SELECT returns {} columns but target has {} columns",
                source_indices.len(),
                target_names.len()
            )));
        }

        let mut values: Vec<Vec<crate::data::Value>> = Vec::with_capacity(select_batch.num_rows());
        for row_idx in 0..select_batch.num_rows() {
            let mut row: Vec<crate::data::Value> = Vec::with_capacity(source_indices.len());
            for &col_idx in &source_indices {
                let col = select_batch.column(col_idx);
                let val = Self::arrow_value_at_col(col, row_idx);
                row.push(val);
            }
            values.push(row);
        }

        Self::execute_insert(target_path, Some(&target_names), &values)
    }

    /// Hive-compatible `INSERT OVERWRITE TABLE ... PARTITION (...) SELECT ...`.
    /// A static partition is replaced atomically under the caller's table lock;
    /// without PARTITION the whole table is replaced.
    fn execute_insert_overwrite(
        target_path: &Path,
        partition: &[(String, Value)],
        query: SqlStatement,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<ApexResult> {
        let select_result = Self::execute_parsed_multi(query, base_dir, default_table_path)?;
        let select_batch = select_result
            .to_record_batch()
            .map_err(|e| err_data(format!("INSERT OVERWRITE query must return rows: {}", e)))?;

        if !target_path.exists() {
            if let Some(parent) = target_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let backend = TableStorageBackend::create(target_path)?;
            for field in select_batch.schema().fields() {
                if field.name() == "_id" {
                    continue;
                }
                let ty = match field.data_type() {
                    ArrowDataType::Int64 | ArrowDataType::UInt64 => DataType::Int64,
                    ArrowDataType::Float64 => DataType::Float64,
                    ArrowDataType::Boolean => DataType::Bool,
                    _ => DataType::String,
                };
                backend.add_column(field.name(), ty)?;
            }
            for (name, value) in partition {
                let ty = match value {
                    Value::Int64(_) => DataType::Int64,
                    Value::Float64(_) => DataType::Float64,
                    Value::Bool(_) => DataType::Bool,
                    _ => DataType::String,
                };
                backend.add_column(name, ty)?;
            }
            backend.save()?;
            invalidate_storage_cache(target_path);
        }

        if partition.is_empty() {
            Self::execute_truncate(target_path)?;
        } else {
            let predicate = partition.iter().fold(None, |acc, (column, value)| {
                let equality = SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::Column(column.clone())),
                    op: BinaryOperator::Eq,
                    right: Box::new(SqlExpr::Literal(value.clone())),
                };
                Some(match acc {
                    None => equality,
                    Some(left) => SqlExpr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::And,
                        right: Box::new(equality),
                    },
                })
            });
            Self::execute_delete(target_path, predicate.as_ref())?;
        }

        if select_batch.num_rows() == 0 {
            return Ok(ApexResult::Scalar(0));
        }
        if select_batch.num_columns() >= 64 {
            return Self::insert_overwrite_batch_columnar(target_path, &select_batch, partition);
        }
        let data_columns: Vec<(usize, String)> = select_batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name() != "_id")
            .map(|(i, f)| (i, f.name().clone()))
            .collect();
        let mut column_names: Vec<String> = data_columns.iter().map(|(_, n)| n.clone()).collect();
        column_names.extend(partition.iter().map(|(n, _)| n.clone()));
        let mut rows = Vec::with_capacity(select_batch.num_rows());
        for row_index in 0..select_batch.num_rows() {
            let mut row = Vec::with_capacity(column_names.len());
            for (column_index, _) in &data_columns {
                row.push(Self::arrow_value_at_col(
                    select_batch.column(*column_index),
                    row_index,
                ));
            }
            row.extend(partition.iter().map(|(_, v)| v.clone()));
            rows.push(row);
        }
        Self::execute_insert(target_path, Some(&column_names), &rows)
    }

    /// Columnar fast path for very wide INSERT OVERWRITE results. Avoid the
    /// Arrow -> Vec<Vec<Value>> -> HashMap-per-row -> column-store round trip.
    fn insert_overwrite_batch_columnar(
        target_path: &Path,
        batch: &RecordBatch,
        partition: &[(String, Value)],
    ) -> io::Result<ApexResult> {
        use arrow::array::{LargeStringArray, UInt64Array};
        use std::collections::HashMap;

        let rows = batch.num_rows();
        let mut ints: HashMap<String, Vec<i64>> = HashMap::new();
        let mut floats: HashMap<String, Vec<f64>> = HashMap::new();
        let mut strings: HashMap<String, Vec<String>> = HashMap::new();
        let binaries: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut bools: HashMap<String, Vec<bool>> = HashMap::new();
        let mut nulls: HashMap<String, Vec<bool>> = HashMap::new();
        let backend = TableStorageBackend::open_for_write(target_path)?;
        let target_types: HashMap<String, DataType> = backend.get_schema().into_iter().collect();

        for (index, field) in batch.schema().fields().iter().enumerate() {
            if field.name() == "_id" {
                continue;
            }
            let name = field.name().clone();
            let array = batch.column(index);
            let null_bitmap = (0..rows).map(|row| array.is_null(row)).collect::<Vec<_>>();
            match field.data_type() {
                ArrowDataType::Int64 => {
                    let values = array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| err_data(format!("Invalid Int64 column {}", name)))?;
                    if matches!(
                        target_types.get(&name),
                        Some(DataType::Float32 | DataType::Float64)
                    ) {
                        floats.insert(
                            name.clone(),
                            (0..rows)
                                .map(|row| {
                                    if values.is_null(row) {
                                        0.0
                                    } else {
                                        values.value(row) as f64
                                    }
                                })
                                .collect(),
                        );
                    } else {
                        ints.insert(
                            name.clone(),
                            (0..rows)
                                .map(|row| {
                                    if values.is_null(row) {
                                        0
                                    } else {
                                        values.value(row)
                                    }
                                })
                                .collect(),
                        );
                    }
                }
                ArrowDataType::UInt64 => {
                    let values = array
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| err_data(format!("Invalid UInt64 column {}", name)))?;
                    if matches!(
                        target_types.get(&name),
                        Some(DataType::Float32 | DataType::Float64)
                    ) {
                        floats.insert(
                            name.clone(),
                            (0..rows)
                                .map(|row| {
                                    if values.is_null(row) {
                                        0.0
                                    } else {
                                        values.value(row) as f64
                                    }
                                })
                                .collect(),
                        );
                    } else {
                        ints.insert(
                            name.clone(),
                            (0..rows)
                                .map(|row| {
                                    if values.is_null(row) {
                                        0
                                    } else {
                                        values.value(row) as i64
                                    }
                                })
                                .collect(),
                        );
                    }
                }
                ArrowDataType::Float64 => {
                    let values = array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| err_data(format!("Invalid Float64 column {}", name)))?;
                    if matches!(
                        target_types.get(&name),
                        Some(
                            DataType::Int8
                                | DataType::Int16
                                | DataType::Int32
                                | DataType::Int64
                                | DataType::UInt8
                                | DataType::UInt16
                                | DataType::UInt32
                                | DataType::UInt64
                        )
                    ) {
                        ints.insert(
                            name.clone(),
                            (0..rows)
                                .map(|row| {
                                    if values.is_null(row) {
                                        0
                                    } else {
                                        values.value(row) as i64
                                    }
                                })
                                .collect(),
                        );
                    } else {
                        floats.insert(
                            name.clone(),
                            (0..rows)
                                .map(|row| {
                                    if values.is_null(row) {
                                        0.0
                                    } else {
                                        values.value(row)
                                    }
                                })
                                .collect(),
                        );
                    }
                }
                ArrowDataType::Boolean => {
                    let values = array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| err_data(format!("Invalid Boolean column {}", name)))?;
                    bools.insert(
                        name.clone(),
                        (0..rows)
                            .map(|row| !values.is_null(row) && values.value(row))
                            .collect(),
                    );
                }
                ArrowDataType::Utf8 => {
                    let values = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| err_data(format!("Invalid Utf8 column {}", name)))?;
                    strings.insert(
                        name.clone(),
                        (0..rows)
                            .map(|row| {
                                if values.is_null(row) {
                                    String::new()
                                } else {
                                    values.value(row).to_string()
                                }
                            })
                            .collect(),
                    );
                }
                ArrowDataType::LargeUtf8 => {
                    let values = array
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| err_data(format!("Invalid LargeUtf8 column {}", name)))?;
                    strings.insert(
                        name.clone(),
                        (0..rows)
                            .map(|row| {
                                if values.is_null(row) {
                                    String::new()
                                } else {
                                    values.value(row).to_string()
                                }
                            })
                            .collect(),
                    );
                }
                _ => {
                    strings.insert(
                        name.clone(),
                        (0..rows)
                            .map(|row| {
                                if array.is_null(row) {
                                    String::new()
                                } else {
                                    Self::arrow_value_at_col(array, row)
                                        .as_str()
                                        .unwrap_or_default()
                                        .to_string()
                                }
                            })
                            .collect(),
                    );
                }
            }
            nulls.insert(name, null_bitmap);
        }

        for (name, value) in partition {
            let null_bitmap = vec![matches!(value, Value::Null); rows];
            match value {
                Value::Int64(value) => {
                    ints.insert(name.clone(), vec![*value; rows]);
                }
                Value::Float64(value) => {
                    floats.insert(name.clone(), vec![*value; rows]);
                }
                Value::Bool(value) => {
                    bools.insert(name.clone(), vec![*value; rows]);
                }
                Value::String(value) => {
                    strings.insert(name.clone(), vec![value.clone(); rows]);
                }
                Value::Null => {
                    strings.insert(name.clone(), vec![String::new(); rows]);
                }
                other => {
                    strings.insert(name.clone(), vec![format!("{:?}", other); rows]);
                }
            }
            nulls.insert(name.clone(), null_bitmap);
        }

        let inserted = backend
            .insert_typed_with_nulls(ints, floats, strings, binaries, bools, nulls)?
            .len();
        backend.save_full()?;
        invalidate_storage_cache(target_path);
        crate::storage::engine::engine().invalidate(target_path);
        Ok(ApexResult::Scalar(inserted as i64))
    }

    /// Execute CREATE TABLE ... AS SELECT statement
    fn execute_create_table_as(
        base_dir: &Path,
        default_table_path: &Path,
        table: &str,
        query: SqlStatement,
        if_not_exists: bool,
    ) -> io::Result<ApexResult> {
        let table_path = Self::resolve_table_path(table, base_dir, default_table_path);

        if table_path.exists() {
            if if_not_exists {
                return Ok(ApexResult::Scalar(0));
            }
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Table '{}' already exists", table),
            ));
        }
        let epoch_write = crate::storage::epoch::logical_write(&table_path);

        // Step 1: Execute the SELECT query
        let select_result = Self::execute_parsed_multi(query, base_dir, default_table_path)?;
        let select_batch = select_result
            .to_record_batch()
            .map_err(|e| err_data(format!("CTAS query must return a result set: {}", e)))?;

        // Step 2: Create the table with schema from SELECT result
        let schema = select_batch.schema();
        let backend = TableStorageBackend::create(&table_path)?;
        for field in schema.fields() {
            if field.name() == "_id" {
                continue;
            }
            let col_type = match field.data_type() {
                ArrowDataType::Int64 | ArrowDataType::UInt64 => crate::data::DataType::Int64,
                ArrowDataType::Float64 => crate::data::DataType::Float64,
                ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => crate::data::DataType::String,
                ArrowDataType::Binary | ArrowDataType::LargeBinary => {
                    crate::data::DataType::Blob
                }
                ArrowDataType::Boolean => crate::data::DataType::Bool,
                _ => crate::data::DataType::String,
            };
            backend.add_column(field.name(), col_type)?;
        }

        // Step 3: Insert rows if any
        let inserted = select_batch.num_rows();
        if inserted > 0 {
            let mut rows: Vec<std::collections::HashMap<String, crate::data::Value>> =
                Vec::with_capacity(inserted);
            for row_idx in 0..inserted {
                let mut row = std::collections::HashMap::new();
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    if field.name() == "_id" {
                        continue;
                    }
                    let col = select_batch.column(col_idx);
                    let val = match Self::arrow_value_at_col(col, row_idx) {
                        Value::Binary(bytes)
                            if matches!(
                                field.data_type(),
                                ArrowDataType::Binary | ArrowDataType::LargeBinary
                            ) =>
                        {
                            Value::Blob(bytes)
                        }
                        value => value,
                    };
                    row.insert(field.name().clone(), val);
                }
                rows.push(row);
            }
            backend.insert_rows(&rows)?;
        }
        backend.save()?;
        invalidate_storage_cache(&table_path);
        invalidate_table_stats(&table_path.to_string_lossy());
        epoch_write.commit();

        Ok(ApexResult::Scalar(inserted as i64))
    }

    // ========== FTS DDL Handlers ==========

    /// Path of the FTS config JSON file for a given database directory.
    fn fts_config_path(base_dir: &Path) -> PathBuf {
        base_dir.join("fts_config.json")
    }

    /// Read the fts_config.json as a serde_json::Value (object).  Returns empty object if missing.
    pub(in crate::query::executor) fn read_fts_config(base_dir: &Path) -> serde_json::Value {
        let path = Self::fts_config_path(base_dir);
        if !path.exists() {
            return serde_json::Value::Object(serde_json::Map::new());
        }
        match std::fs::read_to_string(&path) {
            Ok(s) => serde_json::from_str(&s)
                .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new())),
            Err(_) => serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    /// Write the fts_config.json from a serde_json::Value.
    fn write_fts_config(base_dir: &Path, cfg: &serde_json::Value) {
        let path = Self::fts_config_path(base_dir);
        if let Ok(s) = serde_json::to_string(cfg) {
            let temp = path.with_extension("json.tmp");
            if std::fs::write(&temp, s).is_ok() {
                let _ = std::fs::rename(temp, path);
            }
        }
    }

    /// CREATE FTS INDEX ON table [(col1, col2)] [WITH (lazy_load=.., cache_size=..)]
    pub(super) fn execute_create_fts_index(
        base_dir: &Path,
        table: &str,
        fields: Option<&[String]>,
        lazy_load: bool,
        cache_size: usize,
    ) -> io::Result<ApexResult> {
        use crate::fts::{FtsConfig, FtsManager};

        // Update fts_config.json
        let mut cfg = Self::read_fts_config(base_dir);
        let obj = cfg
            .as_object_mut()
            .ok_or_else(|| err_input("Corrupt fts_config.json"))?;
        let table_cfg = serde_json::json!({
            "enabled": true,
            "index_fields": fields.map(|f| serde_json::json!(f)).unwrap_or(serde_json::Value::Null),
            "config": {
                "lazy_load": lazy_load,
                "cache_size": cache_size
            }
        });
        obj.insert(table.to_string(), table_cfg);
        Self::write_fts_config(base_dir, &cfg);

        // Ensure FtsManager for this base_dir exists and the engine for this table is created
        let fts_cfg = FtsConfig {
            lazy_load,
            cache_size,
            ..FtsConfig::default()
        };
        let fts_dir = base_dir.join("fts_indexes");
        let mgr = {
            let existing = crate::query::executor::get_fts_manager(base_dir);
            match existing {
                Some(m) => {
                    // CREATE is a rebuild: retaining the old postings would keep
                    // terms from columns that are no longer indexed.
                    m.remove_engine(table, true)
                        .map_err(|e| err_data(e.to_string()))?;
                    m
                }
                None => {
                    let m = std::sync::Arc::new(FtsManager::new(&fts_dir, fts_cfg.clone()));
                    crate::query::executor::register_fts_manager(base_dir, m.clone());
                    m
                }
            }
        };
        mgr.configure_table(table, fts_cfg);
        // Warm up the engine (creates it if not present)
        let _ = mgr.get_engine(table);

        // Back-fill existing rows into FTS index
        let backfilled = Self::fts_backfill_table(base_dir, table, fields, mgr.clone())?;

        let fields_desc = fields
            .map(|f| f.join(", "))
            .unwrap_or_else(|| "all string cols".to_string());
        let msg = format!(
            "FTS index created on '{}' (fields: {}, {} rows indexed)",
            table, fields_desc, backfilled
        );

        // Return a one-row status RecordBatch
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "status",
            ArrowDataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(StringArray::from(vec![msg.as_str()]))],
        )
        .map_err(|e| err_data(e.to_string()))?;
        Ok(ApexResult::Data(batch))
    }

    /// Back-fill existing table rows into an FTS engine.
    /// Reads all rows with their string columns and adds them to the FTS index.
    /// Returns the number of rows indexed.
    pub(crate) fn fts_backfill_table(
        base_dir: &Path,
        table: &str,
        fields: Option<&[String]>,
        mgr: std::sync::Arc<crate::fts::FtsManager>,
    ) -> io::Result<usize> {
        Self::fts_backfill_table_inner(base_dir, table, fields, mgr, true)
    }

    fn fts_backfill_table_sync(
        base_dir: &Path,
        table: &str,
        fields: Option<&[String]>,
        mgr: std::sync::Arc<crate::fts::FtsManager>,
    ) -> io::Result<usize> {
        Self::fts_backfill_table_inner(base_dir, table, fields, mgr, false)
    }

    fn fts_backfill_table_inner(
        base_dir: &Path,
        table: &str,
        fields: Option<&[String]>,
        mgr: std::sync::Arc<crate::fts::FtsManager>,
        allow_async: bool,
    ) -> io::Result<usize> {
        use arrow::array::{Int64Array, StringArray, UInt64Array};
        use arrow::compute;

        let table_path = base_dir.join(format!("{}.apex", table));
        if !table_path.exists() {
            return Ok(0);
        }

        // Scheduling a large initial build must not first pay the cost of
        // opening the full table/footer. Read only the fixed-size header here;
        // the worker performs the authoritative open and active-row scan.
        if allow_async {
            let estimated_count =
                crate::storage::TableStorageBackend::persisted_row_count_hint(&table_path)?
                    as usize;
            if estimated_count > 100_000 {
                let base_dir = base_dir.to_path_buf();
                let table_name = table.to_string();
                let fields = fields.map(|f| f.to_vec());
                let handle = std::thread::spawn({
                    let base_dir = base_dir.clone();
                    let table_name = table_name.clone();
                    move || {
                        let _ = Self::fts_backfill_table_sync(
                            &base_dir,
                            &table_name,
                            fields.as_deref(),
                            mgr,
                        );
                    }
                });
                crate::query::executor::register_fts_backfill_task(&base_dir, &table_name, handle);
                return Ok(estimated_count);
            }
        }

        let engine = mgr.get_engine(table).map_err(|e| err_data(e.to_string()))?;
        let persist_empty = || engine.compact().map_err(|e| err_data(e.to_string()));

        let storage = crate::storage::TableStorageBackend::open(&table_path)?;
        let schema = storage.get_schema();

        // Determine which string columns to index
        let string_cols: Vec<String> = if let Some(f) = fields {
            f.to_vec()
        } else {
            schema
                .iter()
                .filter(|(_, dt)| matches!(dt, crate::data::DataType::String))
                .map(|(n, _)| n.clone())
                .collect()
        };

        if string_cols.is_empty() {
            persist_empty()?;
            return Ok(0);
        }

        if let Some((doc_ids, column_data)) = storage.read_fts_string_columns_mmap(&string_cols)? {
            if doc_ids.is_empty() || column_data.is_empty() {
                persist_empty()?;
                return Ok(0);
            }

            let columns: Vec<(String, Vec<&str>)> = column_data
                .iter()
                .filter_map(|(col_name, data)| {
                    if let crate::storage::on_demand::ColumnData::String { offsets, data } = data {
                        let values = (0..doc_ids.len())
                            .map(|row_idx| {
                                if row_idx + 1 < offsets.len() {
                                    let start = offsets[row_idx] as usize;
                                    let end = offsets[row_idx + 1] as usize;
                                    if end <= data.len() && start <= end {
                                        // Storage writes valid UTF-8; skip repeat validation during FTS build.
                                        unsafe { std::str::from_utf8_unchecked(&data[start..end]) }
                                    } else {
                                        ""
                                    }
                                } else {
                                    ""
                                }
                            })
                            .collect();
                        Some((col_name.clone(), values))
                    } else {
                        None
                    }
                })
                .collect();

            if !columns.is_empty() {
                let count = doc_ids.len();
                let doc_ids: Vec<u64> = doc_ids.into_iter().map(u64::from).collect();
                engine
                    .add_documents_arrow_str(&doc_ids, columns)
                    .map_err(|e| err_data(e.to_string()))?;
                engine.flush_async().map_err(|e| err_data(e.to_string()))?;
                return Ok(count);
            }
        }

        // Read _id + string columns
        let mut col_names = vec!["_id".to_string()];
        col_names.extend(string_cols.iter().cloned());
        col_names.sort();
        col_names.dedup();
        let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

        let batch = storage.read_columns_to_arrow(Some(&col_refs), 0, None)?;
        if batch.num_rows() == 0 {
            persist_empty()?;
            return Ok(0);
        }

        // Extract row IDs
        let id_col = match batch.column_by_name("_id") {
            Some(c) => c,
            None => {
                persist_empty()?;
                return Ok(0);
            }
        };
        let ids: Vec<u64> = if let Some(arr) = id_col.as_any().downcast_ref::<UInt64Array>() {
            (0..arr.len()).map(|i| arr.value(i)).collect()
        } else if let Some(arr) = id_col.as_any().downcast_ref::<Int64Array>() {
            (0..arr.len()).map(|i| arr.value(i) as u64).collect()
        } else {
            persist_empty()?;
            return Ok(0);
        };

        let mut string_arrays: Vec<(String, arrow::array::ArrayRef)> = Vec::new();
        for col_name in &string_cols {
            if let Some(col) = batch.column_by_name(col_name) {
                if col.as_any().downcast_ref::<StringArray>().is_some() {
                    string_arrays.push((col_name.clone(), col.clone()));
                } else if let Ok(casted) = compute::cast(col, &ArrowDataType::Utf8) {
                    if casted.as_any().downcast_ref::<StringArray>().is_some() {
                        string_arrays.push((col_name.clone(), casted));
                    }
                }
            }
        }

        if string_arrays.is_empty() {
            persist_empty()?;
            return Ok(0);
        }

        let count = ids.len();
        let columns = string_arrays
            .iter()
            .map(|(col_name, col)| {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                let values = (0..count)
                    .map(|row_idx| {
                        if row_idx < arr.len() && !arr.is_null(row_idx) {
                            arr.value(row_idx)
                        } else {
                            ""
                        }
                    })
                    .collect();
                (col_name.clone(), values)
            })
            .collect();

        let doc_ids = ids;
        engine
            .add_documents_arrow_str(&doc_ids, columns)
            .map_err(|e| err_data(e.to_string()))?;
        engine.flush_async().map_err(|e| err_data(e.to_string()))?;
        Ok(count)
    }

    /// DROP FTS INDEX ON table — remove config entry and delete index files
    pub(super) fn execute_drop_fts_index(base_dir: &Path, table: &str) -> io::Result<ApexResult> {
        crate::query::executor::wait_fts_backfill(base_dir, table);

        // Update config
        let mut cfg = Self::read_fts_config(base_dir);
        if let Some(obj) = cfg.as_object_mut() {
            obj.remove(table);
        }
        Self::write_fts_config(base_dir, &cfg);

        // Remove from FTS manager if present
        if let Some(mgr) = crate::query::executor::get_fts_manager(base_dir) {
            let _ = mgr.remove_engine(table, true);
        }

        let msg = format!("FTS index dropped for table '{}'", table);
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "status",
            ArrowDataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(StringArray::from(vec![msg.as_str()]))],
        )
        .map_err(|e| err_data(e.to_string()))?;
        Ok(ApexResult::Data(batch))
    }

    /// ALTER FTS INDEX ON table ENABLE — mark enabled and back-fill missing rows
    pub(super) fn execute_alter_fts_index_enable(
        base_dir: &Path,
        table: &str,
    ) -> io::Result<ApexResult> {
        use crate::fts::{FtsConfig, FtsManager};

        // Update fts_config.json: set enabled = true
        let mut cfg = Self::read_fts_config(base_dir);
        let obj = cfg
            .as_object_mut()
            .ok_or_else(|| err_input("Corrupt fts_config.json"))?;
        if let Some(entry) = obj.get_mut(table) {
            if let Some(map) = entry.as_object_mut() {
                map.insert("enabled".to_string(), serde_json::Value::Bool(true));
            }
        } else {
            obj.insert(
                table.to_string(),
                serde_json::json!({
                    "enabled": true,
                    "index_fields": null,
                    "config": { "lazy_load": false, "cache_size": 10000 }
                }),
            );
        }
        Self::write_fts_config(base_dir, &cfg);

        // Re-read to get the per-table settings
        let cfg2 = Self::read_fts_config(base_dir);
        let entry = cfg2.as_object().and_then(|o| o.get(table));
        let lazy_load = entry
            .and_then(|e| e.get("config"))
            .and_then(|c| c.get("lazy_load"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let cache_size = entry
            .and_then(|e| e.get("config"))
            .and_then(|c| c.get("cache_size"))
            .and_then(|v| v.as_u64())
            .unwrap_or(10000) as usize;
        let fields: Option<Vec<String>> = entry
            .and_then(|e| e.get("index_fields"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            });

        // Get or create FTS manager
        let fts_dir = base_dir.join("fts_indexes");
        let fts_cfg = FtsConfig {
            lazy_load,
            cache_size,
            ..FtsConfig::default()
        };
        let mgr = {
            let existing = crate::query::executor::get_fts_manager(base_dir);
            match existing {
                Some(m) => m,
                None => {
                    let m = std::sync::Arc::new(FtsManager::new(&fts_dir, fts_cfg.clone()));
                    crate::query::executor::register_fts_manager(base_dir, m.clone());
                    m
                }
            }
        };
        mgr.configure_table(table, fts_cfg);

        // Back-fill all rows into FTS (safe to re-index already-indexed rows)
        let backfilled = Self::fts_backfill_table(base_dir, table, fields.as_deref(), mgr.clone())?;

        let msg = format!(
            "FTS index enabled for '{}' ({} rows indexed)",
            table, backfilled
        );
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "status",
            ArrowDataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(StringArray::from(vec![msg.as_str()]))],
        )
        .map_err(|e| err_data(e.to_string()))?;
        Ok(ApexResult::Data(batch))
    }

    /// ALTER FTS INDEX ON table DISABLE — mark disabled, keep files
    pub(super) fn execute_alter_fts_index_disable(
        base_dir: &Path,
        table: &str,
    ) -> io::Result<ApexResult> {
        let mut cfg = Self::read_fts_config(base_dir);
        if let Some(obj) = cfg.as_object_mut() {
            if let Some(entry) = obj.get_mut(table) {
                if let Some(map) = entry.as_object_mut() {
                    map.insert("enabled".to_string(), serde_json::Value::Bool(false));
                }
            }
        }
        Self::write_fts_config(base_dir, &cfg);

        let msg = format!(
            "FTS index disabled for table '{}' (index files kept)",
            table
        );
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "status",
            ArrowDataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(StringArray::from(vec![msg.as_str()]))],
        )
        .map_err(|e| err_data(e.to_string()))?;
        Ok(ApexResult::Data(batch))
    }

    /// SHOW FTS INDEXES — list FTS-configured tables for this database and all named sub-databases
    pub(super) fn execute_show_fts_indexes(base_dir: &Path) -> io::Result<ApexResult> {
        use arrow::array::{BooleanArray, Int64Array, StringArray};
        use arrow::datatypes::{Field, Schema};

        let mut databases: Vec<String> = Vec::new();
        let mut tables: Vec<String> = Vec::new();
        let mut enabled_list: Vec<bool> = Vec::new();
        let mut fields_list: Vec<String> = Vec::new();
        let mut lazy_list: Vec<bool> = Vec::new();
        let mut cache_list: Vec<i64> = Vec::new();

        // Helper closure to add entries from a config object
        let mut add_from_config =
            |db_name: &str, obj: &serde_json::Map<String, serde_json::Value>| {
                for (tname, entry) in obj {
                    let enabled = entry
                        .get("enabled")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let fields = entry
                        .get("index_fields")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|x| x.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or_else(|| "(all string cols)".to_string());
                    let inner = entry.get("config");
                    let lazy = inner
                        .and_then(|c| c.get("lazy_load"))
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let cache = inner
                        .and_then(|c| c.get("cache_size"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(10000);
                    databases.push(db_name.to_string());
                    tables.push(tname.clone());
                    enabled_list.push(enabled);
                    fields_list.push(fields);
                    lazy_list.push(lazy);
                    cache_list.push(cache);
                }
            };

        // Current database (default / root)
        let cfg = Self::read_fts_config(base_dir);
        if let Some(obj) = cfg.as_object() {
            add_from_config("default", obj);
        }

        // Named sub-databases (immediate subdirectories that have fts_config.json)
        if let Ok(entries) = std::fs::read_dir(base_dir) {
            let mut sub_dirs: Vec<(String, std::path::PathBuf)> = entries
                .flatten()
                .filter_map(|e| {
                    let p = e.path();
                    if !p.is_dir() {
                        return None;
                    }
                    let name = p.file_name()?.to_string_lossy().into_owned();
                    if name == "fts_indexes" {
                        return None;
                    }
                    let sub_cfg = p.join("fts_config.json");
                    if sub_cfg.exists() {
                        Some((name, p))
                    } else {
                        None
                    }
                })
                .collect();
            sub_dirs.sort_by(|a, b| a.0.cmp(&b.0));
            for (db_name, db_path) in sub_dirs {
                let sub_cfg = Self::read_fts_config(&db_path);
                if let Some(obj) = sub_cfg.as_object() {
                    add_from_config(&db_name, obj);
                }
            }
        }

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("database", ArrowDataType::Utf8, false),
            Field::new("table", ArrowDataType::Utf8, false),
            Field::new("enabled", ArrowDataType::Boolean, false),
            Field::new("fields", ArrowDataType::Utf8, false),
            Field::new("lazy_load", ArrowDataType::Boolean, false),
            Field::new("cache_size", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(databases)),
                std::sync::Arc::new(StringArray::from(tables)),
                std::sync::Arc::new(BooleanArray::from(enabled_list)),
                std::sync::Arc::new(StringArray::from(fields_list)),
                std::sync::Arc::new(BooleanArray::from(lazy_list)),
                std::sync::Arc::new(Int64Array::from(cache_list)),
            ],
        )
        .map_err(|e| err_data(e.to_string()))?;
        Ok(ApexResult::Data(batch))
    }
}
