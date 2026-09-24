// SELECT execution: fast paths, index scans, late materialization.

/// Represents a single scannable leaf predicate from an OR decomposition.
enum OrLeafPredicate {
    StringEq(String, String),       // (col, value)
    NumericRange(String, f64, f64), // (col, low, high) — covers =, >, >=, <, <=, BETWEEN
    NumericIn(String, Vec<i64>),    // (col, values)
    StringIn(String, Vec<String>),  // (col, values)
}

/// Records the generic executor route on drop when no fast path has claimed
/// the query first (architecture review R5: EXPLAIN ANALYZE physical path
/// trace).  `record_path` only writes an empty trace, so an earlier or nested
/// record wins.
struct GenericRouteGuard;

impl Drop for GenericRouteGuard {
    fn drop(&mut self) {
        crate::query::executor::record_path("generic_executor");
    }
}

impl ApexExecutor {
    /// Execute SELECT statement with base_dir for proper subquery table resolution
    fn execute_select_with_base_dir(
        mut stmt: SelectStatement,
        storage_path: &Path,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<ApexResult> {
        let (_, table_name) = crate::query::executor::base_dir_and_table_pub(storage_path);
        // Records the generic route if no fast path below claims the query
        // (architecture review R5: EXPLAIN ANALYZE physical path trace).
        let _path_guard = GenericRouteGuard;
        Self::resolve_fts_scores_in_statement(&mut stmt, base_dir, &table_name)?;

        // Resolve MATCH()/FUZZY_MATCH() once to compressed runtime bitmaps.
        if let Some(ref wc) = stmt.where_clause {
            if Self::expr_has_fts_match(wc) {
                let resolved = Self::resolve_fts_in_expr(
                    stmt.where_clause.take().unwrap(),
                    base_dir,
                    &table_name,
                )?;
                stmt.where_clause = Some(resolved);
            }
        }

        // FAST PATH: sparse-read deep OFFSET (ORDER BY low-card numeric, string LIMIT/OFFSET).
        if let Ok(Some(result)) = Self::try_fast_deep_offset(&stmt, storage_path) {
            crate::query::executor::record_path("fast_deep_offset");
            return Ok(result);
        }

        // FAST PATH: explode_rename(topk_distance(col,[q],k,'m'), "name1", "name2") FROM table
        // Single-pass O(n log k) topk that generates k rows with 2 user-named columns.
        if let Some((col, query, k, metric, names)) = Self::detect_topk_explode(&stmt) {
            let result = Self::execute_topk_explode(
                storage_path,
                col,
                query,
                k,
                metric,
                names,
                stmt.where_clause.as_ref(),
            )?;
            crate::query::executor::record_path("topk_explode");
            return Ok(ApexResult::Data(result));
        }

        // FAST PATH: Pure COUNT(*) without WHERE/GROUP BY - O(1) from metadata
        // Skip for TableFunction sources (read_csv/read_parquet/read_json) — no stored backend.
        // Also skip for TopkDistance — it has different row semantics.
        let from_is_table_fn = matches!(
            &stmt.from,
            Some(FromItem::TableFunction { .. })
                | Some(FromItem::TopkDistance { .. })
                | Some(FromItem::DirectFile { .. })
        );
        if !from_is_table_fn && Self::is_pure_count_star(&stmt) {
            crate::query::executor::record_path("count_star_metadata");
            let count = if let Some(batch) = get_cached_cte_batch(storage_path) {
                batch.num_rows() as i64
            } else if !crate::storage::engine::engine().table_exists(storage_path)
                && !crate::storage::table_catalog::file_exists_or_registered(storage_path)?
            {
                let tbl = storage_path
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy();
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", tbl),
                ));
            } else {
                let backend = get_cached_backend(storage_path)?;
                backend.active_row_count() as i64
            };

            let output_name =
                if let Some(SelectColumn::Aggregate { alias, .. }) = stmt.columns.first() {
                    alias.clone().unwrap_or_else(|| "COUNT(*)".to_string())
                } else {
                    "COUNT(*)".to_string()
                };
            let schema = Arc::new(Schema::new(vec![Field::new(
                &output_name,
                ArrowDataType::Int64,
                false,
            )]));
            let array: ArrayRef = Arc::new(Int64Array::from(vec![count]));
            let batch =
                RecordBatch::try_new(schema, vec![array]).map_err(|e| err_data(e.to_string()))?;
            return Ok(ApexResult::Data(batch));
        }

        // Check for derived table (FROM subquery) - resolve table path from subquery's FROM clause
        let cached_cte_batch = if matches!(&stmt.from, Some(FromItem::Table { .. })) {
            get_cached_cte_batch(storage_path)
        } else {
            None
        };
        let batch = if let Some(batch) = cached_cte_batch {
            crate::query::executor::record_path("cte_batch");
            batch
        } else {
            match &stmt.from {
                Some(FromItem::TopkDistance {
                    col,
                    query,
                    k,
                    metric,
                    ..
                }) => {
                    crate::query::executor::record_path("topk_distance");
                    Self::execute_topk_distance(storage_path, col, query, *k, metric)?
                },
                Some(FromItem::TableFunction {
                    func,
                    file,
                    options,
                    ..
                }) => {
                    crate::query::executor::record_path("table_function");
                    if func.eq_ignore_ascii_case("READ_CSV") {
                        if let Some(result) = Self::try_fast_csv_aggregation(
                            &stmt, file, options,
                        )? {
                            return Ok(result);
                        }
                    }
                    if let Some(count_batch) =
                        Self::try_fast_parquet_count_table_function(&stmt, func, file)?
                    {
                        return Ok(ApexResult::Data(count_batch));
                    }
                    if let Some(count_batch) =
                        Self::try_fast_json_count_table_function(&stmt, func, file)?
                    {
                        return Ok(ApexResult::Data(count_batch));
                    }
                    if let Some(count_batch) =
                        Self::try_fast_csv_count_table_function(&stmt, func, file, options)?
                    {
                        return Ok(ApexResult::Data(count_batch));
                    }
                    let mut opts = options.clone();
                    if let Some(ref wc) = stmt.where_clause {
                        if let Some(pushdown) = Self::try_extract_filter_for_pushdown(wc) {
                            opts.push(("filter".to_string(), pushdown));
                        }
                    }
                    if let Some(columns) = Self::get_col_refs(&stmt) {
                        opts.push(("columns".to_string(), columns.join("\u{1f}")));
                    }
                    let row_limit = Self::simple_file_row_limit(&stmt);
                    Self::read_table_function(func, file, &opts, row_limit)?
                }
                Some(FromItem::DirectFile { file, .. }) => {
                    crate::query::executor::record_path("direct_file");
                    let lower = file.to_lowercase();
                    if lower.ends_with(".csv") || lower.ends_with(".tsv") {
                        let options = if lower.ends_with(".tsv") {
                            vec![("delimiter".to_string(), "\t".to_string())]
                        } else {
                            Vec::new()
                        };
                        if let Some(result) = Self::try_fast_csv_aggregation(
                            &stmt, file, &options,
                        )? {
                            return Ok(result);
                        }
                    }
                    if let Some(count_batch) = Self::try_fast_csv_count_direct_file(&stmt, file)? {
                        return Ok(ApexResult::Data(count_batch));
                    }
                    let row_limit = Self::simple_file_row_limit(&stmt);
                    Self::read_direct_file(file, row_limit)?
                }
                Some(
                    FromItem::LateralExplode { .. }
                    | FromItem::LateralPosExplode { .. }
                    | FromItem::LateralStack { .. },
                ) => {
                    return Err(err_input("LATERAL VIEW requires a base FROM source"));
                }
                Some(FromItem::Subquery { stmt: sub_stmt, .. }) => match sub_stmt.as_ref() {
                    crate::query::SqlStatement::Select(sel) => {
                        let sub_path =
                            Self::resolve_from_table_path(sel, base_dir, default_table_path);
                        // Skip the probe when the derived source is an in-memory
                        // CTE batch (never on disk); the fall-through below reads
                        // it through the cache-aware path.
                        if matches!(&sel.from, Some(FromItem::Table { .. }))
                            && !has_cached_cte_batch(&sub_path)
                        {
                            let sub_backend = get_cached_backend(&sub_path)?;
                            if let Some(result) =
                                Self::try_fast_derived_case_group_by(&sub_backend, &stmt, sel)?
                            {
                                return Ok(result);
                            }
                        }
                        let sub_select = sel.clone();
                        // The `ROW_NUMBER() <= k` pushdown is disabled: it gathers
                        // the retained rows with `take` using positions that can
                        // reference rows outside the evaluated partition batch,
                        // which panics in arrow's take. The outer WHERE predicate
                        // is still applied after the subquery, so results stay
                        // correct; only the shortcut is lost.
                        let _ = &stmt.where_clause;
                        (if sel.joins.is_empty() {
                            Self::execute_select_with_base_dir(
                                sub_select,
                                &sub_path,
                                base_dir,
                                default_table_path,
                            )
                        } else {
                            Self::execute_select_with_joins(
                                sub_select,
                                base_dir,
                                default_table_path,
                            )
                        })?
                        .to_record_batch()?
                    }
                    crate::query::SqlStatement::Union(u) => {
                        Self::execute_union(u.clone(), base_dir, default_table_path)?
                            .to_record_batch()?
                    }
                    _ => return Err(err_input("Subquery must be SELECT or set operation")),
                },
                None => {
                    // No FROM clause (e.g., SELECT 1, 1) — create a single-row virtual batch
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        "_dummy",
                        ArrowDataType::Int64,
                        false,
                    )]));
                    RecordBatch::try_new(
                        schema,
                        vec![Arc::new(Int64Array::from(vec![0i64])) as ArrayRef],
                    )
                    .map_err(|e| err_data(e.to_string()))?
                }
                Some(FromItem::Table { .. }) => {
                    // Normal table - read from storage
                    if !crate::storage::engine::engine().table_exists(storage_path)
                        && !crate::storage::table_catalog::file_exists_or_registered(storage_path)?
                    {
                        let tbl = storage_path
                            .file_stem()
                            .unwrap_or_default()
                            .to_string_lossy();
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("Table '{}' does not exist", tbl),
                        ));
                    } else {
                        let backend = get_cached_backend(storage_path)?;
                        if let Some(where_clause) = stmt.where_clause.as_mut() {
                            Self::coerce_predicate_literals(where_clause, &backend)?;
                        }
                        if let Some(result) =
                            Self::try_fast_cached_distinct_projection(&backend, &stmt)?
                        {
                            crate::query::executor::record_path("fast_distinct_projection");
                            return Ok(result);
                        }

                        // Check if any SELECT column contains a scalar subquery
                        // Scalar subqueries may reference arbitrary columns, so read all
                        let has_scalar_subquery = stmt.columns.iter().any(|col| {
                            if let SelectColumn::Expression { expr, .. } = col {
                                Self::expr_contains_scalar_subquery(expr)
                            } else {
                                false
                            }
                        });

                        if backend.pending_v4_in_memory_rows() > 0 {
                            let col_refs = if has_scalar_subquery
                                || stmt.where_clause.is_some()
                                || backend.has_pending_deltas()
                                || backend.has_delta()
                            {
                                None
                            } else {
                                Self::get_col_refs(&stmt)
                            };
                            backend.read_columns_to_arrow(
                                col_refs
                                    .as_ref()
                                    .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                                    .as_deref(),
                                0,
                                None,
                            )?
                        } else if has_scalar_subquery {
                            let col_refs = Self::get_col_refs(&stmt);
                            backend.read_columns_to_arrow(
                                col_refs
                                    .as_ref()
                                    .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                                    .as_deref(),
                                0,
                                None,
                            )?
                        } else {
                            // Check conditions for late materialization optimization
                            let has_aggregation_check = stmt.columns.iter().any(|col| {
                            matches!(col, SelectColumn::Aggregate { .. })
                                || matches!(col, SelectColumn::Expression { expr, .. } if Self::expr_contains_aggregate(expr))
                        });

                            let has_window_func = stmt
                                .columns
                                .iter()
                                .any(|column| matches!(column, SelectColumn::WindowFunction { .. }));
                            if !has_aggregation_check
                                && !has_window_func
                                && stmt.where_clause.is_some()
                                && !stmt.order_by.is_empty()
                                && stmt.limit.is_some()
                            {
                                if let Some(result) =
                                    Self::try_fast_numeric_filter_order_topk(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_numeric_filter_topk");
return Ok(result);
                                }
                                if let Some(result) =
                                    Self::try_fast_not_null_order_topk(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_not_null_topk");
return Ok(result);
                                }
                            }

                            // FAST PATH: Direct aggregation for simple numeric aggregates
                            // Compute COUNT/SUM/AVG/MIN/MAX directly from V4 columns (mmap or in-memory)
                            if has_aggregation_check
                                && stmt.where_clause.is_none()
                                && stmt.group_by.is_empty()
                                && stmt.joins.is_empty()
                                && !backend.has_pending_deltas()
                                && !backend.has_delta()
                            {
                                if let Some(result) =
                                    Self::try_fast_count_distinct_scalars(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_count_distinct_scalars");
return Ok(result);
                                }
                                if let Some(result) =
                                    Self::try_fast_numeric_case_aggregation(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_numeric_case_aggregation");
return Ok(result);
                                }
                                if let Some(result) =
                                    Self::try_fast_null_count_aggregation(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_null_count_aggregation");
return Ok(result);
                                }
                                if let Some(result) = Self::try_mmap_aggregation(&backend, &stmt)? {
                                    crate::query::executor::record_path("mmap_aggregation");
                                    return Ok(result);
                                }
                            }

                            // FAST PATH: fused COUNT for NOT BETWEEN AND NOT LIKE
                            // (single mmap pass over both columns, no Arrow materialization).
                            if has_aggregation_check
                                && stmt.where_clause.is_some()
                                && stmt.group_by.is_empty()
                                && stmt.joins.is_empty()
                                && !backend.has_pending_deltas()
                                && !backend.has_delta()
                            {
                                if let Some(result) =
                                    Self::try_fast_not_filter_count(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_not_filter_count");
return Ok(result);
                                }
                            }

                            // FAST PATH: Filtered aggregation with string equality
                            // SELECT COUNT(*), AVG(col), MAX(col) FROM table WHERE str_col = 'val'
                            if has_aggregation_check
                                && stmt.where_clause.is_some()
                                && stmt.group_by.is_empty()
                                && stmt.joins.is_empty()
                                && stmt.limit.is_none()
                                && stmt.order_by.is_empty()
                                && !backend.has_pending_deltas()
                                && !backend.has_delta()
                            {
                                let table_has_indexes =
                                    Self::table_has_index_catalog(Some(base_dir), storage_path);
                                if table_has_indexes {
                                    if let Some(where_clause) = &stmt.where_clause {
                                        if let Some((filter_col, _)) =
                                            Self::extract_string_equality(where_clause)
                                        {
                                            let mut cols = Self::aggregate_input_columns(&stmt);
                                            if !cols.iter().any(|c| c == &filter_col) {
                                                cols.push(filter_col);
                                            }
                                            let col_refs: Vec<&str> =
                                                cols.iter().map(|s| s.as_str()).collect();
                                            let batch = backend.read_columns_to_arrow(
                                                Some(&col_refs),
                                                0,
                                                None,
                                            )?;
                                            let filtered =
                                                Self::apply_filter(&batch, where_clause)?;
                                            crate::query::executor::record_path("filtered_aggregation");
                                            return Self::execute_aggregation(&filtered, &stmt);
                                        }
                                    }
                                } else {
                                    if let Some(result) =
                                        Self::try_fast_filtered_string_agg(&backend, &stmt)?
                                    {
                                                                                crate::query::executor::record_path("fast_filtered_string_agg");
return Ok(result);
                                    }
                                }
                                if let Some(result) =
                                    Self::try_fast_filtered_numeric_agg(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_filtered_numeric_agg");
return Ok(result);
                                }
                                if let Some(result) =
                                    Self::try_fast_in_subquery_count(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_in_subquery_count");
return Ok(result);
                                }
                                if let Some(result) =
                                    Self::try_fast_dict_scalar_count(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_dict_scalar_count");
return Ok(result);
                                }
                                if let Some(result) =
                                    Self::try_fast_exists_count(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_exists_count");
return Ok(result);
                                }
                            }

                            // FAST PATH: fuse a numeric range filter with string GROUP BY and
                            // COUNT(*) / SUM / AVG over one numeric column in a single scan.
                            if has_aggregation_check
                                && stmt.where_clause.is_some()
                                && !stmt.group_by.is_empty()
                                && stmt.joins.is_empty()
                                && stmt.having.is_none()
                                && !backend.has_pending_deltas()
                                && !backend.has_delta()
                            {
                                if let Some(result) =
                                    Self::try_fast_numeric_filter_group_by(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_numeric_filter_group_by");
return Ok(result);
                                }
                            }

                            // FAST PATH: fuse a boolean predicate tree (numeric
                            // ranges, numeric IN, dictionary IN/Eq on the group
                            // column) with a single low-cardinality string GROUP
                            // BY and COUNT/SUM/AVG/MIN/MAX in one streaming mmap
                            // scan; HAVING/ORDER BY/LIMIT run on aggregated rows.
                            if has_aggregation_check
                                && stmt.where_clause.is_some()
                                && !stmt.group_by.is_empty()
                                && stmt.joins.is_empty()
                                && !backend.has_pending_deltas()
                                && !backend.has_delta()
                            {
                                if let Some(result) =
                                    Self::try_fast_fused_group_by(&backend, &stmt)?
                                {
                                                                        crate::query::executor::record_path("fast_fused_group_by");
return Ok(result);
                                }
                            }

                            // Correctness fallback for filtered aggregates when delta-backed
                            // rows are present. The generic full-batch path can miss string
                            // overlay state, but the string-filter reader already merges it.
                            if has_aggregation_check
                                && stmt.where_clause.is_some()
                                && stmt.group_by.is_empty()
                                && stmt.joins.is_empty()
                                && stmt.limit.is_none()
                                && stmt.order_by.is_empty()
                            {
                                if let Some(filtered) =
                                    Self::try_fast_string_filter_no_limit(&backend, &stmt)?
                                {
                                    crate::query::executor::record_path("filtered_aggregation");
                                    return Self::execute_aggregation(&filtered, &stmt);
                                }
                            }

                            // If a dictionary string filter is provably true for every active
                            // row (common partition column shape), skip materializing/filtering it.
                            if has_aggregation_check
                                && stmt.where_clause.is_some()
                                && !stmt.group_by.is_empty()
                                && stmt.joins.is_empty()
                                && stmt.having.is_none()
                                && stmt.limit.is_none()
                                && stmt.offset.is_none()
                                && stmt.order_by.is_empty()
                                && !backend.has_pending_deltas()
                                && !backend.has_delta()
                            {
                                if let Some(where_clause) = &stmt.where_clause {
                                    if let Some((filter_col, filter_value)) =
                                        Self::extract_string_equality(where_clause)
                                    {
                                        if backend.string_eq_matches_all(&filter_col, &filter_value)?
                                        {
                                            let mut grouped_stmt = stmt.clone();
                                            grouped_stmt.where_clause = None;
                                            if let Some(result) =
                                                Self::try_fast_native_string_group_by(
                                                    &backend,
                                                    &grouped_stmt,
                                                )?
                                            {
                                                crate::query::executor::record_path("fast_native_string_group_by");
                                                return Ok(result);
                                            }
                                            let col_refs = Self::get_col_refs(&grouped_stmt);
                                            let col_refs_vec: Option<Vec<&str>> = col_refs
                                                .as_ref()
                                                .map(|v| v.iter().map(|s| s.as_str()).collect());
                                            let batch = backend.read_columns_to_arrow(
                                                col_refs_vec.as_deref(),
                                                0,
                                                None,
                                            )?;
                                            return Self::execute_group_by(&batch, &grouped_stmt);
                                        }
                                    }
                                }
                            }

                            // GROUP BY + simple string equality can filter at the storage layer
                            // and aggregate immediately, avoiding materializing the filter column
                            // only to apply the same WHERE mask again in Arrow.
                            if has_aggregation_check
                                && stmt.where_clause.is_some()
                                && !stmt.group_by.is_empty()
                                && stmt.joins.is_empty()
                                && stmt.having.is_none()
                                && stmt.limit.is_none()
                                && stmt.offset.is_none()
                                && stmt.order_by.is_empty()
                                && !backend.has_pending_deltas()
                                && !backend.has_delta()
                                && !stmt.columns.iter().any(|column| {
                                    matches!(
                                        column,
                                        SelectColumn::Aggregate {
                                            distinct: true,
                                            ..
                                        }
                                    )
                                })
                            {
                                if let Some(where_clause) = &stmt.where_clause {
                                    if let Some((filter_col, filter_value)) =
                                        Self::extract_string_equality(where_clause)
                                    {
                                        if Self::column_is_string(&backend, &filter_col) {
                                            let mut grouped_stmt = stmt.clone();
                                            grouped_stmt.where_clause = None;
                                            let col_refs = Self::get_col_refs(&grouped_stmt);
                                            let col_refs_vec: Option<Vec<&str>> = col_refs
                                                .as_ref()
                                                .map(|v| v.iter().map(|s| s.as_str()).collect());
                                            let filtered = backend
                                                .read_columns_filtered_string_to_arrow(
                                                    col_refs_vec.as_deref(),
                                                    &filter_col,
                                                    &filter_value,
                                                    true,
                                                )?;
                                            crate::query::executor::record_path("storage_string_eq_group_by");
                                            return Self::execute_group_by(
                                                &filtered,
                                                &grouped_stmt,
                                            );
                                        }
                                    }
                                }
                            }

                            // Late Materialization for WHERE: with WHERE (no ORDER BY)
                            // Works for both SELECT * and projected column queries.
                            let where_cols = stmt.where_columns();
                            let has_window_func = stmt
                                .columns
                                .iter()
                                .any(|col| matches!(col, SelectColumn::WindowFunction { .. }));
                            let can_late_materialize_where = stmt.where_clause.is_some()
                                && stmt.order_by.is_empty()
                                && stmt.group_by.is_empty()
                                && !has_aggregation_check
                                && !has_window_func
                                && !where_cols.is_empty();

                            // Late Materialization for ORDER BY: SELECT * with ORDER BY + LIMIT (no WHERE)
                            let can_late_materialize_order = stmt.is_select_star()
                                && stmt.where_clause.is_none()
                                && !stmt.order_by.is_empty()
                                && stmt.limit.is_some()
                                && stmt.group_by.is_empty()
                                && !has_aggregation_check;

                            // EARLY FAST PATH: _id = X point lookup — skip CBO entirely
                            if !has_scalar_subquery
                                && stmt.group_by.is_empty()
                                && !has_aggregation_check
                                && !backend.has_pending_deltas()
                                && !backend.has_delta()
                            {
                                if let Some(ref where_clause) = stmt.where_clause {
                                    if let Some(id) = Self::extract_id_equality_filter(where_clause)
                                    {
                                        if let Some(row_batch) =
                                            backend.read_row_by_id_to_arrow(id)?
                                        {
                                            crate::query::executor::record_path("id_point_lookup");
                                            let projected = Self::apply_projection_with_storage(
                                                &row_batch,
                                                &stmt.columns,
                                                Some(storage_path),
                                            )?;
                                            return Ok(ApexResult::Data(projected));
                                        }
                                    }
                                }
                            }

                            // CBO: the planner's chosen strategy directly drives the physical
                            // index route (architecture review R5.2). Planning is skipped for:
                            // (a) no WHERE clause, (b) table has no indexes.
                            let (
                                plan_uses_index_route,
                                plan_uses_secondary_index,
                                plan_index_spec,
                            ) = if stmt.where_clause.is_none() {
                                (false, false, None)
                            } else {
                                    let (bd, tname) = base_dir_and_table(storage_path);
                                    let idx_mgr_arc = get_index_manager(&bd, &tname);
                                    let idx_mgr = idx_mgr_arc.lock();
                                    // Fast exit: if table has no indexes, the plan can only choose a scan
                                    if idx_mgr.catalog_is_empty() {
                                        (false, false, None)
                                    } else {
                                        let table_key = storage_path.to_string_lossy();
                                        let cbo_plan = QueryPlanner::plan_select_details(
                                            &stmt,
                                            Some(&*idx_mgr),
                                            &table_key,
                                            Self::planner_context(&backend, stmt.where_clause.as_ref()),
                                        );
                                        (
                                            matches!(
                                                cbo_plan.strategy,
                                                ExecutionStrategy::OltpIndexLookup { .. }
                                                    | ExecutionStrategy::OltpPrimaryKey { .. }
                                            ),
                                            matches!(
                                                cbo_plan.strategy,
                                                ExecutionStrategy::OltpIndexLookup { .. }
                                            ),
                                            cbo_plan.execution,
                                        )
                                    }
                                };

                            // FAST PATH INDEX: taken exactly when the plan chose the index route
                            if plan_uses_index_route {
                                if let Some(ref where_clause) = stmt.where_clause {
                                    if let Some(result) = Self::try_index_accelerated_read(
                                        &backend,
                                        &stmt,
                                        where_clause,
                                        plan_index_spec.as_ref(),
                                        base_dir,
                                        storage_path,
                                    )? {
                                        crate::query::executor::record_path("index_accelerated_read");
                                        return Ok(result);
                                    }
                                    if plan_uses_secondary_index {
                                        crate::query::executor::record_plan_divergence(
                                            "plan chose index access; index route unavailable at execution; fell back to scan",
                                        );
                                    }
                                }
                            }

                            // FAST PATH 0: Check for _id = X pattern (O(1) lookup)
                            if let Some(where_clause) = &stmt.where_clause {
                                if let Some(id) = Self::extract_id_equality_filter(where_clause) {
                                    if !backend.has_pending_deltas() && !backend.has_delta() {
                                        if let Some(batch) = backend.read_row_by_id_to_arrow(id)? {
                                            crate::query::executor::record_path("id_point_lookup");
                                            batch
                                        } else {
                                            // Not in memory — fall through to general mmap → Arrow → WHERE filter path
                                            let batch =
                                                backend.read_columns_to_arrow(None, 0, None)?;
                                            if batch.num_rows() == 0 {
                                                backend.read_columns_to_arrow(None, 0, Some(0))?
                                            } else {
                                                // Apply WHERE filter on the mmap-read batch
                                                let filtered = Self::apply_filter_with_storage(
                                                    &batch,
                                                    where_clause,
                                                    storage_path,
                                                )?;
                                                if filtered.num_rows() == 0 {
                                                    return Ok(ApexResult::Empty(
                                                        filtered.schema(),
                                                    ));
                                                }
                                                return Ok(ApexResult::Data(
                                                    Self::apply_projection_with_storage(
                                                        &filtered,
                                                        &stmt.columns,
                                                        Some(storage_path),
                                                    )?,
                                                ));
                                            }
                                        }
                                    } else {
                                        // Pending DeltaStore updates must be merged through the full scan path.
                                        let batch = backend.read_columns_to_arrow(None, 0, None)?;
                                        if batch.num_rows() == 0 {
                                            backend.read_columns_to_arrow(None, 0, Some(0))?
                                        } else {
                                            let filtered = Self::apply_filter_with_storage(
                                                &batch,
                                                where_clause,
                                                storage_path,
                                            )?;
                                            if filtered.num_rows() == 0 {
                                                return Ok(ApexResult::Empty(filtered.schema()));
                                            }
                                            return Ok(ApexResult::Data(
                                                Self::apply_projection_with_storage(
                                                    &filtered,
                                                    &stmt.columns,
                                                    Some(storage_path),
                                                )?,
                                            ));
                                        }
                                    }
                                } else if let Some(result) =
                                    Self::try_fast_filter_group_order(&backend, &stmt)?
                                {
                                    // FAST PATH for Complex (Filter+Group+Order) - biggest optimization
                                    return Ok(result);
                                } else if can_late_materialize_where {
                                    // FAST PATH 1: Try dictionary-based filter for simple string equality (with LIMIT)
                                    if let Some(result) =
                                        Self::try_fast_string_filter(&backend, &stmt)?
                                    {
                                        result
                                    // FAST PATH 1b: String equality without LIMIT - storage-level scan
                                    } else if let Some(result) =
                                        Self::try_fast_string_filter_no_limit(&backend, &stmt)?
                                    {
                                        if !stmt.is_pure_star() {
                                            let projected = Self::apply_projection_with_storage(
                                                &result,
                                                &stmt.columns,
                                                Some(storage_path),
                                            )?;
                                            return Ok(ApexResult::Data(projected));
                                        }
                                        return Ok(ApexResult::Data(result));
                                    // FAST PATH 1c: LIKE pattern scan (prefix/suffix/contains)
                                    } else if let Some(result) =
                                        Self::try_fast_like_filter(&backend, &stmt)?
                                    {
                                        if !stmt.is_pure_star() {
                                            let projected = Self::apply_projection_with_storage(
                                                &result,
                                                &stmt.columns,
                                                Some(storage_path),
                                            )?;
                                            return Ok(ApexResult::Data(projected));
                                        }
                                        return Ok(ApexResult::Data(result));
                                    // FAST PATH 2: Try numeric range filter for BETWEEN
                                    } else if let Some(result) =
                                        Self::try_fast_numeric_range_filter(&backend, &stmt)?
                                    {
                                        // The scan consumed the whole WHERE
                                        // (single comparison/BETWEEN), so the
                                        // batch is final except for OFFSET.
                                        let offset = stmt.offset.unwrap_or(0);
                                        let limited = if offset > 0
                                            && result.num_rows() > offset
                                        {
                                            result.slice(
                                                offset,
                                                result.num_rows() - offset,
                                            )
                                        } else {
                                            result
                                        };
                                        if !stmt.is_pure_star() {
                                            let projected =
                                                Self::apply_projection_with_storage(
                                                    &limited,
                                                    &stmt.columns,
                                                    Some(storage_path),
                                                )?;
                                            return Ok(ApexResult::Data(projected));
                                        }
                                        return Ok(ApexResult::Data(limited));
                                    // FAST PATH 3: Try combined string + numeric filter for multi-condition
                                    } else if let Some(result) =
                                        Self::try_fast_multi_condition_filter(&backend, &stmt)?
                                    {
                                        result
                                    // FAST PATH 4: Mmap multi-condition AND on two different numeric columns
                                    } else if let Some(result) =
                                        Self::try_fast_mmap_multi_condition(
                                            &backend,
                                            &stmt,
                                            storage_path,
                                        )?
                                    {
                                        return Ok(result);
                                    // FAST PATH 5: Mmap IN filter on string column
                                    } else if let Some(result) = Self::try_fast_mmap_in_filter(
                                        &backend,
                                        &stmt,
                                        storage_path,
                                    )? {
                                        return Ok(result);
                                    } else if backend.is_mmap_only()
                                        && !backend.has_pending_deltas()
                                        && !backend.has_delta()
                                    {
                                        // MMAP FAST PATH: byte-level scan + point lookups
                                        if let Some(where_clause) = &stmt.where_clause {
                                            let _limit_with_off =
                                                stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
                                            let (matching_indices, prefer_index_materialization) =
                                                if let Some((col, val)) =
                                                    Self::extract_string_equality(where_clause)
                                                {
                                                    (
                                                        backend.scan_string_filter_mmap(
                                                            &col,
                                                            &val,
                                                            _limit_with_off,
                                                        )?,
                                                        false,
                                                    )
                                                } else if let Some((col, low, high)) =
                                                    Self::extract_between_range(where_clause)
                                                {
                                                    (
                                                        backend.scan_numeric_range_mmap(
                                                            &col,
                                                            low,
                                                            high,
                                                            _limit_with_off,
                                                        )?,
                                                        false,
                                                    )
                                                } else if let Some((col, low, high)) =
                                                    Self::extract_two_sided_same_col_range(
                                                        where_clause,
                                                    )
                                                {
                                                    // col >= N AND col <= M — logically equivalent to BETWEEN
                                                    (
                                                        backend.scan_numeric_range_mmap(
                                                            &col,
                                                            low,
                                                            high,
                                                            _limit_with_off,
                                                        )?,
                                                        false,
                                                    )
                                                } else if let Some((col, low, high)) =
                                                    Self::extract_single_comparison_as_range(
                                                        where_clause,
                                                    )
                                                {
                                                    (
                                                        backend.scan_numeric_range_mmap(
                                                            &col,
                                                            low,
                                                            high,
                                                            _limit_with_off,
                                                        )?,
                                                        false,
                                                    )
                                                } else if let Some((col, values)) =
                                                    Self::extract_in_string_filter(where_clause)
                                                {
                                                    (
                                                        backend.scan_string_in_mmap(
                                                            &col,
                                                            &values,
                                                            _limit_with_off,
                                                        )?,
                                                        true,
                                                    )
                                                } else if let Some((col, nums)) =
                                                    Self::extract_in_numeric_filter(where_clause)
                                                        .or_else(|| {
                                                            Self::extract_or_numeric_equalities(
                                                                where_clause,
                                                            )
                                                        })
                                                {
                                                    // Numeric IN or OR-of-equalities: single-pass mmap scan
                                                    (
                                                        backend.scan_numeric_in_mmap(
                                                            &col,
                                                            &nums,
                                                            _limit_with_off,
                                                        )?,
                                                        true,
                                                    )
                                                } else if let Some(leaves) =
                                                    Self::extract_or_leaf_predicates(where_clause)
                                                {
                                                    // General OR decomposition: scan each leaf, union indices
                                                    match Self::scan_or_leaves_mmap(
                                                        &backend,
                                                        &leaves,
                                                        _limit_with_off,
                                                    )? {
                                                        Some(v) if !v.is_empty() => (Some(v), true),
                                                        Some(_) => (None, true), // empty result
                                                        None => (None, true),
                                                    }
                                                } else {
                                                    (None, false)
                                            };
                                            if let Some(indices) = matching_indices {
                                                if has_aggregation_check
                                                    && stmt.group_by.is_empty()
                                                    && stmt.joins.is_empty()
                                                {
                                                    let agg_cols =
                                                        Self::aggregate_input_columns(&stmt);
                                                    let agg_col_refs: Vec<&str> = agg_cols
                                                        .iter()
                                                        .map(|s| s.as_str())
                                                        .collect();
                                                    let batch = if indices.is_empty() {
                                                        backend.read_columns_to_arrow(
                                                            Some(&agg_col_refs),
                                                            0,
                                                            Some(0),
                                                        )?
                                                    } else if backend.is_mmap_only()
                                                        && !Self::should_use_scatter_read(
                                                            backend.row_count() as usize,
                                                            indices.len(),
                                                        )
                                                    {
                                                        let full_batch = backend
                                                            .read_columns_to_arrow(
                                                                Some(&agg_col_refs),
                                                                0,
                                                                None,
                                                            )?;
                                                        Self::take_rows_from_full_batch(
                                                            &full_batch,
                                                            &indices,
                                                        )?
                                                    } else {
                                                        backend.read_columns_by_indices_to_arrow(
                                                            &indices,
                                                            Some(&agg_col_refs),
                                                        )?
                                                    };
                                                    return Self::execute_aggregation(&batch, &stmt);
                                                }

                                                let batch = if prefer_index_materialization {
                                                    Self::read_matching_rows_by_indices(
                                                        &backend, &stmt, &indices,
                                                    )?
                                                } else {
                                                    Self::read_matching_rows_adaptive(
                                                        &backend, &stmt, &indices,
                                                    )?
                                                };

                                                // Apply ORDER BY with LIMIT if needed
                                                if !stmt.order_by.is_empty() {
                                                    let k = stmt
                                                        .limit
                                                        .map(|l| l + stmt.offset.unwrap_or(0));
                                                    let sort_batch =
                                                        Self::augment_batch_for_order_by(
                                                            &batch,
                                                            &stmt.columns,
                                                            &stmt.order_by,
                                                        )?;
                                                    let sorted = Self::apply_order_by_topk(
                                                        &sort_batch,
                                                        &stmt.order_by,
                                                        k,
                                                    )?;
                                                    let limited = Self::apply_limit_offset(
                                                        &sorted,
                                                        stmt.limit,
                                                        stmt.offset,
                                                    )?;
                                                    let projected =
                                                        Self::apply_projection_with_storage(
                                                            &limited,
                                                            &stmt.columns,
                                                            Some(storage_path),
                                                        )?;
                                                    return Ok(ApexResult::Data(projected));
                                                }

                                                if !stmt.is_pure_star() {
                                                    let projected =
                                                        Self::apply_projection_with_storage(
                                                            &batch,
                                                            &stmt.columns,
                                                            Some(storage_path),
                                                        )?;
                                                    return Ok(ApexResult::Data(projected));
                                                }
                                                return Ok(ApexResult::Data(batch));
                                            }
                                        }
                                        let filtered = Self::execute_with_late_materialization(
                                            &backend,
                                            &stmt,
                                            storage_path,
                                        )?;
                                        if filtered.num_rows() == 0 {
                                            return Ok(ApexResult::Empty(filtered.schema()));
                                        }
                                        if has_aggregation_check
                                            && stmt.group_by.is_empty()
                                            && stmt.joins.is_empty()
                                        {
                                            return Self::execute_aggregation(&filtered, &stmt);
                                        }
                                        if !stmt.is_pure_star() {
                                            let projected = Self::apply_projection_with_storage(
                                                &filtered,
                                                &stmt.columns,
                                                Some(storage_path),
                                            )?;
                                            return Ok(ApexResult::Data(projected));
                                        }
                                        return Ok(ApexResult::Data(filtered));
                                    } else {
                                        // Late materialization for WHERE path
                                        // Return directly to avoid applying WHERE filter twice
                                        let filtered = Self::execute_with_late_materialization(
                                            &backend,
                                            &stmt,
                                            storage_path,
                                        )?;
                                        if filtered.num_rows() == 0 {
                                            return Ok(ApexResult::Empty(filtered.schema()));
                                        }
                                        if has_aggregation_check
                                            && stmt.group_by.is_empty()
                                            && stmt.joins.is_empty()
                                        {
                                            return Self::execute_aggregation(&filtered, &stmt);
                                        }
                                        if !stmt.is_pure_star() {
                                            let projected = Self::apply_projection_with_storage(
                                                &filtered,
                                                &stmt.columns,
                                                Some(storage_path),
                                            )?;
                                            return Ok(ApexResult::Data(projected));
                                        }
                                        return Ok(ApexResult::Data(filtered));
                                    }
                                } else if !stmt.group_by.is_empty() {
                                    // GROUP BY with WHERE: use dict-encoded path for faster string aggregation
                                    let col_refs = Self::get_col_refs(&stmt);
                                    let col_refs_vec: Option<Vec<&str>> = col_refs
                                        .as_ref()
                                        .map(|v| v.iter().map(|s| s.as_str()).collect());
                                    // NULL group keys force a plain read: the
                                    // dictionary fast paths assume no NULLs.
                                    let group_has_nulls = stmt
                                        .group_by
                                        .iter()
                                        .any(|col| {
                                            let clean = col
                                                .trim_matches('"')
                                                .rsplit('.')
                                                .next()
                                                .unwrap_or(col.trim_matches('"'));
                                            backend.column_has_nulls(clean)
                                        });
                                    // Expression/aggregate-only projections
                                    // (e.g. COUNT(CASE ...)) run the generic
                                    // with-indices path; a plain read matches
                                    // the pre-dictionary behaviour and avoids
                                    // per-row dict-value hashing on the key.
                                    let has_expression = stmt.columns.iter().any(|column| {
                                        matches!(column, SelectColumn::Expression { .. })
                                    });
                                    if group_has_nulls || has_expression {
                                        backend.read_columns_to_arrow(
                                            col_refs_vec.as_deref(),
                                            0,
                                            None,
                                        )?
                                    } else {
                                        let table_refs: Option<Vec<&str>> = match &col_refs_vec {
                                            Some(refs) => {
                                                Self::filter_columns_for_backend(&backend, refs)
                                            }
                                            None => None,
                                        };
                                        backend
                                            .read_columns_to_arrow_dict(table_refs.as_deref())?
                                    }
                                } else if let Some(batch) =
                                    Self::try_numeric_predicate_pushdown(&backend, &stmt)?
                                {
                                    batch
                                } else {
                                    let col_refs = Self::get_col_refs(&stmt);
                                    let col_refs_vec: Option<Vec<&str>> = col_refs
                                        .as_ref()
                                        .map(|v| v.iter().map(|s| s.as_str()).collect());
                                    let table_refs: Option<Vec<&str>> = match &col_refs_vec {
                                        Some(refs) => {
                                            Self::filter_columns_for_backend(&backend, refs)
                                        }
                                        None => None,
                                    };
                                    backend.read_columns_to_arrow_dict(table_refs.as_deref())?
                                }
                            } else if can_late_materialize_where {
                                // FAST PATH 1: Try dictionary-based filter for simple string equality
                                if let Some(result) = Self::try_fast_string_filter(&backend, &stmt)?
                                {
                                    result
                                // FAST PATH 1b: No-LIMIT string filter (uses mmap scan + late materialization)
                                } else if let Some(result) =
                                    Self::try_fast_string_filter_no_limit(&backend, &stmt)?
                                {
                                    result
                                // FAST PATH 1c: LIKE pattern scan (prefix/suffix/contains)
                                } else if let Some(result) =
                                    Self::try_fast_like_filter(&backend, &stmt)?
                                {
                                    result
                                // FAST PATH 2: Try numeric range filter for BETWEEN
                                } else if let Some(result) =
                                    Self::try_fast_numeric_range_filter(&backend, &stmt)?
                                {
                                    // The scan consumed the whole WHERE; the
                                    // batch is final except for OFFSET.
                                    let offset = stmt.offset.unwrap_or(0);
                                    let limited = if offset > 0
                                        && result.num_rows() > offset
                                    {
                                        result.slice(offset, result.num_rows() - offset)
                                    } else {
                                        result
                                    };
                                    if !stmt.is_pure_star() {
                                        let projected =
                                            Self::apply_projection_with_storage(
                                                &limited,
                                                &stmt.columns,
                                                Some(storage_path),
                                            )?;
                                        return Ok(ApexResult::Data(projected));
                                    }
                                    return Ok(ApexResult::Data(limited));
                                // FAST PATH 3: Try combined string + numeric filter for multi-condition
                                } else if let Some(result) =
                                    Self::try_fast_multi_condition_filter(&backend, &stmt)?
                                {
                                    result
                                } else {
                                    // Late materialization for SELECT * WHERE path
                                    Self::execute_with_late_materialization(
                                        &backend,
                                        &stmt,
                                        storage_path,
                                    )?
                                }
                            } else if stmt.where_clause.is_some() && stmt.limit.is_none() {
                                // FAST PATH: String filter without LIMIT (uses dictionary scan)
                                if let Some(result) =
                                    Self::try_fast_string_filter_no_limit(&backend, &stmt)?
                                {
                                    result
                                // FAST PATH: LIKE pattern scan (prefix/suffix/contains)
                                } else if let Some(result) =
                                    Self::try_fast_like_filter(&backend, &stmt)?
                                {
                                    result
                                } else if let Some(batch) =
                                    Self::try_numeric_predicate_pushdown(&backend, &stmt)?
                                {
                                    batch
                                } else {
                                    let col_refs = Self::get_col_refs(&stmt);
                                    let col_refs_vec: Option<Vec<&str>> = col_refs
                                        .as_ref()
                                        .map(|v| v.iter().map(|s| s.as_str()).collect());
                                    let table_refs: Option<Vec<&str>> = match &col_refs_vec {
                                        Some(refs) => {
                                            Self::filter_columns_for_backend(&backend, refs)
                                        }
                                        None => None,
                                    };
                                    backend.read_columns_to_arrow_dict(table_refs.as_deref())?
                                }
                            } else if can_late_materialize_order {
                                // Late materialization for ORDER BY + LIMIT path
                                Self::execute_with_order_late_materialization(&backend, &stmt)?
                            } else {
                                let col_refs = Self::get_col_refs(&stmt);
                                let col_refs_vec: Option<Vec<&str>> = col_refs
                                    .as_ref()
                                    .map(|v| v.iter().map(|s| s.as_str()).collect());
                                let can_pushdown_limit = stmt.where_clause.is_none()
                                    && stmt.order_by.is_empty()
                                    && stmt.group_by.is_empty()
                                    && !has_aggregation_check;

                                // Note: V4 fast agg disabled - Arrow clone+SIMD outperforms due to cache warming

                                if !stmt.group_by.is_empty() {
                                    if let Some(result) =
                                        Self::try_fast_cached_transform_group_by(&backend, &stmt)?
                                    {
                                        crate::query::executor::record_path("fast_transform_group_by");
                                        return Ok(result);
                                    }
                                    if let Some(result) =
                                        Self::try_fast_cached_ratio_group_by(&backend, &stmt)?
                                    {
                                        crate::query::executor::record_path("fast_ratio_group_by");
                                        return Ok(result);
                                    }
                                    // V4 FAST PATH: integral GROUP BY keys with the
                                    // full numeric aggregate family.
                                    if let Some(result) =
                                        Self::try_fast_numeric_group_by(&backend, &stmt)?
                                    {
                                        crate::query::executor::record_path("fast_numeric_group_by");
                                        return Ok(result);
                                    }
                                    // Mmap-native string groups for COUNT/DISTINCT/CASE/MAX,
                                    // including HAVING and ordered/limited result sets.
                                    if let Some(result) =
                                        Self::try_fast_native_string_group_by(&backend, &stmt)?
                                    {
                                        crate::query::executor::record_path("fast_native_string_group_by");
                                        return Ok(result);
                                    }
                                    if let Some(result) =
                                        Self::try_fast_cached_numeric_case_group_by(&backend, &stmt)?
                                    {
                                        crate::query::executor::record_path("fast_numeric_case_group_by");
                                        return Ok(result);
                                    }
                                    // V4 FAST PATH: Cached GROUP BY
                                    if let Some(result) =
                                        Self::try_fast_cached_group_by(&backend, &stmt)?
                                    {
                                        crate::query::executor::record_path("fast_cached_group_by");
                                        return Ok(result);
                                    }
                                    // V4 FAST PATH: COUNT(CASE WHEN range THEN 1 END)
                                    // over a cached string group, no WHERE.
                                    if let Some(result) =
                                        Self::try_fast_cached_case_count(&backend, &stmt)?
                                    {
                                        crate::query::executor::record_path("fast_cached_case_count");
                                        return Ok(result);
                                    }
                                    // Fallback: dict-encoded Arrow path
                                    let table_refs = col_refs_vec.as_ref().and_then(|refs| {
                                        Self::filter_columns_for_backend(&backend, refs)
                                    });
                                    backend.read_columns_to_arrow_dict(table_refs.as_deref())?
                                } else {
                                    // V4 FAST PATH: ORDER BY LENGTH(col) [DESC], <tie> LIMIT n
                                    // scanned from the mmap string offsets.
                                    if let Some(result) =
                                        Self::try_fast_order_by_length(&backend, &stmt, storage_path)?
                                    {
                                        crate::query::executor::record_path("fast_order_by_length");
                                        return Ok(result);
                                    }
                                    let _row_limit = if can_pushdown_limit {
                                        stmt.limit.map(|l| l + stmt.offset.unwrap_or(0))
                                    } else {
                                        None
                                    };
                                    if can_pushdown_limit {
                                        backend.read_columns_to_arrow(
                                            col_refs_vec.as_deref(),
                                            0,
                                            _row_limit,
                                        )?
                                    } else {
                                        // Dictionary-encoded reads make string
                                        // filters/expressions evaluate over the
                                        // distinct values; projections decode
                                        // back to plain arrays when needed.
                                        backend.read_columns_to_arrow_dict(col_refs_vec.as_deref())?
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        // Determine row limit for early termination

        // Check for aggregation BEFORE checking empty batch
        // Aggregations like COUNT(*) should return 0 for empty tables
        // Also check for aggregates inside expressions (e.g., CASE WHEN SUM(x) > 100 ...)
        let has_aggregation = stmt.columns.iter().any(|col| match col {
            SelectColumn::Aggregate { .. } => true,
            SelectColumn::Expression { expr, .. } => Self::expr_contains_aggregate(expr),
            _ => false,
        });

        if batch.num_rows() == 0 {
            // For aggregations on empty tables, still execute aggregation (COUNT(*) returns 0)
            if has_aggregation && stmt.group_by.is_empty() {
                return Self::execute_aggregation(&batch, &stmt);
            }
            // Empty relations still have the SELECT-list schema.  Returning the
            // input schema here drops literal/expression aliases from an empty
            // CTE and makes a later LEFT JOIN unable to resolve those columns.
            let projected = if stmt.is_pure_star() {
                batch
            } else {
                Self::apply_projection_with_storage(&batch, &stmt.columns, Some(storage_path))?
            };
            return Ok(ApexResult::Empty(projected.schema()));
        }

        // Apply WHERE filter (with storage path for subquery support)
        // Pure COUNT(*) with WHERE: evaluate the mask and count directly,
        // skipping the filtered-batch materialization.
        let filtered = if let Some(ref where_clause) = stmt.where_clause {
            if Self::is_simple_count_star(&stmt) {
                let mask =
                    Self::evaluate_predicate_with_storage(&batch, where_clause, storage_path)?;
                // Count the true bits directly: per-row BooleanArray access is
                // far slower than the packed-buffer count.
                let count = if mask.null_count() == 0 {
                    mask.values().count_set_bits() as i64
                } else {
                    let mut count: i64 = 0;
                    for i in 0..mask.len() {
                        if !mask.is_null(i) && mask.value(i) {
                            count += 1;
                        }
                    }
                    count
                };
                // Match execute_aggregation's output shape: a one-row Data
                // batch carrying the declared alias.
                let mut fields = Vec::with_capacity(stmt.columns.len());
                let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());
                for col in &stmt.columns {
                    if let SelectColumn::Aggregate { alias, column, .. } = col {
                        let output = alias.clone().unwrap_or_else(|| {
                            format!("COUNT({})", column.as_deref().unwrap_or("*"))
                        });
                        fields.push(Field::new(output, ArrowDataType::Int64, false));
                        arrays.push(Arc::new(Int64Array::from(vec![count])) as ArrayRef);
                    }
                }
                let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
                    .map_err(|e| err_data(e.to_string()))?;
                return Ok(ApexResult::Data(batch));
            }
            Self::apply_filter_with_storage(&batch, where_clause, storage_path)?
        } else {
            batch
        };

        if filtered.num_rows() == 0 {
            // For aggregations on filtered empty result, still execute aggregation
            if has_aggregation && stmt.group_by.is_empty() {
                return Self::execute_aggregation(&filtered, &stmt);
            }
            let projected = if stmt.is_pure_star() {
                filtered
            } else {
                Self::apply_projection_with_storage(&filtered, &stmt.columns, Some(storage_path))?
            };
            return Ok(ApexResult::Empty(projected.schema()));
        }

        // Check for window functions
        let has_window = stmt
            .columns
            .iter()
            .any(|col| matches!(col, SelectColumn::WindowFunction { .. }));
        if has_window && has_aggregation {
            let grouped = if stmt.group_by.is_empty() {
                Self::execute_aggregation(&filtered, &stmt)?
            } else {
                Self::execute_group_by(&filtered, &stmt)?
            }
            .to_record_batch()?;
            let mut window_stmt = stmt.clone();
            let mut window_columns = vec![SelectColumn::All];
            for column in &stmt.columns {
                if let SelectColumn::WindowFunction {
                    name,
                    args,
                    partition_by,
                    order_by,
                    alias,
                    ..
                } = column
                {
                    let mut resolved_order = order_by.clone();
                    for clause in &mut resolved_order {
                        let clean = clause.column.trim_matches('"');
                        if grouped.column_by_name(clean).is_some() {
                            continue;
                        }
                        let aggregate_name = clean.split('(').next().unwrap_or(clean);
                        if let Some(resolved) = stmt.columns.iter().find_map(|candidate| {
                            if let SelectColumn::Expression {
                                expr: SqlExpr::Function { name, .. },
                                alias: Some(alias),
                            } = candidate
                            {
                                if name.eq_ignore_ascii_case(aggregate_name)
                                    || (name.eq_ignore_ascii_case("COUNT_DISTINCT")
                                        && aggregate_name.eq_ignore_ascii_case("COUNT"))
                                {
                                    return Some(alias.clone());
                                }
                            }
                            None
                        }) {
                            clause.column = resolved;
                            clause.expr = None;
                        }
                    }
                    window_columns.push(SelectColumn::WindowFunction {
                        name: name.clone(),
                        args: args.clone(),
                        partition_by: partition_by.clone(),
                        order_by: resolved_order,
                        alias: alias.clone(),
                        wrapper: None,
                        frame: None,
                    });
                }
            }
            window_stmt.columns = window_columns;
            return Self::execute_window_function(&grouped, &window_stmt);
        }
        if has_window {
            return Self::execute_window_function(&filtered, &stmt);
        }

        if has_aggregation && stmt.group_by.is_empty() {
            // Simple aggregation without GROUP BY
            return Self::execute_aggregation(&filtered, &stmt);
        }

        // Handle GROUP BY.
        //
        // A GROUP BY must run even when the projection has no aggregate and
        // there is no HAVING: `SELECT cat FROM t GROUP BY cat` is a legitimate
        // (aggregate-free) grouped projection and used to fall through to the
        // plain projection path, emitting one row per input row instead of one
        // row per group.
        if !stmt.group_by.is_empty() {
            return Self::execute_group_by(&filtered, &stmt);
        }

        // For DISTINCT: sort without top-k limit, project, deduplicate, then limit
        // For DISTINCT ON: sort, deduplicate by ON columns, project, then limit/offset
        // For non-DISTINCT: apply top-k sort + limit, then project
        let result = if stmt.distinct {
            if let Some(ref on_cols) = stmt.distinct_on {
                // DISTINCT ON: deduplicate by ON columns, then project + limit/offset
                let sorted = if !stmt.order_by.is_empty() {
                    Self::apply_order_by(&filtered, &stmt.order_by)?
                } else {
                    filtered
                };
                let deduped = Self::deduplicate_batch_on(&sorted, on_cols)?;
                let projected = Self::apply_projection_with_storage(
                    &deduped,
                    &stmt.columns,
                    Some(storage_path),
                )?;
                Self::apply_limit_offset(&projected, stmt.limit, stmt.offset)?
            } else {
                // Regular DISTINCT: project and deduplicate BEFORE sorting.
                // Sorting the full input first is O(N log N) over every row,
                // while the deduplicated result is usually far smaller; the
                // ORDER BY semantics are identical because the sort key must be
                // part of the SELECT list for DISTINCT queries.
                //
                // When the read columns are exactly the SELECT list (no WHERE),
                // deduplicate the dictionary-encoded batch directly — the
                // decode-to-plain projection then only touches the survivors.
                let can_dedup_raw = stmt.where_clause.is_none()
                    && stmt.columns.len() == filtered.num_columns()
                    && stmt.columns.iter().all(|column| {
                        matches!(
                            column,
                            SelectColumn::Column(_) | SelectColumn::ColumnAlias { .. }
                        )
                    });
                let deduped = if can_dedup_raw {
                    Self::deduplicate_batch(&filtered)?
                } else {
                    let projected = Self::apply_projection_with_storage(
                        &filtered,
                        &stmt.columns,
                        Some(storage_path),
                    )?;
                    Self::deduplicate_batch(&projected)?
                };
                // ORDER BY must see plain values (dictionary key ids are not
                // in value order); decoding after dedup touches few rows.
                let deduped = Self::decode_dict_columns(&deduped);
                let sorted = if !stmt.order_by.is_empty() {
                    Self::apply_order_by(&deduped, &stmt.order_by)?
                } else {
                    deduped
                };
                let limited = Self::apply_limit_offset(&sorted, stmt.limit, stmt.offset)?;
                let projected = Self::apply_projection_with_storage(
                    &limited,
                    &stmt.columns,
                    Some(storage_path),
                )?;
                // The deduplicated input may still carry dictionary-encoded
                // columns; decode them for the client.
                Self::decode_dict_columns(&projected)
            }
        } else {
            // Apply ORDER BY with LIMIT optimization (top-k heap sort)
            let limited = if !stmt.order_by.is_empty() {
                let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
                // Pre-evaluate any SELECT expression aliases referenced in ORDER BY
                // (e.g. SELECT array_distance(vec,[…]) AS dist … ORDER BY dist)
                let sort_batch =
                    Self::augment_batch_for_order_by(&filtered, &stmt.columns, &stmt.order_by)?;
                let sorted = Self::apply_order_by_topk(&sort_batch, &stmt.order_by, k)?;
                Self::apply_limit_offset(&sorted, stmt.limit, stmt.offset)?
            } else {
                Self::apply_limit_offset(&filtered, stmt.limit, stmt.offset)?
            };
            Self::apply_projection_with_storage(&limited, &stmt.columns, Some(storage_path))?
        };

        Ok(ApexResult::Data(result))
    }

    /// Multiple scalar COUNT(DISTINCT col) expressions. String cardinalities
    /// come directly from the typed dictionary cache; bounded integral domains
    /// use a parallel exact bitset scan.
    fn try_fast_count_distinct_scalars(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        if stmt.columns.is_empty()
            || stmt.where_clause.is_some()
            || !stmt.group_by.is_empty()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || stmt.distinct
            || stmt.distinct_on.is_some()
        {
            return Ok(None);
        }
        let mut fields = Vec::with_capacity(stmt.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());
        for select_column in &stmt.columns {
            let SelectColumn::Aggregate {
                func: AggregateFunc::Count,
                column: Some(column),
                distinct: true,
                alias,
            } = select_column
            else {
                return Ok(None);
            };
            let clean = column
                .trim_matches('"')
                .rsplit('.')
                .next()
                .unwrap_or(column.trim_matches('"'));
            let count = if Self::column_is_string(backend, clean) {
                let Some(count) = backend.cached_string_distinct_count(clean)? else {
                    return Ok(None);
                };
                count
            } else {
                let Some(count) = backend.execute_numeric_distinct_count_mmap(clean)? else {
                    return Ok(None);
                };
                count
            };
            fields.push(Field::new(
                alias
                    .clone()
                    .unwrap_or_else(|| format!("COUNT(DISTINCT {})", clean)),
                ArrowDataType::Int64,
                false,
            ));
            arrays.push(Arc::new(Int64Array::from(vec![count])));
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| err_data(error.to_string()))?;
        Ok(Some(ApexResult::Data(batch)))
    }

    /// DISTINCT over a small set of low-cardinality string/integral columns.
    /// Reuses typed categorical caches and records observed combinations in a
    /// compact dense bitmap instead of materializing and hashing Arrow rows.
    fn try_fast_cached_distinct_projection(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        if !stmt.distinct
            || stmt.distinct_on.is_some()
            || stmt.columns.is_empty()
            || stmt.columns.len() > 4
            || stmt.where_clause.is_some()
            || !stmt.group_by.is_empty()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
        {
            return Ok(None);
        }

        struct CachedColumn {
            output_name: String,
            values: std::sync::Arc<(Vec<String>, Vec<u16>)>,
            numeric: bool,
        }
        let mut columns = Vec::with_capacity(stmt.columns.len());
        let active_rows = backend.active_row_count() as usize;
        for column in &stmt.columns {
            let (name, output_name) = match column {
                SelectColumn::Column(name) => {
                    let clean = name
                        .trim_matches('"')
                        .rsplit('.')
                        .next()
                        .unwrap_or(name.trim_matches('"'));
                    (clean, clean.to_string())
                }
                SelectColumn::ColumnAlias { column, alias } => {
                    let clean = column
                        .trim_matches('"')
                        .rsplit('.')
                        .next()
                        .unwrap_or(column.trim_matches('"'));
                    (clean, alias.clone())
                }
                _ => return Ok(None),
            };
            let numeric = !Self::column_is_string(backend, name);
            let entry = if numeric {
                crate::storage::backend::get_global_numeric_dict_cache(
                    backend.path(),
                    name,
                    &backend.storage,
                )?
            } else {
                crate::storage::backend::get_global_dict_cache_with_nulls(
                    backend.path(),
                    name,
                    &backend.storage,
                )?
            };
            let Some((values, has_nulls, max_group_id)) = entry else {
                return Ok(None);
            };
            if has_nulls
                || values.1.len() != active_rows
                || max_group_id.is_some_and(|id| id as usize >= values.0.len())
            {
                return Ok(None);
            }
            columns.push(CachedColumn {
                output_name,
                values,
                numeric,
            });
        }

        let mut strides = Vec::with_capacity(columns.len());
        let mut combinations = 1usize;
        for column in columns.iter().rev() {
            strides.push(combinations);
            combinations = match combinations.checked_mul(column.values.0.len()) {
                Some(value) if value <= 1_000_000 => value,
                _ => return Ok(None),
            };
        }
        strides.reverse();
        let mut seen = vec![false; combinations];
        for row in 0..active_rows {
            let mut slot = 0usize;
            for (column, stride) in columns.iter().zip(&strides) {
                slot += column.values.1[row] as usize * *stride;
            }
            seen[slot] = true;
        }
        let used: Vec<usize> = seen
            .into_iter()
            .enumerate()
            .filter_map(|(slot, present)| present.then_some(slot))
            .collect();

        let mut fields = Vec::with_capacity(columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
        for (column_index, column) in columns.iter().enumerate() {
            let value_index = |slot: usize| {
                (slot / strides[column_index]) % column.values.0.len()
            };
            if column.numeric {
                let values: Option<Vec<i64>> = used
                    .iter()
                    .map(|&slot| column.values.0[value_index(slot)].parse::<i64>().ok())
                    .collect();
                let Some(values) = values else {
                    return Ok(None);
                };
                fields.push(Field::new(&column.output_name, ArrowDataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(values)));
            } else {
                fields.push(Field::new(&column.output_name, ArrowDataType::Utf8, false));
                arrays.push(Arc::new(StringArray::from(
                    used.iter()
                        .map(|&slot| column.values.0[value_index(slot)].as_str())
                        .collect::<Vec<_>>(),
                )));
            }
        }
        let mut batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| err_data(error.to_string()))?;
        if !stmt.order_by.is_empty() {
            let order_by = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let top_k = stmt.limit.map(|limit| limit + stmt.offset.unwrap_or(0));
            batch = Self::apply_order_by_topk(&batch, &order_by, top_k)?;
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            batch = Self::apply_limit_offset(&batch, stmt.limit, stmt.offset)?;
        }
        Ok(Some(if batch.num_rows() == 0 {
            ApexResult::Empty(batch.schema())
        } else {
            ApexResult::Data(batch)
        }))
    }

    /// Fuse an outer GROUP BY over a derived ordered numeric CASE expression.
    /// This avoids materializing the derived label column and its full source
    /// batch for common feature-binning queries.
    fn try_fast_derived_case_group_by(
        backend: &TableStorageBackend,
        outer: &SelectStatement,
        inner: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
            || outer.group_by.len() != 1
            || outer.group_by_exprs.iter().any(Option::is_some)
            || outer.where_clause.is_some()
            || outer.having.is_some()
            || !outer.joins.is_empty()
            || outer.distinct
            || outer.distinct_on.is_some()
            || inner.where_clause.is_some()
            || !inner.group_by.is_empty()
            || !inner.joins.is_empty()
            || inner.distinct
            || inner.limit.is_some()
            || inner.offset.is_some()
        {
            return Ok(None);
        }

        fn clean_name(name: &str) -> &str {
            let trimmed = name.trim_matches('"');
            trimmed
                .rsplit('.')
                .next()
                .unwrap_or(trimmed)
                .trim_matches('"')
        }

        let group_alias = clean_name(&outer.group_by[0]);
        let mut source_columns = std::collections::HashMap::new();
        let mut bucket_spec: Option<(String, Vec<i64>, Vec<String>)> = None;
        for column in &inner.columns {
            match column {
                SelectColumn::Column(name) => {
                    source_columns.insert(clean_name(name).to_string(), clean_name(name).to_string());
                }
                SelectColumn::ColumnAlias { column, alias } => {
                    source_columns.insert(alias.clone(), clean_name(column).to_string());
                }
                SelectColumn::Expression {
                    expr: SqlExpr::Case { when_then, else_expr },
                    alias: Some(alias),
                } if clean_name(alias) == group_alias => {
                    let Some(SqlExpr::Literal(Value::String(else_label))) = else_expr.as_deref()
                    else {
                        return Ok(None);
                    };
                    let mut source_column: Option<String> = None;
                    let mut bounds = Vec::with_capacity(when_then.len());
                    let mut labels = Vec::with_capacity(when_then.len() + 1);
                    for (condition, value) in when_then {
                        let SqlExpr::Literal(Value::String(label)) = value else {
                            return Ok(None);
                        };
                        let Some((column, operator, threshold)) =
                            Self::extract_numeric_comparison(condition)
                        else {
                            return Ok(None);
                        };
                        if !threshold.is_finite() || threshold.fract() != 0.0 {
                            return Ok(None);
                        }
                        let bound = match operator.as_str() {
                            "<" => threshold as i64,
                            "<=" => (threshold as i64).checked_add(1).ok_or_else(|| {
                                err_input("CASE bucket boundary is out of range")
                            })?,
                            _ => return Ok(None),
                        };
                        if source_column.as_deref().is_some_and(|existing| existing != clean_name(&column))
                        {
                            return Ok(None);
                        }
                        source_column = Some(clean_name(&column).to_string());
                        bounds.push(bound);
                        labels.push(label.clone());
                    }
                    if bounds.is_empty() || bounds.windows(2).any(|pair| pair[0] >= pair[1]) {
                        return Ok(None);
                    }
                    labels.push(else_label.clone());
                    bucket_spec = Some((source_column.unwrap(), bounds, labels));
                }
                _ => return Ok(None),
            }
        }
        let Some((bucket_column, bounds, labels)) = bucket_spec else {
            return Ok(None);
        };
        if backend.column_has_nulls(&bucket_column) {
            return Ok(None);
        }

        enum Output {
            Key(String),
            Aggregate(String, AggregateFunc, usize),
        }
        let mut outputs = Vec::with_capacity(outer.columns.len());
        let mut aggregate_specs: Vec<(String, bool)> = Vec::new();
        let mut aggregate_ops = Vec::new();
        for column in &outer.columns {
            match column {
                SelectColumn::Column(name) if clean_name(name) == group_alias => {
                    outputs.push(Output::Key(clean_name(name).to_string()));
                }
                SelectColumn::ColumnAlias { column, alias }
                    if clean_name(column) == group_alias =>
                {
                    outputs.push(Output::Key(alias.clone()));
                }
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct: false,
                    alias,
                } => {
                    let count_star = matches!(func, AggregateFunc::Count)
                        && column.as_ref().map_or(true, |column| {
                            column == "*"
                                || column.chars().next().is_some_and(|c| c.is_ascii_digit())
                        });
                    let source = if count_star {
                        "*".to_string()
                    } else {
                        let Some(column) = column else {
                            return Ok(None);
                        };
                        let Some(source) = source_columns.get(clean_name(column)) else {
                            return Ok(None);
                        };
                        source.clone()
                    };
                    let slot = aggregate_specs.len();
                    aggregate_specs.push((source.clone(), count_star));
                    aggregate_ops.push(match func {
                        AggregateFunc::Count => 0,
                        AggregateFunc::Sum | AggregateFunc::Avg => 1,
                        AggregateFunc::Min => 2,
                        AggregateFunc::Max => 4,
                    });
                    let function = match func {
                        AggregateFunc::Count => "COUNT",
                        AggregateFunc::Sum => "SUM",
                        AggregateFunc::Avg => "AVG",
                        AggregateFunc::Min => "MIN",
                        AggregateFunc::Max => "MAX",
                    };
                    outputs.push(Output::Aggregate(
                        alias
                            .clone()
                            .unwrap_or_else(|| format!("{}({})", function, source)),
                        func.clone(),
                        slot,
                    ));
                }
                _ => return Ok(None),
            }
        }
        if aggregate_specs.is_empty() {
            return Ok(None);
        }
        let aggregate_refs: Vec<(&str, bool)> = aggregate_specs
            .iter()
            .map(|(column, count_star)| (column.as_str(), *count_star))
            .collect();
        let Some(raw) = backend.execute_numeric_bucket_group_agg_mmap(
            &bucket_column,
            &bounds,
            &aggregate_refs,
            &aggregate_ops,
        )? else {
            return Ok(None);
        };

        let mut fields = Vec::with_capacity(outputs.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(outputs.len());
        for output in outputs {
            match output {
                Output::Key(name) => {
                    fields.push(Field::new(name, ArrowDataType::Utf8, false));
                    arrays.push(Arc::new(StringArray::from(
                        raw.iter()
                            .map(|row| labels.get(row.key as usize).map(String::as_str))
                            .collect::<Vec<_>>(),
                    )));
                }
                Output::Aggregate(name, func, slot) => match func {
                    AggregateFunc::Count => {
                        fields.push(Field::new(name, ArrowDataType::Int64, false));
                        arrays.push(Arc::new(Int64Array::from(
                            raw.iter().map(|row| row.stats[slot].0).collect::<Vec<_>>(),
                        )));
                    }
                    AggregateFunc::Avg => {
                        fields.push(Field::new(name, ArrowDataType::Float64, true));
                        arrays.push(Arc::new(Float64Array::from(
                            raw.iter()
                                .map(|row| {
                                    let stat = row.stats[slot];
                                    (stat.0 > 0).then_some(stat.1 / stat.0 as f64)
                                })
                                .collect::<Vec<_>>(),
                        )));
                    }
                    AggregateFunc::Sum | AggregateFunc::Min | AggregateFunc::Max => {
                        let all_int = raw.iter().all(|row| row.stats[slot].4);
                        if all_int {
                            fields.push(Field::new(name, ArrowDataType::Int64, true));
                            arrays.push(Arc::new(Int64Array::from(
                                raw.iter()
                                    .map(|row| {
                                        let stat = row.stats[slot];
                                        (stat.0 > 0).then_some(match func {
                                            AggregateFunc::Sum => stat.1 as i64,
                                            AggregateFunc::Min => stat.2 as i64,
                                            AggregateFunc::Max => stat.3 as i64,
                                            _ => unreachable!(),
                                        })
                                    })
                                    .collect::<Vec<_>>(),
                            )));
                        } else {
                            fields.push(Field::new(name, ArrowDataType::Float64, true));
                            arrays.push(Arc::new(Float64Array::from(
                                raw.iter()
                                    .map(|row| {
                                        let stat = row.stats[slot];
                                        (stat.0 > 0).then_some(match func {
                                            AggregateFunc::Sum => stat.1,
                                            AggregateFunc::Min => stat.2,
                                            AggregateFunc::Max => stat.3,
                                            _ => unreachable!(),
                                        })
                                    })
                                    .collect::<Vec<_>>(),
                            )));
                        }
                    }
                },
            }
        }
        let mut batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| err_data(error.to_string()))?;
        if !outer.order_by.is_empty() {
            let order_by = Self::resolve_order_by_cols(&outer.columns, &outer.order_by);
            let top_k = outer.limit.map(|limit| limit + outer.offset.unwrap_or(0));
            batch = Self::apply_order_by_topk(&batch, &order_by, top_k)?;
        }
        if outer.limit.is_some() || outer.offset.is_some() {
            batch = Self::apply_limit_offset(&batch, outer.limit, outer.offset)?;
        }
        Ok(Some(if batch.num_rows() == 0 {
            ApexResult::Empty(batch.schema())
        } else {
            ApexResult::Data(batch)
        }))
    }

    /// Rows to read from a direct-file / table-function source when the query is a
    /// plain `SELECT … LIMIT k [OFFSET o]` with no WHERE, ORDER BY, GROUP BY, HAVING,
    /// DISTINCT, aggregation, expression or window column.  In that case the LIMIT is
    /// applied in file order, so only the first `k + o` rows need to be materialized.
    fn simple_file_row_limit(stmt: &SelectStatement) -> Option<usize> {
        if stmt.where_clause.is_some()
            || !stmt.order_by.is_empty()
            || !stmt.group_by.is_empty()
            || !stmt.group_by_exprs.is_empty()
            || stmt.having.is_some()
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || !stmt.joins.is_empty()
        {
            return None;
        }
        for col in &stmt.columns {
            match col {
                SelectColumn::All
                | SelectColumn::AllExclude(..)
                | SelectColumn::AllReplace(..)
                | SelectColumn::Columns(..)
                | SelectColumn::Column(_)
                | SelectColumn::ColumnAlias { .. } => {}
                _ => return None,
            }
        }
        let limit = stmt.limit?;
        Some(limit + stmt.offset.unwrap_or(0))
    }

    /// True when the statement is exactly `SELECT COUNT(*)` / `COUNT(1)` with
    /// no GROUP BY, HAVING, DISTINCT, ORDER BY, LIMIT or window columns.
    fn is_simple_count_star(stmt: &SelectStatement) -> bool {
        use crate::query::AggregateFunc;
        stmt.group_by.is_empty()
            && stmt.having.is_none()
            && !stmt.distinct
            && stmt.order_by.is_empty()
            && stmt.limit.is_none()
            && stmt.offset.is_none()
            && stmt.columns.len() == 1
            && matches!(
                &stmt.columns[0],
                SelectColumn::Aggregate {
                    func: AggregateFunc::Count,
                    column,
                    distinct: false,
                    ..
                } if column
                    .as_deref()
                    .map_or(true, |c| c == "*" || c == "1")
            )
    }

    /// Fast path for simple string equality filters on dictionary-encoded columns
    /// Uses storage-level early termination for LIMIT queries when limit is Some
    /// Supports column projection pushdown (not limited to SELECT *)
    fn try_fast_string_filter(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<RecordBatch>> {
        // Must have LIMIT for early termination benefit
        if stmt.limit.is_none() {
            return Ok(None);
        }
        Self::try_fast_string_filter_impl(backend, stmt, stmt.limit)
    }

    /// Fast path for string equality filters WITHOUT LIMIT
    fn try_fast_string_filter_no_limit(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<RecordBatch>> {
        Self::try_fast_string_filter_impl(backend, stmt, None)
    }

    /// Fast path for LIKE filters: adaptive strategy based on selectivity.
    /// Uses full table scan + Arrow filter for high-selectivity queries,
    /// and index extraction for low-selectivity queries.
    fn try_fast_like_filter(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<RecordBatch>> {
        // Skip if there are pending deltas (use slower but accurate path)
        if backend.has_pending_deltas() || backend.is_mmap_only() {
            return Ok(None);
        }

        // Must have LIMIT for early termination benefit
        if stmt.limit.is_none() {
            return Ok(None);
        }

        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        let (col_name, pattern) = match Self::extract_like_pattern(where_clause) {
            Some(v) => v,
            None => return Ok(None),
        };

        // Fast path: single-pass parallel scan+extract (V4 mmap, any selectivity).
        // Avoids materializing non-matching rows — only builds Arrow arrays for hits.
        // Returns None for compressed/non-RCIX files → falls through to old paths.
        let limit_with_offset = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
        if let Some(batch) =
            backend.scan_like_and_extract_mmap(&col_name, &pattern, limit_with_offset)?
        {
            let offset = stmt.offset.unwrap_or(0);
            let result = if offset > 0 {
                let n = batch.num_rows().saturating_sub(offset);
                batch.slice(offset, n)
            } else {
                batch
            };
            if let Some(lim) = stmt.limit {
                let n = result.num_rows().min(lim);
                return Ok(Some(result.slice(0, n)));
            }
            return Ok(Some(result));
        }

        // Fallback: index-based extraction (compressed/non-RCIX files)
        let mut indices =
            match backend.scan_like_filter_mmap(&col_name, &pattern, limit_with_offset)? {
                Some(v) => v,
                None => return Ok(None),
            };

        if indices.is_empty() {
            let empty = Self::read_matching_rows_adaptive(backend, stmt, &indices)?;
            return Ok(Some(empty));
        }

        let offset = stmt.offset.unwrap_or(0);
        if offset > 0 {
            if offset >= indices.len() {
                let empty = Self::read_matching_rows_adaptive(backend, stmt, &[])?;
                return Ok(Some(empty));
            }
            indices = indices[offset..].to_vec();
        }
        if let Some(lim) = stmt.limit {
            indices.truncate(lim);
        }

        Ok(Some(Self::read_matching_rows_adaptive(
            backend, stmt, &indices,
        )?))
    }

    /// Unified implementation for string equality filter fast path
    fn try_fast_string_filter_impl(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
        limit: Option<usize>,
    ) -> io::Result<Option<RecordBatch>> {
        if backend.has_pending_writes() {
            return Ok(None);
        }

        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        let (col_name, filter_value) = match Self::extract_string_equality(where_clause) {
            Some(v) => v,
            None => return Ok(None),
        };
        if !Self::column_is_string(backend, &col_name) {
            return Ok(None);
        }

        // Column projection pushdown
        let projected_cols: Option<Vec<String>> = if stmt.is_select_star() {
            None
        } else {
            Some(stmt.required_columns().unwrap_or_default())
        };
        let col_refs: Option<Vec<&str>> = projected_cols
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());

        let result = if let Some(lim) = limit {
            if backend.is_mmap_only()
                && stmt.offset.unwrap_or(0) == 0
                && !backend.has_delta()
                && !backend.has_pending_deltas()
            {
                let indices =
                    match backend.scan_string_filter_mmap(&col_name, &filter_value, Some(lim))? {
                        Some(v) => v,
                        None => return Ok(None),
                    };
                return Ok(Some(Self::read_matching_rows_adaptive(
                    backend, stmt, &indices,
                )?));
            }
            if backend.pending_delta_updates_column(&col_name) || backend.has_delta() {
                let full = backend.read_columns_filtered_string_to_arrow(
                    col_refs.as_deref(),
                    &col_name,
                    &filter_value,
                    true,
                )?;
                let offset = stmt.offset.unwrap_or(0).min(full.num_rows());
                let len = lim.min(full.num_rows().saturating_sub(offset));
                full.slice(offset, len)
            } else {
                backend.read_columns_filtered_string_with_limit_to_arrow(
                    col_refs.as_deref(),
                    &col_name,
                    &filter_value,
                    true,
                    lim,
                    stmt.offset.unwrap_or(0),
                )?
            }
        } else {
            backend.read_columns_filtered_string_to_arrow(
                col_refs.as_deref(),
                &col_name,
                &filter_value,
                true,
            )?
        };

        Ok(Some(result))
    }

    /// Fast path for numeric range filters (BETWEEN)
    /// Uses streaming scan with early termination for LIMIT queries
    /// Supports column projection pushdown (not limited to SELECT *)
    fn try_fast_numeric_range_filter(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<RecordBatch>> {
        use crate::query::sql_parser::BinaryOperator;

        if backend.has_pending_deltas() || backend.has_delta() {
            return Ok(None);
        }

        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        if backend.is_mmap_only() {
            if stmt.limit.is_none() {
                return Ok(None);
            }
            let (col, lo, hi) = match Self::extract_any_numeric_range(where_clause) {
                Some(v) => v,
                None => return Ok(None),
            };
            let limit_with_off = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
            let indices = match backend.scan_numeric_range_mmap(&col, lo, hi, limit_with_off)? {
                Some(v) => v,
                None => return Ok(None),
            };
            if indices.is_empty() {
                let schema = backend.read_columns_to_arrow(None, 0, Some(0))?;
                return Ok(Some(schema));
            }
            let batch = Self::read_matching_rows_adaptive(backend, stmt, &indices)?;
            return Ok(Some(batch));
        }

        // The storage-level range reader is a LIMIT-oriented fast path.
        if stmt.limit.is_none() {
            return Ok(None);
        }

        // Extract BETWEEN pattern: col BETWEEN low AND high
        let (col_name, low, high) = match where_clause {
            SqlExpr::Between {
                column,
                low,
                high,
                negated,
            } => {
                if *negated {
                    return Ok(None);
                }
                let low_val = Self::extract_numeric_value(low)?;
                let high_val = Self::extract_numeric_value(high)?;
                (column.trim_matches('"').to_string(), low_val, high_val)
            }
            // Also handle col >= low AND col <= high pattern
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let (col1, op1, val1) = match Self::extract_comparison(left) {
                    Ok(v) => v,
                    Err(_) => return Ok(None),
                };
                let (col2, op2, val2) = match Self::extract_comparison(right) {
                    Ok(v) => v,
                    Err(_) => return Ok(None),
                };

                if col1 != col2 {
                    return Ok(None);
                }

                // Determine low and high from the operators
                let (low, high) = match (op1, op2) {
                    (BinaryOperator::Ge, BinaryOperator::Le) => (val1, val2),
                    (BinaryOperator::Le, BinaryOperator::Ge) => (val2, val1),
                    (BinaryOperator::Gt, BinaryOperator::Lt) => (val1, val2),
                    (BinaryOperator::Lt, BinaryOperator::Gt) => (val2, val1),
                    _ => return Ok(None),
                };
                (col1, low, high)
            }
            _ => return Ok(None),
        };

        let projected_cols: Option<Vec<String>> = if stmt.is_select_star() {
            None
        } else {
            Some(stmt.required_columns().unwrap_or_default())
        };
        let col_refs: Option<Vec<&str>> = projected_cols
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());

        let limit = stmt.limit.unwrap_or(100);
        let offset = stmt.offset.unwrap_or(0);

        // Use storage-level numeric range filter with early termination
        let result = backend.read_columns_filtered_range_with_limit_to_arrow(
            col_refs.as_deref(),
            &col_name,
            low,
            high,
            limit,
            offset,
        )?;

        Ok(Some(result))
    }

    /// Helper to extract numeric value from SqlExpr
    fn extract_numeric_value(expr: &SqlExpr) -> io::Result<f64> {
        match expr {
            SqlExpr::Literal(Value::Int64(n)) => Ok(*n as f64),
            SqlExpr::Literal(Value::Int32(n)) => Ok(*n as f64),
            SqlExpr::Literal(Value::Float64(n)) => Ok(*n),
            SqlExpr::Literal(Value::Float32(n)) => Ok(*n as f64),
            _ => Err(err_input("not a number")),
        }
    }

    /// Helper to extract comparison from binary op
    fn extract_comparison(
        expr: &SqlExpr,
    ) -> io::Result<(String, crate::query::sql_parser::BinaryOperator, f64)> {
        use crate::query::sql_parser::BinaryOperator;
        match expr {
            SqlExpr::BinaryOp { left, op, right } => {
                match (left.as_ref(), right.as_ref()) {
                    (SqlExpr::Column(col), lit) => {
                        let val = Self::extract_numeric_value(lit)?;
                        Ok((col.trim_matches('"').to_string(), op.clone(), val))
                    }
                    (lit, SqlExpr::Column(col)) => {
                        let val = Self::extract_numeric_value(lit)?;
                        // Flip the operator
                        let flipped_op = match op {
                            BinaryOperator::Gt => BinaryOperator::Lt,
                            BinaryOperator::Lt => BinaryOperator::Gt,
                            BinaryOperator::Ge => BinaryOperator::Le,
                            BinaryOperator::Le => BinaryOperator::Ge,
                            _ => return Err(err_input("unsupported op")),
                        };
                        Ok((col.trim_matches('"').to_string(), flipped_op, val))
                    }
                    _ => Err(err_input("not a comparison")),
                }
            }
            _ => Err(err_input("not a binary op")),
        }
    }

    /// Fast path for multi-condition WHERE with string equality AND numeric comparison
    /// Handles: SELECT * WHERE string_col = 'value' AND numeric_col > N LIMIT n
    fn try_fast_multi_condition_filter(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<RecordBatch>> {
        use crate::query::sql_parser::BinaryOperator;

        if backend.has_pending_deltas() || backend.is_mmap_only() {
            return Ok(None);
        }

        // Must have LIMIT
        if stmt.limit.is_none() {
            return Ok(None);
        }

        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        // Must be AND of two conditions
        let (left_cond, right_cond) = match where_clause {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => (left.as_ref(), right.as_ref()),
            _ => return Ok(None),
        };

        // Try to extract string equality and numeric comparison from either order
        let (str_col, str_val, num_col, num_op, num_val) =
            if let (Some((sc, sv)), Some((nc, no, nv))) = (
                Self::extract_string_equality(left_cond),
                Self::extract_numeric_comparison(right_cond),
            ) {
                (sc, sv, nc, no, nv)
            } else if let (Some((sc, sv)), Some((nc, no, nv))) = (
                Self::extract_string_equality(right_cond),
                Self::extract_numeric_comparison(left_cond),
            ) {
                (sc, sv, nc, no, nv)
            } else {
                return Ok(None);
            };
        if !Self::column_is_string(backend, &str_col) {
            return Ok(None);
        }

        let limit = stmt.limit.unwrap_or(100);
        let offset = stmt.offset.unwrap_or(0);

        // Use storage-level combined filter
        let result = backend.read_columns_filtered_string_numeric_with_limit_to_arrow(
            None, // All columns (SELECT *)
            &str_col, &str_val, &num_col, &num_op, num_val, limit, offset,
        )?;

        Ok(Some(result))
    }

    /// Fuse one numeric range filter, one string GROUP BY key, and compatible
    /// COUNT/SUM/AVG projections into a single storage scan.
    fn try_fast_numeric_filter_group_by(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        if backend.pending_v4_in_memory_rows() > 0
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || stmt.group_by.len() != 1
            || stmt.having.is_some()
            || stmt.group_by_exprs.iter().any(Option::is_some)
            || stmt.order_by.iter().any(|clause| clause.expr.is_some())
        {
            return Ok(None);
        }

        let where_clause = match &stmt.where_clause {
            Some(expr) => expr,
            None => return Ok(None),
        };
        let (filter_col, low, high) = match Self::extract_any_numeric_range(where_clause) {
            Some(range) => range,
            None => return Ok(None),
        };
        let group_col = stmt.group_by[0].trim_matches('"');

        let mut aggregate_col: Option<String> = None;
        let mut has_aggregate = false;
        for column in &stmt.columns {
            match column {
                SelectColumn::Column(name) if name.trim_matches('"') == group_col => {}
                SelectColumn::ColumnAlias { column, .. }
                    if column.trim_matches('"') == group_col => {}
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct,
                    ..
                } => {
                    if *distinct {
                        return Ok(None);
                    }
                    has_aggregate = true;
                    match func {
                        AggregateFunc::Count => {
                            let is_count_star = column.as_ref().map_or(true, |name| {
                                name == "*"
                                    || name
                                        .chars()
                                        .next()
                                        .map(|ch| ch.is_ascii_digit())
                                        .unwrap_or(false)
                            });
                            if !is_count_star {
                                return Ok(None);
                            }
                        }
                        AggregateFunc::Sum | AggregateFunc::Avg => {
                            let name = match column {
                                Some(name) if name != "*" => name.trim_matches('"'),
                                _ => return Ok(None),
                            };
                            if aggregate_col.as_deref().is_some_and(|current| current != name) {
                                return Ok(None);
                            }
                            aggregate_col = Some(name.to_string());
                        }
                        AggregateFunc::Min | AggregateFunc::Max => return Ok(None),
                    }
                }
                _ => return Ok(None),
            }
        }
        if !has_aggregate {
            return Ok(None);
        }

        // The storage aggregation intentionally omits null checks in its inner loop.
        // Keep nullable inputs on the generic Arrow path to preserve SQL semantics.
        if backend.column_has_nulls(&filter_col)
            || backend.column_has_nulls(group_col)
            || aggregate_col
                .as_deref()
                .is_some_and(|name| backend.column_has_nulls(name))
        {
            return Ok(None);
        }

        let raw = if let Some(dict_arc) = crate::storage::backend::get_global_dict_cache(
            backend.path(),
            group_col,
            &backend.storage,
        )? {
            backend.execute_between_group_agg_cached(
                &filter_col,
                low,
                high,
                &dict_arc.0,
                &dict_arc.1,
                aggregate_col.as_deref(),
            )?
        } else {
            backend.execute_between_group_agg(
                &filter_col,
                low,
                high,
                group_col,
                aggregate_col.as_deref(),
            )?
        };
        let raw = match raw {
            Some(raw) => raw,
            None => return Ok(None),
        };

        let mut fields = Vec::with_capacity(stmt.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());
        for column in &stmt.columns {
            match column {
                SelectColumn::Column(_) => {
                    let values: Vec<&str> = raw.iter().map(|(group, _, _)| group.as_str()).collect();
                    fields.push(Field::new(
                        Self::group_output_name(stmt, group_col),
                        ArrowDataType::Utf8,
                        false,
                    ));
                    arrays.push(Arc::new(StringArray::from(values)));
                }
                SelectColumn::ColumnAlias { alias, .. } => {
                    let values: Vec<&str> = raw.iter().map(|(group, _, _)| group.as_str()).collect();
                    fields.push(Field::new(alias, ArrowDataType::Utf8, false));
                    arrays.push(Arc::new(StringArray::from(values)));
                }
                SelectColumn::Aggregate {
                    func,
                    column,
                    alias,
                    distinct: false,
                } => {
                    let source = column.as_deref().unwrap_or("*");
                    let output_name = alias.clone().unwrap_or_else(|| format!("{}({})", func, source));
                    if matches!(func, AggregateFunc::Count) {
                        fields.push(Field::new(&output_name, ArrowDataType::Int64, false));
                        arrays.push(Arc::new(Int64Array::from(
                            raw.iter().map(|(_, _, count)| *count).collect::<Vec<_>>(),
                        )));
                    } else {
                        fields.push(Field::new(&output_name, ArrowDataType::Float64, false));
                        arrays.push(Arc::new(Float64Array::from(
                            raw.iter()
                                .map(|(_, sum, count)| match func {
                                    AggregateFunc::Avg if *count > 0 => *sum / *count as f64,
                                    AggregateFunc::Avg => 0.0,
                                    _ => *sum,
                                })
                                .collect::<Vec<_>>(),
                        )));
                    }
                }
                _ => unreachable!(),
            }
        }

        let schema = Arc::new(Schema::new(fields));
        let mut result =
            RecordBatch::try_new(schema, arrays).map_err(|error| err_data(error.to_string()))?;
        if !stmt.order_by.is_empty() {
            let resolved = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            if resolved
                .iter()
                .any(|clause| result.schema().field_with_name(&clause.column).is_err())
            {
                return Ok(None);
            }
            let top_k = stmt.limit.map(|limit| limit + stmt.offset.unwrap_or(0));
            result = Self::apply_order_by_topk(&result, &resolved, top_k)?;
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            result = Self::apply_limit_offset(&result, stmt.limit, stmt.offset)?;
        }

        if result.num_rows() == 0 {
            Ok(Some(ApexResult::Empty(result.schema())))
        } else {
            Ok(Some(ApexResult::Data(result)))
        }
    }


    /// Count scalar numeric `SUM(CASE WHEN predicate THEN 1 ELSE 0 END)`
    /// expressions in one parallel Row Group scan.
    fn try_fast_numeric_case_aggregation(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        if stmt.columns.is_empty()
            || stmt.where_clause.is_some()
            || !stmt.group_by.is_empty()
            || !stmt.group_by_exprs.is_empty()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || !stmt.order_by.is_empty()
            || stmt.limit.is_some()
            || stmt.offset.is_some()
            || stmt.distinct
            || stmt.distinct_on.is_some()
        {
            return Ok(None);
        }

        fn numeric_literal(expr: &SqlExpr) -> Option<f64> {
            match expr {
                SqlExpr::Literal(Value::Int64(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Int32(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Float64(value)) => Some(*value),
                SqlExpr::Literal(Value::Float32(value)) => Some(*value as f64),
                _ => None,
            }
        }

        let mut owned_specs = Vec::with_capacity(stmt.columns.len());
        let mut output_names = Vec::with_capacity(stmt.columns.len());
        for (index, select_column) in stmt.columns.iter().enumerate() {
            let SelectColumn::Expression { expr, alias } = select_column else {
                return Ok(None);
            };
            let SqlExpr::Function { name, args } = expr else {
                return Ok(None);
            };
            if !name.eq_ignore_ascii_case("SUM") || args.len() != 1 {
                return Ok(None);
            }
            let SqlExpr::Case {
                when_then,
                else_expr,
            } = &args[0]
            else {
                return Ok(None);
            };
            if when_then.len() != 1
                || numeric_literal(&when_then[0].1) != Some(1.0)
                || else_expr.as_deref().and_then(numeric_literal) != Some(0.0)
            {
                return Ok(None);
            }
            let Some((column, low, high)) = Self::extract_any_numeric_range(&when_then[0].0)
            else {
                return Ok(None);
            };
            owned_specs.push((column, low, high));
            output_names.push(
                alias
                    .clone()
                    .unwrap_or_else(|| format!("expr{}", index + 1)),
            );
        }

        let specs: Vec<(&str, f64, f64)> = owned_specs
            .iter()
            .map(|(column, low, high)| (column.as_str(), *low, *high))
            .collect();
        let mut cached_counts = Vec::with_capacity(specs.len());
        let mut all_cached = true;
        for &(column, low, high) in &specs {
            let Some((dictionary, has_nulls, max_group_id)) =
                crate::storage::backend::get_global_numeric_dict_cache(
                    backend.path(),
                    column,
                    &backend.storage,
                )?
            else {
                all_cached = false;
                break;
            };
            if has_nulls
                || dictionary.1.len() != backend.active_row_count() as usize
                || max_group_id.is_some_and(|id| id as usize >= dictionary.0.len())
            {
                all_cached = false;
                break;
            }
            let matching: Option<Vec<bool>> = dictionary
                .0
                .iter()
                .map(|label| {
                    label
                        .parse::<i64>()
                        .ok()
                        .map(|value| (value as f64) >= low && (value as f64) <= high)
                })
                .collect();
            let Some(matching) = matching else {
                all_cached = false;
                break;
            };
            let count = dictionary
                .1
                .iter()
                .filter(|&&group_id| matching[group_id as usize])
                .count() as i64;
            cached_counts.push(count);
        }
        let counts = if all_cached {
            Some(cached_counts)
        } else {
            backend.execute_numeric_case_counts_mmap(&specs)?
        };
        let Some(counts) = counts else {
            return Ok(None);
        };
        let has_rows = backend.active_row_count() > 0;
        let fields: Vec<Field> = output_names
            .iter()
            .map(|name| Field::new(name, ArrowDataType::Float64, true))
            .collect();
        let arrays: Vec<ArrayRef> = counts
            .into_iter()
            .map(|count| {
                Arc::new(Float64Array::from(vec![has_rows.then_some(count as f64)])) as ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| err_data(error.to_string()))?;
        Ok(Some(ApexResult::Data(batch)))
    }

    /// Answer scalar `SUM(CASE WHEN col IS [NOT] NULL THEN 1 ELSE 0 END)`
    /// expressions from the same column statistics used by COUNT/MIN/MAX.
    fn try_fast_null_count_aggregation(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        if stmt.columns.is_empty()
            || stmt.where_clause.is_some()
            || !stmt.group_by.is_empty()
            || !stmt.group_by_exprs.is_empty()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || !stmt.order_by.is_empty()
            || stmt.limit.is_some()
            || stmt.offset.is_some()
            || stmt.distinct
            || stmt.distinct_on.is_some()
        {
            return Ok(None);
        }

        fn numeric_literal(expr: &SqlExpr) -> Option<f64> {
            match expr {
                SqlExpr::Literal(Value::Int64(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Int32(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Float64(value)) => Some(*value),
                SqlExpr::Literal(Value::Float32(value)) => Some(*value as f64),
                _ => None,
            }
        }

        let mut specs: Vec<(String, String, bool)> = Vec::with_capacity(stmt.columns.len());
        for (index, select_column) in stmt.columns.iter().enumerate() {
            let SelectColumn::Expression { expr, alias } = select_column else {
                return Ok(None);
            };
            let SqlExpr::Function { name, args } = expr else {
                return Ok(None);
            };
            if !name.eq_ignore_ascii_case("SUM") || args.len() != 1 {
                return Ok(None);
            }
            let SqlExpr::Case {
                when_then,
                else_expr,
            } = &args[0]
            else {
                return Ok(None);
            };
            if when_then.len() != 1
                || numeric_literal(&when_then[0].1) != Some(1.0)
                || else_expr.as_deref().and_then(numeric_literal) != Some(0.0)
            {
                return Ok(None);
            }
            let SqlExpr::IsNull { column, negated } = &when_then[0].0 else {
                return Ok(None);
            };
            let actual = column
                .trim_matches('"')
                .rsplit('.')
                .next()
                .unwrap_or(column.trim_matches('"'))
                .trim_matches('"')
                .to_string();
            let output_name = alias
                .clone()
                .unwrap_or_else(|| format!("expr{}", index + 1));
            specs.push((actual, output_name, *negated));
        }

        let mut unique_columns = Vec::with_capacity(specs.len());
        for (column, _, _) in &specs {
            if !unique_columns.contains(column) {
                unique_columns.push(column.clone());
            }
        }
        let column_refs: Vec<&str> = unique_columns.iter().map(String::as_str).collect();
        let Some(null_counts) = backend.execute_null_counts_mmap(&column_refs)? else {
            return Ok(None);
        };
        if null_counts.len() != unique_columns.len() {
            return Ok(None);
        }

        let total = backend.active_row_count() as i64;
        let mut fields = Vec::with_capacity(specs.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(specs.len());
        for (column, output_name, count_not_null) in specs {
            let Some(index) = unique_columns.iter().position(|name| name == &column) else {
                return Ok(None);
            };
            let null_count = null_counts[index];
            let value = if count_not_null {
                total.saturating_sub(null_count)
            } else {
                null_count
            };
            fields.push(Field::new(&output_name, ArrowDataType::Float64, true));
            arrays.push(Arc::new(Float64Array::from(vec![(total > 0).then_some(value as f64)])));
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| err_data(error.to_string()))?;
        Ok(Some(ApexResult::Data(batch)))
    }

    /// V4 FAST PATH: Filtered aggregation with string equality WHERE
    /// Handles: SELECT COUNT(*), AVG(col), MAX(col) FROM table WHERE str_col = 'val'
    /// Scans string column for matching indices, then computes aggregates directly.
    fn try_fast_filtered_string_agg(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
        {
            return Ok(None);
        }

        if Self::table_has_index_catalog(None, backend.path()) {
            return Ok(None);
        }

        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        let (filter_col, filter_val, is_prefix) =
            if let Some((column, value)) = Self::extract_string_equality(where_clause) {
                (column, value, false)
            } else if let Some((column, pattern)) = Self::extract_like_pattern(where_clause) {
                let Some(prefix) = pattern.strip_suffix('%') else {
                    return Ok(None);
                };
                if prefix.is_empty() || prefix.contains(['%', '_']) {
                    return Ok(None);
                }
                (column, prefix.to_string(), true)
            } else {
                return Ok(None);
        };
        if !Self::column_is_string(backend, &filter_col) {
            return Ok(None);
        }

        // Collect unique aggregation columns. Add "*" when COUNT(*)/COUNT(1)
        // is present so the storage path returns the real match count.
        let mut unique_cols: Vec<String> = Vec::new();
        for col in &stmt.columns {
            if let SelectColumn::Aggregate {
                func,
                column,
                distinct,
                ..
            } = col
            {
                if *distinct {
                    return Ok(None);
                }
                if let Some(col_name) = column {
                    let is_count_star = matches!(func, AggregateFunc::Count)
                        && (col_name.as_str() == "*"
                            || col_name
                                .chars()
                                .next()
                                .map(|c| c.is_ascii_digit())
                                .unwrap_or(false));
                    if is_count_star {
                        if !unique_cols.iter().any(|c| c == "*") {
                            unique_cols.push("*".to_string());
                        }
                    } else if matches!(func, AggregateFunc::Count)
                        && !backend.storage.column_has_nulls(col_name)
                    {
                        if !unique_cols.iter().any(|c| c == "*") {
                            unique_cols.push("*".to_string());
                        }
                    } else if !unique_cols.contains(col_name) {
                        unique_cols.push(col_name.clone());
                    }
                } else if !matches!(func, AggregateFunc::Count) {
                    return Ok(None);
                } else if !unique_cols.iter().any(|c| c == "*") {
                    unique_cols.push("*".to_string());
                }
            } else {
                return Ok(None);
            }
        }

        let col_refs: Vec<&str> = unique_cols.iter().map(|s| s.as_str()).collect();

        // Single-pass: scan string filter + aggregate in one sequential pass
        use std::collections::HashMap;
        let agg_results = if let Some(dict_arc) = crate::storage::backend::get_global_dict_cache(
            backend.path(),
            &filter_col,
            &backend.storage,
        )? {
            let target_present = dict_arc.0.iter().any(|value| {
                if is_prefix {
                    value.starts_with(&filter_val)
                } else {
                    value == &filter_val
                }
            });
            let cached = if is_prefix {
                backend.storage.execute_filtered_string_prefix_agg_cached(
                    &filter_col,
                    &dict_arc.0,
                    &dict_arc.1,
                    &filter_val,
                    &col_refs,
                )?
            } else {
                backend.storage.execute_filtered_string_agg_cached(
                    &filter_col,
                    &dict_arc.0,
                    &dict_arc.1,
                    &filter_val,
                    &col_refs,
                )?
            };
            match cached {
                Some(results)
                    if !(target_present && results.iter().all(|stat| stat.0 == 0)) =>
                {
                    results
                }
                Some(_) => return Ok(None),
                None if is_prefix => return Ok(None),
                None => match backend.execute_filtered_string_agg_mmap(
                    &filter_col,
                    &filter_val,
                    &col_refs,
                )? {
                    Some(results) => results,
                    None => return Ok(None),
                },
            }
        } else {
            match backend.execute_filtered_string_agg_mmap(&filter_col, &filter_val, &col_refs)? {
                Some(results) => results,
                None => return Ok(None),
            }
        };

        if agg_results.iter().all(|stat| stat.0 == 0) {
            return Ok(None);
        }

        // Build stat lookup: column name -> (count, sum, min, max, is_int)
        let mut stat_map: HashMap<&str, (i64, f64, f64, f64, bool)> = HashMap::new();
        for (i, &col_name) in col_refs.iter().enumerate() {
            if i < agg_results.len() {
                stat_map.insert(col_name, agg_results[i]);
            }
        }

        let match_count = stat_map.get("*").map(|s| s.0).unwrap_or(0);

        // Build result
        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for col in &stmt.columns {
            if let SelectColumn::Aggregate {
                func,
                column,
                alias,
                ..
            } = col
            {
                let fn_name = match func {
                    AggregateFunc::Count => "COUNT",
                    AggregateFunc::Sum => "SUM",
                    AggregateFunc::Avg => "AVG",
                    AggregateFunc::Min => "MIN",
                    AggregateFunc::Max => "MAX",
                };
                let output_name = alias.clone().unwrap_or_else(|| {
                    if let Some(c) = column {
                        format!("{}({})", fn_name, c)
                    } else {
                        format!("{}(*)", fn_name)
                    }
                });

                match func {
                    AggregateFunc::Count => {
                        let count = if let Some(col_name) = column {
                            let is_count_star = col_name.as_str() == "*"
                                || col_name
                                    .chars()
                                    .next()
                                    .map(|c| c.is_ascii_digit())
                                    .unwrap_or(false);
                            if is_count_star {
                                match_count
                            } else {
                                stat_map
                                    .get(col_name.as_str())
                                    .map(|s| s.0)
                                    .unwrap_or(match_count)
                            }
                        } else {
                            match_count
                        };
                        fields.push(Field::new(&output_name, ArrowDataType::Int64, false));
                        arrays.push(Arc::new(Int64Array::from(vec![count])));
                    }
                    AggregateFunc::Sum
                    | AggregateFunc::Avg
                    | AggregateFunc::Min
                    | AggregateFunc::Max => {
                        let col_name = column.as_ref().unwrap();
                        let (count, sum, min_v, max_v, is_int) = stat_map
                            .get(col_name.as_str())
                            .copied()
                            .unwrap_or((0, 0.0, 0.0, 0.0, false));

                        match func {
                            AggregateFunc::Sum => {
                                if is_int {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Int64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Int64Array::from(vec![sum as i64])));
                                } else {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Float64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Float64Array::from(vec![sum])));
                                }
                            }
                            AggregateFunc::Avg => {
                                let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                                fields.push(Field::new(&output_name, ArrowDataType::Float64, true));
                                arrays.push(Arc::new(Float64Array::from(vec![avg])));
                            }
                            AggregateFunc::Min => {
                                if count == 0 {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Float64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Float64Array::from(vec![None::<f64>])));
                                } else if is_int {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Int64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Int64Array::from(vec![min_v as i64])));
                                } else {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Float64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Float64Array::from(vec![min_v])));
                                }
                            }
                            AggregateFunc::Max => {
                                if count == 0 {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Float64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Float64Array::from(vec![None::<f64>])));
                                } else if is_int {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Int64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Int64Array::from(vec![max_v as i64])));
                                } else {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Float64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Float64Array::from(vec![max_v])));
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            }
        }

        if fields.is_empty() {
            return Ok(None);
        }
        let schema = Arc::new(Schema::new(fields));
        let result = RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;
        Ok(Some(ApexResult::Data(result)))
    }

    /// V4 FAST PATH: COUNT(*) WHERE str_col IN (non-correlated subquery).
    /// Materializes the small subquery set once, then counts matching rows at
    /// the storage layer without building the full row mask.
    fn try_fast_in_subquery_count(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
        {
            return Ok(None);
        }
        if !Self::is_simple_count_star(stmt) {
            return Ok(None);
        }
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        let (col, sub_stmt) = match where_clause {
            SqlExpr::InSubquery {
                column,
                stmt,
                negated: false,
            } => (column, stmt.as_ref()),
            _ => return Ok(None),
        };
        let col = col
            .trim_matches('"')
            .rsplit('.')
            .next()
            .unwrap_or(col.trim_matches('"'))
            .to_string();
        if !Self::column_is_string(backend, &col) {
            return Ok(None);
        }

        // Execute the (non-correlated) subquery once and collect its values.
        let sub_result = Self::execute_select(sub_stmt.clone(), backend.path())?;
        let sub_batch = sub_result.to_record_batch()?;
        if sub_batch.num_columns() == 0 {
            return Ok(None);
        }
        let sub_col = sub_batch.column(0);
        let mut values: Vec<String> = Vec::with_capacity(sub_batch.num_rows());
        for i in 0..sub_batch.num_rows() {
            if sub_col.is_null(i) {
                continue;
            }
            values.push(Self::arrow_value_to_string(sub_col, i));
        }

        let count = match backend.count_string_in_set_mmap(&col, &values)? {
            Some(c) => c,
            None => return Ok(None),
        };

        // Mirror execute_aggregation's one-row COUNT output shape.
        let mut fields = Vec::with_capacity(stmt.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());
        for c in &stmt.columns {
            if let SelectColumn::Aggregate { alias, column, .. } = c {
                let output = alias.clone().unwrap_or_else(|| {
                    format!("COUNT({})", column.as_deref().unwrap_or("*"))
                });
                fields.push(Field::new(output, ArrowDataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(vec![count])) as ArrayRef);
            }
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| err_data(e.to_string()))?;
        Ok(Some(ApexResult::Data(batch)))
    }

    /// V4 FAST PATH: COUNT(*) WHERE EXISTS (correlated subquery).
    /// Decorrelates the `inner_col = outer_col` equality (plus any other inner
    /// predicates) into a single non-correlated subquery, collects the set of
    /// qualifying inner values once, then counts matching outer rows at the
    /// storage layer via `count_string_in_set_mmap` — avoiding both the full
    /// Arrow batch and the 1M-row boolean mask.
    fn try_fast_exists_count(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use std::sync::Arc;

        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
        {
            return Ok(None);
        }
        if !Self::is_simple_count_star(stmt) {
            return Ok(None);
        }
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        let sub_stmt = match where_clause {
            SqlExpr::ExistsSubquery { stmt } => stmt.as_ref(),
            _ => return Ok(None),
        };
        let sub_where = match &sub_stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        // Resolve the subquery's table path from its FROM clause.
        let subquery_path = Self::resolve_subquery_table_path(sub_stmt, backend.path())?;

        // Detect outer column references.  find_outer_column_refs only reads the
        // outer batch's schema (column names), so a 0-row batch with the main
        // table's columns is sufficient and avoids materializing any rows.
        let outer_schema = Arc::new(Schema::new(
            backend
                .column_names()
                .iter()
                .map(|n| Field::new(n.as_str(), ArrowDataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let empty_outer = RecordBatch::new_empty(outer_schema);
        let outer_cols = Self::find_outer_column_refs(sub_stmt, &empty_outer);
        if outer_cols.is_empty() {
            return Ok(None); // non-correlated EXISTS — handled by the generic path
        }

        let (outer_col, inner_col, remaining_pred) =
            match Self::extract_correlation_equality(sub_where, &outer_cols) {
                Some(v) => v,
                None => return Ok(None),
            };

        // If the remaining predicate still references outer columns we cannot
        // decorrelate safely — fall back.
        if let Some(ref remaining) = remaining_pred {
            let mut refs = Vec::new();
            Self::collect_outer_refs_from_expr(remaining, &outer_cols, "", &mut refs);
            let mut refs2 = Vec::new();
            let unqualified: Vec<String> = outer_cols
                .iter()
                .map(|s| {
                    if let Some(d) = s.rfind('.') {
                        s[d + 1..].to_string()
                    } else {
                        s.clone()
                    }
                })
                .collect();
            Self::collect_outer_refs_from_expr(remaining, &unqualified, "", &mut refs2);
            if !refs.is_empty() || !refs2.is_empty() {
                return Ok(None);
            }
        }

        // Build the decorrelated subquery: SELECT inner_col FROM ... WHERE remaining_pred.
        let mut decorrelated = sub_stmt.clone();
        decorrelated.columns = vec![SelectColumn::Column(inner_col.clone())];
        decorrelated.where_clause = remaining_pred;
        let sub_result = Self::execute_select(decorrelated, &subquery_path)?;
        let sub_batch = sub_result.to_record_batch()?;
        if sub_batch.num_columns() == 0 {
            let count = 0i64;
            let mut fields = Vec::with_capacity(stmt.columns.len());
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());
            for c in &stmt.columns {
                if let SelectColumn::Aggregate { alias, column, .. } = c {
                    let output = alias.clone().unwrap_or_else(|| {
                        format!("COUNT({})", column.as_deref().unwrap_or("*"))
                    });
                    fields.push(Field::new(output, ArrowDataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(vec![count])) as ArrayRef);
                }
            }
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
                .map_err(|e| err_data(e.to_string()))?;
            return Ok(Some(ApexResult::Data(batch)));
        }

        // Collect the inner join-column values.
        let sub_col = sub_batch.column(0);
        let mut values: Vec<String> = Vec::with_capacity(sub_batch.num_rows());
        for i in 0..sub_batch.num_rows() {
            if sub_col.is_null(i) {
                continue;
            }
            values.push(Self::arrow_value_to_string(sub_col, i));
        }

        // Resolve the outer column name (strip table prefix) and confirm it is a
        // string column eligible for the set-count fast path.
        let outer_col_clean = if let Some(dot_pos) = outer_col.rfind('.') {
            &outer_col[dot_pos + 1..]
        } else {
            &outer_col
        };
        if !Self::column_is_string(backend, outer_col_clean) {
            return Ok(None);
        }

        let count = match backend.count_string_in_set_mmap(outer_col_clean, &values)? {
            Some(c) => c,
            None => return Ok(None),
        };

        // Mirror execute_aggregation's one-row COUNT output shape.
        let mut fields = Vec::with_capacity(stmt.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());
        for c in &stmt.columns {
            if let SelectColumn::Aggregate { alias, column, .. } = c {
                let output = alias.clone().unwrap_or_else(|| {
                    format!("COUNT({})", column.as_deref().unwrap_or("*"))
                });
                fields.push(Field::new(output, ArrowDataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(vec![count])) as ArrayRef);
            }
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| err_data(e.to_string()))?;
        Ok(Some(ApexResult::Data(batch)))
    }

    /// Deep OFFSET fast path: `SELECT <cols> ORDER BY <low-card numeric>, <string>
    /// LIMIT k OFFSET o` with no WHERE / GROUP BY / aggregation.  Reads only the
    /// leading numeric column, buckets it to find the top-(k+o) boundary, then reads
    /// the order/projected columns sparsely (only the boundary rows) before sorting —
    /// avoiding materializing the full high-cardinality string column.
    fn try_fast_deep_offset(
        stmt: &SelectStatement,
        storage_path: &Path,
    ) -> io::Result<Option<ApexResult>> {
        use arrow::array::Int64Array;

        if stmt.where_clause.is_some()
            || !stmt.group_by.is_empty()
            || stmt.distinct
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
        {
            return Ok(None);
        }
        for c in &stmt.columns {
            match c {
                SelectColumn::Column(_) | SelectColumn::ColumnAlias { .. } => {}
                _ => return Ok(None),
            }
        }
        if stmt.order_by.len() != 2 {
            return Ok(None);
        }
        let o0 = &stmt.order_by[0];
        let o1 = &stmt.order_by[1];
        if o0.descending || o1.descending || o0.expr.is_some() || o1.expr.is_some() {
            return Ok(None);
        }
        let c0 = Self::clean_order_col(&o0.column).to_string();
        let c1 = Self::clean_order_col(&o1.column).to_string();
        let limit = match stmt.limit {
            Some(l) => l,
            None => return Ok(None),
        };
        let offset = stmt.offset.unwrap_or(0);
        let k = limit + offset;
        if k == 0 {
            return Ok(None);
        }
        if stmt.from.is_none() {
            return Ok(None);
        }
        // Read only the leading numeric column (cheap — no string materialization).
        let backend = get_cached_backend(storage_path)?;
        let num_batch = backend.read_columns_to_arrow(Some(&[c0.as_str()]), 0, None)?;
        if num_batch.num_columns() == 0 {
            return Ok(None);
        }
        let num_arr = num_batch.column(0);
        let num_arr = match num_arr.as_any().downcast_ref::<Int64Array>() {
            Some(arr) => arr,
            None => return Ok(None),
        };
        let n = num_arr.len();
        if k >= n || num_arr.null_count() > 0 {
            return Ok(None);
        }
        let (candidates, before, boundary_only) = match Self::bucket_candidates(num_arr, k, offset)
        {
            Some(v) => v,
            None => return Ok(None),
        };
        let order_refs = vec![c0.as_str(), c1.as_str()];
        let result = backend.read_columns_by_indices_to_arrow(&candidates, Some(&order_refs))?;
        if result.num_rows() == 0 {
            return Ok(None);
        }
        // When the output lies entirely within the boundary bucket, only that bucket
        // was read; the in-bucket offset is (offset - before), otherwise it is offset.
        let partial_offset = if boundary_only { offset - before } else { offset };
        let needed = partial_offset + limit;
        let sorted = Self::apply_order_by_topk(&result, &stmt.order_by, Some(needed))?;
        let limited = sorted.slice(partial_offset.min(sorted.num_rows()), limit.min(sorted.num_rows().saturating_sub(partial_offset)));
        let projected =
            Self::apply_projection_with_storage(&limited, &stmt.columns, Some(storage_path))?;
        Ok(Some(ApexResult::Data(projected)))
    }

    /// Collect the row indices to materialize for a bucket-based deep OFFSET, plus
    /// the count of rows strictly below the boundary key.  When `offset >= before`
    /// the output lies entirely inside the boundary bucket, so only that bucket's
    /// rows are returned (`boundary_only = true`) — far fewer scattered reads.
    fn bucket_candidates(
        num_arr: &Int64Array,
        k: usize,
        offset: usize,
    ) -> Option<(Vec<usize>, usize, bool)> {
        use ahash::AHashMap;
        let n = num_arr.len();
        // For a small bounded integer range, histogram with a direct-indexed count
        // array (much faster than a HashMap over 1M rows); otherwise fall back.
        let mut min_v = i64::MAX;
        let mut max_v = i64::MIN;
        for i in 0..n {
            let v = num_arr.value(i);
            if v < min_v {
                min_v = v;
            }
            if v > max_v {
                max_v = v;
            }
        }
        if max_v < min_v {
            return None;
        }
        if (max_v - min_v + 1) as u64 <= 4096 {
            let lo = min_v;
            let size = (max_v - min_v + 1) as usize;
            let mut counts = vec![0i64; size];
            for i in 0..n {
                counts[(num_arr.value(i) - lo) as usize] += 1;
            }
            // Distinct used values, in ascending order.
            let mut used: Vec<i64> = Vec::new();
            for i in 0..size {
                if counts[i] > 0 {
                    used.push(lo + i as i64);
                }
            }
            let mut cum: i64 = 0;
            let mut boundary = 0i64;
            let mut before: i64 = 0;
            let mut found = false;
            for &v in &used {
                let cnt = counts[(v - lo) as usize];
                if cum + cnt >= k as i64 {
                    boundary = v;
                    before = cum;
                    found = true;
                    break;
                }
                cum += cnt;
            }
            if !found {
                return None;
            }
            let before = before as usize;
            let boundary_count = counts[(boundary - lo) as usize] as usize;
            let boundary_only = offset >= before;
            let cap = if boundary_only {
                boundary_count
            } else {
                before + boundary_count
            };
            let mut rows = Vec::with_capacity(cap);
            for i in 0..n {
                let v = num_arr.value(i);
                if boundary_only {
                    if v == boundary {
                        rows.push(i);
                    }
                } else if v <= boundary {
                    rows.push(i);
                }
            }
            Some((rows, before, boundary_only))
        } else {
            // Wide/negative range: HashMap-based histogram.
            let mut counts: AHashMap<i64, i64> = AHashMap::new();
            let mut keys: Vec<i64> = Vec::new();
            for i in 0..n {
                let v = num_arr.value(i);
                match counts.get_mut(&v) {
                    Some(c) => *c += 1,
                    None => {
                        counts.insert(v, 1);
                        keys.push(v);
                    }
                }
            }
            if keys.is_empty() || keys.len() > 1024 {
                return None;
            }
            keys.sort_unstable();
            let mut cum: i64 = 0;
            let mut boundary = 0i64;
            let mut before: i64 = 0;
            let mut found = false;
            for &v in &keys {
                let cnt = counts[&v];
                if cum + cnt >= k as i64 {
                    boundary = v;
                    before = cum;
                    found = true;
                    break;
                }
                cum += cnt;
            }
            if !found {
                return None;
            }
            let before = before as usize;
            let boundary_count = counts[&boundary] as usize;
            let boundary_only = offset >= before;
            let cap = if boundary_only {
                boundary_count
            } else {
                before + boundary_count
            };
            let mut rows = Vec::with_capacity(cap);
            for i in 0..n {
                let v = num_arr.value(i);
                if boundary_only {
                    if v == boundary {
                        rows.push(i);
                    }
                } else if v <= boundary {
                    rows.push(i);
                }
            }
            Some((rows, before, boundary_only))
        }
    }

    /// V4 FAST PATH: COUNT(*) WHERE <expr> <op> <literal>, where <expr> is a
    /// deterministic function of a single dictionary-encoded string column.
    /// Evaluates the predicate once per distinct dict value (using the warm
    /// global dict cache), then counts matching rows at the storage layer —
    /// avoiding both the full Arrow batch and the per-row mask.
    fn try_fast_dict_scalar_count(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::sql_parser::BinaryOperator;
        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
        {
            return Ok(None);
        }
        if !Self::is_simple_count_star(stmt) {
            return Ok(None);
        }
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        // Must be a comparison between an expression and a literal.
        let expr = match where_clause {
            SqlExpr::BinaryOp { left, op, right } => {
                if !matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Lt
                        | BinaryOperator::Le
                        | BinaryOperator::Gt
                        | BinaryOperator::Ge
                ) {
                    return Ok(None);
                }
                match (left.as_ref(), right.as_ref()) {
                    (e, SqlExpr::Literal(_)) | (SqlExpr::Literal(_), e) => e,
                    _ => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        if matches!(expr, SqlExpr::Column(_)) {
            return Ok(None); // column-vs-literal already has a scalar fast path
        }

        // Collect the single referenced column from the expression.
        fn collect_columns(expr: &SqlExpr, out: &mut Vec<String>, has_subquery: &mut bool) {
            match expr {
                SqlExpr::Column(name) => {
                    let bare = name
                        .trim_matches('"')
                        .rsplit('.')
                        .next()
                        .unwrap_or(name.trim_matches('"'))
                        .to_string();
                    if !out.contains(&bare) {
                        out.push(bare);
                    }
                }
                SqlExpr::InSubquery { .. }
                | SqlExpr::ExistsSubquery { .. }
                | SqlExpr::ScalarSubquery { .. } => *has_subquery = true,
                SqlExpr::BinaryOp { left, right, .. } => {
                    collect_columns(left, out, has_subquery);
                    collect_columns(right, out, has_subquery);
                }
                SqlExpr::UnaryOp { expr, .. }
                | SqlExpr::Paren(expr)
                | SqlExpr::Cast { expr, .. } => collect_columns(expr, out, has_subquery),
                SqlExpr::Function { args, .. } => {
                    for arg in args {
                        collect_columns(arg, out, has_subquery);
                    }
                }
                SqlExpr::Case {
                    when_then,
                    else_expr,
                } => {
                    for (cond, value) in when_then {
                        collect_columns(cond, out, has_subquery);
                        collect_columns(value, out, has_subquery);
                    }
                    if let Some(value) = else_expr {
                        collect_columns(value, out, has_subquery);
                    }
                }
                _ => {}
            }
        }
        let mut cols: Vec<String> = Vec::new();
        let mut has_subquery = false;
        collect_columns(expr, &mut cols, &mut has_subquery);
        if has_subquery || cols.len() != 1 {
            return Ok(None);
        }
        let col = cols[0].clone();
        if !Self::column_is_string(backend, &col) {
            return Ok(None);
        }

        // Use the global dict cache (validated against file mtime + table epoch
        // by `get_*`; it rebuilds transparently if stale). Only low-cardinality
        // columns are served, and null-containing columns fall back.
        let Some((dict_arc, has_nulls, _)) =
            crate::storage::backend::get_global_dict_cache_with_nulls(
                backend.path(),
                &col,
                &backend.storage,
            )?
        else {
            return Ok(None);
        };
        if has_nulls {
            return Ok(None);
        }
        let values: Vec<String> = dict_arc.0.iter().cloned().collect();
        if values.len() > 4096 {
            return Ok(None);
        }

        // Evaluate the full predicate on the distinct values.
        let arr: ArrayRef = Arc::new(StringArray::from(values.clone()));
        let schema = Arc::new(Schema::new(vec![Field::new(
            &col,
            ArrowDataType::Utf8,
            false,
        )]));
        let small_batch =
            RecordBatch::try_new(schema, vec![arr]).map_err(|e| err_data(e.to_string()))?;
        let mask = Self::evaluate_predicate(&small_batch, where_clause)?;
        let mut matching: Vec<String> = Vec::with_capacity(values.len());
        for i in 0..values.len() {
            if !mask.is_null(i) && mask.value(i) {
                matching.push(values[i].clone());
            }
        }

        let count = match backend.count_string_in_set_mmap(&col, &matching)? {
            Some(c) => c,
            None => return Ok(None),
        };

        let mut fields = Vec::with_capacity(stmt.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());
        for c in &stmt.columns {
            if let SelectColumn::Aggregate { alias, column, .. } = c {
                let output = alias.clone().unwrap_or_else(|| {
                    format!("COUNT({})", column.as_deref().unwrap_or("*"))
                });
                fields.push(Field::new(output, ArrowDataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(vec![count])) as ArrayRef);
            }
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| err_data(e.to_string()))?;
        Ok(Some(ApexResult::Data(batch)))
    }

    /// V4 FAST PATH: Filtered aggregation with a numeric WHERE predicate.
    /// Handles: SELECT COUNT(*), AVG(col), MAX(col) FROM table WHERE num_col > value
    fn try_fast_filtered_numeric_agg(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
        {
            return Ok(None);
        }

        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        let Some(predicates) = Self::extract_numeric_conjunction(where_clause) else {
            return Ok(None);
        };
        let single_filter = (predicates.len() == 1).then(|| predicates[0].clone());

        let mut unique_cols: Vec<String> = Vec::new();
        for col in &stmt.columns {
            if let SelectColumn::Aggregate {
                func,
                column,
                distinct,
                ..
            } = col
            {
                if *distinct {
                    return Ok(None);
                }
                if let Some(col_name) = column {
                    let is_count_star = matches!(func, AggregateFunc::Count)
                        && (col_name.as_str() == "*"
                            || col_name
                                .chars()
                                .next()
                                .map(|c| c.is_ascii_digit())
                                .unwrap_or(false));
                    if is_count_star {
                        if !unique_cols.iter().any(|c| c == "*") {
                            unique_cols.push("*".to_string());
                        }
                    } else if !unique_cols.contains(col_name) {
                        unique_cols.push(col_name.clone());
                    }
                } else if !matches!(func, AggregateFunc::Count) {
                    return Ok(None);
                } else if !unique_cols.iter().any(|c| c == "*") {
                    unique_cols.push("*".to_string());
                }
            } else {
                return Ok(None);
            }
        }
        if unique_cols.is_empty() {
            return Ok(None);
        }

        let col_refs: Vec<&str> = unique_cols.iter().map(|s| s.as_str()).collect();
        let predicate_refs = predicates
            .iter()
            .map(|(column, low, high)| (column.as_str(), *low, *high))
            .collect::<Vec<_>>();
        let cached_results = backend.execute_filtered_numeric_agg_cached(
            &predicate_refs,
            &col_refs,
        )?;
        let agg_results = if let Some(results) = cached_results {
            results
        } else if let Some((filter_col, low, high)) = single_filter.clone()
            .filter(|(_, low, high)| low == high && low.is_finite() && low.fract() == 0.0)
        {
            let target = (low as i64).to_string();
            match crate::storage::backend::get_global_numeric_dict_cache(
                backend.path(),
                &filter_col,
                &backend.storage,
            )? {
                Some((dict, _, _)) => match backend.storage.execute_filtered_string_agg_cached(
                    &filter_col,
                    &dict.0,
                    &dict.1,
                    &target,
                    &col_refs,
                )? {
                    Some(results) => results,
                    None => match backend.execute_filtered_numeric_agg_mmap(
                        &filter_col,
                        low,
                        high,
                        &col_refs,
                    )? {
                        Some(results) => results,
                        None => return Ok(None),
                    },
                },
                None => match backend.execute_filtered_numeric_agg_mmap(
                    &filter_col,
                    low,
                    high,
                    &col_refs,
                )? {
                    Some(results) => results,
                    None => return Ok(None),
                },
            }
        } else if let Some((filter_col, low, high)) = single_filter {
            match backend.execute_filtered_numeric_agg_mmap(&filter_col, low, high, &col_refs)? {
                Some(results) => results,
                None => return Ok(None),
            }
        } else {
            return Ok(None);
        };

        use std::collections::HashMap;
        let mut stat_map: HashMap<&str, (i64, f64, f64, f64, bool)> = HashMap::new();
        for (i, &col_name) in col_refs.iter().enumerate() {
            if i < agg_results.len() {
                stat_map.insert(col_name, agg_results[i]);
            }
        }
        let match_count = stat_map.get("*").map(|s| s.0).unwrap_or(0);

        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for col in &stmt.columns {
            if let SelectColumn::Aggregate {
                func,
                column,
                alias,
                ..
            } = col
            {
                let fn_name = match func {
                    AggregateFunc::Count => "COUNT",
                    AggregateFunc::Sum => "SUM",
                    AggregateFunc::Avg => "AVG",
                    AggregateFunc::Min => "MIN",
                    AggregateFunc::Max => "MAX",
                };
                let output_name = alias.clone().unwrap_or_else(|| {
                    if let Some(c) = column {
                        format!("{}({})", fn_name, c)
                    } else {
                        format!("{}(*)", fn_name)
                    }
                });

                match func {
                    AggregateFunc::Count => {
                        let count = if let Some(col_name) = column {
                            let is_count_star = col_name.as_str() == "*"
                                || col_name
                                    .chars()
                                    .next()
                                    .map(|c| c.is_ascii_digit())
                                    .unwrap_or(false);
                            if is_count_star {
                                match_count
                            } else {
                                stat_map.get(col_name.as_str()).map(|s| s.0).unwrap_or(0)
                            }
                        } else {
                            match_count
                        };
                        fields.push(Field::new(&output_name, ArrowDataType::Int64, false));
                        arrays.push(Arc::new(Int64Array::from(vec![count])));
                    }
                    AggregateFunc::Sum
                    | AggregateFunc::Avg
                    | AggregateFunc::Min
                    | AggregateFunc::Max => {
                        let col_name = column.as_ref().unwrap();
                        let (count, sum, min_v, max_v, is_int) = stat_map
                            .get(col_name.as_str())
                            .copied()
                            .unwrap_or((0, 0.0, 0.0, 0.0, false));

                        match func {
                            AggregateFunc::Sum => {
                                if is_int {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Int64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Int64Array::from(vec![sum as i64])));
                                } else {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Float64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Float64Array::from(vec![sum])));
                                }
                            }
                            AggregateFunc::Avg => {
                                let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                                fields.push(Field::new(&output_name, ArrowDataType::Float64, true));
                                arrays.push(Arc::new(Float64Array::from(vec![avg])));
                            }
                            AggregateFunc::Min => {
                                if count == 0 {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Float64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Float64Array::from(vec![None::<f64>])));
                                } else if is_int {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Int64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Int64Array::from(vec![min_v as i64])));
                                } else {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Float64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Float64Array::from(vec![min_v])));
                                }
                            }
                            AggregateFunc::Max => {
                                if count == 0 {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Float64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Float64Array::from(vec![None::<f64>])));
                                } else if is_int {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Int64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Int64Array::from(vec![max_v as i64])));
                                } else {
                                    fields.push(Field::new(
                                        &output_name,
                                        ArrowDataType::Float64,
                                        true,
                                    ));
                                    arrays.push(Arc::new(Float64Array::from(vec![max_v])));
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            }
        }

        if fields.is_empty() {
            return Ok(None);
        }
        let schema = Arc::new(Schema::new(fields));
        let result = RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;
        Ok(Some(ApexResult::Data(result)))
    }

    /// Mmap-native GROUP BY for the portable Hive behavior aggregates.
    fn try_fast_native_string_group_by(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        if backend.is_in_memory()
            || backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
            || stmt.group_by.len() != 1
            || stmt.where_clause.is_some()
            || !stmt.joins.is_empty()
        {
            return Ok(None);
        }

        fn clean_name(name: &str) -> &str {
            let trimmed = name.trim_matches('"');
            trimmed
                .rsplit('.')
                .next()
                .unwrap_or(trimmed)
                .trim_matches('"')
        }

        fn numeric_literal(expr: &SqlExpr) -> Option<f64> {
            match expr {
                SqlExpr::Literal(Value::Int64(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Int32(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Float64(value)) => Some(*value),
                SqlExpr::Literal(Value::Float32(value)) => Some(*value as f64),
                _ => None,
            }
        }

        fn case_counter(expr: &SqlExpr) -> Option<(String, Vec<String>)> {
            let SqlExpr::Function { name, args } = expr else {
                return None;
            };
            if !name.eq_ignore_ascii_case("SUM") || args.len() != 1 {
                return None;
            }
            let SqlExpr::Case {
                when_then,
                else_expr,
            } = &args[0]
            else {
                return None;
            };
            if when_then.len() != 1
                || numeric_literal(&when_then[0].1) != Some(1.0)
                || else_expr.as_deref().and_then(numeric_literal) != Some(0.0)
            {
                return None;
            }
            match &when_then[0].0 {
                SqlExpr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                } => match (left.as_ref(), right.as_ref()) {
                    (SqlExpr::Column(column), SqlExpr::Literal(Value::String(value)))
                    | (SqlExpr::Literal(Value::String(value)), SqlExpr::Column(column)) => {
                        Some((clean_name(column).to_string(), vec![value.clone()]))
                    }
                    _ => None,
                },
                SqlExpr::In {
                    column,
                    values,
                    negated: false,
                } => {
                    let mut literals = Vec::with_capacity(values.len());
                    for value in values {
                        match value {
                            Value::String(value) => literals.push(value.clone()),
                            _ => return None,
                        }
                    }
                    Some((clean_name(column).to_string(), literals))
                }
                _ => None,
            }
        }

        fn aggregate_output_name(
            func: &AggregateFunc,
            column: Option<&String>,
            alias: &Option<String>,
        ) -> String {
            if let Some(alias) = alias {
                return alias.clone();
            }
            match (func, column) {
                (AggregateFunc::Count, None) => "COUNT(*)".to_string(),
                (AggregateFunc::Count, Some(column)) => format!("COUNT({})", clean_name(column)),
                (AggregateFunc::Max, Some(column)) => format!("MAX({})", clean_name(column)),
                _ => "aggregate".to_string(),
            }
        }

        enum NativeOutput {
            Key(String),
            Count(String),
            Distinct(String, usize),
            CaseCount(String, usize),
            MaxString(String, usize),
        }

        let group_col = clean_name(&stmt.group_by[0]);
        let mut outputs = Vec::with_capacity(stmt.columns.len());
        let mut distinct_cols: Vec<String> = Vec::new();
        let mut case_specs: Vec<(String, Vec<String>)> = Vec::new();
        let mut max_cols: Vec<String> = Vec::new();
        let mut having_columns = std::collections::HashMap::new();

        for column in &stmt.columns {
            match column {
                SelectColumn::Column(name) => {
                    if clean_name(name) != group_col {
                        return Ok(None);
                    }
                    outputs.push(NativeOutput::Key(clean_name(name).to_string()));
                }
                SelectColumn::ColumnAlias { column, alias } => {
                    if clean_name(column) != group_col {
                        return Ok(None);
                    }
                    outputs.push(NativeOutput::Key(alias.clone()));
                }
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct,
                    alias,
                } => match (func, column, distinct) {
                    (AggregateFunc::Count, None, false) => {
                        let name = aggregate_output_name(func, column.as_ref(), alias);
                        having_columns.insert("COUNT(*)".to_string(), name.clone());
                        outputs.push(NativeOutput::Count(name));
                    }
                    (AggregateFunc::Count, Some(column), false)
                        if column == "*" || column.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) =>
                    {
                        let name = aggregate_output_name(func, None, alias);
                        having_columns.insert("COUNT(*)".to_string(), name.clone());
                        outputs.push(NativeOutput::Count(name));
                    }
                    (AggregateFunc::Count, Some(column), true) => {
                        let slot = distinct_cols.len();
                        let actual = clean_name(column).to_string();
                        // The native distinct accumulator below hashes decoded
                        // string slices. Numeric DISTINCT must stay on the
                        // typed general path or its encoded bytes can be
                        // mistaken for one value per row.
                        if !Self::column_is_string(backend, &actual) {
                            return Ok(None);
                        }
                        distinct_cols.push(actual.clone());
                        outputs.push(NativeOutput::Distinct(
                            alias
                                .clone()
                                .unwrap_or_else(|| format!("COUNT(DISTINCT {})", actual)),
                            slot,
                        ));
                    }
                    (AggregateFunc::Max, Some(column), false) => {
                        let slot = max_cols.len();
                        max_cols.push(clean_name(column).to_string());
                        outputs.push(NativeOutput::MaxString(
                            aggregate_output_name(func, Some(column), alias),
                            slot,
                        ));
                    }
                    _ => return Ok(None),
                },
                SelectColumn::Expression { expr, alias } => {
                    let Some(alias) = alias.clone() else {
                        return Ok(None);
                    };
                    let Some((case_col, literals)) = case_counter(expr) else {
                        return Ok(None);
                    };
                    let slot = case_specs.len();
                    case_specs.push((case_col, literals));
                    outputs.push(NativeOutput::CaseCount(alias, slot));
                }
                _ => return Ok(None),
            }
        }

        if outputs.is_empty() {
            return Ok(None);
        }

        // Plain COUNT/AVG/SUM groups are already handled by the typed
        // dictionary cache below in `try_fast_cached_group_by`.  The native
        // string accumulator is intended for the additional DISTINCT/CASE/
        // MAX shapes that cached group statistics cannot answer; do not let
        // it replace the cheaper established path for the common case.
        if distinct_cols.is_empty() && case_specs.is_empty() && max_cols.is_empty() {
            return Ok(None);
        }
        if backend.column_has_nulls(group_col) {
            return Ok(None);
        }

        // HAVING may reference aggregates that are not projected. This fast
        // path only materializes its explicit output columns, so let the
        // general executor supply any implicit aggregate instead of failing
        // while evaluating it against an incomplete batch.
        fn having_refs_available(
            expr: &SqlExpr,
            columns: &std::collections::HashMap<String, String>,
        ) -> bool {
            match expr {
                SqlExpr::Function { name, .. }
                    if matches!(
                        name.to_ascii_uppercase().as_str(),
                        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
                    ) =>
                {
                    name.eq_ignore_ascii_case("COUNT") && columns.contains_key("COUNT(*)")
                }
                SqlExpr::BinaryOp { left, right, .. } => {
                    having_refs_available(left, columns)
                        && having_refs_available(right, columns)
                }
                SqlExpr::UnaryOp { expr, .. }
                | SqlExpr::Paren(expr)
                | SqlExpr::Cast { expr, .. } => having_refs_available(expr, columns),
                _ => true,
            }
        }
        if stmt
            .having
            .as_ref()
            .is_some_and(|having| !having_refs_available(having, &having_columns))
        {
            return Ok(None);
        }

        let distinct_refs: Vec<&str> = distinct_cols.iter().map(|s| s.as_str()).collect();
        let max_refs: Vec<&str> = max_cols.iter().map(|s| s.as_str()).collect();
        let case_refs: Vec<(&str, Vec<String>)> = case_specs
            .iter()
            .map(|(col, literals)| (col.as_str(), literals.clone()))
            .collect();
        let raw = match backend.execute_string_group_distinct_case_agg(
            group_col,
            &distinct_refs,
            &case_refs,
            &max_refs,
        )? {
            Some(raw) if !raw.is_empty() => raw,
            _ => return Ok(None),
        };

        let mut fields = Vec::with_capacity(outputs.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(outputs.len());
        for output in outputs {
            match output {
                NativeOutput::Key(name) => {
                    fields.push(Field::new(&name, ArrowDataType::Utf8, false));
                    arrays.push(Arc::new(StringArray::from(
                        raw.iter().map(|row| row.key.as_str()).collect::<Vec<_>>(),
                    )) as ArrayRef);
                }
                NativeOutput::Count(name) => {
                    fields.push(Field::new(&name, ArrowDataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(
                        raw.iter().map(|row| row.count).collect::<Vec<_>>(),
                    )) as ArrayRef);
                }
                NativeOutput::Distinct(name, slot) => {
                    fields.push(Field::new(&name, ArrowDataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(
                        raw.iter()
                            .map(|row| row.distinct_counts[slot])
                            .collect::<Vec<_>>(),
                    )) as ArrayRef);
                }
                NativeOutput::CaseCount(name, slot) => {
                    fields.push(Field::new(&name, ArrowDataType::Float64, true));
                    arrays.push(Arc::new(Float64Array::from(
                        raw.iter()
                            .map(|row| Some(row.case_counts[slot] as f64))
                            .collect::<Vec<_>>(),
                    )) as ArrayRef);
                }
                NativeOutput::MaxString(name, slot) => {
                    fields.push(Field::new(&name, ArrowDataType::Utf8, true));
                    arrays.push(Arc::new(StringArray::from(
                        raw.iter()
                            .map(|row| row.max_values[slot].clone())
                            .collect::<Vec<_>>(),
                    )) as ArrayRef);
                }
            }
        }

        let mut batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| err_data(e.to_string()))?;
        if let Some(having) = &stmt.having {
            fn resolve_count_refs(
                expr: &SqlExpr,
                columns: &std::collections::HashMap<String, String>,
            ) -> SqlExpr {
                match expr {
                    SqlExpr::Function { name, .. } if name.eq_ignore_ascii_case("COUNT") => {
                        columns
                            .get("COUNT(*)")
                            .map(|column| SqlExpr::Column(column.clone()))
                            .unwrap_or_else(|| expr.clone())
                    }
                    SqlExpr::BinaryOp { left, op, right } => SqlExpr::BinaryOp {
                        left: Box::new(resolve_count_refs(left, columns)),
                        op: op.clone(),
                        right: Box::new(resolve_count_refs(right, columns)),
                    },
                    SqlExpr::UnaryOp { op, expr } => SqlExpr::UnaryOp {
                        op: op.clone(),
                        expr: Box::new(resolve_count_refs(expr, columns)),
                    },
                    SqlExpr::Paren(expr) => {
                        SqlExpr::Paren(Box::new(resolve_count_refs(expr, columns)))
                    }
                    _ => expr.clone(),
                }
            }
            let resolved = resolve_count_refs(having, &having_columns);
            let mask = Self::evaluate_predicate(&batch, &resolved)?;
            batch = arrow::compute::filter_record_batch(&batch, &mask)
                .map_err(|error| err_data(error.to_string()))?;
        }
        if !stmt.order_by.is_empty() {
            let order_by = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let top_k = stmt.limit.map(|limit| limit + stmt.offset.unwrap_or(0));
            batch = Self::apply_order_by_topk(&batch, &order_by, top_k)?;
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            batch = Self::apply_limit_offset(&batch, stmt.limit, stmt.offset)?;
        }
        Ok(Some(if batch.num_rows() == 0 {
            ApexResult::Empty(batch.schema())
        } else {
            ApexResult::Data(batch)
        }))
    }

    /// Cached string GROUP BY with multiple numeric SUM(CASE) counters and
    /// ordinary COUNT/SUM/AVG aggregates. Each projected numeric column is
    /// scanned at storage level without materializing a table-wide Arrow batch.
    fn try_fast_cached_numeric_case_group_by(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
            || stmt.group_by.len() != 1
            || stmt.group_by_exprs.iter().any(Option::is_some)
            || stmt.where_clause.is_some()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || stmt.distinct
            || stmt.distinct_on.is_some()
        {
            return Ok(None);
        }

        fn clean_name(name: &str) -> &str {
            let trimmed = name.trim_matches('"');
            trimmed
                .rsplit('.')
                .next()
                .unwrap_or(trimmed)
                .trim_matches('"')
        }
        fn numeric_literal(expr: &SqlExpr) -> Option<f64> {
            match expr {
                SqlExpr::Literal(Value::Int64(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Int32(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Float64(value)) => Some(*value),
                SqlExpr::Literal(Value::Float32(value)) => Some(*value as f64),
                _ => None,
            }
        }

        enum Output {
            Key(String),
            Case(String, usize),
            Aggregate(String, AggregateFunc, usize),
        }

        let group_col = clean_name(&stmt.group_by[0]);
        if !Self::column_is_string(backend, group_col) || backend.column_has_nulls(group_col) {
            return Ok(None);
        }
        let mut outputs = Vec::with_capacity(stmt.columns.len());
        let mut cases: Vec<(String, f64, f64)> = Vec::new();
        let mut aggregates: Vec<(String, bool)> = Vec::new();
        for column in &stmt.columns {
            match column {
                SelectColumn::Column(name) if clean_name(name) == group_col => {
                    outputs.push(Output::Key(Self::group_output_name(stmt, group_col)));
                }
                SelectColumn::ColumnAlias { column, alias }
                    if clean_name(column) == group_col =>
                {
                    outputs.push(Output::Key(alias.clone()));
                }
                SelectColumn::Expression { expr, alias } => {
                    let SqlExpr::Function { name, args } = expr else {
                        return Ok(None);
                    };
                    if !name.eq_ignore_ascii_case("SUM") || args.len() != 1 {
                        return Ok(None);
                    }
                    let SqlExpr::Case { when_then, else_expr } = &args[0] else {
                        return Ok(None);
                    };
                    if when_then.len() != 1
                        || numeric_literal(&when_then[0].1) != Some(1.0)
                        || else_expr.as_deref().and_then(numeric_literal) != Some(0.0)
                    {
                        return Ok(None);
                    }
                    let Some((filter_col, low, high)) =
                        Self::extract_any_numeric_range(&when_then[0].0)
                    else {
                        return Ok(None);
                    };
                    let slot = cases.len();
                    cases.push((filter_col, low, high));
                    outputs.push(Output::Case(
                        alias.clone().unwrap_or_else(|| format!("expr{}", slot + 1)),
                        slot,
                    ));
                }
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct: false,
                    alias,
                } if matches!(
                    func,
                    AggregateFunc::Count | AggregateFunc::Sum | AggregateFunc::Avg
                ) => {
                    let count_star = matches!(func, AggregateFunc::Count)
                        && column.as_ref().map_or(true, |column| {
                            column == "*"
                                || column.chars().next().is_some_and(|c| c.is_ascii_digit())
                        });
                    let source = if count_star {
                        "*".to_string()
                    } else {
                        let Some(column) = column else {
                            return Ok(None);
                        };
                        clean_name(column).to_string()
                    };
                    let slot = aggregates.len();
                    aggregates.push((source.clone(), count_star));
                    let function = match func {
                        AggregateFunc::Count => "COUNT",
                        AggregateFunc::Sum => "SUM",
                        AggregateFunc::Avg => "AVG",
                        _ => unreachable!(),
                    };
                    outputs.push(Output::Aggregate(
                        alias.clone().unwrap_or_else(|| format!("{}({})", function, source)),
                        func.clone(),
                        slot,
                    ));
                }
                _ => return Ok(None),
            }
        }
        if cases.is_empty() {
            return Ok(None);
        }

        let Some(dictionary) = crate::storage::backend::get_global_dict_cache(
            backend.path(),
            group_col,
            &backend.storage,
        )? else {
            return Ok(None);
        };
        if dictionary.1.len() != backend.active_row_count() as usize {
            return Ok(None);
        }

        let mut case_values = Vec::with_capacity(cases.len());
        for (filter_col, low, high) in &cases {
            if let Some((numeric_dictionary, has_nulls, max_group_id)) =
                crate::storage::backend::get_global_numeric_dict_cache(
                    backend.path(),
                    filter_col,
                    &backend.storage,
                )?
            {
                if !has_nulls
                    && numeric_dictionary.1.len() == dictionary.1.len()
                    && max_group_id
                        .is_none_or(|id| (id as usize) < numeric_dictionary.0.len())
                {
                    let matching: Option<Vec<bool>> = numeric_dictionary
                        .0
                        .iter()
                        .map(|label| {
                            label.parse::<i64>().ok().map(|value| {
                                (value as f64) >= *low && (value as f64) <= *high
                            })
                        })
                        .collect();
                    if let Some(matching) = matching {
                        let mut counts = vec![0i64; dictionary.0.len()];
                        for (&group_id, &numeric_id) in
                            dictionary.1.iter().zip(&numeric_dictionary.1)
                        {
                            if matching[numeric_id as usize] {
                                counts[group_id as usize] += 1;
                            }
                        }
                        case_values.push(counts);
                        continue;
                    }
                }
            }
            let Some(rows) = backend.execute_between_group_agg_cached(
                filter_col,
                *low,
                *high,
                &dictionary.0,
                &dictionary.1,
                None,
            )? else {
                return Ok(None);
            };
            let counts: std::collections::HashMap<_, _> = rows
                .into_iter()
                .map(|(group, _, count)| (group, count))
                .collect();
            case_values.push(
                dictionary
                    .0
                    .iter()
                    .map(|group| counts.get(group).copied().unwrap_or(0))
                    .collect::<Vec<_>>(),
            );
        }

        let aggregate_values = if aggregates.is_empty() {
            None
        } else {
            let refs: Vec<(&str, bool)> = aggregates
                .iter()
                .map(|(column, count_star)| (column.as_str(), *count_star))
                .collect();
            let Some(rows) = backend.execute_group_agg_cached(
                &dictionary.0,
                &dictionary.1,
                &refs,
            )? else {
                return Ok(None);
            };
            let values: std::collections::HashMap<_, _> = rows.into_iter().collect();
            Some(values)
        };

        let mut fields = Vec::with_capacity(outputs.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(outputs.len());
        for output in outputs {
            match output {
                Output::Key(name) => {
                    fields.push(Field::new(name, ArrowDataType::Utf8, false));
                    arrays.push(Arc::new(StringArray::from(
                        dictionary.0.iter().map(String::as_str).collect::<Vec<_>>(),
                    )));
                }
                Output::Case(name, slot) => {
                    fields.push(Field::new(name, ArrowDataType::Float64, true));
                    arrays.push(Arc::new(Float64Array::from(
                        case_values[slot]
                            .iter()
                            .map(|&count| Some(count as f64))
                            .collect::<Vec<_>>(),
                    )));
                }
                Output::Aggregate(name, func, slot) => {
                    let values = aggregate_values.as_ref().unwrap();
                    if matches!(func, AggregateFunc::Count) {
                        fields.push(Field::new(name, ArrowDataType::Int64, false));
                        arrays.push(Arc::new(Int64Array::from(
                            dictionary
                                .0
                                .iter()
                                .map(|group| values.get(group).map_or(0, |stats| stats[slot].1))
                                .collect::<Vec<_>>(),
                        )));
                    } else {
                        fields.push(Field::new(name, ArrowDataType::Float64, true));
                        arrays.push(Arc::new(Float64Array::from(
                            dictionary
                                .0
                                .iter()
                                .map(|group| {
                                    let (sum, count) = values
                                        .get(group)
                                        .map_or((0.0, 0), |stats| stats[slot]);
                                    (count > 0).then_some(if matches!(func, AggregateFunc::Avg) {
                                        sum / count as f64
                                    } else {
                                        sum
                                    })
                                })
                                .collect::<Vec<_>>(),
                        )));
                    }
                }
            }
        }
        let mut batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| err_data(error.to_string()))?;
        if !stmt.order_by.is_empty() {
            let resolved = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let top_k = stmt.limit.map(|limit| limit + stmt.offset.unwrap_or(0));
            batch = Self::apply_order_by_topk(&batch, &resolved, top_k)?;
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            batch = Self::apply_limit_offset(&batch, stmt.limit, stmt.offset)?;
        }
        Ok(Some(ApexResult::Data(batch)))
    }

    /// V4 FAST PATH: Cached GROUP BY (builds dict cache on first call, reuses on subsequent calls)
    fn try_fast_cached_case_count(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || stmt.group_by.len() != 1
            || stmt.having.is_some()
            || stmt.group_by_exprs.iter().any(Option::is_some)
            || stmt.order_by.iter().any(|clause| clause.expr.is_some())
            || stmt.where_clause.is_some()
        {
            return Ok(None);
        }
        let group_col = stmt.group_by[0].trim_matches('"');

        // Columns: the group column plus one COUNT(CASE WHEN <range> THEN 1 END)
        // with no ELSE (an ELSE makes every non-null condition row count, which
        // is the group size and not a range count).
        let mut case_alias: Option<String> = None;
        let mut range: Option<(String, f64, f64)> = None;
        for column in &stmt.columns {
            match column {
                SelectColumn::Column(name) if name.trim_matches('"') == group_col => {}
                SelectColumn::ColumnAlias { column, .. }
                    if column.trim_matches('"') == group_col => {}
                SelectColumn::Expression { expr, alias } => {
                    if range.is_some() {
                        return Ok(None);
                    }
                    let SqlExpr::Function { name, args } = expr else {
                        return Ok(None);
                    };
                    if !name.eq_ignore_ascii_case("COUNT") || args.len() != 1 {
                        return Ok(None);
                    }
                    let Some((cond, count_true)) = Self::count_case_condition(&args[0]) else {
                        return Ok(None);
                    };
                    if !count_true {
                        return Ok(None);
                    }
                    let Some((col, lo, hi)) = Self::extract_any_numeric_range(&cond) else {
                        return Ok(None);
                    };
                    range = Some((col, lo, hi));
                    case_alias = alias.clone();
                }
                _ => return Ok(None),
            }
        }
        let Some((filter_col, lo, hi)) = range else {
            return Ok(None);
        };
        if backend.column_has_nulls(&filter_col) || backend.column_has_nulls(group_col) {
            return Ok(None);
        }

        let Some(dict_arc) = crate::storage::backend::get_global_dict_cache(
            backend.path(),
            group_col,
            &backend.storage,
        )? else {
            return Ok(None);
        };
        let Some(raw) = backend.execute_between_group_agg_cached(
            &filter_col,
            lo,
            hi,
            &dict_arc.0,
            &dict_arc.1,
            None,
        )? else {
            return Ok(None);
        };

        // `raw` only lists groups with a non-zero CASE count, but GROUP BY
        // must emit every group (with 0 for groups that match no row).
        let count_map: std::collections::HashMap<&str, i64> = raw
            .iter()
            .map(|(group, _, count)| (group.as_str(), *count))
            .collect();
        let group_name = Self::group_output_name(stmt, group_col);
        let count_name = case_alias.unwrap_or_else(|| "expr".to_string());
        let fields = vec![
            Field::new(&group_name, ArrowDataType::Utf8, false),
            Field::new(&count_name, ArrowDataType::Int64, false),
        ];
        let group_values: Vec<&str> = dict_arc.0.iter().map(|s| s.as_str()).collect();
        let counts: Vec<i64> = dict_arc
            .0
            .iter()
            .map(|s| count_map.get(s.as_str()).copied().unwrap_or(0))
            .collect();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(group_values)),
            Arc::new(Int64Array::from(counts)),
        ];
        let schema = Arc::new(Schema::new(fields));
        let mut batch = RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;

        if !stmt.order_by.is_empty() {
            let resolved_ob = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
            batch = Self::apply_order_by_topk(&batch, &resolved_ob, k)?;
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            batch = Self::apply_limit_offset(&batch, stmt.limit, stmt.offset)?;
        }
        Ok(Some(ApexResult::Data(batch)))
    }

    /// V4 FAST PATH: Cached GROUP BY (builds dict cache on first call, reuses on subsequent calls)
    fn try_fast_order_by_length(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
        storage_path: &Path,
    ) -> io::Result<Option<ApexResult>> {
        if stmt.where_clause.is_some()
            || !stmt.group_by.is_empty()
            || stmt.distinct
            || stmt.offset.is_some()
            || stmt.having.is_some()
        {
            return Ok(None);
        }
        let Some(limit) = stmt.limit else {
            return Ok(None);
        };
        // Single ORDER BY key: LENGTH(col), optionally followed by one tie column.
        if stmt.order_by.is_empty() || stmt.order_by.len() > 2 {
            return Ok(None);
        }
        let first = &stmt.order_by[0];
        let Some(expr) = &first.expr else {
            return Ok(None);
        };
        let SqlExpr::Function { name, args } = expr else {
            return Ok(None);
        };
        if !name.eq_ignore_ascii_case("LENGTH") || args.len() != 1 {
            return Ok(None);
        }
        let SqlExpr::Column(length_col) = &args[0] else {
            return Ok(None);
        };
        let length_col = length_col
            .trim_matches('"')
            .rsplit('.')
            .next()
            .unwrap_or(length_col.trim_matches('"'))
            .to_string();
        let tie_col = if stmt.order_by.len() == 2 {
            let second = &stmt.order_by[1];
            if second.expr.is_some() {
                return Ok(None);
            }
            let c = second.column.trim_matches('"');
            let c = c.rsplit('.').next().unwrap_or(c);
            Some((c.to_string(), second.descending))
        } else {
            None
        };

        let tie_col_name = tie_col.as_ref().map(|(c, _)| c.as_str());
        let tie_desc = tie_col.as_ref().map_or(false, |(_, d)| *d);
        let Some(indices) = backend.scan_top_k_by_length_mmap(
            &length_col,
            tie_col_name,
            limit,
            first.descending,
            tie_desc,
        )? else {
            return Ok(None);
        };

        // Read the SELECT columns for the top-k rows only.
        let select_cols: Option<Vec<String>> = if stmt.is_select_star() {
            None
        } else {
            let mut cols: Vec<String> = Vec::new();
            for column in &stmt.columns {
                match column {
                    SelectColumn::Column(name) | SelectColumn::ColumnAlias { column: name, .. } => {
                        let plain = name
                            .trim_matches('"')
                            .rsplit('.')
                            .next()
                            .unwrap_or(name.trim_matches('"'))
                            .to_string();
                        if !cols.contains(&plain) {
                            cols.push(plain);
                        }
                    }
                    _ => return Ok(None),
                }
            }
            if cols.is_empty() {
                return Ok(None);
            }
            Some(cols)
        };
        let select_refs: Option<Vec<&str>> = select_cols
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.as_str()).collect());
        let batch = backend.read_columns_by_indices_to_arrow(&indices, select_refs.as_deref())?;
        let projected =
            Self::apply_projection_with_storage(&batch, &stmt.columns, Some(storage_path))?;
        Ok(Some(ApexResult::Data(projected)))
    }

    /// V4 FAST PATH: Cached GROUP BY (builds dict cache on first call, reuses on subsequent calls)
    fn try_fast_cached_group_by(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
        {
            return Ok(None);
        }

        use crate::query::AggregateFunc;

        // Must be 1 or 2 GROUP BY columns, no WHERE.
        // ORDER BY/LIMIT can be applied after the cached aggregate result is built.
        if stmt.group_by.is_empty()
            || stmt.group_by.len() > 2
            || stmt.group_by_exprs.iter().any(Option::is_some)
            || stmt.where_clause.is_some()
        {
            return Ok(None);
        }

        // Handle 2-column GROUP BY as a separate fast path
        if stmt.group_by.len() == 2 {
            return Self::try_fast_cached_group_by_2col(backend, stmt);
        }

        // Must be single GROUP BY column, no WHERE
        if stmt.group_by.len() != 1 {
            return Ok(None);
        }

        let group_col = stmt.group_by[0].trim_matches('"');

        // Extract aggregate info
        let mut agg_info: Vec<(&str, bool, AggregateFunc, Option<String>)> = Vec::new();
        for col in &stmt.columns {
            match col {
                SelectColumn::Aggregate {
                    func,
                    column,
                    alias,
                    distinct: false,
                } => {
                    // The storage cached path only tracks (sum, count) per group,
                    // so MIN/MAX cannot be answered here.  Let the Arrow path run.
                    if matches!(
                        func,
                        AggregateFunc::Min | AggregateFunc::Max
                    ) {
                        return Ok(None);
                    }
                    let is_count_star = matches!(func, AggregateFunc::Count) && column.is_none();
                    let col_name = column.as_deref().unwrap_or("*");
                    agg_info.push((col_name, is_count_star, func.clone(), alias.clone()));
                }
                SelectColumn::Column(name) => {
                    if name.trim_matches('"') == group_col {
                        continue;
                    }
                    return Ok(None);
                }
                SelectColumn::ColumnAlias { column, .. } => {
                    if column.trim_matches('"') == group_col {
                        continue;
                    }
                    return Ok(None);
                }
                _ => return Ok(None),
            }
        }
        if agg_info.is_empty() {
            return Ok(None);
        }

        // Get or build cached dict (global cache — survives backend reopens)
        let (dict_arc, group_has_nulls, _) =
            match crate::storage::backend::get_global_dict_cache_with_nulls(
            backend.path(),
            group_col,
            &backend.storage,
        )? {
            Some(c) => c,
            None => return Ok(None),
        };
        if group_has_nulls {
            return Ok(None);
        }
        // The cached dictionary must cover the CURRENT active row set.  A
        // stale entry (e.g. built before a later insert) has fewer group ids
        // than rows and would silently aggregate only the prefix of the table.
        if dict_arc.1.len() != backend.active_row_count() as usize {
            return Ok(None);
        }
        let (dict_strings, group_ids) = (dict_arc.0.as_slice(), dict_arc.1.as_slice());

        let agg_cols: Vec<(&str, bool)> = agg_info
            .iter()
            .map(|(col, is_count, _, _)| (*col, *is_count))
            .collect();

        let raw = match backend.execute_group_agg_cached(dict_strings, group_ids, &agg_cols)? {
            Some(r) if !r.is_empty() => r,
            _ => return Ok(None),
        };

        // Build result
        let group_values: Vec<&str> = raw.iter().map(|(k, _)| k.as_str()).collect();

        let mut fields: Vec<Field> = vec![Field::new(group_col, ArrowDataType::Utf8, false)];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(group_values))];

        for (ai, (agg_col, _, func, alias)) in agg_info.iter().enumerate() {
            let output_name;
            let col_name = match alias {
                Some(alias) => alias.as_str(),
                None => {
                    output_name = match func {
                        AggregateFunc::Count if *agg_col == "*" => "COUNT(*)".to_string(),
                        AggregateFunc::Count => format!("COUNT({})", agg_col),
                        AggregateFunc::Avg => format!("AVG({})", agg_col),
                        AggregateFunc::Sum => format!("SUM({})", agg_col),
                        AggregateFunc::Min => format!("MIN({})", agg_col),
                        AggregateFunc::Max => format!("MAX({})", agg_col),
                    };
                    output_name.as_str()
                }
            };
            let values: Vec<f64> = raw
                .iter()
                .map(|(_, aggs)| {
                    let (sum, count) = aggs[ai];
                    match func {
                        AggregateFunc::Count => count as f64,
                        AggregateFunc::Avg => {
                            if count > 0 {
                                sum / count as f64
                            } else {
                                0.0
                            }
                        }
                        _ => sum,
                    }
                })
                .collect();
            if matches!(func, AggregateFunc::Count) {
                let int_values: Vec<i64> = values.iter().map(|v| *v as i64).collect();
                fields.push(Field::new(col_name, ArrowDataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(int_values)));
            } else {
                fields.push(Field::new(col_name, ArrowDataType::Float64, false));
                arrays.push(Arc::new(Float64Array::from(values)));
            }
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;

        // Apply HAVING
        let mut result = if let Some(having) = &stmt.having {
            let mask = Self::evaluate_predicate(&batch, having)?;
            let filtered = arrow::compute::filter_record_batch(&batch, &mask)
                .map_err(|e| err_data(e.to_string()))?;
            if filtered.num_rows() == 0 {
                return Ok(Some(ApexResult::Empty(filtered.schema())));
            }
            filtered
        } else {
            batch
        };

        if !stmt.order_by.is_empty() {
            let resolved_ob = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
            result = Self::apply_order_by_topk(&result, &resolved_ob, k)?;
        }

        if stmt.limit.is_some() || stmt.offset.is_some() {
            result = Self::apply_limit_offset(&result, stmt.limit, stmt.offset)?;
        }

        Ok(Some(ApexResult::Data(result)))
    }

    /// 2-column GROUP BY fast path using dict caches for both columns.
    fn try_fast_cached_group_by_2col(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        let group_col1 = stmt.group_by[0].trim_matches('"');
        let group_col2 = stmt.group_by[1].trim_matches('"');

        // Extract aggregate info (support multiple aggregates)
        let mut agg_info: Vec<(&str, bool, AggregateFunc, Option<String>)> = Vec::new();
        for col in &stmt.columns {
            match col {
                SelectColumn::Aggregate {
                    func,
                    column,
                    alias,
                    distinct: false,
                } => {
                    let is_count_star = matches!(func, AggregateFunc::Count) && column.is_none();
                    let col_name = column.as_deref().unwrap_or("*");
                    agg_info.push((col_name, is_count_star, func.clone(), alias.clone()));
                }
                SelectColumn::Column(name) => {
                    let n = name.trim_matches('"');
                    if n == group_col1 || n == group_col2 {
                        continue;
                    }
                    return Ok(None);
                }
                SelectColumn::ColumnAlias { column, .. } => {
                    let n = column.trim_matches('"');
                    if n == group_col1 || n == group_col2 {
                        continue;
                    }
                    return Ok(None);
                }
                _ => return Ok(None),
            }
        }
        if agg_info.is_empty() {
            return Ok(None);
        }

        // Get dict caches for both group columns
        let group_cache = |column: &str| {
            if Self::column_is_string(backend, column) {
                crate::storage::backend::get_global_dict_cache_with_nulls(
                    backend.path(),
                    column,
                    &backend.storage,
                )
            } else {
                crate::storage::backend::get_global_numeric_dict_cache(
                    backend.path(),
                    column,
                    &backend.storage,
                )
            }
        };
        let (dict1_arc, dict1_has_nulls, dict1_max_group_id) = match group_cache(group_col1)? {
            Some(c) => c,
            None => return Ok(None),
        };
        let (dict2_arc, dict2_has_nulls, dict2_max_group_id) = match group_cache(group_col2)? {
            Some(c) => c,
            None => return Ok(None),
        };
        if dict1_has_nulls || dict2_has_nulls {
            return Ok(None);
        }
        if dict1_max_group_id
            .is_some_and(|max_id| max_id as usize >= dict1_arc.0.len())
            || dict2_max_group_id
                .is_some_and(|max_id| max_id as usize >= dict2_arc.0.len())
        {
            return Ok(None);
        }
        // Stale caches (built before a later insert) hold fewer group ids than
        // active rows and would aggregate only the row prefix.
        if dict1_arc.1.len() != backend.active_row_count() as usize
            || dict2_arc.1.len() != backend.active_row_count() as usize
        {
            return Ok(None);
        }

        let (dict1_strings, group_ids1) = (dict1_arc.0.as_slice(), dict1_arc.1.as_slice());
        let (dict2_strings, group_ids2) = (dict2_arc.0.as_slice(), dict2_arc.1.as_slice());

        let agg_cols: Vec<(&str, bool)> = agg_info
            .iter()
            .map(|(col, is_count, _, _)| (*col, *is_count))
            .collect();

        if agg_info
            .iter()
            .any(|(_, _, func, _)| matches!(func, AggregateFunc::Min | AggregateFunc::Max))
        {
            if agg_info
                .iter()
                .any(|(column, count_star, _, _)| !count_star && backend.column_has_nulls(column))
            {
                return Ok(None);
            }
            let Some(raw) = backend.execute_group_stats_2col_cached(
                group_ids1,
                group_ids2,
                dict2_strings.len(),
                &agg_cols,
            )? else {
                return Ok(None);
            };
            let mut fields = Vec::with_capacity(2 + agg_info.len());
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(2 + agg_info.len());
            for (column, first) in [(group_col1, true), (group_col2, false)] {
                let output_name = Self::group_output_name(stmt, column);
                let values_for = |slot: usize| {
                    let group1 = slot / dict2_strings.len();
                    let group2 = slot % dict2_strings.len();
                    if first { group1 } else { group2 }
                };
                if Self::column_is_string(backend, column) {
                    let dictionary = if first { dict1_strings } else { dict2_strings };
                    fields.push(Field::new(output_name, ArrowDataType::Utf8, false));
                    arrays.push(Arc::new(StringArray::from(
                        raw.iter()
                            .map(|(slot, _)| dictionary[values_for(*slot)].as_str())
                            .collect::<Vec<_>>(),
                    )));
                } else {
                    let dictionary = if first { dict1_strings } else { dict2_strings };
                    let values: Option<Vec<i64>> = raw
                        .iter()
                        .map(|(slot, _)| dictionary[values_for(*slot)].parse::<i64>().ok())
                        .collect();
                    let Some(values) = values else {
                        return Ok(None);
                    };
                    fields.push(Field::new(output_name, ArrowDataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(values)));
                }
            }
            for (aggregate, (_, _, func, alias)) in agg_info.iter().enumerate() {
                let output_name = alias.clone().unwrap_or_else(|| {
                    let source = agg_info[aggregate].0;
                    match func {
                        AggregateFunc::Count => format!("COUNT({source})"),
                        AggregateFunc::Sum => format!("SUM({source})"),
                        AggregateFunc::Avg => format!("AVG({source})"),
                        AggregateFunc::Min => format!("MIN({source})"),
                        AggregateFunc::Max => format!("MAX({source})"),
                    }
                });
                match func {
                    AggregateFunc::Count => {
                        fields.push(Field::new(output_name, ArrowDataType::Int64, false));
                        arrays.push(Arc::new(Int64Array::from(
                            raw.iter().map(|(_, stats)| stats[aggregate].0).collect::<Vec<_>>(),
                        )));
                    }
                    AggregateFunc::Avg => {
                        fields.push(Field::new(output_name, ArrowDataType::Float64, true));
                        arrays.push(Arc::new(Float64Array::from(
                            raw.iter()
                                .map(|(_, stats)| {
                                    let stat = stats[aggregate];
                                    (stat.0 > 0).then_some(stat.1 / stat.0 as f64)
                                })
                                .collect::<Vec<_>>(),
                        )));
                    }
                    AggregateFunc::Sum | AggregateFunc::Min | AggregateFunc::Max => {
                        let all_int = raw.iter().all(|(_, stats)| stats[aggregate].4);
                        if all_int {
                            fields.push(Field::new(output_name, ArrowDataType::Int64, true));
                            arrays.push(Arc::new(Int64Array::from(
                                raw.iter()
                                    .map(|(_, stats)| {
                                        let stat = stats[aggregate];
                                        (stat.0 > 0).then_some(match func {
                                            AggregateFunc::Sum => stat.1 as i64,
                                            AggregateFunc::Min => stat.2 as i64,
                                            AggregateFunc::Max => stat.3 as i64,
                                            _ => unreachable!(),
                                        })
                                    })
                                    .collect::<Vec<_>>(),
                            )));
                        } else {
                            fields.push(Field::new(output_name, ArrowDataType::Float64, true));
                            arrays.push(Arc::new(Float64Array::from(
                                raw.iter()
                                    .map(|(_, stats)| {
                                        let stat = stats[aggregate];
                                        (stat.0 > 0).then_some(match func {
                                            AggregateFunc::Sum => stat.1,
                                            AggregateFunc::Min => stat.2,
                                            AggregateFunc::Max => stat.3,
                                            _ => unreachable!(),
                                        })
                                    })
                                    .collect::<Vec<_>>(),
                            )));
                        }
                    }
                }
            }
            let mut batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
                .map_err(|error| err_data(error.to_string()))?;
            if !stmt.order_by.is_empty() {
                let order_by = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
                let top_k = stmt.limit.map(|limit| limit + stmt.offset.unwrap_or(0));
                batch = Self::apply_order_by_topk(&batch, &order_by, top_k)?;
            }
            if stmt.limit.is_some() || stmt.offset.is_some() {
                batch = Self::apply_limit_offset(&batch, stmt.limit, stmt.offset)?;
            }
            return Ok(Some(if batch.num_rows() == 0 {
                ApexResult::Empty(batch.schema())
            } else {
                ApexResult::Data(batch)
            }));
        }

        let raw = match backend.execute_group_agg_2col_cached(
            dict1_strings,
            group_ids1,
            dict2_strings,
            group_ids2,
            &agg_cols,
            true,
        )? {
            Some(r) if !r.is_empty() => r,
            _ => return Ok(None),
        };

        // Build result RecordBatch
        let mut fields: Vec<Field> = Vec::with_capacity(2 + agg_info.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(2 + agg_info.len());
        for (column, first) in [(group_col1, true), (group_col2, false)] {
            let output_name = Self::group_output_name(stmt, column);
            if Self::column_is_string(backend, column) {
                let values: Vec<&str> = raw
                    .iter()
                    .map(|((key1, key2), _)| if first { key1.as_str() } else { key2.as_str() })
                    .collect();
                fields.push(Field::new(output_name, ArrowDataType::Utf8, false));
                arrays.push(Arc::new(StringArray::from(values)));
            } else {
                let values: Option<Vec<i64>> = raw
                    .iter()
                    .map(|((key1, key2), _)| {
                        (if first { key1 } else { key2 }).parse::<i64>().ok()
                    })
                    .collect();
                let Some(values) = values else {
                    return Ok(None);
                };
                fields.push(Field::new(output_name, ArrowDataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(values)));
            }
        }

        for (ai, (_, _, func, alias)) in agg_info.iter().enumerate() {
            let col_name = alias.as_deref().unwrap_or(match func {
                AggregateFunc::Count => "COUNT(*)",
                AggregateFunc::Avg => "AVG",
                AggregateFunc::Sum => "SUM",
                AggregateFunc::Min => "MIN",
                AggregateFunc::Max => "MAX",
            });
            match func {
                AggregateFunc::Count => {
                    let vals: Vec<i64> = raw.iter().map(|(_, aggs)| aggs[ai].1).collect();
                    fields.push(Field::new(col_name, ArrowDataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(vals)));
                }
                AggregateFunc::Avg => {
                    let vals: Vec<f64> = raw
                        .iter()
                        .map(|(_, aggs)| {
                            let (sum, cnt) = aggs[ai];
                            if cnt > 0 {
                                sum / cnt as f64
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    fields.push(Field::new(col_name, ArrowDataType::Float64, false));
                    arrays.push(Arc::new(Float64Array::from(vals)));
                }
                AggregateFunc::Sum | AggregateFunc::Min | AggregateFunc::Max => {
                    let vals: Vec<f64> = raw.iter().map(|(_, aggs)| aggs[ai].0).collect();
                    fields.push(Field::new(col_name, ArrowDataType::Float64, false));
                    arrays.push(Arc::new(Float64Array::from(vals)));
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        let mut batch =
            RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;

        // Apply HAVING
        if let Some(having) = &stmt.having {
            let mask = Self::evaluate_predicate(&batch, having)?;
            batch = arrow::compute::filter_record_batch(&batch, &mask)
                .map_err(|e| err_data(e.to_string()))?;
        }

        if batch.num_rows() == 0 {
            return Ok(Some(ApexResult::Empty(batch.schema())));
        }
        if !stmt.order_by.is_empty() {
            let resolved = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let top_k = stmt.limit.map(|limit| limit + stmt.offset.unwrap_or(0));
            batch = Self::apply_order_by_topk(&batch, &resolved, top_k)?;
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            batch = Self::apply_limit_offset(&batch, stmt.limit, stmt.offset)?;
        }
        Ok(Some(ApexResult::Data(batch)))
    }

    /// MMAP fast path for AND of two numeric conditions on DIFFERENT columns.
    /// Example: WHERE age > 30 AND score > 50 [LIMIT n]
    /// Strategy: scan each column independently → merge-intersect sorted index sets → scatter read.
    fn try_fast_mmap_multi_condition(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
        storage_path: &Path,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::sql_parser::BinaryOperator;
        if !backend.is_mmap_only() || backend.has_pending_deltas() || backend.has_delta() {
            return Ok(None);
        }
        // Without LIMIT the result set can be very large; sequential Arrow scan is faster
        // than index intersection + scatter read for high-selectivity filters.
        if stmt.limit.is_none() {
            return Ok(None);
        }

        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        // Must be top-level AND
        let (left_cond, right_cond) = match where_clause {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => (left.as_ref(), right.as_ref()),
            _ => return Ok(None),
        };

        // --- Case A: numeric AND numeric (two different columns) ---
        if let (Some((col1, lo1, hi1)), Some((col2, lo2, hi2))) = (
            Self::extract_any_numeric_range(left_cond),
            Self::extract_any_numeric_range(right_cond),
        ) {
            if col1 != col2 {
                let idxs1 = match backend.scan_numeric_range_mmap(&col1, lo1, hi1, None)? {
                    Some(v) => v,
                    None => return Ok(None),
                };
                let idxs2 = match backend.scan_numeric_range_mmap(&col2, lo2, hi2, None)? {
                    Some(v) => v,
                    None => return Ok(None),
                };
                let mut intersected = Self::intersect_sorted_indices(&idxs1, &idxs2);
                let offset = stmt.offset.unwrap_or(0);
                if offset > 0 {
                    if offset >= intersected.len() {
                        return Ok(None);
                    }
                    intersected = intersected[offset..].to_vec();
                }
                if let Some(lim) = stmt.limit {
                    intersected.truncate(lim);
                }
                if intersected.is_empty() {
                    return Ok(None);
                }
                let batch = Self::read_matching_rows_adaptive(backend, stmt, &intersected)?;
                if !stmt.is_pure_star() {
                    let projected = Self::apply_projection_with_storage(
                        &batch,
                        &stmt.columns,
                        Some(storage_path),
                    )?;
                    return Ok(Some(ApexResult::Data(projected)));
                }
                return Ok(Some(ApexResult::Data(batch)));
            }
        }

        // --- Case B: string equality AND numeric range ---
        // Try both orderings: (str, num) and (num, str)
        let str_num = Self::extract_string_equality(left_cond)
            .and_then(|(sc, sv)| Self::extract_any_numeric_range(right_cond).map(|r| (sc, sv, r)))
            .or_else(|| {
                Self::extract_string_equality(right_cond).and_then(|(sc, sv)| {
                    Self::extract_any_numeric_range(left_cond).map(|r| (sc, sv, r))
                })
            });

        let (str_col, str_val, (num_col, num_lo, num_hi)) = match str_num {
            Some(v) => v,
            None => return Ok(None),
        };

        let str_indices = match backend.scan_string_filter_mmap(&str_col, &str_val, None)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let num_indices = match backend.scan_numeric_range_mmap(&num_col, num_lo, num_hi, None)? {
            Some(v) => v,
            None => return Ok(None),
        };

        let mut intersected = Self::intersect_sorted_indices(&str_indices, &num_indices);

        // Apply offset + limit
        let offset = stmt.offset.unwrap_or(0);
        if offset > 0 {
            if offset >= intersected.len() {
                return Ok(None);
            }
            intersected = intersected[offset..].to_vec();
        }
        if let Some(lim) = stmt.limit {
            intersected.truncate(lim);
        }

        if intersected.is_empty() {
            return Ok(None);
        }

        let batch = Self::read_matching_rows_adaptive(backend, stmt, &intersected)?;
        if !stmt.is_pure_star() {
            let projected =
                Self::apply_projection_with_storage(&batch, &stmt.columns, Some(storage_path))?;
            return Ok(Some(ApexResult::Data(projected)));
        }
        Ok(Some(ApexResult::Data(batch)))
    }

    /// Fused storage count for `COUNT(*) WHERE num NOT BETWEEN lo AND hi AND str NOT LIKE pat`.
    /// Counts both negated predicates in one mmap pass without Arrow materialization.
    fn try_fast_not_filter_count(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::sql_parser::BinaryOperator;
        if !backend.is_mmap_only() || backend.has_pending_deltas() || backend.has_delta() {
            return Ok(None);
        }
        if !Self::is_simple_count_star(stmt) {
            return Ok(None);
        }
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        let w = match where_clause {
            SqlExpr::Paren(inner) => inner.as_ref(),
            other => other,
        };
        let (left, right) = match w {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => (left.as_ref(), right.as_ref()),
            _ => return Ok(None),
        };
        // Try both orderings: (NOT BETWEEN, NOT LIKE) and (NOT LIKE, NOT BETWEEN).
        let left = match left {
            SqlExpr::Paren(inner) => inner.as_ref(),
            other => other,
        };
        let right = match right {
            SqlExpr::Paren(inner) => inner.as_ref(),
            other => other,
        };
        let (num_col, lo, hi, str_col, pattern) = match (
            Self::extract_not_between(left),
            Self::extract_not_like(right),
        ) {
            (Some((nc, l, h)), Some((sc, p))) => (nc, l, h, sc, p),
            _ => match (
                Self::extract_not_between(right),
                Self::extract_not_like(left),
            ) {
                (Some((nc, l, h)), Some((sc, p))) => (nc, l, h, sc, p),
                _ => return Ok(None),
            },
        };

        let count = match backend.scan_not_filter_count_mmap(&num_col, lo, hi, &str_col, &pattern)? {
            Some(c) => c,
            None => return Ok(None),
        };

        // Mirror execute_aggregation's one-row COUNT output shape.
        let mut fields = Vec::with_capacity(stmt.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());
        for col in &stmt.columns {
            if let SelectColumn::Aggregate { alias, column, .. } = col {
                let output = alias.clone().unwrap_or_else(|| {
                    format!("COUNT({})", column.as_deref().unwrap_or("*"))
                });
                fields.push(Field::new(output, ArrowDataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(vec![count])) as ArrayRef);
            }
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| err_data(e.to_string()))?;
        Ok(Some(ApexResult::Data(batch)))
    }

    /// Extract IN list of string values: col IN ('a', 'b', 'c')
    /// Returns (column_name, vec_of_string_values) if all values are strings.
    fn extract_in_string_filter(expr: &SqlExpr) -> Option<(String, Vec<String>)> {
        match expr {
            SqlExpr::In {
                column,
                values,
                negated,
            } => {
                if *negated {
                    return None;
                }
                let col = column.trim_matches('"').to_string();
                let mut strs = Vec::with_capacity(values.len());
                for v in values {
                    match v {
                        Value::String(s) => strs.push(s.clone()),
                        _ => return None,
                    }
                }
                if strs.is_empty() || strs.iter().any(String::is_empty) {
                    return None;
                }
                Some((col, strs))
            }
            _ => None,
        }
    }

    /// Extract IN list of numeric (integer) values: col IN (1, 2, 3)
    /// Returns (column_name, vec_of_i64_values) if all values are integers.
    fn extract_in_numeric_filter(expr: &SqlExpr) -> Option<(String, Vec<i64>)> {
        match expr {
            SqlExpr::In {
                column,
                values,
                negated,
            } => {
                if *negated {
                    return None;
                }
                let col = column.trim_matches('"').to_string();
                let mut nums = Vec::with_capacity(values.len());
                for v in values {
                    match v {
                        Value::Int64(n) => nums.push(*n),
                        Value::Int32(n) => nums.push(*n as i64),
                        _ => return None,
                    }
                }
                if nums.is_empty() {
                    return None;
                }
                Some((col, nums))
            }
            _ => None,
        }
    }

    /// Extract OR chain of same-column numeric equalities: col = 1 OR col = 2 OR ...
    /// Returns (column_name, vec_of_i64_values) — equivalent to numeric IN.
    fn extract_or_numeric_equalities(expr: &SqlExpr) -> Option<(String, Vec<i64>)> {
        let mut values = Vec::new();
        let mut col_name: Option<String> = None;
        Self::collect_or_numeric_equalities(expr, &mut col_name, &mut values)?;
        let col = col_name?;
        if values.len() < 2 {
            return None;
        }
        Some((col, values))
    }

    /// Recursively collect col = N leaves from an OR tree.
    fn collect_or_numeric_equalities(
        expr: &SqlExpr,
        col_name: &mut Option<String>,
        values: &mut Vec<i64>,
    ) -> Option<()> {
        use crate::query::sql_parser::BinaryOperator;
        match expr {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Or,
                right,
            } => {
                Self::collect_or_numeric_equalities(left, col_name, values)?;
                Self::collect_or_numeric_equalities(right, col_name, values)?;
                Some(())
            }
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let (c, v) = match (left.as_ref(), right.as_ref()) {
                    (SqlExpr::Column(c), lit) | (lit, SqlExpr::Column(c)) => {
                        let val = match lit {
                            SqlExpr::Literal(Value::Int64(n)) => *n,
                            SqlExpr::Literal(Value::Int32(n)) => *n as i64,
                            _ => return None,
                        };
                        (c.trim_matches('"').to_string(), val)
                    }
                    _ => return None,
                };
                match col_name {
                    Some(ref existing) => {
                        if *existing != c {
                            return None;
                        }
                    }
                    None => {
                        *col_name = Some(c);
                    }
                }
                values.push(v);
                Some(())
            }
            _ => None,
        }
    }

    /// Decompose an OR tree into leaf predicates that can each be scanned via mmap.
    /// Returns None if any leaf is not a simple scannable predicate.
    fn extract_or_leaf_predicates(expr: &SqlExpr) -> Option<Vec<OrLeafPredicate>> {
        use crate::query::sql_parser::BinaryOperator;
        match expr {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Or,
                right,
            } => {
                let mut left_leaves = Self::extract_or_leaf_predicates(left)?;
                let right_leaves = Self::extract_or_leaf_predicates(right)?;
                left_leaves.extend(right_leaves);
                Some(left_leaves)
            }
            _ => {
                // Try to classify this as a single scannable predicate
                if let Some((col, val)) = Self::extract_string_equality(expr) {
                    Some(vec![OrLeafPredicate::StringEq(col, val)])
                } else if let Some((col, low, high)) =
                    Self::extract_single_comparison_as_range(expr)
                {
                    Some(vec![OrLeafPredicate::NumericRange(col, low, high)])
                } else if let Some((col, low, high)) = Self::extract_between_range(expr) {
                    Some(vec![OrLeafPredicate::NumericRange(col, low, high)])
                } else if let Some((col, nums)) = Self::extract_in_numeric_filter(expr) {
                    Some(vec![OrLeafPredicate::NumericIn(col, nums)])
                } else if let Some((col, strs)) = Self::extract_in_string_filter(expr) {
                    Some(vec![OrLeafPredicate::StringIn(col, strs)])
                } else {
                    None
                }
            }
        }
    }

    /// Execute each OR leaf via the appropriate mmap scan, union all index sets.
    /// For 2+ leaves, uses parallel scanning (single mmap lock, rayon dispatch).
    fn scan_or_leaves_mmap(
        backend: &TableStorageBackend,
        leaves: &[OrLeafPredicate],
        limit: Option<usize>,
    ) -> io::Result<Option<Vec<usize>>> {
        use crate::storage::on_demand::MmapScanPred;

        // Try parallel path: convert leaves to MmapScanPred (skip StringIn — rare, needs multi-call)
        let has_string_in = leaves
            .iter()
            .any(|l| matches!(l, OrLeafPredicate::StringIn(..)));
        if leaves.len() >= 2 && !has_string_in {
            let preds: Vec<MmapScanPred> = leaves
                .iter()
                .map(|leaf| match leaf {
                    OrLeafPredicate::StringEq(col, val) => {
                        MmapScanPred::StringEq { col, value: val }
                    }
                    OrLeafPredicate::NumericRange(col, low, high) => MmapScanPred::NumericRange {
                        col,
                        low: *low,
                        high: *high,
                    },
                    OrLeafPredicate::NumericIn(col, nums) => {
                        MmapScanPred::NumericIn { col, values: nums }
                    }
                    OrLeafPredicate::StringIn(..) => unreachable!(),
                })
                .collect();
            if let Some(mut indices) = backend.scan_multi_predicates_parallel(&preds)? {
                if let Some(lim) = limit {
                    indices.truncate(lim);
                }
                return Ok(Some(indices));
            }
            // Parallel path returned None (e.g. compressed data) — fall through to sequential
        }

        // Sequential fallback
        let mut all_indices: Vec<usize> = Vec::new();
        for leaf in leaves {
            let indices = match leaf {
                OrLeafPredicate::StringEq(col, val) => {
                    backend.scan_string_filter_mmap(col, val, None)?
                }
                OrLeafPredicate::NumericRange(col, low, high) => {
                    backend.scan_numeric_range_mmap(col, *low, *high, None)?
                }
                OrLeafPredicate::NumericIn(col, nums) => {
                    backend.scan_numeric_in_mmap(col, nums, None)?
                }
                OrLeafPredicate::StringIn(col, strs) => {
                    backend.scan_string_in_mmap(col, strs, None)?
                }
            };
            if let Some(mut idxs) = indices {
                all_indices.append(&mut idxs);
            }
        }
        if all_indices.is_empty() {
            return Ok(Some(Vec::new()));
        }
        all_indices.sort_unstable();
        all_indices.dedup();
        if let Some(lim) = limit {
            all_indices.truncate(lim);
        }
        Ok(Some(all_indices))
    }

    #[inline]
    fn should_use_scatter_read(total_rows: usize, matched_rows: usize) -> bool {
        matched_rows < 200_000 && matched_rows.saturating_mul(4) < total_rows
    }

    fn take_rows_from_full_batch(
        full_batch: &RecordBatch,
        row_indices: &[usize],
    ) -> io::Result<RecordBatch> {
        use arrow::array::{ArrayRef, UInt32Array};

        let indices_arr =
            UInt32Array::from(row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
        let taken_columns: Vec<ArrayRef> = full_batch
            .columns()
            .iter()
            .map(|col| {
                arrow::compute::take(col.as_ref(), &indices_arr, None)
                    .map_err(|e| err_data(e.to_string()))
            })
            .collect::<io::Result<Vec<_>>>()?;
        RecordBatch::try_new(full_batch.schema(), taken_columns)
            .map_err(|e| err_data(e.to_string()))
    }

    fn aggregate_input_columns(stmt: &SelectStatement) -> Vec<String> {
        let mut cols = Vec::new();
        for column in &stmt.columns {
            if let SelectColumn::Aggregate {
                column: Some(name),
                ..
            } = column
            {
                let is_count_star_or_const = name == "*"
                    || name
                        .chars()
                        .next()
                        .map(|c| c.is_ascii_digit())
                        .unwrap_or(false);
                let input = if is_count_star_or_const { "_id" } else { name };
                if !cols.iter().any(|existing: &String| existing == input) {
                    cols.push(input.to_string());
                }
            }
        }
        if cols.is_empty() {
            cols.push("_id".to_string());
        }
        cols
    }

    fn read_matching_rows_adaptive(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
        row_indices: &[usize],
    ) -> io::Result<RecordBatch> {
        // Every caller reaches this with the WHERE already applied by a
        // storage-level scan, so only the post-filter columns (SELECT /
        // ORDER BY / GROUP BY) are needed — reading the filter columns again
        // wastes a full-column scatter.
        let col_refs = Self::post_filter_col_refs(stmt);
        let col_refs_vec: Option<Vec<&str>> = col_refs
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());

        if row_indices.is_empty() {
            return backend.read_columns_to_arrow(col_refs_vec.as_deref(), 0, Some(0));
        }

        let total_rows = backend.row_count() as usize;
        if backend.is_mmap_only() && !Self::should_use_scatter_read(total_rows, row_indices.len()) {
            let full_batch = backend.read_columns_to_arrow(col_refs_vec.as_deref(), 0, None)?;
            return Self::take_rows_from_full_batch(&full_batch, row_indices);
        }

        backend.read_columns_by_indices_to_arrow(row_indices, col_refs_vec.as_deref())
    }

    fn read_matching_rows_by_indices(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
        row_indices: &[usize],
    ) -> io::Result<RecordBatch> {
        let col_refs = Self::post_filter_col_refs(stmt);
        let col_refs_vec: Option<Vec<&str>> = col_refs
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());

        if row_indices.is_empty() {
            return backend.read_columns_to_arrow(col_refs_vec.as_deref(), 0, Some(0));
        }

        backend.read_columns_by_indices_to_arrow(row_indices, col_refs_vec.as_deref())
    }

    /// Columns needed after the storage-level scan has applied the WHERE:
    /// required columns minus WHERE-only references (WHERE columns that are
    /// also SELECT/ORDER BY/GROUP BY outputs must still be read).
    fn post_filter_col_refs(stmt: &SelectStatement) -> Option<Vec<String>> {
        let required = stmt.required_columns()?;
        let where_cols = stmt.where_columns();
        let mut output: Vec<String> = Vec::new();
        let mut push = |name: &str| {
            let plain = name
                .trim_matches('"')
                .rsplit('.')
                .next()
                .unwrap_or(name.trim_matches('"'))
                .to_string();
            if !output.contains(&plain) {
                output.push(plain);
            }
        };
        for col in &stmt.columns {
            match col {
                SelectColumn::Column(name) => push(name),
                SelectColumn::ColumnAlias { column, .. } => push(column),
                SelectColumn::Aggregate {
                    column: Some(c), ..
                } => {
                    if c != "*"
                        && !c
                            .chars()
                            .next()
                            .map(|ch| ch.is_ascii_digit())
                            .unwrap_or(false)
                    {
                        push(c);
                    }
                }
                SelectColumn::Expression { expr, .. } => {
                    for col in Self::expr_referenced_columns(expr) {
                        push(&col);
                    }
                }
                _ => {}
            }
        }
        for ob in &stmt.order_by {
            if let Some(ref expr) = ob.expr {
                for col in Self::expr_referenced_columns(expr) {
                    push(&col);
                }
            } else {
                push(&ob.column);
            }
        }
        for group in &stmt.group_by {
            push(group);
        }
        let post: Vec<String> = required
            .into_iter()
            .filter(|c| {
                !where_cols.iter().any(|w| w == c) || output.iter().any(|o| o == c)
            })
            .collect();
        if post.is_empty() {
            None
        } else {
            Some(post)
        }
    }

    /// MMAP fast path for IN filter on string column.
    /// Strategy: scan each IN value independently, merge-union sorted indices, scatter read.
    fn try_fast_mmap_in_filter(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
        storage_path: &Path,
    ) -> io::Result<Option<ApexResult>> {
        if !backend.is_mmap_only() || backend.has_pending_deltas() || backend.has_delta() {
            return Ok(None);
        }

        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        let (col, values) = match Self::extract_in_string_filter(where_clause) {
            Some(v) => v,
            None => return Ok(None),
        };

        let mut all_indices = match backend.scan_string_in_mmap(&col, &values, None)? {
            Some(v) => v,
            None => return Ok(None),
        };

        // Apply offset + limit
        let offset = stmt.offset.unwrap_or(0);
        if offset > 0 {
            if offset >= all_indices.len() {
                return Ok(None);
            }
            all_indices = all_indices[offset..].to_vec();
        }
        if let Some(lim) = stmt.limit {
            all_indices.truncate(lim);
        }

        if all_indices.is_empty() {
            return Ok(None);
        }

        let batch = Self::read_matching_rows_by_indices(backend, stmt, &all_indices)?;
        if !stmt.is_pure_star() {
            let projected =
                Self::apply_projection_with_storage(&batch, &stmt.columns, Some(storage_path))?;
            return Ok(Some(ApexResult::Data(projected)));
        }
        Ok(Some(ApexResult::Data(batch)))
    }

    /// Helper to extract numeric comparison: col > N, col >= N, col < N, col <= N
    fn extract_numeric_comparison(expr: &SqlExpr) -> Option<(String, String, f64)> {
        use crate::query::sql_parser::BinaryOperator;
        match expr {
            SqlExpr::BinaryOp { left, op, right } => {
                let op_str = match op {
                    BinaryOperator::Gt => ">",
                    BinaryOperator::Ge => ">=",
                    BinaryOperator::Lt => "<",
                    BinaryOperator::Le => "<=",
                    BinaryOperator::Eq => "=",
                    _ => return None,
                };

                match (left.as_ref(), right.as_ref()) {
                    (SqlExpr::Column(col), lit) => {
                        if let Ok(val) = Self::extract_numeric_value(lit) {
                            Some((col.trim_matches('"').to_string(), op_str.to_string(), val))
                        } else {
                            None
                        }
                    }
                    (lit, SqlExpr::Column(col)) => {
                        if let Ok(val) = Self::extract_numeric_value(lit) {
                            // Flip operator for reversed order
                            let flipped = match op_str {
                                ">" => "<",
                                ">=" => "<=",
                                "<" => ">",
                                "<=" => ">=",
                                _ => op_str,
                            };
                            Some((col.trim_matches('"').to_string(), flipped.to_string(), val))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Systematic predicate pushdown: extract simple numeric comparison from WHERE
    /// and use storage-level filtered read instead of full table scan.
    /// Handles: col > N, col >= N, col < N, col <= N, col = N, col != N
    /// Returns Some(batch) if pushdown succeeded, None to fall through.
    fn try_numeric_predicate_pushdown(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<RecordBatch>> {
        if backend.has_pending_deltas() || backend.has_delta() {
            return Ok(None);
        }
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        // --- mmap_only path: scan → indices → scatter read (LIMIT only) ---
        if backend.is_mmap_only() {
            if stmt.limit.is_none() {
                return Ok(None);
            }
            let (col, lo, hi) = match Self::extract_any_numeric_range(where_clause) {
                Some(v) => v,
                None => return Ok(None),
            };
            // With ORDER BY the scan must see the full matching set: applying
            // LIMIT during the row-group scan truncates to the first matching
            // row group, so the later sort cannot find the global top-k rows.
            let limit_with_off = if stmt.order_by.is_empty() {
                stmt.limit.map(|l| l + stmt.offset.unwrap_or(0))
            } else {
                None
            };
            let indices = match backend.scan_numeric_range_mmap(&col, lo, hi, limit_with_off)? {
                Some(v) => v,
                None => return Ok(None),
            };
            if indices.is_empty() {
                let schema = backend.read_columns_to_arrow(None, 0, Some(0))?;
                return Ok(Some(schema));
            }
            let batch = Self::read_matching_rows_adaptive(backend, stmt, &indices)?;
            return Ok(Some(batch));
        }

        // --- in-memory path: storage-level filtered read ---
        let (col_name, op_str, value) = match Self::extract_numeric_comparison(where_clause) {
            Some(v) => v,
            None => return Ok(None),
        };
        // Column projection pushdown
        let col_refs = Self::get_col_refs(stmt);
        let col_refs_vec: Option<Vec<&str>> = col_refs
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let batch = backend.read_columns_filtered_to_arrow(
            col_refs_vec.as_deref(),
            &col_name,
            &op_str,
            value,
        )?;
        Ok(Some(batch))
    }

}
