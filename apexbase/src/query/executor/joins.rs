// JOIN execution: hash join, full outer join, cross join

impl ApexExecutor {
    /// Execute SELECT statement with JOINs
    fn execute_select_with_joins(
        stmt: SelectStatement,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<ApexResult> {
        // CBO: choose a dependency-safe order for small INNER JOIN graphs.
        let joins = Self::maybe_reorder_joins(
            &stmt.from,
            &stmt.joins,
            stmt.where_clause.as_ref(),
            base_dir,
            default_table_path,
        );
        let required_columns = stmt.required_columns();
        let required_refs: Option<Vec<&str>> = required_columns
            .as_ref()
            .map(|columns| columns.iter().map(String::as_str).collect());

        // Get the left (base) table - supports both Table and Subquery (VIEW)
        let mut result_batch = match &stmt.from {
            Some(FromItem::Table { table, .. }) => {
                let left_path = Self::resolve_table_path(table, base_dir, default_table_path);
                if let Some(batch) = get_cached_cte_batch(&left_path) {
                    batch
                } else {
                    let left_backend = get_cached_backend(&left_path)?;
                    left_backend.read_columns_to_arrow(required_refs.as_deref(), 0, None)?
                }
            }
            Some(FromItem::Subquery { stmt: sub_stmt, .. }) => match sub_stmt.as_ref() {
                crate::query::SqlStatement::Select(sel) => {
                    let sub_path = Self::resolve_from_table_path(sel, base_dir, default_table_path);
                    (if sel.joins.is_empty() {
                        Self::execute_select_with_base_dir(
                            sel.clone(),
                            &sub_path,
                            base_dir,
                            default_table_path,
                        )
                    } else {
                        Self::execute_select_with_joins(sel.clone(), base_dir, default_table_path)
                    })?
                    .to_record_batch()?
                }
                crate::query::SqlStatement::Union(u) => {
                    Self::execute_union(u.clone(), base_dir, default_table_path)?
                        .to_record_batch()?
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Subquery must be SELECT or set operation",
                    ))
                }
            },
            Some(FromItem::TableFunction {
                func,
                file,
                options,
                ..
            }) => Self::read_table_function(func, file, options)?,
            Some(FromItem::TopkDistance {
                col,
                query,
                k,
                metric,
                ..
            }) => Self::execute_topk_distance(default_table_path, col, query, *k, metric)?,
            Some(FromItem::DirectFile { file, .. }) => Self::read_direct_file(file)?,
            Some(
                FromItem::LateralExplode { .. }
                | FromItem::LateralPosExplode { .. }
                | FromItem::LateralStack { .. },
            ) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "LATERAL VIEW requires a base FROM source",
                ));
            }
            None => {
                let left_backend = get_cached_backend(default_table_path)?;
                left_backend.read_columns_to_arrow(None, 0, None)?
            }
        };

        // Push predicates that reference only the base relation before the
        // first join.  The original WHERE remains applied after joining for
        // correctness; this early pass only reduces the join input.
        if let (Some(where_clause), Some(source_names)) = (
            stmt.where_clause.as_ref(),
            Self::source_names(&stmt.from),
        ) {
            if Self::predicate_is_local(&result_batch, where_clause, &source_names) {
                result_batch = Self::apply_filter_with_storage(
                    &result_batch,
                    where_clause,
                    default_table_path,
                )?;
            }
        }

        // Process each JOIN clause - supports both Table and Subquery (VIEW)
        for join_clause in &joins {
            match &join_clause.right {
                FromItem::LateralExplode {
                    expr,
                    column_alias,
                    outer,
                    ..
                } => {
                    result_batch =
                        Self::apply_lateral_explode(&result_batch, expr, column_alias, *outer)?;
                    continue;
                }
                FromItem::LateralPosExplode {
                    expr,
                    position_alias,
                    column_alias,
                    outer,
                    ..
                } => {
                    result_batch = Self::apply_lateral_posexplode(
                        &result_batch,
                        expr,
                        position_alias,
                        column_alias,
                        *outer,
                    )?;
                    continue;
                }
                FromItem::LateralStack {
                    rows,
                    values,
                    column_aliases,
                    ..
                } => {
                    result_batch =
                        Self::apply_lateral_stack(&result_batch, *rows, values, column_aliases)?;
                    continue;
                }
                _ => {}
            }
            let mut right_batch = match &join_clause.right {
                FromItem::Table { table, .. } => {
                    let right_path = Self::resolve_table_path(table, base_dir, default_table_path);
                    if let Some(batch) = get_cached_cte_batch(&right_path) {
                        batch
                    } else {
                        let right_backend = get_cached_backend(&right_path)?;
                        right_backend.read_columns_to_arrow(required_refs.as_deref(), 0, None)?
                    }
                }
                FromItem::Subquery { stmt: sub_stmt, .. } => match sub_stmt.as_ref() {
                    crate::query::SqlStatement::Select(sel) => {
                        let sub_path =
                            Self::resolve_from_table_path(sel, base_dir, default_table_path);
                        (if sel.joins.is_empty() {
                            Self::execute_select_with_base_dir(
                                sel.clone(),
                                &sub_path,
                                base_dir,
                                default_table_path,
                            )
                        } else {
                            Self::execute_select_with_joins(
                                sel.clone(),
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
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Subquery must be SELECT or set operation",
                        ))
                    }
                },
                FromItem::TableFunction {
                    func,
                    file,
                    options,
                    ..
                } => Self::read_table_function(func, file, options)?,
                FromItem::TopkDistance {
                    col,
                    query,
                    k,
                    metric,
                    ..
                } => Self::execute_topk_distance(default_table_path, col, query, *k, metric)?,
                FromItem::DirectFile { file, .. } => Self::read_direct_file(file)?,
                FromItem::LateralExplode { .. }
                | FromItem::LateralPosExplode { .. }
                | FromItem::LateralStack { .. } => unreachable!(),
            };

            if let (Some(where_clause), Some(source_names)) = (
                stmt.where_clause.as_ref(),
                Self::source_names(&Some(join_clause.right.clone())),
            ) {
                if Self::predicate_is_local(&right_batch, where_clause, &source_names) {
                    right_batch = Self::apply_filter_with_storage(
                        &right_batch,
                        where_clause,
                        default_table_path,
                    )?;
                }
            }

            if matches!(join_clause.join_type, JoinType::Semi | JoinType::Anti) {
                let (left_key, right_key, _, _, extra_filter) =
                    Self::extract_join_keys_with_filter(&join_clause.on)?;
                let filtered_right = if let Some(filter) = extra_filter {
                    Self::apply_filter_with_storage(&right_batch, &filter, default_table_path)?
                } else {
                    right_batch
                };
                result_batch = Self::hash_semi_anti_join(
                    &result_batch,
                    &filtered_right,
                    &left_key,
                    &right_key,
                    join_clause.join_type == JoinType::Anti,
                )?;
                continue;
            }

            // CROSS JOIN has no ON clause — use cartesian product directly
            if join_clause.join_type == JoinType::Cross {
                let right_qualifier = match &join_clause.right {
                    FromItem::Table { table, alias, .. } => {
                        alias.as_deref().unwrap_or(table.as_str())
                    }
                    FromItem::DirectFile { file, alias, .. } => {
                        alias.as_deref().unwrap_or(file.as_str())
                    }
                    FromItem::TableFunction { func, alias, .. } => {
                        alias.as_deref().unwrap_or(func.as_str())
                    }
                    FromItem::Subquery { alias, .. } => alias.as_str(),
                    _ => "",
                };
                let prepared_right = Self::prepare_right_join_columns(
                    &right_batch,
                    &result_batch,
                    "",
                    (!right_qualifier.is_empty()).then_some(right_qualifier),
                    None,
                )?;
                result_batch = Self::cross_join(&result_batch, &prepared_right)?;
            } else {
                let right_alias = match &join_clause.right {
                    FromItem::Table { table, alias, .. } => {
                        Some(alias.clone().unwrap_or_else(|| table.clone()))
                    }
                    FromItem::DirectFile { file, alias, .. } => {
                        Some(alias.clone().unwrap_or_else(|| file.clone()))
                    }
                    FromItem::TableFunction { func, alias, .. } => {
                        Some(alias.clone().unwrap_or_else(|| func.clone()))
                    }
                    FromItem::Subquery { alias, .. } => Some(alias.clone()),
                    _ => None,
                };
                // Extract join keys from ON condition (supports AND with extra filter)
                let Ok((
                    left_key,
                    right_key,
                    left_key_qualifier,
                    right_key_qualifier,
                    extra_filter,
                )) = Self::extract_join_keys_with_filter(&join_clause.on)
                else {
                    if let Some(joined) = Self::try_hash_join_on_expression(
                        &result_batch,
                        &right_batch,
                        &join_clause.on,
                        &join_clause.join_type,
                        right_alias.as_deref(),
                    )? {
                        result_batch = joined;
                        continue;
                    }
                    result_batch = Self::nested_loop_join_on(
                        &result_batch,
                        &right_batch,
                        &join_clause.on,
                        &join_clause.join_type,
                        right_alias.as_deref(),
                        default_table_path,
                    )?;
                    continue;
                };

                // Perform the join (passing right alias for self-join column naming)
                result_batch = Self::hash_join_aliased(
                    &result_batch,
                    &right_batch,
                    &left_key,
                    &right_key,
                    &join_clause.join_type,
                    right_alias.as_deref(),
                    left_key_qualifier.as_deref(),
                    right_key_qualifier.as_deref(),
                )?;

                // Apply extra ON predicates (non-equality conditions from AND)
                if let Some(filter) = extra_filter {
                    result_batch = Self::apply_filter_with_storage(
                        &result_batch,
                        &filter,
                        default_table_path,
                    )?;
                }
            }
        }

        // Determine this before the empty checks: COUNT over an empty join must
        // still produce a row, while ordinary projections must retain aliases.
        let has_aggregation = stmt.columns.iter().any(|col| match col {
            SelectColumn::Aggregate { .. } => true,
            SelectColumn::Expression { expr, .. } => Self::expr_contains_aggregate(expr),
            _ => false,
        });

        if result_batch.num_rows() == 0 {
            if has_aggregation && stmt.group_by.is_empty() {
                return Self::execute_aggregation(&result_batch, &stmt);
            }
            let projected = Self::apply_projection_with_storage(
                &result_batch,
                &stmt.columns,
                Some(default_table_path),
            )?;
            return Ok(ApexResult::Empty(projected.schema()));
        }

        // Apply WHERE filter (with storage path for subquery support)
        let filtered = if let Some(ref where_clause) = stmt.where_clause {
            Self::apply_filter_with_storage(&result_batch, where_clause, default_table_path)?
        } else {
            result_batch
        };

        if filtered.num_rows() == 0 {
            if has_aggregation && stmt.group_by.is_empty() {
                return Self::execute_aggregation(&filtered, &stmt);
            }
            let projected = Self::apply_projection_with_storage(
                &filtered,
                &stmt.columns,
                Some(default_table_path),
            )?;
            return Ok(ApexResult::Empty(projected.schema()));
        }

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
            let mut columns = vec![SelectColumn::All];
            for col in &stmt.columns {
                if let SelectColumn::WindowFunction {
                    name,
                    args,
                    partition_by,
                    order_by,
                    alias,
                } = col
                {
                    let mut resolved_order = order_by.clone();
                    for clause in &mut resolved_order {
                        if grouped
                            .column_by_name(clause.column.trim_matches('"'))
                            .is_some()
                        {
                            continue;
                        }
                        let aggregate_name =
                            clause.column.split('(').next().unwrap_or(&clause.column);
                        if let Some(output) = stmt.columns.iter().find_map(|candidate| {
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
                            clause.column = output;
                            clause.expr = None;
                        }
                    }
                    columns.push(SelectColumn::WindowFunction {
                        name: name.clone(),
                        args: args.clone(),
                        partition_by: partition_by.clone(),
                        order_by: resolved_order,
                        alias: alias.clone(),
                    });
                }
            }
            window_stmt.columns = columns;
            return Self::execute_window_function(&grouped, &window_stmt);
        }
        if has_window {
            return Self::execute_window_function(&filtered, &stmt);
        }

        if has_aggregation && stmt.group_by.is_empty() {
            return Self::execute_aggregation(&filtered, &stmt);
        }

        // Handle GROUP BY with aggregation
        if has_aggregation && !stmt.group_by.is_empty() {
            return Self::execute_group_by(&filtered, &stmt);
        }

        // Apply ORDER BY
        let sorted = if !stmt.order_by.is_empty() {
            Self::apply_order_by(&filtered, &stmt.order_by)?
        } else {
            filtered
        };

        // Apply LIMIT/OFFSET
        let limited = Self::apply_limit_offset(&sorted, stmt.limit, stmt.offset)?;

        // Apply projection - pass default_table_path for scalar subqueries
        let projected =
            Self::apply_projection_with_storage(&limited, &stmt.columns, Some(default_table_path))?;

        // Apply DISTINCT if specified
        let result = if stmt.distinct {
            Self::deduplicate_batch(&projected)?
        } else {
            projected
        };

        Ok(ApexResult::Data(result))
    }

    fn source_names(from: &Option<FromItem>) -> Option<Vec<String>> {
        match from.as_ref()? {
            FromItem::Table { table, alias } => {
                let mut names = vec![table.clone()];
                if let Some(alias) = alias {
                    names.push(alias.clone());
                }
                Some(names)
            }
            _ => None,
        }
    }

    /// Whether an expression can be evaluated against one input relation.
    /// Qualified references must name that relation and bare references must
    /// exist in the input batch.  AND/OR are deliberately treated as a whole
    /// expression, so an OR crossing relations is never pushed down.
    fn predicate_is_local(
        batch: &RecordBatch,
        expr: &SqlExpr,
        source_names: &[String],
    ) -> bool {
        fn column_is_local(batch: &RecordBatch, name: &str, source_names: &[String]) -> bool {
            let (qualifier, bare) = match name.rsplit_once('.') {
                Some((qualifier, bare)) => (Some(qualifier), bare),
                None => (None, name),
            };
            qualifier.map_or(true, |q| source_names.iter().any(|name| name == q))
                && batch.column_by_name(bare).is_some()
        }

        match expr {
            SqlExpr::Column(name) => column_is_local(batch, name, source_names),
            SqlExpr::Literal(_) | SqlExpr::Variable(_) | SqlExpr::FtsMatch { .. } => true,
            SqlExpr::FtsResolved { .. }
            | SqlExpr::FtsScore { .. }
            | SqlExpr::FtsScoreResolved { .. } => column_is_local(batch, "_id", source_names),
            SqlExpr::BinaryOp { left, right, .. } => {
                Self::predicate_is_local(batch, left, source_names)
                    && Self::predicate_is_local(batch, right, source_names)
            }
            SqlExpr::UnaryOp { expr, .. } | SqlExpr::Paren(expr) | SqlExpr::Cast { expr, .. } => {
                Self::predicate_is_local(batch, expr, source_names)
            }
            SqlExpr::Like { column, .. }
            | SqlExpr::Regexp { column, .. }
            | SqlExpr::In { column, .. }
            | SqlExpr::Between { column, .. }
            | SqlExpr::IsNull { column, .. } => column_is_local(batch, column, source_names),
            SqlExpr::Function { args, .. } => args
                .iter()
                .all(|arg| Self::predicate_is_local(batch, arg, source_names)),
            SqlExpr::Case {
                when_then,
                else_expr,
            } => {
                when_then.iter().all(|(when_expr, then_expr)| {
                    Self::predicate_is_local(batch, when_expr, source_names)
                        && Self::predicate_is_local(batch, then_expr, source_names)
                }) && else_expr.as_ref().map_or(true, |expr| {
                    Self::predicate_is_local(batch, expr, source_names)
                })
            }
            SqlExpr::ArrayIndex { array, index } => {
                Self::predicate_is_local(batch, array, source_names)
                    && Self::predicate_is_local(batch, index, source_names)
            }
            SqlExpr::ArrayLiteral(_) => true,
            SqlExpr::TopkDistance { .. } | SqlExpr::ExplodeRename { .. } => false,
            SqlExpr::InSubquery { .. }
            | SqlExpr::ExistsSubquery { .. }
            | SqlExpr::ScalarSubquery { .. } => false,
        }
    }

    fn apply_lateral_explode(
        batch: &RecordBatch,
        expr: &SqlExpr,
        column_alias: &str,
        outer: bool,
    ) -> io::Result<RecordBatch> {
        let array = Self::evaluate_expr_to_array(batch, expr)?;
        let strings = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| err_data("EXPLODE currently requires a string or SPLIT result"))?;
        let estimated_rows = batch.num_rows().saturating_mul(2);
        let mut indices = Vec::with_capacity(estimated_rows);
        let mut values = arrow::array::StringBuilder::with_capacity(
            estimated_rows,
            batch.num_rows().saturating_mul(8),
        );
        for row in 0..batch.num_rows() {
            if strings.is_null(row) {
                if outer {
                    indices.push(row as u32);
                    values.append_null();
                }
                continue;
            }
            let encoded = strings.value(row);
            for item in encoded.split('\0') {
                indices.push(row as u32);
                values.append_value(item);
            }
        }
        let take = arrow::array::UInt32Array::from(indices);
        let mut fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        use rayon::prelude::*;
        let mut columns: Vec<ArrayRef> = batch
            .columns()
            .par_iter()
            .map(|col| {
                compute::take(col.as_ref(), &take, None).map_err(|e| err_data(e.to_string()))
            })
            .collect::<io::Result<_>>()?;
        fields.push(Field::new(column_alias, ArrowDataType::Utf8, true));
        columns.push(Arc::new(values.finish()) as ArrayRef);
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|e| err_data(e.to_string()))
    }

    fn apply_lateral_posexplode(
        batch: &RecordBatch,
        expr: &SqlExpr,
        position_alias: &str,
        column_alias: &str,
        outer: bool,
    ) -> io::Result<RecordBatch> {
        let array = Self::evaluate_expr_to_array(batch, expr)?;
        let strings = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| err_data("POSEXPLODE currently requires a string or SPLIT result"))?;
        let estimated_rows = batch.num_rows().saturating_mul(2);
        let mut indices = Vec::with_capacity(estimated_rows);
        let mut positions = Vec::with_capacity(estimated_rows);
        let mut values = arrow::array::StringBuilder::with_capacity(
            estimated_rows,
            batch.num_rows().saturating_mul(8),
        );
        for row in 0..batch.num_rows() {
            if strings.is_null(row) {
                if outer {
                    indices.push(row as u32);
                    positions.push(None);
                    values.append_null();
                }
                continue;
            }
            for (position, item) in strings.value(row).split('\0').enumerate() {
                indices.push(row as u32);
                positions.push(Some(position as i64));
                values.append_value(item);
            }
        }
        let take = arrow::array::UInt32Array::from(indices);
        let mut fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        use rayon::prelude::*;
        let mut columns: Vec<ArrayRef> = batch
            .columns()
            .par_iter()
            .map(|col| {
                compute::take(col.as_ref(), &take, None).map_err(|e| err_data(e.to_string()))
            })
            .collect::<io::Result<_>>()?;
        fields.push(Field::new(position_alias, ArrowDataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(positions)) as ArrayRef);
        fields.push(Field::new(column_alias, ArrowDataType::Utf8, true));
        columns.push(Arc::new(values.finish()) as ArrayRef);
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|e| err_data(e.to_string()))
    }

    fn apply_lateral_stack(
        batch: &RecordBatch,
        rows: usize,
        values: &[SqlExpr],
        column_aliases: &[String],
    ) -> io::Result<RecordBatch> {
        if rows == 0 || values.len() != rows * column_aliases.len() {
            return Err(err_input("Invalid STACK shape"));
        }
        let evaluated: Vec<ArrayRef> = values
            .iter()
            .map(|expr| Self::evaluate_expr_to_array(batch, expr))
            .collect::<io::Result<_>>()?;
        let mut indices = Vec::with_capacity(batch.num_rows() * rows);
        for _ in 0..rows {
            indices.extend((0..batch.num_rows()).map(|row| row as u32));
        }
        let take = arrow::array::UInt32Array::from(indices);
        let mut fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let mut columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| {
                compute::take(col.as_ref(), &take, None).map_err(|e| err_data(e.to_string()))
            })
            .collect::<io::Result<_>>()?;

        for (col_idx, alias) in column_aliases.iter().enumerate() {
            let sources: Vec<&ArrayRef> = (0..rows)
                .map(|row| &evaluated[row * column_aliases.len() + col_idx])
                .collect();
            let has_string = sources.iter().any(|array| {
                matches!(
                    array.data_type(),
                    ArrowDataType::Utf8 | ArrowDataType::LargeUtf8
                )
            });
            let has_float = sources.iter().any(|array| {
                matches!(
                    array.data_type(),
                    ArrowDataType::Float32 | ArrowDataType::Float64
                )
            });
            if has_string {
                let mut output = Vec::with_capacity(batch.num_rows() * rows);
                for source in sources {
                    for row in 0..batch.num_rows() {
                        let value = Self::arrow_value_at_col(source, row);
                        output.push(value.as_str().map(str::to_string));
                    }
                }
                fields.push(Field::new(alias, ArrowDataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(output)) as ArrayRef);
            } else if has_float {
                let mut output = Vec::with_capacity(batch.num_rows() * rows);
                for source in sources {
                    for row in 0..batch.num_rows() {
                        output.push(Self::arrow_value_at_col(source, row).as_f64());
                    }
                }
                fields.push(Field::new(alias, ArrowDataType::Float64, true));
                columns.push(Arc::new(Float64Array::from(output)) as ArrayRef);
            } else {
                let mut output = Vec::with_capacity(batch.num_rows() * rows);
                for source in sources {
                    for row in 0..batch.num_rows() {
                        output.push(Self::arrow_value_at_col(source, row).as_i64());
                    }
                }
                fields.push(Field::new(alias, ArrowDataType::Int64, true));
                columns.push(Arc::new(Int64Array::from(output)) as ArrayRef);
            }
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|e| err_data(e.to_string()))
    }

    /// Resolve table path from FROM clause.
    /// Delegates to resolve_table_path so that db.table qualified names are handled uniformly.
    fn resolve_from_table_path(
        stmt: &SelectStatement,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> std::path::PathBuf {
        if let Some(FromItem::Table { table, .. }) = &stmt.from {
            let table_name = table.trim_matches('"');
            // Check if table matches the default table's file stem (unqualified fast path)
            if !table_name.contains('.') {
                if let Some(stem) = default_table_path.file_stem() {
                    if stem.to_string_lossy() == table_name {
                        return default_table_path.to_path_buf();
                    }
                }
            }
            return Self::resolve_table_path(table_name, base_dir, default_table_path);
        }
        // No FROM clause - use default_table_path
        default_table_path.to_path_buf()
    }

    /// Resolve table path from table name.
    /// Supports qualified `database.table` syntax for cross-database queries.
    /// The root directory (parent of all databases) is retrieved from the
    /// thread-local QUERY_ROOT_DIR set by Python bindings before execution.
    fn resolve_table_path(
        table_name: &str,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> std::path::PathBuf {
        let clean_name = table_name.trim_matches('"').trim_matches('`');

        // Check temp dir first: temp tables shadow persistent tables
        let safe_check: String = clean_name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let truncated_check = if safe_check.len() > 200 {
            &safe_check[..200]
        } else {
            &safe_check
        };
        if let Some(temp_dir) = crate::query::executor::get_temp_dir() {
            let temp_path = temp_dir.join(format!("{}.apex", truncated_check));
            if temp_path.exists() {
                return temp_path;
            }
        }

        // Handle qualified db.table syntax
        if let Some(dot_pos) = clean_name.find('.') {
            let db_name = clean_name[..dot_pos].trim();
            let tbl_name = clean_name[dot_pos + 1..].trim();
            let safe_tbl: String = tbl_name
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c == '_' || c == '-' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            let safe_tbl = if safe_tbl.len() > 200 {
                &safe_tbl[..200]
            } else {
                &safe_tbl
            };

            // Determine root_dir: from thread-local if available, else base_dir.parent()
            let root_dir = crate::query::executor::get_query_root_dir()
                .unwrap_or_else(|| base_dir.parent().unwrap_or(base_dir).to_path_buf());

            let db_dir = if db_name.is_empty() || db_name.eq_ignore_ascii_case("default") {
                root_dir
            } else {
                root_dir.join(db_name)
            };
            return db_dir.join(format!("{}.apex", safe_tbl));
        }

        if clean_name == "default" {
            if default_table_path.is_dir() {
                base_dir.join("default.apex")
            } else {
                default_table_path.to_path_buf()
            }
        } else {
            let safe_name: String = clean_name
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c == '_' || c == '-' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            let truncated_name = if safe_name.len() > 200 {
                &safe_name[..200]
            } else {
                &safe_name
            };
            base_dir.join(format!("{}.apex", truncated_name))
        }
    }

    /// Resolve the table path for a point-lookup query by extracting the FROM clause table name.
    /// Used by the QuerySignature::PointLookup pre-parse fast path.
    fn resolve_point_lookup_table_path(
        sql: &str,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> std::path::PathBuf {
        let su = sql.trim().to_ascii_uppercase();
        if let Some(fp) = su.find(" FROM ") {
            let after_from = su[fp + 6..].trim_start();
            let tn_end = after_from
                .find(|c: char| c == ' ' || c == '\t' || c == '\n' || c == ';')
                .unwrap_or(after_from.len());
            let tname = after_from[..tn_end].trim_matches('"').to_lowercase();
            if !tname.is_empty() {
                let default_stem = default_table_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_lowercase();
                if tname == default_stem {
                    return default_table_path.to_path_buf();
                }
                return base_dir.join(format!("{}.apex", tname));
            }
        }
        default_table_path.to_path_buf()
    }

    /// CBO: Reorder INNER JOIN clauses by ascending right-table row count.
    /// For up to eight relations this uses a bounded subset-DP memo.  Larger
    /// graphs intentionally keep their original order to avoid making planning
    /// itself more expensive than execution.
    fn maybe_reorder_joins(
        from: &Option<FromItem>,
        joins: &[JoinClause],
        where_clause: Option<&SqlExpr>,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> Vec<JoinClause> {
        // Two joins have only two possible orders; keep the original light
        // path and reserve memo planning for graphs where it can pay back its
        // own planning cost.
        if joins.len() < 3 || where_clause.is_none() {
            return joins.to_vec();
        }
        // Only reorder if ALL joins are INNER (LEFT/RIGHT/FULL/CROSS are order-dependent)
        if !joins.iter().all(|j| j.join_type == JoinType::Inner) {
            return joins.to_vec();
        }
        // Only reorder if all ON conditions are simple equalities (safe to reorder)
        for j in joins {
            if Self::extract_join_keys(&j.on).is_err() {
                return joins.to_vec();
            }
        }
        let Some(base_names) = Self::source_names(from) else {
            return joins.to_vec();
        };
        if joins
            .iter()
            .any(|join| !matches!(&join.right, FromItem::Table { .. }))
        {
            return joins.to_vec();
        }
        const MAX_JOIN_RELATIONS: usize = 8;
        const MAX_MEMO_STATES: usize = 512;
        const PLANNING_BUDGET: std::time::Duration = std::time::Duration::from_millis(2);
        if joins.len() > MAX_JOIN_RELATIONS {
            return joins.to_vec();
        }

        #[derive(Clone)]
        struct JoinEstimate {
            names: Vec<String>,
            rows: f64,
            selectivity: f64,
        }

        let estimates: Vec<JoinEstimate> = joins
            .iter()
            .map(|join| {
                let FromItem::Table { table, alias } = &join.right else {
                    unreachable!()
                };
                let path = Self::resolve_table_path(table, base_dir, default_table_path);
                let base_rows = get_cached_backend(&path)
                    .map(|backend| backend.active_row_count() as f64)
                    .unwrap_or(f64::MAX / 4.0);
                let mut names = vec![table.clone()];
                if let Some(alias) = alias {
                    names.push(alias.clone());
                }
                let rows = where_clause
                    .and_then(|expr| get_table_stats(&path.to_string_lossy()).map(|stats| {
                        base_rows * Self::estimate_local_selectivity(expr, &names, &stats)
                    }))
                    .unwrap_or(base_rows)
                    .clamp(1.0, f64::MAX / 4.0);
                let selectivity = Self::estimate_join_selectivity(join, &names, &path);
                JoinEstimate {
                    names,
                    rows,
                    selectivity,
                }
            })
            .collect();

        #[derive(Clone)]
        struct JoinState {
            cost: f64,
            output_rows: f64,
            order: Vec<usize>,
            available: Vec<String>,
        }

        let state_count = 1usize << joins.len();
        if state_count > MAX_MEMO_STATES {
            return joins.to_vec();
        }
        let planning_started = std::time::Instant::now();
        let full_mask = state_count - 1;
        let base_rows = match from {
            Some(FromItem::Table { table, .. }) => {
                let path = Self::resolve_table_path(table, base_dir, default_table_path);
                get_cached_backend(&path)
                    .map(|backend| backend.active_row_count() as f64)
                    .unwrap_or(1.0)
            }
            _ => 1.0,
        };
        let mut memo: Vec<Option<JoinState>> = vec![None; state_count];
        memo[0] = Some(JoinState {
            cost: 0.0,
            output_rows: base_rows.max(1.0),
            order: Vec::new(),
            available: base_names,
        });

        for mask in 0..state_count {
            if planning_started.elapsed() > PLANNING_BUDGET {
                return joins.to_vec();
            }
            let Some(state) = memo[mask].clone() else {
                continue;
            };
            for index in 0..joins.len() {
                let bit = 1usize << index;
                if mask & bit != 0
                    || !Self::join_can_follow(
                        &joins[index].on,
                        &state.available,
                        &estimates[index].names,
                    )
                {
                    continue;
                }
                let estimate = &estimates[index];
                let output_rows = (state.output_rows * estimate.rows * estimate.selectivity)
                    .clamp(1.0, f64::MAX / 4.0);
                let next = JoinState {
                    // Hash join work is dominated by the current probe side
                    // and the new build side.  The output estimate makes the
                    // memo prefer selective joins early.
                    cost: state.cost + state.output_rows + estimate.rows + output_rows * 0.01,
                    output_rows,
                    order: {
                        let mut order = state.order.clone();
                        order.push(index);
                        order
                    },
                    available: {
                        let mut available = state.available.clone();
                        available.extend(estimate.names.iter().cloned());
                        available
                    },
                };
                let replace = memo[mask | bit]
                    .as_ref()
                    .map(|current| next.cost < current.cost)
                    .unwrap_or(true);
                if replace {
                    memo[mask | bit] = Some(next);
                }
            }
        }

        let Some(best) = memo[full_mask].as_ref() else {
            return joins.to_vec();
        };
        best.order.iter().map(|&index| joins[index].clone()).collect()
    }

    fn join_can_follow(on: &SqlExpr, available: &[String], right: &[String]) -> bool {
        let Ok((left_key, right_key)) = Self::extract_join_keys_qualified(on) else {
            return false;
        };
        let mut has_right = false;
        let mut has_available = false;
        for key in [left_key, right_key] {
            let Some((qualifier, _)) = key.rsplit_once('.') else {
                has_right = true;
                has_available = true;
                continue;
            };
            if right.iter().any(|name| name == qualifier) {
                has_right = true;
            } else if available.iter().any(|name| name == qualifier) {
                has_available = true;
            } else {
                return false;
            }
        }
        has_right && has_available
    }

    fn estimate_join_selectivity(join: &JoinClause, right: &[String], path: &Path) -> f64 {
        let Ok((left_key, right_key)) = Self::extract_join_keys_qualified(&join.on) else {
            return 0.1;
        };
        let fallback_key = right_key.clone();
        let right_column = [left_key, right_key]
            .into_iter()
            .find(|key| {
                key.rsplit_once('.')
                    .map(|(qualifier, _)| right.iter().any(|name| name == qualifier))
                    .unwrap_or(false)
            })
            .unwrap_or(fallback_key);
        let column = right_column.rsplit('.').next().unwrap_or(&right_column);
        get_table_stats(&path.to_string_lossy())
            .and_then(|stats| stats.columns.get(column).map(|column| column.ndv))
            .map(|ndv| {
                if ndv > 0 {
                    (1.0 / ndv as f64).clamp(0.000_001, 1.0)
                } else {
                    0.1
                }
            })
            .unwrap_or(0.1)
    }

    fn estimate_local_selectivity(
        expr: &SqlExpr,
        source_names: &[String],
        stats: &crate::query::planner::TableStats,
    ) -> f64 {
        fn column_is_local(
            column: &str,
            source_names: &[String],
            stats: &crate::query::planner::TableStats,
        ) -> bool {
            let (qualifier, bare) = match column.rsplit_once('.') {
                Some((qualifier, bare)) => (Some(qualifier), bare),
                None => (None, column),
            };
            qualifier.map_or(true, |q| source_names.iter().any(|name| name == q))
                && stats.columns.contains_key(bare)
        }

        match expr {
            SqlExpr::BinaryOp {
                left,
                op: crate::query::sql_parser::BinaryOperator::And,
                right,
            } => {
                Self::estimate_local_selectivity(left, source_names, stats)
                    * Self::estimate_local_selectivity(right, source_names, stats)
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                let columns = [left.as_ref(), right.as_ref()];
                let is_local = columns.iter().all(|child| match child {
                    SqlExpr::Column(column) => column_is_local(column, source_names, stats),
                    SqlExpr::Literal(_) => true,
                    _ => false,
                });
                if is_local {
                    crate::query::planner::QueryPlanner::estimate_selectivity(expr, stats)
                } else {
                    1.0
                }
            }
            SqlExpr::Between { column, .. }
            | SqlExpr::In { column, .. }
            | SqlExpr::Like { column, .. }
            | SqlExpr::Regexp { column, .. }
            | SqlExpr::IsNull { column, .. } => {
                if column_is_local(column, source_names, stats) {
                    crate::query::planner::QueryPlanner::estimate_selectivity(expr, stats)
                } else {
                    1.0
                }
            }
            _ => 1.0,
        }
        .clamp(0.0, 1.0)
    }

    /// Extract join keys preserving table qualifiers (e.g., "a.project_id" not just "project_id")
    fn extract_join_keys_qualified(on_expr: &SqlExpr) -> io::Result<(String, String)> {
        use crate::query::sql_parser::BinaryOperator;
        match on_expr {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let left_col = match left.as_ref() {
                    SqlExpr::Column(name) => name.clone(),
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "JOIN key must be a column reference",
                        ))
                    }
                };
                let right_col = match right.as_ref() {
                    SqlExpr::Column(name) => name.clone(),
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "JOIN key must be a column reference",
                        ))
                    }
                };
                Ok((left_col, right_col))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "JOIN ON clause must be a simple equality",
            )),
        }
    }

    /// Extract join keys from ON condition (expects simple equality: left.col = right.col)
    fn extract_join_keys(on_expr: &SqlExpr) -> io::Result<(String, String)> {
        let (left_col, right_col, _, _) = Self::extract_join_key_refs(on_expr)?;
        Ok((left_col, right_col))
    }

    /// Extract join key references from ON condition, preserving optional table qualifiers.
    fn extract_join_key_refs(
        on_expr: &SqlExpr,
    ) -> io::Result<(String, String, Option<String>, Option<String>)> {
        use crate::query::sql_parser::BinaryOperator;
        match on_expr {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let (left_col, left_qualifier) = Self::extract_column_reference(left)?;
                let (right_col, right_qualifier) = Self::extract_column_reference(right)?;
                Ok((left_col, right_col, left_qualifier, right_qualifier))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "JOIN ON clause must be a simple equality",
            )),
        }
    }

    /// Extract join keys from ON condition, returning an optional extra filter for non-equality predicates.
    /// Supports compound AND: ON a.id=b.id AND a.x<b.x → key=(id,id), filter=a.x<b.x
    fn extract_join_keys_with_filter(
        on_expr: &SqlExpr,
    ) -> io::Result<(
        String,
        String,
        Option<String>,
        Option<String>,
        Option<SqlExpr>,
    )> {
        use crate::query::sql_parser::BinaryOperator;
        fn flatten_and(expr: &SqlExpr, out: &mut Vec<SqlExpr>) {
            if let SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } = expr
            {
                flatten_and(left, out);
                flatten_and(right, out);
            } else {
                out.push(expr.clone());
            }
        }
        let mut predicates = Vec::new();
        flatten_and(on_expr, &mut predicates);
        let key_index = predicates
            .iter()
            .position(|predicate| Self::extract_join_key_refs(predicate).is_ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "No equijoin condition found in JOIN ON clause",
                )
            })?;
        let (left_col, right_col, left_qualifier, right_qualifier) =
            Self::extract_join_key_refs(&predicates[key_index])?;
        predicates.remove(key_index);
        let extra_filter = predicates
            .into_iter()
            .reduce(|left, right| SqlExpr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            });
        Ok((
            left_col,
            right_col,
            left_qualifier,
            right_qualifier,
            extra_filter,
        ))
    }

    /// Wrapper around hash_join that supports an optional right-table alias for column naming.
    /// When alias is provided, duplicate columns are named '{alias}.{col}' instead of '{col}_right'.
    fn hash_join_aliased(
        left: &RecordBatch,
        right: &RecordBatch,
        left_key: &str,
        right_key: &str,
        join_type: &JoinType,
        right_alias: Option<&str>,
        left_key_qualifier: Option<&str>,
        right_key_qualifier: Option<&str>,
    ) -> io::Result<RecordBatch> {
        let preserve_qualified_left_key =
            *join_type == JoinType::Right && left_key_qualifier.is_some();
        let preserve_qualified_right_key =
            matches!(join_type, JoinType::Left | JoinType::Right | JoinType::Full)
                && right_key_qualifier.is_some();
        if right_alias.is_none() && !preserve_qualified_left_key && !preserve_qualified_right_key {
            return Self::hash_join(left, right, left_key, right_key, join_type);
        }
        let prepared_left = if preserve_qualified_left_key {
            Some(Self::append_qualified_join_key(
                left,
                left_key,
                left_key_qualifier,
            )?)
        } else {
            None
        };
        // Prepare right-side columns before joining:
        // - preserve alias-qualified conflicting columns for self-joins
        // - for outer joins, keep a qualified copy of the right join key so
        //   predicates/projections like `r.id IS NULL` preserve unmatched-row
        //   semantics instead of falling back to the coalesced left key
        let prepared_right = Self::prepare_right_join_columns(
            right,
            left,
            right_key,
            right_alias,
            if preserve_qualified_right_key {
                right_key_qualifier
            } else {
                None
            },
        )?;
        let left_batch = prepared_left.as_ref().unwrap_or(left);
        Self::hash_join(left_batch, &prepared_right, left_key, right_key, join_type)
    }

    /// Prepare right-side columns for join output.
    ///
    /// - Conflicting non-key columns can be exposed as `{alias}.{col}` for self-joins.
    /// - Outer joins can retain a qualified copy of the right join key (for
    ///   predicates/projections like `table.id IS NULL` on unmatched rows).
    fn prepare_right_join_columns(
        right: &RecordBatch,
        left: &RecordBatch,
        join_key: &str,
        conflict_alias: Option<&str>,
        qualified_join_key: Option<&str>,
    ) -> io::Result<RecordBatch> {
        let left_name_vec: Vec<String> = left
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let left_names: std::collections::HashSet<&str> =
            left_name_vec.iter().map(|s| s.as_str()).collect();
        let mut fields: Vec<arrow::datatypes::Field> = Vec::new();
        let mut arrays: Vec<arrow::array::ArrayRef> = Vec::new();
        for (i, field) in right.schema().fields().iter().enumerate() {
            let name = field.name();
            let new_name = if name != join_key && left_names.contains(name.as_str()) {
                if let Some(alias) = conflict_alias {
                    format!("{}.{}", alias, name)
                } else {
                    format!("{}_right", name)
                }
            } else {
                name.clone()
            };
            fields.push(arrow::datatypes::Field::new(
                &new_name,
                field.data_type().clone(),
                field.is_nullable(),
            ));
            arrays.push(right.column(i).clone());

            if name == join_key {
                if let Some(qualifier) = qualified_join_key {
                    let qualified_name = format!("{}.{}", qualifier, name);
                    if qualified_name != *name
                        && !fields.iter().any(|f| f.name() == &qualified_name)
                    {
                        fields.push(arrow::datatypes::Field::new(
                            &qualified_name,
                            field.data_type().clone(),
                            field.is_nullable(),
                        ));
                        arrays.push(right.column(i).clone());
                    }
                }
            }
        }
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    /// Execute a non-equality INNER/LEFT join. Hive commonly uses this for one-row
    /// parameter CTEs and range maps (for example `x BETWEEN low AND high`).
    /// The implementation remains vectorized: form the small-side Cartesian batch,
    /// evaluate ON once with Arrow kernels, and restore unmatched LEFT rows.
    fn nested_loop_join_on(
        left: &RecordBatch,
        right: &RecordBatch,
        on: &SqlExpr,
        join_type: &JoinType,
        right_alias: Option<&str>,
        storage_path: &Path,
    ) -> io::Result<RecordBatch> {
        if !matches!(join_type, JoinType::Inner | JoinType::Left) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Non-equality joins currently support INNER JOIN and LEFT JOIN",
            ));
        }

        const ROW_ID: &str = "__apex_left_row_id";
        let mut left_fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let mut left_columns = left.columns().to_vec();
        left_fields.push(Field::new(ROW_ID, ArrowDataType::UInt64, false));
        left_columns
            .push(Arc::new(UInt64Array::from_iter_values(0..left.num_rows() as u64)) as ArrayRef);
        let tagged_left = RecordBatch::try_new(Arc::new(Schema::new(left_fields)), left_columns)
            .map_err(|e| err_data(e.to_string()))?;

        // Rename conflicting right columns to alias-qualified names so predicates
        // such as `p.dt = ...` remain unambiguous after the Cartesian expansion.
        let prepared_right =
            Self::prepare_right_join_columns(right, &tagged_left, "", right_alias, None)?;
        let crossed = Self::cross_join(&tagged_left, &prepared_right)?;
        let matched = Self::apply_filter_with_storage(&crossed, on, storage_path)?;

        let joined = if *join_type == JoinType::Inner {
            matched
        } else {
            let ids = matched
                .column_by_name(ROW_ID)
                .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| err_data("LEFT range join lost its row identifier"))?;
            let mut seen = ahash::AHashSet::with_capacity(ids.len());
            for i in 0..ids.len() {
                if !ids.is_null(i) {
                    seen.insert(ids.value(i));
                }
            }
            let unmatched: Vec<u32> = (0..left.num_rows())
                .filter(|i| !seen.contains(&(*i as u64)))
                .map(|i| i as u32)
                .collect();
            if unmatched.is_empty() {
                matched
            } else {
                let indices = arrow::array::UInt32Array::from(unmatched);
                let mut extra_columns = Vec::with_capacity(crossed.num_columns());
                for col in tagged_left.columns() {
                    extra_columns.push(
                        compute::take(col.as_ref(), &indices, None)
                            .map_err(|e| err_data(e.to_string()))?,
                    );
                }
                let extra_rows = indices.len();
                for field in prepared_right.schema().fields() {
                    extra_columns.push(arrow::array::new_null_array(field.data_type(), extra_rows));
                }
                let nullable_schema = Arc::new(Schema::new(
                    crossed
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| Field::new(f.name(), f.data_type().clone(), true))
                        .collect::<Vec<_>>(),
                ));
                let matched =
                    RecordBatch::try_new(nullable_schema.clone(), matched.columns().to_vec())
                        .map_err(|e| err_data(e.to_string()))?;
                let extra = RecordBatch::try_new(nullable_schema.clone(), extra_columns)
                    .map_err(|e| err_data(e.to_string()))?;
                arrow::compute::concat_batches(&nullable_schema, &[matched, extra])
                    .map_err(|e| err_data(e.to_string()))?
            }
        };

        let row_id_index = joined
            .schema()
            .index_of(ROW_ID)
            .map_err(|e| err_data(e.to_string()))?;
        let fields = joined
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != row_id_index)
            .map(|(_, f)| f.as_ref().clone())
            .collect::<Vec<_>>();
        let columns = joined
            .columns()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != row_id_index)
            .map(|(_, c)| c.clone())
            .collect::<Vec<_>>();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|e| err_data(e.to_string()))
    }

    /// Hash an equality JOIN whose keys are expressions rather than bare columns.
    ///
    /// This avoids materializing a Cartesian product for common normalization
    /// joins such as `CAST(a.id AS TEXT) = REPLACE(b.file, '.jpg', '')`.
    fn try_hash_join_on_expression(
        left: &RecordBatch,
        right: &RecordBatch,
        on: &SqlExpr,
        join_type: &JoinType,
        right_alias: Option<&str>,
    ) -> io::Result<Option<RecordBatch>> {
        if !matches!(join_type, JoinType::Inner) {
            return Ok(None);
        }
        let SqlExpr::BinaryOp {
            left: left_expr,
            op: BinaryOperator::Eq,
            right: right_expr,
        } = on
        else {
            return Ok(None);
        };

        let evaluated = Self::evaluate_expr_to_array(left, left_expr)
            .and_then(|left_key| {
                Self::evaluate_expr_to_array(right, right_expr)
                    .map(|right_key| (left_key, right_key))
            })
            .or_else(|_| {
                Self::evaluate_expr_to_array(left, right_expr).and_then(|left_key| {
                    Self::evaluate_expr_to_array(right, left_expr)
                        .map(|right_key| (left_key, right_key))
                })
            });
        let Ok((left_key, right_key)) = evaluated else {
            return Ok(None);
        };
        if left_key.len() != left.num_rows()
            || right_key.len() != right.num_rows()
            || left_key.data_type() != right_key.data_type()
        {
            return Ok(None);
        }

        const LEFT_KEY: &str = "__apex_join_expr_left";
        const RIGHT_KEY: &str = "__apex_join_expr_right";
        let append_key =
            |batch: &RecordBatch, key: ArrayRef, name: &str| -> io::Result<RecordBatch> {
                let mut fields: Vec<Field> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect();
                fields.push(Field::new(name, key.data_type().clone(), true));
                let mut columns = batch.columns().to_vec();
                columns.push(key);
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                    .map_err(|error| err_data(error.to_string()))
            };

        let keyed_left = append_key(left, left_key, LEFT_KEY)?;
        let keyed_right = append_key(right, right_key, RIGHT_KEY)?;
        let joined = Self::hash_join_aliased(
            &keyed_left,
            &keyed_right,
            LEFT_KEY,
            RIGHT_KEY,
            join_type,
            right_alias,
            None,
            None,
        )?;

        let fields = joined
            .schema()
            .fields()
            .iter()
            .filter(|field| !matches!(field.name().as_str(), LEFT_KEY | RIGHT_KEY))
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        let columns = joined
            .schema()
            .fields()
            .iter()
            .zip(joined.columns())
            .filter(|(field, _)| !matches!(field.name().as_str(), LEFT_KEY | RIGHT_KEY))
            .map(|(_, column)| Arc::clone(column))
            .collect::<Vec<_>>();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map(Some)
            .map_err(|error| err_data(error.to_string()))
    }

    /// Append a qualified copy of the join key without renaming any existing columns.
    ///
    /// RIGHT JOIN needs this for the original left key because the swap-to-LEFT path
    /// would otherwise drop it from the result schema.
    fn append_qualified_join_key(
        batch: &RecordBatch,
        join_key: &str,
        qualified_join_key: Option<&str>,
    ) -> io::Result<RecordBatch> {
        let Some(qualifier) = qualified_join_key else {
            return Ok(batch.clone());
        };
        let qualified_name = format!("{}.{}", qualifier, join_key);
        if qualified_name == join_key || batch.column_by_name(&qualified_name).is_some() {
            return Ok(batch.clone());
        }

        let join_key_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == join_key)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Join key '{}' not found", join_key),
                )
            })?;

        let mut fields: Vec<arrow::datatypes::Field> = Vec::with_capacity(batch.num_columns() + 1);
        let mut arrays: Vec<arrow::array::ArrayRef> = Vec::with_capacity(batch.num_columns() + 1);
        for (i, field) in batch.schema().fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            arrays.push(batch.column(i).clone());
            if i == join_key_idx {
                fields.push(arrow::datatypes::Field::new(
                    &qualified_name,
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
                arrays.push(batch.column(i).clone());
            }
        }

        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    /// Extract column reference from expression (handles table.column and column).
    fn extract_column_reference(expr: &SqlExpr) -> io::Result<(String, Option<String>)> {
        match expr {
            SqlExpr::Column(name) => {
                let clean_name = name.trim_matches('"');
                if let Some(dot_pos) = clean_name.rfind('.') {
                    Ok((
                        clean_name[dot_pos + 1..].to_string(),
                        Some(clean_name[..dot_pos].to_string()),
                    ))
                } else {
                    Ok((clean_name.to_string(), None))
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "JOIN key must be a column reference",
            )),
        }
    }

    /// Convert expression to column name string (for display/field naming)
    fn expr_to_column_name(expr: &SqlExpr) -> String {
        match expr {
            SqlExpr::Column(name) => {
                // Handle table.column format - take the column part
                if let Some(dot_pos) = name.rfind('.') {
                    name[dot_pos + 1..].trim_matches('"').to_string()
                } else {
                    name.trim_matches('"').to_string()
                }
            }
            _ => "group".to_string(),
        }
    }

    /// Perform hash join between two RecordBatches
    fn hash_join(
        left: &RecordBatch,
        right: &RecordBatch,
        left_key: &str,
        right_key: &str,
        join_type: &JoinType,
    ) -> io::Result<RecordBatch> {
        if matches!(join_type, JoinType::Semi | JoinType::Anti) {
            return Self::hash_semi_anti_join(
                left,
                right,
                left_key,
                right_key,
                *join_type == JoinType::Anti,
            );
        }
        // RIGHT JOIN → LEFT JOIN with swapped tables, then reorder columns
        if *join_type == JoinType::Right {
            let swapped = Self::hash_join(right, left, right_key, left_key, &JoinType::Left)?;
            return Self::reorder_join_columns(&swapped, left, right, right_key);
        }

        // FULL OUTER JOIN → LEFT JOIN + append unmatched right rows
        if *join_type == JoinType::Full {
            return Self::full_outer_join(left, right, left_key, right_key);
        }

        // CROSS JOIN → cartesian product
        if *join_type == JoinType::Cross {
            return Self::cross_join(left, right);
        }

        // Get key columns
        let left_key_col = left.column_by_name(left_key).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Left join key '{}' not found", left_key),
            )
        })?;
        let right_key_col = right.column_by_name(right_key).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Right join key '{}' not found", right_key),
            )
        })?;

        if *join_type == JoinType::Left {
            if let Some(joined) = Self::try_unique_string_left_join(
                left,
                right,
                left_key,
                right_key,
                left_key_col,
                right_key_col,
            )? {
                return Ok(joined);
            }
        }

        // For INNER JOIN, swap tables if right is larger (build from smaller table)
        let should_swap =
            !matches!(join_type, JoinType::Left) && right.num_rows() > left.num_rows() * 2;

        let (build_batch, probe_batch, build_key_col, probe_key_col, build_key, probe_key, swapped) =
            if should_swap {
                (
                    left,
                    right,
                    left_key_col,
                    right_key_col,
                    left_key,
                    right_key,
                    true,
                )
            } else {
                (
                    right,
                    left,
                    right_key_col,
                    left_key_col,
                    right_key,
                    left_key,
                    false,
                )
            };

        // Build hash table from build side (smaller table for INNER JOIN)
        let build_rows = build_batch.num_rows();

        // For Int64 keys, use optimized direct hash building
        let hash_table: AHashMap<u64, Vec<usize>> =
            if let Some(build_int_arr) = build_key_col.as_any().downcast_ref::<Int64Array>() {
                let mut table: AHashMap<u64, Vec<usize>> = AHashMap::with_capacity(build_rows);
                for i in 0..build_rows {
                    if !build_int_arr.is_null(i) {
                        let val = build_int_arr.value(i);
                        let hash = {
                            let mut h = AHasher::default();
                            val.hash(&mut h);
                            h.finish()
                        };
                        table
                            .entry(hash)
                            .or_insert_with(|| Vec::with_capacity(2))
                            .push(i);
                    }
                }
                table
            } else {
                let mut table: AHashMap<u64, Vec<usize>> = AHashMap::with_capacity(build_rows);
                for i in 0..build_rows {
                    let hash = Self::hash_array_value_fast(build_key_col, i);
                    table
                        .entry(hash)
                        .or_insert_with(|| Vec::with_capacity(4))
                        .push(i);
                }
                table
            };

        let right_rows = right.num_rows();

        // Probe phase - use probe_batch (which may be swapped)
        let probe_rows = probe_batch.num_rows();
        let is_left_join = matches!(join_type, JoinType::Left);

        // Check if key columns are Int64 (most common case) - can skip equality check
        let is_int64_key = probe_key_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .is_some()
            && build_key_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .is_some();

        // For INNER JOIN with Int64 keys, use optimized path without Option overhead
        let is_inner_join_fast_path = is_int64_key && (!is_left_join || swapped);

        // Collect probe_idx -> build_idx mappings
        let (probe_indices, build_indices_u32, build_indices_opt): (
            Vec<u32>,
            Vec<u32>,
            Vec<Option<u32>>,
        ) = if is_inner_join_fast_path {
            // Ultra-fast INNER JOIN path with direct u32 vectors - no Option overhead
            let probe_int_arr = probe_key_col.as_any().downcast_ref::<Int64Array>().unwrap();

            let est_matches = probe_rows;
            let mut probe_idx_vec: Vec<u32> = Vec::with_capacity(est_matches);
            let mut build_idx_vec: Vec<u32> = Vec::with_capacity(est_matches);

            for probe_idx in 0..probe_rows {
                let probe_val = probe_int_arr.value(probe_idx);
                let probe_hash = {
                    let mut h = AHasher::default();
                    probe_val.hash(&mut h);
                    h.finish()
                };

                if let Some(build_matches) = hash_table.get(&probe_hash) {
                    for &build_idx in build_matches {
                        probe_idx_vec.push(probe_idx as u32);
                        build_idx_vec.push(build_idx as u32);
                    }
                }
            }
            (probe_idx_vec, build_idx_vec, Vec::new())
        } else if is_int64_key {
            // LEFT JOIN with Int64 keys - needs Option for NULL handling
            let probe_int_arr = probe_key_col.as_any().downcast_ref::<Int64Array>().unwrap();

            let est_matches = probe_rows;
            let mut probe_idx_vec: Vec<u32> = Vec::with_capacity(est_matches);
            let mut build_idx_vec: Vec<Option<u32>> = Vec::with_capacity(est_matches);

            for probe_idx in 0..probe_rows {
                let probe_val = probe_int_arr.value(probe_idx);
                let probe_hash = {
                    let mut h = AHasher::default();
                    probe_val.hash(&mut h);
                    h.finish()
                };

                if let Some(build_matches) = hash_table.get(&probe_hash) {
                    for &build_idx in build_matches {
                        probe_idx_vec.push(probe_idx as u32);
                        build_idx_vec.push(Some(build_idx as u32));
                    }
                } else {
                    probe_idx_vec.push(probe_idx as u32);
                    build_idx_vec.push(None);
                }
            }
            (probe_idx_vec, Vec::new(), build_idx_vec)
        } else {
            let mut probe_idx_vec: Vec<u32> = Vec::with_capacity(probe_rows);
            let mut build_idx_vec: Vec<Option<u32>> = Vec::with_capacity(probe_rows);

            if is_left_join && !swapped {
                for probe_idx in 0..probe_rows {
                    let probe_hash = Self::hash_array_value_fast(probe_key_col, probe_idx);
                    let mut found_match = false;

                    if let Some(build_matches) = hash_table.get(&probe_hash) {
                        for &build_idx in build_matches {
                            if Self::arrays_equal_at(
                                probe_key_col,
                                probe_idx,
                                build_key_col,
                                build_idx,
                            ) {
                                probe_idx_vec.push(probe_idx as u32);
                                build_idx_vec.push(Some(build_idx as u32));
                                found_match = true;
                            }
                        }
                    }

                    if !found_match {
                        probe_idx_vec.push(probe_idx as u32);
                        build_idx_vec.push(None);
                    }
                }
            } else {
                for probe_idx in 0..probe_rows {
                    let probe_hash = Self::hash_array_value_fast(probe_key_col, probe_idx);

                    if let Some(build_matches) = hash_table.get(&probe_hash) {
                        for &build_idx in build_matches {
                            if Self::arrays_equal_at(
                                probe_key_col,
                                probe_idx,
                                build_key_col,
                                build_idx,
                            ) {
                                probe_idx_vec.push(probe_idx as u32);
                                build_idx_vec.push(Some(build_idx as u32));
                            }
                        }
                    }
                }
            }
            (probe_idx_vec, Vec::new(), build_idx_vec)
        };

        // Convert back to left/right indices based on whether we swapped
        // For INNER JOIN fast path, use direct u32 indices
        let (left_indices, right_indices_u32, right_indices_opt): (
            Vec<u32>,
            Vec<u32>,
            Vec<Option<u32>>,
        ) = if is_inner_join_fast_path {
            if swapped {
                (build_indices_u32.clone(), probe_indices.clone(), Vec::new())
            } else {
                (probe_indices, build_indices_u32, Vec::new())
            }
        } else if swapped {
            (
                build_indices_opt.iter().map(|x| x.unwrap_or(0)).collect(),
                Vec::new(),
                probe_indices.iter().map(|x| Some(*x)).collect(),
            )
        } else {
            (probe_indices, Vec::new(), build_indices_opt)
        };

        let left_rows = left.num_rows();

        // Build result schema (combine left and right, avoiding duplicate key column)
        let mut fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for field in right.schema().fields() {
            if field.name() != right_key {
                let new_name = if fields.iter().any(|f| f.name() == field.name()) {
                    format!("{}_{}", field.name(), "right")
                } else {
                    field.name().clone()
                };
                fields.push(Field::new(&new_name, field.data_type().clone(), true));
            }
        }
        let result_schema = Arc::new(Schema::new(fields));

        // Build result columns - pre-allocate for all columns
        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(left.num_columns() + right.num_columns());

        // Star-schema LEFT JOINs normally preserve every left row exactly once
        // and in order.  Re-running Arrow `take` for all accumulated left-side
        // columns at every join makes very wide queries quadratic in column
        // count.  In that common identity-mapping case the immutable arrays can
        // be shared directly; only newly joined right columns need gathering.
        let left_is_identity = left_indices.len() == left_rows
            && left_indices
                .iter()
                .enumerate()
                .all(|(index, &row)| row as usize == index);
        if left_is_identity {
            columns.extend(left.columns().iter().cloned());
        } else {
            let left_indices_array = arrow::array::UInt32Array::from(left_indices);
            for col in left.columns() {
                let taken = compute::take(col.as_ref(), &left_indices_array, None)
                    .map_err(|e| err_data(e.to_string()))?;
                columns.push(taken);
            }
        }

        // Take from right (excluding join key to avoid duplication)
        // Use direct u32 indices for INNER JOIN fast path (no Option overhead)
        if is_inner_join_fast_path {
            let right_indices_array = arrow::array::UInt32Array::from(right_indices_u32);
            for (col_idx, field) in right.schema().fields().iter().enumerate() {
                if field.name() != right_key {
                    let right_col = right.column(col_idx);
                    let taken = compute::take(right_col.as_ref(), &right_indices_array, None)
                        .map_err(|e| err_data(e.to_string()))?;
                    columns.push(taken);
                }
            }
        } else {
            // LEFT JOIN path - handle nulls with Option indices
            for (col_idx, field) in right.schema().fields().iter().enumerate() {
                if field.name() != right_key {
                    let right_col = right.column(col_idx);
                    let taken = Self::take_with_nulls(right_col, &right_indices_opt)?;
                    columns.push(taken);
                }
            }
        }

        RecordBatch::try_new(result_schema, columns).map_err(|e| err_data(e.to_string()))
    }

    fn try_unique_string_left_join(
        left: &RecordBatch,
        right: &RecordBatch,
        _left_key: &str,
        right_key: &str,
        left_key_col: &ArrayRef,
        right_key_col: &ArrayRef,
    ) -> io::Result<Option<RecordBatch>> {
        let Some(left_keys) = left_key_col.as_any().downcast_ref::<StringArray>() else {
            return Ok(None);
        };
        let Some(right_keys) = right_key_col.as_any().downcast_ref::<StringArray>() else {
            return Ok(None);
        };
        if left_keys.null_count() > 0 || right_keys.null_count() > 0 {
            return Ok(None);
        }

        let mut right_index: AHashMap<&str, u32> = AHashMap::with_capacity(right.num_rows());
        for row in 0..right.num_rows() {
            if right_index
                .insert(right_keys.value(row), row as u32)
                .is_some()
            {
                return Ok(None);
            }
        }

        let mut right_indices = Vec::with_capacity(left.num_rows());
        for row in 0..left.num_rows() {
            right_indices.push(right_index.get(left_keys.value(row)).copied());
        }

        let mut fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        for field in right.schema().fields() {
            if field.name() != right_key {
                let new_name = if fields.iter().any(|left_field| left_field.name() == field.name())
                {
                    format!("{}_{}", field.name(), "right")
                } else {
                    field.name().clone()
                };
                fields.push(Field::new(&new_name, field.data_type().clone(), true));
            }
        }

        let mut columns = Vec::with_capacity(left.num_columns() + right.num_columns());
        columns.extend(left.columns().iter().cloned());
        for (col_idx, field) in right.schema().fields().iter().enumerate() {
            if field.name() != right_key {
                let taken = Self::take_with_nulls(right.column(col_idx), &right_indices)?;
                columns.push(taken);
            }
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map(Some)
            .map_err(|e| err_data(e.to_string()))
    }

    fn hash_semi_anti_join(
        left: &RecordBatch,
        right: &RecordBatch,
        left_key: &str,
        right_key: &str,
        anti: bool,
    ) -> io::Result<RecordBatch> {
        let left_col = left
            .column_by_name(left_key)
            .ok_or_else(|| err_data(format!("Left join key '{}' not found", left_key)))?;
        let right_col = right
            .column_by_name(right_key)
            .ok_or_else(|| err_data(format!("Right join key '{}' not found", right_key)))?;
        let mut right_hashes: AHashMap<u64, Vec<usize>> = AHashMap::with_capacity(right.num_rows());
        for row in 0..right.num_rows() {
            if !right_col.is_null(row) {
                right_hashes
                    .entry(Self::hash_array_value_fast(right_col, row))
                    .or_default()
                    .push(row);
            }
        }
        let mut indices = Vec::with_capacity(left.num_rows());
        for row in 0..left.num_rows() {
            let matched = if left_col.is_null(row) {
                false
            } else {
                right_hashes
                    .get(&Self::hash_array_value_fast(left_col, row))
                    .is_some_and(|candidates| {
                        candidates.iter().any(|candidate| {
                            Self::arrays_equal_at(left_col, row, right_col, *candidate)
                        })
                    })
            };
            if matched != anti {
                indices.push(row as u32);
            }
        }
        let take = arrow::array::UInt32Array::from(indices);
        let columns = left
            .columns()
            .iter()
            .map(|column| {
                compute::take(column.as_ref(), &take, None).map_err(|e| err_data(e.to_string()))
            })
            .collect::<io::Result<Vec<_>>>()?;
        RecordBatch::try_new(left.schema(), columns).map_err(|e| err_data(e.to_string()))
    }

    /// Reorder columns from a swapped LEFT JOIN back to original left→right order.
    /// Used by RIGHT JOIN: we did LEFT JOIN(right, left) so result has right cols first.
    /// We need to put left cols first, right cols second (excluding join key from right).
    fn reorder_join_columns(
        swapped_result: &RecordBatch,
        original_left: &RecordBatch,
        original_right: &RecordBatch,
        right_key: &str,
    ) -> io::Result<RecordBatch> {
        // swapped_result has: [right_columns..., left_columns_minus_join_key...]
        // We want:            [left_columns...(nullable), right_columns_minus_join_key...]
        let left_ncols = original_left.num_columns();
        let right_ncols = original_right.num_columns();
        // In swapped result: first right_ncols are from original_right, rest are from original_left (minus key)
        let right_cols_in_result = right_ncols;
        let left_cols_in_result = swapped_result.num_columns() - right_cols_in_result;

        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        // Left columns come from the tail of swapped_result (but they are nullable — RIGHT JOIN preserves all right rows)
        for i in 0..left_cols_in_result {
            let col_idx = right_cols_in_result + i;
            fields.push(swapped_result.schema().field(col_idx).as_ref().clone());
            arrays.push(swapped_result.column(col_idx).clone());
        }

        // Right columns (excluding join key) come from the head of swapped_result
        let swapped_schema = swapped_result.schema();
        for i in 0..right_cols_in_result {
            let field = swapped_schema.field(i);
            if field.name() == right_key {
                continue;
            }
            let new_name = if fields.iter().any(|f| f.name() == field.name()) {
                format!("{}_right", field.name())
            } else {
                field.name().clone()
            };
            fields.push(Field::new(&new_name, field.data_type().clone(), true));
            arrays.push(swapped_result.column(i).clone());
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))
    }

    /// FULL OUTER JOIN: LEFT JOIN + append unmatched right rows with NULL left columns
    fn full_outer_join(
        left: &RecordBatch,
        right: &RecordBatch,
        left_key: &str,
        right_key: &str,
    ) -> io::Result<RecordBatch> {
        // Step 1: Do LEFT JOIN to get all left rows + matched right rows
        let left_result = Self::hash_join(left, right, left_key, right_key, &JoinType::Left)?;

        // Step 2: Find unmatched right rows
        let left_key_col = left
            .column_by_name(left_key)
            .ok_or_else(|| err_data(format!("Left join key '{}' not found", left_key)))?;
        let right_key_col = right
            .column_by_name(right_key)
            .ok_or_else(|| err_data(format!("Right join key '{}' not found", right_key)))?;

        // Keep the rows for each hash bucket so collision verification only
        // examines candidates with the same hash. Scanning the entire left
        // side after every hash hit makes a fully matched join quadratic.
        let mut left_key_rows: AHashMap<u64, Vec<usize>> =
            AHashMap::with_capacity(left.num_rows());
        for i in 0..left.num_rows() {
            left_key_rows
                .entry(Self::hash_array_value_fast(left_key_col, i))
                .or_insert_with(|| Vec::with_capacity(1))
                .push(i);
        }

        // Find right rows whose keys don't exist in left
        let mut unmatched_right_indices: Vec<u32> = Vec::new();
        for i in 0..right.num_rows() {
            let hash = Self::hash_array_value_fast(right_key_col, i);
            let matched = left_key_rows.get(&hash).is_some_and(|candidates| {
                candidates
                    .iter()
                    .any(|&j| Self::arrays_equal_at(right_key_col, i, left_key_col, j))
            });
            if !matched {
                unmatched_right_indices.push(i as u32);
            }
        }

        if unmatched_right_indices.is_empty() {
            return Ok(left_result);
        }

        // Step 3: Build rows for unmatched right: NULL left cols + right values
        // Make all fields nullable for FULL OUTER JOIN (left side can be NULL for unmatched right rows)
        let nullable_fields: Vec<Field> = left_result
            .schema()
            .fields()
            .iter()
            .map(|f| Field::new(f.name(), f.data_type().clone(), true))
            .collect();
        let nullable_schema = Arc::new(Schema::new(nullable_fields));

        // Re-wrap left_result columns with nullable schema
        let left_result =
            RecordBatch::try_new(nullable_schema.clone(), left_result.columns().to_vec())
                .map_err(|e| err_data(e.to_string()))?;

        let left_ncols = left.num_columns();
        let n_unmatched = unmatched_right_indices.len();
        let right_take = arrow::array::UInt32Array::from(unmatched_right_indices);

        let mut extra_columns: Vec<ArrayRef> = Vec::with_capacity(nullable_schema.fields().len());

        // Left columns → all NULL
        for i in 0..left_ncols {
            let dt = nullable_schema.field(i).data_type();
            extra_columns.push(arrow::array::new_null_array(dt, n_unmatched));
        }

        // Right columns (excluding join key) → take from right batch
        for (col_idx, field) in right.schema().fields().iter().enumerate() {
            if field.name() == right_key {
                continue;
            }
            let taken = compute::take(right.column(col_idx).as_ref(), &right_take, None)
                .map_err(|e| err_data(e.to_string()))?;
            extra_columns.push(taken);
        }

        let extra_batch = RecordBatch::try_new(nullable_schema.clone(), extra_columns)
            .map_err(|e| err_data(e.to_string()))?;

        // Step 4: Concat left_result + extra_batch
        arrow::compute::concat_batches(&nullable_schema, &[left_result, extra_batch])
            .map_err(|e| err_data(e.to_string()))
    }

    /// CROSS JOIN: Cartesian product of two tables
    fn cross_join(left: &RecordBatch, right: &RecordBatch) -> io::Result<RecordBatch> {
        let left_rows = left.num_rows();
        let right_rows = right.num_rows();
        let total = left_rows.checked_mul(right_rows).ok_or_else(|| {
            err_data("JOIN result cardinality exceeds the platform addressable range")
        })?;

        if total == 0 {
            // Build empty schema
            let mut fields: Vec<Field> = left
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            for field in right.schema().fields() {
                let new_name = if fields.iter().any(|f| f.name() == field.name()) {
                    format!("{}_right", field.name())
                } else {
                    field.name().clone()
                };
                fields.push(Field::new(&new_name, field.data_type().clone(), true));
            }
            let schema = Arc::new(Schema::new(fields));
            let empty_cols: Vec<ArrayRef> = schema
                .fields()
                .iter()
                .map(|f| arrow::array::new_null_array(f.data_type(), 0) as ArrayRef)
                .collect();
            return RecordBatch::try_new(schema, empty_cols).map_err(|e| err_data(e.to_string()));
        }

        let utf8_limit = i32::MAX as usize;
        for column in left.columns() {
            let value_bytes = if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                array.value_data().len()
            } else if let Some(array) = column
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
            {
                array.value_data().len()
            } else {
                0
            };
            if value_bytes
                .checked_mul(right_rows)
                .map_or(true, |bytes| bytes > utf8_limit)
            {
                return Err(err_data(
                    "JOIN intermediate exceeds Arrow Utf8/Binary 32-bit offset capacity; \
                     use an equality join key or materialize normalized keys before joining",
                ));
            }
        }
        for column in right.columns() {
            let value_bytes = if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                array.value_data().len()
            } else if let Some(array) = column
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
            {
                array.value_data().len()
            } else {
                0
            };
            if value_bytes
                .checked_mul(left_rows)
                .map_or(true, |bytes| bytes > utf8_limit)
            {
                return Err(err_data(
                    "JOIN intermediate exceeds Arrow Utf8/Binary 32-bit offset capacity; \
                     use an equality join key or materialize normalized keys before joining",
                ));
            }
        }

        // Build index arrays: left[0,0,...,1,1,...] right[0,1,2,...,0,1,2,...]
        let mut left_idx: Vec<u32> = Vec::with_capacity(total);
        let mut right_idx: Vec<u32> = Vec::with_capacity(total);
        for l in 0..left_rows {
            for r in 0..right_rows {
                left_idx.push(l as u32);
                right_idx.push(r as u32);
            }
        }

        let left_take = arrow::array::UInt32Array::from(left_idx);
        let right_take = arrow::array::UInt32Array::from(right_idx);

        let mut fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(left.num_columns() + right.num_columns());

        for col in left.columns() {
            let taken = compute::take(col.as_ref(), &left_take, None)
                .map_err(|e| err_data(e.to_string()))?;
            columns.push(taken);
        }

        for field in right.schema().fields() {
            let new_name = if fields.iter().any(|f| f.name() == field.name()) {
                format!("{}_right", field.name())
            } else {
                field.name().clone()
            };
            fields.push(Field::new(&new_name, field.data_type().clone(), true));
        }
        for col in right.columns() {
            let taken = compute::take(col.as_ref(), &right_take, None)
                .map_err(|e| err_data(e.to_string()))?;
            columns.push(taken);
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).map_err(|e| err_data(e.to_string()))
    }

    /// Hash a value at given index in an array (legacy, uses DefaultHasher)
    fn hash_array_value(array: &ArrayRef, idx: usize) -> u64 {
        Self::hash_array_value_fast(array, idx)
    }

    /// Fast hash using ahash (2-3x faster than DefaultHasher)
    #[inline]
    fn hash_array_value_fast(array: &ArrayRef, idx: usize) -> u64 {
        let mut hasher = AHasher::default();

        if array.is_null(idx) {
            0u64.hash(&mut hasher);
        } else if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
            arr.value(idx).hash(&mut hasher);
        } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            arr.value(idx).hash(&mut hasher);
        } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
            arr.value(idx).to_bits().hash(&mut hasher);
        } else if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
            arr.value(idx).hash(&mut hasher);
        } else {
            idx.hash(&mut hasher);
        }

        hasher.finish()
    }

    /// Strip table alias prefix from column name (e.g., "o.user_id" -> "user_id")
    fn strip_table_prefix(col_name: &str) -> &str {
        if let Some(dot_pos) = col_name.find('.') {
            &col_name[dot_pos + 1..]
        } else {
            col_name
        }
    }

    /// Get column from batch, stripping table prefix if needed
    fn get_column_by_name<'a>(batch: &'a RecordBatch, col_name: &str) -> Option<&'a ArrayRef> {
        let clean_name = col_name.trim_matches('"');
        // Try exact match first
        if let Some(col) = batch.column_by_name(clean_name) {
            return Some(col);
        }
        // Try without table prefix (e.g., "o.user_id" -> "user_id")
        let stripped = Self::strip_table_prefix(clean_name);
        batch.column_by_name(stripped)
    }

    /// Check if two array values are equal at given indices
    fn arrays_equal_at(
        left: &ArrayRef,
        left_idx: usize,
        right: &ArrayRef,
        right_idx: usize,
    ) -> bool {
        if left.is_null(left_idx) && right.is_null(right_idx) {
            return true;
        }
        if left.is_null(left_idx) || right.is_null(right_idx) {
            return false;
        }

        if let (Some(l), Some(r)) = (
            left.as_any().downcast_ref::<Int64Array>(),
            right.as_any().downcast_ref::<Int64Array>(),
        ) {
            return l.value(left_idx) == r.value(right_idx);
        }
        if let (Some(l), Some(r)) = (
            left.as_any().downcast_ref::<StringArray>(),
            right.as_any().downcast_ref::<StringArray>(),
        ) {
            return l.value(left_idx) == r.value(right_idx);
        }
        if let (Some(l), Some(r)) = (
            left.as_any().downcast_ref::<Float64Array>(),
            right.as_any().downcast_ref::<Float64Array>(),
        ) {
            return (l.value(left_idx) - r.value(right_idx)).abs() < f64::EPSILON;
        }

        false
    }

    /// Take values from array with optional null indices (for LEFT JOIN)
    fn take_with_nulls(array: &ArrayRef, indices: &[Option<u32>]) -> io::Result<ArrayRef> {
        use arrow::array::*;

        let len = indices.len();

        if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
            let values: Vec<Option<i64>> = indices
                .iter()
                .map(|opt_idx| opt_idx.map(|idx| arr.value(idx as usize)))
                .collect();
            return Ok(Arc::new(Int64Array::from(values)));
        }

        if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
            let values: Vec<Option<f64>> = indices
                .iter()
                .map(|opt_idx| opt_idx.map(|idx| arr.value(idx as usize)))
                .collect();
            return Ok(Arc::new(Float64Array::from(values)));
        }

        if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            let values: Vec<Option<&str>> = indices
                .iter()
                .map(|opt_idx| opt_idx.map(|idx| arr.value(idx as usize)))
                .collect();
            return Ok(Arc::new(StringArray::from(values)));
        }

        if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
            let values: Vec<Option<bool>> = indices
                .iter()
                .map(|opt_idx| opt_idx.map(|idx| arr.value(idx as usize)))
                .collect();
            return Ok(Arc::new(BooleanArray::from(values)));
        }

        // Fallback: create null array
        Ok(arrow::array::new_null_array(array.data_type(), len))
    }
}

#[cfg(test)]
mod join_optimizer_tests {
    use super::*;

    #[test]
    fn join_order_requires_one_available_relation() {
        let on = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column("fact.customer_id".to_string())),
            op: crate::query::sql_parser::BinaryOperator::Eq,
            right: Box::new(SqlExpr::Column("customer.id".to_string())),
        };
        assert!(ApexExecutor::join_can_follow(
            &on,
            &["fact".to_string()],
            &["customer".to_string()]
        ));
        assert!(!ApexExecutor::join_can_follow(
            &on,
            &["product".to_string()],
            &["customer".to_string()]
        ));
    }
}
