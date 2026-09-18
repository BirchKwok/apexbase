// Scan-predicate GROUP BY fast paths: filter+group+order, cached transform/ratio/numeric, v4.

impl ApexExecutor {
    /// Stream a single-table projection SELECT in row-group batches over the
    /// stable persisted read view (S3).
    ///
    /// `emit` receives one `RecordBatch` per row group and returns `false` to
    /// stop early (the consumer disconnected or rejected the batch). Returns
    /// `Ok(None)` when the statement shape, projection or table state is
    /// outside the streaming gate, so the caller falls back to the
    /// materialized path. Only shapes whose output is identical to
    /// `execute` (verified by the parity test) are admitted.
    pub(crate) fn execute_streaming_select(
        sql: &str,
        base_dir: &Path,
        default_table_path: &Path,
        emit: &mut dyn FnMut(RecordBatch) -> bool,
    ) -> io::Result<Option<u64>> {
        use crate::query::sql_parser::{FromItem, SqlStatement};

        // Keep the per-query context consistent with the materialized path;
        // a streamed scan holds one row group, so the budget is not charged.
        let _memory_budget = QueryMemoryBudgetGuard::ensure();

        let stmt = match crate::query::sql_parser::SqlParser::parse(sql) {
            Ok(SqlStatement::Select(stmt)) => stmt,
            _ => return Ok(None),
        };
        if stmt.distinct
            || stmt.distinct_on.is_some()
            || !stmt.joins.is_empty()
            || !stmt.group_by.is_empty()
            || stmt.group_by_exprs.iter().any(Option::is_some)
            || stmt.having.is_some()
            || !stmt.order_by.is_empty()
            || stmt.limit.is_some()
            || stmt.offset.is_some()
            || stmt.window_row_number_limit.is_some()
        {
            return Ok(None);
        }
        // Projection: `SELECT *` or plain columns in SELECT order. Aliases
        // would rename output fields, and expressions/aggregates/EXCLUDE/
        // REPLACE/COLUMNS/windows keep their materialized path. The
        // materialized path reads the sorted `required_columns`, so it is the
        // scan projection order here that must match the SELECT list.
        let mut all_columns = false;
        let mut projection_names: Vec<String> = Vec::new();
        for column in &stmt.columns {
            match column {
                SelectColumn::All => {
                    if !projection_names.is_empty() {
                        return Ok(None);
                    }
                    all_columns = true;
                }
                SelectColumn::Column(name) => {
                    if all_columns {
                        return Ok(None);
                    }
                    let clean = name.trim_matches('"');
                    let clean = clean.rsplit('.').next().unwrap_or(clean);
                    projection_names.push(clean.trim_matches('"').to_string());
                }
                _ => return Ok(None),
            }
        }
        let Some(FromItem::Table { table, .. }) = &stmt.from else {
            return Ok(None);
        };
        let table_path = Self::resolve_table_path(table, base_dir, default_table_path);
        if !table_path.exists() {
            return Ok(None);
        }
        let backend = match get_cached_backend(&table_path) {
            Ok(backend) => backend,
            Err(_) => return Ok(None),
        };
        let predicate = match &stmt.where_clause {
            None => None,
            Some(expr) => match Self::build_scan_predicate(expr) {
                Some(predicate) => Some(predicate),
                None => return Ok(None),
            },
        };
        let projection: Option<Vec<String>> = if all_columns {
            None
        } else {
            let mut seen = std::collections::HashSet::new();
            if !projection_names
                .iter()
                .all(|name| seen.insert(name.clone()))
            {
                return Ok(None);
            }
            let refs: Vec<&str> = projection_names.iter().map(String::as_str).collect();
            match Self::filter_columns_for_backend(&backend, &refs) {
                Some(filtered) if filtered.len() == refs.len() => {
                    Some(filtered.into_iter().map(str::to_string).collect())
                }
                _ => return Ok(None),
            }
        };
        let projection_refs: Option<Vec<&str>> = projection
            .as_ref()
            .map(|columns| columns.iter().map(String::as_str).collect());
        let request = crate::storage::ScanRequest {
            projection: projection_refs.as_deref(),
            predicate: predicate.as_ref(),
        };
        let Some(stream) = backend.scan_batches(&request)? else {
            return Ok(None);
        };

        // Pull the first morsel before emitting: an unsupported batch can
        // still fall back without having produced partial output. A later
        // `Unsupported` is an error, because falling back would duplicate the
        // rows already emitted.
        let mut emitted_any = false;
        let mut rows: u64 = 0;
        for outcome in stream {
            match outcome? {
                crate::storage::BatchMorselOutcome::Morsel(morsel) => {
                    let batch = morsel.into_record_batch()?;
                    rows = rows.saturating_add(batch.num_rows() as u64);
                    emitted_any = true;
                    if !emit(batch) {
                        return Ok(Some(rows));
                    }
                }
                crate::storage::BatchMorselOutcome::Unsupported => {
                    if emitted_any {
                        return Err(err_data(
                            "streaming scan met an unsupported batch after output started",
                        ));
                    }
                    return Ok(None);
                }
            }
        }
        Ok(Some(rows))
    }

    /// FAST PATH for Complex (Filter+Group+Order) queries.
    /// Uses single-pass execution with direct dictionary indexing.
    fn try_fast_filter_group_order(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        // Reject unrelated query shapes before touching backend state. This
        // candidate is also probed for no-WHERE GROUP BY queries, including
        // the cached CASE aggregate path, where even extra hot-path checks are
        // measurable and the scan pipeline cannot apply.
        let where_clause = match &stmt.where_clause {
            Some(where_clause) => where_clause,
            None => return Ok(None),
        };

        // The legacy fused kernel does not receive a HAVING expression. Never
        // let it accept HAVING and silently filter on the wrong side of TopK;
        // the normalized scan pipeline below owns that complete semantic path.
        if backend.has_pending_deltas() || backend.has_delta() || stmt.having.is_some() {
            // Delta-aware and HAVING queries use the normalized scan/operator
            // pipeline. Keeping this fallback behind the existing fused-path
            // dispatch leaves unrelated WHERE hot paths byte-for-byte free of
            // an additional candidate branch.
            return Self::try_scan_group_pipeline(backend, stmt);
        }

        use crate::query::sql_parser::BinaryOperator;
        use crate::query::AggregateFunc;

        // Keep the proven fused kernel for its exact legacy shape. Every other
        // representable filtered GROUP BY is delegated to the shared physical
        // pipeline instead of growing another query-shaped branch here.
        if stmt.group_by.is_empty() || stmt.order_by.is_empty() || stmt.limit.is_none() {
            return Self::try_scan_group_pipeline(backend, stmt);
        }

        // Support: string equality (col = 'val') OR BETWEEN (col BETWEEN low AND high)
        enum FilterType<'a> {
            StringEq(String, &'a str),
            Between(String, f64, f64),
        }

        let filter = match where_clause {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => match (left.as_ref(), right.as_ref()) {
                (SqlExpr::Column(col), SqlExpr::Literal(Value::String(val))) => {
                    FilterType::StringEq(col.trim_matches('"').to_string(), val.as_str())
                }
                (SqlExpr::Literal(Value::String(val)), SqlExpr::Column(col)) => {
                    FilterType::StringEq(col.trim_matches('"').to_string(), val.as_str())
                }
                _ => return Self::try_scan_group_pipeline(backend, stmt),
            },
            SqlExpr::Between {
                column,
                low,
                high,
                negated,
            } if !negated => {
                let low_val = Self::extract_numeric_value(low).ok();
                let high_val = Self::extract_numeric_value(high).ok();
                if let (Some(lo), Some(hi)) = (low_val, high_val) {
                    FilterType::Between(column.trim_matches('"').to_string(), lo, hi)
                } else {
                    return Self::try_scan_group_pipeline(backend, stmt);
                }
            }
            _ => return Self::try_scan_group_pipeline(backend, stmt),
        };

        // Must have exactly one GROUP BY column (string)
        if stmt.group_by.len() != 1 {
            return Self::try_scan_group_pipeline(backend, stmt);
        }
        let group_col = stmt.group_by[0].trim_matches('"');

        // Must have exactly one ORDER BY clause
        if stmt.order_by.len() != 1 {
            return Self::try_scan_group_pipeline(backend, stmt);
        }
        let order_clause = &stmt.order_by[0];
        let order_col = order_clause.column.trim_matches('"');
        let descending = order_clause.descending;

        // Check if we have exactly one aggregate column
        let aggregates = stmt
            .columns
            .iter()
            .filter_map(|column| match column {
                SelectColumn::Aggregate { func, column, .. } => {
                    Some((func.clone(), column.as_deref()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let [(agg_func, agg_col)] = aggregates.as_slice() else {
            return Self::try_scan_group_pipeline(backend, stmt);
        };

        // Support SUM, COUNT, and AVG
        if !matches!(
            *agg_func,
            AggregateFunc::Sum | AggregateFunc::Count | AggregateFunc::Avg
        ) {
            return Self::try_scan_group_pipeline(backend, stmt);
        }

        let limit = stmt.limit.unwrap_or(100);
        let offset = stmt.offset.unwrap_or(0);

        // For string equality filter, use existing storage-level path
        match &filter {
            FilterType::StringEq(filter_col, filter_val) => {
                // Only SUM/COUNT for the storage-level string eq path
                if !matches!(agg_func, AggregateFunc::Sum | AggregateFunc::Count) {
                    return Self::try_scan_group_pipeline(backend, stmt);
                }
                match backend.execute_filter_group_order(
                    filter_col,
                    filter_val,
                    group_col,
                    *agg_col,
                    agg_func.clone(),
                    order_col,
                    descending,
                    limit,
                    offset,
                ) {
                    Ok(Some(result)) => {
                        crate::query::executor::record_path("fused_filter_group_order");
                        Ok(Some(ApexResult::Data(result)))
                    }
                    Ok(None) => Self::try_scan_group_pipeline(backend, stmt),
                    Err(e) => Err(e),
                }
            }
            FilterType::Between(filter_col, lo, hi) => {
                let raw = if let Some(dict_arc) = crate::storage::backend::get_global_dict_cache(
                    backend.path(),
                    group_col,
                    &backend.storage,
                )? {
                    backend.execute_between_group_agg_cached(
                        filter_col,
                        *lo,
                        *hi,
                        &dict_arc.0,
                        &dict_arc.1,
                        *agg_col,
                    )?
                } else {
                    backend
                        .storage
                        .execute_between_group_agg(
                            filter_col,
                            *lo,
                            *hi,
                            group_col,
                            *agg_col,
                        )?
                };

                let raw = match raw {
                    Some(r) if !r.is_empty() => r,
                    _ => return Self::try_scan_group_pipeline(backend, stmt),
                };

                // Compute final aggregated values
                let mut results: Vec<(String, f64)> = raw
                    .iter()
                    .map(|(k, sum, count)| {
                        let val = match agg_func {
                            AggregateFunc::Sum => *sum,
                            AggregateFunc::Count => *count as f64,
                            AggregateFunc::Avg => {
                                if *count > 0 {
                                    *sum / *count as f64
                                } else {
                                    0.0
                                }
                            }
                            _ => *sum,
                        };
                        (k.clone(), val)
                    })
                    .collect();

                // Sort
                if descending {
                    results
                        .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                } else {
                    results
                        .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                }
                let results: Vec<_> = results.into_iter().skip(offset).take(limit).collect();

                if results.is_empty() {
                    return Self::try_scan_group_pipeline(backend, stmt);
                }

                // Build Arrow result
                let group_values: Vec<&str> = results.iter().map(|(k, _)| k.as_str()).collect();
                let agg_values: Vec<f64> = results.iter().map(|(_, v)| *v).collect();

                let group_col_name = group_col.to_string();
                let agg_col_name = stmt
                    .columns
                    .iter()
                    .find_map(|c| {
                        if let SelectColumn::Aggregate { alias, .. } = c {
                            alias.clone().or_else(|| Some(order_col.to_string()))
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| order_col.to_string());

                let schema = Arc::new(Schema::new(vec![
                    Field::new(&group_col_name, ArrowDataType::Utf8, false),
                    Field::new(&agg_col_name, ArrowDataType::Float64, false),
                ]));
                let arrays: Vec<ArrayRef> = vec![
                    Arc::new(StringArray::from(group_values)),
                    Arc::new(Float64Array::from(agg_values)),
                ];
                let result =
                    RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;

                crate::query::executor::record_path("fused_between_group_agg");
                Ok(Some(ApexResult::Data(result)))
            }
        }
    }

    /// Translate an exactly representable SQL predicate into the storage scan
    /// protocol. The result preserves boolean structure and typed literals;
    /// GROUP BY, HAVING, ordering, and aggregate choices remain downstream
    /// physical-operator concerns.
    fn build_scan_predicate(expr: &SqlExpr) -> Option<crate::storage::ScanPredicateExpr> {
        use crate::query::sql_parser::{BinaryOperator, UnaryOperator};
        use crate::storage::{
            ScanBound, ScanComparison, ScanPredicate, ScanPredicateExpr, ScanValue,
        };

        fn clean(column: &str) -> String {
            column
                .trim_matches('"')
                .rsplit('.')
                .next()
                .unwrap_or(column)
                .trim_matches('"')
                .to_string()
        }

        fn scalar(value: &SqlExpr) -> Option<ScanValue> {
            match value {
                SqlExpr::Literal(Value::Int8(value)) => Some(ScanValue::Int(*value as i64)),
                SqlExpr::Literal(Value::Int16(value)) => Some(ScanValue::Int(*value as i64)),
                SqlExpr::Literal(Value::Int32(value)) => Some(ScanValue::Int(*value as i64)),
                SqlExpr::Literal(Value::Int64(value)) => Some(ScanValue::Int(*value)),
                SqlExpr::Literal(Value::UInt8(value)) => Some(ScanValue::UInt(*value as u64)),
                SqlExpr::Literal(Value::UInt16(value)) => Some(ScanValue::UInt(*value as u64)),
                SqlExpr::Literal(Value::UInt32(value)) => Some(ScanValue::UInt(*value as u64)),
                SqlExpr::Literal(Value::UInt64(value)) => Some(ScanValue::UInt(*value)),
                SqlExpr::Literal(Value::Float32(value)) if value.is_finite() => {
                    Some(ScanValue::Float(*value as f64))
                }
                SqlExpr::Literal(Value::Float64(value)) if value.is_finite() => {
                    Some(ScanValue::Float(*value))
                }
                SqlExpr::Literal(Value::String(value)) => {
                    Some(ScanValue::String(value.clone()))
                }
                SqlExpr::Literal(Value::Bool(value)) => Some(ScanValue::Bool(*value)),
                // Negative literals parse as UnaryOp(Minus, literal); fold
                // them so ranges with negative bounds stay inside the typed
                // scan protocol.
                SqlExpr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr,
                } => match expr.as_ref() {
                    SqlExpr::Literal(Value::Int8(value)) => {
                        Some(ScanValue::Int(-(*value as i64)))
                    }
                    SqlExpr::Literal(Value::Int16(value)) => {
                        Some(ScanValue::Int(-(*value as i64)))
                    }
                    SqlExpr::Literal(Value::Int32(value)) => {
                        Some(ScanValue::Int(-(*value as i64)))
                    }
                    SqlExpr::Literal(Value::Int64(value)) => {
                        value.checked_neg().map(ScanValue::Int)
                    }
                    SqlExpr::Literal(Value::Float32(value)) if value.is_finite() => {
                        Some(ScanValue::Float(-(*value as f64)))
                    }
                    SqlExpr::Literal(Value::Float64(value)) if value.is_finite() => {
                        Some(ScanValue::Float(-*value))
                    }
                    _ => None,
                },
                _ => None,
            }
        }

        fn comparison(operator: &BinaryOperator) -> Option<ScanComparison> {
            Some(match operator {
                BinaryOperator::Eq => ScanComparison::Eq,
                BinaryOperator::NotEq => ScanComparison::NotEq,
                BinaryOperator::Lt => ScanComparison::Lt,
                BinaryOperator::Le => ScanComparison::Le,
                BinaryOperator::Gt => ScanComparison::Gt,
                BinaryOperator::Ge => ScanComparison::Ge,
                _ => return None,
            })
        }

        fn reversed(operator: ScanComparison) -> ScanComparison {
            match operator {
                ScanComparison::Eq => ScanComparison::Eq,
                ScanComparison::NotEq => ScanComparison::NotEq,
                ScanComparison::Gt => ScanComparison::Lt,
                ScanComparison::Ge => ScanComparison::Le,
                ScanComparison::Lt => ScanComparison::Gt,
                ScanComparison::Le => ScanComparison::Ge,
            }
        }

        match expr {
            SqlExpr::Paren(inner) => Self::build_scan_predicate(inner),
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => Some(ScanPredicateExpr::And(
                Box::new(Self::build_scan_predicate(left)?),
                Box::new(Self::build_scan_predicate(right)?),
            )),
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Or,
                right,
            } => Some(ScanPredicateExpr::Or(
                Box::new(Self::build_scan_predicate(left)?),
                Box::new(Self::build_scan_predicate(right)?),
            )),
            SqlExpr::Between {
                column,
                low,
                high,
                negated: false,
            } => {
                let (low, high) = (scalar(low)?, scalar(high)?);
                Some(ScanPredicateExpr::Predicate(ScanPredicate::Between {
                    column: clean(column),
                    lower: Some(ScanBound::inclusive(low)),
                    upper: Some(ScanBound::inclusive(high)),
                }))
            }
            SqlExpr::BinaryOp { left, op, right } => {
                let op = comparison(op)?;
                match (left.as_ref(), right.as_ref()) {
                    (SqlExpr::Column(column), literal) => Some(ScanPredicateExpr::Predicate(
                        ScanPredicate::Compare {
                            column: clean(column),
                            op,
                            value: scalar(literal)?,
                        },
                    )),
                    (literal, SqlExpr::Column(column)) => Some(ScanPredicateExpr::Predicate(
                        ScanPredicate::Compare {
                            column: clean(column),
                            op: reversed(op),
                            value: scalar(literal)?,
                        },
                    )),
                    _ => None,
                }
            }
            SqlExpr::In {
                column,
                values,
                negated: false,
            } => {
                let values = values
                    .iter()
                    .filter(|value| !matches!(value, Value::Null))
                    .map(|value| scalar(&SqlExpr::Literal(value.clone())))
                    .collect::<Option<Vec<_>>>()?;
                Some(ScanPredicateExpr::Predicate(ScanPredicate::In {
                    column: clean(column),
                    values,
                }))
            }
            SqlExpr::IsNull { column, negated } => Some(ScanPredicateExpr::Predicate(
                ScanPredicate::IsNull {
                    column: clean(column),
                    negated: *negated,
                },
            )),
            _ => None,
        }
    }

    /// First vertical physical pipeline over the shared scan protocol:
    /// Filter -> GROUP BY -> HAVING -> ordered TopK. The storage lane decides
    /// between selective mmap materialization and a delta-aware merged scan;
    /// every downstream operator sees the same filtered RecordBatch contract.
    fn try_scan_group_pipeline(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        if stmt.where_clause.is_none()
            || stmt.group_by.is_empty()
            || !stmt.joins.is_empty()
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || stmt.group_by_exprs.iter().any(Option::is_some)
            || stmt
                .columns
                .iter()
                .any(|column| matches!(column, SelectColumn::WindowFunction { .. }))
        {
            return Ok(None);
        }

        let Some(predicate) =
            Self::build_scan_predicate(stmt.where_clause.as_ref().unwrap())
        else {
            return Ok(None);
        };

        // Batched physical pipeline first: row-group-sized morsels with an
        // incremental group state keep scan memory bounded by one row group.
        // Any shape or state outside its gate falls through to the
        // single-batch path below.
        if let Some(result) = Self::try_batch_group_pipeline(
            backend, stmt, &predicate, backend.table_key(),
        )? {
            return Ok(Some(result));
        }

        let Some(columns) = Self::get_col_refs(stmt) else {
            return Ok(None);
        };
        let column_refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
        let Some(projection) = Self::filter_columns_for_backend(backend, &column_refs) else {
            return Ok(None);
        };
        let request = crate::storage::ScanRequest {
            projection: Some(&projection),
            predicate: Some(&predicate),
        };
        let Some(morsel) = backend.scan(&request)? else {
            return Ok(None);
        };
        let filtered = morsel.into_record_batch()?;

        // The scan protocol consumed the complete WHERE predicate. Removing
        // it prevents the generic executor from evaluating the mask twice;
        // HAVING remains attached and is applied after aggregation, before TopK.
        let mut physical_stmt = stmt.clone();
        physical_stmt.where_clause = None;
        let result = Self::execute_group_by(&filtered, &physical_stmt)?;
        crate::query::executor::record_path("scan_group_pipeline");
        Ok(Some(result))
    }

    /// Apply a low-cost dictionary transform (currently SQL SUBSTR) once per
    /// distinct value, then aggregate through remapped categorical IDs.
    fn try_fast_cached_transform_group_by(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        if backend.is_in_memory()
            || backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
            || stmt.group_by.len() != 1
            || stmt.where_clause.is_some()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || stmt.distinct
            || stmt.distinct_on.is_some()
        {
            return Ok(None);
        }
        let Some(Some(SqlExpr::Function { name, args })) = stmt.group_by_exprs.first() else {
            return Ok(None);
        };
        if !matches!(name.to_ascii_uppercase().as_str(), "SUBSTR" | "SUBSTRING")
            || !(args.len() == 2 || args.len() == 3)
        {
            return Ok(None);
        }
        let SqlExpr::Column(source_column) = &args[0] else {
            return Ok(None);
        };
        let integer = |expr: &SqlExpr| match expr {
            SqlExpr::Literal(Value::Int64(value)) => Some(*value),
            SqlExpr::Literal(Value::Int32(value)) => Some(*value as i64),
            _ => None,
        };
        let Some(start) = integer(&args[1]) else {
            return Ok(None);
        };
        let length = if args.len() == 3 {
            let Some(length) = integer(&args[2]) else {
                return Ok(None);
            };
            Some(length)
        } else {
            None
        };
        if start == 0 || length.is_some_and(|length| length < 0) {
            return Ok(None);
        }
        let source_column = source_column
            .trim_matches('"')
            .rsplit('.')
            .next()
            .unwrap_or(source_column.trim_matches('"'));
        if !Self::column_is_string(backend, source_column)
            || backend.column_has_nulls(source_column)
        {
            return Ok(None);
        }

        enum Output {
            Key(String),
            Aggregate(String, AggregateFunc, usize),
        }
        let mut outputs = Vec::with_capacity(stmt.columns.len());
        let mut aggregate_specs: Vec<(String, bool)> = Vec::new();
        let mut saw_key = false;
        for column in &stmt.columns {
            match column {
                SelectColumn::Expression {
                    expr: SqlExpr::Function { name, .. },
                    alias,
                } if matches!(name.to_ascii_uppercase().as_str(), "SUBSTR" | "SUBSTRING")
                    && !saw_key =>
                {
                    saw_key = true;
                    outputs.push(Output::Key(alias.clone().unwrap_or_else(|| stmt.group_by[0].clone())));
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
                        column.trim_matches('"').to_string()
                    };
                    let slot = aggregate_specs.len();
                    aggregate_specs.push((source.clone(), count_star));
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
        if !saw_key || aggregate_specs.is_empty() {
            return Ok(None);
        }

        let Some(transformed_dictionary) = crate::storage::backend::get_global_substr_dict_cache(
            backend.path(),
            source_column,
            start,
            length,
            &backend.storage,
        )? else {
            return Ok(None);
        };
        if transformed_dictionary.1.len() != backend.active_row_count() as usize {
            return Ok(None);
        }
        let aggregate_refs: Vec<(&str, bool)> = aggregate_specs
            .iter()
            .map(|(column, count_star)| (column.as_str(), *count_star))
            .collect();
        let Some(raw) = backend.execute_group_agg_cached(
            &transformed_dictionary.0,
            &transformed_dictionary.1,
            &aggregate_refs,
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
                        raw.iter().map(|(key, _)| key.as_str()).collect::<Vec<_>>(),
                    )));
                }
                Output::Aggregate(name, func, slot) => {
                    if matches!(func, AggregateFunc::Count) {
                        fields.push(Field::new(name, ArrowDataType::Int64, false));
                        arrays.push(Arc::new(Int64Array::from(
                            raw.iter().map(|(_, stats)| stats[slot].1).collect::<Vec<_>>(),
                        )));
                    } else {
                        fields.push(Field::new(name, ArrowDataType::Float64, true));
                        arrays.push(Arc::new(Float64Array::from(
                            raw.iter()
                                .map(|(_, stats)| {
                                    let (sum, count) = stats[slot];
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

    /// Parallel native GROUP BY for an integral key and numeric aggregates.
    fn try_fast_cached_ratio_group_by(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::sql_parser::BinaryOperator;

        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
            || stmt.group_by.len() != 1
            || stmt.group_by_exprs.first().and_then(Option::as_ref).is_some()
            || stmt.where_clause.is_some()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || stmt.distinct
            || stmt.distinct_on.is_some()
        {
            return Ok(None);
        }
        fn clean(name: &str) -> &str {
            let trimmed = name.trim_matches('"');
            trimmed
                .rsplit('.')
                .next()
                .unwrap_or(trimmed)
                .trim_matches('"')
        }
        fn literal_f64(expr: &SqlExpr) -> Option<f64> {
            match expr {
                SqlExpr::Literal(Value::Int64(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Int32(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Float64(value)) => Some(*value),
                SqlExpr::Literal(Value::Float32(value)) => Some(*value as f64),
                _ => None,
            }
        }
        fn denominator(expr: &SqlExpr) -> Option<(&str, f64)> {
            match expr {
                SqlExpr::Column(column) => Some((clean(column), 0.0)),
                SqlExpr::Paren(expr) => denominator(expr),
                SqlExpr::BinaryOp {
                    left,
                    op: BinaryOperator::Add,
                    right,
                } => match (left.as_ref(), right.as_ref()) {
                    (SqlExpr::Column(column), literal) => {
                        literal_f64(literal).map(|offset| (clean(column), offset))
                    }
                    (literal, SqlExpr::Column(column)) => {
                        literal_f64(literal).map(|offset| (clean(column), offset))
                    }
                    _ => None,
                },
                _ => None,
            }
        }
        let group_column = clean(&stmt.group_by[0]);
        enum Output {
            Group(String),
            Ratio(String, usize),
        }
        let mut outputs = Vec::with_capacity(stmt.columns.len());
        let mut ratios = Vec::<(String, String, f64)>::new();
        for column in &stmt.columns {
            match column {
                SelectColumn::Column(name) if clean(name) == group_column => {
                    outputs.push(Output::Group(Self::group_output_name(stmt, group_column)));
                }
                SelectColumn::ColumnAlias { column, alias }
                    if clean(column) == group_column =>
                {
                    outputs.push(Output::Group(alias.clone()));
                }
                SelectColumn::Expression {
                    expr: SqlExpr::Function { name, args },
                    alias,
                } if name.eq_ignore_ascii_case("AVG") && args.len() == 1 => {
                    let SqlExpr::BinaryOp {
                        left,
                        op: BinaryOperator::Div,
                        right,
                    } = &args[0]
                    else {
                        return Ok(None);
                    };
                    let SqlExpr::Column(numerator) = left.as_ref() else {
                        return Ok(None);
                    };
                    let Some((denominator, offset)) = denominator(right) else {
                        return Ok(None);
                    };
                    let slot = ratios.len();
                    ratios.push((
                        clean(numerator).to_string(),
                        denominator.to_string(),
                        offset,
                    ));
                    outputs.push(Output::Ratio(
                        alias.clone().unwrap_or_else(|| "AVG".to_string()),
                        slot,
                    ));
                }
                _ => return Ok(None),
            }
        }
        if ratios.is_empty() || !outputs.iter().any(|output| matches!(output, Output::Group(_))) {
            return Ok(None);
        }
        let group_cache = if Self::column_is_string(backend, group_column) {
            crate::storage::backend::get_global_dict_cache_with_nulls(
                backend.path(),
                group_column,
                &backend.storage,
            )?
        } else {
            crate::storage::backend::get_global_numeric_dict_cache(
                backend.path(),
                group_column,
                &backend.storage,
            )?
        };
        let Some((cache, has_nulls, max_group_id)) = group_cache else {
            return Ok(None);
        };
        if has_nulls
            || cache.1.len() != backend.active_row_count() as usize
            || max_group_id.is_some_and(|id| (id as usize) >= cache.0.len())
        {
            return Ok(None);
        }
        let ratio_refs = ratios
            .iter()
            .map(|(numerator, denominator, offset)| {
                (numerator.as_str(), denominator.as_str(), *offset)
            })
            .collect::<Vec<_>>();
        let Some(rows) = backend.execute_group_ratio_avg_cached(
            &cache.1,
            cache.0.len(),
            &ratio_refs,
        )? else {
            return Ok(None);
        };
        let mut fields = Vec::with_capacity(outputs.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(outputs.len());
        for output in outputs {
            match output {
                Output::Group(name) if Self::column_is_string(backend, group_column) => {
                    fields.push(Field::new(name, ArrowDataType::Utf8, false));
                    arrays.push(Arc::new(StringArray::from(
                        rows.iter()
                            .map(|(group, _)| cache.0[*group].as_str())
                            .collect::<Vec<_>>(),
                    )));
                }
                Output::Group(name) => {
                    let values = rows
                        .iter()
                        .map(|(group, _)| cache.0[*group].parse::<i64>())
                        .collect::<Result<Vec<_>, _>>();
                    let Ok(values) = values else {
                        return Ok(None);
                    };
                    fields.push(Field::new(name, ArrowDataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(values)));
                }
                Output::Ratio(name, slot) => {
                    fields.push(Field::new(name, ArrowDataType::Float64, true));
                    arrays.push(Arc::new(Float64Array::from(
                        rows.iter()
                            .map(|(_, ratios)| {
                                let (sum, count) = ratios[slot];
                                (count > 0).then_some(sum / count as f64)
                            })
                            .collect::<Vec<_>>(),
                    )));
                }
            }
        }
        let mut batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| err_data(error.to_string()))?;
        if !stmt.order_by.is_empty() {
            let order = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let top_k = stmt.limit.map(|limit| limit + stmt.offset.unwrap_or(0));
            batch = Self::apply_order_by_topk(&batch, &order, top_k)?;
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

    /// Parallel native GROUP BY for an integral key and numeric aggregates.
    fn try_fast_numeric_group_by(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
            || stmt.group_by.len() != 1
            || stmt.where_clause.is_some()
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

        let (group_col, modulus) = match stmt.group_by_exprs.first().and_then(Option::as_ref) {
            None => (clean_name(&stmt.group_by[0]), None),
            Some(SqlExpr::Function { name, args })
                if name.eq_ignore_ascii_case("MOD") && args.len() == 2 =>
            {
                let SqlExpr::Column(column) = &args[0] else {
                    return Ok(None);
                };
                let modulus = match &args[1] {
                    SqlExpr::Literal(Value::Int64(value)) => *value,
                    SqlExpr::Literal(Value::Int32(value)) => *value as i64,
                    _ => return Ok(None),
                };
                if modulus <= 0 || modulus > 32_768 {
                    return Ok(None);
                }
                (clean_name(column), Some(modulus))
            }
            Some(_) => return Ok(None),
        };
        // This path is only for integral keys.  Check the schema before the
        // null scan; string GROUP BY queries are handled by the dictionary
        // cache and should not pay a full-column nullability pass here.
        if Self::column_is_string(backend, group_col) {
            return Ok(None);
        }
        if backend.column_has_nulls(group_col) {
            return Ok(None);
        }

        enum Output {
            Key(String),
            Aggregate {
                name: String,
                func: AggregateFunc,
                slot: usize,
            },
        }
        let mut outputs = Vec::with_capacity(stmt.columns.len());
        let mut agg_specs: Vec<(String, bool)> = Vec::new();
        let mut having_columns = std::collections::HashMap::new();
        for select_column in &stmt.columns {
            match select_column {
                SelectColumn::Column(name)
                    if modulus.is_none() && clean_name(name) == group_col =>
                {
                    outputs.push(Output::Key(Self::group_output_name(stmt, group_col)));
                }
                SelectColumn::ColumnAlias { column, alias }
                    if modulus.is_none() && clean_name(column) == group_col =>
                {
                    outputs.push(Output::Key(alias.clone()));
                }
                SelectColumn::Expression {
                    expr: SqlExpr::Function { name, args },
                    alias,
                } if modulus.is_some()
                    && name.eq_ignore_ascii_case("MOD")
                    && args.len() == 2 =>
                {
                    let matches_group = matches!(
                        (&args[0], &args[1]),
                        (
                            SqlExpr::Column(column),
                            SqlExpr::Literal(Value::Int64(value))
                        ) if clean_name(column) == group_col && Some(*value) == modulus
                    ) || matches!(
                        (&args[0], &args[1]),
                        (
                            SqlExpr::Column(column),
                            SqlExpr::Literal(Value::Int32(value))
                        ) if clean_name(column) == group_col && Some(*value as i64) == modulus
                    );
                    if !matches_group {
                        return Ok(None);
                    }
                    outputs.push(Output::Key(
                        alias.clone().unwrap_or_else(|| stmt.group_by[0].clone()),
                    ));
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
                                || column
                                    .chars()
                                    .next()
                                    .is_some_and(|character| character.is_ascii_digit())
                        });
                    let source = if count_star {
                        "*".to_string()
                    } else {
                        let Some(column) = column else {
                            return Ok(None);
                        };
                        clean_name(column).to_string()
                    };
                    let slot = agg_specs.len();
                    agg_specs.push((source.clone(), count_star));
                    let function_name = match func {
                        AggregateFunc::Count => "COUNT",
                        AggregateFunc::Sum => "SUM",
                        AggregateFunc::Avg => "AVG",
                        AggregateFunc::Min => "MIN",
                        AggregateFunc::Max => "MAX",
                    };
                    let name = alias.clone().unwrap_or_else(|| {
                        format!("{}({})", function_name, if count_star { "*" } else { &source })
                    });
                    having_columns.insert(
                        format!(
                            "{}({})",
                            function_name,
                            if count_star { "*" } else { &source }
                        )
                        .to_ascii_uppercase(),
                        name.clone(),
                    );
                    outputs.push(Output::Aggregate {
                        name,
                        func: func.clone(),
                        slot,
                    });
                }
                _ => return Ok(None),
            }
        }
        if agg_specs.is_empty() {
            return Ok(None);
        }

        let agg_refs: Vec<(&str, bool)> = agg_specs
            .iter()
            .map(|(name, count_star)| (name.as_str(), *count_star))
            .collect();
        let agg_ops: Vec<u8> = outputs
            .iter()
            .filter_map(|output| match output {
                Output::Aggregate { func, .. } => Some(match func {
                    AggregateFunc::Count => 0,
                    AggregateFunc::Sum | AggregateFunc::Avg => 1,
                    AggregateFunc::Min => 2,
                    AggregateFunc::Max => 4,
                }),
                Output::Key(_) => None,
            })
            .collect();
        let mut cached_mod_raw = None;
        if let Some(modulus) = modulus {
            let cache_eligible = outputs.iter().all(|output| match output {
                Output::Key(_) => true,
                Output::Aggregate { func, .. } => {
                    matches!(func, AggregateFunc::Count | AggregateFunc::Avg)
                }
            });
            if cache_eligible {
                if let Some(cache) =
                    crate::storage::backend::get_global_numeric_mod_dict_cache(
                        backend.path(),
                        group_col,
                        modulus,
                        &backend.storage,
                    )?
                {
                    if let Some(rows) = backend.execute_group_agg_cached(
                        &cache.0,
                        &cache.1,
                        &agg_refs,
                    )? {
                        cached_mod_raw = Some(
                            rows.into_iter()
                                .filter_map(|(key, stats)| {
                                    key.parse::<i64>().ok().map(|key| {
                                        crate::storage::on_demand::NativeNumericGroupAgg {
                                            key,
                                            stats: stats
                                                .into_iter()
                                                .map(|(sum, count)| {
                                                    (count, sum, 0.0, 0.0, false)
                                                })
                                                .collect(),
                                        }
                                    })
                                })
                                .collect(),
                        );
                    }
                }
            }
        }
        let mut cached_identity_raw = None;
        if modulus.is_none() {
            if let Some((cache, has_nulls, max_group_id)) =
                crate::storage::backend::get_global_numeric_dict_cache(
                    backend.path(),
                    group_col,
                    &backend.storage,
                )?
            {
                if !has_nulls
                    && cache.1.len() == backend.active_row_count() as usize
                    && max_group_id.is_none_or(|id| (id as usize) < cache.0.len())
                {
                    if let Some(rows) = backend.execute_group_stats_cached(
                        &cache.1,
                        cache.0.len(),
                        &agg_refs,
                    )? {
                        let parsed = rows
                            .into_iter()
                            .map(|(slot, stats)| {
                                cache.0[slot]
                                    .parse::<i64>()
                                    .map(|key| crate::storage::on_demand::NativeNumericGroupAgg {
                                        key,
                                        stats,
                                    })
                            })
                            .collect::<Result<Vec<_>, _>>();
                        if let Ok(parsed) = parsed {
                            cached_identity_raw = Some(parsed);
                        }
                    }
                }
            }
        }
        let raw = if cached_mod_raw.is_some() {
            cached_mod_raw
        } else if cached_identity_raw.is_some() {
            cached_identity_raw
        } else if let Some(modulus) = modulus {
            backend.execute_numeric_mod_group_agg_mmap(
                group_col,
                modulus,
                &agg_refs,
                &agg_ops,
            )?
        } else {
            backend.execute_numeric_group_agg_mmap(group_col, &agg_refs, &agg_ops)?
        };
        let Some(raw) = raw else {
            return Ok(None);
        };
        if raw.is_empty() {
            return Ok(None);
        }

        let mut fields = Vec::with_capacity(outputs.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(outputs.len());
        for output in outputs {
            match output {
                Output::Key(name) => {
                    fields.push(Field::new(&name, ArrowDataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(
                        raw.iter().map(|row| row.key).collect::<Vec<_>>(),
                    )));
                }
                Output::Aggregate { name, func, slot } => match func {
                    AggregateFunc::Count => {
                        fields.push(Field::new(&name, ArrowDataType::Int64, false));
                        arrays.push(Arc::new(Int64Array::from(
                            raw.iter()
                                .map(|row| row.stats[slot].0)
                                .collect::<Vec<_>>(),
                        )));
                    }
                    AggregateFunc::Avg => {
                        fields.push(Field::new(&name, ArrowDataType::Float64, true));
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
                            fields.push(Field::new(&name, ArrowDataType::Int64, true));
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
                            fields.push(Field::new(&name, ArrowDataType::Float64, true));
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
        let schema = Arc::new(Schema::new(fields));
        let mut batch = RecordBatch::try_new(schema, arrays)
            .map_err(|error| err_data(error.to_string()))?;
        if let Some(having) = &stmt.having {
            fn resolve_aggregate_refs(
                expr: &SqlExpr,
                columns: &std::collections::HashMap<String, String>,
            ) -> SqlExpr {
                match expr {
                    SqlExpr::Function { name, args }
                        if matches!(
                            name.to_ascii_uppercase().as_str(),
                            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
                        ) =>
                    {
                        let argument = args.first().map_or("*".to_string(), |argument| {
                            match argument {
                                SqlExpr::Column(column) => clean_name(column).to_string(),
                                SqlExpr::Literal(Value::String(value)) if value == "*" => {
                                    "*".to_string()
                                }
                                _ => "*".to_string(),
                            }
                        });
                        let key = format!("{}({})", name, argument).to_ascii_uppercase();
                        columns
                            .get(&key)
                            .map(|column| SqlExpr::Column(column.clone()))
                            .unwrap_or_else(|| expr.clone())
                    }
                    SqlExpr::BinaryOp { left, op, right } => SqlExpr::BinaryOp {
                        left: Box::new(resolve_aggregate_refs(left, columns)),
                        op: op.clone(),
                        right: Box::new(resolve_aggregate_refs(right, columns)),
                    },
                    SqlExpr::UnaryOp { op, expr } => SqlExpr::UnaryOp {
                        op: op.clone(),
                        expr: Box::new(resolve_aggregate_refs(expr, columns)),
                    },
                    SqlExpr::Paren(expr) => {
                        SqlExpr::Paren(Box::new(resolve_aggregate_refs(expr, columns)))
                    }
                    SqlExpr::Cast { expr, data_type } => SqlExpr::Cast {
                        expr: Box::new(resolve_aggregate_refs(expr, columns)),
                        data_type: data_type.clone(),
                    },
                    _ => expr.clone(),
                }
            }
            let resolved = resolve_aggregate_refs(having, &having_columns);
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

    /// V4 FAST PATH for GROUP BY queries without WHERE
    /// Handles: SELECT group_col, AGG1(col1), AGG2(col2) FROM table GROUP BY group_col
    fn try_fast_v4_group_by(
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

        // Must be single GROUP BY column, no WHERE, no ORDER BY
        if stmt.group_by.len() != 1 || stmt.where_clause.is_some() || !stmt.order_by.is_empty() {
            return Ok(None);
        }

        let group_col = stmt.group_by[0].trim_matches('"');

        // Extract aggregate columns: (col_name_or_"*", is_count_star, func, alias)
        let mut agg_info: Vec<(&str, bool, AggregateFunc, Option<String>)> = Vec::new();

        for col in &stmt.columns {
            match col {
                SelectColumn::Aggregate {
                    func,
                    column,
                    alias,
                    ..
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

        // Build agg_cols for storage call
        let agg_cols: Vec<(&str, bool)> = agg_info
            .iter()
            .map(|(col, is_count, _, _)| (*col, *is_count))
            .collect();

        let raw = match backend.execute_group_agg(group_col, &agg_cols)? {
            Some(r) if !r.is_empty() => r,
            _ => return Ok(None),
        };

        // Build result: group_col + one column per aggregate
        let num_groups = raw.len();
        let group_values: Vec<&str> = raw.iter().map(|(k, _)| k.as_str()).collect();

        let mut fields: Vec<Field> = vec![Field::new(
            Self::group_output_name(stmt, group_col),
            ArrowDataType::Utf8,
            false,
        )];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(group_values))];

        for (ai, (_, _, func, alias)) in agg_info.iter().enumerate() {
            let col_name = alias.as_deref().unwrap_or(match func {
                AggregateFunc::Count => "COUNT(*)",
                AggregateFunc::Avg => "AVG",
                AggregateFunc::Sum => "SUM",
                AggregateFunc::Min => "MIN",
                AggregateFunc::Max => "MAX",
            });

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
                        AggregateFunc::Sum => sum,
                        _ => sum,
                    }
                })
                .collect();

            // Use Int64 for COUNT, Float64 for others
            if matches!(func, AggregateFunc::Count) {
                let int_values: Vec<i64> = values.iter().map(|v| *v as i64).collect();
                fields.push(Field::new(col_name, ArrowDataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(int_values)));
            } else {
                fields.push(Field::new(col_name, ArrowDataType::Float64, false));
                arrays.push(Arc::new(Float64Array::from(values)));
            }
        }

        // Apply HAVING if present
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;

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

        // Apply ORDER BY with aggregate expression resolver
        if !stmt.order_by.is_empty() {
            let resolved_ob = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
            result = Self::apply_order_by_topk(&result, &resolved_ob, k)?;
        }

        // Apply LIMIT + OFFSET
        if stmt.limit.is_some() || stmt.offset.is_some() {
            result = Self::apply_limit_offset(&result, stmt.limit, stmt.offset)?;
        }

        Ok(Some(ApexResult::Data(result)))
    }
}
