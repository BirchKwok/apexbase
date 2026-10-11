// External file fast paths: CSV/JSON/Parquet count, aggregation, and filter pushdown.

impl ApexExecutor {
    /// Extract a simple comparison filter from a WHERE clause for pushdown
    /// into file readers (CSV/JSON/Parquet). Returns "col>val" style string.
    fn try_extract_filter_for_pushdown(expr: &SqlExpr) -> Option<String> {
        use crate::query::sql_parser::BinaryOperator;
        if let SqlExpr::BinaryOp { left, op, right } = expr {
            if let (SqlExpr::Column(col), SqlExpr::Literal(val)) = (left.as_ref(), right.as_ref()) {
                let col_name = col.trim_matches('"');
                let col_name = if let Some(d) = col_name.rfind('.') {
                    &col_name[d + 1..]
                } else {
                    col_name
                };
                let op_str = match op {
                    BinaryOperator::Gt => ">",
                    BinaryOperator::Lt => "<",
                    BinaryOperator::Ge => ">=",
                    BinaryOperator::Le => "<=",
                    BinaryOperator::Eq => "=",
                    BinaryOperator::NotEq => "!=",
                    _ => return None,
                };
                let val_str = match val {
                    crate::data::Value::Int64(v) => v.to_string(),
                    crate::data::Value::Int32(v) => v.to_string(),
                    crate::data::Value::Float64(v) => v.to_string(),
                    crate::data::Value::Float32(v) => v.to_string(),
                    crate::data::Value::String(v) => format!("'{}'", v),
                    _ => return None,
                };
                return Some(format!("{}{}{}", col_name, op_str, val_str));
            }
        }
        None
    }

    fn count_star_output_name_for_table_fn(stmt: &SelectStatement) -> Option<String> {
        if stmt.columns.len() != 1
            || !stmt.group_by.is_empty()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || !stmt.order_by.is_empty()
            || stmt.limit.is_some()
            || stmt.offset.is_some()
        {
            return None;
        }

        match &stmt.columns[0] {
            SelectColumn::Aggregate {
                func,
                column,
                distinct,
                alias,
            } if matches!(func, AggregateFunc::Count) && !distinct => {
                let column_ok = column
                    .as_ref()
                    .map(|c| {
                        c == "*"
                            || c.chars()
                                .next()
                                .map(|ch| ch.is_ascii_digit())
                                .unwrap_or(false)
                    })
                    .unwrap_or(true);
                if column_ok {
                    Some(alias.clone().unwrap_or_else(|| "COUNT(*)".to_string()))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn try_fast_json_count_table_function(
        stmt: &SelectStatement,
        func: &str,
        file: &str,
    ) -> io::Result<Option<RecordBatch>> {
        if !func.eq_ignore_ascii_case("READ_JSON") {
            return Ok(None);
        }
        let Some(output_name) = Self::count_star_output_name_for_table_fn(stmt) else {
            return Ok(None);
        };
        let Some(count) = Self::try_fast_json_count(file, stmt.where_clause.as_ref())? else {
            return Ok(None);
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            &output_name,
            ArrowDataType::Int64,
            false,
        )]));
        let array: ArrayRef = Arc::new(Int64Array::from(vec![count]));
        RecordBatch::try_new(schema, vec![array])
            .map(Some)
            .map_err(|e| err_data(e.to_string()))
    }

    fn try_fast_parquet_count_table_function(
        stmt: &SelectStatement,
        func: &str,
        file: &str,
    ) -> io::Result<Option<RecordBatch>> {
        if !func.eq_ignore_ascii_case("READ_PARQUET") {
            return Ok(None);
        }
        let Some(output_name) = Self::count_star_output_name_for_table_fn(stmt) else {
            return Ok(None);
        };
        let filter = match stmt.where_clause.as_ref() {
            Some(expr) => match Self::try_extract_filter_for_pushdown(expr) {
                Some(filter) => Some(filter),
                None => return Ok(None),
            },
            None => None,
        };
        let Some(count) = Self::try_fast_parquet_count(file, filter.as_deref())? else {
            return Ok(None);
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            &output_name,
            ArrowDataType::Int64,
            false,
        )]));
        let array: ArrayRef = Arc::new(Int64Array::from(vec![count]));
        RecordBatch::try_new(schema, vec![array])
            .map(Some)
            .map_err(|e| err_data(e.to_string()))
    }

    /// Fast one-string-key GROUP BY with numeric aggregates over a CSV source.
    fn try_fast_csv_aggregation(
        stmt: &SelectStatement,
        file: &str,
        options: &[(String, String)],
    ) -> io::Result<Option<ApexResult>> {
        let has_aggregate = stmt
            .columns
            .iter()
            .any(|column| matches!(column, SelectColumn::Aggregate { .. }));
        if !has_aggregate {
            return Ok(None);
        }
        if !stmt.group_by.is_empty() {
            return Self::try_fast_csv_string_group_aggregation(stmt, file, options);
        }
        if stmt.columns.iter().all(|column| {
            matches!(
                column,
                SelectColumn::Aggregate {
                    distinct: true,
                    ..
                }
            )
        }) {
            return Self::try_fast_csv_distinct_aggregation(stmt, file, options);
        }
        Self::try_fast_csv_numeric_aggregation(stmt, file, options)
    }

    /// Fast one-string-key GROUP BY with numeric aggregates over a CSV source.
    fn try_fast_csv_string_group_aggregation(
        stmt: &SelectStatement,
        file: &str,
        options: &[(String, String)],
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        if stmt.group_by.len() != 1
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || !stmt.joins.is_empty()
            || stmt.offset.unwrap_or(0) != 0
            || stmt.limit == Some(0)
        {
            return Ok(None);
        }
        let clean_name = |name: &str| {
            let trimmed = name.trim_matches('"');
            trimmed
                .rsplit('.')
                .next()
                .unwrap_or(trimmed)
                .trim_matches('"')
                .to_string()
        };
        let predicates = match stmt.where_clause.as_ref() {
            Some(where_clause) => {
                let Some(predicates) = Self::extract_numeric_conjunction(where_clause) else {
                    return Ok(None);
                };
                predicates
            }
            None => Vec::new(),
        };
        if let Some(having) = &stmt.having {
            let mut having_aggregates = Vec::new();
            Self::walk_having_expr(having, &[], &mut having_aggregates);
            let selected = |having_func: &AggregateFunc, having_column: &Option<String>| {
                stmt.columns.iter().any(|column| {
                    let SelectColumn::Aggregate {
                        func,
                        column,
                        distinct,
                        ..
                    } = column
                    else {
                        return false;
                    };
                    if *distinct || func != having_func {
                        return false;
                    }
                    let selected_column = column
                        .as_deref()
                        .filter(|column| *column != "*")
                        .map(&clean_name);
                    let having_column = having_column.as_deref().map(&clean_name);
                    selected_column == having_column
                })
            };
            if having_aggregates
                .iter()
                .any(|(func, column)| !selected(func, column))
            {
                return Ok(None);
            }
        }
        let group_column = clean_name(&stmt.group_by[0]);
        let mut aggregate_columns = Vec::new();
        let mut group_output_name = None;
        for column in &stmt.columns {
            match column {
                SelectColumn::Column(name) if clean_name(name) == group_column => {
                    group_output_name = Some(group_column.clone());
                }
                SelectColumn::ColumnAlias { column, alias }
                    if clean_name(column) == group_column =>
                {
                    group_output_name = Some(alias.clone());
                }
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct,
                    ..
                } => {
                    if *distinct {
                        return Ok(None);
                    }
                    let is_count_star = matches!(func, AggregateFunc::Count)
                        && column.as_deref().map_or(true, |name| name == "*");
                    if !is_count_star {
                        let Some(column) = column else {
                            return Ok(None);
                        };
                        let name = clean_name(column);
                        if name != "*" && !aggregate_columns.contains(&name) {
                            aggregate_columns.push(name);
                        }
                    }
                }
                _ => return Ok(None),
            }
        }
        let Some(group_output_name) = group_output_name else {
            return Ok(None);
        };
        if aggregate_columns.is_empty() {
            return Ok(None);
        }
        let Some((group_key_type, groups)) = Self::try_fast_csv_group_numeric_agg(
            file,
            options,
            &group_column,
            &aggregate_columns,
            &predicates,
        )? else {
            return Ok(None);
        };

        let mut fields = Vec::with_capacity(stmt.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());
        for column in &stmt.columns {
            match column {
                SelectColumn::Column(_) | SelectColumn::ColumnAlias { .. } => {
                    match group_key_type {
                        coordination::CsvGroupKeyType::Utf8 => {
                            let values = groups
                                .iter()
                                .map(|(group, _, _)| {
                                    group
                                        .as_deref()
                                        .map(std::str::from_utf8)
                                        .transpose()
                                        .map(|value| value.map(str::to_owned))
                                })
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(|_| err_data("invalid UTF-8 group key"))?;
                            fields.push(Field::new(
                                &group_output_name,
                                ArrowDataType::Utf8,
                                true,
                            ));
                            arrays.push(Arc::new(StringArray::from(values)));
                        }
                        coordination::CsvGroupKeyType::Int64 => {
                            let values = groups
                                .iter()
                                .map(|(group, _, _)| {
                                    group.as_deref().map(|value| {
                                        let bytes: [u8; 8] = value.try_into().unwrap();
                                        i64::from_le_bytes(bytes)
                                    })
                                })
                                .collect::<Vec<_>>();
                            fields.push(Field::new(
                                &group_output_name,
                                ArrowDataType::Int64,
                                true,
                            ));
                            arrays.push(Arc::new(Int64Array::from(values)));
                        }
                    }
                }
                SelectColumn::Aggregate {
                    func,
                    column,
                    alias,
                    ..
                } => {
                    let source = column.as_deref().unwrap_or("*");
                    let output = alias.clone().unwrap_or_else(|| {
                        let name = match func {
                            AggregateFunc::Count => "COUNT",
                            AggregateFunc::Sum => "SUM",
                            AggregateFunc::Avg => "AVG",
                            AggregateFunc::Min => "MIN",
                            AggregateFunc::Max => "MAX",
                        };
                        format!("{name}({source})")
                    });
                    let is_count_star = matches!(func, AggregateFunc::Count) && source == "*";
                    let stat_index = if is_count_star {
                        None
                    } else {
                        let source = clean_name(source);
                        aggregate_columns.iter().position(|name| name == &source)
                    };
                    match func {
                        AggregateFunc::Count => {
                            fields.push(Field::new(&output, ArrowDataType::Int64, false));
                            arrays.push(Arc::new(Int64Array::from(
                                groups
                                    .iter()
                                    .map(|(_, rows, stats)| {
                                        stat_index.map_or(*rows, |index| stats[index].count)
                                    })
                                    .collect::<Vec<_>>(),
                            )));
                        }
                        AggregateFunc::Avg => {
                            let index = stat_index.unwrap();
                            fields.push(Field::new(&output, ArrowDataType::Float64, true));
                            arrays.push(Arc::new(Float64Array::from(
                                groups
                                    .iter()
                                    .map(|(_, _, stats)| {
                                        let stat = stats[index];
                                        (stat.count > 0).then_some(stat.sum / stat.count as f64)
                                    })
                                    .collect::<Vec<_>>(),
                            )));
                        }
                        AggregateFunc::Sum | AggregateFunc::Min | AggregateFunc::Max => {
                            let index = stat_index.unwrap();
                            let is_int = groups
                                .first()
                                .map(|(_, _, stats)| stats[index].is_int)
                                .unwrap_or(false);
                            if is_int {
                                fields.push(Field::new(&output, ArrowDataType::Int64, true));
                                arrays.push(Arc::new(Int64Array::from(
                                    groups
                                        .iter()
                                        .map(|(_, _, stats)| {
                                            let stat = stats[index];
                                            (stat.count > 0).then_some(match func {
                                                AggregateFunc::Sum => stat.sum as i64,
                                                AggregateFunc::Min => stat.min as i64,
                                                AggregateFunc::Max => stat.max as i64,
                                                _ => unreachable!(),
                                            })
                                        })
                                        .collect::<Vec<_>>(),
                                )));
                            } else {
                                fields.push(Field::new(&output, ArrowDataType::Float64, true));
                                arrays.push(Arc::new(Float64Array::from(
                                    groups
                                        .iter()
                                        .map(|(_, _, stats)| {
                                            let stat = stats[index];
                                            (stat.count > 0).then_some(match func {
                                                AggregateFunc::Sum => stat.sum,
                                                AggregateFunc::Min => stat.min,
                                                AggregateFunc::Max => stat.max,
                                                _ => unreachable!(),
                                            })
                                        })
                                        .collect::<Vec<_>>(),
                                )));
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
        let mut batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| err_data(error.to_string()))?;
        if let Some(having) = &stmt.having {
            let mask = Self::evaluate_predicate(&batch, having)?;
            batch = arrow::compute::filter_record_batch(&batch, &mask)
                .map_err(|error| err_data(error.to_string()))?;
            if batch.num_rows() == 0 {
                return Ok(Some(ApexResult::Empty(batch.schema())));
            }
        }
        if !stmt.order_by.is_empty() {
            batch = Self::apply_order_by_topk(&batch, &stmt.order_by, stmt.limit)?;
        } else if let Some(limit) = stmt.limit {
            batch = batch.slice(0, limit.min(batch.num_rows()));
        }
        Ok(Some(ApexResult::Data(batch)))
    }

    /// Fast scalar numeric aggregation over a CSV source.
    ///
    /// A normal CSV SELECT first materialises every requested column into an
    /// Arrow batch and only then aggregates it.  For a scalar aggregate with no
    /// relational clauses, a single pass over the referenced fields is enough.
    /// This keeps wide CSV files (such as network-flow exports) bounded to the
    /// requested scalar state instead of allocating one array per input column.
    fn try_fast_csv_numeric_aggregation(
        stmt: &SelectStatement,
        file: &str,
        options: &[(String, String)],
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        if !stmt.group_by.is_empty()
            || !stmt.group_by_exprs.is_empty()
            || stmt.having.is_some()
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || !stmt.joins.is_empty()
            || !stmt.order_by.is_empty()
            || stmt.offset.unwrap_or(0) != 0
            || stmt.limit == Some(0)
        {
            return Ok(None);
        }
        let predicates = match stmt.where_clause.as_ref() {
            Some(where_clause) => {
                let Some(predicates) = Self::extract_numeric_conjunction(where_clause) else {
                    return Ok(None);
                };
                predicates
            }
            None => Vec::new(),
        };

        fn clean_column_name(name: &str) -> String {
            let trimmed = name.trim_matches('"');
            trimmed
                .rsplit('.')
                .next()
                .unwrap_or(trimmed)
                .trim_matches('"')
                .to_string()
        }

        let mut aggregate_columns = Vec::new();
        for column in &stmt.columns {
            let SelectColumn::Aggregate {
                func,
                column,
                distinct,
                ..
            } = column
            else {
                return Ok(None);
            };
            if *distinct {
                return Ok(None);
            }
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
                        let name = clean_column_name(column.as_ref().unwrap());
                        if name.is_empty() || !aggregate_columns.contains(&name) {
                            aggregate_columns.push(name);
                        }
                    }
                }
                AggregateFunc::Sum
                | AggregateFunc::Avg
                | AggregateFunc::Min
                | AggregateFunc::Max => {
                    let Some(column) = column else {
                        return Ok(None);
                    };
                    if column == "*" {
                        return Ok(None);
                    }
                    let name = clean_column_name(column);
                    if name.is_empty() {
                        return Ok(None);
                    }
                    if !aggregate_columns.contains(&name) {
                        aggregate_columns.push(name);
                    }
                }
            }
        }
        // COUNT(*) alone is already covered by the cheaper row-boundary path.
        // Keep this path for one or more actual numeric columns, including a
        // mixed COUNT(*) + MAX/SUM/AVG/MIN query.
        if aggregate_columns.is_empty() {
            // A pure `COUNT(*)` filtered by numeric predicates is fully supported: the kernel
            // scans the predicate columns and reports how many rows matched. Declining here
            // sent the query to the whole-file reader, which pinned the entire CSV in memory
            // (20M rows / 643 MB: +1397 MB RSS under a 256 MB cap).
            if predicates.is_empty() {
                return Ok(None);
            }
            for (column, _, _) in &predicates {
                if !aggregate_columns.contains(column) {
                    aggregate_columns.push(column.clone());
                }
            }
        }

        let Some((row_count, stats)) =
            Self::try_fast_csv_numeric_agg(file, options, &aggregate_columns, &predicates)?
        else {
            return Ok(None);
        };

        let mut fields = Vec::with_capacity(stmt.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());
        for column in &stmt.columns {
            let SelectColumn::Aggregate {
                func,
                column,
                alias,
                ..
            } = column
            else {
                unreachable!();
            };
            let source_name = column.as_deref().unwrap_or("*");
            let output_name = alias.clone().unwrap_or_else(|| {
                let fn_name = match func {
                    AggregateFunc::Count => "COUNT",
                    AggregateFunc::Sum => "SUM",
                    AggregateFunc::Avg => "AVG",
                    AggregateFunc::Min => "MIN",
                    AggregateFunc::Max => "MAX",
                };
                format!("{}({})", fn_name, source_name)
            });

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
                    let count = if is_count_star {
                        row_count
                    } else {
                        let source_name = clean_column_name(column.as_ref().unwrap());
                        let index = aggregate_columns
                            .iter()
                            .position(|name| name == &source_name)
                            .unwrap();
                        stats[index].count
                    };
                    fields.push(Field::new(&output_name, ArrowDataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(vec![count])));
                }
                AggregateFunc::Sum => {
                    let source_name = clean_column_name(column.as_ref().unwrap());
                    let index = aggregate_columns
                        .iter()
                        .position(|name| name == &source_name)
                        .unwrap();
                    let stat = stats[index];
                    if stat.is_int {
                        fields.push(Field::new(&output_name, ArrowDataType::Int64, false));
                        arrays.push(Arc::new(Int64Array::from(vec![stat.sum as i64])));
                    } else {
                        fields.push(Field::new(&output_name, ArrowDataType::Float64, false));
                        arrays.push(Arc::new(Float64Array::from(vec![stat.sum])));
                    }
                }
                AggregateFunc::Avg => {
                    let source_name = clean_column_name(column.as_ref().unwrap());
                    let index = aggregate_columns
                        .iter()
                        .position(|name| name == &source_name)
                        .unwrap();
                    let stat = stats[index];
                    let value = if stat.count > 0 {
                        stat.sum / stat.count as f64
                    } else {
                        0.0
                    };
                    fields.push(Field::new(&output_name, ArrowDataType::Float64, false));
                    arrays.push(Arc::new(Float64Array::from(vec![value])));
                }
                AggregateFunc::Min | AggregateFunc::Max => {
                    let source_name = clean_column_name(column.as_ref().unwrap());
                    let index = aggregate_columns
                        .iter()
                        .position(|name| name == &source_name)
                        .unwrap();
                    let stat = stats[index];
                    let value = if matches!(func, AggregateFunc::Min) {
                        stat.min
                    } else {
                        stat.max
                    };
                    if stat.is_int {
                        fields.push(Field::new(&output_name, ArrowDataType::Int64, true));
                        let value = (stat.count > 0).then_some(value as i64);
                        arrays.push(Arc::new(Int64Array::from(vec![value])));
                    } else {
                        fields.push(Field::new(&output_name, ArrowDataType::Float64, true));
                        let value = (stat.count > 0).then_some(value);
                        arrays.push(Arc::new(Float64Array::from(vec![value])));
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;
        Ok(Some(ApexResult::Data(batch)))
    }

    /// Fast scalar COUNT(DISTINCT col) profile over a direct CSV source.
    fn try_fast_csv_distinct_aggregation(
        stmt: &SelectStatement,
        file: &str,
        options: &[(String, String)],
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;

        if !stmt.group_by.is_empty()
            || !stmt.group_by_exprs.is_empty()
            || stmt.where_clause.is_some()
            || stmt.having.is_some()
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || !stmt.joins.is_empty()
            || !stmt.order_by.is_empty()
            || stmt.offset.unwrap_or(0) != 0
            || stmt.limit == Some(0)
        {
            return Ok(None);
        }
        let clean = |name: &str| {
            let trimmed = name.trim_matches('"');
            trimmed
                .rsplit('.')
                .next()
                .unwrap_or(trimmed)
                .trim_matches('"')
                .to_string()
        };
        let mut columns = Vec::with_capacity(stmt.columns.len());
        let mut names = Vec::with_capacity(stmt.columns.len());
        for select in &stmt.columns {
            let SelectColumn::Aggregate {
                func: AggregateFunc::Count,
                column: Some(column),
                distinct: true,
                alias,
            } = select
            else {
                return Ok(None);
            };
            if column == "*" {
                return Ok(None);
            }
            columns.push(clean(column));
            names.push(
                alias
                    .clone()
                    .unwrap_or_else(|| format!("COUNT(DISTINCT {})", column)),
            );
        }
        let Some(counts) = Self::try_fast_csv_distinct_counts(file, options, &columns)? else {
            return Ok(None);
        };
        let fields = names
            .iter()
            .map(|name| Field::new(name, ArrowDataType::Int64, false))
            .collect::<Vec<_>>();
        let arrays = counts
            .into_iter()
            .map(|count| Arc::new(Int64Array::from(vec![count])) as ArrayRef)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| err_data(error.to_string()))?;
        Ok(Some(ApexResult::Data(batch)))
    }

    /// Fast `COUNT(*)`/`COUNT(1)` over a `READ_CSV(...)` table function.
    /// Allows a non-trivial `LIMIT`/`OFFSET` only when it is a no-op on the
    /// single-row COUNT result (no offset, limit == 0 disallowed).
    fn try_fast_csv_count_table_function(
        stmt: &SelectStatement,
        func: &str,
        file: &str,
        options: &[(String, String)],
    ) -> io::Result<Option<RecordBatch>> {
        if !func.eq_ignore_ascii_case("READ_CSV") || stmt.where_clause.is_some() {
            return Ok(None);
        }
        let Some(output_name) = Self::count_output_name_allowing_limit(stmt) else {
            return Ok(None);
        };
        let Some(count) = Self::try_fast_csv_count(file, options)? else {
            return Ok(None);
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            &output_name,
            ArrowDataType::Int64,
            false,
        )]));
        let array: ArrayRef = Arc::new(Int64Array::from(vec![count]));
        RecordBatch::try_new(schema, vec![array])
            .map(Some)
            .map_err(|e| err_data(e.to_string()))
    }

    /// Fast `COUNT(*)`/`COUNT(1)` over a DuckDB-style direct file `'path.csv'`.
    fn try_fast_csv_count_direct_file(
        stmt: &SelectStatement,
        file: &str,
    ) -> io::Result<Option<RecordBatch>> {
        if stmt.where_clause.is_some() {
            return Ok(None);
        }
        let Some(output_name) = Self::count_output_name_allowing_limit(stmt) else {
            return Ok(None);
        };
        let lower = file.to_lowercase();
        let count = if lower.ends_with(".csv") {
            Self::try_fast_csv_count(file, &[])?
        } else if lower.ends_with(".tsv") {
            Self::try_fast_csv_count(file, &[("delimiter".to_string(), "\t".to_string())])?
        } else {
            return Ok(None);
        };
        let Some(count) = count else {
            return Ok(None);
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            &output_name,
            ArrowDataType::Int64,
            false,
        )]));
        let array: ArrayRef = Arc::new(Int64Array::from(vec![count]));
        RecordBatch::try_new(schema, vec![array])
            .map(Some)
            .map_err(|e| err_data(e.to_string()))
    }

    /// Like `count_star_output_name_for_table_fn`, but permits a `LIMIT>=1`
    /// with no `OFFSET` — a no-op on the single-row COUNT result — so the fast
    /// CSV count still applies to `SELECT COUNT(*) FROM file LIMIT 100`.
    fn count_output_name_allowing_limit(stmt: &SelectStatement) -> Option<String> {
        if !stmt.group_by.is_empty()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || !stmt.order_by.is_empty()
            || stmt.limit == Some(0)
            || stmt.offset.unwrap_or(0) != 0
        {
            return None;
        }
        match stmt.columns.len() {
            1 => match &stmt.columns[0] {
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct,
                    alias,
                } if matches!(func, AggregateFunc::Count) && !distinct => {
                    let column_ok = column
                        .as_ref()
                        .map(|c| {
                            c == "*"
                                || c.chars()
                                    .next()
                                    .map(|ch| ch.is_ascii_digit())
                                    .unwrap_or(false)
                        })
                        .unwrap_or(true);
                    if column_ok {
                        Some(alias.clone().unwrap_or_else(|| "COUNT(*)".to_string()))
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        }
    }
}
