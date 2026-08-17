use super::*;

impl ApexExecutor {
    pub(in crate::query::executor) fn execute_group_by(
        batch: &RecordBatch,
        stmt: &SelectStatement,
    ) -> io::Result<ApexResult> {
        if stmt.group_by.is_empty() {
            return Err(err_input("GROUP BY requires at least one column"));
        }

        // Materialize expression-based GROUP BY columns (e.g., YEAR(date), MONTH(ts))
        let batch = Self::materialize_group_by_exprs(batch, stmt)?;

        // Build group keys - strip table prefix if present (e.g., "u.tier" -> "tier")
        let group_cols: Vec<String> = stmt
            .group_by
            .iter()
            .map(|s| {
                let trimmed = s.trim_matches('"');
                if let Some(dot_pos) = trimmed.rfind('.') {
                    trimmed[dot_pos + 1..].to_string()
                } else {
                    trimmed.to_string()
                }
            })
            .collect();

        // If HAVING references aggregates that are not in the SELECT list (e.g.
        // "SELECT city … HAVING COUNT(*) > 1"), inject them as extra SELECT columns
        // so every GROUP-BY sub-path can materialise the value for filter evaluation.
        // We strip the extra columns from the final result after HAVING is applied.
        let select_col_count = stmt.columns.len();
        let extra_agg_count;
        let owned_stmt: SelectStatement;
        let effective_stmt: &SelectStatement;
        if let Some(having_expr) = &stmt.having {
            let extras = Self::collect_having_extra_aggs(having_expr, &stmt.columns);
            if !extras.is_empty() {
                extra_agg_count = extras.len();
                let mut s = stmt.clone();
                for (func, col) in extras {
                    use crate::query::AggregateFunc;
                    let fn_name = match func {
                        AggregateFunc::Count => "COUNT",
                        AggregateFunc::Sum => "SUM",
                        AggregateFunc::Avg => "AVG",
                        AggregateFunc::Min => "MIN",
                        AggregateFunc::Max => "MAX",
                    };
                    let alias = format!("{}({})", fn_name, col.as_deref().unwrap_or("*"));
                    s.columns.push(crate::query::SelectColumn::Aggregate {
                        func,
                        column: col,
                        distinct: false,
                        alias: Some(alias),
                    });
                }
                owned_stmt = s;
                effective_stmt = &owned_stmt;
            } else {
                extra_agg_count = 0;
                effective_stmt = stmt;
                owned_stmt = stmt.clone(); // unused but required for lifetime
            }
        } else {
            extra_agg_count = 0;
            effective_stmt = stmt;
            owned_stmt = stmt.clone(); // unused but required for lifetime
        }

        if let Some(result) =
            Self::try_execute_single_key_streaming_group_by(&batch, effective_stmt, &group_cols)?
        {
            return Ok(Self::decode_group_by_dict_columns(result));
        }

        // Single dict-encoded group key + COUNT(CASE WHEN cond THEN 1 END):
        // count in one direct-indexed pass, avoiding the hash-based group
        // construction and per-group row-index vectors.
        if let Some(result) = Self::try_execute_dict_case_count(&batch, effective_stmt, &group_cols)?
        {
            return Ok(result);
        }

        // Check if we can use fast path: only simple aggregates (COUNT, SUM, AVG, MIN, MAX)
        // without DISTINCT, expressions, or HAVING that needs row access
        let can_use_incremental = Self::can_use_incremental_aggregation(effective_stmt);

        let mut result = if can_use_incremental {
            // Try vectorized execution for single-column GROUP BY
            if group_cols.len() == 1 {
                if let Ok(r) =
                    Self::execute_group_by_vectorized(&batch, effective_stmt, &group_cols[0])
                {
                    r
                } else {
                    Self::execute_group_by_incremental(&batch, effective_stmt, &group_cols)?
                }
            } else {
                Self::execute_group_by_incremental(&batch, effective_stmt, &group_cols)?
            }
        } else {
            // Fall back to full row-index based aggregation for complex cases
            Self::execute_group_by_with_indices(&batch, effective_stmt, &group_cols)?
        };
        result = Self::decode_group_by_dict_columns(result);

        // Strip the extra HAVING-only aggregate columns we injected above
        if extra_agg_count > 0 {
            if let ApexResult::Data(ref rb) = result {
                let keep = select_col_count.min(rb.num_columns());
                let new_schema = Arc::new(Schema::new(
                    rb.schema().fields()[..keep]
                        .iter()
                        .map(|f| f.as_ref().clone())
                        .collect::<Vec<_>>(),
                ));
                let new_arrays: Vec<ArrayRef> = (0..keep).map(|i| rb.column(i).clone()).collect();
                match RecordBatch::try_new(new_schema, new_arrays) {
                    Ok(trimmed) => result = ApexResult::Data(trimmed),
                    Err(e) => return Err(err_data(e.to_string())),
                }
            }
        }

        Ok(result)
    }

    /// Grouped results may carry dictionary-encoded string columns (the input
    /// batch is dict-encoded for low-cardinality keys).  Decode them so the
    /// Python bindings receive plain strings.
    fn decode_group_by_dict_columns(result: ApexResult) -> ApexResult {
        if let ApexResult::Data(batch) = result {
            ApexResult::Data(Self::decode_dict_columns(&batch))
        } else {
            result
        }
    }

    pub(in crate::query::executor) fn try_execute_single_key_streaming_group_by(
        batch: &RecordBatch,
        stmt: &SelectStatement,
        group_cols: &[String],
    ) -> io::Result<Option<ApexResult>> {
        if group_cols.len() != 1
            || stmt.having.is_some()
            || !stmt.order_by.is_empty()
            || stmt.limit.is_some()
            || stmt.offset.is_some()
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

        fn numeric_value(array: &ArrayRef, row: usize) -> Option<f64> {
            if array.is_null(row) {
                return None;
            }
            if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
                Some(values.value(row))
            } else if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
                Some(values.value(row) as f64)
            } else if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
                Some(values.value(row) as f64)
            } else if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
                Some(if values.value(row) { 1.0 } else { 0.0 })
            } else {
                None
            }
        }

        fn numeric_literal(expr: &SqlExpr) -> Option<f64> {
            match expr {
                SqlExpr::Literal(Value::Int64(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Float64(value)) => Some(*value),
                _ => None,
            }
        }

        fn output_name(
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
                (AggregateFunc::Sum, Some(column)) => format!("SUM({})", clean_name(column)),
                (AggregateFunc::Avg, Some(column)) => format!("AVG({})", clean_name(column)),
                (AggregateFunc::Min, Some(column)) => format!("MIN({})", clean_name(column)),
                (AggregateFunc::Max, Some(column)) => format!("MAX({})", clean_name(column)),
                _ => "aggregate".to_string(),
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

        fn sum_binary(expr: &SqlExpr) -> Option<(String, String, BinaryOperator)> {
            let SqlExpr::Function { name, args } = expr else {
                return None;
            };
            if !name.eq_ignore_ascii_case("SUM") || args.len() != 1 {
                return None;
            }
            let SqlExpr::BinaryOp { left, op, right } = &args[0] else {
                return None;
            };
            if !matches!(op, BinaryOperator::Add | BinaryOperator::Sub) {
                return None;
            }
            let (SqlExpr::Column(left), SqlExpr::Column(right)) = (left.as_ref(), right.as_ref())
            else {
                return None;
            };
            Some((
                clean_name(left).to_string(),
                clean_name(right).to_string(),
                op.clone(),
            ))
        }

        #[inline]
        fn string_fingerprint(value: &str) -> u64 {
            let bytes = value.as_bytes();
            let mut split = 0usize;
            while split < bytes.len() && !bytes[split].is_ascii_digit() {
                split += 1;
            }
            if split > 0 && split < bytes.len() {
                let mut number = 0u64;
                let mut valid = true;
                for &byte in &bytes[split..] {
                    if !byte.is_ascii_digit() {
                        valid = false;
                        break;
                    }
                    number = number
                        .saturating_mul(10)
                        .saturating_add((byte - b'0') as u64);
                }
                if valid {
                    let mut prefix = 0u64;
                    for &byte in &bytes[..split.min(8)] {
                        prefix = (prefix << 8) | byte as u64;
                    }
                    return 0xA5A5_5A5A_D3C3_B4B4u64
                        ^ number
                        ^ prefix.rotate_left(27)
                        ^ ((split as u64) << 56);
                }
            }

            let mut hasher = AHasher::default();
            value.hash(&mut hasher);
            hasher.finish()
        }

        enum Output {
            Key(String),
            Count {
                name: String,
                slot: usize,
            },
            Distinct {
                name: String,
                slot: usize,
            },
            Sum {
                name: String,
                slot: usize,
            },
            Avg {
                name: String,
                sum_slot: usize,
                count_slot: usize,
            },
        }

        enum RowOp<'a> {
            Count {
                slot: usize,
            },
            CountNonNull {
                slot: usize,
                array: &'a ArrayRef,
            },
            DistinctString {
                slot: usize,
                array: &'a StringArray,
            },
            DistinctGeneric {
                slot: usize,
                array: &'a ArrayRef,
            },
            SumColumn {
                slot: usize,
                array: &'a ArrayRef,
            },
            SumBinary {
                slot: usize,
                left: &'a ArrayRef,
                right: &'a ArrayRef,
                op: BinaryOperator,
            },
            SumCases {
                array: &'a StringArray,
                cases: Vec<(usize, Vec<String>)>,
            },
            AvgColumn {
                sum_slot: usize,
                count_slot: usize,
                array: &'a ArrayRef,
            },
        }

        struct State {
            key: String,
            counts: Vec<i64>,
            sums: Vec<f64>,
            sum_counts: Vec<i64>,
            avg_counts: Vec<i64>,
            distinct: Vec<Vec<u64>>,
        }

        let group_col = clean_name(&group_cols[0]);
        let Some(group_values) = batch
            .column_by_name(group_col)
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        else {
            return Ok(None);
        };
        if group_values.null_count() > 0 {
            return Ok(None);
        }

        let mut outputs = Vec::with_capacity(stmt.columns.len());
        let mut row_ops = Vec::new();
        let mut count_slots = 0usize;
        let mut sum_slots = 0usize;
        let mut avg_count_slots = 0usize;
        let mut distinct_slots = 0usize;

        for column in &stmt.columns {
            match column {
                SelectColumn::Column(name) => {
                    if clean_name(name) != group_col {
                        return Ok(None);
                    }
                    outputs.push(Output::Key(clean_name(name).to_string()));
                }
                SelectColumn::ColumnAlias { column, alias } => {
                    if clean_name(column) != group_col {
                        return Ok(None);
                    }
                    outputs.push(Output::Key(alias.clone()));
                }
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct,
                    alias,
                } => match (func, column, distinct) {
                    (AggregateFunc::Count, None, false) => {
                        let slot = count_slots;
                        count_slots += 1;
                        row_ops.push(RowOp::Count { slot });
                        outputs.push(Output::Count {
                            name: output_name(func, column.as_ref(), alias),
                            slot,
                        });
                    }
                    (AggregateFunc::Count, Some(column), false) => {
                        let actual = clean_name(column);
                        let Some(array) = batch.column_by_name(actual) else {
                            return Ok(None);
                        };
                        let slot = count_slots;
                        count_slots += 1;
                        row_ops.push(RowOp::CountNonNull { slot, array });
                        outputs.push(Output::Count {
                            name: output_name(func, Some(column), alias),
                            slot,
                        });
                    }
                    (AggregateFunc::Count, Some(column), true) => {
                        let actual = clean_name(column);
                        let Some(array) = batch.column_by_name(actual) else {
                            return Ok(None);
                        };
                        let slot = distinct_slots;
                        distinct_slots += 1;
                        if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
                            row_ops.push(RowOp::DistinctString {
                                slot,
                                array: strings,
                            });
                        } else {
                            row_ops.push(RowOp::DistinctGeneric { slot, array });
                        }
                        outputs.push(Output::Distinct {
                            name: alias
                                .clone()
                                .unwrap_or_else(|| format!("COUNT(DISTINCT {})", actual)),
                            slot,
                        });
                    }
                    (AggregateFunc::Sum, Some(column), false) => {
                        let actual = clean_name(column);
                        let Some(array) = batch.column_by_name(actual) else {
                            return Ok(None);
                        };
                        let slot = sum_slots;
                        sum_slots += 1;
                        row_ops.push(RowOp::SumColumn { slot, array });
                        outputs.push(Output::Sum {
                            name: output_name(func, Some(column), alias),
                            slot,
                        });
                    }
                    (AggregateFunc::Avg, Some(column), false) => {
                        let actual = clean_name(column);
                        let Some(array) = batch.column_by_name(actual) else {
                            return Ok(None);
                        };
                        let sum_slot = sum_slots;
                        let count_slot = avg_count_slots;
                        sum_slots += 1;
                        avg_count_slots += 1;
                        row_ops.push(RowOp::AvgColumn {
                            sum_slot,
                            count_slot,
                            array,
                        });
                        outputs.push(Output::Avg {
                            name: output_name(func, Some(column), alias),
                            sum_slot,
                            count_slot,
                        });
                    }
                    _ => return Ok(None),
                },
                SelectColumn::Expression { expr, alias } => {
                    let Some(alias) = alias.clone() else {
                        return Ok(None);
                    };
                    if let Some((column, literals)) = case_counter(expr) {
                        let Some(array) = batch
                            .column_by_name(&column)
                            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
                        else {
                            return Ok(None);
                        };
                        let slot = sum_slots;
                        sum_slots += 1;
                        let mut merged = false;
                        for op in &mut row_ops {
                            if let RowOp::SumCases {
                                array: existing,
                                cases,
                            } = op
                            {
                                if std::ptr::eq(*existing, array) {
                                    cases.push((slot, literals.clone()));
                                    merged = true;
                                    break;
                                }
                            }
                        }
                        if !merged {
                            row_ops.push(RowOp::SumCases {
                                array,
                                cases: vec![(slot, literals)],
                            });
                        }
                        outputs.push(Output::Sum { name: alias, slot });
                    } else if let Some((left, right, op)) = sum_binary(expr) {
                        let Some(left_array) = batch.column_by_name(&left) else {
                            return Ok(None);
                        };
                        let Some(right_array) = batch.column_by_name(&right) else {
                            return Ok(None);
                        };
                        let slot = sum_slots;
                        sum_slots += 1;
                        row_ops.push(RowOp::SumBinary {
                            slot,
                            left: left_array,
                            right: right_array,
                            op,
                        });
                        outputs.push(Output::Sum { name: alias, slot });
                    } else {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            }
        }

        if row_ops.is_empty() {
            return Ok(None);
        }
        if distinct_slots == 0 {
            return Ok(None);
        }

        let mut apply_row_ops = |row: usize, state: &mut State| {
            for op in &row_ops {
                match op {
                    RowOp::Count { slot } => {
                        state.counts[*slot] += 1;
                    }
                    RowOp::CountNonNull { slot, array } => {
                        if !array.is_null(row) {
                            state.counts[*slot] += 1;
                        }
                    }
                    RowOp::DistinctString { slot, array } => {
                        if !array.is_null(row) {
                            let value = string_fingerprint(array.value(row));
                            let seen = &mut state.distinct[*slot];
                            if !seen.contains(&value) {
                                seen.push(value);
                            }
                        }
                    }
                    RowOp::DistinctGeneric { slot, array } => {
                        if !array.is_null(row) {
                            let value = Self::hash_array_value_fast(array, row);
                            let seen = &mut state.distinct[*slot];
                            if !seen.contains(&value) {
                                seen.push(value);
                            }
                        }
                    }
                    RowOp::SumColumn { slot, array } => {
                        if let Some(value) = numeric_value(array, row) {
                            state.sums[*slot] += value;
                            state.sum_counts[*slot] += 1;
                        }
                    }
                    RowOp::SumBinary {
                        slot,
                        left,
                        right,
                        op,
                    } => {
                        if let (Some(left), Some(right)) =
                            (numeric_value(left, row), numeric_value(right, row))
                        {
                            state.sums[*slot] += match op {
                                BinaryOperator::Add => left + right,
                                BinaryOperator::Sub => left - right,
                                _ => unreachable!(),
                            };
                            state.sum_counts[*slot] += 1;
                        }
                    }
                    RowOp::SumCases { array, cases } => {
                        if !array.is_null(row) {
                            let value = array.value(row);
                            for (slot, literals) in cases {
                                if literals.iter().any(|literal| literal == value) {
                                    state.sums[*slot] += 1.0;
                                }
                            }
                        }
                        for (slot, _) in cases {
                            state.sum_counts[*slot] += 1;
                        }
                    }
                    RowOp::AvgColumn {
                        sum_slot,
                        count_slot,
                        array,
                    } => {
                        if let Some(value) = numeric_value(array, row) {
                            state.sums[*sum_slot] += value;
                            state.avg_counts[*count_slot] += 1;
                        }
                    }
                }
            }
        };

        let estimated_groups = (batch.num_rows() / 100).clamp(16, batch.num_rows().max(16));
        let mut states = Vec::with_capacity(estimated_groups);
        let mut group_index: AHashMap<&str, usize> = AHashMap::with_capacity(estimated_groups);
        for row in 0..batch.num_rows() {
            let key = group_values.value(row);
            let group = if let Some(group) = group_index.get(key) {
                *group
            } else {
                let group = states.len();
                group_index.insert(key, group);
                states.push(State {
                    key: key.to_string(),
                    counts: vec![0; count_slots],
                    sums: vec![0.0; sum_slots],
                    sum_counts: vec![0; sum_slots],
                    avg_counts: vec![0; avg_count_slots],
                    distinct: (0..distinct_slots).map(|_| Vec::with_capacity(4)).collect(),
                });
                group
            };
            let state = unsafe { states.get_unchecked_mut(group) };
            apply_row_ops(row, state);
        }

        let mut fields = Vec::with_capacity(outputs.len());
        let mut arrays = Vec::with_capacity(outputs.len());
        for output in outputs {
            match output {
                Output::Key(name) => {
                    fields.push(Field::new(&name, ArrowDataType::Utf8, false));
                    arrays.push(Arc::new(StringArray::from(
                        states
                            .iter()
                            .map(|state| state.key.as_str())
                            .collect::<Vec<_>>(),
                    )) as ArrayRef);
                }
                Output::Count { name, slot } => {
                    fields.push(Field::new(&name, ArrowDataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(
                        states
                            .iter()
                            .map(|state| state.counts[slot])
                            .collect::<Vec<_>>(),
                    )) as ArrayRef);
                }
                Output::Distinct { name, slot } => {
                    fields.push(Field::new(&name, ArrowDataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(
                        states
                            .iter()
                            .map(|state| state.distinct[slot].len() as i64)
                            .collect::<Vec<_>>(),
                    )) as ArrayRef);
                }
                Output::Sum { name, slot } => {
                    fields.push(Field::new(&name, ArrowDataType::Float64, true));
                    arrays.push(Arc::new(Float64Array::from(
                        states
                            .iter()
                            .map(|state| {
                                if state.sum_counts[slot] == 0 {
                                    None
                                } else {
                                    Some(state.sums[slot])
                                }
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef);
                }
                Output::Avg {
                    name,
                    sum_slot,
                    count_slot,
                } => {
                    fields.push(Field::new(&name, ArrowDataType::Float64, true));
                    arrays.push(Arc::new(Float64Array::from(
                        states
                            .iter()
                            .map(|state| {
                                let count = state.avg_counts[count_slot];
                                if count == 0 {
                                    None
                                } else {
                                    Some(state.sums[sum_slot] / count as f64)
                                }
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef);
                }
            }
        }

        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| err_data(e.to_string()))?;
        Ok(Some(ApexResult::Data(batch)))
    }

    pub(in crate::query::executor) fn materialize_group_by_exprs(
        batch: &RecordBatch,
        stmt: &SelectStatement,
    ) -> io::Result<RecordBatch> {
        let mut fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let mut arrays: Vec<ArrayRef> = (0..batch.num_columns())
            .map(|i| batch.column(i).clone())
            .collect();
        let mut added_any = false;

        // 1. Evaluate explicit expression-based GROUP BY columns (e.g., YEAR(date))
        for (i, expr_opt) in stmt.group_by_exprs.iter().enumerate() {
            if let Some(expr) = expr_opt {
                let col_name = &stmt.group_by[i];
                if batch.column_by_name(col_name).is_some() {
                    continue;
                }
                let result_array = Self::evaluate_expr_to_array(batch, expr)?;
                let dt = result_array.data_type().clone();
                fields.push(Field::new(col_name, dt, true));
                arrays.push(result_array);
                added_any = true;
            }
        }

        // 2. Resolve GROUP BY aliases: if a GROUP BY name is not in the batch but
        //    matches a SELECT alias, evaluate that SELECT expression.
        for gb_name in &stmt.group_by {
            let trimmed = gb_name.trim_matches('"');
            if batch.column_by_name(trimmed).is_some() {
                continue;
            }
            // Already added above?
            if fields.iter().any(|f| f.name() == trimmed) {
                continue;
            }
            // Look for a SELECT column with this alias
            for sel_col in &stmt.columns {
                match sel_col {
                    SelectColumn::Expression {
                        expr,
                        alias: Some(alias),
                    } if alias == trimmed => {
                        let result_array = Self::evaluate_expr_to_array(batch, expr)?;
                        let dt = result_array.data_type().clone();
                        fields.push(Field::new(trimmed, dt, true));
                        arrays.push(result_array);
                        added_any = true;
                        break;
                    }
                    SelectColumn::Aggregate {
                        alias: Some(alias), ..
                    } if alias == trimmed => {
                        // GROUP BY on an aggregate alias is not meaningful, skip
                        break;
                    }
                    _ => {}
                }
            }
        }

        if !added_any {
            return Ok(batch.clone());
        }
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    pub(in crate::query::executor) fn execute_group_by_vectorized(
        batch: &RecordBatch,
        stmt: &SelectStatement,
        group_col_name: &str,
    ) -> io::Result<ApexResult> {
        use crate::query::vectorized::{execute_vectorized_group_by, VectorizedHashAgg};
        use crate::query::AggregateFunc;

        // If group column has NULLs, fall back to incremental path which handles null keys
        if let Some(col) = batch.column_by_name(group_col_name) {
            if col.null_count() > 0 {
                return Err(io::Error::new(io::ErrorKind::Unsupported, "null group key"));
            }
        }

        // OPTIMIZATION: For DictionaryArray columns, use direct indexing (much faster)
        if let Some(col) = batch.column_by_name(group_col_name) {
            use arrow::array::DictionaryArray;
            use arrow::datatypes::UInt32Type;
            if let Some(dict_arr) = col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
                let keys = dict_arr.keys();
                let values = dict_arr.values();
                if let Some(str_values) = values.as_any().downcast_ref::<StringArray>() {
                    let num_rows = batch.num_rows();
                    let dict_size = str_values.len() + 1; // +1 for NULL slot

                    // Check if this is COUNT(*) only - can optimize by streaming directly
                    let is_count_only = stmt.columns.iter().all(|c| {
                        matches!(
                            c,
                            SelectColumn::Aggregate {
                                func: AggregateFunc::Count,
                                column: None,
                                ..
                            }
                        )
                    });

                    if is_count_only {
                        // OPTIMIZED: Direct aggregation without building indices Vec
                        let mut counts: Vec<i64> = vec![0; dict_size];

                        for row_idx in 0..num_rows {
                            if !keys.is_null(row_idx) {
                                let group_idx = keys.value(row_idx) as usize + 1;
                                unsafe {
                                    *counts.get_unchecked_mut(group_idx) += 1;
                                }
                            }
                        }

                        // Build result directly
                        let active_groups: Vec<usize> =
                            (1..dict_size).filter(|&i| counts[i] > 0).collect();

                        let mut result_fields: Vec<Field> = Vec::new();
                        let mut result_arrays: Vec<ArrayRef> = Vec::new();

                        // Add group column
                        let group_col_name_clean = stmt
                            .group_by
                            .first()
                            .map(|s| {
                                let trimmed = s.trim_matches('"');
                                if let Some(dot_pos) = trimmed.rfind('.') {
                                    trimmed[dot_pos + 1..].to_string()
                                } else {
                                    trimmed.to_string()
                                }
                            })
                            .unwrap_or_else(|| "group".to_string());

                        let group_values: Vec<&str> = active_groups
                            .iter()
                            .map(|&i| str_values.value(i - 1))
                            .collect();
                        result_fields.push(Field::new(
                            &group_col_name_clean,
                            ArrowDataType::Utf8,
                            false,
                        ));
                        result_arrays.push(Arc::new(StringArray::from(group_values)));

                        // Add COUNT(*) column
                        let count_values: Vec<i64> =
                            active_groups.iter().map(|&i| counts[i]).collect();
                        result_fields.push(Field::new("COUNT(*)", ArrowDataType::Int64, false));
                        result_arrays.push(Arc::new(Int64Array::from(count_values)));

                        let schema = Arc::new(Schema::new(result_fields));
                        let result_batch = RecordBatch::try_new(schema, result_arrays)
                            .map_err(|e| err_data(e.to_string()))?;

                        return Ok(ApexResult::Data(result_batch));
                    }

                    // For other aggregates, use the standard path
                    let indices: Vec<u32> = (0..num_rows)
                        .map(|i| {
                            if keys.is_null(i) {
                                0u32
                            } else {
                                keys.value(i) + 1
                            }
                        })
                        .collect();

                    let dict_values: Vec<&str> =
                        (0..str_values.len()).map(|i| str_values.value(i)).collect();

                    return Self::execute_group_by_string_dict(
                        batch,
                        stmt,
                        str_values,
                        &indices,
                        &dict_values,
                        dict_size,
                    );
                }
            }

            // OPTIMIZATION: For low-cardinality StringArray, build dictionary on the fly
            // REMOVED sampling to stabilize performance - always try dictionary path first
            if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
                let num_rows = batch.num_rows();

                // Build dictionary directly without sampling - more stable performance
                let mut dict: AHashMap<&str, u32> = AHashMap::with_capacity(200);
                let mut dict_values: Vec<&str> = Vec::with_capacity(200);
                let mut next_id = 1u32;

                // First pass: build dictionary and check cardinality
                let mut indices: Vec<u32> = Vec::with_capacity(num_rows);
                indices.resize(num_rows, 0);

                for i in 0..num_rows {
                    if !str_arr.is_null(i) {
                        let s = str_arr.value(i);
                        let id = *dict.entry(s).or_insert_with(|| {
                            let id = next_id;
                            next_id += 1;
                            dict_values.push(s);
                            id
                        });
                        indices[i] = id;
                    }
                }

                // Only use dict indexing if cardinality is reasonable (<=1000)
                let dict_size = dict_values.len() + 1;
                if dict_size <= 1000 {
                    return Self::execute_group_by_string_dict(
                        batch,
                        stmt,
                        str_arr,
                        &indices,
                        &dict_values,
                        dict_size,
                    );
                }
                // Fall through to hash-based aggregation for high cardinality
            }
        }

        // Find aggregate column name and type
        let mut agg_col_name: Option<&str> = None;
        let mut has_int_agg = false;

        for col in &stmt.columns {
            if let SelectColumn::Aggregate {
                column: Some(col_name),
                ..
            } = col
            {
                let actual_col = col_name.trim_matches('"');
                let actual_col = if let Some(dot_pos) = actual_col.rfind('.') {
                    &actual_col[dot_pos + 1..]
                } else {
                    actual_col
                };
                if actual_col != "*" {
                    agg_col_name = Some(actual_col);
                    // Check if it's an int column
                    if let Some(arr) = batch.column_by_name(actual_col) {
                        has_int_agg = arr.as_any().downcast_ref::<Int64Array>().is_some();
                    }
                }
                break;
            }
        }

        // Execute vectorized GROUP BY for non-dictionary columns
        let hash_agg =
            execute_vectorized_group_by(batch, group_col_name, agg_col_name, has_int_agg)?;

        // Build result from hash aggregation table
        Self::build_group_by_result_from_vectorized(stmt, group_col_name, &hash_agg, has_int_agg)
    }

    pub(in crate::query::executor) fn build_group_by_result_from_vectorized(
        stmt: &SelectStatement,
        group_col_name: &str,
        hash_agg: &crate::query::vectorized::VectorizedHashAgg,
        has_int_agg: bool,
    ) -> io::Result<ApexResult> {
        use crate::query::AggregateFunc;

        let num_groups = hash_agg.num_groups();
        if num_groups == 0 {
            // Return empty result
            let schema = Arc::new(Schema::new(vec![Field::new(
                group_col_name,
                ArrowDataType::Utf8,
                false,
            )]));
            return Ok(ApexResult::Empty(schema));
        }

        let states = hash_agg.states();
        let group_keys_str = hash_agg.group_keys_str();
        let group_keys_int = hash_agg.group_keys_int();

        let mut result_fields: Vec<Field> = Vec::new();
        let mut result_arrays: Vec<ArrayRef> = Vec::new();

        // Check if group column has an alias in the SELECT clause
        let mut group_col_alias: Option<&str> = None;
        for col in &stmt.columns {
            if let SelectColumn::ColumnAlias { column, alias } = col {
                let col_name = column.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                if actual_col == group_col_name {
                    group_col_alias = Some(alias.as_str());
                    break;
                }
            }
        }
        let output_group_name = group_col_alias.unwrap_or(group_col_name);

        // Add group column
        if !group_keys_str.is_empty() {
            result_fields.push(Field::new(output_group_name, ArrowDataType::Utf8, false));
            result_arrays.push(Arc::new(StringArray::from(
                group_keys_str
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )));
        } else {
            result_fields.push(Field::new(output_group_name, ArrowDataType::Int64, false));
            result_arrays.push(Arc::new(Int64Array::from(group_keys_int.to_vec())));
        }

        // Add aggregate columns
        for col in &stmt.columns {
            if let SelectColumn::Aggregate {
                func,
                column,
                alias,
                ..
            } = col
            {
                let func_name = match func {
                    AggregateFunc::Count => "COUNT",
                    AggregateFunc::Sum => "SUM",
                    AggregateFunc::Avg => "AVG",
                    AggregateFunc::Min => "MIN",
                    AggregateFunc::Max => "MAX",
                };
                let field_name = alias.clone().unwrap_or_else(|| {
                    format!("{}({})", func_name, column.as_deref().unwrap_or("*"))
                });

                match func {
                    AggregateFunc::Count => {
                        result_fields.push(Field::new(&field_name, ArrowDataType::Int64, false));
                        result_arrays.push(Arc::new(Int64Array::from(
                            states.iter().map(|s| s.count).collect::<Vec<_>>(),
                        )));
                    }
                    AggregateFunc::Sum => {
                        if has_int_agg {
                            result_fields.push(Field::new(&field_name, ArrowDataType::Int64, true));
                            result_arrays.push(Arc::new(Int64Array::from(
                                states.iter().map(|s| s.sum_int).collect::<Vec<_>>(),
                            )));
                        } else {
                            result_fields.push(Field::new(
                                &field_name,
                                ArrowDataType::Float64,
                                true,
                            ));
                            result_arrays.push(Arc::new(Float64Array::from(
                                states.iter().map(|s| s.sum_float).collect::<Vec<_>>(),
                            )));
                        }
                    }
                    AggregateFunc::Avg => {
                        result_fields.push(Field::new(&field_name, ArrowDataType::Float64, true));
                        result_arrays.push(Arc::new(Float64Array::from(
                            states
                                .iter()
                                .map(|s| {
                                    if s.count > 0 {
                                        if has_int_agg {
                                            s.sum_int as f64 / s.count as f64
                                        } else {
                                            s.sum_float / s.count as f64
                                        }
                                    } else {
                                        0.0
                                    }
                                })
                                .collect::<Vec<_>>(),
                        )));
                    }
                    AggregateFunc::Min => {
                        if has_int_agg {
                            result_fields.push(Field::new(&field_name, ArrowDataType::Int64, true));
                            result_arrays.push(Arc::new(Int64Array::from(
                                states.iter().map(|s| s.min_int).collect::<Vec<_>>(),
                            )));
                        } else {
                            result_fields.push(Field::new(
                                &field_name,
                                ArrowDataType::Float64,
                                true,
                            ));
                            result_arrays.push(Arc::new(Float64Array::from(
                                states.iter().map(|s| s.min_float).collect::<Vec<_>>(),
                            )));
                        }
                    }
                    AggregateFunc::Max => {
                        if has_int_agg {
                            result_fields.push(Field::new(&field_name, ArrowDataType::Int64, true));
                            result_arrays.push(Arc::new(Int64Array::from(
                                states.iter().map(|s| s.max_int).collect::<Vec<_>>(),
                            )));
                        } else {
                            result_fields.push(Field::new(
                                &field_name,
                                ArrowDataType::Float64,
                                true,
                            ));
                            result_arrays.push(Arc::new(Float64Array::from(
                                states.iter().map(|s| s.max_float).collect::<Vec<_>>(),
                            )));
                        }
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(result_fields));
        let mut result_batch =
            RecordBatch::try_new(schema, result_arrays).map_err(|e| err_data(e.to_string()))?;

        // Apply HAVING clause if present
        if let Some(having_expr) = &stmt.having {
            let mask = Self::evaluate_predicate(&result_batch, having_expr)?;
            result_batch = compute::filter_record_batch(&result_batch, &mask)
                .map_err(|e| err_data(e.to_string()))?;
        }

        // Apply ORDER BY (resolve aggregate expressions to output column names first),
        // then LIMIT/OFFSET so grouped queries with ORDER BY LIMIT return bounded rows.
        if !stmt.order_by.is_empty() {
            let resolved_ob = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
            result_batch = Self::apply_order_by_topk(&result_batch, &resolved_ob, k)?;
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            result_batch = Self::apply_limit_offset(&result_batch, stmt.limit, stmt.offset)?;
        }

        Ok(ApexResult::Data(result_batch))
    }

    pub(in crate::query::executor) fn can_use_incremental_aggregation(stmt: &SelectStatement) -> bool {
        let mut aggregate_source: Option<&str> = None;
        for col in &stmt.columns {
            match col {
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct,
                    ..
                } => {
                    if *distinct {
                        return false;
                    }
                    // COUNT(col) with specific col needs null-aware path
                    if matches!(func, crate::query::AggregateFunc::Count) {
                        if let Some(c) = column {
                            if c != "*" && c != "1" {
                                return false;
                            }
                        }
                    }
                    if !matches!(func, crate::query::AggregateFunc::Count) {
                        if let Some(column) = column {
                            let source = column
                                .trim_matches('"')
                                .rsplit('.')
                                .next()
                                .unwrap_or(column);
                            if aggregate_source.is_some_and(|existing| existing != source) {
                                return false;
                            }
                            aggregate_source = Some(source);
                        }
                    }
                }
                SelectColumn::Expression { .. } => {
                    return false; // Expressions may need row access
                }
                _ => {}
            }
        }
        // HAVING with aggregates is OK, but complex expressions aren't
        true
    }

    pub(in crate::query::executor) fn group_output_name(stmt: &SelectStatement, group_column: &str) -> String {
        let clean = group_column.trim_matches('"');
        stmt.columns
            .iter()
            .find_map(|column| {
                if let SelectColumn::ColumnAlias { column, alias } = column {
                    let source = column
                        .trim_matches('"')
                        .rsplit('.')
                        .next()
                        .unwrap_or(column);
                    if source == clean.rsplit('.').next().unwrap_or(clean) {
                        return Some(alias.clone());
                    }
                }
                None
            })
            .unwrap_or_else(|| clean.rsplit('.').next().unwrap_or(clean).to_string())
    }

    /// Fast path: a single dictionary-encoded string GROUP BY key with one
    /// `COUNT(CASE WHEN cond THEN 1 END)` (or `COUNT(IF(cond, 1, 0))`)
    /// aggregate.  The condition mask is evaluated once and each row is
    /// counted into its key slot directly — no hash map, no per-group row
    /// index vectors.
    pub(in crate::query::executor) fn try_execute_dict_case_count(
        batch: &RecordBatch,
        stmt: &SelectStatement,
        group_cols: &[String],
    ) -> io::Result<Option<ApexResult>> {
        if group_cols.len() != 1 || stmt.distinct {
            return Ok(None);
        }
        let group_col = &group_cols[0];
        let Some(col) = batch.column_by_name(group_col) else {
            return Ok(None);
        };
        use arrow::array::DictionaryArray;
        use arrow::datatypes::UInt32Type;
        let Some(dict_arr) = col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() else {
            return Ok(None);
        };
        let Some(str_values) = dict_arr.values().as_any().downcast_ref::<StringArray>() else {
            return Ok(None);
        };

        // Columns: the group column plus exactly one COUNT(CASE ...) expression.
        let mut count_alias: Option<String> = None;
        let mut count_spec: Option<(SqlExpr, bool)> = None;
        for column in &stmt.columns {
            match column {
                SelectColumn::Column(_) | SelectColumn::ColumnAlias { .. } => {}
                SelectColumn::Expression { expr, alias } => {
                    if count_spec.is_some() {
                        return Ok(None);
                    }
                    let SqlExpr::Function { name, args } = expr else {
                        return Ok(None);
                    };
                    if !name.eq_ignore_ascii_case("COUNT") || args.len() != 1 {
                        return Ok(None);
                    }
                    let Some(spec) = Self::count_case_condition(&args[0]) else {
                        return Ok(None);
                    };
                    count_spec = Some(spec);
                    count_alias = alias.clone();
                }
                _ => return Ok(None),
            }
        }
        let Some((cond_expr, count_true)) = count_spec else {
            return Ok(None);
        };
        let cond = Self::evaluate_predicate(batch, &cond_expr)?;

        let dict_size = str_values.len() + 1;
        let mut counts: Vec<i64> = vec![0; dict_size];
        let keys = dict_arr.keys();
        for row in 0..batch.num_rows() {
            let idx = if keys.is_null(row) {
                0usize
            } else {
                keys.value(row) as usize + 1
            };
            let hit = if count_true {
                !cond.is_null(row) && cond.value(row)
            } else {
                !cond.is_null(row)
            };
            if hit {
                counts[idx] += 1;
            }
        }

        let active_groups: Vec<usize> = (0..dict_size).filter(|&i| counts[i] > 0).collect();
        let group_col_name = Self::group_output_name(stmt, group_col);
        let group_values: Vec<Option<&str>> = active_groups
            .iter()
            .map(|&i| {
                if i == 0 {
                    None
                } else {
                    Some(str_values.value(i - 1))
                }
            })
            .collect();
        let count_name = count_alias.unwrap_or_else(|| "expr".to_string());

        let mut fields = vec![Field::new(&group_col_name, ArrowDataType::Utf8, true)];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(group_values))];
        fields.push(Field::new(&count_name, ArrowDataType::Int64, false));
        arrays.push(Arc::new(Int64Array::from(
            active_groups.iter().map(|&i| counts[i]).collect::<Vec<_>>(),
        )));

        let schema = Arc::new(Schema::new(fields));
        let mut result_batch =
            RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;

        if !stmt.order_by.is_empty() {
            let resolved_ob = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
            result_batch = Self::apply_order_by_topk(&result_batch, &resolved_ob, k)?;
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            result_batch = Self::apply_limit_offset(&result_batch, stmt.limit, stmt.offset)?;
        }

        Ok(Some(ApexResult::Data(result_batch)))
    }

    pub(in crate::query::executor) fn execute_group_by_string_dict(
        batch: &RecordBatch,
        stmt: &SelectStatement,        _str_arr: &StringArray,
        indices: &[u32],
        dict_values: &[&str],
        dict_size: usize,
    ) -> io::Result<ApexResult> {
        use crate::query::AggregateFunc;

        let num_rows = batch.num_rows();

        // Direct-indexed aggregate state - pre-allocated for all possible groups
        let mut counts: Vec<i64> = vec![0; dict_size];
        let mut sums_int: Vec<i64> = vec![0; dict_size];
        let mut sums_float: Vec<f64> = vec![0.0; dict_size];
        let mut mins_int: Vec<Option<i64>> = vec![None; dict_size];
        let mut maxs_int: Vec<Option<i64>> = vec![None; dict_size];
        let mut mins_float: Vec<Option<f64>> = vec![None; dict_size];
        let mut maxs_float: Vec<Option<f64>> = vec![None; dict_size];

        // Find aggregate column
        let mut agg_col_int: Option<&Int64Array> = None;
        let mut agg_col_float: Option<&Float64Array> = None;

        for col in &stmt.columns {
            if let SelectColumn::Aggregate {
                column: Some(col_name),
                ..
            } = col
            {
                let actual_col = col_name.trim_matches('"');
                let actual_col = if let Some(dot_pos) = actual_col.rfind('.') {
                    &actual_col[dot_pos + 1..]
                } else {
                    actual_col
                };
                if actual_col != "*" {
                    if let Some(arr) = batch.column_by_name(actual_col) {
                        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                            agg_col_int = Some(int_arr);
                        } else if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>()
                        {
                            agg_col_float = Some(float_arr);
                        }
                    }
                }
                break;
            }
        }

        // OPTIMIZED AGGREGATION: Single pass with bounds-check elimination
        // Uses unsafe for hot path when no nulls present
        if let Some(int_arr) = agg_col_int {
            if int_arr.null_count() == 0 {
                // Fast path: no nulls - use raw slice access
                let values = int_arr.values();
                for row_idx in 0..num_rows {
                    let group_idx = unsafe { *indices.get_unchecked(row_idx) as usize };
                    unsafe {
                        *counts.get_unchecked_mut(group_idx) += 1;
                        let val = *values.get_unchecked(row_idx);
                        *sums_int.get_unchecked_mut(group_idx) =
                            sums_int.get_unchecked(group_idx).wrapping_add(val);
                        let min_slot = mins_int.get_unchecked_mut(group_idx);
                        *min_slot = Some(min_slot.map_or(val, |m| m.min(val)));
                        let max_slot = maxs_int.get_unchecked_mut(group_idx);
                        *max_slot = Some(max_slot.map_or(val, |m| m.max(val)));
                    }
                }
            } else {
                // Slow path: has nulls
                for row_idx in 0..num_rows {
                    let group_idx = indices[row_idx] as usize;
                    counts[group_idx] += 1;
                    if !int_arr.is_null(row_idx) {
                        let val = int_arr.value(row_idx);
                        sums_int[group_idx] = sums_int[group_idx].wrapping_add(val);
                        mins_int[group_idx] = Some(mins_int[group_idx].map_or(val, |m| m.min(val)));
                        maxs_int[group_idx] = Some(maxs_int[group_idx].map_or(val, |m| m.max(val)));
                    }
                }
            }
        } else if let Some(float_arr) = agg_col_float {
            if float_arr.null_count() == 0 {
                let values = float_arr.values();
                for row_idx in 0..num_rows {
                    let group_idx = unsafe { *indices.get_unchecked(row_idx) as usize };
                    unsafe {
                        *counts.get_unchecked_mut(group_idx) += 1;
                        let val = *values.get_unchecked(row_idx);
                        *sums_float.get_unchecked_mut(group_idx) += val;
                        let min_slot = mins_float.get_unchecked_mut(group_idx);
                        *min_slot = Some(min_slot.map_or(val, |m| m.min(val)));
                        let max_slot = maxs_float.get_unchecked_mut(group_idx);
                        *max_slot = Some(max_slot.map_or(val, |m| m.max(val)));
                    }
                }
            } else {
                for row_idx in 0..num_rows {
                    let group_idx = indices[row_idx] as usize;
                    counts[group_idx] += 1;
                    if !float_arr.is_null(row_idx) {
                        let val = float_arr.value(row_idx);
                        sums_float[group_idx] += val;
                        mins_float[group_idx] =
                            Some(mins_float[group_idx].map_or(val, |m| m.min(val)));
                        maxs_float[group_idx] =
                            Some(maxs_float[group_idx].map_or(val, |m| m.max(val)));
                    }
                }
            }
        } else {
            // COUNT(*) only
            for row_idx in 0..num_rows {
                let group_idx = unsafe { *indices.get_unchecked(row_idx) as usize };
                unsafe {
                    *counts.get_unchecked_mut(group_idx) += 1;
                }
            }
        }

        // Index 0 is the NULL group; real strings start at 1, including "".
        let active_groups: Vec<usize> = (0..dict_size).filter(|&i| counts[i] > 0).collect();

        // Build result arrays
        let mut result_fields: Vec<Field> = Vec::new();
        let mut result_arrays: Vec<ArrayRef> = Vec::new();

        // Add group column (string values from dictionary)
        // OPTIMIZATION: Check if group column has an alias in SELECT clause
        let group_by_col = stmt
            .group_by
            .first()
            .map(|s| s.trim_matches('"'))
            .unwrap_or("");
        let group_col_name = stmt
            .columns
            .iter()
            .find_map(|col| {
                // Check for ColumnAlias pattern: column AS alias
                if let SelectColumn::ColumnAlias { column, alias } = col {
                    let col_trimmed = column.trim_matches('"');
                    // Match either full name (u.tier) or just column name (tier)
                    if col_trimmed == group_by_col
                        || (group_by_col.contains('.')
                            && col_trimmed == group_by_col.split('.').next().unwrap_or(""))
                        || (col_trimmed.contains('.')
                            && col_trimmed.ends_with(&format!(
                                ". {}",
                                group_by_col.split('.').last().unwrap_or("")
                            )))
                    {
                        return Some(alias.clone());
                    }
                }
                None
            })
            .unwrap_or_else(|| {
                // No alias found, use column name (stripping table prefix)
                if let Some(dot_pos) = group_by_col.rfind('.') {
                    group_by_col[dot_pos + 1..].to_string()
                } else {
                    group_by_col.to_string()
                }
            });

        let group_values: Vec<Option<&str>> = active_groups
            .iter()
            .map(|&i| {
                if i == 0 {
                    None
                } else {
                    Some(dict_values[i - 1])
                }
            })
            .collect();
        result_fields.push(Field::new(&group_col_name, ArrowDataType::Utf8, true));
        result_arrays.push(Arc::new(StringArray::from(group_values)));

        // Add aggregate columns
        for col in &stmt.columns {
            if let SelectColumn::Aggregate {
                func,
                column,
                alias,
                ..
            } = col
            {
                let func_name = match func {
                    AggregateFunc::Count => "COUNT",
                    AggregateFunc::Sum => "SUM",
                    AggregateFunc::Avg => "AVG",
                    AggregateFunc::Min => "MIN",
                    AggregateFunc::Max => "MAX",
                };
                let field_name = alias.clone().unwrap_or_else(|| {
                    format!("{}({})", func_name, column.as_deref().unwrap_or("*"))
                });

                match func {
                    AggregateFunc::Count => {
                        let values: Vec<i64> = active_groups.iter().map(|&i| counts[i]).collect();
                        result_fields.push(Field::new(&field_name, ArrowDataType::Int64, false));
                        result_arrays.push(Arc::new(Int64Array::from(values)));
                    }
                    AggregateFunc::Sum => {
                        if agg_col_int.is_some() {
                            let values: Vec<i64> =
                                active_groups.iter().map(|&i| sums_int[i]).collect();
                            result_fields.push(Field::new(&field_name, ArrowDataType::Int64, true));
                            result_arrays.push(Arc::new(Int64Array::from(values)));
                        } else {
                            let values: Vec<f64> =
                                active_groups.iter().map(|&i| sums_float[i]).collect();
                            result_fields.push(Field::new(
                                &field_name,
                                ArrowDataType::Float64,
                                true,
                            ));
                            result_arrays.push(Arc::new(Float64Array::from(values)));
                        }
                    }
                    AggregateFunc::Avg => {
                        let values: Vec<f64> = active_groups
                            .iter()
                            .map(|&i| {
                                if counts[i] > 0 {
                                    if agg_col_int.is_some() {
                                        sums_int[i] as f64 / counts[i] as f64
                                    } else {
                                        sums_float[i] / counts[i] as f64
                                    }
                                } else {
                                    0.0
                                }
                            })
                            .collect();
                        result_fields.push(Field::new(&field_name, ArrowDataType::Float64, true));
                        result_arrays.push(Arc::new(Float64Array::from(values)));
                    }
                    AggregateFunc::Min => {
                        if agg_col_int.is_some() {
                            let values: Vec<Option<i64>> =
                                active_groups.iter().map(|&i| mins_int[i]).collect();
                            result_fields.push(Field::new(&field_name, ArrowDataType::Int64, true));
                            result_arrays.push(Arc::new(Int64Array::from(values)));
                        } else {
                            let values: Vec<Option<f64>> =
                                active_groups.iter().map(|&i| mins_float[i]).collect();
                            result_fields.push(Field::new(
                                &field_name,
                                ArrowDataType::Float64,
                                true,
                            ));
                            result_arrays.push(Arc::new(Float64Array::from(values)));
                        }
                    }
                    AggregateFunc::Max => {
                        if agg_col_int.is_some() {
                            let values: Vec<Option<i64>> =
                                active_groups.iter().map(|&i| maxs_int[i]).collect();
                            result_fields.push(Field::new(&field_name, ArrowDataType::Int64, true));
                            result_arrays.push(Arc::new(Int64Array::from(values)));
                        } else {
                            let values: Vec<Option<f64>> =
                                active_groups.iter().map(|&i| maxs_float[i]).collect();
                            result_fields.push(Field::new(
                                &field_name,
                                ArrowDataType::Float64,
                                true,
                            ));
                            result_arrays.push(Arc::new(Float64Array::from(values)));
                        }
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(result_fields));
        let mut result_batch =
            RecordBatch::try_new(schema, result_arrays).map_err(|e| err_data(e.to_string()))?;

        // Apply HAVING clause if present
        if let Some(having_expr) = &stmt.having {
            let mask = Self::evaluate_predicate(&result_batch, having_expr)?;
            result_batch = compute::filter_record_batch(&result_batch, &mask)
                .map_err(|e| err_data(e.to_string()))?;
        }

        // Apply ORDER BY (resolve aggregate expressions to output column names first).
        // A top-k sort is used when LIMIT is present; LIMIT/OFFSET then applies.
        if !stmt.order_by.is_empty() {
            let resolved_ob = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
            result_batch = Self::apply_order_by_topk(&result_batch, &resolved_ob, k)?;
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            result_batch = Self::apply_limit_offset(&result_batch, stmt.limit, stmt.offset)?;
        }

        Ok(ApexResult::Data(result_batch))
    }

    pub(in crate::query::executor) fn execute_group_by_direct_index(
        batch: &RecordBatch,
        stmt: &SelectStatement,
        group_col: &Int64Array,
        min_val: usize,
        range: usize,
    ) -> io::Result<ApexResult> {
        use crate::query::AggregateFunc;

        let num_rows = batch.num_rows();

        // Direct-indexed aggregate state: [count, sum_int, sum_float, min_int, max_int, first_row]
        let mut counts: Vec<i64> = vec![0; range];
        let mut sums_int: Vec<i64> = vec![0; range];
        let mut sums_float: Vec<f64> = vec![0.0; range];
        let mut mins_int: Vec<Option<i64>> = vec![None; range];
        let mut maxs_int: Vec<Option<i64>> = vec![None; range];
        let mut mins_float: Vec<Option<f64>> = vec![None; range];
        let mut maxs_float: Vec<Option<f64>> = vec![None; range];
        let mut first_rows: Vec<usize> = vec![usize::MAX; range];
        // Separate state for NULL-key rows
        let mut null_count: i64 = 0;
        let mut null_sum_int: i64 = 0;
        let mut null_sum_float: f64 = 0.0;
        let mut null_min_int: Option<i64> = None;
        let mut null_max_int: Option<i64> = None;
        let mut null_min_float: Option<f64> = None;
        let mut null_max_float: Option<f64> = None;
        let mut null_first_row: usize = usize::MAX;

        // Find aggregate column
        let mut agg_col_int: Option<&Int64Array> = None;
        let mut agg_col_float: Option<&Float64Array> = None;

        for col in &stmt.columns {
            if let SelectColumn::Aggregate {
                column: Some(col_name),
                ..
            } = col
            {
                let actual_col = col_name.trim_matches('"');
                let actual_col = if let Some(dot_pos) = actual_col.rfind('.') {
                    &actual_col[dot_pos + 1..]
                } else {
                    actual_col
                };
                if actual_col != "*" {
                    if let Some(arr) = batch.column_by_name(actual_col) {
                        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                            agg_col_int = Some(int_arr);
                        } else if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>()
                        {
                            agg_col_float = Some(float_arr);
                        }
                    }
                }
                break;
            }
        }

        // Single pass aggregation with direct indexing
        for row_idx in 0..num_rows {
            if group_col.is_null(row_idx) {
                // NULL key: track separately
                null_count += 1;
                if null_first_row == usize::MAX {
                    null_first_row = row_idx;
                }
                if let Some(int_arr) = agg_col_int {
                    if !int_arr.is_null(row_idx) {
                        let val = int_arr.value(row_idx);
                        null_sum_int = null_sum_int.wrapping_add(val);
                        null_min_int = Some(null_min_int.map_or(val, |m: i64| m.min(val)));
                        null_max_int = Some(null_max_int.map_or(val, |m: i64| m.max(val)));
                    }
                }
                if let Some(float_arr) = agg_col_float {
                    if !float_arr.is_null(row_idx) {
                        let val = float_arr.value(row_idx);
                        null_sum_float += val;
                        null_min_float = Some(null_min_float.map_or(val, |m| m.min(val)));
                        null_max_float = Some(null_max_float.map_or(val, |m| m.max(val)));
                    }
                }
                continue;
            }
            let group_val = group_col.value(row_idx) as usize - min_val;

            counts[group_val] += 1;
            if first_rows[group_val] == usize::MAX {
                first_rows[group_val] = row_idx;
            }

            if let Some(int_arr) = agg_col_int {
                if !int_arr.is_null(row_idx) {
                    let val = int_arr.value(row_idx);
                    sums_int[group_val] = sums_int[group_val].wrapping_add(val);
                    mins_int[group_val] = Some(mins_int[group_val].map_or(val, |m| m.min(val)));
                    maxs_int[group_val] = Some(maxs_int[group_val].map_or(val, |m| m.max(val)));
                }
            }
            if let Some(float_arr) = agg_col_float {
                if !float_arr.is_null(row_idx) {
                    let val = float_arr.value(row_idx);
                    sums_float[group_val] += val;
                    mins_float[group_val] =
                        Some(mins_float[group_val].map_or(val, |m| m.min(val)));
                    maxs_float[group_val] =
                        Some(maxs_float[group_val].map_or(val, |m| m.max(val)));
                }
            }
        }

        // Collect non-empty groups
        let active_groups: Vec<usize> = (0..range).filter(|&i| counts[i] > 0).collect();

        // Build result arrays
        let mut result_fields: Vec<Field> = Vec::new();
        let mut result_arrays: Vec<ArrayRef> = Vec::new();

        // Add group column
        let group_col_name = stmt
            .group_by
            .first()
            .map(|s| s.trim_matches('"').to_string())
            .unwrap_or_else(|| "group".to_string());

        // Build group key array including optional NULL group
        let has_null_group = null_count > 0;
        let total_groups = active_groups.len() + if has_null_group { 1 } else { 0 };
        let _ = total_groups; // suppress warning
        {
            // Build Int64 array with possible null entry
            let mut key_vals: Vec<Option<i64>> = active_groups
                .iter()
                .map(|&i| Some((i + min_val) as i64))
                .collect();
            if has_null_group {
                key_vals.push(None);
            }
            result_fields.push(Field::new(&group_col_name, ArrowDataType::Int64, true));
            result_arrays.push(Arc::new(Int64Array::from(key_vals)));
        }

        // Add aggregate columns
        for col in &stmt.columns {
            if let SelectColumn::Aggregate {
                func,
                column,
                alias,
                ..
            } = col
            {
                let field_name = alias.clone().unwrap_or_else(|| {
                    format!("{}({})", func.to_string(), column.as_deref().unwrap_or("*"))
                });
                match func {
                    AggregateFunc::Count => {
                        let mut vals: Vec<i64> = active_groups.iter().map(|&i| counts[i]).collect();
                        if has_null_group {
                            vals.push(null_count);
                        }
                        result_fields.push(Field::new(&field_name, ArrowDataType::Int64, false));
                        result_arrays.push(Arc::new(Int64Array::from(vals)));
                    }
                    AggregateFunc::Sum => {
                        if agg_col_int.is_some() {
                            let mut vals: Vec<i64> =
                                active_groups.iter().map(|&i| sums_int[i]).collect();
                            if has_null_group {
                                vals.push(null_sum_int);
                            }
                            result_fields.push(Field::new(&field_name, ArrowDataType::Int64, true));
                            result_arrays.push(Arc::new(Int64Array::from(vals)));
                        } else {
                            let mut vals: Vec<f64> =
                                active_groups.iter().map(|&i| sums_float[i]).collect();
                            if has_null_group {
                                vals.push(null_sum_float);
                            }
                            result_fields.push(Field::new(
                                &field_name,
                                ArrowDataType::Float64,
                                true,
                            ));
                            result_arrays.push(Arc::new(Float64Array::from(vals)));
                        }
                    }
                    AggregateFunc::Avg => {
                        let mut vals: Vec<f64> = active_groups
                            .iter()
                            .map(|&i| {
                                if counts[i] > 0 {
                                    if agg_col_int.is_some() {
                                        sums_int[i] as f64 / counts[i] as f64
                                    } else {
                                        sums_float[i] / counts[i] as f64
                                    }
                                } else {
                                    0.0
                                }
                            })
                            .collect();
                        if has_null_group {
                            vals.push(if null_count > 0 {
                                if agg_col_int.is_some() {
                                    null_sum_int as f64 / null_count as f64
                                } else {
                                    null_sum_float / null_count as f64
                                }
                            } else {
                                0.0
                            });
                        }
                        result_fields.push(Field::new(&field_name, ArrowDataType::Float64, true));
                        result_arrays.push(Arc::new(Float64Array::from(vals)));
                    }
                    AggregateFunc::Min => {
                        if agg_col_int.is_some() {
                            let mut vals: Vec<Option<i64>> =
                                active_groups.iter().map(|&i| mins_int[i]).collect();
                            if has_null_group {
                                vals.push(null_min_int);
                            }
                            result_fields
                                .push(Field::new(&field_name, ArrowDataType::Int64, true));
                            result_arrays.push(Arc::new(Int64Array::from(vals)));
                        } else {
                            let mut vals: Vec<Option<f64>> =
                                active_groups.iter().map(|&i| mins_float[i]).collect();
                            if has_null_group {
                                vals.push(null_min_float);
                            }
                            result_fields
                                .push(Field::new(&field_name, ArrowDataType::Float64, true));
                            result_arrays.push(Arc::new(Float64Array::from(vals)));
                        }
                    }
                    AggregateFunc::Max => {
                        if agg_col_int.is_some() {
                            let mut vals: Vec<Option<i64>> =
                                active_groups.iter().map(|&i| maxs_int[i]).collect();
                            if has_null_group {
                                vals.push(null_max_int);
                            }
                            result_fields
                                .push(Field::new(&field_name, ArrowDataType::Int64, true));
                            result_arrays.push(Arc::new(Int64Array::from(vals)));
                        } else {
                            let mut vals: Vec<Option<f64>> =
                                active_groups.iter().map(|&i| maxs_float[i]).collect();
                            if has_null_group {
                                vals.push(null_max_float);
                            }
                            result_fields
                                .push(Field::new(&field_name, ArrowDataType::Float64, true));
                            result_arrays.push(Arc::new(Float64Array::from(vals)));
                        }
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(result_fields));
        let mut result_batch =
            RecordBatch::try_new(schema, result_arrays).map_err(|e| err_data(e.to_string()))?;
        if let Some(having_expr) = &stmt.having {
            let mask = Self::evaluate_predicate(&result_batch, having_expr)?;
            result_batch = compute::filter_record_batch(&result_batch, &mask)
                .map_err(|e| err_data(e.to_string()))?;
        }
        if !stmt.order_by.is_empty() {
            let resolved_ob = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
            result_batch = Self::apply_order_by_topk(&result_batch, &resolved_ob, k)?;
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            result_batch = Self::apply_limit_offset(&result_batch, stmt.limit, stmt.offset)?;
        }
        Ok(ApexResult::Data(result_batch))
    }

    pub(in crate::query::executor) fn execute_group_by_incremental(
        batch: &RecordBatch,
        stmt: &SelectStatement,
        group_cols: &[String],
    ) -> io::Result<ApexResult> {
        use crate::query::AggregateFunc;

        let num_rows = batch.num_rows();

        // FAST PATH: two string keys with COUNT(*). Encode both dictionaries and
        // aggregate in one pass. The packed pair of u32 IDs is collision-free and
        // avoids two row-sized index buffers plus partition-map merging.
        let is_two_string_count = group_cols.len() == 2
            && stmt.columns.iter().any(|column| {
                matches!(
                    column,
                    SelectColumn::Aggregate {
                        func: AggregateFunc::Count,
                        column: None,
                        distinct: false,
                        ..
                    }
                )
            })
            && stmt.columns.iter().all(|column| match column {
                SelectColumn::Column(name) | SelectColumn::ColumnAlias { column: name, .. } => {
                    let name = name.trim_matches('"');
                    let name = name.rsplit('.').next().unwrap_or(name);
                    group_cols.iter().any(|group| group == name)
                }
                SelectColumn::Aggregate {
                    func: AggregateFunc::Count,
                    column: None,
                    distinct: false,
                    ..
                } => true,
                _ => false,
            });

        if is_two_string_count {
            if let (Some(col1), Some(col2)) = (
                batch.column_by_name(&group_cols[0]),
                batch.column_by_name(&group_cols[1]),
            ) {
                if let (Some(arr1), Some(arr2)) = (
                    col1.as_any().downcast_ref::<StringArray>(),
                    col2.as_any().downcast_ref::<StringArray>(),
                ) {
                    let estimated = (num_rows / 10).clamp(16, 65_536);
                    let mut dict1: AHashMap<&str, u32> = AHashMap::with_capacity(estimated);
                    let mut dict2: AHashMap<&str, u32> = AHashMap::with_capacity(estimated);
                    let mut values1: Vec<Option<&str>> = Vec::with_capacity(estimated + 1);
                    let mut values2: Vec<Option<&str>> = Vec::with_capacity(estimated + 1);
                    values1.push(None);
                    values2.push(None);
                    let mut groups: AHashMap<u64, i64> = AHashMap::with_capacity(estimated);

                    for row_idx in 0..num_rows {
                        let id1 = if arr1.is_null(row_idx) {
                            0
                        } else {
                            let value = arr1.value(row_idx);
                            if value == "\x00__NULL__\x00" {
                                0
                            } else {
                                *dict1.entry(value).or_insert_with(|| {
                                    let id = values1.len() as u32;
                                    values1.push(Some(value));
                                    id
                                })
                            }
                        };
                        let id2 = if arr2.is_null(row_idx) {
                            0
                        } else {
                            let value = arr2.value(row_idx);
                            if value == "\x00__NULL__\x00" {
                                0
                            } else {
                                *dict2.entry(value).or_insert_with(|| {
                                    let id = values2.len() as u32;
                                    values2.push(Some(value));
                                    id
                                })
                            }
                        };
                        let key = ((id1 as u64) << 32) | id2 as u64;
                        *groups.entry(key).or_insert(0) += 1;
                    }

                    let grouped: Vec<(u64, i64)> = groups.into_iter().collect();
                    let counts: Vec<i64> = grouped.iter().map(|(_, count)| *count).collect();
                    let result1: ArrayRef = Arc::new(StringArray::from(
                        grouped
                            .iter()
                            .map(|(key, _)| values1[(key >> 32) as usize])
                            .collect::<Vec<_>>(),
                    ));
                    let result2: ArrayRef = Arc::new(StringArray::from(
                        grouped
                            .iter()
                            .map(|(key, _)| values2[(*key as u32) as usize])
                            .collect::<Vec<_>>(),
                    ));

                    let mut result_fields = Vec::with_capacity(stmt.columns.len());
                    let mut result_arrays = Vec::with_capacity(stmt.columns.len());
                    for column in &stmt.columns {
                        match column {
                            SelectColumn::Column(name)
                            | SelectColumn::ColumnAlias { column: name, .. } => {
                                let actual = name.trim_matches('"');
                                let actual = actual.rsplit('.').next().unwrap_or(actual);
                                let output = match column {
                                    SelectColumn::ColumnAlias { alias, .. } => alias.as_str(),
                                    _ => actual,
                                };
                                let array = if actual == group_cols[0] {
                                    result1.clone()
                                } else {
                                    result2.clone()
                                };
                                result_fields.push(Field::new(output, ArrowDataType::Utf8, true));
                                result_arrays.push(array);
                            }
                            SelectColumn::Aggregate { alias, .. } => {
                                let output = alias.as_deref().unwrap_or("COUNT(*)");
                                result_fields.push(Field::new(output, ArrowDataType::Int64, false));
                                result_arrays.push(Arc::new(Int64Array::from(counts.clone())));
                            }
                            _ => unreachable!(),
                        }
                    }

                    let schema = Arc::new(Schema::new(result_fields));
                    let mut result = RecordBatch::try_new(schema, result_arrays)
                        .map_err(|e| err_data(e.to_string()))?;
                    if let Some(having_expr) = &stmt.having {
                        let mask = Self::evaluate_predicate(&result, having_expr)?;
                        result = compute::filter_record_batch(&result, &mask)
                            .map_err(|e| err_data(e.to_string()))?;
                    }
                    if !stmt.order_by.is_empty() {
                        let resolved = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
                        let k = stmt.limit.map(|limit| limit + stmt.offset.unwrap_or(0));
                        result = Self::apply_order_by_topk(&result, &resolved, k)?;
                    }
                    result = Self::apply_limit_offset(&result, stmt.limit, stmt.offset)?;
                    return Ok(ApexResult::Data(result));
                }
            }
        }

        // FAST PATH: Single column GROUP BY on small integer range (e.g., category_id 0-999)
        // Uses direct array indexing instead of hash map - much faster
        if group_cols.len() == 1 {
            if let Some(col) = batch.column_by_name(&group_cols[0]) {
                if let Some(int_arr) = col.as_any().downcast_ref::<Int64Array>() {
                    // Check if values are in a small range for direct indexing
                    let (min_val, max_val) = {
                        let mut min = i64::MAX;
                        let mut max = i64::MIN;
                        for i in 0..num_rows {
                            if !int_arr.is_null(i) {
                                let v = int_arr.value(i);
                                min = min.min(v);
                                max = max.max(v);
                            }
                        }
                        (min, max)
                    };

                    // Use direct indexing if range is reasonable (< 10000 unique values)
                    let range = (max_val - min_val + 1) as usize;
                    if min_val >= 0 && range <= 10000 && range > 0 {
                        return Self::execute_group_by_direct_index(
                            batch,
                            stmt,
                            int_arr,
                            min_val as usize,
                            range,
                        );
                    }
                }
            }
        }

        let estimated_groups = (num_rows / 10).max(16);

        // Incremental aggregate state per group
        #[derive(Clone)]
        struct GroupState {
            first_row: usize,
            count: i64,
            sum_int: i64,
            sum_float: f64,
            min_int: Option<i64>,
            max_int: Option<i64>,
            min_float: Option<f64>,
            max_float: Option<f64>,
        }

        impl GroupState {
            #[inline(always)]
            fn new(first_row: usize) -> Self {
                Self {
                    first_row,
                    count: 0,
                    sum_int: 0,
                    sum_float: 0.0,
                    min_int: None,
                    max_int: None,
                    min_float: None,
                    max_float: None,
                }
            }
        }

        // Pre-downcast group columns and build runtime dictionaries for strings
        // OPTIMIZATION: Build dictionary (string -> integer ID) for low-cardinality string columns
        // This converts string hashing to integer operations, similar to DuckDB's storage-level dictionary
        enum TypedCol<'a> {
            Int64(&'a Int64Array),
            Float64(&'a Float64Array),
            StringDict(&'a ArrayRef, Vec<u32>), // (array, dictionary indices per row)
            Bool(&'a BooleanArray),
            Other(&'a ArrayRef),
        }

        // OPTIMIZATION: For single column GROUP BY, use direct dictionary indexing
        // This is much faster than hash-based grouping for low-cardinality columns
        if group_cols.len() == 1 {
            if let Some(col) = batch.column_by_name(&group_cols[0]) {
                // FAST PATH 1: Arrow DictionaryArray - indices already available, no conversion needed!
                use arrow::array::DictionaryArray;
                use arrow::datatypes::UInt32Type;
                if let Some(dict_arr) = col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
                    let keys = dict_arr.keys();
                    let values = dict_arr.values();
                    if let Some(str_values) = values.as_any().downcast_ref::<StringArray>() {
                        let dict_size = str_values.len() + 1; // +1 for NULL slot

                        // Extract indices directly - no dictionary building needed!
                        let indices: Vec<u32> = (0..num_rows)
                            .map(|i| {
                                if keys.is_null(i) {
                                    0u32
                                } else {
                                    keys.value(i) + 1
                                } // +1 for NULL at 0
                            })
                            .collect();

                        // Build dict_values from StringArray
                        let dict_values: Vec<&str> =
                            (0..str_values.len()).map(|i| str_values.value(i)).collect();

                        return Self::execute_group_by_string_dict(
                            batch,
                            stmt,
                            str_values,
                            &indices,
                            &dict_values,
                            dict_size,
                        );
                    }
                }

                // FAST PATH 2: Regular StringArray - build dictionary
                // REMOVED sampling to stabilize performance
                if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
                    // Build dictionary directly without sampling
                    let mut dict: AHashMap<&str, u32> = AHashMap::with_capacity(200);
                    let mut dict_values: Vec<&str> = Vec::with_capacity(200);
                    let mut next_id = 1u32;

                    let mut indices: Vec<u32> = Vec::with_capacity(num_rows);
                    indices.resize(num_rows, 0);

                    for i in 0..num_rows {
                        if !str_arr.is_null(i) {
                            let s = str_arr.value(i);
                            let id = *dict.entry(s).or_insert_with(|| {
                                let id = next_id;
                                next_id += 1;
                                dict_values.push(s);
                                id
                            });
                            indices[i] = id;
                        }
                    }

                    // Only use dict indexing if cardinality is reasonable
                    let dict_size = dict_values.len() + 1;
                    if dict_size <= 1000 {
                        return Self::execute_group_by_string_dict(
                            batch,
                            stmt,
                            str_arr,
                            &indices,
                            &dict_values,
                            dict_size,
                        );
                    }
                }
            }
        }

        // FAST PATH: 2-column GROUP BY with low-cardinality string columns
        // Uses composite dictionary indexing: (dict1_id * dict2_size + dict2_id) as direct array index
        if group_cols.len() == 2
            && stmt.columns.iter().all(|column| {
                !matches!(
                    column,
                    SelectColumn::Aggregate {
                        func: AggregateFunc::Min | AggregateFunc::Max,
                        ..
                    }
                )
            })
        {
            use arrow::array::DictionaryArray;
            use arrow::datatypes::UInt32Type;

            let col1 = batch.column_by_name(&group_cols[0]);
            let col2 = batch.column_by_name(&group_cols[1]);

            if let (Some(c1), Some(c2)) = (col1, col2) {
                // Build dictionaries for both columns - handles both StringArray and DictionaryArray
                let build_dict = |col: &ArrayRef,
                                  n_rows: usize|
                 -> Option<(Vec<u32>, Vec<String>, usize)> {
                    // Case 1: DictionaryArray - already dictionary encoded!
                    if let Some(dict_arr) =
                        col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>()
                    {
                        let keys = dict_arr.keys();
                        let values = dict_arr.values();
                        if let Some(str_values) = values.as_any().downcast_ref::<StringArray>() {
                            let dict_size = str_values.len() + 1;
                            if dict_size <= 1000 {
                                let indices: Vec<u32> = (0..n_rows)
                                    .map(|i| {
                                        if keys.is_null(i) {
                                            0u32
                                        } else {
                                            keys.value(i) + 1
                                        }
                                    })
                                    .collect();
                                let dict_values: Vec<String> = (0..str_values.len())
                                    .map(|i| str_values.value(i).to_string())
                                    .collect();
                                return Some((indices, dict_values, dict_size));
                            }
                        }
                    }

                    // Case 2: StringArray - build dictionary
                    if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
                        let mut dict: AHashMap<&str, u32> = AHashMap::with_capacity(200);
                        let mut dict_values: Vec<String> = Vec::with_capacity(200);
                        let mut next_id = 1u32;

                        let indices: Vec<u32> = (0..n_rows)
                            .map(|i| {
                                if str_arr.is_null(i) {
                                    0u32
                                } else {
                                    let s = str_arr.value(i);
                                    *dict.entry(s).or_insert_with(|| {
                                        let id = next_id;
                                        next_id += 1;
                                        dict_values.push(s.to_string());
                                        id
                                    })
                                }
                            })
                            .collect();

                        let dict_size = dict_values.len() + 1;
                        if dict_size <= 1000 {
                            return Some((indices, dict_values, dict_size));
                        }
                    }

                    // Case 3: LargeStringArray - build dictionary
                    if let Some(str_arr) = col
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                    {
                        let mut dict: AHashMap<String, u32> = AHashMap::with_capacity(200);
                        let mut dict_values: Vec<String> = Vec::with_capacity(200);
                        let mut next_id = 1u32;

                        let indices: Vec<u32> = (0..n_rows)
                            .map(|i| {
                                if str_arr.is_null(i) {
                                    0u32
                                } else {
                                    let s = str_arr.value(i);
                                    *dict.entry(s.to_string()).or_insert_with(|| {
                                        let id = next_id;
                                        next_id += 1;
                                        dict_values.push(s.to_string());
                                        id
                                    })
                                }
                            })
                            .collect();

                        let dict_size = dict_values.len() + 1;
                        if dict_size <= 1000 {
                            return Some((indices, dict_values, dict_size));
                        }
                    }

                    // Case 4: BinaryArray - build dictionary
                    if let Some(bin_arr) = col.as_any().downcast_ref::<arrow::array::BinaryArray>()
                    {
                        let mut dict: AHashMap<String, u32> = AHashMap::with_capacity(200);
                        let mut dict_values: Vec<String> = Vec::with_capacity(200);
                        let mut next_id = 1u32;

                        let indices: Vec<u32> = (0..n_rows)
                            .map(|i| {
                                if bin_arr.is_null(i) {
                                    0u32
                                } else {
                                    let s = bin_arr.value(i);
                                    let s_str = String::from_utf8_lossy(s);
                                    *dict.entry(s_str.to_string()).or_insert_with(|| {
                                        let id = next_id;
                                        next_id += 1;
                                        dict_values.push(s_str.to_string());
                                        id
                                    })
                                }
                            })
                            .collect();

                        let dict_size = dict_values.len() + 1;
                        if dict_size <= 1000 {
                            return Some((indices, dict_values, dict_size));
                        }
                    }

                    None
                };

                if let (
                    Some((indices1, dict1_values, dict1_size)),
                    Some((indices2, dict2_values, dict2_size)),
                ) = (build_dict(c1, num_rows), build_dict(c2, num_rows))
                {
                    // Use composite key: (idx1 * dict2_size + idx2) for direct array indexing
                    let total_size = dict1_size * dict2_size;
                    if total_size <= 100_000 {
                        // Find aggregate column - support both Int64 and Float64
                        let mut agg_col_float: Option<&Float64Array> = None;
                        let mut agg_col_int: Option<&Int64Array> = None;
                        for col in &stmt.columns {
                            if let SelectColumn::Aggregate {
                                column: Some(col_name),
                                ..
                            } = col
                            {
                                let actual_col = col_name.trim_matches('"');
                                let actual_col = if let Some(dot_pos) = actual_col.rfind('.') {
                                    &actual_col[dot_pos + 1..]
                                } else {
                                    actual_col
                                };
                                if actual_col != "*" {
                                    if let Some(arr) = batch.column_by_name(actual_col) {
                                        if let Some(float_arr) =
                                            arr.as_any().downcast_ref::<Float64Array>()
                                        {
                                            agg_col_float = Some(float_arr);
                                        } else if let Some(int_arr) =
                                            arr.as_any().downcast_ref::<Int64Array>()
                                        {
                                            agg_col_int = Some(int_arr);
                                        }
                                    }
                                }
                                break;
                            }
                        }

                        // Direct-indexed aggregation - no hash map needed!
                        let mut counts: Vec<i64> = vec![0; total_size];
                        let mut sums_int: Vec<i64> = vec![0; total_size];
                        let mut sums_float: Vec<f64> = vec![0.0; total_size];

                        if let Some(int_arr) = agg_col_int {
                            // Int64 aggregate.  NULL group keys (idx 0) form
                            // their own group: composite = idx1*dict2_size+idx2
                            // already maps (0, x), (x, 0) and (0, 0) to the
                            // NULL-group slots, so every row participates.
                            if int_arr.null_count() == 0 {
                                let values = int_arr.values();
                                for row_idx in 0..num_rows {
                                    let idx1 = unsafe { *indices1.get_unchecked(row_idx) as usize };
                                    let idx2 = unsafe { *indices2.get_unchecked(row_idx) as usize };
                                    let composite = idx1 * dict2_size + idx2;
                                    unsafe {
                                        *counts.get_unchecked_mut(composite) += 1;
                                        *sums_int.get_unchecked_mut(composite) +=
                                            *values.get_unchecked(row_idx);
                                    }
                                }
                            } else {
                                for row_idx in 0..num_rows {
                                    let idx1 = indices1[row_idx] as usize;
                                    let idx2 = indices2[row_idx] as usize;
                                    let composite = idx1 * dict2_size + idx2;
                                    counts[composite] += 1;
                                    if !int_arr.is_null(row_idx) {
                                        sums_int[composite] += int_arr.value(row_idx);
                                    }
                                }
                            }
                        } else if let Some(float_arr) = agg_col_float {
                            // Float64 aggregate (NULL group keys included)
                            if float_arr.null_count() == 0 {
                                let values = float_arr.values();
                                for row_idx in 0..num_rows {
                                    let idx1 = unsafe { *indices1.get_unchecked(row_idx) as usize };
                                    let idx2 = unsafe { *indices2.get_unchecked(row_idx) as usize };
                                    let composite = idx1 * dict2_size + idx2;
                                    unsafe {
                                        *counts.get_unchecked_mut(composite) += 1;
                                        *sums_float.get_unchecked_mut(composite) +=
                                            *values.get_unchecked(row_idx);
                                    }
                                }
                            } else {
                                for row_idx in 0..num_rows {
                                    let idx1 = indices1[row_idx] as usize;
                                    let idx2 = indices2[row_idx] as usize;
                                    let composite = idx1 * dict2_size + idx2;
                                    counts[composite] += 1;
                                    if !float_arr.is_null(row_idx) {
                                        sums_float[composite] += float_arr.value(row_idx);
                                    }
                                }
                            }
                        } else {
                            // COUNT(*) only
                            for row_idx in 0..num_rows {
                                let idx1 = unsafe { *indices1.get_unchecked(row_idx) as usize };
                                let idx2 = unsafe { *indices2.get_unchecked(row_idx) as usize };
                                let composite = idx1 * dict2_size + idx2;
                                unsafe {
                                    *counts.get_unchecked_mut(composite) += 1;
                                }
                            }
                        }

                        // Collect active groups, emitting NULL for the idx-0 slots.
                        let mut result_col1: Vec<Option<&str>> =
                            Vec::with_capacity(dict1_size * dict2_size / 10);
                        let mut result_col2: Vec<Option<&str>> =
                            Vec::with_capacity(dict1_size * dict2_size / 10);
                        let mut result_counts: Vec<i64> =
                            Vec::with_capacity(dict1_size * dict2_size / 10);
                        let mut result_sums_int: Vec<i64> =
                            Vec::with_capacity(dict1_size * dict2_size / 10);
                        let mut result_sums_float: Vec<f64> =
                            Vec::with_capacity(dict1_size * dict2_size / 10);

                        for idx1 in 0..dict1_size {
                            for idx2 in 0..dict2_size {
                                let composite = idx1 * dict2_size + idx2;
                                if counts[composite] > 0 {
                                    result_col1.push(if idx1 == 0 {
                                        None
                                    } else {
                                        Some(dict1_values[idx1 - 1].as_str())
                                    });
                                    result_col2.push(if idx2 == 0 {
                                        None
                                    } else {
                                        Some(dict2_values[idx2 - 1].as_str())
                                    });
                                    result_counts.push(counts[composite]);
                                    result_sums_int.push(sums_int[composite]);
                                    result_sums_float.push(sums_float[composite]);
                                }
                            }
                        }

                        // Build result
                        use crate::query::AggregateFunc;
                        let mut result_fields: Vec<Field> = Vec::new();
                        let mut result_arrays: Vec<ArrayRef> = Vec::new();
                        result_fields.push(Field::new(
                            Self::group_output_name(stmt, &group_cols[0]),
                            ArrowDataType::Utf8,
                            true,
                        ));
                        result_arrays.push(Arc::new(StringArray::from(result_col1)));
                        result_fields.push(Field::new(
                            Self::group_output_name(stmt, &group_cols[1]),
                            ArrowDataType::Utf8,
                            true,
                        ));
                        result_arrays.push(Arc::new(StringArray::from(result_col2)));
                        let has_int_agg = agg_col_int.is_some();
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
                                let field_name = alias.clone().unwrap_or_else(|| {
                                    format!("{}({})", fn_name, column.as_deref().unwrap_or("*"))
                                });
                                match func {
                                    AggregateFunc::Count => {
                                        result_fields.push(Field::new(
                                            &field_name,
                                            ArrowDataType::Int64,
                                            false,
                                        ));
                                        result_arrays.push(Arc::new(Int64Array::from(
                                            result_counts.clone(),
                                        )));
                                    }
                                    AggregateFunc::Sum => {
                                        if has_int_agg {
                                            result_fields.push(Field::new(
                                                &field_name,
                                                ArrowDataType::Int64,
                                                true,
                                            ));
                                            result_arrays.push(Arc::new(Int64Array::from(
                                                result_sums_int.clone(),
                                            )));
                                        } else {
                                            result_fields.push(Field::new(
                                                &field_name,
                                                ArrowDataType::Float64,
                                                true,
                                            ));
                                            result_arrays.push(Arc::new(Float64Array::from(
                                                result_sums_float.clone(),
                                            )));
                                        }
                                    }
                                    AggregateFunc::Avg => {
                                        let avgs: Vec<f64> =
                                            if has_int_agg {
                                                result_counts
                                                    .iter()
                                                    .zip(result_sums_int.iter())
                                                    .map(|(&c, &s)| {
                                                        if c > 0 {
                                                            s as f64 / c as f64
                                                        } else {
                                                            0.0
                                                        }
                                                    })
                                                    .collect()
                                            } else {
                                                result_counts
                                                    .iter()
                                                    .zip(result_sums_float.iter())
                                                    .map(
                                                        |(&c, &s)| {
                                                            if c > 0 {
                                                                s / c as f64
                                                            } else {
                                                                0.0
                                                            }
                                                        },
                                                    )
                                                    .collect()
                                            };
                                        result_fields.push(Field::new(
                                            &field_name,
                                            ArrowDataType::Float64,
                                            true,
                                        ));
                                        result_arrays.push(Arc::new(Float64Array::from(avgs)));
                                    }
                                    _ => {}
                                }
                            }
                        }
                        let schema = Arc::new(Schema::new(result_fields));
                        let mut result_batch = RecordBatch::try_new(schema, result_arrays)
                            .map_err(|e| err_data(e.to_string()))?;

                        // Apply HAVING/ORDER BY/LIMIT
                        if let Some(ref having) = stmt.having {
                            let mask = Self::evaluate_predicate(&result_batch, having)?;
                            result_batch = compute::filter_record_batch(&result_batch, &mask)
                                .map_err(|e| err_data(e.to_string()))?;
                        }

                        if !stmt.order_by.is_empty() {
                            let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
                            result_batch =
                                Self::apply_order_by_topk(&result_batch, &stmt.order_by, k)?;
                        }

                        result_batch =
                            Self::apply_limit_offset(&result_batch, stmt.limit, stmt.offset)?;

                        return Ok(ApexResult::Data(result_batch));
                    }
                }
            }
        }

        // FAST PATH: String + Int64 2-column GROUP BY (common case: category + numeric id)
        // Uses composite key: (string_dict_id * int_range + int_value_offset) for direct array indexing
        if group_cols.len() == 2 {
            let col1 = batch.column_by_name(&group_cols[0]);
            let col2 = batch.column_by_name(&group_cols[1]);

            if let (Some(c1), Some(c2)) = (col1, col2) {
                // Try to build dictionary for string column and get int range for int column
                let string_dict_result: Option<(Vec<u32>, Vec<String>, usize)> = {
                    use arrow::array::DictionaryArray;
                    use arrow::datatypes::UInt32Type;

                    // Case 1: DictionaryArray
                    if let Some(dict_arr) =
                        c1.as_any().downcast_ref::<DictionaryArray<UInt32Type>>()
                    {
                        let keys = dict_arr.keys();
                        let values = dict_arr.values();
                        if let Some(str_values) = values.as_any().downcast_ref::<StringArray>() {
                            let dict_size = str_values.len() + 1;
                            if dict_size <= 1000 {
                                let indices: Vec<u32> = (0..num_rows)
                                    .map(|i| {
                                        if keys.is_null(i) {
                                            0u32
                                        } else {
                                            keys.value(i) + 1
                                        }
                                    })
                                    .collect();
                                let dict_values: Vec<String> = (0..str_values.len())
                                    .map(|i| str_values.value(i).to_string())
                                    .collect();
                                Some((indices, dict_values, dict_size))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    // Case 2: StringArray - build dictionary
                    else if let Some(str_arr) = c1.as_any().downcast_ref::<StringArray>() {
                        let mut dict: AHashMap<&str, u32> = AHashMap::with_capacity(200);
                        let mut dict_values: Vec<String> = Vec::with_capacity(200);
                        let mut next_id = 1u32;

                        let indices: Vec<u32> = (0..num_rows)
                            .map(|i| {
                                if str_arr.is_null(i) {
                                    0u32
                                } else {
                                    let s = str_arr.value(i);
                                    *dict.entry(s).or_insert_with(|| {
                                        let id = next_id;
                                        next_id += 1;
                                        dict_values.push(s.to_string());
                                        id
                                    })
                                }
                            })
                            .collect();

                        let dict_size = dict_values.len() + 1;
                        if dict_size <= 1000 {
                            Some((indices, dict_values, dict_size))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                };

                // Get int column range
                let int_range_result: Option<(Vec<u32>, i64, usize)> =
                    if let Some(int_arr) = c2.as_any().downcast_ref::<Int64Array>() {
                        let (min_val, max_val) = {
                            let mut min = i64::MAX;
                            let mut max = i64::MIN;
                            for i in 0..num_rows {
                                if !int_arr.is_null(i) {
                                    let v = int_arr.value(i);
                                    min = min.min(v);
                                    max = max.max(v);
                                }
                            }
                            (min, max)
                        };

                        let range = (max_val - min_val + 1) as usize;
                        if min_val >= 0 && range <= 1000 && range > 0 {
                            let indices: Vec<u32> = (0..num_rows)
                                .map(|i| {
                                    if int_arr.is_null(i) {
                                        0u32
                                    } else {
                                        (int_arr.value(i) - min_val + 1) as u32
                                    }
                                })
                                .collect();
                            Some((indices, min_val, range))
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                // If both columns can use dictionary indexing
                if let (
                    Some((str_indices, str_values, str_size)),
                    Some((int_indices, int_min, int_range)),
                ) = (string_dict_result, int_range_result)
                {
                    let total_size = str_size * (int_range + 1);
                    if total_size <= 100_000 {
                        // Find aggregate column
                        let mut agg_col_int: Option<&Int64Array> = None;
                        let mut agg_col_float: Option<&Float64Array> = None;
                        for col in &stmt.columns {
                            if let SelectColumn::Aggregate {
                                column: Some(col_name),
                                ..
                            } = col
                            {
                                let actual_col = col_name.trim_matches('"');
                                let actual_col = if let Some(dot_pos) = actual_col.rfind('.') {
                                    &actual_col[dot_pos + 1..]
                                } else {
                                    actual_col
                                };
                                if actual_col != "*" {
                                    if let Some(arr) = batch.column_by_name(actual_col) {
                                        if let Some(float_arr) =
                                            arr.as_any().downcast_ref::<Float64Array>()
                                        {
                                            agg_col_float = Some(float_arr);
                                        } else if let Some(int_arr) =
                                            arr.as_any().downcast_ref::<Int64Array>()
                                        {
                                            agg_col_int = Some(int_arr);
                                        }
                                    }
                                }
                                break;
                            }
                        }

                        // Direct-indexed aggregation
                        let mut counts: Vec<i64> = vec![0; total_size];
                        let mut sums_int: Vec<i64> = vec![0; total_size];
                        let mut sums_float: Vec<f64> = vec![0.0; total_size];

                        if let Some(int_arr) = agg_col_int {
                            // NULL group keys (idx 0) form their own group;
                            // composite = str_idx*(int_range+1)+int_idx maps
                            // every combination including NULL slots.
                            if int_arr.null_count() == 0 {
                                let values = int_arr.values();
                                for row_idx in 0..num_rows {
                                    let str_idx =
                                        unsafe { *str_indices.get_unchecked(row_idx) as usize };
                                    let int_idx =
                                        unsafe { *int_indices.get_unchecked(row_idx) as usize };
                                    let composite = str_idx * (int_range + 1) + int_idx;
                                    unsafe {
                                        *counts.get_unchecked_mut(composite) += 1;
                                        *sums_int.get_unchecked_mut(composite) +=
                                            *values.get_unchecked(row_idx);
                                    }
                                }
                            } else {
                                for row_idx in 0..num_rows {
                                    let str_idx = str_indices[row_idx] as usize;
                                    let int_idx = int_indices[row_idx] as usize;
                                    let composite = str_idx * (int_range + 1) + int_idx;
                                    counts[composite] += 1;
                                    if !int_arr.is_null(row_idx) {
                                        sums_int[composite] += int_arr.value(row_idx);
                                    }
                                }
                            }
                        } else if let Some(float_arr) = agg_col_float {
                            if float_arr.null_count() == 0 {
                                let values = float_arr.values();
                                for row_idx in 0..num_rows {
                                    let str_idx =
                                        unsafe { *str_indices.get_unchecked(row_idx) as usize };
                                    let int_idx =
                                        unsafe { *int_indices.get_unchecked(row_idx) as usize };
                                    let composite = str_idx * (int_range + 1) + int_idx;
                                    unsafe {
                                        *counts.get_unchecked_mut(composite) += 1;
                                        *sums_float.get_unchecked_mut(composite) +=
                                            *values.get_unchecked(row_idx);
                                    }
                                }
                            } else {
                                for row_idx in 0..num_rows {
                                    let str_idx = str_indices[row_idx] as usize;
                                    let int_idx = int_indices[row_idx] as usize;
                                    let composite = str_idx * (int_range + 1) + int_idx;
                                    counts[composite] += 1;
                                    if !float_arr.is_null(row_idx) {
                                        sums_float[composite] += float_arr.value(row_idx);
                                    }
                                }
                            }
                        } else {
                            // COUNT(*) only
                            for row_idx in 0..num_rows {
                                let str_idx =
                                    unsafe { *str_indices.get_unchecked(row_idx) as usize };
                                let int_idx =
                                    unsafe { *int_indices.get_unchecked(row_idx) as usize };
                                let composite = str_idx * (int_range + 1) + int_idx;
                                unsafe {
                                    *counts.get_unchecked_mut(composite) += 1;
                                }
                            }
                        }

                        // Collect active groups, emitting NULL for the idx-0 slots.
                        let mut result_col1: Vec<Option<&str>> = Vec::with_capacity(total_size / 10);
                        let mut result_col2: Vec<Option<i64>> = Vec::with_capacity(total_size / 10);
                        let mut result_counts: Vec<i64> = Vec::with_capacity(total_size / 10);
                        let mut result_sums_int: Vec<i64> = Vec::with_capacity(total_size / 10);
                        let mut result_sums_float: Vec<f64> = Vec::with_capacity(total_size / 10);

                        for str_idx in 0..str_size {
                            for int_offset in 0..=int_range {
                                let composite = str_idx * (int_range + 1) + int_offset;
                                if counts[composite] > 0 {
                                    result_col1.push(if str_idx == 0 {
                                        None
                                    } else {
                                        Some(str_values[str_idx - 1].as_str())
                                    });
                                    result_col2.push(if int_offset == 0 {
                                        None
                                    } else {
                                        Some(int_min + (int_offset - 1) as i64)
                                    });
                                    result_counts.push(counts[composite]);
                                    result_sums_int.push(sums_int[composite]);
                                    result_sums_float.push(sums_float[composite]);
                                }
                            }
                        }

                        // Build result
                        use crate::query::AggregateFunc;
                        let mut result_fields: Vec<Field> = Vec::new();
                        let mut result_arrays: Vec<ArrayRef> = Vec::new();

                        result_fields.push(Field::new(
                            Self::group_output_name(stmt, &group_cols[0]),
                            ArrowDataType::Utf8,
                            true,
                        ));
                        result_arrays.push(Arc::new(StringArray::from(result_col1)));
                        result_fields.push(Field::new(
                            Self::group_output_name(stmt, &group_cols[1]),
                            ArrowDataType::Int64,
                            true,
                        ));
                        result_arrays.push(Arc::new(Int64Array::from(result_col2)));

                        let has_int_agg = agg_col_int.is_some();

                        for col in &stmt.columns {
                            if let SelectColumn::Aggregate {
                                func,
                                column,
                                alias,
                                ..
                            } = col
                            {
                                let func_name = match func {
                                    AggregateFunc::Count => "COUNT",
                                    AggregateFunc::Sum => "SUM",
                                    AggregateFunc::Avg => "AVG",
                                    AggregateFunc::Min => "MIN",
                                    AggregateFunc::Max => "MAX",
                                };
                                let field_name = alias.clone().unwrap_or_else(|| {
                                    format!("{}({})", func_name, column.as_deref().unwrap_or("*"))
                                });

                                match func {
                                    AggregateFunc::Count => {
                                        result_fields.push(Field::new(
                                            &field_name,
                                            ArrowDataType::Int64,
                                            false,
                                        ));
                                        result_arrays.push(Arc::new(Int64Array::from(
                                            result_counts.clone(),
                                        )));
                                    }
                                    AggregateFunc::Sum => {
                                        if has_int_agg {
                                            result_fields.push(Field::new(
                                                &field_name,
                                                ArrowDataType::Int64,
                                                true,
                                            ));
                                            result_arrays.push(Arc::new(Int64Array::from(
                                                result_sums_int.clone(),
                                            )));
                                        } else {
                                            result_fields.push(Field::new(
                                                &field_name,
                                                ArrowDataType::Float64,
                                                true,
                                            ));
                                            result_arrays.push(Arc::new(Float64Array::from(
                                                result_sums_float.clone(),
                                            )));
                                        }
                                    }
                                    AggregateFunc::Avg => {
                                        let avgs: Vec<f64> =
                                            if has_int_agg {
                                                result_counts
                                                    .iter()
                                                    .zip(result_sums_int.iter())
                                                    .map(|(&c, &s)| {
                                                        if c > 0 {
                                                            s as f64 / c as f64
                                                        } else {
                                                            0.0
                                                        }
                                                    })
                                                    .collect()
                                            } else {
                                                result_counts
                                                    .iter()
                                                    .zip(result_sums_float.iter())
                                                    .map(
                                                        |(&c, &s)| {
                                                            if c > 0 {
                                                                s / c as f64
                                                            } else {
                                                                0.0
                                                            }
                                                        },
                                                    )
                                                    .collect()
                                            };
                                        result_fields.push(Field::new(
                                            &field_name,
                                            ArrowDataType::Float64,
                                            true,
                                        ));
                                        result_arrays.push(Arc::new(Float64Array::from(avgs)));
                                    }
                                    _ => {}
                                }
                            }
                        }

                        let schema = Arc::new(Schema::new(result_fields));
                        let mut result_batch = RecordBatch::try_new(schema, result_arrays)
                            .map_err(|e| err_data(e.to_string()))?;

                        // Apply HAVING/ORDER BY/LIMIT
                        if let Some(ref having) = stmt.having {
                            let mask = Self::evaluate_predicate(&result_batch, having)?;
                            result_batch = compute::filter_record_batch(&result_batch, &mask)
                                .map_err(|e| err_data(e.to_string()))?;
                        }

                        if !stmt.order_by.is_empty() {
                            let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
                            result_batch =
                                Self::apply_order_by_topk(&result_batch, &stmt.order_by, k)?;
                        }

                        result_batch =
                            Self::apply_limit_offset(&result_batch, stmt.limit, stmt.offset)?;

                        return Ok(ApexResult::Data(result_batch));
                    }
                }
            }
        }

        // FAST PATH: Multi-column GROUP BY (3+ columns) using vectorized execution
        // This is faster than the general path because it uses pre-typed columns and batch processing
        if group_cols.len() >= 3 {
            use crate::query::multi_column::{
                build_multi_column_result, execute_multi_column_group_by,
            };

            // Extract aggregate function info
            let (agg_func, agg_col_name) = stmt
                .columns
                .iter()
                .find_map(|col| {
                    if let SelectColumn::Aggregate { func, column, .. } = col {
                        Some((func.clone(), column.as_deref()))
                    } else {
                        None
                    }
                })
                .unwrap_or((crate::query::AggregateFunc::Count, None));

            // Execute optimized multi-column group by
            match execute_multi_column_group_by(batch, group_cols, agg_col_name) {
                Ok(hash_agg) => {
                    let result_batch = build_multi_column_result(
                        &hash_agg,
                        batch,
                        group_cols,
                        Some(agg_func),
                        agg_col_name,
                    )?;

                    // Apply HAVING if present
                    let mut result = result_batch;
                    if let Some(having_expr) = &stmt.having {
                        let mask = Self::evaluate_predicate(&result, having_expr)?;
                        result = compute::filter_record_batch(&result, &mask)
                            .map_err(|e| err_data(e.to_string()))?;
                    }

                    // Apply ORDER BY with top-k optimization
                    if !stmt.order_by.is_empty() {
                        let resolved_ob =
                            Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
                        let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
                        result = Self::apply_order_by_topk(&result, &resolved_ob, k)?;
                    }

                    // Apply LIMIT + OFFSET
                    if stmt.limit.is_some() || stmt.offset.is_some() {
                        result = Self::apply_limit_offset(&result, stmt.limit, stmt.offset)?;
                    }

                    return Ok(ApexResult::Data(result));
                }
                Err(_) => {
                    // Fall through to general path
                }
            }
        }

        let typed_group_cols: Vec<Option<TypedCol>> = group_cols
            .iter()
            .map(|col_name| {
                batch.column_by_name(col_name).map(|col| {
                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        TypedCol::Int64(arr)
                    } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        // Build runtime dictionary: string -> unique ID
                        // This converts O(string_len) hashing to O(1) integer operations
                        let mut dict: AHashMap<&str, u32> = AHashMap::with_capacity(1000);
                        let mut next_id = 1u32; // 0 reserved for NULL
                        let indices: Vec<u32> = (0..num_rows)
                            .map(|i| {
                                if arr.is_null(i) {
                                    0u32
                                } else {
                                    let s = arr.value(i);
                                    *dict.entry(s).or_insert_with(|| {
                                        let id = next_id;
                                        next_id += 1;
                                        id
                                    })
                                }
                            })
                            .collect();
                        TypedCol::StringDict(col, indices)
                    } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                        TypedCol::Float64(arr)
                    } else if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                        TypedCol::Bool(arr)
                    } else if let Some(arr) = col.as_any().downcast_ref::<
                        arrow::array::DictionaryArray<arrow::datatypes::UInt32Type>,
                    >() {
                        // Dictionary-encoded string: reuse the existing key ids
                        // (+1 for the NULL slot) instead of building a runtime
                        // dictionary and per-row string hashing.
                        let indices: Vec<u32> = (0..num_rows)
                            .map(|i| {
                                if arr.keys().is_null(i) {
                                    0u32
                                } else {
                                    arr.keys().value(i) + 1
                                }
                            })
                            .collect();
                        TypedCol::StringDict(col, indices)
                    } else {
                        TypedCol::Other(col)
                    }
                })
            })
            .collect();

        // Find aggregate columns for incremental updates
        let mut agg_col_int: Option<&Int64Array> = None;
        let mut agg_col_float: Option<&Float64Array> = None;

        for col in &stmt.columns {
            if let SelectColumn::Aggregate {
                column: Some(col_name),
                ..
            } = col
            {
                let actual_col = col_name.trim_matches('"');
                let actual_col = if let Some(dot_pos) = actual_col.rfind('.') {
                    &actual_col[dot_pos + 1..]
                } else {
                    actual_col
                };
                if actual_col != "*" {
                    if let Some(arr) = batch.column_by_name(actual_col) {
                        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                            agg_col_int = Some(int_arr);
                        } else if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>()
                        {
                            agg_col_float = Some(float_arr);
                        }
                    }
                }
                break;
            }
        }

        // Pre-compute all group keys (row_idx -> hash) for fast parallel access
        // OPTIMIZATION: Parallel hash computation for large datasets
        use rayon::prelude::*;
        let group_keys: Vec<u64> = if num_rows > 50_000 {
            // Parallel hash computation
            (0..num_rows)
                .into_par_iter()
                .map(|row_idx| {
                    let mut hasher = AHasher::default();
                    for col_opt in &typed_group_cols {
                        match col_opt {
                            Some(TypedCol::Int64(arr)) => {
                                if !arr.is_null(row_idx) {
                                    hasher.write_i64(arr.value(row_idx));
                                } else {
                                    hasher.write_u8(0);
                                }
                            }
                            Some(TypedCol::StringDict(_arr, indices)) => {
                                hasher.write_u32(indices[row_idx]);
                            }
                            Some(TypedCol::Float64(arr)) => {
                                if !arr.is_null(row_idx) {
                                    hasher.write_u64(arr.value(row_idx).to_bits());
                                } else {
                                    hasher.write_u8(0);
                                }
                            }
                            Some(TypedCol::Bool(arr)) => {
                                if !arr.is_null(row_idx) {
                                    hasher.write_u8(arr.value(row_idx) as u8);
                                } else {
                                    hasher.write_u8(2);
                                }
                            }
                            Some(TypedCol::Other(col)) => {
                                hasher.write_u64(Self::hash_array_value_fast(col, row_idx));
                            }
                            None => {}
                        }
                    }
                    hasher.finish()
                })
                .collect()
        } else {
            // Sequential for small datasets
            (0..num_rows)
                .map(|row_idx| {
                    let mut hasher = AHasher::default();
                    for col_opt in &typed_group_cols {
                        match col_opt {
                            Some(TypedCol::Int64(arr)) => {
                                if !arr.is_null(row_idx) {
                                    hasher.write_i64(arr.value(row_idx));
                                } else {
                                    hasher.write_u8(0);
                                }
                            }
                            Some(TypedCol::StringDict(_arr, indices)) => {
                                hasher.write_u32(indices[row_idx]);
                            }
                            Some(TypedCol::Float64(arr)) => {
                                if !arr.is_null(row_idx) {
                                    hasher.write_u64(arr.value(row_idx).to_bits());
                                } else {
                                    hasher.write_u8(0);
                                }
                            }
                            Some(TypedCol::Bool(arr)) => {
                                if !arr.is_null(row_idx) {
                                    hasher.write_u8(arr.value(row_idx) as u8);
                                } else {
                                    hasher.write_u8(2);
                                }
                            }
                            Some(TypedCol::Other(col)) => {
                                hasher.write_u64(Self::hash_array_value_fast(col, row_idx));
                            }
                            None => {}
                        }
                    }
                    hasher.finish()
                })
                .collect()
        };

        // Pre-compute aggregate values for parallel access
        let agg_int_vals: Option<Vec<Option<i64>>> = agg_col_int.map(|arr| {
            (0..num_rows)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect()
        });
        let agg_float_vals: Option<Vec<Option<f64>>> = agg_col_float.map(|arr| {
            (0..num_rows)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect()
        });

        // Parallel partitioned aggregation for large datasets
        use rayon::prelude::*;
        let use_parallel = num_rows > 50_000;

        let groups: AHashMap<u64, GroupState> = if use_parallel {
            let num_partitions = rayon::current_num_threads().max(4);
            let partition_size = (num_rows + num_partitions - 1) / num_partitions;

            // Each partition aggregates independently
            let partition_results: Vec<AHashMap<u64, GroupState>> = (0..num_partitions)
                .into_par_iter()
                .map(|p| {
                    let start = p * partition_size;
                    let end = ((p + 1) * partition_size).min(num_rows);
                    let mut local: AHashMap<u64, GroupState> =
                        AHashMap::with_capacity(estimated_groups / num_partitions + 1);

                    for row_idx in start..end {
                        let key = group_keys[row_idx];
                        let state = local.entry(key).or_insert_with(|| GroupState::new(row_idx));
                        state.count += 1;

                        if let Some(ref vals) = agg_int_vals {
                            if let Some(val) = vals[row_idx] {
                                state.sum_int = state.sum_int.wrapping_add(val);
                                state.min_int = Some(state.min_int.map_or(val, |m| m.min(val)));
                                state.max_int = Some(state.max_int.map_or(val, |m| m.max(val)));
                            }
                        }
                        if let Some(ref vals) = agg_float_vals {
                            if let Some(val) = vals[row_idx] {
                                state.sum_float += val;
                                state.min_float = Some(state.min_float.map_or(val, |m| m.min(val)));
                                state.max_float = Some(state.max_float.map_or(val, |m| m.max(val)));
                            }
                        }
                    }
                    local
                })
                .collect();

            // Merge partition results
            let mut merged: AHashMap<u64, GroupState> = AHashMap::with_capacity(estimated_groups);
            for local in partition_results {
                for (key, state) in local {
                    merged
                        .entry(key)
                        .and_modify(|e| {
                            e.count += state.count;
                            e.sum_int = e.sum_int.wrapping_add(state.sum_int);
                            e.sum_float += state.sum_float;
                            if let Some(v) = state.min_int {
                                e.min_int = Some(e.min_int.map_or(v, |m| m.min(v)));
                            }
                            if let Some(v) = state.max_int {
                                e.max_int = Some(e.max_int.map_or(v, |m| m.max(v)));
                            }
                            if let Some(v) = state.min_float {
                                e.min_float = Some(e.min_float.map_or(v, |m| m.min(v)));
                            }
                            if let Some(v) = state.max_float {
                                e.max_float = Some(e.max_float.map_or(v, |m| m.max(v)));
                            }
                        })
                        .or_insert(state);
                }
            }
            merged
        } else {
            // Sequential for small datasets
            let mut groups: AHashMap<u64, GroupState> = AHashMap::with_capacity(estimated_groups);
            for row_idx in 0..num_rows {
                let key = group_keys[row_idx];
                let state = groups
                    .entry(key)
                    .or_insert_with(|| GroupState::new(row_idx));
                state.count += 1;

                if let Some(ref vals) = agg_int_vals {
                    if let Some(val) = vals[row_idx] {
                        state.sum_int = state.sum_int.wrapping_add(val);
                        state.min_int = Some(state.min_int.map_or(val, |m| m.min(val)));
                        state.max_int = Some(state.max_int.map_or(val, |m| m.max(val)));
                    }
                }
                if let Some(ref vals) = agg_float_vals {
                    if let Some(val) = vals[row_idx] {
                        state.sum_float += val;
                        state.min_float = Some(state.min_float.map_or(val, |m| m.min(val)));
                        state.max_float = Some(state.max_float.map_or(val, |m| m.max(val)));
                    }
                }
            }
            groups
        };

        // Build result arrays from group states
        let num_groups = groups.len();
        let states: Vec<GroupState> = groups.into_values().collect();

        let mut result_fields: Vec<Field> = Vec::new();
        let mut result_arrays: Vec<ArrayRef> = Vec::new();

        for col in &stmt.columns {
            match col {
                SelectColumn::Column(name) | SelectColumn::ColumnAlias { column: name, .. } => {
                    let col_name = name.trim_matches('"');
                    let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                        &col_name[dot_pos + 1..]
                    } else {
                        col_name
                    };
                    let output_name = match col {
                        SelectColumn::ColumnAlias { alias, .. } => alias.as_str(),
                        _ => actual_col,
                    };

                    if let Some(src_col) = batch.column_by_name(actual_col) {
                        // Take value from first row of each group
                        let first_indices: Vec<usize> =
                            states.iter().map(|s| s.first_row).collect();
                        let indices_arr = arrow::array::UInt32Array::from(
                            first_indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
                        );
                        let taken = compute::take(src_col.as_ref(), &indices_arr, None)
                            .map_err(|e| err_data(e.to_string()))?;
                        result_fields.push(Field::new(
                            output_name,
                            taken.data_type().clone(),
                            true,
                        ));
                        result_arrays.push(taken);
                    }
                }
                SelectColumn::Aggregate {
                    func,
                    column,
                    alias,
                    ..
                } => {
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
                    let has_int = agg_col_int.is_some();
                    match func {
                        AggregateFunc::Count => {
                            result_fields.push(Field::new(
                                &output_name,
                                ArrowDataType::Int64,
                                false,
                            ));
                            result_arrays.push(Arc::new(Int64Array::from(
                                states.iter().map(|s| s.count).collect::<Vec<_>>(),
                            )));
                        }
                        AggregateFunc::Sum => {
                            if has_int {
                                result_fields.push(Field::new(
                                    &output_name,
                                    ArrowDataType::Int64,
                                    false,
                                ));
                                result_arrays.push(Arc::new(Int64Array::from(
                                    states.iter().map(|s| s.sum_int).collect::<Vec<_>>(),
                                )));
                            } else {
                                result_fields.push(Field::new(
                                    &output_name,
                                    ArrowDataType::Float64,
                                    false,
                                ));
                                result_arrays.push(Arc::new(Float64Array::from(
                                    states.iter().map(|s| s.sum_float).collect::<Vec<_>>(),
                                )));
                            }
                        }
                        AggregateFunc::Avg => {
                            let avgs: Vec<Option<f64>> = states
                                .iter()
                                .map(|s| {
                                    if s.count > 0 {
                                        Some(if has_int {
                                            s.sum_int as f64 / s.count as f64
                                        } else {
                                            s.sum_float / s.count as f64
                                        })
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            result_fields.push(Field::new(
                                &output_name,
                                ArrowDataType::Float64,
                                true,
                            ));
                            result_arrays.push(Arc::new(Float64Array::from(avgs)));
                        }
                        AggregateFunc::Min => {
                            if has_int {
                                result_fields.push(Field::new(
                                    &output_name,
                                    ArrowDataType::Int64,
                                    true,
                                ));
                                result_arrays.push(Arc::new(Int64Array::from(
                                    states.iter().map(|s| s.min_int).collect::<Vec<_>>(),
                                )));
                            } else {
                                result_fields.push(Field::new(
                                    &output_name,
                                    ArrowDataType::Float64,
                                    true,
                                ));
                                result_arrays.push(Arc::new(Float64Array::from(
                                    states.iter().map(|s| s.min_float).collect::<Vec<_>>(),
                                )));
                            }
                        }
                        AggregateFunc::Max => {
                            if has_int {
                                result_fields.push(Field::new(
                                    &output_name,
                                    ArrowDataType::Int64,
                                    true,
                                ));
                                result_arrays.push(Arc::new(Int64Array::from(
                                    states.iter().map(|s| s.max_int).collect::<Vec<_>>(),
                                )));
                            } else {
                                result_fields.push(Field::new(
                                    &output_name,
                                    ArrowDataType::Float64,
                                    true,
                                ));
                                result_arrays.push(Arc::new(Float64Array::from(
                                    states.iter().map(|s| s.max_float).collect::<Vec<_>>(),
                                )));
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        if result_fields.is_empty() {
            return Ok(ApexResult::Scalar(num_groups as i64));
        }

        let schema = Arc::new(Schema::new(result_fields));
        let mut result =
            RecordBatch::try_new(schema, result_arrays).map_err(|e| err_data(e.to_string()))?;

        // Apply HAVING clause if present
        if let Some(having_expr) = &stmt.having {
            let mask = Self::evaluate_predicate(&result, having_expr)?;
            result = compute::filter_record_batch(&result, &mask)
                .map_err(|e| err_data(e.to_string()))?;
        }

        // Apply ORDER BY with top-k optimization if LIMIT is present
        if !stmt.order_by.is_empty() {
            let resolved_ob = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
            result = Self::apply_order_by_topk(&result, &resolved_ob, k)?;
        }

        // Apply LIMIT + OFFSET
        if stmt.limit.is_some() || stmt.offset.is_some() {
            result = Self::apply_limit_offset(&result, stmt.limit, stmt.offset)?;
        }

        Ok(ApexResult::Data(result))
    }

    pub(in crate::query::executor) fn execute_group_by_with_indices(
        batch: &RecordBatch,
        stmt: &SelectStatement,
        group_cols: &[String],
    ) -> io::Result<ApexResult> {
        // Create groups: key -> row indices (using AHashMap for speed)
        let num_rows = batch.num_rows();
        let estimated_groups = (num_rows / 10).max(16); // Estimate ~10 rows per group
        let mut groups: AHashMap<u64, Vec<usize>> = AHashMap::with_capacity(estimated_groups);

        // OPTIMIZATION: Pre-downcast columns to typed arrays for faster access
        // This avoids repeated dynamic dispatch in the hot loop
        enum TypedColumn<'a> {
            Int64(&'a Int64Array),
            Float64(&'a Float64Array),
            String(&'a StringArray),
            Bool(&'a BooleanArray),
            StringDict(&'a arrow::array::DictionaryArray<arrow::datatypes::UInt32Type>),
            Other(&'a ArrayRef),
        }

        let typed_cols: Vec<Option<TypedColumn>> = group_cols
            .iter()
            .map(|col_name| {
                batch.column_by_name(col_name).map(|col| {
                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        TypedColumn::Int64(arr)
                    } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        TypedColumn::String(arr)
                    } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                        TypedColumn::Float64(arr)
                    } else if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                        TypedColumn::Bool(arr)
                    } else if let Some(arr) = col.as_any().downcast_ref::<
                        arrow::array::DictionaryArray<arrow::datatypes::UInt32Type>,
                    >() {
                        // Dictionary-encoded string: the key id uniquely
                        // identifies the value, so hashing the id is exact and
                        // avoids per-row string hashing / dynamic dispatch.
                        TypedColumn::StringDict(arr)
                    } else {
                        TypedColumn::Other(col)
                    }
                })
            })
            .collect();

        // Build groups with optimized type-specific hashing
        for row_idx in 0..num_rows {
            let mut hasher = AHasher::default();
            for col_opt in &typed_cols {
                match col_opt {
                    Some(TypedColumn::Int64(arr)) => {
                        if !arr.is_null(row_idx) {
                            hasher.write_i64(arr.value(row_idx));
                        } else {
                            hasher.write_u8(0);
                        }
                    }
                    Some(TypedColumn::String(arr)) => {
                        if !arr.is_null(row_idx) {
                            hasher.write(arr.value(row_idx).as_bytes());
                        } else {
                            hasher.write_u8(0);
                        }
                    }
                    Some(TypedColumn::Float64(arr)) => {
                        if !arr.is_null(row_idx) {
                            hasher.write_u64(arr.value(row_idx).to_bits());
                        } else {
                            hasher.write_u8(0);
                        }
                    }
                    Some(TypedColumn::Bool(arr)) => {
                        if !arr.is_null(row_idx) {
                            hasher.write_u8(arr.value(row_idx) as u8);
                        } else {
                            hasher.write_u8(2);
                        }
                    }
                    Some(TypedColumn::StringDict(arr)) => {
                        if !arr.keys().is_null(row_idx) {
                            hasher.write_u32(arr.keys().value(row_idx));
                        } else {
                            hasher.write_u8(0);
                        }
                    }
                    Some(TypedColumn::Other(col)) => {
                        hasher.write_u64(Self::hash_array_value_fast(col, row_idx));
                    }
                    None => {}
                }
            }
            let key = hasher.finish();
            groups
                .entry(key)
                .or_insert_with(|| Vec::with_capacity(16))
                .push(row_idx);
        }

        // Build result arrays
        let mut result_fields: Vec<Field> = Vec::new();
        let mut result_arrays: Vec<ArrayRef> = Vec::new();

        let num_groups = groups.len();
        let group_indices: Vec<Vec<usize>> = groups.into_values().collect();
        let mut aggregate_cache: AHashMap<String, ArrayRef> = AHashMap::new();
        Self::precompute_categorical_conditional_sums(
            &batch,
            stmt,
            &group_indices,
            &mut aggregate_cache,
        )?;
        Self::precompute_shared_percentiles(&batch, stmt, &group_indices, &mut aggregate_cache)?;

        for col in &stmt.columns {
            match col {
                SelectColumn::Column(name) => {
                    let col_name = name.trim_matches('"');
                    // Strip table prefix if present (e.g., "u.tier" -> "tier")
                    let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                        &col_name[dot_pos + 1..]
                    } else {
                        col_name
                    };
                    if let Some(src_col) = batch.column_by_name(actual_col) {
                        let (field, array) =
                            Self::take_first_from_groups(src_col, &group_indices, actual_col)?;
                        result_fields.push(field);
                        result_arrays.push(array);
                    }
                }
                SelectColumn::ColumnAlias { column, alias } => {
                    let col_name = column.trim_matches('"');
                    // Strip table prefix if present
                    let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                        &col_name[dot_pos + 1..]
                    } else {
                        col_name
                    };
                    if let Some(src_col) = batch.column_by_name(actual_col) {
                        let (field, array) =
                            Self::take_first_from_groups(src_col, &group_indices, alias)?;
                        result_fields.push(field);
                        result_arrays.push(array);
                    }
                }
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct,
                    alias,
                } => {
                    let (field, array) = Self::compute_aggregate_for_groups(
                        batch,
                        func,
                        column,
                        alias,
                        &group_indices,
                        *distinct,
                    )?;
                    result_fields.push(field);
                    result_arrays.push(array);
                }
                SelectColumn::Expression { expr, alias } => {
                    // For expressions containing aggregates (like CASE WHEN SUM(x) > 100 THEN ...),
                    // we need to evaluate the expression for each group
                    let (field, array) = Self::evaluate_expr_for_groups(
                        batch,
                        expr,
                        alias.as_deref(),
                        &group_indices,
                        &mut aggregate_cache,
                    )?;
                    result_fields.push(field);
                    result_arrays.push(array);
                }
                _ => {}
            }
        }

        if result_fields.is_empty() {
            return Ok(ApexResult::Scalar(num_groups as i64));
        }

        let schema = Arc::new(Schema::new(result_fields));
        let mut result =
            RecordBatch::try_new(schema, result_arrays).map_err(|e| err_data(e.to_string()))?;

        // Apply HAVING clause if present
        if let Some(having_expr) = &stmt.having {
            let mask = Self::evaluate_predicate(&result, having_expr)?;
            result = compute::filter_record_batch(&result, &mask)
                .map_err(|e| err_data(e.to_string()))?;
        }

        // Apply ORDER BY (top-k when LIMIT is present), then LIMIT/OFFSET.
        if !stmt.order_by.is_empty() {
            let resolved_ob = Self::resolve_order_by_cols(&stmt.columns, &stmt.order_by);
            let k = stmt.limit.map(|l| l + stmt.offset.unwrap_or(0));
            result = Self::apply_order_by_topk(&result, &resolved_ob, k)?;
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            result = Self::apply_limit_offset(&result, stmt.limit, stmt.offset)?;
        }

        Ok(ApexResult::Data(result))
    }

    pub(in crate::query::executor) fn precompute_categorical_conditional_sums(
        batch: &RecordBatch,
        stmt: &SelectStatement,
        groups: &[Vec<usize>],
        cache: &mut AHashMap<String, ArrayRef>,
    ) -> io::Result<()> {
        fn numeric_literal(expr: &SqlExpr) -> Option<f64> {
            match expr {
                SqlExpr::Literal(Value::Int64(value)) => Some(*value as f64),
                SqlExpr::Literal(Value::Float64(value)) => Some(*value),
                _ => None,
            }
        }

        fn categorical_sum(expr: &SqlExpr) -> Option<(String, String, String)> {
            let SqlExpr::Function { name, args } = expr else {
                return None;
            };
            if !name.eq_ignore_ascii_case("SUM") || args.len() != 1 {
                return None;
            }
            let condition = match &args[0] {
                SqlExpr::Function { name, args }
                    if name.eq_ignore_ascii_case("IF")
                        && args.len() == 3
                        && numeric_literal(&args[1]) == Some(1.0)
                        && numeric_literal(&args[2]) == Some(0.0) =>
                {
                    &args[0]
                }
                SqlExpr::Case {
                    when_then,
                    else_expr,
                } if when_then.len() == 1
                    && numeric_literal(&when_then[0].1) == Some(1.0)
                    && else_expr.as_deref().and_then(numeric_literal) == Some(0.0) =>
                {
                    &when_then[0].0
                }
                _ => return None,
            };
            let SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = condition
            else {
                return None;
            };
            let pair = match (left.as_ref(), right.as_ref()) {
                (SqlExpr::Column(column), SqlExpr::Literal(Value::String(value)))
                | (SqlExpr::Literal(Value::String(value)), SqlExpr::Column(column)) => {
                    (column, value)
                }
                _ => return None,
            };
            let column = pair
                .0
                .rsplit('.')
                .next()
                .unwrap_or(pair.0)
                .trim_matches('"');
            Some((column.to_string(), pair.1.clone(), format!("{:?}", expr)))
        }

        fn collect(expr: &SqlExpr, output: &mut Vec<(String, String, String)>) {
            if let Some(spec) = categorical_sum(expr) {
                output.push(spec);
            }
            match expr {
                SqlExpr::Function { args, .. } => {
                    for arg in args {
                        collect(arg, output);
                    }
                }
                SqlExpr::BinaryOp { left, right, .. } => {
                    collect(left, output);
                    collect(right, output);
                }
                SqlExpr::UnaryOp { expr, .. }
                | SqlExpr::Cast { expr, .. }
                | SqlExpr::Paren(expr) => collect(expr, output),
                SqlExpr::Case {
                    when_then,
                    else_expr,
                } => {
                    for (condition, value) in when_then {
                        collect(condition, output);
                        collect(value, output);
                    }
                    if let Some(value) = else_expr {
                        collect(value, output);
                    }
                }
                _ => {}
            }
        }

        let mut specs = Vec::new();
        for column in &stmt.columns {
            if let SelectColumn::Expression { expr, .. } = column {
                collect(expr, &mut specs);
            }
        }
        specs.sort_by(|a, b| a.2.cmp(&b.2));
        specs.dedup_by(|a, b| a.2 == b.2);
        if specs.len() < 2 || groups.is_empty() {
            return Ok(());
        }

        let mut row_group = vec![usize::MAX; batch.num_rows()];
        for (group, rows) in groups.iter().enumerate() {
            for &row in rows {
                row_group[row] = group;
            }
        }

        let mut by_column: AHashMap<String, Vec<(String, String)>> = AHashMap::new();
        for (column, literal, key) in specs {
            by_column.entry(column).or_default().push((literal, key));
        }
        for (column, column_specs) in by_column {
            let Some(values) = batch
                .column_by_name(&column)
                .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            else {
                continue;
            };
            let mut literal_outputs: AHashMap<&str, Vec<usize>> = AHashMap::new();
            for (index, (literal, _)) in column_specs.iter().enumerate() {
                literal_outputs
                    .entry(literal.as_str())
                    .or_default()
                    .push(index);
            }
            let mut counts = vec![vec![0.0f64; groups.len()]; column_specs.len()];
            for row in 0..values.len() {
                if values.is_null(row) || row_group[row] == usize::MAX {
                    continue;
                }
                if let Some(outputs) = literal_outputs.get(values.value(row)) {
                    for &output in outputs {
                        counts[output][row_group[row]] += 1.0;
                    }
                }
            }
            for ((_, key), values) in column_specs.into_iter().zip(counts) {
                cache.insert(key, Arc::new(Float64Array::from(values)) as ArrayRef);
            }
        }
        Ok(())
    }

    pub(in crate::query::executor) fn precompute_shared_percentiles(
        batch: &RecordBatch,
        stmt: &SelectStatement,
        groups: &[Vec<usize>],
        cache: &mut AHashMap<String, ArrayRef>,
    ) -> io::Result<()> {
        use rayon::prelude::*;

        fn percentile(expr: &SqlExpr) -> Option<(String, SqlExpr, f64, String)> {
            let SqlExpr::Function { name, args } = expr else {
                return None;
            };
            if !name.eq_ignore_ascii_case("PERCENTILE_APPROX") || args.len() != 2 {
                return None;
            }
            let quantile = match &args[1] {
                SqlExpr::Literal(Value::Float64(value)) => *value,
                SqlExpr::Literal(Value::Int64(value)) => *value as f64,
                _ => return None,
            }
            .clamp(0.0, 1.0);
            Some((
                format!("{:?}", args[0]),
                args[0].clone(),
                quantile,
                format!("{:?}", expr),
            ))
        }

        fn collect(expr: &SqlExpr, output: &mut Vec<(String, SqlExpr, f64, String)>) {
            if let Some(spec) = percentile(expr) {
                output.push(spec);
            }
            match expr {
                SqlExpr::Function { args, .. } => {
                    for arg in args {
                        collect(arg, output);
                    }
                }
                SqlExpr::BinaryOp { left, right, .. } => {
                    collect(left, output);
                    collect(right, output);
                }
                SqlExpr::UnaryOp { expr, .. }
                | SqlExpr::Cast { expr, .. }
                | SqlExpr::Paren(expr) => collect(expr, output),
                SqlExpr::Case {
                    when_then,
                    else_expr,
                } => {
                    for (condition, value) in when_then {
                        collect(condition, output);
                        collect(value, output);
                    }
                    if let Some(value) = else_expr {
                        collect(value, output);
                    }
                }
                _ => {}
            }
        }

        let mut found = Vec::new();
        for column in &stmt.columns {
            if let SelectColumn::Expression { expr, .. } = column {
                collect(expr, &mut found);
            }
        }
        let mut by_argument: AHashMap<String, (SqlExpr, Vec<(f64, String)>)> = AHashMap::new();
        for (argument_key, argument, quantile, cache_key) in found {
            by_argument
                .entry(argument_key)
                .or_insert_with(|| (argument, Vec::new()))
                .1
                .push((quantile, cache_key));
        }

        for (_, (argument, mut specs)) in by_argument {
            specs.sort_by(|a, b| a.1.cmp(&b.1));
            specs.dedup_by(|a, b| a.1 == b.1);
            if specs.len() < 2 {
                continue;
            }
            let values = Self::evaluate_expr_to_array(batch, &argument)?;
            let grouped = groups
                .par_iter()
                .map(|group| {
                    let mut numbers = Vec::with_capacity(group.len());
                    for &row in group {
                        if values.is_null(row) {
                            continue;
                        }
                        if let Some(array) = values.as_any().downcast_ref::<Int64Array>() {
                            numbers.push(array.value(row) as f64);
                        } else if let Some(array) = values.as_any().downcast_ref::<UInt64Array>() {
                            numbers.push(array.value(row) as f64);
                        } else if let Some(array) = values.as_any().downcast_ref::<Float64Array>() {
                            numbers.push(array.value(row));
                        }
                    }
                    if !numbers.is_empty() {
                        numbers.sort_unstable_by(|a, b| a.total_cmp(b));
                    }
                    specs
                        .iter()
                        .map(|(quantile, _)| {
                            if numbers.is_empty() {
                                None
                            } else {
                                let index =
                                    ((numbers.len() - 1) as f64 * quantile).round() as usize;
                                Some(numbers[index])
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            for (spec_index, (_, cache_key)) in specs.into_iter().enumerate() {
                let output = grouped
                    .iter()
                    .map(|values| values[spec_index])
                    .collect::<Vec<_>>();
                cache.insert(cache_key, Arc::new(Float64Array::from(output)) as ArrayRef);
            }
        }
        Ok(())
    }

    pub(in crate::query::executor) fn evaluate_expr_for_groups(
        batch: &RecordBatch,
        expr: &SqlExpr,
        alias: Option<&str>,
        group_indices: &[Vec<usize>],
        aggregate_cache: &mut AHashMap<String, ArrayRef>,
    ) -> io::Result<(Field, ArrayRef)> {
        let output_name = alias.unwrap_or("expr");
        let representatives = if batch.num_rows() == 0 {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "__dummy",
                    ArrowDataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![0])) as ArrayRef],
            )
            .map_err(|e| err_data(e.to_string()))?
        } else {
            let first_indices = arrow::array::UInt64Array::from(
                group_indices
                    .iter()
                    .map(|g| g[0] as u64)
                    .collect::<Vec<_>>(),
            );
            compute::take_record_batch(batch, &first_indices)
                .map_err(|e| err_data(e.to_string()))?
        };
        let mut aggregate_fields = Vec::new();
        let mut aggregate_arrays = Vec::new();
        let lowered = Self::lower_group_aggregates(
            batch,
            expr,
            group_indices,
            &mut aggregate_fields,
            &mut aggregate_arrays,
            aggregate_cache,
        )?;
        let eval_batch = if aggregate_fields.is_empty() {
            representatives
        } else {
            let mut fields = representatives
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>();
            fields.extend(aggregate_fields);
            let mut arrays = representatives.columns().to_vec();
            arrays.extend(aggregate_arrays);
            RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
                .map_err(|e| err_data(e.to_string()))?
        };
        let array = Self::evaluate_expr_to_array(&eval_batch, &lowered)?;
        Ok((
            Field::new(output_name, array.data_type().clone(), true),
            array,
        ))
    }

    pub(in crate::query::executor) fn is_group_aggregate_name(name: &str) -> bool {
        matches!(
            name.to_ascii_uppercase().as_str(),
            "SUM"
                | "COUNT"
                | "COUNT_DISTINCT"
                | "AVG"
                | "MIN"
                | "MAX"
                | "COLLECT_SET"
                | "COLLECT_LIST"
                | "PERCENTILE_APPROX"
        )
    }

    pub(in crate::query::executor) fn lower_group_aggregates(
        batch: &RecordBatch,
        expr: &SqlExpr,
        groups: &[Vec<usize>],
        fields: &mut Vec<Field>,
        arrays: &mut Vec<ArrayRef>,
        cache: &mut AHashMap<String, ArrayRef>,
    ) -> io::Result<SqlExpr> {
        match expr {
            SqlExpr::Function { name, args } if Self::is_group_aggregate_name(name) => {
                let hidden = format!("__apex_agg_{}", arrays.len());
                let key = format!("{:?}", expr);
                let array = if let Some(cached) = cache.get(&key) {
                    cached.clone()
                } else {
                    let computed = Self::compute_expression_aggregate(batch, name, args, groups)?;
                    cache.insert(key, computed.clone());
                    computed
                };
                fields.push(Field::new(&hidden, array.data_type().clone(), true));
                arrays.push(array);
                Ok(SqlExpr::Column(hidden))
            }
            SqlExpr::Function { name, args } => Ok(SqlExpr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| {
                        Self::lower_group_aggregates(batch, arg, groups, fields, arrays, cache)
                    })
                    .collect::<io::Result<Vec<_>>>()?,
            }),
            SqlExpr::BinaryOp { left, op, right } => Ok(SqlExpr::BinaryOp {
                left: Box::new(Self::lower_group_aggregates(
                    batch, left, groups, fields, arrays, cache,
                )?),
                op: op.clone(),
                right: Box::new(Self::lower_group_aggregates(
                    batch, right, groups, fields, arrays, cache,
                )?),
            }),
            SqlExpr::UnaryOp { op, expr } => Ok(SqlExpr::UnaryOp {
                op: op.clone(),
                expr: Box::new(Self::lower_group_aggregates(
                    batch, expr, groups, fields, arrays, cache,
                )?),
            }),
            SqlExpr::Case {
                when_then,
                else_expr,
            } => Ok(SqlExpr::Case {
                when_then: when_then
                    .iter()
                    .map(|(when, then)| {
                        Ok((
                            Self::lower_group_aggregates(
                                batch, when, groups, fields, arrays, cache,
                            )?,
                            Self::lower_group_aggregates(
                                batch, then, groups, fields, arrays, cache,
                            )?,
                        ))
                    })
                    .collect::<io::Result<Vec<_>>>()?,
                else_expr: else_expr
                    .as_ref()
                    .map(|e| {
                        Self::lower_group_aggregates(batch, e, groups, fields, arrays, cache)
                            .map(Box::new)
                    })
                    .transpose()?,
            }),
            SqlExpr::Cast { expr, data_type } => Ok(SqlExpr::Cast {
                expr: Box::new(Self::lower_group_aggregates(
                    batch, expr, groups, fields, arrays, cache,
                )?),
                data_type: data_type.clone(),
            }),
            SqlExpr::Paren(expr) => Ok(SqlExpr::Paren(Box::new(Self::lower_group_aggregates(
                batch, expr, groups, fields, arrays, cache,
            )?))),
            SqlExpr::ArrayIndex { array, index } => Ok(SqlExpr::ArrayIndex {
                array: Box::new(Self::lower_group_aggregates(
                    batch, array, groups, fields, arrays, cache,
                )?),
                index: Box::new(Self::lower_group_aggregates(
                    batch, index, groups, fields, arrays, cache,
                )?),
            }),
            _ => Ok(expr.clone()),
        }
    }

    pub(in crate::query::executor) fn compute_expression_aggregate(
        batch: &RecordBatch,
        name: &str,
        args: &[SqlExpr],
        groups: &[Vec<usize>],
    ) -> io::Result<ArrayRef> {
        use rayon::prelude::*;

        let upper = name.to_ascii_uppercase();
        let use_parallel = groups.len() > 100;
        if upper == "COUNT" && args.is_empty() {
            let counts = if use_parallel {
                groups
                    .par_iter()
                    .map(|g| g.len() as i64)
                    .collect::<Vec<_>>()
            } else {
                groups.iter().map(|g| g.len() as i64).collect::<Vec<_>>()
            };
            return Ok(Arc::new(Int64Array::from(counts)));
        }
        let arg = args
            .first()
            .ok_or_else(|| err_input(format!("{} requires an argument", name)))?;
        // COUNT(CASE WHEN cond THEN 1 END) / COUNT(IF(cond, 1, 0)): count the
        // condition mask directly instead of materializing a million-row
        // CASE result array.
        if upper == "COUNT" {
            if let Some((cond_expr, count_true)) = Self::count_case_condition(arg) {
                let cond = Self::evaluate_predicate(batch, &cond_expr)?;
                let count_group = |group: &Vec<usize>| {
                    if count_true {
                        group
                            .iter()
                            .filter(|&&i| !cond.is_null(i) && cond.value(i))
                            .count() as i64
                    } else {
                        group.iter().filter(|&&i| !cond.is_null(i)).count() as i64
                    }
                };
                let counts = if use_parallel {
                    groups.par_iter().map(count_group).collect::<Vec<_>>()
                } else {
                    groups.iter().map(count_group).collect::<Vec<_>>()
                };
                return Ok(Arc::new(Int64Array::from(counts)));
            }
        }
        let values = Self::evaluate_expr_to_array(batch, arg)?;
        match upper.as_str() {
            "COUNT" => {
                let counts = if use_parallel {
                    groups
                        .par_iter()
                        .map(|g| g.iter().filter(|&&i| !values.is_null(i)).count() as i64)
                        .collect::<Vec<_>>()
                } else {
                    groups
                        .iter()
                        .map(|g| g.iter().filter(|&&i| !values.is_null(i)).count() as i64)
                        .collect::<Vec<_>>()
                };
                Ok(Arc::new(Int64Array::from(counts)))
            }
            "COUNT_DISTINCT" => {
                let count_group = |g: &Vec<usize>| {
                    let mut set = ahash::AHashSet::with_capacity(g.len());
                    for &i in g {
                        if !values.is_null(i) {
                            set.insert(Self::hash_array_value_fast(&values, i));
                        }
                    }
                    set.len() as i64
                };
                let counts = if use_parallel {
                    groups.par_iter().map(count_group).collect::<Vec<_>>()
                } else {
                    groups.iter().map(count_group).collect::<Vec<_>>()
                };
                Ok(Arc::new(Int64Array::from(counts)))
            }
            "SUM" | "AVG" | "MIN" | "MAX" | "PERCENTILE_APPROX" => {
                if matches!(upper.as_str(), "MIN" | "MAX") {
                    if let Some(strings) = values.as_any().downcast_ref::<StringArray>() {
                        let aggregate_group = |group: &Vec<usize>| {
                            group
                                .iter()
                                .filter_map(|&i| {
                                    if strings.is_null(i) {
                                        None
                                    } else {
                                        Some(strings.value(i))
                                    }
                                })
                                .reduce(|a, b| {
                                    if (upper == "MIN" && a <= b) || (upper == "MAX" && a >= b) {
                                        a
                                    } else {
                                        b
                                    }
                                })
                                .map(str::to_string)
                        };
                        let result: Vec<Option<String>> = if use_parallel {
                            groups.par_iter().map(aggregate_group).collect()
                        } else {
                            groups.iter().map(aggregate_group).collect()
                        };
                        return Ok(Arc::new(StringArray::from(result)));
                    }
                }
                let percentile = args
                    .get(1)
                    .and_then(|e| match e {
                        SqlExpr::Literal(Value::Float64(v)) => Some(*v),
                        SqlExpr::Literal(Value::Int64(v)) => Some(*v as f64),
                        _ => None,
                    })
                    .unwrap_or(0.5)
                    .clamp(0.0, 1.0);
                let aggregate_group = |group: &Vec<usize>| {
                    let mut nums = Vec::with_capacity(group.len());
                    for &i in group {
                        if values.is_null(i) {
                            continue;
                        }
                        if let Some(a) = values.as_any().downcast_ref::<Int64Array>() {
                            nums.push(a.value(i) as f64);
                        } else if let Some(a) = values.as_any().downcast_ref::<UInt64Array>() {
                            nums.push(a.value(i) as f64);
                        } else if let Some(a) = values.as_any().downcast_ref::<Float64Array>() {
                            nums.push(a.value(i));
                        }
                    }
                    if nums.is_empty() {
                        None
                    } else {
                        Some(match upper.as_str() {
                            "SUM" => nums.iter().sum(),
                            "AVG" => nums.iter().sum::<f64>() / nums.len() as f64,
                            "MIN" => nums.iter().copied().fold(f64::INFINITY, f64::min),
                            "MAX" => nums.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                            _ => {
                                nums.sort_unstable_by(|a, b| a.total_cmp(b));
                                nums[((nums.len() - 1) as f64 * percentile).round() as usize]
                            }
                        })
                    }
                };
                let out = if use_parallel {
                    groups.par_iter().map(aggregate_group).collect::<Vec<_>>()
                } else {
                    groups.iter().map(aggregate_group).collect::<Vec<_>>()
                };
                Ok(Arc::new(Float64Array::from(out)))
            }
            "COLLECT_SET" | "COLLECT_LIST" => {
                let distinct = upper == "COLLECT_SET";
                if let Some(strings) = values.as_any().downcast_ref::<StringArray>() {
                    let aggregate_strings = |group: &Vec<usize>| {
                        let mut joined = String::with_capacity(group.len().saturating_mul(8));
                        let mut seen = ahash::AHashSet::with_capacity(group.len());
                        let mut first = true;
                        for &row in group {
                            if strings.is_null(row) {
                                continue;
                            }
                            let value = strings.value(row);
                            if distinct && !seen.insert(value) {
                                continue;
                            }
                            if !first {
                                joined.push('\0');
                            }
                            joined.push_str(value);
                            first = false;
                        }
                        Some(joined)
                    };
                    let output = if use_parallel {
                        groups.par_iter().map(aggregate_strings).collect::<Vec<_>>()
                    } else {
                        groups.iter().map(aggregate_strings).collect::<Vec<_>>()
                    };
                    return Ok(Arc::new(StringArray::from(output)));
                }
                let aggregate_group = |group: &Vec<usize>| {
                    let mut items = Vec::with_capacity(group.len());
                    let mut seen = ahash::AHashSet::with_capacity(group.len());
                    for &i in group {
                        if values.is_null(i) {
                            continue;
                        }
                        let text = match Self::arrow_value_at_col(&values, i) {
                            Value::String(v) => v,
                            Value::Int64(v) => v.to_string(),
                            Value::Float64(v) => v.to_string(),
                            Value::Bool(v) => v.to_string(),
                            _ => continue,
                        };
                        if !distinct || seen.insert(text.clone()) {
                            items.push(text);
                        }
                    }
                    Some(items.join("\0"))
                };
                let out = if use_parallel {
                    groups.par_iter().map(aggregate_group).collect::<Vec<_>>()
                } else {
                    groups.iter().map(aggregate_group).collect::<Vec<_>>()
                };
                Ok(Arc::new(StringArray::from(out)))
            }
            _ => Err(err_input(format!(
                "Unsupported aggregate function {}",
                name
            ))),
        }
    }

    /// Recognize `CASE WHEN cond THEN <non-null const> [ELSE NULL] END` and
    /// `IF(cond, <non-null const>, <const>)` shapes used by COUNT(CASE ...).
    /// Returns (condition, count_true): when true the count is the number of
    /// TRUE condition rows; when false it is the number of non-NULL condition
    /// rows (ELSE makes the CASE result non-NULL for FALSE conditions).
    pub(in crate::query::executor) fn count_case_condition(
        arg: &SqlExpr,
    ) -> Option<(SqlExpr, bool)> {
        fn is_null_literal(expr: &SqlExpr) -> bool {
            matches!(expr, SqlExpr::Literal(crate::data::Value::Null))
        }
        match arg {
            SqlExpr::Case {
                when_then,
                else_expr,
            } if when_then.len() == 1 => {
                let (cond, then) = &when_then[0];
                if is_null_literal(then) {
                    return None;
                }
                match else_expr {
                    None => Some((cond.clone(), true)),
                    Some(e) if is_null_literal(e) => Some((cond.clone(), true)),
                    Some(e) if !is_null_literal(e) => Some((cond.clone(), false)),
                    _ => None,
                }
            }
            SqlExpr::Function { name, args } if name.eq_ignore_ascii_case("IF") && args.len() == 3 => {
                // IF(cond, a, b): NULL result only when cond is NULL.
                if !is_null_literal(&args[1]) {
                    Some((args[0].clone(), false))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub(in crate::query::executor) fn create_group_batch(        batch: &RecordBatch,
        indices: &[usize],
    ) -> io::Result<RecordBatch> {
        let indices_array =
            arrow::array::UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
        compute::take_record_batch(batch, &indices_array).map_err(|e| err_data(e.to_string()))
    }

    pub(in crate::query::executor) fn evaluate_aggregate_condition(
        batch: &RecordBatch,
        expr: &SqlExpr,
    ) -> io::Result<bool> {
        match expr {
            SqlExpr::BinaryOp { left, op, right } => {
                // Check if this is a comparison operation
                match op {
                    BinaryOperator::Ge
                    | BinaryOperator::Gt
                    | BinaryOperator::Le
                    | BinaryOperator::Lt
                    | BinaryOperator::Eq
                    | BinaryOperator::NotEq => {
                        // Evaluate left and right, handling aggregates
                        let left_val = Self::evaluate_aggregate_expr_scalar(batch, left)?;
                        let right_val = Self::evaluate_aggregate_expr_scalar(batch, right)?;

                        match op {
                            BinaryOperator::Ge => Ok(left_val >= right_val),
                            BinaryOperator::Gt => Ok(left_val > right_val),
                            BinaryOperator::Le => Ok(left_val <= right_val),
                            BinaryOperator::Lt => Ok(left_val < right_val),
                            BinaryOperator::Eq => Ok((left_val - right_val).abs() < f64::EPSILON),
                            BinaryOperator::NotEq => {
                                Ok((left_val - right_val).abs() >= f64::EPSILON)
                            }
                            _ => unreachable!(),
                        }
                    }
                    _ => {
                        // For logical operators, evaluate as predicate
                        let result = Self::evaluate_predicate(batch, expr)?;
                        Ok(result.len() > 0 && result.value(0))
                    }
                }
            }
            _ => {
                // For other expressions, try to evaluate as predicate
                let result = Self::evaluate_predicate(batch, expr)?;
                Ok(result.len() > 0 && result.value(0))
            }
        }
    }

    pub(in crate::query::executor) fn evaluate_aggregate_expr_scalar(
        batch: &RecordBatch,
        expr: &SqlExpr,
    ) -> io::Result<f64> {
        match expr {
            SqlExpr::Function { name, args } => {
                // Check if this is an aggregate function (zero-allocation)
                let func_opt = if name.eq_ignore_ascii_case("SUM") {
                    Some(AggregateFunc::Sum)
                } else if name.eq_ignore_ascii_case("COUNT") {
                    Some(AggregateFunc::Count)
                } else if name.eq_ignore_ascii_case("AVG") {
                    Some(AggregateFunc::Avg)
                } else if name.eq_ignore_ascii_case("MIN") {
                    Some(AggregateFunc::Min)
                } else if name.eq_ignore_ascii_case("MAX") {
                    Some(AggregateFunc::Max)
                } else {
                    None
                };
                if let Some(func) = func_opt {
                    let col_name = if args.is_empty() {
                        "*"
                    } else if let SqlExpr::Column(c) = &args[0] {
                        c.as_str()
                    } else {
                        "*"
                    };
                    // Create group indices covering all rows in the batch
                    let all_indices: Vec<usize> = (0..batch.num_rows()).collect();
                    let (_, result_arr) = Self::compute_aggregate_for_groups(
                        batch,
                        &func,
                        &Some(col_name.to_string()),
                        &None,
                        &[all_indices],
                        false,
                    )?;
                    if let Some(int_arr) = result_arr.as_any().downcast_ref::<Int64Array>() {
                        Ok(if int_arr.len() > 0 && !int_arr.is_null(0) {
                            int_arr.value(0) as f64
                        } else {
                            0.0
                        })
                    } else if let Some(float_arr) =
                        result_arr.as_any().downcast_ref::<Float64Array>()
                    {
                        Ok(if float_arr.len() > 0 && !float_arr.is_null(0) {
                            float_arr.value(0)
                        } else {
                            0.0
                        })
                    } else {
                        Ok(0.0)
                    }
                } else {
                    let arr = Self::evaluate_expr_to_array(batch, expr)?;
                    Self::extract_scalar_from_array(&arr)
                }
            }
            SqlExpr::Literal(Value::Int64(i)) => Ok(*i as f64),
            SqlExpr::Literal(Value::Float64(f)) => Ok(*f),
            _ => {
                let arr = Self::evaluate_expr_to_array(batch, expr)?;
                Self::extract_scalar_from_array(&arr)
            }
        }
    }

    pub(in crate::query::executor) fn take_first_from_groups(
        array: &ArrayRef,
        group_indices: &[Vec<usize>],
        output_name: &str,
    ) -> io::Result<(Field, ArrayRef)> {
        use arrow::datatypes::DataType;
        let first_indices: Vec<usize> = group_indices.iter().map(|g| g[0]).collect();
        match array.data_type() {
            DataType::Int64 => {
                let src = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok((
                    Field::new(output_name, DataType::Int64, true),
                    Arc::new(Int64Array::from(
                        first_indices
                            .iter()
                            .map(|&i| {
                                if src.is_null(i) {
                                    None
                                } else {
                                    Some(src.value(i))
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                ))
            }
            DataType::Float64 => {
                let src = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok((
                    Field::new(output_name, DataType::Float64, true),
                    Arc::new(Float64Array::from(
                        first_indices
                            .iter()
                            .map(|&i| {
                                if src.is_null(i) {
                                    None
                                } else {
                                    Some(src.value(i))
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                ))
            }
            DataType::Utf8 => {
                let src = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok((
                    Field::new(output_name, DataType::Utf8, true),
                    Arc::new(StringArray::from(
                        first_indices
                            .iter()
                            .map(|&i| {
                                if src.is_null(i) {
                                    None
                                } else {
                                    Some(src.value(i))
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                ))
            }
            DataType::Boolean => {
                let src = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok((
                    Field::new(output_name, DataType::Boolean, true),
                    Arc::new(BooleanArray::from(
                        first_indices
                            .iter()
                            .map(|&i| {
                                if src.is_null(i) {
                                    None
                                } else {
                                    Some(src.value(i))
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                ))
            }
            DataType::Dictionary(_, _) => {
                // Dictionary-encoded string group key: take the first row per
                // group and decode to a plain Utf8 array.
                use arrow::array::DictionaryArray;
                use arrow::datatypes::UInt32Type;
                let src = array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .ok_or_else(|| err_data("expected dictionary group column"))?;
                let values = src
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| err_data("dictionary group values must be strings"))?;
                let strings: Vec<Option<&str>> = first_indices
                    .iter()
                    .map(|&i| {
                        if src.keys().is_null(i) {
                            None
                        } else {
                            let key = src.keys().value(i) as usize;
                            if key < values.len() && !values.is_null(key) {
                                Some(values.value(key))
                            } else {
                                None
                            }
                        }
                    })
                    .collect();
                Ok((
                    Field::new(output_name, DataType::Utf8, true),
                    Arc::new(StringArray::from(strings)),
                ))
            }
            _ => Ok((
                Field::new(output_name, DataType::Int64, true),
                Arc::new(Int64Array::from(vec![None::<i64>; group_indices.len()])),
            )),
        }
    }

    pub(in crate::query::executor) fn compute_aggregate_for_groups(
        batch: &RecordBatch,
        func: &crate::query::AggregateFunc,
        column: &Option<String>,
        alias: &Option<String>,
        group_indices: &[Vec<usize>],
        distinct: bool,
    ) -> io::Result<(Field, ArrayRef)> {
        use crate::query::AggregateFunc;
        use ahash::AHashSet;
        use rayon::prelude::*;

        let func_name = match func {
            AggregateFunc::Count => "COUNT",
            AggregateFunc::Sum => "SUM",
            AggregateFunc::Avg => "AVG",
            AggregateFunc::Min => "MIN",
            AggregateFunc::Max => "MAX",
        };

        let output_name = alias.clone().unwrap_or_else(|| {
            if let Some(col) = column {
                format!("{}({})", func_name, col)
            } else {
                format!("{}(*)", func_name)
            }
        });

        // Strip table prefix from column name if present (e.g., "o.amount" -> "amount")
        let actual_column: Option<String> = column.as_ref().map(|c| {
            let trimmed = c.trim_matches('"');
            if let Some(dot_pos) = trimmed.rfind('.') {
                trimmed[dot_pos + 1..].to_string()
            } else {
                trimmed.to_string()
            }
        });

        match func {
            AggregateFunc::Count => {
                let use_parallel = group_indices.len() > 100;
                let counts: Vec<i64> = if let Some(col_name) = &actual_column {
                    if col_name == "*"
                        || col_name
                            .chars()
                            .next()
                            .map(|c| c.is_ascii_digit())
                            .unwrap_or(false)
                    {
                        if use_parallel {
                            group_indices.par_iter().map(|g| g.len() as i64).collect()
                        } else {
                            group_indices.iter().map(|g| g.len() as i64).collect()
                        }
                    } else if let Some(array) = batch.column_by_name(col_name) {
                        if distinct {
                            // COUNT(DISTINCT column) - count unique values per group
                            if let Some(int_arr) = array.as_any().downcast_ref::<Int64Array>() {
                                if use_parallel {
                                    group_indices
                                        .par_iter()
                                        .map(|g| {
                                            let unique: AHashSet<i64> = g
                                                .iter()
                                                .filter(|&&i| !int_arr.is_null(i))
                                                .map(|&i| int_arr.value(i))
                                                .collect();
                                            unique.len() as i64
                                        })
                                        .collect()
                                } else {
                                    group_indices
                                        .iter()
                                        .map(|g| {
                                            let unique: AHashSet<i64> = g
                                                .iter()
                                                .filter(|&&i| !int_arr.is_null(i))
                                                .map(|&i| int_arr.value(i))
                                                .collect();
                                            unique.len() as i64
                                        })
                                        .collect()
                                }
                            } else if let Some(str_arr) =
                                array.as_any().downcast_ref::<StringArray>()
                            {
                                if use_parallel {
                                    group_indices
                                        .par_iter()
                                        .map(|g| {
                                            let unique: AHashSet<&str> = g
                                                .iter()
                                                .filter(|&&i| !str_arr.is_null(i))
                                                .map(|&i| str_arr.value(i))
                                                .collect();
                                            unique.len() as i64
                                        })
                                        .collect()
                                } else {
                                    group_indices
                                        .iter()
                                        .map(|g| {
                                            let unique: AHashSet<&str> = g
                                                .iter()
                                                .filter(|&&i| !str_arr.is_null(i))
                                                .map(|&i| str_arr.value(i))
                                                .collect();
                                            unique.len() as i64
                                        })
                                        .collect()
                                }
                            } else {
                                if use_parallel {
                                    group_indices
                                        .par_iter()
                                        .map(|g| {
                                            g.iter().filter(|&&i| !array.is_null(i)).count() as i64
                                        })
                                        .collect()
                                } else {
                                    group_indices
                                        .iter()
                                        .map(|g| {
                                            g.iter().filter(|&&i| !array.is_null(i)).count() as i64
                                        })
                                        .collect()
                                }
                            }
                        } else {
                            if use_parallel {
                                group_indices
                                    .par_iter()
                                    .map(|g| {
                                        g.iter().filter(|&&i| !array.is_null(i)).count() as i64
                                    })
                                    .collect()
                            } else {
                                group_indices
                                    .iter()
                                    .map(|g| {
                                        g.iter().filter(|&&i| !array.is_null(i)).count() as i64
                                    })
                                    .collect()
                            }
                        }
                    } else {
                        vec![0; group_indices.len()]
                    }
                } else {
                    if use_parallel {
                        group_indices.par_iter().map(|g| g.len() as i64).collect()
                    } else {
                        group_indices.iter().map(|g| g.len() as i64).collect()
                    }
                };
                Ok((
                    Field::new(&output_name, ArrowDataType::Int64, false),
                    Arc::new(Int64Array::from(counts)),
                ))
            }
            AggregateFunc::Sum => {
                let col_name = actual_column
                    .as_ref()
                    .ok_or_else(|| err_input("SUM requires column"))?;
                let array = batch
                    .column_by_name(col_name)
                    .ok_or_else(|| err_not_found(format!("Column: {}", col_name)))?;

                if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                    // Fast path: direct slice access with loop unrolling
                    let values = int_array.values();
                    let use_parallel = group_indices.len() > 100;

                    let sums: Vec<i64> = if use_parallel {
                        group_indices
                            .par_iter()
                            .map(|g| {
                                // Unrolled summation for better instruction pipelining
                                let mut sum0: i64 = 0;
                                let mut sum1: i64 = 0;
                                let mut sum2: i64 = 0;
                                let mut sum3: i64 = 0;
                                let chunks = g.chunks_exact(4);
                                let remainder = chunks.remainder();
                                for chunk in chunks {
                                    sum0 = sum0.wrapping_add(values[chunk[0]]);
                                    sum1 = sum1.wrapping_add(values[chunk[1]]);
                                    sum2 = sum2.wrapping_add(values[chunk[2]]);
                                    sum3 = sum3.wrapping_add(values[chunk[3]]);
                                }
                                for &i in remainder {
                                    sum0 = sum0.wrapping_add(values[i]);
                                }
                                sum0.wrapping_add(sum1)
                                    .wrapping_add(sum2)
                                    .wrapping_add(sum3)
                            })
                            .collect()
                    } else {
                        group_indices
                            .iter()
                            .map(|g| {
                                let mut sum: i64 = 0;
                                for &i in g {
                                    sum = sum.wrapping_add(values[i]);
                                }
                                sum
                            })
                            .collect()
                    };
                    Ok((
                        Field::new(&output_name, ArrowDataType::Int64, false),
                        Arc::new(Int64Array::from(sums)),
                    ))
                } else if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                    let values = float_array.values();
                    let use_parallel = group_indices.len() > 100;

                    let sums: Vec<f64> = if use_parallel {
                        group_indices
                            .par_iter()
                            .map(|g| {
                                let mut sum: f64 = 0.0;
                                for &i in g {
                                    sum += values[i];
                                }
                                sum
                            })
                            .collect()
                    } else {
                        group_indices
                            .iter()
                            .map(|g| {
                                let mut sum: f64 = 0.0;
                                for &i in g {
                                    sum += values[i];
                                }
                                sum
                            })
                            .collect()
                    };
                    Ok((
                        Field::new(&output_name, ArrowDataType::Float64, false),
                        Arc::new(Float64Array::from(sums)),
                    ))
                } else {
                    Err(err_data("SUM requires numeric column"))
                }
            }
            AggregateFunc::Avg => {
                let col_name = actual_column
                    .as_ref()
                    .ok_or_else(|| err_input("AVG requires column"))?;
                let array = batch
                    .column_by_name(col_name)
                    .ok_or_else(|| err_not_found(format!("Column: {}", col_name)))?;

                if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                    // Fast path: direct slice access, compute sum and count together
                    let values = int_array.values();
                    let avgs: Vec<f64> = group_indices
                        .iter()
                        .map(|g| {
                            if g.is_empty() {
                                return 0.0;
                            }
                            let mut sum: i64 = 0;
                            for &i in g {
                                sum = sum.wrapping_add(values[i]);
                            }
                            sum as f64 / g.len() as f64
                        })
                        .collect();
                    Ok((
                        Field::new(&output_name, ArrowDataType::Float64, false),
                        Arc::new(Float64Array::from(avgs)),
                    ))
                } else if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                    let values = float_array.values();
                    let avgs: Vec<f64> = group_indices
                        .iter()
                        .map(|g| {
                            if g.is_empty() {
                                return 0.0;
                            }
                            let mut sum: f64 = 0.0;
                            for &i in g {
                                sum += values[i];
                            }
                            sum / g.len() as f64
                        })
                        .collect();
                    Ok((
                        Field::new(&output_name, ArrowDataType::Float64, false),
                        Arc::new(Float64Array::from(avgs)),
                    ))
                } else {
                    Err(err_data("AVG requires numeric column"))
                }
            }
            AggregateFunc::Min => {
                let col_name = actual_column
                    .as_ref()
                    .ok_or_else(|| err_input("MIN requires column"))?;
                let array = batch
                    .column_by_name(col_name)
                    .ok_or_else(|| err_not_found(format!("Column: {}", col_name)))?;

                if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                    let mins: Vec<Option<i64>> = group_indices
                        .iter()
                        .map(|g| {
                            g.iter()
                                .filter_map(|&i| {
                                    if int_array.is_null(i) {
                                        None
                                    } else {
                                        Some(int_array.value(i))
                                    }
                                })
                                .min()
                        })
                        .collect();
                    Ok((
                        Field::new(&output_name, ArrowDataType::Int64, true),
                        Arc::new(Int64Array::from(mins)),
                    ))
                } else if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                    let mins: Vec<Option<f64>> = group_indices
                        .iter()
                        .map(|g| {
                            g.iter()
                                .filter_map(|&i| {
                                    if float_array.is_null(i) {
                                        None
                                    } else {
                                        Some(float_array.value(i))
                                    }
                                })
                                .reduce(f64::min)
                        })
                        .collect();
                    Ok((
                        Field::new(&output_name, ArrowDataType::Float64, true),
                        Arc::new(Float64Array::from(mins)),
                    ))
                } else if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                    let mins: Vec<Option<String>> = group_indices
                        .iter()
                        .map(|g| {
                            g.iter()
                                .filter_map(|&i| {
                                    if string_array.is_null(i) {
                                        None
                                    } else {
                                        Some(string_array.value(i))
                                    }
                                })
                                .min()
                                .map(str::to_string)
                        })
                        .collect();
                    Ok((
                        Field::new(&output_name, ArrowDataType::Utf8, true),
                        Arc::new(StringArray::from(mins)),
                    ))
                } else {
                    Err(err_data("MIN requires numeric column"))
                }
            }
            AggregateFunc::Max => {
                let col_name = actual_column
                    .as_ref()
                    .ok_or_else(|| err_input("MAX requires column"))?;
                let array = batch
                    .column_by_name(col_name)
                    .ok_or_else(|| err_not_found(format!("Column: {}", col_name)))?;

                if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                    let maxs: Vec<Option<i64>> = group_indices
                        .iter()
                        .map(|g| {
                            g.iter()
                                .filter_map(|&i| {
                                    if int_array.is_null(i) {
                                        None
                                    } else {
                                        Some(int_array.value(i))
                                    }
                                })
                                .max()
                        })
                        .collect();
                    Ok((
                        Field::new(&output_name, ArrowDataType::Int64, true),
                        Arc::new(Int64Array::from(maxs)),
                    ))
                } else if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                    let maxs: Vec<Option<f64>> = group_indices
                        .iter()
                        .map(|g| {
                            g.iter()
                                .filter_map(|&i| {
                                    if float_array.is_null(i) {
                                        None
                                    } else {
                                        Some(float_array.value(i))
                                    }
                                })
                                .reduce(f64::max)
                        })
                        .collect();
                    Ok((
                        Field::new(&output_name, ArrowDataType::Float64, true),
                        Arc::new(Float64Array::from(maxs)),
                    ))
                } else if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                    let maxs: Vec<Option<String>> = group_indices
                        .iter()
                        .map(|g| {
                            g.iter()
                                .filter_map(|&i| {
                                    if string_array.is_null(i) {
                                        None
                                    } else {
                                        Some(string_array.value(i))
                                    }
                                })
                                .max()
                                .map(str::to_string)
                        })
                        .collect();
                    Ok((
                        Field::new(&output_name, ArrowDataType::Utf8, true),
                        Arc::new(StringArray::from(maxs)),
                    ))
                } else {
                    Err(err_data("MAX requires numeric column"))
                }
            }
        }
    }
}
