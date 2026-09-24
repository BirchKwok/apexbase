// Fused GROUP BY fast path: single streaming scan with predicate tree and aggregate lanes.

impl ApexExecutor {
    /// Strip table prefix and quotes from a column reference.
    fn clean_fused_column(name: &str) -> String {
        let trimmed = name.trim_matches('"');
        trimmed
            .rsplit('.')
            .next()
            .unwrap_or(trimmed)
            .trim_matches('"')
            .to_string()
    }

    /// Aggregation mask bits for the fused kernel: bit 0 sum (SUM/AVG),
    /// bit 1 min (MIN), bit 2 max (MAX); COUNT(*) needs none.
    fn fused_agg_mask_bit(func: &crate::query::AggregateFunc) -> u8 {
        match func {
            crate::query::AggregateFunc::Sum | crate::query::AggregateFunc::Avg => 1,
            crate::query::AggregateFunc::Min => 2,
            crate::query::AggregateFunc::Max => 4,
            crate::query::AggregateFunc::Count => 0,
        }
    }

    /// FAST PATH: fuse an arbitrary boolean predicate tree (numeric ranges,
    /// numeric IN, dictionary IN/Eq on the group column, AND/OR/NOT) with a
    /// single low-cardinality string GROUP BY and COUNT/SUM/AVG/MIN/MAX over
    /// one numeric column in one streaming mmap scan. HAVING / ORDER BY /
    /// LIMIT / OFFSET are applied on the aggregated (≤ dictionary size) rows.
    /// Falls back to the generic pipeline for anything outside the gate.
    fn try_fast_fused_group_by(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::AggregateFunc;
        use crate::storage::on_demand::{FusedGroupAgg, FusedLaneSpec};

        if backend.pending_v4_in_memory_rows() > 0
            || backend.has_pending_deltas()
            || backend.has_delta()
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || stmt.group_by.len() != 1
            || stmt.group_by_exprs.iter().any(Option::is_some)
            || !stmt.joins.is_empty()
            || stmt.order_by.iter().any(|clause| clause.expr.is_some())
        {
            return Ok(None);
        }

        let where_clause = match &stmt.where_clause {
            Some(expr) => expr,
            None => return Ok(None),
        };
        let group_col = Self::clean_fused_column(&stmt.group_by[0]);

        // The group column must be dictionary-encoded with a usable global
        // dictionary cache (same gates as the indexed dictionary reads).
        let Some((dict, has_nulls, max_id)) =
            crate::storage::backend::get_global_dict_cache_with_nulls(
                backend.path(),
                &group_col,
                &backend.storage,
            )?
        else {
            return Ok(None);
        };
        if has_nulls {
            return Ok(None);
        }
        let Some(max) = max_id else {
            return Ok(None);
        };
        let (dict_strings, group_ids) = dict.as_ref();
        if max as usize >= dict_strings.len() || dict_strings.len() > 4096 {
            return Ok(None);
        }

        // WHERE → fused predicate tree over ≤2 numeric lanes + group column.
        let mut lanes: Vec<FusedLaneSpec> = Vec::new();
        let Some(predicate) =
            Self::extract_fused_predicate(backend, where_clause, &group_col, dict_strings, &mut lanes)?
        else {
            return Ok(None);
        };
        if lanes.len() > 2 {
            return Ok(None);
        }

        // SELECT shape: group column + at most one aggregate over one column.
        let mut agg_col: Option<String> = None;
        let mut has_value_agg = false;
        for column in &stmt.columns {
            match column {
                SelectColumn::Column(name)
                    if Self::clean_fused_column(name) == group_col => {}
                SelectColumn::ColumnAlias { column, .. }
                    if Self::clean_fused_column(column) == group_col => {}
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct,
                    ..
                } => {
                    if *distinct {
                        return Ok(None);
                    }
                    if matches!(func, AggregateFunc::Count)
                        && column
                            .as_ref()
                            .map_or(true, |name| name == "*" || name.trim_matches('"').is_empty())
                    {
                        // COUNT(*) consumes no lane; it may coexist with the
                        // single value aggregate.
                    } else {
                        if has_value_agg {
                            return Ok(None);
                        }
                        let name = match column {
                            Some(name) if name != "*" => Self::clean_fused_column(name),
                            _ => return Ok(None),
                        };
                        if Self::clean_fused_column(&name) == group_col {
                            return Ok(None);
                        }
                        agg_col = Some(name);
                        has_value_agg = true;
                    }
                }
                _ => return Ok(None),
            }
        }
        let agg_lane = match &agg_col {
            Some(col) => Some(Self::fused_lane_for(backend, col, &mut lanes)?
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "lane budget exceeded")
                })?),
            None => None,
        };
        if lanes.len() > 2 {
            return Ok(None);
        }

        // Aggregate only what SELECT/HAVING actually reference: bit 0 sum
        // (SUM/AVG), bit 1 min (MIN), bit 2 max (MAX).
        let mut agg_mask: u8 = 0;
        for column in &stmt.columns {
            if let SelectColumn::Aggregate { func, .. } = column {
                agg_mask |= Self::fused_agg_mask_bit(&func);
            }
        }
        if let Some(having_expr) = &stmt.having {
            for (func, _col) in &Self::collect_having_extra_aggs(having_expr, &stmt.columns) {
                agg_mask |= Self::fused_agg_mask_bit(&func);
            }
        }

        // The kernel omits null checks in its inner loop: keep nullable
        // inputs on the generic Arrow path.
        for spec in &lanes {
            if backend.column_has_nulls(&spec.col) {
                return Ok(None);
            }
        }
        let raw = backend.execute_fused_group_agg(
            dict_strings,
            group_ids,
            &lanes,
            &predicate,
            agg_lane,
            agg_mask,
        )?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let surviving: Vec<(usize, &FusedGroupAgg)> = raw
            .iter()
            .enumerate()
            .filter(|(_, agg)| agg.count > 0)
            .collect();
        if surviving.is_empty() {
            // Build an empty result with the right schema.
            let (fields, arrays) = Self::fused_group_result_columns(
                stmt,
                &group_col,
                dict_strings,
                &[],
            )?;
            let schema = Arc::new(Schema::new(fields));
            let result = RecordBatch::try_new(schema, arrays)
                .map_err(|e| err_data(e.to_string()))?;
            return Ok(Some(ApexResult::Empty(result.schema())));
        }

        let mut fields: Vec<Field> = Vec::with_capacity(stmt.columns.len() + 4);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len() + 4);
        let (col_fields, col_arrays) = Self::fused_group_result_columns(
            stmt,
            &group_col,
            dict_strings,
            &surviving,
        )?;
        fields.extend(col_fields);
        arrays.extend(col_arrays);

        // HAVING: references only COUNT(*) or aggregates over the agg column;
        // extra aggregates are materialized, filtered, then stripped.
        let mut extra_names: Vec<String> = Vec::new();
        if let Some(having_expr) = &stmt.having {
            let extras = Self::collect_having_extra_aggs(having_expr, &stmt.columns);
            for (func, col) in &extras {
                let supported = match func {
                    AggregateFunc::Count => col.is_none(),
                    _ => col.as_deref() == agg_col.as_deref() && agg_col.is_some(),
                };
                if !supported {
                    return Ok(None);
                }
            }
            for (func, col) in &extras {
                let name = match func {
                    AggregateFunc::Count => "COUNT(*)".to_string(),
                    AggregateFunc::Sum => format!("SUM({})", col.as_deref().unwrap_or("*")),
                    AggregateFunc::Avg => format!("AVG({})", col.as_deref().unwrap_or("*")),
                    AggregateFunc::Min => format!("MIN({})", col.as_deref().unwrap_or("*")),
                    AggregateFunc::Max => format!("MAX({})", col.as_deref().unwrap_or("*")),
                };
                extra_names.push(name.clone());
                let values: Vec<f64> = surviving
                    .iter()
                    .map(|(_slot, agg)| match func {
                        AggregateFunc::Count => agg.count as f64,
                        AggregateFunc::Sum => agg.sum,
                        AggregateFunc::Avg if agg.count > 0 => agg.sum / agg.count as f64,
                        AggregateFunc::Avg => 0.0,
                        AggregateFunc::Min => agg.min,
                        AggregateFunc::Max => agg.max,
                    })
                    .collect();
                if matches!(func, AggregateFunc::Count) {
                    fields.push(Field::new(&name, ArrowDataType::Int64, false));
                    arrays.push(Arc::new(Int64Array::from(
                        values.iter().map(|v| *v as i64).collect::<Vec<_>>(),
                    )));
                } else {
                    fields.push(Field::new(&name, ArrowDataType::Float64, false));
                    arrays.push(Arc::new(Float64Array::from(values)));
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        let mut result =
            RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;
        if let Some(having_expr) = &stmt.having {
            let mask = Self::evaluate_predicate(&result, having_expr)?;
            result = arrow::compute::filter_record_batch(&result, &mask)
                .map_err(|e| err_data(e.to_string()))?;
        }
        if !extra_names.is_empty() {
            let keep_count = result.num_columns().saturating_sub(extra_names.len());
            let new_schema = Arc::new(Schema::new(
                result.schema().fields()[..keep_count]
                    .iter()
                    .map(|f| f.as_ref().clone())
                    .collect::<Vec<_>>(),
            ));
            let new_arrays: Vec<ArrayRef> = (0..keep_count).map(|i| result.column(i).clone()).collect();
            result = RecordBatch::try_new(new_schema, new_arrays)
                .map_err(|e| err_data(e.to_string()))?;
        }

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

    /// Build the result columns for a fused group aggregation: the group
    /// column plus the aggregate column(s) in SELECT-list order.
    fn fused_group_result_columns(
        stmt: &SelectStatement,
        group_col: &str,
        dict_strings: &[String],
        surviving: &[(usize, &crate::storage::on_demand::FusedGroupAgg)],
    ) -> io::Result<(Vec<Field>, Vec<ArrayRef>)> {
        let mut fields: Vec<Field> = Vec::with_capacity(stmt.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());
        for column in &stmt.columns {
            match column {
                SelectColumn::Column(_) => {
                    let values: Vec<&str> = surviving
                        .iter()
                        .map(|(slot, _)| dict_strings[*slot].as_str())
                        .collect();
                    fields.push(Field::new(
                        Self::group_output_name(stmt, group_col),
                        ArrowDataType::Utf8,
                        false,
                    ));
                    arrays.push(Arc::new(StringArray::from(values)));
                }
                SelectColumn::ColumnAlias { alias, .. } => {
                    let values: Vec<&str> = surviving
                        .iter()
                        .map(|(slot, _)| dict_strings[*slot].as_str())
                        .collect();
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
                    let output_name =
                        alias.clone().unwrap_or_else(|| format!("{}({})", func, source));
                    match func {
                        AggregateFunc::Count => {
                            fields.push(Field::new(&output_name, ArrowDataType::Int64, false));
                            arrays.push(Arc::new(Int64Array::from(
                                surviving
                                    .iter()
                                    .map(|(_, agg)| agg.count)
                                    .collect::<Vec<_>>(),
                            )));
                        }
                        AggregateFunc::Sum | AggregateFunc::Min | AggregateFunc::Max => {
                            let values: Vec<f64> = surviving
                                .iter()
                                .map(|(_, agg)| match func {
                                    AggregateFunc::Sum => agg.sum,
                                    AggregateFunc::Min => agg.min,
                                    AggregateFunc::Max => agg.max,
                                    _ => unreachable!(),
                                })
                                .collect();
                            fields.push(Field::new(&output_name, ArrowDataType::Float64, false));
                            arrays.push(Arc::new(Float64Array::from(values)));
                        }
                        AggregateFunc::Avg => {
                            let values: Vec<f64> = surviving
                                .iter()
                                .map(|(_, agg)| {
                                    if agg.count > 0 {
                                        agg.sum / agg.count as f64
                                    } else {
                                        0.0
                                    }
                                })
                                .collect();
                            fields.push(Field::new(&output_name, ArrowDataType::Float64, false));
                            arrays.push(Arc::new(Float64Array::from(values)));
                        }
                    }
                }
                _ => return Err(err_input("unsupported column in fused group by")),
            }
        }
        Ok((fields, arrays))
    }

    /// Parse a WHERE clause into the fused predicate tree. Registers numeric
    /// lanes (≤2) as they are encountered; returns None for anything outside
    /// the fused grammar.
    fn extract_fused_predicate(
        backend: &TableStorageBackend,
        expr: &SqlExpr,
        group_col: &str,
        dict_strings: &[String],
        lanes: &mut Vec<crate::storage::on_demand::FusedLaneSpec>,
    ) -> io::Result<Option<crate::storage::on_demand::FusedPredicate>> {
        use crate::storage::on_demand::{FusedLeaf, FusedPredicate};
        use crate::query::sql_parser::BinaryOperator;

        match expr {
            SqlExpr::Paren(inner) => Self::extract_fused_predicate(
                backend,
                inner,
                group_col,
                dict_strings,
                lanes,
            ),
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let left = Self::extract_fused_predicate(
                    backend, left, group_col, dict_strings, lanes,
                )?;
                let right = Self::extract_fused_predicate(
                    backend, right, group_col, dict_strings, lanes,
                )?;
                match (left, right) {
                    (Some(left), Some(right)) => Ok(Some(FusedPredicate::And(
                        Box::new(left),
                        Box::new(right),
                    ))),
                    _ => Ok(None),
                }
            }
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Or,
                right,
            } => {
                let left = Self::extract_fused_predicate(
                    backend, left, group_col, dict_strings, lanes,
                )?;
                let right = Self::extract_fused_predicate(
                    backend, right, group_col, dict_strings, lanes,
                )?;
                match (left, right) {
                    (Some(left), Some(right)) => Ok(Some(FusedPredicate::Or(
                        Box::new(left),
                        Box::new(right),
                    ))),
                    _ => Ok(None),
                }
            }
            SqlExpr::UnaryOp {
                op: crate::query::sql_parser::UnaryOperator::Not,
                expr: inner,
            } => {
                let inner_pred = Self::extract_fused_predicate(
                    backend, inner, group_col, dict_strings, lanes,
                )?;
                Ok(inner_pred.map(|p| FusedPredicate::Not(Box::new(p))))
            }
            SqlExpr::Between {
                column,
                low,
                high,
                negated,
            } => {
                let col = Self::clean_fused_column(column);
                let leaf = (col != group_col)
                    .then(|| {
                        Self::fused_range_leaf(
                            backend, &col, Some(low.as_ref()), Some(high.as_ref()), lanes,
                        )
                    })
                    .flatten();
                let Some(leaf) = leaf else {
                    return Ok(None);
                };
                Ok(Some(if *negated {
                    FusedPredicate::Not(Box::new(FusedPredicate::Leaf(leaf)))
                } else {
                    FusedPredicate::Leaf(leaf)
                }))
            }
            SqlExpr::In {
                column,
                values,
                negated,
            } => {
                if values.is_empty() || values.len() > 16 {
                    return Ok(None);
                }
                let col = Self::clean_fused_column(column);
                let leaf = if col == group_col {
                    let mut flags = vec![0u8; dict_strings.len()];
                    for value in values {
                        let crate::data::Value::String(s) = value else {
                            return Ok(None);
                        };
                        // Values absent from the dictionary can never match a
                        // row; they simply leave the slot flag unset.
                        if let Some(key) = dict_strings.iter().position(|d| d == s.as_str()) {
                            flags[key] = 1;
                        }
                    }
                    Some(FusedLeaf::DictIn { flags })
                } else {
                    let Some(lane) = Self::fused_lane_for(backend, &col, lanes)? else {
                        return Ok(None);
                    };
                    if lanes[lane].is_int {
                        let mut vals: Vec<i64> =
                            values.iter().filter_map(Self::fused_value_i64).collect();
                        if vals.len() != values.len() {
                            return Ok(None);
                        }
                        vals.sort_unstable();
                        vals.dedup();
                        Some(FusedLeaf::InI64 { lane, values: vals })
                    } else {
                        let mut vals: Vec<f64> =
                            values.iter().filter_map(Self::fused_value_f64).collect();
                        if vals.len() != values.len() {
                            return Ok(None);
                        }
                        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        vals.dedup_by(|a, b| a == b);
                        Some(FusedLeaf::InF64 { lane, values: vals })
                    }
                };
                let Some(leaf) = leaf else {
                    return Ok(None);
                };
                Ok(Some(if *negated {
                    FusedPredicate::Not(Box::new(FusedPredicate::Leaf(leaf)))
                } else {
                    FusedPredicate::Leaf(leaf)
                }))
            }
            SqlExpr::BinaryOp { left, op, right } => {
                let (col, effective_op, lit) = match (left.as_ref(), right.as_ref()) {
                    (SqlExpr::Column(c), lit) => (Self::clean_fused_column(c), op.clone(), lit),
                    (lit, SqlExpr::Column(c)) => {
                        let flipped = match op {
                            BinaryOperator::Gt => BinaryOperator::Lt,
                            BinaryOperator::Ge => BinaryOperator::Le,
                            BinaryOperator::Lt => BinaryOperator::Gt,
                            BinaryOperator::Le => BinaryOperator::Ge,
                            BinaryOperator::Eq => BinaryOperator::Eq,
                            BinaryOperator::NotEq => BinaryOperator::NotEq,
                            _ => return Ok(None),
                        };
                        (Self::clean_fused_column(c), flipped, lit)
                    }
                    _ => return Ok(None),
                };
                let negated = matches!(&effective_op, BinaryOperator::NotEq);
                let leaf = Self::fused_range_leaf_for_op(
                    backend, &col, &group_col, dict_strings, &effective_op, lit, lanes,
                )?;
                let Some(leaf) = leaf else {
                    return Ok(None);
                };
                let pred = FusedPredicate::Leaf(leaf);
                Ok(Some(if negated {
                    FusedPredicate::Not(Box::new(pred))
                } else {
                    pred
                }))
            }
            _ => Ok(None),
        }
    }

    /// Build a range/eq leaf for `column OP literal`. The group column maps
    /// to a dictionary equality; numeric columns map to a lane range using
    /// exact i64 bounds for integral columns and the next/prev-representable
    /// epsilon bounds for strict float inequalities.
    fn fused_range_leaf_for_op(
        backend: &TableStorageBackend,
        col: &str,
        group_col: &str,
        dict_strings: &[String],
        op: &BinaryOperator,
        lit: &SqlExpr,
        lanes: &mut Vec<crate::storage::on_demand::FusedLaneSpec>,
    ) -> io::Result<Option<crate::storage::on_demand::FusedLeaf>> {
        use crate::storage::on_demand::FusedLeaf;
        use crate::query::sql_parser::BinaryOperator;

        if col == group_col {
            if !matches!(op, BinaryOperator::Eq) {
                return Ok(None);
            }
            let s = match lit {
                SqlExpr::Literal(crate::data::Value::String(s)) => s.as_str(),
                _ => return Ok(None),
            };
            let key = dict_strings
                .iter()
                .position(|d| d == s)
                .unwrap_or(usize::MAX)
                .min(u16::MAX as usize);
            // A key that is not in the dictionary can never match a row; the
            // sentinel u16::MAX is unreachable because dictionaries are
            // capped at 4096 entries.
            return Ok(Some(FusedLeaf::DictEq { key: key as u16 }));
        }

        let Some(lane) = Self::fused_lane_for(backend, col, lanes)? else {
            return Ok(None);
        };
        if lanes[lane].is_int {
            let Some(v) = Self::fused_literal_i64(lit) else {
                return Ok(None);
            };
            let (lo, hi) = match op {
                BinaryOperator::Lt => (i64::MIN, v.saturating_sub(1)),
                BinaryOperator::Le => (i64::MIN, v),
                BinaryOperator::Gt => (v.saturating_add(1), i64::MAX),
                BinaryOperator::Ge => (v, i64::MAX),
                BinaryOperator::Eq => (v, v),
                _ => return Ok(None),
            };
            Ok(Some(FusedLeaf::RangeI64 { lane, lo, hi }))
        } else {
            let Some(v) = Self::fused_literal_f64(lit) else {
                return Ok(None);
            };
            let (lo, hi) = match op {
                BinaryOperator::Lt => (f64::NEG_INFINITY, Self::fused_prev_f64(v)),
                BinaryOperator::Le => (f64::NEG_INFINITY, v),
                BinaryOperator::Gt => (Self::fused_next_f64(v), f64::INFINITY),
                BinaryOperator::Ge => (v, f64::INFINITY),
                BinaryOperator::Eq => (v, v),
                _ => return Ok(None),
            };
            Ok(Some(FusedLeaf::RangeF64 { lane, lo, hi }))
        }
    }

    /// BETWEEN bounds → lane range leaf (open bounds are unbounded).
    fn fused_range_leaf(
        backend: &TableStorageBackend,
        col: &str,
        low: Option<&SqlExpr>,
        high: Option<&SqlExpr>,
        lanes: &mut Vec<crate::storage::on_demand::FusedLaneSpec>,
    ) -> Option<crate::storage::on_demand::FusedLeaf> {
        use crate::storage::on_demand::FusedLeaf;
        let lane = match Self::fused_lane_for(backend, col, lanes) {
            Ok(lane) => lane?,
            Err(_) => return None,
        };
        if lanes[lane].is_int {
            let lo = low.map(Self::fused_literal_i64).unwrap_or(Some(i64::MIN))?;
            let hi = high.map(Self::fused_literal_i64).unwrap_or(Some(i64::MAX))?;
            Some(FusedLeaf::RangeI64 { lane, lo, hi })
        } else {
            let lo = low.map(Self::fused_literal_f64).unwrap_or(Some(f64::NEG_INFINITY))?;
            let hi = high.map(Self::fused_literal_f64).unwrap_or(Some(f64::INFINITY))?;
            Some(FusedLeaf::RangeF64 { lane, lo, hi })
        }
    }

    fn fused_lane_for(
        backend: &TableStorageBackend,
        col: &str,
        lanes: &mut Vec<crate::storage::on_demand::FusedLaneSpec>,
    ) -> io::Result<Option<usize>> {
        if let Some(pos) = lanes.iter().position(|l| l.col == col) {
            return Ok(Some(pos));
        }
        if lanes.len() >= 2 {
            return Ok(None);
        }
        let dtype = match backend.get_column_type(col) {
            Some(t) => t,
            None => return Ok(None),
        };
        let is_int = matches!(
            dtype,
            crate::data::DataType::Int8
                | crate::data::DataType::Int16
                | crate::data::DataType::Int32
                | crate::data::DataType::Int64
                | crate::data::DataType::UInt8
                | crate::data::DataType::UInt16
                | crate::data::DataType::UInt32
                | crate::data::DataType::UInt64
        );
        let is_float = matches!(
            dtype,
            crate::data::DataType::Float32 | crate::data::DataType::Float64
        );
        if !is_int && !is_float {
            return Ok(None);
        }
        lanes.push(crate::storage::on_demand::FusedLaneSpec {
            col: col.to_string(),
            is_int,
        });
        Ok(Some(lanes.len() - 1))
    }

    /// Exact i64 literal extraction for integral lanes. Integer-valued
    /// floats are accepted below 2^53 to avoid precision loss.
    fn fused_literal_i64(expr: &SqlExpr) -> Option<i64> {
        match expr {
            SqlExpr::Literal(crate::data::Value::Int64(n)) => Some(*n),
            SqlExpr::Literal(crate::data::Value::Int32(n)) => Some(*n as i64),
            SqlExpr::Literal(crate::data::Value::UInt64(n)) if *n <= i64::MAX as u64 => {
                Some(*n as i64)
            }
            SqlExpr::Literal(crate::data::Value::Float64(f))
                if f.fract() == 0.0 && f.abs() < 9.0e15 =>
            {
                Some(*f as i64)
            }
            SqlExpr::Literal(crate::data::Value::Float32(f))
                if (*f as f64).fract() == 0.0 && (*f as f64).abs() < 9.0e15 =>
            {
                Some(*f as f64 as i64)
            }
            _ => None,
        }
    }

    fn fused_literal_f64(expr: &SqlExpr) -> Option<f64> {
        match expr {
            SqlExpr::Literal(crate::data::Value::Int64(n)) => Some(*n as f64),
            SqlExpr::Literal(crate::data::Value::Int32(n)) => Some(*n as f64),
            SqlExpr::Literal(crate::data::Value::UInt64(n)) => Some(*n as f64),
            SqlExpr::Literal(crate::data::Value::Float64(f)) => Some(*f),
            SqlExpr::Literal(crate::data::Value::Float32(f)) => Some(*f as f64),
            _ => None,
        }
    }

    /// Exact i64 extraction from an IN-list literal (see fused_literal_i64).
    fn fused_value_i64(value: &crate::data::Value) -> Option<i64> {
        use crate::data::Value;
        match value {
            Value::Int8(n) => Some(*n as i64),
            Value::Int16(n) => Some(*n as i64),
            Value::Int32(n) => Some(*n as i64),
            Value::Int64(n) => Some(*n),
            Value::UInt8(n) => Some(*n as i64),
            Value::UInt16(n) => Some(*n as i64),
            Value::UInt32(n) => Some(*n as i64),
            Value::UInt64(n) if *n <= i64::MAX as u64 => Some(*n as i64),
            Value::Float64(f) if f.fract() == 0.0 && f.abs() < 9.0e15 => Some(*f as i64),
            Value::Float32(f)
                if (*f as f64).fract() == 0.0 && (*f as f64).abs() < 9.0e15 =>
            {
                Some(*f as f64 as i64)
            }
            _ => None,
        }
    }

    fn fused_value_f64(value: &crate::data::Value) -> Option<f64> {
        use crate::data::Value;
        match value {
            Value::Int8(n) => Some(*n as f64),
            Value::Int16(n) => Some(*n as f64),
            Value::Int32(n) => Some(*n as f64),
            Value::Int64(n) => Some(*n as f64),
            Value::UInt8(n) => Some(*n as f64),
            Value::UInt16(n) => Some(*n as f64),
            Value::UInt32(n) => Some(*n as f64),
            Value::UInt64(n) => Some(*n as f64),
            Value::Float32(f) => Some(*f as f64),
            Value::Float64(f) => Some(*f),
            _ => None,
        }
    }

    /// Smallest representable f64 strictly above `v` (strict-inequality
    /// epsilon bounds; mirrors extract_single_comparison_as_range).
    #[inline]
    fn fused_next_f64(v: f64) -> f64 {
        if v >= 0.0 {
            f64::from_bits(v.to_bits() + 1)
        } else {
            f64::from_bits(v.to_bits() - 1)
        }
    }

    /// Largest representable f64 strictly below `v`.
    #[inline]
    fn fused_prev_f64(v: f64) -> f64 {
        if v > 0.0 {
            f64::from_bits(v.to_bits() - 1)
        } else {
            f64::from_bits(v.to_bits() + 1)
        }
    }
}
