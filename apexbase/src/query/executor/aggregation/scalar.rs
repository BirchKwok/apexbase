use super::*;

impl ApexExecutor {
    pub(in crate::query::executor) fn execute_aggregation(
        batch: &RecordBatch,
        stmt: &SelectStatement,
    ) -> io::Result<ApexResult> {
        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        let mut aggregate_cache: AHashMap<String, ArrayRef> = AHashMap::new();

        for col in &stmt.columns {
            match col {
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct,
                    alias,
                } => {
                    let (field, array) =
                        Self::compute_aggregate(batch, func, column, *distinct, alias)?;
                    fields.push(field);
                    arrays.push(array);
                }
                SelectColumn::Expression { expr, alias } if Self::expr_contains_aggregate(expr) => {
                    let all_rows = vec![(0..batch.num_rows()).collect::<Vec<_>>()];
                    let (field, array) = Self::evaluate_expr_for_groups(
                        batch,
                        expr,
                        alias.as_deref(),
                        &all_rows,
                        &mut aggregate_cache,
                    )?;
                    fields.push(field);
                    arrays.push(array);
                }
                _ => {}
            }
        }

        if fields.is_empty() {
            return Ok(ApexResult::Scalar(batch.num_rows() as i64));
        }

        let schema = Arc::new(Schema::new(fields));
        let result = RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;

        Ok(ApexResult::Data(result))
    }

    pub(in crate::query::executor) fn compute_aggregate(
        batch: &RecordBatch,
        func: &crate::query::AggregateFunc,
        column: &Option<String>,
        distinct: bool,
        alias: &Option<String>,
    ) -> io::Result<(Field, ArrayRef)> {
        use crate::query::AggregateFunc;

        let fn_name = match func {
            AggregateFunc::Count => "COUNT",
            AggregateFunc::Sum => "SUM",
            AggregateFunc::Avg => "AVG",
            AggregateFunc::Min => "MIN",
            AggregateFunc::Max => "MAX",
        };
        let output_name = alias.clone().unwrap_or_else(|| {
            if let Some(col) = column {
                format!("{}({})", fn_name, col)
            } else {
                format!("{}(*)", fn_name)
            }
        });

        match func {
            AggregateFunc::Count => {
                let count = if let Some(col_name) = column {
                    // Treat "*" and numeric constants (like "1") as COUNT(*) - count all rows
                    if col_name == "*"
                        || col_name
                            .chars()
                            .next()
                            .map(|c| c.is_ascii_digit())
                            .unwrap_or(false)
                    {
                        batch.num_rows() as i64
                    } else {
                        let clean_name = col_name.trim_matches('"');
                        let clean_name = clean_name.rsplit('.').next().unwrap_or(clean_name);
                        if let Some(array) = batch.column_by_name(clean_name) {
                            if distinct {
                                Self::count_distinct(array)
                            } else {
                                (array.len() - array.null_count()) as i64
                            }
                        } else {
                            0
                        }
                    }
                } else {
                    batch.num_rows() as i64
                };
                Ok((
                    Field::new(&output_name, ArrowDataType::Int64, false),
                    Arc::new(Int64Array::from(vec![count])),
                ))
            }
            AggregateFunc::Sum => {
                let col_name = column
                    .as_ref()
                    .ok_or_else(|| err_input("SUM requires column"))?;
                let clean_name = col_name.trim_matches('"');
                let clean_name = clean_name.rsplit('.').next().unwrap_or(clean_name);
                let array = batch
                    .column_by_name(clean_name)
                    .ok_or_else(|| err_not_found(format!("Column: {}", clean_name)))?;

                if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                    // SIMD-optimized sum for non-null arrays
                    let sum = if int_array.null_count() == 0 {
                        simd_sum_i64(int_array.values())
                    } else {
                        int_array.iter().filter_map(|v| v).sum()
                    };
                    Ok((
                        Field::new(&output_name, ArrowDataType::Int64, false),
                        Arc::new(Int64Array::from(vec![sum])),
                    ))
                } else if let Some(uint_array) = array.as_any().downcast_ref::<UInt64Array>() {
                    let sum: i64 = uint_array.iter().filter_map(|v| v).map(|v| v as i64).sum();
                    Ok((
                        Field::new(&output_name, ArrowDataType::Int64, false),
                        Arc::new(Int64Array::from(vec![sum])),
                    ))
                } else if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                    // SIMD-optimized sum for non-null arrays
                    let sum = if float_array.null_count() == 0 {
                        simd_sum_f64(float_array.values())
                    } else {
                        float_array.iter().filter_map(|v| v).sum()
                    };
                    Ok((
                        Field::new(&output_name, ArrowDataType::Float64, false),
                        Arc::new(Float64Array::from(vec![sum])),
                    ))
                } else {
                    Err(err_data("SUM requires numeric column"))
                }
            }
            AggregateFunc::Avg => {
                let col_name = column
                    .as_ref()
                    .ok_or_else(|| err_input("AVG requires column"))?;
                let clean_name = col_name.trim_matches('"');
                let clean_name = clean_name.rsplit('.').next().unwrap_or(clean_name);
                let array = batch
                    .column_by_name(clean_name)
                    .ok_or_else(|| err_not_found(format!("Column: {}", clean_name)))?;

                if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                    // SIMD-optimized AVG for non-null arrays
                    let (sum, count) = if int_array.null_count() == 0 {
                        (simd_sum_i64(int_array.values()), int_array.len())
                    } else {
                        let values: Vec<i64> = int_array.iter().filter_map(|v| v).collect();
                        (values.iter().sum::<i64>(), values.len())
                    };
                    let avg = if count == 0 {
                        0.0
                    } else {
                        sum as f64 / count as f64
                    };
                    Ok((
                        Field::new(&output_name, ArrowDataType::Float64, false),
                        Arc::new(Float64Array::from(vec![avg])),
                    ))
                } else if let Some(uint_array) = array.as_any().downcast_ref::<UInt64Array>() {
                    let values: Vec<u64> = uint_array.iter().filter_map(|v| v).collect();
                    let avg = if values.is_empty() {
                        0.0
                    } else {
                        values.iter().sum::<u64>() as f64 / values.len() as f64
                    };
                    Ok((
                        Field::new(&output_name, ArrowDataType::Float64, false),
                        Arc::new(Float64Array::from(vec![avg])),
                    ))
                } else if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                    // SIMD-optimized AVG for non-null arrays
                    let (sum, count) = if float_array.null_count() == 0 {
                        (simd_sum_f64(float_array.values()), float_array.len())
                    } else {
                        let values: Vec<f64> = float_array.iter().filter_map(|v| v).collect();
                        (values.iter().sum::<f64>(), values.len())
                    };
                    let avg = if count == 0 { 0.0 } else { sum / count as f64 };
                    Ok((
                        Field::new(&output_name, ArrowDataType::Float64, false),
                        Arc::new(Float64Array::from(vec![avg])),
                    ))
                } else {
                    Err(err_data("AVG requires numeric column"))
                }
            }
            AggregateFunc::Min | AggregateFunc::Max => {
                let is_min = matches!(func, AggregateFunc::Min);
                let fn_name = if is_min { "MIN" } else { "MAX" };
                let col_name = column
                    .as_ref()
                    .ok_or_else(|| err_input(format!("{} requires column", fn_name)))?;
                let clean_name = col_name.trim_matches('"');
                let clean_name = clean_name.rsplit('.').next().unwrap_or(clean_name);
                let array = batch
                    .column_by_name(clean_name)
                    .ok_or_else(|| err_not_found(format!("Column: {}", clean_name)))?;

                if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                    let val = if is_min {
                        compute::min(int_array)
                    } else {
                        compute::max(int_array)
                    };
                    Ok((
                        Field::new(&output_name, ArrowDataType::Int64, true),
                        Arc::new(Int64Array::from(vec![val])),
                    ))
                } else if let Some(uint_array) = array.as_any().downcast_ref::<UInt64Array>() {
                    let val = if is_min {
                        compute::min(uint_array)
                    } else {
                        compute::max(uint_array)
                    }
                    .map(|v| v as i64);
                    Ok((
                        Field::new(&output_name, ArrowDataType::Int64, true),
                        Arc::new(Int64Array::from(vec![val])),
                    ))
                } else if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                    let val = if is_min {
                        compute::min(float_array)
                    } else {
                        compute::max(float_array)
                    };
                    Ok((
                        Field::new(&output_name, ArrowDataType::Float64, true),
                        Arc::new(Float64Array::from(vec![val])),
                    ))
                } else {
                    Err(err_data(format!("{} requires numeric column", fn_name)))
                }
            }
        }
    }
}
