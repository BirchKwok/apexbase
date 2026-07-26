// Sorting, projection, and shared aggregation support.

use super::*;

#[path = "distinct.rs"]
mod distinct;
#[path = "grouped.rs"]
mod grouped;
#[path = "having.rs"]
mod having;
#[path = "scalar.rs"]
mod scalar;

#[derive(Clone, Copy)]
enum JsonProjectionTransform {
    Scalar,
    StringArray,
}

impl ApexExecutor {
    /// Recognize projection expressions that can share one JSON parse per row.
    /// Hive ETL commonly extracts many paths from the same JSON column; parsing
    /// the document once for every GET_JSON_OBJECT call is needlessly O(paths).
    pub(in crate::query::executor) fn json_projection_spec(expr: &SqlExpr) -> Option<(String, String, JsonProjectionTransform)> {
        match expr {
            SqlExpr::Function { name, args }
                if name.eq_ignore_ascii_case("GET_JSON_OBJECT") && args.len() == 2 =>
            {
                let column = match &args[0] {
                    SqlExpr::Column(column) => column
                        .rsplit('.')
                        .next()
                        .unwrap_or(column)
                        .trim_matches('"')
                        .to_string(),
                    _ => return None,
                };
                let path = match &args[1] {
                    SqlExpr::Literal(Value::String(path)) => path.clone(),
                    _ => return None,
                };
                Some((column, path, JsonProjectionTransform::Scalar))
            }
            SqlExpr::Function { name, args }
                if name.eq_ignore_ascii_case("SPLIT") && args.len() == 2 =>
            {
                // SPLIT(REGEXP_REPLACE(...GET_JSON_OBJECT(array path)...), ',')
                // is the standard Hive representation used before EXPLODE.
                fn find_json(expr: &SqlExpr) -> Option<(String, String)> {
                    match expr {
                        SqlExpr::Function { name, args }
                            if name.eq_ignore_ascii_case("GET_JSON_OBJECT") && args.len() == 2 =>
                        {
                            let column = match &args[0] {
                                SqlExpr::Column(column) => column
                                    .rsplit('.')
                                    .next()
                                    .unwrap_or(column)
                                    .trim_matches('"')
                                    .to_string(),
                                _ => return None,
                            };
                            let path = match &args[1] {
                                SqlExpr::Literal(Value::String(path)) => path.clone(),
                                _ => return None,
                            };
                            Some((column, path))
                        }
                        // Hive commonly protects an optional JSON array with
                        // COALESCE(GET_JSON_OBJECT(...), '[]') before removing
                        // brackets and splitting.  It is still the same source
                        // extraction and can use the fused JSON-array builder.
                        SqlExpr::Function { name, args }
                            if name.eq_ignore_ascii_case("COALESCE")
                                && args.len() == 2
                                && matches!(&args[1], SqlExpr::Literal(Value::String(fallback)) if fallback == "[]") =>
                        {
                            find_json(&args[0])
                        }
                        SqlExpr::Function { name, args }
                            if name.eq_ignore_ascii_case("REGEXP_REPLACE")
                                && args.len() == 3
                                && matches!(&args[2], SqlExpr::Literal(Value::String(value)) if value.is_empty())
                                && matches!(&args[1], SqlExpr::Literal(Value::String(pattern))
                                    if pattern.contains("\\s")
                                        || (pattern.contains("\\[")
                                            && pattern.contains("\\]")
                                            && pattern.contains('"'))) =>
                        {
                            find_json(&args[0])
                        }
                        _ => None,
                    }
                }
                let delimiter_is_comma = matches!(
                    &args[1],
                    SqlExpr::Literal(Value::String(delimiter)) if delimiter == ","
                );
                if !delimiter_is_comma {
                    return None;
                }
                find_json(&args[0])
                    .map(|(column, path)| (column, path, JsonProjectionTransform::StringArray))
            }
            _ => None,
        }
    }

    pub(in crate::query::executor) fn fused_json_projection_arrays(
        batch: &RecordBatch,
        columns: &[SelectColumn],
    ) -> io::Result<HashMap<usize, ArrayRef>> {
        use serde_json::value::RawValue;

        let mut by_source: HashMap<String, Vec<(usize, Vec<String>, JsonProjectionTransform)>> =
            HashMap::new();
        for (index, column) in columns.iter().enumerate() {
            if let SelectColumn::Expression { expr, .. } = column {
                if let Some((source, path, transform)) = Self::json_projection_spec(expr) {
                    by_source.entry(source).or_default().push((
                        index,
                        path.trim_start_matches("$.")
                            .split('.')
                            .filter(|key| !key.is_empty())
                            .map(str::to_string)
                            .collect(),
                        transform,
                    ));
                }
            }
        }

        let mut output = HashMap::new();
        for (source, specs) in by_source {
            // A single extraction is already optimal in the normal expression
            // evaluator. Fusion matters only when the JSON parse is shared.
            if specs.len() < 2 {
                continue;
            }
            let Some(jsons) = batch
                .column_by_name(&source)
                .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            else {
                continue;
            };
            let build_chunk = |start: usize, end: usize| -> Vec<ArrayRef> {
                let rows = end - start;
                let mut builders: Vec<arrow::array::StringBuilder> = specs
                    .iter()
                    .map(|_| arrow::array::StringBuilder::with_capacity(rows, rows * 8))
                    .collect();
                for row in start..end {
                    let root: Option<HashMap<&str, &RawValue>> = if jsons.is_null(row) {
                        None
                    } else {
                        serde_json::from_str(jsons.value(row)).ok()
                    };
                    let mut nested_cache: Option<(&str, HashMap<&str, &RawValue>)> = None;
                    for (builder, (_, keys, transform)) in builders.iter_mut().zip(specs.iter()) {
                        let Some(root) = root.as_ref() else {
                            builder.append_null();
                            continue;
                        };
                        let Some(first_key) = keys.first() else {
                            builder.append_null();
                            continue;
                        };
                        let Some(mut value) = root.get(first_key.as_str()).copied() else {
                            builder.append_null();
                            continue;
                        };
                        let mut found = true;
                        if keys.len() == 2 {
                            let cache_matches = nested_cache
                                .as_ref()
                                .is_some_and(|(key, _)| *key == first_key.as_str());
                            if !cache_matches {
                                nested_cache = serde_json::from_str(value.get())
                                    .ok()
                                    .map(|nested| (first_key.as_str(), nested));
                            }
                            if let Some(next) = nested_cache
                                .as_ref()
                                .and_then(|(_, nested)| nested.get(keys[1].as_str()))
                                .copied()
                            {
                                value = next;
                            } else {
                                found = false;
                            }
                        } else {
                            for key in &keys[1..] {
                                let nested: HashMap<&str, &RawValue> =
                                    match serde_json::from_str(value.get()) {
                                        Ok(nested) => nested,
                                        Err(_) => {
                                            found = false;
                                            break;
                                        }
                                    };
                                if let Some(next) = nested.get(key.as_str()).copied() {
                                    value = next;
                                } else {
                                    found = false;
                                    break;
                                }
                            }
                        }
                        let raw = value.get();
                        if !found || raw == "null" {
                            builder.append_null();
                            continue;
                        }
                        match transform {
                            JsonProjectionTransform::StringArray => {
                                if let Ok(values) = serde_json::from_str::<Vec<&str>>(raw) {
                                    builder.append_value(values.join("\0"));
                                } else if let Ok(values) =
                                    serde_json::from_str::<Vec<serde_json::Value>>(raw)
                                {
                                    let mut joined = String::new();
                                    for (index, item) in values.iter().enumerate() {
                                        if index > 0 {
                                            joined.push('\0');
                                        }
                                        if let Some(text) = item.as_str() {
                                            joined.push_str(text);
                                        } else if !item.is_null() {
                                            joined.push_str(&item.to_string());
                                        }
                                    }
                                    builder.append_value(&joined);
                                } else {
                                    builder.append_null();
                                }
                            }
                            JsonProjectionTransform::Scalar => {
                                if raw.starts_with('"') {
                                    if let Ok(text) = serde_json::from_str::<&str>(raw) {
                                        builder.append_value(text);
                                    } else if let Ok(text) = serde_json::from_str::<String>(raw) {
                                        builder.append_value(text);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_value(raw);
                                }
                            }
                        }
                    }
                }
                builders
                    .into_iter()
                    .map(|mut builder| Arc::new(builder.finish()) as ArrayRef)
                    .collect()
            };

            let arrays = if batch.num_rows() >= 100_000 && rayon::current_num_threads() > 1 {
                use rayon::prelude::*;
                let partitions = rayon::current_num_threads().min(batch.num_rows());
                let chunk_rows = batch.num_rows().div_ceil(partitions);
                let chunks: Vec<Vec<ArrayRef>> = (0..partitions)
                    .into_par_iter()
                    .map(|partition| {
                        let start = partition * chunk_rows;
                        let end = (start + chunk_rows).min(batch.num_rows());
                        build_chunk(start, end)
                    })
                    .collect();
                (0..specs.len())
                    .map(|column| {
                        let refs: Vec<&dyn Array> =
                            chunks.iter().map(|chunk| chunk[column].as_ref()).collect();
                        compute::concat(&refs).map_err(|error| err_data(error.to_string()))
                    })
                    .collect::<io::Result<Vec<_>>>()?
            } else {
                build_chunk(0, batch.num_rows())
            };
            for ((index, _, _), array) in specs.into_iter().zip(arrays) {
                output.insert(index, array);
            }
        }
        Ok(output)
    }

    /// Resolve ORDER BY column names against SELECT list aliases.
    /// Maps expressions like `SUM(amount)` to their output column name (e.g. `total`)
    /// so ORDER BY works correctly when aggregate columns are aliased.
    pub(in crate::query::executor) fn resolve_order_by_cols(
        columns: &[crate::query::SelectColumn],
        order_by: &[crate::query::OrderByClause],
    ) -> Vec<crate::query::OrderByClause> {
        use crate::query::{AggregateFunc, SelectColumn};
        order_by
            .iter()
            .map(|clause| {
                // Try to match against SELECT aggregate columns
                for sel_col in columns {
                    if let SelectColumn::Aggregate {
                        func,
                        column,
                        alias,
                        ..
                    } = sel_col
                    {
                        let fn_str = match func {
                            AggregateFunc::Sum => "SUM",
                            AggregateFunc::Count => "COUNT",
                            AggregateFunc::Avg => "AVG",
                            AggregateFunc::Min => "MIN",
                            AggregateFunc::Max => "MAX",
                        };
                        let col_part = column.as_deref().unwrap_or("*");
                        let default_name = format!("{}({})", fn_str, col_part);
                        if default_name.eq_ignore_ascii_case(&clause.column) {
                            let out_name = if let Some(a) = alias {
                                a.clone()
                            } else {
                                default_name
                            };
                            return crate::query::OrderByClause {
                                column: out_name,
                                descending: clause.descending,
                                nulls_first: clause.nulls_first,
                                expr: None,
                            };
                        }
                    }
                }
                clause.clone()
            })
            .collect()
    }

    /// Apply ORDER BY clause
    /// OPTIMIZATION: Use parallel sort for large datasets (>100K rows) using Rayon
    pub(in crate::query::executor) fn apply_order_by(
        batch: &RecordBatch,
        order_by: &[crate::query::OrderByClause],
    ) -> io::Result<RecordBatch> {
        use arrow::compute::SortColumn;
        use rayon::prelude::*;

        // Pre-evaluate expression ORDER BY columns (e.g. ORDER BY array_distance(...))
        let batch_cow = if order_by.iter().any(|o| {
            o.expr.is_some() && {
                let cn = o.column.trim_matches('"');
                let cn = cn.rfind('.').map_or(cn, |p| &cn[p + 1..]);
                batch.column_by_name(cn).is_none()
            }
        }) {
            let mut extra: Vec<(String, arrow::array::ArrayRef)> = Vec::new();
            for o in order_by {
                if let Some(ref expr) = o.expr {
                    let cn = o.column.trim_matches('"');
                    let cn = cn.rfind('.').map_or(cn, |p| &cn[p + 1..]);
                    if batch.column_by_name(cn).is_none() {
                        let arr = Self::evaluate_expr_to_array(batch, expr)?;
                        extra.push((o.column.clone(), arr));
                    }
                }
            }
            if extra.is_empty() {
                std::borrow::Cow::Borrowed(batch)
            } else {
                let mut fields: Vec<arrow::datatypes::Field> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| (**f).clone())
                    .collect();
                let mut cols = batch.columns().to_vec();
                for (name, arr) in extra {
                    fields.push(arrow::datatypes::Field::new(
                        &name,
                        arr.data_type().clone(),
                        true,
                    ));
                    cols.push(arr);
                }
                let nb =
                    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), cols)
                        .map_err(|e| err_data(e.to_string()))?;
                std::borrow::Cow::Owned(nb)
            }
        } else {
            std::borrow::Cow::Borrowed(batch)
        };
        let batch: &RecordBatch = &batch_cow;

        let sort_columns: Vec<SortColumn> = order_by
            .iter()
            .filter_map(|clause| {
                let col_name = clause.column.trim_matches('"');
                // Strip table prefix if present (e.g., "u.tier" -> "tier")
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                batch.column_by_name(actual_col).map(|col| SortColumn {
                    values: col.clone(),
                    options: Some(SortOptions {
                        descending: clause.descending,
                        nulls_first: clause.nulls_first.unwrap_or(clause.descending),
                    }),
                })
            })
            .collect();

        if sort_columns.is_empty() {
            return Ok(batch.clone());
        }

        let num_rows = batch.num_rows();

        // For large datasets (>50K rows), use parallel sort with Rayon
        let indices = if num_rows > 50_000 && sort_columns.len() == 1 {
            // Single column sort - use parallel sort for better performance
            Self::parallel_sort_indices(batch, order_by)?
        } else {
            // Multi-column or small dataset - use Arrow's lexsort
            compute::lexsort_to_indices(&sort_columns, None).map_err(|e| err_data(e.to_string()))?
        };

        // OPTIMIZATION: Use SIMD-accelerated take for better performance
        let indices_array = &indices;
        let columns: Vec<ArrayRef> = if num_rows > 100_000 {
            use crate::query::simd_take::optimized_take;
            use rayon::prelude::*;
            batch
                .columns()
                .par_iter()
                .map(|col| Arc::new(optimized_take(col, indices_array)) as ArrayRef)
                .collect()
        } else {
            use crate::query::simd_take::optimized_take;
            batch
                .columns()
                .iter()
                .map(|col| Arc::new(optimized_take(col, indices_array)) as ArrayRef)
                .collect()
        };

        RecordBatch::try_new(batch.schema(), columns).map_err(|e| err_data(e.to_string()))
    }

    /// Parallel sort for single-column ORDER BY using Rayon
    /// Uses indices with custom comparator for better memory efficiency
    pub(in crate::query::executor) fn parallel_sort_indices(
        batch: &RecordBatch,
        order_by: &[crate::query::OrderByClause],
    ) -> io::Result<arrow::array::UInt32Array> {
        use rayon::prelude::*;

        let clause = &order_by[0];
        let col_name = clause.column.trim_matches('"');
        let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
            &col_name[dot_pos + 1..]
        } else {
            col_name
        };

        let col = batch.column_by_name(actual_col).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Column {} not found", actual_col),
            )
        })?;

        let num_rows = batch.num_rows();

        // For Int64 - use fast parallel sort with custom comparator
        if let Some(int_arr) = col.as_any().downcast_ref::<Int64Array>() {
            let descending = clause.descending;

            // Check if we can use counting sort (range is limited)
            if !descending {
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
                // Use counting sort if range is reasonable (< 5M values)
                if range <= 5_000_000 && range > 0 {
                    return Self::counting_sort_indices(int_arr, min_val, max_val, num_rows);
                }
            }

            // Create (value, index) pairs and sort in parallel
            let mut pairs: Vec<(i64, usize)> = (0..num_rows)
                .map(|i| {
                    let val = if int_arr.is_null(i) {
                        if descending {
                            i64::MIN
                        } else {
                            i64::MAX
                        }
                    } else {
                        int_arr.value(i)
                    };
                    (val, i)
                })
                .collect();

            // Parallel sort using unstable sort for better performance
            if descending {
                pairs.par_sort_unstable_by(|a, b| b.0.cmp(&a.0));
            } else {
                pairs.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));
            }

            let sorted_indices: Vec<u32> = pairs.iter().map(|(_, idx)| *idx as u32).collect();
            return Ok(arrow::array::UInt32Array::from(sorted_indices));
        }

        // For Float64 - use parallel sort
        if let Some(float_arr) = col.as_any().downcast_ref::<Float64Array>() {
            let descending = clause.descending;

            let mut pairs: Vec<(f64, usize)> = (0..num_rows)
                .map(|i| {
                    let val = if float_arr.is_null(i) {
                        if descending {
                            f64::NEG_INFINITY
                        } else {
                            f64::INFINITY
                        }
                    } else {
                        float_arr.value(i)
                    };
                    (val, i)
                })
                .collect();

            if descending {
                pairs.par_sort_unstable_by(|a, b| {
                    b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal)
                });
            } else {
                pairs.par_sort_unstable_by(|a, b| {
                    a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
                });
            }

            let sorted_indices: Vec<u32> = pairs.iter().map(|(_, idx)| *idx as u32).collect();
            return Ok(arrow::array::UInt32Array::from(sorted_indices));
        }

        // Fallback to Arrow's lexsort for other types
        use arrow::compute::{SortColumn, SortOptions};
        let sort_columns: Vec<_> = order_by
            .iter()
            .filter_map(|clause| {
                let col_name = clause.column.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                batch.column_by_name(actual_col).map(|col| SortColumn {
                    values: col.clone(),
                    options: Some(SortOptions {
                        descending: clause.descending,
                        nulls_first: clause.nulls_first.unwrap_or(clause.descending),
                    }),
                })
            })
            .collect();

        compute::lexsort_to_indices(&sort_columns, None).map_err(|e| err_data(e.to_string()))
    }

    /// Counting sort for integer arrays with limited range
    /// O(n + range) complexity, much faster than comparison sort for small ranges
    pub(in crate::query::executor) fn counting_sort_indices(
        arr: &Int64Array,
        min_val: i64,
        max_val: i64,
        num_rows: usize,
    ) -> io::Result<arrow::array::UInt32Array> {
        let range = (max_val - min_val + 1) as usize;
        let mut counts: Vec<usize> = vec![0; range + 1]; // +1 for nulls

        // Count occurrences
        for i in 0..num_rows {
            if arr.is_null(i) {
                counts[range] += 1;
            } else {
                let idx = (arr.value(i) - min_val) as usize;
                counts[idx] += 1;
            }
        }

        // Compute prefix sums for output positions
        let mut positions: Vec<usize> = vec![0; range + 1];
        let mut total = 0;
        for i in 0..range {
            positions[i] = total;
            total += counts[i];
        }
        positions[range] = total; // nulls at the end

        // Build result indices
        let mut result: Vec<u32> = vec![0; num_rows];
        for i in 0..num_rows {
            let pos = if arr.is_null(i) {
                positions[range]
            } else {
                let idx = (arr.value(i) - min_val) as usize;
                positions[idx]
            };
            result[pos] = i as u32;
            if arr.is_null(i) {
                positions[range] += 1;
            } else {
                positions[(arr.value(i) - min_val) as usize] += 1;
            }
        }

        Ok(arrow::array::UInt32Array::from(result))
    }

    /// Apply ORDER BY with top-k optimization (heap sort for LIMIT queries)
    /// When k is Some, uses partial sort O(n log k) instead of full sort O(n log n)
    /// OPTIMIZATION: Pre-downcast columns once to avoid repeated dynamic dispatch
    pub(in crate::query::executor) fn apply_order_by_topk(
        batch: &RecordBatch,
        order_by: &[crate::query::OrderByClause],
        k: Option<usize>,
    ) -> io::Result<RecordBatch> {
        use std::cmp::Ordering;

        // Pre-evaluate any expression-based ORDER BY columns not yet in the batch.
        // e.g. ORDER BY array_distance(vec, [1,2,3]) — expression not in SELECT output yet.
        let batch = if order_by.iter().any(|o| {
            o.expr.is_some() && {
                let cn = o.column.trim_matches('"');
                let cn = cn.rfind('.').map_or(cn, |p| &cn[p + 1..]);
                batch.column_by_name(cn).is_none()
            }
        }) {
            let mut new_cols: Vec<(String, arrow::array::ArrayRef)> = Vec::new();
            for o in order_by {
                if let Some(ref expr) = o.expr {
                    let cn = o.column.trim_matches('"');
                    let cn = cn.rfind('.').map_or(cn, |p| &cn[p + 1..]);
                    if batch.column_by_name(cn).is_none() {
                        let arr = Self::evaluate_expr_to_array(batch, expr)?;
                        new_cols.push((o.column.clone(), arr));
                    }
                }
            }
            if new_cols.is_empty() {
                std::borrow::Cow::Borrowed(batch)
            } else {
                let mut fields: Vec<arrow::datatypes::Field> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| (**f).clone())
                    .collect();
                let mut cols: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
                for (name, arr) in new_cols {
                    fields.push(arrow::datatypes::Field::new(
                        &name,
                        arr.data_type().clone(),
                        true,
                    ));
                    cols.push(arr);
                }
                let schema = Arc::new(arrow::datatypes::Schema::new(fields));
                let nb = RecordBatch::try_new(schema, cols).map_err(|e| err_data(e.to_string()))?;
                std::borrow::Cow::Owned(nb)
            }
        } else {
            std::borrow::Cow::Borrowed(batch)
        };
        let batch: &RecordBatch = &batch;

        // If no limit or limit >= rows, use standard sort
        let num_rows = batch.num_rows();
        if k.is_none() || k.unwrap() >= num_rows {
            return Self::apply_order_by(batch, order_by);
        }
        let k = k.unwrap();

        if k == 0 {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        // Pre-downcast sort columns for fast comparison (avoid repeated dynamic dispatch)
        enum TypedSortCol<'a> {
            Int64(&'a Int64Array, bool), // (array, descending)
            Float64(&'a Float64Array, bool),
            String(&'a StringArray, bool),
            Other(&'a ArrayRef, bool),
        }

        let typed_sort_cols: Vec<TypedSortCol> = order_by
            .iter()
            .filter_map(|clause| {
                let col_name = clause.column.trim_matches('"');
                // Try exact name first; only strip table prefix if the prefix part is a
                // simple identifier (no parens/brackets) — avoids mis-splitting float literals.
                let actual_col = if batch.column_by_name(col_name).is_some() {
                    col_name
                } else if let Some(dot_pos) = col_name.rfind('.') {
                    let prefix = &col_name[..dot_pos];
                    if prefix.chars().all(|c| c.is_alphanumeric() || c == '_') {
                        &col_name[dot_pos + 1..]
                    } else {
                        col_name
                    }
                } else {
                    col_name
                };
                batch.column_by_name(actual_col).map(|col| {
                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        TypedSortCol::Int64(arr, clause.descending)
                    } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                        TypedSortCol::Float64(arr, clause.descending)
                    } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        TypedSortCol::String(arr, clause.descending)
                    } else {
                        TypedSortCol::Other(col, clause.descending)
                    }
                })
            })
            .collect();

        if typed_sort_cols.is_empty() {
            return Ok(batch.clone());
        }

        // Fast comparison using pre-downcast columns
        let compare_rows = |a: usize, b: usize| -> Ordering {
            for col in &typed_sort_cols {
                let ord = match col {
                    TypedSortCol::Int64(arr, desc) => {
                        let a_null = arr.is_null(a);
                        let b_null = arr.is_null(b);
                        let ord = if a_null && b_null {
                            Ordering::Equal
                        } else if a_null {
                            Ordering::Greater
                        } else if b_null {
                            Ordering::Less
                        } else {
                            arr.value(a).cmp(&arr.value(b))
                        };
                        if *desc {
                            ord.reverse()
                        } else {
                            ord
                        }
                    }
                    TypedSortCol::Float64(arr, desc) => {
                        let a_null = arr.is_null(a);
                        let b_null = arr.is_null(b);
                        let ord = if a_null && b_null {
                            Ordering::Equal
                        } else if a_null {
                            Ordering::Greater
                        } else if b_null {
                            Ordering::Less
                        } else {
                            arr.value(a)
                                .partial_cmp(&arr.value(b))
                                .unwrap_or(Ordering::Equal)
                        };
                        if *desc {
                            ord.reverse()
                        } else {
                            ord
                        }
                    }
                    TypedSortCol::String(arr, desc) => {
                        let a_null = arr.is_null(a);
                        let b_null = arr.is_null(b);
                        let ord = if a_null && b_null {
                            Ordering::Equal
                        } else if a_null {
                            Ordering::Greater
                        } else if b_null {
                            Ordering::Less
                        } else {
                            arr.value(a).cmp(arr.value(b))
                        };
                        if *desc {
                            ord.reverse()
                        } else {
                            ord
                        }
                    }
                    TypedSortCol::Other(arr, desc) => {
                        let ord = Self::compare_array_values(arr, a, b);
                        if *desc {
                            ord.reverse()
                        } else {
                            ord
                        }
                    }
                };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        };

        // OPTIMIZATION: For small k, use heap-based top-k (O(n log k))
        // For larger k, use partial sort (O(n + k log k))
        let indices: Vec<usize> = if k <= 100 {
            // Heap-based approach for small k - maintains a max-heap of size k
            use std::collections::BinaryHeap;

            // Wrapper for reverse comparison (we want min-heap behavior for top-k)
            struct HeapItem(usize);

            impl PartialEq for HeapItem {
                fn eq(&self, other: &Self) -> bool {
                    self.0 == other.0
                }
            }
            impl Eq for HeapItem {}

            // Create comparison closure that captures typed_sort_cols
            let mut heap: BinaryHeap<(std::cmp::Reverse<usize>, usize)> =
                BinaryHeap::with_capacity(k + 1);

            // Simple approach: store (score, index) where score is computed once
            // For numeric DESC sorting, we can use the value directly as score
            if typed_sort_cols.len() == 1 {
                match &typed_sort_cols[0] {
                    TypedSortCol::Float64(arr, true) => {
                        // DESC sort on float - use value as score, keep k largest
                        let mut top_k: Vec<(i64, usize)> = Vec::with_capacity(k);
                        for i in 0..num_rows {
                            let score = if arr.is_null(i) {
                                i64::MIN
                            } else {
                                (arr.value(i) * 1e10) as i64 // Scale to preserve precision
                            };
                            if top_k.len() < k {
                                top_k.push((score, i));
                                if top_k.len() == k {
                                    top_k.sort_by(|a, b| b.0.cmp(&a.0));
                                }
                            } else if score > top_k[k - 1].0 {
                                top_k[k - 1] = (score, i);
                                // Bubble up
                                let mut j = k - 1;
                                while j > 0 && top_k[j].0 > top_k[j - 1].0 {
                                    top_k.swap(j, j - 1);
                                    j -= 1;
                                }
                            }
                        }
                        top_k.iter().map(|(_, i)| *i).collect()
                    }
                    TypedSortCol::Int64(arr, true) => {
                        // DESC sort on int - use value as score, keep k largest
                        let mut top_k: Vec<(i64, usize)> = Vec::with_capacity(k);
                        for i in 0..num_rows {
                            let score = if arr.is_null(i) {
                                i64::MIN
                            } else {
                                arr.value(i)
                            };
                            if top_k.len() < k {
                                top_k.push((score, i));
                                if top_k.len() == k {
                                    top_k.sort_by(|a, b| b.0.cmp(&a.0));
                                }
                            } else if score > top_k[k - 1].0 {
                                top_k[k - 1] = (score, i);
                                let mut j = k - 1;
                                while j > 0 && top_k[j].0 > top_k[j - 1].0 {
                                    top_k.swap(j, j - 1);
                                    j -= 1;
                                }
                            }
                        }
                        top_k.iter().map(|(_, i)| *i).collect()
                    }
                    _ => {
                        // Fall back to generic approach
                        let mut all_indices: Vec<usize> = (0..num_rows).collect();
                        all_indices.select_nth_unstable_by(k - 1, |&a, &b| compare_rows(a, b));
                        all_indices.truncate(k);
                        all_indices.sort_by(|&a, &b| compare_rows(a, b));
                        all_indices
                    }
                }
            } else {
                // Multi-column sort - use generic approach
                let mut all_indices: Vec<usize> = (0..num_rows).collect();
                all_indices.select_nth_unstable_by(k - 1, |&a, &b| compare_rows(a, b));
                all_indices.truncate(k);
                all_indices.sort_by(|&a, &b| compare_rows(a, b));
                all_indices
            }
        } else {
            // Larger k - use partial sort (more efficient for k > 100)
            let mut all_indices: Vec<usize> = (0..num_rows).collect();
            if k < num_rows {
                all_indices.select_nth_unstable_by(k - 1, |&a, &b| compare_rows(a, b));
                all_indices.truncate(k);
            }
            all_indices.sort_by(|&a, &b| compare_rows(a, b));
            all_indices
        };

        // Take rows by indices
        let indices_array =
            arrow::array::UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());

        let columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| compute::take(col, &indices_array, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| err_data(e.to_string()))?;

        RecordBatch::try_new(batch.schema(), columns).map_err(|e| err_data(e.to_string()))
    }

    /// Apply LIMIT and OFFSET
    pub(in crate::query::executor) fn apply_limit_offset(
        batch: &RecordBatch,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> io::Result<RecordBatch> {
        let start = offset.unwrap_or(0);
        let end = limit
            .map(|l| (start + l).min(batch.num_rows()))
            .unwrap_or(batch.num_rows());

        if start >= batch.num_rows() {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        let length = end - start;
        Ok(batch.slice(start, length))
    }

    /// Project columns according to SELECT list
    pub(in crate::query::executor) fn apply_projection(batch: &RecordBatch, columns: &[SelectColumn]) -> io::Result<RecordBatch> {
        Self::apply_projection_with_storage(batch, columns, None)
    }

    /// Project columns according to SELECT list (with optional storage path for scalar subqueries)
    pub(in crate::query::executor) fn apply_projection_with_storage(
        batch: &RecordBatch,
        columns: &[SelectColumn],
        storage_path: Option<&Path>,
    ) -> io::Result<RecordBatch> {
        // Handle simple SELECT * (only * with no other columns)
        if columns.len() == 1 && matches!(columns[0], SelectColumn::All) {
            return Ok(batch.clone());
        }

        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        let mut added_columns: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Collect all explicitly named columns to exclude from * expansion
        let mut explicit_cols: std::collections::HashSet<String> = std::collections::HashSet::new();
        for col in columns {
            match col {
                SelectColumn::Column(name) => {
                    let col_name = name.trim_matches('"');
                    let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                        &col_name[dot_pos + 1..]
                    } else {
                        col_name
                    };
                    explicit_cols.insert(actual_col.to_string());
                }
                SelectColumn::ColumnAlias { column, .. } => {
                    let col_name = column.trim_matches('"');
                    explicit_cols.insert(col_name.to_string());
                }
                _ => {}
            }
        }

        let json_candidates = columns
            .iter()
            .filter(|column| {
                matches!(
                    column,
                    SelectColumn::Expression { expr, .. }
                        if Self::json_projection_spec(expr).is_some()
                )
            })
            .take(2)
            .count();
        let mut fused_json = if json_candidates >= 2 {
            Self::fused_json_projection_arrays(batch, columns)?
        } else {
            HashMap::new()
        };

        // Process columns in order - preserving user-specified order
        for (column_index, col) in columns.iter().enumerate() {
            match col {
                SelectColumn::Column(name) => {
                    let col_name = name.trim_matches('"');
                    // Try full qualified name first (e.g., 'b.name' for self-join),
                    // then fall back to bare name.
                    let bare_col = if let Some(dot_pos) = col_name.rfind('.') {
                        &col_name[dot_pos + 1..]
                    } else {
                        col_name
                    };
                    let (out_name, array) = if let Some(arr) = batch.column_by_name(col_name) {
                        (bare_col, arr)
                    } else if let Some(arr) = batch.column_by_name(bare_col) {
                        (bare_col, arr)
                    } else {
                        continue;
                    };
                    if !added_columns.contains(out_name) {
                        fields.push(Field::new(out_name, array.data_type().clone(), true));
                        arrays.push(array.clone());
                        added_columns.insert(out_name.to_string());
                    }
                }
                SelectColumn::ColumnAlias { column, alias } => {
                    let col_name = column.trim_matches('"');
                    // Try full qualified name first (for self-join disambiguation),
                    // then bare name.
                    let bare_col = if let Some(dot_pos) = col_name.rfind('.') {
                        &col_name[dot_pos + 1..]
                    } else {
                        col_name
                    };
                    let array = batch
                        .column_by_name(col_name)
                        .or_else(|| batch.column_by_name(bare_col));
                    if let Some(array) = array {
                        fields.push(Field::new(alias, array.data_type().clone(), true));
                        arrays.push(array.clone());
                        added_columns.insert(alias.clone());
                    }
                }
                SelectColumn::Expression { expr, alias } => {
                    // Use storage-aware evaluation for expressions that may contain scalar subqueries
                    let array = if let Some(array) = fused_json.remove(&column_index) {
                        array
                    } else if let Some(path) = storage_path {
                        Self::evaluate_expr_to_array_with_storage(batch, expr, path)?
                    } else {
                        Self::evaluate_expr_to_array(batch, expr)?
                    };
                    let output_name = alias.clone().unwrap_or_else(|| "expr".to_string());
                    fields.push(Field::new(&output_name, array.data_type().clone(), true));
                    arrays.push(array);
                }
                SelectColumn::All => {
                    // Add all columns from batch EXCEPT:
                    // 1. _id (always skip here - it will be added at its explicit position if requested)
                    // 2. Columns already added (from explicit references before *)
                    for (i, field) in batch.schema().fields().iter().enumerate() {
                        let col_name = field.name();
                        if col_name == "_id" {
                            continue;
                        }
                        if !added_columns.contains(col_name) {
                            fields.push(field.as_ref().clone());
                            arrays.push(batch.column(i).clone());
                            added_columns.insert(col_name.clone());
                        }
                    }
                }
                SelectColumn::AllExclude(exclude) => {
                    for (i, field) in batch.schema().fields().iter().enumerate() {
                        let col_name = field.name();
                        if col_name == "_id" || exclude.iter().any(|e| e == col_name) {
                            continue;
                        }
                        if !added_columns.contains(col_name) {
                            fields.push(field.as_ref().clone());
                            arrays.push(batch.column(i).clone());
                            added_columns.insert(col_name.clone());
                        }
                    }
                }
                SelectColumn::AllReplace(replacements) => {
                    let replace_cols: Vec<&str> =
                        replacements.iter().map(|(_, col)| col.as_str()).collect();
                    for (i, field) in batch.schema().fields().iter().enumerate() {
                        let col_name = field.name();
                        if col_name == "_id" || replace_cols.iter().any(|c| *c == col_name) {
                            continue;
                        }
                        if !added_columns.contains(col_name) {
                            fields.push(field.as_ref().clone());
                            arrays.push(batch.column(i).clone());
                            added_columns.insert(col_name.clone());
                        }
                    }
                    for (expr, alias) in replacements {
                        let array = if let Some(path) = storage_path {
                            Self::evaluate_expr_to_array_with_storage(batch, expr, path)?
                        } else {
                            Self::evaluate_expr_to_array(batch, expr)?
                        };
                        fields.push(Field::new(alias, array.data_type().clone(), true));
                        arrays.push(array);
                    }
                }
                SelectColumn::Columns(pattern) => {
                    let re = regex::Regex::new(pattern).map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("Invalid regex pattern '{}': {}", pattern, e),
                        )
                    })?;
                    for (i, field) in batch.schema().fields().iter().enumerate() {
                        let col_name = field.name();
                        if col_name == "_id" {
                            continue;
                        }
                        if re.is_match(col_name) && !added_columns.contains(col_name) {
                            fields.push(field.as_ref().clone());
                            arrays.push(batch.column(i).clone());
                            added_columns.insert(col_name.clone());
                        }
                    }
                }
                SelectColumn::Aggregate { .. } => {
                    // Aggregates are handled separately
                }
                SelectColumn::WindowFunction { .. } => {
                    // Window functions not yet supported
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))
    }

    /// Execute aggregation query
    /// Compute a single aggregate function
    /// Execute GROUP BY aggregation query
    /// OPTIMIZATION: Uses vectorized execution engine for maximum performance
    /// Collect aggregate functions referenced in a HAVING expression that are NOT
    /// already present in the SELECT column list.  Returns (AggregateFunc, column) pairs.
    /// Materialize expression-based GROUP BY columns into the batch as virtual columns.
    /// For example, GROUP BY YEAR(date) evaluates YEAR(date) for each row and adds a
    /// column named "YEAR(date)" to the batch so the grouping logic can use it.
    /// Execute GROUP BY using vectorized execution engine
    /// Processes data in 2048-row batches for cache efficiency
    /// Build GROUP BY result from vectorized hash aggregation
    /// Check if we can use incremental aggregation (no DISTINCT, no complex expressions)
    /// Ultra-fast GROUP BY for string columns using direct dictionary indexing
    /// Uses cache-friendly sequential aggregation with bounds-check elimination
    /// Ultra-fast GROUP BY using direct array indexing for small integer ranges
    /// This avoids hash map overhead entirely - O(1) per row instead of hash lookup
    /// Fast incremental GROUP BY with parallel partitioned aggregation (DuckDB-style)
    /// Key optimizations:
    /// 1. Parallel partition-based aggregation for large datasets
    /// 2. Single-pass hash+aggregate for each partition
    /// 3. Merge partition results at the end
    /// Original GROUP BY with full row indices (for complex queries with DISTINCT or expressions)
    /// Fuse SUM(IF(string_column = 'literal', 1, 0)) aggregates that share a
    /// categorical source column. Wide Hive profiles commonly contain 10-30 of
    /// these counters. Evaluating each IF into a million-row temporary array and
    /// scanning all groups separately is avoidable: one categorical lookup per
    /// input row can update every requested output counter.
    /// Compute several PERCENTILE_APPROX calls over the same expression with one
    /// sort per group. Profile queries frequently request p10/p25/p50/p75/p90;
    /// independently sorting the same values five times is unnecessary.
    /// Evaluate an expression once per group. Aggregate subexpressions are first
    /// lowered to hidden Arrow columns, then the ordinary vectorized expression
    /// evaluator handles CASE/arithmetic/scalar functions over the group batch.
    /// Create a sub-batch containing only the specified row indices
    /// Evaluate condition that may contain aggregate functions
    /// Evaluate expression that may be an aggregate, returning scalar value
    /// Extract scalar value from array
    pub(in crate::query::executor) fn extract_scalar_from_array(arr: &ArrayRef) -> io::Result<f64> {
        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
            Ok(if int_arr.len() > 0 && !int_arr.is_null(0) {
                int_arr.value(0) as f64
            } else {
                0.0
            })
        } else if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>() {
            Ok(if float_arr.len() > 0 && !float_arr.is_null(0) {
                float_arr.value(0)
            } else {
                0.0
            })
        } else {
            Ok(0.0)
        }
    }

    /// Check if an expression contains a correlated subquery
    pub(in crate::query::executor) fn has_correlated_subquery(expr: &SqlExpr) -> bool {
        match expr {
            SqlExpr::ExistsSubquery { .. }
            | SqlExpr::InSubquery { .. }
            | SqlExpr::ScalarSubquery { .. } => true,
            SqlExpr::BinaryOp { left, right, .. } => {
                Self::has_correlated_subquery(left) || Self::has_correlated_subquery(right)
            }
            SqlExpr::UnaryOp { expr, .. } => Self::has_correlated_subquery(expr),
            SqlExpr::Paren(inner) => Self::has_correlated_subquery(inner),
            _ => false,
        }
    }

    pub(in crate::query::executor) fn coerce_numeric_for_comparison(
        left: ArrayRef,
        right: ArrayRef,
    ) -> io::Result<(ArrayRef, ArrayRef)> {
        use arrow::compute::cast;
        use arrow::datatypes::DataType;

        let l_is_f = left.as_any().downcast_ref::<Float64Array>().is_some();
        let r_is_f = right.as_any().downcast_ref::<Float64Array>().is_some();
        let l_is_i = left.as_any().downcast_ref::<Int64Array>().is_some();
        let r_is_i = right.as_any().downcast_ref::<Int64Array>().is_some();
        let l_is_u = left.as_any().downcast_ref::<UInt64Array>().is_some();
        let r_is_u = right.as_any().downcast_ref::<UInt64Array>().is_some();

        if l_is_f && r_is_i {
            let r2 = cast(&right, &DataType::Float64).map_err(|e| err_data(e.to_string()))?;
            return Ok((left, r2));
        }
        if l_is_i && r_is_f {
            let l2 = cast(&left, &DataType::Float64).map_err(|e| err_data(e.to_string()))?;
            return Ok((l2, right));
        }
        // UInt64 ↔ Int64 coercion (e.g. _id column is UInt64, literals are Int64)
        if l_is_u && r_is_i {
            let l2 = cast(&left, &DataType::Int64).map_err(|e| err_data(e.to_string()))?;
            return Ok((l2, right));
        }
        if l_is_i && r_is_u {
            let r2 = cast(&right, &DataType::Int64).map_err(|e| err_data(e.to_string()))?;
            return Ok((left, r2));
        }

        Ok((left, right))
    }

    /// OPTIMIZED: Extract _id = X pattern for O(1) lookup
    /// Returns Some(id) if WHERE clause is simple `_id = literal` or `literal = _id`
    #[inline]
    pub(in crate::query::executor) fn extract_id_equality_filter(expr: &SqlExpr) -> Option<u64> {
        use crate::query::sql_parser::BinaryOperator;

        if let SqlExpr::BinaryOp { left, op, right } = expr {
            if !matches!(op, BinaryOperator::Eq) {
                return None;
            }

            // Check _id = literal pattern
            if let SqlExpr::Column(col) = left.as_ref() {
                let col_name = col.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                if actual_col == "_id" {
                    if let SqlExpr::Literal(Value::Int64(id)) = right.as_ref() {
                        return Some(*id as u64);
                    }
                }
            }

            // Check literal = _id pattern
            if let SqlExpr::Column(col) = right.as_ref() {
                let col_name = col.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                if actual_col == "_id" {
                    if let SqlExpr::Literal(Value::Int64(id)) = left.as_ref() {
                        return Some(*id as u64);
                    }
                }
            }
        }
        None
    }

    /// Extract simple string equality filter: column = 'literal' or 'literal' = column
    /// Returns (column_name, literal_value, is_equality) if matches, None otherwise
    #[inline]
    pub(in crate::query::executor) fn extract_simple_string_filter(expr: &SqlExpr) -> Option<(String, String, bool)> {
        use crate::query::sql_parser::BinaryOperator;

        if let SqlExpr::BinaryOp { left, op, right } = expr {
            let is_eq = matches!(op, BinaryOperator::Eq);
            let is_neq = matches!(op, BinaryOperator::NotEq);

            if !is_eq && !is_neq {
                return None;
            }

            // Check column = 'literal' pattern
            if let (SqlExpr::Column(col), SqlExpr::Literal(Value::String(lit))) =
                (left.as_ref(), right.as_ref())
            {
                let col_name = col.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                return Some((actual_col.to_string(), lit.clone(), is_eq));
            }

            // Check 'literal' = column pattern
            if let (SqlExpr::Literal(Value::String(lit)), SqlExpr::Column(col)) =
                (left.as_ref(), right.as_ref())
            {
                let col_name = col.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                return Some((actual_col.to_string(), lit.clone(), is_eq));
            }
        }

        None
    }

    /// Collect column names from an expression (for determining required columns)
    pub(in crate::query::executor) fn collect_columns_from_expr(expr: &SqlExpr, columns: &mut Vec<String>) {
        match expr {
            SqlExpr::Column(name) => {
                let col_name = name.trim_matches('"');
                // Strip table alias prefix if present
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                if !columns.contains(&actual_col.to_string()) {
                    columns.push(actual_col.to_string());
                }
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                Self::collect_columns_from_expr(left, columns);
                Self::collect_columns_from_expr(right, columns);
            }
            SqlExpr::UnaryOp { expr, .. } => {
                Self::collect_columns_from_expr(expr, columns);
            }
            SqlExpr::Paren(inner) => {
                Self::collect_columns_from_expr(inner, columns);
            }
            SqlExpr::Function { args, .. } => {
                for arg in args {
                    Self::collect_columns_from_expr(arg, columns);
                }
            }
            SqlExpr::Case {
                when_then,
                else_expr,
            } => {
                for (cond, then_expr) in when_then {
                    Self::collect_columns_from_expr(cond, columns);
                    Self::collect_columns_from_expr(then_expr, columns);
                }
                if let Some(else_e) = else_expr {
                    Self::collect_columns_from_expr(else_e, columns);
                }
            }
            SqlExpr::Cast { expr, .. } => {
                Self::collect_columns_from_expr(expr, columns);
            }
            SqlExpr::Between {
                column, low, high, ..
            } => {
                // Handle BETWEEN expression: column BETWEEN low AND high
                let col_name = column.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                if !columns.contains(&actual_col.to_string()) {
                    columns.push(actual_col.to_string());
                }
                Self::collect_columns_from_expr(low, columns);
                Self::collect_columns_from_expr(high, columns);
            }
            SqlExpr::Like { column, .. } | SqlExpr::Regexp { column, .. } => {
                // Handle LIKE/REGEXP expressions
                let col_name = column.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                if !columns.contains(&actual_col.to_string()) {
                    columns.push(actual_col.to_string());
                }
            }
            SqlExpr::In { column, .. } => {
                // Handle IN expression (values are literals, not column refs)
                let col_name = column.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                if !columns.contains(&actual_col.to_string()) {
                    columns.push(actual_col.to_string());
                }
            }
            SqlExpr::IsNull { column, .. } => {
                // Handle IS NULL / IS NOT NULL (negated field handles both)
                let col_name = column.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                if !columns.contains(&actual_col.to_string()) {
                    columns.push(actual_col.to_string());
                }
            }
            SqlExpr::ExistsSubquery { stmt } | SqlExpr::ScalarSubquery { stmt } => {
                // For correlated subqueries, we need all outer columns that might be referenced
                // The subquery might reference outer columns like u.user_id
                if let Some(ref where_clause) = stmt.where_clause {
                    Self::collect_columns_from_expr(where_clause, columns);
                }
            }
            SqlExpr::InSubquery { column, stmt, .. } => {
                let col_name = column.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                if !columns.contains(&actual_col.to_string()) {
                    columns.push(actual_col.to_string());
                }
                if let Some(ref where_clause) = stmt.where_clause {
                    Self::collect_columns_from_expr(where_clause, columns);
                }
            }
            SqlExpr::ArrayIndex { array, index } => {
                Self::collect_columns_from_expr(array, columns);
                Self::collect_columns_from_expr(index, columns);
            }
            _ => {}
        }
    }

    /// Check if this is a pure COUNT(*) query that can be answered from metadata
    /// Returns true if: SELECT COUNT(*) FROM table (no WHERE, no GROUP BY, no HAVING)
    pub(in crate::query::executor) fn is_pure_count_star(stmt: &SelectStatement) -> bool {
        // Must have exactly one column which is COUNT(*)
        if stmt.columns.len() != 1 {
            return false;
        }

        // Must be COUNT(*) aggregate
        let is_count_star = match &stmt.columns[0] {
            SelectColumn::Aggregate {
                func,
                column,
                distinct,
                ..
            } => {
                matches!(func, AggregateFunc::Count)
                    && !distinct
                    && column
                        .as_ref()
                        .map(|c| {
                            c == "*"
                                || c.chars()
                                    .next()
                                    .map(|ch| ch.is_ascii_digit())
                                    .unwrap_or(false)
                        })
                        .unwrap_or(true)
            }
            _ => false,
        };

        if !is_count_star {
            return false;
        }

        // No WHERE clause
        if stmt.where_clause.is_some() {
            return false;
        }

        // No GROUP BY
        if !stmt.group_by.is_empty() {
            return false;
        }

        // No HAVING
        if stmt.having.is_some() {
            return false;
        }

        // No subquery in FROM
        if matches!(stmt.from, Some(FromItem::Subquery { .. })) {
            return false;
        }

        // No JOINs
        if !stmt.joins.is_empty() {
            return false;
        }

        true
    }

    /// Check if an expression contains an aggregate function (SUM, COUNT, AVG, MIN, MAX)
    pub(in crate::query::executor) fn expr_contains_aggregate(expr: &SqlExpr) -> bool {
        match expr {
            SqlExpr::Function { name, args } => {
                Self::is_group_aggregate_name(name)
                    || args.iter().any(Self::expr_contains_aggregate)
            }
            SqlExpr::Case {
                when_then,
                else_expr,
            } => {
                when_then.iter().any(|(c, t)| {
                    Self::expr_contains_aggregate(c) || Self::expr_contains_aggregate(t)
                }) || else_expr
                    .as_ref()
                    .map_or(false, |e| Self::expr_contains_aggregate(e))
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                Self::expr_contains_aggregate(left) || Self::expr_contains_aggregate(right)
            }
            SqlExpr::UnaryOp { expr, .. } | SqlExpr::Paren(expr) | SqlExpr::Cast { expr, .. } => {
                Self::expr_contains_aggregate(expr)
            }
            _ => false,
        }
    }

    /// Check if an expression contains a scalar subquery
    pub(in crate::query::executor) fn expr_contains_scalar_subquery(expr: &SqlExpr) -> bool {
        match expr {
            SqlExpr::ScalarSubquery { .. } => true,
            SqlExpr::BinaryOp { left, right, .. } => {
                Self::expr_contains_scalar_subquery(left)
                    || Self::expr_contains_scalar_subquery(right)
            }
            SqlExpr::UnaryOp { expr, .. } | SqlExpr::Paren(expr) | SqlExpr::Cast { expr, .. } => {
                Self::expr_contains_scalar_subquery(expr)
            }
            SqlExpr::Case {
                when_then,
                else_expr,
            } => {
                when_then.iter().any(|(c, t)| {
                    Self::expr_contains_scalar_subquery(c) || Self::expr_contains_scalar_subquery(t)
                }) || else_expr
                    .as_ref()
                    .map_or(false, |e| Self::expr_contains_scalar_subquery(e))
            }
            _ => false,
        }
    }

    /// Take first value from each group
    /// Compute aggregate for each group (parallelized for large datasets)
    /// FAST PATH: Compute simple aggregates directly from mmap bytes.
    /// Handles COUNT(*), COUNT(col), SUM(col), AVG(col), MIN(col), MAX(col)
    /// for numeric columns without WHERE/GROUP BY.
    /// Caches stats per column to avoid redundant mmap scans.
    /// Returns None if the query can't be handled by this fast path.
    pub(crate) fn try_mmap_aggregation(
        backend: &crate::storage::TableStorageBackend,
        stmt: &crate::query::SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::{AggregateFunc, SelectColumn};
        use std::collections::HashMap;
        let active_count = backend.active_row_count() as i64;

        // FAST PATH: COUNT(DISTINCT col) — global dict cache + null-aware count.
        // Warm calls: O(dict_size) ≈ O(10) — dict already cached, just count entries.
        // Cold calls: O(N) to build dict (same as before), but result is cached for next call.
        // Null-aware: uses null bitmaps to exclude NULL rows from the count.
        if stmt.columns.len() == 1 {
            if let SelectColumn::Aggregate {
                func: AggregateFunc::Count,
                column: Some(col_name),
                distinct: true,
                alias,
            } = &stmt.columns[0]
            {
                let actual = col_name.trim_matches('"');
                let actual = if let Some(p) = actual.rfind('.') {
                    &actual[p + 1..]
                } else {
                    actual
                };
                if let Some(dict) = crate::storage::backend::get_global_dict_cache(
                    backend.path(),
                    actual,
                    &backend.storage,
                )? {
                    let (dict_strings, group_ids) = (dict.0.as_slice(), dict.1.as_slice());
                    let count =
                        backend.count_distinct_with_dict(actual, dict_strings, group_ids)?;
                    let out = alias
                        .clone()
                        .unwrap_or_else(|| format!("COUNT(DISTINCT {})", actual));
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        &out,
                        ArrowDataType::Int64,
                        false,
                    )]));
                    let arr: ArrayRef = Arc::new(Int64Array::from(vec![count]));
                    let batch = RecordBatch::try_new(schema, vec![arr])
                        .map_err(|e| err_data(e.to_string()))?;
                    return Ok(Some(ApexResult::Data(batch)));
                }
                return Ok(None); // non-string col — fall through to slow path
            }
        }

        // Collect unique column names for a single-pass multi-column aggregation
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
                    if !is_count_star && !unique_cols.contains(col_name) {
                        unique_cols.push(col_name.clone());
                    }
                } else if !matches!(func, AggregateFunc::Count) {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }

        // Single-pass multi-column aggregation (replaces N separate column scans)
        // execute_simple_agg uses the RCIX streaming path: one mmap pass for all columns
        let col_refs: Vec<&str> = unique_cols.iter().map(|s| s.as_str()).collect();
        // (count: i64, sum: f64, min: f64, max: f64, is_int: bool)
        let stats_vec = match backend.execute_simple_agg(&col_refs)? {
            Some(v) => v,
            None => return Ok(None),
        };
        // Map column name → (count, sum, min, max, is_int)
        let stats_cache: HashMap<&str, (i64, f64, f64, f64, bool)> = unique_cols
            .iter()
            .enumerate()
            .filter_map(|(i, name)| stats_vec.get(i).map(|&s| (name.as_str(), s)))
            .collect();

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
                        let is_star = column.as_deref() == Some("*")
                            || column.is_none()
                            || column
                                .as_ref()
                                .map(|c| {
                                    c.chars()
                                        .next()
                                        .map(|ch| ch.is_ascii_digit())
                                        .unwrap_or(false)
                                })
                                .unwrap_or(false);
                        if is_star {
                            fields.push(Field::new(&output_name, ArrowDataType::Int64, false));
                            arrays.push(Arc::new(Int64Array::from(vec![active_count])));
                        } else {
                            let col_name = column.as_ref().unwrap().as_str();
                            let count = stats_cache.get(col_name).map(|s| s.0).unwrap_or(0);
                            fields.push(Field::new(&output_name, ArrowDataType::Int64, false));
                            arrays.push(Arc::new(Int64Array::from(vec![count])));
                        }
                    }
                    AggregateFunc::Sum => {
                        let col_name = column.as_ref().unwrap().as_str();
                        let s = match stats_cache.get(col_name) {
                            Some(s) => s,
                            None => return Ok(None),
                        };
                        if s.4 {
                            // is_int
                            fields.push(Field::new(&output_name, ArrowDataType::Int64, true));
                            arrays.push(Arc::new(Int64Array::from(vec![s.1 as i64])));
                        } else {
                            fields.push(Field::new(&output_name, ArrowDataType::Float64, true));
                            arrays.push(Arc::new(Float64Array::from(vec![s.1])));
                        }
                    }
                    AggregateFunc::Avg => {
                        let col_name = column.as_ref().unwrap().as_str();
                        let s = match stats_cache.get(col_name) {
                            Some(s) => s,
                            None => return Ok(None),
                        };
                        let avg = if s.0 > 0 { s.1 / s.0 as f64 } else { 0.0 };
                        fields.push(Field::new(&output_name, ArrowDataType::Float64, true));
                        arrays.push(Arc::new(Float64Array::from(vec![avg])));
                    }
                    AggregateFunc::Min => {
                        let col_name = column.as_ref().unwrap().as_str();
                        let s = match stats_cache.get(col_name) {
                            Some(s) => s,
                            None => return Ok(None),
                        };
                        fields.push(Field::new(&output_name, ArrowDataType::Float64, true));
                        if s.0 == 0 {
                            arrays.push(Arc::new(Float64Array::from(vec![None as Option<f64>])));
                        } else {
                            if s.4 {
                                // is_int
                                fields.pop();
                                fields.push(Field::new(&output_name, ArrowDataType::Int64, true));
                                arrays.push(Arc::new(Int64Array::from(vec![s.2 as i64])));
                            } else {
                                arrays.push(Arc::new(Float64Array::from(vec![s.2])));
                            }
                        }
                    }
                    AggregateFunc::Max => {
                        let col_name = column.as_ref().unwrap().as_str();
                        let s = match stats_cache.get(col_name) {
                            Some(s) => s,
                            None => return Ok(None),
                        };
                        fields.push(Field::new(&output_name, ArrowDataType::Float64, true));
                        if s.0 == 0 {
                            arrays.push(Arc::new(Float64Array::from(vec![None as Option<f64>])));
                        } else {
                            if s.4 {
                                // is_int
                                fields.pop();
                                fields.push(Field::new(&output_name, ArrowDataType::Int64, true));
                                arrays.push(Arc::new(Int64Array::from(vec![s.3 as i64])));
                            } else {
                                arrays.push(Arc::new(Float64Array::from(vec![s.3])));
                            }
                        }
                    }
                }
            }
        }

        if fields.is_empty() {
            return Ok(None);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;
        Ok(Some(ApexResult::Data(batch)))
    }
}
