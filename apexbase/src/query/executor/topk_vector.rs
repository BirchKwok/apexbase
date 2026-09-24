// Vector top-k: explode_rename(topk_distance(...)) detection and distance computation.

impl ApexExecutor {
    /// Detect `SELECT explode_rename(topk_distance(col,[q],k,'m'), "n1","n2") FROM table`.
    /// Returns `(col, query, k, metric, names)` if the pattern matches, else `None`.
    fn detect_topk_explode(
        stmt: &SelectStatement,
    ) -> Option<(&str, &[f64], usize, &str, &[String])> {
        // Must have a real FROM table (not None / subquery / table-function)
        if !matches!(&stmt.from, Some(FromItem::Table { .. })) {
            return None;
        }
        // Exactly one SELECT column that is an Expression wrapping ExplodeRename(TopkDistance)
        if stmt.columns.len() == 1 {
            if let SelectColumn::Expression {
                expr: SqlExpr::ExplodeRename { inner, names },
                ..
            } = &stmt.columns[0]
            {
                if let SqlExpr::TopkDistance {
                    col,
                    query,
                    k,
                    metric,
                } = inner.as_ref()
                {
                    return Some((
                        col.as_str(),
                        query.as_slice(),
                        *k,
                        metric.as_str(),
                        names.as_slice(),
                    ));
                }
            }
        }
        None
    }

    /// Execute `explode_rename(topk_distance(col,[q],k,'m'), "name1","name2")`.
    ///
    /// Returns a RecordBatch with exactly 2 columns:
    /// - `names[0]`: Int64 — the `_id` values of the top-k rows
    /// - `names[1]`: Float64 — the corresponding distances
    ///
    /// The result has k rows, sorted ascending by distance.
    fn execute_topk_explode(
        storage_path: &Path,
        col: &str,
        query: &[f64],
        k: usize,
        metric: &str,
        names: &[String],
        where_clause: Option<&SqlExpr>,
    ) -> io::Result<RecordBatch> {
        use crate::query::vector_ops::DistanceMetric;
        use arrow::array::{Float64Array, Int64Array};

        if !storage_path.exists() {
            let tbl = storage_path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy();
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("topk_distance: table '{}' does not exist", tbl),
            ));
        }

        let metric_enum = DistanceMetric::from_str(metric).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("topk_distance: unknown metric '{}'", metric),
            )
        })?;

        let query_f32: Vec<f32> = query.iter().map(|&x| x as f32).collect();
        if query_f32.is_empty() || query_f32.iter().any(|value| !value.is_finite()) {
            return Err(err_input(
                "topk_distance: query vector must be non-empty and contain only finite values",
            ));
        }

        let backend = get_cached_backend(storage_path)?;

        // Output schema: names[0]=Int64(_id), names[1]=Float64(dist)
        let id_field = Field::new(&names[0], ArrowDataType::Int64, false);
        let dist_field = Field::new(&names[1], ArrowDataType::Float64, false);
        let out_schema = Arc::new(Schema::new(vec![id_field, dist_field]));

        use crate::query::vector_ops::DistanceComputer;
        let computer = DistanceComputer::new(metric_enum, query_f32);

        // A TopK expression is an aggregate over its input relation.  When a WHERE
        // clause is present, filter the narrow input (_id, vector, predicate
        // columns) before computing TopK.  This also avoids touching unrelated
        // wide/BLOB columns.
        if let Some(predicate) = where_clause {
            let mut required = vec!["_id".to_string(), col.to_string()];
            Self::collect_columns_from_expr(predicate, &mut required);
            let refs = required.iter().map(String::as_str).collect::<Vec<_>>();
            let input = backend.read_columns_to_arrow(Some(&refs), 0, None)?;
            let filtered = Self::apply_filter_with_storage(&input, predicate, storage_path)?;
            return Self::topk_explode_from_batch(
                &filtered,
                col,
                &computer,
                k,
                out_schema,
            );
        }

        // FAST PATH: zero-copy scan directly on OS mmap (no Arrow batch, no memcpy)
        let direct_topk = backend
            .topk_fixedlist_direct(col, &computer, k)
            .ok()
            .flatten()
            .or_else(|| backend.topk_binary_direct(col, &computer, k).ok().flatten());
        if let Some(topk) = direct_topk {
            if topk.is_empty() {
                return RecordBatch::try_new(
                    out_schema,
                    vec![
                        Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
                        Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
                    ],
                )
                .map_err(|e| err_data(e.to_string()));
            }
            // Read only the _id column (8MB) instead of all columns (512MB+)
            let id_batch = backend.read_columns_to_arrow(Some(&["_id"]), 0, None)?;
            let id_col = id_batch.column_by_name("_id");
            let ids: Vec<i64> = topk
                .iter()
                .map(|(row_idx, _)| {
                    id_col
                        .and_then(|a| a.as_any().downcast_ref::<arrow::array::Int64Array>())
                        .map(|a| a.value(*row_idx))
                        .unwrap_or(*row_idx as i64)
                })
                .collect();
            let dists: Vec<f64> = topk.iter().map(|(_, d)| *d as f64).collect();
            return RecordBatch::try_new(
                out_schema,
                vec![
                    Arc::new(Int64Array::from(ids)) as ArrayRef,
                    Arc::new(Float64Array::from(dists)) as ArrayRef,
                ],
            )
            .map_err(|e| err_data(e.to_string()));
        }

        // FALLBACK: full Arrow path (Binary columns / compressed RGs)
        let full_batch = backend.read_columns_to_arrow(None, 0, None)?;

        if full_batch.num_rows() == 0 {
            return RecordBatch::try_new(
                out_schema,
                vec![
                    Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
                    Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
                ],
            )
            .map_err(|e| err_data(e.to_string()));
        }

        Self::topk_explode_from_batch(&full_batch, col, &computer, k, out_schema)
    }

    fn topk_explode_from_batch(
        batch: &RecordBatch,
        col: &str,
        computer: &crate::query::vector_ops::DistanceComputer,
        k: usize,
        out_schema: Arc<Schema>,
    ) -> io::Result<RecordBatch> {
        use crate::query::vector_ops::{
            topk_heap_direct_parallel, topk_heap_direct_parallel_fixed,
        };
        use arrow::array::{BinaryArray, Float64Array, Int64Array};

        if batch.num_rows() == 0 {
            return RecordBatch::try_new(
                out_schema,
                vec![
                    Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
                    Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
                ],
            )
            .map_err(|e| err_data(e.to_string()));
        }

        let bin_col = batch.column_by_name(col).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("topk_distance: column '{}' not found", col),
            )
        })?;
        let effective_k = k.min(batch.num_rows());
        let topk = if let Some(fixed_arr) = bin_col
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeListArray>()
        {
            if fixed_arr.value_length() as usize != computer.query.len() {
                return Err(err_input(format!(
                    "topk_distance: query dimension {} does not match column dimension {}",
                    computer.query.len(),
                    fixed_arr.value_length()
                )));
            }
            topk_heap_direct_parallel_fixed(fixed_arr, computer, effective_k)
        } else if let Some(bin_arr) = bin_col.as_any().downcast_ref::<BinaryArray>() {
            let expected = computer.query.len() * std::mem::size_of::<f32>();
            if let Some(actual) = (0..bin_arr.len())
                .find(|&idx| !bin_arr.is_null(idx))
                .map(|idx| bin_arr.value(idx).len())
            {
                if actual != expected {
                    return Err(err_input(format!(
                        "topk_distance: query dimension {} does not match column dimension {}",
                        computer.query.len(),
                        actual / std::mem::size_of::<f32>()
                    )));
                }
            }
            topk_heap_direct_parallel(bin_arr, computer, effective_k)
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("topk_distance: column '{}' is not a vector column", col),
            ));
        };

        let id_col = batch.column_by_name("_id");
        let ids: Vec<i64> = topk
            .iter()
            .map(|(row_idx, _)| {
                if let Some(id_arr) = &id_col {
                    if let Some(arr) = id_arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
                        return arr.value(*row_idx);
                    }
                }
                *row_idx as i64
            })
            .collect();
        let dists: Vec<f64> = topk.iter().map(|(_, d)| *d as f64).collect();

        RecordBatch::try_new(
            out_schema,
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(Float64Array::from(dists)) as ArrayRef,
            ],
        )
        .map_err(|e| err_data(e.to_string()))
    }

    /// Execute `TOPK_DISTANCE(col, [vec], k, 'metric')` table function.
    ///
    /// Algorithm (O(n log k)):
    /// 1. Read the full RecordBatch from storage.
    /// 2. Locate the binary vector column `col`.
    /// 3. Run `topk_heap_direct` — single-pass fused distance + max-heap.
    /// 4. Gather only the top-k rows via `arrow::compute::take`.
    /// 5. Append a `dist` (Float64) column with the computed distances.
    fn execute_topk_distance(
        storage_path: &Path,
        col: &str,
        query: &[f64],
        k: usize,
        metric: &str,
    ) -> io::Result<RecordBatch> {
        use crate::query::vector_ops::DistanceMetric;
        use arrow::array::{BinaryArray, Float64Array, UInt32Array};
        use arrow::compute;
        use arrow::datatypes::DataType as ArrowDT;

        if !storage_path.exists() {
            let tbl = storage_path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy();
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("topk_distance: table '{}' does not exist", tbl),
            ));
        }

        let metric_enum = DistanceMetric::from_str(metric).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("topk_distance: unknown metric '{}'", metric),
            )
        })?;

        let query_f32: Vec<f32> = query.iter().map(|&x| x as f32).collect();
        if query_f32.is_empty() || query_f32.iter().any(|value| !value.is_finite()) {
            return Err(err_input(
                "topk_distance: query vector must be non-empty and contain only finite values",
            ));
        }

        let backend = get_cached_backend(storage_path)?;
        let full_batch = backend.read_columns_to_arrow(None, 0, None)?;

        // Build the schema with the extra `dist` column
        let mut fields: Vec<Field> = full_batch
            .schema()
            .fields()
            .iter()
            .map(|f| (**f).clone())
            .collect();
        fields.push(Field::new("dist", ArrowDT::Float64, false));
        let out_schema = Arc::new(Schema::new(fields));

        if full_batch.num_rows() == 0 {
            let empty_cols: Vec<ArrayRef> = out_schema
                .fields()
                .iter()
                .map(|f| arrow::array::new_empty_array(f.data_type()))
                .collect();
            return RecordBatch::try_new(out_schema, empty_cols)
                .map_err(|e| err_data(e.to_string()));
        }

        let bin_col = full_batch.column_by_name(col).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("topk_distance: column '{}' not found", col),
            )
        })?;

        // Parallel O(n/T log k) heap — dispatch on Binary vs FixedSizeList
        use crate::query::vector_ops::{
            topk_heap_direct_parallel, topk_heap_direct_parallel_fixed, DistanceComputer,
        };
        let computer = DistanceComputer::new(metric_enum, query_f32);
        let effective_k = k.min(full_batch.num_rows());
        let topk = if let Some(fixed_arr) = bin_col
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeListArray>()
        {
            if fixed_arr.value_length() as usize != computer.query.len() {
                return Err(err_input(format!(
                    "topk_distance: query dimension {} does not match column dimension {}",
                    computer.query.len(),
                    fixed_arr.value_length()
                )));
            }
            topk_heap_direct_parallel_fixed(fixed_arr, &computer, effective_k)
        } else if let Some(bin_arr) = bin_col.as_any().downcast_ref::<BinaryArray>() {
            let expected = computer.query.len() * std::mem::size_of::<f32>();
            if let Some(actual) = (0..bin_arr.len())
                .find(|&idx| !bin_arr.is_null(idx))
                .map(|idx| bin_arr.value(idx).len())
            {
                if actual != expected {
                    return Err(err_input(format!(
                        "topk_distance: query dimension {} does not match column dimension {}",
                        computer.query.len(),
                        actual / std::mem::size_of::<f32>()
                    )));
                }
            }
            topk_heap_direct_parallel(bin_arr, &computer, effective_k)
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("topk_distance: column '{}' is not a vector column", col),
            ));
        };

        if topk.is_empty() {
            let empty_cols: Vec<ArrayRef> = out_schema
                .fields()
                .iter()
                .map(|f| arrow::array::new_empty_array(f.data_type()))
                .collect();
            return RecordBatch::try_new(out_schema, empty_cols)
                .map_err(|e| err_data(e.to_string()));
        }

        // Gather only the top-k rows from the full batch
        let take_indices =
            UInt32Array::from(topk.iter().map(|(i, _)| *i as u32).collect::<Vec<_>>());
        let distances: Vec<f64> = topk.iter().map(|(_, d)| *d as f64).collect();

        let mut new_cols: Vec<ArrayRef> = Vec::with_capacity(full_batch.num_columns() + 1);
        for col_arr in full_batch.columns() {
            let taken = compute::take(col_arr.as_ref(), &take_indices, None)
                .map_err(|e| err_data(e.to_string()))?;
            new_cols.push(taken);
        }
        new_cols.push(Arc::new(Float64Array::from(distances)) as ArrayRef);

        RecordBatch::try_new(out_schema, new_cols).map_err(|e| err_data(e.to_string()))
    }
}
