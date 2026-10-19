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

impl ApexExecutor {
    /// Metric for a SQL distance function name, or `None` when the name is not
    /// one of the pairwise vector distance functions the SQL layer exposes.
    ///
    /// Must stay in step with the `evaluate_function` dispatch in
    /// `expressions.rs`; a name accepted here but not there (or the reverse)
    /// would make the fast path disagree with the general one.
    fn distance_metric_for_name(name: &str) -> Option<crate::query::vector_ops::DistanceMetric> {
        use crate::query::vector_ops::DistanceMetric;
        Some(match name.to_ascii_uppercase().as_str() {
            "ARRAY_DISTANCE" | "L2_DISTANCE" | "ARRAY_L2_DISTANCE" | "EUCLIDEAN_DISTANCE" => {
                DistanceMetric::L2
            }
            "L2_SQUARED_DISTANCE" | "SQUARED_L2" => DistanceMetric::L2Squared,
            "COSINE_DISTANCE" | "ARRAY_COSINE_DISTANCE" => DistanceMetric::CosineDistance,
            "COSINE_SIMILARITY" | "ARRAY_COSINE_SIMILARITY" => DistanceMetric::CosineSimilarity,
            "INNER_PRODUCT" | "DOT_PRODUCT" | "ARRAY_INNER_PRODUCT" => DistanceMetric::InnerProduct,
            "NEGATIVE_INNER_PRODUCT" | "ARRAY_NEGATIVE_INNER_PRODUCT" => {
                DistanceMetric::NegInnerProduct
            }
            "L1_DISTANCE" | "ARRAY_L1_DISTANCE" | "MANHATTAN_DISTANCE" => DistanceMetric::L1,
            "LINF_DISTANCE" | "ARRAY_LINF_DISTANCE" | "CHEBYSHEV_DISTANCE" => DistanceMetric::LInf,
            _ => return None,
        })
    }

    /// Column, query vector and metric of an `ORDER BY <distance>(col, [q])`
    /// clause, also resolving the `SELECT <expr> AS alias ... ORDER BY alias`
    /// spelling. `None` when the clause is anything else.
    fn order_by_distance_spec(
        stmt: &SelectStatement,
    ) -> Option<(String, Vec<f32>, crate::query::vector_ops::DistanceMetric)> {
        if stmt.order_by.len() != 1 {
            return None;
        }
        let clause = &stmt.order_by[0];
        if clause.descending {
            return None;
        }
        let expression = match clause.expr.as_ref() {
            Some(expression) => expression,
            None => {
                let alias = clause.column.trim_matches('"');
                stmt.columns.iter().find_map(|column| match column {
                    SelectColumn::Expression {
                        expr,
                        alias: Some(name),
                    } if name.eq_ignore_ascii_case(alias) => Some(expr),
                    _ => None,
                })?
            }
        };
        let SqlExpr::Function { name, args } = expression else {
            return None;
        };
        if args.len() != 2 {
            return None;
        }
        let metric = Self::distance_metric_for_name(name)?;
        let SqlExpr::Column(column) = &args[0] else {
            return None;
        };
        // Only a literal query vector can be evaluated without a batch here; the
        // other accepted forms (a vector column reference, …) keep the general
        // path, which resolves them against the row batch.
        let query: Vec<f32> = match &args[1] {
            SqlExpr::ArrayLiteral(values) => values.iter().map(|&value| value as f32).collect(),
            SqlExpr::Literal(crate::data::Value::String(text)) => {
                let text = text.trim().trim_start_matches('[').trim_end_matches(']');
                text.split(',')
                    .map(|token| token.trim().parse::<f32>())
                    .collect::<Result<Vec<f32>, _>>()
                    .ok()?
            }
            _ => return None,
        };
        if query.is_empty() || query.iter().any(|value| !value.is_finite()) {
            return None;
        }
        Some((column.trim_matches('"').to_string(), query, metric))
    }

    /// Rows for `SELECT ... ORDER BY <distance>(col, [q]) LIMIT k` over a table.
    ///
    /// The general path evaluates the distance for every row — allocating a
    /// `Vec<f32>` per row — and then fully sorts the result. The exact vector
    /// TopK lane already has an allocation-free SIMD distance plus a bounded
    /// heap; running that over the vector column and gathering the surviving
    /// rows gives the same rows without the per-row allocation or the full sort.
    /// The shared tail still re-applies ORDER BY (on `k` rows), projection and
    /// LIMIT, so the output shape is unchanged.
    fn try_fast_order_by_distance(
        backend: &crate::storage::backend::TableStorageBackend,
        stmt: &SelectStatement,
        storage_path: &Path,
    ) -> io::Result<Option<RecordBatch>> {
        use crate::query::vector_ops::{
            topk_heap_direct_parallel, topk_heap_direct_parallel_fixed, DistanceComputer,
        };
        use arrow::array::{BinaryArray, FixedSizeListArray, UInt32Array};

        // Grouping, DISTINCT (including `DISTINCT ON`), OFFSET, a window or an
        // aggregate has to run over the whole relation before the TopK is
        // meaningful — or would change meaning if only the surviving rows reached
        // it. A WHERE is allowed: the predicate is applied to the candidate rows
        // before the heap below.
        if !stmt.group_by.is_empty()
            || stmt.having.is_some()
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || stmt.window_row_number_limit.is_some()
            || stmt.offset.unwrap_or(0) > 0
            || stmt
                .columns
                .iter()
                .any(|column| matches!(column, SelectColumn::WindowFunction { .. }))
            || stmt.columns.iter().any(|column| match column {
                SelectColumn::Aggregate { .. } => true,
                SelectColumn::Expression { expr, .. } => Self::expr_contains_aggregate(expr),
                _ => false,
            })
        {
            return Ok(None);
        }
        let Some(limit) = stmt.limit else {
            return Ok(None);
        };
        if limit == 0 {
            return Ok(None);
        }
        let Some((column, query, metric)) = Self::order_by_distance_spec(stmt) else {
            return Ok(None);
        };
        // The heap lane returns the rows *nearest* in the metric's own sense,
        // while `ORDER BY <fn> ASC` means "smallest function value". The two
        // agree for distances and for `negative_inner_product` (whose value
        // already negates the dot product), but the similarity-valued functions
        // would come back in the opposite order.
        if matches!(
            metric,
            crate::query::vector_ops::DistanceMetric::InnerProduct
                | crate::query::vector_ops::DistanceMetric::CosineSimilarity
        ) {
            return Ok(None);
        }

        // Only the columns the statement needs: the TopK keeps `k` rows, so
        // decoding an unprojected wide column (a long text or blob column) for
        // every row would dominate the scan. `required_columns` covers the
        // projection, the ordering expression and any residual predicate, and
        // returns `None` for `SELECT *` — the same contract the general scan
        // branch relies on.
        let col_refs = Self::get_col_refs(stmt);
        let col_refs_vec: Option<Vec<&str>> = col_refs
            .as_ref()
            .map(|refs| refs.iter().map(|name| name.as_str()).collect());

        let computer = DistanceComputer::new(metric, query);

        // Filter-aware pass (V5 §6.4 item 16): when the predicate selects a
        // minority, read only those rows and score just them. The predicate runs
        // again on the candidate batch, so a bitmap conjunct that is not part of
        // the row selection (or a conjunction the scanner only partly applied)
        // still filters exactly.
        if let Some(where_clause) = stmt.where_clause.as_ref() {
            return Self::filtered_distance_topk(
                backend,
                where_clause,
                storage_path,
                &column,
                &computer,
                col_refs_vec.as_deref(),
                limit,
            );
        }

        // Zero-copy scoring straight off the mmap when the persisted base is the
        // whole visible view: the direct lane returns the surviving row indices
        // (it declines deletions, pending deltas, compressed groups and a bare
        // dimension mismatch) already ordered nearest-first, so only those `k`
        // rows need materializing instead of the whole vector column.
        if backend.overlay_state().is_clean_view() {
            let direct = backend
                .topk_fixedlist_direct(&column, &computer, limit)
                .ok()
                .flatten()
                .or_else(|| {
                    backend
                        .topk_binary_direct(&column, &computer, limit)
                        .ok()
                        .flatten()
                });
            if let Some(topk) = direct {
                if topk.is_empty() {
                    return Ok(None);
                }
                crate::query::executor::record_path("order_by_distance_topk_direct");
                let indices: Vec<usize> = topk.iter().map(|(index, _)| *index).collect();
                let selected =
                    backend.read_columns_by_indices_to_arrow(&indices, col_refs_vec.as_deref())?;
                return Ok(Some(selected));
            }
        }

        let batch = backend.read_columns_to_arrow(col_refs_vec.as_deref(), 0, None)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let Some(values) = batch.column_by_name(&column) else {
            return Ok(None);
        };
        let effective_k = limit.min(batch.num_rows());
        let topk = if let Some(fixed) = values.as_any().downcast_ref::<FixedSizeListArray>() {
            // The heap kernel reads the flat child values, so a NULL row would
            // be scored as zeros; the general path sorts NULL distances last and
            // must not be replaced when any exist.
            if fixed.null_count() > 0 || fixed.value_length() as usize != computer.query.len() {
                return Ok(None);
            }
            topk_heap_direct_parallel_fixed(fixed, &computer, effective_k)
        } else if let Some(binary) = values.as_any().downcast_ref::<BinaryArray>() {
            if binary.null_count() > 0 {
                return Ok(None);
            }
            topk_heap_direct_parallel(binary, &computer, effective_k)
        } else {
            return Ok(None);
        };
        if topk.is_empty() {
            return Ok(None);
        }

        let indices: Vec<u32> = topk.iter().map(|(index, _)| *index as u32).collect();
        let take = UInt32Array::from(indices);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        for array in batch.columns() {
            let taken = arrow::compute::take(array.as_ref(), &take, None)
                .map_err(|error| err_data(error.to_string()))?;
            columns.push(taken);
        }
        RecordBatch::try_new(batch.schema(), columns)
            .map(Some)
            .map_err(|error| err_data(error.to_string()))
    }
}

impl ApexExecutor {
    /// Candidate row positions for the filter-aware distance TopK.
    ///
    /// `None` means "this predicate cannot drive a row selection"; the caller
    /// then keeps the general path. Only single-column predicates the storage
    /// layer can scan without materializing the projection are accepted: a
    /// `MATCH()` never reaches here, because the FTS row selection runs first
    /// and claims every shape whose bitmap is selective enough for this lane.
    fn distance_lane_candidates(
        backend: &crate::storage::backend::TableStorageBackend,
        where_clause: &SqlExpr,
    ) -> io::Result<Option<Vec<usize>>> {
        // A single-column predicate the mmap scanners can evaluate directly. The
        // extractors only match a complete predicate, so a conjunction falls
        // through to the general path rather than selecting the wrong rows.
        if let Some((column, low, high)) = Self::extract_between_range(where_clause)
            .or_else(|| Self::extract_two_sided_same_col_range(where_clause))
            .or_else(|| Self::extract_single_comparison_as_range(where_clause))
        {
            return backend.scan_numeric_range_mmap(&column, low, high, None);
        }
        if let Some((column, value)) = Self::extract_string_equality(where_clause) {
            return backend.scan_string_filter_mmap(&column, &value, None);
        }
        Ok(None)
    }

    /// Rows for `SELECT ... WHERE <predicate> ORDER BY <distance>(col, [q]) LIMIT k`.
    ///
    /// Restricting the scan to the predicate's rows keeps the vector column out
    /// of the read for everything the predicate rejects, and the bounded heap
    /// replaces the general path's per-row allocation plus full sort. The shared
    /// tail still applies the predicate (now a no-op), ORDER BY on `k` rows, the
    /// projection and LIMIT.
    fn filtered_distance_topk(
        backend: &crate::storage::backend::TableStorageBackend,
        where_clause: &SqlExpr,
        storage_path: &Path,
        column: &str,
        computer: &crate::query::vector_ops::DistanceComputer,
        col_refs: Option<&[&str]>,
        limit: usize,
    ) -> io::Result<Option<RecordBatch>> {
        use crate::query::vector_ops::{
            topk_heap_direct_parallel, topk_heap_direct_parallel_fixed,
        };
        use arrow::array::{BinaryArray, FixedSizeListArray, UInt32Array};

        // The candidate scanners answer in physical positions (they add each row
        // group's physical offset), and `read_columns_by_indices_to_arrow` groups
        // its indices the same way, so the two agree. Overlays still change
        // visibility after the scan, which this lane cannot merge.
        let overlay = backend.overlay_state();
        if overlay.pending_cells
            || overlay.appended_rows
            || overlay.unflushed_rows
            || overlay.base_in_memory
        {
            return Ok(None);
        }
        let total = backend.active_row_count();
        if total == 0 {
            return Ok(None);
        }
        let Some(mut indices) = Self::distance_lane_candidates(backend, where_clause)? else {
            return Ok(None);
        };
        // A persisted deletion vector leaves the candidate set physical, but the
        // count that gates this lane and sizes its heap must be the *active* one:
        // `total` above is active, and the shared tail counts its LIMIT the same
        // way. The reader still wants physical indices, so only the gating copy
        // is converted.
        let mut active_count = indices.len();
        if !overlay.is_clean_view() {
            let mut active = indices.clone();
            backend.storage.activate_positions(&mut active)?;
            active_count = active.len();
        }
        // Reading by index has a per-row cost, so a predicate that keeps most of
        // the table is cheaper to serve by the full scan.
        if indices.is_empty() || (active_count as u64).saturating_mul(4) > total {
            return Ok(None);
        }

        let candidates = backend.read_columns_by_indices_to_arrow(&indices, col_refs)?;
        let filtered =
            Self::apply_filter_with_storage(&candidates, where_clause, storage_path)?;
        if filtered.num_rows() == 0 {
            return Ok(None);
        }
        let Some(values) = filtered.column_by_name(column) else {
            return Ok(None);
        };
        let effective_k = limit.min(filtered.num_rows());
        let topk = if let Some(fixed) = values.as_any().downcast_ref::<FixedSizeListArray>() {
            if fixed.null_count() > 0 || fixed.value_length() as usize != computer.query.len() {
                return Ok(None);
            }
            topk_heap_direct_parallel_fixed(fixed, computer, effective_k)
        } else if let Some(binary) = values.as_any().downcast_ref::<BinaryArray>() {
            if binary.null_count() > 0 {
                return Ok(None);
            }
            topk_heap_direct_parallel(binary, computer, effective_k)
        } else {
            return Ok(None);
        };
        if topk.is_empty() {
            return Ok(None);
        }

        crate::query::executor::record_path("filtered_distance_topk");
        let take = UInt32Array::from(topk.iter().map(|(index, _)| *index as u32).collect::<Vec<_>>());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(filtered.num_columns());
        for array in filtered.columns() {
            columns.push(
                arrow::compute::take(array.as_ref(), &take, None)
                    .map_err(|error| err_data(error.to_string()))?,
            );
        }
        RecordBatch::try_new(filtered.schema(), columns)
            .map(Some)
            .map_err(|error| err_data(error.to_string()))
    }
}
