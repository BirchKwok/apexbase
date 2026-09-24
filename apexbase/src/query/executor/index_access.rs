// Index-accelerated SELECT: secondary index lookup, predicate extraction, index-only scan.

impl ApexExecutor {
    /// Try to use a secondary index to accelerate a SELECT query.
    /// Returns Some(result) if an index was used, None to fall through to scan paths.
    /// Only used for simple equality WHERE clauses on indexed columns (no GROUP BY/aggregation).
    fn try_index_accelerated_read(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
        where_clause: &SqlExpr,
        spec: Option<&crate::query::planner::IndexExecutionSpec>,
        _base_dir: &Path,
        storage_path: &Path,
    ) -> io::Result<Option<ApexResult>> {
        use crate::storage::index::index_manager::PredicateHint;

        // A committed DML operation may have failed while persisting its
        // secondary index. Until REINDEX repairs it, scans are authoritative.
        if indexes_stale(storage_path) {
            return Ok(None);
        }

        // Only use index for simple queries: no GROUP BY, no aggregation, no JOIN
        if !stmt.group_by.is_empty() || !stmt.joins.is_empty() {
            return Ok(None);
        }
        let has_agg = stmt
            .columns
            .iter()
            .any(|c| matches!(c, SelectColumn::Aggregate { .. }));
        if has_agg {
            return Ok(None);
        }

        // Predicates: single or AND-combined.  A plan-carried spec
        // (architecture review R5.6) supplies the planner's materialization;
        // the legacy path re-derives it from the WHERE clause.
        let predicates: Vec<(String, PredicateHint)> = match spec {
            Some(spec) => spec.predicates.clone(),
            None => {
                let mut predicates = Vec::new();
                QueryPlanner::extract_index_predicates(where_clause, &mut predicates);
                predicates
            }
        };

        if predicates.is_empty() {
            return Ok(None);
        }

        // Check which predicates have indexes available
        let (bd, tname) = base_dir_and_table(storage_path);
        let idx_mgr_arc = get_index_manager(&bd, &tname);
        let mut idx_mgr = idx_mgr_arc.lock();

        // OR is only legal when every branch can be satisfied by indexes. The
        // complete predicate is still reapplied after union and deduplication.
        let disjunctive_row_ids = if spec
            .map(|spec| spec.disjunction.is_some())
            .unwrap_or_else(|| QueryPlanner::contains_disjunction(where_clause))
        {
            Self::lookup_index_expression(&mut idx_mgr, where_clause)?
        } else {
            None
        };

        // Filter to predicates that have usable single-column indexes.  A
        // composite index is handled separately below because its key must be
        // built from all indexed equality columns.
        let equality_values: std::collections::HashMap<String, Value> = predicates
            .iter()
            .filter_map(|(column, hint)| match hint {
                PredicateHint::Eq(value) => Some((column.clone(), value.clone())),
                _ => None,
            })
            .collect();
        let range_values = predicates.iter().find_map(|(column, hint)| match hint {
            PredicateHint::Range { low, high } => Some((column.clone(), low.clone(), high.clone())),
            _ => None,
        });
        let indexed_preds: Vec<(String, PredicateHint)> = predicates
            .into_iter()
            .filter(|(col, _)| idx_mgr.has_single_column_index_on(col))
            .collect();
        let composite_columns = idx_mgr
            .list_indexes()
            .into_iter()
            .filter(|meta| meta.is_composite())
            .map(|meta| {
                meta.effective_columns()
                    .iter()
                    .map(|column| column.to_string())
                    .collect::<Vec<_>>()
            })
            .filter_map(|columns| {
                let prefix_len = columns
                    .iter()
                    .take_while(|column| equality_values.contains_key(*column))
                    .count();
                (prefix_len > 0).then_some((columns, prefix_len))
            })
            .max_by_key(|(_, prefix_len)| *prefix_len);

        if indexed_preds.is_empty() && composite_columns.is_none() && disjunctive_row_ids.is_none() {
            return Ok(None);
        }

        let ordered_index_column = composite_columns
            .as_ref()
            .and_then(|(columns, prefix_len)| columns.get(*prefix_len).cloned())
            .or_else(|| {
                if composite_columns.is_some() || indexed_preds.len() != 1 {
                    return None;
                }
                indexed_preds.iter().find_map(|(column, hint)| {
                    matches!(
                        hint,
                        PredicateHint::Range { .. }
                            | PredicateHint::Gt(_)
                            | PredicateHint::Gte(_)
                            | PredicateHint::Lt(_)
                            | PredicateHint::Lte(_)
                    )
                    .then(|| column.clone())
                })
            });

        // CBO: Pre-estimate selectivity using ANALYZE stats before expensive index lookup
        let table_key = storage_path.to_string_lossy();
        if let Some(stats) = get_table_stats(&table_key) {
            let selectivity = QueryPlanner::estimate_selectivity(where_clause, &stats);
            if stats.row_count > 0
                && !QueryPlanner::should_use_index("", selectivity, stats.row_count)
            {
                return Ok(None); // CBO says full scan is cheaper, skip index lookup
            }
        }

        // Look up a complete composite key first, then intersect any
        // independent single-column indexes.
        let mut row_ids: Option<Vec<u64>> = disjunctive_row_ids;
        if let Some((columns, prefix_len)) = composite_columns.as_ref() {
            let lookup = if *prefix_len == columns.len() {
                idx_mgr.lookup_composite(columns, &equality_values)?
            } else {
                idx_mgr.lookup_composite_prefix(
                    columns,
                    &equality_values,
                    range_values.as_ref().map(|(column, low, high)| (column, low, high)),
                )?
            };
            if let Some(result) = lookup {
                row_ids = Some(match row_ids {
                    Some(existing) => Self::intersect_row_ids(existing, result.row_ids),
                    None => result.row_ids,
                });
            }
        }
        for (col_name, hint) in &indexed_preds {
            let lookup_result = idx_mgr.lookup(col_name, hint)?;
            match lookup_result {
                Some(r) => {
                    row_ids = Some(match row_ids {
                        None => r.row_ids,
                        Some(existing) => Self::intersect_row_ids(existing, r.row_ids),
                    });
                }
                None => {
                    // Index couldn't satisfy this predicate, skip it
                    // (still use other index results if available)
                }
            }
        }

        let row_ids = match row_ids {
            Some(ids) => ids,
            None => return Ok(None),
        };

        if row_ids.is_empty() {
            let empty = backend.read_columns_to_arrow(None, 0, Some(0))?;
            return Ok(Some(ApexResult::Empty(empty.schema())));
        }

        // Read matching rows by their _ids
        // CBO: use ANALYZE stats to decide index vs full scan cost
        let total_rows = backend.active_row_count();
        let selectivity = if total_rows > 0 {
            row_ids.len() as f64 / total_rows as f64
        } else {
            1.0
        };
        if !QueryPlanner::should_use_index("", selectivity, total_rows as u64) {
            return Ok(None); // Cost model says full scan is cheaper
        }

        // Covering index (index-only scan): if all SELECT columns are covered by
        // the index (_id + indexed columns), build result directly without reading
        // the base table — avoids expensive per-row table lookups.
        let indexed_columns: std::collections::HashSet<String> = indexed_preds
            .iter()
            .map(|(column, _)| column.clone())
            .chain(
                composite_columns
                    .iter()
                    .flat_map(|(columns, _)| columns.iter().cloned()),
            )
            .collect();
        let full_predicate_covered =
            QueryPlanner::is_fully_indexable_predicate(where_clause, &indexed_columns);
        let composite_is_full_equality = composite_columns
            .as_ref()
            .map(|(columns, prefix_len)| *prefix_len == columns.len())
            .unwrap_or(false);
        // A plan-carried spec (architecture review R5.6) derives the covering
        // and residual-skip decisions from the planning-time index state.
        // Re-verify that state before trusting the booleans: if index DDL
        // raced the plan, fall back to the decisions re-derived on the live
        // index state (identical to the no-spec behavior).
        let spec_fresh = match spec {
            Some(spec) => {
                let live_composite_columns = composite_columns
                    .as_ref()
                    .map(|(columns, _)| columns.iter().cloned().collect::<Vec<_>>());
                spec.predicates.iter().all(|(column, _)| {
                    idx_mgr.has_single_column_index_on(column)
                        || live_composite_columns
                            .as_ref()
                            .map(|columns| columns.iter().any(|c| c == column))
                            .unwrap_or(false)
                }) && match (&spec.composite_columns, live_composite_columns.as_ref()) {
                    (Some(planned), Some(live)) => planned == live,
                    (Some(_), None) => false,
                    (None, _) => true,
                }
            }
            None => true,
        };
        let (try_covering_scan, skip_residual_filter) = match spec {
            Some(spec) if spec_fresh => (spec.try_covering_scan, spec.skip_residual_filter),
            _ => (
                full_predicate_covered
                    && (composite_columns.is_none() || composite_is_full_equality),
                full_predicate_covered && composite_is_full_equality,
            ),
        };
        if try_covering_scan {
            let mut covering_preds = indexed_preds.clone();
            for (column, value) in &equality_values {
                if !covering_preds
                    .iter()
                    .any(|(candidate, _)| candidate == column)
                {
                    covering_preds.push((column.clone(), PredicateHint::Eq(value.clone())));
                }
            }
            if let Some(covered) = Self::try_index_only_scan(stmt, &covering_preds, &row_ids)? {
                return Ok(Some(covered));
            }
        }

        // Batch materialization amortizes mmap/footer work. Tiny probes retain
        // the lower-latency point path; the crossover is benchmarked.
        const BATCH_ROW_ID_THRESHOLD: usize = 8;
        let combined = if row_ids.len() >= BATCH_ROW_ID_THRESHOLD {
            backend.read_rows_by_ids_to_arrow(&row_ids)?
        } else {
            let mut batches: Vec<RecordBatch> = Vec::with_capacity(row_ids.len());
            for &rid in &row_ids {
                if let Some(batch) = backend.read_row_by_id_to_arrow(rid)? {
                    batches.push(batch);
                }
            }
            if batches.len() != row_ids.len() {
                backend.read_rows_by_ids_to_arrow(&row_ids)?
            } else {
                let schema = batches[0].schema();
                arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| err_data(e.to_string()))?
            }
        };
        if combined.num_rows() == 0 {
            let empty = backend.read_columns_to_arrow(None, 0, Some(0))?;
            return Ok(Some(ApexResult::Empty(empty.schema())));
        }

        // Indexes produce candidate rows.  The residual (full WHERE) filter
        // keeps non-indexed predicates correct; it is skipped only when the
        // covering decision proves full equality coverage.
        let residual = spec.map(|spec| &spec.residual).unwrap_or(where_clause);
        let filtered = if skip_residual_filter {
            combined
        } else {
            Self::apply_filter_with_storage(&combined, residual, storage_path)?
        };
        if filtered.num_rows() == 0 {
            return Ok(Some(ApexResult::Empty(filtered.schema())));
        }

        // Apply ORDER BY if present after residual filtering.
        let index_order_satisfies = stmt.order_by.len() == 1
            && !stmt.order_by[0].descending
            && ordered_index_column.as_deref() == Some(stmt.order_by[0].column.as_str());
        let sorted = if !stmt.order_by.is_empty() && !index_order_satisfies {
            Self::apply_order_by(&filtered, &stmt.order_by)?
        } else {
            filtered
        };

        // Apply OFFSET + LIMIT
        let result = {
            let offset = stmt.offset.unwrap_or(0);
            let total = sorted.num_rows();
            if offset >= total {
                sorted.slice(0, 0)
            } else if let Some(limit) = stmt.limit {
                let end = (offset + limit).min(total);
                sorted.slice(offset, end - offset)
            } else if offset > 0 {
                sorted.slice(offset, total - offset)
            } else {
                sorted
            }
        };

        // Apply column projection if not pure SELECT *
        if !stmt.is_pure_star() {
            let projected = Self::apply_projection(&result, &stmt.columns)?;
            if projected.num_rows() == 0 {
                return Ok(Some(ApexResult::Empty(projected.schema())));
            }
            return Ok(Some(ApexResult::Data(projected)));
        }

        if result.num_rows() == 0 {
            return Ok(Some(ApexResult::Empty(result.schema())));
        }
        Ok(Some(ApexResult::Data(result)))
    }

    fn planner_context(
        backend: &TableStorageBackend,
        where_clause: Option<&SqlExpr>,
    ) -> crate::query::planner::PlannerContext {
        let zone_map = match where_clause {
            Some(SqlExpr::Between {
                column,
                low,
                high,
                negated: false,
            }) => QueryPlanner::expr_to_value(low)
                .and_then(|value| value.as_f64())
                .zip(QueryPlanner::expr_to_value(high).and_then(|value| value.as_f64()))
                .and_then(|(low, high)| {
                    backend
                        .estimate_zone_map_range(column, low, high)
                        .ok()
                        .flatten()
                }),
            _ => None,
        };
        crate::query::planner::PlannerContext {
            mmap_only: backend.is_mmap_only(),
            zone_map,
        }
    }

    fn intersect_row_ids(mut left: Vec<u64>, mut right: Vec<u64>) -> Vec<u64> {
        left.sort_unstable();
        right.sort_unstable();
        let mut out = Vec::with_capacity(left.len().min(right.len()));
        let (mut i, mut j) = (0, 0);
        while i < left.len() && j < right.len() {
            match left[i].cmp(&right[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    if out.last() != Some(&left[i]) { out.push(left[i]); }
                    i += 1;
                    j += 1;
                }
            }
        }
        out
    }

    fn union_row_ids(mut left: Vec<u64>, right: Vec<u64>) -> Vec<u64> {
        left.extend(right);
        left.sort_unstable();
        left.dedup();
        left
    }

    fn lookup_index_expression(
        indexes: &mut crate::storage::index::IndexManager,
        expr: &SqlExpr,
    ) -> io::Result<Option<Vec<u64>>> {
        match expr {
            SqlExpr::BinaryOp { left, op: BinaryOperator::Or, right } => {
                let left = Self::lookup_index_expression(indexes, left)?;
                let right = Self::lookup_index_expression(indexes, right)?;
                Ok(left.zip(right).map(|(left, right)| Self::union_row_ids(left, right)))
            }
            SqlExpr::BinaryOp { left, op: BinaryOperator::And, right } => {
                let left = Self::lookup_index_expression(indexes, left)?;
                let right = Self::lookup_index_expression(indexes, right)?;
                Ok(match (left, right) {
                    (Some(left), Some(right)) => Some(Self::intersect_row_ids(left, right)),
                    (Some(ids), None) | (None, Some(ids)) => Some(ids),
                    (None, None) => None,
                })
            }
            SqlExpr::Paren(inner) => Self::lookup_index_expression(indexes, inner),
            _ => {
                let mut predicates = Vec::with_capacity(1);
                QueryPlanner::extract_index_predicates(expr, &mut predicates);
                if predicates.len() != 1 {
                    return Ok(None);
                }
                let (column, hint) = predicates.pop().unwrap();
                if !indexes.has_single_column_index_on(&column) {
                    return Ok(None);
                }
                Ok(indexes.lookup(&column, &hint)?.map(|result| result.row_ids))
            }
        }
    }

    /// Covering index (index-only scan): build result directly from index data
    /// when all SELECT columns are covered by {_id, indexed_columns}.
    /// For equality predicates, the column value is known from the predicate itself.
    /// Returns None if the query needs columns not available from the index.
    fn try_index_only_scan(
        stmt: &SelectStatement,
        indexed_preds: &[(String, crate::storage::index::index_manager::PredicateHint)],
        row_ids: &[u64],
    ) -> io::Result<Option<ApexResult>> {
        use std::collections::HashMap;
        // Only for non-* queries (SELECT * needs all columns)
        if stmt.is_select_star() {
            return Ok(None);
        }
        // Only for simple equality predicates (we know the exact value)
        // Collect indexed column names and their known values
        use crate::storage::index::index_manager::PredicateHint;
        let mut known_values: HashMap<String, Value> = HashMap::new();
        for (col, hint) in indexed_preds {
            match hint {
                PredicateHint::Eq(val) => {
                    known_values.insert(col.clone(), val.clone());
                }
                PredicateHint::In(_) if row_ids.len() <= 1 => {
                    return Ok(None);
                }
                _ => {
                    return Ok(None);
                } // Range predicates: values vary per row
            }
        }
        if known_values.is_empty() {
            return Ok(None);
        }

        // Check if all SELECT columns are covered by {_id} ∪ {indexed columns}
        let mut need_id = false;
        let mut need_cols: Vec<String> = Vec::new();
        for col in &stmt.columns {
            match col {
                SelectColumn::Column(name) => {
                    let clean = name.trim_matches('"');
                    if clean == "_id" {
                        need_id = true;
                    } else if known_values.contains_key(clean) {
                        need_cols.push(clean.to_string());
                    } else {
                        return Ok(None); // Need a column not in index → can't cover
                    }
                }
                SelectColumn::ColumnAlias { column, .. } => {
                    let clean = column.trim_matches('"');
                    if clean == "_id" {
                        need_id = true;
                    } else if known_values.contains_key(clean) {
                        need_cols.push(clean.to_string());
                    } else {
                        return Ok(None);
                    }
                }
                SelectColumn::All => {
                    return Ok(None);
                }
                _ => {
                    return Ok(None);
                } // Aggregate, expression, etc.
            }
        }

        // Build Arrow RecordBatch directly from index data
        let n = row_ids.len();
        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        if need_id {
            fields.push(Field::new("_id", arrow::datatypes::DataType::Int64, false));
            let id_arr: Int64Array = row_ids.iter().map(|&id| id as i64).collect();
            arrays.push(Arc::new(id_arr) as ArrayRef);
        }

        for col_name in &need_cols {
            let val = &known_values[col_name];
            match val {
                Value::Int64(v) => {
                    fields.push(Field::new(
                        col_name,
                        arrow::datatypes::DataType::Int64,
                        true,
                    ));
                    let arr = Int64Array::from(vec![*v; n]);
                    arrays.push(Arc::new(arr) as ArrayRef);
                }
                Value::Float64(f) => {
                    fields.push(Field::new(
                        col_name,
                        arrow::datatypes::DataType::Float64,
                        true,
                    ));
                    let arr = Float64Array::from(vec![*f; n]);
                    arrays.push(Arc::new(arr) as ArrayRef);
                }
                Value::String(s) => {
                    fields.push(Field::new(col_name, arrow::datatypes::DataType::Utf8, true));
                    let arr = StringArray::from(vec![s.as_str(); n]);
                    arrays.push(Arc::new(arr) as ArrayRef);
                }
                Value::Bool(b) => {
                    fields.push(Field::new(
                        col_name,
                        arrow::datatypes::DataType::Boolean,
                        true,
                    ));
                    let arr = BooleanArray::from(vec![*b; n]);
                    arrays.push(Arc::new(arr) as ArrayRef);
                }
                _ => {
                    return Ok(None);
                } // Unsupported value type for index-only scan
            }
        }

        if fields.is_empty() {
            return Ok(None);
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))?;

        // Apply ORDER BY if present
        let sorted = if !stmt.order_by.is_empty() {
            Self::apply_order_by(&batch, &stmt.order_by)?
        } else {
            batch
        };

        // Apply OFFSET + LIMIT
        let result = Self::apply_limit_offset(&sorted, stmt.limit, stmt.offset)?;

        if result.num_rows() == 0 {
            return Ok(Some(ApexResult::Empty(result.schema())));
        }
        Ok(Some(ApexResult::Data(result)))
    }

    fn table_has_index_catalog(base_dir: Option<&Path>, storage_path: &Path) -> bool {
        if indexes_stale(storage_path) {
            return false;
        }
        let stem = match storage_path.file_stem() {
            Some(stem) => stem.to_string_lossy(),
            None => return false,
        };
        let catalog_name = format!("{}.idxcat", stem);
        if let Some(base_dir) = base_dir {
            if base_dir.join("indexes").join(&catalog_name).exists() {
                return true;
            }
        }
        for ancestor in storage_path.ancestors().skip(1).take(8) {
            if ancestor.join("indexes").join(&catalog_name).exists() {
                return true;
            }
        }
        false
    }
}
