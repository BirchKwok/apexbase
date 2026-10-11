// ORDER BY + LIMIT top-k fast paths: numeric filter, NOT NULL, and generic index comparison.

impl ApexExecutor {
    /// Numeric filter conjunction plus a numeric ORDER BY tuple. Cached decoded
    /// columns drive the row scan; only the winning rows are materialized.
    fn try_fast_numeric_filter_order_topk(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        if !stmt.group_by.is_empty()
            || !stmt.joins.is_empty()
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || stmt.order_by.is_empty()
            || stmt.limit.is_none()
            || stmt.order_by.iter().any(|order| order.expr.is_some())
            || stmt
                .columns
                .iter()
                .any(|column| matches!(column, SelectColumn::WindowFunction { .. }))
        {
            return Ok(None);
        }
        let Some(where_clause) = stmt.where_clause.as_ref() else {
            return Ok(None);
        };
        let Some(predicates) = Self::extract_numeric_conjunction(where_clause) else {
            return Ok(None);
        };
        if !backend.is_mmap_only() || backend.has_pending_deltas() || backend.has_delta() {
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
        let order_storage = stmt
            .order_by
            .iter()
            .map(|order| (clean(&order.column).to_string(), order.descending))
            .collect::<Vec<_>>();
        let order_refs = order_storage
            .iter()
            .map(|(column, descending)| (column.as_str(), *descending))
            .collect::<Vec<_>>();
        let predicate_refs = predicates
            .iter()
            .map(|(column, low, high)| (clean(column), *low, *high))
            .collect::<Vec<_>>();
        let needed = stmt.limit.unwrap() + stmt.offset.unwrap_or(0);
        if needed == 0 {
            return Ok(None);
        }
        let Some(indices) = backend.scan_filtered_numeric_top_k_cached(
            &order_refs,
            &predicate_refs,
            needed,
        )? else {
            return Ok(None);
        };
        let columns = Self::get_col_refs(stmt);
        let refs: Option<Vec<&str>> = columns
            .as_ref()
            .map(|columns| columns.iter().map(String::as_str).collect());
        let mut batch =
            backend.read_columns_by_indices_to_arrow(&indices, refs.as_deref())?;
        batch = Self::apply_order_by_topk(&batch, &stmt.order_by, Some(needed))?;
        batch = Self::apply_limit_offset(&batch, stmt.limit, stmt.offset)?;
        let projected =
            Self::apply_projection_with_storage(&batch, &stmt.columns, Some(backend.path()))?;
        Ok(Some(if projected.num_rows() == 0 {
            ApexResult::Empty(projected.schema())
        } else {
            ApexResult::Data(projected)
        }))
    }

    /// `WHERE sort_key IS NOT NULL ORDER BY sort_key ... LIMIT k` can scan
    /// only the sort key, then materialize the winning rows. Requesting k+1
    /// proves that the boundary is not tied; tied boundaries safely fall back
    /// so secondary ORDER BY semantics remain exact.
    fn try_fast_not_null_order_topk(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
    ) -> io::Result<Option<ApexResult>> {
        if !stmt.group_by.is_empty()
            || !stmt.joins.is_empty()
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || stmt.order_by.is_empty()
            || stmt.limit.is_none()
        {
            return Ok(None);
        }
        let Some(SqlExpr::IsNull {
            column,
            negated: true,
        }) = stmt.where_clause.as_ref()
        else {
            return Ok(None);
        };
        if !backend.is_mmap_only() || backend.has_pending_deltas() || backend.has_delta() {
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
        let sort_column = clean(&stmt.order_by[0].column);
        if clean(column) != sort_column {
            return Ok(None);
        }
        let needed = stmt.limit.unwrap() + stmt.offset.unwrap_or(0);
        if needed == 0 {
            return Ok(None);
        }
        let Some(mut candidates) = backend.scan_top_k_indices_mmap(
            sort_column,
            needed.saturating_add(1),
            stmt.order_by[0].descending,
        )? else {
            return Ok(None);
        };
        if candidates.len() > needed
            && candidates[needed - 1].1 == candidates[needed].1
        {
            return Ok(None);
        }
        candidates.truncate(needed);
        let indices: Vec<usize> = candidates.into_iter().map(|(index, _)| index).collect();
        let columns = Self::get_col_refs(stmt);
        let refs: Option<Vec<&str>> = columns
            .as_ref()
            .map(|columns| columns.iter().map(String::as_str).collect());
        let mut batch =
            backend.read_columns_by_indices_to_arrow(&indices, refs.as_deref())?;
        if !stmt.order_by.is_empty() {
            batch = Self::apply_order_by_topk(&batch, &stmt.order_by, Some(needed))?;
        }
        batch = Self::apply_limit_offset(&batch, stmt.limit, stmt.offset)?;
        let projected =
            Self::apply_projection_with_storage(&batch, &stmt.columns, Some(backend.path()))?;
        Ok(Some(if projected.num_rows() == 0 {
            ApexResult::Empty(projected.schema())
        } else {
            ApexResult::Data(projected)
        }))
    }

    /// Pre-evaluate SELECT expression aliases that are referenced by ORDER BY clauses.
    /// e.g. `SELECT array_distance(vec,[...]) AS dist … ORDER BY dist`
    /// The `dist` column doesn't exist in `batch` yet, but we can evaluate the expression
    /// and add it temporarily so the sort has something to work with.
    /// The extra columns are stripped by the subsequent projection step.
    fn augment_batch_for_order_by(
        batch: &RecordBatch,
        select_cols: &[crate::query::SelectColumn],
        order_by: &[crate::query::OrderByClause],
    ) -> io::Result<RecordBatch> {
        use crate::query::SelectColumn;
        use arrow::datatypes::Field;

        // Build alias→expr map from SELECT columns
        let mut alias_map: std::collections::HashMap<String, &crate::query::SqlExpr> =
            std::collections::HashMap::new();
        for sc in select_cols {
            if let SelectColumn::Expression {
                expr,
                alias: Some(alias),
            } = sc
            {
                alias_map.insert(alias.to_lowercase(), expr);
            }
        }

        if alias_map.is_empty() {
            return Ok(batch.clone());
        }

        // Find ORDER BY columns that are aliases not yet in the batch
        let mut extra: Vec<(String, arrow::array::ArrayRef)> = Vec::new();
        for ob in order_by {
            if ob.expr.is_some() {
                continue; // already handled by apply_order_by_topk expression path
            }
            let cn = ob.column.trim_matches('"');
            let cn = cn.rfind('.').map_or(cn, |p| &cn[p + 1..]);
            if batch.column_by_name(cn).is_none() {
                // Try to resolve as alias
                if let Some(expr) = alias_map.get(&cn.to_lowercase()) {
                    let arr = Self::evaluate_expr_to_array(batch, expr)?;
                    extra.push((cn.to_string(), arr));
                }
            }
        }

        if extra.is_empty() {
            return Ok(batch.clone());
        }

        let mut fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| (**f).clone())
            .collect();
        let mut cols = batch.columns().to_vec();
        for (name, arr) in extra {
            fields.push(Field::new(&name, arr.data_type().clone(), true));
            cols.push(arr);
        }
        RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), cols)
            .map_err(|e| err_data(e.to_string()))
    }

    /// Generic top-k computation using partial sort (fallback for complex cases)
    fn compute_topk_indices_generic(
        sort_batch: &RecordBatch,
        order_by: &[crate::query::OrderByClause],
        k: usize,
        offset: Option<usize>,
    ) -> Vec<usize> {
        let num_rows = sort_batch.num_rows();

        // FAST PATH: 2-column (StringArray, Float64Array) — typed comparison, no closure overhead
        if order_by.len() == 2 {
            let col0_name = {
                let c = order_by[0].column.trim_matches('"');
                if let Some(p) = c.rfind('.') {
                    &c[p + 1..]
                } else {
                    c
                }
            };
            let col1_name = {
                let c = order_by[1].column.trim_matches('"');
                if let Some(p) = c.rfind('.') {
                    &c[p + 1..]
                } else {
                    c
                }
            };
            let arr0 = sort_batch.column_by_name(col0_name);
            let arr1 = sort_batch.column_by_name(col1_name);
            if let (Some(a0), Some(a1)) = (arr0, arr1) {
                use arrow::array::Float64Array as FA;
                use arrow::array::StringArray as SA;
                if let (Some(str_arr), Some(flt_arr)) = (
                    a0.as_any().downcast_ref::<SA>(),
                    a1.as_any().downcast_ref::<FA>(),
                ) {
                    use ahash::AHashMap;
                    // Build dict for string col → u16 id (1-based, 0=null)
                    let mut dict: AHashMap<&str, u16> = AHashMap::with_capacity(64);
                    let mut dict_vals: Vec<&str> = Vec::with_capacity(64);
                    let str_ids: Vec<u16> = (0..num_rows)
                        .map(|i| {
                            if str_arr.is_null(i) {
                                return 0u16;
                            }
                            let s = str_arr.value(i);
                            let next = dict_vals.len() as u16 + 1;
                            *dict.entry(s).or_insert_with(|| {
                                dict_vals.push(s);
                                next
                            })
                        })
                        .collect();
                    // Sort dict entries to get alphabetical rank mapping
                    let mut sorted: Vec<(u16, &str)> = dict_vals
                        .iter()
                        .enumerate()
                        .map(|(i, &s)| (i as u16 + 1, s))
                        .collect();
                    sorted.sort_unstable_by_key(|&(_, s)| s);
                    let mut rank_of = vec![0u16; dict_vals.len() + 1];
                    for (rank, &(id, _)) in sorted.iter().enumerate() {
                        rank_of[id as usize] = rank as u16;
                    }
                    let asc0 = !order_by[0].descending;
                    let desc1 = order_by[1].descending;
                    // Pack (str_rank, score_sortable_bits) into (u64, u64) composite key
                    let mut packed: Vec<(u64, u64, usize)> = (0..num_rows)
                        .map(|i| {
                            let sid = str_ids[i] as usize;
                            let sr = if sid == 0 {
                                u16::MAX as u64
                            } else {
                                rank_of[sid] as u64
                            };
                            let sk0 = if asc0 { sr } else { u16::MAX as u64 - sr };
                            let f = if flt_arr.is_null(i) {
                                f64::NEG_INFINITY
                            } else {
                                flt_arr.value(i)
                            };
                            let fb = f.to_bits();
                            let fs = if fb >> 63 == 0 {
                                fb ^ (1u64 << 63)
                            } else {
                                !fb
                            };
                            let sk1 = if desc1 { !fs } else { fs };
                            (sk0, sk1, i)
                        })
                        .collect();
                    if k < num_rows {
                        packed.select_nth_unstable_by_key(k - 1, |&(a, b, _)| (a, b));
                        packed.truncate(k);
                    }
                    packed.sort_unstable_by_key(|&(a, b, _)| (a, b));
                    let off = offset.unwrap_or(0);
                    return packed
                        .into_iter()
                        .skip(off)
                        .map(|(_, _, idx)| idx)
                        .collect();
                }
            }
        }

        let sort_cols: Vec<(ArrayRef, bool, bool)> = order_by
            .iter()
            .filter_map(|clause| {
                let col_name = clause.column.trim_matches('"');
                let actual_col = if let Some(dot_pos) = col_name.rfind('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name
                };
                sort_batch.column_by_name(actual_col).map(|col| {
                    (
                        col.clone(),
                        clause.descending,
                        clause.nulls_first.unwrap_or(clause.descending),
                    )
                })
            })
            .collect();

        let compare_rows = |a: usize, b: usize| -> std::cmp::Ordering {
            use std::cmp::Ordering;
            for (col, descending, nulls_first) in &sort_cols {
                let a_null = col.is_null(a);
                let b_null = col.is_null(b);
                let ord = if a_null || b_null {
                    // NULL placement is independent of the value direction.
                    if a_null && b_null {
                        Ordering::Equal
                    } else if a_null == *nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                } else {
                    let ord = Self::compare_array_values(col, a, b);
                    if *descending {
                        ord.reverse()
                    } else {
                        ord
                    }
                };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        };

        let mut indices: Vec<usize> = (0..num_rows).collect();

        if k < num_rows {
            indices.select_nth_unstable_by(k - 1, |&a, &b| compare_rows(a, b));
            indices.truncate(k);
        }
        indices.sort_by(|&a, &b| compare_rows(a, b));

        if let Some(off) = offset {
            indices.into_iter().skip(off).collect()
        } else {
            indices
        }
    }
}
