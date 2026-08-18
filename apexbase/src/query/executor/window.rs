// Window functions, UNION execution

impl ApexExecutor {
    /// Execute UNION / INTERSECT / EXCEPT statement
    fn execute_union(union: UnionStatement, base_dir: &Path, default_table_path: &Path) -> io::Result<ApexResult> {
        use crate::query::SetOpType;

        // ── FAST PATH: DISTINCT INTERSECT/EXCEPT over `SELECT dict_col WHERE
        // num_col BETWEEN lo AND hi` (both sides on the same table). The sides
        // are answered directly as small distinct-value sets at the storage
        // layer, avoiding the full row-set materialization.
        if let Some(batch) =
            Self::try_fast_setop_distinct(&union, base_dir, default_table_path)?
        {
            let mut result = batch;
            if !union.order_by.is_empty() {
                result = Self::apply_order_by(&result, &union.order_by)?;
            }
            if union.limit.is_some() || union.offset.is_some() {
                result = Self::apply_limit_offset(&result, union.limit, union.offset)?;
            }
            return Ok(ApexResult::Data(result));
        }

        // ── FAST PATH: `SELECT dict_col WHERE num_col = v1 UNION [ALL] SELECT
        // dict_col WHERE num_col = v2 ORDER BY dict_col [LIMIT k]`. Both sides'
        // per-value counts are computed in a single storage pass and the result
        // (top-k rows for ALL, distinct values for DISTINCT) is built directly,
        // skipping the 2×side-row materialization.
        if let Some(batch) =
            Self::try_fast_union_topk(&union, base_dir, default_table_path)?
        {
            return Ok(ApexResult::Data(batch));
        }

        // Keep dictionary-encoded string columns in the side batches: the set
        // op hashes distinct values once instead of every row, and only the
        // (small) result is decoded below.
        let left_batch = crate::query::executor::with_keep_dict_projection(|| {
            Self::execute_parsed_multi(*union.left, base_dir, default_table_path)?
                .to_record_batch()
        })?;
        let right_batch = crate::query::executor::with_keep_dict_projection(|| {
            Self::execute_parsed_multi(*union.right, base_dir, default_table_path)?
                .to_record_batch()
        })?;

        if left_batch.num_columns() != right_batch.num_columns() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Set operation requires same number of columns",
            ));
        }

        let mut result = match union.set_op {
            SetOpType::Union => {
                let combined = Self::concat_batches(&left_batch, &right_batch)?;
                if union.all { combined } else { Self::deduplicate_batch(&combined)? }
            }
            SetOpType::Intersect => {
                // Keep rows from left that also appear in right (deduplicated)
                Self::intersect_batches(&left_batch, &right_batch, union.all)?
            }
            SetOpType::Except => {
                // Keep rows from left that do NOT appear in right (deduplicated)
                Self::except_batches(&left_batch, &right_batch, union.all)?
            }
        };

        // Decode any dictionary columns before ORDER BY (key ids are not in
        // value order) and before returning to the client.
        result = Self::decode_dict_columns(&result);

        if !union.order_by.is_empty() {
            // With a LIMIT smaller than the result, use a limited top-k sort
            // instead of sorting every row (O(n log n) → O(n log k)).
            let k = union.limit.map(|l| l + union.offset.unwrap_or(0));
            if let Some(k) = k.filter(|&k| k < result.num_rows()) {
                result = Self::apply_order_by_topk(&result, &union.order_by, Some(k))?;
            } else {
                result = Self::apply_order_by(&result, &union.order_by)?;
            }
        }
        if union.limit.is_some() || union.offset.is_some() {
            result = Self::apply_limit_offset(&result, union.limit, union.offset)?;
        }

        Ok(ApexResult::Data(result))
    }

    /// DISTINCT INTERSECT/EXCEPT fast path: both sides are
    /// `SELECT <dict_col> FROM <table> WHERE <num_col> BETWEEN lo AND hi`.
    /// Answers each side as a small distinct-value set via the storage layer and
    /// computes the set operation directly, returning a plain StringArray batch.
    fn try_fast_setop_distinct(
        union: &UnionStatement,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<Option<RecordBatch>> {
        use crate::query::sql_parser::{FromItem, SelectColumn, SqlExpr, SqlStatement};
        use crate::query::SetOpType;
        if union.all || union.set_op == SetOpType::Union {
            return Ok(None);
        }

        let extract = |stmt: &SqlStatement| -> Option<(String, String, String, f64, f64)> {
            let SqlStatement::Select(sel) = stmt else {
                return None;
            };
            if sel.distinct
                || sel.distinct_on.is_some()
                || !sel.group_by.is_empty()
                || !sel.order_by.is_empty()
                || sel.limit.is_some()
                || sel.offset.is_some()
                || !sel.joins.is_empty()
                || sel.having.is_some()
                || sel.columns.len() != 1
            {
                return None;
            }
            let FromItem::Table { table, alias: None } = sel.from.as_ref()? else {
                return None;
            };
            let SelectColumn::Column(dict_col) = &sel.columns[0] else {
                return None;
            };
            let SqlExpr::Between {
                column: num_col,
                low,
                high,
                negated: false,
            } = sel.where_clause.as_ref()?
            else {
                return None;
            };
            let lo = Self::extract_numeric_value(low).ok()?;
            let hi = Self::extract_numeric_value(high).ok()?;
            Some((
                table.clone(),
                dict_col.trim_matches('"').to_string(),
                num_col.trim_matches('"').to_string(),
                lo,
                hi,
            ))
        };

        let (table, dict_col, num_col, lo1, hi1) = match extract(union.left.as_ref()) {
            Some(v) => v,
            None => return Ok(None),
        };
        let (table2, dict_col2, num_col2, lo2, hi2) = match extract(union.right.as_ref()) {
            Some(v) => v,
            None => return Ok(None),
        };
        if table != table2 || dict_col != dict_col2 || num_col != num_col2 {
            return Ok(None);
        }

        let table_path = Self::resolve_table_path(&table, base_dir, default_table_path);
        let backend = crate::query::executor::get_cached_backend(&table_path)?;

        let left_vals = match backend.scan_distinct_dict_values_in_range(&num_col, lo1, hi1, &dict_col)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let right_vals = match backend.scan_distinct_dict_values_in_range(&num_col, lo2, hi2, &dict_col)? {
            Some(v) => v,
            None => return Ok(None),
        };

        let right_set: std::collections::HashSet<&str> =
            right_vals.iter().map(|s| s.as_str()).collect();
        let mut result_vals: Vec<String> = match union.set_op {
            SetOpType::Intersect => left_vals
                .into_iter()
                .filter(|v| right_set.contains(v.as_str()))
                .collect(),
            SetOpType::Except => left_vals
                .into_iter()
                .filter(|v| !right_set.contains(v.as_str()))
                .collect(),
            SetOpType::Union => return Ok(None),
        };
        // `left_vals` is already sorted by the storage scan; a stable filter
        // preserves that order, so no extra sort is needed for ORDER BY.
        result_vals.dedup();

        let arr: ArrayRef = Arc::new(arrow::array::StringArray::from(result_vals));
        let schema = Arc::new(Schema::new(vec![Field::new(
            &dict_col,
            ArrowDataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(schema, vec![arr])
            .map(|b| Some(b))
            .map_err(|e| err_data(e.to_string()))
    }

    /// UNION fast path: both sides are
    /// `SELECT <dict_col> FROM <table> WHERE <num_col> = <v>` (equality) or
    /// `BETWEEN lo AND hi`, and the statement is `ORDER BY <dict_col>` with an
    /// optional LIMIT. The per-value counts of both sides are gathered in one
    /// storage pass; UNION ALL emits the top-k rows directly, UNION DISTINCT
    /// emits the distinct values.
    fn try_fast_union_topk(
        union: &UnionStatement,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<Option<RecordBatch>> {
        use crate::query::sql_parser::{BinaryOperator, FromItem, SelectColumn, SqlExpr, SqlStatement};
        use crate::query::SetOpType;
        if union.set_op != SetOpType::Union {
            return Ok(None);
        }
        // UNION ALL without a LIMIT would materialize the full side row sets;
        // only the bounded top-k form is answered here.
        if union.all && union.limit.is_none() {
            return Ok(None);
        }
        let limit = union.limit;
        let offset = union.offset.unwrap_or(0);
        // OFFSET without LIMIT cannot be answered by a top-k build; fall back.
        if offset > 0 && limit.is_none() {
            return Ok(None);
        }
        if union.order_by.len() != 1 {
            return Ok(None);
        }
        let ob = &union.order_by[0];
        if ob.expr.is_some() || ob.nulls_first.is_some() {
            return Ok(None);
        }

        let extract = |stmt: &SqlStatement| -> Option<(String, String, String, f64, f64)> {
            let SqlStatement::Select(sel) = stmt else {
                return None;
            };
            if sel.distinct
                || sel.distinct_on.is_some()
                || !sel.group_by.is_empty()
                || !sel.order_by.is_empty()
                || sel.limit.is_some()
                || sel.offset.is_some()
                || !sel.joins.is_empty()
                || sel.having.is_some()
                || sel.columns.len() != 1
            {
                return None;
            }
            let FromItem::Table { table, alias: None } = sel.from.as_ref()? else {
                return None;
            };
            let SelectColumn::Column(dict_col) = &sel.columns[0] else {
                return None;
            };
            let (num_col, lo, hi) = match sel.where_clause.as_ref()? {
                SqlExpr::Between {
                    column,
                    low,
                    high,
                    negated: false,
                } => (
                    column.clone(),
                    Self::extract_numeric_value(low).ok()?,
                    Self::extract_numeric_value(high).ok()?,
                ),
                SqlExpr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                } => match (left.as_ref(), right.as_ref()) {
                    (SqlExpr::Column(c), lit) => {
                        (c.clone(), Self::extract_numeric_value(lit).ok()?, Self::extract_numeric_value(lit).ok()?)
                    }
                    (lit, SqlExpr::Column(c)) => {
                        (c.clone(), Self::extract_numeric_value(lit).ok()?, Self::extract_numeric_value(lit).ok()?)
                    }
                    _ => return None,
                },
                _ => return None,
            };
            Some((
                table.clone(),
                dict_col.trim_matches('"').to_string(),
                num_col.trim_matches('"').to_string(),
                lo,
                hi,
            ))
        };

        let (table, dict_col, num_col, lo1, hi1) = match extract(union.left.as_ref()) {
            Some(v) => v,
            None => return Ok(None),
        };
        let (table2, dict_col2, num_col2, lo2, hi2) = match extract(union.right.as_ref()) {
            Some(v) => v,
            None => return Ok(None),
        };
        if table != table2 || dict_col != dict_col2 || num_col != num_col2 {
            return Ok(None);
        }
        if ob.column.trim_matches('"') != dict_col {
            return Ok(None);
        }

        let table_path = Self::resolve_table_path(&table, base_dir, default_table_path);
        let backend = crate::query::executor::get_cached_backend(&table_path)?;
        // NULL dictionary keys are dropped by the count scan; fall back when the
        // dict column can contain NULLs so the null multiplicity/set semantics
        // are preserved.
        if backend.column_has_nulls(&dict_col) {
            return Ok(None);
        }

        let counts = match backend.count_dict_values_two_ranges(
            &num_col, lo1, hi1, lo2, hi2, &dict_col,
        )? {
            Some(v) => v,
            None => return Ok(None),
        };

        let iter: Box<dyn Iterator<Item = &(String, i64)>> = if ob.descending {
            Box::new(counts.iter().rev())
        } else {
            Box::new(counts.iter())
        };
        let mut vals: Vec<String> = Vec::new();
        if union.all {
            let total_need = limit.unwrap() + offset;
            for (v, c) in iter {
                for _ in 0..(*c).max(0) as usize {
                    vals.push(v.clone());
                    if vals.len() >= total_need {
                        break;
                    }
                }
                if vals.len() >= total_need {
                    break;
                }
            }
            vals = if offset > 0 {
                vals.into_iter().skip(offset).take(limit.unwrap()).collect()
            } else {
                vals.into_iter().take(limit.unwrap()).collect()
            };
        } else {
            // UNION DISTINCT: one row per value that appears on either side.
            for (v, c) in iter {
                if *c > 0 {
                    vals.push(v.clone());
                }
            }
            if let Some(l) = limit {
                vals = if offset > 0 {
                    vals.into_iter().skip(offset).take(l).collect()
                } else {
                    vals.into_iter().take(l).collect()
                };
            }
        }

        let arr: ArrayRef = Arc::new(arrow::array::StringArray::from(vals));
        let schema = Arc::new(Schema::new(vec![Field::new(
            &dict_col,
            ArrowDataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(schema, vec![arr])
            .map(|b| Some(b))
            .map_err(|e| err_data(e.to_string()))
    }

    /// INTERSECT: rows in left that also appear in right
    fn intersect_batches(left: &RecordBatch, right: &RecordBatch, all: bool) -> io::Result<RecordBatch> {
        use rayon::prelude::*;
        // Single-column side batches (the common case) skip the per-row downcast:
        // the row fingerprint is the dictionary key id directly.
        let right_hashes: std::collections::HashSet<u64> = if right.num_columns() == 1 {
            Self::single_col_row_hashes(right).into_par_iter().collect()
        } else {
            let right_dict_hashes = Self::dict_value_hashes(right);
            (0..right.num_rows())
                .into_par_iter()
                .map(|i| Self::hash_row_dict_aware(right, i, &right_dict_hashes))
                .collect()
        };

        let left_hashes: Vec<u64> = if left.num_columns() == 1 {
            Self::single_col_row_hashes(left)
        } else {
            let left_dict_hashes = Self::dict_value_hashes(left);
            (0..left.num_rows())
                .into_par_iter()
                .map(|i| Self::hash_row_dict_aware(left, i, &left_dict_hashes))
                .collect()
        };
        let mut keep: Vec<u32> = Vec::new();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (i, &h) in left_hashes.iter().enumerate() {
            if right_hashes.contains(&h) {
                if all || seen.insert(h) {
                    keep.push(i as u32);
                }
            }
        }
        Self::take_rows(left, &keep)
    }

    /// EXCEPT: rows in left that do NOT appear in right
    fn except_batches(left: &RecordBatch, right: &RecordBatch, all: bool) -> io::Result<RecordBatch> {
        use rayon::prelude::*;
        let right_hashes: std::collections::HashSet<u64> = if right.num_columns() == 1 {
            Self::single_col_row_hashes(right).into_par_iter().collect()
        } else {
            let right_dict_hashes = Self::dict_value_hashes(right);
            (0..right.num_rows())
                .into_par_iter()
                .map(|i| Self::hash_row_dict_aware(right, i, &right_dict_hashes))
                .collect()
        };

        let left_hashes: Vec<u64> = if left.num_columns() == 1 {
            Self::single_col_row_hashes(left)
        } else {
            let left_dict_hashes = Self::dict_value_hashes(left);
            (0..left.num_rows())
                .into_par_iter()
                .map(|i| Self::hash_row_dict_aware(left, i, &left_dict_hashes))
                .collect()
        };
        let mut keep: Vec<u32> = Vec::new();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (i, &h) in left_hashes.iter().enumerate() {
            if !right_hashes.contains(&h) {
                if all || seen.insert(h) {
                    keep.push(i as u32);
                }
            }
        }
        Self::take_rows(left, &keep)
    }

    /// Single-column row fingerprints for set operations. Dictionary columns
    /// hash the distinct values once (no per-row downcast) and resolve each row
    /// through its key id; other columns hash the value. Nulls hash to 0.
    fn single_col_row_hashes(batch: &RecordBatch) -> Vec<u64> {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::UInt32Type;
        use rayon::prelude::*;
        use std::hash::Hasher;
        let col = batch.column(0);
        if let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
            let Some(values) = dict.values().as_any().downcast_ref::<StringArray>() else {
                return (0..batch.num_rows())
                    .into_par_iter()
                    .map(|i| Self::hash_array_value_fast(col, i))
                    .collect();
            };
            let value_hashes: Vec<u64> = (0..values.len())
                .map(|i| {
                    if values.is_null(i) {
                        0u64
                    } else {
                        let mut h = AHasher::default();
                        h.write(values.value(i).as_bytes());
                        h.finish()
                    }
                })
                .collect();
            let keys = dict.keys();
            (0..batch.num_rows())
                .into_par_iter()
                .map(|i| {
                    if keys.is_null(i) {
                        0u64
                    } else {
                        value_hashes
                            .get(keys.value(i) as usize)
                            .copied()
                            .unwrap_or(u64::MAX)
                    }
                })
                .collect()
        } else {
            (0..batch.num_rows())
                .into_par_iter()
                .map(|i| Self::hash_array_value_fast(col, i))
                .collect()
        }
    }

    /// Hash all column values in a single row to a u64 fingerprint
    fn hash_row(batch: &RecordBatch, row: usize) -> u64 {
        use std::hash::Hasher;
        let mut hasher = AHasher::default();
        for col in batch.columns() {
            hasher.write_u64(Self::hash_array_value_fast(col, row));
        }
        hasher.finish()
    }

    /// Row hash that resolves dictionary-encoded string columns through their
    /// key ids: O(dict) string hashing for the whole set operation instead of
    /// one hash per row.
    fn hash_row_dict_aware(batch: &RecordBatch, row: usize, dict_hashes: &[Option<Vec<u64>>]) -> u64 {
        use std::hash::Hasher;
        let mut hasher = AHasher::default();
        for (col_idx, col) in batch.columns().iter().enumerate() {
            match dict_hashes.get(col_idx) {
                Some(Some(dict_hash)) => {
                    // Dictionary-encoded string column: hash the key id, which
                    // uniquely identifies the value.
                    use arrow::array::DictionaryArray;
                    use arrow::datatypes::UInt32Type;
                    if let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
                        if dict.keys().is_null(row) {
                            hasher.write_u64(0);
                        } else {
                            hasher.write_u64(
                                dict_hash
                                    .get(dict.keys().value(row) as usize)
                                    .copied()
                                    .unwrap_or(u64::MAX),
                            );
                        }
                        continue;
                    }
                    hasher.write_u64(Self::hash_array_value_fast(col, row));
                }
                _ => hasher.write_u64(Self::hash_array_value_fast(col, row)),
            }
        }
        hasher.finish()
    }

    /// Pre-compute the per-dict-value hashes for every dictionary-encoded
    /// column in the batch (None entries for non-dict columns).
    fn dict_value_hashes(batch: &RecordBatch) -> Vec<Option<Vec<u64>>> {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::UInt32Type;
        use std::hash::Hasher;
        batch
            .columns()
            .iter()
            .map(|col| {
                let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() else {
                    return None;
                };
                let Some(values) = dict.values().as_any().downcast_ref::<StringArray>() else {
                    return None;
                };
                Some(
                    (0..values.len())
                        .map(|i| {
                            let mut h = AHasher::default();
                            h.write(values.value(i).as_bytes());
                            h.finish()
                        })
                        .collect(),
                )
            })
            .collect()
    }

    /// Take rows by index from a RecordBatch
    fn take_rows(batch: &RecordBatch, indices: &[u32]) -> io::Result<RecordBatch> {
        use arrow::array::UInt32Array;
        let idx_arr = UInt32Array::from(indices.to_vec());
        let cols: Vec<ArrayRef> = batch.columns().iter()
            .map(|col| compute::take(col.as_ref(), &idx_arr, None)
                .map_err(|e| err_data(e.to_string())))
            .collect::<io::Result<Vec<_>>>()?;
        RecordBatch::try_new(batch.schema(), cols)
            .map_err(|e| err_data(e.to_string()))
    }

    /// Concatenate two record batches
    fn concat_batches(left: &RecordBatch, right: &RecordBatch) -> io::Result<RecordBatch> {
        if left.num_rows() == 0 {
            return Ok(right.clone());
        }
        if right.num_rows() == 0 {
            return Ok(left.clone());
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(left.num_columns());
        
        for i in 0..left.num_columns() {
            let left_col = left.column(i);
            let right_col = right.column(i);
            
            let concatenated = compute::concat(&[left_col.as_ref(), right_col.as_ref()])
                .map_err(|e| err_data( e.to_string()))?;
            columns.push(concatenated);
        }

        RecordBatch::try_new(left.schema(), columns)
            .map_err(|e| err_data( e.to_string()))
    }

    /// Deduplicate rows in a record batch (for UNION without ALL)
    /// OPTIMIZATION: Fast path for single-column DISTINCT using dictionary indexing
    fn deduplicate_batch(batch: &RecordBatch) -> io::Result<RecordBatch> {
        use ahash::AHashSet;
        use rayon::prelude::*;
        use std::hash::Hasher;
        use arrow::array::DictionaryArray;
        use arrow::datatypes::UInt32Type;
        
        let num_rows = batch.num_rows();
        if num_rows <= 1 {
            return Ok(batch.clone());
        }

        let num_cols = batch.num_columns();
        
        // FAST PATH: Single column DISTINCT - use direct dictionary indexing
        if num_cols == 1 {
            let col = batch.column(0);
            
            // Case 1: DictionaryArray - already has unique values, just get first occurrence of each key
            if let Some(dict_arr) = col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
                let keys = dict_arr.keys();
                let dict_size = dict_arr.values().len() + 1; // +1 for NULL
                let mut first_occurrence: Vec<Option<u32>> = vec![None; dict_size];
                let mut keep_indices: Vec<u32> = Vec::with_capacity(dict_size);
                
                for row_idx in 0..num_rows {
                    let key = if keys.is_null(row_idx) { 0usize } else { keys.value(row_idx) as usize + 1 };
                    if first_occurrence[key].is_none() {
                        first_occurrence[key] = Some(row_idx as u32);
                        keep_indices.push(row_idx as u32);
                    }
                }
                
                if keep_indices.len() == num_rows {
                    return Ok(batch.clone());
                }
                
                let indices = arrow::array::UInt32Array::from(keep_indices);
                let filtered = compute::take(col.as_ref(), &indices, None)
                    .map_err(|e| err_data( e.to_string()))?;
                return RecordBatch::try_new(batch.schema(), vec![filtered])
                    .map_err(|e| err_data( e.to_string()));
            }
            
            // Case 2: StringArray - build dictionary on the fly for low cardinality
            // REMOVED sampling to stabilize performance
            if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
                // Build dictionary directly without sampling
                let mut dict: AHashMap<&str, u32> = AHashMap::with_capacity(1000);
                let mut keep_indices: Vec<u32> = Vec::with_capacity(1000);
                let mut has_null = false;
                
                for row_idx in 0..num_rows {
                    if str_arr.is_null(row_idx) {
                        if !has_null {
                            has_null = true;
                            keep_indices.push(row_idx as u32);
                        }
                    } else {
                        let s = str_arr.value(row_idx);
                        if !dict.contains_key(s) {
                            dict.insert(s, row_idx as u32);
                            keep_indices.push(row_idx as u32);
                        }
                    }
                }
                
                if keep_indices.len() == num_rows {
                    return Ok(batch.clone());
                }
                
                let indices = arrow::array::UInt32Array::from(keep_indices);
                let filtered = compute::take(col.as_ref(), &indices, None)
                    .map_err(|e| err_data( e.to_string()))?;
                return RecordBatch::try_new(batch.schema(), vec![filtered])
                    .map_err(|e| err_data( e.to_string()));
            }
            
            // Case 3: Int64Array - use direct value dedup
            if let Some(int_arr) = col.as_any().downcast_ref::<Int64Array>() {
                let mut seen: AHashSet<i64> = AHashSet::with_capacity(num_rows.min(10000));
                let mut keep_indices: Vec<u32> = Vec::with_capacity(num_rows.min(10000));
                let mut has_null = false;
                
                for row_idx in 0..num_rows {
                    if int_arr.is_null(row_idx) {
                        if !has_null {
                            has_null = true;
                            keep_indices.push(row_idx as u32);
                        }
                    } else if seen.insert(int_arr.value(row_idx)) {
                        keep_indices.push(row_idx as u32);
                    }
                }
                
                if keep_indices.len() == num_rows {
                    return Ok(batch.clone());
                }
                
                let indices = arrow::array::UInt32Array::from(keep_indices);
                let filtered = compute::take(col.as_ref(), &indices, None)
                    .map_err(|e| err_data( e.to_string()))?;
                return RecordBatch::try_new(batch.schema(), vec![filtered])
                    .map_err(|e| err_data( e.to_string()));
            }
        }
        
        // General path for multi-column deduplication
        // Pre-compute column types for faster dispatch
        enum ColType<'a> {
            Int64(&'a Int64Array),
            Float64(&'a Float64Array),
            String(&'a StringArray, Vec<u64>),  // Pre-computed string hashes
            Bool(&'a BooleanArray),
            StringDict(&'a DictionaryArray<UInt32Type>),
            Other(&'a ArrayRef),
        }
        
        let typed_cols: Vec<ColType> = batch.columns().iter().map(|col| {
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                ColType::Int64(arr)
            } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                ColType::Float64(arr)
            } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                // Pre-compute hashes for strings
                let hashes: Vec<u64> = (0..num_rows).map(|i| {
                    if arr.is_null(i) { 0 } else {
                        let mut h = ahash::AHasher::default();
                        h.write(arr.value(i).as_bytes());
                        h.finish()
                    }
                }).collect();
                ColType::String(arr, hashes)
            } else if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                ColType::Bool(arr)
            } else if let Some(arr) =
                col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>()
            {
                // Dictionary-encoded string: the key id uniquely identifies
                // the value, so rows with the same key deduplicate exactly and
                // no per-row string hashing is needed.
                ColType::StringDict(arr)
            } else {
                ColType::Other(col)
            }
        }).collect();
        
        // Pre-compute all row hashes for deduplication (parallel over rows).
        let row_hashes: Vec<u64> = (0..num_rows)
            .into_par_iter()
            .map(|row_idx| {
                let mut hasher = ahash::AHasher::default();
                for typed_col in &typed_cols {
                    match typed_col {
                        ColType::Int64(arr) => {
                            if arr.is_null(row_idx) {
                                hasher.write_u8(0);
                            } else {
                                hasher.write_u8(1);
                                hasher.write_i64(arr.value(row_idx));
                            }
                        }
                        ColType::Float64(arr) => {
                            if arr.is_null(row_idx) {
                                hasher.write_u8(0);
                            } else {
                                hasher.write_u8(1);
                                hasher.write_u64(arr.value(row_idx).to_bits());
                            }
                        }
                        ColType::String(_arr, hashes) => {
                            hasher.write_u64(hashes[row_idx]);
                        }
                        ColType::Bool(arr) => {
                            if arr.is_null(row_idx) {
                                hasher.write_u8(0);
                            } else {
                                hasher.write_u8(if arr.value(row_idx) { 2 } else { 1 });
                            }
                        }
                        ColType::StringDict(arr) => {
                            if arr.keys().is_null(row_idx) {
                                hasher.write_u8(0);
                            } else {
                                hasher.write_u8(1);
                                hasher.write_u32(arr.keys().value(row_idx));
                            }
                        }
                        ColType::Other(arr) => {
                            // Dictionary-encoded string column: hash the VALUE
                            // so distinct rows with the same key deduplicate.
                            if let Some(dict) = arr
                                .as_any()
                                .downcast_ref::<DictionaryArray<UInt32Type>>()
                            {
                                if dict.keys().is_null(row_idx) {
                                    hasher.write_u8(0);
                                } else {
                                    if let Some(values) = dict
                                        .values()
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                    {
                                        let key = dict.keys().value(row_idx) as usize;
                                        if key < values.len() && !values.is_null(key) {
                                            hasher.write_u8(1);
                                            hasher.write(values.value(key).as_bytes());
                                            continue;
                                        }
                                    }
                                    hasher.write_u8(0);
                                }
                            } else {
                                hasher.write_u8(if arr.is_null(row_idx) { 0 } else { 1 });
                                hasher.write_usize(row_idx);
                            }
                        }
                    }
                }
                hasher.finish()
            })
            .collect();
        
        // Sequential deduplication using pre-computed hashes
        let mut seen: AHashSet<u64> = AHashSet::with_capacity(num_rows.min(10000));
        let mut keep_indices: Vec<u32> = Vec::with_capacity(num_rows.min(10000));

        for (row_idx, &hash) in row_hashes.iter().enumerate() {
            if seen.insert(hash) {
                keep_indices.push(row_idx as u32);
            }
        }

        if keep_indices.len() == num_rows {
            return Ok(batch.clone());
        }

        // Create filtered batch
        let indices = arrow::array::UInt32Array::from(keep_indices);
        let mut result_columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);
        
        for col in batch.columns() {
            let filtered = compute::take(col.as_ref(), &indices, None)
                .map_err(|e| err_data( e.to_string()))?;
            result_columns.push(filtered);
        }

        RecordBatch::try_new(batch.schema(), result_columns)
            .map_err(|e| err_data( e.to_string()))
    }

    /// DISTINCT ON: keep first row per unique combination of specified columns
    pub(crate) fn deduplicate_batch_on(batch: &RecordBatch, on_columns: &[String]) -> io::Result<RecordBatch> {
        if batch.num_rows() <= 1 || on_columns.is_empty() {
            return Ok(batch.clone());
        }

        // Find column indices for the ON columns
        let col_indices: Vec<usize> = on_columns
            .iter()
            .filter_map(|col_name| {
                batch.schema().index_of(col_name).ok()
            })
            .collect();

        if col_indices.is_empty() {
            return Ok(batch.clone());
        }

        let num_rows = batch.num_rows();
        let mut keep_indices: Vec<u32> = Vec::with_capacity(num_rows.min(10000));
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::with_capacity(num_rows.min(10000));

        for row_idx in 0..num_rows {
            let mut key = Vec::with_capacity(col_indices.len() * 16);
            for &col_idx in &col_indices {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    key.push(0);
                } else {
                    key.push(1);
                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        key.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                    } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                        key.extend_from_slice(&arr.value(row_idx).to_bits().to_le_bytes());
                    } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        key.extend_from_slice(arr.value(row_idx).as_bytes());
                    } else if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                        key.push(arr.value(row_idx) as u8);
                    } else if let Some(arr) = col.as_any().downcast_ref::<
                        arrow::array::DictionaryArray<arrow::datatypes::UInt32Type>,
                    >() {
                        if !arr.keys().is_null(row_idx) {
                            if let Some(values) =
                                arr.values().as_any().downcast_ref::<StringArray>()
                            {
                                let dict_key = arr.keys().value(row_idx) as usize;
                                if dict_key < values.len() && !values.is_null(dict_key) {
                                    key.extend_from_slice(values.value(dict_key).as_bytes());
                                }
                            }
                        }
                    } else {
                        // Fallback: use Debug representation
                        key.extend_from_slice(format!("{:?}", col).as_bytes());
                    }
                }
            }
            if seen.insert(key) {
                keep_indices.push(row_idx as u32);
            }
        }

        if keep_indices.len() == num_rows {
            return Ok(batch.clone());
        }

        let indices = arrow::array::UInt32Array::from(keep_indices);
        let mut result_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        for col in batch.columns() {
            let filtered = compute::take(col.as_ref(), &indices, None)
                .map_err(|e| err_data( e.to_string()))?;
            result_columns.push(filtered);
        }

        RecordBatch::try_new(batch.schema(), result_columns)
            .map_err(|e| err_data( e.to_string()))
    }

    /// Append value signature for deduplication
    fn append_value_signature(sig: &mut Vec<u8>, array: &ArrayRef, idx: usize) {
        if array.is_null(idx) {
            sig.push(0);
            return;
        }
        sig.push(1);

        if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
            sig.extend_from_slice(&arr.value(idx).to_le_bytes());
        } else if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
            sig.extend_from_slice(&arr.value(idx).to_le_bytes());
        } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
            sig.extend_from_slice(&arr.value(idx).to_bits().to_le_bytes());
        } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            let s = arr.value(idx);
            sig.extend_from_slice(&(s.len() as u32).to_le_bytes());
            sig.extend_from_slice(s.as_bytes());
        } else if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
            sig.push(if arr.value(idx) { 1 } else { 0 });
        }
    }

    /// Execute window function (ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST, LAG, LEAD, SUM, AVG, etc.)
    fn execute_window_function(batch: &RecordBatch, stmt: &SelectStatement) -> io::Result<ApexResult> {
        // Collect window specs: (func_name, args, partition_by, order_by, output_name)
        let mut window_specs: Vec<(String, Vec<String>, Vec<String>, Vec<crate::query::OrderByClause>, String)> = Vec::new();
        
        let supported = ["ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "PERCENT_RANK", "CUME_DIST", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "SUM", "AVG", "COUNT", "MIN", "MAX", "RUNNING_SUM"];
        
        for col in &stmt.columns {
            if let SelectColumn::WindowFunction { name, args, partition_by, order_by, alias } = col {
                if !supported.iter().any(|s| name.eq_ignore_ascii_case(s)) {
                    return Err(err_input(format!("Unsupported window function: {}", name)));
                }
                let out_name = alias.clone().unwrap_or_else(|| name.to_lowercase());
                let upper = name.to_ascii_uppercase();
                window_specs.push((upper, args.clone(), partition_by.clone(), order_by.clone(), out_name));
            }
        }

        if window_specs.is_empty() {
            return Err(err_input("No window function found"));
        }

        let num_rows = batch.num_rows();
        let use_float: Vec<bool> = window_specs
            .iter()
            .map(|(func_name, func_args, _, _, _)| {
                let source_is_float = func_args
                    .first()
                    .and_then(|name| Self::get_column_by_name(batch, name.trim_matches('"')))
                    .is_some_and(|column| column.as_any().is::<Float64Array>());
                source_is_float
                    || matches!(func_name.as_str(), "AVG" | "PERCENT_RANK" | "CUME_DIST")
            })
            .collect();
        // Per-spec nullable output: int (rank/row_number) or float (sum/avg/lag)
        let mut per_int: Vec<Vec<Option<i64>>> = use_float
            .iter()
            .map(|float| if *float { Vec::new() } else { vec![None; num_rows] })
            .collect();
        let mut per_flt: Vec<Vec<Option<f64>>> = use_float
            .iter()
            .map(|float| if *float { vec![None; num_rows] } else { Vec::new() })
            .collect();
        // Multiple window functions in analytics SQL usually share an identical
        // PARTITION BY / ORDER BY clause.  Hashing and sorting the million-row
        // input independently for every ROW_NUMBER/LAG/LEAD is pure duplicate
        // work, so retain the ordered row groups for the duration of this SELECT.
        let mut ordered_group_cache: AHashMap<String, Arc<Vec<Vec<usize>>>> = AHashMap::new();

        for (spec_idx, (func_name, func_args, partition_by, order_by, _)) in window_specs.iter().enumerate() {
            let cache_key = format!("{:?}|{:?}", partition_by, order_by);
            let order_cols: Vec<(ArrayRef, bool)> = order_by
                .iter()
                .filter_map(|order| {
                    Self::get_column_by_name(batch, order.column.trim_matches('"'))
                        .cloned()
                        .map(|column| (column, order.descending))
                })
                .collect();
            let compare_order = |a: usize, b: usize| {
                for (column, descending) in &order_cols {
                    let mut ordering = Self::compare_array_values(column, a, b);
                    if *descending {
                        ordering = ordering.reverse();
                    }
                    if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }
                std::cmp::Ordering::Equal
            };
            let groups = if let Some(cached) = ordered_group_cache.get(&cache_key) {
                Arc::clone(cached)
            } else {
                let mut grouped: AHashMap<u64, Vec<usize>> =
                    AHashMap::with_capacity(num_rows / 10 + 1);
                let part_cols: Vec<Option<&ArrayRef>> = partition_by.iter()
                    .map(|cn| Self::get_column_by_name(batch, cn.trim_matches('"')))
                    .collect();
                for row_idx in 0..num_rows {
                    let mut hasher = AHasher::default();
                    for col_opt in &part_cols {
                        if let Some(col) = col_opt {
                            hasher.write_u64(Self::hash_array_value_fast(col, row_idx));
                        }
                    }
                    let key = if partition_by.is_empty() { 0 } else { hasher.finish() };
                    grouped.entry(key).or_insert_with(|| Vec::with_capacity(16)).push(row_idx);
                }
                let mut ordered = grouped.into_values().collect::<Vec<_>>();
                if !order_cols.is_empty() {
                    for indices in &mut ordered {
                        indices.sort_by(|&a, &b| compare_order(a, b));
                    }
                }
                let ordered = Arc::new(ordered);
                ordered_group_cache.insert(cache_key, Arc::clone(&ordered));
                ordered
            };

            for indices in groups.iter() {
                match func_name.as_str() {
                    "ROW_NUMBER" => {
                        for (pos, &row_idx) in indices.iter().enumerate() {
                            per_int[spec_idx][row_idx] = Some((pos + 1) as i64);
                        }
                    }
                    "RANK" => {
                        let mut rank = 1i64;
                        let mut prev: Option<usize> = None;
                        for (pos, &row_idx) in indices.iter().enumerate() {
                            if let Some(p) = prev {
                                if compare_order(p, row_idx) != std::cmp::Ordering::Equal {
                                    rank = (pos + 1) as i64;
                                }
                            }
                            per_int[spec_idx][row_idx] = Some(rank);
                            prev = Some(row_idx);
                        }
                    }
                    "DENSE_RANK" => {
                        let mut rank = 1i64;
                        let mut prev: Option<usize> = None;
                        for &row_idx in indices {
                            if let Some(p) = prev {
                                if compare_order(p, row_idx) != std::cmp::Ordering::Equal {
                                    rank += 1;
                                }
                            }
                            per_int[spec_idx][row_idx] = Some(rank);
                            prev = Some(row_idx);
                        }
                    }
                    "NTILE" => {
                        let n = func_args
                            .get(0)
                            .and_then(|s| {
                                s.trim_start_matches("Int64(")
                                    .trim_end_matches(')')
                                    .parse::<i64>()
                                    .ok()
                            })
                            .unwrap_or(4);
                        let count = indices.len() as i64;
                        for (pos, &row_idx) in indices.iter().enumerate() {
                            per_int[spec_idx][row_idx] = Some(((pos as i64 * n / count) + 1).min(n));
                        }
                    }
                    "PERCENT_RANK" => {
                        let count = indices.len();
                        let mut rank = 1i64;
                        let mut prev: Option<usize> = None;
                        for (pos, &row_idx) in indices.iter().enumerate() {
                            if let Some(p) = prev {
                                if compare_order(p, row_idx) != std::cmp::Ordering::Equal {
                                    rank = (pos + 1) as i64;
                                }
                            }
                            let pct = if count <= 1 { 0.0 } else { (rank - 1) as f64 / (count - 1) as f64 };
                            per_flt[spec_idx][row_idx] = Some(pct);
                            prev = Some(row_idx);
                        }
                    }
                    "CUME_DIST" => {
                        let count = indices.len();
                        let mut peer_start = 0usize;
                        while peer_start < count {
                            let mut peer_end = peer_start + 1;
                            while peer_end < count
                                && compare_order(indices[peer_start], indices[peer_end])
                                    == std::cmp::Ordering::Equal
                            {
                                peer_end += 1;
                            }
                            let value = peer_end as f64 / count as f64;
                            for &row_idx in &indices[peer_start..peer_end] {
                                per_flt[spec_idx][row_idx] = Some(value);
                            }
                            peer_start = peer_end;
                        }
                    }
                    "LAG" => {
                        let offset = func_args.get(1).and_then(|s| s.trim_start_matches("Int64(").trim_end_matches(')').parse().ok()).unwrap_or(1usize);
                        let col_name = func_args.get(0).map(|s| s.trim_matches('"')).unwrap_or("");
                        if let Some(src_col) = Self::get_column_by_name(batch, col_name) {
                            if let Some(fa) = src_col.as_any().downcast_ref::<Float64Array>() {
                                for (pos, &ri) in indices.iter().enumerate() {
                                    per_flt[spec_idx][ri] = if pos >= offset {
                                        let pr = indices[pos - offset];
                                        if fa.is_null(pr) { None } else { Some(fa.value(pr)) }
                                    } else { None };
                                }
                            } else if let Some(ia) = src_col.as_any().downcast_ref::<Int64Array>() {
                                for (pos, &ri) in indices.iter().enumerate() {
                                    per_int[spec_idx][ri] = if pos >= offset {
                                        let pr = indices[pos - offset];
                                        if ia.is_null(pr) { None } else { Some(ia.value(pr)) }
                                    } else { None };
                                }
                            }
                        }
                    }
                    "LEAD" => {
                        let offset = func_args.get(1).and_then(|s| s.trim_start_matches("Int64(").trim_end_matches(')').parse().ok()).unwrap_or(1usize);
                        let col_name = func_args.get(0).map(|s| s.trim_matches('"')).unwrap_or("");
                        let len = indices.len();
                        if let Some(src_col) = Self::get_column_by_name(batch, col_name) {
                            if let Some(fa) = src_col.as_any().downcast_ref::<Float64Array>() {
                                for (pos, &ri) in indices.iter().enumerate() {
                                    per_flt[spec_idx][ri] = if pos + offset < len {
                                        let nr = indices[pos + offset];
                                        if fa.is_null(nr) { None } else { Some(fa.value(nr)) }
                                    } else { None };
                                }
                            } else if let Some(ia) = src_col.as_any().downcast_ref::<Int64Array>() {
                                for (pos, &ri) in indices.iter().enumerate() {
                                    per_int[spec_idx][ri] = if pos + offset < len {
                                        let nr = indices[pos + offset];
                                        if ia.is_null(nr) { None } else { Some(ia.value(nr)) }
                                    } else { None };
                                }
                            }
                        }
                    }
                    "FIRST_VALUE" => {
                        let col_name = func_args.get(0).map(|s| s.trim_matches('"')).unwrap_or("");
                        if let Some(src_col) = Self::get_column_by_name(batch, col_name) {
                            let fr = indices[0];
                            if let Some(fa) = src_col.as_any().downcast_ref::<Float64Array>() {
                                let v = if fa.is_null(fr) { None } else { Some(fa.value(fr)) };
                                for &ri in indices { per_flt[spec_idx][ri] = v; }
                            } else if let Some(ia) = src_col.as_any().downcast_ref::<Int64Array>() {
                                let v = if ia.is_null(fr) { None } else { Some(ia.value(fr)) };
                                for &ri in indices { per_int[spec_idx][ri] = v; }
                            }
                        }
                    }
                    "LAST_VALUE" => {
                        let col_name = func_args.get(0).map(|s| s.trim_matches('"')).unwrap_or("");
                        if let Some(src_col) = Self::get_column_by_name(batch, col_name) {
                            let lr = indices[indices.len() - 1];
                            if let Some(fa) = src_col.as_any().downcast_ref::<Float64Array>() {
                                let v = if fa.is_null(lr) { None } else { Some(fa.value(lr)) };
                                for &ri in indices { per_flt[spec_idx][ri] = v; }
                            } else if let Some(ia) = src_col.as_any().downcast_ref::<Int64Array>() {
                                let v = if ia.is_null(lr) { None } else { Some(ia.value(lr)) };
                                for &ri in indices { per_int[spec_idx][ri] = v; }
                            }
                        }
                    }
                    "SUM" => {
                        let col_name = func_args.get(0).map(|s| s.trim_matches('"')).unwrap_or("");
                        if let Some(src_col) = Self::get_column_by_name(batch, col_name) {
                            if !order_by.is_empty() {
                                // Running (cumulative) sum when ORDER BY is present
                                if let Some(fa) = src_col.as_any().downcast_ref::<Float64Array>() {
                                    let mut running = 0.0f64;
                                    for &ri in indices {
                                        if !fa.is_null(ri) { running += fa.value(ri); }
                                        per_flt[spec_idx][ri] = Some(running);
                                    }
                                } else if let Some(ia) = src_col.as_any().downcast_ref::<Int64Array>() {
                                    let mut running = 0i64;
                                    for &ri in indices {
                                        if !ia.is_null(ri) { running += ia.value(ri); }
                                        per_int[spec_idx][ri] = Some(running);
                                    }
                                }
                            } else {
                                // Total partition sum when no ORDER BY
                                if let Some(fa) = src_col.as_any().downcast_ref::<Float64Array>() {
                                    let total: f64 = indices.iter().filter_map(|&i| if fa.is_null(i) { None } else { Some(fa.value(i)) }).sum();
                                    for &ri in indices { per_flt[spec_idx][ri] = Some(total); }
                                } else if let Some(ia) = src_col.as_any().downcast_ref::<Int64Array>() {
                                    let total: i64 = indices.iter().filter_map(|&i| if ia.is_null(i) { None } else { Some(ia.value(i)) }).sum();
                                    for &ri in indices { per_int[spec_idx][ri] = Some(total); }
                                }
                            }
                        }
                    }
                    "RUNNING_SUM" => {
                        let col_name = func_args.get(0).map(|s| s.trim_matches('"')).unwrap_or("");
                        if let Some(src_col) = Self::get_column_by_name(batch, col_name) {
                            if let Some(fa) = src_col.as_any().downcast_ref::<Float64Array>() {
                                let mut running = 0.0f64;
                                for &ri in indices { if !fa.is_null(ri) { running += fa.value(ri); } per_flt[spec_idx][ri] = Some(running); }
                            } else if let Some(ia) = src_col.as_any().downcast_ref::<Int64Array>() {
                                let mut running = 0i64;
                                for &ri in indices { if !ia.is_null(ri) { running += ia.value(ri); } per_int[spec_idx][ri] = Some(running); }
                            }
                        }
                    }
                    "AVG" => {
                        let col_name = func_args.get(0).map(|s| s.trim_matches('"')).unwrap_or("");
                        if let Some(src_col) = Self::get_column_by_name(batch, col_name) {
                            if let Some(fa) = src_col.as_any().downcast_ref::<Float64Array>() {
                                let vals: Vec<f64> = indices.iter().filter_map(|&i| if fa.is_null(i) { None } else { Some(fa.value(i)) }).collect();
                                let avg = if vals.is_empty() { 0.0 } else { vals.iter().sum::<f64>() / vals.len() as f64 };
                                for &ri in indices { per_flt[spec_idx][ri] = Some(avg); }
                            } else if let Some(ia) = src_col.as_any().downcast_ref::<Int64Array>() {
                                let vals: Vec<i64> = indices.iter().filter_map(|&i| if ia.is_null(i) { None } else { Some(ia.value(i)) }).collect();
                                let avg = if vals.is_empty() { 0.0 } else { vals.iter().sum::<i64>() as f64 / vals.len() as f64 };
                                for &ri in indices { per_flt[spec_idx][ri] = Some(avg); }
                            }
                        }
                    }
                    "COUNT" => {
                        let cnt = indices.len() as i64;
                        for &ri in indices { per_int[spec_idx][ri] = Some(cnt); }
                    }
                    "MIN" => {
                        let col_name = func_args.get(0).map(|s| s.trim_matches('"')).unwrap_or("");
                        if let Some(src_col) = Self::get_column_by_name(batch, col_name) {
                            if let Some(fa) = src_col.as_any().downcast_ref::<Float64Array>() {
                                let mv = indices.iter().filter_map(|&i| if fa.is_null(i) { None } else { Some(fa.value(i)) }).fold(f64::INFINITY, f64::min);
                                let mv = if mv == f64::INFINITY { None } else { Some(mv) };
                                for &ri in indices { per_flt[spec_idx][ri] = mv; }
                            } else if let Some(ia) = src_col.as_any().downcast_ref::<Int64Array>() {
                                let mv = indices.iter().filter_map(|&i| if ia.is_null(i) { None } else { Some(ia.value(i)) }).min();
                                for &ri in indices { per_int[spec_idx][ri] = mv; }
                            }
                        }
                    }
                    "MAX" => {
                        let col_name = func_args.get(0).map(|s| s.trim_matches('"')).unwrap_or("");
                        if let Some(src_col) = Self::get_column_by_name(batch, col_name) {
                            if let Some(fa) = src_col.as_any().downcast_ref::<Float64Array>() {
                                let mv = indices.iter().filter_map(|&i| if fa.is_null(i) { None } else { Some(fa.value(i)) }).fold(f64::NEG_INFINITY, f64::max);
                                let mv = if mv == f64::NEG_INFINITY { None } else { Some(mv) };
                                for &ri in indices { per_flt[spec_idx][ri] = mv; }
                            } else if let Some(ia) = src_col.as_any().downcast_ref::<Int64Array>() {
                                let mv = indices.iter().filter_map(|&i| if ia.is_null(i) { None } else { Some(ia.value(i)) }).max();
                                for &ri in indices { per_int[spec_idx][ri] = mv; }
                            }
                        }
                    }
                    "NTH_VALUE" => {
                        let col_name = func_args.get(0).map(|s| s.trim_matches('"')).unwrap_or("");
                        let n = func_args.get(1).and_then(|s| s.trim_start_matches("Int64(").trim_end_matches(')').parse::<usize>().ok()).unwrap_or(1);
                        if let Some(src_col) = Self::get_column_by_name(batch, col_name) {
                            if let Some(fa) = src_col.as_any().downcast_ref::<Float64Array>() {
                                let v = if n > 0 && n <= indices.len() {
                                    let nr = indices[n-1]; if fa.is_null(nr) { None } else { Some(fa.value(nr)) }
                                } else { None };
                                for &ri in indices { per_flt[spec_idx][ri] = v; }
                            } else if let Some(ia) = src_col.as_any().downcast_ref::<Int64Array>() {
                                let v = if n > 0 && n <= indices.len() {
                                    let nr = indices[n-1]; if ia.is_null(nr) { None } else { Some(ia.value(nr)) }
                                } else { None };
                                for &ri in indices { per_int[spec_idx][ri] = v; }
                            }
                        }
                    }
                    _ => {}
                }
            } // end groups loop
        } // end spec loop

        let row_number_keep: Option<Vec<u32>> =
            stmt.window_row_number_limit.as_ref().and_then(|(limit_alias, limit)| {
                let spec_idx = window_specs.iter().position(|(func_name, _, _, _, out_name)| {
                    func_name == "ROW_NUMBER" && out_name.eq_ignore_ascii_case(limit_alias)
                })?;
                let keep = per_int[spec_idx]
                    .iter()
                    .enumerate()
                    .filter_map(|(row_idx, rank)| {
                        rank.and_then(|rank| {
                            if rank >= 1 && (rank as usize) <= *limit {
                                Some(row_idx as u32)
                            } else {
                                None
                            }
                        })
                    })
                    .collect::<Vec<_>>();
                Some(keep)
            });
        let row_number_take: Option<arrow::array::UInt32Array> = row_number_keep
            .as_ref()
            .map(|indices: &Vec<u32>| arrow::array::UInt32Array::from(indices.clone()));

        // Build result with original columns + window function result columns
        let mut result_fields: Vec<Field> = Vec::new();
        let mut result_arrays: Vec<ArrayRef> = Vec::new();
        let mut spec_idx = 0usize;

        for col in &stmt.columns {
            match col {
                SelectColumn::Column(name) => {
                    let col_name = name.trim_matches('"');
                    if let Some(arr) = Self::get_column_by_name(batch, col_name) {
                        result_fields.push(Field::new(
                            Self::strip_table_prefix(col_name),
                            arr.data_type().clone(),
                            true,
                        ));
                        if let Some(indices) = &row_number_take {
                            result_arrays.push(
                                compute::take(arr.as_ref(), indices, None)
                                    .map_err(|e| err_data(e.to_string()))?,
                            );
                        } else {
                            result_arrays.push(arr.clone());
                        }
                    }
                }
                SelectColumn::ColumnAlias { column, alias } => {
                    let col_name = column.trim_matches('"');
                    if let Some(arr) = Self::get_column_by_name(batch, col_name) {
                        result_fields.push(Field::new(alias, arr.data_type().clone(), true));
                        if let Some(indices) = &row_number_take {
                            result_arrays.push(
                                compute::take(arr.as_ref(), indices, None)
                                    .map_err(|e| err_data(e.to_string()))?,
                            );
                        } else {
                            result_arrays.push(arr.clone());
                        }
                    }
                }
                SelectColumn::All => {
                    for (i, field) in batch.schema().fields().iter().enumerate() {
                        result_fields.push(field.as_ref().clone());
                        if let Some(indices) = &row_number_take {
                            result_arrays.push(
                                compute::take(batch.column(i).as_ref(), indices, None)
                                    .map_err(|e| err_data(e.to_string()))?,
                            );
                        } else {
                            result_arrays.push(batch.column(i).clone());
                        }
                    }
                }
                SelectColumn::Expression { expr, alias } => {
                    let array = Self::evaluate_expr_to_array(batch, expr)?;
                    let name = alias.clone().unwrap_or_else(|| "expression".to_string());
                    result_fields.push(Field::new(&name, array.data_type().clone(), true));
                    if let Some(indices) = &row_number_take {
                        result_arrays.push(
                            compute::take(array.as_ref(), indices, None)
                                .map_err(|e| err_data(e.to_string()))?,
                        );
                    } else {
                        result_arrays.push(array);
                    }
                }
                SelectColumn::WindowFunction { name, alias, .. } => {
                    let out_name = alias.clone().unwrap_or_else(|| name.to_lowercase());
                    if use_float[spec_idx] {
                        result_fields.push(Field::new(&out_name, ArrowDataType::Float64, true));
                        let values: Vec<Option<f64>> = if let Some(indices) = &row_number_keep {
                            indices
                                .iter()
                                .map(|&row| per_flt[spec_idx][row as usize])
                                .collect()
                        } else {
                            per_flt[spec_idx].clone()
                        };
                        result_arrays.push(Arc::new(Float64Array::from(values)));
                    } else {
                        result_fields.push(Field::new(&out_name, ArrowDataType::Int64, true));
                        let values: Vec<Option<i64>> = if let Some(indices) = &row_number_keep {
                            indices
                                .iter()
                                .map(|&row| per_int[spec_idx][row as usize])
                                .collect()
                        } else {
                            per_int[spec_idx].clone()
                        };
                        result_arrays.push(Arc::new(Int64Array::from(values)));
                    }
                    spec_idx += 1;
                }
                _ => {}
            }
        }

        let schema = Arc::new(Schema::new(result_fields));
        let mut result = RecordBatch::try_new(schema, result_arrays)
            .map_err(|e| err_data(e.to_string()))?;

        // Window evaluation must still honour the outer ORDER BY and LIMIT/OFFSET.
        // Apply them after the window columns are materialized so aliases such as
        // `ORDER BY rk LIMIT 10` resolve against the projected result.
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

    /// Compare two array values for sorting
    fn compare_array_values(array: &ArrayRef, a: usize, b: usize) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        
        if array.is_null(a) && array.is_null(b) {
            return Ordering::Equal;
        }
        if array.is_null(a) {
            return Ordering::Greater;
        }
        if array.is_null(b) {
            return Ordering::Less;
        }

        if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
            arr.value(a).cmp(&arr.value(b))
        } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
            arr.value(a).partial_cmp(&arr.value(b)).unwrap_or(Ordering::Equal)
        } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            arr.value(a).cmp(arr.value(b))
        } else if let Some(arr) = array.as_any().downcast_ref::<
            arrow::array::DictionaryArray<arrow::datatypes::UInt32Type>,
        >() {
            // Dictionary-encoded string column: compare the VALUES, not the
            // key ids, so ORDER BY over dict-encoded inputs stays correct.
            let get = |idx: usize| -> Option<&str> {
                if arr.keys().is_null(idx) {
                    return None;
                }
                let key = arr.keys().value(idx) as usize;
                arr.values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .and_then(|values| {
                        if key < values.len() && !values.is_null(key) {
                            Some(values.value(key))
                        } else {
                            None
                        }
                    })
            };
            match (get(a), get(b)) {
                (Some(a_val), Some(b_val)) => a_val.cmp(b_val),
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
            }
        } else {
            Ordering::Equal
        }
    }

}
