// Predicate extraction helpers: LIKE/IN/BETWEEN/comparison patterns for fast paths.

impl ApexExecutor {
    /// Helper to extract LIKE pattern: col LIKE 'pattern' (non-negated only)
    fn extract_like_pattern(expr: &SqlExpr) -> Option<(String, String)> {
        match expr {
            SqlExpr::Like {
                column,
                pattern,
                negated,
            } if !negated => Some((column.trim_matches('"').to_string(), pattern.clone())),
            _ => None,
        }
    }

    /// Helper to extract string equality: col = 'value'
    fn extract_string_equality(expr: &SqlExpr) -> Option<(String, String)> {
        use crate::query::sql_parser::BinaryOperator;
        match expr {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => match (left.as_ref(), right.as_ref()) {
                (SqlExpr::Column(col), SqlExpr::Literal(Value::String(val)))
                | (SqlExpr::Literal(Value::String(val)), SqlExpr::Column(col))
                    if !val.is_empty() =>
                {
                    let clean = col.trim_matches('"');
                    Some((
                        clean.rsplit('.').next().unwrap_or(clean).to_string(),
                        val.clone(),
                    ))
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Helper to extract a string IN list: `col IN ('a', 'b', ...)` (non-negated only).
    ///
    /// Returns `None` unless every element is a non-empty string literal, so a list that
    /// mixes types keeps the ordinary path.
    fn extract_string_in_list(expr: &SqlExpr) -> Option<(String, Vec<String>)> {
        match expr {
            SqlExpr::In {
                column,
                values,
                negated: false,
            } => {
                let clean = column.trim_matches('"');
                let name = clean.rsplit('.').next().unwrap_or(clean).to_string();
                let mut out: Vec<String> = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        Value::String(text) if !text.is_empty() => out.push(text.clone()),
                        _ => return None,
                    }
                }
                if out.is_empty() {
                    None
                } else {
                    Some((name, out))
                }
            }
            _ => None,
        }
    }

    #[inline]
    fn column_is_string(backend: &TableStorageBackend, column: &str) -> bool {
        matches!(
            backend.get_column_type(column),
            Some(crate::data::DataType::String)
        )
    }

    fn coerce_predicate_literal(
        backend: &TableStorageBackend,
        column: &str,
        value: &mut Value,
    ) -> io::Result<()> {
        let Value::String(text) = value else {
            return Ok(());
        };
        let clean = column.trim_matches('"');
        let clean = clean.rsplit('.').next().unwrap_or(clean);
        let data_type = if clean == "_id" {
            None
        } else {
            backend.get_column_type(clean)
        };
        let coerced = match data_type {
            Some(
                crate::data::DataType::Int64
                | crate::data::DataType::Int32
                | crate::data::DataType::Int16
                | crate::data::DataType::Int8,
            ) => Value::Int64(text.parse::<i64>().map_err(|_| {
                err_input(format!(
                    "cannot compare integer column '{}' with non-integer string {:?}",
                    clean, text
                ))
            })?),
            Some(crate::data::DataType::Float64 | crate::data::DataType::Float32) => {
                let parsed = text.parse::<f64>().map_err(|_| {
                    err_input(format!(
                        "cannot compare floating-point column '{}' with non-numeric string {:?}",
                        clean, text
                    ))
                })?;
                if !parsed.is_finite() {
                    return Err(err_input(format!(
                        "floating-point comparison for column '{}' requires a finite value",
                        clean
                    )));
                }
                Value::Float64(parsed)
            }
            None if clean == "_id" => Value::UInt64(text.parse::<u64>().map_err(|_| {
                err_input(format!(
                    "cannot compare unsigned integer column '_id' with non-integer string {:?}",
                    text
                ))
            })?),
            _ => return Ok(()),
        };
        *value = coerced;
        Ok(())
    }

    fn coerce_predicate_literals(
        expr: &mut SqlExpr,
        backend: &TableStorageBackend,
    ) -> io::Result<()> {
        match expr {
            SqlExpr::BinaryOp { left, right, .. } => {
                match (left.as_mut(), right.as_mut()) {
                    (SqlExpr::Column(column), SqlExpr::Literal(value)) => {
                        Self::coerce_predicate_literal(backend, column, value)?;
                    }
                    (SqlExpr::Literal(value), SqlExpr::Column(column)) => {
                        Self::coerce_predicate_literal(backend, column, value)?;
                    }
                    _ => {}
                }
                Self::coerce_predicate_literals(left, backend)?;
                Self::coerce_predicate_literals(right, backend)
            }
            SqlExpr::Between {
                column, low, high, ..
            } => {
                if let SqlExpr::Literal(value) = low.as_mut() {
                    Self::coerce_predicate_literal(backend, column, value)?;
                }
                if let SqlExpr::Literal(value) = high.as_mut() {
                    Self::coerce_predicate_literal(backend, column, value)?;
                }
                Ok(())
            }
            SqlExpr::In { column, values, .. } => {
                for value in values {
                    Self::coerce_predicate_literal(backend, column, value)?;
                }
                Ok(())
            }
            SqlExpr::Paren(inner) | SqlExpr::UnaryOp { expr: inner, .. } => {
                Self::coerce_predicate_literals(inner, backend)
            }
            _ => Ok(()),
        }
    }

    /// Helper to extract BETWEEN range: col BETWEEN low AND high
    fn extract_between_range(expr: &SqlExpr) -> Option<(String, f64, f64)> {
        match expr {
            SqlExpr::Between {
                column,
                low,
                high,
                negated,
            } => {
                if *negated {
                    return None;
                }
                let col = column.trim_matches('"').to_string();
                let low_val = Self::extract_numeric_value(low).ok()?;
                let high_val = Self::extract_numeric_value(high).ok()?;
                Some((col, low_val, high_val))
            }
            _ => None,
        }
    }

    /// Extract a negated BETWEEN: `col NOT BETWEEN low AND high`.
    fn extract_not_between(expr: &SqlExpr) -> Option<(String, f64, f64)> {
        match expr {
            SqlExpr::Between {
                column,
                low,
                high,
                negated: true,
            } => {
                let col = column.trim_matches('"').to_string();
                let low_val = Self::extract_numeric_value(low).ok()?;
                let high_val = Self::extract_numeric_value(high).ok()?;
                Some((col, low_val, high_val))
            }
            _ => None,
        }
    }

    /// Extract a negated LIKE: `col NOT LIKE 'pattern'`.
    fn extract_not_like(expr: &SqlExpr) -> Option<(String, String)> {
        match expr {
            SqlExpr::Like {
                column,
                pattern,
                negated: true,
            } => Some((column.trim_matches('"').to_string(), pattern.clone())),
            _ => None,
        }
    }

    /// Convert a single-sided numeric comparison to an inclusive range for scan_numeric_range_mmap.
    /// col > N  → (col, next_f64(N), MAX)   exclusive lower bound via next representable f64
    /// col >= N → (col, N, MAX)
    /// col < N  → (col, MIN, prev_f64(N))   exclusive upper bound via prev representable f64
    /// col <= N → (col, MIN, N)
    fn extract_single_comparison_as_range(expr: &SqlExpr) -> Option<(String, f64, f64)> {
        use crate::query::sql_parser::BinaryOperator;
        match expr {
            SqlExpr::BinaryOp { left, op, right } => {
                // col OP literal  OR  literal OP col (reversed)
                let (col, effective_op, val) = match (left.as_ref(), right.as_ref()) {
                    (SqlExpr::Column(c), lit) => {
                        let v = Self::extract_numeric_value(lit).ok()?;
                        (c.trim_matches('"').to_string(), op.clone(), v)
                    }
                    (lit, SqlExpr::Column(c)) => {
                        let v = Self::extract_numeric_value(lit).ok()?;
                        // Flip: N > col → col < N
                        let flipped = match op {
                            BinaryOperator::Gt => BinaryOperator::Lt,
                            BinaryOperator::Ge => BinaryOperator::Le,
                            BinaryOperator::Lt => BinaryOperator::Gt,
                            BinaryOperator::Le => BinaryOperator::Ge,
                            _ => return None,
                        };
                        (c.trim_matches('"').to_string(), flipped, v)
                    }
                    _ => return None,
                };
                // Next/prev representable f64 for strict inequalities so the inclusive
                // range bounds stay exact. `next_down_f64(0.0)` is the negative side of
                // zero; computing `from_bits(0) + 1` instead yields the smallest
                // *positive* value, which silently made `k < 0` match `k = 0`.
                let (low, high) = match effective_op {
                    BinaryOperator::Gt => {
                        (
                            crate::query::query_signature::next_up_f64(val),
                            f64::INFINITY,
                        )
                    }
                    BinaryOperator::Ge => (val, f64::INFINITY),
                    BinaryOperator::Lt => {
                        (
                            f64::NEG_INFINITY,
                            crate::query::query_signature::next_down_f64(val),
                        )
                    }
                    BinaryOperator::Le => (f64::NEG_INFINITY, val),
                    BinaryOperator::Eq => (val, val), // exact match as degenerate range [N, N]
                    _ => return None,
                };
                Some((col, low, high))
            }
            _ => None,
        }
    }

    /// Extract a two-sided AND range on the SAME column: col >= N AND col <= M etc.
    /// Returns (col, inclusive_low, inclusive_high) with strict-inequality adjustment.
    /// Each side is extracted via extract_single_comparison_as_range; the intersection
    /// of the two (lo, hi) intervals gives the final range.
    fn extract_two_sided_same_col_range(expr: &SqlExpr) -> Option<(String, f64, f64)> {
        use crate::query::sql_parser::BinaryOperator;
        match expr {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let (col1, lo1, hi1) = Self::extract_single_comparison_as_range(left.as_ref())?;
                let (col2, lo2, hi2) = Self::extract_single_comparison_as_range(right.as_ref())?;
                if col1 != col2 {
                    return None;
                }
                let combined_low = lo1.max(lo2);
                let combined_high = hi1.min(hi2);
                if combined_low > combined_high {
                    return None;
                }
                Some((col1, combined_low, combined_high))
            }
            _ => None,
        }
    }

    /// Extract any single-column numeric range from an expression.
    /// Handles: BETWEEN, col op N (single comparison including equality).
    fn extract_any_numeric_range(expr: &SqlExpr) -> Option<(String, f64, f64)> {
        if let Some(r) = Self::extract_between_range(expr) {
            return Some(r);
        }
        if let Some(r) = Self::extract_single_comparison_as_range(expr) {
            return Some(r);
        }
        None
    }

    /// Flatten an AND tree of independent numeric comparisons/BETWEEN ranges.
    /// Each leaf remains a separate range so conjunctions over different
    /// feature columns can be fused by the storage scan.
    fn extract_numeric_conjunction(expr: &SqlExpr) -> Option<Vec<(String, f64, f64)>> {
        use crate::query::sql_parser::BinaryOperator;
        match expr {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let mut predicates = Self::extract_numeric_conjunction(left)?;
                predicates.extend(Self::extract_numeric_conjunction(right)?);
                (predicates.len() <= 8).then_some(predicates)
            }
            SqlExpr::Paren(inner) => Self::extract_numeric_conjunction(inner),
            _ => Self::extract_any_numeric_range(expr).map(|predicate| vec![predicate]),
        }
    }

    /// Merge-intersect two sorted index slices in O(n+m).
    fn intersect_sorted_indices(a: &[usize], b: &[usize]) -> Vec<usize> {
        let mut result = Vec::new();
        let (mut i, mut j) = (0, 0);
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Equal => {
                    result.push(a[i]);
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
            }
        }
        result
    }
}
