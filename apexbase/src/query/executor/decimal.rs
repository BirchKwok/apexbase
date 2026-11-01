// DECIMAL(p,s) query semantics: numeric literal handling, predicate kernels and
// ORDER BY sort keys.
//
// The storage engine keeps a DECIMAL value in its fixed 16-byte little-endian
// i128 payload and the public surface keeps that payload too (D4). A file that a
// write path re-materializes may lose the logical-type section, so a column's
// scale comes from the table catalog and is installed once per statement into a
// thread-local map (nested statements restore the outer map on drop). Only the
// predicate kernels and the ORDER BY sort-key rewrite ever consult it, so no
// per-row schema lookup happens on the read path.

use std::cmp::Ordering as CmpOrdering;

thread_local! {
    static DECIMAL_SCALES: std::cell::RefCell<Option<std::collections::HashMap<String, i8>>> =
        const { std::cell::RefCell::new(None) };
}

/// Restores the enclosing statement's DECIMAL scale map when the statement ends.
pub(in crate::query::executor) struct DecimalHintGuard {
    previous: Option<Option<std::collections::HashMap<String, i8>>>,
}

impl Drop for DecimalHintGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            DECIMAL_SCALES.with(|cell| *cell.borrow_mut() = previous);
        }
    }
}

impl DecimalHintGuard {
    /// A guard that does not touch the map, for statements that cannot produce a
    /// DECIMAL predicate or sort key.
    pub(in crate::query::executor) fn none() -> Self {
        Self { previous: None }
    }
}

/// Materialized catalog schema for `table_path`, if the catalog registers it.
///
/// The catalog (not the data file) is the authoritative source of logical types:
/// a re-materialized file may carry only the physical column type.
pub(in crate::query::executor) fn catalog_schema_snapshot(
    table_path: &Path,
) -> Option<crate::storage::OnDemandSchema> {
    let base_dir = table_path.parent()?;
    let name = table_path.file_stem()?.to_str()?;
    let bytes = crate::storage::table_catalog::schema_bytes(base_dir, name).ok()??;
    crate::storage::OnDemandSchema::from_bytes(&bytes).ok()
}

/// Install the table's DECIMAL column scales for the current statement.
pub(in crate::query::executor) fn install_decimal_hints(
    table_path: &Path,
) -> DecimalHintGuard {
    let Some(hints) = build_decimal_scale_map(table_path) else {
        return DecimalHintGuard::none();
    };
    let previous = DECIMAL_SCALES.with(|cell| cell.borrow_mut().replace(hints));
    DecimalHintGuard {
        previous: Some(previous),
    }
}

fn build_decimal_scale_map(
    table_path: &Path,
) -> Option<std::collections::HashMap<String, i8>> {
    // A CTE batch lives only in memory and has no catalog row.
    if crate::query::executor::has_cached_cte_batch(table_path) {
        return None;
    }
    let backend = crate::query::executor::get_cached_backend(table_path).ok()?;
    if !backend.storage.has_any_decimal_column() {
        return None;
    }
    let mut map = std::collections::HashMap::new();
    if let Some(schema) = catalog_schema_snapshot(table_path) {
        for (index, (name, _)) in schema.columns.iter().enumerate() {
            if let Some(crate::storage::LogicalType::Decimal { scale, .. }) =
                schema.logical_type(index)
            {
                map.insert(name.clone(), *scale);
                map.insert(name.to_lowercase(), *scale);
            }
        }
    }
    if map.is_empty() {
        // Memory tables (and files whose catalog row predates the logical-type
        // section) still carry the type on the open storage schema.
        let snapshot = backend.storage.schema_snapshot();
        for (index, (name, column_type)) in snapshot.columns.iter().enumerate() {
            if !matches!(column_type, crate::storage::ColumnType::Decimal128) {
                continue;
            }
            if let Some(crate::storage::LogicalType::Decimal { scale, .. }) =
                snapshot.logical_type(index)
            {
                map.insert(name.clone(), *scale);
                map.insert(name.to_lowercase(), *scale);
            }
        }
    }
    (!map.is_empty()).then_some(map)
}

/// The declared scale of a DECIMAL column referenced by the current statement.
#[inline]
pub(in crate::query::executor) fn decimal_scale_hint(column: &str) -> Option<i8> {
    DECIMAL_SCALES.with(|cell| {
        let borrow = cell.borrow();
        let hints = borrow.as_ref()?;
        let trimmed = column.trim_matches('"');
        if let Some(scale) = hints.get(trimmed) {
            return Some(*scale);
        }
        let bare = trimmed.rsplit('.').next().unwrap_or(trimmed);
        hints
            .get(bare)
            .or_else(|| hints.get(&bare.to_lowercase()))
            .copied()
    })
}

/// DECIMAL payload bytes (16-byte little-endian i128) of every row, with NULLs
/// preserved. Returns `None` for arrays that are not a DECIMAL-shaped column.
pub(in crate::query::executor) fn decimal_column_values(
    array: &ArrayRef,
) -> Option<Vec<Option<i128>>> {
    use arrow::array::{BinaryArray, Decimal128Array, FixedSizeBinaryArray};

    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        let mut out = Vec::with_capacity(arr.len());
        for index in 0..arr.len() {
            if arr.is_null(index) {
                out.push(None);
                continue;
            }
            out.push(decimal_payload_to_i128(arr.value(index)));
        }
        return Some(out);
    }
    if let Some(arr) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        if arr.value_length() != 16 {
            return None;
        }
        return Some(
            (0..arr.len())
                .map(|index| {
                    if arr.is_null(index) {
                        None
                    } else {
                        decimal_payload_to_i128(arr.value(index))
                    }
                })
                .collect(),
        );
    }
    if let Some(arr) = array.as_any().downcast_ref::<Decimal128Array>() {
        return Some(
            (0..arr.len())
                .map(|index| {
                    if arr.is_null(index) {
                        None
                    } else {
                        Some(arr.value(index))
                    }
                })
                .collect(),
        );
    }
    None
}

#[inline]
fn decimal_payload_to_i128(bytes: &[u8]) -> Option<i128> {
    if bytes.len() != 16 {
        return None;
    }
    let mut payload = [0u8; 16];
    payload.copy_from_slice(bytes);
    Some(i128::from_le_bytes(payload))
}

/// Exact decimal literal (`"1.23"`, `"-45.60"`, `"7"`) to `(unscaled, scale)`.
fn parse_decimal_text(text: &str) -> Option<(i128, i8)> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (negative, unsigned) = match trimmed.as_bytes()[0] {
        b'-' => (true, &trimmed[1..]),
        b'+' => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    if unsigned.is_empty() || unsigned.len() > 40 {
        return None;
    }
    let (integer_part, fractional_part) = match unsigned.split_once('.') {
        Some((integer, fractional)) => (integer, fractional),
        None => (unsigned, ""),
    };
    if integer_part.is_empty() && fractional_part.is_empty() {
        return None;
    }
    if !integer_part.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional_part.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    if fractional_part.len() > i8::MAX as usize {
        return None;
    }
    let mut unscaled: i128 = 0;
    for byte in integer_part.bytes().chain(fractional_part.bytes()) {
        unscaled = unscaled.checked_mul(10)?;
        unscaled = unscaled.checked_add((byte - b'0') as i128)?;
    }
    let scale = fractional_part.len() as i8;
    Some((if negative { -unscaled } else { unscaled }, scale))
}

/// `(unscaled, scale)` of a SQL literal compared against a DECIMAL column.
pub(in crate::query::executor) fn decimal_literal_parts(value: &Value) -> Option<(i128, i8)> {
    match value {
        Value::Int8(v) => Some((*v as i128, 0)),
        Value::Int16(v) => Some((*v as i128, 0)),
        Value::Int32(v) => Some((*v as i128, 0)),
        Value::Int64(v) => Some((*v as i128, 0)),
        Value::UInt8(v) => Some((*v as i128, 0)),
        Value::UInt16(v) => Some((*v as i128, 0)),
        Value::UInt32(v) => Some((*v as i128, 0)),
        Value::UInt64(v) => Some((*v as i128, 0)),
        // A float literal is written in its shortest round-trip decimal form, so
        // `d > 1.1` compares against decimal 1.1 rather than its binary image.
        Value::Float32(v) => decimal_literal_parts(&Value::Float64(*v as f64)),
        Value::Float64(v) => {
            if !v.is_finite() {
                return None;
            }
            parse_decimal_text(&format!("{}", v))
        }
        Value::String(text) => parse_decimal_text(text),
        _ => None,
    }
}

const POW10_U128: [u128; 39] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
    10_000_000_000_000_000_000,
    100_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000_000_000,
];

#[inline]
fn pow10_mul(value: u128, digits: usize) -> Option<u128> {
    // Zero stays zero at any exponent; only a non-zero magnitude can overflow.
    if value == 0 {
        return Some(0);
    }
    POW10_U128
        .get(digits)
        .and_then(|factor| value.checked_mul(*factor))
}

/// Compare two non-negative magnitudes expressed as `magnitude * 10^-scale`.
#[inline]
fn cmp_decimal_magnitudes(a: u128, a_scale: i8, b: u128, b_scale: i8) -> CmpOrdering {
    if a_scale == b_scale {
        return a.cmp(&b);
    }
    // Scale the magnitude that carries the larger exponent up so both share one
    // exponent. An overflow means the scaled magnitude exceeds `u128::MAX`,
    // which is strictly larger than any magnitude that did not overflow.
    if a_scale > b_scale {
        match pow10_mul(b, (a_scale - b_scale) as usize) {
            Some(scaled) => a.cmp(&scaled),
            None => CmpOrdering::Less,
        }
    } else {
        match pow10_mul(a, (b_scale - a_scale) as usize) {
            Some(scaled) => scaled.cmp(&b),
            None => CmpOrdering::Greater,
        }
    }
}

/// Exact comparison of two scaled i128 values.
pub(in crate::query::executor) fn cmp_decimal(
    a: i128,
    a_scale: i8,
    b: i128,
    b_scale: i8,
) -> CmpOrdering {
    let sign_a = a.signum();
    let sign_b = b.signum();
    if sign_a != sign_b {
        return sign_a.cmp(&sign_b);
    }
    if sign_a == 0 {
        return CmpOrdering::Equal;
    }
    let magnitude = cmp_decimal_magnitudes(a.unsigned_abs(), a_scale, b.unsigned_abs(), b_scale);
    if sign_a < 0 {
        magnitude.reverse()
    } else {
        magnitude
    }
}

#[inline]
pub(in crate::query::executor) fn cmp_decimal_matches(
    op: &BinaryOperator,
    ordering: CmpOrdering,
) -> bool {
    match op {
        BinaryOperator::Eq => ordering == CmpOrdering::Equal,
        BinaryOperator::NotEq => ordering != CmpOrdering::Equal,
        BinaryOperator::Lt => ordering == CmpOrdering::Less,
        BinaryOperator::Le => ordering != CmpOrdering::Greater,
        BinaryOperator::Gt => ordering == CmpOrdering::Greater,
        BinaryOperator::Ge => ordering != CmpOrdering::Less,
        _ => false,
    }
}

#[inline]
pub(in crate::query::executor) fn flip_comparison(
    op: &BinaryOperator,
) -> Option<BinaryOperator> {
    Some(match op {
        BinaryOperator::Eq => BinaryOperator::Eq,
        BinaryOperator::NotEq => BinaryOperator::NotEq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::Le => BinaryOperator::Ge,
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::Ge => BinaryOperator::Le,
        _ => return None,
    })
}

/// Build the boolean mask of `decimal_column OP literal` (and column vs column).
pub(in crate::query::executor) fn decimal_comparison_mask(
    batch: &RecordBatch,
    left: &SqlExpr,
    op: &BinaryOperator,
    right: &SqlExpr,
) -> io::Result<Option<BooleanArray>> {
    use arrow::array::BooleanBuilder;

    let (column_name, literal, reversed) = match (left, right) {
        (SqlExpr::Column(name), SqlExpr::Literal(value)) => {
            (name.clone(), Some(value), false)
        }
        (SqlExpr::Literal(value), SqlExpr::Column(name)) => {
            (name.clone(), Some(value), true)
        }
        (SqlExpr::Column(left_name), SqlExpr::Column(right_name)) => {
            return decimal_column_pair_mask(batch, left_name, op, right_name);
        }
        _ => return Ok(None),
    };
    let Some(scale) = decimal_scale_hint(&column_name) else {
        return Ok(None);
    };
    let clean = column_name.trim_matches('"');
    let clean = clean.rsplit('.').next().unwrap_or(clean);
    let Some(array) = batch.column_by_name(clean) else {
        return Ok(None);
    };
    let Some(values) = decimal_column_values(array) else {
        return Ok(None);
    };
    let effective_op = if reversed {
        match flip_comparison(op) {
            Some(flipped) => flipped,
            None => return Ok(None),
        }
    } else {
        op.clone()
    };
    let Some((literal_value, literal_scale)) = literal.and_then(decimal_literal_parts) else {
        return Err(err_input(format!(
            "DECIMAL column '{}' can only be compared with a numeric literal",
            clean
        )));
    };
    let mut builder = BooleanBuilder::with_capacity(values.len());
    for value in values {
        match value {
            None => builder.append_null(),
            Some(value) => builder.append_value(cmp_decimal_matches(
                &effective_op,
                cmp_decimal(value, scale, literal_value, literal_scale),
            )),
        }
    }
    Ok(Some(builder.finish()))
}

fn decimal_column_pair_mask(
    batch: &RecordBatch,
    left_name: &str,
    op: &BinaryOperator,
    right_name: &str,
) -> io::Result<Option<BooleanArray>> {
    use arrow::array::BooleanBuilder;

    let (Some(left_scale), Some(right_scale)) = (
        decimal_scale_hint(left_name),
        decimal_scale_hint(right_name),
    ) else {
        return Ok(None);
    };
    let clean = |name: &str| {
        let trimmed = name.trim_matches('"');
        trimmed.rsplit('.').next().unwrap_or(trimmed).to_string()
    };
    let (Some(left_array), Some(right_array)) = (
        batch.column_by_name(&clean(left_name)),
        batch.column_by_name(&clean(right_name)),
    ) else {
        return Ok(None);
    };
    let (Some(left_values), Some(right_values)) = (
        decimal_column_values(left_array),
        decimal_column_values(right_array),
    ) else {
        return Ok(None);
    };
    if left_values.len() != right_values.len() {
        return Ok(None);
    }
    let mut builder = BooleanBuilder::with_capacity(left_values.len());
    for (left_value, right_value) in left_values.into_iter().zip(right_values) {
        match (left_value, right_value) {
            (Some(left_value), Some(right_value)) => builder.append_value(cmp_decimal_matches(
                op,
                cmp_decimal(left_value, left_scale, right_value, right_scale),
            )),
            _ => builder.append_null(),
        }
    }
    Ok(Some(builder.finish()))
}

/// Build the boolean mask of `column BETWEEN low AND high`.
pub(in crate::query::executor) fn decimal_between_mask(
    batch: &RecordBatch,
    column: &str,
    low: &SqlExpr,
    high: &SqlExpr,
    negated: bool,
) -> io::Result<Option<BooleanArray>> {
    use arrow::array::BooleanBuilder;

    let Some(scale) = decimal_scale_hint(column) else {
        return Ok(None);
    };
    let clean = column.trim_matches('"');
    let clean = clean.rsplit('.').next().unwrap_or(clean);
    let Some(array) = batch.column_by_name(clean) else {
        return Ok(None);
    };
    let Some(values) = decimal_column_values(array) else {
        return Ok(None);
    };
    let (SqlExpr::Literal(low), SqlExpr::Literal(high)) = (low, high) else {
        return Ok(None);
    };
    let (Some((low_value, low_scale)), Some((high_value, high_scale))) = (
        decimal_literal_parts(low),
        decimal_literal_parts(high),
    ) else {
        return Err(err_input(format!(
            "DECIMAL column '{}' can only be compared with numeric literals",
            clean
        )));
    };
    let mut builder = BooleanBuilder::with_capacity(values.len());
    for value in values {
        match value {
            None => builder.append_null(),
            Some(value) => {
                let matches = cmp_decimal(value, scale, low_value, low_scale)
                    != CmpOrdering::Less
                    && cmp_decimal(value, scale, high_value, high_scale)
                        != CmpOrdering::Greater;
                builder.append_value(if negated { !matches } else { matches });
            }
        }
    }
    Ok(Some(builder.finish()))
}

/// Build the boolean mask of `column [NOT] IN (literals)`.
pub(in crate::query::executor) fn decimal_in_mask(
    batch: &RecordBatch,
    column: &str,
    values: &[Value],
    negated: bool,
) -> io::Result<Option<BooleanArray>> {
    use arrow::array::BooleanBuilder;

    let Some(scale) = decimal_scale_hint(column) else {
        return Ok(None);
    };
    let clean = column.trim_matches('"');
    let clean = clean.rsplit('.').next().unwrap_or(clean);
    let Some(array) = batch.column_by_name(clean) else {
        return Ok(None);
    };
    let Some(rows) = decimal_column_values(array) else {
        return Ok(None);
    };
    let mut literals: Vec<(i128, i8)> = Vec::with_capacity(values.len());
    let mut has_null = false;
    for value in values {
        if matches!(value, Value::Null) {
            has_null = true;
            continue;
        }
        match decimal_literal_parts(value) {
            Some(parts) => literals.push(parts),
            None => {
                return Err(err_input(format!(
                    "DECIMAL column '{}' IN list requires numeric literals",
                    clean
                )))
            }
        }
    }
    let mut builder = BooleanBuilder::with_capacity(rows.len());
    for row in rows {
        match row {
            None => builder.append_null(),
            Some(value) => {
                let matched = literals
                    .iter()
                    .any(|&(literal, literal_scale)| {
                        cmp_decimal(value, scale, literal, literal_scale) == CmpOrdering::Equal
                    });
                if matched {
                    builder.append_value(!negated);
                } else if has_null {
                    builder.append_null();
                } else {
                    builder.append_value(negated);
                }
            }
        }
    }
    Ok(Some(builder.finish()))
}

/// Rewrite the sort-key columns of an ORDER BY into `Decimal128` arrays so the
/// Arrow sort kernels order them numerically instead of byte-wise.
pub(in crate::query::executor) fn decimal_sort_batch(
    batch: &RecordBatch,
    order_by: &[crate::query::OrderByClause],
) -> io::Result<Option<RecordBatch>> {
    use arrow::array::Decimal128Array;

    if DECIMAL_SCALES.with(|cell| cell.borrow().is_none()) || order_by.is_empty() {
        return Ok(None);
    }
    let mut replacements: Vec<(usize, Field, ArrayRef)> = Vec::new();
    for clause in order_by {
        if clause.expr.is_some() {
            continue;
        }
        let name = ApexExecutor::resolve_order_col_name(batch, &clause.column);
        let Ok(index) = batch.schema().index_of(name) else {
            continue;
        };
        if replacements.iter().any(|(existing, _, _)| *existing == index) {
            continue;
        }
        let column = batch.column(index);
        if matches!(column.data_type(), ArrowDataType::Decimal128(_, _)) {
            continue;
        }
        let Some(scale) = decimal_scale_hint(name) else {
            continue;
        };
        let Some(values) = decimal_column_values(column) else {
            continue;
        };
        let array = Decimal128Array::from(values)
            .with_precision_and_scale(38, scale)
            .map_err(|error| err_data(error.to_string()))?;
        let field = Field::new(
            batch.schema().field(index).name(),
            ArrowDataType::Decimal128(38, scale),
            true,
        );
        replacements.push((index, field, Arc::new(array) as ArrayRef));
    }
    if replacements.is_empty() {
        return Ok(None);
    }
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| (**field).clone())
        .collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    for (index, field, array) in replacements {
        fields[index] = field;
        columns[index] = array;
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map(Some)
        .map_err(|error| err_data(error.to_string()))
}

/// Public results carry DECIMAL as the 16-byte payload (D4); the engine
/// materializes `Decimal128` arrays internally for numeric math, and this turns
/// them back into the wire representation on the way out.
pub(in crate::query::executor) fn decimal_result_to_payload(result: ApexResult) -> ApexResult {
    use arrow::array::BinaryArray;

    fn convert(batch: RecordBatch) -> RecordBatch {
        if !batch
            .schema()
            .fields()
            .iter()
            .any(|field| matches!(field.data_type(), ArrowDataType::Decimal128(_, _)))
        {
            return batch;
        }
        let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        for (index, field) in batch.schema().fields().iter().enumerate() {
            let column = batch.column(index);
            if let (ArrowDataType::Decimal128(_, _), Some(array)) = (
                field.data_type(),
                column.as_any().downcast_ref::<arrow::array::Decimal128Array>(),
            ) {
                let payloads: Vec<Option<[u8; 16]>> = (0..array.len())
                    .map(|row| {
                        if array.is_null(row) {
                            None
                        } else {
                            Some(array.value(row).to_le_bytes())
                        }
                    })
                    .collect();
                let payload_refs: Vec<Option<&[u8]>> = payloads
                    .iter()
                    .map(|value| value.as_ref().map(|bytes| bytes.as_slice()))
                    .collect();
                fields.push(Field::new(field.name(), ArrowDataType::Binary, true));
                columns.push(Arc::new(BinaryArray::from(payload_refs)) as ArrayRef);
            } else {
                fields.push((**field).clone());
                columns.push(column.clone());
            }
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap_or(batch)
    }

    match result {
        ApexResult::Data(batch) => ApexResult::Data(convert(batch)),
        ApexResult::Empty(schema) => {
            let fields: Vec<Field> = schema
                .fields()
                .iter()
                .map(|field| {
                    if matches!(field.data_type(), ArrowDataType::Decimal128(_, _)) {
                        Field::new(field.name(), ArrowDataType::Binary, true)
                    } else {
                        (**field).clone()
                    }
                })
                .collect();
            ApexResult::Empty(Arc::new(Schema::new(fields)))
        }
        other => other,
    }
}

#[cfg(test)]
mod decimal_query_tests {
    use super::*;
    use std::cmp::Ordering as CmpOrdering;

    fn cmp(a: i128, a_scale: i8, b: i128, b_scale: i8) -> CmpOrdering {
        cmp_decimal(a, a_scale, b, b_scale)
    }

    #[test]
    fn decimal_comparison_is_numeric_within_one_scale() {
        assert_eq!(cmp(123, 2, 120, 2), CmpOrdering::Greater);
        assert_eq!(cmp(-700, 2, 0, 2), CmpOrdering::Less);
        assert_eq!(cmp(-700, 2, -701, 2), CmpOrdering::Greater);
        assert_eq!(cmp(0, 2, 0, 2), CmpOrdering::Equal);
    }

    #[test]
    fn decimal_comparison_aligns_scales_exactly() {
        assert_eq!(cmp(123, 2, 1230, 3), CmpOrdering::Equal);
        assert_eq!(cmp(123, 2, 1231, 3), CmpOrdering::Less);
        assert_eq!(cmp(-123, 2, -1231, 3), CmpOrdering::Greater);
        assert_eq!(cmp(1, 0, 999, 2), CmpOrdering::Less);
        // Negative scales (unscaled * 10) still compare numerically.
        assert_eq!(cmp(i128::MAX, 0, 1, -1), CmpOrdering::Greater);
        assert_eq!(cmp(1, -1, i128::MAX, 0), CmpOrdering::Less);
        // A scale gap wide enough to overflow the alignment multiplier puts the
        // tiny value first without losing exactness.
        assert_eq!(cmp(1, 100, 2, 0), CmpOrdering::Less);
        assert_eq!(cmp(2, 0, 1, 100), CmpOrdering::Greater);
    }

    #[test]
    fn decimal_comparison_handles_zero_at_any_scale() {
        // Zero must stay zero when the alignment multiplier would overflow
        // u128: 0 * 10^76 == 0, which is less than 5.
        assert_eq!(cmp(0, 100, 5, 0, ), CmpOrdering::Less);
        assert_eq!(cmp(5, 0, 0, 100), CmpOrdering::Greater);
        assert_eq!(cmp(0, 76, 0, 0), CmpOrdering::Equal);
    }

    #[test]
    fn decimal_literal_parsing_keeps_exact_scale() {
        assert_eq!(parse_decimal_text("1.23"), Some((123, 2)));
        assert_eq!(parse_decimal_text("-45.60"), Some((-4560, 2)));
        assert_eq!(parse_decimal_text("+7"), Some((7, 0)));
        assert_eq!(parse_decimal_text(" .5 "), Some((5, 1)));
        assert_eq!(parse_decimal_text("1."), Some((1, 0)));
        assert_eq!(parse_decimal_text("abc"), None);
        assert_eq!(parse_decimal_text(""), None);
        assert_eq!(parse_decimal_text("1.2.3"), None);
        assert_eq!(parse_decimal_text("1e3"), None);
        assert_eq!(decimal_literal_parts(&Value::Float64(1.1)), Some((11, 1)));
        assert_eq!(decimal_literal_parts(&Value::Int64(-4)), Some((-4, 0)));
        assert_eq!(decimal_literal_parts(&Value::Bool(true)), None);
    }

    #[test]
    fn decimal_payload_round_trips_little_endian_i128() {
        assert_eq!(decimal_payload_to_i128(&123i128.to_le_bytes()), Some(123));
        assert_eq!(decimal_payload_to_i128(&(-700i128).to_le_bytes()), Some(-700));
        assert_eq!(decimal_payload_to_i128(&[0u8; 8]), None);
    }
}
