//! Storage-facing scan protocol shared by query execution paths.
//!
//! SQL parsing stays above this module. A scan request describes only physical
//! columns and typed Boolean predicates, so mmap, delta-aware, and future
//! parallel-morsel implementations can share one correctness contract.

use std::io;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, LargeStringArray,
    StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{Field, UInt32Type};
use arrow::record_batch::RecordBatch;

/// A typed scalar owned by the physical scan protocol. SQL AST values are
/// normalized into this smaller set before crossing the storage boundary, so
/// integer comparisons never have to round-trip through `f64`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ScanValue {
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bool(bool),
}

impl ScanValue {
    /// Convert to the legacy mmap numeric lane only when the conversion is
    /// exact. The selection evaluator still reapplies the typed predicate.
    #[inline]
    pub(crate) fn lossless_f64(&self) -> Option<f64> {
        const MAX_CONSECUTIVE_INTEGER: u64 = 1_u64 << 53;
        match self {
            Self::Int(value) if value.unsigned_abs() <= MAX_CONSECUTIVE_INTEGER => {
                Some(*value as f64)
            }
            Self::UInt(value) if *value <= MAX_CONSECUTIVE_INTEGER => Some(*value as f64),
            Self::Float(value) if value.is_finite() => Some(*value),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn lossless_i64(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            Self::UInt(value) => i64::try_from(*value).ok(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ScanBound {
    pub(crate) value: ScanValue,
    pub(crate) inclusive: bool,
}

impl ScanBound {
    #[inline]
    pub(crate) fn inclusive(value: ScanValue) -> Self {
        Self {
            value,
            inclusive: true,
        }
    }

    #[inline]
    pub(crate) fn exclusive(value: ScanValue) -> Self {
        Self {
            value,
            inclusive: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanComparison {
    Eq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
}

/// One SQL-independent physical predicate leaf.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ScanPredicate {
    Compare {
        column: String,
        op: ScanComparison,
        value: ScanValue,
    },
    Between {
        column: String,
        lower: Option<ScanBound>,
        upper: Option<ScanBound>,
    },
    In {
        column: String,
        values: Vec<ScanValue>,
    },
    IsNull {
        column: String,
        negated: bool,
    },
}

impl ScanPredicate {
    #[inline]
    pub(crate) fn column(&self) -> &str {
        match self {
            Self::Compare { column, .. }
            | Self::Between { column, .. }
            | Self::In { column, .. }
            | Self::IsNull { column, .. } => column,
        }
    }
}

/// Boolean structure shared by mmap, overlay-aware, and future parallel scan
/// lanes. Keeping this tree below SQL execution lets every downstream operator
/// benefit from the same exact selection semantics.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ScanPredicateExpr {
    Predicate(ScanPredicate),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl ScanPredicateExpr {
    pub(crate) fn visit_columns(&self, visitor: &mut impl FnMut(&str)) {
        match self {
            Self::Predicate(predicate) => visitor(predicate.column()),
            Self::And(left, right) | Self::Or(left, right) => {
                left.visit_columns(visitor);
                right.visit_columns(visitor);
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ScanRequest<'a> {
    pub(crate) projection: Option<&'a [&'a str]>,
    pub(crate) predicate: Option<&'a ScanPredicateExpr>,
}

/// A shared immutable physical column. Arrow's `ArrayRef` keeps ownership and
/// buffer lifetime independent from the mmap/backend lock that produced it.
#[derive(Clone)]
pub(crate) struct ColumnView {
    field: Arc<Field>,
    values: ArrayRef,
}

impl ColumnView {
    #[inline]
    pub(crate) fn name(&self) -> &str {
        self.field.name()
    }
}

/// Row positions that remain active inside one morsel.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SelectionVector {
    All(usize),
    Indices(Vec<u32>),
}

impl SelectionVector {
    #[cfg(test)]
    #[inline]
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::All(len) => *len,
            Self::Indices(indices) => indices.len(),
        }
    }
}

/// The unit handed from storage scanning to physical operators. The current
/// vertical slice emits one morsel; the type already carries a physical row
/// offset so the scheduler can split the same protocol later without changing
/// operator inputs.
pub(crate) struct Morsel {
    pub(crate) row_offset: usize,
    pub(crate) row_count: usize,
    columns: Vec<ColumnView>,
    selection: SelectionVector,
}

impl Morsel {
    pub(crate) fn from_batch(batch: RecordBatch, row_offset: usize) -> Self {
        let schema = batch.schema();
        let row_count = batch.num_rows();
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(index, values)| ColumnView {
                field: schema.fields()[index].clone(),
                values: values.clone(),
            })
            .collect();
        Self {
            row_offset,
            row_count,
            columns,
            selection: SelectionVector::All(row_count),
        }
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn selection(&self) -> &SelectionVector {
        &self.selection
    }

    /// Resolve predicate columns and scalar coercions once, then evaluate the
    /// predicate with row-local short circuiting. Pure conjunctions keep the
    /// flat per-leaf evaluation shape; trees with OR use the recursive boolean
    /// evaluator. `Ok(None)` means a type or coercion is outside this protocol
    /// slice and the caller must use the general SQL evaluator.
    pub(crate) fn select(
        mut self,
        predicate: Option<&ScanPredicateExpr>,
    ) -> io::Result<Option<Self>> {
        let Some(predicate) = predicate else {
            return Ok(Some(self));
        };
        if self.row_count == 0 {
            return Ok(Some(self));
        }

        let mut conjuncts = Vec::new();
        let mut leaves = None;
        if flatten_conjunctions(predicate, &mut conjuncts) {
            let mut resolved = Vec::with_capacity(conjuncts.len());
            for leaf in conjuncts.iter().copied() {
                let Some(leaf) = ResolvedPredicate::new(leaf, &self.columns) else {
                    return Ok(None);
                };
                resolved.push(leaf);
            }
            leaves = Some(resolved);
        }

        let mut indices = Vec::new();
        match leaves {
            Some(conjuncts) => {
                'rows: for row in 0..self.row_count {
                    for leaf in &conjuncts {
                        if !leaf.matches(row) {
                            continue 'rows;
                        }
                    }
                    indices.push(row as u32);
                }
            }
            None => {
                let Some(resolved) = ResolvedPredicateExpr::new(predicate, &self.columns) else {
                    return Ok(None);
                };
                for row in 0..self.row_count {
                    if resolved.matches(row) {
                        indices.push(row as u32);
                    }
                }
            }
        }
        self.selection = if indices.len() == self.row_count {
            SelectionVector::All(self.row_count)
        } else {
            SelectionVector::Indices(indices)
        };
        Ok(Some(self))
    }

    /// Materialize the current selection only at the operator boundary.
    pub(crate) fn into_record_batch(self) -> io::Result<RecordBatch> {
        use arrow::datatypes::Schema;

        debug_assert!(self.row_offset.checked_add(self.row_count).is_some());
        let fields = self
            .columns
            .iter()
            .map(|column| column.field.as_ref().clone())
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let arrays = match self.selection {
            SelectionVector::All(_) => self
                .columns
                .into_iter()
                .map(|column| column.values)
                .collect(),
            SelectionVector::Indices(indices) => {
                let indices = UInt32Array::from(indices);
                self.columns
                    .into_iter()
                    .map(|column| {
                        arrow::compute::take(column.values.as_ref(), &indices, None)
                            .map_err(|error| invalid_data(error.to_string()))
                    })
                    .collect::<io::Result<Vec<_>>>()?
            }
        };
        RecordBatch::try_new(schema, arrays).map_err(|error| invalid_data(error.to_string()))
    }
}

#[inline]
fn clean_column_name(column: &str) -> &str {
    column
        .trim_matches('"')
        .rsplit('.')
        .next()
        .unwrap_or(column)
        .trim_matches('"')
}

#[inline]
fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Clone, Copy)]
enum ResolvedColumn<'a> {
    Int64(&'a Int64Array),
    UInt64(&'a UInt64Array),
    Float64(&'a Float64Array),
    Bool(&'a BooleanArray),
    String(&'a StringArray),
    LargeString(&'a LargeStringArray),
    StringDictionary {
        array: &'a DictionaryArray<UInt32Type>,
        values: &'a StringArray,
    },
}

impl<'a> ResolvedColumn<'a> {
    fn new(array: &'a dyn Array) -> Option<Self> {
        if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
            Some(Self::Int64(array))
        } else if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
            Some(Self::UInt64(array))
        } else if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
            Some(Self::Float64(array))
        } else if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
            Some(Self::Bool(array))
        } else if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
            Some(Self::String(array))
        } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
            Some(Self::LargeString(array))
        } else {
            let array = array
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()?;
            let values = array.values();
            Some(Self::StringDictionary {
                array,
                values: values.as_any().downcast_ref::<StringArray>()?,
            })
        }
    }

    #[inline(always)]
    fn is_null(self, row: usize) -> bool {
        match self {
            Self::Int64(array) => array.is_null(row),
            Self::UInt64(array) => array.is_null(row),
            Self::Float64(array) => array.is_null(row),
            Self::Bool(array) => array.is_null(row),
            Self::String(array) => array.is_null(row),
            Self::LargeString(array) => array.is_null(row),
            Self::StringDictionary { array, .. } => array.is_null(row),
        }
    }

    #[inline(always)]
    fn str_value(self, row: usize) -> Option<&'a str> {
        match self {
            Self::String(array) => {
                if array.is_null(row) {
                    None
                } else {
                    Some(array.value(row))
                }
            }
            Self::LargeString(array) => {
                if array.is_null(row) {
                    None
                } else {
                    Some(array.value(row))
                }
            }
            Self::StringDictionary { array, values } => {
                if array.is_null(row) {
                    return None;
                }
                let key = array.keys().value(row) as usize;
                if key >= values.len() || values.is_null(key) {
                    return None;
                }
                Some(values.value(key))
            }
            _ => None,
        }
    }

    fn accepts_comparison(self, value: &ScanValue) -> bool {
        match self {
            Self::Int64(_) => comparison_i64(value).is_some(),
            Self::UInt64(_) => matches!(value, ScanValue::UInt(_))
                || matches!(value, ScanValue::Int(value) if *value >= 0),
            Self::Float64(_) => matches!(value, ScanValue::Int(_) | ScanValue::Float(_)),
            Self::Bool(_) => matches!(value, ScanValue::Bool(_)),
            Self::String(_) | Self::LargeString(_) | Self::StringDictionary { .. } => {
                matches!(value, ScanValue::String(_))
            }
        }
    }

    fn accepts_between(self, value: &ScanValue) -> bool {
        match self {
            Self::Int64(_) => matches!(value, ScanValue::Int(_) | ScanValue::Float(_))
                || matches!(value, ScanValue::UInt(value) if i64::try_from(*value).is_ok()),
            Self::UInt64(_) => matches!(value, ScanValue::UInt(_))
                || matches!(value, ScanValue::Int(value) if *value >= 0),
            Self::Float64(_) => matches!(value, ScanValue::Int(_) | ScanValue::Float(_)),
            Self::Bool(_) => matches!(value, ScanValue::Bool(_)),
            Self::String(_) | Self::LargeString(_) | Self::StringDictionary { .. } => {
                matches!(value, ScanValue::String(_))
            }
        }
    }

    fn accepts_in(self, value: &ScanValue) -> bool {
        match self {
            Self::Int64(_) => matches!(value, ScanValue::Int(_))
                || matches!(value, ScanValue::UInt(value) if i64::try_from(*value).is_ok()),
            Self::UInt64(_) => matches!(value, ScanValue::UInt(_)),
            Self::Float64(_) => matches!(value, ScanValue::Float(_)),
            Self::Bool(_) => matches!(value, ScanValue::Bool(_)),
            Self::String(_) | Self::LargeString(_) | Self::StringDictionary { .. } => {
                matches!(value, ScanValue::String(_))
            }
        }
    }

    #[inline(always)]
    fn compare(self, row: usize, value: &ScanValue) -> Option<std::cmp::Ordering> {
        if self.is_null(row) {
            return None;
        }
        match self {
            Self::Int64(array) => compare_i64(array.value(row), value),
            Self::UInt64(array) => compare_u64(array.value(row), value),
            Self::Float64(array) => compare_f64(array.value(row), value),
            Self::Bool(array) => match value {
                ScanValue::Bool(value) => array.value(row).partial_cmp(value),
                _ => None,
            },
            Self::String(array) => match value {
                ScanValue::String(value) => array.value(row).partial_cmp(value.as_str()),
                _ => None,
            },
            Self::LargeString(array) => match value {
                ScanValue::String(value) => array.value(row).partial_cmp(value.as_str()),
                _ => None,
            },
            Self::StringDictionary { array, values } => {
                let key = array.keys().value(row) as usize;
                if key >= values.len() || values.is_null(key) {
                    return None;
                }
                match value {
                    ScanValue::String(value) => values.value(key).partial_cmp(value.as_str()),
                    _ => None,
                }
            }
        }
    }

    #[inline(always)]
    fn compare_scalar(self, row: usize, value: &ScanValue) -> Option<std::cmp::Ordering> {
        if self.is_null(row) {
            return None;
        }
        match (self, value) {
            (Self::Int64(array), ScanValue::Float(_)) => {
                array.value(row).partial_cmp(&comparison_i64(value)?)
            }
            _ => self.compare(row, value),
        }
    }
}

/// Magnitude gate for integer-valued f64 targets: below 2^53 every nearby
/// integer round-trips exactly, so a range check on the column type is
/// bit-identical to the generic float comparison.
const EXACT_INTEGER_MAGNITUDE: f64 = 9_007_199_254_740_992.0;

/// Exact typed side for an Int64 column. `None` keeps the generic leaf.
#[inline]
fn int_side(value: &ScanValue, inclusive: bool) -> Option<(i64, bool)> {
    let value = match value {
        ScanValue::Int(value) => *value,
        ScanValue::UInt(value) => i64::try_from(*value).ok()?,
        ScanValue::Float(value)
            if value.is_finite()
                && value.fract() == 0.0
                && value.abs() < EXACT_INTEGER_MAGNITUDE =>
        {
            *value as i64
        }
        _ => return None,
    };
    Some((value, inclusive))
}

/// Exact typed side for a UInt64 column. `None` keeps the generic leaf.
#[inline]
fn uint_side(value: &ScanValue, inclusive: bool) -> Option<(u64, bool)> {
    let value = match value {
        ScanValue::UInt(value) => *value,
        ScanValue::Int(value) if *value >= 0 => *value as u64,
        _ => return None,
    };
    Some((value, inclusive))
}

/// Exact typed side for a Float64 column. `None` keeps the generic leaf.
#[inline]
fn float_side(value: &ScanValue, inclusive: bool) -> Option<(f64, bool)> {
    let value = match value {
        ScanValue::Int(value) => *value as f64,
        ScanValue::Float(value) if value.is_finite() => *value,
        _ => return None,
    };
    Some((value, inclusive))
}

fn bounds_from_side<T: Copy>(
    bound: T,
    op: ScanComparison,
) -> (Option<(T, bool)>, Option<(T, bool)>) {
    match op {
        ScanComparison::Eq => (Some((bound, true)), Some((bound, true))),
        ScanComparison::Gt => (Some((bound, false)), None),
        ScanComparison::Ge => (Some((bound, true)), None),
        ScanComparison::Lt => (None, Some((bound, false))),
        ScanComparison::Le => (None, Some((bound, true))),
        ScanComparison::NotEq => (None, None),
    }
}

#[inline]
fn int_compare_bounds(op: ScanComparison, value: &ScanValue) -> Option<(Option<(i64, bool)>, Option<(i64, bool)>)> {
    if op == ScanComparison::NotEq {
        return None;
    }
    Some(bounds_from_side(int_side(value, true)?.0, op))
}

#[inline]
fn uint_compare_bounds(op: ScanComparison, value: &ScanValue) -> Option<(Option<(u64, bool)>, Option<(u64, bool)>)> {
    if op == ScanComparison::NotEq {
        return None;
    }
    Some(bounds_from_side(uint_side(value, true)?.0, op))
}

#[inline]
fn float_compare_bounds(op: ScanComparison, value: &ScanValue) -> Option<(Option<(f64, bool)>, Option<(f64, bool)>)> {
    if op == ScanComparison::NotEq {
        return None;
    }
    Some(bounds_from_side(float_side(value, true)?.0, op))
}

/// Collect the leaves of a pure conjunction in SQL order. `false` marks the
/// first OR, where the recursive boolean evaluator must be used.
fn flatten_conjunctions<'a>(
    expr: &'a ScanPredicateExpr,
    leaves: &mut Vec<&'a ScanPredicate>,
) -> bool {
    match expr {
        ScanPredicateExpr::Predicate(predicate) => {
            leaves.push(predicate);
            true
        }
        ScanPredicateExpr::And(left, right) => {
            flatten_conjunctions(left, leaves) && flatten_conjunctions(right, leaves)
        }
        ScanPredicateExpr::Or(..) => false,
    }
}

#[inline]
fn comparison_i64(value: &ScanValue) -> Option<i64> {
    const I64_UPPER_EXCLUSIVE: f64 = 9_223_372_036_854_775_808.0;

    match value {
        ScanValue::Int(value) => Some(*value),
        ScanValue::UInt(value) => i64::try_from(*value).ok(),
        ScanValue::Float(value)
            if value.is_finite()
                && value.fract() == 0.0
                && *value >= i64::MIN as f64
                && *value < I64_UPPER_EXCLUSIVE =>
        {
            Some(*value as i64)
        }
        _ => None,
    }
}

enum ResolvedPredicate<'a> {
    IntRange {
        array: &'a Int64Array,
        lower: Option<(i64, bool)>,
        upper: Option<(i64, bool)>,
    },
    UIntRange {
        array: &'a UInt64Array,
        lower: Option<(u64, bool)>,
        upper: Option<(u64, bool)>,
    },
    FloatRange {
        array: &'a Float64Array,
        lower: Option<(f64, bool)>,
        upper: Option<(f64, bool)>,
    },
    StringEq {
        column: ResolvedColumn<'a>,
        value: &'a str,
    },
    DictEq {
        column: ResolvedColumn<'a>,
        key: u32,
    },
    DictIn {
        column: ResolvedColumn<'a>,
        keys: Vec<u32>,
    },
    Compare {
        column: ResolvedColumn<'a>,
        op: ScanComparison,
        value: &'a ScanValue,
    },
    Between {
        column: ResolvedColumn<'a>,
        lower: Option<&'a ScanBound>,
        upper: Option<&'a ScanBound>,
    },
    In {
        column: ResolvedColumn<'a>,
        values: &'a [ScanValue],
    },
    IsNull {
        column: ResolvedColumn<'a>,
        negated: bool,
    },
}

impl<'a> ResolvedPredicate<'a> {
    fn new(predicate: &'a ScanPredicate, columns: &'a [ColumnView]) -> Option<Self> {
        let clean = clean_column_name(predicate.column());
        let column = columns
            .iter()
            .find(|column| column.name().eq_ignore_ascii_case(clean))?;
        let column = ResolvedColumn::new(column.values.as_ref())?;
        match predicate {
            ScanPredicate::Compare { op, value, .. } => {
                let specialized = match column {
                    ResolvedColumn::Int64(array) => int_compare_bounds(*op, value)
                        .map(|(lower, upper)| Self::IntRange {
                            array,
                            lower,
                            upper,
                        }),
                    ResolvedColumn::UInt64(array) => uint_compare_bounds(*op, value)
                        .map(|(lower, upper)| Self::UIntRange {
                            array,
                            lower,
                            upper,
                        }),
                    ResolvedColumn::Float64(array) => float_compare_bounds(*op, value)
                        .map(|(lower, upper)| Self::FloatRange {
                            array,
                            lower,
                            upper,
                        }),
                    ResolvedColumn::StringDictionary { .. }
                        if *op == ScanComparison::Eq => match value {
                            ScanValue::String(text) => {
                                Some(Self::dict_eq_or_string_eq(column, text))
                            }
                            _ => None,
                        },
                    ResolvedColumn::String(_)
                    | ResolvedColumn::LargeString(_)
                        if *op == ScanComparison::Eq => match value {
                            ScanValue::String(text) => Some(Self::StringEq {
                                column,
                                value: text.as_str(),
                            }),
                            _ => None,
                        },
                    _ => None,
                };
                specialized.or_else(|| {
                    column
                        .accepts_comparison(value)
                        .then_some(Self::Compare {
                            column,
                            op: *op,
                            value,
                        })
                })
            }
            ScanPredicate::Between { lower, upper, .. } => match column {
                ResolvedColumn::Int64(array) => {
                    let lower_side = match lower.as_ref() {
                        None => Ok(None),
                        Some(bound) => int_side(&bound.value, bound.inclusive).map(Some).ok_or(()),
                    };
                    let upper_side = match upper.as_ref() {
                        None => Ok(None),
                        Some(bound) => int_side(&bound.value, bound.inclusive).map(Some).ok_or(()),
                    };
                    match (lower_side, upper_side) {
                        (Ok(lower), Ok(upper)) => {
                            Some(Self::IntRange { array, lower, upper })
                        }
                        _ => Self::between(column, lower, upper),
                    }
                }
                ResolvedColumn::UInt64(array) => {
                    let lower_side = match lower.as_ref() {
                        None => Ok(None),
                        Some(bound) => uint_side(&bound.value, bound.inclusive).map(Some).ok_or(()),
                    };
                    let upper_side = match upper.as_ref() {
                        None => Ok(None),
                        Some(bound) => uint_side(&bound.value, bound.inclusive).map(Some).ok_or(()),
                    };
                    match (lower_side, upper_side) {
                        (Ok(lower), Ok(upper)) => {
                            Some(Self::UIntRange { array, lower, upper })
                        }
                        _ => Self::between(column, lower, upper),
                    }
                }
                ResolvedColumn::Float64(array) => {
                    let lower_side = match lower.as_ref() {
                        None => Ok(None),
                        Some(bound) => float_side(&bound.value, bound.inclusive).map(Some).ok_or(()),
                    };
                    let upper_side = match upper.as_ref() {
                        None => Ok(None),
                        Some(bound) => float_side(&bound.value, bound.inclusive).map(Some).ok_or(()),
                    };
                    match (lower_side, upper_side) {
                        (Ok(lower), Ok(upper)) => {
                            Some(Self::FloatRange { array, lower, upper })
                        }
                        _ => Self::between(column, lower, upper),
                    }
                }
                _ => Self::between(column, lower, upper),
            }
            ScanPredicate::In { values, .. } => {
                let dict_keys = match column {
                    ResolvedColumn::StringDictionary {
                        values: dict_values, ..
                    } if values
                        .iter()
                        .all(|value| matches!(value, ScanValue::String(_))) =>
                    {
                        let mut keys: Vec<u32> = Vec::new();
                        for value in values {
                            let ScanValue::String(text) = value else {
                                unreachable!()
                            };
                            for key in 0..dict_values.len() {
                                if !dict_values.is_null(key)
                                    && dict_values.value(key) == text.as_str()
                                {
                                    keys.push(key as u32);
                                    break;
                                }
                            }
                        }
                        keys.sort_unstable();
                        keys.dedup();
                        Some(keys)
                    }
                    _ => None,
                };
                if let Some(keys) = dict_keys {
                    Some(Self::DictIn { column, keys })
                } else {
                    values
                        .iter()
                        .all(|value| column.accepts_in(value))
                        .then_some(Self::In { column, values })
                }
            }
            ScanPredicate::IsNull { negated, .. } => Some(Self::IsNull {
                column,
                negated: *negated,
            }),
        }
    }

    /// Dictionary-encoded equality resolves to a single key comparison when
    /// the text exists in the dictionary; unknown texts keep the string
    /// comparison leaf (which then matches no row).
    fn dict_eq_or_string_eq(column: ResolvedColumn<'a>, text: &'a str) -> Self {
        let ResolvedColumn::StringDictionary { values, .. } = column else {
            return Self::StringEq { column, value: text };
        };
        for key in 0..values.len() {
            if !values.is_null(key) && values.value(key) == text {
                return Self::DictEq { column, key: key as u32 };
            }
        }
        Self::StringEq { column, value: text }
    }

    fn between(
        column: ResolvedColumn<'a>,
        lower: &'a Option<ScanBound>,
        upper: &'a Option<ScanBound>,
    ) -> Option<Self> {
        let compatible = lower
            .as_ref()
            .is_none_or(|bound| column.accepts_between(&bound.value))
            && upper
                .as_ref()
                .is_none_or(|bound| column.accepts_between(&bound.value));
        compatible.then_some(Self::Between {
            column,
            lower: lower.as_ref(),
            upper: upper.as_ref(),
        })
    }

    #[inline(always)]
    fn matches(&self, row: usize) -> bool {
        match self {
            Self::IntRange { array, lower, upper } => {
                if array.is_null(row) {
                    return false;
                }
                let value = array.value(row);
                lower.is_none_or(|(bound, inclusive)| {
                    if inclusive {
                        value >= bound
                    } else {
                        value > bound
                    }
                }) && upper.is_none_or(|(bound, inclusive)| {
                    if inclusive {
                        value <= bound
                    } else {
                        value < bound
                    }
                })
            }
            Self::UIntRange { array, lower, upper } => {
                if array.is_null(row) {
                    return false;
                }
                let value = array.value(row);
                lower.is_none_or(|(bound, inclusive)| {
                    if inclusive {
                        value >= bound
                    } else {
                        value > bound
                    }
                }) && upper.is_none_or(|(bound, inclusive)| {
                    if inclusive {
                        value <= bound
                    } else {
                        value < bound
                    }
                })
            }
            Self::FloatRange { array, lower, upper } => {
                if array.is_null(row) {
                    return false;
                }
                let value = array.value(row);
                lower.is_none_or(|(bound, inclusive)| {
                    if inclusive {
                        value >= bound
                    } else {
                        value > bound
                    }
                }) && upper.is_none_or(|(bound, inclusive)| {
                    if inclusive {
                        value <= bound
                    } else {
                        value < bound
                    }
                })
            }
            Self::StringEq { column, value } => column.str_value(row).is_some_and(|s| s == *value),
            Self::DictEq { column, key } => {
                let ResolvedColumn::StringDictionary { array, .. } = column else {
                    unreachable!()
                };
                !array.is_null(row) && array.keys().value(row) == *key
            }
            Self::DictIn { column, keys } => {
                let ResolvedColumn::StringDictionary { array, .. } = column else {
                    unreachable!()
                };
                !array.is_null(row) && keys.binary_search(&array.keys().value(row)).is_ok()
            }
            Self::Compare { column, op, value } => column
                .compare_scalar(row, value)
                .is_some_and(|ordering| comparison_matches(ordering, *op)),
            Self::Between {
                column,
                lower,
                upper,
            } => {
                if column.is_null(row) {
                    return false;
                }
                let lower_matches = lower.is_none_or(|bound| {
                    column.compare(row, &bound.value).is_some_and(|ordering| {
                        ordering == std::cmp::Ordering::Greater
                            || (bound.inclusive && ordering == std::cmp::Ordering::Equal)
                    })
                });
                lower_matches
                    && upper.is_none_or(|bound| {
                        column.compare(row, &bound.value).is_some_and(|ordering| {
                            ordering == std::cmp::Ordering::Less
                                || (bound.inclusive && ordering == std::cmp::Ordering::Equal)
                        })
                    })
            }
            Self::In { column, values } => {
                !column.is_null(row)
                    && values.iter().any(|value| {
                        column.compare(row, value) == Some(std::cmp::Ordering::Equal)
                    })
            }
            Self::IsNull { column, negated } => column.is_null(row) != *negated,
        }
    }
}

enum ResolvedPredicateExpr<'a> {
    Predicate(ResolvedPredicate<'a>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl<'a> ResolvedPredicateExpr<'a> {
    fn new(predicate: &'a ScanPredicateExpr, columns: &'a [ColumnView]) -> Option<Self> {
        match predicate {
            ScanPredicateExpr::Predicate(predicate) => {
                ResolvedPredicate::new(predicate, columns).map(Self::Predicate)
            }
            ScanPredicateExpr::And(left, right) => Some(Self::And(
                Box::new(Self::new(left, columns)?),
                Box::new(Self::new(right, columns)?),
            )),
            ScanPredicateExpr::Or(left, right) => Some(Self::Or(
                Box::new(Self::new(left, columns)?),
                Box::new(Self::new(right, columns)?),
            )),
        }
    }

    #[inline(always)]
    fn matches(&self, row: usize) -> bool {
        match self {
            Self::Predicate(predicate) => predicate.matches(row),
            Self::And(left, right) => left.matches(row) && right.matches(row),
            Self::Or(left, right) => left.matches(row) || right.matches(row),
        }
    }
}

#[inline(always)]
fn comparison_matches(ordering: std::cmp::Ordering, op: ScanComparison) -> bool {
    match op {
        ScanComparison::Eq => ordering == std::cmp::Ordering::Equal,
        ScanComparison::NotEq => ordering != std::cmp::Ordering::Equal,
        ScanComparison::Lt => ordering == std::cmp::Ordering::Less,
        ScanComparison::Le => ordering != std::cmp::Ordering::Greater,
        ScanComparison::Gt => ordering == std::cmp::Ordering::Greater,
        ScanComparison::Ge => ordering != std::cmp::Ordering::Less,
    }
}

#[inline(always)]
fn compare_i64(value: i64, target: &ScanValue) -> Option<std::cmp::Ordering> {
    match target {
        ScanValue::Int(target) => value.partial_cmp(target),
        ScanValue::UInt(target) => match i64::try_from(*target) {
            Ok(target) => value.partial_cmp(&target),
            Err(_) => Some(std::cmp::Ordering::Less),
        },
        ScanValue::Float(target) => (value as f64).partial_cmp(target),
        _ => None,
    }
}

#[inline(always)]
fn compare_u64(value: u64, target: &ScanValue) -> Option<std::cmp::Ordering> {
    match target {
        ScanValue::Int(target) if *target < 0 => Some(std::cmp::Ordering::Greater),
        ScanValue::Int(target) => value.partial_cmp(&(*target as u64)),
        ScanValue::UInt(target) => value.partial_cmp(target),
        ScanValue::Float(target) => (value as f64).partial_cmp(target),
        _ => None,
    }
}

#[inline(always)]
fn compare_f64(value: f64, target: &ScanValue) -> Option<std::cmp::Ordering> {
    match target {
        ScanValue::Int(target) => value.partial_cmp(&(*target as f64)),
        ScanValue::UInt(target) => value.partial_cmp(&(*target as f64)),
        ScanValue::Float(target) => value.partial_cmp(target),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Schema};

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("A"),
                    Some("B"),
                    None,
                    Some("A"),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(20),
                    Some(30),
                    Some(35),
                    Some(40),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0),
                    Some(50.0),
                    Some(70.0),
                    None,
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn selection_vector_preserves_strict_bounds_and_null_semantics() {
        let predicate = ScanPredicateExpr::And(
            Box::new(ScanPredicateExpr::Predicate(ScanPredicate::Between {
                column: "age".to_string(),
                lower: Some(ScanBound::exclusive(ScanValue::Int(20))),
                upper: Some(ScanBound::inclusive(ScanValue::Int(35))),
            })),
            Box::new(ScanPredicateExpr::Predicate(ScanPredicate::Between {
                column: "score".to_string(),
                lower: Some(ScanBound::inclusive(ScanValue::Float(50.0))),
                upper: None,
            })),
        );
        let morsel = Morsel::from_batch(sample_batch(), 128)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.row_offset, 128);
        assert_eq!(morsel.row_count, 4);
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![1, 2]));
        let selected = morsel.into_record_batch().unwrap();
        assert_eq!(selected.num_rows(), 2);
    }

    #[test]
    fn string_dictionary_column_view_is_supported() {
        let values = Arc::new(StringArray::from(vec!["A", "B"]));
        let keys = UInt32Array::from(vec![0, 1, 0]);
        let dictionary = DictionaryArray::<UInt32Type>::try_new(keys, values).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "city",
                dictionary.data_type().clone(),
                false,
            )])),
            vec![Arc::new(dictionary)],
        )
        .unwrap();
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "city".to_string(),
            op: ScanComparison::Eq,
            value: ScanValue::String("A".to_string()),
        });
        let morsel = Morsel::from_batch(batch, 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection().len(), 2);
    }

    #[test]
    fn boolean_tree_composes_or_in_and_is_not_null() {
        let predicate = ScanPredicateExpr::And(
            Box::new(ScanPredicateExpr::Or(
                Box::new(ScanPredicateExpr::Predicate(ScanPredicate::In {
                    column: "city".to_string(),
                    values: vec![ScanValue::String("A".to_string())],
                })),
                Box::new(ScanPredicateExpr::Predicate(ScanPredicate::Compare {
                    column: "age".to_string(),
                    op: ScanComparison::Eq,
                    value: ScanValue::Int(30),
                })),
            )),
            Box::new(ScanPredicateExpr::Predicate(ScanPredicate::IsNull {
                column: "score".to_string(),
                negated: true,
            })),
        );
        let morsel = Morsel::from_batch(sample_batch(), 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![0, 1]));
    }

    #[test]
    fn int64_comparison_does_not_round_through_f64() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "wide",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![
                9_007_199_254_740_992,
                9_007_199_254_740_993,
            ]))],
        )
        .unwrap();
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "wide".to_string(),
            op: ScanComparison::Gt,
            value: ScanValue::Int(9_007_199_254_740_992),
        });
        let morsel = Morsel::from_batch(batch, 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![1]));
    }

    #[test]
    fn legacy_mmap_scalar_conversions_are_conservative() {
        let exact = 1_i64 << 53;
        assert_eq!(ScanValue::Int(exact).lossless_f64(), Some(exact as f64));
        assert_eq!(ScanValue::Int(exact + 1).lossless_f64(), None);
        assert_eq!(ScanValue::UInt((1_u64 << 53) + 1).lossless_f64(), None);
        assert_eq!(ScanValue::Float(1.0).lossless_i64(), None);
        assert_eq!(ScanValue::Int(i64::MAX).lossless_i64(), Some(i64::MAX));
    }



    #[test]
    fn flat_conjunction_uses_specialized_range_leaves() {
        let predicate = ScanPredicateExpr::And(
            Box::new(ScanPredicateExpr::Predicate(ScanPredicate::Compare {
                column: "age".to_string(),
                op: ScanComparison::Gt,
                value: ScanValue::Int(20),
            })),
            Box::new(ScanPredicateExpr::And(
                Box::new(ScanPredicateExpr::Predicate(ScanPredicate::Compare {
                    column: "age".to_string(),
                    op: ScanComparison::Le,
                    value: ScanValue::Int(35),
                })),
                Box::new(ScanPredicateExpr::Predicate(ScanPredicate::Compare {
                    column: "score".to_string(),
                    op: ScanComparison::Ge,
                    value: ScanValue::Float(50.0),
                })),
            )),
        );
        let morsel = Morsel::from_batch(sample_batch(), 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![1, 2]));
    }

    #[test]
    fn not_equality_falls_back_to_generic_leaf() {
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "age".to_string(),
            op: ScanComparison::NotEq,
            value: ScanValue::Int(35),
        });
        let morsel = Morsel::from_batch(sample_batch(), 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![0, 1, 3]));
    }

    #[test]
    fn string_equality_uses_direct_leaf() {
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "city".to_string(),
            op: ScanComparison::Eq,
            value: ScanValue::String("A".to_string()),
        });
        let morsel = Morsel::from_batch(sample_batch(), 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![0, 3]));
    }

    #[test]
    fn integer_between_uses_range_leaf() {
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Between {
            column: "age".to_string(),
            lower: Some(ScanBound::inclusive(ScanValue::Int(25))),
            upper: Some(ScanBound::exclusive(ScanValue::Int(40))),
        });
        let morsel = Morsel::from_batch(sample_batch(), 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![1, 2]));
    }

    #[test]
    fn unsigned_range_bounds_are_exact() {
        let schema = Arc::new(Schema::new(vec![Field::new("hits", DataType::UInt64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt64Array::from(vec![Some(1), Some(9), None, Some(10)]))],
        )
        .unwrap();
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "hits".to_string(),
            op: ScanComparison::Ge,
            value: ScanValue::UInt(9),
        });
        let morsel = Morsel::from_batch(batch, 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![1, 3]));
    }

    #[test]
    fn or_tree_still_uses_recursive_evaluator() {
        let predicate = ScanPredicateExpr::Or(
            Box::new(ScanPredicateExpr::Predicate(ScanPredicate::Compare {
                column: "age".to_string(),
                op: ScanComparison::Eq,
                value: ScanValue::Int(20),
            })),
            Box::new(ScanPredicateExpr::Predicate(ScanPredicate::Compare {
                column: "score".to_string(),
                op: ScanComparison::Gt,
                value: ScanValue::Float(60.0),
            })),
        );
        let morsel = Morsel::from_batch(sample_batch(), 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![0, 2]));
    }

    fn dict_city_batch() -> RecordBatch {
        let values = Arc::new(StringArray::from(vec!["A", "B", "C"]));
        let keys = UInt32Array::from(vec![0, 1, 2, 0]);
        let dictionary = DictionaryArray::<UInt32Type>::try_new(keys, values).unwrap();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "city",
                    dictionary.data_type().clone(),
                    true,
                ),
                Field::new("age", DataType::Int64, true),
            ])),
            vec![
                Arc::new(dictionary),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn dictionary_in_resolves_keys_once_and_matches_generic_semantics() {
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::In {
            column: "city".to_string(),
            values: vec![
                ScanValue::String("A".to_string()),
                ScanValue::String("C".to_string()),
                ScanValue::String("A".to_string()),
            ],
        });
        let morsel = Morsel::from_batch(dict_city_batch(), 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![0, 2, 3]));

        // Unknown dictionary values match no row, same as the generic leaf.
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::In {
            column: "city".to_string(),
            values: vec![ScanValue::String("Z".to_string())],
        });
        let morsel = Morsel::from_batch(dict_city_batch(), 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection().len(), 0);
    }

    #[test]
    fn dictionary_eq_uses_key_comparison() {
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "city".to_string(),
            op: ScanComparison::Eq,
            value: ScanValue::String("B".to_string()),
        });
        let morsel = Morsel::from_batch(dict_city_batch(), 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![1]));

        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "city".to_string(),
            op: ScanComparison::Eq,
            value: ScanValue::String("Z".to_string()),
        });
        let morsel = Morsel::from_batch(dict_city_batch(), 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection().len(), 0);
    }

    #[test]
    fn dictionary_in_skips_null_rows_like_generic_leaf() {
        let values = Arc::new(StringArray::from(vec!["A", "B"]));
        let keys = UInt32Array::new(
            arrow::buffer::ScalarBuffer::from(vec![0u32, 1, 0]),
            Some(arrow::buffer::NullBuffer::from(vec![true, true, false])),
        );
        let dictionary =
            DictionaryArray::<UInt32Type>::try_new(keys, values).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "city",
                dictionary.data_type().clone(),
                true,
            )])),
            vec![Arc::new(dictionary)],
        )
        .unwrap();
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::In {
            column: "city".to_string(),
            values: vec![ScanValue::String("A".to_string())],
        });
        let morsel = Morsel::from_batch(batch, 0)
            .select(Some(&predicate))
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection(), &SelectionVector::Indices(vec![0]));
    }
}
