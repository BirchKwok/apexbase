//! Storage-facing scan protocol shared by query execution paths.
//!
//! SQL parsing stays above this module. A scan request describes only physical
//! columns and conjunctive predicates, so mmap, delta-aware, and future
//! parallel-morsel implementations can share one correctness contract.

use std::io;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, Int64Array, LargeStringArray, StringArray,
    UInt32Array,
};
use arrow::datatypes::{Field, UInt32Type};
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ScanBound {
    pub(crate) value: f64,
    pub(crate) inclusive: bool,
}

impl ScanBound {
    #[inline]
    pub(crate) const fn inclusive(value: f64) -> Self {
        Self {
            value,
            inclusive: true,
        }
    }

    #[inline]
    pub(crate) const fn exclusive(value: f64) -> Self {
        Self {
            value,
            inclusive: false,
        }
    }
}

/// A storage-level predicate. Multiple entries in `ScanRequest::predicates`
/// are combined with AND; OR remains an executor concern until the protocol
/// grows an explicit boolean expression tree.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ScanPredicate {
    NumericRange {
        column: String,
        lower: Option<ScanBound>,
        upper: Option<ScanBound>,
    },
    StringEq {
        column: String,
        value: String,
    },
}

impl ScanPredicate {
    #[inline]
    pub(crate) fn column(&self) -> &str {
        match self {
            Self::NumericRange { column, .. } | Self::StringEq { column, .. } => column,
        }
    }

    #[inline]
    pub(crate) fn inclusive_numeric_range(&self) -> Option<(&str, f64, f64)> {
        match self {
            Self::NumericRange {
                column,
                lower,
                upper,
            } => Some((
                column,
                lower.map_or(f64::NEG_INFINITY, |bound| bound.value),
                upper.map_or(f64::INFINITY, |bound| bound.value),
            )),
            Self::StringEq { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ScanRequest<'a> {
    pub(crate) projection: Option<&'a [&'a str]>,
    pub(crate) predicates: &'a [ScanPredicate],
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

    /// Resolve predicate columns once, then evaluate every row in a single
    /// conjunctive pass. `Ok(None)` means a type is outside this protocol slice
    /// and the caller must use the general SQL evaluator.
    pub(crate) fn select(mut self, predicates: &[ScanPredicate]) -> io::Result<Option<Self>> {
        if predicates.is_empty() || self.row_count == 0 {
            return Ok(Some(self));
        }

        let mut resolved = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            let clean = clean_column_name(predicate.column());
            let Some(column) = self
                .columns
                .iter()
                .find(|column| column.name().eq_ignore_ascii_case(clean))
            else {
                return Ok(None);
            };
            let Some(predicate) = ResolvedPredicate::new(predicate, column.values.as_ref()) else {
                return Ok(None);
            };
            resolved.push(predicate);
        }

        let mut indices = Vec::new();
        for row in 0..self.row_count {
            let mut matches = true;
            for predicate in &resolved {
                if !predicate.matches(row) {
                    matches = false;
                    break;
                }
            }
            if matches {
                indices.push(row as u32);
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

enum ResolvedPredicate<'a> {
    Int64 {
        array: &'a Int64Array,
        lower: Option<ScanBound>,
        upper: Option<ScanBound>,
    },
    Float64 {
        array: &'a Float64Array,
        lower: Option<ScanBound>,
        upper: Option<ScanBound>,
    },
    String {
        array: &'a StringArray,
        value: &'a str,
    },
    LargeString {
        array: &'a LargeStringArray,
        value: &'a str,
    },
    StringDictionary {
        array: &'a DictionaryArray<UInt32Type>,
        values: &'a StringArray,
        value: &'a str,
    },
}

impl<'a> ResolvedPredicate<'a> {
    fn new(predicate: &'a ScanPredicate, array: &'a dyn Array) -> Option<Self> {
        match predicate {
            ScanPredicate::NumericRange { lower, upper, .. } => {
                if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
                    Some(Self::Int64 {
                        array,
                        lower: *lower,
                        upper: *upper,
                    })
                } else {
                    array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .map(|array| Self::Float64 {
                            array,
                            lower: *lower,
                            upper: *upper,
                        })
                }
            }
            ScanPredicate::StringEq { value, .. } => {
                if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
                    Some(Self::String { array, value })
                } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
                    Some(Self::LargeString { array, value })
                } else {
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<UInt32Type>>()?;
                    let values = array.values();
                    Some(Self::StringDictionary {
                        array,
                        values: values.as_any().downcast_ref::<StringArray>()?,
                        value,
                    })
                }
            }
        }
    }

    #[inline(always)]
    fn matches(&self, row: usize) -> bool {
        match self {
            Self::Int64 {
                array,
                lower,
                upper,
            } => !array.is_null(row) && numeric_matches(array.value(row) as f64, *lower, *upper),
            Self::Float64 {
                array,
                lower,
                upper,
            } => !array.is_null(row) && numeric_matches(array.value(row), *lower, *upper),
            Self::String { array, value } => !array.is_null(row) && array.value(row) == *value,
            Self::LargeString { array, value } => !array.is_null(row) && array.value(row) == *value,
            Self::StringDictionary {
                array,
                values,
                value,
            } => {
                if array.is_null(row) {
                    return false;
                }
                let key = array.keys().value(row) as usize;
                key < values.len() && !values.is_null(key) && values.value(key) == *value
            }
        }
    }
}

#[inline(always)]
fn numeric_matches(value: f64, lower: Option<ScanBound>, upper: Option<ScanBound>) -> bool {
    let lower_matches = lower.is_none_or(|bound| {
        if bound.inclusive {
            value >= bound.value
        } else {
            value > bound.value
        }
    });
    lower_matches
        && upper.is_none_or(|bound| {
            if bound.inclusive {
                value <= bound.value
            } else {
                value < bound.value
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
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
        let predicates = vec![
            ScanPredicate::NumericRange {
                column: "age".to_string(),
                lower: Some(ScanBound::exclusive(20.0)),
                upper: Some(ScanBound::inclusive(35.0)),
            },
            ScanPredicate::NumericRange {
                column: "score".to_string(),
                lower: Some(ScanBound::inclusive(50.0)),
                upper: None,
            },
        ];
        let morsel = Morsel::from_batch(sample_batch(), 128)
            .select(&predicates)
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
        let predicate = ScanPredicate::StringEq {
            column: "city".to_string(),
            value: "A".to_string(),
        };
        let morsel = Morsel::from_batch(batch, 0)
            .select(&[predicate])
            .unwrap()
            .unwrap();
        assert_eq!(morsel.selection().len(), 2);
    }
}
