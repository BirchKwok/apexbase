// Time-family (timestamp/timestamptz/date/time/interval) aggregation helpers.
//
// The family shares one 8-byte payload. Reads present it as typed Arrow arrays
// (D5), so value aggregates over it must (a) emit a typed result from MIN/MAX
// and (b) refuse SUM/AVG instead of silently adding raw payload integers.

use arrow::array::{
    Date32Array, DurationMicrosecondArray, PrimitiveArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::TimeUnit;

/// `Some((field, array))` for a single-value MIN/MAX output over a time-family
/// input array; `None` when the array is not a time-family array.
pub(in crate::query::executor) fn time_family_min_max(
    array: &ArrayRef,
    output_name: &str,
    is_min: bool,
) -> Option<(Field, ArrayRef)> {
    fn pick<I>(iter: I, is_min: bool) -> Option<i64>
    where
        I: Iterator<Item = Option<i64>>,
    {
        let mut best: Option<i64> = None;
        for value in iter.flatten() {
            best = Some(match best {
                None => value,
                Some(current) => {
                    if is_min {
                        current.min(value)
                    } else {
                        current.max(value)
                    }
                }
            });
        }
        best
    }

    match array.data_type() {
        ArrowDataType::Timestamp(unit, timezone) => {
            let value = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                    .and_then(|arr| pick((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))), is_min)),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .and_then(|arr| pick((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))), is_min)),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .and_then(|arr| pick((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))), is_min)),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .and_then(|arr| pick((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))), is_min)),
            };
            let typed = PrimitiveArray::<arrow::datatypes::TimestampMicrosecondType>::from(vec![value]);
            let typed = if timezone.is_some() {
                typed.with_timezone(timezone.as_deref().unwrap_or("+00:00"))
            } else {
                typed
            };
            Some((
                Field::new(
                    output_name,
                    ArrowDataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
                    true,
                ),
                Arc::new(typed) as ArrayRef,
            ))
        }
        ArrowDataType::Date32 => {
            let value = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .and_then(|arr| {
                    pick(
                        (0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64)),
                        is_min,
                    )
                })
                .map(|value| value as i32);
            Some((
                Field::new(output_name, ArrowDataType::Date32, true),
                Arc::new(Date32Array::from(vec![value])) as ArrayRef,
            ))
        }
        ArrowDataType::Time64(unit) => {
            let value = match unit {
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .and_then(|arr| pick((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))), is_min)),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<arrow::array::Time64NanosecondArray>()
                    .and_then(|arr| pick((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))), is_min)),
                _ => None,
            };
            let typed = PrimitiveArray::<arrow::datatypes::Time64MicrosecondType>::from(vec![value]);
            Some((
                Field::new(output_name, ArrowDataType::Time64(*unit), true),
                Arc::new(typed) as ArrayRef,
            ))
        }
        ArrowDataType::Duration(unit) => {
            let value = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<arrow::array::DurationSecondArray>()
                    .and_then(|arr| pick((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))), is_min)),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<arrow::array::DurationMillisecondArray>()
                    .and_then(|arr| pick((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))), is_min)),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<DurationMicrosecondArray>()
                    .and_then(|arr| pick((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))), is_min)),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<arrow::array::DurationNanosecondArray>()
                    .and_then(|arr| pick((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))), is_min)),
            };
            let typed = PrimitiveArray::<arrow::datatypes::DurationMicrosecondType>::from(vec![value]);
            Some((
                Field::new(output_name, ArrowDataType::Duration(*unit), true),
                Arc::new(typed) as ArrayRef,
            ))
        }
        _ => None,
    }
}

/// Typed MIN/MAX output from a storage `DataType` and the raw payload value.
pub(in crate::query::executor) fn time_family_value_output(
    dtype: crate::data::DataType,
    value: Option<i64>,
    output_name: &str,
) -> Option<(Field, ArrayRef)> {
    match dtype {
        crate::data::DataType::Timestamp => Some((
            Field::new(
                output_name,
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Arc::new(TimestampMicrosecondArray::from(vec![value])) as ArrayRef,
        )),
        crate::data::DataType::TimestampTz => {
            let array = TimestampMicrosecondArray::from(vec![value]).with_timezone("+00:00");
            Some((
                Field::new(
                    output_name,
                    ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                    true,
                ),
                Arc::new(array) as ArrayRef,
            ))
        }
        crate::data::DataType::Date => Some((
            Field::new(output_name, ArrowDataType::Date32, true),
            Arc::new(Date32Array::from(vec![value.map(|value| value as i32)])) as ArrayRef,
        )),
        crate::data::DataType::Time => Some((
            Field::new(output_name, ArrowDataType::Time64(TimeUnit::Microsecond), true),
            Arc::new(Time64MicrosecondArray::from(vec![value])) as ArrayRef,
        )),
        crate::data::DataType::Interval => Some((
            Field::new(
                output_name,
                ArrowDataType::Duration(TimeUnit::Microsecond),
                true,
            ),
            Arc::new(DurationMicrosecondArray::from(vec![value])) as ArrayRef,
        )),
        _ => None,
    }
}

/// True when the aggregate column's storage type is a time-family type, i.e.
/// SUM/AVG must be refused instead of summing the raw payload.
pub(in crate::query::executor) fn reject_time_value_aggregate(
    dtype: Option<crate::data::DataType>,
    column: &str,
    function: &str,
) -> io::Result<()> {
    if dtype.map(|dtype| dtype.is_time_family()).unwrap_or(false) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{} requires a numeric column; '{}' is a time/timestamp-family column",
                function, column
            ),
        ));
    }
    Ok(())
}

/// Per-group typed MIN/MAX output for a time-family input array.
pub(in crate::query::executor) fn time_family_group_min_max(
    array: &ArrayRef,
    values: Vec<Option<i64>>,
    output_name: &str,
    is_min: bool,
) -> Option<(Field, ArrayRef)> {
    // Reuse the single-value builder by presenting each aggregate as a typed
    // one-element array is not possible; build from the raw payloads instead.
    let arrow_type = array.data_type().clone();
    let best: Vec<Option<i64>> = values;
    match arrow_type {
        ArrowDataType::Timestamp(unit, timezone) => {
            let raw = best;
            if unit != TimeUnit::Microsecond {
                return None;
            }
            let typed = TimestampMicrosecondArray::from(raw);
            let typed = if timezone.is_some() {
                typed.with_timezone(timezone.as_deref().unwrap_or("+00:00"))
            } else {
                typed
            };
            Some((
                Field::new(
                    output_name,
                    ArrowDataType::Timestamp(TimeUnit::Microsecond, timezone),
                    true,
                ),
                Arc::new(typed) as ArrayRef,
            ))
        }
        ArrowDataType::Date32 => Some((
            Field::new(output_name, ArrowDataType::Date32, true),
            Arc::new(Date32Array::from(
                best.into_iter()
                    .map(|value| value.map(|value| value as i32))
                    .collect::<Vec<Option<i32>>>(),
            )) as ArrayRef,
        )),
        ArrowDataType::Time64(unit) => {
            if unit != TimeUnit::Microsecond {
                return None;
            }
            Some((
                Field::new(output_name, ArrowDataType::Time64(unit), true),
                Arc::new(Time64MicrosecondArray::from(best)) as ArrayRef,
            ))
        }
        ArrowDataType::Duration(unit) => {
            if unit != TimeUnit::Microsecond {
                return None;
            }
            Some((
                Field::new(output_name, ArrowDataType::Duration(unit), true),
                Arc::new(DurationMicrosecondArray::from(best)) as ArrayRef,
            ))
        }
        _ => {
            let _ = is_min;
            None
        }
    }
}

/// Raw `i64` payloads of a typed time-family array (NULL preserved).
pub(in crate::query::executor) fn time_family_raw_i64(array: &ArrayRef) -> Option<Vec<Option<i64>>> {
    match array.data_type() {
        ArrowDataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => array
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
                .map(|arr| (0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i) * 1_000_000)).collect()),
            TimeUnit::Millisecond => array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .map(|arr| (0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i) * 1_000)).collect()),
            TimeUnit::Microsecond => array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map(|arr| (0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))).collect()),
            TimeUnit::Nanosecond => array
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .map(|arr| (0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i) / 1_000)).collect()),
        },
        ArrowDataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|arr| (0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64)).collect()),
        ArrowDataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .map(|arr| (0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))).collect()),
            _ => None,
        },
        ArrowDataType::Duration(unit) => match unit {
            TimeUnit::Microsecond => array
                .as_any()
                .downcast_ref::<DurationMicrosecondArray>()
                .map(|arr| (0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))).collect()),
            _ => None,
        },
        _ => None,
    }
}

/// Typed Arrow array for a time-family `DataType` from raw payload values.
pub(in crate::query::executor) fn time_family_arrow_from_values(
    dtype: crate::data::DataType,
    values: Vec<Option<i64>>,
) -> (ArrowDataType, ArrayRef) {
    match dtype {
        crate::data::DataType::Timestamp => (
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef,
        ),
        crate::data::DataType::TimestampTz => {
            let array = TimestampMicrosecondArray::from(values).with_timezone("+00:00");
            (
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                Arc::new(array) as ArrayRef,
            )
        }
        crate::data::DataType::Date => (
            ArrowDataType::Date32,
            Arc::new(Date32Array::from(
                values
                    .into_iter()
                    .map(|value| value.map(|value| value as i32))
                    .collect::<Vec<Option<i32>>>(),
            )) as ArrayRef,
        ),
        crate::data::DataType::Time => (
            ArrowDataType::Time64(TimeUnit::Microsecond),
            Arc::new(Time64MicrosecondArray::from(values)) as ArrayRef,
        ),
        _ => (
            ArrowDataType::Duration(TimeUnit::Microsecond),
            Arc::new(DurationMicrosecondArray::from(values)) as ArrayRef,
        ),
    }
}
