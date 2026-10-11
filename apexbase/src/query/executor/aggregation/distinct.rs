use super::*;
use ahash::AHashSet;

impl ApexExecutor {
    #[inline]
    pub(in crate::query::executor) fn count_distinct(array: &ArrayRef) -> io::Result<i64> {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::UInt32Type;

        // The distinct set holds roughly one entry per surviving value: charge the column's
        // footprint so a COUNT(DISTINCT) over a huge column cannot grow unbounded (OOC-3).
        // This function used to return a bare `i64`, which is exactly why it could not report an
        // over-budget state without fabricating a wrong 0 — it now propagates the error instead.
        let _distinct_charge =
            crate::query::executor::memory::QueryMemoryCharge::new(array.get_array_memory_size())?;

        Ok(if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
            values
                .iter()
                .filter_map(|value| value)
                .collect::<AHashSet<_>>()
                .len() as i64
        } else if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            (0..values.len())
                .filter(|&index| !values.is_null(index))
                .map(|index| values.value(index))
                .collect::<AHashSet<_>>()
                .len() as i64
        } else if let Some(values) = array
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
        {
            let Some(dictionary) = values.values().as_any().downcast_ref::<StringArray>() else {
                return Ok((array.len() - array.null_count()) as i64);
            };
            (0..values.len())
                .filter(|&index| !values.is_null(index))
                .map(|index| dictionary.value(values.keys().value(index) as usize))
                .collect::<AHashSet<_>>()
                .len() as i64
        } else if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
            values
                .iter()
                .filter_map(|value| value.map(f64::to_bits))
                .collect::<AHashSet<_>>()
                .len() as i64
        } else {
            (array.len() - array.null_count()) as i64
        })
    }
}
