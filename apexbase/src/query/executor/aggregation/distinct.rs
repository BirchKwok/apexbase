use super::*;
use ahash::AHashSet;

impl ApexExecutor {
    #[inline]
    pub(in crate::query::executor) fn count_distinct(array: &ArrayRef) -> i64 {
        if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
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
        } else if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
            values
                .iter()
                .filter_map(|value| value.map(f64::to_bits))
                .collect::<AHashSet<_>>()
                .len() as i64
        } else {
            (array.len() - array.null_count()) as i64
        }
    }
}
