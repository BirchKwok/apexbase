// Aggregation fast paths, WAL/durability, accessors, insert_rows, flush, close, Drop

#[derive(Debug, Clone)]
pub struct NativeStringGroupAgg {
    pub key: String,
    pub count: i64,
    pub distinct_counts: Vec<i64>,
    pub case_counts: Vec<i64>,
    pub max_values: Vec<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct NativeNumericGroupAgg {
    pub key: i64,
    pub stats: Vec<(i64, f64, f64, f64, bool)>,
}

#[derive(Clone, Copy)]
enum NumericGroupKeyTransform<'a> {
    Identity,
    Modulo(i64),
    UpperBounds(&'a [i64]),
}

// Rayon dispatch is visible at sub-millisecond scale when a filtered numeric
// aggregation is split into the default 65K-row groups.  Require enough work
// per participating worker to amortize scheduling and wake-up costs; this keeps
// moderate tables deterministic under background CPU load while retaining
// parallel scans for genuinely large tables.
const FILTERED_AGG_MIN_ROWS_PER_WORKER: usize = 262_144;

#[inline]
fn should_parallel_filtered_agg(row_count: usize, row_group_count: usize, threads: usize) -> bool {
    let workers = row_group_count.min(threads);
    workers > 1
        && row_count
            >= workers.saturating_mul(FILTERED_AGG_MIN_ROWS_PER_WORKER)
}

#[inline(never)]
unsafe fn aggregate_validated_2col_count_f64(
    group_ids1: &[u16],
    group_ids2: &[u16],
    values: *const u8,
    rows: usize,
    dict2_size: usize,
    counts: &mut [i64],
    sums: &mut [f64],
) {
    let counts_ptr = counts.as_mut_ptr();
    let sums_ptr = sums.as_mut_ptr();
    for row in 0..rows {
        unsafe {
            let idx1 = *group_ids1.get_unchecked(row) as usize;
            let idx2 = *group_ids2.get_unchecked(row) as usize;
            let slot = (idx1 * dict2_size + idx2) * 2;
            *counts_ptr.add(slot) += 1;
            *sums_ptr.add(slot + 1) +=
                std::ptr::read_unaligned(values.add(row * 8) as *const f64);
        }
    }
}

#[inline(never)]
unsafe fn aggregate_validated_2col_count_i64(
    group_ids1: &[u16],
    group_ids2: &[u16],
    values: *const u8,
    rows: usize,
    dict2_size: usize,
    counts: &mut [i64],
    sums: &mut [f64],
) {
    let counts_ptr = counts.as_mut_ptr();
    let sums_ptr = sums.as_mut_ptr();
    for row in 0..rows {
        unsafe {
            let idx1 = *group_ids1.get_unchecked(row) as usize;
            let idx2 = *group_ids2.get_unchecked(row) as usize;
            let slot = (idx1 * dict2_size + idx2) * 2;
            *counts_ptr.add(slot) += 1;
            *sums_ptr.add(slot + 1) +=
                std::ptr::read_unaligned(values.add(row * 8) as *const i64) as f64;
        }
    }
}

impl OnDemandStorage {
    /// Exact COUNT(DISTINCT integral_column) for a bounded numeric domain.
    /// Each Row Group fills a local bitset and the final reduction ORs them,
    /// avoiding a million-entry hash table for domains such as ports/codes.
    pub fn execute_numeric_distinct_count_mmap(
        &self,
        column: &str,
    ) -> io::Result<Option<i64>> {
        use rayon::prelude::*;

        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        let Some(column_index) = footer.schema.get_index(column) else {
            return Ok(None);
        };
        if !matches!(
            footer.schema.columns[column_index].1,
            ColumnType::Int64
                | ColumnType::Int8
                | ColumnType::Int16
                | ColumnType::Int32
                | ColumnType::UInt8
                | ColumnType::UInt16
                | ColumnType::UInt32
                | ColumnType::UInt64
                | ColumnType::Timestamp
                | ColumnType::Date
                | ColumnType::Bool
        ) {
            return Ok(None);
        }
        let mut minimum = i64::MAX;
        let mut maximum = i64::MIN;
        for zone_maps in &footer.zone_maps {
            let Some(zone_map) = zone_maps
                .iter()
                .find(|zone_map| zone_map.col_idx as usize == column_index && !zone_map.is_float)
            else {
                return Ok(None);
            };
            minimum = minimum.min(zone_map.min_bits);
            maximum = maximum.max(zone_map.max_bits);
        }
        let width = maximum.saturating_sub(minimum).saturating_add(1);
        if width <= 0 || width > 1_000_000 {
            return Ok(None);
        }
        let words = (width as usize).div_ceil(64);

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for numeric DISTINCT"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let mmap_ptr = mmap_ref.as_ptr() as usize;
        let mmap_len = mmap_ref.len();
        let parts: Option<Vec<Vec<u64>>> = footer
            .row_groups
            .par_iter()
            .enumerate()
            .map(|(row_group_index, row_group)| {
                let rows = row_group.row_count as usize;
                let mut seen = vec![0u64; words];
                if rows == 0 {
                    return Some(seen);
                }
                let mmap = unsafe {
                    std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len)
                };
                let end = (row_group.offset + row_group.data_size) as usize;
                if end > mmap.len() || end < row_group.offset as usize + 32 {
                    return None;
                }
                let row_group_bytes = &mmap[row_group.offset as usize..end];
                let encoded_body = &row_group_bytes[32..];
                let decompressed =
                    decompress_rg_body(row_group_bytes[28], encoded_body).ok()?;
                let body = decompressed.as_deref().unwrap_or(encoded_body);
                let offsets = footer.col_offsets.get(row_group_index)?;
                let offset = *offsets.get(column_index)? as usize;
                let bitmap_bytes = rows.div_ceil(8);
                if offset + bitmap_bytes >= body.len() {
                    return None;
                }
                let nulls = &body[offset..offset + bitmap_bytes];
                let deletion_offset = rg_id_section_len(
                    rows,
                    row_group_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
                );
                let deleted = body.get(deletion_offset..deletion_offset + bitmap_bytes)?;
                let (values, _) = read_column_encoded(
                    &body[offset + bitmap_bytes..],
                    footer.schema.columns[column_index].1,
                )
                .ok()?;
                let ColumnData::Int64(values) = values else {
                    return None;
                };
                for (row, &value) in values.iter().take(rows).enumerate() {
                    if ((nulls[row / 8] | deleted[row / 8]) >> (row % 8)) & 1 != 0 {
                        continue;
                    }
                    let slot = value.saturating_sub(minimum) as usize;
                    if slot >= width as usize {
                        return None;
                    }
                    seen[slot / 64] |= 1u64 << (slot % 64);
                }
                Some(seen)
            })
            .collect();
        drop(mmap_guard);
        drop(file_guard);
        let Some(parts) = parts else {
            return Ok(None);
        };
        let mut seen = vec![0u64; words];
        for part in parts {
            for (target, source) in seen.iter_mut().zip(part) {
                *target |= source;
            }
        }
        Ok(Some(seen.into_iter().map(|word| word.count_ones() as i64).sum()))
    }

    /// Count NULL values for several columns by reading only the per-column
    /// null bitmaps. Deleted rows are excluded without decoding column values.
    pub fn execute_null_counts_mmap(
        &self,
        columns: &[&str],
    ) -> io::Result<Option<Vec<i64>>> {
        use rayon::prelude::*;

        if columns.is_empty() || self.has_pending_deltas() {
            return Ok(None);
        }
        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        let indices: Option<Vec<usize>> = columns
            .iter()
            .map(|column| footer.schema.get_index(column))
            .collect();
        let Some(indices) = indices else {
            return Ok(None);
        };

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for NULL aggregation"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let mmap_ptr = mmap_ref.as_ptr() as usize;
        let mmap_len = mmap_ref.len();

        let parts: Option<Vec<Vec<i64>>> = footer
            .row_groups
            .par_iter()
            .enumerate()
            .map(|(row_group_index, row_group)| {
                let rows = row_group.row_count as usize;
                let mut counts = vec![0i64; indices.len()];
                if rows == 0 {
                    return Some(counts);
                }
                let mmap = unsafe {
                    std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len)
                };
                let row_group_end = (row_group.offset + row_group.data_size) as usize;
                if row_group_end > mmap.len() || row_group_end < row_group.offset as usize + 32 {
                    return None;
                }
                let row_group_bytes = &mmap[row_group.offset as usize..row_group_end];
                let encoded_body = &row_group_bytes[32..];
                let decompressed = decompress_rg_body(row_group_bytes[28], encoded_body).ok()?;
                let body = decompressed.as_deref().unwrap_or(encoded_body);
                let offsets = footer.col_offsets.get(row_group_index)?;
                let bitmap_len = rows.div_ceil(8);
                let deletion_offset = rg_id_section_len(
                    rows,
                    row_group_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
                );
                let deleted = body.get(deletion_offset..deletion_offset + bitmap_len)?;

                for (slot, &column_index) in indices.iter().enumerate() {
                    let offset = *offsets.get(column_index)? as usize;
                    let nulls = body.get(offset..offset + bitmap_len)?;
                    let mut count = 0u32;
                    for byte_index in 0..bitmap_len {
                        let mut mask = u8::MAX;
                        if byte_index + 1 == bitmap_len && rows % 8 != 0 {
                            mask = (1u16.checked_shl((rows % 8) as u32).unwrap_or(0) - 1) as u8;
                        }
                        count += (nulls[byte_index] & !deleted[byte_index] & mask).count_ones();
                    }
                    counts[slot] = count as i64;
                }
                Some(counts)
            })
            .collect();
        drop(mmap_guard);
        drop(file_guard);
        let Some(parts) = parts else {
            return Ok(None);
        };
        let mut counts = vec![0i64; columns.len()];
        for part in parts {
            for (total, count) in counts.iter_mut().zip(part) {
                *total += count;
            }
        }
        Ok(Some(counts))
    }

    /// Count several simple numeric CASE predicates in one parallel Row Group scan.
    pub fn execute_numeric_case_counts_mmap(
        &self,
        specs: &[(&str, f64, f64)],
    ) -> io::Result<Option<Vec<i64>>> {
        use rayon::prelude::*;

        if specs.is_empty() {
            return Ok(None);
        }
        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        if footer.row_groups.iter().any(|row_group| row_group.deletion_count > 0) {
            return Ok(None);
        }
        let schema = &footer.schema;
        let mut unique_indices = Vec::new();
        let mut spec_columns = Vec::with_capacity(specs.len());
        for &(column, _, _) in specs {
            let Some(index) = schema.get_index(column) else {
                return Ok(None);
            };
            if !matches!(
                schema.columns[index].1,
                ColumnType::Int64
                    | ColumnType::Int8
                    | ColumnType::Int16
                    | ColumnType::Int32
                    | ColumnType::UInt8
                    | ColumnType::UInt16
                    | ColumnType::UInt32
                    | ColumnType::UInt64
                    | ColumnType::Timestamp
                    | ColumnType::Date
                    | ColumnType::Float64
                    | ColumnType::Float32
                    | ColumnType::Bool
            ) {
                return Ok(None);
            }
            let slot = unique_indices
                .iter()
                .position(|&existing| existing == index)
                .unwrap_or_else(|| {
                    unique_indices.push(index);
                    unique_indices.len() - 1
                });
            spec_columns.push(slot);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for numeric CASE aggregation"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let mmap_ptr = mmap_ref.as_ptr() as usize;
        let mmap_len = mmap_ref.len();

        let parts: Option<Vec<Vec<i64>>> = footer
            .row_groups
            .par_iter()
            .enumerate()
            .map(|(row_group_index, row_group)| {
                let rows = row_group.row_count as usize;
                let mut counts = vec![0i64; specs.len()];
                if rows == 0 {
                    return Some(counts);
                }
                let mmap = unsafe {
                    std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len)
                };
                let row_group_end = (row_group.offset + row_group.data_size) as usize;
                if row_group_end > mmap.len() || row_group_end < row_group.offset as usize + 32 {
                    return None;
                }
                let row_group_bytes = &mmap[row_group.offset as usize..row_group_end];
                let encoded_body = &row_group_bytes[32..];
                let decompressed = decompress_rg_body(row_group_bytes[28], encoded_body).ok()?;
                let body = decompressed.as_deref().unwrap_or(encoded_body);
                let offsets = footer.col_offsets.get(row_group_index)?;
                let null_bytes = rows.div_ceil(8);
                let decoded: Option<Vec<(ColumnData, &[u8])>> = unique_indices
                    .iter()
                    .map(|&index| {
                        let offset = *offsets.get(index)? as usize;
                        if offset + null_bytes >= body.len() {
                            return None;
                        }
                        let nulls = &body[offset..offset + null_bytes];
                        let (column, _) = read_column_encoded(
                            &body[offset + null_bytes..],
                            schema.columns[index].1,
                        )
                        .ok()?;
                        Some((column, nulls))
                    })
                    .collect();
                let decoded = decoded?;

                for (spec_index, &(_, low, high)) in specs.iter().enumerate() {
                    let (column, nulls) = &decoded[spec_columns[spec_index]];
                    match column {
                        ColumnData::Int64(values) => {
                            let low = low.ceil() as i64;
                            let high = high.floor() as i64;
                            for (row, &value) in values.iter().take(rows).enumerate() {
                                if nulls
                                    .get(row / 8)
                                    .is_some_and(|byte| ((byte >> (row % 8)) & 1) != 0)
                                {
                                    continue;
                                }
                                counts[spec_index] += (value >= low && value <= high) as i64;
                            }
                        }
                        ColumnData::Float64(values) => {
                            for (row, &value) in values.iter().take(rows).enumerate() {
                                if nulls
                                    .get(row / 8)
                                    .is_some_and(|byte| ((byte >> (row % 8)) & 1) != 0)
                                {
                                    continue;
                                }
                                counts[spec_index] += (value >= low && value <= high) as i64;
                            }
                        }
                        ColumnData::Bool { data, len } => {
                            for row in 0..rows.min(*len) {
                                if nulls
                                    .get(row / 8)
                                    .is_some_and(|byte| ((byte >> (row % 8)) & 1) != 0)
                                {
                                    continue;
                                }
                                let value = data
                                    .get(row / 8)
                                    .map_or(0.0, |byte| ((byte >> (row % 8)) & 1) as f64);
                                counts[spec_index] += (value >= low && value <= high) as i64;
                            }
                        }
                        _ => return None,
                    }
                }
                Some(counts)
            })
            .collect();
        drop(mmap_guard);
        drop(file_guard);
        let Some(parts) = parts else {
            return Ok(None);
        };
        let mut counts = vec![0i64; specs.len()];
        for part in parts {
            for (total, count) in counts.iter_mut().zip(part) {
                *total += count;
            }
        }
        Ok(Some(counts))
    }

    /// Parallel Row Group aggregation for an integral GROUP BY key.
    /// Each requested aggregate receives (non-null count, sum, min, max, is_int).
    pub fn execute_numeric_group_agg_mmap(
        &self,
        group_col: &str,
        agg_cols: &[(&str, bool)],
        agg_ops: &[u8],
    ) -> io::Result<Option<Vec<NativeNumericGroupAgg>>> {
        self.execute_numeric_group_agg_mmap_impl(
            group_col,
            agg_cols,
            agg_ops,
            NumericGroupKeyTransform::Identity,
        )
    }

    /// GROUP BY MOD(integral_column, positive_modulus) using the same native
    /// aggregation core as a plain integral key.
    pub fn execute_numeric_mod_group_agg_mmap(
        &self,
        group_col: &str,
        modulus: i64,
        agg_cols: &[(&str, bool)],
        agg_ops: &[u8],
    ) -> io::Result<Option<Vec<NativeNumericGroupAgg>>> {
        if modulus <= 0 || modulus > 32_768 {
            return Ok(None);
        }
        self.execute_numeric_group_agg_mmap_impl(
            group_col,
            agg_cols,
            agg_ops,
            NumericGroupKeyTransform::Modulo(modulus),
        )
    }

    /// Build reusable dense group IDs for MOD(integral_column, modulus).
    /// The dictionary contains the complete Rust/SQL remainder domain so row
    /// IDs can be generated branch-light and later reused across warm queries.
    pub fn build_numeric_mod_dict_cache(
        &self,
        column: &str,
        modulus: i64,
    ) -> io::Result<Option<(Vec<String>, Vec<u16>)>> {
        if modulus <= 0 || modulus > 32_768 || self.has_pending_deltas() {
            return Ok(None);
        }
        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        let Some(index) = footer.schema.get_index(column) else {
            return Ok(None);
        };
        if !matches!(
            footer.schema.columns[index].1,
            ColumnType::Int64
                | ColumnType::Int8
                | ColumnType::Int16
                | ColumnType::Int32
                | ColumnType::UInt8
                | ColumnType::UInt16
                | ColumnType::UInt32
                | ColumnType::UInt64
        ) {
            return Ok(None);
        }
        let (columns, deleted) = self.scan_columns_mmap(&[index], &footer)?;
        if deleted.iter().any(|&byte| byte != 0) {
            return Ok(None);
        }
        let Some(ColumnData::Int64(values)) = columns.first() else {
            return Ok(None);
        };
        let minimum = 1 - modulus;
        let labels = (minimum..modulus)
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let group_ids = values
            .iter()
            .map(|value| (value % modulus - minimum) as u16)
            .collect();
        Ok(Some((labels, group_ids)))
    }

    pub fn execute_numeric_bucket_group_agg_mmap(
        &self,
        group_col: &str,
        upper_bounds: &[i64],
        agg_cols: &[(&str, bool)],
        agg_ops: &[u8],
    ) -> io::Result<Option<Vec<NativeNumericGroupAgg>>> {
        if upper_bounds.is_empty()
            || upper_bounds.len() >= 4096
            || upper_bounds.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Ok(None);
        }
        self.execute_numeric_group_agg_mmap_impl(
            group_col,
            agg_cols,
            agg_ops,
            NumericGroupKeyTransform::UpperBounds(upper_bounds),
        )
    }

    fn execute_numeric_group_agg_mmap_impl(
        &self,
        group_col: &str,
        agg_cols: &[(&str, bool)],
        agg_ops: &[u8],
        transform: NumericGroupKeyTransform<'_>,
    ) -> io::Result<Option<Vec<NativeNumericGroupAgg>>> {
        use rayon::prelude::*;

        if agg_cols.len() != agg_ops.len() {
            return Ok(None);
        }
        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let Some(group_idx) = schema.get_index(group_col) else {
            return Ok(None);
        };
        if !matches!(
            schema.columns[group_idx].1,
            ColumnType::Int64
                | ColumnType::Int8
                | ColumnType::Int16
                | ColumnType::Int32
                | ColumnType::UInt8
                | ColumnType::UInt16
                | ColumnType::UInt32
                | ColumnType::UInt64
                | ColumnType::Timestamp
                | ColumnType::Date
                | ColumnType::Bool
        ) || footer.row_groups.iter().any(|row_group| row_group.deletion_count > 0)
        {
            return Ok(None);
        }

        let mut logical_indices = Vec::with_capacity(agg_cols.len());
        for &(name, count_star) in agg_cols {
            if count_star {
                logical_indices.push(None);
                continue;
            }
            let Some(index) = schema.get_index(name) else {
                return Ok(None);
            };
            if !matches!(
                schema.columns[index].1,
                ColumnType::Int64
                    | ColumnType::Int8
                    | ColumnType::Int16
                    | ColumnType::Int32
                    | ColumnType::UInt8
                    | ColumnType::UInt16
                    | ColumnType::UInt32
                    | ColumnType::UInt64
                    | ColumnType::Timestamp
                    | ColumnType::Date
                    | ColumnType::Float64
                    | ColumnType::Float32
                    | ColumnType::Bool
            ) {
                return Ok(None);
            }
            logical_indices.push(Some(index));
        }

        // Multiple SQL aggregates often reuse one source column. Aggregate it
        // once with a combined operation mask, and map the physical state back
        // to every logical output. COUNT(*) can share any known non-null source
        // already being scanned, which halves the state for COUNT+AVG profiles.
        let mut physical_indices: Vec<Option<usize>> = Vec::new();
        let mut physical_ops: Vec<u8> = Vec::new();
        let mut logical_to_physical = vec![usize::MAX; agg_cols.len()];
        for (logical, index) in logical_indices.iter().copied().enumerate() {
            let Some(index) = index else {
                continue;
            };
            let physical = physical_indices
                .iter()
                .position(|candidate| *candidate == Some(index))
                .unwrap_or_else(|| {
                    physical_indices.push(Some(index));
                    physical_ops.push(0);
                    physical_indices.len() - 1
                });
            physical_ops[physical] |= agg_ops[logical];
            logical_to_physical[logical] = physical;
        }
        for (logical, index) in logical_indices.iter().enumerate() {
            if index.is_some() {
                continue;
            }
            let reusable = physical_indices.iter().position(|index| {
                index.is_some_and(|index| {
                    !self.column_has_nulls(&schema.columns[index].0)
                })
            });
            let physical = reusable.unwrap_or_else(|| {
                let existing = physical_indices.iter().position(Option::is_none);
                existing.unwrap_or_else(|| {
                    physical_indices.push(None);
                    physical_ops.push(0);
                    physical_indices.len() - 1
                })
            });
            logical_to_physical[logical] = physical;
        }

        #[derive(Clone, Copy)]
        struct Stat {
            count: i64,
            sum: f64,
            min: f64,
            max: f64,
            is_int: bool,
        }
        impl Stat {
            fn new(is_int: bool) -> Self {
                Self {
                    count: 0,
                    sum: 0.0,
                    min: f64::INFINITY,
                    max: f64::NEG_INFINITY,
                    is_int,
                }
            }
            fn add(&mut self, value: f64, operations: u8) {
                self.count += 1;
                if operations & 1 != 0 {
                    self.sum += value;
                }
                if operations & 2 != 0 {
                    self.min = self.min.min(value);
                }
                if operations & 4 != 0 {
                    self.max = self.max.max(value);
                }
            }
            fn merge(&mut self, other: Self, operations: u8) {
                self.count += other.count;
                if operations & 1 != 0 {
                    self.sum += other.sum;
                }
                if operations & 2 != 0 {
                    self.min = self.min.min(other.min);
                }
                if operations & 4 != 0 {
                    self.max = self.max.max(other.max);
                }
            }
        }

        let agg_is_int: Vec<bool> = physical_indices
            .iter()
            .map(|index| {
                index.is_none()
                    || index.is_some_and(|index| {
                        !matches!(
                            schema.columns[index].1,
                            ColumnType::Float64 | ColumnType::Float32
                        )
                    })
            })
            .collect();
        let dense_bounds = match transform {
            NumericGroupKeyTransform::Modulo(modulus) => {
                Some((1 - modulus, (modulus * 2 - 1) as usize))
            }
            NumericGroupKeyTransform::UpperBounds(bounds) => Some((0, bounds.len() + 1)),
            NumericGroupKeyTransform::Identity => {
            let mut minimum = i64::MAX;
            let mut maximum = i64::MIN;
            let mut complete = true;
            for zone_maps in &footer.zone_maps {
                match zone_maps
                    .iter()
                    .find(|zone_map| zone_map.col_idx as usize == group_idx && !zone_map.is_float)
                {
                    Some(zone_map) => {
                        minimum = minimum.min(zone_map.min_bits);
                        maximum = maximum.max(zone_map.max_bits);
                    }
                    None => {
                        complete = false;
                        break;
                    }
                }
            }
            let width = maximum.saturating_sub(minimum).saturating_add(1);
            // Dense integral domains such as ports (0..65535) are much faster
            // as a flat accumulator than as one hash-map entry and heap Vec
            // per group. The upper bound keeps per-row-group state bounded.
                (complete && width > 0 && width <= 65_536)
                    .then_some((minimum, width as usize))
            }
        };
        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for numeric GROUP BY"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let mmap_ptr = mmap_ref.as_ptr() as usize;
        let mmap_len = mmap_ref.len();

        enum Part {
            Dense {
                stats: Vec<Stat>,
                seen: Vec<bool>,
            },
            Sparse(ahash::AHashMap<i64, Vec<Stat>>),
        }

        let new_dense_stats = |width: usize| {
            (0..width)
                .flat_map(|_| agg_is_int.iter().map(|&is_int| Stat::new(is_int)))
                .collect::<Vec<_>>()
        };

        let parts: Option<Vec<Part>> = footer
            .row_groups
            .par_iter()
            .enumerate()
            .map(|(row_group_index, row_group)| {
                let rows = row_group.row_count as usize;
                let mut groups: ahash::AHashMap<i64, Vec<Stat>> = ahash::AHashMap::new();
                let mut dense_groups = dense_bounds.map(|(minimum, width)| {
                    (
                        minimum,
                        new_dense_stats(width),
                        vec![false; width],
                    )
                });
                if rows == 0 {
                    return Some(match dense_groups {
                        Some((_, stats, seen)) => Part::Dense { stats, seen },
                        None => Part::Sparse(groups),
                    });
                }
                let mmap = unsafe {
                    std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len)
                };
                let row_group_end = (row_group.offset + row_group.data_size) as usize;
                if row_group_end > mmap.len() || row_group_end < row_group.offset as usize + 32 {
                    return None;
                }
                let row_group_bytes = &mmap[row_group.offset as usize..row_group_end];
                let compression = row_group_bytes[28];
                let encoded_body = &row_group_bytes[32..];
                let decompressed = decompress_rg_body(compression, encoded_body).ok()?;
                let body = decompressed.as_deref().unwrap_or(encoded_body);
                let offsets = footer.col_offsets.get(row_group_index)?;
                let null_bytes = rows.div_ceil(8);

                let decode_column = |index: usize| -> Option<(ColumnData, &[u8])> {
                    let offset = *offsets.get(index)? as usize;
                    if offset + null_bytes >= body.len() {
                        return None;
                    }
                    let nulls = &body[offset..offset + null_bytes];
                    let (column, _) =
                        read_column_encoded(&body[offset + null_bytes..], schema.columns[index].1)
                            .ok()?;
                    Some((column, nulls))
                };

                let (group_column, group_nulls) = decode_column(group_idx)?;
                let ColumnData::Int64(group_values) = group_column else {
                    return None;
                };
                let decoded_aggs: Option<Vec<Option<(ColumnData, &[u8])>>> = physical_indices
                    .iter()
                    .map(|index| match index {
                        Some(index) => decode_column(*index).map(Some),
                        None => Some(None),
                    })
                    .collect();
                let decoded_aggs = decoded_aggs?;

                for row in 0..rows.min(group_values.len()) {
                    if group_nulls
                        .get(row / 8)
                        .is_some_and(|byte| ((byte >> (row % 8)) & 1) != 0)
                    {
                        continue;
                    }
                    let key = match transform {
                        NumericGroupKeyTransform::Identity => group_values[row],
                        NumericGroupKeyTransform::Modulo(modulus) => {
                            group_values[row] % modulus
                        }
                        NumericGroupKeyTransform::UpperBounds(bounds) => bounds
                            .partition_point(|&bound| group_values[row] >= bound)
                            as i64,
                    };
                    let stats = if let Some((minimum, dense, seen)) = &mut dense_groups {
                        let slot = key.saturating_sub(*minimum) as usize;
                        if slot >= seen.len() {
                            return None;
                        }
                        seen[slot] = true;
                        let start = slot * physical_indices.len();
                        &mut dense[start..start + physical_indices.len()]
                    } else {
                        groups.entry(key).or_insert_with(|| {
                            agg_is_int.iter().map(|&is_int| Stat::new(is_int)).collect()
                        })
                    };
                    for (agg_index, decoded) in decoded_aggs.iter().enumerate() {
                        let stat = &mut stats[agg_index];
                        let Some((column, nulls)) = decoded else {
                            stat.add(0.0, physical_ops[agg_index]);
                            continue;
                        };
                        if nulls
                            .get(row / 8)
                            .is_some_and(|byte| ((byte >> (row % 8)) & 1) != 0)
                        {
                            continue;
                        }
                        match column {
                            ColumnData::Int64(values) if row < values.len() => {
                                stat.add(values[row] as f64, physical_ops[agg_index])
                            }
                            ColumnData::Float64(values) if row < values.len() => {
                                stat.add(values[row], physical_ops[agg_index])
                            }
                            ColumnData::Bool { data, len } if row < *len => {
                                let value = data
                                    .get(row / 8)
                                    .map_or(0.0, |byte| ((byte >> (row % 8)) & 1) as f64);
                                stat.add(value, physical_ops[agg_index]);
                            }
                            _ => {}
                        }
                    }
                }
                Some(match dense_groups {
                    Some((_, stats, seen)) => Part::Dense { stats, seen },
                    None => Part::Sparse(groups),
                })
            })
            .collect();
        drop(mmap_guard);
        drop(file_guard);
        let Some(parts) = parts else {
            return Ok(None);
        };

        let merged_rows: Vec<(i64, Vec<Stat>)> = if let Some((minimum, width)) = dense_bounds {
            let mut merged = new_dense_stats(width);
            let mut merged_seen = vec![false; width];
            for part in parts {
                let Part::Dense { stats, seen, .. } = part else {
                    return Ok(None);
                };
                for (slot, present) in seen.into_iter().enumerate() {
                    if !present {
                        continue;
                    }
                    merged_seen[slot] = true;
                    let start = slot * physical_indices.len();
                    for aggregate in 0..physical_indices.len() {
                        merged[start + aggregate]
                            .merge(stats[start + aggregate], physical_ops[aggregate]);
                    }
                }
            }
            merged_seen
                .into_iter()
                .enumerate()
                .filter_map(|(slot, present)| {
                    present.then(|| {
                        let start = slot * physical_indices.len();
                        (
                            minimum + slot as i64,
                            merged[start..start + physical_indices.len()].to_vec(),
                        )
                    })
                })
                .collect()
        } else {
            let mut merged: ahash::AHashMap<i64, Vec<Stat>> = ahash::AHashMap::new();
            for part in parts {
                let Part::Sparse(groups) = part else {
                    return Ok(None);
                };
                for (key, source_stats) in groups {
                    let target_stats = merged.entry(key).or_insert_with(|| {
                        agg_is_int.iter().map(|&is_int| Stat::new(is_int)).collect()
                    });
                    for (aggregate, (target, source)) in target_stats
                        .iter_mut()
                        .zip(source_stats)
                        .enumerate()
                    {
                        target.merge(source, physical_ops[aggregate]);
                    }
                }
            }
            merged.into_iter().collect()
        };
        Ok(Some(
            merged_rows
                .into_iter()
                .map(|(key, stats)| NativeNumericGroupAgg {
                    key,
                    stats: logical_to_physical
                        .iter()
                        .map(|&physical| {
                            let stat = stats[physical];
                            (
                                stat.count,
                                stat.sum,
                                if stat.count == 0 { 0.0 } else { stat.min },
                                if stat.count == 0 { 0.0 } else { stat.max },
                                stat.is_int,
                            )
                        })
                        .collect(),
                })
                .collect(),
        ))
    }

    /// Execute simple aggregation (no GROUP BY, no WHERE) directly on V4 columns.
    /// Supports both in-memory and mmap-only paths.
    /// Returns (count, sum, min, max, is_int) for each requested column
    pub fn execute_simple_agg(
        &self,
        agg_cols: &[&str],
    ) -> io::Result<Option<Vec<(i64, f64, f64, f64, bool)>>> {
        use arrow::array::PrimitiveArray;
        use arrow::buffer::{Buffer, ScalarBuffer};
        use arrow::datatypes::{Int64Type, Float64Type};
        use std::sync::Arc;

        // Check if in-memory data is available for fast path
        let columns = self.columns.read();
        let has_in_memory = !columns.is_empty() && columns.iter().any(|c| c.len() > 0);

        if !has_in_memory {
            drop(columns);
            // MMAP PATH: scan columns from disk without loading into memory
            return self.execute_simple_agg_mmap(agg_cols);
        }

        let schema = self.schema.read();
        let deleted = self.deleted.read();
        let nulls = self.nulls.read();
        let total_rows = columns.first().map(|c| c.len()).unwrap_or(0);

        let has_deleted = deleted.iter().any(|&b| b != 0);
        // Bail to Arrow path if there are deleted rows (need filtered arrays)
        if has_deleted { return Ok(None); }

        let active_count = total_rows as i64;

        // Helper: check if row i is NULL using the null bitmap (bit=1 means null)
        #[inline]
        fn is_null_at(bitmap: &[u8], i: usize) -> bool {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            byte_idx < bitmap.len() && (bitmap[byte_idx] >> bit_idx) & 1 == 1
        }

        let mut results: Vec<(i64, f64, f64, f64, bool)> = Vec::with_capacity(agg_cols.len());

        for &col_name in agg_cols {
            if col_name == "*" || col_name == "1" {
                results.push((active_count, 0.0, 0.0, 0.0, false));
                continue;
            }

            let col_idx = match schema.get_index(col_name) {
                Some(idx) => idx,
                None => return Ok(None), // unknown column (e.g. _id) — fall back to Arrow path
            };
            if col_idx >= columns.len() { return Ok(None); }

            // Get the null bitmap for this column (empty = no nulls)
            let null_bm: &[u8] = if col_idx < nulls.len() { &nulls[col_idx] } else { &[] };
            let has_nulls = !null_bm.is_empty() && null_bm.iter().any(|&b| b != 0);

            match &columns[col_idx] {
                ColumnData::Int64(vals) => {
                    if !has_nulls {
                        // Fast path: no nulls — use SIMD sum
                        let sum: i64 = vals.iter().sum();
                        let min_v = vals.iter().copied().min().unwrap_or(i64::MAX);
                        let max_v = vals.iter().copied().max().unwrap_or(i64::MIN);
                        results.push((vals.len() as i64, sum as f64, min_v as f64, max_v as f64, true));
                    } else {
                        let mut count = 0i64;
                        let mut sum = 0i64;
                        let mut min_v = i64::MAX;
                        let mut max_v = i64::MIN;
                        for (i, &v) in vals.iter().enumerate() {
                            if !is_null_at(null_bm, i) {
                                count += 1;
                                sum += v;
                                if v < min_v { min_v = v; }
                                if v > max_v { max_v = v; }
                            }
                        }
                        results.push((count, sum as f64, min_v as f64, max_v as f64, true));
                    }
                }
                ColumnData::Float64(vals) => {
                    if !has_nulls {
                        let sum: f64 = vals.iter().sum();
                        let min_v = vals.iter().copied().fold(f64::INFINITY, f64::min);
                        let max_v = vals.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                        results.push((vals.len() as i64, sum, min_v, max_v, false));
                    } else {
                        let mut count = 0i64;
                        let mut sum = 0.0f64;
                        let mut min_v = f64::INFINITY;
                        let mut max_v = f64::NEG_INFINITY;
                        for (i, &v) in vals.iter().enumerate() {
                            if !is_null_at(null_bm, i) {
                                count += 1;
                                sum += v;
                                if v < min_v { min_v = v; }
                                if v > max_v { max_v = v; }
                            }
                        }
                        results.push((count, sum, min_v, max_v, false));
                    }
                }
                _ => {
                    let null_count = if has_nulls {
                        (0..total_rows).filter(|&i| is_null_at(null_bm, i)).count() as i64
                    } else {
                        0
                    };
                    results.push((active_count - null_count, 0.0, 0.0, 0.0, false));
                }
            }
        }

        Ok(Some(results))
    }

    /// MMAP PATH: Execute simple aggregation by scanning V4 RGs via mmap.
    /// Uses per-RG streaming to avoid building a full 1M-element Vec.
    /// PLAIN-encoded columns are processed zero-copy from mmap; RLE/BITPACK decoded per-RG.
    fn execute_simple_agg_mmap(
        &self,
        agg_cols: &[&str],
    ) -> io::Result<Option<Vec<(i64, f64, f64, f64, bool)>>> {
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };

        let schema = &footer.schema;
        let non_star: Vec<&str> = agg_cols.iter()
            .filter(|&&n| n != "*" && n != "1" && !n.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false))
            .copied()
            .collect();
        let col_indices: Vec<usize> = non_star.iter()
            .filter_map(|&n| schema.get_index(n))
            .collect();
        // If any requested column doesn't exist in schema, bail to the Arrow path
        if col_indices.len() != non_star.len() { return Ok(None); }

        let total_active: i64 = footer.row_groups.iter()
            .map(|rg| rg.active_rows() as i64)
            .sum();

        if col_indices.is_empty() {
            return Ok(Some(agg_cols.iter().map(|_| (total_active, 0.0, 0.0, 0.0, false)).collect()));
        }

        // FAST PATH: use pre-computed sidecar stats if data is clean (no deletes, no deltas)
        let has_any_deletes = footer.row_groups.iter().any(|rg| rg.deletion_count > 0);
        if !has_any_deletes && !self.has_pending_deltas() {
            if let Some(sidecar) = self.try_read_col_stats_sidecar() {
                let mut results: Vec<(i64, f64, f64, f64, bool)> = Vec::with_capacity(agg_cols.len());
                let mut all_found = true;
                for &col_name in agg_cols {
                    if col_name == "*" || col_name == "1"
                        || col_name.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false)
                    {
                        results.push((total_active, 0.0, 0.0, 0.0, false));
                    } else if let Some(&(count, sum, min, max, is_int)) = sidecar.get(col_name) {
                        results.push((count, sum, min, max, is_int));
                    } else {
                        all_found = false;
                        break;
                    }
                }
                if all_found { return Ok(Some(results)); }
            }
        }

        // Per-column streaming accumulators (no large Vec allocation)
        let nc = col_indices.len();
        let mut col_counts = vec![0i64; nc];
        let mut col_sums   = vec![0.0f64; nc];
        let mut col_mins   = vec![f64::INFINITY; nc];
        let mut col_maxs   = vec![f64::NEG_INFINITY; nc];
        let mut col_is_int = vec![false; nc];

        // Check whether all RGs qualify for the streaming zero-copy path
        // (uncompressed + RCIX available for ALL requested columns).
        let max_col_idx = col_indices.iter().copied().max().unwrap_or(0);
        let all_rcix = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
            if rg_meta.row_count == 0 { return true; }
            footer.col_offsets.get(rg_i).map_or(false, |v| v.len() > max_col_idx)
        });

        if !all_rcix {
            // FALLBACK: old path — builds full Vec but handles compressed/old files
            let (scanned_cols, del_bytes) = self.scan_columns_mmap(&col_indices, &footer)?;
            let has_deleted = del_bytes.iter().any(|&b| b != 0);
            let mut results: Vec<(i64, f64, f64, f64, bool)> = Vec::with_capacity(agg_cols.len());
            let mut scan_idx = 0usize;
            for &col_name in agg_cols {
                if col_name == "*" || col_name == "1" {
                    results.push((total_active, 0.0, 0.0, 0.0, false));
                    continue;
                }
                if scan_idx >= scanned_cols.len() { results.push((0, 0.0, 0.0, 0.0, false)); continue; }
                let col = &scanned_cols[scan_idx]; scan_idx += 1;
                match col {
                    ColumnData::Int64(vals) => {
                        let n = vals.len() as i64;
                        let sum: i64 = vals.iter().sum();
                        let min_v = vals.iter().copied().min().unwrap_or(i64::MAX);
                        let max_v = vals.iter().copied().max().unwrap_or(i64::MIN);
                        let _ = has_deleted;
                        results.push((n, sum as f64, min_v as f64, max_v as f64, true));
                    }
                    ColumnData::Float64(vals) => {
                        let n = vals.len() as i64;
                        let sum: f64 = vals.iter().sum();
                        let min_v = vals.iter().copied().fold(f64::INFINITY, f64::min);
                        let max_v = vals.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                        results.push((n, sum, min_v, max_v, false));
                    }
                    _ => { results.push((total_active, 0.0, 0.0, 0.0, false)); }
                }
            }
            return Ok(Some(results));
        }

        // STREAMING ZERO-COPY PATH: process each RG directly from mmap
        let file_guard = self.file.read();
        let file = file_guard.as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "File not open"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        const PLAIN: u8 = 0u8;

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 { continue; }

            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() { continue; }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize .. rg_end];
            if rg_bytes.len() < 32 { continue; }

            let compress_flag = rg_bytes[28];
            let encoding_version = rg_bytes[29];
            let null_bitmap_len = (rg_rows + 7) / 8;
            let del_start_body = rg_id_section_len(
                rg_rows,
                rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
            );
            let del_vec_len = null_bitmap_len;

            let rcix = match footer.col_offsets.get(rg_i).filter(|v| !v.is_empty()) {
                Some(r) => r,
                None => continue,
            };

            // Must be uncompressed for zero-copy
            if compress_flag != RG_COMPRESS_NONE || encoding_version < 1 {
                // Compressed RG: fall back to read_column_encoded per column
                let body_raw = &rg_bytes[32..];
                let decompressed_buf = decompress_rg_body(compress_flag, body_raw)?;
                let body: &[u8] = decompressed_buf.as_deref().unwrap_or(body_raw);
                for (ci, &col_idx) in col_indices.iter().enumerate() {
                    if col_idx >= rcix.len() { continue; }
                    let col_off = rcix[col_idx] as usize;
                    let data_start = col_off + null_bitmap_len;
                    if data_start >= body.len() { continue; }
                    let null_bm = if col_off + null_bitmap_len <= body.len() { &body[col_off..col_off + null_bitmap_len] } else { &[] };
                    let col_type = schema.columns[col_idx].1;
                    let (col_data, _) = read_column_encoded(&body[data_start..], col_type)?;
                    match &col_data {
                        ColumnData::Int64(v) => { col_is_int[ci] = true; for (i, &x) in v.iter().enumerate() { if !((null_bm.get(i/8).copied().unwrap_or(0) >> (i%8)) & 1 != 0) { col_counts[ci] += 1; col_sums[ci] += x as f64; let xf = x as f64; if xf < col_mins[ci] { col_mins[ci] = xf; } if xf > col_maxs[ci] { col_maxs[ci] = xf; } } } }
                        ColumnData::Float64(v) => { for (i, &x) in v.iter().enumerate() { if !((null_bm.get(i/8).copied().unwrap_or(0) >> (i%8)) & 1 != 0) { col_counts[ci] += 1; col_sums[ci] += x; if x < col_mins[ci] { col_mins[ci] = x; } if x > col_maxs[ci] { col_maxs[ci] = x; } } } }
                        _ => { col_counts[ci] += col_data.len() as i64; }
                    }
                }
                continue;
            }

            let body = &rg_bytes[32..];
            let has_deleted = del_start_body + del_vec_len <= body.len()
                && body[del_start_body..del_start_body + del_vec_len].iter().any(|&b| b != 0);

            for (ci, &col_idx) in col_indices.iter().enumerate() {
                if col_idx >= rcix.len() { continue; }
                let col_off = rcix[col_idx] as usize;
                let data_start = col_off + null_bitmap_len;
                if data_start + 1 > body.len() { continue; }

                let col_type = schema.columns[col_idx].1;
                let encoding = body[data_start];
                let payload = &body[data_start + 1..];

                if encoding == PLAIN && payload.len() >= 8 {
                    let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                    let raw = &payload[8..];
                    match col_type {
                        ColumnType::Float64 | ColumnType::Float32 => {
                            let n = count.min(rg_rows).min(raw.len() / 8);
                            let vals: Vec<f64> = raw.chunks_exact(8).take(n).map(|c| f64::from_le_bytes(c.try_into().unwrap())).collect();
                            let null_bm = if col_off + null_bitmap_len <= body.len() { &body[col_off..col_off + null_bitmap_len] } else { &[][..] };
                            let has_col_nulls = null_bm.iter().any(|&b| b != 0);
                            if !has_deleted && !has_col_nulls {
                                let s: f64 = vals.iter().sum();
                                let mn = vals.iter().copied().fold(col_mins[ci], f64::min);
                                let mx = vals.iter().copied().fold(col_maxs[ci], f64::max);
                                col_counts[ci] += n as i64; col_sums[ci] += s; col_mins[ci] = mn; col_maxs[ci] = mx;
                            } else {
                                let del: &[u8] = if has_deleted { &body[del_start_body..del_start_body + del_vec_len] } else { &[] };
                                for (i, &v) in vals.iter().enumerate() {
                                    if has_deleted && (del[i / 8] >> (i % 8)) & 1 != 0 { continue; }
                                    if has_col_nulls && (null_bm.get(i/8).copied().unwrap_or(0) >> (i%8)) & 1 != 0 { continue; }
                                    col_counts[ci] += 1; col_sums[ci] += v;
                                    if v < col_mins[ci] { col_mins[ci] = v; } if v > col_maxs[ci] { col_maxs[ci] = v; }
                                }
                            }
                        }
                        ColumnType::Int64 | ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 |
                        ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64 |
                        ColumnType::Timestamp | ColumnType::Date => {
                            col_is_int[ci] = true;
                            let n = count.min(rg_rows).min(raw.len() / 8);
                            let vals: Vec<i64> = raw.chunks_exact(8).take(n).map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect();
                            let null_bm = if col_off + null_bitmap_len <= body.len() { &body[col_off..col_off + null_bitmap_len] } else { &[][..] };
                            let has_col_nulls = null_bm.iter().any(|&b| b != 0);
                            if !has_deleted && !has_col_nulls {
                                // Fast path: no deletes, no nulls
                                let s: i64 = vals.iter().copied().sum();
                                let mn = vals.iter().copied().min().unwrap_or(i64::MAX);
                                let mx = vals.iter().copied().max().unwrap_or(i64::MIN);
                                col_counts[ci] += n as i64; col_sums[ci] += s as f64;
                                if (mn as f64) < col_mins[ci] { col_mins[ci] = mn as f64; }
                                if (mx as f64) > col_maxs[ci] { col_maxs[ci] = mx as f64; }
                            } else {
                                let del: &[u8] = if has_deleted { &body[del_start_body..del_start_body + del_vec_len] } else { &[] };
                                for (i, &v) in vals.iter().enumerate() {
                                    if has_deleted && (del[i / 8] >> (i % 8)) & 1 != 0 { continue; }
                                    if has_col_nulls && (null_bm.get(i/8).copied().unwrap_or(0) >> (i%8)) & 1 != 0 { continue; }
                                    col_counts[ci] += 1; col_sums[ci] += v as f64;
                                    let vf = v as f64;
                                    if vf < col_mins[ci] { col_mins[ci] = vf; } if vf > col_maxs[ci] { col_maxs[ci] = vf; }
                                }
                            }
                        }
                        _ => {
                            // Non-numeric (String/StringDict/Bool/Binary): count elements for COUNT(col)
                            let n = count.min(rg_rows);
                            let null_bm = if col_off + null_bitmap_len <= body.len() {
                                &body[col_off..col_off + null_bitmap_len]
                            } else {
                                &[][..]
                            };
                            let del = if has_deleted {
                                &body[del_start_body..del_start_body + del_vec_len]
                            } else {
                                &[][..]
                            };
                            if !has_deleted && null_bm.iter().all(|&byte| byte == 0) {
                                col_counts[ci] += n as i64;
                            } else {
                                for i in 0..n {
                                    let deleted =
                                        has_deleted && (del[i / 8] >> (i % 8)) & 1 != 0;
                                    let is_null = (null_bm
                                        .get(i / 8)
                                        .copied()
                                        .unwrap_or(0)
                                        >> (i % 8))
                                        & 1
                                        != 0;
                                    if !deleted && !is_null {
                                        col_counts[ci] += 1;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    const BITPACK: u8 = 2u8;
                    let null_bm = &body[col_off..col_off + null_bitmap_len];
                    let has_col_nulls = null_bm.iter().any(|&byte| byte != 0);
                    // BITPACK Int64: accumulate directly without Vec allocation
                    if encoding == BITPACK && matches!(col_type, ColumnType::Int64 | ColumnType::Int8 |
                        ColumnType::Int16 | ColumnType::Int32 | ColumnType::UInt8 | ColumnType::UInt16 |
                        ColumnType::UInt32 | ColumnType::UInt64 | ColumnType::Timestamp | ColumnType::Date)
                        && !has_deleted && !has_col_nulls
                    {
                        col_is_int[ci] = true;
                        if let Some((sum, mn, mx, n, _)) = bitpack_agg_i64(payload) {
                            col_counts[ci] += n as i64;
                            col_sums[ci] += sum as f64;
                            let mnf = mn as f64; let mxf = mx as f64;
                            if mnf < col_mins[ci] { col_mins[ci] = mnf; }
                            if mxf > col_maxs[ci] { col_maxs[ci] = mxf; }
                        }
                    } else {
                        // RLE or other: decode per-RG, accumulate, drop
                        let (col_data, _) = read_column_encoded(&body[data_start..], col_type)?;
                        let deleted = if has_deleted {
                            &body[del_start_body..del_start_body + del_vec_len]
                        } else {
                            &[]
                        };
                        let is_active = |row: usize| {
                            !(has_deleted && (deleted[row / 8] >> (row % 8)) & 1 != 0)
                                && !(has_col_nulls
                                    && (null_bm[row / 8] >> (row % 8)) & 1 != 0)
                        };
                        match &col_data {
                            ColumnData::Int64(v) => { col_is_int[ci] = true; for (row, &x) in v.iter().enumerate() { if is_active(row) { col_counts[ci] += 1; col_sums[ci] += x as f64; let xf = x as f64; if xf < col_mins[ci] { col_mins[ci] = xf; } if xf > col_maxs[ci] { col_maxs[ci] = xf; } } } }
                            ColumnData::Float64(v) => { for (row, &x) in v.iter().enumerate() { if is_active(row) { col_counts[ci] += 1; col_sums[ci] += x; if x < col_mins[ci] { col_mins[ci] = x; } if x > col_maxs[ci] { col_maxs[ci] = x; } } } }
                            _ => { col_counts[ci] += (0..col_data.len()).filter(|&row| is_active(row)).count() as i64; }
                        }
                    }
                }
            }
        }

        drop(mmap_guard);
        drop(file_guard);

        // Build results
        let mut results: Vec<(i64, f64, f64, f64, bool)> = Vec::with_capacity(agg_cols.len());
        let mut ci = 0usize;
        for &col_name in agg_cols {
            if col_name == "*" || col_name == "1" {
                results.push((total_active, 0.0, 0.0, 0.0, false));
            } else if ci < nc {
                let mn = if col_mins[ci] == f64::INFINITY { 0.0 } else { col_mins[ci] };
                let mx = if col_maxs[ci] == f64::NEG_INFINITY { 0.0 } else { col_maxs[ci] };
                results.push((col_counts[ci], col_sums[ci], mn, mx, col_is_int[ci]));
                ci += 1;
            } else {
                results.push((0, 0.0, 0.0, 0.0, false));
            }
        }
        if !has_any_deletes && !self.has_pending_deltas() {
            let mut sidecar = self.try_read_col_stats_sidecar().unwrap_or_default();
            for (&column, &stats) in agg_cols.iter().zip(&results) {
                if column != "*"
                    && column != "1"
                    && !column
                        .chars()
                        .next()
                        .is_some_and(|character| character.is_ascii_digit())
                {
                    sidecar.insert(column.to_string(), stats);
                }
            }
            if !sidecar.is_empty() {
                let _ = self.write_col_stats_map_sidecar(schema, &sidecar);
            }
        }
        Ok(Some(results))
    }

    /// Single-pass filtered string aggregation: scan string column and aggregate
    /// numeric columns in one sequential pass per row group, avoiding random-access reads.
    /// Returns (count, sum, min, max, is_int) per agg_col.
    pub fn execute_filtered_string_agg_mmap(
        &self,
        filter_col: &str,
        target: &str,
        agg_cols: &[&str],
    ) -> io::Result<Option<Vec<(i64, f64, f64, f64, bool)>>> {
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let filter_idx = match schema.get_index(filter_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let filter_type = schema.columns[filter_idx].1;
        if !matches!(filter_type, ColumnType::String | ColumnType::StringDict) { return Ok(None); }

        let agg_indices: Vec<(usize, bool)> = agg_cols.iter()
            .filter(|&&n| n != "*" && n != "1")
            .filter_map(|&n| schema.get_index(n).map(|i| (i, false)))
            .collect();
        // Must resolve all requested agg columns
        let non_star_count = agg_cols.iter().filter(|&&n| n != "*" && n != "1").count();
        if agg_indices.len() != non_star_count { return Ok(None); }

        let file_guard = self.file.read();
        let file = file_guard.as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "File not open"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let target_bytes = target.as_bytes();
        let memmem_finder = memchr::memmem::Finder::new(target_bytes);

        let nc = agg_indices.len();
        let mut col_counts = vec![0i64; nc];
        let mut col_sums   = vec![0.0f64; nc];
        let mut col_mins   = vec![f64::INFINITY; nc];
        let mut col_maxs   = vec![f64::NEG_INFINITY; nc];
        let mut col_is_int = vec![false; nc];
        let mut match_count = 0i64;

        if matches!(filter_type, ColumnType::StringDict) && nc <= 1 && footer.row_groups.len() > 1
        {
            use rayon::prelude::*;

            #[derive(Clone, Copy)]
            struct Part {
                matches: i64,
                count: i64,
                sum: f64,
                min: f64,
                max: f64,
                is_int: bool,
            }

            impl Part {
                #[inline]
                fn empty() -> Self {
                    Self {
                        matches: 0,
                        count: 0,
                        sum: 0.0,
                        min: f64::INFINITY,
                        max: f64::NEG_INFINITY,
                        is_int: false,
                    }
                }

                #[inline]
                fn add_f64(&mut self, v: f64) {
                    self.count += 1;
                    self.sum += v;
                    if v < self.min {
                        self.min = v;
                    }
                    if v > self.max {
                        self.max = v;
                    }
                }
            }

            let mmap_ptr = mmap_ref.as_ptr() as usize;
            let mmap_len = mmap_ref.len();
            let parts: Option<Vec<Part>> = footer
                .row_groups
                .par_iter()
                .enumerate()
                .map(|(rg_i, rg_meta)| {
                    let mut part = Part::empty();
                    let rg_rows = rg_meta.row_count as usize;
                    if rg_rows == 0 {
                        return Some(part);
                    }

                    let mmap =
                        unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                    let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
                    if rg_end > mmap.len() || rg_end < rg_meta.offset as usize + 32 {
                        return None;
                    }
                    let rg_bytes = &mmap[rg_meta.offset as usize..rg_end];
                    let compress_flag = rg_bytes[28];
                    let encoding_version = rg_bytes[29];
                    if compress_flag != RG_COMPRESS_NONE || encoding_version < 1 {
                        return None;
                    }

                    let rcix = footer.col_offsets.get(rg_i)?;
                    if rcix.len() <= filter_idx
                        || agg_indices.iter().any(|&(ci, _)| rcix.len() <= ci)
                    {
                        return None;
                    }

                    let body = &rg_bytes[32..];
                    let null_bitmap_len = (rg_rows + 7) / 8;
                    let del_start = rg_id_section_len(
                        rg_rows,
                        rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
                    );
                    let del_len = null_bitmap_len;
                    if del_start + del_len > body.len() {
                        return None;
                    }
                    let del_bytes = &body[del_start..del_start + del_len];
                    let has_deletes = rg_meta.deletion_count > 0;

                    let filter_col_off = rcix[filter_idx] as usize;
                    if filter_col_off + null_bitmap_len >= body.len() {
                        return None;
                    }
                    let filter_nulls = &body[filter_col_off..filter_col_off + null_bitmap_len];
                    let filter_encoding = body[filter_col_off + null_bitmap_len];
                    if !matches!(
                        filter_encoding,
                        COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                    ) {
                        return None;
                    }
                    let filter_payload = &body[filter_col_off + null_bitmap_len + 1..];
                    let view = StringDictView::parse(
                        filter_payload,
                        filter_encoding == COL_ENCODING_COMPACT_DICTIONARY,
                    ).ok()?;
                    if view.dict_size <= 1 {
                        return Some(part);
                    }
                    let Some(tdi) = view.find_value(target_bytes) else {
                        return Some(part);
                    };

                    let n = view.row_count.min(rg_rows);
                    let filter_has_nulls = filter_nulls.iter().any(|&b| b != 0);

                    if nc == 0 {
                        view.for_each_index_eq(tdi, n, |i| {
                            if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                return true;
                            }
                            if filter_has_nulls && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                return true;
                            }
                            part.matches += 1;
                            true
                        });
                        return Some(part);
                    }

                    let (agg_col_idx, _) = agg_indices[0];
                    let agg_col_off = rcix[agg_col_idx] as usize;
                    if agg_col_off + null_bitmap_len >= body.len() {
                        return None;
                    }
                    let agg_nulls = &body[agg_col_off..agg_col_off + null_bitmap_len];
                    let agg_encoding = body[agg_col_off + null_bitmap_len];
                    if agg_encoding != COL_ENCODING_PLAIN {
                        return None;
                    }
                    let agg_payload = &body[agg_col_off + null_bitmap_len + 1..];
                    if agg_payload.len() < 8 {
                        return None;
                    }
                    let agg_count =
                        u64::from_le_bytes(agg_payload[0..8].try_into().ok()?) as usize;
                    let agg_n = agg_count.min(rg_rows).min((agg_payload.len() - 8) / 8);
                    let scan_n = n.min(agg_n);
                    let agg_values = &agg_payload[8..];
                    let agg_has_nulls = agg_nulls.iter().any(|&b| b != 0);
                    let agg_type = schema.columns[agg_col_idx].1;

                    match agg_type {
                        ColumnType::Float64 | ColumnType::Float32 => {
                            view.for_each_index_eq(tdi, scan_n, |i| {
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    return true;
                                }
                                if filter_has_nulls
                                    && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1
                                {
                                    return true;
                                }
                                part.matches += 1;
                                if agg_has_nulls && (agg_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                    return true;
                                }
                                let off = i * 8;
                                let v = f64::from_le_bytes(
                                    agg_values[off..off + 8].try_into().unwrap(),
                                );
                                part.add_f64(v);
                                true
                            });
                        }
                        ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date => {
                            part.is_int = true;
                            view.for_each_index_eq(tdi, scan_n, |i| {
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    return true;
                                }
                                if filter_has_nulls
                                    && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1
                                {
                                    return true;
                                }
                                part.matches += 1;
                                if agg_has_nulls && (agg_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                    return true;
                                }
                                let off = i * 8;
                                let v = i64::from_le_bytes(
                                    agg_values[off..off + 8].try_into().unwrap(),
                                ) as f64;
                                part.add_f64(v);
                                true
                            });
                        }
                        _ => return None,
                    }

                    Some(part)
                })
                .collect();

            if let Some(parts) = parts {
                let mut total = Part::empty();
                for part in parts {
                    total.matches += part.matches;
                    total.count += part.count;
                    total.sum += part.sum;
                    if part.min < total.min {
                        total.min = part.min;
                    }
                    if part.max > total.max {
                        total.max = part.max;
                    }
                    total.is_int |= part.is_int;
                }

                let mut results = Vec::with_capacity(agg_cols.len());
                let mut ci = 0usize;
                for &col_name in agg_cols {
                    if col_name == "*" || col_name == "1" {
                        results.push((total.matches, 0.0, 0.0, 0.0, false));
                    } else if ci < nc {
                        let mn = if total.min == f64::INFINITY {
                            0.0
                        } else {
                            total.min
                        };
                        let mx = if total.max == f64::NEG_INFINITY {
                            0.0
                        } else {
                            total.max
                        };
                        results.push((total.count, total.sum, mn, mx, total.is_int));
                        ci += 1;
                    } else {
                        results.push((0, 0.0, 0.0, 0.0, false));
                    }
                }
                return Ok(Some(results));
            }
        }

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 { continue; }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() { continue; }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.len() < 32 { continue; }

            let compress_flag = rg_bytes[28];
            let encoding_version = rg_bytes[29];
            let null_bitmap_len = (rg_rows + 7) / 8;
            let del_vec_len = null_bitmap_len;
            let del_start_body = rg_id_section_len(
                rg_rows,
                rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
            );

            // Need RCIX for filter column AND all agg columns
            let rcix = if compress_flag == RG_COMPRESS_NONE && encoding_version >= 1 {
                footer.col_offsets.get(rg_i).filter(|v| {
                    v.len() > filter_idx && agg_indices.iter().all(|&(ci, _)| v.len() > ci)
                })
            } else { None };

            let body = &rg_bytes[32..];
            let has_deletes = del_start_body + del_vec_len <= body.len()
                && body[del_start_body..del_start_body + del_vec_len].iter().any(|&b| b != 0);

            if let Some(rcix) = rcix {
                // ── RCIX FAST PATH: uncompressed, direct column access ──
                // Step 1: Find matching rows in string column
                let filter_col_off = rcix[filter_idx] as usize;
                if filter_col_off + null_bitmap_len > body.len() { continue; }
                let filter_data_start = filter_col_off + null_bitmap_len + 1; // +1 for encoding byte
                if filter_data_start > body.len() { continue; }
                let filter_encoding = body[filter_col_off + null_bitmap_len];
                let filter_payload = &body[filter_data_start..];

                // Find matching local row indices
                let matching_local: Vec<usize> = if filter_encoding == COL_ENCODING_PLAIN
                    && matches!(filter_type, ColumnType::String) && filter_payload.len() >= 8
                {
                    let count = u64::from_le_bytes(filter_payload[0..8].try_into().unwrap()) as usize;
                    let all_offsets_len = (count + 1) * 4;
                    if 8 + all_offsets_len <= filter_payload.len() {
                        let data_len_off = 8 + all_offsets_len;
                        if data_len_off + 8 <= filter_payload.len() {
                            let data_start = data_len_off + 8;
                            let offsets = bytes_as_u32_slice(&filter_payload[8..], count + 1);
                            let data_len_val = u64::from_le_bytes(
                                filter_payload[data_len_off..data_len_off+8].try_into().unwrap_or([0;8])
                            ) as usize;
                            let raw_end = (data_start + data_len_val).min(filter_payload.len());
                            let raw_str = &filter_payload[data_start..raw_end];
                            let tlen = target_bytes.len();
                            let n = count.min(rg_rows);
                            let mut result = Vec::new();
                            // memmem scan + binary search
                            let mut search_from = 0usize;
                            while let Some(rel) = memmem_finder.find(&raw_str[search_from..]) {
                                let abs = search_from + rel;
                                if let Ok(di) = offsets[..count].binary_search(&(abs as u32)) {
                                    let end_off = offsets[di + 1] as usize;
                                    if end_off - abs == tlen && di < n {
                                        if !has_deletes || (body[del_start_body + di/8] >> (di%8)) & 1 == 0 {
                                            let null_byte = body.get(filter_col_off + di/8).copied().unwrap_or(0);
                                            if (null_byte >> (di%8)) & 1 == 0 {
                                                result.push(di);
                                            }
                                        }
                                    }
                                }
                                search_from += rel + 1;
                                if search_from >= raw_str.len() { break; }
                            }
                            result
                        } else { vec![] }
                    } else { vec![] }
                } else if matches!(
                    filter_encoding,
                    COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                ) && matches!(filter_type, ColumnType::StringDict)
                {
                    if let Ok(view) = StringDictView::parse(
                        filter_payload,
                        filter_encoding == COL_ENCODING_COMPACT_DICTIONARY,
                    ) {
                        if let Some(tdi) = view.find_value(target_bytes) {
                                if nc <= 1 {
                                    let n = view.row_count.min(rg_rows);
                                    if nc == 0 {
                                        for i in 0..n {
                                            if view.index(i) == Some(tdi) {
                                                if has_deletes
                                                    && (body[del_start_body + i / 8] >> (i % 8))
                                                        & 1
                                                        == 1
                                                {
                                                    continue;
                                                }
                                                let null_byte = body
                                                    .get(filter_col_off + i / 8)
                                                    .copied()
                                                    .unwrap_or(0);
                                                if (null_byte >> (i % 8)) & 1 == 0 {
                                                    match_count += 1;
                                                }
                                            }
                                        }
                                        continue;
                                    }

                                    let (agg_col_idx, _) = agg_indices[0];
                                    let agg_col_off = rcix[agg_col_idx] as usize;
                                    if agg_col_off + null_bitmap_len <= body.len() {
                                        let agg_null =
                                            &body[agg_col_off..agg_col_off + null_bitmap_len];
                                        let agg_data_start = agg_col_off + null_bitmap_len + 1;
                                        if agg_data_start <= body.len() {
                                            let agg_encoding = body[agg_col_off + null_bitmap_len];
                                            let agg_payload = &body[agg_data_start..];
                                            let agg_type = schema.columns[agg_col_idx].1;
                                            if agg_encoding == COL_ENCODING_PLAIN
                                                && agg_payload.len() >= 8
                                            {
                                                let agg_count = u64::from_le_bytes(
                                                    agg_payload[0..8].try_into().unwrap(),
                                                )
                                                    as usize;
                                                let agg_n =
                                                    agg_count.min(rg_rows).min((agg_payload.len() - 8) / 8);
                                                let n = n.min(agg_n);
                                                if matches!(
                                                    agg_type,
                                                    ColumnType::Float64 | ColumnType::Float32
                                                ) {
                                                    let vals =
                                                        bytes_as_f64_slice(&agg_payload[8..], agg_n);
                                                    for i in 0..n {
                                                        if view.index(i) != Some(tdi) {
                                                            continue;
                                                        }
                                                        if has_deletes
                                                            && (body[del_start_body + i / 8]
                                                                >> (i % 8))
                                                                & 1
                                                                == 1
                                                        {
                                                            continue;
                                                        }
                                                        let filter_null = body
                                                            .get(filter_col_off + i / 8)
                                                            .copied()
                                                            .unwrap_or(0);
                                                        if (filter_null >> (i % 8)) & 1 == 1 {
                                                            continue;
                                                        }
                                                        match_count += 1;
                                                        if (agg_null[i / 8] >> (i % 8)) & 1 == 1 {
                                                            continue;
                                                        }
                                                        let v = vals[i];
                                                        col_counts[0] += 1;
                                                        col_sums[0] += v;
                                                        if v < col_mins[0] {
                                                            col_mins[0] = v;
                                                        }
                                                        if v > col_maxs[0] {
                                                            col_maxs[0] = v;
                                                        }
                                                    }
                                                    continue;
                                                } else if matches!(
                                                    agg_type,
                                                    ColumnType::Int64
                                                        | ColumnType::Int8
                                                        | ColumnType::Int16
                                                        | ColumnType::Int32
                                                        | ColumnType::UInt8
                                                        | ColumnType::UInt16
                                                        | ColumnType::UInt32
                                                        | ColumnType::UInt64
                                                        | ColumnType::Timestamp
                                                        | ColumnType::Date
                                                ) {
                                                    col_is_int[0] = true;
                                                    let vals =
                                                        bytes_as_i64_slice(&agg_payload[8..], agg_n);
                                                    for i in 0..n {
                                                        if view.index(i) != Some(tdi) {
                                                            continue;
                                                        }
                                                        if has_deletes
                                                            && (body[del_start_body + i / 8]
                                                                >> (i % 8))
                                                                & 1
                                                                == 1
                                                        {
                                                            continue;
                                                        }
                                                        let filter_null = body
                                                            .get(filter_col_off + i / 8)
                                                            .copied()
                                                            .unwrap_or(0);
                                                        if (filter_null >> (i % 8)) & 1 == 1 {
                                                            continue;
                                                        }
                                                        match_count += 1;
                                                        if (agg_null[i / 8] >> (i % 8)) & 1 == 1 {
                                                            continue;
                                                        }
                                                        let v = vals[i];
                                                        let vf = v as f64;
                                                        col_counts[0] += 1;
                                                        col_sums[0] += vf;
                                                        if vf < col_mins[0] {
                                                            col_mins[0] = vf;
                                                        }
                                                        if vf > col_maxs[0] {
                                                            col_maxs[0] = vf;
                                                        }
                                                    }
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                }

                                let n = view.row_count.min(rg_rows);
                                let mut result = Vec::new();
                                for i in 0..n {
                                    if view.index(i) == Some(tdi) {
                                        if !has_deletes || (body[del_start_body + i/8] >> (i%8)) & 1 == 0 {
                                            let null_byte = body.get(filter_col_off + i/8).copied().unwrap_or(0);
                                            if (null_byte >> (i%8)) & 1 == 0 {
                                                result.push(i);
                                            }
                                        }
                                    }
                                }
                                result
                            } else { vec![] }
                        } else { vec![] }
                } else {
                    vec![]
                };

                if matching_local.is_empty() { continue; }
                match_count += matching_local.len() as i64;

                // Step 2: For each agg column, read values at matching indices and accumulate
                for (ci, &(col_idx, _)) in agg_indices.iter().enumerate() {
                    let col_off = rcix[col_idx] as usize;
                    if col_off + null_bitmap_len > body.len() { continue; }
                    let null_bm = &body[col_off..col_off + null_bitmap_len];
                    let col_type = schema.columns[col_idx].1;
                    let data_start = col_off + null_bitmap_len + 1;
                    if data_start > body.len() { continue; }
                    let encoding = body[col_off + null_bitmap_len];
                    let payload = &body[data_start..];

                    if encoding == COL_ENCODING_PLAIN && payload.len() >= 8 {
                        let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                        let raw = &payload[8..];
                        match col_type {
                            ColumnType::Float64 | ColumnType::Float32 => {
                                let n = count.min(rg_rows).min(raw.len() / 8);
                                // Zero-copy: cast raw bytes to f64 slice
                                let vals = bytes_as_f64_slice(raw, n);
                                for &local_i in &matching_local {
                                    if local_i < n && (null_bm[local_i/8] >> (local_i%8)) & 1 == 0 {
                                        let v = vals[local_i];
                                        col_counts[ci] += 1; col_sums[ci] += v;
                                        if v < col_mins[ci] { col_mins[ci] = v; }
                                        if v > col_maxs[ci] { col_maxs[ci] = v; }
                                    }
                                }
                            }
                            ColumnType::Int64 | ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 |
                            ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64 |
                            ColumnType::Timestamp | ColumnType::Date => {
                                col_is_int[ci] = true;
                                let n = count.min(rg_rows).min(raw.len() / 8);
                                let vals = bytes_as_i64_slice(raw, n);
                                for &local_i in &matching_local {
                                    if local_i < n && (null_bm[local_i/8] >> (local_i%8)) & 1 == 0 {
                                        let v = vals[local_i];
                                        col_counts[ci] += 1; col_sums[ci] += v as f64;
                                        let vf = v as f64;
                                        if vf < col_mins[ci] { col_mins[ci] = vf; }
                                        if vf > col_maxs[ci] { col_maxs[ci] = vf; }
                                    }
                                }
                            }
                            _ => {
                                // Non-numeric: just count
                                let n = count.min(rg_rows);
                                for &local_i in &matching_local {
                                    if local_i < n && (null_bm[local_i/8] >> (local_i%8)) & 1 == 0 {
                                        col_counts[ci] += 1;
                                    }
                                }
                            }
                        }
                    } else if encoding == COL_ENCODING_BITPACK && matches!(col_type,
                        ColumnType::Int64 | ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 |
                        ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64 |
                        ColumnType::Timestamp | ColumnType::Date)
                    {
                        col_is_int[ci] = true;
                        if let Some((sum, mn, mx, n, _)) = bitpack_agg_i64(payload) {
                            // Bitpack doesn't support random access; decode all and pick matching rows
                            // Fall back to full decode for this column
                            let (col_data, _) = read_column_encoded(payload, col_type)?;
                            if let ColumnData::Int64(v) = &col_data {
                                let _ = (sum, mn, mx, n); // unused
                                for &local_i in &matching_local {
                                    if local_i < v.len() && (null_bm[local_i/8] >> (local_i%8)) & 1 == 0 {
                                        let val = v[local_i];
                                        col_counts[ci] += 1; col_sums[ci] += val as f64;
                                        let vf = val as f64;
                                        if vf < col_mins[ci] { col_mins[ci] = vf; }
                                        if vf > col_maxs[ci] { col_maxs[ci] = vf; }
                                    }
                                }
                            }
                        }
                    } else {
                        // RLE or other encoding: decode and pick matching rows
                        let (col_data, _) = read_column_encoded(payload, col_type)?;
                        match &col_data {
                            ColumnData::Int64(v) => {
                                col_is_int[ci] = true;
                                for &local_i in &matching_local {
                                    if local_i < v.len() && (null_bm[local_i/8] >> (local_i%8)) & 1 == 0 {
                                        let val = v[local_i];
                                        col_counts[ci] += 1; col_sums[ci] += val as f64;
                                        let vf = val as f64;
                                        if vf < col_mins[ci] { col_mins[ci] = vf; }
                                        if vf > col_maxs[ci] { col_maxs[ci] = vf; }
                                    }
                                }
                            }
                            ColumnData::Float64(v) => {
                                for &local_i in &matching_local {
                                    if local_i < v.len() && (null_bm[local_i/8] >> (local_i%8)) & 1 == 0 {
                                        let val = v[local_i];
                                        col_counts[ci] += 1; col_sums[ci] += val;
                                        if val < col_mins[ci] { col_mins[ci] = val; }
                                        if val > col_maxs[ci] { col_maxs[ci] = val; }
                                    }
                                }
                            }
                            _ => {
                                for &local_i in &matching_local {
                                    if local_i < col_data.len() {
                                        col_counts[ci] += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // ── FALLBACK: compressed/pre-RCIX — decode columns sequentially ──
                let decompressed = decompress_rg_body(compress_flag, body)?;
                let body: &[u8] = decompressed.as_deref().unwrap_or(body);

                // Find matching rows in filter column
                let filter_col_idx = filter_idx;
                let mut matching_local: Vec<usize> = Vec::new();
                let mut pos = del_start_body + del_vec_len;
                for ci in 0..schema.column_count() {
                    if pos + null_bitmap_len > body.len() { break; }
                    let null_bytes = &body[pos..pos + null_bitmap_len];
                    pos += null_bitmap_len;
                    let ct = schema.columns[ci].1;
                    if ci == filter_col_idx {
                        let col_bytes = &body[pos..];
                        let enc_offset = if encoding_version >= 1 { 1 } else { 0 };
                        let encoding = if encoding_version >= 1 && !col_bytes.is_empty() { col_bytes[0] } else { COL_ENCODING_PLAIN };
                        let data = if enc_offset <= col_bytes.len() { &col_bytes[enc_offset..] } else { &[] };
                        if encoding == COL_ENCODING_PLAIN && matches!(ct, ColumnType::StringDict) && data.len() >= 16 {
                            let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
                            let dict_size = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
                            if dict_size > 0 {
                                let dict_off_start = 16 + count * 4;
                                let dict_data_len_off = dict_off_start + dict_size * 4;
                                if dict_data_len_off + 8 <= data.len() {
                                    let dict_data_len = u64::from_le_bytes(data[dict_data_len_off..dict_data_len_off+8].try_into().unwrap_or([0;8])) as usize;
                                    let dict_data_start = dict_data_len_off + 8;
                                    let dict_offsets = bytes_as_u32_slice(&data[dict_off_start..], dict_size);
                                    let indices = bytes_as_u32_slice(&data[16..], count);
                                    let tlen = target_bytes.len();
                                    let mut tdi: Option<u32> = None;
                                    for di in 0..dict_size {
                                        let ds = dict_offsets[di] as usize;
                                        let de = if di + 1 < dict_size { dict_offsets[di+1] as usize } else { dict_data_len };
                                        if de - ds == tlen && dict_data_start + de <= data.len() && &data[dict_data_start + ds..dict_data_start + de] == target_bytes {
                                            tdi = Some((di + 1) as u32); break;
                                        }
                                    }
                                    if let Some(tdi) = tdi {
                                        let n = count.min(rg_rows);
                                        for i in 0..n {
                                            if indices[i] == tdi {
                                                if !has_deletes || (body[del_start_body + i/8] >> (i%8)) & 1 == 0 {
                                                    if (null_bytes[i/8] >> (i%8)) & 1 == 0 {
                                                        matching_local.push(i);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // Advance pos past column data
                    let col_bytes = &body[pos..];
                    let enc_offset = if encoding_version >= 1 { 1 } else { 0 };
                    let encoding = if encoding_version >= 1 && !col_bytes.is_empty() { col_bytes[0] } else { COL_ENCODING_PLAIN };
                    let data = if enc_offset <= col_bytes.len() { &col_bytes[enc_offset..] } else { &[] };
                    if encoding == COL_ENCODING_PLAIN && data.len() >= 8 {
                        let count = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0;8])) as usize;
                        match ct {
                            ColumnType::String => {
                                let all_offsets_len = (count + 1) * 4;
                                if 8 + all_offsets_len + 8 <= data.len() {
                                    let data_len = u64::from_le_bytes(data[8 + all_offsets_len..8 + all_offsets_len + 8].try_into().unwrap_or([0;8])) as usize;
                                    pos += enc_offset + 8 + all_offsets_len + 8 + data_len;
                                } else { pos += col_bytes.len(); }
                            }
                            ColumnType::StringDict => {
                                if data.len() >= 16 {
                                    let row_count = count;
                                    let dict_size = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0;8])) as usize;
                                    let indices_len = row_count * 4;
                                    let dict_offsets_len = dict_size * 4;
                                    if 16 + indices_len + dict_offsets_len + 8 <= data.len() {
                                        let dict_data_len = u64::from_le_bytes(data[16 + indices_len + dict_offsets_len..16 + indices_len + dict_offsets_len + 8].try_into().unwrap_or([0;8])) as usize;
                                        pos += enc_offset + 16 + indices_len + dict_offsets_len + 8 + dict_data_len;
                                    } else { pos += col_bytes.len(); }
                                } else { pos += col_bytes.len(); }
                            }
                            _ => {
                                let n = count.min(rg_rows);
                                pos += enc_offset + 8 + n * 8;
                            }
                        }
                    } else {
                        // Non-plain: skip entire remaining body for this column
                        pos += col_bytes.len();
                    }
                }

                if matching_local.is_empty() { continue; }
                match_count += matching_local.len() as i64;

                // Now decode agg columns and accumulate
                // Re-scan to find agg column positions
                for (ci, &(col_idx, _)) in agg_indices.iter().enumerate() {
                    {
                        // Find column data offset in body
                        let mut pos2 = del_start_body + del_vec_len;
                        let mut found = None;
                        for cii in 0..schema.column_count() {
                            if pos2 + null_bitmap_len > body.len() { break; }
                            let null_bytes = &body[pos2..pos2 + null_bitmap_len];
                            pos2 += null_bitmap_len;
                            let ct = schema.columns[cii].1;
                            let col_bytes = &body[pos2..];
                            let enc_offset = if encoding_version >= 1 { 1 } else { 0 };
                            let encoding = if encoding_version >= 1 && !col_bytes.is_empty() { col_bytes[0] } else { COL_ENCODING_PLAIN };
                            let data = if enc_offset <= col_bytes.len() { &col_bytes[enc_offset..] } else { &[] };

                            if cii == col_idx {
                                let null_bm = null_bytes;
                                if encoding == COL_ENCODING_PLAIN && data.len() >= 8 {
                                    let count = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0;8])) as usize;
                                    let raw = &data[8..];
                                    match ct {
                                        ColumnType::Int64 | ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 |
                                        ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64 |
                                        ColumnType::Timestamp | ColumnType::Date => {
                                            col_is_int[ci] = true;
                                            let n = count.min(rg_rows).min(raw.len() / 8);
                                            let vals = bytes_as_i64_slice(raw, n);
                                            for &local_i in &matching_local {
                                                if local_i < n && (null_bm[local_i/8] >> (local_i%8)) & 1 == 0 {
                                                    let v = vals[local_i];
                                                    col_counts[ci] += 1; col_sums[ci] += v as f64;
                                                    let vf = v as f64;
                                                    if vf < col_mins[ci] { col_mins[ci] = vf; }
                                                    if vf > col_maxs[ci] { col_maxs[ci] = vf; }
                                                }
                                            }
                                        }
                                        ColumnType::Float64 | ColumnType::Float32 => {
                                            let n = count.min(rg_rows).min(raw.len() / 8);
                                            let vals = bytes_as_f64_slice(raw, n);
                                            for &local_i in &matching_local {
                                                if local_i < n && (null_bm[local_i/8] >> (local_i%8)) & 1 == 0 {
                                                    let v = vals[local_i];
                                                    col_counts[ci] += 1; col_sums[ci] += v;
                                                    if v < col_mins[ci] { col_mins[ci] = v; }
                                                    if v > col_maxs[ci] { col_maxs[ci] = v; }
                                                }
                                            }
                                        }
                                        _ => {
                                            let n = count.min(rg_rows);
                                            for &local_i in &matching_local {
                                                if local_i < n && (null_bm[local_i/8] >> (local_i%8)) & 1 == 0 {
                                                    col_counts[ci] += 1;
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    // Non-plain encoding: decode fully
                                    let (cd, _) = read_column_encoded(&body[pos2 + null_bitmap_len..], ct)?;
                                    match &cd {
                                        ColumnData::Int64(v) => {
                                            col_is_int[ci] = true;
                                            for &local_i in &matching_local {
                                                if local_i < v.len() && (null_bm[local_i/8] >> (local_i%8)) & 1 == 0 {
                                                    let val = v[local_i];
                                                    col_counts[ci] += 1; col_sums[ci] += val as f64;
                                                    let vf = val as f64;
                                                    if vf < col_mins[ci] { col_mins[ci] = vf; }
                                                    if vf > col_maxs[ci] { col_maxs[ci] = vf; }
                                                }
                                            }
                                        }
                                        ColumnData::Float64(v) => {
                                            for &local_i in &matching_local {
                                                if local_i < v.len() && (null_bm[local_i/8] >> (local_i%8)) & 1 == 0 {
                                                    let val = v[local_i];
                                                    col_counts[ci] += 1; col_sums[ci] += val;
                                                    if val < col_mins[ci] { col_mins[ci] = val; }
                                                    if val > col_maxs[ci] { col_maxs[ci] = val; }
                                                }
                                            }
                                        }
                                        _ => {
                                            for &local_i in &matching_local {
                                                if local_i < cd.len() { col_counts[ci] += 1; }
                                            }
                                        }
                                    }
                                }
                                found = Some(());
                                break;
                            }

                            // Advance past column data
                            if encoding == COL_ENCODING_PLAIN && data.len() >= 8 {
                                let count = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0;8])) as usize;
                                match ct {
                                    ColumnType::String => {
                                        let all_offsets_len = (count + 1) * 4;
                                        if 8 + all_offsets_len + 8 <= data.len() {
                                            let data_len = u64::from_le_bytes(data[8 + all_offsets_len..8 + all_offsets_len + 8].try_into().unwrap_or([0;8])) as usize;
                                            pos2 += enc_offset + 8 + all_offsets_len + 8 + data_len;
                                        } else { pos2 += col_bytes.len(); }
                                    }
                                    ColumnType::StringDict => {
                                        if data.len() >= 16 {
                                            let row_count = count;
                                            let dict_size = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0;8])) as usize;
                                            let indices_len = row_count * 4;
                                            let dict_offsets_len = dict_size * 4;
                                            if 16 + indices_len + dict_offsets_len + 8 <= data.len() {
                                                let dict_data_len = u64::from_le_bytes(data[16 + indices_len + dict_offsets_len..16 + indices_len + dict_offsets_len + 8].try_into().unwrap_or([0;8])) as usize;
                                                pos2 += enc_offset + 16 + indices_len + dict_offsets_len + 8 + dict_data_len;
                                            } else { pos2 += col_bytes.len(); }
                                        } else { pos2 += col_bytes.len(); }
                                    }
                                    _ => {
                                        let n = count.min(rg_rows);
                                        pos2 += enc_offset + 8 + n * 8;
                                    }
                                }
                            } else {
                                pos2 += col_bytes.len();
                            }
                        }
                        found
                    };
                }
            }
        }

        drop(mmap_guard);
        drop(file_guard);

        // Build results
        let mut results: Vec<(i64, f64, f64, f64, bool)> = Vec::with_capacity(agg_cols.len());
        let mut ci = 0usize;
        for &col_name in agg_cols {
            if col_name == "*" || col_name == "1" {
                results.push((match_count, 0.0, 0.0, 0.0, false));
            } else if ci < nc {
                let mn = if col_mins[ci] == f64::INFINITY { 0.0 } else { col_mins[ci] };
                let mx = if col_maxs[ci] == f64::NEG_INFINITY { 0.0 } else { col_maxs[ci] };
                results.push((col_counts[ci], col_sums[ci], mn, mx, col_is_int[ci]));
                ci += 1;
            } else {
                results.push((0, 0.0, 0.0, 0.0, false));
            }
        }
        Ok(Some(results))
    }

    /// Fast filtered aggregation using a pre-built string dictionary cache.
    /// This avoids reparsing the filter string column when earlier GROUP BY or
    /// equality paths have already materialized row-wise group ids.
    pub fn execute_filtered_string_agg_cached(
        &self,
        filter_col: &str,
        dict_strings: &[String],
        group_ids: &[u16],
        target: &str,
        agg_cols: &[&str],
    ) -> io::Result<Option<Vec<(i64, f64, f64, f64, bool)>>> {
        if dict_strings.is_empty() || group_ids.is_empty() || self.column_has_nulls(filter_col) {
            return Ok(None);
        }
        let target_gid = match dict_strings.iter().position(|value| value == target) {
            Some(index) => index as u16,
            None => {
                return Ok(Some(
                    agg_cols
                        .iter()
                        .map(|_| (0, 0.0, 0.0, 0.0, false))
                        .collect(),
                ));
            }
        };

        let real_cols: Vec<&str> = agg_cols
            .iter()
            .copied()
            .filter(|name| *name != "*" && *name != "1")
            .collect();
        for col_name in &real_cols {
            if self.column_has_nulls(col_name) {
                return Ok(None);
            }
        }

        if real_cols.len() == 1 && !self.has_pending_deltas() {
            let direct = self
                .execute_filtered_cached_numeric_mmap(
                    group_ids,
                    target_gid,
                    None,
                    real_cols[0],
                )?;
            let stats = match direct {
                Some(stats) => Some(stats),
                None => self.execute_filtered_cached_numeric_decoded(
                    group_ids,
                    target_gid,
                    None,
                    real_cols[0],
                )?,
            };
            if let Some((count, sum, min, max, is_int)) = stats {
                return Ok(Some(
                    agg_cols
                        .iter()
                        .map(|name| {
                            if *name == "*" || *name == "1" {
                                (count, 0.0, 0.0, 0.0, false)
                            } else {
                                (count, sum, min, max, is_int)
                            }
                        })
                        .collect(),
                ));
            }
        }

        let (scanned_cols, deleted): (Vec<ColumnData>, Vec<u8>) = if self.has_v4_in_memory_data() {
            let schema = self.schema.read();
            let columns = self.columns.read();
            let deleted = self.deleted.read().clone();
            let mut out = Vec::with_capacity(real_cols.len());
            for col_name in &real_cols {
                let Some(col_idx) = schema.get_index(col_name) else {
                    return Ok(None);
                };
                let Some(col) = columns.get(col_idx) else {
                    return Ok(None);
                };
                out.push(col.clone());
            }
            (out, deleted)
        } else if real_cols.is_empty() {
            let footer = match self.get_or_load_footer()? {
                Some(f) => f,
                None => return Ok(None),
            };
            if footer.row_groups.iter().any(|rg| rg.deletion_count > 0) {
                return Ok(None);
            }
            let total_rows: usize = footer.row_groups.iter().map(|rg| rg.row_count as usize).sum();
            (Vec::new(), vec![0; total_rows.div_ceil(8)])
        } else {
            let footer = match self.get_or_load_footer()? {
                Some(f) => f,
                None => return Ok(None),
            };
            let mut col_indices = Vec::with_capacity(real_cols.len());
            for col_name in &real_cols {
                let Some(col_idx) = footer.schema.get_index(col_name) else {
                    return Ok(None);
                };
                col_indices.push(col_idx);
            }
            let (cols, del) = self.scan_columns_mmap(&col_indices, &footer)?;
            (cols, del)
        };

        let has_deleted = deleted.iter().any(|&byte| byte != 0);
        let scan_rows = group_ids.len();
        let mut match_count = 0i64;
        let mut col_counts = vec![0i64; real_cols.len()];
        let mut col_sums = vec![0.0f64; real_cols.len()];
        let mut col_mins = vec![f64::INFINITY; real_cols.len()];
        let mut col_maxs = vec![f64::NEG_INFINITY; real_cols.len()];
        let mut col_is_int = vec![false; real_cols.len()];

        if real_cols.is_empty() {
            for i in 0..scan_rows {
                if has_deleted && i / 8 < deleted.len() && (deleted[i / 8] >> (i % 8)) & 1 != 0 {
                    continue;
                }
                if unsafe { *group_ids.get_unchecked(i) } == target_gid {
                    match_count += 1;
                }
            }
        } else if real_cols.len() == 1 {
            match &scanned_cols[0] {
                ColumnData::Float64(values) => {
                    let limit = scan_rows.min(values.len());
                    for i in 0..limit {
                        if has_deleted
                            && i / 8 < deleted.len()
                            && (deleted[i / 8] >> (i % 8)) & 1 != 0
                        {
                            continue;
                        }
                        if unsafe { *group_ids.get_unchecked(i) } != target_gid {
                            continue;
                        }
                        match_count += 1;
                        let v = unsafe { *values.get_unchecked(i) };
                        col_counts[0] += 1;
                        col_sums[0] += v;
                        if v < col_mins[0] {
                            col_mins[0] = v;
                        }
                        if v > col_maxs[0] {
                            col_maxs[0] = v;
                        }
                    }
                }
                ColumnData::Int64(values) => {
                    col_is_int[0] = true;
                    let limit = scan_rows.min(values.len());
                    for i in 0..limit {
                        if has_deleted
                            && i / 8 < deleted.len()
                            && (deleted[i / 8] >> (i % 8)) & 1 != 0
                        {
                            continue;
                        }
                        if unsafe { *group_ids.get_unchecked(i) } != target_gid {
                            continue;
                        }
                        match_count += 1;
                        let vf = unsafe { *values.get_unchecked(i) } as f64;
                        col_counts[0] += 1;
                        col_sums[0] += vf;
                        if vf < col_mins[0] {
                            col_mins[0] = vf;
                        }
                        if vf > col_maxs[0] {
                            col_maxs[0] = vf;
                        }
                    }
                }
                _ => return Ok(None),
            }
        } else {
            let scan_limit = scanned_cols
                .iter()
                .fold(scan_rows, |limit, col| limit.min(col.len()));
            for i in 0..scan_limit {
                if has_deleted && i / 8 < deleted.len() && (deleted[i / 8] >> (i % 8)) & 1 != 0 {
                    continue;
                }
                if unsafe { *group_ids.get_unchecked(i) } != target_gid {
                    continue;
                }
                match_count += 1;
                for (ci, col) in scanned_cols.iter().enumerate() {
                    match col {
                        ColumnData::Float64(values) => {
                            let v = unsafe { *values.get_unchecked(i) };
                            col_counts[ci] += 1;
                            col_sums[ci] += v;
                            if v < col_mins[ci] {
                                col_mins[ci] = v;
                            }
                            if v > col_maxs[ci] {
                                col_maxs[ci] = v;
                            }
                        }
                        ColumnData::Int64(values) => {
                            col_is_int[ci] = true;
                            let vf = unsafe { *values.get_unchecked(i) } as f64;
                            col_counts[ci] += 1;
                            col_sums[ci] += vf;
                            if vf < col_mins[ci] {
                                col_mins[ci] = vf;
                            }
                            if vf > col_maxs[ci] {
                                col_maxs[ci] = vf;
                            }
                        }
                        _ => return Ok(None),
                    }
                }
            }
        }

        let mut results = Vec::with_capacity(agg_cols.len());
        let mut ci = 0usize;
        for &col_name in agg_cols {
            if col_name == "*" || col_name == "1" {
                results.push((match_count, 0.0, 0.0, 0.0, false));
            } else if ci < real_cols.len() {
                let mn = if col_mins[ci] == f64::INFINITY {
                    0.0
                } else {
                    col_mins[ci]
                };
                let mx = if col_maxs[ci] == f64::NEG_INFINITY {
                    0.0
                } else {
                    col_maxs[ci]
                };
                results.push((col_counts[ci], col_sums[ci], mn, mx, col_is_int[ci]));
                ci += 1;
            } else {
                results.push((0, 0.0, 0.0, 0.0, false));
            }
        }
        Ok(Some(results))
    }

    /// Fast prefix-filtered aggregation over an existing string dictionary.
    /// LIKE `prefix%` is evaluated once per dictionary value, then the compact
    /// row-wise group IDs drive the same parallel numeric scan as equality.
    pub fn execute_filtered_string_prefix_agg_cached(
        &self,
        filter_col: &str,
        dict_strings: &[String],
        group_ids: &[u16],
        prefix: &str,
        agg_cols: &[&str],
    ) -> io::Result<Option<Vec<(i64, f64, f64, f64, bool)>>> {
        if prefix.is_empty()
            || dict_strings.is_empty()
            || group_ids.is_empty()
            || self.column_has_nulls(filter_col)
            || self.has_pending_deltas()
        {
            return Ok(None);
        }
        let matching_groups: Vec<bool> = dict_strings
            .iter()
            .map(|value| value.starts_with(prefix))
            .collect();
        if !matching_groups.iter().any(|&matches| matches) {
            return Ok(Some(
                agg_cols
                    .iter()
                    .map(|_| (0, 0.0, 0.0, 0.0, false))
                    .collect(),
            ));
        }
        let real_cols: Vec<&str> = agg_cols
            .iter()
            .copied()
            .filter(|name| *name != "*" && *name != "1")
            .collect();
        if real_cols.len() != 1 || self.column_has_nulls(real_cols[0]) {
            return Ok(None);
        }
        let direct = self
            .execute_filtered_cached_numeric_mmap(
                group_ids,
                0,
                Some(&matching_groups),
                real_cols[0],
            )?;
        let stats = match direct {
            Some(stats) => Some(stats),
            None => self.execute_filtered_cached_numeric_decoded(
                group_ids,
                0,
                Some(&matching_groups),
                real_cols[0],
            )?,
        };
        let Some((count, sum, min, max, is_int)) = stats else {
            return Ok(None);
        };
        Ok(Some(
            agg_cols
                .iter()
                .map(|name| {
                    if *name == "*" || *name == "1" {
                        (count, 0.0, 0.0, 0.0, false)
                    } else {
                        (count, sum, min, max, is_int)
                    }
                })
                .collect(),
        ))
    }

    /// Zero-copy numeric side of a cached string-equality aggregation.
    /// Group IDs already encode the string predicate, so only the numeric
    /// column needs to be streamed from each row group.
    fn execute_filtered_cached_numeric_mmap(
        &self,
        group_ids: &[u16],
        target_gid: u16,
        target_gids: Option<&[bool]>,
        agg_col: &str,
    ) -> io::Result<Option<(i64, f64, f64, f64, bool)>> {
        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        let Some(col_idx) = footer.schema.get_index(agg_col) else {
            return Ok(None);
        };
        let col_type = footer.schema.columns[col_idx].1;
        let is_int = matches!(
            col_type,
            ColumnType::Int64
                | ColumnType::Int8
                | ColumnType::Int16
                | ColumnType::Int32
                | ColumnType::UInt8
                | ColumnType::UInt16
                | ColumnType::UInt32
                | ColumnType::UInt64
        );
        if !is_int && !matches!(col_type, ColumnType::Float64 | ColumnType::Float32) {
            return Ok(None);
        }
        if footer.row_groups.iter().any(|rg| rg.deletion_count > 0) {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for filtered aggregation"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        #[derive(Clone, Copy)]
        struct NumericRg {
            row_offset: usize,
            rows: usize,
            values: usize,
        }

        let mut row_offset = 0usize;
        let mut row_groups = Vec::with_capacity(footer.row_groups.len());

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            let Some(col_offsets) = footer.col_offsets.get(rg_i) else {
                return Ok(None);
            };
            let Some(&col_offset) = col_offsets.get(col_idx) else {
                return Ok(None);
            };
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("RG extends past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.len() < 32
                || rg_bytes[28] != RG_COMPRESS_NONE
                || rg_bytes[29] < 1
            {
                return Ok(None);
            }
            let body = &rg_bytes[32..];
            let null_bitmap_len = rg_rows.div_ceil(8);
            let data_start = col_offset as usize + null_bitmap_len;
            if data_start + 9 > body.len() || body[data_start] != COL_ENCODING_PLAIN {
                return Ok(None);
            }
            let payload = &body[data_start + 1..];
            let encoded_rows = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
            let rows = encoded_rows
                .min(rg_rows)
                .min(group_ids.len().saturating_sub(row_offset))
                .min((payload.len() - 8) / 8);
            row_groups.push(NumericRg {
                row_offset,
                rows,
                values: unsafe { payload.as_ptr().add(8) } as usize,
            });
            row_offset += rg_rows;
        }

        #[derive(Clone, Copy)]
        struct Part {
            count: i64,
            sum: f64,
            min: f64,
            max: f64,
        }

        let scan = |rg: &NumericRg| {
            let mut part = Part {
                count: 0,
                sum: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
            };
            let values = rg.values as *const u8;
            for local_row in 0..rg.rows {
                let group_id =
                    unsafe { *group_ids.get_unchecked(rg.row_offset + local_row) };
                let matches = match target_gids {
                    Some(targets) => targets.get(group_id as usize).copied().unwrap_or(false),
                    None => group_id == target_gid,
                };
                if !matches {
                    continue;
                }
                let value = if is_int {
                    unsafe {
                        std::ptr::read_unaligned(values.add(local_row * 8) as *const i64) as f64
                    }
                } else {
                    unsafe {
                        std::ptr::read_unaligned(values.add(local_row * 8) as *const f64)
                    }
                };
                part.count += 1;
                part.sum += value;
                part.min = part.min.min(value);
                part.max = part.max.max(value);
            }
            part
        };

        let parts: Vec<Part> = if should_parallel_filtered_agg(
            group_ids.len(),
            row_groups.len(),
            rayon::current_num_threads(),
        ) {
            use rayon::prelude::*;
            row_groups.par_iter().map(scan).collect()
        } else {
            row_groups.iter().map(scan).collect()
        };
        let mut count = 0i64;
        let mut sum = 0.0f64;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        for part in parts {
            count += part.count;
            sum += part.sum;
            min = min.min(part.min);
            max = max.max(part.max);
        }

        Ok(Some((
            count,
            sum,
            if min == f64::INFINITY { 0.0 } else { min },
            if max == f64::NEG_INFINITY { 0.0 } else { max },
            is_int,
        )))
    }

    /// Encoded/compressed fallback for cached categorical filters. The column
    /// decoder preserves all storage encodings; the aggregation over decoded
    /// values remains parallel and never materializes matching row indices.
    fn execute_filtered_cached_numeric_decoded(
        &self,
        group_ids: &[u16],
        target_gid: u16,
        target_gids: Option<&[bool]>,
        agg_col: &str,
    ) -> io::Result<Option<(i64, f64, f64, f64, bool)>> {
        use rayon::prelude::*;

        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        let Some(column_index) = footer.schema.get_index(agg_col) else {
            return Ok(None);
        };
        let parallel = self.scan_numeric_columns_mmap_parallel(&[column_index], &footer)?;
        let (columns, deleted) = match parallel {
            Some(columns) => (columns, Vec::new()),
            None => {
                let (columns, deleted) = self.scan_columns_mmap(&[column_index], &footer)?;
                (
                    columns.into_iter().map(std::sync::Arc::new).collect(),
                    deleted,
                )
            }
        };
        let Some(column) = columns.first() else {
            return Ok(None);
        };
        let (row_count, is_int) = match column.as_ref() {
            ColumnData::Int64(values) => (values.len().min(group_ids.len()), true),
            ColumnData::Float64(values) => (values.len().min(group_ids.len()), false),
            _ => return Ok(None),
        };
        #[derive(Clone, Copy)]
        struct Part {
            count: i64,
            sum: f64,
            min: f64,
            max: f64,
        }
        let empty = || Part {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        };
        let result = (0..row_count)
            .into_par_iter()
            .fold(empty, |mut part, row| {
                if deleted
                    .get(row / 8)
                    .is_some_and(|byte| ((byte >> (row % 8)) & 1) != 0)
                {
                    return part;
                }
                let group_id = unsafe { *group_ids.get_unchecked(row) };
                let matches = match target_gids {
                    Some(targets) => targets.get(group_id as usize).copied().unwrap_or(false),
                    None => group_id == target_gid,
                };
                if !matches {
                    return part;
                }
                let value = match column.as_ref() {
                    ColumnData::Int64(values) => unsafe { *values.get_unchecked(row) as f64 },
                    ColumnData::Float64(values) => unsafe { *values.get_unchecked(row) },
                    _ => unreachable!(),
                };
                part.count += 1;
                part.sum += value;
                part.min = part.min.min(value);
                part.max = part.max.max(value);
                part
            })
            .reduce(empty, |left, right| Part {
                count: left.count + right.count,
                sum: left.sum + right.sum,
                min: left.min.min(right.min),
                max: left.max.max(right.max),
            });
        Ok(Some((
            result.count,
            result.sum,
            if result.count == 0 { 0.0 } else { result.min },
            if result.count == 0 { 0.0 } else { result.max },
            is_int,
        )))
    }

    /// Decode independent numeric row groups in parallel, preserving row
    /// order when their per-group vectors are concatenated. This is the
    /// encoded/compressed counterpart of the zero-copy mmap fast paths.
    fn scan_numeric_columns_mmap_parallel(
        &self,
        column_indices: &[usize],
        footer: &V4Footer,
    ) -> io::Result<Option<Vec<std::sync::Arc<ColumnData>>>> {
        use rayon::prelude::*;

        if column_indices.is_empty()
            || footer
                .row_groups
                .iter()
                .any(|row_group| row_group.deletion_count > 0)
            || column_indices.iter().any(|&index| {
                !matches!(
                    footer.schema.columns[index].1,
                    ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date
                        | ColumnType::Float64
                        | ColumnType::Float32
                )
            })
        {
            return Ok(None);
        }
        {
            let cache = self.numeric_scan_cache.read();
            let cached: Option<Vec<std::sync::Arc<ColumnData>>> = column_indices
                .iter()
                .map(|index| {
                    cache
                        .iter()
                        .find(|(cached_index, _, _)| cached_index == index)
                        .map(|(_, column, _)| std::sync::Arc::clone(column))
                })
                .collect();
            if let Some(cached) = cached {
                return Ok(Some(cached));
            }
        }
        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for parallel numeric decode"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap = mmap_guard.get_or_create(file)?;
        let mmap_ptr = mmap.as_ptr() as usize;
        let mmap_len = mmap.len();
        let parts: Option<Vec<Vec<ColumnData>>> = footer
            .row_groups
            .par_iter()
            .enumerate()
            .map(|(row_group_index, row_group)| {
                let rows = row_group.row_count as usize;
                let bytes = unsafe {
                    std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len)
                };
                let end = (row_group.offset + row_group.data_size) as usize;
                if end > bytes.len() || end < row_group.offset as usize + 32 {
                    return None;
                }
                let row_group_bytes = &bytes[row_group.offset as usize..end];
                let decompressed =
                    decompress_rg_body(row_group_bytes[28], &row_group_bytes[32..]).ok()?;
                let body = decompressed
                    .as_deref()
                    .unwrap_or(&row_group_bytes[32..]);
                let offsets = footer.col_offsets.get(row_group_index)?;
                let null_bytes = rows.div_ceil(8);
                column_indices
                    .iter()
                    .map(|&index| {
                        let offset = *offsets.get(index)? as usize;
                        if offset + null_bytes >= body.len()
                            || body[offset..offset + null_bytes]
                                .iter()
                                .any(|&byte| byte != 0)
                        {
                            return None;
                        }
                        let (column, _) = read_column_encoded(
                            &body[offset + null_bytes..],
                            footer.schema.columns[index].1,
                        )
                        .ok()?;
                        matches!(column, ColumnData::Int64(_) | ColumnData::Float64(_))
                            .then_some(column)
                    })
                    .collect()
            })
            .collect();
        drop(mmap_guard);
        drop(file_guard);
        let Some(mut parts) = parts else {
            return Ok(None);
        };
        if parts.is_empty() {
            return Ok(None);
        }
        let mut merged = parts.remove(0);
        for part in parts {
            for (target, source) in merged.iter_mut().zip(part) {
                target.append(&source);
            }
        }
        let decoded = merged
            .into_iter()
            .map(std::sync::Arc::new)
            .collect::<Vec<_>>();
        const MAX_CACHE_BYTES: usize = 128 * 1024 * 1024;
        const MAX_CACHE_COLUMNS: usize = 16;
        let mut cache = self.numeric_scan_cache.write();
        for (&index, column) in column_indices.iter().zip(&decoded) {
            if cache.iter().any(|(cached_index, _, _)| *cached_index == index) {
                continue;
            }
            let bytes = match column.as_ref() {
                ColumnData::Int64(values) => values.len() * std::mem::size_of::<i64>(),
                ColumnData::Float64(values) => values.len() * std::mem::size_of::<f64>(),
                _ => 0,
            };
            if bytes == 0 || bytes > MAX_CACHE_BYTES {
                continue;
            }
            let mut current_bytes = cache.iter().map(|(_, _, bytes)| *bytes).sum::<usize>();
            while !cache.is_empty()
                && (cache.len() >= MAX_CACHE_COLUMNS
                    || current_bytes.saturating_add(bytes) > MAX_CACHE_BYTES)
            {
                let (_, _, removed_bytes) = cache.remove(0);
                current_bytes = current_bytes.saturating_sub(removed_bytes);
            }
            cache.push((index, std::sync::Arc::clone(column), bytes));
            self.numeric_scan_cache_populated
                .store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(Some(decoded))
    }

    /// Single-pass filtered numeric aggregation from mmap.
    /// Handles simple predicates such as `WHERE age > 30` without materializing
    /// matching rows or Arrow arrays.
    pub fn execute_filtered_numeric_agg_mmap(
        &self,
        filter_col: &str,
        low: f64,
        high: f64,
        agg_cols: &[&str],
    ) -> io::Result<Option<Vec<(i64, f64, f64, f64, bool)>>> {
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let filter_idx = match schema.get_index(filter_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let filter_type = schema.columns[filter_idx].1;
        let filter_is_int = matches!(
            filter_type,
            ColumnType::Int64
                | ColumnType::Int8
                | ColumnType::Int16
                | ColumnType::Int32
                | ColumnType::UInt8
                | ColumnType::UInt16
                | ColumnType::UInt32
                | ColumnType::UInt64
                | ColumnType::Timestamp
                | ColumnType::Date
                | ColumnType::Bool
        );
        let filter_is_float = matches!(filter_type, ColumnType::Float64 | ColumnType::Float32);
        if !filter_is_int && !filter_is_float {
            return Ok(None);
        }

        let agg_indices: Vec<usize> = agg_cols
            .iter()
            .filter(|&&n| n != "*" && n != "1")
            .filter_map(|&n| schema.get_index(n))
            .collect();
        let non_star_count = agg_cols.iter().filter(|&&n| n != "*" && n != "1").count();
        if agg_indices.len() != non_star_count {
            return Ok(None);
        }
        for &idx in &agg_indices {
            let ct = schema.columns[idx].1;
            let is_numeric = matches!(
                ct,
                ColumnType::Int64
                    | ColumnType::Int8
                    | ColumnType::Int16
                    | ColumnType::Int32
                    | ColumnType::UInt8
                    | ColumnType::UInt16
                    | ColumnType::UInt32
                    | ColumnType::UInt64
                    | ColumnType::Timestamp
                    | ColumnType::Date
                    | ColumnType::Float64
                    | ColumnType::Float32
                    | ColumnType::Bool
            );
            if !is_numeric {
                return Ok(None);
            }
        }

        enum NumView<'a> {
            I64(std::borrow::Cow<'a, [i64]>),
            F64(std::borrow::Cow<'a, [f64]>),
            Bitpack(&'a [u8]),
            Bool(&'a [u8]),
        }
        macro_rules! num_at {
            ($view:expr, $idx:expr) => {
                match $view {
                    NumView::I64(vals) => vals.get($idx).map(|&v| v as f64),
                    NumView::F64(vals) => vals.get($idx).copied(),
                    NumView::Bitpack(bytes) => {
                        crate::storage::on_demand::bitpack_decode_at_idx(bytes, $idx)
                            .map(|v| v as f64)
                    }
                    NumView::Bool(bytes) => bytes
                        .get($idx / 8)
                        .map(|b| ((b >> ($idx % 8)) & 1) as f64),
                }
            };
        }

        let nc = agg_indices.len();
        let mut col_counts = vec![0i64; nc];
        let mut col_sums = vec![0.0f64; nc];
        let mut col_mins = vec![f64::INFINITY; nc];
        let mut col_maxs = vec![f64::NEG_INFINITY; nc];
        let mut col_is_int = vec![false; nc];
        let mut match_count = 0i64;

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "File not open"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        if nc <= 1 && footer.row_groups.len() > 1 {
            use rayon::prelude::*;

            #[derive(Clone, Copy)]
            struct Part {
                matches: i64,
                count: i64,
                sum: f64,
                min: f64,
                max: f64,
                is_int: bool,
            }

            impl Part {
                #[inline]
                fn empty() -> Self {
                    Self {
                        matches: 0,
                        count: 0,
                        sum: 0.0,
                        min: f64::INFINITY,
                        max: f64::NEG_INFINITY,
                        is_int: false,
                    }
                }

                #[inline]
                fn add_f64(&mut self, v: f64) {
                    self.count += 1;
                    self.sum += v;
                    if v < self.min {
                        self.min = v;
                    }
                    if v > self.max {
                        self.max = v;
                    }
                }
            }

            let mmap_ptr = mmap_ref.as_ptr() as usize;
            let mmap_len = mmap_ref.len();

            let parts: Option<Vec<Part>> = footer
                .row_groups
                .par_iter()
                .enumerate()
                .map(|(rg_i, rg_meta)| {
                    let mut part = Part::empty();
                    let rg_rows = rg_meta.row_count as usize;
                    if rg_rows == 0 {
                        return Some(part);
                    }

                    if let Some(zmaps) = footer.zone_maps.get(rg_i) {
                        if let Some(zm) = zmaps.iter().find(|z| z.col_idx as usize == filter_idx)
                        {
                            let skip = if zm.is_float {
                                !zm.may_overlap_float_range(low, high)
                            } else {
                                !zm.may_overlap_int_range(low.ceil() as i64, high.floor() as i64)
                            };
                            if skip {
                                return Some(part);
                            }
                        }
                    }

                    let mmap =
                        unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                    let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
                    if rg_end > mmap.len() || rg_end < rg_meta.offset as usize + 32 {
                        return None;
                    }
                    let rg_bytes = &mmap[rg_meta.offset as usize..rg_end];
                    let compress_flag = rg_bytes[28];
                    let encoding_version = rg_bytes[29];
                    if compress_flag != RG_COMPRESS_NONE || encoding_version < 1 {
                        return None;
                    }
                    let rcix = footer.col_offsets.get(rg_i)?;
                    if rcix.len() <= filter_idx || agg_indices.iter().any(|&ci| rcix.len() <= ci)
                    {
                        return None;
                    }

                    let body = &rg_bytes[32..];
                    let null_bitmap_len = (rg_rows + 7) / 8;
                    let del_start = rg_id_section_len(
                        rg_rows,
                        rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
                    );
                    let del_len = null_bitmap_len;
                    if del_start + del_len > body.len() {
                        return None;
                    }
                    let del_bytes = &body[del_start..del_start + del_len];
                    let has_deletes = rg_meta.deletion_count > 0;

                    enum AggView<'a> {
                        None,
                        F64(&'a [u8], usize),
                        I64(&'a [u8], usize),
                        Bitpack(&'a [u8]),
                    }

                    let agg_view = if nc == 0 {
                        AggView::None
                    } else {
                        let agg_idx = agg_indices[0];
                        let agg_off = rcix[agg_idx] as usize;
                        if agg_off + null_bitmap_len >= body.len() {
                            return None;
                        }
                        let agg_encoding = body[agg_off + null_bitmap_len];
                        let agg_payload = &body[agg_off + null_bitmap_len + 1..];
                        if agg_payload.len() < 8 {
                            return None;
                        }
                        if agg_encoding == COL_ENCODING_BITPACK {
                            part.is_int = true;
                            AggView::Bitpack(agg_payload)
                        } else if agg_encoding != COL_ENCODING_PLAIN {
                            return None;
                        } else {
                        let agg_count =
                            u64::from_le_bytes(agg_payload[0..8].try_into().ok()?) as usize;
                        let agg_n = agg_count.min(rg_rows).min((agg_payload.len() - 8) / 8);
                        let agg_values = &agg_payload[8..];
                        match schema.columns[agg_idx].1 {
                            ColumnType::Float64 | ColumnType::Float32 => {
                                AggView::F64(agg_values, agg_n)
                            }
                            ColumnType::Int64
                            | ColumnType::Int8
                            | ColumnType::Int16
                            | ColumnType::Int32
                            | ColumnType::UInt8
                            | ColumnType::UInt16
                            | ColumnType::UInt32
                            | ColumnType::UInt64
                            | ColumnType::Timestamp
                            | ColumnType::Date
                            | ColumnType::Bool => {
                                part.is_int = true;
                                AggView::I64(agg_values, agg_n)
                            }
                            _ => return None,
                        }
                        }
                    };

                    let filter_off = rcix[filter_idx] as usize;
                    if filter_off + null_bitmap_len >= body.len() {
                        return None;
                    }
                    let filter_nulls = &body[filter_off..filter_off + null_bitmap_len];
                    let filter_has_nulls = filter_nulls.iter().any(|&b| b != 0);
                    let filter_encoding = body[filter_off + null_bitmap_len];
                    let filter_payload = &body[filter_off + null_bitmap_len + 1..];
                    let low_i = low.ceil() as i64;
                    let high_i = high.floor() as i64;

                    macro_rules! add_match {
                        ($idx:expr, $filter_value:expr) => {{
                            let row_idx = $idx;
                            let fv = $filter_value as f64;
                            if fv >= low && fv <= high {
                                part.matches += 1;
                                match agg_view {
                                    AggView::None => {}
                                    AggView::F64(values, n) => {
                                        if row_idx < n {
                                            let off = row_idx * 8;
                                            let v = f64::from_le_bytes(
                                                values[off..off + 8].try_into().ok()?,
                                            );
                                            part.add_f64(v);
                                        }
                                    }
                                    AggView::I64(values, n) => {
                                        if row_idx < n {
                                            let off = row_idx * 8;
                                            let v = i64::from_le_bytes(
                                                values[off..off + 8].try_into().ok()?,
                                            ) as f64;
                                            part.add_f64(v);
                                        }
                                    }
                                    AggView::Bitpack(values) => {
                                        let value = crate::storage::on_demand::bitpack_decode_at_idx(
                                            values,
                                            row_idx,
                                        )?;
                                        part.add_f64(value as f64);
                                    }
                                }
                            }
                        }};
                    }

                    macro_rules! add_int_match {
                        ($idx:expr, $filter_value:expr) => {{
                            let row_idx = $idx;
                            let fv = $filter_value;
                            if fv >= low_i && fv <= high_i {
                                part.matches += 1;
                                match agg_view {
                                    AggView::None => {}
                                    AggView::F64(values, n) => {
                                        if row_idx < n {
                                            let off = row_idx * 8;
                                            let v = f64::from_le_bytes(
                                                values[off..off + 8].try_into().ok()?,
                                            );
                                            part.add_f64(v);
                                        }
                                    }
                                    AggView::I64(values, n) => {
                                        if row_idx < n {
                                            let off = row_idx * 8;
                                            let v = i64::from_le_bytes(
                                                values[off..off + 8].try_into().ok()?,
                                            ) as f64;
                                            part.add_f64(v);
                                        }
                                    }
                                    AggView::Bitpack(values) => {
                                        let value = crate::storage::on_demand::bitpack_decode_at_idx(
                                            values,
                                            row_idx,
                                        )?;
                                        part.add_f64(value as f64);
                                    }
                                }
                            }
                        }};
                    }

                    match filter_type {
                        ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date => {
                            if filter_encoding == COL_ENCODING_BITPACK {
                                if filter_payload.len() < 17 {
                                    return None;
                                }
                                let count =
                                    u64::from_le_bytes(filter_payload[0..8].try_into().ok()?)
                                        as usize;
                                let bit_width = filter_payload[8] as usize;
                                let min_val =
                                    i64::from_le_bytes(filter_payload[9..17].try_into().ok()?);
                                let packed_bytes = (count * bit_width + 7) / 8;
                                if filter_payload.len() < 17 + packed_bytes {
                                    return None;
                                }
                                let packed = &filter_payload[17..17 + packed_bytes];
                                let n = count.min(rg_rows);

                                if bit_width == 0 {
                                    for i in 0..n {
                                        if has_deletes
                                            && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        if filter_has_nulls
                                            && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        add_int_match!(i, min_val);
                                    }
                                } else if bit_width <= 5 {
                                    // Decode eight small integers from one little-endian
                                    // word. The generic per-row decoder reloads up to three
                                    // bytes for every value, which dominates low-cardinality
                                    // predicates such as protocol/status/category codes.
                                    let mask = (1u64 << bit_width) - 1;
                                    let groups = n / 8;
                                    for group in 0..groups {
                                        let byte_offset = group * bit_width;
                                        if byte_offset + bit_width > packed.len() {
                                            return None;
                                        }
                                        let mut word = 0u64;
                                        for byte in 0..bit_width {
                                            word |= (packed[byte_offset + byte] as u64) << (byte * 8);
                                        }
                                        let base = group * 8;
                                        for lane in 0..8 {
                                            let row = base + lane;
                                            if has_deletes
                                                && (del_bytes[row / 8] >> (row % 8)) & 1 == 1
                                            {
                                                continue;
                                            }
                                            if filter_has_nulls
                                                && (filter_nulls[row / 8] >> (row % 8)) & 1 == 1
                                            {
                                                continue;
                                            }
                                            let delta = ((word >> (lane * bit_width)) & mask) as i64;
                                            add_int_match!(row, min_val.wrapping_add(delta));
                                        }
                                    }
                                    for row in (groups * 8)..n {
                                        if has_deletes
                                            && (del_bytes[row / 8] >> (row % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        if filter_has_nulls
                                            && (filter_nulls[row / 8] >> (row % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        let bit_offset = row * bit_width;
                                        let byte_idx = bit_offset / 8;
                                        let shift = bit_offset % 8;
                                        let mut word = 0u64;
                                        for byte in 0..=bit_width / 8 {
                                            word |= (packed
                                                .get(byte_idx + byte)
                                                .copied()
                                                .unwrap_or(0) as u64)
                                                << (byte * 8);
                                        }
                                        let delta = ((word >> shift) & mask) as i64;
                                        add_int_match!(row, min_val.wrapping_add(delta));
                                    }
                                } else if bit_width == 6 {
                                    if !has_deletes && !filter_has_nulls {
                                        let n_full = n / 8;
                                        match &agg_view {
                                            AggView::F64(values, agg_n) => {
                                                let n_scan = n.min(*agg_n);
                                                let n_full_scan = n_scan / 8;
                                                for g in 0..n_full_scan {
                                                    let off = g * 6;
                                                    if off + 6 > packed.len() {
                                                        return None;
                                                    }
                                                    let b = &packed[off..];
                                                    let word = u64::from_le_bytes([
                                                        b[0], b[1], b[2], b[3], b[4], b[5], 0, 0,
                                                    ]);
                                                    let base = g * 8;
                                                    for k in 0..8usize {
                                                        let i = base + k;
                                                        let delta = ((word >> (k * 6)) & 0x3F) as i64;
                                                        let fv = min_val.wrapping_add(delta);
                                                        if fv >= low_i && fv <= high_i {
                                                            part.matches += 1;
                                                            let off = i * 8;
                                                            let v = f64::from_le_bytes(
                                                                values[off..off + 8]
                                                                    .try_into()
                                                                    .ok()?,
                                                            );
                                                            part.add_f64(v);
                                                        }
                                                    }
                                                }
                                                for i in (n_full_scan * 8)..n_scan {
                                                    let bit_offset = i * 6;
                                                    let byte_idx = bit_offset / 8;
                                                    let shift = bit_offset % 8;
                                                    let b0 =
                                                        packed.get(byte_idx).copied().unwrap_or(0)
                                                            as u64;
                                                    let b1 = packed
                                                        .get(byte_idx + 1)
                                                        .copied()
                                                        .unwrap_or(0)
                                                        as u64;
                                                    let delta =
                                                        ((b0 | (b1 << 8)) >> shift) & 0x3F;
                                                    let fv = min_val.wrapping_add(delta as i64);
                                                    if fv >= low_i && fv <= high_i {
                                                        part.matches += 1;
                                                        let off = i * 8;
                                                        let v = f64::from_le_bytes(
                                                            values[off..off + 8]
                                                                .try_into()
                                                                .ok()?,
                                                        );
                                                        part.add_f64(v);
                                                    }
                                                }
                                                return Some(part);
                                            }
                                            AggView::None => {
                                                for g in 0..n_full {
                                                    let off = g * 6;
                                                    if off + 6 > packed.len() {
                                                        return None;
                                                    }
                                                    let b = &packed[off..];
                                                    let word = u64::from_le_bytes([
                                                        b[0], b[1], b[2], b[3], b[4], b[5], 0, 0,
                                                    ]);
                                                    for k in 0..8usize {
                                                        let delta = ((word >> (k * 6)) & 0x3F) as i64;
                                                        let fv = min_val.wrapping_add(delta);
                                                        if fv >= low_i && fv <= high_i {
                                                            part.matches += 1;
                                                        }
                                                    }
                                                }
                                                for i in (n_full * 8)..n {
                                                    let bit_offset = i * 6;
                                                    let byte_idx = bit_offset / 8;
                                                    let shift = bit_offset % 8;
                                                    let b0 =
                                                        packed.get(byte_idx).copied().unwrap_or(0)
                                                            as u64;
                                                    let b1 = packed
                                                        .get(byte_idx + 1)
                                                        .copied()
                                                        .unwrap_or(0)
                                                        as u64;
                                                    let delta =
                                                        ((b0 | (b1 << 8)) >> shift) & 0x3F;
                                                    let fv = min_val.wrapping_add(delta as i64);
                                                    if fv >= low_i && fv <= high_i {
                                                        part.matches += 1;
                                                    }
                                                }
                                                return Some(part);
                                            }
                                            AggView::I64(_, _) | AggView::Bitpack(_) => {}
                                        }
                                    }
                                    let n_full = n / 8;
                                    for g in 0..n_full {
                                        let off = g * 6;
                                        if off + 6 > packed.len() {
                                            return None;
                                        }
                                        let b = &packed[off..];
                                        let word = u64::from_le_bytes([
                                            b[0], b[1], b[2], b[3], b[4], b[5], 0, 0,
                                        ]);
                                        let base = g * 8;
                                        for k in 0..8usize {
                                            let i = base + k;
                                            if has_deletes
                                                && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                            {
                                                continue;
                                            }
                                            if filter_has_nulls
                                                && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1
                                            {
                                                continue;
                                            }
                                            let delta = ((word >> (k * 6)) & 0x3F) as i64;
                                            add_int_match!(i, min_val.wrapping_add(delta));
                                        }
                                    }
                                    for i in (n_full * 8)..n {
                                        if has_deletes
                                            && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        if filter_has_nulls
                                            && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        let bit_offset = i * 6;
                                        let byte_idx = bit_offset / 8;
                                        let shift = bit_offset % 8;
                                        let b0 = packed.get(byte_idx).copied().unwrap_or(0) as u64;
                                        let b1 =
                                            packed.get(byte_idx + 1).copied().unwrap_or(0) as u64;
                                        let delta = ((b0 | (b1 << 8)) >> shift) & 0x3F;
                                        add_int_match!(i, min_val.wrapping_add(delta as i64));
                                    }
                                } else if bit_width == 7 {
                                    let n_full = n / 8;
                                    for g in 0..n_full {
                                        let off = g * 7;
                                        if off + 7 > packed.len() {
                                            return None;
                                        }
                                        let b = &packed[off..off + 7];
                                        let word = u64::from_le_bytes([
                                            b[0], b[1], b[2], b[3], b[4], b[5], b[6], 0,
                                        ]);
                                        let base = g * 8;
                                        for k in 0..8usize {
                                            let i = base + k;
                                            if has_deletes
                                                && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                            {
                                                continue;
                                            }
                                            if filter_has_nulls
                                                && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1
                                            {
                                                continue;
                                            }
                                            let delta = ((word >> (k * 7)) & 0x7F) as i64;
                                            add_int_match!(i, min_val.wrapping_add(delta));
                                        }
                                    }
                                    for i in (n_full * 8)..n {
                                        if has_deletes
                                            && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        if filter_has_nulls
                                            && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        let bit_offset = i * 7;
                                        let byte_idx = bit_offset / 8;
                                        let shift = bit_offset % 8;
                                        let b0 = packed.get(byte_idx).copied().unwrap_or(0) as u64;
                                        let b1 =
                                            packed.get(byte_idx + 1).copied().unwrap_or(0) as u64;
                                        let delta = ((b0 | (b1 << 8)) >> shift) & 0x7F;
                                        add_int_match!(i, min_val.wrapping_add(delta as i64));
                                    }
                                } else if bit_width == 8 {
                                    for i in 0..n {
                                        if has_deletes
                                            && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        if filter_has_nulls
                                            && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        let delta = packed.get(i).copied().unwrap_or(0) as i64;
                                        add_int_match!(i, min_val.wrapping_add(delta));
                                    }
                                } else if bit_width <= 16 {
                                    let mask = (1u64 << bit_width) - 1;
                                    for i in 0..n {
                                        if has_deletes
                                            && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        if filter_has_nulls
                                            && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1
                                        {
                                            continue;
                                        }
                                        let bit_offset = i * bit_width;
                                        let byte_idx = bit_offset / 8;
                                        let shift = bit_offset % 8;
                                        let b0 = packed.get(byte_idx).copied().unwrap_or(0) as u64;
                                        let b1 =
                                            packed.get(byte_idx + 1).copied().unwrap_or(0) as u64;
                                        let b2 =
                                            packed.get(byte_idx + 2).copied().unwrap_or(0) as u64;
                                        let word = b0 | (b1 << 8) | (b2 << 16);
                                        let delta = ((word >> shift) & mask) as i64;
                                        add_int_match!(i, min_val.wrapping_add(delta));
                                    }
                                } else {
                                    return None;
                                }
                            } else if filter_encoding == COL_ENCODING_PLAIN {
                                if filter_payload.len() < 8 {
                                    return None;
                                }
                                let count =
                                    u64::from_le_bytes(filter_payload[0..8].try_into().ok()?)
                                        as usize;
                                let n = count.min(rg_rows).min((filter_payload.len() - 8) / 8);
                                let values = &filter_payload[8..];
                                for i in 0..n {
                                    if has_deletes
                                        && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                    {
                                        continue;
                                    }
                                    if filter_has_nulls
                                        && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1
                                    {
                                        continue;
                                    }
                                    let off = i * 8;
                                    let v =
                                        i64::from_le_bytes(values[off..off + 8].try_into().ok()?);
                                    add_int_match!(i, v);
                                }
                            } else {
                                return None;
                            }
                        }
                        ColumnType::Float64 | ColumnType::Float32 => {
                            if filter_encoding != COL_ENCODING_PLAIN || filter_payload.len() < 8 {
                                return None;
                            }
                            let count =
                                u64::from_le_bytes(filter_payload[0..8].try_into().ok()?) as usize;
                            let n = count.min(rg_rows).min((filter_payload.len() - 8) / 8);
                            let values = &filter_payload[8..];
                            for i in 0..n {
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if filter_has_nulls
                                    && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1
                                {
                                    continue;
                                }
                                let off = i * 8;
                                let v =
                                    f64::from_le_bytes(values[off..off + 8].try_into().ok()?);
                                add_match!(i, v);
                            }
                        }
                        _ => return None,
                    }

                    Some(part)
                })
                .collect();

            if let Some(parts) = parts {
                let mut total = Part::empty();
                for part in parts {
                    total.matches += part.matches;
                    total.count += part.count;
                    total.sum += part.sum;
                    if part.min < total.min {
                        total.min = part.min;
                    }
                    if part.max > total.max {
                        total.max = part.max;
                    }
                    total.is_int |= part.is_int;
                }

                let mut results = Vec::with_capacity(agg_cols.len());
                let mut ci = 0usize;
                for &col_name in agg_cols {
                    if col_name == "*" || col_name == "1" {
                        results.push((total.matches, 0.0, 0.0, 0.0, false));
                    } else if ci < nc {
                        let mn = if total.min == f64::INFINITY {
                            0.0
                        } else {
                            total.min
                        };
                        let mx = if total.max == f64::NEG_INFINITY {
                            0.0
                        } else {
                            total.max
                        };
                        results.push((total.count, total.sum, mn, mx, total.is_int));
                        ci += 1;
                    } else {
                        results.push((0, 0.0, 0.0, 0.0, false));
                    }
                }
                return Ok(Some(results));
            }
        }

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }

            if let Some(zmaps) = footer.zone_maps.get(rg_i) {
                if let Some(zm) = zmaps.iter().find(|z| z.col_idx as usize == filter_idx) {
                    let skip = if zm.is_float {
                        !zm.may_overlap_float_range(low, high)
                    } else {
                        !zm.may_overlap_int_range(low.ceil() as i64, high.floor() as i64)
                    };
                    if skip {
                        continue;
                    }
                }
            }

            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("RG extends past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.len() < 32 {
                continue;
            }
            let compress_flag = rg_bytes[28];
            let encoding_version = rg_bytes[29];
            if compress_flag != RG_COMPRESS_NONE || encoding_version < 1 {
                return Ok(None);
            }
            let rcix = match footer.col_offsets.get(rg_i) {
                Some(offsets)
                    if offsets.len() > filter_idx
                        && agg_indices.iter().all(|&ci| offsets.len() > ci) =>
                {
                    offsets
                }
                _ => return Ok(None),
            };

            let body = &rg_bytes[32..];
            let null_bitmap_len = (rg_rows + 7) / 8;
            let del_start = rg_id_section_len(
                rg_rows,
                rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
            );
            let del_len = null_bitmap_len;
            if del_start + del_len > body.len() {
                return Ok(None);
            }
            let del_bytes = &body[del_start..del_start + del_len];
            let has_deletes = rg_meta.deletion_count > 0;

            macro_rules! build_numeric_view {
                ($col_idx:expr) => {{
                    let col_idx = $col_idx;
                    let col_off = rcix[col_idx] as usize;
                    if col_off + null_bitmap_len >= body.len() {
                        None
                    } else {
                        let null_bytes = &body[col_off..col_off + null_bitmap_len];
                        let col_bytes = &body[col_off + null_bitmap_len..];
                        if col_bytes.is_empty() {
                            None
                        } else {
                            let encoding = col_bytes[0];
                            let payload = &col_bytes[1..];
                            let ct = schema.columns[col_idx].1;
                            match ct {
                                ColumnType::Bool => {
                                    let need = (rg_rows + 7) / 8;
                                    if payload.len() >= need {
                                        Some((null_bytes, NumView::Bool(&payload[..need]), true))
                                    } else {
                                        None
                                    }
                                }
                                ColumnType::Int64
                                | ColumnType::Int8
                                | ColumnType::Int16
                                | ColumnType::Int32
                                | ColumnType::UInt8
                                | ColumnType::UInt16
                                | ColumnType::UInt32
                                | ColumnType::UInt64
                                | ColumnType::Timestamp
                                | ColumnType::Date => {
                                    if encoding == COL_ENCODING_PLAIN && payload.len() >= 8 {
                                        let count =
                                            u64::from_le_bytes(payload[0..8].try_into().unwrap())
                                                as usize;
                                        let n = count.min(rg_rows).min((payload.len() - 8) / 8);
                                        Some((
                                            null_bytes,
                                            NumView::I64(bytes_as_i64_slice(&payload[8..], n)),
                                            true,
                                        ))
                                    } else if encoding == COL_ENCODING_BITPACK {
                                        Some((null_bytes, NumView::Bitpack(payload), true))
                                    } else {
                                        match read_column_encoded(col_bytes, ct) {
                                            Ok((ColumnData::Int64(vals), _)) => Some((
                                                null_bytes,
                                                NumView::I64(std::borrow::Cow::Owned(vals)),
                                                true,
                                            )),
                                            _ => None,
                                        }
                                    }
                                }
                                ColumnType::Float64 | ColumnType::Float32 => {
                                    if encoding == COL_ENCODING_PLAIN && payload.len() >= 8 {
                                        let count =
                                            u64::from_le_bytes(payload[0..8].try_into().unwrap())
                                                as usize;
                                        let n = count.min(rg_rows).min((payload.len() - 8) / 8);
                                        Some((
                                            null_bytes,
                                            NumView::F64(bytes_as_f64_slice(&payload[8..], n)),
                                            false,
                                        ))
                                    } else {
                                        match read_column_encoded(col_bytes, ct) {
                                            Ok((ColumnData::Float64(vals), _)) => Some((
                                                null_bytes,
                                                NumView::F64(std::borrow::Cow::Owned(vals)),
                                                false,
                                            )),
                                            _ => None,
                                        }
                                    }
                                }
                                _ => None,
                            }
                        }
                    }
                }};
            }

            let Some((filter_nulls, filter_view, _)) = build_numeric_view!(filter_idx) else {
                return Ok(None);
            };

            let mut agg_views = Vec::with_capacity(nc);
            for &agg_idx in &agg_indices {
                let Some(view) = build_numeric_view!(agg_idx) else {
                    return Ok(None);
                };
                agg_views.push(view);
            }
            for (ai, (_, _, is_int)) in agg_views.iter().enumerate() {
                if *is_int {
                    col_is_int[ai] = true;
                }
            }

            let no_filter_nulls = !filter_nulls.iter().any(|&b| b != 0);
            for i in 0..rg_rows {
                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                    continue;
                }
                if !no_filter_nulls && (filter_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                    continue;
                }
                let Some(v) = num_at!(&filter_view, i) else {
                    continue;
                };
                if v < low || v > high {
                    continue;
                }
                match_count += 1;

                for (ai, (null_bytes, view, _)) in agg_views.iter().enumerate() {
                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                        continue;
                    }
                    let Some(av) = num_at!(view, i) else {
                        continue;
                    };
                    col_counts[ai] += 1;
                    col_sums[ai] += av;
                    if av < col_mins[ai] {
                        col_mins[ai] = av;
                    }
                    if av > col_maxs[ai] {
                        col_maxs[ai] = av;
                    }
                }
            }
        }

        drop(mmap_guard);
        drop(file_guard);

        let mut results = Vec::with_capacity(agg_cols.len());
        let mut ci = 0usize;
        for &col_name in agg_cols {
            if col_name == "*" || col_name == "1" {
                results.push((match_count, 0.0, 0.0, 0.0, false));
            } else if ci < nc {
                let mn = if col_mins[ci] == f64::INFINITY {
                    0.0
                } else {
                    col_mins[ci]
                };
                let mx = if col_maxs[ci] == f64::NEG_INFINITY {
                    0.0
                } else {
                    col_maxs[ci]
                };
                results.push((col_counts[ci], col_sums[ci], mn, mx, col_is_int[ci]));
                ci += 1;
            } else {
                results.push((0, 0.0, 0.0, 0.0, false));
            }
        }

        Ok(Some(results))
    }

    /// Compute numeric column aggregates from in-memory V4 columns.
    /// Returns (count, sum, min, max) for the specified column.
    pub fn compute_column_stats_inmemory(&self, col_name: &str) -> io::Result<Option<(u64, f64, f64, f64)>> {
        if !self.has_v4_in_memory_data() { return Ok(None); }

        let schema = self.schema.read();
        let columns = self.columns.read();
        let deleted = self.deleted.read();
        let total_rows = self.ids.read().len();

        let col_idx = match schema.get_index(col_name) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        if col_idx >= columns.len() { return Ok(None); }

        let has_deleted = deleted.iter().any(|&b| b != 0);

        match &columns[col_idx] {
            ColumnData::Int64(vals) => {
                let count = vals.len().min(total_rows);
                if !has_deleted {
                    let mut sum = 0i64;
                    let mut min_v = i64::MAX;
                    let mut max_v = i64::MIN;
                    for i in 0..count {
                        let v = unsafe { *vals.get_unchecked(i) };
                        sum += v;
                        if v < min_v { min_v = v; }
                        if v > max_v { max_v = v; }
                    }
                    Ok(Some((count as u64, sum as f64, min_v as f64, max_v as f64)))
                } else {
                    let mut c = 0u64;
                    let mut sum = 0i64;
                    let mut min_v = i64::MAX;
                    let mut max_v = i64::MIN;
                    for i in 0..count {
                        let b = i / 8; let bit = i % 8;
                        if b < deleted.len() && (deleted[b] >> bit) & 1 != 0 { continue; }
                        let v = vals[i];
                        c += 1; sum += v;
                        if v < min_v { min_v = v; }
                        if v > max_v { max_v = v; }
                    }
                    Ok(Some((c, sum as f64, min_v as f64, max_v as f64)))
                }
            }
            ColumnData::Float64(vals) => {
                let count = vals.len().min(total_rows);
                if !has_deleted {
                    let mut sum = 0.0f64;
                    let mut min_v = f64::INFINITY;
                    let mut max_v = f64::NEG_INFINITY;
                    for i in 0..count {
                        let v = unsafe { *vals.get_unchecked(i) };
                        sum += v;
                        if v < min_v { min_v = v; }
                        if v > max_v { max_v = v; }
                    }
                    Ok(Some((count as u64, sum, min_v, max_v)))
                } else {
                    let mut c = 0u64;
                    let mut sum = 0.0f64;
                    let mut min_v = f64::INFINITY;
                    let mut max_v = f64::NEG_INFINITY;
                    for i in 0..count {
                        let b = i / 8; let bit = i % 8;
                        if b < deleted.len() && (deleted[b] >> bit) & 1 != 0 { continue; }
                        let v = vals[i];
                        c += 1; sum += v;
                        if v < min_v { min_v = v; }
                        if v > max_v { max_v = v; }
                    }
                    Ok(Some((c, sum, min_v, max_v)))
                }
            }
            _ => Ok(None),
        }
    }

    /// Returns true if the column has any NULL values in the in-memory store
    /// or in the V4 footer zone maps (for mmap-only data).
    pub fn column_has_nulls(&self, col_name: &str) -> bool {
        let schema = self.schema.read();
        let col_idx = match schema.get_index(col_name) {
            Some(idx) => idx,
            None => return false,
        };
        let nulls = self.nulls.read();
        if col_idx < nulls.len() && nulls[col_idx].iter().any(|&b| b != 0) {
            return true;
        }
        drop(nulls);
        drop(schema);
        if let Ok(Some(footer)) = self.get_or_load_footer() {
            for rg_zms in &footer.zone_maps {
                for zm in rg_zms {
                    if zm.col_idx as usize == col_idx && zm.has_nulls {
                        return true;
                    }
                }
            }

            // String zone maps are not guaranteed to carry null metadata.
            // Inspect only the compact per-column validity bitmaps; this avoids
            // materializing values and keeps the common no-null GROUP BY fast.
            let file_guard = self.file.read();
            let Some(file) = file_guard.as_ref() else {
                return true;
            };
            let Ok(mmap_arc) = self.mmap_cache.write().get_mmap_arc(file) else {
                return true;
            };
            drop(file_guard);
            let mmap: &[u8] = &mmap_arc;
            for (group_idx, group) in footer.row_groups.iter().enumerate() {
                if group.row_count == 0 {
                    continue;
                }
                let Some(offsets) = footer.col_offsets.get(group_idx) else {
                    return true;
                };
                let Some(&column_offset) = offsets.get(col_idx) else {
                    return true;
                };
                let group_start = group.offset as usize;
                let Some(group_end) = group_start.checked_add(group.data_size as usize) else {
                    return true;
                };
                if group_end > mmap.len() || group_end.saturating_sub(group_start) < 32 {
                    return true;
                }
                let group_bytes = &mmap[group_start..group_end];
                let decompressed = match decompress_rg_body(group_bytes[28], &group_bytes[32..]) {
                    Ok(body) => body,
                    Err(_) => return true,
                };
                let body = decompressed.as_deref().unwrap_or(&group_bytes[32..]);
                let bitmap_len = (group.row_count as usize + 7) / 8;
                let start = column_offset as usize;
                let Some(end) = start.checked_add(bitmap_len) else {
                    return true;
                };
                if end > body.len() {
                    return true;
                }
                if body[start..end].iter().any(|&byte| byte != 0) {
                    return true;
                }
            }
        }
        false
    }

    /// Fast COUNT(DISTINCT col) for string columns using the dict cache.
    /// Correctly excludes NULL values via null bitmaps (both in-memory and mmap paths).
    /// Returns None if the column is not a string or not scannable.
    pub fn count_distinct_string(&self, col_name: &str) -> io::Result<Option<i64>> {
        let dict_pair = match self.build_string_dict_cache(col_name)? {
            Some(d) => d,
            None => return Ok(None),
        };
        Ok(Some(self.count_distinct_with_dict(col_name, &dict_pair.0, &dict_pair.1)?))
    }

    /// Fast COUNT(DISTINCT col) given a pre-built dict (dict_strings, group_ids).
    /// Uses the pre-built group_ids so no O(N) string-hashing is needed on warm cache hits.
    /// Correctly excludes NULL values via null bitmaps.
    pub fn count_distinct_with_dict(
        &self,
        col_name: &str,
        dict_strings: &[String],
        group_ids: &[u16],
    ) -> io::Result<i64> {
        const NULL_MARKER: &str = "\x00__NULL__\x00";
        let num_groups = dict_strings.len();
        if num_groups == 0 { return Ok(0); }

        if self.has_v4_in_memory_data() {
            // IN-MEMORY PATH: use null bitmap from self.nulls
            let schema = self.schema.read();
            let col_idx = match schema.get_index(col_name) {
                Some(idx) => idx,
                None => return Ok(num_groups as i64),
            };
            let nulls = self.nulls.read();
            let null_bm: &[u8] = if col_idx < nulls.len() { &nulls[col_idx] } else { &[] };
            let has_null_bm = !null_bm.is_empty() && null_bm.iter().any(|&b| b != 0);
            let deleted = self.deleted.read();
            let has_deleted = deleted.iter().any(|&b| b != 0);

            if !has_null_bm && !has_deleted {
                // No nulls, no deletes — count entries excluding NULL_MARKER (O(dict_size), fast)
                let count = dict_strings.iter().filter(|s| s.as_str() != NULL_MARKER).count() as i64;
                return Ok(count);
            }

            let mut seen = vec![false; num_groups];
            for (i, &gid) in group_ids.iter().enumerate() {
                if has_deleted { let b = i/8; let bit = i%8; if b < deleted.len() && (deleted[b] >> bit) & 1 != 0 { continue; } }
                if has_null_bm { let b = i/8; let bit = i%8; if b < null_bm.len() && (null_bm[b] >> bit) & 1 != 0 { continue; } }
                let g = gid as usize;
                if g < num_groups { seen[g] = true; }
            }
            // Also exclude any NULL_MARKER groups
            let count = (0..num_groups).filter(|&g| seen[g] && dict_strings[g].as_str() != NULL_MARKER).count() as i64;
            return Ok(count);
        }

        // MMAP PATH: read per-column null bitmaps from RG data
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(dict_strings.iter().filter(|s| s.as_str() != NULL_MARKER).count() as i64),
        };
        let col_idx = match footer.schema.get_index(col_name) {
            Some(idx) => idx,
            None => return Ok(dict_strings.iter().filter(|s| s.as_str() != NULL_MARKER).count() as i64),
        };
        let col_type = footer.schema.columns[col_idx].1;
        let has_any_deleted = footer.row_groups.iter().any(|rg| rg.deletion_count > 0);

        // Warm fast path for dictionary-encoded mmap columns:
        // the on-disk dictionary already contains only non-null distinct values,
        // so without deletions COUNT(DISTINCT) is just the dictionary cardinality.
        if !has_any_deleted && matches!(col_type, crate::storage::ColumnType::StringDict) {
            return Ok(num_groups as i64);
        }

        let max_col_idx = col_idx;
        let all_rcix = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
            if rg_meta.row_count == 0 { return true; }
            footer.col_offsets.get(rg_i).map_or(false, |v| v.len() > max_col_idx)
        });

        if !all_rcix {
            // Can't read null bitmaps — conservative count (may include null row groups)
            return Ok(dict_strings.iter().filter(|s| s.as_str() != NULL_MARKER).count() as i64);
        }

        let file_guard = self.file.read();
        let file = match file_guard.as_ref() {
            Some(f) => f,
            None => return Ok(dict_strings.iter().filter(|s| s.as_str() != NULL_MARKER).count() as i64),
        };
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let mut seen = vec![false; num_groups];
        let mut rg_row_offset = 0usize;

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 { rg_row_offset += rg_rows; continue; }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() { rg_row_offset += rg_rows; continue; }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.len() < 32 { rg_row_offset += rg_rows; continue; }

            let null_bitmap_len = (rg_rows + 7) / 8;
            let compress_flag = rg_bytes[28];
            let encoding_version = rg_bytes[29];
            if compress_flag != crate::storage::on_demand::RG_COMPRESS_NONE || encoding_version < 1 {
                rg_row_offset += rg_rows; continue;
            }
            let body = &rg_bytes[32..];
            let rcix = &footer.col_offsets[rg_i];
            if col_idx >= rcix.len() { rg_row_offset += rg_rows; continue; }
            let col_off = rcix[col_idx] as usize;
            let null_bm = if col_off + null_bitmap_len <= body.len() { &body[col_off..col_off + null_bitmap_len] } else { &[] };
            let has_null_bm = null_bm.iter().any(|&b| b != 0);
            let has_deleted = rg_meta.deletion_count > 0;
            let del_start = rg_id_section_len(
                rg_rows,
                rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
            );
            let del_vec_len = null_bitmap_len;
            let del_bytes = if del_start + del_vec_len <= body.len() { &body[del_start..del_start + del_vec_len] } else { &[] };

            let gids_slice = &group_ids[rg_row_offset..(rg_row_offset + rg_rows).min(group_ids.len())];
            let rg_n = gids_slice.len();

            for i in 0..rg_n {
                if has_deleted { if !del_bytes.is_empty() && (del_bytes[i/8] >> (i%8)) & 1 != 0 { continue; } }
                if has_null_bm { if !null_bm.is_empty() && (null_bm[i/8] >> (i%8)) & 1 != 0 { continue; } }
                let g = unsafe { *gids_slice.get_unchecked(i) } as usize;
                if g < num_groups { unsafe { *seen.get_unchecked_mut(g) = true; } }
            }
            rg_row_offset += rg_rows;
        }

        Ok((0..num_groups)
            .filter(|&g| seen[g] && dict_strings[g].as_str() != NULL_MARKER)
            .count() as i64)
    }

    /// Fast top-k for ORDER BY (string_col, float_col) without Arrow string conversion.
    /// Accepts pre-built dict (dict_strings, row_gids) from global dict cache for O(1) warm calls.
    /// Returns None if not applicable (non-float second col, data not in memory, etc).
    pub fn order_topk_str_float64_with_dict(
        &self,
        dict_strings: &[String], row_gids: &[u16], str_asc: bool,
        f64_col: &str, f64_asc: bool,
        k: usize, offset: usize,
    ) -> io::Result<Option<Vec<usize>>> {
        // Build alphabetical rank mapping (O(dict_size log dict_size), trivial for small dicts)
        let num_dict = dict_strings.len();
        if num_dict == 0 { return Ok(None); }
        let mut sorted_idx: Vec<u16> = (0..num_dict as u16).collect();
        sorted_idx.sort_unstable_by_key(|&i| dict_strings[i as usize].as_str());
        let mut rank_of = vec![0u16; num_dict];
        for (rank, &orig) in sorted_idx.iter().enumerate() {
            rank_of[orig as usize] = rank as u16;
        }
        let k_plus_offset = k + offset;

        // Helper: run the streaming heap top-k loop given f64 values and deletion bitmap
        let run_topk = |f64_vals: &[f64], del_bytes: &[u8]| -> Vec<usize> {
            let scan_rows = row_gids.len().min(f64_vals.len());
            let has_deleted = del_bytes.iter().any(|&b| b != 0);
            let mut heap: std::collections::BinaryHeap<(u64, u64, usize)> =
                std::collections::BinaryHeap::with_capacity(k_plus_offset + 1);
            for i in 0..scan_rows {
                if has_deleted {
                    let b = i / 8; let bit = i % 8;
                    if b < del_bytes.len() && (del_bytes[b] >> bit) & 1 != 0 { continue; }
                }
                let gid = row_gids[i] as usize;
                let sr = if gid < num_dict { rank_of[gid] as u64 } else { u16::MAX as u64 };
                let sk0 = if str_asc { sr } else { u16::MAX as u64 - sr };
                let f = f64_vals[i];
                let fb = f.to_bits();
                let fs = if fb >> 63 == 0 { fb ^ (1u64 << 63) } else { !fb };
                let sk1 = if f64_asc { fs } else { !fs };
                if heap.len() < k_plus_offset {
                    heap.push((sk0, sk1, i));
                } else if let Some(&(h0, h1, _)) = heap.peek() {
                    if (sk0, sk1) < (h0, h1) { heap.pop(); heap.push((sk0, sk1, i)); }
                }
            }
            let mut results: Vec<(u64, u64, usize)> = heap.into_vec();
            results.sort_unstable_by_key(|&(a, b, _)| (a, b));
            results.into_iter().skip(offset).map(|(_, _, idx)| idx).collect()
        };

        // IN-MEMORY PATH
        if self.has_v4_in_memory_data() {
            let schema = self.schema.read();
            let columns = self.columns.read();
            let deleted = self.deleted.read();
            let f64_idx = match schema.get_index(f64_col) { Some(i) => i, None => return Ok(None) };
            let f64_vals: &[f64] = match columns.get(f64_idx) {
                Some(ColumnData::Float64(v)) => v.as_slice(),
                _ => return Ok(None),
            };
            return Ok(Some(run_topk(f64_vals, &deleted)));
        }

        // MMAP PATH: data was auto-flushed to V4 format; read f64 col via scan_columns_mmap
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let f64_idx = match footer.schema.get_index(f64_col) { Some(i) => i, None => return Ok(None) };
        let (scanned, del_bytes) = self.scan_columns_mmap(&[f64_idx], &footer)?;
        if scanned.is_empty() { return Ok(None); }
        let f64_vals: &[f64] = match &scanned[0] {
            ColumnData::Float64(v) => v.as_slice(),
            _ => return Ok(None),
        };
        Ok(Some(run_topk(f64_vals, &del_bytes)))
    }

    /// Supports both in-memory and mmap-only paths.
    pub fn build_string_dict_cache(
        &self,
        col_name: &str,
    ) -> io::Result<Option<(Vec<String>, Vec<u16>)>> {
        if self.has_v4_in_memory_data() {
            // In-memory fast path
            let schema = self.schema.read();
            let columns = self.columns.read();
            let total_rows = self.ids.read().len();

            let col_idx = match schema.get_index(col_name) {
                Some(idx) => idx,
                None => return Ok(None),
            };
            if col_idx >= columns.len() { return Ok(None); }

            let (offsets, data) = match &columns[col_idx] {
                ColumnData::String { offsets, data } => (offsets, data),
                _ => return Ok(None),
            };
            let count = offsets.len().saturating_sub(1).min(total_rows);

            let mut dict_map: ahash::AHashMap<&[u8], u16> = ahash::AHashMap::with_capacity(64);
            let mut dict_strings: Vec<String> = Vec::with_capacity(64);
            let mut group_ids: Vec<u16> = Vec::with_capacity(count);

            for i in 0..count {
                let s = offsets[i] as usize;
                let e = offsets[i + 1] as usize;
                let key = &data[s..e];
                let gid = match dict_map.get(key) {
                    Some(&id) => id,
                    None => {
                        if dict_map.len() >= 4096 {
                            // High-cardinality column: a dictionary is not
                            // worth building; let the caller use plain reads.
                            return Ok(None);
                        }
                        let id = dict_strings.len() as u16;
                        dict_map.insert(key, id);
                        dict_strings.push(std::str::from_utf8(key).unwrap_or("").to_string());
                        id
                    }
                };
                group_ids.push(gid);
            }

            return Ok(Some((dict_strings, group_ids)));
        }

        // MMAP PATH: scan string column from V4 RGs
        self.build_string_dict_cache_mmap(col_name)
    }

    /// Build a compact categorical cache for an integral column without
    /// changing the string-dictionary cache contract.
    pub fn build_numeric_dict_cache(
        &self,
        col_name: &str,
    ) -> io::Result<Option<(Vec<i64>, Vec<u16>)>> {
        let values = if self.has_v4_in_memory_data() {
            let schema = self.schema.read();
            let columns = self.columns.read();
            let Some(column_index) = schema.get_index(col_name) else {
                return Ok(None);
            };
            match columns.get(column_index) {
                Some(ColumnData::Int64(values)) => values.clone(),
                _ => return Ok(None),
            }
        } else {
            let footer = match self.get_or_load_footer()? {
                Some(footer) => footer,
                None => return Ok(None),
            };
            let Some(column_index) = footer.schema.get_index(col_name) else {
                return Ok(None);
            };
            let (columns, _) = self.scan_columns_mmap(&[column_index], &footer)?;
            match columns.into_iter().next() {
                Some(ColumnData::Int64(values)) => values,
                _ => return Ok(None),
            }
        };

        let mut dictionary = ahash::AHashMap::with_capacity(64);
        let mut labels = Vec::with_capacity(64);
        let mut group_ids = Vec::with_capacity(values.len());
        for value in values {
            let group_id = match dictionary.get(&value) {
                Some(&group_id) => group_id,
                None => {
                    if dictionary.len() >= 4096 {
                        return Ok(None);
                    }
                    let group_id = labels.len() as u16;
                    dictionary.insert(value, group_id);
                    labels.push(value);
                    group_id
                }
            };
            group_ids.push(group_id);
        }
        Ok(Some((labels, group_ids)))
    }

    /// Build a compact dictionary for a SQL SUBSTR transform. Unlike the
    /// source-column dictionary cache, this remains useful when the input is
    /// high-cardinality but the transformed value is categorical (for example
    /// an hour extracted from a timestamp).
    pub fn build_string_substr_dict_cache(
        &self,
        col_name: &str,
        start: i64,
        length: Option<i64>,
    ) -> io::Result<Option<(Vec<String>, Vec<u16>)>> {
        if start == 0 || length.is_some_and(|length| length < 0) {
            return Ok(None);
        }

        fn transform(value: &str, start: i64, length: Option<i64>) -> String {
            let chars: Vec<char> = value.chars().collect();
            let begin = if start > 0 {
                (start - 1) as usize
            } else {
                chars.len().saturating_sub((-start) as usize)
            }
            .min(chars.len());
            let end = length
                .map(|length| begin.saturating_add(length as usize).min(chars.len()))
                .unwrap_or(chars.len());
            chars[begin..end].iter().collect()
        }

        let column = if self.has_v4_in_memory_data() {
            let schema = self.schema.read();
            let columns = self.columns.read();
            let Some(index) = schema.get_index(col_name) else {
                return Ok(None);
            };
            let Some(column) = columns.get(index) else {
                return Ok(None);
            };
            column.clone()
        } else {
            let footer = match self.get_or_load_footer()? {
                Some(footer) => footer,
                None => return Ok(None),
            };
            let Some(index) = footer.schema.get_index(col_name) else {
                return Ok(None);
            };
            let (columns, _) = self.scan_columns_mmap(&[index], &footer)?;
            let Some(column) = columns.into_iter().next() else {
                return Ok(None);
            };
            column
        };

        let mut labels = Vec::with_capacity(64);
        let mut label_ids: ahash::AHashMap<String, u16> = ahash::AHashMap::with_capacity(64);
        let mut intern = |value: String| -> Option<u16> {
            if let Some(&group_id) = label_ids.get(&value) {
                return Some(group_id);
            }
            if labels.len() >= 4096 {
                return None;
            }
            let group_id = labels.len() as u16;
            label_ids.insert(value.clone(), group_id);
            labels.push(value);
            Some(group_id)
        };

        let group_ids = match column {
            ColumnData::String { offsets, data } => {
                let mut group_ids = Vec::with_capacity(offsets.len().saturating_sub(1));
                for pair in offsets.windows(2) {
                    let value = std::str::from_utf8(&data[pair[0] as usize..pair[1] as usize])
                        .unwrap_or("");
                    let Some(group_id) = intern(transform(value, start, length)) else {
                        return Ok(None);
                    };
                    group_ids.push(group_id);
                }
                group_ids
            }
            ColumnData::StringDict {
                indices,
                dict_offsets,
                dict_data,
            } => {
                let mut transformed_dictionary = Vec::with_capacity(dict_offsets.len());
                for pair in dict_offsets.windows(2) {
                    let value =
                        std::str::from_utf8(&dict_data[pair[0] as usize..pair[1] as usize])
                            .unwrap_or("");
                    let Some(group_id) = intern(transform(value, start, length)) else {
                        return Ok(None);
                    };
                    transformed_dictionary.push(group_id);
                }
                indices
                    .into_iter()
                    .map(|index| {
                        if index == 0 {
                            0
                        } else {
                            transformed_dictionary
                                .get(index as usize - 1)
                                .copied()
                                .unwrap_or(0)
                        }
                    })
                    .collect()
            }
            _ => return Ok(None),
        };
        Ok(Some((labels, group_ids)))
    }

    /// MMAP PATH: build string dict cache by scanning V4 RGs
    fn build_string_dict_cache_mmap(
        &self,
        col_name: &str,
    ) -> io::Result<Option<(Vec<String>, Vec<u16>)>> {
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let col_idx = match footer.schema.get_index(col_name) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let (scanned_cols, _del_bytes) = self.scan_columns_mmap(&[col_idx], &footer)?;
        if scanned_cols.is_empty() { return Ok(None); }

        match &scanned_cols[0] {
            ColumnData::StringDict { indices, dict_offsets, dict_data } => {
                // FAST PATH: column stored as StringDict — decode only the small dict (~10 entries),
                // then map u32 indices directly to u16 group_ids without any string hashing.
                let dict_count = dict_offsets.len().saturating_sub(1);
                let dict_strings: Vec<String> = (0..dict_count)
                    .map(|i| {
                        let s = dict_offsets[i] as usize;
                        let e = dict_offsets[i + 1] as usize;
                        std::str::from_utf8(&dict_data[s..e]).unwrap_or("").to_string()
                    })
                    .collect();
                // Indices are 1-based (0 = null sentinel); subtract 1 for group_id
                let group_ids: Vec<u16> = indices.iter()
                    .map(|&idx| if idx == 0 { 0 } else { (idx - 1) as u16 })
                    .collect();
                Ok(Some((dict_strings, group_ids)))
            }
            ColumnData::String { offsets, data } => {
                // Fallback: plain string column — build dict via hash map
                let count = offsets.len().saturating_sub(1);
                let mut dict_map: ahash::AHashMap<&[u8], u16> = ahash::AHashMap::with_capacity(64);
                let mut dict_strings: Vec<String> = Vec::with_capacity(64);
                let mut group_ids: Vec<u16> = Vec::with_capacity(count);
                for i in 0..count {
                    let s = offsets[i] as usize;
                    let e = offsets[i + 1] as usize;
                    let key = &data[s..e];
                    let gid = match dict_map.get(key) {
                        Some(&id) => id,
                        None => {
                            if dict_map.len() >= 4096 {
                                return Ok(None);
                            }
                            let id = dict_strings.len() as u16;
                            dict_map.insert(key, id);
                            dict_strings.push(std::str::from_utf8(key).unwrap_or("").to_string());
                            id
                        }
                    };
                    group_ids.push(gid);
                }
                Ok(Some((dict_strings, group_ids)))
            }
            _ => Ok(None),
        }
    }

    /// Execute a narrow mmap-native GROUP BY for string keys with COUNT(*),
    /// optional COUNT(DISTINCT string_col), SUM(CASE string_col IN (...) THEN 1 ELSE 0 END),
    /// and MAX(string_col). It computes the result from column bytes for this call.
    pub fn execute_string_group_distinct_case_agg(
        &self,
        group_col: &str,
        distinct_cols: &[&str],
        case_specs: &[(&str, Vec<String>)],
        max_cols: &[&str],
    ) -> io::Result<Option<Vec<NativeStringGroupAgg>>> {
        if self.has_v4_in_memory_data() || self.column_has_nulls(group_col) {
            return Ok(None);
        }

        struct RawStringCol<'a> {
            kind: RawStringKind<'a>,
            nulls: &'a [u8],
        }

        enum RawStringKind<'a> {
            Dict {
                indices: std::borrow::Cow<'a, [u32]>,
                offsets: std::borrow::Cow<'a, [u32]>,
                data: &'a [u8],
                rows: usize,
            },
            Plain {
                offsets: std::borrow::Cow<'a, [u32]>,
                data: &'a [u8],
                rows: usize,
            },
        }

        impl<'a> RawStringCol<'a> {
            #[inline(always)]
            fn is_null(&self, row: usize) -> bool {
                !self.nulls.is_empty()
                    && row / 8 < self.nulls.len()
                    && ((self.nulls[row / 8] >> (row % 8)) & 1) != 0
            }

            #[inline(always)]
            fn value(&self, row: usize) -> Option<&'a [u8]> {
                if self.is_null(row) {
                    return None;
                }
                match &self.kind {
                    RawStringKind::Dict {
                        indices,
                        offsets,
                        data,
                        rows,
                    } => {
                        if row >= *rows || row >= indices.len() {
                            return None;
                        }
                        let idx = indices[row];
                        if idx == 0 {
                            return None;
                        }
                        let di = (idx - 1) as usize;
                        if di >= offsets.len() {
                            return None;
                        }
                        let start = offsets[di] as usize;
                        let end = if di + 1 < offsets.len() {
                            offsets[di + 1] as usize
                        } else {
                            data.len()
                        };
                        if start <= end && end <= data.len() {
                            Some(&data[start..end])
                        } else {
                            None
                        }
                    }
                    RawStringKind::Plain {
                        offsets,
                        data,
                        rows,
                    } => {
                        if row >= *rows || row + 1 >= offsets.len() {
                            return None;
                        }
                        let start = offsets[row] as usize;
                        let end = offsets[row + 1] as usize;
                        if start <= end && end <= data.len() {
                            Some(&data[start..end])
                        } else {
                            None
                        }
                    }
                }
            }
        }

        fn parse_raw_string_col<'a>(
            body: &'a [u8],
            rcix: &[u32],
            col_idx: usize,
            col_type: ColumnType,
            rg_rows: usize,
            null_bitmap_len: usize,
        ) -> Option<RawStringCol<'a>> {
            if col_idx >= rcix.len() {
                return None;
            }
            let col_off = rcix[col_idx] as usize;
            if col_off + null_bitmap_len + 1 > body.len() {
                return None;
            }
            let nulls = &body[col_off..col_off + null_bitmap_len];
            let encoded = &body[col_off + null_bitmap_len..];
            if encoded.first().copied()? != COL_ENCODING_PLAIN {
                return None;
            }
            let data = &encoded[1..];
            match col_type {
                ColumnType::StringDict => {
                    if data.len() < 16 {
                        return None;
                    }
                    let rows = u64::from_le_bytes(data[0..8].try_into().ok()?) as usize;
                    let dict_size = u64::from_le_bytes(data[8..16].try_into().ok()?) as usize;
                    let rows = rows.min(rg_rows);
                    let indices_start = 16usize;
                    let indices_len = rows.checked_mul(4)?;
                    let stored_indices_len = (u64::from_le_bytes(
                        data[0..8].try_into().ok()?,
                    ) as usize)
                        .checked_mul(4)?;
                    let dict_off_start = indices_start.checked_add(stored_indices_len)?;
                    let dict_offsets_len = dict_size.checked_mul(4)?;
                    let dict_data_len_off = dict_off_start.checked_add(dict_offsets_len)?;
                    if indices_start + indices_len > data.len()
                        || dict_data_len_off + 8 > data.len()
                    {
                        return None;
                    }
                    let dict_data_len = u64::from_le_bytes(
                        data[dict_data_len_off..dict_data_len_off + 8]
                            .try_into()
                            .ok()?,
                    ) as usize;
                    let dict_data_start = dict_data_len_off + 8;
                    if dict_data_start + dict_data_len > data.len() {
                        return None;
                    }
                    let indices = bytes_as_u32_slice(&data[indices_start..], rows);
                    let offsets = bytes_as_u32_slice(&data[dict_off_start..], dict_size);
                    Some(RawStringCol {
                        kind: RawStringKind::Dict {
                            indices,
                            offsets,
                            data: &data[dict_data_start..dict_data_start + dict_data_len],
                            rows,
                        },
                        nulls,
                    })
                }
                ColumnType::String => {
                    if data.len() < 8 {
                        return None;
                    }
                    let stored_rows = u64::from_le_bytes(data[0..8].try_into().ok()?) as usize;
                    let rows = stored_rows.min(rg_rows);
                    let offsets_len = stored_rows.checked_add(1)?.checked_mul(4)?;
                    let data_len_off = 8usize.checked_add(offsets_len)?;
                    if data_len_off + 8 > data.len() {
                        return None;
                    }
                    let string_data_len = u64::from_le_bytes(
                        data[data_len_off..data_len_off + 8].try_into().ok()?,
                    ) as usize;
                    let string_data_start = data_len_off + 8;
                    if string_data_start + string_data_len > data.len() {
                        return None;
                    }
                    let offsets = bytes_as_u32_slice(&data[8..], stored_rows + 1);
                    Some(RawStringCol {
                        kind: RawStringKind::Plain {
                            offsets,
                            data: &data[string_data_start..string_data_start + string_data_len],
                            rows,
                        },
                        nulls,
                    })
                }
                _ => None,
            }
        }

        #[inline(always)]
        fn fingerprint_bytes(bytes: &[u8]) -> u64 {
            let mut split = 0usize;
            while split < bytes.len() && !bytes[split].is_ascii_digit() {
                split += 1;
            }
            if split > 0 && split < bytes.len() {
                let mut number = 0u64;
                let mut valid = true;
                for &byte in &bytes[split..] {
                    if !byte.is_ascii_digit() {
                        valid = false;
                        break;
                    }
                    number = number
                        .saturating_mul(10)
                        .saturating_add((byte - b'0') as u64);
                }
                if valid {
                    let mut prefix = 0u64;
                    for &byte in &bytes[..split.min(8)] {
                        prefix = (prefix << 8) | byte as u64;
                    }
                    return 0xA5A5_5A5A_D3C3_B4B4u64
                        ^ number
                        ^ prefix.rotate_left(27)
                        ^ ((split as u64) << 56);
                }
            }

            use std::hash::Hasher;
            let mut hasher = ahash::AHasher::default();
            hasher.write(bytes);
            hasher.finish()
        }

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        if footer.row_groups.iter().any(|rg| rg.deletion_count > 0) {
            return Ok(None);
        }

        let schema = &footer.schema;
        let group_idx = match schema.get_index(group_col) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        if !matches!(schema.columns[group_idx].1, ColumnType::String | ColumnType::StringDict) {
            return Ok(None);
        }

        let mut distinct_indices = Vec::with_capacity(distinct_cols.len());
        for col in distinct_cols {
            let Some(idx) = schema.get_index(col) else {
                return Ok(None);
            };
            if !matches!(schema.columns[idx].1, ColumnType::String | ColumnType::StringDict) {
                return Ok(None);
            }
            distinct_indices.push(idx);
        }

        let mut case_indices = Vec::with_capacity(case_specs.len());
        let mut case_slots = Vec::with_capacity(case_specs.len());
        for (col, _) in case_specs {
            let Some(idx) = schema.get_index(col) else {
                return Ok(None);
            };
            if !matches!(schema.columns[idx].1, ColumnType::String | ColumnType::StringDict) {
                return Ok(None);
            }
            if let Some(slot) = case_indices.iter().position(|&existing| existing == idx) {
                case_slots.push(slot);
            } else {
                let slot = case_indices.len();
                case_indices.push(idx);
                case_slots.push(slot);
            }
        }

        let mut max_indices = Vec::with_capacity(max_cols.len());
        for col in max_cols {
            let Some(idx) = schema.get_index(col) else {
                return Ok(None);
            };
            if !matches!(schema.columns[idx].1, ColumnType::String | ColumnType::StringDict) {
                return Ok(None);
            }
            max_indices.push(idx);
        }

        let max_col_idx = std::iter::once(group_idx)
            .chain(distinct_indices.iter().copied())
            .chain(case_indices.iter().copied())
            .chain(max_indices.iter().copied())
            .max()
            .unwrap_or(group_idx);
        let all_rcix = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
            rg_meta.row_count == 0
                || footer
                    .col_offsets
                    .get(rg_i)
                    .map_or(false, |v| v.len() > max_col_idx)
        });
        if !all_rcix {
            return Ok(None);
        }

        let (dict_strings, group_ids) = match self.build_string_dict_cache(group_col)? {
            Some(pair) if !pair.0.is_empty() => pair,
            _ => return Ok(None),
        };
        let num_groups = dict_strings.len();
        let distinct_len = distinct_indices.len();
        let case_len = case_specs.len();
        let max_len = max_indices.len();

        let mut counts = vec![0i64; num_groups];
        let mut distinct_seen: Vec<Vec<u64>> = (0..num_groups * distinct_len)
            .map(|_| Vec::with_capacity(4))
            .collect();
        let mut case_counts = vec![0i64; num_groups * case_len];
        let mut max_values: Vec<Option<Vec<u8>>> = (0..num_groups * max_len)
            .map(|_| None)
            .collect();

        let case_literals: Vec<Vec<&[u8]>> = case_specs
            .iter()
            .map(|(_, literals)| literals.iter().map(|s| s.as_bytes()).collect())
            .collect();
        let mut case_specs_by_col = vec![Vec::new(); case_indices.len()];
        for (case_idx, &slot) in case_slots.iter().enumerate() {
            case_specs_by_col[slot].push(case_idx);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for native string group aggregation"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let mut rg_row_offset = 0usize;

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                rg_row_offset += rg_rows;
                continue;
            }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("native string group aggregation RG past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.len() < 32
                || rg_bytes[28] != RG_COMPRESS_NONE
                || rg_bytes[29] < 1
            {
                return Ok(None);
            }

            let body = &rg_bytes[32..];
            let null_bitmap_len = (rg_rows + 7) / 8;
            let rcix = &footer.col_offsets[rg_i];
            let gids_slice = match group_ids.get(rg_row_offset..(rg_row_offset + rg_rows).min(group_ids.len())) {
                Some(slice) if slice.len() == rg_rows => slice,
                _ => return Ok(None),
            };

            let distinct_cols_raw: Option<Vec<_>> = distinct_indices
                .iter()
                .map(|&idx| {
                    parse_raw_string_col(
                        body,
                        rcix,
                        idx,
                        schema.columns[idx].1,
                        rg_rows,
                        null_bitmap_len,
                    )
                })
                .collect();
            let Some(distinct_cols_raw) = distinct_cols_raw else {
                return Ok(None);
            };

            let case_cols_raw: Option<Vec<_>> = case_indices
                .iter()
                .map(|&idx| {
                    parse_raw_string_col(
                        body,
                        rcix,
                        idx,
                        schema.columns[idx].1,
                        rg_rows,
                        null_bitmap_len,
                    )
                })
                .collect();
            let Some(case_cols_raw) = case_cols_raw else {
                return Ok(None);
            };

            let max_cols_raw: Option<Vec<_>> = max_indices
                .iter()
                .map(|&idx| {
                    parse_raw_string_col(
                        body,
                        rcix,
                        idx,
                        schema.columns[idx].1,
                        rg_rows,
                        null_bitmap_len,
                    )
                })
                .collect();
            let Some(max_cols_raw) = max_cols_raw else {
                return Ok(None);
            };

            for row in 0..rg_rows {
                let gid = unsafe { *gids_slice.get_unchecked(row) } as usize;
                if gid >= num_groups {
                    continue;
                }
                unsafe { *counts.get_unchecked_mut(gid) += 1; }

                for (di, col) in distinct_cols_raw.iter().enumerate() {
                    if let Some(value) = col.value(row) {
                        let fp = fingerprint_bytes(value);
                        let slot = gid * distinct_len + di;
                        let seen = unsafe { distinct_seen.get_unchecked_mut(slot) };
                        if !seen.contains(&fp) {
                            seen.push(fp);
                        }
                    }
                }

                for (case_col_slot, col) in case_cols_raw.iter().enumerate() {
                    if let Some(value) = col.value(row) {
                        for &ci in unsafe { case_specs_by_col.get_unchecked(case_col_slot) } {
                            if case_literals[ci].iter().any(|literal| *literal == value) {
                                unsafe {
                                    *case_counts.get_unchecked_mut(gid * case_len + ci) += 1;
                                }
                            }
                        }
                    }
                }

                for (mi, col) in max_cols_raw.iter().enumerate() {
                    if let Some(value) = col.value(row) {
                        let slot = gid * max_len + mi;
                        let current = unsafe { max_values.get_unchecked_mut(slot) };
                        if current
                            .as_ref()
                            .map_or(true, |existing| value > existing.as_slice())
                        {
                            *current = Some(value.to_vec());
                        }
                    }
                }
            }

            rg_row_offset += rg_rows;
        }

        let mut results = Vec::with_capacity(num_groups);
        for gid in 0..num_groups {
            if counts[gid] == 0 {
                continue;
            }
            let distinct_counts = (0..distinct_len)
                .map(|di| distinct_seen[gid * distinct_len + di].len() as i64)
                .collect();
            let group_case_counts = (0..case_len)
                .map(|ci| case_counts[gid * case_len + ci])
                .collect();
            let group_max_values = (0..max_len)
                .map(|mi| {
                    max_values[gid * max_len + mi]
                        .as_ref()
                        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                })
                .collect();
            results.push(NativeStringGroupAgg {
                key: dict_strings[gid].clone(),
                count: counts[gid],
                distinct_counts,
                case_counts: group_case_counts,
                max_values: group_max_values,
            });
        }

        Ok(Some(results))
    }

    /// Execute GROUP BY + aggregate using pre-built dict cache.
    /// Supports both in-memory and mmap-only paths.
    pub fn execute_group_agg_cached(
        &self,
        dict_strings: &[String],
        group_ids: &[u16],
        agg_cols: &[(&str, bool)], // (col_name, is_count_star)
    ) -> io::Result<Option<Vec<(String, Vec<(f64, i64)>)>>> {
        if !self.has_v4_in_memory_data() {
            // MMAP PATH: scan agg columns from disk, then aggregate with pre-built dict
            return self.execute_group_agg_cached_mmap(dict_strings, group_ids, agg_cols);
        }

        let schema = self.schema.read();
        let columns = self.columns.read();
        let deleted = self.deleted.read();
        let total_rows = self.ids.read().len();

        let has_deleted = deleted.iter().any(|&b| b != 0);
        let scan_rows = total_rows.min(group_ids.len());
        let num_groups = dict_strings.len();
        let num_aggs = agg_cols.len();

        struct AggSlice<'a> { i64_vals: Option<&'a [i64]>, f64_vals: Option<&'a [f64]>, is_count: bool }
        let agg_slices: Vec<AggSlice> = agg_cols.iter().map(|(name, is_count)| {
            if *is_count {
                AggSlice { i64_vals: None, f64_vals: None, is_count: true }
            } else if let Some(idx) = schema.get_index(name) {
                if idx < columns.len() {
                    match &columns[idx] {
                        ColumnData::Int64(v) => AggSlice { i64_vals: Some(v.as_slice()), f64_vals: None, is_count: false },
                        ColumnData::Float64(v) => AggSlice { i64_vals: None, f64_vals: Some(v.as_slice()), is_count: false },
                        _ => AggSlice { i64_vals: None, f64_vals: None, is_count: true },
                    }
                } else { AggSlice { i64_vals: None, f64_vals: None, is_count: true } }
            } else { AggSlice { i64_vals: None, f64_vals: None, is_count: true } }
        }).collect();

        let flat_len = num_groups * num_aggs;
        let mut flat_sums = vec![0.0f64; flat_len];
        let mut flat_counts = vec![0i64; flat_len];

        // Single-pass aggregation with O(1) group lookup via cached group_ids
        if has_deleted {
            for i in 0..scan_rows {
                let b = i / 8; let bit = i % 8;
                if b < deleted.len() && (deleted[b] >> bit) & 1 != 0 { continue; }
                let base = group_ids[i] as usize * num_aggs;
                for (ai, agg) in agg_slices.iter().enumerate() {
                    flat_counts[base + ai] += 1;
                    if !agg.is_count {
                        if let Some(vals) = agg.f64_vals { if i < vals.len() { flat_sums[base + ai] += vals[i]; } }
                        else if let Some(vals) = agg.i64_vals { if i < vals.len() { flat_sums[base + ai] += vals[i] as f64; } }
                    }
                }
            }
        } else {
            for i in 0..scan_rows {
                let base = group_ids[i] as usize * num_aggs;
                for (ai, agg) in agg_slices.iter().enumerate() {
                    flat_counts[base + ai] += 1;
                    if !agg.is_count {
                        if let Some(vals) = agg.f64_vals { if i < vals.len() { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *vals.get_unchecked(i); } } }
                        else if let Some(vals) = agg.i64_vals { if i < vals.len() { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *vals.get_unchecked(i) as f64; } } }
                    }
                }
            }
        }

        let results: Vec<(String, Vec<(f64, i64)>)> = (0..num_groups)
            .filter(|&gid| flat_counts[gid * num_aggs] > 0)
            .map(|gid| {
                let aggs: Vec<(f64, i64)> = (0..num_aggs)
                    .map(|ai| (flat_sums[gid * num_aggs + ai], flat_counts[gid * num_aggs + ai]))
                    .collect();
                (dict_strings[gid].clone(), aggs)
            })
            .collect();

        Ok(Some(results))
    }

    /// Full numeric aggregate family over two pre-encoded categorical keys.
    /// Returns the dense combined key slot and per-aggregate
    /// (count, sum, min, max, is_integral) state.
    pub fn execute_group_stats_2col_cached(
        &self,
        group_ids1: &[u16],
        group_ids2: &[u16],
        dict2_size: usize,
        agg_cols: &[(&str, bool)],
    ) -> io::Result<Option<Vec<(usize, Vec<(i64, f64, f64, f64, bool)>)>>> {
        use rayon::prelude::*;

        if group_ids1.len() != group_ids2.len()
            || group_ids1.is_empty()
            || dict2_size == 0
            || agg_cols.is_empty()
            || self.has_pending_deltas()
        {
            return Ok(None);
        }
        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        let mut unique_indices = Vec::new();
        let mut aggregate_sources = Vec::with_capacity(agg_cols.len());
        for &(column, count_star) in agg_cols {
            if count_star {
                aggregate_sources.push(None);
                continue;
            }
            let Some(index) = footer.schema.get_index(column) else {
                return Ok(None);
            };
            if !matches!(
                footer.schema.columns[index].1,
                ColumnType::Int64
                    | ColumnType::Int8
                    | ColumnType::Int16
                    | ColumnType::Int32
                    | ColumnType::UInt8
                    | ColumnType::UInt16
                    | ColumnType::UInt32
                    | ColumnType::UInt64
                    | ColumnType::Timestamp
                    | ColumnType::Date
                    | ColumnType::Float64
                    | ColumnType::Float32
            ) {
                return Ok(None);
            }
            let source = unique_indices
                .iter()
                .position(|&existing| existing == index)
                .unwrap_or_else(|| {
                    unique_indices.push(index);
                    unique_indices.len() - 1
                });
            aggregate_sources.push(Some(source));
        }
        let parallel = self.scan_numeric_columns_mmap_parallel(&unique_indices, &footer)?;
        let (columns, deleted) = match parallel {
            Some(columns) => (columns, Vec::new()),
            None => {
                let (columns, deleted) = self.scan_columns_mmap(&unique_indices, &footer)?;
                (
                    columns.into_iter().map(std::sync::Arc::new).collect(),
                    deleted,
                )
            }
        };
        if columns.len() != unique_indices.len() {
            return Ok(None);
        }
        let group_count1 = group_ids1
            .iter()
            .copied()
            .max()
            .map_or(0, |value| value as usize + 1);
        let group_count = match group_count1.checked_mul(dict2_size) {
            Some(count) if count <= 65_536 => count,
            _ => return Ok(None),
        };
        let aggregate_count = agg_cols.len();
        let row_count = group_ids1.len();

        struct Flat {
            seen: Vec<bool>,
            counts: Vec<i64>,
            sums: Vec<f64>,
            mins: Vec<f64>,
            maxs: Vec<f64>,
        }
        let empty = || Flat {
            seen: vec![false; group_count],
            counts: vec![0; group_count * aggregate_count],
            sums: vec![0.0; group_count * aggregate_count],
            mins: vec![f64::INFINITY; group_count * aggregate_count],
            maxs: vec![f64::NEG_INFINITY; group_count * aggregate_count],
        };
        let merged = (0..row_count)
            .into_par_iter()
            .fold(empty, |mut part, row| {
                if deleted
                    .get(row / 8)
                    .is_some_and(|byte| ((byte >> (row % 8)) & 1) != 0)
                {
                    return part;
                }
                let group = group_ids1[row] as usize * dict2_size + group_ids2[row] as usize;
                if group >= group_count {
                    return part;
                }
                part.seen[group] = true;
                let base = group * aggregate_count;
                for (aggregate, source) in aggregate_sources.iter().enumerate() {
                    let value = match source {
                        None => Some(0.0),
                        Some(source) => match columns[*source].as_ref() {
                            ColumnData::Int64(values) => values.get(row).map(|&value| value as f64),
                            ColumnData::Float64(values) => values.get(row).copied(),
                            _ => None,
                        },
                    };
                    let Some(value) = value else {
                        continue;
                    };
                    let slot = base + aggregate;
                    part.counts[slot] += 1;
                    part.sums[slot] += value;
                    part.mins[slot] = part.mins[slot].min(value);
                    part.maxs[slot] = part.maxs[slot].max(value);
                }
                part
            })
            .reduce(empty, |mut target, source| {
                for group in 0..group_count {
                    target.seen[group] |= source.seen[group];
                }
                for slot in 0..target.counts.len() {
                    target.counts[slot] += source.counts[slot];
                    target.sums[slot] += source.sums[slot];
                    target.mins[slot] = target.mins[slot].min(source.mins[slot]);
                    target.maxs[slot] = target.maxs[slot].max(source.maxs[slot]);
                }
                target
            });
        let aggregate_is_int: Vec<bool> = aggregate_sources
            .iter()
            .map(|source| {
                source.is_none()
                    || source.is_some_and(|source| {
                        matches!(columns[source].as_ref(), ColumnData::Int64(_))
                    })
            })
            .collect();
        Ok(Some(
            merged
                .seen
                .into_iter()
                .enumerate()
                .filter_map(|(group, present)| {
                    present.then(|| {
                        let base = group * aggregate_count;
                        let stats = (0..aggregate_count)
                            .map(|aggregate| {
                                let slot = base + aggregate;
                                (
                                    merged.counts[slot],
                                    merged.sums[slot],
                                    merged.mins[slot],
                                    merged.maxs[slot],
                                    aggregate_is_int[aggregate],
                                )
                            })
                            .collect();
                        (group, stats)
                    })
                })
                .collect(),
        ))
    }

    /// Full numeric aggregate family over one pre-encoded categorical key.
    /// Decoded numeric columns are shared with other analytical scans, while
    /// every invocation still performs the GROUP BY reduction over all rows.
    pub fn execute_group_stats_cached(
        &self,
        group_ids: &[u16],
        group_count: usize,
        agg_cols: &[(&str, bool)],
    ) -> io::Result<Option<Vec<(usize, Vec<(i64, f64, f64, f64, bool)>)>>> {
        use rayon::prelude::*;

        if group_ids.is_empty()
            || group_count == 0
            || group_count > 65_536
            || agg_cols.is_empty()
            || self.has_pending_deltas()
        {
            return Ok(None);
        }
        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        let mut unique_indices = Vec::new();
        let mut aggregate_sources = Vec::with_capacity(agg_cols.len());
        for &(column, count_star) in agg_cols {
            if count_star {
                aggregate_sources.push(None);
                continue;
            }
            let Some(index) = footer.schema.get_index(column) else {
                return Ok(None);
            };
            if self.column_has_nulls(column)
                || !matches!(
                    footer.schema.columns[index].1,
                    ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date
                        | ColumnType::Float64
                        | ColumnType::Float32
                )
            {
                return Ok(None);
            }
            let source = unique_indices
                .iter()
                .position(|&existing| existing == index)
                .unwrap_or_else(|| {
                    unique_indices.push(index);
                    unique_indices.len() - 1
                });
            aggregate_sources.push(Some(source));
        }
        let columns = match self.scan_numeric_columns_mmap_parallel(&unique_indices, &footer)? {
            Some(columns) => columns,
            None => return Ok(None),
        };
        if columns.len() != unique_indices.len() {
            return Ok(None);
        }
        let aggregate_count = agg_cols.len();
        struct Flat {
            seen: Vec<bool>,
            counts: Vec<i64>,
            sums: Vec<f64>,
            mins: Vec<f64>,
            maxs: Vec<f64>,
        }
        let empty = || Flat {
            seen: vec![false; group_count],
            counts: vec![0; group_count * aggregate_count],
            sums: vec![0.0; group_count * aggregate_count],
            mins: vec![f64::INFINITY; group_count * aggregate_count],
            maxs: vec![f64::NEG_INFINITY; group_count * aggregate_count],
        };
        let merged = (0..group_ids.len())
            .into_par_iter()
            .fold(empty, |mut part, row| {
                let group = unsafe { *group_ids.get_unchecked(row) } as usize;
                if group >= group_count {
                    return part;
                }
                part.seen[group] = true;
                let base = group * aggregate_count;
                for (aggregate, source) in aggregate_sources.iter().enumerate() {
                    let value = match source {
                        None => Some(0.0),
                        Some(source) => match columns[*source].as_ref() {
                            ColumnData::Int64(values) => {
                                values.get(row).map(|&value| value as f64)
                            }
                            ColumnData::Float64(values) => values.get(row).copied(),
                            _ => None,
                        },
                    };
                    let Some(value) = value else {
                        continue;
                    };
                    let slot = base + aggregate;
                    part.counts[slot] += 1;
                    part.sums[slot] += value;
                    part.mins[slot] = part.mins[slot].min(value);
                    part.maxs[slot] = part.maxs[slot].max(value);
                }
                part
            })
            .reduce(empty, |mut target, source| {
                for group in 0..group_count {
                    target.seen[group] |= source.seen[group];
                }
                for slot in 0..target.counts.len() {
                    target.counts[slot] += source.counts[slot];
                    target.sums[slot] += source.sums[slot];
                    target.mins[slot] = target.mins[slot].min(source.mins[slot]);
                    target.maxs[slot] = target.maxs[slot].max(source.maxs[slot]);
                }
                target
            });
        let aggregate_is_int = aggregate_sources
            .iter()
            .map(|source| {
                source.is_none()
                    || source.is_some_and(|source| {
                        matches!(columns[source].as_ref(), ColumnData::Int64(_))
                    })
            })
            .collect::<Vec<_>>();
        Ok(Some(
            merged
                .seen
                .into_iter()
                .enumerate()
                .filter_map(|(group, present)| {
                    present.then(|| {
                        let base = group * aggregate_count;
                        let stats = (0..aggregate_count)
                            .map(|aggregate| {
                                let slot = base + aggregate;
                                (
                                    merged.counts[slot],
                                    merged.sums[slot],
                                    merged.mins[slot],
                                    merged.maxs[slot],
                                    aggregate_is_int[aggregate],
                                )
                            })
                            .collect();
                        (group, stats)
                    })
                })
                .collect(),
        ))
    }

    /// AVG(numerator / (denominator + offset)) for one or more derived
    /// features over a cached categorical key. This is the common feature-
    /// engineering shape used for ratios and per-event rates.
    pub fn execute_group_ratio_avg_cached(
        &self,
        group_ids: &[u16],
        group_count: usize,
        ratios: &[(&str, &str, f64)],
    ) -> io::Result<Option<Vec<(usize, Vec<(f64, i64)>)>>> {
        use rayon::prelude::*;

        if group_ids.is_empty()
            || group_count == 0
            || group_count > 65_536
            || ratios.is_empty()
            || self.has_pending_deltas()
        {
            return Ok(None);
        }
        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        let mut unique_indices = Vec::new();
        let mut sources = Vec::with_capacity(ratios.len());
        for &(numerator, denominator, offset) in ratios {
            if !offset.is_finite()
                || self.column_has_nulls(numerator)
                || self.column_has_nulls(denominator)
            {
                return Ok(None);
            }
            let Some(numerator_index) = footer.schema.get_index(numerator) else {
                return Ok(None);
            };
            let Some(denominator_index) = footer.schema.get_index(denominator) else {
                return Ok(None);
            };
            let numeric = |index: usize| {
                matches!(
                    footer.schema.columns[index].1,
                    ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date
                        | ColumnType::Float64
                        | ColumnType::Float32
                )
            };
            if !numeric(numerator_index) || !numeric(denominator_index) {
                return Ok(None);
            }
            let mut position = |index: usize| {
                unique_indices
                    .iter()
                    .position(|&existing| existing == index)
                    .unwrap_or_else(|| {
                        unique_indices.push(index);
                        unique_indices.len() - 1
                    })
            };
            sources.push((position(numerator_index), position(denominator_index), offset));
        }
        let Some(columns) = self.scan_numeric_columns_mmap_parallel(&unique_indices, &footer)? else {
            return Ok(None);
        };
        let value_at = |column: usize, row: usize| match columns[column].as_ref() {
            ColumnData::Int64(values) => values.get(row).map(|&value| value as f64),
            ColumnData::Float64(values) => values.get(row).copied(),
            _ => None,
        };
        struct Flat {
            seen: Vec<bool>,
            sums: Vec<f64>,
            counts: Vec<i64>,
            valid: bool,
        }
        let ratio_count = ratios.len();
        let empty = || Flat {
            seen: vec![false; group_count],
            sums: vec![0.0; group_count * ratio_count],
            counts: vec![0; group_count * ratio_count],
            valid: true,
        };
        let merged = (0..group_ids.len())
            .into_par_iter()
            .fold(empty, |mut part, row| {
                let group = unsafe { *group_ids.get_unchecked(row) } as usize;
                if group >= group_count {
                    part.valid = false;
                    return part;
                }
                part.seen[group] = true;
                let base = group * ratio_count;
                for (ratio, &(numerator, denominator, offset)) in sources.iter().enumerate() {
                    let Some(numerator) = value_at(numerator, row) else {
                        part.valid = false;
                        continue;
                    };
                    let Some(denominator) = value_at(denominator, row) else {
                        part.valid = false;
                        continue;
                    };
                    let divisor = denominator + offset;
                    if divisor == 0.0 {
                        part.valid = false;
                        continue;
                    }
                    let slot = base + ratio;
                    part.sums[slot] += numerator / divisor;
                    part.counts[slot] += 1;
                }
                part
            })
            .reduce(empty, |mut target, source| {
                target.valid &= source.valid;
                for group in 0..group_count {
                    target.seen[group] |= source.seen[group];
                }
                for slot in 0..target.sums.len() {
                    target.sums[slot] += source.sums[slot];
                    target.counts[slot] += source.counts[slot];
                }
                target
            });
        if !merged.valid {
            return Ok(None);
        }
        Ok(Some(
            merged
                .seen
                .into_iter()
                .enumerate()
                .filter_map(|(group, present)| {
                    present.then(|| {
                        let base = group * ratio_count;
                        (
                            group,
                            (0..ratio_count)
                                .map(|ratio| {
                                    (merged.sums[base + ratio], merged.counts[base + ratio])
                                })
                                .collect(),
                        )
                    })
                })
                .collect(),
        ))
    }

    /// Fused conjunction of numeric ranges with scalar numeric aggregates.
    /// All referenced columns come from the bounded decoded-column cache, so
    /// repeated exploratory filters avoid decompression but still scan rows.
    pub fn execute_filtered_numeric_agg_cached(
        &self,
        predicates: &[(&str, f64, f64)],
        agg_cols: &[&str],
    ) -> io::Result<Option<Vec<(i64, f64, f64, f64, bool)>>> {
        use rayon::prelude::*;

        if predicates.is_empty() || agg_cols.is_empty() || self.has_pending_deltas() {
            return Ok(None);
        }
        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        let mut unique_indices = Vec::new();
        let mut position_for = |column: &str| -> Option<usize> {
            let index = footer.schema.get_index(column)?;
            if self.column_has_nulls(column)
                || !matches!(
                    footer.schema.columns[index].1,
                    ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date
                        | ColumnType::Float64
                        | ColumnType::Float32
                )
            {
                return None;
            }
            Some(
                unique_indices
                    .iter()
                    .position(|&existing| existing == index)
                    .unwrap_or_else(|| {
                        unique_indices.push(index);
                        unique_indices.len() - 1
                    }),
            )
        };
        let predicate_sources = predicates
            .iter()
            .map(|(column, low, high)| {
                position_for(column).map(|source| (source, *low, *high))
            })
            .collect::<Option<Vec<_>>>();
        let Some(predicate_sources) = predicate_sources else {
            return Ok(None);
        };
        let aggregate_sources = agg_cols
            .iter()
            .map(|column| {
                if *column == "*" || *column == "1" {
                    Some(None)
                } else {
                    position_for(column).map(Some)
                }
            })
            .collect::<Option<Vec<_>>>();
        let Some(aggregate_sources) = aggregate_sources else {
            return Ok(None);
        };
        let Some(columns) = self.scan_numeric_columns_mmap_parallel(&unique_indices, &footer)? else {
            return Ok(None);
        };
        let value_at = |column: usize, row: usize| match columns[column].as_ref() {
            ColumnData::Int64(values) => values.get(row).map(|&value| value as f64),
            ColumnData::Float64(values) => values.get(row).copied(),
            _ => None,
        };
        let row_count = columns
            .first()
            .map(|column| column.len())
            .unwrap_or_else(|| self.active_row_count() as usize);
        struct Part {
            counts: Vec<i64>,
            sums: Vec<f64>,
            mins: Vec<f64>,
            maxs: Vec<f64>,
            valid: bool,
        }
        let empty = || Part {
            counts: vec![0; agg_cols.len()],
            sums: vec![0.0; agg_cols.len()],
            mins: vec![f64::INFINITY; agg_cols.len()],
            maxs: vec![f64::NEG_INFINITY; agg_cols.len()],
            valid: true,
        };
        let merged = (0..row_count)
            .into_par_iter()
            .fold(empty, |mut part, row| {
                for &(source, low, high) in &predicate_sources {
                    let Some(value) = value_at(source, row) else {
                        part.valid = false;
                        return part;
                    };
                    if value < low || value > high {
                        return part;
                    }
                }
                for (aggregate, source) in aggregate_sources.iter().enumerate() {
                    let value = match source {
                        None => Some(0.0),
                        Some(source) => value_at(*source, row),
                    };
                    let Some(value) = value else {
                        part.valid = false;
                        continue;
                    };
                    part.counts[aggregate] += 1;
                    part.sums[aggregate] += value;
                    part.mins[aggregate] = part.mins[aggregate].min(value);
                    part.maxs[aggregate] = part.maxs[aggregate].max(value);
                }
                part
            })
            .reduce(empty, |mut target, source| {
                target.valid &= source.valid;
                for aggregate in 0..agg_cols.len() {
                    target.counts[aggregate] += source.counts[aggregate];
                    target.sums[aggregate] += source.sums[aggregate];
                    target.mins[aggregate] =
                        target.mins[aggregate].min(source.mins[aggregate]);
                    target.maxs[aggregate] =
                        target.maxs[aggregate].max(source.maxs[aggregate]);
                }
                target
            });
        if !merged.valid {
            return Ok(None);
        }
        Ok(Some(
            aggregate_sources
                .iter()
                .enumerate()
                .map(|(aggregate, source)| {
                    let count = merged.counts[aggregate];
                    let is_int = source.is_none()
                        || source.is_some_and(|source| {
                            matches!(columns[source].as_ref(), ColumnData::Int64(_))
                        });
                    (
                        count,
                        merged.sums[aggregate],
                        if count == 0 { 0.0 } else { merged.mins[aggregate] },
                        if count == 0 { 0.0 } else { merged.maxs[aggregate] },
                        is_int,
                    )
                })
                .collect(),
        ))
    }

    /// Top-K numeric sort after an AND-conjunction of numeric ranges. The
    /// result contains physical row positions in final sort-key order; callers
    /// can sparsely materialize secondary keys and projections.
    pub fn scan_filtered_numeric_top_k_cached(
        &self,
        sort_columns: &[(&str, bool)],
        predicates: &[(&str, f64, f64)],
        k: usize,
    ) -> io::Result<Option<Vec<usize>>> {
        use rayon::prelude::*;

        if k == 0
            || sort_columns.is_empty()
            || predicates.is_empty()
            || self.has_pending_deltas()
        {
            return Ok(None);
        }
        let footer = match self.get_or_load_footer()? {
            Some(footer) => footer,
            None => return Ok(None),
        };
        let mut unique_indices = Vec::new();
        let mut position_for = |column: &str| -> Option<usize> {
            let index = footer.schema.get_index(column)?;
            if self.column_has_nulls(column)
                || !matches!(
                    footer.schema.columns[index].1,
                    ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date
                        | ColumnType::Float64
                        | ColumnType::Float32
                )
            {
                return None;
            }
            Some(
                unique_indices
                    .iter()
                    .position(|&existing| existing == index)
                    .unwrap_or_else(|| {
                        unique_indices.push(index);
                        unique_indices.len() - 1
                    }),
            )
        };
        let sort_sources = sort_columns
            .iter()
            .map(|(column, descending)| {
                position_for(column).map(|source| (source, *descending))
            })
            .collect::<Option<Vec<_>>>();
        let Some(sort_sources) = sort_sources else {
            return Ok(None);
        };
        let predicate_sources = predicates
            .iter()
            .map(|(column, low, high)| {
                position_for(column).map(|source| (source, *low, *high))
            })
            .collect::<Option<Vec<_>>>();
        let Some(predicate_sources) = predicate_sources else {
            return Ok(None);
        };
        let Some(columns) = self.scan_numeric_columns_mmap_parallel(&unique_indices, &footer)? else {
            return Ok(None);
        };
        let value_at = |column: usize, row: usize| match columns[column].as_ref() {
            ColumnData::Int64(values) => values.get(row).map(|&value| value as f64),
            ColumnData::Float64(values) => values.get(row).copied(),
            _ => None,
        };
        let row_count = columns[sort_sources[0].0].len();
        let better = |left: &usize, right: &usize| {
            for &(source, descending) in &sort_sources {
                let left_value = value_at(source, *left).unwrap();
                let right_value = value_at(source, *right).unwrap();
                let order = left_value.total_cmp(&right_value);
                let order = if descending { order.reverse() } else { order };
                if !order.is_eq() {
                    return order;
                }
            }
            left.cmp(right)
        };
        let valid = std::sync::atomic::AtomicBool::new(true);
        let empty = Vec::<usize>::new;
        let parts = (0..row_count)
            .into_par_iter()
            .fold(empty, |mut rows, row| {
                for &(source, low, high) in &predicate_sources {
                    let Some(value) = value_at(source, row) else {
                        return rows;
                    };
                    if value < low || value > high {
                        return rows;
                    }
                }
                for &(source, _) in &sort_sources {
                    let Some(value) = value_at(source, row) else {
                        valid.store(false, std::sync::atomic::Ordering::Relaxed);
                        return rows;
                    };
                    if !value.is_finite() {
                        valid.store(false, std::sync::atomic::Ordering::Relaxed);
                        return rows;
                    }
                }
                let candidate = row;
                let position = rows.partition_point(|existing| {
                    better(existing, &candidate).is_lt()
                });
                if position < k {
                    rows.insert(position, candidate);
                    if rows.len() > k {
                        rows.pop();
                    }
                } else if rows.len() < k {
                    rows.push(candidate);
                }
                rows
            })
            .collect::<Vec<_>>();
        if !valid.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(None);
        }
        let mut merged = Vec::with_capacity(k);
        for part in parts {
            for candidate in part {
                let position = merged.partition_point(|existing| {
                    better(existing, &candidate).is_lt()
                });
                if position < k {
                    merged.insert(position, candidate);
                    if merged.len() > k {
                        merged.pop();
                    }
                }
            }
        }
        Ok(Some(merged))
    }

    /// MMAP PATH: execute GROUP BY + aggregate using pre-built dict cache.
    /// Streaming per-RG path: reads each column directly from mmap without full materialization.
    fn execute_group_agg_cached_mmap(
        &self,
        dict_strings: &[String],
        group_ids: &[u16],
        agg_cols: &[(&str, bool)],
    ) -> io::Result<Option<Vec<(String, Vec<(f64, i64)>)>>> {
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;

        let agg_col_indices: Vec<Option<usize>> = agg_cols.iter()
            .map(|(name, is_count)| if *is_count { None } else { schema.get_index(name) })
            .collect();
        let needed: Vec<usize> = agg_col_indices.iter().filter_map(|&x| x).collect();

        let num_groups = dict_strings.len();
        let num_aggs = agg_cols.len();
        let flat_len = num_groups * num_aggs;
        let mut flat_sums = vec![0.0f64; flat_len];
        let mut flat_counts = vec![0i64; flat_len];

        // Common analytical shape: COUNT(*) plus one AVG/SUM over a cached
        // categorical key. Build zero-copy row-group descriptors once and let
        // Rayon scan independent mmap ranges into tiny thread-local states.
        // The older generic loop below copies every row-group payload and then
        // updates one shared state sequentially.
        let count_slot = agg_cols.iter().position(|(_, count_star)| *count_star);
        let numeric_slot = agg_cols.iter().position(|(_, count_star)| !*count_star);
        if num_aggs == 2
            && count_slot.is_some()
            && numeric_slot.is_some()
            && agg_cols.iter().filter(|(_, count_star)| *count_star).count() == 1
        {
            let numeric_slot = numeric_slot.unwrap();
            let numeric_col = agg_cols[numeric_slot].0;
            if !self.column_has_nulls(numeric_col)
                && footer
                    .row_groups
                    .iter()
                    .all(|row_group| row_group.deletion_count == 0)
            {
                let numeric_idx = schema.get_index(numeric_col);
                if let Some(numeric_idx) = numeric_idx {
                    let col_type = schema.columns[numeric_idx].1;
                    let is_int = matches!(
                        col_type,
                        ColumnType::Int64
                            | ColumnType::Int8
                            | ColumnType::Int16
                            | ColumnType::Int32
                            | ColumnType::UInt8
                            | ColumnType::UInt16
                            | ColumnType::UInt32
                            | ColumnType::UInt64
                    );
                    let is_float =
                        matches!(col_type, ColumnType::Float64 | ColumnType::Float32);
                    if is_int || is_float {
                        let file_guard = self.file.read();
                        let file = file_guard.as_ref().ok_or_else(|| {
                            err_not_conn("File not open for cached parallel group-by")
                        })?;
                        let mut mmap_guard = self.mmap_cache.write();
                        let mmap_ref = mmap_guard.get_or_create(file)?;
                        struct NumericRg {
                            row_offset: usize,
                            rows: usize,
                            values: usize,
                        }
                        let mut descriptors = Vec::with_capacity(footer.row_groups.len());
                        let mut row_offset = 0usize;
                        let mut complete = group_ids.len()
                            == footer
                                .row_groups
                                .iter()
                                .map(|row_group| row_group.row_count as usize)
                                .sum::<usize>();
                        for (row_group_index, row_group) in
                            footer.row_groups.iter().enumerate()
                        {
                            let rows = row_group.row_count as usize;
                            let Some(offsets) = footer.col_offsets.get(row_group_index) else {
                                complete = false;
                                break;
                            };
                            let Some(&column_offset) = offsets.get(numeric_idx) else {
                                complete = false;
                                break;
                            };
                            let row_group_end =
                                (row_group.offset + row_group.data_size) as usize;
                            if rows == 0
                                || row_group_end > mmap_ref.len()
                                || row_group_end < row_group.offset as usize + 32
                            {
                                complete &= rows == 0;
                                row_offset += rows;
                                continue;
                            }
                            let bytes =
                                &mmap_ref[row_group.offset as usize..row_group_end];
                            if bytes[28] != RG_COMPRESS_NONE || bytes[29] < 1 {
                                complete = false;
                                break;
                            }
                            let body = &bytes[32..];
                            let data_start =
                                column_offset as usize + rows.div_ceil(8);
                            if data_start + 9 > body.len()
                                || body[data_start] != COL_ENCODING_PLAIN
                            {
                                complete = false;
                                break;
                            }
                            let payload = &body[data_start + 1..];
                            let encoded_rows = u64::from_le_bytes(
                                payload[0..8].try_into().unwrap(),
                            ) as usize;
                            let scan_rows = encoded_rows
                                .min(rows)
                                .min((payload.len() - 8) / 8);
                            if scan_rows != rows {
                                complete = false;
                                break;
                            }
                            descriptors.push(NumericRg {
                                row_offset,
                                rows,
                                values: unsafe { payload.as_ptr().add(8) } as usize,
                            });
                            row_offset += rows;
                        }
                        if complete {
                            use rayon::prelude::*;
                            let count_slot = count_slot.unwrap();
                            let parts: Vec<(Vec<f64>, Vec<i64>)> = descriptors
                                .par_iter()
                                .map(|descriptor| {
                                    let mut sums = vec![0.0f64; flat_len];
                                    let mut counts = vec![0i64; flat_len];
                                    let values = descriptor.values as *const u8;
                                    for row in 0..descriptor.rows {
                                        let group = unsafe {
                                            *group_ids
                                                .get_unchecked(descriptor.row_offset + row)
                                        } as usize;
                                        if group >= num_groups {
                                            continue;
                                        }
                                        let base = group * num_aggs;
                                        let value = if is_int {
                                            unsafe {
                                                std::ptr::read_unaligned(
                                                    values.add(row * 8) as *const i64,
                                                ) as f64
                                            }
                                        } else {
                                            unsafe {
                                                std::ptr::read_unaligned(
                                                    values.add(row * 8) as *const f64,
                                                )
                                            }
                                        };
                                        unsafe {
                                            *counts.get_unchecked_mut(base + count_slot) += 1;
                                            *counts.get_unchecked_mut(base + numeric_slot) += 1;
                                            *sums.get_unchecked_mut(base + numeric_slot) += value;
                                        }
                                    }
                                    (sums, counts)
                                })
                                .collect();
                            for (sums, counts) in parts {
                                for slot in 0..flat_len {
                                    flat_sums[slot] += sums[slot];
                                    flat_counts[slot] += counts[slot];
                                }
                            }
                            let results = (0..num_groups)
                                .filter(|&group| flat_counts[group * num_aggs] > 0)
                                .map(|group| {
                                    let stats = (0..num_aggs)
                                        .map(|aggregate| {
                                            (
                                                flat_sums[group * num_aggs + aggregate],
                                                flat_counts[group * num_aggs + aggregate],
                                            )
                                        })
                                        .collect();
                                    (dict_strings[group].clone(), stats)
                                })
                                .collect();
                            return Ok(Some(results));
                        }
                    }
                }
            }
        }

        // Check if all RGs support the streaming zero-copy path
        let max_col_idx = needed.iter().copied().max().unwrap_or(0);
        let all_rcix = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
            if rg_meta.row_count == 0 { return true; }
            footer.col_offsets.get(rg_i).map_or(false, |v| v.len() > max_col_idx)
        });

        if all_rcix && !needed.is_empty() {
            // STREAMING ZERO-COPY: process each RG directly without materializing full columns
            let file_guard = self.file.read();
            let file = file_guard.as_ref()
                .ok_or_else(|| err_not_conn("File not open for group-by agg"))?;
            let mut mmap_guard = self.mmap_cache.write();
            let mmap_ref = mmap_guard.get_or_create(file)?;
            let mut rg_row_offset = 0usize;
            let mut streaming_complete = true;

            for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
                let rg_rows = rg_meta.row_count as usize;
                if rg_rows == 0 { rg_row_offset += rg_rows; continue; }
                let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
                if rg_end > mmap_ref.len() { return Err(crate::storage::on_demand::err_data("RG past EOF")); }
                let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
                if rg_bytes.len() < 32 { rg_row_offset += rg_rows; continue; }
                let compress_flag = rg_bytes[28];
                let encoding_version = rg_bytes[29];
                let null_bitmap_len = (rg_rows + 7) / 8;
                let del_start = rg_id_section_len(
                    rg_rows,
                    rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
                );
                let del_vec_len = null_bitmap_len;
                let rcix = &footer.col_offsets[rg_i];

                if compress_flag != RG_COMPRESS_NONE || encoding_version < 1 {
                    // Restart through the full decoder below. Mixing partial
                    // streaming results with a whole-column fallback would
                    // double count the row groups already consumed.
                    streaming_complete = false;
                    break;
                }

                let body = &rg_bytes[32..];
                let has_deleted = rg_meta.deletion_count > 0;
                let del_bytes = if del_start + del_vec_len <= body.len() {
                    &body[del_start..del_start + del_vec_len]
                } else { &[] };

                // For each agg column, get raw slice from mmap via RCIX
                let gids_slice = &group_ids[rg_row_offset..(rg_row_offset + rg_rows).min(group_ids.len())];
                let rg_n = gids_slice.len();

                // Specialized single-agg Float64 fast path (most common: AVG/SUM of one column)
                if num_aggs == 1 {
                    let is_count = agg_cols[0].1;
                    if is_count {
                        if !has_deleted {
                            for i in 0..rg_n { let gid = unsafe { *gids_slice.get_unchecked(i) } as usize; unsafe { *flat_counts.get_unchecked_mut(gid) += 1; } }
                        } else {
                            for i in 0..rg_n { if !del_bytes.is_empty() && (del_bytes[i/8] >> (i%8)) & 1 != 0 { continue; } let gid = unsafe { *gids_slice.get_unchecked(i) } as usize; unsafe { *flat_counts.get_unchecked_mut(gid) += 1; } }
                        }
                    } else if let Some(&col_idx) = needed.first() {
                        let col_off = rcix[col_idx] as usize;
                        let data_start = col_off + null_bitmap_len;
                        if data_start + 1 < body.len() {
                            let encoding = body[data_start];
                            let payload = &body[data_start + 1..];
                            if encoding == 0u8 && payload.len() >= 8 {
                                let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                                let n = count.min(rg_rows).min(rg_n).min((payload.len() - 8) / 8);
                                let col_type = schema.columns[col_idx].1;
                                if matches!(col_type, ColumnType::Float64 | ColumnType::Float32) {
                                    let bp = unsafe { payload.as_ptr().add(8) };
                                    if !has_deleted {
                                        for i in 0..n { let gid = unsafe { *gids_slice.get_unchecked(i) } as usize; let v = unsafe { std::ptr::read_unaligned(bp.add(i * 8) as *const f64) }; unsafe { *flat_counts.get_unchecked_mut(gid) += 1; *flat_sums.get_unchecked_mut(gid) += v; } }
                                    } else {
                                        for i in 0..n { if !del_bytes.is_empty() && (del_bytes[i/8] >> (i%8)) & 1 != 0 { continue; } let gid = unsafe { *gids_slice.get_unchecked(i) } as usize; let v = unsafe { std::ptr::read_unaligned(bp.add(i * 8) as *const f64) }; unsafe { *flat_counts.get_unchecked_mut(gid) += 1; *flat_sums.get_unchecked_mut(gid) += v; } }
                                    }
                                    rg_row_offset += rg_rows; continue;
                                } else if matches!(col_type, ColumnType::Int64 | ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 | ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64) {
                                    let bp = unsafe { payload.as_ptr().add(8) };
                                    if !has_deleted {
                                        for i in 0..n { let gid = unsafe { *gids_slice.get_unchecked(i) } as usize; let v = unsafe { std::ptr::read_unaligned(bp.add(i * 8) as *const i64) }; unsafe { *flat_counts.get_unchecked_mut(gid) += 1; *flat_sums.get_unchecked_mut(gid) += v as f64; } }
                                    } else {
                                        for i in 0..n { if !del_bytes.is_empty() && (del_bytes[i/8] >> (i%8)) & 1 != 0 { continue; } let gid = unsafe { *gids_slice.get_unchecked(i) } as usize; let v = unsafe { std::ptr::read_unaligned(bp.add(i * 8) as *const i64) }; unsafe { *flat_counts.get_unchecked_mut(gid) += 1; *flat_sums.get_unchecked_mut(gid) += v as f64; } }
                                    }
                                    rg_row_offset += rg_rows; continue;
                                }
                            }
                        }
                        // Fallback: decode column
                        if col_off + null_bitmap_len < body.len() {
                            let (col_data, _) = read_column_encoded(&body[col_off + null_bitmap_len..], schema.columns[col_idx].1)?;
                            match &col_data { ColumnData::Float64(v) => { let n = v.len().min(rg_n); for i in 0..n { if has_deleted && !del_bytes.is_empty() && (del_bytes[i/8] >> (i%8)) & 1 != 0 { continue; } let gid = gids_slice[i] as usize; flat_counts[gid] += 1; flat_sums[gid] += v[i]; } } ColumnData::Int64(v) => { let n = v.len().min(rg_n); for i in 0..n { if has_deleted && !del_bytes.is_empty() && (del_bytes[i/8] >> (i%8)) & 1 != 0 { continue; } let gid = gids_slice[i] as usize; flat_counts[gid] += 1; flat_sums[gid] += v[i] as f64; } } _ => { for i in 0..rg_n { if has_deleted && !del_bytes.is_empty() && (del_bytes[i/8] >> (i%8)) & 1 != 0 { continue; } let gid = gids_slice[i] as usize; flat_counts[gid] += 1; } } }
                        }
                    }
                    rg_row_offset += rg_rows; continue;
                }
                // MULTI-AGG STREAMING: pre-load all agg column slices for this RG, single-pass hot loop
                // Enumerate each agg col: get PLAIN zero-copy slice or bail to outer fallback
                enum RgColSlice { Count, F64(Vec<f64>), I64(Vec<i64>) }
                let mut rg_slices: Vec<RgColSlice> = Vec::with_capacity(num_aggs);
                let mut ok = true;
                for (ai, &opt_col_idx) in agg_col_indices.iter().enumerate() {
                    if agg_cols[ai].1 || opt_col_idx.is_none() {
                        rg_slices.push(RgColSlice::Count); continue;
                    }
                    let col_idx = opt_col_idx.unwrap();
                    if col_idx >= rcix.len() { ok = false; break; }
                    let col_off = rcix[col_idx] as usize + null_bitmap_len;
                    if col_off + 1 >= body.len() { ok = false; break; }
                    let enc = body[col_off];
                    let payload = &body[col_off + 1..];
                    if enc != 0u8 || payload.len() < 8 { ok = false; break; }
                    let cnt = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                    let col_type = schema.columns[col_idx].1;
                    let n = cnt.min(rg_rows).min((payload.len() - 8) / 8);
                    if matches!(col_type, ColumnType::Float64 | ColumnType::Float32) {
                        let mut vals = vec![0f64; n];
                        unsafe { std::ptr::copy_nonoverlapping(payload[8..].as_ptr(), vals.as_mut_ptr() as *mut u8, n * 8); }
                        rg_slices.push(RgColSlice::F64(vals));
                    } else if matches!(col_type, ColumnType::Int64 | ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 | ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64) {
                        let mut vals = vec![0i64; n];
                        unsafe { std::ptr::copy_nonoverlapping(payload[8..].as_ptr(), vals.as_mut_ptr() as *mut u8, n * 8); }
                        rg_slices.push(RgColSlice::I64(vals));
                    } else { ok = false; break; }
                }
                if ok && !has_deleted {
                    // Specialized 2-agg [Count, F64] fast path (most common: COUNT(*)+AVG/SUM)
                    if num_aggs == 2 {
                        // 2 writes/row: COUNT(*) + AVG/SUM numerator; defer AVG denominator fixup O(groups)
                        if let (RgColSlice::Count, RgColSlice::F64(vals)) = (&rg_slices[0], &rg_slices[1]) {
                            let n = rg_n.min(vals.len());
                            for i in 0..n {
                                let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                let base = gid * 2;
                                unsafe { *flat_counts.get_unchecked_mut(base) += 1; *flat_sums.get_unchecked_mut(base + 1) += *vals.get_unchecked(i); }
                            }
                            for g in 0..num_groups { flat_counts[g * 2 + 1] = flat_counts[g * 2]; }
                            rg_row_offset += rg_rows; continue;
                        }
                        if let (RgColSlice::Count, RgColSlice::I64(vals)) = (&rg_slices[0], &rg_slices[1]) {
                            let n = rg_n.min(vals.len());
                            for i in 0..n {
                                let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                let base = gid * 2;
                                unsafe { *flat_counts.get_unchecked_mut(base) += 1; *flat_sums.get_unchecked_mut(base + 1) += *vals.get_unchecked(i) as f64; }
                            }
                            for g in 0..num_groups { flat_counts[g * 2 + 1] = flat_counts[g * 2]; }
                            rg_row_offset += rg_rows; continue;
                        }
                    }
                    // General multi-agg path
                    let n_limit: usize = rg_slices.iter().map(|sl| match sl { RgColSlice::F64(v) => v.len(), RgColSlice::I64(v) => v.len(), RgColSlice::Count => usize::MAX }).min().unwrap_or(rg_n).min(rg_n);
                    for i in 0..n_limit {
                        let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                        let base = gid * num_aggs;
                        for (ai, sl) in rg_slices.iter().enumerate() {
                            unsafe { *flat_counts.get_unchecked_mut(base + ai) += 1; }
                            match sl {
                                RgColSlice::Count => {}
                                RgColSlice::F64(v) => { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *v.get_unchecked(i); } }
                                RgColSlice::I64(v) => { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *v.get_unchecked(i) as f64; } }
                            }
                        }
                    }
                    rg_row_offset += rg_rows; continue;
                }
                // An encoded column or deletion bitmap not handled by the
                // specialized loop must use the complete decoder below.
                streaming_complete = false;
                break;
            }

            if streaming_complete {
                let results: Vec<(String, Vec<(f64, i64)>)> = (0..num_groups)
                    .filter(|&gid| {
                        flat_counts[gid * num_aggs] > 0
                            || agg_col_indices.iter().all(|x| x.is_none())
                    })
                    .map(|gid| {
                        let aggs: Vec<(f64, i64)> = (0..num_aggs)
                            .map(|ai| {
                                (
                                    flat_sums[gid * num_aggs + ai],
                                    flat_counts[gid * num_aggs + ai],
                                )
                            })
                            .collect();
                        (dict_strings[gid].clone(), aggs)
                    })
                    .collect();
                return Ok(Some(results));
            }
        }

        // FALLBACK: full materialization via scan_columns_mmap (compressed or no RCIX)
        let (scanned, del_bytes) = if needed.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            match self.scan_numeric_columns_mmap_parallel(&needed, &footer)? {
                Some(columns) => (columns, Vec::new()),
                None => {
                    let (columns, deleted) = self.scan_columns_mmap(&needed, &footer)?;
                    (
                        columns.into_iter().map(std::sync::Arc::new).collect(),
                        deleted,
                    )
                }
            }
        };
        let has_deleted = del_bytes.iter().any(|&b| b != 0);

        // Pre-resolve column data slices — resolve HashMap once, not per row
        let needed_to_pos: Vec<(usize, usize)> = needed.iter().enumerate()
            .map(|(pos, &idx)| (idx, pos))
            .collect();

        struct AggSlice<'a> { i64_vals: Option<&'a [i64]>, f64_vals: Option<&'a [f64]>, is_count: bool }
        let agg_slices: Vec<AggSlice> = agg_col_indices.iter().map(|opt_idx| {
            if opt_idx.is_none() { return AggSlice { i64_vals: None, f64_vals: None, is_count: true }; }
            let col_idx = opt_idx.unwrap();
            let pos = needed_to_pos.iter().find(|&&(idx, _)| idx == col_idx).map(|&(_, p)| p);
            if let Some(pos) = pos {
                if pos < scanned.len() {
                    match scanned[pos].as_ref() {
                        ColumnData::Int64(v)   => return AggSlice { i64_vals: Some(v.as_slice()), f64_vals: None, is_count: false },
                        ColumnData::Float64(v) => return AggSlice { i64_vals: None, f64_vals: Some(v.as_slice()), is_count: false },
                        _ => {}
                    }
                }
            }
            AggSlice { i64_vals: None, f64_vals: None, is_count: true }
        }).collect();

        let num_groups = dict_strings.len();
        let num_aggs = agg_cols.len();
        let scan_rows = group_ids.len();
        let flat_len = num_groups * num_aggs;
        let mut flat_sums = vec![0.0f64; flat_len];
        let mut flat_counts = vec![0i64; flat_len];

        if !has_deleted && scan_rows >= 131_072 && flat_len <= 1_000_000 {
            use rayon::prelude::*;
            const CHUNK_ROWS: usize = 65_536;
            let parts: Vec<(Vec<f64>, Vec<i64>)> = group_ids[..scan_rows]
                .par_chunks(CHUNK_ROWS)
                .enumerate()
                .map(|(chunk_index, groups)| {
                    let start = chunk_index * CHUNK_ROWS;
                    let mut sums = vec![0.0f64; flat_len];
                    let mut counts = vec![0i64; flat_len];
                    for (local_row, &group_id) in groups.iter().enumerate() {
                        let row = start + local_row;
                        let base = group_id as usize * num_aggs;
                        if base + num_aggs > flat_len {
                            continue;
                        }
                        for (aggregate, source) in agg_slices.iter().enumerate() {
                            unsafe { *counts.get_unchecked_mut(base + aggregate) += 1; }
                            if let Some(values) = source.f64_vals {
                                if row < values.len() {
                                    unsafe {
                                        *sums.get_unchecked_mut(base + aggregate) +=
                                            *values.get_unchecked(row);
                                    }
                                }
                            } else if let Some(values) = source.i64_vals {
                                if row < values.len() {
                                    unsafe {
                                        *sums.get_unchecked_mut(base + aggregate) +=
                                            *values.get_unchecked(row) as f64;
                                    }
                                }
                            }
                        }
                    }
                    (sums, counts)
                })
                .collect();
            for (sums, counts) in parts {
                for slot in 0..flat_len {
                    flat_sums[slot] += sums[slot];
                    flat_counts[slot] += counts[slot];
                }
            }
        // Specialized fast path for single agg column (most common) — no inner loop
        } else if num_aggs == 1 && !has_deleted {
            match &agg_slices[0] {
                AggSlice { f64_vals: Some(vals), .. } => {
                    let limit = scan_rows.min(vals.len());
                    for i in 0..limit {
                        let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                        unsafe { *flat_counts.get_unchecked_mut(gid) += 1; }
                        unsafe { *flat_sums.get_unchecked_mut(gid) += *vals.get_unchecked(i); }
                    }
                }
                AggSlice { i64_vals: Some(vals), .. } => {
                    let limit = scan_rows.min(vals.len());
                    for i in 0..limit {
                        let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                        unsafe { *flat_counts.get_unchecked_mut(gid) += 1; }
                        unsafe { *flat_sums.get_unchecked_mut(gid) += *vals.get_unchecked(i) as f64; }
                    }
                }
                _ => {
                    for i in 0..scan_rows {
                        let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                        unsafe { *flat_counts.get_unchecked_mut(gid) += 1; }
                    }
                }
            }
        } else {
            for i in 0..scan_rows {
                if has_deleted {
                    let b = i / 8; let bit = i % 8;
                    if b < del_bytes.len() && (del_bytes[b] >> bit) & 1 != 0 { continue; }
                }
                let base = group_ids[i] as usize * num_aggs;
                if base + num_aggs > flat_len { continue; }
                for (ai, agg) in agg_slices.iter().enumerate() {
                    unsafe { *flat_counts.get_unchecked_mut(base + ai) += 1; }
                    if !agg.is_count {
                        if let Some(vals) = agg.f64_vals { if i < vals.len() { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *vals.get_unchecked(i); } } }
                        else if let Some(vals) = agg.i64_vals { if i < vals.len() { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *vals.get_unchecked(i) as f64; } } }
                    }
                }
            }
        }

        let results: Vec<(String, Vec<(f64, i64)>)> = (0..num_groups)
            .filter(|&gid| flat_counts[gid * num_aggs] > 0)
            .map(|gid| {
                let aggs: Vec<(f64, i64)> = (0..num_aggs)
                    .map(|ai| (flat_sums[gid * num_aggs + ai], flat_counts[gid * num_aggs + ai]))
                    .collect();
                (dict_strings[gid].clone(), aggs)
            })
            .collect();

        Ok(Some(results))
    }

    /// Execute 2-column GROUP BY + aggregate using pre-built dict caches for both group columns.
    /// Uses composite index: idx1 * dict2_size + idx2 for O(1) group lookup per row.
    /// group_ids are 0-based (matching build_string_dict_cache output).
    /// Returns Vec<((group1_val, group2_val), [(sum, count) per agg])>
    pub fn execute_group_agg_2col_cached(
        &self,
        dict1_strings: &[String], group_ids1: &[u16],
        dict2_strings: &[String], group_ids2: &[u16],
        agg_cols: &[(&str, bool)], // (col_name, is_count_star)
        ids_validated: bool,
    ) -> io::Result<Option<Vec<((String, String), Vec<(f64, i64)>)>>> {
        // 0-based: dict indices 0..len-1 (no null slot)
        let dict1_size = dict1_strings.len();
        let dict2_size = dict2_strings.len();
        let total_size = dict1_size * dict2_size;
        if dict1_size == 0 || dict2_size == 0 { return Ok(Some(Vec::new())); }
        if total_size > 200_000 { return Ok(None); }

        let num_aggs = agg_cols.len();
        if num_aggs == 0 {
            return Ok(Some(Vec::new()));
        }
        let group_rows = group_ids1.len().min(group_ids2.len());
        if !ids_validated
            && group_ids1[..group_rows]
            .iter()
            .zip(&group_ids2[..group_rows])
            .any(|(&idx1, &idx2)| idx1 as usize >= dict1_size || idx2 as usize >= dict2_size)
        {
            return Ok(None);
        }
        let flat_len = total_size * num_aggs;
        let mut flat_sums = vec![0.0f64; flat_len];
        let mut flat_counts = vec![0i64; flat_len];

        if !self.has_v4_in_memory_data() {
            // MMAP PATH: read agg columns from disk, group_ids already built
            let footer = match self.get_or_load_footer()? {
                Some(f) => f,
                None => return Ok(None),
            };
            let schema = &footer.schema;
            let agg_col_indices: Vec<Option<usize>> = agg_cols.iter()
                .map(|(name, is_count)| if *is_count { None } else { schema.get_index(name) })
                .collect();
            let needed: Vec<usize> = agg_col_indices.iter().filter_map(|&x| x).collect();
            let scan_rows = group_rows;

            if needed.is_empty() {
                // COUNT(*) only — no column read needed
                for i in 0..scan_rows {
                    let idx1 = unsafe { *group_ids1.get_unchecked(i) } as usize;
                    let idx2 = unsafe { *group_ids2.get_unchecked(i) } as usize;
                    let composite = idx1 * dict2_size + idx2;
                    unsafe { *flat_counts.get_unchecked_mut(composite * num_aggs) += 1; }
                }
            } else {
                // STREAMING ZERO-COPY: per-RG direct mmap scan+aggregate (avoids 8MB+ column copy)
                let max_ci = needed.iter().copied().max().unwrap_or(0);
                let all_rcix = footer.row_groups.iter().enumerate().all(|(rg_i, rm)| {
                    rm.row_count == 0 || footer.col_offsets.get(rg_i).map_or(false, |v| v.len() > max_ci)
                });
                let mut streaming_done = false;
                if all_rcix {
                    let file_guard = self.file.read();
                    if let Some(file) = file_guard.as_ref() {
                        let mut mmap_guard = self.mmap_cache.write();
                        if let Ok(mmap_ref) = mmap_guard.get_or_create(file) {
                            let mut row_off = 0usize;
                            let mut ok = true;
                            for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
                                let rg_rows = rg_meta.row_count as usize;
                                if rg_rows == 0 { continue; }
                                let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
                                if rg_end > mmap_ref.len() { ok = false; break; }
                                let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
                                if rg_bytes.len() < 32 || rg_bytes[28] != RG_COMPRESS_NONE || rg_bytes[29] < 1 {
                                    ok = false; break;
                                }
                                let body = &rg_bytes[32..];
                                let nbm = (rg_rows + 7) / 8;
                                let has_del = rg_meta.deletion_count > 0;
                                let del_start = rg_id_section_len(
                                    rg_rows,
                                    rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
                                );
                                let del_bm: &[u8] = if has_del && del_start + nbm <= body.len() { &body[del_start..del_start + nbm] } else { &[] };
                                let rcix = &footer.col_offsets[rg_i];
                                let g1e = (row_off + rg_rows).min(group_ids1.len());
                                let g2e = (row_off + rg_rows).min(group_ids2.len());
                                if row_off >= g1e || row_off >= g2e { row_off += rg_rows; continue; }
                                let gids1 = &group_ids1[row_off..g1e];
                                let gids2 = &group_ids2[row_off..g2e];
                                let rg_n = gids1.len().min(gids2.len());

                                // Specialized 2-agg [COUNT(*), F64/I64] — most common: COUNT(*)+AVG/SUM
                                if num_aggs == 2 && agg_cols[0].1 {
                                    if let Some(&ci) = needed.first() {
                                        if ci < rcix.len() {
                                            let co = rcix[ci] as usize;
                                            let doff = co + nbm;
                                            if doff + 9 <= body.len() && body[doff] == 0u8 {
                                                let pl = &body[doff + 1..];
                                                if pl.len() >= 8 {
                                                    let cnt = u64::from_le_bytes(pl[0..8].try_into().unwrap()) as usize;
                                                    let n = cnt.min(rg_rows).min(rg_n).min((pl.len() - 8) / 8);
                                                    let base_ptr = unsafe { pl.as_ptr().add(8) };
                                                    let ct = schema.columns[ci].1;
                                                    if matches!(ct, ColumnType::Float64 | ColumnType::Float32) {
                                                        if !has_del {
                                                            unsafe {
                                                                aggregate_validated_2col_count_f64(
                                                                    gids1,
                                                                    gids2,
                                                                    base_ptr,
                                                                    n,
                                                                    dict2_size,
                                                                    &mut flat_counts,
                                                                    &mut flat_sums,
                                                                );
                                                            }
                                                        } else {
                                                            for i in 0..n {
                                                                if !del_bm.is_empty() && (del_bm[i/8] >> (i%8)) & 1 != 0 { continue; }
                                                                let i1 = unsafe { *gids1.get_unchecked(i) } as usize;
                                                                let i2 = unsafe { *gids2.get_unchecked(i) } as usize;
                                                                let c = (i1 * dict2_size + i2) * 2;
                                                                let v = unsafe { std::ptr::read_unaligned(base_ptr.add(i * 8) as *const f64) };
                                                                unsafe { *flat_counts.get_unchecked_mut(c) += 1; *flat_sums.get_unchecked_mut(c + 1) += v; }
                                                            }
                                                        }
                                                        for comp in 0..total_size { flat_counts[comp * 2 + 1] = flat_counts[comp * 2]; }
                                                        row_off += rg_rows; continue;
                                                    } else if matches!(ct, ColumnType::Int64 | ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 | ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64 | ColumnType::Timestamp | ColumnType::Date) {
                                                        if !has_del {
                                                            unsafe {
                                                                aggregate_validated_2col_count_i64(
                                                                    gids1,
                                                                    gids2,
                                                                    base_ptr,
                                                                    n,
                                                                    dict2_size,
                                                                    &mut flat_counts,
                                                                    &mut flat_sums,
                                                                );
                                                            }
                                                        } else {
                                                            for i in 0..n {
                                                                if !del_bm.is_empty() && (del_bm[i/8] >> (i%8)) & 1 != 0 { continue; }
                                                                let i1 = unsafe { *gids1.get_unchecked(i) } as usize;
                                                                let i2 = unsafe { *gids2.get_unchecked(i) } as usize;
                                                                let c = (i1 * dict2_size + i2) * 2;
                                                                let v = unsafe { std::ptr::read_unaligned(base_ptr.add(i * 8) as *const i64) };
                                                                unsafe { *flat_counts.get_unchecked_mut(c) += 1; *flat_sums.get_unchecked_mut(c + 1) += v as f64; }
                                                            }
                                                        }
                                                        for comp in 0..total_size { flat_counts[comp * 2 + 1] = flat_counts[comp * 2]; }
                                                        row_off += rg_rows; continue;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                // General multi-agg: read each agg col directly from mmap
                                let mut rg_ok = true;
                                for (ai, &opt_ci) in agg_col_indices.iter().enumerate() {
                                    if agg_cols[ai].1 || opt_ci.is_none() { continue; }
                                    let ci = opt_ci.unwrap();
                                    if ci >= rcix.len() { rg_ok = false; break; }
                                    let co = rcix[ci] as usize + nbm;
                                    if co + 9 > body.len() || body[co] != 0u8 { rg_ok = false; break; }
                                }
                                if !rg_ok { ok = false; break; }
                                // Multi-agg aggregation loop
                                for i in 0..rg_n {
                                    if has_del && !del_bm.is_empty() && (del_bm[i/8] >> (i%8)) & 1 != 0 { continue; }
                                    let i1 = unsafe { *gids1.get_unchecked(i) } as usize;
                                    let i2 = unsafe { *gids2.get_unchecked(i) } as usize;
                                    let base = (i1 * dict2_size + i2) * num_aggs;
                                    for (ai, &opt_ci) in agg_col_indices.iter().enumerate() {
                                        unsafe { *flat_counts.get_unchecked_mut(base + ai) += 1; }
                                        if agg_cols[ai].1 || opt_ci.is_none() { continue; }
                                        let ci = opt_ci.unwrap();
                                        let co = rcix[ci] as usize + nbm;
                                        let pl = &body[co + 1..];
                                        let ct = schema.columns[ci].1;
                                        let bp = unsafe { pl.as_ptr().add(8) };
                                        if matches!(ct, ColumnType::Float64 | ColumnType::Float32) {
                                            let v = unsafe { std::ptr::read_unaligned(bp.add(i * 8) as *const f64) };
                                            unsafe { *flat_sums.get_unchecked_mut(base + ai) += v; }
                                        } else {
                                            let v = unsafe { std::ptr::read_unaligned(bp.add(i * 8) as *const i64) };
                                            unsafe { *flat_sums.get_unchecked_mut(base + ai) += v as f64; }
                                        }
                                    }
                                }
                                row_off += rg_rows;
                            }
                            streaming_done = ok;
                        }
                    }
                }
                if !streaming_done {
                    // FALLBACK: full materialization via scan_columns_mmap (compressed or no RCIX)
                    let (scanned, del_bytes) = self.scan_columns_mmap(&needed, &footer)?;
                    let has_deleted = del_bytes.iter().any(|&b| b != 0);
                    struct AggSl<'a> { f64v: Option<&'a [f64]>, i64v: Option<&'a [i64]>, is_count: bool }
                    let slices: Vec<AggSl> = agg_col_indices.iter().map(|opt_idx| {
                        if opt_idx.is_none() { return AggSl { f64v: None, i64v: None, is_count: true }; }
                        let col_idx = opt_idx.unwrap();
                        let pos = needed.iter().position(|&x| x == col_idx);
                        if let Some(pos) = pos {
                            if pos < scanned.len() {
                                match &scanned[pos] {
                                    ColumnData::Float64(v) => return AggSl { f64v: Some(v.as_slice()), i64v: None, is_count: false },
                                    ColumnData::Int64(v)   => return AggSl { f64v: None, i64v: Some(v.as_slice()), is_count: false },
                                    _ => {}
                                }
                            }
                        }
                        AggSl { f64v: None, i64v: None, is_count: true }
                    }).collect();
                    for i in 0..scan_rows {
                        if has_deleted { let b = i/8; let bit = i%8; if b < del_bytes.len() && (del_bytes[b] >> bit) & 1 != 0 { continue; } }
                        let idx1 = unsafe { *group_ids1.get_unchecked(i) } as usize;
                        let idx2 = unsafe { *group_ids2.get_unchecked(i) } as usize;
                        let composite = idx1 * dict2_size + idx2;
                        let base = composite * num_aggs;
                        for (ai, sl) in slices.iter().enumerate() {
                            unsafe { *flat_counts.get_unchecked_mut(base + ai) += 1; }
                            if !sl.is_count {
                                if let Some(v) = sl.f64v { if i < v.len() { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *v.get_unchecked(i); } } }
                                else if let Some(v) = sl.i64v { if i < v.len() { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *v.get_unchecked(i) as f64; } } }
                            }
                        }
                    }
                }
            }
        } else {
            // IN-MEMORY PATH
            let schema = self.schema.read();
            let columns = self.columns.read();
            let deleted = self.deleted.read();
            let total_rows = self.ids.read().len();
            let scan_rows = total_rows.min(group_rows);
            let has_deleted = deleted.iter().any(|&b| b != 0);

            struct AggSl<'a> { f64v: Option<&'a [f64]>, i64v: Option<&'a [i64]>, is_count: bool }
            let slices: Vec<AggSl> = agg_cols.iter().map(|(name, is_count)| {
                if *is_count { return AggSl { f64v: None, i64v: None, is_count: true }; }
                if let Some(idx) = schema.get_index(name) {
                    if idx < columns.len() {
                        match &columns[idx] {
                            ColumnData::Float64(v) => return AggSl { f64v: Some(v.as_slice()), i64v: None, is_count: false },
                            ColumnData::Int64(v)   => return AggSl { f64v: None, i64v: Some(v.as_slice()), is_count: false },
                            _ => {}
                        }
                    }
                }
                AggSl { f64v: None, i64v: None, is_count: true }
            }).collect();

            if !has_deleted && num_aggs == 2 {
                // Specialized 2-agg path (COUNT+AVG/SUM) — most common case
                let sl0 = &slices[0]; let sl1 = &slices[1];
                if sl0.is_count {
                    if let Some(fv) = sl1.f64v {
                        let n = scan_rows.min(fv.len());
                        for i in 0..n {
                            let idx1 = unsafe { *group_ids1.get_unchecked(i) } as usize;
                            let idx2 = unsafe { *group_ids2.get_unchecked(i) } as usize;
                            let c = (idx1 * dict2_size + idx2) * 2;
                            unsafe { *flat_counts.get_unchecked_mut(c) += 1; *flat_sums.get_unchecked_mut(c + 1) += *fv.get_unchecked(i); }
                        }
                        for comp in 0..total_size { flat_counts[comp * 2 + 1] = flat_counts[comp * 2]; }
                    } else if let Some(iv) = sl1.i64v {
                        let n = scan_rows.min(iv.len());
                        for i in 0..n {
                            let idx1 = unsafe { *group_ids1.get_unchecked(i) } as usize;
                            let idx2 = unsafe { *group_ids2.get_unchecked(i) } as usize;
                            let c = (idx1 * dict2_size + idx2) * 2;
                            unsafe { *flat_counts.get_unchecked_mut(c) += 1; *flat_sums.get_unchecked_mut(c + 1) += *iv.get_unchecked(i) as f64; }
                        }
                        for comp in 0..total_size { flat_counts[comp * 2 + 1] = flat_counts[comp * 2]; }
                    } else {
                        for i in 0..scan_rows {
                            let idx1 = unsafe { *group_ids1.get_unchecked(i) } as usize;
                            let idx2 = unsafe { *group_ids2.get_unchecked(i) } as usize;
                            let c = (idx1 * dict2_size + idx2) * 2;
                            unsafe { *flat_counts.get_unchecked_mut(c) += 1; *flat_counts.get_unchecked_mut(c + 1) += 1; }
                        }
                    }
                } else {
                    // General 2-agg
                    for i in 0..scan_rows {
                        let idx1 = unsafe { *group_ids1.get_unchecked(i) } as usize;
                        let idx2 = unsafe { *group_ids2.get_unchecked(i) } as usize;
                        let base = (idx1 * dict2_size + idx2) * 2;
                        for (ai, sl) in slices.iter().enumerate() {
                            unsafe { *flat_counts.get_unchecked_mut(base + ai) += 1; }
                            if !sl.is_count {
                                if let Some(v) = sl.f64v { if i < v.len() { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *v.get_unchecked(i); } } }
                                else if let Some(v) = sl.i64v { if i < v.len() { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *v.get_unchecked(i) as f64; } } }
                            }
                        }
                    }
                }
            } else {
                for i in 0..scan_rows {
                    if has_deleted { let b = i/8; let bit = i%8; if b < deleted.len() && (deleted[b] >> bit) & 1 != 0 { continue; } }
                    let idx1 = unsafe { *group_ids1.get_unchecked(i) } as usize;
                    let idx2 = unsafe { *group_ids2.get_unchecked(i) } as usize;
                    let base = (idx1 * dict2_size + idx2) * num_aggs;
                    for (ai, sl) in slices.iter().enumerate() {
                        unsafe { *flat_counts.get_unchecked_mut(base + ai) += 1; }
                        if !sl.is_count {
                            if let Some(v) = sl.f64v { if i < v.len() { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *v.get_unchecked(i); } } }
                            else if let Some(v) = sl.i64v { if i < v.len() { unsafe { *flat_sums.get_unchecked_mut(base + ai) += *v.get_unchecked(i) as f64; } } }
                        }
                    }
                }
            }
        }

        // Collect non-empty groups (0-based indices)
        let mut results: Vec<((String, String), Vec<(f64, i64)>)> = Vec::new();
        for idx1 in 0..dict1_size {
            for idx2 in 0..dict2_size {
                let composite = idx1 * dict2_size + idx2;
                let base = composite * num_aggs;
                if flat_counts[base] > 0 {
                    let aggs: Vec<(f64, i64)> = (0..num_aggs)
                        .map(|ai| (flat_sums[base + ai], flat_counts[base + ai]))
                        .collect();
                    results.push(((dict1_strings[idx1].clone(), dict2_strings[idx2].clone()), aggs));
                }
            }
        }
        Ok(Some(results))
    }

    /// Execute BETWEEN + GROUP BY using pre-built dict cache.
    /// Supports both in-memory and mmap-only paths.
    pub fn execute_between_group_agg_cached(
        &self,
        filter_col: &str,
        lo: f64,
        hi: f64,
        dict_strings: &[String],
        group_ids: &[u16],
        agg_col: Option<&str>,
    ) -> io::Result<Option<Vec<(String, f64, i64)>>> {
        if !self.has_v4_in_memory_data() {
            // MMAP PATH: scan filter+agg columns from disk
            return self.execute_between_group_agg_cached_mmap(filter_col, lo, hi, dict_strings, group_ids, agg_col);
        }

        let schema = self.schema.read();
        let columns = self.columns.read();
        let deleted = self.deleted.read();
        let total_rows = self.ids.read().len();

        let filter_idx = match schema.get_index(filter_col) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        if filter_idx >= columns.len() { return Ok(None); }

        let has_deleted = deleted.iter().any(|&b| b != 0);
        // `lo`/`hi` carry strict-inequality epsilon bounds (next/prev f64) for
        // `>` / `<`; ceil/floor map those to the exact inclusive integer
        // range (age > 40 -> [41, MAX]), which a plain truncation would get
        // wrong (40.000000000000007 as i64 == 40).
        let lo_i64 = lo.ceil() as i64;
        let hi_i64 = hi.floor() as i64;
        let scan_rows = total_rows.min(group_ids.len());
        let num_groups = dict_strings.len();

        let mut group_sums = vec![0.0f64; num_groups];
        let mut group_counts = vec![0i64; num_groups];

        let agg_idx = agg_col.and_then(|ac| schema.get_index(ac));
        let agg_f64 = agg_idx.and_then(|idx| {
            if idx < columns.len() { match &columns[idx] { ColumnData::Float64(v) => Some(v.as_slice()), _ => None } } else { None }
        });
        let agg_i64 = agg_idx.and_then(|idx| {
            if idx < columns.len() { match &columns[idx] { ColumnData::Int64(v) => Some(v.as_slice()), _ => None } } else { None }
        });

        // Branchless accumulation: eliminates branch misprediction at ~50% BETWEEN hit rate
        // mask = (in_range) as i64 → 0 or 1, multiply with agg value so non-matching adds 0
        macro_rules! between_agg_branchy {
            ($filter_vals:expr, $lo_cmp:expr, $hi_cmp:expr, $limit:expr) => {{
                for i in 0..$limit {
                    let b = i / 8; let bit = i % 8;
                    if b < deleted.len() && (deleted[b] >> bit) & 1 != 0 { continue; }
                    let fv = unsafe { *$filter_vals.get_unchecked(i) };
                    if fv >= $lo_cmp && fv <= $hi_cmp {
                        let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                        unsafe { *group_counts.get_unchecked_mut(gid) += 1; }
                        if let Some(av) = agg_f64 { unsafe { *group_sums.get_unchecked_mut(gid) += *av.get_unchecked(i); } }
                        else if let Some(av) = agg_i64 { unsafe { *group_sums.get_unchecked_mut(gid) += *av.get_unchecked(i) as f64; } }
                    }
                }
            }};
        }

        if let Some(filter_vals) = match &columns[filter_idx] { ColumnData::Int64(v) => Some(v.as_slice()), _ => None } {
            let limit = scan_rows.min(filter_vals.len()).min(group_ids.len());
            let limit = limit.min(agg_f64.map_or(usize::MAX, |a| a.len())).min(agg_i64.map_or(usize::MAX, |a| a.len()));
            if has_deleted {
                between_agg_branchy!(filter_vals, lo_i64, hi_i64, limit);
            } else if let Some(av) = agg_f64 {
                // HOT PATH: branchless i64 filter + f64 agg (no deleted)
                for i in 0..limit {
                    let fv = unsafe { *filter_vals.get_unchecked(i) };
                    let mask = (fv >= lo_i64 && fv <= hi_i64) as i64;
                    let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                    unsafe {
                        *group_counts.get_unchecked_mut(gid) += mask;
                        *group_sums.get_unchecked_mut(gid) += mask as f64 * *av.get_unchecked(i);
                    }
                }
            } else if let Some(av) = agg_i64 {
                for i in 0..limit {
                    let fv = unsafe { *filter_vals.get_unchecked(i) };
                    let mask = (fv >= lo_i64 && fv <= hi_i64) as i64;
                    let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                    unsafe {
                        *group_counts.get_unchecked_mut(gid) += mask;
                        *group_sums.get_unchecked_mut(gid) += mask as f64 * (*av.get_unchecked(i) as f64);
                    }
                }
            } else {
                // COUNT-only: branchless
                for i in 0..limit {
                    let fv = unsafe { *filter_vals.get_unchecked(i) };
                    let mask = (fv >= lo_i64 && fv <= hi_i64) as i64;
                    let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                    unsafe { *group_counts.get_unchecked_mut(gid) += mask; }
                }
            }
        } else if let Some(filter_vals) = match &columns[filter_idx] { ColumnData::Float64(v) => Some(v.as_slice()), _ => None } {
            let limit = scan_rows.min(filter_vals.len()).min(group_ids.len());
            let limit = limit.min(agg_f64.map_or(usize::MAX, |a| a.len())).min(agg_i64.map_or(usize::MAX, |a| a.len()));
            if has_deleted {
                between_agg_branchy!(filter_vals, lo, hi, limit);
            } else if let Some(av) = agg_f64 {
                for i in 0..limit {
                    let fv = unsafe { *filter_vals.get_unchecked(i) };
                    let mask = (fv >= lo && fv <= hi) as i64;
                    let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                    unsafe {
                        *group_counts.get_unchecked_mut(gid) += mask;
                        *group_sums.get_unchecked_mut(gid) += mask as f64 * *av.get_unchecked(i);
                    }
                }
            } else if let Some(av) = agg_i64 {
                for i in 0..limit {
                    let fv = unsafe { *filter_vals.get_unchecked(i) };
                    let mask = (fv >= lo && fv <= hi) as i64;
                    let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                    unsafe {
                        *group_counts.get_unchecked_mut(gid) += mask;
                        *group_sums.get_unchecked_mut(gid) += mask as f64 * (*av.get_unchecked(i) as f64);
                    }
                }
            } else {
                for i in 0..limit {
                    let fv = unsafe { *filter_vals.get_unchecked(i) };
                    let mask = (fv >= lo && fv <= hi) as i64;
                    let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                    unsafe { *group_counts.get_unchecked_mut(gid) += mask; }
                }
            }
        }

        let results: Vec<(String, f64, i64)> = (0..num_groups)
            .filter(|&gid| group_counts[gid] > 0)
            .map(|gid| (dict_strings[gid].clone(), group_sums[gid], group_counts[gid]))
            .collect();

        Ok(Some(results))
    }

    /// MMAP PATH: execute BETWEEN + GROUP BY using pre-built dict cache, scanning filter+agg from disk.
    fn execute_between_group_agg_cached_mmap(
        &self,
        filter_col: &str,
        lo: f64,
        hi: f64,
        dict_strings: &[String],
        group_ids: &[u16],
        agg_col: Option<&str>,
    ) -> io::Result<Option<Vec<(String, f64, i64)>>> {
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;

        let filter_idx = match schema.get_index(filter_col) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let agg_idx = agg_col.and_then(|ac| schema.get_index(ac));

        // Check RCIX availability
        let max_col_idx = [Some(filter_idx), agg_idx].iter().filter_map(|&x| x).max().unwrap_or(0);
        let all_rcix = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
            if rg_meta.row_count == 0 { return true; }
            footer.col_offsets.get(rg_i).map_or(false, |v| v.len() > max_col_idx)
        });

        let num_groups = dict_strings.len();
        let mut group_sums = vec![0.0f64; num_groups];
        let mut group_counts = vec![0i64; num_groups];
        let lo_i64 = lo.ceil() as i64;
        let hi_i64 = hi.floor() as i64;

        if all_rcix {
            // STREAMING: process per-RG without materializing full columns
            let file_guard = self.file.read();
            let file = file_guard.as_ref().ok_or_else(|| err_not_conn("File not open"))?;
            let mut mmap_guard = self.mmap_cache.write();
            let mmap_ref = mmap_guard.get_or_create(file)?;
            let mut rg_row_offset = 0usize;

            for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
                let rg_rows = rg_meta.row_count as usize;
                if rg_rows == 0 { rg_row_offset += rg_rows; continue; }
                let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
                if rg_end > mmap_ref.len() { return Err(crate::storage::on_demand::err_data("RG past EOF")); }
                let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
                if rg_bytes.len() < 32 { rg_row_offset += rg_rows; continue; }
                let compress_flag = rg_bytes[28];
                let encoding_version = rg_bytes[29];
                let null_bitmap_len = (rg_rows + 7) / 8;
                let rcix = &footer.col_offsets[rg_i];
                let has_deleted = rg_meta.deletion_count > 0;
                let del_start = rg_id_section_len(
                    rg_rows,
                    rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
                );
                let del_vec_len = null_bitmap_len;

                // Decompress if needed
                let decompressed_buf = decompress_rg_body(compress_flag, &rg_bytes[32..])?;
                let body: &[u8] = decompressed_buf.as_deref().unwrap_or(&rg_bytes[32..]);
                let del_bytes: &[u8] = if del_start + del_vec_len <= body.len() {
                    &body[del_start..del_start + del_vec_len]
                } else { &[] };

                let gids_slice = &group_ids[rg_row_offset..(rg_row_offset + rg_rows).min(group_ids.len())];
                let rg_n = gids_slice.len();

                // Get filter column data — zero-copy for BITPACK, full decode for others
                let f_col_off = rcix[filter_idx] as usize;
                let f_data_start = f_col_off + null_bitmap_len;
                if f_data_start >= body.len() { rg_row_offset += rg_rows; continue; }
                let f_bytes = &body[f_data_start..];
                let f_encoding = if encoding_version >= 1 && !f_bytes.is_empty() { f_bytes[0] } else { 0 };
                // Check for BITPACK filter: inline decode to avoid 500KB Vec<i64> per RG
                let filter_is_bitpack = f_encoding == 2u8 && f_bytes.len() >= 18; // 1 enc + 17 header
                let (bp_count, bp_bit_width, bp_min_val, bp_packed): (usize, usize, i64, &[u8]) = if filter_is_bitpack {
                    let d = &f_bytes[1..];
                    let cnt = u64::from_le_bytes(d[0..8].try_into().unwrap()) as usize;
                    let bw = d[8] as usize;
                    let mv = i64::from_le_bytes(d[9..17].try_into().unwrap());
                    let pb = (cnt * bw + 7) / 8;
                    let packed = if 17 + pb <= d.len() { &d[17..17+pb] } else { &[] as &[u8] };
                    (cnt, bw, mv, packed)
                } else { (0, 0, 0, &[]) };
                let filter_float_dict = if f_encoding == COL_ENCODING_FLOAT_DICTIONARY
                    && matches!(
                        schema.columns[filter_idx].1,
                        ColumnType::Float64 | ColumnType::Float32
                    )
                {
                    Some(FloatDictView::parse(&f_bytes[1..])?)
                } else {
                    None
                };
                let filter_data_owned: Option<ColumnData> = if filter_is_bitpack
                    || filter_float_dict.is_some()
                {
                    None
                } else {
                    Some(if encoding_version >= 1 {
                        read_column_encoded(f_bytes, schema.columns[filter_idx].1)?.0
                    } else {
                        ColumnData::from_bytes_typed(f_bytes, schema.columns[filter_idx].1)?.0
                    })
                };

                // Get agg column via zero-copy PLAIN slice or fallback decode
                enum AggBuf { None, F64(Vec<f64>), I64(Vec<i64>), Owned(ColumnData) }
                let agg_buf: AggBuf = match agg_idx {
                    None => AggBuf::None,
                    Some(ai) if ai == filter_idx => AggBuf::None,
                    Some(ai) => {
                        let a_col_off = rcix[ai] as usize;
                        let a_data_start = a_col_off + null_bitmap_len;
                        if a_data_start + 1 < body.len() && encoding_version >= 1 {
                            let enc = body[a_data_start];
                            let payload = &body[a_data_start + 1..];
                            if enc == 0u8 && payload.len() >= 8 {
                                let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                                let col_type = schema.columns[ai].1;
                                if matches!(col_type, ColumnType::Float64 | ColumnType::Float32) {
                                    let n = count.min(rg_rows).min((payload.len() - 8) / 8);
                                    AggBuf::F64(payload[8..].chunks_exact(8).take(n).map(|c| f64::from_le_bytes(c.try_into().unwrap())).collect())
                                } else if matches!(col_type, ColumnType::Int64 | ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 | ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64) {
                                    let n = count.min(rg_rows).min((payload.len() - 8) / 8);
                                    AggBuf::I64(payload[8..].chunks_exact(8).take(n).map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect())
                                } else {
                                    let (ad, _) = read_column_encoded(&body[a_data_start..], col_type)?;
                                    AggBuf::Owned(ad)
                                }
                            } else {
                                let (ad, _) = read_column_encoded(&body[a_data_start..], schema.columns[ai].1)?;
                                AggBuf::Owned(ad)
                            }
                        } else if a_data_start < body.len() {
                            let (ad, _) = ColumnData::from_bytes_typed(&body[a_data_start..], schema.columns[ai].1)?;
                            AggBuf::Owned(ad)
                        } else { AggBuf::None }
                    }
                };
                // Expose as slices (owned or decoded)
                let agg_data_owned = if let AggBuf::Owned(ref cd) = agg_buf { Some(cd) } else { None };
                let agg_f64_own = agg_data_owned.and_then(|acd| match acd { ColumnData::Float64(v) => Some(v.as_slice()), _ => None });
                let agg_i64_own = agg_data_owned.and_then(|acd| match acd { ColumnData::Int64(v) => Some(v.as_slice()), _ => None });
                let agg_f64: Option<&[f64]> = if let AggBuf::F64(ref v) = agg_buf { Some(v.as_slice()) } else { agg_f64_own };
                let agg_i64: Option<&[i64]> = if let AggBuf::I64(ref v) = agg_buf { Some(v.as_slice()) } else { agg_i64_own };
                let (agg_f64, agg_i64) = if agg_idx == Some(filter_idx) {
                    // Same column as filter — derive from decoded data (non-BITPACK fallback)
                    match filter_data_owned.as_ref() {
                        Some(ColumnData::Float64(v)) => (Some(v.as_slice()), None::<&[i64]>),
                        Some(ColumnData::Int64(v)) => (None, Some(v.as_slice())),
                        _ => (agg_f64, agg_i64)
                    }
                } else { (agg_f64, agg_i64) };

                macro_rules! between_rg {
                    ($fvals:expr, $lo_c:expr, $hi_c:expr) => {{
                        let limit = rg_n.min($fvals.len());
                        if !has_deleted {
                            if let Some(av) = agg_f64 {
                                let limit = limit.min(av.len());
                                for i in 0..limit {
                                    let fv = unsafe { *$fvals.get_unchecked(i) };
                                    let mask = (fv >= $lo_c && fv <= $hi_c) as i64;
                                    let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                    unsafe { *group_counts.get_unchecked_mut(gid) += mask; *group_sums.get_unchecked_mut(gid) += mask as f64 * *av.get_unchecked(i); }
                                }
                            } else if let Some(av) = agg_i64 {
                                let limit = limit.min(av.len());
                                for i in 0..limit {
                                    let fv = unsafe { *$fvals.get_unchecked(i) };
                                    let mask = (fv >= $lo_c && fv <= $hi_c) as i64;
                                    let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                    unsafe { *group_counts.get_unchecked_mut(gid) += mask; *group_sums.get_unchecked_mut(gid) += mask as f64 * (*av.get_unchecked(i) as f64); }
                                }
                            } else {
                                for i in 0..limit {
                                    let fv = unsafe { *$fvals.get_unchecked(i) };
                                    let mask = (fv >= $lo_c && fv <= $hi_c) as i64;
                                    let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                    unsafe { *group_counts.get_unchecked_mut(gid) += mask; }
                                }
                            }
                        } else {
                            for i in 0..limit {
                                if !del_bytes.is_empty() && (del_bytes[i/8] >> (i%8)) & 1 != 0 { continue; }
                                let fv = unsafe { *$fvals.get_unchecked(i) };
                                if fv >= $lo_c && fv <= $hi_c {
                                    let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                    if gid < num_groups {
                                        unsafe { *group_counts.get_unchecked_mut(gid) += 1; }
                                        if let Some(av) = agg_f64 { unsafe { *group_sums.get_unchecked_mut(gid) += *av.get_unchecked(i); } }
                                        else if let Some(av) = agg_i64 { unsafe { *group_sums.get_unchecked_mut(gid) += *av.get_unchecked(i) as f64; } }
                                    }
                                }
                            }
                        }
                    }};
                }

                if filter_is_bitpack && !bp_packed.is_empty() {
                    // BITPACK zero-copy: 64-bit wide unaligned reads — no Vec allocation
                    let delta_lo = (lo_i64.saturating_sub(bp_min_val)).max(0) as u64;
                    let delta_hi = if hi_i64 >= bp_min_val { (hi_i64 - bp_min_val) as u64 } else { u64::MAX };
                    let bp_mask = if bp_bit_width == 0 { 0u64 } else { (1u64 << bp_bit_width) - 1 };
                    let n = bp_count.min(rg_n);
                    // Inline extractor: reads 8-byte window, shifts, masks
                    macro_rules! bp_delta { ($i:expr) => {{
                        let bit_off = $i * bp_bit_width;
                        let byte_idx = bit_off / 8;
                        let bit_shift = bit_off % 8;
                        let raw = if byte_idx + 8 <= bp_packed.len() {
                            unsafe { (bp_packed.as_ptr().add(byte_idx) as *const u64).read_unaligned() }
                        } else {
                            let mut buf = [0u8; 8];
                            let avail = bp_packed.len().saturating_sub(byte_idx);
                            unsafe { std::ptr::copy_nonoverlapping(bp_packed.as_ptr().add(byte_idx), buf.as_mut_ptr(), avail); }
                            u64::from_le_bytes(buf)
                        };
                        (raw >> bit_shift) & bp_mask
                    }}}
                    if !has_deleted {
                        if let Some(av) = agg_f64 {
                            let n = n.min(av.len());
                            if bp_bit_width == 0 {
                                let passes = (bp_min_val >= lo_i64 && bp_min_val <= hi_i64) as i64;
                                for i in 0..n { let gid = unsafe { *gids_slice.get_unchecked(i) } as usize; unsafe { *group_counts.get_unchecked_mut(gid) += passes; *group_sums.get_unchecked_mut(gid) += passes as f64 * *av.get_unchecked(i); } }
                            } else if bp_bit_width == 6 {
                                // Specialized 6-bit path: 8 values per 6-byte (48-bit) chunk, fully inlined
                                macro_rules! do_scatter6 { ($w:expr, $sh:expr, $ii:expr) => {{
                                    let d = ($w >> $sh) & 63u64;
                                    let mask = (d >= delta_lo && d <= delta_hi) as i64;
                                    let gid = unsafe { *gids_slice.get_unchecked($ii) } as usize;
                                    unsafe { *group_counts.get_unchecked_mut(gid) += mask; *group_sums.get_unchecked_mut(gid) += mask as f64 * *av.get_unchecked($ii); }
                                }}}
                                let chunks = n / 8;
                                for c in 0..chunks {
                                    let base_byte = c * 6;
                                    let base_i = c * 8;
                                    let word = unsafe { (bp_packed.as_ptr().add(base_byte) as *const u64).read_unaligned() };
                                    do_scatter6!(word,  0, base_i+0);
                                    do_scatter6!(word,  6, base_i+1);
                                    do_scatter6!(word, 12, base_i+2);
                                    do_scatter6!(word, 18, base_i+3);
                                    do_scatter6!(word, 24, base_i+4);
                                    do_scatter6!(word, 30, base_i+5);
                                    do_scatter6!(word, 36, base_i+6);
                                    do_scatter6!(word, 42, base_i+7);
                                }
                                for i in (chunks * 8)..n {
                                    let delta = bp_delta!(i);
                                    let mask = (delta >= delta_lo && delta <= delta_hi) as i64;
                                    let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                    unsafe { *group_counts.get_unchecked_mut(gid) += mask; *group_sums.get_unchecked_mut(gid) += mask as f64 * *av.get_unchecked(i); }
                                }
                            } else {
                                for i in 0..n {
                                    let delta = bp_delta!(i);
                                    let mask = (delta >= delta_lo && delta <= delta_hi) as i64;
                                    let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                    unsafe { *group_counts.get_unchecked_mut(gid) += mask; *group_sums.get_unchecked_mut(gid) += mask as f64 * *av.get_unchecked(i); }
                                }
                            }
                        } else if let Some(av) = agg_i64 {
                            let n = n.min(av.len());
                            for i in 0..n {
                                let delta = if bp_bit_width == 0 { 0 } else { bp_delta!(i) };
                                let mask = (bp_bit_width == 0 || (delta >= delta_lo && delta <= delta_hi)) as i64;
                                let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                unsafe { *group_counts.get_unchecked_mut(gid) += mask; *group_sums.get_unchecked_mut(gid) += mask as f64 * *av.get_unchecked(i) as f64; }
                            }
                        } else if agg_idx == Some(filter_idx) {
                            // agg col == filter col (BITPACK): sum the filtered BITPACK values inline
                            for i in 0..n {
                                let delta = if bp_bit_width == 0 { 0u64 } else { bp_delta!(i) };
                                let mask = (bp_bit_width == 0 || (delta >= delta_lo && delta <= delta_hi)) as i64;
                                let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                let val = bp_min_val + delta as i64;
                                unsafe { *group_counts.get_unchecked_mut(gid) += mask; *group_sums.get_unchecked_mut(gid) += mask as f64 * val as f64; }
                            }
                        } else {
                            for i in 0..n {
                                let delta = if bp_bit_width == 0 { 0u64 } else { bp_delta!(i) };
                                let mask = (bp_bit_width == 0 || (delta >= delta_lo && delta <= delta_hi)) as i64;
                                let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                unsafe { *group_counts.get_unchecked_mut(gid) += mask; }
                            }
                        }
                    }
                } else if let Some(ref view) = filter_float_dict {
                    let n = view.row_count.min(rg_n);
                    let mut dict_matches = Vec::with_capacity(view.dict_size);
                    let mut dict_sums = Vec::with_capacity(view.dict_size);
                    for index in 0..view.dict_size {
                        let value = view.dictionary_value(index).unwrap_or(0.0);
                        let matches = value >= lo && value <= hi;
                        dict_matches.push(matches as i64);
                        dict_sums.push(if matches { value } else { 0.0 });
                    }
                    if !has_deleted {
                        if agg_idx == Some(filter_idx) {
                            for i in 0..n {
                                let index = unsafe { view.index_unchecked(i) };
                                if index >= view.dict_size {
                                    continue;
                                }
                                let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                unsafe {
                                    *group_counts.get_unchecked_mut(gid) +=
                                        *dict_matches.get_unchecked(index);
                                    *group_sums.get_unchecked_mut(gid) +=
                                        *dict_sums.get_unchecked(index);
                                }
                            }
                        } else if let Some(av) = agg_f64 {
                            let n = n.min(av.len());
                            for i in 0..n {
                                let index = unsafe { view.index_unchecked(i) };
                                if index >= view.dict_size {
                                    continue;
                                }
                                let mask = unsafe { *dict_matches.get_unchecked(index) };
                                let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                unsafe {
                                    *group_counts.get_unchecked_mut(gid) += mask;
                                    *group_sums.get_unchecked_mut(gid) +=
                                        mask as f64 * *av.get_unchecked(i);
                                }
                            }
                        } else {
                            for i in 0..n {
                                let index = unsafe { view.index_unchecked(i) };
                                if index >= view.dict_size {
                                    continue;
                                }
                                let mask = unsafe { *dict_matches.get_unchecked(index) };
                                let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                unsafe { *group_counts.get_unchecked_mut(gid) += mask; }
                            }
                        }
                    } else {
                        for i in 0..n {
                            if !del_bytes.is_empty()
                                && (del_bytes[i / 8] >> (i % 8)) & 1 != 0
                            {
                                continue;
                            }
                            let index = unsafe { view.index_unchecked(i) };
                            if index >= view.dict_size {
                                continue;
                            }
                            if unsafe { *dict_matches.get_unchecked(index) } != 0 {
                                let gid = unsafe { *gids_slice.get_unchecked(i) } as usize;
                                *group_counts.get_mut(gid).unwrap() += 1;
                                if agg_idx == Some(filter_idx) {
                                    *group_sums.get_mut(gid).unwrap() +=
                                        unsafe { *dict_sums.get_unchecked(index) };
                                }
                            }
                        }
                    }
                } else if let Some(ref filter_data) = filter_data_owned {
                    match filter_data {
                        ColumnData::Int64(vals) => { between_rg!(vals, lo_i64, hi_i64); }
                        ColumnData::Float64(vals) => { between_rg!(vals, lo, hi); }
                        _ => {}
                    }
                }
                rg_row_offset += rg_rows;
            }

            let results: Vec<(String, f64, i64)> = (0..num_groups)
                .filter(|&gid| group_counts[gid] > 0)
                .map(|gid| (dict_strings[gid].clone(), group_sums[gid], group_counts[gid]))
                .collect();
            return Ok(Some(results));
        }

        // FALLBACK: full materialization (no RCIX or old format)
        let mut needed: Vec<usize> = vec![filter_idx];
        if let Some(ai) = agg_idx { if ai != filter_idx { needed.push(ai); } }
        let (scanned, del_bytes) = self.scan_columns_mmap(&needed, &footer)?;
        let has_deleted = del_bytes.iter().any(|&b| b != 0);

        let filter_col_data = &scanned[0];
        let agg_col_data = agg_idx.map(|ai| {
            if ai == filter_idx { &scanned[0] } else { &scanned[1] }
        });

        let scan_rows = group_ids.len();

        // Pre-resolve agg data slice — avoids match/option check inside hot loop
        let agg_f64 = agg_col_data.and_then(|acd| match acd { ColumnData::Float64(v) => Some(v.as_slice()), _ => None });
        let agg_i64 = agg_col_data.and_then(|acd| match acd { ColumnData::Int64(v) => Some(v.as_slice()), _ => None });

        macro_rules! between_hot {
            ($fvals:expr, $lo_c:expr, $hi_c:expr) => {{
                let limit = scan_rows.min($fvals.len()).min(group_ids.len());
                if !has_deleted {
                    if let Some(av) = agg_f64 {
                        let limit = limit.min(av.len());
                        for i in 0..limit {
                            let fv = unsafe { *$fvals.get_unchecked(i) };
                            let mask = (fv >= $lo_c && fv <= $hi_c) as i64;
                            let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                            unsafe {
                                *group_counts.get_unchecked_mut(gid) += mask;
                                *group_sums.get_unchecked_mut(gid) += mask as f64 * *av.get_unchecked(i);
                            }
                        }
                    } else if let Some(av) = agg_i64 {
                        let limit = limit.min(av.len());
                        for i in 0..limit {
                            let fv = unsafe { *$fvals.get_unchecked(i) };
                            let mask = (fv >= $lo_c && fv <= $hi_c) as i64;
                            let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                            unsafe {
                                *group_counts.get_unchecked_mut(gid) += mask;
                                *group_sums.get_unchecked_mut(gid) += mask as f64 * (*av.get_unchecked(i) as f64);
                            }
                        }
                    } else {
                        for i in 0..limit {
                            let fv = unsafe { *$fvals.get_unchecked(i) };
                            let mask = (fv >= $lo_c && fv <= $hi_c) as i64;
                            let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                            unsafe { *group_counts.get_unchecked_mut(gid) += mask; }
                        }
                    }
                } else {
                    for i in 0..limit {
                        let b = i / 8; let bit = i % 8;
                        if b < del_bytes.len() && (del_bytes[b] >> bit) & 1 != 0 { continue; }
                        let fv = unsafe { *$fvals.get_unchecked(i) };
                        if fv >= $lo_c && fv <= $hi_c {
                            let gid = unsafe { *group_ids.get_unchecked(i) } as usize;
                            if gid < num_groups {
                                unsafe { *group_counts.get_unchecked_mut(gid) += 1; }
                                if let Some(av) = agg_f64 { unsafe { *group_sums.get_unchecked_mut(gid) += *av.get_unchecked(i); } }
                                else if let Some(av) = agg_i64 { unsafe { *group_sums.get_unchecked_mut(gid) += *av.get_unchecked(i) as f64; } }
                            }
                        }
                    }
                }
            }};
        }

        match filter_col_data {
            ColumnData::Int64(vals) => { between_hot!(vals, lo_i64, hi_i64); }
            ColumnData::Float64(vals) => { between_hot!(vals, lo, hi); }
            _ => return Ok(None),
        }

        let results: Vec<(String, f64, i64)> = (0..num_groups)
            .filter(|&gid| group_counts[gid] > 0)
            .map(|gid| (dict_strings[gid].clone(), group_sums[gid], group_counts[gid]))
            .collect();

        Ok(Some(results))
    }

    /// Execute BETWEEN + GROUP BY + aggregate directly on V4 columns.
    /// Supports both in-memory and mmap-only paths.
    pub fn execute_between_group_agg(
        &self,
        filter_col: &str,
        lo: f64,
        hi: f64,
        group_col: &str,
        agg_col: Option<&str>,
    ) -> io::Result<Option<Vec<(String, f64, i64)>>> {
        if !self.has_v4_in_memory_data() {
            // MMAP PATH: build dict + scan filter+agg from disk
            if let Some((dict_strings, group_ids)) = self.build_string_dict_cache(group_col)? {
                return self.execute_between_group_agg_cached_mmap(filter_col, lo, hi, &dict_strings, &group_ids, agg_col);
            }
            return Ok(None);
        }

        let schema = self.schema.read();
        let columns = self.columns.read();
        let deleted = self.deleted.read();
        let total_rows = self.ids.read().len();

        let filter_idx = match schema.get_index(filter_col) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let group_idx = match schema.get_index(group_col) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let agg_idx = agg_col.and_then(|ac| schema.get_index(ac));

        if filter_idx >= columns.len() || group_idx >= columns.len() {
            return Ok(None);
        }

        let has_deleted = deleted.iter().any(|&b| b != 0);
        let lo_i64 = lo.ceil() as i64;
        let hi_i64 = hi.floor() as i64;

        let (group_offsets, group_bytes) = match &columns[group_idx] {
            ColumnData::String { offsets, data } => (offsets, data),
            _ => return Ok(None),
        };
        let group_count = group_offsets.len().saturating_sub(1);
        let scan_rows = total_rows.min(group_count);

        // Single-pass: build group dict + filter + aggregate simultaneously
        // Use small linear-scan dict for ≤64 groups (faster than hash map for short strings)
        let mut dict_entries: Vec<(u32, u32)> = Vec::with_capacity(32); // (start, end) in group_bytes
        let mut dict_strings: Vec<String> = Vec::with_capacity(32);
        let mut group_sums = vec![0.0f64; 64]; // pre-alloc for up to 64 groups
        let mut group_counts = vec![0i64; 64];

        let filter_i64 = match &columns[filter_idx] {
            ColumnData::Int64(v) => Some(v.as_slice()),
            _ => None,
        };
        let filter_f64 = match &columns[filter_idx] {
            ColumnData::Float64(v) => Some(v.as_slice()),
            _ => None,
        };
        let agg_i64 = agg_idx.and_then(|idx| {
            if idx < columns.len() { match &columns[idx] { ColumnData::Int64(v) => Some(v.as_slice()), _ => None } } else { None }
        });
        let agg_f64 = agg_idx.and_then(|idx| {
            if idx < columns.len() { match &columns[idx] { ColumnData::Float64(v) => Some(v.as_slice()), _ => None } } else { None }
        });

        // Macro for the inner aggregation to avoid code duplication
        macro_rules! agg_row {
            ($gid:expr, $i:expr) => {
                group_counts[$gid] += 1;
                if let Some(av) = agg_f64 { if $i < av.len() { group_sums[$gid] += av[$i]; } }
                else if let Some(av) = agg_i64 { if $i < av.len() { group_sums[$gid] += av[$i] as f64; } }
            }
        }

        // Inline group lookup: linear scan of ≤64 entries
        #[inline(always)]
        fn find_group(dict: &[(u32, u32)], group_bytes: &[u8], s: usize, e: usize) -> Option<usize> {
            let needle = &group_bytes[s..e];
            let needle_len = (e - s) as u32;
            for (idx, &(ds, de)) in dict.iter().enumerate() {
                if de - ds == needle_len && &group_bytes[ds as usize..de as usize] == needle {
                    return Some(idx);
                }
            }
            None
        }

        // Single-pass: filter + group + aggregate
        if let Some(vals) = filter_i64 {
            let limit = scan_rows.min(vals.len());
            if has_deleted {
                for i in 0..limit {
                    let b = i / 8; let bit = i % 8;
                    if b < deleted.len() && (deleted[b] >> bit) & 1 != 0 { continue; }
                    if vals[i] >= lo_i64 && vals[i] <= hi_i64 && i < group_count {
                        let s = group_offsets[i] as usize;
                        let e = group_offsets[i + 1] as usize;
                        let gid = if let Some(g) = find_group(&dict_entries, group_bytes, s, e) { g }
                        else {
                            let g = dict_entries.len();
                            dict_entries.push((s as u32, e as u32));
                            dict_strings.push(std::str::from_utf8(&group_bytes[s..e]).unwrap_or("").to_string());
                            if g >= group_sums.len() { group_sums.resize(g + 16, 0.0); group_counts.resize(g + 16, 0); }
                            g
                        };
                        agg_row!(gid, i);
                    }
                }
            } else {
                for i in 0..limit {
                    if vals[i] >= lo_i64 && vals[i] <= hi_i64 && i < group_count {
                        let s = group_offsets[i] as usize;
                        let e = group_offsets[i + 1] as usize;
                        let gid = if let Some(g) = find_group(&dict_entries, group_bytes, s, e) { g }
                        else {
                            let g = dict_entries.len();
                            dict_entries.push((s as u32, e as u32));
                            dict_strings.push(std::str::from_utf8(&group_bytes[s..e]).unwrap_or("").to_string());
                            if g >= group_sums.len() { group_sums.resize(g + 16, 0.0); group_counts.resize(g + 16, 0); }
                            g
                        };
                        agg_row!(gid, i);
                    }
                }
            }
        } else if let Some(vals) = filter_f64 {
            let limit = scan_rows.min(vals.len());
            for i in 0..limit {
                if has_deleted {
                    let b = i / 8; let bit = i % 8;
                    if b < deleted.len() && (deleted[b] >> bit) & 1 != 0 { continue; }
                }
                if vals[i] >= lo && vals[i] <= hi && i < group_count {
                    let s = group_offsets[i] as usize;
                    let e = group_offsets[i + 1] as usize;
                    let gid = if let Some(g) = find_group(&dict_entries, group_bytes, s, e) { g }
                    else {
                        let g = dict_entries.len();
                        dict_entries.push((s as u32, e as u32));
                        dict_strings.push(std::str::from_utf8(&group_bytes[s..e]).unwrap_or("").to_string());
                        if g >= group_sums.len() { group_sums.resize(g + 16, 0.0); group_counts.resize(g + 16, 0); }
                        g
                    };
                    agg_row!(gid, i);
                }
            }
        }

        let num_groups = dict_entries.len();

        let results: Vec<(String, f64, i64)> = (0..num_groups)
            .filter(|&gid| group_counts[gid] > 0)
            .map(|gid| (dict_strings[gid].clone(), group_sums[gid], group_counts[gid]))
            .collect();

        Ok(Some(results))
    }

    /// Execute GROUP BY + aggregate directly on V4 columns (no WHERE filter).
    /// Supports both in-memory and mmap-only paths.
    pub fn execute_group_agg(
        &self,
        group_col: &str,
        agg_cols: &[(&str, bool)],
    ) -> io::Result<Option<Vec<(String, Vec<(f64, i64)>)>>> {
        if !self.has_v4_in_memory_data() {
            // MMAP PATH: build dict cache from disk, then aggregate
            if let Some((dict_strings, group_ids)) = self.build_string_dict_cache(group_col)? {
                return self.execute_group_agg_cached_mmap(&dict_strings, &group_ids, agg_cols);
            }
            return Ok(None);
        }

        let schema = self.schema.read();
        let columns = self.columns.read();
        let deleted = self.deleted.read();
        let total_rows = self.ids.read().len();

        let group_idx = match schema.get_index(group_col) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        if group_idx >= columns.len() { return Ok(None); }

        let has_deleted = deleted.iter().any(|&b| b != 0);

        let (group_offsets, group_bytes) = match &columns[group_idx] {
            ColumnData::String { offsets, data } => (offsets, data),
            _ => return Ok(None),
        };
        let group_count = group_offsets.len().saturating_sub(1);
        let scan_rows = total_rows.min(group_count);
        let num_aggs = agg_cols.len();

        // Resolve agg column slices
        struct AggSlice<'a> { i64_vals: Option<&'a [i64]>, f64_vals: Option<&'a [f64]>, is_count: bool }
        let agg_slices: Vec<AggSlice> = agg_cols.iter().map(|(name, is_count)| {
            if *is_count {
                AggSlice { i64_vals: None, f64_vals: None, is_count: true }
            } else if let Some(idx) = schema.get_index(name) {
                if idx < columns.len() {
                    match &columns[idx] {
                        ColumnData::Int64(v) => AggSlice { i64_vals: Some(v.as_slice()), f64_vals: None, is_count: false },
                        ColumnData::Float64(v) => AggSlice { i64_vals: None, f64_vals: Some(v.as_slice()), is_count: false },
                        _ => AggSlice { i64_vals: None, f64_vals: None, is_count: true },
                    }
                } else { AggSlice { i64_vals: None, f64_vals: None, is_count: true } }
            } else { AggSlice { i64_vals: None, f64_vals: None, is_count: true } }
        }).collect();

        // Linear-scan dictionary for ≤64 groups (faster than hash map for short strings)
        let mut dict_entries: Vec<(u32, u32)> = Vec::with_capacity(32);
        let mut dict_strings: Vec<String> = Vec::with_capacity(32);
        let max_flat = 64 * num_aggs;
        let mut flat_sums = vec![0.0f64; max_flat];
        let mut flat_counts = vec![0i64; max_flat];

        #[inline(always)]
        fn find_group_ga(dict: &[(u32, u32)], gb: &[u8], s: usize, e: usize) -> Option<usize> {
            let needle = &gb[s..e];
            let nlen = (e - s) as u32;
            for (idx, &(ds, de)) in dict.iter().enumerate() {
                if de - ds == nlen && &gb[ds as usize..de as usize] == needle { return Some(idx); }
            }
            None
        }

        // Single-pass: group + aggregate
        for i in 0..scan_rows {
            if has_deleted {
                let b = i / 8; let bit = i % 8;
                if b < deleted.len() && (deleted[b] >> bit) & 1 != 0 { continue; }
            }
            let s = group_offsets[i] as usize;
            let e = group_offsets[i + 1] as usize;
            let gid = if let Some(g) = find_group_ga(&dict_entries, group_bytes, s, e) { g }
            else {
                let g = dict_entries.len();
                dict_entries.push((s as u32, e as u32));
                dict_strings.push(std::str::from_utf8(&group_bytes[s..e]).unwrap_or("").to_string());
                if (g + 1) * num_aggs > flat_sums.len() {
                    flat_sums.resize((g + 16) * num_aggs, 0.0);
                    flat_counts.resize((g + 16) * num_aggs, 0);
                }
                g
            };
            let base = gid * num_aggs;
            for (ai, agg) in agg_slices.iter().enumerate() {
                flat_counts[base + ai] += 1;
                if !agg.is_count {
                    if let Some(vals) = agg.f64_vals { if i < vals.len() { flat_sums[base + ai] += vals[i]; } }
                    else if let Some(vals) = agg.i64_vals { if i < vals.len() { flat_sums[base + ai] += vals[i] as f64; } }
                }
            }
        }

        let num_groups = dict_entries.len();
        let results: Vec<(String, Vec<(f64, i64)>)> = (0..num_groups)
            .filter(|&gid| flat_counts[gid * num_aggs] > 0)
            .map(|gid| {
                let aggs: Vec<(f64, i64)> = (0..num_aggs)
                    .map(|ai| (flat_sums[gid * num_aggs + ai], flat_counts[gid * num_aggs + ai]))
                    .collect();
                (dict_strings[gid].clone(), aggs)
            })
            .collect();

        Ok(Some(results))
    }

    /// Get the current durability level
    pub fn durability(&self) -> super::DurabilityLevel {
        self.durability
    }

    /// Set the durability level
    ///
    /// Note: This only affects future operations. Existing buffered data
    /// is not automatically synced when changing to a higher durability level.
    pub fn set_durability(&mut self, level: super::DurabilityLevel) {
        self.durability = level;
    }

    /// Checkpoint: merge WAL records into main file and clear WAL
    ///
    /// This is called automatically on save() for safe/max modes.
    /// After checkpoint, all data is in the main file and WAL is cleared.
    /// This improves read performance by eliminating WAL merge overhead.
    pub fn checkpoint(&self) -> io::Result<()> {
        if self.durability == super::DurabilityLevel::Fast {
            return Ok(()); // No WAL in fast mode
        }

        let wal_buffer = self.wal_buffer.read();
        if wal_buffer.is_empty() {
            return Ok(()); // Nothing to checkpoint
        }
        drop(wal_buffer);

        // Save main file (this persists all in-memory data including WAL records)
        self.save()?;

        // Clear WAL after successful save
        {
            let mut wal_buffer = self.wal_buffer.write();
            let mut wal_writer = self.wal_writer.write();

            wal_buffer.clear();

            // Create fresh WAL file
            if let Some(_) = wal_writer.take() {
                let wal_path = Self::wal_path(&self.path);
                *wal_writer = Some(super::incremental::WalWriter::create(
                    &wal_path,
                    self.next_id.load(Ordering::SeqCst)
                )?);
            }
        }

        Ok(())
    }

    /// Get number of pending WAL records
    pub fn wal_record_count(&self) -> usize {
        self.wal_buffer.read().len()
    }

    /// Check if WAL needs checkpoint (has pending records)
    pub fn needs_checkpoint(&self) -> bool {
        !self.wal_buffer.read().is_empty()
    }

    /// Get constraints for a column by name (returns default if none set)
    pub fn get_column_constraints(&self, name: &str) -> ColumnConstraints {
        let schema = self.schema.read();
        schema.get_constraints(name).cloned().unwrap_or_default()
    }

    /// Check if any column has constraints defined
    pub fn has_constraints(&self) -> bool {
        let schema = self.schema.read();
        schema.constraints.iter().any(|c| {
            c.not_null
                || c.primary_key
                || c.unique
                || c.default_value.is_some()
                || c.check_expr_sql.is_some()
                || c.foreign_key.is_some()
                || c.autoincrement
        })
    }

    /// Set constraints for a column by name
    pub fn set_column_constraints(&self, name: &str, cons: ColumnConstraints) {
        let mut schema = self.schema.write();
        if let Some(idx) = schema.get_index(name) {
            if idx < schema.constraints.len() {
                schema.constraints[idx] = cons;
            }
        }
    }

    /// Write a transactional INSERT record to WAL (P0-4: WAL-first for crash recovery)
    pub fn wal_write_txn_insert(&self, txn_id: u64, id: u64, data: HashMap<String, super::on_demand::ColumnValue>) -> io::Result<()> {
        let mut wal_writer = self.wal_writer.write();
        if let Some(writer) = wal_writer.as_mut() {
            let record = super::incremental::WalRecord::Insert { id, data, txn_id };
            writer.append(&record)?;
        }
        Ok(())
    }

    /// Write a transactional DELETE record to WAL (P0-4: WAL-first for crash recovery)
    pub fn wal_write_txn_delete(&self, txn_id: u64, id: u64) -> io::Result<()> {
        let mut wal_writer = self.wal_writer.write();
        if let Some(writer) = wal_writer.as_mut() {
            let record = super::incremental::WalRecord::Delete { id, txn_id };
            writer.append(&record)?;
        }
        Ok(())
    }

    /// Write a transaction BEGIN marker to WAL (for crash recovery)
    pub fn wal_write_txn_begin(&self, txn_id: u64) -> io::Result<()> {
        let mut wal_writer = self.wal_writer.write();
        if let Some(writer) = wal_writer.as_mut() {
            let record = super::incremental::WalRecord::TxnBegin { txn_id };
            writer.append(&record)?;
        }
        Ok(())
    }

    /// Write a transaction COMMIT marker to WAL (for crash recovery)
    pub fn wal_write_txn_commit(&self, txn_id: u64) -> io::Result<()> {
        let mut wal_writer = self.wal_writer.write();
        if let Some(writer) = wal_writer.as_mut() {
            let record = super::incremental::WalRecord::TxnCommit { txn_id };
            writer.append(&record)?;
            writer.flush()?;
            if self.durability == super::DurabilityLevel::Max {
                writer.sync()?;
            }
        }
        Ok(())
    }

    /// Write a transaction ROLLBACK marker to WAL (for crash recovery)
    pub fn wal_write_txn_rollback(&self, txn_id: u64) -> io::Result<()> {
        let mut wal_writer = self.wal_writer.write();
        if let Some(writer) = wal_writer.as_mut() {
            let record = super::incremental::WalRecord::TxnRollback { txn_id };
            writer.append(&record)?;
            writer.flush()?;
        }
        Ok(())
    }

    /// Sync the WAL to disk (fsync)
    pub fn wal_sync(&self) -> io::Result<()> {
        let mut wal_writer = self.wal_writer.write();
        if let Some(writer) = wal_writer.as_mut() {
            writer.sync()?;
        }
        Ok(())
    }

    // ========================================================================
    // Query APIs
    // ========================================================================

    /// Get row count (includes both base file and delta rows)
    pub fn row_count(&self) -> u64 {
        let base_rows = self.header.read().row_count;
        let delta_rows = self.delta_row_count() as u64;
        base_rows + delta_rows
    }

    /// Fast path: Get base table row count only (O(1) lock-free atomic read)
    /// Use this for COUNT(*) without WHERE clause
    pub fn base_row_count(&self) -> u64 {
        self.active_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get column names
    pub fn column_names(&self) -> Vec<String> {
        let mut names = vec!["_id".to_string()];
        names.extend(self.schema.read().columns.iter().map(|(name, _)| name.clone()));
        names
    }

    /// Get schema
    pub fn get_schema(&self) -> Vec<(String, ColumnType)> {
        self.schema.read().columns.clone()
    }

    pub fn vector_derivations(&self) -> Vec<VectorDerivation> {
        self.schema.read().vector_derivations.clone()
    }

    pub fn register_vector_derivation(
        &self,
        source: &str,
        target: &str,
        codec_version: u16,
    ) -> io::Result<()> {
        self.schema
            .write()
            .add_vector_derivation(source, target, codec_version)
    }

    /// Rename a column in the in-memory schema.
    /// Must be called alongside `TableStorageBackend::rename_column` so that
    /// `update_v4_footer_schema()` and `save()` persist the new name.
    pub fn rename_column_in_schema(&self, old_name: &str, new_name: &str) -> bool {
        self.schema.write().rename_column(old_name, new_name)
    }

    /// Get header info: (footer_offset, row_count)
    #[inline]
    pub fn header_info(&self) -> (u64, u64) {
        let h = self.header.read();
        (h.footer_offset, h.row_count)
    }

    /// Get the next available ID value
    #[inline]
    pub fn next_id_value(&self) -> u64 {
        self.next_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    // ========================================================================
    // Compatibility APIs (matching ColumnarStorage interface)
    // ========================================================================

    /// Insert rows using generic value type (compatibility with ColumnarStorage)
    /// Optimized with single-pass column collection
    ///
    /// For safe/max durability modes, rows are written to WAL first for crash recovery.
    /// - Safe mode: WAL is flushed but fsync is deferred to flush() call
    /// - Max mode: WAL is fsync'd immediately after each insert for strongest guarantee
    pub fn insert_rows(&self, rows: &[HashMap<String, ColumnValue>]) -> io::Result<Vec<u64>> {
        self.insert_rows_impl(rows)
    }

    /// Insert row-oriented facade values without first cloning them into an
    /// owned `ColumnValue` batch.
    pub(crate) fn insert_value_rows(
        &self,
        rows: &[HashMap<String, crate::data::Value>],
    ) -> io::Result<Vec<u64>> {
        self.insert_rows_impl(rows)
    }

    fn insert_rows_impl<V: AsColumnValueRef>(
        &self,
        rows: &[HashMap<String, V>],
    ) -> io::Result<Vec<u64>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // For safe/max durability with batch writes: use WAL for efficiency
        // Single-row writes skip WAL (original fsync-on-save behavior is faster)
        // WAL benefit: single I/O for many rows; WAL overhead: extra I/O for single rows
        let start_id = self.next_id.load(Ordering::SeqCst);
        let use_wal = self.durability != super::DurabilityLevel::Fast && rows.len() > 1;

        if use_wal {
            // Batch writes: use WAL for efficiency (single I/O for all rows)
            let mut wal_writer = self.wal_writer.write();

            if let Some(writer) = wal_writer.as_mut() {
                let wal_rows = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|(name, value)| {
                                (name.clone(), value.to_owned_column_value())
                            })
                            .collect()
                    })
                    .collect();
                let record = super::incremental::WalRecord::BatchInsert {
                    start_id,
                    rows: wal_rows,
                    txn_id: 0,
                };
                writer.append(&record)?;
                writer.flush()?;

                // For max durability: fsync WAL immediately
                if self.durability == super::DurabilityLevel::Max {
                    writer.sync()?;
                }
            }
        }
        // Note: For single-row writes, fsync happens in save() based on durability level

        // Handle case where all rows are empty dicts - still create rows with just _id
        let all_empty = rows.iter().all(|r| r.is_empty());
        if all_empty {
            let row_count = rows.len();
            let start_id = self.next_id.fetch_add(row_count as u64, Ordering::SeqCst);
            let ids: Vec<u64> = (start_id..start_id + row_count as u64).collect();

            // Add IDs
            self.ids.write().extend_from_slice(&ids);

            // Update header
            {
                let mut header = self.header.write();
                header.row_count = self.ids.read().len() as u64;
            }

            // Update id_to_idx mapping only if it's already built
            {
                let ids_guard = self.ids.read();
                let mut id_to_idx = self.id_to_idx.write();
                if let Some(map) = id_to_idx.as_mut() {
                    let start_idx = ids_guard.len() - ids.len();
                    for (i, &id) in ids.iter().enumerate() {
                        map.insert(id, start_idx + i);
                    }
                }
            }

            // Extend deleted bitmap
            {
                let mut deleted = self.deleted.write();
                let new_len = (self.ids.read().len() + 7) / 8;
                deleted.resize(new_len, 0);
            }

            // Update active count
            self.active_count.fetch_add(row_count as u64, Ordering::Relaxed);

            // Update pending rows counter and check auto-flush
            self.pending_rows.fetch_add(row_count as u64, Ordering::Relaxed);
            self.maybe_auto_flush()?;

            return Ok(ids);
        }

        // Single-pass optimized: determine column types from first non-empty row
        // and pre-allocate all vectors
        let num_rows = rows.len();
        let mut int_columns: HashMap<String, Vec<i64>> = HashMap::new();
        let mut float_columns: HashMap<String, Vec<f64>> = HashMap::new();
        let mut string_columns: HashMap<String, Vec<String>> = HashMap::new();
        let mut binary_columns: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut fixedlist_columns: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut blob_columns: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut bool_columns: HashMap<String, Vec<bool>> = HashMap::new();
        let mut null_positions: HashMap<String, Vec<bool>> = HashMap::new();

        // CRITICAL: Include ALL existing schema columns to ensure proper alignment
        // This fixes the partial column insert bug where missing columns don't get padded
        {
            let schema = self.schema.read();
            for (col_name, col_type) in &schema.columns {
                match col_type {
                    ColumnType::Int64 | ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 |
                    ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64 |
                    ColumnType::Timestamp | ColumnType::Date => {
                        int_columns.insert(col_name.clone(), Vec::with_capacity(num_rows));
                    }
                    ColumnType::Float64 | ColumnType::Float32 => {
                        float_columns.insert(col_name.clone(), Vec::with_capacity(num_rows));
                    }
                    ColumnType::String | ColumnType::StringDict | ColumnType::Null => {
                        string_columns.insert(col_name.clone(), Vec::with_capacity(num_rows));
                    }
                    ColumnType::Binary => {
                        binary_columns.insert(col_name.clone(), Vec::with_capacity(num_rows));
                    }
                    ColumnType::Blob => {
                        blob_columns.insert(col_name.clone(), Vec::with_capacity(num_rows));
                    }
                    ColumnType::FixedList
                    | ColumnType::Float16List
                    | ColumnType::BFloat16List
                    | ColumnType::Int8Vector
                    | ColumnType::UInt8Vector
                    | ColumnType::Bit1Vector
                    | ColumnType::TurboQuant2Vector
                    | ColumnType::TurboQuant3Vector
                    | ColumnType::TurboQuant4Vector => {
                        fixedlist_columns.insert(col_name.clone(), Vec::with_capacity(num_rows));
                    }
                    ColumnType::Bool => {
                        bool_columns.insert(col_name.clone(), Vec::with_capacity(num_rows));
                    }
                }
                null_positions.insert(col_name.clone(), Vec::with_capacity(num_rows));
            }
        }

        // Also determine schema from input rows for NEW columns
        let sample_size = std::cmp::min(10, num_rows);
        for row in rows.iter().take(sample_size) {
            for (key, val) in row {
                if int_columns.contains_key(key) || float_columns.contains_key(key)
                    || string_columns.contains_key(key) || binary_columns.contains_key(key)
                    || blob_columns.contains_key(key)
                    || bool_columns.contains_key(key) {
                    continue;
                }
                match val.as_column_value_ref() {
                    ColumnValueRef::Int64(_) => { int_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                    ColumnValueRef::Float64(_) => { float_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                    ColumnValueRef::String(_) => { string_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                    ColumnValueRef::Binary(_) => { binary_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                    ColumnValueRef::Blob(_) => { blob_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                    ColumnValueRef::FixedList(_) => { fixedlist_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                    ColumnValueRef::Bool(_) => { bool_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                    ColumnValueRef::Null => { string_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                }
                null_positions.insert(key.clone(), Vec::with_capacity(num_rows));
            }
        }

        // Pre-allocate NULL string to avoid repeated allocation
        static NULL_MARKER: &str = "\x00__NULL__\x00";

        // Single pass: collect all values and track NULLs
        // Note: For homogeneous data (common case), new columns won't be discovered mid-stream
        for row in rows {
            // Handle new columns discovered mid-stream (rare case for heterogeneous data)
            for (key, val) in row {
                if !int_columns.contains_key(key) && !float_columns.contains_key(key)
                    && !string_columns.contains_key(key) && !binary_columns.contains_key(key)
                    && !blob_columns.contains_key(key)
                    && !fixedlist_columns.contains_key(key) && !bool_columns.contains_key(key) {
                    match val.as_column_value_ref() {
                        ColumnValueRef::Int64(_) => { int_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                        ColumnValueRef::Float64(_) => { float_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                        ColumnValueRef::String(_) => { string_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                        ColumnValueRef::Binary(_) => { binary_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                        ColumnValueRef::Blob(_) => { blob_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                        ColumnValueRef::FixedList(_) => { fixedlist_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                        ColumnValueRef::Bool(_) => { bool_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                        ColumnValueRef::Null => { string_columns.insert(key.clone(), Vec::with_capacity(num_rows)); }
                    }
                    null_positions.insert(key.clone(), Vec::with_capacity(num_rows));
                }
            }

            // Collect values for all columns and track NULL positions
            for (key, col) in int_columns.iter_mut() {
                let (val, is_null) = match row.get(key).map(|v| v.as_column_value_ref()) {
                    Some(ColumnValueRef::Int64(v)) => (v, false),
                    Some(ColumnValueRef::Null) | None => (0, true),
                    _ => (0, true),
                };
                col.push(val);
                null_positions.get_mut(key).unwrap().push(is_null);
            }
            for (key, col) in float_columns.iter_mut() {
                let (val, is_null) = match row.get(key).map(|v| v.as_column_value_ref()) {
                    Some(ColumnValueRef::Float64(v)) => (v, false),
                    Some(ColumnValueRef::Null) | None => (0.0, true),
                    _ => (0.0, true),
                };
                col.push(val);
                null_positions.get_mut(key).unwrap().push(is_null);
            }
            for (key, col) in string_columns.iter_mut() {
                let (val, is_null) = match row.get(key).map(|v| v.as_column_value_ref()) {
                    Some(ColumnValueRef::String(v)) => (v.into_owned(), false),
                    Some(ColumnValueRef::Null) => (NULL_MARKER.to_string(), true),
                    None => (String::new(), true),
                    _ => (String::new(), true),
                };
                col.push(val);
                null_positions.get_mut(key).unwrap().push(is_null);
            }
            for (key, col) in binary_columns.iter_mut() {
                let (val, is_null) = match row.get(key).map(|v| v.as_column_value_ref()) {
                    Some(ColumnValueRef::Binary(v)) => (v.to_vec(), false),
                    Some(ColumnValueRef::Null) | None => (Vec::new(), true),
                    _ => (Vec::new(), true),
                };
                col.push(val);
                null_positions.get_mut(key).unwrap().push(is_null);
            }
            for (key, col) in blob_columns.iter_mut() {
                let (val, is_null) = match row.get(key).map(|v| v.as_column_value_ref()) {
                    Some(ColumnValueRef::Blob(v)) | Some(ColumnValueRef::Binary(v)) => {
                        (self.write_blob_value(v)?, false)
                    }
                    Some(ColumnValueRef::Null) | None => (Vec::new(), true),
                    _ => (Vec::new(), true),
                };
                col.push(val);
                null_positions.get_mut(key).unwrap().push(is_null);
            }
            for (key, col) in fixedlist_columns.iter_mut() {
                let (val, is_null) = match row.get(key).map(|v| v.as_column_value_ref()) {
                    Some(ColumnValueRef::FixedList(v)) => (v.to_vec(), false),
                    Some(ColumnValueRef::Null) | None => (Vec::new(), true),
                    _ => (Vec::new(), true),
                };
                col.push(val);
                null_positions.get_mut(key).unwrap().push(is_null);
            }
            for (key, col) in bool_columns.iter_mut() {
                let (val, is_null) = match row.get(key).map(|v| v.as_column_value_ref()) {
                    Some(ColumnValueRef::Bool(v)) => (v, false),
                    Some(ColumnValueRef::Null) | None => (false, true),
                    _ => (false, true),
                };
                col.push(val);
                null_positions.get_mut(key).unwrap().push(is_null);
            }
        }

        let result = self.insert_typed_with_nulls_full_with_blobs(
            int_columns,
            float_columns,
            string_columns,
            binary_columns,
            fixedlist_columns,
            blob_columns,
            bool_columns,
            null_positions,
        )?;
        self.maybe_auto_flush()?;

        Ok(result)
    }

    /// Insert typed columns and immediately persist to disk
    ///
    /// This is the preferred method for direct writes - data is immediately
    /// visible to the executor after this call returns.
    pub fn insert_typed_and_persist(
        &self,
        int_columns: HashMap<String, Vec<i64>>,
        float_columns: HashMap<String, Vec<f64>>,
        string_columns: HashMap<String, Vec<String>>,
        bool_columns: HashMap<String, Vec<bool>>,
    ) -> io::Result<Vec<u64>> {
        let ids = self.insert_typed(int_columns, float_columns, string_columns, HashMap::new(), bool_columns)?;
        if !ids.is_empty() {
            self.save()?;
        }
        Ok(ids)
    }

    /// Append delta (for compatibility - just calls insert_rows + save)
    pub fn append_delta(&self, rows: &[HashMap<String, ColumnValue>]) -> io::Result<Vec<u64>> {
        let ids = self.insert_rows(rows)?;
        self.save()?;
        Ok(ids)
    }

    /// Fast delta append (same as append_delta for this format)
    pub fn append_delta_fast(&self, rows: &[HashMap<String, ColumnValue>]) -> io::Result<Vec<u64>> {
        self.append_delta(rows)
    }

    /// Check if compaction is needed (true if delta file exists)
    pub fn needs_compaction(&self) -> bool {
        self.has_delta()
    }

    /// Flush changes to disk
    pub fn flush(&self) -> io::Result<()> {
        self.save()
    }

    /// Close storage and release all resources
    /// IMPORTANT: On Windows, mmap must be released before temp directory cleanup
    pub fn close(&self) -> io::Result<()> {
        // Save any pending changes first
        self.save()?;

        // Release mmap cache BEFORE closing file (critical for Windows)
        self.mmap_cache.write().invalidate();

        // Close file handle
        *self.file.write() = None;
        *self.write_file.write() = None;
        *self.delta_file.write() = None;

        Ok(())
    }

    /// Release mmap without saving (for cleanup scenarios)
    pub fn release_mmap(&self) {
        self.mmap_cache.write().invalidate();
    }

    fn try_read_col_stats_sidecar(&self) -> Option<std::collections::HashMap<String, (i64, f64, f64, f64, bool)>> {
        let sidecar_path = std::path::PathBuf::from(format!("{}.stats", self.path.display()));
        let data = std::fs::read(&sidecar_path).ok()?;
        if data.len() < 12 || &data[..8] != b"APEXSTAT" { return None; }
        let num_cols = u32::from_le_bytes(data[8..12].try_into().ok()?) as usize;
        let mut pos = 12usize;
        let mut result = std::collections::HashMap::with_capacity(num_cols);
        for _ in 0..num_cols {
            if pos + 2 > data.len() { return None; }
            let name_len = u16::from_le_bytes(data[pos..pos+2].try_into().ok()?) as usize;
            pos += 2;
            if pos + name_len + 33 > data.len() { return None; }
            let name = std::str::from_utf8(&data[pos..pos+name_len]).ok()?.to_string();
            pos += name_len;
            let count = i64::from_le_bytes(data[pos..pos+8].try_into().ok()?); pos += 8;
            let sum = f64::from_bits(u64::from_le_bytes(data[pos..pos+8].try_into().ok()?)); pos += 8;
            let min = f64::from_bits(u64::from_le_bytes(data[pos..pos+8].try_into().ok()?)); pos += 8;
            let max = f64::from_bits(u64::from_le_bytes(data[pos..pos+8].try_into().ok()?)); pos += 8;
            let is_int = data[pos] != 0; pos += 1;
            result.insert(name, (count, sum, min, max, is_int));
        }
        Some(result)
    }
}

impl Drop for OnDemandStorage {
    fn drop(&mut self) {
        // catch_unwind: on Windows, this destructor may run after parking_lot's
        // internal TLS is torn down, causing try_write() to panic.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Release mmap first (critical for Windows)
            // parking_lot's try_write returns Option, not Result
            if let Some(mut cache) = self.mmap_cache.try_write() {
                cache.invalidate();
            }
            // File handle will be dropped automatically after mmap is released
        }));
    }
}
