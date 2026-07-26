use super::*;

impl OnDemandStorage {
    pub fn execute_filter_group_order(
        &self,
        filter_col: &str,
        filter_val: &str,
        group_col: &str,
        agg_col: Option<&str>,
        agg_func: crate::data::AggregateFunc,
        descending: bool,
        limit: usize,
        offset: usize,
    ) -> io::Result<Option<RecordBatch>> {
        use crate::data::AggregateFunc;
        use std::collections::HashMap;

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;

        // Resolve column indices
        let filter_idx = match schema.get_index(filter_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let group_idx = match schema.get_index(group_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let agg_idx = agg_col.and_then(|ac| schema.get_index(ac));

        let filter_type = schema.columns[filter_idx].1;
        let group_type = schema.columns[group_idx].1;

        // Only support string-typed filter and group columns
        if !matches!(filter_type, ColumnType::String | ColumnType::StringDict) {
            return Ok(None);
        }
        if !matches!(group_type, ColumnType::String | ColumnType::StringDict) {
            return Ok(None);
        }

        // Check agg column is numeric (if present)
        if let Some(ai) = agg_idx {
            let at = schema.columns[ai].1;
            if !matches!(
                at,
                ColumnType::Int64
                    | ColumnType::Int8
                    | ColumnType::Int16
                    | ColumnType::Int32
                    | ColumnType::UInt8
                    | ColumnType::UInt16
                    | ColumnType::UInt32
                    | ColumnType::UInt64
                    | ColumnType::Float64
                    | ColumnType::Float32
            ) {
                return Ok(None);
            }
        }

        // Verify all RGs have RCIX and are uncompressed
        let max_col = [filter_idx, group_idx]
            .iter()
            .copied()
            .chain(agg_idx.iter().copied())
            .max()
            .unwrap_or(0);
        let all_rcix = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
            if rg_meta.row_count == 0 {
                return true;
            }
            footer
                .col_offsets
                .get(rg_i)
                .map_or(false, |v| v.len() > max_col)
        });
        if !all_rcix {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for filter-group-order"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let filter_val_bytes = filter_val.as_bytes();
        let filter_val_len = filter_val_bytes.len();

        // Accumulate: group_key → (sum, count)
        let mut groups: HashMap<String, (f64, i64)> = HashMap::new();

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("RG past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.len() < 32 {
                continue;
            }
            let compress_flag = rg_bytes[28];
            let encoding_version = rg_bytes[29];
            if compress_flag != RG_COMPRESS_NONE || encoding_version < 1 {
                return Ok(None); // fallback for compressed RGs
            }

            let body = &rg_bytes[32..];
            let null_bitmap_len = (rg_rows + 7) / 8;
            let has_deleted = rg_meta.deletion_count > 0;
            let del_start =
                rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));
            let del_vec_len = null_bitmap_len;
            let del_bytes: &[u8] = if has_deleted && del_start + del_vec_len <= body.len() {
                &body[del_start..del_start + del_vec_len]
            } else {
                &[]
            };
            let rcix = &footer.col_offsets[rg_i];

            // ── Parse filter column (StringDict or String) ──
            let f_col_off = rcix[filter_idx] as usize;
            let f_data_start = f_col_off + null_bitmap_len;
            if f_data_start >= body.len() {
                continue;
            }
            let f_bytes = &body[f_data_start..];
            if f_bytes.is_empty() {
                continue;
            }
            let f_encoding = f_bytes[0];
            if f_encoding != COL_ENCODING_PLAIN {
                return Ok(None);
            }
            let f_data = &f_bytes[1..];

            // ── Parse group column ──
            let g_col_off = rcix[group_idx] as usize;
            let g_data_start = g_col_off + null_bitmap_len;
            if g_data_start >= body.len() {
                continue;
            }
            let g_bytes = &body[g_data_start..];
            if g_bytes.is_empty() {
                continue;
            }
            let g_encoding = g_bytes[0];
            if g_encoding != COL_ENCODING_PLAIN {
                return Ok(None);
            }
            let g_data = &g_bytes[1..];

            // ── Parse agg column (optional, numeric) ──
            enum AggSlice<'a> {
                None,
                F64(&'a [f64]),
                I64(&'a [i64]),
                OwnedF64(Vec<f64>),
                OwnedI64(Vec<i64>),
            }
            let agg_slice: AggSlice = if let Some(ai) = agg_idx {
                let a_col_off = rcix[ai] as usize;
                let a_data_start = a_col_off + null_bitmap_len;
                if a_data_start + 1 >= body.len() {
                    AggSlice::None
                } else {
                    let a_enc = body[a_data_start];
                    let a_payload = &body[a_data_start + 1..];
                    if a_enc == COL_ENCODING_PLAIN && a_payload.len() >= 8 {
                        let count =
                            u64::from_le_bytes(a_payload[0..8].try_into().unwrap()) as usize;
                        let col_type = schema.columns[ai].1;
                        let n = count.min(rg_rows);
                        if matches!(col_type, ColumnType::Float64 | ColumnType::Float32) {
                            let cow = bytes_as_f64_slice(&a_payload[8..], n);
                            match cow {
                                std::borrow::Cow::Borrowed(s) => AggSlice::F64(s),
                                std::borrow::Cow::Owned(v) => AggSlice::OwnedF64(v),
                            }
                        } else {
                            let cow = bytes_as_i64_slice(&a_payload[8..], n);
                            match cow {
                                std::borrow::Cow::Borrowed(s) => AggSlice::I64(s),
                                std::borrow::Cow::Owned(v) => AggSlice::OwnedI64(v),
                            }
                        }
                    } else {
                        AggSlice::None
                    }
                }
            } else {
                AggSlice::None
            };
            let agg_f64: Option<&[f64]> = match &agg_slice {
                AggSlice::F64(s) => Some(s),
                AggSlice::OwnedF64(v) => Some(v.as_slice()),
                _ => None,
            };
            let agg_i64: Option<&[i64]> = match &agg_slice {
                AggSlice::I64(s) => Some(s),
                AggSlice::OwnedI64(v) => Some(v.as_slice()),
                _ => None,
            };

            // ── Determine matching rows from filter column ──
            let filter_ct = schema.columns[filter_idx].1;
            let group_ct = schema.columns[group_idx].1;

            // ── FAST PATH: Both filter and group are StringDict ──
            // Use flat-array accumulation indexed by group dict_index (zero allocs in hot loop).
            // After this RG, merge O(dict_size) entries into global HashMap.
            if matches!(filter_ct, ColumnType::StringDict)
                && matches!(group_ct, ColumnType::StringDict)
            {
                // Parse filter dict
                if f_data.len() < 16 {
                    continue;
                }
                let f_row_count = u64::from_le_bytes(f_data[0..8].try_into().unwrap()) as usize;
                let f_dict_size = u64::from_le_bytes(f_data[8..16].try_into().unwrap()) as usize;
                if f_dict_size == 0 {
                    continue;
                }
                let f_indices = bytes_as_u32_slice(&f_data[16..], f_row_count);
                let f_dict_off_start = 16 + f_row_count * 4;
                let f_dict_offsets = bytes_as_u32_slice(&f_data[f_dict_off_start..], f_dict_size);
                let f_dict_data_len_off = f_dict_off_start + f_dict_size * 4;
                if f_dict_data_len_off + 8 > f_data.len() {
                    continue;
                }
                let f_dict_data_len = u64::from_le_bytes(
                    f_data[f_dict_data_len_off..f_dict_data_len_off + 8]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let f_dict_data_start = f_dict_data_len_off + 8;
                let f_raw_end = (f_dict_data_start + f_dict_data_len).min(f_data.len());
                let f_raw_dict = &f_data[f_dict_data_start..f_raw_end];

                // Find target filter dict index
                let mut target_dict_idx: Option<u32> = None;
                for di in 0..f_dict_size {
                    let start = f_dict_offsets[di] as usize;
                    let end = if di + 1 < f_dict_size {
                        f_dict_offsets[di + 1] as usize
                    } else {
                        f_raw_dict.len()
                    };
                    if end - start == filter_val_len && &f_raw_dict[start..end] == filter_val_bytes
                    {
                        target_dict_idx = Some((di + 1) as u32);
                        break;
                    }
                }
                let tdi = match target_dict_idx {
                    Some(v) => v,
                    None => continue,
                };

                // Parse group dict
                if g_data.len() < 16 {
                    continue;
                }
                let g_row_count = u64::from_le_bytes(g_data[0..8].try_into().unwrap()) as usize;
                let g_dict_size = u64::from_le_bytes(g_data[8..16].try_into().unwrap()) as usize;
                let g_indices = bytes_as_u32_slice(&g_data[16..], g_row_count);
                let g_dict_off_start = 16 + g_row_count * 4;
                let g_dict_offsets = bytes_as_u32_slice(&g_data[g_dict_off_start..], g_dict_size);
                let g_dict_data_len_off = g_dict_off_start + g_dict_size * 4;
                if g_dict_data_len_off + 8 > g_data.len() {
                    continue;
                }
                let g_dict_data_len = u64::from_le_bytes(
                    g_data[g_dict_data_len_off..g_dict_data_len_off + 8]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let g_dict_data_start = g_dict_data_len_off + 8;
                let g_dict_data_end = (g_dict_data_start + g_dict_data_len).min(g_data.len());
                let g_raw_dict = &g_data[g_dict_data_start..g_dict_data_end];

                // Flat accumulation arrays indexed by group dict_index (1-based, slot 0 = null)
                let flat_size = g_dict_size + 1; // +1 for null slot at index 0
                let mut flat_sums = vec![0.0f64; flat_size];
                let mut flat_counts = vec![0i64; flat_size];

                // Hot loop: scan filter indices, accumulate into flat arrays (zero allocations)
                let n = f_row_count.min(rg_rows).min(g_row_count);
                if !has_deleted {
                    if let Some(av) = agg_f64 {
                        let an = av.len();
                        for i in 0..n {
                            if unsafe { *f_indices.get_unchecked(i) } == tdi {
                                let gid = unsafe { *g_indices.get_unchecked(i) } as usize;
                                if gid < flat_size {
                                    unsafe {
                                        *flat_counts.get_unchecked_mut(gid) += 1;
                                    }
                                    if i < an {
                                        unsafe {
                                            *flat_sums.get_unchecked_mut(gid) +=
                                                *av.get_unchecked(i);
                                        }
                                    }
                                }
                            }
                        }
                    } else if let Some(av) = agg_i64 {
                        let an = av.len();
                        for i in 0..n {
                            if unsafe { *f_indices.get_unchecked(i) } == tdi {
                                let gid = unsafe { *g_indices.get_unchecked(i) } as usize;
                                if gid < flat_size {
                                    unsafe {
                                        *flat_counts.get_unchecked_mut(gid) += 1;
                                    }
                                    if i < an {
                                        unsafe {
                                            *flat_sums.get_unchecked_mut(gid) +=
                                                *av.get_unchecked(i) as f64;
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        // COUNT(*) only
                        for i in 0..n {
                            if unsafe { *f_indices.get_unchecked(i) } == tdi {
                                let gid = unsafe { *g_indices.get_unchecked(i) } as usize;
                                if gid < flat_size {
                                    unsafe {
                                        *flat_counts.get_unchecked_mut(gid) += 1;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Path with deletion checks
                    for i in 0..n {
                        if !del_bytes.is_empty() && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                            continue;
                        }
                        if f_indices[i] == tdi {
                            let gid = g_indices[i] as usize;
                            if gid < flat_size {
                                flat_counts[gid] += 1;
                                if let Some(av) = agg_f64 {
                                    if i < av.len() {
                                        flat_sums[gid] += av[i];
                                    }
                                } else if let Some(av) = agg_i64 {
                                    if i < av.len() {
                                        flat_sums[gid] += av[i] as f64;
                                    }
                                }
                            }
                        }
                    }
                }

                // Merge flat arrays into global HashMap (O(dict_size), not O(matching_rows))
                for gid in 1..flat_size {
                    if flat_counts[gid] > 0 {
                        let di = gid - 1;
                        let start = g_dict_offsets[di] as usize;
                        let end = if di + 1 < g_dict_size {
                            g_dict_offsets[di + 1] as usize
                        } else {
                            g_raw_dict.len()
                        };
                        let key = std::str::from_utf8(&g_raw_dict[start..end]).unwrap_or("");
                        let entry = groups.entry(key.to_string()).or_insert((0.0, 0));
                        entry.0 += flat_sums[gid];
                        entry.1 += flat_counts[gid];
                    }
                }
                continue; // done with this RG
            }

            // ── FALLBACK: non-StringDict group or filter column ──
            // Parse group column for per-row lookup
            enum GroupResolver<'a> {
                Dict {
                    indices: std::borrow::Cow<'a, [u32]>,
                    strings: Vec<&'a str>,
                },
                Plain {
                    count: usize,
                    offsets: std::borrow::Cow<'a, [u32]>,
                    data: &'a [u8],
                    data_start: usize,
                },
            }
            impl<'a> GroupResolver<'a> {
                fn get(&self, row: usize) -> Option<&str> {
                    match self {
                        GroupResolver::Dict { indices, strings } => {
                            if row >= indices.len() {
                                return None;
                            }
                            let idx = indices[row] as usize;
                            if idx == 0 {
                                return Some("");
                            }
                            strings.get(idx - 1).copied()
                        }
                        GroupResolver::Plain {
                            count,
                            offsets,
                            data,
                            data_start,
                        } => {
                            if row >= *count {
                                return None;
                            }
                            let s = offsets[row] as usize;
                            let e = offsets[row + 1] as usize;
                            if *data_start + e > data.len() {
                                return None;
                            }
                            std::str::from_utf8(&data[*data_start + s..*data_start + e]).ok()
                        }
                    }
                }
            }

            let g_resolver = if matches!(group_ct, ColumnType::StringDict) {
                if g_data.len() < 16 {
                    continue;
                }
                let row_count = u64::from_le_bytes(g_data[0..8].try_into().unwrap()) as usize;
                let dict_size = u64::from_le_bytes(g_data[8..16].try_into().unwrap()) as usize;
                let indices = bytes_as_u32_slice(&g_data[16..], row_count);
                let dict_off_start = 16 + row_count * 4;
                let dict_offsets = bytes_as_u32_slice(&g_data[dict_off_start..], dict_size);
                let dict_data_len_off = dict_off_start + dict_size * 4;
                if dict_data_len_off + 8 > g_data.len() {
                    continue;
                }
                let dict_data_len = u64::from_le_bytes(
                    g_data[dict_data_len_off..dict_data_len_off + 8]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let dict_data_start = dict_data_len_off + 8;
                let dict_data_end = (dict_data_start + dict_data_len).min(g_data.len());
                let raw_dict = &g_data[dict_data_start..dict_data_end];
                let mut strings: Vec<&str> = Vec::with_capacity(dict_size);
                for di in 0..dict_size {
                    let start = dict_offsets[di] as usize;
                    let end = if di + 1 < dict_size {
                        dict_offsets[di + 1] as usize
                    } else {
                        raw_dict.len()
                    };
                    strings.push(std::str::from_utf8(&raw_dict[start..end]).unwrap_or(""));
                }
                GroupResolver::Dict { indices, strings }
            } else {
                if g_data.len() < 8 {
                    continue;
                }
                let count = u64::from_le_bytes(g_data[0..8].try_into().unwrap()) as usize;
                let offsets = bytes_as_u32_slice(&g_data[8..], count + 1);
                let data_len_off = 8 + (count + 1) * 4;
                if data_len_off + 8 > g_data.len() {
                    continue;
                }
                let data_len =
                    u64::from_le_bytes(g_data[data_len_off..data_len_off + 8].try_into().unwrap())
                        as usize;
                let data_start = data_len_off + 8;
                GroupResolver::Plain {
                    count,
                    offsets,
                    data: g_data,
                    data_start,
                }
            };

            macro_rules! accumulate {
                ($row:expr) => {{
                    if let Some(group_str) = g_resolver.get($row) {
                        let entry = groups.entry(group_str.to_string()).or_insert((0.0, 0));
                        entry.1 += 1;
                        if let Some(av) = agg_f64 {
                            if $row < av.len() {
                                entry.0 += av[$row];
                            }
                        } else if let Some(av) = agg_i64 {
                            if $row < av.len() {
                                entry.0 += av[$row] as f64;
                            }
                        }
                    }
                }};
            }

            // ── Scan filter column for matching rows ──
            if matches!(filter_ct, ColumnType::StringDict) {
                if f_data.len() < 16 {
                    continue;
                }
                let row_count = u64::from_le_bytes(f_data[0..8].try_into().unwrap()) as usize;
                let dict_size = u64::from_le_bytes(f_data[8..16].try_into().unwrap()) as usize;
                if dict_size == 0 {
                    continue;
                }
                let indices_cow = bytes_as_u32_slice(&f_data[16..], row_count);
                let indices: &[u32] = &indices_cow;
                let dict_off_start = 16 + row_count * 4;
                let dict_offsets_cow = bytes_as_u32_slice(&f_data[dict_off_start..], dict_size);
                let dict_offsets: &[u32] = &dict_offsets_cow;
                let dict_data_len_off = dict_off_start + dict_size * 4;
                if dict_data_len_off + 8 > f_data.len() {
                    continue;
                }
                let dict_data_len = u64::from_le_bytes(
                    f_data[dict_data_len_off..dict_data_len_off + 8]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let f_dict_data_start = dict_data_len_off + 8;
                let f_raw_end = (f_dict_data_start + dict_data_len).min(f_data.len());
                let f_raw_dict = &f_data[f_dict_data_start..f_raw_end];
                // Find target dict index
                let mut target_dict_idx: Option<u32> = None;
                for di in 0..dict_size {
                    let start = dict_offsets[di] as usize;
                    let end = if di + 1 < dict_size {
                        dict_offsets[di + 1] as usize
                    } else {
                        f_raw_dict.len()
                    };
                    if end - start == filter_val_len && &f_raw_dict[start..end] == filter_val_bytes
                    {
                        target_dict_idx = Some((di + 1) as u32);
                        break;
                    }
                }
                if let Some(tdi) = target_dict_idx {
                    let n = row_count.min(rg_rows);
                    for i in 0..n {
                        if has_deleted
                            && !del_bytes.is_empty()
                            && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                        {
                            continue;
                        }
                        if indices[i] == tdi {
                            accumulate!(i);
                        }
                    }
                }
            } else {
                // Plain String filter
                if f_data.len() < 8 {
                    continue;
                }
                let count = u64::from_le_bytes(f_data[0..8].try_into().unwrap()) as usize;
                let offsets_cow = bytes_as_u32_slice(&f_data[8..], count + 1);
                let offsets: &[u32] = &offsets_cow;
                let data_len_off = 8 + (count + 1) * 4;
                if data_len_off + 8 > f_data.len() {
                    continue;
                }
                let data_start = data_len_off + 8;
                let n = count.min(rg_rows);
                for i in 0..n {
                    if has_deleted
                        && !del_bytes.is_empty()
                        && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                    {
                        continue;
                    }
                    let s = offsets[i] as usize;
                    let e = offsets[i + 1] as usize;
                    if e - s == filter_val_len
                        && &f_data[data_start + s..data_start + e] == filter_val_bytes
                    {
                        accumulate!(i);
                    }
                }
            }
        } // end RG loop

        if groups.is_empty() {
            return Ok(None);
        }

        // Compute final aggregate values and sort
        let mut results: Vec<(String, f64)> = groups
            .into_iter()
            .map(|(k, (sum, count))| {
                let val = match agg_func {
                    AggregateFunc::Sum => sum,
                    AggregateFunc::Count => count as f64,
                    AggregateFunc::Avg => {
                        if count > 0 {
                            sum / count as f64
                        } else {
                            0.0
                        }
                    }
                    _ => sum,
                };
                (k, val)
            })
            .collect();

        if descending {
            results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        }

        let results: Vec<_> = results.into_iter().skip(offset).take(limit).collect();
        if results.is_empty() {
            return Ok(None);
        }

        // Build Arrow RecordBatch
        use arrow::array::{Float64Array, StringArray};
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use std::sync::Arc;
        let group_values: Vec<&str> = results.iter().map(|(k, _)| k.as_str()).collect();
        let agg_values: Vec<f64> = results.iter().map(|(_, v)| *v).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new(group_col, ArrowDataType::Utf8, false),
            Field::new("agg_result", ArrowDataType::Float64, false),
        ]));
        let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(StringArray::from(group_values)),
            Arc::new(Float64Array::from(agg_values)),
        ];
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(Some(batch))
    }
}
