use super::*;

impl OnDemandStorage {
    pub fn scan_top_k_indices_mmap(
        &self,
        col_name: &str,
        k: usize,
        descending: bool,
    ) -> io::Result<Option<Vec<(usize, f64)>>> {
        if k == 0 {
            return Ok(Some(vec![]));
        }
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let col_idx = match schema.get_index(col_name) {
            Some(i) => i,
            None => return Ok(None),
        };
        let col_type = schema.columns[col_idx].1;
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
                | ColumnType::Timestamp
                | ColumnType::Date
        );
        let is_float = matches!(col_type, ColumnType::Float64 | ColumnType::Float32);
        if !is_int && !is_float {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for top-k scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        // heap: sorted Vec<(value, global_idx)>; descending → keep k largest
        let mut heap: Vec<(f64, usize)> = Vec::with_capacity(k + 1);
        let mut global_offset: usize = 0;

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                global_offset += rg_rows;
                continue;
            }

            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("RG extends past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            let compress_flag = if rg_bytes.len() >= 32 {
                rg_bytes[28]
            } else {
                RG_COMPRESS_NONE
            };
            let encoding_version = if rg_bytes.len() >= 32 {
                rg_bytes[29]
            } else {
                0
            };
            let decompressed = decompress_rg_body(compress_flag, &rg_bytes[32..])?;
            let body: &[u8] = decompressed.as_deref().unwrap_or(&rg_bytes[32..]);
            let id_section =
                rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));
            let del_vec_len = (rg_rows + 7) / 8;
            let null_bitmap_len = (rg_rows + 7) / 8;
            let has_deletes = rg_meta.deletion_count > 0;
            let del_bytes = if id_section + del_vec_len <= body.len() {
                &body[id_section..id_section + del_vec_len]
            } else {
                &[]
            };

            // Get pointer to column data via RCIX if available
            let col_bytes: &[u8] = if rg_i < footer.col_offsets.len()
                && col_idx < footer.col_offsets[rg_i].len()
                && compress_flag == RG_COMPRESS_NONE
            {
                let col_body_off = footer.col_offsets[rg_i][col_idx] as usize;
                let data_start = col_body_off + null_bitmap_len;
                if data_start > body.len() {
                    global_offset += rg_rows;
                    continue;
                }
                &body[data_start..]
            } else {
                // Fallback: sequential column scan
                let mut pos = id_section + del_vec_len;
                let mut found: &[u8] = &[];
                for ci in 0..schema.column_count() {
                    if pos + null_bitmap_len > body.len() {
                        break;
                    }
                    pos += null_bitmap_len;
                    if ci == col_idx {
                        found = &body[pos..];
                        break;
                    }
                    let consumed = if encoding_version >= 1 {
                        skip_column_encoded(&body[pos..], schema.columns[ci].1)?
                    } else {
                        ColumnData::skip_bytes_typed(&body[pos..], schema.columns[ci].1)?
                    };
                    pos += consumed;
                }
                found
            };

            if col_bytes.is_empty() {
                global_offset += rg_rows;
                continue;
            }

            let enc_offset = if encoding_version >= 1 { 1 } else { 0 };
            let encoding = if encoding_version >= 1 && !col_bytes.is_empty() {
                col_bytes[0]
            } else {
                COL_ENCODING_PLAIN
            };

            if encoding == COL_ENCODING_PLAIN && col_bytes.len() > enc_offset + 8 {
                let payload = &col_bytes[enc_offset..];
                let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                let n = count.min(rg_rows).min((payload.len() - 8) / 8);

                macro_rules! topk_scan {
                    ($vals:expr) => {{
                        if descending {
                            // Keep k largest: heap sorted descending, threshold = heap[k-1]
                            for i in 0..n {
                                if has_deletes
                                    && !del_bytes.is_empty()
                                    && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                {
                                    continue;
                                }
                                let val = $vals[i];
                                if heap.len() < k {
                                    let pos = heap.partition_point(|(v, _)| *v > val);
                                    heap.insert(pos, (val, global_offset + i));
                                } else if val > heap[k - 1].0 {
                                    let pos = heap.partition_point(|(v, _)| *v > val);
                                    heap.insert(pos, (val, global_offset + i));
                                    heap.pop();
                                }
                            }
                        } else {
                            // Keep k smallest: heap sorted ascending, threshold = heap[k-1]
                            for i in 0..n {
                                if has_deletes
                                    && !del_bytes.is_empty()
                                    && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                {
                                    continue;
                                }
                                let val = $vals[i];
                                if heap.len() < k {
                                    let pos = heap.partition_point(|(v, _)| *v < val);
                                    heap.insert(pos, (val, global_offset + i));
                                } else if val < heap[k - 1].0 {
                                    let pos = heap.partition_point(|(v, _)| *v < val);
                                    heap.insert(pos, (val, global_offset + i));
                                    heap.pop();
                                }
                            }
                        }
                    }};
                }

                if is_float {
                    let ptr = payload[8..].as_ptr();
                    if ptr as usize % std::mem::align_of::<f64>() == 0 {
                        let vals = unsafe { std::slice::from_raw_parts(ptr as *const f64, n) };
                        topk_scan!(vals);
                    } else {
                        let data = &payload[8..8 + n * 8];
                        let vals: Vec<f64> = (0..n)
                            .map(|i| f64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap()))
                            .collect();
                        topk_scan!(vals);
                    }
                } else {
                    let ptr = payload[8..].as_ptr();
                    if ptr as usize % std::mem::align_of::<i64>() == 0 {
                        let vals = unsafe { std::slice::from_raw_parts(ptr as *const i64, n) };
                        let fvals: Vec<f64> = vals.iter().map(|&v| v as f64).collect();
                        topk_scan!(fvals);
                    } else {
                        let data = &payload[8..8 + n * 8];
                        let fvals: Vec<f64> = (0..n)
                            .map(|i| {
                                i64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap())
                                    as f64
                            })
                            .collect();
                        topk_scan!(fvals);
                    }
                }
            } else {
                // Non-PLAIN: decode and scan
                let (col_data, _) = if encoding_version >= 1 {
                    read_column_encoded(col_bytes, col_type)?
                } else {
                    ColumnData::from_bytes_typed(col_bytes, col_type)?
                };
                let fvals: Vec<f64> = match &col_data {
                    ColumnData::Float64(v) => v.iter().map(|&x| x).collect(),
                    ColumnData::Int64(v) => v.iter().map(|&x| x as f64).collect(),
                    _ => {
                        global_offset += rg_rows;
                        continue;
                    }
                };
                let n = fvals.len().min(rg_rows);
                macro_rules! topk_scan2 {
                    ($vals:expr) => {{
                        if descending {
                            for i in 0..n {
                                if has_deletes
                                    && !del_bytes.is_empty()
                                    && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                {
                                    continue;
                                }
                                let val = $vals[i];
                                if heap.len() < k {
                                    let pos = heap.partition_point(|(v, _)| *v > val);
                                    heap.insert(pos, (val, global_offset + i));
                                } else if val > heap[k - 1].0 {
                                    let pos = heap.partition_point(|(v, _)| *v > val);
                                    heap.insert(pos, (val, global_offset + i));
                                    heap.pop();
                                }
                            }
                        } else {
                            for i in 0..n {
                                if has_deletes
                                    && !del_bytes.is_empty()
                                    && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                {
                                    continue;
                                }
                                let val = $vals[i];
                                if heap.len() < k {
                                    let pos = heap.partition_point(|(v, _)| *v < val);
                                    heap.insert(pos, (val, global_offset + i));
                                } else if val < heap[k - 1].0 {
                                    let pos = heap.partition_point(|(v, _)| *v < val);
                                    heap.insert(pos, (val, global_offset + i));
                                    heap.pop();
                                }
                            }
                        }
                    }};
                }
                topk_scan2!(fvals);
            }
            global_offset += rg_rows;
        }
        Ok(Some(heap.into_iter().map(|(v, i)| (i, v)).collect()))
    }

    /// Top-K rows by `LENGTH(str_col)` with a numeric tie-break column, scanned
    /// directly from the mmap string offsets.  For ASCII columns the character
    /// length equals the byte length; a non-ASCII column is counted correctly
    /// (continuation bytes are not characters) in a single prefix pass.
    /// Returns row indices sorted by (length, tie) in the requested order.
    pub fn scan_top_k_by_length_mmap(
        &self,
        str_col: &str,
        tie_col: Option<&str>,
        k: usize,
        length_desc: bool,
        tie_desc: bool,
    ) -> io::Result<Option<Vec<usize>>> {
        if k == 0 {
            return Ok(Some(vec![]));
        }
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let str_idx = match schema.get_index(str_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let tie_idx = match tie_col {
            Some(col) => match schema.get_index(col) {
                Some(i) => Some(i),
                None => return Ok(None),
            },
            None => None,
        };
        let str_type = schema.columns[str_idx].1;
        if !matches!(str_type, ColumnType::String) {
            return Ok(None); // dict-encoded or non-string → fall back
        }
        if let Some(i) = tie_idx {
            let t = schema.columns[i].1;
            let is_int = matches!(
                t,
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
            );
            let is_float = matches!(t, ColumnType::Float64 | ColumnType::Float32);
            if !is_int && !is_float {
                return Ok(None);
            }
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for LENGTH top-k scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        // heap: sorted best-first by (length, tie, idx)
        let better = |a: &(f64, f64, usize), b: &(f64, f64, usize)| -> bool {
            if a.0 != b.0 {
                if length_desc {
                    a.0 > b.0
                } else {
                    a.0 < b.0
                }
            } else if a.1 != b.1 {
                if tie_desc {
                    a.1 > b.1
                } else {
                    a.1 < b.1
                }
            } else {
                a.2 < b.2
            }
        };
        let mut heap: Vec<(f64, f64, usize)> = Vec::with_capacity(k + 1);
        let mut global_offset: usize = 0;

        // ── PARALLEL FAST PATH ───────────────────────────────────────────────
        // Multiple uncompressed RCIX row groups: compute a local top-k per RG in
        // parallel, then merge the (small) per-RG results. Falls back to the
        // sequential loop on any ineligible row group.
        if footer.row_groups.len() > 1 {
            use rayon::prelude::*;
            let all_fast = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
                let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
                if rg_end > mmap_ref.len() {
                    return false;
                }
                let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
                let compress_flag = rg_bytes.get(28).copied().unwrap_or(RG_COMPRESS_NONE);
                let enc_ver = rg_bytes.get(29).copied().unwrap_or(0);
                if compress_flag != RG_COMPRESS_NONE || enc_ver < 1 {
                    return false;
                }
                let Some(col_offsets) = footer.col_offsets.get(rg_i) else {
                    return false;
                };
                if col_offsets.len() <= str_idx {
                    return false;
                }
                if let Some(ti) = tie_idx {
                    if col_offsets.len() <= ti {
                        return false;
                    }
                }
                let bitmap_len = (rg_meta.row_count as usize + 7) / 8;
                // String column: no nulls + PLAIN encoding.
                let str_off = 32 + col_offsets[str_idx] as usize;
                if str_off + bitmap_len + 1 > rg_bytes.len() {
                    return false;
                }
                if rg_bytes[str_off..str_off + bitmap_len]
                    .iter()
                    .any(|&b| b != 0)
                {
                    return false;
                }
                if rg_bytes[str_off + bitmap_len] != COL_ENCODING_PLAIN {
                    return false;
                }
                // Tie column: no nulls.
                if let Some(ti) = tie_idx {
                    let t_off = 32 + col_offsets[ti] as usize;
                    if t_off + bitmap_len > rg_bytes.len() {
                        return false;
                    }
                    if rg_bytes[t_off..t_off + bitmap_len]
                        .iter()
                        .any(|&b| b != 0)
                    {
                        return false;
                    }
                }
                true
            });

            if all_fast {
                struct RgDesc {
                    rg_offset: usize,
                    rg_data_size: usize,
                    rg_rows: usize,
                    global_off: usize,
                    str_rcix: usize,
                    tie_rcix: Option<usize>,
                    has_deletes: bool,
                    id_section_len: usize,
                }
                let mut rg_descs: Vec<RgDesc> = Vec::with_capacity(footer.row_groups.len());
                let mut off = 0usize;
                for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
                    rg_descs.push(RgDesc {
                        rg_offset: rg_meta.offset as usize,
                        rg_data_size: rg_meta.data_size as usize,
                        rg_rows: rg_meta.row_count as usize,
                        global_off: off,
                        str_rcix: footer.col_offsets[rg_i][str_idx] as usize,
                        tie_rcix: tie_idx.map(|ti| footer.col_offsets[rg_i][ti] as usize),
                        has_deletes: rg_meta.deletion_count > 0,
                        id_section_len: rg_id_section_len(
                            rg_meta.row_count as usize,
                            mmap_ref
                                .get(rg_meta.offset as usize + 30)
                                .copied()
                                .unwrap_or(RG_IDS_PLAIN),
                        ),
                    });
                    off += rg_meta.row_count as usize;
                }

                let mmap_ptr: usize = mmap_ref.as_ptr() as usize;
                let mmap_len: usize = mmap_ref.len();
                let tie_type = tie_idx.map(|ti| schema.columns[ti].1);
                let results: Vec<io::Result<Option<Vec<(f64, f64, usize)>>>> = rg_descs
                    .par_iter()
                    .map(|desc| {
                        let mmap = unsafe {
                            std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len)
                        };
                        let rg_end = desc.rg_offset + desc.rg_data_size;
                        if rg_end > mmap.len() || rg_end < desc.rg_offset + 32 {
                            return Ok(None);
                        }
                        let body = &mmap[desc.rg_offset + 32..rg_end];
                        let rg_rows = desc.rg_rows;
                        let bitmap_len = (rg_rows + 7) / 8;
                        let del_bytes: Option<&[u8]> =
                            if desc.has_deletes && desc.id_section_len + bitmap_len <= body.len()
                            {
                                Some(&body[desc.id_section_len..desc.id_section_len + bitmap_len])
                            } else {
                                None
                            };
                        if desc.has_deletes && del_bytes.is_none() {
                            return Ok(None);
                        }

                        // String column (validated in all_fast: PLAIN, no nulls).
                        let str_col_body = &body[desc.str_rcix..];
                        let str_payload = &str_col_body[bitmap_len + 1..];
                        if str_payload.len() < 8 {
                            return Ok(None);
                        }
                        let count =
                            u64::from_le_bytes(str_payload[0..8].try_into().unwrap()) as usize;
                        let data_len_off = 8 + (count + 1) * 4;
                        if data_len_off + 8 > str_payload.len() {
                            return Ok(None);
                        }
                        let data_str_len = u64::from_le_bytes(
                            str_payload[data_len_off..data_len_off + 8].try_into().unwrap(),
                        ) as usize;
                        let data_start = data_len_off + 8;
                        let data_end = (data_start + data_str_len).min(str_payload.len());
                        if data_end < data_start {
                            return Ok(None);
                        }
                        let data_region = &str_payload[data_start..data_end];
                        let offsets = bytes_as_u32_slice(&str_payload[8..], count + 1);
                        let offsets: &[u32] = &offsets;
                        let n = count.min(rg_rows);
                        let all_ascii = !data_region.iter().any(|&b| b >= 0x80);
                        let mut cont_prefix: Vec<u32> = Vec::new();
                        if !all_ascii {
                            cont_prefix = vec![0u32; data_region.len() + 1];
                            for (i, &b) in data_region.iter().enumerate() {
                                cont_prefix[i + 1] = cont_prefix[i]
                                    + if (0x80..=0xBF).contains(&b) { 1 } else { 0 };
                            }
                        }

                        // Tie column.
                        let tie_vals: Vec<f64> = match (desc.tie_rcix, tie_type) {
                            (Some(tr), Some(tt)) => {
                                let tie_col_body = &body[tr..];
                                let (col_data, _) =
                                    read_column_encoded(&tie_col_body[bitmap_len..], tt)?;
                                match col_data {
                                    ColumnData::Int64(v) => {
                                        if v.len() < n {
                                            return Ok(None);
                                        }
                                        v.into_iter().map(|x| x as f64).collect()
                                    }
                                    ColumnData::Float64(v) => {
                                        if v.len() < n {
                                            return Ok(None);
                                        }
                                        v
                                    }
                                    _ => return Ok(None),
                                }
                            }
                            _ => Vec::new(),
                        };

                        let mut local: Vec<(f64, f64, usize)> = Vec::with_capacity(k + 1);
                        for i in 0..n {
                            if let Some(db) = del_bytes {
                                if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                            }
                            let s = offsets[i] as usize;
                            let e = offsets[i + 1] as usize;
                            if e > data_region.len() {
                                continue;
                            }
                            let byte_len = (e - s) as f64;
                            let char_len = if all_ascii {
                                byte_len
                            } else {
                                byte_len - (cont_prefix[e] - cont_prefix[s]) as f64
                            };
                            let tie = if tie_idx.is_none() {
                                0.0
                            } else {
                                tie_vals[i]
                            };
                            let item = (char_len, tie, desc.global_off + i);
                            if local.len() == k && better(&local[k - 1], &item) {
                                continue;
                            }
                            let pos = local.partition_point(|x| better(x, &item));
                            local.insert(pos, item);
                            if local.len() > k {
                                local.pop();
                            }
                        }
                        Ok(Some(local))
                    })
                    .collect();

                let mut all_local: Vec<(f64, f64, usize)> = Vec::new();
                for r in results {
                    match r? {
                        Some(local) => all_local.extend(local),
                        None => {
                            return Ok(None);
                        }
                    }
                }
                // Merge: keep the best k across all row groups.
                all_local.sort_by(|a, b| {
                    if better(a, b) {
                        std::cmp::Ordering::Less
                    } else if better(b, a) {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Equal
                    }
                });
                all_local.truncate(k);
                drop(mmap_guard);
                drop(file_guard);
                return Ok(Some(
                    all_local.into_iter().map(|(_, _, idx)| idx).collect(),
                ));
            }
        }

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                global_offset += rg_rows;
                continue;
            }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("RG extends past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            let compress_flag = if rg_bytes.len() >= 32 {
                rg_bytes[28]
            } else {
                RG_COMPRESS_NONE
            };
            let encoding_version = if rg_bytes.len() >= 32 {
                rg_bytes[29]
            } else {
                0
            };
            if compress_flag != RG_COMPRESS_NONE || encoding_version < 1 {
                return Ok(None); // compressed / legacy encodings → fall back
            }
            let body = &rg_bytes[32..];
            let id_section = rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));
            let del_vec_len = (rg_rows + 7) / 8;
            let null_bitmap_len = (rg_rows + 7) / 8;
            let has_deletes = rg_meta.deletion_count > 0;
            let del_bytes = if id_section + del_vec_len <= body.len() {
                &body[id_section..id_section + del_vec_len]
            } else {
                &[]
            };
            if has_deletes && del_bytes.is_empty() {
                return Ok(None);
            }

            // Access the string column via RCIX, and the tie column when present.
            let col_offsets = match footer.col_offsets.get(rg_i) {
                Some(v) if v.len() > str_idx => v,
                _ => return Ok(None),
            };
            let str_col_body = &body[col_offsets[str_idx] as usize..];

            // String column: null bitmap + encoding byte + payload.
            if str_col_body.len() < null_bitmap_len + 1 {
                return Ok(None);
            }
            let str_nulls = &str_col_body[..null_bitmap_len];
            if str_nulls.iter().any(|&b| b != 0) {
                return Ok(None); // null strings → fall back (length semantics)
            }
            // Only PLAIN string layout is parsed directly; anything else falls back.
            if str_col_body[null_bitmap_len] != COL_ENCODING_PLAIN {
                return Ok(None);
            }
            let str_payload = &str_col_body[null_bitmap_len + 1..];
            if str_payload.len() < 8 {
                return Ok(None);
            }
            let count = u64::from_le_bytes(str_payload[0..8].try_into().unwrap()) as usize;
            let data_len_off = 8 + (count + 1) * 4;
            if data_len_off + 8 > str_payload.len() {
                return Ok(None);
            }
            let data_str_len = u64::from_le_bytes(
                str_payload[data_len_off..data_len_off + 8].try_into().unwrap(),
            ) as usize;
            let data_start = data_len_off + 8;
            let data_end = (data_start + data_str_len).min(str_payload.len());
            if data_end < data_start {
                return Ok(None);
            }
            let data_region = &str_payload[data_start..data_end];
            let offsets = bytes_as_u32_slice(&str_payload[8..], count + 1);
            let offsets: &[u32] = &offsets;
            let n = count.min(rg_rows);

            // ASCII fast path: if the whole data region has no byte >= 0x80,
            // character length == byte length.
            let all_ascii = !data_region.iter().any(|&b| b >= 0x80);
            let mut cont_prefix: Vec<u32> = Vec::new();
            if !all_ascii {
                cont_prefix = vec![0u32; data_region.len() + 1];
                for (i, &b) in data_region.iter().enumerate() {
                    cont_prefix[i + 1] =
                        cont_prefix[i] + if (0x80..=0xBF).contains(&b) { 1 } else { 0 };
                }
            }

            // Tie column: decode via read_column_encoded so RLE / bit-packed
            // numeric encodings are handled, not just PLAIN.
            let tie_vals: Vec<f64> = match tie_idx {
                Some(ti) => {
                    if col_offsets.len() <= ti {
                        return Ok(None);
                    }
                    let tie_col_body = &body[col_offsets[ti] as usize..];
                    if tie_col_body.len() < null_bitmap_len + 1 {
                        return Ok(None);
                    }
                    let tie_nulls = &tie_col_body[..null_bitmap_len];
                    if tie_nulls.iter().any(|&b| b != 0) {
                        return Ok(None);
                    }
                    let (col_data, _) =
                        read_column_encoded(&tie_col_body[null_bitmap_len..], schema.columns[ti].1)?;
                    match col_data {
                        ColumnData::Int64(v) => {
                            if v.len() < n {
                                return Ok(None);
                            }
                            v.into_iter().map(|x| x as f64).collect()
                        }
                        ColumnData::Float64(v) => {
                            if v.len() < n {
                                return Ok(None);
                            }
                            v
                        }
                        _ => return Ok(None),
                    }
                }
                None => Vec::new(),
            };

            for i in 0..n {
                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                    continue;
                }
                let s = offsets[i] as usize;
                let e = offsets[i + 1] as usize;
                if e > data_region.len() {
                    continue;
                }
                let byte_len = (e - s) as f64;
                let char_len = if all_ascii {
                    byte_len
                } else {
                    byte_len - (cont_prefix[e] - cont_prefix[s]) as f64
                };
                let tie = if tie_idx.is_none() {
                    0.0
                } else {
                    tie_vals[i]
                };
                let item = (char_len, tie, global_offset + i);
                // Fast skip: once the heap is full, most rows are worse than the
                // current k-th best; test that (one comparison) before the
                // O(log k) binary search + O(k) insert.
                if heap.len() == k && better(&heap[k - 1], &item) {
                    continue;
                }
                let pos = heap.partition_point(|x| better(x, &item));
                heap.insert(pos, item);
                if heap.len() > k {
                    heap.pop();
                }
            }

            global_offset += rg_rows;
        }

        Ok(Some(heap.into_iter().map(|(_, _, idx)| idx).collect()))
    }
}
