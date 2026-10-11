use super::*;

impl OnDemandStorage {
    /// Top-k row indices for a plain (non-dictionary) string column.
    ///
    /// High-cardinality strings are not dictionary-encoded, so
    /// `scan_top_k_string_indices_mmap` declines them and `ORDER BY <col> LIMIT k` read
    /// the whole column into Arrow and sorted it (200k rows: 7.6 ms against 0.3 ms for
    /// the dictionary form). The column is stored as a chain of blocks, each
    /// `[count: u64][offsets: u32 x (count+1)][len: u64][data]` (a 65k-row group arrives
    /// as 256-value blocks), so a bounded heap over the byte slices picks the k best
    /// without building any Arrow strings.
    ///
    /// A group whose sort column holds NULLs declines the query, as in the other lanes:
    /// a heap of byte slices cannot express NULLS FIRST/LAST.
    pub fn scan_top_k_plain_string_indices_mmap(
        &self,
        col_name: &str,
        k: usize,
        descending: bool,
    ) -> io::Result<Option<Vec<usize>>> {
        // Ascending only. The bounded heap improves on every row when the values arrive
        // in the opposite order to the request, so `DESC` over ascending data degraded
        // to O(n*k) churn -- 1M rows: 43 ms against 1.7 ms on the general path. (The
        // dictionary lane avoids this by ranking the dictionary instead.)
        if descending {
            return Ok(None);
        }
        if k == 0 {
            return Ok(Some(Vec::new()));
        }
        let Some(footer) = self.get_or_load_footer()? else {
            return Ok(None);
        };
        let schema = &footer.schema;
        let Some(col_idx) = schema.get_index(col_name) else {
            return Ok(None);
        };
        if !matches!(schema.columns[col_idx].1, ColumnType::String) {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for top-k scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let mut merged: Vec<(Vec<u8>, usize)> = Vec::new();
        let mut global_offset: usize = 0;
        // Still non-decreasing across every group seen so far.
        let mut monotone = true;

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("RG extends past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.len() < 32 {
                return Ok(None);
            }
            let compress_flag = rg_bytes[28];
            let encoding_version = rg_bytes[29];
            let id_section =
                rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));
            let del_vec_len = (rg_rows + 7) / 8;
            let null_bitmap_len = (rg_rows + 7) / 8;
            let has_deletes = rg_meta.deletion_count > 0;

            let decompressed = self.decompressed_rg_body(
                rg_meta.offset,
                rg_meta.offset + rg_meta.data_size,
                compress_flag,
                &rg_bytes[32..],
            )?;
            let body: &[u8] = decompressed
                .as_ref()
                .map(|body| body.as_slice())
                .unwrap_or(&rg_bytes[32..]);

            let del_bytes = if id_section + del_vec_len <= body.len() {
                &body[id_section..id_section + del_vec_len]
            } else {
                &[]
            };

            let (col_bytes, col_nulls): (&[u8], &[u8]) = if rg_i < footer.col_offsets.len()
                && col_idx < footer.col_offsets[rg_i].len()
                && compress_flag == RG_COMPRESS_NONE
            {
                let col_body_off = footer.col_offsets[rg_i][col_idx] as usize;
                let data_start = col_body_off + null_bitmap_len;
                if data_start > body.len() {
                    return Ok(None);
                }
                (&body[data_start..], &body[col_body_off..data_start])
            } else {
                let mut pos = id_section + del_vec_len;
                let mut found: &[u8] = &[];
                let mut found_nulls: &[u8] = &[];
                for ci in 0..schema.column_count() {
                    if pos + null_bitmap_len > body.len() {
                        break;
                    }
                    let nulls = &body[pos..pos + null_bitmap_len];
                    pos += null_bitmap_len;
                    if ci == col_idx {
                        found = &body[pos..];
                        found_nulls = nulls;
                        break;
                    }
                    let consumed = if encoding_version == RG_ENCODING_VERSION {
                        skip_column_encoded(&body[pos..], schema.columns[ci].1)?
                    } else {
                        ColumnData::skip_bytes_typed(&body[pos..], schema.columns[ci].1)?
                    };
                    pos += consumed;
                }
                (found, found_nulls)
            };

            if col_bytes.is_empty() || col_nulls.iter().any(|&byte| byte != 0) {
                return Ok(None);
            }
            // The column tag is the first byte of a versioned payload; a legacy group has
            // no tag and starts straight at the data. (The dictionary lane indexes
            // `col_bytes[enc_offset..]` because `StringDictView::parse` expects the
            // payload *including* that byte, which is why this split differs.)
            let (encoding, payload) = if encoding_version == RG_ENCODING_VERSION {
                let Some(&tag) = col_bytes.first() else {
                    return Ok(None);
                };
                (tag, &col_bytes[1..])
            } else {
                (COL_ENCODING_PLAIN, col_bytes)
            };
            if encoding != COL_ENCODING_PLAIN {
                return Ok(None);
            }

            // Walk the block chain, recording where each block's offsets and data live.
            // Anything that does not cover exactly `rg_rows` values declines.
            let mut blocks: Vec<(usize, usize, usize, usize, usize)> = Vec::new();
            let mut cursor = 0usize;
            let mut seen = 0usize;
            while seen < rg_rows {
                if cursor + 16 > payload.len() {
                    return Ok(None);
                }
                let count =
                    u64::from_le_bytes(payload[cursor..cursor + 8].try_into().unwrap()) as usize;
                if count == 0 || seen + count > rg_rows {
                    return Ok(None);
                }
                let offsets_pos = cursor + 8;
                let offsets_end = offsets_pos + (count + 1) * 4;
                if offsets_end + 8 > payload.len() {
                    return Ok(None);
                }
                let data_len = u64::from_le_bytes(
                    payload[offsets_end..offsets_end + 8].try_into().unwrap(),
                ) as usize;
                let data_start = offsets_end + 8;
                if count == 0 || seen + count > rg_rows {
                    return Ok(None);
                }
                if data_start + data_len > payload.len() {
                    return Ok(None);
                }
                blocks.push((seen, count, offsets_pos, data_start, data_len));
                seen += count;
                cursor = data_start + data_len;
            }

            // Values arriving in the requested order (the common case for append-only
            // data) need no selection at all: verify the column is still non-decreasing
            // and take the first k live rows. The check exits on the first inversion, so
            // unsorted data pays almost nothing for it.
            if monotone && seen > 0 {
                let mut previous: Option<&[u8]> = None;
                let offset_at_for = |offsets_pos: usize, idx: usize| -> usize {
                    u32::from_le_bytes(
                        payload[offsets_pos + idx * 4..offsets_pos + idx * 4 + 4]
                            .try_into()
                            .unwrap(),
                    ) as usize
                };
                'scan: for (row0, rows, offsets_pos, data_start, data_len) in blocks.iter() {
                    for local_row in 0..*rows {
                        let i = row0 + local_row;
                        if has_deletes
                            && !del_bytes.is_empty()
                            && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                        {
                            continue;
                        }
                        let (start, end) = (
                            offset_at_for(*offsets_pos, local_row),
                            offset_at_for(*offsets_pos, local_row + 1),
                        );
                        if start > end || end > *data_len || data_start + end > payload.len() {
                            return Ok(None);
                        }
                        let value = &payload[data_start + start..data_start + end];
                        if let Some(previous) = previous {
                            if value < previous {
                                monotone = false;
                                break 'scan;
                            }
                        }
                        previous = Some(value);
                    }
                }
                if monotone {
                    let mut picked: Vec<(Vec<u8>, usize)> = Vec::with_capacity(k);
                    'pick: for (row0, rows, offsets_pos, data_start, data_len) in blocks.iter() {
                        for local_row in 0..*rows {
                            let i = row0 + local_row;
                            if has_deletes
                                && !del_bytes.is_empty()
                                && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                            {
                                continue;
                            }
                            let (start, end) = (
                                offset_at_for(*offsets_pos, local_row),
                                offset_at_for(*offsets_pos, local_row + 1),
                            );
                            if start > end || end > *data_len || data_start + end > payload.len() {
                                return Ok(None);
                            }
                            picked.push((
                                payload[data_start + start..data_start + end].to_vec(),
                                global_offset + i,
                            ));
                            if picked.len() == k {
                                break 'pick;
                            }
                        }
                    }
                    merged.extend(picked);
                    global_offset += rg_rows;
                    continue;
                }
            }

            let mut local: Vec<(Vec<u8>, usize)> = Vec::with_capacity(k + 1);
            for (row0, rows, offsets_pos, data_start, data_len) in blocks {
                let offset_at = |idx: usize| -> usize {
                    u32::from_le_bytes(
                        payload[offsets_pos + idx * 4..offsets_pos + idx * 4 + 4]
                            .try_into()
                            .unwrap(),
                    ) as usize
                };
                for local_row in 0..rows {
                    let i = row0 + local_row;
                    if has_deletes && !del_bytes.is_empty() && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                    {
                        continue;
                    }
                    let (start, end) = (offset_at(local_row), offset_at(local_row + 1));
                    if start > end || end > data_len || data_start + end > payload.len() {
                        return Ok(None);
                    }
                    let value = &payload[data_start + start..data_start + end];
                    let better = match local.last() {
                        None => true,
                        Some((last, _)) => {
                            if descending {
                                value > last.as_slice()
                            } else {
                                value < last.as_slice()
                            }
                        }
                    };
                    if local.len() < k || better {
                        let pos = local.partition_point(|(v, _)| {
                            if descending {
                                v.as_slice() > value
                            } else {
                                v.as_slice() < value
                            }
                        });
                        local.insert(pos, (value.to_vec(), global_offset + i));
                        if local.len() > k {
                            local.pop();
                        }
                    }
                }
            }
            merged.extend(local);
            global_offset += rg_rows;
        }

        merged.sort_by(|a, b| {
            if descending {
                b.0.cmp(&a.0)
            } else {
                a.0.cmp(&b.0)
            }
        });
        merged.truncate(k);
        Ok(Some(merged.into_iter().map(|(_, idx)| idx).collect()))
    }

    /// Top-k row indices for a dictionary-encoded string column.
    ///
    /// `scan_top_k_indices_mmap` compares raw numeric values; a string column has none,
    /// so `ORDER BY <string> LIMIT k` fell back to materialising and sorting every row
    /// (200k rows: `ORDER BY city LIMIT 5` cost 10.3 ms against 0.33 ms for the numeric
    /// shape). Each row group carries its own dictionary, so rank that dictionary once
    /// per group, take the group's own top-k by rank, and merge the (already ordered)
    /// per-group lists -- no row sort and no value materialisation.
    ///
    /// A group whose sort column holds NULLs declines the whole query, exactly like the
    /// numeric lane, because a rank cannot express NULLS FIRST/LAST.
    pub fn scan_top_k_string_indices_mmap(
        &self,
        col_name: &str,
        k: usize,
        descending: bool,
    ) -> io::Result<Option<Vec<usize>>> {
        if k == 0 {
            return Ok(Some(Vec::new()));
        }
        let Some(footer) = self.get_or_load_footer()? else {
            return Ok(None);
        };
        let schema = &footer.schema;
        let Some(col_idx) = schema.get_index(col_name) else {
            return Ok(None);
        };
        if !matches!(schema.columns[col_idx].1, ColumnType::StringDict) {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for top-k scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let mut merged: Vec<(String, usize)> = Vec::new();
        let mut global_offset: usize = 0;

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("RG extends past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.len() < 32 {
                return Ok(None);
            }
            let compress_flag = rg_bytes[28];
            let encoding_version = rg_bytes[29];
            let id_section =
                rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));
            let del_vec_len = (rg_rows + 7) / 8;
            let null_bitmap_len = (rg_rows + 7) / 8;
            let has_deletes = rg_meta.deletion_count > 0;

            // Decompression borrows from a local, so the group is fully resolved
            // inside this scope before the next iteration.
            let decompressed = self.decompressed_rg_body(
                rg_meta.offset,
                rg_meta.offset + rg_meta.data_size,
                compress_flag,
                &rg_bytes[32..],
            )?;
            let body: &[u8] = decompressed
                .as_ref()
                .map(|body| body.as_slice())
                .unwrap_or(&rg_bytes[32..]);

            let del_bytes = if id_section + del_vec_len <= body.len() {
                &body[id_section..id_section + del_vec_len]
            } else {
                &[]
            };

            let (col_bytes, col_nulls): (&[u8], &[u8]) = if rg_i < footer.col_offsets.len()
                && col_idx < footer.col_offsets[rg_i].len()
                && compress_flag == RG_COMPRESS_NONE
            {
                let col_body_off = footer.col_offsets[rg_i][col_idx] as usize;
                let data_start = col_body_off + null_bitmap_len;
                if data_start > body.len() {
                    return Ok(None);
                }
                (&body[data_start..], &body[col_body_off..data_start])
            } else {
                let mut pos = id_section + del_vec_len;
                let mut found: &[u8] = &[];
                let mut found_nulls: &[u8] = &[];
                for ci in 0..schema.column_count() {
                    if pos + null_bitmap_len > body.len() {
                        break;
                    }
                    let nulls = &body[pos..pos + null_bitmap_len];
                    pos += null_bitmap_len;
                    if ci == col_idx {
                        found = &body[pos..];
                        found_nulls = nulls;
                        break;
                    }
                    let consumed = if encoding_version == RG_ENCODING_VERSION {
                        skip_column_encoded(&body[pos..], schema.columns[ci].1)?
                    } else {
                        ColumnData::skip_bytes_typed(&body[pos..], schema.columns[ci].1)?
                    };
                    pos += consumed;
                }
                (found, found_nulls)
            };

            if col_bytes.is_empty() || col_nulls.iter().any(|&byte| byte != 0) {
                return Ok(None);
            }
            let enc_offset = if encoding_version == RG_ENCODING_VERSION {
                1
            } else {
                0
            };
            let Some(&encoding) = col_bytes.first() else {
                return Ok(None);
            };
            if !matches!(
                encoding,
                COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
            ) {
                return Ok(None);
            }
            let view = match StringDictView::parse(
                &col_bytes[enc_offset..],
                encoding == COL_ENCODING_COMPACT_DICTIONARY,
            ) {
                Ok(view) => view,
                Err(_) => return Ok(None),
            };
            if view.row_count < rg_rows || view.dict_size <= 1 {
                return Ok(None);
            }

            // Order this group's dictionary once, then rank the rows by it.
            let mut order: Vec<u32> = (1..view.dict_size as u32).collect();
            let value_of = |key: u32| -> Option<&str> {
                view.value(key).and_then(|bytes| std::str::from_utf8(bytes).ok())
            };
            order.sort_unstable_by(|a, b| match (value_of(*a), value_of(*b)) {
                (Some(x), Some(y)) => x.cmp(y),
                _ => std::cmp::Ordering::Equal,
            });
            let mut rank_of_dict = vec![u32::MAX; view.dict_size];
            for (rank, key) in order.iter().enumerate() {
                rank_of_dict[*key as usize] = rank as u32;
            }

            let mut counts = vec![0u32; order.len()];
            for i in 0..rg_rows {
                if has_deletes && !del_bytes.is_empty() && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                    continue;
                }
                let Some(key) = view.index(i) else { continue };
                if let Some(slot) = rank_of_dict.get(key as usize) {
                    if *slot != u32::MAX {
                        counts[*slot as usize] += 1;
                    }
                }
            }

            // Walk ranks best-first until k rows are accounted for.
            let mut needed = k;
            let mut threshold: Option<u32> = None;
            let indices: Vec<usize> = if descending {
                (0..counts.len()).rev().collect()
            } else {
                (0..counts.len()).collect()
            };
            for rank in indices {
                let count = counts[rank] as usize;
                if count == 0 {
                    continue;
                }
                if count >= needed {
                    threshold = Some(rank as u32);
                    break;
                }
                needed -= count;
            }
            let Some(threshold) = threshold else { continue };
            let at_threshold = needed;

            // Collect them: everything better than the threshold, then the first
            // `at_threshold` rows that sit exactly on it.
            let mut taken_at_threshold = 0usize;
            let mut local: Vec<(String, usize)> = Vec::new();
            for i in 0..rg_rows {
                if has_deletes && !del_bytes.is_empty() && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                    continue;
                }
                let Some(key) = view.index(i) else { continue };
                let Some(slot) = rank_of_dict.get(key as usize).copied() else {
                    continue;
                };
                if slot == u32::MAX {
                    continue;
                }
                // Ranks ascend with the value, so a rank "better" than the
                // threshold is a *larger* one when descending and a smaller one
                // when ascending.
                // Ranks ascend with the value, so a rank better than the threshold is
                // a larger one when descending and a smaller one when ascending.
                let better = if descending {
                    slot > threshold
                } else {
                    slot < threshold
                };
                if better {
                    if let Some(value) = value_of(key) {
                        local.push((value.to_string(), global_offset + i));
                    }
                } else if slot == threshold && taken_at_threshold < at_threshold {
                    taken_at_threshold += 1;
                    if let Some(value) = value_of(key) {
                        local.push((value.to_string(), global_offset + i));
                    }
                }
                if local.len() >= k {
                    break;
                }
            }
            merged.extend(local);
            global_offset += rg_rows;
        }

        merged.sort_by(|a, b| if descending { b.0.cmp(&a.0) } else { a.0.cmp(&b.0) });
        merged.truncate(k);
        Ok(Some(merged.into_iter().map(|(_, idx)| idx).collect()))
    }

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
            let decompressed = self.decompressed_rg_body(
                rg_meta.offset,
                rg_meta.offset + rg_meta.data_size,
                compress_flag,
                &rg_bytes[32..],
            )?;
            let body: &[u8] = decompressed
                .as_ref()
                .map(|body| body.as_slice())
                .unwrap_or(&rg_bytes[32..]);
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
            let (col_bytes, col_nulls): (&[u8], &[u8]) = if rg_i < footer.col_offsets.len()
                && col_idx < footer.col_offsets[rg_i].len()
                && compress_flag == RG_COMPRESS_NONE
            {
                let col_body_off = footer.col_offsets[rg_i][col_idx] as usize;
                let data_start = col_body_off + null_bitmap_len;
                if data_start > body.len() {
                    global_offset += rg_rows;
                    continue;
                }
                (&body[data_start..], &body[col_body_off..data_start])
            } else {
                // Fallback: sequential column scan
                let mut pos = id_section + del_vec_len;
                let mut found: &[u8] = &[];
                let mut found_nulls: &[u8] = &[];
                for ci in 0..schema.column_count() {
                    if pos + null_bitmap_len > body.len() {
                        break;
                    }
                    let nulls = &body[pos..pos + null_bitmap_len];
                    pos += null_bitmap_len;
                    if ci == col_idx {
                        found = &body[pos..];
                        found_nulls = nulls;
                        break;
                    }
                    let consumed = if encoding_version == RG_ENCODING_VERSION {
                        skip_column_encoded(&body[pos..], schema.columns[ci].1)?
                    } else {
                        ColumnData::skip_bytes_typed(&body[pos..], schema.columns[ci].1)?
                    };
                    pos += consumed;
                }
                (found, found_nulls)
            };

            if col_bytes.is_empty() {
                global_offset += rg_rows;
                continue;
            }
            // This lane ranks raw column values, so a NULL would compete as its
            // stored placeholder (0) instead of being ordered by NULLS
            // FIRST/LAST. A row group whose sort column holds NULLs therefore
            // cannot be served here and falls back to the general path.
            //
            // The per-row-group null bitmap is authoritative. The persisted
            // zone-map null flag is not usable as a shortcut: it is written as
            // `false` on every path that builds zone maps.
            if col_nulls.iter().any(|&byte| byte != 0) {
                return Ok(None);
            }

            let enc_offset = if encoding_version == RG_ENCODING_VERSION { 1 } else { 0 };
            let encoding = if encoding_version == RG_ENCODING_VERSION && !col_bytes.is_empty() {
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
                let (col_data, _) = if encoding_version == RG_ENCODING_VERSION {
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
                if compress_flag != RG_COMPRESS_NONE || enc_ver != RG_ENCODING_VERSION {
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
            if compress_flag != RG_COMPRESS_NONE || encoding_version != RG_ENCODING_VERSION {
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
