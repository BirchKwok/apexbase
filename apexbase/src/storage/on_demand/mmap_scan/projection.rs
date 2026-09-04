use super::*;

impl OnDemandStorage {
    pub(crate) fn get_or_load_footer(&self) -> io::Result<Option<V4Footer>> {
        // Fast path: if mmap is valid (file_size > 0) and footer is cached, return immediately
        // without any syscall. mmap_cache is always invalidated after writes, so file_size == 0
        // when stale. This avoids a metadata() syscall on every query — especially costly on Windows
        // where NtQueryAttributesFile requires a kernel transition + security descriptor check.
        {
            let mc = self.mmap_cache.read();
            if mc.file_size > 0 {
                let cached = self.v4_footer.read();
                if cached.is_some() {
                    return Ok(cached.clone());
                }
            }
        }

        let file_len = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        if file_len < HEADER_SIZE as u64 {
            return Ok(None);
        }

        // Secondary check: return cached footer if mmap size still matches
        {
            let cached = self.v4_footer.read();
            if let Some(ref footer) = *cached {
                let mc = self.mmap_cache.read();
                if mc.file_size == file_len {
                    return Ok(Some(footer.clone()));
                }
            }
        }

        // Invalidate mmap if file size changed (another instance appended data)
        {
            let mut mc = self.mmap_cache.write();
            if mc.file_size != 0 && mc.file_size != file_len {
                mc.invalidate();
            }
        }

        // Ensure file handle is open (reopen if needed after save_v4 replaced file)
        {
            let fg = self.file.read();
            if fg.is_none() {
                drop(fg);
                if let Ok(f) = open_for_sequential_read(&self.path) {
                    *self.file.write() = Some(f);
                }
            }
        }

        let file_guard = self.file.read();
        let file_handle = match file_guard.as_ref() {
            Some(f) => f,
            None => return Ok(None),
        };
        let mut mmap = self.mmap_cache.write();

        // Read the on-disk header fresh to get current footer_offset
        let mut header_bytes = [0u8; HEADER_SIZE];
        mmap.read_at(file_handle, &mut header_bytes, 0)?;
        let on_disk_header = OnDemandHeader::from_bytes(&header_bytes)?;

        if on_disk_header.footer_offset == 0 || on_disk_header.version != FORMAT_VERSION_V4 {
            return Ok(None);
        }
        let footer_offset = on_disk_header.footer_offset;
        if footer_offset >= file_len {
            return Ok(None);
        }

        let footer_byte_count = (file_len - footer_offset) as usize;
        if footer_byte_count < 16 {
            // Too small to hold even footer_size + magic trailer
            return Ok(None);
        }
        let mut footer_bytes = vec![0u8; footer_byte_count];
        mmap.read_at(file_handle, &mut footer_bytes, footer_offset)?;
        drop(mmap);
        drop(file_guard);

        // Validate footer magic before parsing.
        // During concurrent append_row_group, the header may still reference the
        // old footer_offset after the old footer has been overwritten with RG data.
        // In that case the bytes here are not a valid footer — return None so the
        // caller gracefully retries or falls back.
        if footer_byte_count < 8 || &footer_bytes[footer_byte_count - 8..] != MAGIC_V4_FOOTER {
            return Ok(None);
        }

        let footer = match V4Footer::from_bytes(&footer_bytes) {
            Ok(f) => f,
            Err(_) => return Ok(None), // transient inconsistency during concurrent write
        };
        // Cache the footer for subsequent reads
        *self.v4_footer.write() = Some(footer.clone());
        Ok(Some(footer))
    }

    pub(super) fn invalidate_footer_cache(&self) {
        *self.v4_footer.write() = None;
    }

    pub fn to_arrow_batch_mmap(
        &self,
        column_names: Option<&[&str]>,
        include_id: bool,
        row_limit: Option<usize>,
        dict_encode_strings: bool,
    ) -> io::Result<Option<RecordBatch>> {
        self.to_arrow_batch_mmap_range(column_names, include_id, 0, row_limit, dict_encode_strings)
    }

    pub(crate) fn read_fts_string_columns_mmap(
        &self,
        column_names: &[String],
    ) -> io::Result<Option<(Vec<u32>, Vec<(String, ColumnData)>)>> {
        if column_names.is_empty() || self.has_delta() || self.has_v4_in_memory_data() {
            return Ok(None);
        }

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(Some((Vec::new(), Vec::new()))),
        };
        let schema = &footer.schema;

        let mut col_indices = Vec::with_capacity(column_names.len());
        for name in column_names {
            let Some(idx) = schema.get_index(name) else {
                return Ok(None);
            };
            match schema.columns[idx].1 {
                ColumnType::String | ColumnType::StringDict => col_indices.push(idx),
                _ => return Ok(None),
            }
        }

        let total_active = footer.total_active_rows() as usize;
        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for FTS mmap read"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let mut doc_ids: Vec<u32> = Vec::with_capacity(total_active);
        let mut columns: Vec<(String, ColumnData)> = column_names
            .iter()
            .map(|name| (name.clone(), ColumnData::new(ColumnType::String)))
            .collect();

        for (rg_idx, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 || rg_meta.active_rows() == 0 {
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
            if compress_flag != RG_COMPRESS_NONE
                || encoding_version < 1
                || rg_idx >= footer.col_offsets.len()
                || footer.col_offsets[rg_idx].is_empty()
            {
                return Ok(None);
            }

            let body = &rg_bytes[32..];
            let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
            let id_byte_len = rg_id_section_len(rg_rows, id_encoding);
            let del_vec_len = (rg_rows + 7) / 8;
            if id_byte_len + del_vec_len > body.len() {
                return Err(err_data("RG body truncated"));
            }

            let has_deletes = rg_meta.deletion_count > 0;
            let del_bytes = &body[id_byte_len..id_byte_len + del_vec_len];
            let active_indices: Option<Vec<usize>> = if has_deletes {
                Some(
                    (0..rg_rows)
                        .filter(|&i| (del_bytes[i / 8] >> (i % 8)) & 1 == 0)
                        .collect(),
                )
            } else {
                None
            };
            let active_len = active_indices.as_ref().map(|v| v.len()).unwrap_or(rg_rows);

            if let Some(indices) = active_indices.as_ref() {
                if id_encoding == RG_IDS_IMPLICIT_CONTIGUOUS {
                    doc_ids.extend(indices.iter().map(|&i| (rg_meta.min_id + i as u64) as u32));
                } else {
                    let ids = bytes_as_u64_slice(&body[..id_byte_len], rg_rows);
                    doc_ids.extend(indices.iter().map(|&i| ids[i] as u32));
                }
            } else if id_encoding == RG_IDS_IMPLICIT_CONTIGUOUS
                || rg_meta.max_id == rg_meta.min_id + rg_rows as u64 - 1
            {
                doc_ids.extend((0..rg_rows).map(|i| (rg_meta.min_id + i as u64) as u32));
            } else {
                let ids = bytes_as_u64_slice(&body[..id_byte_len], rg_rows);
                doc_ids.extend(ids.iter().map(|&id| id as u32));
            }

            let rg_col_offsets = &footer.col_offsets[rg_idx];
            let null_bitmap_len = (rg_rows + 7) / 8;
            for (out_pos, &col_idx) in col_indices.iter().enumerate() {
                let Some(&col_start_u32) = rg_col_offsets.get(col_idx) else {
                    let default_col = Self::create_default_column(ColumnType::String, active_len);
                    columns[out_pos].1.append(&default_col);
                    continue;
                };
                let col_start = col_start_u32 as usize;
                if col_start + null_bitmap_len > body.len() {
                    return Err(err_data("RG column null bitmap truncated"));
                }
                let null_bytes = &body[col_start..col_start + null_bitmap_len];
                let data_start = col_start + null_bitmap_len;
                if data_start > body.len() {
                    return Err(err_data("RG column data truncated"));
                }

                let col_type = schema.columns[col_idx].1;
                let (col_data, _) = read_column_encoded(&body[data_start..], col_type)?;
                let mut col_data = if matches!(col_data, ColumnData::StringDict { .. }) {
                    col_data.decode_string_dict()
                } else {
                    col_data
                };

                if let Some(indices) = active_indices.as_ref() {
                    col_data = col_data.filter_by_indices(indices);
                    if null_bytes.iter().any(|&b| b != 0) {
                        let mut active_nulls = vec![0u8; (indices.len() + 7) / 8];
                        for (new_idx, &old_idx) in indices.iter().enumerate() {
                            if (null_bytes[old_idx / 8] >> (old_idx % 8)) & 1 == 1 {
                                active_nulls[new_idx / 8] |= 1 << (new_idx % 8);
                            }
                        }
                        col_data.apply_null_bitmap(&active_nulls);
                    }
                } else if null_bytes.iter().any(|&b| b != 0) {
                    col_data.apply_null_bitmap(null_bytes);
                }

                columns[out_pos].1.append(&col_data);
            }
        }

        Ok(Some((doc_ids, columns)))
    }

    pub fn to_arrow_batch_mmap_range(
        &self,
        column_names: Option<&[&str]>,
        include_id: bool,
        row_offset: usize,
        row_limit: Option<usize>,
        dict_encode_strings: bool,
    ) -> io::Result<Option<RecordBatch>> {
        use arrow::array::{BooleanArray, Int64Array, PrimitiveArray, StringArray};
        use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
        use arrow::datatypes::{DataType as ArrowDataType, Field, Float64Type, Int64Type, Schema};
        use std::sync::Arc;

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None), // footer not yet written (empty/new file)
        };

        let schema = &footer.schema;
        let col_count = schema.column_count();

        // Determine which columns to read (indices into schema)
        let col_indices: Vec<usize> = if let Some(names) = column_names {
            names
                .iter()
                .filter(|&&n| n != "_id")
                .filter_map(|&name| schema.get_index(name))
                .collect()
        } else {
            (0..col_count).collect()
        };

        // Compute total active rows across all RGs
        let total_active: usize = footer
            .row_groups
            .iter()
            .map(|rg| rg.active_rows() as usize)
            .sum();

        if total_active == 0 {
            // Build empty schema and return empty batch
            let mut fields: Vec<Field> = Vec::new();
            if include_id {
                fields.push(Field::new("_id", ArrowDataType::Int64, false));
            }
            for &ci in &col_indices {
                let (name, ct) = &schema.columns[ci];
                let dt = match ct {
                    ColumnType::Int64
                    | ColumnType::Int8
                    | ColumnType::Int16
                    | ColumnType::Int32
                    | ColumnType::UInt8
                    | ColumnType::UInt16
                    | ColumnType::UInt32
                    | ColumnType::UInt64 => ArrowDataType::Int64,
                    ColumnType::Float64 | ColumnType::Float32 => ArrowDataType::Float64,
                    ColumnType::Bool => ArrowDataType::Boolean,
                    _ => ArrowDataType::Utf8,
                };
                fields.push(Field::new(name, dt, true));
            }
            let arrow_schema = Arc::new(Schema::new(fields));
            return Ok(Some(RecordBatch::new_empty(arrow_schema)));
        }

        let effective_start = row_offset.min(total_active);
        let effective_limit = row_limit
            .unwrap_or_else(|| total_active.saturating_sub(effective_start))
            .min(total_active.saturating_sub(effective_start));

        // Get mmap for the file
        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for mmap read"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        // Accumulators for each output column + _id
        let mut all_ids: Vec<i64> = Vec::with_capacity(effective_limit);
        // For each requested column, accumulate ColumnData across RGs
        let mut col_accumulators: Vec<ColumnData> = col_indices
            .iter()
            .map(|&ci| {
                let ct = schema.columns[ci].1;
                // StringDict is decoded to String before accumulation,
                // so accumulator must be String type
                let acc_type = if ct == ColumnType::StringDict {
                    ColumnType::String
                } else {
                    ct
                };
                ColumnData::new(acc_type)
            })
            .collect();
        let mut null_accumulators: Vec<Vec<bool>> = vec![Vec::new(); col_indices.len()];
        let mut rows_collected: usize = 0;
        let mut active_rows_seen: usize = 0;

        for (rg_idx, rg_meta) in footer.row_groups.iter().enumerate() {
            if rows_collected >= effective_limit {
                break;
            }
            if rg_meta.row_count == 0 {
                continue;
            }

            let rg_rows = rg_meta.row_count as usize;
            let rg_active = rg_meta.active_rows() as usize;
            if active_rows_seen + rg_active <= effective_start {
                active_rows_seen += rg_active;
                continue;
            }
            let active_skip = effective_start
                .saturating_sub(active_rows_seen)
                .min(rg_active);
            let rows_to_take = (effective_limit - rows_collected).min(rg_active - active_skip);
            if rows_to_take == 0 {
                active_rows_seen += rg_active;
                continue;
            }

            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("RG extends past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];

            // Check compression flag at RG header byte 28
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
            let has_deletes = rg_meta.deletion_count > 0;
            let null_bitmap_len = (rg_rows + 7) / 8;

            // === RCIX fast path: O(1) direct seeks for no-compression, no-deletes ===
            // Skips sequential column scanning — jumps directly to each column via footer index.
            // For LIMIT 100 with 65536-row RG: touches ~800B of IDs + targeted column pages
            // instead of scanning 512KB IDs + full column sequence.
            if compress_flag == RG_COMPRESS_NONE
                && !has_deletes
                && encoding_version >= 1
                && rg_idx < footer.col_offsets.len()
                && !footer.col_offsets[rg_idx].is_empty()
            {
                let rg_body_abs = (rg_meta.offset + 32) as usize;
                let col_offsets = &footer.col_offsets[rg_idx];

                // Read only the requested IDs; contiguous row groups reconstruct
                // them from min_id without touching a physical ID section.
                let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
                if id_encoding == RG_IDS_IMPLICIT_CONTIGUOUS {
                    all_ids.extend(
                        (0..rows_to_take)
                            .map(|i| (rg_meta.min_id + (active_skip + i) as u64) as i64),
                    );
                } else {
                    let id_start = rg_body_abs + active_skip * 8;
                    let id_end = id_start + rows_to_take * 8;
                    if id_end <= mmap_ref.len() {
                        let id_bytes = &mmap_ref[id_start..id_end];
                        for i in 0..rows_to_take {
                            let id = u64::from_le_bytes(
                                id_bytes[i * 8..(i + 1) * 8].try_into().unwrap(),
                            );
                            all_ids.push(id as i64);
                        }
                    }
                }

                // Direct column reads via RCIX — no sequential scan of preceding columns
                // OPTIMIZATION: parallelize column reads for large RGs with multiple columns
                if rows_to_take >= 50_000 && col_indices.len() >= 2 {
                    use rayon::prelude::*;
                    let create_default = Self::create_default_column;
                    let rg_col_results: Vec<io::Result<(ColumnData, Vec<bool>)>> = col_indices
                        .par_iter()
                        .map(|&col_idx| {
                            if col_idx >= col_offsets.len() {
                                let col_type = schema.columns[col_idx].1;
                                let default_col = create_default(col_type, rows_to_take);
                                let nulls = vec![true; rows_to_take];
                                return Ok((default_col, nulls));
                            }
                            let col_abs = rg_body_abs + col_offsets[col_idx] as usize;
                            if col_abs + null_bitmap_len > mmap_ref.len() {
                                let col_type = schema.columns[col_idx].1;
                                return Ok((
                                    create_default(col_type, rows_to_take),
                                    vec![true; rows_to_take],
                                ));
                            }
                            let null_bytes = &mmap_ref[col_abs..col_abs + null_bitmap_len];
                            let data_abs = col_abs + null_bitmap_len;
                            if data_abs >= mmap_ref.len() {
                                let col_type = schema.columns[col_idx].1;
                                return Ok((
                                    create_default(col_type, rows_to_take),
                                    vec![true; rows_to_take],
                                ));
                            }
                            let col_type = schema.columns[col_idx].1;
                            let (col_data, _) = if active_skip == 0 && rows_to_take < rg_rows {
                                read_column_encoded_partial(
                                    &mmap_ref[data_abs..],
                                    col_type,
                                    rows_to_take,
                                )?
                            } else {
                                read_column_encoded(&mmap_ref[data_abs..], col_type)?
                            };
                            let col_data = if matches!(&col_data, ColumnData::StringDict { .. }) {
                                col_data.decode_string_dict()
                            } else {
                                col_data
                            };
                            let col_data = if active_skip > 0 || rows_to_take < col_data.len() {
                                col_data.slice_range(active_skip, active_skip + rows_to_take)
                            } else {
                                col_data
                            };
                            let mut nulls = Vec::with_capacity(rows_to_take);
                            for i in 0..rows_to_take {
                                let row = active_skip + i;
                                nulls.push((null_bytes[row / 8] >> (row % 8)) & 1 == 1);
                            }
                            Ok((col_data, nulls))
                        })
                        .collect();
                    for (out_pos, result) in rg_col_results.into_iter().enumerate() {
                        let (col_data, nulls) = result?;
                        col_accumulators[out_pos].append(&col_data);
                        null_accumulators[out_pos].extend(nulls);
                    }
                } else {
                    for (out_pos, &col_idx) in col_indices.iter().enumerate() {
                        if col_idx >= col_offsets.len() {
                            let col_type = schema.columns[col_idx].1;
                            let default_col = Self::create_default_column(col_type, rows_to_take);
                            col_accumulators[out_pos].append(&default_col);
                            null_accumulators[out_pos]
                                .extend(std::iter::repeat(true).take(rows_to_take));
                            continue;
                        }
                        let col_abs = rg_body_abs + col_offsets[col_idx] as usize;
                        if col_abs + null_bitmap_len > mmap_ref.len() {
                            continue;
                        }
                        let null_bytes = &mmap_ref[col_abs..col_abs + null_bitmap_len];
                        let data_abs = col_abs + null_bitmap_len;
                        if data_abs >= mmap_ref.len() {
                            continue;
                        }
                        let col_type = schema.columns[col_idx].1;
                        let (col_data, _) = if active_skip == 0 && rows_to_take < rg_rows {
                            read_column_encoded_partial(
                                &mmap_ref[data_abs..],
                                col_type,
                                rows_to_take,
                            )?
                        } else {
                            read_column_encoded(&mmap_ref[data_abs..], col_type)?
                        };
                        let col_data = if matches!(&col_data, ColumnData::StringDict { .. }) {
                            col_data.decode_string_dict()
                        } else {
                            col_data
                        };
                        let col_data = if active_skip > 0 || rows_to_take < col_data.len() {
                            col_data.slice_range(active_skip, active_skip + rows_to_take)
                        } else {
                            col_data
                        };
                        col_accumulators[out_pos].append(&col_data);
                        for i in 0..rows_to_take {
                            let row = active_skip + i;
                            null_accumulators[out_pos]
                                .push((null_bytes[row / 8] >> (row % 8)) & 1 == 1);
                        }
                    }
                }

                rows_collected += rows_to_take;
                active_rows_seen += rg_active;
                continue; // skip sequential scan path below
            }
            // === End RCIX fast path ===

            // Get the body bytes (after 32-byte RG header), decompressing if needed
            let decompressed_buf = decompress_rg_body(compress_flag, &rg_bytes[32..])?;
            let body: &[u8] = decompressed_buf.as_deref().unwrap_or(&rg_bytes[32..]);
            let mut pos: usize = 0;

            // Read IDs
            let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
            let id_byte_len = rg_id_section_len(rg_rows, id_encoding);
            if pos + id_byte_len > body.len() {
                return Err(err_data("RG IDs truncated"));
            }
            pos += id_byte_len;

            // Read deletion vector
            let del_vec_len = (rg_rows + 7) / 8;
            if pos + del_vec_len > body.len() {
                return Err(err_data("RG deletion vector truncated"));
            }
            let del_bytes = &body[pos..pos + del_vec_len];
            pos += del_vec_len;

            // Always collect active IDs from this RG (needed for DeltaMerger overlay)
            {
                let mut skipped = 0usize;
                let mut taken = 0;
                for i in 0..rg_rows {
                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                        continue; // deleted
                    }
                    if skipped < active_skip {
                        skipped += 1;
                        continue;
                    }
                    let id = rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, i)
                        .ok_or_else(|| err_data("RG ID section truncated"))?;
                    all_ids.push(id as i64);
                    taken += 1;
                    if taken >= rows_to_take {
                        break;
                    }
                }
            }

            // Parse columns — read requested, skip others
            // Build mapping: disk col_idx → output position in col_accumulators
            // This ensures correct data placement regardless of column ordering
            // between the footer schema and the requested column list.
            let col_idx_to_out: HashMap<usize, usize> = col_indices
                .iter()
                .enumerate()
                .map(|(out_pos, &col_idx)| (col_idx, out_pos))
                .collect();
            // Track which output columns got data from this RG
            let mut rg_filled: Vec<bool> = vec![false; col_indices.len()];
            for col_idx in 0..col_count {
                // Schema evolution: RG may have fewer columns than footer schema.
                // If we've exhausted the RG data, remaining columns get defaults.
                if pos + null_bitmap_len > body.len() {
                    break;
                }
                let null_bytes = &body[pos..pos + null_bitmap_len];
                pos += null_bitmap_len;

                let col_type = schema.columns[col_idx].1;

                if let Some(&out_pos) = col_idx_to_out.get(&col_idx) {
                    // OPTIMIZATION: For LIMIT queries without deletes, use partial column read
                    // to avoid allocating/copying full column data (e.g., 1M rows → only 100)
                    if !has_deletes
                        && active_skip == 0
                        && rows_to_take < rg_rows
                        && encoding_version >= 1
                    {
                        let (col_data, consumed) =
                            read_column_encoded_partial(&body[pos..], col_type, rows_to_take)?;
                        pos += consumed;
                        let col_data = if matches!(&col_data, ColumnData::StringDict { .. }) {
                            col_data.decode_string_dict()
                        } else {
                            col_data
                        };
                        col_accumulators[out_pos].append(&col_data);
                        for i in 0..rows_to_take {
                            let is_null = (null_bytes[i / 8] >> (i % 8)) & 1 == 1;
                            null_accumulators[out_pos].push(is_null);
                        }
                    } else {
                        // Full column read path
                        let (col_data, consumed) = if encoding_version >= 1 {
                            read_column_encoded(&body[pos..], col_type)?
                        } else {
                            ColumnData::from_bytes_typed(&body[pos..], col_type)?
                        };
                        pos += consumed;

                        let col_data = if matches!(&col_data, ColumnData::StringDict { .. }) {
                            col_data.decode_string_dict()
                        } else {
                            col_data
                        };

                        if has_deletes {
                            let active_indices: Vec<usize> = (0..rg_rows)
                                .filter(|&i| (del_bytes[i / 8] >> (i % 8)) & 1 == 0)
                                .skip(active_skip)
                                .take(rows_to_take)
                                .collect();
                            let filtered = col_data.filter_by_indices(&active_indices);
                            col_accumulators[out_pos].append(&filtered);

                            for &old_idx in &active_indices {
                                let ob = old_idx / 8;
                                let obit = old_idx % 8;
                                let is_null =
                                    ob < null_bytes.len() && (null_bytes[ob] >> obit) & 1 == 1;
                                null_accumulators[out_pos].push(is_null);
                            }
                        } else {
                            if active_skip > 0 || rows_to_take < rg_rows {
                                let range_data =
                                    col_data.slice_range(active_skip, active_skip + rows_to_take);
                                col_accumulators[out_pos].append(&range_data);
                                for i in 0..rows_to_take {
                                    let row = active_skip + i;
                                    let is_null = (null_bytes[row / 8] >> (row % 8)) & 1 == 1;
                                    null_accumulators[out_pos].push(is_null);
                                }
                            } else {
                                col_accumulators[out_pos].append(&col_data);
                                for i in 0..rg_rows {
                                    let is_null = (null_bytes[i / 8] >> (i % 8)) & 1 == 1;
                                    null_accumulators[out_pos].push(is_null);
                                }
                            }
                        }
                    }
                    rg_filled[out_pos] = true;
                } else {
                    // Skip this column (no allocation, encoding-aware)
                    let consumed = if encoding_version >= 1 {
                        skip_column_encoded(&body[pos..], col_type)?
                    } else {
                        ColumnData::skip_bytes_typed(&body[pos..], col_type)?
                    };
                    pos += consumed;
                }
            }
            // Fill default values for columns that weren't in this RG (schema evolution)
            for (out_pos, filled) in rg_filled.iter().enumerate() {
                if !filled {
                    let col_type = schema.columns[col_indices[out_pos]].1;
                    let default_col = Self::create_default_column(col_type, rows_to_take);
                    col_accumulators[out_pos].append(&default_col);
                    // All rows are null for this missing column
                    null_accumulators[out_pos].extend(std::iter::repeat(true).take(rows_to_take));
                }
            }
            rows_collected += rows_to_take;
            active_rows_seen += rg_active;
        }

        drop(mmap_guard);
        drop(file_guard);

        // Build Arrow RecordBatch from accumulated data
        let active_count = rows_collected;
        let mut fields: Vec<Field> = Vec::with_capacity(col_indices.len() + 1);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_indices.len() + 1);

        // Save row IDs for potential DeltaMerger overlay
        let row_ids_for_delta: Vec<u64> = all_ids.iter().map(|&id| id as u64).collect();

        // _id column
        if include_id {
            fields.push(Field::new("_id", ArrowDataType::Int64, false));
            arrays.push(Arc::new(Int64Array::from(all_ids)));
        }

        // Data columns — parallel conversion for large tables
        use arrow::array::ArrayRef;
        let convert_mmap_column = |out_idx: usize,
                                   col_idx: usize|
         -> io::Result<(Field, ArrayRef)> {
            let (col_name, schema_col_type_ref) = &schema.columns[col_idx];
            let schema_col_type = *schema_col_type_ref;
            let col_data = &col_accumulators[out_idx];
            let null_bitmap = &null_accumulators[out_idx];

            // Build Arrow null buffer from per-bit bool accumulator
            let null_buf: Option<NullBuffer> =
                if !null_bitmap.is_empty() && null_bitmap.iter().any(|&b| b) {
                    let mut validity_bytes = vec![0xFFu8; (active_count + 7) / 8];
                    for (i, &is_null) in null_bitmap.iter().enumerate() {
                        if is_null {
                            // Clear validity bit (Arrow: 1=valid, 0=null)
                            validity_bytes[i / 8] &= !(1u8 << (i % 8));
                        }
                    }
                    Some(NullBuffer::new(BooleanBuffer::new(
                        Buffer::from(validity_bytes),
                        0,
                        active_count,
                    )))
                } else {
                    None
                };

            let (arrow_dt, array): (ArrowDataType, ArrayRef) = match col_data {
                ColumnData::Int64(values) => match schema_col_type {
                    ColumnType::Timestamp => {
                        use arrow::datatypes::TimestampMicrosecondType;
                        let arr = PrimitiveArray::<TimestampMicrosecondType>::new(
                            ScalarBuffer::from(values.clone()),
                            null_buf,
                        );
                        (
                            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                            Arc::new(arr) as ArrayRef,
                        )
                    }
                    ColumnType::Date => {
                        use arrow::datatypes::Date32Type;
                        let data_i32: Vec<i32> = values.iter().map(|&v| v as i32).collect();
                        let arr = PrimitiveArray::<Date32Type>::new(
                            ScalarBuffer::from(data_i32),
                            null_buf,
                        );
                        (ArrowDataType::Date32, Arc::new(arr) as ArrayRef)
                    }
                    _ => {
                        let arr = PrimitiveArray::<Int64Type>::new(
                            ScalarBuffer::from(values.clone()),
                            null_buf,
                        );
                        (ArrowDataType::Int64, Arc::new(arr) as ArrayRef)
                    }
                },
                ColumnData::Float64(values) => {
                    let arr = PrimitiveArray::<Float64Type>::new(
                        ScalarBuffer::from(values.clone()),
                        null_buf,
                    );
                    (ArrowDataType::Float64, Arc::new(arr) as ArrayRef)
                }
                ColumnData::String { offsets, data } => {
                    let count = offsets.len().saturating_sub(1);
                    if null_buf.is_some() {
                        let strings: Vec<Option<&str>> = (0..count.min(active_count))
                            .map(|i| {
                                if i < null_bitmap.len() && null_bitmap[i] {
                                    None
                                } else {
                                    let start = offsets[i] as usize;
                                    let end = offsets[i + 1] as usize;
                                    std::str::from_utf8(&data[start..end]).ok()
                                }
                            })
                            .collect();
                        if super::requires_large_arrow_offsets(data.len()) {
                            (
                                ArrowDataType::LargeUtf8,
                                Arc::new(arrow::array::LargeStringArray::from(strings)),
                            )
                        } else {
                            (ArrowDataType::Utf8, Arc::new(StringArray::from(strings)))
                        }
                    } else {
                        // OPTIMIZATION: build StringArray directly from u32 offsets + data bytes
                        let row_count = count.min(active_count);
                        let data_end = offsets[row_count] as usize;
                        // OPTIMIZATION: dictionary-encode low-cardinality string
                        // columns directly from the mmap byte ranges.  Downstream
                        // filters/joins/aggregations then hash the distinct values
                        // once instead of every row.
                        if dict_encode_strings
                            && row_count >= 100
                            && data_end <= i32::MAX as usize
                        {
                            let sample_size = row_count.min(1000);
                            let mut sample_unique: ahash::AHashSet<&[u8]> =
                                ahash::AHashSet::with_capacity(100);
                            let step = if row_count > sample_size {
                                row_count / sample_size
                            } else {
                                1
                            };
                            let mut si = 0usize;
                            while si < row_count && sample_unique.len() < 1000 {
                                sample_unique
                                    .insert(&data[offsets[si] as usize..offsets[si + 1] as usize]);
                                si += step;
                            }
                            if sample_unique.len() < 1000 {
                                use arrow::array::{DictionaryArray, UInt32Array};
                                use arrow::datatypes::UInt32Type;
                                let mut dict_map: ahash::AHashMap<&[u8], u32> =
                                    ahash::AHashMap::with_capacity(sample_unique.len());
                                let mut dict_strings: Vec<&str> =
                                    Vec::with_capacity(sample_unique.len());
                                let mut next_id = 0u32;
                                let mut keys: Vec<u32> = Vec::with_capacity(row_count);
                                for i in 0..row_count {
                                    let bytes = &data[offsets[i] as usize..offsets[i + 1] as usize];
                                    let id = *dict_map.entry(bytes).or_insert_with(|| {
                                        let id = next_id;
                                        next_id += 1;
                                        dict_strings.push(std::str::from_utf8(bytes).unwrap_or(""));
                                        id
                                    });
                                    keys.push(id);
                                }
                                let dict_array = DictionaryArray::<UInt32Type>::try_new(
                                    UInt32Array::from(keys),
                                    Arc::new(StringArray::from_iter_values(dict_strings)),
                                )
                                .map_err(|e| err_data(e.to_string()))?;
                                let arr_ref: ArrayRef = Arc::new(dict_array);
                                (arr_ref.data_type().clone(), arr_ref)
                            } else {
                                // High cardinality → zero-copy plain StringArray
                                // (same construction as the non-dict path).
                                let offsets_i32: Vec<i32> = offsets[..row_count + 1]
                                    .iter()
                                    .map(|&o| o as i32)
                                    .collect();
                                let offset_buf = unsafe {
                                    arrow::buffer::OffsetBuffer::new_unchecked(
                                        ScalarBuffer::from(offsets_i32),
                                    )
                                };
                                let data_buf = Buffer::from_slice_ref(&data[..data_end]);
                                (
                                    ArrowDataType::Utf8,
                                    Arc::new(unsafe {
                                        StringArray::new_unchecked(offset_buf, data_buf, None)
                                    }) as ArrayRef,
                                )
                            }
                        } else if super::requires_large_arrow_offsets(data_end) {
                            let strings = (0..row_count).map(|i| {
                                let start = offsets[i] as usize;
                                let end = offsets[i + 1] as usize;
                                std::str::from_utf8(&data[start..end]).unwrap_or("")
                            });
                            (
                                ArrowDataType::LargeUtf8,
                                Arc::new(arrow::array::LargeStringArray::from_iter_values(
                                    strings,
                                )) as ArrayRef,
                            )
                        } else {
                            let offsets_i32: Vec<i32> = offsets[..row_count + 1]
                                .iter()
                                .map(|&o| o as i32)
                                .collect();
                            let offset_buf = unsafe {
                                arrow::buffer::OffsetBuffer::new_unchecked(ScalarBuffer::from(
                                    offsets_i32,
                                ))
                            };
                            let data_buf = Buffer::from_slice_ref(&data[..data_end]);
                            (
                                ArrowDataType::Utf8,
                                Arc::new(unsafe {
                                    StringArray::new_unchecked(offset_buf, data_buf, None)
                                }) as ArrayRef,
                            )
                        }
                    }
                }
                ColumnData::Bool {
                    data: bool_data,
                    len: bool_len,
                } => {
                    let bools: Vec<Option<bool>> = (0..*bool_len.min(&active_count))
                        .map(|i| {
                            if null_buf.is_some() {
                                if i < null_bitmap.len() && null_bitmap[i] {
                                    return None;
                                }
                            }
                            let byte_idx = i / 8;
                            let bit_idx = i % 8;
                            let val = byte_idx < bool_data.len()
                                && (bool_data[byte_idx] >> bit_idx) & 1 == 1;
                            Some(val)
                        })
                        .collect();
                    (ArrowDataType::Boolean, Arc::new(BooleanArray::from(bools)))
                }
                ColumnData::Binary { offsets, data } => {
                    use arrow::array::{BinaryArray, LargeBinaryArray};
                    let count = offsets.len().saturating_sub(1);
                    if schema_col_type == ColumnType::Blob {
                        let values =
                            self.materialize_blob_column(offsets, data, null_bitmap, active_count)?;
                        let refs: Vec<Option<&[u8]>> =
                            values.iter().map(|v| v.as_deref()).collect();
                        (
                            ArrowDataType::LargeBinary,
                            Arc::new(LargeBinaryArray::from(refs)),
                        )
                    } else if null_buf.is_some() {
                        let bins: Vec<Option<&[u8]>> = (0..count.min(active_count))
                            .map(|i| {
                                if i < null_bitmap.len() && null_bitmap[i] {
                                    None
                                } else {
                                    let start = offsets[i] as usize;
                                    let end = offsets[i + 1] as usize;
                                    Some(&data[start..end])
                                }
                            })
                            .collect();
                        if super::requires_large_arrow_offsets(data.len()) {
                            (
                                ArrowDataType::LargeBinary,
                                Arc::new(LargeBinaryArray::from(bins)),
                            )
                        } else {
                            (ArrowDataType::Binary, Arc::new(BinaryArray::from(bins)))
                        }
                    } else {
                        let bins: Vec<&[u8]> = (0..count.min(active_count))
                            .map(|i| {
                                let start = offsets[i] as usize;
                                let end = offsets[i + 1] as usize;
                                &data[start..end]
                            })
                            .collect();
                        if super::requires_large_arrow_offsets(data.len()) {
                            (
                                ArrowDataType::LargeBinary,
                                Arc::new(LargeBinaryArray::from(bins)),
                            )
                        } else {
                            (ArrowDataType::Binary, Arc::new(BinaryArray::from(bins)))
                        }
                    }
                }
                ColumnData::FixedList { data, dim } => {
                    use arrow::array::{FixedSizeListArray, Float32Array};
                    let dim_usize = *dim as usize;
                    let row_count = if dim_usize == 0 {
                        0
                    } else {
                        data.len() / (dim_usize * 4)
                    }
                    .min(active_count);
                    let byte_len = row_count * dim_usize * 4;
                    let float_arr = Float32Array::from(
                        crate::storage::on_demand::f32_le_bytes_to_values(&data[..byte_len]),
                    );
                    let list_dt = ArrowDataType::FixedSizeList(
                        Arc::new(Field::new("item", ArrowDataType::Float32, false)),
                        dim_usize as i32,
                    );
                    let arr = FixedSizeListArray::new(
                        Arc::new(Field::new("item", ArrowDataType::Float32, false)),
                        dim_usize as i32,
                        Arc::new(float_arr),
                        None,
                    );
                    (list_dt, Arc::new(arr) as ArrayRef)
                }
                ColumnData::Float16List { data, dim } => {
                    use arrow::array::{FixedSizeListArray, Float32Array};
                    let dim_usize = *dim as usize;
                    let row_count = if dim_usize == 0 {
                        0
                    } else {
                        data.len() / (dim_usize * 2)
                    }
                    .min(active_count);
                    let mut f32_values: Vec<f32> = Vec::with_capacity(row_count * dim_usize);
                    for chunk in data[..row_count * dim_usize * 2].chunks_exact(2) {
                        let bits = u16::from_le_bytes(chunk.try_into().unwrap());
                        f32_values.push(crate::storage::on_demand::f16_to_f32(bits));
                    }
                    let float_arr = Float32Array::from(f32_values);
                    let list_dt = ArrowDataType::FixedSizeList(
                        Arc::new(Field::new("item", ArrowDataType::Float32, false)),
                        dim_usize as i32,
                    );
                    let arr = FixedSizeListArray::new(
                        Arc::new(Field::new("item", ArrowDataType::Float32, false)),
                        dim_usize as i32,
                        Arc::new(float_arr),
                        None,
                    );
                    (list_dt, Arc::new(arr) as ArrayRef)
                }
                ColumnData::QuantizedList { data, dim, codec } => {
                    use arrow::array::{FixedSizeListArray, Float32Array};
                    let dim_usize = *dim as usize;
                    let stride = codec.row_width(dim_usize)?;
                    let row_count = if stride == 0 { 0 } else { data.len() / stride }
                        .min(active_count);
                    let mut values = vec![0.0f32; row_count * dim_usize];
                    for row_idx in 0..row_count {
                        crate::compute::vector_quantization::decode_vector(
                            *codec,
                            &data[row_idx * stride..(row_idx + 1) * stride],
                            dim_usize,
                            &mut values[row_idx * dim_usize..(row_idx + 1) * dim_usize],
                        )?;
                    }
                    let item = Arc::new(Field::new("item", ArrowDataType::Float32, false));
                    let list_dt = ArrowDataType::FixedSizeList(item.clone(), dim_usize as i32);
                    let arr = FixedSizeListArray::new(
                        item,
                        dim_usize as i32,
                        Arc::new(Float32Array::from(values)),
                        null_buf,
                    );
                    (list_dt, Arc::new(arr) as ArrayRef)
                }
                _ => {
                    // Fallback: null array
                    let arr = arrow::array::new_null_array(&ArrowDataType::Utf8, active_count);
                    (ArrowDataType::Utf8, arr)
                }
            };

            Ok((Field::new(col_name, arrow_dt, true), array))
        };

        if active_count >= 50_000 && col_indices.len() >= 2 {
            use rayon::prelude::*;
            let results: Vec<io::Result<(Field, ArrayRef)>> = col_indices
                .iter()
                .enumerate()
                .collect::<Vec<_>>()
                .par_iter()
                .map(|&(out_idx, &col_idx)| convert_mmap_column(out_idx, col_idx))
                .collect();
            for r in results {
                let (f, a) = r?;
                fields.push(f);
                arrays.push(a);
            }
        } else {
            for (out_idx, &col_idx) in col_indices.iter().enumerate() {
                let (f, a) = convert_mmap_column(out_idx, col_idx)?;
                fields.push(f);
                arrays.push(a);
            }
        }

        let arrow_schema = Arc::new(Schema::new(fields));
        let batch =
            RecordBatch::try_new(arrow_schema, arrays).map_err(|e| err_data(e.to_string()))?;

        // Apply DeltaStore overlay if there are pending cell-level updates
        let ds = self.delta_store.read();
        if !ds.is_empty() && batch.num_rows() > 0 {
            let merged =
                crate::storage::delta::DeltaMerger::merge(&batch, &ds, &row_ids_for_delta)?;
            return Ok(Some(merged));
        }

        Ok(Some(batch))
    }

    pub(super) fn read_column_auto(
        &self,
        col_idx: usize,
        col_type: ColumnType,
        start: usize,
        count: usize,
        _total_rows: usize,
        _is_v4: bool,
    ) -> io::Result<ColumnData> {
        let columns = self.columns.read();
        if col_idx < columns.len() && columns[col_idx].len() > 0 {
            Ok(columns[col_idx].slice_range(start, start + count))
        } else {
            Ok(Self::create_default_column(col_type, count))
        }
    }

    pub(super) fn read_column_scattered_auto(
        &self,
        col_idx: usize,
        col_type: ColumnType,
        indices: &[usize],
        _total_rows: usize,
        _is_v4: bool,
    ) -> io::Result<ColumnData> {
        let columns = self.columns.read();
        if col_idx < columns.len() && columns[col_idx].len() > 0 {
            Ok(columns[col_idx].filter_by_indices(indices))
        } else {
            Ok(Self::create_default_column(col_type, indices.len()))
        }
    }

    pub(super) fn ensure_ids_loaded(&self) -> io::Result<()> {
        // Quick check without write lock
        if !self.ids.read().is_empty() {
            return Ok(());
        }

        let header = self.header.read();
        let id_count = header.row_count as usize;

        if id_count == 0 {
            return Ok(());
        }

        drop(header);
        self.ensure_ids_loaded_v4()
    }

    pub(super) fn ensure_ids_loaded_v4(&self) -> io::Result<()> {
        // Double-check under write lock
        let mut ids = self.ids.write();
        if !ids.is_empty() {
            return Ok(());
        }

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(()),
        };

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for V4 ID load"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let total_active: usize = footer
            .row_groups
            .iter()
            .map(|rg| rg.row_count as usize)
            .sum();
        ids.reserve(total_active);
        let mut deleted_acc: Vec<u8> = Vec::new();
        let mut max_id: u64 = 0;

        for rg_meta in &footer.row_groups {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }

            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("RG extends past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];

            // Check compression flag at RG header byte 28
            let compress_flag = if rg_bytes.len() >= 32 {
                rg_bytes[28]
            } else {
                RG_COMPRESS_NONE
            };

            // Get the body bytes (after 32-byte RG header), decompressing if needed
            let decompressed_buf = decompress_rg_body(compress_flag, &rg_bytes[32..])?;
            let body: &[u8] = decompressed_buf.as_deref().unwrap_or(&rg_bytes[32..]);
            let mut pos: usize = 0;

            // Read IDs
            let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
            let id_byte_len = rg_id_section_len(rg_rows, id_encoding);
            if pos + id_byte_len > body.len() {
                return Err(err_data("RG IDs truncated"));
            }
            pos += id_byte_len;

            for i in 0..rg_rows {
                let id = rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, i)
                    .ok_or_else(|| err_data("RG ID section truncated"))?;
                ids.push(id);
                if id > max_id {
                    max_id = id;
                }
            }

            // Read deletion vector
            let del_vec_len = (rg_rows + 7) / 8;
            if pos + del_vec_len > body.len() {
                return Err(err_data("RG deletion vector truncated"));
            }
            deleted_acc.extend_from_slice(&body[pos..pos + del_vec_len]);
        }

        drop(mmap_guard);
        drop(file_guard);

        // Update deletion bitmap — always overwrite on first load (ids were empty above).
        // The pre-allocated zeros in self.deleted must not shadow the real on-disk state.
        if !deleted_acc.is_empty() {
            *self.deleted.write() = deleted_acc;
        }

        // Update next_id
        let current_next = self.next_id.load(Ordering::SeqCst);
        if max_id + 1 > current_next {
            self.next_id.store(max_id + 1, Ordering::SeqCst);
        }

        Ok(())
    }

    pub(super) fn scan_columns_mmap(
        &self,
        col_indices: &[usize],
        footer: &V4Footer,
    ) -> io::Result<(Vec<ColumnData>, Vec<u8>)> {
        let (cols, del, _nulls) = self.scan_columns_mmap_with_nulls(col_indices, footer)?;
        Ok((cols, del))
    }

    pub(super) fn scan_columns_mmap_with_nulls(
        &self,
        col_indices: &[usize],
        footer: &V4Footer,
    ) -> io::Result<(Vec<ColumnData>, Vec<u8>, Vec<Vec<u8>>)> {
        let schema = &footer.schema;
        let col_count = schema.column_count();

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for mmap scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        // Pre-allocate accumulators to total row count — eliminates 16+ reallocations during RG iteration
        let total_rows: usize = footer
            .row_groups
            .iter()
            .map(|rg| rg.row_count as usize)
            .sum();
        let total_del_bytes = (total_rows + 7) / 8;
        let mut col_accumulators: Vec<ColumnData> = col_indices
            .iter()
            .map(|&ci| {
                let ct = schema.columns[ci].1;
                let acc_type = if ct == ColumnType::StringDict {
                    ColumnType::String
                } else {
                    ct
                };
                ColumnData::new(acc_type)
            })
            .collect();
        let mut all_del_bytes: Vec<u8> = Vec::with_capacity(total_del_bytes);
        let mut col_null_bitmaps: Vec<Vec<u8>> = col_indices
            .iter()
            .map(|_| Vec::with_capacity(total_del_bytes))
            .collect();

        let col_idx_to_out: HashMap<usize, usize> = col_indices
            .iter()
            .enumerate()
            .map(|(out_pos, &col_idx)| (col_idx, out_pos))
            .collect();

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            // Track which requested columns actually have data in this RG, so
            // schema-evolution columns (footer-only ADD COLUMN) can be padded
            // with all-NULL values below.
            let mut rg_filled: Vec<bool> = vec![false; col_indices.len()];

            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("RG extends past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];

            // Check compression flag at RG header byte 28, encoding version at byte 29
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

            let null_bitmap_len = (rg_rows + 7) / 8;
            let del_vec_len = (rg_rows + 7) / 8;

            // RCIX FAST PATH: uncompressed + RCIX available → jump directly to each column
            // Eliminates sequential skip of unneeded columns (O(1) per column seek).
            let rcix = footer.col_offsets.get(rg_i).filter(|v| !v.is_empty());
            if compress_flag == RG_COMPRESS_NONE && encoding_version >= 1 && rcix.is_some() {
                let body = &rg_bytes[32..];
                let rg_col_offsets = rcix.unwrap();

                // Deletion vector starts after IDs
                let del_start =
                    rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));
                if del_start + del_vec_len <= body.len() {
                    all_del_bytes.extend_from_slice(&body[del_start..del_start + del_vec_len]);
                }

                // Read only requested columns using RCIX offsets
                for (&col_idx, &out_pos) in &col_idx_to_out {
                    if col_idx >= rg_col_offsets.len() {
                        continue;
                    }
                    let col_start = rg_col_offsets[col_idx] as usize;
                    if col_start + null_bitmap_len > body.len() {
                        continue;
                    }

                    col_null_bitmaps[out_pos]
                        .extend_from_slice(&body[col_start..col_start + null_bitmap_len]);

                    let data_start = col_start + null_bitmap_len;
                    if data_start > body.len() {
                        continue;
                    }
                    let col_type = schema.columns[col_idx].1;
                    let (col_data, _) = read_column_encoded(&body[data_start..], col_type)?;
                    let col_data = if matches!(&col_data, ColumnData::StringDict { .. }) {
                        col_data.decode_string_dict()
                    } else {
                        col_data
                    };
                    col_accumulators[out_pos].append(&col_data);
                    rg_filled[out_pos] = true;
                }
                Self::pad_missing_rg_columns(
                    schema,
                    col_indices,
                    &rg_filled,
                    rg_rows,
                    null_bitmap_len,
                    &mut col_accumulators,
                    &mut col_null_bitmaps,
                );
                continue; // Skip sequential path
            }

            // SEQUENTIAL PATH: compressed or no RCIX — decompress and scan all columns
            let decompressed_buf = decompress_rg_body(compress_flag, &rg_bytes[32..])?;
            let body: &[u8] = decompressed_buf.as_deref().unwrap_or(&rg_bytes[32..]);
            let mut pos: usize = 0;

            // Skip IDs
            pos += rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));

            // Read deletion vector
            if pos + del_vec_len > body.len() {
                return Err(err_data("RG deletion vector truncated"));
            }
            all_del_bytes.extend_from_slice(&body[pos..pos + del_vec_len]);
            pos += del_vec_len;

            // Parse columns — read requested, skip others
            for col_idx in 0..col_count {
                if pos + null_bitmap_len > body.len() {
                    break;
                }
                let null_bytes = &body[pos..pos + null_bitmap_len];
                pos += null_bitmap_len;

                let col_type = schema.columns[col_idx].1;

                if let Some(&out_pos) = col_idx_to_out.get(&col_idx) {
                    col_null_bitmaps[out_pos].extend_from_slice(null_bytes);

                    let (col_data, consumed) = if encoding_version >= 1 {
                        read_column_encoded(&body[pos..], col_type)?
                    } else {
                        ColumnData::from_bytes_typed(&body[pos..], col_type)?
                    };
                    pos += consumed;
                    let col_data = if matches!(&col_data, ColumnData::StringDict { .. }) {
                        col_data.decode_string_dict()
                    } else {
                        col_data
                    };
                    col_accumulators[out_pos].append(&col_data);
                    rg_filled[out_pos] = true;
                } else {
                    let consumed = if encoding_version >= 1 {
                        skip_column_encoded(&body[pos..], col_type)?
                    } else {
                        ColumnData::skip_bytes_typed(&body[pos..], col_type)?
                    };
                    pos += consumed;
                }
            }
            Self::pad_missing_rg_columns(
                schema,
                col_indices,
                &rg_filled,
                rg_rows,
                null_bitmap_len,
                &mut col_accumulators,
                &mut col_null_bitmaps,
            );
        }

        Ok((col_accumulators, all_del_bytes, col_null_bitmaps))
    }

    /// Append all-NULL default values for requested columns that have no data
    /// in the current Row Group (footer-only ALTER TABLE ADD COLUMN).
    fn pad_missing_rg_columns(
        schema: &OnDemandSchema,
        col_indices: &[usize],
        rg_filled: &[bool],
        rg_rows: usize,
        null_bitmap_len: usize,
        col_accumulators: &mut [ColumnData],
        col_null_bitmaps: &mut [Vec<u8>],
    ) {
        for (out_pos, filled) in rg_filled.iter().enumerate() {
            if !filled {
                let col_idx = col_indices[out_pos];
                let ct = schema.columns[col_idx].1;
                col_accumulators[out_pos].append(&Self::create_default_column(ct, rg_rows));
                // Only the first rg_rows bits are valid; leave trailing bits
                // clear so later Row Groups' rows are not marked NULL.
                let mut pad = vec![0u8; null_bitmap_len];
                for i in 0..rg_rows {
                    pad[i / 8] |= 1 << (i % 8);
                }
                col_null_bitmaps[out_pos].extend_from_slice(&pad);
            }
        }
    }

    pub(super) fn zone_map_prune_rgs(
        footer: &V4Footer,
        filter_col_idx: usize,
        filter_op: &str,
        filter_value: f64,
    ) -> HashSet<usize> {
        let mut skip: HashSet<usize> = HashSet::new();
        let filter_val_i64 = filter_value as i64;
        for (rg_idx, rg_zmaps) in footer.zone_maps.iter().enumerate() {
            for zm in rg_zmaps {
                if zm.col_idx as usize != filter_col_idx {
                    continue;
                }
                let dominated = if zm.is_float {
                    let mn = f64::from_bits(zm.min_bits as u64);
                    let mx = f64::from_bits(zm.max_bits as u64);
                    match filter_op {
                        ">" => mx <= filter_value,
                        ">=" => mx < filter_value,
                        "<" => mn >= filter_value,
                        "<=" => mn > filter_value,
                        "=" | "==" => filter_value < mn || filter_value > mx,
                        _ => false,
                    }
                } else {
                    let mn = zm.min_bits;
                    let mx = zm.max_bits;
                    match filter_op {
                        ">" => mx <= filter_val_i64,
                        ">=" => mx < filter_val_i64,
                        "<" => mn >= filter_val_i64,
                        "<=" => mn > filter_val_i64,
                        "=" | "==" => filter_val_i64 < mn || filter_val_i64 > mx,
                        "!=" | "<>" => mn == mx && mn == filter_val_i64,
                        _ => false,
                    }
                };
                if dominated {
                    skip.insert(rg_idx);
                }
                break; // only one zone map per column per RG
            }
        }
        skip
    }

    pub(super) fn scan_columns_mmap_skip_rgs(
        &self,
        col_indices: &[usize],
        footer: &V4Footer,
        skip_rgs: &HashSet<usize>,
    ) -> io::Result<(Vec<ColumnData>, Vec<u8>)> {
        if skip_rgs.is_empty() {
            // No pruning needed — delegate to normal scan
            return self.scan_columns_mmap(col_indices, footer);
        }

        let schema = &footer.schema;
        let col_count = schema.column_count();

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for mmap scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let mut col_accumulators: Vec<ColumnData> = col_indices
            .iter()
            .map(|&ci| {
                let ct = schema.columns[ci].1;
                let acc_type = if ct == ColumnType::StringDict {
                    ColumnType::String
                } else {
                    ct
                };
                ColumnData::new(acc_type)
            })
            .collect();
        let mut all_del_bytes: Vec<u8> = Vec::new();

        let col_idx_to_out: HashMap<usize, usize> = col_indices
            .iter()
            .enumerate()
            .map(|(out_pos, &col_idx)| (col_idx, out_pos))
            .collect();

        for (rg_idx, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            if skip_rgs.contains(&rg_idx) {
                continue;
            } // Zone map pruned!

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
            let decompressed_buf = decompress_rg_body(compress_flag, &rg_bytes[32..])?;
            let body: &[u8] = decompressed_buf.as_deref().unwrap_or(&rg_bytes[32..]);
            let mut pos: usize = 0;

            pos += rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));

            let del_vec_len = (rg_rows + 7) / 8;
            if pos + del_vec_len > body.len() {
                return Err(err_data("RG deletion vector truncated"));
            }
            all_del_bytes.extend_from_slice(&body[pos..pos + del_vec_len]);
            pos += del_vec_len;

            let null_bitmap_len = (rg_rows + 7) / 8;
            for col_idx in 0..col_count {
                if pos + null_bitmap_len > body.len() {
                    break;
                }
                pos += null_bitmap_len; // skip null bitmap

                let col_type = schema.columns[col_idx].1;
                if let Some(&out_pos) = col_idx_to_out.get(&col_idx) {
                    let (col_data, consumed) = if encoding_version >= 1 {
                        read_column_encoded(&body[pos..], col_type)?
                    } else {
                        ColumnData::from_bytes_typed(&body[pos..], col_type)?
                    };
                    pos += consumed;
                    let col_data = if matches!(&col_data, ColumnData::StringDict { .. }) {
                        col_data.decode_string_dict()
                    } else {
                        col_data
                    };
                    col_accumulators[out_pos].append(&col_data);
                } else {
                    let consumed = if encoding_version >= 1 {
                        skip_column_encoded(&body[pos..], col_type)?
                    } else {
                        ColumnData::skip_bytes_typed(&body[pos..], col_type)?
                    };
                    pos += consumed;
                }
            }
        }

        Ok((col_accumulators, all_del_bytes))
    }

    pub fn read_ids(&self, start_row: usize, row_count: Option<usize>) -> io::Result<Vec<u64>> {
        // Ensure IDs are loaded (lazy loading)
        self.ensure_ids_loaded()?;

        let ids = self.ids.read();
        let base_total = ids.len();
        let delta_rows = self.delta_row_count();
        let total = base_total + delta_rows;
        let start = start_row.min(total);
        let count = row_count
            .map(|c| c.min(total - start))
            .unwrap_or(total - start);
        let end = start + count;

        let mut result = Vec::with_capacity(count);
        if start < base_total {
            let base_end = end.min(base_total);
            result.extend_from_slice(&ids[start..base_end]);
        }
        drop(ids);

        if end > base_total {
            if let Some((delta_ids, _)) = self.read_delta_data()? {
                let delta_start = start.saturating_sub(base_total);
                let delta_end = end.saturating_sub(base_total).min(delta_ids.len());
                if delta_start < delta_end {
                    result.extend_from_slice(&delta_ids[delta_start..delta_end]);
                }
            }
        }

        Ok(result)
    }

    pub fn read_ids_by_indices(&self, row_indices: &[usize]) -> io::Result<Vec<i64>> {
        // Ensure IDs are loaded (lazy loading)
        self.ensure_ids_loaded()?;

        let ids = self.ids.read();
        let total = ids.len();
        Ok(row_indices
            .iter()
            .map(|&i| if i < total { ids[i] as i64 } else { 0 })
            .collect())
    }

    pub(super) fn ensure_id_index(&self) {
        // First ensure IDs are loaded (since we lazy-load them now)
        let _ = self.ensure_ids_loaded();

        let mut id_to_idx = self.id_to_idx.write();
        if id_to_idx.is_none() {
            let ids = self.ids.read();
            let mut map = ahash::AHashMap::with_capacity(ids.len());
            for (idx, &id) in ids.iter().enumerate() {
                map.insert(id, idx);
            }
            *id_to_idx = Some(map);
        }
    }

    pub(super) fn read_column_range_mmap(
        &self,
        mmap_cache: &mut MmapCache,
        file: &File,
        index: &ColumnIndexEntry,
        dtype: ColumnType,
        start_row: usize,
        row_count: usize,
        _total_rows: usize,
    ) -> io::Result<ColumnData> {
        // ColumnData format has an 8-byte count header for all types
        // Format: [count:u64][data...]
        const HEADER_SIZE: u64 = 8;

        match dtype {
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
                // Format: [count:u64][values:i64*]
                // Zero-copy optimization: read directly into i64 buffer
                let byte_offset = HEADER_SIZE + (start_row * 8) as u64;

                let mut values: Vec<i64> = vec![0i64; row_count];
                // SAFETY: i64 has the same memory layout as [u8; 8] on little-endian systems
                // We read directly into the Vec's backing memory to avoid byte-by-byte parsing
                let bytes_slice = unsafe {
                    std::slice::from_raw_parts_mut(values.as_mut_ptr() as *mut u8, row_count * 8)
                };
                mmap_cache.read_at(file, bytes_slice, index.data_offset + byte_offset)?;

                // Handle endianness: convert from LE if on BE system
                #[cfg(target_endian = "big")]
                for v in &mut values {
                    *v = i64::from_le(*v);
                }

                Ok(ColumnData::Int64(values))
            }
            ColumnType::Float64 | ColumnType::Float32 => {
                // Format: [count:u64][values:f64*]
                // Zero-copy optimization: read directly into f64 buffer
                let byte_offset = HEADER_SIZE + (start_row * 8) as u64;

                let mut values: Vec<f64> = vec![0f64; row_count];
                // SAFETY: f64 has the same memory layout as [u8; 8] on little-endian systems
                let bytes_slice = unsafe {
                    std::slice::from_raw_parts_mut(values.as_mut_ptr() as *mut u8, row_count * 8)
                };
                mmap_cache.read_at(file, bytes_slice, index.data_offset + byte_offset)?;

                // Handle endianness: convert from LE if on BE system
                #[cfg(target_endian = "big")]
                for v in &mut values {
                    *v = f64::from_le_bytes(v.to_ne_bytes());
                }

                Ok(ColumnData::Float64(values))
            }
            ColumnType::Bool => {
                // Format: [len:u64][packed_bits...]
                let start_byte = start_row / 8;
                let end_byte = (start_row + row_count + 7) / 8;
                let byte_count = end_byte - start_byte;

                let mut packed = vec![0u8; byte_count];
                mmap_cache.read_at(
                    file,
                    &mut packed,
                    index.data_offset + HEADER_SIZE + start_byte as u64,
                )?;

                Ok(ColumnData::Bool {
                    data: packed,
                    len: row_count,
                })
            }
            ColumnType::String | ColumnType::Binary | ColumnType::Blob => {
                // Variable-length type: need to read offsets first
                self.read_variable_column_range_mmap(
                    mmap_cache, file, index, dtype, start_row, row_count,
                )
            }
            ColumnType::StringDict => {
                // Native dictionary-encoded string reading
                self.read_string_dict_column_range_mmap(
                    mmap_cache, file, index, start_row, row_count,
                )
            }
            ColumnType::FixedList => {
                let mut dim_buf = [0u8; 4];
                let _ = mmap_cache.read_at(file, &mut dim_buf, index.data_offset + 8);
                let dim = u32::from_le_bytes(dim_buf);
                let dim_usize = dim as usize;
                let byte_len = row_count * dim_usize * 4;
                let byte_offset = index.data_offset + 12 + (start_row * dim_usize * 4) as u64;
                let mut data = vec![0u8; byte_len];
                if byte_len > 0 {
                    mmap_cache.read_at(file, &mut data, byte_offset)?;
                }
                Ok(ColumnData::FixedList { data, dim })
            }
            ColumnType::Float16List => {
                let mut dim_buf = [0u8; 4];
                let _ = mmap_cache.read_at(file, &mut dim_buf, index.data_offset + 8);
                let dim = u32::from_le_bytes(dim_buf);
                let dim_usize = dim as usize;
                let byte_len = row_count * dim_usize * 2;
                let byte_offset = index.data_offset + 12 + (start_row * dim_usize * 2) as u64;
                let mut data = vec![0u8; byte_len];
                if byte_len > 0 {
                    mmap_cache.read_at(file, &mut data, byte_offset)?;
                }
                Ok(ColumnData::Float16List { data, dim })
            }
            dtype @ (ColumnType::BFloat16List
            | ColumnType::Int8Vector
            | ColumnType::UInt8Vector
            | ColumnType::Bit1Vector
            | ColumnType::TurboQuant2Vector
            | ColumnType::TurboQuant3Vector
            | ColumnType::TurboQuant4Vector) => {
                let mut dim_buf = [0u8; 4];
                mmap_cache.read_at(file, &mut dim_buf, index.data_offset + 8)?;
                let dim = u32::from_le_bytes(dim_buf);
                let codec = dtype.vector_codec().unwrap();
                let stride = codec.row_width(dim as usize)?;
                let byte_len = row_count.checked_mul(stride)
                    .ok_or_else(|| err_data("quantized vector range length overflow"))?;
                let mut data = vec![0u8; byte_len];
                if byte_len > 0 {
                    mmap_cache.read_at(file, &mut data, index.data_offset + 12 + (start_row * stride) as u64)?;
                }
                Ok(ColumnData::QuantizedList { data, dim, codec })
            }
            ColumnType::Null => Ok(ColumnData::Int64(vec![0; row_count])),
        }
    }

    pub(super) fn read_variable_column_range_mmap(
        &self,
        mmap_cache: &mut MmapCache,
        file: &File,
        index: &ColumnIndexEntry,
        dtype: ColumnType,
        start_row: usize,
        row_count: usize,
    ) -> io::Result<ColumnData> {
        // Variable-length format: [count:u64][offsets:u32*][data_len:u64][data:bytes]
        // Read header to get total count
        let mut header_buf = [0u8; 8];
        mmap_cache.read_at(file, &mut header_buf, index.data_offset)?;
        let total_count = u64::from_le_bytes(header_buf) as usize;

        if start_row >= total_count {
            return Ok(ColumnData::String {
                offsets: vec![0u64],
                data: Vec::new(),
            });
        }

        let actual_count = row_count.min(total_count - start_row);

        // OPTIMIZATION: Read offsets directly into u32 Vec using bulk read
        let offset_start = 8 + start_row * 4; // skip count header
        let offset_count = actual_count + 1;
        let mut disk_offsets: Vec<u32> = vec![0u32; offset_count];
        // SAFETY: u32 slice can be safely viewed as bytes for reading
        let offset_bytes = unsafe {
            std::slice::from_raw_parts_mut(disk_offsets.as_mut_ptr() as *mut u8, offset_count * 4)
        };
        mmap_cache.read_at(file, offset_bytes, index.data_offset + offset_start as u64)?;

        // Handle endianness on big-endian systems
        #[cfg(target_endian = "big")]
        for off in &mut disk_offsets {
            *off = u32::from_le(*off);
        }

        let mut offsets: Vec<u64> = disk_offsets.iter().map(|&o| o as u64).collect();

        // Calculate data range
        let data_start = offsets[0];
        let data_end = offsets[actual_count];
        let data_len = (data_end - data_start) as usize;

        // Read data portion
        // Data starts after: 8 (count) + (total_count+1)*4 (offsets) + 8 (data_len)
        let data_offset_in_file =
            index.data_offset + 8 + (total_count + 1) as u64 * 4 + 8 + data_start as u64;
        let mut data = vec![0u8; data_len];
        if data_len > 0 {
            mmap_cache.read_at(file, &mut data, data_offset_in_file)?;
        }

        // Normalize offsets to start at 0 using SIMD-friendly subtraction
        let base = offsets[0];
        if base != 0 {
            for off in &mut offsets {
                *off -= base;
            }
        }

        match dtype {
            ColumnType::String => Ok(ColumnData::String { offsets, data }),
            ColumnType::Binary | ColumnType::Blob => Ok(ColumnData::Binary { offsets, data }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Not a variable type",
            )),
        }
    }

    pub(super) fn read_string_dict_column_range_mmap(
        &self,
        mmap_cache: &mut MmapCache,
        file: &File,
        index: &ColumnIndexEntry,
        start_row: usize,
        row_count: usize,
    ) -> io::Result<ColumnData> {
        let base_offset = index.data_offset;

        // Read header: [row_count:u64][dict_size:u64]
        let mut header = [0u8; 16];
        mmap_cache.read_at(file, &mut header, base_offset)?;
        let total_rows = u64::from_le_bytes(header[0..8].try_into().unwrap()) as usize;
        let dict_size = u64::from_le_bytes(header[8..16].try_into().unwrap()) as usize;

        if start_row >= total_rows {
            return Ok(ColumnData::StringDict {
                indices: Vec::new(),
                dict_offsets: vec![0],
                dict_data: Vec::new(),
            });
        }

        let actual_count = row_count.min(total_rows - start_row);

        // OPTIMIZATION: Read indices directly into Vec<u32>
        let indices_offset = base_offset + 16 + (start_row * 4) as u64;
        let mut indices: Vec<u32> = vec![0u32; actual_count];
        let indices_bytes = unsafe {
            std::slice::from_raw_parts_mut(indices.as_mut_ptr() as *mut u8, actual_count * 4)
        };
        mmap_cache.read_at(file, indices_bytes, indices_offset)?;

        #[cfg(target_endian = "big")]
        for idx in &mut indices {
            *idx = u32::from_le(*idx);
        }

        // OPTIMIZATION: Read dict_offsets directly into Vec<u32>
        let dict_offsets_offset = base_offset + 16 + (total_rows * 4) as u64;
        let mut disk_dict_offsets: Vec<u32> = vec![0u32; dict_size];
        let dict_offsets_bytes = unsafe {
            std::slice::from_raw_parts_mut(
                disk_dict_offsets.as_mut_ptr() as *mut u8,
                dict_size * 4,
            )
        };
        mmap_cache.read_at(file, dict_offsets_bytes, dict_offsets_offset)?;

        #[cfg(target_endian = "big")]
        for off in &mut disk_dict_offsets {
            *off = u32::from_le(*off);
        }
        let dict_offsets: Vec<u64> = disk_dict_offsets.iter().map(|&o| o as u64).collect();

        // Read dict_data_len and dict_data
        let dict_data_len_offset = dict_offsets_offset + (dict_size * 4) as u64;
        let mut data_len_buf = [0u8; 8];
        mmap_cache.read_at(file, &mut data_len_buf, dict_data_len_offset)?;
        let dict_data_len = u64::from_le_bytes(data_len_buf) as usize;

        let dict_data_offset = dict_data_len_offset + 8;
        let mut dict_data = vec![0u8; dict_data_len];
        if dict_data_len > 0 {
            mmap_cache.read_at(file, &mut dict_data, dict_data_offset)?;
        }

        Ok(ColumnData::StringDict {
            indices,
            dict_offsets,
            dict_data,
        })
    }

    pub(super) fn read_string_dict_column_scattered_mmap(
        &self,
        mmap_cache: &mut MmapCache,
        file: &File,
        index: &ColumnIndexEntry,
        row_indices: &[usize],
    ) -> io::Result<ColumnData> {
        if row_indices.is_empty() {
            return Ok(ColumnData::StringDict {
                indices: Vec::new(),
                dict_offsets: vec![0],
                dict_data: Vec::new(),
            });
        }

        let base_offset = index.data_offset;

        // Read header: [row_count:u64][dict_size:u64]
        let mut header = [0u8; 16];
        mmap_cache.read_at(file, &mut header, base_offset)?;
        let total_rows = u64::from_le_bytes(header[0..8].try_into().unwrap()) as usize;
        let dict_size = u64::from_le_bytes(header[8..16].try_into().unwrap()) as usize;

        let all_indices_offset = base_offset + 16;
        let n = row_indices.len();

        // OPTIMIZED: Read only the specific indices we need instead of all indices
        // For small scattered reads, read individually; for dense reads, read a range
        let mut indices = Vec::with_capacity(n);

        if n <= 256 {
            // Small number of indices - read each one individually using thread-local buffer
            SCATTERED_READ_BUF.with(|buf| {
                let mut buf = buf.borrow_mut();
                buf.resize(4, 0);
                for &row_idx in row_indices {
                    if row_idx < total_rows {
                        mmap_cache.read_at(
                            file,
                            &mut buf[..4],
                            all_indices_offset + (row_idx * 4) as u64,
                        )?;
                        indices.push(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]));
                    } else {
                        indices.push(0);
                    }
                }
                Ok::<(), io::Error>(())
            })?;
        } else {
            // For larger reads, find min/max and read that range
            let min_idx = *row_indices.iter().min().unwrap_or(&0);
            let max_idx = *row_indices.iter().max().unwrap_or(&0);
            let range_size = max_idx - min_idx + 1;

            // OPTIMIZATION: If range is reasonably dense, read whole range as Vec<u32>
            if range_size <= n * 4 && range_size <= total_rows {
                let mut range_values: Vec<u32> = vec![0u32; range_size];
                let range_bytes = unsafe {
                    std::slice::from_raw_parts_mut(
                        range_values.as_mut_ptr() as *mut u8,
                        range_size * 4,
                    )
                };
                mmap_cache.read_at(file, range_bytes, all_indices_offset + (min_idx * 4) as u64)?;

                #[cfg(target_endian = "big")]
                for v in &mut range_values {
                    *v = u32::from_le(*v);
                }

                for &row_idx in row_indices {
                    if row_idx < total_rows {
                        let local_idx = row_idx - min_idx;
                        indices.push(range_values[local_idx]);
                    } else {
                        indices.push(0);
                    }
                }
            } else {
                // Sparse - read individually using thread-local buffer
                SCATTERED_READ_BUF.with(|buf| {
                    let mut buf = buf.borrow_mut();
                    buf.resize(4, 0);
                    for &row_idx in row_indices {
                        if row_idx < total_rows {
                            mmap_cache.read_at(
                                file,
                                &mut buf[..4],
                                all_indices_offset + (row_idx * 4) as u64,
                            )?;
                            indices.push(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]));
                        } else {
                            indices.push(0);
                        }
                    }
                    Ok::<(), io::Error>(())
                })?;
            }
        }

        // OPTIMIZATION: Read dict_offsets directly into Vec<u32>
        let dict_offsets_offset = base_offset + 16 + (total_rows * 4) as u64;
        let mut disk_dict_offsets: Vec<u32> = vec![0u32; dict_size];
        let dict_offsets_bytes = unsafe {
            std::slice::from_raw_parts_mut(
                disk_dict_offsets.as_mut_ptr() as *mut u8,
                dict_size * 4,
            )
        };
        mmap_cache.read_at(file, dict_offsets_bytes, dict_offsets_offset)?;

        #[cfg(target_endian = "big")]
        for off in &mut disk_dict_offsets {
            *off = u32::from_le(*off);
        }
        let dict_offsets: Vec<u64> = disk_dict_offsets.iter().map(|&o| o as u64).collect();

        // Read dict_data_len and dict_data
        let dict_data_len_offset = dict_offsets_offset + (dict_size * 4) as u64;
        let mut data_len_buf = [0u8; 8];
        mmap_cache.read_at(file, &mut data_len_buf, dict_data_len_offset)?;
        let dict_data_len = u64::from_le_bytes(data_len_buf) as usize;

        let dict_data_offset = dict_data_len_offset + 8;
        let mut dict_data = vec![0u8; dict_data_len];
        if dict_data_len > 0 {
            mmap_cache.read_at(file, &mut dict_data, dict_data_offset)?;
        }

        Ok(ColumnData::StringDict {
            indices,
            dict_offsets,
            dict_data,
        })
    }

    pub(super) fn read_variable_column_scattered_mmap(
        &self,
        mmap_cache: &mut MmapCache,
        file: &File,
        index: &ColumnIndexEntry,
        dtype: ColumnType,
        row_indices: &[usize],
    ) -> io::Result<ColumnData> {
        if row_indices.is_empty() {
            return Ok(match dtype {
                ColumnType::String => ColumnData::String {
                    offsets: vec![0],
                    data: Vec::new(),
                },
                _ => ColumnData::Binary {
                    offsets: vec![0],
                    data: Vec::new(),
                },
            });
        }

        // Variable-length format: [count:u64][offsets:u32*(count+1)][data_len:u64][data:bytes]
        // Read header to get total count
        let mut header_buf = [0u8; 8];
        mmap_cache.read_at(file, &mut header_buf, index.data_offset)?;
        let total_count = u64::from_le_bytes(header_buf) as usize;

        // Read only the offsets we need (need idx and idx+1 for each row)
        // Collect unique offset indices needed
        let mut offset_indices: Vec<usize> = Vec::with_capacity(row_indices.len() * 2);
        for &idx in row_indices {
            if idx < total_count {
                offset_indices.push(idx);
                offset_indices.push(idx + 1);
            }
        }
        offset_indices.sort_unstable();
        offset_indices.dedup();

        if offset_indices.is_empty() {
            return Ok(match dtype {
                ColumnType::String => ColumnData::String {
                    offsets: vec![0],
                    data: Vec::new(),
                },
                _ => ColumnData::Binary {
                    offsets: vec![0],
                    data: Vec::new(),
                },
            });
        }

        // Read required offsets in batches (optimize for contiguous ranges)
        let mut offset_map: HashMap<usize, u32> = HashMap::with_capacity(offset_indices.len());
        let offset_base = index.data_offset + 8; // skip count header

        // For small number of indices, read individually
        // For larger sets, read a range that covers all needed offsets
        let min_idx = *offset_indices.first().unwrap();
        let max_idx = *offset_indices.last().unwrap();

        if max_idx - min_idx < offset_indices.len() * 4 {
            // Indices are sparse enough - read range
            let range_count = max_idx - min_idx + 1;
            let mut offset_buf = vec![0u8; range_count * 4];
            mmap_cache.read_at(file, &mut offset_buf, offset_base + (min_idx * 4) as u64)?;

            for &idx in &offset_indices {
                let local_idx = idx - min_idx;
                let off = u32::from_le_bytes(
                    offset_buf[local_idx * 4..(local_idx + 1) * 4]
                        .try_into()
                        .unwrap(),
                );
                offset_map.insert(idx, off);
            }
        } else {
            // Very sparse - read individually
            let mut buf = [0u8; 4];
            for &idx in &offset_indices {
                mmap_cache.read_at(file, &mut buf, offset_base + (idx * 4) as u64)?;
                offset_map.insert(idx, u32::from_le_bytes(buf));
            }
        }

        // Calculate data offset base: skip count(8) + offsets((total_count+1)*4) + data_len(8)
        let data_base = index.data_offset + 8 + (total_count + 1) as u64 * 4 + 8;

        // Read data for each requested row and build result
        let mut result_offsets = vec![0u64];
        let mut result_data = Vec::new();

        for &idx in row_indices {
            if idx < total_count {
                let start = *offset_map.get(&idx).unwrap_or(&0);
                let end = *offset_map.get(&(idx + 1)).unwrap_or(&start);
                let len = (end - start) as usize;

                if len > 0 {
                    let mut chunk = vec![0u8; len];
                    mmap_cache.read_at(file, &mut chunk, data_base + start as u64)?;
                    result_data.extend_from_slice(&chunk);
                }
                result_offsets.push(result_data.len() as u64);
            } else {
                // Out of bounds - push empty
                result_offsets.push(result_data.len() as u64);
            }
        }

        match dtype {
            ColumnType::String => Ok(ColumnData::String {
                offsets: result_offsets,
                data: result_data,
            }),
            ColumnType::Binary | ColumnType::Blob => Ok(ColumnData::Binary {
                offsets: result_offsets,
                data: result_data,
            }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Not a variable type",
            )),
        }
    }

    pub(super) fn read_numeric_scattered_optimized<T: Copy + Default + 'static>(
        mmap_cache: &mut MmapCache,
        file: &File,
        index: &ColumnIndexEntry,
        row_indices: &[usize],
        header_size: u64,
    ) -> io::Result<Vec<T>> {
        if row_indices.is_empty() {
            return Ok(Vec::new());
        }

        let n = row_indices.len();
        let elem_size = std::mem::size_of::<T>();

        // For small numbers, simple sequential read without sorting is faster
        // Typical LIMIT queries (100-500 rows) benefit from avoiding sort overhead
        if n <= 256 {
            let mut values = Vec::with_capacity(n);
            SCATTERED_READ_BUF.with(|buf| {
                let mut buf = buf.borrow_mut();
                buf.resize(8, 0);
                for &idx in row_indices {
                    mmap_cache.read_at(
                        file,
                        &mut buf[..elem_size],
                        index.data_offset + header_size + (idx * elem_size) as u64,
                    )?;
                    let val: T = unsafe { std::ptr::read(buf.as_ptr() as *const T) };
                    values.push(val);
                }
                Ok::<(), io::Error>(())
            })?;
            return Ok(values);
        }

        // ROW-GROUP BASED READING for larger scattered reads
        const ROW_GROUP_SIZE: usize = 8192;

        // Sort indices and track original positions
        let mut indexed: Vec<(usize, usize)> = row_indices
            .iter()
            .enumerate()
            .map(|(i, &idx)| (idx, i))
            .collect();
        indexed.sort_unstable_by_key(|&(idx, _)| idx);

        let mut result: Vec<T> = vec![T::default(); n];
        let mut i = 0;

        // Process by row-groups
        while i < indexed.len() {
            let first_idx = indexed[i].0;
            let group_start = (first_idx / ROW_GROUP_SIZE) * ROW_GROUP_SIZE;
            let group_end = group_start + ROW_GROUP_SIZE;

            // Find all indices within this row-group
            let mut group_indices = Vec::new();
            while i < indexed.len() && indexed[i].0 < group_end {
                group_indices.push(indexed[i]);
                i += 1;
            }

            // Decide read strategy based on density within group
            let indices_in_group = group_indices.len();
            let span = group_indices.last().unwrap().0 - group_indices.first().unwrap().0 + 1;

            // If indices are dense enough, read the span; otherwise read full group
            if indices_in_group * 4 >= span || span <= 256 {
                // Dense or small span: read just the span
                let read_start = group_indices.first().unwrap().0;
                let read_len = span;
                let mut buf: Vec<u8> = vec![0u8; read_len * elem_size];
                mmap_cache.read_at(
                    file,
                    &mut buf,
                    index.data_offset + header_size + (read_start * elem_size) as u64,
                )?;

                for (idx, orig_pos) in group_indices {
                    let offset = idx - read_start;
                    let val: T =
                        unsafe { std::ptr::read(buf.as_ptr().add(offset * elem_size) as *const T) };
                    result[orig_pos] = val;
                }
            } else {
                // Sparse: read individual values (but they're sorted so still sequential-ish)
                let mut buf = [0u8; 8];
                for (idx, orig_pos) in group_indices {
                    mmap_cache.read_at(
                        file,
                        &mut buf[..elem_size],
                        index.data_offset + header_size + (idx * elem_size) as u64,
                    )?;
                    let val: T = unsafe { std::ptr::read(buf.as_ptr() as *const T) };
                    result[orig_pos] = val;
                }
            }
        }

        Ok(result)
    }

    pub(super) fn read_fixed_scattered_optimized(
        mmap_cache: &mut MmapCache,
        file: &File,
        index: &ColumnIndexEntry,
        row_indices: &[usize],
        elem_bytes: usize,
    ) -> io::Result<(Vec<u8>, u32)> {
        // Read dim from header: [count:u64][dim:u32][data...]
        let mut dim_buf = [0u8; 4];
        mmap_cache.read_at(file, &mut dim_buf, index.data_offset + 8)?;
        let dim = u32::from_le_bytes(dim_buf);
        let dim_usize = dim as usize;
        let row_byte_len = dim_usize * elem_bytes;

        if row_indices.is_empty() || dim_usize == 0 {
            return Ok((Vec::new(), dim));
        }

        let n = row_indices.len();
        let data_base = index.data_offset + 12; // skip count(8) + dim(4)
        let mut result = vec![0u8; n * row_byte_len];

        if n <= 256 {
            // Small: sequential reads using thread-local buffer
            SCATTERED_READ_BUF.with(|buf| {
                let mut buf = buf.borrow_mut();
                buf.resize(row_byte_len, 0);
                for (out_i, &row) in row_indices.iter().enumerate() {
                    mmap_cache.read_at(
                        file,
                        &mut buf[..row_byte_len],
                        data_base + (row * row_byte_len) as u64,
                    )?;
                    result[out_i * row_byte_len..(out_i + 1) * row_byte_len]
                        .copy_from_slice(&buf[..row_byte_len]);
                }
                Ok::<(), io::Error>(())
            })?;
        } else {
            // Large: sort indices, batch into contiguous span reads
            const ROW_GROUP_SIZE: usize = 8192;
            let mut indexed: Vec<(usize, usize)> = row_indices
                .iter()
                .enumerate()
                .map(|(i, &idx)| (idx, i))
                .collect();
            indexed.sort_unstable_by_key(|&(idx, _)| idx);

            let mut i = 0;
            while i < indexed.len() {
                let first_idx = indexed[i].0;
                let group_start = (first_idx / ROW_GROUP_SIZE) * ROW_GROUP_SIZE;
                let group_end = group_start + ROW_GROUP_SIZE;

                let mut group_indices = Vec::new();
                while i < indexed.len() && indexed[i].0 < group_end {
                    group_indices.push(indexed[i]);
                    i += 1;
                }

                let span = group_indices.last().unwrap().0 - group_indices.first().unwrap().0 + 1;
                let indices_in_group = group_indices.len();

                if indices_in_group * 4 >= span || span <= 256 {
                    // Dense or small span: read contiguous range
                    let read_start = group_indices.first().unwrap().0;
                    let buf_len = span * row_byte_len;
                    let mut buf = vec![0u8; buf_len];
                    mmap_cache.read_at(
                        file,
                        &mut buf,
                        data_base + (read_start * row_byte_len) as u64,
                    )?;
                    for (idx, orig_pos) in group_indices {
                        let offset = (idx - read_start) * row_byte_len;
                        result[orig_pos * row_byte_len..(orig_pos + 1) * row_byte_len]
                            .copy_from_slice(&buf[offset..offset + row_byte_len]);
                    }
                } else {
                    // Sparse: individual reads
                    let mut buf = vec![0u8; row_byte_len];
                    for (idx, orig_pos) in group_indices {
                        mmap_cache.read_at(
                            file,
                            &mut buf,
                            data_base + (idx * row_byte_len) as u64,
                        )?;
                        result[orig_pos * row_byte_len..(orig_pos + 1) * row_byte_len]
                            .copy_from_slice(&buf);
                    }
                }
            }
        }

        Ok((result, dim))
    }

    pub(super) fn read_column_scattered_mmap(
        &self,
        mmap_cache: &mut MmapCache,
        file: &File,
        index: &ColumnIndexEntry,
        dtype: ColumnType,
        row_indices: &[usize],
        _total_rows: usize,
    ) -> io::Result<ColumnData> {
        // ColumnData format has an 8-byte count header
        const HEADER_SIZE: u64 = 8;

        match dtype {
            ColumnType::Int64
            | ColumnType::Int8
            | ColumnType::Int16
            | ColumnType::Int32
            | ColumnType::UInt8
            | ColumnType::UInt16
            | ColumnType::UInt32
            | ColumnType::UInt64
            | ColumnType::Timestamp
            | ColumnType::Date => Self::read_numeric_scattered_optimized::<i64>(
                mmap_cache,
                file,
                index,
                row_indices,
                HEADER_SIZE,
            )
            .map(ColumnData::Int64),
            ColumnType::Float64 | ColumnType::Float32 => {
                Self::read_numeric_scattered_optimized::<f64>(
                    mmap_cache,
                    file,
                    index,
                    row_indices,
                    HEADER_SIZE,
                )
                .map(ColumnData::Float64)
            }
            ColumnType::String | ColumnType::Binary | ColumnType::Blob => {
                // Optimized scattered read for variable-length types
                self.read_variable_column_scattered_mmap(
                    mmap_cache,
                    file,
                    index,
                    dtype,
                    row_indices,
                )
            }
            ColumnType::Bool => {
                // Bool is stored as packed bits: [count:u64][packed_bits...]
                // Read the packed bits and extract specific indices
                let packed_len = (index.data_length as usize - 8 + 7) / 8;
                let mut packed = vec![0u8; packed_len.max(1)];
                if packed_len > 0 {
                    mmap_cache.read_at(file, &mut packed, index.data_offset + HEADER_SIZE)?;
                }

                // Extract the specific bits for requested indices
                let mut result_packed = vec![0u8; (row_indices.len() + 7) / 8];
                for (result_idx, &src_idx) in row_indices.iter().enumerate() {
                    let src_byte = src_idx / 8;
                    let src_bit = src_idx % 8;
                    let bit_value = if src_byte < packed.len() {
                        (packed[src_byte] >> src_bit) & 1
                    } else {
                        0
                    };

                    let dst_byte = result_idx / 8;
                    let dst_bit = result_idx % 8;
                    if bit_value == 1 {
                        result_packed[dst_byte] |= 1 << dst_bit;
                    }
                }

                Ok(ColumnData::Bool {
                    data: result_packed,
                    len: row_indices.len(),
                })
            }
            ColumnType::StringDict => {
                // Native dictionary-encoded string scattered read
                self.read_string_dict_column_scattered_mmap(mmap_cache, file, index, row_indices)
            }
            ColumnType::FixedList => {
                Self::read_fixed_scattered_optimized(mmap_cache, file, index, row_indices, 4)
                    .map(|(data, dim)| ColumnData::FixedList { data, dim })
            }
            ColumnType::Float16List => {
                Self::read_fixed_scattered_optimized(mmap_cache, file, index, row_indices, 2)
                    .map(|(data, dim)| ColumnData::Float16List { data, dim })
            }
            dtype @ (ColumnType::BFloat16List
            | ColumnType::Int8Vector
            | ColumnType::UInt8Vector
            | ColumnType::Bit1Vector
            | ColumnType::TurboQuant2Vector
            | ColumnType::TurboQuant3Vector
            | ColumnType::TurboQuant4Vector) => {
                let mut dim_buf = [0u8; 4];
                mmap_cache.read_at(file, &mut dim_buf, index.data_offset + 8)?;
                let dim = u32::from_le_bytes(dim_buf);
                let codec = dtype.vector_codec().unwrap();
                let stride = codec.row_width(dim as usize)?;
                let mut data = vec![0u8; row_indices.len().checked_mul(stride)
                    .ok_or_else(|| err_data("quantized scattered length overflow"))?];
                for (output_row, &source_row) in row_indices.iter().enumerate() {
                    mmap_cache.read_at(file,
                        &mut data[output_row * stride..(output_row + 1) * stride],
                        index.data_offset + 12 + (source_row * stride) as u64)?;
                }
                Ok(ColumnData::QuantizedList { data, dim, codec })
            }
            ColumnType::Null => Ok(ColumnData::Int64(vec![0i64; row_indices.len()])),
        }
    }

    pub fn extract_rows_by_indices_to_arrow(
        &self,
        indices: &[usize],
        col_refs: Option<&[&str]>,
    ) -> io::Result<Option<arrow::record_batch::RecordBatch>> {
        use arrow::array::{
            ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, StringBuilder,
        };
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use std::sync::Arc;

        if indices.is_empty() {
            return Ok(Some(arrow::record_batch::RecordBatch::new_empty(Arc::new(
                Schema::empty(),
            ))));
        }

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let col_count = schema.column_count();

        // Column projection: build a mask of which columns to actually extract
        let col_needed: Vec<bool> = if let Some(refs) = col_refs {
            schema
                .columns
                .iter()
                .map(|(name, _)| refs.iter().any(|r| r.eq_ignore_ascii_case(name)))
                .collect()
        } else {
            vec![true; col_count]
        };

        // Build RG cumulative bounds (binary-search friendly)
        let mut cumulative = 0usize;
        let rg_bounds: Vec<(usize, usize)> = footer
            .row_groups
            .iter()
            .map(|rg| {
                let s = cumulative;
                cumulative += rg.row_count as usize;
                (s, cumulative)
            })
            .collect();

        // Group indices by RG: (output_position, local_index_within_rg)
        // indices from scan_like_filter_mmap are sorted → out_idx is monotonically increasing
        let mut rg_local_indices: Vec<Vec<(usize, usize)>> =
            vec![Vec::new(); footer.row_groups.len()];
        for (out_idx, &global_idx) in indices.iter().enumerate() {
            let rg_i = rg_bounds.partition_point(|&(_, end)| end <= global_idx);
            if rg_i < footer.row_groups.len() {
                rg_local_indices[rg_i].push((out_idx, global_idx - rg_bounds[rg_i].0));
            }
        }

        let n_out = indices.len();
        let mut out_ids: Vec<i64> = vec![0i64; n_out];

        // Typed per-column storage — no Value enum boxing, no String heap alloc
        enum ColBuf {
            I64(Vec<Option<i64>>),
            F64(Vec<Option<f64>>),
            Str(StringBuilder), // String/StringDict: sequential append, zero alloc
            Bool(Vec<Option<bool>>),
            Bin(Vec<Option<Vec<u8>>>), // Binary columns: preserve raw bytes
            FixedVec(Vec<Option<Vec<u8>>>, u32), // vector columns decoded to raw f32 bytes
            Other(Vec<Option<crate::data::Value>>), // rare fallback
        }
        let mut col_bufs: Vec<ColBuf> = schema
            .columns
            .iter()
            .enumerate()
            .map(|(ci, (_, ct))| {
                if !col_needed[ci] {
                    return ColBuf::I64(Vec::new()); // placeholder, never filled
                }
                match ct {
                    ColumnType::Int64
                    | ColumnType::Int8
                    | ColumnType::Int16
                    | ColumnType::Int32
                    | ColumnType::UInt8
                    | ColumnType::UInt16
                    | ColumnType::UInt32
                    | ColumnType::UInt64
                    | ColumnType::Timestamp
                    | ColumnType::Date => ColBuf::I64(vec![None; n_out]),
                    ColumnType::Float64 | ColumnType::Float32 => ColBuf::F64(vec![None; n_out]),
                    ColumnType::String | ColumnType::StringDict => {
                        ColBuf::Str(StringBuilder::with_capacity(n_out, n_out * 10))
                    }
                    ColumnType::Bool => ColBuf::Bool(vec![None; n_out]),
                    ColumnType::Binary | ColumnType::Blob => {
                        ColBuf::Bin(vec![None; n_out])
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
                        ColBuf::FixedVec(vec![None; n_out], 0)
                    }
                    _ => ColBuf::Other(vec![None; n_out]),
                }
            })
            .collect();

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for batch extract"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        // ── PARALLEL COLUMN EXTRACTION ────────────────────────────────────
        // Pre-compute column offsets for each RG (from RCIX or by scanning).
        // Then process each column independently in parallel via rayon.
        let needed_col_indices: Vec<usize> = (0..col_count).filter(|&ci| col_needed[ci]).collect();
        // Schema evolution (footer-only ADD COLUMN): Row Groups predating a
        // requested column lack its data. Fall back to the padded full-read +
        // take path (scan_columns_mmap_with_nulls) instead of producing
        // short/misaligned columns.
        for (rg_i, local_pairs) in rg_local_indices.iter().enumerate() {
            if local_pairs.is_empty() {
                continue;
            }
            if rg_i >= footer.col_offsets.len()
                || needed_col_indices
                    .iter()
                    .any(|&ci| ci >= footer.col_offsets[rg_i].len())
            {
                return Ok(None);
            }
        }
        let mut par_col_offsets: Vec<Option<Vec<u32>>> = Vec::new(); // per-RG col offsets
        let mut par_eligible = needed_col_indices.len() >= 2
            && n_out >= 500
            && needed_col_indices.iter().all(|&ci| {
                matches!(
                    schema.columns[ci].1,
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
                        | ColumnType::String
                        | ColumnType::StringDict
                        | ColumnType::Bool
                )
            });
        if par_eligible {
            for (rg_i, local_pairs) in rg_local_indices.iter().enumerate() {
                if local_pairs.is_empty() {
                    par_col_offsets.push(None); // no rows needed
                    continue;
                }
                if rg_i >= footer.row_groups.len() {
                    par_eligible = false;
                    break;
                }
                let rg_meta = &footer.row_groups[rg_i];
                let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
                if rg_end > mmap_ref.len() {
                    par_eligible = false;
                    break;
                }
                let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
                if rg_bytes.len() < 32 {
                    par_eligible = false;
                    break;
                }
                let compress_flag = rg_bytes[28];
                let enc_ver = rg_bytes[29];
                if compress_flag != RG_COMPRESS_NONE || enc_ver < 1 {
                    par_eligible = false;
                    break;
                }
                // Try RCIX first
                if rg_i < footer.col_offsets.len() && footer.col_offsets[rg_i].len() >= col_count {
                    par_col_offsets.push(Some(footer.col_offsets[rg_i].clone()));
                } else {
                    // Compute offsets by scanning through columns
                    let body = &rg_bytes[32..];
                    let rg_rows = rg_meta.row_count as usize;
                    let null_bm_len = (rg_rows + 7) / 8;
                    let mut offsets = Vec::with_capacity(col_count);
                    let mut pos = rg_id_section_len(
                        rg_rows,
                        rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
                    ) + null_bm_len;
                    let mut ok = true;
                    for ci in 0..col_count {
                        offsets.push(pos as u32);
                        // Advance past null bitmap + encoded column data
                        pos += null_bm_len;
                        if pos > body.len() {
                            ok = false;
                            break;
                        }
                        match skip_column_encoded(&body[pos..], schema.columns[ci].1) {
                            Ok(consumed) => pos += consumed,
                            Err(_) => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    if !ok || offsets.len() < col_count {
                        par_eligible = false;
                        break;
                    }
                    par_col_offsets.push(Some(offsets));
                }
            }
        }

        if par_eligible {
            use rayon::prelude::*;
            // Extract IDs
            for (rg_i, local_pairs) in rg_local_indices.iter().enumerate() {
                if local_pairs.is_empty() {
                    continue;
                }
                let rg_meta = &footer.row_groups[rg_i];
                let rg_rows = rg_meta.row_count as usize;
                let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
                let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
                let body = &rg_bytes[32..];
                let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
                for &(out_idx, local_idx) in local_pairs {
                    if let Some(id) =
                        rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, local_idx)
                    {
                        out_ids[out_idx] = id as i64;
                    }
                }
            }

            let mmap_ptr = mmap_ref.as_ptr() as usize;
            let mmap_len = mmap_ref.len();

            // Each parallel task extracts data AND builds the Arrow array in one pass.
            let col_arrays: Vec<(usize, Field, ArrayRef)> = needed_col_indices
                .par_iter()
                .map(|&ci| {
                    let mmap =
                        unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                    let ct = schema.columns[ci].1;
                    let col_name = &schema.columns[ci].0;

                    macro_rules! for_each_rg {
                        ($handler:expr) => {{
                            for (rg_i, local_pairs) in rg_local_indices.iter().enumerate() {
                                if local_pairs.is_empty() {
                                    continue;
                                }
                                let offsets = match &par_col_offsets[rg_i] {
                                    Some(o) => o,
                                    None => continue,
                                };
                                let rg_meta = &footer.row_groups[rg_i];
                                let rg_rows = rg_meta.row_count as usize;
                                let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
                                if rg_end > mmap.len() {
                                    continue;
                                }
                                let rg_bytes = &mmap[rg_meta.offset as usize..rg_end];
                                let body = &rg_bytes[32..];
                                let null_bitmap_len = (rg_rows + 7) / 8;
                                let col_off = offsets[ci] as usize;
                                if col_off + null_bitmap_len > body.len() {
                                    continue;
                                }
                                let null_bytes = &body[col_off..col_off + null_bitmap_len];
                                let col_bytes = &body[col_off + null_bitmap_len..];
                                if col_bytes.is_empty() {
                                    continue;
                                }
                                let encoding = col_bytes[0];
                                let payload = &col_bytes[1..];
                                #[allow(clippy::redundant_closure_call)]
                                ($handler)(local_pairs, null_bytes, encoding, payload, rg_rows);
                            }
                        }};
                    }

                    match ct {
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
                            // Primitive buffer + validity: 9 B/row instead of the
                            // 16 B Option layout, and the final array is a buffer
                            // move rather than an element-wise rebuild.
                            let mut vals: Vec<i64> = vec![0; n_out];
                            let mut valid: Vec<bool> = vec![false; n_out];
                            for_each_rg!(
                                |pairs: &[(usize, usize)],
                                 null_bytes: &[u8],
                                 encoding: u8,
                                 payload: &[u8],
                                 _rg_rows: usize| {
                                    if encoding == COL_ENCODING_PLAIN && payload.len() >= 8 {
                                        for &(out_idx, local_idx) in pairs {
                                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1
                                                == 1
                                            {
                                                continue;
                                            }
                                            let off = 8 + local_idx * 8;
                                            if off + 8 <= payload.len() {
                                                vals[out_idx] = i64::from_le_bytes(
                                                    payload[off..off + 8].try_into().unwrap(),
                                                );
                                                valid[out_idx] = true;
                                            }
                                        }
                                    } else if encoding == 2 {
                                        for &(out_idx, local_idx) in pairs {
                                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1
                                                == 1
                                            {
                                                continue;
                                            }
                                            if let Some(v) =
                                                crate::storage::on_demand::bitpack_decode_at_idx(
                                                    payload, local_idx,
                                                )
                                            {
                                                vals[out_idx] = v;
                                                valid[out_idx] = true;
                                            }
                                        }
                                    }
                                }
                            );
                            let null_buf = valid
                                .iter()
                                .any(|&v| !v)
                                .then(|| arrow::buffer::NullBuffer::from(valid));
                            (
                                ci,
                                Field::new(col_name, ArrowDataType::Int64, true),
                                Arc::new(Int64Array::new(
                                    arrow::buffer::ScalarBuffer::from(vals),
                                    null_buf,
                                )) as ArrayRef,
                            )
                        }
                        ColumnType::Float64 | ColumnType::Float32 => {
                            // Primitive buffer + validity (see Int64 arm).
                            let mut vals: Vec<f64> = vec![0.0; n_out];
                            let mut valid: Vec<bool> = vec![false; n_out];
                            for_each_rg!(
                                |pairs: &[(usize, usize)],
                                 null_bytes: &[u8],
                                 encoding: u8,
                                 payload: &[u8],
                                 _rg_rows: usize| {
                                    if encoding == COL_ENCODING_PLAIN && payload.len() >= 8 {
                                        for &(out_idx, local_idx) in pairs {
                                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1
                                                == 1
                                            {
                                                continue;
                                            }
                                            let off = 8 + local_idx * 8;
                                            if off + 8 <= payload.len() {
                                                vals[out_idx] = f64::from_le_bytes(
                                                    payload[off..off + 8].try_into().unwrap(),
                                                );
                                                valid[out_idx] = true;
                                            }
                                        }
                                    } else if encoding == COL_ENCODING_FLOAT_DICTIONARY {
                                        if let Ok(view) = FloatDictView::parse(payload) {
                                            for &(out_idx, local_idx) in pairs {
                                                if (null_bytes[local_idx / 8] >> (local_idx % 8))
                                                    & 1
                                                    == 1
                                                {
                                                    continue;
                                                }
                                                if let Some(v) = view.value(local_idx) {
                                                    vals[out_idx] = v;
                                                    valid[out_idx] = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            );
                            let null_buf = valid
                                .iter()
                                .any(|&v| !v)
                                .then(|| arrow::buffer::NullBuffer::from(valid));
                            (
                                ci,
                                Field::new(col_name, ArrowDataType::Float64, true),
                                Arc::new(Float64Array::new(
                                    arrow::buffer::ScalarBuffer::from(vals),
                                    null_buf,
                                )) as ArrayRef,
                            )
                        }
                        ColumnType::StringDict => {
                            // Extract: collect (out_idx, start, end) ranges into mmap
                            let mut ranges: Vec<(usize, usize, usize)> = Vec::new();
                            for_each_rg!(
                                |pairs: &[(usize, usize)],
                                 null_bytes: &[u8],
                                 encoding: u8,
                                 payload: &[u8],
                                 _rg_rows: usize| {
                                    if !matches!(
                                        encoding,
                                        COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                                    ) {
                                        return;
                                    }
                                    let Ok(view) = StringDictView::parse(
                                        payload,
                                        encoding == COL_ENCODING_COMPACT_DICTIONARY,
                                    ) else {
                                        return;
                                    };
                                    for &(out_idx, local_idx) in pairs {
                                        if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                            continue;
                                        }
                                        if let Some(value) = view
                                            .index(local_idx)
                                            .and_then(|index| view.value(index))
                                        {
                                            let start = value.as_ptr() as usize - mmap_ptr;
                                            ranges.push((out_idx, start, start + value.len()));
                                        }
                                    }
                                }
                            );
                            // Build StringArray: null-prefilled builder, overwrite with values
                            // More efficient: just build from Option<&str> vec
                            let mut strs: Vec<Option<&str>> = vec![None; n_out];
                            for &(idx, s, e) in &ranges {
                                strs[idx] = Some(std::str::from_utf8(&mmap[s..e]).unwrap_or(""));
                            }
                            let arr: StringArray = strs.into_iter().collect();
                            (
                                ci,
                                Field::new(col_name, ArrowDataType::Utf8, true),
                                Arc::new(arr) as ArrayRef,
                            )
                        }
                        ColumnType::String => {
                            let mut ranges: Vec<(usize, usize, usize)> = Vec::new();
                            for_each_rg!(
                                |pairs: &[(usize, usize)],
                                 null_bytes: &[u8],
                                 encoding: u8,
                                 payload: &[u8],
                                 rg_rows: usize| {
                                    if encoding != COL_ENCODING_PLAIN || payload.len() < 8 {
                                        return;
                                    }
                                    let count =
                                        u64::from_le_bytes(payload[0..8].try_into().unwrap())
                                            as usize;
                                    let data_len_off = 8 + (count + 1) * 4;
                                    if data_len_off + 8 > payload.len() {
                                        return;
                                    }
                                    let data_start = data_len_off + 8;
                                    let n = count.min(rg_rows);
                                    let payload_abs = payload.as_ptr() as usize - mmap_ptr;
                                    for &(out_idx, local_idx) in pairs {
                                        if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                            continue;
                                        }
                                        if local_idx >= n {
                                            continue;
                                        }
                                        let s_off = 8 + local_idx * 4;
                                        let e_off = s_off + 4;
                                        if e_off + 4 > payload.len() {
                                            continue;
                                        }
                                        let s = u32::from_le_bytes(
                                            payload[s_off..s_off + 4].try_into().unwrap(),
                                        ) as usize;
                                        let e = u32::from_le_bytes(
                                            payload[e_off..e_off + 4].try_into().unwrap(),
                                        ) as usize;
                                        if data_start + e <= payload.len() {
                                            ranges.push((
                                                out_idx,
                                                payload_abs + data_start + s,
                                                payload_abs + data_start + e,
                                            ));
                                        }
                                    }
                                }
                            );
                            let mut strs: Vec<Option<&str>> = vec![None; n_out];
                            for &(idx, s, e) in &ranges {
                                strs[idx] = Some(std::str::from_utf8(&mmap[s..e]).unwrap_or(""));
                            }
                            let arr: StringArray = strs.into_iter().collect();
                            (
                                ci,
                                Field::new(col_name, ArrowDataType::Utf8, true),
                                Arc::new(arr) as ArrayRef,
                            )
                        }
                        ColumnType::Bool => {
                            let mut vals: Vec<Option<bool>> = vec![None; n_out];
                            for_each_rg!(
                                |pairs: &[(usize, usize)],
                                 null_bytes: &[u8],
                                 encoding: u8,
                                 payload: &[u8],
                                 _rg_rows: usize| {
                                    if encoding != COL_ENCODING_PLAIN || payload.len() < 8 {
                                        return;
                                    }
                                    for &(out_idx, local_idx) in pairs {
                                        if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                            continue;
                                        }
                                        let byte_off = 8 + local_idx / 8;
                                        if byte_off < payload.len() {
                                            vals[out_idx] = Some(
                                                (payload[byte_off] >> (local_idx % 8)) & 1 == 1,
                                            );
                                        }
                                    }
                                }
                            );
                            let arr: BooleanArray = vals.into_iter().collect();
                            (
                                ci,
                                Field::new(col_name, ArrowDataType::Boolean, true),
                                Arc::new(arr) as ArrayRef,
                            )
                        }
                        _ => {
                            // Binary / FixedVec / unknown: produce null Utf8 column (fallback path handles these)
                            let arr = StringArray::from(vec![None::<&str>; n_out]);
                            (
                                ci,
                                Field::new(col_name, ArrowDataType::Utf8, true),
                                Arc::new(arr) as ArrayRef,
                            )
                        }
                    }
                })
                .collect();

            // Assemble RecordBatch from parallel results
            let mut fields: Vec<Field> = Vec::with_capacity(col_count + 1);
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_count + 1);
            let include_id = col_refs
                .map(|refs| refs.iter().any(|r| r.eq_ignore_ascii_case("_id")))
                .unwrap_or(true);
            if include_id {
                fields.push(Field::new("_id", ArrowDataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(out_ids)));
            }
            for (_, field, arr) in col_arrays {
                fields.push(field);
                arrays.push(arr);
            }
            let schema_ref = Arc::new(Schema::new(fields));
            let batch = arrow::record_batch::RecordBatch::try_new(schema_ref, arrays)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            drop(mmap_guard);
            drop(file_guard);
            return Ok(Some(batch));
        }
        // ── END PARALLEL COLUMN EXTRACTION ─────────────────────────────────

        for (rg_i, local_pairs) in rg_local_indices.iter().enumerate() {
            if local_pairs.is_empty() {
                continue;
            }
            let rg_meta = &footer.row_groups[rg_i];
            let rg_rows = rg_meta.row_count as usize;
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
            let null_bitmap_len = (rg_rows + 7) / 8;

            // Extract IDs for target rows (no mmap copy — slice read)
            let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
            let id_section_len = rg_id_section_len(rg_rows, id_encoding);
            for &(out_idx, local_idx) in local_pairs {
                if let Some(id) = rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, local_idx) {
                    out_ids[out_idx] = id as i64;
                }
            }

            // Use RCIX for O(1) per-column access when available
            let rcix = if compress_flag == RG_COMPRESS_NONE
                && encoding_version >= 1
                && rg_i < footer.col_offsets.len()
                && footer.col_offsets[rg_i].len() >= col_count
            {
                Some(&footer.col_offsets[rg_i])
            } else {
                None
            };

            let mut pos = id_section_len + null_bitmap_len;

            for ci in 0..col_count {
                let ct = schema.columns[ci].1;
                // Column projection: skip columns not needed
                if !col_needed[ci] {
                    if rcix.is_none() {
                        // Sequential layout: must advance pos past null bitmap + column data
                        if pos + null_bitmap_len > body.len() {
                            break;
                        }
                        pos += null_bitmap_len;
                        let consumed = if encoding_version >= 1 {
                            skip_column_encoded(&body[pos..], ct)?
                        } else {
                            ColumnData::skip_bytes_typed(&body[pos..], ct)?
                        };
                        pos += consumed;
                    }
                    // RCIX: no pos tracking needed, just skip
                    continue;
                }
                let (null_bytes, col_bytes) = if let Some(rcix) = rcix {
                    let col_off = rcix[ci] as usize;
                    if col_off + null_bitmap_len > body.len() {
                        // Column not present: append nulls for Str buffers (maintain length invariant)
                        if let ColBuf::Str(ref mut b) = col_bufs[ci] {
                            for _ in local_pairs {
                                b.append_null();
                            }
                        }
                        continue;
                    }
                    (
                        &body[col_off..col_off + null_bitmap_len],
                        &body[col_off + null_bitmap_len..],
                    )
                } else {
                    if pos + null_bitmap_len > body.len() {
                        break;
                    }
                    let nb = &body[pos..pos + null_bitmap_len];
                    pos += null_bitmap_len;
                    (nb, &body[pos..])
                };
                let enc_offset = if encoding_version >= 1 { 1 } else { 0 };
                let encoding = if encoding_version >= 1 && !col_bytes.is_empty() {
                    col_bytes[0]
                } else {
                    COL_ENCODING_PLAIN
                };
                let data_bytes = if enc_offset <= col_bytes.len() {
                    &col_bytes[enc_offset..]
                } else {
                    &[] as &[u8]
                };

                // Fast typed extraction — no Value boxing
                let extracted = match (encoding, ct, &mut col_bufs[ci]) {
                    // Plain Int64-compatible
                    (
                        COL_ENCODING_PLAIN,
                        ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date,
                        ColBuf::I64(vals),
                    ) if data_bytes.len() >= 8 => {
                        for &(out_idx, local_idx) in local_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            let off = 8 + local_idx * 8;
                            if off + 8 <= data_bytes.len() {
                                vals[out_idx] = Some(i64::from_le_bytes(
                                    data_bytes[off..off + 8].try_into().unwrap(),
                                ));
                            }
                        }
                        true
                    }
                    // Plain Float64/Float32
                    (
                        COL_ENCODING_PLAIN,
                        ColumnType::Float64 | ColumnType::Float32,
                        ColBuf::F64(vals),
                    ) if data_bytes.len() >= 8 => {
                        for &(out_idx, local_idx) in local_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            let off = 8 + local_idx * 8;
                            if off + 8 <= data_bytes.len() {
                                vals[out_idx] = Some(f64::from_le_bytes(
                                    data_bytes[off..off + 8].try_into().unwrap(),
                                ));
                            }
                        }
                        true
                    }
                    (
                        COL_ENCODING_FLOAT_DICTIONARY,
                        ColumnType::Float64 | ColumnType::Float32,
                        ColBuf::F64(vals),
                    ) => {
                        let view = FloatDictView::parse(data_bytes)?;
                        for &(out_idx, local_idx) in local_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            vals[out_idx] = view.value(local_idx);
                        }
                        true
                    }
                    // Plain String — zero-copy slice to StringBuilder
                    (COL_ENCODING_PLAIN, ColumnType::String, ColBuf::Str(b))
                        if data_bytes.len() >= 8 =>
                    {
                        let count =
                            u64::from_le_bytes(data_bytes[0..8].try_into().unwrap()) as usize;
                        let data_len_off = 8 + (count + 1) * 4;
                        if data_len_off + 8 <= data_bytes.len() {
                            let data_start = data_len_off + 8;
                            for &(_, local_idx) in local_pairs {
                                if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                    b.append_null();
                                    continue;
                                }
                                if local_idx >= count {
                                    b.append_null();
                                    continue;
                                }
                                let s_off = 8 + local_idx * 4;
                                let e_off = s_off + 4;
                                if e_off + 4 <= data_bytes.len() {
                                    let s = u32::from_le_bytes(
                                        data_bytes[s_off..s_off + 4].try_into().unwrap(),
                                    ) as usize;
                                    let e = u32::from_le_bytes(
                                        data_bytes[e_off..e_off + 4].try_into().unwrap(),
                                    ) as usize;
                                    if data_start + e <= data_bytes.len() {
                                        b.append_value(
                                            std::str::from_utf8(
                                                &data_bytes[data_start + s..data_start + e],
                                            )
                                            .unwrap_or(""),
                                        );
                                    } else {
                                        b.append_null();
                                    }
                                } else {
                                    b.append_null();
                                }
                            }
                            true
                        } else {
                            false
                        }
                    }
                    // StringDict — zero-copy dict lookup to StringBuilder
                    (
                        encoding @ (COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY),
                        ColumnType::StringDict,
                        ColBuf::Str(b),
                    ) => {
                        let Ok(view) = StringDictView::parse(
                            data_bytes,
                            encoding == COL_ENCODING_COMPACT_DICTIONARY,
                        ) else {
                            return Ok(None);
                        };
                        for &(_, local_idx) in local_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                b.append_null();
                                continue;
                            }
                            match view.index(local_idx).and_then(|index| view.value(index)) {
                                Some(value) => {
                                    b.append_value(std::str::from_utf8(value).unwrap_or(""))
                                }
                                None => b.append_null(),
                            }
                        }
                        true
                    }
                    // Bitpack Int64-compatible
                    (
                        2u8,
                        ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date,
                        ColBuf::I64(vals),
                    ) => {
                        for &(out_idx, local_idx) in local_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            if let Some(v) = crate::storage::on_demand::bitpack_decode_at_idx(
                                data_bytes, local_idx,
                            ) {
                                vals[out_idx] = Some(v);
                            }
                        }
                        true
                    }
                    // Plain Bool
                    (COL_ENCODING_PLAIN, ColumnType::Bool, ColBuf::Bool(vals))
                        if data_bytes.len() >= 8 =>
                    {
                        for &(out_idx, local_idx) in local_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            let byte_off = 8 + local_idx / 8;
                            if byte_off < data_bytes.len() {
                                vals[out_idx] =
                                    Some((data_bytes[byte_off] >> (local_idx % 8)) & 1 == 1);
                            }
                        }
                        true
                    }
                    _ => false,
                };

                if !extracted {
                    // Fallback: decode full column and pick values at target indices
                    let (col_data, _) = if encoding_version >= 1 {
                        read_column_encoded(col_bytes, ct)?
                    } else {
                        ColumnData::from_bytes_typed(col_bytes, ct)?
                    };
                    match &mut col_bufs[ci] {
                        ColBuf::I64(vals) => {
                            if let ColumnData::Int64(v) = &col_data {
                                for &(out_idx, local_idx) in local_pairs {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if local_idx < v.len() {
                                        vals[out_idx] = Some(v[local_idx]);
                                    }
                                }
                            }
                        }
                        ColBuf::F64(vals) => {
                            if let ColumnData::Float64(v) = &col_data {
                                for &(out_idx, local_idx) in local_pairs {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if local_idx < v.len() {
                                        vals[out_idx] = Some(v[local_idx]);
                                    }
                                }
                            }
                        }
                        ColBuf::Bool(vals) => {
                            if let ColumnData::Bool { data, len } = &col_data {
                                for &(out_idx, local_idx) in local_pairs {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if local_idx < *len {
                                        vals[out_idx] =
                                            Some((data[local_idx / 8] >> (local_idx % 8)) & 1 == 1);
                                    }
                                }
                            }
                        }
                        ColBuf::Str(b) => match &col_data {
                            ColumnData::String { offsets, data } => {
                                let cnt = offsets.len().saturating_sub(1);
                                for &(_, local_idx) in local_pairs {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        b.append_null();
                                        continue;
                                    }
                                    if local_idx < cnt {
                                        let s = offsets[local_idx] as usize;
                                        let e = offsets[local_idx + 1] as usize;
                                        b.append_value(
                                            std::str::from_utf8(&data[s..e]).unwrap_or(""),
                                        );
                                    } else {
                                        b.append_null();
                                    }
                                }
                            }
                            ColumnData::StringDict {
                                indices: idx_arr,
                                dict_offsets,
                                dict_data,
                                ..
                            } => {
                                for &(_, local_idx) in local_pairs {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        b.append_null();
                                        continue;
                                    }
                                    if local_idx < idx_arr.len() {
                                        let di = idx_arr[local_idx];
                                        if di == 0 {
                                            b.append_null();
                                            continue;
                                        }
                                        let d = (di - 1) as usize;
                                        if d < dict_offsets.len() {
                                            let s = dict_offsets[d] as usize;
                                            let e = if d + 1 < dict_offsets.len() {
                                                dict_offsets[d + 1] as usize
                                            } else {
                                                dict_data.len()
                                            };
                                            b.append_value(
                                                std::str::from_utf8(
                                                    &dict_data[s..e.min(dict_data.len())],
                                                )
                                                .unwrap_or(""),
                                            );
                                        } else {
                                            b.append_null();
                                        }
                                    } else {
                                        b.append_null();
                                    }
                                }
                            }
                            _ => {
                                for _ in local_pairs {
                                    b.append_null();
                                }
                            }
                        },
                        ColBuf::Bin(vals) => {
                            if let ColumnData::Binary { offsets, data } = &col_data {
                                let cnt = offsets.len().saturating_sub(1);
                                for &(out_idx, local_idx) in local_pairs {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if local_idx < cnt {
                                        let s = offsets[local_idx] as usize;
                                        let e = offsets[local_idx + 1] as usize;
                                        vals[out_idx] = Some(data[s..e].to_vec());
                                    }
                                }
                            }
                        }
                        ColBuf::FixedVec(vals, ref mut dim_out) => match &col_data {
                            ColumnData::FixedList { data, dim } => {
                                *dim_out = *dim;
                                let d = *dim as usize;
                                let row_bytes = d * 4;
                                let row_count = if d == 0 { 0 } else { data.len() / row_bytes };
                                for &(out_idx, local_idx) in local_pairs {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if local_idx < row_count {
                                        let s = local_idx * row_bytes;
                                        vals[out_idx] = Some(data[s..s + row_bytes].to_vec());
                                    }
                                }
                            }
                            ColumnData::Float16List { data, dim } => {
                                *dim_out = *dim;
                                let d = *dim as usize;
                                let row_bytes_f16 = d * 2;
                                let row_count = if d == 0 {
                                    0
                                } else {
                                    data.len() / row_bytes_f16
                                };
                                for &(out_idx, local_idx) in local_pairs {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if local_idx < row_count {
                                        let s = local_idx * row_bytes_f16;
                                        let f16_bytes = &data[s..s + row_bytes_f16];
                                        let f32_bytes: Vec<u8> = f16_bytes
                                            .chunks_exact(2)
                                            .flat_map(|c| {
                                                crate::storage::on_demand::f16_to_f32(
                                                    u16::from_le_bytes(c.try_into().unwrap()),
                                                )
                                                .to_le_bytes()
                                            })
                                            .collect();
                                        vals[out_idx] = Some(f32_bytes);
                                    }
                                }
                            }
                            ColumnData::QuantizedList { data, dim, codec } => {
                                *dim_out = *dim;
                                let d = *dim as usize;
                                let row_bytes = codec.row_width(d)?;
                                let row_count = if row_bytes == 0 {
                                    0
                                } else {
                                    data.len() / row_bytes
                                };
                                let mut decoded = vec![0.0f32; d];
                                for &(out_idx, local_idx) in local_pairs {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if local_idx < row_count {
                                        let start = local_idx * row_bytes;
                                        crate::compute::vector_quantization::decode_vector(
                                            *codec,
                                            &data[start..start + row_bytes],
                                            d,
                                            &mut decoded,
                                        )?;
                                        let mut bytes = Vec::with_capacity(d * 4);
                                        for &value in &decoded {
                                            bytes.extend_from_slice(&value.to_le_bytes());
                                        }
                                        vals[out_idx] = Some(bytes);
                                    }
                                }
                            }
                            _ => {}
                        },
                        ColBuf::Other(vals) => {
                            for &(out_idx, local_idx) in local_pairs {
                                if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                    continue;
                                }
                                let val = match &col_data {
                                    ColumnData::Int64(v) => {
                                        if local_idx < v.len() {
                                            Some(crate::data::Value::Int64(v[local_idx]))
                                        } else {
                                            None
                                        }
                                    }
                                    ColumnData::Float64(v) => {
                                        if local_idx < v.len() {
                                            Some(crate::data::Value::Float64(v[local_idx]))
                                        } else {
                                            None
                                        }
                                    }
                                    ColumnData::String { offsets, data } => {
                                        let cnt = offsets.len().saturating_sub(1);
                                        if local_idx < cnt {
                                            let s = offsets[local_idx] as usize;
                                            let e = offsets[local_idx + 1] as usize;
                                            Some(crate::data::Value::String(
                                                std::str::from_utf8(&data[s..e])
                                                    .unwrap_or("")
                                                    .to_string(),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    _ => None,
                                };
                                if let Some(v) = val {
                                    vals[out_idx] = Some(v);
                                }
                            }
                        }
                    }
                }

                if rcix.is_none() {
                    let consumed = if encoding_version >= 1 {
                        skip_column_encoded(&body[pos..], ct)?
                    } else {
                        ColumnData::skip_bytes_typed(&body[pos..], ct)?
                    };
                    pos += consumed;
                }
            }
        }

        drop(mmap_guard);
        drop(file_guard);

        // Build Arrow RecordBatch directly from typed ColBuf — no extra copies
        let mut fields: Vec<Field> = Vec::with_capacity(col_count + 1);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_count + 1);

        let include_id = col_refs
            .map(|refs| refs.iter().any(|r| r.eq_ignore_ascii_case("_id")))
            .unwrap_or(true);
        if include_id {
            fields.push(Field::new("_id", ArrowDataType::Int64, false));
            arrays.push(Arc::new(Int64Array::from(out_ids)));
        }

        for (ci, buf) in col_bufs.into_iter().enumerate() {
            if !col_needed[ci] {
                continue;
            }
            let col_name = &schema.columns[ci].0;
            let ct = schema.columns[ci].1;
            match buf {
                ColBuf::I64(vals) => {
                    fields.push(Field::new(col_name, ArrowDataType::Int64, true));
                    arrays.push(Arc::new(Int64Array::from(vals)));
                }
                ColBuf::F64(vals) => {
                    fields.push(Field::new(col_name, ArrowDataType::Float64, true));
                    arrays.push(Arc::new(Float64Array::from(vals)));
                }
                ColBuf::Str(mut b) => {
                    fields.push(Field::new(col_name, ArrowDataType::Utf8, true));
                    arrays.push(Arc::new(b.finish()) as ArrayRef);
                }
                ColBuf::Bool(vals) => {
                    let arr: BooleanArray = vals.into_iter().collect();
                    fields.push(Field::new(col_name, ArrowDataType::Boolean, true));
                    arrays.push(Arc::new(arr));
                }
                ColBuf::Bin(vals) => {
                    if matches!(ct, ColumnType::Blob) {
                        use arrow::array::LargeBinaryArray;
                        let mut payloads = Vec::with_capacity(vals.len());
                        for descriptor in vals {
                            payloads.push(
                                descriptor
                                    .as_deref()
                                    .map(|bytes| self.read_blob_value(bytes))
                                    .transpose()?,
                            );
                        }
                        let refs = payloads.iter().map(|value| value.as_deref()).collect::<Vec<_>>();
                        fields.push(Field::new(col_name, ArrowDataType::LargeBinary, true));
                        arrays.push(Arc::new(LargeBinaryArray::from(refs)) as ArrayRef);
                    } else {
                        use arrow::array::BinaryArray;
                        let bin_data: Vec<Option<&[u8]>> =
                            vals.iter().map(|value| value.as_deref()).collect();
                        fields.push(Field::new(col_name, ArrowDataType::Binary, true));
                        arrays.push(Arc::new(BinaryArray::from(bin_data)) as ArrayRef);
                    }
                }
                ColBuf::FixedVec(vals, dim) => {
                    let d = dim as usize;
                    if d > 0 {
                        let mut all_f32: Vec<f32> = Vec::with_capacity(n_out * d);
                        let mut null_bits: Vec<bool> = Vec::with_capacity(n_out);
                        for v in &vals {
                            match v {
                                Some(bytes) if bytes.len() == d * 4 => {
                                    null_bits.push(true);
                                    for chunk in bytes.chunks_exact(4) {
                                        all_f32.push(f32::from_le_bytes(chunk.try_into().unwrap()));
                                    }
                                }
                                _ => {
                                    null_bits.push(false);
                                    all_f32.extend(std::iter::repeat(0.0f32).take(d));
                                }
                            }
                        }
                        use arrow::array::{FixedSizeListArray, Float32Array};
                        use arrow::datatypes::Field as ArrowField;
                        let float_arr = Float32Array::from(all_f32);
                        let item_field =
                            Arc::new(ArrowField::new("item", ArrowDataType::Float32, false));
                        let null_buf: Option<arrow::buffer::NullBuffer> =
                            if null_bits.iter().any(|b| !b) {
                                Some(arrow::buffer::NullBuffer::from(
                                    null_bits.iter().map(|&b| b).collect::<Vec<bool>>(),
                                ))
                            } else {
                                None
                            };
                        let list_arr = FixedSizeListArray::new(
                            item_field.clone(),
                            d as i32,
                            Arc::new(float_arr),
                            null_buf,
                        );
                        let list_dt = ArrowDataType::FixedSizeList(item_field, d as i32);
                        fields.push(Field::new(col_name, list_dt, true));
                        arrays.push(Arc::new(list_arr) as ArrayRef);
                    } else {
                        fields.push(Field::new(col_name, ArrowDataType::Utf8, true));
                        arrays.push(
                            Arc::new(StringArray::from(vec![None::<&str>; n_out])) as ArrayRef
                        );
                    }
                }
                ColBuf::Other(vals) => {
                    let mut b = StringBuilder::with_capacity(n_out, n_out * 8);
                    for v in vals {
                        match v {
                            Some(crate::data::Value::String(s)) => b.append_value(&s),
                            Some(crate::data::Value::Int64(n)) => b.append_value(&n.to_string()),
                            _ => b.append_null(),
                        }
                    }
                    let dt = match ct {
                        _ => ArrowDataType::Utf8,
                    };
                    fields.push(Field::new(col_name, dt, true));
                    arrays.push(Arc::new(b.finish()) as ArrayRef);
                }
            }
        }

        let batch_schema = Arc::new(Schema::new(fields));
        arrow::record_batch::RecordBatch::try_new(batch_schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
            .map(Some)
    }

    pub(crate) fn extract_rows_by_indices_mmap_columns(
        &self,
        indices: &[usize],
        col_refs: Option<&[&str]>,
    ) -> io::Result<Option<MmapBatchColumns>> {
        if indices.is_empty() {
            return Ok(Some(MmapBatchColumns {
                row_count: 0,
                columns: Vec::new(),
            }));
        }

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let col_count = schema.column_count();

        let include_id = col_refs
            .map(|refs| refs.iter().any(|r| r.eq_ignore_ascii_case("_id")))
            .unwrap_or(true);
        let col_needed: Vec<bool> = if let Some(refs) = col_refs {
            schema
                .columns
                .iter()
                .map(|(name, _)| refs.iter().any(|r| r.eq_ignore_ascii_case(name)))
                .collect()
        } else {
            vec![true; col_count]
        };
        if let Some(refs) = col_refs {
            for requested in refs {
                if !requested.eq_ignore_ascii_case("_id") && schema.get_index(requested).is_none() {
                    return Ok(None);
                }
            }
        }

        let mut cumulative = 0usize;
        let rg_bounds: Vec<(usize, usize)> = footer
            .row_groups
            .iter()
            .map(|rg| {
                let start = cumulative;
                cumulative += rg.row_count as usize;
                (start, cumulative)
            })
            .collect();

        let n_rows = indices.len();
        let mut rg_local_indices: Vec<Vec<(usize, usize)>> =
            vec![Vec::new(); footer.row_groups.len()];
        for (out_idx, &global_idx) in indices.iter().enumerate() {
            let rg_i = rg_bounds.partition_point(|&(_, end)| end <= global_idx);
            if rg_i < footer.row_groups.len() {
                rg_local_indices[rg_i].push((out_idx, global_idx - rg_bounds[rg_i].0));
            }
        }

        for (rg_i, local_pairs) in rg_local_indices.iter().enumerate() {
            if local_pairs.is_empty() {
                continue;
            }
            if rg_i >= footer.col_offsets.len() {
                return Ok(None);
            }
        }

        enum ColBuf {
            I64(Vec<Option<i64>>),
            F64(Vec<Option<f64>>),
            Str(Vec<Option<String>>),
            Bool(Vec<Option<bool>>),
            Bin(Vec<Option<Vec<u8>>>),
        }

        let mut found_mask = vec![false; n_rows];
        let mut out_ids = vec![0i64; n_rows];
        let mut col_bufs: Vec<ColBuf> = schema
            .columns
            .iter()
            .enumerate()
            .map(|(ci, (_, ct))| {
                if !col_needed[ci] {
                    return ColBuf::I64(Vec::new());
                }
                match ct {
                    ColumnType::Int64
                    | ColumnType::Int8
                    | ColumnType::Int16
                    | ColumnType::Int32
                    | ColumnType::UInt8
                    | ColumnType::UInt16
                    | ColumnType::UInt32
                    | ColumnType::UInt64
                    | ColumnType::Timestamp
                    | ColumnType::Date => ColBuf::I64(vec![None; n_rows]),
                    ColumnType::Float64 | ColumnType::Float32 => ColBuf::F64(vec![None; n_rows]),
                    ColumnType::String | ColumnType::StringDict => ColBuf::Str(vec![None; n_rows]),
                    ColumnType::Bool => ColBuf::Bool(vec![None; n_rows]),
                    ColumnType::Binary => ColBuf::Bin(vec![None; n_rows]),
                    _ => ColBuf::Str(vec![None; n_rows]),
                }
            })
            .collect();

        let file_guard = self.file.read();
        let file = match file_guard.as_ref() {
            Some(f) => f,
            None => return Ok(None),
        };
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        for (rg_i, local_pairs) in rg_local_indices.iter().enumerate() {
            if local_pairs.is_empty() {
                continue;
            }
            let rg_meta = &footer.row_groups[rg_i];
            let rg_rows = rg_meta.row_count as usize;
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_not_conn("RG extends past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.len() < 32 {
                return Ok(None);
            }
            let compress_flag = rg_bytes[28];
            let encoding_version = rg_bytes[29];
            if compress_flag != RG_COMPRESS_NONE || encoding_version < 1 {
                return Ok(None);
            }

            let body = &rg_bytes[32..];
            let null_bitmap_len = (rg_rows + 7) / 8;
            let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
            let ids_section_len = rg_id_section_len(rg_rows, id_encoding);
            if ids_section_len + null_bitmap_len > body.len() {
                return Ok(None);
            }
            let del_bytes = &body[ids_section_len..ids_section_len + null_bitmap_len];
            let has_deletes = rg_meta.deletion_count > 0;
            let rcix = &footer.col_offsets[rg_i];

            let mut valid_pairs: Vec<(usize, usize)> = Vec::with_capacity(local_pairs.len());
            for &(out_idx, local_idx) in local_pairs {
                if local_idx >= rg_rows {
                    continue;
                }
                if has_deletes && (del_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                    continue;
                }
                let Some(id) = rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, local_idx)
                else {
                    continue;
                };
                found_mask[out_idx] = true;
                out_ids[out_idx] = id as i64;
                valid_pairs.push((out_idx, local_idx));
            }
            if valid_pairs.is_empty() {
                continue;
            }

            for ci in 0..col_count {
                if !col_needed[ci] {
                    continue;
                }
                // Schema evolution (footer-only ADD COLUMN): RGs predating the
                // new column have no data — the output buffer already holds
                // all-NULL values, so skip and leave them NULL.
                if ci >= rcix.len() {
                    continue;
                }
                let ct = schema.columns[ci].1;
                let col_off = rcix[ci] as usize;
                if col_off + null_bitmap_len > body.len() {
                    return Ok(None);
                }
                let null_bytes = &body[col_off..col_off + null_bitmap_len];
                let col_bytes = &body[col_off + null_bitmap_len..];
                if col_bytes.is_empty() {
                    return Ok(None);
                }
                let encoding = col_bytes[0];
                let data_bytes = &col_bytes[1..];

                let extracted = match (encoding, ct, &mut col_bufs[ci]) {
                    (
                        COL_ENCODING_PLAIN,
                        ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date,
                        ColBuf::I64(vals),
                    ) if data_bytes.len() >= 8 => {
                        for &(out_idx, local_idx) in &valid_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            let off = 8 + local_idx * 8;
                            if off + 8 <= data_bytes.len() {
                                vals[out_idx] = Some(i64::from_le_bytes(
                                    data_bytes[off..off + 8].try_into().unwrap(),
                                ));
                            }
                        }
                        true
                    }
                    (
                        COL_ENCODING_PLAIN,
                        ColumnType::Float64 | ColumnType::Float32,
                        ColBuf::F64(vals),
                    ) if data_bytes.len() >= 8 => {
                        for &(out_idx, local_idx) in &valid_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            let off = 8 + local_idx * 8;
                            if off + 8 <= data_bytes.len() {
                                vals[out_idx] = Some(f64::from_le_bytes(
                                    data_bytes[off..off + 8].try_into().unwrap(),
                                ));
                            }
                        }
                        true
                    }
                    (COL_ENCODING_PLAIN, ColumnType::String, ColBuf::Str(vals))
                        if data_bytes.len() >= 8 =>
                    {
                        let count =
                            u64::from_le_bytes(data_bytes[0..8].try_into().unwrap()) as usize;
                        let data_len_off = 8 + (count + 1) * 4;
                        if data_len_off + 8 > data_bytes.len() {
                            return Ok(None);
                        }
                        let data_start = data_len_off + 8;
                        for &(out_idx, local_idx) in &valid_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1
                                || local_idx >= count
                            {
                                continue;
                            }
                            let s_off = 8 + local_idx * 4;
                            let e_off = s_off + 4;
                            if e_off + 4 <= data_bytes.len() {
                                let s = u32::from_le_bytes(
                                    data_bytes[s_off..s_off + 4].try_into().unwrap(),
                                ) as usize;
                                let e = u32::from_le_bytes(
                                    data_bytes[e_off..e_off + 4].try_into().unwrap(),
                                ) as usize;
                                if data_start + e <= data_bytes.len() {
                                    vals[out_idx] = Some(
                                        std::str::from_utf8(
                                            &data_bytes[data_start + s..data_start + e],
                                        )
                                        .unwrap_or("")
                                        .to_string(),
                                    );
                                }
                            }
                        }
                        true
                    }
                    (
                        encoding @ (COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY),
                        ColumnType::StringDict,
                        ColBuf::Str(vals),
                    ) => {
                        let view = StringDictView::parse(
                            data_bytes,
                            encoding == COL_ENCODING_COMPACT_DICTIONARY,
                        )?;
                        for &(out_idx, local_idx) in &valid_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            if let Some(value) =
                                view.index(local_idx).and_then(|index| view.value(index))
                            {
                                vals[out_idx] =
                                    Some(std::str::from_utf8(value).unwrap_or("").to_string());
                            }
                        }
                        true
                    }
                    (
                        COL_ENCODING_BITPACK,
                        ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date,
                        ColBuf::I64(vals),
                    ) => {
                        for &(out_idx, local_idx) in &valid_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            if let Some(v) = crate::storage::on_demand::bitpack_decode_at_idx(
                                data_bytes, local_idx,
                            ) {
                                vals[out_idx] = Some(v);
                            }
                        }
                        true
                    }
                    (COL_ENCODING_PLAIN, ColumnType::Bool, ColBuf::Bool(vals))
                        if data_bytes.len() >= 8 =>
                    {
                        for &(out_idx, local_idx) in &valid_pairs {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            let byte_off = 8 + local_idx / 8;
                            if byte_off < data_bytes.len() {
                                vals[out_idx] =
                                    Some((data_bytes[byte_off] >> (local_idx % 8)) & 1 == 1);
                            }
                        }
                        true
                    }
                    _ => false,
                };
                if !extracted {
                    return Ok(None);
                }
            }
        }

        drop(mmap_guard);
        drop(file_guard);

        let n_out = found_mask.iter().filter(|&&b| b).count();
        if n_out == 0 {
            return Ok(Some(MmapBatchColumns {
                row_count: 0,
                columns: Vec::new(),
            }));
        }
        let all_found = n_out == n_rows;

        let mut columns = Vec::with_capacity(col_count + usize::from(include_id));
        if include_id {
            columns.push((
                "_id".to_string(),
                MmapBatchColumn::I64(if all_found {
                    out_ids.into_iter().map(Some).collect()
                } else {
                    out_ids
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, value)| found_mask[i].then_some(Some(value)))
                        .collect()
                }),
            ));
        }

        for (ci, buf) in col_bufs.into_iter().enumerate() {
            if !col_needed[ci] {
                continue;
            }
            let name = schema.columns[ci].0.clone();
            match buf {
                ColBuf::I64(vals) => columns.push((
                    name,
                    MmapBatchColumn::I64(if all_found {
                        vals
                    } else {
                        vals.into_iter()
                            .enumerate()
                            .filter_map(|(i, value)| found_mask[i].then_some(value))
                            .collect()
                    }),
                )),
                ColBuf::F64(vals) => columns.push((
                    name,
                    MmapBatchColumn::F64(if all_found {
                        vals
                    } else {
                        vals.into_iter()
                            .enumerate()
                            .filter_map(|(i, value)| found_mask[i].then_some(value))
                            .collect()
                    }),
                )),
                ColBuf::Str(vals) => columns.push((
                    name,
                    MmapBatchColumn::Str(if all_found {
                        vals
                    } else {
                        vals.into_iter()
                            .enumerate()
                            .filter_map(|(i, value)| found_mask[i].then_some(value))
                            .collect()
                    }),
                )),
                ColBuf::Bool(vals) => columns.push((
                    name,
                    MmapBatchColumn::Bool(if all_found {
                        vals
                    } else {
                        vals.into_iter()
                            .enumerate()
                            .filter_map(|(i, value)| found_mask[i].then_some(value))
                            .collect()
                    }),
                )),
                ColBuf::Bin(vals) => columns.push((
                    name,
                    MmapBatchColumn::Bin(if all_found {
                        vals
                    } else {
                        vals.into_iter()
                            .enumerate()
                            .filter_map(|(i, value)| found_mask[i].then_some(value))
                            .collect()
                    }),
                )),
            }
        }

        Ok(Some(MmapBatchColumns {
            row_count: n_out,
            columns,
        }))
    }

    fn retrieve_many_mmap_columns_visible(
        &self,
        ids: &[u64],
    ) -> io::Result<Option<MmapBatchColumns>> {
        let active_ids = {
            let delta_store = self.delta_store.read();
            if delta_store.delete_count() == 0 {
                drop(delta_store);
                return self.retrieve_many_mmap_columns(ids);
            }
            let deletes = delta_store.delete_bitmap();
            ids.iter()
                .copied()
                .filter(|id| !deletes.is_deleted(*id))
                .collect::<Vec<_>>()
        };
        self.retrieve_many_mmap_columns(&active_ids)
    }

    /// Decode persisted rows. Callers must prove there are no pending delta
    /// deletions or apply a delta overlay before exposing the result.
    pub(crate) fn retrieve_many_mmap_columns(
        &self,
        ids: &[u64],
    ) -> io::Result<Option<MmapBatchColumns>> {
        self.retrieve_many_mmap_columns_impl(ids, None)
    }

    /// Decode persisted rows for a projection. Only the requested columns are
    /// staged and decoded, which keeps small projected point/batch reads fast
    /// and avoids building unrequested column data.
    pub fn retrieve_many_mmap_columns_projected(
        &self,
        ids: &[u64],
        columns: &[String],
    ) -> io::Result<Option<MmapBatchColumns>> {
        self.retrieve_many_mmap_columns_impl(ids, Some(columns))
    }

    fn retrieve_many_mmap_columns_impl(
        &self,
        ids: &[u64],
        columns: Option<&[String]>,
    ) -> io::Result<Option<MmapBatchColumns>> {
        if ids.is_empty() {
            return Ok(Some(MmapBatchColumns {
                row_count: 0,
                columns: Vec::new(),
            }));
        }

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let col_count = schema.column_count();
        let n_ids = ids.len();
        let col_indices: Vec<usize> = match columns {
            Some(requested) => {
                let mut indices = Vec::with_capacity(requested.len());
                for name in requested {
                    let Some(index) = schema.get_index(name) else {
                        return Ok(None);
                    };
                    indices.push(index);
                }
                indices
            }
            None => (0..col_count).collect(),
        };

        // ── Step 1: Map each input ID → rg_i (one footer read, no per-ID lock) ─
        let non_empty_row_groups: Vec<(u64, u64, usize)> = footer
            .row_groups
            .iter()
            .enumerate()
            .filter_map(|(rg_i, rg)| (rg.row_count > 0).then_some((rg.min_id, rg.max_id, rg_i)))
            .collect();
        let mut rg_map: Vec<Vec<(usize, u64)>> = vec![Vec::new(); footer.row_groups.len()];
        if !non_empty_row_groups.is_empty() {
            let ids_are_sorted = ids.windows(2).all(|pair| pair[0] <= pair[1]);
            if ids_are_sorted {
                let mut rg_pos = 0usize;
                for (out_pos, &id) in ids.iter().enumerate() {
                    while rg_pos < non_empty_row_groups.len() && non_empty_row_groups[rg_pos].1 < id
                    {
                        rg_pos += 1;
                    }
                    if let Some(&(min_id, max_id, rg_i)) = non_empty_row_groups.get(rg_pos) {
                        if min_id <= id && id <= max_id {
                            rg_map[rg_i].push((out_pos, id));
                        }
                    }
                }
            } else {
                for (out_pos, &id) in ids.iter().enumerate() {
                    let rg_pos =
                        non_empty_row_groups.partition_point(|(_, max_id, _)| *max_id < id);
                    if let Some(&(min_id, max_id, rg_i)) = non_empty_row_groups.get(rg_pos) {
                        if min_id <= id && id <= max_id {
                            rg_map[rg_i].push((out_pos, id));
                        }
                    }
                }
            }
        }

        // Upfront check: all needed RGs must have RCIX (encoding_version >= 1 + col_offsets)
        for (rg_i, hits) in rg_map.iter().enumerate() {
            if hits.is_empty() {
                continue;
            }
            if rg_i >= footer.col_offsets.len() || footer.col_offsets[rg_i].len() < col_count {
                return Ok(None);
            }
        }

        // ── Step 2: Allocate per-column staging buffers indexed by out_pos ──────
        // Vec<Option<T>> allows random-access writes so we can build output in ID order.
        let mut found_mask: Vec<bool> = vec![false; n_ids];
        let mut out_ids: Vec<i64> = vec![0i64; n_ids];

        enum ColBuf {
            I64(Vec<Option<i64>>),
            F64(Vec<Option<f64>>),
            Str(Vec<Option<String>>),
            Bool(Vec<Option<bool>>),
            Bin(Vec<Option<Vec<u8>>>),
        }

        let mut col_bufs: Vec<ColBuf> = col_indices
            .iter()
            .map(|&ci| match schema.columns[ci].1 {
                ColumnType::Int64
                | ColumnType::Int8
                | ColumnType::Int16
                | ColumnType::Int32
                | ColumnType::UInt8
                | ColumnType::UInt16
                | ColumnType::UInt32
                | ColumnType::UInt64
                | ColumnType::Timestamp
                | ColumnType::Date => ColBuf::I64(vec![None; n_ids]),
                ColumnType::Float64 | ColumnType::Float32 => ColBuf::F64(vec![None; n_ids]),
                ColumnType::String | ColumnType::StringDict => ColBuf::Str(vec![None; n_ids]),
                ColumnType::Bool => ColBuf::Bool(vec![None; n_ids]),
                ColumnType::Binary => ColBuf::Bin(vec![None; n_ids]),
                _ => ColBuf::Str(vec![None; n_ids]), // rare types as string fallback
            })
            .collect();

        // ── Step 3: Acquire mmap once, then process each RG in a single body slice
        let file_guard = self.file.read();
        let file = match file_guard.as_ref() {
            Some(f) => f,
            None => return Ok(None),
        };
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        for (rg_i, hits) in rg_map.iter().enumerate() {
            if hits.is_empty() {
                continue;
            }
            let rg_meta = &footer.row_groups[rg_i];
            let rg_rows = rg_meta.row_count as usize;
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_not_conn("RG extends past EOF"));
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

            let decompressed = decompress_rg_body(compress_flag, &rg_bytes[32..])?;
            let body: &[u8] = decompressed.as_deref().unwrap_or(&rg_bytes[32..]);
            let null_bitmap_len = (rg_rows + 7) / 8;
            let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
            let ids_section_len = rg_id_section_len(rg_rows, id_encoding);
            if ids_section_len > body.len() {
                continue;
            }

            let rcix = &footer.col_offsets[rg_i]; // already validated above

            // Resolve local_idx for each ID in this RG (O(1) guess + optional binary search)
            let mut valid_hits: Vec<(usize, usize)> = Vec::with_capacity(hits.len()); // (out_pos, local_idx)
            let mut ids_cow_cache = None;
            for &(out_pos, id) in hits {
                let guess = id.saturating_sub(rg_meta.min_id) as usize;
                let local_idx = if guess < rg_rows {
                    if let Some(stored) =
                        rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, guess)
                    {
                        if stored == id {
                            guess
                        } else {
                            let ids_cow = ids_cow_cache.get_or_insert_with(|| {
                                bytes_as_u64_slice(&body[..ids_section_len], rg_rows)
                            });
                            match ids_cow.binary_search(&id) {
                                Ok(i) => i,
                                Err(_) => continue,
                            }
                        }
                    } else {
                        continue;
                    }
                } else {
                    let ids_cow = ids_cow_cache.get_or_insert_with(|| {
                        bytes_as_u64_slice(&body[..ids_section_len], rg_rows)
                    });
                    match ids_cow.binary_search(&id) {
                        Ok(i) => i,
                        Err(_) => continue,
                    }
                };

                // Deletion check
                let del_off = ids_section_len + local_idx / 8;
                if del_off >= body.len() {
                    continue;
                }
                if (body[del_off] >> (local_idx % 8)) & 1 == 1 {
                    continue;
                }

                found_mask[out_pos] = true;
                out_ids[out_pos] = id as i64;
                valid_hits.push((out_pos, local_idx));
            }

            if valid_hits.is_empty() {
                continue;
            }

            // Extract column values for the requested projection only
            for (buf_pos, &ci) in col_indices.iter().enumerate() {
                let ct = schema.columns[ci].1;
                let col_off = rcix[ci] as usize;
                if col_off + null_bitmap_len > body.len() {
                    continue;
                }
                let null_bytes = &body[col_off..col_off + null_bitmap_len];
                let col_bytes = &body[col_off + null_bitmap_len..];

                if col_bytes.is_empty() {
                    continue;
                }
                let encoding = col_bytes[0];
                let data_bytes = &col_bytes[1..];

                let extracted = match (encoding, ct, &mut col_bufs[buf_pos]) {
                    (
                        COL_ENCODING_PLAIN,
                        ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date,
                        ColBuf::I64(vals),
                    ) if data_bytes.len() >= 8 => {
                        for &(out_pos, local_idx) in &valid_hits {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            let off = 8 + local_idx * 8;
                            if off + 8 <= data_bytes.len() {
                                vals[out_pos] = Some(i64::from_le_bytes(
                                    data_bytes[off..off + 8].try_into().unwrap(),
                                ));
                            }
                        }
                        true
                    }
                    (
                        COL_ENCODING_PLAIN,
                        ColumnType::Float64 | ColumnType::Float32,
                        ColBuf::F64(vals),
                    ) if data_bytes.len() >= 8 => {
                        for &(out_pos, local_idx) in &valid_hits {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            let off = 8 + local_idx * 8;
                            if off + 8 <= data_bytes.len() {
                                vals[out_pos] = Some(f64::from_le_bytes(
                                    data_bytes[off..off + 8].try_into().unwrap(),
                                ));
                            }
                        }
                        true
                    }
                    (
                        COL_ENCODING_FLOAT_DICTIONARY,
                        ColumnType::Float64 | ColumnType::Float32,
                        ColBuf::F64(vals),
                    ) => {
                        let view = FloatDictView::parse(data_bytes)?;
                        for &(out_pos, local_idx) in &valid_hits {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            vals[out_pos] = view.value(local_idx);
                        }
                        true
                    }
                    (COL_ENCODING_PLAIN, ColumnType::String, ColBuf::Str(vals))
                        if data_bytes.len() >= 8 =>
                    {
                        let count =
                            u64::from_le_bytes(data_bytes[0..8].try_into().unwrap()) as usize;
                        let data_len_off = 8 + (count + 1) * 4;
                        if data_len_off + 8 > data_bytes.len() {
                            continue;
                        }
                        let data_start = data_len_off + 8;
                        for &(out_pos, local_idx) in &valid_hits {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            if local_idx >= count {
                                continue;
                            }
                            let s_off = 8 + local_idx * 4;
                            let e_off = s_off + 4;
                            if e_off + 4 <= data_bytes.len() {
                                let s = u32::from_le_bytes(
                                    data_bytes[s_off..s_off + 4].try_into().unwrap(),
                                ) as usize;
                                let e = u32::from_le_bytes(
                                    data_bytes[e_off..e_off + 4].try_into().unwrap(),
                                ) as usize;
                                if data_start + e <= data_bytes.len() {
                                    vals[out_pos] = Some(
                                        std::str::from_utf8(
                                            &data_bytes[data_start + s..data_start + e],
                                        )
                                        .unwrap_or("")
                                        .to_string(),
                                    );
                                }
                            }
                        }
                        true
                    }
                    (
                        encoding @ (COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY),
                        ColumnType::StringDict,
                        ColBuf::Str(vals),
                    ) => {
                        let view = StringDictView::parse(
                            data_bytes,
                            encoding == COL_ENCODING_COMPACT_DICTIONARY,
                        )?;
                        for &(out_pos, local_idx) in &valid_hits {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            if let Some(value) =
                                view.index(local_idx).and_then(|index| view.value(index))
                            {
                                vals[out_pos] =
                                    Some(std::str::from_utf8(value).unwrap_or("").to_string());
                            }
                        }
                        true
                    }
                    (
                        2u8,
                        ColumnType::Int64
                        | ColumnType::Int8
                        | ColumnType::Int16
                        | ColumnType::Int32
                        | ColumnType::UInt8
                        | ColumnType::UInt16
                        | ColumnType::UInt32
                        | ColumnType::UInt64
                        | ColumnType::Timestamp
                        | ColumnType::Date,
                        ColBuf::I64(vals),
                    ) => {
                        for &(out_pos, local_idx) in &valid_hits {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            if let Some(v) = crate::storage::on_demand::bitpack_decode_at_idx(
                                data_bytes, local_idx,
                            ) {
                                vals[out_pos] = Some(v);
                            }
                        }
                        true
                    }
                    (COL_ENCODING_PLAIN, ColumnType::Bool, ColBuf::Bool(vals))
                        if data_bytes.len() >= 8 =>
                    {
                        for &(out_pos, local_idx) in &valid_hits {
                            if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                continue;
                            }
                            let byte_off = 8 + local_idx / 8;
                            if byte_off < data_bytes.len() {
                                vals[out_pos] =
                                    Some((data_bytes[byte_off] >> (local_idx % 8)) & 1 == 1);
                            }
                        }
                        true
                    }
                    _ => false,
                };

                if !extracted {
                    // Fallback: full column decode, pick values at target indices
                    let (col_data, _) = read_column_encoded(col_bytes, ct)?;
                    match &mut col_bufs[buf_pos] {
                        ColBuf::I64(vals) => {
                            if let ColumnData::Int64(v) = &col_data {
                                for &(out_pos, local_idx) in &valid_hits {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if local_idx < v.len() {
                                        vals[out_pos] = Some(v[local_idx]);
                                    }
                                }
                            }
                        }
                        ColBuf::F64(vals) => {
                            if let ColumnData::Float64(v) = &col_data {
                                for &(out_pos, local_idx) in &valid_hits {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if local_idx < v.len() {
                                        vals[out_pos] = Some(v[local_idx]);
                                    }
                                }
                            }
                        }
                        ColBuf::Str(vals) => match &col_data {
                            ColumnData::String { offsets, data } => {
                                let cnt = offsets.len().saturating_sub(1);
                                for &(out_pos, local_idx) in &valid_hits {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if local_idx < cnt {
                                        let s = offsets[local_idx] as usize;
                                        let e = offsets[local_idx + 1] as usize;
                                        vals[out_pos] = Some(
                                            std::str::from_utf8(&data[s..e])
                                                .unwrap_or("")
                                                .to_string(),
                                        );
                                    }
                                }
                            }
                            _ => {}
                        },
                        ColBuf::Bin(vals) => {
                            if let ColumnData::Binary { offsets, data } = &col_data {
                                let cnt = offsets.len().saturating_sub(1);
                                for &(out_pos, local_idx) in &valid_hits {
                                    if (null_bytes[local_idx / 8] >> (local_idx % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if local_idx < cnt {
                                        let s = offsets[local_idx] as usize;
                                        let e = offsets[local_idx + 1] as usize;
                                        vals[out_pos] = Some(data[s..e].to_vec());
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        drop(mmap_guard);
        drop(file_guard);

        // ── Step 4: Build native column buffers in input-ID order ─────────────
        let n_out: usize = found_mask.iter().filter(|&&b| b).count();
        if n_out == 0 {
            return Ok(Some(MmapBatchColumns {
                row_count: 0,
                columns: Vec::new(),
            }));
        }

        let mut columns: Vec<(String, MmapBatchColumn)> =
            Vec::with_capacity(col_indices.len() + 1);

        // _id column
        let id_vals: Vec<i64> = (0..n_ids)
            .filter(|&i| found_mask[i])
            .map(|i| out_ids[i])
            .collect();
        columns.push((
            "_id".to_string(),
            MmapBatchColumn::I64(id_vals.into_iter().map(Some).collect()),
        ));

        for (buf_pos, buf) in col_bufs.into_iter().enumerate() {
            let ci = col_indices[buf_pos];
            let col_name = &schema.columns[ci].0;
            match buf {
                ColBuf::I64(vals) => {
                    let v: Vec<Option<i64>> = (0..n_ids)
                        .filter(|&i| found_mask[i])
                        .map(|i| vals[i])
                        .collect();
                    columns.push((col_name.clone(), MmapBatchColumn::I64(v)));
                }
                ColBuf::F64(vals) => {
                    let v: Vec<Option<f64>> = (0..n_ids)
                        .filter(|&i| found_mask[i])
                        .map(|i| vals[i])
                        .collect();
                    columns.push((col_name.clone(), MmapBatchColumn::F64(v)));
                }
                ColBuf::Str(vals) => {
                    let v: Vec<Option<String>> = (0..n_ids)
                        .filter(|&i| found_mask[i])
                        .map(|i| vals[i].clone())
                        .collect();
                    columns.push((col_name.clone(), MmapBatchColumn::Str(v)));
                }
                ColBuf::Bool(vals) => {
                    let v: Vec<Option<bool>> = (0..n_ids)
                        .filter(|&i| found_mask[i])
                        .map(|i| vals[i])
                        .collect();
                    columns.push((col_name.clone(), MmapBatchColumn::Bool(v)));
                }
                ColBuf::Bin(vals) => {
                    let v: Vec<Option<Vec<u8>>> = (0..n_ids)
                        .filter(|&i| found_mask[i])
                        .map(|i| vals[i].clone())
                        .collect();
                    columns.push((col_name.clone(), MmapBatchColumn::Bin(v)));
                }
            }
        }

        Ok(Some(MmapBatchColumns {
            row_count: n_out,
            columns,
        }))
    }

    pub fn retrieve_many_mmap(
        &self,
        ids: &[u64],
    ) -> io::Result<Option<arrow::record_batch::RecordBatch>> {
        let Some(batch_cols) = self.retrieve_many_mmap_columns_visible(ids)? else {
            return Ok(None);
        };
        Self::mmap_batch_columns_to_arrow(batch_cols)
    }

    /// Decode persisted rows without consulting DeltaStore.
    ///
    /// Callers must apply their own delta overlay before exposing the batch.
    pub(crate) fn retrieve_many_mmap_persisted(
        &self,
        ids: &[u64],
    ) -> io::Result<Option<arrow::record_batch::RecordBatch>> {
        let Some(batch_cols) = self.retrieve_many_mmap_columns(ids)? else {
            return Ok(None);
        };
        Self::mmap_batch_columns_to_arrow(batch_cols)
    }

    fn mmap_batch_columns_to_arrow(
        batch_cols: MmapBatchColumns,
    ) -> io::Result<Option<arrow::record_batch::RecordBatch>> {
        use arrow::array::{
            ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
        };
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use std::sync::Arc;

        if batch_cols.row_count == 0 {
            return Ok(Some(arrow::record_batch::RecordBatch::new_empty(Arc::new(
                Schema::empty(),
            ))));
        }

        let mut fields: Vec<Field> = Vec::with_capacity(batch_cols.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(batch_cols.columns.len());
        for (name, col) in batch_cols.columns {
            match col {
                MmapBatchColumn::I64(vals) => {
                    fields.push(Field::new(&name, ArrowDataType::Int64, true));
                    arrays.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
                }
                MmapBatchColumn::F64(vals) => {
                    fields.push(Field::new(&name, ArrowDataType::Float64, true));
                    arrays.push(Arc::new(Float64Array::from(vals)) as ArrayRef);
                }
                MmapBatchColumn::Str(vals) => {
                    fields.push(Field::new(&name, ArrowDataType::Utf8, true));
                    arrays.push(Arc::new(StringArray::from(vals)) as ArrayRef);
                }
                MmapBatchColumn::Bool(vals) => {
                    let arr: BooleanArray = vals.into_iter().collect();
                    fields.push(Field::new(&name, ArrowDataType::Boolean, true));
                    arrays.push(Arc::new(arr) as ArrayRef);
                }
                MmapBatchColumn::Bin(vals) => {
                    let bin_data: Vec<Option<&[u8]>> = vals.iter().map(|o| o.as_deref()).collect();
                    fields.push(Field::new(&name, ArrowDataType::Binary, true));
                    arrays.push(Arc::new(BinaryArray::from(bin_data)) as ArrayRef);
                }
            }
        }

        let batch_schema = Arc::new(Schema::new(fields));
        arrow::record_batch::RecordBatch::try_new(batch_schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
            .map(Some)
    }
}
