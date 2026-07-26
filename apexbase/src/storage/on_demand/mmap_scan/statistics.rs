use super::*;

impl OnDemandStorage {
    // ========================================================================
    // Internal helpers
    // ========================================================================

    // ========================================================================
    // Internal read helpers (mmap-based for cross-platform zero-copy reads)
    // ========================================================================

    pub fn compute_column_stats_mmap(
        &self,
        col_name: &str,
    ) -> io::Result<Option<(u64, f64, f64, f64)>> {
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

        let col_count = schema.column_count();
        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for mmap agg"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let mut total_count: u64 = 0;
        let mut total_sum: f64 = 0.0;
        let mut total_min: f64 = f64::INFINITY;
        let mut total_max: f64 = f64::NEG_INFINITY;

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
            let mut pos =
                rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));
            let del_vec_len = (rg_rows + 7) / 8;
            if pos + del_vec_len > body.len() {
                return Err(err_data("RG del vec truncated"));
            }
            let del_bytes = &body[pos..pos + del_vec_len];
            let has_deletes = rg_meta.deletion_count > 0;
            pos += del_vec_len;

            let null_bitmap_len = (rg_rows + 7) / 8;
            for ci in 0..col_count {
                if pos + null_bitmap_len > body.len() {
                    break;
                }
                let null_bytes = &body[pos..pos + null_bitmap_len];
                pos += null_bitmap_len;

                let ct = schema.columns[ci].1;
                if ci == col_idx {
                    let col_bytes = &body[pos..];
                    let enc_offset = if encoding_version >= 1 { 1 } else { 0 };
                    let encoding = if encoding_version >= 1 {
                        col_bytes[0]
                    } else {
                        COL_ENCODING_PLAIN
                    };

                    if encoding == COL_ENCODING_PLAIN {
                        let data = &col_bytes[enc_offset..];
                        if data.len() >= 8 {
                            let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
                            let values_start = 8usize;
                            if is_int {
                                for i in 0..count.min(rg_rows) {
                                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    let off = values_start + i * 8;
                                    if off + 8 > data.len() {
                                        break;
                                    }
                                    let v =
                                        i64::from_le_bytes(data[off..off + 8].try_into().unwrap())
                                            as f64;
                                    total_count += 1;
                                    total_sum += v;
                                    if v < total_min {
                                        total_min = v;
                                    }
                                    if v > total_max {
                                        total_max = v;
                                    }
                                }
                            } else {
                                for i in 0..count.min(rg_rows) {
                                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    let off = values_start + i * 8;
                                    if off + 8 > data.len() {
                                        break;
                                    }
                                    let v =
                                        f64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                                    if !v.is_nan() {
                                        total_count += 1;
                                        total_sum += v;
                                        if v < total_min {
                                            total_min = v;
                                        }
                                        if v > total_max {
                                            total_max = v;
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        // Encoded column: fallback to full decode
                        let (col_data, _) = if encoding_version >= 1 {
                            read_column_encoded(col_bytes, ct)?
                        } else {
                            ColumnData::from_bytes_typed(col_bytes, ct)?
                        };
                        match &col_data {
                            ColumnData::Int64(vals) => {
                                for (i, &v) in vals.iter().enumerate() {
                                    if has_deletes
                                        && i < rg_rows
                                        && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                    {
                                        continue;
                                    }
                                    if i < rg_rows && (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    let fv = v as f64;
                                    total_count += 1;
                                    total_sum += fv;
                                    if fv < total_min {
                                        total_min = fv;
                                    }
                                    if fv > total_max {
                                        total_max = fv;
                                    }
                                }
                            }
                            ColumnData::Float64(vals) => {
                                for (i, &v) in vals.iter().enumerate() {
                                    if has_deletes
                                        && i < rg_rows
                                        && (del_bytes[i / 8] >> (i % 8)) & 1 == 1
                                    {
                                        continue;
                                    }
                                    if i < rg_rows && (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if !v.is_nan() {
                                        total_count += 1;
                                        total_sum += v;
                                        if v < total_min {
                                            total_min = v;
                                        }
                                        if v > total_max {
                                            total_max = v;
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                // Skip column data to advance pos
                let consumed = if encoding_version >= 1 {
                    skip_column_encoded(&body[pos..], ct)?
                } else {
                    ColumnData::skip_bytes_typed(&body[pos..], ct)?
                };
                pos += consumed;
            }
        }

        if total_count == 0 {
            return Ok(Some((0, 0.0, 0.0, 0.0)));
        }
        Ok(Some((total_count, total_sum, total_min, total_max)))
    }
}
