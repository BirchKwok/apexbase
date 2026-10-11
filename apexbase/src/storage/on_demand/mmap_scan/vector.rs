use super::*;

impl OnDemandStorage {
    pub fn topk_binary_direct(
        &self,
        col_name: &str,
        computer: &crate::compute::vector_ops::DistanceComputer,
        k: usize,
    ) -> io::Result<Option<Vec<(usize, f32)>>> {
        use crate::compute::vector_ops::topk_heap_on_floats;

        // Direct mmap row offsets match visible Arrow row positions only while
        // no overlay update or deletion is pending.
        if self.has_pending_deltas() {
            return Ok(None);
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
        if schema.columns[col_idx].1 != ColumnType::Binary {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = match file_guard.as_ref() {
            Some(f) => f,
            None => return Ok(None),
        };
        let mmap_arc = self.mmap_cache.write().get_mmap_arc(file)?;
        drop(file_guard);
        let mmap: &[u8] = &mmap_arc;

        // ── PASS 1: validate all RGs, determine dim, compute total rows ──────
        let query_dim = computer.query.len();
        let total_active: usize = footer
            .row_groups
            .iter()
            .map(|rg| rg.active_rows() as usize)
            .sum();
        if total_active == 0 {
            return Ok(Some(vec![]));
        }

        struct RgDesc {
            count: usize,
            dim: usize,
            data_start: usize,
            byte_len: usize,
        }
        let mut rg_descs: Vec<Option<RgDesc>> = Vec::with_capacity(footer.row_groups.len());

        for (rg_idx, rg_meta) in footer.row_groups.iter().enumerate() {
            if rg_meta.row_count == 0 {
                rg_descs.push(None);
                continue;
            }
            let rg_rows = rg_meta.row_count as usize;

            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap.len() {
                return Ok(None);
            }
            let rg_bytes = &mmap[rg_meta.offset as usize..rg_end];
            let compress_flag = if rg_bytes.len() >= 32 {
                rg_bytes[28]
            } else {
                1
            };
            let encoding_ver = if rg_bytes.len() >= 32 {
                rg_bytes[29]
            } else {
                0
            };

            if compress_flag != RG_COMPRESS_NONE
                || encoding_ver < 1
                || rg_meta.deletion_count > 0
                || rg_idx >= footer.col_offsets.len()
                || col_idx >= footer.col_offsets[rg_idx].len()
            {
                return Ok(None);
            }

            let rg_body_abs = (rg_meta.offset + 32) as usize;
            let col_abs = rg_body_abs + footer.col_offsets[rg_idx][col_idx] as usize;
            let null_bm_len = (rg_rows + 7) / 8;
            let data_abs = col_abs + null_bm_len;

            if data_abs + 9 > mmap.len() {
                return Ok(None);
            }
            if mmap[data_abs] != COL_ENCODING_PLAIN {
                return Ok(None);
            }

            let count =
                u64::from_le_bytes(mmap[data_abs + 1..data_abs + 9].try_into().unwrap()) as usize;
            if count == 0 {
                rg_descs.push(None);
                continue;
            }

            let off_base = data_abs + 9;
            if off_base + 8 > mmap.len() {
                return Ok(None);
            }
            let off0 =
                u32::from_le_bytes(mmap[off_base..off_base + 4].try_into().unwrap()) as usize;
            let off1 =
                u32::from_le_bytes(mmap[off_base + 4..off_base + 8].try_into().unwrap()) as usize;
            if off1 <= off0 || (off1 - off0) % 4 != 0 {
                return Ok(None);
            }
            let dim = (off1 - off0) / 4;
            if dim != query_dim {
                return Ok(None);
            }

            // Binary column format: [count:u64][(count+1)*u32 offsets][data_len:u64][data bytes]
            // Must skip the 8-byte data_len field between the offsets array and the float data.
            let data_start = off_base + (count + 1) * 4 + 8;
            let byte_len = count * dim * 4;
            if data_start + byte_len > mmap.len() {
                return Ok(None);
            }
            rg_descs.push(Some(RgDesc {
                count,
                dim,
                data_start,
                byte_len,
            }));
        }

        // ── PASS 2: fill reusable buffer and run ONE topk scan ───────────────
        // scan_buf caches the float data for this column. On repeated queries the
        // data is already present — skip the 512MB mmap→heap copy entirely.
        // Invalidated by invalidate_page_cache() on every write.
        let needed = total_active * query_dim;
        let row_bytes = query_dim * 4;
        let budget = self.scan_buffer_budget_bytes();
        let window_rows = if budget == usize::MAX || row_bytes == 0 {
            total_active
        } else {
            (budget / row_bytes).max(1).min(total_active)
        };
        if window_rows < total_active {
            // Windowed scoring (OOC-6) for the binary/fixed-list lane: this lane declines pending
            // deltas above, so a window is a plain contiguous byte range per row group, and the
            // score call is the same as the f32 lane.
            let mut scratch: Vec<f32> = vec![0.0; window_rows.saturating_mul(query_dim)];
            let mut best: Vec<(usize, f32)> = Vec::with_capacity(k.min(total_active));
            let mut start = 0usize;
            while start < total_active {
                let rows = window_rows.min(total_active - start);
                let mut written = 0usize;
                let mut active_base = 0usize;
                for desc in rg_descs.iter() {
                    let Some(d) = desc else { continue };
                    let rg_end = active_base + d.count;
                    if rg_end <= start || active_base >= start + rows {
                        active_base = rg_end;
                        continue;
                    }
                    let from = start.max(active_base) - active_base;
                    let take = (start + rows).min(rg_end) - start.max(active_base);
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            mmap.as_ptr().add(d.data_start + from * row_bytes),
                            (scratch.as_mut_ptr() as *mut u8).add(written * row_bytes),
                            take * row_bytes,
                        );
                    }
                    written += take;
                    active_base = rg_end;
                }
                if written != rows {
                    return Ok(None);
                }
                let partial =
                    topk_heap_on_floats(&scratch[..rows * query_dim], rows, query_dim, computer, k);
                for (index, distance) in partial {
                    best.push((start + index, distance));
                }
                best.sort_by(|left, right| {
                    left.1
                        .partial_cmp(&right.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                best.truncate(k);
                start += rows;
            }
            return Ok(Some(best));
        }
        let file_size = mmap.len() as u64;
        let mut buf_guard = self.scan_buf.lock().unwrap();
        let cached_size = self
            .scan_buf_file_size
            .load(std::sync::atomic::Ordering::Acquire);
        let col_guard = self.scan_buf_col.lock().unwrap();
        let cache_hit =
            cached_size == file_size && buf_guard.len() >= needed && col_guard.as_str() == col_name;
        drop(col_guard);

        if !cache_hit {
            if buf_guard.capacity() < needed {
                let cur = buf_guard.len();
                buf_guard.reserve(needed - cur);
            }
            unsafe {
                buf_guard.set_len(needed);
            }

            let buf_ptr = buf_guard.as_mut_ptr();
            let mut filled_floats = 0usize;
            for desc in rg_descs.iter() {
                let Some(d) = desc else { continue };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        mmap.as_ptr().add(d.data_start),
                        (buf_ptr as *mut u8).add(filled_floats * 4),
                        d.byte_len,
                    );
                }
                filled_floats += d.count * d.dim;
            }

            // Mark cache valid
            let mut cg = self.scan_buf_col.lock().unwrap();
            cg.clear();
            cg.push_str(col_name);
            drop(cg);
            self.scan_buf_file_size
                .store(file_size, std::sync::atomic::Ordering::Release);
        }
        drop(mmap_arc);

        // SAFETY: scan_buf holds at least `needed` valid f32 elements.
        let buf_ptr = buf_guard.as_ptr();
        let floats: &[f32] = unsafe { std::slice::from_raw_parts(buf_ptr, needed) };
        let total_rows = needed / query_dim;
        let mut result = topk_heap_on_floats(floats, total_rows, query_dim, computer, k);
        drop(buf_guard);
        result.truncate(k);
        Ok(Some(result))
    }

    pub fn topk_fixedlist_direct(
        &self,
        col_name: &str,
        computer: &crate::compute::vector_ops::DistanceComputer,
        k: usize,
    ) -> io::Result<Option<Vec<(usize, f32)>>> {
        use crate::compute::vector_ops::topk_heap_on_floats;

        if self.has_pending_deltas() {
            return Ok(None);
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
        let column_type = schema.columns[col_idx].1;
        let is_f16 = column_type == ColumnType::Float16List;
        let quant_codec = match column_type {
            ColumnType::BFloat16List
            | ColumnType::Int8Vector
            | ColumnType::UInt8Vector
            | ColumnType::Bit1Vector
            | ColumnType::TurboQuant2Vector
            | ColumnType::TurboQuant3Vector
            | ColumnType::TurboQuant4Vector => column_type.vector_codec(),
            _ => None,
        };
        if column_type != ColumnType::FixedList && !is_f16 && quant_codec.is_none() {
            return Ok(None);
        }

        let query_dim = computer.query.len();

        // Get Arc<Mmap> and immediately release the write lock.
        let file_guard = self.file.read();
        let file = match file_guard.as_ref() {
            Some(f) => f,
            None => return Ok(None),
        };
        let mmap_arc = self.mmap_cache.write().get_mmap_arc(file)?;
        drop(file_guard);

        let mmap: &[u8] = &mmap_arc;
        let null_bitmap_len_fn = |rg_rows: usize| (rg_rows + 7) / 8;

        // ── PASS 1: validate all RGs, collect descriptors ──────────────────
        struct RgDesc {
            count: usize,
            float_abs: usize,
            byte_len: usize,
            /// Absolute offset of this row group's deletion bitmap, when it has one.
            del_abs: Option<usize>,
        }
        let mut rg_descs: Vec<Option<RgDesc>> = Vec::with_capacity(footer.row_groups.len());
        let mut total_active: usize = 0;
        // co_idx tracks position in footer.col_offsets independently of rg_idx.
        // Empty RGs (row_count==0, e.g. the initial RG from CREATE TABLE) never
        // push a col_offsets entry, so rg_idx and co_idx can diverge.
        let mut co_idx: usize = 0;

        for (_rg_idx, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_active = rg_meta.active_rows() as usize;
            total_active += rg_active;

            if rg_meta.row_count == 0 {
                rg_descs.push(None);
                continue;
            }
            let rg_rows = rg_meta.row_count as usize;

            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap.len() {
                return Ok(None);
            }
            let rg_bytes = &mmap[rg_meta.offset as usize..rg_end];

            let compress_flag = if rg_bytes.len() >= 32 {
                rg_bytes[28]
            } else {
                1
            };
            let encoding_ver = if rg_bytes.len() >= 32 {
                rg_bytes[29]
            } else {
                0
            };

            let has_deletions = rg_meta.deletion_count > 0;
            if compress_flag != RG_COMPRESS_NONE
                || encoding_ver < 1
                // Every branch below (f32, f16, quantized) now compacts live rows using
                // the row group's deletion bitmap, so deletions no longer force the lane
                // to stand down and fall back to whole-column Arrow materialization.
                || co_idx >= footer.col_offsets.len()
                || col_idx >= footer.col_offsets[co_idx].len()
            {
                return Ok(None);
            }

            let rg_body_abs = (rg_meta.offset + 32) as usize;
            let col_abs = rg_body_abs + footer.col_offsets[co_idx][col_idx] as usize;
            let data_abs = col_abs + null_bitmap_len_fn(rg_rows);

            // FixedList/Float16List layout: [encoding:u8][count:u64][dim:u32][elem * count * dim]
            if data_abs + 13 > mmap.len() {
                return Ok(None);
            }
            if mmap[data_abs] != COL_ENCODING_PLAIN {
                return Ok(None);
            }

            let count =
                u64::from_le_bytes(mmap[data_abs + 1..data_abs + 9].try_into().unwrap()) as usize;
            let dim =
                u32::from_le_bytes(mmap[data_abs + 9..data_abs + 13].try_into().unwrap()) as usize;

            if count == 0 || dim == 0 {
                rg_descs.push(None);
                continue;
            }
            if dim != query_dim {
                return Ok(None);
            }

            let row_width = if let Some(codec) = quant_codec {
                codec.row_width(dim)?
            } else {
                dim * if is_f16 { 2 } else { 4 }
            };
            let float_abs = data_abs + 13;
            let byte_len = count
                .checked_mul(row_width)
                .ok_or_else(|| err_data("vector block length overflow"))?;
            if float_abs + byte_len > mmap.len() {
                return Ok(None);
            }

            let del_abs = if has_deletions {
                // Body layout: [id section][deletion bitmap][per-column null bitmap +
                // data]. `col_offsets` already accounts for both leading sections; the
                // bitmap itself starts right after the id section.
                let id_encoding = mmap
                    .get(rg_meta.offset as usize + 30)
                    .copied()
                    .unwrap_or(crate::storage::on_demand::RG_IDS_PLAIN);
                let del_start = rg_body_abs
                    + crate::storage::on_demand::rg_id_section_len(rg_rows, id_encoding);
                let del_len = null_bitmap_len_fn(rg_rows);
                if del_start + del_len > mmap.len() || count != rg_rows {
                    return Ok(None);
                }
                Some(del_start)
            } else {
                None
            };

            rg_descs.push(Some(RgDesc {
                count,
                float_abs,
                byte_len,
                del_abs,
            }));
            co_idx += 1;
        }

        if total_active == 0 {
            return Ok(Some(vec![]));
        }

        if let Some(codec) = quant_codec {
            let mut blocks = rg_descs.iter().filter_map(Option::as_ref);
            if let Some(desc) = blocks.next() {
                if blocks.next().is_none() && desc.count == total_active {
                    return crate::compute::vector_quantization::topk_encoded_rows(
                        &mmap[desc.float_abs..desc.float_abs + desc.byte_len],
                        query_dim,
                        codec,
                        computer,
                        k,
                    )
                    .map(Some);
                }
            }
            let row_width = codec.row_width(query_dim)?;
            let needed_bytes = total_active.saturating_mul(row_width);
            let mut encoded: Vec<u8> = Vec::with_capacity(needed_bytes);
            // Fill through a raw pointer: per-row `extend_from_slice` adds capacity and
            // bounds bookkeeping that dominated the small-row codecs (int8 at dim 32 kept
            // a 3.2x gap that way, versus 1.7-1.9x for the wider ones).
            unsafe {
                encoded.set_len(needed_bytes);
            }
            let dst = encoded.as_mut_ptr();
            let mut written = 0usize;
            for desc in &rg_descs {
                let Some(desc) = desc else { continue };
                match desc.del_abs {
                    None => {
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                mmap.as_ptr().add(desc.float_abs),
                                dst.add(written),
                                desc.byte_len,
                            );
                        }
                        written += desc.byte_len;
                    }
                    Some(del_abs) => {
                        // Quantized columns carried the same penalty as f32/f16 (a single
                        // DELETE used to send the lane home: turboquant4 0.36 -> 12.87 ms,
                        // 35.8x). Copy only live rows, which keeps the encoded buffer at
                        // `active_rows * row_width` - exactly what the scorer derives rows
                        // from - so the returned indices stay in the active row space.
                        for row in 0..desc.count {
                            if (mmap[del_abs + row / 8] >> (row % 8)) & 1 == 1 {
                                continue;
                            }
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    mmap.as_ptr().add(desc.float_abs + row * row_width),
                                    dst.add(written),
                                    row_width,
                                );
                            }
                            written += row_width;
                        }
                    }
                }
            }
            // Same safety net as the f32/f16 paths: never score a partial buffer.
            if written != needed_bytes {
                return Ok(None);
            }
            return crate::compute::vector_quantization::topk_encoded_rows(
                &encoded, query_dim, codec, computer, k,
            )
            .map(Some);
        }

        let file_size = mmap.len() as u64;

        // ── PASS 2 (f16): cache raw f16 bytes, decode per-element during topk ─
        if is_f16 {
            let f16_needed = total_active * query_dim * 2;
            let f16_row_bytes = query_dim * 2;
            let budget = self.scan_buffer_budget_bytes();
            let window_rows = if budget == usize::MAX || f16_row_bytes == 0 {
                total_active
            } else {
                (budget / f16_row_bytes).max(1).min(total_active)
            };
            if window_rows < total_active {
                // Windowed scoring (OOC-6) for the f16 lane: same active-row semantics and the
                // same top-k as the whole-column scan, but RSS stays inside one window.
                let live_per_rg: Vec<usize> = rg_descs
                    .iter()
                    .map(|desc| match desc {
                        None => 0,
                        Some(d) => match d.del_abs {
                            None => d.count,
                            Some(del_abs) => (0..d.count)
                                .filter(|row| (mmap[del_abs + row / 8] >> (row % 8)) & 1 == 0)
                                .count(),
                        },
                    })
                    .collect();

                let mut scratch: Vec<u8> = vec![0u8; window_rows.saturating_mul(f16_row_bytes)];
                let mut best: Vec<(usize, f32)> = Vec::with_capacity(k.min(total_active));
                let mut start = 0usize;
                while start < total_active {
                    let rows = window_rows.min(total_active - start);
                    let mut written = 0usize;
                    let mut active_base = 0usize;
                    for (desc, live) in rg_descs.iter().zip(live_per_rg.iter()) {
                        let Some(d) = desc else { continue };
                        let rg_start = active_base;
                        let rg_end = active_base + live;
                        active_base = rg_end;
                        if rg_end <= start || rg_start >= start + rows {
                            continue;
                        }
                        let from = start.max(rg_start) - rg_start;
                        let take = (start + rows).min(rg_end) - start.max(rg_start);
                        match d.del_abs {
                            None => unsafe {
                                std::ptr::copy_nonoverlapping(
                                    mmap.as_ptr().add(d.float_abs + from * f16_row_bytes),
                                    scratch.as_mut_ptr().add(written * f16_row_bytes),
                                    take * f16_row_bytes,
                                );
                            },
                            Some(del_abs) => {
                                let mut live_index = 0usize;
                                let mut copied = 0usize;
                                for row in 0..d.count {
                                    if (mmap[del_abs + row / 8] >> (row % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if live_index >= from {
                                        unsafe {
                                            std::ptr::copy_nonoverlapping(
                                                mmap.as_ptr()
                                                    .add(d.float_abs + row * f16_row_bytes),
                                                scratch
                                                    .as_mut_ptr()
                                                    .add((written + copied) * f16_row_bytes),
                                                f16_row_bytes,
                                            );
                                        }
                                        copied += 1;
                                        if copied >= take {
                                            break;
                                        }
                                    }
                                    live_index += 1;
                                }
                            }
                        }
                        written += take;
                    }
                    if written != rows {
                        return Ok(None);
                    }
                    let partial = crate::compute::vector_ops::topk_heap_on_f16_bytes(
                        &scratch[..rows * f16_row_bytes],
                        rows,
                        query_dim,
                        computer,
                        k,
                    );
                    for (index, distance) in partial {
                        best.push((start + index, distance));
                    }
                    best.sort_by(|left, right| {
                        left.1
                            .partial_cmp(&right.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    best.truncate(k);
                    start += rows;
                }
                return Ok(Some(best));
            }
            let mut f16_guard = self.scan_buf_f16.lock().unwrap();
            let f16_cached = self
                .scan_buf_f16_file_size
                .load(std::sync::atomic::Ordering::Acquire);
            let f16_cg = self.scan_buf_f16_col.lock().unwrap();
            let f16_hit = f16_cached == file_size
                && f16_guard.len() >= f16_needed
                && f16_cg.as_str() == col_name;
            drop(f16_cg);

            if !f16_hit {
                let cur = f16_guard.len();
                if f16_guard.capacity() < f16_needed {
                    f16_guard.reserve(f16_needed - cur);
                }
                unsafe {
                    f16_guard.set_len(f16_needed);
                }
                let f16_ptr = f16_guard.as_mut_ptr();
                let mut filled = 0usize;
                let f16_row_bytes = query_dim * 2;
                for desc in rg_descs.iter() {
                    let Some(d) = desc else { continue };
                    match d.del_abs {
                        None => {
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    mmap.as_ptr().add(d.float_abs),
                                    f16_ptr.add(filled),
                                    d.byte_len,
                                );
                            }
                            filled += d.byte_len;
                        }
                        Some(del_abs) => {
                            // Same compaction as the f32 path: one DELETE used to send the f16
                            // lane home entirely (the column is half the size, so the Arrow
                            // fallback looked even worse: 0.50 -> 13.32 ms). Copy only live rows
                            // so the scored indices stay in the active row space.
                            for row in 0..d.count {
                                if (mmap[del_abs + row / 8] >> (row % 8)) & 1 == 1 {
                                    continue;
                                }
                                unsafe {
                                    std::ptr::copy_nonoverlapping(
                                        mmap.as_ptr().add(d.float_abs + row * f16_row_bytes),
                                        f16_ptr.add(filled),
                                        f16_row_bytes,
                                    );
                                }
                                filled += f16_row_bytes;
                            }
                        }
                    }
                }
                // Same safety net as the f32 path: never score a partial buffer.
                if filled != f16_needed {
                    return Ok(None);
                }
                let mut cg = self.scan_buf_f16_col.lock().unwrap();
                cg.clear();
                cg.push_str(col_name);
                drop(cg);
                self.scan_buf_f16_file_size
                    .store(file_size, std::sync::atomic::Ordering::Release);
            }
            drop(mmap_arc);

            let f16_ptr = f16_guard.as_ptr();
            let f16_slice = unsafe { std::slice::from_raw_parts(f16_ptr, f16_needed) };
            let mut result = crate::compute::vector_ops::topk_heap_on_f16_bytes(
                f16_slice,
                total_active,
                query_dim,
                computer,
                k,
            );
            drop(f16_guard);
            result.truncate(k);
            return Ok(Some(result));
        }

        // ── PASS 2 (f32): fill scan_buf and run ONE topk scan ────────────────
        let needed = total_active * query_dim;
        let row_bytes = query_dim * 4;
        let budget = self.scan_buffer_budget_bytes();
        let window_rows = if budget == usize::MAX || row_bytes == 0 {
            total_active
        } else {
            (budget / row_bytes).max(1).min(total_active)
        };

        if window_rows < total_active {
            // Windowed scoring (OOC-6): score the column one bounded window at a time and merge
            // each window's top-k into the running best, instead of either pinning the whole
            // column or standing down and losing the zero-copy lane. Deleted rows are skipped
            // exactly as in the whole-column fill, so the returned indices stay in active row
            // space and the result is identical to the uncapped lane.
            let live_per_rg: Vec<usize> = rg_descs
                .iter()
                .map(|desc| match desc {
                    None => 0,
                    Some(d) => match d.del_abs {
                        None => d.count,
                        Some(del_abs) => (0..d.count)
                            .filter(|row| (mmap[del_abs + row / 8] >> (row % 8)) & 1 == 0)
                            .count(),
                    },
                })
                .collect();

            let mut scratch: Vec<f32> = vec![0.0; window_rows.saturating_mul(query_dim)];
            let mut best: Vec<(usize, f32)> = Vec::with_capacity(k.min(total_active));
            let mut start = 0usize;
            while start < total_active {
                let rows = window_rows.min(total_active - start);
                let mut written = 0usize;
                let mut active_base = 0usize;
                for (desc, live) in rg_descs.iter().zip(live_per_rg.iter()) {
                    let Some(d) = desc else { continue };
                    let rg_start = active_base;
                    let rg_end = active_base + live;
                    active_base = rg_end;
                    if rg_end <= start || rg_start >= start + rows {
                        continue;
                    }
                    let from = start.max(rg_start) - rg_start;
                    let take = (start + rows).min(rg_end) - start.max(rg_start);
                    match d.del_abs {
                        None => unsafe {
                            std::ptr::copy_nonoverlapping(
                                mmap.as_ptr().add(d.float_abs + from * row_bytes),
                                (scratch.as_mut_ptr() as *mut u8).add(written * row_bytes),
                                take * row_bytes,
                            );
                        },
                        Some(del_abs) => {
                            let mut live_index = 0usize;
                            let mut copied = 0usize;
                            for row in 0..d.count {
                                if (mmap[del_abs + row / 8] >> (row % 8)) & 1 == 1 {
                                    continue;
                                }
                                if live_index >= from {
                                    unsafe {
                                        std::ptr::copy_nonoverlapping(
                                            mmap.as_ptr().add(d.float_abs + row * row_bytes),
                                            (scratch.as_mut_ptr() as *mut u8)
                                                .add((written + copied) * row_bytes),
                                            row_bytes,
                                        );
                                    }
                                    copied += 1;
                                    if copied >= take {
                                        break;
                                    }
                                }
                                live_index += 1;
                            }
                        }
                    }
                    written += take;
                }
                // Safety net: never score a partially filled window.
                if written != rows {
                    return Ok(None);
                }
                let partial = crate::compute::vector_ops::topk_heap_on_floats(
                    &scratch[..rows * query_dim],
                    rows,
                    query_dim,
                    computer,
                    k,
                );
                for (index, distance) in partial {
                    best.push((start + index, distance));
                }
                best.sort_by(|left, right| {
                    left.1
                        .partial_cmp(&right.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                best.truncate(k);
                start += rows;
            }
            return Ok(Some(best));
        }
        let mut buf_guard = self.scan_buf.lock().unwrap();
        let cached_size = self
            .scan_buf_file_size
            .load(std::sync::atomic::Ordering::Acquire);
        let col_guard = self.scan_buf_col.lock().unwrap();
        let cache_hit =
            cached_size == file_size && buf_guard.len() >= needed && col_guard.as_str() == col_name;
        drop(col_guard);

        if !cache_hit {
            let cur = buf_guard.len();
            if buf_guard.capacity() < needed {
                buf_guard.reserve(needed - cur);
            }
            unsafe {
                buf_guard.set_len(needed);
            }
            let buf_ptr = buf_guard.as_mut_ptr();
            let mut filled_floats = 0usize;
            for desc in rg_descs.iter() {
                let Some(d) = desc else { continue };
                let row_bytes = query_dim * 4;
                match d.del_abs {
                    None => {
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                mmap.as_ptr().add(d.float_abs),
                                (buf_ptr as *mut u8).add(filled_floats * 4),
                                d.byte_len,
                            );
                        }
                        filled_floats += d.count * query_dim;
                    }
                    Some(del_abs) => {
                        // A single DELETE used to send this lane home entirely (it assumed
                        // contiguously mapped rows), costing ~10x on every later TopK. Copy
                        // only the live rows: the score pass then returns indices that are
                        // already in the active row space the caller reads with.
                        for row in 0..d.count {
                            if (mmap[del_abs + row / 8] >> (row % 8)) & 1 == 1 {
                                continue;
                            }
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    mmap.as_ptr().add(d.float_abs + row * row_bytes),
                                    (buf_ptr as *mut u8).add(filled_floats * 4),
                                    row_bytes,
                                );
                            }
                            filled_floats += query_dim;
                        }
                    }
                }
            }
            // Safety net: the buffer was sized from `active_rows()`. If compaction
            // disagrees with that count, decline rather than score a partial buffer.
            if filled_floats != needed {
                return Ok(None);
            }
            let mut cg = self.scan_buf_col.lock().unwrap();
            cg.clear();
            cg.push_str(col_name);
            drop(cg);
            self.scan_buf_file_size
                .store(file_size, std::sync::atomic::Ordering::Release);
        }
        drop(mmap_arc);

        let buf_ptr = buf_guard.as_ptr();
        let floats: &[f32] = unsafe { std::slice::from_raw_parts(buf_ptr, needed) };
        let total_rows = needed / query_dim;
        let mut result = topk_heap_on_floats(floats, total_rows, query_dim, computer, k);
        drop(buf_guard);
        result.truncate(k);
        Ok(Some(result))
    }

    pub fn batch_topk_fixedlist_direct(
        &self,
        col_name: &str,
        queries: &[f32],
        n_queries: usize,
        k: usize,
        metric: crate::compute::vector_ops::DistanceMetric,
    ) -> io::Result<Option<Vec<Vec<(usize, f32)>>>> {
        use crate::compute::vector_ops::batch_topk_on_floats;

        if n_queries == 0 || queries.len() == 0 {
            return Ok(Some(vec![vec![]; n_queries]));
        }
        if self.has_pending_deltas() {
            return Ok(None);
        }
        let query_dim = queries.len() / n_queries;
        if query_dim == 0 || queries.len() != n_queries * query_dim {
            return Ok(None);
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
        let column_type = schema.columns[col_idx].1;
        let is_f16_batch = column_type == ColumnType::Float16List;
        let quant_codec = match column_type {
            ColumnType::BFloat16List
            | ColumnType::Int8Vector
            | ColumnType::UInt8Vector
            | ColumnType::Bit1Vector
            | ColumnType::TurboQuant2Vector
            | ColumnType::TurboQuant3Vector
            | ColumnType::TurboQuant4Vector => column_type.vector_codec(),
            _ => None,
        };
        if column_type != ColumnType::FixedList && !is_f16_batch && quant_codec.is_none() {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = match file_guard.as_ref() {
            Some(f) => f,
            None => return Ok(None),
        };
        let mmap_arc = self.mmap_cache.write().get_mmap_arc(file)?;
        drop(file_guard);
        let mmap: &[u8] = &mmap_arc;
        let null_bitmap_len_fn = |rg_rows: usize| (rg_rows + 7) / 8;

        // ── PASS 1: validate all RGs, collect descriptors ──────────────────
        struct RgDesc {
            count: usize,
            float_abs: usize,
            byte_len: usize,
        }
        let mut rg_descs: Vec<Option<RgDesc>> = Vec::with_capacity(footer.row_groups.len());
        let mut total_active: usize = 0;
        // co_idx tracks position in footer.col_offsets independently of rg_idx.
        // Empty RGs (row_count==0) never push a col_offsets entry.
        let mut co_idx: usize = 0;

        for (_rg_idx, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_active = rg_meta.active_rows() as usize;
            total_active += rg_active;

            if rg_meta.row_count == 0 {
                rg_descs.push(None);
                continue;
            }
            let rg_rows = rg_meta.row_count as usize;

            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap.len() {
                return Ok(None);
            }
            let rg_bytes = &mmap[rg_meta.offset as usize..rg_end];

            let compress_flag = if rg_bytes.len() >= 32 {
                rg_bytes[28]
            } else {
                1
            };
            let encoding_ver = if rg_bytes.len() >= 32 {
                rg_bytes[29]
            } else {
                0
            };

            if compress_flag != RG_COMPRESS_NONE
                || encoding_ver < 1
                || rg_meta.deletion_count > 0
                || co_idx >= footer.col_offsets.len()
                || col_idx >= footer.col_offsets[co_idx].len()
            {
                return Ok(None);
            }

            let rg_body_abs = (rg_meta.offset + 32) as usize;
            let col_abs = rg_body_abs + footer.col_offsets[co_idx][col_idx] as usize;
            let data_abs = col_abs + null_bitmap_len_fn(rg_rows);

            if data_abs + 13 > mmap.len() {
                return Ok(None);
            }
            if mmap[data_abs] != COL_ENCODING_PLAIN {
                return Ok(None);
            }

            let count =
                u64::from_le_bytes(mmap[data_abs + 1..data_abs + 9].try_into().unwrap()) as usize;
            let dim =
                u32::from_le_bytes(mmap[data_abs + 9..data_abs + 13].try_into().unwrap()) as usize;

            if count == 0 || dim == 0 {
                rg_descs.push(None);
                continue;
            }
            if dim != query_dim {
                return Ok(None);
            }

            let row_width = if let Some(codec) = quant_codec {
                codec.row_width(dim)?
            } else {
                dim * if is_f16_batch { 2 } else { 4 }
            };
            let float_abs = data_abs + 13;
            let byte_len = count
                .checked_mul(row_width)
                .ok_or_else(|| err_data("vector block length overflow"))?;
            if float_abs + byte_len > mmap.len() {
                return Ok(None);
            }
            rg_descs.push(Some(RgDesc {
                count,
                float_abs,
                byte_len,
            }));
            co_idx += 1;
        }

        if total_active == 0 {
            return Ok(Some(vec![vec![]; n_queries]));
        }

        if let Some(codec) = quant_codec {
            let mut blocks = rg_descs.iter().filter_map(Option::as_ref);
            if let Some(desc) = blocks.next() {
                if blocks.next().is_none() && desc.count == total_active {
                    return crate::compute::vector_quantization::batch_topk_encoded_rows(
                        &mmap[desc.float_abs..desc.float_abs + desc.byte_len],
                        query_dim,
                        codec,
                        queries,
                        n_queries,
                        k,
                        metric,
                    )
                    .map(Some);
                }
            }
            let row_width = codec.row_width(query_dim)?;
            let mut encoded = Vec::with_capacity(total_active.saturating_mul(row_width));
            for desc in &rg_descs {
                let Some(desc) = desc else { continue };
                encoded.extend_from_slice(&mmap[desc.float_abs..desc.float_abs + desc.byte_len]);
            }
            return crate::compute::vector_quantization::batch_topk_encoded_rows(
                &encoded, query_dim, codec, queries, n_queries, k, metric,
            )
            .map(Some);
        }

        let file_size = mmap.len() as u64;

        // ── PASS 2 (f16 batch): cache raw f16 bytes, decode per-row during topk
        if is_f16_batch {
        // A whole-column buffer is only worth pinning while it fits the budget: on a
        // larger column decline here so the caller reads a windowed batch instead
        // (identical results, bounded RSS).
            let f16_needed = total_active * query_dim * 2;
            let f16_row_bytes = query_dim * 2;
            let budget = self.scan_buffer_budget_bytes();
            let window_rows = if budget == usize::MAX || f16_row_bytes == 0 {
                total_active
            } else {
                (budget / f16_row_bytes).max(1).min(total_active)
            };
            if window_rows < total_active {
                // Windowed batch scoring (OOC-6): score one bounded window at a time and merge
                // each query's top-k, so a capped buffer keeps this lane instead of standing
                // down. This lane declines pending deltas above, so windows are plain byte
                // ranges of live rows.
                let mut scratch: Vec<u8> = vec![0u8; window_rows.saturating_mul(f16_row_bytes)];
                let mut best: Vec<Vec<(usize, f32)>> = vec![Vec::new(); n_queries];
                let mut start = 0usize;
                while start < total_active {
                    let rows = window_rows.min(total_active - start);
                    let mut written = 0usize;
                    let mut active_base = 0usize;
                    for desc in rg_descs.iter() {
                        let Some(d) = desc else { continue };
                        let rg_end = active_base + d.count;
                        if rg_end <= start || active_base >= start + rows {
                            active_base = rg_end;
                            continue;
                        }
                        let from = start.max(active_base) - active_base;
                        let take = (start + rows).min(rg_end) - start.max(active_base);
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                mmap.as_ptr().add(d.float_abs + from * f16_row_bytes),
                                scratch.as_mut_ptr().add(written * f16_row_bytes),
                                take * f16_row_bytes,
                            );
                        }
                        written += take;
                        active_base = rg_end;
                    }
                    if written != rows {
                        return Ok(None);
                    }
                    let partial = crate::compute::vector_ops::batch_topk_on_f16_bytes(
                        &scratch[..rows * f16_row_bytes],
                        rows,
                        query_dim,
                        queries,
                        n_queries,
                        k,
                        metric,
                    );
                    for (query_index, items) in partial.into_iter().enumerate() {
                        for (index, distance) in items {
                            best[query_index].push((start + index, distance));
                        }
                        best[query_index].sort_by(|left, right| {
                            left.1
                                .partial_cmp(&right.1)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });
                        best[query_index].truncate(k);
                    }
                    start += rows;
                }
                return Ok(Some(best));
            }
            let mut f16_guard = self.scan_buf_f16.lock().unwrap();
            let f16_cached = self
                .scan_buf_f16_file_size
                .load(std::sync::atomic::Ordering::Acquire);
            let f16_cg = self.scan_buf_f16_col.lock().unwrap();
            let f16_hit = f16_cached == file_size
                && f16_guard.len() >= f16_needed
                && f16_cg.as_str() == col_name;
            drop(f16_cg);

            if !f16_hit {
                let cur = f16_guard.len();
                if f16_guard.capacity() < f16_needed {
                    f16_guard.reserve(f16_needed - cur);
                }
                unsafe {
                    f16_guard.set_len(f16_needed);
                }
                let f16_ptr = f16_guard.as_mut_ptr();
                let mut filled = 0usize;
                for desc in rg_descs.iter() {
                    let Some(d) = desc else { continue };
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            mmap.as_ptr().add(d.float_abs),
                            f16_ptr.add(filled),
                            d.byte_len,
                        );
                    }
                    filled += d.byte_len;
                }
                let mut cg = self.scan_buf_f16_col.lock().unwrap();
                cg.clear();
                cg.push_str(col_name);
                drop(cg);
                self.scan_buf_f16_file_size
                    .store(file_size, std::sync::atomic::Ordering::Release);
            }
            drop(mmap_arc);

            let f16_ptr = f16_guard.as_ptr();
            let f16_slice = unsafe { std::slice::from_raw_parts(f16_ptr, f16_needed) };
            let results = crate::compute::vector_ops::batch_topk_on_f16_bytes(
                f16_slice,
                total_active,
                query_dim,
                queries,
                n_queries,
                k,
                metric,
            );
            drop(f16_guard);
            return Ok(Some(results));
        }

        // ── PASS 2 (f32 batch): fill scan_buf once, run all N queries ────────
        // A whole-column buffer is only worth pinning while it fits the budget: on a
        // larger column decline here so the caller reads a windowed batch instead
        // (identical results, bounded RSS).
        let needed = total_active * query_dim;
        let row_bytes = query_dim * 4;
        let budget = self.scan_buffer_budget_bytes();
        let window_rows = if budget == usize::MAX || row_bytes == 0 {
            total_active
        } else {
            (budget / row_bytes).max(1).min(total_active)
        };
        if window_rows < total_active {
            // Windowed batch scoring (OOC-6) for f32, mirroring the f16 batch lane.
            let mut scratch: Vec<f32> = vec![0.0; window_rows.saturating_mul(query_dim)];
            let mut best: Vec<Vec<(usize, f32)>> = vec![Vec::new(); n_queries];
            let mut start = 0usize;
            while start < total_active {
                let rows = window_rows.min(total_active - start);
                let mut written = 0usize;
                let mut active_base = 0usize;
                for desc in rg_descs.iter() {
                    let Some(d) = desc else { continue };
                    let rg_end = active_base + d.count;
                    if rg_end <= start || active_base >= start + rows {
                        active_base = rg_end;
                        continue;
                    }
                    let from = start.max(active_base) - active_base;
                    let take = (start + rows).min(rg_end) - start.max(active_base);
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            mmap.as_ptr().add(d.float_abs + from * row_bytes),
                            (scratch.as_mut_ptr() as *mut u8).add(written * row_bytes),
                            take * row_bytes,
                        );
                    }
                    written += take;
                    active_base = rg_end;
                }
                if written != rows {
                    return Ok(None);
                }
                let partial = crate::compute::vector_ops::batch_topk_on_floats(
                    &scratch[..rows * query_dim],
                    rows,
                    query_dim,
                    queries,
                    n_queries,
                    k,
                    metric,
                );
                for (query_index, items) in partial.into_iter().enumerate() {
                    for (index, distance) in items {
                        best[query_index].push((start + index, distance));
                    }
                    best[query_index].sort_by(|left, right| {
                        left.1
                            .partial_cmp(&right.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    best[query_index].truncate(k);
                }
                start += rows;
            }
            return Ok(Some(best));
        }
        let mut buf_guard = self.scan_buf.lock().unwrap();
        let cached_size = self
            .scan_buf_file_size
            .load(std::sync::atomic::Ordering::Acquire);
        let col_guard = self.scan_buf_col.lock().unwrap();
        let cache_hit =
            cached_size == file_size && buf_guard.len() >= needed && col_guard.as_str() == col_name;
        drop(col_guard);

        if !cache_hit {
            if buf_guard.capacity() < needed {
                let cur = buf_guard.len();
                buf_guard.reserve(needed - cur);
            }
            unsafe {
                buf_guard.set_len(needed);
            }
            let buf_ptr = buf_guard.as_mut_ptr();
            let mut filled_floats = 0usize;
            for desc in rg_descs.iter() {
                let Some(d) = desc else { continue };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        mmap.as_ptr().add(d.float_abs),
                        (buf_ptr as *mut u8).add(filled_floats * 4),
                        d.byte_len,
                    );
                }
                filled_floats += d.count * query_dim;
            }
            let mut cg = self.scan_buf_col.lock().unwrap();
            cg.clear();
            cg.push_str(col_name);
            drop(cg);
            self.scan_buf_file_size
                .store(file_size, std::sync::atomic::Ordering::Release);
        }
        drop(mmap_arc);

        let buf_ptr = buf_guard.as_ptr();
        let floats: &[f32] = unsafe { std::slice::from_raw_parts(buf_ptr, needed) };
        let total_rows = needed / query_dim;

        let results =
            batch_topk_on_floats(floats, total_rows, query_dim, queries, n_queries, k, metric);
        drop(buf_guard);
        Ok(Some(results))
    }

    pub fn batch_topk_binary_direct(
        &self,
        col_name: &str,
        queries: &[f32],
        n_queries: usize,
        k: usize,
        metric: crate::compute::vector_ops::DistanceMetric,
    ) -> io::Result<Option<Vec<Vec<(usize, f32)>>>> {
        use crate::compute::vector_ops::batch_topk_on_floats;

        if n_queries == 0 || queries.len() == 0 {
            return Ok(Some(vec![vec![]; n_queries]));
        }
        if self.has_pending_deltas() {
            return Ok(None);
        }
        let query_dim = queries.len() / n_queries;
        if query_dim == 0 || queries.len() != n_queries * query_dim {
            return Ok(None);
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
        if schema.columns[col_idx].1 != ColumnType::Binary {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = match file_guard.as_ref() {
            Some(f) => f,
            None => return Ok(None),
        };
        let mmap_arc = self.mmap_cache.write().get_mmap_arc(file)?;
        drop(file_guard);
        let mmap: &[u8] = &mmap_arc;

        // ── PASS 1: validate all RGs ────────────────────────────────────────
        let total_active: usize = footer
            .row_groups
            .iter()
            .map(|rg| rg.active_rows() as usize)
            .sum();
        if total_active == 0 {
            return Ok(Some(vec![vec![]; n_queries]));
        }

        struct RgDesc {
            count: usize,
            dim: usize,
            data_start: usize,
            byte_len: usize,
        }
        let mut rg_descs: Vec<Option<RgDesc>> = Vec::with_capacity(footer.row_groups.len());

        for (rg_idx, rg_meta) in footer.row_groups.iter().enumerate() {
            if rg_meta.row_count == 0 {
                rg_descs.push(None);
                continue;
            }
            let rg_rows = rg_meta.row_count as usize;

            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap.len() {
                return Ok(None);
            }
            let rg_bytes = &mmap[rg_meta.offset as usize..rg_end];
            let compress_flag = if rg_bytes.len() >= 32 {
                rg_bytes[28]
            } else {
                1
            };
            let encoding_ver = if rg_bytes.len() >= 32 {
                rg_bytes[29]
            } else {
                0
            };

            if compress_flag != RG_COMPRESS_NONE
                || encoding_ver < 1
                || rg_meta.deletion_count > 0
                || rg_idx >= footer.col_offsets.len()
                || col_idx >= footer.col_offsets[rg_idx].len()
            {
                return Ok(None);
            }

            let rg_body_abs = (rg_meta.offset + 32) as usize;
            let null_bm_len = (rg_rows + 7) / 8;
            let col_abs = rg_body_abs + footer.col_offsets[rg_idx][col_idx] as usize;
            let data_abs = col_abs + null_bm_len;

            if data_abs + 9 > mmap.len() {
                return Ok(None);
            }
            if mmap[data_abs] != COL_ENCODING_PLAIN {
                return Ok(None);
            }

            let count =
                u64::from_le_bytes(mmap[data_abs + 1..data_abs + 9].try_into().unwrap()) as usize;
            if count == 0 {
                rg_descs.push(None);
                continue;
            }

            let off_base = data_abs + 9;
            if off_base + 8 > mmap.len() {
                return Ok(None);
            }
            let off0 =
                u32::from_le_bytes(mmap[off_base..off_base + 4].try_into().unwrap()) as usize;
            let off1 =
                u32::from_le_bytes(mmap[off_base + 4..off_base + 8].try_into().unwrap()) as usize;
            if off1 <= off0 || (off1 - off0) % 4 != 0 {
                return Ok(None);
            }
            let dim = (off1 - off0) / 4;
            if dim != query_dim {
                return Ok(None);
            }

            let data_start = off_base + (count + 1) * 4 + 8;
            let byte_len = count * dim * 4;
            if data_start + byte_len > mmap.len() {
                return Ok(None);
            }
            rg_descs.push(Some(RgDesc {
                count,
                dim,
                data_start,
                byte_len,
            }));
        }

        // ── PASS 2: fill scan_buf once, run all N queries in parallel ───────
        let needed = total_active * query_dim;
        let row_bytes = query_dim * 4;
        let budget = self.scan_buffer_budget_bytes();
        let window_rows = if budget == usize::MAX || row_bytes == 0 {
            total_active
        } else {
            (budget / row_bytes).max(1).min(total_active)
        };
        if window_rows < total_active {
            // Windowed batch scoring (OOC-6) for the binary lane, mirroring the f32 batch lane:
            // this lane declines deleted/compressed row groups above, so a window is a plain
            // contiguous byte range per row group.
            let mut scratch: Vec<f32> = vec![0.0; window_rows.saturating_mul(query_dim)];
            let mut best: Vec<Vec<(usize, f32)>> = vec![Vec::new(); n_queries];
            let mut start = 0usize;
            while start < total_active {
                let rows = window_rows.min(total_active - start);
                let mut written = 0usize;
                let mut active_base = 0usize;
                for desc in rg_descs.iter() {
                    let Some(d) = desc else { continue };
                    let rg_end = active_base + d.count;
                    if rg_end <= start || active_base >= start + rows {
                        active_base = rg_end;
                        continue;
                    }
                    let from = start.max(active_base) - active_base;
                    let take = (start + rows).min(rg_end) - start.max(active_base);
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            mmap.as_ptr().add(d.data_start + from * row_bytes),
                            (scratch.as_mut_ptr() as *mut u8).add(written * row_bytes),
                            take * row_bytes,
                        );
                    }
                    written += take;
                    active_base = rg_end;
                }
                if written != rows {
                    return Ok(None);
                }
                let partial = crate::compute::vector_ops::batch_topk_on_floats(
                    &scratch[..rows * query_dim],
                    rows,
                    query_dim,
                    queries,
                    n_queries,
                    k,
                    metric,
                );
                for (query_index, items) in partial.into_iter().enumerate() {
                    for (index, distance) in items {
                        best[query_index].push((start + index, distance));
                    }
                    best[query_index].sort_by(|left, right| {
                        left.1
                            .partial_cmp(&right.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    best[query_index].truncate(k);
                }
                start += rows;
            }
            return Ok(Some(best));
        }
        let file_size = mmap.len() as u64;
        let mut buf_guard = self.scan_buf.lock().unwrap();
        let cached_size = self
            .scan_buf_file_size
            .load(std::sync::atomic::Ordering::Acquire);
        let col_guard = self.scan_buf_col.lock().unwrap();
        let cache_hit =
            cached_size == file_size && buf_guard.len() >= needed && col_guard.as_str() == col_name;
        drop(col_guard);

        if !cache_hit {
            if buf_guard.capacity() < needed {
                let cur = buf_guard.len();
                buf_guard.reserve(needed - cur);
            }
            unsafe {
                buf_guard.set_len(needed);
            }
            let buf_ptr = buf_guard.as_mut_ptr();
            let mut filled_floats = 0usize;
            for desc in rg_descs.iter() {
                let Some(d) = desc else { continue };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        mmap.as_ptr().add(d.data_start),
                        (buf_ptr as *mut u8).add(filled_floats * 4),
                        d.byte_len,
                    );
                }
                filled_floats += d.count * d.dim;
            }
            let mut cg = self.scan_buf_col.lock().unwrap();
            cg.clear();
            cg.push_str(col_name);
            drop(cg);
            self.scan_buf_file_size
                .store(file_size, std::sync::atomic::Ordering::Release);
        }
        drop(mmap_arc);

        let buf_ptr = buf_guard.as_ptr();
        let floats: &[f32] = unsafe { std::slice::from_raw_parts(buf_ptr, needed) };
        let total_rows = needed / query_dim;

        let results =
            batch_topk_on_floats(floats, total_rows, query_dim, queries, n_queries, k, metric);
        drop(buf_guard);
        Ok(Some(results))
    }
}
