use super::*;

impl OnDemandStorage {
    pub fn topk_binary_direct(
        &self,
        col_name: &str,
        computer: &crate::compute::vector_ops::DistanceComputer,
        k: usize,
    ) -> io::Result<Option<Vec<(usize, f32)>>> {
        use crate::compute::vector_ops::topk_heap_on_floats;

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

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let col_idx = match schema.get_index(col_name) {
            Some(i) => i,
            None => return Ok(None),
        };
        let is_f16 = schema.columns[col_idx].1 == ColumnType::Float16List;
        if schema.columns[col_idx].1 != ColumnType::FixedList && !is_f16 {
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

            let elem_bytes = if is_f16 { 2 } else { 4 };
            let float_abs = data_abs + 13;
            let byte_len = count * dim * elem_bytes;
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
            return Ok(Some(vec![]));
        }

        let file_size = mmap.len() as u64;

        // ── PASS 2 (f16): cache raw f16 bytes, decode per-element during topk ─
        if is_f16 {
            let f16_needed = total_active * query_dim * 2;
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
        let is_f16_batch = schema.columns[col_idx].1 == ColumnType::Float16List;
        if schema.columns[col_idx].1 != ColumnType::FixedList && !is_f16_batch {
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

            let elem_bytes_b = if is_f16_batch { 2 } else { 4 };
            let float_abs = data_abs + 13;
            let byte_len = count * dim * elem_bytes_b;
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

        let file_size = mmap.len() as u64;

        // ── PASS 2 (f16 batch): cache raw f16 bytes, decode per-row during topk
        if is_f16_batch {
            let f16_needed = total_active * query_dim * 2;
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
        let needed = total_active * query_dim;
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
