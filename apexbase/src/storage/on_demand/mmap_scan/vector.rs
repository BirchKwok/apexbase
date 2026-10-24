use super::*;
/// Decompressed float buffers for the opt-in IVF path, keyed by table path and
/// column. A file fingerprint (length + mtime) invalidates it after a write, so
/// the buffer is rebuilt once instead of on every query.
/// Memory-mapped index files (`<table>.apex.ivf`), keyed by path and file
/// fingerprint. v2 indexes carry the vector payload, so mapping beats reading
/// and copying tens of megabytes per query.
static IVF_MMAP_CACHE: once_cell::sync::Lazy<
    parking_lot::Mutex<std::collections::HashMap<std::path::PathBuf, (u64, u64, std::sync::Arc<memmap2::Mmap>)>>,
> = once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

static IVF_VECTOR_CACHE: once_cell::sync::Lazy<
    parking_lot::Mutex<std::collections::HashMap<(std::path::PathBuf, String), (u64, u64, std::sync::Arc<Vec<f32>>, usize)>>,
> = once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));


/// Metadata for the persisted coarse vector index (`<table>.apex.ivf`).
#[derive(Debug, Clone)]
pub struct VectorIndexInfo {
    pub version: u8,
    pub dim: usize,
    pub nlist: usize,
    pub rows: usize,
    pub metric: String,
    pub inertia: f64,
    pub list_sizes: Vec<usize>,
    /// `true` when the table's active row count no longer matches the rows the
    /// index was built over (rows were added or deleted since the build).
    pub stale: bool,
}

/// Metric codes stored in an AVIX header: 0 = L2, 1 = cosine distance.
fn vector_index_metric_code(metric: &str) -> io::Result<u8> {
    use crate::compute::vector_ops::DistanceMetric;
    match DistanceMetric::from_str(metric) {
        Some(DistanceMetric::L2) => Ok(0),
        Some(DistanceMetric::CosineDistance) => Ok(1),
        _ => Err(err_data(
            "create_vector_index supports metric 'l2' or 'cosine' only",
        )),
    }
}

fn vector_index_metric(code: u8) -> io::Result<crate::compute::vector_ops::DistanceMetric> {
    use crate::compute::vector_ops::DistanceMetric;
    match code {
        0 => Ok(DistanceMetric::L2),
        1 => Ok(DistanceMetric::CosineDistance),
        other => Err(err_data(format!(
            "vector index uses unsupported metric code {other}"
        ))),
    }
}

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

    /// Exact distances for a coarse candidate set read straight from the flat
    /// vector bytes.
    ///
    /// The quantized rescore stage used to materialise the candidate rows
    /// through Arrow, which cost several milliseconds for a few hundred rows and
    /// made coarse+rescore slower than the exact lane. This reads the same rows
    /// from the mapped row groups (no Arrow batch) and returns the candidates in
    /// the active row space, so callers map ids with `ids_at_active_positions`.
    /// `None` means the layout is not a flat uncompressed f32 list.
    pub fn rescore_vector_rows(
        &self,
        col_name: &str,
        candidates: &[usize],
        computer: &crate::compute::vector_ops::DistanceComputer,
        k: usize,
    ) -> io::Result<Option<Vec<(i64, f32)>>> {
        if candidates.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let Some(footer) = self.get_or_load_footer()? else {
            return Ok(None);
        };
        let Some(col_idx) = footer.schema.get_index(col_name) else {
            return Ok(None);
        };
        if footer.schema.columns[col_idx].1 != ColumnType::FixedList {
            return Ok(None);
        }
        let dim = computer.query.len();
        let file_guard = self.file.read();
        let Some(file) = file_guard.as_ref() else {
            return Ok(None);
        };
        let mmap_arc = self.mmap_cache.write().get_mmap_arc(file)?;
        drop(file_guard);
        let mmap: &[u8] = &mmap_arc;

        struct RgDesc {
            active_start: usize,
            active_count: usize,
            vector_abs: usize,
            row_width: usize,
            del_abs: Option<usize>,
            count: usize,
            ids_abs: usize,
            min_id: u64,
            implicit_ids: bool,
        }
        let mut rgs: Vec<RgDesc> = Vec::with_capacity(footer.row_groups.len());
        let mut co_idx = 0usize;
        let mut active_cursor = 0usize;
        for rg_meta in &footer.row_groups {
            if rg_meta.row_count == 0 {
                continue;
            }
            let rg_rows = rg_meta.row_count as usize;
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap.len() {
                return Ok(None);
            }
            let active = rg_meta.active_rows() as usize;
            let compress = mmap[rg_meta.offset as usize + 28];
            let encoding = mmap[rg_meta.offset as usize + 29];
            if compress != RG_COMPRESS_NONE
                || encoding < 1
                || co_idx >= footer.col_offsets.len()
                || col_idx >= footer.col_offsets[co_idx].len()
            {
                return Ok(None);
            }
            let bitmap = (rg_rows + 7) / 8;
            let data_abs =
                rg_meta.offset as usize + 32 + footer.col_offsets[co_idx][col_idx] as usize + bitmap;
            if data_abs + 13 > mmap.len() || mmap[data_abs] != COL_ENCODING_PLAIN {
                return Ok(None);
            }
            let count =
                u64::from_le_bytes(mmap[data_abs + 1..data_abs + 9].try_into().unwrap()) as usize;
            let row_dim =
                u32::from_le_bytes(mmap[data_abs + 9..data_abs + 13].try_into().unwrap()) as usize;
            if count != rg_rows || row_dim != dim {
                return Ok(None);
            }
            let row_width = dim * 4;
            let vector_abs = data_abs + 13;
            if vector_abs + count * row_width > mmap.len() {
                return Ok(None);
            }
            let del_abs = if rg_meta.deletion_count > 0 {
                let id_encoding = mmap[rg_meta.offset as usize + 30];
                let del_start = rg_meta.offset as usize + 32
                    + crate::storage::on_demand::rg_id_section_len(rg_rows, id_encoding);
                if del_start + bitmap > mmap.len() {
                    return Ok(None);
                }
                Some(del_start)
            } else {
                None
            };
            let id_encoding = mmap[rg_meta.offset as usize + 30];
            let implicit_ids =
                id_encoding == crate::storage::on_demand::RG_IDS_IMPLICIT_CONTIGUOUS;
            rgs.push(RgDesc {
                active_start: active_cursor,
                active_count: active,
                vector_abs,
                row_width,
                del_abs,
                count,
                ids_abs: rg_meta.offset as usize + 32,
                min_id: rg_meta.min_id,
                implicit_ids,
            });
            active_cursor += active;
            co_idx += 1;
        }

        let mut scored: Vec<(i64, f32)> = Vec::with_capacity(candidates.len());
        for &candidate in candidates {
            if candidate >= active_cursor {
                continue;
            }
            let Some(rg) = rgs
                .iter()
                .find(|rg| candidate >= rg.active_start && candidate < rg.active_start + rg.active_count)
            else {
                continue;
            };
            let local_active = candidate - rg.active_start;
            let physical = match rg.del_abs {
                None => local_active,
                Some(del_abs) => {
                    let mut live = 0usize;
                    let mut row = 0usize;
                    while row < rg.count {
                        if (mmap[del_abs + row / 8] >> (row % 8)) & 1 == 0 {
                            if live == local_active {
                                break;
                            }
                            live += 1;
                        }
                        row += 1;
                    }
                    if row >= rg.count {
                        continue;
                    }
                    row
                }
            };
            let start = rg.vector_abs + physical * rg.row_width;
            let mut values = Vec::with_capacity(dim);
            for chunk in mmap[start..start + rg.row_width].chunks_exact(4) {
                values.push(f32::from_le_bytes(chunk.try_into().unwrap()));
            }
            let score = computer.compute_topk_ordering(&values);
            let id = if rg.implicit_ids {
                (rg.min_id + physical as u64) as i64
            } else {
                let id_abs = rg.ids_abs + physical * 8;
                if id_abs + 8 > mmap.len() {
                    return Ok(None);
                }
                u64::from_le_bytes(mmap[id_abs..id_abs + 8].try_into().unwrap()) as i64
            };
            scored.push((id, computer.finalize_topk_distance(score)));
        }
        scored.sort_unstable_by(|left, right| left.1.total_cmp(&right.1));
        scored.truncate(k);
        Ok(Some(scored))
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

    // ── Coarse vector index (W5-1 stage 1) ────────────────────────────────
    //
    // Builds a persisted IVF index (`<table>.apex.ivf`) over a fixed-list or
    // binary f32 vector column: k-means centroids plus per-list row positions.
    // Query integration (nprobe) comes in the next stage; this stage owns the
    // build/inspect/drop lifecycle so it can be reviewed and tested alone.

    fn vector_index_path(&self) -> std::path::PathBuf {
        let mut path = self.path.clone();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        path.set_file_name(format!("{name}.ivf"));
        path
    }

    /// Read the active rows of a fixed-list/binary f32 vector column as a flat
    /// row-major f32 buffer. `None` means the column is not a supported vector
    /// column.
    fn collect_vector_rows(&self, col_name: &str) -> io::Result<Option<(Vec<f32>, usize)>> {
        let Some(footer) = self.get_or_load_footer()? else {
            return Ok(None);
        };
        let Some(col_idx) = footer.schema.get_index(col_name) else {
            return Ok(None);
        };
        let (columns, deleted) = self.scan_columns_mmap(&[col_idx], &footer)?;
        let Some(column) = columns.first() else {
            return Ok(None);
        };
        let mut vectors: Vec<f32> = Vec::new();
        let mut dim = 0usize;
        match column {
            ColumnData::FixedList { data, dim: column_dim } => {
                dim = *column_dim as usize;
                if dim == 0 || data.len() % (dim * 4) != 0 {
                    return Ok(None);
                }
                let mut row = 0usize;
                for chunk in data.chunks_exact(dim * 4) {
                    let alive = deleted
                        .get(row / 8)
                        .map_or(true, |byte| (byte >> (row % 8)) & 1 == 0);
                    row += 1;
                    if !alive {
                        continue;
                    }
                    for value in chunk.chunks_exact(4) {
                        vectors.push(f32::from_le_bytes(value.try_into().unwrap()));
                    }
                }
            }
            ColumnData::Binary { offsets, data } => {
                let row_count = offsets.len().saturating_sub(1);
                let mut row = 0usize;
                while row < row_count {
                    let start = offsets[row] as usize;
                    let end = offsets[row + 1] as usize;
                    let alive = deleted
                        .get(row / 8)
                        .map_or(true, |byte| (byte >> (row % 8)) & 1 == 0);
                    row += 1;
                    if !alive || start > end || end > data.len() {
                        continue;
                    }
                    let bytes = &data[start..end];
                    if bytes.len() % 4 != 0 {
                        return Ok(None);
                    }
                    let row_dim = bytes.len() / 4;
                    if dim == 0 {
                        dim = row_dim;
                    } else if dim != row_dim {
                        return Ok(None);
                    }
                    for value in bytes.chunks_exact(4) {
                        vectors.push(f32::from_le_bytes(value.try_into().unwrap()));
                    }
                }
            }
            _ => return Ok(None),
        }
        if dim == 0 {
            return Ok(None);
        }
        Ok(Some((vectors, dim)))
    }

    /// Build and persist a coarse IVF index for `col_name`.
    pub fn create_vector_index(
        &self,
        col_name: &str,
        nlist: usize,
        iterations: usize,
        metric: &str,
    ) -> io::Result<()> {
        let metric_code = vector_index_metric_code(metric)?;
        if self.has_delta() || self.has_pending_deltas() {
            return Err(err_data(
                "create_vector_index requires a flushed table with no pending writes",
            ));
        }
        let Some((vectors, dim)) = self.collect_vector_rows(col_name)? else {
            return Err(err_data(format!(
                "column '{}' is not a supported f32 vector column",
                col_name
            )));
        };
        let rows = vectors.len() / dim;
        if rows == 0 {
            return Err(err_data("create_vector_index: the table has no rows"));
        }
        // Cosine distance is invariant to vector length, so the coarse lists and
        // the stored payloads use normalised copies: L2 over them equals the
        // cosine distance of the originals, which keeps every probe exact.
        let vectors = if metric_code == 1 {
            let mut normalised = vectors;
            for row in 0..rows {
                let start = row * dim;
                let norm = normalised[start..start + dim]
                    .iter()
                    .map(|value| value * value)
                    .sum::<f32>()
                    .sqrt();
                if norm > 0.0 {
                    for value in &mut normalised[start..start + dim] {
                        *value /= norm;
                    }
                }
            }
            normalised
        } else {
            vectors
        };
        let nlist = nlist.max(1).min(rows);
        let iterations = iterations.max(1);
        let (centroids, assignments, inertia) =
            kmeans_ivf(&vectors, rows, dim, nlist, iterations);

        let mut list_offsets = vec![0u64; nlist + 1];
        for &assignment in &assignments {
            list_offsets[assignment as usize + 1] += 1;
        }
        for index in 1..list_offsets.len() {
            list_offsets[index] += list_offsets[index - 1];
        }
        let mut order: Vec<u32> = (0..rows as u32).collect();
        order.sort_unstable_by_key(|&row| assignments[row as usize]);
        let mut payload = Vec::with_capacity(
            32 + centroids.len() * 4 + list_offsets.len() * 8 + order.len() * 4 + order.len() * dim * 4,
        );
        payload.extend_from_slice(b"AVIX");
        payload.push(2);
        payload.push(metric_code);
        payload.extend_from_slice(&(dim as u32).to_le_bytes());
        payload.extend_from_slice(&(nlist as u32).to_le_bytes());
        payload.extend_from_slice(&(rows as u64).to_le_bytes());
        payload.extend_from_slice(&inertia.to_le_bytes());
        payload.extend_from_slice(&[0u8, 0u8]); // pad to a 32-byte, 4-aligned header
        for centroid in &centroids {
            payload.extend_from_slice(&centroid.to_le_bytes());
        }
        for offset in &list_offsets {
            payload.extend_from_slice(&offset.to_le_bytes());
        }
        for row in &order {
            payload.extend_from_slice(&row.to_le_bytes());
        }
        // v2: per-list contiguous vector copies, in the same order as row_ids.
        // Probes then read each selected list as one sequential block instead of
        // chasing random rows through the original column buffer.
        for &row in &order {
            let row = row as usize;
            for index in 0..dim {
                payload.extend_from_slice(&vectors[row * dim + index].to_le_bytes());
            }
        }
        let path = self.vector_index_path();
        let tmp = path.with_extension("ivf.tmp");
        std::fs::write(&tmp, &payload)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }


    /// Remove the persisted coarse index. Returns whether a file was removed.
    pub fn drop_vector_index(&self) -> io::Result<bool> {
        match std::fs::remove_file(self.vector_index_path()) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }
    fn vector_index_mmap(
        &self,
    ) -> io::Result<Option<std::sync::Arc<memmap2::Mmap>>> {
        let path = self.vector_index_path();
        let metadata = match std::fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let modified = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos() as u64)
            .unwrap_or(0);
        if let Some((length, cached_modified, mmap)) = IVF_MMAP_CACHE.lock().get(&path) {
            if *length == metadata.len() && *cached_modified == modified {
                return Ok(Some(mmap.clone()));
            }
        }
        let file = std::fs::File::open(&path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        let mmap = std::sync::Arc::new(mmap);
        IVF_MMAP_CACHE.lock().insert(
            path,
            (metadata.len(), modified, mmap.clone()),
        );
        Ok(Some(mmap))
    }

    /// `(version, dim, nlist, rows, inertia)` from an index file header.
    fn vector_index_header(bytes: &[u8]) -> io::Result<(u8, usize, usize, usize, f64)> {
        if bytes.len() < 32 || &bytes[0..4] != b"AVIX" {
            return Err(err_data("vector index file is not an AVIX index"));
        }
        Ok((
            bytes[4],
            u32::from_le_bytes(bytes[6..10].try_into().unwrap()) as usize,
            u32::from_le_bytes(bytes[10..14].try_into().unwrap()) as usize,
            u64::from_le_bytes(bytes[14..22].try_into().unwrap()) as usize,
            f64::from_le_bytes(bytes[22..30].try_into().unwrap()),
        ))
    }

    /// Read the persisted coarse index metadata, if one exists.
    pub fn vector_index_info(&self) -> io::Result<Option<VectorIndexInfo>> {
        let Some(mmap) = self.vector_index_mmap()? else {
            return Ok(None);
        };
        let bytes: &[u8] = &mmap;
        let (version, dim, nlist, rows, inertia) = Self::vector_index_header(bytes)?;
        let metric = match bytes[5] {
            0 => "l2".to_string(),
            1 => "cosine".to_string(),
            other => format!("metric_{other}"),
        };
        let offsets_start = 32 + nlist * dim * 4;
        let offsets_end = offsets_start + (nlist + 1) * 8;
        if offsets_end > bytes.len() {
            return Err(err_data("vector index file is truncated"));
        }
        let mut list_sizes = Vec::with_capacity(nlist);
        for index in 0..nlist {
            let start = u64::from_le_bytes(
                bytes[offsets_start + index * 8..offsets_start + index * 8 + 8]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let end = u64::from_le_bytes(
                bytes[offsets_start + (index + 1) * 8..offsets_start + (index + 1) * 8 + 8]
                    .try_into()
                    .unwrap(),
            ) as usize;
            list_sizes.push(end.saturating_sub(start));
        }
        let stale = self.active_row_count() as usize != rows;
        Ok(Some(VectorIndexInfo {
            version,
            dim,
            nlist,
            rows,
            metric,
            inertia,
            list_sizes,
            stale,
        }))
    }

    /// Exact distances over the rows of the closest `nprobe` inverted lists.
    ///
    /// Returns positions in the active row space (the same space the index
    /// stores) and their l2 distances, sorted ascending. With `nprobe == nlist`
    /// this equals the brute-force TopK.
    pub fn topk_ivf(
        &self,
        col_name: &str,
        query: &[f32],
        k: usize,
        nprobe: usize,
        metric: &str,
    ) -> io::Result<Option<Vec<(usize, f32)>>> {
        let Some(mmap) = self.vector_index_mmap()? else {
            return Ok(None);
        };
        let bytes: &[u8] = &mmap;
        let (version, dim, nlist, rows, _inertia) = Self::vector_index_header(bytes)?;
        if dim != query.len() {
            return Err(err_data(format!(
                "vector index dimension {dim} does not match the query dimension {}",
                query.len()
            )));
        }
        let active = self.active_row_count() as usize;
        if active != rows {
            return Err(err_data(format!(
                "vector index is stale: built over {rows} rows but the table now has {active}; rebuild it with create_vector_index"
            )));
        }
        let centroids_start = 32;
        let centroids_end = centroids_start + nlist * dim * 4;
        let offsets_end = centroids_end + (nlist + 1) * 8;
        let ids_end = offsets_end + rows * 4;
        if ids_end > bytes.len() {
            return Err(err_data("vector index file is truncated"));
        }
        let mut centroids = Vec::with_capacity(nlist * dim);
        for chunk in bytes[centroids_start..centroids_end].chunks_exact(4) {
            centroids.push(f32::from_le_bytes(chunk.try_into().unwrap()));
        }
        let mut offsets = Vec::with_capacity(nlist + 1);
        for chunk in bytes[centroids_end..offsets_end].chunks_exact(8) {
            offsets.push(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        let mut row_ids = Vec::with_capacity(rows);
        for chunk in bytes[offsets_end..ids_end].chunks_exact(4) {
            row_ids.push(u32::from_le_bytes(chunk.try_into().unwrap()));
        }
        let requested_metric = vector_index_metric_code(metric)?;
        if requested_metric != bytes[5] {
            return Err(err_data(
                "topk_distance metric does not match the coarse index; rebuild the index for this metric",
            ));
        }
        let metric = vector_index_metric(bytes[5])?;
        let computer = crate::compute::vector_ops::DistanceComputer::new(
            metric,
            query.to_vec(),
        );
        let mut lists: Vec<(f32, usize)> = (0..nlist)
            .map(|list| {
                let centroid = &centroids[list * dim..(list + 1) * dim];
                (computer.compute(centroid), list)
            })
            .collect();
        lists.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
        let probe = nprobe.clamp(1, nlist);
        let mut best: Vec<(usize, f32)> = Vec::with_capacity(k.saturating_mul(probe).max(16));

        if version >= 2 {
            use crate::compute::vector_ops::topk_heap_on_floats;
            let vectors_end = ids_end + rows * dim * 4;
            if vectors_end > bytes.len() {
                return Err(err_data("vector index vector payload is truncated"));
            }
            let vector_bytes = &bytes[ids_end..vectors_end];
            debug_assert_eq!(vector_bytes.as_ptr() as usize % 4, 0);
            // SAFETY: the header is padded to 32 bytes and every preceding section
            // is a multiple of 4 bytes, so the payload is 4-byte aligned; the
            // mapping outlives this borrow.
            let list_vectors: &[f32] =
                unsafe { std::slice::from_raw_parts(vector_bytes.as_ptr() as *const f32, rows * dim) };
            for (_, list) in lists.iter().take(probe) {
                let start = offsets[*list] as usize;
                let end = offsets[*list + 1] as usize;
                if start >= end || end > row_ids.len() {
                    continue;
                }
                let block = &list_vectors[start * dim..end * dim];
                let take = k.min(end - start);
                for (position, distance) in
                    topk_heap_on_floats(block, end - start, dim, &computer, take)
                {
                    if let Some(&row) = row_ids.get(start + position) {
                        best.push((row as usize, distance));
                    }
                }
            }
            if best.len() > k {
                best.select_nth_unstable_by(k - 1, |left, right| left.1.total_cmp(&right.1));
                best.truncate(k);
            }
            best.sort_unstable_by(|left, right| left.1.total_cmp(&right.1));
            return Ok(Some(best));
        }

        // v1 index files carry no vector payload: they were only ever built for
        // L2, so keep the legacy scalar path for them.
        if bytes[5] != 0 {
            return Err(err_data(
                "legacy v1 vector indexes only support the l2 metric; rebuild the index",
            ));
        }
        let fingerprint = std::fs::metadata(&self.path).ok().map(|metadata| {
            let modified = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_nanos() as u64)
                .unwrap_or(0);
            (metadata.len(), modified)
        });
        let cache_key = (self.path.clone(), col_name.to_string());
        let cached = fingerprint.and_then(|(length, modified)| {
            IVF_VECTOR_CACHE
                .lock()
                .get(&cache_key)
                .filter(|(cached_length, cached_modified, _, _)| {
                    *cached_length == length && *cached_modified == modified
                })
                .map(|(_, _, vectors, dim)| (vectors.clone(), *dim))
        });
        let (vectors, column_dim) = match cached {
            Some((vectors, dim)) => (vectors, dim),
            None => {
                let Some((vectors, dim)) = self.collect_vector_rows(col_name)? else {
                    return Ok(None);
                };
                let vectors = std::sync::Arc::new(vectors);
                if let Some((length, modified)) = fingerprint {
                    let mut cache = IVF_VECTOR_CACHE.lock();
                    cache.insert(cache_key, (length, modified, vectors.clone(), dim));
                }
                (vectors, dim)
            }
        };
        let rows = vectors.len() / column_dim;
        let mut candidates: Vec<usize> = Vec::new();
        for (_, list) in lists.into_iter().take(probe) {
            let start = offsets[list] as usize;
            let end = offsets[list + 1] as usize;
            for &row in &row_ids[start..end.min(row_ids.len())] {
                let row = row as usize;
                if row < rows {
                    candidates.push(row);
                }
            }
        }
        let score = |row: usize| -> (usize, f32) {
            let vector = &vectors[row * column_dim..(row + 1) * column_dim];
            let mut distance = 0f32;
            for index in 0..dim {
                let delta = query[index] - vector[index];
                distance += delta * delta;
            }
            (row, distance.sqrt())
        };
        let scored: Vec<(usize, f32)> = if candidates.len() >= 8192 {
            candidates.par_iter().map(|&row| score(row)).collect()
        } else {
            candidates.iter().map(|&row| score(row)).collect()
        };
        let mut best = scored;
        if best.len() > k {
            best.select_nth_unstable_by(k - 1, |left, right| left.1.total_cmp(&right.1));
            best.truncate(k);
        }
        best.sort_unstable_by(|left, right| left.1.total_cmp(&right.1));
        Ok(Some(best))
    }
}



/// Deterministic k-means over a flat row-major f32 buffer.
///
/// Centroid seeding takes evenly spaced rows so repeated builds are identical;
/// empty clusters keep their previous centroid. Returns (centroids, assignment
/// per row, final inertia).
fn kmeans_ivf(
    vectors: &[f32],
    rows: usize,
    dim: usize,
    nlist: usize,
    iterations: usize,
) -> (Vec<f32>, Vec<u32>, f64) {
    let mut centroids = vec![0f32; nlist * dim];
    for cluster in 0..nlist {
        let row = cluster * rows / nlist;
        centroids[cluster * dim..(cluster + 1) * dim]
            .copy_from_slice(&vectors[row * dim..(row + 1) * dim]);
    }
    let mut assignments = vec![0u32; rows];
    for _ in 0..iterations {
        let mut sums = vec![0f32; nlist * dim];
        let mut counts = vec![0u64; nlist];
        for row in 0..rows {
            let vector = &vectors[row * dim..(row + 1) * dim];
            let mut best = 0usize;
            let mut best_distance = f32::INFINITY;
            for cluster in 0..nlist {
                let centroid = &centroids[cluster * dim..(cluster + 1) * dim];
                let mut distance = 0f32;
                for index in 0..dim {
                    let delta = vector[index] - centroid[index];
                    distance += delta * delta;
                }
                if distance < best_distance {
                    best_distance = distance;
                    best = cluster;
                }
            }
            assignments[row] = best as u32;
            counts[best] += 1;
            let target = &mut sums[best * dim..(best + 1) * dim];
            for index in 0..dim {
                target[index] += vector[index];
            }
        }
        for cluster in 0..nlist {
            if counts[cluster] == 0 {
                continue;
            }
            let inverse = 1.0f32 / counts[cluster] as f32;
            for index in 0..dim {
                centroids[cluster * dim + index] = sums[cluster * dim + index] * inverse;
            }
        }
    }
    let mut inertia = 0f64;
    for row in 0..rows {
        let vector = &vectors[row * dim..(row + 1) * dim];
        let centroid =
            &centroids[assignments[row] as usize * dim..(assignments[row] as usize + 1) * dim];
        let mut distance = 0f64;
        for index in 0..dim {
            let delta = (vector[index] - centroid[index]) as f64;
            distance += delta * delta;
        }
        inertia += distance;
    }
    (centroids, assignments, inertia)
}
