use super::*;

// Mmap scanning, footer loading, column range readers, filter+group+order fast path

/// Sorted-column cache: `(path, col_idx) -> (mtime, epoch, is_sorted)`.
/// A sorted string column lets prefix-LIKE range scans binary-search instead of
/// scanning every row.
static SORTED_COL_CACHE: once_cell::sync::Lazy<
    parking_lot::RwLock<
        std::collections::HashMap<
            (std::path::PathBuf, usize),
            (std::time::SystemTime, u64, bool),
        >,
    >,
> = once_cell::sync::Lazy::new(|| parking_lot::RwLock::new(std::collections::HashMap::new()));

/// Safe: cast byte slice to &[i64]. Falls back to owned Vec when pointer is not 8-byte aligned.
#[inline(always)]
pub(super) fn bytes_as_i64_slice(bytes: &[u8], n: usize) -> std::borrow::Cow<'_, [i64]> {
    let ptr = bytes.as_ptr();
    if ptr as usize % 8 == 0 && bytes.len() >= n * 8 {
        std::borrow::Cow::Borrowed(unsafe { std::slice::from_raw_parts(ptr as *const i64, n) })
    } else {
        std::borrow::Cow::Owned(
            (0..n)
                .map(|i| i64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap()))
                .collect(),
        )
    }
}

/// Safe: cast byte slice to &[f64]. Falls back to owned Vec when pointer is not 8-byte aligned.
#[inline(always)]
pub(super) fn bytes_as_f64_slice(bytes: &[u8], n: usize) -> std::borrow::Cow<'_, [f64]> {
    let ptr = bytes.as_ptr();
    if ptr as usize % 8 == 0 && bytes.len() >= n * 8 {
        std::borrow::Cow::Borrowed(unsafe { std::slice::from_raw_parts(ptr as *const f64, n) })
    } else {
        std::borrow::Cow::Owned(
            (0..n)
                .map(|i| f64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap()))
                .collect(),
        )
    }
}

/// Safe: cast byte slice to &[u64]. Falls back to owned Vec when pointer is not 8-byte aligned.
#[inline(always)]
pub(super) fn bytes_as_u64_slice(bytes: &[u8], n: usize) -> std::borrow::Cow<'_, [u64]> {
    let ptr = bytes.as_ptr();
    if ptr as usize % 8 == 0 && bytes.len() >= n * 8 {
        std::borrow::Cow::Borrowed(unsafe { std::slice::from_raw_parts(ptr as *const u64, n) })
    } else {
        std::borrow::Cow::Owned(
            (0..n)
                .map(|i| u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap()))
                .collect(),
        )
    }
}

/// Safe: cast byte slice to &[u32]. Falls back to owned Vec when pointer is not 4-byte aligned.
#[inline(always)]
pub(super) fn bytes_as_u32_slice(bytes: &[u8], n: usize) -> std::borrow::Cow<'_, [u32]> {
    let ptr = bytes.as_ptr();
    if ptr as usize % 4 == 0 && bytes.len() >= n * 4 {
        std::borrow::Cow::Borrowed(unsafe { std::slice::from_raw_parts(ptr as *const u32, n) })
    } else {
        std::borrow::Cow::Owned(
            (0..n)
                .map(|i| u32::from_le_bytes(bytes[i * 4..i * 4 + 4].try_into().unwrap()))
                .collect(),
        )
    }
}

/// Find a little-endian u32 in a sorted byte-backed array without requiring
/// the mmap address to be aligned. Sparse string equality scans only need the
/// matching offset and its successor, so decoding the whole offsets table on
/// an unaligned column would add avoidable O(rows) work to an O(matches log rows)
/// path.
#[inline(always)]
fn binary_search_u32_le(bytes: &[u8], n: usize, target: u32) -> Option<usize> {
    if bytes.len() < n.saturating_mul(4) {
        return None;
    }
    let mut left = 0usize;
    let mut right = n;
    while left < right {
        let mid = left + (right - left) / 2;
        let offset = mid * 4;
        let value = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        match value.cmp(&target) {
            std::cmp::Ordering::Less => left = mid + 1,
            std::cmp::Ordering::Greater => right = mid,
            std::cmp::Ordering::Equal => return Some(mid),
        }
    }
    None
}

#[inline(always)]
fn u32_le_at(bytes: &[u8], index: usize) -> Option<u32> {
    let offset = index.checked_mul(4)?;
    let end = offset.checked_add(4)?;
    Some(u32::from_le_bytes(bytes.get(offset..end)?.try_into().ok()?))
}

#[inline(always)]
pub(super) fn bitpack_value_at(
    packed: &[u8],
    bit_width: usize,
    min_val: i64,
    idx: usize,
) -> Option<i64> {
    if bit_width == 0 {
        return Some(min_val);
    }
    if bit_width >= 64 {
        return None;
    }
    let bit_pos = idx.checked_mul(bit_width)?;
    let byte_off = bit_pos / 8;
    let bit_shift = bit_pos % 8;
    let mask = (1u64 << bit_width) - 1;

    if bit_shift + bit_width <= 64 && byte_off + 8 <= packed.len() {
        let raw = unsafe { std::ptr::read_unaligned(packed.as_ptr().add(byte_off) as *const u64) };
        return Some(min_val.wrapping_add(((raw >> bit_shift) & mask) as i64));
    }

    let bytes_needed = (bit_shift + bit_width + 7) / 8;
    if byte_off + bytes_needed > packed.len() {
        return None;
    }
    let mut raw = 0u64;
    for j in 0..bytes_needed {
        raw |= (packed[byte_off + j] as u64) << (j * 8);
    }
    Some(min_val.wrapping_add(((raw >> bit_shift) & mask) as i64))
}

pub(crate) enum MmapBatchColumn {
    I64(Vec<Option<i64>>),
    F64(Vec<Option<f64>>),
    Str(Vec<Option<String>>),
    Bool(Vec<Option<bool>>),
    Bin(Vec<Option<Vec<u8>>>),
}

pub(crate) struct MmapBatchColumns {
    pub(crate) row_count: usize,
    pub(crate) columns: Vec<(String, MmapBatchColumn)>,
}

// ─── MULTI-PREDICATE PARALLEL SCAN ──────────────────────────────────────────

/// Scan predicate for `scan_multi_predicates_parallel`.
/// Each variant targets a single column and can be scanned independently.
pub enum MmapScanPred<'a> {
    NumericRange { col: &'a str, low: f64, high: f64 },
    StringEq { col: &'a str, value: &'a str },
    NumericIn { col: &'a str, values: &'a [i64] },
    StringIn { col: &'a str, values: &'a [String] },
}

// ─── LIKE PATTERN SUPPORT ────────────────────────────────────────────────────

/// Pre-classified LIKE pattern for zero-alloc byte-level matching.
/// Owned pattern bytes allow thread-safe sharing across Rayon parallel tasks.
#[derive(Clone)]
pub(crate) enum LikeKind {
    /// 'prefix%' — match strings starting with prefix bytes
    Prefix(Vec<u8>),
    /// '%suffix' — match strings ending with suffix bytes
    Suffix(Vec<u8>),
    /// '%substr%' — memmem scan within string bytes
    Contains(Vec<u8>),
    /// 'prefix_suffix%' — prefix, one `_` wildcard (any byte), a literal suffix,
    /// then `%`. Matched with two fixed-offset byte compares instead of a regex.
    PrefixWildcardSuffix { prefix: Vec<u8>, suffix: Vec<u8> },
    /// '%' — match all non-null rows
    Any,
    /// Complex pattern with '_' or multiple '%' — compiled regex
    Regex(regex::Regex),
}

/// Pre-compiled finder for contains patterns (much faster than on-the-fly)
/// Uses memchr's precompilation which caches SIMD state
pub struct PrecompiledFinder {
    finder: memchr::memmem::Finder<'static>,
}

/// Test whether a raw byte slice matches a LikeKind pattern.
/// Must not allocate — called inside Rayon parallel closures.
#[inline(always)]
pub(crate) fn like_matches_bytes(kind: &LikeKind, s: &[u8]) -> bool {
    match kind {
        LikeKind::Prefix(p) => s.len() >= p.len() && fast_eq(p, s),
        LikeKind::Suffix(p) => s.len() >= p.len() && fast_eq(p, &s[s.len() - p.len()..]),
        LikeKind::Contains(p) => memchr::memmem::find(s, p).is_some(),
        LikeKind::PrefixWildcardSuffix { prefix, suffix } => {
            let need = prefix.len() + 1 + suffix.len();
            s.len() >= need
                && s.starts_with(prefix.as_slice())
                && &s[prefix.len() + 1..prefix.len() + 1 + suffix.len()] == suffix.as_slice()
        }
        LikeKind::Any => true,
        LikeKind::Regex(re) => std::str::from_utf8(s)
            .map(|st| re.is_match(st))
            .unwrap_or(false),
    }
}

/// Fast equality check using memchr's optimized comparison
/// Uses word-at-a-time comparison for longer prefixes
#[inline(always)]
fn fast_eq(pattern: &[u8], s: &[u8]) -> bool {
    if pattern.len() != s.len() {
        if pattern.len() > s.len() {
            return false;
        }
        // For prefix match: just check first pattern.len() bytes
        if s.len() < pattern.len() {
            return false;
        }
    }
    pattern == &s[..pattern.len()]
}

/// Classify a SQL LIKE pattern into a LikeKind for fast byte-level matching.
/// Returns None when the pattern has no wildcards (exact match → use scan_string_filter_mmap).
pub(crate) fn classify_like_pattern(pattern: &str) -> Option<LikeKind> {
    let pb = pattern.as_bytes();
    let plen = pb.len();
    if plen == 0 {
        return None;
    }
    if !pb.contains(&b'%') && !pb.contains(&b'_') {
        return None;
    }
    if pattern == "%" {
        return Some(LikeKind::Any);
    }
    let sw = pb[0] == b'%';
    let ew = pb[plen - 1] == b'%';
    if !sw && ew {
        let prefix = &pattern[..plen - 1];
        if !prefix.contains('%') && !prefix.contains('_') {
            return Some(LikeKind::Prefix(prefix.as_bytes().to_vec()));
        }
        // `literal_ suffix %` — a single `_` wildcard: match with two fixed
        // byte compares (much faster than compiling a regex per pattern).
        if let Some(pos) = prefix.find('_') {
            let head = &prefix[..pos];
            let tail = &prefix[pos + 1..];
            if !head.contains('%')
                && !head.contains('_')
                && !tail.contains('%')
                && !tail.contains('_')
                && !tail.is_empty()
            {
                return Some(LikeKind::PrefixWildcardSuffix {
                    prefix: head.as_bytes().to_vec(),
                    suffix: tail.as_bytes().to_vec(),
                });
            }
        }
    }
    if sw && !ew {
        let suffix = &pattern[1..];
        if !suffix.contains('%') && !suffix.contains('_') {
            return Some(LikeKind::Suffix(suffix.as_bytes().to_vec()));
        }
    }
    if sw && ew && plen > 2 {
        let middle = &pattern[1..plen - 1];
        if !middle.contains('%') && !middle.contains('_') {
            return Some(LikeKind::Contains(middle.as_bytes().to_vec()));
        }
    }
    // Complex pattern → compile regex once, reuse across all rows
    let mut re_str = String::with_capacity(plen * 2 + 2);
    re_str.push('^');
    for c in pattern.chars() {
        match c {
            '%' => re_str.push_str(".*"),
            '_' => re_str.push('.'),
            '.' | '+' | '*' | '?' | '^' | '$' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\' => {
                re_str.push('\\');
                re_str.push(c);
            }
            _ => re_str.push(c),
        }
    }
    re_str.push('$');
    regex::Regex::new(&re_str).ok().map(LikeKind::Regex)
}

// ─────────────────────────────────────────────────────────────────────────────

impl OnDemandStorage {
    pub fn scan_string_filter_mmap(
        &self,
        col_name: &str,
        target: &str,
        limit: Option<usize>,
    ) -> io::Result<Option<Vec<usize>>> {
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
        if !matches!(col_type, ColumnType::String | ColumnType::StringDict) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("column '{col_name}' is not a string column"),
            ));
        }
        let col_count = schema.column_count();
        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for string scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let target_bytes = target.as_bytes();
        let max_matches = limit.unwrap_or(usize::MAX);
        let mut matches: Vec<usize> = Vec::new();
        let mut global_row_offset: usize = 0;
        // Build the memmem Finder once (precomputes Boyer-Moore table from target_bytes)
        let memmem_finder = memchr::memmem::Finder::new(target_bytes);

        // ── PARALLEL FAST PATH ───────────────────────────────────────────────
        // For no-limit StringDict scans on uncompressed+RCIX data, scan all RGs
        // in parallel using Rayon to exploit all CPU cores simultaneously.
        if limit.is_none()
            && footer.row_groups.len() > 1
            && matches!(col_type, ColumnType::StringDict)
            && !target_bytes.is_empty()
        {
            // Check whether every RG qualifies for the parallel fast path
            let all_fast = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
                let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
                if rg_end > mmap_ref.len() {
                    return false;
                }
                let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
                let compress_flag = rg_bytes.get(28).copied().unwrap_or(RG_COMPRESS_NONE);
                let enc_ver = rg_bytes.get(29).copied().unwrap_or(0);
                compress_flag == RG_COMPRESS_NONE
                    && enc_ver >= 1
                    && footer
                        .col_offsets
                        .get(rg_i)
                        .map_or(false, |v| v.len() > col_idx)
            });

            if all_fast {
                // Cast pointer to usize (Send+Sync) — safe because mmap_guard keeps Mmap
                // alive for the entire scope and all parallel tasks are read-only.
                let mmap_ptr: usize = mmap_ref.as_ptr() as usize;
                let mmap_len: usize = mmap_ref.len();

                // Build per-RG scan descriptors upfront
                struct RgDesc {
                    rg_offset: usize,
                    rg_data_size: usize,
                    rg_rows: usize,
                    global_off: usize,
                    col_rcix: usize,
                    has_deletes: bool,
                    id_section_len: usize,
                }
                let mut rg_descs: Vec<RgDesc> = Vec::with_capacity(footer.row_groups.len());
                let target_len_i64 = target_bytes.len() as i64;
                let mut off = 0usize;
                for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
                    let global_off = off;
                    off += rg_meta.row_count as usize;

                    if let Some(zmaps) = footer.zone_maps.get(rg_i) {
                        if let Some(zm) = zmaps
                            .iter()
                            .find(|z| z.col_idx as usize == col_idx && !z.is_float)
                        {
                            if target_len_i64 < zm.min_bits || target_len_i64 > zm.max_bits {
                                continue;
                            }
                        }
                    }

                    rg_descs.push(RgDesc {
                        rg_offset: rg_meta.offset as usize,
                        rg_data_size: rg_meta.data_size as usize,
                        rg_rows: rg_meta.row_count as usize,
                        global_off,
                        col_rcix: footer.col_offsets[rg_i][col_idx] as usize,
                        has_deletes: rg_meta.deletion_count > 0,
                        id_section_len: rg_id_section_len(
                            rg_meta.row_count as usize,
                            mmap_ref
                                .get(rg_meta.offset as usize + 30)
                                .copied()
                                .unwrap_or(RG_IDS_PLAIN),
                        ),
                    });
                }

                let scan_rg = |desc: &RgDesc| {
                        let mmap =
                            unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                        let rg_end = desc.rg_offset + desc.rg_data_size;
                        if rg_end > mmap.len() || rg_end < desc.rg_offset + 32 {
                            return vec![];
                        }
                        let rg_bytes = &mmap[desc.rg_offset..rg_end];
                        let body = &rg_bytes[32..];
                        let rg_rows = desc.rg_rows;
                        let null_bitmap_len = (rg_rows + 7) / 8;
                        let del_vec_len = null_bitmap_len;
                        let del_start = desc.id_section_len;

                        let col_off = desc.col_rcix;
                        if col_off + null_bitmap_len > body.len() {
                            return vec![];
                        }
                        let col_bytes = &body[col_off + null_bitmap_len..];
                        if col_bytes.is_empty() {
                            return vec![];
                        }
                        let encoding = col_bytes[0];
                        if !matches!(
                            encoding,
                            COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                        ) {
                            return vec![];
                        }
                        let data = &col_bytes[1..];
                        let Ok(view) = StringDictView::parse(
                            data,
                            encoding == COL_ENCODING_COMPACT_DICTIONARY,
                        ) else {
                            return vec![];
                        };
                        let Some(tdi) = view.find_value(target_bytes) else {
                            return vec![];
                        };

                        let n = view.row_count.min(rg_rows);
                        let mut local: Vec<usize> = Vec::new();
                        if !desc.has_deletes {
                            view.for_each_index_eq(tdi, n, |i| {
                                local.push(desc.global_off + i);
                                true
                            });
                        } else {
                            if del_start + del_vec_len > body.len() {
                                return local;
                            }
                            let del_bytes = &body[del_start..del_start + del_vec_len];
                            view.for_each_index_eq(tdi, n, |i| {
                                if (del_bytes[i / 8] >> (i % 8)) & 1 == 0 {
                                    local.push(desc.global_off + i);
                                }
                                true
                            });
                        }
                        local
                    };
                let all_rg_matches: Vec<Vec<usize>> = if rg_descs.len() <= 1 {
                    rg_descs.iter().map(&scan_rg).collect()
                } else {
                    rg_descs.par_iter().map(scan_rg).collect()
                };

                // Merge results in RG order (already ordered since RGs are enumerated in order)
                matches = all_rg_matches.into_iter().flatten().collect();
                return Ok(Some(matches));
            }
        }
        // ── END PARALLEL FAST PATH ───────────────────────────────────────────

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            if matches.len() >= max_matches {
                break;
            }
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                global_row_offset += rg_rows;
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
            let del_vec_len = (rg_rows + 7) / 8;
            let del_start =
                rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));
            if del_start + del_vec_len > body.len() {
                return Err(err_data("RG del vec truncated"));
            }
            let del_bytes = &body[del_start..del_start + del_vec_len];
            let has_deletes = rg_meta.deletion_count > 0;
            let null_bitmap_len = (rg_rows + 7) / 8;

            // Zone-map skip: if RG has a string-length zone map for this column,
            // skip the entire RG when target_len is outside [min_len, max_len].
            // This eliminates scanning 15/16 RGs for typical queries (e.g. 9-char target
            // in a dataset where only RG0 has 9-char strings).
            if let Some(zmaps) = footer.zone_maps.get(rg_i) {
                if let Some(zm) = zmaps
                    .iter()
                    .find(|z| z.col_idx as usize == col_idx && !z.is_float)
                {
                    let tlen = target_bytes.len() as i64;
                    if tlen < zm.min_bits || tlen > zm.max_bits {
                        global_row_offset += rg_rows;
                        continue;
                    }
                }
            }

            // RCIX fast path: jump directly to target column without scanning all columns
            let rcix = if compress_flag == RG_COMPRESS_NONE && encoding_version >= 1 {
                footer.col_offsets.get(rg_i).filter(|v| v.len() > col_idx)
            } else {
                None
            };

            if let Some(rcix) = rcix {
                let col_off = rcix[col_idx] as usize;
                if col_off + null_bitmap_len > body.len() {
                    global_row_offset += rg_rows;
                    continue;
                }
                let null_bytes = &body[col_off..col_off + null_bitmap_len];
                let ct = schema.columns[col_idx].1;
                let col_bytes = &body[col_off + null_bitmap_len..];
                {
                    let enc_offset = if encoding_version >= 1 { 1 } else { 0 };
                    let encoding = if encoding_version >= 1 {
                        col_bytes[0]
                    } else {
                        COL_ENCODING_PLAIN
                    };
                    let data = &col_bytes[enc_offset..];

                    if encoding == COL_ENCODING_PLAIN
                        && matches!(ct, ColumnType::String)
                        && data.len() >= 8
                    {
                        let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
                        let all_offsets_len = (count + 1) * 4;
                        if 8 + all_offsets_len <= data.len() {
                            let data_len_off = 8 + all_offsets_len;
                            if data_len_off + 8 <= data.len() {
                                let data_start = data_len_off + 8;
                                let offsets_bytes = &data[8..8 + all_offsets_len];
                                let target_len = target_bytes.len();
                                let n = count.min(rg_rows);
                                // FAST: memmem scan raw string data + binary search boundary check
                                // Replaces O(n) sequential scan with O(scan + k·log n) where k = rare hits
                                let data_len_val = u64::from_le_bytes(
                                    data[data_len_off..data_len_off + 8]
                                        .try_into()
                                        .unwrap_or([0; 8]),
                                ) as usize;
                                let raw_end = (data_start + data_len_val).min(data.len());
                                let raw_str = &data[data_start..raw_end];
                                let mut search_from = 0usize;
                                while let Some(rel) = memmem_finder.find(&raw_str[search_from..]) {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    let abs = search_from + rel;
                                    // Binary search: find if abs is a valid string start offset
                                    if let Some(di) =
                                        binary_search_u32_le(offsets_bytes, count, abs as u32)
                                    {
                                        let end_off =
                                            u32_le_at(offsets_bytes, di + 1).unwrap_or(0) as usize;
                                        if end_off >= abs
                                            && end_off - abs == target_len
                                            && di < n
                                        {
                                            // Verify not deleted / null
                                            let skip = if has_deletes {
                                                (del_bytes[di / 8] >> (di % 8)) & 1 == 1
                                            } else {
                                                false
                                            };
                                            if !skip && (null_bytes[di / 8] >> (di % 8)) & 1 == 0 {
                                                matches.push(global_row_offset + di);
                                            }
                                        }
                                    }
                                    search_from += rel + 1;
                                    if search_from >= raw_str.len() {
                                        break;
                                    }
                                }
                            }
                        }
                    } else if encoding != COL_ENCODING_PLAIN && matches!(ct, ColumnType::String) {
                        // Non-PLAIN encoded String: decode then scan
                        let (col_data, _consumed) = if encoding_version >= 1 {
                            read_column_encoded(col_bytes, ct)?
                        } else {
                            ColumnData::from_bytes_typed(col_bytes, ct)?
                        };
                        if let ColumnData::String {
                            offsets,
                            data: str_data,
                        } = &col_data
                        {
                            let count = offsets.len().saturating_sub(1);
                            for i in 0..count.min(rg_rows) {
                                if matches.len() >= max_matches {
                                    break;
                                }
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                let s = offsets[i] as usize;
                                let e = offsets[i + 1] as usize;
                                if e - s == target_bytes.len() && &str_data[s..e] == target_bytes {
                                    matches.push(global_row_offset + i);
                                }
                            }
                        }
                    } else if matches!(ct, ColumnType::StringDict)
                        && matches!(
                            encoding,
                            COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                        )
                    {
                        let view = StringDictView::parse(
                            data,
                            encoding == COL_ENCODING_COMPACT_DICTIONARY,
                        )?;
                        if let Some(target_dict_idx) = view.find_value(target_bytes) {
                            let n = view.row_count.min(rg_rows);
                            view.for_each_index_eq(target_dict_idx, n, |i| {
                                if matches.len() >= max_matches {
                                    return false;
                                }
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    return true;
                                }
                                matches.push(global_row_offset + i);
                                true
                            });
                        }
                    }
                }
            } else {
                // Fallback: sequential pos scan for compressed or pre-RCIX row groups
                let mut pos = del_start + del_vec_len;
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
                        let encoding = if encoding_version >= 1 && !col_bytes.is_empty() {
                            col_bytes[0]
                        } else {
                            COL_ENCODING_PLAIN
                        };
                        let data = if enc_offset <= col_bytes.len() {
                            &col_bytes[enc_offset..]
                        } else {
                            &[]
                        };
                        if encoding == COL_ENCODING_PLAIN
                            && matches!(ct, ColumnType::String)
                            && data.len() >= 8
                        {
                            let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
                            let all_offsets_len = (count + 1) * 4;
                            if 8 + all_offsets_len <= data.len() {
                                let data_len_off = 8 + all_offsets_len;
                                if data_len_off + 8 <= data.len() {
                                    let data_start = data_len_off + 8;
                                    let offsets = bytes_as_u32_slice(&data[8..], count + 1);
                                    let tlen = target_bytes.len();
                                    for i in 0..count.min(rg_rows) {
                                        if matches.len() >= max_matches {
                                            break;
                                        }
                                        if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                            continue;
                                        }
                                        let s = offsets[i] as usize;
                                        let e = offsets[i + 1] as usize;
                                        if e - s == tlen
                                            && data_start + e <= data.len()
                                            && &data[data_start + s..data_start + e] == target_bytes
                                        {
                                            matches.push(global_row_offset + i);
                                        }
                                    }
                                }
                            }
                        } else if matches!(ct, ColumnType::StringDict)
                            && matches!(
                                encoding,
                                COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                            )
                        {
                            let view = StringDictView::parse(
                                data,
                                encoding == COL_ENCODING_COMPACT_DICTIONARY,
                            )?;
                            if let Some(target_dict_idx) = view.find_value(target_bytes) {
                                view.for_each_index_eq(target_dict_idx, rg_rows, |i| {
                                    if matches.len() >= max_matches {
                                        return false;
                                    }
                                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        return true;
                                    }
                                    matches.push(global_row_offset + i);
                                    true
                                });
                            }
                        }
                    }
                    let consumed = if encoding_version >= 1 {
                        skip_column_encoded(&body[pos..], ct)?
                    } else {
                        ColumnData::skip_bytes_typed(&body[pos..], ct)?
                    };
                    pos += consumed;
                }
            }
            global_row_offset += rg_rows;
        }
        drop(mmap_guard);
        drop(file_guard);
        Ok(Some(matches))
    }

    pub fn string_eq_matches_all_mmap(
        &self,
        col_name: &str,
        target: &str,
    ) -> io::Result<Option<bool>> {
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let col_idx = match schema.get_index(col_name) {
            Some(i) => i,
            None => return Ok(None),
        };
        if !matches!(schema.columns[col_idx].1, ColumnType::StringDict) {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for string metadata scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let target_bytes = target.as_bytes();

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            if rg_meta.deletion_count > 0 {
                return Ok(None);
            }
            let rg_start = rg_meta.offset as usize;
            let rg_end = rg_start + rg_meta.data_size as usize;
            if rg_end > mmap_ref.len() || rg_end < rg_start + 32 {
                return Err(err_data("RG extends past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_start..rg_end];
            let compress_flag = rg_bytes.get(28).copied().unwrap_or(RG_COMPRESS_NONE);
            let encoding_version = rg_bytes.get(29).copied().unwrap_or(0);
            if compress_flag != RG_COMPRESS_NONE || encoding_version < 1 {
                return Ok(None);
            }
            let Some(rcix) = footer.col_offsets.get(rg_i).filter(|v| v.len() > col_idx) else {
                return Ok(None);
            };
            let body = &rg_bytes[32..];
            let null_bitmap_len = (rg_rows + 7) / 8;
            let col_off = rcix[col_idx] as usize;
            if col_off + null_bitmap_len > body.len() {
                return Ok(None);
            }
            let null_bytes = &body[col_off..col_off + null_bitmap_len];
            if null_bytes.iter().any(|&byte| byte != 0) {
                return Ok(None);
            }
            let col_bytes = &body[col_off + null_bitmap_len..];
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
                &col_bytes[1..],
                encoding == COL_ENCODING_COMPACT_DICTIONARY,
            ) {
                Ok(view) => view,
                Err(_) => return Ok(None),
            };
            if view.row_count < rg_rows || view.dict_size <= 1 {
                return Ok(None);
            }
            let Some(target_dict_idx) = view.find_value(target_bytes) else {
                return Ok(Some(false));
            };
            for row in 0..rg_rows {
                if view.index(row) != Some(target_dict_idx) {
                    return Ok(Some(false));
                }
            }
        }

        Ok(Some(true))
    }

    pub fn scan_string_in_mmap(
        &self,
        col_name: &str,
        values: &[String],
        limit: Option<usize>,
    ) -> io::Result<Option<Vec<usize>>> {
        use rayon::prelude::*;

        if values.is_empty() {
            return Ok(Some(Vec::new()));
        }
        if values.len() == 1 {
            return self.scan_string_filter_mmap(col_name, &values[0], limit);
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
        let is_dict = matches!(col_type, ColumnType::StringDict);
        let is_string = matches!(col_type, ColumnType::String);
        if !is_dict && !is_string {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for string IN scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let all_fast = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return false;
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            let compress_flag = rg_bytes.get(28).copied().unwrap_or(RG_COMPRESS_NONE);
            let enc_ver = rg_bytes.get(29).copied().unwrap_or(0);
            compress_flag == RG_COMPRESS_NONE
                && enc_ver >= 1
                && footer
                    .col_offsets
                    .get(rg_i)
                    .map_or(false, |v| v.len() > col_idx)
        });

        if !all_fast {
            drop(mmap_guard);
            drop(file_guard);
            let mut all_indices: Vec<usize> = Vec::new();
            for value in values {
                if let Some(mut idxs) = self.scan_string_filter_mmap(col_name, value, None)? {
                    all_indices.append(&mut idxs);
                }
            }
            all_indices.sort_unstable();
            all_indices.dedup();
            if let Some(lim) = limit {
                all_indices.truncate(lim);
            }
            return Ok(Some(all_indices));
        }

        let target_bytes: Vec<&[u8]> = values.iter().map(|s| s.as_bytes()).collect();
        let min_len = values.iter().map(|s| s.len()).min().unwrap_or(0) as i64;
        let max_len = values.iter().map(|s| s.len()).max().unwrap_or(0) as i64;
        let matches_any = |bytes: &[u8]| -> bool {
            target_bytes
                .iter()
                .any(|target| target.len() == bytes.len() && *target == bytes)
        };

        struct RgDesc {
            rg_idx: usize,
            rg_offset: usize,
            rg_data_size: usize,
            rg_rows: usize,
            global_off: usize,
            col_rcix: usize,
            has_deletes: bool,
            id_section_len: usize,
        }

        let mut rg_descs: Vec<RgDesc> = Vec::with_capacity(footer.row_groups.len());
        let mut off = 0usize;
        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            rg_descs.push(RgDesc {
                rg_idx: rg_i,
                rg_offset: rg_meta.offset as usize,
                rg_data_size: rg_meta.data_size as usize,
                rg_rows: rg_meta.row_count as usize,
                global_off: off,
                col_rcix: footer.col_offsets[rg_i][col_idx] as usize,
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

        let scan_rg = |desc: &RgDesc, mmap: &[u8]| -> Vec<usize> {
            let rg_end = desc.rg_offset + desc.rg_data_size;
            if rg_end > mmap.len() || rg_end < desc.rg_offset + 32 {
                return vec![];
            }
            let body = &mmap[desc.rg_offset + 32..rg_end];
            let bitmap_len = (desc.rg_rows + 7) / 8;
            let del_start = desc.id_section_len;
            if desc.col_rcix + bitmap_len > body.len() {
                return vec![];
            }

            let null_bytes = &body[desc.col_rcix..desc.col_rcix + bitmap_len];
            let col_bytes = &body[desc.col_rcix + bitmap_len..];
            let Some(&encoding) = col_bytes.first() else {
                return vec![];
            };
            if (is_string && encoding != COL_ENCODING_PLAIN)
                || (is_dict
                    && !matches!(
                        encoding,
                        COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                    ))
            {
                return vec![];
            }
            let payload = &col_bytes[1..];
            let mut local: Vec<usize> = Vec::new();

            if is_string {
                if payload.len() < 8 {
                    return local;
                }
                let count = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8])) as usize;
                let offsets_len = (count + 1) * 4;
                let data_len_off = 8 + offsets_len;
                if data_len_off + 8 > payload.len() {
                    return local;
                }
                let data_len = u64::from_le_bytes(
                    payload[data_len_off..data_len_off + 8]
                        .try_into()
                        .unwrap_or([0; 8]),
                ) as usize;
                let data_start = data_len_off + 8;
                let data_end = (data_start + data_len).min(payload.len());
                if data_end < data_start {
                    return local;
                }
                let raw = &payload[data_start..data_end];
                let offsets_cow = bytes_as_u32_slice(&payload[8..], count + 1);
                let offsets: &[u32] = &offsets_cow;
                let n = count.min(desc.rg_rows);

                if !desc.has_deletes {
                    for i in 0..n {
                        if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                            continue;
                        }
                        let s = offsets[i] as usize;
                        let e = offsets[i + 1] as usize;
                        if e >= s && e <= raw.len() && matches_any(&raw[s..e]) {
                            local.push(desc.global_off + i);
                        }
                    }
                } else {
                    if del_start + bitmap_len > body.len() {
                        return local;
                    }
                    let del_bytes = &body[del_start..del_start + bitmap_len];
                    for i in 0..n {
                        if (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                            continue;
                        }
                        if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                            continue;
                        }
                        let s = offsets[i] as usize;
                        let e = offsets[i + 1] as usize;
                        if e >= s && e <= raw.len() && matches_any(&raw[s..e]) {
                            local.push(desc.global_off + i);
                        }
                    }
                }
            } else {
                let Ok(view) =
                    StringDictView::parse(payload, encoding == COL_ENCODING_COMPACT_DICTIONARY)
                else {
                    return local;
                };

                let mut match_flags = vec![false; view.dict_size];
                for dict_index in 1..view.dict_size {
                    if view.value(dict_index as u32).is_some_and(&matches_any) {
                        match_flags[dict_index] = true;
                    }
                }

                let n = view.row_count.min(desc.rg_rows);
                if !desc.has_deletes {
                    for i in 0..n {
                        if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                            continue;
                        }
                        let idx = view.index(i).unwrap_or(0) as usize;
                        if idx < match_flags.len() && match_flags[idx] {
                            local.push(desc.global_off + i);
                        }
                    }
                } else {
                    if del_start + bitmap_len > body.len() {
                        return local;
                    }
                    let del_bytes = &body[del_start..del_start + bitmap_len];
                    for i in 0..n {
                        if (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                            continue;
                        }
                        if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                            continue;
                        }
                        let idx = view.index(i).unwrap_or(0) as usize;
                        if idx < match_flags.len() && match_flags[idx] {
                            local.push(desc.global_off + i);
                        }
                    }
                }
            }

            local
        };

        let max_matches = limit.unwrap_or(usize::MAX);
        let mut matches: Vec<usize> = if limit.is_none() && rg_descs.len() > 1 {
            let mmap_ptr = mmap_ref.as_ptr() as usize;
            let mmap_len = mmap_ref.len();
            rg_descs
                .par_iter()
                .filter_map(|desc| {
                    if let Some(zmaps) = footer.zone_maps.get(desc.rg_idx) {
                        if let Some(zm) = zmaps
                            .iter()
                            .find(|z| z.col_idx as usize == col_idx && !z.is_float)
                        {
                            if max_len < zm.min_bits || min_len > zm.max_bits {
                                return None;
                            }
                        }
                    }
                    let mmap =
                        unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                    Some(scan_rg(desc, mmap))
                })
                .flatten()
                .collect()
        } else {
            let mut out = Vec::new();
            for desc in &rg_descs {
                if out.len() >= max_matches {
                    break;
                }
                if let Some(zmaps) = footer.zone_maps.get(desc.rg_idx) {
                    if let Some(zm) = zmaps
                        .iter()
                        .find(|z| z.col_idx as usize == col_idx && !z.is_float)
                    {
                        if max_len < zm.min_bits || min_len > zm.max_bits {
                            continue;
                        }
                    }
                }
                let mut local = scan_rg(desc, mmap_ref);
                out.append(&mut local);
                if out.len() >= max_matches {
                    out.truncate(max_matches);
                    break;
                }
            }
            out
        };

        if let Some(lim) = limit {
            matches.truncate(lim);
        }
        Ok(Some(matches))
    }

    /// Count rows where a string/dict column value is IN a set, without
    /// materializing the matching indices or an Arrow batch. Returns `None`
    /// when the column is not string/dict or the layout is ineligible.
    pub fn count_string_in_set_mmap(
        &self,
        col_name: &str,
        values: &[String],
    ) -> io::Result<Option<i64>> {
        use rayon::prelude::*;
        if values.is_empty() {
            return Ok(Some(0));
        }
        let mut values = values.to_vec();
        values.sort();
        values.dedup();
        if values.len() == 1 {
            return Ok(self
                .scan_string_filter_mmap(col_name, &values[0], None)?
                .map(|v| v.len() as i64));
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
        let is_dict = matches!(col_type, ColumnType::StringDict);
        let is_string = matches!(col_type, ColumnType::String);
        if !is_dict && !is_string {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for string IN count"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let all_fast = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return false;
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            let compress_flag = rg_bytes.get(28).copied().unwrap_or(RG_COMPRESS_NONE);
            let enc_ver = rg_bytes.get(29).copied().unwrap_or(0);
            compress_flag == RG_COMPRESS_NONE
                && enc_ver >= 1
                && footer
                    .col_offsets
                    .get(rg_i)
                    .map_or(false, |v| v.len() > col_idx)
        });
        if !all_fast {
            drop(mmap_guard);
            drop(file_guard);
            // Fallback: sum per-value equality scans. Values are deduped, so
            // no row can match more than one target.
            let mut total = 0i64;
            for v in &values {
                if let Some(idxs) = self.scan_string_filter_mmap(col_name, v, None)? {
                    total += idxs.len() as i64;
                }
            }
            return Ok(Some(total));
        }

        let target_bytes: Vec<&[u8]> = values.iter().map(|s| s.as_bytes()).collect();
        let matches_any = |bytes: &[u8]| -> bool {
            target_bytes
                .iter()
                .any(|target| target.len() == bytes.len() && *target == bytes)
        };

        struct RgDesc {
            rg_offset: usize,
            rg_data_size: usize,
            rg_rows: usize,
            col_rcix: usize,
            has_deletes: bool,
            id_section_len: usize,
        }
        let mut rg_descs: Vec<RgDesc> = Vec::with_capacity(footer.row_groups.len());
        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            rg_descs.push(RgDesc {
                rg_offset: rg_meta.offset as usize,
                rg_data_size: rg_meta.data_size as usize,
                rg_rows: rg_meta.row_count as usize,
                col_rcix: footer.col_offsets[rg_i][col_idx] as usize,
                has_deletes: rg_meta.deletion_count > 0,
                id_section_len: rg_id_section_len(
                    rg_meta.row_count as usize,
                    mmap_ref
                        .get(rg_meta.offset as usize + 30)
                        .copied()
                        .unwrap_or(RG_IDS_PLAIN),
                ),
            });
        }

        let mmap_ptr: usize = mmap_ref.as_ptr() as usize;
        let mmap_len: usize = mmap_ref.len();
        let results: Vec<i64> = rg_descs
            .par_iter()
            .map(|desc| {
                let mmap =
                    unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                let rg_end = desc.rg_offset + desc.rg_data_size;
                if rg_end > mmap.len() || rg_end < desc.rg_offset + 32 {
                    return 0i64;
                }
                let body = &mmap[desc.rg_offset + 32..rg_end];
                let bitmap_len = (desc.rg_rows + 7) / 8;
                if desc.col_rcix + bitmap_len > body.len() {
                    return 0i64;
                }
                let null_bytes = &body[desc.col_rcix..desc.col_rcix + bitmap_len];
                let col_bytes = &body[desc.col_rcix + bitmap_len..];
                let Some(&encoding) = col_bytes.first() else {
                    return 0i64;
                };
                if (is_string && encoding != COL_ENCODING_PLAIN)
                    || (is_dict
                        && !matches!(
                            encoding,
                            COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                        ))
                {
                    return 0i64;
                }
                let del_bytes: Option<&[u8]> =
                    if desc.has_deletes && desc.id_section_len + bitmap_len <= body.len() {
                        Some(&body[desc.id_section_len..desc.id_section_len + bitmap_len])
                    } else {
                        None
                    };
                let payload = &col_bytes[1..];
                let mut c: i64 = 0;

                if is_string {
                    if payload.len() < 8 {
                        return 0i64;
                    }
                    let count = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8])) as usize;
                    let data_len_off = 8 + (count + 1) * 4;
                    if data_len_off + 8 > payload.len() {
                        return 0i64;
                    }
                    let data_len = u64::from_le_bytes(
                        payload[data_len_off..data_len_off + 8].try_into().unwrap_or([0; 8]),
                    ) as usize;
                    let data_start = data_len_off + 8;
                    let data_end = (data_start + data_len).min(payload.len());
                    if data_end < data_start {
                        return 0i64;
                    }
                    let raw = &payload[data_start..data_end];
                    let offsets_cow = bytes_as_u32_slice(&payload[8..], count + 1);
                    let offsets: &[u32] = &offsets_cow;
                    let n = count.min(desc.rg_rows);
                    for i in 0..n {
                        if let Some(db) = del_bytes {
                            if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                        }
                        if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                            continue;
                        }
                        let s = offsets[i] as usize;
                        let e = offsets[i + 1] as usize;
                        if e >= s && e <= raw.len() && matches_any(&raw[s..e]) {
                            c += 1;
                        }
                    }
                } else {
                    let Ok(view) =
                        StringDictView::parse(payload, encoding == COL_ENCODING_COMPACT_DICTIONARY)
                    else {
                        return 0i64;
                    };
                    let mut match_flags = vec![false; view.dict_size];
                    for dict_index in 1..view.dict_size {
                        if view.value(dict_index as u32).is_some_and(&matches_any) {
                            match_flags[dict_index] = true;
                        }
                    }
                    let n = view.row_count.min(desc.rg_rows);
                    for i in 0..n {
                        if let Some(db) = del_bytes {
                            if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                        }
                        if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                            continue;
                        }
                        let idx = view.index(i).unwrap_or(0) as usize;
                        if idx < match_flags.len() && match_flags[idx] {
                            c += 1;
                        }
                    }
                }
                c
            })
            .collect();

        drop(mmap_guard);
        drop(file_guard);
        Ok(Some(results.into_iter().sum()))
    }

    /// Return the distinct dictionary VALUES of a StringDict column for rows
    /// where a numeric column is in [lo, hi], without materializing the row set.
    /// Used to answer set-operation sides (`SELECT dict_col WHERE num BETWEEN ...`)
    /// directly as a small value set. Returns `None` when ineligible.
    pub fn scan_distinct_dict_values_in_range(
        &self,
        num_col: &str,
        lo: f64,
        hi: f64,
        dict_col: &str,
    ) -> io::Result<Option<Vec<String>>> {
        use rayon::prelude::*;

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let num_idx = match schema.get_index(num_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let dict_idx = match schema.get_index(dict_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let num_type = schema.columns[num_idx].1;
        let num_is_int = matches!(
            num_type,
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
        let num_is_float = matches!(num_type, ColumnType::Float64 | ColumnType::Float32);
        if !num_is_int && !num_is_float {
            return Ok(None);
        }
        if schema.columns[dict_idx].1 != ColumnType::StringDict {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for distinct dict scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

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
            if col_offsets.len() <= num_idx.max(dict_idx) {
                return false;
            }
            true
        });
        if !all_fast {
            return Ok(None);
        }

        struct RgDesc {
            rg_offset: usize,
            rg_data_size: usize,
            rg_rows: usize,
            num_rcix: usize,
            dict_rcix: usize,
            has_deletes: bool,
            id_section_len: usize,
        }
        let mut rg_descs: Vec<RgDesc> = Vec::with_capacity(footer.row_groups.len());
        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            rg_descs.push(RgDesc {
                rg_offset: rg_meta.offset as usize,
                rg_data_size: rg_meta.data_size as usize,
                rg_rows: rg_meta.row_count as usize,
                num_rcix: footer.col_offsets[rg_i][num_idx] as usize,
                dict_rcix: footer.col_offsets[rg_i][dict_idx] as usize,
                has_deletes: rg_meta.deletion_count > 0,
                id_section_len: rg_id_section_len(
                    rg_meta.row_count as usize,
                    mmap_ref
                        .get(rg_meta.offset as usize + 30)
                        .copied()
                        .unwrap_or(RG_IDS_PLAIN),
                ),
            });
        }

        let mmap_ptr: usize = mmap_ref.as_ptr() as usize;
        let mmap_len: usize = mmap_ref.len();
        let low_i = lo.ceil() as i64;
        let high_i = hi.floor() as i64;
        let results: Vec<io::Result<Option<Vec<String>>>> = rg_descs
            .par_iter()
            .map(|desc| {
                let mmap =
                    unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                let rg_end = desc.rg_offset + desc.rg_data_size;
                if rg_end > mmap.len() || rg_end < desc.rg_offset + 32 {
                    return Ok(None);
                }
                let body = &mmap[desc.rg_offset + 32..rg_end];
                let rg_rows = desc.rg_rows;
                let bitmap_len = (rg_rows + 7) / 8;
                let del_bytes: Option<&[u8]> =
                    if desc.has_deletes && desc.id_section_len + bitmap_len <= body.len() {
                        Some(&body[desc.id_section_len..desc.id_section_len + bitmap_len])
                    } else {
                        None
                    };
                if desc.has_deletes && del_bytes.is_none() {
                    return Ok(None);
                }

                // Numeric column.
                if desc.num_rcix + bitmap_len > body.len() {
                    return Ok(None);
                }
                let num_nulls = &body[desc.num_rcix..desc.num_rcix + bitmap_len];
                let (num_col_data, _) =
                    read_column_encoded(&body[desc.num_rcix + bitmap_len..], num_type)?;

                // Dict column: pre-compute the distinct value strings.
                if desc.dict_rcix + bitmap_len > body.len() {
                    return Ok(None);
                }
                let dict_nulls = &body[desc.dict_rcix..desc.dict_rcix + bitmap_len];
                let dict_col_bytes = &body[desc.dict_rcix + bitmap_len..];
                let Some(&dict_encoding) = dict_col_bytes.first() else {
                    return Ok(None);
                };
                if !matches!(
                    dict_encoding,
                    COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                ) {
                    return Ok(None);
                }
                let Ok(view) = StringDictView::parse(
                    &dict_col_bytes[1..],
                    dict_encoding == COL_ENCODING_COMPACT_DICTIONARY,
                ) else {
                    return Ok(None);
                };
                let values: Vec<String> = (1..view.dict_size)
                    .filter_map(|k| view.value(k as u32).map(|b| String::from_utf8_lossy(b).into_owned()))
                    .collect();
                let value_index = |key: u32| -> Option<usize> {
                    if key == 0 {
                        None
                    } else {
                        Some((key as usize) - 1)
                    }
                };

                let n = rg_rows;
                let mut seen: Vec<bool> = vec![false; values.len()];
                match &num_col_data {
                    ColumnData::Int64(v) => {
                        if v.len() < n {
                            return Ok(None);
                        }
                        for i in 0..n {
                            if let Some(db) = del_bytes {
                                if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                            }
                            if (num_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if (dict_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            let vv = v[i];
                            if vv < low_i || vv > high_i {
                                continue;
                            }
                            if let Some(idx) = view.index(i).and_then(value_index) {
                                if idx < seen.len() {
                                    seen[idx] = true;
                                }
                            }
                        }
                    }
                    ColumnData::Float64(v) => {
                        if v.len() < n {
                            return Ok(None);
                        }
                        for i in 0..n {
                            if let Some(db) = del_bytes {
                                if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                            }
                            if (num_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if (dict_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            let vv = v[i];
                            if vv < lo || vv > hi {
                                continue;
                            }
                            if let Some(idx) = view.index(i).and_then(value_index) {
                                if idx < seen.len() {
                                    seen[idx] = true;
                                }
                            }
                        }
                    }
                    _ => return Ok(None),
                }
                Ok(Some(
                    values
                        .into_iter()
                        .zip(seen.iter())
                        .filter(|(_, &s)| s)
                        .map(|(v, _)| v)
                        .collect(),
                ))
            })
            .collect();

        drop(mmap_guard);
        drop(file_guard);

        let mut merged: Vec<String> = Vec::new();
        for r in results {
            match r? {
                Some(vals) => merged.extend(vals),
                None => return Ok(None),
            }
        }
        merged.sort();
        merged.dedup();
        Ok(Some(merged))
    }

    /// Counts rows of `dict_col` whose `num_col` falls in each of two ranges in
    /// a single pass, returning `(value, left_count, right_count)` sorted by
    /// value. Keeping the side counts separate lets UNION, INTERSECT, and EXCEPT
    /// share the same scan while preserving ALL and DISTINCT semantics.
    pub fn count_dict_values_two_ranges(
        &self,
        num_col: &str,
        lo1: f64,
        hi1: f64,
        lo2: f64,
        hi2: f64,
        dict_col: &str,
    ) -> io::Result<Option<Vec<(String, i64, i64)>>> {
        use rayon::prelude::*;

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let num_idx = match schema.get_index(num_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let dict_idx = match schema.get_index(dict_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let num_type = schema.columns[num_idx].1;
        let num_is_int = matches!(
            num_type,
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
        let num_is_float = matches!(num_type, ColumnType::Float64 | ColumnType::Float32);
        if !num_is_int && !num_is_float {
            return Ok(None);
        }
        if schema.columns[dict_idx].1 != ColumnType::StringDict {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for dict count scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

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
            if col_offsets.len() <= num_idx.max(dict_idx) {
                return false;
            }
            true
        });
        if !all_fast {
            return Ok(None);
        }

        struct RgDesc {
            rg_offset: usize,
            rg_data_size: usize,
            rg_rows: usize,
            num_rcix: usize,
            dict_rcix: usize,
            has_deletes: bool,
            id_section_len: usize,
        }
        let mut rg_descs: Vec<RgDesc> = Vec::with_capacity(footer.row_groups.len());
        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            rg_descs.push(RgDesc {
                rg_offset: rg_meta.offset as usize,
                rg_data_size: rg_meta.data_size as usize,
                rg_rows: rg_meta.row_count as usize,
                num_rcix: footer.col_offsets[rg_i][num_idx] as usize,
                dict_rcix: footer.col_offsets[rg_i][dict_idx] as usize,
                has_deletes: rg_meta.deletion_count > 0,
                id_section_len: rg_id_section_len(
                    rg_meta.row_count as usize,
                    mmap_ref
                        .get(rg_meta.offset as usize + 30)
                        .copied()
                        .unwrap_or(RG_IDS_PLAIN),
                ),
            });
        }

        let mmap_ptr: usize = mmap_ref.as_ptr() as usize;
        let mmap_len: usize = mmap_ref.len();
        let low1 = lo1.ceil() as i64;
        let high1 = hi1.floor() as i64;
        let low2 = lo2.ceil() as i64;
        let high2 = hi2.floor() as i64;
        let results: Vec<io::Result<Option<Vec<(String, i64, i64)>>>> = rg_descs
            .par_iter()
            .map(|desc| {
                let mmap =
                    unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                let rg_end = desc.rg_offset + desc.rg_data_size;
                if rg_end > mmap.len() || rg_end < desc.rg_offset + 32 {
                    return Ok(None);
                }
                let body = &mmap[desc.rg_offset + 32..rg_end];
                let rg_rows = desc.rg_rows;
                let bitmap_len = (rg_rows + 7) / 8;
                let del_bytes: Option<&[u8]> =
                    if desc.has_deletes && desc.id_section_len + bitmap_len <= body.len() {
                        Some(&body[desc.id_section_len..desc.id_section_len + bitmap_len])
                    } else {
                        None
                    };
                if desc.has_deletes && del_bytes.is_none() {
                    return Ok(None);
                }

                if desc.num_rcix + bitmap_len > body.len() {
                    return Ok(None);
                }
                let num_nulls = &body[desc.num_rcix..desc.num_rcix + bitmap_len];
                let (num_col_data, _) =
                    read_column_encoded(&body[desc.num_rcix + bitmap_len..], num_type)?;

                if desc.dict_rcix + bitmap_len > body.len() {
                    return Ok(None);
                }
                let dict_nulls = &body[desc.dict_rcix..desc.dict_rcix + bitmap_len];
                let dict_col_bytes = &body[desc.dict_rcix + bitmap_len..];
                let Some(&dict_encoding) = dict_col_bytes.first() else {
                    return Ok(None);
                };
                if !matches!(
                    dict_encoding,
                    COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                ) {
                    return Ok(None);
                }
                let Ok(view) = StringDictView::parse(
                    &dict_col_bytes[1..],
                    dict_encoding == COL_ENCODING_COMPACT_DICTIONARY,
                ) else {
                    return Ok(None);
                };
                let values: Vec<String> = (1..view.dict_size)
                    .filter_map(|k| {
                        view.value(k as u32)
                            .map(|b| String::from_utf8_lossy(b).into_owned())
                    })
                    .collect();
                let value_index = |key: u32| -> Option<usize> {
                    if key == 0 {
                        None
                    } else {
                        Some((key as usize) - 1)
                    }
                };

                let n = rg_rows;
                let mut left_counts: Vec<i64> = vec![0; values.len()];
                let mut right_counts: Vec<i64> = vec![0; values.len()];
                match &num_col_data {
                    ColumnData::Int64(v) => {
                        if v.len() < n {
                            return Ok(None);
                        }
                        for i in 0..n {
                            if let Some(db) = del_bytes {
                                if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                            }
                            if (num_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if (dict_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            let vv = v[i];
                            let in_left = vv >= low1 && vv <= high1;
                            let in_right = vv >= low2 && vv <= high2;
                            if !in_left && !in_right {
                                continue;
                            }
                            if let Some(idx) = view.index(i).and_then(value_index) {
                                if idx < left_counts.len() {
                                    left_counts[idx] += i64::from(in_left);
                                    right_counts[idx] += i64::from(in_right);
                                }
                            }
                        }
                    }
                    ColumnData::Float64(v) => {
                        if v.len() < n {
                            return Ok(None);
                        }
                        for i in 0..n {
                            if let Some(db) = del_bytes {
                                if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                            }
                            if (num_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if (dict_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            let vv = v[i];
                            let in_left = vv >= lo1 && vv <= hi1;
                            let in_right = vv >= lo2 && vv <= hi2;
                            if !in_left && !in_right {
                                continue;
                            }
                            if let Some(idx) = view.index(i).and_then(value_index) {
                                if idx < left_counts.len() {
                                    left_counts[idx] += i64::from(in_left);
                                    right_counts[idx] += i64::from(in_right);
                                }
                            }
                        }
                    }
                    _ => return Ok(None),
                }
                Ok(Some(
                    values.into_iter()
                        .zip(left_counts.into_iter().zip(right_counts))
                        .filter_map(|(value, (left, right))| {
                            (left > 0 || right > 0).then_some((value, left, right))
                        })
                        .collect(),
                ))
            })
            .collect();

        drop(mmap_guard);
        drop(file_guard);

        let mut merged: std::collections::HashMap<String, (i64, i64)> =
            std::collections::HashMap::new();
        for r in results {
            match r? {
                Some(pairs) => {
                    for (value, left, right) in pairs {
                        let counts = merged.entry(value).or_insert((0, 0));
                        counts.0 += left;
                        counts.1 += right;
                    }
                }
                None => return Ok(None),
            }
        }
        let mut out: Vec<(String, i64, i64)> = merged
            .into_iter()
            .map(|(value, (left, right))| (value, left, right))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Some(out))
    }

    pub fn scan_bool_filter_mmap(
        &self,
        col_name: &str,
        target_value: bool,
        limit: Option<usize>,
    ) -> io::Result<Option<Vec<usize>>> {
        use rayon::prelude::*;

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
        if !matches!(col_type, ColumnType::Bool) {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for bool scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let max_matches = limit.unwrap_or(usize::MAX);
        let target_bit: u8 = if target_value { 1 } else { 0 };

        // Use parallel scan for better performance on multi-core
        if footer.row_groups.len() > 1 {
            let mmap_ptr: usize = mmap_ref.as_ptr() as usize;
            let mmap_len: usize = mmap_ref.len();

            struct RgDesc {
                rg_offset: usize,
                rg_data_size: usize,
                rg_rows: usize,
                global_off: usize,
                col_rcix: usize,
                has_deletes: bool,
            }
            let mut rg_descs: Vec<RgDesc> = Vec::with_capacity(footer.row_groups.len());
            let mut off = 0usize;
            for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
                rg_descs.push(RgDesc {
                    rg_offset: rg_meta.offset as usize,
                    rg_data_size: rg_meta.data_size as usize,
                    rg_rows: rg_meta.row_count as usize,
                    global_off: off,
                    col_rcix: footer.col_offsets[rg_i][col_idx] as usize,
                    has_deletes: rg_meta.deletion_count > 0,
                });
                off += rg_meta.row_count as usize;
            }

            let all_rg_matches: Vec<Vec<usize>> = rg_descs
                .par_iter()
                .map(|desc| {
                    let mmap =
                        unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                    let rg_end = desc.rg_offset + desc.rg_data_size;
                    if rg_end > mmap.len() || rg_end < desc.rg_offset + 32 {
                        return vec![];
                    }
                    let rg_bytes = &mmap[desc.rg_offset..rg_end];
                    let body = &rg_bytes[32..];
                    let rg_rows = desc.rg_rows;
                    let null_bitmap_len = (rg_rows + 7) / 8;
                    let del_vec_len = null_bitmap_len;

                    let col_off = desc.col_rcix;
                    if col_off + null_bitmap_len > body.len() {
                        return vec![];
                    }
                    let bool_data = &body[col_off..col_off + null_bitmap_len];

                    let mut matches = Vec::new();
                    for i in 0..rg_rows {
                        if matches.len() >= max_matches {
                            break;
                        }
                        if desc.has_deletes {
                            let b = i / 8;
                            let bit = i % 8;
                            if b < del_vec_len && (body[b] >> bit) & 1 != 0 {
                                continue;
                            }
                        }
                        let bool_val = (bool_data[i / 8] >> (i % 8)) & 1;
                        if bool_val == target_bit {
                            matches.push(desc.global_off + i);
                        }
                    }
                    matches
                })
                .collect();

            let mut result: Vec<usize> = all_rg_matches.into_iter().flatten().collect();
            if let Some(lim) = limit {
                result.truncate(lim);
            }
            return Ok(Some(result));
        }

        // Fallback for single RG
        let mut matches: Vec<usize> = Vec::new();
        let mut global_row_offset: usize = 0;

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            if matches.len() >= max_matches {
                break;
            }
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                global_row_offset += rg_rows;
                continue;
            }

            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                continue;
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            let body = &rg_bytes[32..];
            let null_bitmap_len = (rg_rows + 7) / 8;
            let del_vec_len = null_bitmap_len;

            let col_off =
                if rg_i < footer.col_offsets.len() && col_idx < footer.col_offsets[rg_i].len() {
                    footer.col_offsets[rg_i][col_idx] as usize
                } else {
                    continue;
                };

            if col_off + null_bitmap_len > body.len() {
                global_row_offset += rg_rows;
                continue;
            }
            let bool_data = &body[col_off..col_off + null_bitmap_len];

            let has_deletes = rg_meta.deletion_count > 0;
            for i in 0..rg_rows {
                if matches.len() >= max_matches {
                    break;
                }
                if has_deletes && (body[i / 8] >> (i % 8)) & 1 != 0 {
                    continue;
                }
                let bool_val = (bool_data[i / 8] >> (i % 8)) & 1;
                if bool_val == target_bit {
                    matches.push(global_row_offset + i);
                }
            }
            global_row_offset += rg_rows;
        }

        Ok(Some(matches))
    }

    pub fn scan_like_filter_mmap(
        &self,
        col_name: &str,
        pattern: &str,
        limit: Option<usize>,
    ) -> io::Result<Option<Vec<usize>>> {
        use rayon::prelude::*;

        // No wildcards → exact equality: delegate to the optimised equality scanner
        if !pattern.contains('%') && !pattern.contains('_') {
            return self.scan_string_filter_mmap(col_name, pattern, limit);
        }
        let like_kind = match classify_like_pattern(pattern) {
            Some(k) => k,
            None => return Ok(None),
        };

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
        if !matches!(col_type, ColumnType::String | ColumnType::StringDict) {
            return Ok(None);
        }
        let is_dict = matches!(col_type, ColumnType::StringDict);

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for LIKE scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let max_matches = limit.unwrap_or(usize::MAX);
        let mut matches: Vec<usize> = Vec::new();
        let mut global_row_offset: usize = 0;

        // ── PARALLEL FAST PATH ───────────────────────────────────────────────
        // Requires: no limit, multiple RGs, all uncompressed+RCIX
        if limit.is_none() && footer.row_groups.len() > 1 {
            let all_fast = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
                let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
                if rg_end > mmap_ref.len() {
                    return false;
                }
                let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
                let compress_flag = rg_bytes.get(28).copied().unwrap_or(RG_COMPRESS_NONE);
                let enc_ver = rg_bytes.get(29).copied().unwrap_or(0);
                compress_flag == RG_COMPRESS_NONE
                    && enc_ver >= 1
                    && footer
                        .col_offsets
                        .get(rg_i)
                        .map_or(false, |v| v.len() > col_idx)
            });

            if all_fast {
                let mmap_ptr: usize = mmap_ref.as_ptr() as usize;
                let mmap_len: usize = mmap_ref.len();

                struct RgDesc {
                    rg_offset: usize,
                    rg_data_size: usize,
                    rg_rows: usize,
                    global_off: usize,
                    col_rcix: usize,
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
                        col_rcix: footer.col_offsets[rg_i][col_idx] as usize,
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

                let like_kind_ref = &like_kind;
                let all_rg_matches: Vec<Vec<usize>> = rg_descs
                    .par_iter()
                    .map(|desc| {
                        let mmap =
                            unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                        let rg_end = desc.rg_offset + desc.rg_data_size;
                        if rg_end > mmap.len() || rg_end < desc.rg_offset + 32 {
                            return vec![];
                        }
                        let body = &mmap[desc.rg_offset + 32..rg_end];
                        let rg_rows = desc.rg_rows;
                        let bitmap_len = (rg_rows + 7) / 8;
                        let del_start = desc.id_section_len;

                        let col_off = desc.col_rcix;
                        if col_off + bitmap_len > body.len() {
                            return vec![];
                        }
                        let null_bytes = &body[col_off..col_off + bitmap_len];
                        let col_bytes = &body[col_off + bitmap_len..];
                        let Some(&encoding) = col_bytes.first() else {
                            return vec![];
                        };
                        if (!is_dict && encoding != COL_ENCODING_PLAIN)
                            || (is_dict
                                && !matches!(
                                    encoding,
                                    COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                                ))
                        {
                            return vec![];
                        }
                        let data = &col_bytes[1..];

                        let del_bytes_opt: Option<&[u8]> =
                            if desc.has_deletes && del_start + bitmap_len <= body.len() {
                                Some(&body[del_start..del_start + bitmap_len])
                            } else {
                                None
                            };

                        let mut local: Vec<usize> = Vec::new();

                        if !is_dict {
                            // ── String PLAIN ──────────────────────────────────────
                            if data.len() < 8 {
                                return local;
                            }
                            let count = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0; 8]))
                                as usize;
                            let data_len_off = 8 + (count + 1) * 4;
                            if data_len_off + 8 > data.len() {
                                return local;
                            }
                            let data_str_len = u64::from_le_bytes(
                                data[data_len_off..data_len_off + 8]
                                    .try_into()
                                    .unwrap_or([0; 8]),
                            ) as usize;
                            let data_start = data_len_off + 8;
                            let data_end = (data_start + data_str_len).min(data.len());
                            if data_end < data_start {
                                return local;
                            }
                            let data_region = &data[data_start..data_end];
                            let offsets_cow = bytes_as_u32_slice(&data[8..], count + 1);
                            let offsets: &[u32] = &offsets_cow;
                            let n = count.min(rg_rows);

                            // Fast path: no deletions and no nulls - skip bitmap checks entirely
                            let no_deletes = del_bytes_opt.is_none();
                            let no_nulls = null_bytes.iter().all(|&b| b == 0);

                            if no_deletes && no_nulls {
                                // Ultra-fast path: no bitmap checks needed
                                for i in 0..n {
                                    let s = offsets[i] as usize;
                                    let e = offsets[i + 1] as usize;
                                    if e > data_region.len() {
                                        continue;
                                    }
                                    if like_matches_bytes(like_kind_ref, &data_region[s..e]) {
                                        local.push(desc.global_off + i);
                                    }
                                }
                            } else {
                                // Standard path with deletion/null checks
                                for i in 0..n {
                                    if let Some(db) = del_bytes_opt {
                                        if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                            continue;
                                        }
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    let s = offsets[i] as usize;
                                    let e = offsets[i + 1] as usize;
                                    if e > data_region.len() {
                                        continue;
                                    }
                                    if like_matches_bytes(like_kind_ref, &data_region[s..e]) {
                                        local.push(desc.global_off + i);
                                    }
                                }
                            }
                        } else {
                            let Ok(view) = StringDictView::parse(
                                data,
                                encoding == COL_ENCODING_COMPACT_DICTIONARY,
                            ) else {
                                return local;
                            };

                            // Pre-compute per-dict-entry match flags (O(dict_size), very fast)
                            let mut match_flags = vec![false; view.dict_size];
                            for dict_index in 1..view.dict_size {
                                if let Some(value) = view.value(dict_index as u32) {
                                    match_flags[dict_index] =
                                        like_matches_bytes(like_kind_ref, value);
                                }
                            }

                            let n = view.row_count.min(rg_rows);
                            for i in 0..n {
                                if let Some(db) = del_bytes_opt {
                                    if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                }
                                let idx = view.index(i).unwrap_or(0) as usize;
                                if idx < match_flags.len() && match_flags[idx] {
                                    local.push(desc.global_off + i);
                                }
                            }
                        }
                        local
                    })
                    .collect();

                matches = all_rg_matches.into_iter().flatten().collect();
                return Ok(Some(matches));
            }
        }
        // ── END PARALLEL FAST PATH ───────────────────────────────────────────

        // SEQUENTIAL PATH: single-RG, limited scans, or compressed files
        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            if matches.len() >= max_matches {
                break;
            }
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                global_row_offset += rg_rows;
                continue;
            }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("LIKE scan: RG extends past EOF"));
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
            let bitmap_len = (rg_rows + 7) / 8;
            let del_start =
                rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));
            let del_bytes: &[u8] = if del_start + bitmap_len <= body.len() {
                &body[del_start..del_start + bitmap_len]
            } else {
                &[]
            };
            let has_deletes = rg_meta.deletion_count > 0;

            // RCIX: jump directly to the target column
            let rcix = if compress_flag == RG_COMPRESS_NONE && encoding_version >= 1 {
                footer.col_offsets.get(rg_i).filter(|v| v.len() > col_idx)
            } else {
                None
            };

            if let Some(rcix) = rcix {
                let col_off = rcix[col_idx] as usize;
                if col_off + bitmap_len > body.len() {
                    global_row_offset += rg_rows;
                    continue;
                }
                let null_bytes = &body[col_off..col_off + bitmap_len];
                let col_bytes = &body[col_off + bitmap_len..];
                if col_bytes.is_empty() {
                    global_row_offset += rg_rows;
                    continue;
                }
                let encoding = col_bytes[0];
                if (!is_dict && encoding == COL_ENCODING_PLAIN)
                    || (is_dict
                        && matches!(
                            encoding,
                            COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                        ))
                {
                    let data = &col_bytes[1..];
                    if !is_dict {
                        // String PLAIN
                        if data.len() >= 8 {
                            let count = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0; 8]))
                                as usize;
                            let data_len_off = 8 + (count + 1) * 4;
                            if data_len_off + 8 <= data.len() {
                                let data_str_len = u64::from_le_bytes(
                                    data[data_len_off..data_len_off + 8]
                                        .try_into()
                                        .unwrap_or([0; 8]),
                                ) as usize;
                                let data_start = data_len_off + 8;
                                let data_end = (data_start + data_str_len).min(data.len());
                                let data_region = &data[data_start..data_end];
                                let offsets_cow = bytes_as_u32_slice(&data[8..], count + 1);
                                let offsets: &[u32] = &offsets_cow;
                                let n = count.min(rg_rows);
                                for i in 0..n {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    let s = offsets[i] as usize;
                                    let e = offsets[i + 1] as usize;
                                    if e <= data_region.len()
                                        && like_matches_bytes(&like_kind, &data_region[s..e])
                                    {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            }
                        }
                    } else {
                        if let Ok(view) =
                            StringDictView::parse(data, encoding == COL_ENCODING_COMPACT_DICTIONARY)
                        {
                            let mut match_flags = vec![false; view.dict_size];
                            for dict_index in 1..view.dict_size {
                                if let Some(value) = view.value(dict_index as u32) {
                                    match_flags[dict_index] = like_matches_bytes(&like_kind, value);
                                }
                            }
                            for i in 0..view.row_count.min(rg_rows) {
                                if matches.len() >= max_matches {
                                    break;
                                }
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                let index = view.index(i).unwrap_or(0) as usize;
                                if index < match_flags.len() && match_flags[index] {
                                    matches.push(global_row_offset + i);
                                }
                            }
                        }
                    }
                }
            }
            // Non-RCIX or non-PLAIN: skip (caller falls back to executor)
            global_row_offset += rg_rows;
        }
        drop(mmap_guard);
        drop(file_guard);
        Ok(Some(matches))
    }

    pub fn scan_numeric_range_mmap(
        &self,
        col_name: &str,
        low: f64,
        high: f64,
        limit: Option<usize>,
    ) -> io::Result<Option<Vec<usize>>> {
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
        let is_bool = matches!(col_type, ColumnType::Bool);
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
        if !is_bool && !is_int && !is_float {
            return Ok(None);
        }

        // For boolean columns, convert to integer range: false=0, true=1
        let (low, high) = if is_bool {
            let bool_low = if low > 0.5 { 1 } else { 0 };
            let bool_high = if high > 0.5 { 1 } else { 0 };
            (bool_low as f64, bool_high as f64)
        } else {
            (low, high)
        };

        let col_count = schema.column_count();
        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for range scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let max_matches = limit.unwrap_or(usize::MAX);
        let mut matches: Vec<usize> = Vec::new();
        let mut global_row_offset: usize = 0;

        // ── PARALLEL FAST PATH ───────────────────────────────────────────────
        // No limit + multiple uncompressed RCIX row groups: scan each RG in
        // parallel and flatten, instead of the single-threaded loop below.
        if limit.is_none() && footer.row_groups.len() > 1 {
            use rayon::prelude::*;
            let all_fast = footer
                .row_groups
                .iter()
                .enumerate()
                .all(|(rg_i, rg_meta)| {
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
                    if col_offsets.len() <= col_idx {
                        return false;
                    }
                    // Only PLAIN and (integer) BITPACK are handled in parallel;
                    // anything else falls back to the sequential loop.
                    let bitmap_len = (rg_meta.row_count as usize + 7) / 8;
                    let enc_pos = 32 + col_offsets[col_idx] as usize + bitmap_len;
                    let Some(&enc) = rg_bytes.get(enc_pos) else {
                        return false;
                    };
                    if enc != COL_ENCODING_PLAIN && !(enc == COL_ENCODING_BITPACK && is_int) {
                        return false;
                    }
                    // Deletion vector must be fully present when this RG has
                    // deletes, so the parallel loop never silently skips them.
                    if rg_meta.deletion_count > 0 {
                        let id_section = rg_id_section_len(
                            rg_meta.row_count as usize,
                            rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
                        );
                        if 32 + id_section + bitmap_len > rg_bytes.len() {
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
                    col_rcix: usize,
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
                        col_rcix: footer.col_offsets[rg_i][col_idx] as usize,
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
                let low_i = low.ceil() as i64;
                let high_i = high.floor() as i64;
                let all_matches: Vec<Vec<usize>> = rg_descs
                    .par_iter()
                    .map(|desc| {
                        let mmap = unsafe {
                            std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len)
                        };
                        let rg_end = desc.rg_offset + desc.rg_data_size;
                        if rg_end > mmap.len() || rg_end < desc.rg_offset + 32 {
                            return vec![];
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
                        if desc.col_rcix + bitmap_len > body.len() {
                            return vec![];
                        }
                        let null_bytes = &body[desc.col_rcix..desc.col_rcix + bitmap_len];
                        let col_bytes = &body[desc.col_rcix + bitmap_len..];
                        if col_bytes.is_empty() {
                            return vec![];
                        }
                        let encoding = col_bytes[0];
                        let payload = &col_bytes[1..];
                        let mut local: Vec<usize> = Vec::new();

                        if encoding == COL_ENCODING_PLAIN && payload.len() >= 8 {
                            let count =
                                u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                            let n = count.min(rg_rows).min((payload.len() - 8) / 8);
                            if is_int {
                                let vals = bytes_as_i64_slice(&payload[8..], n);
                                for i in 0..n {
                                    if let Some(db) = del_bytes {
                                        if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                            continue;
                                        }
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if vals[i] >= low_i && vals[i] <= high_i {
                                        local.push(desc.global_off + i);
                                    }
                                }
                            } else {
                                let vals = bytes_as_f64_slice(&payload[8..], n);
                                for i in 0..n {
                                    if let Some(db) = del_bytes {
                                        if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                            continue;
                                        }
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if vals[i] >= low && vals[i] <= high {
                                        local.push(desc.global_off + i);
                                    }
                                }
                            }
                        } else if encoding == COL_ENCODING_BITPACK
                            && is_int
                            && payload.len() >= 17
                        {
                            let count =
                                u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                            let bit_width = payload[8] as usize;
                            if bit_width < 64 {
                                let min_val = i64::from_le_bytes(payload[9..17].try_into().unwrap());
                                let packed_bytes = (count * bit_width + 7) / 8;
                                if payload.len() >= 17 + packed_bytes {
                                    let packed = &payload[17..17 + packed_bytes];
                                    let n = count.min(rg_rows);
                                    for i in 0..n {
                                        if let Some(db) = del_bytes {
                                            if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                                continue;
                                            }
                                        }
                                        if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                            continue;
                                        }
                                        if let Some(v) = bitpack_value_at(packed, bit_width, min_val, i)
                                        {
                                            if v >= low_i && v <= high_i {
                                                local.push(desc.global_off + i);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        local
                    })
                    .collect();
                matches = all_matches.into_iter().flatten().collect();
                drop(mmap_guard);
                drop(file_guard);
                return Ok(Some(matches));
            }
        }

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            if matches.len() >= max_matches {
                break;
            }
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                global_row_offset += rg_rows;
                continue;
            }

            // Zone map pruning: skip RG if filter range can't overlap
            if rg_i < footer.zone_maps.len() {
                if let Some(zm) = footer.zone_maps[rg_i]
                    .iter()
                    .find(|z| z.col_idx as usize == col_idx)
                {
                    let skip = if zm.is_float {
                        !zm.may_overlap_float_range(low, high)
                    } else {
                        !zm.may_overlap_int_range(low.ceil() as i64, high.floor() as i64)
                    };
                    if skip {
                        global_row_offset += rg_rows;
                        continue;
                    }
                }
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
            let del_vec_len = (rg_rows + 7) / 8;
            let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
            let id_section = rg_id_section_len(rg_rows, id_encoding);
            if id_section + del_vec_len > body.len() {
                return Err(err_data("RG del vec truncated"));
            }
            let del_bytes = &body[id_section..id_section + del_vec_len];
            let has_deletes = rg_meta.deletion_count > 0;
            let null_bitmap_len = (rg_rows + 7) / 8;

            // RCIX fast path: jump directly to target column offset
            let rcix_available = rg_i < footer.col_offsets.len()
                && col_idx < footer.col_offsets[rg_i].len()
                && compress_flag == RG_COMPRESS_NONE;
            if rcix_available {
                let col_body_off = footer.col_offsets[rg_i][col_idx] as usize;
                if col_body_off + null_bitmap_len > body.len() {
                    global_row_offset += rg_rows;
                    continue;
                }
                let null_bytes = &body[col_body_off..col_body_off + null_bitmap_len];
                let data_start = col_body_off + null_bitmap_len;
                let col_bytes = &body[data_start..];
                let enc_offset = if encoding_version >= 1 { 1 } else { 0 };
                let encoding = if encoding_version >= 1 && !col_bytes.is_empty() {
                    col_bytes[0]
                } else {
                    COL_ENCODING_PLAIN
                };

                // Handle boolean columns (packed bits)
                if is_bool {
                    let bool_data_len = (rg_rows + 7) / 8;
                    if col_bytes.len() >= enc_offset + bool_data_len {
                        let bool_data = &col_bytes[enc_offset..enc_offset + bool_data_len];
                        let low_i = low.ceil() as i64;
                        let high_i = high.floor() as i64;
                        for i in 0..rg_rows {
                            if matches.len() >= max_matches {
                                break;
                            }
                            if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            let bool_val = (bool_data[i / 8] >> (i % 8)) & 1;
                            if bool_val as i64 >= low_i && bool_val as i64 <= high_i {
                                matches.push(global_row_offset + i);
                            }
                        }
                        global_row_offset += rg_rows;
                        continue;
                    }
                }

                if encoding == COL_ENCODING_PLAIN && col_bytes.len() > enc_offset + 8 {
                    let payload = &col_bytes[enc_offset..];
                    let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                    let n = count.min(rg_rows).min((payload.len() - 8) / 8);
                    let no_nulls = !null_bytes.iter().any(|&b| b != 0);
                    let unlimited = max_matches == usize::MAX;
                    if is_int {
                        let low_i = low.ceil() as i64;
                        let high_i = high.floor() as i64;
                        let vals = bytes_as_i64_slice(&payload[8..], n);
                        if !has_deletes && no_nulls && unlimited {
                            for (i, &v) in vals.iter().enumerate() {
                                if v >= low_i && v <= high_i {
                                    matches.push(global_row_offset + i);
                                }
                            }
                        } else {
                            for i in 0..n {
                                if matches.len() >= max_matches {
                                    break;
                                }
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if !no_nulls && (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if vals[i] >= low_i && vals[i] <= high_i {
                                    matches.push(global_row_offset + i);
                                }
                            }
                        }
                    } else {
                        let vals = bytes_as_f64_slice(&payload[8..], n);
                        if !has_deletes && no_nulls && unlimited {
                            for (i, &v) in vals.iter().enumerate() {
                                if v >= low && v <= high {
                                    matches.push(global_row_offset + i);
                                }
                            }
                        } else {
                            for i in 0..n {
                                if matches.len() >= max_matches {
                                    break;
                                }
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if !no_nulls && (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if vals[i] >= low && vals[i] <= high {
                                    matches.push(global_row_offset + i);
                                }
                            }
                        }
                    }
                    global_row_offset += rg_rows;
                    continue;
                } else if encoding == COL_ENCODING_BITPACK
                    && is_int
                    && col_bytes.len() >= enc_offset + 17
                {
                    let payload = &col_bytes[enc_offset..];
                    let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                    let bit_width = payload[8] as usize;
                    if bit_width < 64 {
                        let min_val = i64::from_le_bytes(payload[9..17].try_into().unwrap());
                        let packed_bytes = (count * bit_width + 7) / 8;
                        if payload.len() >= 17 + packed_bytes {
                            let packed = &payload[17..17 + packed_bytes];
                            let n = count.min(rg_rows);
                            let low_i = low.ceil() as i64;
                            let high_i = high.floor() as i64;
                            let no_nulls = !null_bytes.iter().any(|&b| b != 0);
                            for i in 0..n {
                                if matches.len() >= max_matches {
                                    break;
                                }
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if !no_nulls && (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if let Some(v) = bitpack_value_at(packed, bit_width, min_val, i) {
                                    if v >= low_i && v <= high_i {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            }
                            global_row_offset += rg_rows;
                            continue;
                        }
                    }
                }
            }

            // Fallback: sequential column scan (compressed or no RCIX)
            let mut pos = id_section + del_vec_len;
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
                    let data_slice = &col_bytes[enc_offset..];

                    if encoding == COL_ENCODING_PLAIN && data_slice.len() >= 8 {
                        let count =
                            u64::from_le_bytes(data_slice[0..8].try_into().unwrap()) as usize;
                        let values_start = 8usize;
                        let n = count.min(rg_rows);
                        if is_int {
                            let low_i = low.ceil() as i64;
                            let high_i = high.floor() as i64;
                            let nn = n.min((data_slice.len() - values_start) / 8);
                            let vals = bytes_as_i64_slice(&data_slice[values_start..], nn);
                            if !has_deletes {
                                for i in 0..vals.len() {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if vals[i] >= low_i && vals[i] <= high_i {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            } else {
                                for i in 0..vals.len() {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if vals[i] >= low_i && vals[i] <= high_i {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            }
                        } else {
                            let nn = n.min((data_slice.len() - values_start) / 8);
                            let vals = bytes_as_f64_slice(&data_slice[values_start..], nn);
                            if !has_deletes {
                                for i in 0..vals.len() {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if vals[i] >= low && vals[i] <= high {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            } else {
                                for i in 0..vals.len() {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if vals[i] >= low && vals[i] <= high {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            }
                        }
                    } else {
                        // Non-PLAIN encoding (RLE, BITPACK, etc.): decode then scan
                        let (col_data, _consumed) = if encoding_version >= 1 {
                            read_column_encoded(col_bytes, ct)?
                        } else {
                            ColumnData::from_bytes_typed(col_bytes, ct)?
                        };
                        match &col_data {
                            ColumnData::Int64(vals) => {
                                let low_i = low.ceil() as i64;
                                let high_i = high.floor() as i64;
                                for i in 0..vals.len().min(rg_rows) {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if vals[i] >= low_i && vals[i] <= high_i {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            }
                            ColumnData::Float64(vals) => {
                                for i in 0..vals.len().min(rg_rows) {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if vals[i] >= low && vals[i] <= high {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                let consumed = if encoding_version >= 1 {
                    skip_column_encoded(&body[pos..], ct)?
                } else {
                    ColumnData::skip_bytes_typed(&body[pos..], ct)?
                };
                pos += consumed;
            }
            global_row_offset += rg_rows;
        }
        drop(mmap_guard);
        drop(file_guard);
        Ok(Some(matches))
    }

    /// Whether a string column is globally sorted (lexicographically), cached by
    /// file mtime + table epoch. A sorted column lets prefix-LIKE range scans
    /// binary-search instead of scanning every row.
    fn column_sorted_cached(&self, col_idx: usize) -> bool {
        let Ok(meta) = std::fs::metadata(&self.path) else {
            return false;
        };
        let mtime = meta
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let epoch = crate::storage::epoch::current(&self.path);
        let key = (self.path.clone(), col_idx);
        if let Some((cm, ce, sorted)) = SORTED_COL_CACHE.read().get(&key) {
            if *ce == epoch && *cm >= mtime {
                return *sorted;
            }
        }
        let sorted = self.detect_column_sorted(col_idx).unwrap_or(false);
        SORTED_COL_CACHE.write().insert(key, (mtime, epoch, sorted));
        sorted
    }

    /// Scan a PLAIN string column (no nulls) and report whether it is globally
    /// sorted lexicographically.
    fn detect_column_sorted(&self, col_idx: usize) -> io::Result<bool> {
        let Some(footer) = self.get_or_load_footer()? else {
            return Ok(false);
        };
        let file_guard = self.file.read();
        let Some(file) = file_guard.as_ref() else {
            return Ok(false);
        };
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let mut prev: Option<&[u8]> = None;
        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Ok(false);
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.get(28).copied().unwrap_or(RG_COMPRESS_NONE) != RG_COMPRESS_NONE
                || rg_bytes.get(29).copied().unwrap_or(0) < 1
            {
                return Ok(false);
            }
            let Some(col_offsets) = footer.col_offsets.get(rg_i) else {
                return Ok(false);
            };
            if col_offsets.len() <= col_idx {
                return Ok(false);
            }
            let body = &rg_bytes[32..];
            let bitmap_len = (rg_rows + 7) / 8;
            let str_col_body = &body[col_offsets[col_idx] as usize..];
            if str_col_body.len() < bitmap_len + 1 {
                return Ok(false);
            }
            if str_col_body[..bitmap_len].iter().any(|&b| b != 0) {
                return Ok(false);
            }
            if str_col_body[bitmap_len] != COL_ENCODING_PLAIN {
                return Ok(false);
            }
            let str_payload = &str_col_body[bitmap_len + 1..];
            if str_payload.len() < 8 {
                return Ok(false);
            }
            let count = u64::from_le_bytes(str_payload[0..8].try_into().unwrap()) as usize;
            let data_len_off = 8 + (count + 1) * 4;
            if data_len_off + 8 > str_payload.len() {
                return Ok(false);
            }
            let data_len =
                u64::from_le_bytes(str_payload[data_len_off..data_len_off + 8].try_into().unwrap())
                    as usize;
            let data_start = data_len_off + 8;
            let data_end = (data_start + data_len).min(str_payload.len());
            if data_end < data_start {
                return Ok(false);
            }
            let data = &str_payload[data_start..data_end];
            let offsets = bytes_as_u32_slice(&str_payload[8..], count + 1);
            let offs: &[u32] = &offsets;
            let n = count.min(rg_rows);
            for i in 0..n {
                let s = offs[i] as usize;
                let e = offs[i + 1] as usize;
                if e > data.len() {
                    return Ok(false);
                }
                let cur = &data[s..e];
                if let Some(p) = prev {
                    if p > cur {
                        return Ok(false);
                    }
                }
                prev = Some(cur);
            }
        }
        Ok(true)
    }

    /// Binary-search a sorted PLAIN string column for the contiguous row range
    /// whose value starts with `prefix`. Returns `(start, end)` global indices.
    fn prefix_like_range_sorted(
        &self,
        col_idx: usize,
        prefix: &[u8],
    ) -> io::Result<Option<(usize, usize)>> {
        let Some(footer) = self.get_or_load_footer()? else {
            return Ok(None);
        };
        let file_guard = self.file.read();
        let Some(file) = file_guard.as_ref() else {
            return Ok(None);
        };
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let mut prefix_end = prefix.to_vec();
        {
            let mut i = prefix_end.len();
            while i > 0 {
                i -= 1;
                if prefix_end[i] != 0xFF {
                    prefix_end[i] += 1;
                    break;
                }
            }
        }
        let lo = self.lower_bound_sorted(col_idx, prefix, &footer, mmap_ref)?;
        let hi = self.lower_bound_sorted(col_idx, &prefix_end, &footer, mmap_ref)?;
        Ok(Some((lo, hi)))
    }

    /// First global row index whose string value is >= `target` in a sorted
    /// PLAIN string column.
    fn lower_bound_sorted(
        &self,
        col_idx: usize,
        target: &[u8],
        footer: &V4Footer,
        mmap_ref: &[u8],
    ) -> io::Result<usize> {
        let mut global = 0usize;
        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                break;
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.get(28).copied().unwrap_or(RG_COMPRESS_NONE) != RG_COMPRESS_NONE
                || rg_bytes.get(29).copied().unwrap_or(0) < 1
            {
                break;
            }
            let Some(col_offsets) = footer.col_offsets.get(rg_i) else {
                break;
            };
            if col_offsets.len() <= col_idx {
                break;
            }
            let body = &rg_bytes[32..];
            let bitmap_len = (rg_rows + 7) / 8;
            let str_col_body = &body[col_offsets[col_idx] as usize..];
            if str_col_body.len() < bitmap_len + 1 || str_col_body[bitmap_len] != COL_ENCODING_PLAIN {
                break;
            }
            let str_payload = &str_col_body[bitmap_len + 1..];
            if str_payload.len() < 8 {
                break;
            }
            let count = u64::from_le_bytes(str_payload[0..8].try_into().unwrap()) as usize;
            let data_len_off = 8 + (count + 1) * 4;
            if data_len_off + 8 > str_payload.len() {
                break;
            }
            let data_len =
                u64::from_le_bytes(str_payload[data_len_off..data_len_off + 8].try_into().unwrap())
                    as usize;
            let data_start = data_len_off + 8;
            let data_end = (data_start + data_len).min(str_payload.len());
            if data_end < data_start {
                break;
            }
            let data = &str_payload[data_start..data_end];
            let offsets = bytes_as_u32_slice(&str_payload[8..], count + 1);
            let offs: &[u32] = &offsets;
            let n = count.min(rg_rows);
            if n == 0 {
                continue;
            }
            let first = &data[offs[0] as usize..offs[1] as usize];
            let last = &data[offs[n - 1] as usize..offs[n] as usize];
            if last < target {
                global += n;
                continue;
            }
            if first >= target {
                return Ok(global);
            }
            // Binary-search within this RG.
            let mut lo = 0usize;
            let mut hi = n;
            while lo < hi {
                let mid = (lo + hi) / 2;
                let s = offs[mid] as usize;
                let e = offs[mid + 1] as usize;
                if &data[s..e] < target {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            return Ok(global + lo);
        }
        Ok(global)
    }

    /// Sorted fast path for `num NOT BETWEEN lo AND hi AND str NOT LIKE 'prefix%'`.
    /// Decomposes the count using inclusion–exclusion so only the (small) prefix
    /// range needs its numeric values checked, instead of scanning every string.
    fn try_not_filter_count_sorted(
        &self,
        num_col: &str,
        num_idx: usize,
        lo: f64,
        hi: f64,
        str_idx: usize,
        not_like_kind: &LikeKind,
    ) -> io::Result<Option<i64>> {
        let LikeKind::Prefix(prefix) = not_like_kind else {
            return Ok(None);
        };
        let Some((lo_idx, hi_idx)) = self.prefix_like_range_sorted(str_idx, prefix)? else {
            return Ok(None);
        };
        let like_count = (hi_idx - lo_idx) as i64;
        let total = self.active_row_count() as i64;
        let not_like = total - like_count;

        // |age ∈ [lo, hi]| via the numeric aggregation.
        let age_in = match self.execute_filtered_numeric_agg_mmap(num_col, lo, hi, &["*"])? {
            Some(v) if !v.is_empty() => v[0].0,
            _ => return Ok(None),
        };
        let age_out = total - age_in;

        // |age ∉ [lo, hi] AND name LIKE| = rows in [lo_idx, hi_idx) with age out.
        let like_age_out = match self.count_age_out_in_range(num_idx, lo_idx, hi_idx, lo, hi)? {
            Some(c) => c,
            None => return Ok(None),
        };
        Ok(Some(age_out - like_age_out))
    }

    /// Count rows in the global row range [lo_idx, hi_idx) whose numeric value is
    /// outside [lo, hi]. Reads only the overlapping row groups.
    fn count_age_out_in_range(
        &self,
        num_idx: usize,
        lo_idx: usize,
        hi_idx: usize,
        lo: f64,
        hi: f64,
    ) -> io::Result<Option<i64>> {
        let low_i = lo.ceil() as i64;
        let high_i = hi.floor() as i64;
        let Some(footer) = self.get_or_load_footer()? else {
            return Ok(None);
        };
        let num_type = footer.schema.columns[num_idx].1;
        let file_guard = self.file.read();
        let Some(file) = file_guard.as_ref() else {
            return Ok(None);
        };
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let mut global = 0usize;
        let mut count = 0i64;
        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            let rg_start = global;
            let rg_end = global + rg_rows;
            global = rg_end;
            if rg_end <= lo_idx || rg_start >= hi_idx {
                continue;
            }
            let local_lo = lo_idx.saturating_sub(rg_start);
            let local_hi = (hi_idx - rg_start).min(rg_rows);
            let rg_data_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_data_end > mmap_ref.len() {
                return Ok(None);
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_data_end];
            if rg_bytes.get(28).copied().unwrap_or(RG_COMPRESS_NONE) != RG_COMPRESS_NONE
                || rg_bytes.get(29).copied().unwrap_or(0) < 1
            {
                return Ok(None);
            }
            let Some(col_offsets) = footer.col_offsets.get(rg_i) else {
                return Ok(None);
            };
            if col_offsets.len() <= num_idx {
                return Ok(None);
            }
            let body = &rg_bytes[32..];
            let bitmap_len = (rg_rows + 7) / 8;
            let num_col_body = &body[col_offsets[num_idx] as usize..];
            if num_col_body.len() < bitmap_len {
                return Ok(None);
            }
            let num_col_bytes = &num_col_body[bitmap_len..];
            let (col_data, _) = read_column_encoded(num_col_bytes, num_type)?;
            match col_data {
                ColumnData::Int64(v) => {
                    if v.len() < local_hi {
                        return Ok(None);
                    }
                    for i in local_lo..local_hi {
                        let vv = v[i];
                        if vv < low_i || vv > high_i {
                            count += 1;
                        }
                    }
                }
                ColumnData::Float64(v) => {
                    if v.len() < local_hi {
                        return Ok(None);
                    }
                    for i in local_lo..local_hi {
                        let vv = v[i];
                        if vv < lo as f64 || vv > hi as f64 {
                            count += 1;
                        }
                    }
                }
                _ => return Ok(None),
            }
        }
        Ok(Some(count))
    }

    /// Fused mmap count for `num_col NOT BETWEEN lo AND hi AND str_col NOT LIKE kind`.
    /// Returns the count of rows matching both negated predicates (nulls excluded),
    /// without materializing Arrow arrays or row-index vectors. Returns `None` when
    /// the file layout is not eligible (compressed, non-RCIX, or unsupported types).
    pub fn scan_not_filter_count_mmap(
        &self,
        num_col: &str,
        lo: f64,
        hi: f64,
        str_col: &str,
        pattern: &str,
    ) -> io::Result<Option<i64>> {
        use rayon::prelude::*;

        // `NOT LIKE` requires a wildcard pattern (prefix/suffix/contains/complex).
        // A wildcard-free pattern would be `!=`, handled elsewhere.
        let not_like_kind = match classify_like_pattern(pattern) {
            Some(k) => k,
            None => return Ok(None),
        };

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let num_idx = match schema.get_index(num_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let str_idx = match schema.get_index(str_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let num_type = schema.columns[num_idx].1;
        let num_is_int = matches!(
            num_type,
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
        let num_is_float = matches!(num_type, ColumnType::Float64 | ColumnType::Float32);
        if !num_is_int && !num_is_float {
            return Ok(None);
        }
        let str_type = schema.columns[str_idx].1;
        let str_is_dict = matches!(str_type, ColumnType::StringDict);
        if !matches!(str_type, ColumnType::String | ColumnType::StringDict) {
            return Ok(None);
        }

        // ── SORTED FAST PATH ────────────────────────────────────────────────
        // A prefix NOT LIKE on a sorted, null-free plain-string column has a
        // contiguous matching range: binary-search it (O(log n)) instead of
        // scanning every string. `column_sorted_cached` validates null-freeness
        // for the string column; the numeric column is checked separately.
        let no_deletes = footer.row_groups.iter().all(|rg| rg.deletion_count == 0);
        if !str_is_dict
            && matches!(&not_like_kind, LikeKind::Prefix(_))
            && !self.column_has_nulls(num_col)
            && no_deletes
        {
            if self.column_sorted_cached(str_idx) {
                if let Some(result) = self.try_not_filter_count_sorted(
                    num_col,
                    num_idx,
                    lo,
                    hi,
                    str_idx,
                    &not_like_kind,
                )? {
                    return Ok(Some(result));
                }
            }
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for NOT filter count scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        // All row groups must be uncompressed with RCIX offsets for both columns.
        let max_idx = num_idx.max(str_idx);
        let all_fast = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return false;
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            let compress_flag = rg_bytes.get(28).copied().unwrap_or(RG_COMPRESS_NONE);
            let enc_ver = rg_bytes.get(29).copied().unwrap_or(0);
            compress_flag == RG_COMPRESS_NONE
                && enc_ver >= 1
                && footer
                    .col_offsets
                    .get(rg_i)
                    .map_or(false, |v| v.len() > max_idx)
        });
        if !all_fast {
            return Ok(None);
        }

        struct RgDesc {
            rg_offset: usize,
            rg_data_size: usize,
            rg_rows: usize,
            num_rcix: usize,
            str_rcix: usize,
            has_deletes: bool,
            id_section_len: usize,
        }
        let mut rg_descs: Vec<RgDesc> = Vec::with_capacity(footer.row_groups.len());
        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            rg_descs.push(RgDesc {
                rg_offset: rg_meta.offset as usize,
                rg_data_size: rg_meta.data_size as usize,
                rg_rows: rg_meta.row_count as usize,
                num_rcix: footer.col_offsets[rg_i][num_idx] as usize,
                str_rcix: footer.col_offsets[rg_i][str_idx] as usize,
                has_deletes: rg_meta.deletion_count > 0,
                id_section_len: rg_id_section_len(
                    rg_meta.row_count as usize,
                    mmap_ref
                        .get(rg_meta.offset as usize + 30)
                        .copied()
                        .unwrap_or(RG_IDS_PLAIN),
                ),
            });
        }

        let mmap_ptr: usize = mmap_ref.as_ptr() as usize;
        let mmap_len: usize = mmap_ref.len();
        let results: Vec<io::Result<i64>> = rg_descs
            .par_iter()
            .map(|desc| {
                let mmap =
                    unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                let rg_end = desc.rg_offset + desc.rg_data_size;
                if rg_end > mmap.len() || rg_end < desc.rg_offset + 32 {
                    return Ok(0i64);
                }
                let body = &mmap[desc.rg_offset + 32..rg_end];
                let rg_rows = desc.rg_rows;
                let bitmap_len = (rg_rows + 7) / 8;

                let del_bytes_opt: Option<&[u8]> =
                    if desc.has_deletes && desc.id_section_len + bitmap_len <= body.len() {
                        Some(&body[desc.id_section_len..desc.id_section_len + bitmap_len])
                    } else {
                        None
                    };

                // ── String column: parse offsets (PLAIN) or dict view ────────────
                if desc.str_rcix + bitmap_len > body.len() {
                    return Ok(0i64);
                }
                let str_nulls = &body[desc.str_rcix..desc.str_rcix + bitmap_len];
                let str_col_bytes = &body[desc.str_rcix + bitmap_len..];
                if str_col_bytes.is_empty() {
                    return Ok(0i64);
                }
                let str_encoding = str_col_bytes[0];
                let str_data = &str_col_bytes[1..];

                // ── Numeric column ───────────────────────────────────────────────
                if desc.num_rcix + bitmap_len > body.len() {
                    return Ok(0i64);
                }
                let num_nulls = &body[desc.num_rcix..desc.num_rcix + bitmap_len];
                // Inline BITPACK integer decode: avoids the 8MB Vec<i64> the
                // generic `read_column_encoded` allocates for bit-packed columns.
                let num_col_bytes = &body[desc.num_rcix + bitmap_len..];
                let (num_col_data, num_bitpack): (ColumnData, Option<(&[u8], usize, i64)>) =
                    if num_is_int
                        && num_col_bytes.first() == Some(&COL_ENCODING_BITPACK)
                        && num_col_bytes.len() >= 18
                    {
                        let payload = &num_col_bytes[1..];
                        let _count =
                            u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                        let bit_width = payload[8] as usize;
                        let min_val = i64::from_le_bytes(payload[9..17].try_into().unwrap());
                        let packed = &payload[17..];
                        (ColumnData::Int64(Vec::new()), Some((packed, bit_width, min_val)))
                    } else {
                        let (cd, _) = read_column_encoded(num_col_bytes, num_type)?;
                        (cd, None)
                    };
                // No-null fast path: skip the two per-row null-bit checks when
                // neither column has nulls (the common case).
                let no_nulls = num_nulls.iter().all(|&b| b == 0) && str_nulls.iter().all(|&b| b == 0);

                let n = rg_rows;
                let low_i = lo.ceil() as i64;
                let high_i = hi.floor() as i64;
                let mut c: i64 = 0;

                // ── Fused single-pass count loop ─────────────────────────────────
                // Dispatch numeric (int/float) and string (plain/dict) outside the
                // loop so the hot loop has no per-row branching beyond the match.
                if !str_is_dict {
                    if str_encoding != COL_ENCODING_PLAIN || str_data.len() < 8 {
                        return Ok(0i64);
                    }
                    let count = u64::from_le_bytes(str_data[0..8].try_into().unwrap()) as usize;
                    let data_len_off = 8 + (count + 1) * 4;
                    if data_len_off + 8 > str_data.len() {
                        return Ok(0i64);
                    }
                    let data_str_len =
                        u64::from_le_bytes(str_data[data_len_off..data_len_off + 8].try_into().unwrap())
                            as usize;
                    let data_start = data_len_off + 8;
                    let data_end = (data_start + data_str_len).min(str_data.len());
                    if data_end < data_start {
                        return Ok(0i64);
                    }
                    let data_region = &str_data[data_start..data_end];
                    let offsets = bytes_as_u32_slice(&str_data[8..], count + 1);
                    let offs: &[u32] = &offsets;
                    let m = count.min(n);
                    match &num_col_data {
                        ColumnData::Int64(v) => {
                            if num_bitpack.is_none() && v.len() < m {
                                return Ok(0i64);
                            }
                            for i in 0..m {
                                if let Some(db) = del_bytes_opt {
                                    if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                }
                                if !no_nulls {
                                    if (num_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (str_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                }
                                let vv = match num_bitpack {
                                    Some((packed, bw, minv)) => {
                                        bitpack_value_at(packed, bw, minv, i).unwrap_or(0)
                                    }
                                    None => v[i],
                                };
                                if vv >= low_i && vv <= high_i {
                                    continue;
                                }
                                let s = offs[i] as usize;
                                let e = offs[i + 1] as usize;
                                if e <= data_region.len()
                                    && !like_matches_bytes(&not_like_kind, &data_region[s..e])
                                {
                                    c += 1;
                                }
                            }
                        }
                        ColumnData::Float64(v) => {
                            if v.len() < m {
                                return Ok(0i64);
                            }
                            for i in 0..m {
                                if let Some(db) = del_bytes_opt {
                                    if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                }
                                if !no_nulls {
                                    if (num_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (str_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                }
                                let vv = v[i];
                                if vv >= lo && vv <= hi {
                                    continue;
                                }
                                let s = offs[i] as usize;
                                let e = offs[i + 1] as usize;
                                if e <= data_region.len()
                                    && !like_matches_bytes(&not_like_kind, &data_region[s..e])
                                {
                                    c += 1;
                                }
                            }
                        }
                        _ => return Ok(0i64),
                    }
                } else {
                    if !matches!(
                        str_encoding,
                        COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                    ) {
                        return Ok(0i64);
                    }
                    let Ok(view) =
                        StringDictView::parse(str_data, str_encoding == COL_ENCODING_COMPACT_DICTIONARY)
                    else {
                        return Ok(0i64);
                    };
                    let mut match_flags = vec![false; view.dict_size];
                    for di in 1..view.dict_size {
                        if let Some(value) = view.value(di as u32) {
                            match_flags[di] = like_matches_bytes(&not_like_kind, value);
                        }
                    }
                    let m = view.row_count.min(n);
                    match &num_col_data {
                        ColumnData::Int64(v) => {
                            if num_bitpack.is_none() && v.len() < m {
                                return Ok(0i64);
                            }
                            for i in 0..m {
                                if let Some(db) = del_bytes_opt {
                                    if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                }
                                if !no_nulls {
                                    if (num_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (str_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                }
                                let vv = match num_bitpack {
                                    Some((packed, bw, minv)) => {
                                        bitpack_value_at(packed, bw, minv, i).unwrap_or(0)
                                    }
                                    None => v[i],
                                };
                                if vv >= low_i && vv <= high_i {
                                    continue;
                                }
                                let idx = view.index(i).unwrap_or(0) as usize;
                                if !(idx < match_flags.len() && match_flags[idx]) {
                                    c += 1;
                                }
                            }
                        }
                        ColumnData::Float64(v) => {
                            if v.len() < m {
                                return Ok(0i64);
                            }
                            for i in 0..m {
                                if let Some(db) = del_bytes_opt {
                                    if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                }
                                if !no_nulls {
                                    if (num_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (str_nulls[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                }
                                let vv = v[i];
                                if vv >= lo && vv <= hi {
                                    continue;
                                }
                                let idx = view.index(i).unwrap_or(0) as usize;
                                if !(idx < match_flags.len() && match_flags[idx]) {
                                    c += 1;
                                }
                            }
                        }
                        _ => return Ok(0i64),
                    }
                }
                Ok(c)
            })
            .collect();
        drop(mmap_guard);
        drop(file_guard);

        let mut total: i64 = 0;
        for r in results {
            total += r?;
        }
        Ok(Some(total))
    }

    pub fn scan_numeric_in_mmap(
        &self,
        col_name: &str,
        values: &[i64],
        limit: Option<usize>,
    ) -> io::Result<Option<Vec<usize>>> {
        if values.is_empty() {
            return Ok(Some(Vec::new()));
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

        let mut small_values: Vec<i64> = values.to_vec();
        small_values.sort_unstable();
        small_values.dedup();
        let use_small_values = small_values.len() <= 16;
        let value_set: ahash::AHashSet<i64> = if use_small_values {
            ahash::AHashSet::new()
        } else {
            small_values.iter().copied().collect()
        };
        let matches_value = |v: i64| -> bool {
            if use_small_values {
                small_values.contains(&v)
            } else {
                value_set.contains(&v)
            }
        };
        // For zone map pruning: compute min/max of IN values
        let in_min = *small_values.first().unwrap();
        let in_max = *small_values.last().unwrap();

        let col_count = schema.column_count();
        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for IN scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let max_matches = limit.unwrap_or(usize::MAX);
        let mut matches: Vec<usize> = Vec::new();
        let mut global_row_offset: usize = 0;

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            if matches.len() >= max_matches {
                break;
            }
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                global_row_offset += rg_rows;
                continue;
            }

            // Zone map pruning
            if rg_i < footer.zone_maps.len() {
                if let Some(zm) = footer.zone_maps[rg_i]
                    .iter()
                    .find(|z| z.col_idx as usize == col_idx)
                {
                    let skip = if zm.is_float {
                        !zm.may_overlap_float_range(in_min as f64, in_max as f64)
                    } else {
                        !zm.may_overlap_int_range(in_min, in_max)
                    };
                    if skip {
                        global_row_offset += rg_rows;
                        continue;
                    }
                }
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
            let del_vec_len = (rg_rows + 7) / 8;
            let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
            let id_section = rg_id_section_len(rg_rows, id_encoding);
            if id_section + del_vec_len > body.len() {
                return Err(err_data("RG del vec truncated"));
            }
            let del_bytes = &body[id_section..id_section + del_vec_len];
            let has_deletes = rg_meta.deletion_count > 0;
            let null_bitmap_len = (rg_rows + 7) / 8;

            // RCIX fast path
            let rcix_available = rg_i < footer.col_offsets.len()
                && col_idx < footer.col_offsets[rg_i].len()
                && compress_flag == RG_COMPRESS_NONE;
            if rcix_available {
                let col_body_off = footer.col_offsets[rg_i][col_idx] as usize;
                if col_body_off + null_bitmap_len > body.len() {
                    global_row_offset += rg_rows;
                    continue;
                }
                let null_bytes = &body[col_body_off..col_body_off + null_bitmap_len];
                let data_start = col_body_off + null_bitmap_len;
                let col_bytes = &body[data_start..];
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
                    let no_nulls = !null_bytes.iter().any(|&b| b != 0);
                    let unlimited = max_matches == usize::MAX;
                    if is_int {
                        let vals = bytes_as_i64_slice(&payload[8..], n);
                        if !has_deletes && no_nulls && unlimited {
                            for (i, &v) in vals.iter().enumerate() {
                                if matches_value(v) {
                                    matches.push(global_row_offset + i);
                                }
                            }
                        } else {
                            for i in 0..n {
                                if matches.len() >= max_matches {
                                    break;
                                }
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if !no_nulls && (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if matches_value(vals[i]) {
                                    matches.push(global_row_offset + i);
                                }
                            }
                        }
                    } else {
                        let vals = bytes_as_f64_slice(&payload[8..], n);
                        if !has_deletes && no_nulls && unlimited {
                            for (i, &v) in vals.iter().enumerate() {
                                if matches_value(v as i64) {
                                    matches.push(global_row_offset + i);
                                }
                            }
                        } else {
                            for i in 0..n {
                                if matches.len() >= max_matches {
                                    break;
                                }
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if !no_nulls && (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if matches_value(vals[i] as i64) {
                                    matches.push(global_row_offset + i);
                                }
                            }
                        }
                    }
                    global_row_offset += rg_rows;
                    continue;
                }
            }

            // Fallback: sequential column scan
            let mut pos = id_section + del_vec_len;
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
                    let data_slice = &col_bytes[enc_offset..];

                    if encoding == COL_ENCODING_PLAIN && data_slice.len() >= 8 {
                        let count =
                            u64::from_le_bytes(data_slice[0..8].try_into().unwrap()) as usize;
                        let values_start = 8usize;
                        let n = count.min(rg_rows);
                        if is_int {
                            let nn = n.min((data_slice.len() - values_start) / 8);
                            let vals = bytes_as_i64_slice(&data_slice[values_start..], nn);
                            if !has_deletes {
                                for i in 0..vals.len() {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if matches_value(vals[i]) {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            } else {
                                for i in 0..vals.len() {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if matches_value(vals[i]) {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            }
                        } else {
                            let nn = n.min((data_slice.len() - values_start) / 8);
                            let vals = bytes_as_f64_slice(&data_slice[values_start..], nn);
                            if !has_deletes {
                                for i in 0..vals.len() {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if matches_value(vals[i] as i64) {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            } else {
                                for i in 0..vals.len() {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if matches_value(vals[i] as i64) {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            }
                        }
                    } else {
                        // Non-PLAIN encoding: decode then scan
                        let (col_data, _consumed) = if encoding_version >= 1 {
                            read_column_encoded(col_bytes, ct)?
                        } else {
                            ColumnData::from_bytes_typed(col_bytes, ct)?
                        };
                        match &col_data {
                            ColumnData::Int64(vals) => {
                                for i in 0..vals.len().min(rg_rows) {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if matches_value(vals[i]) {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            }
                            ColumnData::Float64(vals) => {
                                for i in 0..vals.len().min(rg_rows) {
                                    if matches.len() >= max_matches {
                                        break;
                                    }
                                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if matches_value(vals[i] as i64) {
                                        matches.push(global_row_offset + i);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                let consumed = if encoding_version >= 1 {
                    skip_column_encoded(&body[pos..], ct)?
                } else {
                    ColumnData::skip_bytes_typed(&body[pos..], ct)?
                };
                pos += consumed;
            }
            global_row_offset += rg_rows;
        }
        drop(mmap_guard);
        drop(file_guard);
        Ok(Some(matches))
    }

    pub fn scan_and_update_inplace(
        &self,
        where_col: &str,
        low: f64,
        high: f64,
        set_col: &str,
        new_value_bytes: &[u8; 8], // raw little-endian bytes of the new value (f64 or i64)
    ) -> io::Result<Option<i64>> {
        let mut footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let where_idx = match schema.get_index(where_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let set_idx = match schema.get_index(set_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let where_type = schema.columns[where_idx].1;
        let is_int = matches!(
            where_type,
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
        let is_float = matches!(where_type, ColumnType::Float64 | ColumnType::Float32);
        if !is_int && !is_float {
            return Ok(None);
        }
        // Require RCIX for both columns in all row groups
        let n_rgs = footer.row_groups.len();
        if footer.col_offsets.len() < n_rgs {
            return Ok(None);
        }
        let low_i = low.ceil() as i64;
        let high_i = high.floor() as i64;
        let mut total_updated: i64 = 0;
        let mut physically_written = false;
        let mut footer_changed = false;
        let mut pending_writes: Vec<(u64, Vec<u8>)> = Vec::new();

        // Need read-write access: open separate write handle
        let mut write_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)?;

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            // Zone map pruning for WHERE column
            if rg_i < footer.zone_maps.len() {
                if let Some(zm) = footer.zone_maps[rg_i]
                    .iter()
                    .find(|z| z.col_idx as usize == where_idx)
                {
                    let skip = if zm.is_float {
                        !zm.may_overlap_float_range(low, high)
                    } else {
                        !zm.may_overlap_int_range(low_i, high_i)
                    };
                    if skip {
                        continue;
                    }
                }
            }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Ok(None);
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            let compress_flag = if rg_bytes.len() >= 32 {
                rg_bytes[28]
            } else {
                1
            };
            let encoding_version = if rg_bytes.len() >= 32 {
                rg_bytes[29]
            } else {
                0
            };
            // Require uncompressed + RCIX for in-place write
            if compress_flag != RG_COMPRESS_NONE {
                return Ok(None);
            }
            if rg_i >= footer.col_offsets.len() {
                return Ok(None);
            }
            let rg_col_offsets = &footer.col_offsets[rg_i];
            if where_idx >= rg_col_offsets.len() || set_idx >= rg_col_offsets.len() {
                return Ok(None);
            }

            let body = &rg_bytes[32..];
            let del_vec_len = (rg_rows + 7) / 8;
            let null_bitmap_len = del_vec_len;
            let id_section =
                rg_id_section_len(rg_rows, rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN));
            if id_section + del_vec_len > body.len() {
                continue;
            }
            let del_bytes = &body[id_section..id_section + del_vec_len];
            let has_deletes = rg_meta.deletion_count > 0;

            // Read WHERE column (any encoding — decode it)
            let where_col_off = rg_col_offsets[where_idx] as usize;
            if where_col_off + null_bitmap_len > body.len() {
                continue;
            }
            let where_null = &body[where_col_off..where_col_off + null_bitmap_len];
            let where_col_bytes = &body[where_col_off + null_bitmap_len..];

            // Decode WHERE column values (supports PLAIN, BITPACK, RLE).
            // Keep PLAIN mmap data borrowed; copying an 8MB predicate column
            // dominates idempotent UPDATEs on the OLTP benchmark.
            use std::borrow::Cow;
            enum WhereVals<'a> {
                Int(Cow<'a, [i64]>),
                Flt(Cow<'a, [f64]>),
                BitPackI64 {
                    packed: &'a [u8],
                    bit_width: usize,
                    min_val: i64,
                },
            }
            let where_vals: WhereVals<'_>;
            let n: usize;

            if is_int {
                // Try zero-copy PLAIN path first
                let enc = if encoding_version >= 1 && !where_col_bytes.is_empty() {
                    where_col_bytes[0]
                } else {
                    COL_ENCODING_PLAIN
                };
                if enc == COL_ENCODING_PLAIN {
                    let payload = &where_col_bytes[1..];
                    if payload.len() < 8 {
                        continue;
                    }
                    let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                    let nn = count.min(rg_rows).min((payload.len() - 8) / 8);
                    let vals_cow = bytes_as_i64_slice(&payload[8..], nn);
                    n = nn;
                    where_vals = WhereVals::Int(vals_cow);
                } else if enc == COL_ENCODING_BITPACK {
                    let payload = &where_col_bytes[1..];
                    if payload.len() < 17 {
                        continue;
                    }
                    let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                    let bit_width = payload[8] as usize;
                    if bit_width >= 64 {
                        return Ok(None);
                    }
                    let min_val = i64::from_le_bytes(payload[9..17].try_into().unwrap());
                    let packed_bytes = (count * bit_width + 7) / 8;
                    if payload.len() < 17 + packed_bytes {
                        return Ok(None);
                    }
                    n = count.min(rg_rows);
                    where_vals = WhereVals::BitPackI64 {
                        packed: &payload[17..17 + packed_bytes],
                        bit_width,
                        min_val,
                    };
                } else {
                    // Decode (BITPACK, RLE, etc.)
                    let (col_data, _) = read_column_encoded(where_col_bytes, where_type)?;
                    match col_data {
                        ColumnData::Int64(v) => {
                            let nn = v.len().min(rg_rows);
                            n = nn;
                            where_vals = WhereVals::Int(Cow::Owned(v));
                        }
                        _ => continue,
                    }
                }
            } else {
                let enc = if encoding_version >= 1 && !where_col_bytes.is_empty() {
                    where_col_bytes[0]
                } else {
                    COL_ENCODING_PLAIN
                };
                if enc == COL_ENCODING_PLAIN {
                    let payload = &where_col_bytes[1..];
                    if payload.len() < 8 {
                        continue;
                    }
                    let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                    let nn = count.min(rg_rows).min((payload.len() - 8) / 8);
                    let vals_cow = bytes_as_f64_slice(&payload[8..], nn);
                    n = nn;
                    where_vals = WhereVals::Flt(vals_cow);
                } else {
                    let (col_data, _) = read_column_encoded(where_col_bytes, where_type)?;
                    match col_data {
                        ColumnData::Float64(v) => {
                            let nn = v.len().min(rg_rows);
                            n = nn;
                            where_vals = WhereVals::Flt(Cow::Owned(v));
                        }
                        _ => continue,
                    }
                }
            };

            // Verify SET column is PLAIN (required for in-place overwrite)
            let set_col_off = rg_col_offsets[set_idx] as usize;
            if set_col_off + null_bitmap_len > body.len() {
                continue;
            }
            let set_data = &body[set_col_off + null_bitmap_len..];
            let set_enc = if encoding_version >= 1 && !set_data.is_empty() {
                set_data[0]
            } else {
                COL_ENCODING_PLAIN
            };
            if set_enc != COL_ENCODING_PLAIN {
                return Ok(None);
            }
            if set_data.len() < 9 {
                return Ok(None);
            }
            let set_count = u64::from_le_bytes(set_data[1..9].try_into().unwrap()) as usize;
            if set_count < n || set_data.len() < 9 + n * 8 {
                return Ok(None);
            }
            let set_values = &set_data[9..9 + n * 8];

            // File offset of SET column's value array:
            // rg_meta.offset (RG start) + 32 (RG header) + set_col_off + null_bitmap_len + 1 (enc byte) + 8 (count)
            let values_file_offset =
                (rg_meta.offset as usize + 32 + set_col_off + null_bitmap_len + 1 + 8) as u64;

            // Lazily copy and rewrite the SET value array only when at least one
            // matching row would physically change. Repeated idempotent UPDATEs
            // still return the matched-row count without dirtying the file.
            use std::io::{Seek, SeekFrom, Write};
            let mut value_buf: Option<Vec<u8>> = None;
            let mut rg_updated = 0i64;
            match where_vals {
                WhereVals::Int(vals) => {
                    let no_nulls = !where_null.iter().any(|&b| b != 0);
                    if !has_deletes && no_nulls {
                        for i in 0..n {
                            if vals[i] >= low_i && vals[i] <= high_i {
                                let off = i * 8;
                                if &set_values[off..off + 8] != new_value_bytes {
                                    let buf = value_buf.get_or_insert_with(|| set_values.to_vec());
                                    buf[off..off + 8].copy_from_slice(new_value_bytes);
                                }
                                rg_updated += 1;
                            }
                        }
                    } else {
                        for i in 0..n {
                            if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if !no_nulls && (where_null[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if vals[i] >= low_i && vals[i] <= high_i {
                                let off = i * 8;
                                if &set_values[off..off + 8] != new_value_bytes {
                                    let buf = value_buf.get_or_insert_with(|| set_values.to_vec());
                                    buf[off..off + 8].copy_from_slice(new_value_bytes);
                                }
                                rg_updated += 1;
                            }
                        }
                    }
                }
                WhereVals::Flt(vals) => {
                    let no_nulls = !where_null.iter().any(|&b| b != 0);
                    if !has_deletes && no_nulls {
                        for i in 0..n {
                            if vals[i] >= low && vals[i] <= high {
                                let off = i * 8;
                                if &set_values[off..off + 8] != new_value_bytes {
                                    let buf = value_buf.get_or_insert_with(|| set_values.to_vec());
                                    buf[off..off + 8].copy_from_slice(new_value_bytes);
                                }
                                rg_updated += 1;
                            }
                        }
                    } else {
                        for i in 0..n {
                            if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if !no_nulls && (where_null[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if vals[i] >= low && vals[i] <= high {
                                let off = i * 8;
                                if &set_values[off..off + 8] != new_value_bytes {
                                    let buf = value_buf.get_or_insert_with(|| set_values.to_vec());
                                    buf[off..off + 8].copy_from_slice(new_value_bytes);
                                }
                                rg_updated += 1;
                            }
                        }
                    }
                }
                WhereVals::BitPackI64 {
                    packed,
                    bit_width,
                    min_val,
                } => {
                    let mask = if bit_width == 0 {
                        0
                    } else {
                        (1u64 << bit_width) - 1
                    };
                    let no_nulls = !where_null.iter().any(|&b| b != 0);
                    let handle_match =
                        |i: usize, value_buf: &mut Option<Vec<u8>>, rg_updated: &mut i64| {
                            let off = i * 8;
                            if &set_values[off..off + 8] != new_value_bytes {
                                let buf = value_buf.get_or_insert_with(|| set_values.to_vec());
                                buf[off..off + 8].copy_from_slice(new_value_bytes);
                            }
                            *rg_updated += 1;
                        };
                    if bit_width == 0 {
                        if min_val >= low_i && min_val <= high_i {
                            if !has_deletes && no_nulls {
                                for i in 0..n {
                                    handle_match(i, &mut value_buf, &mut rg_updated);
                                }
                            } else {
                                for i in 0..n {
                                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if !no_nulls && (where_null[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    handle_match(i, &mut value_buf, &mut rg_updated);
                                }
                            }
                        }
                    } else if !has_deletes && no_nulls {
                        for i in 0..n {
                            let bit_pos = i * bit_width;
                            let byte_off = bit_pos / 8;
                            let bit_shift = bit_pos % 8;
                            let bytes_needed = (bit_shift + bit_width + 7) / 8;
                            if byte_off + bytes_needed > packed.len() {
                                continue;
                            }
                            let mut raw = 0u64;
                            for j in 0..bytes_needed {
                                raw |= (packed[byte_off + j] as u64) << (j * 8);
                            }
                            let v = min_val.wrapping_add(((raw >> bit_shift) & mask) as i64);
                            if v >= low_i && v <= high_i {
                                handle_match(i, &mut value_buf, &mut rg_updated);
                            }
                        }
                    } else {
                        for i in 0..n {
                            if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if !no_nulls && (where_null[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            let bit_pos = i * bit_width;
                            let byte_off = bit_pos / 8;
                            let bit_shift = bit_pos % 8;
                            let bytes_needed = (bit_shift + bit_width + 7) / 8;
                            if byte_off + bytes_needed > packed.len() {
                                continue;
                            }
                            let mut raw = 0u64;
                            for j in 0..bytes_needed {
                                raw |= (packed[byte_off + j] as u64) << (j * 8);
                            }
                            let v = min_val.wrapping_add(((raw >> bit_shift) & mask) as i64);
                            if v >= low_i && v <= high_i {
                                handle_match(i, &mut value_buf, &mut rg_updated);
                            }
                        }
                    }
                }
            }
            if rg_updated > 0 {
                if let Some(value_buf) = value_buf {
                    if let Some(zone_map) = footer.zone_maps.get_mut(rg_i).and_then(|maps| {
                        maps.iter_mut()
                            .find(|map| map.col_idx as usize == set_idx)
                    }) {
                        if zone_map.is_float {
                            let value = f64::from_le_bytes(*new_value_bytes);
                            let old_min = f64::from_bits(zone_map.min_bits as u64);
                            let old_max = f64::from_bits(zone_map.max_bits as u64);
                            if !value.is_nan() && value < old_min {
                                zone_map.min_bits = value.to_bits() as i64;
                                footer_changed = true;
                            }
                            if !value.is_nan() && value > old_max {
                                zone_map.max_bits = value.to_bits() as i64;
                                footer_changed = true;
                            }
                        } else {
                            let value = i64::from_le_bytes(*new_value_bytes);
                            if value < zone_map.min_bits {
                                zone_map.min_bits = value;
                                footer_changed = true;
                            }
                            if value > zone_map.max_bits {
                                zone_map.max_bits = value;
                                footer_changed = true;
                            }
                        }
                    }
                    pending_writes.push((values_file_offset, value_buf));
                    physically_written = true;
                }
                total_updated += rg_updated;
            }
        }
        drop(mmap_guard);
        drop(file_guard);
        if physically_written {
            // Exact aggregate stats and pruning metadata must become
            // conservative before any cell bytes change. A crash at any later
            // point can therefore only cause extra scanning, never stale
            // aggregates or false-negative pruning.
            self.delete_col_stats_sidecar()?;
            if footer_changed {
                write_file.seek(SeekFrom::Start(self.footer_offset_hint()))?;
                write_file.write_all(&footer.to_bytes())?;
            }
            for (offset, value_buf) in pending_writes {
                write_file.seek(SeekFrom::Start(offset))?;
                write_file.write_all(&value_buf)?;
            }
            self.mmap_cache.write().invalidate();
            self.invalidate_page_cache();
            *self.v4_footer.write() = Some(footer);
            crate::storage::epoch::bump(&self.path);
        }
        Ok(Some(total_updated))
    }

    pub fn locate_numeric_cell_for_update(
        &self,
        id: u64,
        set_col: &str,
    ) -> io::Result<Option<(u64, u64, u8, u64)>> {
        if set_col == "_id" {
            return Ok(None);
        }

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let footer_offset = self.footer_offset_hint();
        if footer_offset == 0 {
            return Ok(None);
        }

        let schema = &footer.schema;
        let set_idx = match schema.get_index(set_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let set_type = schema.columns[set_idx].1;
        let is_numeric = matches!(
            set_type,
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
        );
        if !is_numeric {
            return Ok(None);
        }

        let (rg_i, rg_meta) = match footer
            .row_groups
            .iter()
            .enumerate()
            .find(|(_, rg)| rg.min_id <= id && id <= rg.max_id && rg.row_count > 0)
        {
            Some(v) => v,
            None => return Ok(Some((footer_offset, 0, 0, 0))),
        };
        if rg_i >= footer.col_offsets.len() || set_idx >= footer.col_offsets[rg_i].len() {
            return Ok(None);
        }

        let rg_rows = rg_meta.row_count as usize;
        let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        if rg_end > mmap_ref.len() {
            return Ok(None);
        }

        let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
        let compress_flag = if rg_bytes.len() >= 32 {
            rg_bytes[28]
        } else {
            1
        };
        let encoding_version = if rg_bytes.len() >= 32 {
            rg_bytes[29]
        } else {
            0
        };
        if compress_flag != RG_COMPRESS_NONE || encoding_version < 1 {
            return Ok(None);
        }

        let body = &rg_bytes[32..];
        let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
        let id_section_len = rg_id_section_len(rg_rows, id_encoding);
        let guess = id.saturating_sub(rg_meta.min_id) as usize;
        if guess >= rg_rows {
            return Ok(Some((footer_offset, 0, 0, 0)));
        }
        let actual_id = match rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, guess) {
            Some(id) => id,
            None => return Ok(None),
        };
        if actual_id != id {
            return Ok(None);
        }

        let bitmap_len = (rg_rows + 7) / 8;
        let del_off = id_section_len + guess / 8;
        if del_off >= body.len() {
            return Ok(None);
        }
        if ((body[del_off] >> (guess % 8)) & 1) == 1 {
            return Ok(Some((footer_offset, 0, 0, 0)));
        }

        let set_col_off = footer.col_offsets[rg_i][set_idx] as usize;
        if set_col_off + bitmap_len + 1 + 8 > body.len() {
            return Ok(None);
        }
        let set_data = &body[set_col_off + bitmap_len..];
        if set_data.is_empty() || set_data[0] != COL_ENCODING_PLAIN {
            return Ok(None);
        }

        let value_file_offset =
            rg_meta.offset + 32 + (set_col_off + bitmap_len + 1 + 8 + guess * 8) as u64;
        let null_byte_file_offset = rg_meta.offset + 32 + (set_col_off + guess / 8) as u64;
        Ok(Some((
            footer_offset,
            null_byte_file_offset,
            1u8 << (guess % 8),
            value_file_offset,
        )))
    }

    pub fn update_by_id_inplace(
        &self,
        id: u64,
        set_col: &str,
        new_value_bytes: &[u8; 8],
    ) -> io::Result<Option<(i64, bool)>> {
        if set_col == "_id" {
            return Ok(None);
        }

        let mut footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let set_idx = match schema.get_index(set_col) {
            Some(i) => i,
            None => return Ok(None),
        };
        let set_type = schema.columns[set_idx].1;
        let is_numeric = matches!(
            set_type,
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
        );
        if !is_numeric {
            return Ok(None);
        }

        let (rg_i, rg_meta) = match footer
            .row_groups
            .iter()
            .enumerate()
            .find(|(_, rg)| rg.min_id <= id && id <= rg.max_id && rg.row_count > 0)
        {
            Some(v) => v,
            None => return Ok(Some((0, false))),
        };
        if rg_i >= footer.col_offsets.len() || set_idx >= footer.col_offsets[rg_i].len() {
            return Ok(None);
        }

        let rg_rows = rg_meta.row_count as usize;
        let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        if rg_end > mmap_ref.len() {
            return Ok(None);
        }

        let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
        let compress_flag = if rg_bytes.len() >= 32 {
            rg_bytes[28]
        } else {
            1
        };
        let encoding_version = if rg_bytes.len() >= 32 {
            rg_bytes[29]
        } else {
            0
        };
        if compress_flag != RG_COMPRESS_NONE || encoding_version < 1 {
            return Ok(None);
        }

        let body = &rg_bytes[32..];
        let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
        let id_section_len = rg_id_section_len(rg_rows, id_encoding);
        let guess = id.saturating_sub(rg_meta.min_id) as usize;
        if guess >= rg_rows {
            return Ok(Some((0, false)));
        }
        let actual_id = match rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, guess) {
            Some(id) => id,
            None => return Ok(None),
        };
        if actual_id != id {
            return Ok(None);
        }

        let bitmap_len = (rg_rows + 7) / 8;
        let del_off = id_section_len + guess / 8;
        if del_off >= body.len() {
            return Ok(None);
        }
        if ((body[del_off] >> (guess % 8)) & 1) == 1 {
            return Ok(Some((0, false)));
        }

        let set_col_off = footer.col_offsets[rg_i][set_idx] as usize;
        if set_col_off + bitmap_len + 1 + 8 > body.len() {
            return Ok(None);
        }
        let set_data = &body[set_col_off + bitmap_len..];
        if set_data.is_empty() || set_data[0] != COL_ENCODING_PLAIN {
            return Ok(None);
        }

        let value_file_offset =
            rg_meta.offset + 32 + (set_col_off + bitmap_len + 1 + 8 + guess * 8) as u64;
        let null_byte_file_offset = rg_meta.offset + 32 + (set_col_off + guess / 8) as u64;
        let value_body_offset = set_col_off + bitmap_len + 1 + 8 + guess * 8;
        let null_byte = body[set_col_off + guess / 8];
        if ((null_byte >> (guess % 8)) & 1) == 0
            && value_body_offset + 8 <= body.len()
            && &body[value_body_offset..value_body_offset + 8] == new_value_bytes
        {
            return Ok(Some((1, false)));
        }

        use std::io::{Seek, SeekFrom, Write};
        let mut write_file_guard = self.write_file.write();
        if write_file_guard.is_none() {
            *write_file_guard = Some(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&self.path)?,
            );
        }
        let write_file = write_file_guard
            .as_mut()
            .ok_or_else(|| err_not_conn("Write file not open"))?;

        // Zone maps are a pruning hint, so widening is sufficient for
        // correctness and avoids rescanning the whole row group.  Persist the
        // widened footer before the cell write: after a crash, readers may see
        // the old or new value, but the pruning range covers both.
        let mut footer_changed = false;
        if let Some(zone_map) = footer
            .zone_maps
            .get_mut(rg_i)
            .and_then(|maps| maps.iter_mut().find(|map| map.col_idx as usize == set_idx))
        {
            if zone_map.is_float {
                let value = f64::from_le_bytes(*new_value_bytes);
                let old_min = f64::from_bits(zone_map.min_bits as u64);
                let old_max = f64::from_bits(zone_map.max_bits as u64);
                if value.is_finite() {
                    if value < old_min {
                        zone_map.min_bits = value.to_bits() as i64;
                        footer_changed = true;
                    }
                    if value > old_max {
                        zone_map.max_bits = value.to_bits() as i64;
                        footer_changed = true;
                    }
                }
            } else {
                let value = i64::from_le_bytes(*new_value_bytes);
                if value < zone_map.min_bits {
                    zone_map.min_bits = value;
                    footer_changed = true;
                }
                if value > zone_map.max_bits {
                    zone_map.max_bits = value;
                    footer_changed = true;
                }
            }
        }
        if footer_changed {
            let footer_bytes = footer.to_bytes();
            write_file.seek(SeekFrom::Start(self.footer_offset_hint()))?;
            write_file.write_all(&footer_bytes)?;
        }

        // The sidecar contains exact SUM/MIN/MAX values. Remove it before the
        // cell mutation so a crash can never leave exact-looking stale stats;
        // the next aggregation falls back to the mmap scan.
        self.delete_col_stats_sidecar()?;
        let mut null_byte = null_byte;
        null_byte &= !(1u8 << (guess % 8));
        write_file.seek(SeekFrom::Start(null_byte_file_offset))?;
        write_file.write_all(&[null_byte])?;
        write_file.seek(SeekFrom::Start(value_file_offset))?;
        write_file.write_all(new_value_bytes)?;

        drop(mmap_guard);
        drop(file_guard);
        self.mmap_cache.write().invalidate();
        self.invalidate_page_cache();
        *self.v4_footer.write() = Some(footer);
        crate::storage::epoch::bump(&self.path);
        Ok(Some((1, true)))
    }

    pub fn update_numeric_cell_cached(
        &self,
        footer_offset: u64,
        null_byte_file_offset: u64,
        null_mask: u8,
        value_file_offset: u64,
        new_value_bytes: &[u8; 8],
    ) -> io::Result<Option<(i64, bool)>> {
        if footer_offset == 0 || self.footer_offset_hint() != footer_offset {
            return Ok(None);
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open"))?;

        let mut null_byte = [0u8; 1];
        pread_fallback(file, &mut null_byte, null_byte_file_offset)?;
        if (null_byte[0] & null_mask) != 0 {
            return Ok(Some((0, false)));
        }

        let mut current = [0u8; 8];
        pread_fallback(file, &mut current, value_file_offset)?;
        if current == *new_value_bytes {
            return Ok(Some((1, false)));
        }

        use std::io::{Seek, SeekFrom, Write};
        let mut write_file_guard = self.write_file.write();
        if write_file_guard.is_none() {
            *write_file_guard = Some(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&self.path)?,
            );
        }
        let write_file = write_file_guard
            .as_mut()
            .ok_or_else(|| err_not_conn("Write file not open"))?;

        let updated_null = null_byte[0] & !null_mask;
        if updated_null != null_byte[0] {
            write_file.seek(SeekFrom::Start(null_byte_file_offset))?;
            write_file.write_all(&[updated_null])?;
        }
        write_file.seek(SeekFrom::Start(value_file_offset))?;
        write_file.write_all(new_value_bytes)?;

        crate::storage::epoch::bump(&self.path);
        Ok(Some((1, true)))
    }

    pub fn row_id_active_rcix(&self, id: u64) -> io::Result<Option<bool>> {
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let rg_meta = match footer
            .row_groups
            .iter()
            .find(|rg| rg.min_id <= id && id <= rg.max_id && rg.row_count > 0)
        {
            Some(rg) => rg,
            None => return Ok(Some(false)),
        };
        let rg_rows = rg_meta.row_count as usize;
        let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        if rg_end > mmap_ref.len() {
            return Ok(None);
        }
        let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
        if rg_bytes.len() < 32 || rg_bytes[28] != RG_COMPRESS_NONE {
            return Ok(None);
        }

        let body = &rg_bytes[32..];
        let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
        let id_section_len = rg_id_section_len(rg_rows, id_encoding);
        if body.len() < id_section_len {
            return Ok(None);
        }
        let guess = id.saturating_sub(rg_meta.min_id) as usize;
        let local_idx = if guess < rg_rows {
            let actual = rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, guess)
                .ok_or_else(|| err_data("RG ID section truncated"))?;
            if actual == id {
                guess
            } else {
                let ids_cow = bytes_as_u64_slice(&body[..id_section_len], rg_rows);
                match ids_cow.binary_search(&id) {
                    Ok(i) => i,
                    Err(_) => return Ok(Some(false)),
                }
            }
        } else {
            return Ok(Some(false));
        };

        let del_off = id_section_len + local_idx / 8;
        if del_off >= body.len() {
            return Ok(None);
        }
        let deleted = ((body[del_off] >> (local_idx % 8)) & 1) == 1;
        Ok(Some(!deleted))
    }

    pub fn scan_numeric_range_mmap_with_ids(
        &self,
        col_name: &str,
        low: f64,
        high: f64,
    ) -> io::Result<Option<Vec<u64>>> {
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
            .ok_or_else(|| err_not_conn("File not open for range+id scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        let low_i = low.ceil() as i64;
        let high_i = high.floor() as i64;
        let mut result: Vec<u64> = Vec::new();

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                continue;
            }
            // Skip fully-deleted row groups — zone maps may still show overlap
            if rg_meta.active_rows() == 0 {
                continue;
            }

            // Zone map pruning
            if rg_i < footer.zone_maps.len() {
                if let Some(zm) = footer.zone_maps[rg_i]
                    .iter()
                    .find(|z| z.col_idx as usize == col_idx)
                {
                    let skip = if zm.is_float {
                        !zm.may_overlap_float_range(low, high)
                    } else {
                        !zm.may_overlap_int_range(low_i, high_i)
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

            let id_encoding = rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN);
            let id_section = rg_id_section_len(rg_rows, id_encoding);
            let del_vec_len = (rg_rows + 7) / 8;
            let null_bitmap_len = del_vec_len;
            if id_section + del_vec_len > body.len() {
                continue;
            }

            let del_bytes = &body[id_section..id_section + del_vec_len];
            let has_deletes = rg_meta.deletion_count > 0;

            // RCIX fast path: jump directly to target column
            let rcix_available = rg_i < footer.col_offsets.len()
                && col_idx < footer.col_offsets[rg_i].len()
                && compress_flag == RG_COMPRESS_NONE;
            if rcix_available {
                let col_body_off = footer.col_offsets[rg_i][col_idx] as usize;
                if col_body_off + null_bitmap_len > body.len() {
                    continue;
                }
                let null_bytes = &body[col_body_off..col_body_off + null_bitmap_len];
                let data_start = col_body_off + null_bitmap_len;
                let col_bytes = &body[data_start..];
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
                    if is_int {
                        let vals = bytes_as_i64_slice(&payload[8..], n);
                        for i in 0..n {
                            if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if vals[i] >= low_i && vals[i] <= high_i {
                                if let Some(id) =
                                    rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, i)
                                {
                                    result.push(id);
                                }
                            }
                        }
                    } else {
                        let vals = bytes_as_f64_slice(&payload[8..], n);
                        for i in 0..n {
                            if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                            if vals[i] >= low && vals[i] <= high {
                                if let Some(id) =
                                    rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, i)
                                {
                                    result.push(id);
                                }
                            }
                        }
                    }
                    continue;
                }
                // RCIX encoding-aware range pruning: read min/max from encoding header
                // without allocating, skip RG if range can't overlap [low_i, high_i].
                if is_int {
                    let data = &col_bytes[enc_offset..];
                    let can_skip = if encoding == COL_ENCODING_BITPACK && data.len() >= 17 {
                        // Header: [count:u64][bit_width:u8][min_value:i64]
                        let bit_width = data[8] as u32;
                        let min_val = i64::from_le_bytes(data[9..17].try_into().unwrap());
                        let max_val = if bit_width == 0 {
                            min_val
                        } else {
                            min_val.saturating_add(((1u64 << bit_width) - 1) as i64)
                        };
                        max_val < low_i || min_val > high_i
                    } else if encoding == COL_ENCODING_RLE && data.len() >= 16 {
                        // Header: [count:u64][num_runs:u64][(value:i64,run_len:u32)...]
                        let num_runs = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
                        let mut rle_min = i64::MAX;
                        let mut rle_max = i64::MIN;
                        let mut ok = true;
                        for r in 0..num_runs {
                            let off = 16 + r * 12;
                            if off + 8 > data.len() {
                                ok = false;
                                break;
                            }
                            let v = i64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                            if v < rle_min {
                                rle_min = v;
                            }
                            if v > rle_max {
                                rle_max = v;
                            }
                        }
                        ok && (rle_max < low_i || rle_min > high_i)
                    } else {
                        false
                    };
                    if can_skip {
                        continue;
                    }
                }
            }

            // Sequential fallback: scan columns until we reach col_idx
            let mut pos = id_section + del_vec_len;
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
                    let encoding = if encoding_version >= 1 && !col_bytes.is_empty() {
                        col_bytes[0]
                    } else {
                        COL_ENCODING_PLAIN
                    };
                    let data_slice = &col_bytes[enc_offset..];
                    if encoding == COL_ENCODING_PLAIN && data_slice.len() >= 8 {
                        let count =
                            u64::from_le_bytes(data_slice[0..8].try_into().unwrap()) as usize;
                        let n = count.min(rg_rows);
                        if is_int {
                            let nn = n.min((data_slice.len() - 8) / 8);
                            let vals = bytes_as_i64_slice(&data_slice[8..], nn);
                            for i in 0..vals.len() {
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if vals[i] >= low_i && vals[i] <= high_i {
                                    if let Some(id) =
                                        rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, i)
                                    {
                                        result.push(id);
                                    }
                                }
                            }
                        } else {
                            let nn = n.min((data_slice.len() - 8) / 8);
                            let vals = bytes_as_f64_slice(&data_slice[8..], nn);
                            for i in 0..vals.len() {
                                if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if vals[i] >= low && vals[i] <= high {
                                    if let Some(id) =
                                        rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, i)
                                    {
                                        result.push(id);
                                    }
                                }
                            }
                        }
                    } else {
                        // Range pruning before full decode: read min/max from encoding
                        // header without allocating to skip RGs with no matching values.
                        if is_int && encoding_version >= 1 {
                            let data = &col_bytes[enc_offset..];
                            let can_skip = if encoding == COL_ENCODING_BITPACK && data.len() >= 17 {
                                let bit_width = data[8] as u32;
                                let min_val = i64::from_le_bytes(data[9..17].try_into().unwrap());
                                let max_val = if bit_width == 0 {
                                    min_val
                                } else {
                                    min_val.saturating_add(((1u64 << bit_width) - 1) as i64)
                                };
                                max_val < low_i || min_val > high_i
                            } else if encoding == COL_ENCODING_RLE && data.len() >= 16 {
                                let num_runs =
                                    u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
                                let mut rle_min = i64::MAX;
                                let mut rle_max = i64::MIN;
                                let mut ok = true;
                                for r in 0..num_runs {
                                    let off = 16 + r * 12;
                                    if off + 8 > data.len() {
                                        ok = false;
                                        break;
                                    }
                                    let v =
                                        i64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                                    if v < rle_min {
                                        rle_min = v;
                                    }
                                    if v > rle_max {
                                        rle_max = v;
                                    }
                                }
                                ok && (rle_max < low_i || rle_min > high_i)
                            } else {
                                false
                            };
                            if can_skip {
                                break;
                            }
                        }
                        let (col_data, _) = if encoding_version >= 1 {
                            read_column_encoded(col_bytes, ct)?
                        } else {
                            ColumnData::from_bytes_typed(col_bytes, ct)?
                        };
                        match &col_data {
                            ColumnData::Int64(vals) => {
                                for i in 0..vals.len().min(rg_rows) {
                                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if vals[i] >= low_i && vals[i] <= high_i {
                                        if let Some(id) =
                                            rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, i)
                                        {
                                            result.push(id);
                                        }
                                    }
                                }
                            }
                            ColumnData::Float64(vals) => {
                                for i in 0..vals.len().min(rg_rows) {
                                    if has_deletes && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                        continue;
                                    }
                                    if vals[i] >= low && vals[i] <= high {
                                        if let Some(id) =
                                            rg_id_at(body, rg_rows, rg_meta.min_id, id_encoding, i)
                                        {
                                            result.push(id);
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    break; // Found and processed the target column, move to next RG
                }
                let consumed = if encoding_version >= 1 {
                    skip_column_encoded(&body[pos..], ct)?
                } else {
                    ColumnData::skip_bytes_typed(&body[pos..], ct)?
                };
                pos += consumed;
            }
        }
        drop(mmap_guard);
        drop(file_guard);
        Ok(Some(result))
    }

    pub fn estimate_zone_map_range(
        &self,
        col_name: &str,
        low: f64,
        high: f64,
    ) -> io::Result<Option<(u64, u64, u32, u32)>> {
        let Some(footer) = self.get_or_load_footer()? else {
            return Ok(None);
        };
        let Some(col_idx) = footer.schema.get_index(col_name) else {
            return Ok(None);
        };
        let low_i = low.ceil() as i64;
        let high_i = high.floor() as i64;
        let mut matching_rows = 0u64;
        let mut total_rows = 0u64;
        let mut matching_groups = 0u32;
        let mut saw_zone_map = false;
        for (rg_idx, row_group) in footer.row_groups.iter().enumerate() {
            let active = row_group.active_rows() as u64;
            total_rows = total_rows.saturating_add(active);
            let zone_map = footer
                .zone_maps
                .get(rg_idx)
                .and_then(|maps| maps.iter().find(|map| map.col_idx as usize == col_idx));
            let may_match = match zone_map {
                Some(map) => {
                    saw_zone_map = true;
                    if map.is_float {
                        map.may_overlap_float_range(low, high)
                    } else {
                        map.may_overlap_int_range(low_i, high_i)
                    }
                }
                None => true,
            };
            if may_match && active > 0 {
                matching_rows = matching_rows.saturating_add(active);
                matching_groups = matching_groups.saturating_add(1);
            }
        }
        Ok(saw_zone_map.then_some((
            matching_rows,
            total_rows,
            matching_groups,
            footer.row_groups.len() as u32,
        )))
    }

    pub fn scan_multi_predicates_parallel(
        &self,
        predicates: &[MmapScanPred],
    ) -> io::Result<Option<Vec<usize>>> {
        use rayon::prelude::*;

        if predicates.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;

        // Pre-resolve column indices and validate types
        struct PredDesc {
            col_idx: usize,
            is_int: bool,
            is_float: bool,
            is_string: bool,
            is_dict: bool,
        }
        let mut descs: Vec<Option<PredDesc>> = Vec::with_capacity(predicates.len());
        for pred in predicates {
            let col_name = match pred {
                MmapScanPred::NumericRange { col, .. } => *col,
                MmapScanPred::StringEq { col, .. } => *col,
                MmapScanPred::NumericIn { col, .. } => *col,
                MmapScanPred::StringIn { col, .. } => *col,
            };
            let col_idx = match schema.get_index(col_name) {
                Some(i) => i,
                None => {
                    descs.push(None);
                    continue;
                }
            };
            let ct = schema.columns[col_idx].1;
            let is_int = matches!(
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
            );
            let is_float = matches!(ct, ColumnType::Float64 | ColumnType::Float32);
            let is_string = matches!(ct, ColumnType::String);
            let is_dict = matches!(ct, ColumnType::StringDict);
            descs.push(Some(PredDesc {
                col_idx,
                is_int,
                is_float,
                is_string,
                is_dict,
            }));
        }

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for multi-pred scan"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        // Cast mmap pointer for safe sharing across rayon threads (read-only, mmap_guard keeps it alive)
        let mmap_ptr: usize = mmap_ref.as_ptr() as usize;
        let mmap_len: usize = mmap_ref.len();

        // Pre-build per-RG descriptors
        struct RgInfo {
            offset: usize,
            data_size: usize,
            row_count: usize,
            global_off: usize,
            deletion_count: u64,
        }
        let mut rg_infos: Vec<RgInfo> = Vec::with_capacity(footer.row_groups.len());
        let mut cumul = 0usize;
        for rg in &footer.row_groups {
            rg_infos.push(RgInfo {
                offset: rg.offset as usize,
                data_size: rg.data_size as usize,
                row_count: rg.row_count as usize,
                global_off: cumul,
                deletion_count: rg.deletion_count as u64,
            });
            cumul += rg.row_count as usize;
        }

        // Scan each predicate in parallel
        let results: Vec<Vec<usize>> = predicates
            .par_iter()
            .enumerate()
            .map(|(pi, pred)| {
                let desc = match &descs[pi] {
                    Some(d) => d,
                    None => return vec![],
                };
                let mmap = unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                let col_idx = desc.col_idx;
                let mut matches: Vec<usize> = Vec::new();

                for (rg_i, rg) in rg_infos.iter().enumerate() {
                    let rg_rows = rg.row_count;
                    if rg_rows == 0 {
                        continue;
                    }

                    // Zone map pruning
                    if rg_i < footer.zone_maps.len() {
                        if let Some(zm) = footer.zone_maps[rg_i]
                            .iter()
                            .find(|z| z.col_idx as usize == col_idx)
                        {
                            let skip = match pred {
                                MmapScanPred::NumericRange { low, high, .. } => {
                                    if zm.is_float {
                                        !zm.may_overlap_float_range(*low, *high)
                                    } else {
                                        !zm.may_overlap_int_range(
                                            low.ceil() as i64,
                                            high.floor() as i64,
                                        )
                                    }
                                }
                                MmapScanPred::NumericIn { values, .. } => {
                                    if let (Some(&mn), Some(&mx)) =
                                        (values.iter().min(), values.iter().max())
                                    {
                                        if zm.is_float {
                                            !zm.may_overlap_float_range(mn as f64, mx as f64)
                                        } else {
                                            !zm.may_overlap_int_range(mn, mx)
                                        }
                                    } else {
                                        false
                                    }
                                }
                                MmapScanPred::StringEq { value, .. } => {
                                    if !zm.is_float {
                                        let tlen = value.len() as i64;
                                        tlen < zm.min_bits || tlen > zm.max_bits
                                    } else {
                                        false
                                    }
                                }
                                MmapScanPred::StringIn { values, .. } => {
                                    if !zm.is_float && !values.is_empty() {
                                        let min_len =
                                            values.iter().map(|s| s.len()).min().unwrap() as i64;
                                        let max_len =
                                            values.iter().map(|s| s.len()).max().unwrap() as i64;
                                        max_len < zm.min_bits || min_len > zm.max_bits
                                    } else {
                                        false
                                    }
                                }
                            };
                            if skip {
                                continue;
                            }
                        }
                    }

                    let rg_end = rg.offset + rg.data_size;
                    if rg_end > mmap.len() || rg_end < rg.offset + 32 {
                        continue;
                    }
                    let rg_bytes = &mmap[rg.offset..rg_end];
                    let compress_flag = rg_bytes[28];
                    let encoding_version = rg_bytes[29];
                    if compress_flag != RG_COMPRESS_NONE {
                        continue;
                    } // skip compressed RGs in parallel path
                    let body = &rg_bytes[32..];
                    let null_bitmap_len = (rg_rows + 7) / 8;
                    let del_start = rg_id_section_len(
                        rg_rows,
                        rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
                    );
                    let has_deletes = rg.deletion_count > 0;

                    // RCIX required for parallel path
                    let rcix = footer.col_offsets.get(rg_i).filter(|v| v.len() > col_idx);
                    let rcix = match rcix {
                        Some(r) => r,
                        None => continue,
                    };
                    let col_off = rcix[col_idx] as usize;
                    if col_off + null_bitmap_len > body.len() {
                        continue;
                    }
                    let null_bytes = &body[col_off..col_off + null_bitmap_len];
                    let col_bytes = &body[col_off + null_bitmap_len..];
                    let enc_offset = if encoding_version >= 1 { 1usize } else { 0 };
                    let encoding = if encoding_version >= 1 && !col_bytes.is_empty() {
                        col_bytes[0]
                    } else {
                        COL_ENCODING_PLAIN
                    };
                    if encoding != COL_ENCODING_PLAIN {
                        continue;
                    }
                    let payload = if enc_offset <= col_bytes.len() {
                        &col_bytes[enc_offset..]
                    } else {
                        continue;
                    };

                    match pred {
                        MmapScanPred::NumericRange { low, high, .. }
                            if desc.is_int && payload.len() >= 8 =>
                        {
                            let count =
                                u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                            let n = count.min(rg_rows).min((payload.len() - 8) / 8);
                            let vals = bytes_as_i64_slice(&payload[8..], n);
                            let low_i = low.ceil() as i64;
                            let high_i = high.floor() as i64;
                            for i in 0..n {
                                if has_deletes
                                    && del_start + null_bitmap_len <= body.len()
                                    && (body[del_start + i / 8] >> (i % 8)) & 1 == 1
                                {
                                    continue;
                                }
                                if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if vals[i] >= low_i && vals[i] <= high_i {
                                    matches.push(rg.global_off + i);
                                }
                            }
                        }
                        MmapScanPred::NumericRange { low, high, .. }
                            if desc.is_float && payload.len() >= 8 =>
                        {
                            let count =
                                u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                            let n = count.min(rg_rows).min((payload.len() - 8) / 8);
                            let vals = bytes_as_f64_slice(&payload[8..], n);
                            for i in 0..n {
                                if has_deletes
                                    && del_start + null_bitmap_len <= body.len()
                                    && (body[del_start + i / 8] >> (i % 8)) & 1 == 1
                                {
                                    continue;
                                }
                                if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if vals[i] >= *low && vals[i] <= *high {
                                    matches.push(rg.global_off + i);
                                }
                            }
                        }
                        MmapScanPred::NumericIn { values, .. }
                            if desc.is_int && payload.len() >= 8 =>
                        {
                            let value_set: std::collections::HashSet<i64> =
                                values.iter().copied().collect();
                            let count =
                                u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                            let n = count.min(rg_rows).min((payload.len() - 8) / 8);
                            let vals = bytes_as_i64_slice(&payload[8..], n);
                            for i in 0..n {
                                if has_deletes
                                    && del_start + null_bitmap_len <= body.len()
                                    && (body[del_start + i / 8] >> (i % 8)) & 1 == 1
                                {
                                    continue;
                                }
                                if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                if value_set.contains(&vals[i]) {
                                    matches.push(rg.global_off + i);
                                }
                            }
                        }
                        MmapScanPred::StringEq { value, .. }
                            if desc.is_dict && payload.len() >= 16 =>
                        {
                            let target_bytes = value.as_bytes();
                            let row_count =
                                u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                            let dict_size =
                                u64::from_le_bytes(payload[8..16].try_into().unwrap()) as usize;
                            if dict_size == 0 {
                                continue;
                            }
                            let indices_start = 16usize;
                            let dict_off_start = indices_start + row_count * 4;
                            let dict_data_len_off = dict_off_start + dict_size * 4;
                            if dict_data_len_off + 8 > payload.len() {
                                continue;
                            }
                            let dict_data_len = u64::from_le_bytes(
                                payload[dict_data_len_off..dict_data_len_off + 8]
                                    .try_into()
                                    .unwrap(),
                            ) as usize;
                            let dict_data_start = dict_data_len_off + 8;

                            let dict_offsets =
                                bytes_as_u32_slice(&payload[dict_off_start..], dict_size);
                            let indices = bytes_as_u32_slice(&payload[indices_start..], row_count);

                            // Find target in dictionary
                            let raw_end = (dict_data_start + dict_data_len).min(payload.len());
                            let raw_dict = &payload[dict_data_start..raw_end];
                            let finder = memchr::memmem::Finder::new(target_bytes);
                            let target_len = target_bytes.len();
                            let mut target_dict_idx: Option<u32> = None;
                            let mut search_from = 0usize;
                            while let Some(rel) = finder.find(&raw_dict[search_from..]) {
                                let abs = search_from + rel;
                                if let Ok(di) = dict_offsets.binary_search(&(abs as u32)) {
                                    let de = if di + 1 < dict_size {
                                        dict_offsets[di + 1] as usize
                                    } else {
                                        dict_data_len
                                    };
                                    if de - abs == target_len {
                                        target_dict_idx = Some((di + 1) as u32);
                                        break;
                                    }
                                }
                                search_from += rel + 1;
                                if search_from >= raw_dict.len() {
                                    break;
                                }
                            }

                            if let Some(tdi) = target_dict_idx {
                                let n = row_count.min(rg_rows);
                                if !has_deletes {
                                    for i in 0..n {
                                        if indices[i] == tdi {
                                            matches.push(rg.global_off + i);
                                        }
                                    }
                                } else if del_start + null_bitmap_len <= body.len() {
                                    let del_bytes = &body[del_start..del_start + null_bitmap_len];
                                    for i in 0..n {
                                        if (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                            continue;
                                        }
                                        if indices[i] == tdi {
                                            matches.push(rg.global_off + i);
                                        }
                                    }
                                }
                            }
                        }
                        MmapScanPred::StringEq { value, .. }
                            if desc.is_string && payload.len() >= 8 =>
                        {
                            let target_bytes = value.as_bytes();
                            let count =
                                u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                            let off_start = 8usize;
                            let data_len_off = off_start + (count + 1) * 4;
                            if data_len_off + 8 > payload.len() {
                                continue;
                            }
                            let data_start = data_len_off + 8;
                            let n = count.min(rg_rows);
                            for i in 0..n {
                                if has_deletes
                                    && del_start + null_bitmap_len <= body.len()
                                    && (body[del_start + i / 8] >> (i % 8)) & 1 == 1
                                {
                                    continue;
                                }
                                if (null_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                                    continue;
                                }
                                let s_off = off_start + i * 4;
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
                                if data_start + e <= payload.len()
                                    && e - s == target_bytes.len()
                                    && &payload[data_start + s..data_start + e] == target_bytes
                                {
                                    matches.push(rg.global_off + i);
                                }
                            }
                        }
                        MmapScanPred::StringIn { values, .. } if desc.is_dict || desc.is_string => {
                            // Delegate: scan each value and merge
                            for val in *values {
                                let sub = MmapScanPred::StringEq {
                                    col: match pred {
                                        MmapScanPred::StringIn { col, .. } => col,
                                        _ => unreachable!(),
                                    },
                                    value: val.as_str(),
                                };
                                // Inline: we can't recurse, so just note — StringIn with multiple values
                                // is handled by the caller splitting into multiple StringEq predicates.
                                // For now, skip (this path is rarely used in OR decomposition).
                                let _ = sub;
                            }
                        }
                        _ => {} // unsupported predicate/type combo: skip
                    }
                }
                matches
            })
            .collect();

        drop(mmap_guard);
        drop(file_guard);

        let mut all_indices: Vec<usize> = Vec::with_capacity(results.iter().map(|r| r.len()).sum());
        for r in results {
            all_indices.extend(r);
        }
        all_indices.sort_unstable();
        all_indices.dedup();
        Ok(Some(all_indices))
    }

    pub fn scan_like_and_extract_mmap(
        &self,
        col_name: &str,
        pattern: &str,
        limit: Option<usize>,
    ) -> io::Result<Option<arrow::record_batch::RecordBatch>> {
        use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use rayon::prelude::*;
        use std::sync::Arc;

        if !pattern.contains('%') && !pattern.contains('_') {
            return Ok(None); // exact match → caller uses string equality scanner
        }
        let like_kind = match classify_like_pattern(pattern) {
            Some(k) => k,
            None => return Ok(None),
        };

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
        if !matches!(col_type, ColumnType::String | ColumnType::StringDict) {
            return Ok(None);
        }
        let is_dict_col = matches!(col_type, ColumnType::StringDict);
        let col_count = schema.column_count();

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for LIKE scan+extract"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;

        // All RGs must be uncompressed + have RCIX
        let all_fast = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return false;
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            let compress_flag = rg_bytes.get(28).copied().unwrap_or(RG_COMPRESS_NONE);
            let enc_ver = rg_bytes.get(29).copied().unwrap_or(0);
            compress_flag == RG_COMPRESS_NONE
                && enc_ver >= 1
                && footer
                    .col_offsets
                    .get(rg_i)
                    .map_or(false, |v| v.len() >= col_count)
        });
        if !all_fast {
            return Ok(None);
        }

        // Build output Arrow schema: _id + all schema columns
        let schema_col_types: Vec<(String, ColumnType)> = schema
            .columns
            .iter()
            .map(|(n, ct)| (n.clone(), *ct))
            .collect();
        let mut out_fields: Vec<Field> = Vec::with_capacity(col_count + 1);
        out_fields.push(Field::new("_id", ArrowDataType::Int64, false));
        for (cn, ct) in &schema_col_types {
            let adt = match ct {
                ColumnType::Int64
                | ColumnType::Int8
                | ColumnType::Int16
                | ColumnType::Int32
                | ColumnType::UInt8
                | ColumnType::UInt16
                | ColumnType::UInt32
                | ColumnType::UInt64
                | ColumnType::Timestamp
                | ColumnType::Date => ArrowDataType::Int64,
                ColumnType::Float64 | ColumnType::Float32 => ArrowDataType::Float64,
                ColumnType::String | ColumnType::StringDict => ArrowDataType::Utf8,
                ColumnType::Bool => ArrowDataType::Boolean,
                _ => ArrowDataType::Utf8,
            };
            out_fields.push(Field::new(cn.as_str(), adt, true));
        }
        let out_schema: Arc<Schema> = Arc::new(Schema::new(out_fields));

        // Unsafe raw pointer for Rayon (mmap lives for the duration of this fn)
        let mmap_ptr: usize = mmap_ref.as_ptr() as usize;
        let mmap_len: usize = mmap_ref.len();

        struct RgDesc {
            rg_offset: usize,
            rg_data_size: usize,
            rg_rows: usize,
            has_deletes: bool,
            col_rcix: Vec<u32>,
            id_section_len: usize,
            min_id: u64,
        }
        let rg_descs: Vec<RgDesc> = footer
            .row_groups
            .iter()
            .enumerate()
            .map(|(rg_i, rg_meta)| RgDesc {
                rg_offset: rg_meta.offset as usize,
                rg_data_size: rg_meta.data_size as usize,
                rg_rows: rg_meta.row_count as usize,
                has_deletes: rg_meta.deletion_count > 0,
                col_rcix: footer.col_offsets[rg_i].clone(),
                id_section_len: rg_id_section_len(
                    rg_meta.row_count as usize,
                    mmap_ref
                        .get(rg_meta.offset as usize + 30)
                        .copied()
                        .unwrap_or(RG_IDS_PLAIN),
                ),
                min_id: rg_meta.min_id,
            })
            .collect();

        let like_kind_ref = &like_kind;
        let schema_types_ref = &schema_col_types;
        let out_schema_arc = out_schema.clone();

        // ── Parallel per-RG: scan LIKE col → extract all cols ────────────────
        let rg_batches: Vec<Option<arrow::record_batch::RecordBatch>> = rg_descs
            .par_iter()
            .map(|desc| {
                let mmap: &[u8] =
                    unsafe { std::slice::from_raw_parts(mmap_ptr as *const u8, mmap_len) };
                let rg_end = desc.rg_offset + desc.rg_data_size;
                if rg_end > mmap.len() || desc.rg_data_size < 32 {
                    return None;
                }
                let body = &mmap[desc.rg_offset + 32..rg_end];
                let rg_rows = desc.rg_rows;
                let bitmap_len = (rg_rows + 7) / 8;
                let del_start = desc.id_section_len;
                let del_bytes_opt: Option<&[u8]> =
                    if desc.has_deletes && del_start + bitmap_len <= body.len() {
                        Some(&body[del_start..del_start + bitmap_len])
                    } else {
                        None
                    };

                // ── Scan LIKE column ─────────────────────────────────────────────
                let lc_off = desc.col_rcix[col_idx] as usize;
                if lc_off + bitmap_len > body.len() {
                    return None;
                }
                let lc_null = &body[lc_off..lc_off + bitmap_len];
                let lc_col = &body[lc_off + bitmap_len..];
                let &lc_encoding = lc_col.first()?;
                if (!is_dict_col && lc_encoding != COL_ENCODING_PLAIN)
                    || (is_dict_col
                        && !matches!(
                            lc_encoding,
                            COL_ENCODING_PLAIN | COL_ENCODING_COMPACT_DICTIONARY
                        ))
                {
                    return None;
                }
                let lc_data = &lc_col[1..];

                let mut matched: Vec<usize> = Vec::new();

                if !is_dict_col {
                    // String PLAIN
                    if lc_data.len() < 8 {
                        return None;
                    }
                    let count = u64::from_le_bytes(lc_data[0..8].try_into().ok()?) as usize;
                    let off_end = 8 + (count + 1) * 4;
                    if off_end + 8 > lc_data.len() {
                        return None;
                    }
                    let dsl =
                        u64::from_le_bytes(lc_data[off_end..off_end + 8].try_into().ok()?) as usize;
                    let ds = off_end + 8;
                    let de = (ds + dsl).min(lc_data.len());
                    let data_region = &lc_data[ds..de];
                    let off_cow = bytes_as_u32_slice(&lc_data[8..], count + 1);
                    let offsets: &[u32] = &off_cow;
                    let n = count.min(rg_rows);
                    for i in 0..n {
                        if let Some(db) = del_bytes_opt {
                            if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                        }
                        if (lc_null[i / 8] >> (i % 8)) & 1 == 1 {
                            continue;
                        }
                        let s = offsets[i] as usize;
                        let e = offsets[i + 1] as usize;
                        if e <= data_region.len()
                            && like_matches_bytes(like_kind_ref, &data_region[s..e])
                        {
                            matched.push(i);
                        }
                    }
                } else {
                    let view = StringDictView::parse(
                        lc_data,
                        lc_encoding == COL_ENCODING_COMPACT_DICTIONARY,
                    )
                    .ok()?;
                    let mut match_flags = vec![false; view.dict_size];
                    for dict_index in 1..view.dict_size {
                        if let Some(value) = view.value(dict_index as u32) {
                            match_flags[dict_index] = like_matches_bytes(like_kind_ref, value);
                        }
                    }
                    let n = view.row_count.min(rg_rows);
                    for i in 0..n {
                        if let Some(db) = del_bytes_opt {
                            if (db[i / 8] >> (i % 8)) & 1 == 1 {
                                continue;
                            }
                        }
                        let idx = view.index(i).unwrap_or(0) as usize;
                        if idx < match_flags.len() && match_flags[idx] {
                            matched.push(i);
                        }
                    }
                }

                if matched.is_empty() {
                    return None;
                }
                let n_match = matched.len();

                // ── Extract _id + all columns for matched rows ───────────────────
                let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema_types_ref.len() + 1);

                let id_encoding = if desc.id_section_len == 0 {
                    RG_IDS_IMPLICIT_CONTIGUOUS
                } else {
                    RG_IDS_PLAIN
                };
                let id_vals: Vec<i64> = matched
                    .iter()
                    .map(|&li| {
                        rg_id_at(body, rg_rows, desc.min_id, id_encoding, li).unwrap_or(0) as i64
                    })
                    .collect();
                arrays.push(Arc::new(Int64Array::from(id_vals)) as ArrayRef);

                for ci in 0..schema_types_ref.len() {
                    let ct = schema_types_ref[ci].1;
                    let col_off = desc.col_rcix[ci] as usize;

                    macro_rules! push_null_arr {
                        ($ct:expr, $n:expr) => {
                            match $ct {
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
                                    Arc::new(Int64Array::from(vec![None::<i64>; $n])) as ArrayRef
                                }
                                ColumnType::Float64 | ColumnType::Float32 => {
                                    Arc::new(Float64Array::from(vec![None::<f64>; $n])) as ArrayRef
                                }
                                ColumnType::Bool => {
                                    Arc::new(BooleanArray::from(vec![None::<bool>; $n])) as ArrayRef
                                }
                                _ => {
                                    Arc::new(StringArray::from(vec![None::<&str>; $n])) as ArrayRef
                                }
                            }
                        };
                    }

                    if col_off + bitmap_len > body.len() {
                        arrays.push(push_null_arr!(ct, n_match));
                        continue;
                    }
                    let null_bytes = &body[col_off..col_off + bitmap_len];
                    let col_bytes = &body[col_off + bitmap_len..];
                    if col_bytes.is_empty() {
                        arrays.push(push_null_arr!(ct, n_match));
                        continue;
                    }
                    let encoding = col_bytes[0];
                    let data_bytes = if col_bytes.len() > 1 {
                        &col_bytes[1..]
                    } else {
                        &[] as &[u8]
                    };

                    let arr: ArrayRef = match (encoding, ct) {
                        // ── Plain Int64-family ───────────────────────────────────
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
                        ) if data_bytes.len() >= 8 => {
                            let vals: Vec<Option<i64>> = matched
                                .iter()
                                .map(|&li| {
                                    if (null_bytes[li / 8] >> (li % 8)) & 1 == 1 {
                                        return None;
                                    }
                                    let off = 8 + li * 8;
                                    if off + 8 <= data_bytes.len() {
                                        Some(i64::from_le_bytes(
                                            data_bytes[off..off + 8].try_into().unwrap_or([0; 8]),
                                        ))
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            Arc::new(Int64Array::from(vals))
                        }
                        // ── Plain Float64-family ─────────────────────────────────
                        (COL_ENCODING_PLAIN, ColumnType::Float64 | ColumnType::Float32)
                            if data_bytes.len() >= 8 =>
                        {
                            let vals: Vec<Option<f64>> = matched
                                .iter()
                                .map(|&li| {
                                    if (null_bytes[li / 8] >> (li % 8)) & 1 == 1 {
                                        return None;
                                    }
                                    let off = 8 + li * 8;
                                    if off + 8 <= data_bytes.len() {
                                        Some(f64::from_le_bytes(
                                            data_bytes[off..off + 8].try_into().unwrap_or([0; 8]),
                                        ))
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            Arc::new(Float64Array::from(vals))
                        }
                        (
                            COL_ENCODING_FLOAT_DICTIONARY,
                            ColumnType::Float64 | ColumnType::Float32,
                        ) => {
                            let view = FloatDictView::parse(data_bytes).ok()?;
                            let vals: Vec<Option<f64>> = matched
                                .iter()
                                .map(|&li| {
                                    if (null_bytes[li / 8] >> (li % 8)) & 1 == 1 {
                                        None
                                    } else {
                                        view.value(li)
                                    }
                                })
                                .collect();
                            Arc::new(Float64Array::from(vals))
                        }
                        // ── Plain String ─────────────────────────────────────────
                        (COL_ENCODING_PLAIN, ColumnType::String) if data_bytes.len() >= 8 => {
                            let count =
                                u64::from_le_bytes(data_bytes[0..8].try_into().unwrap_or([0; 8]))
                                    as usize;
                            let off_end = 8 + (count + 1) * 4;
                            if off_end + 8 > data_bytes.len() {
                                push_null_arr!(ct, n_match)
                            } else {
                                let dsl = u64::from_le_bytes(
                                    data_bytes[off_end..off_end + 8]
                                        .try_into()
                                        .unwrap_or([0; 8]),
                                ) as usize;
                                let dstart = off_end + 8;
                                let dend = (dstart + dsl).min(data_bytes.len());
                                let dr = &data_bytes[dstart..dend];
                                let oc = bytes_as_u32_slice(&data_bytes[8..], count + 1);
                                let offs: &[u32] = &oc;
                                let vals: Vec<Option<&str>> = matched
                                    .iter()
                                    .map(|&li| {
                                        if (null_bytes[li / 8] >> (li % 8)) & 1 == 1 {
                                            return None;
                                        }
                                        if li >= count {
                                            return None;
                                        }
                                        let s = offs[li] as usize;
                                        let e = offs[li + 1] as usize;
                                        if e <= dr.len() {
                                            std::str::from_utf8(&dr[s..e]).ok()
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();
                                Arc::new(StringArray::from(vals))
                            }
                        }
                        // ── Plain StringDict ─────────────────────────────────────
                        (COL_ENCODING_PLAIN, ColumnType::StringDict) if data_bytes.len() >= 16 => {
                            let row_count =
                                u64::from_le_bytes(data_bytes[0..8].try_into().unwrap_or([0; 8]))
                                    as usize;
                            let dict_size =
                                u64::from_le_bytes(data_bytes[8..16].try_into().unwrap_or([0; 8]))
                                    as usize;
                            if dict_size == 0 {
                                push_null_arr!(ct, n_match)
                            } else {
                                let doff_s = 16 + row_count * 4;
                                let ddl_o = doff_s + dict_size * 4;
                                if ddl_o + 8 > data_bytes.len() {
                                    push_null_arr!(ct, n_match)
                                } else {
                                    let ddl = u64::from_le_bytes(
                                        data_bytes[ddl_o..ddl_o + 8].try_into().unwrap_or([0; 8]),
                                    ) as usize;
                                    let dds = ddl_o + 8;
                                    let raw_dict =
                                        &data_bytes[dds..(dds + ddl).min(data_bytes.len())];
                                    let doff_c =
                                        bytes_as_u32_slice(&data_bytes[doff_s..], dict_size);
                                    let doffs: &[u32] = &doff_c;
                                    let idx_c = bytes_as_u32_slice(&data_bytes[16..], row_count);
                                    let idxs: &[u32] = &idx_c;
                                    let vals: Vec<Option<&str>> = matched
                                        .iter()
                                        .map(|&li| {
                                            if (null_bytes[li / 8] >> (li % 8)) & 1 == 1 {
                                                return None;
                                            }
                                            if li >= row_count {
                                                return None;
                                            }
                                            let dk = idxs[li] as usize;
                                            if dk == 0 || dk > dict_size {
                                                return None;
                                            }
                                            let di = dk - 1;
                                            let a = doffs[di] as usize;
                                            let b = if di + 1 < dict_size {
                                                doffs[di + 1] as usize
                                            } else {
                                                ddl
                                            };
                                            if a <= b && b <= raw_dict.len() {
                                                std::str::from_utf8(&raw_dict[a..b]).ok()
                                            } else {
                                                None
                                            }
                                        })
                                        .collect();
                                    Arc::new(StringArray::from(vals))
                                }
                            }
                        }
                        // ── Plain Bool ───────────────────────────────────────────
                        (COL_ENCODING_PLAIN, ColumnType::Bool) => {
                            let vals: Vec<Option<bool>> = matched
                                .iter()
                                .map(|&li| {
                                    if (null_bytes[li / 8] >> (li % 8)) & 1 == 1 {
                                        return None;
                                    }
                                    let off = 8 + li;
                                    if off < data_bytes.len() {
                                        Some(data_bytes[off] != 0)
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            Arc::new(BooleanArray::from(vals))
                        }
                        // ── Encoded (non-PLAIN): decode full column, pick rows ───
                        _ => match read_column_encoded(col_bytes, ct) {
                            Ok((col_data, _)) => {
                                let col_data = if matches!(&col_data, ColumnData::StringDict { .. })
                                {
                                    col_data.decode_string_dict()
                                } else {
                                    col_data
                                };
                                match col_data {
                                    ColumnData::Int64(v) => {
                                        let vals: Vec<Option<i64>> = matched
                                            .iter()
                                            .map(|&li| {
                                                if (null_bytes[li / 8] >> (li % 8)) & 1 == 1 {
                                                    return None;
                                                }
                                                v.get(li).copied()
                                            })
                                            .collect();
                                        Arc::new(Int64Array::from(vals))
                                    }
                                    ColumnData::Float64(v) => {
                                        let vals: Vec<Option<f64>> = matched
                                            .iter()
                                            .map(|&li| {
                                                if (null_bytes[li / 8] >> (li % 8)) & 1 == 1 {
                                                    return None;
                                                }
                                                v.get(li).copied()
                                            })
                                            .collect();
                                        Arc::new(Float64Array::from(vals))
                                    }
                                    ColumnData::String {
                                        offsets,
                                        data: str_data,
                                    } => {
                                        let cnt = offsets.len().saturating_sub(1);
                                        let vals: Vec<Option<&str>> = matched
                                            .iter()
                                            .map(|&li| {
                                                if (null_bytes[li / 8] >> (li % 8)) & 1 == 1 {
                                                    return None;
                                                }
                                                if li >= cnt {
                                                    return None;
                                                }
                                                let s = offsets[li] as usize;
                                                let e = offsets[li + 1] as usize;
                                                if e <= str_data.len() {
                                                    std::str::from_utf8(&str_data[s..e]).ok()
                                                } else {
                                                    None
                                                }
                                            })
                                            .collect();
                                        Arc::new(StringArray::from(vals))
                                    }
                                    _ => push_null_arr!(ct, n_match),
                                }
                            }
                            Err(_) => push_null_arr!(ct, n_match),
                        },
                    };
                    arrays.push(arr);
                }

                arrow::record_batch::RecordBatch::try_new(out_schema_arc.clone(), arrays).ok()
            })
            .collect();

        // ── Concatenate per-RG batches ────────────────────────────────────────
        let non_empty: Vec<&arrow::record_batch::RecordBatch> =
            rg_batches.iter().filter_map(|b| b.as_ref()).collect();

        let result = if non_empty.is_empty() {
            arrow::record_batch::RecordBatch::new_empty(out_schema)
        } else if non_empty.len() == 1 {
            non_empty[0].clone()
        } else {
            // Per-column concat (most efficient for Arrow)
            let n_fields = out_schema.fields().len();
            let mut final_arrays: Vec<ArrayRef> = Vec::with_capacity(n_fields);
            for ci in 0..n_fields {
                let cols: Vec<&dyn arrow::array::Array> =
                    non_empty.iter().map(|b| b.column(ci).as_ref()).collect();
                let arr = arrow::compute::concat(&cols)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                final_arrays.push(arr);
            }
            arrow::record_batch::RecordBatch::try_new(out_schema, final_arrays)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
        };

        // Apply limit if requested
        let result = if let Some(lim) = limit {
            let n = result.num_rows().min(lim);
            result.slice(0, n)
        } else {
            result
        };

        Ok(Some(result))
    }
}

#[cfg(test)]
mod byte_offset_tests {
    use super::{binary_search_u32_le, u32_le_at};

    #[test]
    fn unaligned_u32_offsets_support_sparse_binary_search() {
        let values = [0u32, 4, 11, 19, 31];
        let mut storage = vec![0xFF];
        for value in values {
            storage.extend_from_slice(&value.to_le_bytes());
        }
        let unaligned = &storage[1..];

        assert_eq!(binary_search_u32_le(unaligned, values.len(), 11), Some(2));
        assert_eq!(binary_search_u32_le(unaligned, values.len(), 12), None);
        assert_eq!(u32_le_at(unaligned, 3), Some(19));
        assert_eq!(u32_le_at(unaligned, values.len()), None);
    }
}
