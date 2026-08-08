// OnDemandStorage: struct definition, constructors, delta operations, compact

// ============================================================================
// On-Demand Storage Engine
// ============================================================================

const SYNC_PENDING_MAIN: u8 = 0b001;
const SYNC_PENDING_DELTA: u8 = 0b010;
const SYNC_PENDING_DELTASTORE: u8 = 0b100;

struct DeltaStringIndexCache {
    len: u64,
    modified: std::time::SystemTime,
    epoch: u64,
    index: HashMap<String, HashMap<String, Vec<u64>>>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct DeltaNumericRangeKey {
    column: String,
    low_bits: u64,
    high_bits: u64,
}

struct DeltaNumericRangeEntry {
    len: u64,
    modified: std::time::SystemTime,
    ids: Vec<u64>,
}

static DELTA_STRING_INDEX_CACHE: once_cell::sync::Lazy<RwLock<HashMap<PathBuf, DeltaStringIndexCache>>> =
    once_cell::sync::Lazy::new(|| RwLock::new(HashMap::new()));
static DELTA_NUMERIC_RANGE_CACHE: once_cell::sync::Lazy<
    RwLock<HashMap<PathBuf, HashMap<DeltaNumericRangeKey, DeltaNumericRangeEntry>>>,
> = once_cell::sync::Lazy::new(|| RwLock::new(HashMap::new()));
static DELTA_ROW_COUNT_CACHE: once_cell::sync::Lazy<
    RwLock<HashMap<PathBuf, (u64, std::time::SystemTime, usize, u64)>>,
> = once_cell::sync::Lazy::new(|| RwLock::new(HashMap::new()));

// ============================================================================
// Streaming V4 rewrite helpers (bounded-memory compaction / rewrites)
// ============================================================================

/// Parsed contents of a single V4 Row Group (uncompressed, decoded).
struct ParsedV4RowGroup {
    ids: Vec<u64>,
    deleted: Vec<u8>,
    columns: Vec<ColumnData>,
    nulls: Vec<Vec<u8>>,
}

/// Parse one V4 Row Group from its on-disk bytes (32-byte header + body).
/// Dictionary-encoded string columns are normalized back to plain `String`.
/// Peak memory per call is O(Row Group size), independent of table size.
fn parse_v4_row_group(
    rg_buf: &[u8],
    rg_meta: &RowGroupMeta,
    col_count: usize,
    schema_cols: &[(String, ColumnType)],
) -> io::Result<ParsedV4RowGroup> {
    let rg_rows = rg_meta.row_count as usize;
    if rg_rows == 0 {
        return Ok(ParsedV4RowGroup {
            ids: Vec::new(),
            deleted: Vec::new(),
            columns: (0..col_count).map(|_| ColumnData::new(ColumnType::Int64)).collect(),
            nulls: vec![Vec::new(); col_count],
        });
    }
    let compress_flag = if rg_buf.len() >= 32 {
        rg_buf[28]
    } else {
        RG_COMPRESS_NONE
    };
    let encoding_version = if rg_buf.len() >= 32 {
        rg_buf[29]
    } else {
        0
    };
    let id_encoding = rg_buf.get(30).copied().unwrap_or(RG_IDS_PLAIN);

    let decompressed = decompress_rg_body(compress_flag, &rg_buf[32..])?;
    let body: &[u8] = decompressed.as_deref().unwrap_or(&rg_buf[32..]);
    let mut pos = 0usize;

    // IDs (plain array or implicit contiguous range).
    let id_byte_len = rg_id_section_len(rg_rows, id_encoding);
    let mut ids = Vec::with_capacity(rg_rows);
    if id_encoding == RG_IDS_IMPLICIT_CONTIGUOUS {
        ids.extend((0..rg_rows as u64).map(|o| rg_meta.min_id + o));
    } else {
        if id_byte_len > body.len() {
            return Err(err_data("RG ID section truncated"));
        }
        ids.resize(rg_rows, 0);
        unsafe {
            std::ptr::copy_nonoverlapping(body.as_ptr(), ids.as_mut_ptr() as *mut u8, id_byte_len);
        }
    }
    pos += id_byte_len;

    // Deletion vector.
    let del_vec_len = (rg_rows + 7) / 8;
    if pos + del_vec_len > body.len() {
        return Err(err_data("RG deletion vector truncated"));
    }
    let deleted = body[pos..pos + del_vec_len].to_vec();
    pos += del_vec_len;

    // Columns: null bitmap + column data per schema column.
    let null_bitmap_len = (rg_rows + 7) / 8;
    let mut columns = Vec::with_capacity(col_count);
    let mut nulls = Vec::with_capacity(col_count);
    for col_idx in 0..col_count {
        if pos + null_bitmap_len > body.len() {
            // Schema evolution: this Row Group predates the trailing column(s)
            // (footer-only ALTER TABLE ADD COLUMN). Pad with all-NULL values.
            for missing_idx in col_idx..col_count {
                let ct = schema_cols
                    .get(missing_idx)
                    .map(|(_, ct)| *ct)
                    .unwrap_or(ColumnType::Null);
                let mem_ct = if matches!(ct, ColumnType::StringDict) {
                    ColumnType::String
                } else {
                    ct
                };
                columns.push(ColumnData::new(mem_ct));
                let mut pad_nulls = vec![0u8; null_bitmap_len];
                for i in 0..rg_rows {
                    pad_nulls[i / 8] |= 1 << (i % 8);
                }
                nulls.push(pad_nulls);
            }
            break;
        }
        let col_nulls = body[pos..pos + null_bitmap_len].to_vec();
        pos += null_bitmap_len;
        let col_type = schema_cols
            .get(col_idx)
            .map(|(_, ct)| *ct)
            .unwrap_or(ColumnType::Null);
        let (mut col, consumed) = if encoding_version >= 1 {
            read_column_encoded(&body[pos..], col_type)?
        } else {
            ColumnData::from_bytes_typed(&body[pos..], col_type)?
        };
        pos += consumed;
        if matches!(col, ColumnData::StringDict { .. }) {
            col = col.decode_string_dict();
        }
        columns.push(col);
        nulls.push(col_nulls);
    }

    Ok(ParsedV4RowGroup {
        ids,
        deleted,
        columns,
        nulls,
    })
}

/// Push a default (non-null) value for a column that has no source data.
#[inline]
fn push_default_value(col: &mut ColumnData) {
    match col {
        ColumnData::Int64(v) => v.push(0),
        ColumnData::Float64(v) => v.push(0.0),
        ColumnData::String { offsets, .. } => {
            let last = offsets.last().copied().unwrap_or(0);
            offsets.push(last);
        }
        ColumnData::Binary { offsets, .. } => {
            let last = offsets.last().copied().unwrap_or(0);
            offsets.push(last);
        }
        ColumnData::Bool { data, len } => {
            let byte_idx = *len / 8;
            if byte_idx >= data.len() {
                data.push(0);
            }
            *len += 1;
        }
        ColumnData::StringDict { indices, .. } => indices.push(0),
        ColumnData::FixedList { data, dim } => {
            if *dim > 0 {
                data.extend(std::iter::repeat(0u8).take(*dim as usize * 4));
            }
        }
        ColumnData::Float16List { data, dim } => {
            if *dim > 0 {
                data.extend(std::iter::repeat(0u8).take(*dim as usize * 2));
            }
        }
    }
}

/// Push a NULL row value (default value + null bit set by the caller).
#[inline]
fn push_null_value(col: &mut ColumnData) {
    push_default_value(col);
}

/// Read a single bool from a packed Bool column.
#[inline]
fn src_bool_at(col: &ColumnData, row: usize) -> bool {
    match col {
        ColumnData::Bool { data, .. } => (data[row / 8] >> (row % 8)) & 1 == 1,
        _ => false,
    }
}

/// Append one row value from a source column into a destination column.
#[inline]
fn push_source_value(dst: &mut ColumnData, src: &ColumnData, row: usize, is_null: bool) {
    if is_null {
        push_null_value(dst);
        return;
    }
    match (&mut *dst, src) {
        (ColumnData::Int64(v), ColumnData::Int64(s)) => v.push(s[row]),
        (ColumnData::Float64(v), ColumnData::Float64(s)) => v.push(s[row]),
        (
            ColumnData::String { offsets, data },
            ColumnData::String {
                offsets: so,
                data: sd,
            },
        ) => {
            if row + 1 >= so.len() {
                push_null_value(dst);
                return;
            }
            let start = so[row] as usize;
            let end = so[row + 1] as usize;
            if start <= end && end <= sd.len() {
                data.extend_from_slice(&sd[start..end]);
            }
            offsets.push(data.len() as u64);
        }
        (
            ColumnData::Binary { offsets, data },
            ColumnData::Binary {
                offsets: so,
                data: sd,
            },
        ) => {
            if row + 1 >= so.len() {
                push_null_value(dst);
                return;
            }
            let start = so[row] as usize;
            let end = so[row + 1] as usize;
            if start <= end && end <= sd.len() {
                data.extend_from_slice(&sd[start..end]);
            }
            offsets.push(data.len() as u64);
        }
        (ColumnData::Bool { data, len }, s) => {
            let byte_idx = *len / 8;
            let bit_idx = *len % 8;
            if byte_idx >= data.len() {
                data.push(0);
            }
            if src_bool_at(s, row) {
                data[byte_idx] |= 1 << bit_idx;
            }
            *len += 1;
        }
        (ColumnData::FixedList { data, dim }, ColumnData::FixedList { data: sd, dim: sdim }) => {
            if *dim == 0 {
                *dim = *sdim;
            }
            if *dim > 0 {
                let start = row * (*dim as usize * 4);
                if start + *dim as usize * 4 <= sd.len() {
                    data.extend_from_slice(&sd[start..start + *dim as usize * 4]);
                } else {
                    push_null_value(dst);
                }
            }
        }
        (
            ColumnData::Float16List { data, dim },
            ColumnData::Float16List { data: sd, dim: sdim },
        ) => {
            if *dim == 0 {
                *dim = *sdim;
            }
            if *dim > 0 {
                let start = row * (*dim as usize * 2);
                if start + *dim as usize * 2 <= sd.len() {
                    data.extend_from_slice(&sd[start..start + *dim as usize * 2]);
                } else {
                    push_null_value(dst);
                }
            }
        }
        _ => push_default_value(dst),
    }
}

/// Apply a DeltaStore update value to a destination column. Returns `Some(())`
/// when the value type matches the column type and was applied, `None` when the
/// update is ignored (same parity as the legacy in-memory compaction path).
#[inline]
fn try_push_updated_value(dst: &mut ColumnData, value: &crate::data::Value) -> Option<()> {
    match (dst, value) {
        (ColumnData::Int64(v), crate::data::Value::Int64(x)) => {
            v.push(*x);
            Some(())
        }
        (ColumnData::Float64(v), crate::data::Value::Float64(x)) => {
            v.push(*x);
            Some(())
        }
        (ColumnData::String { offsets, data }, crate::data::Value::String(s)) => {
            data.extend_from_slice(s.as_bytes());
            offsets.push(data.len() as u64);
            Some(())
        }
        (ColumnData::Bool { data, len }, crate::data::Value::Bool(b)) => {
            let byte_idx = *len / 8;
            let bit_idx = *len % 8;
            if byte_idx >= data.len() {
                data.push(0);
            }
            if *b {
                data[byte_idx] |= 1 << bit_idx;
            }
            *len += 1;
            Some(())
        }
        _ => None,
    }
}

/// Set or clear the NULL bit for a row in a packed null bitmap.
#[inline]
fn set_null_bit(bitmap: &mut Vec<u8>, row: usize, is_null: bool) {
    let byte_idx = row / 8;
    let bit_idx = row % 8;
    if byte_idx >= bitmap.len() {
        bitmap.resize(byte_idx + 1, 0);
    }
    if is_null {
        bitmap[byte_idx] |= 1 << bit_idx;
    } else {
        bitmap[byte_idx] &= !(1 << bit_idx);
    }
}

/// Flush the current output Row Group buffer to the streaming writer and reset
/// the buffer. No-op when the buffer is empty.
#[allow(clippy::too_many_arguments)]
fn flush_streamed_out_rg<W: std::io::Write + std::io::Seek>(
    writer: &mut W,
    schema_cols: &[(String, ColumnType)],
    ids: &mut Vec<u64>,
    columns: &mut Vec<ColumnData>,
    nulls: &mut Vec<Vec<u8>>,
    first_rg: &mut bool,
    actual_col_types: &mut Vec<ColumnType>,
    rg_metas: &mut Vec<RowGroupMeta>,
    all_zone_maps: &mut RgZoneMaps,
    all_rg_col_offsets: &mut Vec<Vec<u32>>,
    compression: CompressionType,
    implicit_ids_ok: bool,
    written_rows: &mut u64,
) -> io::Result<()> {
    let n = ids.len();
    if n == 0 {
        return Ok(());
    }
    OnDemandStorage::write_streamed_v4_row_group(
        writer,
        schema_cols,
        ids,
        columns,
        nulls,
        0,
        n,
        *first_rg,
        actual_col_types,
        rg_metas,
        all_zone_maps,
        all_rg_col_offsets,
        compression,
        implicit_ids_ok,
    )?;
    *first_rg = false;
    *written_rows += n as u64;
    ids.clear();
    for c in columns.iter_mut() {
        *c = ColumnData::new(c.column_type());
    }
    for nn in nulls.iter_mut() {
        nn.clear();
    }
    Ok(())
}

/// High-performance on-demand columnar storage
///
/// Key features:
/// - Read only required columns (column projection)
/// - Read only required row ranges  
/// - Uses mmap for zero-copy reads with OS page cache (cross-platform)
/// - Soft delete with deleted bitmap
/// - Update via delete + insert
pub struct OnDemandStorage {
    path: PathBuf,
    file: RwLock<Option<File>>,
    write_file: RwLock<Option<File>>,
    delta_file: RwLock<Option<File>>,
    /// Memory-mapped file cache for fast repeated reads
    mmap_cache: RwLock<MmapCache>,
    header: RwLock<OnDemandHeader>,
    schema: RwLock<OnDemandSchema>,
    column_index: RwLock<Vec<ColumnIndexEntry>>,
    /// In-memory column data (legacy: used as write buffer for pending inserts)
    columns: RwLock<Vec<ColumnData>>,
    /// Row IDs (legacy: used as write buffer for pending inserts)
    ids: RwLock<Vec<u64>>,
    /// Next row ID
    next_id: AtomicU64,
    /// Null bitmaps per column (legacy: used as write buffer for pending inserts)
    nulls: RwLock<Vec<Vec<u8>>>,
    /// Deleted row bitmap (packed bits, 1 = deleted)
    deleted: RwLock<Vec<u8>>,
    /// ID to row index mapping for fast lookups (lazy-loaded)
    /// Only built when needed for delete/exists operations
    /// Uses AHashMap for faster hash computation on u64 keys
    id_to_idx: RwLock<Option<ahash::AHashMap<u64, usize>>>,
    /// Cached count of active (non-deleted) rows for O(1) COUNT(*)
    active_count: AtomicU64,
    /// Durability level for controlling fsync behavior
    durability: super::DurabilityLevel,
    /// WAL writer for safe/max durability modes (None for fast mode)
    wal_writer: RwLock<Option<super::incremental::WalWriter>>,
    /// WAL buffer for pending writes (used for recovery)
    wal_buffer: RwLock<Vec<super::incremental::WalRecord>>,
    /// Auto-flush threshold: number of pending rows (0 = disabled)
    auto_flush_rows: AtomicU64,
    /// Auto-flush threshold: estimated memory bytes (0 = disabled)
    auto_flush_bytes: AtomicU64,
    /// Count of rows inserted since last save (for auto-flush)
    pending_rows: AtomicU64,
    /// Total rows physically on disk (including deleted). Only updated after disk writes.
    /// Used by save() to distinguish in-memory-only rows from persisted rows.
    persisted_row_count: AtomicU64,
    /// Whether V4 base data was bulk-loaded into memory (only in tests via open_v4_data).
    /// Production code never sets this — in-memory data is always just the write buffer.
    v4_base_loaded: AtomicBool,
    /// Lock-free cache of header.footer_offset for V4 detection on the read path.
    /// Avoids acquiring header RwLock on every to_arrow_batch / read call.
    /// Updated atomically whenever header.footer_offset changes (save_v4, open, append_row_group).
    cached_footer_offset: AtomicU64,
    /// Cached V4 footer with Row Group metadata (lazy-loaded from disk).
    /// Enables on-demand mmap reads without loading all data into memory.
    v4_footer: RwLock<Option<V4Footer>>,
    /// Delta store for cell-level update tracking (Phase 4.5).
    /// Tracks pending UPDATE changes without rewriting the base file.
    /// On read, DeltaMerger overlays these changes on top of base data.
    delta_store: RwLock<DeltaStore>,
    /// Bitmask of files that were written directly to disk and still need fsync.
    sync_pending: AtomicU8,
    /// Row Group body compression algorithm. Default: None (no compression).
    /// Persisted in header flags bits 0-1. Can only be set on empty tables.
    compression: std::sync::atomic::AtomicU8,
    /// User-space page cache for retrieve_rcix point lookups.
    /// Caches 4KB file pages as heap memory to avoid mmap page-fault overhead on macOS.
    /// On-demand: only pages actually accessed are cached (~13 pages = ~52KB per backend).
    /// Invalidated after every write (save_v4).
    pub(crate) page_cache: RwLock<HashMap<u64, Box<[u8; 4096]>>>,
    /// Reusable scratch buffer for vector TopK scans.
    /// Pre-allocated on first use; grown as needed; reused to avoid per-query
    /// 512MB allocation + soft-page-fault overhead on the destination pages.
    /// Never shrinks — sized to the largest scan seen so far.
    pub(crate) scan_buf: std::sync::Mutex<Vec<f32>>,
    /// File size when scan_buf was last populated; 0 = cache invalid.
    /// Used to skip re-copying vector data when file hasn't changed.
    pub(crate) scan_buf_file_size: std::sync::atomic::AtomicU64,
    /// Column name whose data is currently in scan_buf (empty = none).
    pub(crate) scan_buf_col: std::sync::Mutex<String>,
    /// Raw f16 byte cache for Float16List TopK scans.
    /// Stores n_rows × dim × 2 raw LE f16 bytes; f32 decode happens per-row
    /// during distance computation — halves memory vs a decoded f32 scan_buf.
    pub(crate) scan_buf_f16: std::sync::Mutex<Vec<u8>>,
    /// File size when scan_buf_f16 was last populated; 0 = cache invalid.
    pub(crate) scan_buf_f16_file_size: std::sync::atomic::AtomicU64,
    /// Column name whose f16 data is currently in scan_buf_f16 (empty = none).
    pub(crate) scan_buf_f16_col: std::sync::Mutex<String>,
    /// Global lock for thread-safe concurrent access to file and mmap.
    /// This prevents "File not open" and "V4 footer: schema overflow" errors
    /// when multiple threads access the storage simultaneously.
    pub(crate) global_lock: parking_lot::RwLock<()>,
}

impl OnDemandStorage {
    fn validate_v4_layout(
        header: &OnDemandHeader,
        footer: &V4Footer,
        file_len: u64,
    ) -> io::Result<usize> {
        if header.footer_offset < HEADER_SIZE as u64 || header.footer_offset > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Corrupt Apex file: footer offset {} is outside file length {}",
                    header.footer_offset, file_len
                ),
            ));
        }
        if header.column_count as usize != footer.schema.column_count() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Corrupt Apex file: header/footer column counts differ",
            ));
        }
        let footer_rows = footer.row_groups.iter().try_fold(0u64, |total, group| {
            if group.deletion_count > group.row_count {
                return None;
            }
            total.checked_add((group.row_count - group.deletion_count) as u64)
        });
        if footer_rows != Some(header.row_count) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Corrupt Apex file: header/footer row counts differ",
            ));
        }
        for group in &footer.row_groups {
            let end = group.offset.checked_add(group.data_size).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Corrupt Apex row-group bounds")
            })?;
            if group.offset < HEADER_SIZE as u64 || end > header.footer_offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Corrupt Apex file: row group lies outside the data region",
                ));
            }
        }
        usize::try_from(header.row_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Corrupt Apex file: row count exceeds platform capacity",
            )
        })
    }

    /// Create a new storage file with default durability (Fast)
    pub fn create(path: &Path) -> io::Result<Self> {
        Self::create_with_durability(path, super::DurabilityLevel::Fast)
    }

    /// Create a new storage file with specified durability level
    pub fn create_with_durability(
        path: &Path,
        durability: super::DurabilityLevel,
    ) -> io::Result<Self> {
        Self::create_with_schema_and_durability(path, durability, &[])
    }

    /// Create a new storage file with pre-defined schema and durability level.
    /// Pre-defining schema avoids schema inference on the first insert, providing
    /// a performance benefit: columns and null vectors are pre-allocated with
    /// correct types so insert_typed() hits the fast path immediately.
    pub fn create_with_schema_and_durability(
        path: &Path,
        durability: super::DurabilityLevel,
        schema_cols: &[(String, ColumnType)],
    ) -> io::Result<Self> {
        let header = OnDemandHeader::new();
        let mut schema = OnDemandSchema::new();
        let mut columns = Vec::with_capacity(schema_cols.len());
        let mut nulls = Vec::with_capacity(schema_cols.len());

        // Pre-populate schema and empty column vectors
        for (name, dtype) in schema_cols {
            schema.add_column(name, *dtype);
            columns.push(ColumnData::new(*dtype));
            nulls.push(Vec::new());
        }

        // Initialize WAL for safe/max durability modes
        let wal_writer = if durability != super::DurabilityLevel::Fast {
            let wal_path = Self::wal_path(path);
            Some(super::incremental::WalWriter::create(
                &wal_path,
                crate::storage::FIRST_ROW_ID,
            )?)
        } else {
            None
        };

        let storage = Self {
            path: path.to_path_buf(),
            file: RwLock::new(None),
            write_file: RwLock::new(None),
            delta_file: RwLock::new(None),
            mmap_cache: RwLock::new(MmapCache::new()),
            header: RwLock::new(header),
            schema: RwLock::new(schema),
            column_index: RwLock::new(Vec::new()),
            columns: RwLock::new(columns),
            ids: RwLock::new(Vec::new()),
            next_id: AtomicU64::new(crate::storage::FIRST_ROW_ID),
            nulls: RwLock::new(nulls),
            deleted: RwLock::new(Vec::new()),
            id_to_idx: RwLock::new(Some(ahash::AHashMap::new())),
            active_count: AtomicU64::new(0),
            durability,
            wal_writer: RwLock::new(wal_writer),
            wal_buffer: RwLock::new(Vec::new()),
            auto_flush_rows: AtomicU64::new(100000),
            auto_flush_bytes: AtomicU64::new(500 * 1024 * 1024),
            pending_rows: AtomicU64::new(0),
            persisted_row_count: AtomicU64::new(0),
            v4_base_loaded: AtomicBool::new(false),
            cached_footer_offset: AtomicU64::new(0),
            v4_footer: RwLock::new(None),
            delta_store: RwLock::new(DeltaStore::new(path)),
            sync_pending: AtomicU8::new(0),
            compression: std::sync::atomic::AtomicU8::new(CompressionType::None as u8),
            page_cache: RwLock::new(HashMap::new()),
            scan_buf: std::sync::Mutex::new(Vec::new()),
            scan_buf_file_size: std::sync::atomic::AtomicU64::new(0),
            scan_buf_col: std::sync::Mutex::new(String::new()),
            scan_buf_f16: std::sync::Mutex::new(Vec::new()),
            scan_buf_f16_file_size: std::sync::atomic::AtomicU64::new(0),
            scan_buf_f16_col: std::sync::Mutex::new(String::new()),
            global_lock: parking_lot::RwLock::new(()),
        };

        // Write initial file (single-shot empty-file writer).
        storage.save_initial_file()?;

        Ok(storage)
    }

    /// Get WAL file path for a given data file path
    fn wal_path(main_path: &Path) -> PathBuf {
        let mut wal_path = main_path.to_path_buf();
        let ext = wal_path
            .extension()
            .map(|e| format!("{}.wal", e.to_string_lossy()))
            .unwrap_or_else(|| "wal".to_string());
        wal_path.set_extension(ext);
        wal_path
    }

    /// Open existing storage with default durability (Fast)
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_with_durability(path, super::DurabilityLevel::Fast)
    }

    /// Open existing storage with specified durability level
    /// Uses mmap for fast zero-copy reads with OS page cache
    pub fn open_with_durability(
        path: &Path,
        durability: super::DurabilityLevel,
    ) -> io::Result<Self> {
        // Clean up stale .tmp files from crashed atomic writes
        let tmp_path = path.with_extension("apex.tmp");
        if tmp_path.exists() {
            let _ = std::fs::remove_file(&tmp_path);
        }
        // Clean up stale .deltastore.tmp from crashed DeltaStore save
        let ds_tmp = {
            let mut p = path.to_path_buf();
            let name = p
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            p.set_file_name(format!("{}.deltastore.tmp", name));
            p
        };
        if ds_tmp.exists() {
            let _ = std::fs::remove_file(&ds_tmp);
        }
        // Apply any deferred delete state before reading the file
        let _ = apply_pending_deletes(path);

        let file = open_for_sequential_read(path)?;

        // Create mmap cache and use it for initial reads
        let mut mmap_cache = MmapCache::new();

        // Read header using mmap (zero-copy)
        let mut header_bytes = [0u8; HEADER_SIZE];
        mmap_cache.read_at(&file, &mut header_bytes, 0)?;
        let header = OnDemandHeader::from_bytes(&header_bytes)?;

        if header.footer_offset == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unsupported legacy file format (V3). Please re-create the table.",
            ));
        }

        // V4 Row Group format: read schema from footer
        let file_len = file.metadata()?.len();
        if header.footer_offset < HEADER_SIZE as u64 || header.footer_offset > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Corrupt Apex file: invalid footer offset",
            ));
        }
        let footer_byte_count = usize::try_from(file_len - header.footer_offset).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "Corrupt Apex file: footer is too large")
        })?;
        let mut footer_bytes = vec![0u8; footer_byte_count];
        mmap_cache.read_at(&file, &mut footer_bytes, header.footer_offset)?;
        let footer = V4Footer::from_bytes(&footer_bytes)?;
        let id_count = Self::validate_v4_layout(&header, &footer, file_len)?;
        let schema = footer.schema.clone();
        let column_index: Vec<ColumnIndexEntry> = Vec::new();
        // Use max_id from non-empty RG metadata (row_count may be < max _id after deletes)
        let next_id = footer
            .row_groups
            .iter()
            .filter(|rg| rg.row_count > 0)
            .map(|rg| rg.max_id)
            .max()
            .map(|m| m + 1)
            .unwrap_or(crate::storage::FIRST_ROW_ID);
        let cached_v4_footer: Option<V4Footer> = Some(footer);

        let columns: Vec<ColumnData> = schema
            .columns
            .iter()
            .map(|(_, col_type)| ColumnData::new(*col_type))
            .collect();
        let nulls = vec![Vec::new(); header.column_count as usize];
        let deleted_len = (id_count + 7) / 8;
        let deleted = vec![0u8; deleted_len];

        // Handle WAL recovery and initialization for safe/max durability
        let wal_path = Self::wal_path(path);
        let (wal_writer, wal_buffer, recovered_next_id) =
            if durability != super::DurabilityLevel::Fast {
                if wal_path.exists() {
                    // Replay WAL for crash recovery
                    let mut reader = super::incremental::WalReader::open(&wal_path)?;
                    let all_records = reader.read_all()?;

                    // P0-3: Collect committed txn_ids for recovery filtering
                    let committed_txns: std::collections::HashSet<u64> = all_records
                        .iter()
                        .filter_map(|r| match r {
                            super::incremental::WalRecord::TxnCommit { txn_id } => Some(*txn_id),
                            _ => None,
                        })
                        .collect();

                    // Filter: keep only auto-commit (txn_id=0) and committed txn DML records
                    // ALSO: idempotency guard — skip Insert/BatchInsert records whose IDs
                    // are already in the base file (id < next_id). This prevents duplicate
                    // rows if WAL is replayed after the base file was already saved.
                    let base_next_id = next_id; // next_id from base file before WAL recovery
                    let records: Vec<_> = all_records
                        .into_iter()
                        .filter(|r| {
                            match r {
                                super::incremental::WalRecord::Insert { txn_id, id, .. } => {
                                    (*txn_id == 0 || committed_txns.contains(txn_id))
                                        && *id >= base_next_id // Skip if already persisted
                                }
                                super::incremental::WalRecord::BatchInsert {
                                    txn_id,
                                    start_id,
                                    rows,
                                    ..
                                } => {
                                    let end_id = *start_id + rows.len() as u64;
                                    (*txn_id == 0 || committed_txns.contains(txn_id))
                                        && end_id > base_next_id // Keep if any rows are new
                                }
                                super::incremental::WalRecord::Delete { txn_id, id, .. } => {
                                    (*txn_id == 0 || committed_txns.contains(txn_id))
                                        && *id < base_next_id // Only delete rows that exist in base
                                }
                                _ => true, // Keep checkpoints, txn boundaries
                            }
                        })
                        .collect();

                    // Find max ID from WAL records (handles both Insert and BatchInsert)
                    let max_wal_id = records
                        .iter()
                        .filter_map(|r| match r {
                            super::incremental::WalRecord::Insert { id, .. } => Some(*id),
                            super::incremental::WalRecord::BatchInsert {
                                start_id, rows, ..
                            } => Some(*start_id + rows.len() as u64 - 1),
                            _ => None,
                        })
                        .max();

                    let recovered_id = max_wal_id.map(|id| id + 1).unwrap_or(next_id);

                    // Open for append
                    let writer = super::incremental::WalWriter::open(&wal_path)?;
                    (Some(writer), records, recovered_id)
                } else {
                    // Create new WAL
                    let writer = super::incremental::WalWriter::create(&wal_path, next_id)?;
                    (Some(writer), Vec::new(), next_id)
                }
            } else {
                (None, Vec::new(), next_id)
            };

        let delta_next_id = {
            let delta_path = Self::delta_path(path);
            if delta_path.exists() {
                Self::get_max_id_from_delta_fast(&delta_path)
                    .ok()
                    .map(|id| id.saturating_add(1))
                    .unwrap_or(next_id)
            } else {
                next_id
            }
        };
        let final_next_id = recovered_next_id.max(next_id).max(delta_next_id);
        let cached_fo = header.footer_offset;

        // Read compression type from header flags
        let comp_type = CompressionType::from_flags(header.flags);

        Ok(Self {
            path: path.to_path_buf(),
            file: RwLock::new(Some(file)),
            write_file: RwLock::new(None),
            delta_file: RwLock::new(None),
            mmap_cache: RwLock::new(mmap_cache),
            header: RwLock::new(header),
            schema: RwLock::new(schema),
            column_index: RwLock::new(column_index),
            columns: RwLock::new(columns),
            ids: RwLock::new(Vec::new()), // Empty - lazy loaded when needed
            next_id: AtomicU64::new(final_next_id),
            nulls: RwLock::new(nulls),
            deleted: RwLock::new(deleted),
            id_to_idx: RwLock::new(None), // Lazy loaded when needed
            active_count: AtomicU64::new(if let Some(ref f) = cached_v4_footer {
                // Footer already loaded: derive active count from per-RG metadata.
                // Allows DELETE to skip header pwrite while fresh backends still get
                // the correct count (footer.deletion_count is always kept in sync).
                f.row_groups
                    .iter()
                    .map(|rg| (rg.row_count as u64).saturating_sub(rg.deletion_count as u64))
                    .sum::<u64>()
            } else {
                id_count as u64
            }),
            durability,
            wal_writer: RwLock::new(wal_writer),
            wal_buffer: RwLock::new(wal_buffer),
            auto_flush_rows: AtomicU64::new(10000),
            auto_flush_bytes: AtomicU64::new(500 * 1024 * 1024),
            pending_rows: AtomicU64::new(0),
            persisted_row_count: AtomicU64::new(id_count as u64),
            v4_base_loaded: AtomicBool::new(false),
            cached_footer_offset: AtomicU64::new(cached_fo),
            v4_footer: RwLock::new(cached_v4_footer),
            delta_store: RwLock::new(
                DeltaStore::load(path).unwrap_or_else(|_| DeltaStore::new(path)),
            ),
            sync_pending: AtomicU8::new(0),
            compression: std::sync::atomic::AtomicU8::new(comp_type as u8),
            page_cache: RwLock::new(HashMap::new()),
            scan_buf: std::sync::Mutex::new(Vec::new()),
            scan_buf_file_size: std::sync::atomic::AtomicU64::new(0),
            scan_buf_col: std::sync::Mutex::new(String::new()),
            scan_buf_f16: std::sync::Mutex::new(Vec::new()),
            scan_buf_f16_file_size: std::sync::atomic::AtomicU64::new(0),
            scan_buf_f16_col: std::sync::Mutex::new(String::new()),
            global_lock: parking_lot::RwLock::new(()),
        })
    }

    /// Open for reading only, reusing a pre-opened File and known file_len.
    /// Skips DeltaStore::load (saves 1 stat syscall) and internal File::open (saves 1 open syscall).
    /// For pure read paths only — DeltaStore is initialized empty (no pending updates).
    pub fn open_for_read_with_file(path: &Path, file: File, file_len: u64) -> io::Result<Self> {
        // Apply pending delete state before creating the mmap so reads see fresh data
        let _ = apply_pending_deletes(path);
        let mut mmap_cache = MmapCache::new();

        let mut header_bytes = [0u8; HEADER_SIZE];
        mmap_cache.read_at(&file, &mut header_bytes, 0)?;
        let header = OnDemandHeader::from_bytes(&header_bytes)?;

        if header.footer_offset == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unsupported legacy file format (V3). Please re-create the table.",
            ));
        }

        if header.footer_offset < HEADER_SIZE as u64 || header.footer_offset > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Corrupt Apex file: invalid footer offset",
            ));
        }
        let footer_byte_count = usize::try_from(file_len - header.footer_offset).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "Corrupt Apex file: footer is too large")
        })?;
        let mut footer_bytes = vec![0u8; footer_byte_count];
        mmap_cache.read_at(&file, &mut footer_bytes, header.footer_offset)?;
        let footer = V4Footer::from_bytes(&footer_bytes)?;
        let id_count = Self::validate_v4_layout(&header, &footer, file_len)?;
        let schema = footer.schema.clone();
        let column_index: Vec<ColumnIndexEntry> = Vec::new();
        let next_id = footer
            .row_groups
            .iter()
            .filter(|rg| rg.row_count > 0)
            .map(|rg| rg.max_id)
            .max()
            .map(|m| m + 1)
            .unwrap_or(crate::storage::FIRST_ROW_ID);
        let cached_v4_footer: Option<V4Footer> = Some(footer);

        let columns: Vec<ColumnData> = schema
            .columns
            .iter()
            .map(|(_, col_type)| ColumnData::new(*col_type))
            .collect();
        let nulls = vec![Vec::new(); header.column_count as usize];
        let deleted_len = (id_count + 7) / 8;
        let deleted = vec![0u8; deleted_len];
        let cached_fo = header.footer_offset;
        let comp_type = CompressionType::from_flags(header.flags);

        Ok(Self {
            path: path.to_path_buf(),
            file: RwLock::new(Some(file)),
            write_file: RwLock::new(None),
            delta_file: RwLock::new(None),
            mmap_cache: RwLock::new(mmap_cache),
            header: RwLock::new(header),
            schema: RwLock::new(schema),
            column_index: RwLock::new(column_index),
            columns: RwLock::new(columns),
            ids: RwLock::new(Vec::new()),
            next_id: AtomicU64::new(next_id),
            nulls: RwLock::new(nulls),
            deleted: RwLock::new(deleted),
            id_to_idx: RwLock::new(None),
            active_count: AtomicU64::new(if let Some(ref f) = cached_v4_footer {
                f.row_groups
                    .iter()
                    .map(|rg| (rg.row_count as u64).saturating_sub(rg.deletion_count as u64))
                    .sum::<u64>()
            } else {
                id_count as u64
            }),
            durability: super::DurabilityLevel::Fast,
            wal_writer: RwLock::new(None),
            wal_buffer: RwLock::new(Vec::new()),
            auto_flush_rows: AtomicU64::new(10000),
            auto_flush_bytes: AtomicU64::new(500 * 1024 * 1024),
            pending_rows: AtomicU64::new(0),
            persisted_row_count: AtomicU64::new(id_count as u64),
            v4_base_loaded: AtomicBool::new(false),
            cached_footer_offset: AtomicU64::new(cached_fo),
            v4_footer: RwLock::new(cached_v4_footer),
            delta_store: RwLock::new(
                DeltaStore::load(path).unwrap_or_else(|_| DeltaStore::new(path)),
            ),
            sync_pending: AtomicU8::new(0),
            compression: std::sync::atomic::AtomicU8::new(comp_type as u8),
            page_cache: RwLock::new(HashMap::new()),
            scan_buf: std::sync::Mutex::new(Vec::new()),
            scan_buf_file_size: std::sync::atomic::AtomicU64::new(0),
            scan_buf_col: std::sync::Mutex::new(String::new()),
            scan_buf_f16: std::sync::Mutex::new(Vec::new()),
            scan_buf_f16_file_size: std::sync::atomic::AtomicU64::new(0),
            scan_buf_f16_col: std::sync::Mutex::new(String::new()),
            global_lock: parking_lot::RwLock::new(()),
        })
    }

    /// Set auto-flush thresholds for automatic persistence
    /// * `rows` - Auto-flush when pending rows exceed this count (0 = disabled)
    /// * `bytes` - Auto-flush when estimated memory exceeds this size (0 = disabled)
    pub fn set_auto_flush(&self, rows: u64, bytes: u64) {
        self.auto_flush_rows.store(rows, Ordering::SeqCst);
        self.auto_flush_bytes.store(bytes, Ordering::SeqCst);
    }

    /// Get current auto-flush configuration
    pub fn get_auto_flush(&self) -> (u64, u64) {
        (
            self.auto_flush_rows.load(Ordering::SeqCst),
            self.auto_flush_bytes.load(Ordering::SeqCst),
        )
    }

    #[inline]
    pub fn mark_sync_pending(&self) {
        self.mark_main_sync_pending();
    }

    #[inline]
    pub fn mark_main_sync_pending(&self) {
        self.sync_pending
            .fetch_or(SYNC_PENDING_MAIN, Ordering::SeqCst);
    }

    #[inline]
    pub fn mark_delta_sync_pending(&self) {
        self.sync_pending
            .fetch_or(SYNC_PENDING_DELTA, Ordering::SeqCst);
    }

    #[inline]
    pub fn mark_deltastore_sync_pending(&self) {
        self.sync_pending
            .fetch_or(SYNC_PENDING_DELTASTORE, Ordering::SeqCst);
    }

    #[inline]
    pub fn sync_pending(&self) -> bool {
        self.sync_pending_bits() != 0
    }

    #[inline]
    pub fn footer_offset_hint(&self) -> u64 {
        self.cached_footer_offset.load(Ordering::Acquire)
    }

    #[inline]
    pub fn sync_pending_bits(&self) -> u8 {
        self.sync_pending.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn main_sync_pending(&self) -> bool {
        self.sync_pending_bits() & SYNC_PENDING_MAIN != 0
    }

    #[inline]
    pub fn delta_sync_pending(&self) -> bool {
        self.sync_pending_bits() & SYNC_PENDING_DELTA != 0
    }

    #[inline]
    pub fn deltastore_sync_pending(&self) -> bool {
        self.sync_pending_bits() & SYNC_PENDING_DELTASTORE != 0
    }

    #[inline]
    pub fn clear_sync_pending(&self) {
        self.sync_pending.store(0, Ordering::SeqCst);
    }

    #[inline]
    pub fn clear_main_sync_pending(&self) {
        self.sync_pending
            .fetch_and(!SYNC_PENDING_MAIN, Ordering::SeqCst);
    }

    #[inline]
    pub fn clear_delta_sync_pending(&self) {
        self.sync_pending
            .fetch_and(!SYNC_PENDING_DELTA, Ordering::SeqCst);
    }

    #[inline]
    pub fn clear_deltastore_sync_pending(&self) {
        self.sync_pending
            .fetch_and(!SYNC_PENDING_DELTASTORE, Ordering::SeqCst);
    }

    /// Acquire global read lock for thread-safe concurrent reads.
    /// Returns a guard that releases the lock when dropped.
    /// Multiple readers can hold the lock simultaneously.
    #[inline]
    pub fn read_lock(&self) -> parking_lot::RwLockReadGuard<()> {
        self.global_lock.read()
    }

    /// Acquire global write lock for thread-safe writes.
    /// Returns a guard that releases the lock when dropped.
    /// Only one writer can hold the lock; readers are blocked while held.
    #[inline]
    pub fn write_lock(&self) -> parking_lot::RwLockWriteGuard<()> {
        self.global_lock.write()
    }

    /// Estimate current in-memory data size in bytes
    pub fn estimate_memory_bytes(&self) -> u64 {
        let columns = self.columns.read();
        let mut total: u64 = 0;

        for col in columns.iter() {
            total += col.estimate_memory_bytes() as u64;
        }

        // Add overhead for IDs (8 bytes each)
        total += self.ids.read().len() as u64 * 8;

        // Add overhead for null bitmaps
        for null_bitmap in self.nulls.read().iter() {
            total += null_bitmap.len() as u64;
        }

        // Add deleted bitmap
        total += self.deleted.read().len() as u64;

        total
    }

    /// Read bytes from the file using the user-space page cache.
    /// On cache miss, performs a positioned read (pread) and caches the 4KB page.
    /// On cache hit, copies bytes from the cached heap page — zero mmap page faults.
    /// This eliminates repeated soft page faults on macOS for point lookup paths.
    pub(crate) fn read_cached_bytes(&self, abs_offset: u64, dst: &mut [u8]) -> io::Result<()> {
        let len = dst.len();
        if len == 0 {
            return Ok(());
        }
        let mut written = 0usize;
        let mut cur_off = abs_offset;
        while written < len {
            let page_num = cur_off / 4096;
            let page_off = (cur_off % 4096) as usize;
            let to_copy = (len - written).min(4096 - page_off);
            // Fast path: page is in cache
            {
                let cache = self.page_cache.read();
                if let Some(page) = cache.get(&page_num) {
                    dst[written..written + to_copy]
                        .copy_from_slice(&page[page_off..page_off + to_copy]);
                    written += to_copy;
                    cur_off += to_copy as u64;
                    continue;
                }
            }
            // Cache miss: pread from file and cache the page
            let mut buf = [0u8; 4096];
            {
                let file_guard = self.file.read();
                let file = file_guard
                    .as_ref()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "file not open"))?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileExt;
                    let _ = file.read_at(&mut buf, page_num * 4096);
                }
                #[cfg(windows)]
                {
                    use std::os::windows::fs::FileExt;
                    let _ = file.seek_read(&mut buf, page_num * 4096);
                }
            }
            dst[written..written + to_copy].copy_from_slice(&buf[page_off..page_off + to_copy]);
            written += to_copy;
            cur_off += to_copy as u64;
            self.page_cache.write().insert(page_num, Box::new(buf));
        }
        Ok(())
    }

    /// Invalidate the user-space page cache and raw Arrow batch cache.
    /// Called after every write (save_v4, append_row_group, open_v4_data).
    pub(crate) fn invalidate_page_cache(&self) {
        self.page_cache.write().clear();
        self.scan_buf_file_size
            .store(0, std::sync::atomic::Ordering::Release);
        self.scan_buf_f16_file_size
            .store(0, std::sync::atomic::Ordering::Release);
    }

    /// Check if auto-flush is needed and perform it if so
    /// Returns true if auto-flush was performed
    fn maybe_auto_flush(&self) -> io::Result<bool> {
        let rows_threshold = self.auto_flush_rows.load(Ordering::SeqCst);
        let bytes_threshold = self.auto_flush_bytes.load(Ordering::SeqCst);

        // Check row threshold
        if rows_threshold > 0 {
            let pending = self.pending_rows.load(Ordering::SeqCst);
            if pending >= rows_threshold {
                self.save()?;
                self.pending_rows.store(0, Ordering::SeqCst);
                return Ok(true);
            }
        }

        // Check memory threshold
        if bytes_threshold > 0 {
            let mem_bytes = self.estimate_memory_bytes();
            if mem_bytes >= bytes_threshold {
                self.save()?;
                self.pending_rows.store(0, Ordering::SeqCst);
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Record rows appended through a typed/backend entry point.
    ///
    /// The low-level typed encoders are also used by compaction, so pending-row
    /// ownership lives at the public backend boundary instead of inside those
    /// encoders. Keeping this counter accurate prevents save() from mistaking
    /// buffered data for a schema-only metadata update. Auto-flush is invoked
    /// by the buffering entry points after their schema/cache bookkeeping is
    /// complete; immediate-persist callers invoke save() themselves.
    pub(crate) fn record_pending_rows(&self, row_count: usize) -> io::Result<()> {
        if row_count == 0 {
            return Ok(());
        }
        self.pending_rows
            .fetch_add(row_count as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Get the current compression type.
    pub fn compression(&self) -> CompressionType {
        match self.compression.load(Ordering::Relaxed) {
            1 => CompressionType::Lz4,
            2 => CompressionType::Zstd,
            _ => CompressionType::None,
        }
    }

    /// Set compression type. Only effective on empty tables (row_count == 0).
    /// The setting is persisted in the header flags and survives restarts.
    /// Returns Ok(true) if applied, Ok(false) if table is non-empty (no-op).
    pub fn set_compression(&self, comp: CompressionType) -> io::Result<bool> {
        if self.active_count.load(Ordering::SeqCst) > 0
            || self.persisted_row_count.load(Ordering::SeqCst) > 0
        {
            return Ok(false);
        }
        self.compression.store(comp as u8, Ordering::SeqCst);
        // Persist to header flags
        {
            let mut header = self.header.write();
            header.flags = (header.flags & !FLAG_COMPRESS_MASK) | comp.to_flags_bits();
        }
        // Re-save header to disk
        self.save()?;
        Ok(true)
    }

    /// Helper: Get file reference or return NotConnected error
    /// Reduces boilerplate in read methods
    #[inline]
    fn get_file_ref(&self) -> io::Result<parking_lot::RwLockReadGuard<'_, Option<File>>> {
        let guard = self.file.read();
        if guard.is_none() {
            return Err(err_not_conn("File not open"));
        }
        Ok(guard)
    }

    /// Create or open storage with default durability (Fast)
    pub fn open_or_create(path: &Path) -> io::Result<Self> {
        Self::open_or_create_with_durability(path, super::DurabilityLevel::Fast)
    }

    /// Create or open storage with specified durability level
    pub fn open_or_create_with_durability(
        path: &Path,
        durability: super::DurabilityLevel,
    ) -> io::Result<Self> {
        if path.exists() {
            Self::open_with_durability(path, durability)
        } else {
            Self::create_with_durability(path, durability)
        }
    }

    /// Open for write with default durability (Fast)
    pub fn open_for_write(path: &Path) -> io::Result<Self> {
        Self::open_for_write_with_durability(path, super::DurabilityLevel::Fast)
    }

    /// Open for write with specified durability level
    /// IMPORTANT: For memory efficiency, column data is loaded lazily.
    /// - For INSERT: use open_for_insert() which only loads metadata
    /// - For UPDATE/DELETE: this function loads all column data
    pub fn open_for_write_with_durability(
        path: &Path,
        durability: super::DurabilityLevel,
    ) -> io::Result<Self> {
        if !path.exists() {
            return Self::create_with_durability(path, durability);
        }

        // Open the storage first
        let storage = Self::open_with_durability(path, durability)?;

        // If there are existing rows, load all column data into memory
        // This is required because save() rewrites the entire file from self.columns
        let row_count = storage.header.read().row_count as usize;
        if row_count > 0 {
            storage.load_all_columns_into_memory()?;
        } else {
            // Even with 0 rows, initialize empty columns based on schema
            // This is needed for INSERT after ALTER TABLE (columns defined but no data)
            let schema = storage.schema.read();
            let mut columns = storage.columns.write();
            let mut nulls = storage.nulls.write();

            // Always reinitialize columns with correct types from schema
            // The initial columns vector may have placeholder Int64 types
            if schema.column_count() > 0 {
                columns.clear();
                nulls.clear();
                for (_name, col_type) in schema.columns.iter() {
                    columns.push(ColumnData::new(*col_type));
                    nulls.push(Vec::new());
                }
            }
        }

        Ok(storage)
    }

    /// Open for INSERT operations only - memory efficient!
    /// Only loads metadata (header, schema, ids), NOT column data.
    /// New data is written to a delta file and merged on read or compact.
    pub fn open_for_insert(path: &Path) -> io::Result<Self> {
        Self::open_for_insert_with_durability(path, super::DurabilityLevel::Fast)
    }

    /// Open for INSERT with specified durability - memory efficient!
    pub fn open_for_insert_with_durability(
        path: &Path,
        durability: super::DurabilityLevel,
    ) -> io::Result<Self> {
        if !path.exists() {
            return Self::create_with_durability(path, durability);
        }

        // Just open without loading column data - metadata only
        Self::open_with_durability(path, durability)
    }

    /// Open for SCHEMA changes only - MOST memory efficient!
    /// Only loads header, schema, and column index. Does NOT load IDs or column data.
    /// Use for: ALTER TABLE ADD/DROP/RENAME COLUMN, TRUNCATE
    pub fn open_for_schema_change(path: &Path) -> io::Result<Self> {
        Self::open_for_schema_change_with_durability(path, super::DurabilityLevel::Fast)
    }

    /// Open for SCHEMA changes with specified durability.
    /// Delegates to open_with_durability (V4-only format).
    pub fn open_for_schema_change_with_durability(
        path: &Path,
        durability: super::DurabilityLevel,
    ) -> io::Result<Self> {
        if !path.exists() {
            return Self::create_with_durability(path, durability);
        }
        Self::open_with_durability(path, durability)
    }

    /// Get the delta file path for this storage
    fn delta_path(base_path: &Path) -> PathBuf {
        let mut delta = base_path.to_path_buf();
        let name = delta.file_name().unwrap_or_default().to_string_lossy();
        delta.set_file_name(format!("{}.delta", name));
        delta
    }

    fn delta_meta_path(delta_path: &Path) -> PathBuf {
        let mut meta = delta_path.to_path_buf();
        let name = meta.file_name().unwrap_or_default().to_string_lossy();
        meta.set_file_name(format!("{}.meta", name));
        meta
    }

    // ========================================================================
    // DeltaStore accessors (Phase 4.5)
    // ========================================================================

    /// Record a cell-level update in the delta store.
    /// Used by UPDATE to avoid delete+insert for single-cell changes.
    pub fn delta_update_cell(&self, row_id: u64, column_name: &str, new_value: crate::data::Value) {
        self.delta_store
            .write()
            .update_cell(row_id, column_name, new_value);
        crate::storage::epoch::bump(&self.path);
    }

    /// Record a full row update in the delta store.
    pub fn delta_update_row(&self, row_id: u64, values: &HashMap<String, crate::data::Value>) {
        self.delta_store.write().update_row(row_id, values);
        crate::storage::epoch::bump(&self.path);
    }

    fn row_active_for_delta_overlay(&self, row_id: u64) -> io::Result<bool> {
        if row_id >= self.next_id.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(false);
        }
        if self.delta_store.read().is_deleted(row_id) {
            return Ok(false);
        }

        match self.row_id_active_rcix(row_id)? {
            Some(true) => Ok(true),
            Some(false) => {
                if self.pending_v4_in_memory_rows() == 0 {
                    Ok(false)
                } else {
                    Ok(self.exists(row_id))
                }
            }
            None => Ok(self.exists(row_id)),
        }
    }

    /// Record a row deletion in the delta store without rewriting the base file.
    pub fn delta_delete_row(&self, row_id: u64) -> io::Result<bool> {
        if !self.row_active_for_delta_overlay(row_id)? {
            return Ok(false);
        }
        self.delta_store.write().delete_row(row_id);
        crate::storage::epoch::bump(&self.path);
        Ok(true)
    }

    /// Record already-resolved logical row IDs as deleted in one batch.
    pub fn delta_delete_rows(&self, row_ids: &[u64]) -> usize {
        let deleted = self.delta_store.write().delete_rows(row_ids);
        if deleted > 0 {
            crate::storage::epoch::bump(&self.path);
        }
        deleted
    }

    /// Match rows that still live in the append-only row delta.
    pub fn delta_numeric_range_ids(
        &self,
        column_name: &str,
        low: f64,
        high: f64,
    ) -> io::Result<Vec<u64>> {
        let delta_path = Self::delta_path(&self.path);
        let Ok(metadata) = std::fs::metadata(&delta_path) else {
            DELTA_NUMERIC_RANGE_CACHE.write().remove(&delta_path);
            return Ok(Vec::new());
        };
        let file_len = metadata.len();
        let modified = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let key = DeltaNumericRangeKey {
            column: column_name.to_string(),
            low_bits: low.to_bits(),
            high_bits: high.to_bits(),
        };

        let (start, mut ids) = {
            let cache = DELTA_NUMERIC_RANGE_CACHE.read();
            match cache.get(&delta_path).and_then(|queries| queries.get(&key)) {
                Some(entry) if entry.len == file_len && entry.modified >= modified => {
                    let deleted = self.delta_store.read();
                    return Ok(entry
                        .ids
                        .iter()
                        .copied()
                        .filter(|id| !deleted.is_deleted(*id))
                        .collect());
                }
                Some(entry) if entry.len < file_len && entry.modified <= modified => {
                    (entry.len, entry.ids.clone())
                }
                _ => (0, Vec::new()),
            }
        };

        let mut file = File::open(&delta_path)?;
        file.seek(SeekFrom::Start(start))?;
        let mut bytes = Vec::with_capacity((file_len - start) as usize);
        file.read_to_end(&mut bytes)?;
        Self::scan_delta_numeric_range_bytes(&bytes, column_name, low, high, &mut ids)?;

        {
            let mut cache = DELTA_NUMERIC_RANGE_CACHE.write();
            if cache.len() > 128 {
                cache.clear();
            }
            let queries = cache.entry(delta_path).or_default();
            if queries.len() > 32 && !queries.contains_key(&key) {
                queries.clear();
            }
            queries.insert(
                key,
                DeltaNumericRangeEntry {
                    len: file_len,
                    modified,
                    ids: ids.clone(),
                },
            );
        }

        let deleted = self.delta_store.read();
        ids.retain(|id| !deleted.is_deleted(*id));
        Ok(ids)
    }

    pub fn delta_string_equality_ids(
        &self,
        column_name: &str,
        expected: &str,
    ) -> io::Result<Vec<u64>> {
        let mut ids = self.delta_string_match_ids(column_name, expected)?;
        let deleted = self.delta_store.read();
        ids.retain(|id| !deleted.is_deleted(*id));
        Ok(ids)
    }

    pub fn pending_numeric_range_ids(
        &self,
        column_name: &str,
        low: f64,
        high: f64,
    ) -> Vec<u64> {
        let pending = self.pending_v4_in_memory_rows();
        if pending == 0 {
            return Vec::new();
        }
        let Some(column_index) = self.schema.read().get_index(column_name) else {
            return Vec::new();
        };
        let ids = self.ids.read();
        let columns = self.columns.read();
        let Some(column) = columns.get(column_index) else {
            return Vec::new();
        };
        let rows = pending.min(ids.len()).min(column.len());
        let id_start = ids.len() - rows;
        let value_start = column.len() - rows;
        match column {
            ColumnData::Int64(values) => (0..rows)
                .filter_map(|offset| {
                    let value = values[value_start + offset] as f64;
                    (value >= low && value <= high).then_some(ids[id_start + offset])
                })
                .collect(),
            ColumnData::Float64(values) => (0..rows)
                .filter_map(|offset| {
                    let value = values[value_start + offset];
                    (value >= low && value <= high).then_some(ids[id_start + offset])
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    pub fn pending_string_equality_ids(&self, column_name: &str, expected: &str) -> Vec<u64> {
        let pending = self.pending_v4_in_memory_rows();
        if pending == 0 {
            return Vec::new();
        }
        let Some(column_index) = self.schema.read().get_index(column_name) else {
            return Vec::new();
        };
        let ids = self.ids.read();
        let columns = self.columns.read();
        let Some(column) = columns.get(column_index) else {
            return Vec::new();
        };
        let rows = pending.min(ids.len()).min(column.len());
        let id_start = ids.len() - rows;
        let value_start = column.len() - rows;
        (0..rows)
            .filter_map(|offset| {
                (Self::column_string_at(column, value_start + offset) == Some(expected))
                    .then_some(ids[id_start + offset])
            })
            .collect()
    }

    #[inline]
    pub fn persisted_row_count(&self) -> u64 {
        self.persisted_row_count.load(Ordering::Relaxed)
    }

    fn scan_delta_numeric_range_bytes(
        bytes: &[u8],
        column_name: &str,
        low: f64,
        high: f64,
        matched: &mut Vec<u64>,
    ) -> io::Result<()> {
        #[inline]
        fn take<'a>(bytes: &'a [u8], pos: &mut usize, len: usize) -> io::Result<&'a [u8]> {
            let end = pos
                .checked_add(len)
                .ok_or_else(|| err_data("delta numeric scan offset overflow"))?;
            if end > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "delta numeric scan truncated",
                ));
            }
            let value = &bytes[*pos..end];
            *pos = end;
            Ok(value)
        }

        #[inline]
        fn read_u16(bytes: &[u8], pos: &mut usize) -> io::Result<u16> {
            Ok(u16::from_le_bytes(take(bytes, pos, 2)?.try_into().unwrap()))
        }

        #[inline]
        fn read_u32(bytes: &[u8], pos: &mut usize) -> io::Result<u32> {
            Ok(u32::from_le_bytes(take(bytes, pos, 4)?.try_into().unwrap()))
        }

        #[inline]
        fn read_u64(bytes: &[u8], pos: &mut usize) -> io::Result<u64> {
            Ok(u64::from_le_bytes(take(bytes, pos, 8)?.try_into().unwrap()))
        }

        let target = column_name.as_bytes();
        let mut pos = 0usize;
        while pos < bytes.len() {
            let rows = read_u64(bytes, &mut pos)? as usize;
            let ids = take(bytes, &mut pos, rows.saturating_mul(8))?;

            let int_columns = read_u32(bytes, &mut pos)? as usize;
            for _ in 0..int_columns {
                let name_len = read_u16(bytes, &mut pos)? as usize;
                let name = take(bytes, &mut pos, name_len)?;
                let values = take(bytes, &mut pos, rows.saturating_mul(8))?;
                if name == target {
                    for row in 0..rows {
                        let offset = row * 8;
                        let value =
                            i64::from_le_bytes(values[offset..offset + 8].try_into().unwrap())
                                as f64;
                        if value >= low && value <= high {
                            matched.push(u64::from_le_bytes(
                                ids[offset..offset + 8].try_into().unwrap(),
                            ));
                        }
                    }
                }
            }

            let float_columns = read_u32(bytes, &mut pos)? as usize;
            for _ in 0..float_columns {
                let name_len = read_u16(bytes, &mut pos)? as usize;
                let name = take(bytes, &mut pos, name_len)?;
                let values = take(bytes, &mut pos, rows.saturating_mul(8))?;
                if name == target {
                    for row in 0..rows {
                        let offset = row * 8;
                        let value =
                            f64::from_le_bytes(values[offset..offset + 8].try_into().unwrap());
                        if value >= low && value <= high {
                            matched.push(u64::from_le_bytes(
                                ids[offset..offset + 8].try_into().unwrap(),
                            ));
                        }
                    }
                }
            }

            let string_columns = read_u32(bytes, &mut pos)? as usize;
            for _ in 0..string_columns {
                let name_len = read_u16(bytes, &mut pos)? as usize;
                take(bytes, &mut pos, name_len)?;
                for _ in 0..rows {
                    let value_len = read_u32(bytes, &mut pos)? as usize;
                    take(bytes, &mut pos, value_len)?;
                }
            }

            let bool_columns = read_u32(bytes, &mut pos)? as usize;
            for _ in 0..bool_columns {
                let name_len = read_u16(bytes, &mut pos)? as usize;
                take(bytes, &mut pos, name_len)?;
                take(bytes, &mut pos, rows)?;
            }
        }
        Ok(())
    }

    /// Record a full-row replacement in the delta store for an existing row.
    pub fn delta_update_existing_row(
        &self,
        row_id: u64,
        values: &HashMap<String, crate::data::Value>,
    ) -> io::Result<bool> {
        if !self.row_active_for_delta_overlay(row_id)? {
            return Ok(false);
        }
        self.delta_store.write().update_row(row_id, values);
        crate::storage::epoch::bump(&self.path);
        Ok(true)
    }

    /// Batch update multiple rows in a single lock acquisition.
    /// `batch` is a slice of (row_id, col_name, new_value) triples.
    pub fn delta_batch_update_rows(&self, batch: &[(u64, &str, crate::data::Value)]) {
        if !batch.is_empty() {
            self.delta_store.write().batch_update_rows(batch);
            crate::storage::epoch::bump(&self.path);
        }
    }

    /// Scan a numeric column for rows in [low, high] and return their row IDs directly.
    /// Returns None if not applicable (column not found, etc.).
    pub fn scan_numeric_range_with_ids(
        &self,
        col_name: &str,
        low: f64,
        high: f64,
    ) -> io::Result<Option<Vec<u64>>> {
        self.scan_numeric_range_mmap_with_ids(col_name, low, high)
    }

    /// Check if the delta store has any pending changes.
    pub fn has_pending_deltas(&self) -> bool {
        !self.delta_store.read().is_empty()
    }

    /// Get the number of pending delta updates.
    pub fn delta_update_count(&self) -> usize {
        self.delta_store.read().update_count()
    }

    /// Get the number of pending delta deletes.
    pub fn delta_delete_count(&self) -> usize {
        self.delta_store.read().delete_count()
    }

    /// Check whether pending DeltaStore updates modify a specific column.
    pub fn delta_updates_column(&self, column_name: &str) -> bool {
        self.delta_store.read().updates_column(column_name)
    }

    /// Return row IDs whose pending DeltaStore update sets `column_name` to `value`.
    pub fn delta_rows_with_string_update(&self, column_name: &str, value: &str) -> Vec<u64> {
        self.delta_store
            .read()
            .rows_with_string_update(column_name, value)
    }

    /// Save the delta store to disk (called during save path).
    pub fn save_delta_store(&self) -> io::Result<()> {
        let mut delta_store = self.delta_store.write();
        let was_dirty = delta_store.is_dirty();
        delta_store.save()?;
        drop(delta_store);

        if was_dirty {
            if self.durability == super::DurabilityLevel::Max {
                self.clear_deltastore_sync_pending();
            } else {
                self.mark_deltastore_sync_pending();
            }
        }

        Ok(())
    }

    /// Clear the delta store (called after compaction merges deltas into base).
    pub fn clear_delta_store(&self) -> io::Result<()> {
        let mut ds = self.delta_store.write();
        ds.clear();
        ds.save()?;
        ds.remove_file()?;
        self.clear_deltastore_sync_pending();
        Ok(())
    }

    /// Get a read reference to the delta store (for DeltaMerger on read path).
    pub fn delta_store(&self) -> parking_lot::RwLockReadGuard<'_, DeltaStore> {
        self.delta_store.read()
    }

    /// Check if delta compaction is needed based on update/delete count vs base rows.
    pub fn needs_delta_compaction(&self) -> bool {
        let ds = self.delta_store.read();
        let base_rows = self.active_count.load(std::sync::atomic::Ordering::Relaxed);
        ds.needs_compaction(base_rows)
    }

    /// Compact deltas into the base file: load base data, apply updates in-place,
    /// then do a full save_v4 rewrite which clears the delta store.
    pub fn compact_deltas(&self) -> io::Result<()> {
        let ds = self.delta_store.read();
        if ds.is_empty() {
            return Ok(());
        }

        // Collect updates and deletes before releasing the lock
        let all_updates = ds.all_updates().clone();
        let delete_bitmap = ds.delete_bitmap().clone();
        drop(ds);

        // Skip compaction if V4 data isn't in memory — deltas stay in DeltaStore
        // and are applied at read time via DeltaMerger overlay.
        if self.is_v4_format() && !self.has_v4_in_memory_data() {
            return Ok(());
        }

        // Apply deletes: mark deleted rows in the deleted bitmap
        {
            let ids = self.ids.read();
            let mut deleted = self.deleted.write();
            for (idx, id) in ids.iter().enumerate() {
                if delete_bitmap.is_deleted(*id) {
                    let byte_idx = idx / 8;
                    let bit_idx = idx % 8;
                    if byte_idx < deleted.len() {
                        deleted[byte_idx] |= 1 << bit_idx;
                    }
                }
            }
        }

        // Apply cell-level updates to in-memory columns
        {
            let ids = self.ids.read();
            let schema = self.schema.read();
            let mut columns = self.columns.write();

            // Build id→index map for fast lookup
            let id_to_idx: std::collections::HashMap<u64, usize> =
                ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();

            for (row_id, col_updates) in &all_updates {
                if let Some(&row_idx) = id_to_idx.get(row_id) {
                    for (col_name, record) in col_updates {
                        if let Some(col_idx) = schema.get_index(col_name) {
                            if col_idx < columns.len() {
                                match &record.new_value {
                                    crate::data::Value::Int64(v) => {
                                        if let ColumnData::Int64(ref mut data) = columns[col_idx] {
                                            if row_idx < data.len() {
                                                data[row_idx] = *v;
                                            }
                                        }
                                    }
                                    crate::data::Value::Float64(v) => {
                                        if let ColumnData::Float64(ref mut data) = columns[col_idx]
                                        {
                                            if row_idx < data.len() {
                                                data[row_idx] = *v;
                                            }
                                        }
                                    }
                                    crate::data::Value::String(s) => {
                                        if let ColumnData::String { offsets, data } =
                                            &mut columns[col_idx]
                                        {
                                            // For strings, we need to rebuild — update in-place is complex
                                            // For compaction (rare), this is acceptable
                                            let mut strings: Vec<String> =
                                                Vec::with_capacity(offsets.len().saturating_sub(1));
                                            for i in 0..offsets.len().saturating_sub(1) {
                                                let start = offsets[i] as usize;
                                                let end = offsets[i + 1] as usize;
                                                if i == row_idx {
                                                    strings.push(s.clone());
                                                } else {
                                                    strings.push(
                                                        String::from_utf8_lossy(&data[start..end])
                                                            .to_string(),
                                                    );
                                                }
                                            }
                                            // Rebuild
                                            data.clear();
                                            offsets.clear();
                                            offsets.push(0u64);
                                            for st in &strings {
                                                data.extend_from_slice(st.as_bytes());
                                                offsets.push(data.len() as u64);
                                            }
                                        }
                                    }
                                    crate::data::Value::Bool(v) => {
                                        if let ColumnData::Bool { data, .. } = &mut columns[col_idx]
                                        {
                                            let byte_idx = row_idx / 8;
                                            let bit_idx = row_idx % 8;
                                            if byte_idx < data.len() {
                                                if *v {
                                                    data[byte_idx] |= 1 << bit_idx;
                                                } else {
                                                    data[byte_idx] &= !(1 << bit_idx);
                                                }
                                            }
                                        }
                                    }
                                    _ => {} // UInt64, Null, etc. — skip for now
                                }
                            }
                        }
                    }
                }
            }
        }

        // Full rewrite, then clear delta store (updates are now baked into base file)
        self.save_v4()?;
        self.clear_delta_store()
    }

    /// Apply any pending delta store updates/deletes to already-loaded in-memory columns.
    /// Must be called AFTER load_all_columns_into_memory() so self.ids/columns/deleted are populated.
    /// This ensures save_v4() always writes the correct (post-update) values and can safely
    /// clear the delta store afterwards.
    fn apply_pending_deltas_in_place(&self) {
        let ds = self.delta_store.read();
        if ds.is_empty() {
            return;
        }
        let all_updates = ds.all_updates().clone();
        let delete_bitmap = ds.delete_bitmap().clone();
        drop(ds);

        if !delete_bitmap.is_empty() {
            let ids = self.ids.read();
            let mut deleted = self.deleted.write();
            for (idx, id) in ids.iter().enumerate() {
                if delete_bitmap.is_deleted(*id) {
                    let byte_idx = idx / 8;
                    let bit_idx = idx % 8;
                    if byte_idx >= deleted.len() {
                        deleted.resize(byte_idx + 1, 0);
                    }
                    deleted[byte_idx] |= 1 << bit_idx;
                }
            }
        }

        if !all_updates.is_empty() {
            let ids = self.ids.read();
            let schema = self.schema.read();
            let mut columns = self.columns.write();

            let id_to_idx: ahash::AHashMap<u64, usize> =
                ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();

            for (row_id, col_updates) in &all_updates {
                if let Some(&row_idx) = id_to_idx.get(row_id) {
                    for (col_name, record) in col_updates {
                        if let Some(col_idx) = schema.get_index(col_name) {
                            if col_idx < columns.len() {
                                match &record.new_value {
                                    crate::data::Value::Int64(v) => {
                                        if let ColumnData::Int64(ref mut data) = columns[col_idx] {
                                            if row_idx < data.len() {
                                                data[row_idx] = *v;
                                            }
                                        }
                                    }
                                    crate::data::Value::Float64(v) => {
                                        if let ColumnData::Float64(ref mut data) = columns[col_idx]
                                        {
                                            if row_idx < data.len() {
                                                data[row_idx] = *v;
                                            }
                                        }
                                    }
                                    crate::data::Value::String(s) => {
                                        if let ColumnData::String { offsets, data } =
                                            &mut columns[col_idx]
                                        {
                                            let mut strings: Vec<String> =
                                                Vec::with_capacity(offsets.len().saturating_sub(1));
                                            for i in 0..offsets.len().saturating_sub(1) {
                                                let start = offsets[i] as usize;
                                                let end = offsets[i + 1] as usize;
                                                if i == row_idx {
                                                    strings.push(s.clone());
                                                } else {
                                                    strings.push(
                                                        String::from_utf8_lossy(&data[start..end])
                                                            .to_string(),
                                                    );
                                                }
                                            }
                                            data.clear();
                                            offsets.clear();
                                            offsets.push(0u64);
                                            for st in &strings {
                                                data.extend_from_slice(st.as_bytes());
                                                offsets.push(data.len() as u64);
                                            }
                                        }
                                    }
                                    crate::data::Value::Bool(v) => {
                                        if let ColumnData::Bool { data, .. } = &mut columns[col_idx]
                                        {
                                            let byte_idx = row_idx / 8;
                                            let bit_idx = row_idx % 8;
                                            if byte_idx < data.len() {
                                                if *v {
                                                    data[byte_idx] |= 1 << bit_idx;
                                                } else {
                                                    data[byte_idx] &= !(1 << bit_idx);
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Get the maximum ID from a delta file (for computing next_id on open)
    fn get_max_id_from_delta(delta_path: &Path) -> io::Result<u64> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = File::open(delta_path)?;
        let mut max_id: u64 = 0;

        loop {
            // Read record count
            let mut count_buf = [0u8; 8];
            match file.read_exact(&mut count_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let record_count = u64::from_le_bytes(count_buf) as usize;

            // Read IDs and track max
            for _ in 0..record_count {
                let mut id_buf = [0u8; 8];
                file.read_exact(&mut id_buf)?;
                let id = u64::from_le_bytes(id_buf);
                max_id = max_id.max(id);
            }

            // Skip rest of record (int columns)
            let mut count_buf4 = [0u8; 4];
            file.read_exact(&mut count_buf4)?;
            let int_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..int_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                file.seek(SeekFrom::Current(name_len as i64))?;
                file.seek(SeekFrom::Current((record_count * 8) as i64))?;
            }

            // Skip float columns
            file.read_exact(&mut count_buf4)?;
            let float_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..float_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                file.seek(SeekFrom::Current(name_len as i64))?;
                file.seek(SeekFrom::Current((record_count * 8) as i64))?;
            }

            // Skip string columns (variable length - need to read lengths)
            file.read_exact(&mut count_buf4)?;
            let string_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..string_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                file.seek(SeekFrom::Current(name_len as i64))?;
                for _ in 0..record_count {
                    let mut str_len_buf = [0u8; 4];
                    file.read_exact(&mut str_len_buf)?;
                    let str_len = u32::from_le_bytes(str_len_buf) as usize;
                    file.seek(SeekFrom::Current(str_len as i64))?;
                }
            }

            // Skip bool columns
            file.read_exact(&mut count_buf4)?;
            let bool_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..bool_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                file.seek(SeekFrom::Current(name_len as i64))?;
                let skip_bytes = (record_count + 7) / 8;
                file.seek(SeekFrom::Current(skip_bytes as i64))?;
            }

            // Skip binary columns (variable length)
            file.read_exact(&mut count_buf4)?;
            let binary_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..binary_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                file.seek(SeekFrom::Current(name_len as i64))?;
                for _ in 0..record_count {
                    let mut bin_len_buf = [0u8; 4];
                    file.read_exact(&mut bin_len_buf)?;
                    let bin_len = u32::from_le_bytes(bin_len_buf) as usize;
                    file.seek(SeekFrom::Current(bin_len as i64))?;
                }
            }
        }

        Ok(max_id)
    }

    fn get_max_id_from_delta_fast(delta_path: &Path) -> io::Result<u64> {
        let meta_path = Self::delta_meta_path(delta_path);
        if let Ok(bytes) = std::fs::read(&meta_path) {
            if bytes.len() == 8 {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                return Ok(u64::from_le_bytes(buf));
            }
        }

        let max_id = Self::get_max_id_from_delta(delta_path)?;
        let _ = Self::write_delta_max_id(delta_path, max_id);
        Ok(max_id)
    }

    fn write_delta_max_id(delta_path: &Path, max_id: u64) -> io::Result<()> {
        std::fs::write(Self::delta_meta_path(delta_path), max_id.to_le_bytes())
    }

    /// Check if delta file exists
    pub fn has_delta(&self) -> bool {
        Self::delta_path(&self.path).exists()
    }

    /// Load all column data from disk into memory
    /// This is needed before write operations to preserve existing data
    fn load_all_columns_into_memory(&self) -> io::Result<()> {
        let header = self.header.read();
        let total_rows = header.row_count as usize;

        if total_rows == 0 {
            return Ok(());
        }

        // V4 files: load all RG data into memory for write operations
        if header.footer_offset > 0 {
            drop(header);
            self.open_v4_data()?;
            // Apply any pending delta store updates so save_v4() bakes them in correctly.
            // Without this, a subsequent save() would write pre-update values to disk and
            // then clear the delta store, permanently losing the updates.
            self.apply_pending_deltas_in_place();
            return Ok(());
        }

        let schema = self.schema.read();
        let column_index = self.column_index.read();

        // CRITICAL: Load IDs first since they're lazy-loaded
        // Without this, insert operations will think there are 0 existing rows
        drop(header);
        drop(schema);
        drop(column_index);
        self.ensure_ids_loaded()?;
        let header = self.header.read();
        let schema = self.schema.read();
        let column_index = self.column_index.read();

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open"))?;

        let mut mmap_cache = self.mmap_cache.write();
        let mut columns = self.columns.write();
        let mut nulls = self.nulls.write();

        let column_index_len = column_index.len();

        // Load each column from disk
        for col_idx in 0..schema.column_count() {
            let (_, col_type) = &schema.columns[col_idx];

            // Handle columns added via ALTER TABLE that don't have disk data yet
            if col_idx >= column_index_len {
                // Column exists in schema but not on disk - create padded column
                let mut col_data = ColumnData::new(*col_type);
                // Pad with defaults for existing rows
                for _ in 0..total_rows {
                    match &mut col_data {
                        ColumnData::Int64(v) => v.push(0),
                        ColumnData::Float64(v) => v.push(0.0),
                        ColumnData::String { offsets, .. } => {
                            offsets.push(*offsets.last().unwrap_or(&0))
                        }
                        ColumnData::Binary { offsets, .. } => {
                            offsets.push(*offsets.last().unwrap_or(&0))
                        }
                        ColumnData::Bool { data, len } => {
                            let byte_idx = *len / 8;
                            if byte_idx >= data.len() {
                                data.push(0);
                            }
                            *len += 1;
                        }
                        ColumnData::StringDict { indices, .. } => indices.push(0),
                        ColumnData::FixedList { .. } => {} // pads implicitly
                        ColumnData::Float16List { .. } => {} // pads implicitly
                    }
                }

                if col_idx < columns.len() {
                    columns[col_idx] = col_data;
                } else {
                    columns.push(col_data);
                }

                // Empty null bitmap for new columns
                if col_idx < nulls.len() {
                    nulls[col_idx] = Vec::new();
                } else {
                    nulls.push(Vec::new());
                }
                continue;
            }

            let index_entry = &column_index[col_idx];

            // Read column data
            let col_data = self.read_column_range_mmap(
                &mut mmap_cache,
                file,
                index_entry,
                *col_type,
                0,
                total_rows,
                total_rows,
            )?;

            // Store in columns array
            if col_idx < columns.len() {
                columns[col_idx] = col_data;
            } else {
                columns.push(col_data);
            }

            // Read null bitmap for this column
            let null_len = index_entry.null_length as usize;
            if null_len > 0 {
                let mut null_bitmap = vec![0u8; null_len];
                mmap_cache.read_at(file, &mut null_bitmap, index_entry.null_offset)?;
                if col_idx < nulls.len() {
                    nulls[col_idx] = null_bitmap;
                } else {
                    nulls.push(null_bitmap);
                }
            }
        }

        Ok(())
    }

    fn append_typed_to_delta_with_ids(
        &self,
        ids: &[u64],
        int_columns: &HashMap<String, Vec<i64>>,
        float_columns: &HashMap<String, Vec<f64>>,
        string_columns: &HashMap<String, Vec<String>>,
        bool_columns: &HashMap<String, Vec<bool>>,
    ) -> io::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let delta_path = Self::delta_path(&self.path);
        let mut delta_file = self.delta_file.write();
        if delta_file.is_none() {
            if let Some(parent) = delta_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&delta_path)?;
            *delta_file = Some(file);
        }
        let file = delta_file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "delta file not open"))?;

        // Delta spill is the hot path for explicit flush() on tiny OLTP bursts.
        // Buffer small per-field writes so a 1-row durable flush is not dominated
        // by dozens of tiny append syscalls.
        let mut writer = std::io::BufWriter::with_capacity(64 * 1024, &mut *file);

        writer.write_all(&(ids.len() as u64).to_le_bytes())?;
        for id in ids {
            writer.write_all(&id.to_le_bytes())?;
        }

        let int_col_count = int_columns.len() as u32;
        writer.write_all(&int_col_count.to_le_bytes())?;
        for (name, values) in int_columns {
            let name_bytes = name.as_bytes();
            writer.write_all(&(name_bytes.len() as u16).to_le_bytes())?;
            writer.write_all(name_bytes)?;
            for v in values {
                writer.write_all(&v.to_le_bytes())?;
            }
        }

        let float_col_count = float_columns.len() as u32;
        writer.write_all(&float_col_count.to_le_bytes())?;
        for (name, values) in float_columns {
            let name_bytes = name.as_bytes();
            writer.write_all(&(name_bytes.len() as u16).to_le_bytes())?;
            writer.write_all(name_bytes)?;
            for v in values {
                writer.write_all(&v.to_le_bytes())?;
            }
        }

        let string_col_count = string_columns.len() as u32;
        writer.write_all(&string_col_count.to_le_bytes())?;
        for (name, values) in string_columns {
            let name_bytes = name.as_bytes();
            writer.write_all(&(name_bytes.len() as u16).to_le_bytes())?;
            writer.write_all(name_bytes)?;
            for v in values {
                let v_bytes = v.as_bytes();
                writer.write_all(&(v_bytes.len() as u32).to_le_bytes())?;
                writer.write_all(v_bytes)?;
            }
        }

        let bool_col_count = bool_columns.len() as u32;
        writer.write_all(&bool_col_count.to_le_bytes())?;
        for (name, values) in bool_columns {
            let name_bytes = name.as_bytes();
            writer.write_all(&(name_bytes.len() as u16).to_le_bytes())?;
            writer.write_all(name_bytes)?;
            for v in values {
                writer.write_all(&[if *v { 1u8 } else { 0u8 }])?;
            }
        }

        writer.flush()?;
        drop(writer);
        if self.durability == super::DurabilityLevel::Max {
            file.sync_all()?;
            self.clear_delta_sync_pending();
        } else {
            self.mark_delta_sync_pending();
        }
        if let Some(max_id) = ids.iter().copied().max() {
            let _ = Self::write_delta_max_id(&delta_path, max_id);
        }

        Ok(())
    }

    /// Insert rows to delta file (memory efficient - doesn't load existing data)
    /// Returns the IDs assigned to the inserted rows
    pub fn insert_rows_to_delta(
        &self,
        rows: &[HashMap<String, ColumnValue>],
    ) -> io::Result<Vec<u64>> {
        self.insert_rows_to_delta_impl(rows)
    }

    /// Delta counterpart to `insert_value_rows`: borrow facade values until
    /// they are copied into the final column buffers written to disk.
    pub(crate) fn insert_value_rows_to_delta(
        &self,
        rows: &[HashMap<String, crate::data::Value>],
    ) -> io::Result<Vec<u64>> {
        self.insert_rows_to_delta_impl(rows)
    }

    fn insert_rows_to_delta_impl<V: AsColumnValueRef>(
        &self,
        rows: &[HashMap<String, V>],
    ) -> io::Result<Vec<u64>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let delta_path = Self::delta_path(&self.path);
        let delta_before = std::fs::metadata(&delta_path).ok().map(|metadata| {
            (
                metadata.len(),
                metadata
                    .modified()
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            )
        });

        // Get schema to handle partial columns correctly
        let schema = self.schema.read();

        // Build column data from rows - ensure all columns have same length
        let mut int_columns: HashMap<String, Vec<i64>> = HashMap::new();
        let mut float_columns: HashMap<String, Vec<f64>> = HashMap::new();
        let mut string_columns: HashMap<String, Vec<String>> = HashMap::new();
        let mut binary_columns: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut bool_columns: HashMap<String, Vec<bool>> = HashMap::new();

        // Initialize column vectors based on schema
        for (col_name, col_type) in &schema.columns {
            match col_type {
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
                    int_columns.insert(col_name.clone(), Vec::with_capacity(rows.len()));
                }
                ColumnType::Float64 | ColumnType::Float32 => {
                    float_columns.insert(col_name.clone(), Vec::with_capacity(rows.len()));
                }
                ColumnType::String | ColumnType::StringDict => {
                    string_columns.insert(col_name.clone(), Vec::with_capacity(rows.len()));
                }
                ColumnType::Binary | ColumnType::Blob => {
                    binary_columns.insert(col_name.clone(), Vec::with_capacity(rows.len()));
                }
                ColumnType::FixedList | ColumnType::Float16List => {
                    binary_columns.insert(col_name.clone(), Vec::with_capacity(rows.len()));
                }
                ColumnType::Bool => {
                    bool_columns.insert(col_name.clone(), Vec::with_capacity(rows.len()));
                }
                ColumnType::Null => {
                    // Null columns are handled as strings with empty default
                    string_columns.insert(col_name.clone(), Vec::with_capacity(rows.len()));
                }
            }
        }

        // For each row, add values for ALL schema columns (default for missing)
        for row in rows {
            for (col_name, col_type) in &schema.columns {
                let val = row.get(col_name);
                match col_type {
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
                        let v = val
                            .and_then(|v| {
                                if let ColumnValueRef::Int64(n) = v.as_delta_column_value_ref() {
                                    Some(n)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0);
                        int_columns.get_mut(col_name).unwrap().push(v);
                    }
                    ColumnType::Float64 | ColumnType::Float32 => {
                        let v = val
                            .and_then(|v| {
                                if let ColumnValueRef::Float64(n) = v.as_delta_column_value_ref() {
                                    Some(n)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0.0);
                        float_columns.get_mut(col_name).unwrap().push(v);
                    }
                    ColumnType::String | ColumnType::StringDict | ColumnType::Null => {
                        let v = val
                            .and_then(|v| {
                                if let ColumnValueRef::String(s) = v.as_delta_column_value_ref() {
                                    Some(s.into_owned())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_default();
                        string_columns.get_mut(col_name).unwrap().push(v);
                    }
                    ColumnType::Binary | ColumnType::Blob => {
                        let v = val
                            .and_then(|v| match v.as_delta_column_value_ref() {
                                ColumnValueRef::Binary(b) | ColumnValueRef::Blob(b) => {
                                    Some(b.to_vec())
                                }
                                _ => None,
                            })
                            .unwrap_or_default();
                        binary_columns.get_mut(col_name).unwrap().push(v);
                    }
                    ColumnType::FixedList | ColumnType::Float16List => {
                        let v = val
                            .and_then(|v| match v.as_delta_column_value_ref() {
                                ColumnValueRef::FixedList(b) | ColumnValueRef::Binary(b) => {
                                    Some(b.to_vec())
                                }
                                _ => None,
                            })
                            .unwrap_or_default();
                        binary_columns.get_mut(col_name).unwrap().push(v);
                    }
                    ColumnType::Bool => {
                        let v = val
                            .and_then(|v| {
                                if let ColumnValueRef::Bool(b) = v.as_delta_column_value_ref() {
                                    Some(b)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(false);
                        bool_columns.get_mut(col_name).unwrap().push(v);
                    }
                }
            }
        }

        drop(schema);

        // Allocate IDs
        let mut ids = Vec::with_capacity(rows.len());
        for _ in 0..rows.len() {
            ids.push(self.next_id.fetch_add(1, Ordering::SeqCst));
        }

        self.append_typed_to_delta_with_ids(
            &ids,
            &int_columns,
            &float_columns,
            &string_columns,
            &bool_columns,
        )?;
        Self::refresh_delta_insert_caches(
            &self.path,
            &delta_path,
            delta_before,
            &ids,
            &string_columns,
        );
        crate::storage::epoch::bump(&self.path);
        Ok(ids)
    }

    fn refresh_delta_insert_caches(
        table_path: &Path,
        delta_path: &Path,
        before: Option<(u64, std::time::SystemTime)>,
        ids: &[u64],
        string_columns: &HashMap<String, Vec<String>>,
    ) {
        if ids.is_empty() {
            return;
        }
        if before.is_none() {
            DELTA_NUMERIC_RANGE_CACHE.write().remove(delta_path);
        }

        let Ok(metadata) = std::fs::metadata(delta_path) else {
            return;
        };
        let file_len = metadata.len();
        let modified = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let epoch = crate::storage::epoch::current(table_path);

        {
            let mut cache = DELTA_ROW_COUNT_CACHE.write();
            if cache.len() > 128 {
                cache.clear();
            }
            match before {
                None => {
                    cache.insert(
                        delta_path.to_path_buf(),
                        (file_len, modified, ids.len(), epoch),
                    );
                }
                Some((before_len, before_modified)) => {
                    if let Some(entry) = cache.get_mut(delta_path) {
                        if entry.0 == before_len && entry.1 >= before_modified {
                            entry.0 = file_len;
                            entry.1 = modified;
                            entry.2 += ids.len();
                            entry.3 = epoch;
                        }
                    }
                }
            }
        }

        let append_strings =
            |index: &mut HashMap<String, HashMap<String, Vec<u64>>>| {
                for (column, values) in string_columns {
                    let value_index = index.entry(column.clone()).or_default();
                    for (row_idx, id) in ids.iter().copied().enumerate() {
                        if let Some(value) = values.get(row_idx) {
                            value_index.entry(value.clone()).or_default().push(id);
                        }
                    }
                }
            };

        let mut cache = DELTA_STRING_INDEX_CACHE.write();
        if cache.len() > 128 {
            cache.clear();
        }
        match before {
            None => {
                let mut index = HashMap::new();
                append_strings(&mut index);
                cache.insert(
                    delta_path.to_path_buf(),
                    DeltaStringIndexCache {
                        len: file_len,
                        modified,
                        epoch,
                        index,
                    },
                );
            }
            Some((before_len, before_modified)) => {
                if let Some(entry) = cache.get_mut(delta_path) {
                    if entry.len == before_len && entry.modified >= before_modified {
                        append_strings(&mut entry.index);
                        entry.len = file_len;
                        entry.modified = modified;
                        entry.epoch = epoch;
                    }
                }
            }
        }
    }

    /// Insert typed columns to delta file (memory efficient - doesn't load existing data)
    /// Returns the IDs assigned to the inserted rows
    fn insert_typed_to_delta(
        &self,
        int_columns: HashMap<String, Vec<i64>>,
        float_columns: HashMap<String, Vec<f64>>,
        string_columns: HashMap<String, Vec<String>>,
        _binary_columns: HashMap<String, Vec<Vec<u8>>>, // Not yet implemented in delta
        bool_columns: HashMap<String, Vec<bool>>,
    ) -> io::Result<Vec<u64>> {
        // Determine row count
        let row_count = int_columns
            .values()
            .map(|v| v.len())
            .max()
            .unwrap_or(0)
            .max(float_columns.values().map(|v| v.len()).max().unwrap_or(0))
            .max(string_columns.values().map(|v| v.len()).max().unwrap_or(0))
            .max(bool_columns.values().map(|v| v.len()).max().unwrap_or(0));

        if row_count == 0 {
            return Ok(Vec::new());
        }

        let delta_path = Self::delta_path(&self.path);

        // Allocate IDs
        let mut ids = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            ids.push(self.next_id.fetch_add(1, Ordering::SeqCst));
        }

        self.append_typed_to_delta_with_ids(
            &ids,
            &int_columns,
            &float_columns,
            &string_columns,
            &bool_columns,
        )?;
        Ok(ids)
    }

    fn discard_pending_v4_rows_from(&self, pending_start: usize) {
        let truncate_bitmap = |bitmap: &mut Vec<u8>, row_count: usize| {
            let new_len = (row_count + 7) / 8;
            bitmap.truncate(new_len);
            if row_count == 0 {
                bitmap.clear();
            } else if row_count % 8 != 0 {
                if let Some(last) = bitmap.last_mut() {
                    *last &= (1u8 << (row_count % 8)) - 1;
                }
            }
        };

        if pending_start == 0 {
            self.ids.write().clear();
            self.columns.write().clear();
            self.nulls.write().clear();
            self.deleted.write().clear();
        } else {
            self.ids.write().truncate(pending_start);
            {
                let mut columns = self.columns.write();
                for column in columns.iter_mut() {
                    *column = column.slice_range(0, pending_start);
                }
            }
            {
                let mut nulls = self.nulls.write();
                for bitmap in nulls.iter_mut() {
                    truncate_bitmap(bitmap, pending_start);
                }
            }
            {
                let mut deleted = self.deleted.write();
                truncate_bitmap(&mut deleted, pending_start);
            }
        }
        *self.id_to_idx.write() = None;
        self.pending_rows.store(0, Ordering::SeqCst);
    }

    /// Spill mmap-only V4 memtable rows to the delta sidecar instead of
    /// rewriting the base file. This keeps explicit `flush()` on small OLTP
    /// bursts fast while preserving cross-process visibility through the
    /// existing delta merge path.
    pub fn spill_pending_v4_rows_to_delta(&self) -> io::Result<bool> {
        if !self.is_v4_format() {
            return Ok(false);
        }

        let pending = self.pending_v4_in_memory_rows();
        if pending == 0 {
            return Ok(false);
        }

        let (on_disk_rows, footer_schema_columns) = {
            let footer_guard = self.v4_footer.read();
            let Some(footer) = footer_guard.as_ref() else {
                return Ok(false);
            };
            let rows = footer
                .row_groups
                .iter()
                .map(|rg| rg.row_count as usize)
                .sum();
            (rows, footer.schema.columns.clone())
        };
        if on_disk_rows == 0 {
            // New/empty tables must write the initial base file so schema/footer metadata
            // is persisted together with the first rows.
            return Ok(false);
        }

        let ids = self.ids.read();
        let ids_len = ids.len();
        if ids_len < pending {
            return Ok(false);
        }
        let pending_start = ids_len - pending;
        if ids.first().copied().unwrap_or(0) == 1 && pending_start < on_disk_rows {
            // Some persisted base rows are mixed into the in-memory prefix. Fall back to a
            // full save so we do not misclassify base rows as pending append-only rows.
            return Ok(false);
        }
        let deleted = self.deleted.read();
        if ids.first().copied().unwrap_or(0) == 1 {
            for row_idx in 0..pending_start.min(on_disk_rows) {
                let byte_idx = row_idx / 8;
                let bit_idx = row_idx % 8;
                if byte_idx < deleted.len() && ((deleted[byte_idx] >> bit_idx) & 1 == 1) {
                    // Persisted-base deletes require a full rewrite or delete-vector update.
                    return Ok(false);
                }
            }
        }
        if pending == 1 {
            let row_idx_abs = pending_start;
            let byte_idx = row_idx_abs / 8;
            let bit_idx = row_idx_abs % 8;
            let is_deleted =
                byte_idx < deleted.len() && ((deleted[byte_idx] >> bit_idx) & 1 == 1);
            if !is_deleted {
                let pending_id = ids[row_idx_abs];
                drop(ids);

                let schema = self.schema.read();
                let columns = self.columns.read();
                let nulls = self.nulls.read();
                if schema.columns != footer_schema_columns {
                    return Ok(false);
                }
                if columns.len() < schema.column_count() {
                    return Ok(false);
                }

                let mut int_columns: HashMap<String, Vec<i64>> =
                    HashMap::with_capacity(schema.column_count());
                let mut float_columns: HashMap<String, Vec<f64>> =
                    HashMap::with_capacity(schema.column_count());
                let mut string_columns: HashMap<String, Vec<String>> =
                    HashMap::with_capacity(schema.column_count());
                let mut bool_columns: HashMap<String, Vec<bool>> =
                    HashMap::with_capacity(schema.column_count());

                for (col_idx, (col_name, col_type)) in schema.columns.iter().enumerate() {
                    if let Some(bitmap) = nulls.get(col_idx) {
                        let byte_idx = row_idx_abs / 8;
                        let bit_idx = row_idx_abs % 8;
                        if byte_idx < bitmap.len() && (bitmap[byte_idx] >> bit_idx) & 1 == 1 {
                            return Ok(false);
                        }
                    }

                    match (&columns[col_idx], col_type) {
                        (
                            ColumnData::Int64(values),
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
                        ) => {
                            let Some(&value) = values.get(row_idx_abs) else {
                                return Ok(false);
                            };
                            int_columns.insert(col_name.clone(), vec![value]);
                        }
                        (ColumnData::Float64(values), ColumnType::Float64 | ColumnType::Float32) => {
                            let Some(&value) = values.get(row_idx_abs) else {
                                return Ok(false);
                            };
                            float_columns.insert(col_name.clone(), vec![value]);
                        }
                        (
                            ColumnData::String { offsets, data },
                            ColumnType::String | ColumnType::Null,
                        ) => {
                            let Some((&start, &end)) =
                                offsets.get(row_idx_abs).zip(offsets.get(row_idx_abs + 1))
                            else {
                                return Ok(false);
                            };
                            let start = start as usize;
                            let end = end as usize;
                            if start > end || end > data.len() {
                                return Ok(false);
                            }
                            string_columns.insert(
                                col_name.clone(),
                                vec![std::str::from_utf8(&data[start..end])
                                    .unwrap_or("")
                                    .to_string()],
                            );
                        }
                        (
                            ColumnData::StringDict {
                                indices,
                                dict_offsets,
                                dict_data,
                            },
                            ColumnType::StringDict,
                        ) => {
                            let Some(&dict_idx) = indices.get(row_idx_abs) else {
                                return Ok(false);
                            };
                            if dict_idx == 0 {
                                return Ok(false);
                            }
                            let di = (dict_idx - 1) as usize;
                            let Some((&start, &end)) =
                                dict_offsets.get(di).zip(dict_offsets.get(di + 1))
                            else {
                                return Ok(false);
                            };
                            let start = start as usize;
                            let end = end as usize;
                            if start > end || end > dict_data.len() {
                                return Ok(false);
                            }
                            string_columns.insert(
                                col_name.clone(),
                                vec![std::str::from_utf8(&dict_data[start..end])
                                    .unwrap_or("")
                                    .to_string()],
                            );
                        }
                        (ColumnData::Bool { data, len }, ColumnType::Bool) => {
                            if row_idx_abs >= *len {
                                return Ok(false);
                            }
                            let byte_idx = row_idx_abs / 8;
                            let bit_idx = row_idx_abs % 8;
                            let value =
                                byte_idx < data.len() && ((data[byte_idx] >> bit_idx) & 1 == 1);
                            bool_columns.insert(col_name.clone(), vec![value]);
                        }
                        _ => return Ok(false),
                    }
                }

                drop(deleted);
                drop(nulls);
                drop(columns);
                drop(schema);

                self.append_typed_to_delta_with_ids(
                    &[pending_id],
                    &int_columns,
                    &float_columns,
                    &string_columns,
                    &bool_columns,
                )?;
                self.discard_pending_v4_rows_from(pending_start);
                return Ok(true);
            }
        }
        let mut live_row_indices_abs = Vec::with_capacity(pending);
        let mut live_row_indices_local = Vec::with_capacity(pending);
        for row_idx in pending_start..ids_len {
            let byte_idx = row_idx / 8;
            let bit_idx = row_idx % 8;
            let is_deleted = byte_idx < deleted.len() && ((deleted[byte_idx] >> bit_idx) & 1 == 1);
            if !is_deleted {
                live_row_indices_abs.push(row_idx);
                live_row_indices_local.push(row_idx - pending_start);
            }
        }
        let pending_ids: Vec<u64> = live_row_indices_abs
            .iter()
            .map(|&row_idx| ids[row_idx])
            .collect();
        drop(ids);

        let schema = self.schema.read();
        let columns = self.columns.read();
        let nulls = self.nulls.read();
        if schema.columns != footer_schema_columns {
            // Schema evolution must go through the normal save path so the base footer and
            // column layout stay in sync with what readers expect on reopen.
            return Ok(false);
        }
        if columns.len() < schema.column_count() {
            return Ok(false);
        }

        let mut int_columns: HashMap<String, Vec<i64>> = HashMap::new();
        let mut float_columns: HashMap<String, Vec<f64>> = HashMap::new();
        let mut string_columns: HashMap<String, Vec<String>> = HashMap::new();
        let mut bool_columns: HashMap<String, Vec<bool>> = HashMap::new();

        for (col_idx, (col_name, col_type)) in schema.columns.iter().enumerate() {
            if let Some(bitmap) = nulls.get(col_idx) {
                for &row_idx in &live_row_indices_abs {
                    let byte_idx = row_idx / 8;
                    let bit_idx = row_idx % 8;
                    if byte_idx < bitmap.len() && (bitmap[byte_idx] >> bit_idx) & 1 == 1 {
                        return Ok(false);
                    }
                }
            }

            let sliced = columns[col_idx].slice_range(pending_start, ids_len);
            match col_type {
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
                    let ColumnData::Int64(values) = sliced else {
                        return Ok(false);
                    };
                    if live_row_indices_local
                        .iter()
                        .any(|&row_idx| row_idx >= values.len())
                    {
                        return Ok(false);
                    }
                    let filtered: Vec<i64> = live_row_indices_local
                        .iter()
                        .map(|&row_idx| values[row_idx])
                        .collect();
                    int_columns.insert(col_name.clone(), filtered);
                }
                ColumnType::Float64 | ColumnType::Float32 => {
                    let ColumnData::Float64(values) = sliced else {
                        return Ok(false);
                    };
                    if live_row_indices_local
                        .iter()
                        .any(|&row_idx| row_idx >= values.len())
                    {
                        return Ok(false);
                    }
                    let filtered: Vec<f64> = live_row_indices_local
                        .iter()
                        .map(|&row_idx| values[row_idx])
                        .collect();
                    float_columns.insert(col_name.clone(), filtered);
                }
                ColumnType::String | ColumnType::StringDict | ColumnType::Null => {
                    let normalized = if matches!(sliced, ColumnData::StringDict { .. }) {
                        sliced.decode_string_dict()
                    } else {
                        sliced
                    };
                    let ColumnData::String { offsets, data } = normalized else {
                        return Ok(false);
                    };
                    let mut values = Vec::with_capacity(live_row_indices_local.len());
                    for &row_idx in &live_row_indices_local {
                        if row_idx + 1 >= offsets.len() {
                            return Ok(false);
                        }
                        let start = offsets[row_idx] as usize;
                        let end = offsets[row_idx + 1] as usize;
                        if start > end || end > data.len() {
                            return Ok(false);
                        }
                        values.push(
                            std::str::from_utf8(&data[start..end])
                                .unwrap_or("")
                                .to_string(),
                        );
                    }
                    string_columns.insert(col_name.clone(), values);
                }
                ColumnType::Bool => {
                    let ColumnData::Bool { data, len } = sliced else {
                        return Ok(false);
                    };
                    let mut values = Vec::with_capacity(live_row_indices_local.len());
                    for &row_idx in &live_row_indices_local {
                        if row_idx >= len {
                            return Ok(false);
                        }
                        let byte_idx = row_idx / 8;
                        let bit_idx = row_idx % 8;
                        let value = byte_idx < data.len() && ((data[byte_idx] >> bit_idx) & 1 == 1);
                        values.push(value);
                    }
                    bool_columns.insert(col_name.clone(), values);
                }
                ColumnType::Binary
                | ColumnType::Blob
                | ColumnType::FixedList
                | ColumnType::Float16List => {
                    return Ok(false);
                }
            }
        }

        drop(deleted);
        drop(nulls);
        drop(columns);
        drop(schema);

        if !pending_ids.is_empty() {
            self.append_typed_to_delta_with_ids(
                &pending_ids,
                &int_columns,
                &float_columns,
                &string_columns,
                &bool_columns,
            )?;
        }
        self.discard_pending_v4_rows_from(pending_start);
        Ok(true)
    }

    /// Compact: merge delta file into base file.
    ///
    /// For mmap-only V4 backends (the production path) this uses
    /// `compact_streaming_v4`, which merges Row Group by Row Group via mmap so
    /// peak memory is O(largest Row Group + delta payloads) instead of the whole
    /// table. The legacy in-memory path is retained for backends that already
    /// have the base materialized in memory (tests / legacy open_for_write).
    pub fn compact(&self) -> io::Result<()> {
        let delta_path = Self::delta_path(&self.path);
        if !delta_path.exists() {
            return Ok(());
        }

        let header = self.header.read();
        let is_v4 = header.version == FORMAT_VERSION_V4 && header.footer_offset > 0;
        let base_loaded = self.v4_base_loaded.load(Ordering::SeqCst);
        drop(header);

        if is_v4 && !base_loaded {
            return self.compact_streaming_v4();
        }

        // Legacy in-memory path (base already loaded)
        self.load_all_columns_into_memory()?;
        self.merge_delta_file(&delta_path)?;
        self.save()?;

        // Delete delta file
        *self.delta_file.write() = None;
        let _ = std::fs::remove_file(&delta_path);
        let _ = std::fs::remove_file(Self::delta_meta_path(&delta_path));
        DELTA_NUMERIC_RANGE_CACHE.write().remove(&delta_path);

        Ok(())
    }

    /// Streaming V4 compaction (bounded memory).
    ///
    /// Merges the append-only delta file (`.apex.delta`) and the DeltaStore
    /// (updates/deletes) into the base V4 file Row Group by Row Group via mmap.
    /// Base data is never loaded wholesale: peak memory is O(largest Row Group +
    /// delta payloads), so tables larger than physical memory can be compacted.
    /// The output is written to `.apex.tmp` and atomically renamed over the
    /// original, mirroring the `save_v4` atomic-write protocol.
    pub fn compact_streaming_v4(&self) -> io::Result<()> {
        self.stream_rewrite_v4(true, false)
    }

    /// Streaming rewrite that drops rows marked deleted (in-memory `deleted`
    /// bitmap) plus any pending DeltaStore/delta-file changes. Used by the
    /// compressed-Row-Group delete path so deletes never require loading the
    /// whole table into memory.
    pub fn rewrite_v4_active_rows(&self) -> io::Result<()> {
        self.stream_rewrite_v4(false, true)
    }

    /// Shared streaming V4 rewrite engine: merges the append-only delta file
    /// (when present), DeltaStore updates/deletes, and optionally the
    /// in-memory deletion bitmap, Row Group by Row Group via mmap.
    fn stream_rewrite_v4(&self, require_delta: bool, filter_mem_deletes: bool) -> io::Result<()> {
        let header = self.header.read();
        if header.version != FORMAT_VERSION_V4 || header.footer_offset == 0 {
            return Err(err_data("streaming V4 rewrite requires a V4 base file"));
        }
        let rg_size_target = (header.row_group_size as usize).max(1024);
        drop(header);

        let delta_path = Self::delta_path(&self.path);
        let has_delta = delta_path.exists();
        if require_delta && !has_delta {
            return Ok(());
        }
        let tmp_path = self.path.with_extension("apex.tmp");

        // Apply any deferred delete state first (idempotent).
        let _ = apply_pending_deletes(&self.path);

        // Bounded delta payloads: the append-only delta file is capped by
        // DELTA_COMPACT_SIZE / DELTA_COMPACT_ROWS; the DeltaStore is bounded by
        // update/delete volume.
        let delta_data = if has_delta {
            self.read_delta_data()?
        } else {
            None
        };
        let (all_updates, delete_bitmap) = {
            let ds = self.delta_store.read();
            (ds.all_updates().clone(), ds.delete_bitmap().clone())
        };
        let delta_ids: Vec<u64> = delta_data
            .as_ref()
            .map(|(ids, _)| ids.clone())
            .unwrap_or_default();
        // Footer must be loaded before acquiring mmap_cache.write() below
        // (get_or_load_footer takes the mmap write lock internally).
        let footer = self
            .get_or_load_footer()?
            .ok_or_else(|| err_data("V4 footer missing"))?;
        let base_schema = footer.schema.clone();
        let base_col_count = base_schema.column_count();

        // Merged schema: base footer schema + in-memory schema columns
        // (footer-only ALTER TABLE ADD COLUMN may not have been persisted yet)
        // + delta-only columns.
        let mut schema = base_schema.clone();
        {
            let mem_schema = self.schema.read();
            for (name, ct) in &mem_schema.columns {
                if schema.get_index(name).is_none() {
                    schema.add_column(name, *ct);
                }
            }
        }
        if let Some((_, delta_cols)) = &delta_data {
            for (name, col) in delta_cols {
                if schema.get_index(name).is_none() {
                    schema.add_column(name, col.column_type());
                }
            }
        }
        let schema_cols: Vec<(String, ColumnType)> = schema.columns.clone();
        let col_count = schema.column_count();

        // Output RG buffers (one Row Group worth at a time). String columns are
        // kept as plain String; dict encoding is decided at write time.
        let mut out_ids: Vec<u64> = Vec::new();
        let mut out_columns: Vec<ColumnData> = schema_cols
            .iter()
            .map(|(_, ct)| {
                if matches!(ct, ColumnType::StringDict) {
                    ColumnData::new(ColumnType::String)
                } else {
                    ColumnData::new(*ct)
                }
            })
            .collect();
        let mut out_nulls: Vec<Vec<u8>> = vec![Vec::new(); col_count];

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open"))?;
        let mut mmap = self.mmap_cache.write();

        let tmp_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut writer = std::io::BufWriter::with_capacity(256 * 1024, tmp_file);
        writer.write_all(&[0u8; HEADER_SIZE])?;

        let mut rg_metas: Vec<RowGroupMeta> = Vec::new();
        let mut all_zone_maps: RgZoneMaps = Vec::new();
        let mut all_rg_col_offsets: Vec<Vec<u32>> = Vec::new();
        let mut actual_col_types: Vec<ColumnType> = Vec::new();
        let mut max_id_seen: u64 = 0;
        let mut written_rows: u64 = 0;
        let mut first_rg = true;
        let mut out_row_count: usize = 0;
        let implicit_ids_ok = self
            .path
            .components()
            .any(|part| part.as_os_str() == std::ffi::OsStr::new(".apex_tmp"));
        let compression = self.compression();

        macro_rules! flush_out_rg {
            () => {
                flush_streamed_out_rg(
                    &mut writer,
                    &schema_cols,
                    &mut out_ids,
                    &mut out_columns,
                    &mut out_nulls,
                    &mut first_rg,
                    &mut actual_col_types,
                    &mut rg_metas,
                    &mut all_zone_maps,
                    &mut all_rg_col_offsets,
                    compression,
                    implicit_ids_ok,
                    &mut written_rows,
                )?;
                out_row_count = out_ids.len();
            };
        }

        // Merge base Row Groups one at a time.
        for rg_meta in &footer.row_groups {
            if rg_meta.row_count == 0 {
                continue;
            }
            let rg_size = rg_meta.data_size as usize;
            let mut rg_buf = vec![0u8; rg_size];
            mmap.read_at(file, &mut rg_buf, rg_meta.offset)?;
            let parsed =
                parse_v4_row_group(&rg_buf, rg_meta, base_col_count, &base_schema.columns)?;

            for row in 0..parsed.ids.len() {
                let id = parsed.ids[row];
                let mut is_deleted = ((parsed.deleted[row / 8] >> (row % 8)) & 1) == 1
                    || delete_bitmap.is_deleted(id);
                if !is_deleted && filter_mem_deletes {
                    if let Some(map) = self.id_to_idx.read().as_ref() {
                        if let Some(&idx) = map.get(&id) {
                            let db = self.deleted.read();
                            is_deleted = (db[idx / 8] >> (idx % 8)) & 1 == 1;
                        }
                    }
                }
                if is_deleted {
                    continue;
                }
                if id > max_id_seen {
                    max_id_seen = id;
                }
                let updates = all_updates.get(&id);
                for col_idx in 0..col_count {
                    if col_idx < base_col_count {
                        let is_null =
                            ((parsed.nulls[col_idx][row / 8] >> (row % 8)) & 1) == 1;
                        let col_name = &schema_cols[col_idx].0;
                        let mut applied_update = false;
                        if let Some(upd) = updates {
                            if let Some(rec) = upd.get(col_name) {
                                if try_push_updated_value(&mut out_columns[col_idx], &rec.new_value)
                                    .is_some()
                                {
                                    set_null_bit(&mut out_nulls[col_idx], out_row_count, false);
                                    applied_update = true;
                                }
                            }
                        }
                        if !applied_update {
                            push_source_value(
                                &mut out_columns[col_idx],
                                &parsed.columns[col_idx],
                                row,
                                is_null,
                            );
                            if is_null {
                                set_null_bit(&mut out_nulls[col_idx], out_row_count, true);
                            }
                        }
                    } else {
                        // Column added after these rows were written: pad with default.
                        push_default_value(&mut out_columns[col_idx]);
                    }
                }
                out_ids.push(id);
                out_row_count += 1;
                if out_row_count >= rg_size_target {
                    flush_out_rg!();
                }
            }
        }
        flush_out_rg!();

        // Merge appended delta rows (fresh IDs that do not collide with base).
        if has_delta {
        if let Some((delta_ids_all, delta_cols)) = &delta_data {
            for (delta_row, &id) in delta_ids_all.iter().enumerate() {
                if delete_bitmap.is_deleted(id) {
                    continue;
                }
                if id > max_id_seen {
                    max_id_seen = id;
                }
                let updates = all_updates.get(&id);
                for col_idx in 0..col_count {
                    let (name, _) = &schema_cols[col_idx];
                    let src_col = delta_cols.get(name);
                    let mut applied_update = false;
                    if let Some(upd) = updates {
                        if let Some(rec) = upd.get(name) {
                            if try_push_updated_value(&mut out_columns[col_idx], &rec.new_value)
                                .is_some()
                            {
                                set_null_bit(&mut out_nulls[col_idx], out_row_count, false);
                                applied_update = true;
                            }
                        }
                    }
                    if !applied_update {
                        match src_col {
                            Some(col) if delta_row < col.len() => {
                                push_source_value(
                                    &mut out_columns[col_idx],
                                    col,
                                    delta_row,
                                    false,
                                );
                            }
                            _ => push_default_value(&mut out_columns[col_idx]),
                        }
                    }
                }
                out_ids.push(id);
                out_row_count += 1;
                if out_row_count >= rg_size_target {
                    flush_out_rg!();
                }
            }
        }
        }
        flush_out_rg!();

        // Release the source mmap before touching mmap_cache again (invalidation
        // below would otherwise deadlock on the write lock).
        drop(mmap);
        drop(file_guard);

        // Empty output: write a single bootstrap Row Group (mirrors save_v4).
        if rg_metas.is_empty() {
            Self::write_streamed_v4_row_group(
                &mut writer,
                &schema_cols,
                &[],
                &[],
                &[],
                0,
                0,
                true,
                &mut actual_col_types,
                &mut rg_metas,
                &mut all_zone_maps,
                &mut all_rg_col_offsets,
                compression,
                implicit_ids_ok,
            )?;
        }

        // Build the modified schema with actual types (StringDict if dict-encoded).
        let modified_schema = if !actual_col_types.is_empty() {
            let mut ms = OnDemandSchema::new();
            for (col_idx, (name, _)) in schema_cols.iter().enumerate() {
                ms.add_column(name, actual_col_types[col_idx]);
            }
            ms.constraints = schema.constraints.clone();
            ms
        } else {
            schema.clone()
        };

        // Write footer + trailer, then fix the header.
        let footer_offset = writer.stream_position()?;
        let final_footer = V4Footer {
            schema: modified_schema,
            row_groups: rg_metas.clone(),
            zone_maps: all_zone_maps,
            col_offsets: all_rg_col_offsets,
        };
        writer.write_all(&final_footer.to_bytes())?;
        writer.flush()?;
        {
            let mut header = self.header.write();
            header.version = FORMAT_VERSION_V4;
            header.row_count = written_rows;
            header.column_count = col_count as u32;
            header.footer_offset = footer_offset;
            header.row_group_count = rg_metas.len() as u32;
            header.schema_offset = 0;
            header.column_index_offset = 0;
            header.id_column_offset = 0;
        }
        self.cached_footer_offset
            .store(footer_offset, Ordering::Release);
        let header = self.header.read();
        let writer_inner = writer.get_mut();
        writer_inner.seek(std::io::SeekFrom::Start(0))?;
        writer_inner.write_all(&header.to_bytes())?;
        writer_inner.flush()?;
        if self.durability != super::DurabilityLevel::Fast {
            writer_inner.sync_all()?;
        }
        drop(header);
        drop(writer);

        // Atomic tmp + rename (crash leaves the original file intact).
        #[cfg(windows)]
        {
            let mut last_err = None;
            for attempt in 0u64..5 {
                match std::fs::rename(&tmp_path, &self.path) {
                    Ok(()) => {
                        last_err = None;
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        if attempt < 4 {
                            std::thread::sleep(std::time::Duration::from_millis(
                                10 * (attempt + 1),
                            ));
                        }
                    }
                }
            }
            if let Some(e) = last_err {
                return Err(e);
            }
        }
        #[cfg(not(windows))]
        std::fs::rename(&tmp_path, &self.path)?;

        // Drop delta artifacts; deltas are now baked into the base file.
        *self.delta_file.write() = None;
        if has_delta {
            let _ = std::fs::remove_file(&delta_path);
            let _ = std::fs::remove_file(Self::delta_meta_path(&delta_path));
            DELTA_NUMERIC_RANGE_CACHE.write().remove(&delta_path);
            DELTA_ROW_COUNT_CACHE.write().remove(&delta_path);
            DELTA_STRING_INDEX_CACHE.write().remove(&delta_path);
        }
        self.delete_col_stats_sidecar()?;
        self.clear_delta_store()?;

        // Restore mmap-only in-memory state (empty write buffer, updated counts).
        *self.column_index.write() = Vec::new();
        *self.ids.write() = Vec::new();
        *self.columns.write() = schema_cols
            .iter()
            .map(|(_, ct)| {
                if matches!(ct, ColumnType::StringDict) {
                    ColumnData::new(ColumnType::String)
                } else {
                    ColumnData::new(*ct)
                }
            })
            .collect();
        *self.nulls.write() = vec![Vec::new(); col_count];
        *self.deleted.write() = Vec::new();
        *self.id_to_idx.write() = None;
        self.mmap_cache.write().invalidate();
        self.invalidate_page_cache();
        self.active_count.store(written_rows, Ordering::SeqCst);
        self.persisted_row_count
            .store(written_rows, Ordering::SeqCst);
        self.v4_base_loaded.store(false, Ordering::SeqCst);
        let candidate = max_id_seen.saturating_add(1);
        let current = self.next_id.load(Ordering::SeqCst);
        if candidate > current {
            self.next_id.store(candidate, Ordering::SeqCst);
        }
        *self.v4_footer.write() = Some(final_footer);

        let file = open_for_sequential_read(&self.path)?;
        *self.file.write() = Some(file);

        if self.durability == super::DurabilityLevel::Fast {
            self.mark_main_sync_pending();
        } else {
            self.sync_main_file_data()?;
            self.clear_main_sync_pending();
        }

        Ok(())
    }

    /// Serialize one Row Group (from in-memory columns) into a fresh V4 writer.
    /// Mirrors the `save_v4` per-RG serialization: dict encoding on the first
    /// RG, per-RG zone maps, RCIX offsets, optional body compression, and the
    /// 32-byte RG header. Shared by the streaming compaction path.
    #[allow(clippy::too_many_arguments)]
    fn write_streamed_v4_row_group<W: std::io::Write + std::io::Seek>(
        writer: &mut W,
        schema_cols: &[(String, ColumnType)],
        ids: &[u64],
        columns: &[ColumnData],
        nulls: &[Vec<u8>],
        chunk_start: usize,
        chunk_end: usize,
        is_first_rg: bool,
        actual_col_types: &mut Vec<ColumnType>,
        rg_metas: &mut Vec<RowGroupMeta>,
        all_zone_maps: &mut RgZoneMaps,
        all_rg_col_offsets: &mut Vec<Vec<u32>>,
        compression: CompressionType,
        implicit_ids_ok: bool,
    ) -> io::Result<()> {
        let chunk_rows = chunk_end - chunk_start;
        let col_count = schema_cols.len();
        let rg_offset = writer.stream_position()?;

        if chunk_rows == 0 {
            // Empty bootstrap Row Group (empty table).
            writer.write_all(MAGIC_ROW_GROUP)?;
            writer.write_all(&0u32.to_le_bytes())?;
            writer.write_all(&(col_count as u32).to_le_bytes())?;
            writer.write_all(&0u64.to_le_bytes())?;
            writer.write_all(&0u64.to_le_bytes())?;
            writer.write_all(&[RG_COMPRESS_NONE, 1, RG_IDS_PLAIN, 0])?;
            let rg_end = writer.stream_position()?;
            rg_metas.push(RowGroupMeta {
                offset: rg_offset,
                data_size: rg_end - rg_offset,
                row_count: 0,
                min_id: 0,
                max_id: 0,
                deletion_count: 0,
            });
            return Ok(());
        }

        let chunk_ids = &ids[chunk_start..chunk_end];
        let min_id = chunk_ids.iter().copied().min().unwrap_or(0);
        let max_id = chunk_ids.iter().copied().max().unwrap_or(0);
        let id_encoding = if implicit_ids_ok && ids_are_contiguous(chunk_ids) {
            RG_IDS_IMPLICIT_CONTIGUOUS
        } else {
            RG_IDS_PLAIN
        };
        let id_section_len = rg_id_section_len(chunk_rows, id_encoding);
        let null_bitmap_len = (chunk_rows + 7) / 8;
        let mut body_buf: Vec<u8> = Vec::with_capacity(id_section_len + chunk_rows * col_count);
        {
            let mut bw = std::io::Cursor::new(&mut body_buf);
            if id_encoding == RG_IDS_PLAIN {
                let id_bytes = unsafe {
                    std::slice::from_raw_parts(
                        chunk_ids.as_ptr() as *const u8,
                        chunk_ids.len() * 8,
                    )
                };
                bw.write_all(id_bytes)?;
            }
            // Fresh compaction output has no deleted rows.
            bw.write_all(&vec![0u8; null_bitmap_len])?;
            let mut rg_col_offsets: Vec<u32> = Vec::with_capacity(col_count);
            for col_idx in 0..col_count {
                let chunk_col = columns[col_idx].slice_range(chunk_start, chunk_end);
                let dict_encoded = if is_first_rg {
                    Self::dict_encode_if_smaller(&chunk_col)
                } else if matches!(actual_col_types.get(col_idx), Some(ColumnType::StringDict)) {
                    chunk_col.to_dict_encoded()
                } else {
                    None
                };
                let processed = dict_encoded.as_ref().unwrap_or(&chunk_col);
                if is_first_rg {
                    let actual_type = match processed {
                        ColumnData::StringDict { .. } => ColumnType::StringDict,
                        _ => schema_cols[col_idx].1,
                    };
                    actual_col_types.push(actual_type);
                }
                rg_col_offsets.push(bw.position() as u32);
                let chunk_nulls =
                    Self::slice_null_bitmap(&nulls[col_idx], chunk_start, chunk_end);
                bw.write_all(&chunk_nulls)?;
                write_column_encoded(processed, schema_cols[col_idx].1, &mut bw)?;
            }
            all_rg_col_offsets.push(rg_col_offsets);
        }

        let (compress_flag, disk_body) = compress_rg_body(body_buf, compression);
        writer.write_all(MAGIC_ROW_GROUP)?;
        writer.write_all(&(chunk_rows as u32).to_le_bytes())?;
        writer.write_all(&(col_count as u32).to_le_bytes())?;
        writer.write_all(&min_id.to_le_bytes())?;
        writer.write_all(&max_id.to_le_bytes())?;
        writer.write_all(&[compress_flag, 1, id_encoding, 0])?;
        writer.write_all(&disk_body)?;

        // Per-RG zone maps (numeric min/max; string byte-length min/max).
        let mut rg_zmaps: Vec<RgColumnZoneMap> = Vec::new();
        for col_idx in 0..col_count {
            match &columns[col_idx] {
                ColumnData::Int64(data) => {
                    let slice = &data[chunk_start..chunk_end];
                    if !slice.is_empty() {
                        let (mut mn, mut mx) = (i64::MAX, i64::MIN);
                        for &v in slice {
                            mn = mn.min(v);
                            mx = mx.max(v);
                        }
                        rg_zmaps.push(RgColumnZoneMap {
                            col_idx: col_idx as u16,
                            min_bits: mn,
                            max_bits: mx,
                            has_nulls: false,
                            is_float: false,
                        });
                    }
                }
                ColumnData::Float64(data) => {
                    let slice = &data[chunk_start..chunk_end];
                    if !slice.is_empty() {
                        let (mut mn, mut mx) = (f64::INFINITY, f64::NEG_INFINITY);
                        for &v in slice {
                            if v < mn {
                                mn = v;
                            }
                            if v > mx {
                                mx = v;
                            }
                        }
                        rg_zmaps.push(RgColumnZoneMap {
                            col_idx: col_idx as u16,
                            min_bits: mn.to_bits() as i64,
                            max_bits: mx.to_bits() as i64,
                            has_nulls: false,
                            is_float: true,
                        });
                    }
                }
                ColumnData::String { offsets, .. } => {
                    let end_idx = chunk_end.min(offsets.len().saturating_sub(1));
                    if end_idx > chunk_start {
                        let (mut mn, mut mx) = (u64::MAX, 0u64);
                        for i in chunk_start..end_idx {
                            let len = offsets[i + 1].saturating_sub(offsets[i]);
                            if len < mn {
                                mn = len;
                            }
                            if len > mx {
                                mx = len;
                            }
                        }
                        if mn <= mx {
                            rg_zmaps.push(RgColumnZoneMap {
                                col_idx: col_idx as u16,
                                min_bits: mn as i64,
                                max_bits: mx as i64,
                                has_nulls: false,
                                is_float: false,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
        all_zone_maps.push(rg_zmaps);

        let rg_end = writer.stream_position()?;
        rg_metas.push(RowGroupMeta {
            offset: rg_offset,
            data_size: rg_end - rg_offset,
            row_count: chunk_rows as u32,
            min_id,
            max_id,
            deletion_count: 0,
        });
        Ok(())
    }

    /// Streaming physical rewrite that drops columns from the V4 base file
    /// without loading the table into memory.
    ///
    /// `drop_column` is a logical schema change; because RG column layout is
    /// positional, dropped columns must be physically removed from every Row
    /// Group. This rewrites RG-by-RG via mmap (peak memory O(RG size)) and
    /// atomically replaces the file, then updates the footer schema from the
    /// current in-memory schema (which `drop_column` already updated).
    pub fn rewrite_v4_drop_columns(&self) -> io::Result<()> {
        let header = self.header.read();
        if header.version != FORMAT_VERSION_V4 || header.footer_offset == 0 {
            return Err(err_data("rewrite_v4_drop_columns requires a V4 base file"));
        }
        let rg_size_target = (header.row_group_size as usize).max(1024);
        drop(header);

        let tmp_path = self.path.with_extension("apex.tmp");
        let footer = self
            .get_or_load_footer()?
            .ok_or_else(|| err_data("V4 footer missing"))?;
        let old_schema = footer.schema.clone();
        let old_col_count = old_schema.column_count();

        // New schema: current in-memory schema (already had the column removed).
        let new_schema = self.schema.read().clone();
        if new_schema.column_count() >= old_col_count {
            return Ok(());
        }
        // Map old column index -> kept output index (old order, minus dropped).
        let mut kept_cols: Vec<usize> = Vec::new();
        for (i, (name, _)) in old_schema.columns.iter().enumerate() {
            if new_schema.get_index(name).is_some() {
                kept_cols.push(i);
            }
        }
        let new_col_count = new_schema.column_count();
        let new_schema_cols: Vec<(String, ColumnType)> = new_schema.columns.clone();

        let mut out_ids: Vec<u64> = Vec::new();
        let mut out_columns: Vec<ColumnData> = new_schema_cols
            .iter()
            .map(|(_, ct)| {
                if matches!(ct, ColumnType::StringDict) {
                    ColumnData::new(ColumnType::String)
                } else {
                    ColumnData::new(*ct)
                }
            })
            .collect();
        let mut out_nulls: Vec<Vec<u8>> = vec![Vec::new(); new_col_count];

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open"))?;
        let mut mmap = self.mmap_cache.write();

        let tmp_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut writer = std::io::BufWriter::with_capacity(256 * 1024, tmp_file);
        writer.write_all(&[0u8; HEADER_SIZE])?;

        let mut rg_metas: Vec<RowGroupMeta> = Vec::new();
        let mut all_zone_maps: RgZoneMaps = Vec::new();
        let mut all_rg_col_offsets: Vec<Vec<u32>> = Vec::new();
        let mut actual_col_types: Vec<ColumnType> = Vec::new();
        let mut max_id_seen: u64 = 0;
        let mut written_rows: u64 = 0;
        let mut first_rg = true;
        let mut out_row_count: usize = 0;
        let implicit_ids_ok = self
            .path
            .components()
            .any(|part| part.as_os_str() == std::ffi::OsStr::new(".apex_tmp"));
        let compression = self.compression();

        macro_rules! flush_out_rg {
            () => {
                flush_streamed_out_rg(
                    &mut writer,
                    &new_schema_cols,
                    &mut out_ids,
                    &mut out_columns,
                    &mut out_nulls,
                    &mut first_rg,
                    &mut actual_col_types,
                    &mut rg_metas,
                    &mut all_zone_maps,
                    &mut all_rg_col_offsets,
                    compression,
                    implicit_ids_ok,
                    &mut written_rows,
                )?;
                out_row_count = out_ids.len();
            };
        }

        for rg_meta in &footer.row_groups {
            if rg_meta.row_count == 0 {
                continue;
            }
            let rg_size = rg_meta.data_size as usize;
            let mut rg_buf = vec![0u8; rg_size];
            mmap.read_at(file, &mut rg_buf, rg_meta.offset)?;
            let parsed = parse_v4_row_group(&rg_buf, rg_meta, old_col_count, &old_schema.columns)?;

            for row in 0..parsed.ids.len() {
                let id = parsed.ids[row];
                if ((parsed.deleted[row / 8] >> (row % 8)) & 1) == 1 {
                    continue;
                }
                if id > max_id_seen {
                    max_id_seen = id;
                }
                for (out_idx, &src_idx) in kept_cols.iter().enumerate() {
                    let is_null =
                        ((parsed.nulls[src_idx][row / 8] >> (row % 8)) & 1) == 1;
                    push_source_value(
                        &mut out_columns[out_idx],
                        &parsed.columns[src_idx],
                        row,
                        is_null,
                    );
                    if is_null {
                        set_null_bit(&mut out_nulls[out_idx], out_row_count, true);
                    }
                }
                out_ids.push(id);
                out_row_count += 1;
                if out_row_count >= rg_size_target {
                    flush_out_rg!();
                }
            }
        }
        flush_out_rg!();

        drop(mmap);
        drop(file_guard);

        if rg_metas.is_empty() {
            Self::write_streamed_v4_row_group(
                &mut writer,
                &new_schema_cols,
                &[],
                &[],
                &[],
                0,
                0,
                true,
                &mut actual_col_types,
                &mut rg_metas,
                &mut all_zone_maps,
                &mut all_rg_col_offsets,
                compression,
                implicit_ids_ok,
            )?;
        }

        let modified_schema = if !actual_col_types.is_empty() {
            let mut ms = OnDemandSchema::new();
            for (col_idx, (name, _)) in new_schema_cols.iter().enumerate() {
                ms.add_column(name, actual_col_types[col_idx]);
            }
            ms.constraints = new_schema.constraints.clone();
            ms
        } else {
            new_schema.clone()
        };

        let footer_offset = writer.stream_position()?;
        let final_footer = V4Footer {
            schema: modified_schema,
            row_groups: rg_metas.clone(),
            zone_maps: all_zone_maps,
            col_offsets: all_rg_col_offsets,
        };
        writer.write_all(&final_footer.to_bytes())?;
        writer.flush()?;
        {
            let mut header = self.header.write();
            header.version = FORMAT_VERSION_V4;
            header.row_count = written_rows;
            header.column_count = new_col_count as u32;
            header.footer_offset = footer_offset;
            header.row_group_count = rg_metas.len() as u32;
            header.schema_offset = 0;
            header.column_index_offset = 0;
            header.id_column_offset = 0;
        }
        self.cached_footer_offset
            .store(footer_offset, Ordering::Release);
        let header = self.header.read();
        let writer_inner = writer.get_mut();
        writer_inner.seek(std::io::SeekFrom::Start(0))?;
        writer_inner.write_all(&header.to_bytes())?;
        writer_inner.flush()?;
        if self.durability != super::DurabilityLevel::Fast {
            writer_inner.sync_all()?;
        }
        drop(header);
        drop(writer);

        #[cfg(windows)]
        {
            let mut last_err = None;
            for attempt in 0u64..5 {
                match std::fs::rename(&tmp_path, &self.path) {
                    Ok(()) => {
                        last_err = None;
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        if attempt < 4 {
                            std::thread::sleep(std::time::Duration::from_millis(
                                10 * (attempt + 1),
                            ));
                        }
                    }
                }
            }
            if let Some(e) = last_err {
                return Err(e);
            }
        }
        #[cfg(not(windows))]
        std::fs::rename(&tmp_path, &self.path)?;

        self.delete_col_stats_sidecar()?;
        *self.column_index.write() = Vec::new();
        *self.ids.write() = Vec::new();
        *self.columns.write() = new_schema_cols
            .iter()
            .map(|(_, ct)| {
                if matches!(ct, ColumnType::StringDict) {
                    ColumnData::new(ColumnType::String)
                } else {
                    ColumnData::new(*ct)
                }
            })
            .collect();
        *self.nulls.write() = vec![Vec::new(); new_col_count];
        *self.deleted.write() = Vec::new();
        *self.id_to_idx.write() = None;
        self.mmap_cache.write().invalidate();
        self.invalidate_page_cache();
        self.active_count.store(written_rows, Ordering::SeqCst);
        self.persisted_row_count
            .store(written_rows, Ordering::SeqCst);
        self.v4_base_loaded.store(false, Ordering::SeqCst);
        let candidate = max_id_seen.saturating_add(1);
        let current = self.next_id.load(Ordering::SeqCst);
        if candidate > current {
            self.next_id.store(candidate, Ordering::SeqCst);
        }
        *self.v4_footer.write() = Some(final_footer);

        let file = open_for_sequential_read(&self.path)?;
        *self.file.write() = Some(file);

        if self.durability == super::DurabilityLevel::Fast {
            self.mark_main_sync_pending();
        } else {
            self.sync_main_file_data()?;
            self.clear_main_sync_pending();
        }

        Ok(())
    }


    /// Convert an Arrow ArrayRef to ColumnData, preserving nulls.
    fn arrow_array_to_column_data(array: &dyn arrow::array::Array) -> ColumnData {
        use arrow::array::{
            Array, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
        };
        use arrow::datatypes::DataType as ArrowDT;
        match array.data_type() {
            ArrowDT::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                ColumnData::Int64(arr.values().to_vec())
            }
            ArrowDT::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                ColumnData::Float64(arr.values().to_vec())
            }
            ArrowDT::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                let mut offsets = Vec::with_capacity(arr.len() + 1);
                let mut data = Vec::new();
                offsets.push(0u64);
                for j in 0..arr.len() {
                    if arr.is_null(j) {
                        offsets.push(data.len() as u64);
                    } else {
                        let s = arr.value(j).as_bytes();
                        data.extend_from_slice(s);
                        offsets.push(data.len() as u64);
                    }
                }
                ColumnData::String { offsets, data }
            }
            ArrowDT::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                let n = arr.len();
                let byte_len = (n + 7) / 8;
                let mut bits = vec![0u8; byte_len];
                for j in 0..n {
                    if !arr.is_null(j) && arr.value(j) {
                        bits[j / 8] |= 1 << (j % 8);
                    }
                }
                ColumnData::Bool { data: bits, len: n }
            }
            ArrowDT::Binary => {
                let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                let mut offsets = Vec::with_capacity(arr.len() + 1);
                let mut data = Vec::new();
                offsets.push(0u64);
                for j in 0..arr.len() {
                    if arr.is_null(j) {
                        offsets.push(data.len() as u64);
                    } else {
                        data.extend_from_slice(arr.value(j));
                        offsets.push(data.len() as u64);
                    }
                }
                ColumnData::Binary { offsets, data }
            }
            _ => ColumnData::new(ColumnType::Int64),
        }
    }

    /// Create a column filled with default values (0, 0.0, "", false).
    /// Used for columns added via ALTER TABLE that have no disk data yet.
    fn create_default_column(dtype: ColumnType, count: usize) -> ColumnData {
        if count == 0 {
            return ColumnData::new(dtype);
        }
        match dtype {
            ColumnType::Bool => ColumnData::Bool {
                data: vec![0u8; (count + 7) / 8],
                len: count,
            },
            ColumnType::Int64
            | ColumnType::Int8
            | ColumnType::Int16
            | ColumnType::Int32
            | ColumnType::UInt8
            | ColumnType::UInt16
            | ColumnType::UInt32
            | ColumnType::UInt64
            | ColumnType::Timestamp
            | ColumnType::Date => ColumnData::Int64(vec![0i64; count]),
            ColumnType::Float64 | ColumnType::Float32 => ColumnData::Float64(vec![0.0f64; count]),
            ColumnType::String | ColumnType::StringDict => ColumnData::String {
                offsets: vec![0u64; count + 1],
                data: Vec::new(),
            },
            ColumnType::Binary | ColumnType::Blob => ColumnData::Binary {
                offsets: vec![0u64; count + 1],
                data: Vec::new(),
            },
            ColumnType::FixedList => ColumnData::FixedList {
                data: Vec::new(),
                dim: 0,
            },
            ColumnType::Float16List => ColumnData::Float16List {
                data: Vec::new(),
                dim: 0,
            },
            ColumnType::Null => ColumnData::Int64(vec![0i64; count]),
        }
    }

    // compact_column_streaming removed — was legacy dead code (326 lines).
    // save() always produces V4 format; compact() uses in-memory merge path.

    /// Read delta file and merge into in-memory columns
    fn merge_delta_file(&self, delta_path: &Path) -> io::Result<()> {
        let mut file = File::open(delta_path)?;

        loop {
            // Try to read record count
            let mut count_buf = [0u8; 8];
            match file.read_exact(&mut count_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let record_count = u64::from_le_bytes(count_buf) as usize;

            // Read IDs
            let mut delta_ids = Vec::with_capacity(record_count);
            for _ in 0..record_count {
                let mut id_buf = [0u8; 8];
                file.read_exact(&mut id_buf)?;
                delta_ids.push(u64::from_le_bytes(id_buf));
            }

            // Read int columns
            let mut int_columns: HashMap<String, Vec<i64>> = HashMap::new();
            let mut count_buf = [0u8; 4];
            file.read_exact(&mut count_buf)?;
            let int_col_count = u32::from_le_bytes(count_buf) as usize;
            for _ in 0..int_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                let mut name_buf = vec![0u8; name_len];
                file.read_exact(&mut name_buf)?;
                let name = String::from_utf8_lossy(&name_buf).to_string();
                let mut values = Vec::with_capacity(record_count);
                for _ in 0..record_count {
                    let mut v_buf = [0u8; 8];
                    file.read_exact(&mut v_buf)?;
                    values.push(i64::from_le_bytes(v_buf));
                }
                int_columns.insert(name, values);
            }

            // Read float columns
            let mut float_columns: HashMap<String, Vec<f64>> = HashMap::new();
            file.read_exact(&mut count_buf)?;
            let float_col_count = u32::from_le_bytes(count_buf) as usize;
            for _ in 0..float_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                let mut name_buf = vec![0u8; name_len];
                file.read_exact(&mut name_buf)?;
                let name = String::from_utf8_lossy(&name_buf).to_string();
                let mut values = Vec::with_capacity(record_count);
                for _ in 0..record_count {
                    let mut v_buf = [0u8; 8];
                    file.read_exact(&mut v_buf)?;
                    values.push(f64::from_le_bytes(v_buf));
                }
                float_columns.insert(name, values);
            }

            // Read string columns
            let mut string_columns: HashMap<String, Vec<String>> = HashMap::new();
            file.read_exact(&mut count_buf)?;
            let string_col_count = u32::from_le_bytes(count_buf) as usize;
            for _ in 0..string_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                let mut name_buf = vec![0u8; name_len];
                file.read_exact(&mut name_buf)?;
                let name = String::from_utf8_lossy(&name_buf).to_string();
                let mut values = Vec::with_capacity(record_count);
                for _ in 0..record_count {
                    let mut str_len_buf = [0u8; 4];
                    file.read_exact(&mut str_len_buf)?;
                    let str_len = u32::from_le_bytes(str_len_buf) as usize;
                    let mut str_buf = vec![0u8; str_len];
                    file.read_exact(&mut str_buf)?;
                    let val = String::from_utf8_lossy(&str_buf).to_string();
                    values.push(val);
                }
                string_columns.insert(name, values);
            }

            // Read bool columns
            let mut bool_columns: HashMap<String, Vec<bool>> = HashMap::new();
            file.read_exact(&mut count_buf)?;
            let bool_col_count = u32::from_le_bytes(count_buf) as usize;
            for _ in 0..bool_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                let mut name_buf = vec![0u8; name_len];
                file.read_exact(&mut name_buf)?;
                let name = String::from_utf8_lossy(&name_buf).to_string();
                let mut values = Vec::with_capacity(record_count);
                for _ in 0..record_count {
                    let mut v_buf = [0u8; 1];
                    file.read_exact(&mut v_buf)?;
                    values.push(v_buf[0] != 0);
                }
                bool_columns.insert(name, values);
            }

            // Merge into in-memory columns PRESERVING original delta IDs
            // This is critical for correct ID assignment after delete operations
            self.insert_typed_with_ids(
                &delta_ids,
                int_columns,
                float_columns,
                string_columns,
                HashMap::new(), // binary columns (not implemented in delta yet)
                bool_columns,
            )?;
        }

        Ok(())
    }

    /// Read delta file and return column data without merging into memory
    /// Returns: (delta_ids, column_data_map) where column_data_map is column_name -> ColumnData
    fn read_delta_data(&self) -> io::Result<Option<(Vec<u64>, HashMap<String, ColumnData>)>> {
        let delta_path = Self::delta_path(&self.path);
        if !delta_path.exists() {
            return Ok(None);
        }

        let mut file = File::open(&delta_path)?;
        let mut all_ids: Vec<u64> = Vec::new();
        let mut all_columns: HashMap<String, ColumnData> = HashMap::new();

        loop {
            // Try to read record count
            let mut count_buf = [0u8; 8];
            match file.read_exact(&mut count_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let record_count = u64::from_le_bytes(count_buf) as usize;

            // Read IDs
            for _ in 0..record_count {
                let mut id_buf = [0u8; 8];
                file.read_exact(&mut id_buf)?;
                all_ids.push(u64::from_le_bytes(id_buf));
            }

            // Read int columns
            let mut count_buf4 = [0u8; 4];
            file.read_exact(&mut count_buf4)?;
            let int_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..int_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                let mut name_buf = vec![0u8; name_len];
                file.read_exact(&mut name_buf)?;
                let name = String::from_utf8_lossy(&name_buf).to_string();

                let col_data = all_columns
                    .entry(name)
                    .or_insert_with(|| ColumnData::new(ColumnType::Int64));
                for _ in 0..record_count {
                    let mut v_buf = [0u8; 8];
                    file.read_exact(&mut v_buf)?;
                    col_data.push_i64(i64::from_le_bytes(v_buf));
                }
            }

            // Read float columns
            file.read_exact(&mut count_buf4)?;
            let float_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..float_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                let mut name_buf = vec![0u8; name_len];
                file.read_exact(&mut name_buf)?;
                let name = String::from_utf8_lossy(&name_buf).to_string();

                let col_data = all_columns
                    .entry(name)
                    .or_insert_with(|| ColumnData::new(ColumnType::Float64));
                for _ in 0..record_count {
                    let mut v_buf = [0u8; 8];
                    file.read_exact(&mut v_buf)?;
                    col_data.push_f64(f64::from_le_bytes(v_buf));
                }
            }

            // Read string columns
            file.read_exact(&mut count_buf4)?;
            let string_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..string_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                let mut name_buf = vec![0u8; name_len];
                file.read_exact(&mut name_buf)?;
                let name = String::from_utf8_lossy(&name_buf).to_string();

                let col_data = all_columns
                    .entry(name)
                    .or_insert_with(|| ColumnData::new(ColumnType::String));
                for _ in 0..record_count {
                    let mut str_len_buf = [0u8; 4];
                    file.read_exact(&mut str_len_buf)?;
                    let str_len = u32::from_le_bytes(str_len_buf) as usize;
                    let mut str_buf = vec![0u8; str_len];
                    file.read_exact(&mut str_buf)?;
                    let val = String::from_utf8_lossy(&str_buf).to_string();
                    col_data.push_string(&val);
                }
            }

            // Read bool columns
            file.read_exact(&mut count_buf4)?;
            let bool_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..bool_col_count {
                let mut len_buf = [0u8; 2];
                file.read_exact(&mut len_buf)?;
                let name_len = u16::from_le_bytes(len_buf) as usize;
                let mut name_buf = vec![0u8; name_len];
                file.read_exact(&mut name_buf)?;
                let name = String::from_utf8_lossy(&name_buf).to_string();

                let col_data = all_columns
                    .entry(name)
                    .or_insert_with(|| ColumnData::new(ColumnType::Bool));
                for _ in 0..record_count {
                    let mut v_buf = [0u8; 1];
                    file.read_exact(&mut v_buf)?;
                    col_data.push_bool(v_buf[0] != 0);
                }
            }
        }

        if all_ids.is_empty() {
            Ok(None)
        } else {
            Ok(Some((all_ids, all_columns)))
        }
    }

    #[inline]
    fn column_string_at(col: &ColumnData, row_idx: usize) -> Option<&str> {
        match col {
            ColumnData::String { offsets, data } => {
                if row_idx + 1 >= offsets.len() {
                    return None;
                }
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                if start <= end && end <= data.len() {
                    std::str::from_utf8(&data[start..end]).ok()
                } else {
                    None
                }
            }
            ColumnData::StringDict {
                indices,
                dict_offsets,
                dict_data,
            } => {
                let dict_idx = *indices.get(row_idx)?;
                if dict_idx == 0 {
                    return None;
                }
                let di = (dict_idx - 1) as usize;
                let start = *dict_offsets.get(di)? as usize;
                let end = if di + 1 < dict_offsets.len() {
                    dict_offsets[di + 1] as usize
                } else {
                    dict_data.len()
                };
                if start <= end && end <= dict_data.len() {
                    std::str::from_utf8(&dict_data[start..end]).ok()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    #[inline]
    fn column_binary_at(col: &ColumnData, row_idx: usize) -> Option<&[u8]> {
        match col {
            ColumnData::Binary { offsets, data } => {
                if row_idx + 1 >= offsets.len() {
                    return None;
                }
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                if start <= end && end <= data.len() {
                    Some(&data[start..end])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    #[inline]
    fn column_bool_at(col: &ColumnData, row_idx: usize) -> Option<bool> {
        match col {
            ColumnData::Bool { data, len } if row_idx < *len => {
                let byte_idx = row_idx / 8;
                let bit_idx = row_idx % 8;
                data.get(byte_idx).map(|b| ((b >> bit_idx) & 1) == 1)
            }
            _ => None,
        }
    }

    /// Return committed append-only delta row IDs whose string column equals `target`.
    /// This lets string equality filters stay mmap-fast without compacting `.delta`.
    pub fn delta_string_match_ids(&self, column_name: &str, target: &str) -> io::Result<Vec<u64>> {
        let delta_path = Self::delta_path(&self.path);
        if !delta_path.exists() {
            DELTA_STRING_INDEX_CACHE.write().remove(&delta_path);
            return Ok(Vec::new());
        };

        #[inline]
        fn take_slice<'a>(bytes: &'a [u8], pos: &mut usize, len: usize) -> io::Result<&'a [u8]> {
            let end = pos
                .checked_add(len)
                .ok_or_else(|| err_data("delta string scan offset overflow"))?;
            if end > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "delta string scan truncated",
                ));
            }
            let out = &bytes[*pos..end];
            *pos = end;
            Ok(out)
        }

        #[inline]
        fn read_u16(bytes: &[u8], pos: &mut usize) -> io::Result<u16> {
            let raw = take_slice(bytes, pos, 2)?;
            Ok(u16::from_le_bytes(raw.try_into().unwrap()))
        }

        #[inline]
        fn read_u32(bytes: &[u8], pos: &mut usize) -> io::Result<u32> {
            let raw = take_slice(bytes, pos, 4)?;
            Ok(u32::from_le_bytes(raw.try_into().unwrap()))
        }

        #[inline]
        fn read_u64(bytes: &[u8], pos: &mut usize) -> io::Result<u64> {
            let raw = take_slice(bytes, pos, 8)?;
            Ok(u64::from_le_bytes(raw.try_into().unwrap()))
        }

        fn parse_delta_string_index(
            bytes: &[u8],
            index: &mut HashMap<String, HashMap<String, Vec<u64>>>,
        ) -> io::Result<()> {
            let mut pos = 0usize;
            while pos < bytes.len() {
            if bytes.len() - pos < 8 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "delta string scan truncated record count",
                ));
            }
            let record_count = read_u64(&bytes, &mut pos)? as usize;

            let mut ids = Vec::with_capacity(record_count);
            for _ in 0..record_count {
                ids.push(read_u64(&bytes, &mut pos)?);
            }

            let int_col_count = read_u32(&bytes, &mut pos)? as usize;
            for _ in 0..int_col_count {
                let name_len = read_u16(&bytes, &mut pos)? as usize;
                take_slice(&bytes, &mut pos, name_len)?;
                take_slice(&bytes, &mut pos, record_count * 8)?;
            }

            let float_col_count = read_u32(&bytes, &mut pos)? as usize;
            for _ in 0..float_col_count {
                let name_len = read_u16(&bytes, &mut pos)? as usize;
                take_slice(&bytes, &mut pos, name_len)?;
                take_slice(&bytes, &mut pos, record_count * 8)?;
            }

            let string_col_count = read_u32(&bytes, &mut pos)? as usize;
            for _ in 0..string_col_count {
                let name_len = read_u16(&bytes, &mut pos)? as usize;
                let name = take_slice(&bytes, &mut pos, name_len)?;
                let col_name = String::from_utf8_lossy(name).into_owned();
                let col_index = index.entry(col_name).or_default();

                for row_idx in 0..record_count {
                    let str_len = read_u32(&bytes, &mut pos)? as usize;
                    let value = take_slice(&bytes, &mut pos, str_len)?;
                    if let Some(id) = ids.get(row_idx) {
                        let value = String::from_utf8_lossy(value).into_owned();
                        col_index.entry(value).or_default().push(*id);
                    }
                }
            }

            let bool_col_count = read_u32(&bytes, &mut pos)? as usize;
            for _ in 0..bool_col_count {
                let name_len = read_u16(&bytes, &mut pos)? as usize;
                take_slice(&bytes, &mut pos, name_len)?;
                take_slice(&bytes, &mut pos, record_count)?;
            }
        }
            Ok(())
        }

        let metadata = std::fs::metadata(&delta_path)?;
        let file_len = metadata.len();
        let modified = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let epoch = crate::storage::epoch::current(&self.path);

        let mut cache = DELTA_STRING_INDEX_CACHE.write();
        if cache.len() > 128 {
            cache.clear();
        }

        let entry = cache.entry(delta_path.clone()).or_insert_with(|| DeltaStringIndexCache {
            len: 0,
            modified: std::time::SystemTime::UNIX_EPOCH,
            epoch,
            index: HashMap::new(),
        });

        // This cache still observes the table epoch, but its contents are
        // derived solely from the append-only `.delta` file. On an epoch
        // change, matching file metadata is sufficient to adopt the new
        // epoch without rebuilding an unchanged index.
        let up_to_date = entry.len == file_len && entry.modified >= modified;
        let can_append = entry.len > 0
            && entry.len < file_len
            && entry.modified <= modified;
        let epoch_changed = entry.epoch != epoch;
        if !up_to_date && !can_append {
            entry.len = 0;
            entry.index.clear();
        }

        if !up_to_date {
            let bytes = std::fs::read(&delta_path)?;
            let start = if can_append { entry.len as usize } else { 0 };
            parse_delta_string_index(&bytes[start..], &mut entry.index)?;
            entry.len = file_len;
            entry.modified = modified;
            entry.epoch = epoch;
        }
        if epoch_changed {
            entry.epoch = epoch;
        }

        Ok(entry
            .index
            .get(column_name)
            .and_then(|values| values.get(target))
            .cloned()
            .unwrap_or_default())
    }

    /// Materialize committed append-only delta rows by ID in caller order.
    pub fn read_delta_rows_by_ids_to_arrow(
        &self,
        ids: &[u64],
    ) -> io::Result<arrow::record_batch::RecordBatch> {
        use arrow::array::{
            ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
        };
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use std::sync::Arc;

        let schema_cols = self.schema.read().columns.clone();
        let empty_batch = || -> io::Result<arrow::record_batch::RecordBatch> {
            let mut fields = Vec::with_capacity(schema_cols.len() + 1);
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema_cols.len() + 1);
            fields.push(Field::new("_id", ArrowDataType::Int64, false));
            arrays.push(Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef);
            for (name, col_type) in &schema_cols {
                let (dt, array): (ArrowDataType, ArrayRef) = match col_type {
                    ColumnType::Bool => (
                        ArrowDataType::Boolean,
                        Arc::new(BooleanArray::from(Vec::<Option<bool>>::new())),
                    ),
                    ColumnType::Float32 | ColumnType::Float64 => (
                        ArrowDataType::Float64,
                        Arc::new(Float64Array::from(Vec::<Option<f64>>::new())),
                    ),
                    ColumnType::Binary | ColumnType::FixedList | ColumnType::Float16List => (
                        ArrowDataType::Binary,
                        Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new())),
                    ),
                    ColumnType::String | ColumnType::StringDict | ColumnType::Null => (
                        ArrowDataType::Utf8,
                        Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                    ),
                    _ => (
                        ArrowDataType::Int64,
                        Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
                    ),
                };
                fields.push(Field::new(name, dt, true));
                arrays.push(array);
            }
            arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
        };

        if ids.is_empty() {
            return empty_batch();
        }

        let Some((delta_ids, delta_columns)) = self.read_delta_data()? else {
            return empty_batch();
        };
        let delta_pos: HashMap<u64, usize> = delta_ids
            .iter()
            .enumerate()
            .map(|(idx, id)| (*id, idx))
            .collect();
        let positions: Vec<(u64, usize)> = ids
            .iter()
            .filter_map(|id| delta_pos.get(id).copied().map(|pos| (*id, pos)))
            .collect();
        if positions.is_empty() {
            return empty_batch();
        }

        let mut fields = Vec::with_capacity(schema_cols.len() + 1);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema_cols.len() + 1);
        let row_ids: Vec<i64> = positions.iter().map(|(id, _)| *id as i64).collect();
        fields.push(Field::new("_id", ArrowDataType::Int64, false));
        arrays.push(Arc::new(Int64Array::from(row_ids)) as ArrayRef);

        for (name, col_type) in &schema_cols {
            let column = delta_columns.get(name);
            let (dt, array): (ArrowDataType, ArrayRef) = match col_type {
                ColumnType::Bool => {
                    let values: Vec<Option<bool>> = positions
                        .iter()
                        .map(|(_, row_idx)| column.and_then(|c| Self::column_bool_at(c, *row_idx)))
                        .collect();
                    (ArrowDataType::Boolean, Arc::new(BooleanArray::from(values)))
                }
                ColumnType::Float32 | ColumnType::Float64 => {
                    let values: Vec<Option<f64>> = positions
                        .iter()
                        .map(|(_, row_idx)| match column {
                            Some(ColumnData::Float64(values)) => values.get(*row_idx).copied(),
                            _ => None,
                        })
                        .collect();
                    (ArrowDataType::Float64, Arc::new(Float64Array::from(values)))
                }
                ColumnType::String | ColumnType::StringDict | ColumnType::Null => {
                    let values: Vec<Option<String>> = positions
                        .iter()
                        .map(|(_, row_idx)| {
                            column
                                .and_then(|c| Self::column_string_at(c, *row_idx))
                                .map(str::to_owned)
                        })
                        .collect();
                    let refs: Vec<Option<&str>> = values.iter().map(|v| v.as_deref()).collect();
                    (ArrowDataType::Utf8, Arc::new(StringArray::from(refs)))
                }
                ColumnType::Binary | ColumnType::FixedList | ColumnType::Float16List => {
                    let values: Vec<Option<Vec<u8>>> = positions
                        .iter()
                        .map(|(_, row_idx)| {
                            column
                                .and_then(|c| Self::column_binary_at(c, *row_idx))
                                .map(|b| b.to_vec())
                        })
                        .collect();
                    let refs: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
                    (ArrowDataType::Binary, Arc::new(BinaryArray::from(refs)))
                }
                _ => {
                    let values: Vec<Option<i64>> = positions
                        .iter()
                        .map(|(_, row_idx)| match column {
                            Some(ColumnData::Int64(values)) => values.get(*row_idx).copied(),
                            _ => None,
                        })
                        .collect();
                    (ArrowDataType::Int64, Arc::new(Int64Array::from(values)))
                }
            };
            fields.push(Field::new(name, dt, true));
            arrays.push(array);
        }

        arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    #[inline]
    fn read_delta_bytes_bounded(file: &mut File, buffer: &mut [u8], snapshot_len: u64) -> bool {
        let Ok(position) = file.stream_position() else {
            return false;
        };
        let Some(end) = position.checked_add(buffer.len() as u64) else {
            return false;
        };
        end <= snapshot_len && file.read_exact(buffer).is_ok()
    }

    #[inline]
    fn skip_delta_bytes_bounded(file: &mut File, bytes: u64, snapshot_len: u64) -> bool {
        let Ok(position) = file.stream_position() else {
            return false;
        };
        let Some(end) = position.checked_add(bytes) else {
            return false;
        };
        end <= snapshot_len && file.seek(SeekFrom::Start(end)).is_ok()
    }

    fn scan_delta_row_count(file: &mut File, snapshot_len: u64, mut total: usize) -> usize {
        'records: loop {
            let mut count_buf = [0u8; 8];
            if !Self::read_delta_bytes_bounded(file, &mut count_buf, snapshot_len) {
                break;
            }
            let record_count = u64::from_le_bytes(count_buf) as usize;
            let Some(fixed_width_bytes) = (record_count as u64).checked_mul(8) else {
                break;
            };

            // IDs
            if !Self::skip_delta_bytes_bounded(file, fixed_width_bytes, snapshot_len) {
                break;
            }

            // Int columns
            let mut count_buf4 = [0u8; 4];
            if !Self::read_delta_bytes_bounded(file, &mut count_buf4, snapshot_len) {
                break;
            }
            let int_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..int_col_count {
                let mut len_buf = [0u8; 2];
                if !Self::read_delta_bytes_bounded(file, &mut len_buf, snapshot_len) {
                    break 'records;
                }
                let name_len = u16::from_le_bytes(len_buf) as u64;
                let Some(column_bytes) = name_len.checked_add(fixed_width_bytes) else {
                    break 'records;
                };
                if !Self::skip_delta_bytes_bounded(file, column_bytes, snapshot_len) {
                    break 'records;
                }
            }

            // Float columns
            if !Self::read_delta_bytes_bounded(file, &mut count_buf4, snapshot_len) {
                break;
            }
            let float_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..float_col_count {
                let mut len_buf = [0u8; 2];
                if !Self::read_delta_bytes_bounded(file, &mut len_buf, snapshot_len) {
                    break 'records;
                }
                let name_len = u16::from_le_bytes(len_buf) as u64;
                let Some(column_bytes) = name_len.checked_add(fixed_width_bytes) else {
                    break 'records;
                };
                if !Self::skip_delta_bytes_bounded(file, column_bytes, snapshot_len) {
                    break 'records;
                }
            }

            // String columns
            if !Self::read_delta_bytes_bounded(file, &mut count_buf4, snapshot_len) {
                break;
            }
            let string_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..string_col_count {
                let mut len_buf = [0u8; 2];
                if !Self::read_delta_bytes_bounded(file, &mut len_buf, snapshot_len)
                    || !Self::skip_delta_bytes_bounded(
                        file,
                        u16::from_le_bytes(len_buf) as u64,
                        snapshot_len,
                    )
                {
                    break 'records;
                }
                for _ in 0..record_count {
                    let mut str_len_buf = [0u8; 4];
                    if !Self::read_delta_bytes_bounded(file, &mut str_len_buf, snapshot_len)
                        || !Self::skip_delta_bytes_bounded(
                            file,
                            u32::from_le_bytes(str_len_buf) as u64,
                            snapshot_len,
                        )
                    {
                        break 'records;
                    }
                }
            }

            // Bool columns
            if !Self::read_delta_bytes_bounded(file, &mut count_buf4, snapshot_len) {
                break;
            }
            let bool_col_count = u32::from_le_bytes(count_buf4) as usize;
            for _ in 0..bool_col_count {
                let mut len_buf = [0u8; 2];
                if !Self::read_delta_bytes_bounded(file, &mut len_buf, snapshot_len) {
                    break 'records;
                }
                let Some(column_bytes) =
                    (u16::from_le_bytes(len_buf) as u64).checked_add(record_count as u64)
                else {
                    break 'records;
                };
                if !Self::skip_delta_bytes_bounded(file, column_bytes, snapshot_len) {
                    break 'records;
                }
            }

            let Some(next_total) = total.checked_add(record_count) else {
                break;
            };
            total = next_total;
        }

        total
    }

    /// Get the total row count including delta rows (for accurate row_count reporting)
    fn delta_row_count(&self) -> usize {
        let delta_path = Self::delta_path(&self.path);
        let Ok(metadata) = std::fs::metadata(&delta_path) else {
            DELTA_ROW_COUNT_CACHE.write().remove(&delta_path);
            return 0;
        };

        let file_len = metadata.len();
        let modified = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let epoch = crate::storage::epoch::current(&self.path);
        {
            let cache = DELTA_ROW_COUNT_CACHE.read();
            if let Some((cached_len, cached_modified, cached_count, observed_epoch)) =
                cache.get(&delta_path)
            {
                if *observed_epoch == epoch
                    && *cached_len == file_len
                    && *cached_modified >= modified
                {
                    return *cached_count;
                }
            }
        }

        // An unrelated logical write may advance the table epoch without
        // changing the append-only delta bytes. Revalidate that cheap file
        // identity once, then let subsequent reads use the normal epoch hit.
        {
            let mut cache = DELTA_ROW_COUNT_CACHE.write();
            if let Some((cached_len, cached_modified, cached_count, observed_epoch)) =
                cache.get_mut(&delta_path)
            {
                if *cached_len == file_len && *cached_modified >= modified {
                    *observed_epoch = epoch;
                    return *cached_count;
                }
            }
        }

        let (start, mut total) = {
            let cache = DELTA_ROW_COUNT_CACHE.read();
            if let Some((cached_len, cached_modified, cached_count, _)) = cache.get(&delta_path) {
                if *cached_len < file_len && *cached_modified <= modified {
                    (*cached_len, *cached_count)
                } else {
                    (0, 0)
                }
            } else {
                (0, 0)
            }
        };

        let Ok(mut file) = File::open(&delta_path) else {
            return 0;
        };
        if start > 0 && file.seek(SeekFrom::Start(start)).is_err() {
            total = 0;
            let _ = file.seek(SeekFrom::Start(0));
        }

        total = Self::scan_delta_row_count(&mut file, file_len, total);

        let mut cache = DELTA_ROW_COUNT_CACHE.write();
        if cache.len() > 128 {
            cache.clear();
        }
        cache.insert(
            delta_path,
            (
                file_len,
                modified,
                total,
                epoch,
            ),
        );
        total
    }
}
