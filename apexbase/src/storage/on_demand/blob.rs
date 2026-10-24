// Blob sidecar storage helpers.

const BLOB_INLINE_THRESHOLD: usize = 64 * 1024;
const BLOB_PACKED_THRESHOLD: usize = 4 * 1024 * 1024;
const BLOB_DESC_MAGIC: &[u8; 4] = b"ABLB";
const BLOB_DESC_VERSION: u8 = 1;
const BLOB_MODE_INLINE: u8 = 0;
const BLOB_MODE_PACKED: u8 = 1;
const BLOB_MODE_DEDICATED: u8 = 2;
const BLOB_MODE_EXTERNAL: u8 = 3;
static BLOB_OBJECT_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobStorageMode {
    Inline,
    Packed,
    Dedicated,
    External,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobDescriptorInfo {
    pub mode: BlobStorageMode,
    pub len: u64,
    pub checksum: u32,
    pub locator_len: usize,
}

#[derive(Debug, Clone)]
struct BlobDescriptor {
    mode: u8,
    len: u64,
    checksum: u32,
    locator: Vec<u8>,
}

pub(crate) struct BlobReadCache {
    packed_file: Option<File>,
    objects_dir: PathBuf,
}

/// Packed payload offsets keyed by (table path, length, crc32). A client often
/// stores duplicate payloads in separate batches/flushes, which recreate the
/// storage object, so the dedup state lives at process scope; it is cleared for
/// a path when that table's files are reaped.
static BLOB_PACKED_DEDUP: once_cell::sync::Lazy<
    parking_lot::RwLock<std::collections::HashMap<(PathBuf, u64, u32), u64>>,
> = once_cell::sync::Lazy::new(|| parking_lot::RwLock::new(std::collections::HashMap::new()));

pub(crate) fn forget_blob_packed_dedup(path: &Path) {
    BLOB_PACKED_DEDUP.write().retain(|(table, _, _), _| table != path);
}

#[inline]
fn null_bit_is_set(bitmap: &[u8], row_idx: usize) -> bool {
    let byte_idx = row_idx / 8;
    byte_idx < bitmap.len() && ((bitmap[byte_idx] >> (row_idx % 8)) & 1 == 1)
}

fn blob_dir_for_path(path: &Path) -> PathBuf {
    let mut dir = path.to_path_buf();
    let name = dir
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    dir.set_file_name(format!("{}.blobs", name));
    dir
}

fn encode_blob_descriptor(mode: u8, len: u64, checksum: u32, locator: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(22 + locator.len());
    out.extend_from_slice(BLOB_DESC_MAGIC);
    out.push(BLOB_DESC_VERSION);
    out.push(mode);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&(locator.len() as u32).to_le_bytes());
    out.extend_from_slice(locator);
    out
}

fn decode_blob_descriptor(bytes: &[u8]) -> Option<BlobDescriptor> {
    if bytes.len() < 22 || &bytes[0..4] != BLOB_DESC_MAGIC || bytes[4] != BLOB_DESC_VERSION {
        return None;
    }
    let mode = bytes[5];
    let len = u64::from_le_bytes(bytes[6..14].try_into().ok()?);
    let checksum = u32::from_le_bytes(bytes[14..18].try_into().ok()?);
    let locator_len = u32::from_le_bytes(bytes[18..22].try_into().ok()?) as usize;
    if 22 + locator_len > bytes.len() {
        return None;
    }
    Some(BlobDescriptor {
        mode,
        len,
        checksum,
        locator: bytes[22..22 + locator_len].to_vec(),
    })
}

fn blob_descriptor_bounds<'a>(
    offsets: &[u64],
    data: &'a [u8],
    row_idx: usize,
) -> Option<&'a [u8]> {
    if row_idx + 1 >= offsets.len() {
        return None;
    }
    let start = offsets[row_idx] as usize;
    let end = offsets[row_idx + 1] as usize;
    if start > end || end > data.len() {
        return None;
    }
    Some(&data[start..end])
}

impl OnDemandStorage {
    fn read_v4_null_bit(
        &self,
        col_idx: usize,
        row_idx: usize,
    ) -> io::Result<Option<bool>> {
        let Some(footer) = self.get_or_load_footer()? else {
            return Ok(None);
        };
        if col_idx >= footer.schema.column_count() {
            return Ok(None);
        }

        let mut row_base = 0usize;
        let mut target = None;
        for rg_meta in &footer.row_groups {
            let rg_rows = rg_meta.row_count as usize;
            let row_end = row_base + rg_rows;
            if row_idx < row_end {
                target = Some((rg_meta, rg_rows, row_idx - row_base));
                break;
            }
            row_base = row_end;
        }
        let Some((rg_meta, rg_rows, local_idx)) = target else {
            return Ok(None);
        };

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open for V4 blob null read"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
        if rg_end > mmap_ref.len() {
            return Err(err_data("Blob null read RG extends past EOF"));
        }
        let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
        let compress_flag = if rg_bytes.len() >= 32 {
            rg_bytes[28]
        } else {
            RG_COMPRESS_NONE
        };
        let encoding_version = if rg_bytes.len() >= 32 { rg_bytes[29] } else { 0 };
        let decompressed = self.decompressed_rg_body(
            rg_meta.offset,
            rg_meta.offset + rg_meta.data_size,
            compress_flag,
            &rg_bytes[32..],
        )?;
        let body = decompressed
            .as_ref()
            .map(|buf| buf.as_slice())
            .unwrap_or(&rg_bytes[32..]);

        let mut pos = rg_id_section_len(
            rg_rows,
            rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
        );
        let null_bitmap_len = (rg_rows + 7) / 8;
        pos = pos
            .checked_add(null_bitmap_len)
            .ok_or_else(|| err_data("Blob null read offset overflow"))?;
        if pos > body.len() {
            return Err(err_data("Blob null read truncated row group"));
        }

        for idx in 0..=col_idx {
            if pos + null_bitmap_len > body.len() {
                return Err(err_data("Blob null read truncated null bitmap"));
            }
            let null_bitmap = &body[pos..pos + null_bitmap_len];
            pos += null_bitmap_len;
            if idx == col_idx {
                return Ok(Some(null_bit_is_set(null_bitmap, local_idx)));
            }

            let col_type = footer.schema.columns[idx].1;
            let consumed = if rg_columns_are_encoded(encoding_version) {
                skip_column_encoded(&body[pos..], col_type)?
            } else {
                ColumnData::skip_bytes_typed(&body[pos..], col_type)?
            };
            pos = pos
                .checked_add(consumed)
                .ok_or_else(|| err_data("Blob null read column offset overflow"))?;
            if pos > body.len() {
                return Err(err_data("Blob null read column data truncated"));
            }
        }

        Ok(None)
    }

    fn is_blob_null_at_row(&self, col_idx: usize, row_idx: usize) -> io::Result<bool> {
        if self.is_v4_format() && !self.has_v4_in_memory_data() {
            if let Some(is_null) = self.read_v4_null_bit(col_idx, row_idx)? {
                return Ok(is_null);
            }
        }

        let nulls = self.nulls.read();
        Ok(nulls
            .get(col_idx)
            .map_or(false, |bitmap| null_bit_is_set(bitmap, row_idx)))
    }

    pub fn blob_sidecar_dir(&self) -> PathBuf {
        blob_dir_for_path(&self.path)
    }
    /// One cache for a batch of blob reads: packed blobs share a single open
    /// file instead of opening `packed.blob` once per row.
    pub(crate) fn blob_read_cache(&self) -> BlobReadCache {
        BlobReadCache {
            packed_file: None,
            objects_dir: self.blob_sidecar_dir().join("objects"),
        }
    }


    pub fn write_blob_value(&self, value: &[u8]) -> io::Result<Vec<u8>> {
        let checksum = crc32fast::hash(value);
        let len = value.len() as u64;

        if self.in_memory || value.len() <= BLOB_INLINE_THRESHOLD {
            return Ok(encode_blob_descriptor(BLOB_MODE_INLINE, len, checksum, value));
        }

        let blob_dir = self.blob_sidecar_dir();
        std::fs::create_dir_all(&blob_dir)?;

        if value.len() <= BLOB_PACKED_THRESHOLD {
            let packed_path = blob_dir.join("packed.blob");
            let mut file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&packed_path)?;
            let offset = file.seek(SeekFrom::End(0))?;
            file.write_all(value)?;
            file.flush()?;
            if self.durability != super::DurabilityLevel::Fast {
                file.sync_all()?;
            }
            return Ok(encode_blob_descriptor(
                BLOB_MODE_PACKED,
                len,
                checksum,
                &offset.to_le_bytes(),
            ));
        }

        let objects_dir = blob_dir.join("objects");
        std::fs::create_dir_all(&objects_dir)?;
        let object_id = format!("{:x}-{:08x}-{}.blob", md5::compute(value), checksum, len);
        let object_path = objects_dir.join(&object_id);
        if !object_path.exists() {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&object_path)?;
            file.write_all(value)?;
            file.flush()?;
            if self.durability != super::DurabilityLevel::Fast {
                file.sync_all()?;
            }
        }
        Ok(encode_blob_descriptor(
            BLOB_MODE_DEDICATED,
            len,
            checksum,
            object_id.as_bytes(),
        ))
    }

    pub fn write_blob_values(&self, values: &[Vec<u8>]) -> io::Result<Vec<Vec<u8>>> {
        if self.in_memory {
            return Ok(values
                .iter()
                .map(|value| {
                    encode_blob_descriptor(
                        BLOB_MODE_INLINE,
                        value.len() as u64,
                        crc32fast::hash(value),
                        value,
                    )
                })
                .collect());
        }
        let mut descriptors = Vec::with_capacity(values.len());
        // Repeated payloads inside one batch (e.g. a table of duplicated
        // documents) are stored once in packed.blob; the descriptor already
        // carries the 32-bit checksum readers verify against.
        let mut packed_dedup: std::collections::HashMap<(u64, u32), u64> =
            std::collections::HashMap::new();
        let mut packed_file: Option<File> = None;
        let mut packed_offset = 0u64;
        let mut blob_dir_created = false;
        let mut objects_dir_created = false;
        let batch_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let blob_dir = self.blob_sidecar_dir();
        let objects_dir = blob_dir.join("objects");

        for value in values {
            let checksum = crc32fast::hash(value);
            let len = value.len() as u64;

            if value.len() <= BLOB_INLINE_THRESHOLD {
                descriptors.push(encode_blob_descriptor(
                    BLOB_MODE_INLINE,
                    len,
                    checksum,
                    value,
                ));
                continue;
            }

            if !blob_dir_created {
                std::fs::create_dir_all(&blob_dir)?;
                blob_dir_created = true;
            }

            if value.len() <= BLOB_PACKED_THRESHOLD {
                let shared = BLOB_PACKED_DEDUP
                    .read()
                    .get(&(self.path.clone(), len, checksum))
                    .copied();
                if let Some(offset) = packed_dedup.get(&(len, checksum)).copied().or(shared) {
                    descriptors.push(encode_blob_descriptor(
                        BLOB_MODE_PACKED,
                        len,
                        checksum,
                        &offset.to_le_bytes(),
                    ));
                    continue;
                }
                if packed_file.is_none() {
                    let mut file = OpenOptions::new()
                        .create(true)
                        .read(true)
                        .append(true)
                        .open(blob_dir.join("packed.blob"))?;
                    packed_offset = file.seek(SeekFrom::End(0))?;
                    packed_file = Some(file);
                }
                let offset = packed_offset;
                let file = packed_file.as_mut().unwrap();
                file.write_all(value)?;
                packed_offset += value.len() as u64;
                packed_dedup.insert((len, checksum), offset);
                {
                    let mut map = BLOB_PACKED_DEDUP.write();
                    if map.len() >= 4096 {
                        map.clear();
                    }
                    map.insert((self.path.clone(), len, checksum), offset);
                }
                descriptors.push(encode_blob_descriptor(
                    BLOB_MODE_PACKED,
                    len,
                    checksum,
                    &offset.to_le_bytes(),
                ));
                continue;
            }

            if !objects_dir_created {
                std::fs::create_dir_all(&objects_dir)?;
                objects_dir_created = true;
            }
            let object_id = loop {
                let seq = BLOB_OBJECT_COUNTER.fetch_add(1, Ordering::Relaxed);
                let object_id = format!("{batch_id:032x}-{seq:016x}-{checksum:08x}-{len}.blob");
                let object_path = objects_dir.join(&object_id);
                match OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&object_path)
                {
                    Ok(mut file) => {
                        file.write_all(value)?;
                        if self.durability != super::DurabilityLevel::Fast {
                            file.sync_all()?;
                        }
                        break object_id;
                    }
                    Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(err) => return Err(err),
                }
            };
            descriptors.push(encode_blob_descriptor(
                BLOB_MODE_DEDICATED,
                len,
                checksum,
                object_id.as_bytes(),
            ));
        }

        if let Some(mut file) = packed_file {
            file.flush()?;
            if self.durability != super::DurabilityLevel::Fast {
                file.sync_all()?;
            }
        }

        Ok(descriptors)
    }

    /// Remove dedicated blob objects that no live row references.
    ///
    /// Packed regions cannot be reclaimed without rewriting the descriptors that
    /// point at them, so this only sweeps `<table>.blobs/objects`. The scan reads
    /// every blob column of the persisted base and honours the deletion vector;
    /// call it while no writer is active (it is a maintenance operation, not a
    /// hot-path guard).
    pub fn vacuum_blob_objects(&self) -> io::Result<u64> {
        // Pending rows and appended delta rows are not part of the base scan
        // below; their payloads must not be mistaken for garbage.
        if self.has_delta()
            || self.has_pending_deltas()
            || self.pending_rows.load(std::sync::atomic::Ordering::Relaxed) > 0
            || self.columns.read().iter().any(|column| !column.is_empty())
        {
            return Err(err_data(
                "vacuum_blob_objects requires a flushed table with no pending writes",
            ));
        }
        let Some(footer) = self.get_or_load_footer()? else {
            return Ok(0);
        };
        let blob_columns: Vec<usize> = footer
            .schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, (_, column_type))| matches!(column_type, ColumnType::Blob))
            .map(|(index, _)| index)
            .collect();
        if blob_columns.is_empty() {
            return Ok(0);
        }
        let mut live: std::collections::HashSet<String> = std::collections::HashSet::new();
        for column_index in blob_columns {
            let (columns, deleted) = self.scan_columns_mmap(&[column_index], &footer)?;
            let Some(ColumnData::Binary { offsets, data }) = columns.first() else {
                continue;
            };
            for row in 0..offsets.len().saturating_sub(1) {
                if let Some(byte) = deleted.get(row / 8) {
                    if (byte >> (row % 8)) & 1 == 1 {
                        continue;
                    }
                }
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                if start > end || end > data.len() {
                    continue;
                }
                if let Some(descriptor) = decode_blob_descriptor(&data[start..end]) {
                    if descriptor.mode == BLOB_MODE_DEDICATED {
                        if let Ok(object_id) = std::str::from_utf8(&descriptor.locator) {
                            live.insert(object_id.to_string());
                        }
                    }
                }
            }
        }
        let objects_dir = self.blob_sidecar_dir().join("objects");
        let mut removed = 0u64;
        let Ok(entries) = std::fs::read_dir(&objects_dir) else {
            return Ok(0);
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if !live.contains(&name) && std::fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Rewrite `packed.blob` with only the payloads live rows reference.
    ///
    /// `vacuum_blob_objects` reclaims dedicated objects but packed payloads are
    /// append-only, so deleted rows leak their bytes forever. This compacts the
    /// packed file and rewrites the 8-byte locator of every live descriptor.
    ///
    /// Only uncompressed, flat (non-chunked) row groups are supported: their
    /// column bytes sit at a fixed file position, so the descriptors can be
    /// patched in a copy of the table file and published by an atomic rename.
    /// A `<table>.apex.packedvac` journal makes the rename sequence restartable:
    /// the next open completes it before any read (see `recover_packed_vacuum`).
    pub fn vacuum_packed_blobs(&self) -> io::Result<u64> {
        if self.has_delta()
            || self.has_pending_deltas()
            || self.pending_rows.load(std::sync::atomic::Ordering::Relaxed) > 0
            || self.columns.read().iter().any(|column| !column.is_empty())
        {
            return Err(err_data(
                "vacuum_packed_blobs requires a flushed table with no pending writes",
            ));
        }
        let Some(footer) = self.get_or_load_footer()? else {
            return Ok(0);
        };
        let blob_columns: Vec<usize> = footer
            .schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, (_, column_type))| matches!(column_type, ColumnType::Blob))
            .map(|(index, _)| index)
            .collect();
        if blob_columns.is_empty() {
            return Ok(0);
        }
        let sidecar = self.blob_sidecar_dir();
        let packed_path = sidecar.join("packed.blob");
        if !packed_path.exists() {
            return Ok(0);
        }
        let table_file = std::fs::File::open(&self.path)?;
        let table_mmap = unsafe { memmap2::Mmap::map(&table_file)? };
        let table_bytes: &[u8] = &table_mmap;
        if table_uses_chunk_layout(&footer.chunk_tables) == Some(true) {
            return Err(err_data(
                "vacuum_packed_blobs requires flat row groups (chunked tables are not supported yet)",
            ));
        }
        let mut rg_start_rows = Vec::with_capacity(footer.row_groups.len());
        let mut row_cursor = 0usize;
        for row_group in &footer.row_groups {
            rg_start_rows.push(row_cursor);
            let base = row_group.offset as usize;
            if base + 32 > table_bytes.len() {
                return Err(err_data("row group header is outside the table file"));
            }
            if table_bytes[base + 28] != 0 {
                return Err(err_data(
                    "vacuum_packed_blobs requires uncompressed row groups",
                ));
            }
            if table_bytes[base + 29] > 1 {
                return Err(err_data(
                    "vacuum_packed_blobs requires flat (non-chunked) row groups",
                ));
            }
            row_cursor += row_group.row_count as usize;
        }

        // (locator byte position in the table file, old packed offset, payload length)
        let mut descriptors: Vec<(usize, u64, u64)> = Vec::new();
        for column_index in blob_columns {
            let (columns, deleted) = self.scan_columns_mmap(&[column_index], &footer)?;
            let Some(ColumnData::Binary { offsets, data }) = columns.first() else {
                continue;
            };
            for (rg_index, row_group) in footer.row_groups.iter().enumerate() {
                let rg_rows = row_group.row_count as usize;
                if rg_rows == 0 {
                    continue;
                }
                let data_start = Self::flat_column_data_start(
                    table_bytes,
                    &footer,
                    rg_index,
                    column_index,
                    offsets.len().saturating_sub(1),
                )?;
                let first_row = rg_start_rows[rg_index];
                for local_row in 0..rg_rows {
                    let row = first_row + local_row;
                    if let Some(byte) = deleted.get(row / 8) {
                        if (byte >> (row % 8)) & 1 == 1 {
                            continue;
                        }
                    }
                    let start = offsets[row] as usize;
                    let end = offsets[row + 1] as usize;
                    if start > end || end > data.len() {
                        continue;
                    }
                    let descriptor_bytes = &data[start..end];
                    let Some(descriptor) = decode_blob_descriptor(descriptor_bytes) else {
                        continue;
                    };
                    if descriptor.mode != BLOB_MODE_PACKED || descriptor.locator.len() != 8 {
                        continue;
                    }
                    let old_offset = u64::from_le_bytes(descriptor.locator[..8].try_into().unwrap());
                    let locator_position = data_start + start + 22;
                    let locator_end = locator_position + 8;
                    if locator_end > table_bytes.len()
                        || &table_bytes[locator_position..locator_end]
                            != descriptor.locator.as_slice()
                    {
                        return Err(err_data(
                            "packed blob descriptor does not match the table file layout",
                        ));
                    }
                    descriptors.push((locator_position, old_offset, descriptor.len));
                }
            }
        }
        if descriptors.is_empty() {
            return Ok(0);
        }

        // Copy each distinct live payload in old-offset order into a new packed file.
        let new_packed_path = sidecar.join("packed.blob.vacuum");
        let mut old_packed = std::fs::File::open(&packed_path)?;
        let old_len = old_packed.metadata()?.len();
        let mut new_packed = std::io::BufWriter::new(std::fs::File::create(&new_packed_path)?);
        let mut new_offsets: std::collections::HashMap<u64, u64> =
            std::collections::HashMap::new();
        let mut new_cursor = 0u64;
        let mut distinct: Vec<(u64, u64)> = descriptors
            .iter()
            .map(|(_, offset, len)| (*offset, *len))
            .collect();
        distinct.sort_unstable();
        distinct.dedup();
        for (old_offset, len) in distinct {
            old_packed.seek(SeekFrom::Start(old_offset))?;
            let mut payload = vec![0u8; len as usize];
            old_packed.read_exact(&mut payload)?;
            new_offsets.insert(old_offset, new_cursor);
            new_packed.write_all(&payload)?;
            new_cursor += len;
        }
        new_packed.flush()?;
        let new_packed_file = new_packed.into_inner().map_err(|error| error.into_error())?;
        if self.durability != super::DurabilityLevel::Fast {
            new_packed_file.sync_all()?;
        }
        drop(new_packed_file);

        // Patch a copy of the table file so the live table stays readable until
        // the journal is published.
        let table_copy_path = packed_vacuum_table_copy(&self.path);
        std::fs::copy(&self.path, &table_copy_path)?;
        {
            let mut copy = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&table_copy_path)?;
            for (locator_position, old_offset, _len) in &descriptors {
                let new_offset = new_offsets.get(old_offset).copied().ok_or_else(|| {
                    err_data("packed blob payload is missing from the vacuum copy")
                })?;
                copy.seek(SeekFrom::Start(*locator_position as u64))?;
                copy.write_all(&new_offset.to_le_bytes())?;
            }
            copy.sync_all()?;
        }

        let journal_path = packed_vacuum_journal(&self.path);
        std::fs::write(&journal_path, b"APXVAC1")?;
        {
            let journal = std::fs::OpenOptions::new().read(true).write(true).open(&journal_path)?;
            journal.sync_all()?;
        }
        recover_packed_vacuum(&self.path)?;
        // The rename replaced the table inode: reopen it, drop the mmap of the
        // old inode and the decode caches keyed by the old file.
        *self.file.write() = Some(open_for_sequential_read(&self.path)?);
        self.mmap_cache.write().invalidate();
        self.invalidate_page_cache();
        Ok(old_len.saturating_sub(new_cursor))
    }

    fn flat_column_data_start(
        table_bytes: &[u8],
        footer: &V4Footer,
        rg_index: usize,
        column_index: usize,
        row_count: usize,
    ) -> io::Result<usize> {
        let row_group = &footer.row_groups[rg_index];
        let rg_rows = row_group.row_count as usize;
        let base = row_group.offset as usize + 32;
        let encoding_version = table_bytes[row_group.offset as usize + 29];
        let bitmap_len = (rg_rows + 7) / 8;
        // The footer records where each column's null bitmap starts inside the
        // flat body, so no prior column has to be decoded. Fall back to walking
        // the columns for older files that predate that directory.
        let column_start = footer
            .col_offsets
            .get(rg_index)
            .and_then(|columns| columns.get(column_index))
            .map(|offset| base + *offset as usize);
        let column_start = match column_start {
            Some(offset) => offset,
            None => {
                let id_encoding = table_bytes[row_group.offset as usize + 30];
                let mut pos = base + rg_id_section_len(rg_rows, id_encoding) + bitmap_len;
                for prior in 0..column_index {
                    pos += bitmap_len;
                    let column_type = footer.schema.columns[prior].1;
                    let (_, consumed) = if rg_columns_are_encoded(encoding_version) {
                        read_column_encoded(&table_bytes[pos..], column_type)?
                    } else {
                        ColumnData::from_bytes_typed(&table_bytes[pos..], column_type)?
                    };
                    pos += consumed;
                }
                pos
            }
        };
        let mut data_start = column_start + bitmap_len;
        if rg_columns_are_encoded(encoding_version) {
            if data_start >= table_bytes.len() || table_bytes[data_start] != COL_ENCODING_PLAIN {
                return Err(err_data(
                    "vacuum_packed_blobs requires plain binary column encoding",
                ));
            }
            data_start += 1;
        }
        // Binary payload: count u64, (count + 1) u32 offsets, data_len u64, data.
        let count = row_count;
        data_start += 8 + (count + 1) * 4 + 8;
        Ok(data_start)
    }

    pub fn read_blob_value(&self, descriptor_bytes: &[u8]) -> io::Result<Vec<u8>> {
        let Some(desc) = decode_blob_descriptor(descriptor_bytes) else {
            return Ok(descriptor_bytes.to_vec());
        };

        let data = match desc.mode {
            BLOB_MODE_INLINE => desc.locator,
            BLOB_MODE_PACKED => {
                if desc.locator.len() != 8 {
                    return Err(err_data("Invalid packed blob descriptor"));
                }
                let offset = u64::from_le_bytes(desc.locator[..8].try_into().unwrap());
                let packed_path = self.blob_sidecar_dir().join("packed.blob");
                let mut file = File::open(&packed_path)?;
                file.seek(SeekFrom::Start(offset))?;
                let mut data = vec![0u8; desc.len as usize];
                file.read_exact(&mut data)?;
                data
            }
            BLOB_MODE_DEDICATED => {
                let object_id = std::str::from_utf8(&desc.locator)
                    .map_err(|_| err_data("Invalid dedicated blob object id"))?;
                let object_path = self.blob_sidecar_dir().join("objects").join(object_id);
                std::fs::read(object_path)?
            }
            BLOB_MODE_EXTERNAL => {
                return Err(err_data("External blob URI descriptors are not materialized locally"));
            }
            _ => return Err(err_data("Unknown blob descriptor mode")),
        };

        if data.len() as u64 != desc.len {
            return Err(err_data("Blob length mismatch"));
        }
        if crc32fast::hash(&data) != desc.checksum {
            return Err(err_data("Blob checksum mismatch"));
        }
        Ok(data)
    }

    pub fn read_blob_value_range(
        &self,
        descriptor_bytes: &[u8],
        offset: u64,
        length: Option<usize>,
    ) -> io::Result<Vec<u8>> {
        let Some(desc) = decode_blob_descriptor(descriptor_bytes) else {
            let start = (offset as usize).min(descriptor_bytes.len());
            let end = length
                .map(|len| start.saturating_add(len).min(descriptor_bytes.len()))
                .unwrap_or(descriptor_bytes.len());
            return Ok(descriptor_bytes[start..end].to_vec());
        };

        if offset >= desc.len {
            return Ok(Vec::new());
        }
        let max_len = (desc.len - offset) as usize;
        let read_len = length.unwrap_or(max_len).min(max_len);

        match desc.mode {
            BLOB_MODE_INLINE => {
                let start = offset as usize;
                let end = start + read_len;
                Ok(desc.locator[start..end].to_vec())
            }
            BLOB_MODE_PACKED => {
                if desc.locator.len() != 8 {
                    return Err(err_data("Invalid packed blob descriptor"));
                }
                let base_offset = u64::from_le_bytes(desc.locator[..8].try_into().unwrap());
                let packed_path = self.blob_sidecar_dir().join("packed.blob");
                let mut file = File::open(&packed_path)?;
                file.seek(SeekFrom::Start(base_offset + offset))?;
                let mut data = vec![0u8; read_len];
                file.read_exact(&mut data)?;
                Ok(data)
            }
            BLOB_MODE_DEDICATED => {
                let object_id = std::str::from_utf8(&desc.locator)
                    .map_err(|_| err_data("Invalid dedicated blob object id"))?;
                let object_path = self.blob_sidecar_dir().join("objects").join(object_id);
                let mut file = File::open(object_path)?;
                file.seek(SeekFrom::Start(offset))?;
                let mut data = vec![0u8; read_len];
                file.read_exact(&mut data)?;
                Ok(data)
            }
            BLOB_MODE_EXTERNAL => {
                Err(err_data("External blob URI descriptors are not materialized locally"))
            }
            _ => Err(err_data("Unknown blob descriptor mode")),
        }
    }

    fn read_blob_desc_with_cache(
        &self,
        desc: BlobDescriptor,
        cache: &mut BlobReadCache,
        range: Option<(u64, Option<usize>)>,
        verify_full: bool,
    ) -> io::Result<Vec<u8>> {
        let (offset, length) = range.unwrap_or((0, None));
        if offset >= desc.len {
            return Ok(Vec::new());
        }
        let max_len = (desc.len - offset) as usize;
        let read_len = length.unwrap_or(max_len).min(max_len);

        let data = match desc.mode {
            BLOB_MODE_INLINE => {
                let start = offset as usize;
                let end = start + read_len;
                desc.locator[start..end].to_vec()
            }
            BLOB_MODE_PACKED => {
                if desc.locator.len() != 8 {
                    return Err(err_data("Invalid packed blob descriptor"));
                }
                if cache.packed_file.is_none() {
                    cache.packed_file = Some(File::open(self.blob_sidecar_dir().join("packed.blob"))?);
                }
                let base_offset = u64::from_le_bytes(desc.locator[..8].try_into().unwrap());
                let file = cache.packed_file.as_mut().unwrap();
                file.seek(SeekFrom::Start(base_offset + offset))?;
                let mut data = vec![0u8; read_len];
                file.read_exact(&mut data)?;
                data
            }
            BLOB_MODE_DEDICATED => {
                let object_id = std::str::from_utf8(&desc.locator)
                    .map_err(|_| err_data("Invalid dedicated blob object id"))?;
                let object_path = cache.objects_dir.join(object_id);
                let mut file = File::open(object_path)?;
                file.seek(SeekFrom::Start(offset))?;
                let mut data = vec![0u8; read_len];
                file.read_exact(&mut data)?;
                data
            }
            BLOB_MODE_EXTERNAL => {
                return Err(err_data("External blob URI descriptors are not materialized locally"));
            }
            _ => return Err(err_data("Unknown blob descriptor mode")),
        };

        if verify_full && offset == 0 && read_len as u64 == desc.len {
            if data.len() as u64 != desc.len {
                return Err(err_data("Blob length mismatch"));
            }
            if crc32fast::hash(&data) != desc.checksum {
                return Err(err_data("Blob checksum mismatch"));
            }
        }
        Ok(data)
    }

    fn read_blob_bytes_with_cache(
        &self,
        descriptor_bytes: &[u8],
        cache: &mut BlobReadCache,
        range: Option<(u64, Option<usize>)>,
    ) -> io::Result<Vec<u8>> {
        let Some(desc) = decode_blob_descriptor(descriptor_bytes) else {
            let (offset, length) = range.unwrap_or((0, None));
            let start = (offset as usize).min(descriptor_bytes.len());
            let end = length
                .map(|len| start.saturating_add(len).min(descriptor_bytes.len()))
                .unwrap_or(descriptor_bytes.len());
            return Ok(descriptor_bytes[start..end].to_vec());
        };
        self.read_blob_desc_with_cache(desc, cache, range, range.is_none())
    }

    pub fn blob_descriptor_info(descriptor_bytes: &[u8]) -> Option<BlobDescriptorInfo> {
        let desc = decode_blob_descriptor(descriptor_bytes)?;
        let mode = match desc.mode {
            BLOB_MODE_INLINE => BlobStorageMode::Inline,
            BLOB_MODE_PACKED => BlobStorageMode::Packed,
            BLOB_MODE_DEDICATED => BlobStorageMode::Dedicated,
            BLOB_MODE_EXTERNAL => BlobStorageMode::External,
            _ => return None,
        };
        Some(BlobDescriptorInfo {
            mode,
            len: desc.len,
            checksum: desc.checksum,
            locator_len: desc.locator.len(),
        })
    }

    pub fn blob_descriptor_mode(descriptor_bytes: &[u8]) -> Option<BlobStorageMode> {
        match decode_blob_descriptor(descriptor_bytes)?.mode {
            BLOB_MODE_INLINE => Some(BlobStorageMode::Inline),
            BLOB_MODE_PACKED => Some(BlobStorageMode::Packed),
            BLOB_MODE_DEDICATED => Some(BlobStorageMode::Dedicated),
            BLOB_MODE_EXTERNAL => Some(BlobStorageMode::External),
            _ => None,
        }
    }

    pub fn read_blob_descriptor_by_id(
        &self,
        column_name: &str,
        id: u64,
    ) -> io::Result<Option<Vec<u8>>> {
        let col_idx = {
            let schema = self.schema.read();
            let Some(idx) = schema.get_index(column_name) else {
                return Ok(None);
            };
            if schema.columns[idx].1 != ColumnType::Blob {
                return Ok(None);
            }
            idx
        };

        let Some(row_idx) = self.get_row_idx(id) else {
            return Ok(None);
        };
        if self.is_blob_null_at_row(col_idx, row_idx)? {
            return Ok(None);
        }

        let Some(cols) = self.read_row_by_id(id, Some(&[column_name]))? else {
            return Ok(None);
        };
        let Some(ColumnData::Binary { offsets, data }) = cols.get(column_name) else {
            return Ok(None);
        };
        if offsets.len() < 2 {
            return Ok(None);
        }
        let start = offsets[0] as usize;
        let end = offsets[1] as usize;
        if start > end || end > data.len() {
            return Ok(None);
        }
        Ok(Some(data[start..end].to_vec()))
    }

    pub fn read_blob_by_id(&self, column_name: &str, id: u64) -> io::Result<Option<Vec<u8>>> {
        let Some(descriptor) = self.read_blob_descriptor_by_id(column_name, id)? else {
            return Ok(None);
        };
        Ok(Some(self.read_blob_value(&descriptor)?))
    }

    pub fn read_blobs_by_ids(
        &self,
        column_name: &str,
        ids: &[u64],
    ) -> io::Result<Vec<Option<Vec<u8>>>> {
        let descriptors = self.read_blob_descriptors_by_ids(column_name, ids)?;
        let mut cache = BlobReadCache {
            packed_file: None,
            objects_dir: self.blob_sidecar_dir().join("objects"),
        };
        descriptors
            .iter()
            .map(|descriptor| {
                descriptor
                    .as_deref()
                    .map(|bytes| self.read_blob_bytes_with_cache(bytes, &mut cache, None))
                    .transpose()
            })
            .collect()
    }

    pub fn read_blob_range_by_id(
        &self,
        column_name: &str,
        id: u64,
        offset: u64,
        length: Option<usize>,
    ) -> io::Result<Option<Vec<u8>>> {
        let Some(descriptor) = self.read_blob_descriptor_by_id(column_name, id)? else {
            return Ok(None);
        };
        Ok(Some(self.read_blob_value_range(&descriptor, offset, length)?))
    }

    pub fn read_blob_ranges_by_ids(
        &self,
        column_name: &str,
        ids: &[u64],
        offsets: &[u64],
        length: Option<usize>,
    ) -> io::Result<Vec<Option<Vec<u8>>>> {
        if ids.len() != offsets.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ids and offsets must have the same length",
            ));
        }
        let descriptors = self.read_blob_descriptors_by_ids(column_name, ids)?;
        let mut cache = BlobReadCache {
            packed_file: None,
            objects_dir: self.blob_sidecar_dir().join("objects"),
        };
        descriptors
            .iter()
            .zip(offsets.iter())
            .map(|(descriptor, offset)| {
                descriptor
                    .as_deref()
                    .map(|bytes| {
                        self.read_blob_bytes_with_cache(bytes, &mut cache, Some((*offset, length)))
                    })
                    .transpose()
            })
            .collect()
    }

    pub fn read_blob_descriptor_info_by_id(
        &self,
        column_name: &str,
        id: u64,
    ) -> io::Result<Option<BlobDescriptorInfo>> {
        let Some(descriptor) = self.read_blob_descriptor_by_id(column_name, id)? else {
            return Ok(None);
        };
        Ok(Self::blob_descriptor_info(&descriptor))
    }

    pub fn read_blob_descriptor_infos_by_ids(
        &self,
        column_name: &str,
        ids: &[u64],
    ) -> io::Result<Vec<Option<BlobDescriptorInfo>>> {
        let descriptors = self.read_blob_descriptors_by_ids(column_name, ids)?;
        Ok(descriptors
            .iter()
            .map(|descriptor| descriptor.as_deref().and_then(Self::blob_descriptor_info))
            .collect())
    }

    pub fn read_blob_descriptors_by_ids(
        &self,
        column_name: &str,
        ids: &[u64],
    ) -> io::Result<Vec<Option<Vec<u8>>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let col_idx = {
            let schema = self.schema.read();
            let Some(idx) = schema.get_index(column_name) else {
                return Ok(vec![None; ids.len()]);
            };
            if schema.columns[idx].1 != ColumnType::Blob {
                return Ok(vec![None; ids.len()]);
            }
            idx
        };

        let row_indices: Vec<Option<usize>> = ids.iter().map(|id| self.get_row_idx(*id)).collect();

        if self.is_v4_format()
            && !self.has_v4_in_memory_data()
            && self.pending_v4_in_memory_rows() == 0
        {
            if let Some(footer) = self.get_or_load_footer()? {
                if col_idx < footer.schema.column_count() {
                    let (columns, _deleted, nulls) =
                        self.scan_columns_mmap_with_nulls(&[col_idx], &footer)?;
                    let Some(ColumnData::Binary { offsets, data }) = columns.get(0) else {
                        return Ok(vec![None; ids.len()]);
                    };
                    let null_bitmap = nulls.get(0).map_or(&[][..], |bitmap| bitmap.as_slice());
                    return Ok(row_indices
                        .iter()
                        .map(|row_idx| {
                            let row_idx = (*row_idx)?;
                            if null_bit_is_set(null_bitmap, row_idx) {
                                return None;
                            }
                            blob_descriptor_bounds(offsets, data, row_idx)
                                .map(|bytes| bytes.to_vec())
                        })
                        .collect());
                }
            }
        }

        let columns = self.read_columns(Some(&[column_name]), 0, None)?;
        let Some(ColumnData::Binary { offsets, data }) = columns.get(column_name) else {
            return Ok(vec![None; ids.len()]);
        };
        let nulls = self.nulls.read();
        let null_bitmap = nulls.get(col_idx).map_or(&[][..], |bitmap| bitmap.as_slice());

        Ok(ids
            .iter()
            .zip(row_indices.iter())
            .map(|(_id, row_idx)| {
                (*row_idx).and_then(|row_idx| {
                    if null_bit_is_set(null_bitmap, row_idx) {
                        return None;
                    }
                    blob_descriptor_bounds(offsets, data, row_idx).map(|bytes| bytes.to_vec())
                })
            })
            .collect())
    }

    pub fn materialize_blob_column(
        &self,
        offsets: &[u64],
        data: &[u8],
        null_bitmap: &[bool],
        row_count: usize,
    ) -> io::Result<Vec<Option<Vec<u8>>>> {
        let count = offsets.len().saturating_sub(1).min(row_count);
        let mut values = Vec::with_capacity(row_count);
        let mut cache = BlobReadCache {
            packed_file: None,
            objects_dir: self.blob_sidecar_dir().join("objects"),
        };
        for i in 0..row_count {
            if i >= count || (i < null_bitmap.len() && null_bitmap[i]) {
                values.push(None);
                continue;
            }
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            if start > end || end > data.len() {
                values.push(None);
                continue;
            }
            values.push(Some(self.read_blob_bytes_with_cache(
                &data[start..end],
                &mut cache,
                None,
            )?));
        }
        Ok(values)
    }
}

// ---------------------------------------------------------------------------
// Packed-blob vacuum (journaled rename sequence)
// ---------------------------------------------------------------------------

fn packed_vacuum_table_copy(table: &std::path::Path) -> std::path::PathBuf {
    let mut copy = table.to_path_buf();
    let name = copy
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    copy.set_file_name(format!("{name}.vacuum"));
    copy
}

fn packed_vacuum_journal(table: &std::path::Path) -> std::path::PathBuf {
    let mut journal = table.to_path_buf();
    let name = journal
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    journal.set_file_name(format!("{name}.packedvac"));
    journal
}

/// Complete or discard a packed-blob vacuum left behind by a crash.
///
/// Two complete files and a journal are published before any live path changes:
/// the journaled sequence is `packed.blob` → `packed.blob.old`,
/// `packed.blob.vacuum` → `packed.blob`, patched table copy → table. Every step
/// is idempotent and the patched table copy is only visible after the packed
/// payloads it points into are in place. Without a journal, leftover `.vacuum`
/// files are incomplete and safe to remove.
pub(crate) fn recover_packed_vacuum(table: &std::path::Path) -> io::Result<()> {
    let journal = packed_vacuum_journal(table);
    let sidecar = blob_dir_for_path(table);
    let packed = sidecar.join("packed.blob");
    let new_packed = sidecar.join("packed.blob.vacuum");
    let old_packed = sidecar.join("packed.blob.old");
    let table_copy = packed_vacuum_table_copy(table);
    if !journal.exists() {
        let _ = std::fs::remove_file(&new_packed);
        let _ = std::fs::remove_file(&table_copy);
        return Ok(());
    }
    if new_packed.exists() {
        if packed.exists() {
            let _ = std::fs::remove_file(&old_packed);
            std::fs::rename(&packed, &old_packed)?;
        }
        std::fs::rename(&new_packed, &packed)?;
    }
    if table_copy.exists() {
        std::fs::rename(&table_copy, table)?;
    }
    let _ = std::fs::remove_file(&old_packed);
    let _ = std::fs::remove_file(&journal);
    Ok(())
}
