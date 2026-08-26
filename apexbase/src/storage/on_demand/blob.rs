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

struct BlobReadCache {
    packed_file: Option<File>,
    objects_dir: PathBuf,
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
        let decompressed = decompress_rg_body(compress_flag, &rg_bytes[32..])?;
        let body = decompressed.as_deref().unwrap_or(&rg_bytes[32..]);

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
            let consumed = if encoding_version >= 1 {
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
            if self.durability == super::DurabilityLevel::Max {
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
            if self.durability == super::DurabilityLevel::Max {
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
                        if self.durability == super::DurabilityLevel::Max {
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
            if self.durability == super::DurabilityLevel::Max {
                file.sync_all()?;
            }
        }

        Ok(descriptors)
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
