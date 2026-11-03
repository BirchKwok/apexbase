fn write_u32(writer: &mut impl Write, value: u32) -> io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

fn write_u64(writer: &mut impl Write, value: u64) -> io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

struct CrcReader<R> {
    inner: R,
    hasher: crc32fast::Hasher,
}

impl<R: Read> Read for CrcReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.hasher.update(&buffer[..read]);
        Ok(read)
    }
}

struct CrcWriter<W> {
    inner: W,
    hasher: crc32fast::Hasher,
}

impl<W: Write> Write for CrcWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.hasher.update(&buffer[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Table generation/row-set a snapshot was built from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SourceMeta {
    pub rows: u64,
    pub epoch: u64,
    pub generation: u64,
}

fn source_meta_path(index_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.meta", index_path.display()))
}

/// Atomically replace `dest` with `temp`.
///
/// `std::fs::rename` cannot replace an existing file on Windows, so a rebuild
/// that regains an index file would otherwise lose its source metadata. This
/// keeps the same atomic rename on Unix and falls back to remove+rename on
/// Windows (or any platform that refuses to overwrite).
fn replace_file(temp: &Path, dest: &Path) -> io::Result<()> {
    match fs::rename(temp, dest) {
        Ok(()) => Ok(()),
        Err(_) if dest.exists() => {
            fs::remove_file(dest)?;
            fs::rename(temp, dest)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn read_source_meta(index_path: &Path) -> FtsResult<Option<SourceMeta>> {
    let path = source_meta_path(index_path);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if bytes.len() != SOURCE_META_BYTES || &bytes[..8] != SOURCE_META_MAGIC {
        return Err(FtsError::CorruptIndex("invalid ApexFTS source metadata".into()));
    }
    let expected_crc = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
    if crc32fast::hash(&bytes[..32]) != expected_crc {
        return Err(FtsError::CorruptIndex("source metadata checksum mismatch".into()));
    }
    Ok(Some(SourceMeta {
        rows: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        epoch: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        generation: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
    }))
}

fn write_source_meta(index_path: &Path, meta: SourceMeta) -> FtsResult<()> {
    let path = source_meta_path(index_path);
    let temp_path = PathBuf::from(format!("{}.tmp", path.display()));
    let mut bytes = Vec::with_capacity(SOURCE_META_BYTES);
    bytes.extend_from_slice(SOURCE_META_MAGIC);
    bytes.extend_from_slice(&meta.rows.to_le_bytes());
    bytes.extend_from_slice(&meta.epoch.to_le_bytes());
    bytes.extend_from_slice(&meta.generation.to_le_bytes());
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    fs::write(&temp_path, &bytes)?;
    replace_file(&temp_path, &path)?;
    Ok(())
}

pub(super) fn remove_source_meta(index_path: &Path) {
    let _ = fs::remove_file(source_meta_path(index_path));
}

fn load_snapshot_v2(path: &Path, state: &mut IndexState) -> FtsResult<()> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if file_len < 36 {
        return Err(FtsError::CorruptIndex("truncated ApexFTS snapshot".into()));
    }
    file.seek(SeekFrom::End(-4))?;
    let expected_crc = read_u32(&mut file)?;
    file.seek(SeekFrom::Start(0))?;
    let payload_len = file_len - 4;
    let mut reader = CrcReader {
        inner: BufReader::new(file).take(payload_len),
        hasher: crc32fast::Hasher::new(),
    };
    let mut magic = [0; 8];
    reader
        .read_exact(&mut magic)
        .map_err(|e| FtsError::CorruptIndex(format!("truncated snapshot header: {e}")))?;
    if &magic != SNAPSHOT_MAGIC {
        return Err(FtsError::CorruptIndex(format!(
            "unsupported index format at {}",
            path.display()
        )));
    }
    let version = read_u32(&mut reader)
        .map_err(|e| FtsError::CorruptIndex(format!("invalid snapshot version: {e}")))?;
    let analyzer = read_u32(&mut reader)
        .map_err(|e| FtsError::CorruptIndex(format!("invalid analyzer version: {e}")))?;
    if version != LEGACY_SNAPSHOT_VERSION || analyzer != ANALYZER_VERSION {
        return Err(FtsError::CorruptIndex(format!(
            "index/analyzer version {version}/{analyzer} is not supported"
        )));
    }
    let term_count = read_u64(&mut reader)
        .map_err(|e| FtsError::CorruptIndex(format!("invalid term directory: {e}")))?;
    for _ in 0..term_count {
        let term_len = read_u32(&mut reader)
            .map_err(|e| FtsError::CorruptIndex(format!("invalid term length: {e}")))?
            as usize;
        if term_len > MAX_TERM_BYTES || term_len > reader.inner.limit() as usize {
            return Err(FtsError::CorruptIndex(
                "term exceeds snapshot bounds".into(),
            ));
        }
        let mut term = vec![0; term_len];
        reader
            .read_exact(&mut term)
            .map_err(|e| FtsError::CorruptIndex(format!("truncated term: {e}")))?;
        let term_id = read_u32(&mut reader)
            .map_err(|e| FtsError::CorruptIndex(format!("invalid term id: {e}")))?;
        let bitmap_len = read_u64(&mut reader)
            .map_err(|e| FtsError::CorruptIndex(format!("invalid bitmap length: {e}")))?;
        if bitmap_len > reader.inner.limit() {
            return Err(FtsError::CorruptIndex(
                "posting bitmap exceeds snapshot bounds".into(),
            ));
        }
        let mut limited = reader.by_ref().take(bitmap_len);
        let docs = RoaringTreemap::deserialize_from(&mut limited)
            .map_err(|e| FtsError::CorruptIndex(format!("invalid posting bitmap: {e}")))?;
        io::copy(&mut limited, &mut io::sink())
            .map_err(|e| FtsError::CorruptIndex(format!("truncated posting bitmap: {e}")))?;
        let term = String::from_utf8(term)
            .map_err(|e| FtsError::CorruptIndex(format!("invalid term UTF-8: {e}")))?;
        state.base_postings.insert(
            term,
            Posting {
                term_id,
                docs: PostingDocs::from_roaring(docs),
            },
        );
        state.next_term_id = state.next_term_id.max(term_id.saturating_add(1));
    }
    let docs_len = read_u64(&mut reader)
        .map_err(|e| FtsError::CorruptIndex(format!("invalid document bitmap length: {e}")))?;
    if docs_len > reader.inner.limit() {
        return Err(FtsError::CorruptIndex(
            "document bitmap exceeds snapshot bounds".into(),
        ));
    }
    let mut docs_reader = reader.by_ref().take(docs_len);
    state.base_docs = RoaringTreemap::deserialize_from(&mut docs_reader)
        .map_err(|e| FtsError::CorruptIndex(format!("invalid document bitmap: {e}")))?;
    io::copy(&mut docs_reader, &mut io::sink())
        .map_err(|e| FtsError::CorruptIndex(format!("truncated document bitmap: {e}")))?;
    let token_doc_count = read_u64(&mut reader)
        .map_err(|e| FtsError::CorruptIndex(format!("invalid token document count: {e}")))?;
    if token_doc_count > reader.inner.limit() / 12 {
        return Err(FtsError::CorruptIndex(
            "token document directory exceeds snapshot bounds".into(),
        ));
    }
    let mut token_records = Vec::with_capacity(token_doc_count as usize);
    for _ in 0..token_doc_count {
        let doc_id = read_u64(&mut reader)
            .map_err(|e| FtsError::CorruptIndex(format!("invalid token document id: {e}")))?;
        let token_count = read_u32(&mut reader)
            .map_err(|e| FtsError::CorruptIndex(format!("invalid document token count: {e}")))?
            as usize;
        if token_count > reader.inner.limit() as usize / 4 {
            return Err(FtsError::CorruptIndex(
                "document tokens exceed snapshot bounds".into(),
            ));
        }
        let mut tokens = Vec::with_capacity(token_count);
        for _ in 0..token_count {
            tokens.push(
                read_u32(&mut reader).map_err(|e| {
                    FtsError::CorruptIndex(format!("truncated document tokens: {e}"))
                })?,
            );
        }
        token_records.push((doc_id, tokens.into_boxed_slice()));
    }
    state.base_tokens = PackedDocTokens::from_records(token_records);
    if reader.inner.limit() != 0 {
        return Err(FtsError::CorruptIndex(
            "unexpected data at end of snapshot".into(),
        ));
    }
    if reader.hasher.finalize() != expected_crc {
        return Err(FtsError::CorruptIndex("snapshot checksum mismatch".into()));
    }
    state.needs_rebuild = false;
    Ok(())
}

fn replay_wal(path: &Path, state: &mut IndexState) -> FtsResult<u64> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file.try_clone()?);
    let generation_two = file_len >= WAL_MAGIC.len() as u64
        && {
            let mut magic = [0u8; 8];
            reader.read_exact(&mut magic)?;
            let matches = &magic == WAL_MAGIC;
            if !matches {
                // Legacy WAL: reset to the start and replay the old layout.
                use std::io::Seek;
                file.try_clone()?.seek(SeekFrom::Start(0))?;
            }
            matches
        };
    if generation_two {
        // header consumed
    } else {
        reader = BufReader::new(file.try_clone()?);
    }
    let mut valid_len = if generation_two {
        WAL_MAGIC.len() as u64
    } else {
        0
    };
    let mut max_epoch = 0u64;
    loop {
        let len = match read_u32(&mut reader) {
            Ok(len) => len as usize,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        };
        if len > MAX_WAL_RECORD_BYTES {
            break;
        }
        let expected_crc = match read_u32(&mut reader) {
            Ok(crc) => crc,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        };
        let epoch = if generation_two {
            match read_u64(&mut reader) {
                Ok(epoch) => epoch,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        } else {
            0
        };
        let mut payload = vec![0; len];
        if let Err(e) = reader.read_exact(&mut payload) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(e.into());
        }
        let mut crc_input = Vec::with_capacity(8 + payload.len());
        if generation_two {
            crc_input.extend_from_slice(&epoch.to_le_bytes());
        }
        crc_input.extend_from_slice(&payload);
        if crc32fast::hash(&crc_input) != expected_crc {
            break;
        }
        let op: WalOp = match bincode::deserialize(&payload) {
            Ok(op) => op,
            Err(_) => break,
        };
        match op {
            WalOp::Upsert {
                doc_id,
                terms,
                tokens,
            } => state.apply_upsert(doc_id, AnalyzedDocument { terms, tokens }, false),
            WalOp::Delete { doc_id } => state.apply_delete(doc_id, false),
        }
        max_epoch = max_epoch.max(epoch);
        valid_len += 8 + len as u64 + if generation_two { 8 } else { 0 };
    }
    drop(reader);
    if valid_len < file_len {
        file.set_len(valid_len)?;
        file.sync_data()?;
    }
    Ok(max_epoch)
}

fn append_pending_wal(core: &EngineCore) -> FtsResult<usize> {
    let mut state = core.state.write();
    if state.pending_wal.is_empty() {
        return Ok(0);
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&core.wal_path)?;
    if file.metadata()?.len() == 0 {
        file.write_all(WAL_MAGIC)?;
    }
    // The epoch is captured once per flush (the table write that produced
    // these pending ops already bumped it), so recovery can tell whether the
    // table generation moved past everything the WAL accounts for.
    let epoch = core
        .table_path
        .as_ref()
        .map(|path| crate::storage::epoch::current(path))
        .unwrap_or(0);
    let count = state.pending_wal.len();
    for op in &state.pending_wal {
        let payload = bincode::serialize(op)
            .map_err(|e| FtsError::CorruptIndex(format!("cannot encode WAL entry: {e}")))?;
        let epoch_bytes = epoch.to_le_bytes();
        let mut crc_input = Vec::with_capacity(8 + payload.len());
        crc_input.extend_from_slice(&epoch_bytes);
        crc_input.extend_from_slice(&payload);
        write_u32(&mut file, payload.len() as u32)?;
        write_u32(&mut file, crc32fast::hash(&crc_input))?;
        file.write_all(&epoch_bytes)?;
        file.write_all(&payload)?;
    }
    file.sync_data()?;
    core.wal_epoch.fetch_max(epoch, std::sync::atomic::Ordering::AcqRel);
    state.pending_wal.clear();
    Ok(count)
}

fn slice_u32(bytes: &[u8], offset: usize) -> FtsResult<u32> {
    let value = bytes
        .get(offset..offset.saturating_add(4))
        .ok_or_else(|| FtsError::CorruptIndex("truncated ApexFTS v3 metadata".into()))?;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn slice_u64(bytes: &[u8], offset: usize) -> FtsResult<u64> {
    let value = bytes
        .get(offset..offset.saturating_add(8))
        .ok_or_else(|| FtsError::CorruptIndex("truncated ApexFTS v3 metadata".into()))?;
    Ok(u64::from_le_bytes(value.try_into().unwrap()))
}

fn load_snapshot(path: &Path, state: &mut IndexState, config: &FtsConfig) -> FtsResult<()> {
    let file = File::open(path)?;
    if file.metadata()?.len() < 16 {
        return Err(FtsError::CorruptIndex("truncated ApexFTS snapshot".into()));
    }
    let mmap = Arc::new(unsafe { Mmap::map(&file)? });
    if mmap.get(..8) != Some(SNAPSHOT_MAGIC.as_slice()) {
        return Err(FtsError::CorruptIndex(format!(
            "unsupported index format at {}",
            path.display()
        )));
    }
    match slice_u32(&mmap, 8)? {
        SNAPSHOT_VERSION => load_snapshot_v3(mmap, state, config),
        LEGACY_SNAPSHOT_VERSION => load_snapshot_v2(path, state),
        version => Err(FtsError::CorruptIndex(format!(
            "index version {version} is not supported"
        ))),
    }
}

fn load_snapshot_v3(mmap: Arc<Mmap>, state: &mut IndexState, config: &FtsConfig) -> FtsResult<()> {
    if mmap.len() < SNAPSHOT_HEADER_BYTES + 4 {
        return Err(FtsError::CorruptIndex(
            "truncated ApexFTS v3 snapshot".into(),
        ));
    }
    let analyzer = slice_u32(&mmap, 12)?;
    if analyzer != ANALYZER_VERSION {
        return Err(FtsError::CorruptIndex(format!(
            "analyzer version {analyzer} is not supported"
        )));
    }
    let term_count = usize::try_from(slice_u64(&mmap, 16)?)
        .map_err(|_| FtsError::CorruptIndex("term count exceeds platform bounds".into()))?;
    let directory_offset = slice_u64(&mmap, 24)? as usize;
    let term_data_offset = slice_u64(&mmap, 32)? as usize;
    let postings_offset = slice_u64(&mmap, 40)? as usize;
    let docs_offset = slice_u64(&mmap, 48)? as usize;
    let tokens_offset = slice_u64(&mmap, 56)? as usize;
    let payload_end = slice_u64(&mmap, 64)? as usize;
    let directory_end = directory_offset
        .checked_add(
            term_count
                .checked_mul(TERM_DIRECTORY_ENTRY_BYTES)
                .ok_or_else(|| FtsError::CorruptIndex("term directory overflow".into()))?,
        )
        .ok_or_else(|| FtsError::CorruptIndex("term directory overflow".into()))?;
    if directory_offset != SNAPSHOT_HEADER_BYTES
        || directory_end > term_data_offset
        || term_data_offset > postings_offset
        || postings_offset > docs_offset
        || docs_offset > tokens_offset
        || tokens_offset > payload_end
        || payload_end.checked_add(4) != Some(mmap.len())
    {
        return Err(FtsError::CorruptIndex(
            "invalid ApexFTS v3 section directory".into(),
        ));
    }
    if !config.lazy_load {
        let expected = slice_u32(&mmap, payload_end)?;
        if crc32fast::hash(&mmap[..payload_end]) != expected {
            return Err(FtsError::CorruptIndex("snapshot checksum mismatch".into()));
        }
    }

    let directory = Arc::new(MmapTermDirectory::new(
        Arc::clone(&mmap),
        directory_offset,
        term_count,
        if config.lazy_load {
            config.cache_size
        } else {
            0
        },
    )?);

    let docs_len = slice_u64(&mmap, docs_offset)? as usize;
    let docs_start = docs_offset + 8;
    let docs_end = docs_start
        .checked_add(docs_len)
        .ok_or_else(|| FtsError::CorruptIndex("document bitmap overflow".into()))?;
    if docs_end > tokens_offset {
        return Err(FtsError::CorruptIndex(
            "document bitmap exceeds its section".into(),
        ));
    }
    state.base_docs =
        RoaringTreemap::deserialize_from(&mut std::io::Cursor::new(&mmap[docs_start..docs_end]))
            .map_err(|error| FtsError::CorruptIndex(format!("invalid document bitmap: {error}")))?;

    let token_doc_count = usize::try_from(slice_u64(&mmap, tokens_offset)?).map_err(|_| {
        FtsError::CorruptIndex("token document count exceeds platform bounds".into())
    })?;
    let mut cursor = tokens_offset + 8;
    let mut records = Vec::with_capacity(token_doc_count);
    for _ in 0..token_doc_count {
        let doc_id = slice_u64(&mmap, cursor)?;
        let count = slice_u32(&mmap, cursor + 8)? as usize;
        cursor = cursor
            .checked_add(12)
            .ok_or_else(|| FtsError::CorruptIndex("token directory overflow".into()))?;
        let byte_len = count
            .checked_mul(4)
            .ok_or_else(|| FtsError::CorruptIndex("token record overflow".into()))?;
        if cursor.checked_add(byte_len).is_none() || cursor + byte_len > payload_end {
            return Err(FtsError::CorruptIndex(
                "token record exceeds snapshot bounds".into(),
            ));
        }
        let mut tokens = Vec::with_capacity(count);
        for offset in (cursor..cursor + byte_len).step_by(4) {
            tokens.push(slice_u32(&mmap, offset)?);
        }
        cursor += byte_len;
        records.push((doc_id, tokens.into_boxed_slice()));
    }
    if cursor != payload_end {
        return Err(FtsError::CorruptIndex(
            "unexpected bytes after token records".into(),
        ));
    }
    state.base_tokens = PackedDocTokens::from_records(records);
    state.next_term_id = (0..term_count)
        .filter_map(|index| directory.meta(index).ok().map(|meta| meta.term_id))
        .max()
        .unwrap_or(0)
        .saturating_add(1)
        .max(1);
    if config.lazy_load {
        state.base_postings.clear();
        state.mmap_postings = Some(directory);
    } else {
        state.base_postings = directory.materialize()?;
        state.mmap_postings = None;
    }
    state.needs_rebuild = false;
    Ok(())
}

fn persist_snapshot(core: &EngineCore) -> FtsResult<usize> {
    {
        let mut state = core.state.write();
        state.merge_delta()?;
        state.needs_rebuild = false;
    }

    let temp_path = PathBuf::from(format!("{}.tmp", core.index_path.display()));
    let config = core.config.read().clone();
    let state = core.state.read();
    let mut terms: Vec<&String> = state
        .base_postings
        .iter()
        .filter_map(|(term, posting)| (!posting.docs.is_empty()).then_some(term))
        .collect();
    terms.sort_unstable();
    let written_terms = terms.len();
    let directory_offset = SNAPSHOT_HEADER_BYTES;
    let term_data_offset = directory_offset
        .checked_add(
            written_terms
                .checked_mul(TERM_DIRECTORY_ENTRY_BYTES)
                .ok_or_else(|| FtsError::CorruptIndex("term directory overflow".into()))?,
        )
        .ok_or_else(|| FtsError::CorruptIndex("term directory overflow".into()))?;
    let postings_offset = term_data_offset
        .checked_add(terms.iter().map(|term| term.len()).sum::<usize>())
        .ok_or_else(|| FtsError::CorruptIndex("term data overflow".into()))?;
    let posting_sizes: Vec<usize> = terms
        .iter()
        .map(|term| state.base_postings[*term].docs.snapshot_len())
        .collect();
    let docs_offset = postings_offset
        .checked_add(posting_sizes.iter().sum::<usize>())
        .ok_or_else(|| FtsError::CorruptIndex("posting data overflow".into()))?;
    let docs_size = state.base_docs.serialized_size();
    let tokens_offset = docs_offset
        .checked_add(8 + docs_size)
        .ok_or_else(|| FtsError::CorruptIndex("document data overflow".into()))?;
    let token_bytes = state
        .base_tokens
        .doc_ids
        .len()
        .checked_mul(12)
        .and_then(|fixed| {
            state
                .base_tokens
                .tokens
                .len()
                .checked_mul(4)
                .and_then(|tokens| fixed.checked_add(tokens))
        })
        .ok_or_else(|| FtsError::CorruptIndex("token data overflow".into()))?;
    let payload_end = tokens_offset
        .checked_add(8 + token_bytes)
        .ok_or_else(|| FtsError::CorruptIndex("snapshot size overflow".into()))?;

    let file = File::create(&temp_path)?;
    let mut writer = CrcWriter {
        inner: BufWriter::new(file),
        hasher: crc32fast::Hasher::new(),
    };
    writer.write_all(SNAPSHOT_MAGIC)?;
    write_u32(&mut writer, SNAPSHOT_VERSION)?;
    write_u32(&mut writer, ANALYZER_VERSION)?;
    write_u64(&mut writer, written_terms as u64)?;
    write_u64(&mut writer, directory_offset as u64)?;
    write_u64(&mut writer, term_data_offset as u64)?;
    write_u64(&mut writer, postings_offset as u64)?;
    write_u64(&mut writer, docs_offset as u64)?;
    write_u64(&mut writer, tokens_offset as u64)?;
    write_u64(&mut writer, payload_end as u64)?;

    let mut term_cursor = term_data_offset;
    let mut posting_cursor = postings_offset;
    for (term, posting_len) in terms.iter().zip(&posting_sizes) {
        let posting = &state.base_postings[*term];
        write_u64(&mut writer, term_cursor as u64)?;
        write_u32(&mut writer, term.len() as u32)?;
        write_u32(&mut writer, posting.term_id)?;
        write_u64(
            &mut writer,
            posting.docs.snapshot_value(posting_cursor),
        )?;
        write_u64(&mut writer, *posting_len as u64)?;
        write_u32(&mut writer, posting.docs.snapshot_codec())?;
        write_u32(&mut writer, 0)?;
        term_cursor += term.len();
        posting_cursor += posting_len;
    }
    for term in &terms {
        writer.write_all(term.as_bytes())?;
    }
    for term in &terms {
        state.base_postings[*term].docs.serialize_snapshot(&mut writer)?;
    }
    write_u64(&mut writer, docs_size as u64)?;
    state.base_docs.serialize_into(&mut writer)?;
    write_u64(&mut writer, state.base_tokens.doc_ids.len() as u64)?;
    for (index, &doc_id) in state.base_tokens.doc_ids.iter().enumerate() {
        let start = state.base_tokens.offsets[index] as usize;
        let end = state.base_tokens.offsets[index + 1] as usize;
        write_u64(&mut writer, doc_id)?;
        write_u32(&mut writer, (end - start) as u32)?;
        for &token in &state.base_tokens.tokens[start..end] {
            write_u32(&mut writer, token)?;
        }
    }
    writer.flush()?;
    let CrcWriter { inner, hasher } = writer;
    let checksum = hasher.finalize();
    drop(inner);
    drop(state);
    let mut file = OpenOptions::new().append(true).open(&temp_path)?;
    write_u32(&mut file, checksum)?;
    file.flush()?;
    drop(file);
    replace_file(&temp_path, &core.index_path)?;
    if core.wal_path.exists() {
        fs::remove_file(&core.wal_path)?;
    }
    let previous_generation = core
        .snapshot_meta
        .read()
        .map(|meta| meta.generation)
        .unwrap_or(0);
    let source_meta = SourceMeta {
        rows: core.state.read().live_docs().len() as u64,
        epoch: core
            .table_path
            .as_ref()
            .map(|path| crate::storage::epoch::current(path))
            .unwrap_or(0),
        generation: previous_generation.saturating_add(1),
    };
    write_source_meta(&core.index_path, source_meta)?;
    *core.snapshot_meta.write() = Some(source_meta);
    core.wal_epoch.store(0, std::sync::atomic::Ordering::Release);
    // A fresh snapshot accounts for the table: clear a previous stale verdict so
    // MATCH is accepted again after the rebuild.
    *core.stale_reason.write() = None;

    if config.lazy_load {
        let file = File::open(&core.index_path)?;
        let mmap = Arc::new(unsafe { Mmap::map(&file)? });
        let directory = Arc::new(MmapTermDirectory::new(
            mmap,
            directory_offset,
            written_terms,
            config.cache_size,
        )?);
        let mut state = core.state.write();
        state.base_postings.clear();
        state.mmap_postings = Some(directory);
    }
    Ok(written_terms)
}
