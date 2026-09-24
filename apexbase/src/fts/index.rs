#[derive(Serialize, Deserialize)]
enum WalOp {
    Upsert {
        doc_id: u64,
        terms: Vec<String>,
        tokens: Vec<u32>,
    },
    Delete {
        doc_id: u64,
    },
}

#[derive(Clone)]
enum PostingDocs {
    Empty,
    One(u64),
    Roaring(Box<RoaringTreemap>),
}

impl Default for PostingDocs {
    fn default() -> Self {
        Self::Empty
    }
}

impl PostingDocs {
    fn from_roaring(docs: RoaringTreemap) -> Self {
        match docs.len() {
            0 => Self::Empty,
            1 => Self::One(docs.iter().next().unwrap()),
            _ => Self::Roaring(Box::new(docs)),
        }
    }

    fn insert(&mut self, doc_id: u64) -> bool {
        match self {
            Self::Empty => {
                *self = Self::One(doc_id);
                true
            }
            Self::One(existing) if *existing == doc_id => false,
            Self::One(existing) => {
                let mut docs = RoaringTreemap::new();
                docs.insert(*existing);
                docs.insert(doc_id);
                *self = Self::Roaring(Box::new(docs));
                true
            }
            Self::Roaring(docs) => docs.insert(doc_id),
        }
    }

    fn remove(&mut self, doc_id: u64) -> bool {
        match self {
            Self::Empty => false,
            Self::One(existing) if *existing == doc_id => {
                *self = Self::Empty;
                true
            }
            Self::One(_) => false,
            Self::Roaring(docs) => {
                if !docs.remove(doc_id) {
                    return false;
                }
                if docs.len() == 1 {
                    *self = Self::One(docs.iter().next().unwrap());
                }
                true
            }
        }
    }

    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    fn to_roaring(&self) -> RoaringTreemap {
        match self {
            Self::Empty => RoaringTreemap::new(),
            Self::One(doc_id) => RoaringTreemap::from_iter([*doc_id]),
            Self::Roaring(docs) => (**docs).clone(),
        }
    }

    fn union_into(&self, result: &mut RoaringTreemap) {
        match self {
            Self::Empty => {}
            Self::One(doc_id) => {
                result.insert(*doc_id);
            }
            Self::Roaring(docs) => *result |= docs.as_ref(),
        }
    }

    fn union_owned(&mut self, other: Self) {
        match other {
            Self::Empty => {}
            Self::One(doc_id) => {
                self.insert(doc_id);
            }
            Self::Roaring(other_docs) => match self {
                Self::Empty => *self = Self::Roaring(other_docs),
                Self::One(doc_id) => {
                    let doc_id = *doc_id;
                    let mut other_docs = other_docs;
                    other_docs.insert(doc_id);
                    *self = Self::Roaring(other_docs);
                }
                Self::Roaring(docs) => **docs |= other_docs.as_ref(),
            },
        }
    }

    fn subtract(&mut self, removed: &RoaringTreemap) {
        match self {
            Self::Empty => {}
            Self::One(doc_id) => {
                if removed.contains(*doc_id) {
                    *self = Self::Empty;
                }
            }
            Self::Roaring(docs) => {
                **docs -= removed;
                match docs.len() {
                    0 => *self = Self::Empty,
                    1 => *self = Self::One(docs.iter().next().unwrap()),
                    _ => {}
                }
            }
        }
    }

    fn snapshot_codec(&self) -> u32 {
        match self {
            Self::One(_) => POSTING_CODEC_SINGLE_DOC,
            Self::Empty | Self::Roaring(_) => POSTING_CODEC_ROARING,
        }
    }

    fn snapshot_len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_) => 0,
            Self::Roaring(docs) => docs.serialized_size(),
        }
    }

    fn snapshot_value(&self, posting_cursor: usize) -> u64 {
        match self {
            Self::One(doc_id) => *doc_id,
            Self::Empty | Self::Roaring(_) => posting_cursor as u64,
        }
    }

    fn serialize_snapshot(&self, writer: &mut impl Write) -> io::Result<()> {
        if let Self::Roaring(docs) = self {
            docs.serialize_into(writer)?;
        }
        Ok(())
    }
}

#[derive(Default, Clone)]
struct Posting {
    term_id: u32,
    docs: PostingDocs,
}

#[derive(Clone, Copy)]
struct MmapTermMeta {
    term_offset: usize,
    term_len: usize,
    term_id: u32,
    posting_value: u64,
    posting_len: usize,
    posting_codec: u32,
}

struct MmapTermDirectory {
    mmap: Arc<Mmap>,
    directory_offset: usize,
    term_count: usize,
    cache_capacity: usize,
    cache: Mutex<(AHashMap<usize, Posting>, VecDeque<usize>)>,
}

impl MmapTermDirectory {
    fn new(
        mmap: Arc<Mmap>,
        directory_offset: usize,
        term_count: usize,
        cache_capacity: usize,
    ) -> FtsResult<Self> {
        let directory = Self {
            mmap,
            directory_offset,
            term_count,
            cache_capacity,
            cache: Mutex::new((AHashMap::new(), VecDeque::new())),
        };
        let mut previous: Option<&[u8]> = None;
        for index in 0..term_count {
            let meta = directory.meta(index)?;
            let term = directory.term_bytes(meta)?;
            std::str::from_utf8(term)
                .map_err(|error| FtsError::CorruptIndex(format!("invalid term UTF-8: {error}")))?;
            if previous.is_some_and(|value| value >= term) {
                return Err(FtsError::CorruptIndex(
                    "mmap term directory is not strictly sorted".into(),
                ));
            }
            previous = Some(term);
        }
        Ok(directory)
    }

    fn read_u32_at(&self, offset: usize) -> FtsResult<u32> {
        let bytes = self
            .mmap
            .get(offset..offset.saturating_add(4))
            .ok_or_else(|| {
                FtsError::CorruptIndex("term directory exceeds snapshot bounds".into())
            })?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_u64_at(&self, offset: usize) -> FtsResult<u64> {
        let bytes = self
            .mmap
            .get(offset..offset.saturating_add(8))
            .ok_or_else(|| {
                FtsError::CorruptIndex("term directory exceeds snapshot bounds".into())
            })?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn meta(&self, index: usize) -> FtsResult<MmapTermMeta> {
        if index >= self.term_count {
            return Err(FtsError::CorruptIndex(
                "term directory index out of bounds".into(),
            ));
        }
        let offset = self.directory_offset + index * TERM_DIRECTORY_ENTRY_BYTES;
        let term_offset = self.read_u64_at(offset)? as usize;
        let term_len = self.read_u32_at(offset + 8)? as usize;
        let term_id = self.read_u32_at(offset + 12)?;
        let posting_value = self.read_u64_at(offset + 16)?;
        let posting_len = self.read_u64_at(offset + 24)? as usize;
        let posting_codec = self.read_u32_at(offset + 32)?;
        if term_len > MAX_TERM_BYTES
            || term_offset.checked_add(term_len).is_none()
            || term_offset + term_len > self.mmap.len().saturating_sub(4)
        {
            return Err(FtsError::CorruptIndex(
                "term entry exceeds snapshot bounds".into(),
            ));
        }
        match posting_codec {
            POSTING_CODEC_ROARING => {
                let posting_offset = usize::try_from(posting_value).map_err(|_| {
                    FtsError::CorruptIndex("posting offset exceeds platform bounds".into())
                })?;
                if posting_offset.checked_add(posting_len).is_none()
                    || posting_offset + posting_len > self.mmap.len().saturating_sub(4)
                {
                    return Err(FtsError::CorruptIndex(
                        "posting entry exceeds snapshot bounds".into(),
                    ));
                }
            }
            POSTING_CODEC_SINGLE_DOC if posting_len == 0 => {}
            POSTING_CODEC_SINGLE_DOC => {
                return Err(FtsError::CorruptIndex(
                    "single-document posting has a payload".into(),
                ));
            }
            _ => {
                return Err(FtsError::CorruptIndex(format!(
                    "unsupported posting codec {posting_codec}"
                )));
            }
        }
        Ok(MmapTermMeta {
            term_offset,
            term_len,
            term_id,
            posting_value,
            posting_len,
            posting_codec,
        })
    }

    fn term_bytes(&self, meta: MmapTermMeta) -> FtsResult<&[u8]> {
        self.mmap
            .get(meta.term_offset..meta.term_offset + meta.term_len)
            .ok_or_else(|| FtsError::CorruptIndex("term exceeds snapshot bounds".into()))
    }

    fn term(&self, index: usize) -> FtsResult<&str> {
        let meta = self.meta(index)?;
        std::str::from_utf8(self.term_bytes(meta)?)
            .map_err(|error| FtsError::CorruptIndex(format!("invalid term UTF-8: {error}")))
    }

    fn find(&self, term: &str) -> FtsResult<Option<(usize, MmapTermMeta)>> {
        let mut low = 0usize;
        let mut high = self.term_count;
        while low < high {
            let mid = low + (high - low) / 2;
            let meta = self.meta(mid)?;
            match self.term_bytes(meta)?.cmp(term.as_bytes()) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return Ok(Some((mid, meta))),
            }
        }
        Ok(None)
    }

    fn posting(&self, term: &str) -> FtsResult<Option<Posting>> {
        let Some((index, meta)) = self.find(term)? else {
            return Ok(None);
        };
        if meta.posting_codec == POSTING_CODEC_SINGLE_DOC {
            return Ok(Some(Posting {
                term_id: meta.term_id,
                docs: PostingDocs::One(meta.posting_value),
            }));
        }
        if self.cache_capacity > 0 {
            let mut cache = self.cache.lock();
            if let Some(posting) = cache.0.get(&index).cloned() {
                if let Some(position) = cache.1.iter().position(|cached| *cached == index) {
                    cache.1.remove(position);
                }
                cache.1.push_back(index);
                return Ok(Some(posting));
            }
        }
        let posting = self.decode_posting(meta)?;
        if self.cache_capacity > 0 {
            let mut cache = self.cache.lock();
            if let Some(position) = cache.1.iter().position(|cached| *cached == index) {
                cache.1.remove(position);
            }
            cache.0.insert(index, posting.clone());
            cache.1.push_back(index);
            while cache.0.len() > self.cache_capacity {
                if let Some(evicted) = cache.1.pop_front() {
                    cache.0.remove(&evicted);
                }
            }
        }
        Ok(Some(posting))
    }

    fn decode_posting(&self, meta: MmapTermMeta) -> FtsResult<Posting> {
        if meta.posting_codec == POSTING_CODEC_SINGLE_DOC {
            return Ok(Posting {
                term_id: meta.term_id,
                docs: PostingDocs::One(meta.posting_value),
            });
        }
        let posting_offset = usize::try_from(meta.posting_value)
            .map_err(|_| FtsError::CorruptIndex("posting offset exceeds platform bounds".into()))?;
        let bytes = &self.mmap[posting_offset..posting_offset + meta.posting_len];
        let mut cursor = std::io::Cursor::new(bytes);
        let docs = RoaringTreemap::deserialize_from(&mut cursor)
            .map_err(|error| FtsError::CorruptIndex(format!("invalid mmap posting: {error}")))?;
        if cursor.position() as usize != bytes.len() {
            return Err(FtsError::CorruptIndex(
                "unexpected bytes after mmap posting".into(),
            ));
        }
        Ok(Posting {
            term_id: meta.term_id,
            docs: PostingDocs::from_roaring(docs),
        })
    }

    fn term_id(&self, term: &str) -> Option<u32> {
        self.find(term).ok().flatten().map(|(_, meta)| meta.term_id)
    }

    fn terms(&self) -> FtsResult<Vec<&str>> {
        (0..self.term_count).map(|index| self.term(index)).collect()
    }

    fn materialize(&self) -> FtsResult<AHashMap<String, Posting>> {
        let mut postings = AHashMap::with_capacity(self.term_count);
        for index in 0..self.term_count {
            let meta = self.meta(index)?;
            let term = std::str::from_utf8(self.term_bytes(meta)?)
                .map_err(|error| FtsError::CorruptIndex(format!("invalid term UTF-8: {error}")))?
                .to_owned();
            let posting = self.decode_posting(meta)?;
            postings.insert(term, posting);
        }
        Ok(postings)
    }

    #[cfg(test)]
    fn cached_len(&self) -> usize {
        self.cache.lock().0.len()
    }
}

#[derive(Default)]
struct PackedDocTokens {
    doc_ids: Vec<u64>,
    offsets: Vec<u64>,
    tokens: Vec<u32>,
}

impl PackedDocTokens {
    fn get(&self, doc_id: u64) -> Option<&[u32]> {
        let index = self.doc_ids.binary_search(&doc_id).ok()?;
        Some(&self.tokens[self.offsets[index] as usize..self.offsets[index + 1] as usize])
    }

    fn from_records(mut records: Vec<(u64, Box<[u32]>)>) -> Self {
        records.sort_unstable_by_key(|(doc_id, _)| *doc_id);
        let token_count = records.iter().map(|(_, tokens)| tokens.len()).sum();
        let mut packed = Self {
            doc_ids: Vec::with_capacity(records.len()),
            offsets: Vec::with_capacity(records.len() + 1),
            tokens: Vec::with_capacity(token_count),
        };
        packed.offsets.push(0);
        for (doc_id, tokens) in records {
            packed.doc_ids.push(doc_id);
            packed.tokens.extend_from_slice(&tokens);
            packed.offsets.push(packed.tokens.len() as u64);
        }
        packed
    }

    fn total_terms(&self) -> u64 {
        self.tokens.iter().filter(|&&token| token != 0).count() as u64
    }
}

struct AnalyzedDocument {
    terms: Vec<String>,
    /// One-based indexes into `terms`; zero is a field boundary.
    tokens: Vec<u32>,
}

#[derive(Clone, Copy, Debug)]
pub struct RankedHit {
    pub doc_id: u64,
    pub score: f32,
}

impl PartialEq for RankedHit {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id == other.doc_id && self.score.to_bits() == other.score.to_bits()
    }
}

impl Eq for RankedHit {}

impl PartialOrd for RankedHit {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedHit {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.doc_id.cmp(&self.doc_id))
    }
}

#[derive(Default)]
struct IndexState {
    base_postings: AHashMap<String, Posting>,
    mmap_postings: Option<Arc<MmapTermDirectory>>,
    base_docs: RoaringTreemap,
    base_tokens: PackedDocTokens,
    delta_postings: AHashMap<String, Posting>,
    delta_docs: RoaringTreemap,
    delta_tokens: AHashMap<u64, Box<[u32]>>,
    /// Every base document superseded by a delta upsert or delete.
    shadowed_docs: RoaringTreemap,
    deleted_docs: RoaringTreemap,
    pending_wal: Vec<WalOp>,
    next_term_id: u32,
    needs_rebuild: bool,
}

impl IndexState {
    fn can_build_base(&self) -> bool {
        self.base_postings.is_empty()
            && self.base_docs.is_empty()
            && self.mmap_postings.is_none()
            && self.base_tokens.doc_ids.is_empty()
            && self.delta_postings.is_empty()
            && self.delta_docs.is_empty()
            && self.delta_tokens.is_empty()
            && self.pending_wal.is_empty()
    }

    /// Build an immutable base index directly for the initial backfill.
    ///
    /// The incremental path maintains shadow/delete/WAL state per document. That
    /// work is necessary for updates, but catastrophically expensive for a fresh
    /// million-row index. Initial construction assigns stable term IDs once and
    /// fills the base bitmaps/tokens without touching any delta structures.
    fn build_base(&mut self, analyzed: Vec<(u64, AnalyzedDocument)>) {
        const CARDINALITY_SAMPLE_DOCS: usize = 4096;
        let sample_docs = analyzed.len().min(CARDINALITY_SAMPLE_DOCS);
        let mut sample_terms = AHashSet::new();
        for term in analyzed
            .iter()
            .take(sample_docs)
            .flat_map(|(_, document)| document.terms.iter())
        {
            sample_terms.insert(term.as_str());
        }
        let estimated_terms = if sample_docs == 0 {
            0
        } else {
            sample_terms
                .len()
                .saturating_mul(analyzed.len())
                .div_ceil(sample_docs)
        };
        drop(sample_terms);
        self.base_postings = AHashMap::with_capacity(estimated_terms);

        let mut token_records = Vec::with_capacity(analyzed.len());
        for (doc_id, document) in analyzed {
            self.base_docs.insert(doc_id);
            let mut global_term_ids = Vec::with_capacity(document.terms.len());
            for term in document.terms {
                let posting = self.base_postings.entry(term).or_insert_with(|| {
                    let term_id = self.next_term_id.max(1);
                    self.next_term_id = self.next_term_id.saturating_add(1);
                    Posting {
                        term_id,
                        docs: PostingDocs::Empty,
                    }
                });
                posting.docs.insert(doc_id);
                global_term_ids.push(posting.term_id);
            }
            let tokens = document
                .tokens
                .iter()
                .map(|&local_id| {
                    if local_id == 0 {
                        0
                    } else {
                        global_term_ids
                            .get(local_id as usize - 1)
                            .copied()
                            .unwrap_or(0)
                    }
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            token_records.push((doc_id, tokens));
        }
        self.base_tokens = PackedDocTokens::from_records(token_records);
        self.needs_rebuild = false;
    }

    fn remove_from_delta(&mut self, doc_id: u64) {
        if !self.delta_docs.remove(doc_id) {
            return;
        }
        self.delta_tokens.remove(&doc_id);
        self.delta_postings.retain(|_, posting| {
            posting.docs.remove(doc_id);
            !posting.docs.is_empty()
        });
    }

    fn apply_upsert(&mut self, doc_id: u64, analyzed: AnalyzedDocument, record_wal: bool) {
        self.remove_from_delta(doc_id);
        self.shadowed_docs.insert(doc_id);
        self.deleted_docs.remove(doc_id);
        self.delta_docs.insert(doc_id);

        let mut global_term_ids = Vec::with_capacity(analyzed.terms.len());
        for term in &analyzed.terms {
            let base_term_id = self.term_id(term);
            let posting = self.delta_postings.entry(term.clone()).or_insert_with(|| {
                let term_id = base_term_id.unwrap_or_else(|| {
                    let term_id = self.next_term_id.max(1);
                    self.next_term_id = self.next_term_id.saturating_add(1);
                    if self.next_term_id <= term_id {
                        self.next_term_id = term_id.saturating_add(1);
                    }
                    term_id
                });
                Posting {
                    term_id,
                    docs: PostingDocs::Empty,
                }
            });
            posting.docs.insert(doc_id);
            global_term_ids.push(posting.term_id);
        }

        let tokens: Vec<u32> = analyzed
            .tokens
            .iter()
            .map(|&term_id| {
                if term_id == 0 {
                    0
                } else {
                    global_term_ids
                        .get(term_id as usize - 1)
                        .copied()
                        .unwrap_or(0)
                }
            })
            .collect();
        self.delta_tokens.insert(doc_id, tokens.into_boxed_slice());

        if record_wal {
            self.pending_wal.push(WalOp::Upsert {
                doc_id,
                terms: analyzed.terms,
                tokens: analyzed.tokens,
            });
        }
    }

    fn apply_delete(&mut self, doc_id: u64, record_wal: bool) {
        self.remove_from_delta(doc_id);
        self.shadowed_docs.insert(doc_id);
        self.deleted_docs.insert(doc_id);
        if record_wal {
            self.pending_wal.push(WalOp::Delete { doc_id });
        }
    }

    fn posting(&self, term: &str) -> FtsResult<RoaringTreemap> {
        let mut result = if let Some(posting) = self.base_postings.get(term) {
            posting.docs.to_roaring()
        } else if let Some(directory) = &self.mmap_postings {
            directory
                .posting(term)?
                .map(|posting| posting.docs.to_roaring())
                .unwrap_or_default()
        } else {
            RoaringTreemap::new()
        };
        result -= &self.shadowed_docs;
        if let Some(delta) = self.delta_postings.get(term) {
            delta.docs.union_into(&mut result);
        }
        result -= &self.deleted_docs;
        Ok(result)
    }

    fn term_id(&self, term: &str) -> Option<u32> {
        self.delta_postings
            .get(term)
            .or_else(|| self.base_postings.get(term))
            .map(|posting| posting.term_id)
            .or_else(|| {
                self.mmap_postings
                    .as_ref()
                    .and_then(|directory| directory.term_id(term))
            })
    }

    fn tokens_for_doc(&self, doc_id: u64) -> Option<&[u32]> {
        if self.deleted_docs.contains(doc_id) {
            return None;
        }
        if let Some(tokens) = self.delta_tokens.get(&doc_id) {
            return Some(tokens);
        }
        if self.shadowed_docs.contains(doc_id) {
            return None;
        }
        self.base_tokens.get(doc_id)
    }

    fn live_term_total(&self) -> u64 {
        let shadowed_base_terms: u64 = self
            .shadowed_docs
            .iter()
            .filter_map(|doc_id| self.base_tokens.get(doc_id))
            .map(|tokens| tokens.iter().filter(|&&token| token != 0).count() as u64)
            .sum();
        let delta_terms: u64 = self
            .delta_tokens
            .iter()
            .filter(|(doc_id, _)| !self.deleted_docs.contains(**doc_id))
            .map(|(_, tokens)| tokens.iter().filter(|&&token| token != 0).count() as u64)
            .sum();
        self.base_tokens
            .total_terms()
            .saturating_sub(shadowed_base_terms)
            + delta_terms
    }

    fn live_docs(&self) -> RoaringTreemap {
        let mut docs = self.base_docs.clone();
        docs -= &self.shadowed_docs;
        docs |= &self.delta_docs;
        docs -= &self.deleted_docs;
        docs
    }

    /// Fold the delta into the immutable snapshot representation.
    fn merge_delta(&mut self) -> FtsResult<()> {
        if let Some(directory) = self.mmap_postings.take() {
            self.base_postings = directory.materialize()?;
        }
        if !self.shadowed_docs.is_empty() {
            self.base_postings.retain(|_, posting| {
                posting.docs.subtract(&self.shadowed_docs);
                !posting.docs.is_empty()
            });
            self.base_docs -= &self.shadowed_docs;
        }
        for (term, delta) in std::mem::take(&mut self.delta_postings) {
            if let Some(base) = self.base_postings.get_mut(&term) {
                base.docs.union_owned(delta.docs);
            } else {
                self.base_postings.insert(term, delta);
            }
        }

        let mut token_records =
            Vec::with_capacity(self.base_tokens.doc_ids.len() + self.delta_tokens.len());
        for (index, &doc_id) in self.base_tokens.doc_ids.iter().enumerate() {
            if !self.shadowed_docs.contains(doc_id) && !self.deleted_docs.contains(doc_id) {
                let start = self.base_tokens.offsets[index] as usize;
                let end = self.base_tokens.offsets[index + 1] as usize;
                token_records.push((doc_id, self.base_tokens.tokens[start..end].into()));
            }
        }
        for (doc_id, tokens) in std::mem::take(&mut self.delta_tokens) {
            if !self.deleted_docs.contains(doc_id) {
                token_records.push((doc_id, tokens));
            }
        }
        self.base_tokens = PackedDocTokens::from_records(token_records);
        self.base_docs |= &self.delta_docs;
        self.base_docs -= &self.deleted_docs;
        self.delta_docs.clear();
        self.shadowed_docs.clear();
        self.deleted_docs.clear();
        Ok(())
    }

    fn base_terms(&self) -> FtsResult<Vec<&str>> {
        let mut terms: Vec<&str> = self.base_postings.keys().map(String::as_str).collect();
        if let Some(directory) = &self.mmap_postings {
            terms.extend(directory.terms()?);
        }
        Ok(terms)
    }
}
