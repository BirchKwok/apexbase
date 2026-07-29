struct EngineCore {
    index_path: PathBuf,
    wal_path: PathBuf,
    config: RwLock<FtsConfig>,
    state: RwLock<IndexState>,
    search_count: AtomicU64,
}

pub struct FtsEngine {
    core: Arc<EngineCore>,
    flush_handle: Mutex<Option<std::thread::JoinHandle<FtsResult<usize>>>>,
}

impl FtsEngine {
    pub fn new<P: AsRef<Path>>(index_path: P, config: FtsConfig) -> FtsResult<Self> {
        let index_path = index_path.as_ref().to_path_buf();
        if let Some(parent) = index_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let wal_path = PathBuf::from(format!("{}.wal", index_path.display()));
        let mut state = IndexState::default();
        state.needs_rebuild = !index_path.exists();
        if index_path.exists() {
            if let Err(error) = load_snapshot(&index_path, &mut state, &config) {
                match error {
                    FtsError::CorruptIndex(_) => {
                        let quarantine =
                            PathBuf::from(format!("{}.incompatible", index_path.display()));
                        let _ = fs::rename(&index_path, quarantine);
                        let _ = fs::remove_file(&wal_path);
                        state = IndexState::default();
                        state.needs_rebuild = true;
                    }
                    other => return Err(other),
                }
            }
        }
        if wal_path.exists() {
            replay_wal(&wal_path, &mut state)?;
        }
        Ok(Self {
            core: Arc::new(EngineCore {
                index_path,
                wal_path,
                config: RwLock::new(config),
                state: RwLock::new(state),
                search_count: AtomicU64::new(0),
            }),
            flush_handle: Mutex::new(None),
        })
    }

    pub fn memory_only(config: FtsConfig) -> FtsResult<Self> {
        Ok(Self {
            core: Arc::new(EngineCore {
                index_path: PathBuf::new(),
                wal_path: PathBuf::new(),
                config: RwLock::new(config),
                state: RwLock::new(IndexState::default()),
                search_count: AtomicU64::new(0),
            }),
            flush_handle: Mutex::new(None),
        })
    }

    pub fn needs_rebuild(&self) -> bool {
        self.core.state.read().needs_rebuild
    }

    pub fn add_document(&self, doc_id: u64, fields: HashMap<String, String>) -> FtsResult<()> {
        let values: Vec<String> = fields.into_values().collect();
        let analyzed = analyze_values(values.iter().map(String::as_str), &self.core.config.read());
        let record_wal = self.core.index_path.exists();
        self.core
            .state
            .write()
            .apply_upsert(doc_id, analyzed, record_wal);
        Ok(())
    }

    pub fn add_documents(&self, docs: Vec<(u64, HashMap<String, String>)>) -> FtsResult<()> {
        let config = self.core.config.read().clone();
        let analyzed: Vec<(u64, AnalyzedDocument)> = docs
            .into_par_iter()
            .map(|(id, fields)| {
                let values: Vec<String> = fields.into_values().collect();
                (
                    id,
                    analyze_values(values.iter().map(String::as_str), &config),
                )
            })
            .collect();
        self.apply_analyzed(analyzed);
        Ok(())
    }

    pub fn add_documents_texts(&self, doc_ids: Vec<u64>, texts: Vec<String>) -> FtsResult<()> {
        if doc_ids.len() != texts.len() {
            return Err(FtsError::CorruptIndex(
                "document/text length mismatch".into(),
            ));
        }
        let config = self.core.config.read().clone();
        let analyzed = doc_ids
            .into_par_iter()
            .zip(texts.into_par_iter())
            .map(|(id, text)| (id, analyze_document(&text, &config)))
            .collect();
        self.apply_analyzed(analyzed);
        Ok(())
    }

    pub fn add_documents_columnar(
        &self,
        doc_ids: Vec<u64>,
        columns: Vec<(String, Vec<String>)>,
    ) -> FtsResult<()> {
        let refs: Vec<(String, Vec<&str>)> = columns
            .iter()
            .map(|(name, values)| (name.clone(), values.iter().map(String::as_str).collect()))
            .collect();
        self.add_documents_arrow_str(&doc_ids, refs)
    }

    pub fn add_documents_arrow_texts(&self, doc_ids: &[u64], texts: &[&str]) -> FtsResult<()> {
        if doc_ids.len() != texts.len() {
            return Err(FtsError::CorruptIndex(
                "document/text length mismatch".into(),
            ));
        }
        let config = self.core.config.read().clone();
        let analyzed = doc_ids
            .par_iter()
            .zip(texts.par_iter())
            .map(|(&id, text)| (id, analyze_document(text, &config)))
            .collect();
        self.apply_analyzed(analyzed);
        Ok(())
    }

    pub fn add_documents_arrow_str(
        &self,
        doc_ids: &[u64],
        columns: Vec<(String, Vec<&str>)>,
    ) -> FtsResult<()> {
        if columns
            .iter()
            .any(|(_, values)| values.len() != doc_ids.len())
        {
            return Err(FtsError::CorruptIndex(
                "document/column length mismatch".into(),
            ));
        }
        let config = self.core.config.read().clone();
        let analyzed: Vec<(u64, AnalyzedDocument)> = (0..doc_ids.len())
            .into_par_iter()
            .map(|row| {
                (
                    doc_ids[row],
                    analyze_values(columns.iter().map(|(_, values)| values[row]), &config),
                )
            })
            .collect();
        self.apply_analyzed(analyzed);
        Ok(())
    }

    fn apply_analyzed(&self, analyzed: Vec<(u64, AnalyzedDocument)>) {
        let mut state = self.core.state.write();
        if analyzed.is_empty() {
            return;
        }
        if !self.core.index_path.exists() && state.can_build_base() {
            state.build_base(analyzed);
            return;
        }
        state.needs_rebuild = false;
        let record_wal = self.core.index_path.exists();
        for (id, document) in analyzed {
            state.apply_upsert(id, document, record_wal);
        }
    }

    pub fn remove_document(&self, doc_id: u64) -> FtsResult<()> {
        let record_wal = self.core.index_path.exists();
        self.core.state.write().apply_delete(doc_id, record_wal);
        Ok(())
    }

    pub fn remove_documents(&self, doc_ids: &[u64]) -> FtsResult<()> {
        let mut state = self.core.state.write();
        let record_wal = self.core.index_path.exists();
        for &doc_id in doc_ids {
            state.apply_delete(doc_id, record_wal);
        }
        Ok(())
    }

    pub fn update_document(&self, doc_id: u64, fields: HashMap<String, String>) -> FtsResult<()> {
        self.add_document(doc_id, fields)
    }

    pub fn search(&self, query: &str) -> FtsResult<ResultHandle> {
        self.core.search_count.fetch_add(1, Ordering::Relaxed);
        let config = self.core.config.read().clone();
        let state = self.core.state.read();
        if let Some(result) = matching_boolean_query(&state, query, &config)? {
            return Ok(ResultHandle::new(result));
        }
        let (query, phrase) = phrase_query(query);
        let analyzed = analyze_document(query, &config);
        if analyzed.terms.is_empty() {
            return Ok(ResultHandle::new(RoaringTreemap::new()));
        }
        let result = matching_docs(&state, &analyzed, phrase)?;
        Ok(ResultHandle::new(result))
    }

    pub fn fuzzy_search(&self, query: &str, min_results: usize) -> FtsResult<ResultHandle> {
        let exact = self.search(query)?;
        if exact.total_hits() >= min_results as u64 {
            return Ok(exact);
        }
        let config = self.core.config.read().clone();
        let query_terms = analyze(query, &config);
        if query_terms.is_empty() {
            return Ok(ResultHandle::new(RoaringTreemap::new()));
        }
        let state = self.core.state.read();
        let mut all_terms: AHashSet<&str> = state.base_terms()?.into_iter().collect();
        all_terms.extend(state.delta_postings.keys().map(String::as_str));
        let max_candidates = config.fuzzy_max_candidates.clamp(1, 256);
        let mut best = RoaringTreemap::new();
        let mut threshold = config.fuzzy_threshold;
        loop {
            let mut result: Option<RoaringTreemap> = None;
            for query_term in &query_terms {
                let mut candidates: Vec<(&str, usize, f64)> = all_terms
                    .iter()
                    .filter_map(|term| {
                        let distance =
                            levenshtein(query_term, term, config.fuzzy_max_distance)?;
                        let width = query_term.chars().count().max(term.chars().count()).max(1);
                        let similarity = 1.0 - distance as f64 / width as f64;
                        (similarity >= threshold).then_some((*term, distance, similarity))
                    })
                    .collect();
                candidates.sort_unstable_by(|a, b| {
                    a.1.cmp(&b.1).then_with(|| {
                        b.2.partial_cmp(&a.2)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                });
                candidates.truncate(max_candidates);
                let mut word_docs = RoaringTreemap::new();
                for (term, _, _) in candidates {
                    word_docs |= state.posting(term)?;
                }
                result = Some(match result {
                    Some(mut current) => {
                        current &= word_docs;
                        current
                    }
                    None => word_docs,
                });
            }
            best = result.unwrap_or_default();
            if best.len() >= min_results as u64 || threshold <= 0.0 {
                break;
            }
            threshold = (threshold - 0.1).max(0.0);
        }
        Ok(ResultHandle::new(best))
    }

    pub fn search_ids(&self, query: &str) -> FtsResult<Vec<u64>> {
        Ok(self.search(query)?.iter().collect())
    }

    pub fn search_ranked(&self, query: &str, limit: usize) -> FtsResult<Vec<RankedHit>> {
        self.core.search_count.fetch_add(1, Ordering::Relaxed);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (query, phrase) = phrase_query(query);
        let analyzed = analyze_document(query, &self.core.config.read());
        if analyzed.terms.is_empty() {
            return Ok(Vec::new());
        }
        let state = self.core.state.read();
        let candidates = matching_docs(&state, &analyzed, phrase)?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let mut query_terms: Vec<(&str, u32)> = analyzed
            .tokens
            .iter()
            .filter_map(|&local_id| {
                if local_id == 0 {
                    return None;
                }
                let term = analyzed.terms.get(local_id as usize - 1)?;
                state.term_id(term).map(|term_id| (term.as_str(), term_id))
            })
            .collect();
        query_terms.sort_unstable_by_key(|(_, term_id)| *term_id);
        query_terms.dedup_by_key(|(_, term_id)| *term_id);
        if query_terms.is_empty() {
            return Ok(Vec::new());
        }

        let doc_count = state.live_docs().len().max(1) as f32;
        let average_length = (state.live_term_total() as f32 / doc_count).max(1.0);
        let mut idf = Vec::with_capacity(query_terms.len());
        for (term, term_id) in query_terms {
            let document_frequency = state.posting(term)?.len() as f32;
            let score =
                ((doc_count - document_frequency + 0.5) / (document_frequency + 0.5) + 1.0).ln();
            idf.push((term_id, score));
        }

        const K1: f32 = 1.2;
        const B: f32 = 0.75;
        let heap_limit = limit.min(candidates.len() as usize);
        let mut top = BinaryHeap::with_capacity(heap_limit.saturating_add(1));
        for doc_id in candidates.iter() {
            let Some(tokens) = state.tokens_for_doc(doc_id) else {
                continue;
            };
            let document_length = tokens.iter().filter(|&&token| token != 0).count() as f32;
            let normalization = K1 * (1.0 - B + B * document_length / average_length);
            let mut score = 0.0f32;
            for &(term_id, term_idf) in &idf {
                let frequency = tokens.iter().filter(|&&token| token == term_id).count() as f32;
                if frequency > 0.0 {
                    score += term_idf * frequency * (K1 + 1.0) / (frequency + normalization);
                }
            }
            let hit = RankedHit { doc_id, score };
            if top.len() < heap_limit {
                top.push(Reverse(hit));
            } else if top.peek().is_some_and(|worst| hit > worst.0) {
                top.pop();
                top.push(Reverse(hit));
            }
        }
        let mut hits: Vec<RankedHit> = top.into_iter().map(|Reverse(hit)| hit).collect();
        hits.sort_unstable_by(|left, right| right.cmp(left));
        Ok(hits)
    }

    /// Return every BM25-scored match. SQL `FTS_SCORE()` uses this once per
    /// distinct query and shares the resulting doc-id map across projection and
    /// ordering expressions.
    pub fn search_scored(&self, query: &str) -> FtsResult<Vec<RankedHit>> {
        self.search_ranked(query, usize::MAX)
    }

    pub fn search_top_n(&self, query: &str, n: usize) -> FtsResult<Vec<u64>> {
        Ok(self
            .search_ranked(query, n)?
            .into_iter()
            .map(|hit| hit.doc_id)
            .collect())
    }

    pub fn search_page(&self, query: &str, offset: usize, limit: usize) -> FtsResult<Vec<u64>> {
        Ok(self
            .search_ranked(query, offset.saturating_add(limit))?
            .into_iter()
            .skip(offset)
            .map(|hit| hit.doc_id)
            .collect())
    }

    pub fn flush(&self) -> FtsResult<()> {
        self.wait_flush()?;
        if self.core.index_path.as_os_str().is_empty() {
            return Ok(());
        }
        let snapshot = !self.core.index_path.exists()
            || self.core.state.read().pending_wal.len() >= WAL_SNAPSHOT_THRESHOLD;
        if snapshot {
            persist_snapshot(&self.core)?;
        } else {
            append_pending_wal(&self.core)?;
        }
        Ok(())
    }

    pub fn flush_async(&self) -> FtsResult<()> {
        self.wait_flush()?;
        if self.core.index_path.as_os_str().is_empty() {
            return Ok(());
        }
        let core = Arc::clone(&self.core);
        let snapshot = !core.index_path.exists()
            || core.state.read().pending_wal.len() >= WAL_SNAPSHOT_THRESHOLD;
        *self.flush_handle.lock() = Some(std::thread::spawn(move || {
            if snapshot {
                persist_snapshot(&core)
            } else {
                append_pending_wal(&core)
            }
        }));
        Ok(())
    }

    pub fn wait_flush(&self) -> FtsResult<usize> {
        match self.flush_handle.lock().take() {
            Some(handle) => handle
                .join()
                .map_err(|_| FtsError::BackgroundFlush("flush thread panicked".into()))?,
            None => Ok(0),
        }
    }

    pub fn compact(&self) -> FtsResult<()> {
        self.wait_flush()?;
        if !self.core.index_path.as_os_str().is_empty() {
            persist_snapshot(&self.core)?;
        }
        Ok(())
    }

    pub fn stats(&self) -> HashMap<String, u64> {
        let state = self.core.state.read();
        let mut stats = HashMap::new();
        stats.insert("doc_count".into(), state.live_docs().len());
        let mut terms: AHashSet<&str> = state.base_terms().unwrap_or_default().into_iter().collect();
        terms.extend(state.delta_postings.keys().map(String::as_str));
        let live_term_count = terms
            .into_iter()
            .filter(|term| state.posting(term).is_ok_and(|posting| !posting.is_empty()))
            .count();
        stats.insert("term_count".into(), live_term_count as u64);
        stats.insert("deleted_count".into(), state.deleted_docs.len());
        stats.insert(
            "search_count".into(),
            self.core.search_count.load(Ordering::Relaxed),
        );
        stats
    }

    pub fn set_fuzzy_config(&self, threshold: f64, max_distance: usize, max_candidates: usize) {
        let mut config = self.core.config.write();
        config.fuzzy_threshold = threshold.clamp(0.0, 1.0);
        config.fuzzy_max_distance = max_distance;
        config.fuzzy_max_candidates = max_candidates;
    }

    pub fn warmup_terms(&self, terms: &[String]) -> usize {
        let config = self.core.config.read();
        let analyzed: AHashSet<String> = terms
            .iter()
            .flat_map(|term| analyze(term, &config))
            .collect();
        let state = self.core.state.read();
        analyzed
            .iter()
            .filter(|term| {
                state
                    .posting(term.as_str())
                    .is_ok_and(|docs| !docs.is_empty())
            })
            .count()
    }
}

pub struct FtsManager {
    base_path: PathBuf,
    engines: RwLock<HashMap<String, Arc<FtsEngine>>>,
    table_configs: RwLock<HashMap<String, FtsConfig>>,
    default_config: FtsConfig,
}

impl FtsManager {
    pub fn new<P: AsRef<Path>>(base_path: P, config: FtsConfig) -> Self {
        let base_path = base_path.as_ref().to_path_buf();
        let _ = fs::create_dir_all(&base_path);
        Self {
            base_path,
            engines: RwLock::new(HashMap::new()),
            table_configs: RwLock::new(HashMap::new()),
            default_config: config,
        }
    }

    pub fn configure_table(&self, table_name: &str, config: FtsConfig) {
        self.table_configs
            .write()
            .insert(table_name.to_string(), config.clone());
        if let Some(engine) = self.engines.read().get(table_name) {
            *engine.core.config.write() = config;
        }
    }

    pub fn get_engine(&self, table_name: &str) -> FtsResult<Arc<FtsEngine>> {
        if let Some(engine) = self.engines.read().get(table_name) {
            return Ok(Arc::clone(engine));
        }
        let mut engines = self.engines.write();
        if let Some(engine) = engines.get(table_name) {
            return Ok(Arc::clone(engine));
        }
        let config = self
            .table_configs
            .read()
            .get(table_name)
            .cloned()
            .unwrap_or_else(|| self.default_config.clone());
        let index_path = self.base_path.join(format!("{table_name}.afts"));
        let engine = Arc::new(FtsEngine::new(index_path, config)?);
        engines.insert(table_name.to_string(), Arc::clone(&engine));
        Ok(engine)
    }

    pub fn remove_engine(&self, table_name: &str, delete_files: bool) -> FtsResult<()> {
        if let Some(engine) = self.engines.write().remove(table_name) {
            engine.wait_flush()?;
        }
        self.table_configs.write().remove(table_name);
        if delete_files {
            for suffix in ["afts", "afts.wal", "afts.tmp", "nfts", "nfts.wal"] {
                let path = self.base_path.join(format!("{table_name}.{suffix}"));
                if path.exists() {
                    fs::remove_file(path)?;
                }
            }
        }
        Ok(())
    }

    pub fn flush_all(&self) -> FtsResult<()> {
        for engine in self.engines.read().values() {
            engine.flush()?;
        }
        Ok(())
    }
}
