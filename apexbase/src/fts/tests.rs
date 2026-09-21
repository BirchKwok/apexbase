#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_analyzer_covers_multiple_scripts_and_punctuation() {
        let config = FtsConfig::default();
        let engine = FtsEngine::memory_only(config).unwrap();
        let docs = vec![
            (1, "Hello, café"),
            (2, "Привет мир"),
            (3, "مرحبا بالعالم"),
            (4, "日本語テスト"),
            (5, "한국어 검색"),
            (6, "人工智能数据库"),
        ];
        for (id, text) in docs {
            engine.add_documents_arrow_texts(&[id], &[text]).unwrap();
        }
        for (query, id) in [
            ("hello,", 1),
            ("CAFÉ", 1),
            ("привет", 2),
            ("مرحبا", 3),
            ("テスト", 4),
            ("한국어", 5),
            ("人工智能", 6),
        ] {
            assert_eq!(engine.search_ids(query).unwrap(), vec![id]);
        }
    }

    #[test]
    fn bm25_ranks_frequency_and_phrase_requires_adjacent_tokens() {
        let engine = FtsEngine::memory_only(FtsConfig::default()).unwrap();
        engine
            .add_documents_arrow_texts(
                &[1, 2, 3],
                &[
                    "quick brown fox rust rust rust",
                    "quick red brown fox rust",
                    "slow brown fox",
                ],
            )
            .unwrap();

        let ranked = engine.search_ranked("rust", 10).unwrap();
        assert_eq!(
            ranked.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(ranked[0].score > ranked[1].score);
        assert_eq!(engine.search_ids("quick brown").unwrap(), vec![1, 2]);
        assert_eq!(engine.search_ids("\"quick brown\"").unwrap(), vec![1]);
    }

    #[test]
    fn initial_batch_builds_base_without_delta_bookkeeping() {
        let engine = FtsEngine::memory_only(FtsConfig::default()).unwrap();
        engine
            .add_documents_arrow_texts(&[1, 2], &["alpha beta", "beta gamma"])
            .unwrap();
        let state = engine.core.state.read();
        assert_eq!(state.base_docs.len(), 2);
        assert!(state.delta_docs.is_empty());
        assert!(state.shadowed_docs.is_empty());
        assert!(state.pending_wal.is_empty());
    }

    #[test]
    fn posting_docs_inline_singletons_and_demote_after_removal() {
        let mut docs = PostingDocs::default();
        assert!(docs.insert(u32::MAX as u64 + 9));
        assert!(matches!(docs, PostingDocs::One(_)));
        assert!(!docs.insert(u32::MAX as u64 + 9));

        assert!(docs.insert(7));
        assert!(matches!(docs, PostingDocs::Roaring(_)));
        assert!(docs.remove(7));
        assert!(matches!(docs, PostingDocs::One(id) if id == u32::MAX as u64 + 9));
        assert!(docs.remove(u32::MAX as u64 + 9));
        assert!(docs.is_empty());
    }

    #[test]
    fn lazy_v3_snapshot_mmaps_directory_and_bounds_posting_cache() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lazy.afts");
        let config = FtsConfig { lazy_load: true, cache_size: 1, ..FtsConfig::default() };
        {
            let engine = FtsEngine::new(&path, config.clone()).unwrap();
            engine
                .add_documents_arrow_texts(
                    &[1, 2, 3],
                    &["alpha beta beta", "beta gamma", "delta alpha"],
                )
                .unwrap();
            engine.compact().unwrap();
            let state = engine.core.state.read();
            assert!(state.base_postings.is_empty());
            assert!(state.mmap_postings.is_some());
        }
        let bytes = fs::read(&path).unwrap();
        assert_eq!(u32::from_le_bytes(bytes[8..12].try_into().unwrap()), SNAPSHOT_VERSION);
        let term_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let directory_offset = u64::from_le_bytes(bytes[24..32].try_into().unwrap()) as usize;
        let codecs = (0..term_count)
            .map(|index| {
                let offset = directory_offset + index * TERM_DIRECTORY_ENTRY_BYTES + 32;
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
            })
            .collect::<Vec<_>>();
        assert!(codecs.contains(&POSTING_CODEC_ROARING));
        assert!(codecs.contains(&POSTING_CODEC_SINGLE_DOC));

        let engine = FtsEngine::new(&path, config).unwrap();
        assert_eq!(engine.search_ids("alpha").unwrap(), vec![1, 3]);
        let cached_after_roaring = {
            let state = engine.core.state.read();
            state.mmap_postings.as_ref().unwrap().cached_len()
        };
        assert_eq!(engine.search_ids("gamma").unwrap(), vec![2]);
        let state = engine.core.state.read();
        let directory = state.mmap_postings.as_ref().unwrap();
        assert_eq!(directory.cached_len(), cached_after_roaring);
        assert!(directory.cached_len() <= 1);
    }

    #[test]
    fn phrase_tokens_and_scores_survive_snapshot_and_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ranked.afts");
        {
            let engine = FtsEngine::new(&path, FtsConfig::default()).unwrap();
            engine
                .add_documents_arrow_texts(&[1], &["alpha beta beta"])
                .unwrap();
            engine.flush().unwrap();
            engine
                .add_documents_arrow_texts(&[2], &["alpha beta"])
                .unwrap();
            engine.flush().unwrap();
        }
        let engine = FtsEngine::new(&path, FtsConfig::default()).unwrap();
        assert_eq!(engine.search_ids("\"alpha beta\"").unwrap(), vec![1, 2]);
        let ranked = engine.search_ranked("beta", 10).unwrap();
        assert_eq!(ranked[0].doc_id, 1);
        assert!(ranked[0].score > ranked[1].score);
    }

    #[test]
    fn update_delete_and_u64_ids_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.afts");
        let high_id = u32::MAX as u64 + 17;
        {
            let engine = FtsEngine::new(&path, FtsConfig::default()).unwrap();
            engine
                .add_documents_arrow_texts(&[high_id], &["old value"])
                .unwrap();
            engine.flush().unwrap();
            engine
                .add_documents_arrow_texts(&[high_id], &["new value"])
                .unwrap();
            assert!(engine.search_ids("old").unwrap().is_empty());
            assert_eq!(engine.search_ids("new").unwrap(), vec![high_id]);
            engine.flush().unwrap();
        }
        {
            let engine = FtsEngine::new(&path, FtsConfig::default()).unwrap();
            assert!(engine.search_ids("old").unwrap().is_empty());
            assert_eq!(engine.search_ids("new").unwrap(), vec![high_id]);
            engine.remove_document(high_id).unwrap();
            engine.flush().unwrap();
        }
        let engine = FtsEngine::new(&path, FtsConfig::default()).unwrap();
        assert!(engine.search_ids("new").unwrap().is_empty());
    }

    #[test]
    fn corrupt_snapshot_is_quarantined_for_source_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.afts");
        {
            let engine = FtsEngine::new(&path, FtsConfig::default()).unwrap();
            engine
                .add_documents_arrow_texts(&[1], &["checksum protected"])
                .unwrap();
            engine.flush().unwrap();
        }
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            file.seek(SeekFrom::Start(12)).unwrap();
            file.write_all(&[0xff]).unwrap();
            file.sync_all().unwrap();
        }

        let engine = FtsEngine::new(&path, FtsConfig::default()).unwrap();
        assert!(engine.needs_rebuild());
        assert!(engine.search_ids("checksum").unwrap().is_empty());
        assert!(dir.path().join("docs.afts.incompatible").exists());
    }

    #[test]
    fn truncated_wal_tail_is_repaired_before_future_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.afts");
        let wal_path = dir.path().join("docs.afts.wal");
        {
            let engine = FtsEngine::new(&path, FtsConfig::default()).unwrap();
            engine.add_documents_arrow_texts(&[1], &["base"]).unwrap();
            engine.flush().unwrap();
            engine
                .add_documents_arrow_texts(&[2], &["wal two"])
                .unwrap();
            engine.flush().unwrap();
        }
        let valid_len = fs::metadata(&wal_path).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap()
            .write_all(&[1, 2, 3])
            .unwrap();
        {
            let engine = FtsEngine::new(&path, FtsConfig::default()).unwrap();
            assert_eq!(engine.search_ids("two").unwrap(), vec![2]);
            assert_eq!(fs::metadata(&wal_path).unwrap().len(), valid_len);
            engine
                .add_documents_arrow_texts(&[3], &["wal three"])
                .unwrap();
            engine.flush().unwrap();
        }
        let engine = FtsEngine::new(&path, FtsConfig::default()).unwrap();
        assert_eq!(engine.search_ids("three").unwrap(), vec![3]);
    }

    #[test]
    fn memory_only_engine_requires_its_first_backfill() {
        // A process-local engine loads no snapshot, so it must report a pending
        // rebuild. Otherwise a Python init_fts() issued after writes would
        // never back-fill the rows that already exist.
        let engine = FtsEngine::memory_only(FtsConfig::default()).unwrap();
        assert!(engine.needs_rebuild());
        engine.add_documents_arrow_texts(&[1], &["rust"]).unwrap();
        assert!(!engine.needs_rebuild());
        assert_eq!(engine.search_ids("rust").unwrap(), vec![1]);
    }
}
