//! ApexFTS: ApexBase-owned full-text index.
//!
//! The index is a rebuildable secondary structure with an atomic `.afts`
//! snapshot and a checksummed `.afts.wal`.  A small in-memory delta shadows
//! immutable snapshot postings, so updates and deletes are immediately correct
//! without rewriting the whole index on every table mutation.

use ahash::{AHashMap, AHashSet};
use memmap2::Mmap;
use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering as CmpOrdering, Reverse};
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

const SNAPSHOT_MAGIC: &[u8; 8] = b"APEXFTS1";
const SNAPSHOT_VERSION: u32 = 3;
const LEGACY_SNAPSHOT_VERSION: u32 = 2;
const ANALYZER_VERSION: u32 = 2;
const SNAPSHOT_HEADER_BYTES: usize = 72;
const TERM_DIRECTORY_ENTRY_BYTES: usize = 40;
const POSTING_CODEC_ROARING: u32 = 0;
const POSTING_CODEC_SINGLE_DOC: u32 = 1;
const WAL_SNAPSHOT_THRESHOLD: usize = 100_000;
const MAX_WAL_RECORD_BYTES: usize = 64 * 1024 * 1024;
const MAX_TERM_BYTES: usize = 1024 * 1024;
/// WAL generation-2 file header: every record carries the table epoch it was
/// applied at, so a crash can be reconciled against the table generation.
const WAL_MAGIC: &[u8; 8] = b"APXFTSW2";
/// Sidecar recording which table generation/row set a snapshot was built from.
const SOURCE_META_MAGIC: &[u8; 8] = b"APXFTSM1";
const SOURCE_META_BYTES: usize = 36;

#[derive(Clone, Debug, PartialEq)]
pub struct FtsConfig {
    /// Maximum n-gram size for Han, Hiragana, Katakana and Hangul runs.
    pub max_chinese_length: usize,
    /// Minimum Unicode scalar count for non-CJK terms.
    pub min_term_length: usize,
    pub fuzzy_threshold: f64,
    pub fuzzy_max_distance: usize,
    pub fuzzy_max_candidates: usize,
    /// Kept for API compatibility. ApexFTS always supports update/delete.
    pub track_doc_terms: bool,
    /// Keep the immutable term directory and postings mmap-backed after reopen.
    pub lazy_load: bool,
    /// Maximum number of lazily decoded posting bitmaps retained in memory.
    pub cache_size: usize,
}

impl Default for FtsConfig {
    fn default() -> Self {
        Self {
            max_chinese_length: 3,
            min_term_length: 2,
            fuzzy_threshold: 0.7,
            fuzzy_max_distance: 2,
            fuzzy_max_candidates: 20,
            track_doc_terms: true,
            lazy_load: false,
            cache_size: 10_000,
        }
    }
}

#[derive(Debug)]
pub enum FtsError {
    Io(io::Error),
    CorruptIndex(String),
    BackgroundFlush(String),
}

impl std::fmt::Display for FtsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "ApexFTS I/O error: {e}"),
            Self::CorruptIndex(e) => write!(f, "ApexFTS index error: {e}"),
            Self::BackgroundFlush(e) => write!(f, "ApexFTS background flush error: {e}"),
        }
    }
}

impl std::error::Error for FtsError {}

impl From<io::Error> for FtsError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

pub type FtsResult<T> = Result<T, FtsError>;

#[derive(Clone)]
pub struct ResultHandle {
    bitmap: Arc<RoaringTreemap>,
}

impl ResultHandle {
    fn new(bitmap: RoaringTreemap) -> Self {
        Self {
            bitmap: Arc::new(bitmap),
        }
    }

    pub fn total_hits(&self) -> u64 {
        self.bitmap.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.bitmap.iter()
    }

    pub fn page(&self, offset: usize, limit: usize) -> Vec<u64> {
        self.bitmap.iter().skip(offset).take(limit).collect()
    }

    pub(crate) fn shared_bitmap(&self) -> Arc<RoaringTreemap> {
        Arc::clone(&self.bitmap)
    }
}

include!("index.rs");
include!("engine.rs");
include!("query.rs");
include!("analyzer.rs");
include!("storage.rs");
include!("tests.rs");
