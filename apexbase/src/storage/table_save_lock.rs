//! Per-table read/write coordination for on-disk mutations.
//!
//! The documented contract for the storage engine is:
//!
//! > Reads (…) are lock-free on V4 mmap-only tables. **Writes are serialized
//! > per-table** via an internal write lock in the storage engine.
//!
//! The V4 store keeps an in-memory column buffer per `TableStorageBackend` and
//! flushes it by rewriting or appending to the base `.apex` file. Two threads
//! flushing the same table concurrently would each perform a read-modify-write
//! on one shared file with their own private buffer, which loses rows and fails
//! with `No such file or directory`. This module supplies the lock that makes
//! the documented guarantee true.
//!
//! Design:
//! * one `RwLock` per distinct table path, so unrelated tables stay fully
//!   parallel;
//! * writers (insert / replace / delete / flush) take it exclusively, so a
//!   whole read-modify-write is atomic with respect to other writers;
//! * readers that materialize a backend take it shared, so concurrent reads
//!   keep running in parallel.
//!
//! A second, independent lock per path ([`rewrite_lock`]) serializes base-file
//! replacement. It cannot be the write lock above: that one is held across a
//! whole mutation, and the flush happens *inside* that section, so reusing it
//! would nest a non-reentrant lock and deadlock.
//!
//! Locks are leaked deliberately: there is exactly one per distinct path for the
//! process lifetime, so handing out `&'static` references is sound and avoids any
//! risk of a freed lock being reused by a different table.

use std::path::{Path, PathBuf};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// One read/write lock per distinct table path.
static TABLE_LOCKS: Lazy<DashMap<PathBuf, &'static RwLock<()>>> = Lazy::new(DashMap::new);

/// One base-file rewrite lock per distinct table path.
static REWRITE_LOCKS: Lazy<DashMap<PathBuf, &'static Mutex<()>>> = Lazy::new(DashMap::new);

/// Fetch (or create) the lock for `path`.
///
/// The entry API is used so every caller for a path agrees on a single lock even
/// under a creation race.
fn lock_for(path: &Path) -> &'static RwLock<()> {
    if let Some(existing) = TABLE_LOCKS.get(path) {
        return *existing;
    }
    let candidate: &'static RwLock<()> = Box::leak(Box::new(RwLock::new(())));
    *TABLE_LOCKS
        .entry(path.to_path_buf())
        .or_insert(candidate)
}

/// Take the table's exclusive write lock.
///
/// Hold this across an entire mutation (read-modify-write plus flush) so that
/// concurrent writers to the same table cannot interleave.
pub(crate) fn write_lock(path: &Path) -> RwLockWriteGuard<'static, ()> {
    lock_for(path).write()
}


/// Take the table's shared read lock.
///
/// Used when a reader has to materialize or open the backend. Readers that hit
/// an already-materialized mmap backend never reach this and stay lock-free.
pub(crate) fn read_lock(path: &Path) -> RwLockReadGuard<'static, ()> {
    lock_for(path).read()
}

/// Fetch (or create) the base-file rewrite lock for `path`.
fn rewrite_lock_for(path: &Path) -> &'static Mutex<()> {
    if let Some(existing) = REWRITE_LOCKS.get(path) {
        return *existing;
    }
    let candidate: &'static Mutex<()> = Box::leak(Box::new(Mutex::new(())));
    *REWRITE_LOCKS
        .entry(path.to_path_buf())
        .or_insert(candidate)
}

/// Take the table's exclusive base-file rewrite lock.
///
/// Every path that publishes a new base file (full rewrite, streaming
/// compaction, column-drop rewrite) holds this for the whole write-scratch →
/// `rename` sequence. Without it two participants can rewrite the same table at
/// once and publish out of order, so the older snapshot wins and rows disappear
/// — and, because the scratch file is replaced by `rename`, the loser can also
/// fail outright with `No such file or directory`.
///
/// This is *not* the write lock: writers hold that one across the entire
/// mutation, and the flush runs inside that section.
pub(crate) fn rewrite_lock(path: &Path) -> MutexGuard<'static, ()> {
    rewrite_lock_for(path).lock()
}

/// Run `operation` while holding the table's exclusive write lock.
pub(crate) fn with_write_lock<T>(path: &Path, operation: impl FnOnce() -> T) -> T {
    let _guard = write_lock(path);
    operation()
}

/// Run `operation` while holding the table's shared read lock.
pub(crate) fn with_read_lock<T>(path: &Path, operation: impl FnOnce() -> T) -> T {
    let _guard = read_lock(path);
    operation()
}
