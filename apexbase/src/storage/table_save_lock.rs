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
//!   whole read-modify-write is atomic with respect to other writers.
//!
//! Readers do not take this lock: the documented contract is that reads are
//! lock-free on V4 mmap-only tables, and they stay correct through atomic
//! publication (a new base file is renamed into place), the per-table epoch
//! that invalidates stale cached backends, and bounded retries around the
//! short window in which a publish is visible.
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
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockWriteGuard};

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

/// Take the table's exclusive write lock only when it is free.
///
/// A path reached both from a mutation (which already owns the lock) and from a
/// reader uses this: the caller is then either the exclusive owner already, or
/// takes it for the section it is about to run.
pub(crate) fn try_write_lock(path: &Path) -> Option<RwLockWriteGuard<'static, ()>> {
    lock_for(path).try_write()
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
