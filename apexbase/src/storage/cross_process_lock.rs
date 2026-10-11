//! Cross-process write exclusion for one table.
//!
//! `RESOURCE_OWNERSHIP.md` promises that writes to one table are serialized
//! across processes. The in-process lock ([`super::table_save_lock`]) only covers
//! one process, and the public write paths used to take their `flock` on
//! different call sites — or not at all: the Python single-row (`store_one`) and
//! columnar (`store_columnar`) fast paths published through
//! `Database::write_typed*`, which only takes the in-process lock. Two processes
//! writing one table through different paths could therefore read the same base,
//! append their own rows and let the last publish win, silently dropping the
//! other writer's rows.
//!
//! Every layer now goes through [`lock_write`]: the Python bindings, the
//! executor's DML path and the storage write lock
//! ([`super::table_save_lock::write_lock`], which every storage mutation takes).
//!
//! The guard is **re-entrant per thread**. `flock` is associated with the open
//! file description, so a nested acquisition on a second descriptor would block
//! (and fail after the retry budget) instead of succeeding; a thread that already
//! holds the table's lock gets a no-op guard instead.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use once_cell::sync::Lazy;

use fs2::FileExt;

/// Lock file path for one table (`<table>.apex` → `<table>.apex.lock`).
///
/// This is the single naming rule: the SQL/executor path, the Python bindings,
/// the epoch tracker and this module must all agree on it.
#[inline]
pub fn lock_path(table_path: &Path) -> PathBuf {
    table_path.with_extension("apex.lock")
}

/// Retry budget for a contended lock, matching the bindings' historical
/// behaviour (they surfaced "Database is locked" after 30 s of backoff).
const MAX_WAIT: Duration = Duration::from_secs(30);
const MIN_BACKOFF: Duration = Duration::from_micros(100);
const MAX_BACKOFF: Duration = Duration::from_millis(5);

/// Drop the cached lock descriptors under `base_dir` that nobody is holding.
///
/// The cache saves an `open`/`close` per write, but a cached descriptor keeps
/// `<table>.apex.lock` open for the lifetime of the process. A client that closes
/// a database calls this (next to `table_catalog::release`) so the directory stops
/// holding file handles: Windows refuses to remove a directory while one of its
/// files is open. An entry whose lock another thread still holds must stay
/// cached — dropping its descriptor would release the `flock` out from under it.
pub fn release(base_dir: &Path) {
    EXCLUSIVE_LOCKS.retain(|path, entry| {
        if !path.starts_with(base_dir) {
            return true;
        }
        entry
            .owner
            .lock()
            .map(|owner| owner.is_some())
            .unwrap_or(true)
    });
}

/// One cached exclusive-lock descriptor per table, plus the thread that owns it.
///
/// The descriptor is cached because `open`/`close` cost far more than the `flock`
/// itself on the write hot path (per-statement `INSERT` would pay ~10-20 µs).
/// Caching is only safe together with the ownership handshake below: `flock`
/// belongs to the open file description, so a second thread re-locking the same
/// descriptor would succeed instead of waiting and every writer would proceed.
struct ExclusiveLock {
    file: File,
    owner: Mutex<Option<std::thread::ThreadId>>,
    available: Condvar,
}

static EXCLUSIVE_LOCKS: Lazy<DashMap<PathBuf, Arc<ExclusiveLock>>> = Lazy::new(DashMap::new);

/// Held cross-process lock. Dropping it releases the lock.
pub struct LockGuard {
    path: PathBuf,
    /// `Some` while this guard owns the exclusive lock (a nested acquisition and
    /// an in-memory table leave it `None`).
    owned: Option<Arc<ExclusiveLock>>,
    /// Shared locks use their own descriptor so several readers keep running.
    shared: Option<File>,
}

impl LockGuard {
    /// Table path this guard was acquired for.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether this guard actually holds the lock (false for a nested acquisition
    /// and for in-memory tables).
    #[inline]
    pub fn holds_lock(&self) -> bool {
        self.owned.is_some() || self.shared.is_some()
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(entry) = self.owned.take() {
            let _ = entry.file.unlock();
            let mut owner = entry
                .owner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            *owner = None;
            entry.available.notify_one();
        }
        if let Some(file) = self.shared.take() {
            let _ = file.unlock();
        }
    }
}

/// Acquire the exclusive cross-process write lock for one table.
pub fn lock_write(table_path: &Path) -> io::Result<LockGuard> {
    acquire_exclusive(table_path)
}

/// Acquire the shared cross-process lock for one table.
pub fn lock_read(table_path: &Path) -> io::Result<LockGuard> {
    let path = table_path.to_path_buf();
    if crate::storage::is_memory_path(table_path) {
        return Ok(LockGuard {
            path,
            owned: None,
            shared: None,
        });
    }
    let file = open_lock_file(table_path)?;
    retry_flock(&file, false)?;
    Ok(LockGuard {
        path,
        owned: None,
        shared: Some(file),
    })
}

fn open_lock_file(table_path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path(table_path))
}

fn acquire_exclusive(table_path: &Path) -> io::Result<LockGuard> {
    let path = table_path.to_path_buf();
    // Process-local databases have no sidecar to coordinate on; the in-process
    // locks are sufficient and no lock file may be created for them.
    if crate::storage::is_memory_path(table_path) {
        return Ok(LockGuard {
            path,
            owned: None,
            shared: None,
        });
    }

    let entry = match EXCLUSIVE_LOCKS.get(table_path) {
        Some(existing) => Arc::clone(existing.value()),
        None => {
            let candidate = Arc::new(ExclusiveLock {
                file: open_lock_file(table_path)?,
                owner: Mutex::new(None),
                available: Condvar::new(),
            });
            Arc::clone(
                EXCLUSIVE_LOCKS
                    .entry(path.clone())
                    .or_insert(candidate)
                    .value(),
            )
        }
    };

    let current = std::thread::current().id();
    {
        let mut owner = entry
            .owner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if *owner == Some(current) {
            // Nested acquisition on the same thread: `flock` cannot be taken twice
            // from one descriptor, and the caller is already inside the lock.
            return Ok(LockGuard {
                path,
                owned: None,
                shared: None,
            });
        }
        while owner.is_some() {
            owner = entry
                .available
                .wait(owner)
                .unwrap_or_else(|error| error.into_inner());
        }
        *owner = Some(current);
    }

    if let Err(error) = retry_flock(&entry.file, true) {
        let mut owner = entry
            .owner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *owner = None;
        entry.available.notify_one();
        return Err(error);
    }

    Ok(LockGuard {
        path,
        owned: Some(entry),
        shared: None,
    })
}

fn retry_flock(file: &File, exclusive: bool) -> io::Result<()> {
    let start = Instant::now();
    let mut backoff = MIN_BACKOFF;
    loop {
        let result = if exclusive {
            file.try_lock_exclusive()
        } else {
            file.try_lock_shared()
                .map_err(|error| io::Error::new(io::ErrorKind::WouldBlock, error.to_string()))
        };
        match result {
            Ok(()) => return Ok(()),
            Err(_) if start.elapsed() < MAX_WAIT => {
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            Err(error) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "Database is locked (waited {}ms): {}",
                        start.elapsed().as_millis(),
                        error
                    ),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc as StdArc;

    #[test]
    fn lock_path_replaces_the_table_extension() {
        assert_eq!(
            lock_path(Path::new("/db/t.apex")),
            PathBuf::from("/db/t.apex.lock")
        );
        assert_eq!(
            lock_path(Path::new("/db/other")),
            PathBuf::from("/db/other.apex.lock")
        );
    }

    #[test]
    fn memory_paths_never_touch_the_filesystem() {
        let guard = lock_write(Path::new("apexbase_memory:lock-test")).unwrap();
        assert!(!guard.holds_lock());
    }

    #[test]
    fn nested_acquisition_is_a_no_op_instead_of_a_deadlock() {
        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("t.apex");
        let outer = lock_write(&table).unwrap();
        assert!(outer.holds_lock());
        // A second acquisition on the same thread must not block on its own lock.
        let inner = lock_write(&table).unwrap();
        assert!(!inner.holds_lock());
        drop(inner);
        assert!(outer.holds_lock());
        drop(outer);
        // After the outermost guard drops, the lock is free again.
        let again = lock_write(&table).unwrap();
        assert!(again.holds_lock());
    }

    #[test]
    fn release_drops_idle_descriptors_and_keeps_held_ones() {
        let dir = tempfile::tempdir().unwrap();
        let idle_table = dir.path().join("idle.apex");
        let held_table = dir.path().join("held.apex");
        let other_dir = tempfile::tempdir().unwrap();
        let other_table = other_dir.path().join("other.apex");

        // Populate the cache: one idle entry, one still held, one elsewhere.
        // Other tests share this process-global cache and run in parallel, so only
        // these three paths are asserted on, never the total entry count.
        drop(lock_write(&idle_table).unwrap());
        let held = lock_write(&held_table).unwrap();
        drop(lock_write(&other_table).unwrap());
        assert!(EXCLUSIVE_LOCKS.contains_key(&idle_table));

        release(dir.path());

        assert!(
            !EXCLUSIVE_LOCKS.contains_key(&idle_table),
            "an idle descriptor under the directory must be dropped"
        );
        assert!(
            EXCLUSIVE_LOCKS.contains_key(&held_table),
            "a held descriptor must stay cached or its flock would be released"
        );
        assert!(
            EXCLUSIVE_LOCKS.contains_key(&other_table),
            "a descriptor outside the directory must not be touched"
        );
        assert!(held.holds_lock());

        drop(held);
        release(dir.path());
        assert!(!EXCLUSIVE_LOCKS.contains_key(&held_table));
        release(other_dir.path());
        assert!(!EXCLUSIVE_LOCKS.contains_key(&other_table));
    }

    #[test]
    fn another_process_or_thread_waits_for_the_holder() {
        // Two *threads* with separate descriptors exercise the same exclusion a
        // second process would: flock belongs to the descriptor, so the second
        // acquisition only succeeds after the first is released.
        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("t.apex");
        let released = StdArc::new(AtomicBool::new(false));
        let flag = StdArc::clone(&released);
        let holder = {
            let table = table.clone();
            std::thread::spawn(move || {
                let guard = lock_write(&table).unwrap();
                std::thread::sleep(Duration::from_millis(120));
                flag.store(true, Ordering::SeqCst);
                drop(guard);
            })
        };
        std::thread::sleep(Duration::from_millis(20));
        let waiter = lock_write(&table).unwrap();
        assert!(
            released.load(Ordering::SeqCst),
            "the second acquisition must wait for the first to release"
        );
        drop(waiter);
        holder.join().unwrap();
    }

    #[test]
    fn shared_locks_wait_for_a_writer() {
        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("t.apex");
        let released = StdArc::new(AtomicBool::new(false));
        let flag = StdArc::clone(&released);
        let writer = {
            let table = table.clone();
            std::thread::spawn(move || {
                let guard = lock_write(&table).unwrap();
                std::thread::sleep(Duration::from_millis(120));
                flag.store(true, Ordering::SeqCst);
                drop(guard);
            })
        };
        std::thread::sleep(Duration::from_millis(20));
        // A shared lock is only granted once the writer released.
        let reader = lock_read(&table).unwrap();
        assert!(released.load(Ordering::SeqCst));
        drop(reader);
        writer.join().unwrap();
    }
}
