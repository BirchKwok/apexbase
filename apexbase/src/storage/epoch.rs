//! Per-table generations used to validate cross-layer caches.
//!
//! Storage owns the generation because it is the only layer that can commit a
//! logical table mutation. Readers keep the generation they observed and
//! discard cached state when it no longer matches.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use memmap2::{MmapMut, MmapOptions};
use once_cell::sync::Lazy;

const SHARED_EPOCH_BYTES: u64 = std::mem::size_of::<AtomicU64>() as u64;

struct SharedEpoch {
    mapping: MmapMut,
}

impl SharedEpoch {
    fn open(table_path: &Path) -> io::Result<Self> {
        let path = table_path.with_extension("apex.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        if file.metadata()?.len() < SHARED_EPOCH_BYTES {
            file.set_len(SHARED_EPOCH_BYTES)?;
        }
        let mapping = unsafe {
            MmapOptions::new()
                .len(SHARED_EPOCH_BYTES as usize)
                .map_mut(&file)?
        };
        Ok(Self { mapping })
    }

    #[inline]
    fn atomic(&self) -> &AtomicU64 {
        debug_assert_eq!(
            self.mapping
                .as_ptr()
                .align_offset(std::mem::align_of::<AtomicU64>()),
            0
        );
        // SAFETY: file mappings are page-aligned and remain alive for the
        // lifetime of `self`; all accesses to these bytes use AtomicU64.
        unsafe { &*self.mapping.as_ptr().cast::<AtomicU64>() }
    }

    #[inline]
    fn current(&self) -> u64 {
        self.atomic().load(Ordering::Acquire)
    }

    #[inline]
    fn bump(&self) -> u64 {
        self.atomic().fetch_add(1, Ordering::AcqRel) + 1
    }
}

struct TableEpoch {
    local: AtomicU64,
    shared: Option<SharedEpoch>,
}

impl TableEpoch {
    fn open(table_path: &Path) -> Self {
        let shared = SharedEpoch::open(table_path).ok();
        let initial = shared.as_ref().map_or(0, SharedEpoch::current);
        Self {
            local: AtomicU64::new(initial),
            shared,
        }
    }

    #[inline]
    fn current(&self) -> u64 {
        self.shared
            .as_ref()
            .map_or_else(|| self.local.load(Ordering::Acquire), SharedEpoch::current)
    }

    #[inline]
    fn bump(&self) -> u64 {
        let epoch = self.shared.as_ref().map_or_else(
            || self.local.fetch_add(1, Ordering::AcqRel) + 1,
            SharedEpoch::bump,
        );
        self.local.store(epoch, Ordering::Release);
        epoch
    }
}

static TABLE_EPOCHS: Lazy<DashMap<PathBuf, TableEpoch>> = Lazy::new(DashMap::new);
static GLOBAL_EPOCH: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct WriteScopeState {
    depth: usize,
    changed: bool,
}

thread_local! {
    static WRITE_SCOPES: RefCell<HashMap<PathBuf, WriteScopeState>> =
        RefCell::new(HashMap::new());
}

/// Coalesces nested physical mutations into one published table epoch.
pub struct LogicalWrite {
    table_path: PathBuf,
}

impl LogicalWrite {
    /// Mark the logical write successful. The outermost scope publishes one
    /// epoch when it exits, regardless of how many nested storage operations
    /// also reported changes.
    #[inline]
    pub fn commit(&self) {
        WRITE_SCOPES.with(|scopes| {
            if let Some(state) = scopes.borrow_mut().get_mut(&self.table_path) {
                state.changed = true;
            }
        });
    }
}

impl Drop for LogicalWrite {
    fn drop(&mut self) {
        let publish = WRITE_SCOPES.with(|scopes| {
            let mut scopes = scopes.borrow_mut();
            let should_remove = match scopes.get_mut(&self.table_path) {
                Some(state) => {
                    state.depth -= 1;
                    state.depth == 0
                }
                None => return false,
            };
            if should_remove {
                scopes
                    .remove(&self.table_path)
                    .map(|state| state.changed)
                    .unwrap_or(false)
            } else {
                false
            }
        });
        if publish {
            bump_now(&self.table_path);
        }
    }
}

#[inline]
pub fn logical_write(table_path: &Path) -> LogicalWrite {
    let table_path = table_path.to_path_buf();
    WRITE_SCOPES.with(|scopes| {
        let mut scopes = scopes.borrow_mut();
        scopes.entry(table_path.clone()).or_default().depth += 1;
    });
    LogicalWrite { table_path }
}

#[inline]
pub fn current(table_path: &Path) -> u64 {
    if let Some(entry) = TABLE_EPOCHS.get(table_path) {
        return entry.current();
    }
    TABLE_EPOCHS
        .entry(table_path.to_path_buf())
        .or_insert_with(|| TableEpoch::open(table_path))
        .current()
}

#[inline]
pub fn global_current() -> u64 {
    GLOBAL_EPOCH.load(Ordering::Acquire)
}

/// Advance the epoch once after a logical write has committed.
#[inline]
pub fn bump(table_path: &Path) -> u64 {
    let deferred = WRITE_SCOPES.with(|scopes| {
        let mut scopes = scopes.borrow_mut();
        if let Some(state) = scopes.get_mut(table_path) {
            state.changed = true;
            true
        } else {
            false
        }
    });
    if deferred {
        current(table_path).saturating_add(1)
    } else {
        bump_now(table_path)
    }
}

#[inline]
fn bump_now(table_path: &Path) -> u64 {
    let entry = TABLE_EPOCHS
        .entry(table_path.to_path_buf())
        .or_insert_with(|| TableEpoch::open(table_path));
    let epoch = entry.bump();
    GLOBAL_EPOCH.fetch_add(1, Ordering::AcqRel);
    epoch
}

/// Drop obsolete bookkeeping for tables removed as part of DDL.
#[inline]
pub fn remove(table_path: &Path) {
    TABLE_EPOCHS.remove(table_path);
}

#[inline]
pub fn remove_dir(dir: &Path) {
    TABLE_EPOCHS.retain(|path, _| !path.starts_with(dir));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_logical_write_publishes_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epoch.apex");
        let before = current(&path);

        {
            let outer = logical_write(&path);
            bump(&path);
            bump(&path);
            {
                let inner = logical_write(&path);
                bump(&path);
                inner.commit();
            }
            assert_eq!(current(&path), before);
            outer.commit();
        }

        assert_eq!(current(&path), before + 1);
    }

    #[test]
    fn uncommitted_logical_write_does_not_publish() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no_change.apex");
        let before = current(&path);
        drop(logical_write(&path));
        assert_eq!(current(&path), before);
    }

    #[test]
    fn shared_epoch_mappings_observe_the_same_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.apex");
        let first = SharedEpoch::open(&path).unwrap();
        let second = SharedEpoch::open(&path).unwrap();
        let before = second.current();

        assert_eq!(first.bump(), before + 1);
        assert_eq!(second.current(), before + 1);
        assert_eq!(
            std::fs::metadata(path.with_extension("apex.lock"))
                .unwrap()
                .len(),
            8
        );
    }
}
