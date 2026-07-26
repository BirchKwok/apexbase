//! Per-table generations used to validate cross-layer caches.
//!
//! Storage owns the generation because it is the only layer that can commit a
//! logical table mutation. Readers keep the generation they observed and
//! discard cached state when it no longer matches.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use once_cell::sync::Lazy;

static TABLE_EPOCHS: Lazy<DashMap<PathBuf, AtomicU64>> = Lazy::new(DashMap::new);
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
    TABLE_EPOCHS
        .get(table_path)
        .map(|entry| entry.load(Ordering::Acquire))
        .unwrap_or(0)
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
        .or_insert_with(|| AtomicU64::new(0));
    let epoch = entry.fetch_add(1, Ordering::AcqRel) + 1;
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
}
