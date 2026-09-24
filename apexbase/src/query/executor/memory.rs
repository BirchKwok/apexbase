//! Per-query aggregation memory budget (architecture review A5, S1).
//!
//! Row-group scanning is already bounded by R3, but aggregation state
//! (high-cardinality GROUP BY maps, interned key lanes) grows with the
//! number of groups, and the parallel kernel multiplies that state by the
//! worker count while partials are alive. This module gives one query a
//! byte-measured budget shared by its worker threads:
//!
//! - The limit comes from `APEX_QUERY_MEMORY_MB` (`0` disables the budget,
//!   malformed/unset uses the default) and is read when a top-level query
//!   installs its budget, so it can be toggled between queries.
//! - A top-level query installs a budget through
//!   [`QueryMemoryBudgetGuard::ensure`]; nested queries share it, and the
//!   guard restores the previous context on every exit path (RAII), so a
//!   cancelled or failed query cannot leak a charge into the next query.
//! - Operators charge state growth and receive an explicit
//!   [`std::io::ErrorKind::OutOfMemory`] error when the budget is exceeded.
//!
//! A top-level query pays one `Arc` plus one thread-local install and
//! restore; with `APEX_QUERY_MEMORY_MB=0` the slot stays empty and the cost
//! is one env read, far below query cost.

use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Default budget when `APEX_QUERY_MEMORY_MB` is unset. Chosen to leave
/// ordinary analytical queries untouched while turning runaway
/// high-cardinality aggregation into a clear error instead of an OOM.
pub const DEFAULT_QUERY_MEMORY_BYTES: usize = 1024 * 1024 * 1024;

/// Bytes of one `key -> Vec<row index>` group entry (key, vector header and
/// map control overhead); element storage is charged separately.
pub(in crate::query::executor) const GROUP_INDEX_ENTRY_BYTES: usize =
    std::mem::size_of::<u64>() + std::mem::size_of::<Vec<usize>>() + 8;

/// Initial row-index capacity of a new group, matching the allocation the
/// row-index fallback makes per group.
pub(in crate::query::executor) const GROUP_INDEX_INITIAL_CAPACITY: usize = 16;

/// Bytes of one interned group-key map entry (`&str -> state index`); the
/// owned key and the aggregate vectors are charged separately.
pub(in crate::query::executor) const GROUP_STATE_ENTRY_BYTES: usize =
    std::mem::size_of::<&str>() + std::mem::size_of::<usize>() + 8;

/// Bytes of the per-candidate aggregate state in direct-indexed GROUP BY
/// kernels that pre-allocate one slot per possible group value (count, two
/// sums, and four optional min/max slots).
pub(in crate::query::executor) const DIRECT_INDEX_SLOT_BYTES: usize =
    std::mem::size_of::<i64>() * 3
        + std::mem::size_of::<Option<i64>>() * 2
        + std::mem::size_of::<Option<f64>>() * 2;

/// Rows between budget checks in per-row accumulation loops: one atomic
/// reservation per interval keeps the accounting off the per-row path while
/// bounding the overshoot to a few thousand entries.
pub(in crate::query::executor) const GROUP_BUDGET_CHECK_INTERVAL: usize = 4096;

/// Shared byte budget for one query. The counter is atomic so parallel
/// partial folds charge the same pool.
#[derive(Debug)]
pub struct QueryMemoryBudget {
    limit: usize,
    used: AtomicUsize,
}

impl QueryMemoryBudget {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Bytes currently charged by this query.
    #[cfg(test)]
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Charge `bytes`. Returns [`std::io::ErrorKind::OutOfMemory`] when the
    /// reservation would cross the limit; a rejected reservation leaves the
    /// counter unchanged.
    #[inline]
    pub fn reserve(&self, bytes: usize) -> std::io::Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let previous = self.used.fetch_add(bytes, Ordering::Relaxed);
        let total = previous.saturating_add(bytes);
        if total > self.limit {
            self.used.fetch_sub(bytes, Ordering::Relaxed);
            return Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                format!(
                    "query memory budget exceeded: aggregation state would use {} bytes \
                     ({} already charged, limit {}); increase APEX_QUERY_MEMORY_MB or \
                     reduce the group cardinality",
                    total, previous, self.limit
                ),
            ));
        }
        Ok(())
    }

    /// Release a previously charged amount (operator state dropped before the
    /// query ends, e.g. a kernel fallback).
    #[inline]
    pub fn release(&self, bytes: usize) {
        if bytes != 0 {
            self.used.fetch_sub(bytes, Ordering::Relaxed);
        }
    }
}

thread_local! {
    static QUERY_MEMORY_BUDGET: RefCell<Option<Arc<QueryMemoryBudget>>> =
        const { RefCell::new(None) };
}

/// The budget installed for the current thread's top-level query, if any.
pub fn query_memory_budget() -> Option<Arc<QueryMemoryBudget>> {
    QUERY_MEMORY_BUDGET.with(|budget| budget.borrow().clone())
}

/// Install `budget` for the current thread, returning a guard that restores
/// the previous context on drop. Unconditional; used by tests and by callers
/// that own the query boundary.
pub fn install_query_memory_budget(
    budget: Option<Arc<QueryMemoryBudget>>,
) -> QueryMemoryBudgetGuard {
    let previous = QUERY_MEMORY_BUDGET.with(|slot| {
        let mut slot = slot.borrow_mut();
        std::mem::replace(&mut *slot, budget)
    });
    QueryMemoryBudgetGuard {
        previous,
        installed: true,
    }
}

/// RAII scope for the per-query budget. Drop restores whatever was installed
/// before, including `None`, so worker threads and nested queries cannot leak
/// state across queries.
pub struct QueryMemoryBudgetGuard {
    previous: Option<Arc<QueryMemoryBudget>>,
    installed: bool,
}

impl QueryMemoryBudgetGuard {
    /// Install the process-configured budget for a top-level query. Nested
    /// queries keep the already-installed budget so a subquery cannot escape
    /// the outer query's bound.
    pub fn ensure() -> Self {
        if query_memory_budget().is_some() {
            return QueryMemoryBudgetGuard {
                previous: None,
                installed: false,
            };
        }
        let limit = configured_query_memory_limit();
        if limit == 0 {
            // `APEX_QUERY_MEMORY_MB=0` means unlimited: keep the slot empty so
            // an embeddable point query pays nothing at all.
            return QueryMemoryBudgetGuard {
                previous: None,
                installed: false,
            };
        }
        install_query_memory_budget(Some(Arc::new(QueryMemoryBudget::new(limit))))
    }
}

impl Drop for QueryMemoryBudgetGuard {
    fn drop(&mut self) {
        if self.installed {
            QUERY_MEMORY_BUDGET.with(|slot| *slot.borrow_mut() = self.previous.take());
        }
    }
}

/// Process-configured limit in bytes; `0` means unlimited. Read when a
/// top-level query installs its budget, so `APEX_QUERY_MEMORY_MB` can be
/// toggled between queries (same contract as `APEX_BATCH_SCAN`).
pub fn configured_query_memory_limit() -> usize {
    match std::env::var_os("APEX_QUERY_MEMORY_MB") {
        None => DEFAULT_QUERY_MEMORY_BYTES,
        Some(value) => match value
            .to_str()
            .and_then(|text| text.trim().parse::<u64>().ok())
        {
            Some(0) => 0,
            Some(megabytes) => (megabytes as usize).saturating_mul(1024 * 1024),
            None => DEFAULT_QUERY_MEMORY_BYTES,
        },
    }
}
