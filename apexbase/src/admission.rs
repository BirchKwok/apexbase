//! Process-level memory admission and bounded query queueing (V5 S10 / W3-2).
//!
//! The per-query budget (`APEX_QUERY_MEMORY_MB`, see
//! [`crate::query::executor::memory`]) bounds one statement. This module adds a
//! process-wide ceiling on how many statements may reserve process memory at the
//! same time, and a **bounded** queue for the ones that do not fit yet:
//!
//! - `APEX_PROCESS_BUDGET_MB`: process-wide admission budget in MiB. Unset,
//!   malformed or `0` disables admission/queueing (the default: embedded point
//!   queries pay nothing). The per-query budget is still enforced.
//! - `APEX_QUERY_ADMISSION_MB` (default `16`): reservation for ordinary
//!   point/short/scan statements. `0` makes every statement reserve the whole
//!   process budget.
//! - `APEX_QUERY_QUEUE_MAX` (default `64`): maximum statements waiting for a
//!   slot. One more waiter is rejected immediately.
//! - `APEX_QUERY_QUEUE_WAIT_MS` (default `5000`): per-statement wait limit.
//!   Exceeding it is rejected explicitly; `0` fails fast.
//!
//! Heavy statements (aggregation / ORDER BY / JOIN / DISTINCT / UNION / CTE /
//! window) reserve `min(APEX_QUERY_MEMORY_MB, process budget)` so they serialize
//! according to their per-query cap. The per-query budget remains the hard bound
//! on one statement's operator state; the process budget is the additional
//! admission ceiling (set `APEX_QUERY_MEMORY_MB <= APEX_PROCESS_BUDGET_MB` for a
//! strict sum bound).
//!
//! Rejections carry an explicit code in the message:
//! `[APEX_ADMISSION_QUEUE_FULL]` (queue capacity reached) and
//! `[APEX_ADMISSION_TIMEOUT]` (slot did not free within the wait budget). No
//! path silently drops a statement, and the queue is bounded by construction.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Default reservation for ordinary statements (16 MiB).
pub const DEFAULT_LIGHT_ADMISSION_BYTES: usize = 16 * 1024 * 1024;
/// Default bounded-queue capacity (waiting statements).
pub const DEFAULT_QUEUE_MAX: usize = 64;
/// Default wait before an explicit timeout rejection (5 s).
pub const DEFAULT_QUEUE_WAIT_MS: u64 = 5_000;

/// Admission configuration for one statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionConfig {
    /// Process-wide reservation budget in bytes; `0` disables admission.
    pub budget_bytes: usize,
    /// Reservation for non-heavy statements.
    pub light_bytes: usize,
    /// Maximum number of statements waiting for a slot.
    pub queue_max: usize,
    /// Maximum wait for a slot before an explicit rejection.
    pub wait: Duration,
}

impl AdmissionConfig {
    /// Disabled configuration (default when `APEX_PROCESS_BUDGET_MB` is unset).
    pub const DISABLED: Self = Self {
        budget_bytes: 0,
        light_bytes: DEFAULT_LIGHT_ADMISSION_BYTES,
        queue_max: DEFAULT_QUEUE_MAX,
        wait: Duration::from_millis(DEFAULT_QUEUE_WAIT_MS),
    };

    /// Read the configuration for the statement about to run.
    ///
    /// Only `APEX_PROCESS_BUDGET_MB` is read when admission is disabled, so the
    /// default path pays one environment lookup per top-level statement. The
    /// other variables are parsed when the budget is enabled, so they can be
    /// toggled between statements like `APEX_QUERY_MEMORY_MB`.
    pub fn from_env() -> Self {
        let budget_mb = match std::env::var_os("APEX_PROCESS_BUDGET_MB") {
            None => return Self::DISABLED,
            Some(value) => match value.to_str().and_then(|text| text.trim().parse::<u64>().ok()) {
                None => return Self::DISABLED,
                Some(0) => return Self::DISABLED,
                Some(megabytes) => megabytes as usize,
            },
        };
        let budget_bytes = budget_mb.saturating_mul(1024 * 1024);
        let light_bytes = match std::env::var_os("APEX_QUERY_ADMISSION_MB")
            .and_then(|value| value.to_str().and_then(|text| text.trim().parse::<usize>().ok()))
        {
            Some(megabytes) => megabytes.saturating_mul(1024 * 1024),
            None => DEFAULT_LIGHT_ADMISSION_BYTES,
        };
        let light_bytes = if light_bytes == 0 {
            budget_bytes
        } else {
            light_bytes.min(budget_bytes)
        };
        let queue_max = std::env::var_os("APEX_QUERY_QUEUE_MAX")
            .and_then(|value| value.to_str().and_then(|text| text.trim().parse::<usize>().ok()))
            .unwrap_or(DEFAULT_QUEUE_MAX);
        let wait_ms = std::env::var_os("APEX_QUERY_QUEUE_WAIT_MS")
            .and_then(|value| value.to_str().and_then(|text| text.trim().parse::<u64>().ok()))
            .unwrap_or(DEFAULT_QUEUE_WAIT_MS);
        Self {
            budget_bytes,
            light_bytes,
            queue_max,
            wait: Duration::from_millis(wait_ms),
        }
    }

    /// Reservation this statement needs, given its SQL text.
    ///
    /// Classification is a cheap keyword scan (no second parse): aggregation,
    /// sorting, joins, distinct, set operations, CTEs, window functions and
    /// large COPY import forms are "heavy" and reserve the per-query cap.
    pub fn reservation_for(&self, sql: &str) -> usize {
        if self.budget_bytes == 0 {
            return 0;
        }
        if is_heavy_statement(sql) {
            let per_query = per_query_budget_bytes();
            let reservation = if per_query == 0 {
                self.budget_bytes
            } else {
                per_query.min(self.budget_bytes)
            };
            reservation.max(1)
        } else {
            self.light_bytes.min(self.budget_bytes).max(1)
        }
    }
}

/// Per-query budget in bytes (`APEX_QUERY_MEMORY_MB`), `0` = unlimited.
fn per_query_budget_bytes() -> usize {
    crate::query::executor::memory::configured_query_memory_limit()
}

/// Cheap statement-shape classification used for admission reservations.
pub fn is_heavy_statement(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let head = trimmed.get(..16).unwrap_or(trimmed).to_ascii_uppercase();
    // A CTE can hide any heavy operator; treat the whole statement as heavy.
    if head.starts_with("WITH") {
        return true;
    }
    if !head.starts_with("SELECT") {
        return false;
    }
    let upper = sql.to_ascii_uppercase();
    const HEAVY: [&str; 10] = [
        " GROUP BY ",
        " ORDER BY ",
        " JOIN ",
        " DISTINCT ",
        " UNION ",
        " INTERSECT ",
        " EXCEPT ",
        " HAVING ",
        " OVER (",
        " OVER(",
    ];
    HEAVY.iter().any(|needle| upper.contains(needle))
}

#[derive(Debug, Default)]
struct QueueState {
    used_bytes: usize,
    waiting: usize,
    admitted: u64,
    rejected: u64,
    timed_out: u64,
}

/// Process-wide admission controller: bounded reservations plus a bounded queue.
pub struct AdmissionController {
    state: Mutex<QueueState>,
    slot_freed: Condvar,
}

impl Default for AdmissionController {
    fn default() -> Self {
        Self::new()
    }
}

impl AdmissionController {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(QueueState::default()),
            slot_freed: Condvar::new(),
        }
    }

    /// Try to reserve `bytes`; wait up to `config.wait` for a slot while fewer
    /// than `config.queue_max` statements are already waiting.
    pub fn acquire(
        &self,
        config: &AdmissionConfig,
        bytes: usize,
    ) -> io::Result<QueryAdmission<'_>> {
        if config.budget_bytes == 0 || bytes == 0 {
            return Ok(QueryAdmission {
                controller: self,
                reserved: 0,
            });
        }
        let started = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let deadline = started.checked_add(config.wait);
        loop {
            if state.used_bytes.saturating_add(bytes) <= config.budget_bytes {
                state.used_bytes += bytes;
                state.admitted += 1;
                return Ok(QueryAdmission {
                    controller: self,
                    reserved: bytes,
                });
            }
            if state.waiting >= config.queue_max {
                state.rejected += 1;
                return Err(admission_error(
                    "QUEUE_FULL",
                    format!(
                        "process memory admission queue is full ({} statements waiting, budget {} MiB); \
                         increase APEX_PROCESS_BUDGET_MB / APEX_QUERY_QUEUE_MAX or retry",
                        state.waiting,
                        config.budget_bytes / (1024 * 1024)
                    ),
                ));
            }
            let Some(deadline) = deadline else {
                state.timed_out += 1;
                return Err(admission_error(
                    "TIMEOUT",
                    "process memory admission timed out (wait budget 0 ms)".to_string(),
                ));
            };
            let now = Instant::now();
            if now >= deadline {
                state.timed_out += 1;
                return Err(admission_error(
                    "TIMEOUT",
                    format!(
                        "process memory admission timed out after {} ms ({} MiB in use of {} MiB); \
                         increase APEX_PROCESS_BUDGET_MB or APEX_QUERY_QUEUE_WAIT_MS",
                        config.wait.as_millis(),
                        state.used_bytes / (1024 * 1024),
                        config.budget_bytes / (1024 * 1024)
                    ),
                ));
            }
            state.waiting += 1;
            let (guard, timeout) = self
                .slot_freed
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = guard;
            state.waiting -= 1;
            if timeout.timed_out() && Instant::now() >= deadline {
                state.timed_out += 1;
                return Err(admission_error(
                    "TIMEOUT",
                    format!(
                        "process memory admission timed out after {} ms ({} MiB in use of {} MiB); \
                         increase APEX_PROCESS_BUDGET_MB or APEX_QUERY_QUEUE_WAIT_MS",
                        config.wait.as_millis(),
                        state.used_bytes / (1024 * 1024),
                        config.budget_bytes / (1024 * 1024)
                    ),
                ));
            }
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.used_bytes = state.used_bytes.saturating_sub(bytes);
        drop(state);
        self.slot_freed.notify_all();
    }

    /// Current observability counters.
    pub fn stats(&self) -> AdmissionStats {
        let state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        AdmissionStats {
            used_bytes: state.used_bytes,
            waiting: state.waiting,
            admitted: state.admitted,
            rejected: state.rejected,
            timed_out: state.timed_out,
        }
    }

    /// Test hook: reset counters between cases that share the process controller.
    #[cfg(test)]
    fn reset(&self) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = QueueState::default();
    }
}

impl QueryAdmission<'_> {
    /// Bytes reserved by this admission (0 when disabled).
    pub fn reserved(&self) -> usize {
        self.reserved
    }
}

/// RAII reservation released when the statement finishes (success, error or
/// cancellation), so a panic cannot leak process budget.
pub struct QueryAdmission<'a> {
    controller: &'a AdmissionController,
    reserved: usize,
}

impl Drop for QueryAdmission<'_> {
    fn drop(&mut self) {
        self.controller.release(self.reserved);
    }
}

/// Snapshot of admission counters for observability/tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdmissionStats {
    pub used_bytes: usize,
    pub waiting: usize,
    pub admitted: u64,
    pub rejected: u64,
    pub timed_out: u64,
}

impl AdmissionStats {
    pub fn to_tuple(self) -> (u64, u64, u64, u64, u64) {
        (
            self.used_bytes as u64,
            self.waiting as u64,
            self.admitted,
            self.rejected,
            self.timed_out,
        )
    }
}

fn admission_error(code: &str, detail: String) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        format!("[APEX_ADMISSION_{code}] {detail}"),
    )
}

static PROCESS_CONTROLLER: once_cell::sync::Lazy<AdmissionController> =
    once_cell::sync::Lazy::new(AdmissionController::new);

/// Admit one top-level statement using the process controller and environment
/// configuration. The returned guard releases the reservation on drop.
pub fn admit_statement(sql: &str) -> io::Result<QueryAdmission<'static>> {
    let config = AdmissionConfig::from_env();
    if config.budget_bytes == 0 {
        return Ok(QueryAdmission {
            controller: &PROCESS_CONTROLLER,
            reserved: 0,
        });
    }
    let reservation = config.reservation_for(sql);
    PROCESS_CONTROLLER.acquire(&config, reservation)
}

/// Admit a statement batch that may contain any operator shape (multi-statement
/// execution): reserves the heavy-statement amount.
pub fn admit_heavy_statement() -> io::Result<QueryAdmission<'static>> {
    let config = AdmissionConfig::from_env();
    if config.budget_bytes == 0 {
        return Ok(QueryAdmission {
            controller: &PROCESS_CONTROLLER,
            reserved: 0,
        });
    }
    let per_query = per_query_budget_bytes();
    let reservation = if per_query == 0 {
        config.budget_bytes
    } else {
        per_query.min(config.budget_bytes)
    }
    .max(1);
    PROCESS_CONTROLLER.acquire(&config, reservation)
}

/// Process admission counters plus the currently configured budget in bytes.
pub fn process_stats() -> AdmissionStats {
    PROCESS_CONTROLLER.stats()
}

/// Configured process budget in bytes (`0` = admission disabled).
pub fn configured_process_budget_bytes() -> usize {
    AdmissionConfig::from_env().budget_bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(budget: usize, light: usize, queue_max: usize, wait_ms: u64) -> AdmissionConfig {
        AdmissionConfig {
            budget_bytes: budget,
            light_bytes: light,
            queue_max,
            wait: Duration::from_millis(wait_ms),
        }
    }

    #[test]
    fn disabled_config_never_blocks_or_charges() {
        let controller = AdmissionController::new();
        let guard = controller.acquire(&AdmissionConfig::DISABLED, 0).unwrap();
        assert_eq!(guard.reserved(), 0);
        assert_eq!(controller.stats(), AdmissionStats::default());
    }

    #[test]
    fn heavy_statement_classification() {
        assert!(is_heavy_statement("SELECT a, COUNT(*) FROM t GROUP BY a"));
        assert!(is_heavy_statement("select * from a join b on a.k = b.k"));
        assert!(is_heavy_statement("SELECT * FROM t ORDER BY score DESC LIMIT 10"));
        assert!(is_heavy_statement("SELECT DISTINCT city FROM t"));
        assert!(is_heavy_statement("WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(!is_heavy_statement("SELECT * FROM t WHERE _id = 5"));
        assert!(!is_heavy_statement("SELECT COUNT(*) FROM t"));
        assert!(!is_heavy_statement("INSERT INTO t VALUES (1)"));
    }

    #[test]
    fn reservation_uses_light_and_process_caps() {
        let cfg = config(64 << 20, 8 << 20, 4, 10);
        assert_eq!(cfg.reservation_for("SELECT * FROM t WHERE _id = 1"), 8 << 20);
        let heavy = cfg.reservation_for("SELECT a, COUNT(*) FROM t GROUP BY a");
        assert_eq!(heavy, (per_query_budget_bytes()).min(64 << 20).max(1));
    }

    #[test]
    fn queue_full_is_rejected_with_explicit_code() {
        let controller = std::sync::Arc::new(AdmissionController::new());
        let cfg = config(16, 16, 1, 50);
        let held = controller.acquire(&cfg, 16).unwrap();
        // Occupy the single waiting slot from another thread.
        let waiter = {
            let controller = std::sync::Arc::clone(&controller);
            std::thread::spawn(move || controller.acquire(&cfg, 16).map(|_| ()))
        };
        while controller.stats().waiting == 0 {
            std::thread::yield_now();
        }
        let error = controller.acquire(&cfg, 16).err().expect("queue must reject");
        assert!(error.to_string().contains("APEX_ADMISSION_QUEUE_FULL"));
        drop(held);
        // The queued statement eventually acquires its slot.
        let result = waiter.join().unwrap();
        assert!(result.is_ok());
        assert_eq!(controller.stats().used_bytes, 0);
    }

    #[test]
    fn timeout_is_rejected_with_explicit_code() {
        let controller = AdmissionController::new();
        let cfg = config(16, 16, 8, 5);
        let _held = controller.acquire(&cfg, 16).unwrap();
        let error = controller.acquire(&cfg, 16).err().expect("must time out");
        assert!(error.to_string().contains("APEX_ADMISSION_TIMEOUT"));
        assert_eq!(controller.stats().timed_out, 1);
    }

    #[test]
    fn release_wakes_queued_statements_without_leaking_budget() {
        let controller = AdmissionController::new();
        let cfg = config(16, 16, 8, 500);
        let controller = std::sync::Arc::new(AdmissionController::new());
        let held = controller.acquire(&cfg, 16).unwrap();
        let waiter = {
            let controller = std::sync::Arc::clone(&controller);
            std::thread::spawn(move || {
                let started = Instant::now();
                let guard = controller.acquire(&cfg, 16).unwrap();
                let waited = started.elapsed();
                drop(guard);
                waited
            })
        };
        while controller.stats().waiting == 0 {
            std::thread::yield_now();
        }
        std::thread::sleep(Duration::from_millis(10));
        drop(held);
        let waited = waiter.join().unwrap();
        assert!(waited >= Duration::from_millis(5), "waiter was not queued");
        // All reservations were released.
        let stats = controller.stats();
        assert_eq!(stats.used_bytes, 0);
        assert_eq!(stats.admitted, 2);
    }

    #[test]
    fn concurrent_admissions_stay_within_budget_and_never_deadlock() {
        let controller = std::sync::Arc::new(AdmissionController::new());
        controller.reset();
        let cfg = config(64, 8, 64, 1_000);
        let threads = 8;
        let per_thread = 32;
        let mut handles = Vec::new();
        for _ in 0..threads {
            let controller = std::sync::Arc::clone(&controller);
            let cfg = cfg;
            handles.push(std::thread::spawn(move || {
                let mut max_used = 0usize;
                for _ in 0..per_thread {
                    let guard = controller.acquire(&cfg, 8).unwrap();
                    max_used = max_used.max(controller.stats().used_bytes);
                    drop(guard);
                }
                max_used
            }));
        }
        let max_used = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .max()
            .unwrap();
        assert!(max_used <= 64, "budget exceeded: {max_used}");
        let stats = controller.stats();
        assert_eq!(stats.used_bytes, 0);
        assert_eq!(stats.admitted, (threads * per_thread) as u64);
    }
}
