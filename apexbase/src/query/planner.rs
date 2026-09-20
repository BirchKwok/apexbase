//! Query Planner - Routes queries to OLTP or OLAP execution paths
//!
//! Analyzes SQL queries and selects the optimal execution strategy:
//! - **OLTP path**: Index-based point lookups, single-row mutations
//! - **OLAP path**: Vectorized columnar scans with SIMD/JIT
//!
//! Architecture:
//! ```text
//! ┌─────────────────┐
//! │   SQL Query      │
//! └────────┬────────┘
//!          │
//! ┌────────▼────────┐
//! │  QueryPlanner    │
//! │  - Analyze AST   │
//! │  - Check indexes │
//! │  - Estimate cost │
//! └────────┬────────┘
//!          │
//!   ┌──────┴──────────┐
//!   │                 │
//!   ▼                 ▼
//! ┌──────────┐  ┌──────────────┐
//! │ OLTP     │  │  OLAP        │
//! │ Executor │  │  Executor    │
//! │ (index)  │  │  (vectorized)│
//! └──────────┘  └──────────────┘
//! ```

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::data::Value;
use crate::query::sql_parser::BinaryOperator;
use crate::query::{SelectColumn, SelectStatement, SqlExpr, SqlStatement};
use crate::storage::index::IndexManager;
use crate::storage::index::index_manager::PredicateHint;

// ============================================================================
// Table Statistics Cache (for CBO)
// ============================================================================

/// Per-column statistics collected by ANALYZE
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Number of distinct values
    pub ndv: u64,
    /// Number of null values
    pub null_count: u64,
    /// Min value (as string for universal comparison)
    pub min_value: String,
    /// Max value (as string for universal comparison)
    pub max_value: String,
    /// Typed numeric bounds used by range selectivity estimation.
    #[serde(default)]
    pub numeric_min: Option<f64>,
    #[serde(default)]
    pub numeric_max: Option<f64>,
    /// Optional equi-width histogram for numeric predicates.
    #[serde(default)]
    pub histogram: Vec<HistogramBucket>,
    /// Most common values and their estimated frequencies. Values use the
    /// same canonical string representation as ANALYZE min/max.
    #[serde(default)]
    pub most_common_values: Vec<(String, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub lower: f64,
    pub upper: f64,
    pub row_count: u64,
}

/// Per-table statistics collected by ANALYZE
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    /// Serialized statistics schema version.
    #[serde(default)]
    pub schema_version: u32,
    /// Table schema generation observed by ANALYZE.
    #[serde(default)]
    pub schema_generation: u64,
    /// Table data generation observed by ANALYZE.
    #[serde(default)]
    pub data_generation: u64,
    /// Total row count
    pub row_count: u64,
    /// Per-column statistics: column_name → stats
    pub columns: HashMap<String, ColumnStats>,
    /// Timestamp when stats were collected (epoch millis)
    pub collected_at: u64,
    /// Source table size when the statistics were collected.
    #[serde(default)]
    pub source_size: u64,
}

/// Global stats cache: table_path → entry. Bounded (S2): a long-lived process
/// that touches many tables must not grow this without limit. Eviction is
/// FIFO by insertion, and because a read only clones the entry it adds no
/// write-lock traffic to the planning hot path.
const STATS_CACHE_CAP: usize = 1024;
static STATS_CACHE: Lazy<RwLock<HashMap<String, StatsCacheEntry>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));
static STATS_CACHE_CLOCK: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

struct StatsCacheEntry {
    stats: TableStats,
    observed_epoch: u64,
    inserted_at: u64,
}

/// Insert with a FIFO cap: when full, the oldest inserted table is dropped and
/// its stats are re-read from the sidecar on the next planning access.
fn stats_cache_insert(
    cache: &mut HashMap<String, StatsCacheEntry>,
    table_key: &str,
    stats: TableStats,
    observed_epoch: u64,
) {
    if !cache.contains_key(table_key) && cache.len() >= STATS_CACHE_CAP {
        if let Some(victim) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.inserted_at)
            .map(|(key, _)| key.clone())
        {
            cache.remove(&victim);
        }
    }
    let inserted_at = STATS_CACHE_CLOCK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    cache.insert(
        table_key.to_string(),
        StatsCacheEntry {
            stats,
            observed_epoch,
            inserted_at,
        },
    );
}

const STATS_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PlanFeedback {
    pub(crate) strategy: ExecutionStrategy,
    pub(crate) estimated_rows: f64,
    pub(crate) actual_rows: f64,
    pub(crate) samples: u64,
    /// Running model-cost / measured-time averages per cost class, recorded
    /// for the class that actually executed (architecture review R5.3).
    pub(crate) scan_cost_avg: f64,
    pub(crate) scan_time_avg_us: f64,
    pub(crate) scan_samples: u64,
    pub(crate) index_cost_avg: f64,
    pub(crate) index_time_avg_us: f64,
    pub(crate) index_samples: u64,
    /// Parallel batch scan class (architecture review R5.12): the
    /// auto-enabled fused parallel scan records here so its measured time
    /// never contaminates the serial prediction the auto-enable decision
    /// compares against.
    pub(crate) parallel_cost_avg: f64,
    pub(crate) parallel_time_avg_us: f64,
    pub(crate) parallel_samples: u64,
}

/// Process-global plan feedback, keyed by table key and then by shape hash
/// (architecture review R5.8): the per-table prefix keeps cross-session
/// persistence in one per-table sidecar file.
static PLAN_FEEDBACK: Lazy<RwLock<HashMap<String, HashMap<u64, PlanFeedback>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Tables whose feedback sidecar this process already attempted to load
/// (at most once per table; a file that another process updates later
/// becomes visible at the next process start).
static FEEDBACK_LOADED: Lazy<RwLock<HashSet<String>>> =
    Lazy::new(|| RwLock::new(HashSet::new()));

/// Serializes the feedback persistence read-modify-write (memory update +
/// sidecar write) across tables. The planning read path never takes it.
static FEEDBACK_PERSIST_LOCK: Lazy<std::sync::Mutex<()>> =
    Lazy::new(|| std::sync::Mutex::new(()));

// R5.12 added the parallel cost class to PlanFeedback: sidecars written
// before the bump no longer match the layout and count as "no persisted
// feedback" (their shape re-calibrates on the next EXPLAIN ANALYZE).
// The environment fingerprint bump (Q2) follows the same rule: v2 files have
// no fingerprint field and are ignored.
const FEEDBACK_SCHEMA_VERSION: u32 = 3;

/// Environment identity for persisted plan feedback.
///
/// Time calibration is machine-specific: a sidecar carried to another host,
/// container or CPU count would otherwise feed its microsecond averages into
/// the auto-parallel decision, and the planner would trust a threshold that
/// never held there. OS/arch/parallelism cover the environments this project
/// runs in; a mismatch counts as "no persisted feedback" and the shape
/// re-calibrates on the next EXPLAIN ANALYZE.
static FEEDBACK_ENVIRONMENT: Lazy<String> = Lazy::new(|| {
    let parallelism = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(0);
    format!(
        "{}/{}/{parallelism}",
        std::env::consts::OS,
        std::env::consts::ARCH
    )
});

/// Bounds for process-global plan feedback (S2). Only EXPLAIN ANALYZE records
/// feedback, so growth needs explicit user action; the caps keep a long-lived
/// process (or an automated calibration sweep over many shapes) bounded.
/// Eviction drops the least-observed entry, which recalibrates on its next
/// EXPLAIN ANALYZE.
const PLAN_FEEDBACK_SHAPES_PER_TABLE: usize = 256;
const PLAN_FEEDBACK_TABLES: usize = 256;
/// Loaded-marker bound; dropping a marker only allows a later sidecar reload
/// (in-process entries always win on merge).
const FEEDBACK_LOADED_CAP: usize = PLAN_FEEDBACK_TABLES * 4;

fn feedback_samples(inner: &HashMap<u64, PlanFeedback>) -> u64 {
    inner.values().map(|entry| entry.samples).sum()
}

/// Evict the least-observed shape when a new shape would exceed the per-table
/// cap.
fn feedback_evict_shape_if_full(inner: &mut HashMap<u64, PlanFeedback>, incoming: u64) {
    if !inner.contains_key(&incoming) && inner.len() >= PLAN_FEEDBACK_SHAPES_PER_TABLE {
        if let Some(victim) = inner
            .iter()
            .min_by_key(|(_, entry)| entry.samples)
            .map(|(shape, _)| *shape)
        {
            inner.remove(&victim);
        }
    }
}

/// Borrow one table's feedback map, dropping the least-observed table when a
/// new table would exceed the table cap. The dropped table's loaded marker is
/// intentionally left alone: feedback is advisory and a later EXPLAIN ANALYZE
/// re-records it in-process.
fn feedback_table_mut<'a>(
    cache: &'a mut HashMap<String, HashMap<u64, PlanFeedback>>,
    table_key: &str,
) -> &'a mut HashMap<u64, PlanFeedback> {
    if !cache.contains_key(table_key) && cache.len() >= PLAN_FEEDBACK_TABLES {
        if let Some(victim) = cache
            .iter()
            .min_by_key(|(_, inner)| feedback_samples(inner))
            .map(|(key, _)| key.clone())
        {
            cache.remove(&victim);
        }
    }
    cache.entry(table_key.to_string()).or_default()
}

/// Bound the loaded-marker set; eviction allows a later sidecar reload.
fn feedback_mark_loaded(table_key: &str) {
    let mut loaded = FEEDBACK_LOADED.write();
    if !loaded.contains(table_key) && loaded.len() >= FEEDBACK_LOADED_CAP {
        if let Some(victim) = loaded.iter().next().cloned() {
            loaded.remove(&victim);
        }
    }
    loaded.insert(table_key.to_string());
}

/// On-disk form of one table's plan feedback entries.
#[derive(Debug, Serialize, Deserialize)]
struct PersistedPlanFeedback {
    version: u32,
    /// Environment the averages were measured in (`FEEDBACK_ENVIRONMENT`).
    fingerprint: String,
    entries: Vec<(u64, PlanFeedback)>,
}

/// Per-table feedback sidecar, colocated with the table file (same
/// placement as the `.cbo_stats` sidecar; reaped with the table via
/// `TABLE_FILE_SUFFIXES`).
fn feedback_sidecar_path(table_key: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.plan_feedback", table_key))
}

/// Lazily load a table's feedback sidecar into the process-global map, at
/// most once per (process, table): the planning read path must not do
/// per-query IO. Missing or unreadable files, version mismatches and files
/// written in another environment all count as "no persisted feedback".
/// Entries already recorded by this process win over file entries.
fn ensure_feedback_loaded(table_key: &str) {
    if FEEDBACK_LOADED.read().contains(table_key) {
        return;
    }
    let data = match std::fs::read(feedback_sidecar_path(table_key)) {
        Ok(data) => data,
        Err(_) => return feedback_mark_loaded(table_key),
    };
    let file: PersistedPlanFeedback =
        match bincode::deserialize::<PersistedPlanFeedback>(&data) {
            Ok(file)
                if file.version == FEEDBACK_SCHEMA_VERSION
                    && file.fingerprint == *FEEDBACK_ENVIRONMENT =>
            {
                file
            }
            _ => return feedback_mark_loaded(table_key),
        };
    if !file.entries.is_empty() {
        let mut cache = PLAN_FEEDBACK.write();
        let inner = feedback_table_mut(&mut cache, table_key);
        for (key, entry) in file.entries {
            if inner.contains_key(&key) {
                continue;
            }
            feedback_evict_shape_if_full(inner, key);
            inner.insert(key, entry);
        }
    }
    feedback_mark_loaded(table_key);
}

/// Store ANALYZE results into the stats cache
pub fn store_table_stats(table_key: &str, mut stats: TableStats) {
    stats.schema_version = STATS_SCHEMA_VERSION;
    stats.schema_generation = 0;
    stats.data_generation = 0;
    if let Ok(data) = bincode::serialize(&stats) {
        let _ = std::fs::write(stats_sidecar_path(table_key), data);
    }
    let epoch = crate::storage::epoch::current(std::path::Path::new(table_key));
    stats_cache_insert(&mut STATS_CACHE.write(), table_key, stats, epoch);
}

/// Retrieve cached stats for a table
pub fn get_table_stats(table_key: &str) -> Option<TableStats> {
    let epoch = crate::storage::epoch::current(std::path::Path::new(table_key));
    let cached = {
        let cache = STATS_CACHE.read();
        match cache.get(table_key) {
            Some(entry) if entry.observed_epoch == epoch => {
                if !stats_are_fresh(table_key, &entry.stats) {
                    return None;
                }
                Some(entry.stats.clone())
            }
            _ => None,
        }
    };
    if let Some(stats) = cached {
        return Some(stats);
    }
    STATS_CACHE.write().remove(table_key);

    let sidecar = stats_sidecar_path(table_key);
    let data = std::fs::read(sidecar).ok()?;
    let stats: TableStats = bincode::deserialize(&data).ok()?;
    if !stats_are_fresh(table_key, &stats) {
        return None;
    }
    stats_cache_insert(&mut STATS_CACHE.write(), table_key, stats.clone(), epoch);
    Some(stats)
}

/// Invalidate stats for a table (e.g., after DML)
pub fn invalidate_table_stats(table_key: &str) {
    STATS_CACHE.write().remove(table_key);
}

/// Invalidate statistics after a schema-changing DDL operation. Plan feedback
/// is calibrated per query shape against the old schema and table shape, so it
/// is dropped as well (memory and sidecar); each shape recalibrates on its next
/// EXPLAIN ANALYZE (Q2). Data-only writes keep their calibration because the
/// per-shape sliding averages are meant to age with the table.
pub fn invalidate_table_schema_stats(table_key: &str) {
    STATS_CACHE.write().remove(table_key);
    invalidate_table_plan_feedback(table_key);
}

/// Forget one table's plan feedback in memory and on disk.
pub fn invalidate_table_plan_feedback(table_key: &str) {
    PLAN_FEEDBACK.write().remove(table_key);
    FEEDBACK_LOADED.write().remove(table_key);
    let _ = std::fs::remove_file(feedback_sidecar_path(table_key));
}

fn feedback_key(table_key: &str, select: &SelectStatement) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    table_key.hash(&mut hasher);
    format!("{:?}", select).hash(&mut hasher);
    hasher.finish()
}

/// True when the strategy belongs to the index cost class.
pub fn is_index_cost_class(strategy: &ExecutionStrategy) -> bool {
    matches!(
        strategy,
        ExecutionStrategy::OltpIndexLookup { .. } | ExecutionStrategy::OltpPrimaryKey { .. }
    )
}

/// The cost class that actually executed, as recorded into PLAN_FEEDBACK
/// (architecture review R5.3; the parallel batch scan class added by R5.12).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutedCostClass {
    Scan,
    Index,
    ParallelScan,
}

/// The initial threshold for the cost-based auto-enable of the parallel
/// batch scan (architecture review 14.8.5): the R5.3-calibrated serial
/// prediction must reach 2 ms for the parallel dispatch cost to amortize.
pub const PARALLEL_SCAN_AUTO_ENABLE_US: f64 = 2000.0;

/// Decision input for the R5.12 auto-enable: the calibrated serial
/// prediction, the parallel-class sample count, and the parallel-class
/// measured-time average for (table, shape), in microseconds. None while
/// the shape has no calibrated serial sample (the default stays serial).
pub fn parallel_decision_input(
    table_key: &str,
    select: &SelectStatement,
) -> Option<(f64, u64, f64)> {
    ensure_feedback_loaded(table_key);
    let guard = PLAN_FEEDBACK.read();
    let entry = guard.get(table_key)?.get(&feedback_key(table_key, select))?;
    (entry.scan_samples > 0).then_some((
        entry.scan_time_avg_us,
        entry.parallel_samples,
        entry.parallel_time_avg_us,
    ))
}

/// Record runtime feedback for EXPLAIN ANALYZE and future executions of the
/// same normalized AST shape: row estimates (row-dimension calibration) and
/// model cost vs measured time for the cost class that actually executed
/// (time-dimension calibration, architecture review R5.3; the parallel
/// batch scan class, R5.12).
pub fn record_plan_feedback(
    table_key: &str,
    select: &SelectStatement,
    strategy: &ExecutionStrategy,
    estimated_rows: f64,
    actual_rows: f64,
    executed_class: ExecutedCostClass,
    executed_cost: f64,
    actual_time_us: f64,
) {
    ensure_feedback_loaded(table_key);
    // The persist lock spans the memory update and the sidecar write so
    // concurrent records cannot truncate each other's file snapshots; the
    // planning read path never takes this lock.
    let _persist = FEEDBACK_PERSIST_LOCK.lock().unwrap();
    let key = feedback_key(table_key, select);
    let snapshot = {
        let mut cache = PLAN_FEEDBACK.write();
        let inner = feedback_table_mut(&mut cache, table_key);
        feedback_evict_shape_if_full(inner, key);
        let entry = inner.entry(key).or_insert_with(|| PlanFeedback {
            strategy: strategy.clone(),
            estimated_rows: 0.0,
            actual_rows: 0.0,
            samples: 0,
            scan_cost_avg: 0.0,
            scan_time_avg_us: 0.0,
            scan_samples: 0,
            index_cost_avg: 0.0,
            index_time_avg_us: 0.0,
            index_samples: 0,
            parallel_cost_avg: 0.0,
            parallel_time_avg_us: 0.0,
            parallel_samples: 0,
        });
        entry.strategy = strategy.clone();
        entry.estimated_rows =
            (entry.estimated_rows * entry.samples as f64 + estimated_rows)
                / (entry.samples as f64 + 1.0);
        entry.actual_rows = (entry.actual_rows * entry.samples as f64 + actual_rows)
            / (entry.samples as f64 + 1.0);
        entry.samples = entry.samples.saturating_add(1);
        let bucket = match executed_class {
            ExecutedCostClass::Index => (
                &mut entry.index_cost_avg,
                &mut entry.index_time_avg_us,
                &mut entry.index_samples,
            ),
            ExecutedCostClass::ParallelScan => (
                &mut entry.parallel_cost_avg,
                &mut entry.parallel_time_avg_us,
                &mut entry.parallel_samples,
            ),
            ExecutedCostClass::Scan => (
                &mut entry.scan_cost_avg,
                &mut entry.scan_time_avg_us,
                &mut entry.scan_samples,
            ),
        };
        let (cost_avg, time_avg_us, samples) = bucket;
        *cost_avg = (*cost_avg * *samples as f64 + executed_cost) / (*samples as f64 + 1.0);
        *time_avg_us =
            (*time_avg_us * *samples as f64 + actual_time_us) / (*samples as f64 + 1.0);
        *samples = samples.saturating_add(1);
        inner
            .iter()
            .map(|(shape, entry)| (*shape, entry.clone()))
            .collect::<Vec<_>>()
    };
    if let Ok(data) = bincode::serialize(&PersistedPlanFeedback {
        version: FEEDBACK_SCHEMA_VERSION,
        fingerprint: FEEDBACK_ENVIRONMENT.clone(),
        entries: snapshot,
    }) {
        let _ = std::fs::write(feedback_sidecar_path(table_key), data);
    }
}

/// Test-only: forget one table's in-memory feedback and its loaded mark
/// (simulates a process restart for that table).
#[cfg(test)]
pub(crate) fn feedback_reset_table_for_tests(table_key: &str) {
    PLAN_FEEDBACK.write().remove(table_key);
    FEEDBACK_LOADED.write().remove(table_key);
}

/// Test-only: load (once) and look up one (table, shape) feedback entry.
#[cfg(test)]
pub(crate) fn feedback_lookup_for_tests(
    table_key: &str,
    select: &SelectStatement,
) -> Option<PlanFeedback> {
    ensure_feedback_loaded(table_key);
    PLAN_FEEDBACK
        .read()
        .get(table_key)
        .and_then(|inner| inner.get(&feedback_key(table_key, select)))
        .cloned()
}

fn stats_sidecar_path(table_key: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.cbo_stats", table_key))
}

fn table_data_paths(table_key: &str) -> [std::path::PathBuf; 3] {
    let base = std::path::PathBuf::from(table_key);
    let name = base.file_name().unwrap_or_default().to_string_lossy();
    let mut delta = base.clone();
    delta.set_file_name(format!("{}.delta", name));
    let mut delta_store = base.clone();
    delta_store.set_file_name(format!("{}.deltastore", name));
    [base, delta, delta_store]
}

pub fn table_data_size(table_key: &str) -> u64 {
    table_data_paths(table_key)
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .fold(0u64, |size, metadata| size.saturating_add(metadata.len()))
}

fn stats_are_fresh(table_key: &str, stats: &TableStats) -> bool {
    if stats.schema_version != STATS_SCHEMA_VERSION {
        return false;
    }
    let paths = table_data_paths(table_key);
    if stats.source_size != 0 && table_data_size(table_key) != stats.source_size {
        return false;
    }
    paths
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .filter_map(|metadata| metadata.modified().ok())
        .all(|modified| {
            modified
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
                <= stats.collected_at
        })
}

// ============================================================================
// Cost Model
// ============================================================================

/// Cost of different operations (relative units)
const COST_SEQ_SCAN_PER_ROW: f64 = 1.0;
const COST_INDEX_LOOKUP: f64 = 4.0;
const COST_INDEX_SCAN_PER_ROW: f64 = 1.5;
const COST_INDEX_BASE_FETCH_PER_ROW: f64 = 1.0;
const COST_HASH_BUILD_PER_ROW: f64 = 2.0;
const COST_HASH_PROBE_PER_ROW: f64 = 0.5;
const COST_SORT_PER_ROW_LOG: f64 = 0.1;

/// Estimated cost of an execution plan
#[derive(Debug, Clone)]
pub struct PlanCost {
    /// Total estimated cost (lower is better)
    pub total: f64,
    /// Estimated output rows
    pub output_rows: f64,
    /// Estimated rows read from the storage layer.
    pub rows_read: f64,
}

impl PlanCost {
    fn seq_scan(row_count: f64) -> Self {
        Self {
            total: row_count * COST_SEQ_SCAN_PER_ROW,
            output_rows: row_count,
            rows_read: row_count,
        }
    }

    fn index_scan(row_count: f64, selectivity: f64) -> Self {
        let output = row_count * selectivity;
        Self {
            // Index lookup returns row ids first; materializing non-covering
            // rows has a separate random/scatter-read component.
            total: COST_INDEX_LOOKUP
                + output * (COST_INDEX_SCAN_PER_ROW + COST_INDEX_BASE_FETCH_PER_ROW),
            output_rows: output,
            rows_read: output,
        }
    }

    fn hash_join(left: &PlanCost, right: &PlanCost) -> Self {
        let build = left.output_rows * COST_HASH_BUILD_PER_ROW;
        let probe = right.output_rows * COST_HASH_PROBE_PER_ROW;
        Self {
            total: left.total + right.total + build + probe,
            output_rows: left.output_rows.min(right.output_rows),
            rows_read: left.rows_read + right.rows_read,
        }
    }
}

/// A physical candidate considered by the planner.
#[derive(Debug, Clone)]
pub struct PlanCandidate {
    pub name: String,
    pub strategy: ExecutionStrategy,
    pub cost: PlanCost,
    /// Directly executable index access decision (index candidates only).
    pub execution: Option<IndexExecutionSpec>,
}

/// The complete decision handed to the executor and EXPLAIN.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub strategy: ExecutionStrategy,
    pub cost: PlanCost,
    pub candidates: Vec<PlanCandidate>,
    pub stats_available: bool,
    pub feedback_applied: bool,
    /// Execution spec of the chosen candidate (index routes only).
    pub execution: Option<IndexExecutionSpec>,
    /// Time spent constructing and costing candidates.
    pub planning_time_micros: u64,
}

/// Directly executable index access decision materialized at planning time
/// (architecture review R5.6).  The executor consumes this instead of
/// re-deriving predicate materialization from the WHERE clause.
#[derive(Debug, Clone)]
pub struct IndexExecutionSpec {
    /// Index-usable predicates extracted from the AND-flattened WHERE clause.
    pub predicates: Vec<(String, PredicateHint)>,
    /// The full WHERE clause when it contains a disjunction.
    pub disjunction: Option<SqlExpr>,
    /// The full WHERE clause, reapplied after fetch to enforce residual
    /// (non-indexed) predicates.
    pub residual: SqlExpr,
    /// Attempt the index-only (covering) scan.
    pub try_covering_scan: bool,
    /// Skip the post-fetch residual filter.
    pub skip_residual_filter: bool,
    /// Columns of the composite index the covering decision was made on
    /// (None when the decision did not depend on a composite index).
    pub composite_columns: Option<Vec<String>>,
}

/// Storage facts that affect physical plan legality and cost.
#[derive(Debug, Clone, Copy, Default)]
pub struct PlannerContext {
    pub mmap_only: bool,
    /// Rows and row groups surviving storage zone-map pruning.
    pub zone_map: Option<(u64, u64, u32, u32)>,
}

// ============================================================================
// Selectivity Estimator
// ============================================================================

impl QueryPlanner {
    /// Estimate selectivity of a WHERE expression using table stats
    pub fn estimate_selectivity(expr: &SqlExpr, stats: &TableStats) -> f64 {
        match expr {
            SqlExpr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::Eq => {
                        let (col, literal) = match (left.as_ref(), right.as_ref()) {
                            (SqlExpr::Column(col), SqlExpr::Literal(value)) => {
                                (Some(col), Some(value))
                            }
                            (SqlExpr::Literal(value), SqlExpr::Column(col)) => {
                                (Some(col), Some(value))
                            }
                            _ => (None, None),
                        };
                        if let Some(cs) = col.and_then(|column| stats.columns.get(column)) {
                            if let Some(value) = literal {
                                let rendered = value.to_string();
                                if let Some((_, count)) = cs
                                    .most_common_values
                                    .iter()
                                    .find(|(candidate, _)| candidate == &rendered)
                                {
                                    return (*count as f64 / stats.row_count.max(1) as f64)
                                        .clamp(0.0, 1.0);
                                }
                            }
                            if cs.ndv > 0 {
                                return (1.0 / cs.ndv as f64).min(1.0);
                            }
                        }
                        0.01 // default for equality
                    }
                    BinaryOperator::Gt
                    | BinaryOperator::Ge
                    | BinaryOperator::Lt
                    | BinaryOperator::Le => {
                        let (column, literal) = match (left.as_ref(), right.as_ref()) {
                            (SqlExpr::Column(column), literal) => {
                                (Some(column), Self::literal_f64(literal))
                            }
                            (literal, SqlExpr::Column(column)) => {
                                (Some(column), Self::literal_f64(literal))
                            }
                            _ => (None, None),
                        };
                        if let (Some(column), Some(value)) = (column, literal) {
                            if let Some(cs) = stats.columns.get(column) {
                                return Self::estimate_range_selectivity(cs, op, value);
                            }
                        }
                        0.33
                    }
                    BinaryOperator::And => {
                        let s1 = Self::estimate_selectivity(left, stats);
                        let s2 = Self::estimate_selectivity(right, stats);
                        (s1 * s2).clamp(0.0, 1.0)
                    }
                    BinaryOperator::Or => {
                        let s1 = Self::estimate_selectivity(left, stats);
                        let s2 = Self::estimate_selectivity(right, stats);
                        (s1 + s2 - s1 * s2).min(1.0)
                    }
                    BinaryOperator::NotEq => {
                        if let SqlExpr::Column(col) = left.as_ref() {
                            if let Some(cs) = stats.columns.get(col) {
                                if cs.ndv > 0 {
                                    return 1.0 - 1.0 / cs.ndv as f64;
                                }
                            }
                        }
                        0.99
                    }
                    _ => 0.5,
                }
            }
            SqlExpr::Between {
                column, low, high, ..
            } => {
                let Some(cs) = stats.columns.get(column) else {
                    return 0.15;
                };
                let (Some(low), Some(high)) = (Self::literal_f64(low), Self::literal_f64(high))
                else {
                    return 0.15;
                };
                match (cs.numeric_min, cs.numeric_max) {
                    (Some(min), Some(max)) if max > min => {
                        let low_cdf = ((low - min) / (max - min)).clamp(0.0, 1.0);
                        let high_cdf = ((high - min) / (max - min)).clamp(0.0, 1.0);
                        (high_cdf - low_cdf).clamp(0.0, 1.0)
                    }
                    _ => 0.15,
                }
            }
            SqlExpr::In { column, values, .. } => {
                if let Some(cs) = stats.columns.get(column) {
                    if cs.ndv > 0 {
                        return (values.len() as f64 / cs.ndv as f64).min(1.0);
                    }
                }
                (values.len() as f64 * 0.01).min(0.5)
            }
            SqlExpr::Like { .. } => 0.1,
            SqlExpr::IsNull { negated, .. } => {
                if let SqlExpr::IsNull { column, .. } = expr {
                    if let Some(cs) = stats.columns.get(column) {
                        let null_fraction = if stats.row_count == 0 {
                            0.0
                        } else {
                            cs.null_count as f64 / stats.row_count as f64
                        };
                        return if *negated {
                            (1.0 - null_fraction).clamp(0.0, 1.0)
                        } else {
                            null_fraction.clamp(0.0, 1.0)
                        };
                    }
                }
                if *negated {
                    0.95
                } else {
                    0.05
                }
            }
            SqlExpr::UnaryOp {
                op: crate::query::sql_parser::UnaryOperator::Not,
                expr,
            } => 1.0 - Self::estimate_selectivity(expr, stats),
            _ => 0.5,
        }
    }

    fn literal_f64(expr: &SqlExpr) -> Option<f64> {
        match expr {
            SqlExpr::Literal(Value::Int64(value)) => Some(*value as f64),
            SqlExpr::Literal(Value::UInt64(value)) => Some(*value as f64),
            SqlExpr::Literal(Value::Float64(value)) => Some(*value),
            _ => None,
        }
    }

    fn estimate_range_selectivity(stats: &ColumnStats, op: &BinaryOperator, value: f64) -> f64 {
        let Some(min) = stats.numeric_min else {
            return 0.33;
        };
        let Some(max) = stats.numeric_max else {
            return 0.33;
        };
        if max <= min {
            return match op {
                BinaryOperator::Ge | BinaryOperator::Le => 1.0,
                BinaryOperator::Gt | BinaryOperator::Lt => 0.0,
                _ => 1.0,
            };
        }
        let cdf = |x: f64| {
            if stats.histogram.is_empty() {
                return ((x - min) / (max - min)).clamp(0.0, 1.0);
            }
            let total: u64 = stats.histogram.iter().map(|bucket| bucket.row_count).sum();
            if total == 0 {
                return ((x - min) / (max - min)).clamp(0.0, 1.0);
            }
            let mut before = 0u64;
            for bucket in &stats.histogram {
                if x >= bucket.upper {
                    before += bucket.row_count;
                    continue;
                }
                if x <= bucket.lower || bucket.upper <= bucket.lower {
                    break;
                }
                let fraction = ((x - bucket.lower) / (bucket.upper - bucket.lower)).clamp(0.0, 1.0);
                return (before as f64 + fraction * bucket.row_count as f64) / total as f64;
            }
            before as f64 / total as f64
        };
        match op {
            BinaryOperator::Gt => 1.0 - cdf(value),
            BinaryOperator::Ge => 1.0 - cdf(value),
            BinaryOperator::Lt => cdf(value),
            BinaryOperator::Le => cdf(value),
            _ => 0.33,
        }
    }

    /// Determine whether to use an index or full scan based on cost
    pub fn should_use_index(col: &str, selectivity: f64, row_count: u64) -> bool {
        let scan_cost = PlanCost::seq_scan(row_count as f64);
        let index_cost = PlanCost::index_scan(row_count as f64, selectivity);
        index_cost.total < scan_cost.total
    }

    /// Detect an OR anywhere in the expression.
    pub fn contains_disjunction(expr: &SqlExpr) -> bool {
        matches!(expr, SqlExpr::BinaryOp { op: BinaryOperator::Or, .. })
            || match expr {
                SqlExpr::BinaryOp { left, right, .. } => {
                    Self::contains_disjunction(left) || Self::contains_disjunction(right)
                }
                SqlExpr::Paren(inner) => Self::contains_disjunction(inner),
                _ => false,
            }
    }

    /// Convert a SqlExpr literal to a Value (for index lookup).
    pub fn expr_to_value(expr: &SqlExpr) -> Option<Value> {
        match expr {
            SqlExpr::Literal(v) => Some(v.clone()),
            _ => None,
        }
    }

    /// Extract index-usable predicates from an expression (flattens AND chains).
    /// Each predicate is (column_name, PredicateHint).
    pub fn extract_index_predicates(
        expr: &SqlExpr,
        out: &mut Vec<(String, PredicateHint)>,
    ) {
        match expr {
            // AND chain: recurse into both sides
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                Self::extract_index_predicates(left, out);
                Self::extract_index_predicates(right, out);
            }
            // col OP literal or literal OP col
            SqlExpr::BinaryOp { left, op, right } => {
                if let SqlExpr::Column(col) = left.as_ref() {
                    if col != "_id" {
                        if let Some(val) = Self::expr_to_value(right) {
                            let h = match op {
                                BinaryOperator::Eq => Some(PredicateHint::Eq(val)),
                                BinaryOperator::Gt => Some(PredicateHint::Gt(val)),
                                BinaryOperator::Ge => Some(PredicateHint::Gte(val)),
                                BinaryOperator::Lt => Some(PredicateHint::Lt(val)),
                                BinaryOperator::Le => Some(PredicateHint::Lte(val)),
                                _ => None,
                            };
                            if let Some(hint) = h {
                                out.push((col.clone(), hint));
                            }
                        }
                    }
                } else if let SqlExpr::Column(col) = right.as_ref() {
                    if col != "_id" {
                        if let Some(val) = Self::expr_to_value(left) {
                            let h = match op {
                                BinaryOperator::Eq => Some(PredicateHint::Eq(val)),
                                BinaryOperator::Gt => Some(PredicateHint::Lt(val)),
                                BinaryOperator::Ge => Some(PredicateHint::Lte(val)),
                                BinaryOperator::Lt => Some(PredicateHint::Gt(val)),
                                BinaryOperator::Le => Some(PredicateHint::Gte(val)),
                                _ => None,
                            };
                            if let Some(hint) = h {
                                out.push((col.clone(), hint));
                            }
                        }
                    }
                }
            }
            SqlExpr::Between {
                column,
                low,
                high,
                negated,
            } => {
                if !negated && column != "_id" {
                    if let (Some(low_val), Some(high_val)) =
                        (Self::expr_to_value(low), Self::expr_to_value(high))
                    {
                        out.push((
                            column.clone(),
                            PredicateHint::Range {
                                low: low_val,
                                high: high_val,
                            },
                        ));
                    }
                }
            }
            SqlExpr::In {
                column,
                values,
                negated,
            } => {
                if !negated && column != "_id" {
                    out.push((column.clone(), PredicateHint::In(values.clone())));
                }
            }
            _ => {}
        }
    }

    /// Return true only when every predicate can be represented by an index
    /// candidate.  This controls index-only scans; ordinary index scans still
    /// accept partial coverage and apply the residual filter.
    pub fn is_fully_indexable_predicate(
        expr: &SqlExpr,
        indexed_columns: &HashSet<String>,
    ) -> bool {
        match expr {
            SqlExpr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    Self::is_fully_indexable_predicate(left, indexed_columns)
                        && Self::is_fully_indexable_predicate(right, indexed_columns)
                }
                BinaryOperator::Eq
                | BinaryOperator::Gt
                | BinaryOperator::Ge
                | BinaryOperator::Lt
                | BinaryOperator::Le => {
                    (matches!(left.as_ref(), SqlExpr::Column(_))
                        && Self::expr_to_value(right).is_some()
                        && matches!(left.as_ref(), SqlExpr::Column(column) if indexed_columns.contains(column)))
                        || (matches!(right.as_ref(), SqlExpr::Column(_))
                            && Self::expr_to_value(left).is_some()
                            && matches!(right.as_ref(), SqlExpr::Column(column) if indexed_columns.contains(column)))
                }
                _ => false,
            },
            SqlExpr::Between {
                column,
                low,
                high,
                negated,
            } => {
                !negated
                    && indexed_columns.contains(column)
                    && Self::expr_to_value(low).is_some()
                    && Self::expr_to_value(high).is_some()
            }
            SqlExpr::In {
                column,
                values,
                negated,
            } => !negated && indexed_columns.contains(column) && !values.is_empty(),
            _ => false,
        }
    }

    /// Materialize the directly executable index access decision carried by
    /// every index candidate (architecture review R5.6).  The covering and
    /// residual-skip flags mirror the executor's live conditions on the
    /// planning-time index state, so a fresh spec never changes the physical
    /// route; the executor re-verifies the state before trusting them.
    pub fn build_index_execution_spec(
        index_manager: &IndexManager,
        where_clause: &SqlExpr,
    ) -> IndexExecutionSpec {
        let mut predicates = Vec::new();
        Self::extract_index_predicates(where_clause, &mut predicates);
        let equality_values: HashMap<String, Value> = predicates
            .iter()
            .filter_map(|(column, hint)| match hint {
                PredicateHint::Eq(value) => Some((column.clone(), value.clone())),
                _ => None,
            })
            .collect();
        let composite_columns = index_manager
            .list_indexes()
            .into_iter()
            .filter(|meta| meta.is_composite())
            .map(|meta| {
                meta.effective_columns()
                    .iter()
                    .map(|column| column.to_string())
                    .collect::<Vec<_>>()
            })
            .filter_map(|columns| {
                let prefix_len = columns
                    .iter()
                    .take_while(|column| equality_values.contains_key(*column))
                    .count();
                (prefix_len > 0).then_some((columns, prefix_len))
            })
            .max_by_key(|(_, prefix_len)| *prefix_len)
            .map(|(columns, _)| columns);
        let indexed_columns: HashSet<String> = predicates
            .iter()
            .filter(|(column, _)| index_manager.has_single_column_index_on(column))
            .map(|(column, _)| column.clone())
            .chain(composite_columns.iter().flat_map(|columns| columns.iter().cloned()))
            .collect();
        let full_predicate_covered =
            Self::is_fully_indexable_predicate(where_clause, &indexed_columns);
        let composite_is_full_equality = composite_columns
            .as_ref()
            .map(|columns| {
                columns
                    .iter()
                    .take_while(|column| equality_values.contains_key(*column))
                    .count()
                    == columns.len()
            })
            .unwrap_or(false);
        IndexExecutionSpec {
            disjunction: Self::contains_disjunction(where_clause).then(|| where_clause.clone()),
            residual: where_clause.clone(),
            try_covering_scan: full_predicate_covered
                && (composite_columns.is_none() || composite_is_full_equality),
            skip_residual_filter: full_predicate_covered && composite_is_full_equality,
            composite_columns,
            predicates,
        }
    }

    /// Optimize join order for a list of tables with estimated row counts
    /// Returns tables sorted by ascending row count (smallest table first = build side)
    pub fn optimize_join_order(tables: &[(String, u64)]) -> Vec<String> {
        let mut sorted = tables.to_vec();
        sorted.sort_by_key(|(_, rows)| *rows);
        sorted.into_iter().map(|(name, _)| name).collect()
    }
}

// ============================================================================
// Execution Strategy
// ============================================================================

/// The chosen execution strategy for a query
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionStrategy {
    /// Use OLTP path: index-based lookups
    OltpIndexLookup {
        /// Column to use for index lookup
        column: String,
        /// Type of lookup
        lookup_type: IndexLookupType,
    },
    /// Use OLTP path: primary key lookup (_id = X)
    OltpPrimaryKey {
        /// The _id value to look up
        id_value: i64,
    },
    /// Use OLAP path: full vectorized columnar scan
    OlapFullScan,
    /// Use OLAP path: vectorized scan with filter pushdown
    OlapFilteredScan,
    /// Use OLAP path: aggregation query
    OlapAggregation,
    /// Direct write (INSERT/UPDATE/DELETE)
    DirectWrite,
    /// DDL operation (CREATE/ALTER/DROP TABLE)
    Ddl,
}

/// Type of index lookup for OLTP path
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexLookupType {
    /// Exact equality: col = value
    Equality,
    /// Range: col BETWEEN low AND high
    Range,
    /// IN list: col IN (v1, v2, ...)
    InList,
    /// Intersect independently indexed AND predicates.
    Intersection,
    /// Union and deduplicate fully indexed OR branches.
    Union,
    /// Equality prefix of a composite BTree index.
    CompositePrefix,
    /// Equality prefix followed by a range on the next composite key.
    CompositeRange,
}

// ============================================================================
// Query Characteristics
// ============================================================================

/// Analyzed characteristics of a query
#[derive(Debug, Clone)]
pub struct QueryCharacteristics {
    /// Whether the query has aggregation functions
    pub has_aggregation: bool,
    /// Whether the query has GROUP BY
    pub has_group_by: bool,
    /// Whether the query has ORDER BY
    pub has_order_by: bool,
    /// Whether the query has JOIN
    pub has_join: bool,
    /// Whether the query has subqueries
    pub has_subquery: bool,
    /// Whether the predicate contains OR, which requires an index union plan.
    pub has_disjunction: bool,
    /// Whether the query has LIMIT
    pub has_limit: bool,
    /// Whether the query filters on _id (primary key)
    pub filters_on_pk: bool,
    /// Columns used in WHERE clause equality conditions
    pub equality_filter_columns: Vec<String>,
    /// Columns used in WHERE clause range conditions
    pub range_filter_columns: Vec<String>,
    /// Estimated selectivity (0.0 = no rows, 1.0 = all rows)
    pub estimated_selectivity: f64,
    /// Whether this is a write operation
    pub is_write: bool,
    /// Whether this is a DDL operation
    pub is_ddl: bool,
}

impl Default for QueryCharacteristics {
    fn default() -> Self {
        Self {
            has_aggregation: false,
            has_group_by: false,
            has_order_by: false,
            has_join: false,
            has_subquery: false,
            has_disjunction: false,
            has_limit: false,
            filters_on_pk: false,
            equality_filter_columns: Vec::new(),
            range_filter_columns: Vec::new(),
            estimated_selectivity: 1.0,
            is_write: false,
            is_ddl: false,
        }
    }
}

// ============================================================================
// Query Planner
// ============================================================================

/// Query planner that analyzes SQL and selects execution strategy
pub struct QueryPlanner;

impl QueryPlanner {
    /// Analyze a parsed SQL statement and determine the best execution strategy
    pub fn plan(stmt: &SqlStatement, index_manager: Option<&IndexManager>) -> ExecutionStrategy {
        match stmt {
            SqlStatement::Select(select) => Self::plan_select(select, index_manager),
            SqlStatement::Insert { .. } => ExecutionStrategy::DirectWrite,
            SqlStatement::Update { .. } => ExecutionStrategy::DirectWrite,
            SqlStatement::Delete { .. } => ExecutionStrategy::DirectWrite,
            SqlStatement::CreateTable { .. }
            | SqlStatement::DropTable { .. }
            | SqlStatement::AlterTable { .. }
            | SqlStatement::TruncateTable { .. } => ExecutionStrategy::Ddl,
            _ => ExecutionStrategy::OlapFullScan,
        }
    }

    /// Plan a SELECT query
    fn plan_select(
        select: &SelectStatement,
        index_manager: Option<&IndexManager>,
    ) -> ExecutionStrategy {
        Self::plan_select_with_stats(select, index_manager, None, PlannerContext::default())
            .strategy
    }

    /// Plan with CBO: use table stats for cost-based index/scan decisions
    pub fn plan_with_stats(
        stmt: &SqlStatement,
        index_manager: Option<&IndexManager>,
        table_key: &str,
    ) -> ExecutionStrategy {
        let stats = get_table_stats(table_key);
        match stmt {
            SqlStatement::Select(select) => {
                Self::plan_select_with_stats(
                    select,
                    index_manager,
                    stats.as_ref(),
                    PlannerContext::default(),
                )
                .strategy
            }
            _ => Self::plan(stmt, index_manager),
        }
    }

    /// Plan SELECT with CBO — takes &SelectStatement directly, avoiding a clone at the call site.
    pub fn plan_select_pub(
        select: &SelectStatement,
        index_manager: Option<&IndexManager>,
        table_key: &str,
    ) -> ExecutionStrategy {
        let stats = get_table_stats(table_key);
        Self::plan_select_with_stats(
            select,
            index_manager,
            stats.as_ref(),
            PlannerContext::default(),
        )
        .strategy
    }

    /// Plan a SELECT and retain candidate/cost information for execution and
    /// EXPLAIN.  Unlike the legacy strategy-only API, this compares all legal
    /// single-table access paths before choosing one.
    pub fn plan_select_details(
        select: &SelectStatement,
        index_manager: Option<&IndexManager>,
        table_key: &str,
        context: PlannerContext,
    ) -> QueryPlan {
        let planning_started = std::time::Instant::now();
        let stats = get_table_stats(table_key);
        let mut plan = Self::plan_select_with_stats(select, index_manager, stats.as_ref(), context);
        if !plan.candidates.is_empty() {
            ensure_feedback_loaded(table_key);
            let guard = PLAN_FEEDBACK.read();
            let feedback = guard
                .get(table_key)
                .and_then(|inner| inner.get(&feedback_key(table_key, select)));
            if let Some(feedback) = feedback {
                let mut corrected = false;
                if feedback.samples > 0 && feedback.estimated_rows > 0.0 {
                    let correction =
                        (feedback.actual_rows / feedback.estimated_rows).clamp(0.25, 4.0);
                    for candidate in &mut plan.candidates {
                        if candidate.strategy == feedback.strategy {
                            candidate.cost.total *= correction;
                        }
                    }
                    corrected = true;
                }
                // Time-dimension calibration (architecture review R5.3):
                // rescale each candidate from the model-cost unit into the
                // measured-time unit of the cost class that actually
                // executed, so scan and index candidates are compared on a
                // common (microsecond) scale.
                for candidate in &mut plan.candidates {
                    let (samples, cost_avg, time_avg_us) =
                        if is_index_cost_class(&candidate.strategy) {
                            (
                                feedback.index_samples,
                                feedback.index_cost_avg,
                                feedback.index_time_avg_us,
                            )
                        } else {
                            (
                                feedback.scan_samples,
                                feedback.scan_cost_avg,
                                feedback.scan_time_avg_us,
                            )
                        };
                    if samples > 0 && cost_avg > 0.0 && time_avg_us > 0.0 {
                        candidate.cost.total /= cost_avg / time_avg_us;
                        corrected = true;
                    }
                }
                if corrected {
                    if let Some(chosen) = plan.candidates.iter().min_by(|left, right| {
                        left.cost
                            .total
                            .partial_cmp(&right.cost.total)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    }) {
                        plan.strategy = chosen.strategy.clone();
                        plan.cost = chosen.cost.clone();
                        plan.execution = chosen.execution.clone();
                        plan.feedback_applied = true;
                    }
                }
            }
        }
        plan.planning_time_micros = planning_started.elapsed().as_micros() as u64;
        plan
    }

    /// Plan SELECT with cost-based optimization using ANALYZE stats
    fn plan_select_with_stats(
        select: &SelectStatement,
        index_manager: Option<&IndexManager>,
        stats: Option<&TableStats>,
        context: PlannerContext,
    ) -> QueryPlan {
        let chars = Self::analyze_select(select);

        let fixed = |strategy: ExecutionStrategy| QueryPlan {
            strategy,
            cost: PlanCost {
                total: 0.0,
                output_rows: 0.0,
                rows_read: 0.0,
            },
            candidates: Vec::new(),
            stats_available: stats.is_some(),
            feedback_applied: false,
            execution: None,
            planning_time_micros: 0,
        };

        if chars.is_write {
            return fixed(ExecutionStrategy::DirectWrite);
        }
        if chars.is_ddl {
            return fixed(ExecutionStrategy::Ddl);
        }
        if chars.has_aggregation || chars.has_group_by {
            return fixed(ExecutionStrategy::OlapAggregation);
        }
        if chars.has_join || chars.has_subquery {
            return fixed(ExecutionStrategy::OlapFullScan);
        }

        // Primary key lookup
        if chars.filters_on_pk {
            if let Some(id) = Self::extract_pk_value(&select.where_clause) {
                return fixed(ExecutionStrategy::OltpPrimaryKey { id_value: id });
            }
        }

        let row_count = stats.map(|s| s.row_count).unwrap_or(10_000).max(1) as f64;
        let selectivity = select
            .where_clause
            .as_ref()
            .map(|expr| {
                stats
                    .map(|s| Self::estimate_selectivity(expr, s))
                    .unwrap_or(chars.estimated_selectivity)
            })
            .unwrap_or(1.0)
            .clamp(0.0, 1.0);
        let output_rows = (row_count * selectivity).max(0.0);
        let scan_strategy = if select.where_clause.is_some()
            && (selectivity < 0.1 || chars.has_limit || context.mmap_only)
        {
            ExecutionStrategy::OlapFilteredScan
        } else {
            ExecutionStrategy::OlapFullScan
        };
        let scan_rows = context
            .zone_map
            .map(|(matching, _, _, _)| matching as f64)
            .unwrap_or(row_count)
            .min(row_count);
        let mut scan_cost = PlanCost::seq_scan(scan_rows);
        scan_cost.output_rows = output_rows;
        let mut candidates = vec![PlanCandidate {
            name: if let Some((_, _, matching_groups, total_groups)) = context.zone_map {
                format!("zone-map-scan({}/{})", matching_groups, total_groups)
            } else if select.where_clause.is_some() {
                "sequential-filtered-scan".to_string()
            } else {
                "sequential-scan".to_string()
            },
            strategy: scan_strategy,
            cost: scan_cost,
            execution: None,
        }];

        if let (Some(idx_mgr), Some(_where_expr)) = (index_manager, &select.where_clause) {
            // Materialize the execution spec once; every index candidate
            // below carries it (architecture review R5.6).
            let index_spec = Self::build_index_execution_spec(idx_mgr, _where_expr);
            if chars.has_disjunction {
                let all_indexed = chars
                    .equality_filter_columns
                    .iter()
                    .all(|column| idx_mgr.has_usable_index_for_predicate(column, false))
                    && chars
                        .range_filter_columns
                        .iter()
                        .all(|column| idx_mgr.has_usable_index_for_predicate(column, true));
                if all_indexed
                    && (!chars.equality_filter_columns.is_empty()
                        || !chars.range_filter_columns.is_empty())
                {
                    let mut cost = PlanCost::index_scan(row_count, selectivity);
                    cost.total += cost.output_rows * 0.15;
                    candidates.push(PlanCandidate {
                        name: "index-union(deduplicated)".to_string(),
                        strategy: ExecutionStrategy::OltpIndexLookup {
                            column: chars
                                .equality_filter_columns
                                .first()
                                .or(chars.range_filter_columns.first())
                                .cloned()
                                .unwrap_or_default(),
                            lookup_type: IndexLookupType::Union,
                        },
                        cost,
                        execution: Some(index_spec.clone()),
                    });
                }
            } else {
                for col in &chars.equality_filter_columns {
                    if !idx_mgr.has_usable_index_for_predicate(col, false) {
                        continue;
                    }
                    let col_selectivity = stats
                        .and_then(|s| s.columns.get(col))
                        .map(|cs| {
                            if cs.ndv > 0 {
                                (1.0 / cs.ndv as f64).min(1.0)
                            } else {
                                selectivity
                            }
                        })
                        .unwrap_or(selectivity);
                    let mut cost = PlanCost::index_scan(row_count, col_selectivity);
                    cost.output_rows = row_count * col_selectivity;
                    candidates.push(PlanCandidate {
                        name: format!("index-scan({})", col),
                        strategy: ExecutionStrategy::OltpIndexLookup {
                            column: col.clone(),
                            lookup_type: IndexLookupType::Equality,
                        },
                        cost,
                        execution: Some(index_spec.clone()),
                    });
                }
                for col in &chars.range_filter_columns {
                    if !idx_mgr.has_usable_index_for_predicate(col, true) {
                        continue;
                    }
                    // Range selectivity must use the range predicate estimate,
                    // not 1/NDV (which is only valid for equality).
                    let mut cost = PlanCost::index_scan(row_count, selectivity);
                    cost.output_rows = output_rows;
                    candidates.push(PlanCandidate {
                        name: format!("index-range-scan({})", col),
                        strategy: ExecutionStrategy::OltpIndexLookup {
                            column: col.clone(),
                            lookup_type: IndexLookupType::Range,
                        },
                        cost,
                        execution: Some(index_spec.clone()),
                    });
                }

                let independently_indexed = chars
                    .equality_filter_columns
                    .iter()
                    .filter(|column| idx_mgr.has_usable_index_for_predicate(column, false))
                    .count()
                    + chars
                        .range_filter_columns
                        .iter()
                        .filter(|column| idx_mgr.has_usable_index_for_predicate(column, true))
                        .count();
                if independently_indexed > 1 {
                    let mut cost = PlanCost::index_scan(row_count, selectivity);
                    cost.total += output_rows * 0.05;
                    candidates.push(PlanCandidate {
                        name: "index-intersection".to_string(),
                        strategy: ExecutionStrategy::OltpIndexLookup {
                            column: chars
                                .equality_filter_columns
                                .first()
                                .cloned()
                                .unwrap_or_default(),
                            lookup_type: IndexLookupType::Intersection,
                        },
                        cost,
                        execution: Some(index_spec.clone()),
                    });
                }

                let composite = idx_mgr
                    .list_indexes()
                    .into_iter()
                    .filter(|meta| meta.is_composite())
                    .filter_map(|meta| {
                        let columns = meta
                            .effective_columns()
                            .iter()
                            .map(|column| column.to_string())
                            .collect::<Vec<_>>();
                        let prefix_len = columns
                            .iter()
                            .take_while(|column| chars.equality_filter_columns.contains(column))
                            .count();
                        let next_is_range = columns
                            .get(prefix_len)
                            .map(|column| chars.range_filter_columns.contains(column))
                            .unwrap_or(false);
                        let legal = prefix_len == columns.len()
                            || (meta.index_type == crate::storage::index::IndexType::BTree
                                && prefix_len > 0);
                        legal.then_some((columns, prefix_len, next_is_range))
                    })
                    .max_by_key(|(_, prefix_len, next_is_range)| (*prefix_len, *next_is_range));
                if let Some((columns, prefix_len, next_is_range)) = composite {
                    let mut cost = PlanCost::index_scan(row_count, selectivity);
                    cost.output_rows = output_rows;
                    candidates.push(PlanCandidate {
                        name: if prefix_len == columns.len() {
                            format!("composite-index-scan({})", columns.join(","))
                        } else if next_is_range {
                            format!(
                                "composite-prefix-range({})",
                                columns[..=prefix_len].join(",")
                            )
                        } else {
                            format!("composite-prefix-scan({})", columns[..prefix_len].join(","))
                        },
                        strategy: ExecutionStrategy::OltpIndexLookup {
                            column: columns.get(prefix_len).unwrap_or(&columns[0]).clone(),
                            lookup_type: if prefix_len == columns.len() {
                                IndexLookupType::Equality
                            } else if next_is_range {
                                IndexLookupType::CompositeRange
                            } else {
                                IndexLookupType::CompositePrefix
                            },
                        },
                        cost,
                        execution: Some(index_spec.clone()),
                    });
                }
            }
        }

        candidates = candidates
            .into_iter()
            .map(|mut candidate| {
                let projection_width = if select.is_select_star() {
                    8.0
                } else {
                    select.columns.len().max(1) as f64
                };
                if candidate.name.starts_with("sequential")
                    || candidate.name.starts_with("zone-map")
                {
                    candidate.cost.total *=
                        (0.30 + projection_width.min(8.0) * 0.0875).clamp(0.30, 1.0);
                } else if candidate.name.contains("index") {
                    let covered = !select.is_select_star()
                        && select.columns.iter().all(|column| match column {
                            SelectColumn::Column(name) => {
                                name == "_id" || chars.equality_filter_columns.contains(name)
                            }
                            SelectColumn::ColumnAlias { column, .. } => {
                                column == "_id" || chars.equality_filter_columns.contains(column)
                            }
                            _ => false,
                        });
                    let width_factor = if covered {
                        0.55
                    } else {
                        (0.75 + projection_width.min(8.0) * 0.0625).min(1.25)
                    };
                    candidate.cost.total = COST_INDEX_LOOKUP
                        + (candidate.cost.total - COST_INDEX_LOOKUP).max(0.0) * width_factor;
                }
                let preserves_order = select.order_by.len() == 1
                    && !select.order_by[0].descending
                    && match &candidate.strategy {
                        ExecutionStrategy::OltpIndexLookup {
                            column,
                            lookup_type:
                                IndexLookupType::Range
                                | IndexLookupType::CompositePrefix
                                | IndexLookupType::CompositeRange,
                        } => select.order_by[0].column == *column,
                        _ => false,
                    };
                if !select.order_by.is_empty() && !preserves_order {
                    let rows = candidate.cost.output_rows.max(1.0);
                    candidate.cost.total += COST_SORT_PER_ROW_LOG * rows * rows.ln();
                }
                if let Some(limit) = select.limit {
                    let wanted = (limit + select.offset.unwrap_or(0)) as f64;
                    if candidate.name.contains("index") && candidate.cost.output_rows > wanted {
                        let ratio = wanted / candidate.cost.output_rows.max(1.0);
                        candidate.cost.total =
                            COST_INDEX_LOOKUP + (candidate.cost.total - COST_INDEX_LOOKUP) * ratio;
                    }
                    candidate.cost.output_rows = candidate.cost.output_rows.min(wanted);
                }
                candidate
            })
            .collect::<Vec<_>>();
        let chosen = candidates
            .iter()
            .min_by(|left, right| {
                left.cost
                    .total
                    .partial_cmp(&right.cost.total)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
            .unwrap_or_else(|| candidates[0].clone());
        let execution = chosen.execution.clone();
        QueryPlan {
            strategy: chosen.strategy,
            cost: chosen.cost,
            candidates,
            stats_available: stats.is_some(),
            feedback_applied: false,
            execution,
            planning_time_micros: 0,
        }
    }

    /// Analyze a SELECT statement to extract characteristics
    fn analyze_select(select: &SelectStatement) -> QueryCharacteristics {
        let mut chars = QueryCharacteristics::default();

        // Check for aggregation in select columns
        for col in &select.columns {
            match col {
                SelectColumn::Aggregate { .. } => {
                    chars.has_aggregation = true;
                }
                _ => {}
            }
        }

        // Check GROUP BY
        if !select.group_by.is_empty() {
            chars.has_group_by = true;
        }

        // Check ORDER BY
        if !select.order_by.is_empty() {
            chars.has_order_by = true;
        }

        // Check LIMIT
        if select.limit.is_some() {
            chars.has_limit = true;
        }

        // Check JOINs
        if !select.joins.is_empty() {
            chars.has_join = true;
        }

        // Analyze WHERE clause
        if let Some(ref where_expr) = select.where_clause {
            Self::analyze_where(where_expr, &mut chars);
        }

        chars
    }

    /// Analyze WHERE clause for index-friendly patterns
    fn analyze_where(expr: &SqlExpr, chars: &mut QueryCharacteristics) {
        match expr {
            SqlExpr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::Eq => {
                        let column = match (left.as_ref(), right.as_ref()) {
                            (SqlExpr::Column(column), SqlExpr::Literal(_))
                            | (SqlExpr::Literal(_), SqlExpr::Column(column)) => Some(column),
                            _ => None,
                        };
                        if let Some(col) = column {
                            if col == "_id" {
                                chars.filters_on_pk = true;
                            }
                            chars.equality_filter_columns.push(col.clone());
                            chars.estimated_selectivity *= 0.01; // Very selective
                        }
                    }
                    BinaryOperator::Gt
                    | BinaryOperator::Ge
                    | BinaryOperator::Lt
                    | BinaryOperator::Le => {
                        let column = match (left.as_ref(), right.as_ref()) {
                            (SqlExpr::Column(column), SqlExpr::Literal(_))
                            | (SqlExpr::Literal(_), SqlExpr::Column(column)) => Some(column),
                            _ => None,
                        };
                        if let Some(col) = column {
                            chars.range_filter_columns.push(col.clone());
                            chars.estimated_selectivity *= 0.3; // Moderately selective
                        }
                    }
                    BinaryOperator::And => {
                        Self::analyze_where(left, chars);
                        Self::analyze_where(right, chars);
                    }
                    BinaryOperator::Or => {
                        Self::analyze_where(left, chars);
                        Self::analyze_where(right, chars);
                        chars.has_disjunction = true;
                        chars.estimated_selectivity = (chars.estimated_selectivity * 2.0).min(1.0);
                    }
                    _ => {}
                }
            }
            SqlExpr::Between {
                column, low, high, ..
            } => {
                chars.range_filter_columns.push(column.clone());
                chars.estimated_selectivity *= 0.2;
            }
            SqlExpr::In { column, values, .. } => {
                chars.equality_filter_columns.push(column.clone());
                chars.estimated_selectivity *= (values.len() as f64 * 0.01).min(0.5);
            }
            _ => {}
        }
    }

    /// Extract primary key value from WHERE _id = X
    fn extract_pk_value(where_clause: &Option<SqlExpr>) -> Option<i64> {
        if let Some(SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        }) = where_clause
        {
            if let SqlExpr::Column(col) = left.as_ref() {
                if col == "_id" {
                    if let SqlExpr::Literal(val) = right.as_ref() {
                        return val.as_i64();
                    }
                }
            }
        }
        None
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use crate::data::DataType;
    use crate::query::sql_parser::SqlParser;
    use crate::storage::index::IndexType;

    fn select_statement(sql: &str) -> SelectStatement {
        match SqlParser::parse(sql).unwrap() {
            SqlStatement::Select(select) => select,
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_strategy_display() {
        let strategy = ExecutionStrategy::OltpPrimaryKey { id_value: 42 };
        assert_eq!(strategy, ExecutionStrategy::OltpPrimaryKey { id_value: 42 });
    }

    #[test]
    fn test_query_characteristics_default() {
        let chars = QueryCharacteristics::default();
        assert!(!chars.has_aggregation);
        assert!(!chars.has_group_by);
        assert!(!chars.has_disjunction);
        assert_eq!(chars.estimated_selectivity, 1.0);
    }

    #[test]
    fn stats_source_size_change_is_stale() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"before").unwrap();
        let stats = TableStats {
            schema_version: STATS_SCHEMA_VERSION,
            schema_generation: 0,
            data_generation: 0,
            row_count: 1,
            columns: HashMap::new(),
            collected_at: u64::MAX,
            source_size: table_data_size(file.path().to_str().unwrap()),
        };
        assert!(stats_are_fresh(file.path().to_str().unwrap(), &stats));

        file.write_all(b"-after").unwrap();
        file.flush().unwrap();
        assert!(!stats_are_fresh(file.path().to_str().unwrap(), &stats));
    }

    #[test]
    fn composite_candidate_uses_declared_index_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut indexes = IndexManager::new("t", dir.path());
        indexes
            .create_index_multi(
                "idx_city_age",
                &["city".to_string(), "age".to_string()],
                IndexType::Hash,
                false,
                DataType::String,
            )
            .unwrap();
        let select = select_statement(
            "SELECT name FROM t WHERE age = 30 AND city = 'NYC' AND name = 'alice'",
        );

        let plan = QueryPlanner::plan_select_with_stats(
            &select,
            Some(&indexes),
            None,
            PlannerContext {
                mmap_only: true,
                zone_map: None,
            },
        );
        assert!(plan
            .candidates
            .iter()
            .any(|candidate| candidate.name == "composite-index-scan(city,age)"));
    }

    #[test]
    fn or_does_not_offer_partial_index_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let mut indexes = IndexManager::new("t", dir.path());
        indexes
            .create_index("idx_age", "age", IndexType::BTree, false, DataType::Int64)
            .unwrap();
        let select = select_statement("SELECT name FROM t WHERE age = 25 OR age = 30");

        let plan = QueryPlanner::plan_select_with_stats(
            &select,
            Some(&indexes),
            None,
            PlannerContext::default(),
        );
        assert!(plan
            .candidates
            .iter()
            .any(|candidate| candidate.name == "index-union(deduplicated)"));
        assert!(!plan
            .candidates
            .iter()
            .any(|candidate| candidate.name == "index-scan(age)"));
    }

    #[test]
    fn mcv_corrects_skewed_equality_selectivity() {
        let stats = TableStats {
            schema_version: STATS_SCHEMA_VERSION,
            schema_generation: 0,
            data_generation: 0,
            row_count: 1000,
            columns: HashMap::from([(
                "category".to_string(),
                ColumnStats {
                    ndv: 100,
                    null_count: 0,
                    min_value: "common".to_string(),
                    max_value: "rare".to_string(),
                    numeric_min: None,
                    numeric_max: None,
                    histogram: Vec::new(),
                    most_common_values: vec![("common".to_string(), 600)],
                },
            )]),
            collected_at: 0,
            source_size: 0,
        };
        let common = select_statement("SELECT * FROM t WHERE category = 'common'")
            .where_clause
            .unwrap();
        let rare = select_statement("SELECT * FROM t WHERE category = 'rare'")
            .where_clause
            .unwrap();
        assert_eq!(QueryPlanner::estimate_selectivity(&common, &stats), 0.6);
        assert_eq!(QueryPlanner::estimate_selectivity(&rare, &stats), 0.01);
    }

    #[test]
    fn stats_cache_is_bounded_and_evicts_oldest() {
        let mut cache: HashMap<String, StatsCacheEntry> = HashMap::new();
        let stats = TableStats {
            schema_version: STATS_SCHEMA_VERSION,
            schema_generation: 0,
            data_generation: 0,
            row_count: 1,
            columns: HashMap::new(),
            collected_at: 0,
            source_size: 0,
        };
        for i in 0..STATS_CACHE_CAP + 32 {
            stats_cache_insert(&mut cache, &format!("table_{i}"), stats.clone(), i as u64);
        }
        assert_eq!(cache.len(), STATS_CACHE_CAP);
        // The oldest insertions are evicted; recent tables stay cached.
        assert!(!cache.contains_key("table_0"));
        assert!(cache.contains_key(&format!("table_{}", STATS_CACHE_CAP + 31)));
        // A replacement of an existing key does not evict anything.
        let before = cache.len();
        stats_cache_insert(&mut cache, "table_1", stats, 999);
        assert_eq!(cache.len(), before);
    }

    #[test]
    fn plan_feedback_shape_and_table_caps_evict_least_observed() {
        fn entry(samples: u64) -> PlanFeedback {
            PlanFeedback {
                strategy: ExecutionStrategy::OlapFullScan,
                estimated_rows: 0.0,
                actual_rows: 0.0,
                samples,
                scan_cost_avg: 0.0,
                scan_time_avg_us: 0.0,
                scan_samples: 0,
                index_cost_avg: 0.0,
                index_time_avg_us: 0.0,
                index_samples: 0,
                parallel_cost_avg: 0.0,
                parallel_time_avg_us: 0.0,
                parallel_samples: 0,
            }
        }

        // Per-table shape cap: the least-observed shape is dropped first.
        let mut shapes: HashMap<u64, PlanFeedback> = HashMap::new();
        for shape in 0..PLAN_FEEDBACK_SHAPES_PER_TABLE as u64 {
            shapes.insert(shape, entry(shape + 1));
        }
        feedback_evict_shape_if_full(&mut shapes, u64::MAX);
        assert_eq!(shapes.len(), PLAN_FEEDBACK_SHAPES_PER_TABLE - 1);
        assert!(!shapes.contains_key(&0), "shape with the fewest samples is evicted");
        // Recording an existing shape never evicts.
        feedback_evict_shape_if_full(&mut shapes, 7);
        assert!(shapes.contains_key(&7));

        // Table cap: the table with the fewest total samples is dropped.
        let mut cache: HashMap<String, HashMap<u64, PlanFeedback>> = HashMap::new();
        for table in 0..PLAN_FEEDBACK_TABLES {
            let mut inner = HashMap::new();
            inner.insert(0u64, entry(table as u64 + 1));
            cache.insert(format!("table_{table}"), inner);
        }
        let _ = feedback_table_mut(&mut cache, "table_new");
        assert_eq!(cache.len(), PLAN_FEEDBACK_TABLES);
        assert!(!cache.contains_key("table_0"));
        assert!(cache.contains_key("table_new"));
    }

    #[test]
    fn schema_change_clears_table_plan_feedback() {
        let dir = std::env::temp_dir().join(format!(
            "apex_planner_feedback_invalidate_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let table_key = dir.join("t.apex").to_string_lossy().to_string();
        let sidecar = format!("{table_key}.plan_feedback");
        let select = select_statement("SELECT k, COUNT(*) FROM t WHERE k >= 1 GROUP BY k");

        record_plan_feedback(
            &table_key,
            &select,
            &ExecutionStrategy::OlapAggregation,
            100.0,
            100.0,
            ExecutedCostClass::Scan,
            1.0,
            10.0,
        );
        assert!(std::path::Path::new(&sidecar).exists());
        let recorded = feedback_lookup_for_tests(&table_key, &select).unwrap();
        assert_eq!(recorded.samples, 1);

        // A schema change drops the calibration in memory and on disk; data-only
        // writes keep it.
        invalidate_table_schema_stats(&table_key);
        assert!(feedback_lookup_for_tests(&table_key, &select).is_none());
        assert!(!std::path::Path::new(&sidecar).exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn plan_feedback_from_another_environment_is_ignored() {
        let dir = std::env::temp_dir().join(format!(
            "apex_planner_feedback_environment_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let table_key = dir.join("t.apex").to_string_lossy().to_string();
        let sidecar = format!("{table_key}.plan_feedback");
        let select = select_statement("SELECT k, COUNT(*) FROM t WHERE k >= 1 GROUP BY k");
        let key = feedback_key(&table_key, &select);
        let entry = |samples: u64| PlanFeedback {
            strategy: ExecutionStrategy::OlapAggregation,
            estimated_rows: 0.0,
            actual_rows: 0.0,
            samples,
            scan_cost_avg: 0.0,
            scan_time_avg_us: 0.0,
            scan_samples: 0,
            index_cost_avg: 0.0,
            index_time_avg_us: 0.0,
            index_samples: 0,
            parallel_cost_avg: 0.0,
            parallel_time_avg_us: 0.0,
            parallel_samples: 0,
        };

        // A sidecar measured in another environment must not feed the
        // auto-parallel thresholds of this one.
        let foreign = PersistedPlanFeedback {
            version: FEEDBACK_SCHEMA_VERSION,
            fingerprint: format!("{}-other/0", FEEDBACK_ENVIRONMENT.as_str()),
            entries: vec![(key, entry(3))],
        };
        std::fs::write(&sidecar, bincode::serialize(&foreign).unwrap()).unwrap();
        feedback_reset_table_for_tests(&table_key);
        assert!(
            feedback_lookup_for_tests(&table_key, &select).is_none(),
            "feedback from another environment must be ignored"
        );

        // The matching environment loads normally.
        record_plan_feedback(
            &table_key,
            &select,
            &ExecutionStrategy::OlapAggregation,
            100.0,
            100.0,
            ExecutedCostClass::Scan,
            1.0,
            10.0,
        );
        feedback_reset_table_for_tests(&table_key);
        let loaded = feedback_lookup_for_tests(&table_key, &select).unwrap();
        assert_eq!(loaded.samples, 1);

        // A pre-fingerprint (older schema version) sidecar is ignored too, so
        // the version bump retires every file written before Q2.
        let stale = PersistedPlanFeedback {
            version: FEEDBACK_SCHEMA_VERSION - 1,
            fingerprint: FEEDBACK_ENVIRONMENT.clone(),
            entries: vec![(key, entry(5))],
        };
        std::fs::write(&sidecar, bincode::serialize(&stale).unwrap()).unwrap();
        feedback_reset_table_for_tests(&table_key);
        assert!(
            feedback_lookup_for_tests(&table_key, &select).is_none(),
            "an older feedback schema version must be ignored"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
