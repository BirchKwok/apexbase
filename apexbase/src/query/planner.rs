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

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::data::Value;
use crate::query::sql_parser::BinaryOperator;
use crate::query::{SelectColumn, SelectStatement, SqlExpr, SqlStatement};
use crate::storage::index::IndexManager;

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

/// Global stats cache: table_path → (TableStats, observed table epoch)
static STATS_CACHE: Lazy<RwLock<HashMap<String, (TableStats, u64)>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

const STATS_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone)]
struct PlanFeedback {
    strategy: ExecutionStrategy,
    estimated_rows: f64,
    actual_rows: f64,
    samples: u64,
}

static PLAN_FEEDBACK: Lazy<RwLock<HashMap<u64, PlanFeedback>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Store ANALYZE results into the stats cache
pub fn store_table_stats(table_key: &str, mut stats: TableStats) {
    stats.schema_version = STATS_SCHEMA_VERSION;
    stats.schema_generation = 0;
    stats.data_generation = 0;
    if let Ok(data) = bincode::serialize(&stats) {
        let _ = std::fs::write(stats_sidecar_path(table_key), data);
    }
    let epoch = crate::storage::epoch::current(std::path::Path::new(table_key));
    STATS_CACHE
        .write()
        .insert(table_key.to_string(), (stats, epoch));
}

/// Retrieve cached stats for a table
pub fn get_table_stats(table_key: &str) -> Option<TableStats> {
    let epoch = crate::storage::epoch::current(std::path::Path::new(table_key));
    if let Some((stats, observed_epoch)) = STATS_CACHE.read().get(table_key).cloned() {
        if observed_epoch == epoch {
            return stats_are_fresh(table_key, &stats).then_some(stats);
        }
        STATS_CACHE.write().remove(table_key);
    }

    let sidecar = stats_sidecar_path(table_key);
    let data = std::fs::read(sidecar).ok()?;
    let stats: TableStats = bincode::deserialize(&data).ok()?;
    if !stats_are_fresh(table_key, &stats) {
        return None;
    }
    STATS_CACHE
        .write()
        .insert(table_key.to_string(), (stats.clone(), epoch));
    Some(stats)
}

/// Invalidate stats for a table (e.g., after DML)
pub fn invalidate_table_stats(table_key: &str) {
    STATS_CACHE.write().remove(table_key);
}

/// Invalidate statistics after a schema-changing DDL operation.
pub fn invalidate_table_schema_stats(table_key: &str) {
    STATS_CACHE.write().remove(table_key);
}

fn feedback_key(table_key: &str, select: &SelectStatement) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    table_key.hash(&mut hasher);
    format!("{:?}", select).hash(&mut hasher);
    hasher.finish()
}

/// Record runtime cardinality feedback for EXPLAIN ANALYZE and future
/// executions of the same normalized AST shape.
pub fn record_plan_feedback(
    table_key: &str,
    select: &SelectStatement,
    strategy: &ExecutionStrategy,
    estimated_rows: f64,
    actual_rows: f64,
) {
    let key = feedback_key(table_key, select);
    let mut cache = PLAN_FEEDBACK.write();
    let entry = cache.entry(key).or_insert_with(|| PlanFeedback {
        strategy: strategy.clone(),
        estimated_rows,
        actual_rows,
        samples: 0,
    });
    entry.strategy = strategy.clone();
    entry.estimated_rows = (entry.estimated_rows * entry.samples as f64 + estimated_rows)
        / (entry.samples as f64 + 1.0);
    entry.actual_rows =
        (entry.actual_rows * entry.samples as f64 + actual_rows) / (entry.samples as f64 + 1.0);
    entry.samples = entry.samples.saturating_add(1);
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
}

/// The complete decision handed to the executor and EXPLAIN.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub strategy: ExecutionStrategy,
    pub cost: PlanCost,
    pub candidates: Vec<PlanCandidate>,
    pub stats_available: bool,
    pub feedback_applied: bool,
    /// Time spent constructing and costing candidates.
    pub planning_time_micros: u64,
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
            if let Some(feedback) = PLAN_FEEDBACK.read().get(&feedback_key(table_key, select)) {
                if feedback.samples > 0 && feedback.estimated_rows > 0.0 {
                    let correction =
                        (feedback.actual_rows / feedback.estimated_rows).clamp(0.25, 4.0);
                    for candidate in &mut plan.candidates {
                        if candidate.strategy == feedback.strategy {
                            candidate.cost.total *= correction;
                        }
                    }
                    if let Some(chosen) = plan.candidates.iter().min_by(|left, right| {
                        left.cost
                            .total
                            .partial_cmp(&right.cost.total)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    }) {
                        plan.strategy = chosen.strategy.clone();
                        plan.cost = chosen.cost.clone();
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
        }];

        if let (Some(idx_mgr), Some(_where_expr)) = (index_manager, &select.where_clause) {
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
        QueryPlan {
            strategy: chosen.strategy,
            cost: chosen.cost,
            candidates,
            stats_available: stats.is_some(),
            feedback_applied: false,
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
}
