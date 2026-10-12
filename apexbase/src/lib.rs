//! ApexBase Core Storage Engine
//!
//! A high-performance embedded database storage engine implemented in Rust.
//! Provides Python bindings via PyO3 for seamless integration.

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;

pub mod compute;
pub mod data;
pub mod database;
pub mod embedded;
#[cfg(feature = "flight")]
pub mod flight;
pub mod fts;
#[cfg(feature = "python")]
pub mod python;
pub mod query;
pub mod scaling;
#[cfg(feature = "server")]
pub mod server;
pub mod storage;
pub mod table;
pub mod txn;

// Re-export main types
pub use data::{DataType, Row, Value};
pub use database::{Database, Session};
pub use query::{ApexExecutor, ApexResult};
pub use storage::{ColumnType, ColumnValue, ColumnarStorage, FileSchema};
pub use table::TableCatalog;

// Re-export embedded API for Rust users
pub use embedded::{ApexDB, ResultSet, Table};

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// Python module entry point
#[cfg(feature = "python")]
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // On Windows, rayon's global thread pool threads survive past Python process exit.
    // ExitProcess forcibly terminates them, triggering a panic in Rust's thread lifecycle
    // code ("threads should not terminate unexpectedly"). Suppress this specific panic.
    #[cfg(target_os = "windows")]
    {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let msg = if let Some(s) = info.payload().downcast_ref::<&str>() {
                *s
            } else if let Some(s) = info.payload().downcast_ref::<String>() {
                s.as_str()
            } else {
                ""
            };
            if msg.contains("should not terminate unexpectedly") {
                return; // suppress Windows thread-shutdown panic
            }
            prev(info);
        }));
    }

    m.add_class::<python::ApexStorage>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Add scheduler functions
    m.add_function(pyo3::wrap_pyfunction!(init_query_scheduler, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(get_scheduler_status, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(execute_scheduled, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(execute_scheduled_batch, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(submit_scheduled, m)?)?;
    m.add_class::<ScheduledHandle>()?;

    #[cfg(feature = "server")]
    m.add_function(pyo3::wrap_pyfunction!(python::start_pg_server, m)?)?;
    #[cfg(feature = "flight")]
    m.add_function(pyo3::wrap_pyfunction!(python::start_flight_server, m)?)?;
    Ok(())
}

/// Initialize the query scheduler with specified number of threads and
/// an optional bound on queued queries (admission control).
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (num_threads = None, max_queue = None))]
fn init_query_scheduler(
    num_threads: Option<usize>,
    max_queue: Option<usize>,
) -> PyResult<()> {
    let threads = num_threads.unwrap_or(4);
    match max_queue {
        Some(capacity) => crate::query::scheduler::init_scheduler_with_capacity(threads, capacity),
        None => crate::query::scheduler::init_scheduler(threads),
    }
    Ok(())
}

/// Get scheduler status - returns (initialized: bool, active_count: int or -1)
#[cfg(feature = "python")]
#[pyfunction]
fn get_scheduler_status() -> PyResult<(bool, i32)> {
    let initialized = crate::query::scheduler::is_scheduler_initialized();
    let active = if initialized {
        crate::query::scheduler::get_active_count()
            .map(|c| c as i32)
            .unwrap_or(-1)
    } else {
        -1
    };
    Ok((initialized, active))
}

/// Execute a query through the scheduler (for parallel execution)
/// Returns a tuple of (success: bool, error_message: str)
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (sql, table_path, root_dir = None, temp_dir = None))]
fn execute_scheduled(
    sql: String,
    table_path: String,
    root_dir: Option<String>,
    temp_dir: Option<String>,
) -> PyResult<(bool, String)> {
    use crate::query::scheduler::{execute_through_scheduler, QueryContext};
    use std::path::PathBuf;

    let context = QueryContext {
        root_dir: root_dir.map(PathBuf::from),
        temp_dir: temp_dir.map(PathBuf::from),
    };
    let path = PathBuf::from(table_path);

    // Submit and wait for the result
    match execute_through_scheduler(sql, path, &context) {
        Some(query) => Ok(wait_scheduled_result(query)),
        None => Ok((
            false,
            "Scheduler not initialized or queue full".to_string(),
        )),
    }
}

/// Submit a query through the scheduler and return a cancellable handle.
#[cfg(feature = "python")]
#[pyclass]
pub struct ScheduledHandle {
    inner: Option<crate::query::scheduler::ScheduledQuery>,
}

#[cfg(feature = "python")]
#[pymethods]
impl ScheduledHandle {
    /// Request cancellation; the query observes the token at its next batch boundary.
    fn cancel(&self) {
        if let Some(query) = &self.inner {
            query.cancel();
        }
    }

    /// Block until the query finishes. Returns (success, error_message).
    fn wait(&mut self) -> PyResult<(bool, String)> {
        use crate::query::scheduler::QueryResult;

        let query = self
            .inner
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("already waited"))?;
        Ok(match query.wait() {
            Ok(QueryResult::Data(_batch)) => (true, String::new()),
            Ok(QueryResult::Error(e)) => (false, e),
            Ok(QueryResult::Done) => (true, String::new()),
            Err(e) => (false, e),
        })
    }
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (sql, table_path, root_dir = None, temp_dir = None))]
fn submit_scheduled(
    sql: String,
    table_path: String,
    root_dir: Option<String>,
    temp_dir: Option<String>,
) -> PyResult<ScheduledHandle> {
    use crate::query::scheduler::{execute_through_scheduler, QueryContext};
    use std::path::PathBuf;

    let context = QueryContext {
        root_dir: root_dir.map(PathBuf::from),
        temp_dir: temp_dir.map(PathBuf::from),
    };
    let path = PathBuf::from(table_path);

    match execute_through_scheduler(sql, path, &context) {
        Some(query) => Ok(ScheduledHandle { inner: Some(query) }),
        None => Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Scheduler not initialized or queue full",
        )),
    }
}

/// Execute multiple queries in parallel through the scheduler
/// Returns list of (success: bool, error_message: str)
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (sqls, table_path, root_dir = None, temp_dir = None))]
fn execute_scheduled_batch(
    sqls: Vec<String>,
    table_path: String,
    root_dir: Option<String>,
    temp_dir: Option<String>,
) -> PyResult<Vec<(bool, String)>> {
    use crate::query::scheduler::{execute_through_scheduler, QueryContext};
    use std::path::PathBuf;

    let context = QueryContext {
        root_dir: root_dir.map(PathBuf::from),
        temp_dir: temp_dir.map(PathBuf::from),
    };
    let path = PathBuf::from(table_path);

    // Submit all queries and collect handles
    let mut handles = Vec::new();
    for sql in sqls {
        match execute_through_scheduler(sql, path.clone(), &context) {
            Some(query) => handles.push(query),
            None => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "Scheduler not initialized or queue full",
                ))
            }
        }
    }

    // Wait for all results
    let mut results = Vec::new();
    for query in handles {
        results.push(wait_scheduled_result(query));
    }

    Ok(results)
}

#[cfg(feature = "python")]
fn wait_scheduled_result(
    query: crate::query::scheduler::ScheduledQuery,
) -> (bool, String) {
    use crate::query::scheduler::QueryResult;

    match query.wait() {
        Ok(QueryResult::Data(_batch)) => (true, String::new()),
        Ok(QueryResult::Error(e)) => (false, e),
        Ok(QueryResult::Done) => (true, String::new()),
        Err(e) => (false, e),
    }
}

/// Storage engine error type
#[derive(Debug, thiserror::Error)]
pub enum ApexError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Table already exists: {0}")]
    TableExists(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Column already exists: {0}")]
    ColumnExists(String),

    #[error("Row not found: {0}")]
    RowNotFound(u64),

    #[error("Invalid data type: {0}")]
    InvalidDataType(String),

    #[error("Query parse error: {0}")]
    QueryParseError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Checksum mismatch")]
    ChecksumMismatch,

    #[error("Invalid file format")]
    InvalidFileFormat,

    #[error("Version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u32, actual: u32 },

    #[error("Cannot drop default table")]
    CannotDropDefaultTable,

    #[error("Cannot modify _id column")]
    CannotModifyIdColumn,
}

pub type Result<T> = std::result::Result<T, ApexError>;
