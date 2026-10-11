//! PyO3 bridge for the PostgreSQL wire protocol server.
//!
//! Exposes `start_pg_server` as a Python function so `pip install apexbase`
//! users can launch the server without a separate Rust binary.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::PathBuf;

use crate::server::{self, ServerConfig};

/// Start the ApexBase PostgreSQL-compatible server (blocking).
///
/// This function blocks until the server is stopped (Ctrl-C).
///
/// Args:
///     data_dir (str): Directory containing ApexBase database files.
///     host (str): Host to bind to. Default "127.0.0.1".
///     port (int): Port to listen on. Default 5432.
///
/// Statements execute at most `APEX_SERVER_QUERY_CONCURRENCY` at a time
/// (default: twice the CPU count) on the blocking pool, so connections stay
/// responsive while scans run. A statement waits at most
/// `APEX_SERVER_QUERY_WAIT_MS` (default 30000) for a slot and is then rejected
/// with SQLSTATE 53300.
#[pyfunction]
#[pyo3(signature = (data_dir, host = "127.0.0.1".to_string(), port = 5432))]
pub fn start_pg_server(py: Python<'_>, data_dir: String, host: String, port: u16) -> PyResult<()> {
    let config = ServerConfig {
        data_dir: PathBuf::from(data_dir),
        host,
        port,
        query_concurrency: crate::server::default_query_concurrency(),
        query_wait: crate::server::default_query_wait(),
    };

    // Release the GIL so Python threads remain responsive, then block on the
    // tokio runtime running the server.
    py.allow_threads(|| {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e))
        })?;

        rt.block_on(async {
            server::start_server(config)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Server error: {}", e)))
        })
    })
}
