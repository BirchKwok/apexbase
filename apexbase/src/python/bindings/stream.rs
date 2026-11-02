//! Opt-in streaming result handle (V5 S12 / W3-5).
//!
//! `execute_stream()` returns an iterator of Arrow IPC-encoded batches instead
//! of one materialized result. A background thread drives the existing
//! `Session::execute_streaming` row-group stream and sends one encoded batch per
//! channel message, so callers only hold one batch at a time. Query shapes
//! outside the streaming gate fall back to the regular materialized execution
//! and are delivered as a single batch (ORDER BY / GROUP BY may materialize
//! internally, bounded by the spill operators).
//!
//! Dropping the iterator drops the channel: the producer observes the closed
//! sink at its next batch boundary and stops early.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;

use arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

enum StreamMessage {
    Batch(Vec<u8>),
    Error(String),
    End,
}

/// Python-side handle for one streaming query.
#[pyclass]
pub struct ArrowBatchStream {
    receiver: Mutex<Option<Receiver<StreamMessage>>>,
    finished: AtomicBool,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for ArrowBatchStream {
    fn drop(&mut self) {
        // Dropping the receiver stops the producer at its next batch boundary.
        if let Ok(mut guard) = self.receiver.lock() {
            *guard = None;
        }
        self.finished.store(true, Ordering::Release);
        // Detach: a slow scan may still be finishing its current batch, and
        // Python's iterator finalization must not block on it.
        self.handle.take();
    }
}

#[pymethods]
impl ArrowBatchStream {
    /// Next Arrow IPC stream payload, or `None` when the result is exhausted.
    fn next_batch(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        if self.finished.load(Ordering::Acquire) {
            return Ok(None);
        }
        let receiver = {
            let mut guard = self
                .receiver
                .lock()
                .map_err(|_| PyRuntimeError::new_err("stream handle poisoned"))?;
            guard.take()
        };
        let Some(receiver) = receiver else {
            return Ok(None);
        };
        // Block without holding the GIL; the producer runs the query. The
        // receiver is moved into the closure (it is Send, not Sync) and handed
        // back so the next call can keep polling.
        let (receiver, message) =
            py.allow_threads(move || {
                let message = receiver.recv();
                (receiver, message)
            });
        let terminal = matches!(
            message,
            Ok(StreamMessage::End) | Ok(StreamMessage::Error(_)) | Err(_)
        );
        if !terminal {
            if let Ok(mut guard) = self.receiver.lock() {
                *guard = Some(receiver);
            }
        } else {
            self.finished.store(true, Ordering::Release);
        }
        match message {
            Ok(StreamMessage::Batch(bytes)) => Ok(Some(PyBytes::new_bound(py, &bytes).into())),
            Ok(StreamMessage::End) => Ok(None),
            Ok(StreamMessage::Error(error)) => Err(PyRuntimeError::new_err(error)),
            // Producer gone without a terminator (e.g. it panicked).
            Err(_) => Ok(None),
        }
    }

    /// Stop consuming early; the producer stops at its next batch boundary.
    fn close(&mut self) {
        if let Ok(mut guard) = self.receiver.lock() {
            *guard = None;
        }
        self.finished.store(true, Ordering::Release);
    }
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut buffer = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, batch.schema().as_ref())
            .map_err(|error| format!("stream encode failed: {error}"))?;
        writer
            .write(batch)
            .map_err(|error| format!("stream encode failed: {error}"))?;
        writer
            .finish()
            .map_err(|error| format!("stream encode failed: {error}"))?;
    }
    Ok(buffer)
}

/// Start a streaming execution for `sql` and return its Python handle.
pub(crate) fn start_stream(
    sql: String,
    base_dir: std::path::PathBuf,
    table_path: std::path::PathBuf,
    root_dir: std::path::PathBuf,
    temp_dir: std::path::PathBuf,
) -> ArrowBatchStream {
    let (sender, receiver) = channel::<StreamMessage>();
    let handle = std::thread::spawn(move || {
        let session = crate::Session::new(&base_dir, &table_path)
            .with_root_dir(&root_dir)
            .with_temp_dir(&temp_dir);
        let mut emit = |batch: RecordBatch| -> bool {
            match encode_batch(&batch) {
                Ok(bytes) => sender.send(StreamMessage::Batch(bytes)).is_ok(),
                Err(error) => {
                    let _ = sender.send(StreamMessage::Error(error));
                    false
                }
            }
        };
        match session.execute_streaming(&sql, &mut emit) {
            Ok(Some(_rows)) => {
                let _ = sender.send(StreamMessage::End);
            }
            Ok(None) => {
                // Outside the streaming gate: materialize once and deliver a
                // single batch (ORDER BY / GROUP BY stay internally bounded by
                // the spill operators).
                match session.execute(&sql) {
                    Ok(result) => match result.to_record_batch() {
                        Ok(batch) => {
                            if let Ok(bytes) = encode_batch(&batch) {
                                let _ = sender.send(StreamMessage::Batch(bytes));
                            }
                            let _ = sender.send(StreamMessage::End);
                        }
                        Err(error) => {
                            let _ = sender.send(StreamMessage::Error(error.to_string()));
                        }
                    },
                    Err(error) => {
                        let _ = sender.send(StreamMessage::Error(error.to_string()));
                    }
                }
            }
            Err(error) => {
                let _ = sender.send(StreamMessage::Error(error.to_string()));
            }
        }
    });
    ArrowBatchStream {
        receiver: Mutex::new(Some(receiver)),
        finished: AtomicBool::new(false),
        handle: Some(handle),
    }
}

/// Unused sender type helper kept for clarity of the channel contract.
#[allow(dead_code)]
fn channel_end(sender: Sender<StreamMessage>) {
    let _ = sender.send(StreamMessage::End);
}

#[pymethods]
impl super::ApexStorageImpl {
    /// Start an opt-in streaming execution; returns an iterator of Arrow IPC
    /// batch payloads consumed by ``ApexClient.execute_stream``.
    fn _stream_batches(&self, sql: &str) -> PyResult<ArrowBatchStream> {
        let base_dir = self.current_base_dir();
        let table_path = self
            .get_current_table_path()
            .unwrap_or_else(|_| base_dir.clone());
        Ok(start_stream(
            sql.to_string(),
            base_dir,
            table_path,
            self.root_dir.clone(),
            self.temp_dir.clone(),
        ))
    }
}
