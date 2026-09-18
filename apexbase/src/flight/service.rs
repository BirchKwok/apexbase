//! Arrow Flight gRPC service for ApexBase
//!
//! Protocol:
//!   do_get(Ticket { ticket: sql_bytes })   → stream RecordBatch as Arrow IPC (SELECT)
//!   do_action(Action { type: "sql",        → execute DML/DDL, return affected rows JSON
//!                       body: sql_bytes })
//!   list_actions()                         → describes available actions
//!   get_flight_info(FlightDescriptor{cmd}) → returns schema + ticket for a query
//!
//! `do_get` streams simple single-table SELECTs as the executor produces
//! row-group batches (S3): the blocking producer runs inside a
//! `spawn_blocking` task and feeds a small bounded channel, so server memory
//! is bounded by the row group plus the channel, a slow consumer applies
//! backpressure, and a disconnect drops the receiver so the producer stops at
//! the next batch boundary. Shapes outside the streaming gate (aggregation,
//! joins, sort, expressions, deltas) fall back to the materialized path and
//! are delivered in bounded row chunks. Schema requests never materialize the
//! result: streamable shapes take the schema from the first row-group batch,
//! other shapes execute once and the IPC schema is cached per SQL.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use arrow_flight::{
    encode::FlightDataEncoderBuilder, flight_service_server::FlightService, Action, ActionType,
    Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};

use tonic::{Request, Response, Status, Streaming};

// ── tunables ─────────────────────────────────────────────────────────────────

/// In-flight batches between the blocking producer and the gRPC stream. Small
/// on purpose: it bounds server-side delivery buffering and turns a slow
/// consumer into backpressure instead of unbounded queueing.
const STREAM_CHANNEL_CAPACITY: usize = 2;
/// Row cap for the materialized fallback: the result is one batch in memory,
/// but it is sliced before encoding so no single Flight message is huge.
const DELIVERY_CHUNK_ROWS: usize = 65_536;
/// Bounded per-SQL IPC schema cache (the schema is immutable for a SQL text).
const SCHEMA_CACHE_CAP: usize = 256;

// ── helpers ──────────────────────────────────────────────────────────────────

fn apex_err(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

fn invalid(msg: impl Into<String>) -> Status {
    Status::invalid_argument(msg.into())
}

/// Execute SQL through the shared session façade. Runs synchronously (for
/// spawn_blocking).
fn execute_sql(sql: &str, base_dir: &PathBuf) -> Result<arrow::record_batch::RecordBatch, Status> {
    let default_table_path = base_dir.join("apexbase.apex");
    crate::Session::new(base_dir, &default_table_path)
        .with_root_dir(base_dir)
        .execute(sql)
        .map_err(apex_err)?
        .to_record_batch()
        .map_err(apex_err)
}

/// Derive a result schema without materializing the result. Streamable shapes
/// stop at the first row-group batch; everything else executes once (the IPC
/// schema is then cached by the caller).
fn derive_schema(
    sql: &str,
    base_dir: &PathBuf,
) -> Result<arrow::datatypes::SchemaRef, Status> {
    let default_table_path = base_dir.join("apexbase.apex");
    let session = crate::Session::new(base_dir, &default_table_path).with_root_dir(base_dir);
    let mut schema: Option<arrow::datatypes::SchemaRef> = None;
    let streamed = session
        .execute_streaming(sql, &mut |batch| {
            schema = Some(batch.schema());
            false
        })
        .map_err(apex_err)?;
    if streamed.is_some() {
        if let Some(schema) = schema {
            return Ok(schema);
        }
    }
    let batch = session
        .execute(sql)
        .map_err(apex_err)?
        .to_record_batch()
        .map_err(apex_err)?;
    Ok(batch.schema())
}

/// Encode an Arrow Schema as IPC bytes (for FlightInfo / SchemaResult).
fn schema_ipc_bytes(schema: &arrow::datatypes::Schema) -> Result<bytes::Bytes, Status> {
    let ipc_opts = arrow::ipc::writer::IpcWriteOptions::default();
    let data_gen = arrow::ipc::writer::IpcDataGenerator::default();
    let mut dict_tracker = arrow::ipc::writer::DictionaryTracker::new(false);
    let encoded =
        data_gen.schema_to_bytes_with_dictionary_tracker(schema, &mut dict_tracker, &ipc_opts);
    Ok(encoded.ipc_message.into())
}

/// Map an encoder error back onto the gRPC status, preserving a status that
/// came from the producer.
fn flight_error_to_status(error: arrow_flight::error::FlightError) -> Status {
    match error {
        arrow_flight::error::FlightError::Tonic(status) => *status,
        other => Status::internal(other.to_string()),
    }
}

/// Encode a stream of RecordBatches as Arrow IPC FlightData messages.
fn encode_batches<S>(stream: S) -> BoxStream<'static, Result<FlightData, Status>>
where
    S: Stream<Item = Result<arrow::record_batch::RecordBatch, arrow_flight::error::FlightError>>
        + Send
        + 'static,
{
    FlightDataEncoderBuilder::new()
        .build(stream)
        .map_err(flight_error_to_status)
        .boxed()
}

/// Slice one materialized batch into bounded delivery chunks.
fn chunk_batch(batch: &arrow::record_batch::RecordBatch) -> Vec<arrow::record_batch::RecordBatch> {
    if batch.num_rows() <= DELIVERY_CHUNK_ROWS {
        return vec![batch.clone()];
    }
    (0..batch.num_rows())
        .step_by(DELIVERY_CHUNK_ROWS)
        .map(|start| {
            batch.slice(
                start,
                DELIVERY_CHUNK_ROWS.min(batch.num_rows().saturating_sub(start)),
            )
        })
        .collect()
}

// ── service ───────────────────────────────────────────────────────────────────

pub struct ApexFlightService {
    base_dir: PathBuf,
    schema_cache: Mutex<HashMap<String, bytes::Bytes>>,
}

impl ApexFlightService {
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            base_dir,
            schema_cache: Mutex::new(HashMap::new()),
        }
    }

    /// IPC schema bytes for `sql`, cached per SQL text. Does not materialize
    /// the result for streamable shapes.
    async fn schema_for(&self, sql: &str) -> Result<bytes::Bytes, Status> {
        if let Some(bytes) = self.schema_cache.lock().unwrap().get(sql).cloned() {
            return Ok(bytes);
        }
        let base_dir = self.base_dir.clone();
        let sql_owned = sql.to_string();
        let schema = tokio::task::spawn_blocking(move || derive_schema(&sql_owned, &base_dir))
            .await
            .map_err(apex_err)??;
        let bytes = schema_ipc_bytes(&schema)?;
        let mut cache = self.schema_cache.lock().unwrap();
        if cache.len() >= SCHEMA_CACHE_CAP {
            cache.clear();
        }
        cache.insert(sql.to_string(), bytes.clone());
        Ok(bytes)
    }
}

#[tonic::async_trait]
impl FlightService for ApexFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    // ── handshake (no-auth passthrough) ──────────────────────────────────────
    async fn handshake(
        &self,
        _req: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Ok(Response::new(futures::stream::empty().boxed()))
    }

    // ── list_flights ─────────────────────────────────────────────────────────
    async fn list_flights(
        &self,
        _req: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(futures::stream::empty().boxed()))
    }

    // ── get_flight_info: returns schema + ticket for a SQL query ─────────────
    async fn get_flight_info(
        &self,
        req: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = req.into_inner();
        let sql = std::str::from_utf8(&descriptor.cmd)
            .map_err(|_| invalid("FlightDescriptor.cmd must be valid UTF-8 SQL"))?
            .to_string();

        if sql.trim().is_empty() {
            return Err(invalid("Empty SQL"));
        }

        let schema_bytes = self.schema_for(&sql).await?;
        let ticket = Ticket {
            ticket: descriptor.cmd.clone().into(),
        };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
            expiration_time: None,
            app_metadata: Default::default(),
        };

        Ok(Response::new(FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(descriptor),
            endpoint: vec![endpoint],
            // The row count is no longer computed here: reporting it would
            // require the full execution this metadata path avoids. Clients
            // get the exact count from `do_get`.
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: Default::default(),
        }))
    }

    // ── poll_flight_info ─────────────────────────────────────────────────────
    async fn poll_flight_info(
        &self,
        req: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        let info = self.get_flight_info(req).await?.into_inner();
        Ok(Response::new(PollInfo {
            info: Some(info),
            flight_descriptor: None,
            progress: Some(1.0),
            expiration_time: None,
        }))
    }

    // ── get_schema ───────────────────────────────────────────────────────────
    async fn get_schema(
        &self,
        req: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = req.into_inner();
        let sql = std::str::from_utf8(&descriptor.cmd)
            .map_err(|_| invalid("cmd must be valid UTF-8 SQL"))?
            .to_string();
        let schema_bytes = self.schema_for(&sql).await?;
        Ok(Response::new(SchemaResult {
            schema: schema_bytes,
        }))
    }

    // ── do_get: execute SQL, stream results as Arrow IPC ─────────────────────
    async fn do_get(&self, req: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = req.into_inner();
        let sql = std::str::from_utf8(&ticket.ticket)
            .map_err(|_| invalid("Ticket must be valid UTF-8 SQL"))?
            .to_string();

        if sql.trim().is_empty() {
            return Err(invalid("Empty SQL in ticket"));
        }

        log::debug!("Flight do_get: {}", sql);

        let (sender, receiver) = tokio::sync::mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let base_dir = self.base_dir.clone();
        // The blocking producer owns the read view and stops as soon as the
        // receiver is gone (disconnect) or the consumer keeps up.
        tokio::task::spawn_blocking(move || {
            let default_table_path = base_dir.join("apexbase.apex");
            let session = crate::Session::new(&base_dir, &default_table_path).with_root_dir(&base_dir);
            let streamed = session.execute_streaming(&sql, &mut |batch| {
                sender.blocking_send(Ok(batch)).is_ok()
            });
            match streamed {
                Ok(Some(_)) => {}
                Ok(None) => match session
                    .execute(&sql)
                    .map_err(apex_err)
                    .and_then(|result| result.to_record_batch().map_err(apex_err))
                {
                    Ok(batch) => {
                        for chunk in chunk_batch(&batch) {
                            if sender.blocking_send(Ok(chunk)).is_err() {
                                break;
                            }
                        }
                    }
                    Err(status) => {
                        let _ = sender.blocking_send(Err(status));
                    }
                },
                Err(error) => {
                    let _ = sender.blocking_send(Err(apex_err(error)));
                }
            }
        });

        let batch_stream = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver
                .recv()
                .await
                .map(|item| (item.map_err(arrow_flight::error::FlightError::from), receiver))
        });
        Ok(Response::new(encode_batches(batch_stream)))
    }

    // ── do_put: not yet implemented ───────────────────────────────────────────
    async fn do_put(
        &self,
        _req: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not yet implemented"))
    }

    // ── do_exchange ──────────────────────────────────────────────────────────
    async fn do_exchange(
        &self,
        _req: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    // ── do_action: execute DML/DDL, return affected rows JSON ────────────────
    async fn do_action(
        &self,
        req: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = req.into_inner();
        let sql = std::str::from_utf8(&action.body)
            .map_err(|_| invalid("Action body must be valid UTF-8 SQL"))?
            .to_string();

        log::debug!("Flight do_action({}): {}", action.r#type, sql);

        let base_dir = self.base_dir.clone();
        let batch = tokio::task::spawn_blocking(move || execute_sql(&sql, &base_dir))
            .await
            .map_err(apex_err)??;

        let affected = batch.num_rows() as i64;
        let body: bytes::Bytes = format!("{{\"affected_rows\":{}}}", affected).into();
        let result = arrow_flight::Result { body };
        let stream = futures::stream::once(futures::future::ready(Ok(result))).boxed();
        Ok(Response::new(stream))
    }

    // ── list_actions ─────────────────────────────────────────────────────────
    async fn list_actions(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![ActionType {
            r#type: "sql".to_string(),
            description: "Execute DML/DDL. Body = UTF-8 SQL. Returns {affected_rows:N}."
                .to_string(),
        }];
        let stream = futures::stream::iter(actions.into_iter().map(Ok)).boxed();
        Ok(Response::new(stream))
    }
}
