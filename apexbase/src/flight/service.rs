//! Arrow Flight gRPC service for ApexBase
//!
//! Protocol:
//!   do_get(Ticket { ticket: sql_bytes })   → stream RecordBatch as Arrow IPC (SELECT)
//!   do_action(Action { type: "sql",        → execute DML/DDL, return affected rows JSON
//!                       body: sql_bytes })
//!   list_actions()                         → describes available actions
//!   get_flight_info(FlightDescriptor{cmd}) → returns schema + ticket for a query

use std::path::PathBuf;

use arrow_flight::{
    encode::FlightDataEncoderBuilder, flight_service_server::FlightService, Action, ActionType,
    Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

// ── helpers ──────────────────────────────────────────────────────────────────

fn apex_err(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

fn invalid(msg: impl Into<String>) -> Status {
    Status::invalid_argument(msg.into())
}

/// Execute SQL through the shared session façade. Runs synchronously (for spawn_blocking).
fn execute_sql(sql: &str, base_dir: &PathBuf) -> Result<arrow::record_batch::RecordBatch, Status> {
    let default_table_path = base_dir.join("apexbase.apex");
    let result = crate::Session::new(base_dir, &default_table_path)
        .with_root_dir(base_dir)
        .execute(sql);
    result
        .map_err(apex_err)?
        .to_record_batch()
        .map_err(apex_err)
}

/// Encode a RecordBatch as a stream of Arrow IPC FlightData messages.
fn batch_to_flight_data(
    batch: arrow::record_batch::RecordBatch,
) -> BoxStream<'static, Result<FlightData, Status>> {
    let batch_stream = futures::stream::once(futures::future::ready(Ok(batch)));
    FlightDataEncoderBuilder::new()
        .build(batch_stream)
        .map_err(|e| Status::internal(e.to_string()))
        .boxed()
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

// ── service ───────────────────────────────────────────────────────────────────

pub struct ApexFlightService {
    base_dir: PathBuf,
}

impl ApexFlightService {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
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

        let base_dir = self.base_dir.clone();
        let batch = tokio::task::spawn_blocking(move || execute_sql(&sql, &base_dir))
            .await
            .map_err(apex_err)??;

        let schema_bytes = schema_ipc_bytes(batch.schema_ref())?;
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
            total_records: batch.num_rows() as i64,
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

        let base_dir = self.base_dir.clone();
        let batch = tokio::task::spawn_blocking(move || execute_sql(&sql, &base_dir))
            .await
            .map_err(apex_err)??;

        let schema_bytes = schema_ipc_bytes(batch.schema_ref())?;
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

        let base_dir = self.base_dir.clone();
        let batch = tokio::task::spawn_blocking(move || execute_sql(&sql, &base_dir))
            .await
            .map_err(apex_err)??;

        Ok(Response::new(batch_to_flight_data(batch)))
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
