//! Flight service tests (S3): streaming delivery, fallback and schema paths.
//! Compiled only with the `flight` feature.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::decode::FlightDataDecoder;
use arrow_flight::{FlightDescriptor, Ticket};
use futures::StreamExt;
use tempfile::tempdir;
use tonic::Request;

use super::ApexFlightService;

const ROWS: usize = 200_000;

fn fixture(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("stream_t.apex");
    let storage = crate::storage::OnDemandStorage::create_with_schema_and_durability(
        &path,
        crate::storage::DurabilityLevel::Fast,
        &[
            ("code".to_string(), crate::storage::ColumnType::Int64),
            ("amount".to_string(), crate::storage::ColumnType::Int64),
        ],
    )
    .unwrap();
    storage
        .insert_typed(
            HashMap::from([
                ("code".to_string(), (0..ROWS).map(|i| (i % 7) as i64).collect()),
                (
                    "amount".to_string(),
                    (0..ROWS).map(|i| (i % 500) as i64 - 250).collect(),
                ),
            ]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();
    path
}

async fn decode_ticket(
    service: &ApexFlightService,
    sql: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
    let response = service
        .do_get(Request::new(Ticket {
            ticket: sql.as_bytes().to_vec().into(),
        }))
        .await
        .unwrap();
    let data_stream = response
        .into_inner()
        .map(|item| item.map_err(FlightError::from));
    let mut decoder = FlightDataDecoder::new(data_stream);
    let mut batches = Vec::new();
    while let Some(item) = decoder.next().await {
        let decoded = item.unwrap();
        if let arrow_flight::decode::DecodedPayload::RecordBatch(batch) = decoded.payload {
            batches.push(batch);
        }
    }
    batches
}

fn column_i64(batches: &[arrow::record_batch::RecordBatch], name: &str) -> Vec<i64> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch.column_by_name(name).unwrap();
        let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
        values.extend(array.values().iter().copied());
    }
    values
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn do_get_streams_row_groups_in_order() {
    let dir = tempdir().unwrap();
    fixture(dir.path());
    let service = ApexFlightService::new(dir.path().to_path_buf());

    let batches = decode_ticket(&service, "SELECT code, amount FROM stream_t").await;
    assert!(
        batches.len() >= 2,
        "a multi-row-group table must stream more than one batch, got {}",
        batches.len()
    );
    assert_eq!(
        batches[0].schema().fields().len(),
        2,
        "projection must keep both selected columns"
    );
    let codes = column_i64(&batches, "code");
    let amounts = column_i64(&batches, "amount");
    assert_eq!(codes.len(), ROWS);
    assert_eq!(amounts.len(), ROWS);
    assert_eq!(codes[0], 0);
    assert_eq!(codes[ROWS - 1], ((ROWS - 1) % 7) as i64);
    assert_eq!(amounts[ROWS - 1], ((ROWS - 1) % 500) as i64 - 250);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn do_get_falls_back_for_non_streamable_shapes() {
    let dir = tempdir().unwrap();
    fixture(dir.path());
    let service = ApexFlightService::new(dir.path().to_path_buf());

    let batches = decode_ticket(&service, "SELECT COUNT(*) FROM stream_t").await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let count = column_i64(&batches, "COUNT(*)");
    assert_eq!(count, vec![ROWS as i64]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_requests_do_not_execute_the_result() {
    let dir = tempdir().unwrap();
    fixture(dir.path());
    let service = ApexFlightService::new(dir.path().to_path_buf());
    let sql = "SELECT code, amount FROM stream_t";

    let descriptor = FlightDescriptor {
        r#type: 0,
        cmd: sql.as_bytes().to_vec().into(),
        path: vec![],
    };
    let info = service
        .get_flight_info(Request::new(descriptor.clone()))
        .await
        .unwrap()
        .into_inner();
    // The metadata path reports no row count because computing it would
    // require the full execution this path avoids.
    assert_eq!(info.total_records, -1);
    assert!(!info.schema.is_empty());

    let schema_result = service
        .get_schema(Request::new(descriptor))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(schema_result.schema, info.schema);

    // The advertised schema is the schema of the actual streamed batches.
    let batches = decode_ticket(&service, sql).await;
    let decoded_schema = batches[0].schema();
    let expected = {
        let ipc_opts = arrow::ipc::writer::IpcWriteOptions::default();
        let data_gen = arrow::ipc::writer::IpcDataGenerator::default();
        let mut tracker = arrow::ipc::writer::DictionaryTracker::new(false);
        let encoded = data_gen.schema_to_bytes_with_dictionary_tracker(
            decoded_schema.as_ref(),
            &mut tracker,
            &ipc_opts,
        );
        bytes::Bytes::from(encoded.ipc_message)
    };
    assert_eq!(schema_result.schema, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_response_stream_stops_the_producer() {
    let dir = tempdir().unwrap();
    fixture(dir.path());
    let service = ApexFlightService::new(dir.path().to_path_buf());

    // Take one message and drop the rest: the blocking producer must observe
    // the closed channel and stop instead of materializing the full result.
    {
        let response = service
            .do_get(Request::new(Ticket {
                ticket: b"SELECT code, amount FROM stream_t".to_vec().into(),
            }))
            .await
            .unwrap();
        let mut stream = response.into_inner();
        let first = stream.next().await.unwrap();
        assert!(first.is_ok());
    }

    // The service is still usable afterwards.
    let batches = decode_ticket(&service, "SELECT COUNT(*) FROM stream_t").await;
    assert_eq!(batches[0].num_rows(), 1);
}
