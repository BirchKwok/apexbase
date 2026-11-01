//! V5 S3: list-column presentation and query-surface guards.
//!
//! List columns are stored as binary row payloads; this module re-types the
//! projected output columns as Arrow `ListArray`s at the result boundary and
//! rejects the query shapes V5 S3 keeps out of scope (ORDER BY / GROUP BY).

use crate::query::{ApexResult, SelectColumn, SelectStatement};
use arrow::array::{Array, ArrayRef, BinaryArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::io;
use std::sync::Arc;

fn err_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Re-type list output columns as Arrow `ListArray`s (V5 S3 ③).
pub(in crate::query::executor) fn present_list_outputs(
    result: ApexResult,
    outputs: &[(String, String)],
) -> io::Result<ApexResult> {
    if outputs.is_empty() {
        return Ok(result);
    }
    let ApexResult::Data(batch) = result else {
        return Ok(result);
    };
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| (**field).clone())
        .collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let mut changed = false;
    for (output, _source) in outputs {
        let Ok(index) = batch.schema().index_of(output) else {
            continue;
        };
        let data = batch.column(index);
        let Some(binary) = data.as_any().downcast_ref::<BinaryArray>() else {
            continue;
        };
        let offsets: Vec<u64> = (0..binary.len() + 1)
            .map(|row| binary.value_offsets()[row] as u64)
            .collect();
        let values = binary.values();
        let (arrow_dt, array) = crate::storage::on_demand::list_arrow_from_payloads(
            &offsets,
            values,
        )?;
        fields[index] = Field::new(output, arrow_dt, true);
        columns[index] = array;
        changed = true;
    }
    if !changed {
        return Ok(ApexResult::Data(batch));
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| err_data(error.to_string()))?;
    Ok(ApexResult::Data(batch))
}

/// Reject ORDER BY / GROUP BY / DISTINCT ON over list columns (V5 S3 ⑤).
pub(in crate::query::executor) fn reject_list_query_shapes(
    stmt: &SelectStatement,
    backend: &crate::storage::TableStorageBackend,
) -> io::Result<()> {
    let is_list = |name: &str| -> bool {
        let clean = name.trim_matches('"');
        let bare = clean.rsplit('.').next().unwrap_or(clean);
        backend
            .get_column_type(bare)
            .map(|dtype| dtype == crate::data::DataType::Array)
            .unwrap_or(false)
    };
    let is_extension = |name: &str| -> bool {
        let clean = name.trim_matches('"');
        let bare = clean.rsplit('.').next().unwrap_or(clean);
        matches!(
            backend.column_storage_type(bare),
            Some(crate::storage::on_demand::ColumnType::Extension)
        )
    };
    for order in &stmt.order_by {
        if is_list(&order.column) || is_extension(&order.column) {
            return Err(err_data(format!(
                "ORDER BY over list column '{}' is not supported (V5 S3 keeps list columns to round-trip reads)",
                order.column
            )));
        }
    }
    for group in &stmt.group_by {
        if is_list(group) || is_extension(group) {
            return Err(err_data(format!(
                "GROUP BY over list column '{}' is not supported (V5 S3 keeps list columns to round-trip reads)",
                group
            )));
        }
    }
    for column in &stmt.columns {
        if let SelectColumn::Aggregate {
            column: Some(name), ..
        } = column
        {
            if is_list(name) || is_extension(name) {
                return Err(err_data(format!(
                    "aggregates over list column '{}' are not supported (V5 S3 keeps list columns to round-trip reads)",
                    name
                )));
            }
        }
    }
    for name in &stmt.distinct_on.clone().unwrap_or_default() {
        if is_list(name) || is_extension(name) {
            return Err(err_data(format!(
                "DISTINCT ON over list column '{}' is not supported (V5 S3)",
                name
            )));
        }
    }
    Ok(())
}
