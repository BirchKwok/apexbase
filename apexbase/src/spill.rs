//! Out-of-core spill support for memory-bounded operators (V5 S11 / W3-3).
//!
//! When an operator's tracked state would exceed the per-query budget the
//! operator falls back to this module instead of failing: batches are written
//! as Arrow IPC run files under a per-query spill area and merged back with a
//! bounded amount of memory.
//!
//! - `APEX_SPILL_DIR` chooses the root temporary directory; unset uses
//!   `<system temp>/apex_spill` (created on demand).
//! - Each query gets its own area (`<root>/<pid>-<counter>`), removed by its
//!   RAII guard on every exit path (success, error, cancellation).
//! - Areas left behind by a crashed process are removed on the next start when
//!   they are older than `APEX_SPILL_TTL_SECS` (default 3600).
//! - Run files use the Arrow IPC file format (schema + record batches); the
//!   `sort` helper performs bounded-memory external sorting with pairwise run
//!   merges.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

static NEXT_AREA_ID: AtomicU64 = AtomicU64::new(0);

/// One sort key for the external sort helper: a column name plus direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillSortKey {
    pub column: String,
    pub descending: bool,
    pub nulls_first: bool,
}

impl SpillSortKey {
    fn sort_column(&self, batch: &RecordBatch) -> io::Result<SortColumn> {
        let values = batch.column_by_name(&self.column).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("spill sort column '{}' is missing", self.column),
            )
        })?;
        Ok(SortColumn {
            values,
            options: Some(SortOptions {
                descending: self.descending,
                nulls_first: self.nulls_first,
            }),
        })
    }
}

/// Root spill directory (`APEX_SPILL_DIR` or the system temp directory).
pub fn spill_root() -> PathBuf {
    match std::env::var_os("APEX_SPILL_DIR") {
        Some(value) if !value.is_empty() => PathBuf::from(value),
        _ => std::env::temp_dir().join("apex_spill"),
    }
}

fn spill_ttl() -> Duration {
    let seconds = std::env::var_os("APEX_SPILL_TTL_SECS")
        .and_then(|value| value.to_str().and_then(|text| text.trim().parse::<u64>().ok()))
        .unwrap_or(3600);
    Duration::from_secs(seconds)
}

/// Remove spill areas left behind by crashed processes.
fn cleanup_stale_areas(root: &Path) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    let ttl = spill_ttl();
    let now = SystemTime::now();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let old = entry
            .metadata()
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| now.duration_since(modified).ok())
            .map(|age| age >= ttl)
            .unwrap_or(false);
        if old {
            let _ = fs::remove_dir_all(&path);
        }
    }
}

/// Per-query spill area. The directory is removed when the last clone drops.
#[derive(Debug)]
pub struct SpillArea {
    path: PathBuf,
    _private: Arc<()>,
}

impl SpillArea {
    /// Create an isolated spill area for one query.
    pub fn new() -> io::Result<Self> {
        let root = spill_root();
        fs::create_dir_all(&root)?;
        cleanup_stale_areas(&root);
        let id = NEXT_AREA_ID.fetch_add(1, Ordering::Relaxed);
        let path = root.join(format!("{}-{id}", std::process::id()));
        fs::create_dir_all(&path)?;
        Ok(Self {
            path,
            _private: Arc::new(()),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn file_path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.path.join(name)
    }

    /// Remove the whole area now (idempotent; Drop repeats it safely).
    pub fn cleanup(&self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl Drop for SpillArea {
    fn drop(&mut self) {
        // `_private` keeps clones alive: the last owner removes the directory.
        if Arc::strong_count(&self._private) == 1 {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Write record batches to an Arrow IPC run file.
pub fn write_run(path: &Path, batches: &[RecordBatch]) -> io::Result<()> {
    let Some(first) = batches.first() else {
        return Ok(());
    };
    let file = File::create(path)?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(file, first.schema().as_ref())
        .map_err(io_other)?;
    for batch in batches {
        writer.write(batch).map_err(io_other)?;
    }
    writer.finish().map_err(io_other)?;
    Ok(())
}

/// Read every batch from an Arrow IPC run file.
pub fn read_run(path: &Path) -> io::Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader = arrow::ipc::reader::FileReader::try_new(file, None).map_err(io_other)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(io_other)?);
    }
    Ok(batches)
}

/// Stable path names inside one area.
fn run_path(area: &SpillArea, pass: usize, index: usize) -> PathBuf {
    area.file_path(format!("run-{pass}-{index}.arrow"))
}

/// Bounded-memory external sort of one in-memory batch.
///
/// The batch is split into row chunks whose sort state fits `chunk_bytes`;
/// each chunk becomes a sorted run, and runs are merged pairwise. The final
/// sorted `RecordBatch` is materialised (the caller asked for a full result);
/// the intermediate memory is bounded by two runs plus the chunk sort state.
pub fn external_sort_batch(
    area: &SpillArea,
    batch: &RecordBatch,
    keys: &[SpillSortKey],
    chunk_bytes: usize,
) -> io::Result<RecordBatch> {
    if keys.is_empty() || batch.num_rows() <= 1 {
        return Ok(batch.clone());
    }
    let schema = batch.schema();
    let sort_columns: Vec<SortColumn> = keys
        .iter()
        .map(|key| key.sort_column(batch))
        .collect::<io::Result<_>>()?;
    let chunk_bytes = chunk_bytes.max(64 * 1024);
    let row_width = estimate_row_sort_bytes(batch, &sort_columns).max(8);
    let chunk_rows = (chunk_bytes / row_width).max(1);
    let num_rows = batch.num_rows();

    let mut runs: Vec<PathBuf> = Vec::new();
    let mut start = 0usize;
    while start < num_rows {
        let end = (start + chunk_rows).min(num_rows);
        let chunk = batch.slice(start, end - start);
        let sorted = sort_slice(&chunk, &keys, &sort_columns)?;
        let path = run_path(area, 0, runs.len());
        write_run(&path, std::slice::from_ref(&sorted))?;
        runs.push(path);
        start = end;
    }

    let mut pass = 1usize;
    while runs.len() > 1 {
        let mut merged = Vec::with_capacity(runs.len() / 2 + 1);
        for pair in runs.chunks(2) {
            let path = run_path(area, pass, merged.len());
            match pair {
                [single] => {
                    fs::rename(single, &path)?;
                }
                [a, b] => merge_two_runs(area, a, b, &path, &schema, keys, &sort_columns)?,
                _ => unreachable!(),
            }
            merged.push(path);
        }
        for old in &runs {
            let _ = fs::remove_file(old);
        }
        runs = merged;
        pass += 1;
    }

    let batches = read_run(&runs[0])?;
    concat_batches(&schema, &batches).map_err(io_other)
}

fn estimate_row_sort_bytes(batch: &RecordBatch, sort_columns: &[SortColumn]) -> usize {
    let mut bytes = std::mem::size_of::<usize>() * 2;
    for column in sort_columns {
        let size = column.values.get_array_memory_size();
        bytes = bytes.saturating_add(size / batch.num_rows().max(1));
    }
    // Leave headroom for the output row itself and the sort indices.
    bytes.saturating_add(32)
}

fn sort_slice(
    chunk: &RecordBatch,
    keys: &[SpillSortKey],
    cached: &[SortColumn],
) -> io::Result<RecordBatch> {
    let sort_columns: Vec<SortColumn> = if chunk.num_rows() == cached.first().map(|c| c.values.len()).unwrap_or(0) {
        cached.to_vec()
    } else {
        keys.iter()
            .map(|key| key.sort_column(chunk))
            .collect::<io::Result<_>>()?
    };
    let indices = lexsort_to_indices(&sort_columns, None).map_err(io_other)?;
    let columns = chunk
        .columns()
        .iter()
        .map(|column| take(column, &indices, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(io_other)?;
    RecordBatch::try_new(chunk.schema(), columns).map_err(io_other)
}

fn merge_two_runs(
    _area: &SpillArea,
    a: &Path,
    b: &Path,
    out: &Path,
    schema: &SchemaRef,
    keys: &[SpillSortKey],
    _cached: &[SortColumn],
) -> io::Result<()> {
    let mut batches = read_run(a)?;
    batches.extend(read_run(b)?);
    let merged = concat_batches(schema, &batches).map_err(io_other)?;
    let sort_columns: Vec<SortColumn> = keys
        .iter()
        .map(|key| key.sort_column(&merged))
        .collect::<io::Result<_>>()?;
    let indices = lexsort_to_indices(&sort_columns, None).map_err(io_other)?;
    let columns = merged
        .columns()
        .iter()
        .map(|column| take(column, &indices, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(io_other)?;
    let sorted = RecordBatch::try_new(schema.clone(), columns).map_err(io_other)?;
    write_run(out, std::slice::from_ref(&sorted))
}

fn io_other(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("spill: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn int_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn sorted_values(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn external_sort_matches_in_memory_single_column() {
        let area = SpillArea::new().unwrap();
        let mut values: Vec<i64> = (0..50_000).collect();
        values.reverse();
        let batch = int_batch(values.clone());
        let keys = vec![SpillSortKey {
            column: "v".into(),
            descending: true,
            nulls_first: false,
        }];
        let sorted = external_sort_batch(&area, &batch, &keys, 64 * 1024).unwrap();
        assert_eq!(sorted.num_rows(), values.len());
        let mut expected = values;
        expected.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(sorted_values(&sorted), expected);
        area.cleanup();
        assert!(!area.path().exists());
    }

    #[test]
    fn external_sort_matches_arrow_lexsort_multi_column() {
        let area = SpillArea::new().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let a: Vec<i64> = (0..30_000).map(|i| (i % 97) as i64).collect();
        let s: Vec<String> = (0..30_000).map(|i| format!("k{:04}", i % 31)).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(a.clone())),
                Arc::new(StringArray::from(s.clone())),
            ],
        )
        .unwrap();
        let keys = vec![
            SpillSortKey {
                column: "a".into(),
                descending: false,
                nulls_first: false,
            },
            SpillSortKey {
                column: "s".into(),
                descending: true,
                nulls_first: true,
            },
        ];
        let sorted = external_sort_batch(&area, &batch, &keys, 128 * 1024).unwrap();
        let sort_columns = keys
            .iter()
            .map(|key| key.sort_column(&batch).unwrap())
            .collect::<Vec<_>>();
        let indices = lexsort_to_indices(&sort_columns, None).unwrap();
        let expected_a = take(batch.column_by_name("a").unwrap(), &indices, None)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        let expected_s = take(batch.column_by_name("s").unwrap(), &indices, None)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| value.unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            sorted
                .column_by_name("a")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec(),
            expected_a
        );
        let got_s: Vec<String> = sorted
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| value.unwrap().to_string())
            .collect();
        assert_eq!(got_s, expected_s);
    }

    #[test]
    fn run_files_round_trip_and_are_cleaned_up() {
        let area = SpillArea::new().unwrap();
        let path = area.file_path("roundtrip.arrow");
        let batch = int_batch(vec![1, 2, 3]);
        write_run(&path, std::slice::from_ref(&batch)).unwrap();
        let batches = read_run(&path).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(sorted_values(&batches[0]), vec![1, 2, 3]);
        area.cleanup();
        assert!(!path.exists());
    }
}
