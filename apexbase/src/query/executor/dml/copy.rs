use super::*;

/// File size above which a full sequential scan switches from `mmap` to a
/// buffered `read`.
///
/// Measured on this machine, the per-line field-scan hot loop (what CSV count
/// and full parse actually run) crosses over around ~500MB: below it a warm
/// `mmap` is slightly faster (zero copy, no buffer allocation), above it a
/// sequential `read` wins decisively (2.7x at 9.3GB) because it avoids the
/// per-page fault overhead of a very large mapping.
const SEQUENTIAL_READ_THRESHOLD: u64 = 512 * 1024 * 1024;

/// Read an entire CSV file into memory with a single sequential pass.
///
/// The buffer is pre-sized from the file length so `read_to_end` performs no
/// reallocation. Only used for full scans of large files (see
/// [`SEQUENTIAL_READ_THRESHOLD`]); bounded reads and small files stay on a lazy
/// `mmap`, which is strictly better there.
fn read_file_sequential(path: &str) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(|e| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Cannot open CSV file '{}': {}", path, e),
        )
    })?;
    let len = file.metadata()?.len() as usize;
    let mut buf = Vec::with_capacity(len + 1);
    file.read_to_end(&mut buf).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("read error: {}", e))
    })?;
    Ok(buf)
}

/// Backing buffer for a CSV scan: a lazily-faulted `mmap` (small files and
/// bounded reads) or an eagerly-read `Vec` (large full sequential scans).
enum CsvBuffer {
    Mmap(memmap2::Mmap),
    Owned(Vec<u8>),
}

impl CsvBuffer {
    fn as_slice(&self) -> &[u8] {
        match self {
            CsvBuffer::Mmap(m) => m,
            CsvBuffer::Owned(v) => v,
        }
    }
}

/// Numeric state produced by a direct CSV aggregation scan.
///
/// The scanner keeps only these scalars per requested column.  It deliberately
/// mirrors the scalar aggregate types used by the Arrow execution path so a
/// query can return without materialising unrelated CSV columns.
#[derive(Clone, Copy, Debug)]
pub(in crate::query::executor) struct CsvNumericStats {
    pub(in crate::query::executor) count: i64,
    pub(in crate::query::executor) sum: f64,
    pub(in crate::query::executor) min: f64,
    pub(in crate::query::executor) max: f64,
    pub(in crate::query::executor) is_int: bool,
}

impl CsvNumericStats {
    fn new(is_int: bool) -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            is_int,
        }
    }

    #[inline]
    fn add(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

    #[inline]
    fn merge(&mut self, other: Self) {
        if other.count == 0 {
            return;
        }
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }
}

#[derive(Clone, Copy)]
enum CsvGroupTarget {
    Group,
    Numeric { output: usize, is_int: bool },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::query::executor) enum CsvGroupKeyType {
    Utf8,
    Int64,
}

#[derive(Eq, Hash, PartialEq)]
enum CsvGroupKey {
    Null,
    Utf8(Vec<u8>),
    Int64(i64),
}

#[derive(Clone, Copy)]
enum CsvDistinctKeyType {
    Utf8,
    Int64,
}

#[derive(Eq, Hash, PartialEq)]
enum CsvDistinctValue {
    Utf8(Vec<u8>),
    Int64(i64),
}

impl ApexExecutor {
    pub(in crate::query::executor) fn execute_copy_to_parquet(
        storage_path: &Path,
        table_name: &str,
        file_path: &str,
    ) -> io::Result<ApexResult> {
        crate::storage::table_catalog::ensure_table_file(
            storage_path,
            crate::storage::DurabilityLevel::Fast,
        )?;
        let storage = TableStorageBackend::open(storage_path)?;
        let batch = storage.read_columns_to_arrow(None, 0, None)?;
        let schema = batch.schema();

        let file = std::fs::File::create(file_path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Cannot create parquet file '{}': {}", file_path, e),
            )
        })?;

        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut writer =
            parquet::arrow::arrow_writer::ArrowWriter::try_new(file, schema.clone(), Some(props))
                .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Parquet writer error: {}", e))
            })?;

        writer.write(&batch).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Parquet write error: {}", e))
        })?;
        writer.close().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Parquet close error: {}", e))
        })?;

        Ok(ApexResult::Scalar(batch.num_rows() as i64))
    }

    pub(in crate::query::executor) fn execute_copy_export(
        storage_path: &Path,
        table_name: &str,
        file_path: &str,
        format: &str,
        options: &[(String, String)],
    ) -> io::Result<ApexResult> {
        use std::io::Write;

        if format.eq_ignore_ascii_case("PARQUET") {
            return Self::execute_copy_to_parquet(storage_path, table_name, file_path);
        }
        crate::storage::table_catalog::ensure_table_file(
            storage_path,
            crate::storage::DurabilityLevel::Fast,
        )?;

        let storage = TableStorageBackend::open(storage_path)?;
        let batch = storage.read_columns_to_arrow(None, 0, None)?;
        let schema = batch.schema();

        match format.to_uppercase().as_str() {
            "CSV" | "TSV" => {
                let delimiter = options
                    .iter()
                    .find(|(k, _)| k == "delimiter" || k == "delim" || k == "sep")
                    .and_then(|(_, v)| v.chars().next())
                    .unwrap_or(if format.eq_ignore_ascii_case("TSV") {
                        '\t'
                    } else {
                        ','
                    });
                let header = options
                    .iter()
                    .find(|(k, _)| k == "header")
                    .map(|(_, v)| !matches!(v.to_lowercase().as_str(), "false" | "0"))
                    .unwrap_or(true);
                let file = std::fs::File::create(file_path)?;
                let mut writer = std::io::BufWriter::new(file);
                if header {
                    let columns: Vec<String> = schema
                        .fields()
                        .iter()
                        .map(|field| field.name().clone())
                        .collect();
                    writeln!(writer, "{}", columns.join(&delimiter.to_string()))?;
                }
                for row in 0..batch.num_rows() {
                    let mut cells = Vec::with_capacity(batch.num_columns());
                    for col in 0..batch.num_columns() {
                        let value = Self::arrow_value_at_col(batch.column(col), row);
                        let mut cell = value.to_string();
                        if cell.contains(delimiter)
                            || cell.contains('"')
                            || cell.contains('\n')
                            || cell.contains('\r')
                        {
                            cell = format!("\"{}\"", cell.replace('"', "\"\""));
                        }
                        cells.push(cell);
                    }
                    writeln!(writer, "{}", cells.join(&delimiter.to_string()))?;
                }
                writer.flush()?;
                Ok(ApexResult::Scalar(batch.num_rows() as i64))
            }
            "JSON" | "NDJSON" | "JSONL" => {
                let file = std::fs::File::create(file_path)?;
                let mut writer = std::io::BufWriter::new(file);
                for row in 0..batch.num_rows() {
                    let mut obj = serde_json::Map::with_capacity(batch.num_columns());
                    for (col_idx, field) in schema.fields().iter().enumerate() {
                        let value = Self::arrow_value_at_col(batch.column(col_idx), row);
                        obj.insert(field.name().clone(), value.to_json_value());
                    }
                    writeln!(writer, "{}", serde_json::Value::Object(obj))?;
                }
                writer.flush()?;
                Ok(ApexResult::Scalar(batch.num_rows() as i64))
            }
            other => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Unsupported COPY TO format: {}", other),
            )),
        }
    }

    pub(in crate::query::executor) fn execute_copy_from_parquet(
        storage_path: &Path,
        table_name: &str,
        file_path: &str,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<ApexResult> {
        let _epoch_write = crate::storage::epoch::logical_write(storage_path);
        let file = std::fs::File::open(file_path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Cannot open parquet file '{}': {}", file_path, e),
            )
        })?;

        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Parquet reader error: {}", e))
            })?
            .build()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Parquet reader build error: {}", e),
                )
            })?;

        let mut total_rows = 0i64;
        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Parquet read error: {}", e))
            })?;
            let schema = batch.schema();
            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            // Convert RecordBatch rows to Value vectors for insert
            let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
            let mut values: Vec<Vec<Value>> = Vec::with_capacity(num_rows);
            for row_idx in 0..num_rows {
                let mut row: Vec<Value> = Vec::with_capacity(col_names.len());
                for col_idx in 0..col_names.len() {
                    let col = batch.column(col_idx);
                    row.push(Self::arrow_value_at_col(col, row_idx));
                }
                values.push(row);
            }

            // Ensure table exists — a registered lazy table is materialized
            // with its catalog schema; an unregistered target is created.
            if !storage_path.exists() {
                if crate::storage::table_catalog::file_exists_or_registered(storage_path)? {
                    crate::storage::table_catalog::materialize_table_backend(
                        storage_path,
                        crate::storage::DurabilityLevel::Fast,
                    )?;
                } else {
                    let mut col_defs = Vec::new();
                    for field in schema.fields() {
                        let type_str = match field.data_type() {
                            arrow::datatypes::DataType::Int64 => "INTEGER",
                            arrow::datatypes::DataType::Float64 => "REAL",
                            arrow::datatypes::DataType::Boolean => "BOOLEAN",
                            arrow::datatypes::DataType::UInt64 => "INTEGER",
                            _ => "TEXT",
                        };
                        col_defs.push(format!("{} {}", field.name(), type_str));
                    }
                    let create_sql = format!("CREATE TABLE {} ({})", table_name, col_defs.join(", "));
                    let create_stmt = SqlParser::parse(&create_sql).map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("Failed to parse CREATE TABLE: {}", e),
                        )
                    })?;
                    Self::execute_parsed_multi(create_stmt, base_dir, default_table_path)?;
                }
            }

            Self::execute_insert(storage_path, Some(&col_names), &values)?;
            total_rows += num_rows as i64;
        }

        Ok(ApexResult::Scalar(total_rows))
    }

    pub(crate) fn read_table_function(
        func: &str,
        file: &str,
        options: &[(String, String)],
        row_limit: Option<usize>,
    ) -> io::Result<RecordBatch> {
        match func.to_uppercase().as_str() {
            "READ_CSV" => Self::read_csv_to_batch(file, options, row_limit),
            "READ_JSON" => Self::read_json_to_batch(file, options),
            "READ_PARQUET" => Self::read_parquet_to_batch(file, options, row_limit),
            other => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Unknown table function: {}", other),
            )),
        }
    }

    pub(crate) fn read_direct_file(file: &str, row_limit: Option<usize>) -> io::Result<RecordBatch> {
        let lower = file.to_lowercase();
        if lower.ends_with(".csv.gz") || lower.ends_with(".csv.gzip") {
            return Self::read_csv_to_batch(file, &[], row_limit);
        }
        if lower.ends_with(".csv") {
            return Self::read_csv_to_batch(file, &[], row_limit);
        }
        if lower.ends_with(".tsv") {
            return Self::read_csv_to_batch(
                file,
                &[("delimiter".to_string(), "\t".to_string())],
                row_limit,
            );
        }
        if lower.ends_with(".txt") {
            return Self::read_csv_to_batch(
                file,
                &[("delimiter".to_string(), "auto".to_string())],
                row_limit,
            );
        }
        if lower.ends_with(".json") || lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
            return Self::read_json_to_batch(file, &[]);
        }
        if lower.ends_with(".parquet") {
            return Self::read_parquet_to_batch(file, &[], row_limit);
        }
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "Unsupported file format for '{}'. Supported: .csv, .tsv, .txt, .json, .jsonl, .ndjson, .parquet, .csv.gz",
                file
            ),
        ))
    }

    pub(in crate::query::executor) fn read_csv_to_batch(
        path: &str,
        options: &[(String, String)],
        row_limit: Option<usize>,
    ) -> io::Result<RecordBatch> {
        use rayon::prelude::*;

        let has_header = options
            .iter()
            .find(|(k, _)| k == "header")
            .map(|(_, v)| !matches!(v.to_lowercase().as_str(), "false" | "0"))
            .unwrap_or(true);

        let file = std::fs::File::open(path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Cannot open CSV file '{}': {}", path, e),
            )
        })?;

        // Bounded reads (`LIMIT n`) only touch the first few pages: a lazy mmap is
        // strictly better there. For full scans, choose by size: small files keep
        // mmap (zero-copy wins on warm cache); large files use a buffered read
        // (avoids per-page fault overhead of a huge mapping).
        let buffer = match row_limit {
            Some(_) => {
                let mmap = unsafe { memmap2::Mmap::map(&file) }
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("mmap error: {}", e)))?;
                CsvBuffer::Mmap(mmap)
            }
            None => {
                if file.metadata()?.len() >= SEQUENTIAL_READ_THRESHOLD {
                    use std::io::Read;
                    let mut f = file;
                    let len = f.metadata()?.len() as usize;
                    let mut buf = Vec::with_capacity(len + 1);
                    f.read_to_end(&mut buf).map_err(|e| {
                        io::Error::new(io::ErrorKind::Other, format!("read error: {}", e))
                    })?;
                    CsvBuffer::Owned(buf)
                } else {
                    let mmap = unsafe { memmap2::Mmap::map(&file) }
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("mmap error: {}", e)))?;
                    CsvBuffer::Mmap(mmap)
                }
            }
        };
        let data: &[u8] = buffer.as_slice();

        // Delimiter: explicit option, or "auto" to sniff from the header line (.txt).
        let delimiter: u8 = options
            .iter()
            .find(|(k, _)| k == "delimiter" || k == "delim" || k == "sep")
            .map(|(_, v)| v.as_str())
            .map(|v| {
                if v.eq_ignore_ascii_case("auto") {
                    Self::sniff_csv_delimiter(data)
                } else {
                    v.chars().next().map(|c| c as u8).unwrap_or(b',')
                }
            })
            .unwrap_or(b',');
        let bad_line_policy = csv_bad_line_policy(options)?;

        // Fast schema inference: single-pass over first 100 data rows,
        // bypasses Arrow's CSV reader overhead (extra buffering + allocation layer).
        let schema = Arc::new(Self::infer_csv_schema_fast(
            data, has_header, delimiter, 100,
        )?);

        // Find end of header line (SIMD via memchr)
        let header_end = if has_header {
            memchr::memchr(b'\n', data)
                .map(|i| i + 1)
                .unwrap_or(data.len())
        } else {
            0
        };
        let data_section = &data[header_end..];
        if data_section.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&schema)));
        }

        // Parse filter pushdown option: "col>val" or "col<val" style
        let filter_info = options
            .iter()
            .find(|(k, _)| k == "filter")
            .and_then(|(_, v)| parse_pushdown_filter(v, &schema));

        // On-demand read: parse only the first `row_limit` data rows.  The parser
        // exits its newline scan as soon as that many rows are produced, so a large
        // file is never fully scanned or materialized (memory stays bounded to `n`).
        if let Some(n) = row_limit {
            if n == 0 {
                return Ok(RecordBatch::new_empty(Arc::clone(&schema)));
            }
            return Self::parse_csv_chunk_fast(
                data_section,
                &schema,
                delimiter,
                filter_info.as_ref(),
                bad_line_policy,
                Some(n),
            );
        }

        // Compute split offsets — one chunk per core, aligned to newlines (SIMD search)
        let n_threads = rayon::current_num_threads().min(16).max(1);
        let mut starts: Vec<usize> = vec![0];
        if n_threads > 1 {
            let chunk = (data_section.len() + n_threads - 1) / n_threads;
            for t in 1..n_threads {
                let approx = t * chunk;
                if approx >= data_section.len() {
                    break;
                }
                let nl = memchr::memchr(b'\n', &data_section[approx..])
                    .map(|p| approx + p + 1)
                    .unwrap_or(data_section.len());
                if nl != *starts.last().unwrap() {
                    starts.push(nl);
                }
            }
        }
        starts.push(data_section.len());

        // Raw-pointer wrapper so slices can cross rayon thread boundaries safely.
        // Safe: `data` Vec outlives all spawned tasks (rayon join before function returns).
        struct SendSlice(*const u8, usize);
        unsafe impl Send for SendSlice {}
        unsafe impl Sync for SendSlice {}

        let chunks: Vec<SendSlice> = starts
            .windows(2)
            .map(|w| {
                let s = &data_section[w[0]..w[1]];
                SendSlice(s.as_ptr(), s.len())
            })
            .collect();

        let schema_ref = Arc::clone(&schema);
        let batches: Vec<io::Result<RecordBatch>> = chunks
            .par_iter()
            .map(|ss| {
                let chunk = unsafe { std::slice::from_raw_parts(ss.0, ss.1) };
                if chunk.is_empty() {
                    return Ok(RecordBatch::new_empty(Arc::clone(&schema_ref)));
                }
                Self::parse_csv_chunk_fast(
                    chunk,
                    &schema_ref,
                    delimiter,
                    filter_info.as_ref(),
                    bad_line_policy,
                    None,
                )
            })
            .collect();

        let all: Vec<RecordBatch> = batches.into_iter().collect::<io::Result<Vec<_>>>()?;
        Self::merge_record_batches(all)
    }

    /// Sniff the most frequent delimiter from the first line of a text file.
    /// Used for `.txt` files (and `delimiter='auto'`), matching DuckDB's
    /// `read_csv_auto` behavior for the common `,` / `\t` / `;` / `|` cases.
    fn sniff_csv_delimiter(data: &[u8]) -> u8 {
        let end = memchr::memchr(b'\n', data).unwrap_or(data.len());
        let line = &data[..end];
        let mut best = b',';
        let mut best_count = 0usize;
        for &c in &[b',', b'\t', b';', b'|'] {
            let count = memchr::memchr_iter(c, line).count();
            if count > best_count {
                best = c;
                best_count = count;
            }
        }
        best
    }

    pub(in crate::query::executor) fn parse_csv_chunk_fast(
        data: &[u8],
        schema: &arrow::datatypes::Schema,
        delimiter: u8,
        filter_info: Option<&PushdownFilter>,
        bad_line_policy: CsvBadLinePolicy,
        max_rows: Option<usize>,
    ) -> io::Result<RecordBatch> {
        use arrow::array::{BooleanArray, BooleanBuilder};
        use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
        use arrow::datatypes::DataType;

        let n_cols = schema.fields().len();
        if n_cols == 0 {
            return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
        }

        // Exact row-capacity: count line boundaries with SIMD memchr instead of a
        // bytes/20 heuristic. The heuristic over-allocates ~23x for wide rows
        // (e.g. 465B/line), blowing up peak memory and causing huge allocator
        // churn. The newline count is a cheap single pass over the already-cached
        // bytes and makes `Vec::with_capacity` reserve the true row count.
        // When max_rows is set (on-demand read), the capacity is bounded to that.
        if data.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
        }
        let n_rows = max_rows.unwrap_or_else(|| memchr::memchr_iter(b'\n', data).count());

        // Per-column raw buffers — direct Vec ops, no builder hierarchy.
        // has_null starts false; nulls Vec is only written when a null IS seen.
        enum ColBuf {
            I64 {
                vals: Vec<i64>,
                nulls: Vec<bool>,
                has_null: bool,
            },
            F64 {
                vals: Vec<f64>,
                nulls: Vec<bool>,
                has_null: bool,
            },
            Str {
                bytes: Vec<u8>,
                offsets: Vec<i32>,
            },
            Bool(BooleanBuilder),
        }

        let mut cols: Vec<ColBuf> = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                    ColBuf::I64 {
                        vals: Vec::with_capacity(n_rows),
                        nulls: Vec::new(),
                        has_null: false,
                    }
                }
                DataType::Float64 | DataType::Float32 => ColBuf::F64 {
                    vals: Vec::with_capacity(n_rows),
                    nulls: Vec::new(),
                    has_null: false,
                },
                DataType::Boolean => ColBuf::Bool(BooleanBuilder::with_capacity(n_rows)),
                _ => {
                    let mut offsets = Vec::with_capacity(n_rows + 1);
                    offsets.push(0i32);
                    ColBuf::Str {
                        bytes: Vec::with_capacity(n_rows * 12),
                        offsets,
                    }
                }
            })
            .collect();

        macro_rules! push_field {
            ($c:expr, $f:expr) => {
                match $c {
                    ColBuf::I64 {
                        vals,
                        nulls,
                        has_null,
                    } => {
                        if $f.is_empty() {
                            if !*has_null {
                                *has_null = true;
                                nulls.resize(vals.len(), true);
                            }
                            vals.push(0);
                            nulls.push(false);
                        } else {
                            vals.push(Self::parse_i64_bytes($f));
                            if *has_null {
                                nulls.push(true);
                            }
                        }
                    }
                    ColBuf::F64 {
                        vals,
                        nulls,
                        has_null,
                    } => match fast_float::parse::<f64, _>($f) {
                        Ok(v) => {
                            vals.push(v);
                            if *has_null {
                                nulls.push(true);
                            }
                        }
                        Err(_) => {
                            if !*has_null {
                                *has_null = true;
                                nulls.resize(vals.len(), true);
                            }
                            vals.push(0.0);
                            nulls.push(false);
                        }
                    },
                    ColBuf::Str { bytes, offsets } => {
                        bytes.extend_from_slice($f);
                        offsets.push(bytes.len() as i32);
                    }
                    ColBuf::Bool(b) => match $f {
                        b"true" | b"True" | b"TRUE" | b"1" => b.append_value(true),
                        b"false" | b"False" | b"FALSE" | b"0" => b.append_value(false),
                        _ => b.append_null(),
                    },
                }
            };
        }
        macro_rules! push_null {
            ($c:expr) => {
                match $c {
                    ColBuf::I64 {
                        vals,
                        nulls,
                        has_null,
                    } => {
                        if !*has_null {
                            *has_null = true;
                            nulls.resize(vals.len(), true);
                        }
                        vals.push(0);
                        nulls.push(false);
                    }
                    ColBuf::F64 {
                        vals,
                        nulls,
                        has_null,
                    } => {
                        if !*has_null {
                            *has_null = true;
                            nulls.resize(vals.len(), true);
                        }
                        vals.push(0.0);
                        nulls.push(false);
                    }
                    ColBuf::Str { bytes, offsets } => {
                        offsets.push(bytes.len() as i32);
                    }
                    ColBuf::Bool(b) => b.append_null(),
                }
            };
        }

        // Single forward pass — outer newline search via SIMD memchr_iter.
        // Inner delimiter search also uses SIMD memchr_iter (replaces scalar byte loop).
        let mut line_start = 0usize;
        let mut line_number = 1usize;
        let mut rows_out = 0usize;
        for nl in memchr::memchr_iter(b'\n', data) {
            let raw = &data[line_start..nl];
            line_start = nl + 1;
            let line = if raw.last() == Some(&b'\r') {
                &raw[..raw.len() - 1]
            } else {
                raw
            };
            if line.is_empty() {
                line_number += 1;
                continue;
            }
            if bad_line_policy != CsvBadLinePolicy::Error {
                let actual_fields = csv_field_count(line, delimiter);
                if actual_fields != n_cols {
                    if bad_line_policy == CsvBadLinePolicy::Warn {
                        eprintln!(
                            "ApexBase CSV warning: skipped row {} with {} fields; expected {}",
                            line_number, actual_fields, n_cols
                        );
                    }
                    line_number += 1;
                    continue;
                }
            }
            // Pushdown filter: skip row if filter column's value doesn't match
            if let Some(ref fi) = filter_info {
                let fv = Self::get_csv_field(line, fi.col_idx, delimiter);
                if !csv_filter_match(fv, fi) {
                    line_number += 1;
                    continue;
                }
            }
            let mut fs = 0usize;
            let mut col = 0usize;
            for i in memchr::memchr_iter(delimiter, line) {
                if col < n_cols {
                    push_field!(&mut cols[col], &line[fs..i]);
                }
                col += 1;
                fs = i + 1;
            }
            if bad_line_policy == CsvBadLinePolicy::Error && col + 1 != n_cols {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "CSV row {} has {} fields; expected {}",
                        line_number,
                        col + 1,
                        n_cols
                    ),
                ));
            }
            if col < n_cols {
                push_field!(&mut cols[col], &line[fs..]);
                col += 1;
            }
            while col < n_cols {
                push_null!(&mut cols[col]);
                col += 1;
            }
            rows_out += 1;
            if let Some(m) = max_rows {
                if rows_out >= m {
                    break;
                }
            }
            line_number += 1;
        }
        if line_start < data.len() && max_rows.map_or(true, |m| rows_out < m) {
            let raw = &data[line_start..];
            let line = if raw.last() == Some(&b'\r') {
                &raw[..raw.len() - 1]
            } else {
                raw
            };
            if !line.is_empty() {
                let parse_tail = if bad_line_policy == CsvBadLinePolicy::Error {
                    true
                } else {
                    let actual_fields = csv_field_count(line, delimiter);
                    if actual_fields == n_cols {
                        true
                    } else {
                        if bad_line_policy == CsvBadLinePolicy::Warn {
                            eprintln!(
                                "ApexBase CSV warning: skipped row {} with {} fields; expected {}",
                                line_number, actual_fields, n_cols
                            );
                        }
                        false
                    }
                };
                if parse_tail {
                    let mut fs = 0usize;
                    let mut col = 0usize;
                    for i in memchr::memchr_iter(delimiter, line) {
                        if col < n_cols {
                            push_field!(&mut cols[col], &line[fs..i]);
                        }
                        col += 1;
                        fs = i + 1;
                    }
                    if bad_line_policy == CsvBadLinePolicy::Error && col + 1 != n_cols {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "CSV row {} has {} fields; expected {}",
                                line_number,
                                col + 1,
                                n_cols
                            ),
                        ));
                    }
                    if col < n_cols {
                        push_field!(&mut cols[col], &line[fs..]);
                        col += 1;
                    }
                    while col < n_cols {
                        push_null!(&mut cols[col]);
                        col += 1;
                    }
                }
            }
        }

        // Materialize Arrow arrays from raw buffers
        use arrow::array::{Float64Array, Int64Array, StringArray};
        let arrays: Vec<arrow::array::ArrayRef> = cols
            .into_iter()
            .map(|c| match c {
                ColBuf::I64 {
                    vals,
                    nulls,
                    has_null,
                } => {
                    let null_buf = if has_null {
                        Some(NullBuffer::from(nulls))
                    } else {
                        None
                    };
                    Arc::new(Int64Array::new(ScalarBuffer::from(vals), null_buf)) as _
                }
                ColBuf::F64 {
                    vals,
                    nulls,
                    has_null,
                } => {
                    let null_buf = if has_null {
                        Some(NullBuffer::from(nulls))
                    } else {
                        None
                    };
                    Arc::new(Float64Array::new(ScalarBuffer::from(vals), null_buf)) as _
                }
                ColBuf::Str { bytes, offsets } => {
                    let ob = OffsetBuffer::new(ScalarBuffer::from(offsets));
                    let vb = Buffer::from_vec(bytes);
                    Arc::new(StringArray::new(ob, vb, None)) as _
                }
                ColBuf::Bool(mut b) => Arc::new(b.finish()) as _,
            })
            .collect();

        RecordBatch::try_new(Arc::new(schema.clone()), arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub(in crate::query::executor) fn infer_csv_schema_fast(
        data: &[u8],
        has_header: bool,
        delimiter: u8,
        max_rows: usize,
    ) -> io::Result<arrow::datatypes::Schema> {
        use arrow::datatypes::{DataType as ArrowDT, Field, Schema};

        if data.is_empty() {
            return Ok(Schema::empty());
        }

        // ── Parse header row ──────────────────────────────────────────────
        let first_nl = memchr::memchr(b'\n', data).unwrap_or(data.len());
        let first_line_raw = &data[..first_nl];
        let first_line = if first_line_raw.last() == Some(&b'\r') {
            &first_line_raw[..first_line_raw.len() - 1]
        } else {
            first_line_raw
        };

        let col_names: Vec<String> = if has_header {
            // Split header by delimiter — field names (UTF-8)
            let mut names = Vec::new();
            let mut fs = 0usize;
            for i in memchr::memchr_iter(delimiter, first_line) {
                let raw = &first_line[fs..i];
                let s = std::str::from_utf8(raw)
                    .unwrap_or("")
                    .trim()
                    .trim_matches('"')
                    .trim()
                    .to_string();
                names.push(if s.is_empty() {
                    format!("col_{}", names.len())
                } else {
                    s
                });
                fs = i + 1;
            }
            let tail = &first_line[fs..];
            let s = std::str::from_utf8(tail)
                .unwrap_or("")
                .trim()
                .trim_matches('"')
                .trim()
                .to_string();
            names.push(if s.is_empty() {
                format!("col_{}", names.len())
            } else {
                s
            });
            names
        } else {
            // No header: synthesise f0, f1, …
            let n_cols = memchr::memchr_iter(delimiter, first_line).count() + 1;
            (0..n_cols).map(|i| format!("f{}", i)).collect()
        };

        let n_cols = col_names.len();
        if n_cols == 0 {
            return Ok(Schema::empty());
        }

        // ── Sample up to max_rows data lines to infer types ───────────────
        // Type priority: 0=unknown, 1=Int64, 2=Float64, 3=String
        let mut col_types: Vec<u8> = vec![0u8; n_cols];

        let data_start = if has_header { first_nl + 1 } else { 0 };
        let mut rows_seen = 0usize;
        let mut ls = data_start;

        // Iterate lines via SIMD newline search
        for nl in memchr::memchr_iter(b'\n', &data[data_start..]).map(|p| p + data_start) {
            if rows_seen >= max_rows {
                break;
            }
            let raw = &data[ls..nl];
            ls = nl + 1;
            let line = if raw.last() == Some(&b'\r') {
                &raw[..raw.len() - 1]
            } else {
                raw
            };
            if line.is_empty() {
                continue;
            }

            let mut col = 0usize;
            let mut fs = 0usize;
            for i in memchr::memchr_iter(delimiter, line) {
                if col < n_cols {
                    Self::update_col_type(&mut col_types[col], &line[fs..i]);
                }
                col += 1;
                fs = i + 1;
            }
            if col < n_cols {
                Self::update_col_type(&mut col_types[col], &line[fs..]);
            }
            rows_seen += 1;
        }
        // Handle tail line (no trailing newline)
        if ls < data.len() && rows_seen < max_rows {
            let raw = &data[ls..];
            let line = if raw.last() == Some(&b'\r') {
                &raw[..raw.len() - 1]
            } else {
                raw
            };
            if !line.is_empty() {
                let mut col = 0usize;
                let mut fs = 0usize;
                for i in memchr::memchr_iter(delimiter, line) {
                    if col < n_cols {
                        Self::update_col_type(&mut col_types[col], &line[fs..i]);
                    }
                    col += 1;
                    fs = i + 1;
                }
                if col < n_cols {
                    Self::update_col_type(&mut col_types[col], &line[fs..]);
                }
            }
        }

        let fields: Vec<Field> = col_names
            .iter()
            .zip(col_types.iter())
            .map(|(name, &t)| {
                let dt = match t {
                    0 | 1 => ArrowDT::Int64, // unknown or Int64 → Int64
                    2 => ArrowDT::Float64,
                    _ => ArrowDT::Utf8,
                };
                Field::new(name, dt, true)
            })
            .collect();

        Ok(Schema::new(fields))
    }

    pub(in crate::query::executor) fn update_col_type(col_type: &mut u8, field: &[u8]) {
        if *col_type >= 3 {
            return;
        } // already String — no point checking
        if field.is_empty() {
            return;
        } // null/empty — don't escalate

        // Strip surrounding quotes (common in exported CSVs)
        let f = if field.first() == Some(&b'"') && field.last() == Some(&b'"') && field.len() >= 2 {
            &field[1..field.len() - 1]
        } else {
            field
        };
        if f.is_empty() {
            return;
        }

        // Try Int64 first (cheapest check)
        if *col_type <= 1 {
            let digits = match f.first() {
                Some(&b'-') | Some(&b'+') => &f[1..],
                _ => f,
            };
            if !digits.is_empty() && digits.iter().all(|&b| b >= b'0' && b <= b'9') {
                *col_type = 1; // Int64
                return;
            }
        }
        // Try Float64
        if *col_type <= 2 {
            if fast_float::parse::<f64, _>(f).is_ok() {
                *col_type = 2; // Float64
                return;
            }
        }
        // Must be String
        *col_type = 3;
    }

    pub(in crate::query::executor) fn parse_i64_bytes(b: &[u8]) -> i64 {
        let (neg, digits) = match b.first() {
            Some(&b'-') => (true, &b[1..]),
            Some(&b'+') => (false, &b[1..]),
            _ => (false, b),
        };
        let mut v = 0i64;
        for &d in digits {
            v = v * 10 + (d.wrapping_sub(b'0')) as i64;
        }
        if neg {
            -v
        } else {
            v
        }
    }

    pub(in crate::query::executor) fn parse_u64_bytes(b: &[u8]) -> u64 {
        let digits = if b.first() == Some(&b'+') { &b[1..] } else { b };
        let mut v = 0u64;
        for &d in digits {
            v = v * 10 + (d.wrapping_sub(b'0')) as u64;
        }
        v
    }

    pub(in crate::query::executor) fn get_csv_field(line: &[u8], col: usize, delimiter: u8) -> &[u8] {
        let mut count = 0usize;
        let mut start = 0usize;
        for (i, &b) in line.iter().enumerate() {
            if b == delimiter {
                if count == col {
                    return &line[start..i];
                }
                count += 1;
                start = i + 1;
            }
        }
        if count == col {
            &line[start..]
        } else {
            b""
        }
    }

    pub(in crate::query::executor) fn extract_csv_column(
        data: &[u8],
        col_idx: usize,
        dtype: &arrow::datatypes::DataType,
        delimiter: u8,
        n_rows: usize,
    ) -> io::Result<arrow::array::ArrayRef> {
        use arrow::array::{
            BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder,
        };
        use arrow::datatypes::DataType;

        // Shared line iterator body
        macro_rules! scan_lines {
            ($callback:expr) => {{
                let mut ls = 0usize;
                for nl in memchr::memchr_iter(b'\n', data) {
                    let raw = &data[ls..nl];
                    ls = nl + 1;
                    let line = if raw.last() == Some(&b'\r') {
                        &raw[..raw.len() - 1]
                    } else {
                        raw
                    };
                    if !line.is_empty() {
                        $callback(Self::get_csv_field(line, col_idx, delimiter));
                    }
                }
                if ls < data.len() {
                    let raw = &data[ls..];
                    let line = if raw.last() == Some(&b'\r') {
                        &raw[..raw.len() - 1]
                    } else {
                        raw
                    };
                    if !line.is_empty() {
                        $callback(Self::get_csv_field(line, col_idx, delimiter));
                    }
                }
            }};
        }

        match dtype {
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                let mut b = Int64Builder::with_capacity(n_rows);
                scan_lines!(|f: &[u8]| if f.is_empty() {
                    b.append_null()
                } else {
                    b.append_value(Self::parse_i64_bytes(f))
                });
                Ok(Arc::new(b.finish()) as _)
            }
            DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => {
                let mut b = UInt64Builder::with_capacity(n_rows);
                scan_lines!(|f: &[u8]| if f.is_empty() {
                    b.append_null()
                } else {
                    b.append_value(Self::parse_u64_bytes(f))
                });
                Ok(Arc::new(b.finish()) as _)
            }
            DataType::Float64 | DataType::Float32 => {
                let mut b = Float64Builder::with_capacity(n_rows);
                scan_lines!(|f: &[u8]| match fast_float::parse::<f64, _>(f) {
                    Ok(v) => b.append_value(v),
                    Err(_) => b.append_null(),
                });
                Ok(Arc::new(b.finish()) as _)
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(n_rows);
                scan_lines!(|f: &[u8]| match f {
                    b"true" | b"True" | b"TRUE" | b"1" => b.append_value(true),
                    b"false" | b"False" | b"FALSE" | b"0" => b.append_value(false),
                    _ => b.append_null(),
                });
                Ok(Arc::new(b.finish()) as _)
            }
            _ => {
                let mut b = StringBuilder::with_capacity(n_rows, n_rows * 12);
                scan_lines!(|f: &[u8]| {
                    // SAFETY: CSV is text data — valid UTF-8 in practice
                    b.append_value(unsafe { std::str::from_utf8_unchecked(f) });
                });
                Ok(Arc::new(b.finish()) as _)
            }
        }
    }

    pub(in crate::query::executor) fn read_json_to_batch(
        path: &str,
        _options: &[(String, String)],
    ) -> io::Result<RecordBatch> {
        use rayon::prelude::*;

        let file = std::fs::File::open(path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Cannot open JSON file '{}': {}", path, e),
            )
        })?;
        let mmap = unsafe { memmap2::Mmap::map(&file) }
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("mmap error: {}", e)))?;
        let bytes: &[u8] = &mmap;

        // Trim leading/trailing whitespace via byte scan (no UTF-8 decode overhead)
        let start = bytes
            .iter()
            .position(|&b| !b.is_ascii_whitespace())
            .unwrap_or(bytes.len());
        let end = bytes
            .iter()
            .rposition(|&b| !b.is_ascii_whitespace())
            .map(|i| i + 1)
            .unwrap_or(start);
        let trimmed_bytes = &bytes[start..end];
        if trimmed_bytes.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(
                arrow::datatypes::Schema::empty(),
            )));
        }

        // Fast path: pandas "columns" / structured JSON format (starts with '{').
        // Convert mmap bytes to str for serde — safe because JSON is UTF-8.
        if trimmed_bytes.first() == Some(&b'{') {
            let trimmed = unsafe { std::str::from_utf8_unchecked(trimmed_bytes) };
            if let Some(batch) = Self::try_columns_format_fast(trimmed)? {
                return Ok(batch);
            }
            // Try single-value parse (split/index/records format)
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
                return Self::json_value_to_batch(value);
            }
        }

        // Try array-of-records format
        if trimmed_bytes.first() == Some(&b'[') {
            let trimmed = unsafe { std::str::from_utf8_unchecked(trimmed_bytes) };
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
                return Self::json_value_to_batch(value);
            }
        }

        // NDJSON path: parallel chunk parsing.
        // 1. Infer schema from first 100 lines (sequential, fast).
        // 2. Split file into N_threads line-aligned chunks.
        // 3. Parse each chunk in parallel → Vec<RecordBatch>.
        // 4. Merge.
        let n_threads = rayon::current_num_threads().min(16).max(1);

        // Schema inference: read first 100 lines with Arrow (sequential, small)
        let schema = {
            use arrow::json::reader::infer_json_schema_from_seekable;
            use std::io::BufReader;
            // Take first 100 lines for inference
            let mut ls = 0usize;
            let mut rows = 0usize;
            let mut infer_end = trimmed_bytes.len();
            for nl in memchr::memchr_iter(b'\n', trimmed_bytes) {
                ls = nl + 1;
                rows += 1;
                if rows >= 100 {
                    infer_end = nl + 1;
                    break;
                }
            }
            let _ = ls;
            let mut buf = BufReader::new(std::io::Cursor::new(&trimmed_bytes[..infer_end]));
            let (schema, _) =
                infer_json_schema_from_seekable(&mut buf, Some(100)).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("JSON schema inference: {}", e),
                    )
                })?;
            Arc::new(schema)
        };

        // Build line-aligned chunk boundaries (SIMD newline search)
        let mut starts: Vec<usize> = vec![0];
        if n_threads > 1 {
            let chunk = (trimmed_bytes.len() + n_threads - 1) / n_threads;
            for t in 1..n_threads {
                let approx = t * chunk;
                if approx >= trimmed_bytes.len() {
                    break;
                }
                let nl = memchr::memchr(b'\n', &trimmed_bytes[approx..])
                    .map(|p| approx + p + 1)
                    .unwrap_or(trimmed_bytes.len());
                if nl != *starts.last().unwrap() {
                    starts.push(nl);
                }
            }
        }
        starts.push(trimmed_bytes.len());

        // Raw-pointer wrapper for cross-thread mmap slice sharing
        struct SendSlice(*const u8, usize);
        unsafe impl Send for SendSlice {}
        unsafe impl Sync for SendSlice {}

        let chunks: Vec<SendSlice> = starts
            .windows(2)
            .map(|w| {
                let s = &trimmed_bytes[w[0]..w[1]];
                SendSlice(s.as_ptr(), s.len())
            })
            .collect();

        let schema_ref = Arc::clone(&schema);
        let batches: Vec<io::Result<RecordBatch>> = chunks
            .par_iter()
            .map(|ss| {
                use std::io::BufReader;
                let chunk = unsafe { std::slice::from_raw_parts(ss.0, ss.1) };
                if chunk.is_empty() {
                    return Ok(RecordBatch::new_empty(Arc::clone(&schema_ref)));
                }
                let mut buf = BufReader::new(std::io::Cursor::new(chunk));
                let reader = arrow::json::ReaderBuilder::new(Arc::clone(&schema_ref))
                    .build(&mut buf)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                let sub: Vec<RecordBatch> = reader
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                Self::merge_record_batches(sub)
            })
            .collect();

        let all: Vec<RecordBatch> = batches.into_iter().collect::<io::Result<Vec<_>>>()?;
        Self::merge_record_batches(all)
    }

    pub(in crate::query::executor) fn try_fast_json_count(
        path: &str,
        where_clause: Option<&SqlExpr>,
    ) -> io::Result<Option<i64>> {
        use rayon::prelude::*;

        let file = std::fs::File::open(path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Cannot open JSON file '{}': {}", path, e),
            )
        })?;
        let mmap = unsafe { memmap2::Mmap::map(&file) }
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("mmap error: {}", e)))?;
        let bytes: &[u8] = &mmap;

        let start = bytes
            .iter()
            .position(|&b| !b.is_ascii_whitespace())
            .unwrap_or(bytes.len());
        let end = bytes
            .iter()
            .rposition(|&b| !b.is_ascii_whitespace())
            .map(|i| i + 1)
            .unwrap_or(start);
        let trimmed = &bytes[start..end];
        if trimmed.is_empty() {
            return Ok(Some(0));
        }
        if !Self::looks_like_ndjson(trimmed) {
            return Ok(None);
        }

        let filter = match where_clause {
            Some(expr) => Some(match Self::extract_json_numeric_filter(expr) {
                Some(f) => f,
                None => return Ok(None),
            }),
            None => None,
        };

        let n_threads = rayon::current_num_threads().min(16).max(1);
        let mut starts: Vec<usize> = vec![0];
        if n_threads > 1 {
            let chunk = (trimmed.len() + n_threads - 1) / n_threads;
            for t in 1..n_threads {
                let approx = t * chunk;
                if approx >= trimmed.len() {
                    break;
                }
                let nl = memchr::memchr(b'\n', &trimmed[approx..])
                    .map(|p| approx + p + 1)
                    .unwrap_or(trimmed.len());
                if nl != *starts.last().unwrap() {
                    starts.push(nl);
                }
            }
        }
        starts.push(trimmed.len());

        struct SendSlice(*const u8, usize);
        unsafe impl Send for SendSlice {}
        unsafe impl Sync for SendSlice {}

        let chunks: Vec<SendSlice> = starts
            .windows(2)
            .map(|w| {
                let s = &trimmed[w[0]..w[1]];
                SendSlice(s.as_ptr(), s.len())
            })
            .collect();

        let count: usize = chunks
            .par_iter()
            .map(|ss| {
                let chunk = unsafe { std::slice::from_raw_parts(ss.0, ss.1) };
                Self::count_json_chunk_rows(chunk, filter.as_ref())
            })
            .sum();
        Ok(Some(count as i64))
    }

    /// Fast `COUNT(*)`/`COUNT(1)` over a CSV/TSV file: counts row boundaries via
    /// SIMD `memchr` newline scan and per-line field-count validation, WITHOUT
    /// materialising any typed column buffers. Mirrors `parse_csv_chunk_fast`
    /// semantics exactly (header skip, empty-line skip, bad-line policy) so the
    /// result equals the full-parse count for a pure count (*) with no WHERE.
    ///
    /// Only a plain (non-gzip) CSV/TSV without a WHERE clause is handled; every
    /// other shape returns `Ok(None)` so the caller falls back to the regular
    /// parse (which still raises the proper error for malformed rows).
    pub(in crate::query::executor) fn try_fast_csv_count(
        path: &str,
        options: &[(String, String)],
    ) -> io::Result<Option<i64>> {
        use rayon::prelude::*;

        let has_header = options
            .iter()
            .find(|(k, _)| k == "header")
            .map(|(_, v)| !matches!(v.to_lowercase().as_str(), "false" | "0"))
            .unwrap_or(true);

        let bad_line_policy = csv_bad_line_policy(options)?;

        // A count always scans the whole file. Choose the backing by size: mmap
        // for small files (zero-copy wins on warm cache), buffered read for large
        // ones (avoids per-page fault overhead of a huge mapping).
        let buffer = {
            let file = std::fs::File::open(path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Cannot open CSV file '{}': {}", path, e),
                )
            })?;
            if file.metadata()?.len() >= SEQUENTIAL_READ_THRESHOLD {
                CsvBuffer::Owned(read_file_sequential(path)?)
            } else {
                let mmap = unsafe { memmap2::Mmap::map(&file) }
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("mmap error: {}", e)))?;
                CsvBuffer::Mmap(mmap)
            }
        };
        let data: &[u8] = buffer.as_slice();

        let delimiter: u8 = {
            let delim_opt = options
                .iter()
                .find(|(k, _)| k == "delimiter" || k == "delim" || k == "sep")
                .map(|(_, v)| v.as_str());
            match delim_opt {
                Some(v) if v.eq_ignore_ascii_case("auto") => Self::sniff_csv_delimiter(data),
                Some(v) => v.chars().next().map(|c| c as u8).unwrap_or(b','),
                None => b',',
            }
        };

        // Skip the header line (same as read_csv_to_batch: data starts after the
        // first newline). The count never needs column names for COUNT(*).
        let data_section = if has_header {
            memchr::memchr(b'\n', data)
                .map(|i| i + 1)
                .unwrap_or(data.len())
        } else {
            0
        };
        let data = &data[data_section..];
        if data.is_empty() {
            return Ok(Some(0));
        }

        // The full parser's Error-policy field-count check and the actual column
        // split are quote-naive (memchr over the delimiter), so quoted data can
        // produce a different count than the quote-aware `csv_field_count`.
        // Decline the fast path on any quote so the full parser governs.
        if memchr::memchr(b'"', data).is_some() {
            return Ok(None);
        }

        // Column count from the first data row; consistent with schema inference
        // which assumes rows have identical shape.
        let first_line_end = memchr::memchr(b'\n', data).unwrap_or(data.len());
        let first_line = {
            let raw = &data[..first_line_end];
            if raw.last() == Some(&b'\r') {
                &raw[..raw.len() - 1]
            } else {
                raw
            }
        };
        if first_line.is_empty() {
            return Ok(Some(0));
        }
        let n_cols = csv_field_count(first_line, delimiter);

        // Compute split offsets — one chunk per core, aligned to newlines.
        let n_threads = rayon::current_num_threads().min(16).max(1);
        let mut starts: Vec<usize> = vec![0];
        if n_threads > 1 {
            let chunk = (data.len() + n_threads - 1) / n_threads;
            for t in 1..n_threads {
                let approx = t * chunk;
                if approx >= data.len() {
                    break;
                }
                let nl = memchr::memchr(b'\n', &data[approx..])
                    .map(|p| approx + p + 1)
                    .unwrap_or(data.len());
                if nl != *starts.last().unwrap() {
                    starts.push(nl);
                }
            }
        }
        starts.push(data.len());

        struct SendSlice(*const u8, usize);
        unsafe impl Send for SendSlice {}
        unsafe impl Sync for SendSlice {}

        let chunks: Vec<SendSlice> = starts
            .windows(2)
            .map(|w| {
                let s = &data[w[0]..w[1]];
                SendSlice(s.as_ptr(), s.len())
            })
            .collect();

        let invalid = std::sync::atomic::AtomicBool::new(false);
        let invalid_ref = &invalid;
        let count: usize = chunks
            .par_iter()
            .map(|ss| {
                let chunk = unsafe { std::slice::from_raw_parts(ss.0, ss.1) };
                match Self::count_csv_chunk_rows(chunk, delimiter, n_cols, bad_line_policy) {
                    Ok(c) => c,
                    Err(_) => {
                        // Malformed row under the Error policy: fall back to the
                        // real parser, which will raise the proper error.
                        invalid_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                        0
                    }
                }
            })
            .sum();
        if invalid.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(None);
        }
        Ok(Some(count as i64))
    }

    /// Scan only the requested numeric CSV columns and return scalar aggregate
    /// state without constructing an Arrow batch for the whole file.
    ///
    /// This is intentionally limited to quote-free CSV.  The regular parser is
    /// still the correctness path for quoted fields, and for malformed rows the
    /// caller receives `None` so it can preserve the parser's existing error or
    /// skip-row behaviour.  The scan validates field counts while it finds the
    /// requested fields, so valid rows are never silently omitted.
    pub(in crate::query::executor) fn try_fast_csv_numeric_agg(
        path: &str,
        options: &[(String, String)],
        columns: &[String],
        predicates: &[(String, f64, f64)],
    ) -> io::Result<Option<(i64, Vec<CsvNumericStats>)>> {
        use arrow::datatypes::DataType;
        use rayon::prelude::*;

        if columns.is_empty() {
            return Ok(None);
        }

        let has_header = options
            .iter()
            .find(|(k, _)| k == "header")
            .map(|(_, v)| !matches!(v.to_lowercase().as_str(), "false" | "0"))
            .unwrap_or(true);
        let bad_line_policy = csv_bad_line_policy(options)?;
        // The full parser emits one warning for every skipped malformed row.
        // Keep that observable behaviour by letting it handle the `warn`
        // policy instead of silently skipping rows in the parallel scanner.
        if bad_line_policy == CsvBadLinePolicy::Warn {
            return Ok(None);
        }

        let file = std::fs::File::open(path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Cannot open CSV file '{}': {}", path, e),
            )
        })?;
        let buffer = if file.metadata()?.len() >= SEQUENTIAL_READ_THRESHOLD {
            CsvBuffer::Owned(read_file_sequential(path)?)
        } else {
            let mmap = unsafe { memmap2::Mmap::map(&file) }
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("mmap error: {}", e)))?;
            CsvBuffer::Mmap(mmap)
        };
        let data = buffer.as_slice();

        let delimiter = options
            .iter()
            .find(|(k, _)| k == "delimiter" || k == "delim" || k == "sep")
            .map(|(_, v)| {
                if v.eq_ignore_ascii_case("auto") {
                    Self::sniff_csv_delimiter(data)
                } else {
                    v.chars().next().map(|c| c as u8).unwrap_or(b',')
                }
            })
            .unwrap_or(b',');

        let schema = Self::infer_csv_schema_fast(data, has_header, delimiter, 100)?;
        let mut scan_columns = columns.to_vec();
        for (column, _, _) in predicates {
            if !scan_columns.contains(column) {
                scan_columns.push(column.clone());
            }
        }
        let mut target_specs: Vec<(usize, usize, bool)> = Vec::with_capacity(scan_columns.len());
        for (output_index, column) in scan_columns.iter().enumerate() {
            let Some(field_index) = schema.fields().iter().position(|field| field.name() == column)
            else {
                return Ok(None);
            };
            let is_int = match schema.field(field_index).data_type() {
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => true,
                DataType::Float64 | DataType::Float32 => false,
                _ => return Ok(None),
            };
            target_specs.push((field_index, output_index, is_int));
        }
        target_specs.sort_unstable_by_key(|(field_index, _, _)| *field_index);
        let predicate_specs = predicates
            .iter()
            .map(|(column, low, high)| {
                scan_columns
                    .iter()
                    .position(|candidate| candidate == column)
                    .map(|index| (index, *low, *high))
            })
            .collect::<Option<Vec<_>>>();
        let Some(predicate_specs) = predicate_specs else {
            return Ok(None);
        };

        let data_start = if has_header {
            memchr::memchr(b'\n', data)
                .map(|index| index + 1)
                .unwrap_or(data.len())
        } else {
            0
        };
        let data = &data[data_start..];
        let empty_stats = target_specs
            .iter()
            .map(|(_, output_index, is_int)| (*output_index, CsvNumericStats::new(*is_int)))
            .collect::<Vec<_>>();
        if data.is_empty() {
            let mut stats = vec![CsvNumericStats::new(true); columns.len()];
            for (output_index, stat) in empty_stats {
                stats[output_index] = stat;
            }
            return Ok(Some((0, stats)));
        }

        // The fast line/field scanner has the same quote-naive contract as the
        // existing CSV fast count.  Decline quoted data instead of changing CSV
        // semantics for files that need a quote-aware parser.
        if memchr::memchr(b'"', data).is_some() {
            return Ok(None);
        }

        let first_line = data
            .split(|&byte| byte == b'\n')
            .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
            .find(|line| !line.is_empty())
            .unwrap_or(&[]);
        if first_line.is_empty() {
            let mut stats = vec![CsvNumericStats::new(true); columns.len()];
            for (output_index, stat) in empty_stats {
                stats[output_index] = stat;
            }
            return Ok(Some((0, stats)));
        }
        let n_cols = memchr::memchr_iter(delimiter, first_line).count() + 1;

        let mut starts = vec![0usize];
        let n_threads = rayon::current_num_threads().min(16).max(1);
        if n_threads > 1 {
            let chunk = (data.len() + n_threads - 1) / n_threads;
            for thread in 1..n_threads {
                let approx = thread * chunk;
                if approx >= data.len() {
                    break;
                }
                let start = memchr::memchr(b'\n', &data[approx..])
                    .map(|index| approx + index + 1)
                    .unwrap_or(data.len());
                if start != *starts.last().unwrap() {
                    starts.push(start);
                }
            }
        }
        starts.push(data.len());

        struct SendSlice(*const u8, usize);
        unsafe impl Send for SendSlice {}
        unsafe impl Sync for SendSlice {}

        let chunks: Vec<SendSlice> = starts
            .windows(2)
            .map(|window| {
                let chunk = &data[window[0]..window[1]];
                SendSlice(chunk.as_ptr(), chunk.len())
            })
            .collect();
        let partials: io::Result<Vec<(i64, Vec<CsvNumericStats>)>> = chunks
            .par_iter()
            .map(|slice| {
                let chunk = unsafe { std::slice::from_raw_parts(slice.0, slice.1) };
                Self::scan_csv_numeric_chunk(
                    chunk,
                    delimiter,
                    n_cols,
                    bad_line_policy,
                    &target_specs,
                    scan_columns.len(),
                    columns.len(),
                    &predicate_specs,
                )
            })
            .collect();

        let partials = match partials {
            Ok(partials) => partials,
            // Let the regular parser produce the established malformed-row
            // error (or apply its skip/warn policy) rather than returning a
            // fast-path-specific error.
            Err(_) => return Ok(None),
        };
        let mut row_count = 0i64;
        let mut stats = vec![CsvNumericStats::new(true); columns.len()];
        for (partial_rows, partial_stats) in partials {
            row_count += partial_rows;
            for (output_index, stat) in partial_stats.into_iter().enumerate() {
                if stats[output_index].count == 0 {
                    stats[output_index].is_int = stat.is_int;
                }
                stats[output_index].merge(stat);
            }
        }
        Ok(Some((row_count, stats)))
    }

    /// Group a quote-free CSV by one string or integer column while aggregating only the
    /// requested numeric columns. Each worker owns a compact local group map;
    /// maps are merged after the parallel scan, so wide input rows are never
    /// materialised as Arrow arrays.
    pub(in crate::query::executor) fn try_fast_csv_group_numeric_agg(
        path: &str,
        options: &[(String, String)],
        group_column: &str,
        columns: &[String],
        predicates: &[(String, f64, f64)],
    ) -> io::Result<
        Option<(
            CsvGroupKeyType,
            Vec<(Option<Vec<u8>>, i64, Vec<CsvNumericStats>)>,
        )>,
    > {
        use arrow::datatypes::DataType;
        use rayon::prelude::*;

        if columns.is_empty() {
            return Ok(None);
        }
        let has_header = options
            .iter()
            .find(|(key, _)| key == "header")
            .map(|(_, value)| !matches!(value.to_lowercase().as_str(), "false" | "0"))
            .unwrap_or(true);
        let bad_line_policy = csv_bad_line_policy(options)?;
        if bad_line_policy == CsvBadLinePolicy::Warn {
            return Ok(None);
        }
        let file = std::fs::File::open(path)?;
        let buffer = if file.metadata()?.len() >= SEQUENTIAL_READ_THRESHOLD {
            CsvBuffer::Owned(read_file_sequential(path)?)
        } else {
            CsvBuffer::Mmap(unsafe { memmap2::Mmap::map(&file) }?)
        };
        let all_data = buffer.as_slice();
        let delimiter = options
            .iter()
            .find(|(key, _)| key == "delimiter" || key == "delim" || key == "sep")
            .map(|(_, value)| {
                if value.eq_ignore_ascii_case("auto") {
                    Self::sniff_csv_delimiter(all_data)
                } else {
                    value.chars().next().map(|value| value as u8).unwrap_or(b',')
                }
            })
            .unwrap_or(b',');
        let schema = Self::infer_csv_schema_fast(all_data, has_header, delimiter, 100)?;
        let Some(group_index) = schema
            .fields()
            .iter()
            .position(|field| field.name() == group_column)
        else {
            return Ok(None);
        };
        let group_key_type = match schema.field(group_index).data_type() {
            DataType::Utf8 => CsvGroupKeyType::Utf8,
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                CsvGroupKeyType::Int64
            }
            _ => return Ok(None),
        };
        let mut scan_columns = columns.to_vec();
        for (column, _, _) in predicates {
            if !scan_columns.contains(column) {
                scan_columns.push(column.clone());
            }
        }
        let mut targets = vec![(group_index, CsvGroupTarget::Group)];
        let mut scan_is_int = Vec::with_capacity(scan_columns.len());
        for (output, column) in scan_columns.iter().enumerate() {
            let Some(index) = schema.fields().iter().position(|field| field.name() == column) else {
                return Ok(None);
            };
            let is_int = match schema.field(index).data_type() {
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => true,
                DataType::Float64 | DataType::Float32 => false,
                _ => return Ok(None),
            };
            scan_is_int.push(is_int);
            targets.push((index, CsvGroupTarget::Numeric { output, is_int }));
        }
        targets.sort_unstable_by_key(|(index, _)| *index);
        let predicate_specs = predicates
            .iter()
            .map(|(column, low, high)| {
                scan_columns
                    .iter()
                    .position(|candidate| candidate == column)
                    .map(|index| (index, *low, *high))
            })
            .collect::<Option<Vec<_>>>();
        let Some(predicate_specs) = predicate_specs else {
            return Ok(None);
        };

        let data_start = if has_header {
            memchr::memchr(b'\n', all_data)
                .map(|index| index + 1)
                .unwrap_or(all_data.len())
        } else {
            0
        };
        let data = &all_data[data_start..];
        if data.is_empty() || memchr::memchr(b'"', data).is_some() {
            return Ok(None);
        }
        let first_line = data
            .split(|&byte| byte == b'\n')
            .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
            .find(|line| !line.is_empty())
            .unwrap_or(&[]);
        if first_line.is_empty() {
            return Ok(Some((group_key_type, Vec::new())));
        }
        let column_count = memchr::memchr_iter(delimiter, first_line).count() + 1;
        let mut starts = vec![0usize];
        let thread_count = rayon::current_num_threads().clamp(1, 16);
        let chunk_size = data.len().div_ceil(thread_count);
        for thread in 1..thread_count {
            let approximate = thread * chunk_size;
            if approximate >= data.len() {
                break;
            }
            let start = memchr::memchr(b'\n', &data[approximate..])
                .map(|index| approximate + index + 1)
                .unwrap_or(data.len());
            if start != *starts.last().unwrap() {
                starts.push(start);
            }
        }
        starts.push(data.len());

        struct SendSlice(*const u8, usize);
        unsafe impl Send for SendSlice {}
        unsafe impl Sync for SendSlice {}
        let chunks = starts
            .windows(2)
            .map(|window| {
                let chunk = &data[window[0]..window[1]];
                SendSlice(chunk.as_ptr(), chunk.len())
            })
            .collect::<Vec<_>>();
        type GroupMap = ahash::AHashMap<CsvGroupKey, (i64, Vec<CsvNumericStats>)>;
        let partials: io::Result<Vec<GroupMap>> = chunks
            .par_iter()
            .map(|slice| {
                let chunk = unsafe { std::slice::from_raw_parts(slice.0, slice.1) };
                let mut groups = GroupMap::new();
                let mut row_stats = scan_is_int
                    .iter()
                    .map(|&is_int| CsvNumericStats::new(is_int))
                    .collect::<Vec<_>>();
                let mut line_start = 0usize;
                for line_end in memchr::memchr_iter(b'\n', chunk)
                    .chain(std::iter::once(chunk.len()))
                {
                    let raw_line = &chunk[line_start..line_end];
                    line_start = line_end.saturating_add(1);
                    let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
                    if line.is_empty() {
                        continue;
                    }
                    for (stat, &is_int) in row_stats.iter_mut().zip(&scan_is_int) {
                        *stat = CsvNumericStats::new(is_int);
                    }
                    let mut group = None;
                    let mut field_index = 0usize;
                    let mut field_start = 0usize;
                    let mut target_index = 0usize;
                    for field_end in memchr::memchr_iter(delimiter, line) {
                        while target_index < targets.len()
                            && targets[target_index].0 == field_index
                        {
                            match targets[target_index].1 {
                                CsvGroupTarget::Group => group = Some(&line[field_start..field_end]),
                                CsvGroupTarget::Numeric { output, is_int } => {
                                    Self::update_csv_numeric_stat(
                                        &mut row_stats[output],
                                        &line[field_start..field_end],
                                        is_int,
                                    )?;
                                }
                            }
                            target_index += 1;
                        }
                        field_index += 1;
                        field_start = field_end + 1;
                    }
                    while target_index < targets.len() && targets[target_index].0 == field_index {
                        match targets[target_index].1 {
                            CsvGroupTarget::Group => group = Some(&line[field_start..]),
                            CsvGroupTarget::Numeric { output, is_int } => {
                                Self::update_csv_numeric_stat(
                                    &mut row_stats[output],
                                    &line[field_start..],
                                    is_int,
                                )?;
                            }
                        }
                        target_index += 1;
                    }
                    if field_index + 1 != column_count || group.is_none() {
                        if bad_line_policy == CsvBadLinePolicy::Error {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "CSV row has wrong field count",
                            ));
                        }
                        continue;
                    }
                    if !predicate_specs.iter().all(|&(index, low, high)| {
                        let value = row_stats[index];
                        value.count == 1 && value.min >= low && value.max <= high
                    }) {
                        continue;
                    }
                    let group = group.unwrap();
                    let key = match group_key_type {
                        CsvGroupKeyType::Utf8 => {
                            if group.is_empty() {
                                CsvGroupKey::Null
                            } else {
                                CsvGroupKey::Utf8(group.to_vec())
                            }
                        }
                        CsvGroupKeyType::Int64 => {
                            if group.is_empty() {
                                CsvGroupKey::Null
                            } else {
                                let value = Self::parse_csv_i64_checked(group)
                                    .ok_or_else(|| {
                                        io::Error::new(
                                            io::ErrorKind::InvalidData,
                                            "invalid integer CSV group key",
                                        )
                                    })?;
                                CsvGroupKey::Int64(value)
                            }
                        }
                    };
                    if let Some((count, stats)) = groups.get_mut(&key) {
                        *count += 1;
                        for (stat, row_stat) in stats.iter_mut().zip(&row_stats[..columns.len()]) {
                            stat.merge(*row_stat);
                        }
                    } else {
                        groups.insert(key, (1, row_stats[..columns.len()].to_vec()));
                    }
                }
                Ok(groups)
            })
            .collect();
        let partials = match partials {
            Ok(partials) => partials,
            Err(_) => return Ok(None),
        };
        let mut merged = GroupMap::new();
        for partial in partials {
            for (key, (count, stats)) in partial {
                if let Some((merged_count, merged_stats)) = merged.get_mut(&key) {
                    *merged_count += count;
                    for (stat, partial_stat) in merged_stats.iter_mut().zip(stats) {
                        stat.merge(partial_stat);
                    }
                } else {
                    merged.insert(key, (count, stats));
                }
            }
        }
        let mut result = Vec::with_capacity(merged.len());
        for (key, (count, stats)) in merged {
            let group = match key {
                CsvGroupKey::Null => None,
                CsvGroupKey::Utf8(value) => Some(value),
                CsvGroupKey::Int64(value) => Some(value.to_le_bytes().to_vec()),
            };
            result.push((group, count, stats));
        }
        Ok(Some((group_key_type, result)))
    }

    /// Fused quote-free CSV scan for one or more scalar COUNT(DISTINCT col)
    /// expressions over string and integer columns. Nulls are ignored, and
    /// integer text is normalized before hashing so equivalent values such as
    /// `6` and `006` belong to the same SQL distinct value.
    pub(in crate::query::executor) fn try_fast_csv_distinct_counts(
        path: &str,
        options: &[(String, String)],
        columns: &[String],
    ) -> io::Result<Option<Vec<i64>>> {
        use arrow::datatypes::DataType;
        use rayon::prelude::*;

        if columns.is_empty() {
            return Ok(None);
        }
        let has_header = options
            .iter()
            .find(|(key, _)| key == "header")
            .map(|(_, value)| !matches!(value.to_lowercase().as_str(), "false" | "0"))
            .unwrap_or(true);
        let bad_line_policy = csv_bad_line_policy(options)?;
        if bad_line_policy == CsvBadLinePolicy::Warn {
            return Ok(None);
        }
        let file = std::fs::File::open(path)?;
        let buffer = if file.metadata()?.len() >= SEQUENTIAL_READ_THRESHOLD {
            CsvBuffer::Owned(read_file_sequential(path)?)
        } else {
            CsvBuffer::Mmap(unsafe { memmap2::Mmap::map(&file) }?)
        };
        let all_data = buffer.as_slice();
        let delimiter = options
            .iter()
            .find(|(key, _)| key == "delimiter" || key == "delim" || key == "sep")
            .map(|(_, value)| {
                if value.eq_ignore_ascii_case("auto") {
                    Self::sniff_csv_delimiter(all_data)
                } else {
                    value.chars().next().map(|value| value as u8).unwrap_or(b',')
                }
            })
            .unwrap_or(b',');
        let schema = Self::infer_csv_schema_fast(all_data, has_header, delimiter, 100)?;
        let mut targets = Vec::with_capacity(columns.len());
        for (output, column) in columns.iter().enumerate() {
            let Some(index) = schema.fields().iter().position(|field| field.name() == column) else {
                return Ok(None);
            };
            let key_type = match schema.field(index).data_type() {
                DataType::Utf8 => CsvDistinctKeyType::Utf8,
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                    CsvDistinctKeyType::Int64
                }
                _ => return Ok(None),
            };
            targets.push((index, output, key_type));
        }
        targets.sort_unstable_by_key(|(index, _, _)| *index);

        let data_start = if has_header {
            memchr::memchr(b'\n', all_data)
                .map(|index| index + 1)
                .unwrap_or(all_data.len())
        } else {
            0
        };
        let data = &all_data[data_start..];
        if data.is_empty() {
            return Ok(Some(vec![0; columns.len()]));
        }
        if memchr::memchr(b'"', data).is_some() {
            return Ok(None);
        }
        let first_line = data
            .split(|&byte| byte == b'\n')
            .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
            .find(|line| !line.is_empty())
            .unwrap_or(&[]);
        if first_line.is_empty() {
            return Ok(Some(vec![0; columns.len()]));
        }
        let column_count = memchr::memchr_iter(delimiter, first_line).count() + 1;

        let mut starts = vec![0usize];
        let thread_count = rayon::current_num_threads().clamp(1, 16);
        let chunk_size = data.len().div_ceil(thread_count);
        for thread in 1..thread_count {
            let approximate = thread * chunk_size;
            if approximate >= data.len() {
                break;
            }
            let start = memchr::memchr(b'\n', &data[approximate..])
                .map(|index| approximate + index + 1)
                .unwrap_or(data.len());
            if start != *starts.last().unwrap() {
                starts.push(start);
            }
        }
        starts.push(data.len());

        struct SendSlice(*const u8, usize);
        unsafe impl Send for SendSlice {}
        unsafe impl Sync for SendSlice {}
        let chunks = starts
            .windows(2)
            .map(|window| {
                let chunk = &data[window[0]..window[1]];
                SendSlice(chunk.as_ptr(), chunk.len())
            })
            .collect::<Vec<_>>();
        let partials: io::Result<Vec<Vec<ahash::AHashSet<CsvDistinctValue>>>> = chunks
            .par_iter()
            .map(|slice| {
                let chunk = unsafe { std::slice::from_raw_parts(slice.0, slice.1) };
                let mut sets = (0..columns.len())
                    .map(|_| ahash::AHashSet::new())
                    .collect::<Vec<_>>();
                let mut row_fields = vec![None; columns.len()];
                let mut line_start = 0usize;
                for line_end in memchr::memchr_iter(b'\n', chunk)
                    .chain(std::iter::once(chunk.len()))
                {
                    let raw_line = &chunk[line_start..line_end];
                    line_start = line_end.saturating_add(1);
                    let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
                    if line.is_empty() {
                        continue;
                    }
                    row_fields.fill(None);
                    let mut field_index = 0usize;
                    let mut field_start = 0usize;
                    let mut target_index = 0usize;
                    for field_end in memchr::memchr_iter(delimiter, line) {
                        while target_index < targets.len()
                            && targets[target_index].0 == field_index
                        {
                            row_fields[targets[target_index].1] =
                                Some(&line[field_start..field_end]);
                            target_index += 1;
                        }
                        field_index += 1;
                        field_start = field_end + 1;
                    }
                    while target_index < targets.len() && targets[target_index].0 == field_index {
                        row_fields[targets[target_index].1] = Some(&line[field_start..]);
                        target_index += 1;
                    }
                    if field_index + 1 != column_count {
                        if bad_line_policy == CsvBadLinePolicy::Error {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "CSV row has wrong field count",
                            ));
                        }
                        continue;
                    }
                    for &(_, output, key_type) in &targets {
                        let Some(field) = row_fields[output] else {
                            continue;
                        };
                        if field.is_empty() {
                            continue;
                        }
                        let value = match key_type {
                            CsvDistinctKeyType::Utf8 => {
                                std::str::from_utf8(field).map_err(|_| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "invalid UTF-8 CSV distinct value",
                                    )
                                })?;
                                CsvDistinctValue::Utf8(field.to_vec())
                            }
                            CsvDistinctKeyType::Int64 => {
                                let value = Self::parse_csv_i64_checked(field)
                                    .ok_or_else(|| {
                                        io::Error::new(
                                            io::ErrorKind::InvalidData,
                                            "invalid integer CSV distinct value",
                                        )
                                    })?;
                                CsvDistinctValue::Int64(value)
                            }
                        };
                        sets[output].insert(value);
                    }
                }
                Ok(sets)
            })
            .collect();
        let partials = match partials {
            Ok(partials) => partials,
            Err(_) => return Ok(None),
        };
        let mut merged = (0..columns.len())
            .map(|_| ahash::AHashSet::new())
            .collect::<Vec<_>>();
        for partial in partials {
            for (target, values) in merged.iter_mut().zip(partial) {
                target.extend(values);
            }
        }
        Ok(Some(
            merged.into_iter().map(|values| values.len() as i64).collect(),
        ))
    }

    fn scan_csv_numeric_chunk(
        data: &[u8],
        delimiter: u8,
        n_cols: usize,
        bad_line_policy: CsvBadLinePolicy,
        target_specs: &[(usize, usize, bool)],
        n_targets: usize,
        n_outputs: usize,
        predicate_specs: &[(usize, f64, f64)],
    ) -> io::Result<(i64, Vec<CsvNumericStats>)> {
        let mut rows = 0i64;
        let mut stats = vec![CsvNumericStats::new(true); n_outputs];
        let mut row_stats = vec![CsvNumericStats::new(true); n_targets];
        for &(_, output_index, is_int) in target_specs {
            if output_index < n_outputs {
                stats[output_index] = CsvNumericStats::new(is_int);
            }
            row_stats[output_index] = CsvNumericStats::new(is_int);
        }
        let mut line_start = 0usize;

        for newline in memchr::memchr_iter(b'\n', data) {
            let raw = &data[line_start..newline];
            line_start = newline + 1;
            let line = raw.strip_suffix(b"\r").unwrap_or(raw);
            if line.is_empty() {
                continue;
            }
            if Self::scan_csv_numeric_line(
                line,
                delimiter,
                n_cols,
                bad_line_policy,
                target_specs,
                &mut row_stats,
            )? && predicate_specs.iter().all(|&(index, low, high)| {
                let value = row_stats[index];
                value.count == 1 && value.min >= low && value.max <= high
            }) {
                rows += 1;
                for (stat, row_stat) in stats.iter_mut().zip(row_stats.iter().take(n_outputs)) {
                    stat.merge(*row_stat);
                }
            }
        }
        if line_start < data.len() {
            let raw = &data[line_start..];
            let line = raw.strip_suffix(b"\r").unwrap_or(raw);
            if !line.is_empty()
                && Self::scan_csv_numeric_line(
                    line,
                    delimiter,
                    n_cols,
                    bad_line_policy,
                    target_specs,
                    &mut row_stats,
                )?
                && predicate_specs.iter().all(|&(index, low, high)| {
                    let value = row_stats[index];
                    value.count == 1 && value.min >= low && value.max <= high
                })
            {
                rows += 1;
                for (stat, row_stat) in stats.iter_mut().zip(row_stats.iter().take(n_outputs)) {
                    stat.merge(*row_stat);
                }
            }
        }
        Ok((rows, stats))
    }

    fn scan_csv_numeric_line(
        line: &[u8],
        delimiter: u8,
        n_cols: usize,
        bad_line_policy: CsvBadLinePolicy,
        target_specs: &[(usize, usize, bool)],
        row_stats: &mut [CsvNumericStats],
    ) -> io::Result<bool> {
        for &(_, output_index, is_int) in target_specs {
            row_stats[output_index] = CsvNumericStats::new(is_int);
        }

        let mut field_index = 0usize;
        let mut field_start = 0usize;
        let mut target_index = 0usize;
        for field_end in memchr::memchr_iter(delimiter, line) {
            if target_index < target_specs.len() && target_specs[target_index].0 == field_index {
                let (_, output_index, is_int) = target_specs[target_index];
                Self::update_csv_numeric_stat(
                    &mut row_stats[output_index],
                    &line[field_start..field_end],
                    is_int,
                )?;
                target_index += 1;
            }
            field_index += 1;
            field_start = field_end + 1;
        }

        let actual_fields = field_index + 1;
        if actual_fields != n_cols {
            if bad_line_policy == CsvBadLinePolicy::Error {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "CSV row has wrong field count",
                ));
            }
            return Ok(false);
        }

        if target_index < target_specs.len() && target_specs[target_index].0 == field_index {
            let (_, output_index, is_int) = target_specs[target_index];
            Self::update_csv_numeric_stat(
                &mut row_stats[output_index],
                &line[field_start..],
                is_int,
            )?;
        }
        Ok(true)
    }

    #[inline]
    fn update_csv_numeric_stat(
        stat: &mut CsvNumericStats,
        field: &[u8],
        is_int: bool,
    ) -> io::Result<()> {
        if field.is_empty() {
            return Ok(());
        }
        if is_int {
            let value = Self::parse_csv_i64_checked(field)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid integer CSV field"))?;
            stat.add(value as f64);
        } else {
            let value = fast_float::parse::<f64, _>(field)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid float CSV field"))?;
            stat.add(value);
        }
        Ok(())
    }

    #[inline]
    fn parse_csv_i64_checked(field: &[u8]) -> Option<i64> {
        let (&first, rest) = field.split_first()?;
        let (negative, digits) = match first {
            b'-' => (true, rest),
            b'+' => (false, rest),
            _ => (false, field),
        };
        if digits.is_empty() {
            return None;
        }
        let mut value = 0u64;
        for &digit in digits {
            if !digit.is_ascii_digit() {
                return None;
            }
            value = value.checked_mul(10)?.checked_add((digit - b'0') as u64)?;
        }
        if negative {
            if value == 1u64 << 63 {
                Some(i64::MIN)
            } else if value <= i64::MAX as u64 {
                Some(-(value as i64))
            } else {
                None
            }
        } else {
            (value <= i64::MAX as u64).then_some(value as i64)
        }
    }

    /// Count non-empty newline-delimited data rows in *chunk* that match the
    /// schema shape. Returns an error when a row has the wrong field count and
    /// the policy is `Error` (mirrors the full parse's error semantics).
    ///
    /// The fast path only runs on quote-free data (a prior `memchr('"')` guard
    /// declines otherwise), so SIMD `memchr_iter` field counting is exact.
    fn count_csv_chunk_rows(
        data: &[u8],
        delimiter: u8,
        n_cols: usize,
        bad_line_policy: CsvBadLinePolicy,
    ) -> io::Result<usize> {
        let mut count = 0usize;
        let mut line_start = 0usize;
        for nl in memchr::memchr_iter(b'\n', data) {
            let raw = &data[line_start..nl];
            line_start = nl + 1;
            let line = if raw.last() == Some(&b'\r') {
                &raw[..raw.len() - 1]
            } else {
                raw
            };
            if line.is_empty() {
                continue;
            }
            // SIMD field count: field_count = delimiter_occurrences + 1.
            if memchr::memchr_iter(delimiter, line).count() + 1 != n_cols {
                if bad_line_policy == CsvBadLinePolicy::Error {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "CSV row has wrong field count",
                    ));
                }
                continue;
            }
            count += 1;
        }
        if line_start < data.len() {
            let raw = &data[line_start..];
            let line = if raw.last() == Some(&b'\r') {
                &raw[..raw.len() - 1]
            } else {
                raw
            };
            if !line.is_empty() {
                let actual_fields = memchr::memchr_iter(delimiter, line).count() + 1;
                if actual_fields != n_cols {
                    if bad_line_policy == CsvBadLinePolicy::Error {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "CSV row has wrong field count",
                        ));
                    }
                } else {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    pub(in crate::query::executor) fn looks_like_ndjson(bytes: &[u8]) -> bool {
        let mut checked = 0usize;
        let mut line_start = 0usize;
        for nl in memchr::memchr_iter(b'\n', bytes).take(16) {
            let raw = &bytes[line_start..nl];
            line_start = nl + 1;
            let line = Self::trim_ascii_json_line(raw);
            if line.is_empty() {
                continue;
            }
            if line.first() != Some(&b'{') || line.last() != Some(&b'}') {
                return false;
            }
            checked += 1;
            if checked >= 4 {
                return true;
            }
        }
        if checked == 0 && line_start < bytes.len() {
            let line = Self::trim_ascii_json_line(&bytes[line_start..]);
            checked = usize::from(
                !line.is_empty() && line.first() == Some(&b'{') && line.last() == Some(&b'}'),
            );
        }
        checked > 0 && memchr::memchr(b'\n', bytes).is_some()
    }

    pub(in crate::query::executor) fn trim_ascii_json_line(mut line: &[u8]) -> &[u8] {
        while line.first().is_some_and(|b| b.is_ascii_whitespace()) {
            line = &line[1..];
        }
        while line.last().is_some_and(|b| b.is_ascii_whitespace()) {
            line = &line[..line.len() - 1];
        }
        line
    }

    pub(in crate::query::executor) fn extract_json_numeric_filter(expr: &SqlExpr) -> Option<JsonNumericFilter> {
        let SqlExpr::BinaryOp { left, op, right } = expr else {
            return None;
        };
        let (col, val, flipped) = if let SqlExpr::Column(c) = left.as_ref() {
            (c.as_str(), Self::literal_to_f64(right)?, false)
        } else if let SqlExpr::Column(c) = right.as_ref() {
            (c.as_str(), Self::literal_to_f64(left)?, true)
        } else {
            return None;
        };
        if !matches!(
            op,
            BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::Le
                | BinaryOperator::Gt
                | BinaryOperator::Ge
        ) {
            return None;
        }
        let col = col.trim_matches('"');
        let col = col.rsplit('.').next().unwrap_or(col);
        let mut key = Vec::with_capacity(col.len() + 2);
        key.push(b'"');
        key.extend_from_slice(col.as_bytes());
        key.push(b'"');
        Some(JsonNumericFilter {
            key,
            op: op.clone(),
            flipped,
            val_f64: val,
        })
    }

    pub(in crate::query::executor) fn count_json_chunk_rows(data: &[u8], filter: Option<&JsonNumericFilter>) -> usize {
        let mut count = 0usize;
        let mut line_start = 0usize;
        for nl in memchr::memchr_iter(b'\n', data) {
            let line = Self::trim_ascii_json_line(&data[line_start..nl]);
            line_start = nl + 1;
            if Self::json_line_matches_filter(line, filter) {
                count += 1;
            }
        }
        if line_start < data.len() {
            let line = Self::trim_ascii_json_line(&data[line_start..]);
            if Self::json_line_matches_filter(line, filter) {
                count += 1;
            }
        }
        count
    }

    pub(in crate::query::executor) fn json_line_matches_filter(
        line: &[u8],
        filter: Option<&JsonNumericFilter>,
    ) -> bool {
        if line.is_empty() {
            return false;
        }
        let Some(filter) = filter else {
            return true;
        };
        let Some(value) = Self::json_line_numeric_value(line, &filter.key) else {
            return false;
        };
        let (lhs, rhs) = if filter.flipped {
            (filter.val_f64, value)
        } else {
            (value, filter.val_f64)
        };
        match filter.op {
            BinaryOperator::Eq => lhs == rhs,
            BinaryOperator::NotEq => lhs != rhs,
            BinaryOperator::Lt => lhs < rhs,
            BinaryOperator::Le => lhs <= rhs,
            BinaryOperator::Gt => lhs > rhs,
            BinaryOperator::Ge => lhs >= rhs,
            _ => false,
        }
    }

    pub(in crate::query::executor) fn json_line_numeric_value(line: &[u8], key: &[u8]) -> Option<f64> {
        let mut search_from = 0usize;
        while search_from < line.len() {
            let pos = memchr::memmem::find(&line[search_from..], key)?;
            let mut i = search_from + pos + key.len();
            while i < line.len() && line[i].is_ascii_whitespace() {
                i += 1;
            }
            if i >= line.len() || line[i] != b':' {
                search_from += pos + key.len();
                continue;
            }
            i += 1;
            while i < line.len() && line[i].is_ascii_whitespace() {
                i += 1;
            }
            let quoted = i < line.len() && line[i] == b'"';
            if quoted {
                i += 1;
            }
            let mut end = i;
            while end < line.len() {
                let b = line[end];
                if quoted {
                    if b == b'"' {
                        break;
                    }
                } else if b == b',' || b == b'}' || b == b']' || b.is_ascii_whitespace() {
                    break;
                }
                end += 1;
            }
            if end > i {
                return fast_float::parse::<f64, _>(&line[i..end]).ok();
            }
            return None;
        }
        None
    }

    pub(in crate::query::executor) fn try_columns_format_fast(content: &str) -> io::Result<Option<RecordBatch>> {
        use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType as ArrowDT, Field, Schema};
        use serde_json::value::RawValue;

        // Step 1: parse outer map lazily — each column value stays as unparsed raw JSON.
        // std::HashMap is required here; serde_json cannot deserialize into AHashMap<K,V,S>.
        let outer: std::collections::HashMap<String, Box<RawValue>> =
            match serde_json::from_str(content) {
                Ok(m) => m,
                Err(_) => return Ok(None),
            };
        if outer.is_empty() {
            return Ok(None);
        }

        // Confirm "columns" format: each value must start with '{' (it's a nested object)
        let first_raw = outer.values().next().unwrap().get().trim_start();
        if !first_raw.starts_with('{') {
            return Ok(None); // index/split/records format — fall through to slow path
        }

        // Step 2+3+4: per column — detect type, parse typed, sort, build Arrow array
        let col_names: Vec<String> = outer.keys().cloned().collect();
        let mut fields: Vec<Field> = Vec::with_capacity(col_names.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_names.len());

        for col in &col_names {
            let raw_col = outer[col].get();

            // Peek at first non-null value's raw bytes to determine type
            let first_token = Self::peek_first_json_value(raw_col);
            let col_type = match first_token {
                Some(t) if t.starts_with('"') => 3u8, // String
                Some("true") | Some("false") => 2u8,  // Bool
                Some(t) if t.contains('.') || t.contains('e') || t.contains('E') => 1u8, // Float
                _ => 0u8,                             // Int (default)
            };

            match col_type {
                0 => {
                    // Integer column: HashMap<u64, Option<i64>> — no String key allocs
                    let map: std::collections::HashMap<u64, Option<i64>> =
                        serde_json::from_str(raw_col).map_err(|e| {
                            io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                        })?;
                    let mut entries: Vec<(u64, Option<i64>)> = map.into_iter().collect();
                    entries.sort_unstable_by_key(|(k, _)| *k);
                    let data: Vec<Option<i64>> = entries.into_iter().map(|(_, v)| v).collect();
                    fields.push(Field::new(col, ArrowDT::Int64, true));
                    arrays.push(Arc::new(Int64Array::from(data)));
                }
                1 => {
                    // Float column
                    let map: std::collections::HashMap<u64, Option<f64>> =
                        serde_json::from_str(raw_col).map_err(|e| {
                            io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                        })?;
                    let mut entries: Vec<(u64, Option<f64>)> = map.into_iter().collect();
                    entries.sort_unstable_by_key(|(k, _)| *k);
                    let data: Vec<Option<f64>> = entries.into_iter().map(|(_, v)| v).collect();
                    fields.push(Field::new(col, ArrowDT::Float64, true));
                    arrays.push(Arc::new(Float64Array::from(data)));
                }
                2 => {
                    // Bool column
                    let map: std::collections::HashMap<u64, Option<bool>> =
                        serde_json::from_str(raw_col).map_err(|e| {
                            io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                        })?;
                    let mut entries: Vec<(u64, Option<bool>)> = map.into_iter().collect();
                    entries.sort_unstable_by_key(|(k, _)| *k);
                    let data: Vec<Option<bool>> = entries.into_iter().map(|(_, v)| v).collect();
                    fields.push(Field::new(col, ArrowDT::Boolean, true));
                    arrays.push(Arc::new(BooleanArray::from(data)));
                }
                _ => {
                    // String column: HashMap<u64, Option<String>> — no String key allocs
                    let map: std::collections::HashMap<u64, Option<String>> =
                        serde_json::from_str(raw_col).map_err(|e| {
                            io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                        })?;
                    let mut entries: Vec<(u64, Option<String>)> = map.into_iter().collect();
                    entries.sort_unstable_by_key(|(k, _)| *k);
                    let data: Vec<Option<String>> = entries.into_iter().map(|(_, v)| v).collect();
                    fields.push(Field::new(col, ArrowDT::Utf8, true));
                    arrays.push(Arc::new(StringArray::from(data)));
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(Some(batch))
    }

    pub(in crate::query::executor) fn peek_first_json_value(obj_json: &str) -> Option<&str> {
        let bytes = obj_json.as_bytes();
        let mut i = 0;
        // Skip opening '{'
        while i < bytes.len() && bytes[i] != b'{' {
            i += 1;
        }
        i += 1;
        // Find first value (skip key, colon, then read value token)
        'outer: loop {
            // skip whitespace / comma
            while i < bytes.len()
                && (bytes[i] == b' '
                    || bytes[i] == b'\n'
                    || bytes[i] == b'\r'
                    || bytes[i] == b'\t'
                    || bytes[i] == b',')
            {
                i += 1;
            }
            if i >= bytes.len() || bytes[i] == b'}' {
                break;
            }
            // skip key string
            if bytes[i] == b'"' {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1; // closing quote
            }
            // skip whitespace + colon
            while i < bytes.len()
                && (bytes[i] == b' '
                    || bytes[i] == b'\n'
                    || bytes[i] == b'\r'
                    || bytes[i] == b'\t'
                    || bytes[i] == b':')
            {
                i += 1;
            }
            if i >= bytes.len() {
                break;
            }
            // read value token
            let start = i;
            let end = match bytes[i] {
                b'"' => {
                    i += 1;
                    while i < bytes.len() && bytes[i] != b'"' {
                        if bytes[i] == b'\\' {
                            i += 1;
                        }
                        i += 1;
                    }
                    i + 1
                }
                b'n' => {
                    // null — skip and try next entry
                    i += 4;
                    continue 'outer;
                }
                _ => {
                    while i < bytes.len()
                        && bytes[i] != b','
                        && bytes[i] != b'}'
                        && bytes[i] != b' '
                        && bytes[i] != b'\n'
                    {
                        i += 1;
                    }
                    i
                }
            };
            if start < end && end <= bytes.len() {
                return Some(&obj_json[start..end]);
            }
            break;
        }
        None
    }

    pub(in crate::query::executor) fn json_value_to_batch(value: serde_json::Value) -> io::Result<RecordBatch> {
        use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType as ArrowDT, Field, Schema};

        let err = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());

        match value {
            // ── Array of records ─────────────────────────────────────────────
            serde_json::Value::Array(records) => {
                if records.is_empty() {
                    return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
                }
                // Collect column names from first record
                let col_names: Vec<String> = if let serde_json::Value::Object(ref m) = records[0] {
                    m.keys().cloned().collect()
                } else {
                    return Err(err("Expected array of objects"));
                };
                let n = records.len();
                let mut fields = Vec::with_capacity(col_names.len());
                let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_names.len());
                for col in &col_names {
                    // Detect type from first non-null value
                    let first = records.iter().find_map(|r| {
                        r.get(col)
                            .and_then(|v| if v.is_null() { None } else { Some(v) })
                    });
                    match first {
                        Some(serde_json::Value::Bool(_)) => {
                            let data: Vec<Option<bool>> = records
                                .iter()
                                .map(|r| r.get(col).and_then(|v| v.as_bool()))
                                .collect();
                            fields.push(Field::new(col, ArrowDT::Boolean, true));
                            arrays.push(Arc::new(BooleanArray::from(data)));
                        }
                        Some(serde_json::Value::Number(num))
                            if num.as_i64().is_some()
                                && !num.as_f64().map(|f| f.fract() != 0.0).unwrap_or(false) =>
                        {
                            let data: Vec<Option<i64>> = records
                                .iter()
                                .map(|r| r.get(col).and_then(|v| v.as_i64()))
                                .collect();
                            fields.push(Field::new(col, ArrowDT::Int64, true));
                            arrays.push(Arc::new(Int64Array::from(data)));
                        }
                        Some(serde_json::Value::Number(_)) => {
                            let data: Vec<Option<f64>> = records
                                .iter()
                                .map(|r| r.get(col).and_then(|v| v.as_f64()))
                                .collect();
                            fields.push(Field::new(col, ArrowDT::Float64, true));
                            arrays.push(Arc::new(Float64Array::from(data)));
                        }
                        _ => {
                            let data: Vec<Option<&str>> = records
                                .iter()
                                .map(|r| r.get(col).and_then(|v| v.as_str()))
                                .collect();
                            fields.push(Field::new(col, ArrowDT::Utf8, true));
                            arrays.push(Arc::new(StringArray::from(data)));
                        }
                    }
                    let _ = n;
                }
                let schema = Arc::new(Schema::new(fields));
                RecordBatch::try_new(schema, arrays)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
            }

            serde_json::Value::Object(map) => {
                // ── pandas "split": {"columns":[...], "data":[[...],...]} ──────
                if let (
                    Some(serde_json::Value::Array(cols)),
                    Some(serde_json::Value::Array(data)),
                ) = (map.get("columns").cloned(), map.get("data").cloned())
                {
                    let col_names: Vec<String> = cols
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    let n = data.len();
                    let ncols = col_names.len();
                    // Transpose: row-major data → column vecs of serde_json::Value
                    let mut cols_data: Vec<Vec<serde_json::Value>> =
                        vec![Vec::with_capacity(n); ncols];
                    for row in &data {
                        if let serde_json::Value::Array(cells) = row {
                            for ci in 0..ncols {
                                cols_data[ci].push(
                                    cells.get(ci).cloned().unwrap_or(serde_json::Value::Null),
                                );
                            }
                        } else {
                            for ci in 0..ncols {
                                cols_data[ci].push(serde_json::Value::Null);
                            }
                        }
                    }
                    let col_refs: Vec<Vec<&serde_json::Value>> =
                        cols_data.iter().map(|c| c.iter().collect()).collect();
                    return Self::column_vecs_to_batch(col_names, col_refs);
                }

                // ── pandas "columns": {"col": {"0": v, ...}} ──────────────────
                let is_columns = !map.is_empty()
                    && map.values().all(|v| {
                        matches!(v, serde_json::Value::Object(inner)
                        if !inner.is_empty() && inner.keys().all(|k| k.parse::<u64>().is_ok()))
                    });
                if is_columns {
                    let first = map.values().next().unwrap();
                    let mut indices: Vec<u64> = if let serde_json::Value::Object(inner) = first {
                        inner.keys().filter_map(|k| k.parse().ok()).collect()
                    } else {
                        vec![]
                    };
                    indices.sort_unstable();

                    let col_names: Vec<String> = map.keys().cloned().collect();
                    let null = serde_json::Value::Null;
                    let col_vecs: Vec<Vec<&serde_json::Value>> = col_names
                        .iter()
                        .map(|col| {
                            if let Some(serde_json::Value::Object(inner)) = map.get(col) {
                                indices
                                    .iter()
                                    .map(|i| inner.get(&i.to_string()).unwrap_or(&null))
                                    .collect()
                            } else {
                                vec![]
                            }
                        })
                        .collect();
                    return Self::column_vecs_to_batch(col_names, col_vecs);
                }

                // ── pandas "index": {"0": {"col": v}, ...} ────────────────────
                let is_index = !map.is_empty()
                    && map.keys().all(|k| k.parse::<u64>().is_ok())
                    && map
                        .values()
                        .all(|v| matches!(v, serde_json::Value::Object(_)));
                if is_index {
                    let mut entries: Vec<(u64, serde_json::Value)> = map
                        .into_iter()
                        .filter_map(|(k, v)| k.parse::<u64>().ok().map(|n| (n, v)))
                        .collect();
                    entries.sort_by_key(|(n, _)| *n);
                    let records: Vec<serde_json::Value> =
                        entries.into_iter().map(|(_, v)| v).collect();
                    return Self::json_value_to_batch(serde_json::Value::Array(records));
                }

                // ── Single record ──────────────────────────────────────────────
                Self::json_value_to_batch(serde_json::Value::Array(vec![
                    serde_json::Value::Object(map),
                ]))
            }
            _ => Err(err("Unsupported top-level JSON type")),
        }
    }

    pub(in crate::query::executor) fn column_vecs_to_batch(
        col_names: Vec<String>,
        cols: Vec<Vec<&serde_json::Value>>,
    ) -> io::Result<RecordBatch> {
        use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType as ArrowDT, Field, Schema};

        let mut fields = Vec::with_capacity(col_names.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_names.len());

        for (col, values) in col_names.iter().zip(cols.iter()) {
            let first = values.iter().find(|v| !v.is_null()).copied();
            match first {
                Some(serde_json::Value::Bool(_)) => {
                    let data: Vec<Option<bool>> = values.iter().map(|v| v.as_bool()).collect();
                    fields.push(Field::new(col, ArrowDT::Boolean, true));
                    arrays.push(Arc::new(BooleanArray::from(data)));
                }
                Some(serde_json::Value::Number(num))
                    if num.as_i64().is_some()
                        && !num.as_f64().map(|f| f.fract() != 0.0).unwrap_or(false) =>
                {
                    let data: Vec<Option<i64>> = values.iter().map(|v| v.as_i64()).collect();
                    fields.push(Field::new(col, ArrowDT::Int64, true));
                    arrays.push(Arc::new(Int64Array::from(data)));
                }
                Some(serde_json::Value::Number(_)) => {
                    let data: Vec<Option<f64>> = values.iter().map(|v| v.as_f64()).collect();
                    fields.push(Field::new(col, ArrowDT::Float64, true));
                    arrays.push(Arc::new(Float64Array::from(data)));
                }
                _ => {
                    let data: Vec<Option<&str>> = values.iter().map(|v| v.as_str()).collect();
                    fields.push(Field::new(col, ArrowDT::Utf8, true));
                    arrays.push(Arc::new(StringArray::from(data)));
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    pub(in crate::query::executor) fn normalize_json_to_ndjson(content: &str) -> io::Result<String> {
        let trimmed = content.trim();
        let value: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => return Ok(content.to_owned()),
        };
        // Re-use the direct converter path, then serialize each row as NDJSON
        // (COPY path only; read_json_to_batch uses json_value_to_batch directly)
        match &value {
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                let batch = Self::json_value_to_batch(value)?;
                // Serialize batch rows back to NDJSON for the COPY insert pipeline
                let mut out = String::new();
                let schema = batch.schema();
                for row_i in 0..batch.num_rows() {
                    let mut obj = serde_json::Map::with_capacity(schema.fields().len());
                    for (col_i, field) in schema.fields().iter().enumerate() {
                        let col = batch.column(col_i);
                        let val = Self::arrow_value_at_col(col, row_i);
                        let jval = match val {
                            crate::data::Value::Int64(n) => serde_json::Value::Number(n.into()),
                            crate::data::Value::Int32(n) => {
                                serde_json::Value::Number((n as i64).into())
                            }
                            crate::data::Value::Float64(f) => serde_json::json!(f),
                            crate::data::Value::Float32(f) => serde_json::json!(f as f64),
                            crate::data::Value::String(s) => serde_json::Value::String(s),
                            crate::data::Value::Bool(b) => serde_json::Value::Bool(b),
                            _ => serde_json::Value::Null,
                        };
                        obj.insert(field.name().clone(), jval);
                    }
                    out.push_str(
                        &serde_json::to_string(&obj)
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?,
                    );
                    out.push('\n');
                }
                Ok(out)
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unsupported JSON type",
            )),
        }
    }

    pub(in crate::query::executor) fn read_parquet_to_batch(
        path: &str,
        options: &[(String, String)],
        row_limit: Option<usize>,
    ) -> io::Result<RecordBatch> {
        use parquet::arrow::arrow_reader::{
            ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions,
            ParquetRecordBatchReaderBuilder, RowFilter,
        };
        use parquet::arrow::ProjectionMask;
        use parquet::file::metadata::PageIndexPolicy;
        use rayon::prelude::*;

        let file = std::fs::File::open(path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Cannot open Parquet file '{}': {}", path, e),
            )
        })?;
        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("mmap error on '{}': {}", path, e),
            )
        })?;
        let shared = Arc::new(bytes::Bytes::from_owner(mmap));

        // Parse metadata ONCE; all parallel readers share it via clone() (cheap Arc increments).
        // Bytes implements ChunkReader; clone() is O(1) (just increments the Arc refcount).
        let has_filter = options.iter().any(|(key, _)| key == "filter");
        let reader_options =
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::from(has_filter));
        let arrow_meta = ArrowReaderMetadata::load(&(*shared).clone(), reader_options)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let filter_info = options
            .iter()
            .find(|(key, _)| key == "filter")
            .and_then(|(_, value)| parse_pushdown_filter(value, arrow_meta.schema()))
            .filter(|filter| {
                Self::parquet_filter_type_supported(
                    arrow_meta.schema().field(filter.col_idx).data_type(),
                )
            });
        let projection_names = options
            .iter()
            .find(|(key, _)| key == "columns")
            .map(|(_, value)| {
                value
                    .split('\u{1f}')
                    .filter(|name| !name.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|names| {
                !names.is_empty()
                    && names
                        .iter()
                        .all(|name| arrow_meta.schema().index_of(name).is_ok())
            });

        // On-demand read: stop decoding once `row_limit` rows are collected, so a
        // multi-row-group file is not fully decoded.  Only valid without a filter —
        // a filter must observe every row before LIMIT is applied, which is exactly
        // the simple-LIMIT case that triggers this path.
        if let Some(n) = row_limit {
            if filter_info.is_none() {
                return Self::read_parquet_limited(
                    shared.as_ref(),
                    &arrow_meta,
                    projection_names.as_deref(),
                    n,
                );
            }
        }

        // Query-aware path: decode only referenced columns and evaluate simple numeric
        // predicates inside the Parquet reader. The predicate column is cached by the
        // reader when it is also part of the output projection.
        if filter_info.is_some() || projection_names.is_some() {
            let parquet_schema = arrow_meta.parquet_schema().clone();
            let projection = projection_names.as_ref().map(|names| {
                ProjectionMask::columns(&parquet_schema, names.iter().map(String::as_str))
            });
            let filter_projection = filter_info.map(|filter| {
                let filter_name = arrow_meta.schema().field(filter.col_idx).name();
                (
                    ProjectionMask::columns(&parquet_schema, [filter_name.as_str()]),
                    filter,
                )
            });
            let n_groups = arrow_meta.metadata().num_row_groups();

            if n_groups > 1 {
                let row_groups: Vec<usize> = (0..n_groups).collect();
                let group_chunk = ((n_groups + rayon::current_num_threads() - 1)
                    / rayon::current_num_threads())
                .max(1);
                let row_group_chunks: Vec<Vec<usize>> = row_groups
                    .chunks(group_chunk)
                    .map(<[usize]>::to_vec)
                    .collect();
                let batches: Vec<io::Result<RecordBatch>> = row_group_chunks
                    .into_par_iter()
                    .map(|row_groups| {
                        let rows = row_groups
                            .iter()
                            .map(|row_group| {
                                arrow_meta.metadata().row_group(*row_group).num_rows() as usize
                            })
                            .sum::<usize>();
                        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                            (*shared).clone(),
                            arrow_meta.clone(),
                        )
                        .with_row_groups(row_groups)
                        .with_batch_size(rows.max(1));
                        if let Some(mask) = projection.as_ref() {
                            builder = builder.with_projection(mask.clone());
                        }
                        if let Some((mask, filter)) = filter_projection.as_ref() {
                            let filter = *filter;
                            let predicate =
                                ArrowPredicateFn::new(mask.clone(), move |batch: RecordBatch| {
                                    Self::parquet_numeric_filter(batch.column(0), filter)
                                });
                            builder =
                                builder.with_row_filter(RowFilter::new(vec![Box::new(predicate)]));
                        }
                        let reader = builder.build().map_err(|error| {
                            io::Error::new(io::ErrorKind::Other, error.to_string())
                        })?;
                        let batches = reader.collect::<Result<Vec<_>, _>>().map_err(|error| {
                            io::Error::new(io::ErrorKind::Other, error.to_string())
                        })?;
                        Self::merge_record_batches(batches)
                    })
                    .collect();
                return Self::merge_record_batches(
                    batches.into_iter().collect::<io::Result<Vec<_>>>()?,
                );
            }

            let mut builder =
                ParquetRecordBatchReaderBuilder::new_with_metadata((*shared).clone(), arrow_meta)
                    .with_batch_size(65_536);

            if let Some(mask) = projection {
                builder = builder.with_projection(mask);
            }
            if let Some((mask, filter)) = filter_projection {
                let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                    Self::parquet_numeric_filter(batch.column(0), filter)
                });
                builder = builder.with_row_filter(RowFilter::new(vec![Box::new(predicate)]));
            }

            let reader = builder
                .build()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            let batches = reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            return Self::merge_record_batches(batches);
        }

        let n_groups = arrow_meta.metadata().num_row_groups();
        let total_rows = arrow_meta.metadata().file_metadata().num_rows() as usize;

        if n_groups <= 1 {
            // Single row group: read columns in parallel for max decompression throughput.
            // Each column reader shares the same mmap bytes (O(1) Arc clone).
            // We cap at min(n_cols, n_threads) to bound metadata-parse overhead.
            let n_threads = rayon::current_num_threads();
            let schema = arrow_meta.schema().clone();
            let n_cols = schema.fields().len();

            if n_cols <= 1 {
                // Trivial case: single column, build directly.
                let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                    (*shared).clone(),
                    arrow_meta,
                )
                .with_batch_size(total_rows.max(1))
                .build()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                let batches: Vec<RecordBatch> = reader
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                return Self::merge_record_batches(batches);
            }

            // Parquet schema descriptor (needed for ProjectionMask::leaves).
            let parquet_schema = arrow_meta.parquet_schema().clone();

            // Group columns into at most n_threads buckets.
            let bucket_size = ((n_cols + n_threads - 1) / n_threads).max(1);
            let col_buckets: Vec<Vec<usize>> = (0..n_cols)
                .collect::<Vec<_>>()
                .chunks(bucket_size)
                .map(|c| c.to_vec())
                .collect();

            let bucket_results: Vec<io::Result<RecordBatch>> = col_buckets
                .into_par_iter()
                .map(|col_idxs| {
                    // new_with_metadata: reuses pre-parsed metadata (cheap clone — all Arc internals).
                    let b = ParquetRecordBatchReaderBuilder::new_with_metadata(
                        (*shared).clone(),
                        arrow_meta.clone(),
                    );
                    let mask = ProjectionMask::leaves(&parquet_schema, col_idxs);
                    let reader = b
                        .with_batch_size(total_rows.max(1))
                        .with_projection(mask)
                        .build()
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    let batches: Vec<RecordBatch> = reader
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    Self::merge_record_batches(batches)
                })
                .collect();

            // Reassemble columns in original order
            let sub_batches: Vec<RecordBatch> =
                bucket_results.into_iter().collect::<io::Result<Vec<_>>>()?;

            // Stitch columns from sub-batches back into one RecordBatch
            let mut all_arrays: Vec<(usize, arrow::array::ArrayRef)> = Vec::with_capacity(n_cols);
            let mut col_written = 0usize;
            for sb in &sub_batches {
                for ci in 0..sb.num_columns() {
                    all_arrays.push((col_written + ci, sb.column(ci).clone()));
                }
                col_written += sb.num_columns();
            }
            all_arrays.sort_by_key(|(i, _)| *i);
            let arrays: Vec<arrow::array::ArrayRef> =
                all_arrays.into_iter().map(|(_, a)| a).collect();
            return RecordBatch::try_new(schema, arrays)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()));
        }

        // Multiple row groups: decode each in parallel, sharing pre-parsed metadata.
        let batches: Vec<io::Result<RecordBatch>> = (0..n_groups)
            .into_par_iter()
            .map(|rg| {
                let b = ParquetRecordBatchReaderBuilder::new_with_metadata(
                    (*shared).clone(),
                    arrow_meta.clone(),
                );

                let rows_in_group = b.metadata().row_group(rg).num_rows() as usize;
                let reader = b
                    .with_row_groups(vec![rg])
                    .with_batch_size(rows_in_group.max(1))
                    .build()
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                let sub: Vec<RecordBatch> = reader
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                Self::merge_record_batches(sub)
            })
            .collect();

        let all: Vec<RecordBatch> = batches.into_iter().collect::<io::Result<Vec<_>>>()?;
        Self::merge_record_batches(all)
    }

    /// Sequential on-demand Parquet read: decode only the first `n` rows (bounded
    /// batch), honoring an optional column projection, then merge and return.
    fn read_parquet_limited(
        shared: &bytes::Bytes,
        arrow_meta: &parquet::arrow::arrow_reader::ArrowReaderMetadata,
        projection_names: Option<&[String]>,
        n: usize,
    ) -> io::Result<RecordBatch> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::arrow::ProjectionMask;

        let schema: arrow::datatypes::SchemaRef = match projection_names {
            Some(names) if !names.is_empty() => {
                let full = arrow_meta.schema();
                let fields: Vec<arrow::datatypes::Field> = names
                    .iter()
                    .filter_map(|name| {
                        full.index_of(name)
                            .ok()
                            .map(|i| full.field(i).as_ref().clone())
                    })
                    .collect();
                if fields.is_empty() {
                    full.clone()
                } else {
                    Arc::new(arrow::datatypes::Schema::new(fields))
                }
            }
            _ => arrow_meta.schema().clone(),
        };

        if n == 0 {
            return Ok(RecordBatch::new_empty(schema));
        }

        let parquet_schema = arrow_meta.parquet_schema().clone();
        let batch_size = n.min(8192).max(1);
        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            (*shared).clone(),
            arrow_meta.clone(),
        )
        .with_batch_size(batch_size);
        if let Some(names) = projection_names {
            if !names.is_empty() {
                let mask =
                    ProjectionMask::columns(&parquet_schema, names.iter().map(String::as_str));
                builder = builder.with_projection(mask);
            }
        }
        let reader = builder
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut collected: Vec<RecordBatch> = Vec::new();
        let mut remaining = n;
        for batch in reader {
            let batch =
                batch.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            if batch.num_rows() == 0 {
                continue;
            }
            if batch.num_rows() >= remaining {
                collected.push(batch.slice(0, remaining));
                remaining = 0;
                break;
            }
            remaining -= batch.num_rows();
            collected.push(batch);
        }
        Self::merge_record_batches(collected)
    }

    pub(in crate::query::executor) fn try_fast_parquet_count(
        path: &str,
        filter: Option<&str>,
    ) -> io::Result<Option<i64>> {
        use parquet::arrow::arrow_reader::{
            ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
        };
        use parquet::arrow::ProjectionMask;
        use rayon::prelude::*;

        let file = std::fs::File::open(path).map_err(|error| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Cannot open Parquet file '{}': {}", path, error),
            )
        })?;
        let mmap = unsafe { memmap2::Mmap::map(&file) }
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error.to_string()))?;
        let input = bytes::Bytes::from_owner(mmap);
        let options = ArrowReaderOptions::new();
        let metadata = ArrowReaderMetadata::load(&input, options)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;

        let Some(filter_text) = filter else {
            return Ok(Some(metadata.metadata().file_metadata().num_rows() as i64));
        };
        let Some(filter) = parse_pushdown_filter(filter_text, metadata.schema()) else {
            return Ok(None);
        };
        if !Self::parquet_filter_type_supported(metadata.schema().field(filter.col_idx).data_type())
        {
            return Ok(None);
        }
        let filter_name = metadata.schema().field(filter.col_idx).name().clone();
        let parquet_schema = metadata.parquet_schema().clone();
        let mask = ProjectionMask::columns(&parquet_schema, [filter_name.as_str()]);
        let row_groups = metadata.metadata().num_row_groups();
        let counts: io::Result<Vec<i64>> = (0..row_groups)
            .into_par_iter()
            .map(|row_group| {
                let rows = metadata.metadata().row_group(row_group).num_rows() as usize;
                let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                    input.clone(),
                    metadata.clone(),
                )
                .with_row_groups(vec![row_group])
                .with_batch_size(rows.max(1))
                .with_projection(mask.clone())
                .build()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;

                let mut count = 0i64;
                for batch in reader {
                    let batch = batch.map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                    })?;
                    count += Self::parquet_numeric_filter(batch.column(0), filter)
                        .map_err(|error| {
                            io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                        })?
                        .true_count() as i64;
                }
                Ok(count)
            })
            .collect();
        Ok(Some(counts?.into_iter().sum()))
    }

    pub(in crate::query::executor) fn parquet_filter_type_supported(data_type: &arrow::datatypes::DataType) -> bool {
        matches!(
            data_type,
            arrow::datatypes::DataType::Float32 | arrow::datatypes::DataType::Float64
        )
    }

    pub(in crate::query::executor) fn parquet_numeric_filter(
        array: &ArrayRef,
        filter: PushdownFilter,
    ) -> arrow::error::Result<BooleanArray> {
        if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
            let scalar = Float64Array::new_scalar(filter.val_f64);
            return match filter.op {
                b'>' if filter.op_eq => cmp::gt_eq(values, &scalar),
                b'<' if filter.op_eq => cmp::lt_eq(values, &scalar),
                b'>' => cmp::gt(values, &scalar),
                b'<' => cmp::lt(values, &scalar),
                b'=' => cmp::eq(values, &scalar),
                b'!' => cmp::neq(values, &scalar),
                _ => Ok(BooleanArray::from_iter(std::iter::repeat_n(
                    Some(true),
                    array.len(),
                ))),
            };
        }

        macro_rules! primitive_mask {
            ($array_type:ty) => {
                if let Some(values) = array.as_any().downcast_ref::<$array_type>() {
                    return Ok(BooleanArray::from_iter((0..values.len()).map(|index| {
                        Some(!values.is_null(index) && filter.matches(values.value(index) as f64))
                    })));
                }
            };
        }

        primitive_mask!(arrow::array::Int8Array);
        primitive_mask!(arrow::array::Int16Array);
        primitive_mask!(arrow::array::Int32Array);
        primitive_mask!(arrow::array::Int64Array);
        primitive_mask!(arrow::array::UInt8Array);
        primitive_mask!(arrow::array::UInt16Array);
        primitive_mask!(arrow::array::UInt32Array);
        primitive_mask!(arrow::array::UInt64Array);
        primitive_mask!(arrow::array::Float32Array);

        // Unsupported physical types must remain in the generic SQL filter path.
        Ok(BooleanArray::from_iter(std::iter::repeat_n(
            Some(true),
            array.len(),
        )))
    }

    pub(in crate::query::executor) fn merge_record_batches(batches: Vec<RecordBatch>) -> io::Result<RecordBatch> {
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(
                arrow::datatypes::Schema::empty(),
            )));
        }
        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }
        let schema = batches[0].schema();
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        arrow::compute::concat_batches(&schema, refs).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Concat batches error: {}", e))
        })
    }

    pub(in crate::query::executor) fn for_each_import_batch<F>(
        file_path: &str,
        format: &str,
        options: &[(String, String)],
        mut visit: F,
    ) -> io::Result<()>
    where
        F: FnMut(RecordBatch) -> io::Result<()>,
    {
        match format.to_uppercase().as_str() {
            "CSV" | "TSV" => {
                if csv_bad_line_policy(options)? != CsvBadLinePolicy::Error {
                    let batch = Self::read_csv_to_batch(file_path, options, None)?;
                    if batch.num_rows() > 0 {
                        visit(batch)?;
                    }
                    return Ok(());
                }
                let has_header = options
                    .iter()
                    .find(|(key, _)| key == "header")
                    .map(|(_, value)| !matches!(value.to_lowercase().as_str(), "false" | "0"))
                    .unwrap_or(true);
                let delimiter = options
                    .iter()
                    .find(|(key, _)| key == "delimiter" || key == "delim" || key == "sep")
                    .and_then(|(_, value)| value.as_bytes().first().copied())
                    .unwrap_or(if format.eq_ignore_ascii_case("TSV") {
                        b'\t'
                    } else {
                        b','
                    });

                let file = std::fs::File::open(file_path).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("Cannot open CSV file '{}': {}", file_path, error),
                    )
                })?;
                let mmap = unsafe { memmap2::Mmap::map(&file) }
                    .map_err(|error| io::Error::new(io::ErrorKind::Other, error.to_string()))?;
                let schema = Arc::new(Self::infer_csv_schema_fast(
                    &mmap, has_header, delimiter, 100,
                )?);
                drop(mmap);
                drop(file);

                let input =
                    std::io::BufReader::with_capacity(1024 * 1024, std::fs::File::open(file_path)?);
                let reader = arrow::csv::ReaderBuilder::new(schema)
                    .with_header(has_header)
                    .with_delimiter(delimiter)
                    .with_batch_size(Self::IMPORT_BATCH_ROWS)
                    .build(input)
                    .map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                    })?;
                for batch in reader {
                    visit(batch.map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                    })?)?;
                }
            }
            "JSON" | "NDJSON" | "JSONL" => {
                let file = std::fs::File::open(file_path).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("Cannot open JSON file '{}': {}", file_path, error),
                    )
                })?;
                let mmap = unsafe { memmap2::Mmap::map(&file) }
                    .map_err(|error| io::Error::new(io::ErrorKind::Other, error.to_string()))?;
                let start = mmap
                    .iter()
                    .position(|byte| !byte.is_ascii_whitespace())
                    .unwrap_or(mmap.len());
                let is_ndjson = start < mmap.len() && Self::looks_like_ndjson(&mmap[start..]);
                drop(mmap);
                drop(file);

                if !is_ndjson {
                    let batch = Self::read_json_to_batch(file_path, options)?;
                    if batch.num_rows() > 0 {
                        visit(batch)?;
                    }
                    return Ok(());
                }

                let schema = {
                    use arrow::json::reader::infer_json_schema_from_seekable;
                    let mut input = std::io::BufReader::new(std::fs::File::open(file_path)?);
                    let (schema, _) = infer_json_schema_from_seekable(&mut input, Some(100))
                        .map_err(|error| {
                            io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                        })?;
                    Arc::new(schema)
                };
                let input =
                    std::io::BufReader::with_capacity(1024 * 1024, std::fs::File::open(file_path)?);
                let reader = arrow::json::ReaderBuilder::new(schema)
                    .with_batch_size(Self::IMPORT_BATCH_ROWS)
                    .build(input)
                    .map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                    })?;
                for batch in reader {
                    visit(batch.map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                    })?)?;
                }
            }
            _ => {
                use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

                let file = std::fs::File::open(file_path).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("Cannot open Parquet file '{}': {}", file_path, error),
                    )
                })?;
                let mmap = unsafe { memmap2::Mmap::map(&file) }
                    .map_err(|error| io::Error::new(io::ErrorKind::Other, error.to_string()))?;
                let input = bytes::Bytes::from_owner(mmap);
                let reader = ParquetRecordBatchReaderBuilder::try_new(input)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?
                    .with_batch_size(Self::IMPORT_BATCH_ROWS)
                    .build()
                    .map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                    })?;
                for batch in reader {
                    visit(batch.map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                    })?)?;
                }
            }
        }
        Ok(())
    }

    pub(in crate::query::executor) fn ensure_import_table(
        storage_path: &Path,
        table_name: &str,
        schema: &arrow::datatypes::Schema,
    ) -> io::Result<()> {
        if storage_path.exists() {
            return Ok(());
        }

        use crate::data::DataType as ApexDataType;
        use crate::query::sql_parser::ColumnDef;
        let columns: Vec<ColumnDef> = schema
            .fields()
            .iter()
            .map(|field| {
                let apex_type = match field.data_type() {
                    arrow::datatypes::DataType::Int64
                    | arrow::datatypes::DataType::Int32
                    | arrow::datatypes::DataType::Int16
                    | arrow::datatypes::DataType::Int8
                    | arrow::datatypes::DataType::UInt64
                    | arrow::datatypes::DataType::UInt32
                    | arrow::datatypes::DataType::UInt16
                    | arrow::datatypes::DataType::UInt8 => ApexDataType::Int64,
                    arrow::datatypes::DataType::Float64 | arrow::datatypes::DataType::Float32 => {
                        ApexDataType::Float64
                    }
                    arrow::datatypes::DataType::Boolean => ApexDataType::Bool,
                    arrow::datatypes::DataType::Binary => ApexDataType::Binary,
                    _ => ApexDataType::String,
                };
                ColumnDef {
                    name: field.name().clone(),
                    data_type: apex_type,
                    constraints: vec![],
                }
            })
            .collect();
        Self::execute_create_table(storage_path, table_name, &columns, true)?;
        Ok(())
    }

    pub(in crate::query::executor) fn append_import_batch(
        storage_path: &Path,
        table_name: &str,
        batch: &RecordBatch,
    ) -> io::Result<usize> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        Self::ensure_import_table(storage_path, table_name, batch.schema().as_ref())?;

        if let Some(columns) = crate::data::arrow_convert::record_batch_to_typed_columns(batch) {
            crate::storage::engine::engine().write_typed(
                storage_path,
                columns.ints,
                columns.floats,
                columns.strings,
                columns.binaries,
                HashMap::new(),
                columns.bools,
                columns.nulls,
                crate::storage::DurabilityLevel::Fast,
            )?;
            return Ok(batch.num_rows());
        }

        let schema = batch.schema();
        let col_names: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let mut values = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                row.push(Self::arrow_value_at_col(column, row_idx));
            }
            values.push(row);
        }
        Self::execute_insert(storage_path, Some(&col_names), &values)?;
        Ok(batch.num_rows())
    }

    pub(crate) fn execute_copy_import(
        storage_path: &Path,
        table_name: &str,
        file_path: &str,
        format: &str,
        options: &[(String, String)],
        _base_dir: &Path,
        _default_table_path: &Path,
    ) -> io::Result<ApexResult> {
        let _epoch_write = crate::storage::epoch::logical_write(storage_path);
        let mut num_rows = 0usize;
        Self::for_each_import_batch(file_path, format, options, |batch| {
            num_rows += Self::append_import_batch(storage_path, table_name, &batch)?;
            Ok(())
        })?;
        Ok(ApexResult::Scalar(num_rows as i64))
    }
}

#[cfg(test)]
mod csv_fast_count_tests {
    use super::*;
    use tempfile::tempdir;

    fn write(path: &std::path::Path, contents: &str) {
        std::fs::write(path, contents).unwrap();
    }

    /// Full-parse row count = truth for comparison, with matching options.
    fn full_count(path: &str, options: &[(String, String)]) -> i64 {
        let batch = ApexExecutor::read_csv_to_batch(path, options, None).unwrap();
        batch.num_rows() as i64
    }

    fn fast_count(path: &str, options: &[(String, String)]) -> Option<i64> {
        ApexExecutor::try_fast_csv_count(path, options).unwrap()
    }

    #[test]
    fn fast_count_matches_full_parse_basic() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("a.csv");
        let path = p.to_str().unwrap().to_string();
        write(&p, "a,b,c\n1,2,3\n4,5,6\n7,8,9\n");
        assert_eq!(fast_count(&path, &[]), Some(3));
        assert_eq!(full_count(&path, &[]), 3);
    }

    #[test]
    fn fast_count_no_trailing_newline() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("b.csv");
        let path = p.to_str().unwrap().to_string();
        write(&p, "a,b\n1,2\n3,4");
        assert_eq!(fast_count(&path, &[]), Some(2));
        assert_eq!(full_count(&path, &[]), 2);
    }

    #[test]
    fn fast_count_skips_empty_lines() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.csv");
        let path = p.to_str().unwrap().to_string();
        write(&p, "a,b\n1,2\n\n3,4\n\n");
        assert_eq!(fast_count(&path, &[]), Some(2));
        assert_eq!(full_count(&path, &[]), 2);
    }

    #[test]
    fn fast_count_no_header() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("d.csv");
        let path = p.to_str().unwrap().to_string();
        write(&p, "1,2\n3,4\n5,6\n");
        let opt = vec![("header".to_string(), "false".to_string())];
        assert_eq!(fast_count(&path, &opt), Some(3));
        assert_eq!(full_count(&path, &opt), 3);
    }

    #[test]
    fn fast_count_tsv_delimiter() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("e.tsv");
        let path = p.to_str().unwrap().to_string();
        write(&p, "a\tb\n1\t2\n3\t4\n5\t6\n");
        let opt = vec![("delimiter".to_string(), "\t".to_string())];
        assert_eq!(fast_count(&path, &opt), Some(3));
        assert_eq!(full_count(&path, &opt), 3);
    }

    #[test]
    fn fast_count_empty_file() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("f.csv");
        let path = p.to_str().unwrap().to_string();
        write(&p, "a,b\n");
        assert_eq!(fast_count(&path, &[]), Some(0));
        assert_eq!(full_count(&path, &[]), 0);
    }

    #[test]
    fn fast_count_declines_quoted_data() {
        // Quoted CN fields make the full parser's count differ from the
        // quote-aware fast path, so the fast path must decline.
        let dir = tempdir().unwrap();
        let p = dir.path().join("g.csv");
        let path = p.to_str().unwrap().to_string();
        write(&p, "a,b\n\"x,y\",1\n\"z\",2\n");
        assert_eq!(fast_count(&path, &[]), None);
    }

    #[test]
    fn fast_count_skip_bad_lines_policy() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("h.csv");
        let path = p.to_str().unwrap().to_string();
        // One malformed row (3 fields vs 2).
        write(&p, "a,b\n1,2\n3,4,5\n5,6\n");
        let opt = vec![("on_bad_lines".to_string(), "skip".to_string())];
        assert_eq!(fast_count(&path, &opt), Some(2));
        assert_eq!(full_count(&path, &opt), 2);
    }

    #[test]
    fn fast_count_errors_on_bad_lines_with_error_policy() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("i.csv");
        let path = p.to_str().unwrap().to_string();
        write(&p, "a,b\n1,2\n3,4,5\n5,6\n");
        // Default policy is Error -> fast count must decline (None) so the full
        // parse raises the error.
        assert_eq!(fast_count(&path, &[]), None);
    }

    #[test]
    fn fast_count_crlf_lines() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("j.csv");
        let path = p.to_str().unwrap().to_string();
        write(&p, "a,b\r\n1,2\r\n3,4\r\n");
        assert_eq!(fast_count(&path, &[]), Some(2));
        assert_eq!(full_count(&path, &[]), 2);
    }

    fn fast_numeric_agg(path: &str, columns: &[&str]) -> Option<(i64, Vec<CsvNumericStats>)> {
        let columns = columns.iter().map(|column| (*column).to_string()).collect::<Vec<_>>();
        ApexExecutor::try_fast_csv_numeric_agg(path, &[], &columns, &[]).unwrap()
    }

    #[test]
    fn fast_numeric_agg_trims_header_whitespace_and_matches_values() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("numeric.csv");
        let path = p.to_str().unwrap().to_string();
        write(&p, "id, Protocol ,score\n1,6,10.5\n2,17,20.25\n3,,5.0\n");

        let schema = ApexExecutor::infer_csv_schema_fast(
            std::fs::read(&p).unwrap().as_slice(),
            true,
            b',',
            100,
        )
        .unwrap();
        assert_eq!(
            schema.fields().iter().map(|field| field.name()).collect::<Vec<_>>(),
            vec!["id", "Protocol", "score"]
        );

        let (rows, stats) = fast_numeric_agg(&path, &["Protocol", "score"]).unwrap();
        assert_eq!(rows, 3);
        assert_eq!(stats[0].count, 2);
        assert_eq!(stats[0].max, 17.0);
        assert_eq!(stats[1].count, 3);
        assert!((stats[1].max - 20.25).abs() < f64::EPSILON);
    }

    #[test]
    fn fast_numeric_agg_skips_bad_lines_and_declines_quoted_data() {
        let dir = tempdir().unwrap();
        let bad = dir.path().join("bad.csv");
        let bad_path = bad.to_str().unwrap().to_string();
        write(&bad, "id,value\n1,10\n2,20,extra\n3,30\n");
        assert!(fast_numeric_agg(&bad_path, &["value"]).is_none());

        let skipped = vec![("on_bad_lines".to_string(), "skip".to_string())];
        let columns = vec!["value".to_string()];
        let (rows, stats) = ApexExecutor::try_fast_csv_numeric_agg(
            &bad_path,
            &skipped,
            &columns,
            &[],
        )
        .unwrap()
        .unwrap();
        assert_eq!(rows, 2);
        assert_eq!(stats[0].max, 30.0);

        let quoted = dir.path().join("quoted.csv");
        let quoted_path = quoted.to_str().unwrap().to_string();
        write(&quoted, "id,value\n1,\"10\"\n2,20\n");
        assert!(fast_numeric_agg(&quoted_path, &["value"]).is_none());
    }

    #[test]
    fn fast_numeric_agg_applies_numeric_conjunction_before_merging() {
        let dir = tempdir().unwrap();
        let csv = dir.path().join("filtered.csv");
        let path = csv.to_str().unwrap().to_string();
        write(
            &csv,
            "protocol,duration,bytes\n6,10,100\n17,20,200\n6,30,300\n6,,400\n",
        );
        let columns = vec!["duration".to_string(), "bytes".to_string()];
        let predicates = vec![
            ("protocol".to_string(), 6.0, 6.0),
            ("duration".to_string(), 15.0, f64::INFINITY),
        ];
        let (rows, stats) = ApexExecutor::try_fast_csv_numeric_agg(
            &path,
            &[],
            &columns,
            &predicates,
        )
        .unwrap()
        .unwrap();
        assert_eq!(rows, 1);
        assert_eq!(stats[0].count, 1);
        assert_eq!(stats[0].sum, 30.0);
        assert_eq!(stats[1].sum, 300.0);
    }

    #[test]
    fn fast_string_group_numeric_agg_merges_worker_state() {
        let dir = tempdir().unwrap();
        let csv = dir.path().join("groups.csv");
        let path = csv.to_str().unwrap().to_string();
        write(
            &csv,
            "label,duration,packets\nbenign,10,1\nattack,20,2\nbenign,30,3\n,40,4\n",
        );
        let (_, groups) = ApexExecutor::try_fast_csv_group_numeric_agg(
            &path,
            &[],
            "label",
            &["duration".to_string(), "packets".to_string()],
            &[],
        )
        .unwrap()
        .unwrap();
        let groups = groups
            .into_iter()
            .map(|(key, rows, stats)| {
                (
                    key.map(|value| String::from_utf8(value).unwrap()),
                    (rows, stats[0].sum, stats[1].max),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(groups[&Some("benign".to_string())], (2, 40.0, 3.0));
        assert_eq!(groups[&Some("attack".to_string())], (1, 20.0, 2.0));
        assert_eq!(groups[&None], (1, 40.0, 4.0));
    }

    #[test]
    fn fast_string_group_numeric_agg_applies_numeric_conjunction() {
        let dir = tempdir().unwrap();
        let csv = dir.path().join("filtered_groups.csv");
        let path = csv.to_str().unwrap().to_string();
        write(
            &csv,
            "label,duration,protocol,packets\nbenign,10,6,1\nattack,20,17,2\nbenign,30,17,3\nattack,40,6,4\n",
        );
        let (_, groups) = ApexExecutor::try_fast_csv_group_numeric_agg(
            &path,
            &[],
            "label",
            &["duration".to_string(), "packets".to_string()],
            &[
                ("protocol".to_string(), 17.0, 17.0),
                ("duration".to_string(), 15.0, f64::INFINITY),
            ],
        )
        .unwrap()
        .unwrap();
        let groups = groups
            .into_iter()
            .map(|(key, rows, stats)| {
                (
                    key.map(|value| String::from_utf8(value).unwrap()),
                    (rows, stats[0].sum, stats[1].max),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(groups[&Some("benign".to_string())], (1, 30.0, 3.0));
        assert_eq!(groups[&Some("attack".to_string())], (1, 20.0, 2.0));
    }

    #[test]
    fn fast_integer_group_numeric_agg_normalizes_keys() {
        let dir = tempdir().unwrap();
        let csv = dir.path().join("integer_groups.csv");
        let path = csv.to_str().unwrap().to_string();
        write(
            &csv,
            "protocol,duration\n6,10\n006,20\n17,30\n,40\n",
        );
        let (key_type, groups) = ApexExecutor::try_fast_csv_group_numeric_agg(
            &path,
            &[],
            "protocol",
            &["duration".to_string()],
            &[],
        )
        .unwrap()
        .unwrap();
        assert_eq!(key_type, CsvGroupKeyType::Int64);
        let groups = groups
            .into_iter()
            .map(|(key, rows, stats)| {
                let key = key.map(|value| i64::from_le_bytes(value.try_into().unwrap()));
                (key, (rows, stats[0].sum))
            })
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(groups[&Some(6)], (2, 30.0));
        assert_eq!(groups[&Some(17)], (1, 30.0));
        assert_eq!(groups[&None], (1, 40.0));
    }

    #[test]
    fn fast_csv_distinct_counts_handles_string_integer_and_null() {
        let dir = tempdir().unwrap();
        let csv = dir.path().join("distinct.csv");
        let path = csv.to_str().unwrap().to_string();
        write(
            &csv,
            "protocol,label\n6,benign\n006,benign\n17,attack\n,attack\n17,\n",
        );
        let counts = ApexExecutor::try_fast_csv_distinct_counts(
            &path,
            &[],
            &["protocol".to_string(), "label".to_string()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(counts, vec![2, 2]);
    }

    #[test]
    fn checked_csv_integer_parser_handles_boundaries() {
        assert_eq!(ApexExecutor::parse_csv_i64_checked(b"+17"), Some(17));
        assert_eq!(
            ApexExecutor::parse_csv_i64_checked(b"-9223372036854775808"),
            Some(i64::MIN)
        );
        assert_eq!(
            ApexExecutor::parse_csv_i64_checked(b"9223372036854775807"),
            Some(i64::MAX)
        );
        assert_eq!(ApexExecutor::parse_csv_i64_checked(b""), None);
        assert_eq!(ApexExecutor::parse_csv_i64_checked(b"12x"), None);
        assert_eq!(
            ApexExecutor::parse_csv_i64_checked(b"9223372036854775808"),
            None
        );
    }
}
