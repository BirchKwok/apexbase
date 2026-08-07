use super::*;

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
    ) -> io::Result<RecordBatch> {
        match func.to_uppercase().as_str() {
            "READ_CSV" => Self::read_csv_to_batch(file, options),
            "READ_JSON" => Self::read_json_to_batch(file, options),
            "READ_PARQUET" => Self::read_parquet_to_batch(file, options),
            other => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Unknown table function: {}", other),
            )),
        }
    }

    pub(crate) fn read_direct_file(file: &str) -> io::Result<RecordBatch> {
        let lower = file.to_lowercase();
        if lower.ends_with(".csv.gz") || lower.ends_with(".csv.gzip") {
            return Self::read_csv_to_batch(file, &[]);
        }
        if lower.ends_with(".csv") {
            return Self::read_csv_to_batch(file, &[]);
        }
        if lower.ends_with(".tsv") {
            return Self::read_csv_to_batch(file, &[("delimiter".to_string(), "\t".to_string())]);
        }
        if lower.ends_with(".json") || lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
            return Self::read_json_to_batch(file, &[]);
        }
        if lower.ends_with(".parquet") {
            return Self::read_parquet_to_batch(file, &[]);
        }
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "Unsupported file format for '{}'. Supported: .csv, .tsv, .json, .jsonl, .ndjson, .parquet, .csv.gz",
                file
            ),
        ))
    }

    pub(in crate::query::executor) fn read_csv_to_batch(
        path: &str,
        options: &[(String, String)],
    ) -> io::Result<RecordBatch> {
        use rayon::prelude::*;

        let has_header = options
            .iter()
            .find(|(k, _)| k == "header")
            .map(|(_, v)| !matches!(v.to_lowercase().as_str(), "false" | "0"))
            .unwrap_or(true);

        let delimiter: u8 = options
            .iter()
            .find(|(k, _)| k == "delimiter" || k == "delim" || k == "sep")
            .and_then(|(_, v)| v.chars().next())
            .map(|c| c as u8)
            .unwrap_or(b',');
        let bad_line_policy = csv_bad_line_policy(options)?;

        let file = std::fs::File::open(path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Cannot open CSV file '{}': {}", path, e),
            )
        })?;
        let mmap = unsafe { memmap2::Mmap::map(&file) }
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("mmap error: {}", e)))?;
        let data: &[u8] = &mmap;

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

        // Parse filter pushdown option: "col>val" or "col<val" style
        let filter_info = options
            .iter()
            .find(|(k, _)| k == "filter")
            .and_then(|(_, v)| parse_pushdown_filter(v, &schema));

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
                )
            })
            .collect();

        let all: Vec<RecordBatch> = batches.into_iter().collect::<io::Result<Vec<_>>>()?;
        Self::merge_record_batches(all)
    }

    pub(in crate::query::executor) fn parse_csv_chunk_fast(
        data: &[u8],
        schema: &arrow::datatypes::Schema,
        delimiter: u8,
        filter_info: Option<&PushdownFilter>,
        bad_line_policy: CsvBadLinePolicy,
    ) -> io::Result<RecordBatch> {
        use arrow::array::{BooleanArray, BooleanBuilder};
        use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
        use arrow::datatypes::DataType;

        let n_cols = schema.fields().len();
        if n_cols == 0 {
            return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
        }

        // Estimate row count from chunk size — avoids an extra full-scan just for capacity.
        // Assume ≥20 bytes per line (conservative underestimate ⇒ slight overalloc, no resize).
        if data.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
        }
        let n_rows = (data.len() / 20).max(1);

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
            line_number += 1;
        }
        if line_start < data.len() {
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
                    .trim_matches('"')
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
                .trim_matches('"')
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
                    let batch = Self::read_csv_to_batch(file_path, options)?;
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
