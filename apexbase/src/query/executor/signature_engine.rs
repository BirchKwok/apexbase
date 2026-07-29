// Normalized pre-parse read execution driven by QuerySignature.

impl ApexExecutor {
    #[inline]
    fn signature_table_path(
        table: Option<&str>,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> PathBuf {
        table
            .map(|name| Self::resolve_table_path(name, base_dir, default_table_path))
            .unwrap_or_else(|| default_table_path.to_path_buf())
    }

    #[inline]
    fn signature_column_refs(
        projection: crate::query::query_signature::ReadProjection<'_>,
    ) -> Option<Vec<&str>> {
        projection
            .columns()
            .map(|columns| columns.iter().map(String::as_str).collect())
    }

    fn signature_values_batch(values: &[(String, Value)]) -> io::Result<RecordBatch> {
        let mut fields = Vec::with_capacity(values.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(values.len());
        for (name, value) in values {
            let nullable = matches!(value, Value::Null);
            let (data_type, array): (ArrowDataType, ArrayRef) = match value {
                Value::Int64(value) => (
                    ArrowDataType::Int64,
                    Arc::new(Int64Array::from(vec![*value])),
                ),
                Value::Float64(value) => (
                    ArrowDataType::Float64,
                    Arc::new(Float64Array::from(vec![*value])),
                ),
                Value::String(value) => (
                    ArrowDataType::Utf8,
                    Arc::new(StringArray::from(vec![value.as_str()])),
                ),
                Value::Bool(value) => (
                    ArrowDataType::Boolean,
                    Arc::new(BooleanArray::from(vec![*value])),
                ),
                _ => (
                    ArrowDataType::Utf8,
                    Arc::new(StringArray::from(vec![None as Option<&str>])),
                ),
            };
            fields.push(Field::new(name, data_type, nullable));
            arrays.push(array);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| err_data(error.to_string()))
    }

    /// Execute all simple read signatures through one normalized engine.
    /// `Ok(None)` means storage state or format requires the parsed fallback.
    fn try_execute_read_signature(
        read: crate::query::query_signature::ReadSignature<'_>,
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<Option<ApexResult>> {
        use crate::query::query_signature::{ReadPredicate, ReadProjection};

        let table_path =
            Self::signature_table_path(read.table, base_dir, default_table_path);
        let backend = match get_cached_backend(&table_path) {
            Ok(backend) => backend,
            Err(_) => return Ok(None),
        };
        let column_refs = Self::signature_column_refs(read.projection);
        let columns = column_refs.as_deref();

        let batch = match read.predicate {
            ReadPredicate::All => match read.limit {
                None => backend.read_columns_to_arrow(columns, 0, None).ok(),
                Some((limit, offset)) => {
                    if backend.pending_v4_in_memory_rows() > 0 {
                        None
                    } else if matches!(read.projection, ReadProjection::All) && offset > 0 {
                        if backend.has_pending_deltas()
                            || backend.has_delta()
                            || backend.active_row_count() != backend.row_count()
                        {
                            None
                        } else {
                            let end = offset.saturating_add(limit).min(backend.row_count() as usize);
                            let indices: Vec<usize> = (offset..end).collect();
                            backend
                                .read_columns_by_indices_to_arrow(&indices, columns)
                                .ok()
                        }
                    } else {
                        backend
                            .read_columns_to_arrow(columns, offset, Some(limit))
                            .ok()
                    }
                }
            },
            ReadPredicate::Id(id) => {
                if backend.has_pending_deltas()
                    || backend.has_delta()
                    || backend.active_row_count() != backend.row_count()
                {
                    None
                } else if matches!(read.projection, ReadProjection::All) {
                    if backend.storage.is_v4_format()
                        && !backend.storage.has_v4_in_memory_data()
                    {
                        backend
                            .storage
                            .retrieve_rcix(id)
                            .ok()
                            .flatten()
                            .map(|values| Self::signature_values_batch(&values))
                            .transpose()?
                    } else {
                        None
                    }
                } else if let Some(columns) = read.projection.columns() {
                    if let Some(batch) = backend.read_row_by_id_to_arrow(id).ok().flatten() {
                        Self::project_batch_by_names(&batch, columns)?
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ReadPredicate::Ids(ids) => {
                let sorted_ids = sort_and_dedupe_ids(ids);
                match backend.read_rows_by_ids_to_arrow(&sorted_ids) {
                    Ok(batch) if batch.num_rows() > 0 => match read.projection.columns() {
                        Some(columns) => Self::project_batch_by_names(&batch, columns)?,
                        None => Some(batch),
                    },
                    Ok(_) => backend.read_columns_to_arrow(columns, 0, Some(0)).ok(),
                    Err(_) => None,
                }
            }
            ReadPredicate::StringEq { column, value } => {
                if backend.pending_v4_in_memory_rows() > 0 || backend.has_pending_deltas() {
                    None
                } else {
                    match read.limit {
                        Some((limit, offset)) if backend.has_delta() => backend
                            .read_columns_filtered_string_to_arrow(
                                columns, column, value, true,
                            )
                            .ok()
                            .map(|full| {
                                let offset = offset.min(full.num_rows());
                                let len = limit.min(full.num_rows().saturating_sub(offset));
                                full.slice(offset, len)
                            }),
                        Some((limit, offset)) => backend
                            .read_columns_filtered_string_with_limit_to_arrow(
                                columns, column, value, true, limit, offset,
                            )
                            .ok(),
                        None => backend
                            .read_columns_filtered_string_to_arrow(
                                columns, column, value, true,
                            )
                            .ok(),
                    }
                }
            }
            ReadPredicate::NumericRange { column, low, high } => {
                if backend.has_pending_deltas()
                    || backend.has_delta()
                    || backend.pending_v4_in_memory_rows() > 0
                {
                    None
                } else if let Some((limit, offset)) = read.limit {
                    let needed = offset.saturating_add(limit);
                    match backend.scan_numeric_range_mmap(column, low, high, Some(needed)) {
                        Ok(Some(indices)) => {
                            let start = offset.min(indices.len());
                            let end = start.saturating_add(limit).min(indices.len());
                            if start == end {
                                backend.read_columns_to_arrow(columns, 0, Some(0)).ok()
                            } else {
                                backend
                                    .read_columns_by_indices_to_arrow(&indices[start..end], columns)
                                    .ok()
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            }
            ReadPredicate::Like { column, pattern } => {
                if backend.pending_v4_in_memory_rows() > 0 {
                    None
                } else {
                    backend
                        .scan_like_and_extract_mmap(column, pattern, None)
                        .ok()
                        .flatten()
                }
            }
        };

        Ok(batch.map(ApexResult::Data))
    }
}
