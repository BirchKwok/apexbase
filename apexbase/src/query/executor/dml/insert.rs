use super::*;

impl ApexExecutor {
    pub(in crate::query::executor) fn epoch_days(date: chrono::NaiveDate) -> i32 {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (date - epoch).num_days() as i32
    }

    pub(in crate::query::executor) fn default_value_to_value(
        default: &crate::storage::on_demand::DefaultValue,
        col_type: Option<crate::storage::on_demand::ColumnType>,
        ctx: &DefaultEvalContext,
    ) -> Value {
        use crate::storage::on_demand::{ColumnType, DefaultValue};

        match default {
            DefaultValue::Int64(v) => match col_type {
                Some(ColumnType::Date) => Value::Date(*v as i32),
                Some(ColumnType::Timestamp) => Value::Timestamp(*v),
                Some(ColumnType::Float32) | Some(ColumnType::Float64) => Value::Float64(*v as f64),
                _ => Value::Int64(*v),
            },
            DefaultValue::Float64(v) => match col_type {
                Some(ColumnType::Int8)
                | Some(ColumnType::Int16)
                | Some(ColumnType::Int32)
                | Some(ColumnType::Int64)
                | Some(ColumnType::UInt8)
                | Some(ColumnType::UInt16)
                | Some(ColumnType::UInt32)
                | Some(ColumnType::UInt64) => Value::Int64(*v as i64),
                _ => Value::Float64(*v),
            },
            DefaultValue::String(v) => match col_type {
                Some(ColumnType::Date) => Value::Date(Self::parse_date_string(v) as i32),
                Some(ColumnType::Timestamp) => Value::Timestamp(Self::parse_timestamp_string(v)),
                Some(ColumnType::Binary) => Self::try_parse_vector_string(v)
                    .map(Value::Binary)
                    .unwrap_or_else(|| Value::String(v.clone())),
                _ => Value::String(v.clone()),
            },
            DefaultValue::Bool(v) => Value::Bool(*v),
            DefaultValue::Null => Value::Null,
            DefaultValue::Date(days) => match col_type {
                Some(ColumnType::String) | Some(ColumnType::StringDict) => {
                    Value::String(Self::date_string_from_days(*days))
                }
                Some(ColumnType::Timestamp) => Value::Timestamp(*days as i64 * 86_400_000_000),
                Some(ColumnType::Float32) | Some(ColumnType::Float64) => {
                    Value::Float64(*days as f64)
                }
                _ => Value::Date(*days),
            },
            DefaultValue::Timestamp(micros) => match col_type {
                Some(ColumnType::String) | Some(ColumnType::StringDict) => {
                    Value::String(Self::timestamp_string_from_micros(*micros))
                }
                Some(ColumnType::Date) => Value::Date((*micros / 86_400_000_000) as i32),
                Some(ColumnType::Float32) | Some(ColumnType::Float64) => {
                    Value::Float64(*micros as f64)
                }
                _ => Value::Timestamp(*micros),
            },
            DefaultValue::CurrentDate => {
                let today = ctx.now.date_naive();
                let days = Self::epoch_days(today);
                match col_type {
                    Some(ColumnType::Date) => Value::Date(days),
                    Some(ColumnType::Timestamp) => {
                        let dt = today.and_hms_opt(0, 0, 0).unwrap();
                        Value::Timestamp(dt.and_utc().timestamp_micros())
                    }
                    Some(ColumnType::String) | Some(ColumnType::StringDict) => {
                        Value::String(today.format("%Y-%m-%d").to_string())
                    }
                    Some(ColumnType::Float32) | Some(ColumnType::Float64) => {
                        Value::Float64(days as f64)
                    }
                    _ => Value::Int64(days as i64),
                }
            }
            DefaultValue::CurrentTimestamp => {
                let today = ctx.now.date_naive();
                let seconds = ctx.now.timestamp();
                match col_type {
                    Some(ColumnType::Timestamp) => Value::Timestamp(ctx.now.timestamp_micros()),
                    Some(ColumnType::Date) => Value::Date(Self::epoch_days(today)),
                    Some(ColumnType::String) | Some(ColumnType::StringDict) => {
                        Value::String(ctx.now.format("%Y-%m-%d %H:%M:%S").to_string())
                    }
                    Some(ColumnType::Float32) | Some(ColumnType::Float64) => {
                        Value::Float64(seconds as f64)
                    }
                    _ => Value::Int64(seconds),
                }
            }
            DefaultValue::UnixTimestamp => {
                let seconds = ctx.now.timestamp();
                match col_type {
                    Some(ColumnType::Timestamp) => Value::Timestamp(seconds * 1_000_000),
                    Some(ColumnType::Date) => Value::Date(Self::epoch_days(ctx.now.date_naive())),
                    Some(ColumnType::String) | Some(ColumnType::StringDict) => {
                        Value::String(seconds.to_string())
                    }
                    Some(ColumnType::Float32) | Some(ColumnType::Float64) => {
                        Value::Float64(seconds as f64)
                    }
                    _ => Value::Int64(seconds),
                }
            }
        }
    }

    pub(in crate::query::executor) fn date_string_from_days(days: i32) -> String {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (epoch + chrono::Duration::days(days as i64))
            .format("%Y-%m-%d")
            .to_string()
    }

    pub(in crate::query::executor) fn timestamp_string_from_micros(micros: i64) -> String {
        chrono::DateTime::from_timestamp_micros(micros)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| micros.to_string())
    }

    pub(in crate::query::executor) fn resolve_insert_values_for_path(
        storage_path: &Path,
        columns: Option<&[String]>,
        values: &[Vec<InsertValue>],
    ) -> io::Result<Vec<Vec<Value>>> {
        let has_default = values
            .iter()
            .any(|row| row.iter().any(|v| matches!(v, InsertValue::Default)));

        if !has_default {
            return Ok(values
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|v| match v {
                            InsertValue::Value(value) => value.clone(),
                            InsertValue::Default => Value::Null,
                        })
                        .collect()
                })
                .collect());
        }

        crate::storage::table_catalog::ensure_table_file(
            storage_path,
            crate::storage::DurabilityLevel::Fast,
        )?;

        let storage = TableStorageBackend::open_for_write(storage_path)?;
        let col_names: Vec<String> = if let Some(cols) = columns {
            cols.to_vec()
        } else {
            storage
                .get_schema()
                .iter()
                .map(|(n, _)| n.clone())
                .collect()
        };
        let table_schema = storage.get_schema();
        let known_columns: std::collections::HashSet<&str> =
            table_schema.iter().map(|(name, _)| name.as_str()).collect();
        let mut seen_columns = std::collections::HashSet::new();
        for column in &col_names {
            if !known_columns.contains(column.as_str()) {
                return Err(err_input(format!(
                    "INSERT target column '{}' does not exist",
                    column
                )));
            }
            if !seen_columns.insert(column.as_str()) {
                return Err(err_input(format!(
                    "INSERT target column '{}' is specified more than once",
                    column
                )));
            }
        }
        for (row_index, row) in values.iter().enumerate() {
            if row.len() != col_names.len() {
                return Err(err_input(format!(
                    "INSERT row {} has {} values but {} target columns",
                    row_index + 1,
                    row.len(),
                    col_names.len()
                )));
            }
        }
        let schema_types: std::collections::HashMap<String, crate::storage::on_demand::ColumnType> =
            storage
                .get_schema()
                .iter()
                .map(|(name, dt)| {
                    (
                        name.clone(),
                        crate::storage::backend::datatype_to_column_type(dt),
                    )
                })
                .collect();
        let ctx = DefaultEvalContext::new();

        Ok(values
            .iter()
            .map(|row| {
                row.iter()
                    .enumerate()
                    .filter_map(|(i, item)| {
                        let col_name = col_names.get(i)?;
                        let value = match item {
                            InsertValue::Value(value) => value.clone(),
                            InsertValue::Default => {
                                let cons = storage.storage.get_column_constraints(col_name);
                                cons.default_value
                                    .as_ref()
                                    .map(|dv| {
                                        let col_schema_type = schema_types.get(col_name).copied();
                                        Self::default_value_to_value(dv, col_schema_type, &ctx)
                                    })
                                    .unwrap_or(Value::Null)
                            }
                        };
                        Some(value)
                    })
                    .collect()
            })
            .collect())
    }

    pub(in crate::query::executor) fn is_insert_default_values(
        columns: Option<&[String]>,
        values: &[Vec<InsertValue>],
    ) -> bool {
        columns.is_some_and(|cols| cols.is_empty())
            && values.len() == 1
            && values.first().is_some_and(|row| row.is_empty())
    }

    pub(in crate::query::executor) fn resolve_default_values_insert_for_path(
        storage_path: &Path,
    ) -> io::Result<(Vec<String>, Vec<Vec<Value>>)> {
        crate::storage::table_catalog::ensure_table_file(
            storage_path,
            crate::storage::DurabilityLevel::Fast,
        )?;

        let storage = TableStorageBackend::open_for_write(storage_path)?;
        let schema = storage.get_schema();
        let schema_types: std::collections::HashMap<String, crate::storage::on_demand::ColumnType> =
            schema
                .iter()
                .map(|(name, dt)| {
                    (
                        name.clone(),
                        crate::storage::backend::datatype_to_column_type(dt),
                    )
                })
                .collect();
        let ctx = DefaultEvalContext::new();
        let mut columns = Vec::with_capacity(schema.len());
        let mut row = Vec::with_capacity(schema.len());

        for (col_name, _) in &schema {
            columns.push(col_name.clone());
            let cons = storage.storage.get_column_constraints(col_name);
            let value = cons
                .default_value
                .as_ref()
                .map(|dv| {
                    let col_schema_type = schema_types.get(col_name).copied();
                    Self::default_value_to_value(dv, col_schema_type, &ctx)
                })
                .unwrap_or(Value::Null);
            row.push(value);
        }

        Ok((columns, vec![row]))
    }

    pub(in crate::query::executor) fn execute_insert_items(
        storage_path: &Path,
        columns: Option<&[String]>,
        values: &[Vec<InsertValue>],
    ) -> io::Result<ApexResult> {
        if Self::is_insert_default_values(columns, values) {
            let (columns, resolved) = Self::resolve_default_values_insert_for_path(storage_path)?;
            return Self::execute_insert(storage_path, Some(&columns), &resolved);
        }

        let resolved = Self::resolve_insert_values_for_path(storage_path, columns, values)?;
        Self::execute_insert(storage_path, columns, &resolved)
    }

    pub(in crate::query::executor) fn execute_insert(
        storage_path: &Path,
        columns: Option<&[String]>,
        values: &[Vec<Value>],
    ) -> io::Result<ApexResult> {
        use std::collections::HashMap;

        crate::storage::table_catalog::ensure_table_file(
            storage_path,
            crate::storage::DurabilityLevel::Fast,
        )?;
        let _epoch_write = crate::storage::epoch::logical_write(storage_path);

        // Invalidate cache before write
        invalidate_storage_cache(storage_path);

        // Use open_for_write to load all data (needed for correct column alignment)
        let storage = TableStorageBackend::open_for_write(storage_path)?;

        // Get column names from schema or explicit list
        let col_names: Vec<String> = if let Some(cols) = columns {
            cols.to_vec()
        } else {
            storage
                .get_schema()
                .iter()
                .map(|(n, _)| n.clone())
                .collect()
        };
        let table_schema = storage.get_schema();
        let known_columns: std::collections::HashSet<&str> =
            table_schema.iter().map(|(name, _)| name.as_str()).collect();
        let mut seen_columns = std::collections::HashSet::with_capacity(col_names.len());
        for column in &col_names {
            if !known_columns.contains(column.as_str()) {
                return Err(err_input(format!(
                    "INSERT target column '{}' does not exist",
                    column
                )));
            }
            if !seen_columns.insert(column.as_str()) {
                return Err(err_input(format!(
                    "INSERT target column '{}' is specified more than once",
                    column
                )));
            }
        }
        for (row_index, row) in values.iter().enumerate() {
            if row.len() != col_names.len() {
                return Err(err_input(format!(
                    "INSERT row {} has {} values but {} target columns",
                    row_index + 1,
                    row.len(),
                    col_names.len()
                )));
            }
        }

        // Build a schema type lookup for auto-coercing string→timestamp/date
        let schema_types: std::collections::HashMap<String, crate::storage::on_demand::ColumnType> = {
            let schema = storage.get_schema();
            schema
                .iter()
                .map(|(name, dt)| {
                    (
                        name.clone(),
                        crate::storage::backend::datatype_to_column_type(dt),
                    )
                })
                .collect()
        };

        // Build row-based data with proper NULL handling via Value::Null
        // Auto-coerce string→timestamp/date based on schema type
        let mut rows: Vec<HashMap<String, Value>> = Vec::with_capacity(values.len());
        for row_values in values {
            let mut row = HashMap::new();
            for (i, value) in row_values.iter().enumerate() {
                if i < col_names.len() {
                    let col_name = &col_names[i];
                    let col_schema_type = schema_types.get(col_name).copied();
                    let coerced = match value {
                        Value::Binary(bytes)
                            if matches!(
                                col_schema_type,
                                Some(crate::storage::on_demand::ColumnType::Blob)
                            ) =>
                        {
                            Value::Blob(bytes.clone())
                        }
                        Value::Int64(number)
                            if matches!(
                                col_schema_type,
                                Some(
                                    crate::storage::on_demand::ColumnType::Float32
                                        | crate::storage::on_demand::ColumnType::Float64
                                )
                            ) =>
                        {
                            Value::Float64(*number as f64)
                        }
                        Value::String(v) => {
                            match col_schema_type {
                                Some(crate::storage::on_demand::ColumnType::Timestamp) => {
                                    Value::Timestamp(Self::parse_timestamp_string(v))
                                }
                                Some(crate::storage::on_demand::ColumnType::Date) => {
                                    Value::Date(Self::parse_date_string(v) as i32)
                                }
                                Some(crate::storage::on_demand::ColumnType::Binary) => {
                                    // Auto-encode JSON-style float array string '[1.0,2.0,…]' as binary vector
                                    Self::try_parse_vector_string(v)
                                        .map(Value::Binary)
                                        .unwrap_or_else(|| value.clone())
                                }
                                _ => value.clone(),
                            }
                        }
                        _ => value.clone(),
                    };
                    row.insert(col_name.clone(), coerced);
                }
            }
            rows.push(row);
        }

        let rows_inserted = values.len() as i64;

        // Enforce NOT NULL constraints
        if storage.storage.has_constraints() {
            for (row_idx, row_values) in values.iter().enumerate() {
                for (i, value) in row_values.iter().enumerate() {
                    if i < col_names.len() {
                        let col_name = &col_names[i];
                        let cons = storage.storage.get_column_constraints(col_name);
                        if cons.not_null && matches!(value, Value::Null) {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("NOT NULL constraint violated: column '{}' cannot be NULL (row {})", col_name, row_idx + 1),
                            ));
                        }
                    }
                }
                // Check for missing NOT NULL columns — allow if DEFAULT is set
                let schema = storage.get_schema();
                for (schema_col, _) in &schema {
                    if !col_names.iter().any(|c| c == schema_col) {
                        let cons = storage.storage.get_column_constraints(schema_col);
                        if cons.not_null && cons.default_value.is_none() && !cons.autoincrement {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("NOT NULL constraint violated: column '{}' has no value and no DEFAULT", schema_col),
                            ));
                        }
                    }
                }
            }
        }

        // Fill in DEFAULT values for missing columns into row maps
        if storage.storage.has_constraints() {
            let schema = storage.get_schema();
            let default_ctx = DefaultEvalContext::new();
            for (schema_col, _) in &schema {
                if !col_names.iter().any(|c| c == schema_col) {
                    let cons = storage.storage.get_column_constraints(schema_col);
                    if let Some(ref dv) = cons.default_value {
                        let col_schema_type = schema_types.get(schema_col).copied();
                        let default_val =
                            Self::default_value_to_value(dv, col_schema_type, &default_ctx);
                        for row in rows.iter_mut() {
                            row.entry(schema_col.clone())
                                .or_insert_with(|| default_val.clone());
                        }
                    }
                }
            }
        }

        // AUTOINCREMENT: auto-fill sequential values for autoincrement columns
        if storage.storage.has_constraints() {
            let schema = storage.get_schema();
            for (schema_col, _) in &schema {
                let cons = storage.storage.get_column_constraints(schema_col);
                if cons.autoincrement {
                    // Find current max value for this column
                    let col_refs = [schema_col.as_str()];
                    let existing = storage.read_columns_to_arrow(Some(&col_refs), 0, None)?;
                    let mut next_val: i64 = if let Some(col) = existing.column_by_name(schema_col) {
                        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                            arr.iter().filter_map(|v| v).max().unwrap_or(0) + 1
                        } else {
                            1
                        }
                    } else {
                        1
                    };
                    // Fill in autoincrement values for rows that don't have this column set
                    for row in rows.iter_mut() {
                        if !row.contains_key(schema_col)
                            || matches!(row.get(schema_col), Some(Value::Null))
                        {
                            row.insert(schema_col.clone(), Value::Int64(next_val));
                            next_val += 1;
                        }
                    }
                }
            }
        }

        // Enforce UNIQUE / PRIMARY KEY constraints
        if storage.storage.has_constraints() {
            // Collect columns that need uniqueness checks
            let schema = storage.get_schema();
            let unique_cols: Vec<String> = schema
                .iter()
                .filter(|(name, _)| {
                    let c = storage.storage.get_column_constraints(name);
                    c.unique || c.primary_key
                })
                .map(|(name, _)| name.clone())
                .collect();

            if !unique_cols.is_empty() {
                // Read existing values for uniqueness columns
                let col_refs: Vec<&str> = unique_cols.iter().map(|s| s.as_str()).collect();
                let existing_batch = storage.read_columns_to_arrow(Some(&col_refs), 0, None)?;

                for uc in &unique_cols {
                    let constraint_kind = if storage.storage.get_column_constraints(uc).primary_key
                    {
                        "PRIMARY KEY"
                    } else {
                        "UNIQUE"
                    };

                    // Collect new values for this column from the INSERT
                    let mut new_vals: Vec<Value> = Vec::new();
                    for row_values in values.iter() {
                        for (i, value) in row_values.iter().enumerate() {
                            if i < col_names.len() && col_names[i] == *uc {
                                new_vals.push(value.clone());
                            }
                        }
                    }

                    // Check duplicates within new values themselves
                    {
                        let mut seen = std::collections::HashSet::new();
                        for v in &new_vals {
                            if !matches!(v, Value::Null) {
                                let key = format!("{:?}", v);
                                if !seen.insert(key) {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        format!("{} constraint violated: duplicate value in column '{}'", constraint_kind, uc),
                                    ));
                                }
                            }
                        }
                    }

                    // Check against existing data
                    if let Some(existing_col) = existing_batch.column_by_name(uc) {
                        use arrow::array::{
                            BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
                        };
                        for new_val in &new_vals {
                            if matches!(new_val, Value::Null) {
                                continue;
                            }
                            let len = existing_col.len();
                            for row in 0..len {
                                if existing_col.is_null(row) {
                                    continue;
                                }
                                let matches = match new_val {
                                    Value::Int64(v) => existing_col
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .map(|a| a.value(row) == *v)
                                        .or_else(|| {
                                            existing_col
                                                .as_any()
                                                .downcast_ref::<UInt64Array>()
                                                .map(|a| a.value(row) as i64 == *v)
                                        })
                                        .unwrap_or(false),
                                    Value::Int32(v) => existing_col
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .map(|a| a.value(row) == *v as i64)
                                        .unwrap_or(false),
                                    Value::Float64(v) => existing_col
                                        .as_any()
                                        .downcast_ref::<Float64Array>()
                                        .map(|a| (a.value(row) - v).abs() < f64::EPSILON)
                                        .unwrap_or(false),
                                    Value::String(v) => existing_col
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .map(|a| a.value(row) == v.as_str())
                                        .unwrap_or(false),
                                    Value::Bool(v) => existing_col
                                        .as_any()
                                        .downcast_ref::<BooleanArray>()
                                        .map(|a| a.value(row) == *v)
                                        .unwrap_or(false),
                                    _ => false,
                                };
                                if matches {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        format!("{} constraint violated: duplicate value in column '{}'", constraint_kind, uc),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Enforce CHECK constraints
        if storage.storage.has_constraints() {
            let schema = storage.get_schema();
            for (schema_col, _) in &schema {
                let cons = storage.storage.get_column_constraints(schema_col);
                if let Some(ref check_sql) = cons.check_expr_sql {
                    // Parse the CHECK expression once
                    let check_expr = {
                        // Wrap in a SELECT WHERE to reuse the expression parser
                        let parse_sql = format!("SELECT 1 FROM _dummy WHERE {}", check_sql);
                        match crate::query::sql_parser::SqlParser::parse(&parse_sql) {
                            Ok(crate::query::sql_parser::SqlStatement::Select(sel)) => {
                                sel.where_clause.clone()
                            }
                            _ => None,
                        }
                    };
                    if let Some(ref expr) = check_expr {
                        // Evaluate CHECK for each row
                        for (row_idx, row_values) in values.iter().enumerate() {
                            // Build a 1-row RecordBatch with this row's values
                            let mut fields = Vec::new();
                            let mut arrays: Vec<ArrayRef> = Vec::new();
                            for (i, value) in row_values.iter().enumerate() {
                                if i < col_names.len() {
                                    let cn = &col_names[i];
                                    match value {
                                        Value::Int64(v) => {
                                            fields.push(Field::new(cn, ArrowDataType::Int64, true));
                                            arrays.push(Arc::new(Int64Array::from(vec![*v])));
                                        }
                                        Value::Float64(v) => {
                                            fields.push(Field::new(
                                                cn,
                                                ArrowDataType::Float64,
                                                true,
                                            ));
                                            arrays.push(Arc::new(Float64Array::from(vec![*v])));
                                        }
                                        Value::String(v) => {
                                            fields.push(Field::new(cn, ArrowDataType::Utf8, true));
                                            arrays.push(Arc::new(StringArray::from(vec![
                                                v.as_str()
                                            ])));
                                        }
                                        Value::Bool(v) => {
                                            fields.push(Field::new(
                                                cn,
                                                ArrowDataType::Boolean,
                                                true,
                                            ));
                                            arrays.push(Arc::new(BooleanArray::from(vec![*v])));
                                        }
                                        Value::Null => {
                                            fields.push(Field::new(cn, ArrowDataType::Utf8, true));
                                            arrays.push(Arc::new(StringArray::from(vec![
                                                Option::<&str>::None,
                                            ])));
                                        }
                                        _ => {
                                            fields.push(Field::new(cn, ArrowDataType::Utf8, true));
                                            arrays.push(Arc::new(StringArray::from(vec![
                                                format!("{:?}", value).as_str(),
                                            ])));
                                        }
                                    }
                                }
                            }
                            if !fields.is_empty() {
                                let batch_schema = Arc::new(Schema::new(fields));
                                if let Ok(row_batch) = RecordBatch::try_new(batch_schema, arrays) {
                                    match Self::evaluate_predicate(&row_batch, expr) {
                                        Ok(mask) => {
                                            if mask.len() > 0 && !mask.value(0) {
                                                return Err(io::Error::new(
                                                    io::ErrorKind::InvalidInput,
                                                    format!("CHECK constraint violated: {} (column '{}', row {})", check_sql, schema_col, row_idx + 1),
                                                ));
                                            }
                                        }
                                        Err(_) => {
                                            // If evaluation fails (e.g., column not in batch), treat as violation
                                            // This handles cases where the CHECK references a column not present
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Enforce FOREIGN KEY constraints
        if storage.storage.has_constraints() {
            let (base_dir, _) = base_dir_and_table(storage_path);
            let schema = storage.get_schema();
            for (schema_col, _) in &schema {
                let cons = storage.storage.get_column_constraints(schema_col);
                if let Some((ref ref_table, ref ref_column)) = cons.foreign_key {
                    // Find column index in inserted data
                    let col_idx = col_names.iter().position(|n| n == schema_col);
                    if col_idx.is_none() {
                        continue;
                    }
                    let col_idx = col_idx.unwrap();

                    // Open referenced table
                    let ref_path = base_dir.join(format!("{}.apex", ref_table));
                    if !crate::storage::table_catalog::file_exists_or_registered(&ref_path)? {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!(
                                "FOREIGN KEY: referenced table '{}' does not exist",
                                ref_table
                            ),
                        ));
                    }
                    crate::storage::table_catalog::ensure_table_file(
                        &ref_path,
                        crate::storage::DurabilityLevel::Fast,
                    )?;
                    let ref_storage = TableStorageBackend::open(&ref_path)?;
                    let ref_batch =
                        ref_storage.read_columns_to_arrow(Some(&[ref_column.as_str()]), 0, None)?;
                    let ref_col_arr = ref_batch.column_by_name(ref_column);

                    // Check each inserted value exists in referenced column
                    for (row_idx, row_values) in values.iter().enumerate() {
                        if col_idx >= row_values.len() {
                            continue;
                        }
                        let val = &row_values[col_idx];
                        if matches!(val, Value::Null) {
                            continue;
                        } // NULL is allowed in FK

                        let found = if let Some(ref_arr) = ref_col_arr {
                            let mut exists = false;
                            for r in 0..ref_arr.len() {
                                if ref_arr.is_null(r) {
                                    continue;
                                }
                                let matches = match val {
                                    Value::Int64(v) => ref_arr
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .map(|a| a.value(r) == *v)
                                        .unwrap_or(false),
                                    Value::Float64(v) => ref_arr
                                        .as_any()
                                        .downcast_ref::<Float64Array>()
                                        .map(|a| (a.value(r) - v).abs() < f64::EPSILON)
                                        .unwrap_or(false),
                                    Value::String(v) => ref_arr
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .map(|a| a.value(r) == v.as_str())
                                        .unwrap_or(false),
                                    Value::Bool(v) => ref_arr
                                        .as_any()
                                        .downcast_ref::<BooleanArray>()
                                        .map(|a| a.value(r) == *v)
                                        .unwrap_or(false),
                                    _ => false,
                                };
                                if matches {
                                    exists = true;
                                    break;
                                }
                            }
                            exists
                        } else {
                            false
                        };

                        if !found {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("FOREIGN KEY constraint violated: value in column '{}' not found in {}.{} (row {})", schema_col, ref_table, ref_column, row_idx + 1),
                            ));
                        }
                    }
                }
            }
        }

        let storage = if storage.has_delta() {
            storage.compact()?;
            TableStorageBackend::open_for_write(storage_path)?
        } else {
            storage
        };

        // Capture row count before insert for index maintenance
        let pre_insert_count = storage.row_count();

        // Use insert_rows for proper NULL handling (Value::Null → ColumnValue::Null)
        storage.insert_rows(&rows)?;
        storage.save_full()?;

        // Update indexes for newly inserted rows
        Self::notify_index_insert(storage_path, &storage, pre_insert_count);
        // Update FTS index for newly inserted rows
        Self::notify_fts_insert(storage_path, &storage, pre_insert_count);

        // Invalidate cache after write to ensure subsequent reads get fresh data
        invalidate_storage_cache(storage_path);
        invalidate_table_stats(&storage_path.to_string_lossy());

        Ok(ApexResult::Scalar(rows_inserted))
    }

    pub(in crate::query::executor) fn execute_insert_on_conflict(
        storage_path: &Path,
        columns: Option<&[String]>,
        values: &[Vec<InsertValue>],
        conflict_columns: &[String],
        do_update: Option<&[(String, SqlExpr)]>,
    ) -> io::Result<ApexResult> {
        use std::collections::HashMap;

        crate::storage::table_catalog::ensure_table_file(
            storage_path,
            crate::storage::DurabilityLevel::Fast,
        )?;
        let _epoch_write = crate::storage::epoch::logical_write(storage_path);

        invalidate_storage_cache(storage_path);
        let storage = TableStorageBackend::open_for_write(storage_path)?;
        let schema = storage.get_schema();

        let col_names: Vec<String> = if let Some(cols) = columns {
            cols.to_vec()
        } else {
            schema.iter().map(|(n, _)| n.clone()).collect()
        };
        let resolved_values = Self::resolve_insert_values_for_path(storage_path, columns, values)?;
        let values = resolved_values.as_slice();

        // Read existing data for conflict detection
        let conflict_refs: Vec<&str> = conflict_columns.iter().map(|s| s.as_str()).collect();
        let mut read_cols: Vec<&str> = vec!["_id"];
        for c in &conflict_refs {
            if !read_cols.contains(c) {
                read_cols.push(c);
            }
        }
        let existing = storage.read_columns_to_arrow(Some(&read_cols), 0, None)?;

        // Build lookup: conflict key → row index in existing data
        let existing_rows = existing.num_rows();

        // Helper: extract value from Arrow column at row
        let extract_val = |col_name: &str, row: usize| -> Option<Value> {
            existing
                .column_by_name(col_name)
                .map(|c| Self::arrow_value_at_col(c, row))
        };

        // For each input row, check if it conflicts
        let mut inserted = 0i64;
        let mut updated = 0i64;

        for row_values in values {
            // Build this row's conflict key values
            let mut conflict_vals: Vec<Value> = Vec::new();
            for cc in conflict_columns {
                if let Some(idx) = col_names.iter().position(|n| n == cc) {
                    if idx < row_values.len() {
                        conflict_vals.push(row_values[idx].clone());
                    } else {
                        conflict_vals.push(Value::Null);
                    }
                } else {
                    conflict_vals.push(Value::Null);
                }
            }

            // Search for a matching existing row
            let mut conflict_row_id: Option<u64> = None;
            for er in 0..existing_rows {
                let mut all_match = true;
                for (ci, cc) in conflict_columns.iter().enumerate() {
                    let existing_val = extract_val(cc, er);
                    let new_val = &conflict_vals[ci];
                    let matches = match (&existing_val, new_val) {
                        (Some(Value::Int64(a)), Value::Int64(b)) => a == b,
                        (Some(Value::Int32(a)), Value::Int32(b)) => a == b,
                        (Some(Value::Int64(a)), Value::Int32(b)) => *a == *b as i64,
                        (Some(Value::Float64(a)), Value::Float64(b)) => {
                            (a - b).abs() < f64::EPSILON
                        }
                        (Some(Value::String(a)), Value::String(b)) => a == b,
                        (Some(Value::Bool(a)), Value::Bool(b)) => a == b,
                        (Some(Value::UInt64(a)), Value::Int64(b)) => *a as i64 == *b,
                        (Some(Value::Int64(a)), Value::UInt64(b)) => *a == *b as i64,
                        (Some(Value::Null), Value::Null) => false, // NULL != NULL
                        _ => false,
                    };
                    if !matches {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    // Found conflict — get row_id
                    if let Some(id_col) = existing.column_by_name("_id") {
                        if let Some(a) = id_col.as_any().downcast_ref::<UInt64Array>() {
                            conflict_row_id = Some(a.value(er));
                        } else if let Some(a) = id_col.as_any().downcast_ref::<Int64Array>() {
                            conflict_row_id = Some(a.value(er) as u64);
                        }
                    }
                    break;
                }
            }

            if let Some(rid) = conflict_row_id {
                // Conflict found
                if let Some(assignments) = do_update {
                    // DO UPDATE SET ...
                    // Build a row data map for expression evaluation
                    let mut row_data: HashMap<String, Value> = HashMap::new();
                    for (i, val) in row_values.iter().enumerate() {
                        if i < col_names.len() {
                            row_data.insert(col_names[i].clone(), val.clone());
                        }
                    }

                    let mut update_assignments: Vec<(String, SqlExpr)> = Vec::new();
                    for (col, expr) in assignments {
                        // Support EXCLUDED.col syntax: replace with the new row's value
                        let resolved_expr = match expr {
                            SqlExpr::Column(ref name)
                                if name.starts_with("EXCLUDED.")
                                    || name.starts_with("excluded.") =>
                            {
                                let src_col = &name[9..]; // skip "EXCLUDED." or "excluded."
                                if let Some(val) = row_data.get(src_col) {
                                    SqlExpr::Literal(val.clone())
                                } else {
                                    expr.clone()
                                }
                            }
                            _ => expr.clone(),
                        };
                        update_assignments.push((col.clone(), resolved_expr));
                    }

                    // Execute UPDATE for this specific row
                    let where_clause = SqlExpr::BinaryOp {
                        left: Box::new(SqlExpr::Column("_id".to_string())),
                        op: BinaryOperator::Eq,
                        right: Box::new(SqlExpr::Literal(Value::UInt64(rid))),
                    };
                    Self::execute_update(storage_path, &update_assignments, Some(&where_clause))?;
                    updated += 1;
                }
                // else DO NOTHING — skip this row
            } else {
                // No conflict — insert the row
                Self::execute_insert(storage_path, Some(&col_names), &[row_values.clone()])?;
                inserted += 1;
            }
        }

        invalidate_storage_cache(storage_path);
        invalidate_table_stats(&storage_path.to_string_lossy());
        Ok(ApexResult::Scalar(inserted + updated))
    }
}
