//! pg_catalog compatibility layer for DBeaver and other PostgreSQL clients
//!
//! Intercepts system catalog queries that clients use to discover schema metadata
//! and returns compatible responses from ApexBase's actual table metadata.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;

/// Check if a query targets pg_catalog or information_schema and handle it
pub fn try_handle_catalog_query(sql: &str, base_dir: &Path) -> Option<RecordBatch> {
    let sql_lower = sql.trim().to_lowercase();

    // DBeaver initial connection probes
    if sql_lower.contains("select version()") || sql_lower == "select version()" {
        return Some(make_version_batch());
    }

    if sql_lower.contains("current_database()") && !sql_lower.contains("pg_catalog") {
        return Some(make_current_database_batch());
    }

    if sql_lower.contains("current_schema()") || sql_lower.contains("current_schema") {
        return Some(make_current_schema_batch());
    }

    if sql_lower.contains("pg_catalog.pg_namespace") || sql_lower.contains("from pg_namespace") {
        return Some(make_pg_namespace_batch());
    }

    if sql_lower.contains("pg_catalog.pg_database") || sql_lower.contains("from pg_database") {
        return Some(make_pg_database_batch());
    }

    if sql_lower.contains("pg_catalog.pg_class") || sql_lower.contains("from pg_class") {
        return Some(make_pg_class_batch(base_dir));
    }

    if sql_lower.contains("pg_catalog.pg_attribute") || sql_lower.contains("from pg_attribute") {
        return Some(make_pg_attribute_batch(base_dir));
    }

    if sql_lower.contains("pg_catalog.pg_type") || sql_lower.contains("from pg_type") {
        return Some(make_pg_type_batch());
    }

    if sql_lower.contains("information_schema.tables") {
        return Some(make_information_schema_tables(base_dir));
    }

    if sql_lower.contains("information_schema.columns") {
        return Some(make_information_schema_columns(base_dir));
    }

    if sql_lower.contains("pg_catalog.pg_settings") || sql_lower.contains("from pg_settings") {
        return Some(make_pg_settings_batch());
    }

    if sql_lower.contains("pg_catalog.pg_tables") || sql_lower.contains("from pg_tables") {
        return Some(make_pg_tables_batch(base_dir));
    }

    if sql_lower.contains("pg_stat_user_tables") || sql_lower.contains("pg_stat_all_tables") {
        return Some(make_pg_stat_tables_batch(base_dir));
    }

    if sql_lower.contains("pg_catalog.pg_am") || sql_lower.contains("from pg_am") {
        return Some(make_empty_batch_with_schema(&[
            ("amname", ArrowDataType::Utf8),
            ("oid", ArrowDataType::Int32),
        ]));
    }

    if sql_lower.contains("pg_catalog.pg_index") || sql_lower.contains("from pg_index") {
        return Some(make_empty_batch_with_schema(&[
            ("indexrelid", ArrowDataType::Int32),
            ("indrelid", ArrowDataType::Int32),
        ]));
    }

    if sql_lower.contains("pg_catalog.pg_constraint") || sql_lower.contains("from pg_constraint") {
        return Some(make_empty_batch_with_schema(&[
            ("conname", ArrowDataType::Utf8),
            ("contype", ArrowDataType::Utf8),
        ]));
    }

    if sql_lower.contains("pg_catalog.pg_proc") || sql_lower.contains("from pg_proc") {
        return Some(make_empty_batch_with_schema(&[
            ("proname", ArrowDataType::Utf8),
            ("oid", ArrowDataType::Int32),
        ]));
    }

    if sql_lower.contains("pg_catalog.pg_description") || sql_lower.contains("from pg_description")
    {
        return Some(make_empty_batch_with_schema(&[
            ("objoid", ArrowDataType::Int32),
            ("description", ArrowDataType::Utf8),
        ]));
    }

    // Transaction control: treat as no-ops (auto-commit semantics)
    // DBeaver/JDBC with autocommit OFF wraps every statement in BEGIN/COMMIT.
    // Our DDL/DML already auto-commits, so we just acknowledge these.
    if sql_lower.starts_with("begin")
        || sql_lower.starts_with("commit")
        || sql_lower.starts_with("rollback")
        || sql_lower.starts_with("savepoint")
        || sql_lower.starts_with("release ")
        || sql_lower.starts_with("end")
    {
        return Some(make_empty_ok_batch());
    }

    // SET statements (DBeaver sends these on connect)
    if sql_lower.starts_with("set ") {
        return Some(make_empty_ok_batch());
    }

    // SHOW statements
    if sql_lower.starts_with("show ") {
        if sql_lower.contains("transaction isolation level") {
            return Some(make_single_string_batch(
                "transaction_isolation",
                "read committed",
            ));
        }
        if sql_lower.contains("server_version") {
            return Some(make_single_string_batch("server_version", "15.0"));
        }
        if sql_lower.contains("standard_conforming_strings") {
            return Some(make_single_string_batch(
                "standard_conforming_strings",
                "on",
            ));
        }
        if sql_lower.contains("search_path") {
            return Some(make_single_string_batch("search_path", "public"));
        }
        return Some(make_single_string_batch("setting", ""));
    }

    None
}

// ============================================================================
// Catalog response builders
// ============================================================================

fn make_version_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "version",
        ArrowDataType::Utf8,
        false,
    )]));
    let array: ArrayRef = Arc::new(StringArray::from(vec!["PostgreSQL 15.0 (ApexBase)"]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

fn make_current_database_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "current_database",
        ArrowDataType::Utf8,
        false,
    )]));
    let array: ArrayRef = Arc::new(StringArray::from(vec!["apexbase"]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

fn make_current_schema_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "current_schema",
        ArrowDataType::Utf8,
        false,
    )]));
    let array: ArrayRef = Arc::new(StringArray::from(vec!["public"]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

fn make_pg_namespace_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("nspname", ArrowDataType::Utf8, false),
        Field::new("nspowner", ArrowDataType::Int32, false),
    ]));
    let oids: ArrayRef = Arc::new(Int32Array::from(vec![11, 2200]));
    let names: ArrayRef = Arc::new(StringArray::from(vec!["pg_catalog", "public"]));
    let owners: ArrayRef = Arc::new(Int32Array::from(vec![10, 10]));
    RecordBatch::try_new(schema, vec![oids, names, owners]).unwrap()
}

fn make_pg_database_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("datname", ArrowDataType::Utf8, false),
        Field::new("datdba", ArrowDataType::Int32, false),
        Field::new("encoding", ArrowDataType::Int32, false),
    ]));
    let oids: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let names: ArrayRef = Arc::new(StringArray::from(vec!["apexbase"]));
    let owners: ArrayRef = Arc::new(Int32Array::from(vec![10]));
    let encodings: ArrayRef = Arc::new(Int32Array::from(vec![6])); // UTF8
    RecordBatch::try_new(schema, vec![oids, names, owners, encodings]).unwrap()
}

fn make_pg_settings_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", ArrowDataType::Utf8, false),
        Field::new("setting", ArrowDataType::Utf8, false),
    ]));
    let names: ArrayRef = Arc::new(StringArray::from(vec![
        "server_version",
        "server_encoding",
        "client_encoding",
        "standard_conforming_strings",
        "search_path",
    ]));
    let settings: ArrayRef = Arc::new(StringArray::from(vec![
        "15.0", "UTF8", "UTF8", "on", "public",
    ]));
    RecordBatch::try_new(schema, vec![names, settings]).unwrap()
}

fn make_pg_type_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("typname", ArrowDataType::Utf8, false),
        Field::new("typnamespace", ArrowDataType::Int32, false),
        Field::new("typlen", ArrowDataType::Int32, false),
    ]));
    let oids: ArrayRef = Arc::new(Int32Array::from(vec![
        20, 23, 25, 701, 16, 17, 1043, 1082, 1114,
    ]));
    let names: ArrayRef = Arc::new(StringArray::from(vec![
        "int8",
        "int4",
        "text",
        "float8",
        "bool",
        "bytea",
        "varchar",
        "date",
        "timestamp",
    ]));
    let ns: ArrayRef = Arc::new(Int32Array::from(vec![11; 9]));
    let lens: ArrayRef = Arc::new(Int32Array::from(vec![8, 4, -1, 8, 1, -1, -1, 4, 8]));
    RecordBatch::try_new(schema, vec![oids, names, ns, lens]).unwrap()
}

/// List .apex files in data_dir as tables
fn discover_tables(base_dir: &Path) -> Vec<String> {
    let mut tables = Vec::new();
    if let Ok(entries) = std::fs::read_dir(base_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "apex") {
                if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                    // Skip the main apexbase.apex — tables are separate files
                    tables.push(name.to_string());
                }
            }
            // Also check subdirectories (table directories)
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    // Check if it contains .apex files
                    if let Ok(sub_entries) = std::fs::read_dir(&path) {
                        for sub_entry in sub_entries.flatten() {
                            if sub_entry
                                .path()
                                .extension()
                                .map_or(false, |ext| ext == "apex")
                            {
                                if let Some(tbl_name) =
                                    sub_entry.path().file_stem().and_then(|s| s.to_str())
                                {
                                    tables.push(tbl_name.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if tables.is_empty() {
        tables.push("default".to_string());
    }
    tables.sort();
    tables.dedup();
    tables
}

/// Get column info for a table: (name, arrow_type)
fn get_table_columns(base_dir: &Path, table_name: &str) -> Vec<(String, ArrowDataType)> {
    // Try to find the .apex file
    let table_path = base_dir.join(format!("{}.apex", table_name));
    if !table_path.exists() {
        return vec![("_id".to_string(), ArrowDataType::Int64)];
    }

    // Execute a LIMIT 0 query to get schema
    match crate::Session::new(&table_path, &table_path).execute("SELECT * FROM data LIMIT 0") {
        Ok(result) => match result.to_record_batch() {
            Ok(batch) => batch
                .schema()
                .fields()
                .iter()
                .map(|f| (f.name().clone(), f.data_type().clone()))
                .collect(),
            Err(_) => vec![("_id".to_string(), ArrowDataType::Int64)],
        },
        Err(_) => vec![("_id".to_string(), ArrowDataType::Int64)],
    }
}

fn make_pg_class_batch(base_dir: &Path) -> RecordBatch {
    let tables = discover_tables(base_dir);
    let n = tables.len();

    // Comprehensive pg_class schema matching PostgreSQL's catalog structure.
    // DBeaver queries c.* and expects many of these columns by name.
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("relname", ArrowDataType::Utf8, false),
        Field::new("relnamespace", ArrowDataType::Int32, false),
        Field::new("reltype", ArrowDataType::Int32, false),
        Field::new("reloftype", ArrowDataType::Int32, false),
        Field::new("relowner", ArrowDataType::Int32, false),
        Field::new("relam", ArrowDataType::Int32, false),
        Field::new("relfilenode", ArrowDataType::Int32, false),
        Field::new("reltablespace", ArrowDataType::Int32, false),
        Field::new("relpages", ArrowDataType::Int32, false),
        Field::new("reltuples", ArrowDataType::Float64, false),
        Field::new("relallvisible", ArrowDataType::Int32, false),
        Field::new("reltoastrelid", ArrowDataType::Int32, false),
        Field::new("relhasindex", ArrowDataType::Boolean, false),
        Field::new("relisshared", ArrowDataType::Boolean, false),
        Field::new("relpersistence", ArrowDataType::Utf8, false),
        Field::new("relkind", ArrowDataType::Utf8, false),
        Field::new("relnatts", ArrowDataType::Int32, false),
        Field::new("relchecks", ArrowDataType::Int32, false),
        Field::new("relhasrules", ArrowDataType::Boolean, false),
        Field::new("relhastriggers", ArrowDataType::Boolean, false),
        Field::new("relhassubclass", ArrowDataType::Boolean, false),
        Field::new("relrowsecurity", ArrowDataType::Boolean, false),
        Field::new("relforcerowsecurity", ArrowDataType::Boolean, false),
        Field::new("relispopulated", ArrowDataType::Boolean, false),
        Field::new("relreplident", ArrowDataType::Utf8, false),
        Field::new("relispartition", ArrowDataType::Boolean, false),
    ]));

    let oid_vals: Vec<i32> = (0..n as i32).map(|i| 16384 + i).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(oid_vals.clone())), // oid
        Arc::new(StringArray::from(tables)),          // relname
        Arc::new(Int32Array::from(vec![2200; n])),    // relnamespace (public)
        Arc::new(Int32Array::from(vec![0; n])),       // reltype
        Arc::new(Int32Array::from(vec![0; n])),       // reloftype
        Arc::new(Int32Array::from(vec![10; n])),      // relowner
        Arc::new(Int32Array::from(vec![2; n])),       // relam (heap)
        Arc::new(Int32Array::from(oid_vals)),         // relfilenode = oid
        Arc::new(Int32Array::from(vec![0; n])),       // reltablespace
        Arc::new(Int32Array::from(vec![0; n])),       // relpages
        Arc::new(arrow::array::Float64Array::from(vec![-1.0; n])), // reltuples (-1 = unknown)
        Arc::new(Int32Array::from(vec![0; n])),       // relallvisible
        Arc::new(Int32Array::from(vec![0; n])),       // reltoastrelid
        Arc::new(BooleanArray::from(vec![false; n])), // relhasindex
        Arc::new(BooleanArray::from(vec![false; n])), // relisshared
        Arc::new(StringArray::from(vec!["p"; n])),    // relpersistence (permanent)
        Arc::new(StringArray::from(vec!["r"; n])),    // relkind (regular table)
        Arc::new(Int32Array::from(vec![0; n])), // relnatts (filled below would need per-table)
        Arc::new(Int32Array::from(vec![0; n])), // relchecks
        Arc::new(BooleanArray::from(vec![false; n])), // relhasrules
        Arc::new(BooleanArray::from(vec![false; n])), // relhastriggers
        Arc::new(BooleanArray::from(vec![false; n])), // relhassubclass
        Arc::new(BooleanArray::from(vec![false; n])), // relrowsecurity
        Arc::new(BooleanArray::from(vec![false; n])), // relforcerowsecurity
        Arc::new(BooleanArray::from(vec![true; n])), // relispopulated
        Arc::new(StringArray::from(vec!["d"; n])), // relreplident (default)
        Arc::new(BooleanArray::from(vec![false; n])), // relispartition
    ];

    RecordBatch::try_new(schema, columns).unwrap()
}

fn make_pg_tables_batch(base_dir: &Path) -> RecordBatch {
    let tables = discover_tables(base_dir);
    let n = tables.len();

    let schema = Arc::new(Schema::new(vec![
        Field::new("schemaname", ArrowDataType::Utf8, false),
        Field::new("tablename", ArrowDataType::Utf8, false),
        Field::new("tableowner", ArrowDataType::Utf8, false),
        Field::new("tablespace", ArrowDataType::Utf8, true),
        Field::new("hasindexes", ArrowDataType::Boolean, false),
        Field::new("hasrules", ArrowDataType::Boolean, false),
        Field::new("hastriggers", ArrowDataType::Boolean, false),
        Field::new("rowsecurity", ArrowDataType::Boolean, false),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["public"; n])),
        Arc::new(StringArray::from(tables)),
        Arc::new(StringArray::from(vec!["apex"; n])),
        Arc::new(StringArray::from(vec![None::<&str>; n])),
        Arc::new(BooleanArray::from(vec![false; n])),
        Arc::new(BooleanArray::from(vec![false; n])),
        Arc::new(BooleanArray::from(vec![false; n])),
        Arc::new(BooleanArray::from(vec![false; n])),
    ];

    RecordBatch::try_new(schema, columns).unwrap()
}

fn make_pg_stat_tables_batch(base_dir: &Path) -> RecordBatch {
    let tables = discover_tables(base_dir);
    let n = tables.len();

    let schema = Arc::new(Schema::new(vec![
        Field::new("relid", ArrowDataType::Int32, false),
        Field::new("schemaname", ArrowDataType::Utf8, false),
        Field::new("relname", ArrowDataType::Utf8, false),
        Field::new("n_live_tup", ArrowDataType::Int64, false),
        Field::new("n_dead_tup", ArrowDataType::Int64, false),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(
            (0..n as i32).map(|i| 16384 + i).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec!["public"; n])),
        Arc::new(StringArray::from(tables)),
        Arc::new(Int64Array::from(vec![0i64; n])),
        Arc::new(Int64Array::from(vec![0i64; n])),
    ];

    RecordBatch::try_new(schema, columns).unwrap()
}

fn make_pg_attribute_batch(base_dir: &Path) -> RecordBatch {
    let tables = discover_tables(base_dir);

    let mut attr_names = Vec::new();
    let mut attr_relids = Vec::new();
    let mut attr_nums = Vec::new();
    let mut attr_types = Vec::new();
    let mut attr_notnulls = Vec::new();

    for (table_idx, table_name) in tables.iter().enumerate() {
        let columns = get_table_columns(base_dir, table_name);
        for (col_idx, (col_name, arrow_type)) in columns.iter().enumerate() {
            attr_names.push(col_name.clone());
            attr_relids.push(16384 + table_idx as i32);
            attr_nums.push(col_idx as i32 + 1);
            attr_types.push(arrow_type_to_pg_oid(arrow_type));
            attr_notnulls.push(col_name == "_id");
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("attrelid", ArrowDataType::Int32, false),
        Field::new("attname", ArrowDataType::Utf8, false),
        Field::new("attnum", ArrowDataType::Int32, false),
        Field::new("atttypid", ArrowDataType::Int32, false),
        Field::new("attnotnull", ArrowDataType::Boolean, false),
    ]));

    let relids: ArrayRef = Arc::new(Int32Array::from(attr_relids));
    let names: ArrayRef = Arc::new(StringArray::from(attr_names));
    let nums: ArrayRef = Arc::new(Int32Array::from(attr_nums));
    let types: ArrayRef = Arc::new(Int32Array::from(attr_types));
    let notnulls: ArrayRef = Arc::new(BooleanArray::from(attr_notnulls));

    RecordBatch::try_new(schema, vec![relids, names, nums, types, notnulls]).unwrap()
}

fn make_information_schema_tables(base_dir: &Path) -> RecordBatch {
    let tables = discover_tables(base_dir);

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("table_type", ArrowDataType::Utf8, false),
    ]));

    let n = tables.len();
    let catalogs: ArrayRef = Arc::new(StringArray::from(vec!["apexbase"; n]));
    let schemas: ArrayRef = Arc::new(StringArray::from(vec!["public"; n]));
    let names: ArrayRef = Arc::new(StringArray::from(tables));
    let types: ArrayRef = Arc::new(StringArray::from(vec!["BASE TABLE"; n]));

    RecordBatch::try_new(schema, vec![catalogs, schemas, names, types]).unwrap()
}

fn make_information_schema_columns(base_dir: &Path) -> RecordBatch {
    let tables = discover_tables(base_dir);

    let mut col_catalogs = Vec::new();
    let mut col_schemas = Vec::new();
    let mut col_tables = Vec::new();
    let mut col_names = Vec::new();
    let mut col_positions = Vec::new();
    let mut col_types = Vec::new();
    let mut col_nullables = Vec::new();

    for table_name in &tables {
        let columns = get_table_columns(base_dir, table_name);
        for (idx, (col_name, arrow_type)) in columns.iter().enumerate() {
            col_catalogs.push("apexbase".to_string());
            col_schemas.push("public".to_string());
            col_tables.push(table_name.clone());
            col_names.push(col_name.clone());
            col_positions.push(idx as i64 + 1);
            col_types.push(arrow_type_to_pg_name(arrow_type));
            col_nullables.push(if col_name == "_id" { "NO" } else { "YES" }.to_string());
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("column_name", ArrowDataType::Utf8, false),
        Field::new("ordinal_position", ArrowDataType::Int64, false),
        Field::new("data_type", ArrowDataType::Utf8, false),
        Field::new("is_nullable", ArrowDataType::Utf8, false),
    ]));

    let catalogs: ArrayRef = Arc::new(StringArray::from(col_catalogs));
    let schemas: ArrayRef = Arc::new(StringArray::from(col_schemas));
    let tables_arr: ArrayRef = Arc::new(StringArray::from(col_tables));
    let names: ArrayRef = Arc::new(StringArray::from(col_names));
    let positions: ArrayRef = Arc::new(Int64Array::from(col_positions));
    let types: ArrayRef = Arc::new(StringArray::from(col_types));
    let nullables: ArrayRef = Arc::new(StringArray::from(col_nullables));

    RecordBatch::try_new(
        schema,
        vec![
            catalogs, schemas, tables_arr, names, positions, types, nullables,
        ],
    )
    .unwrap()
}

// ============================================================================
// Helpers
// ============================================================================

fn make_empty_ok_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "result",
        ArrowDataType::Utf8,
        false,
    )]));
    RecordBatch::new_empty(schema)
}

fn make_single_string_batch(col_name: &str, value: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        col_name,
        ArrowDataType::Utf8,
        false,
    )]));
    let array: ArrayRef = Arc::new(StringArray::from(vec![value]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

fn make_empty_batch_with_schema(cols: &[(&str, ArrowDataType)]) -> RecordBatch {
    let fields: Vec<Field> = cols
        .iter()
        .map(|(name, dt)| Field::new(*name, dt.clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::new_empty(schema)
}

fn arrow_type_to_pg_oid(dt: &ArrowDataType) -> i32 {
    match dt {
        ArrowDataType::Boolean => 16,
        ArrowDataType::Int8
        | ArrowDataType::UInt8
        | ArrowDataType::Int16
        | ArrowDataType::UInt16 => 21,
        ArrowDataType::Int32 | ArrowDataType::UInt32 => 23,
        ArrowDataType::Int64 | ArrowDataType::UInt64 => 20,
        ArrowDataType::Float32 => 700,
        ArrowDataType::Float64 => 701,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => 25,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => 17,
        ArrowDataType::Date32 | ArrowDataType::Date64 => 1082,
        ArrowDataType::Timestamp(_, _) => 1114,
        ArrowDataType::Dictionary(_, v) => arrow_type_to_pg_oid(v),
        _ => 25, // text fallback
    }
}

fn arrow_type_to_pg_name(dt: &ArrowDataType) -> String {
    match dt {
        ArrowDataType::Boolean => "boolean".to_string(),
        ArrowDataType::Int8
        | ArrowDataType::UInt8
        | ArrowDataType::Int16
        | ArrowDataType::UInt16 => "smallint".to_string(),
        ArrowDataType::Int32 | ArrowDataType::UInt32 => "integer".to_string(),
        ArrowDataType::Int64 | ArrowDataType::UInt64 => "bigint".to_string(),
        ArrowDataType::Float32 => "real".to_string(),
        ArrowDataType::Float64 => "double precision".to_string(),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => "text".to_string(),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => "bytea".to_string(),
        ArrowDataType::Date32 | ArrowDataType::Date64 => "date".to_string(),
        ArrowDataType::Timestamp(_, _) => "timestamp".to_string(),
        ArrowDataType::Dictionary(_, v) => arrow_type_to_pg_name(v),
        _ => "text".to_string(),
    }
}
