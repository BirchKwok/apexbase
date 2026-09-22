//! Scenario R3: CSV -> column-store batch ETL pipeline
//!
//! # Business background
//!
//! An upstream business system exports a CSV detail file every day. The typical
//! requirement is: **bulk-load it as fast as possible**, then clean, aggregate and
//! materialize it for downstream queries. This example shows how to land a CSV
//! directly into the column store from Rust and do the cleaning and topology in SQL.
//!
//! # ApexBase capabilities demonstrated
//!
//! 1. `register_temp_table()`: register a CSV as an mmap-backed temp table (the
//!    format is detected from the file extension).
//! 2. `insert_batch()` + a predefined schema: type-stable bulk writes.
//! 3. `insert_arrow()`: write an in-memory Arrow `RecordBatch` directly (the
//!    fastest bulk path).
//! 4. Cleaning (`CASE`/`CAST`/string functions), deduplication and aggregation in SQL.
//! 5. `count()` / aggregate-result validation to guarantee the import was complete.
//!
//! # How to run
//!
//! ```bash
//! cargo run --example r03_batch_etl_csv --no-default-features
//! ```

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

use apexbase::data::Value;
use apexbase::embedded::{ApexDB, Row};
use apexbase::storage::on_demand::ColumnType;
use apexbase::storage::DurabilityLevel;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema};
use arrow::record_batch::RecordBatch;

const ROW_COUNT: usize = 2_000;

/// Generate a "dirty data" CSV: inconsistent city capitalization, missing values and
/// one duplicate row. Dirty data is normal in real ETL, which is what makes the
/// cleaning step meaningful.
fn write_source_csv(path: &std::path::Path) -> std::io::Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    writeln!(w, "order_id,raw_city,amount,currency")?;
    for i in 0..ROW_COUNT {
        // Mix upper/lower case to mimic sloppy manual data entry.
        let city = match i % 4 {
            0 => "beijing",
            1 => "BEIJING",
            2 => "shanghai",
            _ => "Shanghai",
        };
        // Insert an empty amount every 50 rows to mimic missing values.
        let amount = if i % 50 == 0 {
            String::new()
        } else {
            format!("{:.2}", 10.0 + (i % 400) as f64)
        };
        writeln!(w, "{i},{city},{amount},CNY")?;
    }
    // Deliberately append one row with a duplicate order_id to demo deduplication.
    writeln!(w, "0,beijing,999.00,CNY")?;
    w.flush()
}

fn main() -> apexbase::Result<()> {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let csv_path = tmp.path().join("orders.csv");
    write_source_csv(&csv_path).expect("write csv");
    println!("Generated source CSV: {} ({} rows + 1 duplicate)", csv_path.display(), ROW_COUNT);

    let db = ApexDB::builder(tmp.path())
        .durability(DurabilityLevel::Fast)
        .drop_if_exists(true)
        .build()?;

    // ── Step 1: register the CSV as a temp table (zero-copy mmap read) ──────
    // register_temp_table parses the file once; later queries go through mmap +
    // zone maps. Note: temp tables are queried directly by name through
    // db.execute(), without use_table.
    db.register_temp_table("raw_orders", csv_path.to_str().unwrap())?;
    let raw_n = db
        .execute("SELECT COUNT(*) AS n FROM raw_orders")?
        .scalar()
        .unwrap_or(-1);
    println!("\nTemp table raw_orders row count: {raw_n}");
    assert_eq!(raw_n as usize, ROW_COUNT + 1, "the CSV row count must include the duplicate");

    // Probe the raw CSV directly: which city spellings are inconsistent.
    let rs = db.execute(
        "SELECT raw_city, COUNT(*) AS n
         FROM raw_orders
         GROUP BY raw_city
         ORDER BY n DESC",
    )?;
    println!("Raw city spelling distribution (before cleaning):");
    let raw_rows = rs.to_rows()?;
    let distinct_raw = raw_rows.len();
    for row in &raw_rows {
        println!("  {:?} -> {:?}", row.get("raw_city"), row.get("n"));
    }
    // The dirty data spells the same city with different capitalization, so there
    // are several "cities" before cleaning.
    assert!(
        distinct_raw > 2,
        "before cleaning there should be case-inconsistent duplicate city spellings (actual {distinct_raw})"
    );

    // ── Step 2: clean and materialize into a column-store table ─────────────
    // Predefine the schema: bulk writes are more stable with fixed types, and it
    // avoids re-inferring types per batch.
    let clean = db.create_table_with_schema(
        "clean_orders",
        &[
            ("order_id".to_string(), ColumnType::Int64),
            ("city".to_string(), ColumnType::String),
            ("amount".to_string(), ColumnType::Float64),
            ("currency".to_string(), ColumnType::String),
        ],
    )?;

    // Step 2a: use SQL to find duplicate order_ids.
    // GROUP BY order_id counts how often each order appears; COUNT(*) > 1 means a
    // duplicate. (String aggregation is not used here: ApexBase returns NULL for
    // MIN/MAX on a string column, so choosing among string values is left to the
    // Rust side — see step 2b.)
    let rs = db.execute(
        "SELECT order_id, COUNT(*) AS n
         FROM raw_orders
         GROUP BY order_id
         HAVING COUNT(*) > 1",
    )?;
    let dup_rows = rs.to_rows()?;
    println!("\nDetected duplicate order_ids: {}", dup_rows.len());
    let mut duplicate_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for row in &dup_rows {
        if let Some(Value::Int64(id)) = row.get("order_id") {
            duplicate_ids.insert(*id);
            println!("  order_id={id} appears {:?} times", row.get("n"));
        }
    }
    assert_eq!(duplicate_ids.len(), 1, "this example deliberately creates exactly 1 duplicate order");

    // Step 2b: read every row and normalize + dedupe by order_id on the Rust side
    // (keeping the first occurrence). ORDER BY order_id makes "first occurrence"
    // deterministic.
    let rs = db.execute(
        "SELECT order_id, raw_city, COALESCE(CAST(amount AS DOUBLE), 0.0) AS amount, currency
         FROM raw_orders
         ORDER BY order_id",
    )?;
    let raw_rows = rs.to_rows()?;

    let mut seen_orders: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut cleaned: Vec<Row> = Vec::with_capacity(ROW_COUNT);
    for row in &raw_rows {
        let order_id = match row.get("order_id") {
            Some(Value::Int64(v)) => *v,
            _ => continue,
        };
        // Deduplicate: keep only the first row for each order_id.
        if !seen_orders.insert(order_id) {
            continue;
        }
        let raw_city = match row.get("raw_city") {
            Some(Value::String(s)) => s.clone(),
            _ => String::new(),
        };
        // Normalize the city to "first letter uppercase + rest lowercase":
        // beijing/BEIJING -> Beijing
        let normalized_city = {
            let mut chars = raw_city.chars();
            match chars.next() {
                Some(first) => {
                    let upper: String = first.to_uppercase().collect();
                    format!("{upper}{}", chars.as_str().to_lowercase())
                }
                None => String::new(),
            }
        };
        let amount = match row.get("amount") {
            Some(Value::Float64(v)) => *v,
            Some(Value::Int64(v)) => *v as f64,
            _ => 0.0,
        };
        let currency = match row.get("currency") {
            Some(Value::String(s)) => s.clone(),
            _ => String::new(),
        };

        let mut clean_row = HashMap::new();
        clean_row.insert("order_id".to_string(), Value::Int64(order_id));
        clean_row.insert("city".to_string(), Value::String(normalized_city));
        clean_row.insert("amount".to_string(), Value::Float64(amount));
        clean_row.insert("currency".to_string(), Value::String(currency));
        cleaned.push(clean_row);
    }
    println!("Rows ready to write after cleaning + dedup: {}", cleaned.len());

    let ids = clean.insert_batch(&cleaned)?;
    println!("Wrote column-store table clean_orders: {} rows", ids.len());

    // After dedup the row count must be exactly the source count (the duplicate
    // order_id was merged away).
    assert_eq!(ids.len(), ROW_COUNT, "dedup should return exactly {ROW_COUNT} rows");

    // ── Step 3: aggregate analysis on the column-store table ────────────────
    let rs = clean.execute(
        "SELECT city, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
         FROM clean_orders
         GROUP BY city
         ORDER BY revenue DESC",
    )?;
    println!("\nTotals by city (after cleaning):");
    let mut total = 0.0f64;
    for row in rs.to_rows()? {
        let city = match row.get("city") {
            Some(Value::String(s)) => s.clone(),
            _ => "?".to_string(),
        };
        let revenue = match row.get("revenue") {
            Some(Value::Float64(v)) => *v,
            _ => 0.0,
        };
        total += revenue;
        println!("  {city}: revenue {revenue}");
    }
    println!("  total: {:.2}", total);

    // ── Step 4: write an Arrow RecordBatch directly (fastest bulk path) ─────
    // When the data already exists upstream as Arrow (for example from
    // DataFusion/Arrow Flight), insert_arrow avoids one row-to-column conversion.
    let fast = db.create_table_with_schema(
        "arrow_orders",
        &[
            ("order_id".to_string(), ColumnType::Int64),
            ("city".to_string(), ColumnType::String),
            ("amount".to_string(), ColumnType::Float64),
        ],
    )?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", ArrowType::Int64, false),
        Field::new("city", ArrowType::Utf8, false),
        Field::new("amount", ArrowType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["Beijing", "Shanghai", "Beijing", "Shenzhen"])),
            Arc::new(Float64Array::from(vec![120.5, 80.0, 45.25, 300.0])),
        ],
    )
    .expect("build record batch");

    let arrow_ids = fast.insert_arrow(&batch)?;
    println!("\ninsert_arrow wrote {} rows", arrow_ids.len());
    assert_eq!(fast.count()?, 4, "there must be 4 rows after the Arrow write");

    let rs = fast.execute(
        "SELECT city, ROUND(SUM(amount), 2) AS revenue
         FROM arrow_orders
         GROUP BY city
         ORDER BY revenue DESC",
    )?;
    println!("Arrow table aggregate result:");
    for row in rs.to_rows()? {
        println!("  {:?} -> {:?}", row.get("city"), row.get("revenue"));
    }

    // ── Step 5: clean up the temp table ─────────────────────────────────────
    // Temp tables are also cleaned up automatically when the ApexDB instance is
    // dropped; releasing it explicitly here shows the full lifecycle.
    db.drop_temp_table("raw_orders")?;
    println!("\nReleased temp table raw_orders");

    // Final check: the cleaned table should have no missing values other than the
    // amount fallback, and the cities should be normalized.
    let cities = clean
        .execute("SELECT COUNT(DISTINCT city) AS n FROM clean_orders")?
        .scalar()
        .unwrap_or(-1);
    assert_eq!(cities, 2, "after cleaning only Beijing/Shanghai should remain");
    println!("[OK] distinct cities after cleaning = {cities}");

    println!("\n=== Scenario R3 complete ===");
    Ok(())
}
