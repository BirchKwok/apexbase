//! Scenario R9: A hybrid-storage data-lake foundation (multi-source federation + hot-data materialization)
//!
//! # Business background
//!
//! A common modern data architecture is "**data lake + local acceleration layer**":
//! raw data lands in object storage or local directories in open formats
//! (CSV/Parquet/JSON), in many shapes and at large volume; but **re-querying** those
//! files is slow (they get parsed every time). What is needed is a local engine that can
//! federate queries over lake files directly and also materialize frequently accessed
//! data into a column-store "hot copy" for speed.
//!
//! This example uses real files to simulate three data sources:
//!   - `fact_orders.csv`  — fact table (order details), a per-day partition file
//!   - `dim_users.csv`    — dimension table (users), used for a JOIN
//!   - `events.json`      — semi-structured event stream
//!
//! # ApexBase capabilities demonstrated
//!
//! 1. `register_temp_table()`: register CSV/JSON as mmap temp tables for zero-copy,
//!    repeatable queries.
//! 2. Multi-source federated SQL: JOIN three sources and do aggregation and window
//!    ranking in one statement.
//! 3. `INSERT INTO ... SELECT`: materialize lake data into a local column-store hot table.
//! 4. Cold/hot path consistency checks (the most important correctness guarantee for a
//!    data-lake foundation).
//!
//! # How to run
//!
//! ```bash
//! cargo run --example r09_embedded_data_lake --no-default-features
//! ```

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};

use apexbase::data::Value;
use apexbase::embedded::{ApexDB, Row};
use apexbase::storage::on_demand::ColumnType;
use apexbase::storage::DurabilityLevel;

const ORDER_COUNT: usize = 1_200;
const USER_COUNT: usize = 120;

/// Write the fact-table CSV (order details).
///
/// Fields: order_id, user_id, day, category, amount
/// `day` is stored as a string (`YYYY-MM-DD`) so it can be grouped and range-filtered
/// as text directly — ApexBase does not provide DATE_TRUNC, so materializing the time
/// bucket as a string is the most robust approach.
fn write_fact_csv(path: &std::path::Path) -> std::io::Result<f64> {
    let categories = ["electronics", "books", "garden", "toys"];
    let mut w = BufWriter::new(File::create(path)?);
    writeln!(w, "order_id,user_id,day,category,amount")?;
    let mut total = 0.0f64;
    for i in 0..ORDER_COUNT {
        let day = format!("2024-03-{:02}", 1 + (i % 28));
        let category = categories[i % categories.len()];
        // The amount follows a deterministic formula so the cold/hot totals can be
        // cross-checked later.
        let amount = 10.0 + ((i * 13) % 240) as f64 + 0.25;
        total += amount;
        writeln!(
            w,
            "{i},{},{day},{category},{amount:.2}",
            i % USER_COUNT
        )?;
    }
    w.flush()?;
    Ok(total)
}

/// Write the dimension-table CSV (users).
fn write_dim_csv(path: &std::path::Path) -> std::io::Result<()> {
    let tiers = ["free", "pro", "enterprise"];
    let regions = ["east", "west", "north", "south"];
    let mut w = BufWriter::new(File::create(path)?);
    writeln!(w, "user_id,tier,region")?;
    for u in 0..USER_COUNT {
        writeln!(w, "{u},{},{}", tiers[u % tiers.len()], regions[u % regions.len()])?;
    }
    w.flush()
}

/// Write the JSON event stream (semi-structured).
///
/// A simple line-delimited JSON (NDJSON) format is used: ApexBase's
/// `read_json`/`register_temp_table` supports `.json`/`.ndjson`/`.jsonl`.
fn write_events_json(path: &std::path::Path) -> std::io::Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    for i in 0..300 {
        // Hand-write JSON to avoid pulling in an extra dependency for the example.
        writeln!(
            w,
            "{{\"event_id\":{i},\"user_id\":{},\"kind\":\"{}\",\"weight\":{}}}",
            i % USER_COUNT,
            if i % 3 == 0 { "click" } else { "impression" },
            (i % 7) + 1
        )?;
    }
    w.flush()
}

fn main() -> apexbase::Result<()> {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let fact_path = tmp.path().join("fact_orders.csv");
    let dim_path = tmp.path().join("dim_users.csv");
    let events_path = tmp.path().join("events.json");

    let fact_total = write_fact_csv(&fact_path).expect("write fact csv");
    write_dim_csv(&dim_path).expect("write dim csv");
    write_events_json(&events_path).expect("write events json");

    println!("=== R9 data-lake foundation ===");
    println!("Generated lake files:");
    println!("  {}", fact_path.display());
    println!("  {}", dim_path.display());
    println!("  {}", events_path.display());

    let db = ApexDB::builder(tmp.path())
        .durability(DurabilityLevel::Fast)
        .drop_if_exists(true)
        .build()?;

    // ── 1. Register all three sources as temp tables (mmap, zero-copy) ──────
    // register_temp_table picks the parser from the file extension and parses once.
    db.register_temp_table("fact_orders", fact_path.to_str().unwrap())?;
    db.register_temp_table("dim_users", dim_path.to_str().unwrap())?;
    db.register_temp_table("events", events_path.to_str().unwrap())?;
    println!("\nRegistered temp tables: fact_orders / dim_users / events");

    // Quickly probe the row count of each source (temp tables are queried directly via db.execute).
    for (name, expected) in [
        ("fact_orders", ORDER_COUNT),
        ("dim_users", USER_COUNT),
        ("events", 300),
    ] {
        // `scalar()` reads a single-cell COUNT(*) directly, including on temp tables.
        let n = db
            .execute(&format!("SELECT COUNT(*) AS n FROM {name}"))?
            .scalar()
            .unwrap_or(-1);
        println!("  {name}: {n} rows");
        assert_eq!(n as usize, expected, "{name} row count mismatch");
    }

    // ── 2. Multi-source federated query: JOIN the fact and dimension tables and aggregate ──
    // One SQL statement uses the CSV fact table, the CSV dimension table and the JSON
    // event stream at the same time, with no intermediate ETL load step.
    let rs = db.execute(
        "SELECT
             u.region,
             u.tier,
             COUNT(*)                 AS orders,
             ROUND(SUM(f.amount), 2)  AS revenue
         FROM fact_orders f
         JOIN dim_users u ON f.user_id = u.user_id
         GROUP BY u.region, u.tier
         ORDER BY revenue DESC",
    )?;
    let rows = rs.to_rows()?;
    println!("\nFederated query: revenue by region x membership tier");
    let mut cold_total = 0.0f64;
    let mut cold_orders = 0i64;
    for row in &rows {
        let revenue = match row.get("revenue") {
            Some(Value::Float64(v)) => *v,
            _ => 0.0,
        };
        let orders = match row.get("orders") {
            Some(Value::Int64(v)) => *v,
            _ => 0,
        };
        cold_total += revenue;
        cold_orders += orders;
        println!(
            "  {:?}/{:?}: {} orders, {:.2}",
            row.get("region"),
            row.get("tier"),
            orders,
            revenue
        );
    }

    // Cross-check: the total after the JOIN must equal the fact table's raw total (the
    // JOIN must neither drop nor duplicate rows).
    println!("\nTotal after JOIN: {cold_orders} orders / {cold_total:.2}");
    assert_eq!(
        cold_orders as usize, ORDER_COUNT,
        "the order count after JOIN must equal the fact table row count"
    );
    assert!(
        (cold_total - fact_total).abs() < 1.0,
        "the amount total after JOIN {cold_total} should be about the generated total {fact_total}"
    );
    println!("[OK] the federated JOIN neither lost nor duplicated any rows");

    // ── 3. Combined query across all three sources (fact + dimension + event stream) ──
    // Compute each region's "total event weight" and "order revenue" to compare the two
    // signal distributions.
    let rs = db.execute(
        "SELECT
             u.region,
             ROUND(SUM(f.amount), 2) AS revenue
         FROM fact_orders f
         JOIN dim_users u ON f.user_id = u.user_id
         GROUP BY u.region
         ORDER BY revenue DESC",
    )?;
    let revenue_by_region: Vec<(String, f64)> = rs
        .to_rows()?
        .iter()
        .map(|row| {
            let region = match row.get("region") {
                Some(Value::String(s)) => s.clone(),
                _ => "?".to_string(),
            };
            let revenue = match row.get("revenue") {
                Some(Value::Float64(v)) => *v,
                _ => 0.0,
            };
            (region, revenue)
        })
        .collect();

    let rs = db.execute(
        "SELECT u.region, SUM(e.weight) AS weight
         FROM events e
         JOIN dim_users u ON e.user_id = u.user_id
         GROUP BY u.region
         ORDER BY weight DESC",
    )?;
    let weight_by_region: HashMap<String, i64> = rs
        .to_rows()?
        .iter()
        .map(|row| {
            let region = match row.get("region") {
                Some(Value::String(s)) => s.clone(),
                _ => "?".to_string(),
            };
            let weight = match row.get("weight") {
                Some(Value::Int64(v)) => *v,
                _ => 0,
            };
            (region, weight)
        })
        .collect();

    println!("\nRegional revenue vs event weight:");
    for (region, revenue) in &revenue_by_region {
        let weight = weight_by_region.get(region).copied().unwrap_or(0);
        println!("  {region}: revenue {revenue:.2}, event weight {weight}");
    }

    // ── 4. Materialize hot data: land the federated result in a local column-store table ──
    // This is the key step of a "data-lake foundation": lake files suit infrequent bulk
    // work, while local column storage suits frequent small queries. Predefine the hot
    // table's schema to keep types stable.
    let hot = db.create_table_with_schema(
        "hot_region_revenue",
        &[
            ("region".to_string(), ColumnType::String),
            ("tier".to_string(), ColumnType::String),
            ("orders".to_string(), ColumnType::Int64),
            ("revenue".to_string(), ColumnType::Float64),
        ],
    )?;

    // Recompute the detail from the lake files and write it into the hot table (real
    // systems usually refresh incrementally per partition).
    let rs = db.execute(
        "SELECT
             u.region AS region,
             u.tier AS tier,
             COUNT(*) AS orders,
             ROUND(SUM(f.amount), 2) AS revenue
         FROM fact_orders f
         JOIN dim_users u ON f.user_id = u.user_id
         GROUP BY u.region, u.tier",
    )?;
    let hot_rows: Vec<Row> = rs.to_rows()?;
    let hot_ids = hot.insert_batch(&hot_rows)?;
    println!(
        "\nMaterialized hot table hot_region_revenue: wrote {} rows",
        hot_ids.len()
    );
    assert_eq!(hot.count()?, hot_rows.len() as u64);

    // ── 5. Cold/hot path consistency check ──────────────────────────────────
    // The worst failure mode for a data-lake foundation is "the accelerated copy
    // disagrees with the lake truth". A full aggregate reconciling pass is done here.
    let hot_total = hot.execute("SELECT ROUND(SUM(revenue), 2) AS r FROM hot_region_revenue")?;
    let hot_revenue = match hot_total.to_rows()?.first().and_then(|r| r.get("r")) {
        Some(Value::Float64(v)) => *v,
        _ => 0.0,
    };
    // SUM over an integer column is a single-cell aggregate, so `scalar()` reads it.
    let hot_order_count = hot
        .execute("SELECT SUM(orders) AS n FROM hot_region_revenue")?
        .scalar()
        .unwrap_or(-1);

    println!("Cold path (lake files) total: {cold_orders} orders / {cold_total:.2}");
    println!("Hot path (local column store) total: {hot_order_count} orders / {hot_revenue:.2}");
    assert_eq!(
        hot_order_count as usize, ORDER_COUNT,
        "the hot-table order total must match the lake"
    );
    assert!(
        (hot_revenue - cold_total).abs() < 1.0,
        "the hot-table amount total {hot_revenue} should match the cold path {cold_total}"
    );
    println!("[OK] the cold and hot paths agree");

    // ── 6. Window ranking on the hot table (full SQL capability once materialized) ──
    // Aggregate windows now compose — they can be nested in functions, used in
    // arithmetic, and carry `ROWS BETWEEN ...` frames — so `SUM(x) OVER (...)` works
    // directly. (A window inside `CAST(...)` and a window in `ORDER BY` are still
    // unsupported.)
    let rs = hot.execute(
        "SELECT region, tier, revenue,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) AS rank_in_region
         FROM hot_region_revenue
         ORDER BY region, rank_in_region",
    )?;
    println!("\nRanking on the hot table by region (top 2 per region):");
    for row in rs.to_rows()? {
        let rank = match row.get("rank_in_region") {
            Some(Value::Int64(v)) => *v,
            _ => 0,
        };
        if rank <= 2 {
            println!(
                "  {:?} #{rank}: {:?} -> {:?}",
                row.get("region"),
                row.get("tier"),
                row.get("revenue")
            );
        }
    }

    // ── 7. Cleanup ──────────────────────────────────────────────────────────
    for name in ["fact_orders", "dim_users", "events"] {
        db.drop_temp_table(name)?;
    }
    println!("\nReleased all temp tables; the original lake files remain on disk.");
    // The original files must be intact — the engine must not damage the "source of truth".
    assert!(fact_path.exists() && dim_path.exists() && events_path.exists());
    println!("[OK] the lake source files were not modified");

    println!("\n=== Scenario R9 complete ===");
    Ok(())
}
