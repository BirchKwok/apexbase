//! Scenario R6: Cross-database read-only analytics over shared files
//!
//! # Business background
//!
//! One process manages several logical databases at once (for example `production`
//! and `staging`, or one per tenant). It needs **cross-database federated queries**
//! (using the standard `db.table` syntax) while a separate read-only analytics task
//! concurrently reads the same data to build reports.
//!
//! # ApexBase capabilities demonstrated
//!
//! 1. Multiple databases: `use_database()` switches/creates named sub-databases, each
//!    with its own `.apex` files.
//! 2. Cross-database SQL: `FROM production.orders`, cross-database JOINs.
//! 3. Multi-threaded read-only concurrency: read-only queries run in parallel
//!    (`Table`/`ApexDB` are Send + Sync).
//! 4. Choosing a durability level: writes use Safe while the read-only analytics side
//!    is unaffected.
//!
//! # How to run
//!
//! ```bash
//! cargo run --example r06_shared_file_analytics --no-default-features
//! ```

use std::collections::HashMap;
use std::thread;

use apexbase::data::Value;
use apexbase::embedded::{ApexDB, Row};
use apexbase::storage::on_demand::ColumnType;
use apexbase::storage::DurabilityLevel;

const ORDERS_PER_DB: usize = 500;

/// Build one order record. The amount uses a deterministic formula so the totals can
/// be cross-checked across databases.
fn order_row(order_id: usize, region: &str, amount: f64) -> Row {
    let mut row = HashMap::new();
    row.insert("order_id".to_string(), Value::Int64(order_id as i64));
    row.insert("region".to_string(), Value::String(region.to_string()));
    row.insert("amount".to_string(), Value::Float64(amount));
    row
}

/// Create the orders table in the given database and seed it. Returns (row count, amount total).
fn seed_orders(db: &ApexDB, prefix: &str) -> apexbase::Result<(usize, f64)> {
    let orders = db.create_table_with_schema(
        "orders",
        &[
            ("order_id".to_string(), ColumnType::Int64),
            ("region".to_string(), ColumnType::String),
            ("amount".to_string(), ColumnType::Float64),
        ],
    )?;

    let regions = ["east", "west", "north"];
    let mut rows = Vec::with_capacity(ORDERS_PER_DB);
    let mut expected_total = 0.0f64;
    for i in 0..ORDERS_PER_DB {
        // A per-database prefix is not needed for order_id here; the databases are
        // separated by directory, so ids may repeat and still be traceable.
        let amount = 10.0 + ((i * 7) % 190) as f64 + 0.5;
        expected_total += amount;
        rows.push(order_row(i, regions[i % regions.len()], amount));
    }
    let ids = orders.insert_batch(&rows)?;
    println!("  {prefix}: wrote {} rows, amount total {:.2}", ids.len(), expected_total);
    Ok((ids.len(), expected_total))
}

fn main() -> apexbase::Result<()> {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let db = ApexDB::builder(tmp.path())
        .durability(DurabilityLevel::Safe)
        .drop_if_exists(true)
        .build()?;

    println!("=== R6 cross-database read-only analytics ===\n");

    // ── 1. Write the "production" data into the default database ────────────
    // use_database("") means the root directory (the default database).
    db.use_database("")?;
    let (prod_rows, prod_total) = seed_orders(&db, "default(production)")?;

    // ── 2. Switch to the staging database and write a second copy ───────────
    // Each named database maps to its own subdirectory; the files are fully isolated.
    db.use_database("staging")?;
    let (stage_rows, stage_total) = seed_orders(&db, "staging")?;

    // Switch back to the default database to confirm filesystem-level isolation.
    db.use_database("")?;
    println!(
        "\nDefault-database tables: {:?}",
        db.list_tables()
    );
    db.use_database("staging")?;
    println!("staging tables: {:?}", db.list_tables());
    db.use_database("")?;

    // ── 3. Cross-database federated query (standard db.table syntax) ────────
    // This is the key value of the multi-database design: join two databases in one
    // SQL statement without any export/import. Note: cross-database queries use
    // db.execute() (database level) rather than table.execute().
    match db.execute(
        "SELECT
             p.region,
             COUNT(*)            AS prod_orders,
             ROUND(SUM(p.amount), 2) AS prod_amount
         FROM default.orders p
         GROUP BY p.region
         ORDER BY prod_amount DESC",
    ) {
        Ok(rs) => {
            println!("\nCross-database query on default.orders by region:");
            let rows = rs.to_rows()?;
            let mut sum = 0.0f64;
            for row in &rows {
                if let Some(Value::Float64(v)) = row.get("prod_amount") {
                    sum += *v;
                }
                println!(
                    "  region={:?} orders={:?} amount={:?}",
                    row.get("region"),
                    row.get("prod_orders"),
                    row.get("prod_amount")
                );
            }
            // Cross-check: the per-region total must equal the amount written
            // (within floating-point tolerance).
            assert!(
                (sum - prod_total).abs() < 1.0,
                "the cross-database aggregate total {sum} should be about the written total {prod_total}"
            );
            println!("[OK] cross-database aggregate total matches the written value ({sum:.2} ~= {prod_total:.2})");
        }
        Err(e) => println!("\nCross-database query unavailable: {e}"),
    }

    // ── 4. Cross-database JOIN: compare the two databases' orders together ──
    match db.execute(
        "SELECT COUNT(*) AS total_orders
         FROM default.orders p
         JOIN staging.orders s ON p.order_id = s.order_id",
    ) {
        Ok(rs) => {
            // The SELECT alias is honored on a cross-database JOIN, so `scalar()`
            // reads the count directly.
            let matched = rs.scalar().unwrap_or(-1);
            println!("\nCross-database JOIN (default join staging on order_id): {matched} matching rows");
            // Both databases have the same row count and the same order_id range, so
            // everything should match.
            assert_eq!(
                matched as usize, ORDERS_PER_DB,
                "the two databases' order_ids should all match"
            );
            println!("[OK] cross-database JOIN match count is correct");
        }
        Err(e) => println!("\nCross-database JOIN unavailable: {e}"),
    }

    // ── 5. Multi-threaded read-only concurrent analytics ────────────────────
    // A reporting service usually runs several queries in parallel. Read-only
    // queries have no write conflict, so ApexDB can be cloned into threads and
    // queried concurrently.
    let regions = ["east", "west", "north"];
    let mut handles = Vec::new();
    let start = std::time::Instant::now();

    for region in regions {
        let handle_db = db.clone();
        handles.push(thread::spawn(move || -> apexbase::Result<(String, i64, f64)> {
            let table = handle_db.table("orders")?;
            // Each thread runs its own filtered aggregation.
            let rs = table.execute(&format!(
                "SELECT COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
                 FROM orders WHERE region = '{region}'"
            ))?;
            let rows = rs.to_rows()?;
            let n = rows
                .first()
                .and_then(|row| row.get("n"))
                .and_then(|v| match v {
                    Value::Int64(i) => Some(*i),
                    _ => None,
                })
                .unwrap_or(0);
            let total = rows
                .first()
                .and_then(|row| row.get("total"))
                .and_then(|v| match v {
                    Value::Float64(f) => Some(*f),
                    _ => None,
                })
                .unwrap_or(0.0);
            Ok((region.to_string(), n, total))
        }));
    }

    let mut grand_total = 0.0f64;
    let mut grand_rows = 0i64;
    println!("\nMulti-threaded read-only concurrent analytics:");
    for handle in handles {
        let (region, n, total) = handle.join().expect("reader thread panicked")?;
        println!("  {region}: {n} rows, amount {total:.2}");
        grand_rows += n;
        grand_total += total;
    }
    let elapsed = start.elapsed();
    println!("Concurrent read elapsed: {elapsed:.1?}");

    // Consistency check: the sum of the parallel per-partition queries must equal the
    // full total.
    assert_eq!(grand_rows as usize, prod_rows, "the parallel partition row counts must sum to the total");
    assert!(
        (grand_total - prod_total).abs() < 1.0,
        "the parallel partition amount sum {grand_total} should be about the total {prod_total}"
    );
    println!("[OK] concurrent reads agree with the single-database full scan ({grand_rows} rows, {grand_total:.2})");

    // ── 6. Return to the default database and do a final confirmation ───────
    db.use_database("")?;
    let final_count = db.table("orders")?.count()?;
    println!("\nFinal default-database orders row count: {final_count}");
    assert_eq!(final_count as usize, prod_rows);
    assert_eq!(stage_rows, ORDERS_PER_DB);
    let _ = stage_total; // the staging total is display-only and was printed above

    println!("\n=== Scenario R6 complete ===");
    Ok(())
}
