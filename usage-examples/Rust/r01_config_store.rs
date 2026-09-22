//! Scenario R1: Embedded configuration center / feature-flag store
//!
//! # Business background
//!
//! A service process needs an authoritative local copy of "configuration entries +
//! feature flags": read/write latency must be low, the data must survive restarts,
//! and it must be isolated per environment (dev/staging/prod). None of that needs a
//! separate database server process, so embedded ApexBase fits well — it offers both
//! **point access (key-value style)** and **SQL (configuration audit/statistics)**.
//!
//! # ApexBase capabilities demonstrated
//!
//! 1. `ApexDB::builder()` plus durability-level (`DurabilityLevel`) selection.
//! 2. `create_table_with_schema()`: declare column types up front for faster,
//!    type-stable writes.
//! 3. Single-row writes with `Row` (a `HashMap<String, Value>`) and batch writes
//!    with `insert_batch`.
//! 4. The three point operations `retrieve` / `delete` / `replace` (the typical
//!    configuration-change actions).
//! 5. `execute()` to run SQL for auditing and aggregation, and
//!    `ResultSet::to_rows()` to read the result.
//! 6. `add_column` / `drop_column` for lossless schema evolution.
//! 7. Multiple databases (`use_database`) to isolate environments.
//!
//! # How to run
//!
//! ```bash
//! cargo run --example r01_config_store --no-default-features
//! ```
//!
//! The example uses a temporary directory that is cleaned up when it exits, so it
//! never pollutes the working tree.

use std::collections::HashMap;

use apexbase::data::{DataType, Value};
use apexbase::embedded::{ApexDB, Row};
use apexbase::storage::on_demand::ColumnType;
use apexbase::storage::DurabilityLevel;

/// Funnel `Row` construction into one helper so the example does not repeat the
/// `insert` boilerplate everywhere.
///
/// A `Row` in ApexBase is just `HashMap<String, Value>`, so it can also be built
/// directly from an iterator with `.collect()`.
fn config_row(key: &str, value: &str, env: &str, version: i64) -> Row {
    let mut row = HashMap::new();
    row.insert("cfg_key".to_string(), Value::String(key.to_string()));
    row.insert("cfg_value".to_string(), Value::String(value.to_string()));
    row.insert("env".to_string(), Value::String(env.to_string()));
    row.insert("version".to_string(), Value::Int64(version));
    row
}

fn main() -> apexbase::Result<()> {
    // ── 1. Open the database ────────────────────────────────────────────────
    // `tempdir()` deletes the directory when the function returns; swapping in a
    // real path (for example "./config_db") is the production usage.
    let tmp = tempfile::tempdir().expect("create temp dir");

    // DurabilityLevel selects the fsync strategy:
    //   Fast — never fsyncs, fastest; a process crash may lose the last batch
    //          (fine for data that can be rebuilt)
    //   Safe — fsyncs on flush()/commit, the default for most production write loads
    //   Max  — fsyncs on every write, safest and slowest
    // Configuration is "rarely changes but must be reliable", so use Safe.
    let db = ApexDB::builder(tmp.path())
        .durability(DurabilityLevel::Safe)
        .drop_if_exists(true)
        .build()?;

    println!("Database directory: {}", db.base_dir().display());

    // ── 2. Create the table with a predefined schema ────────────────────────
    // Declaring types up front avoids re-inferring them for every batch, which
    // keeps the write path stable.
    let configs = db.create_table_with_schema(
        "configs",
        &[
            ("cfg_key".to_string(), ColumnType::String),
            ("cfg_value".to_string(), ColumnType::String),
            ("env".to_string(), ColumnType::String),
            ("version".to_string(), ColumnType::Int64),
        ],
    )?;
    // Note: `name` is a public field (not a method) while `path()` is a method.
    println!("Created table: {} -> {}", configs.name, configs.path().display());

    // ── 3. Batch write ──────────────────────────────────────────────────────
    let seed = vec![
        config_row("api.timeout_ms", "1500", "prod", 3),
        config_row("api.retry", "2", "prod", 5),
        config_row("feature.new_ui", "true", "prod", 1),
        config_row("feature.new_ui", "false", "dev", 1),
        config_row("db.pool_size", "32", "prod", 7),
    ];
    let ids = configs.insert_batch(&seed)?;
    println!("Batch-wrote {} configs, _ids = {:?}", ids.len(), ids);

    // ── 4. Point read: fetch a single row by _id ────────────────────────────
    // `retrieve` returns `Option<Row>`; a miss is `None`, not an error.
    if let Some(row) = configs.retrieve(ids[0])? {
        println!(
            "Point read _id={}: cfg_key={:?}",
            ids[0],
            row.get("cfg_key")
        );
    }

    // ── 5. SQL aggregation: count configs and max version per environment ───
    // Plain `GROUP BY` aggregation is fully supported. Aggregate window functions
    // such as `SUM(...) OVER (...)` also compose in the current engine, but this
    // audit query is clearer as a simple `GROUP BY`.
    let rs = configs.execute(
        "SELECT env, COUNT(*) AS n, MAX(version) AS max_version
         FROM configs
         GROUP BY env
         ORDER BY env",
    )?;
    println!("\nPer-environment stats:");
    for row in rs.to_rows()? {
        println!(
            "  env={:?}  count={:?}  max_version={:?}",
            row.get("env"),
            row.get("n"),
            row.get("max_version")
        );
    }

    // ── 6. Filtered query + sort: fetch every feature flag ──────────────────
    let rs = configs.execute(
        "SELECT cfg_key, cfg_value, env
         FROM configs
         WHERE cfg_key LIKE 'feature%'
         ORDER BY env, cfg_key",
    )?;
    println!("\nFeature flags (LIKE 'feature%'):");
    for row in rs.to_rows()? {
        println!(
            "  {:?} = {:?}  [{}]",
            row.get("cfg_key"),
            row.get("cfg_value"),
            match row.get("env") {
                Some(Value::String(s)) => s.as_str(),
                _ => "?",
            }
        );
    }

    // ── 7. replace: full-row update (bump the config version) ───────────────
    // replace swaps in a brand-new Row for the given _id and reports whether the
    // id was found. The rewritten row is readable again straight away.
    let replaced = configs.replace(ids[0], config_row("api.timeout_ms", "2000", "prod", 4))?;
    println!("Updated api.timeout_ms -> 2000 (hit={replaced})");
    assert!(configs.retrieve(ids[0])?.is_some(), "a replaced row must stay readable");

    // ── 8. delete: retire a decommissioned config ───────────────────────────
    // `replace()` and `delete()` may be combined in any order: each removes or
    // rewrites exactly the targeted row.
    let before_delete = configs.count()?;
    let deleted = configs.delete(ids[1])?;
    println!(
        "Deleted _id={} (hit={deleted}), {} rows remain",
        ids[1],
        configs.count()?
    );
    assert!(deleted, "the seeded row must exist");
    assert_eq!(configs.count()?, before_delete - 1, "delete must remove exactly one row");
    assert!(configs.retrieve(ids[1])?.is_none(), "the deleted row must be gone");

    // ── 9. Schema evolution: add_column / drop_column ───────────────────────
    // New requirement: record who owns each config. add_column is safe for
    // existing data (older rows simply read back as NULL).
    configs.add_column("owner", DataType::String)?;
    println!("\nSchema after adding the owner column:");
    for (name, dtype) in configs.schema()? {
        println!("  {name}: {dtype:?}");
    }

    // Demonstrate drop_column (then immediately restore the earlier shape so the
    // following steps see a consistent schema).
    configs.drop_column("owner")?;
    let columns = configs.columns()?;
    println!("Columns remaining after drop: {columns:?}");

    // ── 10. Isolate environments with multiple databases ────────────────────
    // Each named database is its own subdirectory, so table files never interfere.
    db.use_database("tenant_acme")?;
    let tenant_table = db.create_table_with_schema(
        "configs",
        &[
            ("cfg_key".to_string(), ColumnType::String),
            ("cfg_value".to_string(), ColumnType::String),
            ("env".to_string(), ColumnType::String),
            ("version".to_string(), ColumnType::Int64),
        ],
    )?;
    tenant_table.insert(config_row("feature.new_ui", "true", "prod", 1))?;
    println!(
        "\nTenant database tenant_acme tables: {:?}",
        db.list_tables()
    );

    // Switch back to the default database and confirm the two sides are independent.
    db.use_database("")?;
    println!("Default-database tables: {:?}", db.list_tables());
    println!(
        "Default-database configs row count = {}",
        db.table("configs")?.count()?
    );

    // ── 11. Explicit flush to disk ──────────────────────────────────────────
    // Under Safe/Max, flush writes buffered data and fsyncs, so it is readable
    // again after a restart.
    let configs = db.table("configs")?;
    configs.flush()?;
    println!("Flush complete, configuration is persisted.");

    println!("\n=== Scenario R1 complete ===");
    Ok(())
}
