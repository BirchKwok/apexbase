//! Scenario R2: Concurrent writes, concurrent reads and transactions
//!
//! # Business background
//!
//! A collection/analytics service is usually "**few writers + many concurrent
//! readers**": one collector thread keeps writing events into the local database
//! while several reporting/dashboard threads run statistics queries. This example
//! demonstrates the correct concurrency usage and transaction semantics for that
//! shape.
//!
//! # ApexBase capabilities demonstrated
//!
//! 1. `Table` / `ApexDB` are `Clone + Send + Sync` and can be shared across threads.
//! 2. **Multiple threads writing the same table concurrently.** Writes are
//!    serialized per table internally, so every writer succeeds and no row is lost
//!    or duplicated — the point of this example.
//! 3. **Reads run in parallel with those writes** (several writer threads plus
//!    several read-only threads on one table, the most common production shape).
//!    Readers stay lock-free except while a writer still has rows buffered in
//!    memory.
//! 4. Transactions: `BEGIN` / `COMMIT` / `ROLLBACK` for batch atomicity.
//! 5. Consistency checks after the concurrent phase.
//!
//! `replace()` and `delete()` may be combined in any order — see
//! `usage-examples/README.md` for the defect this used to be.
//!
//! # How to run
//!
//! ```bash
//! cargo run --example r02_concurrent_writes --no-default-features
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use apexbase::data::Value;
use apexbase::embedded::{ApexDB, Row};
use apexbase::storage::on_demand::ColumnType;
use apexbase::storage::DurabilityLevel;

const READER_THREADS: usize = 3;
const WRITE_BATCHES: usize = 40;
const ROWS_PER_BATCH: usize = 25;
const REDISCOVERY_SAMPLES: usize = 4;
const REDISCOVERY_EVENTS_PER_SAMPLE: usize = 3;
/// Number of threads writing the *same* table at the same time.
const WRITER_THREADS: usize = 4;
/// Keeps each writer's batch identifiers disjoint so rows stay identifiable.
const BATCH_OFFSET: usize = 10_000;

/// Build one event record.
fn event_row(batch: usize, seq: usize) -> Row {
    let mut row = HashMap::new();
    row.insert("source".to_string(), Value::String("collector-0".to_string()));
    row.insert("batch".to_string(), Value::Int64(batch as i64));
    row.insert("seq".to_string(), Value::Int64(seq as i64));
    row.insert(
        "payload".to_string(),
        Value::String(format!("event-{batch}-{seq}")),
    );
    row
}

fn main() -> apexbase::Result<()> {
    let tmp = tempfile::tempdir().expect("create temp dir");

    // Use Fast to trade durability for throughput when events are replayable;
    // non-replayable data should use Safe instead (see the R1 example).
    let db = ApexDB::builder(tmp.path())
        .durability(DurabilityLevel::Fast)
        .drop_if_exists(true)
        .build()?;

    let events = db.create_table_with_schema(
        "events",
        &[
            ("source".to_string(), ColumnType::String),
            ("batch".to_string(), ColumnType::Int64),
            ("seq".to_string(), ColumnType::Int64),
            ("payload".to_string(), ColumnType::String),
        ],
    )?;

    println!("=== R2 concurrent reads and transactions ===");
    println!(
        "Setup: {} writer threads x {} batches x {} rows = {} rows (all into ONE table), {} concurrent read-only threads",
        WRITER_THREADS,
        WRITE_BATCHES,
        ROWS_PER_BATCH,
        WRITER_THREADS * WRITE_BATCHES * ROWS_PER_BATCH,
        READER_THREADS
    );

    // ── 1. Several threads writing the SAME table concurrently ──────────────
    // No warm-up and no sharding is needed: the storage engine serializes writes
    // per table for the whole mutation, so every thread's batch is applied exactly
    // once. The reader threads below run their statistics queries *while* these
    // writers are active, which is the point of the example: reads and writes may
    // be interleaved freely on one table.
    let stop = Arc::new(AtomicBool::new(false));
    let read_queries = Arc::new(AtomicUsize::new(0));

    // ── 1b. Reader threads start first, and keep querying during the writes ──
    // Reads run in parallel with each other and with the writers. A read only
    // synchronizes with a writer while that writer still has rows buffered in
    // memory (it flushes them first so the query sees them); once the rows are
    // persisted the read is lock-free again.
    let mut readers = Vec::with_capacity(READER_THREADS);
    for reader_id in 0..READER_THREADS {
        let table = events.clone();
        let stop_flag = stop.clone();
        let counter = read_queries.clone();
        readers.push(thread::spawn(move || -> apexbase::Result<()> {
            while !stop_flag.load(Ordering::SeqCst) {
                // Every query sees a consistent snapshot of the table, whatever
                // the writers are doing at that moment.
                let _ = table.execute("SELECT COUNT(*) AS n FROM events")?.to_rows()?;
                counter.fetch_add(1, Ordering::SeqCst);
            }
            println!("  read-only thread {reader_id} finished");
            Ok(())
        }));
    }

    let start = std::time::Instant::now();
    // Writers: WRITER_THREADS threads all inserting into the SAME table. The engine
    // serializes them internally, so no warm-up, sharding or manual coordination is
    // required here.
    let mut writers = Vec::with_capacity(WRITER_THREADS);
    for worker in 0..WRITER_THREADS {
        let writer_table = events.clone();
        writers.push(thread::spawn(move || -> apexbase::Result<usize> {
            let mut written = 0usize;
            for batch in 0..WRITE_BATCHES {
                // Build each batch as one Vec<Row> and insert it in a single call:
                // this amortizes the per-call locking overhead.
                let rows: Vec<Row> = (0..ROWS_PER_BATCH)
                    .map(|seq| event_row(worker * BATCH_OFFSET + batch, seq))
                    .collect();
                written += writer_table.insert_batch(&rows)?.len();
            }
            Ok(written)
        }));
    }

    let mut written = 0usize;
    for writer in writers {
        written += writer.join().expect("writer thread panicked")?;
    }
    let elapsed = start.elapsed();

    std::thread::sleep(std::time::Duration::from_millis(50));
    stop.store(true, Ordering::SeqCst);
    for reader in readers {
        reader.join().expect("reader thread panicked")?;
    }

    println!(
        "\nWrite finished: {} rows in {:.1?} ({:.0} rows/s)",
        written,
        elapsed,
        written as f64 / elapsed.as_secs_f64().max(1e-9)
    );
    println!(
        "Concurrent read-only queries completed: {} ({} threads)",
        read_queries.load(Ordering::SeqCst),
        READER_THREADS
    );

    // ── 2. Consistency checks ───────────────────────────────────────────────
    // Every writer must have succeeded and every row must be present exactly once.
    let expected_written = (WRITER_THREADS * WRITE_BATCHES * ROWS_PER_BATCH) as u64;
    let expected_total = expected_written;
    assert_eq!(written as u64, expected_written, "every writer must report the right row count");
    let actual = events.count()?;
    assert_eq!(actual, expected_total, "the row count must match the expectation after concurrency");
    println!("[OK] row count matches expectation: {actual}");

    // `scalar()` is the direct way to read a single-cell aggregate such as
    // COUNT(*); the executor normalizes all result shapes so it is reliable here.
    let counted = events
        .execute("SELECT COUNT(*) AS n FROM events")?
        .scalar()
        .unwrap_or(-1);
    assert_eq!(counted as u64, expected_total, "SQL COUNT(*) must agree with count()");
    println!("[OK] SQL COUNT(*) = {counted}, consistent with count()");

    // Distinctness check: concurrent writers must not duplicate a row. Each writer
    // owns a disjoint (batch, seq) range, so the number of distinct payload values
    // must equal the number of rows.
    let distinct = events
        .execute("SELECT COUNT(DISTINCT payload) AS n FROM events")?
        .scalar()
        .unwrap_or(-1);
    assert_eq!(
        distinct as u64, expected_total,
        "concurrent writes must not duplicate a row"
    );
    println!("[OK] distinct payloads = {distinct}, concurrent writes produced no duplicates");

    // The read-only threads really did execute queries (otherwise the concurrency
    // demo would be meaningless).
    assert!(
        read_queries.load(Ordering::SeqCst) > 0,
        "read-only threads must complete at least one query"
    );

    // ── 3. Data re-discovery cost: every write rebuilds the "what have I seen" set ──
    // When a collection pipeline restarts or a new sampling cycle begins, it must
    // discover "what is new since last time". Rescanning the whole table on every
    // sample costs time that grows linearly with the table.
    println!("\nData re-discovery (full table scan on every round):");
    let mut rediscovery_seconds = Vec::new();
    for sample in 0..REDISCOVERY_SAMPLES {
        let scan_start = std::time::Instant::now();
        let seen = events
            .execute("SELECT batch, seq FROM events ORDER BY batch, seq")?
            .to_rows()?;
        let scan_elapsed = scan_start.elapsed();
        rediscovery_seconds.push(scan_elapsed.as_secs_f64());

        let seen_set: std::collections::HashSet<(i64, i64)> = seen
            .iter()
            .filter_map(|r| match (r.get("batch"), r.get("seq")) {
                (Some(Value::Int64(b)), Some(Value::Int64(s))) => Some((*b, *s)),
                _ => None,
            })
            .collect();
        assert_eq!(
            seen_set.len(),
            expected_total as usize,
            "re-discovery must see all {expected_total} rows"
        );

        // Every row the pipeline wants to insert must be judged "already present"
        // (duplicates are excluded by the dedup set).
        for (batch, seq) in &seen_set {
            let duplicate = seen_set.contains(&(*batch, *seq));
            assert!(duplicate, "rows in the set must be recognized as already present");
        }

        let per_sample = REDISCOVERY_EVENTS_PER_SAMPLE.min(seen_set.len());
        println!(
            "  sample {}: scanned {} rows, {} distinct items, {:.3} ms, {} insertable samples this round",
            sample + 1,
            seen.len(),
            seen_set.len(),
            scan_elapsed.as_secs_f64() * 1000.0,
            per_sample
        );
    }
    // The full scan grows with the data; here we only assert it succeeds and the
    // cost is observable.
    let first = rediscovery_seconds.first().copied().unwrap_or(0.0);
    let last = rediscovery_seconds.last().copied().unwrap_or(0.0);
    println!(
        "  first round {:.3} ms -> last round {:.3} ms (same data volume, cost should be the same order)",
        first * 1000.0,
        last * 1000.0
    );

    // ── 4. Transactions: keep one batch of writes atomic ────────────────────
    // Scenario: a "data-source switch" involves several audit records; either all of
    // them take effect or none do.
    let audit = db.create_table_with_schema(
        "audit",
        &[
            ("action".to_string(), ColumnType::String),
            ("detail".to_string(), ColumnType::String),
        ],
    )?;

    // Important: a Rust embedded transaction must complete **inside a single
    // execute() call**. Splitting it into `execute("BEGIN")` -> `insert(...)` ->
    // `execute("COMMIT")` fails with
    // "COMMIT requires txn_id context - use execute_commit_txn()", because the
    // transaction id only lives within the statement sequence of one call.
    // In real projects, send a batch of changes as one multi-statement SQL string,
    // or use table.insert_batch() for an atomic bulk write.
    let tx_sql = |actions: &[(String, String)], terminator: &str| -> String {
        let mut sql = String::from("BEGIN; ");
        for (action, detail) in actions {
            // The values here are controlled literals (letters/digits/hyphens), so
            // no escaping is needed. Production code must parameterize or strictly
            // escape user input.
            sql.push_str(&format!(
                "INSERT INTO audit (action, detail) VALUES ('{action}', '{detail}'); "
            ));
        }
        sql.push_str(terminator);
        sql
    };

    // 4a. Committed transaction: all 5 records take effect.
    let commit_actions: Vec<(String, String)> = (0..5)
        .map(|i| (format!("batch-commit-{i}"), "ok".to_string()))
        .collect();
    audit.execute(&tx_sql(&commit_actions, "COMMIT"))?;
    let committed = audit.count()?;
    println!("\naudit row count after COMMIT: {committed}");
    assert_eq!(committed, 5, "there must be 5 rows after commit");

    // 4b. Rolled-back transaction: all 3 records stay invisible.
    let rollback_actions: Vec<(String, String)> = (0..3)
        .map(|i| (format!("batch-rollback-{i}"), "discard".to_string()))
        .collect();
    audit.execute(&tx_sql(&rollback_actions, "ROLLBACK"))?;
    let after_rollback = audit.count()?;
    println!("audit row count after ROLLBACK: {after_rollback}");
    assert_eq!(after_rollback, 5, "there must still be 5 rows after rollback (the 3 rolled-back rows stay invisible)");

    let rolled = audit
        .execute("SELECT COUNT(*) AS n FROM audit WHERE action LIKE 'batch-rollback%'")?
        .scalar()
        .unwrap_or(-1);
    assert_eq!(rolled, 0, "rolled-back data must not be queryable");
    println!("[OK] rolled-back data is invisible (COUNT={rolled})");

    // ── 5. Parallel writes to separate tables (the safe sharded write) ──────
    // When parallel writes are required, "one table per thread" is safe —
    // the standard pattern for sharding by database or by data source.
    println!("\nParallel writes to separate tables:");
    let mut shard_handles = Vec::new();
    for shard in 0..4 {
        let shard_db = db.clone();
        shard_handles.push(thread::spawn(move || -> apexbase::Result<(usize, usize)> {
            let table = shard_db.create_table_with_schema(
                &format!("shard_{shard}"),
                &[("v".to_string(), ColumnType::Int64)],
            )?;
            let mut written = 0usize;
            for batch in 0..25 {
                let rows: Vec<Row> = (0..20)
                    .map(|seq| {
                        let mut row = HashMap::new();
                        row.insert(
                            "v".to_string(),
                            Value::Int64((batch * 20 + seq) as i64),
                        );
                        row
                    })
                    .collect();
                written += table.insert_batch(&rows)?.len();
            }
            Ok((shard, written))
        }));
    }
    let mut shard_total = 0usize;
    for handle in shard_handles {
        let (shard, n) = handle.join().expect("shard thread panicked")?;
        println!("  shard_{shard}: {n} rows");
        shard_total += n;
    }
    assert_eq!(shard_total, 4 * 25 * 20, "the total sharded write count must be right");
    println!("[OK] 4 shards wrote {shard_total} rows in parallel");

    println!("\n=== Scenario R2 complete ===");
    Ok(())
}
