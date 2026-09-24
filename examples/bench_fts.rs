//! FTS Index Build Timing Benchmark
//!
//! Mirrors the Python `bench_fts_build` benchmark: generates 1 million rows
//! (name, age, score, city, category), writes them to storage, then times the
//! `CREATE FTS INDEX ON bench (name, city, category)` operation end-to-end
//! and phase-by-phase.
//!
//! Run with:
//!   cargo run --example bench_fts --no-default-features --release

use std::collections::HashMap;
use std::time::Instant;

use apexbase::fts::{FtsConfig, FtsEngine};
use apexbase::storage::backend::TableStorageBackend;
use apexbase::storage::engine::StorageEngine;
use apexbase::storage::DurabilityLevel;
use apexbase::ApexExecutor;
use arrow::array::{Array, Int64Array, StringArray, UInt64Array};

const N: usize = 1_000_000;

fn main() {
    let cities = [
        "Beijing",
        "Shanghai",
        "Guangzhou",
        "Shenzhen",
        "Hangzhou",
        "Nanjing",
        "Chengdu",
        "Wuhan",
        "Xian",
        "Qingdao",
    ];
    let categories = [
        "Electronics",
        "Clothing",
        "Food",
        "Sports",
        "Books",
        "Home",
        "Auto",
        "Health",
        "Travel",
        "Gaming",
    ];

    // Simple LCG — same sequence shape as Python's random.Random(42)
    let mut s: u64 = 6364136223846793005u64.wrapping_mul(42).wrapping_add(1);
    let mut lcg = move || -> u64 {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        s >> 33
    };

    // ── Phase 0: generate data ──────────────────────────────────────────────
    println!("\n[FTS_BENCH] Generating {} rows ...", N);
    let t = Instant::now();
    let mut names: Vec<String> = Vec::with_capacity(N);
    let mut ages: Vec<i64> = Vec::with_capacity(N);
    let mut scores: Vec<f64> = Vec::with_capacity(N);
    let mut city_vals: Vec<String> = Vec::with_capacity(N);
    let mut cat_vals: Vec<String> = Vec::with_capacity(N);
    for i in 0..N {
        names.push(format!("user_{}", i));
        ages.push((18 + lcg() % 63) as i64);
        scores.push((lcg() % 10001) as f64 / 100.0);
        city_vals.push(cities[(lcg() % 10) as usize].to_string());
        cat_vals.push(categories[(lcg() % 10) as usize].to_string());
    }
    println!("[FTS_BENCH] data gen:              {:>10.2?}", t.elapsed());

    // ── Phase 1: write 1M rows to storage ──────────────────────────────────
    let dir = tempdir();
    let path = dir.join("bench.apex");

    let engine = StorageEngine::global();
    let t = Instant::now();
    engine
        .write_typed(
            &path,
            HashMap::from([("age".to_string(), ages)]),
            HashMap::from([("score".to_string(), scores)]),
            HashMap::from([
                ("name".to_string(), names),
                ("city".to_string(), city_vals),
                ("category".to_string(), cat_vals),
            ]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            DurabilityLevel::Fast,
        )
        .expect("write_typed failed");
    let t_write = t.elapsed();
    println!("[FTS_BENCH] write {} rows:    {:>10.2?}", N, t_write);

    // ── Phase 2: read_columns_to_arrow ─────────────────────────────────────
    let storage = TableStorageBackend::open(&path).expect("open failed");
    let t = Instant::now();
    let batch = storage
        .read_columns_to_arrow(Some(&["_id", "name", "city", "category"]), 0, None)
        .expect("read failed");
    let t_read = t.elapsed();
    println!(
        "[FTS_BENCH] read_columns_to_arrow ({} rows): {:>10.2?}",
        batch.num_rows(),
        t_read
    );

    // ── Phase 3: build columnar Vec<String> (current backfill path) ─────────
    let t = Instant::now();
    let ids: Vec<u64> = {
        let col = batch.column_by_name("_id").unwrap();
        if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
            (0..a.len()).map(|i| a.value(i)).collect()
        } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            (0..a.len()).map(|i| a.value(i) as u64).collect()
        } else {
            panic!("unexpected _id array type")
        }
    };
    let mut owned_cols: Vec<(String, Vec<String>)> = Vec::new();
    for col_name in &["name", "city", "category"] {
        if let Some(col) = batch.column_by_name(col_name) {
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                let vals: Vec<String> = (0..arr.len())
                    .map(|i| {
                        if arr.is_null(i) {
                            String::new()
                        } else {
                            arr.value(i).to_string()
                        }
                    })
                    .collect();
                owned_cols.push((col_name.to_string(), vals));
            }
        }
    }
    let t_build_owned = t.elapsed();
    println!(
        "[FTS_BENCH] build Vec<String>:     {:>10.2?}",
        t_build_owned
    );

    // ── Phase 4a: add_documents_columnar — current code path ────────────────
    let fts_dir = dir.join("fts_indexes");
    std::fs::create_dir_all(&fts_dir).unwrap();

    let engine_a = FtsEngine::new(fts_dir.join("bench_a.afts"), FtsConfig::default())
        .expect("FtsEngine::new failed");
    let t = Instant::now();
    engine_a
        .add_documents_columnar(ids.clone(), owned_cols)
        .expect("add_documents_columnar failed");
    let t_index_owned = t.elapsed();
    println!(
        "[FTS_BENCH] add_documents_columnar (Vec<String>): {:>10.2?}",
        t_index_owned
    );

    let t = Instant::now();
    engine_a.flush().expect("flush failed");
    let t_flush_owned = t.elapsed();
    println!(
        "[FTS_BENCH] flush (Vec<String>):       {:>10.2?}",
        t_flush_owned
    );

    // ── Phase 4a-async: flush_async + wait_flush ────────────────────────────
    let engine_a2 = FtsEngine::new(fts_dir.join("bench_a2.afts"), FtsConfig::default())
        .expect("FtsEngine::new failed");
    let owned_cols2: Vec<(String, Vec<String>)> = ["name", "city", "category"]
        .iter()
        .map(|col_name| {
            let col = batch.column_by_name(col_name).unwrap();
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            let vals: Vec<String> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        String::new()
                    } else {
                        arr.value(i).to_string()
                    }
                })
                .collect();
            (col_name.to_string(), vals)
        })
        .collect();
    engine_a2
        .add_documents_columnar(ids.clone(), owned_cols2)
        .expect("add_documents_columnar failed");
    let t = Instant::now();
    engine_a2.flush_async().expect("flush_async failed");
    let t_flush_async = t.elapsed();
    println!(
        "[FTS_BENCH] flush_async (returns): {:>10.2?}",
        t_flush_async
    );
    let t = Instant::now();
    engine_a2.wait_flush().expect("wait_flush failed");
    let t_wait_flush = t.elapsed();
    println!("[FTS_BENCH] wait_flush (disk IO):  {:>10.2?}", t_wait_flush);
    println!(
        "[FTS_BENCH] flush_async total:     {:>10.2?}",
        t_flush_async + t_wait_flush
    );

    // ── Phase 4b: add_documents_arrow_str — zero-copy &str path ───────────
    let t = Instant::now();
    let mut arrow_cols: Vec<(String, Vec<&str>)> = Vec::new();
    for col_name in &["name", "city", "category"] {
        if let Some(col) = batch.column_by_name(col_name) {
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                let vals: Vec<&str> = (0..arr.len())
                    .map(|i| if arr.is_null(i) { "" } else { arr.value(i) })
                    .collect();
                arrow_cols.push((col_name.to_string(), vals));
            }
        }
    }
    let t_build_str = t.elapsed();
    println!("[FTS_BENCH] build Vec<&str>:       {:>10.2?}", t_build_str);

    let engine_b = FtsEngine::new(fts_dir.join("bench_b.afts"), FtsConfig::default())
        .expect("FtsEngine::new failed");
    let t = Instant::now();
    engine_b
        .add_documents_arrow_str(&ids, arrow_cols)
        .expect("add_documents_arrow_str failed");
    let t_index_str = t.elapsed();
    println!(
        "[FTS_BENCH] add_documents_arrow_str (&str):       {:>10.2?}",
        t_index_str
    );

    let t = Instant::now();
    engine_b.flush().expect("flush failed");
    let t_flush_str = t.elapsed();
    println!("[FTS_BENCH] flush (&str):          {:>10.2?}", t_flush_str);

    // ── Phase 5: end-to-end via CREATE FTS INDEX ────────────────────────────
    let _ = std::fs::remove_dir_all(&fts_dir);
    drop(storage);
    let t = Instant::now();
    ApexExecutor::execute_with_base_dir(
        "CREATE FTS INDEX ON bench (name, city, category)",
        dir.as_path(),
        &path,
    )
    .expect("CREATE FTS INDEX failed");
    let t_executor = t.elapsed();
    println!(
        "[FTS_BENCH] CREATE FTS INDEX (ApexExecutor):      {:>10.2?}",
        t_executor
    );

    // ── Summary ─────────────────────────────────────────────────────────────
    println!("\n[FTS_BENCH] ─── Summary ({} rows) ───", N);
    println!("  write 1M rows              : {:.2?}", t_write);
    println!(
        "  read_columns_to_arrow      : {:.2?}  (IO + decode)",
        t_read
    );
    println!(
        "  build Vec<String>          : {:.2?}  (string copies)",
        t_build_owned
    );
    println!(
        "  add_documents_columnar     : {:.2?}  [current path]",
        t_index_owned
    );
    println!("  flush (sync)               : {:.2?}", t_flush_owned);
    println!(
        "  flush_async (caller side)  : {:.2?}  <-- caller unblocked here",
        t_flush_async
    );
    println!(
        "  wait_flush (disk IO)       : {:.2?}  (background thread)",
        t_wait_flush
    );
    println!("  ---");
    println!(
        "  build Vec<&str>            : {:.2?}  (zero-copy)",
        t_build_str
    );
    println!(
        "  add_documents_arrow_str    : {:.2?}  [optimized path]",
        t_index_str
    );
    println!("  flush (optimized path)     : {:.2?}", t_flush_str);
    println!("  ---");
    println!(
        "  CREATE FTS INDEX total     : {:.2?}  (uses flush_async)",
        t_executor
    );
    println!(
        "\n  flush_async caller speedup vs sync flush: {:.0}x",
        t_flush_owned.as_secs_f64() / t_flush_async.as_secs_f64().max(1e-9)
    );
    println!(
        "  index only speedup (&str vs Vec<String>): {:.1}x",
        t_index_owned.as_secs_f64() / t_index_str.as_secs_f64().max(1e-9)
    );
}

/// Create a temporary directory and return its path.
/// The directory is NOT cleaned up (intentional for a benchmark binary).
fn tempdir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("apexbase_bench_fts_{}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}
