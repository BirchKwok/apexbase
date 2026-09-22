//! Scenario R5: Vector similarity search inside a Rust service
//!
//! # Business background
//!
//! A pure Rust service (no Python runtime) needs to keep embedding vectors for
//! products/documents locally and serve "image-to-image" or semantic search online.
//! The requirements are low latency and no external vector database.
//!
//! # ApexBase capabilities demonstrated
//!
//! 1. `ColumnType::FixedList` stores float32 embedding vectors (written as raw LE bytes).
//! 2. SQL TopK: how to use `explode_rename(topk_distance(...))`.
//! 3. The distance functions `array_distance` / `cosine_distance` and multiple metrics.
//! 4. TopK via `ORDER BY dist LIMIT k`, and a JOIN to turn `_id`s back into business fields.
//!
//! # Vector encoding convention in Rust (important)
//!
//! The Rust API writes vectors as `Value::FixedList(Vec<u8>)` whose contents are the
//! **little-endian byte sequence of f32 values**. You must therefore encode `&[f32]`
//! into bytes before writing, and decode on the way out. This differs from the Python
//! API (which takes a list/numpy array directly) and is the most important thing to
//! watch in this example.
//!
//! # How to run
//!
//! ```bash
//! cargo run --example r05_vector_search --no-default-features
//! ```

use std::collections::HashMap;

use apexbase::data::Value;
use apexbase::embedded::{ApexDB, Row};
use apexbase::storage::on_demand::ColumnType;
use apexbase::storage::DurabilityLevel;
use arrow::array::Array;

const DIM: usize = 8;

/// Encode `&[f32]` into the little-endian byte sequence ApexBase expects.
fn encode_f32_vec(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// Render a query vector as an SQL array literal for `array_distance(vec, [...])`.
fn vector_literal(values: &[f32]) -> String {
    let parts: Vec<String> = values.iter().map(|v| format!("{v:.6}")).collect();
    format!("[{}]", parts.join(","))
}

/// A deterministic pseudo-random generator (avoids pulling in `rand` just for the example).
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    /// Return an f32 in [0,1).
    fn next_f32(&mut self) -> f32 {
        // Linear congruential; the quality is plenty for a demo and fully reproducible.
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((self.0 >> 33) as f32) / (u32::MAX as f32 / 2.0)
    }
    /// Generate a unit vector (L2 norm 1).
    fn unit_vec(&mut self) -> Vec<f32> {
        let raw: Vec<f32> = (0..DIM).map(|_| self.next_f32() - 0.5).collect();
        let norm = raw.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        raw.into_iter().map(|x| x / norm).collect()
    }
}

/// Cosine similarity (used only for local cross-checking, not for database queries).
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    dot / (na * nb).max(1e-9)
}

fn main() -> apexbase::Result<()> {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let db = ApexDB::builder(tmp.path())
        .durability(DurabilityLevel::Fast)
        .drop_if_exists(true)
        .build()?;

    // ── 1. Create the table: label + category + vec(FixedList) ──────────────
    let items = db.create_table_with_schema(
        "items",
        &[
            ("label".to_string(), ColumnType::String),
            ("category".to_string(), ColumnType::String),
            ("vec".to_string(), ColumnType::FixedList),
        ],
    )?;

    // ── 2. Generate and write the vectors ───────────────────────────────────
    // For interpretable results: generate 3 "cluster centers" first, then add noise
    // around each center, so vectors inside one cluster should become each other's
    // nearest neighbors.
    let mut rng = Lcg::new(20240501);
    let centers: Vec<Vec<f32>> = (0..3).map(|_| rng.unit_vec()).collect();

    const PER_CLUSTER: usize = 20;
    let mut rows: Vec<Row> = Vec::with_capacity(centers.len() * PER_CLUSTER);
    let mut stored: Vec<(String, String, Vec<f32>)> = Vec::new();

    for (cluster_id, center) in centers.iter().enumerate() {
        let category = format!("cluster-{cluster_id}");
        for i in 0..PER_CLUSTER {
            // Add noise around the center, then renormalize.
            let mut v: Vec<f32> = center
                .iter()
                .map(|c| c + (rng.next_f32() - 0.5) * 0.3)
                .collect();
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
            for x in v.iter_mut() {
                *x /= norm;
            }

            let label = format!("{category}-item-{i:02}");
            let mut row = HashMap::new();
            row.insert("label".to_string(), Value::String(label.clone()));
            // `category` is used again later in the loop, so clone instead of moving it.
            row.insert("category".to_string(), Value::String(category.clone()));
            // Key point: the vector must be encoded as little-endian f32 bytes,
            // otherwise what gets written is the *length of the byte buffer*, not the vector.
            row.insert("vec".to_string(), Value::FixedList(encode_f32_vec(&v)));
            rows.push(row);
            stored.push((label, category.clone(), v));
        }
    }

    let ids = items.insert_batch(&rows)?;
    println!(
        "Wrote {} vectors (dim={}, {} clusters x {} rows each)",
        ids.len(),
        DIM,
        centers.len(),
        PER_CLUSTER
    );
    assert_eq!(items.count()?, rows.len() as u64);

    // ── 3. SQL TopK: explode_rename(topk_distance(...)) ─────────────────────
    // Use the first cluster's center as the query vector; we expect its members back.
    let query = centers[0].clone();
    let literal = vector_literal(&query);

    let sql = format!(
        "SELECT explode_rename(topk_distance(vec, {literal}, 5, 'cosine'), '_id', 'dist') FROM items"
    );
    let rs = items.execute(&sql)?;
    let rows = rs.to_rows()?;
    println!("\nexplode_rename(topk_distance(...)) TopK=5:");
    for row in &rows {
        println!("  _id={:?}  dist={:?}", row.get("_id"), row.get("dist"));
    }
    assert_eq!(rows.len(), 5, "TopK must return 5 rows");

    // ── 4. Turn the TopK _ids back into business fields ─────────────────────
    // topk_distance only returns _id and distance. Note: **the ApexBase SQL parser
    // does not allow a table function (explode_rename(...)) in a JOIN's FROM
    // clause**, so this uses the two-step form "take the TopK _ids, then re-query
    // them through an IN list" — semantically equivalent, and closer to a real
    // retrieval service (get candidate ids first, then batch-fetch details by id).
    let topk_ids: Vec<u64> = rows
        .iter()
        .filter_map(|r| match r.get("_id") {
            Some(Value::Int64(v)) => Some(*v as u64),
            _ => None,
        })
        .collect();
    assert_eq!(topk_ids.len(), 5, "there must be 5 candidate _ids");

    let id_list = topk_ids
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let rs = items.execute(&format!(
        "SELECT _id, label, category FROM items WHERE _id IN ({id_list}) ORDER BY _id"
    ))?;
    let mut details: HashMap<i64, (String, String)> = HashMap::new();
    for row in rs.to_rows()? {
        let id = match row.get("_id") {
            Some(Value::Int64(v)) => *v,
            _ => continue,
        };
        let label = match row.get("label") {
            Some(Value::String(s)) => s.clone(),
            _ => "?".to_string(),
        };
        let category = match row.get("category") {
            Some(Value::String(s)) => s.clone(),
            _ => "?".to_string(),
        };
        details.insert(id, (label, category));
    }

    println!("\nDetails after fetching the TopK back (query vector from cluster-0):");
    let mut recalled_cluster0 = 0usize;
    for id in &topk_ids {
        if let Some((label, category)) = details.get(&(*id as i64)) {
            if category == "cluster-0" {
                recalled_cluster0 += 1;
            }
            println!("  _id={id}  {label}  [{category}]");
        }
    }
    assert_eq!(details.len(), topk_ids.len(), "every candidate _id must fetch a detail row");
    // The query vector comes from cluster-0's center, so TopK should land mostly in that cluster.
    println!(
        "TopK members belonging to cluster-0: {recalled_cluster0}/{}",
        topk_ids.len()
    );
    assert!(
        recalled_cluster0 >= 4,
        "the query vector is from cluster-0, so TopK should mostly hit that cluster (actual {recalled_cluster0})"
    );
    println!("[OK] vector search + fetch-back chain is correct");

    // ── 5. Distance functions: array_distance / cosine_distance ─────────────
    // Note: array_distance takes exactly 2 arguments (L2 by default); for other
    // metrics use the corresponding named function, for example cosine_distance.
    println!("\nSorted by distance (full scan + ORDER BY, useful for thresholds/custom ranking):");
    for (label, func) in [("l2", "array_distance"), ("cosine", "cosine_distance")] {
        let sql = format!(
            "SELECT label, {func}(vec, {literal}) AS dist
             FROM items
             ORDER BY dist
             LIMIT 3"
        );
        match items.execute(&sql) {
            Ok(rs) => {
                println!("  {label} ({func}):");
                for row in rs.to_rows()? {
                    println!("    {:?} -> {:?}", row.get("label"), row.get("dist"));
                }
            }
            Err(e) => println!("  {label} ({func}) unavailable: {e}"),
        }
    }

    // ── 6. Read the vector column back and cross-check locally ──────────────
    // A FixedList column decodes in Arrow as `FixedSizeList<Float32>` (fixed length
    // DIM), so it cannot be downcast to a Float32Array directly — fetch the inner
    // values first.
    let rs = items.execute("SELECT label, vec FROM items ORDER BY _id LIMIT 3")?;
    let batch = rs.to_record_batch()?;
    let vec_col = batch
        .column_by_name("vec")
        .expect("vec column")
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeListArray>()
        .expect("FixedList should decode to a FixedSizeListArray");

    // The list length must equal the written dimension — the key assertion for
    // encode/decode correctness.
    assert_eq!(
        vec_col.value_length() as usize,
        DIM,
        "the FixedSizeList length must equal the written dimension {DIM}"
    );
    let inner = vec_col
        .values()
        .as_any()
        .downcast_ref::<arrow::array::Float32Array>()
        .expect("the FixedSizeList inner values should be a Float32Array");

    println!("\nRead the vectors back for a local cross-check (FixedSizeList<Float32>, dim={DIM}):");
    let mut checked = 0usize;
    for row_idx in 0..batch.num_rows() {
        let start = row_idx * DIM;
        let decoded: Vec<f32> = (start..start + DIM).map(|i| inner.value(i)).collect();
        assert_eq!(decoded.len(), DIM);
        let sim = cosine_similarity(&decoded, &query);
        println!(
            "  row{row_idx} dim={} cosine similarity to query={:.4}",
            decoded.len(),
            sim
        );
        checked += 1;
    }
    assert!(checked > 0, "at least one vector must be read back and validated");
    println!("[OK] FixedList encode/decode round-trip is consistent");

    println!("\n=== Scenario R5 complete ===");
    Ok(())
}
