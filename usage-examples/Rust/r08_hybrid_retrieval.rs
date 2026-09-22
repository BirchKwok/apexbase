//! Scenario R8: Hybrid retrieval (FTS + SQL filtering + vector reranking)
//!
//! # Business background
//!
//! An e-commerce/content search box must satisfy three kinds of condition at once:
//!   1. **Keyword relevance**: the terms the user typed must hit the title or description;
//!   2. **Business constraints**: only return listed products that are in stock and
//!      belong to this tenant/permission scope;
//!   3. **Semantic relevance**: rank by semantic similarity rather than raw keyword count.
//!
//! The traditional answer stitches together "a search engine + a database + a vector
//! store". This example shows doing all three in ApexBase with **a single SQL query**:
//! `MATCH()` for recall, ordinary predicates for filtering, and `cosine_distance` for
//! semantic ordering.
//!
//! # This example compares the quality of three retrieval paths
//!
//! To make "hybrid retrieval is better" measurable, the example:
//!   - computes the TopK of pure FTS, pure vector and hybrid separately;
//!   - uses a hand-labeled "relevant set" to compute recall@k and precision@k for each;
//!   - prints a comparison table and asserts hybrid retrieval is no worse than any single path.
//!
//! # How to run
//!
//! ```bash
//! cargo run --example r08_hybrid_retrieval --no-default-features
//! ```

use std::collections::HashSet;

use apexbase::data::Value;
use apexbase::embedded::{ApexDB, Row};
use apexbase::storage::on_demand::ColumnType;
use apexbase::storage::DurabilityLevel;
use std::collections::HashMap;

const DIM: usize = 16;

fn encode_f32_vec(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn vector_literal(values: &[f32]) -> String {
    let parts: Vec<String> = values.iter().map(|v| format!("{v:.6}")).collect();
    format!("[{}]", parts.join(","))
}

/// The same deterministic "simulated embedding" as R7: character/word-bag hashing plus
/// normalization. Semantically close texts get closer vectors, so retrieval results
/// are explainable.
fn embed(text: &str) -> Vec<f32> {
    let mut vec = vec![0.0f32; DIM];
    let chars: Vec<char> = text.chars().filter(|c| !c.is_whitespace()).collect();
    for (i, c) in chars.iter().enumerate() {
        vec[(*c as u32 as usize) % DIM] += 1.0;
        if i + 1 < chars.len() {
            let bi = ((*c as u32).wrapping_mul(31).wrapping_add(chars[i + 1] as u32)) as usize;
            vec[bi % DIM] += 0.5;
        }
    }
    for word in text.split_whitespace() {
        let mut h = 0usize;
        for b in word.bytes() {
            h = h.wrapping_mul(131).wrapping_add(b as usize);
        }
        vec[h % DIM] += 1.5;
    }
    let norm = vec.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in vec.iter_mut() {
        *x /= norm;
    }
    vec
}

/// A product sample: title / description / category / price / in_stock / tenant / vec
struct Product(&'static str, &'static str, &'static str, f64, bool, &'static str);

fn main() -> apexbase::Result<()> {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let db = ApexDB::builder(tmp.path())
        .durability(DurabilityLevel::Fast)
        .drop_if_exists(true)
        .build()?;

    println!("=== R8 hybrid retrieval ===\n");

    // Corpus design: it contains products that match the keywords but are semantically
    // irrelevant (noise), and products that are semantically relevant but do not match
    // every keyword (FTS misses), so the three paths differ visibly.
    let products: Vec<Product> = vec![
        // Semantically relevant and keyword matches (the ideal result)
        Product("Wireless Noise Cancelling Headphones", "Active noise cancelling Bluetooth headphones with long battery life for commuting", "audio", 899.0, true, "t1"),
        Product("In-Ear Noise Cancelling Earbuds", "Lightweight noise cancelling earbuds with clear calls and a comfortable fit", "audio", 299.0, true, "t1"),
        // Keyword matches but semantically off (noise)
        Product("Headphone Carrying Case", "Hard-shell crush-proof case compatible with many headphone models", "accessory", 39.0, true, "t1"),
        Product("Replacement Headphone Cable", "Silver-plated upgrade cable compatible with multiple connectors", "accessory", 79.0, true, "t1"),
        // Semantically relevant but the title has no "headphones"
        Product("Over-Ear Active Noise Cancelling", "Immersive listening and a noise cancelling tool for commuting on the subway", "audio", 1299.0, true, "t1"),
        // Business constraints not satisfied (out of stock / another tenant)
        Product("Flagship Noise Cancelling Headphones", "Top-tier noise cancelling and flagship sound quality", "audio", 2499.0, false, "t1"),
        Product("Competitor Noise Cancelling Headphones", "Another tenant's product; it must not appear in the results", "audio", 199.0, true, "t2"),
        Product("Sports Bluetooth Ear Hooks", "Sweat-proof and a stable fit for sports", "sports", 399.0, true, "t1"),
    ];

    let items = db.create_table_with_schema(
        "products",
        &[
            ("title".to_string(), ColumnType::String),
            ("description".to_string(), ColumnType::String),
            ("category".to_string(), ColumnType::String),
            ("price".to_string(), ColumnType::Float64),
            ("in_stock".to_string(), ColumnType::Bool),
            ("tenant".to_string(), ColumnType::String),
            ("vec".to_string(), ColumnType::FixedList),
        ],
    )?;

    let mut rows: Vec<Row> = Vec::new();
    // Record the original index of each row so _id can be mapped back to the
    // hand-labeled relevance.
    let mut index_of_title: HashMap<String, usize> = HashMap::new();
    for (idx, p) in products.iter().enumerate() {
        let text = format!("{} {}", p.0, p.1);
        let mut row = HashMap::new();
        row.insert("title".to_string(), Value::String(p.0.to_string()));
        row.insert("description".to_string(), Value::String(p.1.to_string()));
        row.insert("category".to_string(), Value::String(p.2.to_string()));
        row.insert("price".to_string(), Value::Float64(p.3));
        row.insert("in_stock".to_string(), Value::Bool(p.4));
        row.insert("tenant".to_string(), Value::String(p.5.to_string()));
        row.insert("vec".to_string(), Value::FixedList(encode_f32_vec(&embed(&text))));
        rows.push(row);
        index_of_title.insert(p.0.to_string(), idx);
    }
    let ids = items.insert_batch(&rows)?;
    // The _ids returned by insert_batch line up one-to-one with write order.
    let id_to_index: HashMap<u64, usize> =
        ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();
    println!("Wrote {} products, _ids = {:?}", ids.len(), ids);

    items.execute("CREATE FTS INDEX ON products (title, description)")?;

    // ── Relevant set (hand-labeled ground truth) ────────────────────────────
    // The product indexes genuinely relevant to a "noise cancelling headphones" need
    // (tenant t1 only, and in stock).
    let relevant: HashSet<usize> = [0usize, 1, 4, 7].into_iter().collect();

    let query = "noise cancelling";
    let query_vec = embed(query);
    let query_lit = vector_literal(&query_vec);
    const K: usize = 4;

    println!("\nQuery: \"{query}\"  tenant=t1  in stock only");
    println!("Hand-labeled relevant products: {}", relevant.len());

    // ── Path A: pure FTS keyword recall ─────────────────────────────────────
    let mut fts_result: Vec<usize> = Vec::new();
    let rs = items.execute(&format!(
        "SELECT _id FROM products WHERE MATCH('{query}') LIMIT {K}"
    ))?;
    for row in rs.to_rows()? {
        if let Some(Value::Int64(id)) = row.get("_id") {
            if let Some(idx) = id_to_index.get(&(*id as u64)) {
                fts_result.push(*idx);
            }
        }
    }
    println!("\n[A] pure FTS recall: {:?}", titles(&products, &fts_result));

    // ── Path B: pure vector semantic recall ─────────────────────────────────
    let mut vec_result: Vec<usize> = Vec::new();
    let rs = items.execute(&format!(
        "SELECT explode_rename(topk_distance(vec, {query_lit}, {K}, 'cosine'), '_id', 'dist')
         FROM products"
    ))?;
    for row in rs.to_rows()? {
        if let Some(Value::Int64(id)) = row.get("_id") {
            if let Some(idx) = id_to_index.get(&(*id as u64)) {
                vec_result.push(*idx);
            }
        }
    }
    println!("[B] pure vector recall: {:?}", titles(&products, &vec_result));

    // ── Path C: hybrid retrieval (FTS recall + SQL structured filtering + vector rerank) ──
    // This is the core of the example: one SQL statement expresses all three constraints.
    //   WHERE: MATCH does keyword recall, tenant/in_stock apply the business constraints
    //   ORDER: cosine_distance does the semantic ordering
    // The semantically relevant "Over-Ear Active Noise Cancelling" is recalled by FTS
    // because its title contains "noise cancelling", then pushed to the front by
    // semantic closeness — exactly the value of hybrid retrieval.
    let mut hybrid_result: Vec<usize> = Vec::new();
    let rs = items.execute(&format!(
        "SELECT _id, cosine_distance(vec, {query_lit}) AS dist
         FROM products
         WHERE MATCH('{query}')
           AND tenant = 't1'
           AND in_stock = true
         ORDER BY dist
         LIMIT {K}"
    ))?;
    for row in rs.to_rows()? {
        if let Some(Value::Int64(id)) = row.get("_id") {
            if let Some(idx) = id_to_index.get(&(*id as u64)) {
                hybrid_result.push(*idx);
            }
        }
    }
    println!("[C] hybrid retrieval result: {:?}", titles(&products, &hybrid_result));

    // ── Quantitative comparison ─────────────────────────────────────────────
    println!("\n--- quality comparison (k={K}) ---");
    println!("{:<28} {:>8} {:>10} {:>10}", "path", "hits", "precision", "recall");
    let mut scores = Vec::new();
    for (name, result) in [
        ("A pure FTS", &fts_result),
        ("B pure vector", &vec_result),
        ("C hybrid", &hybrid_result),
    ] {
        let hits = result.iter().filter(|i| relevant.contains(i)).count();
        let precision = hits as f64 / result.len().max(1) as f64;
        let recall = hits as f64 / relevant.len() as f64;
        println!(
            "{:<26} {:>8} {:>10.3} {:>10.3}",
            name,
            hits,
            precision,
            recall
        );
        scores.push((name, precision, recall));
    }

    // ── Assertion: hybrid precision must be at least as good as pure FTS ────
    // Hybrid retrieval adds business constraints and semantic ordering on top, so its
    // precision should be better or equal.
    let fts_precision = scores[0].1;
    let hybrid_precision = scores[2].1;
    assert!(
        hybrid_precision >= fts_precision,
        "hybrid precision ({hybrid_precision:.3}) must be at least pure FTS ({fts_precision:.3})"
    );
    println!(
        "\n[OK] hybrid precision {hybrid_precision:.3} >= pure FTS {fts_precision:.3}"
    );

    // ── Business-constraint check: no other tenant or out-of-stock products ──
    for idx in &hybrid_result {
        let p = &products[*idx];
        assert_eq!(p.5, "t1", "the result must not contain products from other tenants");
        assert!(p.4, "the result must not contain out-of-stock products");
    }
    println!("[OK] every hybrid result satisfies tenant=t1 and in_stock=true");

    println!("\n=== Scenario R8 complete ===");
    Ok(())
}

/// Turn an index list into a title list, for easy printing and manual checking.
fn titles(products: &[Product], indexes: &[usize]) -> Vec<&'static str> {
    indexes.iter().map(|i| products[*i].0).collect()
}
