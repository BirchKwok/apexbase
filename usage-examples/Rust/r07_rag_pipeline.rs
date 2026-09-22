//! Scenario R7: A pure-Rust local RAG retrieval pipeline
//!
//! # Business background
//!
//! A desktop/edge app written in Rust wants built-in "local knowledge-base QA":
//! documents never leave the machine and no external vector database or search
//! service is involved. The full chain is
//! **chunk documents -> embed -> store -> multi-path recall -> assemble context -> hand to the LLM**.
//!
//! This example implements everything except the "real LLM call" and the "real
//! embedding model": a deterministic hash stands in for the embedding model (in a real
//! project, replace `embed()` with your model), and the final assembled prompt is
//! printed to show how retrieval results become LLM input.
//!
//! # ApexBase capabilities demonstrated
//!
//! 1. Keep the original text and the vector together in one write (a Mixed table shape).
//! 2. An FTS index for keyword recall and a vector column for semantic recall.
//! 3. Use `_id` as the join key to fuse the two recall paths (RRF-style weighting).
//! 4. Assemble the results into context with citation numbers.
//!
//! # How to run
//!
//! ```bash
//! cargo run --example r07_rag_pipeline --no-default-features
//! ```

use std::collections::{HashMap, HashSet};

use apexbase::data::Value;
use apexbase::embedded::{ApexDB, Row};
use apexbase::storage::on_demand::ColumnType;
use apexbase::storage::DurabilityLevel;

const DIM: usize = 16;

/// Encode `&[f32]` into the little-endian byte sequence ApexBase expects.
fn encode_f32_vec(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn vector_literal(values: &[f32]) -> String {
    let parts: Vec<String> = values.iter().map(|v| format!("{v:.6}")).collect();
    format!("[{}]", parts.join(","))
}

/// A minimal "simulated embedding model".
///
/// A real project would call ONNX / candle / a remote API here. To keep the example
/// reproducible and dependency-free, we build a deterministic vector from
/// **character n-gram hashing + a bag of words**: texts that are semantically close
/// (sharing words/characters) get closer vectors, so retrieval results are meaningful
/// rather than purely random. That is enough to demonstrate the whole RAG pipeline.
fn embed(text: &str) -> Vec<f32> {
    let mut vec = vec![0.0f32; DIM];

    // 1) Split by character (friendly to CJK text) and hash each character and its
    //    bigram into some dimension.
    let chars: Vec<char> = text.chars().filter(|c| !c.is_whitespace()).collect();
    for (i, c) in chars.iter().enumerate() {
        let h = (*c as u32 as usize) % DIM;
        vec[h] += 1.0;
        if i + 1 < chars.len() {
            let bi = ((*c as u32).wrapping_mul(31).wrapping_add(chars[i + 1] as u32)) as usize;
            vec[bi % DIM] += 0.5;
        }
    }

    // 2) Then split by whitespace (friendly to English text) with word-level weight.
    for word in text.split_whitespace() {
        let mut h = 0usize;
        for b in word.bytes() {
            h = h.wrapping_mul(131).wrapping_add(b as usize);
        }
        vec[h % DIM] += 1.5;
    }

    // 3) L2-normalize so cosine distance is usable.
    let norm = vec.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in vec.iter_mut() {
        *x /= norm;
    }
    vec
}

/// Split a long document into fixed-size chunks (real systems usually overlap them;
/// a simple split is used here to keep the idea clear).
fn chunk_document(doc_id: usize, title: &str, body: &str, chunk_size: usize) -> Vec<(String, String)> {
    let chars: Vec<char> = body.chars().collect();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut seq = 0usize;
    while start < chars.len() {
        let end = (start + chunk_size).min(chars.len());
        let piece: String = chars[start..end].iter().collect();
        out.push((format!("doc{doc_id}#chunk{seq}"), format!("{title} — {piece}")));
        start = end;
        seq += 1;
    }
    if out.is_empty() {
        // Even an empty body must produce one chunk so no document goes missing upstream.
        out.push((format!("doc{doc_id}#chunk0"), title.to_string()));
    }
    out
}

fn main() -> apexbase::Result<()> {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let db = ApexDB::builder(tmp.path())
        .durability(DurabilityLevel::Fast)
        .drop_if_exists(true)
        .build()?;

    println!("=== R7 local RAG pipeline ===\n");

    // ── 1. Corpus: simulate an internal knowledge base ──────────────────────
    let docs: Vec<(&str, &str)> = vec![
        (
            "ApexBase Vector Search",
            "ApexBase ships built-in vector search with Float16 and TurboQuant quantization: use a quantized column for candidate generation, then rerank exactly with the original vectors.",
        ),
        (
            "Hybrid Retrieval and RAG",
            "Hybrid retrieval combines the recall of full-text search with the semantics of vector search and layers SQL structured filtering on top; it is the key to improving RAG accuracy.",
        ),
        (
            "Federated Data-Lake Queries",
            "Query Parquet CSV and JSON files directly, federate without ETL, and materialize hot local copies to speed up repeated analysis.",
        ),
        (
            "Transactions and Concurrent Writes",
            "Multiple worker threads can bulk-write concurrently, transactions keep a batch atomic, and rolled-back data stays invisible.",
        ),
        (
            "Performance and Indexes",
            "Column-store scans combine predicate pushdown with zone maps to skip irrelevant data blocks and significantly cut the IO cost of analytical queries.",
        ),
    ];

    // ── 2. Create the table: chunk id + source text + vector ────────────────
    let chunks = db.create_table_with_schema(
        "chunks",
        &[
            ("chunk_id".to_string(), ColumnType::String),
            ("text".to_string(), ColumnType::String),
            ("vec".to_string(), ColumnType::FixedList),
        ],
    )?;

    // ── 3. Chunk + embed + store ────────────────────────────────────────────
    let mut rows: Vec<Row> = Vec::new();
    let mut chunk_count = 0usize;
    for (doc_id, (title, body)) in docs.iter().enumerate() {
        for (chunk_id, text) in chunk_document(doc_id, title, body, 24) {
            let mut row = HashMap::new();
            row.insert("chunk_id".to_string(), Value::String(chunk_id));
            row.insert("text".to_string(), Value::String(text.clone()));
            row.insert(
                "vec".to_string(),
                Value::FixedList(encode_f32_vec(&embed(&text))),
            );
            rows.push(row);
            chunk_count += 1;
        }
    }
    let ids = chunks.insert_batch(&rows)?;
    println!("Corpus: {} documents -> {} chunks, wrote {} rows", docs.len(), chunk_count, ids.len());

    // Build the full-text index: in RAG, FTS covers "exact keyword hits" while the
    // vector covers "semantic closeness".
    chunks.execute("CREATE FTS INDEX ON chunks (text)")?;
    println!("Created the FTS index");

    // ── 4. Multi-path recall ────────────────────────────────────────────────
    let query = "How can I improve the retrieval accuracy of RAG?";
    let query_vec = embed(query);
    let query_lit = vector_literal(&query_vec);
    println!("\nUser query: {query}");

    const TOP_K: usize = 4;

    // 4a) Keyword recall: pull content words out of the query and search with MATCH.
    let keywords: Vec<&str> = vec!["RAG", "retrieval", "accuracy"];
    let mut fts_hits: Vec<u64> = Vec::new();
    for kw in &keywords {
        if let Ok(rs) = chunks.execute(&format!(
            "SELECT _id FROM chunks WHERE MATCH('{kw}')"
        )) {
            for row in rs.to_rows()? {
                if let Some(Value::Int64(id)) = row.get("_id") {
                    fts_hits.push(*id as u64);
                }
            }
        }
    }
    // Deduplicate (one chunk can match several keywords), keeping first-seen order.
    let mut seen = HashSet::new();
    fts_hits.retain(|id| seen.insert(*id));
    println!("Keyword recall ({keywords:?}): {} chunks {:?}", fts_hits.len(), fts_hits);

    // 4b) Semantic recall: vector TopK.
    let rs = chunks.execute(&format!(
        "SELECT explode_rename(topk_distance(vec, {query_lit}, {TOP_K}, 'cosine'), '_id', 'dist')
         FROM chunks"
    ))?;
    let mut vec_hits: Vec<(u64, f64)> = Vec::new();
    for row in rs.to_rows()? {
        let id = match row.get("_id") {
            Some(Value::Int64(v)) => *v as u64,
            _ => continue,
        };
        let dist = match row.get("dist") {
            Some(Value::Float64(v)) => *v,
            _ => 1.0,
        };
        vec_hits.push((id, dist));
    }
    println!("Semantic recall (vector Top{TOP_K}): {:?}", vec_hits);

    // ── 5. Fusion ranking (RRF: Reciprocal Rank Fusion) ─────────────────────
    // RRF does not need the two score scales to be comparable; it only uses ranks,
    // which makes it a great fit for hybrid retrieval:
    //   score = sum of 1 / (k + rank), where k is usually 60.
    const RRF_K: f64 = 60.0;
    let mut fused: HashMap<u64, f64> = HashMap::new();

    for (rank, id) in fts_hits.iter().enumerate() {
        *fused.entry(*id).or_insert(0.0) += 1.0 / (RRF_K + (rank + 1) as f64);
    }
    for (rank, (id, _dist)) in vec_hits.iter().enumerate() {
        *fused.entry(*id).or_insert(0.0) += 1.0 / (RRF_K + (rank + 1) as f64);
    }

    let mut ranked: Vec<(u64, f64)> = fused.into_iter().collect();
    // Sort by descending score; break ties by ascending _id so results are stable
    // and reproducible.
    ranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });
    ranked.truncate(TOP_K);

    println!("\nFinal Top{TOP_K} after RRF fusion (_id, score):");
    for (id, score) in &ranked {
        println!("  _id={id}  score={score:.6}");
    }
    assert!(!ranked.is_empty(), "the fused result must not be empty");

    // ── 6. Assemble the context and prompt ──────────────────────────────────
    // In a real project the output of this step is sent to the LLM; it is printed
    // here so you can inspect it.
    println!("\n----- Assembled LLM prompt -----");
    println!("[system] You are an enterprise knowledge-base assistant. Answer only from the material provided below; if it is insufficient, say so explicitly.");
    println!("[context]");
    for (i, (id, score)) in ranked.iter().enumerate() {
        let rs = chunks.execute(&format!(
            "SELECT chunk_id, text FROM chunks WHERE _id = {id}"
        ))?;
        if let Some(row) = rs.to_rows()?.first() {
            let chunk_id = match row.get("chunk_id") {
                Some(Value::String(s)) => s.clone(),
                _ => format!("id-{id}"),
            };
            let text = match row.get("text") {
                Some(Value::String(s)) => s.clone(),
                _ => String::new(),
            };
            // Citation numbers let the model point back to sources, which helps trace answers.
            println!("[{i}]({chunk_id}, rrf={score:.6}) {text}");
        }
    }
    println!("[question] {query}");
    println!("----- end of prompt -----");

    // ── 7. Correctness checks ───────────────────────────────────────────────
    // The semantically most relevant document, "Hybrid Retrieval and RAG", should
    // appear in the final context.
    let mut context_hits_rag_doc = 0usize;
    for (id, _) in &ranked {
        let rs = chunks.execute(&format!("SELECT text FROM chunks WHERE _id = {id}"))?;
        if let Some(Value::String(text)) = rs.to_rows()?.first().and_then(|r| r.get("text")) {
            if text.contains("Hybrid Retrieval") {
                context_hits_rag_doc += 1;
            }
        }
    }
    println!("\nChunks in the context matching the \"Hybrid Retrieval\" topic: {context_hits_rag_doc}");
    assert!(
        context_hits_rag_doc > 0,
        "the most relevant document should appear in the final context"
    );
    println!("[OK] the RAG retrieval chain works end to end");

    println!("\n=== Scenario R7 complete ===");
    Ok(())
}
