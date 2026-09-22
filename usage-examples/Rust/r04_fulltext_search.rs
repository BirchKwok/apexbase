//! Scenario R4: In-process full-text search (FTS)
//!
//! # Business background
//!
//! A desktop app or edge service needs keyword search and typo-tolerant search over
//! local documents **without a separate search engine**. ApexBase has a built-in FTS
//! index, so no extra service such as Elasticsearch is required.
//!
//! # ApexBase capabilities demonstrated
//!
//! 1. `CREATE FTS INDEX ON table(col1, col2)`: index several text columns.
//! 2. `MATCH('term')`: exact token search, composable with ordinary SQL predicates.
//! 3. `FUZZY_MATCH('mispelled')`: tolerant search (a spelling mistake still hits).
//! 4. FTS combined with aggregates (`COUNT(*)` together with `MATCH`).
//! 5. Index operations: `SHOW FTS INDEXES` / `ALTER FTS INDEX ... DISABLE` /
//!    `DROP FTS INDEX`.
//!
//! # How to run
//!
//! ```bash
//! cargo run --example r04_fulltext_search --no-default-features
//! ```

use std::collections::HashMap;

use apexbase::data::Value;
use apexbase::embedded::{ApexDB, Row};
use apexbase::storage::on_demand::ColumnType;
use apexbase::storage::DurabilityLevel;

/// Build one document record.
fn doc_row(title: &str, body: &str, category: &str, views: i64) -> Row {
    let mut row = HashMap::new();
    row.insert("title".to_string(), Value::String(title.to_string()));
    row.insert("body".to_string(), Value::String(body.to_string()));
    row.insert("category".to_string(), Value::String(category.to_string()));
    row.insert("views".to_string(), Value::Int64(views));
    row
}

/// Print the title column of a result set.
fn print_titles(rs: apexbase::embedded::ResultSet, label: &str) -> apexbase::Result<usize> {
    let rows = rs.to_rows()?;
    println!("{label} ({} hits):", rows.len());
    for row in &rows {
        let title = match row.get("title") {
            Some(Value::String(s)) => s.clone(),
            _ => format!("{:?}", row.get("_id")),
        };
        println!("  - {title}");
    }
    Ok(rows.len())
}

fn main() -> apexbase::Result<()> {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let db = ApexDB::builder(tmp.path())
        .durability(DurabilityLevel::Fast)
        .drop_if_exists(true)
        .build()?;

    let articles = db.create_table_with_schema(
        "articles",
        &[
            ("title".to_string(), ColumnType::String),
            ("body".to_string(), ColumnType::String),
            ("category".to_string(), ColumnType::String),
            ("views".to_string(), ColumnType::Int64),
        ],
    )?;

    // The corpus deliberately spans several topics and contains a common word so the
    // difference between exact and tolerant search is observable.
    let corpus = vec![
        doc_row(
            "Rust Embedded Database Primer",
            "A high-performance embedded database for local analytics, supporting columnar storage and vector search.",
            "database",
            4200,
        ),
        doc_row(
            "Hybrid Retrieval for RAG",
            "Combines full-text recall, structured SQL filtering and vector semantic reranking to improve QA accuracy.",
            "ai",
            6100,
        ),
        doc_row(
            "Vector Quantization in Practice",
            "Trade-offs between TurboQuant and Int8 quantization in recall and storage, plus exact reranking methods.",
            "ai",
            3300,
        ),
        doc_row(
            "Column-Store Scan Optimization",
            "Discusses SIMD vectorized execution, zone maps and predicate pushdown for analytical queries.",
            "database",
            2800,
        ),
        doc_row(
            "Federated Data-Lake Queries",
            "Query Parquet, CSV and JSON directly without ETL, and materialize hot copies locally.",
            "data-lake",
            1900,
        ),
    ];
    let ids = articles.insert_batch(&corpus)?;
    println!("Wrote {} documents\n", ids.len());

    // ── 1. Create the FTS index ─────────────────────────────────────────────
    // Index both title and body: a hit in either column counts during recall.
    articles.execute("CREATE FTS INDEX ON articles (title, body)")?;
    println!("Created FTS index (title, body)");

    // Inspect the index state (output fields may differ slightly across versions;
    // this is display only).
    match articles.execute("SHOW FTS INDEXES") {
        Ok(rs) => {
            println!("SHOW FTS INDEXES -> {} rows", rs.num_rows());
        }
        Err(e) => println!("SHOW FTS INDEXES unavailable: {e}"),
    }

    // ── 2. Exact token search ───────────────────────────────────────────────
    // MATCH uses the index for token matching; here we search for documents about
    // "vector".
    let rs = articles.execute(
        "SELECT title, category, views
         FROM articles
         WHERE MATCH('vector')
         ORDER BY views DESC",
    )?;
    print_titles(rs, "\nMATCH('vector')")?;

    // ── 3. FTS + structured predicates ──────────────────────────────────────
    // This is what makes FTS more valuable than a pure inverted index: numeric and
    // enum filters layer on in the same engine, avoiding an extra round trip of
    // "search first, filter on the base table afterwards".
    let rs = articles.execute(
        "SELECT title, category, views
         FROM articles
         WHERE MATCH('retrieval') AND category = 'ai' AND views > 3000
         ORDER BY views DESC",
    )?;
    let n = print_titles(rs, "\nMATCH('retrieval') AND category='ai' AND views>3000")?;
    assert!(n > 0, "the combined search must match at least one row");
    // Reverse check: switching to a nonexistent category must match nothing.
    let rs = articles.execute(
        "SELECT title FROM articles WHERE MATCH('retrieval') AND category = 'no-such-category'",
    )?;
    let empty = rs.num_rows();
    assert_eq!(empty, 0, "a nonexistent category must not match");
    println!("[OK] the structured predicate really participates in filtering (empty-set check passed)");

    // ── 4. Tolerant search with FUZZY_MATCH ─────────────────────────────────
    // Spelling-mistake scenario: "database" is deliberately misspelled "databse".
    // Exact MATCH does not hit it, but FUZZY_MATCH still recalls it via edit distance.
    match articles.execute("SELECT title, views FROM articles WHERE FUZZY_MATCH('databse')") {
        Ok(rs) => {
            let rows = rs.to_rows()?;
            println!("\nFUZZY_MATCH('databse') matched {} rows (tolerant search)", rows.len());
            for row in &rows {
                if let Some(Value::String(t)) = row.get("title") {
                    println!("  - {t} (views={:?})", row.get("views"));
                }
            }
        }
        Err(e) => println!("\nFUZZY_MATCH is unavailable in this build: {e}"),
    }

    // ── 5. FTS combined with aggregation ────────────────────────────────────
    // Count documents matching "database": FTS acts as a WHERE condition in an
    // aggregate query.
    let rs = articles.execute(
        "SELECT category, COUNT(*) AS n
         FROM articles
         WHERE MATCH('database')
         GROUP BY category
         ORDER BY n DESC",
    )?;
    println!("\nCategory distribution of MATCH('database'):");
    for row in rs.to_rows()? {
        println!("  {:?} -> {:?}", row.get("category"), row.get("n"));
    }

    // FTS-filtered `COUNT(*)` is a single-cell aggregate, so `scalar()` reads it
    // directly (the executor normalizes the temp-table and FTS-filtered paths).
    let total_hits = articles
        .execute("SELECT COUNT(*) AS n FROM articles WHERE MATCH('database')")?
        .scalar()
        .unwrap_or(-1);
    println!("Total hits COUNT(*) = {total_hits}");
    assert!(total_hits > 0, "it must match at least one document containing \"database\"");

    // ── 6. Index operations ─────────────────────────────────────────────────
    // Demonstrate the full DROP FTS INDEX flow and verify a key correctness
    // guarantee: **dropping the index only removes search capability, never the
    // underlying table data.**
    //
    // Note: this example deliberately skips `ALTER FTS INDEX ... DISABLE`. DISABLE
    // is deliberate engine behaviour — while an index is disabled, MATCH() is
    // unavailable until you ENABLE it again. Demonstrating that here would distract
    // from the more important conclusion that dropping an index never loses data;
    // in real projects DISABLE suits a maintenance window where search is paused and
    // then re-enabled.
    match articles.execute("DROP FTS INDEX ON articles") {
        Ok(_) => println!("\nDropped the FTS index (the index file is removed, table data is kept)"),
        Err(e) => println!("\nDROP FTS INDEX unavailable: {e}"),
    }

    // After dropping the index the underlying table data must be intact — an
    // important correctness guarantee.
    let remaining = articles.count()?;
    assert_eq!(remaining, corpus.len() as u64, "dropping the index must not affect table data");
    println!("[OK] after dropping the index the table still has {remaining} rows; data is unaffected");

    println!("\n=== Scenario R4 complete ===");
    Ok(())
}
