"""Scenario 10: Local RAG knowledge base (a minimal recall + rerank retrieval pipeline)

Business context
----------------
An internal knowledge-base team needs a **purely local** RAG
(Retrieval-Augmented Generation) retrieval layer: keep product docs, runbooks and
paper notes together with their embedding vectors in ApexBase, then when a user asks
a question first do a coarse recall (keyword full-text search) and then a fine rerank
(vector semantic similarity), and finally assemble the Top-K passages into the context
prompt handed to the LLM.

This example **does not call any real LLM**; it only prints the assembled prompt, so
you can focus on the retrieval pipeline itself and can drop this code straight into any
model service.

ApexBase features demonstrated
------------------------------
1. A single table carries both **structured fields** (title/category/day) and a
   **vector field** (``float32_vector``) — the "HTAP + vector" unified-storage usage.
2. ``CREATE FTS INDEX ON docs(title, body)`` builds a native full-text index, used
   with ``WHERE MATCH('...')`` and ``FTS_SCORE()`` for BM25 coarse recall.
3. "Recall + rerank" inside one SQL: a CTE first pulls the FTS candidate set with
   ``MATCH()``, then the same SQL semantically reranks those candidates with
   ``cosine_distance(emb, [inline literal])`` (a vector literal must be inlined; a
   ``?`` placeholder does not work).
4. Columnar bulk write: the column-oriented ``client.store({"column": [...]})`` form
   (faster bulk load).
5. Brute-force numpy computation as ground truth, to quantify the recall of the
   retrieval pipeline.

Capability boundaries (read this: the example only uses currently-supported forms)
---------------------------------------------------------------------------------
1. Vector distance functions (``cosine_distance`` / ``array_distance``) accept
   **inline array literals only**: ``cosine_distance(emb, [0.1,0.2,...])``. Binding a
   list through a ``?`` placeholder is expanded into several scalar arguments and
   fails with "requires exactly 2 arguments", so this example always renders literals
   with ``vector_literal()``.
2. ``DATE_TRUNC`` / ``strftime`` / ``DATE()`` / ``EXTRACT`` are **unsupported**:
   materialise time buckets as strings at insert time (``YYYY-MM-DD``); string range
   comparison works. Window functions themselves now compose
   (``SUM(x) OVER (PARTITION BY g)``, ``AVG(x) OVER (...)``), but a window inside
   ``CAST(...)`` and a window in ``ORDER BY`` remain unsupported.
3. Reading a vector column back over SQL yields ``None`` (binary vector columns are
   treated as an opaque type), so retrieval consistency is checked through
   ``topk_distance`` / distance functions rather than by fetching vectors into Python.
4. FTS needs real tokens (2+ characters); single characters usually return nothing,
   and a ``:memory:`` database does not support FTS — a file-backed database is
   required.

How to run
----------
    python py10_rag_knowledge_base.py

Output: the console prints the candidate set, the rerank results, the assembled LLM
prompt and the recall check; the database files live in ``_out/py_10/``.
"""

from __future__ import annotations

import os

import numpy as np

from apexbase import ApexClient
from _demo_env import make_embeddings, rng, section, show, vector_literal, work_dir

SLUG = "10"
DIM = 32                 # embedding dimension: a small dim keeps the example to seconds
DOCS_PER_TOPIC = 10      # 10 docs per topic -> 12 topics * 10 = 120 docs
TOP_K = 5                # number of context passages finally handed to the LLM
FTS_CANDIDATES = 60      # coarse FTS candidate count (rerank only runs over these, saving compute)

# 6 business categories, 2 topics each. Every topic owns an independent "word bag":
# the bag both drives the readable title/body (fed to FTS) and defines the semantic
# cluster center (fed to the vector column).
CATEGORIES: dict[str, list[tuple[str, list[str], list[str]]]] = {
    "vector_db": [
        ("Vector search", ["vector", "index", "neighbor", "recall"], ["vector", "index", "nearest", "recall"]),
        ("Hybrid retrieval", ["hybrid", "retrieval", "filter", "rerank"], ["hybrid", "retrieval", "filter", "rerank"]),
    ],
    "rag": [
        ("Retrieval-augmented generation", ["retrieval", "augmented", "generation", "context"], ["retrieval", "augmented", "context", "prompt"]),
        ("Chunking and embedding", ["chunking", "embedding", "semantic", "splitter"], ["chunking", "embedding", "semantic", "splitter"]),
    ],
    "storage": [
        ("Columnar storage", ["columnar", "storage", "compression", "batch"], ["columnar", "storage", "compression", "batch"]),
        ("Memory mapping", ["memory", "mapping", "zerocopy", "latency"], ["mmap", "zerocopy", "latency", "scan"]),
    ],
    "lake": [
        ("Data lake", ["lake", "partition", "lakehouse", "manifest"], ["lake", "partition", "lakehouse", "manifest"]),
        ("Federated query", ["federation", "query", "join", "pushdown"], ["federation", "query", "join", "pushdown"]),
    ],
    "ops": [
        ("Performance benchmark", ["benchmark", "throughput", "latency", "regression"], ["benchmark", "throughput", "latency", "regression"]),
        ("Performance guard", ["guard", "baseline", "acceptance", "threshold"], ["guard", "baseline", "acceptance", "threshold"]),
    ],
    "quant": [
        ("Vector quantization", ["quantization", "codebook", "precision", "accelerator"], ["quantization", "codebook", "precision", "accelerator"]),
        ("Product quantization", ["scalar", "rotation", "hadamard", "compact"], ["scalar", "rotation", "hadamard", "compact"]),
    ],
}

# Natural-language questions (kept lowercase so the keyword membership test matches).
# The keywords guarantee MATCH() hits; the semantics let the vector rerank order them.
QUERIES = [
    "how do i tune vector index recall",
    "how should retrieval augmented generation organize context",
    "data lake partition and lakehouse manifest",
    "how does the quantization accelerator affect precision",
]


def build_corpus() -> tuple[list[dict], dict[str, np.ndarray], list[np.ndarray]]:
    """Generate a deterministic knowledge-base corpus, returning (documents, topic -> semantic center).

    Design notes:
    1. **One semantic center per topic**; a document vector = center + small
       perturbation. Documents of the same topic therefore form a natural cluster in
       vector space, which makes the Top-K results explainable.
    2. The document body embeds the topic word bag, so FTS BM25 scores have
       discrimination.
    3. Topic centers come from ``make_embeddings``, which returns **unit vectors** —
       for unit vectors the cosine and L2 orderings are strictly equivalent, which is
       convenient for cross-checking (see py14).
    """
    rnd = rng(1010)
    topics = [(cat, topic, zh, en) for cat, items in CATEGORIES.items() for topic, zh, en in items]
    centers = make_embeddings(len(topics), DIM, rnd)
    center_map = {topic: np.asarray(centers[i], dtype=np.float32) for i, (_, topic, _, _) in enumerate(topics)}

    # Generate vectors topic by topic (same center + perturbation), then expand into
    # rows: this keeps the result deterministic and efficient.
    docs: list[dict] = []
    emb_vectors: list[np.ndarray] = []
    for t_idx, (category, topic, zh_words, en_words) in enumerate(topics):
        vectors = make_embeddings(DOCS_PER_TOPIC, DIM, rnd, cluster_centers=[centers[t_idx]], spread=0.18)
        for j, vec in enumerate(vectors):
            en_focus = en_words[j % len(en_words)]
            zh_focus = zh_words[(j + 1) % len(zh_words)]
            # The body deliberately mixes both word bags so BM25 always has several
            # distinct tokens to score. Keep the strings free of ASCII double quotes
            # so the f-strings below stay syntactically valid.
            title = f"{topic} field notes {j + 1}: {zh_focus} and {en_focus}"
            body = (
                f"This manual explains {topic} in practice for {zh_focus}, built around {' '.join(zh_words)}, "
                f"with the corresponding terminology {' '.join(en_words)}. "
                f"It focuses on tuning steps for {en_focus}, common pitfalls and verification methods, "
                f"for engineering roll-out in the {category} scenario."
            )
            docs.append(
                {
                    "title": title,
                    "body": body,
                    "category": category,
                    "day": f"2024-{(t_idx % 6) + 1:02d}-{(j % 28) + 1:02d}",
                    "topic": topic,
                }
            )
            emb_vectors.append(np.asarray(vec, dtype=np.float32))
    return docs, center_map, emb_vectors


def brute_force_topk(vectors: np.ndarray, query: np.ndarray, k: int) -> list[int]:
    """Exact numpy brute-force Top-K (cosine similarity), returning 1-based ``_id`` s.

    Why compute it ourselves: recall is only meaningful against trustworthy ground
    truth. For unit vectors cosine similarity is the inner product, so a single matrix
    multiply is enough.
    """
    q = query / (np.linalg.norm(query) or 1.0)
    sims = vectors @ q
    order = np.argsort(-sims, kind="stable")[:k]
    return [int(i) + 1 for i in order]  # ApexBase ``_id`` s start at 1


def as_prompt(question: str, hits: list[dict]) -> str:
    """Assemble the reranked Top-K passages into the LLM context prompt (plain strings).

    In a real project you can hand this text directly to the system/user message of any
    chat model.
    """
    lines = [
        "You are a rigorous knowledge-base assistant: answer only from the material below,",
        "and if the material is insufficient, state exactly which information is missing.",
        "",
        "[Retrieved passages]",
    ]
    for rank, hit in enumerate(hits, start=1):
        lines.append(
            f"[{rank}] category={hit['category']} title={hit['title']}"
            f" (bm25={hit['bm25']:.4f}, semantic_distance={hit['vdist']:.4f})"
        )
        lines.append(f"    body: {hit['body']}")
    lines += ["", "[User question]", question, "", "[Answer]"]
    return "\n".join(lines)


def main() -> None:
    base = work_dir(SLUG)
    docs, center_map, emb_vectors = build_corpus()
    vectors = np.asarray(emb_vectors, dtype=np.float32)

    section("Step 1: columnar bulk-write documents + vectors, then create the FTS index")
    db_path = os.path.join(base, "db")
    with ApexClient(db_path) as client:
        client.create_table(
            "docs",
            {
                "title": "string",
                "body": "string",
                "category": "string",
                "day": "string",
                # FLOAT32_VECTOR is the "authoritative column": any exact rerank reads
                # the original vector from it.
                "emb": "float32_vector",
            },
        )
        client.use_table("docs")
        # Columnar write (dict of lists): fewer FFI calls than a row-by-row dict, so it
        # suits bulk data loading.
        client.store(
            {
                "title": [d["title"] for d in docs],
                "body": [d["body"] for d in docs],
                "category": [d["category"] for d in docs],
                "day": [d["day"] for d in docs],
                "emb": emb_vectors,
            }
        )
        # FTS is a file-level index: create the table and write the data first, then run
        # CREATE FTS INDEX. Incremental writes after index creation are synced
        # automatically (py17 demonstrates the incremental case).
        client.execute("CREATE FTS INDEX ON docs(title, body)")
        show("indexed documents", client.count_rows())
        show("FTS stats", client.get_fts_stats())
        show("persistent tables", client.list_tables())

        # Record, per question: (question, exact full-table Top-K, SQL rerank recall,
        # FTS topic purity, candidate recall ceiling).
        recall_report: list[tuple[str, list[int], float, float, float]] = []

        for qi, question in enumerate(QUERIES, start=1):
            section(f"Step 2.{qi}: ask \"{question}\" — full recall + rerank pipeline")
            # Keyword extraction: a real system uses a tokenizer/LLM; here we do a
            # deterministic match against the category word bags, producing a query
            # string whose tokens actually occur in the bodies, which drives MATCH().
            keywords = [w for items in CATEGORIES.values() for _, zh, en in items for w in (zh + en) if w in question]
            fts_query = " ".join(dict.fromkeys(keywords)) or question[:4]
            show("MATCH query string", fts_query)

            # ---- 2.1 Coarse recall: MATCH + BM25 ----
            # MATCH() goes through the ApexFTS inverted index and only pulls documents
            # with a token hit; the no-argument FTS_SCORE() binds to the single MATCH()
            # query in this statement automatically.
            candidates = client.execute(
                f"""
                SELECT _id, title, body, category, FTS_SCORE() AS bm25
                FROM docs
                WHERE MATCH('{fts_query}')
                ORDER BY bm25 DESC
                LIMIT {FTS_CANDIDATES}
                """
            ).to_dict()
            show("FTS candidates", len(candidates))
            if not candidates:
                print("[WARN] no FTS candidate for this question; skipping (a real system should fall back to pure vector search)")
                continue

            # ---- 2.2 Fine rerank: vector semantic similarity within the candidate set ----
            # The query vector is taken from the semantic center of the topic matched by
            # the question, simulating "encode the question into a vector". A real system
            # calls an embedding model here; this example is offline and uses a
            # deterministic vector instead.
            # First score every category by word-bag hits, take the best category, then
            # take the best-matching topic inside it.
            cat_scores = {
                cat: sum(1 for _, zh, en in items for w in (zh + en) if w in question)
                for cat, items in CATEGORIES.items()
            }
            category = max(cat_scores, key=lambda c: (cat_scores[c], c))
            # Target topic: the topic in that category whose word bag hits the question most.
            topic, zh_words, en_words = max(
                CATEGORIES[category],
                key=lambda t: sum(1 for w in (t[1] + t[2]) if w in question),
            )
            # Simulate "question encoding": add a deterministic small perturbation to the
            # topic center so the candidates get genuinely different semantic distances
            # (otherwise documents of the same topic would tie for the same rank).
            # A real system calls an embedding model; this example uses rng(fixed) for
            # reproducibility.
            qrnd = rng(7000 + qi)
            perturbed = [float(x) + qrnd.gauss(0.0, 0.05) for x in center_map[topic]]
            qnorm = float(np.linalg.norm(perturbed)) or 1.0
            query_vec = np.asarray([x / qnorm for x in perturbed], dtype=np.float32)
            show("inferred category / target topic", f"{category} / {topic}")

            # Key detail: the vector literal must be inlined into the SQL (capability
            # boundary 1 in the module docstring). Here we use
            # cosine_distance(emb, [...]); cosine_distance returns 1 - cos, which for
            # **unit vectors** orders identically to L2 distance (py14 proves the math).
            # Note that the emb column itself is not compared, it is the "searched
            # column", so the literal can be inlined without a CROSS JOIN over a constant
            # row — which also keeps the SQL short and readable.
            literal = vector_literal(query_vec, precision=6)
            reranked = client.execute(
                f"""
                WITH cand AS (
                    SELECT _id, title, body, category, emb, FTS_SCORE() AS bm25
                    FROM docs
                    WHERE MATCH('{fts_query}')
                    ORDER BY bm25 DESC
                    LIMIT {FTS_CANDIDATES}
                )
                SELECT
                    _id          AS id,
                    title        AS title,
                    body         AS body,
                    category     AS category,
                    bm25         AS bm25,
                    cosine_distance(emb, {literal}) AS vdist
                FROM cand
                WHERE category = '{category}'
                ORDER BY vdist ASC
                LIMIT {TOP_K}
                """
            ).to_dict()
            show("reranked Top-K semantic distance (cosine)", [round(r["vdist"], 4) for r in reranked])
            for rank, row in enumerate(reranked, start=1):
                show(f"  #{rank}", f"{row['title']} ({row['category']}) bm25={row['bm25']:.4f}")

            # ---- 2.3 Self-check a: every recalled row must fall in the target category ----
            assert all(r["category"] == category for r in reranked), "structured filter is not effective"
            print(f"[OK] all {len(reranked)} Top-K rows fall inside category {category} (SQL structured filter works)")

            # ---- 2.4 Self-check b: rerank order must match ascending semantic distance ----
            dists = [r["vdist"] for r in reranked]
            assert dists == sorted(dists), "rerank results are not in ascending semantic distance"
            print("[OK] rerank results are strictly ascending by cosine_distance (nearest first)")

            # ---- 2.5 Self-check c: compare with the numpy exact solution, quantify recall ----
            exact_ids = brute_force_topk(vectors, query_vec, TOP_K)
            got_ids = [int(r["id"]) for r in reranked]
            overlap = len(set(exact_ids) & set(got_ids))
            sql_recall = overlap / TOP_K
            # Per-SQL "coarse recall quality": how much of the true semantic Top-K the FTS
            # candidate set covers. This number is the recall ceiling of the whole
            # pipeline — no rerank can conjure up a document that was never recalled.
            pool_recall = len(set(exact_ids) & {int(c["_id"]) for c in candidates}) / TOP_K
            print(
                f"[OK] FTS candidate set covers semantic Top-K: {pool_recall:.2f} (recall ceiling); "
                f"SQL rerank overlaps the numpy exact solution {overlap}/{TOP_K} = {sql_recall:.2f}"
            )
            assert pool_recall >= 0.5, f"FTS candidate recall ceiling too low: {pool_recall}"
            assert sql_recall >= 0.4, f"SQL rerank recall too low: {sql_recall}"

            # ---- 2.5b Self-check d: exact search over the **full table** via topk_distance ----
            # topk_distance is the native ApexBase Top-K primitive (O(n log k) heap); it
            # accepts a numpy array and is the "gold standard" for the retrieval pipeline.
            # Here we assert it matches numpy brute force exactly, proving the recall
            # numbers are trustworthy.
            topk_ids = [int(r["_id"]) for r in client.topk_distance("emb", query_vec, k=TOP_K, metric="cosine").to_dict()]
            assert topk_ids == exact_ids, f"topk_distance disagrees with the numpy exact solution: {topk_ids} vs {exact_ids}"
            print(f"[OK] topk_distance full-table exact Top-K {topk_ids} == numpy brute force (recall@K = 1.00)")

            # ---- 2.5c Self-check e: "topic purity" of the pure FTS candidates ----
            # FTS only understands tokens, not semantics: the candidate set also contains
            # documents with the same tokens but a different topic. This purity is the
            # quantitative evidence for "why a vector rerank is still needed".
            topic_ids = {i + 1 for i, d in enumerate(docs) if d["topic"] == topic}
            purity = sum(1 for c in candidates if int(c["_id"]) in topic_ids) / len(candidates)
            recall_report.append((question, topk_ids, sql_recall, purity, pool_recall))
            print(f"[OK] FTS candidate target-topic purity = {purity:.2%} (the rest is token noise handled by the vector rerank)")
            assert len(candidates) >= TOP_K, "not enough FTS candidates to rerank"

            # ---- 2.6 Assemble the prompt (no real LLM call) ----
            section(f"Step 3.{qi}: build the context prompt for the LLM (printed only, no model call)")
            prompt = as_prompt(question, reranked)
            print(prompt)
            show("prompt characters", len(prompt))
            assert question in prompt and reranked[0]["title"] in prompt, "prompt does not contain the question or the retrieved material"

        section("Step 4: retrieval pipeline quality summary")
        for question, topk_ids, sql_recall, purity, pool_recall in recall_report:
            show("question", question)
            show("  full-table exact Top-K (topk_distance == numpy)", topk_ids)
            show("  single-SQL rerank recall@K / FTS candidate recall ceiling", f"{sql_recall:.2f} / {pool_recall:.2f}")
            show("  FTS candidate topic purity", f"{purity:.2%}")
        mean_recall = sum(r for _, _, r, _, _ in recall_report) / len(recall_report)
        show("single-SQL rerank mean recall@K", round(mean_recall, 4))
        print(
            "[Conclusion] The recall ceiling of the two-stage pipeline is set by the FTS "
            "candidate set; the vector rerank only performs fine semantic ordering inside "
            "it. With this example data, topk_distance full-table recall@K is always 1.00, "
            "which shows the recall numbers are trustworthy and the pipeline works."
        )
        assert mean_recall >= 0.4, "mean recall is too low"

    print(f"\n=== Scenario 10: local RAG knowledge base (recall + rerank) complete ===\nDatabase is at: {db_path}")


if __name__ == "__main__":
    main()
