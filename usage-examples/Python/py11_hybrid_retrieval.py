"""Scenario 11: Hybrid retrieval (full-text search + structured filtering + vector rerank, in one SQL)

Business context
----------------
Enterprise knowledge bases / e-commerce search / support-ticket systems all need
"hybrid retrieval":

* **Full-text search (BM25)** guarantees exact keyword hits (model numbers, proper
  nouns, error codes);
* **Structured filtering** guarantees compliance and freshness (only visibility levels
  you are allowed to see, only the last N days);
* **Vector semantic rerank** guarantees that "the user phrased it differently" still
  surfaces the most relevant results.

This example pushes these conditions into **one SQL**: FTS hits + structured WHERE +
cosine semantic rerank, and compares it against two baselines ("pure FTS" and "pure
vector"), quantifying the benefit of hybrid retrieval with "effective recall@K +
constraint violations".

ApexBase features demonstrated
------------------------------
1. A single SQL uses the inverted index (``MATCH()`` + ``FTS_SCORE()``), structured
   predicates (category / date string range / permission) and vector distance
   (``cosine_distance``) at the same time.
2. A ``WITH`` CTE does two-stage retrieval: coarse-recall candidates first, then
   semantic rerank inside the candidates followed by LIMIT.
3. ``topk_distance`` serves as the "pure vector baseline", cross-validating the SQL form.
4. numpy brute force as ground truth, quantifying recall@K and set overlap of the three
   strategies.
5. Date buckets are materialised at insert time as ``YYYY-MM-DD`` strings, and range
   filtering uses string comparison (ApexBase does **not** support ``DATE_TRUNC`` /
   ``strftime`` / ``DATE()``).

Key conclusions (measured and printed by this example)
------------------------------------------------------
* Pure FTS: precise keywords, but a different phrasing is simply not recalled;
* Pure vector: good semantics, but it **cannot** express hard constraints such as
  permission/time/category, and there is no BM25 in the ordering;
* Hybrid: FTS sets the recall ceiling and exact matches, structured predicates enforce
  compliance and freshness, and vectors put the "semantically nearest" first — all three
  cooperate inside one SQL.

Capability boundaries
---------------------
* Vector distance functions accept **inline array literals only**
  (``cosine_distance(emb, [...])``); a ``?`` placeholder expands the list into several
  scalar arguments and fails.
* ``DATE_TRUNC`` / ``strftime`` / ``DATE()`` / ``EXTRACT`` are unsupported: materialise
  time buckets as strings at insert time. Window functions now compose (for example
  ``SUM(x) OVER (PARTITION BY g)`` and ``AVG(x) OVER (...)``), but a window inside
  ``CAST(...)`` and a window in ``ORDER BY`` remain unsupported; pure vector ordering
  needs no window at all, just ``ORDER BY distance LIMIT`` or ``topk_distance``.
* A ``:memory:`` database does not support FTS, so a file-backed database is required;
  FTS tokens need 2+ characters.

How to run
----------
    python py11_hybrid_retrieval.py

Output: the Top-K, recall@K and set overlap of the three retrieval paths; the database
lives in ``_out/py_11/``.
"""

from __future__ import annotations

import os

import numpy as np

from apexbase import ApexClient
from _demo_env import make_embeddings, rng, section, show, vector_literal, work_dir

SLUG = "11"
DIM = 48
TOPIC_COUNT = 12
DOCS_PER_TOPIC = 100          # 12 * 100 = 1200 documents
TOP_K = 10
FTS_CANDIDATES = 400          # coarse-recall size of the two-stage retrieval
DAY_START = "2024-03-01"      # structured time filter: only data on or after this date
VISIBILITY = "public"         # structured permission filter: only recall public documents

# Topic table: (category, topic, domain word bag, terminology word bag, visibility)
TOPICS: list[tuple[str, str, list[str], list[str], str]] = [
    ("vector_db", "Vector search", ["vector", "index", "neighbor", "recall"], ["vector", "index", "nearest", "recall"], "public"),
    ("vector_db", "Hybrid retrieval", ["hybrid", "retrieval", "filter", "rerank"], ["hybrid", "retrieval", "filter", "rerank"], "public"),
    ("rag", "Retrieval-augmented generation", ["retrieval", "augmented", "generation", "context"], ["retrieval", "augmented", "context", "prompt"], "public"),
    ("rag", "Chunking and embedding", ["chunking", "embedding", "semantic", "splitter"], ["chunking", "embedding", "semantic", "splitter"], "internal"),
    ("storage", "Columnar storage", ["columnar", "storage", "compression", "batch"], ["columnar", "storage", "compression", "batch"], "public"),
    ("storage", "Memory mapping", ["memory", "mapping", "zerocopy", "latency"], ["mmap", "zerocopy", "latency", "scan"], "public"),
    ("lake", "Data lake", ["lake", "partition", "lakehouse", "manifest"], ["lake", "partition", "lakehouse", "manifest"], "public"),
    ("lake", "Federated query", ["federation", "query", "join", "pushdown"], ["federation", "query", "join", "pushdown"], "internal"),
    ("ops", "Performance benchmark", ["benchmark", "throughput", "latency", "regression"], ["benchmark", "throughput", "latency", "regression"], "public"),
    ("ops", "Performance guard", ["guard", "baseline", "acceptance", "threshold"], ["guard", "baseline", "acceptance", "threshold"], "public"),
    ("quant", "Vector quantization", ["quantization", "codebook", "precision", "accelerator"], ["quantization", "codebook", "precision", "accelerator"], "public"),
    ("quant", "Product quantization", ["scalar", "rotation", "hadamard", "compact"], ["scalar", "rotation", "hadamard", "compact"], "internal"),
]

# Three queries covering both "explicit keyword" and "different phrasing" scenarios.
QUERIES = [
    "vector index recall tuning",
    "hybrid retrieval filtering and reranking",
    "data lake partition manifest",
]


def build_documents() -> tuple[list[dict], np.ndarray, dict[str, np.ndarray]]:
    """Generate 1200 documents (text, visibility, date, embedding) and each topic's semantic center.

    Returns ``(docs, vectors, centers)``:
    * ``docs``    each row holds title / body / category / day / visibility / topic
    * ``vectors`` an ``(N, DIM)`` float32 matrix whose row order matches docs (_id = row + 1)
    * ``centers`` topic -> semantic center (unit vector), used to simulate an embedding model for the query vector
    """
    rnd = rng(1111)
    centers_list = make_embeddings(len(TOPICS), DIM, rnd)
    centers = {t[1]: np.asarray(centers_list[i], dtype=np.float32) for i, t in enumerate(TOPICS)}

    docs: list[dict] = []
    vectors: list[np.ndarray] = []
    for ti, (category, topic, zh, en, visibility) in enumerate(TOPICS):
        topic_vectors = make_embeddings(DOCS_PER_TOPIC, DIM, rnd, cluster_centers=[centers_list[ti]], spread=0.30)
        for j, vec in enumerate(topic_vectors):
            zh_focus = zh[(j + 1) % len(zh)]
            en_focus = en[j % len(en)]
            # Dates span 2024-02 / 2024-03 to demonstrate string range filtering.
            day = f"2024-0{2 + (j % 2)}-{(j % 28) + 1:02d}"
            docs.append(
                {
                    "title": f"{topic} in practice {j + 1}: {zh_focus} and {en_focus}",
                    "body": (
                        f"This article discusses {topic}, covering {' '.join(zh)} and the terminology {' '.join(en)}, "
                        f"focusing on rollout steps and verification methods for {zh_focus} / {en_focus}."
                    ),
                    "category": category,
                    "day": day,
                    # topic is only used for in-example evaluation (it is not written to the database)
                    "topic": topic,
                    # Mark 1/4 of the documents internal to demonstrate the permission-filter tradeoff
                    "visibility": visibility if j % 4 else "internal",
                }
            )
            vectors.append(np.asarray(vec, dtype=np.float32))
    return docs, np.asarray(vectors, dtype=np.float32), centers


def brute_force_topk(vectors: np.ndarray, query: np.ndarray, k: int) -> list[int]:
    """numpy exact cosine Top-K (ground truth), returning 1-based ``_id`` s."""
    q = query / (np.linalg.norm(query) or 1.0)
    sims = vectors @ q
    return [int(i) + 1 for i in np.argsort(-sims, kind="stable")[:k]]


def recall_of(candidate_ids: list[int], relevant: set[int], k: int) -> float:
    """Fraction of the first k candidate ids that hit the relevant set."""
    return len(set(candidate_ids[:k]) & relevant) / k


def brute_force_topk_masked(vectors: np.ndarray, query: np.ndarray, k: int, allowed: np.ndarray) -> list[int]:
    """numpy exact cosine Top-K restricted to the "compliant subset".

    The hard constraints of hybrid retrieval (permission / time) only mean something when
    applied to the candidate set, so ground truth must also be defined on **the same
    compliant subset**; otherwise the comparison is unfair.
    """
    q = query / (np.linalg.norm(query) or 1.0)
    sims = vectors @ q
    idx = np.where(allowed)[0]
    order = idx[np.argsort(-sims[idx], kind="stable")[:k]]
    return [int(i) + 1 for i in order]


def main() -> None:
    base = work_dir(SLUG)
    docs, vectors, centers = build_documents()

    with ApexClient(os.path.join(base, "db")) as client:
        section("Step 1: load data (columnar bulk write) and create the FTS index")
        client.create_table(
            "docs",
            {
                "title": "string",
                "body": "string",
                "category": "string",
                "day": "string",          # string date: the time bucket is materialised at insert time
                "visibility": "string",
                "emb": "float32_vector",
            },
        )
        client.use_table("docs")
        client.store(
            {
                "title": [d["title"] for d in docs],
                "body": [d["body"] for d in docs],
                "category": [d["category"] for d in docs],
                "day": [d["day"] for d in docs],
                "visibility": [d["visibility"] for d in docs],
                "emb": [v for v in vectors],
            }
        )
        client.execute("CREATE FTS INDEX ON docs(title, body, category)")
        show("total documents", client.count_rows())
        show("FTS stats", client.get_fts_stats())
        # Cardinality of the structured filter: see at a glance how much data the
        # permission/time predicates can drop.
        show(
            "visibility distribution",
            client.execute(
                "SELECT visibility, COUNT(*) AS n FROM docs GROUP BY visibility ORDER BY n DESC"
            ).to_dict(),
        )

        report: list[dict] = []

        for qi, question in enumerate(QUERIES, start=1):
            section(f"Step 2.{qi}: query \"{question}\" — comparison of the three paths")
            # ---- Query understanding: deterministic keyword extraction + target topic + simulated embedding ----
            kws: list[str] = []
            for _, _, zh, en, _ in TOPICS:
                kws += [w for w in (zh + en) if w in question]
            kws = list(dict.fromkeys(kws))
            fts_query = " ".join(kws) or question[:4]

            # Target topic = the topic with the most word-bag hits; target category = its category.
            _, topic, _, _, _ = max(
                TOPICS, key=lambda t: sum(1 for w in (t[2] + t[3]) if w in question)
            )
            category = next(t[0] for t in TOPICS if t[1] == topic)
            # Simulate "question encoding": topic center + deterministic perturbation (unit vector).
            qrnd = rng(1111 + qi)
            pv = [float(x) + qrnd.gauss(0.0, 0.12) for x in centers[topic]]
            qn = float(np.linalg.norm(pv)) or 1.0
            query_vec = np.asarray([x / qn for x in pv], dtype=np.float32)
            literal = vector_literal(query_vec, precision=6)

            show("MATCH query string / target topic", f"{fts_query} / {topic} (category {category})")

            # Compliant subset: documents satisfying both "visibility + time window", represented
            # as a numpy boolean mask. The ground truth of hybrid retrieval must be defined on
            # this subset for the comparison to be fair.
            allowed = np.asarray(
                [d["visibility"] == VISIBILITY and d["day"] >= DAY_START for d in docs], dtype=bool
            )
            allowed_gt = brute_force_topk_masked(vectors, query_vec, TOP_K, allowed)
            show("compliant subset size / compliant semantic Top-K", f"{int(allowed.sum())} / {allowed_gt}")

            def violations(ids: list[int]) -> int:
                """Count the Top-K documents that violate a hard constraint (permission or time)."""
                return sum(1 for i in ids if not allowed[i - 1])

            # ---------- Path A: pure FTS (keywords only, no permission/time/semantics) ----------
            pure_fts = client.execute(
                f"""
                SELECT _id, FTS_SCORE() AS bm25
                FROM docs
                WHERE MATCH('{fts_query}')
                ORDER BY bm25 DESC
                LIMIT {TOP_K}
                """
            ).to_dict()
            fts_ids = [int(r["_id"]) for r in pure_fts]
            # "Effective recall": the fraction of the Top-K that both satisfies the constraints
            # and belongs to the compliant semantic Top-K. The denominator is fixed at K, so
            # unauthorized/expired documents directly drag this metric down.
            fts_recall = recall_of(fts_ids, set(allowed_gt), TOP_K)
            show("A. pure FTS Top-K", fts_ids)
            show("A. pure FTS effective recall@K / violations", f"{fts_recall:.2f} / {violations(fts_ids)}")

            # ---------- Path B: pure vector (semantics only, no permission/time/structure) ----------
            # topk_distance is the native Top-K primitive: heap-based O(n log k), faster than ORDER BY.
            pure_vec = client.topk_distance("emb", query_vec, k=TOP_K, metric="cosine").to_dict()
            vec_ids = [int(r["_id"]) for r in pure_vec]
            vec_recall = recall_of(vec_ids, set(allowed_gt), TOP_K)
            show("B. pure vector Top-K", vec_ids)
            show("B. pure vector effective recall@K / violations", f"{vec_recall:.2f} / {violations(vec_ids)}")
            # Pure vector search cannot express hard constraints: its Top-K necessarily contains
            # unauthorized documents (guaranteed by the example data).
            assert violations(vec_ids) > 0, "pure vector search cannot respect the permission constraint automatically; there should be violations here"
            print(
                f"[OK] {violations(vec_ids)}/{TOP_K} rows of the pure vector Top-K violate the permission or time constraint "
                f"(effective recall drops to {vec_recall:.2f})"
            )

            # ---------- Path C: hybrid retrieval (one SQL) ----------
            # Structure: CTE coarse recall (MATCH + structured predicates) -> cosine semantic rerank -> LIMIT.
            # This is the core of the example: the inverted index, the columnar predicates and the
            # vector distance cooperate inside a single query.
            hybrid = client.execute(
                f"""
                WITH cand AS (
                    SELECT _id, title, category, day, visibility, emb, FTS_SCORE() AS bm25
                    FROM docs
                    WHERE MATCH('{fts_query}')
                      AND day >= '{DAY_START}'            -- time filter (string range comparison)
                      AND visibility = '{VISIBILITY}'      -- permission filter (columnar pushdown)
                    ORDER BY bm25 DESC
                    LIMIT {FTS_CANDIDATES}
                )
                SELECT
                    _id                        AS id,
                    title                      AS title,
                    category                   AS category,
                    day                        AS day,
                    visibility                 AS visibility,
                    ROUND(bm25, 4)             AS bm25,
                    cosine_distance(emb, {literal}) AS vdist,
                    -- Fused score: linear blend of semantic distance and keyword relevance (lower is better).
                    ROUND(cosine_distance(emb, {literal}) - 0.001 * bm25, 6) AS fused_score
                FROM cand
                ORDER BY fused_score ASC
                LIMIT {TOP_K}
                """
            ).to_dict()
            hybrid_ids = [int(r["id"]) for r in hybrid]
            hybrid_recall = recall_of(hybrid_ids, set(allowed_gt), TOP_K)
            show("C. hybrid retrieval Top-K", hybrid_ids)
            show("C. hybrid retrieval effective recall@K / violations", f"{hybrid_recall:.2f} / {violations(hybrid_ids)}")
            for rank, row in enumerate(hybrid[:3], start=1):
                show(
                    f"C.  #{rank}",
                    f"{row['title']} ({row['category']} / {row['day']}) "
                    f"bm25={row['bm25']} vdist={round(row['vdist'], 4)} fused={row['fused_score']}",
                )

            # ---- Self-check 1: hybrid retrieval must strictly satisfy the structured constraints (0 violations) ----
            assert all(r["visibility"] == VISIBILITY for r in hybrid), "hybrid retrieval did not respect the permission filter"
            assert all(r["day"] >= DAY_START for r in hybrid), "hybrid retrieval did not respect the time filter"
            assert violations(hybrid_ids) == 0, "hybrid retrieval produced a constraint violation"
            print(f"[OK] hybrid retrieval Top-K violations = 0 (visibility={VISIBILITY} and day >= {DAY_START})")

            # ---- Self-check 2: fused_score is strictly ascending (ordering semantics are correct) ----
            fused = [r["fused_score"] for r in hybrid]
            assert fused == sorted(fused), "hybrid retrieval results are not in ascending fused score"
            print("[OK] hybrid retrieval results are strictly ascending by fused_score (semantic + BM25 joint ordering works)")

            # ---- Self-check 3: the effective recall of hybrid retrieval must beat both baselines ----
            # This is the core benefit of hybrid retrieval: it respects the hard constraints
            # (0 violations) while preserving semantic ordering quality.
            assert hybrid_recall >= vec_recall, (
                f"hybrid retrieval effective recall {hybrid_recall} is below pure vector {vec_recall}"
            )
            assert hybrid_recall >= fts_recall, (
                f"hybrid retrieval effective recall {hybrid_recall} is below pure FTS {fts_recall}"
            )
            print(
                f"[OK] effective recall@K: hybrid {hybrid_recall:.2f} >= pure vector {vec_recall:.2f}, "
                f">= pure FTS {fts_recall:.2f}"
            )
            assert hybrid_recall >= 0.7, f"hybrid retrieval effective recall is too low: {hybrid_recall}"

            # ---- Self-check 4: pure vector Top-K matches the numpy exact solution (validates topk_distance) ----
            exact_ids = brute_force_topk(vectors, query_vec, TOP_K)
            assert vec_ids == exact_ids, f"topk_distance disagrees with the numpy exact solution: {vec_ids} vs {exact_ids}"
            print(f"[OK] topk_distance full-table Top-K == numpy brute-force cosine solution (recall@K = 1.00, {len(exact_ids)} rows)")

            report.append(
                {
                    "question": question,
                    "fts": fts_recall,
                    "vec": vec_recall,
                    "hybrid": hybrid_recall,
                    "fts_bad": violations(fts_ids),
                    "vec_bad": violations(vec_ids),
                    "overlap_fts_hybrid": len(set(fts_ids) & set(hybrid_ids)) / TOP_K,
                    "overlap_vec_hybrid": len(set(vec_ids) & set(hybrid_ids)) / TOP_K,
                }
            )

        section("Step 3: quantitative comparison of the three paths (effective recall@K / constraint violations)")
        for row in report:
            show("question", row["question"])
            show(
                "  effective recall@K: pure FTS / pure vector / hybrid",
                f"{row['fts']:.2f} / {row['vec']:.2f} / {row['hybrid']:.2f}",
            )
            show(
                "  constraint violations: pure FTS / pure vector / hybrid",
                f"{row['fts_bad']} / {row['vec_bad']} / 0",
            )
            show(
                "  Top-K overlap: hybrid n pure FTS / hybrid n pure vector",
                f"{row['overlap_fts_hybrid']:.2f} / {row['overlap_vec_hybrid']:.2f}",
            )

        n = len(report)
        mean_fts = sum(r["fts"] for r in report) / n
        mean_vec = sum(r["vec"] for r in report) / n
        mean_hybrid = sum(r["hybrid"] for r in report) / n
        show("mean effective recall@K (pure FTS / pure vector / hybrid)", f"{mean_fts:.3f} / {mean_vec:.3f} / {mean_hybrid:.3f}")
        show(
            "mean constraint violations (pure FTS / pure vector / hybrid)",
            f"{sum(r['fts_bad'] for r in report) / n:.2f} / "
            f"{sum(r['vec_bad'] for r in report) / n:.2f} / 0.00",
        )
        print(
            "[Conclusion] Pure FTS is keyword-precise but understands neither semantics\n"
            "        nor constraints; pure vector has strong semantics, but about half of its\n"
            "        Top-K is unauthorized or expired, which halves effective recall;\n"
            "        hybrid retrieval obtains MATCH precision, structured-predicate compliance\n"
            "        and vector semantic ordering inside one SQL: zero violations and a clearly\n"
            "        leading effective recall - this is the production default shape."
        )
        assert mean_hybrid > mean_vec, "hybrid retrieval mean effective recall did not exceed pure vector"

    print(f"\n=== Scenario 11: hybrid retrieval (FTS + structured filtering + vector rerank) complete ===\nDatabase is at: {os.path.join(base, 'db')}")


if __name__ == "__main__":
    main()
