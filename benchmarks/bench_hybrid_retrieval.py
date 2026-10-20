"""Mixed retrieval coverage: BM25, exact vector TopK and a fused pipeline.

Mixed retrieval is one of the four V5 goals and the guard had no hybrid row at
all: full-text and vector search were measured (if at all) in isolation, never the
pipeline that fuses them. This phase builds one table carrying both a text column
and a 64-dimension vector column, then measures the BM25 branch, the exact vector
branch and a reciprocal-rank-fusion pipeline over their top-50 candidates.

It also builds a scalar twin of the corpus (the same bodies without the vector
column) and measures the SQL ``MATCH()`` shapes on it: a ``MATCH()`` resolves to a
row-id bitmap, so the executor can restrict the scan to the documents it selects
instead of filtering every row it materialized (V5 §6.4 item 15). The twin keeps
the per-id reader on a schema it can stage; the parity assertion compares the
bitmap-restricted result with the scan-only shape on the same data.

Correctness is asserted rather than assumed: every BM25 hit must really contain
the query term, the vector branch's top-10 must match an independent brute-force
ranking over the generated matrix (the engine's TopK is exact, so recall@10 has to
be 1.0), and the SQL ``MATCH()`` selection must equal both the scan-only shape and
the independently computed needle document set. It finally measures the
filter-aware distance lane: a ``WHERE`` clause over the vector table has to
restrict the candidate set, and both the ``MATCH()`` bitmap and a numeric range
answer with the exact cosine TopK of that subset. Recall, hit counts and the
per-branch latencies stay in the informational block; the pipeline, ``MATCH()``
and filtered-distance latencies are the gate rows.
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np

import bench_vs_sqlite_duckdb as full_bench
from apexbase.client import ApexClient

DIM = 64
VOCAB = 500
# Every NEEDLE_EVERY-th document carries the term the BM25 branch searches for.
NEEDLE = "needleterm"
NEEDLE_EVERY = 100
# A deliberately *dense* term: a paginated `MATCH()` used to stage every match, so
# its cost grew with the term's document frequency instead of the page (200k
# documents, 10-row page: 25k hits 0.88 ms, 50k hits 1.77 ms).
DENSE_TERM = "densecommon"
DENSE_EVERY = 4
CANDIDATES = 50
TOP_K = 10
# Reciprocal-rank-fusion constant (the standard 60 from the original paper).
RRF_K = 60
VECTOR_STORE_BATCH = 10_000


def generate_documents(rows):
    """Deterministic documents plus the query vector and its id."""
    rng = np.random.default_rng(20260827)
    bodies = []
    for i in range(rows):
        terms = [f"w{term}" for term in rng.integers(0, VOCAB, size=6)]
        if i % NEEDLE_EVERY == 0:
            terms.insert(0, NEEDLE)
        if i % DENSE_EVERY == 0:
            terms.insert(0, DENSE_TERM)
        bodies.append(" ".join(terms))
    vecs = rng.random((rows, DIM), dtype=np.float32)
    query_id = rows // 2 + 1  # internal `_id`s start at 1
    # An exact hit: the query vector is the row's own vector, so the nearest
    # neighbour is known and the distance is exactly 0.
    query = vecs[query_id - 1].copy()
    return {
        "bodies": bodies,
        "vecs": vecs,
        "query_id": query_id,
        "query": query,
        "needle_ids": [i + 1 for i in range(rows) if i % NEEDLE_EVERY == 0],
        "dense_docs": [i for i in range(rows) if i % DENSE_EVERY == 0],
    }


def brute_force_top_k(vecs, query, k):
    """Exact L2 ranking straight from the generated matrix."""
    distances = np.linalg.norm(vecs - query, axis=1)
    order = np.argsort(distances, kind="stable")[:k]
    return [int(index) + 1 for index in order]


def cosine_top_k(vecs, query, k):
    """Cosine-distance ranking and the distances themselves, in f64.

    Returns `(docs, distances)` where `docs` are 0-based row indices (the same
    numbering as the `doc` column of the fixture).
    """
    values = vecs.astype(np.float64)
    query = query.astype(np.float64)
    distances = 1.0 - (values @ query) / (
        np.linalg.norm(values, axis=1) * np.linalg.norm(query)
    )
    order = np.argsort(distances, kind="stable")[:k]
    return [int(index) for index in order], [float(distances[index]) for index in order]


def rrf_fuse(branch_lists, top_k=TOP_K, rrf_k=RRF_K):
    """Reciprocal-rank fusion over ranked id lists (deterministic tie-break)."""
    scores = {}
    for branch in branch_lists:
        for rank, doc_id in enumerate(branch, start=1):
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (rrf_k + rank)
    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    return [doc_id for doc_id, _score in ranked[:top_k]]


def hybrid_metric_names(rows):
    """Every reported metric name, in report order."""
    labels = [
        "BM25 top-50 (needle)",
        f"vector top-50 (exact, {rows} x {DIM})",
        "fused top-10 (RRF 50+50)",
        "SQL MATCH row selection (needle)",
        "SQL MATCH row selection + filter",
        "SQL MATCH row selection (absent term)",
        "SQL MATCH row selection (dense term, page)",
        "SQL ORDER BY cosine_distance LIMIT 10",
        "SQL ORDER BY distance alias LIMIT 10",
        "SQL MATCH + ORDER BY distance LIMIT 10",
        "SQL filter + ORDER BY distance LIMIT 10",
        "SQL MATCH row selection (vector table)",
    ]
    return [f"Hybrid: {label}" for label in labels]


def run_hybrid_retrieval(rows, warmup, iterations, only=None):
    wanted = set(only) if only else None

    def want(metric):
        return wanted is None or metric in wanted

    full_bench.ensure_optional_imports()
    if not full_bench.HAS_APEXBASE:
        raise RuntimeError("ApexBase is not importable; run maturin develop --release first")

    data = generate_documents(rows)
    vecs = data["vecs"]
    query = data["query"]
    query_id = data["query_id"]
    needle_ids = set(data["needle_ids"])

    tmpdir = Path(tempfile.mkdtemp(prefix="apexbase_hybrid_"))
    db_dir = tmpdir / "db"
    results = []
    detail = {
        "rows": rows,
        "dim": DIM,
        "needle": NEEDLE,
        "needle_rows": len(needle_ids),
        "candidates": CANDIDATES,
        "top_k": TOP_K,
        "rrf_k": RRF_K,
        "shapes": {},
    }
    try:
        db_dir.mkdir(parents=True, exist_ok=True)
        client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)
        client.create_table("docs", {"doc": "int64", "body": "string", "emb": "float32_vector"})
        client.use_table("docs")
        for start in range(0, rows, VECTOR_STORE_BATCH):
            end = min(start + VECTOR_STORE_BATCH, rows)
            client.store(
                [
                    {"doc": i, "body": data["bodies"][i], "emb": vecs[i]}
                    for i in range(start, end)
                ]
            )
        client.flush()
        client.init_fts(index_fields=["body"])

        def bm25():
            return [doc_id for doc_id, _score in client.search_text_with_scores(
                NEEDLE, limit=CANDIDATES
            )]

        def vector():
            return [
                int(row["_id"])
                for row in client.topk_distance(
                    "emb", query, k=CANDIDATES, metric="l2"
                ).to_dict()
            ]

        def fused():
            return rrf_fuse([bm25(), vector()])

        # --- correctness first: a fast wrong answer must not reach the report ---
        bm25_hits = bm25()
        assert len(bm25_hits) == min(CANDIDATES, len(needle_ids)), (
            len(bm25_hits),
            len(needle_ids),
        )
        assert set(bm25_hits) <= needle_ids, "BM25 returned a document without the term"

        vector_hits = vector()
        expected_top = brute_force_top_k(vecs, query, CANDIDATES)
        assert vector_hits == expected_top, "vector TopK diverged from the exact ranking"
        assert vector_hits[0] == query_id, "the query vector's own row must be nearest"

        fused_hits = fused()
        assert fused_hits == rrf_fuse([bm25_hits, vector_hits]), "fusion is not stable"
        assert set(fused_hits) <= set(bm25_hits) | set(vector_hits), "fusion invented an id"
        assert fused_hits[0] in (bm25_hits[0], vector_hits[0]), "fusion lost both branch leaders"

        exact_top10 = brute_force_top_k(vecs, query, TOP_K)
        recall_at_10 = len(set(vector_hits[:TOP_K]) & set(exact_top10)) / TOP_K
        assert recall_at_10 == 1.0, recall_at_10
        detail["recall_at_10"] = recall_at_10
        detail["bm25_hits"] = len(bm25_hits)
        detail["candidate_overlap"] = len(set(bm25_hits) & set(vector_hits))

        # --- timed shapes ---
        shapes = (
            ("BM25 top-50 (needle)", bm25),
            ("vector top-50 (exact, {rows} x {dim})", vector),
            ("fused top-10 (RRF 50+50)", fused),
        )
        for name, call in shapes:
            label = name.format(rows=rows, dim=DIM)
            metric = f"Hybrid: {label}"
            if not want(metric):
                continue
            for _ in range(warmup):
                call()
            samples = []
            for _ in range(iterations):
                start = time.perf_counter()
                call()
                samples.append((time.perf_counter() - start) * 1000.0)
            warm_ms = statistics.median(samples)
            results.append(
                {
                    "category": "ApexBase hybrid retrieval",
                    "query": f"Hybrid: {label}",
                    "ApexBase": round(warm_ms, 6),
                }
            )
            detail["shapes"][label] = {"warm_ms": round(warm_ms, 6)}

        # --- SQL MATCH() row-selection pushdown on the scalar twin ---
        # The vector-bearing table cannot use the per-id reader, so the same
        # corpus is stored without `emb` to measure the bitmap-restricted scan.
        client.create_table(
            "docs_text", {"doc": "int64", "body": "string", "k": "int64"}
        )
        client.use_table("docs_text")
        for start in range(0, rows, VECTOR_STORE_BATCH):
            end = min(start + VECTOR_STORE_BATCH, rows)
            client.store(
                {
                    "doc": list(range(start, end)),
                    "body": data["bodies"][start:end],
                    "k": [i % 1000 for i in range(start, end)],
                }
            )
        client.flush()
        client.init_fts(index_fields=["body"])

        needle_docs = {doc_id - 1 for doc_id in needle_ids}
        match_sql = f"SELECT doc, k FROM docs_text WHERE MATCH('{NEEDLE}')"
        filtered_sql = match_sql + " AND k < 500"
        selected = {int(row["doc"]) for row in client.execute(match_sql).to_dict()}
        assert selected == needle_docs, (len(selected), len(needle_docs))
        # `OR 1 = 0` returns the same rows through the plain scan, so it is the
        # baseline that proves the row selection did not change the answer.
        scanned = {
            int(row["doc"])
            for row in client.execute(match_sql + " OR 1 = 0").to_dict()
        }
        assert scanned == selected, "the SQL MATCH row selection diverged from the scan"
        expected_filtered = {doc for doc in needle_docs if doc % 1000 < 500}
        filtered = {int(row["doc"]) for row in client.execute(filtered_sql).to_dict()}
        assert filtered == expected_filtered, (len(filtered), len(expected_filtered))
        detail["sql_match"] = {
            "needle_docs": len(needle_docs),
            "selected_docs": len(selected),
            "filtered_docs": len(filtered),
        }
        # An absent term must not cost more than a term that matches: the empty
        # bitmap used to decline to the scan just to borrow its schema, which made
        # *not* finding the term the slow case (200k documents: 1.24 ms against
        # 0.48 ms for a 25k-hit term). The row-selection lane now answers an empty
        # bitmap with the same columns and no rows.
        absent_sql = "SELECT doc, k FROM docs_text WHERE MATCH('nosuchtermxyz')"
        assert client.execute(absent_sql).to_dict() == []
        # A dense term behind a small page: the lane must stage the page, not the
        # term's whole posting list.
        dense_sql = f"SELECT doc, k FROM docs_text WHERE MATCH('{DENSE_TERM}') LIMIT 10"
        dense_docs = data["dense_docs"]
        assert len(dense_docs) > 100, "the dense term must cover a real share of the table"
        dense_page = [int(row["doc"]) for row in client.execute(dense_sql).to_dict()]
        assert dense_page == dense_docs[:10], (dense_page[:3], dense_docs[:3])
        detail["dense_term"] = {"docs": len(dense_docs), "page": len(dense_page)}
        for label, sql in (
            ("SQL MATCH row selection (needle)", match_sql),
            ("SQL MATCH row selection + filter", filtered_sql),
            ("SQL MATCH row selection (absent term)", absent_sql),
            ("SQL MATCH row selection (dense term, page)", dense_sql),
        ):
            metric = f"Hybrid: {label}"
            if not want(metric):
                continue
            for _ in range(warmup):
                client.execute(sql)
            samples = []
            for _ in range(iterations):
                start = time.perf_counter()
                client.execute(sql)
                samples.append((time.perf_counter() - start) * 1000.0)
            warm_ms = statistics.median(samples)
            results.append(
                {
                    "category": "ApexBase hybrid retrieval",
                    "query": f"Hybrid: {label}",
                    "ApexBase": round(warm_ms, 6),
                }
            )
            detail["shapes"][label] = {"warm_ms": round(warm_ms, 6)}

        # --- fused distance TopK: ORDER BY <distance>(emb, [q]) LIMIT k ---
        # This is the shape a hybrid pipeline writes when it does not want the
        # dedicated topk_distance API. The fast path runs the allocation-free
        # distance + bounded heap; the base build evaluates a distance per row
        # and fully sorts, so the metric shows the fused lane directly.
        client.use_table("docs")
        expected_docs, expected_distance = cosine_top_k(vecs, query, TOP_K)
        # A tie at the cut would make the expected order ambiguous; the fixture is
        # random, so the margin only guards a future fixture change.
        _, full_distance = cosine_top_k(vecs, query, len(vecs))
        assert full_distance[TOP_K] - full_distance[TOP_K - 1] > 1e-6, (
            "the fixture must separate the k-th and (k+1)-th neighbour"
        )
        literal = "[" + ",".join(f"{float(value):.6f}" for value in query) + "]"
        ordered_sql = (
            f"SELECT doc, cosine_distance(emb, {literal}) AS dist "
            f"FROM docs ORDER BY dist LIMIT {TOP_K}"
        )
        plain_ordered_sql = (
            f"SELECT doc FROM docs ORDER BY cosine_distance(emb, {literal}) "
            f"LIMIT {TOP_K}"
        )
        ordered = client.execute(ordered_sql).to_dict()
        assert [int(row["doc"]) for row in ordered] == expected_docs, (
            len(ordered),
            expected_docs,
        )
        assert all(
            abs(float(row["dist"]) - want) < 1e-5
            for row, want in zip(ordered, expected_distance)
        ), "the reported distances must match the exact computation"
        assert [float(row["dist"]) for row in ordered] == sorted(
            float(row["dist"]) for row in ordered
        ), "the rows must come back nearest-first"
        assert [
            int(row["doc"]) for row in client.execute(plain_ordered_sql).to_dict()
        ] == expected_docs
        detail["sql_order_by_distance"] = {"top_k": TOP_K, "docs_match": True}
        for label, sql in (
            ("SQL ORDER BY cosine_distance LIMIT 10", plain_ordered_sql),
            ("SQL ORDER BY distance alias LIMIT 10", ordered_sql),
        ):
            metric = f"Hybrid: {label}"
            if not want(metric):
                continue
            for _ in range(warmup):
                client.execute(sql)
            samples = []
            for _ in range(iterations):
                start = time.perf_counter()
                client.execute(sql)
                samples.append((time.perf_counter() - start) * 1000.0)
            warm_ms = statistics.median(samples)
            results.append(
                {
                    "category": "ApexBase hybrid retrieval",
                    "query": f"Hybrid: {label}",
                    "ApexBase": round(warm_ms, 6),
                }
            )
            detail["shapes"][label] = {"warm_ms": round(warm_ms, 6)}

        # --- filter-aware distance TopK: the WHERE clause restricts the scan ---
        # A selective predicate resolves to a row-id set, so the distance scan can
        # visit only the matching rows instead of materialising the whole column
        # and discarding the non-matches afterwards (V5 §6.4 item 16). The FTS
        # bitmap and a numeric range exercise the two candidate sources; both are
        # checked against an independent NumPy ranking over the restricted subset.
        needle_zero = [i for i in range(rows) if i % NEEDLE_EVERY == 0]
        needle_rank, _ = cosine_top_k(vecs[needle_zero], query, TOP_K)
        needle_expected = [needle_zero[index] for index in needle_rank]

        range_lo, range_hi = rows // 20, rows // 20 + rows // 10
        range_rank, _ = cosine_top_k(vecs[range_lo:range_hi], query, TOP_K)
        range_expected = [int(index) + range_lo for index in range_rank]

        needle_distance_sql = (
            f"SELECT doc FROM docs WHERE MATCH('{NEEDLE}') "
            f"ORDER BY cosine_distance(emb, {literal}) LIMIT {TOP_K}"
        )
        range_distance_sql = (
            f"SELECT doc FROM docs WHERE doc >= {range_lo} AND doc < {range_hi} "
            f"ORDER BY cosine_distance(emb, {literal}) LIMIT {TOP_K}"
        )
        needle_distance = [
            int(row["doc"]) for row in client.execute(needle_distance_sql).to_dict()
        ]
        assert needle_distance == needle_expected, (needle_distance, needle_expected)
        range_distance = [
            int(row["doc"]) for row in client.execute(range_distance_sql).to_dict()
        ]
        assert range_distance == range_expected, (range_distance, range_expected)
        detail["sql_filtered_distance"] = {
            "fts_docs": len(needle_zero),
            "range_docs": range_hi - range_lo,
            "docs_match": True,
        }
        for label, sql in (
            ("SQL MATCH + ORDER BY distance LIMIT 10", needle_distance_sql),
            ("SQL filter + ORDER BY distance LIMIT 10", range_distance_sql),
        ):
            metric = f"Hybrid: {label}"
            if not want(metric):
                continue
            for _ in range(warmup):
                client.execute(sql)
            samples = []
            for _ in range(iterations):
                start = time.perf_counter()
                client.execute(sql)
                samples.append((time.perf_counter() - start) * 1000.0)
            warm_ms = statistics.median(samples)
            results.append(
                {
                    "category": "ApexBase hybrid retrieval",
                    "query": f"Hybrid: {label}",
                    "ApexBase": round(warm_ms, 6),
                }
            )
            detail["shapes"][label] = {"warm_ms": round(warm_ms, 6)}

        # --- the same MATCH() pushdown on the vector-bearing table ---
        # The scalar per-id reader cannot stage `emb`, so this shape used to keep
        # the full scan even though the bitmap was selective. The position reader
        # decodes only the requested columns, so the pushdown now covers vector
        # tables and every selected vector must come back exactly.
        vector_match_sql = f"SELECT doc, emb FROM docs WHERE MATCH('{NEEDLE}')"
        vector_match = client.execute(vector_match_sql).to_dict()
        assert [int(row["doc"]) for row in vector_match] == needle_zero, len(vector_match)
        for row in vector_match:
            assert np.allclose(
                np.asarray(row["emb"], dtype=np.float32), vecs[int(row["doc"])]
            ), row["doc"]
        detail["sql_match_vector_table"] = {
            "docs": len(vector_match),
            "vectors_match": True,
        }
        for label, sql in (
            ("SQL MATCH row selection (vector table)", vector_match_sql),
        ):
            metric = f"Hybrid: {label}"
            if not want(metric):
                continue
            for _ in range(warmup):
                client.execute(sql)
            samples = []
            for _ in range(iterations):
                start = time.perf_counter()
                client.execute(sql)
                samples.append((time.perf_counter() - start) * 1000.0)
            warm_ms = statistics.median(samples)
            results.append(
                {
                    "category": "ApexBase hybrid retrieval",
                    "query": f"Hybrid: {label}",
                    "ApexBase": round(warm_ms, 6),
                }
            )
            detail["shapes"][label] = {"warm_ms": round(warm_ms, 6)}
        client.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    return results, detail


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=7)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        metavar="METRIC",
        help="Measure only the named metric(s); used by the guard to recheck a "
        "marked metric in a fresh process",
    )
    args = parser.parse_args(argv)
    if args.rows <= 0 or args.warmup < 0 or args.iterations <= 0:
        parser.error("rows and iterations must be positive; warmup must be non-negative")
    known = set(hybrid_metric_names(args.rows))
    unknown = [name for name in args.only if name not in known]
    if unknown:
        parser.error("--only metric(s) not reported by this script: " + ", ".join(unknown))

    results, detail = run_hybrid_retrieval(
        args.rows, args.warmup, args.iterations, only=args.only
    )
    report = {
        **full_bench.get_report_metadata("apexbase-canary"),
        "config": {
            "rows": args.rows,
            "warmup": args.warmup,
            "iterations": args.iterations,
        },
        "results": results,
        "hybrid_retrieval": detail,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    raise SystemExit(main())
