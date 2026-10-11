"""Coverage for the hybrid-retrieval guard phase.

The phase builds one table carrying a text column and a 64-dimension vector
column, then measures the BM25 branch, the exact vector branch and a
reciprocal-rank-fusion pipeline over their top-50 candidates. These tests pin the
report contract, the fixture's deterministic query/needle layout, and the
correctness assertions (BM25 membership, exhaustive vector recall, stable fusion)
that refuse to report a fast wrong answer.
"""

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def hybrid_bench():
    return _load(
        "bench_hybrid_retrieval", ROOT / "benchmarks" / "bench_hybrid_retrieval.py"
    )


def test_hybrid_manifest_is_well_formed(hybrid_bench):
    assert hybrid_bench.DIM == 64
    assert hybrid_bench.NEEDLE_EVERY > 0
    assert hybrid_bench.CANDIDATES > hybrid_bench.TOP_K
    assert hybrid_bench.RRF_K > 0
    # The fusion constant is the standard one; a silent change would alter the
    # fused ranking without any error.
    assert hybrid_bench.RRF_K == 60


def test_hybrid_rejects_invalid_sizes(hybrid_bench, tmp_path):
    for arguments in (("--rows", "0"), ("--warmup", "-1"), ("--iterations", "0")):
        with pytest.raises(SystemExit):
            hybrid_bench.main([*arguments, "--output", str(tmp_path / "out.json")])


def test_hybrid_fixture_is_deterministic(hybrid_bench):
    first = hybrid_bench.generate_documents(1_000)
    second = hybrid_bench.generate_documents(1_000)
    assert first["bodies"] == second["bodies"]
    assert (first["vecs"] == second["vecs"]).all()
    assert first["query_id"] == second["query_id"] == 501
    # The query vector is one of the documents, so its exact nearest neighbour is
    # known before the engine is involved.
    assert (first["query"] == first["vecs"][first["query_id"] - 1]).all()
    # Every NEEDLE_EVERY-th document carries the term.
    ready = [i + 1 for i in range(1_000) if i % hybrid_bench.NEEDLE_EVERY == 0]
    assert first["needle_ids"] == ready
    assert all(
        hybrid_bench.NEEDLE in first["bodies"][index - 1] for index in ready
    )


def test_hybrid_gates_the_pipeline_and_asserts_correctness(hybrid_bench):
    results, detail = hybrid_bench.run_hybrid_retrieval(rows=5_000, warmup=1, iterations=2)

    names = [row["query"] for row in results]
    assert len(names) == len(set(names)) == 12
    assert names[0].startswith("Hybrid: BM25")
    assert "Hybrid: vector top-50" in names[1]
    assert names[2].startswith("Hybrid: fused top-10")
    assert names[3] == "Hybrid: SQL MATCH row selection (needle)"
    assert names[4] == "Hybrid: SQL MATCH row selection + filter"
    assert names[5] == "Hybrid: SQL MATCH row selection (absent term)"
    assert names[6] == "Hybrid: SQL MATCH row selection (dense term, page)"
    assert names[7] == "Hybrid: SQL ORDER BY cosine_distance LIMIT 10"
    assert names[8] == "Hybrid: SQL ORDER BY distance alias LIMIT 10"
    assert names[9] == "Hybrid: SQL MATCH + ORDER BY distance LIMIT 10"
    assert names[10] == "Hybrid: SQL filter + ORDER BY distance LIMIT 10"
    assert names[11] == "Hybrid: SQL MATCH row selection (vector table)"
    for row in results:
        assert row["category"] == "ApexBase hybrid retrieval"
        assert row["ApexBase"] > 0

    # The phase asserts recall internally; the report has to carry the number.
    assert detail["recall_at_10"] == 1.0
    # 5_000 rows / NEEDLE_EVERY = 50 needle documents, all of them returned.
    assert detail["bm25_hits"] == 5_000 // hybrid_bench.NEEDLE_EVERY == 50
    assert detail["rows"] == 5_000
    assert detail["dim"] == hybrid_bench.DIM
    # The SQL `MATCH()` selection returns exactly the needle documents, and the
    # filtered variant keeps the subset with k < 500.
    assert detail["sql_match"]["needle_docs"] == 50
    assert detail["sql_match"]["selected_docs"] == 50
    assert 0 < detail["sql_match"]["filtered_docs"] < 50
    # The distance-ordered shapes returned the exact cosine TopK.
    assert detail["sql_order_by_distance"] == {"top_k": hybrid_bench.TOP_K, "docs_match": True}
    # The filter-aware distance lane answered with the exact TopK of the subset
    # the predicate selected, for both candidate sources.
    assert detail["sql_filtered_distance"] == {
        "fts_docs": 5_000 // hybrid_bench.NEEDLE_EVERY,
        "range_docs": 5_000 // 10,
        "docs_match": True,
    }
    # The vector-bearing table pushes the same bitmap down through the position
    # reader, and every selected vector has to survive the round trip.
    assert detail["sql_match_vector_table"] == {
        "docs": 5_000 // hybrid_bench.NEEDLE_EVERY,
        "vectors_match": True,
    }
    assert len(detail["shapes"]) == 12


def test_hybrid_rrf_fusion_is_deterministic(hybrid_bench):
    fused = hybrid_bench.rrf_fuse([[3, 1, 2], [2, 3, 4]])
    # id 3 and 2 appear in both branches, so they outrank the single-branch ids
    # (3 leads both by rank, so it takes the top slot).
    assert fused[:2] == [3, 2]
    assert set(fused) == {1, 2, 3, 4}
    assert fused == hybrid_bench.rrf_fuse([[3, 1, 2], [2, 3, 4]])


def test_hybrid_report_round_trips_through_main(hybrid_bench, tmp_path):
    output = tmp_path / "hybrid.json"
    hybrid_bench.main(["--rows", "1000", "--warmup", "1", "--iterations", "1",
                       "--output", str(output)])
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["results"]
    assert report["hybrid_retrieval"]["recall_at_10"] == 1.0
    assert report["config"]["rows"] == 1000


def test_hybrid_brute_force_ranking_matches_numpy_order(hybrid_bench):
    import numpy as np

    data = hybrid_bench.generate_documents(500)
    top = hybrid_bench.brute_force_top_k(data["vecs"], data["query"], 10)
    assert top[0] == data["query_id"]
    distances = np.linalg.norm(data["vecs"] - data["query"], axis=1)
    assert distances[top[0] - 1] == 0.0
    # `top` holds 1-based `_id`s; the matrix is 0-based.
    ranked = [float(distances[doc_id - 1]) for doc_id in top]
    assert ranked == sorted(ranked)


def test_hybrid_cosine_top_k_matches_numpy(hybrid_bench):
    import numpy as np

    data = hybrid_bench.generate_documents(500)
    docs, distances = hybrid_bench.cosine_top_k(data["vecs"], data["query"], 10)
    values = data["vecs"].astype(np.float64)
    query = data["query"].astype(np.float64)
    truth = 1.0 - (values @ query) / (
        np.linalg.norm(values, axis=1) * np.linalg.norm(query)
    )
    # The query vector is one of the rows, so its own document is the exact
    # nearest neighbour and the reference distances are monotone.
    assert docs[0] == data["query_id"] - 1
    assert distances == sorted(distances)
    assert all(abs(distances[i] - truth[docs[i]]) < 1e-9 for i in range(10))
