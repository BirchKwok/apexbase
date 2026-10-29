"""Bounded distance TopK: similarity-valued DESC and OFFSET must be exact.

`ORDER BY <distance>(col, q) LIMIT k` runs as a bounded heap over the vector
column. The heap returns the rows *nearest* in the metric's own sense, so a
similarity-valued function (`cosine_similarity`, `inner_product`) is ordered by
its largest value under `DESC`; an `OFFSET` is served by widening the heap to
`limit + offset` and letting the shared tail slice it. The opposite directions
(farthest rows) and the ordering of the returned values keep the general path.
Every shape below is checked against an independent NumPy ranking.
"""

import numpy as np
import pytest

from apexbase.client import ApexClient

ROWS = 2_000
DIM = 8
SEED = 20261101


@pytest.fixture(scope="module")
def vector_table(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("vector_order_offset")
    rng = np.random.default_rng(SEED)
    vecs = rng.standard_normal((ROWS, DIM)).astype(np.float32)
    k = rng.integers(0, 1_000, ROWS).astype(np.int64)
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"doc": "int64", "k": "int64", "emb": "float32_vector"})
    client.use_table("t")
    client.store(
        {
            "doc": list(range(ROWS)),
            "k": k.tolist(),
            "emb": [row.tolist() for row in vecs],
        }
    )
    client.flush()
    query = rng.standard_normal(DIM)
    yield client, vecs, query, k
    client.close()


def _rank(scores, k, offset=0):
    order = np.argsort(scores, kind="stable")[: offset + k]
    return [int(index) for index in order[offset:]]


def _cosine_similarity(vecs, query):
    values = vecs.astype(np.float64)
    query = query.astype(np.float64)
    return (values @ query) / (np.linalg.norm(values, axis=1) * np.linalg.norm(query))


def _l2(vecs, query):
    return np.linalg.norm(vecs.astype(np.float64) - query.astype(np.float64), axis=1)


def _inner(vecs, query):
    return vecs.astype(np.float64) @ query.astype(np.float64)


def _docs(client, sql):
    return [int(row["doc"]) for row in client.execute(sql).to_dict()]


def _literal(query):
    return "[" + ",".join(f"{float(value):.8f}" for value in query) + "]"


def test_similarity_desc_and_offset_match_brute_force(vector_table):
    client, vecs, query, _k = vector_table
    literal = _literal(query)

    similarity = _cosine_similarity(vecs, query)
    assert _docs(
        client,
        f"SELECT doc FROM t ORDER BY cosine_similarity(emb, {literal}) DESC LIMIT 10",
    ) == _rank(-similarity, 10)

    assert _docs(
        client,
        "SELECT doc FROM t ORDER BY cosine_similarity(emb, "
        f"{literal}) DESC LIMIT 10 OFFSET 3",
    ) == _rank(-similarity, 10, offset=3)

    # The ASC spelling wants the *least* similar rows and keeps the general
    # path; the lane must not quietly return the nearest ones.
    assert _docs(
        client,
        f"SELECT doc FROM t ORDER BY cosine_similarity(emb, {literal}) ASC LIMIT 5",
    ) == _rank(similarity, 5)

    inner = _inner(vecs, query)
    assert _docs(
        client,
        f"SELECT doc FROM t ORDER BY inner_product(emb, {literal}) "
        "DESC LIMIT 5 OFFSET 2",
    ) == _rank(-inner, 5, offset=2)


def test_distance_asc_offset_match_brute_force(vector_table):
    client, vecs, query, _k = vector_table
    literal = _literal(query)
    distances = _l2(vecs, query)
    assert _docs(
        client,
        f"SELECT doc FROM t ORDER BY l2_distance(emb, {literal}) "
        "ASC LIMIT 10 OFFSET 5",
    ) == _rank(distances, 10, offset=5)
    # The far end stays on the general path but must keep answering correctly.
    assert _docs(
        client,
        f"SELECT doc FROM t ORDER BY l2_distance(emb, {literal}) DESC LIMIT 5",
    ) == _rank(-distances, 5)


def test_filtered_similarity_desc_offset_matches_restricted_ranking(vector_table):
    client, vecs, query, k = vector_table
    literal = _literal(query)
    keep = k < 200
    expected_rows = np.flatnonzero(keep)
    expected = [
        int(index) for index in expected_rows[np.argsort(-_cosine_similarity(vecs[keep], query))]
    ]
    assert _docs(
        client,
        "SELECT doc FROM t WHERE k < 200 ORDER BY cosine_similarity(emb, "
        f"{literal}) DESC LIMIT 10 OFFSET 2",
    ) == expected[2:12]
