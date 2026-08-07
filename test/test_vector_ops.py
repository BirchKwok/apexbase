"""
Tests for vector distance functions.

Covers:
- encode_vector / decode_vector round-trip
- array_distance (L2), cosine_similarity, cosine_distance,
  inner_product, l1_distance, linf_distance
- ORDER BY array_distance(...) LIMIT k  (TopK)
- ORDER BY array_distance(col, [...])   (expression ORDER BY without alias)
- Auto-encoding of list/numpy vector columns in store()
- vector_dim, vector_norm, vector_to_string utilities
"""

import math
import struct
import numpy as np
import pytest

from apexbase.client import ApexClient, encode_vector, decode_vector


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def client(tmp_path):
    c = ApexClient(dirpath=str(tmp_path), drop_if_exists=True)
    c.create_table("vecs")
    yield c
    c.close()


# ─────────────────────────────────────────────────────────────────────────────
# encode / decode helpers
# ─────────────────────────────────────────────────────────────────────────────

def test_encode_decode_list():
    v = [1.0, 2.0, 3.0]
    assert decode_vector(encode_vector(v)) == pytest.approx(v, abs=1e-6)


def test_encode_decode_numpy():
    v = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    decoded = decode_vector(encode_vector(v))
    assert decoded == pytest.approx(v.tolist(), abs=1e-6)


def test_encode_decode_empty():
    assert decode_vector(encode_vector([])) == []


# ─────────────────────────────────────────────────────────────────────────────
# Auto-encoding in store()
# ─────────────────────────────────────────────────────────────────────────────

def test_auto_encode_list_vectors(client):
    rows = [
        {"name": "a", "vec": [1.0, 0.0, 0.0]},
        {"name": "b", "vec": [0.0, 1.0, 0.0]},
        {"name": "c", "vec": [0.0, 0.0, 1.0]},
    ]
    client.store(rows)
    result = client.execute("SELECT name FROM vecs ORDER BY name").to_dict()
    assert [r["name"] for r in result] == ["a", "b", "c"]


def test_auto_encode_numpy_vectors(client):
    rows = [
        {"name": "x", "vec": np.array([1.0, 2.0, 3.0], dtype=np.float32)},
        {"name": "y", "vec": np.array([4.0, 5.0, 6.0], dtype=np.float32)},
    ]
    client.store(rows)
    result = client.execute("SELECT COUNT(*) FROM vecs").to_dict()
    assert result[0]["COUNT(*)"] == 2


def test_topk_ffi_preserves_tiny_query_components(client):
    client.store([
        {"name": "tiny", "vec": [1.0e-12, 1.0]},
        {"name": "other", "vec": [1.0, 0.0]},
    ])
    rows = client.topk_distance(
        "vec", np.array([1.0e-12, 1.0], dtype=np.float64), k=1,
        metric="l2", id_col="row_id", dist_col="distance",
    ).to_dict()
    assert list(rows[0]) == ["row_id", "distance"]
    assert rows[0]["distance"] == pytest.approx(0.0, abs=1e-12)


def test_topk_sql_array_accepts_scientific_notation(client):
    client.store([
        {"name": "tiny", "vec": [1.0e-12, 1.0]},
        {"name": "other", "vec": [1.0, 0.0]},
    ])
    rows = client.execute(
        "SELECT explode_rename("
        "topk_distance(vec, [1e-12, 1E0], 1, 'l2'), '_id', 'dist'"
        ") FROM vecs"
    ).to_dict()
    assert rows[0]["dist"] == pytest.approx(0.0, abs=1e-6)


def test_cosine_alias_matches_distance_and_similarity_orders_nearest(client):
    client.store([
        {"name": "same", "vec": [1.0, 0.0]},
        {"name": "orthogonal", "vec": [0.0, 1.0]},
        {"name": "opposite", "vec": [-1.0, 0.0]},
    ])
    query = np.array([1.0, 0.0], dtype=np.float32)
    cosine = client.topk_distance("vec", query, k=3, metric="cosine").to_dict()
    distance = client.topk_distance(
        "vec", query, k=3, metric="cosine_distance"
    ).to_dict()
    similarity = client.topk_distance(
        "vec", query, k=3, metric="cosine_similarity"
    ).to_dict()

    assert [row["_id"] for row in cosine] == [row["_id"] for row in distance]
    assert cosine[0]["dist"] == pytest.approx(0.0, abs=1e-5)
    assert similarity[0]["_id"] == cosine[0]["_id"]




# ─────────────────────────────────────────────────────────────────────────────
# Distance functions
# ─────────────────────────────────────────────────────────────────────────────

def _load_3d_vecs(client):
    rows = [
        {"name": "origin",  "vec": [0.0, 0.0, 0.0]},
        {"name": "unit_x",  "vec": [1.0, 0.0, 0.0]},
        {"name": "unit_y",  "vec": [0.0, 1.0, 0.0]},
        {"name": "unit_z",  "vec": [0.0, 0.0, 1.0]},
        {"name": "diag",    "vec": [1.0, 1.0, 1.0]},
    ]
    client.store(rows)


def test_l2_distance(client):
    _load_3d_vecs(client)
    rows = client.execute(
        "SELECT name, array_distance(vec, [1.0, 0.0, 0.0]) AS dist FROM vecs "
        "ORDER BY name"
    ).to_dict()
    dist = {r["name"]: r["dist"] for r in rows}
    assert dist["unit_x"] == pytest.approx(0.0, abs=1e-5)
    assert dist["origin"] == pytest.approx(1.0, abs=1e-5)
    assert dist["unit_y"] == pytest.approx(math.sqrt(2), abs=1e-5)


def test_l2_distance_alias(client):
    _load_3d_vecs(client)
    rows = client.execute(
        "SELECT name, l2_distance(vec, [1.0, 0.0, 0.0]) AS dist FROM vecs "
        "ORDER BY name"
    ).to_dict()
    dist = {r["name"]: r["dist"] for r in rows}
    assert dist["unit_x"] == pytest.approx(0.0, abs=1e-5)


def test_cosine_similarity(client):
    _load_3d_vecs(client)
    rows = client.execute(
        "SELECT name, cosine_similarity(vec, [1.0, 0.0, 0.0]) AS sim FROM vecs "
        "ORDER BY name"
    ).to_dict()
    sim = {r["name"]: r["sim"] for r in rows}
    assert sim["unit_x"] == pytest.approx(1.0, abs=1e-5)
    assert sim["unit_y"] == pytest.approx(0.0, abs=1e-5)
    assert sim["diag"]   == pytest.approx(1.0 / math.sqrt(3), abs=1e-5)


def test_cosine_distance(client):
    _load_3d_vecs(client)
    rows = client.execute(
        "SELECT name, cosine_distance(vec, [1.0, 0.0, 0.0]) AS d FROM vecs "
        "ORDER BY name"
    ).to_dict()
    d = {r["name"]: r["d"] for r in rows}
    assert d["unit_x"] == pytest.approx(0.0, abs=1e-5)
    assert d["unit_y"] == pytest.approx(1.0, abs=1e-5)


def test_inner_product(client):
    _load_3d_vecs(client)
    rows = client.execute(
        "SELECT name, inner_product(vec, [1.0, 2.0, 3.0]) AS ip FROM vecs "
        "ORDER BY name"
    ).to_dict()
    ip = {r["name"]: r["ip"] for r in rows}
    assert ip["origin"] == pytest.approx(0.0, abs=1e-5)
    assert ip["unit_x"] == pytest.approx(1.0, abs=1e-5)
    assert ip["unit_y"] == pytest.approx(2.0, abs=1e-5)
    assert ip["unit_z"] == pytest.approx(3.0, abs=1e-5)
    assert ip["diag"]   == pytest.approx(6.0, abs=1e-5)


def test_l1_distance(client):
    _load_3d_vecs(client)
    rows = client.execute(
        "SELECT name, l1_distance(vec, [1.0, 0.0, 0.0]) AS d FROM vecs "
        "ORDER BY name"
    ).to_dict()
    d = {r["name"]: r["d"] for r in rows}
    assert d["unit_x"] == pytest.approx(0.0, abs=1e-5)
    assert d["origin"] == pytest.approx(1.0, abs=1e-5)
    assert d["diag"]   == pytest.approx(2.0, abs=1e-5)   # |0|+|1|+|1|


def test_linf_distance(client):
    _load_3d_vecs(client)
    rows = client.execute(
        "SELECT name, linf_distance(vec, [0.5, 0.5, 0.5]) AS d FROM vecs "
        "ORDER BY name"
    ).to_dict()
    d = {r["name"]: r["d"] for r in rows}
    assert d["origin"] == pytest.approx(0.5, abs=1e-5)
    assert d["unit_x"] == pytest.approx(0.5, abs=1e-5)
    assert d["diag"]   == pytest.approx(0.5, abs=1e-5)


# ─────────────────────────────────────────────────────────────────────────────
# TopK queries
# ─────────────────────────────────────────────────────────────────────────────

def _load_many(client, n=100, dim=8):
    rng = np.random.default_rng(42)
    rows = [
        {"id": i, "vec": (rng.random(dim).astype(np.float32)).tolist()}
        for i in range(n)
    ]
    client.store(rows)
    return rows


def test_topk_l2_with_alias(client):
    """ORDER BY aliased distance column LIMIT k."""
    rows = _load_many(client, n=50, dim=4)
    query = [0.5, 0.5, 0.5, 0.5]

    result = client.execute(
        f"SELECT id, array_distance(vec, [{','.join(map(str, query))}]) AS dist "
        "FROM vecs ORDER BY dist LIMIT 5"
    ).to_dict()

    assert len(result) == 5
    # Verify ordering
    dists = [r["dist"] for r in result]
    assert dists == sorted(dists)


def test_topk_l2_expression_orderby(client):
    """ORDER BY array_distance(col, [...]) LIMIT k — no alias needed."""
    rows = _load_many(client, n=50, dim=4)
    query = [0.5, 0.5, 0.5, 0.5]

    result = client.execute(
        f"SELECT id FROM vecs "
        f"ORDER BY array_distance(vec, [{','.join(map(str, query))}]) LIMIT 5"
    ).to_dict()

    assert len(result) == 5
    # Verify these are the same top-5 as the aliased version
    result_alias = client.execute(
        f"SELECT id, array_distance(vec, [{','.join(map(str, query))}]) AS dist "
        "FROM vecs ORDER BY dist LIMIT 5"
    ).to_dict()
    ids_expr = {r["id"] for r in result}
    ids_alias = {r["id"] for r in result_alias}
    assert ids_expr == ids_alias


def test_topk_cosine_similarity_desc(client):
    """ORDER BY cosine_similarity(...) DESC LIMIT k."""
    rows = _load_many(client, n=50, dim=4)
    query = [1.0, 0.0, 0.0, 0.0]

    result = client.execute(
        f"SELECT id, cosine_similarity(vec, [{','.join(map(str, query))}]) AS sim "
        "FROM vecs ORDER BY sim DESC LIMIT 5"
    ).to_dict()

    assert len(result) == 5
    sims = [r["sim"] for r in result]
    assert sims == sorted(sims, reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# Utility functions
# ─────────────────────────────────────────────────────────────────────────────

def test_vector_dim(client):
    client.store([{"vec": [1.0, 2.0, 3.0]}, {"vec": [4.0, 5.0]}])
    result = client.execute("SELECT vector_dim(vec) AS dim FROM vecs ORDER BY dim").to_dict()
    dims = sorted([r["dim"] for r in result])
    assert dims == [2, 3]


def test_vector_norm(client):
    client.store([{"vec": [3.0, 4.0]}])
    result = client.execute("SELECT vector_norm(vec) AS n FROM vecs").to_dict()
    assert result[0]["n"] == pytest.approx(5.0, abs=1e-5)


def test_vector_to_string(client):
    client.store([{"vec": [1.0, 0.0]}])
    result = client.execute("SELECT vector_to_string(vec) AS s FROM vecs").to_dict()
    assert result[0]["s"].startswith("[")
    assert "1" in result[0]["s"]


# ─────────────────────────────────────────────────────────────────────────────
# String literal query vector
# ─────────────────────────────────────────────────────────────────────────────

def test_string_literal_query(client):
    """array_distance(col, '[1.0,0.0,0.0]') via string literal."""
    _load_3d_vecs(client)
    result = client.execute(
        "SELECT name, array_distance(vec, '[1.0,0.0,0.0]') AS dist FROM vecs "
        "ORDER BY name"
    ).to_dict()
    d = {r["name"]: r["dist"] for r in result}
    assert d["unit_x"] == pytest.approx(0.0, abs=1e-5)


# ─────────────────────────────────────────────────────────────────────────────
# DuckDB-compatible function names
# ─────────────────────────────────────────────────────────────────────────────

def test_duckdb_names(client):
    _load_3d_vecs(client)
    for fn in ["array_distance", "array_cosine_similarity", "array_inner_product",
               "array_l1_distance", "array_cosine_distance"]:
        result = client.execute(
            f"SELECT {fn}(vec, [1.0, 0.0, 0.0]) AS v FROM vecs LIMIT 1"
        ).to_dict()
        assert len(result) == 1
        assert result[0]["v"] is not None


# ─────────────────────────────────────────────────────────────────────────────
# SQL INSERT with vector literals
# ─────────────────────────────────────────────────────────────────────────────

def _bootstrap_vec_schema(client):
    """Store one row via Python client to establish the binary column schema."""
    client.store([{"id": 0, "vec": encode_vector([0.0, 0.0, 0.0])}])
    client.flush()


def test_sql_insert_array_literal(client):
    """INSERT INTO … VALUES (id, [f1, f2, f3]) array literal."""
    _bootstrap_vec_schema(client)
    client.execute("INSERT INTO vecs (id, vec) VALUES (1, [1.0, 0.0, 0.0])")
    client.execute("INSERT INTO vecs (id, vec) VALUES (2, [0.0, 1.0, 0.0])")
    client.flush()

    result = client.execute(
        "SELECT id, array_distance(vec, [1.0, 0.0, 0.0]) AS dist FROM vecs ORDER BY dist"
    ).to_dict()
    assert result[0]["id"] == 1
    assert result[0]["dist"] == pytest.approx(0.0, abs=1e-5)
    # result[1] is the bootstrapped origin [0,0,0] with dist=1.0
    assert result[1]["dist"] == pytest.approx(1.0, abs=1e-5)
    # result[2] is [0,1,0] with dist=sqrt(2)
    assert result[2]["dist"] == pytest.approx(math.sqrt(2), abs=1e-4)


def test_sql_insert_string_literal(client):
    """INSERT INTO … VALUES (id, '[f1, f2, f3]') string literal auto-coerced."""
    _bootstrap_vec_schema(client)
    client.execute("INSERT INTO vecs (id, vec) VALUES (1, '[1.0, 0.0, 0.0]')")
    client.execute("INSERT INTO vecs (id, vec) VALUES (2, '[0.0, 1.0, 0.0]')")
    client.flush()

    result = client.execute(
        "SELECT id, array_distance(vec, [1.0, 0.0, 0.0]) AS dist FROM vecs ORDER BY dist"
    ).to_dict()
    assert result[0]["id"] == 1
    assert result[0]["dist"] == pytest.approx(0.0, abs=1e-5)


def test_sql_insert_multi_values(client):
    """INSERT INTO … VALUES (…), (…), (…) multi-row in one statement."""
    _bootstrap_vec_schema(client)
    client.execute(
        "INSERT INTO vecs (id, vec) VALUES "
        "(1, [1.0, 0.0, 0.0]), "
        "(2, [0.0, 1.0, 0.0]), "
        "(3, [0.0, 0.0, 1.0])"
    )
    client.flush()

    result = client.execute("SELECT id FROM vecs ORDER BY id").to_dict()
    assert [r["id"] for r in result] == [0, 1, 2, 3]


def test_sql_insert_negative_components(client):
    """INSERT handles negative float components in array literal."""
    _bootstrap_vec_schema(client)
    client.execute("INSERT INTO vecs (id, vec) VALUES (1, [-1.0, -0.5, 0.0])")
    client.flush()

    result = client.execute(
        "SELECT vector_dim(vec) AS d FROM vecs WHERE id = 1"
    ).to_dict()
    assert result[0]["d"] == 3


def test_sql_insert_then_topk(client):
    """TopK query on rows inserted via SQL INSERT."""
    _bootstrap_vec_schema(client)
    client.execute(
        "INSERT INTO vecs (id, vec) VALUES "
        "(1, [1.0, 0.0, 0.0]), "
        "(2, [0.9, 0.1, 0.0]), "
        "(3, [0.0, 0.0, 1.0])"
    )
    client.flush()

    result = client.execute(
        "SELECT id, array_distance(vec, [1.0, 0.0, 0.0]) AS dist "
        "FROM vecs ORDER BY dist LIMIT 2"
    ).to_dict()
    assert len(result) == 2
    ids = [r["id"] for r in result]
    assert 1 in ids
    dists = [r["dist"] for r in result]
    assert dists == sorted(dists)


# ─────────────────────────────────────────────────────────────────────────────
# L2 squared distance
# ─────────────────────────────────────────────────────────────────────────────

def test_l2_squared_distance(client):
    """l2_squared_distance == L2² (no sqrt), verified against numpy."""
    _load_3d_vecs(client)
    q = [1.0, 0.0, 0.0]
    rows = client.execute(
        f"SELECT name, l2_squared_distance(vec, [{','.join(map(str,q))}]) AS d FROM vecs ORDER BY name"
    ).to_dict()
    d = {r["name"]: r["d"] for r in rows}
    assert d["unit_x"] == pytest.approx(0.0, abs=1e-5)
    assert d["origin"] == pytest.approx(1.0, abs=1e-5)       # sqrt(1)² = 1
    assert d["unit_y"] == pytest.approx(2.0, abs=1e-5)       # sqrt(2)² = 2
    assert d["diag"]   == pytest.approx(2.0, abs=1e-5)       # (0)²+(1)²+(1)² = 2


def test_l2_squared_equals_l2_squared_numpy(client):
    """l2_squared_distance result == numpy squared L2 for random vectors."""
    rng = np.random.default_rng(7)
    vecs = rng.random((10, 4), dtype=np.float32)
    q = rng.random(4, dtype=np.float32)
    rows = [{"id": i, "vec": vecs[i].tolist()} for i in range(len(vecs))]
    client.store(rows)

    result = client.execute(
        "SELECT id, l2_squared_distance(vec, [{}]) AS d FROM vecs ORDER BY id".format(
            ",".join(f"{v:.6f}" for v in q)
        )
    ).to_dict()

    for r in result:
        np_sq = float(np.sum((vecs[r["id"]] - q) ** 2))
        assert r["d"] == pytest.approx(np_sq, rel=1e-4)


def test_l2_squared_topk_consistent_with_l2(client):
    """TopK by l2_squared_distance and l2_distance must return the same IDs."""
    rows = _load_many(client, n=60, dim=4)
    q = [0.3, 0.6, 0.1, 0.8]
    q_str = ",".join(map(str, q))

    ids_l2 = {r["id"] for r in client.execute(
        f"SELECT id FROM vecs ORDER BY array_distance(vec, [{q_str}]) LIMIT 5"
    ).to_dict()}
    ids_sq = {r["id"] for r in client.execute(
        f"SELECT id FROM vecs ORDER BY l2_squared_distance(vec, [{q_str}]) LIMIT 5"
    ).to_dict()}
    assert ids_l2 == ids_sq


# ─────────────────────────────────────────────────────────────────────────────
# Negative inner product (MIPS)
# ─────────────────────────────────────────────────────────────────────────────

def test_negative_inner_product_values(client):
    """negative_inner_product == -dot_product, verified against numpy."""
    _load_3d_vecs(client)
    q = [1.0, 2.0, 3.0]
    rows = client.execute(
        "SELECT name, negative_inner_product(vec, [1.0, 2.0, 3.0]) AS v FROM vecs ORDER BY name"
    ).to_dict()
    v = {r["name"]: r["v"] for r in rows}
    assert v["origin"] == pytest.approx(0.0,  abs=1e-5)
    assert v["unit_x"] == pytest.approx(-1.0, abs=1e-5)
    assert v["unit_y"] == pytest.approx(-2.0, abs=1e-5)
    assert v["unit_z"] == pytest.approx(-3.0, abs=1e-5)
    assert v["diag"]   == pytest.approx(-6.0, abs=1e-5)


def test_topk_negative_inner_product(client):
    """ORDER BY negative_inner_product LIMIT k = MIPS (max-inner-product search)."""
    rows = _load_many(client, n=60, dim=4)
    q = [1.0, 0.0, 0.0, 0.0]
    q_str = ",".join(map(str, q))

    result = client.execute(
        f"SELECT id, negative_inner_product(vec, [{q_str}]) AS nip "
        "FROM vecs ORDER BY nip LIMIT 5"
    ).to_dict()

    assert len(result) == 5
    nips = [r["nip"] for r in result]
    assert nips == sorted(nips)
    # All nips must be ≤ 0 (inner product ≥ 0 since q[0]=1, all vecs ∈ [0,1])
    assert all(v <= 0.0 for v in nips)


# ─────────────────────────────────────────────────────────────────────────────
# TopK with L1 and L∞ distances
# ─────────────────────────────────────────────────────────────────────────────

def test_topk_l1_distance(client):
    """TopK by l1_distance returns correctly ordered results."""
    rows = _load_many(client, n=60, dim=4)
    q = [0.5, 0.5, 0.5, 0.5]
    q_str = ",".join(map(str, q))

    result = client.execute(
        f"SELECT id, l1_distance(vec, [{q_str}]) AS dist FROM vecs ORDER BY dist LIMIT 5"
    ).to_dict()

    assert len(result) == 5
    dists = [r["dist"] for r in result]
    assert dists == sorted(dists)


def test_topk_linf_distance(client):
    """TopK by linf_distance returns correctly ordered results."""
    rows = _load_many(client, n=60, dim=4)
    q = [0.5, 0.5, 0.5, 0.5]
    q_str = ",".join(map(str, q))

    result = client.execute(
        f"SELECT id, linf_distance(vec, [{q_str}]) AS dist FROM vecs ORDER BY dist LIMIT 5"
    ).to_dict()

    assert len(result) == 5
    dists = [r["dist"] for r in result]
    assert dists == sorted(dists)


def test_l1_l2_linf_inequality(client):
    """Metric inequality: L∞ ≤ L2 ≤ L1 for same pair of vectors (dim=4)."""
    _load_3d_vecs(client)
    q = [0.5, 0.5, 0.5]
    q_str = ",".join(map(str, q))

    rows = client.execute(
        f"SELECT name, "
        f"l1_distance(vec, [{q_str}]) AS l1, "
        f"array_distance(vec, [{q_str}]) AS l2, "
        f"linf_distance(vec, [{q_str}]) AS linf "
        f"FROM vecs"
    ).to_dict()

    for r in rows:
        assert r["linf"] <= r["l2"] + 1e-5
        assert r["l2"]   <= r["l1"] + 1e-5


# ─────────────────────────────────────────────────────────────────────────────
# Numpy correctness cross-validation
# ─────────────────────────────────────────────────────────────────────────────

def _np_l2(a, b):
    return float(np.sqrt(np.sum((np.array(a, dtype=np.float32) - np.array(b, dtype=np.float32)) ** 2)))

def _np_l1(a, b):
    return float(np.sum(np.abs(np.array(a, dtype=np.float32) - np.array(b, dtype=np.float32))))

def _np_linf(a, b):
    return float(np.max(np.abs(np.array(a, dtype=np.float32) - np.array(b, dtype=np.float32))))

def _np_cosine_sim(a, b):
    a, b = np.array(a, dtype=np.float32), np.array(b, dtype=np.float32)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-12))

def _np_dot(a, b):
    return float(np.dot(np.array(a, dtype=np.float32), np.array(b, dtype=np.float32)))


def test_numpy_correctness_all_metrics(client):
    """Cross-validate every distance function against numpy reference for 10 random vectors."""
    rng = np.random.default_rng(99)
    vecs = rng.random((10, 5), dtype=np.float32)
    q = rng.random(5, dtype=np.float32)
    q_str = ",".join(f"{v:.7f}" for v in q)

    client.store([{"id": i, "vec": vecs[i].tolist()} for i in range(len(vecs))])

    queries = {
        "l2":      f"SELECT id, array_distance(vec, [{q_str}]) AS v FROM vecs ORDER BY id",
        "l1":      f"SELECT id, l1_distance(vec, [{q_str}]) AS v FROM vecs ORDER BY id",
        "linf":    f"SELECT id, linf_distance(vec, [{q_str}]) AS v FROM vecs ORDER BY id",
        "cosine":  f"SELECT id, cosine_similarity(vec, [{q_str}]) AS v FROM vecs ORDER BY id",
        "dot":     f"SELECT id, inner_product(vec, [{q_str}]) AS v FROM vecs ORDER BY id",
    }
    refs = {
        "l2":     lambda v: _np_l2(v, q),
        "l1":     lambda v: _np_l1(v, q),
        "linf":   lambda v: _np_linf(v, q),
        "cosine": lambda v: _np_cosine_sim(v, q),
        "dot":    lambda v: _np_dot(v, q),
    }

    for metric, sql in queries.items():
        rows = client.execute(sql).to_dict()
        for r in rows:
            expected = refs[metric](vecs[r["id"]])
            assert r["v"] == pytest.approx(expected, rel=1e-3, abs=1e-5), \
                f"metric={metric} id={r['id']} got={r['v']} expected={expected}"


# ─────────────────────────────────────────────────────────────────────────────
# Cosine edge cases
# ─────────────────────────────────────────────────────────────────────────────

def test_cosine_identical_vectors(client):
    """cosine_similarity(stored_vec, same_literal) == 1.0.

    Tests the standard retrieval use-case: querying with the exact stored vector
    as a literal query vector.  Both sides decode via the same float32 path so
    result must be 1.0 within tight tolerance.
    """
    stored = [
        [1.0, 0.0, 0.0],
        [0.0, 1.0, 0.0],
        [3.0, 4.0, 0.0],
        [1.0, 2.0, 3.0],
    ]
    client.store([{"id": i, "vec": v} for i, v in enumerate(stored)])

    for i, v in enumerate(stored):
        q_str = ",".join(map(str, v))
        r = client.execute(
            f"SELECT cosine_similarity(vec, [{q_str}]) AS sim FROM vecs WHERE id = {i}"
        ).to_dict()
        assert r[0]["sim"] == pytest.approx(1.0, abs=1e-5), \
            f"id={i} vec={v} got={r[0]['sim']}"


def test_cosine_distance_plus_similarity_equals_one(client):
    """cosine_distance(a,b) + cosine_similarity(a,b) == 1 for all pairs."""
    _load_3d_vecs(client)
    q = [0.6, 0.8, 0.0]
    q_str = ",".join(map(str, q))
    rows = client.execute(
        f"SELECT cosine_similarity(vec, [{q_str}]) AS sim, "
        f"cosine_distance(vec, [{q_str}]) AS dist FROM vecs"
    ).to_dict()
    for r in rows:
        assert r["sim"] + r["dist"] == pytest.approx(1.0, abs=1e-5)


# ─────────────────────────────────────────────────────────────────────────────
# Symmetry and triangle inequality
# ─────────────────────────────────────────────────────────────────────────────

def test_l2_distance_symmetric(client):
    """array_distance(a,b) == array_distance(b,a)."""
    _load_3d_vecs(client)
    q = [0.3, 0.4, 0.5]
    q_str = ",".join(map(str, q))
    rows_fwd = client.execute(
        f"SELECT name, array_distance(vec, [{q_str}]) AS d FROM vecs ORDER BY name"
    ).to_dict()
    # Compare: store query as a row and cross-query is impossible without subquery;
    # instead verify against numpy directly
    for r in rows_fwd:
        stored = {"unit_x": [1,0,0], "unit_y": [0,1,0], "unit_z": [0,0,1],
                  "origin": [0,0,0], "diag": [1,1,1]}.get(r["name"])
        if stored:
            expected_sym = _np_l2(q, stored)
            assert r["d"] == pytest.approx(expected_sym, abs=1e-5)


# ─────────────────────────────────────────────────────────────────────────────
# Negative-component vectors
# ─────────────────────────────────────────────────────────────────────────────

def test_negative_components_all_metrics(client):
    """Distance functions handle negative float components correctly."""
    vecs_data = [
        {"id": 0, "vec": [-1.0,  0.0,  0.0]},
        {"id": 1, "vec": [ 0.0, -1.0,  0.0]},
        {"id": 2, "vec": [-1.0, -1.0, -1.0]},
    ]
    client.store(vecs_data)
    q = [-1.0, 0.0, 0.0]
    q_str = ",".join(map(str, q))

    # L2: distance from id=0 should be 0
    r = client.execute(
        f"SELECT id, array_distance(vec, [{q_str}]) AS d FROM vecs ORDER BY d LIMIT 1"
    ).to_dict()
    assert r[0]["id"] == 0
    assert r[0]["d"] == pytest.approx(0.0, abs=1e-5)

    # L1: id=0 should be closest
    r = client.execute(
        f"SELECT id, l1_distance(vec, [{q_str}]) AS d FROM vecs ORDER BY d LIMIT 1"
    ).to_dict()
    assert r[0]["id"] == 0

    # Cosine sim: id=0 (same direction as q) == 1.0
    r = client.execute(
        f"SELECT id, cosine_similarity(vec, [{q_str}]) AS s FROM vecs WHERE id = 0"
    ).to_dict()
    assert r[0]["s"] == pytest.approx(1.0, abs=1e-5)


# ─────────────────────────────────────────────────────────────────────────────
# Large dimension (128-d)
# ─────────────────────────────────────────────────────────────────────────────

def test_large_dim_128_topk(client):
    """TopK L2 on 128-dim vectors returns correct count and ordering."""
    rng = np.random.default_rng(55)
    n, dim = 200, 128
    vecs = rng.random((n, dim), dtype=np.float32)
    q = rng.random(dim, dtype=np.float32)

    client.store([{"id": i, "vec": vecs[i].tolist()} for i in range(n)])

    q_str = ",".join(f"{v:.6f}" for v in q)
    result = client.execute(
        f"SELECT id, array_distance(vec, [{q_str}]) AS dist FROM vecs ORDER BY dist LIMIT 10"
    ).to_dict()

    assert len(result) == 10
    dists = [r["dist"] for r in result]
    assert dists == sorted(dists)

    # Verify top-1 matches numpy brute-force
    np_dists = np.sqrt(np.sum((vecs - q) ** 2, axis=1))
    best_id = int(np.argmin(np_dists))
    assert result[0]["id"] == best_id


def test_large_dim_128_numpy_correctness(client):
    """array_distance value on 128-dim matches numpy to within float32 error."""
    rng = np.random.default_rng(77)
    vec = rng.random(128, dtype=np.float32)
    q   = rng.random(128, dtype=np.float32)
    client.store([{"id": 0, "vec": vec.tolist()}])

    q_str = ",".join(f"{v:.7f}" for v in q)
    result = client.execute(
        f"SELECT array_distance(vec, [{q_str}]) AS d FROM vecs"
    ).to_dict()

    np_dist = float(np.sqrt(np.sum((vec - q) ** 2)))
    assert result[0]["d"] == pytest.approx(np_dist, rel=1e-3)


# ─────────────────────────────────────────────────────────────────────────────
# WHERE filter + vector TopK
# ─────────────────────────────────────────────────────────────────────────────

def test_filter_then_topk(client):
    """WHERE predicate reduces candidate set before TopK ordering."""
    rng = np.random.default_rng(13)
    vecs = rng.random((50, 4), dtype=np.float32)
    rows = [{"id": i, "tag": "A" if i < 25 else "B", "vec": vecs[i].tolist()} for i in range(50)]
    client.store(rows)

    q_str = "0.5,0.5,0.5,0.5"
    result_all = client.execute(
        f"SELECT id FROM vecs ORDER BY array_distance(vec, [{q_str}]) LIMIT 5"
    ).to_dict()
    result_a = client.execute(
        f"SELECT id, array_distance(vec, [{q_str}]) AS dist FROM vecs "
        f"WHERE tag = 'A' ORDER BY dist LIMIT 5"
    ).to_dict()

    # Results from tag='A' filter must all have id < 25
    assert all(r["id"] < 25 for r in result_a)
    assert len(result_a) == 5
    dists = [r["dist"] for r in result_a]
    assert dists == sorted(dists)


# ─────────────────────────────────────────────────────────────────────────────
# tolist() on vector query results
# ─────────────────────────────────────────────────────────────────────────────

def test_topk_result_via_tolist(client):
    """ResultView.tolist() works correctly on vector query output."""
    rows = _load_many(client, n=30, dim=4)
    q_str = "0.5,0.5,0.5,0.5"

    result = client.execute(
        f"SELECT id, array_distance(vec, [{q_str}]) AS dist FROM vecs ORDER BY dist LIMIT 5"
    )
    lst = result.tolist()

    assert isinstance(lst, list)
    assert len(lst) == 5
    assert isinstance(lst[0], dict)
    assert "dist" in lst[0]
    assert "_id" not in lst[0]
    # Same as to_dict()
    assert lst == result.to_dict()
    # Ordering preserved
    dists = [r["dist"] for r in lst]
    assert dists == sorted(dists)


# ─────────────────────────────────────────────────────────────────────────────
# vector_norm edge cases
# ─────────────────────────────────────────────────────────────────────────────

def test_vector_norm_unit_vectors(client):
    """L2 norm of unit vectors == 1.0."""
    client.store([
        {"id": 0, "vec": [1.0, 0.0, 0.0]},
        {"id": 1, "vec": [0.0, 1.0, 0.0]},
        {"id": 2, "vec": [0.0, 0.0, 1.0]},
    ])
    rows = client.execute("SELECT id, vector_norm(vec) AS n FROM vecs ORDER BY id").to_dict()
    for r in rows:
        assert r["n"] == pytest.approx(1.0, abs=1e-5)


def test_vector_norm_matches_numpy(client):
    """vector_norm matches numpy linalg.norm for random vector."""
    rng = np.random.default_rng(21)
    v = rng.random(8, dtype=np.float32)
    client.store([{"id": 0, "vec": v.tolist()}])
    result = client.execute("SELECT vector_norm(vec) AS n FROM vecs").to_dict()
    assert result[0]["n"] == pytest.approx(float(np.linalg.norm(v)), rel=1e-4)


# ─────────────────────────────────────────────────────────────────────────────
# topk_distance — Python API and SQL API
#
# New design: topk_distance is an expression function used inside explode_rename.
#   SELECT explode_rename(topk_distance(col, [q], k, 'metric'), 'id_col', 'dist_col')
#   FROM table
# Returns k rows with exactly 2 columns: id_col (_id values) and dist_col (distances).
# ─────────────────────────────────────────────────────────────────────────────

def _load_topk_vecs(client):
    client.store([
        {"name": "origin", "vec": encode_vector([0.0, 0.0, 0.0])},
        {"name": "unit_x", "vec": encode_vector([1.0, 0.0, 0.0])},
        {"name": "unit_y", "vec": encode_vector([0.0, 1.0, 0.0])},
        {"name": "unit_z", "vec": encode_vector([0.0, 0.0, 1.0])},
        {"name": "diag",   "vec": encode_vector([1.0, 1.0, 1.0])},
    ])


def _topk_name(client, row_id):
    """Helper: look up the 'name' field for a given _id."""
    rows = client.execute(f"SELECT name FROM vecs WHERE _id = {row_id}").to_dict()
    return rows[0]["name"] if rows else None


def test_topk_distance_python_l2_basic(client):
    """Python topk_distance: returns _id + dist, nearest to [1,0,0] under L2."""
    _load_topk_vecs(client)
    rows = client.topk_distance("vec", [1.0, 0.0, 0.0], k=3, metric="l2").to_dict()
    assert len(rows) == 3
    assert "_id" in rows[0] and "dist" in rows[0]
    assert rows[0]["dist"] == pytest.approx(0.0, abs=1e-5)
    assert rows[1]["dist"] == pytest.approx(1.0, abs=1e-5)
    assert _topk_name(client, rows[0]["_id"]) == "unit_x"


def test_topk_distance_python_returns_id_and_dist(client):
    """topk_distance result has exactly '_id' and 'dist' columns by default."""
    _load_topk_vecs(client)
    rows = client.topk_distance("vec", [0.0, 0.0, 0.0], k=2).to_dict()
    assert set(rows[0].keys()) == {"_id", "dist"}
    assert rows[0]["dist"] == pytest.approx(0.0, abs=1e-5)


def test_topk_distance_python_custom_column_names(client):
    """topk_distance with id_col/dist_col overrides output column names."""
    _load_topk_vecs(client)
    rows = client.topk_distance(
        "vec", [1.0, 0.0, 0.0], k=2, id_col="row_id", dist_col="distance"
    ).to_dict()
    assert "row_id" in rows[0] and "distance" in rows[0]


def test_topk_distance_python_k_limits_results(client):
    """topk_distance returns exactly k rows (or fewer if table has < k rows)."""
    _load_topk_vecs(client)
    for k in (1, 3, 5, 10):
        rows = client.topk_distance("vec", [1.0, 0.0, 0.0], k=k).to_dict()
        assert len(rows) == min(k, 5)


def test_topk_distance_python_sorted_ascending(client):
    """Results are sorted by distance ascending (nearest first)."""
    _load_topk_vecs(client)
    rows = client.topk_distance("vec", [1.0, 0.0, 0.0], k=5).to_dict()
    dists = [r["dist"] for r in rows]
    assert dists == sorted(dists)


def test_topk_distance_python_cosine_metric(client):
    """topk_distance with cosine_distance: nearest _id maps to unit_x."""
    _load_topk_vecs(client)
    rows = client.topk_distance("vec", [1.0, 0.0, 0.0], k=2, metric="cosine_distance").to_dict()
    assert rows[0]["dist"] == pytest.approx(0.0, abs=1e-5)
    assert _topk_name(client, rows[0]["_id"]) == "unit_x"


def test_topk_distance_python_dot_metric(client):
    """dot topk uses negative inner product so max dot sorts first."""
    _load_topk_vecs(client)
    rows = client.topk_distance("vec", [1.0, 0.0, 0.0], k=2, metric="dot").to_dict()
    names = {_topk_name(client, r["_id"]) for r in rows}
    assert names == {"unit_x", "diag"}
    assert [r["dist"] for r in rows] == pytest.approx([-1.0, -1.0], abs=1e-5)


def test_topk_distance_python_l1_metric(client):
    """topk_distance with l1 metric: origin nearest (dist=0), diag farthest (dist=3)."""
    _load_topk_vecs(client)
    rows = client.topk_distance("vec", [0.0, 0.0, 0.0], k=5, metric="l1").to_dict()
    assert rows[0]["dist"] == pytest.approx(0.0, abs=1e-5)
    assert rows[-1]["dist"] == pytest.approx(3.0, abs=1e-5)
    assert _topk_name(client, rows[0]["_id"]) == "origin"
    assert _topk_name(client, rows[-1]["_id"]) == "diag"


def test_topk_distance_python_l2_squared_metric(client):
    """topk_distance with l2_squared: same _id order as l2, values are squared."""
    _load_topk_vecs(client)
    rows_l2 = client.topk_distance("vec", [1.0, 0.0, 0.0], k=5, metric="l2").to_dict()
    rows_sq = client.topk_distance("vec", [1.0, 0.0, 0.0], k=5, metric="l2_squared").to_dict()
    assert [r["_id"] for r in rows_l2] == [r["_id"] for r in rows_sq]
    for l2_row, sq_row in zip(rows_l2, rows_sq):
        assert sq_row["dist"] == pytest.approx(l2_row["dist"] ** 2, rel=1e-4)


def test_topk_distance_python_numpy_query(client):
    """topk_distance accepts numpy array as query vector."""
    _load_topk_vecs(client)
    q = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    rows = client.topk_distance("vec", q, k=1).to_dict()
    assert _topk_name(client, rows[0]["_id"]) == "unit_x"


def test_topk_distance_python_matches_order_by(client):
    """topk_distance _id order matches ORDER BY array_distance LIMIT."""
    _load_topk_vecs(client)
    q = [0.6, 0.3, 0.0]
    k = 3
    topk_rows = client.topk_distance("vec", q, k=k, metric="l2").to_dict()
    sql_rows = client.execute(
        f"SELECT _id, array_distance(vec, [{q[0]}, {q[1]}, {q[2]}]) AS dist "
        f"FROM vecs ORDER BY dist LIMIT {k}"
    ).to_dict()
    topk_rows.sort(key=lambda r: (round(r["dist"], 5), r["_id"]))
    sql_rows.sort(key=lambda r: (round(r["dist"], 5), r["_id"]))
    assert [r["_id"] for r in topk_rows] == [r["_id"] for r in sql_rows]


@pytest.mark.parametrize("metric", ["l2", "l2_squared", "cosine_distance", "dot", "l1", "linf"])
def test_batch_topk_distance_matches_single_queries(client, metric):
    """batch_topk_distance should match repeated topk_distance for f32 vector columns."""
    rng = np.random.default_rng(123)
    n, dim = 80, 8
    vecs = rng.random((n, dim), dtype=np.float32)
    client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    queries = rng.random((3, dim), dtype=np.float32)
    k = 5
    batch = client.batch_topk_distance("vec", queries, k=k, metric=metric)

    assert batch.shape == (3, k, 2)
    for qi, query in enumerate(queries):
        single = client.topk_distance("vec", query, k=k, metric=metric).to_dict()
        batch_ids = [int(batch[qi, j, 0]) for j in range(k)]
        batch_dists = [float(batch[qi, j, 1]) for j in range(k)]
        assert batch_ids == [int(row["_id"]) for row in single]
        assert batch_dists == pytest.approx(
            [float(row["dist"]) for row in single], rel=1e-5, abs=1e-5
        )


def test_topk_distance_sql_explode_rename_basic(client):
    """SQL explode_rename(topk_distance(...)): returns k rows with 2 named columns."""
    _load_topk_vecs(client)
    rows = client.execute(
        "SELECT explode_rename(topk_distance(vec, [1.0, 0.0, 0.0], 3, 'l2'), 'my_id', 'my_dist') FROM vecs"
    ).to_dict()
    assert len(rows) == 3
    assert set(rows[0].keys()) == {"my_id", "my_dist"}
    assert rows[0]["my_dist"] == pytest.approx(0.0, abs=1e-5)


def test_topk_distance_sql_subquery(client):
    """SELECT id, dist FROM (SELECT explode_rename(topk_distance(...)) FROM t) a."""
    _load_topk_vecs(client)
    rows = client.execute("""
        SELECT my_id, my_dist
        FROM (SELECT explode_rename(topk_distance(vec, [1.0, 0.0, 0.0], 3, 'l2'),
                                   'my_id', 'my_dist') FROM vecs) a
    """).to_dict()
    assert len(rows) == 3
    assert rows[0]["my_dist"] == pytest.approx(0.0, abs=1e-5)


def test_topk_distance_sql_join_back(client):
    """JOIN explode_rename result back to original table to get full row data."""
    _load_topk_vecs(client)
    rows = client.execute("""
        SELECT v.name, k.my_dist
        FROM vecs v
        JOIN (SELECT explode_rename(topk_distance(vec, [1.0, 0.0, 0.0], 3, 'l2'),
                                   'my_id', 'my_dist') FROM vecs) k
        ON v._id = k.my_id
        ORDER BY k.my_dist
    """).to_dict()
    assert len(rows) == 3
    assert rows[0]["name"] == "unit_x"
    assert rows[0]["my_dist"] == pytest.approx(0.0, abs=1e-5)


def test_topk_distance_sql_cosine_metric(client):
    """SQL topk_distance with cosine_distance metric."""
    _load_topk_vecs(client)
    rows = client.execute(
        "SELECT explode_rename(topk_distance(vec, [1.0, 0.0, 0.0], 2, 'cosine_distance'), '_id', 'dist') FROM vecs"
    ).to_dict()
    assert rows[0]["dist"] == pytest.approx(0.0, abs=1e-5)
    assert _topk_name(client, rows[0]["_id"]) == "unit_x"


def test_topk_distance_sql_matches_python(client):
    """SQL explode_rename and Python topk_distance return identical _id/dist."""
    _load_topk_vecs(client)
    q = [0.3, 0.6, 0.1]
    k = 4
    py_rows = client.topk_distance("vec", q, k=k, metric="l2").to_dict()
    sql_rows = client.execute(
        f"SELECT explode_rename(topk_distance(vec, [{q[0]}, {q[1]}, {q[2]}], {k}, 'l2'), '_id', 'dist') FROM vecs"
    ).to_dict()
    assert [r["_id"] for r in py_rows] == [r["_id"] for r in sql_rows]
    for a, b in zip(py_rows, sql_rows):
        assert a["dist"] == pytest.approx(b["dist"], rel=1e-4)


def test_topk_distance_large_k(client):
    """topk_distance with k > table size returns all rows."""
    _load_topk_vecs(client)
    rows = client.topk_distance("vec", [0.0, 0.0, 0.0], k=100).to_dict()
    assert len(rows) == 5


def test_topk_distance_large_table(client):
    """topk_distance is correct on a larger random table (ground-truth via numpy)."""
    rng = np.random.default_rng(42)
    n, dim = 500, 16
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    # Store with no extra columns so _id == row index + 1 (1-based insertion order)
    client.store([{"vec": encode_vector(vecs[i])} for i in range(n)])

    k = 10
    topk = client.topk_distance("vec", query, k=k, metric="l2").to_dict()
    assert len(topk) == k

    # Ground truth: top-k by L2 via numpy (use _id as the row index proxy)
    dists = np.linalg.norm(vecs - query, axis=1)
    gt_ids = set(int(i) + 1 for i in np.argsort(dists)[:k])
    result_ids = set(int(r["_id"]) for r in topk)
    assert result_ids == gt_ids


# ─────────────────────────────────────────────────────────────────────────────
# Float16Vector (f16 storage) tests
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def f16_client(tmp_path):
    c = ApexClient(dirpath=str(tmp_path), drop_if_exists=True)
    c.execute("CREATE TABLE f16vecs (name TEXT, vec FLOAT16_VECTOR)")
    c.use_table("f16vecs")
    yield c
    c.close()


def test_f16_create_table(tmp_path):
    """CREATE TABLE with FLOAT16_VECTOR column succeeds."""
    c = ApexClient(dirpath=str(tmp_path), drop_if_exists=True)
    c.execute("CREATE TABLE t (name TEXT, vec FLOAT16_VECTOR)")
    c.close()


def test_f16_store_and_count(f16_client):
    """Rows stored in float16_vector table are retrievable."""
    rows = [
        {"name": "a", "vec": np.array([1.0, 0.0, 0.0], dtype=np.float32)},
        {"name": "b", "vec": np.array([0.0, 1.0, 0.0], dtype=np.float32)},
        {"name": "c", "vec": np.array([0.0, 0.0, 1.0], dtype=np.float32)},
    ]
    f16_client.store(rows)
    result = f16_client.execute("SELECT COUNT(*) FROM f16vecs").to_dict()
    assert result[0]["COUNT(*)"] == 3


def test_f16_topk_distance_l2(f16_client):
    """topk_distance on FLOAT16_VECTOR column returns correct nearest neighbor."""
    rng = np.random.default_rng(7)
    n, dim = 200, 16
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)

    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    k = 5
    topk = f16_client.topk_distance("vec", query, k=k, metric="l2").to_dict()
    assert len(topk) == k

    # Ground-truth via numpy (f16 quantization shifts distances slightly)
    dists_np = np.linalg.norm(vecs - query, axis=1)
    gt_top20_ids = set(int(i) + 1 for i in np.argsort(dists_np)[:20])
    result_ids = set(int(r["_id"]) for r in topk)
    # Top-5 results should mostly overlap with numpy top-20 (f16 has ~3e-4 rel error)
    assert len(result_ids & gt_top20_ids) >= 4


def test_f16_topk_distance_cosine(f16_client):
    """topk_distance with cosine_distance on FLOAT16_VECTOR."""
    rng = np.random.default_rng(13)
    n, dim = 100, 8
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)

    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    topk = f16_client.topk_distance("vec", query, k=3, metric="cosine_distance").to_dict()
    assert len(topk) == 3
    # Distances should be in [0, 2]
    for r in topk:
        assert 0.0 <= r["dist"] <= 2.0 + 1e-4


def test_f16_memory_savings(tmp_path):
    """Float16 column uses ~half the vector-data bytes of float32 for same vectors."""
    import os

    dim = 64
    n = 1000
    rng = np.random.default_rng(0)
    vecs = rng.random((n, dim), dtype=np.float32)

    # f32 table — use create_table() so numpy arrays auto-create FixedList column
    c32 = ApexClient(dirpath=str(tmp_path / "f32"), drop_if_exists=True)
    c32.create_table("t")
    c32.store([{"vec": vecs[i]} for i in range(n)])
    c32.close()

    # f16 table — explicitly FLOAT16_VECTOR
    c16 = ApexClient(dirpath=str(tmp_path / "f16"), drop_if_exists=True)
    c16.execute("CREATE TABLE t (vec FLOAT16_VECTOR)")
    c16.use_table("t")
    c16.store([{"vec": vecs[i]} for i in range(n)])
    c16.close()

    def dir_size(p):
        return sum(
            os.path.getsize(p / f)
            for f in os.listdir(p)
            # Count only the table data file: per-database metadata
            # (.apex_tables, .apex_schemas, lock) is layout detail, not
            # vector-data footprint.
            if os.path.isfile(p / f) and f.endswith(".apex")
        )

    f32_size = dir_size(tmp_path / "f32")
    f16_size = dir_size(tmp_path / "f16")
    # f16 vector data is 2 bytes/elem vs f32's 4 bytes/elem → f16 file should be smaller
    assert f16_size < f32_size, (
        f"Expected f16 ({f16_size}B) < f32 ({f32_size}B) for {n}×{dim} vectors"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Float16Vector — all distance metrics
# ─────────────────────────────────────────────────────────────────────────────

def test_f16_topk_all_metrics(f16_client):
    """topk_distance on FLOAT16_VECTOR supports all 6 distance metrics."""
    rng = np.random.default_rng(55)
    n, dim = 60, 8
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    metrics = ["l2", "l2_squared", "cosine_distance", "l1", "linf", "dot"]
    for m in metrics:
        rows = f16_client.topk_distance("vec", query, k=5, metric=m).to_dict()
        assert len(rows) == 5, f"metric={m}: expected 5 rows"
        dists = [r["dist"] for r in rows]
        assert dists == sorted(dists), f"metric={m}: results not sorted ascending"


def test_f16_topk_l2_squared(f16_client):
    """l2_squared on FLOAT16_VECTOR: same ordering as l2, values ≈ l2²."""
    rng = np.random.default_rng(3)
    n, dim = 80, 8
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    k = 5
    rows_l2 = f16_client.topk_distance("vec", query, k=k, metric="l2").to_dict()
    rows_sq = f16_client.topk_distance("vec", query, k=k, metric="l2_squared").to_dict()

    assert [r["_id"] for r in rows_l2] == [r["_id"] for r in rows_sq]
    for l2r, sqr in zip(rows_l2, rows_sq):
        assert sqr["dist"] == pytest.approx(l2r["dist"] ** 2, rel=5e-3)


def test_f16_topk_l1(f16_client):
    """l1 topk on FLOAT16_VECTOR returns sorted results matching numpy reference."""
    rng = np.random.default_rng(77)
    n, dim = 80, 8
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    k = 5
    rows = f16_client.topk_distance("vec", query, k=k, metric="l1").to_dict()
    assert len(rows) == k
    dists = [r["dist"] for r in rows]
    assert dists == sorted(dists)

    # Ground-truth: L1 via numpy with f16 quantization
    vecs_q = vecs.astype(np.float16).astype(np.float32)
    np_l1 = np.sum(np.abs(vecs_q - query), axis=1)
    gt_top20 = set(int(i) + 1 for i in np.argsort(np_l1)[:20])
    result_ids = set(int(r["_id"]) for r in rows)
    assert len(result_ids & gt_top20) >= 4


def test_f16_topk_linf(f16_client):
    """linf topk on FLOAT16_VECTOR returns sorted results."""
    rng = np.random.default_rng(99)
    n, dim = 80, 8
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    rows = f16_client.topk_distance("vec", query, k=5, metric="linf").to_dict()
    assert len(rows) == 5
    dists = [r["dist"] for r in rows]
    assert dists == sorted(dists)


def test_f16_topk_inner_product(f16_client):
    """dot/inner_product on FLOAT16_VECTOR: ordered by descending dot product."""
    rng = np.random.default_rng(17)
    n, dim = 80, 8
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    rows = f16_client.topk_distance("vec", query, k=5, metric="dot").to_dict()
    assert len(rows) == 5
    dists = [r["dist"] for r in rows]
    assert dists == sorted(dists)


# ─────────────────────────────────────────────────────────────────────────────
# Float16Vector — correctness vs numpy reference
# ─────────────────────────────────────────────────────────────────────────────

def _f16_quantize(v):
    """Simulate f16 quantization: cast f32→f16→f32."""
    return v.astype(np.float16).astype(np.float32)


def test_f16_l2_correctness_vs_numpy(f16_client):
    """f16 L2 distance matches numpy reference (f16-quantized vectors) to 5e-3."""
    rng = np.random.default_rng(42)
    n, dim = 30, 16
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    rows = f16_client.topk_distance("vec", query, k=n, metric="l2").to_dict()

    vecs_q = _f16_quantize(vecs)
    for r in rows:
        idx = int(r["_id"]) - 1
        expected = float(np.sqrt(np.sum((vecs_q[idx] - query) ** 2)))
        assert r["dist"] == pytest.approx(expected, rel=5e-3, abs=1e-4), \
            f"id={idx} got={r['dist']} expected={expected}"


def test_f16_cosine_correctness_vs_numpy(f16_client):
    """f16 cosine distance matches numpy reference to 5e-3."""
    rng = np.random.default_rng(11)
    n, dim = 30, 16
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    rows = f16_client.topk_distance("vec", query, k=n, metric="cosine_distance").to_dict()

    vecs_q = _f16_quantize(vecs)
    for r in rows:
        idx = int(r["_id"]) - 1
        a, b = vecs_q[idx], query
        na, nb = np.linalg.norm(a), np.linalg.norm(b)
        expected = 1.0 - float(np.dot(a, b)) / (na * nb) if na > 0 and nb > 0 else 0.0
        assert r["dist"] == pytest.approx(expected, rel=5e-3, abs=1e-4), \
            f"id={idx} got={r['dist']} expected={expected}"


def test_f16_l1_correctness_vs_numpy(f16_client):
    """f16 L1 distance matches numpy reference to 5e-3."""
    rng = np.random.default_rng(23)
    n, dim = 30, 16
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    rows = f16_client.topk_distance("vec", query, k=n, metric="l1").to_dict()

    vecs_q = _f16_quantize(vecs)
    for r in rows:
        idx = int(r["_id"]) - 1
        expected = float(np.sum(np.abs(vecs_q[idx] - query)))
        assert r["dist"] == pytest.approx(expected, rel=5e-3, abs=1e-4), \
            f"id={idx} got={r['dist']} expected={expected}"


# ─────────────────────────────────────────────────────────────────────────────
# Float16Vector — edge cases
# ─────────────────────────────────────────────────────────────────────────────

def test_f16_dim_not_multiple_of_8(f16_client):
    """FLOAT16_VECTOR works for dimensions not a multiple of 8 (scalar tail path)."""
    rng = np.random.default_rng(7)
    dim = 13   # 8 + 5 — exercises the scalar tail
    n = 30
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    rows = f16_client.topk_distance("vec", query, k=5, metric="l2").to_dict()
    assert len(rows) == 5
    dists = [r["dist"] for r in rows]
    assert dists == sorted(dists)


def test_f16_dim_1(f16_client):
    """FLOAT16_VECTOR with dim=1 (extreme edge case)."""
    f16_client.store([
        {"name": "a", "vec": np.array([1.0], dtype=np.float32)},
        {"name": "b", "vec": np.array([2.0], dtype=np.float32)},
        {"name": "c", "vec": np.array([3.0], dtype=np.float32)},
    ])
    rows = f16_client.topk_distance("vec", [1.0], k=3, metric="l2").to_dict()
    assert len(rows) == 3
    assert rows[0]["dist"] == pytest.approx(0.0, abs=1e-4)


def test_f16_negative_components(f16_client):
    """FLOAT16_VECTOR handles negative float components correctly."""
    vecs = [
        {"name": "neg_x", "vec": np.array([-1.0,  0.0,  0.0], dtype=np.float32)},
        {"name": "pos_x", "vec": np.array([ 1.0,  0.0,  0.0], dtype=np.float32)},
        {"name": "neg_all", "vec": np.array([-1.0, -1.0, -1.0], dtype=np.float32)},
    ]
    f16_client.store(vecs)
    query = [-1.0, 0.0, 0.0]

    rows = f16_client.topk_distance("vec", query, k=1, metric="l2").to_dict()
    assert rows[0]["dist"] == pytest.approx(0.0, abs=1e-3)

    rows_l1 = f16_client.topk_distance("vec", query, k=1, metric="l1").to_dict()
    assert rows_l1[0]["dist"] == pytest.approx(0.0, abs=1e-3)


def test_f16_zero_query_vector(f16_client):
    """topk_distance with a zero query vector returns valid distances."""
    rng = np.random.default_rng(5)
    n, dim = 20, 4
    vecs = rng.random((n, dim), dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    query = [0.0] * dim
    rows = f16_client.topk_distance("vec", query, k=5, metric="l2").to_dict()
    assert len(rows) == 5
    dists = [r["dist"] for r in rows]
    assert dists == sorted(dists)
    assert all(d >= 0.0 for d in dists)


def test_f16_large_dim_128(f16_client):
    """FLOAT16_VECTOR works at dim=128 (typical embedding size)."""
    rng = np.random.default_rng(99)
    n, dim = 200, 128
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    rows = f16_client.topk_distance("vec", query, k=10, metric="l2").to_dict()
    assert len(rows) == 10
    dists = [r["dist"] for r in rows]
    assert dists == sorted(dists)

    # Nearest neighbour should be in numpy top-30 (f16 ≈3e-4 rel error)
    vecs_q = _f16_quantize(vecs)
    np_dists = np.linalg.norm(vecs_q - query, axis=1)
    gt_top30 = set(int(i) + 1 for i in np.argsort(np_dists)[:30])
    result_ids = set(int(r["_id"]) for r in rows)
    assert len(result_ids & gt_top30) >= 8


# ─────────────────────────────────────────────────────────────────────────────
# Float16Vector — SQL interface
# ─────────────────────────────────────────────────────────────────────────────

def test_f16_sql_order_by_distance(f16_client):
    """ORDER BY array_distance(vec, [...]) on FLOAT16_VECTOR column."""
    rng = np.random.default_rng(31)
    n, dim = 50, 4
    vecs = rng.random((n, dim), dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])
    q = rng.random(dim, dtype=np.float32)
    q_str = ",".join(f"{v:.6f}" for v in q)

    rows = f16_client.execute(
        f"SELECT _id, array_distance(vec, [{q_str}]) AS dist FROM f16vecs ORDER BY dist LIMIT 5"
    ).to_dict()
    assert len(rows) == 5
    dists = [r["dist"] for r in rows]
    assert dists == sorted(dists)


def test_f16_sql_cosine_similarity(f16_client):
    """cosine_similarity(vec, [...]) works on FLOAT16_VECTOR column."""
    f16_client.store([
        {"name": "same", "vec": np.array([1.0, 0.0, 0.0], dtype=np.float32)},
        {"name": "ortho", "vec": np.array([0.0, 1.0, 0.0], dtype=np.float32)},
    ])
    rows = f16_client.execute(
        "SELECT name, cosine_similarity(vec, [1.0, 0.0, 0.0]) AS sim FROM f16vecs ORDER BY sim DESC"
    ).to_dict()
    assert rows[0]["name"] == "same"
    assert rows[0]["sim"] == pytest.approx(1.0, abs=2e-3)
    assert rows[1]["sim"] == pytest.approx(0.0, abs=2e-3)


def test_f16_sql_cosine_distance_accepts_python_lists(tmp_path):
    """FLOAT16_VECTOR SQL distance should not panic when rows are inserted as Python lists."""
    c = ApexClient(dirpath=str(tmp_path), drop_if_exists=True)
    c.execute("CREATE TABLE f16docs (name TEXT, vec FLOAT16_VECTOR)")
    c.use_table("f16docs")
    c.store([
        {"name": "near", "vec": [0.10, 0.82, 0.20]},
        {"name": "far", "vec": [0.80, 0.10, 0.10]},
    ])

    rows = c.execute(
        "SELECT name, cosine_distance(vec, [0.12, 0.78, 0.25]) AS dist "
        "FROM f16docs ORDER BY dist"
    ).to_dict()

    assert rows[0]["name"] == "near"
    c.close()


def test_f16_sql_cosine_distance_accepts_single_python_list(tmp_path):
    """A single dict store() should encode Python lists correctly for FLOAT16_VECTOR columns."""
    c = ApexClient(dirpath=str(tmp_path), drop_if_exists=True)
    c.execute("CREATE TABLE f16docs (name TEXT, vec FLOAT16_VECTOR)")
    c.use_table("f16docs")
    c.store({"name": "only", "vec": [0.10, 0.82, 0.20]})

    rows = c.execute(
        "SELECT name, cosine_distance(vec, [0.10, 0.82, 0.20]) AS dist "
        "FROM f16docs"
    ).to_dict()

    assert len(rows) == 1
    assert rows[0]["name"] == "only"
    assert rows[0]["dist"] == pytest.approx(0.0, abs=2e-3)
    c.close()


def test_f16_single_python_list_can_be_first_field(tmp_path):
    """Vector-first single dicts should not be misclassified as columnar input."""
    c = ApexClient(dirpath=str(tmp_path), drop_if_exists=True)
    c.execute("CREATE TABLE f16docs (vec FLOAT16_VECTOR, name TEXT)")
    c.use_table("f16docs")
    c.store({"vec": [0.10, 0.82, 0.20], "name": "vector-first"})

    rows = c.execute(
        "SELECT name, cosine_distance(vec, [0.10, 0.82, 0.20]) AS dist "
        "FROM f16docs"
    ).to_dict()

    assert rows[0]["name"] == "vector-first"
    assert rows[0]["dist"] == pytest.approx(0.0, abs=2e-3)
    c.close()


def test_f16_sql_l1_l2_linf(f16_client):
    """l1_distance / array_distance / linf_distance all work on FLOAT16_VECTOR."""
    f16_client.store([
        {"name": "a", "vec": np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32)},
        {"name": "b", "vec": np.array([0.0, 0.0, 0.0, 0.0], dtype=np.float32)},
    ])
    q_str = "1.0, 2.0, 3.0, 4.0"
    r = f16_client.execute(
        f"SELECT name, l1_distance(vec, [{q_str}]) AS l1, "
        f"array_distance(vec, [{q_str}]) AS l2, "
        f"linf_distance(vec, [{q_str}]) AS linf FROM f16vecs ORDER BY l2"
    ).to_dict()
    # Row "a" is identical to query, so all distances ≈ 0
    assert r[0]["name"] == "a"
    assert r[0]["l1"]   == pytest.approx(0.0, abs=2e-3)
    assert r[0]["l2"]   == pytest.approx(0.0, abs=2e-3)
    assert r[0]["linf"] == pytest.approx(0.0, abs=2e-3)


def test_f16_sql_explode_rename(f16_client):
    """SQL explode_rename(topk_distance(...)) works on FLOAT16_VECTOR column."""
    rng = np.random.default_rng(61)
    n, dim = 40, 4
    vecs = rng.random((n, dim), dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])
    q_str = ",".join(f"{v:.6f}" for v in rng.random(dim, dtype=np.float32))

    rows = f16_client.execute(
        f"SELECT explode_rename(topk_distance(vec, [{q_str}], 5, 'l2'), '_id', 'dist') FROM f16vecs"
    ).to_dict()
    assert len(rows) == 5
    dists = [r["dist"] for r in rows]
    assert dists == sorted(dists)


def test_f16_sql_type_aliases(tmp_path):
    """FLOAT16VECTOR and F16_VECTOR are accepted as type name aliases."""
    c = ApexClient(dirpath=str(tmp_path), drop_if_exists=True)
    c.execute("CREATE TABLE t1 (vec FLOAT16VECTOR)")
    c.execute("CREATE TABLE t2 (vec F16_VECTOR)")
    c.close()


# ─────────────────────────────────────────────────────────────────────────────
# Float16Vector — batch_topk_distance
# ─────────────────────────────────────────────────────────────────────────────

def test_f16_batch_topk(f16_client):
    """batch_topk_distance on FLOAT16_VECTOR returns (N, k, 2) array."""
    rng = np.random.default_rng(19)
    n, dim = 200, 16
    vecs = rng.random((n, dim), dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    N, k = 10, 5
    queries = rng.random((N, dim), dtype=np.float32)
    result = f16_client.batch_topk_distance("vec", queries, k=k, metric="l2")

    assert result.shape == (N, k, 2)
    # Each query's results must be sorted ascending by distance
    for i in range(N):
        dists = result[i, :, 1]
        assert list(dists) == sorted(dists), f"query {i}: not sorted"
    # Distances must be non-negative
    assert (result[:, :, 1] >= 0).all()


def test_f16_batch_topk_consistent_with_single(f16_client):
    """batch_topk_distance and topk_distance return the same IDs for same query."""
    rng = np.random.default_rng(71)
    n, dim = 100, 8
    vecs = rng.random((n, dim), dtype=np.float32)
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(n)])

    query = rng.random(dim, dtype=np.float32)
    k = 5

    single = f16_client.topk_distance("vec", query, k=k, metric="l2").to_dict()
    batch  = f16_client.batch_topk_distance("vec", query.reshape(1, -1), k=k, metric="l2")

    single_ids = set(int(r["_id"]) for r in single)
    batch_ids  = set(int(batch[0, j, 0]) for j in range(k))
    assert single_ids == batch_ids


# ─────────────────────────────────────────────────────────────────────────────
# Float16Vector — input formats
# ─────────────────────────────────────────────────────────────────────────────

def test_f16_store_python_list(f16_client):
    """FLOAT16_VECTOR column accepts raw Python list input."""
    f16_client.store([
        {"name": "a", "vec": [1.0, 0.0, 0.0]},
        {"name": "b", "vec": [0.0, 1.0, 0.0]},
    ])
    result = f16_client.execute("SELECT COUNT(*) FROM f16vecs").to_dict()
    assert result[0]["COUNT(*)"] == 2
    rows = f16_client.topk_distance("vec", [1.0, 0.0, 0.0], k=1, metric="l2").to_dict()
    assert rows[0]["dist"] == pytest.approx(0.0, abs=1e-3)


def test_f16_store_numpy_float16(f16_client):
    """FLOAT16_VECTOR column accepts numpy float16 array input."""
    vecs = [
        {"name": "a", "vec": np.array([1.0, 0.0, 0.0], dtype=np.float16)},
        {"name": "b", "vec": np.array([0.0, 1.0, 0.0], dtype=np.float16)},
    ]
    f16_client.store(vecs)
    result = f16_client.execute("SELECT COUNT(*) FROM f16vecs").to_dict()
    assert result[0]["COUNT(*)"] == 2

    rows = f16_client.topk_distance("vec", [1.0, 0.0, 0.0], k=1, metric="l2").to_dict()
    assert rows[0]["dist"] == pytest.approx(0.0, abs=1e-3)


def test_f16_store_numpy_float32(f16_client):
    """FLOAT16_VECTOR column accepts numpy float32 array input (auto-quantized to f16)."""
    vecs = [
        {"name": "a", "vec": np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)},
        {"name": "b", "vec": np.array([0.5, 0.6, 0.7, 0.8], dtype=np.float32)},
    ]
    f16_client.store(vecs)
    q = [0.1, 0.2, 0.3, 0.4]
    rows = f16_client.topk_distance("vec", q, k=1, metric="l2").to_dict()
    assert rows[0]["dist"] == pytest.approx(0.0, abs=5e-3)


# ─────────────────────────────────────────────────────────────────────────────
# Float16Vector — precision bounds
# ─────────────────────────────────────────────────────────────────────────────

def test_f16_quantization_error_bound(f16_client):
    """f16 relative quantization error is within 2e-3 for typical float values."""
    rng = np.random.default_rng(100)
    dim = 64
    vec = rng.random(dim, dtype=np.float32) * 2.0 - 1.0  # [-1, 1]
    vec2 = rng.random(dim, dtype=np.float32) * 2.0 - 1.0  # second record for batch store
    query = rng.random(dim, dtype=np.float32) * 2.0 - 1.0
    # Use batch store (len>1) so the query goes through _store_columnar → correct f16 encoding
    f16_client.store([{"name": "v", "vec": vec}, {"name": "v2", "vec": vec2}])

    q_str = ",".join(f"{v:.7f}" for v in query)
    rows = f16_client.execute(
        f"SELECT name, array_distance(vec, [{q_str}]) AS dist FROM f16vecs WHERE name = 'v'"
    ).to_dict()
    assert len(rows) == 1, "record 'v' not found"
    got = rows[0]["dist"]

    # Expected with f16 quantization
    expected = float(np.sqrt(np.sum((_f16_quantize(vec) - query) ** 2)))
    assert got == pytest.approx(expected, rel=2e-3, abs=1e-4)


def test_f16_cosine_unit_vectors(f16_client):
    """cosine_distance(v, v) ≈ 0 for unit vectors (self-similarity).

    Note: With float16 storage, there's precision loss (~0.1 tolerance).
    This is expected due to float16's limited precision (~3 decimal digits).
    """
    rng = np.random.default_rng(200)
    vecs = rng.random((5, 8), dtype=np.float32)
    # Normalize to unit vectors
    vecs = vecs / np.linalg.norm(vecs, axis=1, keepdims=True)
    # Batch store (len>1) so all vectors go through _store_columnar → correct f16 encoding
    f16_client.store([{"name": str(i), "vec": vecs[i]} for i in range(len(vecs))])

    for i, v in enumerate(vecs):
        rows = f16_client.topk_distance("vec", v.tolist(), k=1, metric="cosine_distance").to_dict()
        # float16 has ~3 decimal digits of precision, so tolerance is ~0.1
        assert rows[0]["dist"] == pytest.approx(0.0, abs=0.15), \
            f"unit vec {i}: cosine self-distance = {rows[0]['dist']}"


# ─────────────────────────────────────────────────────────────────────────────
# Float16Vector — consistent ordering with f32
# ─────────────────────────────────────────────────────────────────────────────

def test_f16_topk_ordering_consistent_with_f32(tmp_path):
    """f16 TopK order matches f32 TopK order for the same data (top-k overlap ≥ 80%)."""
    rng = np.random.default_rng(42)
    n, dim, k = 300, 32, 10
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)

    # f32 table
    c32 = ApexClient(dirpath=str(tmp_path / "f32"), drop_if_exists=True)
    c32.create_table("t")
    c32.store([{"vec": vecs[i]} for i in range(n)])
    ids_f32 = set(int(r["_id"]) for r in c32.topk_distance("vec", query, k=k).to_dict())
    c32.close()

    # f16 table — quantized vectors
    c16 = ApexClient(dirpath=str(tmp_path / "f16"), drop_if_exists=True)
    c16.execute("CREATE TABLE t (vec FLOAT16_VECTOR)")
    c16.use_table("t")
    c16.store([{"vec": vecs[i]} for i in range(n)])
    ids_f16 = set(int(r["_id"]) for r in c16.topk_distance("vec", query, k=k).to_dict())
    c16.close()

    overlap = len(ids_f32 & ids_f16)
    assert overlap >= int(k * 0.8), (
        f"f16 vs f32 TopK overlap {overlap}/{k} < 80% for dim={dim}"
    )
