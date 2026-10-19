"""Fused distance TopK for ``ORDER BY <distance>(col, [q]) LIMIT k``.

The general path evaluated the distance for every row (allocating a ``Vec<f32>``
per row) and then fully sorted the relation. This shape is the hybrid-retrieval
one (r08/py11): the fast path runs the allocation-free SIMD distance plus the
bounded heap the exact vector TopK lane already uses, and the shared tail still
applies projection, ordering and LIMIT.

These tests pin the parity with the general path (a ``WHERE 1 = 1`` predicate
disables the fast path while keeping the same answer), the metric coverage, the
shapes that must keep the general path (``DESC``, ``OFFSET``, no ``LIMIT``, a
predicate, ``DISTINCT``, a window, NULL vectors), and the physical-path trace.
"""

import shutil
import tempfile

import numpy as np
import pytest

from apexbase import ApexClient

ROWS = 2_000
DIM = 8


def _vectors():
    rng = np.random.default_rng(20260930)
    return rng.standard_normal((ROWS, DIM), dtype=np.float32), rng.standard_normal(
        DIM
    ).astype(np.float32)


def _literal(vector):
    return "[" + ",".join(f"{float(value):.6f}" for value in vector) + "]"


def _path(client, sql):
    plan = client.execute("EXPLAIN ANALYZE " + sql).to_dict()[0]["plan"]
    assert "Actual Path:" in plan, plan
    return plan.split("Actual Path:", 1)[1].splitlines()[0].strip()


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    vectors, query = _vectors()
    apex = ApexClient(root, drop_if_exists=True, enable_cache=False)
    apex.create_table("docs", {"doc": "int64", "body": "string", "emb": "float32_vector"})
    apex.use_table("docs")
    for start in range(0, ROWS, 500):
        end = min(start + 500, ROWS)
        apex.store(
            [
                {
                    "doc": index,
                    "body": f"document {index}",
                    "emb": [float(value) for value in vectors[index]],
                }
                for index in range(start, end)
            ]
        )
    apex.flush()
    apex._vectors = vectors  # type: ignore[attr-defined]
    apex._query = query  # type: ignore[attr-defined]
    try:
        yield apex
    finally:
        apex.close()
        shutil.rmtree(root, ignore_errors=True)


def _cosine_order(vectors, query):
    values = vectors.astype(np.float64)
    query = query.astype(np.float64)
    distances = 1.0 - (values @ query) / (
        np.linalg.norm(values, axis=1) * np.linalg.norm(query)
    )
    order = np.argsort(distances, kind="stable")
    return order, distances


def test_alias_form_selects_the_exact_top_k(client):
    vectors, query = client._vectors, client._query
    literal = _literal(query)
    sql = (
        f"SELECT doc, cosine_distance(emb, {literal}) AS dist "
        "FROM docs ORDER BY dist LIMIT 10"
    )
    rows = client.execute(sql).to_dict()
    assert _path(client, sql) == "order_by_distance_topk_direct"
    assert len(rows) == 10

    order, distances = _cosine_order(vectors, query)
    assert [int(row["doc"]) for row in rows] == [int(index) for index in order[:10]]
    for row in rows:
        assert row["dist"] == pytest.approx(distances[int(row["doc"])], abs=1e-5)
    assert [row["dist"] for row in rows] == sorted(row["dist"] for row in rows)


def test_expression_form_matches_the_general_path(client):
    query = _client_query = client._query
    literal = _literal(query)
    fast = f"SELECT doc FROM docs ORDER BY cosine_distance(emb, {literal}) LIMIT 10"
    # `WHERE 1 = 1` keeps the same rows while making the clause unfit for the
    # fast path, so it is the general-path baseline.
    baseline = (
        f"SELECT doc FROM docs WHERE 1 = 1 "
        f"ORDER BY cosine_distance(emb, {literal}) LIMIT 10"
    )
    assert _path(client, fast) == "order_by_distance_topk_direct"
    assert _path(client, baseline) == "generic_executor"
    assert client.execute(fast).to_dict() == client.execute(baseline).to_dict()


def test_every_distance_metric_matches_the_general_path(client):
    literal = _literal(client._query)
    # Ascending order means "smallest function value", which is nearest for the
    # distance-valued functions and for `negative_inner_product`.
    minimized = (
        "cosine_distance",
        "array_distance",
        "l2_squared_distance",
        "negative_inner_product",
        "l1_distance",
        "linf_distance",
    )
    for function in minimized:
        fast = (
            f"SELECT doc FROM docs ORDER BY {function}(emb, {literal}) LIMIT 7"
        )
        baseline = (
            f"SELECT doc FROM docs WHERE 1 = 1 "
            f"ORDER BY {function}(emb, {literal}) LIMIT 7"
        )
        assert client.execute(fast).to_dict() == client.execute(baseline).to_dict(), (
            function
        )
        assert _path(client, fast) == "order_by_distance_topk_direct", function

    # The similarity-valued functions must keep the general path: the vector
    # TopK heap returns the *most* similar rows, while `ORDER BY ... ASC` asks for
    # the least similar ones.
    for function in ("cosine_similarity", "inner_product"):
        sql = f"SELECT doc FROM docs ORDER BY {function}(emb, {literal}) LIMIT 7"
        assert _path(client, sql) == "generic_executor", function
        assert len(client.execute(sql).to_dict()) == 7, function


def test_star_projection_keeps_the_distance_order(client):
    literal = _literal(client._query)
    sql = f"SELECT * FROM docs ORDER BY cosine_distance(emb, {literal}) LIMIT 5"
    rows = client.execute(sql).to_dict()
    assert _path(client, sql) == "order_by_distance_topk_direct"
    order, _ = _cosine_order(client._vectors, client._query)
    assert [int(row["doc"]) for row in rows] == [int(index) for index in order[:5]]


def test_shapes_outside_the_fast_path_keep_the_general_plan(client):
    literal = _literal(client._query)
    plain = f"cosine_distance(emb, {literal})"
    for sql in (
        f"SELECT doc FROM docs ORDER BY {plain} DESC LIMIT 5",
        f"SELECT doc FROM docs ORDER BY {plain} LIMIT 5 OFFSET 3",
        f"SELECT doc FROM docs ORDER BY {plain}",
        f"SELECT doc FROM docs WHERE doc >= 0 ORDER BY {plain} LIMIT 5",
    ):
        assert _path(client, sql) == "generic_executor", sql
        assert len(client.execute(sql).to_dict()) > 0

    # A descending order must really be descending.
    descending = client.execute(
        f"SELECT doc, cosine_distance(emb, {literal}) AS dist "
        f"FROM docs ORDER BY dist DESC LIMIT 5"
    ).to_dict()
    assert [row["dist"] for row in descending] == sorted(
        (row["dist"] for row in descending), reverse=True
    )


def test_invalid_distance_inputs_still_report_an_error(client):
    literal = _literal(client._query)
    # A scalar column is not a vector; both the fast path and the general path
    # must reject it rather than return rows.
    with pytest.raises(RuntimeError):
        client.execute(f"SELECT doc FROM docs ORDER BY array_distance(doc, {literal}) LIMIT 5")
    # A query whose dimension does not match the column is an input error too.
    with pytest.raises(RuntimeError):
        client.execute("SELECT doc FROM docs ORDER BY array_distance(emb, [1.0, 2.0]) LIMIT 5")


def test_clean_tables_use_the_zero_copy_lane(client):
    literal = _literal(client._query)
    sql = f"SELECT doc FROM docs ORDER BY cosine_distance(emb, {literal}) LIMIT 5"
    assert _path(client, sql) == "order_by_distance_topk_direct"


def test_overlay_and_deletes_fall_back_and_stay_correct(client):
    literal = _literal(client._query)
    sql = (
        f"SELECT doc, cosine_distance(emb, {literal}) AS dist "
        "FROM docs ORDER BY dist LIMIT 10"
    )
    baseline = client.execute(sql).to_dict()
    assert _path(client, sql) == "order_by_distance_topk_direct"

    # A pending DeltaStore cell makes the persisted file an incomplete view, so
    # the zero-copy mmap lane must stand down; the result is unchanged because
    # the updated column is not part of the ordering.
    client.execute("UPDATE docs SET body = 'patched' WHERE doc = 3")
    assert client.execute(sql).to_dict() == baseline
    assert _path(client, sql) != "order_by_distance_topk_direct"

    # A deletion shifts the active row space; the direct lane declines those row
    # groups too. The answer is the previous one minus the deleted document.
    client.execute("DELETE FROM docs WHERE doc = 5")
    after = client.execute(sql).to_dict()
    assert after == [row for row in baseline if int(row["doc"]) != 5]
    assert _path(client, sql) != "order_by_distance_topk_direct"


def test_compressed_tables_fall_back_to_the_scan_lane(tmp_path):
    # Compressed row groups carry no zero-copy descriptor, so the direct lane
    # must decline and the scan lane must still return the exact TopK.
    rng = np.random.default_rng(11)
    vectors = rng.standard_normal((1_000, DIM), dtype=np.float32)
    query = rng.standard_normal(DIM).astype(np.float32)
    apex = ApexClient(str(tmp_path / "z"), drop_if_exists=True, enable_cache=False)
    try:
        apex.create_table("docs", {"doc": "int64", "emb": "float32_vector"})
        apex.use_table("docs")
        assert apex.set_compression("zstd") is not False
        apex.store(
            [{"doc": i, "emb": [float(x) for x in vectors[i]]} for i in range(1_000)]
        )
        apex.flush()
        literal = _literal(query)
        sql = f"SELECT doc FROM docs ORDER BY cosine_distance(emb, {literal}) LIMIT 5"
        assert _path(apex, sql) != "order_by_distance_topk_direct"
        order, _ = _cosine_order(vectors, query)
        assert [int(row["doc"]) for row in apex.execute(sql).to_dict()] == [
            int(index) for index in order[:5]
        ]
    finally:
        apex.close()


def test_filtered_distance_lane_matches_the_general_path(client):
    literal = _literal(client._query)
    for predicate in ("doc < 200", "doc BETWEEN 10 AND 60", "body = 'document 5'"):
        fast = (
            f"SELECT doc FROM docs WHERE {predicate} "
            f"ORDER BY cosine_distance(emb, {literal}) LIMIT 10"
        )
        # `OR 1 = 0` keeps the rows while making the predicate unfit for a row
        # selection, so the general path answers it.
        baseline = (
            f"SELECT doc FROM docs WHERE ({predicate}) OR 1 = 0 "
            f"ORDER BY cosine_distance(emb, {literal}) LIMIT 10"
        )
        assert _path(client, fast) == "filtered_distance_topk", predicate
        assert client.execute(fast).to_dict() == client.execute(baseline).to_dict(), (
            predicate
        )


def test_filtered_distance_lane_survives_a_persisted_delete(client):
    # The lane stages predicate candidates by *physical* position (the mmap
    # scanners add each row group's physical offset) and sizes its bounded heap
    # from them. A persisted deletion used to send the whole statement back to the
    # general path; the candidates are still exact, only the count that gates the
    # lane has to move into the active row space.
    literal = _literal(client._query)
    fast = (
        f"SELECT doc FROM docs WHERE doc < 400 "
        f"ORDER BY cosine_distance(emb, {literal}) LIMIT 10"
    )
    baseline = (
        f"SELECT doc FROM docs WHERE (doc < 400) OR 1 = 0 "
        f"ORDER BY cosine_distance(emb, {literal}) LIMIT 10"
    )
    client.execute("DELETE FROM docs WHERE doc = 123")
    client.flush()
    got = client.execute(fast).to_dict()
    want = client.execute(baseline).to_dict()
    assert got == want, (got, want)
    assert len(got) == 10, "a full LIMIT must still be returned"
    assert 123 not in [int(row["doc"]) for row in got]
    assert _path(client, fast) == "filtered_distance_topk"

    # Deleting a large part of the candidate range must not shrink the page: the
    # gating count has to follow the active space, not the physical one.
    ids = ", ".join(str(i) for i in range(10, 390, 3))
    client.execute(f"DELETE FROM docs WHERE doc IN ({ids})")
    client.flush()
    got = client.execute(fast).to_dict()
    want = client.execute(baseline).to_dict()
    assert got == want, (got[:3], want[:3])
    assert len(got) == 10, f"page shrank after mass deletion: {len(got)}"


def test_filtered_distance_lane_returns_the_exact_topk(client):
    query = client._query
    literal = _literal(query)
    sql = (
        f"SELECT doc FROM docs WHERE doc < 200 "
        f"ORDER BY cosine_distance(emb, {literal}) LIMIT 10"
    )
    got = [int(row["doc"]) for row in client.execute(sql).to_dict()]
    subset = list(range(200))
    values = client._vectors[subset].astype(np.float64)
    q = query.astype(np.float64)
    distances = 1.0 - (values @ q) / (
        np.linalg.norm(values, axis=1) * np.linalg.norm(q)
    )
    expected = [subset[i] for i in np.argsort(distances, kind="stable")[:10]]
    assert got == expected


def test_fts_selection_claims_a_match_shape_and_the_tail_orders_it(client):
    # The FTS row selection is tried before the distance lanes, so a `MATCH()`
    # shape is staged by position (vector column included) and the shared tail
    # orders those rows. The numeric-range lane still owns non-FTS predicates.
    rng = np.random.default_rng(5)
    vectors = rng.standard_normal((400, DIM), dtype=np.float32)
    query = rng.standard_normal(DIM).astype(np.float32)
    client.create_table(
        "fts_docs", {"doc": "int64", "body": "string", "emb": "float32_vector"}
    )
    client.use_table("fts_docs")
    client.store(
        [
            {
                "doc": index,
                "body": ("needle " if index % 40 == 0 else "") + f"text {index}",
                "emb": [float(x) for x in vectors[index]],
            }
            for index in range(400)
        ]
    )
    client.flush()
    client.init_fts(index_fields=["body"])
    literal = _literal(query)
    fast = (
        f"SELECT doc FROM fts_docs WHERE MATCH('needle') "
        f"ORDER BY cosine_distance(emb, {literal}) LIMIT 5"
    )
    baseline = (
        f"SELECT doc FROM fts_docs WHERE MATCH('needle') OR 1 = 0 "
        f"ORDER BY cosine_distance(emb, {literal}) LIMIT 5"
    )
    assert _path(client, fast) == "fts_row_selection"
    got = [int(row["doc"]) for row in client.execute(fast).to_dict()]
    assert client.execute(fast).to_dict() == client.execute(baseline).to_dict()

    # Exact truth: the cosine-nearest documents that carry the term.
    needle = [index for index in range(400) if index % 40 == 0]
    values = vectors[needle].astype(np.float64)
    q = query.astype(np.float64)
    distances = 1.0 - (values @ q) / (
        np.linalg.norm(values, axis=1) * np.linalg.norm(q)
    )
    ranking = [needle[i] for i in np.argsort(distances, kind="stable")]
    assert got == ranking[:5]

    # A LIMIT at or above the hit count routes the tail through the full sort
    # path, which used to drop the clause: its name carries the float literal
    # (`cosine_distance(emb, [0.5, ...])`) and the sort-column lookup cut it at
    # the last dot inside the literal, silently keeping scan order.
    wide = fast.replace("LIMIT 5", f"LIMIT {len(needle)}")
    assert [int(row["doc"]) for row in client.execute(wide).to_dict()] == ranking


def test_filtered_lane_declines_a_majority_or_overlay(client):
    literal = _literal(client._query)
    sql = (
        f"SELECT doc FROM docs WHERE doc >= 0 "
        f"ORDER BY cosine_distance(emb, {literal}) LIMIT 5"
    )
    # `doc >= 0` keeps the whole table, so restricting the read is pointless.
    assert _path(client, sql) == "generic_executor"
    baseline = client.execute(sql).to_dict()

    minority = (
        f"SELECT doc FROM docs WHERE doc < 200 "
        f"ORDER BY cosine_distance(emb, {literal}) LIMIT 5"
    )
    assert _path(client, minority) == "filtered_distance_topk"
    minority_baseline = client.execute(minority).to_dict()

    # A pending overlay makes the persisted base an incomplete view again.
    client.execute("UPDATE docs SET body = 'patched' WHERE doc = 1")
    assert _path(client, minority) != "filtered_distance_topk"
    assert client.execute(minority).to_dict() == minority_baseline
    assert client.execute(sql).to_dict() == baseline

    # Rows buffered but not flushed are an incomplete view as well. They carry
    # `doc >= 200`, so the candidate set (and the answer) is unchanged.
    client.store(
        [
            {
                "doc": 5_000 + offset,
                "body": "buffered",
                "emb": [float(value) for value in client._vectors[offset]],
            }
            for offset in (0, 1)
        ]
    )
    assert _path(client, minority) != "filtered_distance_topk"
    assert client.execute(minority).to_dict() == minority_baseline

    # A persisted deletion shifts the active and physical row spaces apart, so
    # the candidate indices can no longer be used for an indexed read.
    client.flush()
    client.execute("DELETE FROM docs WHERE doc = 3")
    expected = [row for row in minority_baseline if int(row["doc"]) != 3]
    assert _path(client, minority) != "filtered_distance_topk"
    assert client.execute(minority).to_dict() == expected
