"""The shared tail re-evaluates the WHERE on the scan batch.

Storage-level fast paths stage only the post-filter columns (SELECT / ORDER BY /
GROUP BY), while the executor's shared tail evaluates the predicate again. A
predicate column that is neither projected nor an ordering key — ``SELECT doc
FROM t WHERE k < 100 ORDER BY abs(doc)``, a shape hybrid reranking also uses
(``WHERE k < N ORDER BY cosine_distance(...)``) — therefore left the batch
without ``k`` and the filter failed with ``Column 'k' not found``.

Any batch handed to the shared tail now carries the WHERE columns with it
(``read_matching_rows_for_tail``), so the tail finds them and pays only the
scatter for the matched rows. The whole-relation retry stays as a safety net
for callers that still prune (path ``predicate_column_reread``); these tests pin
the exact results and that the pruned shapes no longer take that retry.
"""

import shutil
import tempfile

import numpy as np
import pytest

from apexbase import ApexClient

ROWS = 4_000
DIM = 8


def _path(client, sql):
    plan = client.execute("EXPLAIN ANALYZE " + sql).to_dict()[0]["plan"]
    assert "Actual Path:" in plan, plan
    return plan.split("Actual Path:", 1)[1].splitlines()[0].strip()


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    apex = ApexClient(root, drop_if_exists=True, enable_cache=False)
    apex.create_table("t", {"doc": "int64", "k": "int64", "s": "string"})
    apex.use_table("t")
    apex.store(
        {
            "doc": list(range(ROWS)),
            "k": [i % 1000 for i in range(ROWS)],
            "s": [f"v{i % 7}" for i in range(ROWS)],
        }
    )
    apex.flush()

    rng = np.random.default_rng(20261001)
    vectors = rng.standard_normal((ROWS, DIM), dtype=np.float32)
    apex.create_table("v", {"doc": "int64", "k": "int64", "emb": "float32_vector"})
    apex.use_table("v")
    apex.store(
        [
            {"doc": index, "k": index % 1000, "emb": [float(x) for x in vectors[index]]}
            for index in range(ROWS)
        ]
    )
    apex.flush()
    apex._vectors = vectors  # type: ignore[attr-defined]
    try:
        yield apex
    finally:
        apex.close()
        shutil.rmtree(root, ignore_errors=True)


def _docs(rows):
    return [int(row["doc"]) for row in rows]


def test_order_by_expression_matches_the_projected_read(client):
    # `abs(doc)` is an ordering expression over a column the predicate does not
    # mention, so only `doc` is projected — the case that used to fail. The
    # batch now carries `k`, so the tail filters it without re-reading the table.
    pruned = "SELECT doc FROM t WHERE k < 100 ORDER BY abs(doc) DESC LIMIT 5"
    projected = "SELECT doc, k FROM t WHERE k < 100 ORDER BY abs(doc) DESC LIMIT 5"
    assert _path(client, pruned) != "predicate_column_reread"
    assert _docs(client.execute(pruned).to_dict()) == _docs(
        client.execute(projected).to_dict()
    )


def test_like_and_equality_keep_the_predicate_column_too(client):
    # The LIKE and string-equality lanes hand their batches to the tail as well.
    for pruned, projected in (
        (
            "SELECT doc FROM t WHERE s LIKE 'v1%' ORDER BY abs(doc) DESC LIMIT 5",
            "SELECT doc, s FROM t WHERE s LIKE 'v1%' ORDER BY abs(doc) DESC LIMIT 5",
        ),
        (
            "SELECT doc FROM t WHERE s = 'v3' ORDER BY abs(doc) DESC LIMIT 5",
            "SELECT doc, s FROM t WHERE s = 'v3' ORDER BY abs(doc) DESC LIMIT 5",
        ),
    ):
        assert _path(client, pruned) != "predicate_column_reread", pruned
        assert _docs(client.execute(pruned).to_dict()) == _docs(
            client.execute(projected).to_dict()
        ), pruned


def test_between_and_string_ordering_expressions(client):
    for pruned, projected in (
        (
            "SELECT doc FROM t WHERE k BETWEEN 10 AND 100 ORDER BY length(s) LIMIT 5",
            "SELECT doc, k FROM t WHERE k BETWEEN 10 AND 100 ORDER BY length(s) LIMIT 5",
        ),
        (
            "SELECT doc FROM t WHERE k < 100 ORDER BY abs(doc) LIMIT 4 OFFSET 2",
            "SELECT doc, k FROM t WHERE k < 100 ORDER BY abs(doc) LIMIT 4 OFFSET 2",
        ),
        (
            "SELECT doc FROM t WHERE k < 100 ORDER BY abs(doc) LIMIT 100000",
            "SELECT doc, k FROM t WHERE k < 100 ORDER BY abs(doc) LIMIT 100000",
        ),
    ):
        assert _docs(client.execute(pruned).to_dict()) == _docs(
            client.execute(projected).to_dict()
        ), pruned


def test_distance_rerank_with_an_unprojected_filter_column(client):
    vectors = client._vectors
    query = np.random.default_rng(7).standard_normal(DIM).astype(np.float32)
    literal = "[" + ",".join(f"{float(x):.6f}" for x in query) + "]"
    pruned = (
        f"SELECT doc FROM v WHERE k < 100 "
        f"ORDER BY cosine_distance(emb, {literal}) LIMIT 10"
    )
    projected = (
        f"SELECT doc, k FROM v WHERE k < 100 "
        f"ORDER BY cosine_distance(emb, {literal}) LIMIT 10"
    )
    # The filter-aware distance lane (V5 §6.4 item 16) now selects the
    # predicate's rows first, so this shape never reaches the shared tail's
    # filter and no re-read is needed.
    assert _path(client, pruned) == "filtered_distance_topk"
    got = _docs(client.execute(pruned).to_dict())
    assert got == _docs(client.execute(projected).to_dict())

    # Exact truth: the cosine-nearest rows among the k < 100 subset.
    subset = [index for index in range(ROWS) if index % 1000 < 100]
    values = vectors[subset].astype(np.float64)
    q = query.astype(np.float64)
    distances = 1.0 - (values @ q) / (
        np.linalg.norm(values, axis=1) * np.linalg.norm(q)
    )
    expected = [subset[i] for i in np.argsort(distances, kind="stable")[:10]]
    assert got == expected


def test_projected_predicate_column_keeps_the_scan(client):
    # With `k` in the projection the batch already carries it, so no re-read is
    # needed and the result is unchanged.
    projected = "SELECT doc, k FROM t WHERE k < 100 ORDER BY abs(doc) DESC LIMIT 5"
    assert _path(client, projected) != "predicate_column_reread"
    rows = client.execute(projected).to_dict()
    assert _docs(rows) == sorted(
        (index for index in range(ROWS) if index % 1000 < 100), reverse=True
    )[:5]
