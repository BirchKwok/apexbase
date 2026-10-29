"""ORDER BY <distance> on a quantized accelerator column, both directions.

The bounded distance lane picks the farthest rows with a reversed
`DistanceComputer`. Encoded (quantized) columns only have a nearest-first
scorer, so the lane deliberately declines the encoded direct path when
reversed and serves the shape from the decoded batch instead. This pins that
behavior: the result must equal the general plan (a `WHERE 1 = 1` predicate
forces it), the physical path must stay a bounded lane, and a descending
distance order must really be descending.
"""

import numpy as np
import pytest

from apexbase.client import ApexClient

ROWS = 200
DIM = 8
SEED = 20261102


@pytest.fixture(scope="module")
def quantized_table(tmp_path_factory):
    root = tmp_path_factory.mktemp("quantized_order")
    rng = np.random.default_rng(SEED)
    vectors = rng.standard_normal((ROWS, DIM)).astype(np.float32)
    query = rng.standard_normal(DIM)
    client = ApexClient(str(root), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"doc": "int64", "vec": "float32_vector"})
    client.use_table("t")
    client.store({"doc": list(range(ROWS)), "vec": [row.tolist() for row in vectors]})
    client.flush()
    column = client.create_quantized_column("vec", codec="int8")
    yield client, column, query
    client.close()


def _literal(vector):
    return "[" + ",".join(f"{float(value):.8f}" for value in vector) + "]"


def _path(client, sql):
    plan = client.execute("EXPLAIN ANALYZE " + sql).to_dict()[0]["plan"]
    return plan.split("Actual Path:", 1)[1].splitlines()[0].strip()


def test_quantized_ordering_matches_the_general_plan_in_both_directions(
    quantized_table,
):
    client, column, query = quantized_table
    literal = _literal(query)
    for direction, path in (
        ("ASC", "order_by_distance_topk_direct"),
        # The encoded direct lane declines the reversed direction and the
        # decoded batch lane takes over; both are bounded-lane paths.
        ("DESC", "order_by_distance_topk"),
    ):
        sql = (
            f"SELECT doc FROM t ORDER BY cosine_distance({column}, {literal}) "
            f"{direction} LIMIT 5"
        )
        baseline = (
            f"SELECT doc FROM t WHERE 1 = 1 "
            f"ORDER BY cosine_distance({column}, {literal}) {direction} LIMIT 5"
        )
        fast = client.execute(sql).to_dict()
        general = client.execute(baseline).to_dict()
        assert fast == general, direction
        assert _path(client, sql) == path, direction


def test_quantized_descending_order_is_really_descending(quantized_table):
    client, column, query = quantized_table
    literal = _literal(query)
    rows = client.execute(
        f"SELECT doc, cosine_distance({column}, {literal}) AS dist "
        f"FROM t ORDER BY dist DESC LIMIT 5"
    ).to_dict()
    assert len(rows) == 5
    assert [float(row["dist"]) for row in rows] == sorted(
        (float(row["dist"]) for row in rows), reverse=True
    )
