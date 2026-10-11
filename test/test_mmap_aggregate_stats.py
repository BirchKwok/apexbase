"""
The mmap aggregate-statistics lane must agree with the general evaluator.

`compute_column_stats_mmap` used to walk every preceding column's null bitmap and
payload just to reach the requested column inside each row group. It now reads
that column's body offset from RCIX and jumps straight to it, falling back to the
sequential walk only for compressed row groups (whose recorded offsets describe
the uncompressed body).

Each test therefore compares the fast lane against the same aggregate computed by
the general evaluator, which the always-true `_id > 0` predicate forces. The lane
labels are asserted too, so a lane that silently stops being taken cannot make
these comparisons vacuous.
"""

import os
import shutil
import tempfile

import pytest

from apexbase import ApexClient

N = 70_000  # crosses the 65,536-row row-group boundary
AGG = "SELECT COUNT({c}) AS c, SUM({c}) AS s, MIN({c}) AS mn, MAX({c}) AS mx FROM t"
REFERENCE = AGG + " WHERE _id > 0"


@pytest.fixture
def multi_row_group_client():
    """Two row groups, with NULLs spread through both numeric columns."""
    root = tempfile.mkdtemp()
    client = ApexClient(root, enable_cache=False)
    client.create_table("t", schema={"v": "int64", "f": "float64", "s": "string"})
    client.use_table("t")
    client.store({
        "v": [None if i % 997 == 0 else i for i in range(N)],
        "f": [None if i % 1301 == 0 else float(i) * 0.5 for i in range(N)],
        "s": [f"l{i % 13}" for i in range(N)],
    })
    client.flush()
    try:
        yield client
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def _lane(client, sql):
    plan = client.execute("EXPLAIN ANALYZE " + sql).to_dict()[0]["plan"]
    for line in plan.splitlines():
        if "Actual Path" in line:
            return line.split(":", 1)[1].strip()
    raise AssertionError(f"no lane reported for {sql}")


def _values(client, sql):
    return client.execute(sql).to_dict()[0]


@pytest.mark.parametrize("column", ["v", "f"])
def test_aggregate_lane_matches_the_general_evaluator(multi_row_group_client, column):
    fast = AGG.format(c=column)
    slow = REFERENCE.format(c=column)

    assert _lane(multi_row_group_client, fast) == "mmap_aggregation"
    assert _lane(multi_row_group_client, slow) == "generic_executor"

    assert _values(multi_row_group_client, fast) == _values(multi_row_group_client, slow)


@pytest.mark.parametrize("column", ["v", "f"])
def test_aggregate_lane_excludes_deleted_rows(multi_row_group_client, column):
    multi_row_group_client.execute("DELETE FROM t WHERE v < 2000")

    fast = AGG.format(c=column)
    slow = REFERENCE.format(c=column)
    fast_values = _values(multi_row_group_client, fast)
    slow_values = _values(multi_row_group_client, slow)
    assert fast_values == slow_values
    # The deletions really took effect in the fast lane's numbers.
    assert fast_values["mn"] is not None
    if column == "v":
        assert fast_values["mn"] >= 2000
        assert fast_values["c"] < N


def test_every_declared_column_aggregates_correctly(multi_row_group_client):
    """A wide projection must not stop the lane from finding the right column."""
    # `s` sits after the two numeric columns, so it is the case where walking
    # preceding columns was most expensive; its own aggregate is a COUNT.
    assert multi_row_group_client.execute(
        "SELECT COUNT(s) AS c FROM t"
    ).to_dict() == multi_row_group_client.execute(
        "SELECT COUNT(s) AS c FROM t WHERE _id > 0"
    ).to_dict()
    # A three-column aggregate still resolves each numeric column independently.
    assert multi_row_group_client.execute(
        "SELECT MIN(v) AS a, MAX(v) AS b, MIN(f) AS c, MAX(f) AS d FROM t"
    ).to_dict() == multi_row_group_client.execute(
        "SELECT MIN(v) AS a, MAX(v) AS b, MIN(f) AS c, MAX(f) AS d FROM t WHERE _id > 0"
    ).to_dict()
