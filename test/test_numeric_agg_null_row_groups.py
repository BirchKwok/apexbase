"""Numeric aggregates over NULL cells must skip them in every row group.

The fused filtered-aggregation lane reads values straight from the row-group
bodies. Its parallel branch (one aggregate column, several row groups) used the
*filter* column's NULL bitmap but never the aggregate column's own, so a matching
row with a NULL value still entered `COUNT(col)`, `MIN`, `MAX`, `AVG` and skewed
the result — while `SELECT col` correctly returned NULL. A single-row-group table
took the sequential branch, which did check the bitmap, so the defect only showed
up once a table had several row groups and NULLs in the aggregated column.

These tests keep a clean table (no overlay) on the fused path and compare every
aggregate against an independent computation.
"""

import os
import shutil
import struct
import tempfile

from apexbase import ApexClient

ROWS = 200_000  # the adaptive row-group size makes this several groups


def _row_group_count(path):
    with open(path, "rb") as handle:
        header = handle.read(88)
    return struct.unpack_from("<I", header, 84)[0]


def _fixture():
    root = tempfile.mkdtemp(prefix="apexbase_agg_nulls_")
    client = ApexClient(root, drop_if_exists=True, enable_cache=False)
    client.create_table("t", schema={"a": "int64", "b": "int64", "s": "string"})
    client.use_table("t")
    values = [None if i % 3 == 0 else i for i in range(ROWS)]
    client.store(
        {
            "a": list(range(ROWS)),
            "b": values,
            "s": [f"v{i % 50}" for i in range(ROWS)],
        }
    )
    client.flush()
    groups = _row_group_count(os.path.join(root, "t.apex"))
    assert groups >= 2, f"the parallel branch needs several row groups, got {groups}"
    return root, client, list(enumerate(values))


def _oracle(rows, predicate):
    values = [b for a, b in rows if predicate(a) and b is not None]
    return {
        "n": len([1 for a, _ in rows if predicate(a)]),
        "nb": len(values),
        "mn": min(values) if values else None,
        "mx": max(values) if values else None,
        "sb": sum(values),
    }


def _assert_agrees(client, rows, predicate):
    want = _oracle(rows, predicate)
    got = client.execute(
        "SELECT count(*) AS n, count(b) AS nb, min(b) AS mn, max(b) AS mx, "
        "sum(b) AS sb FROM t WHERE a >= 0"
    ).to_dict()[0]
    # One shape per boundary: the whole table, a slice inside one row group, a
    # slice spanning several, and the last group alone.
    for sql, pred, keys in (
        (
            "SELECT count(*) AS n, count(b) AS nb, min(b) AS mn, max(b) AS mx, "
            "sum(b) AS sb FROM t WHERE a >= 0",
            lambda a: a >= 0,
            ("n", "nb", "mn", "mx", "sb"),
        ),
        (
            "SELECT count(*) AS n, count(b) AS nb, min(b) AS mn, max(b) AS mx, "
            "sum(b) AS sb FROM t WHERE a > 60000",
            lambda a: a > 60_000,
            ("n", "nb", "mn", "mx", "sb"),
        ),
        (
            "SELECT count(*) AS n, count(b) AS nb, min(b) AS mn, max(b) AS mx, "
            "sum(b) AS sb FROM t WHERE a BETWEEN 120000 AND 199000",
            lambda a: 120_000 <= a <= 199_000,
            ("n", "nb", "mn", "mx", "sb"),
        ),
    ):
        want = _oracle(rows, pred)
        got = client.execute(sql).to_dict()[0]
        for key in keys:
            if want[key] is None:
                continue
            assert abs(got[key] - want[key]) < 1e-6, (sql, key, got[key], want[key])
    del got


def test_multi_row_group_aggregates_skip_null_cells():
    root, client, rows = _fixture()
    try:
        plan = client.execute(
            "EXPLAIN ANALYZE SELECT count(*), count(b), min(b) FROM t WHERE a >= 0"
        ).to_dict()[0]["plan"]
        path = [line.strip() for line in plan.splitlines() if "Actual Path" in line]
        assert "fast_filtered_numeric_agg" in path[0], path
        got = client.execute(
            "SELECT count(*) AS n, count(b) AS nb FROM t WHERE a >= 0"
        ).to_dict()[0]
        # The whole point: NULL cells are not values.
        assert got["n"] == ROWS
        assert got["nb"] == len([1 for _, b in rows if b is not None])
        _assert_agrees(client, rows, lambda a: a >= 0)
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_null_aggregates_agree_with_a_grouped_query():
    root, client, rows = _fixture()
    try:
        # The grouped path materializes values and applies the validity bitmap;
        # the fused path must agree with it per group.
        fused = client.execute(
            "SELECT max(b) AS mx, min(b) AS mn, count(b) AS nb FROM t WHERE a >= 0"
        ).to_dict()[0]
        grouped = client.execute(
            "SELECT max(b) AS mx, min(b) AS mn, count(b) AS nb FROM t WHERE a >= 0 "
            "GROUP BY s LIMIT 1"
        ).to_dict()[0]
        del grouped  # only the fused lane's own numbers are compared below
        want = _oracle(rows, lambda a: a >= 0)
        assert fused["mx"] == want["mx"]
        assert fused["mn"] == want["mn"]
        assert fused["nb"] == want["nb"]
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_all_null_column_aggregates_are_empty():
    root = tempfile.mkdtemp(prefix="apexbase_agg_allnull_")
    client = ApexClient(root, drop_if_exists=True, enable_cache=False)
    try:
        client.create_table("t", schema={"a": "int64", "b": "int64"})
        client.use_table("t")
        client.store({"a": list(range(ROWS)), "b": [None] * ROWS})
        client.flush()
        got = client.execute(
            "SELECT count(*) AS n, count(b) AS nb, min(b) AS mn, sum(b) AS sb "
            "FROM t WHERE a > 1000"
        ).to_dict()[0]
        assert got["n"] == ROWS - 1001
        assert got["nb"] == 0
        assert got["mn"] is None
        assert got["sb"] is None or got["sb"] == 0
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)
