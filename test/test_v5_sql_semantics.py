"""V5 S5: SQL semantics convergence D6/D7/D8.

D6 -- `SELECT DISTINCT *` dedupes by the visible columns (the internal `_id`
      never makes otherwise identical rows distinct); explicit `DISTINCT col`
      keeps its existing semantics.
D7 -- duplicate projection aliases are made unique with a stable suffix
      (`x`, `x_1`, `x_2`, ... skipping names already in the output).
D8 -- `ORDER BY <alias>` sorts in the RIGHT JOIN path (and the other join /
      single-table paths).
"""

import pytest

from apexbase import ApexClient


def _client(tmp_path, name="semantics"):
    return ApexClient(str(tmp_path / name), drop_if_exists=True, enable_cache=False)


def _distinct_table(client):
    client.create_table("dup", {"a": "int64", "b": "string"})
    client.store({"a": [1, 1, 2, 2, 1], "b": ["x", "x", "y", "y", "x"]})
    client.flush()


def test_d6_distinct_star_uses_visible_columns(tmp_path):
    client = _client(tmp_path)
    try:
        _distinct_table(client)
        rows = client.execute("SELECT DISTINCT * FROM dup").to_dict()
        assert rows == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

        # `_id` is not part of the DISTINCT key, so showing it keeps one
        # representative row per visible combination.
        with_id = client.execute(
            "SELECT DISTINCT * FROM dup", show_internal_id=True
        ).to_dict()
        assert len(with_id) == 2
        assert [row["a"] for row in with_id] == [1, 2]
        assert len({row["_id"] for row in with_id}) == 2

        # Explicit DISTINCT projections keep their semantics.
        assert client.execute("SELECT DISTINCT a FROM dup").to_dict() == [
            {"a": 1},
            {"a": 2},
        ]
        assert client.execute("SELECT DISTINCT a, b FROM dup").to_dict() == [
            {"a": 1, "b": "x"},
            {"a": 2, "b": "y"},
        ]
        # EXCLUDE / COLUMNS star variants dedupe on their visible columns too.
        assert client.execute(
            "SELECT DISTINCT * EXCLUDE (b) FROM dup"
        ).to_dict() == [{"a": 1}, {"a": 2}]
        assert client.execute(
            "SELECT DISTINCT COLUMNS('^a$') FROM dup"
        ).to_dict() == [{"a": 1}, {"a": 2}]

        # A wider low-cardinality table still dedupes across all visible
        # columns (dictionary fast path included).
        client.create_table("wide_dup", {"a": "int64", "b": "string", "c": "float64"})
        n = 4000
        client.store(
            {
                "a": [i % 3 for i in range(n)],
                "b": [f"v{i % 4}" for i in range(n)],
                "c": [float(i % 2) for i in range(n)],
            }
        )
        client.flush()
        assert len(client.execute("SELECT DISTINCT * FROM wide_dup").to_dict()) == 12
    finally:
        client.close()


def test_d7_duplicate_projection_aliases_are_unique(tmp_path):
    client = _client(tmp_path)
    try:
        _distinct_table(client)
        rows = client.execute("SELECT a AS x, b AS x FROM dup LIMIT 1").to_dict()
        assert rows == [{"x": 1, "x_1": "x"}]
        assert client.execute("SELECT a AS x, b AS x FROM dup").to_arrow().column_names == [
            "x",
            "x_1",
        ]

        # A suffix that collides with an explicit later name advances instead of
        # overwriting it (stable, per-query deterministic).
        rows = client.execute(
            "SELECT a AS x, b AS x, a AS x_1 FROM dup LIMIT 1"
        ).to_dict()
        assert rows == [{"x": 1, "x_1": "x", "x_1_1": 1}]

        # Expression aliases are uniquified the same way.
        rows = client.execute(
            "SELECT a + 1 AS y, a + 2 AS y FROM dup LIMIT 1"
        ).to_dict()
        assert rows == [{"y": 2, "y_1": 3}]

        # Re-running the same query yields the same names (no query-order
        # dependence).
        assert client.execute("SELECT a AS x, b AS x FROM dup").to_arrow().column_names == [
            "x",
            "x_1",
        ]
    finally:
        client.close()


def test_d8_order_by_alias_in_join_paths(tmp_path):
    client = _client(tmp_path)
    try:
        client.create_table("l", {"id": "int64", "v": "int64"})
        client.store({"id": [1, 2, 3], "v": [10, 20, 30]})
        client.create_table("r", {"id": "int64", "w": "int64"})
        # Deliberately unsorted so an ignored ORDER BY is visible.
        client.store({"id": [4, 2, 3], "w": [400, 200, 300]})
        client.flush()

        # RIGHT JOIN: unmatched right rows are preserved; alias ordering must
        # sort the full result (NULL lv sorts first for DESC).
        asc = client.execute(
            "SELECT l.v AS lv, r.w AS rw FROM l RIGHT JOIN r ON l.id = r.id "
            "ORDER BY rw"
        ).to_dict()
        assert [row["rw"] for row in asc] == [200, 300, 400]
        desc = client.execute(
            "SELECT l.v AS lv, r.w AS rw FROM l RIGHT JOIN r ON l.id = r.id "
            "ORDER BY lv DESC"
        ).to_dict()
        assert [row["lv"] for row in desc] == [None, 30, 20]

        # INNER / LEFT joins share the resolution.
        inner = client.execute(
            "SELECT l.v AS lv, r.w AS rw FROM l INNER JOIN r ON l.id = r.id "
            "ORDER BY rw DESC"
        ).to_dict()
        assert [row["rw"] for row in inner] == [300, 200]
        left = client.execute(
            "SELECT l.v AS lv, r.w AS rw FROM l LEFT JOIN r ON l.id = r.id "
            "ORDER BY rw DESC"
        ).to_dict()
        # Arrow's default ordering puts NULLs first for DESC.
        assert [row["rw"] for row in left] == [None, 300, 200]

        # Single-table projections resolve aliases the same way.
        single = client.execute("SELECT l.v AS lv FROM l ORDER BY lv DESC").to_dict()
        assert [row["lv"] for row in single] == [30, 20, 10]
    finally:
        client.close()
