"""V5 S7 D13: count-only join with an expression ON clause.

The fast path evaluates only the columns referenced by the ON expressions into
join keys and counts matches instead of materializing joined rows. These tests
pin its correctness against an independent Python computation for INNER/LEFT
joins, multiple matches, NULL keys, empty sides and non-count aggregates
(which must keep their slower but correct path).
"""

from apexbase import ApexClient


def _client(tmp_path):
    return ApexClient(str(tmp_path / "join_count"), drop_if_exists=True, enable_cache=False)


def _scalar(client, sql):
    return client.execute(sql).scalar()


def test_expression_on_count_join_multiplicities(tmp_path):
    client = _client(tmp_path)
    try:
        client.create_table("b", {"k": "int64", "v": "int64"})
        client.store({"k": [1, 2, 3, 4, None], "v": [10, 20, 30, 40, 50]})
        client.create_table("dim", {"k": "int64"})
        client.store({"k": [1_000_001, 1_000_002, 1_000_002, 1_000_003, None]})
        client.flush()

        # k=1 -> one match, k=2 -> two matches, k=3 -> one, k=4/NULL -> none.
        left = "SELECT COUNT(*) AS c FROM b LEFT JOIN dim ON b.k + 1000000 = dim.k"
        inner = "SELECT COUNT(*) AS c FROM b INNER JOIN dim ON b.k + 1000000 = dim.k"
        assert _scalar(client, left) == 6  # 4 matches + 2 unmatched left rows
        assert _scalar(client, inner) == 4

        # A reversed expression side is the same join.
        reversed_sql = (
            "SELECT COUNT(*) AS c FROM b INNER JOIN dim ON dim.k = b.k + 1000000"
        )
        assert _scalar(client, reversed_sql) == 4
    finally:
        client.close()


def test_expression_on_count_join_empty_and_null_sides(tmp_path):
    client = _client(tmp_path)
    try:
        client.create_table("b", {"k": "int64"})
        client.store({"k": [1, 2, None]})
        client.create_table("dim", {"k": "int64"})
        client.flush()

        assert _scalar(
            client,
            "SELECT COUNT(*) AS c FROM b LEFT JOIN dim ON b.k + 1 = dim.k",
        ) == 3  # all left rows preserved
        assert _scalar(
            client,
            "SELECT COUNT(*) AS c FROM b INNER JOIN dim ON b.k + 1 = dim.k",
        ) == 0
    finally:
        client.close()


def test_expression_on_count_join_matches_full_join_rows(tmp_path):
    client = _client(tmp_path)
    try:
        client.create_table("l", {"x": "int64"})
        client.store({"x": [10, 20, 20, 30, 40]})
        client.create_table("r", {"y": "int64"})
        client.store({"y": [11, 21, 21, 41]})
        client.flush()

        # Independent computation with the same expression semantics.
        left = client.execute("SELECT x FROM l ORDER BY x").to_dict()
        right = client.execute("SELECT y FROM r ORDER BY y").to_dict()
        left_values = [row["x"] for row in left]
        right_values = [row["y"] for row in right]
        per_left = [
            sum(1 for other in right_values if value + 1 == other)
            for value in left_values
        ]
        expected_inner = sum(per_left)
        expected_left = sum(max(count, 1) for count in per_left)

        assert _scalar(
            client,
            "SELECT COUNT(*) AS c FROM l INNER JOIN r ON l.x + 1 = r.y",
        ) == expected_inner
        assert _scalar(
            client,
            "SELECT COUNT(*) AS c FROM l LEFT JOIN r ON l.x + 1 = r.y",
        ) == expected_left

        # COUNT(col) is not COUNT(*) and must keep the general join path.
        counted = _scalar(
            client,
            "SELECT COUNT(r.y) AS c FROM l LEFT JOIN r ON l.x + 1 = r.y",
        )
        assert counted == expected_inner
    finally:
        client.close()


def test_expression_on_count_join_falls_back_with_where(tmp_path):
    client = _client(tmp_path)
    try:
        client.create_table("b", {"k": "int64", "v": "int64"})
        client.store({"k": [1, 2, 3], "v": [10, 20, 30]})
        client.create_table("dim", {"k": "int64"})
        client.store({"k": [1_000_001, 1_000_003]})
        client.flush()

        # WHERE on the preserved side: the general path must still answer right.
        assert _scalar(
            client,
            "SELECT COUNT(*) AS c FROM b LEFT JOIN dim ON b.k + 1000000 = dim.k "
            "WHERE b.v >= 20",
        ) == 2
        # WHERE on the nullable side turns the outer join into an inner one.
        assert _scalar(
            client,
            "SELECT COUNT(*) AS c FROM b LEFT JOIN dim ON b.k + 1000000 = dim.k "
            "WHERE dim.k IS NOT NULL",
        ) == 2
    finally:
        client.close()
