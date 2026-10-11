"""GROUP BY over a rendered expression (V5 correctness).

The grouped lanes resolve a GROUP BY entry to a batch column by name, and an
expression entry is stored as its rendered text (``ABS(score - 1.5)``). Stripping
a table qualifier at the last ``.`` therefore cut the name inside the float
literal, no column matched, and the aggregation silently collapsed to a single
group (``ABS(score - 1.5)`` over 200 distinct values returned one row). These
tests pin the grouped result against an independent Python computation, keep the
plain-column and qualifier forms working, and cover the multi-key and HAVING
interactions.
"""

import collections
import shutil
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 600


def _rows():
    scores = [float(index) + 0.5 for index in range(ROWS)]
    return {
        "doc": list(range(ROWS)),
        "score": scores,
        "city": [f"c{index % 3}" for index in range(ROWS)],
        "name": [f"user_{index:04d}" for index in range(ROWS)],
    }


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    apex = ApexClient(root, drop_if_exists=True, enable_cache=False)
    apex.create_table("t", {"doc": "int64", "score": "float64", "city": "string", "name": "string"})
    apex.use_table("t")
    apex.store(_rows())
    apex.flush()
    try:
        yield apex
    finally:
        apex.close()
        shutil.rmtree(root, ignore_errors=True)


def _groups(client, sql):
    return [
        (list(row.values())[0], int(list(row.values())[1]))
        for row in client.execute(sql).to_dict()
    ]


def test_group_by_float_expression_matches_the_independent_truth(client):
    scores = _rows()["score"]
    truth = collections.Counter(abs(value - 1.5) for value in scores)
    inline = "SELECT ABS(score - 1.5) AS b, COUNT(*) AS n FROM t GROUP BY ABS(score - 1.5)"
    alias = "SELECT ABS(score - 1.5) AS b, COUNT(*) AS n FROM t GROUP BY b"
    inline_groups = dict(_groups(client, inline))
    assert inline_groups == dict(_groups(client, alias))
    assert inline_groups == dict(truth)
    # The shape that used to collapse to a single group.
    assert len(inline_groups) == len(truth) > 1


def test_group_by_integer_expression_still_matches(client):
    scores = _rows()["score"]
    truth = collections.Counter(abs(value - 2) for value in scores)
    groups = dict(
        _groups(
            client,
            "SELECT ABS(score - 2) AS b, COUNT(*) AS n FROM t GROUP BY ABS(score - 2)",
        )
    )
    assert groups == dict(truth)


def test_two_key_group_by_with_an_expression(client):
    rows = _rows()
    truth = collections.Counter(
        (city, abs(value - 1.5)) for city, value in zip(rows["city"], rows["score"])
    )
    result = client.execute(
        "SELECT city, ABS(score - 1.5) AS b, COUNT(*) AS n "
        "FROM t GROUP BY city, ABS(score - 1.5)"
    ).to_dict()
    groups = {
        (row["city"], row["b"]): int(row["n"]) for row in result
    }
    assert len(groups) == len(truth)
    assert all(groups[key] == count for key, count in truth.items()), sorted(groups)[:4]


def test_group_by_expression_with_having_and_argument_ordering(client):
    # HAVING forces the extra-aggregate injection path around the group keys.
    groups = _groups(
        client,
        "SELECT LENGTH(name) AS l, COUNT(*) AS n FROM t GROUP BY LENGTH(name) "
        "HAVING COUNT(*) > 100",
    )
    # `user_0000` is `user_` (5) plus four digits.
    assert [label for label, _ in groups] == [9]
    assert [count for _, count in groups] == [ROWS]


def test_qualified_and_plain_group_columns_are_unchanged(client):
    plain = dict(_groups(client, "SELECT city, COUNT(*) AS n FROM t GROUP BY city"))
    qualified = dict(
        _groups(client, "SELECT city, COUNT(*) AS n FROM t GROUP BY t.city")
    )
    assert plain == qualified == {"c0": ROWS // 3, "c1": ROWS // 3, "c2": ROWS // 3}
