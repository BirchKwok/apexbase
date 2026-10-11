"""``SELECT DISTINCT <expression> AS alias`` (V5 correctness).

The DISTINCT path projected the SELECT list once (dedup needs the computed
values) and then projected the survivors *again*. An expression column resolves
against its inputs — ``UPPER(cat)`` needs ``cat`` — while the projected batch
only carries the outputs, so every ``SELECT DISTINCT <expression>`` failed with
``Column '<input>' not found`` (plain ``SELECT DISTINCT city`` was fine). The
second projection is now skipped when the dedup ran on an already-projected
batch; these tests pin the results against independent Python sets.
"""

import shutil
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 240


def _rows():
    return {
        "doc": list(range(ROWS)),
        "score": [float(index) + 0.5 for index in range(ROWS)],
        "city": [f"c{index % 4}" for index in range(ROWS)],
        "cat": [f"k{index % 7}" for index in range(ROWS)],
        "name": [f"user_{index:04d}" for index in range(ROWS)],
    }


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    apex = ApexClient(root, drop_if_exists=True, enable_cache=False)
    apex.create_table(
        "t",
        {"doc": "int64", "score": "float64", "city": "string", "cat": "string", "name": "string"},
    )
    apex.use_table("t")
    apex.store(_rows())
    apex.flush()
    try:
        yield apex
    finally:
        apex.close()
        shutil.rmtree(root, ignore_errors=True)


def _column(client, sql, name):
    return [row[name] for row in client.execute(sql).to_dict()]


def test_distinct_over_a_string_function_matches_the_set(client):
    rows = _rows()
    got = _column(client, "SELECT DISTINCT UPPER(cat) AS u FROM t", "u")
    assert sorted(got) == sorted({value.upper() for value in rows["cat"]})
    assert len(got) == len(set(got))


def test_distinct_over_arithmetic_matches_the_set(client):
    rows = _rows()
    got = _column(client, "SELECT DISTINCT score + 0.5 AS b FROM t", "b")
    assert sorted(got) == sorted({value + 0.5 for value in rows["score"]})
    got = _column(client, "SELECT DISTINCT ABS(score - 2.5) AS b FROM t", "b")
    assert sorted(got) == sorted({abs(value - 2.5) for value in rows["score"]})


def test_distinct_over_length_matches_the_set(client):
    rows = _rows()
    got = _column(client, "SELECT DISTINCT LENGTH(name) AS l FROM t", "l")
    assert sorted(got) == sorted({len(value) for value in rows["name"]})


def test_distinct_expression_with_order_limit_and_where(client):
    rows = _rows()
    truth = sorted({value.upper() for value in rows["cat"]})
    got = _column(client, "SELECT DISTINCT UPPER(cat) AS u FROM t ORDER BY u DESC LIMIT 2", "u")
    assert got == list(reversed(truth))[:2]

    filtered = [value.upper() for value in rows["cat"] if rows["doc"][rows["cat"].index(value)] >= 0]
    got = _column(client, "SELECT DISTINCT UPPER(cat) AS u FROM t WHERE doc >= 120", "u")
    assert sorted(got) == sorted({value.upper() for value in rows["cat"][120:]})
    assert filtered  # sanity: the fixture is non-empty


def test_distinct_mixes_plain_columns_and_expressions(client):
    rows = _rows()
    truth = {(city, cat.upper()) for city, cat in zip(rows["city"], rows["cat"])}
    result = client.execute(
        "SELECT DISTINCT city, UPPER(cat) AS u FROM t ORDER BY city, u"
    ).to_dict()
    got = [(row["city"], row["u"]) for row in result]
    assert sorted(got) == sorted(truth)
    assert got == sorted(got)


def test_distinct_with_a_where_clause_still_deduplicates(client):
    # The late-materialization lane projects the filtered rows and returns them
    # directly, so it used to skip the DISTINCT entirely: `WHERE doc >= 120`
    # came back with one row per match (120 rows for four distinct cities).
    rows = _rows()
    got = _column(client, "SELECT DISTINCT city FROM t WHERE doc >= 120", "city")
    assert sorted(got) == sorted({value for value in rows["city"][120:]})
    got = _column(client, "SELECT DISTINCT UPPER(cat) AS u FROM t WHERE cat <> 'zz'", "u")
    assert sorted(got) == sorted({value.upper() for value in rows["cat"]})
    # ORDER BY / LIMIT keep working on top of the deduplicated result.
    got = _column(
        client, "SELECT DISTINCT city FROM t WHERE doc >= 120 ORDER BY city DESC LIMIT 3", "city"
    )
    assert got == sorted({value for value in rows["city"][120:]}, reverse=True)[:3]
    assert _column(
        client, "SELECT COUNT(DISTINCT city) AS n FROM t WHERE doc >= 120", "n"
    ) == [len({value for value in rows["city"][120:]})]


def test_plain_distinct_is_unchanged(client):
    rows = _rows()
    got = _column(client, "SELECT DISTINCT city FROM t ORDER BY city", "city")
    assert got == sorted({value for value in rows["city"]})


def test_count_distinct_excludes_deleted_rows_and_drops_empty_values():
    """COUNT(DISTINCT col) must count values that still have a live row.

    The on-disk cardinality path is already deletion-aware (it walks the row groups and
    skips rows marked in the deletion bitmap), so a value whose *only* row is deleted has
    to disappear from the result. That case is what forbids answering from dictionary
    cardinality alone once deletions exist, and it is pinned here.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "s": "string", "n": "int64"})
        client.use_table("t")
        rows = 5_000
        client.store({
            "k": list(range(rows)),
            "s": [f"v{i % 7}" for i in range(rows)],
            "n": [None if i % 4 == 0 else i % 5 for i in range(rows)],
        })
        client.flush()

        def count_distinct(column):
            row = client.execute(f"SELECT COUNT(DISTINCT {column}) AS n FROM t").to_dict()[0]
            return int(list(row.values())[0])

        def oracle(column):
            values = set()
            for row in client.execute(f"SELECT {column} FROM t").to_dict():
                value = list(row.values())[0]
                if value is not None:
                    values.add(value)
            return len(values)

        for column in ("s", "n"):
            assert count_distinct(column) == oracle(column), column

        client.execute("DELETE FROM t WHERE k < 500")
        client.flush()
        for column in ("s", "n"):
            assert count_distinct(column) == oracle(column), column

        # v0's first occurrence is gone with the delete above; delete all of its rows and
        # the value must drop out entirely, even though the dictionary still holds it.
        client.execute("DELETE FROM t WHERE s = 'v0'")
        client.flush()
        assert count_distinct("s") == oracle("s")
        assert "v0" not in {
            list(row.values())[0]
            for row in client.execute("SELECT DISTINCT s FROM t").to_dict()
        }

        # A value added and then removed again must not linger in the cardinality.
        client.store({"k": [rows + 1], "s": ["zz"], "n": [7]})
        client.flush()
        assert count_distinct("s") == oracle("s")
        client.execute("DELETE FROM t WHERE s = 'zz'")
        client.flush()
        assert count_distinct("s") == oracle("s")

        client.execute("DELETE FROM t")
        client.flush()
        assert count_distinct("s") == 0
        assert count_distinct("n") == 0
