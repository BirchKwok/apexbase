"""Comparisons against NULL literal are UNKNOWN, not an error.

`WHERE s = NULL` selects nothing in SQL. ApexBase used to raise
"Invalid comparison operation: Utf8 = ..." for every string column compared with the
NULL literal, while numeric columns happened to work. The result has to be all-NULL
(UNKNOWN) rather than all-false, otherwise `NOT (s = NULL)` would turn into "match
everything" instead of matching nothing.

A NULL *inside* an IN list has the same meaning: `x IN (a, NULL)` is TRUE when x = a
and UNKNOWN otherwise, so `x NOT IN (a, NULL)` matches nothing.
"""

import tempfile

import pytest

from apexbase.client import ApexClient

ROWS = 2000
NULL_EVERY = 3  # every third row has a NULL string
NULL_ROWS = len([i for i in range(ROWS) if i % NULL_EVERY == 0])


def _client(tmp, *, compressed, indexed):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"k": "int64", "s": "string", "g": "string"})
    client.use_table("t")
    if compressed:
        assert client.set_compression(compressed) is not False
    client.store({"k": list(range(ROWS)),
                  "s": [None if i % NULL_EVERY == 0 else f"v{i}" for i in range(ROWS)],
                  "g": [f"cat{i % 7}" for i in range(ROWS)]})
    client.flush()
    if indexed:
        client.execute("CREATE INDEX ix_s ON t (s)")
    return client


def _count(client, sql):
    return client.execute(sql).to_dict()[0]["n"]


@pytest.mark.parametrize("compressed", [None, "zstd"])
@pytest.mark.parametrize("indexed", [False, True])
def test_comparisons_against_null_match_nothing(compressed, indexed):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, compressed=compressed, indexed=indexed)
        try:
            for predicate in ("s = NULL", "s != NULL", "s <> NULL", "s < NULL",
                              "s > NULL", "NULL = s", "k = NULL", "NULL = NULL",
                              "s = 'v1' AND s = NULL", "NOT (s = NULL)",
                              "g = NULL"):
                assert _count(
                    client, f"SELECT COUNT(*) AS n FROM t WHERE {predicate}"
                ) == 0, predicate

            # UNKNOWN OR TRUE stays TRUE, and IS NULL is unaffected.
            assert _count(
                client, "SELECT COUNT(*) AS n FROM t WHERE s = NULL OR k >= 0"
            ) == ROWS
            assert _count(
                client, "SELECT COUNT(*) AS n FROM t WHERE s = NULL OR s IS NULL"
            ) == NULL_ROWS
            assert _count(
                client, "SELECT COUNT(*) AS n FROM t WHERE s IS NULL"
            ) == NULL_ROWS
            assert _count(
                client, "SELECT COUNT(*) AS n FROM t WHERE s IS NOT NULL"
            ) == ROWS - NULL_ROWS

            # A NULL comparison is NULL (not FALSE) in a projection too.
            rows = client.execute("SELECT (s = NULL) AS f FROM t LIMIT 3").to_dict()
            assert [row["f"] for row in rows] == [None, None, None]
        finally:
            client.close()


@pytest.mark.parametrize("compressed", [None, "zstd"])
@pytest.mark.parametrize("indexed", [False, True])
def test_null_inside_an_in_list_is_unknown(compressed, indexed):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, compressed=compressed, indexed=indexed)
        try:
            # Only the non-null members decide membership; every miss is UNKNOWN.
            assert _count(client, "SELECT COUNT(*) AS n FROM t WHERE s IN (NULL)") == 0
            assert _count(client, "SELECT COUNT(*) AS n FROM t WHERE s NOT IN (NULL)") == 0
            assert _count(
                client, "SELECT COUNT(*) AS n FROM t WHERE s IN ('v1', NULL)"
            ) == 1
            assert _count(
                client, "SELECT COUNT(*) AS n FROM t WHERE s NOT IN ('v1', NULL)"
            ) == 0
            assert _count(
                client, "SELECT COUNT(*) AS n FROM t WHERE s IN ('v1', 'v2', NULL)"
            ) == 2
            # Lists without NULL keep their original meaning.
            assert _count(client, "SELECT COUNT(*) AS n FROM t WHERE s IN ('v1')") == 1
            assert _count(
                client, "SELECT COUNT(*) AS n FROM t WHERE s NOT IN ('v1')"
            ) == ROWS - 1
            # Integer and float members take the other two code paths.
            assert _count(client, "SELECT COUNT(*) AS n FROM t WHERE k IN (NULL)") == 0
            assert _count(client, "SELECT COUNT(*) AS n FROM t WHERE k IN (1, NULL)") == 1
            assert _count(client, "SELECT COUNT(*) AS n FROM t WHERE k NOT IN (1, NULL)") == 0
            assert _count(client, "SELECT COUNT(*) AS n FROM t WHERE k IN (1, 2)") == 2
            assert _count(
                client, "SELECT COUNT(*) AS n FROM t WHERE s IN (NULL) OR k >= 1999"
            ) == 1
        finally:
            client.close()


def test_a_null_member_works_on_the_float_path():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("u", schema={"f": "float64"})
        client.use_table("u")
        client.store({"f": [1.5, 2.5, 3.5]})
        client.flush()
        try:
            assert _count(client, "SELECT COUNT(*) AS n FROM u WHERE f IN (1.5, NULL)") == 1
            assert _count(client, "SELECT COUNT(*) AS n FROM u WHERE f NOT IN (1.5, NULL)") == 0
        finally:
            client.close()
