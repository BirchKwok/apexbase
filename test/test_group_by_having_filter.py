"""A filtered `GROUP BY ... HAVING` must never abort the query.

`SELECT g, COUNT(*) ... WHERE s = 'absent' GROUP BY g HAVING COUNT(*) > 0` used to
panic: the pushed-down predicate pruned every row group, so the batch-group kernel
never built its key lane, and its `finish` unwrapped that missing lane. The kernel
now stands down when no morsel was consumed -- the single-batch path answers the
query -- and `finish` reports an error instead of panicking if a lane is ever
missing for another reason.
"""

import tempfile

import pytest

from apexbase.client import ApexClient

ROWS = 50_000


def _client(tmp, *, indexed, compression):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"k": "int64", "s": "string", "g": "string"})
    client.use_table("t")
    if compression:
        assert client.set_compression(compression) is not False
    client.store({"k": list(range(ROWS)),
                  "s": [f"v{i:06d}" for i in range(ROWS)],
                  "g": [f"cat{i % 20}" for i in range(ROWS)]})
    client.flush()
    if indexed:
        client.execute("CREATE INDEX ix_s ON t (s)")
    return client


@pytest.mark.parametrize("indexed", [False, True])
@pytest.mark.parametrize("compression", [None, "zstd"])
def test_a_pruned_filtered_group_by_returns_no_rows(indexed, compression):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, indexed=indexed, compression=compression)
        try:
            # A value that cannot exist prunes every row group; the result is empty
            # rather than a panic.
            assert client.execute(
                "SELECT g, COUNT(*) AS n FROM t WHERE s = 'nope' "
                "GROUP BY g HAVING COUNT(*) > 0"
            ).to_dict() == []
            # The same shape pruned by a numeric predicate.
            assert client.execute(
                "SELECT g, COUNT(*) AS n FROM t WHERE k < 0 "
                "GROUP BY g HAVING COUNT(*) > 0"
            ).to_dict() == []
            # ORDER BY/LIMIT and a second grouping key take the same route.
            assert client.execute(
                "SELECT g, COUNT(*) AS n FROM t WHERE s = 'nope' "
                "GROUP BY g HAVING COUNT(*) > 0 ORDER BY g LIMIT 5"
            ).to_dict() == []
            assert client.execute(
                "SELECT g, s, COUNT(*) AS n FROM t WHERE s = 'nope' "
                "GROUP BY g, s HAVING COUNT(*) > 0"
            ).to_dict() == []
        finally:
            client.close()


@pytest.mark.parametrize("indexed", [False, True])
def test_a_matching_filtered_group_by_still_aggregates(indexed):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, indexed=indexed, compression=None)
        try:
            assert client.execute(
                "SELECT g, COUNT(*) AS n FROM t WHERE s = 'v010000' "
                "GROUP BY g HAVING COUNT(*) > 0"
            ).to_dict() == [{"g": "cat0", "n": 1}]
            # A filter that keeps every row keeps every group.
            groups = client.execute(
                "SELECT g, COUNT(*) AS n FROM t WHERE k >= 0 "
                "GROUP BY g HAVING COUNT(*) > 0 ORDER BY g"
            ).to_dict()
            assert len(groups) == 20
            assert sum(row["n"] for row in groups) == ROWS
        finally:
            client.close()


def test_the_same_shapes_survive_an_overlay():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, indexed=True, compression=None)
        try:
            client.execute("DELETE FROM t WHERE k < 5")
            client.flush()
            client.execute("UPDATE t SET s = 'newval' WHERE k = 10")
            client.flush()
            assert client.execute(
                "SELECT g, COUNT(*) AS n FROM t WHERE s = 'nope' "
                "GROUP BY g HAVING COUNT(*) > 0"
            ).to_dict() == []
            assert client.execute(
                "SELECT g, COUNT(*) AS n FROM t WHERE s = 'newval' "
                "GROUP BY g HAVING COUNT(*) > 0"
            ).to_dict() == [{"g": "cat10", "n": 1}]  # k = 10 lives in cat10
        finally:
            client.close()
