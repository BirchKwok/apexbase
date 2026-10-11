"""Index lookups must agree with scans while an overlay is visible.

A secondary index covers the persisted row groups only. The executor's index
route therefore stands down whenever an overlay is visible — an append delta,
buffered rows or DeltaStore cell updates — and the merged scan answers instead;
otherwise an indexed equality silently returned only the indexed rows while
`count(*)` and full scans saw the rest. The Python `store()` shapes publish their
rows through index-maintaining write paths, so the overlay case exercised here is
a pending cell update; the raw buffered-row case is covered by the storage-level
test `index_route_stands_down_while_an_overlay_is_visible`.

Persisted deletions are the control: they *are* reflected by the index and must
keep working (including through the same fallback).
"""

import shutil
import tempfile

from apexbase import ApexClient

ROWS = 5_000
APPENDED = 900_001


def _fixture(index_columns=("a",)):
    root = tempfile.mkdtemp(prefix="apexbase_index_overlay_")
    client = ApexClient(root, drop_if_exists=True, enable_cache=False)
    client.create_table("t", schema={"a": "int64", "s": "string"})
    client.use_table("t")
    client.store(
        {
            "a": list(range(ROWS)),
            "s": [f"v{i % 10}" for i in range(ROWS)],
        }
    )
    client.flush()
    for column in index_columns:
        client.execute(f"CREATE INDEX idx_{column} ON t({column})")
    return root, client


def _actual_path(client, sql):
    plan = client.execute("EXPLAIN ANALYZE " + sql).to_dict()[0]["plan"]
    return [line.strip() for line in plan.splitlines() if "Actual Path" in line][0]


def _values(client, sql, column="a"):
    return [row[column] for row in client.execute(sql).to_dict()]


def test_clean_view_keeps_the_index_route():
    root, client = _fixture()
    try:
        assert "index_accelerated_read" in _actual_path(client, "SELECT a FROM t WHERE a = 2500")
        assert _values(client, "SELECT a FROM t WHERE a = 2500") == [2500]
        assert _values(client, "SELECT a FROM t WHERE a IN (2500, 2501)") == [2500, 2501]
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_indexed_reads_agree_with_scans_after_appends():
    root, client = _fixture(index_columns=("a", "s"))
    try:
        client.store({"a": APPENDED, "s": "brand_new"})
        client.store({"a": APPENDED + 1, "s": "brand_new"})

        # Covering projection, full row, IN and a string index must all see the
        # appended rows once they are published.
        assert _values(client, f"SELECT a FROM t WHERE a = {APPENDED}") == [APPENDED]
        assert _values(client, f"SELECT * FROM t WHERE a = {APPENDED}") == [APPENDED]
        assert _values(
            client, f"SELECT a FROM t WHERE a IN ({APPENDED}, {APPENDED + 1})"
        ) == [APPENDED, APPENDED + 1]
        assert sorted(_values(client, "SELECT a FROM t WHERE s = 'brand_new'")) == [
            APPENDED,
            APPENDED + 1,
        ]
        assert _values(client, "SELECT a FROM t WHERE a = 2500") == [2500]

        # Flushing (the rows move into the append sidecar or the base) changes
        # nothing about what an indexed equality sees.
        client.flush()
        assert _values(client, f"SELECT a FROM t WHERE a = {APPENDED}") == [APPENDED]
        assert _values(client, "SELECT a FROM t WHERE a = 2500") == [2500]
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_pending_cell_updates_do_not_hide_behind_the_index():
    root, client = _fixture()
    try:
        # A DeltaStore update on a non-indexed column makes the view non-clean
        # while the indexed predicate runs.
        client.execute("UPDATE t SET s = 'rewritten' WHERE a = 2500")
        got = client.execute("SELECT s FROM t WHERE a = 2500").to_dict()
        assert got == [{"s": "rewritten"}], got
        # An update of the indexed column itself must move the posting.
        client.execute("UPDATE t SET a = 999999 WHERE a = 2501")
        assert _values(client, "SELECT a FROM t WHERE a = 999999") == [999999]
        assert _values(client, "SELECT a FROM t WHERE a = 2501") == []
        assert _values(client, "SELECT a FROM t WHERE a >= 999999") == [999999]
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_persisted_deletions_stay_hidden_from_indexed_equality():
    root, client = _fixture()
    try:
        client.execute("DELETE FROM t WHERE a < 50")
        assert _values(client, "SELECT a FROM t WHERE a = 10") == []
        assert _values(client, "SELECT a FROM t WHERE a IN (5, 60)") == [60]
        # With an append on top the fallback must keep hiding them as well.
        client.store({"a": APPENDED, "s": "n1"})
        assert _values(client, "SELECT a FROM t WHERE a = 10") == []
        assert _values(client, f"SELECT a FROM t WHERE a = {APPENDED}") == [APPENDED]
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)
