"""Reads after a predicate SQL UPDATE must stay correct *and* on the fast lanes.

Two regressions are pinned here.

1. Correctness (already correct at HEAD, kept as a guard): after
   ``UPDATE t SET age=43 WHERE name='user_12345'`` every read path - the native
   ``retrieve(_id)`` point read, the SQL ``_id`` point read, indexed ``WHERE``
   lookups, full scans and ``COUNT(*)`` - must return the new value, must hide the
   old value, and must keep agreeing after a ``DELETE`` and after ``REINDEX``.

2. Performance (broken at HEAD): the predicate UPDATE records cell overrides in a
   persistent DeltaStore.  ``retrieve(_id)`` and indexed lookups then abandon
   their fast lanes and fall back to a merged scan, so both become ~17-20x slower
   until ``REINDEX`` merges the sidecar.  The fix applies the pending cell
   overrides in the point-read lane and keeps the index lane eligible, so the
   post-UPDATE p50 must stay close to the pre-UPDATE p50.  Thresholds are loose
   (4x point read, 5x indexed lookup) to absorb machine jitter, while the current
   bug shape (~20x) fails them clearly.  Measurements run after the correctness
   assertions, on the same client, as the median of 30 samples after warmup.
"""

import shutil
import statistics
import tempfile
import time

import pytest

from apexbase import ApexClient

ROWS = 20_000
UPDATED_NAME = "user_12345"
# Implicit _id starts at 1 for a freshly stored table, so the row index maps to
# _id = index + 1.
UPDATED_ROW_INDEX = 12_345
UPDATED_ROW_ID = UPDATED_ROW_INDEX + 1
NEW_AGE = 43
OLD_AGE = UPDATED_ROW_INDEX % 50  # 45
PERF_SAMPLES = 30
PERF_WARMUP = 5


def _fixture():
    root = tempfile.mkdtemp(prefix="apexbase_overlay_read_")
    client = ApexClient(root, enable_cache=False, durability="fast")
    client.create_table("t", schema={"name": "string", "age": "int64"})
    client.use_table("t")
    client.store(
        {
            "name": [f"user_{i}" for i in range(ROWS)],
            "age": [i % 50 for i in range(ROWS)],
        }
    )
    client.flush()
    client.execute("CREATE INDEX idx_age ON t(age)")
    client.execute("CREATE INDEX idx_name ON t(name)")
    return root, client


def _rows(client, sql, params=None):
    return client.execute(sql, params=params).to_dict()


def _count(client, sql):
    return client.execute(sql).scalar()


def _row_id(client, name):
    rows = client.execute(
        f"SELECT _id FROM t WHERE name = '{name}'", show_internal_id=True
    ).to_dict()
    assert len(rows) == 1, rows
    return rows[0]["_id"]


def _median_ms(fn):
    """Warm up, then return (median p50 in ms, last value) over 30 timed calls."""
    for _ in range(PERF_WARMUP):
        fn()
    timings = []
    value = None
    for _ in range(PERF_SAMPLES):
        started = time.perf_counter_ns()
        value = fn()
        timings.append(time.perf_counter_ns() - started)
    return statistics.median(timings) / 1_000_000.0, value


def test_read_paths_return_the_new_values_after_update_and_delete():
    root, client = _fixture()
    try:
        row_id = _row_id(client, UPDATED_NAME)
        assert row_id == UPDATED_ROW_ID, (row_id, UPDATED_ROW_ID)
        before = client.retrieve(row_id)
        assert before is not None and before["age"] == OLD_AGE, before
        assert before["name"] == UPDATED_NAME, before

        client.execute(f"UPDATE t SET age = {NEW_AGE} WHERE name = '{UPDATED_NAME}'")

        # Native point read sees the pending cell override.
        updated = client.retrieve(row_id)
        assert updated is not None, updated
        assert updated["name"] == UPDATED_NAME, updated
        assert updated["age"] == NEW_AGE, updated
        # A neighbouring row is untouched.
        neighbour = client.retrieve(UPDATED_ROW_ID + 1)
        assert neighbour is not None, neighbour
        assert neighbour["name"] == "user_12346", neighbour
        assert neighbour["age"] == 12_346 % 50, neighbour

        # SQL point read by _id (the parameterized shape used by callers).
        assert _rows(client, "SELECT name, age FROM t WHERE _id = ?", [row_id]) == [
            {"name": UPDATED_NAME, "age": NEW_AGE}
        ], _rows(client, "SELECT name, age FROM t WHERE _id = ?", [row_id])

        # The old value no longer matches the row; the new one does.
        old_rows = _rows(client, f"SELECT name FROM t WHERE age = {OLD_AGE}")
        assert UPDATED_NAME not in [row["name"] for row in old_rows], "old value still matches"
        new_rows = _rows(client, f"SELECT name FROM t WHERE age = {NEW_AGE}")
        assert UPDATED_NAME in [row["name"] for row in new_rows], "new value not found"
        # Indexed lookup on a non-updated column still finds the row.
        assert _rows(
            client, f"SELECT age FROM t WHERE name = '{UPDATED_NAME}'"
        ) == [{"age": NEW_AGE}]

        # Scan path agrees with COUNT(*) and with the expected value.
        expected_43 = ROWS // 50 + 1  # 400 rows with i%50==43, plus the updated row
        counted = _count(client, f"SELECT COUNT(*) FROM t WHERE age = {NEW_AGE}")
        scanned = sum(
            1 for row in _rows(client, "SELECT age FROM t") if row["age"] == NEW_AGE
        )
        assert counted == scanned == expected_43, (counted, scanned, expected_43)

        # LIMIT queries read index candidates in id order and may stop early:
        # the first `limit` answers must equal the full predicate's prefix, and
        # the overlaid row must still be found by the unbounded form.
        limited = _rows(
            client, f"SELECT name, age FROM t WHERE age = {NEW_AGE} LIMIT 100"
        )
        full = _rows(client, f"SELECT name, age FROM t WHERE age = {NEW_AGE}")
        assert len(limited) == 100, len(limited)
        assert limited == full[:100], (limited[:2], full[:2])
        assert UPDATED_NAME in [row["name"] for row in full]
        assert UPDATED_NAME not in [
            row["name"] for row in _rows(client, f"SELECT name FROM t WHERE age = {OLD_AGE}")
        ]

        # DELETE hides the row from every lane.
        client.execute(f"DELETE FROM t WHERE name = '{UPDATED_NAME}'")
        assert client.retrieve(row_id) is None
        assert _rows(client, "SELECT name, age FROM t WHERE _id = ?", [row_id]) == []
        assert _rows(client, f"SELECT name FROM t WHERE name = '{UPDATED_NAME}'") == []
        assert UPDATED_NAME not in [
            row["name"]
            for row in _rows(client, f"SELECT name FROM t WHERE age = {NEW_AGE}")
        ]
        assert _count(client, "SELECT COUNT(*) FROM t") == ROWS - 1

        # REINDEX (which materializes the sidecar) must not change any answer.
        client.execute("REINDEX t")
        assert client.retrieve(row_id) is None
        assert _rows(client, "SELECT name, age FROM t WHERE _id = ?", [row_id]) == []
        assert _rows(client, f"SELECT age FROM t WHERE name = '{UPDATED_NAME}'") == []
        assert _count(client, "SELECT COUNT(*) FROM t") == ROWS - 1
        assert (
            _count(client, f"SELECT COUNT(*) FROM t WHERE age = {NEW_AGE}") == expected_43 - 1
        )
        assert client.retrieve(UPDATED_ROW_ID + 1) is not None
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


@pytest.mark.perf
def test_point_and_index_reads_stay_fast_after_a_predicate_update():
    """A predicate UPDATE must not push point/index reads off their fast lanes."""
    root, client = _fixture()
    try:
        row_id = _row_id(client, UPDATED_NAME)

        def retrieve():
            return client.retrieve(row_id)

        def indexed_lookup():
            return client.execute(f"SELECT name FROM t WHERE name = '{UPDATED_NAME}'").to_dict()

        def indexed_limit():
            return client.execute(
                f"SELECT name FROM t WHERE age = {NEW_AGE} LIMIT 100"
            ).to_dict()

        # Sanity-check both reads before timing anything.
        assert retrieve()["age"] == OLD_AGE
        assert indexed_lookup() == [{"name": UPDATED_NAME}]

        retrieve_before_ms, _ = _median_ms(retrieve)
        lookup_before_ms, _ = _median_ms(indexed_lookup)
        limit_before_ms, _ = _median_ms(indexed_limit)

        client.execute(f"UPDATE t SET age = {NEW_AGE} WHERE name = '{UPDATED_NAME}'")

        # Correctness first; the timing block is fixed to run after it.
        after_row = retrieve()
        assert after_row is not None and after_row["age"] == NEW_AGE, after_row
        assert indexed_lookup() == [{"name": UPDATED_NAME}]
        assert _rows(client, "SELECT name, age FROM t WHERE _id = ?", [row_id]) == [
            {"name": UPDATED_NAME, "age": NEW_AGE}
        ]

        retrieve_after_ms, _ = _median_ms(retrieve)
        lookup_after_ms, _ = _median_ms(indexed_lookup)
        limit_after_ms, _ = _median_ms(indexed_limit)

        # The bug shape is ~17-20x; these loose thresholds leave room for jitter
        # but fail loudly on a fallback to the merged-scan lanes.
        assert retrieve_after_ms <= 4.0 * retrieve_before_ms, (
            "retrieve() fell off the point-read fast lane after UPDATE: "
            f"{retrieve_before_ms:.5f} ms -> {retrieve_after_ms:.5f} ms "
            f"({retrieve_after_ms / retrieve_before_ms:.1f}x, allowed 4x)"
        )
        assert limit_after_ms <= 8.0 * limit_before_ms, (
            "indexed LIMIT query materialized every candidate after UPDATE: "
            f"{limit_before_ms:.5f} ms -> {limit_after_ms:.5f} ms "
            f"({limit_after_ms / limit_before_ms:.1f}x, allowed 8x)"
        )
        assert lookup_after_ms <= 5.0 * lookup_before_ms, (
            "indexed lookup fell off the index lane after UPDATE: "
            f"{lookup_before_ms:.5f} ms -> {lookup_after_ms:.5f} ms "
            f"({lookup_after_ms / lookup_before_ms:.1f}x, allowed 5x)"
        )
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)
