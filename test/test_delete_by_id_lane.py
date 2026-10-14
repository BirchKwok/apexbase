"""`DELETE ... WHERE _id = X` must reach the physical-row lane.

`_id` is a synthetic column: it is not in the schema, so the numeric-range lane
(`delete_where_numeric_range_inplace`) could not resolve it and the statement fell
through to materializing every column of the table (1M rows: 3.58 ms against
1.78 ms for the same shape on a user column). UPDATE has had its own `_id` branch
all along; DELETE now has one too, built on the in-place deletion vector.

These tests pin the answers, the reported count and the rows that must *not*
disappear — including the compressed fallback where the cheap lane declines — so a
wrong id, a double count or a resurrection shows up here.

The "is it gone" assertions go through a real column (`k`), not through
`COUNT(*) WHERE _id = <deleted>`: an aggregate over a missing/deleted `_id` returns
an *empty result row* by the repository's documented contract (see `V5_NEXT_STEPS.md`
§5-D4), so `.scalar()` would be `None` for both a deleted row and a never-existing one.
"""

import os
import shutil
import statistics
import tempfile
import time

import pytest

from apexbase import ApexClient

ROWS = 5_000


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    c = ApexClient(root, enable_cache=False, durability="fast")
    c.create_table("t", schema={"k": "int64", "s": "string", "f": "double"})
    c.use_table("t")
    c.store({"k": list(range(ROWS)), "s": [f"v{i}" for i in range(ROWS)],
             "f": [float(i) for i in range(ROWS)]})
    c.flush()
    try:
        yield c, root
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)


def _one(c, sql):
    return c.execute(sql).scalar()


def _id_of(c, key):
    return c.execute(f"SELECT _id FROM t WHERE k = {key}", show_internal_id=True).to_dict()[0]["_id"]


def test_deleting_by_id_removes_exactly_that_row(client):
    c, _ = client
    target = _id_of(c, 42)
    assert _one(c, f"DELETE FROM t WHERE _id = {target}") == 1
    assert _one(c, "SELECT COUNT(*) FROM t") == ROWS - 1
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 42") == 0
    # The neighbouring rows are untouched.
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 41") == 1
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 43") == 1


def test_the_first_and_last_row_are_both_reachable(client):
    c, _ = client
    first = _id_of(c, 0)
    last = _id_of(c, ROWS - 1)
    assert _one(c, f"DELETE FROM t WHERE _id = {first}") == 1
    assert _one(c, f"DELETE FROM t WHERE _id = {last}") == 1
    assert _one(c, "SELECT COUNT(*) FROM t") == ROWS - 2
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 0") == 0
    assert _one(c, f"SELECT COUNT(*) FROM t WHERE k = {ROWS - 1}") == 0
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 1") == 1


def test_a_missing_id_deletes_nothing(client):
    c, _ = client
    assert _one(c, f"DELETE FROM t WHERE _id = {ROWS + 10_000}") == 0
    assert _one(c, "SELECT COUNT(*) FROM t") == ROWS
    # Every row is still there, including the one just past the high-water mark.
    assert _one(c, "SELECT SUM(k) FROM t") == sum(range(ROWS))


def test_repeating_the_same_delete_reports_zero_rows(client):
    c, _ = client
    target = _id_of(c, 7)
    assert _one(c, f"DELETE FROM t WHERE _id = {target}") == 1
    # The row is already gone: the cheap lane must not count it twice.
    assert _one(c, f"DELETE FROM t WHERE _id = {target}") == 0
    assert _one(c, f"DELETE FROM t WHERE _id = {target}") == 0
    assert _one(c, "SELECT COUNT(*) FROM t") == ROWS - 1


def test_the_deletion_survives_a_flush_and_a_later_update(client):
    c, _ = client
    target = _id_of(c, 100)
    assert _one(c, f"DELETE FROM t WHERE _id = {target}") == 1
    c.flush()
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 100") == 0
    # A later cell update of a neighbour must not resurrect the deleted row.
    assert _one(c, "UPDATE t SET s = 'N' WHERE k = 101") == 1
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 101") == 1
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 100") == 0
    assert _one(c, "SELECT COUNT(*) FROM t") == ROWS - 1


def test_a_row_that_carries_a_cell_overlay_can_still_be_deleted_by_id(client):
    c, _ = client
    target = _id_of(c, 21)
    assert _one(c, f"UPDATE t SET s = 'overlay' WHERE _id = {target}") == 1
    assert _one(c, f"DELETE FROM t WHERE _id = {target}") == 1
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 21") == 0
    assert _one(c, "SELECT COUNT(*) FROM t WHERE s = 'overlay'") == 0
    assert _one(c, "SELECT COUNT(*) FROM t") == ROWS - 1


@pytest.mark.parametrize("compression", [None, "zstd"])
def test_a_compressed_table_still_deletes_by_id(compression):
    """The cheap lane declines compressed row groups; the general path must answer."""
    root = tempfile.mkdtemp()
    c = ApexClient(root, enable_cache=False, durability="fast")
    try:
        c.create_table("t", schema={"k": "int64", "s": "string"})
        c.use_table("t")
        if compression:
            assert c.set_compression(compression) is not False
        c.store({"k": list(range(2_000)), "s": [f"v{i}" for i in range(2_000)]})
        c.flush()
        target = _id_of(c, 1_000)
        assert _one(c, f"DELETE FROM t WHERE _id = {target}") == 1
        assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 1000") == 0
        assert _one(c, "SELECT COUNT(*) FROM t") == 1_999
        assert _one(c, f"DELETE FROM t WHERE _id = {target}") == 0
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)


def test_deleting_by_id_is_not_slower_than_a_key_equality():
    """The `_id` lane must not fall back to materializing the table.

    Both statements are timed interleaved in one process, so machine load applies
    to both. Before the lane existed, `_id = X` read the whole `_id` column
    (2.28x the `k = X` cost at 200k rows); with the lane it is the cheaper of the
    two. A factor of 1.5 separates those regimes without being flaky.
    """
    rows = 200_000
    root = tempfile.mkdtemp()
    c = ApexClient(root, enable_cache=False, durability="fast")
    try:
        c.create_table("t", schema={"k": "int64", "s": "string", "f": "double"})
        c.use_table("t")
        step = 100_000
        for start in range(0, rows, step):
            end = min(start + step, rows)
            c.store({"k": list(range(start, end)),
                     "s": [f"v{i}" for i in range(start, end)],
                     "f": [float(i) for i in range(start, end)]})
        c.flush()

        by_id, by_key = [], []
        # Each sample deletes a different row: a repeated DELETE would measure a
        # no-op instead of the lane under test.
        for offset in range(6):
            start = time.perf_counter()
            assert c.execute(f"DELETE FROM t WHERE _id = {10_000 + offset}").scalar() == 1
            by_id.append((time.perf_counter() - start) * 1000.0)

            start = time.perf_counter()
            assert c.execute(f"DELETE FROM t WHERE k = {20_000 + offset}").scalar() == 1
            by_key.append((time.perf_counter() - start) * 1000.0)

        median_by_id = statistics.median(by_id)
        median_by_key = statistics.median(by_key)
        assert median_by_id <= median_by_key * 1.5, (
            f"DELETE by _id fell back to a scan: _id {median_by_id:.3f} ms "
            f"vs k {median_by_key:.3f} ms (samples {by_id} / {by_key})"
        )
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)
