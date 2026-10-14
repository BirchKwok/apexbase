"""A two-sided range DELETE must reach the in-place deletion lane.

`col >= a AND col < b` is one range on one column. The delete path only asked the
single-comparison extractor, so the `AND` form fell through to materializing the
table (measured 11.63 ms for a one-row delete at 1M rows, against 1.39 ms for
`col = v`). It now uses the same two-sided extraction the read lanes use.

The tests pin both the answers and the *equivalence* of the two spellings, so a
lane that silently drops rows (or keeps deleted ones) fails here.
"""

import os
import shutil
import tempfile

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


def test_two_sided_and_deletes_exactly_the_window(client):
    c, _ = client
    assert _one(c, "SELECT COUNT(*) FROM t WHERE f >= 200 AND f < 201") == 1
    assert _one(c, "DELETE FROM t WHERE f >= 200 AND f < 201") == 1
    assert _one(c, "SELECT COUNT(*) FROM t WHERE f >= 200 AND f < 201") == 0
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 200") == 0


def test_between_and_the_and_spelling_agree(client):
    c, _ = client
    # Inclusive BETWEEN covers two rows; the half-open AND covers one.
    assert _one(c, "DELETE FROM t WHERE f BETWEEN 300 AND 301") == 2
    assert _one(c, "SELECT COUNT(*) FROM t WHERE f BETWEEN 300 AND 301") == 0
    assert _one(c, "DELETE FROM t WHERE f >= 400 AND f < 401") == 1
    assert _one(c, "SELECT COUNT(*) FROM t WHERE f >= 400 AND f <= 401") == 1


def test_empty_and_inverted_windows_delete_nothing(client):
    c, _ = client
    before = _one(c, "SELECT COUNT(*) FROM t")
    assert _one(c, "DELETE FROM t WHERE f >= 90000 AND f < 90001") == 0
    assert _one(c, "DELETE FROM t WHERE f >= 500 AND f < 400") == 0
    assert _one(c, "SELECT COUNT(*) FROM t") == before


def test_deletions_survive_a_flush_and_do_not_come_back(client):
    c, _ = client
    assert _one(c, "DELETE FROM t WHERE f >= 700 AND f < 705") == 5
    c.flush()
    assert _one(c, "SELECT COUNT(*) FROM t WHERE f >= 700 AND f < 705") == 0
    assert _one(c, "SELECT COUNT(*) FROM t") == ROWS - 5
    # A later UPDATE of a neighbouring row must not resurrect a deleted one.
    assert _one(c, "UPDATE t SET s = 'n' WHERE k = 706") == 1
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 700") == 0


def test_aggregates_agree_with_the_visible_rows(client):
    c, _ = client
    assert _one(c, "DELETE FROM t WHERE f >= 1000 AND f < 1100") == 100
    assert _one(c, "SELECT COUNT(*) FROM t") == ROWS - 100
    expected = sum(range(ROWS)) - sum(range(1000, 1100))
    assert _one(c, "SELECT SUM(k) FROM t") == expected
