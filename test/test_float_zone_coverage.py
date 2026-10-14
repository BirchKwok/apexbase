"""Float zone maps must not treat a covered *value* as a covered *range*.

The fused filtered-aggregation lane may answer ``COUNT(*)`` from metadata alone
when a row group's whole value range sits inside the queried range. Its float arm
asked the opposite question ("is the queried range inside the group range?"), so
any equality over a float column whose zone contained the value looked fully
covering and every live row was counted:

    SELECT COUNT(*) FROM t a CROSS JOIN m b WHERE a.f = 50.0 AND b.w > 0

returned ``rows * |m|`` instead of ``matches * |m|`` (measured on the v1.37.0
preview build: 10,000,000 instead of 940). The count-join shortcut calls the
storage lane directly, which is why the bug was only reachable through it.

These tests are SQL-visible and pin both directions of the test: an equality that
the zone merely *contains* must count matching rows, while a genuinely covering
range may still use the metadata shortcut.
"""

import os
import shutil
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 200_000
HITS_EVERY = 1_000
EXACT = 50.0
META_POP = [3, 1, 7]  # every row is > 0


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    c = ApexClient(root, enable_cache=False)
    c.create_table("t", schema={"f": "float64", "k": "int64"})
    c.use_table("t")
    floats = [EXACT if i % HITS_EVERY == 0 else float(i % 100) for i in range(ROWS)]
    c.store({"f": floats, "k": list(range(ROWS))})
    c.create_table("m", schema={"w": "int64"})
    c.use_table("m")
    c.store({"w": META_POP})
    c.flush()
    c.use_table("t")
    try:
        yield c, floats
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)


def _scalar(c, sql):
    return c.execute(sql).scalar()


def test_equality_on_a_float_counts_matching_rows(client):
    c, floats = client
    hits = sum(1 for v in floats if v == EXACT)
    assert 0 < hits < ROWS
    assert _scalar(c, f"SELECT COUNT(*) FROM t WHERE f = {EXACT}") == hits


def test_count_join_shortcut_counts_matching_rows_not_row_groups(client):
    c, floats = client
    hits = sum(1 for v in floats if v == EXACT)
    # The shortcut feeds the storage lane a degenerate [v, v] range; the zone map
    # of every row group covers it, which must not be mistaken for "all rows".
    assert _scalar(
        c,
        f"SELECT COUNT(*) FROM t a CROSS JOIN m b WHERE a.f = {EXACT} AND b.w > 0",
    ) == hits * len(META_POP)


def test_partial_float_ranges_stay_exact_through_the_shortcut(client):
    c, floats = client
    expected = sum(1 for v in floats if 20.0 <= v <= 30.0)
    assert _scalar(
        c, "SELECT COUNT(*) FROM t a CROSS JOIN m b WHERE a.f BETWEEN 20 AND 30 AND b.w > 0"
    ) == expected * len(META_POP)
    assert _scalar(
        c, "SELECT COUNT(*) FROM t a CROSS JOIN m b WHERE a.f = 20.5 AND b.w > 0"
    ) == sum(1 for v in floats if v == 20.5) * len(META_POP)


def test_a_covering_range_may_still_use_the_metadata_shortcut(client):
    c, floats = client
    # `f` spans [0, 100] with no NULLs, so `f >= 0` really does match every row.
    assert _scalar(
        c, "SELECT COUNT(*) FROM t a CROSS JOIN m b WHERE a.f >= 0 AND b.w > 0"
    ) == ROWS * len(META_POP)
    assert _scalar(c, "SELECT COUNT(*) FROM t WHERE f >= 0") == ROWS


def test_the_integer_arm_keeps_the_same_direction(client):
    c, _ = client
    assert _scalar(
        c, "SELECT COUNT(*) FROM t a CROSS JOIN m b WHERE a.k = 25 AND b.w > 0"
    ) == 1 * len(META_POP)
    assert _scalar(
        c, "SELECT COUNT(*) FROM t a CROSS JOIN m b WHERE a.k >= 0 AND b.w > 0"
    ) == ROWS * len(META_POP)
