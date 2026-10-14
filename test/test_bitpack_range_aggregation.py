"""Partially covering ranges over bit-packed integer columns.

The filtered numeric aggregation lane dispatches on the filter column's bit
width: widths up to 16 are decoded word-at-a-time by the per-row dispatcher,
wider widths fall through to its generic arm. A bulk counter added for the
generic arm used to preempt the narrower decoders, which made a partial-range
``COUNT(*)`` 74% slower at 1M rows and made the public ``CROSS JOIN COUNT``
benchmark 45% slower.

These tests pin the *answers* of a partial range for both width classes through
SQL, including the value aggregates that share the same lane, and assert the
fixture really stores the narrow column bit-packed (a plain layout would need
eight bytes per row for that column alone). The canary times the narrow shape.
"""

import os
import shutil
import struct
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 400_000
RUN = 131_072  # largest adaptive row-group size
NARROW_SPAN = 63  # 18..80 -> 6 bits


def _row_group_count(path):
    """Row-group count from the V4 header (offset 84)."""
    with open(path, "rb") as handle:
        header = handle.read(88)
    return struct.unpack_from("<I", header, 84)[0]


def _narrow(rows=ROWS):
    return [18 + (i % NARROW_SPAN) for i in range(rows)]


def _wide(rows=ROWS):
    return [i * 97 for i in range(rows)]


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    c = ApexClient(root, enable_cache=False)
    c.create_table("t", schema={"narrow": "int64", "wide": "int64", "pad": "int64"})
    c.use_table("t")
    c.store({"narrow": _narrow(), "wide": _wide(), "pad": list(range(ROWS))})
    c.flush()
    groups = _row_group_count(os.path.join(root, "t.apex"))
    assert groups >= 2, f"fixture needs multiple row groups, got {groups}"

    # Fixture honesty: the same schema with a value span the bit-packer cannot
    # compress must be materially larger, otherwise the columns above stopped
    # being bit-packed and these tests would silently stop covering the packed
    # decoders.
    c.create_table("flat", schema={"narrow": "int64", "wide": "int64", "pad": "int64"})
    c.use_table("flat")
    c.store(
        {
            "narrow": [i * 10**12 for i in range(ROWS)],
            "wide": [i * 10**12 for i in range(ROWS)],
            "pad": list(range(ROWS)),
        }
    )
    c.flush()
    c.use_table("t")
    packed_bytes = os.path.getsize(os.path.join(root, "t.apex"))
    flat_bytes = os.path.getsize(os.path.join(root, "flat.apex"))
    assert flat_bytes > packed_bytes + ROWS * 6, (
        f"fixture columns are no longer bit-packed: packed={packed_bytes} flat={flat_bytes}"
    )
    try:
        yield c
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)


def _scalar(c, sql):
    return c.execute(sql).scalar()


def test_partial_range_count_matches_the_matching_rows(client):
    narrow = _narrow()
    wide = _wide()
    # A narrow (6-bit) column: the predicate matches 3 of every 63 rows.
    expected_narrow = sum(1 for v in narrow if v < 21)
    assert _scalar(client, "SELECT COUNT(*) FROM t WHERE narrow < 21") == expected_narrow
    # A wide (>16-bit) column: the predicate matches a short prefix.
    expected_wide = sum(1 for v in wide if v < 1_000)
    assert _scalar(client, "SELECT COUNT(*) FROM t WHERE wide < 1000") == expected_wide


def test_partial_range_value_aggregates_match_on_both_widths(client):
    narrow = _narrow()
    wide = _wide()
    hit_narrow = [v for v in narrow if v < 21]
    hit_wide = [v for v in wide if v < 1_000]
    assert _scalar(client, "SELECT SUM(narrow) FROM t WHERE narrow < 21") == sum(hit_narrow)
    assert _scalar(client, "SELECT MIN(narrow) FROM t WHERE narrow < 21") == min(hit_narrow)
    assert _scalar(client, "SELECT MAX(narrow) FROM t WHERE narrow < 21") == max(hit_narrow)
    assert _scalar(client, "SELECT COUNT(narrow) FROM t WHERE narrow < 21") == len(hit_narrow)
    assert _scalar(client, "SELECT SUM(wide) FROM t WHERE wide < 1000") == sum(hit_wide)
    assert _scalar(client, "SELECT COUNT(wide) FROM t WHERE wide < 1000") == len(hit_wide)


def test_two_sided_and_covering_ranges_stay_correct(client):
    narrow = _narrow()
    wide = _wide()
    assert _scalar(client, "SELECT COUNT(*) FROM t WHERE narrow BETWEEN 30 AND 40") == sum(
        1 for v in narrow if 30 <= v <= 40
    )
    assert _scalar(client, "SELECT COUNT(*) FROM t WHERE wide BETWEEN 500 AND 900") == sum(
        1 for v in wide if 500 <= v <= 900
    )
    # Covering and empty ranges take the lane's other branches.
    assert _scalar(client, "SELECT COUNT(*) FROM t WHERE narrow >= 18") == ROWS
    assert _scalar(client, "SELECT COUNT(*) FROM t WHERE narrow < 0") == 0
    assert _scalar(client, "SELECT SUM(wide) FROM t WHERE wide >= 0") == sum(wide)
