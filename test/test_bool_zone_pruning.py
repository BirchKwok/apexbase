"""Zone-map pruning for BOOLEAN columns.

Bool columns used to carry no zone map, so `WHERE flag = true` scanned every row
group. They now record the observed bit range (`false < true`), and a row group
whose range cannot contain the requested value is skipped.

These tests are SQL-visible: the fixture clusters `flag` so whole row groups are
uniform, then every predicate shape must return exactly the matching rows. A
wrong prune (skipping a group that does match, or keeping the null/bit
semantics wrong) shows up as a wrong count.
"""

import os
import shutil
import struct
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 400_000
RUN = 131_072  # largest adaptive row-group size: every group stays uniform


def _row_group_count(path):
    """Row-group count from the V4 header (offset 84)."""
    with open(path, "rb") as handle:
        header = handle.read(88)
    return struct.unpack_from("<I", header, 84)[0]


def _columns(rows):
    return {
        # Uniform per row group whichever adaptive size is chosen.
        "flag": [(i // RUN) % 2 == 0 for i in range(rows)],
        # NULL on every 11th row, so NULL handling is exercised alongside.
        "maybe": [None if i % 11 == 0 else (i // RUN) % 2 == 0 for i in range(rows)],
        "pad": list(range(rows)),
    }


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    c = ApexClient(root, enable_cache=False)
    c.create_table("t", schema={"flag": "bool", "maybe": "bool", "pad": "int64"})
    c.use_table("t")
    c.store(_columns(ROWS))
    c.flush()
    groups = _row_group_count(os.path.join(root, "t.apex"))
    assert groups >= 3, f"fixture needs multiple row groups, got {groups}"
    try:
        yield c
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)


def _count(c, where):
    return c.execute(f"SELECT COUNT(*) AS n FROM t WHERE {where}").to_dict()[0]["n"]


def _expected_true(rows=ROWS):
    return sum(1 for i in range(rows) if (i // RUN) % 2 == 0)


def _expected_maybe(rows=ROWS):
    return sum(
        1 for i in range(rows) if i % 11 != 0 and (i // RUN) % 2 == 0
    )


def test_uniform_row_groups_prune_to_the_matching_half(client):
    expected = _expected_true()
    assert _count(client, "flag = true") == expected
    assert _count(client, "flag = false") == ROWS - expected
    assert _count(client, "flag = TRUE") == expected
    assert _count(client, "flag = TRUE OR flag = FALSE") == ROWS


def test_not_equal_never_drops_a_matching_group(client):
    expected = _expected_true()
    assert _count(client, "flag != true") == ROWS - expected
    assert _count(client, "flag != false") == expected
    assert _count(client, "flag <> false") == expected


def test_pruned_predicate_composes_with_other_columns(client):
    expected = _expected_true()
    assert _count(client, "flag = true AND pad >= 0") == expected
    assert _count(client, "flag = true AND pad < 100") == 100
    assert _count(client, "flag = false AND pad >= 0") == ROWS - expected
    assert _count(client, "flag = true OR pad >= 100") == ROWS
    assert _count(client, "flag = true AND flag = false") == 0
    assert _count(client, "flag = true AND pad > 100000000") == 0


def test_nullable_bool_keeps_null_semantics(client):
    assert _count(client, "maybe = true") == _expected_maybe()
    assert _count(client, "maybe = false") == ROWS - _expected_maybe() - len(
        range(0, ROWS, 11)
    )
    assert _count(client, "maybe IS NULL") == len(range(0, ROWS, 11))
    assert _count(client, "maybe IS NOT NULL") == ROWS - len(range(0, ROWS, 11))


def test_projection_and_aggregate_lanes_agree(client):
    # The batch/aggregate lane consults the same predicate; it must not lose
    # rows to pruning either.
    expected = _expected_true()
    grouped = client.execute(
        "SELECT flag, COUNT(*) AS n FROM t GROUP BY flag ORDER BY flag"
    ).to_dict()
    assert grouped == [{"flag": False, "n": ROWS - expected}, {"flag": True, "n": expected}]

    rows = client.execute("SELECT pad FROM t WHERE flag = true LIMIT 3").to_dict()
    assert [row["pad"] for row in rows] == [0, 1, 2]

    total = client.execute(
        "SELECT SUM(pad) AS s FROM t WHERE flag = true"
    ).to_dict()[0]["s"]
    assert total == sum(i for i in range(ROWS) if (i // RUN) % 2 == 0)


def test_bool_update_and_delete_stay_correct(client):
    # In-place updates do not apply to Bool columns; the delta path must still
    # read back the new value and keep pruning honest.
    target = 5
    client.execute(f"UPDATE t SET flag = true WHERE _id = {target + 1}")
    assert client.execute(
        f"SELECT flag FROM t WHERE _id = {target + 1}"
    ).to_dict() == [{"flag": True}]

    client.execute(f"DELETE FROM t WHERE _id = {2 * RUN + 1}")
    # A point lookup of a deleted id yields an empty result (the pre-existing
    # contract, identical on the 1.36.1 base build), not a zero-count row.
    assert client.execute(
        f"SELECT COUNT(*) AS n FROM t WHERE _id = {2 * RUN + 1}"
    ).to_dict() == []
    # The deleted row was in an all-true group, so the surviving count drops.
    assert _count(client, "flag = true") == _expected_true() - 1
