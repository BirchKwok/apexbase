"""Literal single-cell UPDATEs must not pay for the whole table.

The mmap in-place lane writes numeric cells in eight bytes; every other literal
assignment used to abandon the resolved range and re-read the table's needed
columns, which cost 32 ms for a one-row UPDATE at 1M rows. The cheap route
resolves the matched ids through the zone-map-pruned filtered read and records
the overrides in the DeltaStore.

These tests pin the *answers* of that route (and of the path it declines to), so
a wrong id list — an empty projection, a deleted row, an over-limit match set —
shows up as a wrong value instead of a fast one.
"""

import os
import shutil
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 10_000


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    c = ApexClient(root, enable_cache=False, durability="fast")
    c.create_table("t", schema={"k": "int64", "s": "string", "f": "double", "dup": "int64"})
    c.use_table("t")
    c.store({
        "k": list(range(ROWS)),
        "s": [f"v{i}" for i in range(ROWS)],
        "f": [float(i) for i in range(ROWS)],
        "dup": [i % 2 for i in range(ROWS)],
    })
    c.flush()
    try:
        yield c, root
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)


def _one(c, sql):
    return c.execute(sql).scalar()


def _value(c, column, key):
    return c.execute(f"SELECT {column} FROM t WHERE k = {key}").to_dict()[0][column]


def test_a_string_cell_update_lands_and_survives_a_flush(client):
    c, root = client
    assert _one(c, "UPDATE t SET s = 'CHANGED' WHERE k = 5") == 1
    assert _value(c, "s", 5) == "CHANGED"
    assert _one(c, "SELECT COUNT(*) FROM t WHERE s = 'CHANGED'") == 1
    c.flush()
    assert _value(c, "s", 5) == "CHANGED"
    assert os.path.exists(os.path.join(root, "t.apex.deltastore"))


def test_a_missing_key_updates_nothing(client):
    c, _ = client
    assert _one(c, "UPDATE t SET s = 'X' WHERE k = 999999") == 0
    assert _one(c, "SELECT COUNT(*) FROM t WHERE s = 'X'") == 0
    # The rest of the column is untouched.
    assert _one(c, "SELECT COUNT(*) FROM t WHERE s LIKE 'v%'") == ROWS


def test_first_and_last_row_are_both_reachable(client):
    c, _ = client
    assert _one(c, f"UPDATE t SET s = 'FIRST' WHERE k = 0") == 1
    assert _one(c, f"UPDATE t SET s = 'LAST' WHERE k = {ROWS - 1}") == 1
    assert _value(c, "s", 0) == "FIRST"
    assert _value(c, "s", ROWS - 1) == "LAST"


def test_multi_column_set_applies_every_assignment(client):
    c, _ = client
    assert _one(c, "UPDATE t SET s = 'M', f = 1.5 WHERE k = 7") == 1
    row = c.execute("SELECT s, f FROM t WHERE k = 7").to_dict()[0]
    assert row["s"] == "M"
    assert row["f"] == 1.5


def test_a_dense_match_set_still_updates_every_row(client):
    c, _ = client
    # `dup` is not unique, so this equality matches DENSE rows — above the cheap
    # route's limit. The general path must still apply every assignment.
    matches = ROWS // 2  # `dup = 1` on every other row: above the 4096 limit
    assert matches > 4096
    assert _one(c, "UPDATE t SET s = 'DENSE' WHERE dup = 1") == matches
    assert _one(c, "SELECT COUNT(*) FROM t WHERE s = 'DENSE'") == matches


def test_a_deleted_row_is_not_resurrected_by_a_later_update(client):
    c, _ = client
    assert _one(c, "DELETE FROM t WHERE k = 11") == 1
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 11") == 0
    # Updating a neighbour must not bring the deleted row back.
    assert _one(c, "UPDATE t SET s = 'N' WHERE k = 12") == 1
    assert _value(c, "s", 12) == "N"
    assert _one(c, "SELECT COUNT(*) FROM t WHERE k = 11") == 0


def test_the_update_result_count_matches_the_matching_rows(client):
    c, _ = client
    for key in (1, 2, 3):
        assert _one(c, f"UPDATE t SET s = 'R{key}' WHERE k = {key}") == 1
    assert _one(c, "SELECT COUNT(*) FROM t WHERE s LIKE 'R%'") == 3
