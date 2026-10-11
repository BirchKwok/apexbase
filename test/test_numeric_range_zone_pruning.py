"""Zone-map pruning for numeric range scans on clustered columns.

`scan_numeric_range_mmap` runs a parallel fast path when the scan has no limit
and every row group is uncompressed with RCIX offsets. That path used to build a
descriptor for *every* row group and scan them all; it now prunes row groups
whose zone map proves the requested range cannot match (the sequential fallback
always did). These tests pin the SQL-visible behaviour on a clustered column,
where per-row-group zones are tight, plus the corner windows that a wrong prune
would corrupt.
"""

import os
import shutil
import struct
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 300_000


def _row_group_count(path):
    """Row-group count from the V4 header (offset 84)."""
    with open(path, "rb") as handle:
        header = handle.read(88)
    return struct.unpack_from("<I", header, 84)[0]


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    c = ApexClient(root, enable_cache=False)
    c.create_table(
        "t",
        schema={
            "ts": "int64",
            "f": "float64",
            "v": "int64",
            "nullable": "int64",
            "pad": "string",
        },
    )
    c.use_table("t")
    c.store({
        "ts": list(range(ROWS)),
        "f": [float(i) for i in range(ROWS)],
        "v": [i % 5 for i in range(ROWS)],
        "nullable": [None if i % 7 == 0 else i for i in range(ROWS)],
        "pad": ["p" * 40 for _ in range(ROWS)],
    })
    c.flush()
    assert _row_group_count(os.path.join(root, "t.apex")) >= 3
    try:
        yield c
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)


def _count(c, where):
    return c.execute(f"SELECT COUNT(*) AS n FROM t WHERE {where}").to_dict()[0]["n"]


def test_between_returns_exact_windows(client):
    # Window confined to a single row group.
    assert _count(client, "ts BETWEEN 250000 AND 250009") == 10
    # Window straddling a row-group boundary.
    assert _count(client, "ts BETWEEN 65530 AND 65545") == 16
    # Window covering everything.
    assert _count(client, "ts BETWEEN 0 AND 299999") == ROWS
    # Window outside every zone.
    assert _count(client, "ts BETWEEN 400000 AND 500000") == 0
    # Half-open windows around the edges.
    assert _count(client, "ts < 3") == 3
    assert _count(client, "ts >= 299998") == 2
    assert _count(client, "ts > 299999") == 0


def test_point_predicate_prunes_to_a_single_row(client):
    assert _count(client, "ts = 123456") == 1
    assert client.execute(
        "SELECT v FROM t WHERE ts = 123456"
    ).to_dict() == [{"v": 123456 % 5}]
    assert _count(client, "ts = 999999") == 0


def test_float_column_range_keeps_every_row(client):
    assert _count(client, "f BETWEEN 100000.0 AND 100009.0") == 10
    assert _count(client, "f < 4.0") == 4
    assert _count(client, "f >= 299995.0") == 5


def test_nulls_are_never_returned_by_range_predicates(client):
    # `nullable` is NULL on every 7th row, so a full-range predicate returns the
    # non-null rows only, and a nil payload never counts as a zone match.
    expected = ROWS - len(range(0, ROWS, 7))
    assert _count(client, "nullable BETWEEN 0 AND 299999") == expected
    assert _count(client, "nullable < 1400") == 1400 - len(range(0, 1400, 7))


def test_range_delete_on_clustered_column(client):
    deleted = client.execute("DELETE FROM t WHERE ts < 1000").to_dict()
    assert deleted == [{"result": 1000}]
    assert _count(client, "ts >= 0") == ROWS - 1000
    assert client.execute("SELECT MIN(ts) AS m FROM t").to_dict() == [{"m": 1000}]
    assert client.retrieve(1) is None
    assert client.retrieve(1001)["ts"] == 1000

    # A window in the middle of the table, after the first delete. The window
    # spans 121 rows (199990..=200110) of which the delete removed 100.
    deleted = client.execute("DELETE FROM t WHERE ts BETWEEN 200000 AND 200099").to_dict()
    assert deleted == [{"result": 100}]
    assert _count(client, "ts BETWEEN 199990 AND 200110") == 121 - 100
    assert client.retrieve(200001) is None
    assert client.retrieve(200102)["ts"] == 200101
