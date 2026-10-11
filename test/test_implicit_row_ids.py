"""
Contiguous row groups must omit their per-row ID section.

A row group whose IDs are exactly ``min_id..min_id + row_count`` needs no ID
payload: ``rg_id_at`` rebuilds them from ``min_id``, and every reader resolves
the encoding from the row group header's byte 30 (``RG_IDS_IMPLICIT_CONTIGUOUS``).
Storing the section anyway costs 8 bytes per row.

The encoding is enabled for temporary tables, whose path carries an
``.apex_tmp`` component. Making it the default for regular tables was
implemented and measured, then held back: it lowers CPU time and reads fewer
bytes (a 1M-row 2-column table drops from ~226 KB to ~65 KB), but it regresses
the 8-worker parallel batch scan on this machine (8 performance + 2 efficiency
cores) by 18-24% in wall time, because the lighter per-worker work makes a
descheduled worker's range dominate the fold. The parallel fold must be
reworked before that default flips; see ``V5_HYBRID_STORAGE_RESEARCH.md``
§2.8.3.

These tests pin the encoding on the path that enables it, plus every
ID-consuming read path: structural layout, point lookups, scans, aggregates,
the numeric-range in-place delete (whose deletion-vector offset must follow the
ID section length), updates, and append-after-delete.
"""

import os
import shutil
import struct
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 2_000
BIG_ROWS = 140_000


def _rg_headers(path):
    """(id_encoding byte, row_count, min_id, first column body offset) per RG."""
    data = open(path, "rb").read()
    footer_offset = struct.unpack_from("<Q", data, 76)[0]
    footer = data[footer_offset:]
    schema_len = struct.unpack_from("<Q", footer, 0)[0]
    pos = 8 + schema_len
    rg_count = struct.unpack_from("<I", footer, pos)[0]
    pos += 4

    metas = []
    for _ in range(rg_count):
        offset, _size, rows, min_id, max_id, _dels = struct.unpack_from(
            "<QQIQQI", footer, pos
        )
        pos += 40
        metas.append((offset, rows, min_id, max_id))

    if footer[pos:pos + 4] == b"ZMAP":
        pos += 4
        zmap_rgs = struct.unpack_from("<I", footer, pos)[0]
        pos += 4
        for _ in range(zmap_rgs):
            cols = struct.unpack_from("<H", footer, pos)[0]
            pos += 2 + cols * 19

    first_col_offsets = None
    if footer[pos:pos + 4] == b"RCIX":
        pos += 4
        rcix_rgs = struct.unpack_from("<I", footer, pos)[0]
        pos += 4
        first_col_offsets = []
        for _ in range(rcix_rgs):
            cols = struct.unpack_from("<H", footer, pos)[0]
            pos += 2
            offsets = struct.unpack_from(f"<{cols}I", footer, pos)
            pos += cols * 4
            first_col_offsets.append(offsets[0])

    out = []
    for i, (offset, rows, min_id, max_id) in enumerate(metas):
        out.append({
            "id_encoding": data[offset + 30],
            "rows": rows,
            "min_id": min_id,
            "max_id": max_id,
            "first_col_offset": None if first_col_offsets is None else first_col_offsets[i],
        })
    return out


def _write_csv(path, rows, start=0):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("a,b\n")
        for i in range(start, start + rows):
            handle.write(f"{i},v{i % 17}\n")


def _open_temp_table(root, name, rows, start=0):
    """Materialize a CSV temp table (`.apex_tmp/<name>.apex`)."""
    csv_path = os.path.join(root, f"{name}.csv")
    _write_csv(csv_path, rows, start)
    client = ApexClient(root, drop_if_exists=True, enable_cache=False)
    client.create_table("_dummy")
    client.use_table("_dummy")
    client.register_temp_table(name, csv_path)
    client.use_table(name)
    return client


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    c = _open_temp_table(root, "people", ROWS)
    try:
        yield c, root
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)


def _table_path(root, name="people"):
    return os.path.join(root, ".apex_tmp", f"{name}.apex")


def test_temp_table_row_group_omits_the_id_section(client):
    c, root = client
    assert c.execute("SELECT COUNT(*) AS n FROM people").to_dict() == [{"n": ROWS}]

    headers = _rg_headers(_table_path(root))
    assert len(headers) == 1
    rg = headers[0]
    assert rg["id_encoding"] == 1, "contiguous IDs must use the implicit encoding"
    assert rg["min_id"] == 1 and rg["max_id"] == ROWS
    # Body layout is [ids][deletion vector][columns...]: with no ID section the
    # first column starts immediately after the deletion vector.
    assert rg["first_col_offset"] == (rg["rows"] + 7) // 8


def test_multi_row_group_temp_table_omits_it_everywhere():
    root = tempfile.mkdtemp()
    c = _open_temp_table(root, "big", BIG_ROWS)
    try:
        headers = _rg_headers(_table_path(root, "big"))
        assert len(headers) >= 2
        for rg in headers:
            assert rg["id_encoding"] == 1
            assert rg["first_col_offset"] == (rg["rows"] + 7) // 8
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)


def test_appended_row_groups_omit_the_id_section(client):
    """Newly appended rows start at the next id, so later groups stay contiguous."""
    c, root = client
    c.store({"a": list(range(ROWS, ROWS + 500)),
             "b": [f"v{i % 17}" for i in range(ROWS, ROWS + 500)]})
    c.flush()

    assert c.execute("SELECT COUNT(*) AS n FROM people").to_dict() == [{"n": ROWS + 500}]
    for rg in _rg_headers(_table_path(root)):
        assert rg["id_encoding"] == 1
        assert rg["first_col_offset"] == (rg["rows"] + 7) // 8


def test_point_lookups_resolve_every_id(client):
    c, _ = client

    assert c.retrieve(1) == {"_id": 1, "a": 0, "b": "v0"}
    assert c.retrieve(ROWS)["a"] == ROWS - 1
    middle = ROWS // 2
    assert c.retrieve(middle)["a"] == middle - 1
    assert c.retrieve(ROWS + 1) is None

    assert c.execute(f"SELECT a FROM people WHERE _id = {middle}").to_dict() == [
        {"a": middle - 1}
    ]
    ids = list(range(1, 60, 7))
    assert [r["a"] for r in c.retrieve_many(ids)] == [i - 1 for i in ids]
    # The raw `_id` projection must agree with the reconstructed IDs.
    assert [r["_id"] for r in c.execute(
        "SELECT _id FROM people ORDER BY _id LIMIT 5"
    ).to_dict()] == [1, 2, 3, 4, 5]


def test_scan_and_aggregate_paths_still_agree(client):
    c, _ = client

    assert c.execute("SELECT COUNT(*) AS c FROM people").to_dict() == [{"c": ROWS}]
    assert c.execute(
        "SELECT COUNT(*) AS c FROM people WHERE a BETWEEN 100 AND 199"
    ).to_dict() == [{"c": 100}]
    assert c.execute(
        "SELECT a FROM people WHERE b = 'v3' ORDER BY a LIMIT 3"
    ).to_dict() == [{"a": 3}, {"a": 20}, {"a": 37}]
    assert [r["a"] for r in c.execute(
        "SELECT a FROM people ORDER BY a DESC LIMIT 3"
    ).to_dict()] == [ROWS - 1, ROWS - 2, ROWS - 3]
    assert c.execute(
        "SELECT MIN(a) AS mn, MAX(a) AS mx FROM people"
    ).to_dict() == [{"mn": 0, "mx": ROWS - 1}]


def test_numeric_range_delete_uses_the_implicit_body_layout(client):
    """A numeric-range DELETE stays correct on implicit-ID row groups.

    The on-disk body of such a row group is `[deletion vector][columns...]`, so
    the deletion vector sits at offset 0 rather than after `row_count * 8`
    bytes. The offset itself is pinned directly by the Rust test
    `numeric_range_inplace_delete_handles_implicit_id_row_groups`; this test
    covers the SQL-visible behaviour of the same layout.
    """
    c, root = client

    deleted = c.execute("DELETE FROM people WHERE a < 100").to_dict()
    assert deleted == [{"result": 100}]
    assert c.execute("SELECT COUNT(*) AS c FROM people").to_dict() == [{"c": ROWS - 100}]
    assert c.execute("SELECT MIN(a) AS m FROM people").to_dict() == [{"m": 100}]
    assert c.retrieve(1) is None
    assert c.retrieve(100) is None
    assert c.retrieve(101)["a"] == 100
    assert c.retrieve(ROWS)["a"] == ROWS - 1

    for rg in _rg_headers(_table_path(root)):
        assert rg["id_encoding"] == 1


def test_updates_and_deletes_by_id_keep_ids_correct(client):
    c, root = client

    c.execute("UPDATE people SET b = 'patched' WHERE _id = 777")
    assert c.retrieve(777)["b"] == "patched"
    c.execute("DELETE FROM people WHERE _id = 778")
    assert c.retrieve(778) is None
    assert c.retrieve(777)["b"] == "patched"
    assert c.retrieve(ROWS)["a"] == ROWS - 1
    assert c.execute("SELECT COUNT(*) AS c FROM people").to_dict() == [{"c": ROWS - 1}]


def test_append_after_deletes_keeps_both_encodings_readable(client):
    c, root = client

    c.execute("DELETE FROM people WHERE a < 400")
    c.store({"a": list(range(ROWS, ROWS + 500)),
             "b": [f"v{i % 17}" for i in range(ROWS, ROWS + 500)]})
    c.flush()

    assert c.execute("SELECT COUNT(*) AS c FROM people").to_dict() == [{"c": ROWS - 400 + 500}]
    assert c.retrieve(1) is None
    assert c.retrieve(400) is None
    assert c.retrieve(401)["a"] == 400
    assert c.retrieve(ROWS + 1)["a"] == ROWS
    assert c.retrieve(ROWS + 500)["a"] == ROWS + 499
    for rg in _rg_headers(_table_path(root)):
        assert rg["id_encoding"] == 1
