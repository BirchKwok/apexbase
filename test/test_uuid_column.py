"""UUID columns: declared type, 16-byte payload, v6 file version.

The on-disk payload reuses the binary layout and ``ColumnType::Uuid`` carries the
semantics, so a file with a UUID column declares format version 6 (an older
build must refuse it rather than misread the schema). Python values are 16 raw
bytes and project as ``bytes`` (D4 decision: no list/string conversion).
"""

import os

import pytest
import tempfile
import uuid

from apexbase.client import ApexClient

ROWS = 4


def _values():
    return [uuid.UUID(int=index).bytes for index in range(ROWS)]


def _file_version(table_path):
    with open(table_path, "rb") as handle:
        header = handle.read(16)
    return int.from_bytes(header[8:12], "little")


def test_uuid_column_round_trips_and_declares_format_version_six(tmp_path):
    path = str(tmp_path)
    values = _values()
    client = ApexClient(path, drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"id": "int64", "u": "uuid"})
    client.use_table("t")
    client.store({"id": list(range(1, ROWS + 1)), "u": values})
    client.flush()

    rows = client.execute("SELECT id, u FROM t ORDER BY id").to_dict()
    assert [row["u"] for row in rows] == values
    assert _file_version(os.path.join(path, "t.apex")) == 6
    client.close()

    reopened = ApexClient(path, drop_if_exists=False, enable_cache=False)
    reopened.use_table("t")
    rows = reopened.execute("SELECT id, u FROM t ORDER BY id").to_dict()
    assert [row["u"] for row in rows] == values
    assert reopened.execute(
        "SELECT COUNT(*) AS n FROM t WHERE u = NULL"
    ).to_dict() == [{"n": 0}]
    reopened.close()


def test_uuid_column_accepts_the_exact_sixteen_byte_form(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"u": "uuid"})
    client.use_table("t")
    client.store({"u": [b"\x00" * 16]})
    client.flush()
    assert client.execute("SELECT u FROM t").to_dict() == [{"u": b"\x00" * 16}]
    client.close()


def test_uuid_columns_can_be_disabled_by_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("APEX_LOGICAL_TYPES", "0")
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        with pytest.raises(ValueError, match="APEX_LOGICAL_TYPES"):
            client.create_table("t", {"u": "uuid"})
    finally:
        client.close()
    monkeypatch.delenv("APEX_LOGICAL_TYPES")
    client = ApexClient(str(tmp_path), drop_if_exists=False, enable_cache=False)
    try:
        client.create_table("t", {"u": "uuid"})
    finally:
        client.close()


def test_uuid_row_insert_validates_the_sixteen_byte_contract(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"id": "int64", "u": "uuid"})
    client.use_table("t")
    # The row-wise insert path rejects a value the declared type cannot hold.
    with pytest.raises((RuntimeError, OSError), match="16-byte"):
        client.store([{"id": 1, "u": b"short"}])
    client.store([{"id": 1, "u": b"\x01" * 16}])
    client.flush()
    assert client.execute("SELECT u FROM t").to_dict() == [{"u": b"\x01" * 16}]
    client.close()


def test_decimal128_column_round_trips_bytes_and_declares_v6(tmp_path):
    path = str(tmp_path)
    client = ApexClient(path, drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"id": "int64", "d": "decimal128"})
    client.use_table("t")
    # Unscaled 128-bit values (bytes16) until Decimal(p,s) semantics land.
    values = [b"\x00" * 15 + bytes([i]) for i in range(1, 4)]
    client.store([{"id": index + 1, "d": value} for index, value in enumerate(values)])
    client.flush()
    rows = client.execute("SELECT id, d FROM t ORDER BY id").to_dict()
    assert [row["d"] for row in rows] == values
    assert _file_version(os.path.join(path, "t.apex")) == 6
    client.close()
