"""DECIMAL(p,s) columns: Python Decimal values stored as 16-byte i128 payloads."""

from decimal import Decimal

import pytest

from apexbase.client import ApexClient

SCALE = 2


def _unscaled(payload):
    return int.from_bytes(payload, "little", signed=True)


def test_decimal_values_are_scaled_into_the_fixed_payload(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"id": "int64", "d": "decimal(38,2)"})
    client.use_table("t")
    client.store({"id": [1, 2], "d": [Decimal("1.23"), Decimal("-45.6")]})
    client.store([{"id": 3, "d": Decimal("7")}, {"id": 4, "d": "8.90"}])
    client.flush()
    rows = client.execute("SELECT id, d FROM t ORDER BY id").to_dict()
    assert [row["id"] for row in rows] == [1, 2, 3, 4]
    assert [_unscaled(row["d"]) for row in rows] == [123, -4560, 700, 890]
    client.close()


def test_decimal_rejects_more_fractional_digits_than_the_scale(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"d": "decimal(38,2)"})
    client.use_table("t")
    with pytest.raises(ValueError, match="fractional digits"):
        client.store({"d": [Decimal("1.234")]})
    # The exact-scale form still works after the rejected write.
    client.store({"d": [Decimal("1.23")]})
    client.flush()
    assert _unscaled(client.execute("SELECT d FROM t").to_dict()[0]["d"]) == 123
    client.close()


def test_decimal_columns_can_be_disabled_by_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("APEX_LOGICAL_TYPES", "0")
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        for declared in ("decimal(38,2)", "decimal128"):
            with pytest.raises(ValueError, match="APEX_LOGICAL_TYPES"):
                client.create_table("t", {"d": declared})
    finally:
        client.close()
    monkeypatch.delenv("APEX_LOGICAL_TYPES")
    client = ApexClient(str(tmp_path), drop_if_exists=False, enable_cache=False)
    try:
        client.create_table("t", {"d": "decimal(38,2)"})
    finally:
        client.close()


def test_decimal_scale_survives_a_client_reopen(tmp_path):
    """A fresh client fetches decimal(p,s) from the catalog, so Decimal keeps working.

    Before that fetch existed the client could not scale the value, and the write
    silently became NULL (round 343 made it fail loudly in the meantime).
    """
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"d": "decimal(38,2)"})
    client.use_table("t")
    client.store({"d": [Decimal("1.23")]})
    client.flush()
    client.close()

    reopened = ApexClient(str(tmp_path), drop_if_exists=False, enable_cache=False)
    reopened.use_table("t")
    reopened.store({"d": [Decimal("4.56")]})
    reopened.flush()
    rows = reopened.execute("SELECT d FROM t ORDER BY _id").to_dict()
    assert [_unscaled(row["d"]) for row in rows] == [123, 456]
    reopened.close()


def test_recreated_table_does_not_reuse_the_dropped_tables_scale(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"d": "decimal(38,2)"})
    client.use_table("t")
    client.drop_table("t")
    client.create_table("t", {"d": "decimal(38,4)"})
    client.use_table("t")
    client.store({"d": [Decimal("1.23")]})
    client.flush()
    row = client.execute("SELECT d FROM t").to_dict()[0]
    assert _unscaled(row["d"]) == 12300
    client.close()


def test_describe_table_reports_parameterised_types(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"u": "uuid", "d": "decimal(38,2)", "n": "int64"})
    client.use_table("t")
    assert client.describe_table() == {
        "u": "uuid",
        "d": "decimal(38,2)",
        "n": "int64",
    }
    client.close()


def test_describe_table_rejects_unknown_tables(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    with pytest.raises(ValueError):
        client.describe_table("missing")
    client.create_table("t", {"d": "int64"})
    # An explicit name works even while the table has not been written yet.
    assert client.describe_table("t") == {"d": "int64"}
    client.close()


def test_describe_table_caches_and_tracks_ddl_changes(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"d": "decimal(38,2)"})
    client.use_table("t")
    assert client.describe_table() == {"d": "decimal(38,2)"}
    # Cached: no catalog read needed, and identical result.
    assert client.describe_table() == {"d": "decimal(38,2)"}
    client.add_column("n", "int64")
    assert client.describe_table() == {"d": "decimal(38,2)", "n": "int64"}
    client.close()


def test_decimal_rejects_values_outside_the_128_bit_range(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"d": "decimal(38,2)"})
    client.use_table("t")
    with pytest.raises(ValueError, match="128-bit"):
        client.store({"d": [Decimal("1e40")]})
    # The 128-bit boundary payload is accepted as raw bytes (Decimal arithmetic
    # would round it at the default 28-digit precision, so pass the payload).
    limit = (2**127 - 1).to_bytes(16, "little", signed=True)
    client.store({"d": [limit]})
    client.flush()
    assert _unscaled(client.execute("SELECT d FROM t").to_dict()[0]["d"]) == 2**127 - 1
    client.close()
