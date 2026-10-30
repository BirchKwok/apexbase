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


def test_decimal_values_are_not_silently_dropped_after_client_reopen(tmp_path):
    """A fresh client does not know the declared scale, so it must refuse Decimals.

    Previously the value fell through to the storage layer and became NULL
    (silent data loss). The 16-byte payload keeps working because it needs no
    scale.
    """
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"d": "decimal(38,2)"})
    client.use_table("t")
    client.store({"d": [Decimal("1.23")]})
    client.flush()
    client.close()

    reopened = ApexClient(str(tmp_path), drop_if_exists=False, enable_cache=False)
    reopened.use_table("t")
    with pytest.raises(ValueError, match="decimal\\(p,s\\) declaration is not known"):
        reopened.store({"d": [Decimal("4.56")]})
    reopened.store({"d": [(456).to_bytes(16, "little", signed=True)]})
    reopened.flush()
    rows = reopened.execute("SELECT d FROM t ORDER BY _id").to_dict()
    assert [_unscaled(row["d"]) for row in rows] == [123, 456]
    reopened.close()
