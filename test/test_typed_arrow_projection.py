"""Typed Arrow projection for 16-byte parameterised columns (uuid / decimal)."""

from apexbase.client import ApexClient


def test_to_arrow_applies_declared_uuid_and_decimal_types(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"u": "uuid", "d": "decimal(38,2)"})
    client.use_table("t")
    client.store({
        "u": [b"\x00" * 15 + b"\x01"],
        "d": [(123).to_bytes(16, "little", signed=True)],
    })
    client.flush()
    result = client.execute("SELECT u, d FROM t")
    declared = client.describe_table()
    table = result.to_arrow(declared)

    assert str(table.schema.field("u").type) == "fixed_size_binary[16]"
    assert str(table.schema.field("d").type) == "decimal128(38, 2)"
    # decimal128 exposes the unscaled integer scaled by the declared scale.
    assert table.column("d").to_pylist() == [__import__("decimal").Decimal("1.23")]
    client.close()


def test_to_arrow_without_declared_types_stays_binary(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"u": "uuid"})
    client.use_table("t")
    client.store({"u": [b"\x00" * 16]})
    client.flush()
    table = client.execute("SELECT u FROM t").to_arrow()
    assert str(table.schema.field("u").type) == "binary"
    client.close()
