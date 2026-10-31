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


def test_to_arrow_uses_the_clients_declared_types_when_available(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"u": "uuid"})
    client.use_table("t")
    client.store({"u": [b"\x00" * 16]})
    client.flush()
    table = client.execute("SELECT u FROM t").to_arrow(client.describe_table())
    assert str(table.schema.field("u").type) == "fixed_size_binary[16]"
    client.close()


def test_to_arrow_without_metadata_stays_binary_after_reopen(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"u": "uuid"})
    client.use_table("t")
    client.store({"u": [b"\x00" * 16]})
    client.flush()
    client.close()

    reopened = ApexClient(str(tmp_path), drop_if_exists=False, enable_cache=False)
    reopened.use_table("t")
    table = reopened.execute("SELECT u FROM t").to_arrow()
    assert str(table.schema.field("u").type) == "binary"
    reopened.close()


def test_declared_types_do_not_mistype_same_named_columns_from_other_tables(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("dec", {"d": "decimal(38,2)"})
    client.use_table("dec")
    client.store({"d": [(123).to_bytes(16, "little", signed=True)]})
    client.flush()
    client.create_table("txt", {"d": "string"})
    client.use_table("txt")
    client.store({"d": ["hello"]})
    client.flush()
    client.use_table("dec")
    # The current table has a decimal column named d, but this result reads the
    # string column of another table: keep it as string, not decimal128.
    table = client.execute("SELECT d FROM txt").to_arrow()
    assert str(table.schema.field("d").type) == "string"
    assert table.column("d").to_pylist() == ["hello"]
    client.close()


def test_to_polars_keeps_typed_decimal_columns(tmp_path):
    polars = __import__("pytest").importorskip("polars")
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"d": "decimal(38,2)"})
    client.use_table("t")
    client.store({"d": [(123).to_bytes(16, "little", signed=True)]})
    client.flush()
    frame = client.execute("SELECT d FROM t").to_polars(client.describe_table())
    dtype = str(frame.schema["d"])
    assert "Decimal" in dtype or "decimal" in dtype.lower()
    client.close()


def test_to_arrow_projects_raw_decimal128_as_fixed_size_binary(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"d": "decimal128"})
    client.use_table("t")
    client.store({"d": [(123).to_bytes(16, "little", signed=True)]})
    client.flush()
    table = client.execute("SELECT d FROM t").to_arrow()
    assert str(table.schema.field("d").type) == "fixed_size_binary[16]"
    assert table.column("d").to_pylist() == [(123).to_bytes(16, "little", signed=True)]
    client.close()
