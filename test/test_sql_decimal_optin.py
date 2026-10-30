"""APEX_SQL_DECIMAL=1 stores SQL DECIMAL(p,s) columns as 16-byte Decimal128."""

from apexbase.client import ApexClient


def _unscaled(payload):
    return int.from_bytes(payload, "little", signed=True)


def test_sql_decimal_opt_in_round_trips_scaled_values(tmp_path, monkeypatch):
    monkeypatch.setenv("APEX_SQL_DECIMAL", "1")
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.execute("CREATE TABLE t (id INT64, d DECIMAL(10,2))")
    client.use_table("t")
    client.execute("INSERT INTO t (id, d) VALUES (1, '1.23'), (2, '-45.60'), (3, 7)")
    client.flush()
    rows = client.execute("SELECT id, d FROM t ORDER BY id").to_dict()
    assert [_unscaled(row["d"]) for row in rows] == [123, -4560, 700]
    assert client._storage.table_types("t")["d"] == "decimal(10,2)"
    # DECIMAL columns now declare the v6 format (logical-type section present).
    version = int.from_bytes(open(str(tmp_path / "t.apex"), "rb").read(16)[8:12], "little")
    assert version == 6
    client.close()


def test_sql_decimal_opt_in_rejects_extra_fractional_digits(tmp_path, monkeypatch):
    monkeypatch.setenv("APEX_SQL_DECIMAL", "1")
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.execute("CREATE TABLE t (d DECIMAL(10,2))")
    client.use_table("t")
    try:
        client.execute("INSERT INTO t (d) VALUES ('1.234')")
        raise AssertionError("expected the over-scale literal to be rejected")
    except RuntimeError as exc:
        assert "fractional digits" in str(exc)
    client.execute("INSERT INTO t (d) VALUES ('1.23')")
    client.flush()
    assert _unscaled(client.execute("SELECT d FROM t").to_dict()[0]["d"]) == 123
    client.close()
