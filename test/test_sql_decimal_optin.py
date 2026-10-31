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


def test_decimal_aggregates_use_scaled_i128_semantics(tmp_path, monkeypatch):
    """SUM/MIN/MAX return unscaled bytes; AVG is a scaled float."""
    monkeypatch.setenv("APEX_SQL_DECIMAL", "1")
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.execute("CREATE TABLE t (d DECIMAL(10,2))")
    client.use_table("t")
    client.execute("INSERT INTO t (d) VALUES ('1.23'), ('2.50'), ('-0.25')")
    client.flush()
    row = client.execute("SELECT SUM(d) AS s, MIN(d) AS m, MAX(d) AS x, AVG(d) AS a, COUNT(d) AS n FROM t").to_dict()[0]
    assert _unscaled(row["s"]) == 348
    assert _unscaled(row["m"]) == -25
    assert _unscaled(row["x"]) == 250
    assert row["a"] == 1.16
    assert row["n"] == 3
    client.close()

def test_decimal_aggregates_decline_on_large_flushed_tables(tmp_path, monkeypatch):
    """The mmap aggregation path must also decline DECIMAL value aggregates."""
    monkeypatch.setenv("APEX_SQL_DECIMAL", "1")
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.execute("CREATE TABLE t (d DECIMAL(18,2))")
    client.use_table("t")
    payload = [(index % 1000).to_bytes(16, "little", signed=True) for index in range(200_000)]
    client.store({"d": payload})
    client.flush()
    client.close()

    reopened = ApexClient(str(tmp_path), drop_if_exists=False, enable_cache=False)
    reopened.use_table("t")
    import pytest

    row = reopened.execute("SELECT SUM(d) AS s, COUNT(*) AS n FROM t").to_dict()[0]
    # sum over 200 x (0..=999) unscaled payloads.
    assert _unscaled(row["s"]) == 200 * (999 * 1000 // 2)
    assert row["n"] == 200_000
    reopened.close()
