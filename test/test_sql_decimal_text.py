"""`APEX_SQL_DECIMAL=0` restores the legacy text representation.

The V5 default stores ``DECIMAL(p,s)`` as the fixed 16-byte ``Decimal128``
payload with numeric comparison/ordering semantics (``test_sql_decimal_semantics``
pins those). This test keeps the explicit opt-out honest so older deployments
that still compare the decimal string can keep that behaviour.
"""

from apexbase.client import ApexClient


def test_sql_decimal_column_falls_back_to_text(tmp_path, monkeypatch):
    monkeypatch.setenv("APEX_SQL_DECIMAL", "0")
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.execute("CREATE TABLE t (id INT64, d DECIMAL(10,2))")
    client.use_table("t")
    client.execute("INSERT INTO t (id, d) VALUES (1, '1.23'), (2, '45.60')")
    client.flush()
    rows = client.execute("SELECT id, d FROM t ORDER BY id").to_dict()
    assert rows == [{"id": 1, "d": "1.23"}, {"id": 2, "d": "45.60"}]
    # Text-ordering comparison is what the opt-out keeps.
    assert client.execute("SELECT COUNT(*) AS n FROM t WHERE d > '1.2'").to_dict() == [{"n": 2}]
    # A DECIMAL declared while the switch is off must not gain the logical type.
    assert client._storage.table_types("t")["d"] != "decimal(10,2)"
    client.close()
