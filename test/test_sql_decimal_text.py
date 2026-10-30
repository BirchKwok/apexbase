"""Pin the current SQL DECIMAL representation.

The SQL parser (V5 §128/§129) maps ``DECIMAL(p,s)``/``NUMERIC(p,s)`` to a text
column: the parameters are parsed and dropped and values are stored as their
decimal string. The dedicated batch that turns this into ``ColumnType::Decimal128``
plus ``LogicalType::Decimal`` must update this test together with the INSERT
literal conversion, SELECT representation and aggregate semantics.
"""

from apexbase.client import ApexClient


def test_sql_decimal_column_is_stored_as_text_today(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    client.execute("CREATE TABLE t (id INT64, d DECIMAL(10,2))")
    client.use_table("t")
    client.execute("INSERT INTO t (id, d) VALUES (1, '1.23'), (2, '45.60')")
    client.flush()
    rows = client.execute("SELECT id, d FROM t ORDER BY id").to_dict()
    assert rows == [{"id": 1, "d": "1.23"}, {"id": 2, "d": "45.60"}]
    # Numeric comparison on the text column follows text ordering, which is the
    # behaviour the Decimal batch will change.
    assert client.execute("SELECT COUNT(*) AS n FROM t WHERE d > '1.2'").to_dict() == [{"n": 2}]
    client.close()
