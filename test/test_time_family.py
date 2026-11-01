"""V5 S2: time family (timestamp/timestamptz/date/time/interval) + D5.

D5 unifies the timestamp/date presentation to Python ``datetime``/``date`` on
*every* read path. Before S2 a full scan returned ``datetime`` but ``LIMIT`` and
point reads returned raw integers. The tests below pin the presentation for all
lanes, the SQL type names and literals, the Python write path and the
MIN/MAX-vs-SUM/AVG aggregate contract.
"""

from __future__ import annotations

import datetime as dt_module

import pytest

from apexbase.client import ApexClient

DATE = dt_module.date
DATETIME = dt_module.datetime
TIME = dt_module.time
TIMEDELTA = dt_module.timedelta
UTC = dt_module.timezone.utc


def _client(tmp_path, **kwargs):
    return ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False, **kwargs)


def _insert_two_rows(client):
    client.execute(
        "CREATE TABLE t (id INT64, ts TIMESTAMP, tstz TIMESTAMPTZ, d DATE, tm TIME, iv INTERVAL)"
    )
    client.use_table("t")
    client.execute(
        "INSERT INTO t (id, ts, tstz, d, tm, iv) VALUES "
        "(1, '2024-01-02 03:04:05', '2024-01-02T03:04:05+00:00', '2024-01-02', '03:04:05', '1 day'), "
        "(2, '2024-01-03 04:05:06.5', '2024-01-03T04:05:06.5+00:00', '2024-01-03', '04:05:06.5', '2 hours')"
    )


def test_sql_time_family_ddl_and_literals(tmp_path):
    client = _client(tmp_path)
    try:
        _insert_two_rows(client)
        types = client._storage.table_types("t")
        assert types == {
            "id": "int64",
            "ts": "timestamp",
            "tstz": "timestamptz",
            "d": "date",
            "tm": "time",
            "iv": "interval",
        }
        version = int.from_bytes(open(str(tmp_path / "t.apex"), "rb").read(16)[8:12], "little")
        assert version == 5
        rows = client.execute("SELECT * FROM t ORDER BY id").to_dict()
        first, second = rows
        assert first["ts"] == DATETIME(2024, 1, 2, 3, 4, 5)
        assert first["tstz"].utcoffset() == TIMEDELTA(0)
        assert first["tstz"].replace(tzinfo=None) == DATETIME(2024, 1, 2, 3, 4, 5)
        assert first["d"] == DATE(2024, 1, 2)
        assert first["tm"] == TIME(3, 4, 5)
        assert first["iv"] == TIMEDELTA(days=1)
        assert second["ts"] == DATETIME(2024, 1, 3, 4, 5, 6, 500_000)
        assert second["tm"] == TIME(4, 5, 6, 500_000)
        assert second["iv"] == TIMEDELTA(hours=2)
    finally:
        client.close()


def test_d5_presentation_on_every_read_lane(tmp_path):
    """D5: raw integers must never leak from LIMIT/point/projection lanes."""
    client = _client(tmp_path)
    try:
        _insert_two_rows(client)
        expected_ts = DATETIME(2024, 1, 2, 3, 4, 5)
        expected_d = DATE(2024, 1, 2)

        shapes = [
            "SELECT * FROM t",
            "SELECT * FROM t LIMIT 1",
            "SELECT * FROM t WHERE _id = 1",
            "SELECT ts, d FROM t LIMIT 1",
            "SELECT ts FROM t WHERE id = 1",
            "SELECT ts, d FROM t WHERE ts > '2024-01-01' ORDER BY ts LIMIT 1",
        ]
        for sql in shapes:
            row = client.execute(sql).to_dict()[0]
            assert row["ts"] == expected_ts, sql
            if "d" in row:
                assert row["d"] == expected_d, sql

        # Same lanes after a flush (mmap path) and after a reopen.
        client.flush()
        for sql in shapes:
            row = client.execute(sql).to_dict()[0]
            assert row["ts"] == expected_ts, sql
    finally:
        client.close()

    reopened = ApexClient(str(tmp_path), drop_if_exists=False, enable_cache=False)
    try:
        reopened.use_table("t")
        for sql in (
            "SELECT * FROM t LIMIT 1",
            "SELECT * FROM t WHERE _id = 1",
            "SELECT ts, d FROM t LIMIT 1",
        ):
            row = reopened.execute(sql).to_dict()[0]
            assert row["ts"] == DATETIME(2024, 1, 2, 3, 4, 5), sql
            assert row["d"] == DATE(2024, 1, 2), sql
    finally:
        reopened.close()


def test_python_store_writes_datetime_objects(tmp_path):
    client = _client(tmp_path)
    try:
        client.create_table(
            "p",
            {
                "id": "int64",
                "ts": "timestamp",
                "tstz": "timestamptz",
                "d": "date",
                "tm": "time",
                "iv": "interval",
            },
        )
        client.use_table("p")
        client.store(
            {
                "id": [1, 2],
                "ts": [DATETIME(2024, 1, 2, 3, 4, 5), DATETIME(2024, 1, 3)],
                "tstz": [
                    DATETIME(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
                    DATETIME(2024, 1, 3, tzinfo=dt_module.timezone(TIMEDELTA(hours=8))),
                ],
                "d": [DATE(2024, 1, 2), DATE(2024, 1, 3)],
                "tm": [TIME(3, 4, 5), TIME(4, 5, 6, 500_000)],
                "iv": [TIMEDELTA(days=1), TIMEDELTA(hours=-2)],
            }
        )
        rows = client.execute("SELECT * FROM p ORDER BY id").to_dict()
        assert rows[0]["ts"] == DATETIME(2024, 1, 2, 3, 4, 5)
        assert rows[0]["tstz"].utcoffset() == TIMEDELTA(0)
        assert rows[0]["d"] == DATE(2024, 1, 2)
        assert rows[0]["tm"] == TIME(3, 4, 5)
        assert rows[0]["iv"] == TIMEDELTA(days=1)
        # The +08:00 aware value is normalized to the same instant in UTC.
        assert rows[1]["tstz"].replace(tzinfo=None) == DATETIME(2024, 1, 2, 16, 0, 0)
        assert rows[1]["tm"] == TIME(4, 5, 6, 500_000)
        assert rows[1]["iv"] == TIMEDELTA(hours=-2)

        # Single-row (scalar dict) store uses the same conversion.
        client.store({"id": 3, "ts": DATETIME(2024, 2, 1, 1, 2, 3), "d": DATE(2024, 2, 1)})
        row = client.execute("SELECT * FROM p WHERE _id = 3").to_dict()[0]
        assert row["ts"] == DATETIME(2024, 2, 1, 1, 2, 3)
        assert row["d"] == DATE(2024, 2, 1)
    finally:
        client.close()


def test_time_family_comparisons_and_ordering(tmp_path):
    client = _client(tmp_path)
    try:
        _insert_two_rows(client)
        client.execute(
            "INSERT INTO t (id, ts) VALUES (3, '2024-01-01 00:00:00'), (4, NULL)"
        )
        assert [
            row["id"]
            for row in client.execute(
                "SELECT id FROM t WHERE ts > '2024-01-01 12:00:00' ORDER BY id"
            ).to_dict()
        ] == [1, 2]
        assert [
            row["id"]
            for row in client.execute(
                "SELECT id FROM t WHERE ts BETWEEN '2024-01-01' AND '2024-01-02 12:00:00' ORDER BY id"
            ).to_dict()
        ] == [1, 3]
        assert [
            row["id"]
            for row in client.execute(
                "SELECT id FROM t WHERE d IN ('2024-01-02', '2024-01-03') ORDER BY id"
            ).to_dict()
        ] == [1, 2]
        assert [
            row["id"] for row in client.execute("SELECT id FROM t ORDER BY ts LIMIT 2").to_dict()
        ] == [3, 1]
        # Descending puts NULLs first (nulls_first defaults to descending).
        assert [
            row["id"]
            for row in client.execute("SELECT id FROM t ORDER BY ts DESC LIMIT 2").to_dict()
        ] == [4, 2]
    finally:
        client.close()


def test_invalid_time_literals_are_rejected(tmp_path):
    client = _client(tmp_path)
    try:
        _insert_two_rows(client)
        with pytest.raises(RuntimeError):
            client.execute("SELECT id FROM t WHERE ts > 'not-a-time'")
        with pytest.raises(RuntimeError):
            client.execute("INSERT INTO t (id, d) VALUES (9, '2024-13-01')")
        with pytest.raises(RuntimeError):
            client.execute("INSERT INTO t (id, tm) VALUES (9, '25:00:00')")
    finally:
        client.close()


def test_min_max_keep_the_typed_value_and_sum_avg_are_rejected(tmp_path):
    client = _client(tmp_path)
    try:
        _insert_two_rows(client)
        row = client.execute("SELECT MIN(ts) AS mn, MAX(ts) AS mx, MIN(d) AS dn FROM t").to_dict()[0]
        assert row["mn"] == DATETIME(2024, 1, 2, 3, 4, 5)
        assert row["mx"] == DATETIME(2024, 1, 3, 4, 5, 6, 500_000)
        assert row["dn"] == DATE(2024, 1, 2)
        # With a WHERE the aggregate runs through the filtered batch path.
        row = client.execute(
            "SELECT MIN(ts) AS mn, MAX(iv) AS mx FROM t WHERE id <= 2"
        ).to_dict()[0]
        assert row["mn"] == DATETIME(2024, 1, 2, 3, 4, 5)
        assert row["mx"] == TIMEDELTA(days=1)
        for statement in ("SELECT SUM(ts) AS s FROM t", "SELECT AVG(ts) AS a FROM t"):
            with pytest.raises(RuntimeError, match="numeric"):
                client.execute(statement)
    finally:
        client.close()


def test_group_by_time_family_min_max_and_public_retrieve(tmp_path):
    client = _client(tmp_path)
    try:
        _insert_two_rows(client)
        rows = client.execute(
            "SELECT d, MIN(ts) AS mn, MAX(ts) AS mx FROM t GROUP BY d ORDER BY d"
        ).to_dict()
        assert rows[0]["mn"] == DATETIME(2024, 1, 2, 3, 4, 5)
        assert rows[0]["mx"] == DATETIME(2024, 1, 2, 3, 4, 5)
        assert rows[1]["mn"] == DATETIME(2024, 1, 3, 4, 5, 6, 500_000)
        # Public row-retrieval API also presents the family (D5).
        row = client.retrieve(1)
        assert row is not None
        assert row["ts"] == DATETIME(2024, 1, 2, 3, 4, 5)
        assert row["d"] == DATE(2024, 1, 2)
        many = client.retrieve_many([1, 2]).to_dict()
        assert many[0]["ts"] == DATETIME(2024, 1, 2, 3, 4, 5)
        assert many[1]["iv"] == TIMEDELTA(hours=2)
    finally:
        client.close()


def test_arrow_projection_uses_typed_columns(tmp_path):
    client = _client(tmp_path)
    try:
        _insert_two_rows(client)
        schema = client.execute("SELECT * FROM t LIMIT 1").to_arrow().schema
        assert schema.field("ts").type == "timestamp[us]"
        tz_type = str(schema.field("tstz").type)
        assert tz_type.startswith("timestamp[us, tz=")
        assert schema.field("d").type == "date32[day]"
        assert str(schema.field("tm").type).startswith("time64[us]")
        assert str(schema.field("iv").type).startswith("duration[us]")
    finally:
        client.close()


def test_unsupported_time_type_names_are_explicit_errors(tmp_path):
    client = _client(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="Unknown data type"):
            client.execute("CREATE TABLE bad (x NO_SUCH_TIME_TYPE)")
        with pytest.raises(ValueError, match="Unknown column type"):
            client.create_table("bad2", {"x": "no_such_time_type"})
    finally:
        client.close()
