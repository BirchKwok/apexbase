"""The typed-read gate probes stored schema cheaply (cold-reopen fast path).

The first SELECT after a client reopen must be able to decide whether the
table needs the typed Arrow reader without paying the full `table_types()`
formatting/IO cost, and the decision must be correct for plain, time-family,
list and extension tables.
"""

import datetime as dt

from apexbase import ApexClient, register_extension_codec, unregister_extension_codec


def test_typed_read_probe_plain_table(tmp_path):
    path = str(tmp_path / "plain")
    client = ApexClient(path, drop_if_exists=True, enable_cache=False)
    try:
        client.create_table("t", {"id": "int64", "s": "string"})
        client.store({"id": [1, 2], "s": ["a", "b"]})
        client.flush()
        assert client._storage.has_typed_read_columns("t") is False
        assert client.execute("SELECT * FROM t").to_dict() == [
            {"id": 1, "s": "a"},
            {"id": 2, "s": "b"},
        ]
    finally:
        client.close()


def test_typed_read_probe_time_list_extension(tmp_path):
    register_extension_codec("probe", lambda v: v.encode(), lambda b: b.decode())
    try:
        client = ApexClient(str(tmp_path / "typed"), drop_if_exists=True, enable_cache=False)
        try:
            client.create_table(
                "ts_table", {"d": "date", "ts": "timestamp"}
            )
            client.store(
                {
                    "d": [dt.date(2024, 1, 2)],
                    "ts": [dt.datetime(2024, 1, 2, 3, 4, 5)],
                }
            )
            client.create_table("list_table", {"vals": "list<int64>"})
            client.store({"vals": [[1, 2]]})
            client.create_table("ext_table", {"payload": "extension<probe>"})
            client.store({"payload": ['{"a":1}']})
            client.flush()
            for name in ("ts_table", "list_table", "ext_table"):
                assert client._storage.has_typed_read_columns(name) is True, name
            assert client.execute("SELECT d FROM ts_table").to_dict()[0]["d"] == dt.date(
                2024, 1, 2
            )
            assert client.execute("SELECT vals FROM list_table").to_dict()[0][
                "vals"
            ] == [1, 2]
            assert client.execute("SELECT payload FROM ext_table").to_dict()[0][
                "payload"
            ] == '{"a":1}'
        finally:
            client.close()
    finally:
        unregister_extension_codec("probe")


def test_first_query_after_reopen_is_typed(tmp_path):
    path = str(tmp_path / "reopen")
    client = ApexClient(path, drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"ts": "timestamp", "vals": "list<int64>"})
    client.store(
        {
            "ts": [dt.datetime(2024, 1, 2, 3, 4, 5)],
            "vals": [[1, 2, 3]],
        }
    )
    client.flush()
    client.close()

    reopened = ApexClient(path, enable_cache=False)
    try:
        reopened.use_table("t")
        row = reopened.execute("SELECT * FROM t").to_dict()[0]
        assert row["ts"] == dt.datetime(2024, 1, 2, 3, 4, 5)
        assert row["vals"] == [1, 2, 3]
    finally:
        reopened.close()
