"""Regression tests for persistent VIEWs, COPY export, and JSON mutation functions."""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

try:
    from apexbase import ApexClient
except ImportError as e:
    pytest.skip(f"ApexBase not available: {e}", allow_module_level=True)


def test_persistent_view_survives_reopen():
    with tempfile.TemporaryDirectory() as temp_dir:
        client = ApexClient(dirpath=temp_dir)
        client.create_table("default")
        client.store([{"a": 1}, {"a": 2}, {"a": 3}])
        client.flush()

        client.execute("CREATE VIEW v_keep AS SELECT a FROM default WHERE a >= 2")
        assert client.execute("SELECT * FROM v_keep ORDER BY a").to_dict() == [{"a": 2}, {"a": 3}]
        client.close()

        reopened = ApexClient(dirpath=temp_dir)
        assert reopened.execute("SELECT * FROM v_keep ORDER BY a").to_dict() == [{"a": 2}, {"a": 3}]
        reopened.execute("DROP VIEW v_keep")
        with pytest.raises(Exception):
            reopened.execute("SELECT * FROM v_keep").to_dict()
        reopened.close()


def test_copy_to_csv_and_json_roundtrip():
    with tempfile.TemporaryDirectory() as temp_dir:
        client = ApexClient(dirpath=temp_dir)
        client.create_table("people")
        client.use_table("people")
        client.store([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ])
        client.flush()

        csv_path = os.path.join(temp_dir, "people.csv")
        json_path = os.path.join(temp_dir, "people.jsonl")

        assert client.execute(f"COPY people TO '{csv_path}'").scalar() == 2
        assert client.execute(f"COPY people TO '{json_path}'").scalar() == 2

        with open(csv_path, "r", encoding="utf-8") as f:
            csv_text = f.read()
        with open(json_path, "r", encoding="utf-8") as f:
            json_text = f.read()

        assert "Alice" in csv_text and "Bob" in csv_text
        assert '"name":"Alice"' in json_text and '"name":"Bob"' in json_text

        assert client.execute(f"COPY people_csv FROM '{csv_path}'").scalar() == 2
        roundtrip = client.execute("SELECT name FROM people_csv ORDER BY name").to_dict()
        assert roundtrip == [{"name": "Alice"}, {"name": "Bob"}]
        client.close()


def test_copy_from_csv_bad_line_policy():
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_path = os.path.join(temp_dir, "dirty.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("name,age\nAlice,30\nbroken,40,extra\nBob,25\n")

        client = ApexClient(dirpath=temp_dir)
        with pytest.raises(Exception):
            client.execute(f"COPY strict_rows FROM '{csv_path}'")
        assert client.execute(
            f"COPY clean_rows FROM '{csv_path}' (on_bad_lines 'skip')"
        ).scalar() == 2
        assert client.execute(
            "SELECT name FROM clean_rows ORDER BY name"
        ).to_dict() == [{"name": "Alice"}, {"name": "Bob"}]
        client.close()


def test_json_mutation_functions():
    with tempfile.TemporaryDirectory() as temp_dir:
        client = ApexClient(dirpath=temp_dir)
        client.create_table("default")

        res = client.execute(
            """
            SELECT
              JSON_SET('{"a":1}', '$.b', 2) AS set_v,
              JSON_INSERT('{"a":1}', '$.c', 3) AS ins_v,
              JSON_REPLACE('{"a":1}', '$.a', 9) AS rep_v,
              JSON_REMOVE('{"a":1,"b":2}', '$.b') AS rem_v
            """
        ).to_dict()[0]

        assert res["set_v"] == '{"a":1,"b":2}'
        assert res["ins_v"] == '{"a":1,"c":3}'
        assert res["rep_v"] == '{"a":9}'
        assert res["rem_v"] == '{"a":1}'
        client.close()
