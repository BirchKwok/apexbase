"""JSON path functions on the JSON column type (W4-2)."""
from __future__ import annotations

import tempfile

from apexbase.client import ApexClient


def _client(tmp: str) -> ApexClient:
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("docs", {"id": "int64", "doc": "json"})
    client.use_table("docs")
    client.store(
        {
            "id": [1, 2, 3, 4],
            "doc": [
                '{"a": 1, "b": {"c": "x"}, "arr": [10, 20], "flag": true}',
                '{"a": 2}',
                "not json",
                None,
            ],
        }
    )
    client.flush()
    return client


def test_json_value_scalar_paths():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            rows = client.execute(
                "SELECT id, JSON_VALUE(doc, '$.a') AS a, JSON_VALUE(doc, '$.b.c') AS c, "
                "JSON_VALUE(doc, '$.arr[1]') AS second, JSON_VALUE(doc, '$.flag') AS flag "
                "FROM docs ORDER BY id"
            ).to_dict()
            assert [row["a"] for row in rows] == ["1", "2", None, None]
            assert rows[0]["c"] == "x" and rows[1]["c"] is None
            assert rows[0]["second"] == "20"
            assert rows[0]["flag"] == "true"
        finally:
            client.close()


def test_json_value_returns_null_for_non_scalars_and_bad_input():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            rows = client.execute(
                "SELECT JSON_VALUE(doc, '$.b') AS obj, JSON_VALUE(doc, '$.arr') AS arr, "
                "JSON_VALUE(doc, '$.missing') AS gone, JSON_VALUE(doc, 'nope') AS badpath "
                "FROM docs WHERE id = 1"
            ).to_dict()
            assert rows == [{"obj": None, "arr": None, "gone": None, "badpath": None}]
            # Invalid JSON text and NULL documents stay NULL instead of failing.
            rows = client.execute(
                "SELECT JSON_VALUE(doc, '$.a') AS a FROM docs WHERE id >= 3 ORDER BY id"
            ).to_dict()
            assert [row["a"] for row in rows] == [None, None]
        finally:
            client.close()


def test_json_extract_returns_json_text_and_aliases():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            rows = client.execute(
                "SELECT JSON_EXTRACT(doc, '$.b') AS b, "
                "JSON_QUERY(doc, '$.arr') AS arr, "
                "JSON_EXTRACT(doc, '$') AS whole, "
                "JSON_EXTRACT(doc, '$[''a'']') AS quoted "
                "FROM docs WHERE id = 1"
            ).to_dict()
            assert rows[0]["b"] == '{"c":"x"}'
            assert rows[0]["arr"] == "[10,20]"
            assert rows[0]["whole"].startswith('{"a":1')
            assert rows[0]["quoted"] == "1"
        finally:
            client.close()


def test_json_path_can_come_from_a_column():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            rows = client.execute(
                "SELECT id, JSON_VALUE(doc, p) AS v FROM docs, "
                "(SELECT '$.a' AS p) paths WHERE id <= 2 ORDER BY id"
            ).to_dict()
            assert [row["v"] for row in rows] == ["1", "2"]
        finally:
            client.close()
