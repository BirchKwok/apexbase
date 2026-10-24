"""JSON column type (W4-2): stored as canonical text, projected as UTF-8."""
from __future__ import annotations

import tempfile

from apexbase.client import ApexClient

DOCS = ['{"a": 1}', '{"b": [2, 3], "c": null}', '{"a": 2, "nested": {"x": true}}']


def test_json_column_ddl_store_and_query():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.execute("CREATE TABLE docs (id BIGINT, doc JSON)")
        client.use_table("docs")
        client.execute(
            """INSERT INTO docs VALUES
               (1, '{"a": 1}'),
               (2, '{"b": [2, 3]}'),
               (3, '{"a": 2}')"""
        )
        try:
            rows = client.execute("SELECT id, doc FROM docs ORDER BY id").to_dict()
            assert [row["doc"] for row in rows] == ['{"a": 1}', '{"b": [2, 3]}', '{"a": 2}']
            # Equality/ordering go through the string representation.
            assert client.execute(
                "SELECT id FROM docs WHERE doc = '{\"a\": 1}'"
            ).to_dict() == [{"id": 1}]
            assert client.execute(
                "SELECT id FROM docs WHERE doc LIKE '{\"a\": %' ORDER BY id"
            ).to_dict() == [{"id": 1}, {"id": 3}]
            assert client.execute(
                "SELECT COUNT(DISTINCT doc) AS n FROM docs"
            ).to_dict()[0]["n"] == 3
            arrow = client.execute("SELECT doc FROM docs ORDER BY id").to_arrow()
            assert arrow.schema.field(0).type == "string"
        finally:
            client.close()


def test_json_column_python_api_roundtrip_and_reopen():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("docs", {"id": "int64", "doc": "json"})
        client.use_table("docs")
        client.store({"id": [1, 2, 3], "doc": DOCS})
        client.flush()
        client.close()

        reopened = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        reopened.use_table("docs")
        try:
            rows = reopened.execute("SELECT id, doc FROM docs ORDER BY id").to_dict()
            assert [row["doc"] for row in rows] == DOCS
            assert reopened.execute("SELECT COUNT(*) AS n FROM docs").to_dict()[0]["n"] == 3
            added = reopened.add_column("extra", "json")
            assert added is None
            reopened.execute("UPDATE docs SET extra = '{\"ok\": true}' WHERE id = 1")
            rows = reopened.execute("SELECT id, extra FROM docs ORDER BY id").to_dict()
            assert rows[0]["extra"] == '{"ok": true}'
            assert rows[1]["extra"] is None
        finally:
            reopened.close()


def test_json_null_handling_and_pandas():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("docs", {"doc": "json"})
        client.use_table("docs")
        client.store({"doc": ['{"a": 1}', None, '{"a": 3}']})
        client.flush()
        try:
            frame = client.execute("SELECT doc FROM docs").to_pandas()
            assert frame["doc"].iloc[0] == '{"a": 1}'
            assert frame["doc"].isna().iloc[1]
            assert frame["doc"].iloc[2] == '{"a": 3}'
            assert client.execute(
                "SELECT COUNT(doc) AS n FROM docs"
            ).to_dict()[0]["n"] == 2
        finally:
            client.close()
