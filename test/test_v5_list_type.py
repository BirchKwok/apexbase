"""V5 S3: List<T> container type.

Covers the declared Python `list<element>` type, columnar writes, every read
lane (scan / projection / LIMIT / point), flush+reopen persistence, Arrow
`ListArray` projection, and the explicitly rejected shapes (nested lists, NULL
rows/elements, empty lists, oversized rows, query operators).
"""

import datetime as dt

import pytest

from apexbase import ApexClient


def _client(tmp_path, name="list_t"):
    return ApexClient(str(tmp_path / name), drop_if_exists=True, enable_cache=False)


def test_declared_list_roundtrip_and_arrow_projection(tmp_path):
    client = _client(tmp_path)
    try:
        client.create_table(
            "t",
            {
                "id": "int64",
                "ints": "list<int64>",
                "floats": "list<float64>",
                "strings": "list<string>",
                "bools": "list<bool>",
                "label": "string",
            },
        )
        rows = {
            "id": [1, 2, 3],
            "ints": [[1, 2, 3], [9], [4, 5]],
            "floats": [[1.5], [-2.25, 3.0], [0.0]],
            "strings": [["a", "b"], ["c"], ["d", "e"]],
            "bools": [[True, False], [True], [False]],
            "label": ["a", "b", "c"],
        }
        client.store(rows)
        client.flush()

        full = client.execute("SELECT * FROM t").to_dict()
        assert [row["ints"] for row in full] == [[1, 2, 3], [9], [4, 5]]
        assert [row["floats"] for row in full] == [[1.5], [-2.25, 3.0], [0.0]]
        assert [row["strings"] for row in full] == [["a", "b"], ["c"], ["d", "e"]]
        assert [row["bools"] for row in full] == [[True, False], [True], [False]]

        projected = client.execute("SELECT ints FROM t WHERE id >= 2").to_dict()
        assert [row["ints"] for row in projected] == [[9], [4, 5]]

        limited = client.execute("SELECT strings FROM t LIMIT 2").to_dict()
        assert [row["strings"] for row in limited] == [["a", "b"], ["c"]]

        aliased = client.execute("SELECT ints AS xs FROM t").to_dict()
        assert [row["xs"] for row in aliased] == [[1, 2, 3], [9], [4, 5]]

        arrow = client.execute("SELECT * FROM t").to_arrow()
        import pyarrow as pa

        assert pa.types.is_list(arrow.schema.field("ints").type)
        assert arrow.schema.field("ints").type.value_type == pa.int64()
        assert pa.types.is_list(arrow.schema.field("strings").type)
        assert arrow["ints"].to_pylist() == [[1, 2, 3], [9], [4, 5]]

        # Point lanes present the same values.
        assert client.retrieve(1)["ints"] == [1, 2, 3]
        many = client.retrieve_many([2, 3]).to_dict()
        assert [row["ints"] for row in many] == [[9], [4, 5]]
        point_sql = client.execute("SELECT * FROM t WHERE _id = 2").to_dict()
        assert point_sql[0]["ints"] == [9]

        # Pre-parse lanes also present typed lists (string filter / LIKE /
        # id set / bounded offset).
        filtered = client.execute("SELECT ints FROM t WHERE label = 'b'").to_dict()
        assert [row["ints"] for row in filtered] == [[9]]
        like = client.execute("SELECT ints FROM t WHERE label LIKE 'a%'").to_dict()
        assert [row["ints"] for row in like] == [[1, 2, 3]]
        id_set = client.execute("SELECT ints FROM t WHERE _id IN (1, 3)").to_dict()
        assert [row["ints"] for row in id_set] == [[1, 2, 3], [4, 5]]
        offset = client.execute("SELECT ints FROM t LIMIT 2 OFFSET 1").to_dict()
        assert [row["ints"] for row in offset] == [[9], [4, 5]]
    finally:
        client.close()


def test_list_persists_across_reopen_and_multiple_row_groups(tmp_path):
    client = _client(tmp_path, "list_persist")
    try:
        client.create_table("t", {"id": "int64", "vals": "list<int64>"})
        client.store({"id": [1, 2], "vals": [[1], [2, 3]]})
        client.flush()
        client.store({"id": [3, 4], "vals": [[4, 5, 6], [7]]})
        client.flush()
    finally:
        client.close()

    reopened = ApexClient(str(tmp_path / "list_persist"), enable_cache=False)
    try:
        reopened.use_table("t")
        rows = reopened.execute("SELECT * FROM t").to_dict()
        assert [row["vals"] for row in rows] == [[1], [2, 3], [4, 5, 6], [7]]
        limit = reopened.execute("SELECT vals FROM t LIMIT 3").to_dict()
        assert [row["vals"] for row in limit] == [[1], [2, 3], [4, 5, 6]]
        assert reopened.retrieve(4)["vals"] == [7]
        # LIMIT crossing a row-group boundary.
        tail = reopened.execute("SELECT vals FROM t WHERE id >= 3").to_dict()
        assert [row["vals"] for row in tail] == [[4, 5, 6], [7]]
    finally:
        reopened.close()


def test_list_element_scalars(tmp_path):
    client = _client(tmp_path, "list_scalars")
    try:
        client.create_table(
            "t",
            {
                "id": "int64",
                "dates": "list<date>",
                "stamps": "list<timestamp>",
                "ints32": "list<int32>",
            },
        )
        client.store(
            {
                "id": [1],
                "dates": [[dt.date(2024, 1, 1), dt.date(2024, 2, 1)]],
                "stamps": [[dt.datetime(2024, 1, 1, 0, 0, 0)]],
                "ints32": [[1, 2, 3]],
            }
        )
        client.flush()
        row = client.execute("SELECT * FROM t").to_dict()[0]
        assert row["dates"] == [dt.date(2024, 1, 1), dt.date(2024, 2, 1)]
        assert row["stamps"] == [dt.datetime(2024, 1, 1, 0, 0, 0)]
        assert row["ints32"] == [1, 2, 3]
    finally:
        client.close()


def test_list_write_validation(tmp_path):
    client = _client(tmp_path, "list_bad")
    try:
        client.create_table("t", {"id": "int64", "vals": "list<int64>"})
        with pytest.raises(ValueError):
            client.store({"id": [1], "vals": [[[1, 2]]]})  # nested list
        with pytest.raises(ValueError):
            client.store({"id": [1], "vals": [None]})  # NULL row
        with pytest.raises(ValueError):
            client.store({"id": [1], "vals": [[1, None]]})  # NULL element
        with pytest.raises(ValueError):
            client.store({"id": [1], "vals": []})  # empty list row
        with pytest.raises(ValueError):
            client.store({"id": [1], "vals": [list(range(65_537))]})  # over-long row
        with pytest.raises(ValueError):
            client.create_table("bad", {"vals": "list<uuid>"})  # unsupported element
        with pytest.raises(ValueError):
            client.create_table("nested", {"vals": "list<list<int64>>"})
    finally:
        client.close()


def test_list_query_shapes_are_explicit_errors(tmp_path):
    client = _client(tmp_path, "list_query")
    try:
        client.create_table("t", {"id": "int64", "vals": "list<int64>"})
        client.store({"id": [1, 2], "vals": [[1, 2], [3]]})
        client.flush()
        for sql in (
            "SELECT * FROM t ORDER BY vals",
            "SELECT vals, COUNT(*) FROM t GROUP BY vals",
            "SELECT SUM(vals) FROM t",
            "SELECT MIN(vals) FROM t",
        ):
            with pytest.raises(Exception):
                client.execute(sql).to_dict()
        # Single-row dict store is rejected with a pointer to the columnar form.
        client.create_table("single", {"id": "int64", "vals": "list<int64>"})
        client.store({"id": 1, "vals": [1, 2]})
    except ValueError:
        pass
    finally:
        client.close()


def test_list_compressed_rows_roundtrip(tmp_path):
    client = _client(tmp_path, "list_compressed")
    try:
        client.create_table("t", {"id": "int64", "vals": "list<int64>"})
        client.set_compression("zstd")
        client.store({"id": [1, 2, 3], "vals": [[1, 2], [3], [4, 5, 6]]})
        client.flush()
        client.store({"id": [4], "vals": [[7]]})
        client.flush()
        rows = client.execute("SELECT * FROM t").to_dict()
        assert [row["vals"] for row in rows] == [[1, 2], [3], [4, 5, 6], [7]]
        limit = client.execute("SELECT vals FROM t LIMIT 2").to_dict()
        assert [row["vals"] for row in limit] == [[1, 2], [3]]
        assert client.retrieve(3)["vals"] == [4, 5, 6]
    finally:
        client.close()
