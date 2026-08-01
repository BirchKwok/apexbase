"""Regression coverage for the correctness and safety issues reported in issue.md."""

from __future__ import annotations

import math
import multiprocessing
from datetime import date, datetime, timezone

import numpy as np
import pytest

from apexbase import ApexClient


def _client(tmp_path, name: str) -> ApexClient:
    return ApexClient(dirpath=str(tmp_path / name), drop_if_exists=True)


def _concurrent_writer(root: str, start: int) -> None:
    client = ApexClient(dirpath=root)
    try:
        client.use_table("t")
        client.store({"id": list(range(start, start + 50))})
        client.flush()
    finally:
        client.close()


def _external_visibility_writer(root: str, row_id: int) -> None:
    client = ApexClient(dirpath=root)
    try:
        client.use_table("t")
        client.store({"id": [row_id], "name": [f"child-{row_id}"]})
        client.flush()
    finally:
        client.close()


def test_topk_parser_binding_filter_and_safety(tmp_path):
    client = _client(tmp_path, "topk")
    try:
        client.execute(
            "CREATE TABLE vecs (name TEXT, payload BLOB, vec FLOAT16_VECTOR)"
        )
        client.use_table("vecs")
        client.store(
            {
                "name": ["x", "y", "z"],
                "payload": [b"x" * 32, b"y" * 32, b"z" * 32],
                "vec": [
                    np.array([1.0, 0.0], dtype=np.float32),
                    np.array([0.0, 1.0], dtype=np.float32),
                    np.array([-1.0, 0.0], dtype=np.float32),
                ],
            }
        )
        client.flush()

        plus = client.execute(
            "SELECT explode_rename("
            "topk_distance(vec, [+1e0, +0.0], 1, 'l2'), '_id', 'dist'"
            ") FROM vecs"
        ).to_dict()
        assert len(plus) == 1
        assert plus[0]["dist"] == pytest.approx(0.0, abs=1e-4)

        bound = client.execute(
            "SELECT explode_rename("
            "topk_distance(vec, ?, 2, 'l2'), 'row_id', 'distance'"
            ") FROM vecs",
            params=[np.array([1.0, 0.0], dtype=np.float32)],
        ).to_dict()
        assert list(bound[0]) == ["row_id", "distance"]
        assert bound[0]["distance"] == pytest.approx(0.0, abs=1e-4)

        filtered = client.execute(
            "SELECT explode_rename("
            "topk_distance(vec, [1.0, 0.0], 3, 'l2'), '_id', 'dist'"
            ") FROM vecs WHERE name = 'y'"
        ).to_dict()
        assert len(filtered) == 1
        assert filtered[0]["_id"] == 2

        with pytest.raises((RuntimeError, ValueError), match="dimension"):
            client.topk_distance("vec", [1.0], k=1)
        with pytest.raises(ValueError, match="finite"):
            client.topk_distance("vec", [math.nan, 0.0], k=1)
        with pytest.raises(ValueError, match="positive"):
            client.topk_distance("vec", [1.0, 0.0], k=0)

        assert client.delete(id=2)
        remaining = client.topk_distance("vec", [1.0, 0.0], k=100).to_dict()
        assert len(remaining) == 2
        assert {row["_id"] for row in remaining} == {1, 3}
    finally:
        client.close()


def test_topk_join_prunes_blob_and_qualified_filters_work(tmp_path):
    client = _client(tmp_path, "joins")
    try:
        client.execute(
            "CREATE TABLE products "
            "(product_id INT, name TEXT, payload BLOB, vec FLOAT16_VECTOR)"
        )
        client.use_table("products")
        client.store(
            {
                "product_id": [10, 20, 30],
                "name": ["x", "y", "z"],
                "payload": [b"a" * 4096, b"b" * 4096, b"c" * 4096],
                "vec": [
                    np.array([1.0, 0.0], dtype=np.float32),
                    np.array([0.0, 1.0], dtype=np.float32),
                    np.array([-1.0, 0.0], dtype=np.float32),
                ],
            }
        )
        client.flush()
        rows = client.execute(
            """
            SELECT p.name, k.dist
            FROM products p
            JOIN (
                SELECT explode_rename(
                    topk_distance(vec, [1.0, 0.0], 2, 'l2'),
                    '_id', 'dist'
                ) FROM products
            ) k ON p._id = k._id
            ORDER BY k.dist
            """
        ).to_dict()
        assert [row["name"] for row in rows] == ["x", "y"]

        matched = client.execute(
            """
            SELECT p.name
            FROM products p
            LEFT JOIN (
                SELECT explode_rename(
                    topk_distance(vec, [1.0, 0.0], 2, 'l2'),
                    '_id', 'dist'
                ) FROM products
            ) k ON p._id = k._id
            WHERE k._id IS NOT NULL
            ORDER BY p.name
            """
        ).to_dict()
        assert [row["name"] for row in matched] == ["x", "y"]

        unmatched = client.execute(
            """
            SELECT p.name
            FROM products p
            LEFT JOIN (
                SELECT explode_rename(
                    topk_distance(vec, [1.0, 0.0], 2, 'l2'),
                    '_id', 'dist'
                ) FROM products
            ) k ON p._id = k._id
            WHERE k._id IS NULL
            """
        ).to_dict()
        assert unmatched == [{"name": "z"}]

        client.execute("CREATE TABLE ids (product_id INT)")
        client.use_table("ids")
        client.store({"product_id": [10, 20, 30]})
        client.flush()
        client.use_table("products")
        cross = client.execute(
            "SELECT p.name FROM products p CROSS JOIN ids i "
            "WHERE p.product_id = i.product_id ORDER BY p.name"
        ).to_dict()
        assert [row["name"] for row in cross] == ["x", "y", "z"]
    finally:
        client.close()


def test_blob_projection_limit_ctas_and_insert_select(tmp_path):
    client = _client(tmp_path, "blob")
    try:
        client.execute("CREATE TABLE src (id INT, payload BLOB)")
        client.use_table("src")
        blobs = [b"\x00a", b"\x01b", b"\x02c"]
        client.store({"id": [1, 2, 3], "payload": blobs})
        client.flush()

        projected = client.execute(
            "SELECT id, payload FROM src WHERE id IN (1, 2, 3) ORDER BY id"
        ).to_dict()
        assert [row["payload"] for row in projected] == blobs
        offset = client.execute(
            "SELECT payload FROM src ORDER BY id LIMIT 1 OFFSET 1"
        ).to_dict()
        assert offset == [{"payload": blobs[1]}]

        result = client.execute("SELECT payload FROM src ORDER BY id")
        arrow_table = result.to_arrow()
        assert str(arrow_table.schema.field("payload").type) == "large_binary"
        batches = result.to_record_batches(max_chunksize=1)
        assert len(batches) == 3
        assert all(batch.num_rows == 1 for batch in batches)

        client.execute("CREATE TABLE copied AS SELECT id, payload FROM src")
        client.execute("CREATE TABLE inserted (id INT, payload BLOB)")
        client.execute("INSERT INTO inserted SELECT id, payload FROM src")
        for table in ("copied", "inserted"):
            rows = client.execute(
                f"SELECT id, payload FROM {table} ORDER BY id"
            ).to_dict()
            assert [row["payload"] for row in rows] == blobs
    finally:
        client.close()


def test_dml_type_and_arity_validation(tmp_path):
    client = _client(tmp_path, "dml")
    try:
        client.execute("CREATE TABLE t (id INT, score DOUBLE, name TEXT)")
        client.use_table("t")
        client.store({"id": [1, 2], "score": [1.5, 2.5], "name": ["a", "b"]})
        client.flush()

        client.execute("UPDATE t SET score = 99 WHERE id = 1")
        assert client.execute("SELECT score FROM t WHERE id = 1").scalar() == 99.0
        with pytest.raises(RuntimeError):
            client.execute("UPDATE t SET id = 'not-an-int' WHERE id = 1")

        assert client.execute("SELECT id FROM t WHERE id = '2'").to_dict() == [{"id": 2}]
        with pytest.raises(RuntimeError):
            client.execute("SELECT id FROM t WHERE id = 'two'")

        with pytest.raises(RuntimeError):
            client.execute("INSERT INTO t (id, score) VALUES (3)")
        with pytest.raises(RuntimeError):
            client.execute("INSERT INTO t (id) VALUES (3, 4)")

        client.execute("CREATE TABLE dst (name TEXT, id INT, score DOUBLE)")
        client.execute("INSERT INTO dst SELECT name, id, score FROM t")
        copied = client.execute("SELECT name, id, score FROM dst ORDER BY id").to_dict()
        assert copied[0] == {"name": "a", "id": 1, "score": 99.0}
    finally:
        client.close()


def test_null_semantics_coalesce_count_group_and_empty_string(tmp_path):
    client = _client(tmp_path, "nulls")
    try:
        client.execute(
            "CREATE TABLE t (id INT, name TEXT, score DOUBLE, flag BOOL)"
        )
        client.use_table("t")
        client.store(
            {
                "id": [1, 2, 3],
                "name": ["", None, "x"],
                "score": [None, 2.0, 3.0],
                "flag": [True, None, False],
            }
        )
        client.flush()

        assert client.execute("SELECT id FROM t WHERE name = ''").to_dict() == [{"id": 1}]
        counts = client.execute(
            "SELECT COUNT(*) n, COUNT(name) cn, COUNT(score) cs, COUNT(flag) cf FROM t"
        ).to_dict()[0]
        assert counts == {"n": 3, "cn": 2, "cs": 2, "cf": 2}

        coalesced = client.execute(
            "SELECT id, COALESCE(score, 0) value FROM t ORDER BY id"
        ).to_dict()
        assert [row["value"] for row in coalesced] == [0.0, 2.0, 3.0]

        grouped = client.execute(
            "SELECT name, COUNT(*) n FROM t GROUP BY name ORDER BY n"
        ).to_dict()
        assert any(row["name"] is None and row["n"] == 1 for row in grouped)
        assert any(row["name"] == "" and row["n"] == 1 for row in grouped)

        # Warm the no-NULL fast-path cache, then verify a write invalidates it.
        client.execute("CREATE TABLE warm_groups (left_key TEXT, right_key TEXT)")
        client.use_table("warm_groups")
        client.store({"left_key": ["a"], "right_key": ["x"]})
        client.flush()
        client.execute(
            "SELECT left_key, right_key, COUNT(*) n "
            "FROM warm_groups GROUP BY left_key, right_key"
        )
        client.store({"left_key": [None], "right_key": ["y"]})
        client.flush()
        regrouped = client.execute(
            "SELECT left_key, right_key, COUNT(*) n "
            "FROM warm_groups GROUP BY left_key, right_key"
        ).to_dict()
        assert any(row["left_key"] is None and row["n"] == 1 for row in regrouped)
    finally:
        client.close()


def test_update_delete_and_retrieve_paths_share_current_visibility(tmp_path):
    client = _client(tmp_path, "visibility")
    try:
        client.execute(
            "CREATE TABLE t "
            "(id INT, title TEXT, score DOUBLE, payload BLOB, vec FLOAT16_VECTOR)"
        )
        client.use_table("t")
        client.store(
            {
                "id": [1, 2],
                "title": ["old", "keep"],
                "score": [1.0, 2.0],
                "payload": [b"old", b"keep"],
                "vec": [
                    np.array([1.0, 0.0], dtype=np.float32),
                    np.array([0.0, 1.0], dtype=np.float32),
                ],
            }
        )
        client.flush()

        # Exact internal-ID updates use the mmap fast path.  The new value is
        # outside the original [1, 2] zone map and must remain visible after
        # cache invalidation and a subsequent predicate scan.
        client.execute("UPDATE t SET score = 9.0 WHERE _id = 1")
        assert client.execute("SELECT SUM(score) total FROM t").scalar() == 11.0
        assert client.execute("SELECT id FROM t WHERE score = 9.0").to_dict() == [{"id": 1}]
        assert client.delete(where="score = 9.0") == 1
        assert client.retrieve(1) is None

        assert client.delete(id=2)
        assert client.retrieve_many([1, 2]).to_dict() == []
        assert client.read_blob("payload", 2) is None
        assert client.read_blob_range("payload", 2, 0, 2) is None
        assert client.topk_distance("vec", [0.0, 1.0], k=10).to_dict() == []
    finally:
        client.close()


def test_numeric_range_update_refreshes_stats_and_zone_maps_after_reopen(tmp_path):
    root = tmp_path / "range_update"
    client = ApexClient(dirpath=str(root), drop_if_exists=True)
    try:
        client.execute("CREATE TABLE t (id INT, score DOUBLE)")
        client.use_table("t")
        client.store({"id": [1, 2], "score": [1.0, 2.0]})
        client.flush()

        # Avoid _id so this exercises the mmap range-update path.  The
        # replacement deliberately lies outside the old score zone map.
        assert client.execute(
            "UPDATE t SET score = 20.0 WHERE id BETWEEN 1 AND 1"
        ).scalar() == 1
        assert client.execute("SELECT SUM(score) FROM t").scalar() == 22.0
        assert client.execute(
            "SELECT id FROM t WHERE score = 20.0"
        ).to_dict() == [{"id": 1}]
        client.close()

        client = ApexClient(dirpath=str(root))
        client.use_table("t")
        assert client.execute("SELECT SUM(score) FROM t").scalar() == 22.0
        assert client.execute(
            "SELECT id FROM t WHERE score = 20.0"
        ).to_dict() == [{"id": 1}]
    finally:
        client.close()


def test_inplace_update_aborts_if_exact_stats_cannot_be_invalidated(tmp_path):
    root = tmp_path / "stats_failure"
    client = ApexClient(dirpath=str(root), drop_if_exists=True)
    sidecar = None
    try:
        client.execute("CREATE TABLE t (id INT, score DOUBLE)")
        client.use_table("t")
        client.store({"id": [1, 2], "score": [1.0, 2.0]})
        client.flush()

        apex_file = next(root.rglob("t.apex"))
        sidecar = apex_file.with_name(f"{apex_file.name}.stats")
        assert sidecar.is_file()
        sidecar.unlink()
        sidecar.mkdir()

        with pytest.raises((RuntimeError, OSError)):
            client.execute("UPDATE t SET score = 20.0 WHERE id = 1")
        assert client.execute("SELECT score FROM t WHERE id = 1").scalar() == 1.0
    finally:
        if sidecar is not None and sidecar.is_dir():
            sidecar.rmdir()
        client.close()


def test_execute_batch_preserves_statement_order(tmp_path):
    client = _client(tmp_path, "batch")
    try:
        client.execute("CREATE TABLE t (id INT, value INT)")
        client.use_table("t")
        results = client.execute_batch(
            [
                "INSERT INTO t (id, value) VALUES (1, 10)",
                "UPDATE t SET value = 20 WHERE id = 1",
                "SELECT value FROM t WHERE id = 1",
                "DELETE FROM t WHERE value = 20",
                "SELECT COUNT(*) n FROM t",
            ]
        )
        assert results[2].scalar() == 20
        assert results[4].scalar() == 0
        with pytest.raises(ValueError, match="read-only"):
            client.execute_batch_parallel(["INSERT INTO t (id, value) VALUES (2, 2)"])
    finally:
        client.close()


def test_schema_changes_are_null_safe_and_fail_loud(tmp_path):
    root = tmp_path / "schema"
    client = ApexClient(dirpath=str(root), drop_if_exists=True)
    try:
        client.execute("CREATE TABLE t (id INT, old_name TEXT)")
        client.use_table("t")
        client.store({"id": [1, 2], "old_name": ["a", "b"]})
        client.flush()
        client.add_column("added", "float")
        rows = client.execute("SELECT added FROM t").to_dict()
        assert rows == [{"added": None}, {"added": None}]
        client.execute("CREATE TABLE infinities (id INT, score DOUBLE)")
        client.use_table("infinities")
        client.store({"id": [1, 2], "score": [math.inf, -math.inf]})
        client.flush()
        assert client.execute("SELECT id FROM infinities WHERE score > 0").scalar() == 1
        assert client.execute("SELECT id FROM infinities WHERE score < 0").scalar() == 2
        client.use_table("t")

        client.rename_column("old_name", "new_name")
        assert client.execute(
            "SELECT new_name FROM t ORDER BY id"
        ).to_dict() == [{"new_name": "a"}, {"new_name": "b"}]
        with pytest.raises(RuntimeError):
            client.execute("SELECT missing FROM t")
        client.close()

        client = ApexClient(dirpath=str(root))
        client.use_table("t")
        assert client.execute("SELECT new_name FROM t ORDER BY id").to_dict() == [
            {"new_name": "a"},
            {"new_name": "b"},
        ]
    finally:
        client.close()


def test_pandas_nulls_and_empty_arrow_schema_are_preserved(tmp_path):
    pd = pytest.importorskip("pandas")
    client = _client(tmp_path, "pandas")
    try:
        client.execute("CREATE TABLE t (id INT, name TEXT, score DOUBLE)")
        client.use_table("t")
        client.from_pandas(
            pd.DataFrame(
                {
                    "id": pd.Series([1, 2], dtype="Int64"),
                    "name": ["a", None],
                    "score": [1.0, np.nan],
                }
            )
        )
        rows = client.execute("SELECT id, name, score FROM t ORDER BY id").to_dict()
        assert rows[1] == {"id": 2, "name": None, "score": None}

        empty = client.execute("SELECT id, name FROM t WHERE id < 0").to_arrow()
        assert empty.column_names == ["id", "name"]
        assert empty.num_rows == 0
    finally:
        client.close()


def test_temp_csv_encoding_and_catalog_sql(tmp_path):
    source = tmp_path / "latin1.csv"
    source.write_bytes("id,price,name\n1,1.5,caf\xe9\n2,2.5,tea\n".encode("latin-1"))
    client = _client(tmp_path, "catalog")
    try:
        client.execute("CREATE TABLE base (id INT)")
        client.use_table("base")
        client.register_temp_table("imported", str(source), encoding="latin-1")
        row = client.execute("SELECT id, price, name FROM imported WHERE id = 1").to_dict()[0]
        assert row == {"id": 1, "price": 1.5, "name": "café"}
        client.use_table("base")
        client.add_column("name", "string")
        client.execute("UPDATE base SET name = 'base' WHERE id IS NULL")
        client.store({"id": 10, "name": "base"})
        client.flush()
        joined = client.execute(
            "SELECT base.name AS base_name, imported.name AS imported_name "
            "FROM base CROSS JOIN imported WHERE base.id = 10 ORDER BY imported.id"
        ).to_dict()
        assert joined == [
            {"base_name": "base", "imported_name": "café"},
            {"base_name": "base", "imported_name": "tea"},
        ]

        tables = {row["table_name"] for row in client.execute("SHOW TABLES").to_dict()}
        assert "base" in tables
        described = client.execute("DESCRIBE base").to_dict()
        assert any(row["column_name"] == "id" for row in described)
        assert client.execute("SHOW DATABASES").to_dict()
    finally:
        client.close()


def test_fts_configuration_boolean_fuzzy_and_stats(tmp_path):
    client = _client(tmp_path, "fts")
    try:
        client.execute(
            "CREATE TABLE docs "
            "(id INT, title TEXT, body TEXT, vec FLOAT16_VECTOR)"
        )
        client.use_table("docs")
        client.store(
            {
                "id": [1, 2, 3, 4],
                "title": ["Apple Juice", "Apple Pie", "Banana Pie", "O'Reilly"],
                "body": ["red drink", "green tart", "yellow tart", "publisher"],
                "vec": [
                    np.array([1.0, 0.0], dtype=np.float32),
                    np.array([0.9, 0.1], dtype=np.float32),
                    np.array([0.0, 1.0], dtype=np.float32),
                    np.array([-1.0, 0.0], dtype=np.float32),
                ],
            }
        )
        client.flush()

        with pytest.raises(ValueError):
            client.init_fts(index_fields=["missing"])
        client.init_fts(index_fields=["title"])
        assert client.execute(
            "SELECT id FROM docs WHERE MATCH('Apple -Pie')"
        ).to_dict() == [{"id": 1}]
        assert client.execute(
            "SELECT id FROM docs WHERE MATCH('O''Reilly')"
        ).to_dict() == [{"id": 4}]
        assert client.execute(
            "SELECT id FROM docs WHERE MATCH('Apple OR Banana') ORDER BY id"
        ).to_dict() == [{"id": 1}, {"id": 2}, {"id": 3}]
        topk_match = client.execute(
            "SELECT explode_rename("
            "topk_distance(vec, [1.0, 0.0], 4, 'l2'), '_id', 'dist'"
            ") FROM docs WHERE MATCH('Apple')"
        ).to_dict()
        assert {row["_id"] for row in topk_match} == {1, 2}

        fuzzy = client.search_text_with_scores(
            "Aple", limit=10, fuzzy=True, min_results=2
        )
        assert fuzzy
        assert all(isinstance(row_id, int) for row_id, _ in fuzzy)

        client.init_fts(index_fields=["body"])
        assert client.search_text("Apple").size == 0
        assert client.search_text("drink").size == 1
        client.execute("UPDATE docs SET body = 'fresh drink' WHERE id = 1")
        assert client.search_and_retrieve("drink").to_dict()[0]["body"] == "fresh drink"

        client.delete(id=3)
        stats = client.get_fts_stats()
        assert stats["doc_count"] == 3
        client.disable_fts()
        with pytest.raises((RuntimeError, ValueError), match="disabled|not enabled"):
            client.execute("SELECT id FROM docs WHERE MATCH('drink')")
    finally:
        client.close()


def test_lance_roundtrip_preserves_vector_and_temporal_types(tmp_path):
    lance = pytest.importorskip("lance")
    pa = pytest.importorskip("pyarrow")
    client = _client(tmp_path, "lance_src")
    imported = _client(tmp_path, "lance_dst")
    try:
        client.from_pyarrow(
            pa.table(
                {
                    "vec": pa.array(
                        [[1.0, 2.0], [3.0, 4.0]],
                        type=pa.list_(pa.float32(), 2),
                    ),
                    "day": pa.array(
                        [date(2026, 1, 1), date(2026, 1, 2)], type=pa.date32()
                    ),
                    "created": pa.array(
                        [
                            datetime(2026, 1, 1, tzinfo=timezone.utc),
                            datetime(2026, 1, 2, tzinfo=timezone.utc),
                        ],
                        type=pa.timestamp("ms"),
                    ),
                }
            ),
            table_name="typed",
        )
        uri = str(tmp_path / "typed.lance")
        client.to_lance(uri)
        imported.from_lance(uri, table_name="typed")
        imported.use_table("typed")
        schema = imported.execute("DESCRIBE typed").to_dict()
        type_by_name = {row["column_name"]: row["data_type"] for row in schema}
        assert "float16" in type_by_name["vec"].lower()
        assert "date" in type_by_name["day"].lower()
        assert "timestamp" in type_by_name["created"].lower()
        assert lance.dataset(uri).count_rows() == 2

        empty_uri = str(tmp_path / "empty.lance")
        client.from_pyarrow(
            pa.table(
                {
                    "id": pa.array([], type=pa.int64()),
                    "name": pa.array([], type=pa.string()),
                }
            ),
            table_name="empty",
        )
        client.to_lance(empty_uri)
        empty_imported = _client(tmp_path, "empty_lance_dst")
        try:
            empty_imported.from_lance(empty_uri, table_name="empty")
            assert empty_imported.list_fields() == ["id", "name"]
        finally:
            empty_imported.close()
    finally:
        client.close()
        imported.close()


def test_correlated_text_subquery_and_process_safe_writes(tmp_path):
    root = tmp_path / "concurrency"
    client = ApexClient(dirpath=str(root), drop_if_exists=True)
    try:
        client.execute("CREATE TABLE t (id INT)")
        client.use_table("t")
        client.flush()
    finally:
        client.close()

    ctx = multiprocessing.get_context("spawn")
    workers = [
        ctx.Process(target=_concurrent_writer, args=(str(root), start))
        for start in (0, 100)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(30)
        assert worker.exitcode == 0

    client = ApexClient(dirpath=str(root))
    try:
        client.use_table("t")
        assert client.count_rows() == 100
        client.execute("CREATE TABLE correlated (id INT, g TEXT, v INT)")
        client.use_table("correlated")
        client.store(
            {"id": [1, 2, 3, 4], "g": ["a", "a", "b", "b"], "v": [10, 20, 30, 40]}
        )
        client.flush()
        rows = client.execute(
            "SELECT id, (SELECT COUNT(*) FROM correlated t2 "
            "WHERE t2.g = t.g) AS n FROM correlated t ORDER BY id"
        ).to_dict()
        assert [row["n"] for row in rows] == [2, 2, 2, 2]
        maxima = client.execute(
            "SELECT id FROM correlated a WHERE v = "
            "(SELECT MAX(v) FROM correlated b WHERE b.g = a.g) ORDER BY id"
        ).to_dict()
        assert maxima == [{"id": 2}, {"id": 4}]
    finally:
        client.close()


def test_open_client_observes_external_flush_immediately(tmp_path):
    root = tmp_path / "external_visibility"
    client = ApexClient(dirpath=str(root), drop_if_exists=True)
    try:
        client.execute("CREATE TABLE t (id INT, name TEXT)")
        client.use_table("t")
        client.store({"id": [1], "name": ["seed"]})
        client.flush()

        assert client.count_rows() == 1
        assert client.execute("SELECT COUNT(*) FROM t").scalar() == 1

        ctx = multiprocessing.get_context("spawn")
        writer = ctx.Process(target=_external_visibility_writer, args=(str(root), 2))
        writer.start()
        writer.join(30)
        assert writer.exitcode == 0

        # Direct and SQL paths must both invalidate process-local backends
        # without a sleep, close, or manual cache reset.
        assert client.count_rows() == 2
        assert client.execute("SELECT COUNT(*) FROM t").scalar() == 2

        writer = ctx.Process(target=_external_visibility_writer, args=(str(root), 3))
        writer.start()
        writer.join(30)
        assert writer.exitcode == 0

        # Exercise the opposite order so SQL cache invalidation is independent
        # of count_rows() refreshing the per-client backend first.
        assert client.execute("SELECT COUNT(*) FROM t").scalar() == 3
        assert client.count_rows() == 3
        assert client.retrieve(3)["name"] == "child-3"
    finally:
        client.close()


def test_truncated_storage_fails_without_rust_panic(tmp_path):
    root = tmp_path / "corrupt"
    client = ApexClient(dirpath=str(root), drop_if_exists=True)
    client.execute("CREATE TABLE t (id INT, name TEXT)")
    client.use_table("t")
    client.store({"id": list(range(20)), "name": [f"r{i}" for i in range(20)]})
    client.flush()
    client.close()
    assert client._storage is None
    assert client._shared_storage is None
    assert client._store_one is None
    assert client._store_one_memtable is None
    assert client._store_one_delta is None
    assert client._store_one_delta_durable is None

    apex_file = next(root.rglob("t.apex"))
    with apex_file.open("r+b") as handle:
        handle.truncate(max(64, apex_file.stat().st_size // 3))

    reopened = ApexClient(dirpath=str(root))
    try:
        with pytest.raises((RuntimeError, OSError), match="Corrupt|corrupt|footer|storage"):
            reopened.use_table("t")
            reopened.count_rows()
    finally:
        reopened.close()
