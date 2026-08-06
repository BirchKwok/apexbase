"""Regression tests for the five issues in the 2026-08-05 issue.md report."""

from __future__ import annotations

import time

import pytest

from apexbase import ApexClient


def _client(tmp_path, name: str) -> ApexClient:
    return ApexClient(dirpath=str(tmp_path / name), drop_if_exists=True)


def test_create_table_does_not_overwrite_existing_disk_table(tmp_path):
    """A fresh process must never rebuild an existing on-disk table."""
    db = str(tmp_path / "db")
    writer = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        writer.create_table("videos", {"name": "string"})
        writer.store({"name": "v1"})
    finally:
        writer.close()

    reader = ApexClient(dirpath=db)
    try:
        with pytest.raises(ValueError, match="Table already exists"):
            reader.create_table("videos", {"name": "string"})
        assert reader.count_rows("videos") == 1
        assert reader.execute("SELECT name FROM videos").to_dict() == [{"name": "v1"}]
    finally:
        reader.close()


def test_create_table_after_sql_drop_recreates_cleanly(tmp_path):
    """A stale client cache entry must not block recreating a dropped table."""
    client = _client(tmp_path, "recreate")
    try:
        client.create_table("t", {"k": "int64"})
        client.store({"k": 1})
        client.execute("DROP TABLE t")
        client.create_table("t", {"k": "int64"})
        assert client.count_rows("t") == 0
        client.store({"k": 2})
        assert client.count_rows("t") == 1
    finally:
        client.close()


def test_order_by_id_limit_spans_multiple_row_groups(tmp_path):
    """ORDER BY _id + LIMIT must see every flushed row group, not just the first."""
    client = _client(tmp_path, "order")
    try:
        client.create_table("t", {"k": "int64"})
        for batch in (range(0, 4), range(10, 14), range(20, 24)):
            client.store([{"k": k} for k in batch])
            client.flush()
            client.flush_cache()
        assert client.count_rows("t") == 12

        asc = client.execute(
            "SELECT _id, k FROM t ORDER BY _id LIMIT 8", show_internal_id=True
        ).to_dict()
        assert [row["_id"] for row in asc] == [1, 2, 3, 4, 5, 6, 7, 8]
        assert [row["k"] for row in asc] == [0, 1, 2, 3, 10, 11, 12, 13]

        desc = client.execute(
            "SELECT _id, k FROM t ORDER BY _id DESC LIMIT 8", show_internal_id=True
        ).to_dict()
        assert [row["_id"] for row in desc] == [12, 11, 10, 9, 8, 7, 6, 5]

        # A LIMIT larger than the row count must still expose every row group;
        # LIMIT 8 returning 8 rows is standard SQL, not row-group truncation.
        all_rows = client.execute(
            "SELECT _id, k FROM t ORDER BY _id LIMIT 100", show_internal_id=True
        ).to_dict()
        assert [row["_id"] for row in all_rows] == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        all_desc = client.execute(
            "SELECT _id, k FROM t ORDER BY _id DESC LIMIT 100", show_internal_id=True
        ).to_dict()
        assert [row["_id"] for row in all_desc] == [12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]

        star = client.execute(
            "SELECT * FROM t ORDER BY _id DESC LIMIT 8", show_internal_id=True
        ).to_dict()
        assert [row["_id"] for row in star] == [12, 11, 10, 9, 8, 7, 6, 5]
    finally:
        client.close()


def test_numeric_range_order_by_limit_uses_global_topk(tmp_path):
    """WHERE + ORDER BY on the same column + LIMIT must use the global top-k set."""
    client = _client(tmp_path, "order_range")
    try:
        client.create_table("t", {"k": "int64"})
        for batch in (range(0, 4), range(10, 14), range(20, 24)):
            client.store([{"k": k} for k in batch])
            client.flush()

        def kvals(sql):
            return [row["k"] for row in client.execute(sql, show_internal_id=True).to_dict()]

        # Without the fix these returned only the first matching row group
        # ([3, 2, 1, 0] and [13, 12, 11, 10] respectively).
        assert kvals(
            "SELECT k FROM t WHERE k >= 0 ORDER BY k DESC LIMIT 4"
        ) == [23, 22, 21, 20]
        assert kvals(
            "SELECT k FROM t WHERE k >= 10 ORDER BY k DESC LIMIT 4"
        ) == [23, 22, 21, 20]
        assert kvals(
            "SELECT k FROM t WHERE k >= 10 ORDER BY k ASC LIMIT 4"
        ) == [10, 11, 12, 13]
        assert kvals(
            "SELECT k FROM t WHERE k >= 0 ORDER BY k DESC LIMIT 100"
        ) == [23, 22, 21, 20, 13, 12, 11, 10, 3, 2, 1, 0]
        # A different ORDER BY column must stay correct as well.
        assert kvals(
            "SELECT k FROM t WHERE k >= 10 ORDER BY _id DESC LIMIT 4"
        ) == [23, 22, 21, 20]

        # Indexed variant must produce the same global top-k result.
        client.execute("CREATE INDEX idx_k ON t (k)")
        client.flush_cache()
        assert kvals(
            "SELECT k FROM t WHERE k >= 10 ORDER BY k DESC LIMIT 4"
        ) == [23, 22, 21, 20]
    finally:
        client.close()


def test_string_filter_order_by_limit_uses_global_topk(tmp_path):
    """String equality + ORDER BY + LIMIT must not truncate to the first row group."""
    client = _client(tmp_path, "order_str")
    try:
        client.create_table("t", {"name": "string", "score": "float64"})
        for i in range(3):
            client.store(
                {
                    "name": ["n"] * 4,
                    "score": [float(10 * i + j) for j in range(4)],
                }
            )
            client.flush()
        rows = client.execute(
            "SELECT score FROM t WHERE name = 'n' ORDER BY score DESC LIMIT 4",
            show_internal_id=True,
        ).to_dict()
        assert [row["score"] for row in rows] == [23.0, 22.0, 21.0, 20.0]
    finally:
        client.close()


def test_show_internal_id_policy_consistent_before_and_after_flush(tmp_path):
    """Explicit _id projection must be visible regardless of flush state."""
    client = _client(tmp_path, "visibility")
    try:
        client.create_table("t", {"ts": "int64"})
        client.store({"ts": 1})
        assert client.execute("SELECT _id, ts FROM t LIMIT 1").to_dict() == [
            {"_id": 1, "ts": 1}
        ]

        client.flush()
        assert client.execute("SELECT _id, ts FROM t LIMIT 1").to_dict() == [
            {"_id": 1, "ts": 1}
        ]
        assert client.execute("SELECT * FROM t LIMIT 1").to_dict() == [{"ts": 1}]
        assert client.execute("SELECT * FROM t LIMIT 1", show_internal_id=True).to_dict() == [
            {"_id": 1, "ts": 1}
        ]
        assert client.execute(
            "SELECT _id, ts FROM t LIMIT 1", show_internal_id=False
        ).to_dict() == [{"ts": 1}]
    finally:
        client.close()


def test_execute_batch_coalesces_numeric_updates_fast_and_correct(tmp_path):
    """10k single-row numeric updates must complete via the native batch path."""
    client = _client(tmp_path, "batch")
    try:
        client.create_table("faces", {"cluster_id": "int64"})
        client.store([{"cluster_id": i % 7} for i in range(10_000)])
        client.flush()

        queries = [
            f"UPDATE faces SET cluster_id = {i % 9} WHERE _id = {i + 1}"
            for i in range(10_000)
        ]
        started = time.perf_counter()
        results = client.execute_batch(queries)
        elapsed = time.perf_counter() - started

        assert len(results) == len(queries)
        for result in results:
            assert result.to_dict() == [{"rows_affected": 1}]
        # The sequential Python baseline measured ~28-40s for this workload.
        assert elapsed < 10.0, f"batch UPDATE took {elapsed:.3f}s"

        rows = client.execute(
            "SELECT _id, cluster_id FROM faces ORDER BY _id LIMIT 1000",
            show_internal_id=True,
        ).to_dict()
        assert rows
        for row in rows:
            assert row["cluster_id"] == (row["_id"] - 1) % 9
    finally:
        client.close()


def test_execute_batch_preserves_order_with_mixed_statements(tmp_path):
    """Batch coalescing must keep statement order and mix with other DML."""
    client = _client(tmp_path, "mixed")
    try:
        client.create_table("t", {"k": "int64", "name": "string"})
        client.store([{"k": i, "name": f"n{i}"} for i in range(10)])
        client.flush()

        results = client.execute_batch(
            [
                "UPDATE t SET k = 100 WHERE _id = 1",
                "UPDATE t SET k = 200 WHERE _id = 2",
                "INSERT INTO t (k, name) VALUES (300, 'n10')",
                "UPDATE t SET k = 400 WHERE _id = 3",
            ]
        )
        assert len(results) == 4

        rows = client.execute(
            "SELECT _id, k FROM t ORDER BY _id", show_internal_id=True
        ).to_dict()
        assert [row["k"] for row in rows] == [100, 200, 400, 3, 4, 5, 6, 7, 8, 9, 300]
    finally:
        client.close()


def test_execute_batch_numeric_update_reports_missing_rows(tmp_path):
    """Missing row ids return rows_affected = 0 through the batch path."""
    client = _client(tmp_path, "missing")
    try:
        client.create_table("t", {"k": "int64"})
        client.store({"k": 1})
        client.flush()
        results = client.execute_batch(
            [
                "UPDATE t SET k = 5 WHERE _id = 1",
                "UPDATE t SET k = 6 WHERE _id = 99",
            ]
        )
        assert results[0].to_dict() == [{"rows_affected": 1}]
        assert results[1].to_dict() == [{"rows_affected": 0}]
        assert client.execute("SELECT k FROM t WHERE _id = 1").to_dict() == [{"k": 5}]
    finally:
        client.close()


def test_execute_batch_numeric_update_does_not_resurrect_deleted_rows(tmp_path):
    """Batch updates must not bring soft-deleted rows back into the table."""
    client = _client(tmp_path, "deleted")
    try:
        client.create_table("t", {"k": "int64"})
        client.store([{"k": i} for i in range(5)])
        client.flush()
        client.delete(id=2)
        results = client.execute_batch(
            [
                "UPDATE t SET k = 99 WHERE _id = 2",
                "UPDATE t SET k = 50 WHERE _id = 1",
            ]
        )
        assert results[0].to_dict() == [{"rows_affected": 0}]
        assert results[1].to_dict() == [{"rows_affected": 1}]
        assert client.count_rows("t") == 4
        assert client.execute("SELECT k FROM t WHERE _id = 2").to_dict() == []
        assert client.execute("SELECT k FROM t WHERE _id = 1").to_dict() == [{"k": 50}]
    finally:
        client.close()


def test_parameter_binding_positional_named_and_in_list(tmp_path):
    """Generic ?/:name/@name binding plus IN-list expansion."""
    client = _client(tmp_path, "params")
    try:
        client.create_table("t", {"name": "string", "score": "float64"})
        client.store(
            [
                {"name": "alice", "score": 1.5},
                {"name": "bob", "score": 2.5},
                {"name": "carol", "score": 3.5},
                {"name": "o'brien", "score": 4.0},
            ]
        )
        client.flush()

        assert client.execute(
            "SELECT name, score FROM t WHERE name = ?", params=["alice"]
        ).to_dict() == [{"name": "alice", "score": 1.5}]

        assert client.execute(
            "SELECT name, score FROM t WHERE score > ? ORDER BY score", params=[2.0]
        ).to_dict() == [
            {"name": "bob", "score": 2.5},
            {"name": "carol", "score": 3.5},
            {"name": "o'brien", "score": 4.0},
        ]

        assert client.execute(
            "SELECT name, score FROM t WHERE name IN (?) ORDER BY score",
            params=[["alice", "carol"]],
        ).to_dict() == [
            {"name": "alice", "score": 1.5},
            {"name": "carol", "score": 3.5},
        ]

        assert client.execute(
            "SELECT name, score FROM t WHERE name = :who AND score >= :min",
            params={"who": "bob", "min": 2.0},
        ).to_dict() == [{"name": "bob", "score": 2.5}]

        assert client.execute(
            "SELECT name FROM t WHERE name = @who", params={"who": "o'brien"}
        ).to_dict() == [{"name": "o'brien"}]

        assert client.execute(
            "SELECT name FROM t WHERE name = '?' AND score > ?",
            params=[0.0],
        ).to_dict() == []
    finally:
        client.close()


def test_parameter_binding_validates_arity_types_and_missing_names(tmp_path):
    """Binding errors are loud and literal ? inside strings is not consumed."""
    client = _client(tmp_path, "params_bad")
    try:
        client.create_table("t", {"name": "string", "score": "float64"})
        client.store({"name": "alice", "score": 1.5})
        client.flush()

        with pytest.raises(ValueError, match="not enough parameters"):
            client.execute("SELECT name FROM t WHERE name = ?", params=[])
        with pytest.raises(ValueError, match="too many parameters"):
            client.execute("SELECT name FROM t WHERE name = ?", params=["a", "b"])
        with pytest.raises(ValueError, match="missing parameter"):
            client.execute(
                "SELECT name FROM t WHERE name = :who", params={"other": "alice"}
            )
        with pytest.raises(TypeError, match="unsupported parameter type"):
            client.execute("SELECT name FROM t WHERE name = ?", params=[object()])

        # A '?' inside a string literal is data, not a placeholder.
        assert client.execute(
            "SELECT name FROM t WHERE name = '?'", params=[]
        ).to_dict() == []
    finally:
        client.close()
