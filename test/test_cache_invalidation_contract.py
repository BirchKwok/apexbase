"""Architecture contracts for cross-layer cache invalidation."""

from apexbase import ApexClient


def _open_clients(tmp_path):
    writer = ApexClient(str(tmp_path))
    writer.create_table(
        "cache_contract",
        {"name": "string", "score": "int", "category": "string"},
    )
    writer.use_table("cache_contract")
    writer.store(
        [
            {"name": "alpha", "score": 10, "category": "x"},
            {"name": "beta", "score": 20, "category": "y"},
        ]
    )
    writer.flush()

    reader = ApexClient(str(tmp_path))
    reader.use_table("cache_contract")
    return writer, reader


def _warm(reader, sql):
    first = reader.execute(sql).to_dict()
    assert reader.execute(sql).to_dict() == first
    return first


def test_direct_write_invalidates_python_result_cache_across_clients(tmp_path):
    writer, reader = _open_clients(tmp_path)
    sql = "SELECT * FROM cache_contract WHERE score > 15 LIMIT 100"

    try:
        assert [row["name"] for row in _warm(reader, sql)] == ["beta"]
        assert reader._simple_sql_cache[sql][0] == "numeric_range_limit"
        assert reader._numeric_range_rows_cache

        assert writer.replace(
            1, {"name": "alpha", "score": 30, "category": "x"}
        )
        assert [row["name"] for row in reader.execute(sql).to_dict()] == [
            "alpha",
            "beta",
        ]
    finally:
        reader.close()
        writer.close()


def test_batch_write_advances_table_epoch_once(tmp_path):
    writer, reader = _open_clients(tmp_path)

    try:
        before = writer._storage._table_epoch()
        writer.store(
            [
                {"name": "gamma", "score": 30, "category": "x"},
                {"name": "delta", "score": 40, "category": "z"},
            ]
        )
        after = writer._storage._table_epoch()

        assert after == before + 1
        assert reader.execute(
            "SELECT name FROM cache_contract WHERE score >= 30 ORDER BY score"
        ).to_dict() == [{"name": "gamma"}, {"name": "delta"}]
    finally:
        reader.close()
        writer.close()


def test_transaction_commit_invalidates_all_warmed_read_caches(tmp_path):
    writer, reader = _open_clients(tmp_path)
    group_sql = (
        "SELECT category, COUNT(*) AS n FROM cache_contract "
        "GROUP BY category ORDER BY category"
    )
    point_sql = "SELECT name, score FROM cache_contract WHERE _id = 2"

    try:
        assert _warm(reader, group_sql) == [
            {"category": "x", "n": 1},
            {"category": "y", "n": 1},
        ]
        assert _warm(reader, point_sql) == [{"name": "beta", "score": 20}]
        assert _warm(reader, "SELECT COUNT(*) FROM cache_contract") == [
            {"COUNT(*)": 2}
        ]

        writer.execute("BEGIN")
        writer.execute(
            "INSERT INTO cache_contract (name, score, category) "
            "VALUES ('gamma', 30, 'x')"
        )
        writer.execute("UPDATE cache_contract SET score = 25 WHERE _id = 2")
        writer.execute("COMMIT")

        assert reader.execute(group_sql).to_dict() == [
            {"category": "x", "n": 2},
            {"category": "y", "n": 1},
        ]
        assert reader.execute(point_sql).to_dict() == [
            {"name": "beta", "score": 25}
        ]
        assert reader.execute("SELECT COUNT(*) FROM cache_contract").to_dict() == [
            {"COUNT(*)": 3}
        ]
    finally:
        reader.close()
        writer.close()


def test_schema_rewrite_invalidates_cached_backend_schema(tmp_path):
    writer, reader = _open_clients(tmp_path)

    try:
        assert reader.list_fields() == ["name", "score", "category"]
        _warm(reader, "SELECT name, score FROM cache_contract")

        writer.execute("ALTER TABLE cache_contract ADD COLUMN note STRING")

        assert reader.list_fields() == ["name", "score", "category", "note"]
        assert reader.execute("SELECT name, note FROM cache_contract").to_dict() == [
            {"name": "alpha", "note": ""},
            {"name": "beta", "note": ""},
        ]
    finally:
        reader.close()
        writer.close()
