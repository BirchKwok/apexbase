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
            {"name": "alpha", "note": None},
            {"name": "beta", "note": None},
        ]
    finally:
        reader.close()
        writer.close()


def test_compact_analytical_result_cache_returns_independent_views(tmp_path):
    writer, reader = _open_clients(tmp_path)
    sql = (
        "SELECT category, COUNT(*) AS n, AVG(score) AS avg_score "
        "FROM cache_contract GROUP BY category ORDER BY category"
    )

    try:
        expected = reader.execute(sql).to_dict()
        assert reader._query_result_cache

        mutated = reader.execute(sql).to_dict()
        mutated[0]["category"] = "caller mutation"
        assert reader.execute(sql).to_dict() == expected
    finally:
        reader.close()
        writer.close()


def test_compact_analytical_result_cache_can_be_disabled(tmp_path):
    client = ApexClient(str(tmp_path), enable_cache=False)
    try:
        client.create_table("events", {"category": "string"})
        client.use_table("events")
        client.store([{"category": "x"}, {"category": "y"}])
        client.flush()

        sql = "SELECT category, COUNT(*) AS n FROM events GROUP BY category"
        assert len(client.execute(sql).to_dict()) == 2
        assert len(client.execute(sql).to_dict()) == 2
        assert not client._query_result_cache
    finally:
        client.close()


def test_compact_analytical_result_cache_invalidates_across_clients(tmp_path):
    writer, reader = _open_clients(tmp_path)
    sql = "SELECT category, COUNT(*) AS n FROM cache_contract GROUP BY category ORDER BY category"

    try:
        assert _warm(reader, sql) == [
            {"category": "x", "n": 1},
            {"category": "y", "n": 1},
        ]
        assert reader._query_result_cache

        writer.store({"name": "gamma", "score": 30, "category": "x"})
        writer.flush()
        assert reader.execute(sql).to_dict() == [
            {"category": "x", "n": 2},
            {"category": "y", "n": 1},
        ]
    finally:
        reader.close()
        writer.close()


def test_external_file_result_cache_tracks_file_generation(tmp_path):
    csv_path = tmp_path / "events.csv"
    csv_path.write_text("category,score\nx,10\ny,20\n")
    client = ApexClient(str(tmp_path / "db"))
    sql = (
        f"SELECT category, COUNT(*) AS n FROM '{csv_path}' "
        "GROUP BY category ORDER BY category"
    )

    try:
        assert _warm(client, sql) == [
            {"category": "x", "n": 1},
            {"category": "y", "n": 1},
        ]
        assert client._query_result_cache

        csv_path.write_text("category,score\nx,10\nx,30\ny,20\n")
        assert client.execute(sql).to_dict() == [
            {"category": "x", "n": 2},
            {"category": "y", "n": 1},
        ]
    finally:
        client.close()


def test_large_analytical_result_preserves_lazy_result_cache(tmp_path):
    client = ApexClient(str(tmp_path / "db"))
    try:
        client.create_table("events", {"category": "string"})
        client.use_table("events")
        client.store([{"category": f"c{i:03d}"} for i in range(300)])
        client.flush()

        sql = (
            "SELECT category, COUNT(*) AS n FROM events "
            "GROUP BY category ORDER BY category"
        )
        expected = client.execute(sql).to_dict()
        assert len(expected) == 300
        assert client._query_result_cache
        assert next(iter(client._query_result_cache.values()))[1][0] == "pydict"
        assert client.execute(sql).to_dict() == expected
    finally:
        client.close()


def test_in_memory_analytical_cache_and_string_groups_remain_write_visible():
    client = ApexClient(":memory:")
    try:
        client.create_table("events", {"src_ip": "string", "dst_port": "int64"})
        client.use_table("events")
        client.store([
            {"src_ip": "172.16.1.1", "dst_port": 53},
            {"src_ip": "172.16.1.2", "dst_port": 80},
            {"src_ip": "10.0.0.1", "dst_port": 53},
        ])
        client.flush()

        prefix_sql = (
            "SELECT SUBSTR(src_ip, 1, 7) AS prefix, COUNT(*) AS n FROM events "
            "GROUP BY SUBSTR(src_ip, 1, 7) ORDER BY prefix"
        )
        assert client.execute(prefix_sql).to_dict() == [
            {"prefix": "10.0.0.", "n": 1},
            {"prefix": "172.16.", "n": 2},
        ]
        assert client._query_result_cache

        distinct_sql = (
            "SELECT src_ip, COUNT(*) AS n, COUNT(DISTINCT dst_port) AS ports "
            "FROM events GROUP BY src_ip ORDER BY src_ip"
        )
        assert len(client.execute(distinct_sql).to_dict()) == 3

        client.store({"src_ip": "10.0.0.2", "dst_port": 443})
        client.flush()
        assert client.execute(prefix_sql).to_dict() == [
            {"prefix": "10.0.0.", "n": 2},
            {"prefix": "172.16.", "n": 2},
        ]
    finally:
        client.close()
