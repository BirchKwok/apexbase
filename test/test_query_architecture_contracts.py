"""Cross-layer contracts for SQL routing and delta materialization."""

import json
from pathlib import Path

import pytest

from apexbase import ApexClient
from apexbase.client import _classify_sql_route, _sql_route_family


QUERY_SIGNATURE_CORPUS = Path(__file__).with_name("fixtures") / "query_signature_routes.jsonc"


def _load_query_signature_corpus():
    corpus = json.loads(QUERY_SIGNATURE_CORPUS.read_text(encoding="utf-8"))
    assert corpus["format_version"] == 1
    return corpus["cases"]


def _rows_from_rust_result(result):
    """Convert the Rust binding's column-oriented result into public row form."""
    columns = result.get("columns_dict", {})
    row_count = len(next(iter(columns.values()), []))
    return [
        {name: values[row_index] for name, values in columns.items()}
        for row_index in range(row_count)
    ]


def _assert_python_rust_query_parity(client, query):
    public_rows = client.execute(query).to_dict()
    rust_rows = _rows_from_rust_result(client._storage.execute(query))
    assert public_rows == rust_rows, query
    return public_rows


def test_python_and_rust_classifiers_agree_on_route_families(tmp_path):
    """Detailed Python fast paths may differ, but locking route families must agree."""
    client = ApexClient(str(tmp_path))
    for case in _load_query_signature_corpus():
        sql = case["sql"]
        python_route, _, _ = _classify_sql_route(sql)
        assert python_route == case["python_route"], case["id"]
        assert _sql_route_family(python_route) == case["route_family"], case["id"]
        assert client._storage._query_route_family(sql) == case["route_family"], case["id"]

    client.close()


def test_query_signature_corpus_is_maintainable():
    cases = _load_query_signature_corpus()
    case_ids = [case["id"] for case in cases]

    assert len(cases) >= 19
    assert len(case_ids) == len(set(case_ids))
    assert all(set(case) == {"id", "sql", "python_route", "route_family"} for case in cases)
    assert {case["route_family"] for case in cases} == {
        "read", "write", "transaction", "multi", "session"
    }


def test_python_and_rust_sql_routing_contract(tmp_path):
    """Python fast-path routing must preserve the Rust executor's SQL semantics."""
    client = ApexClient(str(tmp_path))
    client.create_table("routing", {"name": "string", "score": "int"})
    client.use_table("routing")
    client.store(
        [
            {"name": "alpha", "score": 10},
            {"name": "semi;colon", "score": 20},
            {"name": "alphabet", "score": 30},
        ]
    )
    client.flush()

    queries = [
        "SELECT COUNT(*) FROM routing",
        "SELECT name FROM routing WHERE _id = 2",
        "SELECT name FROM routing WHERE _id IN (1, 3)",
        "SELECT name, score FROM routing",
        "SELECT name, score FROM routing LIMIT 2",
        "SELECT name FROM routing WHERE name = 'semi;colon'",
        "SELECT name FROM routing WHERE score > 10 LIMIT 2",
        "SELECT name FROM routing WHERE name LIKE 'alpha%'",
        "SELECT name, SUM(score) AS total FROM routing GROUP BY name ORDER BY name",
        "-- routing comment\nSELECT name FROM routing WHERE _id = 1",
        "/* routing comment */ SELECT 'semi;colon' AS marker FROM routing LIMIT 1",
    ]

    for query in queries:
        _assert_python_rust_query_parity(client, query)

    client.close()


def test_delta_compaction_and_reopen_preserve_query_contract(tmp_path):
    """All read routes must expose the same rows before and after sidecar materialization."""
    client = ApexClient(str(tmp_path))
    client.create_table("events", {"name": "string", "score": "int"})
    client.use_table("events")
    client.store(
        [
            {"name": "base_a", "score": 1},
            {"name": "base_b", "score": 2},
            {"name": "base_c", "score": 3},
        ]
    )
    client.flush()

    client.execute("BEGIN")
    client.execute("INSERT INTO events (name, score) VALUES ('semi;colon', 30)")
    client.execute("UPDATE events SET score = 20 WHERE _id = 2")
    client.execute("COMMIT")

    table_path = Path(tmp_path) / "events.apex"
    delta_path = Path(f"{table_path}.delta")
    assert delta_path.exists()

    queries = [
        "SELECT name, score FROM events ORDER BY score, name",
        "SELECT name, score FROM events WHERE _id = 2",
        "SELECT name FROM events WHERE _id IN (1, 4)",
        "SELECT name FROM events WHERE name = 'semi;colon'",
        "SELECT name, score FROM events WHERE score > 2 LIMIT 10",
        "SELECT COUNT(*) FROM events",
    ]
    before = {
        query: _assert_python_rust_query_parity(client, query)
        for query in queries
    }
    retrieve_many_before = client.retrieve_many([1, 4]).to_dict()
    assert retrieve_many_before == [
        {"name": "base_a", "score": 1},
        {"name": "semi;colon", "score": 30},
    ]

    # ALTER is the public schema-rewrite boundary that must materialize sidecars.
    client.execute("ALTER TABLE events ADD COLUMN note STRING")
    assert not delta_path.exists()
    after_compaction = {
        query: _assert_python_rust_query_parity(client, query)
        for query in queries
    }
    assert after_compaction == before
    retrieve_many_after = client.retrieve_many([1, 4]).to_dict()
    assert retrieve_many_after == [
        {**row, "note": None}
        for row in retrieve_many_before
    ]
    client.close()

    reopened = ApexClient(str(tmp_path))
    reopened.use_table("events")
    after_reopen = {
        query: _assert_python_rust_query_parity(reopened, query)
        for query in queries
    }
    assert after_reopen == before
    assert reopened.retrieve_many([1, 4]).to_dict() == retrieve_many_after
    reopened.close()


def test_uncached_parameterized_scan_group_having_topk_sees_delta(tmp_path):
    """The physical scan protocol must keep HAVING/TopK order and delta visibility."""
    client = ApexClient(str(tmp_path), enable_cache=False)
    client.create_table(
        "analytics",
        {"city": "string", "age": "int", "score": "float"},
    )
    client.use_table("analytics")
    client.store(
        [
            {"city": "Beijing", "age": 20, "score": 99.0},
            {"city": "Beijing", "age": 25, "score": 50.0},
            {"city": "Beijing", "age": 30, "score": 70.0},
            {"city": "Shanghai", "age": 32, "score": 60.0},
            {"city": "Shenzhen", "age": 34, "score": 80.0},
        ]
    )
    client.flush()

    query = (
        "SELECT city, COUNT(*) AS n, AVG(score) AS av FROM analytics "
        "WHERE age > ? AND age <= ? AND score >= ? "
        "GROUP BY city HAVING COUNT(*) > ? "
        "ORDER BY n DESC, city LIMIT 3"
    )
    assert client.execute(query, params=[20, 35, 40, 1]).to_dict() == [
        {"city": "Beijing", "n": 2, "av": 60.0}
    ]

    client.execute("BEGIN")
    client.execute(
        "INSERT INTO analytics (city, age, score) VALUES ('Shanghai', 31, 90.0)"
    )
    client.execute(
        "INSERT INTO analytics (city, age, score) VALUES ('Shanghai', 33, 100.0)"
    )
    client.execute("UPDATE analytics SET score = 10 WHERE _id = 3")
    client.execute("COMMIT")

    table_path = Path(tmp_path) / "analytics.apex"
    assert Path(f"{table_path}.delta").exists()
    rows = client.execute(query, params=[20, 35, 40, 1]).to_dict()
    assert rows[0]["city"] == "Shanghai"
    assert rows[0]["n"] == 3
    assert rows[0]["av"] == pytest.approx(250.0 / 3.0)
    assert len(rows) == 1

    # Vary every bound so repeated execution cannot accidentally prove only an
    # exact-SQL cache hit. Strict/inclusive bounds and HAVING remain observable.
    assert client.execute(query, params=[30, 33, 80, 1]).to_dict() == [
        {"city": "Shanghai", "n": 2, "av": 95.0}
    ]
    assert client.execute(query, params=[30, 33, 80, 2]).to_dict() == []
    assert not client._query_result_cache
    client.close()


def test_scan_pipeline_falls_back_for_unrepresentable_integer_bounds(tmp_path):
    """Large Int64 bounds must retain exact semantics outside the f64 scan lane."""
    client = ApexClient(str(tmp_path), enable_cache=False)
    client.create_table("wide_ints", {"city": "string", "age": "int"})
    client.use_table("wide_ints")
    client.store(
        [
            {"city": "exact", "age": 9_007_199_254_740_992},
            {"city": "next", "age": 9_007_199_254_740_993},
        ]
    )
    client.flush()

    query = (
        "SELECT city, COUNT(*) AS n FROM wide_ints "
        "WHERE age > ? AND age <= ? GROUP BY city ORDER BY city"
    )
    assert client.execute(
        query,
        params=[9_007_199_254_740_992, 9_007_199_254_740_993],
    ).to_dict() == [{"city": "next", "n": 1}]
    client.close()
