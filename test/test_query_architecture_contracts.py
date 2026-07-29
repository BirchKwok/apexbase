"""Cross-layer contracts for SQL routing and delta materialization."""

import json
from pathlib import Path

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
