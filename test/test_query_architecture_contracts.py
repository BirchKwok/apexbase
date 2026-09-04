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
            {"city": "Nullville", "age": 31, "score": None},
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

    boolean_query = (
        "SELECT city, COUNT(*) AS n FROM analytics "
        "WHERE (city IN ('Beijing', 'Shanghai') OR score IS NULL) "
        "AND age IN (20, 25, 30, 31, 32) "
        "GROUP BY city HAVING COUNT(*) >= 1 "
        "ORDER BY n DESC, city LIMIT 5"
    )
    assert client.execute(boolean_query).to_dict() == [
        {"city": "Beijing", "n": 3},
        {"city": "Nullville", "n": 1},
        {"city": "Shanghai", "n": 1},
    ]

    # GROUP + window remains a two-stage general-executor shape. The scan
    # pipeline must decline it rather than dropping the window projection.
    window_rows = client.execute(
        "SELECT city, COUNT(*) AS n, "
        "ROW_NUMBER() OVER (ORDER BY city) AS rn FROM analytics "
        "WHERE (city IN ('Beijing', 'Shanghai') OR score IS NULL) "
        "AND age IN (20, 25, 30, 31, 32) GROUP BY city"
    ).to_dict()
    assert {
        row["city"]: (row["n"], row["rn"])
        for row in window_rows
    } == {
        "Beijing": (3, 1),
        "Nullville": (1, 2),
        "Shanghai": (1, 3),
    }

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

    # OR / IN / IS NULL share the same typed physical predicate tree. The age
    # IN leaf can still provide a safe mmap candidate even though the OR branch
    # contains an IS NULL leaf that cannot be used for pruning.
    assert client.execute(boolean_query).to_dict() == [
        {"city": "Beijing", "n": 3},
        {"city": "Shanghai", "n": 2},
        {"city": "Nullville", "n": 1},
    ]
    assert not client._query_result_cache
    client.close()


def test_scan_pipeline_preserves_exact_wide_integer_bounds(tmp_path):
    """Large Int64 bounds remain exact even when mmap pruning cannot use f64."""
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


def test_uncached_or_in_scan_moderate_selectivity_matches_reference(tmp_path):
    """OR/IN filters with a moderate match fraction stay exact when uncached.

    The candidate union of two ordered lane outputs is merged and re-checked
    row by row; the materialized selection must match a Python reference for
    every group, not just the aggregate count.
    """
    client = ApexClient(str(tmp_path), enable_cache=False)
    client.create_table("moderate", {"city": "string", "age": "int", "score": "float"})
    client.use_table("moderate")
    rows = [
        {
            "city": ["North", "South", "East", "West", "Center"][index % 5],
            "age": index % 12,
            "score": (index % 7) * 0.25,
        }
        for index in range(1200)
    ]
    client.store(rows)
    client.flush()

    query = (
        "SELECT city, COUNT(*) AS n, SUM(score) AS total FROM moderate "
        "WHERE (city IN ('North', 'South') OR age IN (0, 1, 2)) AND score >= 1.0 "
        "GROUP BY city ORDER BY city"
    )
    expected_rows = [
        row for row in rows
        if (row["city"] in ("North", "South") or row["age"] in (0, 1, 2))
        and row["score"] >= 1.0
    ]
    assert len(expected_rows) > len(rows) // 5

    by_city = {}
    for row in expected_rows:
        current = by_city.setdefault(row["city"], [0, 0.0])
        current[0] += 1
        current[1] += row["score"]
    expected = [
        {"city": city, "n": count, "total": round(total, 6)}
        for city, (count, total) in sorted(by_city.items())
    ]
    actual = client.execute(query).to_dict()
    assert [
        {"city": r["city"], "n": r["n"], "total": round(r["total"], 6)} for r in actual
    ] == expected
    assert _assert_python_rust_query_parity(client, query)
    client.close()


def test_mixed_dict_gather_filtered_select_matches_reference(tmp_path):
    """Mixed string+numeric gathers stay exact when the string column is
    dictionary-encoded (dictionary keys for the string, primitives for the
    numeric lanes inside one batch)."""
    client = ApexClient(str(tmp_path), enable_cache=False)
    client.create_table("mixed", {"city": "string", "age": "int", "score": "float"})
    client.use_table("mixed")
    rows = [
        {
            "city": ["North", "South", "East", "West", "Center"][index % 5],
            "age": index % 13,
            "score": round((index % 9) * 0.5, 6),
        }
        for index in range(1200)
    ]
    client.store(rows)
    client.flush()

    # Warm the global string dictionary cache through the dictionary group-by
    # path so the gather below can take the mixed dictionary+primitive batch.
    warmup = "SELECT city, COUNT(*) AS n FROM mixed GROUP BY city ORDER BY city"
    assert len(client.execute(warmup).to_dict()) == 5

    query = (
        "SELECT city, age, score FROM mixed "
        "WHERE (city IN ('North', 'South') OR age IN (0, 1, 2)) AND score >= 2.0"
    )
    expected = sorted(
        (
            row["city"],
            row["age"],
            round(row["score"], 6),
        )
        for row in rows
        if (row["city"] in ("North", "South") or row["age"] in (0, 1, 2))
        and row["score"] >= 2.0
    )
    assert len(expected) > 100
    actual = sorted(
        (r["city"], r["age"], round(r["score"], 6))
        for r in client.execute(query).to_dict()
    )
    assert actual == expected
    assert _assert_python_rust_query_parity(client, query)
    client.close()


def test_mixed_dict_gather_boolean_group_having_topk_matches_reference(tmp_path):
    """Boolean filter + GROUP BY + HAVING + TopK on a dictionary group column
    matches the Python reference row for row (benchmark shape at small scale)."""
    client = ApexClient(str(tmp_path), enable_cache=False)
    client.create_table("boolgrp", {"city": "string", "age": "int", "score": "float"})
    client.use_table("boolgrp")
    rows = [
        {
            "city": ["North", "South", "East", "West", "Center"][index % 5],
            "age": index % 13,
            "score": round((index % 9) * 0.5, 6),
        }
        for index in range(1200)
    ]
    client.store(rows)
    client.flush()
    warmup = "SELECT city, COUNT(*) AS n FROM boolgrp GROUP BY city ORDER BY city"
    assert len(client.execute(warmup).to_dict()) == 5

    query = (
        "SELECT city, COUNT(*) AS n, AVG(score) AS av FROM boolgrp "
        "WHERE (city IN ('North', 'South', 'East') OR age IN (3, 7, 11)) "
        "AND score >= 1.5 "
        "GROUP BY city HAVING COUNT(*) > 5 "
        "ORDER BY n DESC, city LIMIT 5"
    )
    by_city = {}
    for row in rows:
        matched = (row["city"] in ("North", "South", "East") or row["age"] in (3, 7, 11))
        if matched and row["score"] >= 1.5:
            count, total = by_city.setdefault(row["city"], [0, 0.0])
            by_city[row["city"]] = [count + 1, total + row["score"]]
    expected = sorted(
        (
            city,
            count,
            round(total / count, 6),
        )
        for city, (count, total) in by_city.items()
        if count > 5
    )
    expected = sorted(expected, key=lambda item: (-item[1], item[0]))[:5]

    actual = sorted(
        (r["city"], r["n"], round(r["av"], 6))
        for r in client.execute(query).to_dict()
    )
    actual = sorted(actual, key=lambda item: (-item[1], item[0]))[:5]
    assert actual == expected
    assert _assert_python_rust_query_parity(client, query)
    client.close()


def test_replaced_dict_value_visible_through_filtered_select(tmp_path):
    """A replace that introduces a string value absent from the persisted
    dictionary stays visible through filtered selects (pending-delta state)."""
    client = ApexClient(str(tmp_path), enable_cache=False)
    client.create_table("updc", {"city": "string", "age": "int"})
    client.use_table("updc")
    rows = [
        {"city": "North" if index % 2 == 0 else "South", "age": index % 50}
        for index in range(200)
    ]
    client.store(rows)
    client.flush()
    warmup = "SELECT city, COUNT(*) AS n FROM updc GROUP BY city ORDER BY city"
    assert len(client.execute(warmup).to_dict()) == 2

    # The North row with the minimum age is unique (age 0); fetch its id
    # without assuming an id base, then replace it with a value that does not
    # exist in the persisted dictionary.
    target = client.execute(
        "SELECT _id, age FROM updc WHERE city = 'North' ORDER BY age LIMIT 1"
    ).to_dict()[0]
    target_id = int(target["_id"])
    target_age = int(target["age"])
    # replace is a full-row update: pass every column so the others survive.
    assert client.replace(target_id, {"city": "Guangzhou", "age": target_age})

    query = "SELECT city, age FROM updc WHERE age IN (0, 1, 2, 7, 8, 12)"
    expected = sorted(
        (row["city"], row["age"])
        for row in rows
        if row["age"] in (0, 1, 2, 7, 8, 12)
    )
    # Only the single replaced row changes; keep the other same-valued rows.
    updated_expected = []
    for city, age in expected:
        if (city, age) == ("North", target_age) and not any(
            item[0] == "Guangzhou" for item in updated_expected
        ):
            updated_expected.append(("Guangzhou", age))
        else:
            updated_expected.append((city, age))
    expected = updated_expected
    assert any(city == "Guangzhou" for city, _ in expected)
    actual = sorted((r["city"], r["age"]) for r in client.execute(query).to_dict())
    assert actual == expected
    client.close()


def _fused_test_rows():
    """Deterministic 1200-row dataset: 5 cities, age mod 13, score mod 9 * 0.5."""
    return [
        {
            "city": ["North", "South", "East", "West", "Center"][index % 5],
            "age": index % 13,
            "score": round((index % 9) * 0.5, 6),
        }
        for index in range(1200)
    ]


def _open_fused_fixture(client, table, rows):
    client.create_table(table, {"city": "string", "age": "int", "score": "float"})
    client.use_table(table)
    client.store(rows)
    client.flush()
    # Warm the global string dictionary cache through the dictionary group-by
    # path so the fused gate can find a usable cache for the group column.
    warmup = f"SELECT city, COUNT(*) AS n FROM {table} GROUP BY city ORDER BY city"
    assert len(client.execute(warmup).to_dict()) == 5


def test_fused_group_agg_boolean_between_max_having_topk_matches_reference(tmp_path):
    """Boolean tree (dict IN OR int IN) AND BETWEEN, MAX aggregate, HAVING and
    TopK all in one fused scan: per-group results must match the reference."""
    client = ApexClient(str(tmp_path), enable_cache=False)
    rows = _fused_test_rows()
    _open_fused_fixture(client, "fusedbool", rows)

    query = (
        "SELECT city, COUNT(*) AS n, MAX(score) AS hi FROM fusedbool "
        "WHERE (city IN ('North', 'South') OR age IN (3, 7, 11)) "
        "AND score BETWEEN 0.5 AND 3.5 "
        "GROUP BY city HAVING COUNT(*) > 100 "
        "ORDER BY n DESC, city LIMIT 5"
    )
    by_city = {}
    for row in rows:
        matched = (row["city"] in ("North", "South") or row["age"] in (3, 7, 11))
        if matched and 0.5 <= row["score"] <= 3.5:
            count, hi = by_city.setdefault(row["city"], [0, 0.0])
            by_city[row["city"]] = [count + 1, max(hi, row["score"])]
    expected = sorted(
        (city, count, round(hi, 6))
        for city, (count, hi) in by_city.items()
        if count > 100
    )
    expected = sorted(expected, key=lambda item: (-item[1], item[0]))[:5]
    # HAVING must filter some groups without emptying the result.
    assert 0 < len(expected) < 5

    actual = [
        (r["city"], r["n"], round(r["hi"], 6))
        for r in client.execute(query).to_dict()
    ]
    assert actual == expected
    assert _assert_python_rust_query_parity(client, query)
    client.close()


def test_fused_group_agg_not_in_not_between_min_having_matches_reference(tmp_path):
    """Negated leaves (NOT IN, NOT BETWEEN), dictionary equality on the group
    column and the MIN aggregate lane: per-group results must match."""
    client = ApexClient(str(tmp_path), enable_cache=False)
    rows = _fused_test_rows()
    _open_fused_fixture(client, "fusednot", rows)

    query = (
        "SELECT city, COUNT(*) AS n, MIN(score) AS lo FROM fusednot "
        "WHERE age NOT IN (0, 1, 2) AND score NOT BETWEEN 0.0 AND 0.5 "
        "AND city = 'North' "
        "GROUP BY city HAVING COUNT(*) > 10 ORDER BY city"
    )
    matched = [
        row for row in rows
        if row["city"] == "North"
        and row["age"] not in (0, 1, 2)
        and not 0.0 <= row["score"] <= 0.5
    ]
    assert len(matched) > 10
    expected = [
        ("North", len(matched), round(min(row["score"] for row in matched), 6))
    ]

    actual = [
        (r["city"], r["n"], round(r["lo"], 6))
        for r in client.execute(query).to_dict()
    ]
    assert actual == expected
    assert _assert_python_rust_query_parity(client, query)
    client.close()


def test_fused_group_agg_count_star_only_boolean_where_matches_reference(tmp_path):
    """COUNT(*)-only SELECT with a boolean WHERE (dict IN OR int IN) AND
    range: the fused kernel must handle the missing value aggregate and the
    per-group counts must match the reference."""
    client = ApexClient(str(tmp_path), enable_cache=False)
    rows = _fused_test_rows()
    _open_fused_fixture(client, "fusedcnt", rows)

    query = (
        "SELECT city, COUNT(*) AS n FROM fusedcnt "
        "WHERE (city IN ('North', 'South') OR age IN (3, 7, 11)) "
        "AND score >= 1.0 "
        "GROUP BY city ORDER BY city"
    )
    by_city = {}
    for row in rows:
        matched = row["city"] in ("North", "South") or row["age"] in (3, 7, 11)
        if matched and row["score"] >= 1.0:
            by_city[row["city"]] = by_city.get(row["city"], 0) + 1
    expected = [(city, count) for city, count in sorted(by_city.items())]
    # Every city must survive: dict hits for two, age-IN hits for the rest.
    assert len(expected) == 5

    actual = [
        (r["city"], r["n"])
        for r in client.execute(query).to_dict()
    ]
    assert actual == expected
    assert _assert_python_rust_query_parity(client, query)
    client.close()


def test_fused_gate_fallback_shapes_match_generic_pipeline(tmp_path):
    """Shapes outside the fused grammar (LIKE in WHERE, HAVING over a
    non-aggregate column) must fall back to the generic pipeline and stay
    exact."""
    client = ApexClient(str(tmp_path), enable_cache=False)
    rows = _fused_test_rows()
    _open_fused_fixture(client, "fusedfb", rows)

    like_query = (
        "SELECT city, COUNT(*) AS n, SUM(score) AS total FROM fusedfb "
        "WHERE (city IN ('North', 'South') OR age IN (3, 7, 11)) "
        "AND city LIKE 'No%' AND score >= 1.0 "
        "GROUP BY city HAVING COUNT(*) > 5 ORDER BY city"
    )
    by_city = {}
    for row in rows:
        matched = (row["city"] in ("North", "South") or row["age"] in (3, 7, 11))
        if matched and row["city"].startswith("No") and row["score"] >= 1.0:
            count, total = by_city.setdefault(row["city"], [0, 0.0])
            by_city[row["city"]] = [count + 1, total + row["score"]]
    expected_like = [
        (city, count, round(total, 6))
        for city, (count, total) in sorted(by_city.items())
        if count > 5
    ]
    assert len(expected_like) == 1

    actual_like = [
        (r["city"], r["n"], round(r["total"], 6))
        for r in client.execute(like_query).to_dict()
    ]
    assert actual_like == expected_like
    assert _assert_python_rust_query_parity(client, like_query)

    having_query = (
        "SELECT city, COUNT(*) AS n, SUM(score) AS total FROM fusedfb "
        "WHERE (city IN ('North', 'South') OR age IN (3, 7, 11)) "
        "AND score >= 1.0 "
        "GROUP BY city HAVING COUNT(*) > 5 AND MIN(age) >= 0 ORDER BY city"
    )
    by_city = {}
    for row in rows:
        matched = (row["city"] in ("North", "South") or row["age"] in (3, 7, 11))
        if matched and row["score"] >= 1.0:
            count, total = by_city.setdefault(row["city"], [0, 0.0])
            by_city[row["city"]] = [count + 1, total + row["score"]]
    expected_having = [
        (city, count, round(total, 6))
        for city, (count, total) in sorted(by_city.items())
        if count > 5
    ]
    assert len(expected_having) == 5

    actual_having = [
        (r["city"], r["n"], round(r["total"], 6))
        for r in client.execute(having_query).to_dict()
    ]
    assert actual_having == expected_having
    assert _assert_python_rust_query_parity(client, having_query)
    client.close()
