"""
Cross-engine SQL parity and regression coverage: ApexBase vs SQLite vs DuckDB.

Each SQL feature exercised here must produce the same result set (values and
cardinality) on all three engines.  Comparisons are set-based and numeric
tolerance aware because engines legitimately differ in row order for unordered
queries, NULL sort position, and integer-vs-float result typing.

This module also pins the regressions found while extending the public
benchmark:
  * MIN()/MAX() with GROUP BY returned SUM() on the cached fast path.
  * GROUP BY ... ORDER BY ... LIMIT dropped LIMIT on join/vectorized paths.
  * Window functions ignored the outer ORDER BY and LIMIT.
  * LEFT/RIGHT/FULL OUTER JOIN treated extra ON predicates as post-join WHERE.
  * Correlated scalar subqueries truncated FLOAT results into INT64.
  * Numeric literals with digit separators (10_000) failed to parse.
  * Comma cross-join syntax (FROM a, b) was rejected.
"""

import os
import random
import shutil
import sqlite3
import tempfile

import pytest

try:
    import duckdb
except ImportError:
    duckdb = None

try:
    from apexbase import ApexClient
except ImportError as exc:
    pytest.skip(f"ApexBase not available: {exc}", allow_module_level=True)


CITIES = [
    "Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou",
    "Nanjing", "Chengdu", "Wuhan", "Xian", "Qingdao",
]
CATEGORIES = [
    "Electronics", "Clothing", "Food", "Sports", "Books",
    "Home", "Auto", "Health", "Travel", "Gaming",
]
NUM_ROWS = 20_000


def generate_data(n=NUM_ROWS):
    rng = random.Random(42)
    return {
        "id": list(range(1, n + 1)),
        "name": [f"user_{i}" for i in range(n)],
        "age": [rng.randint(18, 80) for _ in range(n)],
        "score": [round(rng.uniform(0, 100), 2) for _ in range(n)],
        "city": [rng.choice(CITIES) for _ in range(n)],
        "category": [rng.choice(CATEGORIES) for _ in range(n)],
    }


def _store(client, table, rows):
    client.create_table(table)
    client.use_table(table)
    client.store(rows)
    client.flush()


def setup_apex(tmp, data):
    client = ApexClient(os.path.join(tmp, "apex"), drop_if_exists=True)
    _store(client, "t", data)
    _store(client, "meta", {
        "city": CITIES,
        "pop": [21_540_000, 24_870_000, 18_680_000, 17_680_000,
                10_360_000, 9_490_000, 20_940_000, 13_690_000,
                13_150_000, 9_480_000],
    })
    _store(client, "extra", {
        "id": list(range(1, 2001)),
        "tag": ["T" if i % 3 == 0 else ("U" if i % 3 == 1 else "V")
                for i in range(2000)],
        "val": [(i * 7) % 500 for i in range(2000)],
    })
    return client


def setup_sqlite(tmp, data):
    con = sqlite3.connect(os.path.join(tmp, "sqlite.db"))
    con.execute(
        "CREATE TABLE t (id INTEGER, name TEXT, age INTEGER, score REAL, "
        "city TEXT, category TEXT)"
    )
    con.executemany("INSERT INTO t VALUES (?,?,?,?,?,?)", list(zip(
        data["id"], data["name"], data["age"], data["score"],
        data["city"], data["category"])))
    con.execute("CREATE TABLE meta (city TEXT, pop INTEGER)")
    con.executemany("INSERT INTO meta VALUES (?,?)", zip(CITIES, [
        21_540_000, 24_870_000, 18_680_000, 17_680_000, 10_360_000,
        9_490_000, 20_940_000, 13_690_000, 13_150_000, 9_480_000]))
    con.execute("CREATE TABLE extra (id INTEGER, tag TEXT, val INTEGER)")
    con.executemany(
        "INSERT INTO extra VALUES (?,?,?)",
        [(i + 1, "T" if i % 3 == 0 else ("U" if i % 3 == 1 else "V"),
          (i * 7) % 500) for i in range(2000)])
    con.commit()
    return con


def setup_duckdb(data):
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE t (id INTEGER, name VARCHAR, age INTEGER, score DOUBLE, "
        "city VARCHAR, category VARCHAR)"
    )
    con.executemany("INSERT INTO t VALUES (?,?,?,?,?,?)", list(zip(
        data["id"], data["name"], data["age"], data["score"],
        data["city"], data["category"])))
    con.execute("CREATE TABLE meta (city VARCHAR, pop INTEGER)")
    con.executemany("INSERT INTO meta VALUES (?,?)", zip(CITIES, [
        21_540_000, 24_870_000, 18_680_000, 17_680_000, 10_360_000,
        9_490_000, 20_940_000, 13_690_000, 13_150_000, 9_480_000]))
    con.execute("CREATE TABLE extra (id INTEGER, tag VARCHAR, val INTEGER)")
    con.executemany(
        "INSERT INTO extra VALUES (?,?,?)",
        [(i + 1, "T" if i % 3 == 0 else ("U" if i % 3 == 1 else "V"),
          (i * 7) % 500) for i in range(2000)])
    return con


def normalize(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, int):
        return ("num", float(value))
    if isinstance(value, float):
        return ("num", round(value, 4))
    return ("str", str(value))


def run_all(client, con, dcon, sql):
    """Return {(normalized row tuples)} for each engine, raising on errors."""
    results = {}
    apex_rows = client.execute(sql).to_dict()
    results["apex"] = {
        tuple(normalize(v) for v in row.values()) for row in apex_rows
    }
    for name, handle in (("sqlite", con), ("duckdb", dcon)):
        cur = handle.execute(sql)
        columns = [d[0] for d in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        results[name] = {
            tuple(normalize(v) for v in row.values()) for row in rows
        }
    return results


PARITY_QUERIES = [
    # Aggregation correctness on grouped MAX/MIN (float and int columns).
    "SELECT city, MAX(score) AS mx FROM t GROUP BY city ORDER BY city",
    "SELECT city, MIN(score) AS mn FROM t GROUP BY city ORDER BY city",
    "SELECT city, MAX(score) AS mx, SUM(score) AS s FROM t GROUP BY city ORDER BY city",
    "SELECT city, MIN(age) AS mn, MAX(age) AS mx FROM t GROUP BY city ORDER BY city",
    "SELECT city, COUNT(CASE WHEN age > 40 THEN 1 END) AS c FROM t GROUP BY city ORDER BY city",
    # GROUP BY + ORDER BY + LIMIT on join and non-join paths.
    "SELECT city, COUNT(*) AS c FROM t GROUP BY city ORDER BY c DESC LIMIT 5",
    "SELECT t.city, COUNT(*) AS c FROM t JOIN meta m ON t.city = m.city GROUP BY t.city ORDER BY c DESC LIMIT 5",
    # Joins, including extra ON predicates on outer joins.
    "SELECT COUNT(*) AS c FROM t LEFT JOIN meta m ON t.city = m.city AND m.pop > 5000000",
    "SELECT t.id, m.city FROM t LEFT JOIN meta m ON t.city = m.city AND m.pop > 15000000 WHERE t.id <= 20 ORDER BY t.id",
    "SELECT COUNT(*) AS c FROM t LEFT JOIN extra e ON t.id = e.id AND e.val > 999999",
    "SELECT t.city, COUNT(*) AS c FROM t JOIN meta m ON t.city = m.city WHERE t.age > 30 GROUP BY t.city ORDER BY c DESC LIMIT 5",
    # Window functions with outer ORDER BY + LIMIT.
    "SELECT id, SUM(score) OVER (PARTITION BY city ORDER BY id) AS run FROM t WHERE id <= 2000 ORDER BY id LIMIT 100",
    "SELECT id, city, RANK() OVER (PARTITION BY city ORDER BY score DESC) AS rk FROM t WHERE id <= 2000 ORDER BY rk, id LIMIT 100",
    "SELECT id, city, LAG(score) OVER (PARTITION BY city ORDER BY id) AS prev FROM t WHERE id <= 2000 ORDER BY id LIMIT 100",
    # Set operations and subqueries.
    "SELECT city FROM t WHERE age = 25 UNION SELECT city FROM t WHERE age = 26 ORDER BY city",
    "SELECT city FROM t WHERE age BETWEEN 20 AND 30 INTERSECT SELECT city FROM t WHERE age BETWEEN 25 AND 35 ORDER BY city",
    "SELECT COUNT(*) AS c FROM t WHERE city IN (SELECT city FROM meta WHERE pop > 15000000)",
    "SELECT COUNT(*) AS c FROM t b WHERE EXISTS "
    "(SELECT 1 FROM meta m WHERE m.city = b.city AND m.pop > 15000000)",
    "SELECT city, (SELECT MAX(score) FROM t t2 WHERE t2.city = t.city) AS mx FROM t WHERE id <= 50 ORDER BY id",
    "SELECT city, cnt FROM (SELECT city, COUNT(*) AS cnt FROM t GROUP BY city) d WHERE cnt > 1000 ORDER BY cnt DESC LIMIT 5",
    "WITH c AS (SELECT city, AVG(score) AS av FROM t GROUP BY city) SELECT city FROM c WHERE av > 50 ORDER BY av DESC LIMIT 5",
    # Expressions: CASE, string and numeric functions, NULL handling.
    "SELECT UPPER(city) AS u, LENGTH(name) AS ln, SUBSTR(name, 1, 5) AS sub, "
    "CONCAT(city, '-', category) AS cc, TRIM(category) AS tr FROM t WHERE id <= 100 ORDER BY id",
    "SELECT ROUND(score, 0) AS r, ABS(age - 40) AS a, FLOOR(score) AS f, CEIL(score) AS c FROM t WHERE id <= 100 ORDER BY id",
    "SELECT COUNT(*) AS c FROM t WHERE COALESCE(NULLIF(category, 'Books'), 'none') = 'Books'",
    "SELECT DISTINCT city, category FROM t ORDER BY city, category LIMIT 50",
    "SELECT COUNT(*) AS c FROM t WHERE age NOT BETWEEN 20 AND 40 AND name NOT LIKE 'user_5%'",
    "SELECT city FROM t GROUP BY city HAVING AVG(score) > 50 AND COUNT(*) > 1000 ORDER BY city",
    # Keep the parity query portable to SQLite versions before 3.46. The
    # ApexBase-specific digit-separator regression remains covered below.
    "SELECT id FROM t WHERE id > 10000 ORDER BY id LIMIT 5",
    "SELECT t.id, m.city FROM t, meta m WHERE t.id <= 10 ORDER BY t.id, m.city LIMIT 50",
    "SELECT COUNT(*) AS c FROM t, meta m WHERE t.age < 21",
    "SELECT COUNT(*) AS c FROM t, meta m WHERE m.pop < 10000000",
    "SELECT COUNT(t.age) AS c FROM t, meta m WHERE t.age < 21",
]


# Every test in this module is read-only, so build the identical 20K-row
# cross-engine dataset once instead of rebuilding it for every assertion.
@pytest.fixture(scope="module")
def engines():
    if duckdb is None:
        pytest.skip("duckdb is required for parity tests")
    tmp = tempfile.mkdtemp(prefix="parity_")
    data = generate_data()
    client = setup_apex(tmp, data)
    con = setup_sqlite(tmp, data)
    dcon = setup_duckdb(data)
    yield client, con, dcon
    client.close()
    con.close()
    dcon.close()
    shutil.rmtree(tmp, ignore_errors=True)


class TestCrossEngineParity:

    @pytest.mark.parametrize("sql", PARITY_QUERIES)
    def test_parity(self, engines, sql):
        client, con, dcon = engines
        results = run_all(client, con, dcon, sql)
        apex = results["apex"]
        assert apex == results["sqlite"], (
            f"ApexBase != SQLite for {sql}\n"
            f"apex-only={apex - results['sqlite']}\n"
            f"sqlite-only={results['sqlite'] - apex}"
        )
        assert apex == results["duckdb"], (
            f"ApexBase != DuckDB for {sql}\n"
            f"apex-only={apex - results['duckdb']}\n"
            f"duckdb-only={results['duckdb'] - apex}"
        )


class TestGroupedMinMaxRegressions:
    """MIN/MAX with GROUP BY must not return SUM (cached fast-path bug)."""

    def test_grouped_max_float_is_max_not_sum(self, engines):
        client, _, _ = engines
        rows = client.execute(
            "SELECT city, MAX(score) AS mx FROM t GROUP BY city ORDER BY city"
        ).to_dict()
        for row in rows:
            assert row["mx"] <= 100.0
            assert row["mx"] > 90.0

    def test_grouped_min_float_is_min_not_sum(self, engines):
        client, _, _ = engines
        rows = client.execute(
            "SELECT city, MIN(score) AS mn FROM t GROUP BY city ORDER BY city"
        ).to_dict()
        for row in rows:
            assert row["mn"] < 10.0

    def test_grouped_max_int_is_max_not_sum(self, engines):
        client, _, _ = engines
        rows = client.execute(
            "SELECT city, MAX(age) AS mx FROM t GROUP BY city ORDER BY city"
        ).to_dict()
        assert all(row["mx"] <= 80 for row in rows)

    def test_mixed_min_max_columns(self, engines):
        client, _, _ = engines
        rows = client.execute(
            "SELECT city, MAX(age) AS ma, MAX(score) AS ms, MIN(score) AS mn "
            "FROM t GROUP BY city ORDER BY city"
        ).to_dict()
        for row in rows:
            assert row["ma"] <= 80
            assert 90 < row["ms"] <= 100
            assert row["mn"] < 10


class TestWindowOrderLimitRegression:
    def test_window_outer_order_by_limit(self, engines):
        client, _, _ = engines
        rows = client.execute(
            "SELECT id, SUM(score) OVER (PARTITION BY city ORDER BY id) AS run "
            "FROM t WHERE id <= 5000 ORDER BY id LIMIT 10"
        ).to_dict()
        assert len(rows) == 10
        ids = [row["id"] for row in rows]
        assert ids == sorted(ids)
        assert ids[0] == 1

    def test_window_rank_order_limit(self, engines):
        client, _, _ = engines
        rows = client.execute(
            "SELECT id, city, RANK() OVER (PARTITION BY city ORDER BY score DESC) AS rk "
            "FROM t WHERE id <= 5000 ORDER BY rk, id LIMIT 10"
        ).to_dict()
        assert len(rows) == 10
        assert rows[0]["rk"] == 1


class TestOuterJoinExtraOnRegression:
    def test_left_join_extra_condition_preserves_unmatched(self, engines):
        client, _, _ = engines
        count = client.execute(
            "SELECT COUNT(*) AS c FROM t LEFT JOIN meta m "
            "ON t.city = m.city AND m.pop > 5000000"
        ).scalar()
        assert count == NUM_ROWS

    def test_left_join_extra_condition_rows(self, engines):
        client, _, _ = engines
        rows = client.execute(
            "SELECT t.id, m.city FROM t LEFT JOIN meta m "
            "ON t.city = m.city AND m.pop > 20000000 WHERE t.id <= 20 ORDER BY t.id"
        ).to_dict()
        assert len(rows) == 20
        # Beijing/Shanghai/Chengdu pop > 20M; other cities are NULL.
        for row in rows:
            if row["city"] is not None:
                assert row["city"] in {"Beijing", "Shanghai", "Chengdu"}

    def test_full_outer_join_extra_condition(self, engines):
        client, _, _ = engines
        rows = client.execute(
            "SELECT m.city, t.id FROM meta m FULL OUTER JOIN t "
            "ON t.city = m.city AND t.id = 1 ORDER BY m.city LIMIT 12"
        ).to_dict()
        assert len(rows) == 12
        # Unmatched t rows must survive (m.city NULL, t.id filled).
        assert any(row["city"] is None and row["id"] is not None for row in rows)


class TestCorrelatedScalarSubqueryRegression:
    def test_correlated_scalar_subquery_preserves_float(self, engines):
        client, _, _ = engines
        rows = client.execute(
            "SELECT city, (SELECT MAX(score) FROM t t2 WHERE t2.city = t.city) AS mx "
            "FROM t WHERE id <= 50 ORDER BY id"
        ).to_dict()
        for row in rows:
            assert isinstance(row["mx"], float)
            assert 90.0 < row["mx"] <= 100.0


class TestParserRegressions:
    def test_numeric_literal_underscores(self, engines):
        client, _, _ = engines
        rows = client.execute(
            "SELECT id FROM t WHERE id > 10_000 ORDER BY id LIMIT 5"
        ).to_dict()
        assert rows[0]["id"] == 10_001

    def test_comma_cross_join(self, engines):
        client, _, _ = engines
        rows = client.execute(
            "SELECT t.id, m.city FROM t, meta m WHERE t.id <= 10 "
            "ORDER BY t.id, m.city LIMIT 50"
        ).to_dict()
        assert len(rows) == 50
        cities = {row["city"] for row in rows}
        assert cities == set(CITIES)
