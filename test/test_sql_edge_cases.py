"""SQL edge cases: set ops, subqueries, CTEs, window functions, NULLs, JOINs, etc."""
import pytest
import tempfile
import shutil
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

try:
    from apexbase import ApexClient
except ImportError as e:
    pytest.skip(f"ApexBase not available: {e}", allow_module_level=True)


def xfail_sql(client, sql):
    return client.execute(sql)


@pytest.fixture
def tmp_client():
    d = tempfile.mkdtemp()
    c = ApexClient(dirpath=d)
    yield c
    c.close()
    shutil.rmtree(d, ignore_errors=True)


# ============================================================
# UNION / INTERSECT / EXCEPT
# ============================================================

class TestSetOperations:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE a (val INT)")
        self.c.execute("INSERT INTO a VALUES (1),(2),(3),(4)")
        self.c.execute("CREATE TABLE b (val INT)")
        self.c.execute("INSERT INTO b VALUES (2),(3),(5),(6)")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_union_dedup(self):
        rv = xfail_sql(self.c, "SELECT val FROM a UNION SELECT val FROM b ORDER BY val")
        vals = [r['val'] for r in rv]
        assert sorted(vals) == [1, 2, 3, 4, 5, 6]
        assert len(vals) == len(set(vals))

    def test_union_all_keeps_dupes(self):
        rv = xfail_sql(self.c, "SELECT val FROM a UNION ALL SELECT val FROM b ORDER BY val")
        assert len(rv) == 8

    def test_intersect(self):
        rv = xfail_sql(self.c, "SELECT val FROM a INTERSECT SELECT val FROM b ORDER BY val")
        assert [r['val'] for r in rv] == [2, 3]

    def test_except(self):
        rv = xfail_sql(self.c, "SELECT val FROM a EXCEPT SELECT val FROM b ORDER BY val")
        assert [r['val'] for r in rv] == [1, 4]

    def test_union_with_order_by_limit(self):
        rv = xfail_sql(self.c, "SELECT val FROM a UNION SELECT val FROM b ORDER BY val LIMIT 3")
        assert len(rv) == 3
        vals = [r['val'] for r in rv]
        assert vals == sorted(vals)

    def test_union_with_order_by_offset(self):
        rv = xfail_sql(self.c, "SELECT val FROM a UNION SELECT val FROM b ORDER BY val LIMIT 3 OFFSET 2")
        assert len(rv) == 3
        vals = [r['val'] for r in rv]
        assert vals == sorted(vals)

    def test_intersect_empty_result(self):
        self.c.execute("CREATE TABLE c (val INT)")
        self.c.execute("INSERT INTO c VALUES (99),(100)")
        rv = xfail_sql(self.c, "SELECT val FROM a INTERSECT SELECT val FROM c ORDER BY val")
        assert len(rv) == 0

    def test_except_no_overlap_returns_all_left(self):
        self.c.execute("CREATE TABLE d (val INT)")
        self.c.execute("INSERT INTO d VALUES (99),(100)")
        rv = xfail_sql(self.c, "SELECT val FROM a EXCEPT SELECT val FROM d ORDER BY val")
        assert [r['val'] for r in rv] == [1, 2, 3, 4]

    def test_union_all_count(self):
        rv = xfail_sql(self.c, "SELECT COUNT(*) FROM (SELECT val FROM a UNION ALL SELECT val FROM b) t")
        assert rv.scalar() == 8

    def test_self_union_dedup(self):
        rv = xfail_sql(self.c, "SELECT val FROM a UNION SELECT val FROM a ORDER BY val")
        assert [r['val'] for r in rv] == [1, 2, 3, 4]

    def test_self_union_all_doubles(self):
        rv = xfail_sql(self.c, "SELECT val FROM a UNION ALL SELECT val FROM a ORDER BY val")
        assert len(rv) == 8


class TestSetOperationsMultiCol:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE x (id INT, name STRING)")
        self.c.execute("INSERT INTO x (id, name) VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')")
        self.c.execute("CREATE TABLE y (id INT, name STRING)")
        self.c.execute("INSERT INTO y (id, name) VALUES (2,'Bob'),(3,'Carol'),(4,'Dave')")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_union_multi_col_dedup(self):
        rv = xfail_sql(self.c, "SELECT id, name FROM x UNION SELECT id, name FROM y ORDER BY id")
        ids = [r['id'] for r in rv]
        assert ids == [1, 2, 3, 4]
        assert len(ids) == len(set(ids))

    def test_union_all_multi_col_count(self):
        rv = xfail_sql(self.c, "SELECT id, name FROM x UNION ALL SELECT id, name FROM y ORDER BY id")
        assert len(rv) == 6

    def test_intersect_multi_col(self):
        rv = xfail_sql(self.c, "SELECT id, name FROM x INTERSECT SELECT id, name FROM y ORDER BY id")
        ids = [r['id'] for r in rv]
        assert ids == [2, 3]

    def test_except_multi_col(self):
        rv = xfail_sql(self.c, "SELECT id, name FROM x EXCEPT SELECT id, name FROM y ORDER BY id")
        ids = [r['id'] for r in rv]
        assert ids == [1]

    def test_union_string_values_dedup(self):
        rv = xfail_sql(self.c, "SELECT name FROM x UNION SELECT name FROM y ORDER BY name")
        names = [r['name'] for r in rv]
        assert names == ['Alice', 'Bob', 'Carol', 'Dave']
        assert len(names) == len(set(names))


# ============================================================
# Subqueries
# ============================================================

class TestSubqueries:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE emp (name STRING, dept STRING, salary INT)")
        self.c.execute("""INSERT INTO emp VALUES
            ('Alice','Eng',90000),('Bob','Eng',80000),
            ('Carol','Sales',70000),('Dave','Sales',75000),('Eve','HR',60000)""")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_scalar_subquery_in_where(self):
        rv = xfail_sql(self.c,
            "SELECT name FROM emp WHERE salary > (SELECT AVG(salary) FROM emp) ORDER BY name")
        assert 'Alice' in [r['name'] for r in rv]

    def test_in_subquery(self):
        rv = xfail_sql(self.c,
            "SELECT name FROM emp WHERE dept IN "
            "(SELECT DISTINCT dept FROM emp WHERE salary > 75000) ORDER BY name")
        names = [r['name'] for r in rv]
        assert 'Alice' in names and 'Bob' in names

    def test_not_in_subquery(self):
        rv = xfail_sql(self.c,
            "SELECT name FROM emp WHERE dept NOT IN "
            "(SELECT DISTINCT dept FROM emp WHERE salary > 80000) ORDER BY name")
        names = [r['name'] for r in rv]
        assert 'Carol' in names and 'Alice' not in names

    def test_exists_correlated(self):
        rv = xfail_sql(self.c, """
            SELECT DISTINCT dept FROM emp e1
            WHERE EXISTS (SELECT 1 FROM emp e2 WHERE e2.dept=e1.dept AND e2.name!=e1.name)
            ORDER BY dept""")
        depts = [r['dept'] for r in rv]
        assert 'Eng' in depts and 'HR' not in depts

    def test_scalar_subquery_in_select(self):
        rv = xfail_sql(self.c,
            "SELECT name, (SELECT MAX(salary) FROM emp) as mx FROM emp ORDER BY name")
        rows = {r['name']: r for r in rv}
        assert rows['Alice']['mx'] == 90000 and rows['Eve']['mx'] == 90000


# ============================================================
# Multiple CTEs
# ============================================================

class TestMultipleCTEs:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE orders (customer STRING, amount INT)")
        self.c.execute("""INSERT INTO orders VALUES
            ('Alice',100),('Alice',200),('Bob',300),('Bob',50),
            ('Carol',400),('Carol',80)""")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_two_ctes_chained(self):
        rv = xfail_sql(self.c, """
            WITH totals AS (
                SELECT customer, SUM(amount) as total FROM orders GROUP BY customer
            ),
            big AS (
                SELECT customer, total FROM totals WHERE total > 200
            )
            SELECT customer FROM big ORDER BY total DESC""")
        names = [r['customer'] for r in rv]
        assert 'Carol' in names and 'Bob' in names

    def test_cte_with_window_function(self):
        rv = xfail_sql(self.c, """
            WITH base AS (
                SELECT customer, amount,
                       ROW_NUMBER() OVER (PARTITION BY customer ORDER BY amount DESC) as rn
                FROM orders
            )
            SELECT customer, amount FROM base WHERE rn = 1 ORDER BY customer""")
        rows = {r['customer']: r['amount'] for r in rv}
        assert rows.get('Alice') == 200
        assert rows.get('Bob') == 300
        assert rows.get('Carol') == 400


# ============================================================
# Advanced window functions
# ============================================================

class TestAdvancedWindowFunctions:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE scores (player STRING, game INT, score INT)")
        self.c.execute("""INSERT INTO scores VALUES
            ('Alice',1,90),('Alice',2,85),('Alice',3,95),
            ('Bob',1,70),('Bob',2,80),('Bob',3,75)""")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_running_sum_partition(self):
        rv = xfail_sql(self.c,
            "SELECT player, game, SUM(score) OVER "
            "(PARTITION BY player ORDER BY game) as rs "
            "FROM scores ORDER BY player, game")
        alice = sorted([r for r in rv if r['player'] == 'Alice'], key=lambda x: x['game'])
        assert alice[0]['rs'] == 90
        assert alice[1]['rs'] == 175
        assert alice[2]['rs'] == 270

    def test_rank_vs_dense_rank_ties(self):
        self.c.execute("CREATE TABLE t2 (val INT)")
        self.c.execute("INSERT INTO t2 VALUES (10),(10),(20),(30)")
        rv = xfail_sql(self.c,
            "SELECT val, RANK() OVER (ORDER BY val) as rnk, "
            "DENSE_RANK() OVER (ORDER BY val) as drnk FROM t2 ORDER BY val, rnk")
        rows = rv.to_dict()
        tens = [r for r in rows if r['val'] == 10]
        assert all(r['rnk'] == 1 for r in tens)
        assert all(r['drnk'] == 1 for r in tens)
        twenty = next(r for r in rows if r['val'] == 20)
        assert twenty['rnk'] == 3   # RANK skips 2
        assert twenty['drnk'] == 2  # DENSE_RANK is consecutive

    def test_lag_lead(self):
        rv = xfail_sql(self.c,
            "SELECT player, game, "
            "LAG(score) OVER (PARTITION BY player ORDER BY game) as prev, "
            "LEAD(score) OVER (PARTITION BY player ORDER BY game) as nxt "
            "FROM scores ORDER BY player, game")
        rows = {(r['player'], r['game']): r for r in rv}
        assert rows[('Alice', 1)]['prev'] is None
        assert rows[('Alice', 2)]['prev'] == 90
        assert rows[('Alice', 3)]['nxt'] is None

    def test_first_value(self):
        rv = xfail_sql(self.c,
            "SELECT player, game, "
            "FIRST_VALUE(score) OVER (PARTITION BY player ORDER BY game) as first "
            "FROM scores ORDER BY player, game")
        rows = {(r['player'], r['game']): r for r in rv}
        assert rows[('Alice', 2)]['first'] == 90
        assert rows[('Bob', 3)]['first'] == 70


# ============================================================
# NULL handling
# ============================================================

class TestNullHandling:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE t (a INT, b STRING)")
        self.c.execute("INSERT INTO t VALUES (1,'x'),(NULL,'y'),(3,NULL),(NULL,NULL)")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_is_null_filter(self):
        assert len(self.c.execute("SELECT a FROM t WHERE a IS NULL")) == 2

    def test_is_not_null_filter(self):
        rv = self.c.execute("SELECT a FROM t WHERE a IS NOT NULL ORDER BY a")
        assert [r['a'] for r in rv] == [1, 3]

    def test_null_equals_null_is_empty(self):
        assert len(self.c.execute("SELECT a FROM t WHERE a = NULL")) == 0

    def test_coalesce_int(self):
        rv = self.c.execute("SELECT COALESCE(a, 0) as v FROM t ORDER BY v")
        assert 0 in [r['v'] for r in rv]

    def test_coalesce_string(self):
        rv = self.c.execute("SELECT COALESCE(b, 'missing') as v FROM t ORDER BY v")
        assert 'missing' in [r['v'] for r in rv]

    def test_count_star_includes_nulls(self):
        assert self.c.execute("SELECT COUNT(*) as c FROM t").scalar() == 4

    def test_count_column_excludes_nulls(self):
        assert self.c.execute("SELECT COUNT(a) as c FROM t").scalar() == 2

    def test_sum_ignores_nulls(self):
        assert self.c.execute("SELECT SUM(a) as s FROM t").scalar() == 4

    def test_avg_ignores_nulls(self):
        assert self.c.execute("SELECT AVG(a) as v FROM t").scalar() == 2.0

    def test_group_by_null_forms_own_group(self):
        rv = self.c.execute("SELECT a, COUNT(*) as cnt FROM t GROUP BY a ORDER BY a")
        null_row = next((r for r in rv if r['a'] is None), None)
        assert null_row is not None and null_row['cnt'] == 2

    def test_ifnull(self):
        rv = self.c.execute("SELECT IFNULL(a, -1) as v FROM t ORDER BY v")
        assert -1 in [r['v'] for r in rv]


# ============================================================
# CASE expressions
# ============================================================

class TestCaseExpressions:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE t (name STRING, score INT)")
        self.c.execute("""INSERT INTO t VALUES
            ('Alice',95),('Bob',75),('Carol',55),('Dave',45),('Eve',NULL)""")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_case_when_grade(self):
        rv = self.c.execute("""
            SELECT name,
                   CASE WHEN score >= 90 THEN 'A'
                        WHEN score >= 70 THEN 'B'
                        WHEN score >= 50 THEN 'C'
                        ELSE 'F'
                   END as grade
            FROM t ORDER BY name""")
        rows = {r['name']: r['grade'] for r in rv}
        assert rows['Alice'] == 'A' and rows['Bob'] == 'B'
        assert rows['Carol'] == 'C' and rows['Dave'] == 'F'

    def test_case_null_detection(self):
        rv = self.c.execute("""
            SELECT name,
                   CASE WHEN score IS NULL THEN 'no score' ELSE 'has score' END as status
            FROM t ORDER BY name""")
        rows = {r['name']: r['status'] for r in rv}
        assert rows['Eve'] == 'no score' and rows['Alice'] == 'has score'

    def test_case_in_group_by(self):
        rv = xfail_sql(self.c, """
            SELECT CASE WHEN score >= 70 THEN 'pass' ELSE 'fail' END as result,
                   COUNT(*) as cnt
            FROM t WHERE score IS NOT NULL
            GROUP BY result ORDER BY result""")
        rows = {r['result']: r['cnt'] for r in rv}
        assert rows.get('pass') == 2 and rows.get('fail') == 2


# ============================================================
# String functions
# ============================================================

class TestStringFunctions:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE t (s STRING)")
        self.c.execute("""INSERT INTO t VALUES
            ('Hello World'),('  spaces  '),('foo BAR'),('abc123')""")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_upper(self):
        assert self.c.execute("SELECT UPPER(s) as u FROM t WHERE s='foo BAR'").first()['u'] == 'FOO BAR'

    def test_lower(self):
        assert self.c.execute("SELECT LOWER(s) as l FROM t WHERE s='Hello World'").first()['l'] == 'hello world'

    def test_length(self):
        assert self.c.execute("SELECT LENGTH(s) as n FROM t WHERE s='Hello World'").first()['n'] == 11

    def test_trim(self):
        assert self.c.execute("SELECT TRIM(s) as t FROM t WHERE s='  spaces  '").first()['t'] == 'spaces'

    def test_substr(self):
        assert self.c.execute("SELECT SUBSTR(s,1,5) as sub FROM t WHERE s='Hello World'").first()['sub'] == 'Hello'

    def test_replace(self):
        rv = self.c.execute(
            "SELECT REPLACE(s,'World','ApexBase') as r FROM t WHERE s='Hello World'"
        )
        assert rv.first()['r'] == 'Hello ApexBase'

    def test_like_contains(self):
        assert len(self.c.execute("SELECT s FROM t WHERE s LIKE '%World%'")) == 1

    def test_like_prefix(self):
        assert len(self.c.execute("SELECT s FROM t WHERE s LIKE 'Hello%'")) == 1

    def test_like_suffix(self):
        assert len(self.c.execute("SELECT s FROM t WHERE s LIKE '%123'")) == 1

    def test_case_sensitive_string_filter(self):
        assert len(self.c.execute("SELECT s FROM t WHERE s = 'foo BAR'")) == 1
        assert len(self.c.execute("SELECT s FROM t WHERE s = 'FOO BAR'")) == 0


# ============================================================
# Numeric edge cases
# ============================================================

class TestNumericEdgeCases:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE t (a INT, b FLOAT)")
        self.c.execute("INSERT INTO t VALUES (0,0.0),(1,1.5),(100,3.14),(-5,-2.7),(2,0.001)")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_abs_negative(self):
        assert self.c.execute("SELECT ABS(a) as v FROM t WHERE a=-5").first()['v'] == 5

    def test_round(self):
        assert self.c.execute("SELECT ROUND(b,1) as r FROM t WHERE a=100").first()['r'] == 3.1

    def test_float_multiply(self):
        val = self.c.execute("SELECT b*2 as v FROM t WHERE a=1").first()['v']
        assert abs(val - 3.0) < 1e-9

    def test_scientific_notation_literals(self):
        row = self.c.execute(
            "SELECT 1.23e-5 AS small, 1E10 AS large, -2.5e+3 AS negative "
            "FROM t WHERE a=0"
        ).first()
        assert row['small'] == pytest.approx(1.23e-5)
        assert row['large'] == pytest.approx(1e10)
        assert row['negative'] == pytest.approx(-2500.0)

    def test_max_min_with_negatives(self):
        row = self.c.execute("SELECT MAX(a) as mx, MIN(a) as mn FROM t").first()
        assert row['mx'] == 100 and row['mn'] == -5

    def test_sum_with_zero(self):
        assert self.c.execute("SELECT SUM(a) as s FROM t WHERE a=0").scalar() == 0

    def test_between_inclusive(self):
        rv = self.c.execute("SELECT a FROM t WHERE a BETWEEN 1 AND 100 ORDER BY a")
        vals = [r['a'] for r in rv]
        assert vals == [1, 2, 100]

    def test_negative_values_order(self):
        rv = self.c.execute("SELECT a FROM t ORDER BY a ASC")
        vals = [r['a'] for r in rv]
        assert vals[0] == -5 and vals[-1] == 100


# ============================================================
# JOIN types
# ============================================================

class TestJoins:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE emp (name STRING, dept_id INT)")
        self.c.execute("INSERT INTO emp VALUES ('Alice',1),('Bob',2),('Carol',1),('Dave',NULL)")
        self.c.execute("CREATE TABLE dept (id INT, dept_name STRING)")
        self.c.execute("INSERT INTO dept VALUES (1,'Eng'),(2,'Sales'),(3,'HR')")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_inner_join_excludes_nulls(self):
        rv = self.c.execute(
            "SELECT e.name, d.dept_name FROM emp e JOIN dept d ON e.dept_id=d.id ORDER BY e.name")
        names = [r['name'] for r in rv]
        assert 'Alice' in names and 'Dave' not in names

    def test_left_join_includes_unmatched(self):
        rv = self.c.execute(
            "SELECT e.name, d.dept_name FROM emp e LEFT JOIN dept d ON e.dept_id=d.id ORDER BY e.name")
        assert len(rv) == 4
        dave = next(r for r in rv if r['name'] == 'Dave')
        assert dave['dept_name'] is None

    def test_join_with_where_filter(self):
        rv = self.c.execute("""
            SELECT e.name FROM emp e JOIN dept d ON e.dept_id=d.id
            WHERE d.dept_name='Eng' ORDER BY e.name""")
        assert [r['name'] for r in rv] == ['Alice', 'Carol']

    def test_self_join(self):
        rv = xfail_sql(self.c,
            "SELECT a.name as e1, b.name as e2 FROM emp a "
            "JOIN emp b ON a.dept_id=b.dept_id AND a.name < b.name ORDER BY a.name")
        pairs = [(r['e1'], r['e2']) for r in rv]
        assert ('Alice', 'Carol') in pairs

    def test_cross_join_count(self):
        rv = xfail_sql(self.c,
            "SELECT COUNT(*) as c FROM emp CROSS JOIN dept")
        assert rv.scalar() == 12

    def test_join_aggregate(self):
        rv = self.c.execute("""
            SELECT d.dept_name, COUNT(e.name) as cnt
            FROM dept d LEFT JOIN emp e ON d.id=e.dept_id
            GROUP BY d.dept_name ORDER BY d.dept_name""")
        rows = {r['dept_name']: r['cnt'] for r in rv}
        assert rows['Eng'] == 2 and rows['Sales'] == 1 and rows['HR'] == 0


class TestOuterJoinQualifiedKeys:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE l (id INT, name STRING)")
        self.c.execute("CREATE TABLE r (id INT, tag STRING)")
        self.c.execute("INSERT INTO l VALUES (1,'a'),(2,'b')")
        self.c.execute("INSERT INTO r VALUES (2,'x'),(3,'y')")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_right_join_preserves_qualified_right_key(self):
        rv = self.c.execute("""
            SELECT l.id AS lid, r.id AS rid, l.name, r.tag
            FROM l RIGHT JOIN r ON l.id = r.id
            ORDER BY r.tag
        """)
        assert rv.to_dict() == [
            {"lid": 2, "rid": 2, "name": "b", "tag": "x"},
            {"lid": None, "rid": 3, "name": None, "tag": "y"},
        ]

    def test_full_outer_join_preserves_both_join_keys(self):
        rv = self.c.execute("""
            SELECT l.id AS lid, r.id AS rid, l.name, r.tag
            FROM l FULL OUTER JOIN r ON l.id = r.id
            ORDER BY COALESCE(l.id, r.id)
        """)
        assert rv.to_dict() == [
            {"lid": 1, "rid": None, "name": "a", "tag": None},
            {"lid": 2, "rid": 2, "name": "b", "tag": "x"},
            {"lid": None, "rid": 3, "name": None, "tag": "y"},
        ]


# ============================================================
# GROUP BY + HAVING
# ============================================================

class TestGroupByHaving:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE sales (rep STRING, region STRING, amount INT)")
        self.c.execute("""INSERT INTO sales VALUES
            ('Alice','North',100),('Alice','South',200),
            ('Bob','North',150),('Bob','North',50),
            ('Carol','South',300),('Dave','East',10)""")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_having_sum(self):
        rv = self.c.execute("""
            SELECT rep, SUM(amount) as total FROM sales
            GROUP BY rep HAVING SUM(amount) > 150 ORDER BY total DESC""")
        reps = [r['rep'] for r in rv]
        assert 'Carol' in reps and 'Alice' in reps and 'Dave' not in reps

    def test_having_count(self):
        rv = self.c.execute("""
            SELECT rep, COUNT(*) as cnt FROM sales
            GROUP BY rep HAVING COUNT(*) > 1 ORDER BY rep""")
        reps = [r['rep'] for r in rv]
        assert 'Alice' in reps and 'Bob' in reps and 'Carol' not in reps

    def test_group_by_two_columns(self):
        rv = self.c.execute("""
            SELECT rep, region, SUM(amount) as total FROM sales
            GROUP BY rep, region ORDER BY rep, region""")
        rows = {(r['rep'], r['region']): r['total'] for r in rv}
        assert rows[('Alice', 'North')] == 100 and rows[('Bob', 'North')] == 200

    def test_order_by_aggregate(self):
        rv = self.c.execute("""
            SELECT rep, SUM(amount) as total FROM sales GROUP BY rep ORDER BY SUM(amount) DESC""")
        rows = rv.to_dict()
        totals = [r['total'] for r in rows]
        assert totals == sorted(totals, reverse=True), f"Expected DESC order, got {totals}"
        assert rows[-1]['rep'] == 'Dave'

    def test_having_avg(self):
        rv = self.c.execute("""
            SELECT region, AVG(amount) as avg_amt FROM sales
            GROUP BY region HAVING AVG(amount) > 100 ORDER BY region""")
        regions = [r['region'] for r in rv]
        assert 'South' in regions


# ============================================================
# LIMIT / OFFSET edge cases
# ============================================================

class TestLimitOffsetEdge:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE t (n INT)")
        self.c.execute("INSERT INTO t VALUES " + ",".join(f"({i})" for i in range(10)))

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_limit_exceeds_rows(self):
        assert len(self.c.execute("SELECT n FROM t LIMIT 999")) == 10

    def test_offset_skips_rows(self):
        rv = self.c.execute("SELECT n FROM t ORDER BY n LIMIT 3 OFFSET 5")
        assert [r['n'] for r in rv] == [5, 6, 7]

    def test_offset_equals_row_count(self):
        assert len(self.c.execute("SELECT n FROM t ORDER BY n LIMIT 5 OFFSET 10")) == 0

    def test_offset_exceeds_row_count(self):
        assert len(self.c.execute("SELECT n FROM t ORDER BY n LIMIT 5 OFFSET 100")) == 0

    def test_offset_last_row(self):
        rv = self.c.execute("SELECT n FROM t ORDER BY n LIMIT 1 OFFSET 9")
        assert len(rv) == 1 and rv.first()['n'] == 9

    def test_limit_zero(self):
        rv = xfail_sql(self.c, "SELECT n FROM t ORDER BY n LIMIT 0")
        assert len(rv) == 0

    def test_distinct_with_limit(self):
        self.c.execute("INSERT INTO t VALUES (0),(1),(2)")
        rv = self.c.execute("SELECT DISTINCT n FROM t ORDER BY n LIMIT 3")
        assert len(rv) == 3


class TestOrderByNullHandling:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE t (v INT)")
        self.c.execute("INSERT INTO t VALUES (NULL),(2),(1),(NULL),(3)")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_order_by_nulls_first_last(self):
        asc = self.c.execute("SELECT v FROM t ORDER BY v NULLS FIRST").to_dict()
        desc = self.c.execute("SELECT v FROM t ORDER BY v DESC NULLS LAST").to_dict()
        assert [r["v"] for r in asc] == [None, None, 1, 2, 3]
        assert [r["v"] for r in desc] == [3, 2, 1, None, None]


# ============================================================
# INSERT ... SELECT
# ============================================================

class TestInsertSelect:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE src (name STRING, age INT)")
        self.c.execute("INSERT INTO src VALUES ('Alice',25),('Bob',30),('Carol',22)")
        self.c.execute("CREATE TABLE dst (name STRING, age INT)")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_insert_select_all(self):
        self.c.execute("INSERT INTO dst SELECT name, age FROM src")
        assert self.c.execute("SELECT COUNT(*) as c FROM dst").scalar() == 3

    def test_insert_select_with_where(self):
        self.c.execute("INSERT INTO dst SELECT name, age FROM src WHERE age >= 25")
        assert self.c.execute("SELECT COUNT(*) as c FROM dst").scalar() == 2

    def test_insert_select_with_transform(self):
        self.c.execute("INSERT INTO dst SELECT name, age+1 as age FROM src WHERE name='Alice'")
        row = self.c.execute("SELECT age FROM dst ORDER BY age").first()
        assert row['age'] == 26


# ============================================================
# Persistence / reopen
# ============================================================

class TestPersistenceReopen:
    def test_data_survives_close_reopen(self):
        with tempfile.TemporaryDirectory() as d:
            c1 = ApexClient(dirpath=d)
            c1.create_table('t')
            c1.store([{'x': i} for i in range(50)])
            c1.flush()
            c1.close()
            c2 = ApexClient(dirpath=d)
            c2.use_table('t')
            assert c2.count_rows() == 50
            c2.close()

    def test_schema_survives_reopen(self):
        with tempfile.TemporaryDirectory() as d:
            c1 = ApexClient(dirpath=d)
            c1.create_table('t', schema={'name': 'string', 'age': 'int64'})
            c1.store([{'name': 'Alice', 'age': 25}])
            c1.flush()
            c1.close()
            c2 = ApexClient(dirpath=d)
            c2.use_table('t')
            assert 'int' in c2.get_column_dtype('age').lower()
            c2.close()

    def test_delete_then_reopen_correct_count(self):
        with tempfile.TemporaryDirectory() as d:
            c1 = ApexClient(dirpath=d)
            c1.create_table('t')
            c1.store([{'x': i} for i in range(10)])
            c1.execute("DELETE FROM t WHERE x > 4")
            c1.flush()
            c1.close()
            c2 = ApexClient(dirpath=d)
            c2.use_table('t')
            assert c2.count_rows() == 5
            c2.close()

    def test_update_then_reopen_correct_values(self):
        with tempfile.TemporaryDirectory() as d:
            c1 = ApexClient(dirpath=d)
            c1.create_table('t')
            c1.store([{'x': 1, 'y': 'old'}])
            c1.execute("UPDATE t SET y='new' WHERE x=1")
            c1.flush()
            c1.close()
            c2 = ApexClient(dirpath=d)
            c2.use_table('t')
            assert c2.execute("SELECT y FROM t WHERE x=1").first()['y'] == 'new'
            c2.close()


# ============================================================
# Error handling
# ============================================================

class TestErrorHandling:
    def test_execute_after_close_raises(self):
        with tempfile.TemporaryDirectory() as d:
            c = ApexClient(dirpath=d)
            c.create_table('t')
            c.close()
            with pytest.raises(RuntimeError):
                c.execute("SELECT 1")

    def test_store_without_table_raises(self):
        with tempfile.TemporaryDirectory() as d:
            c = ApexClient(dirpath=d)
            with pytest.raises(RuntimeError):
                c.store({'x': 1})
            c.close()

    def test_delete_no_args_raises(self):
        with tempfile.TemporaryDirectory() as d:
            c = ApexClient(dirpath=d)
            c.create_table('t')
            c.store([{'x': 1}])
            with pytest.raises(ValueError):
                c.delete()
            c.close()

    def test_bad_sql_raises(self):
        with tempfile.TemporaryDirectory() as d:
            c = ApexClient(dirpath=d)
            c.create_table('t')
            with pytest.raises(Exception):
                c.execute("THIS IS NOT SQL AT ALL %%%")
            c.close()

    def test_select_nonexistent_table_raises(self):
        with tempfile.TemporaryDirectory() as d:
            c = ApexClient(dirpath=d)
            with pytest.raises(Exception):
                c.execute("SELECT * FROM no_such_table")
            c.close()

    def test_context_manager_closes_on_exception(self):
        with tempfile.TemporaryDirectory() as d:
            with pytest.raises(ValueError):
                with ApexClient(dirpath=d) as c:
                    c.create_table('t')
                    raise ValueError("intentional")
            assert c._is_closed


# ============================================================
# Compression API
# ============================================================

class TestCompression:
    def test_get_compression_returns_string(self, tmp_client):
        tmp_client.create_table('t')
        assert isinstance(tmp_client.get_compression(), str)

    def test_set_lz4_empty_table(self, tmp_client):
        tmp_client.create_table('t')
        assert isinstance(tmp_client.set_compression('lz4'), bool)

    def test_set_zstd_empty_table(self, tmp_client):
        tmp_client.create_table('t')
        assert isinstance(tmp_client.set_compression('zstd'), bool)

    def test_invalid_compression_raises(self, tmp_client):
        tmp_client.create_table('t')
        with pytest.raises(Exception):
            tmp_client.set_compression('invalid_algo')

    def test_lz4_data_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            c = ApexClient(dirpath=d)
            c.create_table('t')
            c.set_compression('lz4')
            c.store([{'x': i, 'y': f's{i}'} for i in range(100)])
            c.flush()
            assert c.count_rows() == 100
            assert c.execute("SELECT SUM(x) as s FROM t").scalar() == sum(range(100))
            c.close()

    def test_compression_noop_on_nonempty_table(self):
        with tempfile.TemporaryDirectory() as d:
            c = ApexClient(dirpath=d)
            c.create_table('t')
            c.store([{'x': 1}])
            result = c.set_compression('lz4')
            assert result is False
            c.close()


# ============================================================
# DISTINCT edge cases
# ============================================================

class TestDistinct:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE t (a STRING, b INT)")
        self.c.execute("""INSERT INTO t VALUES
            ('x',1),('x',1),('x',2),('y',1),('y',1),('z',3)""")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_distinct_single_col(self):
        rv = self.c.execute("SELECT DISTINCT a FROM t ORDER BY a")
        assert [r['a'] for r in rv] == ['x', 'y', 'z']

    def test_distinct_two_cols(self):
        rv = self.c.execute("SELECT DISTINCT a, b FROM t ORDER BY a, b")
        rows = [(r['a'], r['b']) for r in rv]
        assert ('x', 1) in rows and ('x', 2) in rows
        assert len([r for r in rows if r[0] == 'x']) == 2

    def test_count_distinct(self):
        assert self.c.execute("SELECT COUNT(DISTINCT a) as c FROM t").scalar() == 3

    def test_count_distinct_with_filter(self):
        assert self.c.execute("SELECT COUNT(DISTINCT b) as c FROM t WHERE a='x'").scalar() == 2


# ============================================================
# ORDER BY edge cases
# ============================================================

class TestOrderBy:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE t (a STRING, b INT, c FLOAT)")
        self.c.execute("""INSERT INTO t VALUES
            ('z',3,1.1),('a',1,3.3),('m',2,2.2),('a',5,0.5),('m',NULL,1.5)""")

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_order_by_string_asc(self):
        rv = self.c.execute("SELECT a FROM t ORDER BY a ASC")
        vals = [r['a'] for r in rv]
        assert vals[0] == 'a' and vals[-1] == 'z'

    def test_order_by_string_desc(self):
        rv = self.c.execute("SELECT a FROM t ORDER BY a DESC")
        assert rv.to_dict()[0]['a'] == 'z'

    def test_order_by_two_cols(self):
        rv = self.c.execute("SELECT a, b FROM t ORDER BY a ASC, b DESC")
        rows = [(r['a'], r['b']) for r in rv]
        a_rows = [r for r in rows if r[0] == 'a']
        assert a_rows[0][1] == 5

    def test_order_by_null_last_asc(self):
        rv = self.c.execute("SELECT b FROM t ORDER BY b ASC")
        vals = [r['b'] for r in rv]
        # NULL should sort as NULL; check non-null values are in ascending order
        non_null = [v for v in vals if v is not None]
        assert non_null == sorted(non_null)

    def test_order_by_float(self):
        rv = self.c.execute("SELECT c FROM t ORDER BY c ASC")
        vals = [r['c'] for r in rv]
        non_null = [v for v in vals if v is not None]
        assert non_null == sorted(non_null)
