"""
OLAP Test Suite for ApexBase

Tests analytical workloads:
- Full table scan
- COUNT(*) / SUM / AVG / MIN / MAX aggregations
- GROUP BY single column
- GROUP BY multiple columns
- GROUP BY with HAVING
- ORDER BY ASC/DESC with LIMIT
- WHERE with BETWEEN
- WHERE with IN list
- WHERE with LIKE pattern
- WHERE with AND/OR conditions
- Column projection
- COUNT(DISTINCT)
- Boolean filter
- Empty result handling
- Query to pandas/polars
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

try:
    from apexbase import ApexClient
except ImportError as e:
    pytest.skip(f"ApexBase not available: {e}", allow_module_level=True)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


@pytest.fixture
def olap_client():
    """Create a client with 5000 rows of OLAP-style data."""
    tmp = tempfile.mkdtemp()
    client = ApexClient(os.path.join(tmp, "olap_test"))
    client.create_table("default")
    cities = ["Beijing", "Shanghai", "Shenzhen", "Guangzhou", "Hangzhou",
              "Chengdu", "Wuhan", "Nanjing", "Tianjin", "Xian"]
    depts = ["Engineering", "Sales", "Marketing", "HR", "Finance"]
    rows = []
    for i in range(5000):
        rows.append({
            "emp_id": i + 1,
            "age": 22 + (i % 40),
            "years": i % 20,
            "salary": 50000.0 + (i % 100) * 1000.0,
            "city": cities[i % 10],
            "dept": depts[i % 5],
            "is_manager": i % 10 == 0,
        })
    client.store(rows)
    yield client
    shutil.rmtree(tmp, ignore_errors=True)


class TestOlapFullScan:
    def test_select_star(self, olap_client):
        result = olap_client.execute("SELECT * FROM default")
        assert len(result) == 5000

    def test_schema_columns(self, olap_client):
        result = olap_client.execute("SELECT * FROM default LIMIT 1")
        assert len(result) == 1
        row = result[0]
        assert "emp_id" in row
        assert "salary" in row
        assert "city" in row
        assert "dept" in row

    def test_row_count(self, olap_client):
        result = olap_client.execute("SELECT COUNT(*) as cnt FROM default")
        assert result[0]["cnt"] == 5000


class TestOlapAggregation:
    def test_count_star(self, olap_client):
        result = olap_client.execute("SELECT COUNT(*) as cnt FROM default")
        assert result[0]["cnt"] == 5000

    def test_sum(self, olap_client):
        result = olap_client.execute("SELECT SUM(salary) as total FROM default")
        # salary = 50000 + (i%100)*1000; 50 cycles of 100
        # Each cycle sum = 100*50000 + (0+1+...+99)*1000 = 5000000+4950000 = 9950000
        expected = 50 * 9950000.0
        assert abs(result[0]["total"] - expected) < 1.0

    def test_avg(self, olap_client):
        result = olap_client.execute("SELECT AVG(salary) as avg_sal FROM default")
        assert abs(result[0]["avg_sal"] - 99500.0) < 1.0

    def test_min(self, olap_client):
        result = olap_client.execute("SELECT MIN(salary) as min_sal FROM default")
        assert abs(result[0]["min_sal"] - 50000.0) < 1.0

    def test_max(self, olap_client):
        result = olap_client.execute("SELECT MAX(salary) as max_sal FROM default")
        assert abs(result[0]["max_sal"] - 149000.0) < 1.0

    def test_count_distinct(self, olap_client):
        result = olap_client.execute("SELECT COUNT(DISTINCT city) as n FROM default")
        assert result[0]["n"] == 10

    def test_count_distinct_dept(self, olap_client):
        result = olap_client.execute("SELECT COUNT(DISTINCT dept) as n FROM default")
        assert result[0]["n"] == 5


class TestOlapGroupBy:
    def test_group_by_single_col(self, olap_client):
        result = olap_client.execute(
            "SELECT dept, COUNT(*) as cnt FROM default GROUP BY dept"
        )
        assert len(result) == 5
        for row in result:
            assert row["cnt"] == 1000  # 5000 / 5 depts

    def test_group_by_with_avg(self, olap_client):
        result = olap_client.execute(
            "SELECT city, AVG(salary) as avg_sal FROM default GROUP BY city"
        )
        assert len(result) == 10
        for row in result:
            assert row["avg_sal"] > 0

    def test_group_by_two_cols(self, olap_client):
        result = olap_client.execute(
            "SELECT city, dept, COUNT(*) as cnt FROM default GROUP BY city, dept"
        )
        # 10 cities * 5 depts — not all combos may be uniform but should exist
        total = sum(r["cnt"] for r in result)
        assert total == 5000

    def test_group_by_with_sum(self, olap_client):
        result = olap_client.execute(
            "SELECT dept, SUM(salary) as total FROM default GROUP BY dept ORDER BY total DESC"
        )
        assert len(result) == 5
        # Verify descending order
        totals = [r["total"] for r in result]
        for i in range(1, len(totals)):
            assert totals[i - 1] >= totals[i]


class TestOlapHaving:
    def test_having_avg(self, olap_client):
        result = olap_client.execute(
            "SELECT city, AVG(salary) as avg_sal FROM default GROUP BY city "
            "HAVING AVG(salary) > 99000"
        )
        assert len(result) > 0
        for row in result:
            assert row["avg_sal"] > 99000

    def test_having_count(self, olap_client):
        result = olap_client.execute(
            "SELECT dept, COUNT(*) as cnt FROM default GROUP BY dept HAVING COUNT(*) >= 1000"
        )
        assert len(result) == 5  # all depts have 1000


class TestOlapOrderBy:
    def test_order_by_desc_limit(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default ORDER BY salary DESC LIMIT 10"
        )
        assert len(result) == 10
        salaries = [r["salary"] for r in result]
        for i in range(1, len(salaries)):
            assert salaries[i - 1] >= salaries[i]
        assert abs(salaries[0] - 149000.0) < 1.0

    def test_order_by_asc_limit(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default ORDER BY age ASC LIMIT 5"
        )
        assert len(result) == 5
        for row in result:
            assert row["age"] == 22  # min age

    def test_order_by_length_with_tie(self):
        """ORDER BY LENGTH(col) DESC, <tie> must match the Python reference
        (ties broken by the numeric column, not by row order)."""
        tmp = tempfile.mkdtemp()
        client = ApexClient(os.path.join(tmp, "len_order"))
        client.create_table("default")
        n = 20000
        names = [f"user_{i}" for i in range(n)]
        ages = [18 + (i * 7) % 63 for i in range(n)]
        client.store({"name": names, "age": ages})
        client.flush()
        try:
            result = client.execute(
                "SELECT name FROM default ORDER BY LENGTH(name) DESC, age LIMIT 100"
            )
            got = [r["name"] for r in result]
            expected = sorted(
                range(n), key=lambda i: (-len(names[i]), ages[i])
            )[:100]
            assert got == [names[i] for i in expected]
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestOlapWhereFilter:
    def test_between(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default WHERE age BETWEEN 30 AND 35 LIMIT 100"
        )
        assert 0 < len(result) <= 100
        for row in result:
            assert 30 <= row["age"] <= 35

    def test_in_list(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default WHERE city IN ('Beijing', 'Shanghai')"
        )
        assert len(result) == 1000  # 500 each
        for row in result:
            assert row["city"] in ("Beijing", "Shanghai")

    def test_like_pattern(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default WHERE city LIKE 'Sh%'"
        )
        assert len(result) == 1000  # Shanghai + Shenzhen
        for row in result:
            assert row["city"].startswith("Sh")

    def test_string_eq(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default WHERE city = 'Beijing'"
        )
        assert len(result) == 500

    def test_string_eq_with_limit(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default WHERE city = 'Beijing' LIMIT 10"
        )
        assert len(result) == 10
        for row in result:
            assert row["city"] == "Beijing"

    def test_boolean_filter_true(self, olap_client):
        result = olap_client.execute(
            "SELECT COUNT(*) as cnt FROM default WHERE is_manager = true"
        )
        assert result[0]["cnt"] == 500  # every 10th

    def test_boolean_filter_false(self, olap_client):
        result = olap_client.execute(
            "SELECT COUNT(*) as cnt FROM default WHERE is_manager = false"
        )
        assert result[0]["cnt"] == 4500


class TestOlapDictScalarCount:
    def test_count_function_of_dict_column(self):
        """COUNT(*) with a scalar function predicate over a dict column is
        evaluated per distinct value and counted at the storage layer."""
        tmp = tempfile.mkdtemp()
        client = ApexClient(os.path.join(tmp, "dict_scalar"))
        client.create_table("default")
        n = 20000
        categories = [
            ["Electronics", "Clothing", "Food", "Sports", "Books"][i % 5]
            for i in range(n)
        ]
        client.store({"category": categories})
        client.flush()  # mmap-only so the dict fast path is exercised
        try:
            r = client.execute(
                "SELECT COUNT(*) AS c FROM default "
                "WHERE COALESCE(NULLIF(category, 'Books'), 'none') = 'Books'"
            )
            assert r[0]["c"] == 0  # provably false predicate
            r = client.execute(
                "SELECT COUNT(*) AS c FROM default WHERE UPPER(category) = 'BOOKS'"
            )
            assert r[0]["c"] == sum(1 for c in categories if c.upper() == "BOOKS")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestOlapNotFilter:
    def test_not_between_and_not_like_count(self):
        """Fused mmap count path: NOT BETWEEN AND NOT LIKE must match the
        Python reference exactly (nulls absent here)."""
        tmp = tempfile.mkdtemp()
        client = ApexClient(os.path.join(tmp, "not_filter"))
        client.create_table("default")
        n = 20000
        names = [f"user_{i}" for i in range(n)]
        ages = [18 + (i % 63) for i in range(n)]  # 18..80
        client.store({"name": names, "age": ages})
        client.flush()  # mmap-only so the fused storage count path is exercised
        try:
            result = client.execute(
                "SELECT COUNT(*) AS c FROM default "
                "WHERE age NOT BETWEEN 20 AND 40 AND name NOT LIKE 'user_5%'"
            )
            expected = sum(
                1
                for i in range(n)
                if not (20 <= ages[i] <= 40) and not names[i].startswith("user_5")
            )
            assert result[0]["c"] == expected
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestOlapUnionAllTopk:
    """UNION ALL … ORDER BY <dict_col> LIMIT k fast path: per-value counts are
    gathered in one storage pass and the top-k rows are built directly."""

    def _ref(self, n, cities):
        left = [cities[i % 10] for i in range(n) if 22 + (i % 40) == 25]
        right = [cities[i % 10] for i in range(n) if 22 + (i % 40) == 26]
        return sorted(left + right)

    def _client(self):
        tmp = tempfile.mkdtemp()
        client = ApexClient(os.path.join(tmp, "union_topk"))
        client.create_table("default")
        cities = ["Beijing", "Shanghai", "Shenzhen", "Guangzhou", "Hangzhou",
                  "Chengdu", "Wuhan", "Nanjing", "Tianjin", "Xian"]
        n = 20000
        rows = []
        for i in range(n):
            rows.append({
                "emp_id": i + 1,
                "age": 22 + (i % 40),
                "city": cities[i % 10],
            })
        client.store(rows)
        client.flush()  # mmap-only so the fused storage count path is exercised
        return tmp, client, n, cities

    def test_union_all_order_limit_asc(self):
        tmp, client, n, cities = self._client()
        try:
            result = client.execute(
                "SELECT city FROM default WHERE age = 25 "
                "UNION ALL SELECT city FROM default WHERE age = 26 "
                "ORDER BY city LIMIT 100"
            )
            got = [r["city"] for r in result]
            assert got == self._ref(n, cities)[:100]
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_union_all_order_limit_desc(self):
        tmp, client, n, cities = self._client()
        try:
            result = client.execute(
                "SELECT city FROM default WHERE age = 25 "
                "UNION ALL SELECT city FROM default WHERE age = 26 "
                "ORDER BY city DESC LIMIT 100"
            )
            got = [r["city"] for r in result]
            assert got == self._ref(n, cities)[::-1][:100]
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_union_all_order_limit_offset(self):
        tmp, client, n, cities = self._client()
        try:
            result = client.execute(
                "SELECT city FROM default WHERE age = 25 "
                "UNION ALL SELECT city FROM default WHERE age = 26 "
                "ORDER BY city LIMIT 50 OFFSET 25"
            )
            got = [r["city"] for r in result]
            assert got == self._ref(n, cities)[25:75]
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_union_distinct_order(self):
        tmp, client, n, cities = self._client()
        try:
            result = client.execute(
                "SELECT city FROM default WHERE age = 25 "
                "UNION SELECT city FROM default WHERE age = 26 ORDER BY city"
            )
            got = [r["city"] for r in result]
            assert got == sorted(set(self._ref(n, cities)))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_two_side_counts_preserve_set_operation_semantics(self):
        tmp, client, n, cities = self._client()
        try:
            duplicated = client.execute(
                "SELECT city FROM default WHERE age = 25 "
                "UNION ALL SELECT city FROM default WHERE age = 25 "
                "ORDER BY city LIMIT 20"
            )
            left = [cities[i % 10] for i in range(n) if 22 + (i % 40) == 25]
            assert [row["city"] for row in duplicated] == sorted(left + left)[:20]

            union = client.execute(
                "SELECT city FROM default WHERE age BETWEEN 25 AND 26 "
                "UNION SELECT city FROM default WHERE age BETWEEN 26 AND 27 "
                "ORDER BY city"
            )
            assert [row["city"] for row in union] == sorted({cities[3], cities[4], cities[5]})

            intersect = client.execute(
                "SELECT city FROM default WHERE age BETWEEN 25 AND 26 "
                "INTERSECT SELECT city FROM default WHERE age BETWEEN 26 AND 27 "
                "ORDER BY city"
            )
            assert [row["city"] for row in intersect] == [cities[4]]

            except_result = client.execute(
                "SELECT city FROM default WHERE age BETWEEN 25 AND 26 "
                "EXCEPT SELECT city FROM default WHERE age BETWEEN 26 AND 27 "
                "ORDER BY city"
            )
            assert [row["city"] for row in except_result] == [cities[3]]
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestOlapMultiCondition:
    def test_and_condition(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default WHERE city = 'Beijing' AND age > 50"
        )
        assert len(result) > 0
        for row in result:
            assert row["city"] == "Beijing"
            assert row["age"] > 50

    def test_or_condition(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default WHERE age < 23 OR age > 60"
        )
        assert len(result) > 0
        for row in result:
            assert row["age"] < 23 or row["age"] > 60


class TestOlapProjection:
    def test_select_specific_columns(self, olap_client):
        result = olap_client.execute(
            "SELECT emp_id, salary FROM default LIMIT 10"
        )
        assert len(result) == 10
        for row in result:
            assert "emp_id" in row
            assert "salary" in row
            # Other columns may or may not be present depending on impl

    @pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not available")
    def test_to_pandas(self, olap_client):
        result = olap_client.execute("SELECT * FROM default")
        df = result.to_pandas()
        assert len(df) == 5000
        assert "salary" in df.columns
        assert "city" in df.columns

    @pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not available")
    def test_to_pandas_with_filter(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default WHERE city = 'Shanghai'"
        )
        df = result.to_pandas()
        assert len(df) == 500
        assert all(df["city"] == "Shanghai")


class TestOlapComplexQuery:
    def test_filter_group_order(self, olap_client):
        result = olap_client.execute(
            "SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal FROM default "
            "WHERE city = 'Beijing' GROUP BY dept ORDER BY avg_sal DESC"
        )
        assert len(result) >= 1
        total = sum(r["cnt"] for r in result)
        assert total == 500  # Beijing has 500 rows

    def test_group_by_order_by_limit(self, olap_client):
        result = olap_client.execute(
            "SELECT dept, SUM(salary) as total FROM default "
            "GROUP BY dept ORDER BY total DESC"
        )
        assert len(result) == 5
        totals = [r["total"] for r in result]
        for i in range(1, len(totals)):
            assert totals[i - 1] >= totals[i]


class TestOlapEdgeCases:
    def test_empty_result(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default WHERE city = 'NonExistent'"
        )
        assert len(result) == 0

    def test_count_empty_result(self, olap_client):
        result = olap_client.execute(
            "SELECT COUNT(*) as cnt FROM default WHERE city = 'NonExistent'"
        )
        assert result[0]["cnt"] == 0

    def test_limit_larger_than_data(self, olap_client):
        result = olap_client.execute(
            "SELECT * FROM default LIMIT 99999"
        )
        assert len(result) == 5000

    def test_retrieve_all(self, olap_client):
        all_rows = olap_client.retrieve_all()
        assert len(all_rows) == 5000
