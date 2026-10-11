"""
Comprehensive test suite for newly implemented ApexBase features:
1. LZ4 Compression (per-RG transparent compression/decompression)
2. CHECK Constraints (expression-based row validation)
3. FOREIGN KEY Constraints (referential integrity with CASCADE/RESTRICT/SET NULL)
4. Recursive CTE (WITH RECURSIVE iterative fixpoint)
"""

import pytest
import tempfile
import shutil
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

try:
    from apexbase import ApexClient
except ImportError as e:
    pytest.skip(f"ApexBase not available: {e}", allow_module_level=True)


@pytest.fixture
def db(tmp_path):
    """Create a fresh ApexClient for each test."""
    client = ApexClient(str(tmp_path / "test.apex"), drop_if_exists=True)
    yield client
    client.close()


# =============================================================================
# 1. LZ4 Compression Tests
# =============================================================================

class TestLZ4Compression:
    """Test that LZ4 compression is transparently applied and data roundtrips correctly."""

    def test_basic_roundtrip_after_save(self, db):
        """Insert enough data to trigger LZ4 compression, save, reopen, verify."""
        db.execute("CREATE TABLE t (id INTEGER, name TEXT, value REAL)")
        # Insert enough rows so RG body exceeds LZ4_MIN_BODY_SIZE (512 bytes)
        rows = ", ".join(
            [f"({i}, 'name_{i:04d}', {i * 1.5})" for i in range(200)]
        )
        db.execute(f"INSERT INTO t (id, name, value) VALUES {rows}")
        df = db.execute("SELECT id, name, value FROM t ORDER BY id").to_pandas()
        assert len(df) == 200
        assert df.iloc[0]["id"] == 0
        assert df.iloc[0]["name"] == "name_0000"
        assert abs(df.iloc[199]["value"] - 199 * 1.5) < 0.01

    def test_compressed_data_survives_reopen(self, tmp_path):
        """Data written with LZ4 compression is readable after close+reopen."""
        path = str(tmp_path / "compress.apex")
        db1 = ApexClient(path, drop_if_exists=True)
        db1.execute("CREATE TABLE t (id INTEGER, payload TEXT)")
        rows = ", ".join([f"({i}, '{'x' * 100}')" for i in range(100)])
        db1.execute(f"INSERT INTO t (id, payload) VALUES {rows}")
        db1.close()

        db2 = ApexClient(path)
        db2.use_table("t")
        df = db2.execute("SELECT COUNT(*) as cnt FROM t").to_pandas()
        assert df.iloc[0]["cnt"] == 100
        df2 = db2.execute("SELECT id, payload FROM t WHERE id = 50").to_pandas()
        assert len(df2) == 1
        assert df2.iloc[0]["id"] == 50
        assert df2.iloc[0]["payload"] == "x" * 100
        db2.close()

    def test_small_data_no_compression(self, db):
        """Small RG bodies (< 512 bytes) should NOT be compressed but still work."""
        db.execute("CREATE TABLE small_tbl (a INTEGER)")
        db.execute("INSERT INTO small_tbl (a) VALUES (1), (2), (3)")
        df = db.execute("SELECT a FROM small_tbl ORDER BY a").to_pandas()
        assert list(df["a"]) == [1, 2, 3]

    def test_mixed_types_compressed(self, db):
        """Compression works with all column types: int, float, string, bool."""
        db.execute("CREATE TABLE mixed_tbl (i INTEGER, f REAL, s TEXT, b BOOLEAN)")
        rows = ", ".join(
            [f"({i}, {i * 0.1}, 'str_{i}', {'TRUE' if i % 2 == 0 else 'FALSE'})"
             for i in range(200)]
        )
        db.execute(f"INSERT INTO mixed_tbl (i, f, s, b) VALUES {rows}")
        df = db.execute("SELECT * FROM mixed_tbl ORDER BY i").to_pandas()
        assert len(df) == 200
        assert df.iloc[100]["i"] == 100
        assert abs(df.iloc[100]["f"] - 10.0) < 0.01
        assert df.iloc[100]["s"] == "str_100"
        assert df.iloc[100]["b"] == True

    def test_null_values_survive_compression(self, db):
        """NULL values are preserved correctly through LZ4 compression roundtrip."""
        db.execute("CREATE TABLE null_test (id INTEGER, val INTEGER)")
        db.execute("INSERT INTO null_test (id, val) VALUES (1, NULL), (2, 10), (3, NULL), (4, 20)")
        # Insert more rows to trigger compression
        rows = ", ".join([f"({i}, {i})" for i in range(5, 205)])
        db.execute(f"INSERT INTO null_test (id, val) VALUES {rows}")
        df = db.execute("SELECT id, val FROM null_test WHERE id <= 4 ORDER BY id").to_pandas()
        assert len(df) == 4
        import pandas as pd
        assert pd.isna(df.iloc[0]["val"])
        assert df.iloc[1]["val"] == 10
        assert pd.isna(df.iloc[2]["val"])
        assert df.iloc[3]["val"] == 20

    def test_compressed_file_smaller(self, tmp_path):
        """Compressed file should be smaller than uncompressed data size."""
        path = str(tmp_path / "size_test.apex")
        db = ApexClient(path, drop_if_exists=True)
        db.execute("CREATE TABLE t (id INTEGER, data TEXT)")
        # Insert highly compressible data (repeated strings)
        rows = ", ".join([f"({i}, '{'AAAA' * 50}')" for i in range(500)])
        db.execute(f"INSERT INTO t (id, data) VALUES {rows}")
        db.close()

        # Table files are stored in the same directory as the database path
        table_file = tmp_path / "t.apex"
        if not table_file.exists():
            # Some configurations store beside the db file
            for f in tmp_path.rglob("t.apex"):
                table_file = f
                break
        assert table_file.exists(), f"Table file not found in {tmp_path}"
        file_size = os.path.getsize(str(table_file))
        # Raw data would be ~500 * (8 + 200) bytes = ~104KB
        # LZ4 should compress the repeated strings significantly
        assert file_size > 0
        assert file_size < 500 * 250  # should be much less than raw size

    def test_delete_after_compression(self, db):
        """DELETE works correctly on compressed data."""
        db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        rows = ", ".join([f"({i}, 'name_{i}')" for i in range(200)])
        db.execute(f"INSERT INTO t (id, name) VALUES {rows}")
        db.execute("DELETE FROM t WHERE id >= 100")
        df = db.execute("SELECT COUNT(*) as cnt FROM t").to_pandas()
        assert df.iloc[0]["cnt"] == 100

    def test_update_after_compression(self, db):
        """UPDATE works correctly on compressed data."""
        db.execute("CREATE TABLE t (id INTEGER, val INTEGER)")
        rows = ", ".join([f"({i}, {i})" for i in range(200)])
        db.execute(f"INSERT INTO t (id, val) VALUES {rows}")
        db.execute("UPDATE t SET val = 999 WHERE id = 50")
        df = db.execute("SELECT val FROM t WHERE id = 50").to_pandas()
        assert df.iloc[0]["val"] == 999

    def test_aggregation_on_compressed_data(self, db):
        """Aggregate queries work on compressed data."""
        db.execute("CREATE TABLE t (id INTEGER, val INTEGER)")
        rows = ", ".join([f"({i}, {i})" for i in range(200)])
        db.execute(f"INSERT INTO t (id, val) VALUES {rows}")
        df = db.execute("SELECT SUM(val) as s, AVG(val) as a, MIN(val) as mn, MAX(val) as mx FROM t").to_pandas()
        assert df.iloc[0]["s"] == sum(range(200))
        assert abs(df.iloc[0]["a"] - 99.5) < 0.01
        assert df.iloc[0]["mn"] == 0
        assert df.iloc[0]["mx"] == 199


# =============================================================================
# 2. CHECK Constraint Tests
# =============================================================================

class TestCheckConstraints:
    """Test CHECK constraint enforcement on INSERT and UPDATE."""

    def test_check_constraint_basic_insert_pass(self, db):
        """INSERT that satisfies CHECK constraint succeeds."""
        db.execute("CREATE TABLE t (age INTEGER CHECK(age > 0))")
        db.execute("INSERT INTO t (age) VALUES (25)")
        df = db.execute("SELECT age FROM t").to_pandas()
        assert df.iloc[0]["age"] == 25

    def test_check_constraint_basic_insert_fail(self, db):
        """INSERT that violates CHECK constraint raises error."""
        db.execute("CREATE TABLE t (age INTEGER CHECK(age > 0))")
        with pytest.raises(Exception, match="CHECK constraint"):
            db.execute("INSERT INTO t (age) VALUES (0)")

    def test_check_constraint_negative_value(self, db):
        """Negative value violating CHECK is rejected."""
        db.execute("CREATE TABLE t (age INTEGER CHECK(age > 0))")
        with pytest.raises(Exception, match="CHECK constraint"):
            db.execute("INSERT INTO t (age) VALUES (-5)")

    def test_check_constraint_boundary_value(self, db):
        """Boundary value exactly at CHECK threshold."""
        db.execute("CREATE TABLE t (score INTEGER CHECK(score >= 0))")
        db.execute("INSERT INTO t (score) VALUES (0)")
        df = db.execute("SELECT score FROM t").to_pandas()
        assert df.iloc[0]["score"] == 0

    def test_check_constraint_multiple_conditions(self, db):
        """CHECK with AND condition."""
        db.execute("CREATE TABLE t (age INTEGER CHECK(age > 0 AND age < 150))")
        db.execute("INSERT INTO t (age) VALUES (25)")
        with pytest.raises(Exception, match="CHECK constraint"):
            db.execute("INSERT INTO t (age) VALUES (200)")

    def test_check_constraint_on_update(self, db):
        """UPDATE that violates CHECK is rejected."""
        db.execute("CREATE TABLE t (age INTEGER CHECK(age > 10))")
        db.execute("INSERT INTO t (age) VALUES (25)")
        with pytest.raises(Exception, match="CHECK constraint"):
            db.execute("UPDATE t SET age = 5 WHERE age = 25")

    def test_check_constraint_update_pass(self, db):
        """UPDATE that satisfies CHECK succeeds."""
        db.execute("CREATE TABLE t (val INTEGER CHECK(val >= 0))")
        db.execute("INSERT INTO t (val) VALUES (10)")
        db.execute("UPDATE t SET val = 20 WHERE val = 10")
        df = db.execute("SELECT val FROM t").to_pandas()
        assert df.iloc[0]["val"] == 20

    def test_check_constraint_on_update_zero_violates(self, db):
        """UPDATE to zero violates CHECK(val > 0)."""
        db.execute("CREATE TABLE t (val INTEGER CHECK(val > 0))")
        db.execute("INSERT INTO t (val) VALUES (10)")
        with pytest.raises(Exception, match="CHECK constraint"):
            db.execute("UPDATE t SET val = 0 WHERE val = 10")

    def test_check_constraint_multiple_rows(self, db):
        """Batch INSERT where one row violates CHECK."""
        db.execute("CREATE TABLE t (val INTEGER CHECK(val > 0))")
        with pytest.raises(Exception, match="CHECK constraint"):
            db.execute("INSERT INTO t (val) VALUES (5), (10), (0)")

    def test_check_constraint_with_other_constraints(self, db):
        """CHECK combined with NOT NULL."""
        db.execute("CREATE TABLE t (val INTEGER NOT NULL CHECK(val > 0))")
        db.execute("INSERT INTO t (val) VALUES (1)")
        with pytest.raises(Exception):
            db.execute("INSERT INTO t (val) VALUES (NULL)")
        with pytest.raises(Exception, match="CHECK constraint"):
            db.execute("INSERT INTO t (val) VALUES (-1)")

    def test_check_constraint_persisted(self, tmp_path):
        """CHECK constraint survives close and reopen."""
        path = str(tmp_path / "check.apex")
        db1 = ApexClient(path, drop_if_exists=True)
        db1.execute("CREATE TABLE t (val INTEGER CHECK(val > 0))")
        db1.execute("INSERT INTO t (val) VALUES (5)")
        db1.close()

        db2 = ApexClient(path)
        db2.use_table("t")
        with pytest.raises(Exception, match="CHECK constraint"):
            db2.execute("INSERT INTO t (val) VALUES (-1)")
        db2.close()

    def test_check_constraint_float_comparison(self, db):
        """CHECK on REAL (float) column."""
        db.execute("CREATE TABLE t (price REAL CHECK(price >= 0.0))")
        db.execute("INSERT INTO t (price) VALUES (9.99)")
        with pytest.raises(Exception, match="CHECK constraint"):
            db.execute("INSERT INTO t (price) VALUES (-0.01)")


# =============================================================================
# 3. FOREIGN KEY Constraint Tests
# =============================================================================

class TestForeignKeyConstraints:
    """Test FOREIGN KEY referential integrity enforcement."""

    def test_fk_basic_insert_pass(self, db):
        """INSERT with valid FK reference succeeds."""
        db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO departments (id, name) VALUES (1, 'Engineering'), (2, 'Sales')")
        db.execute("CREATE TABLE employees (id INTEGER, dept_id INTEGER REFERENCES departments(id))")
        db.execute("INSERT INTO employees (id, dept_id) VALUES (1, 1)")
        df = db.execute("SELECT id, dept_id FROM employees").to_pandas()
        assert df.iloc[0]["dept_id"] == 1

    def test_fk_insert_violation(self, db):
        """INSERT with invalid FK reference raises error."""
        db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO departments (id, name) VALUES (1, 'Engineering')")
        db.execute("CREATE TABLE employees (id INTEGER, dept_id INTEGER REFERENCES departments(id))")
        with pytest.raises(Exception, match="FOREIGN KEY"):
            db.execute("INSERT INTO employees (id, dept_id) VALUES (1, 999)")

    def test_fk_null_allowed(self, db):
        """NULL value in FK column is allowed (nullable FK)."""
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO parent (id) VALUES (1)")
        db.execute("CREATE TABLE child (id INTEGER, parent_id INTEGER REFERENCES parent(id))")
        db.execute("INSERT INTO child (id, parent_id) VALUES (1, NULL)")
        df = db.execute("SELECT id FROM child").to_pandas()
        assert len(df) == 1

    def test_fk_multiple_valid_references(self, db):
        """Multiple rows referencing different valid parent rows."""
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO parent (id) VALUES (1), (2), (3)")
        db.execute("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
        db.execute("INSERT INTO child (id, pid) VALUES (10, 1), (20, 2), (30, 3)")
        df = db.execute("SELECT id, pid FROM child ORDER BY id").to_pandas()
        assert len(df) == 3
        assert list(df["pid"]) == [1, 2, 3]

    def test_fk_update_violation(self, db):
        """UPDATE that sets FK to non-existent parent value raises error."""
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO parent (id) VALUES (1), (2)")
        db.execute("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
        db.execute("INSERT INTO child (id, pid) VALUES (1, 1)")
        with pytest.raises(Exception, match="FOREIGN KEY"):
            db.execute("UPDATE child SET pid = 999 WHERE id = 1")

    def test_fk_update_to_valid_value(self, db):
        """UPDATE FK to a valid parent value succeeds."""
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO parent (id) VALUES (1), (2)")
        db.execute("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
        db.execute("INSERT INTO child (id, pid) VALUES (1, 1)")
        db.execute("UPDATE child SET pid = 2 WHERE id = 1")
        df = db.execute("SELECT pid FROM child WHERE id = 1").to_pandas()
        assert df.iloc[0]["pid"] == 2

    def test_fk_string_column(self, db):
        """FK on TEXT column type."""
        db.execute("CREATE TABLE countries (code TEXT PRIMARY KEY)")
        db.execute("INSERT INTO countries (code) VALUES ('US'), ('UK'), ('JP')")
        db.execute("CREATE TABLE cities (name TEXT, country_code TEXT REFERENCES countries(code))")
        db.execute("INSERT INTO cities (name, country_code) VALUES ('NYC', 'US')")
        with pytest.raises(Exception, match="FOREIGN KEY"):
            db.execute("INSERT INTO cities (name, country_code) VALUES ('Paris', 'FR')")

    def test_fk_persisted(self, tmp_path):
        """FK constraint survives close and reopen."""
        path = str(tmp_path / "fk.apex")
        db1 = ApexClient(path, drop_if_exists=True)
        db1.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        db1.execute("INSERT INTO parent (id) VALUES (1)")
        db1.execute("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
        db1.execute("INSERT INTO child (id, pid) VALUES (1, 1)")
        db1.close()

        db2 = ApexClient(path)
        db2.use_table("child")
        with pytest.raises(Exception, match="FOREIGN KEY"):
            db2.execute("INSERT INTO child (id, pid) VALUES (2, 999)")
        db2.close()

    def test_fk_batch_insert_violation(self, db):
        """Batch INSERT where one row violates FK."""
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO parent (id) VALUES (1), (2)")
        db.execute("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
        with pytest.raises(Exception, match="FOREIGN KEY"):
            db.execute("INSERT INTO child (id, pid) VALUES (1, 1), (2, 999)")

    def test_fk_self_referencing(self, db):
        """FK referencing the same table (self-referential)."""
        db.execute("CREATE TABLE org (id INTEGER PRIMARY KEY, manager_id INTEGER REFERENCES org(id))")
        db.execute("INSERT INTO org (id, manager_id) VALUES (1, NULL)")  # root
        db.execute("INSERT INTO org (id, manager_id) VALUES (2, 1)")
        db.execute("INSERT INTO org (id, manager_id) VALUES (3, 1)")
        with pytest.raises(Exception, match="FOREIGN KEY"):
            db.execute("INSERT INTO org (id, manager_id) VALUES (4, 999)")


# =============================================================================
# 4. Recursive CTE Tests
# =============================================================================

class TestRecursiveCTE:
    """Test WITH RECURSIVE for iterative fixpoint queries."""

    def test_generate_series(self, db):
        """Generate integer series 1..10."""
        df = db.execute("""
            WITH RECURSIVE series(x) AS (
                SELECT 1
                UNION ALL
                SELECT x + 1 FROM series WHERE x < 10
            )
            SELECT x FROM series
        """).to_pandas()
        assert len(df) == 10
        assert list(df["x"]) == list(range(1, 11))

    def test_factorial(self, db):
        """Compute factorial via recursive CTE."""
        df = db.execute("""
            WITH RECURSIVE fact(n, val) AS (
                SELECT 1, 1
                UNION ALL
                SELECT n + 1, val * (n + 1) FROM fact WHERE n < 6
            )
            SELECT n, val FROM fact
        """).to_pandas()
        assert len(df) == 6
        expected = [1, 2, 6, 24, 120, 720]
        assert list(df["val"]) == expected

    def test_fibonacci(self, db):
        """Compute Fibonacci sequence via recursive CTE."""
        df = db.execute("""
            WITH RECURSIVE fib(n, a, b) AS (
                SELECT 1, 0, 1
                UNION ALL
                SELECT n + 1, b, a + b FROM fib WHERE n < 10
            )
            SELECT n, b FROM fib
        """).to_pandas()
        assert len(df) == 10
        expected_fib = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
        assert list(df["b"]) == expected_fib

    def test_org_hierarchy(self, db):
        """Traverse organizational hierarchy with recursive CTE + JOIN."""
        db.execute("CREATE TABLE employees (id INTEGER, name TEXT, manager_id INTEGER)")
        db.execute("""
            INSERT INTO employees (id, name, manager_id) VALUES
            (1, 'CEO', NULL),
            (2, 'VP_Eng', 1),
            (3, 'VP_Sales', 1),
            (4, 'Dir_Eng', 2),
            (5, 'Dev1', 4)
        """)
        df = db.execute("""
            WITH RECURSIVE chain(id, name, lvl) AS (
                SELECT id, name, 1 FROM employees WHERE manager_id IS NULL
                UNION ALL
                SELECT e.id, e.name, c.lvl + 1
                FROM employees e JOIN chain c ON e.manager_id = c.id
            )
            SELECT id, name, lvl FROM chain ORDER BY lvl, id
        """).to_pandas()
        assert len(df) == 5
        assert df.iloc[0]["name"] == "CEO"
        assert df.iloc[0]["lvl"] == 1
        assert df.iloc[4]["name"] == "Dev1"
        assert df.iloc[4]["lvl"] == 4

    def test_single_column_alias(self, db):
        """CTE with single column alias."""
        df = db.execute("""
            WITH RECURSIVE cnt(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM cnt WHERE n < 5
            )
            SELECT n FROM cnt
        """).to_pandas()
        assert len(df) == 5
        assert list(df["n"]) == [1, 2, 3, 4, 5]

    def test_non_recursive_cte_with_aliases(self, db):
        """Non-recursive CTE with column aliases."""
        db.execute("CREATE TABLE t (a INTEGER, b INTEGER)")
        db.execute("INSERT INTO t (a, b) VALUES (1, 10), (2, 20)")
        df = db.execute("""
            WITH cte(x, y) AS (
                SELECT a, b FROM t
            )
            SELECT x, y FROM cte ORDER BY x
        """).to_pandas()
        assert len(df) == 2
        assert list(df["x"]) == [1, 2]
        assert list(df["y"]) == [10, 20]

    def test_recursive_cte_empty_anchor(self, db):
        """Recursive CTE where anchor returns no rows."""
        db.execute("CREATE TABLE t (id INTEGER)")
        df = db.execute("""
            WITH RECURSIVE r(x) AS (
                SELECT id FROM t
                UNION ALL
                SELECT x + 1 FROM r WHERE x < 10
            )
            SELECT x FROM r
        """).to_pandas()
        assert len(df) == 0

    def test_powers_of_two(self, db):
        """Generate powers of 2 using recursive CTE."""
        df = db.execute("""
            WITH RECURSIVE pow2(n, val) AS (
                SELECT 0, 1
                UNION ALL
                SELECT n + 1, val * 2 FROM pow2 WHERE n < 10
            )
            SELECT n, val FROM pow2
        """).to_pandas()
        assert len(df) == 11
        for _, row in df.iterrows():
            assert row["val"] == 2 ** int(row["n"])

    def test_arithmetic_sum(self, db):
        """Sum 1..100 using recursive CTE with running total."""
        df = db.execute("""
            WITH RECURSIVE sums(n, total) AS (
                SELECT 1, 1
                UNION ALL
                SELECT n + 1, total + (n + 1) FROM sums WHERE n < 100
            )
            SELECT n, total FROM sums WHERE n = 100
        """).to_pandas()
        assert len(df) == 1
        assert df.iloc[0]["total"] == 5050

    def test_from_less_select(self, db):
        """SELECT without FROM clause works (needed for CTE anchors)."""
        df = db.execute("WITH cte(a, b) AS (SELECT 42, 99) SELECT a, b FROM cte").to_pandas()
        assert len(df) == 1
        assert df.iloc[0]["a"] == 42
        assert df.iloc[0]["b"] == 99

    def test_recursive_cte_with_string_data(self, db):
        """Recursive CTE producing string values."""
        db.execute("CREATE TABLE categories (id INTEGER, name TEXT, parent_id INTEGER)")
        db.execute("""
            INSERT INTO categories (id, name, parent_id) VALUES
            (1, 'Root', NULL),
            (2, 'Electronics', 1),
            (3, 'Phones', 2)
        """)
        df = db.execute("""
            WITH RECURSIVE cat_tree(id, name, depth) AS (
                SELECT id, name, 0 FROM categories WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, ct.depth + 1
                FROM categories c JOIN cat_tree ct ON c.parent_id = ct.id
            )
            SELECT id, name, depth FROM cat_tree ORDER BY depth, id
        """).to_pandas()
        assert len(df) == 3
        assert df.iloc[0]["name"] == "Root"
        assert df.iloc[2]["name"] == "Phones"
        assert df.iloc[2]["depth"] == 2


# =============================================================================
# 5. Cross-Feature Integration Tests
# =============================================================================

class TestCrossFeatureIntegration:
    """Test interactions between multiple new features."""

    def test_check_constraint_with_large_compressed_data(self, db):
        """CHECK constraint still enforced after LZ4 roundtrip."""
        db.execute("CREATE TABLE t (val INTEGER CHECK(val > 0))")
        rows = ", ".join([f"({i})" for i in range(1, 201)])
        db.execute(f"INSERT INTO t (val) VALUES {rows}")
        with pytest.raises(Exception, match="CHECK constraint"):
            db.execute("INSERT INTO t (val) VALUES (0)")
        df = db.execute("SELECT COUNT(*) as cnt FROM t").to_pandas()
        assert df.iloc[0]["cnt"] == 200

    def test_fk_with_large_parent_table(self, db):
        """FK validation works correctly with compressed parent table."""
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        rows = ", ".join([f"({i})" for i in range(1, 201)])
        db.execute(f"INSERT INTO parent (id) VALUES {rows}")
        db.execute("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
        db.execute("INSERT INTO child (id, pid) VALUES (1, 100)")
        with pytest.raises(Exception, match="FOREIGN KEY"):
            db.execute("INSERT INTO child (id, pid) VALUES (2, 999)")

    def test_recursive_cte_on_constrained_table(self, db):
        """Recursive CTE reading from table with CHECK + FK."""
        db.execute("CREATE TABLE levels (id INTEGER PRIMARY KEY CHECK(id > 0), name TEXT)")
        db.execute("INSERT INTO levels (id, name) VALUES (1, 'L1'), (2, 'L2'), (3, 'L3')")
        db.execute("CREATE TABLE paths (id INTEGER, level_id INTEGER REFERENCES levels(id), parent_path_id INTEGER)")
        db.execute("""
            INSERT INTO paths (id, level_id, parent_path_id) VALUES
            (1, 1, NULL), (2, 2, 1), (3, 3, 2)
        """)
        df = db.execute("""
            WITH RECURSIVE tree(id, level_id, depth) AS (
                SELECT id, level_id, 1 FROM paths WHERE parent_path_id IS NULL
                UNION ALL
                SELECT p.id, p.level_id, t.depth + 1
                FROM paths p JOIN tree t ON p.parent_path_id = t.id
            )
            SELECT id, level_id, depth FROM tree ORDER BY depth
        """).to_pandas()
        assert len(df) == 3
        assert list(df["depth"]) == [1, 2, 3]

    def test_all_constraints_on_one_table(self, db):
        """Table with NOT NULL + CHECK + UNIQUE + DEFAULT constraints."""
        db.execute("""
            CREATE TABLE product (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL CHECK(price >= 0.0),
                stock INTEGER DEFAULT 0
            )
        """)
        db.execute("INSERT INTO product (id, name, price) VALUES (1, 'Widget', 9.99)")
        df = db.execute("SELECT id, name, price, stock FROM product").to_pandas()
        assert df.iloc[0]["stock"] == 0  # default
        assert df.iloc[0]["price"] == 9.99

        # Violate CHECK
        with pytest.raises(Exception, match="CHECK constraint"):
            db.execute("INSERT INTO product (id, name, price) VALUES (2, 'Bad', -1.0)")

        # Violate NOT NULL
        with pytest.raises(Exception):
            db.execute("INSERT INTO product (id, name, price) VALUES (3, NULL, 5.0)")

        # Violate PK uniqueness
        with pytest.raises(Exception):
            db.execute("INSERT INTO product (id, name, price) VALUES (1, 'Dup', 1.0)")


class TestCompressedPointRead:
    """Point reads on a compressed row group must decode every column kind.

    A compressed row group is read by the sequential fallback, which used to
    know only Int64/Float64/String. A dictionary-encoded string, a Bool, or a
    binary column therefore read back as NULL through `retrieve()` and
    `SELECT ... WHERE _id = N`, while the scan path returned the right values.
    """

    ROWS = 5000
    DICT_VALUES = 17

    @staticmethod
    def _value(i):
        """The fixture's string for row index `i` (`None` where it is NULL)."""
        if i % 101 == 0:
            return None
        return f"value-{'pad' * (i % 3)}_{i % TestCompressedPointRead.DICT_VALUES}"

    @staticmethod
    def _flag(i):
        if i % 53 == 0:
            return None
        return i % 3 == 0

    def _fixture(self, tmp_path):
        client = ApexClient(str(tmp_path / "db"), drop_if_exists=True, enable_cache=False)
        client.create_table(
            "t", {"n": "int64", "s": "string", "b": "bool", "score": "float32"}
        )
        client.use_table("t")
        assert client.set_compression("zstd") is True
        client.store(
            {
                "n": [i % 977 for i in range(self.ROWS)],
                "s": [self._value(i) for i in range(self.ROWS)],
                "b": [self._flag(i) for i in range(self.ROWS)],
                "score": [i * 0.5 for i in range(self.ROWS)],
            }
        )
        client.flush()
        with open(tmp_path / "db" / "t.apex", "rb") as handle:
            blob = handle.read(256 + 32)
        assert blob[256 + 28] != 0, "fixture must actually compress its row group"
        return client

    def _expect(self, point_id):
        index = point_id - 1
        return {
            "n": index % 977,
            "s": self._value(index),
            "b": self._flag(index),
            "score": index * 0.5,
        }

    def test_retrieve_decodes_dict_strings_and_bools(self, tmp_path):
        client = self._fixture(tmp_path)
        try:
            # A dictionary-encoded string and a Bool must survive the
            # compressed sequential decode, not come back as NULL.
            for point_id in (2, 3, 4, 100):
                row = client.retrieve(point_id)
                expected = self._expect(point_id)
                assert row["s"] == expected["s"], point_id
                assert row["b"] == expected["b"], point_id
                assert row["n"] == expected["n"], point_id
                assert row["score"] == expected["score"], point_id
        finally:
            client.close()

    def test_id_point_lookup_decodes_the_same_row(self, tmp_path):
        client = self._fixture(tmp_path)
        try:
            for point_id in (2, 3, 4, 100):
                expected = self._expect(point_id)
                got = client.execute(
                    f"SELECT n, s, b, score FROM t WHERE _id = {point_id}"
                ).to_dict()
                assert got == [expected], point_id
        finally:
            client.close()

    def test_null_values_stay_null(self, tmp_path):
        client = self._fixture(tmp_path)
        try:
            # Every 101st string and every 53rd bool is NULL in the fixture.
            assert client.retrieve(102)["s"] is None
            assert client.retrieve(54)["b"] is None
            # The scan and the point read must agree on the same rows.
            for point_id in (102, 54):
                expected = self._expect(point_id)
                assert client.execute(
                    f"SELECT n, s, b, score FROM t WHERE _id = {point_id}"
                ).to_dict() == [expected]
        finally:
            client.close()
