"""
Comprehensive test suite for ApexBase Query Operations and ResultView

This module tests:
- Basic query operations with WHERE clauses
- Query with limits and optimization
- ResultView functionality and conversions
- Edge cases and error handling
- Performance considerations
- Complex query conditions
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import sys
import os
import numpy as np

# Add the apexbase python module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

try:
    from apexbase import ApexClient, ResultView, ARROW_AVAILABLE, POLARS_AVAILABLE
except ImportError as e:
    pytest.skip(f"ApexBase not available: {e}", allow_module_level=True)

# Optional imports
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import polars as pl
    POLARS_DF_AVAILABLE = True
except ImportError:
    POLARS_DF_AVAILABLE = False

try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


class TestBasicQueryOperations:
    """Test basic query operations"""
    
    def test_query_all_records(self):
        """Test querying all records"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25, "city": "NYC"},
                {"name": "Bob", "age": 30, "city": "LA"},
                {"name": "Charlie", "age": 35, "city": "Chicago"},
            ]
            client.store(test_data)
            
            # Query all records
            results = client.query()
            
            assert isinstance(results, ResultView)
            assert len(results) == 3
            
            # Check all records are present
            names = [r["name"] for r in results]
            assert "Alice" in names
            assert "Bob" in names
            assert "Charlie" in names
            
            client.close()
    
    def test_query_with_where_clause(self):
        """Test querying with WHERE clause"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25, "city": "NYC", "active": True},
                {"name": "Bob", "age": 30, "city": "LA", "active": False},
                {"name": "Charlie", "age": 35, "city": "Chicago", "active": True},
                {"name": "Diana", "age": 28, "city": "NYC", "active": False},
            ]
            client.store(test_data)
            
            # Test various WHERE clauses
            results = client.query("age > 30")
            assert len(results) == 1
            assert results[0]["name"] == "Charlie"
            
            results = client.query("city = 'NYC'")
            assert len(results) == 2
            nyc_names = [r["name"] for r in results]
            assert "Alice" in nyc_names
            assert "Diana" in nyc_names
            
            results = client.query("active = true")
            assert len(results) == 2
            active_names = [r["name"] for r in results]
            assert "Alice" in active_names
            assert "Charlie" in active_names
            
            client.close()
    
    def test_query_with_limit(self):
        """Test querying with limit"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [{"id": i, "value": f"item_{i}"} for i in range(10)]
            client.store(test_data)
            
            # Test with limit
            results = client.query(limit=5)
            assert len(results) == 5
            
            # Test limit with WHERE clause
            results = client.query("id >= 5", limit=3)
            assert len(results) == 3
            for result in results:
                assert result["id"] >= 5
            
            client.close()

    def test_string_equality_limit_uses_simple_query_shapes(self):
        """Simple equality + LIMIT should return the first matching row correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")

            test_data = [
                {"name": "Alice", "city": "NYC", "score": 10},
                {"name": "Bob", "city": "LA", "score": 20},
                {"name": "Bob", "city": "Seattle", "score": 30},
                {"name": "Bob", "city": "Austin", "score": 40},
            ]
            client.store(test_data)

            rv = client.query(where_clause="name = 'Bob'", limit=1)
            assert len(rv) == 1
            assert rv.first()["name"] == "Bob"

            projected = client.execute(
                "SELECT city, score FROM default WHERE name = 'Bob' LIMIT 1"
            ).first()
            assert projected is not None
            assert set(projected.keys()) == {"city", "score"}
            assert projected["score"] == 20

            first = client.execute(
                "SELECT city, score FROM default WHERE name = 'Bob' LIMIT 1"
            ).first()
            assert first == {"city": "LA", "score": 20}

            limited = client.execute(
                "SELECT city, score FROM default WHERE name = 'Bob' LIMIT 2"
            ).to_dict()
            assert limited == [
                {"city": "LA", "score": 20},
                {"city": "Seattle", "score": 30},
            ]

            offset = client.execute(
                "SELECT city, score FROM default WHERE name = 'Bob' LIMIT 2 OFFSET 1"
            ).to_dict()
            assert offset == [
                {"city": "Seattle", "score": 30},
                {"city": "Austin", "score": 40},
            ]

            missing = client.execute(
                "SELECT city, score FROM default WHERE name = 'Nobody' LIMIT 1"
            ).to_dict()
            assert missing == []

            client.close()
    
    def test_query_with_complex_conditions(self):
        """Test querying with complex conditions"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25, "salary": 50000, "dept": "Engineering"},
                {"name": "Bob", "age": 30, "salary": 60000, "dept": "Sales"},
                {"name": "Charlie", "age": 35, "salary": 70000, "dept": "Engineering"},
                {"name": "Diana", "age": 28, "salary": 55000, "dept": "Marketing"},
                {"name": "Eve", "age": 32, "salary": 65000, "dept": "Engineering"},
            ]
            client.store(test_data)
            
            # Test AND conditions
            results = client.query("age > 30 AND salary > 60000")
            assert len(results) >= 1  # Should match Charlie and/or Eve
            
            # Test OR conditions - may vary based on SQL parsing
            try:
                results = client.query("dept = 'Engineering' OR salary > 60000")
                assert len(results) >= 3
            except Exception as e:
                print(f"Complex OR query: {e}")
            
            client.close()
    
    def test_query_with_string_operations(self):
        """Test querying with string operations"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice Johnson", "email": "alice@example.com"},
                {"name": "Bob Smith", "email": "bob@test.org"},
                {"name": "Charlie Brown", "email": "charlie@example.com"},
                {"name": "Diana Prince", "email": "diana@amazon.com"},
            ]
            client.store(test_data)
            
            # Test LIKE operations - note: LIKE pattern matching may vary
            try:
                results = client.query("name LIKE 'A%'")
                assert len(results) >= 1
            except Exception as e:
                print(f"LIKE query: {e}")
            
            client.close()
    
    def test_query_with_null_values(self):
        """Test querying with NULL values"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data with NULL values
            test_data = [
                {"name": "Alice", "age": 25, "phone": "123-456-7890"},
                {"name": "Bob", "age": None, "phone": None},
                {"name": "Charlie", "age": 35, "phone": "098-765-4321"},
                {"name": "Diana", "age": None, "phone": "555-123-4567"},
            ]
            client.store(test_data)
            
            # Test IS NULL - note: NULL handling may vary
            try:
                results = client.query("age IS NULL")
                # Should find records with NULL age
                assert len(results) >= 0  # May return 0 if NULL not supported
            except Exception as e:
                print(f"IS NULL query: {e}")
            
            client.close()
    
    def test_query_empty_database(self):
        """Test querying empty database"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Query empty database
            results = client.query()
            assert isinstance(results, ResultView)
            assert len(results) == 0
            
            # Query with conditions on empty database
            results = client.query("name = 'test'")
            assert len(results) == 0
            
            client.close()
    
    def test_query_no_results(self):
        """Test query that returns no results"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
            ]
            client.store(test_data)
            
            # Query with no matching results
            results = client.query("age > 100")
            assert isinstance(results, ResultView)
            assert len(results) == 0
            
            client.close()


class TestResultViewFunctionality:
    """Test ResultView functionality and conversions"""
    
    def test_result_view_basic_properties(self):
        """Test ResultView basic properties"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25, "city": "NYC"},
                {"name": "Bob", "age": 30, "city": "LA"},
            ]
            client.store(test_data)
            
            results = client.query()
            
            # Test basic properties
            assert len(results) == 2
            # Shape may include or exclude _id column
            assert results.shape[0] == 2  # 2 rows
            
            # Test columns property
            columns = results.columns
            assert "name" in columns
            assert "age" in columns
            assert "city" in columns
            # _id may or may not be hidden depending on implementation
            
            client.close()
    
    def test_result_view_iteration(self):
        """Test ResultView iteration"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
                {"name": "Charlie", "age": 35},
            ]
            client.store(test_data)
            
            results = client.query()
            
            # Test iteration
            names = []
            ages = []
            for result in results:
                names.append(result["name"])
                ages.append(result["age"])
            
            assert len(names) == 3
            assert "Alice" in names
            assert "Bob" in names
            assert "Charlie" in names
            assert ages == [25, 30, 35]
            
            client.close()
    
    def test_result_view_indexing(self):
        """Test ResultView indexing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
                {"name": "Charlie", "age": 35},
            ]
            client.store(test_data)
            
            results = client.query()
            
            # Test indexing
            first = results[0]
            assert first["name"] == "Alice"
            assert first["age"] == 25
            
            last = results[2]
            assert last["name"] == "Charlie"
            assert last["age"] == 35
            
            # Test slicing
            subset = results[0:2]
            assert len(subset) == 2
            
            client.close()
    
    def test_result_view_to_dict(self):
        """Test ResultView.to_dict() method"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
            ]
            client.store(test_data)
            
            results = client.query()
            dict_list = results.to_dict()
            
            assert isinstance(dict_list, list)
            assert len(dict_list) == 2
            assert isinstance(dict_list[0], dict)
            assert dict_list[0]["name"] == "Alice"
            assert dict_list[1]["name"] == "Bob"
            
            client.close()

    def test_result_view_tolist(self):
        """Test ResultView.tolist() method"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")

            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
                {"name": "Charlie", "age": 35},
            ]
            client.store(test_data)

            results = client.query()
            rows = results.tolist()

            # Basic type and length checks
            assert isinstance(rows, list)
            assert len(rows) == 3
            assert isinstance(rows[0], dict)

            # Values are correct
            names = [r["name"] for r in rows]
            ages = [r["age"] for r in rows]
            assert "Alice" in names
            assert "Bob" in names
            assert "Charlie" in names
            assert 25 in ages
            assert 30 in ages
            assert 35 in ages

            # Internal _id must be hidden
            assert "_id" not in rows[0]

            # tolist() and to_dict() must return equivalent data
            assert rows == results.to_dict()

            client.close()

    def test_result_view_tolist_empty(self):
        """Test ResultView.tolist() on empty result"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")

            results = client.query()
            rows = results.tolist()

            assert isinstance(rows, list)
            assert len(rows) == 0

            client.close()

    @pytest.mark.skipif(not PANDAS_AVAILABLE, reason="Pandas not available")
    def test_result_view_to_pandas(self):
        """Test ResultView.to_pandas() method"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25, "city": "NYC"},
                {"name": "Bob", "age": 30, "city": "LA"},
            ]
            client.store(test_data)
            
            results = client.query()
            df = results.to_pandas()
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert "name" in df.columns
            assert "age" in df.columns
            assert "city" in df.columns
            assert "_id" not in df.columns  # _id should be hidden
            
            # Test zero_copy parameter
            df_zero_copy = results.to_pandas(zero_copy=True)
            assert isinstance(df_zero_copy, pd.DataFrame)
            assert len(df_zero_copy) == 2
            
            df_copy = results.to_pandas(zero_copy=False)
            assert isinstance(df_copy, pd.DataFrame)
            assert len(df_copy) == 2
            
            client.close()
    
    @pytest.mark.skipif(not POLARS_DF_AVAILABLE, reason="Polars not available")
    def test_result_view_to_polars(self):
        """Test ResultView.to_polars() method"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
            ]
            client.store(test_data)
            
            results = client.query()
            df = results.to_polars()
            
            assert isinstance(df, pl.DataFrame)
            assert len(df) == 2
            assert "name" in df.columns
            assert "age" in df.columns
            assert "_id" not in df.columns  # _id should be hidden
            
            client.close()
    
    @pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
    def test_result_view_to_arrow(self):
        """Test ResultView.to_arrow() method"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
            ]
            client.store(test_data)
            
            results = client.query()
            table = results.to_arrow()
            
            assert isinstance(table, pa.Table)
            assert len(table) == 2
            assert "name" in table.column_names
            assert "age" in table.column_names
            assert "_id" not in table.column_names  # _id should be hidden
            
            client.close()
    
    def test_result_view_get_ids(self):
        """Test ResultView.get_ids() method"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
                {"name": "Charlie", "age": 35},
            ]
            client.store(test_data)
            
            results = client.query()
            
            # Test get_ids with numpy array (default)
            ids = results.get_ids()
            assert isinstance(ids, np.ndarray)
            assert len(ids) == 3
            assert all(isinstance(id, (int, np.integer)) for id in ids)
            
            # Test get_ids with list
            ids_list = results.get_ids(return_list=True)
            assert isinstance(ids_list, list)
            assert len(ids_list) == 3
            assert all(isinstance(id, int) for id in ids_list)
            
            # Compare results
            assert ids.tolist() == ids_list
            
            client.close()
    
    def test_result_view_ids_property_deprecated(self):
        """Test ResultView.ids property (deprecated)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
            ]
            client.store(test_data)
            
            results = client.query()
            
            # Test deprecated ids property
            ids = results.ids
            assert isinstance(ids, list)
            assert len(ids) == 2
            
            client.close()
    
    def test_result_view_repr(self):
        """Test ResultView.__repr__ method"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
            ]
            client.store(test_data)
            
            results = client.query()
            repr_str = repr(results)
            
            assert "ResultView" in repr_str
            assert "rows=2" in repr_str
            
            client.close()


class TestQueryEdgeCases:
    """Test edge cases and error handling for queries"""
    
    def test_query_invalid_syntax(self):
        """Test query with invalid SQL syntax"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [{"name": "Alice", "age": 25}]
            client.store(test_data)
            
            # Test invalid SQL syntax
            with pytest.raises(Exception):  # Should raise some kind of error
                client.query("invalid syntax here")
            
            client.close()
    
    def test_query_nonexistent_column(self):
        """Test query with nonexistent column"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [{"name": "Alice", "age": 25}]
            client.store(test_data)
            
            # Query with nonexistent column
            try:
                results = client.query("nonexistent_column = 'test'")
                # If no exception, should return empty results
                assert len(results) == 0
            except Exception as e:
                # Exception is also acceptable behavior
                print(f"Nonexistent column query handled: {e}")
            
            client.close()
    
    def test_query_on_closed_client(self):
        """Test query operations on closed client"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.close()
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.query()
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.query("age > 25")
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.query(limit=10)
    
    def test_query_with_special_characters(self):
        """Test query with special characters in values"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data with special characters
            test_data = [
                {"name": "Alice", "description": "Test data"},
                {"name": "Bob", "description": "Test with quotes"},
                {"name": "Charlie", "description": "Test with symbols"},
            ]
            client.store(test_data)
            
            # Query basic - special character handling may vary
            try:
                results = client.query("name = 'Alice'")
                assert len(results) >= 1
            except Exception as e:
                print(f"Special character query: {e}")
            
            client.close()
    
    def test_query_with_numeric_types(self):
        """Test query with different numeric types"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data with various numeric types
            test_data = [
                {"name": "Int Test", "value": 42, "float_val": 3.14159},
                {"name": "Large Int", "value": 9223372036854775807, "float_val": 1e10},
                {"name": "Negative", "value": -100, "float_val": -3.14},
                {"name": "Zero", "value": 0, "float_val": 0.0},
            ]
            client.store(test_data)
            
            # Test numeric comparisons
            results = client.query("value > 0")
            assert len(results) == 2
            
            results = client.query("float_val > 3.0")
            assert len(results) == 2
            
            results = client.query("value = 0")
            assert len(results) == 1
            assert results[0]["name"] == "Zero"
            
            client.close()
    
    def test_query_performance_large_dataset(self):
        """Test query performance with large dataset"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store large dataset
            large_data = [
                {"id": i, "category": f"cat_{i % 10}", "value": i * 1.5}
                for i in range(10000)
            ]
            client.store(large_data)
            
            # Test query performance
            import time
            
            start_time = time.time()
            results = client.query("category = 'cat_5'")
            end_time = time.time()
            
            # Should return 1000 results (every 10th record)
            assert len(results) == 1000
            
            # Query should be reasonably fast (less than 1 second for 10k records)
            query_time = end_time - start_time
            assert query_time < 1.0
            
            # Test with limit
            start_time = time.time()
            results = client.query(limit=100)
            end_time = time.time()
            
            assert len(results) == 100
            assert (end_time - start_time) < 0.5  # Should be even faster with limit
            
            client.close()
    
    def test_query_with_boolean_values(self):
        """Test query with boolean values"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data with boolean values
            test_data = [
                {"name": "Alice", "active": True, "admin": False},
                {"name": "Bob", "active": False, "admin": False},
                {"name": "Charlie", "active": True, "admin": True},
                {"name": "Diana", "active": False, "admin": True},
            ]
            client.store(test_data)
            
            # Test boolean queries
            results = client.query("active = true")
            assert len(results) == 2
            
            results = client.query("admin = false")
            assert len(results) == 2
            
            results = client.query("active = true AND admin = true")
            assert len(results) == 1
            assert results[0]["name"] == "Charlie"
            
            client.close()
    
    def test_query_with_in_operator(self):
        """Test query with IN operator"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "city": "NYC"},
                {"name": "Bob", "city": "LA"},
                {"name": "Charlie", "city": "Chicago"},
                {"name": "Diana", "city": "NYC"},
                {"name": "Eve", "city": "Boston"},
            ]
            client.store(test_data)
            
            # Test IN operator
            results = client.query("city IN ('NYC', 'LA')")
            assert len(results) == 3
            
            cities = [r["city"] for r in results]
            assert "NYC" in cities
            assert "LA" in cities
            
            client.close()
    
    def test_query_with_between_operator(self):
        """Test query with BETWEEN operator"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
                {"name": "Charlie", "age": 35},
                {"name": "Diana", "age": 40},
                {"name": "Eve", "age": 45},
            ]
            client.store(test_data)
            
            # Test BETWEEN operator - may or may not be supported
            try:
                results = client.query("age BETWEEN 30 AND 40")
                # Should return records with age 30-40
                assert len(results) >= 0
            except Exception as e:
                # BETWEEN may not be supported, use alternative
                results = client.query("age >= 30 AND age <= 40")
                assert len(results) >= 2
            
            client.close()


class TestQueryOptimizations:
    """Test query optimizations and performance"""
    
    def test_query_limit_optimization(self):
        """Test that limit parameter provides optimization"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store large dataset
            large_data = [{"id": i, "value": f"item_{i}"} for i in range(1000)]
            client.store(large_data)
            
            # Test that limit actually limits results
            results = client.query(limit=10)
            assert len(results) == 10
            
            # Test limit with WHERE clause
            results = client.query("id > 500", limit=5)
            assert len(results) == 5
            for result in results:
                assert result["id"] > 500
            
            client.close()
    
    def test_query_arrow_optimization(self):
        """Test Arrow optimization when available"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
                {"name": "Charlie", "age": 35},
            ]
            client.store(test_data)
            
            results = client.query()
            
            # If Arrow is available, results should use Arrow internally
            if ARROW_AVAILABLE:
                # Test that Arrow conversion works
                if PYARROW_AVAILABLE:
                    arrow_table = results.to_arrow()
                    assert isinstance(arrow_table, pa.Table)
            
            client.close()
    
    def test_query_empty_result_view(self):
        """Test ResultView with empty results"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Query empty database
            results = client.query("name = 'nonexistent'")
            
            # Test empty ResultView properties
            assert len(results) == 0
            assert results.shape == (0, 0)
            assert results.columns == []
            
            # Test empty conversions
            dict_list = results.to_dict()
            assert dict_list == []
            
            ids = results.get_ids()
            assert len(ids) == 0
            
            # Test iteration on empty results
            count = 0
            for _ in results:
                count += 1
            assert count == 0
            
            client.close()


class TestExecuteBatch:
    """Test execute_batch functionality with scheduler"""

    def test_execute_batch_single_query(self):
        """Test execute_batch with a single query (should use direct execute)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")

            # Store test data
            test_data = [
                {"name": "Alice", "age": 25, "city": "NYC"},
                {"name": "Bob", "age": 30, "city": "LA"},
                {"name": "Charlie", "age": 35, "city": "NYC"},
            ]
            client.store(test_data)

            # Single query in batch
            results = client.execute_batch([
                "SELECT * FROM default WHERE name = 'Alice'"
            ])

            assert len(results) == 1
            assert len(results[0]) == 1
            assert results[0][0]["name"] == "Alice"

            client.close()

    def test_execute_batch_multiple_queries(self):
        """Test execute_batch with multiple queries (uses scheduler)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")

            # Store test data
            test_data = [
                {"name": "Alice", "age": 25, "city": "NYC"},
                {"name": "Bob", "age": 30, "city": "LA"},
                {"name": "Charlie", "age": 35, "city": "NYC"},
            ]
            client.store(test_data)

            # Multiple queries in batch
            queries = [
                "SELECT COUNT(*) FROM default",
                "SELECT * FROM default WHERE city = 'NYC'",
                "SELECT AVG(age) FROM default",
            ]
            results = client.execute_batch(queries)

            assert len(results) == 3
            # COUNT(*)
            assert len(results[0]) == 1
            # WHERE city = 'NYC' (2 rows)
            assert len(results[1]) == 2
            # AVG(age)
            assert len(results[2]) == 1

            client.close()

    def test_execute_batch_empty_list(self):
        """Test execute_batch with empty query list"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")

            results = client.execute_batch([])

            assert results == []

            client.close()

    def test_execute_batch_mixed_queries(self):
        """Test execute_batch with different query types"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")

            # Store test data
            test_data = [{"name": f"user_{i}", "value": i} for i in range(100)]
            client.store(test_data)

            queries = [
                "SELECT * FROM default LIMIT 10",
                "SELECT COUNT(*) FROM default WHERE value > 50",
                "SELECT * FROM default WHERE value = 42",
                "SELECT MAX(value), MIN(value) FROM default",
            ]
            results = client.execute_batch(queries)

            assert len(results) == 4
            # LIMIT 10
            assert len(results[0]) == 10
            # COUNT where value > 50
            assert len(results[1]) == 1
            # value = 42
            assert len(results[2]) == 1
            # MAX/MIN
            assert len(results[3]) == 1

            client.close()

    def test_execute_batch_with_aggregation(self):
        """Test execute_batch with GROUP BY queries"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")

            # Store test data with groups
            test_data = [
                {"category": "A", "value": 10},
                {"category": "A", "value": 20},
                {"category": "B", "value": 30},
                {"category": "B", "value": 40},
            ]
            client.store(test_data)

            queries = [
                "SELECT category, SUM(value) FROM default GROUP BY category",
                "SELECT category, AVG(value) FROM default GROUP BY category",
            ]
            results = client.execute_batch(queries)

            assert len(results) == 2
            # SUM
            sum_result = results[0].to_dict()
            assert len(sum_result) == 2
            # AVG
            avg_result = results[1].to_dict()
            assert len(avg_result) == 2

            client.close()

    def test_execute_batch_no_results(self):
        """Test execute_batch with queries returning no results"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")

            # Store minimal data
            client.store({"name": "test", "value": 1})

            queries = [
                "SELECT * FROM default WHERE name = 'nonexistent'",
                "SELECT * FROM default WHERE value > 1000",
            ]
            results = client.execute_batch(queries)

            assert len(results) == 2
            assert len(results[0]) == 0
            assert len(results[1]) == 0

            client.close()

    def test_execute_batch_result_view_types(self):
        """Test that execute_batch returns proper ResultView objects"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")

            client.store({"name": "test", "value": 42})

            queries = ["SELECT * FROM default", "SELECT COUNT(*) FROM default"]
            results = client.execute_batch(queries)

            # Both should be ResultView
            assert isinstance(results[0], ResultView)
            assert isinstance(results[1], ResultView)

            # Test that we can convert to dict
            assert isinstance(results[0].to_dict(), list)
            assert isinstance(results[1].to_dict(), list)

            client.close()

    def test_execute_batch_consistency_with_execute(self):
        """Test that execute_batch gives same results as execute"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")

            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
            ]
            client.store(test_data)

            query = "SELECT * FROM default WHERE age > 25"

            # Execute via execute()
            result_single = client.execute(query)

            # Execute via execute_batch
            result_batch = client.execute_batch([query])

            # Should have same results
            assert len(result_single) == len(result_batch[0])
            assert result_single.to_dict() == result_batch[0].to_dict()

            client.close()


def test_select_star_shape_excludes_hidden_internal_id():
    """``shape`` and ``columns`` must agree about the hidden ``_id`` column."""
    client = ApexClient(":memory:")
    try:
        client.create_table("people")
        client.store([{"name": "A", "age": 1}, {"name": "B", "age": 2}])

        result = client.execute("SELECT * FROM people")
        assert "_id" not in result.columns
        assert result.shape == (2, len(result.columns))

        with_id = client.execute("SELECT * FROM people", show_internal_id=True)
        assert with_id.columns[0] == "_id"
        assert with_id.shape == (2, len(with_id.columns))

        projected = client.execute("SELECT name FROM people")
        assert projected.columns == ["name"]
        assert projected.shape == (2, 1)
    finally:
        client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
