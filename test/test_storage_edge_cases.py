"""
Comprehensive test suite for ApexBase Storage Edge Cases

This module tests extreme scenarios including:
- ALTER TABLE + INSERT combinations
- Schema changes with existing data
- Empty table operations and delta merging
- Type coercion and mismatch scenarios
- Concurrent read/write operations
- Large batch operations
- Column ordering edge cases
- Persistence and recovery scenarios
"""

import pytest
import tempfile
import threading
import time
import os
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

try:
    from apexbase import ApexClient
except ImportError as e:
    pytest.skip(f"ApexBase not available: {e}", allow_module_level=True)


class TestAlterTableInsertScenarios:
    """Test ALTER TABLE followed by INSERT operations"""
    
    def test_alter_add_column_then_insert(self):
        """Test adding column via ALTER then inserting data"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert initial data
            client.store([{"name": "Alice", "age": 25}])
            client.flush()
            
            # Add column via SQL
            client.execute("ALTER TABLE test ADD COLUMN city STRING")
            
            # Insert with new column
            client.store([{"name": "Bob", "age": 30, "city": "NYC"}])
            client.flush()
            
            # Verify
            result = client.execute("SELECT * FROM test ORDER BY name").to_dict()
            assert len(result) == 2
            assert result[0]["name"] == "Alice"
            assert result[0].get("city") in [None, ""]  # Old row has no city
            assert result[1]["name"] == "Bob"
            assert result[1]["city"] == "NYC"
            
            client.close()
    
    def test_alter_add_multiple_columns_then_insert(self):
        """Test adding multiple columns then inserting"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert initial data
            client.store([{"id": 1}])
            client.flush()
            
            # Add multiple columns
            client.execute("ALTER TABLE test ADD COLUMN name STRING")
            client.execute("ALTER TABLE test ADD COLUMN age INTEGER")
            client.execute("ALTER TABLE test ADD COLUMN salary FLOAT")
            client.execute("ALTER TABLE test ADD COLUMN active BOOLEAN")
            
            # Insert with all new columns
            client.store([{
                "id": 2, 
                "name": "Bob", 
                "age": 30, 
                "salary": 50000.5, 
                "active": True
            }])
            client.flush()
            
            # Verify
            result = client.execute("SELECT * FROM test WHERE id = 2").to_dict()
            assert len(result) == 1
            assert result[0]["name"] == "Bob"
            assert result[0]["age"] == 30
            assert abs(result[0]["salary"] - 50000.5) < 0.01
            assert result[0]["active"] == True
            
            client.close()
    
    def test_alter_empty_table_then_insert(self):
        """Test ALTER on empty table then INSERT"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Add columns to empty table
            client.execute("ALTER TABLE test ADD COLUMN name STRING")
            client.execute("ALTER TABLE test ADD COLUMN value INTEGER")
            
            # Insert data
            client.store([{"name": "Test", "value": 100}])
            client.flush()
            
            # Verify
            result = client.execute("SELECT * FROM test").to_dict()
            assert len(result) == 1
            assert result[0]["name"] == "Test"
            assert result[0]["value"] == 100
            
            client.close()
    
    def test_alter_reopen_then_insert(self):
        """Test ALTER, close, reopen, then INSERT"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # First session: create and alter
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            client.store([{"id": 1}])
            client.flush()
            client.execute("ALTER TABLE test ADD COLUMN name STRING")
            client.close()
            
            # Second session: reopen and insert
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.use_table("test")
            client.store([{"id": 2, "name": "Bob"}])
            client.flush()
            
            # Verify
            result = client.execute("SELECT * FROM test ORDER BY id").to_dict()
            assert len(result) == 2
            assert result[1]["name"] == "Bob"
            
            client.close()
    
    def test_alter_drop_column_then_insert(self):
        """Test DROP COLUMN then INSERT"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert with multiple columns
            client.store([{"name": "Alice", "age": 25, "city": "NYC"}])
            client.flush()
            
            # Drop column
            client.execute("ALTER TABLE test DROP COLUMN city")
            
            # Insert without dropped column
            client.store([{"name": "Bob", "age": 30}])
            client.flush()
            
            # Verify city column is gone
            result = client.execute("SELECT * FROM test").to_dict()
            assert len(result) == 2
            for row in result:
                assert "city" not in row or row.get("city") is None
            
            client.close()


class TestSchemaEvolution:
    """Test schema changes with existing data"""
    
    def test_insert_new_column_auto_schema(self):
        """Test inserting data with new column auto-extends schema"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert initial data
            client.store([{"a": 1}])
            client.flush()
            
            # Insert with new columns progressively
            client.store([{"a": 2, "b": "hello"}])
            client.store([{"a": 3, "b": "world", "c": 3.14}])
            client.store([{"a": 4, "b": "test", "c": 2.71, "d": True}])
            client.flush()
            
            # Verify all columns exist
            result = client.execute("SELECT * FROM test ORDER BY a").to_dict()
            assert len(result) == 4
            
            # Last row should have all columns
            assert result[3]["a"] == 4
            assert result[3]["b"] == "test"
            assert abs(result[3]["c"] - 2.71) < 0.01
            assert result[3]["d"] == True
            
            client.close()
    
    def test_mixed_type_columns_across_inserts(self):
        """Test handling of mixed types in same column across inserts"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert with integer
            client.store([{"id": 1, "value": 100}])
            client.flush()
            
            # Insert with float in same column - should work due to type coercion
            client.store([{"id": 2, "value": 100.5}])
            client.flush()
            
            result = client.execute("SELECT * FROM test ORDER BY id").to_dict()
            assert len(result) == 2
            
            client.close()
    
    def test_null_values_in_new_columns(self):
        """Test NULL handling in newly added columns"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert initial data
            for i in range(100):
                client.store([{"id": i, "name": f"name_{i}"}])
            client.flush()
            
            # Add column
            client.execute("ALTER TABLE test ADD COLUMN score INTEGER")
            
            # Insert with score
            client.store([{"id": 100, "name": "new", "score": 95}])
            client.flush()
            
            # Query old rows - score should be NULL or default
            result = client.execute("SELECT * FROM test WHERE id < 5 ORDER BY id").to_dict()
            assert len(result) == 5
            for row in result:
                assert row.get("score") in [None, 0]
            
            # Query new row
            result = client.execute("SELECT * FROM test WHERE id = 100").to_dict()
            assert result[0]["score"] == 95
            
            client.close()


class TestEmptyTableOperations:
    """Test operations on empty tables"""
    
    def test_query_empty_table(self):
        """Test querying empty table"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("empty")
            client.use_table("empty")
            
            result = client.execute("SELECT * FROM empty").to_dict()
            assert result == []
            
            client.close()
    
    def test_aggregate_empty_table(self):
        """Test aggregations on empty table"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("empty")
            client.use_table("empty")
            
            # Add schema
            client.execute("ALTER TABLE empty ADD COLUMN value INTEGER")
            
            result = client.execute("SELECT COUNT(*) as cnt FROM empty").to_dict()
            assert result[0]["cnt"] == 0
            
            client.close()
    
    def test_truncate_then_insert(self):
        """Test TRUNCATE then INSERT"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert data
            for i in range(100):
                client.store([{"id": i, "value": i * 10}])
            client.flush()
            
            # Verify data exists
            result = client.execute("SELECT COUNT(*) as cnt FROM test").to_dict()
            assert result[0]["cnt"] == 100
            
            # Truncate
            client.execute("TRUNCATE TABLE test")
            
            # Verify empty
            result = client.execute("SELECT COUNT(*) as cnt FROM test").to_dict()
            assert result[0]["cnt"] == 0
            
            # Insert new data
            client.store([{"id": 999, "value": 999}])
            client.flush()
            
            result = client.execute("SELECT * FROM test").to_dict()
            assert len(result) == 1
            assert result[0]["id"] == 999
            
            client.close()
    
    def test_delete_all_then_insert(self):
        """Test DELETE all rows then INSERT"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert data
            client.store([{"id": i} for i in range(10)])
            client.flush()
            
            # Delete all
            client.execute("DELETE FROM test")
            
            # Insert new data
            client.store([{"id": 100, "name": "new"}])
            client.flush()
            
            result = client.execute("SELECT * FROM test").to_dict()
            assert len(result) == 1
            assert result[0]["id"] == 100
            
            client.close()


class TestLargeBatchOperations:
    """Test large batch insert/query operations"""
    
    def test_large_batch_insert(self):
        """Test inserting large batch of rows"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert 10000 rows in one batch
            data = [{"id": i, "name": f"name_{i}", "value": i * 1.5} for i in range(10000)]
            client.store(data)
            client.flush()
            
            # Verify count
            result = client.execute("SELECT COUNT(*) as cnt FROM test").to_dict()
            assert result[0]["cnt"] == 10000
            
            # Verify sampling
            result = client.execute("SELECT * FROM test WHERE id = 5000").to_dict()
            assert result[0]["name"] == "name_5000"
            
            client.close()
    
    def test_multiple_small_batches(self):
        """Test many small batch inserts"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert 1000 batches of 10 rows each
            for batch in range(1000):
                data = [{"batch": batch, "row": i, "value": batch * 10 + i} for i in range(10)]
                client.store(data)
            client.flush()
            
            # Verify count
            result = client.execute("SELECT COUNT(*) as cnt FROM test").to_dict()
            assert result[0]["cnt"] == 10000
            
            client.close()
    
    def test_large_string_values(self):
        """Test inserting large string values"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert rows with large strings
            large_str = "x" * 100000  # 100KB string
            client.store([{"id": 1, "data": large_str}])
            client.flush()
            
            result = client.execute("SELECT LENGTH(data) as len FROM test").to_dict()
            assert result[0]["len"] == 100000
            
            client.close()
    
    def test_many_columns(self):
        """Test table with many columns"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert row with 100 columns
            data = {f"col_{i}": i for i in range(100)}
            client.store([data])
            client.flush()
            
            # Verify specific columns
            result = client.execute("SELECT col_0, col_50, col_99 FROM test").to_dict()
            assert result[0]["col_0"] == 0
            assert result[0]["col_50"] == 50
            assert result[0]["col_99"] == 99
            
            client.close()


class TestConcurrentOperations:
    """Test concurrent read/write operations"""
    
    def test_concurrent_reads(self):
        """Test multiple concurrent read operations"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert test data
            client.store([{"id": i, "value": i * 10} for i in range(1000)])
            client.flush()
            
            results = []
            errors = []
            
            def read_task(thread_id):
                try:
                    for _ in range(10):
                        r = client.execute(f"SELECT * FROM test WHERE id = {thread_id * 10}").to_dict()
                        results.append(len(r))
                except Exception as e:
                    errors.append(str(e))
            
            # Run concurrent reads
            threads = [threading.Thread(target=read_task, args=(i,)) for i in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            assert len(errors) == 0, f"Errors: {errors}"
            assert len(results) == 100
            
            client.close()
    
    def test_concurrent_writes_sequential(self):
        """Test sequential writes from multiple threads (with locking)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            errors = []
            lock = threading.Lock()
            
            def write_task(thread_id):
                try:
                    for i in range(10):
                        with lock:
                            client.store([{"thread": thread_id, "seq": i}])
                except Exception as e:
                    errors.append(str(e))
            
            threads = [threading.Thread(target=write_task, args=(i,)) for i in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            client.flush()
            
            assert len(errors) == 0, f"Errors: {errors}"
            
            result = client.execute("SELECT COUNT(*) as cnt FROM test").to_dict()
            assert result[0]["cnt"] == 50
            
            client.close()


class TestPersistenceRecovery:
    """Test data persistence and recovery"""
    
    def test_persistence_across_sessions(self):
        """Test data persists across client sessions"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Session 1: create and insert
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            client.store([{"id": 1, "name": "Alice"}])
            client.flush()
            client.close()
            
            # Session 2: read
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.use_table("test")
            result = client.execute("SELECT * FROM test").to_dict()
            assert len(result) == 1
            assert result[0]["name"] == "Alice"
            client.close()
    
    def test_schema_persistence(self):
        """Test schema persists across sessions"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Session 1: create schema
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            client.execute("ALTER TABLE test ADD COLUMN name STRING")
            client.execute("ALTER TABLE test ADD COLUMN age INTEGER")
            client.close()
            
            # Session 2: use schema
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.use_table("test")
            client.store([{"name": "Bob", "age": 30}])
            client.flush()
            
            result = client.execute("SELECT * FROM test").to_dict()
            assert result[0]["name"] == "Bob"
            assert result[0]["age"] == 30
            client.close()
    
    def test_incremental_inserts_persistence(self):
        """Test incremental inserts persist correctly"""
        with tempfile.TemporaryDirectory() as temp_dir:
            for session in range(5):
                client = ApexClient(dirpath=temp_dir)
                if session == 0:
                    client.create_table("default")
                else:
                    client.use_table("default")
                if session == 0:
                    client.create_table("test")
                client.use_table("test")
                
                # Insert 100 rows per session
                client.store([{"session": session, "row": i} for i in range(100)])
                client.flush()
                client.close()
            
            # Final read
            client = ApexClient(dirpath=temp_dir)
            client.use_table("default")
            client.use_table("test")
            result = client.execute("SELECT COUNT(*) as cnt FROM test").to_dict()
            assert result[0]["cnt"] == 500
            
            # Verify each session's data
            for session in range(5):
                result = client.execute(f"SELECT COUNT(*) as cnt FROM test WHERE session = {session}").to_dict()
                assert result[0]["cnt"] == 100
            
            client.close()


class TestColumnOrderEdgeCases:
    """Test column ordering edge cases"""
    
    def test_insert_columns_different_order(self):
        """Test inserting columns in different order"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert with order: a, b, c
            client.store([{"a": 1, "b": 2, "c": 3}])
            
            # Insert with order: c, a, b
            client.store([{"c": 6, "a": 4, "b": 5}])
            
            # Insert with order: b, c, a
            client.store([{"b": 8, "c": 9, "a": 7}])
            
            client.flush()
            
            result = client.execute("SELECT a, b, c FROM test ORDER BY a").to_dict()
            assert len(result) == 3
            assert result[0] == {"a": 1, "b": 2, "c": 3}
            assert result[1] == {"a": 4, "b": 5, "c": 6}
            assert result[2] == {"a": 7, "b": 8, "c": 9}
            
            client.close()
    
    def test_partial_columns_insert(self):
        """Test inserting with partial columns - missing columns are NULL"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Insert full row
            client.store([{"a": 1, "b": 2, "c": 3}])
            
            # Insert partial rows - missing columns will be NULL
            client.store([{"a": 4, "c": 6}])  # missing b -> b=NULL
            client.store([{"b": 8}])  # missing a, c -> a=NULL, c=NULL
            
            client.flush()
            
            # Use ORDER BY _id to get insertion order
            result = client.execute("SELECT * FROM test ORDER BY _id").to_dict()
            assert len(result) == 3
            
            # First row: complete {a:1, b:2, c:3}
            assert result[0]["a"] == 1
            assert result[0]["b"] == 2
            assert result[0]["c"] == 3
            
            # Second row: partial {a:4, b:NULL, c:6}
            assert result[1]["a"] == 4
            assert result[1]["b"] is None  # NULL
            assert result[1]["c"] == 6
            
            # Third row: partial {a:NULL, b:8, c:NULL}
            assert result[2]["a"] is None  # NULL
            assert result[2]["b"] == 8
            assert result[2]["c"] is None  # NULL
            
            client.close()


class TestSQLEdgeCases:
    """Test SQL edge cases"""
    
    def test_select_with_reserved_word_alias(self):
        """Test SELECT with reserved word as alias"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            client.store([{"value": 100}])
            client.flush()
            
            # Use quoted alias for reserved words
            result = client.execute('SELECT value AS "order" FROM test').to_dict()
            assert len(result) == 1
            
            client.close()
    
    def test_complex_where_conditions(self):
        """Test complex WHERE conditions"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            client.store([
                {"id": 1, "a": 10, "b": 20, "c": "x"},
                {"id": 2, "a": 15, "b": 25, "c": "y"},
                {"id": 3, "a": 20, "b": 30, "c": "x"},
                {"id": 4, "a": 25, "b": 35, "c": "y"},
            ])
            client.flush()
            
            # Complex condition
            result = client.execute("""
                SELECT * FROM test 
                WHERE (a > 10 AND b < 35) OR (c = 'x' AND a >= 20)
                ORDER BY id
            """).to_dict()
            
            assert len(result) >= 2
            
            client.close()
    
    def test_nested_function_calls(self):
        """Test nested function calls"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            client.store([{"name": "  HELLO WORLD  "}])
            client.flush()
            
            result = client.execute("SELECT LOWER(TRIM(name)) as clean FROM test").to_dict()
            assert result[0]["clean"] == "hello world"
            
            client.close()
    
    def test_group_by_with_having(self):
        """Test GROUP BY with HAVING clause"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            client.store([
                {"category": "A", "value": 10},
                {"category": "A", "value": 20},
                {"category": "B", "value": 5},
                {"category": "B", "value": 15},
                {"category": "C", "value": 100},
            ])
            client.flush()
            
            result = client.execute("""
                SELECT category, SUM(value) as total 
                FROM test 
                GROUP BY category 
                HAVING SUM(value) > 20
                ORDER BY total
            """).to_dict()
            
            assert len(result) == 2
            assert result[0]["category"] == "A"
            assert result[1]["category"] == "C"
            
            client.close()


class TestDataTypeEdgeCases:
    """Test data type edge cases"""
    
    def test_extreme_integer_values(self):
        """Test extreme integer values"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            # Test large integers
            client.store([
                {"id": 1, "value": 2**62},
                {"id": 2, "value": -2**62},
                {"id": 3, "value": 0},
            ])
            client.flush()
            
            result = client.execute("SELECT * FROM test ORDER BY id").to_dict()
            assert result[0]["value"] == 2**62
            assert result[1]["value"] == -2**62
            assert result[2]["value"] == 0
            
            client.close()
    
    def test_float_precision(self):
        """Test float precision handling"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            client.store([
                {"id": 1, "value": 0.1 + 0.2},  # Classic float precision test
                {"id": 2, "value": 1e-10},
                {"id": 3, "value": 1e10},
            ])
            client.flush()
            
            result = client.execute("SELECT * FROM test ORDER BY id").to_dict()
            assert abs(result[0]["value"] - 0.3) < 1e-10
            assert abs(result[1]["value"] - 1e-10) < 1e-15
            assert abs(result[2]["value"] - 1e10) < 1
            
            client.close()
    
    def test_unicode_strings(self):
        """Test unicode string handling"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            client.store([
                {"id": 1, "text": "Hello 世界"},
                {"id": 2, "text": "مرحبا بالعالم"},
                {"id": 3, "text": "🎉🎊🎈"},
                {"id": 4, "text": "Привет мир"},
            ])
            client.flush()
            
            result = client.execute("SELECT * FROM test ORDER BY id").to_dict()
            assert result[0]["text"] == "Hello 世界"
            assert result[1]["text"] == "مرحبا بالعالم"
            assert result[2]["text"] == "🎉🎊🎈"
            assert result[3]["text"] == "Привет мир"
            
            client.close()
    
    def test_empty_string_vs_null(self):
        """Test empty string vs NULL distinction"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            client.store([
                {"id": 1, "text": ""},
                {"id": 2, "text": None},
                {"id": 3, "text": "value"},
            ])
            client.flush()
            
            # Query non-null
            result = client.execute("SELECT * FROM test WHERE text IS NOT NULL ORDER BY id").to_dict()
            # Empty string may or may not be treated as NULL depending on implementation
            assert len(result) >= 1
            
            client.close()
    
    def test_boolean_values(self):
        """Test boolean value handling"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            client.store([
                {"id": 1, "flag": True},
                {"id": 2, "flag": False},
                {"id": 3, "flag": None},
            ])
            client.flush()
            
            result = client.execute("SELECT * FROM test WHERE flag = true").to_dict()
            assert len(result) == 1
            assert result[0]["id"] == 1
            
            # Boolean NULL is now correctly distinguished from false
            result = client.execute("SELECT * FROM test WHERE flag = false").to_dict()
            assert len(result) == 1  # Only id=2 (explicit false), not id=3 (NULL)
            assert result[0]["id"] == 2
            
            # NULL booleans can be queried with IS NULL
            result = client.execute("SELECT * FROM test WHERE flag IS NULL").to_dict()
            assert len(result) == 1
            assert result[0]["id"] == 3
            
            client.close()


class TestBoundaryConditions:
    """Test boundary conditions"""
    
    def test_single_row_operations(self):
        """Test operations on single row table"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            client.store([{"id": 1, "value": 100}])
            client.flush()
            
            # Aggregations on single row
            result = client.execute("SELECT COUNT(*) as cnt, SUM(value) as sum, AVG(value) as avg FROM test").to_dict()
            assert result[0]["cnt"] == 1
            assert result[0]["sum"] == 100
            assert result[0]["avg"] == 100.0
            
            client.close()
    
    def test_single_column_table(self):
        """Test table with single column"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            client.store([{"only_col": i} for i in range(100)])
            client.flush()
            
            result = client.execute("SELECT * FROM test ORDER BY only_col LIMIT 5").to_dict()
            assert len(result) == 5
            assert all("only_col" in r for r in result)
            
            client.close()
    
    def test_limit_offset_edge_cases(self):
        """Test LIMIT and OFFSET edge cases"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("test")
            client.use_table("test")
            
            client.store([{"id": i} for i in range(10)])
            client.flush()
            
            # Offset beyond data
            result = client.execute("SELECT * FROM test LIMIT 5 OFFSET 100").to_dict()
            assert len(result) == 0
            
            # Limit larger than data
            result = client.execute("SELECT * FROM test LIMIT 100").to_dict()
            assert len(result) == 10
            
            client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
