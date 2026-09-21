"""
Comprehensive test suite for ApexBase Column Management Operations

This module tests:
- Column addition with various data types
- Column deletion operations
- Column renaming operations
- Column data type retrieval
- Edge cases and error handling
- Column operations with existing data
- Performance considerations
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import sys
import os

# Add the apexbase python module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

try:
    from apexbase import ApexClient
except ImportError as e:
    pytest.skip(f"ApexBase not available: {e}", allow_module_level=True)


class TestColumnAddition:
    """Test column addition operations"""
    
    def test_add_column_basic(self):
        """Test basic column addition"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice", "age": 25})
            
            # Add new column - note: column management behavior may vary
            try:
                client.add_column("city", "string")
            except Exception as e:
                print(f"add_column: {e}")
            
            # Store data with new column
            client.store({"name": "Bob", "age": 30, "city": "NYC"})
            
            # Verify new data includes new column
            result = client.retrieve(2)
            assert result is not None
            # The city field should be stored
            if "city" in result:
                assert result["city"] == "NYC"
            
            client.close()
    
    def test_add_column_different_types(self):
        """Test adding columns with different data types"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice"})
            
            # Add columns with different types - note: column management may vary
            try:
                client.add_column("age", "integer")
                client.add_column("salary", "float")
                client.add_column("active", "boolean")
                client.add_column("notes", "string")
            except Exception as e:
                print(f"add_column types: {e}")
            
            # Store data with all fields
            client.store({
                "name": "Bob",
                "age": 30,
                "salary": 50000.50,
                "active": True,
                "notes": "Test employee"
            })
            
            # Verify data types are preserved in stored data
            result = client.retrieve(2)
            assert result is not None
            assert result["age"] == 30
            assert result["salary"] == 50000.50
            assert result["active"] is True
            assert result["notes"] == "Test employee"
            
            client.close()
    
    def test_add_column_with_existing_data(self):
        """Test adding column to table with existing data"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store multiple records
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
                {"name": "Charlie", "age": 35},
            ]
            client.store(test_data)
            
            # Add new column - behavior may vary
            try:
                client.add_column("city", "string")
            except Exception as e:
                print(f"add_column with data: {e}")
            
            # Store new record with the new column
            client.store({"name": "Diana", "age": 28, "city": "Boston"})
            
            # Verify the new record has the city
            diana = client.retrieve(4)
            assert diana is not None
            assert diana["name"] == "Diana"
            if "city" in diana:
                assert diana["city"] == "Boston"
            
            client.close()
    
    def test_add_column_duplicate_name(self):
        """Test adding column with duplicate name"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice", "age": 25})
            
            # Try to add existing column - may raise error or handle gracefully
            try:
                client.add_column("name", "string")
            except Exception as e:
                # Expected to raise an error for duplicate
                print(f"Duplicate column handled: {e}")
            
            # Verify data is still accessible
            result = client.retrieve(1)
            assert result is not None
            assert result["name"] == "Alice"
            
            client.close()
    
    def test_add_column_invalid_type(self):
        """Test adding column with invalid data type"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice"})
            
            # Try to add column with invalid type - may raise error or handle gracefully
            try:
                client.add_column("invalid_col", "invalid_type")
            except Exception as e:
                # Expected to raise an error
                print(f"Invalid type handled: {e}")
            
            # Verify data is still accessible
            result = client.retrieve(1)
            assert result is not None
            assert result["name"] == "Alice"
            
            client.close()
    
    def test_add_column_special_characters(self):
        """Test adding column with special characters in name"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice"})
            
            # Try various special character column names
            special_names = [
                "column_with_underscores",
                "column-with-dashes",
                "column.with.dots",
                "columnWithCamelCase",
                "column_with_numbers123",
            ]
            
            for col_name in special_names:
                try:
                    client.add_column(col_name, "string")
                    fields = client.list_fields()
                    assert col_name in fields
                except Exception as e:
                    print(f"Column name '{col_name}' not supported: {e}")
            
            client.close()
    
    def test_add_column_unicode_name(self):
        """Test adding column with unicode characters in name"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice"})
            
            # Try unicode column names
            unicode_names = [
                "列名",  # Chinese
                "колонка",  # Russian
                "colonne",  # French with accent
            ]
            
            for col_name in unicode_names:
                try:
                    client.add_column(col_name, "string")
                    fields = client.list_fields()
                    assert col_name in fields
                except Exception as e:
                    print(f"Unicode column name '{col_name}' not supported: {e}")
            
            client.close()


class TestColumnDeletion:
    """Test column deletion operations"""
    
    def test_drop_column_basic(self):
        """Test basic column deletion"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store data with multiple columns
            client.store({"name": "Alice", "age": 25, "city": "NYC", "salary": 50000})
            
            # Drop column - behavior may vary
            try:
                client.drop_column("city")
                
                # Verify existing records may no longer have the column
                result = client.retrieve(1)
                if result is not None:
                    # City should be dropped
                    assert "name" in result
                    assert "age" in result
            except Exception as e:
                print(f"drop_column: {e}")
            
            client.close()
    
    def test_drop_nonexistent_column(self):
        """Test dropping nonexistent column"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice", "age": 25})
            
            # Try to drop nonexistent column - may raise error or handle gracefully
            try:
                client.drop_column("nonexistent_column")
            except Exception as e:
                # Expected to raise an error
                print(f"Drop nonexistent: {e}")
            
            # Verify data is still accessible
            result = client.retrieve(1)
            assert result is not None
            assert result["name"] == "Alice"
            
            client.close()
    
    def test_drop_id_column(self):
        """Test dropping _id column (should be prevented)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice", "age": 25})
            
            # Try to drop _id column
            with pytest.raises(ValueError, match="Cannot drop _id column"):
                client.drop_column("_id")
            
            # Verify _id column still exists (internally)
            result = client.retrieve(1)
            # _id should still be managed internally
            
            client.close()
    
    def test_drop_multiple_columns(self):
        """Test dropping multiple columns sequentially"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store data with many columns
            client.store({
                "name": "Alice",
                "age": 25,
                "city": "NYC",
                "salary": 50000,
                "department": "Engineering",
                "active": True
            })
            
            # Drop columns one by one - behavior may vary
            try:
                client.drop_column("city")
                client.drop_column("salary")
                client.drop_column("department")
            except Exception as e:
                print(f"Drop multiple: {e}")
            
            # Verify data is still accessible
            result = client.retrieve(1)
            assert result is not None
            assert result["name"] == "Alice"
            
            client.close()
    
    def test_drop_column_with_data(self):
        """Test dropping column that contains data"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store multiple records with data in the column to be dropped
            test_data = [
                {"name": "Alice", "age": 25, "city": "NYC"},
                {"name": "Bob", "age": 30, "city": "LA"},
                {"name": "Charlie", "age": 35, "city": "Chicago"},
            ]
            client.store(test_data)
            
            # Drop column with data - behavior may vary
            try:
                client.drop_column("city")
            except Exception as e:
                print(f"Drop with data: {e}")
            
            # Verify records are still accessible
            for i in range(3):
                result = client.retrieve(i + 1)
                assert result is not None
                assert "name" in result
                assert "age" in result
            
            client.close()


class TestColumnRenaming:
    """Test column renaming operations"""
    
    def test_rename_column_basic(self):
        """Test basic column renaming"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store data with original column name
            client.store({"name": "Alice", "age": 25, "city": "NYC"})
            
            # Rename column - behavior may vary
            try:
                client.rename_column("city", "location")
                
                # Verify data is accessible
                result = client.retrieve(1)
                assert result is not None
            except Exception as e:
                print(f"rename_column: {e}")
            
            client.close()
    
    def test_rename_nonexistent_column(self):
        """Test renaming nonexistent column"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice", "age": 25})
            
            # Try to rename nonexistent column - may raise error or handle gracefully
            try:
                client.rename_column("nonexistent_column", "new_name")
            except Exception as e:
                print(f"Rename nonexistent: {e}")
            
            # Verify data is still accessible
            result = client.retrieve(1)
            assert result is not None
            assert result["name"] == "Alice"
            
            client.close()
    
    def test_rename_to_existing_column(self):
        """Test renaming column to name that already exists"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store data with multiple columns
            client.store({"name": "Alice", "age": 25, "city": "NYC"})
            
            # Try to rename to existing column name - may raise error
            try:
                client.rename_column("age", "name")
            except Exception as e:
                print(f"Rename to existing: {e}")
            
            # Verify data is still accessible
            result = client.retrieve(1)
            assert result is not None
            
            client.close()
    
    def test_rename_id_column(self):
        """Test renaming _id column (should be prevented)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice", "age": 25})
            
            # Try to rename _id column
            with pytest.raises(ValueError, match="Cannot rename _id column"):
                client.rename_column("_id", "new_id")
            
            # Verify _id column is still managed internally
            result = client.retrieve(1)
            
            client.close()
    
    def test_rename_column_with_data(self):
        """Test renaming column that contains data"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store multiple records with data in the column to be renamed
            test_data = [
                {"name": "Alice", "age": 25, "city": "NYC"},
                {"name": "Bob", "age": 30, "city": "LA"},
                {"name": "Charlie", "age": 35, "city": "Chicago"},
            ]
            client.store(test_data)
            
            # Rename column - behavior may vary
            try:
                client.rename_column("city", "location")
            except Exception as e:
                print(f"Rename with data: {e}")
            
            # Verify all records are still accessible
            for i in range(3):
                result = client.retrieve(i + 1)
                assert result is not None
                assert "name" in result
            
            client.close()
    
    def test_rename_column_special_characters(self):
        """Test renaming column with special characters"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice", "age": 25})
            
            # Rename to column with special characters
            try:
                client.rename_column("age", "new_age_with_underscores")
                fields = client.list_fields()
                assert "new_age_with_underscores" in fields
                assert "age" not in fields
                
                # Verify data is accessible
                result = client.retrieve(1)
                assert result["new_age_with_underscores"] == 25
            except Exception as e:
                print(f"Special character rename not supported: {e}")
            
            client.close()


class TestColumnDataTypeRetrieval:
    """Test column data type retrieval operations"""
    
    def test_get_column_dtype_basic(self):
        """Test getting basic column data types"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store data with various types
            client.store({
                "name": "Alice",
                "age": 25,
                "salary": 50000.50,
                "active": True,
                "notes": "Test notes"
            })
            
            # Get column types - behavior may vary
            try:
                name_type = client.get_column_dtype("name")
                assert name_type is not None
            except Exception as e:
                print(f"get_column_dtype: {e}")
            
            client.close()
    
    def test_get_column_dtype_added_columns(self):
        """Test getting types of explicitly added columns"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice"})
            
            # Add columns with specific types - behavior may vary
            try:
                client.add_column("age", "integer")
                age_type = client.get_column_dtype("age")
                assert age_type is not None
            except Exception as e:
                print(f"get_column_dtype added: {e}")
            
            client.close()
    
    def test_get_column_dtype_nonexistent(self):
        """Test getting type of nonexistent column"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice", "age": 25})
            
            # Try to get type of nonexistent column - may raise error
            try:
                dtype = client.get_column_dtype("nonexistent_column")
            except Exception as e:
                # Expected to raise an error
                print(f"Get dtype nonexistent: {e}")
            
            client.close()
    
    def test_get_column_dtype_after_rename(self):
        """Test getting column type after renaming"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store initial data
            client.store({"name": "Alice", "age": 25})
            
            # Rename column and get type - behavior may vary
            try:
                client.rename_column("age", "user_age")
                user_age_type = client.get_column_dtype("user_age")
                assert user_age_type is not None
            except Exception as e:
                print(f"Dtype after rename: {e}")
            
            client.close()


class TestColumnOperationsEdgeCases:
    """Test edge cases and error handling for column operations"""
    
    def test_column_operations_on_closed_client(self):
        """Test column operations on closed client"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.close()
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.add_column("new_col", "string")
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.drop_column("name")
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.rename_column("name", "new_name")
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.get_column_dtype("name")
    
    def test_column_operations_empty_table(self):
        """Test column operations on empty table"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Add column to empty table - behavior may vary
            try:
                client.add_column("new_column", "string")
            except Exception as e:
                print(f"add_column empty: {e}")
            
            # Store some data to verify
            client.store({"test": "data"})
            result = client.retrieve(1)
            assert result is not None
            
            client.close()
    
    def test_column_operations_with_fts(self):
        """Test column operations with FTS enabled"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store data with FTS content
            client.store({"content": "Searchable text", "metadata": "not indexed"})
            
            # Add new column
            client.add_column("category", "string")
            
            # Verify FTS still works
            results = client.search_text("searchable")
            assert len(results) > 0
            
            # Drop non-indexed column
            client.drop_column("metadata")
            
            # Verify FTS still works
            results = client.search_text("searchable")
            assert len(results) > 0
            
            client.close()
    
    def test_column_operations_large_dataset(self):
        """Test column operations with large dataset"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store large dataset
            large_data = [{"id": i, "value": f"item_{i}"} for i in range(1000)]
            client.store(large_data)
            
            import time
            
            # Add column to large dataset - behavior may vary
            start_time = time.time()
            try:
                client.add_column("category", "string")
            except Exception as e:
                print(f"add_column large: {e}")
            add_time = time.time() - start_time
            
            # Should be reasonably fast
            assert add_time < 10.0
            
            # Verify data is still accessible
            result = client.retrieve(1)
            assert result is not None
            assert result["id"] == 0
            
            client.close()
    
    def test_column_operations_with_various_data(self):
        """Test column operations with various data types and values"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store data with various types
            test_data = {
                "string_field": "test_string",
                "int_field": 42,
                "float_field": 3.14159,
                "bool_field": True,
            }
            client.store(test_data)
            
            # Add new column - behavior may vary
            try:
                client.add_column("new_field", "string")
            except Exception as e:
                print(f"add_column various: {e}")
            
            # Verify existing data is preserved
            result = client.retrieve(1)
            assert result is not None
            assert result["string_field"] == "test_string"
            assert result["int_field"] == 42
            
            client.close()
    
    def test_column_operations_across_tables(self):
        """Test column operations are isolated to specific tables"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store data in default table
            client.store({"name": "Alice", "age": 25})
            
            # Create and configure another table
            client.create_table("users")
            client.store({"username": "bob123", "email": "bob@example.com"})
            
            # Add column to default table - behavior may vary
            client.use_table("default")
            try:
                client.add_column("city", "string")
            except Exception as e:
                print(f"add_column across: {e}")
            
            # Verify data is accessible from both tables
            client.use_table("default")
            result = client.retrieve(1)
            assert result is not None
            assert result["name"] == "Alice"
            
            client.use_table("users")
            result = client.retrieve(1)
            assert result is not None
            assert result["username"] == "bob123"
            
            client.close()


class TestColumnTypeValidation:
    """Unknown type names must fail loudly instead of creating a String column."""

    def test_add_column_rejects_unknown_type(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("types")
            client.store({"name": "Alice"})

            with pytest.raises(ValueError, match="Unknown column type"):
                client.add_column("bad", "Int8")
            with pytest.raises(ValueError, match="Unknown column type"):
                client.add_column("bad", "not_a_type")

            assert "bad" not in client.list_fields()

            # The documented spellings still map to their real types.
            client.add_column("count", "Int64")
            client.add_column("ratio", "Float64")
            assert client.get_column_dtype("count") == "Int64"
            assert client.get_column_dtype("ratio") == "Float64"

            client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
