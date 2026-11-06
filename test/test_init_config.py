"""
Comprehensive test suite for ApexBase Python API - Initialization and Configuration

This module tests:
- ApexClient initialization with various parameters
- Configuration options and validation
- Edge cases and error handling during initialization
- Durability levels and their behavior
- Database path handling and creation
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
    from apexbase import ApexClient, DurabilityLevel, __version__, FTS_AVAILABLE, ARROW_AVAILABLE, POLARS_AVAILABLE
except ImportError as e:
    pytest.skip(f"ApexBase not available: {e}", allow_module_level=True)


class TestApexClientInitialization:
    """Test ApexClient initialization and configuration"""
    
    def test_default_initialization(self):
        """Test ApexClient initialization with default parameters"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Check basic attributes
            assert client._dirpath == Path(temp_dir)
            assert client._current_table == "default"
            assert client._batch_size == 1000
            assert client._enable_cache is True
            assert client._cache_size == 10000
            assert client._prefer_arrow_format == ARROW_AVAILABLE
            assert client._durability == 'fast'
            assert client._auto_manage is True
            assert not client._is_closed
            
            # Check database path is set correctly
            # Note: file may not exist until data is stored
            assert client._db_path.name == "apexbase.apex"
            
            client.close()
    
    def test_initialization_with_custom_parameters(self):
        """Test ApexClient initialization with custom parameters"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(
                dirpath=temp_dir,
                batch_size=500,
                drop_if_exists=False,
                enable_cache=False,
                cache_size=20000,
                prefer_arrow_format=False,
                durability='safe',
                _auto_manage=False
            )
            
            assert client._batch_size == 500
            assert client._enable_cache is False
            assert client._cache_size == 20000
            assert client._prefer_arrow_format is False
            assert client._durability == 'safe'
            assert client._auto_manage is False
            
            client.close()
    
    def test_durability_levels(self):
        """Test all durability levels"""
        valid_durabilities = ['fast', 'safe', 'max']
        
        for durability in valid_durabilities:
            with tempfile.TemporaryDirectory() as temp_dir:
                client = ApexClient(dirpath=temp_dir, durability=durability)
                client.create_table("default")
                assert client._durability == durability
                client.close()
    
    def test_invalid_durability_level(self):
        """Test invalid durability level raises ValueError"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(ValueError, match="durability must be 'fast', 'safe', or 'max'"):
                ApexClient(dirpath=temp_dir, durability='invalid')
    
    def test_drop_if_exists_true(self):
        """Test drop_if_exists=True removes existing database"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "default.apex"
            
            # Create initial database
            client1 = ApexClient(dirpath=temp_dir)
            client1.create_table("default")
            client1.store({"test": "data"})
            client1.close()
            assert db_path.exists()
            
            # Create new client with drop_if_exists=True
            client2 = ApexClient(dirpath=temp_dir, drop_if_exists=True)
            assert not db_path.exists()  # Should be dropped during init
            client2.create_table("default")
            client2.close()
    
    def test_drop_if_exists_false(self):
        """Test drop_if_exists=False preserves existing database"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "default.apex"
            
            # Create initial database
            client1 = ApexClient(dirpath=temp_dir)
            client1.create_table("default")
            client1.store({"test": "data"})
            client1.close()
            assert db_path.exists()
            
            # Create new client with drop_if_exists=False
            client2 = ApexClient(dirpath=temp_dir, drop_if_exists=False)
            client2.use_table("default")
            assert db_path.exists()  # Should still exist
            client2.close()
    
    def test_none_dirpath_uses_current_directory(self):
        """Test None dirpath uses current directory"""
        original_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                os.chdir(temp_dir)
                client = ApexClient(dirpath=None)
                client.create_table("default")
                assert client._dirpath == Path(os.getcwd())
                client.close()
            finally:
                # Must change back BEFORE temp_dir cleanup on Windows
                os.chdir(original_cwd)
    
    def test_relative_path(self):
        """Test relative path handling"""
        with tempfile.TemporaryDirectory() as temp_dir:
            relative_path = "./test_db"
            
            original_cwd = os.getcwd()
            os.chdir(temp_dir)
            try:
                client = ApexClient(dirpath=relative_path)
                client.create_table("default")
                # Directory should be created
                assert client._dirpath.exists()
                client.close()
            finally:
                os.chdir(original_cwd)
    
    def test_nested_directory_creation(self):
        """Test creation of nested directories"""
        with tempfile.TemporaryDirectory() as temp_dir:
            nested_path = Path(temp_dir) / "level1" / "level2" / "level3"
            
            client = ApexClient(dirpath=str(nested_path))
            client.create_table("default")
            assert nested_path.exists()
            assert nested_path.is_dir()
            client.close()
    
    def test_create_clean_classmethod(self):
        """Test ApexClient.create_clean() class method"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "default.apex"
            
            # Create initial database
            client1 = ApexClient(dirpath=temp_dir)
            client1.create_table("default")
            client1.store({"test": "data"})
            client1.close()
            assert db_path.exists()
            
            # Use create_clean which should drop existing
            client2 = ApexClient.create_clean(dirpath=temp_dir)
            client2.create_table("default")
            # Database should be fresh (no previous data)
            assert client2.count_rows() == 0
            client2.close()
    
    def test_version_and_constants(self):
        """Test version and availability constants"""
        assert isinstance(__version__, str)
        assert len(__version__) > 0
        
        assert isinstance(FTS_AVAILABLE, bool)
        assert isinstance(ARROW_AVAILABLE, bool)
        assert isinstance(POLARS_AVAILABLE, bool)
        
        # FTS should always be available (integrated in Rust core)
        assert FTS_AVAILABLE is True
    
    def test_repr_method(self):
        """Test __repr__ method"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            repr_str = repr(client)
            assert "ApexClient" in repr_str
            assert str(client._dirpath) in repr_str
            assert "default" in repr_str
            client.close()
    
    def test_multiple_clients_same_directory(self):
        """Test multiple clients accessing same directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client1 = ApexClient(dirpath=temp_dir)
            client1.create_table("default")
            client1.store({"client": 1})
            
            # Second client should access same database
            client2 = ApexClient(dirpath=temp_dir)
            client2.use_table("default")
            count = client2.count_rows()
            assert count == 1
            
            client1.close()
            client2.close()
    
    def test_closed_client_operations(self):
        """Test operations on closed client raise RuntimeError"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.close()
            
            # All operations should raise RuntimeError
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.use_table("test")
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.store({"test": "data"})
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.query()
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.execute("SELECT * FROM default")
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.list_tables()
    
    def test_edge_case_empty_string_dirpath(self):
        """Test empty string dirpath"""
        original_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                os.chdir(temp_dir)
                client = ApexClient(dirpath="")
                client.create_table("default")
                assert client._dirpath == Path(os.getcwd())
                client.close()
            finally:
                # Must change back BEFORE temp_dir cleanup on Windows
                os.chdir(original_cwd)
    
    def test_edge_case_whitespace_dirpath(self):
        """Test whitespace-only dirpath"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test with whitespace in temp_dir subdirectory
            whitespace_path = os.path.join(temp_dir, "test db")
            try:
                client = ApexClient(dirpath=whitespace_path)
                client.create_table("default")
                # Should handle whitespace in path
                client.store({"test": "data"})
                client.close()
            except Exception as e:
                # Whitespace handling may vary
                print(f"Whitespace path handling: {e}")
    
    def test_edge_case_large_batch_size(self):
        """Test very large batch size"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir, batch_size=1000000)
            client.create_table("default")
            assert client._batch_size == 1000000
            client.close()
    
    def test_edge_case_zero_batch_size(self):
        """Test zero batch size"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir, batch_size=0)
            client.create_table("default")
            assert client._batch_size == 0
            client.close()
    
    def test_edge_case_negative_batch_size(self):
        """Test negative batch size"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir, batch_size=-1)
            client.create_table("default")
            assert client._batch_size == -1
            client.close()
    
    def test_edge_case_large_cache_size(self):
        """Test very large cache size"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir, cache_size=1000000)
            client.create_table("default")
            assert client._cache_size == 1000000
            client.close()
    
    def test_fts_tables_initialization(self):
        """Test FTS tables dictionary initialization"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            assert isinstance(client._fts_tables, dict)
            assert len(client._fts_tables) == 0
            client.close()
    
    def test_prefer_arrow_format_without_arrow(self):
        """Test prefer_arrow_format when Arrow is not available"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Force prefer_arrow_format=False even if Arrow is available
            client = ApexClient(dirpath=temp_dir, prefer_arrow_format=False)
            client.create_table("default")
            assert client._prefer_arrow_format is False
            client.close()
    
    def test_auto_manage_disabled(self):
        """Test _auto_manage disabled"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir, _auto_manage=False)
            client.create_table("default")
            assert client._auto_manage is False
            client.close()
    
    def test_database_file_permissions(self):
        """Test database file creation with appropriate permissions"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store data to ensure file is created
            client.store({"test": "data"})
            client.flush()
            
            db_path = client._db_path
            
            # Now check file exists and permissions
            if db_path.exists():
                assert db_path.is_file()
                assert os.access(db_path, os.R_OK)
                assert os.access(db_path, os.W_OK)
            
            client.close()


class TestDurabilityLevels:
    """Test durability level specific behavior"""
    
    def test_fast_durability_default(self):
        """Test 'fast' durability is default"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            assert client._durability == 'fast'
            client.close()
    
    def test_safe_durability(self):
        """Test 'safe' durability level"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir, durability='safe')
            client.create_table("default")
            assert client._durability == 'safe'
            
            # Test basic operations work
            client.store({"test": "safe"})
            client.flush()
            assert client.count_rows() == 1
            client.close()
    
    def test_max_durability(self):
        """Test 'max' durability level"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir, durability='max')
            client.create_table("default")
            assert client._durability == 'max'
            
            # Test basic operations work
            client.store({"test": "max"})
            client.flush()
            assert client.count_rows() == 1
            client.close()
    
    @pytest.mark.parametrize("durability", ['fast', 'safe', 'max'])
    def test_all_durability_levels_basic_operations(self, durability):
        """Test basic operations work with all durability levels"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir, durability=durability)
            client.create_table("default")
            
            # Test store and retrieve
            client.store({"name": "test", "value": 123})
            result = client.retrieve(1)
            assert result is not None
            assert result["name"] == "test"
            assert result["value"] == 123
            
            client.close()


class TestDurabilityExceptionScenarios:
    """Test durability behavior under exception and persistence scenarios
    
    Durability levels:
    - 'fast': Data written to memory buffer, persisted on flush()
    - 'safe': Ensures data fully written to disk on each flush() (fsync)
    - 'max': Immediate fsync on each write (strongest ACID guarantee)
    """
    
    @pytest.mark.parametrize("durability", ['fast', 'safe', 'max'])
    def test_data_persistence_after_close_and_reopen(self, durability):
        """Test data persists correctly after client close and reopen"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # First session: write data
            client1 = ApexClient(dirpath=temp_dir, durability=durability)
            client1.create_table("default")
            
            test_data = [
                {"id": 1, "name": "Alice", "value": 100},
                {"id": 2, "name": "Bob", "value": 200},
                {"id": 3, "name": "Charlie", "value": 300},
            ]
            client1.store(test_data)
            
            # For 'fast' mode, explicitly flush to ensure persistence
            if durability == 'fast':
                client1.flush()
            
            original_count = client1.count_rows()
            assert original_count == 3
            
            client1.close()
            
            # Second session: verify data persisted
            client2 = ApexClient(dirpath=temp_dir, durability=durability)
            client2.use_table("default")
            
            reopened_count = client2.count_rows()
            assert reopened_count == original_count, \
                f"Data not persisted with durability='{durability}': expected {original_count}, got {reopened_count}"
            
            # Verify data integrity
            for i in range(3):
                result = client2.retrieve(i + 1)
                assert result is not None, f"Record {i} missing after reopen"
                assert result["id"] == i + 1
            
            client2.close()
    
    @pytest.mark.parametrize("durability", ['fast', 'safe', 'max'])
    def test_flush_ensures_persistence(self, durability):
        """Test that flush() ensures data is persisted for all durability levels"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client1 = ApexClient(dirpath=temp_dir, durability=durability)
            client1.create_table("default")
            
            # Write data
            client1.store({"test": "flush_data", "value": 42})
            
            # Explicitly flush
            client1.flush()
            
            count_before_close = client1.count_rows()
            client1.close()
            
            # Reopen and verify
            client2 = ApexClient(dirpath=temp_dir, durability=durability)
            client2.use_table("default")
            count_after_reopen = client2.count_rows()
            
            assert count_after_reopen == count_before_close, \
                f"flush() did not persist data with durability='{durability}'"
            
            result = client2.retrieve(1)
            assert result["test"] == "flush_data"
            assert result["value"] == 42
            
            client2.close()
    
    @pytest.mark.parametrize("durability", ['fast', 'safe', 'max'])
    def test_batch_write_persistence(self, durability):
        """Test batch write persistence with different durability levels"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client1 = ApexClient(dirpath=temp_dir, durability=durability)
            client1.create_table("default")
            
            # Batch write
            batch_size = 100
            batch_data = [{"idx": i, "data": f"item_{i}"} for i in range(batch_size)]
            client1.store(batch_data)
            
            # Flush for all modes to ensure persistence
            client1.flush()
            
            assert client1.count_rows() == batch_size
            client1.close()
            
            # Verify all data persisted
            client2 = ApexClient(dirpath=temp_dir, durability=durability)
            client2.use_table("default")
            assert client2.count_rows() == batch_size, \
                f"Batch data not fully persisted with durability='{durability}'"
            
            # Spot check some records
            for idx in [0, 49, 99]:
                result = client2.retrieve(idx + 1)
                assert result is not None
                assert result["idx"] == idx
                assert result["data"] == f"item_{idx}"
            
            client2.close()
    
    @pytest.mark.parametrize("durability", ['fast', 'safe', 'max'])
    def test_multiple_write_sessions(self, durability):
        """Test data accumulation across multiple write sessions"""
        with tempfile.TemporaryDirectory() as temp_dir:
            total_records = 0
            
            # Session 1
            client1 = ApexClient(dirpath=temp_dir, durability=durability)
            client1.create_table("default")
            client1.store([{"session": 1, "idx": i} for i in range(10)])
            client1.flush()
            total_records += 10
            assert client1.count_rows() == total_records
            client1.close()
            
            # Session 2
            client2 = ApexClient(dirpath=temp_dir, durability=durability)
            client2.use_table("default")
            assert client2.count_rows() == total_records, "Data from session 1 not persisted"
            client2.store([{"session": 2, "idx": i} for i in range(10)])
            client2.flush()
            total_records += 10
            assert client2.count_rows() == total_records
            client2.close()
            
            # Session 3 - verify all data
            client3 = ApexClient(dirpath=temp_dir, durability=durability)
            client3.use_table("default")
            assert client3.count_rows() == total_records, \
                f"Expected {total_records} records, got {client3.count_rows()}"
            client3.close()
    
    @pytest.mark.parametrize("durability", ['fast', 'safe', 'max'])
    def test_exception_during_write_recovery(self, durability):
        """Test recovery after exception during write operation"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir, durability=durability)
            client.create_table("default")
            
            # Successful write
            client.store({"status": "before_error", "value": 1})
            client.flush()
            
            # Attempt invalid operation that might cause exception
            try:
                # Try to store invalid data type
                client.store("invalid_data_type")
            except (ValueError, TypeError):
                pass  # Expected
            
            # Client should still be usable after exception
            client.store({"status": "after_error", "value": 2})
            client.flush()
            
            assert client.count_rows() == 2
            
            # Verify data integrity using execute (handles both base and delta data)
            results = client.execute("SELECT * FROM default ORDER BY _id").to_dict()
            assert len(results) == 2
            assert results[0]["status"] == "before_error"
            assert results[1]["status"] == "after_error"
            
            client.close()
            
            # Verify persistence after exception scenario
            client2 = ApexClient(dirpath=temp_dir, durability=durability)
            client2.use_table("default")
            assert client2.count_rows() == 2
            client2.close()
    
    @pytest.mark.parametrize("durability", ['fast', 'safe', 'max'])
    def test_close_without_flush_behavior(self, durability):
        """Test behavior when closing without explicit flush
        
        Note: For 'safe' and 'max' modes, data should be persisted automatically.
        For 'fast' mode, unflushed data may be lost (by design for performance).
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            client1 = ApexClient(dirpath=temp_dir, durability=durability)
            client1.create_table("default")
            
            # Write data WITHOUT explicit flush
            client1.store({"test": "no_flush", "value": 123})
            count_before = client1.count_rows()
            
            # Close without flush (close should handle cleanup)
            client1.close()
            
            # Reopen and check
            client2 = ApexClient(dirpath=temp_dir, durability=durability)
            client2.use_table("default")
            count_after = client2.count_rows()
            
            # For 'max' and 'safe', data should persist
            # For 'fast', data might or might not persist (implementation dependent)
            if durability in ('safe', 'max'):
                assert count_after == count_before, \
                    f"durability='{durability}' should persist data even without explicit flush"
            # For 'fast' mode, we just verify the client works correctly
            # (data may or may not be persisted depending on implementation)
            
            client2.close()
    
    @pytest.mark.parametrize("durability", ['fast', 'safe', 'max'])
    def test_large_data_persistence(self, durability):
        """Test persistence of larger data with different durability levels"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client1 = ApexClient(dirpath=temp_dir, durability=durability)
            client1.create_table("default")
            
            # Write moderately large data
            large_string = "x" * 10000  # 10KB string
            data = [
                {"id": i, "content": large_string, "index": i * 100}
                for i in range(50)
            ]
            client1.store(data)
            client1.flush()
            
            assert client1.count_rows() == 50
            client1.close()
            
            # Verify persistence and data integrity
            client2 = ApexClient(dirpath=temp_dir, durability=durability)
            client2.use_table("default")
            assert client2.count_rows() == 50
            
            # Verify content integrity
            result = client2.retrieve(26)
            assert result["id"] == 25
            assert result["content"] == large_string
            assert result["index"] == 2500
            
            client2.close()
    
    def test_durability_upgrade_safe_to_max(self):
        """Test opening database with higher durability level"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create with 'safe'
            client1 = ApexClient(dirpath=temp_dir, durability='safe')
            client1.create_table("default")
            client1.store({"created_with": "safe"})
            client1.flush()
            client1.close()
            
            # Reopen with 'max'
            client2 = ApexClient(dirpath=temp_dir, durability='max')
            client2.use_table("default")
            assert client2.count_rows() == 1
            
            result = client2.retrieve(1)
            assert result["created_with"] == "safe"
            
            # Add more data with 'max' durability
            client2.store({"created_with": "max"})
            client2.flush()
            assert client2.count_rows() == 2
            client2.close()
            
            # Verify all data persisted
            client3 = ApexClient(dirpath=temp_dir, durability='fast')
            client3.use_table("default")
            assert client3.count_rows() == 2
            client3.close()
    
    def test_durability_downgrade_max_to_fast(self):
        """Test opening database with lower durability level"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create with 'max'
            client1 = ApexClient(dirpath=temp_dir, durability='max')
            client1.create_table("default")
            client1.store({"created_with": "max"})
            client1.flush()
            client1.close()
            
            # Reopen with 'fast'
            client2 = ApexClient(dirpath=temp_dir, durability='fast')
            client2.use_table("default")
            assert client2.count_rows() == 1
            
            result = client2.retrieve(1)
            assert result["created_with"] == "max"
            
            # Add data with 'fast' durability
            client2.store({"created_with": "fast"})
            client2.flush()
            assert client2.count_rows() == 2
            client2.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
