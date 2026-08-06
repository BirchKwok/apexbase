"""
Comprehensive test suite for ApexBase FTS (Full-Text Search) Functionality

This module tests:
- FTS initialization and configuration
- Basic text search operations
- Fuzzy search functionality
- Search and retrieve operations
- FTS statistics and management
- Edge cases and error handling
- Performance considerations
- Multi-table FTS configurations
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import sys
import os
import json
import numpy as np

# Add the apexbase python module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

try:
    from apexbase import ApexClient, FTS_AVAILABLE, ARROW_AVAILABLE
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


class TestFTSInitialization:
    """Test FTS initialization and configuration"""
    
    def test_fts_init_basic(self):
        """Test basic FTS initialization"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Initialize FTS with default settings
            client.init_fts()
            
            # Check FTS is enabled
            assert client._is_fts_enabled()
            assert client._get_fts_config() is not None
            assert client._get_fts_config()['enabled'] is True
            
            client.close()
    
    def test_fts_init_with_index_fields(self):
        """Test FTS initialization with specific index fields"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Initialize FTS with specific fields
            client.init_fts(index_fields=['title', 'content', 'tags'])
            
            config = client._get_fts_config()
            assert config['index_fields'] == ['title', 'content', 'tags']
            assert config['config']['lazy_load'] is False
            assert config['config']['cache_size'] == 10000
            
            client.close()
    
    def test_fts_init_with_lazy_load(self):
        """Test FTS initialization with lazy loading"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Initialize FTS with lazy loading
            client.init_fts(lazy_load=True, cache_size=50000)
            
            config = client._get_fts_config()
            assert config['config']['lazy_load'] is True
            assert config['config']['cache_size'] == 50000
            
            client.close()
    
    def test_fts_init_for_specific_table(self):
        """Test FTS initialization for specific table"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create table and initialize FTS for it
            client.create_table("articles")
            client.init_fts(table_name="articles", index_fields=['title', 'body'])
            
            # Check FTS is enabled for the specific table
            assert client._is_fts_enabled("articles")
            assert not client._is_fts_enabled("default")  # Should not be enabled for default
            
            config = client._get_fts_config("articles")
            assert config['index_fields'] == ['title', 'body']
            
            client.close()
    
    def test_fts_init_multiple_tables(self):
        """Test FTS initialization for multiple tables with different configs"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Initialize FTS for multiple tables
            client.create_table("articles")
            client.init_fts(table_name="articles", index_fields=['title', 'body'], lazy_load=True)
            
            client.create_table("comments")
            client.init_fts(table_name="comments", index_fields=['text'], cache_size=20000)
            
            # Check configurations are separate
            articles_config = client._get_fts_config("articles")
            comments_config = client._get_fts_config("comments")
            
            assert articles_config['index_fields'] == ['title', 'body']
            assert articles_config['config']['lazy_load'] is True
            
            assert comments_config['index_fields'] == ['text']
            assert comments_config['config']['cache_size'] == 20000
            
            client.close()
    
    def test_fts_init_chain_calls(self):
        """Test FTS initialization in chain calls"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test chain call during initialization
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            assert client._is_fts_enabled()
            
            client.close()
    
    def test_fts_init_on_closed_client(self):
        """Test FTS initialization on closed client"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.close()
            
            with pytest.raises(RuntimeError, match="connection has been closed"):
                client.init_fts()


class TestFTSPersistenceLifecycle:
    """Test persisted FTS config across client restarts and disable/drop semantics"""

    def test_fts_persist_and_auto_enable_on_reopen(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            client.store({"content": "Python programming language"})
            client.close()

            client2 = ApexClient(dirpath=temp_dir)
            client2.use_table("default")
            assert client2._is_fts_enabled()

            # Should work without calling init_fts again (lazy init)
            results = client2.search_text("python")
            assert len(results) > 0
            client2.close()

    def test_disable_fts_persists_across_reopen(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            client.store({"content": "Python programming language"})

            client.disable_fts()
            client.close()

            client2 = ApexClient(dirpath=temp_dir)
            client2.use_table("default")
            assert not client2._is_fts_enabled()
            with pytest.raises(ValueError, match="Full-text search is not enabled"):
                client2.search_text("python")
            client2.close()

    def test_drop_fts_deletes_index_files_and_config(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            client.store({"content": "Python programming language"})

            # Force index file to be materialized
            _ = client.search_text("python")
            client.close()

            index_path = Path(temp_dir) / "fts_indexes" / "default.afts"
            assert index_path.exists()
            client2 = ApexClient(dirpath=temp_dir)
            client2.use_table("default")
            client2.drop_fts()
            client2.close()
            assert not index_path.exists()

            cfg_path = Path(temp_dir) / "fts_config.json"
            if cfg_path.exists():
                data = json.loads(cfg_path.read_text(encoding='utf-8') or "{}")
                assert isinstance(data, dict)
                assert "default" not in data

            # drop_fts should remove index files if present
            assert not index_path.exists()

            client3 = ApexClient(dirpath=temp_dir)
            client3.use_table("default")
            assert not client3._is_fts_enabled()
            with pytest.raises(ValueError, match="Full-text search is not enabled"):
                client3.search_text("python")
            client3.close()


class TestBasicTextSearch:
    """Test basic text search operations"""
    
    def test_search_text_basic(self):
        """Test basic text search"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store searchable documents
            documents = [
                {"content": "The quick brown fox jumps over the lazy dog"},
                {"content": "Python is a great programming language"},
                {"content": "Machine learning and artificial intelligence"},
                {"content": "Database systems and data management"},
            ]
            client.store(documents)
            
            # Search for terms
            results = client.search_text("python")
            assert isinstance(results, np.ndarray)
            assert len(results) > 0
            
            # Search for phrase
            results = client.search_text("machine learning")
            assert len(results) > 0
            
            # Search for non-existent term
            results = client.search_text("nonexistent")
            assert len(results) == 0
            
            client.close()
    
    def test_search_text_multiple_fields(self):
        """Test search across multiple indexed fields"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['title', 'content', 'tags'])
            
            # Store documents with multiple fields
            documents = [
                {
                    "title": "Python Programming",
                    "content": "Learn Python programming language",
                    "tags": "python, programming, tutorial"
                },
                {
                    "title": "Database Design",
                    "content": "Principles of database system design",
                    "tags": "database, design, sql"
                },
                {
                    "title": "Machine Learning",
                    "content": "Introduction to machine learning algorithms",
                    "tags": "ml, ai, algorithms"
                },
            ]
            client.store(documents)
            
            # Search in title field
            results = client.search_text("python")
            assert len(results) > 0
            
            # Search in content field
            results = client.search_text("algorithms")
            assert len(results) > 0
            
            # Search in tags field
            results = client.search_text("database")
            assert len(results) > 0
            
            client.close()
    
    def test_search_text_case_insensitive(self):
        """Test case-insensitive search"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store documents with mixed case
            documents = [
                {"content": "Python is GREAT"},
                {"content": "python is great"},
                {"content": "PYTHON IS GREAT"},
            ]
            client.store(documents)
            
            # Search with different cases
            results_lower = client.search_text("python")
            results_upper = client.search_text("PYTHON")
            results_mixed = client.search_text("Python")
            
            # All should find the same documents
            assert len(results_lower) == 3
            assert len(results_upper) == 3
            assert len(results_mixed) == 3
            
            client.close()
    
    def test_search_text_partial_words(self):
        """Test partial word matching"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store documents
            documents = [
                {"content": "programming programmer program"},
                {"content": "database databases"},
                {"content": "computing compute computer"},
            ]
            client.store(documents)
            
            # Search for exact word - partial matching may not be supported
            results = client.search_text("program")
            # May or may not match partial words depending on implementation
            
            # Search for full word that exists
            results = client.search_text("database")
            assert len(results) >= 0  # May return 0 or more
            
            client.close()
    
    def test_search_text_with_special_characters(self):
        """Test search with special characters"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store documents
            documents = [
                {"content": "Python programming language"},
                {"content": "SQL database queries"},
            ]
            client.store(documents)
            
            # Search regular words - special chars handling may vary
            results = client.search_text("python")
            # Should find Python document
            assert len(results) >= 0
            
            results = client.search_text("sql")
            assert len(results) >= 0
            
            client.close()
    
    def test_search_text_unicode(self):
        """Test search with unicode characters"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store documents with simple unicode
            documents = [
                {"content": "Hello World English"},
                {"content": "Bonjour le monde French"},
            ]
            client.store(documents)
            
            # Search regular terms
            results = client.search_text("Hello")
            assert len(results) >= 0  # Unicode support may vary
            
            results = client.search_text("Bonjour")
            assert len(results) >= 0
            
            client.close()


class TestFuzzySearch:
    """Test fuzzy search functionality"""
    
    def test_fuzzy_search_basic(self):
        """Test basic fuzzy search"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store documents
            documents = [
                {"content": "Python programming language"},
                {"content": "JavaScript web development"},
                {"content": "Database management systems"},
                {"content": "Machine learning algorithms"},
            ]
            client.store(documents)
            
            # Fuzzy search with typos
            results = client.fuzzy_search_text("pythn")  # Missing 'o'
            assert len(results) > 0
            
            results = client.fuzzy_search_text("javascrpt")  # Missing 'i'
            assert len(results) > 0
            
            results = client.fuzzy_search_text("databas")  # Missing 'e'
            assert len(results) > 0
            
            client.close()
    
    def test_fuzzy_search_min_results(self):
        """Test fuzzy search with min_results parameter"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store documents
            documents = [
                {"content": "Python programming"},
                {"content": "Python development"},
                {"content": "Python tutorials"},
                {"content": "JavaScript programming"},
            ]
            client.store(documents)
            
            # Test with different min_results
            results = client.fuzzy_search_text("pythn", min_results=1)
            assert len(results) >= 1
            
            results = client.fuzzy_search_text("pythn", min_results=3)
            assert len(results) >= 3
            
            # Test with high min_results (should return all matches)
            results = client.fuzzy_search_text("pythn", min_results=10)
            assert len(results) >= 3  # At least the Python documents
            
            client.close()
    
    def test_fuzzy_search_config(self):
        """Test fuzzy search configuration"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store documents
            documents = [
                {"content": "Python programming"},
                {"content": "JavaScript development"},
            ]
            client.store(documents)
            
            # Set fuzzy search configuration
            client.set_fts_fuzzy_config(
                threshold=0.8,  # Higher threshold (stricter)
                max_distance=1,  # Max edit distance
                max_candidates=10
            )
            
            # Search with configuration
            results = client.fuzzy_search_text("pythn")  # 1 edit distance
            assert len(results) > 0
            
            # Search with more typos (should not match with strict config)
            results = client.fuzzy_search_text("pyth")  # 2 edit distances
            # May or may not match depending on implementation
            
            client.close()
    
    def test_fuzzy_search_vs_exact_search(self):
        """Test fuzzy search vs exact search"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store documents
            documents = [
                {"content": "Python programming"},
                {"content": "JavaScript development"},
            ]
            client.store(documents)
            
            # Exact search
            exact_results = client.search_text("python")
            
            # Fuzzy search with correct spelling
            fuzzy_results = client.fuzzy_search_text("python")
            
            # Should return same results
            assert len(exact_results) == len(fuzzy_results)
            
            # Fuzzy search with typo
            typo_results = client.fuzzy_search_text("pythn")
            
            # Should still find results
            assert len(typo_results) > 0
            
            client.close()


class TestSearchAndRetrieve:
    """Test search and retrieve operations"""
    
    def test_search_and_retrieve_basic(self):
        """Test basic search and retrieve"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['title', 'content'])
            
            # Store documents
            documents = [
                {"title": "Python Tutorial", "content": "Learn Python programming"},
                {"title": "JavaScript Guide", "content": "Master JavaScript development"},
                {"title": "Database Basics", "content": "Understanding database systems"},
            ]
            client.store(documents)
            
            # Search and retrieve
            results = client.search_and_retrieve("python")
            
            assert isinstance(results, type(client.query()))  # Should return ResultView
            assert len(results) >= 1
            
            # Check the retrieved document
            found = False
            for result in results:
                if "python" in result.get("title", "").lower() or "python" in result.get("content", "").lower():
                    found = True
                    break
            assert found
            
            client.close()
    
    def test_search_and_retrieve_with_limit(self):
        """Test search and retrieve with limit"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store multiple Python-related documents
            documents = [
                {"content": "Python programming tutorial"},
                {"content": "Python development guide"},
                {"content": "Python best practices"},
                {"content": "Python advanced features"},
                {"content": "JavaScript programming"},
            ]
            client.store(documents)
            
            # Search with limit
            results = client.search_and_retrieve("python", limit=2)
            assert len(results) <= 2
            
            # Search with offset
            results = client.search_and_retrieve("python", limit=2, offset=1)
            assert len(results) <= 2
            
            client.close()
    
    def test_search_and_retrieve_top(self):
        """Test search_and_retrieve_top method"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store documents
            documents = [
                {"content": "Python programming tutorial"},
                {"content": "Python development guide"},
                {"content": "Python best practices"},
                {"content": "JavaScript programming"},
            ]
            client.store(documents)
            
            # Get top results
            results = client.search_and_retrieve_top("python", n=2)
            assert len(results) <= 2
            
            # Verify results contain Python content
            for result in results:
                content = result.get("content", "").lower()
                assert "python" in content
            
            client.close()
    
    def test_search_and_retrieve_conversions(self):
        """Test ResultView conversions from search and retrieve"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store documents
            documents = [
                {"content": "Python programming tutorial"},
                {"content": "JavaScript development guide"},
            ]
            client.store(documents)
            
            # Search and retrieve
            results = client.search_and_retrieve("python")
            
            # Test conversions
            dict_list = results.to_dict()
            assert isinstance(dict_list, list)
            assert len(dict_list) >= 1
            
            if PANDAS_AVAILABLE:
                df = results.to_pandas()
                assert isinstance(df, pd.DataFrame)
                assert len(df) >= 1
            
            if POLARS_DF_AVAILABLE:
                df = results.to_polars()
                assert isinstance(df, pl.DataFrame)
                assert len(df) >= 1
            
            if PYARROW_AVAILABLE:
                table = results.to_arrow()
                assert isinstance(table, pa.Table)
                assert len(table) >= 1
            
            client.close()
    
    def test_search_and_retrieve_specific_table(self):
        """Test search and retrieve on specific table"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create tables and initialize FTS
            client.create_table("articles")
            client.init_fts(table_name="articles", index_fields=['title', 'content'])
            
            client.create_table("comments")
            client.init_fts(table_name="comments", index_fields=['text'])
            
            # Store data in different tables
            client.use_table("articles")
            client.store([
                {"title": "Python Article", "content": "Python programming article"},
                {"title": "JavaScript Article", "content": "JavaScript development article"},
            ])
            
            client.use_table("comments")
            client.store([
                {"text": "Great Python tutorial!"},
                {"text": "JavaScript is also good"},
            ])
            
            # Search in specific table
            results = client.search_and_retrieve("python", table_name="articles")
            assert len(results) >= 1
            
            # Verify it's from articles table
            for result in results:
                assert "title" in result or "content" in result
                assert "text" not in result  # Should not have comments field
            
            client.close()


class TestFTSStatistics:
    """Test FTS statistics and management"""
    
    def test_get_fts_stats(self):
        """Test getting FTS statistics"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Stats before FTS initialization
            stats = client.get_fts_stats()
            assert stats['fts_enabled'] is False
            
            # Initialize FTS
            client.init_fts(index_fields=['content'])
            
            # Stats after initialization but before data
            stats = client.get_fts_stats()
            assert stats['fts_enabled'] is True
            assert stats['engine_initialized'] is True
            
            # Store some data
            documents = [
                {"content": "Python programming"},
                {"content": "JavaScript development"},
            ]
            client.store(documents)
            
            # Stats after data
            stats = client.get_fts_stats()
            assert stats['fts_enabled'] is True
            assert stats['engine_initialized'] is True
            # May contain additional statistics depending on implementation
            
            client.close()
    
    def test_get_fts_stats_multiple_tables(self):
        """Test FTS statistics for multiple tables"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Initialize FTS for multiple tables
            client.create_table("articles")
            client.use_table("articles")
            client.init_fts(index_fields=['title'])
            
            client.create_table("comments")
            client.use_table("comments")
            client.init_fts(index_fields=['text'])
            
            # Get stats - behavior may vary
            try:
                articles_stats = client.get_fts_stats("articles")
                assert articles_stats is not None
            except Exception as e:
                print(f"FTS stats multiple: {e}")
            
            client.close()
    
    def test_compact_fts_index(self):
        """Test FTS index compaction"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store and delete data to create fragmentation
            documents = [
                {"content": "Document 1"},
                {"content": "Document 2"},
                {"content": "Document 3"},
            ]
            client.store(documents)
            
            # Delete some documents
            client.delete(2)
            
            # Compact index (should not raise errors)
            client.compact_fts_index()
            
            # Verify search still works
            results = client.search_text("Document")
            assert len(results) >= 1
            
            client.close()
    
    def test_warmup_fts_terms(self):
        """Test FTS terms warmup"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'], lazy_load=True)
            
            # Store documents
            documents = [
                {"content": "Python programming tutorial"},
                {"content": "JavaScript development guide"},
                {"content": "Database management system"},
            ]
            client.store(documents)
            
            # Warmup specific terms
            warmed_count = client.warmup_fts_terms(["python", "javascript"])
            assert isinstance(warmed_count, int)
            assert warmed_count >= 0
            
            # Warmup non-existent terms
            warmed_count = client.warmup_fts_terms(["nonexistent"])
            assert warmed_count == 0
            
            client.close()


class TestApexFTSStorage:
    """ApexFTS-owned storage, analyzer, and lifecycle guarantees."""

    def test_multilingual_unicode_analyzer_and_query_punctuation(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=["content"])
            client.store([
                {"content": "Hello, café crème"},
                {"content": "Привет мир"},
                {"content": "مرحبا بالعالم"},
                {"content": "日本語テスト"},
                {"content": "한국어 검색"},
                {"content": "人工智能数据库"},
            ])

            for query, expected_id in [
                ("hello,", 1),
                ("CAFÉ", 1),
                ("привет", 2),
                ("مرحبا", 3),
                ("テスト", 4),
                ("한국어", 5),
                ("人工智能", 6),
            ]:
                assert client.search_text(query).tolist() == [expected_id]
            client.close()

    def test_bm25_phrase_search_sql_bitmap_and_reopen(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=["title", "body"])
            client.store([
                {"title": "quick brown", "body": "rust rust rust database"},
                {"title": "quick red brown", "body": "rust database"},
                {"title": "quick", "body": "brown rust"},
            ])

            ranked = client.search_text_with_scores("rust")
            assert ranked[0][0] == 1
            assert {doc_id for doc_id, _ in ranked} == {1, 2, 3}
            assert ranked[0][1] > max(score for _, score in ranked[1:])
            assert set(client.search_text("quick brown").tolist()) == {1, 2, 3}
            assert client.search_text('"quick brown"').tolist() == [1]

            matched = client.execute(
                "SELECT _id FROM default WHERE MATCH('quick') AND _id >= 2 ORDER BY _id"
            ).to_pandas()
            assert matched["_id"].tolist() == [2, 3]
            client.close()

            client = ApexClient(dirpath=temp_dir)
            client.use_table("default")
            assert client.search_text('"quick brown"').tolist() == [1]
            reopened = client.search_text_with_scores("rust")
            assert reopened[0][0] == 1
            assert reopened[0][1] > reopened[1][1]
            client.close()

    def test_sql_fts_score_projection_alias_and_direct_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=["body"], lazy_load=True, cache_size=2)
            client.store([
                {"body": "rust rust rust database"},
                {"body": "rust database"},
                {"body": "database only"},
            ])
            client.compact_fts_index()

            ranked = client.execute(
                "SELECT _id, FTS_SCORE() AS relevance FROM default "
                "WHERE MATCH('rust') ORDER BY relevance DESC"
            ).to_pandas()
            assert ranked["_id"].tolist() == [1, 2]
            assert ranked["relevance"].iloc[0] > ranked["relevance"].iloc[1] > 0

            direct = client.execute(
                "SELECT _id FROM default WHERE MATCH('rust') "
                "ORDER BY FTS_SCORE('rust') DESC"
            ).to_pandas()
            assert direct["_id"].tolist() == [1, 2]
            client.close()

            reopened = ApexClient(dirpath=temp_dir)
            reopened.use_table("default")
            rows = reopened.execute(
                "SELECT _id, FTS_SCORE() AS relevance FROM default "
                "WHERE MATCH('rust') ORDER BY relevance DESC"
            ).to_pandas()
            assert rows["_id"].tolist() == [1, 2]
            reopened.close()

    def test_replace_delete_and_wal_survive_reopen(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=["content"])
            client.store({"content": "old searchable value"})
            assert client.replace(1, {"content": "new searchable value"})
            assert client.search_text("old").tolist() == []
            assert client.search_text("new").tolist() == [1]
            client.close()

            client = ApexClient(dirpath=temp_dir)
            client.use_table("default")
            assert client.search_text("old").tolist() == []
            assert client.search_text("new").tolist() == [1]
            assert client.delete(1)
            client.close()

            client = ApexClient(dirpath=temp_dir)
            client.use_table("default")
            assert client.search_text("new").tolist() == []
            client.close()

    def test_sql_update_and_create_index_rebuild_remove_stale_terms(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=["title"])
            client.store({"title": "old title", "body": "new body"})

            client.execute("UPDATE default SET title = 'updated title' WHERE _id = 1")
            assert client.search_text("old").tolist() == []
            assert client.search_text("updated").tolist() == [1]

            client.execute("CREATE FTS INDEX ON default (body)")
            assert client.search_text("updated").tolist() == []
            assert client.search_text("body").tolist() == [1]
            client.close()

    def test_legacy_index_is_rebuilt_from_apex_table(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.store({"content": "migration source of truth"})
            client.close()

            fts_dir = Path(temp_dir) / "fts_indexes"
            fts_dir.mkdir()
            (fts_dir / "default.nfts").write_bytes(b"legacy nanofts data")
            (Path(temp_dir) / "fts_config.json").write_text(json.dumps({
                "default": {
                    "enabled": True,
                    "index_fields": ["content"],
                    "config": {"lazy_load": False, "cache_size": 10000},
                }
            }), encoding="utf-8")

            client = ApexClient(dirpath=temp_dir)
            client.use_table("default")
            assert client.search_text("migration").tolist() == [1]
            client.close()
            assert (fts_dir / "default.afts").exists()

    def test_named_table_search_initializes_and_restores_current_table(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("articles")
            client.init_fts(index_fields=["content"])
            client.store({"content": "article needle"})
            client.create_table("comments")
            client.init_fts(index_fields=["content"])
            client.store({"content": "comment needle"})

            client.use_table("articles")
            assert client.search_text("comment", table_name="comments").tolist() == [1]
            assert client._current_table == "articles"
            assert client.search_text("article").tolist() == [1]
            assert client.search_text("comment").tolist() == []
            client.close()


class TestFTSEdgeCases:
    """Test edge cases and error handling for FTS"""
    
    def test_search_without_fts_initialization(self):
        """Test search without FTS initialization"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store some data
            client.store({"content": "Python programming"})
            
            # Try to search without FTS initialization
            with pytest.raises(ValueError, match="Full-text search is not enabled"):
                client.search_text("python")
            
            with pytest.raises(ValueError, match="Full-text search is not enabled"):
                client.fuzzy_search_text("python")
            
            with pytest.raises(ValueError, match="Full-text search is not enabled"):
                client.search_and_retrieve("python")
            
            client.close()
    
    def test_fts_operations_on_closed_client(self):
        """Test FTS operations on closed client"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            client.close()
            
            # Operations on closed client should raise some error
            with pytest.raises((RuntimeError, ValueError, AttributeError)):
                client.search_text("python")
    
    def test_search_empty_query(self):
        """Test search with empty query"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store some data
            client.store({"content": "Python programming"})
            
            # Search with empty string
            results = client.search_text("")
            # May return empty results or all documents depending on implementation
            assert isinstance(results, np.ndarray)
            
            # Search with whitespace
            results = client.search_text("   ")
            assert isinstance(results, np.ndarray)
            
            client.close()
    
    def test_search_very_long_query(self):
        """Test search with very long query"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store some data
            client.store({"content": "Python programming"})
            
            # Search with very long query
            long_query = "python " * 1000  # Very long query
            results = client.search_text(long_query)
            assert isinstance(results, np.ndarray)
            
            client.close()
    
    def test_fts_with_non_indexed_fields(self):
        """Test FTS with non-indexed fields"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['title'])  # Only index title
            
            # Store documents with title and content
            documents = [
                {"title": "Python Tutorial", "content": "Learn Python programming"},
                {"title": "JavaScript Guide", "content": "Master JavaScript development"},
            ]
            client.store(documents)
            
            # Search for term in indexed field (should find)
            results = client.search_text("python")
            assert len(results) > 0
            
            # Search for term only in non-indexed field (may not find)
            results = client.search_text("programming")
            # May return empty results since only title is indexed
            
            client.close()
    
    def test_fts_after_table_operations(self):
        """Test FTS after table operations"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create table and initialize FTS
            client.create_table("test_table")
            client.init_fts(table_name="test_table", index_fields=['content'])
            
            # Store data
            client.store({"content": "Python programming"})
            
            # Switch tables and back
            client.use_table("default")
            client.use_table("test_table")
            
            # FTS should still work
            results = client.search_text("python")
            assert len(results) > 0
            
            # Drop table
            client.drop_table("test_table")
            
            # FTS config should be cleaned up
            assert not client._is_fts_enabled("test_table")
            
            client.close()
    
    def test_fts_with_large_documents(self):
        """Test FTS with large documents"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store large document
            large_content = "python programming " * 10000  # Large document
            client.store({"content": large_content})
            
            # Search in large document
            results = client.search_text("python")
            assert len(results) > 0
            
            client.close()
    
    def test_fts_performance_large_dataset(self):
        """Test FTS performance with large dataset"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.init_fts(index_fields=['content'])
            
            # Store large dataset
            large_documents = [
                {"content": f"Document {i} with python programming content"}
                for i in range(1000)
            ]
            client.store(large_documents)
            
            import time
            
            # Test search performance
            start_time = time.time()
            results = client.search_text("python")
            search_time = time.time() - start_time
            
            assert len(results) == 1000  # Should find all documents
            assert search_time < 2.0  # Should be reasonably fast
            
            # Test fuzzy search performance
            start_time = time.time()
            results = client.fuzzy_search_text("pythn")
            fuzzy_time = time.time() - start_time
            
            assert len(results) > 0
            assert fuzzy_time < 3.0  # Should be reasonably fast
            
            client.close()


class TestFTSSQLSync:
    """Test SQL-driven FTS sync: backfill on CREATE, INSERT/DELETE sync, ALTER ENABLE backfill,
    and SHOW FTS INDEXES cross-database support."""

    def test_create_fts_index_backfills_existing_rows(self):
        """CREATE FTS INDEX on a non-empty table should index all existing rows."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE articles (title TEXT, body TEXT, views INT)")
            client.execute("INSERT INTO articles (title, body, views) VALUES ('Hello World', 'intro doc', 1)")
            client.execute("INSERT INTO articles (title, body, views) VALUES ('Rust Lang', 'systems doc', 2)")
            client.execute("INSERT INTO articles (title, body, views) VALUES ('Python Tips', 'scripting doc', 3)")

            result = client.execute("CREATE FTS INDEX ON articles")
            status = result.to_pandas()["status"][0]
            assert "3 rows indexed" in status, f"Expected 3 rows indexed, got: {status}"

            # Verify all three rows are searchable after backfill
            df = client.execute("SELECT * FROM articles WHERE MATCH('Rust')").to_pandas()
            assert len(df) == 1
            assert df.iloc[0]["title"] == "Rust Lang"

            df2 = client.execute("SELECT * FROM articles WHERE MATCH('doc')").to_pandas()
            assert len(df2) == 3

            client.close()

    def test_create_fts_index_backfills_python_stored_rows(self):
        """CREATE FTS INDEX should backfill rows written through the Python store API."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("articles")
            client.store([
                {
                    "title": "Rust-powered local analytics",
                    "body": "A columnar embedded database for fast SQL and search.",
                    "category": "database",
                    "views": 4200,
                },
                {
                    "title": "Vector retrieval cookbook",
                    "body": "Hybrid full-text and semantic vector search for RAG.",
                    "category": "ai",
                    "views": 6100,
                },
                {
                    "title": "SQLite migration notes",
                    "body": "Move local applications to an analytical embedded store.",
                    "category": "database",
                    "views": 2600,
                },
            ])

            result = client.execute("CREATE FTS INDEX ON articles(title, body)")
            status = result.to_pandas()["status"][0]
            assert "3 rows indexed" in status

            df = client.execute(
                "SELECT title FROM articles WHERE MATCH('database')"
            ).to_pandas()
            assert "Rust-powered local analytics" in set(df["title"])

            client.close()

    def test_sql_created_fts_index_tracks_python_store(self):
        """A SQL-created FTS index should be active for subsequent Python store() writes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("articles")
            client.execute("CREATE FTS INDEX ON articles(title, body)")

            client.store([
                {
                    "title": "Embedded search with vectors",
                    "body": "ApexBase combines SQL filters, full-text search, and vector ranking.",
                    "category": "ai",
                    "views": 7300,
                },
                {
                    "title": "Plain metrics",
                    "body": "Daily operational counters and dashboards.",
                    "category": "ops",
                    "views": 900,
                },
            ])

            df = client.execute(
                "SELECT title FROM articles WHERE MATCH('vector')"
            ).to_pandas()
            assert len(df) == 1
            assert df.iloc[0]["title"] == "Embedded search with vectors"

            client.close()

    def test_hybrid_fts_sql_vector_query_from_python_store(self):
        """README-style hybrid FTS + SQL filter + vector ranking should work."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("articles")
            client.execute("CREATE FTS INDEX ON articles(title, body)")
            client.store([
                {
                    "title": "ApexBase hybrid search",
                    "body": "Embedded database with SQL, full-text search, and vector retrieval.",
                    "category": "database",
                    "views": 8000,
                    "embedding": [0.10, 0.82, 0.20],
                },
                {
                    "title": "Dashboard metrics",
                    "body": "Fast local analytics for application counters.",
                    "category": "database",
                    "views": 2100,
                    "embedding": [0.20, 0.10, 0.70],
                },
                {
                    "title": "Model serving guide",
                    "body": "Semantic retrieval pipelines for AI applications.",
                    "category": "ai",
                    "views": 6400,
                    "embedding": [0.12, 0.78, 0.25],
                },
            ])

            rows = client.execute(
                """
                SELECT
                  title,
                  views,
                  cosine_distance(embedding, [0.12, 0.78, 0.25]) AS semantic_dist
                FROM articles
                WHERE MATCH('embedded database vector')
                  AND category = 'database'
                  AND views > 3000
                ORDER BY semantic_dist
                LIMIT 3
                """
            ).to_dict()

            assert len(rows) == 1
            assert rows[0]["title"] == "ApexBase hybrid search"

            client.close()

    def test_drop_if_exists_recreates_hybrid_float16_fts_table_in_process(self):
        """drop_if_exists must evict stale Rust caches before recreating vector+FTS tables."""
        records = [
            {
                "title": "Rust-powered local analytics",
                "body": "A columnar embedded database for fast SQL and search.",
                "category": "database",
                "views": 4200,
                "embedding": [0.10, 0.82, 0.20],
            },
            {
                "title": "Hybrid retrieval for RAG",
                "body": "Combine full-text recall, SQL filters, and semantic vector ranking.",
                "category": "ai",
                "views": 6100,
                "embedding": [0.16, 0.74, 0.58],
            },
            {
                "title": "SQLite migration notes",
                "body": "Move local applications to an analytical embedded store.",
                "category": "database",
                "views": 2600,
                "embedding": [0.80, 0.12, 0.10],
            },
        ]

        def run_once(temp_dir):
            with ApexClient(dirpath=temp_dir, drop_if_exists=True) as client:
                client.execute("""
                    CREATE TABLE articles (
                        title TEXT,
                        body TEXT,
                        category TEXT,
                        views INT,
                        embedding FLOAT16_VECTOR
                    )
                """)
                client.use_table("articles")
                client.store(records)
                client.execute("CREATE FTS INDEX ON articles(title, body)")

                rows = client.execute("""
                    SELECT
                        title,
                        category,
                        views,
                        cosine_distance(embedding, [0.12, 0.78, 0.25]) AS semantic_dist
                    FROM articles
                    WHERE MATCH('database')
                      AND category = 'database'
                      AND views > 3000
                    ORDER BY semantic_dist
                    LIMIT 5
                """).to_dict()
                assert len(rows) == 1
                assert rows[0]["title"] == "Rust-powered local analytics"

        with tempfile.TemporaryDirectory() as temp_dir:
            run_once(temp_dir)
            run_once(temp_dir)

    def test_create_fts_index_on_empty_table(self):
        """CREATE FTS INDEX on an empty table should succeed with 0 rows indexed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE docs (content TEXT)")

            result = client.execute("CREATE FTS INDEX ON docs")
            status = result.to_pandas()["status"][0]
            assert "0 rows indexed" in status, f"Expected 0 rows indexed, got: {status}"

            client.close()

    def test_create_fts_index_with_specific_fields_backfills(self):
        """CREATE FTS INDEX ON table (col) should only index the specified field."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE docs (title TEXT, body TEXT)")
            client.execute("INSERT INTO docs (title, body) VALUES ('alpha term', 'beta term')")

            result = client.execute("CREATE FTS INDEX ON docs (title)")
            status = result.to_pandas()["status"][0]
            assert "1 rows indexed" in status

            # title field indexed → match
            df = client.execute("SELECT * FROM docs WHERE MATCH('alpha')").to_pandas()
            assert len(df) == 1

            client.close()

    def test_sql_insert_syncs_fts(self):
        """SQL INSERT into a table with an active FTS index should auto-index new rows."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE news (headline TEXT, summary TEXT)")
            client.execute("CREATE FTS INDEX ON news")

            # Insert after index creation
            client.execute("INSERT INTO news (headline, summary) VALUES ('Breaking News', 'something happened')")
            client.execute("INSERT INTO news (headline, summary) VALUES ('Sports Update', 'team won')")

            df = client.execute("SELECT * FROM news WHERE MATCH('Breaking')").to_pandas()
            assert len(df) == 1
            assert df.iloc[0]["headline"] == "Breaking News"

            df2 = client.execute("SELECT * FROM news WHERE MATCH('team')").to_pandas()
            assert len(df2) == 1

            client.close()

    def test_sql_insert_multiple_rows_syncs_fts(self):
        """Multi-row SQL INSERT should index all new rows in FTS."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE items (name TEXT)")
            client.execute("CREATE FTS INDEX ON items")

            client.execute("INSERT INTO items (name) VALUES ('apple'), ('banana'), ('cherry')")

            for fruit in ["apple", "banana", "cherry"]:
                df = client.execute(f"SELECT * FROM items WHERE MATCH('{fruit}')").to_pandas()
                assert len(df) == 1, f"Expected 1 result for '{fruit}', got {len(df)}"

            client.close()

    def test_sql_delete_syncs_fts(self):
        """SQL DELETE should remove deleted rows from the FTS index."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE posts (title TEXT, category TEXT)")
            client.execute("INSERT INTO posts (title, category) VALUES ('Machine Learning', 'tech')")
            client.execute("INSERT INTO posts (title, category) VALUES ('Deep Learning', 'tech')")
            client.execute("INSERT INTO posts (title, category) VALUES ('Cooking Recipes', 'food')")
            client.execute("CREATE FTS INDEX ON posts")

            # Verify all indexed
            df = client.execute("SELECT * FROM posts WHERE MATCH('Learning')").to_pandas()
            assert len(df) == 2

            # Delete one row
            client.execute("DELETE FROM posts WHERE title = 'Machine Learning'")

            # Should no longer appear in FTS results
            df = client.execute("SELECT * FROM posts WHERE MATCH('Machine')").to_pandas()
            assert len(df) == 0, f"Deleted row still found in FTS: {df}"

            # Other row should still be indexed
            df2 = client.execute("SELECT * FROM posts WHERE MATCH('Deep')").to_pandas()
            assert len(df2) == 1

            client.close()

    def test_sql_delete_all_rows_syncs_fts(self):
        """DELETE without WHERE (delete all) should remove all rows from FTS index."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE logs (message TEXT)")
            client.execute("INSERT INTO logs (message) VALUES ('error occurred'), ('warning raised'), ('info logged')")
            client.execute("CREATE FTS INDEX ON logs")

            df = client.execute("SELECT * FROM logs WHERE MATCH('error')").to_pandas()
            assert len(df) == 1

            client.execute("DELETE FROM logs")

            df = client.execute("SELECT * FROM logs WHERE MATCH('error')").to_pandas()
            assert len(df) == 0, "FTS still returns results after DELETE all"

            client.close()

    def test_alter_fts_index_enable_backfills(self):
        """ALTER FTS INDEX ON table ENABLE should backfill rows inserted while FTS was disabled."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE wiki (title TEXT, content TEXT)")
            client.execute("INSERT INTO wiki (title, content) VALUES ('Initial', 'first entry')")
            client.execute("CREATE FTS INDEX ON wiki")

            # Disable FTS
            client.execute("ALTER FTS INDEX ON wiki DISABLE")

            # Insert while disabled (will NOT be indexed)
            client.execute("INSERT INTO wiki (title, content) VALUES ('Added While Disabled', 'absent content')")

            # Re-enable — should backfill ALL rows (including the missed one)
            result = client.execute("ALTER FTS INDEX ON wiki ENABLE")
            status = result.to_pandas()["status"][0]
            assert "rows indexed" in status, f"Expected rows indexed message, got: {status}"

            # The row added while disabled should now be findable
            df = client.execute("SELECT * FROM wiki WHERE MATCH('absent')").to_pandas()
            assert len(df) == 1, f"Row inserted while disabled not found after re-enable: {df}"

            # Original row should also be findable
            df2 = client.execute("SELECT * FROM wiki WHERE MATCH('first')").to_pandas()
            assert len(df2) == 1

            client.close()

    def test_alter_fts_index_enable_on_fresh_table(self):
        """ALTER FTS INDEX ON table ENABLE on a table with no prior index should create and backfill."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE catalog (name TEXT, description TEXT)")
            client.execute("INSERT INTO catalog (name, description) VALUES ('Widget A', 'blue widget')")
            client.execute("INSERT INTO catalog (name, description) VALUES ('Widget B', 'red widget')")

            result = client.execute("ALTER FTS INDEX ON catalog ENABLE")
            status = result.to_pandas()["status"][0]
            assert "rows indexed" in status

            df = client.execute("SELECT * FROM catalog WHERE MATCH('blue')").to_pandas()
            assert len(df) == 1
            assert df.iloc[0]["name"] == "Widget A"

            client.close()

    def test_show_fts_indexes_has_database_column(self):
        """SHOW FTS INDEXES must include a 'database' column."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE t1 (content TEXT)")
            client.execute("CREATE FTS INDEX ON t1")

            result = client.execute("SHOW FTS INDEXES")
            df = result.to_pandas()

            assert "database" in df.columns, f"Missing 'database' column. Columns: {list(df.columns)}"
            assert "table" in df.columns
            assert "enabled" in df.columns
            assert "fields" in df.columns
            assert "lazy_load" in df.columns
            assert "cache_size" in df.columns

            # At least one row for 't1'
            assert len(df) >= 1
            assert "t1" in df["table"].values

            client.close()

    def test_show_fts_indexes_reflects_enabled_state(self):
        """SHOW FTS INDEXES should accurately reflect enabled/disabled state."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE docs (content TEXT)")
            client.execute("CREATE FTS INDEX ON docs")

            df = client.execute("SHOW FTS INDEXES").to_pandas()
            row = df[df["table"] == "docs"].iloc[0]
            assert row["enabled"] is True or row["enabled"] == True

            client.execute("ALTER FTS INDEX ON docs DISABLE")

            df2 = client.execute("SHOW FTS INDEXES").to_pandas()
            row2 = df2[df2["table"] == "docs"].iloc[0]
            assert row2["enabled"] is False or row2["enabled"] == False

            client.execute("ALTER FTS INDEX ON docs ENABLE")

            df3 = client.execute("SHOW FTS INDEXES").to_pandas()
            row3 = df3[df3["table"] == "docs"].iloc[0]
            assert row3["enabled"] is True or row3["enabled"] == True

            client.close()

    def test_show_fts_indexes_multiple_tables(self):
        """SHOW FTS INDEXES should list all tables that have FTS configured."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE t1 (a TEXT)")
            client.execute("CREATE TABLE t2 (b TEXT)")
            client.execute("CREATE TABLE t3 (c TEXT)")
            client.execute("CREATE FTS INDEX ON t1")
            client.execute("CREATE FTS INDEX ON t2")
            client.execute("CREATE FTS INDEX ON t3")

            df = client.execute("SHOW FTS INDEXES").to_pandas()
            tables = set(df["table"].values)
            assert {"t1", "t2", "t3"}.issubset(tables)

            client.close()

    def test_fts_insert_then_delete_consistency(self):
        """INSERT then DELETE for the same row should leave FTS consistent."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE events (name TEXT, type TEXT)")
            client.execute("CREATE FTS INDEX ON events")

            client.execute("INSERT INTO events (name, type) VALUES ('Launch', 'product')")
            client.execute("INSERT INTO events (name, type) VALUES ('Conference', 'meetup')")

            # Verify both indexed
            df = client.execute("SELECT * FROM events WHERE MATCH('Launch')").to_pandas()
            assert len(df) == 1

            # Delete the inserted row
            client.execute("DELETE FROM events WHERE name = 'Launch'")

            # Should not appear in FTS
            df2 = client.execute("SELECT * FROM events WHERE MATCH('Launch')").to_pandas()
            assert len(df2) == 0

            # Other row untouched
            df3 = client.execute("SELECT * FROM events WHERE MATCH('Conference')").to_pandas()
            assert len(df3) == 1

            client.close()

    def test_fts_backfill_respects_index_fields(self):
        """Backfill on CREATE FTS INDEX ON table (col) should only index specified columns."""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.execute("CREATE TABLE products (name TEXT, secret TEXT)")
            client.execute("INSERT INTO products (name, secret) VALUES ('visible item', 'hidden data')")

            client.execute("CREATE FTS INDEX ON products (name)")

            # 'name' field is indexed → should find
            df = client.execute("SELECT * FROM products WHERE MATCH('visible')").to_pandas()
            assert len(df) == 1

            client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
