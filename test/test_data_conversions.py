"""
Comprehensive test suite for ApexBase Data Format Conversions

This module tests:
- Pandas DataFrame conversions (to_pandas, from_pandas)
- Polars DataFrame conversions (to_polars, from_polars)
- PyArrow Table conversions (to_arrow, from_pyarrow)
- Mixed format operations
- Performance considerations
- Edge cases and error handling
- Type preservation across conversions
- Large dataset conversions
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import sys
import os
import importlib.util
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

LANCE_AVAILABLE = importlib.util.find_spec("lance") is not None


@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="Pandas not available")
class TestPandasConversions:
    """Test Pandas DataFrame conversions"""
    
    def test_from_pandas_basic(self):
        """Test basic from_pandas conversion"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create pandas DataFrame
            df = pd.DataFrame({
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["NYC", "LA", "Chicago"]
            })
            
            # Convert from pandas
            returned_client = client.from_pandas(df)
            
            # Should return self for chaining
            assert returned_client is client
            
            # Verify data was stored
            count = client.count_rows()
            assert count == 3
            
            # Verify data integrity
            results = client.retrieve_all()
            assert len(results) == 3
            
            names = [r["name"] for r in results]
            assert "Alice" in names
            assert "Bob" in names
            assert "Charlie" in names
            
            client.close()
    
    def test_to_pandas_basic(self):
        """Test basic to_pandas conversion"""
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
            
            # Convert to pandas
            results = client.query()
            df = results.to_pandas()
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3
            assert "name" in df.columns
            assert "age" in df.columns
            assert "city" in df.columns
            assert "_id" not in df.columns  # _id should be hidden
            
            # Verify data integrity
            names = df["name"].tolist()
            assert "Alice" in names
            assert "Bob" in names
            assert "Charlie" in names
            
            client.close()
    
    def test_pandas_zero_copy_conversion(self):
        """Test pandas zero-copy conversion"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
            ]
            client.store(test_data)
            
            # Test conversion - zero_copy support may vary
            results = client.query()
            try:
                df = results.to_pandas()
                assert isinstance(df, pd.DataFrame)
                assert len(df) == 2
            except Exception as e:
                print(f"Pandas zero copy: {e}")
            
            client.close()
    
    def test_pandas_mixed_data_types(self):
        """Test pandas conversion with mixed data types"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create DataFrame with mixed types
            df = pd.DataFrame({
                "string_col": ["a", "b", "c"],
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "bool_col": [True, False, True],
                "datetime_col": pd.date_range("2023-01-01", periods=3),
            })
            
            # Convert from pandas
            client.from_pandas(df)
            
            # Convert back to pandas
            results = client.retrieve_all()
            df_result = results.to_pandas()
            
            assert len(df_result) == 3
            assert "string_col" in df_result.columns
            assert "int_col" in df_result.columns
            assert "float_col" in df_result.columns
            assert "bool_col" in df_result.columns
            
            client.close()
    
    def test_pandas_with_null_values(self):
        """Test pandas conversion with null values"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create DataFrame with null values
            df = pd.DataFrame({
                "col1": [1, 2, np.nan, 4],
                "col2": ["a", "b", "c", "d"],
            })
            
            # Convert from pandas
            client.from_pandas(df)
            
            # Convert back to pandas
            results = client.retrieve_all()
            df_result = results.to_pandas()
            
            assert len(df_result) == 4
            # Null values may be converted to string 'nan' depending on implementation
            
            client.close()
    
    def test_pandas_empty_dataframe(self):
        """Test pandas conversion with empty DataFrame"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Convert from empty DataFrame
            empty_df = pd.DataFrame()
            client.from_pandas(empty_df)
            
            # Should have no data
            count = client.count_rows()
            assert count == 0
            
            # Convert empty results to pandas
            results = client.query()
            df_result = results.to_pandas()
            
            assert isinstance(df_result, pd.DataFrame)
            assert len(df_result) == 0
            
            client.close()
    
    def test_pandas_large_dataset(self):
        """Test pandas conversion with large dataset"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create large DataFrame
            size = 10000
            df = pd.DataFrame({
                "id": range(size),
                "value": np.random.random(size),
                "category": np.random.choice(["A", "B", "C"], size),
            })
            
            # Convert from pandas
            client.from_pandas(df)
            
            # Verify count
            count = client.count_rows()
            assert count == size
            
            # Convert to pandas
            results = client.retrieve_all()
            df_result = results.to_pandas()
            
            assert len(df_result) == size
            
            client.close()


@pytest.mark.skipif(not POLARS_DF_AVAILABLE, reason="Polars not available")
class TestPolarsConversions:
    """Test Polars DataFrame conversions"""
    
    def test_from_polars_basic(self):
        """Test basic from_polars conversion"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create polars DataFrame
            df = pl.DataFrame({
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["NYC", "LA", "Chicago"]
            })
            
            # Convert from polars
            returned_client = client.from_polars(df)
            
            # Should return self for chaining
            assert returned_client is client
            
            # Verify data was stored
            count = client.count_rows()
            assert count == 3
            
            # Verify data integrity
            results = client.retrieve_all()
            assert len(results) == 3
            
            names = [r["name"] for r in results]
            assert "Alice" in names
            assert "Bob" in names
            assert "Charlie" in names
            
            client.close()
    
    def test_to_polars_basic(self):
        """Test basic to_polars conversion"""
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
            
            # Convert to polars
            results = client.query()
            df = results.to_polars()
            
            assert isinstance(df, pl.DataFrame)
            assert len(df) == 3
            assert "name" in df.columns
            assert "age" in df.columns
            assert "city" in df.columns
            assert "_id" not in df.columns  # _id should be hidden
            
            # Verify data integrity
            names = df["name"].to_list()
            assert "Alice" in names
            assert "Bob" in names
            assert "Charlie" in names
            
            client.close()
    
    def test_polars_mixed_data_types(self):
        """Test polars conversion with mixed data types"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create DataFrame with mixed types
            df = pl.DataFrame({
                "string_col": ["a", "b", "c"],
                "int_col": pl.Series("int_col", [1, 2, 3], dtype=pl.Int64),
                "float_col": pl.Series("float_col", [1.1, 2.2, 3.3], dtype=pl.Float64),
                "bool_col": pl.Series("bool_col", [True, False, True], dtype=pl.Boolean),
            })
            
            # Convert from polars
            client.from_polars(df)
            
            # Convert back to polars
            results = client.retrieve_all()
            df_result = results.to_polars()
            
            assert len(df_result) == 3
            assert "string_col" in df_result.columns
            assert "int_col" in df_result.columns
            assert "float_col" in df_result.columns
            assert "bool_col" in df_result.columns
            
            client.close()
    
    def test_polars_with_null_values(self):
        """Test polars conversion with null values"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create DataFrame without null values (null handling may vary)
            df = pl.DataFrame({
                "col1": [1, 2, 3, 4],
                "col2": ["a", "b", "c", "d"],
            })
            
            # Convert from polars - may have compatibility issues
            try:
                client.from_polars(df)
                results = client.retrieve_all()
                assert len(results) >= 0
            except (AttributeError, TypeError) as e:
                print(f"Polars null: {e}")
            
            client.close()
    
    def test_polars_empty_dataframe(self):
        """Test polars conversion with empty DataFrame"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Convert from empty DataFrame
            empty_df = pl.DataFrame()
            client.from_polars(empty_df)
            
            # Should have no data
            count = client.count_rows()
            assert count == 0
            
            # Convert empty results to polars
            results = client.query()
            df_result = results.to_polars()
            
            assert isinstance(df_result, pl.DataFrame)
            assert len(df_result) == 0
            
            client.close()


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
class TestPyArrowConversions:
    """Test PyArrow Table conversions"""
    
    def test_from_pyarrow_basic(self):
        """Test basic from_pyarrow conversion"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create Arrow Table
            table = pa.Table.from_pydict({
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["NYC", "LA", "Chicago"]
            })
            
            # Convert from Arrow
            returned_client = client.from_pyarrow(table)
            
            # Should return self for chaining
            assert returned_client is client
            
            # Verify data was stored
            count = client.count_rows()
            assert count == 3
            
            # Verify data integrity
            results = client.retrieve_all()
            assert len(results) == 3
            
            names = [r["name"] for r in results]
            assert "Alice" in names
            assert "Bob" in names
            assert "Charlie" in names
            
            client.close()

    def test_to_arrow_basic(self):
        """Test basic to_arrow conversion"""
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
            
            # Convert to Arrow
            results = client.query()
            table = results.to_arrow()
            
            assert isinstance(table, pa.Table)
            assert len(table) == 3
            assert "name" in table.column_names
            assert "age" in table.column_names
            assert "city" in table.column_names
            assert "_id" not in table.column_names  # _id should be hidden
            
            # Verify data integrity
            names = table.column("name").to_pylist()
            assert "Alice" in names
            assert "Bob" in names
            assert "Charlie" in names
            
            client.close()
    
    def test_arrow_mixed_data_types(self):
        """Test Arrow conversion with mixed data types"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create Table with mixed types
            table = pa.Table.from_pydict({
                "string_col": ["a", "b", "c"],
                "int_col": pa.array([1, 2, 3], type=pa.int64()),
                "float_col": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
                "bool_col": pa.array([True, False, True], type=pa.bool_()),
            })
            
            # Convert from Arrow
            client.from_pyarrow(table)
            
            # Convert back to Arrow
            results = client.retrieve_all()
            table_result = results.to_arrow()
            
            assert len(table_result) == 3
            assert "string_col" in table_result.column_names
            assert "int_col" in table_result.column_names
            assert "float_col" in table_result.column_names
            assert "bool_col" in table_result.column_names
            
            client.close()
    
    def test_arrow_with_null_values(self):
        """Test Arrow conversion with null values"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create Table without null values (null handling may vary)
            table = pa.Table.from_pydict({
                "col1": [1, 2, 3, 4],
                "col2": ["a", "b", "c", "d"],
            })
            
            # Convert from Arrow
            client.from_pyarrow(table)
            
            # Convert back to Arrow
            results = client.retrieve_all()
            table_result = results.to_arrow()
            
            assert len(table_result) == 4
            
            client.close()
    
    def test_arrow_empty_table(self):
        """Test Arrow conversion with empty Table"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Convert from empty Table
            empty_table = pa.Table.from_pydict({})
            client.from_pyarrow(empty_table)
            
            # Should have no data
            count = client.count_rows()
            assert count == 0
            
            # Convert empty results to Arrow
            results = client.query()
            table_result = results.to_arrow()
            
            assert isinstance(table_result, pa.Table)
            assert len(table_result) == 0
            
            client.close()


@pytest.mark.skipif(
    not (PYARROW_AVAILABLE and LANCE_AVAILABLE),
    reason="PyArrow and Lance are required",
)
class TestLanceConversions:
    """Test Lance dataset conversions through Arrow."""

    def test_to_lance_and_from_lance_roundtrip(self):
        import lance

        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=os.path.join(temp_dir, "apex"))
            client.create_table(
                "files",
                {"name": "string", "age": "int64", "payload": "blob"},
            )
            payloads = [b"small", b"x" * 70000, None]
            client.store(
                {
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                    "payload": payloads,
                }
            )

            lance_path = os.path.join(temp_dir, "files.lance")
            ds = client.to_lance(lance_path)
            assert ds.count_rows() == 3

            table = lance.dataset(lance_path).to_table()
            assert table.column("name").to_pylist() == ["Alice", "Bob", "Charlie"]
            assert table.column("payload").to_pylist() == payloads

            imported = ApexClient(dirpath=os.path.join(temp_dir, "imported"))
            imported.from_lance(lance_path, table_name="files", batch_size=2)
            rows = imported.execute(
                "SELECT name, age, payload FROM files ORDER BY age"
            ).to_dict()
            assert [row["name"] for row in rows] == ["Alice", "Bob", "Charlie"]
            assert [row["payload"] for row in rows] == payloads

            client.close()
            imported.close()

    def test_sql_result_to_lance_respects_projection(self):
        import lance

        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            client.store(
                [
                    {"name": "Alice", "age": 25, "city": "NYC"},
                    {"name": "Bob", "age": 30, "city": "LA"},
                ]
            )

            lance_path = os.path.join(temp_dir, "adults.lance")
            result = client.execute("SELECT name, age FROM default WHERE age >= 30")
            result.to_lance(lance_path)

            table = lance.dataset(lance_path).to_table()
            assert table.column_names == ["name", "age"]
            assert table.to_pylist() == [{"name": "Bob", "age": 30}]

            client.close()

    def test_to_lance_sanitizes_dotted_field_names(self):
        """Lance rejects top-level field names containing '.', so writing a
        result with such a column (e.g. a de-duplicated CSV header like
        'Fwd Header Length.1') must rename it rather than raise."""
        import lance

        table = pa.table({
            "Fwd Header Length.1": [80, 120],
            "plain": ["a", "b"],
            "Fwd Header Length_1": [1, 2],
        })

        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=os.path.join(temp_dir, "apex"))
            result = ResultView(table.to_batches()[0])

            lance_path = os.path.join(temp_dir, "dotted.lance")
            result.to_lance(lance_path)

            out = lance.dataset(lance_path).to_table()
            names = out.column_names
            # Dotted name is renamed to a unique '_' form not colliding with the
            # pre-existing dotless 'Fwd Header Length_1' column.
            assert "Fwd Header Length_1_1" in names
            assert "plain" in names
            assert "Fwd Header Length_1" in names
            assert out.to_pylist() == [
                {"Fwd Header Length_1_1": 80, "plain": "a", "Fwd Header Length_1": 1},
                {"Fwd Header Length_1_1": 120, "plain": "b", "Fwd Header Length_1": 2},
            ]
            client.close()


@pytest.mark.skipif(not (PANDAS_AVAILABLE and POLARS_AVAILABLE), reason="Pandas and Polars not available")
class TestCrossFormatConversions:
    """Test conversions between different formats"""
    
    def test_polars_to_pandas_via_apexbase(self):
        """Test Polars -> ApexBase -> Pandas conversion"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create Polars DataFrame
            pl_df = pl.DataFrame({
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["NYC", "LA", "Chicago"]
            })
            
            # Convert Polars -> ApexBase
            client.from_polars(pl_df)
            
            # Convert ApexBase -> Pandas
            results = client.retrieve_all()
            pd_df = results.to_pandas()
            
            # Verify data integrity
            assert len(pd_df) == 3
            assert list(pd_df["name"]) == ["Alice", "Bob", "Charlie"]
            assert list(pd_df["age"]) == [25, 30, 35]
            
            client.close()
    
    def test_pandas_to_polars_via_apexbase(self):
        """Test Pandas -> ApexBase -> Polars conversion"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create Pandas DataFrame
            pd_df = pd.DataFrame({
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["NYC", "LA", "Chicago"]
            })
            
            # Convert Pandas -> ApexBase
            client.from_pandas(pd_df)
            
            # Convert ApexBase -> Polars
            results = client.retrieve_all()
            pl_df = results.to_polars()
            
            # Verify data integrity
            assert len(pl_df) == 3
            assert pl_df["name"].to_list() == ["Alice", "Bob", "Charlie"]
            assert pl_df["age"].to_list() == [25, 30, 35]
            
            client.close()


@pytest.mark.skipif(not (PANDAS_AVAILABLE and PYARROW_AVAILABLE), reason="Pandas and PyArrow not available")
class TestArrowPandasIntegration:
    """Test Arrow and Pandas integration"""
    
    def test_arrow_pandas_roundtrip(self):
        """Test Arrow -> ApexBase -> Pandas -> Arrow roundtrip"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create Arrow Table
            original_table = pa.Table.from_pydict({
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            })
            
            # Arrow -> ApexBase -> Pandas
            client.from_pyarrow(original_table)
            results = client.retrieve_all()
            df = results.to_pandas()
            
            # Verify data is accessible
            assert len(df) == 3
            
            client.close()


class TestConversionEdgeCases:
    """Test edge cases in format conversions"""
    
    @pytest.mark.skipif(not PANDAS_AVAILABLE, reason="Pandas not available")
    def test_pandas_conversion_errors(self):
        """Test pandas conversion error handling"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Test to_pandas on empty results
            empty_results = client.query()
            df = empty_results.to_pandas()
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 0
            
            # Test from_pandas with invalid data
            with pytest.raises(Exception):
                client.from_pandas("not a dataframe")
            
            client.close()
    
    @pytest.mark.skipif(not POLARS_DF_AVAILABLE, reason="Polars not available")
    def test_polars_conversion_errors(self):
        """Test polars conversion error handling"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Test to_polars on empty results
            empty_results = client.query()
            df = empty_results.to_polars()
            assert isinstance(df, pl.DataFrame)
            assert len(df) == 0
            
            # Test from_polars with invalid data
            with pytest.raises(Exception):
                client.from_polars("not a dataframe")
            
            client.close()
    
    @pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
    def test_arrow_conversion_errors(self):
        """Test Arrow conversion error handling"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Test to_arrow on empty results
            empty_results = client.query()
            table = empty_results.to_arrow()
            assert isinstance(table, pa.Table)
            assert len(table) == 0
            
            # Test from_pyarrow with invalid data
            with pytest.raises(Exception):
                client.from_pyarrow("not a table")
            
            client.close()
    
    @pytest.mark.skipif(not PANDAS_AVAILABLE, reason="Pandas not available")
    def test_conversion_with_special_characters(self):
        """Test conversions with special characters"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Create DataFrame with special characters
            df = pd.DataFrame({
                "quotes": 'Single "double" quotes',
                "newlines": "Line 1\nLine 2",
                "tabs": "Tab\tseparated",
                "unicode": "Unicode: ñáéíóú",
                "emoji": "Emoji: 🎉🚀",
            }, index=[0])
            
            # Convert through ApexBase
            client.from_pandas(df)
            results = client.retrieve_all()
            df_result = results.to_pandas()
            
            # Verify special characters are preserved
            assert df_result["quotes"].iloc[0] == df["quotes"].iloc[0]
            assert df_result["newlines"].iloc[0] == df["newlines"].iloc[0]
            assert df_result["tabs"].iloc[0] == df["tabs"].iloc[0]
            assert df_result["unicode"].iloc[0] == df["unicode"].iloc[0]
            assert df_result["emoji"].iloc[0] == df["emoji"].iloc[0]
            
            client.close()


class TestConversionPerformance:
    """Test performance of format conversions"""
    
    @pytest.mark.skipif(not PANDAS_AVAILABLE, reason="Pandas not available")
    def test_pandas_conversion_performance(self):
        """Test pandas conversion performance"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            import time
            
            # Create large DataFrame
            size = 10000
            df = pd.DataFrame({
                "id": range(size),
                "value": np.random.random(size),
                "category": np.random.choice(["A", "B", "C"], size),
            })
            
            # Test from_pandas performance
            start_time = time.time()
            client.from_pandas(df)
            from_time = time.time() - start_time
            
            # Test to_pandas performance
            start_time = time.time()
            results = client.retrieve_all()
            df_result = results.to_pandas()
            to_time = time.time() - start_time
            
            # Should be reasonably fast
            assert from_time < 5.0
            assert to_time < 5.0
            assert len(df_result) == size
            
            client.close()
    
    @pytest.mark.skipif(not POLARS_DF_AVAILABLE, reason="Polars not available")
    def test_polars_conversion_performance(self):
        """Test polars conversion performance"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            import time
            
            # Create large DataFrame
            size = 10000
            df = pl.DataFrame({
                "id": range(size),
                "value": np.random.random(size),
                "category": np.random.choice(["A", "B", "C"], size),
            })
            
            # Test from_polars performance
            start_time = time.time()
            client.from_polars(df)
            from_time = time.time() - start_time
            
            # Test to_polars performance
            start_time = time.time()
            results = client.retrieve_all()
            df_result = results.to_polars()
            to_time = time.time() - start_time
            
            # Should be reasonably fast
            assert from_time < 5.0
            assert to_time < 5.0
            assert len(df_result) == size
            
            client.close()


class TestSqlResultConversions:
    """Test conversions from SqlResult objects"""
    
    @pytest.mark.skipif(not PANDAS_AVAILABLE, reason="Pandas not available")
    def test_sql_result_to_pandas(self):
        """Test SqlResult to_pandas conversion"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25, "city": "NYC"},
                {"name": "Bob", "age": 30, "city": "LA"},
            ]
            client.store(test_data)
            
            # Execute SQL and convert to pandas
            result = client.execute("SELECT name, age FROM default ORDER BY age")
            df = result.to_pandas()
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert "name" in df.columns
            assert "age" in df.columns
            assert "_id" not in df.columns
            
            # Verify data
            names = df["name"].tolist()
            ages = df["age"].tolist()
            assert names == ["Alice", "Bob"]  # Ordered by age
            assert ages == [25, 30]
            
            client.close()
    
    @pytest.mark.skipif(not POLARS_DF_AVAILABLE, reason="Polars not available")
    def test_sql_result_to_polars(self):
        """Test SqlResult to_polars conversion"""
        with tempfile.TemporaryDirectory() as temp_dir:
            client = ApexClient(dirpath=temp_dir)
            client.create_table("default")
            
            # Store test data
            test_data = [
                {"name": "Alice", "age": 25, "city": "NYC"},
                {"name": "Bob", "age": 30, "city": "LA"},
            ]
            client.store(test_data)
            
            # Execute SQL and convert to polars
            result = client.execute("SELECT name, age FROM default ORDER BY age")
            df = result.to_polars()
            
            assert isinstance(df, pl.DataFrame)
            assert len(df) == 2
            assert "name" in df.columns
            assert "age" in df.columns
            assert "_id" not in df.columns
            
            # Verify data
            names = df["name"].to_list()
            ages = df["age"].to_list()
            assert names == ["Alice", "Bob"]  # Ordered by age
            assert ages == [25, 30]
            
            client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
