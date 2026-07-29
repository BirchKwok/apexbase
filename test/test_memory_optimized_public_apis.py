import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa

from apexbase import ApexClient, ResultView
from apexbase.client import encode_vector


def test_dataframe_import_helpers_keep_columnar_inputs(tmp_path):
    client = ApexClient(str(tmp_path))
    seen = []
    original_store = client.store

    def record_input(value):
        seen.append(value)
        original_store(value)

    client.store = record_input
    pandas_frame = pd.DataFrame({"value": [1, 2], "label": ["a", None]})
    arrow_table = pa.table({"value": [3, 4], "label": ["c", "d"]})
    polars_frame = pl.DataFrame({"value": [5, 6], "label": ["e", "f"]})

    client.from_pandas(pandas_frame, "pandas_rows")
    client.from_pyarrow(arrow_table, "arrow_rows")
    client.from_polars(polars_frame, "polars_rows")

    assert seen == [pandas_frame, arrow_table, polars_frame]
    client.use_table("pandas_rows")
    assert client.retrieve_all().to_dict() == [
        {"value": 1, "label": "a"},
        # Preserve missing string values instead of coercing them to empty text.
        {"value": 2, "label": None},
    ]
    client.close()


def test_retrieve_all_uses_arrow_result_and_keeps_pending_rows(tmp_path):
    client = ApexClient(str(tmp_path))
    client.create_table("items")
    client.store({"value": [1, 2, 3], "label": ["a", "b", "c"]})

    result = client.retrieve_all()

    assert isinstance(result, ResultView)
    assert result.to_dict() == [
        {"value": 1, "label": "a"},
        {"value": 2, "label": "b"},
        {"value": 3, "label": "c"},
    ]
    client.close()


def test_row_batch_direct_columnarization_preserves_values(tmp_path):
    client = ApexClient(str(tmp_path))
    client.create_table("rows")
    client.store(
        [
            {"value": 1, "score": 1.5, "name": "one", "enabled": True},
            {"value": 2, "score": 2.5, "name": "two", "enabled": False},
        ]
    )
    client.store(
        [{"value": 3, "score": 3.5, "name": "three", "enabled": True}]
    )

    assert client.retrieve_all().to_dict() == [
        {"value": 1, "score": 1.5, "name": "one", "enabled": True},
        {"value": 2, "score": 2.5, "name": "two", "enabled": False},
        {"value": 3, "score": 3.5, "name": "three", "enabled": True},
    ]
    client.close()


def test_arrow_result_materialization_hides_id_without_changing_rows():
    table = pa.table({"_id": [10, 11], "value": [1, 2], "label": ["a", "b"]})
    result = ResultView(arrow_table=table)

    assert result.to_dict() == [
        {"value": 1, "label": "a"},
        {"value": 2, "label": "b"},
    ]
    assert result.get_ids(return_list=True) == [10, 11]


def test_result_view_single_row_access_stays_lazy_and_preserves_slices():
    table = pa.table({"_id": [10, 11, 12], "value": [1, 2, 3]})
    result = ResultView(arrow_table=table)

    assert result.first() == {"value": 1}
    assert result[-1] == {"value": 3}
    assert result._data is None
    assert result[1:] == [{"value": 2}, {"value": 3}]
    assert result._data is not None


def test_encode_vector_accepts_contiguous_float32_without_mutating_input():
    vector = np.arange(32, dtype=np.float32)
    before = vector.copy()

    encoded = encode_vector(vector)

    assert encoded == vector.astype("<f4").tobytes()
    np.testing.assert_array_equal(vector, before)
