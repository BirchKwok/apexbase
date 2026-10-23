"""W5-1 stage 1: persisted coarse IVF index build/inspect/drop."""
from __future__ import annotations

import os
import tempfile

import numpy as np
import pytest

from apexbase.client import ApexClient

ROWS = 5_000
DIM = 8
CLUSTERS = 4


def _make_table(tmp):
    rng = np.random.default_rng(7)
    centers = rng.standard_normal((CLUSTERS, DIM)).astype(np.float32) * 8.0
    labels = rng.integers(0, CLUSTERS, ROWS)
    matrix = (centers[labels] + rng.standard_normal((ROWS, DIM)).astype(np.float32)).astype(
        np.float32
    )
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", {"label": "int64", "vec": "float32_vector"})
    client.use_table("t")
    client.store({"label": list(range(ROWS)), "vec": [matrix[i] for i in range(ROWS)]})
    client.flush()
    return client, matrix


def test_build_inspect_and_drop_index():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _make_table(tmp)
        try:
            assert client.vector_index_info() is None
            client.create_vector_index("vec", nlist=16, iterations=8)
            index_path = os.path.join(tmp, "t.apex.ivf")
            assert os.path.isfile(index_path)
            info = client.vector_index_info()
            assert info is not None
            assert info["dim"] == DIM
            assert info["nlist"] == 16
            assert info["rows"] == ROWS
            assert info["metric"] == "l2"
            assert info["inertia"] >= 0
            assert len(info["list_sizes"]) == 16
            assert sum(info["list_sizes"]) == ROWS

            # Deterministic rebuild: identical clustering and inertia.
            first = info["inertia"]
            client.create_vector_index("vec", nlist=16, iterations=8)
            assert client.vector_index_info()["inertia"] == first

            # Persists across reopen.
            client.close()
            client = ApexClient(tmp, enable_cache=False)
            client.use_table("t")
            assert client.vector_index_info()["rows"] == ROWS
            assert client.drop_vector_index() is True
            assert not os.path.exists(index_path)
            assert client.vector_index_info() is None
            assert client.drop_vector_index() is False
        finally:
            client.close()


def test_index_requires_a_vector_column_and_flushed_rows():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", {"label": "int64", "vec": "float32_vector"})
        client.use_table("t")
        try:
            with pytest.raises(RuntimeError, match="not a supported"):
                client.create_vector_index("label")
            client.store({"label": [1], "vec": [np.zeros(DIM, dtype=np.float32)]})
            client.flush()
            client.create_vector_index("vec", nlist=64, iterations=4)
            info = client.vector_index_info()
            # nlist is clamped to the row count.
            assert info["nlist"] == 1
            assert info["rows"] == 1
        finally:
            client.close()
