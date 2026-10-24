"""Quantized coarse ranking with an exact rescore stage (W5-3)."""
from __future__ import annotations

import tempfile

import numpy as np

from apexbase.client import ApexClient

ROWS = 2_000
DIM = 32


def _make_table(tmp: str) -> ApexClient:
    rng = np.random.default_rng(17)
    matrix = rng.standard_normal((ROWS, DIM)).astype(np.float32)
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", {"vec": "float32_vector"})
    client.use_table("t")
    client.store({"vec": [row for row in matrix]})
    client.flush()
    return client


def _ids(result) -> list[int]:
    return [int(row["_id"]) for row in result.to_dict()]


def test_float16_coarse_rescore_matches_exact_topk():
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_table(tmp)
        try:
            client.create_quantized_column("vec", codec="float16")
            query = np.arange(DIM, dtype=np.float32)
            exact = _ids(client.topk_distance("vec", query, k=10))
            rescored = _ids(
                client.topk_distance(
                    "vec", query, k=10, accelerator="vec_float16", candidate_k=64
                )
            )
            assert set(rescored) == set(exact)
            assert len(rescored) == 10
        finally:
            client.close()


def test_rescore_handles_deleted_rows():
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_table(tmp)
        try:
            client.create_quantized_column("vec", codec="float16")
            # Drop a leading range: the coarse lane compacts live rows, so the
            # rescore must map its active positions back to physical rows.
            deleted = 700
            client.execute(f"DELETE FROM t WHERE _id <= {deleted}")
            query = np.linspace(-1.0, 1.0, DIM, dtype=np.float32)
            exact = _ids(client.topk_distance("vec", query, k=10))
            rescored = _ids(
                client.topk_distance(
                    "vec", query, k=10, accelerator="vec_float16", candidate_k=64
                )
            )
            assert set(rescored) == set(exact)
            assert client.count_rows() == ROWS - deleted
        finally:
            client.close()


def test_int8_coarse_rescore_returns_ten_neighbours():
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_table(tmp)
        try:
            client.create_quantized_column("vec", codec="int8")
            query = np.ones(DIM, dtype=np.float32)
            exact = _ids(client.topk_distance("vec", query, k=10))
            rescored = _ids(
                client.topk_distance(
                    "vec", query, k=10, accelerator="vec_int8", candidate_k=64
                )
            )
            assert len(rescored) == 10
            assert len(set(rescored) & set(exact)) >= 8
        finally:
            client.close()
