"""W1-5b option B: `topk_distance(raw=True)` returns the ranking flat.

The default method builds an Arrow RecordBatch, imports it as a Table and wraps
it in a ResultView. For a single k=10 query that fixed cost dominates. The raw
form returns ``{"ids": [...], "distances": [...]}`` straight from the direct
mmap scan, with the same ranking.
"""
from __future__ import annotations

import statistics
import tempfile
import time

import numpy as np

from apexbase.client import ApexClient

ROWS = 1_000
DIM = 8


def _make_client(tmp):
    rng = np.random.default_rng(11)
    matrix = rng.standard_normal((ROWS, DIM), dtype=np.float32)
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"label": "int64", "vec": "float32_vector"})
    client.use_table("t")
    client.store({"label": list(range(ROWS)), "vec": [matrix[i] for i in range(ROWS)]})
    client.flush()
    return client, matrix


def test_raw_matches_the_default_ranking():
    with tempfile.TemporaryDirectory() as tmp:
        client, matrix = _make_client(tmp)
        try:
            for query in (matrix[0], matrix[123], np.zeros(DIM, dtype=np.float32)):
                default = client.topk_distance("vec", query.copy(), k=10).to_dict()
                raw = client.topk_distance("vec", query.copy(), k=10, raw=True)
                assert set(raw) == {"ids", "distances"}
                assert raw["ids"] == [row["_id"] for row in default]
                assert raw["distances"] == [row["dist"] for row in default]
                assert raw["distances"] == sorted(raw["distances"])
                assert len(raw["ids"]) == 10
        finally:
            client.close()


def test_raw_handles_empty_table_and_k_larger_than_rows():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"vec": "float32_vector"})
        client.use_table("t")
        try:
            raw = client.topk_distance("vec", [1.0] * DIM, k=5, raw=True)
            assert raw == {"ids": [], "distances": []}
            client.store({"vec": [[float(i)] * DIM for i in range(3)]})
            client.flush()
            raw = client.topk_distance("vec", [0.0] * DIM, k=10, raw=True)
            assert len(raw["ids"]) == 3
            assert len(raw["distances"]) == 3
        finally:
            client.close()


def test_raw_rejects_invalid_metric_and_accelerator():
    with tempfile.TemporaryDirectory() as tmp:
        client, matrix = _make_client(tmp)
        try:
            try:
                client.topk_distance("vec", matrix[0].copy(), k=3, metric="nope", raw=True)
            except (ValueError, RuntimeError) as error:
                assert "metric" in str(error).lower() or "unknown" in str(error).lower()
            else:
                raise AssertionError("unknown metric must raise")
            try:
                client.topk_distance(
                    "vec", matrix[0].copy(), k=3, accelerator="vec", raw=True
                )
            except ValueError as error:
                assert "accelerator" in str(error)
            else:
                raise AssertionError("raw + accelerator must raise")
        finally:
            client.close()


def test_raw_skips_the_result_view_fixed_cost():
    with tempfile.TemporaryDirectory() as tmp:
        client, matrix = _make_client(tmp)
        try:
            query = matrix[0].copy()
            for _ in range(3):
                client.topk_distance("vec", query, k=10)
                client.topk_distance("vec", query, k=10, raw=True)
            # Paired A/B with alternating order: a busy machine slows both calls
            # of a pair together, so the median per-pair ratio stays meaningful
            # while a single median of each side can flip under load.
            ratios = []
            for iteration in range(21):
                if iteration % 2 == 0:
                    start = time.perf_counter()
                    client.topk_distance("vec", query, k=10)
                    default_s = time.perf_counter() - start
                    start = time.perf_counter()
                    client.topk_distance("vec", query, k=10, raw=True)
                    raw_s = time.perf_counter() - start
                else:
                    start = time.perf_counter()
                    client.topk_distance("vec", query, k=10, raw=True)
                    raw_s = time.perf_counter() - start
                    start = time.perf_counter()
                    client.topk_distance("vec", query, k=10)
                    default_s = time.perf_counter() - start
                ratios.append(raw_s / default_s)
            # A micro-timing assertion in pytest is inherently load-sensitive;
            # this guards against a pathological regression while the actual
            # speedup is measured by benchmarks/bench_topk_raw.py (1.6x at 1k x 8).
            assert statistics.median(ratios) < 1.25, statistics.median(ratios)
        finally:
            client.close()
