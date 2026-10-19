"""`topk_distance` must return the real `_id` for rows in any row group.

The direct mmap scan returns *active row positions*; mapping them to ids used to
decode the whole `_id` column on every call. It now reads the id section of each
winning row group directly (`OnDemandStorage::ids_at_active_positions`), which
also has to work across row-group boundaries and must fall back to the column
read when a persisted deletion vector makes the active index ambiguous.
"""
from __future__ import annotations

import tempfile

import numpy as np

from apexbase.client import ApexClient

ROWS = 70_000
DIM = 4


def _brute_ids(matrix, query, k):
    distances = ((matrix - query) ** 2).sum(axis=1)
    return [int(index) + 1 for index in np.argsort(distances)[:k]]


def test_topk_ids_across_row_group_boundaries():
    rng = np.random.default_rng(7)
    matrix = rng.standard_normal((ROWS, DIM), dtype=np.float32)
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"label": "int64", "vec": "float32_vector"})
        client.use_table("t")
        client.store({"label": list(range(ROWS)),
                      "vec": [matrix[i] for i in range(ROWS)]})
        client.flush()
        try:
            # The first row, both sides of the 65_536-row boundary, and the tail.
            for row in (0, 65_535, 65_536, ROWS - 1):
                hits = client.topk_distance(
                    "vec", matrix[row].copy(), k=3, metric="l2").to_dict()
                assert hits[0]["_id"] == row + 1, (row, hits)
                assert hits[0]["dist"] == 0.0, (row, hits)
                assert [hit["_id"] for hit in hits] == _brute_ids(matrix, matrix[row], 3)

            # A persisted deletion hides a row: the id mapping must fall back to
            # the deletion-aware column reader and still report the right ids.
            client.execute("DELETE FROM t WHERE _id = 5")
            client.flush()
            hits = client.topk_distance(
                "vec", matrix[69_999].copy(), k=3, metric="l2").to_dict()
            assert hits[0]["_id"] == 70_000, hits
            assert [hit["_id"] for hit in hits] == _brute_ids(matrix, matrix[69_999], 3)
        finally:
            client.close()
