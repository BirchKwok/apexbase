"""W5-2: first-class hybrid retrieval fusion (vector + BM25)."""
from __future__ import annotations

import tempfile

import numpy as np
import pytest

from apexbase.client import ApexClient

ROWS = 240
DIM = 8


def _make_client(tmp):
    rng = np.random.default_rng(5)
    vectors = rng.standard_normal((ROWS, DIM)).astype(np.float32)
    # Documents 0..19 mention "alpha" and sit near the query vector; others do not.
    texts = []
    for row in range(ROWS):
        if row < 20:
            texts.append(f"alpha document number {row}")
            vectors[row] = np.array([1.0] * DIM, dtype=np.float32) + vectors[row] * 0.01
        else:
            texts.append(f"beta document number {row}")
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("docs", {"content": "string", "vec": "float32_vector"})
    client.use_table("docs")
    client.init_fts(index_fields=["content"])
    client.store({"content": texts, "vec": [vectors[i] for i in range(ROWS)]})
    client.flush()
    return client, vectors


def test_rrf_fusion_returns_unique_ranked_ids():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _make_client(tmp)
        try:
            query = np.array([1.0] * DIM, dtype=np.float32)
            result = client.hybrid_search(query, "alpha", k=10)
            rows = result.to_dict()
            assert len(rows) == 10
            ids = [row["_id"] for row in rows]
            assert len(set(ids)) == 10
            scores = [row["score"] for row in rows]
            assert scores == sorted(scores, reverse=True)
            # Every fused row comes from one of the two branches.
            vector = client.topk_distance("vec", query, k=64, raw=True)
            text = [doc_id for doc_id, _ in client.search_text_with_scores("alpha", limit=64)]
            assert set(ids) <= set(vector["ids"]) | set(text)
        finally:
            client.close()


def test_weighted_fusion_extremes_follow_each_branch():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _make_client(tmp)
        try:
            query = np.array([1.0] * DIM, dtype=np.float32)
            vector_only = client.hybrid_search(
                query, "beta", k=10, fusion="weighted", alpha=1.0
            ).to_dict()
            text_only = client.hybrid_search(
                query, "alpha", k=10, fusion="weighted", alpha=0.0
            ).to_dict()
            vector_top = client.topk_distance("vec", query, k=10, raw=True)["ids"]
            assert [row["_id"] for row in vector_only] == vector_top
            text_top = [doc_id for doc_id, _ in client.search_text_with_scores("alpha", limit=10)]
            assert [row["_id"] for row in text_only] == text_top
        finally:
            client.close()


def test_hybrid_search_validates_arguments():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _make_client(tmp)
        try:
            query = np.zeros(DIM, dtype=np.float32)
            with pytest.raises(ValueError, match="fusion"):
                client.hybrid_search(query, "alpha", fusion="bogus")
            with pytest.raises(ValueError, match="alpha"):
                client.hybrid_search(query, "alpha", fusion="weighted", alpha=1.5)
            with pytest.raises(ValueError, match="k"):
                client.hybrid_search(query, "alpha", k=0)
        finally:
            client.close()
