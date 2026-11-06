"""Regression coverage: SQL UPDATE must not permanently disable the direct mmap scan.

ApexBase 2.0.0 defect
---------------------
A table with a vector column (``float32_vector``) served
``topk_distance(raw=True)`` and ``hybrid_search()`` from the direct mmap vector
lane.  A single SQL ``UPDATE`` made both raise::

    RuntimeError: topk_distance(raw=True): column 'vec' is not served by the direct vector scan

and neither ``flush()`` nor closing and reopening the client restored the lane.

Expected behaviour after the fix
--------------------------------
* An UPDATE that touches only non-vector columns keeps the direct lane, so the
  ranking is unchanged and still identical to the non-raw ``topk_distance()``.
* When the direct lane really cannot serve the table state (DELETE overlay,
  ``SET vec = NULL``, the ``:memory:`` backend), ``raw=True`` falls back to the
  delta-merging Arrow reader instead of raising, with an identical ranking.

The data is tiny (32 rows x 8 dimensions) and deterministic.  Every comparison
is against the non-raw ``topk_distance()`` result, which goes through the same
top-k engine, so ids and distances must match exactly.
"""

from __future__ import annotations

import tempfile

import numpy as np
import pytest

from apexbase.client import ApexClient

ROWS = 32
DIM = 8
K = 8
SEED = 20240917


def _dataset():
    rng = np.random.default_rng(SEED)
    matrix = rng.standard_normal((ROWS, DIM)).astype(np.float32)
    queries = [matrix[0].copy(), matrix[5].copy(), matrix[17].copy()]
    contents = [
        f"alpha document {row}" if row < 8 else f"beta document {row}"
        for row in range(ROWS)
    ]
    labels = list(range(ROWS))
    return matrix, queries, contents, labels


def _make_disk_client(tmp, *, fts=False):
    matrix, queries, contents, labels = _dataset()
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table(
        "docs", {"content": "string", "label": "int64", "vec": "float32_vector"}
    )
    client.use_table("docs")
    if fts:
        client.init_fts(index_fields=["content"])
    client.store(
        {
            "content": contents,
            "label": labels,
            "vec": [matrix[row] for row in range(ROWS)],
        }
    )
    client.flush()
    return client, matrix, queries


def _reopen_disk_client(tmp):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.use_table("docs")
    return client


def _raw_topk(client, query, *, k=K, context=""):
    """Call raw=True and turn the 2.0.0 RuntimeError into a clear failure."""
    try:
        return client.topk_distance("vec", query, k=k, raw=True)
    except RuntimeError as exc:  # pragma: no cover - only on the buggy build
        pytest.fail(
            f"topk_distance(raw=True) must stay available {context}, "
            f"but raised RuntimeError: {exc}"
        )


def _assert_raw_matches_default(client, query, *, context, k=K):
    raw = _raw_topk(client, query, k=k, context=context)
    default = client.topk_distance("vec", query, k=k).to_dict()
    default_ids = [row["_id"] for row in default]
    default_distances = [row["dist"] for row in default]
    assert len(raw["ids"]) == k, (
        f"{context}: expected {k} raw ids, got {len(raw['ids'])}: {raw}"
    )
    assert raw["ids"] == default_ids, (
        f"{context}: raw ids {raw['ids']} != non-raw ids {default_ids}"
    )
    assert raw["distances"] == default_distances, (
        f"{context}: raw distances {raw['distances']} "
        f"!= non-raw distances {default_distances}"
    )
    assert raw["distances"] == sorted(raw["distances"]), (
        f"{context}: raw distances are not sorted ascending: {raw['distances']}"
    )
    return raw


def _hybrid(client, query, text, *, fusion="rrf", k=6, context=""):
    """Call hybrid_search and turn the 2.0.0 RuntimeError into a clear failure."""
    try:
        return client.hybrid_search(query, text, k=k, fusion=fusion).to_dict()
    except RuntimeError as exc:  # pragma: no cover - only on the buggy build
        pytest.fail(
            f"hybrid_search() must stay available {context}, "
            f"but raised RuntimeError: {exc}"
        )


def _assert_batch_matches_default(client, queries, *, context, k=K):
    batch = client.batch_topk_distance("vec", np.stack(queries), k=k)
    assert batch.shape == (len(queries), k, 2), (
        f"{context}: unexpected batch_topk_distance shape {batch.shape}"
    )
    for index, query in enumerate(queries):
        default = client.topk_distance("vec", query, k=k).to_dict()
        default_ids = [row["_id"] for row in default]
        default_distances = [row["dist"] for row in default]
        batch_ids = batch[index, :, 0].astype(np.int64).tolist()
        batch_distances = batch[index, :, 1].tolist()
        assert batch_ids == default_ids, (
            f"{context} (query {index}): batch ids {batch_ids} "
            f"!= non-raw ids {default_ids}"
        )
        # The batch lane computes distances with its own f32 accumulation, so
        # the last bits may differ from the single-query lane; the ids and the
        # ranking are exact. Same tolerance convention as
        # test_vector_ops.py::test_batch_topk_distance_matches_single_queries.
        assert batch_distances == pytest.approx(default_distances, rel=1e-5, abs=1e-5), (
            f"{context} (query {index}): batch distances {batch_distances} "
            f"!= non-raw distances {default_distances}"
        )
    return batch


def test_update_of_a_non_vector_column_keeps_raw_after_update_flush_and_reopen():
    """UPDATE label only: raw must work immediately, after flush, and after reopen."""
    with tempfile.TemporaryDirectory() as tmp:
        client, _matrix, queries = _make_disk_client(tmp)
        try:
            before = [
                _raw_topk(client, query, context="before UPDATE") for query in queries
            ]

            updated = client.execute("UPDATE docs SET label = 1234 WHERE label < 5").to_dict()
            assert updated == [{"result": 5}], f"unexpected UPDATE result: {updated}"

            for index, query in enumerate(queries):
                raw = _assert_raw_matches_default(
                    client, query, context="immediately after UPDATE"
                )
                assert raw == before[index], (
                    f"immediately after UPDATE (query {index}): raw ranking {raw} "
                    f"!= pre-UPDATE ranking {before[index]}"
                )

            client.flush()
            for query in queries:
                _assert_raw_matches_default(client, query, context="after flush")
        finally:
            client.close()

        reopened = _reopen_disk_client(tmp)
        try:
            for query in queries:
                _assert_raw_matches_default(
                    reopened, query, context="after close and reopen"
                )
        finally:
            reopened.close()


def test_hybrid_search_after_unrelated_update_matches_pre_update_ranking():
    """hybrid_search must stay available and unchanged after an unrelated UPDATE."""
    with tempfile.TemporaryDirectory() as tmp:
        client, matrix, _queries = _make_disk_client(tmp, fts=True)
        query = matrix[0].copy()
        try:
            before_rrf = _hybrid(client, query, "alpha", fusion="rrf", context="before UPDATE")
            before_weighted = _hybrid(
                client,
                query,
                "alpha",
                fusion="weighted",
                context="before UPDATE",
            )
            assert len(before_rrf) == 6, before_rrf
            assert len(before_weighted) == 6, before_weighted

            updated = client.execute("UPDATE docs SET label = 1234 WHERE label > 20").to_dict()
            assert updated == [{"result": 11}], f"unexpected UPDATE result: {updated}"

            after_rrf = _hybrid(
                client, query, "alpha", fusion="rrf", context="immediately after UPDATE"
            )
            after_weighted = _hybrid(
                client,
                query,
                "alpha",
                fusion="weighted",
                context="immediately after UPDATE",
            )
            assert after_rrf == before_rrf, (
                f"hybrid_search(rrf) changed after an unrelated UPDATE: "
                f"{after_rrf} != {before_rrf}"
            )
            assert after_weighted == before_weighted, (
                f"hybrid_search(weighted) changed after an unrelated UPDATE: "
                f"{after_weighted} != {before_weighted}"
            )

            client.flush()
            assert (
                _hybrid(client, query, "alpha", fusion="rrf", context="after flush")
                == before_rrf
            )
            assert (
                _hybrid(
                    client,
                    query,
                    "alpha",
                    fusion="weighted",
                    context="after flush",
                )
                == before_weighted
            )
        finally:
            client.close()

        reopened = _reopen_disk_client(tmp)
        try:
            reopened_rrf = _hybrid(
                reopened, query, "alpha", fusion="rrf", context="after close and reopen"
            )
            reopened_weighted = _hybrid(
                reopened,
                query,
                "alpha",
                fusion="weighted",
                context="after close and reopen",
            )
            assert reopened_rrf == before_rrf, (
                f"hybrid_search(rrf) changed after reopen: {reopened_rrf} != {before_rrf}"
            )
            assert reopened_weighted == before_weighted, (
                f"hybrid_search(weighted) changed after reopen: "
                f"{reopened_weighted} != {before_weighted}"
            )
        finally:
            reopened.close()


def test_small_delete_keeps_raw_available_and_matches_default():
    """A small DELETE leaves the vectors untouched; raw must keep the same ranking."""
    with tempfile.TemporaryDirectory() as tmp:
        client, _matrix, queries = _make_disk_client(tmp)
        query = queries[0]
        try:
            deleted_ids = {
                row["_id"]
                for row in client.execute("SELECT _id FROM docs WHERE label < 2").to_dict()
            }
            assert len(deleted_ids) == 2, deleted_ids

            deleted = client.execute("DELETE FROM docs WHERE label < 2").to_dict()
            assert deleted == [{"result": 2}], f"unexpected DELETE result: {deleted}"

            raw = _assert_raw_matches_default(
                client, query, context="immediately after DELETE"
            )
            assert not (set(raw["ids"]) & deleted_ids), (
                f"deleted ids {deleted_ids} still appear in raw ranking {raw['ids']}"
            )

            client.flush()
            _assert_raw_matches_default(client, query, context="after DELETE + flush")
        finally:
            client.close()

        reopened = _reopen_disk_client(tmp)
        try:
            _assert_raw_matches_default(
                reopened, query, context="after DELETE + close and reopen"
            )
        finally:
            reopened.close()


def _null_vector_ids(client):
    return [
        row["_id"]
        for row in client.execute(
            "SELECT _id FROM docs WHERE vec IS NULL ORDER BY _id"
        ).to_dict()
    ]


def _vector_of(client, row_id):
    # A full-column scan goes through the Arrow/DeltaMerger path and returns
    # decoded float lists; the single-row point lane has its own binary shape,
    # so use it only for the presence/absence check below.
    rows = client.execute("SELECT _id, vec FROM docs ORDER BY _id").to_dict()
    values = {row["_id"]: row["vec"] for row in rows}
    assert row_id in values, row_id
    return values[row_id]


def test_nulling_vector_cells_reaches_reads_and_raw_ranking():
    """`UPDATE ... SET vec = NULL` must really null the vector cells.

    In 2.0.0 the executor reported the matched count but the DeltaStore overlay
    for a fixed-list vector column was ignored by ``DeltaMerger``: ``SELECT
    vec`` kept returning the pre-UPDATE values and even ``vec IS NULL`` counted
    zero rows. The overlay now applies the NULL assignment, so the scans, the
    raw/hybrid fast lane and the point reader must all agree with each other.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client, matrix, queries = _make_disk_client(tmp)
        query = queries[0]
        try:
            updated = client.execute(
                "UPDATE docs SET vec = NULL WHERE label < 3"
            ).to_dict()
            assert updated == [{"result": 3}], f"unexpected UPDATE result: {updated}"

            # The stored cells are NULL now, and only those three rows changed.
            assert _null_vector_ids(client) == [1, 2, 3]
            assert _vector_of(client, 1) is None
            assert _vector_of(client, 4) is not None
            np.testing.assert_array_equal(
                np.asarray(_vector_of(client, 4), dtype=np.float32), matrix[3]
            )
            assert client.retrieve(2)["vec"] is None, "point reader must see the NULL"

            # The exact vector branch ignores NULL vectors, so the nulled ids
            # disappear from both the raw and the default ranking.
            raw = _assert_raw_matches_default(
                client, query, context="immediately after vec = NULL UPDATE"
            )
            assert not (set(raw["ids"]) & {1, 2, 3}), raw["ids"]

            client.flush()
            assert _null_vector_ids(client) == [1, 2, 3]
            _assert_raw_matches_default(
                client, query, context="after vec = NULL UPDATE + flush"
            )
        finally:
            client.close()

        reopened = _reopen_disk_client(tmp)
        try:
            assert _null_vector_ids(reopened) == [1, 2, 3]
            raw = _assert_raw_matches_default(
                reopened, query, context="after vec = NULL UPDATE + close and reopen"
            )
            assert not (set(raw["ids"]) & {1, 2, 3}), raw["ids"]
        finally:
            reopened.close()


def test_batch_topk_distance_after_update_matches_raw_and_default():
    """batch_topk_distance must serve the post-UPDATE table state with the same ranking."""
    with tempfile.TemporaryDirectory() as tmp:
        client, _matrix, queries = _make_disk_client(tmp)
        try:
            updated = client.execute("UPDATE docs SET label = 1234 WHERE label < 4").to_dict()
            assert updated == [{"result": 4}], f"unexpected UPDATE result: {updated}"

            _assert_batch_matches_default(
                client, queries, context="immediately after UPDATE"
            )

            client.flush()
            _assert_batch_matches_default(
                client, queries, context="after UPDATE + flush"
            )
        finally:
            client.close()

        reopened = _reopen_disk_client(tmp)
        try:
            _assert_batch_matches_default(
                reopened, queries, context="after UPDATE + close and reopen"
            )
        finally:
            reopened.close()


def test_memory_client_raw_topk_and_hybrid_available_before_and_after_update():
    """:memory: must serve raw=True and hybrid_search (both raised RuntimeError in 2.0.0).

    The fused ranking is also compared against a directory-backed client holding
    the same rows, so the in-memory fallback path must agree with the on-disk
    fallback path.
    """
    matrix, queries, contents, labels = _dataset()
    query = queries[0]
    client = ApexClient(":memory:")
    try:
        client.create_table(
            "docs", {"content": "string", "label": "int64", "vec": "float32_vector"}
        )
        client.use_table("docs")
        client.init_fts(index_fields=["content"])
        client.store(
            {
                "content": contents,
                "label": labels,
                "vec": [matrix[row] for row in range(ROWS)],
            }
        )
        client.flush()

        before = _assert_raw_matches_default(client, query, context=":memory: before UPDATE")
        before_rrf = _hybrid(client, query, "alpha", fusion="rrf", context=":memory: before UPDATE")
        before_weighted = _hybrid(
            client, query, "alpha", fusion="weighted", context=":memory: before UPDATE"
        )
        assert len(before_rrf) == 6, before_rrf
        assert len(before_weighted) == 6, before_weighted

        updated = client.execute("UPDATE docs SET label = 1234 WHERE label < 4").to_dict()
        assert updated == [{"result": 4}], f"unexpected UPDATE result: {updated}"

        after = _assert_raw_matches_default(
            client, query, context=":memory: immediately after UPDATE"
        )
        assert after == before, (
            f":memory: raw ranking changed after an unrelated UPDATE: {after} != {before}"
        )

        after_rrf = _hybrid(
            client, query, "alpha", fusion="rrf", context=":memory: immediately after UPDATE"
        )
        after_weighted = _hybrid(
            client,
            query,
            "alpha",
            fusion="weighted",
            context=":memory: immediately after UPDATE",
        )
        assert after_rrf == before_rrf, (
            f":memory: hybrid_search(rrf) changed after an unrelated UPDATE: "
            f"{after_rrf} != {before_rrf}"
        )
        assert after_weighted == before_weighted, (
            f":memory: hybrid_search(weighted) changed after an unrelated UPDATE: "
            f"{after_weighted} != {before_weighted}"
        )

        # Cross-backend parity: identical data and UPDATE on disk must fuse the
        # same ids with the same scores.
        with tempfile.TemporaryDirectory() as tmp:
            disk, _matrix, _queries = _make_disk_client(tmp, fts=True)
            try:
                disk.execute("UPDATE docs SET label = 1234 WHERE label < 4")
                disk_rrf = _hybrid(
                    disk, query, "alpha", fusion="rrf", context="disk reference after UPDATE"
                )
                disk_weighted = _hybrid(
                    disk,
                    query,
                    "alpha",
                    fusion="weighted",
                    context="disk reference after UPDATE",
                )
            finally:
                disk.close()
        assert after_rrf == disk_rrf, (
            f":memory: hybrid_search(rrf) {after_rrf} != disk hybrid_search(rrf) {disk_rrf}"
        )
        assert after_weighted == disk_weighted, (
            f":memory: hybrid_search(weighted) {after_weighted} != "
            f"disk hybrid_search(weighted) {disk_weighted}"
        )
    finally:
        client.close()
