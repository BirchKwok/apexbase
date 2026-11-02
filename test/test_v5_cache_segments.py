"""S8: memory/cache closure.

Covers the segmented page-cache invalidation contract (append / update /
delete / delta overlay never serve a stale row through any read lane) and the
bounded transaction bookkeeping (long single-shot txn loops do not grow the
committed-write log or keep one empty version store per table).
"""

import gc

from apexbase import ApexClient


def _rows(start, count):
    return {
        "id": list(range(start, start + count)),
        "v": [i * 10 for i in range(start, start + count)],
        "s": [f"row_{i:06d}" for i in range(start, start + count)],
    }


def _point(client, row_id):
    rows = client.execute(
        f"SELECT id, v, s FROM seg_t WHERE id = {row_id}"
    ).to_dict()
    assert len(rows) <= 1, (row_id, rows)
    return rows[0] if rows else None


def _close(client):
    client.close()


def test_append_update_delete_and_overlay_read_after_write(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        client.create_table("seg_t", {"id": "int64", "v": "int64", "s": "string"})
        client.use_table("seg_t")
        client.store(_rows(0, 20000))
        client.flush()

        # Warm every lane the tests exercise so a stale cache would surface.
        for row_id in (0, 1, 9999, 19999):
            assert _point(client, row_id) is not None
        assert client.execute("SELECT COUNT(*) FROM seg_t").scalar() == 20000

        # Append: old rows stay readable and the new tail becomes visible.
        client.store(_rows(20000, 3000))
        client.flush()
        assert client.execute("SELECT COUNT(*) FROM seg_t").scalar() == 23000
        for row_id in (0, 9999, 19999):
            assert _point(client, row_id) is not None, row_id
        new_row = _point(client, 20000)
        assert new_row == {"id": 20000, "v": 200000, "s": "row_020000"}, new_row
        assert _point(client, 22999) is not None

        # Overlay: a pending (not yet flushed) store is visible immediately and
        # must not be shadowed by cached base pages.
        client.store(_rows(23000, 3))
        assert client.execute("SELECT COUNT(*) FROM seg_t").scalar() == 23003
        assert _point(client, 23002) is not None
        client.flush()

        # Cell update replaces both the value and a string column.
        client.replace(100 + 1, {"id": 100, "v": -7, "s": "updated_100"})
        assert _point(client, 100) == {"id": 100, "v": -7, "s": "updated_100"}

        # SQL UPDATE over a range, read through the point lane and a scan.
        client.execute("UPDATE seg_t SET v = 555 WHERE id BETWEEN 50 AND 60")
        assert _point(client, 55)["v"] == 555
        assert (
            client.execute(
                "SELECT COUNT(*) FROM seg_t WHERE v = 555"
            ).scalar()
            == 11
        )

        # Deleting rows through both the SQL and native lanes must not resurrect
        # the deleted values from the warm cache.
        client.execute("DELETE FROM seg_t WHERE id BETWEEN 200 AND 210")
        assert client.execute("SELECT COUNT(*) FROM seg_t").scalar() == 23003 - 11
        assert _point(client, 205) is None
        client.delete(20000 + 1)  # native delete uses the internal _id (data id + 1)
        assert _point(client, 20000) is None
        assert _point(client, 20001) is not None

        # A full rewrite (compaction) retires every cached page too.
        client.flush()
        client.replace(1 + 1, {"id": 1, "v": 123456, "s": "after_rewrite"})
        assert _point(client, 1) == {"id": 1, "v": 123456, "s": "after_rewrite"}
    finally:
        _close(client)


def test_compressed_table_cache_coherence(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        client.create_table("seg_t", {"id": "int64", "v": "int64", "s": "string"})
        client.use_table("seg_t")
        assert client.set_compression("zstd") is not False
        client.store(_rows(0, 8000))
        client.flush()

        assert _point(client, 4000)["v"] == 40000
        client.store(_rows(8000, 1000))
        client.flush()
        assert _point(client, 4000)["v"] == 40000
        assert _point(client, 8000)["v"] == 80000
        client.replace(4000 + 1, {"id": 4000, "v": -1, "s": "zstd_updated"})
        assert _point(client, 4000) == {"id": 4000, "v": -1, "s": "zstd_updated"}
        client.execute("DELETE FROM seg_t WHERE id = 8000")
        assert _point(client, 8000) is None
    finally:
        _close(client)


def test_long_txn_loops_keep_conflict_log_bounded(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        client.create_table("seg_t", {"id": "int64", "v": "int64", "s": "string"})
        client.use_table("seg_t")
        client.store(_rows(0, 1000))
        client.flush()
        gc.collect()
        before = client._storage.txn_diagnostics()

        for i in range(64):
            client.execute("BEGIN")
            client.execute(f"UPDATE seg_t SET v = {i} WHERE id = {i % 1000}")
            client.execute("COMMIT")
            client.execute(f"SELECT COUNT(*) FROM seg_t WHERE v = {i}")

        after = client._storage.txn_diagnostics()
        version_stores, committed_writes, active_snapshots = after
        assert active_snapshots == 0, after
        # Every commit happened with no overlapping snapshot, so the
        # committed-write log is trimmed instead of growing for 64 iterations.
        assert committed_writes == 0, after
        # Only the one written table legitimately keeps a version store.
        assert version_stores - before[0] <= 2, (before, after)
    finally:
        _close(client)
