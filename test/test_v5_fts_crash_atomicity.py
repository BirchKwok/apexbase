"""S9: FTS crash atomicity / stale reconciliation.

A recovered FTS index must either account for every table row (snapshot + WAL
generation replay) or be explicitly rejected; it must never silently answer
MATCH() from an incomplete index. Rebuilding via init_fts() restores coverage.
"""

import os
import shutil

import pytest

from apexbase import ApexClient

TABLE = "docs"
BEFORE = 100
AFTER = 50


def _titles(start, count):
    return {
        "title": [
            f"alpha document {i}" if i % 2 == 0 else f"beta document {i}"
            for i in range(start, start + count)
        ]
    }


def _paths(tmp_path):
    return (
        tmp_path / "fts_indexes" / f"{TABLE}.afts",
        tmp_path / "fts_indexes" / f"{TABLE}.afts.wal",
        tmp_path / "fts_indexes" / f"{TABLE}.afts.meta",
    )


def _prepare(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True)
    client.create_table(TABLE, {"title": "string"})
    client.use_table(TABLE)
    client.store(_titles(0, BEFORE))
    client.flush()
    client.init_fts()
    # Let the initial backfill finish its async snapshot before appending the
    # post-snapshot rows, so the fixture deterministically has a WAL tail.
    client._storage._fts_flush()
    client.store(_titles(BEFORE, AFTER))
    client.flush()
    client.close()
    return _paths(tmp_path)


def _open(tmp_path):
    client = ApexClient(str(tmp_path))
    client.use_table(TABLE)
    return client


def _alpha_count(client):
    return client.execute(
        f"SELECT COUNT(*) FROM {TABLE} WHERE MATCH('alpha')"
    ).scalar()


def _assert_recovered_after_reopen(tmp_path):
    """After a rebuild the index is verifiable again on the next reopen."""
    client = _open(tmp_path)
    try:
        assert _alpha_count(client) == (BEFORE + AFTER) // 2
    finally:
        client.close()


def test_clean_reopen_answers_from_replayed_generation(tmp_path):
    _prepare(tmp_path)
    client = _open(tmp_path)
    try:
        # 75 alpha + 75 beta documents; WAL replay must recover the 50 rows
        # written after the snapshot without forcing a rebuild.
        assert _alpha_count(client) == (BEFORE + AFTER) // 2
    finally:
        client.close()


def test_lost_wal_tail_is_rejected_then_rebuilt(tmp_path):
    snapshot, wal, _ = _prepare(tmp_path)
    assert snapshot.exists() and wal.exists()
    os.remove(wal)

    client = _open(tmp_path)
    try:
        # The recovered index cannot account for the lost WAL generation. The
        # implementation may either reject the query explicitly or rebuild from
        # the table on open (the design allows both); it must never answer from
        # partial coverage. Which path runs first depends on the platform's
        # config-reload timing, so both are accepted here.
        try:
            covered = _alpha_count(client)
        except Exception as error:  # noqa: BLE001 - explicit stale rejection
            message = str(error).lower()
            assert "stale" in message or "rebuild" in message, message
            client.init_fts()
            covered = _alpha_count(client)
        assert covered == (BEFORE + AFTER) // 2
        assert (tmp_path / "fts_indexes" / f"{TABLE}.afts.stale").exists()
        assert snapshot.exists()
        assert (tmp_path / "fts_indexes" / f"{TABLE}.afts.meta").exists()
    finally:
        client.close()
    _assert_recovered_after_reopen(tmp_path)


def test_truncated_wal_tail_is_detected(tmp_path):
    snapshot, wal, _ = _prepare(tmp_path)
    size = wal.stat().st_size
    assert size > 8
    with open(wal, "r+b") as handle:
        handle.truncate(size // 2)

    client = _open(tmp_path)
    try:
        try:
            assert _alpha_count(client) == (BEFORE + AFTER) // 2
        except Exception as error:  # noqa: BLE001 - the gate may reject first
            assert "stale" in str(error).lower() or "rebuild" in str(error).lower()
            client.init_fts()
        assert _alpha_count(client) == (BEFORE + AFTER) // 2
    finally:
        client.close()
    _assert_recovered_after_reopen(tmp_path)


def test_corrupt_snapshot_is_rejected_then_rebuilt(tmp_path):
    snapshot, _, _ = _prepare(tmp_path)
    snapshot_bytes = snapshot.read_bytes()
    assert len(snapshot_bytes) > 32
    corrupted = bytearray(snapshot_bytes)
    corrupted[12] ^= 0xFF
    snapshot.write_bytes(corrupted)

    client = _open(tmp_path)
    try:
        assert _alpha_count(client) == (BEFORE + AFTER) // 2
        assert (tmp_path / "fts_indexes" / f"{TABLE}.afts.incompatible").exists()
    finally:
        client.close()
    _assert_recovered_after_reopen(tmp_path)
