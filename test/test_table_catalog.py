"""Tests for the per-database table metadata registry (.apex_tables.json)."""

from __future__ import annotations

import multiprocessing
from pathlib import Path

import pytest

from apexbase import ApexClient


def _client(tmp_path, name: str) -> ApexClient:
    return ApexClient(dirpath=str(tmp_path / name), drop_if_exists=True)


def _meta_path(db: str) -> Path:
    return Path(db) / ".apex_tables"


def _read_meta(db: str) -> bytes:
    return _meta_path(db).read_bytes()


def test_create_table_writes_metadata_registry(tmp_path):
    client = _client(tmp_path, "meta")
    try:
        client.create_table("videos", {"name": "string"})
        client.create_table("frames", {"ts": "int64"})
        meta = _read_meta(str(tmp_path / "meta"))
        # Binary registry: magic header + integrity checksum, not JSON text.
        assert meta[:8] == b"APXTBL02"
        assert meta[0:1] != b"{"
        assert set(client.list_tables()) == {"frames", "videos"}
    finally:
        client.close()


def test_metadata_registry_is_authoritative_across_reopen(tmp_path):
    db = str(tmp_path / "db")
    writer = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        writer.create_table("videos", {"name": "string"})
        writer.store({"name": "v1"})
    finally:
        writer.close()

    reader = ApexClient(dirpath=db)
    try:
        with pytest.raises(ValueError, match="Table already exists"):
            reader.create_table("videos", {"name": "string"})
        reader.use_table("videos")
        assert reader.count_rows("videos") == 1
        assert reader.list_tables() == ["videos"]
    finally:
        reader.close()


def test_sql_ddl_updates_registry_and_client_cache(tmp_path):
    client = _client(tmp_path, "sql_ddl")
    try:
        client.execute("CREATE TABLE t (k INT64)")
        assert set(client.list_tables()) == {"t"}

        client.execute("DROP TABLE t")
        assert client.list_tables() == []
        # The stale client-side cache entry must be gone after SQL DROP.
        with pytest.raises(ValueError, match="Table not found"):
            client.use_table("t")

        client.create_table("t", {"k": "int64"})
        assert client.count_rows("t") == 0
        client.store({"k": 1})
        assert client.count_rows("t") == 1
    finally:
        client.close()


def test_legacy_database_without_metadata_is_backfilled(tmp_path):
    db = str(tmp_path / "legacy")
    writer = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        writer.create_table("old", {"k": "int64"})
        writer.store({"k": 1})
    finally:
        writer.close()

    # Simulate a legacy database that predates the metadata registry.
    _meta_path(db).unlink()

    reader = ApexClient(dirpath=db)
    try:
        assert reader.list_tables() == ["old"]
        reader.use_table("old")
        assert reader.count_rows("old") == 1
        # Creating a new table persists the backfilled registry (old + new).
        reader.create_table("new", {"k": "int64"})
        assert set(reader.list_tables()) == {"new", "old"}
    finally:
        reader.close()


def test_temp_tables_are_not_part_of_the_registry(tmp_path):
    client = _client(tmp_path, "temp")
    try:
        client.create_table("base", {"k": "int64"})
        csv_path = tmp_path / "rows.csv"
        csv_path.write_text("id\n1\n2\n", encoding="utf-8")
        client.register_temp_table("imported", str(csv_path))
        assert client.list_tables() == ["base"]
        client.drop_temp_table("imported")
    finally:
        client.close()


def test_drop_if_exists_clears_metadata_registry(tmp_path):
    db = str(tmp_path / "drop")
    writer = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        writer.create_table("t", {"k": "int64"})
    finally:
        writer.close()
    assert _meta_path(db).exists()

    fresh = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        assert fresh.list_tables() == []
        assert not _meta_path(db).exists()
        fresh.create_table("t", {"k": "int64"})
        assert fresh.list_tables() == ["t"]
    finally:
        fresh.close()


def test_tampered_registry_is_rejected(tmp_path):
    db = str(tmp_path / "tamper")
    writer = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        writer.create_table("t", {"k": "int64"})
    finally:
        writer.close()

    meta_path = _meta_path(db)
    data = meta_path.read_bytes()
    # Flip a byte inside the first used slot's name region (32-byte header,
    # slot layout: 4-byte name_len + name bytes).
    flip = 32 + 8
    meta_path.write_bytes(data[:flip] + bytes([data[flip] ^ 0xFF]) + data[flip + 1:])

    reader = ApexClient(dirpath=db)
    try:
        with pytest.raises(OSError, match="checksum"):
            reader.list_tables()
        with pytest.raises(OSError, match="checksum"):
            reader.use_table("t")
    finally:
        reader.close()


def _race_create(db: str, name: str, queue):
    client = ApexClient(dirpath=db)
    try:
        client.create_table(name, {"k": "int64"})
        queue.put("ok")
    except Exception as exc:  # pragma: no cover - exact error varies
        queue.put(f"err:{type(exc).__name__}")
    finally:
        client.close()


def test_concurrent_create_table_is_serialized_by_registry_lock(tmp_path):
    """Two processes creating the same table must yield exactly one winner."""
    db = str(tmp_path / "race")
    initial = ApexClient(dirpath=db, drop_if_exists=True)
    initial.close()

    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    procs = [
        ctx.Process(target=_race_create, args=(db, "t", queue))
        for _ in range(2)
    ]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join(timeout=60)
    outcomes = [queue.get(timeout=5) for _ in procs]

    assert outcomes.count("ok") == 1, outcomes
    assert any(outcome.startswith("err:") for outcome in outcomes), outcomes

    verify = ApexClient(dirpath=db)
    try:
        verify.use_table("t")
        assert verify.count_rows("t") == 0
        assert verify.list_tables() == ["t"]
    finally:
        verify.close()
