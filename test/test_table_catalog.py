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


def test_create_table_defers_file_until_first_access(tmp_path):
    db = str(tmp_path / "lazy")
    client = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        client.create_table("t", {"k": "int64"})
        table_file = tmp_path / "lazy" / "t.apex"
        # CREATE is metadata-only: no per-table file until first real access.
        assert not table_file.exists()
        assert client.list_tables() == ["t"]
        client.use_table("t")
        assert client.count_rows() == 0
        client.store({"k": 1})
        assert table_file.exists()
        assert client.count_rows() == 1
    finally:
        client.close()


def test_lazy_schema_survives_reopen_and_typed_write(tmp_path):
    db = str(tmp_path / "lazy_schema")
    writer = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        writer.create_table("t", {"k": "int64"})
    finally:
        writer.close()
    assert not (tmp_path / "lazy_schema" / "t.apex").exists()

    reader = ApexClient(dirpath=db)
    try:
        reader.use_table("t")
        reader.store({"k": 42})
        assert reader.count_rows("t") == 1
        rows = reader.execute("SELECT * FROM t")
        assert rows[0]["k"] == 42
    finally:
        reader.close()


def test_lazy_table_sql_select_describe_alter_truncate(tmp_path):
    db = str(tmp_path / "lazy_sql")
    client = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        client.create_table("t", {"k": "int64"})
        table_file = tmp_path / "lazy_sql" / "t.apex"
        # ALTER/TRUNCATE on a not-yet-materialized table are schema-only.
        client.execute("ALTER TABLE t ADD COLUMN c INT64")
        client.execute("TRUNCATE TABLE t")
        assert not table_file.exists()
        # First read materializes the file with the full schema.
        assert len(client.execute("SELECT * FROM t")) == 0
        desc = client.execute("DESCRIBE t")
        assert any(row["column_name"] == "k" for row in desc)
        assert any(row["column_name"] == "c" for row in desc)
        assert table_file.exists()
        client.store({"k": 1, "c": 2})
        assert client.count_rows("t") == 1
    finally:
        client.close()


def test_lazy_table_drop_without_materialization(tmp_path):
    db = str(tmp_path / "lazy_drop")
    client = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        client.create_table("t", {"k": "int64"})
        assert not (tmp_path / "lazy_drop" / "t.apex").exists()
        client.drop_table("t")
        assert client.list_tables() == []
        # Recreate after drop works and the schema sidecar is clean.
        client.create_table("t", {"k": "int64"})
        client.store({"k": 1})
        assert client.count_rows("t") == 1
    finally:
        client.close()


def test_sql_create_table_defers_file_and_preserves_constraints(tmp_path):
    db = str(tmp_path / "lazy_sql_constraints")
    client = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        client.execute(
            "CREATE TABLE t (k INT NOT NULL DEFAULT 7, s TEXT "
            "CHECK (LENGTH(s) > 0))"
        )
        table_file = tmp_path / "lazy_sql_constraints" / "t.apex"
        assert not table_file.exists()

        # DEFAULT fills the omitted column on first (materializing) write.
        client.execute("INSERT INTO t (s) VALUES ('ok')")
        assert table_file.exists()
        rows = client.execute("SELECT k, s FROM t")
        assert list(rows) == [{"k": 7, "s": "ok"}]

        with pytest.raises(Exception):
            client.execute("INSERT INTO t (s) VALUES ('')")
        with pytest.raises(Exception):
            client.execute("INSERT INTO t (k, s) VALUES (NULL, 'x')")
    finally:
        client.close()

    # Constraints survive reopen because they were persisted in the schema
    # sidecar and then materialized into the file footer.
    reader = ApexClient(dirpath=db)
    try:
        reader.use_table("t")
        with pytest.raises(Exception):
            reader.execute("INSERT INTO t (s) VALUES ('')")
        reader.execute("INSERT INTO t (s) VALUES ('after-reopen')")
        rows = reader.execute("SELECT k, s FROM t ORDER BY s")
        assert list(rows) == [
            {"k": 7, "s": "after-reopen"},
            {"k": 7, "s": "ok"},
        ]
    finally:
        reader.close()


def test_sql_create_table_autoincrement_and_foreign_key(tmp_path):
    db = str(tmp_path / "lazy_sql_fk")
    client = ApexClient(dirpath=db, drop_if_exists=True)
    try:
        client.execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        client.execute(
            "CREATE TABLE child (id INT PRIMARY KEY AUTOINCREMENT, "
            "pid INT REFERENCES parent(id))"
        )
        # Both tables are lazy until the first write materializes them.
        assert not (tmp_path / "lazy_sql_fk" / "parent.apex").exists()
        assert not (tmp_path / "lazy_sql_fk" / "child.apex").exists()

        client.execute("INSERT INTO parent (id) VALUES (1)")
        client.execute("INSERT INTO child (pid) VALUES (1)")
        rows = client.execute("SELECT id, pid FROM child")
        assert list(rows) == [{"id": 1, "pid": 1}]

        with pytest.raises(Exception):
            client.execute("INSERT INTO child (pid) VALUES (999)")
    finally:
        client.close()


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
