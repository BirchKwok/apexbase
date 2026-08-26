"""Feature parity between disk-backed and in-memory (``:memory:``) clients.

The in-memory engine must expose every ApexBase feature with identical
observable behaviour to a directory-backed database; the only permitted
difference is filesystem I/O.
"""

from pathlib import Path

import numpy as np
import pytest

from apexbase import ApexClient


def _scenario(client):
    """Run a representative feature sweep and return a comparable transcript."""
    out = []

    # DDL with constraints and defaults
    client.execute(
        "CREATE TABLE items ("
        "id BIGINT PRIMARY KEY, "
        "name TEXT NOT NULL, "
        "price FLOAT64 DEFAULT 0.0)"
    )

    # DML: positional, named, and multi-row inserts
    client.execute(
        "INSERT INTO items (id, name, price) VALUES (?, ?, ?)",
        params=[1, "one", 1.5],
    )
    client.execute(
        "INSERT INTO items (id, name, price) VALUES (:id, :name, :price)",
        params={"id": 2, "name": "two", "price": 2.5},
    )
    client.execute("INSERT INTO items VALUES (3, 'three', 3.5), (4, 'four', 4.5)")

    out.append(("count", client.execute("SELECT COUNT(*) FROM items").scalar()))
    out.append(
        (
            "ordered",
            client.execute(
                "SELECT id, name FROM items WHERE id >= 2 ORDER BY id DESC"
            ).to_dict(),
        )
    )
    out.append(
        (
            "aggregate",
            client.execute(
                "SELECT name, SUM(price) AS total, COUNT(*) AS n "
                "FROM items GROUP BY name ORDER BY name"
            ).to_dict(),
        )
    )

    # UPDATE / DELETE
    client.execute("UPDATE items SET price = 9.0 WHERE id = 1")
    client.execute("DELETE FROM items WHERE id = 4")
    out.append(
        (
            "after_dml",
            client.execute("SELECT id, price FROM items ORDER BY id").to_dict(),
        )
    )

    # ALTER TABLE: add / rename / drop column
    client.execute("ALTER TABLE items ADD COLUMN note TEXT")
    client.execute("UPDATE items SET note = 'n' WHERE id = 1")
    out.append(("added_col", client.execute("SELECT note FROM items WHERE id = 1").to_dict()))
    client.execute("ALTER TABLE items RENAME COLUMN note TO remark")
    out.append(
        ("renamed_col", client.execute("SELECT remark FROM items WHERE id = 1").to_dict())
    )
    client.execute("ALTER TABLE items DROP COLUMN remark")
    out.append(
        (
            "dropped_col",
            client.execute("SELECT id, name FROM items WHERE id = 1").to_dict(),
        )
    )

    # Views
    client.execute("CREATE VIEW cheap AS SELECT id, name FROM items WHERE price < 5")
    out.append(("view", client.execute("SELECT * FROM cheap ORDER BY id").to_dict()))
    client.execute("DROP VIEW cheap")

    # Index
    client.execute("CREATE INDEX idx_items_name ON items (name)")
    out.append(
        (
            "indexed",
            client.execute("SELECT name FROM items WHERE name = 'one'").to_dict(),
        )
    )

    # Transactions
    client.execute("BEGIN")
    client.execute("INSERT INTO items (id, name) VALUES (100, 'txn')")
    client.execute("ROLLBACK")
    out.append(("after_rollback", client.execute("SELECT COUNT(*) FROM items").scalar()))
    client.execute("BEGIN")
    client.execute("INSERT INTO items (id, name) VALUES (101, 'committed')")
    client.execute("COMMIT")
    out.append(("after_commit", client.execute("SELECT COUNT(*) FROM items").scalar()))

    # TRUNCATE
    client.execute("TRUNCATE TABLE items")
    out.append(("after_truncate", client.execute("SELECT COUNT(*) FROM items").scalar()))

    # Named database
    client.use_database("analytics")
    client.execute("CREATE TABLE metrics (value BIGINT)")
    client.execute("INSERT INTO metrics VALUES (42)")
    out.append(("named_db", client.execute("SELECT value FROM metrics").scalar()))
    # ``indexes`` is a disk-only physical directory that legacy ``list_databases``
    # surfaces; exclude it so the logical database set is compared.
    out.append(
        (
            "databases",
            sorted(d for d in client.list_databases() if d != "indexes"),
        )
    )
    out.append(("tables", sorted(client.list_tables())))
    client.use_database("default")

    return out


def test_disk_and_memory_clients_have_identical_behavior(tmp_path):
    disk = ApexClient(tmp_path / "db")
    memory = ApexClient(":memory:")
    try:
        disk_out = _scenario(disk)
        memory_out = _scenario(memory)
        assert disk_out == memory_out
    finally:
        disk.close()
        memory.close()


def test_memory_client_creates_no_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = ApexClient(":memory:")
    try:
        _scenario(client)
        assert list(tmp_path.iterdir()) == []
    finally:
        client.close()


def test_memory_client_blob_and_fts_parity(tmp_path):
    def blob_and_fts(client):
        client.create_table("files", {"name": "string", "payload": "blob"})
        payload = b"apexbase-memory-parity-payload"
        client.store({"name": ["a.bin"], "payload": [payload]})
        blob = client.read_blob("payload", 1)

        client.create_table("docs")
        client.init_fts(index_fields=["body"])
        client.store(
            [
                {"body": "the quick brown fox jumps over the lazy dog"},
                {"body": "python is a great programming language"},
            ]
        )
        hits = client.search_text("python")
        return blob, int(len(hits))

    disk = ApexClient(tmp_path / "db")
    memory = ApexClient(":memory:")
    try:
        assert blob_and_fts(disk) == blob_and_fts(memory)
    finally:
        disk.close()
        memory.close()


def test_memory_fast_path_update_delete_leaves_no_sidecar_files(tmp_path, monkeypatch):
    """The unconstrained UPDATE/DELETE fast paths must not write delta sidecars."""
    monkeypatch.chdir(tmp_path)
    client = ApexClient(":memory:")
    try:
        client.execute("CREATE TABLE t (id BIGINT, name TEXT)")
        client.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two')")
        client.execute("UPDATE t SET name = 'ONE' WHERE id = 1")
        client.execute("DELETE FROM t WHERE id = 2")
        assert client.execute("SELECT name FROM t ORDER BY id").to_dict() == [
            {"name": "ONE"}
        ]
        assert list(tmp_path.iterdir()) == []
    finally:
        client.close()


def test_memory_client_closes_ephemeral_cleanly(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = ApexClient(":memory:")
    client.execute("CREATE TABLE t (value BIGINT)")
    client.execute("INSERT INTO t VALUES (7)")
    assert client.execute("SELECT value FROM t").scalar() == 7
    client.close()
    assert list(tmp_path.iterdir()) == []
