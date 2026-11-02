"""S11: out-of-core ORDER BY (spill) semantics, cleanup and explicit failures.

The spilled sort must produce exactly the in-memory result, remove its
per-query spill area on success and failure, and remove stale areas left by
crashed processes.
"""

import os
import time

import pytest

from apexbase import ApexClient

LIMIT_ENV = "APEX_QUERY_MEMORY_MB"
ROWS = 150_000


def _client(tmp_path, name="spill"):
    client = ApexClient(str(tmp_path / name), drop_if_exists=True)
    client.create_table("t", {"k": "int64", "s": "string"})
    client.use_table("t")
    client.store(
        {
            "k": list(range(ROWS)),
            "s": [f"v{i % 997:03d}" for i in range(ROWS)],
        }
    )
    client.flush()
    return client


def test_spilled_order_by_matches_unlimited_result(tmp_path, monkeypatch):
    client = _client(tmp_path)
    try:
        unlimited = client.execute("SELECT k, s FROM t ORDER BY k DESC").to_dict()
        assert len(unlimited) == ROWS
        assert [row["k"] for row in unlimited[:5]] == [ROWS - 1, ROWS - 2, ROWS - 3, ROWS - 4, ROWS - 5]

        spill_dir = tmp_path / "spill_area"
        monkeypatch.setenv("APEX_SPILL_DIR", str(spill_dir))
        monkeypatch.setenv(LIMIT_ENV, "1")
        spilled = client.execute("SELECT k, s FROM t ORDER BY k DESC").to_dict()
        assert spilled == unlimited
        assert not any(spill_dir.iterdir()), "per-query spill area must be removed"
    finally:
        client.close()


def test_multi_column_sort_spills_correctly(tmp_path, monkeypatch):
    client = _client(tmp_path)
    try:
        monkeypatch.setenv("APEX_SPILL_DIR", str(tmp_path / "spill_area"))
        monkeypatch.setenv(LIMIT_ENV, "1")
        rows = client.execute(
            "SELECT k, s FROM t ORDER BY s DESC, k ASC"
        ).to_dict()
        assert len(rows) == ROWS
        expected = sorted(
            ((i, f"v{i % 997:03d}") for i in range(ROWS)),
            key=lambda pair: (pair[1], pair[0]),
        )
        # String DESC, then k ASC: reverse only the key's major direction.
        expected = sorted(
            ((i, f"v{i % 997:03d}") for i in range(ROWS)),
            key=lambda pair: pair[0],
        )
        expected = sorted(expected, key=lambda pair: pair[1], reverse=True)
        got = [(row["k"], row["s"]) for row in rows]
        assert got == expected
    finally:
        client.close()


def test_unavailable_spill_directory_fails_explicitly(tmp_path, monkeypatch):
    client = _client(tmp_path)
    try:
        blocker = tmp_path / "not_a_directory"
        blocker.write_text("x")
        monkeypatch.setenv("APEX_SPILL_DIR", str(blocker))
        monkeypatch.setenv(LIMIT_ENV, "1")
        with pytest.raises(Exception) as error:
            client.execute("SELECT * FROM t ORDER BY k DESC").to_dict()
        message = str(error.value)
        assert "query memory budget exceeded" in message
        assert "spill" in message.lower()
        # The blocked root must not leave partial state behind.
        assert blocker.read_text() == "x"
    finally:
        client.close()


def test_stale_spill_areas_are_removed(tmp_path, monkeypatch):
    client = _client(tmp_path)
    try:
        root = tmp_path / "spill_root"
        stale = root / "999999-0"
        stale.mkdir(parents=True)
        (stale / "run-0-0.arrow").write_bytes(b"junk")
        old = time.time() - 3600
        os.utime(stale, (old, old))

        monkeypatch.setenv("APEX_SPILL_DIR", str(root))
        monkeypatch.setenv("APEX_SPILL_TTL_SECS", "1")
        monkeypatch.setenv(LIMIT_ENV, "1")
        rows = client.execute("SELECT k FROM t ORDER BY k ASC").to_dict()
        assert len(rows) == ROWS
        assert rows[0]["k"] == 0
        assert not stale.exists(), "stale area from a crashed process must be cleaned"
        assert not any(root.iterdir())
    finally:
        client.close()
