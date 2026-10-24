"""Packed-blob GC with a journaled rename sequence (W4-6)."""
from __future__ import annotations

import os
import shutil
import tempfile

import pytest

from apexbase.client import ApexClient

PAYLOAD_SIZE = 96 * 1024


def _payload(seed: int) -> bytes:
    pattern = f"packed-vacuum-{seed:05d}:".encode("ascii")
    return (pattern * ((PAYLOAD_SIZE // len(pattern)) + 1))[:PAYLOAD_SIZE]


def _paths(tmp: str, table: str = "files"):
    sidecar = os.path.join(tmp, f"{table}.apex.blobs")
    return {
        "table": os.path.join(tmp, f"{table}.apex"),
        "packed": os.path.join(sidecar, "packed.blob"),
        "new_packed": os.path.join(sidecar, "packed.blob.vacuum"),
        "old_packed": os.path.join(sidecar, "packed.blob.old"),
        "journal": os.path.join(tmp, f"{table}.apex.packedvac"),
        "table_copy": os.path.join(tmp, f"{table}.apex.vacuum"),
    }


def _payload_hashes_in_fresh_process(tmp: str) -> list[str]:
    """Open the table in a new process (a fresh backend, like after a crash)."""
    import hashlib
    import subprocess
    import sys

    script = (
        "import hashlib, json, sys\n"
        "from apexbase.client import ApexClient\n"
        "client = ApexClient(sys.argv[1], drop_if_exists=False, enable_cache=False)\n"
        "client.use_table('files')\n"
        "rows = client.execute('SELECT payload FROM files ORDER BY _id').to_dict()\n"
        "print(json.dumps([hashlib.sha256(row['payload']).hexdigest() for row in rows]))\n"
        "client.close()\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, tmp],
        check=True,
        capture_output=True,
        text=True,
    )
    import json

    return json.loads(completed.stdout.strip().splitlines()[-1])


def _expected_hashes(payloads) -> list[str]:
    import hashlib

    return [hashlib.sha256(payload).hexdigest() for payload in payloads]


def _open(tmp: str) -> ApexClient:
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.use_table("files")
    return client


def test_packed_vacuum_reclaims_deleted_payloads_and_reopens():
    with tempfile.TemporaryDirectory() as tmp:
        payloads = [_payload(seed) for seed in range(24)]
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("files", {"payload": "blob"})
        client.use_table("files")
        client.store({"payload": payloads})
        client.flush()
        client.close()
        paths = _paths(tmp)
        size_before = os.path.getsize(paths["packed"])
        assert size_before >= len(payloads) * PAYLOAD_SIZE

        client = _open(tmp)
        try:
            client.execute("DELETE FROM files WHERE _id <= 12")
            freed = client.vacuum_packed_blobs()
            assert freed > 0
            assert os.path.getsize(paths["packed"]) < size_before
            rows = client.execute("SELECT _id, payload FROM files ORDER BY _id").to_dict()
            assert [row["payload"] for row in rows] == payloads[12:]
            assert [row["_id"] for row in rows] == list(range(13, 25))
            # The compacted file is already minimal: a second pass frees nothing.
            assert client.vacuum_packed_blobs() == 0
            for key in ("journal", "table_copy", "new_packed", "old_packed"):
                assert not os.path.exists(paths[key]), key
        finally:
            client.close()

        reopened = _open(tmp)
        try:
            rows = reopened.execute("SELECT _id, payload FROM files ORDER BY _id").to_dict()
            assert [row["payload"] for row in rows] == payloads[12:]
        finally:
            reopened.close()


def test_packed_vacuum_without_packed_payloads_returns_zero():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("files", {"payload": "blob"})
        client.use_table("files")
        client.store({"payload": [f"inline-{index}".encode() for index in range(8)]})
        client.flush()
        try:
            assert client.vacuum_packed_blobs() == 0
        finally:
            client.close()


def test_packed_vacuum_rejects_compressed_row_groups():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("files", {"name": "string", "payload": "blob"})
        client.use_table("files")
        assert client.set_compression("zstd")
        # A large compressible text column puts the row group over the compress
        # threshold, so the group really is stored compressed.
        client.store(
            {
                "name": ["compressible " * 20_000 for _ in range(8)],
                "payload": [_payload(seed) for seed in range(8)],
            }
        )
        client.flush()
        try:
            with pytest.raises(RuntimeError, match="uncompressed"):
                client.vacuum_packed_blobs()
        finally:
            client.close()


def test_crashed_packed_vacuum_completes_on_next_open():
    with tempfile.TemporaryDirectory() as tmp:
        payloads = [_payload(seed) for seed in range(12)]
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("files", {"payload": "blob"})
        client.use_table("files")
        client.store({"payload": payloads})
        client.flush()
        client.close()
        paths = _paths(tmp)
        # Simulate a crash after both files were published but before any rename:
        # the journal exists, the live paths are untouched.
        shutil.copyfile(paths["packed"], paths["new_packed"])
        shutil.copyfile(paths["table"], paths["table_copy"])
        with open(paths["journal"], "wb") as handle:
            handle.write(b"APXVAC1")

        assert _payload_hashes_in_fresh_process(tmp) == _expected_hashes(payloads)
        for key in ("journal", "table_copy", "new_packed", "old_packed"):
            assert not os.path.exists(paths[key]), key


def test_crashed_packed_vacuum_recovers_after_first_rename():
    with tempfile.TemporaryDirectory() as tmp:
        payloads = [_payload(seed) for seed in range(10)]
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("files", {"payload": "blob"})
        client.use_table("files")
        client.store({"payload": payloads})
        client.flush()
        client.close()
        paths = _paths(tmp)
        # Crash between "packed.blob -> packed.blob.old" and
        # "packed.blob.vacuum -> packed.blob".
        os.rename(paths["packed"], paths["old_packed"])
        shutil.copyfile(paths["old_packed"], paths["new_packed"])
        shutil.copyfile(paths["table"], paths["table_copy"])
        with open(paths["journal"], "wb") as handle:
            handle.write(b"APXVAC1")

        assert _payload_hashes_in_fresh_process(tmp) == _expected_hashes(payloads)
        assert os.path.isfile(paths["packed"])
        for key in ("journal", "table_copy", "new_packed", "old_packed"):
            assert not os.path.exists(paths[key]), key
