"""Blob sidecar durability and batched materialization (W4-6)."""
from __future__ import annotations

import os

from apexbase.client import ApexClient


def _payload(size: int, seed: int) -> bytes:
    pattern = f"sidecar-{seed:04d}:".encode("ascii")
    return (pattern * ((size // len(pattern)) + 1))[:size]


def test_safe_durability_keeps_blob_payloads_across_flush_and_reopen():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False, durability="safe")
        client.create_table("files", {"name": "string", "payload": "blob"})
        client.use_table("files")
        payloads = [
            b"inline",
            _payload(200 * 1024, 1),
            _payload(5 * 1024 * 1024 + 7, 2),
        ]
        client.store({"name": ["a", "b", "c"], "payload": payloads})
        client.flush()
        client.close()

        reopened = ApexClient(tmp, enable_cache=False, durability="safe")
        reopened.use_table("files")
        try:
            assert reopened.count_rows() == 3
            assert reopened.read_blobs("payload", [1, 2, 3]) == payloads
            assert reopened.read_blob_range("payload", 2, 199_000, 512) == payloads[1][
                199_000:199_512
            ]
            rows = reopened.execute("SELECT payload FROM files").to_dict()
            assert [row["payload"] for row in rows] == payloads
        finally:
            reopened.close()


def test_packed_blob_batch_reads_every_row_through_one_sidecar():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False, durability="fast")
        client.create_table("files", {"payload": "blob"})
        client.use_table("files")
        payloads = [_payload(128 * 1024, seed) for seed in range(64)]
        client.store({"payload": payloads})
        client.flush()
        sidecar = os.path.join(tmp, "files.apex.blobs")
        assert os.path.isfile(os.path.join(sidecar, "packed.blob"))
        try:
            rows = client.execute("SELECT payload FROM files").to_dict()
            assert [row["payload"] for row in rows] == payloads
            # The batched Arrow materialization must agree with the point reader.
            assert client.read_blobs("payload", list(range(1, 65))) == payloads
        finally:
            client.close()


def test_drop_table_reclaims_the_blob_sidecar():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("files", {"payload": "blob"})
        client.use_table("files")
        client.store({"payload": [_payload(200 * 1024, 5), _payload(5 * 1024 * 1024, 6)]})
        client.flush()
        sidecar = os.path.join(tmp, "files.apex.blobs")
        assert os.path.isdir(sidecar)
        client.drop_table("files")
        client.close()
        assert not os.path.exists(sidecar)
        assert not os.path.exists(os.path.join(tmp, "files.apex"))


def test_packed_blobs_store_duplicate_payloads_once():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("files", {"payload": "blob"})
        client.use_table("files")
        duplicate = _payload(512 * 1024, 9)
        client.store({"payload": [duplicate] * 8})
        client.flush()
        packed = os.path.join(tmp, "files.apex.blobs", "packed.blob")
        # Eight identical 512 KB payloads: one packed copy, not 4 MB.
        assert os.path.getsize(packed) < 700 * 1024
        try:
            rows = client.execute("SELECT payload FROM files").to_dict()
            assert [row["payload"] for row in rows] == [duplicate] * 8
            assert client.read_blobs("payload", list(range(1, 9))) == [duplicate] * 8
        finally:
            client.close()


def test_packed_blobs_dedupe_across_store_calls():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("files", {"payload": "blob"})
        client.use_table("files")
        duplicate = _payload(512 * 1024, 11)
        client.store({"payload": [duplicate]})
        client.flush()
        client.store({"payload": [duplicate]})
        client.flush()
        packed = os.path.join(tmp, "files.apex.blobs", "packed.blob")
        assert os.path.getsize(packed) < 700 * 1024
        try:
            rows = client.execute("SELECT payload FROM files").to_dict()
            assert [row["payload"] for row in rows] == [duplicate, duplicate]
        finally:
            client.close()
