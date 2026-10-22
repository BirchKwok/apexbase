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
