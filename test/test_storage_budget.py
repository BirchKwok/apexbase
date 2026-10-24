"""Process storage budget: a small budget must not change query results (W3)."""
from __future__ import annotations

import tempfile

from apexbase.client import ApexClient

ROWS = 3_000
PAYLOAD = "compressible-payload-" * 100  # ~2 KB per row -> multi-MB row group


def test_small_storage_budget_keeps_compressed_reads_correct(monkeypatch):
    # A 64 MiB storage budget splits into a 32 MiB scan buffer and a 4 MiB
    # decompressed-row-group cache; the row group below exceeds that cache, so
    # every read decodes it again and the results must still be identical.
    monkeypatch.setenv("APEX_STORAGE_BUDGET_MB", "64")
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", {"v": "int64", "s": "string"})
        client.use_table("t")
        assert client.set_compression("zstd")
        client.store(
            {"v": list(range(ROWS)), "s": [f"{PAYLOAD}{index}" for index in range(ROWS)]}
        )
        client.flush()
        try:
            assert client.execute("SELECT COUNT(*) AS n FROM t").to_dict()[0]["n"] == ROWS
            first = client.execute("SELECT v, s FROM t ORDER BY v").to_dict()
            second = client.execute("SELECT v, s FROM t ORDER BY v").to_dict()
            assert first == second
            assert first[0]["s"] == f"{PAYLOAD}0"
            assert [row["v"] for row in first] == list(range(ROWS))
            client.execute("DELETE FROM t WHERE v < 1000")
            remaining = client.execute("SELECT COUNT(*) AS n FROM t").to_dict()[0]["n"]
            assert remaining == ROWS - 1_000
        finally:
            client.close()
