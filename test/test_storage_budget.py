"""Process storage budget: a small budget must not change query results (W3)."""
from __future__ import annotations

import subprocess
import sys
import tempfile


SCRIPT = r"""
import sys
from apexbase.client import ApexClient

tmp = sys.argv[1]
rows = 3000
payload = "compressible-payload-" * 100  # ~2 KB per row -> multi-MB row group
client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
client.create_table("t", {"v": "int64", "s": "string"})
client.use_table("t")
assert client.set_compression("zstd")
client.store({"v": list(range(rows)), "s": [f"{payload}{index}" for index in range(rows)]})
client.flush()
assert client.execute("SELECT COUNT(*) AS n FROM t").to_dict()[0]["n"] == rows
first = client.execute("SELECT v, s FROM t ORDER BY v").to_dict()
second = client.execute("SELECT v, s FROM t ORDER BY v").to_dict()
assert first == second
assert first[0]["s"] == f"{payload}0"
assert [row["v"] for row in first] == list(range(rows))
client.execute("DELETE FROM t WHERE v < 1000")
assert client.execute("SELECT COUNT(*) AS n FROM t").to_dict()[0]["n"] == rows - 1000
client.close()
print("ok")
"""


def test_small_storage_budget_keeps_compressed_reads_correct():
    # A 64 MiB storage budget splits into a 32 MiB scan buffer and a 4 MiB
    # decompressed-row-group cache; the row group below exceeds that cache, so
    # every read decodes it again and the results must still be identical. The
    # budget is resolved once per process, so the script runs in a subprocess.
    with tempfile.TemporaryDirectory() as tmp:
        completed = subprocess.run(
            [sys.executable, "-c", SCRIPT, tmp],
            capture_output=True,
            text=True,
            env={
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "APEX_STORAGE_BUDGET_MB": "64",
            },
        )
        assert completed.returncode == 0, completed.stderr
        assert completed.stdout.strip().splitlines()[-1] == "ok"
