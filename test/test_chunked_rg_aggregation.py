"""Chunked (per-column compressed) row groups: fused aggregation + reads.

The chunked layout is opt-in via ``APEX_CHUNK_INDEX=1``; these tests run in a
subprocess so the process-wide switch cannot leak into other tests.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap

ROWS = 600_000

SCRIPT = textwrap.dedent(
    f"""
    import json
    import tempfile

    import numpy as np

    from apexbase.client import ApexClient

    rows = {ROWS}
    tmp = tempfile.mkdtemp()
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", {{"k": "int64", "x": "float64", "s": "string"}})
    client.use_table("t")
    client.set_compression("zstd")
    rng = np.random.default_rng(7)
    x = rng.random(rows)
    client.store({{"k": list(range(rows)), "x": x.tolist(),
                  "s": [f"user_{{i % 100}}" for i in range(rows)]}})
    client.flush()
    client.close()

    reader = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    reader.use_table("t")
    out = {{}}
    # Covering COUNT(*) never reads a payload.
    out["count_star"] = reader.execute(
        "SELECT COUNT(*) AS n FROM t WHERE k >= 0"
    ).to_dict()[0]["n"]
    # COUNT(col) reads only the aggregate column's validity bitmap.
    out["count_col"] = reader.execute(
        "SELECT COUNT(x) AS n FROM t WHERE k >= 0"
    ).to_dict()[0]["n"]
    # SUM over the decoded aggregate column.
    out["sum_x"] = reader.execute(
        "SELECT SUM(x) AS n FROM t WHERE k >= 0"
    ).to_dict()[0]["n"]
    # Partially covering filter: filter + value columns are decoded.
    out["sum_part"] = reader.execute(
        "SELECT SUM(x) AS n FROM t WHERE k BETWEEN 100000 AND 200000"
    ).to_dict()[0]["n"]
    out["count_part"] = reader.execute(
        "SELECT COUNT(*) AS n FROM t WHERE k BETWEEN 100000 AND 200000"
    ).to_dict()[0]["n"]
    # Projection of a chunked group without deletions (the prefix frame is not
    # decompressed) must still return rows.
    out["projected"] = reader.execute(
        "SELECT s FROM t LIMIT 5"
    ).to_dict()
    reader.close()

    expected_sum = float(np.sum(x))
    expected_part = float(np.sum(x[100000:200001]))
    assert out["count_star"] == rows, out["count_star"]
    assert out["count_col"] == rows, out["count_col"]
    assert abs(out["sum_x"] - expected_sum) <= abs(expected_sum) * 1e-9 + 1e-6, (out["sum_x"], expected_sum)
    assert abs(out["sum_part"] - expected_part) <= abs(expected_part) * 1e-9 + 1e-6, (out["sum_part"], expected_part)
    assert out["count_part"] == 100001, out["count_part"]
    assert len(out["projected"]) == 5, out["projected"]
    print("OK")
    """
)


def test_chunked_row_group_aggregation_and_projection():
    env = dict(os.environ)
    env["APEX_CHUNK_INDEX"] = "1"
    result = subprocess.run(
        [sys.executable, "-c", SCRIPT],
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "OK" in result.stdout
