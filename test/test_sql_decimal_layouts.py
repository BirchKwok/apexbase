"""DECIMAL query semantics must not depend on the compressed row-group layout.

V5 S1 requires the flat and per-column (chunked) compressed layouts to answer the
same DECIMAL predicates, ordering and aggregates. Both layouts are exercised in
subprocesses: the layout switch is process-wide at write time, and each script
also pins the layout byte it actually produced so a silent fall-back to flat
cannot make the test vacuous.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap

ROWS = 120_000

SCRIPT = textwrap.dedent(
    f"""
    import json
    import os
    import sys
    import tempfile

    from apexbase.client import ApexClient

    rows = {ROWS}
    tmp = tempfile.mkdtemp(prefix="apex_decimal_layout_")
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", {{"k": "int64", "d": "decimal(10,2)"}})
    client.use_table("t")
    client.set_compression("zstd")
    ids = list(range(rows))
    # Mostly-zero high bytes keep the DECIMAL payloads compressible while the
    # values still wrap, so byte order and numeric order disagree.
    values = [(index * 7919) % 100000 for index in ids]
    payloads = [value.to_bytes(16, "little", signed=True) for value in values]
    client.store({{"k": ids, "d": payloads}})
    client.flush()
    expected_high = sum(1 for value in values if value >= 99000)

    out = {{}}
    out["where"] = [
        row["k"] for row in client.execute(
            "SELECT k FROM t WHERE d > '500.00' ORDER BY k LIMIT 20"
        ).to_dict()
    ]
    out["order"] = [
        int.from_bytes(row["d"], "little", signed=True)
        for row in client.execute("SELECT d FROM t ORDER BY d ASC LIMIT 5").to_dict()
    ]
    out["top"] = [
        int.from_bytes(row["d"], "little", signed=True)
        for row in client.execute("SELECT d FROM t ORDER BY d DESC LIMIT 5").to_dict()
    ]
    out["aggregate"] = client.execute(
        "SELECT SUM(d) AS s, MIN(d) AS m, MAX(d) AS x, AVG(d) AS a, COUNT(d) AS n "
        "FROM t WHERE d >= '990.00'"
    ).to_dict()[0]
    out["aggregate"]["s"] = int.from_bytes(out["aggregate"]["s"], "little", signed=True)
    out["aggregate"]["m"] = int.from_bytes(out["aggregate"]["m"], "little", signed=True)
    out["aggregate"]["x"] = int.from_bytes(out["aggregate"]["x"], "little", signed=True)
    out["high_count"] = client.execute(
        "SELECT COUNT(*) AS n FROM t WHERE d >= '990.00'"
    ).to_dict()[0]["n"]
    unfiltered = client.execute(
        "SELECT SUM(d) AS s, MIN(d) AS m, MAX(d) AS x FROM t"
    ).to_dict()[0]
    out["aggregate_all"] = {{
        key: int.from_bytes(unfiltered[key], "little", signed=True)
        for key in ("s", "m", "x")
    }}
    out["expected_high"] = expected_high
    client.close()

    with open(os.path.join(tmp, "t.apex"), "rb") as handle:
        head = handle.read(256 + 32)
    out["encoding"] = head[256 + 29]
    out["version"] = int.from_bytes(head[8:12], "little")
    print(json.dumps(out))
    """
)


def _run(layout):
    env = {"APEX_CHUNK_INDEX": layout}
    import os

    merged = dict(os.environ)
    merged.update(env)
    completed = subprocess.run(
        [sys.executable, "-c", SCRIPT],
        capture_output=True,
        text=True,
        env=merged,
        timeout=600,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_flat_and_chunked_layouts_answer_identically():
    chunked = _run("1")
    flat = _run("0")
    assert chunked["encoding"] == 2, chunked
    assert flat["encoding"] == 1, flat
    assert chunked["version"] == flat["version"] == 5
    for key in (
        "where",
        "order",
        "top",
        "aggregate",
        "aggregate_all",
        "high_count",
        "expected_high",
    ):
        assert chunked[key] == flat[key], (key, chunked[key], flat[key])
    assert flat["high_count"] == flat["expected_high"]
    assert flat["order"] == sorted(flat["order"])
    assert flat["order"][0] < flat["order"][-1]
