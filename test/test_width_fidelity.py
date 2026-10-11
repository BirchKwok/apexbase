"""Width fidelity for Float32 columns.

A Float32 column is declared at four bytes per value. V5 stores it that way by
default (the file declares format version 5, which older builds are not expected
to read); `APEX_WIDTH_FIDELITY=0` keeps the V4 full-width layout for handing a
file to an older build. V4 files stay readable either way.

Each case runs in its own interpreter because the switch is read when a table is
created.
"""

import os
import shutil
import struct
import subprocess
import sys
import tempfile

ROWS = 40_000

SCRIPT = """
import json, os, sys
from apexbase import ApexClient

root = sys.argv[1]
rows = int(sys.argv[2])
client = ApexClient(root, enable_cache=False)
client.create_table("t", schema={"v": "float32"})
client.use_table("t")
client.store({"v": [i * 0.5 for i in range(rows)]})
client.flush()
blob = open(os.path.join(root, "t.apex"), "rb").read(12)
size = os.path.getsize(os.path.join(root, "t.apex"))
version = int.from_bytes(blob[8:12], "little")
values = client.execute("SELECT v FROM t ORDER BY v LIMIT 3").to_dict()
total = client.execute("SELECT SUM(v) AS s FROM t").to_dict()[0]["s"]
print("RESULT", json.dumps({"size": size, "values": values, "sum": total, "version": version}))
client.close()
"""


def _run(root, extra_env=None):
    env = os.environ.copy()
    env.pop("APEX_WIDTH_FIDELITY", None)
    if extra_env:
        env.update(extra_env)
    completed = subprocess.run(
        [sys.executable, "-c", SCRIPT, root, str(ROWS)],
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )
    output = completed.stdout + completed.stderr
    assert "RESULT" in output, output
    import json

    return json.loads(output.split("RESULT", 1)[1].strip())


def test_width_fidelity_halves_float32_storage_by_default():
    # V5 default: a Float32 column is stored at its declared width.
    wide_root = tempfile.mkdtemp()
    narrow_root = tempfile.mkdtemp()
    try:
        wide = _run(wide_root, {"APEX_WIDTH_FIDELITY": "0"})
        narrow = _run(narrow_root)

        # The values are identical in meaning: the narrow file stores them at
        # f32 precision, so read them back with that rounding.
        expected = [struct.unpack("f", struct.pack("f", i * 0.5))[0] for i in range(3)]
        assert [row["v"] for row in narrow["values"]] == expected
        assert [row["v"] for row in wide["values"]] == expected
        assert narrow["sum"] == wide["sum"]

        assert wide["size"] - narrow["size"] == ROWS * 4, (
            f"narrow storage must save 4 bytes/value: {wide['size']} vs {narrow['size']}"
        )
        # A narrow payload is a V5 file; the V4 layout keeps declaring version 4
        # and stays readable by this build.
        assert narrow["version"] == 5, narrow
        assert wide["version"] == 4, wide
    finally:
        shutil.rmtree(wide_root, ignore_errors=True)
        shutil.rmtree(narrow_root, ignore_errors=True)


def test_width_fidelity_can_be_opted_out_for_v4_compatibility():
    # `APEX_WIDTH_FIDELITY=0` keeps the V4 full-width layout so an older build
    # can still read the file.
    root = tempfile.mkdtemp()
    narrow_root = tempfile.mkdtemp()
    try:
        wide = _run(root, {"APEX_WIDTH_FIDELITY": "0"})
        narrow = _run(narrow_root)
        assert wide["size"] - narrow["size"] == ROWS * 4, (
            f"opt-out must store full width: {wide['size']} vs {narrow['size']}"
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(narrow_root, ignore_errors=True)


def test_narrow_float32_point_lookup_keeps_the_fast_lane(tmp_path):
    """A narrow Float32 column must not disable the O(1) point lookup.

    The point lane decodes one cell per column from the footer's column
    offsets. The narrow payload had no arm in that decoder, so `_id = X`
    reported "no such row" and fell out of the fast lane into a full scan
    (measured 0.003 ms -> 0.12 ms on a 200k-row table). Both the lane and the
    value are asserted here.
    """
    pytest = __import__("pytest")
    pytest.importorskip("apexbase")
    import time

    from apexbase import ApexClient

    rows = 100_000
    db_dir = tmp_path / "db"
    client = ApexClient(str(db_dir), drop_if_exists=True)
    try:
        client.create_table("t", {"score": "float32", "age": "int"})
        client.use_table("t")
        client.store(
            {
                "score": [i * 0.5 for i in range(rows)],
                "age": [i % 13 for i in range(rows)],
            }
        )
        client.flush()
        version = int.from_bytes((db_dir / "t.apex").read_bytes()[8:12], "little")
        assert version == 5, "the fixture must use the V5 narrow layout"

        point_id = rows // 2 + 1
        query = f"SELECT age, score FROM t WHERE _id = {point_id}"

        plan = client.execute("EXPLAIN ANALYZE " + query).to_dict()[0]["plan"]
        assert "id_point_lookup" in plan, plan

        row = client.execute(query).to_dict()[0]
        assert row["age"] == (point_id - 1) % 13
        assert row["score"] == struct.unpack(
            "f", struct.pack("f", (point_id - 1) * 0.5)
        )[0]

        samples = []
        for _ in range(5):
            started = time.perf_counter()
            client.execute(query).to_dict()
            samples.append((time.perf_counter() - started) * 1000.0)
        median_ms = sorted(samples)[len(samples) // 2]
        assert median_ms < 5.0, f"point lookup left the fast lane: {median_ms} ms"
    finally:
        client.close()
