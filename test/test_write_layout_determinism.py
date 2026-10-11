"""
A write must produce the same bytes regardless of which process performs it.

Schema inference used to iterate the typed column buckets in `HashMap` order,
and that hasher seeds per process. The identical `store()` call therefore
produced a different column order — and with it a different physical Row Group
layout: different per-column encodings, different column offsets, a different
file hash — in every process. Only 3 of the schema's 5 columns are strings, so
the observations below also pin that sorting did not drop or merge columns.

These tests have to spawn subprocesses: within one process the hash seed is
fixed, so an in-process comparison would pass even with the defect present.
"""

import hashlib
import os
import shutil
import subprocess
import sys
import tempfile

from apexbase import ApexClient

# Written without braces so it can be passed to `python -c` verbatim.
WRITE_SCRIPT = """
import sys
from apexbase import ApexClient
c = ApexClient(sys.argv[1], enable_cache=False)
c.create_table("default")
c.use_table("default")
c.store({
    "name": ["user_" + str(i) for i in range(5000)],
    "age": [18 + (i % 63) for i in range(5000)],
    "score": [float(i) * 0.5 for i in range(5000)],
    "city": ["c" + str(i % 10) for i in range(5000)],
    "category": ["k" + str(i % 7) for i in range(5000)],
})
c.flush()
row = c.execute("SELECT * FROM default LIMIT 1").to_dict()[0]
print("ORDER:" + ",".join(k for k in row if k != "_id"))
c.close()
"""


def _write_and_read_order(root):
    """Run one write in a fresh interpreter; return its SELECT * column order."""
    result = subprocess.run(
        [sys.executable, "-c", WRITE_SCRIPT, root],
        capture_output=True, text=True, check=True,
    )
    for line in result.stdout.splitlines():
        if line.startswith("ORDER:"):
            return line[len("ORDER:"):].split(",")
    raise AssertionError(f"writer produced no column order:\n{result.stdout}\n{result.stderr}")


def _normalized_hash(path):
    """File hash with the header's timestamps and their checksum zeroed."""
    data = bytearray(open(path, "rb").read())
    data[56:76] = b"\x00" * 20  # created_at, modified_at, header checksum
    return hashlib.sha256(bytes(data)).hexdigest()


def _two_runs():
    roots = [tempfile.mkdtemp() for _ in range(2)]
    try:
        orders = [_write_and_read_order(root) for root in roots]
        hashes = [_normalized_hash(os.path.join(root, "default.apex")) for root in roots]
        sizes = [os.path.getsize(os.path.join(root, "default.apex")) for root in roots]
        yield orders, hashes, sizes
    finally:
        for root in roots:
            shutil.rmtree(root, ignore_errors=True)


def test_same_write_produces_identical_bytes_in_every_process():
    for orders, hashes, sizes in _two_runs():
        assert sizes[0] == sizes[1], "same input produced different file sizes"
        assert hashes[0] == hashes[1], (
            "same input produced different bytes; schema inference is not deterministic"
        )


def test_schema_column_order_is_identical_in_every_process():
    for orders, _, _ in _two_runs():
        assert orders[0] == orders[1], (
            f"column order differs between processes: {orders[0]} vs {orders[1]}"
        )
        assert sorted(orders[0]) == ["age", "category", "city", "name", "score"], (
            f"unexpected column set: {orders[0]}"
        )


def test_declared_columns_all_round_trip():
    root = tempfile.mkdtemp()
    try:
        _write_and_read_order(root)
        client = ApexClient(root, enable_cache=False)
        client.use_table("default")
        try:
            row = client.execute(
                "SELECT name, age, score, city, category FROM default LIMIT 1"
            ).to_dict()[0]
            assert row["name"] == "user_0"
            assert row["age"] == 18
            assert row["score"] == 0.0
            assert row["city"] == "c0"
            assert row["category"] == "k0"
            assert client.execute(
                "SELECT COUNT(*) AS c FROM default"
            ).to_dict() == [{"c": 5000}]
        finally:
            client.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_appends_keep_the_inferred_order():
    """A later append must not reshuffle the schema or the file layout."""
    root = tempfile.mkdtemp()
    try:
        _write_and_read_order(root)
        client = ApexClient(root, enable_cache=False)
        client.use_table("default")
        try:
            before = list(client.list_fields())
            client.store({"name": ["extra"], "age": [1], "score": [1.0],
                          "city": ["c1"], "category": ["k1"]})
            client.flush()
            assert list(client.list_fields()) == before
            assert client.execute(
                "SELECT COUNT(*) AS c FROM default"
            ).to_dict() == [{"c": 5001}]
        finally:
            client.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)
