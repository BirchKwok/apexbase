"""Files written by a newer build must be refused, not guessed at.

Every V4 read path is gated on an exact format version, and column bodies carry
their own encoding revision. A file that declares a newer version, or a row
group whose column-body revision is newer, has to fail with a clear error rather
than being interpreted with this build's rules (which would silently return
wrong values).

The checks run in a fresh interpreter: a process that already opened the table
holds a cached backend keyed on its own view, so an in-place patch applied from
outside is not the scenario these tests describe.
"""

import os
import shutil
import struct
import subprocess
import sys
import tempfile

from apexbase import ApexClient

OPEN_AND_QUERY = """
from apexbase import ApexClient
import sys
try:
    client = ApexClient(sys.argv[1], enable_cache=False)
    client.use_table("t")
    # Read column data (not just metadata): COUNT(*) is answered from the
    # footer's row counts and never touches a row-group body.
    print("RESULT", client.execute("SELECT v FROM t LIMIT 5").to_dict())
except Exception as error:  # noqa: BLE001 - the message is what we assert
    print("RAISED", type(error).__name__, error)
"""


def _create_table(root):
    client = ApexClient(root, enable_cache=False)
    client.create_table("t", schema={"v": "int64"})
    client.use_table("t")
    client.store({"v": list(range(2000))})
    client.flush()
    client.close()
    return os.path.join(root, "t.apex")


def _reopen(root):
    """Open the table in a new interpreter and return its combined output."""
    completed = subprocess.run(
        [sys.executable, "-c", OPEN_AND_QUERY, root],
        capture_output=True,
        text=True,
        timeout=120,
    )
    return completed.stdout + completed.stderr


def test_newer_format_version_is_rejected_with_a_clear_error():
    root = tempfile.mkdtemp()
    try:
        path = _create_table(root)
        assert "RESULT" in _reopen(root), "the fixture must be readable first"

        blob = bytearray(open(path, "rb").read())
        blob[8:12] = struct.pack("<I", 99)
        with open(path, "wb") as handle:
            handle.write(bytes(blob))

        output = _reopen(root)
        assert "RESULT" not in output, f"a newer version must not be read: {output}"
        assert "unsupported ApexBase format version 99" in output, output
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_newer_row_group_encoding_version_is_rejected():
    root = tempfile.mkdtemp()
    try:
        path = _create_table(root)
        blob = bytearray(open(path, "rb").read())
        # Row group header: 32 bytes at the start of the body; byte 29 is the
        # column-body encoding revision.
        # Version 2 is the supported chunked layout; the first unsupported
        # revision is 3.
        blob[256 + 29] = 3
        with open(path, "wb") as handle:
            handle.write(bytes(blob))

        output = _reopen(root)
        # The central decoders refuse with an explicit message; lanes that fall
        # back to the unversioned layout read a bogus element count and fail
        # loudly. Either way the read must not return data.
        assert "RESULT" not in output, f"a newer encoding must not be read: {output}"
        assert "RAISED" in output, output
        assert (
            "encoding version" in output or "truncated" in output
        ), output
    finally:
        shutil.rmtree(root, ignore_errors=True)
