"""Implicit ids: contiguous row groups omit the 8-byte-per-row id section.

Readers reconstruct ``min_id + row`` from the row-group header; the id section
used to be written for every table (only ``.apex_tmp`` tables used the implicit
form). `APEX_IMPLICIT_IDS=0` restores the old layout for A/B debugging.
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile

from apexbase.client import ApexClient

ROWS = 200_000
HEADER_SIZE = 256


def _create(root: str, rows: int = ROWS) -> str:
    client = ApexClient(root, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"v": "int64"})
    client.use_table("t")
    client.store({"v": list(range(rows))})
    client.flush()
    client.close()
    return os.path.join(root, "t.apex")


def _first_rg_id_encoding(path: str) -> int:
    with open(path, "rb") as handle:
        data = handle.read(HEADER_SIZE + 32)
    assert data[HEADER_SIZE : HEADER_SIZE + 4] == b"APXG"
    return data[HEADER_SIZE + 30]


def test_contiguous_table_writes_implicit_ids_by_default():
    with tempfile.TemporaryDirectory() as tmp:
        path = _create(tmp)
        assert _first_rg_id_encoding(path) == 1
        # 200k int64 values plus the footer; a plain-id layout would carry
        # 1.6 MB of ids on top of this.
        assert os.path.getsize(path) < 1_000_000

        client = ApexClient(tmp, enable_cache=False)
        client.use_table("t")
        try:
            # Point reads across row-group boundaries and at both ends.
            for row in (0, 65_535, 65_536, ROWS - 1):
                hit = client.execute(
                    f"SELECT v FROM t WHERE _id = {row + 1}", show_internal_id=True
                ).to_dict()
                assert hit == [{"v": row}], (row, hit)
            assert client.execute("SELECT COUNT(*) AS n FROM t").to_dict() == [{"n": ROWS}]
            client.execute("DELETE FROM t WHERE _id = 5")
            client.execute("UPDATE t SET v = 999 WHERE _id = 65_537")
            client.flush()
            assert client.execute("SELECT COUNT(*) AS n FROM t").to_dict() == [{"n": ROWS - 1}]
            assert client.execute("SELECT v FROM t WHERE _id = 5", show_internal_id=True).to_dict() == []
            assert client.execute(
                "SELECT v FROM t WHERE _id = 65_537", show_internal_id=True
            ).to_dict() == [{"v": 999}]
        finally:
            client.close()


def test_plain_id_layout_is_available_for_ab_debugging():
    with tempfile.TemporaryDirectory() as tmp:
        code = (
            "import sys\n"
            "from apexbase import ApexClient\n"
            "client = ApexClient(sys.argv[1], drop_if_exists=False, enable_cache=False)\n"
            "client.create_table('t', schema={'v': 'int64'})\n"
            "client.use_table('t')\n"
            "client.store({'v': list(range(10))})\n"
            "client.flush(); client.close()\n"
        )
        env = dict(os.environ, APEX_IMPLICIT_IDS="0")
        completed = subprocess.run(
            [sys.executable, "-c", code, tmp], env=env, capture_output=True, text=True, timeout=120
        )
        assert completed.returncode == 0, completed.stderr
        assert _first_rg_id_encoding(os.path.join(tmp, "t.apex")) == 0

        client = ApexClient(tmp, enable_cache=False)
        client.use_table("t")
        try:
            assert client.execute("SELECT COUNT(*) AS n FROM t").to_dict() == [{"n": 10}]
            assert client.execute(
                "SELECT v FROM t WHERE _id = 3", show_internal_id=True
            ).to_dict() == [{"v": 2}]
        finally:
            client.close()
