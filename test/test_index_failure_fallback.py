"""Non-transactional secondary-index failure contract (C3.2).

A non-transactional write persists its row before it maintains secondary
indexes. When that index maintenance fails, the error must surface, reads
must fall back to the authoritative scan, and the client must not replay
the mutation through its Arrow IPC fallback (a replay would apply the row
twice, because the first attempt already wrote it).
"""

import os
import sys
import tempfile

import pytest

from apexbase import ApexClient

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="POSIX file permissions are required"
)


def test_failed_insert_is_not_replayed_and_falls_back_to_scan():
    with tempfile.TemporaryDirectory() as d:
        c = ApexClient(d, _auto_manage=False, durability="safe")
        c.create_table("t", {"value": "int"})
        c.use_table("t")
        c.store([{"value": 1}, {"value": 2}])
        c.flush()
        c.execute("CREATE INDEX idx_value ON t (value) USING HASH")

        index_path = os.path.join(d, "indexes", "t_idx_value.hashidx")
        stale_path = os.path.join(d, "t.apex.index.stale")
        assert os.path.exists(index_path)
        assert not os.path.exists(stale_path)

        # A real write failure: the index file cannot be persisted.
        os.chmod(index_path, 0o444)
        try:
            with pytest.raises(RuntimeError):
                c.execute("INSERT INTO t (value) VALUES (999)")
        finally:
            os.chmod(index_path, 0o644)

        # The row was written exactly once. A replayed INSERT would make it 4.
        assert c.execute("SELECT COUNT(*) FROM t").scalar() == 3
        # The failed index must not be used; the scan finds the row.
        assert c.execute("SELECT value FROM t WHERE value = 999").scalar() == 999
        assert os.path.exists(stale_path)
        c.close()

        # The durable marker survives a reopen and keeps reads authoritative.
        c2 = ApexClient(d, _auto_manage=False, durability="safe")
        c2.use_table("t")
        assert c2.execute("SELECT COUNT(*) FROM t").scalar() == 3
        c2.execute("REINDEX t")
        assert not os.path.exists(stale_path)
        assert c2.execute("SELECT value FROM t WHERE value = 999").scalar() == 999
        c2.close()


def test_stale_marker_is_reaped_with_dropped_table():
    with tempfile.TemporaryDirectory() as d:
        c = ApexClient(d, _auto_manage=False, durability="safe")
        c.create_table("t", {"value": "int"})
        c.use_table("t")
        c.store([{"value": 1}])
        c.flush()

        stale_path = os.path.join(d, "t.apex.index.stale")
        with open(stale_path, "w") as handle:
            handle.write("index maintenance incomplete\n")
        c.execute("DROP TABLE t")
        c.close()

        # A same-name recreation must not inherit the old read fallback.
        c2 = ApexClient(d, _auto_manage=False, durability="safe")
        c2.create_table("t", {"value": "int"})
        c2.use_table("t")
        assert not os.path.exists(stale_path)
        c2.close()
