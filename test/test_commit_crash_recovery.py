"""Commit failure contract tests (architecture review R1).

These tests exercise the real commit protocol on real files:

* kill -9 during a transaction commit must be all-or-nothing: after a
  reopen, either none of the transaction's rows are visible (the WAL
  commit marker was not durable) or all of them are (the marker was
  durable and crash recovery re-applied the missing rows).
* a commit that fails with a real I/O error AFTER the WAL commit marker
  is durably committed: the error is propagated, client state is clean,
  and the rows become visible after a reopen.
* a commit that fails BEFORE the commit marker aborts cleanly: the rows
  are not visible after a reopen.
"""

import os
import signal
import subprocess
import sys
import tempfile
import time

import pytest

from apexbase import ApexClient

CHILD_SCRIPT = r'''
import sys
import time

from apexbase import ApexClient

base_dir = sys.argv[1]
rows = int(sys.argv[2])
marker = sys.argv[3]
durability = sys.argv[4]

c = ApexClient(base_dir, _auto_manage=False, durability=durability)
c.create_table("crash_t", {"name": "string", "value": "int"})
c.use_table("crash_t")
c.store([{"name": "seed", "value": 0}])
c.flush()

# A real Rust transaction: BEGIN on the storage handle, DML buffered in
# the shared transaction manager, COMMIT runs the commit protocol.
c._storage.execute("BEGIN")
c._in_txn = True
for i in range(0, rows, 500):
    vals = ",".join(f"('r{i+j:07d}',{i+j})" for j in range(min(500, rows - i)))
    c.execute(f"INSERT INTO crash_t (name, value) VALUES {vals}")

with open(marker, "w") as f:
    f.write("about_to_commit")
time.sleep(0.003)
c.execute("COMMIT")
c.close()
'''


def _wait_file(path, proc, timeout=60.0):
    t0 = time.time()
    while not os.path.exists(path):
        if proc.poll() is not None:
            return False
        if time.time() - t0 > timeout:
            return False
        time.sleep(0.001)
    return True


def _kill_commit_trial(rows, kill_delay, do_kill, durability="safe"):
    with tempfile.TemporaryDirectory() as d:
        marker = os.path.join(d, "about_to_commit")
        script = os.path.join(d, "child.py")
        with open(script, "w") as f:
            f.write(CHILD_SCRIPT)
        proc = subprocess.Popen(
            [sys.executable, script, d, str(rows), marker, durability]
        )
        assert _wait_file(marker, proc), "child failed to reach COMMIT"
        if do_kill:
            time.sleep(kill_delay)
            try:
                os.kill(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        proc.wait()
        c = ApexClient(d, _auto_manage=False, durability=durability)
        c.use_table("crash_t")
        count = c.execute("SELECT COUNT(*) AS n FROM crash_t").first()["n"]
        # The database must remain usable after the crash: a fresh
        # transaction must commit normally.
        c._storage.execute("BEGIN")
        c._in_txn = True
        c.execute("INSERT INTO crash_t (name, value) VALUES ('post', 1)")
        c.execute("COMMIT")
        post_count = c.execute("SELECT COUNT(*) AS n FROM crash_t").first()["n"]
        c.close()
        return count - 1, post_count - count


def test_control_commit_without_crash():
    visible, post = _kill_commit_trial(5000, 0.0, do_kill=False)
    assert visible == 5000
    assert post == 1


@pytest.mark.parametrize("rows,delays", [
    (5000, [0.0, 0.02, 0.05, 0.1, 0.2]),
    (40000, [0.0, 0.05, 0.1, 0.15, 0.2, 0.3]),
])
def test_kill_during_commit_is_all_or_nothing(rows, delays):
    for delay in delays:
        visible, post = _kill_commit_trial(rows, delay, do_kill=True)
        assert visible in (0, rows), (
            f"kill at delay={delay}s left {visible} of {rows} rows visible; "
            "a commit must be all-or-nothing after a crash"
        )
        assert post == 1


def test_failed_commit_after_marker_converges_after_reopen():
    with tempfile.TemporaryDirectory() as d:
        c = ApexClient(d, _auto_manage=False, durability="safe")
        c.create_table("t", {"name": "string", "value": "int"})
        c.use_table("t")
        c.store([{"name": "seed", "value": 0}])
        c.flush()

        c._storage.execute("BEGIN")
        c._in_txn = True
        c.execute("INSERT INTO t (name, value) VALUES ('pending', 1)")

        # Make the delta file non-writable so the storage apply — which
        # runs after the WAL commit marker — fails with a real I/O error.
        delta_path = os.path.join(d, "t.apex.delta")
        with open(delta_path, "wb"):
            pass
        os.chmod(delta_path, 0o444)
        try:
            with pytest.raises(RuntimeError, match="commit_outcome=unknown"):
                c.execute("COMMIT")
        finally:
            os.chmod(delta_path, 0o644)
        assert c._in_txn is False
        c.close()

        # The commit marker was durable: the row must be visible after a
        # reopen (crash recovery re-applies the missing rows).
        c2 = ApexClient(d, _auto_manage=False, durability="safe")
        c2.use_table("t")
        names = [r["name"] for r in c2.execute("SELECT name FROM t ORDER BY _id").to_dict()]
        assert names == ["seed", "pending"]
        c2.close()


def test_failed_commit_before_marker_aborts_cleanly():
    with tempfile.TemporaryDirectory() as d:
        c = ApexClient(d, _auto_manage=False, durability="safe")
        c.create_table("t", {"name": "string", "value": "int"})
        c.use_table("t")
        c.store([{"name": "seed", "value": 0}])
        c.flush()

        c._storage.execute("BEGIN")
        c._in_txn = True
        c.execute("INSERT INTO t (name, value) VALUES ('pending', 1)")

        # Make the WAL non-writable so the commit fails before the commit
        # marker is written.
        wal_path = os.path.join(d, "t.apex.wal")
        os.chmod(wal_path, 0o444)
        try:
            with pytest.raises(RuntimeError, match="commit_outcome=not_committed"):
                c.execute("COMMIT")
        finally:
            os.chmod(wal_path, 0o644)
        assert c._in_txn is False
        c.close()

        c2 = ApexClient(d, _auto_manage=False, durability="safe")
        c2.use_table("t")
        names = [r["name"] for r in c2.execute("SELECT name FROM t ORDER BY _id").to_dict()]
        assert names == ["seed"]
        c2.close()


def test_commit_watermark_failure_is_committed_and_visible(tmp_path):
    c = ApexClient(str(tmp_path), _auto_manage=False, durability="safe")
    c.create_table("t", {"name": "string", "value": "int"})
    c.use_table("t")
    c.store([{"name": "seed", "value": 0}])
    c.flush()
    other = ApexClient(str(tmp_path), _auto_manage=False, durability="safe")
    other.use_table("t")
    assert other.execute("SELECT COUNT(*) FROM t").scalar() == 1

    c._storage.execute("BEGIN")
    c._in_txn = True
    c.execute("INSERT INTO t (name, value) VALUES ('committed', 1)")
    marker = tmp_path / "t.apex.wal.meta"
    if marker.exists():
        marker.unlink()
    marker.mkdir()  # Real I/O failure, without replacing the storage path.
    try:
        with pytest.raises(RuntimeError, match="commit_outcome=committed") as error:
            c.execute("COMMIT")
        assert "do not replay DML" in str(error.value)
        assert c._in_txn is False
    finally:
        marker.rmdir()
    assert other.execute("SELECT COUNT(*) FROM t").scalar() == 2
    c._storage.execute("BEGIN")
    c._in_txn = True
    c.execute("INSERT INTO t (name, value) VALUES ('next', 2)")
    c.execute("COMMIT")
    other.close()
    c.close()
    with ApexClient(str(tmp_path), _auto_manage=False, durability="safe") as reopened:
        reopened.use_table("t")
        assert reopened.execute("SELECT name FROM t ORDER BY _id").to_dict() == [
            {"name": "seed"}, {"name": "committed"}, {"name": "next"},
        ]


def test_cross_table_apply_failure_is_unknown(tmp_path):
    c = ApexClient(str(tmp_path), _auto_manage=False, durability="safe")
    for table in ("left_t", "right_t"):
        c.create_table(table, {"value": "int"})
        c.use_table(table)
        c.store([{"value": 0}])
        c.flush()
    c._storage.execute("BEGIN")
    c._in_txn = True
    c.execute("INSERT INTO left_t (value) VALUES (1)")
    c.execute("INSERT INTO right_t (value) VALUES (2)")
    delta = tmp_path / "right_t.apex.delta"
    delta.touch()
    delta.chmod(0o444)
    try:
        with pytest.raises(RuntimeError, match="commit_outcome=unknown"):
            c.execute("COMMIT")
        assert c._in_txn is False
    finally:
        delta.chmod(0o644)
    c.close()
    with ApexClient(str(tmp_path), _auto_manage=False, durability="safe") as reopened:
        for table, value in (("left_t", 1), ("right_t", 2)):
            reopened.use_table(table)
            assert reopened.execute(f"SELECT value FROM {table} ORDER BY _id").to_dict() == [
                {"value": 0}, {"value": value},
            ]


@pytest.mark.parametrize("durability", ["safe", "max"])
def test_wal_backed_transaction_update_commits_and_survives_reopen(tmp_path, durability):
    c = ApexClient(str(tmp_path), _auto_manage=False, durability=durability)
    c.create_table("t", {"name": "string", "value": "int"})
    c.use_table("t")
    c.store([{"name": "seed", "value": 7}])
    c.flush()

    wal_path = tmp_path / "t.apex.wal"
    wal_len = wal_path.stat().st_size
    c._storage.execute("BEGIN")
    c._in_txn = True
    c.execute("UPDATE t SET value = 9 WHERE _id = 1")
    c.execute("COMMIT")
    assert c._in_txn is False
    assert wal_path.stat().st_size > wal_len
    assert c.execute("SELECT value FROM t WHERE _id = 1").scalar() == 9

    c._storage.execute("BEGIN")
    c._in_txn = True
    c.execute("INSERT INTO t (name, value) VALUES ('next', 8)")
    c.execute("COMMIT")
    c.close()

    with ApexClient(
        str(tmp_path), _auto_manage=False, durability=durability
    ) as reopened:
        reopened.use_table("t")
        assert reopened.execute("SELECT name, value FROM t ORDER BY _id").to_dict() == [
            {"name": "seed", "value": 9},
            {"name": "next", "value": 8},
        ]


def test_wal_backed_transaction_update_apply_failure_recovers(tmp_path):
    c = ApexClient(str(tmp_path), _auto_manage=False, durability="safe")
    c.create_table("t", {"value": "int", "other": "int"})
    c.use_table("t")
    c.store([{"value": 7, "other": 1}])
    c.flush()

    c._storage.execute("BEGIN")
    c._in_txn = True
    c.execute("UPDATE t SET value = 9, other = 2 WHERE _id = 1")
    fault = tmp_path / "t.apex.deltastore.tmp"
    fault.mkdir()
    try:
        with pytest.raises(RuntimeError, match="commit_outcome=unknown"):
            c.execute("COMMIT")
        assert c._in_txn is False
    finally:
        fault.rmdir()
    c.close()

    with ApexClient(str(tmp_path), _auto_manage=False, durability="safe") as reopened:
        reopened.use_table("t")
        assert reopened.execute("SELECT value, other FROM t WHERE _id = 1").to_dict() == [
            {"value": 9, "other": 2},
        ]
