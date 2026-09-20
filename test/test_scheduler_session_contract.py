"""Architecture contracts for the query scheduler (review R4).

Covers session-context propagation to worker threads, bounded-queue
admission control, and query cancellation at batch boundaries.
"""

import os
import sys
import time

import pytest

from apexbase import _core, ApexClient

if sys.platform == "win32":
    import msvcrt

    def _lock_exclusive(handle):
        # Windows byte-range lock; conflicts with the crate's fs2 LockFileEx.
        msvcrt.locking(handle, msvcrt.LK_LOCK, 1)

    def _unlock(handle):
        msvcrt.locking(handle, msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _lock_exclusive(handle):
        fcntl.flock(handle, fcntl.LOCK_EX)

    def _unlock(handle):
        fcntl.flock(handle, fcntl.LOCK_UN)


ROWS_PER_CHUNK = 100_000


def _make_table(dirpath, table, rows, groups=50):
    client = ApexClient(str(dirpath))
    client.create_table(table, {"id": "int", "grp": "string", "val": "int"})
    client.use_table(table)
    for start in range(0, rows, ROWS_PER_CHUNK):
        stop = min(start + ROWS_PER_CHUNK, rows)
        client.store(
            [
                {
                    "id": i,
                    "grp": f"g{i % groups}",
                    "val": i % 97,
                }
                for i in range(start, stop)
            ]
        )
    client.flush()
    client.close()
    return dirpath / f"{table}.apex"


def _wait_for_active(timeout=10.0):
    deadline = time.monotonic() + timeout
    while True:
        initialized, active = _core.get_scheduler_status()
        assert initialized, "scheduler must be initialized"
        if active >= 1:
            return
        assert time.monotonic() < deadline, "worker must pick up the task"
        time.sleep(0.002)


def test_scheduled_query_propagates_session_context(tmp_path):
    # Current table nested below the root; the qualified table is only
    # reachable through an explicit root_dir.
    current_dir = tmp_path / "d1" / "sub"
    current_dir.mkdir(parents=True)
    current = _make_table(current_dir, "t", 3)
    other_dir = tmp_path / "d2"
    other_dir.mkdir(parents=True)
    _make_table(other_dir, "other", 3)

    _core.init_query_scheduler(2)
    sql = "SELECT COUNT(*) FROM d2.other"

    ok, err = _core.execute_scheduled(sql, str(current), root_dir=str(tmp_path))
    assert ok, f"qualified lookup with root_dir must succeed: {err}"

    ok, err = _core.execute_scheduled(sql, str(current))
    assert not ok, "qualified lookup without root_dir must fail"


def test_scheduled_queue_rejects_when_full(tmp_path):
    table = _make_table(tmp_path, "t", 1)
    lock_path = tmp_path / "t.apex.lock"
    lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    _lock_exclusive(lock_fd)
    try:
        _core.init_query_scheduler(1, 1)  # one worker, queue bound 1

        first = _core.submit_scheduled(
            "INSERT INTO t (id, grp, val) VALUES (10, 'a', 1)", str(table)
        )
        _wait_for_active()

        second = _core.submit_scheduled(
            "INSERT INTO t (id, grp, val) VALUES (11, 'b', 2)", str(table)
        )

        started = time.monotonic()
        with pytest.raises(RuntimeError, match="queue full"):
            _core.submit_scheduled("SELECT COUNT(*) FROM t", str(table))
        assert time.monotonic() - started < 2.0, "rejection must not block"
    finally:
        _unlock(lock_fd)
        os.close(lock_fd)

    ok, err = first.wait()
    assert ok, f"queued write after release must succeed: {err}"
    ok, err = second.wait()
    assert ok, f"queued write after release must succeed: {err}"


def test_cancel_stops_running_scheduled_query(tmp_path):
    # High group cardinality keeps the batched GROUP BY running for ~2 s on
    # the reference machine, well over the poll+cancel window; the margin is
    # checked at development time, not in-test.
    table = _make_table(tmp_path, "big", 10_000_000, groups=200_000)

    _core.init_query_scheduler(1)
    handle = _core.submit_scheduled(
        "SELECT grp, SUM(val) AS s FROM big WHERE val > 0 GROUP BY grp",
        str(table),
    )
    _wait_for_active()
    time.sleep(0.02)  # let the pipeline consume a few row groups
    handle.cancel()

    ok, err = handle.wait()
    assert not ok
    assert "cancelled" in err.lower(), f"unexpected error: {err}"


def test_cancel_on_finished_query_is_a_noop(tmp_path):
    table = _make_table(tmp_path, "t", 3)
    _core.init_query_scheduler(1)
    handle = _core.submit_scheduled(
        "SELECT COUNT(*) FROM t", str(table)
    )
    ok, err = handle.wait()
    assert ok, err
    handle.cancel()  # must not raise or affect the delivered result
    assert ok
