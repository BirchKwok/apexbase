"""S1: per-query aggregation memory budget.

The batched aggregation kernel charges its high-cardinality state against a
per-query byte budget (`APEX_QUERY_MEMORY_MB`). These tests drive the real
release build: an over-budget high-cardinality GROUP BY must fail with a
clear error, an unlimited process must succeed, and the released budget must
not leak into the next query on the same thread.
"""

import json
import os
import subprocess
import sys
import tempfile

import pytest

from apexbase import ApexClient

LIMIT_ENV = "APEX_QUERY_MEMORY_MB"
ROWS = 30_000


@pytest.fixture
def client():
    with tempfile.TemporaryDirectory() as tmp:
        instance = ApexClient(tmp, drop_if_exists=True, enable_cache=False)
        instance.create_table("mem_budget_t", {"k": "string", "v": "int"})
        instance.use_table("mem_budget_t")
        instance.store(
            {
                "k": [f"key{i:06d}" for i in range(ROWS)],
                "v": list(range(ROWS)),
            }
        )
        instance.flush()
        yield instance
        instance.close()


def _with_limit(megabytes, fn):
    previous = os.environ.get(LIMIT_ENV)
    if megabytes is None:
        os.environ.pop(LIMIT_ENV, None)
    else:
        os.environ[LIMIT_ENV] = str(megabytes)
    try:
        return fn()
    finally:
        if previous is None:
            os.environ.pop(LIMIT_ENV, None)
        else:
            os.environ[LIMIT_ENV] = previous


HIGH_CARDINALITY_SQL = (
    "SELECT k, v, COUNT(*) AS n FROM mem_budget_t WHERE v >= 0 GROUP BY k, v"
)
LOW_CARDINALITY_SQL = (
    "SELECT v, COUNT(*) AS n FROM mem_budget_t WHERE v < 10 GROUP BY v"
)


def test_over_budget_group_by_reports_clear_error(client):
    # ~30k distinct (k, v) groups need several MiB of tracked state.
    def run():
        with pytest.raises(RuntimeError, match="query memory budget exceeded"):
            client.execute(HIGH_CARDINALITY_SQL)

    _with_limit(1, run)


def test_unlimited_budget_serves_the_same_query(client):
    def run():
        rows = client.execute(HIGH_CARDINALITY_SQL).to_dict()
        assert len(rows) == ROWS

    _with_limit(0, run)


def test_released_budget_does_not_leak_into_the_next_query(client):
    def run():
        # A small aggregation fits the 1 MiB budget.
        small = client.execute(LOW_CARDINALITY_SQL).to_dict()
        assert len(small) == 10

        with pytest.raises(RuntimeError, match="query memory budget exceeded"):
            client.execute(HIGH_CARDINALITY_SQL)

        # The failed query released its charge and its guard restored the
        # context: the same small query and a non-aggregate query still work.
        small = client.execute(LOW_CARDINALITY_SQL).to_dict()
        assert len(small) == 10
        assert client.execute("SELECT COUNT(*) FROM mem_budget_t").scalar() == ROWS

    _with_limit(1, run)


def test_low_cardinality_group_by_stays_within_budget(client):
    def run():
        rows = client.execute(LOW_CARDINALITY_SQL).to_dict()
        assert sorted(row["v"] for row in rows) == list(range(10))

    _with_limit(1, run)


# No WHERE clause: these shapes are served by the single-batch aggregation
# kernels (streaming distinct, vectorized hash, row-index fallback) rather
# than the batched scan pipeline, and each must charge the same budget.
GENERIC_HIGH_CARDINALITY_SHAPES = (
    ("single string key COUNT(*)", "SELECT k, COUNT(*) FROM mem_budget_t GROUP BY k", ROWS),
    ("single string key SUM", "SELECT k, SUM(v) FROM mem_budget_t GROUP BY k", ROWS),
    (
        "single string key COUNT(DISTINCT)",
        "SELECT k, COUNT(DISTINCT v) FROM mem_budget_t GROUP BY k",
        ROWS,
    ),
    ("two keys", "SELECT k, v, COUNT(*) FROM mem_budget_t GROUP BY k, v", ROWS),
    (
        "ordered with limit",
        "SELECT k, COUNT(*) AS n FROM mem_budget_t GROUP BY k ORDER BY n DESC LIMIT 5",
        5,
    ),
)


@pytest.mark.parametrize("label, sql, expected_rows", GENERIC_HIGH_CARDINALITY_SHAPES)
def test_generic_high_cardinality_shapes_are_bounded(client, label, sql, expected_rows):
    def run():
        with pytest.raises(RuntimeError, match="query memory budget exceeded"):
            client.execute(sql)

    _with_limit(1, run)


@pytest.mark.parametrize("label, sql, expected_rows", GENERIC_HIGH_CARDINALITY_SHAPES)
def test_generic_high_cardinality_shapes_run_unlimited(client, label, sql, expected_rows):
    def run():
        rows = client.execute(sql).to_dict()
        assert len(rows) == expected_rows

    _with_limit(0, run)


# --- Peak-RSS boundedness and concurrency (S1 acceptance) --------------------

RSS_ROWS = 200_000

_RSS_CHILD = r'''
import json
import sys

from apexbase import ApexClient


def peak_mb():
    """Peak resident set size of this process, in MB (portable)."""
    if sys.platform.startswith("linux"):
        # Linux keeps `ru_maxrss` across fork/exec, so a child can inherit
        # its parent's peak. VmHWM is the high water mark of this address
        # space and reports only what this process actually used.
        try:
            with open("/proc/self/status", encoding="utf-8") as status:
                for line in status:
                    if line.startswith("VmHWM:"):
                        return int(line.split()[1]) / 1024
        except OSError:
            pass
    try:
        import resource
    except ImportError:  # Windows has no resource module.
        import ctypes
        from ctypes import wintypes

        class _Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.GetCurrentProcess.argtypes = []
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(_Counters),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        counters = _Counters()
        counters.cb = ctypes.sizeof(counters)
        if not psapi.GetProcessMemoryInfo(
            kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        ):
            raise ctypes.WinError(ctypes.get_last_error())
        return counters.PeakWorkingSetSize / (1024 * 1024)

    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return usage / (1024 * 1024)
    return usage / 1024


def main():
    dirpath = sys.argv[1]
    client = ApexClient(dirpath, enable_cache=False)
    client.use_table("mem_budget_t")
    outcome = "ok"
    try:
        client.execute("SELECT k, COUNT(*) FROM mem_budget_t GROUP BY k").to_dict()
    except RuntimeError:
        outcome = "error"
    print(json.dumps({"outcome": outcome, "peak_mb": peak_mb()}))
    client.close()


main()
'''


def _seed_rss_fixture(dirpath):
    client = ApexClient(dirpath, drop_if_exists=True, enable_cache=False)
    client.create_table("mem_budget_t", {"k": "string", "v": "int"})
    client.use_table("mem_budget_t")
    chunk = 50_000
    for start in range(0, RSS_ROWS, chunk):
        end = min(start + chunk, RSS_ROWS)
        client.store(
            {
                "k": [f"key{i:06d}" for i in range(start, end)],
                "v": list(range(start, end)),
            }
        )
    client.flush()
    client.close()


def _run_rss_child(dirpath, limit_mb):
    env = dict(os.environ)
    env[LIMIT_ENV] = str(limit_mb)
    completed = subprocess.run(
        [sys.executable, "-c", _RSS_CHILD, dirpath, str(limit_mb)],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_bounded_run_has_lower_peak_rss_than_unlimited():
    with tempfile.TemporaryDirectory() as tmp:
        _seed_rss_fixture(tmp)
        bounded = _run_rss_child(tmp, 1)
        unlimited = _run_rss_child(tmp, 0)

    assert bounded["outcome"] == "error"
    assert unlimited["outcome"] == "ok"
    # The bounded run aborts after the budget is crossed, so its peak RSS must
    # stay clearly below the run that materializes every group.
    assert bounded["peak_mb"] < unlimited["peak_mb"], (bounded, unlimited)
    assert unlimited["peak_mb"] - bounded["peak_mb"] > 10.0, (bounded, unlimited)


def test_per_thread_budgets_do_not_leak_across_concurrent_queries():
    import threading

    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=True, enable_cache=False)
        client.create_table("mem_budget_t", {"k": "string", "v": "int"})
        client.use_table("mem_budget_t")
        client.store(
            {
                "k": [f"key{i:06d}" for i in range(ROWS)],
                "v": list(range(ROWS)),
            }
        )
        client.flush()
        client.close()

        outcomes = []
        lock = threading.Lock()

        def worker():
            local = ApexClient(tmp, enable_cache=False)
            local.use_table("mem_budget_t")
            result = []
            # A small aggregation must succeed even while other threads run the
            # over-budget one: the budget is per query/thread, not global.
            result.append(len(local.execute(LOW_CARDINALITY_SQL).to_dict()))
            try:
                local.execute(HIGH_CARDINALITY_SQL).to_dict()
                result.append("ok")
            except RuntimeError:
                result.append("error")
            local.close()
            with lock:
                outcomes.append(tuple(result))

        _with_limit(1, lambda: _run_threads(worker))

    assert len(outcomes) == 4
    for small_rows, big in outcomes:
        assert small_rows == 10
        assert big == "error"


def _run_threads(worker, count=4):
    import threading

    threads = [threading.Thread(target=worker) for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
