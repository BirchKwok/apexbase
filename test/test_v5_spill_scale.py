"""S11: 1M-row spill correctness + peak-memory assertion (10M runs in S13)."""

import json
import os
import subprocess
import sys
import tempfile

LIMIT_ENV = "APEX_QUERY_MEMORY_MB"

_CHILD = r'''
import json, sys


def _windows_memory_mb(field):
    import ctypes
    from ctypes import wintypes

    class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
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

    counters = PROCESS_MEMORY_COUNTERS()
    counters.cb = ctypes.sizeof(counters)
    kernel32 = ctypes.WinDLL("kernel32")
    psapi = ctypes.WinDLL("psapi")
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    get_info = psapi.GetProcessMemoryInfo
    get_info.argtypes = [ctypes.c_void_p, ctypes.POINTER(PROCESS_MEMORY_COUNTERS), wintypes.DWORD]
    get_info.restype = wintypes.BOOL
    if not get_info(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
        raise OSError("GetProcessMemoryInfo failed")
    return getattr(counters, field) / (1024 * 1024)


def peak_mb():
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/self/status", encoding="utf-8") as status:
                for line in status:
                    if line.startswith("VmHWM:"):
                        return int(line.split()[1]) / 1024
        except OSError:
            pass
    if sys.platform == "win32":
        return _windows_memory_mb("PeakWorkingSetSize")
    import resource
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage / (1024 * 1024) if sys.platform == "darwin" else usage / 1024

from apexbase import ApexClient

directory = sys.argv[1]
rows = 1_000_000
client = ApexClient(directory, drop_if_exists=True, enable_cache=False)
client.create_table("t", {"k": "int64", "v": "int64", "s": "string"})
client.use_table("t")
chunk = 100_000
for start in range(0, rows, chunk):
    end = min(start + chunk, rows)
    client.store({
        "k": [i % 1000 for i in range(start, end)],
        "v": list(range(start, end)),
        "s": [f"v{i % 100:03d}" for i in range(start, end)],
    })
client.flush()

grouped = client.execute("SELECT k, COUNT(*) AS n FROM t GROUP BY k").to_dict()
distinct = client.execute("SELECT DISTINCT k FROM t").to_dict()
ordered = client.execute("SELECT v FROM t ORDER BY v ASC").to_dict()
print(json.dumps({
    "group_rows": len(grouped),
    "group_sum": sum(int(row["n"]) for row in grouped),
    "distinct_rows": len(distinct),
    "ordered_rows": len(ordered),
    "ordered_first": ordered[0]["v"],
    "ordered_last": ordered[-1]["v"],
    "peak_mb": peak_mb(),
}))
client.close()
'''


def test_one_million_rows_spill_correctly_with_bounded_peak():
    with tempfile.TemporaryDirectory() as tmp:
        env = dict(os.environ)
        env[LIMIT_ENV] = "2"
        env["APEX_SPILL_DIR"] = os.path.join(tmp, "spill")
        completed = subprocess.run(
            [sys.executable, "-c", _CHILD, os.path.join(tmp, "db")],
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert completed.returncode == 0, completed.stderr
        result = json.loads(completed.stdout.strip().splitlines()[-1])
    assert result["group_rows"] == 1000
    assert result["group_sum"] == 1_000_000
    assert result["distinct_rows"] == 1000
    assert result["ordered_rows"] == 1_000_000
    assert result["ordered_first"] == 0
    assert result["ordered_last"] == 999_999
    # Spilled execution keeps the process peak well below materialising every
    # operator state at once (the 10M assertion is part of S13).
    assert result["peak_mb"] < 900, result
