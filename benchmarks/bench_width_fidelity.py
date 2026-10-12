"""Quantify the V5 width-fidelity gain: V4 full-width layout vs V5 narrow.

A `Float32` column is declared at four bytes per value. V5 stores it that way
(`COL_ENCODING_PLAIN_F32`, header flag `FLAG_NARROW_FLOAT32`, file version 5);
the V4 layout always stores eight bytes per value. Both layouts are readable by
this build, so the difference is measurable end to end:

* bytes on disk (table file plus every sidecar file the engine keeps next to it)
* warm query latency, per shape
* first-touch page faults after a client reopen — the process-cold proxy for
  "how many bytes did this query actually touch" (the OS page cache stays warm,
  so real disk blocks are reported for information only)
* the narrow/wide latency ratio per shape, which is the V5 gain itself

The same dataset is written twice from identical input, so the only difference
between the two tables is the physical width of the `Float32` column. A build
without the narrow encoding (any pre-V5 build) ignores the layout switch, so its
two tables are identical: that is what makes the base/current comparison of the
`Width: ... (V5 narrow)` metrics meaningful.

Metric rows are emitted in the guard's report format (`query` + `ApexBase`), so
`benchmarks/compare_perf_baseline.py` and
`benchmarks/run_local_perf_guard.py --mode full` consume them directly. Latency
rows are milliseconds; the `bytes` and `pages` rows are counts (a larger value
is worse, which is what the comparator already assumes).
"""

from __future__ import annotations

import argparse
import json
import os
import random
try:
    import resource
except ImportError:  # Windows has no `resource` module.
    resource = None


def page_faults():
    """Cumulative page faults of this process, on every platform we test.

    Windows has no `resource` module, so importing it at module scope made
    `bench_width_fidelity` unloadable there (the manifest test loads it). Windows
    reports the count through `GetProcessMemoryInfo` instead.
    """
    if resource is not None:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return usage.ru_minflt + usage.ru_majflt

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
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_Counters),
        wintypes.DWORD,
    ]
    counters = _Counters()
    counters.cb = ctypes.sizeof(counters)
    if not psapi.GetProcessMemoryInfo(
        kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
    ):
        raise ctypes.WinError(ctypes.get_last_error())
    return counters.PageFaultCount

import shutil
import statistics
import struct
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench_vs_sqlite_duckdb as full_bench
from apexbase.client import ApexClient


# (metric name, SQL). `{table}` is the layout's table and `{id}` a real
# persisted row id.
SHAPES = (
    ("Float32 projection", "SELECT score FROM {table}"),
    (
        "Float32 aggregates",
        "SELECT COUNT(*), SUM(score), MIN(score), MAX(score) FROM {table}",
    ),
    ("Float32 range filter", "SELECT * FROM {table} WHERE score BETWEEN 20 AND 80"),
    (
        "Float32 selective filter",
        "SELECT score, age FROM {table} WHERE score BETWEEN 42.5 AND 43.5",
    ),
    ("Int64 control filter", "SELECT * FROM {table} WHERE age BETWEEN 25 AND 35"),
    # `flag` is uniform per row group (see `generate_rows`), so a Bool
    # predicate can skip whole groups and the page count shows it.
    ("Bool filter (uniform groups)", "SELECT COUNT(*) AS n FROM {table} WHERE flag = true"),
    ("Bool filter + projection", "SELECT score FROM {table} WHERE flag = false"),
    ("Full row scan", "SELECT * FROM {table}"),
    ("Point lookup", "SELECT * FROM {table} WHERE _id = {id}"),
)

# (label, APEX_WIDTH_FIDELITY value or None, expected file version)
LAYOUTS = (
    ("V4 layout", "0", 4),
    ("V5 narrow", None, 5),
)

PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
FIRST_TOUCH_CYCLES = 3


def f32(value: float) -> float:
    """Round to the precision the narrow layout stores."""
    return struct.unpack("f", struct.pack("f", value))[0]


def generate_rows(n: int):
    """Deterministic dataset whose `score` values survive an f32 round trip."""
    rng = random.Random(4242)
    return {
        "name": [f"wf_{i}" for i in range(n)],
        "age": [rng.randint(18, 80) for _ in range(n)],
        "score": [f32(round(rng.uniform(0, 100), 2)) for _ in range(n)],
        "city": [rng.choice(full_bench.CITIES) for _ in range(n)],
        "category": [rng.choice(full_bench.CATEGORIES) for _ in range(n)],
        # Runs of the largest adaptive row-group size (131_072), which the
        # smaller choices divide, so every row group is uniformly true or
        # uniformly false whichever size the writer picks.
        "flag": [(i // 131_072) % 2 == 0 for i in range(n)],
    }


def sidecar_bytes(db_dir: Path, table: str) -> int:
    """Every file the engine keeps for one table (base file + sidecars)."""
    return sum(
        path.stat().st_size
        for path in db_dir.glob(f"{table}*")
        if path.is_file()
    )


class WidthBench:
    """Minimal bench facade so the layout rows keep the ApexBaseBench shape."""

    def __init__(self, db_dir: Path, table: str):
        self.db_dir = db_dir
        self.table = table
        self.client = None

    def open(self):
        # The result cache would answer the repeated identical SQL without
        # touching the file, which would hide every I/O difference.
        self.client = ApexClient(str(self.db_dir), enable_cache=False)
        self.client.use_table(self.table)

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None

    def query(self, sql: str):
        return self.client.execute(sql).to_dict()


def measure_warm(bench: WidthBench, sql: str, warmup: int, iterations: int) -> float:
    for _ in range(warmup):
        bench.query(sql)
    samples = []
    for _ in range(iterations):
        started = time.perf_counter()
        bench.query(sql)
        samples.append((time.perf_counter() - started) * 1000.0)
    return statistics.median(samples)


def measure_first_touch(bench: WidthBench, sql: str, cycles: int) -> tuple[float, int]:
    """Reopen the client, then report (median ms, minimum page faults).

    A reopen drops the process-side mmap and every in-process cache, so the
    first query of each cycle faults its pages back in. The OS page cache stays
    warm, which is why the byte figure derived from the fault count is a
    *touch* count rather than a disk-read count.

    Page faults are counted with the **minimum** across cycles: allocation and
    interpreter noise can only add faults, so the smallest observation is the
    stable estimate of what the query itself needed (a median over three cycles
    swung between 0 and 142 pages on an identical table, which is not a
    measurable property of the query).
    """
    times = []
    faults = []
    for _ in range(cycles):
        bench.close()
        bench.open()
        before = page_faults()
        started = time.perf_counter()
        bench.query(sql)
        elapsed = (time.perf_counter() - started) * 1000.0
        after = page_faults()
        times.append(elapsed)
        faults.append(after - before)
    return statistics.median(times), int(min(faults))


def run_width_fidelity(rows: int, warmup: int, iterations: int):
    full_bench.ensure_optional_imports()
    if not full_bench.HAS_APEXBASE:
        raise RuntimeError("ApexBase is not importable; run maturin develop --release first")

    data = generate_rows(rows)
    tmpdir = Path(tempfile.mkdtemp(prefix="apexbase_width_"))
    db_dir = tmpdir / "db"
    results = []
    detail = {"rows": rows, "table_bytes": {}, "files": {}, "version": {}}
    previous_env = os.environ.get("APEX_WIDTH_FIDELITY")
    try:
        db_dir.mkdir(parents=True, exist_ok=True)
        client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)
        for label, env_value, expected_version in LAYOUTS:
            table = "wf_v4" if env_value == "0" else "wf_v5"
            if env_value is None:
                os.environ.pop("APEX_WIDTH_FIDELITY", None)
            else:
                os.environ["APEX_WIDTH_FIDELITY"] = env_value
            client.create_table(
                table,
                {
                    "name": "string",
                    "age": "int",
                    "score": "float32",
                    "city": "string",
                    "category": "string",
                    "flag": "bool",
                },
            )
            client.use_table(table)
            client.store(data)
            client.flush()
            version = int.from_bytes((db_dir / f"{table}.apex").read_bytes()[8:12], "little")
            if env_value == "0" and version != 4:
                raise RuntimeError(
                    f"the V4 layout must declare version 4, got {version}"
                )
            if env_value is None:
                detail["narrow_supported"] = version == expected_version
            detail["version"][label] = version
            detail["table_bytes"][label] = sidecar_bytes(db_dir, table)
            detail["files"][label] = sorted(
                path.name for path in db_dir.glob(f"{table}*") if path.is_file()
            )
            results.append({
                "category": "ApexBase width fidelity",
                "query": f"Width: bytes on disk ({label})",
                "ApexBase": detail["table_bytes"][label],
            })
        client.close()

        point_lookup_id = rows // 2 + 1
        for label, env_value, _version in LAYOUTS:
            table = "wf_v4" if env_value == "0" else "wf_v5"
            bench = WidthBench(db_dir, table)
            bench.open()
            try:
                for name, sql in SHAPES:
                    query = sql.format(table=table, id=point_lookup_id)
                    warm_ms = measure_warm(bench, query, warmup, iterations)
                    cold_ms, pages = measure_first_touch(bench, query, FIRST_TOUCH_CYCLES)
                    results.append({
                        "category": "ApexBase width fidelity",
                        "query": f"Width: {name} ({label})",
                        "ApexBase": round(warm_ms, 6),
                    })
                    # Page-fault counts vary by process (they include
                    # interpreter/allocation faults), so they stay in the
                    # informational block rather than gating: the same table
                    # reported between 0 and 142 pages across guard samples.
                    detail.setdefault("shapes", {}).setdefault(name, {})[label] = {
                        "warm_ms": round(warm_ms, 6),
                        "first_touch_ms": round(cold_ms, 6),
                        "pages": pages,
                        "touched_bytes_estimate": pages * PAGE_SIZE,
                    }
                    detail.setdefault("pages_metric", {})[f"{name} ({label})"] = pages
                    print(
                        f"{name:<28} {label:<10} warm {warm_ms:>10.4f} ms  "
                        f"first-touch {cold_ms:>10.4f} ms  pages {pages:>7}"
                    )
            finally:
                bench.close()

        # The V5 gain itself: narrow latency and footprint relative to the V4
        # layout of the same data. Reported for information (ratios are not
        # milliseconds, so they are not gate metrics).
        gain = {}
        for name, _sql in SHAPES:
            entry = detail.get("shapes", {}).get(name)
            if not entry:
                continue
            wide = entry.get("V4 layout")
            narrow = entry.get("V5 narrow")
            if wide and narrow:
                gain[name] = {
                    "warm_ratio": round(narrow["warm_ms"] / wide["warm_ms"], 4)
                    if wide["warm_ms"]
                    else None,
                    "first_touch_ratio": round(
                        narrow["first_touch_ms"] / wide["first_touch_ms"], 4
                    )
                    if wide["first_touch_ms"]
                    else None,
                    "pages_ratio": round(narrow["pages"] / wide["pages"], 4)
                    if wide["pages"]
                    else None,
                }
        detail["gain"] = gain
        wide_bytes = detail["table_bytes"].get("V4 layout", 0)
        narrow_bytes = detail["table_bytes"].get("V5 narrow", 0)
        detail["bytes_ratio"] = (
            round(narrow_bytes / wide_bytes, 4) if wide_bytes else None
        )
        print("\nV5 narrow / V4 layout ratios (below 1.0 means V5 is better):")
        print(f"  bytes on disk {detail['bytes_ratio']}")
        for name, values in gain.items():
            print(
                f"  {name:<28} warm {values['warm_ratio']}  "
                f"first-touch {values['first_touch_ratio']}  pages {values['pages_ratio']}"
            )
    finally:
        if previous_env is None:
            os.environ.pop("APEX_WIDTH_FIDELITY", None)
        else:
            os.environ["APEX_WIDTH_FIDELITY"] = previous_env
        shutil.rmtree(tmpdir, ignore_errors=True)
    return results, detail


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=7)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.rows <= 0 or args.warmup < 0 or args.iterations <= 0:
        parser.error("rows and iterations must be positive; warmup must be non-negative")

    results, detail = run_width_fidelity(args.rows, args.warmup, args.iterations)
    report = {
        **full_bench.get_report_metadata("apexbase-canary"),
        "config": {
            "rows": args.rows,
            "warmup": args.warmup,
            "iterations": args.iterations,
        },
        "results": results,
        "width_fidelity": detail,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
