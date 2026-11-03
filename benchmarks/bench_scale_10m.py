#!/usr/bin/env python3
"""S13: 10M-row scale validation under a restricted memory budget.

This is the release scale harness (V5 S13): it generates 10M rows, runs
aggregation / ordering / join / TopK workloads with ``APEX_QUERY_MEMORY_MB``
limited (default 1024 MB), records peak RSS and spill temp-file usage, validates
IVF recall and latency at 10M, and records DuckDB (plus a 1M-row SQLite subset)
comparison timings.

Run before a release (not part of the default ``--mode full`` gate because of
its 10M-row runtime):

    python benchmarks/bench_scale_10m.py --output local-perf-results/scale10m.json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))



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


def peak_mb() -> float:
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


def dir_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def dir_files(path: Path) -> int:
    return sum(1 for item in path.rglob("*") if item.is_file())


def sampled_spill(spill_dir: Path, interval: float = 0.02):
    """Background sampler returning (peak_files, peak_bytes) during a workload."""
    import threading

    stop = threading.Event()
    peak = [0, 0]

    def sample():
        while not stop.is_set():
            try:
                peak[0] = max(peak[0], dir_files(spill_dir))
                peak[1] = max(peak[1], dir_bytes(spill_dir))
            except OSError:
                pass
            time.sleep(interval)

    thread = threading.Thread(target=sample, daemon=True)
    thread.start()

    class Sampler:
        def stop(self):
            stop.set()
            thread.join(timeout=1.0)
            return peak[0], peak[1]

    return Sampler()


def timed(fn, warmup: int = 1, iterations: int = 3):
    result = None
    for _ in range(warmup):
        result = fn()
    samples = []
    for _ in range(iterations):
        started = time.perf_counter()
        result = fn()
        samples.append((time.perf_counter() - started) * 1000.0)
    return statistics.median(samples), result


def build_fact(client, rows: int, chunk: int = 200_000):
    rng = np.random.default_rng(7)
    client.create_table("fact", {"id": "int64", "k": "int64", "v": "int64", "s": "string"})
    client.use_table("fact")
    started = time.perf_counter()
    for start in range(0, rows, chunk):
        end = min(start + chunk, rows)
        size = end - start
        client.store(
            {
                "id": list(range(start, end)),
                "k": rng.integers(0, 1000, size=size, dtype=np.int64).tolist(),
                "v": rng.integers(0, rows, size=size, dtype=np.int64).tolist(),
                "s": [f"cat_{i % 97:02d}" for i in range(start, end)],
            }
        )
    client.flush()
    return time.perf_counter() - started


def build_dim(client):
    client.create_table("dim", {"k": "int64", "label": "string"})
    client.use_table("dim")
    client.store({"k": list(range(1000)), "label": [f"label_{i}" for i in range(1000)]})
    client.flush()


def apex_workloads(client, spill_dir: Path, rows: int):
    results = {}

    def run(name, sql, expect=None):
        sampler = sampled_spill(spill_dir)
        try:
            elapsed, result = timed(lambda: client.execute(sql).to_arrow())
        finally:
            peak_files, peak_bytes = sampler.stop()
        rows = result.num_rows
        assert expect is None or rows == expect, (name, rows, expect)
        results[name] = {
            "ms": round(elapsed, 3),
            "rows": rows,
            "peak_rss_mb": round(peak_mb(), 1),
            "spill_files": peak_files,
            "spill_bytes": peak_bytes,
        }

    run("count", "SELECT COUNT(*) FROM fact", expect=1)

    def group_low():
        return client.execute(
            "SELECT k, COUNT(*) AS n FROM fact GROUP BY k ORDER BY k"
        ).to_arrow()

    sampler = sampled_spill(spill_dir)
    try:
        elapsed, grouped = timed(group_low)
    finally:
        peak_files, peak_bytes = sampler.stop()
    assert grouped.num_rows == 1000
    results["group_by_k_1000"] = {
        "ms": round(elapsed, 3),
        "rows": grouped.num_rows,
        "peak_rss_mb": round(peak_mb(), 1),
        "spill_files": peak_files,
        "spill_bytes": peak_bytes,
    }

    def group_high():
        return client.execute(
            "SELECT id, COUNT(*) AS n FROM fact GROUP BY id"
        ).to_arrow()

    sampler = sampled_spill(spill_dir)
    try:
        elapsed, high = timed(group_high)
    finally:
        peak_files, peak_bytes = sampler.stop()
    assert high.num_rows == rows, (high.num_rows, rows)
    results["group_by_id_high_cardinality"] = {
        "ms": round(elapsed, 3),
        "rows": high.num_rows,
        "peak_rss_mb": round(peak_mb(), 1),
        "spill_files": peak_files,
        "spill_bytes": peak_bytes,
    }

    run("order_by_v_limit_100", "SELECT id, v FROM fact ORDER BY v DESC LIMIT 100", expect=100)
    run(
        "join_dim_count",
        "SELECT COUNT(*) FROM fact f JOIN dim d ON f.k = d.k",
        expect=1,
    )
    return results


def build_vectors(client, rows: int, dim: int, queries: int, chunk: int = 50_000, seed: int = 11):
    rng = np.random.default_rng(seed)
    vectors = rng.random((rows, dim), dtype=np.float32)
    query_vectors = rng.random((queries, dim), dtype=np.float32)
    client.create_table("vecs")
    client.use_table("vecs")
    started = time.perf_counter()
    for start in range(0, rows, chunk):
        end = min(start + chunk, rows)
        client.store([{"id": i, "vec": vectors[i]} for i in range(start, end)])
    client.flush()
    build_seconds = time.perf_counter() - started
    return vectors, query_vectors, build_seconds


def vector_workloads(client, vectors, query_vectors, k: int = 10, nprobe: int = 16):
    rows = len(vectors)
    exact = []
    for query in query_vectors:
        # ground truth by brute force (l2)
        diff = vectors - query
        distances = np.einsum("ij,ij->i", diff, diff)
        exact.append(set(np.argsort(distances)[:k].tolist()))

    timings = []
    recalls = []
    for query, truth in zip(query_vectors, exact):
        started = time.perf_counter()
        result = client.topk_distance(
            "vec", query.tolist(), k, "l2", raw=True, nprobe=16
        )
        timings.append((time.perf_counter() - started) * 1000.0)
        # `_id` is the internal 1-based row id; the exact set is by row index.
        ids = set(int(item) - 1 for item in result["ids"])
        recalls.append(len(ids & truth) / k)
    return {
        "queries": len(query_vectors),
        "k": k,
        "median_ms": round(statistics.median(timings), 3),
        "p95_ms": round(sorted(timings)[max(0, int(len(timings) * 0.95) - 1)], 3),
        "recall_at_k": round(float(np.mean(recalls)), 4),
        "nprobe": nprobe,
        "rows": rows,
    }


def export_parquet(client, path: Path, limit: int | None = None):
    sql = "SELECT id, k, v FROM fact"
    if limit is not None:
        sql += f" LIMIT {limit}"
    table = client.execute(sql).to_arrow()
    import pyarrow.parquet as pq

    pq.write_table(table, str(path), compression="snappy")
    return path


def duckdb_compare(parquet_path: Path, spill_dir: Path):
    try:
        import duckdb
    except Exception:
        return {"status": "skipped", "reason": "duckdb is not importable"}
    con = duckdb.connect()
    queries = {
        "count": "SELECT COUNT(*) FROM fact",
        "group_by_k_1000": "SELECT k, COUNT(*) FROM fact GROUP BY k ORDER BY k",
        "order_by_v_limit_100": "SELECT id, v FROM fact ORDER BY v DESC LIMIT 100",
    }
    con.execute(f"CREATE VIEW fact AS SELECT * FROM read_parquet('{parquet_path}')")
    out = {}
    for name, sql in queries.items():
        elapsed, rows = timed(lambda: con.execute(sql).fetchall(), warmup=1, iterations=3)
        out[name] = {"ms": round(elapsed, 3), "rows": len(rows)}
    con.close()
    return {"status": "ok", "metrics": out}


def sqlite_compare(parquet_path: Path, rows: int = 1_000_000):
    import sqlite3

    import pyarrow.parquet as pq

    table = pq.read_table(str(parquet_path))
    table = table.slice(0, rows)
    columns = {name: table.column(name).to_pylist() for name in table.column_names}
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE fact (id INTEGER, k INTEGER, v INTEGER)")
    con.execute("PRAGMA synchronous=OFF")
    con.executemany(
        "INSERT INTO fact VALUES (?,?,?)",
        zip(columns["id"], columns["k"], columns["v"]),
    )
    con.commit()
    queries = {
        "count": "SELECT COUNT(*) FROM fact",
        "group_by_k_1000": "SELECT k, COUNT(*) FROM fact GROUP BY k ORDER BY k",
        "order_by_v_limit_100": "SELECT id, v FROM fact ORDER BY v DESC LIMIT 100",
    }
    out = {}
    for name, sql in queries.items():
        started = time.perf_counter()
        rows_result = con.execute(sql).fetchall()
        out[name] = {
            "ms": round((time.perf_counter() - started) * 1000.0, 3),
            "rows": len(rows_result),
        }
    con.close()
    return {"status": "ok", "subset_rows": len(columns["id"]), "metrics": out}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--vector-dim", type=int, default=32)
    parser.add_argument("--vector-queries", type=int, default=10)
    parser.add_argument("--budget-mb", type=int, default=1024)
    parser.add_argument("--skip-vector", action="store_true")
    parser.add_argument("--skip-competitors", action="store_true")
    parser.add_argument("--db-dir", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    tmp_root = Path(tempfile.mkdtemp(prefix="apexbase_scale10m_"))
    db_dir = args.db_dir or (tmp_root / "db")
    db_dir.mkdir(parents=True, exist_ok=True)
    spill_dir = tmp_root / "spill"
    os.environ["APEX_QUERY_MEMORY_MB"] = str(args.budget_mb)
    os.environ["APEX_SPILL_DIR"] = str(spill_dir)
    started_at = time.time()
    report = {
        "format_version": 1,
        "suite": "scale_10m",
        "rows": args.rows,
        "budget_mb": args.budget_mb,
        "vector_dim": args.vector_dim,
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "started_at": started_at,
    }
    try:
        from apexbase import ApexClient

        client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)
        try:
            report["fact_build_seconds"] = round(build_fact(client, args.rows), 2)
            build_dim(client)
            report["fact_db_bytes"] = dir_bytes(db_dir)
            report["apex"] = apex_workloads(client, spill_dir, args.rows)
            report["peak_rss_mb_after_fact"] = round(peak_mb(), 1)

            parquet_path = tmp_root / "fact.parquet"
            export_started = time.perf_counter()
            export_parquet(client, parquet_path)
            report["parquet_export_seconds"] = round(time.perf_counter() - export_started, 2)
            report["parquet_bytes"] = parquet_path.stat().st_size

            if not args.skip_competitors:
                report["duckdb"] = duckdb_compare(parquet_path, spill_dir)
                report["sqlite"] = sqlite_compare(parquet_path)
        finally:
            client.close()

        if not args.skip_vector:
            client = ApexClient(str(db_dir), drop_if_exists=False, enable_cache=False)
            try:
                vectors, query_vectors, build_seconds = build_vectors(
                    client, args.rows, args.vector_dim, args.vector_queries
                )
                report["vector_build_seconds"] = round(build_seconds, 2)
                report["vector_db_bytes"] = dir_bytes(db_dir)
                index_started = time.perf_counter()
                client.create_vector_index(
                    "vec", nlist=64, iterations=5, metric="l2"
                )
                report["vector_index_build_seconds"] = round(
                    time.perf_counter() - index_started, 2
                )
                report["vector"] = vector_workloads(client, vectors, query_vectors)
                report["peak_rss_mb_after_vector"] = round(peak_mb(), 1)
            finally:
                client.close()
    finally:
        report["duration_seconds"] = round(time.time() - started_at, 2)
        report["peak_rss_mb"] = round(peak_mb(), 1)
        report["spill_files"] = dir_files(spill_dir)
        report["spill_bytes"] = dir_bytes(spill_dir)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        if args.db_dir is None:
            shutil.rmtree(tmp_root, ignore_errors=True)

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
