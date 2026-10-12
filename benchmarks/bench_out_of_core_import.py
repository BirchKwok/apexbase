#!/usr/bin/env python3
"""Compare out-of-core file analysis and materialization with DuckDB.

Both engines run in isolated processes against the same generated CSV or
Parquet file. The benchmark separates direct file analysis, disk-backed table
materialization, repeated native-table queries, peak RSS, and storage size.

Increase --rows until the source is several times larger than available memory
for a true out-of-core run. DuckDB receives an explicit memory limit and spill
directory; ApexBase imports in fixed-size RecordBatch chunks.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
try:
    import resource
except ImportError:  # Windows has no `resource` module.
    resource = None
import shutil
import statistics
import subprocess
import sys
import tempfile
import time


GEN_BATCH_ROWS = 65_536
DEFAULT_QUERY_RUNS = 7
DEFAULT_WARMUP_RUNS = 2


def current_rss_mb() -> float:
    if sys.platform == "darwin":
        output = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(os.getpid())], text=True
        )
        return int(output.strip()) / 1024.0
    # `/proc` exists on Linux only; on Windows there is no equivalent file to
    # read without a dependency, so report an unknown increment instead of
    # failing the run (peak RSS still comes from `resource`).
    status_path = Path(f"/proc/{os.getpid()}/status")
    if not status_path.exists():
        return 0.0
    with status_path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    return 0.0


def peak_rss_mb() -> float:
    """Peak resident set of this process, in MB.

    Windows has no `resource` module, so this reports the working-set high-water
    mark through `GetProcessMemoryInfo` there instead of failing the import.
    """
    if resource is not None:
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return value / (1024.0 * 1024.0) if sys.platform == "darwin" else value / 1024.0

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
    return counters.PeakWorkingSetSize / (1024.0 * 1024.0)


def path_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def time_query(call, warmups: int, runs: int) -> dict[str, float]:
    for _ in range(warmups):
        call()
    samples = []
    for _ in range(runs):
        started = time.perf_counter()
        call()
        samples.append((time.perf_counter() - started) * 1000.0)
    return {
        "median_ms": statistics.median(samples),
        "min_ms": min(samples),
        "mean_ms": statistics.fmean(samples),
    }


def generate_csv(path: Path, rows: int) -> None:
    with path.open("w", buffering=1024 * 1024, encoding="utf-8") as handle:
        handle.write("value,score,category,name\n")
        for start in range(0, rows, GEN_BATCH_ROWS):
            end = min(start + GEN_BATCH_ROWS, rows)
            handle.writelines(
                f"{row},{row % 1000 / 10.0},group_{row % 100},user_{row % 10000}\n"
                for row in range(start, end)
            )


def generate_parquet(path: Path, rows: int) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            ("value", pa.int64()),
            ("score", pa.float64()),
            ("category", pa.string()),
            ("name", pa.string()),
        ]
    )
    with pq.ParquetWriter(path, schema, compression="snappy") as writer:
        for start in range(0, rows, GEN_BATCH_ROWS):
            end = min(start + GEN_BATCH_ROWS, rows)
            values = range(start, end)
            batch = pa.record_batch(
                [
                    pa.array(values, type=pa.int64()),
                    pa.array((row % 1000 / 10.0 for row in values), type=pa.float64()),
                    pa.array((f"group_{row % 100}" for row in values)),
                    pa.array((f"user_{row % 10000}" for row in values)),
                ],
                schema=schema,
            )
            writer.write_batch(batch, row_group_size=GEN_BATCH_ROWS)


def limit_budget_mb(text: str, divisor: int) -> int:
    """Turn a DuckDB-style limit ("1GB", "256MB", "512m", bare MB) into an engine budget.

    ApexBase budgets are megabytes; the process cap is split so the query state and the
    whole-column scan buffers together stay well inside it.
    """
    raw = text.strip().lower()
    scale = 1
    for suffix, factor in (("gb", 1024), ("mb", 1), ("g", 1024), ("m", 1)):
        if raw.endswith(suffix):
            raw = raw[: -len(suffix)]
            scale = factor
            break
    value = float(raw) * scale
    return max(1, int(value // divisor))


def apex_worker(
    source: Path,
    database: Path,
    rows: int,
    source_format: str,
    warmups: int,
    runs: int,
    memory_limit: str = "1GB",
) -> dict:
    from apexbase import ApexClient

    # The benchmark only proves out-of-core behaviour if the engine is actually bounded:
    # the per-query aggregation budget and the whole-column scan buffers are split out of
    # the same process cap DuckDB runs under.
    engine_query_mb = limit_budget_mb(memory_limit, 4)
    engine_scan_mb = limit_budget_mb(memory_limit, 8)
    os.environ["APEX_QUERY_MEMORY_MB"] = str(engine_query_mb)
    os.environ["APEX_SCAN_BUFFER_MB"] = str(engine_scan_mb)

    before_rss = current_rss_mb()
    client = ApexClient(str(database), drop_if_exists=True)
    client.create_table("_dummy")
    client.use_table("_dummy")
    function = "read_csv" if source_format == "csv" else "read_parquet"
    direct_sql = (
        f"SELECT COUNT(*) FROM {function}('{sql_path(source)}') WHERE score >= 50"
    )

    started = time.perf_counter()
    direct_filtered_rows = client.execute(direct_sql).scalar()
    direct_seconds = time.perf_counter() - started

    started = time.perf_counter()
    client.register_temp_table("imported", str(source))
    materialize_seconds = time.perf_counter() - started

    imported_rows = client.execute("SELECT COUNT(*) FROM imported").scalar()
    query_sql = (
        "SELECT category, COUNT(*) AS n, AVG(score) AS avg_score FROM imported "
        "WHERE score >= 50 GROUP BY category ORDER BY category"
    )

    def query_materialized():
        return client.execute(query_sql).to_dict()

    result = query_materialized()
    filtered_rows = sum(row["n"] for row in result)
    query_times = time_query(query_materialized, warmups, runs)
    # Capture before close: ApexDB removes its .apex_tmp directory on drop.
    storage_bytes = path_size_bytes(database)
    client.close()
    return {
        "engine": "apexbase",
        "rows": imported_rows,
        "filtered_rows": filtered_rows,
        "groups": len(result),
        "direct_file_query_seconds": direct_seconds,
        "materialize_seconds": materialize_seconds,
        "materialize_rows_per_second": rows / materialize_seconds,
        "materialized_query": query_times,
        "source_bytes": source.stat().st_size,
        "storage_bytes": storage_bytes,
        "rss_before_mb": before_rss,
        "peak_rss_mb": peak_rss_mb(),
        "incremental_peak_rss_mb": max(0.0, peak_rss_mb() - before_rss),
        "direct_filtered_rows": direct_filtered_rows,
        "engine_budgets": {
            "memory_limit": memory_limit,
            "query_memory_mb": engine_query_mb,
            "scan_buffer_mb": engine_scan_mb,
        },
    }


def duckdb_worker(
    source: Path,
    database: Path,
    rows: int,
    source_format: str,
    warmups: int,
    runs: int,
    memory_limit: str,
) -> dict:
    import duckdb

    before_rss = current_rss_mb()
    database.parent.mkdir(parents=True, exist_ok=True)
    spill_dir = database.parent / "duckdb_spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(database))
    conn.execute(f"SET memory_limit = '{memory_limit}'")
    conn.execute(f"SET temp_directory = '{sql_path(spill_dir)}'")
    source_expr = (
        f"read_csv_auto('{sql_path(source)}', header=true)"
        if source_format == "csv"
        else f"read_parquet('{sql_path(source)}')"
    )
    direct_sql = f"SELECT COUNT(*) FROM {source_expr} WHERE score >= 50"

    started = time.perf_counter()
    direct_filtered_rows = conn.execute(direct_sql).fetchone()[0]
    direct_seconds = time.perf_counter() - started

    started = time.perf_counter()
    conn.execute(f"CREATE TABLE imported AS SELECT * FROM {source_expr}")
    materialize_seconds = time.perf_counter() - started

    imported_rows = conn.execute("SELECT COUNT(*) FROM imported").fetchone()[0]
    query_sql = (
        "SELECT category, COUNT(*) AS n, AVG(score) AS avg_score FROM imported "
        "WHERE score >= 50 GROUP BY category ORDER BY category"
    )

    def query_materialized():
        return conn.execute(query_sql).fetchall()

    result = query_materialized()
    filtered_rows = sum(row[1] for row in result)
    query_times = time_query(query_materialized, warmups, runs)
    conn.execute("CHECKPOINT")
    conn.close()
    return {
        "engine": "duckdb",
        "rows": imported_rows,
        "filtered_rows": filtered_rows,
        "groups": len(result),
        "direct_file_query_seconds": direct_seconds,
        "materialize_seconds": materialize_seconds,
        "materialize_rows_per_second": rows / materialize_seconds,
        "materialized_query": query_times,
        "source_bytes": source.stat().st_size,
        "storage_bytes": path_size_bytes(database) + path_size_bytes(spill_dir),
        "rss_before_mb": before_rss,
        "peak_rss_mb": peak_rss_mb(),
        "incremental_peak_rss_mb": max(0.0, peak_rss_mb() - before_rss),
        "direct_filtered_rows": direct_filtered_rows,
        "memory_limit": memory_limit,
    }


def run_worker(args: argparse.Namespace) -> None:
    common = (
        args.source,
        args.database,
        args.rows,
        args.format,
        args.warmups,
        args.query_runs,
    )
    if args.engine == "apexbase":
        result = apex_worker(*common, args.memory_limit)
    else:
        result = duckdb_worker(*common, args.memory_limit)
    if result["rows"] != args.rows:
        raise RuntimeError(
            f"{args.engine} row count mismatch: expected {args.rows}, got {result['rows']}"
        )
    if result["filtered_rows"] != result["direct_filtered_rows"]:
        raise RuntimeError(
            f"{args.engine} direct/materialized filtered row counts disagree: "
            f"{result['direct_filtered_rows']} != {result['filtered_rows']}"
        )
    print(json.dumps(result))


def worker_command(
    args: argparse.Namespace, engine: str, source: Path, database: Path
) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--engine",
        engine,
        "--rows",
        str(args.rows),
        "--format",
        args.format,
        "--source",
        str(source),
        "--database",
        str(database),
        "--query-runs",
        str(args.query_runs),
        "--warmups",
        str(args.warmups),
        "--memory-limit",
        args.memory_limit,
    ]


def ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def comparison(results: dict[str, dict]) -> dict[str, float | None]:
    apex = results.get("apexbase")
    duck = results.get("duckdb")
    if not apex or not duck:
        return {}
    return {
        "direct_file_query_time_apex_over_duckdb": ratio(
            apex["direct_file_query_seconds"], duck["direct_file_query_seconds"]
        ),
        "materialize_time_apex_over_duckdb": ratio(
            apex["materialize_seconds"], duck["materialize_seconds"]
        ),
        "materialized_query_time_apex_over_duckdb": ratio(
            apex["materialized_query"]["median_ms"],
            duck["materialized_query"]["median_ms"],
        ),
        "incremental_peak_rss_apex_over_duckdb": ratio(
            apex["incremental_peak_rss_mb"], duck["incremental_peak_rss_mb"]
        ),
        "storage_size_apex_over_duckdb": ratio(
            apex["storage_bytes"], duck["storage_bytes"]
        ),
    }


def print_summary(report: dict) -> None:
    print(
        f"\nOut-of-core comparison: {report['format'].upper()}, "
        f"{report['rows']:,} rows, {report['source_bytes'] / 1024**2:.1f} MiB source"
    )
    print(
        "Engine     Direct scan   Materialize   Native query   Peak RSS+   Storage"
    )
    print("-" * 78)
    for engine in report["engines"]:
        item = report["engines"][engine]
        print(
            f"{engine:<10} "
            f"{item['direct_file_query_seconds'] * 1000:>8.2f} ms  "
            f"{item['materialize_seconds']:>9.3f} s  "
            f"{item['materialized_query']['median_ms']:>9.3f} ms  "
            f"{item['incremental_peak_rss_mb']:>8.1f} MB  "
            f"{item['storage_bytes'] / 1024**2:>7.1f} MiB"
        )
    if report["comparison"]:
        print("\nApexBase / DuckDB ratios (lower is better):")
        for name, value in report["comparison"].items():
            print(f"  {name}: {value:.3f}x")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--format", choices=("csv", "parquet"), default="csv")
    parser.add_argument(
        "--engines", nargs="+", choices=("apexbase", "duckdb"), default=("apexbase", "duckdb")
    )
    parser.add_argument("--memory-limit", default="1GB")
    parser.add_argument("--query-runs", type=int, default=DEFAULT_QUERY_RUNS)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUP_RUNS)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--json-only", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--engine", choices=("apexbase", "duckdb"), help=argparse.SUPPRESS)
    parser.add_argument("--source", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--database", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        run_worker(args)
        return
    if args.rows <= 0 or args.query_runs <= 0 or args.warmups < 0:
        parser.error("rows/query-runs must be positive and warmups cannot be negative")

    root = args.work_dir or Path(tempfile.mkdtemp(prefix="apex_out_of_core_"))
    created_root = args.work_dir is None
    root.mkdir(parents=True, exist_ok=True)
    source = root / f"source.{args.format}"
    try:
        started = time.perf_counter()
        if args.format == "csv":
            generate_csv(source, args.rows)
        else:
            generate_parquet(source, args.rows)
        generation_seconds = time.perf_counter() - started

        results = {}
        for engine in dict.fromkeys(args.engines):
            database = root / ("apexdb" if engine == "apexbase" else "duckdb.db")
            completed = subprocess.run(
                worker_command(args, engine, source, database),
                check=True,
                capture_output=True,
                text=True,
            )
            results[engine] = json.loads(completed.stdout.strip().splitlines()[-1])

        if len(results) == 2:
            apex = results["apexbase"]
            duck = results["duckdb"]
            for field in ("rows", "filtered_rows", "groups"):
                if apex[field] != duck[field]:
                    raise RuntimeError(
                        f"cross-engine {field} mismatch: {apex[field]} != {duck[field]}"
                    )

        report = {
            "format": args.format,
            "rows": args.rows,
            "source_bytes": source.stat().st_size,
            "generation_seconds": generation_seconds,
            "query_runs": args.query_runs,
            "warmups": args.warmups,
            "duckdb_memory_limit": args.memory_limit,
            "apexbase_engine_budgets": results["apexbase"].get("engine_budgets"),
            "engines": results,
            "comparison": comparison(results),
        }
        if not args.json_only:
            print_summary(report)
        print(json.dumps(report, indent=2))
    finally:
        if created_root:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
