"""B-phase entry evidence for morsel parallelism (architecture review R5.9).

Two measurement modes over the batched GROUP BY shape (the delta-free
__perf_batch_scan copy of the base table, the R3 batch-pipeline shape):

1. `matrix` - concurrency contention matrix: N concurrent queries
   (N = 1/2/4/8 by default) x APEX_PARALLEL_SCAN off/on, recording total
   throughput and p50/p99/p99.9 latency per window. Feeds the phase B
   entry threshold of section 14.8.3-2: "parallel on + global in-flight
   budget" must not lower total throughput at >=2 concurrency and must
   not amplify the p99 tail by 2x or more.
2. `curve` - end-to-end speedup curve: 1 (serial) / 2 / 3 / 4 / 8
   requested threads on the same shape; records median latency and
   speedup vs serial and reports the effective worker count parsed from
   the EXPLAIN ANALYZE path detail
   (batched_scan_pipeline(batches=N, parallel=T)), exposing the
   in-flight worker budget cap (section 14.8.2).

Both modes write JSON reports under local-perf-results/ and record the
machine load (os.getloadavg) with every window, so noisy windows are
identified instead of silently dropped. No pass/fail verdict is made
here: the gates compare base/current, these measurements are the phase
B entry evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench_vs_sqlite_duckdb as full_bench
from apexbase.client import ApexClient

BATCH_SHAPE = (
    "SELECT city, COUNT(*) AS n, AVG(score) AS av, MAX(score) AS mx "
    "FROM __perf_batch_scan WHERE age > ? AND age <= ? AND score >= ? "
    "GROUP BY city HAVING COUNT(*) > ? "
    "ORDER BY n DESC, city LIMIT 5"
)
PARAMETER_SETS = (
    (20, 35, 20.0, 0),
    (25, 40, 30.0, 100),
    (30, 50, 40.0, 250),
    (35, 60, 50.0, 500),
)
ENV_PARALLEL = "APEX_PARALLEL_SCAN"
COLUMNS = ("name", "age", "score", "city", "category")


def fail(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    sys.exit(2)


def parse_int_list(value: str) -> list[int]:
    try:
        items = [int(x) for x in value.split(",") if x.strip()]
    except ValueError:
        fail(f"invalid int list: {value!r}")
    if not items or any(x < 1 for x in items):
        fail(f"int list entries must be >= 1: {value!r}")
    return items


def parse_parallel_list(value: str) -> list[str]:
    items = [x.strip() for x in value.split(",") if x.strip()]
    for item in items:
        if item != "off" and (not item.isdigit() or int(item) < 2):
            fail(f"parallel value must be 'off' or an int >= 2: {item!r}")
    if not items:
        fail("empty parallel list")
    return items


def set_parallel(parallel: str) -> None:
    if parallel == "off":
        os.environ.pop(ENV_PARALLEL, None)
    else:
        os.environ[ENV_PARALLEL] = parallel


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, max(0, round(pct / 100.0 * (len(sorted_values) - 1))))
    return sorted_values[index]


def latency_stats(latencies: list[float]) -> dict:
    ordered = sorted(latencies)
    return {
        "p50_ms": round(percentile(ordered, 50), 6),
        "p99_ms": round(percentile(ordered, 99), 6),
        "p999_ms": round(percentile(ordered, 99.9), 6),
        "max_ms": round(ordered[-1], 6),
        "mean_ms": round(statistics.fmean(latencies), 6),
    }


def ensure_dataset(db_dir: Path, rows: int) -> None:
    """Ensure default.apex exists (generate it if missing) plus the
    delta-free __perf_batch_scan copy the batch shape queries."""
    base = db_dir / "default.apex"
    if not base.exists():
        if rows <= 0:
            fail(f"dataset {base} missing; pass --rows to generate it")
        client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)
        try:
            client.create_table("default", {c: t for c, t in
                (("name", "string"), ("age", "int"), ("score", "float"),
                 ("city", "string"), ("category", "string"))})
            client.use_table("default")
            data = full_bench.generate_data(rows)
            chunk = 50_000
            for start in range(0, rows, chunk):
                end = min(start + chunk, rows)
                client.store({c: data[c][start:end] for c in COLUMNS})
            client.flush()
        finally:
            client.close()
    target = db_dir / "__perf_batch_scan.apex"
    if not target.exists():
        client = ApexClient(str(db_dir), drop_if_exists=False, enable_cache=False)
        try:
            client.create_table("__perf_batch_scan")
            shutil.copy2(base, target)
        finally:
            client.close()


def open_shape_client(db_dir: Path):
    client = full_bench.open_apex_benchmark_client(str(db_dir), drop_if_exists=False)
    client.use_table("__perf_batch_scan")
    return client


def execute_shape(client, counter: int):
    params = PARAMETER_SETS[counter % len(PARAMETER_SETS)]
    return client.execute(BATCH_SHAPE, params=params, show_internal_id=True).to_dict(), counter + 1


def warmup(client, concurrency: int, queries: int) -> None:
    if queries <= 0:
        return

    def worker(worker_id: int, n: int) -> None:
        counter = worker_id
        for _ in range(n):
            _, counter = execute_shape(client, counter)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(worker, range(concurrency), [queries // concurrency] * concurrency))


def timed_window(client, concurrency: int, total_queries: int) -> tuple[float, list[float]]:
    if total_queries < concurrency:
        fail("queries-per-window must be >= concurrency")
    shares = [total_queries // concurrency + (1 if i < total_queries % concurrency else 0)
              for i in range(concurrency)]

    def worker(args) -> list[float]:
        worker_id, n = args
        counter = worker_id
        latencies = []
        for _ in range(n):
            t0 = time.perf_counter()
            _, counter = execute_shape(client, counter)
            latencies.append((time.perf_counter() - t0) * 1000.0)
        return latencies

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        parts = list(pool.map(worker, list(enumerate(shares))))
    wall = time.perf_counter() - t0
    return wall, [x for part in parts for x in part]


def load_sample() -> list[float]:
    if not hasattr(os, "getloadavg"):
        return []
    try:
        return [round(x, 3) for x in os.getloadavg()]
    except OSError:
        return []


def default_output_dir(tag: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return Path("local-perf-results") / f"{stamp}-{tag}"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def dataset_rows(client) -> int:
    return client.execute(
        "SELECT COUNT(*) AS c FROM __perf_batch_scan", show_internal_id=True
    ).scalar()


def cmd_matrix(args) -> int:
    concurrencies = parse_int_list(args.concurrency)
    parallels = parse_parallel_list(args.parallel)
    out = Path(args.output) if args.output else default_output_dir("contention")
    db_dir = Path(args.db_dir)
    ensure_dataset(db_dir, args.rows)
    client = open_shape_client(db_dir)
    try:
        rows = dataset_rows(client)
        records = []
        for window in range(1, args.windows + 1):
            load = load_sample()
            for concurrency in concurrencies:
                for parallel in parallels:
                    set_parallel(parallel)
                    warmup(client, concurrency, args.warmup_queries)
                    wall, latencies = timed_window(client, concurrency, args.queries)
                    record = {
                        "mode": "matrix",
                        "window": window,
                        "concurrency": concurrency,
                        "parallel": parallel,
                        "rows": rows,
                        "total_queries": len(latencies),
                        "wall_s": round(wall, 6),
                        "qps": round(len(latencies) / wall, 3) if wall > 0 else 0.0,
                        **latency_stats(latencies),
                        "loadavg": load,
                        "nproc": os.cpu_count(),
                        "git": full_bench.get_git_info(),
                    }
                    write_json(out / f"matrix-c{concurrency}-p{parallel}-w{window}.json", record)
                    records.append(record)
                    print(
                        f"[matrix] w{window} c={concurrency} p={parallel:<3} "
                        f"{record['qps']:>9.1f} Q/s  p99={record['p99_ms']:>9.3f} ms  "
                        f"load={load[0] if load else 'n/a'}"
                    )
        summary = {
            "config": {
                "db_dir": str(db_dir),
                "rows": rows,
                "concurrency": concurrencies,
                "parallel": parallels,
                "windows": args.windows,
                "queries_per_window": args.queries,
                "warmup_queries": args.warmup_queries,
                "shape": BATCH_SHAPE,
            },
            "records": records,
            "derived": derived_matrix(records, concurrencies, parallels),
        }
        write_json(out / "matrix-summary.json", summary)
        print_matrix_summary(summary)
        return 0
    finally:
        client.close()


def derived_matrix(records, concurrencies, parallels) -> list[dict]:
    """Per (concurrency, parallel!=off): throughput delta vs parallel=off
    and p99 amplification vs parallel=off, per window and across windows
    (median of window medians)."""
    derived = []
    for concurrency in concurrencies:
        for parallel in parallels:
            if parallel == "off":
                continue
            rows_ = [
                (r["concurrency"], r["parallel"], r["window"], r["qps"], r["p99_ms"])
                for r in records
                if r["concurrency"] == concurrency and r["parallel"] == parallel
            ]
            offs = [
                r for r in records
                if r["concurrency"] == concurrency and r["parallel"] == "off"
            ]
            per_window = []
            for _, _, window, qps, p99 in rows_:
                off = next((o for o in offs if o["window"] == window), None)
                if off is None:
                    continue
                per_window.append({
                    "window": window,
                    "throughput_delta_pct": round((qps / off["qps"] - 1) * 100.0, 3),
                    "p99_amplification": round(p99 / off["p99_ms"], 4) if off["p99_ms"] > 0 else None,
                })
            derived.append({
                "concurrency": concurrency,
                "parallel": parallel,
                "per_window": per_window,
                "throughput_delta_pct": _median(
                    [w["throughput_delta_pct"] for w in per_window]
                ),
                "p99_amplification": _median(
                    [w["p99_amplification"] for w in per_window if w["p99_amplification"]]
                ),
            })
    return derived


def _median(values) -> float | None:
    values = [v for v in values if v is not None]
    return round(statistics.median(values), 4) if values else None


def print_matrix_summary(summary) -> None:
    print("\n=== Contention matrix (median across windows) ===")
    print(f"{'C':>3} {'parallel':>9} {'Q/s vs off':>12} {'p99 x':>8}")
    for d in summary["derived"]:
        print(
            f"{d['concurrency']:>3} {d['parallel']:>9} "
            f"{d['throughput_delta_pct'] if d['throughput_delta_pct'] is not None else 'n/a':>12} "
            f"{d['p99_amplification'] if d['p99_amplification'] is not None else 'n/a':>8}"
        )
    print("Phase B threshold (14.8.3-2): at >=2 concurrency, parallel-on throughput")
    print("must stay >= serial-on and p99 amplification must stay below 2.0x.")


def effective_parallel_threads(client) -> int | None:
    """Parse the EXPLAIN ANALYZE path detail of the batch shape. The probe
    inherits the env the caller just set for its thread level."""
    plan = client.execute(
        "EXPLAIN ANALYZE " + BATCH_SHAPE, params=PARAMETER_SETS[0], show_internal_id=True
    ).to_dict()[0]["plan"]
    for line in plan.splitlines():
        if "Actual Path: batched_scan_pipeline" in line:
            marker = "parallel="
            if marker in line:
                return int(line.split(marker, 1)[1].split(")")[0])
            return 1
    return None

def cmd_curve(args) -> int:
    threads = parse_int_list(args.threads)
    if 1 not in threads:
        fail("the thread list must include 1 (serial baseline)")
    out = Path(args.output) if args.output else default_output_dir("speed-curve")
    db_dir = Path(args.db_dir)
    ensure_dataset(db_dir, args.rows)
    client = open_shape_client(db_dir)
    try:
        rows = dataset_rows(client)
        records = []
        for level in threads:
            parallel = "off" if level == 1 else str(level)
            for window in range(1, args.windows + 1):
                set_parallel(parallel)
                warmup(client, 1, args.warmup_queries)
                t0 = time.perf_counter()
                counter = window
                latencies = []
                for _ in range(args.queries):
                    q0 = time.perf_counter()
                    _, counter = execute_shape(client, counter)
                    latencies.append((time.perf_counter() - q0) * 1000.0)
                wall = time.perf_counter() - t0
                record = {
                    "mode": "curve",
                    "threads_requested": level,
                    "parallel": parallel,
                    "window": window,
                    "rows": rows,
                    "total_queries": len(latencies),
                    "wall_s": round(wall, 6),
                    **latency_stats(latencies),
                    "loadavg": load_sample(),
                    "nproc": os.cpu_count(),
                    "git": full_bench.get_git_info(),
                }
                write_json(out / f"curve-t{level}-w{window}.json", record)
                records.append(record)
                print(
                    f"[curve] t={level} w{window}: median={record['p50_ms']:.3f} ms "
                    f"(n={len(latencies)}) load={record['loadavg'][0] if record['loadavg'] else 'n/a'}"
                )
        medians = {}
        for level in threads:
            values = [r["p50_ms"] for r in records if r["threads_requested"] == level]
            medians[level] = statistics.median(values)
        summary = {
            "config": {
                "db_dir": str(db_dir),
                "rows": rows,
                "threads": threads,
                "windows": args.windows,
                "queries_per_window": args.queries,
                "warmup_queries": args.warmup_queries,
                "shape": BATCH_SHAPE,
            },
            "records": records,
            "per_thread_median_ms": {str(k): round(v, 6) for k, v in medians.items()},
            "speedup_vs_serial": {
                str(k): round(medians[1] / v, 4) for k, v in medians.items()
            },
            "effective_threads": {},
        }
        for level in threads:
            set_parallel("off" if level == 1 else str(level))
            summary["effective_threads"][str(level)] = effective_parallel_threads(client)
        write_json(out / "curve-summary.json", summary)
        print_curve_summary(summary)
        return 0
    finally:
        client.close()


def print_curve_summary(summary) -> None:
    print("\n=== Speedup curve (median of window medians, end-to-end) ===")
    print(f"{'requested':>10} {'effective':>10} {'median ms':>12} {'speedup':>9}")
    for level in summary["config"]["threads"]:
        key = str(level)
        print(
            f"{level:>10} "
            f"{summary['effective_threads'][key] if summary['effective_threads'][key] is not None else 'n/a':>10} "
            f"{summary['per_thread_median_ms'][key]:>12.3f} "
            f"{summary['speedup_vs_serial'][key]:>9.3f}"
        )
    print("The effective column exposes the in-flight worker budget cap; a flat")
    print("tail is budget saturation, not measurement error.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    matrix = sub.add_parser("matrix", help="concurrency contention matrix")
    matrix.add_argument("--db-dir", required=True, help="dataset dir containing default.apex")
    matrix.add_argument("--rows", type=int, default=0,
                        help="generate the dataset when default.apex is missing")
    matrix.add_argument("--concurrency", default="1,2,4,8")
    matrix.add_argument("--parallel", default="off,2,4,8")
    matrix.add_argument("--windows", type=int, default=3)
    matrix.add_argument("--queries", type=int, default=2000,
                        help="total timed queries per window (split across workers)")
    matrix.add_argument("--warmup-queries", type=int, default=200)
    matrix.add_argument("--output", default=None)
    matrix.set_defaults(func=cmd_matrix)

    curve = sub.add_parser("curve", help="end-to-end speedup curve")
    curve.add_argument("--db-dir", required=True, help="dataset dir containing default.apex")
    curve.add_argument("--rows", type=int, default=0,
                       help="generate the dataset when default.apex is missing")
    curve.add_argument("--threads", default="1,2,3,4,8")
    curve.add_argument("--windows", type=int, default=3)
    curve.add_argument("--queries", type=int, default=300, help="timed queries per window")
    curve.add_argument("--warmup-queries", type=int, default=50)
    curve.add_argument("--output", default=None)
    curve.set_defaults(func=cmd_curve)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.windows < 1 or args.queries < 1:
        fail("--windows and --queries must be >= 1")
    full_bench.ensure_optional_imports()
    if not full_bench.HAS_APEXBASE:
        fail("ApexBase is not importable; run maturin develop --release first")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
