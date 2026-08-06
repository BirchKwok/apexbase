"""Fast ApexBase-only performance canary for commit-to-commit comparisons."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench_vs_sqlite_duckdb as full_bench


CANARY_SPECS = (
    ("Bulk Insert", "bench_insert", "once"),
    ("COUNT(*)", "bench_count", "mean"),
    ("SELECT * LIMIT 100", "bench_select_limit", "median"),
    ("Projection full scan (3 cols)", "bench_projected_full_scan", "mean"),
    ("String equality filter", "bench_filter_string", "mean"),
    ("Numeric range filter", "bench_filter_range", "mean"),
    ("GROUP BY city", "bench_group_by", "mean"),
    ("ORDER BY score LIMIT 100", "bench_order_limit", "mean"),
    ("Aggregation (5 funcs)", "bench_aggregation", "mean"),
    ("Point lookup (SQL)", "bench_point_lookup", "median"),
    ("Point lookup (direct)", "bench_oltp_direct_point_lookup", "median"),
    ("Projected string equality", "bench_oltp_projected_string_eq", "median"),
    ("Insert 1 row", "bench_oltp_insert_one", "median"),
    ("UPDATE by ID", "bench_oltp_update_by_id", "median"),
    ("Batch UPDATE by ID (10K)", "bench_oltp_batch_update_by_id", "once"),
    ("DELETE 1K", "bench_delete_1k_only", "setup", "bench_delete_1k_setup"),
    ("Table CREATE+DROP cycle", "bench_table_create_drop_cycle", "once"),
    ("List tables (10)", "bench_list_tables", "setup", "bench_list_tables_setup"),
    ("TopK JOIN with unused BLOB", "bench_topk_join_canary", "median"),
)


def _run_metric(bench, method_name, mode, warmup, iterations, setup_method=None):
    method = getattr(bench, method_name)
    if mode == "once":
        started = time.perf_counter()
        method()
        return (time.perf_counter() - started) * 1000.0
    if mode == "median":
        return full_bench.run_bench_nogc_median(method, warmup, iterations)
    if mode == "setup":
        return full_bench.run_bench_with_setup(
            getattr(bench, setup_method),
            method,
            warmup,
            iterations,
        )
    return full_bench.run_bench(method, warmup, iterations)


def run_canary(rows, warmup, iterations):
    full_bench.ensure_optional_imports()
    if not full_bench.HAS_APEXBASE:
        raise RuntimeError("ApexBase is not importable; run maturin develop --release first")

    data = full_bench.generate_data(rows)
    tmpdir = tempfile.mkdtemp(prefix="apexbase_canary_")
    bench = full_bench.ApexBaseBench(tmpdir, data)
    bench.shared_inputs = full_bench.build_shared_inputs(rows)
    results = []
    try:
        bench.setup()
        for spec in CANARY_SPECS:
            name, method_name, mode = spec[0], spec[1], spec[2]
            setup_method = spec[3] if len(spec) > 3 else None
            if method_name == "bench_topk_join_canary":
                bench.setup_topk_join_canary()
            elapsed_ms = _run_metric(
                bench, method_name, mode, warmup, iterations, setup_method
            )
            results.append({
                "category": "ApexBase canary",
                "query": name,
                "ApexBase": round(elapsed_ms, 6),
            })
            print(f"{name:<34} {elapsed_ms:>12.6f} ms")
        return results
    finally:
        bench.close()
        shutil.rmtree(tmpdir, ignore_errors=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=7)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.rows <= 0 or args.warmup < 0 or args.iterations <= 0:
        parser.error("rows and iterations must be positive; warmup must be non-negative")

    results = run_canary(args.rows, args.warmup, args.iterations)
    report = {
        **full_bench.get_report_metadata("apexbase-canary"),
        "config": {
            "rows": args.rows,
            "warmup": args.warmup,
            "iterations": args.iterations,
        },
        "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
