"""Fast ApexBase-only performance canary for commit-to-commit comparisons."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench_vs_sqlite_duckdb as full_bench
from apexbase.client import ApexClient


CANARY_SPECS = (
    ("Bulk Insert", "bench_insert", "once"),
    ("COUNT(*)", "bench_count", "mean"),
    ("SELECT * LIMIT 100", "bench_select_limit", "median"),
    ("Projection full scan (3 cols)", "bench_projected_full_scan", "mean"),
    ("String equality filter", "bench_filter_string", "mean"),
    ("Numeric range filter", "bench_filter_range", "mean"),
    ("Numeric equality aggregation", "bench_numeric_equality_aggregation", "mean"),
    ("Numeric conjunction aggregation", "bench_numeric_conjunction_aggregation", "mean"),
    ("Prefix LIKE aggregation", "bench_prefix_like_aggregation", "mean"),
    ("GROUP BY city", "bench_group_by", "mean"),
    ("ORDER BY score LIMIT 100", "bench_order_limit", "mean"),
    ("IS NOT NULL numeric TopK", "bench_not_null_numeric_topk", "mean"),
    ("Filtered numeric TopK", "bench_filtered_numeric_topk", "mean"),
    ("Aggregation (5 funcs)", "bench_aggregation", "mean"),
    ("Numeric GROUP BY (5 funcs)", "bench_numeric_group_aggregation", "mean"),
    ("Two-key GROUP BY (5 funcs)", "bench_two_key_group_aggregation", "mean"),
    ("Cached analytical CTE", "bench_cached_analytical_cte", "mean"),
    ("Numeric MOD GROUP BY", "bench_numeric_mod_group", "mean"),
    ("High-card SUBSTR GROUP BY", "bench_high_card_substr_group", "mean"),
    ("Derived CASE bucket GROUP BY", "bench_derived_case_bucket_group", "mean"),
    ("Derived ratio GROUP BY", "bench_derived_ratio_group", "mean"),
    ("Mixed cached DISTINCT", "bench_mixed_cached_distinct", "mean"),
    ("Multiple COUNT DISTINCT", "bench_multiple_count_distinct", "mean"),
    ("Dimension JOIN aggregation", "bench_dimension_join_aggregation", "mean"),
    ("Conditional aggregation (2 CASE)", "bench_conditional_aggregation", "mean"),
    ("NULL profile (2 cols)", "bench_null_profile", "mean"),
    ("CSV scalar MAX (direct file)", "bench_csv_scalar_aggregation", "mean"),
    ("CSV filtered scalar aggregation", "bench_csv_filtered_scalar_aggregation", "mean"),
    ("CSV string GROUP BY numeric agg", "bench_csv_string_group_numeric_aggregation", "mean"),
    ("CSV filtered GROUP BY + HAVING", "bench_csv_filtered_group_numeric_aggregation", "mean"),
    ("CSV integer GROUP BY numeric agg", "bench_csv_integer_group_numeric_aggregation", "mean"),
    ("CSV multiple COUNT DISTINCT", "bench_csv_multi_count_distinct", "mean"),
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

QUANTIZED_CODECS = (
    "float16",
    "bfloat16",
    "int8",
    "uint8",
    "bit1",
    "turboquant2",
    "turboquant3",
    "turboquant4",
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


def run_quantized_canary(rows, warmup, iterations):
    """Measure compressed-domain L2 TopK without including column construction."""
    quant_rows = rows
    dim = 128
    rng = np.random.default_rng(20260822)
    vectors = rng.normal(size=(quant_rows, dim)).astype(np.float32)
    queries = vectors[:5] + rng.normal(scale=0.02, size=(5, dim)).astype(np.float32)
    with tempfile.TemporaryDirectory(prefix="apexbase_quant_canary_") as tmpdir:
        client = ApexClient(tmpdir, drop_if_exists=True)
        try:
            client.create_table("vectors", {"embedding": "float32_vector"})
            client.store([{"embedding": vector} for vector in vectors])
            targets = {
                codec: client.create_quantized_column("embedding", codec=codec)
                for codec in QUANTIZED_CODECS
            }
            results = []
            for codec, target in targets.items():
                def scan():
                    client.batch_topk_distance(target, queries, k=10)

                elapsed_ms = full_bench.run_bench_nogc_median(
                    scan, warmup, iterations
                ) / len(queries)
                results.append({
                    "category": "ApexBase quantized vector",
                    "query": f"Batch quantized TopK L2 ({codec})",
                    "ApexBase": round(elapsed_ms, 6),
                })
                print(
                    f"Batch quantized TopK L2 ({codec})".ljust(40)
                    + f" {elapsed_ms:>12.6f} ms/query"
                )
            return results
        finally:
            client.close()


def run_canary(rows, warmup, iterations, qps_only=False):
    full_bench.ensure_optional_imports()
    if not full_bench.HAS_APEXBASE:
        raise RuntimeError("ApexBase is not importable; run maturin develop --release first")

    data = full_bench.generate_data(rows)
    tmpdir = tempfile.mkdtemp(prefix="apexbase_canary_")
    csv_path = Path(tmpdir) / "canary_data.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = full_bench.csv_mod.writer(csv_file)
        writer.writerow(["name", "age", "score", "city", "category"])
        for index in range(rows):
            writer.writerow([
                data["name"][index],
                data["age"][index],
                data["score"][index],
                data["city"][index],
                data["category"][index],
            ])
    bench = full_bench.ApexBaseBench(tmpdir, data, csv_path=str(csv_path))
    bench.shared_inputs = full_bench.build_shared_inputs(rows)
    results = []
    try:
        bench.setup()
        if not qps_only:
            for spec in CANARY_SPECS:
                name, method_name, mode = spec[0], spec[1], spec[2]
                setup_method = spec[3] if len(spec) > 3 else None
                if method_name == "bench_topk_join_canary":
                    bench.setup_topk_join_canary()
                elif method_name == "bench_dimension_join_aggregation":
                    bench.setup_dimension_join_aggregation()
                elapsed_ms = _run_metric(
                    bench, method_name, mode, warmup, iterations, setup_method
                )
                results.append({
                    "category": "ApexBase canary",
                    "query": name,
                    "ApexBase": round(elapsed_ms, 6),
                })
                print(f"{name:<34} {elapsed_ms:>12.6f} ms")
            results.extend(run_quantized_canary(rows, warmup, iterations))

        # OLAP Q/s read profile (ApexBase-only). The harness recreates the
        # engine on a clean loaded copy, so the measurement is independent of
        # the delta-heavy state left by the DML canary metrics.
        qps = full_bench.run_qps_benchmark(
            tmpdir,
            data,
            n_threads=4,
            min_duration=1.0,
            min_iterations=50,
            existing_engines={"ApexBase": bench},
        )
        for label, key in (
            ("Q/s (single thread)", "ApexBase_single"),
            ("Q/s (4 threads)", "ApexBase_concurrent_4"),
        ):
            results.append({
                "category": "ApexBase Q/s",
                "query": label,
                "ApexBase": round(qps.get(key, 0.0), 3),
            })
            print(f"{label:<34} {qps.get(key, 0.0):>12.3f} Q/s")
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
    parser.add_argument(
        "--qps-only",
        action="store_true",
        help="Run only the OLAP Q/s read profile instead of the full canary",
    )
    parser.add_argument(
        "--quant-only",
        action="store_true",
        help="Run only compressed-domain vector TopK metrics",
    )
    args = parser.parse_args(argv)
    if args.rows <= 0 or args.warmup < 0 or args.iterations <= 0:
        parser.error("rows and iterations must be positive; warmup must be non-negative")

    if args.qps_only and args.quant_only:
        parser.error("--qps-only and --quant-only are mutually exclusive")
    if args.quant_only:
        results = run_quantized_canary(args.rows, args.warmup, args.iterations)
    else:
        results = run_canary(
            args.rows, args.warmup, args.iterations, qps_only=args.qps_only
        )
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
