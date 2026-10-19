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
    ("COUNT(col) covered", "bench_count_column_covered", "mean"),
    ("COUNT(col) partial", "bench_count_column_partial", "mean"),
    ("COUNT(col) string filter", "bench_count_column_string_filter", "mean"),
    ("SELECT * LIMIT 100", "bench_select_limit", "median"),
    ("Projection full scan (3 cols)", "bench_projected_full_scan", "mean"),
    ("String equality filter", "bench_filter_string", "mean"),
    ("Numeric range filter", "bench_filter_range", "mean"),
    (
        "Bitpack partial range COUNT",
        "bench_bitpack_partial_range_count",
        "mean",
    ),
    ("Float partial range COUNT", "bench_float_range_count", "mean"),
    ("String cell UPDATE (single row)", "bench_string_cell_update", "mean"),
    ("DELETE by _id", "bench_delete_by_id", "mean"),
    ("DELETE bounded range", "bench_delete_bounded_range", "mean"),
    ("Numeric equality aggregation", "bench_numeric_equality_aggregation", "mean"),
    ("Numeric conjunction aggregation", "bench_numeric_conjunction_aggregation", "mean"),
    ("Prefix LIKE aggregation", "bench_prefix_like_aggregation", "mean"),
    ("GROUP BY city", "bench_group_by", "mean"),
    (
        "Uncached delta Filter+GROUP+HAVING+TopK",
        "bench_uncached_delta_scan_group_having_topk",
        "mean",
    ),
    (
        "Uncached delta Boolean+GROUP+HAVING+TopK",
        "bench_uncached_delta_boolean_scan_group_having_topk",
        "mean",
    ),
    (
        "File-table Filter+GROUP+HAVING+TopK (batch)",
        "bench_batch_scan_group_having_topk",
        "mean",
    ),
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
    ("Projected _id string equality", "bench_oltp_projected_id_string_eq", "median"),
    ("Insert 1 row", "bench_oltp_insert_one", "median"),
    ("UPDATE by ID", "bench_oltp_update_by_id", "median"),
    ("Batch UPDATE by ID (10K)", "bench_oltp_batch_update_by_id", "once"),
    ("DELETE 1K", "bench_delete_1k_only", "setup", "bench_delete_1k_setup"),
    ("Table CREATE+DROP cycle", "bench_table_create_drop_cycle", "once"),
    ("List tables (10)", "bench_list_tables", "setup", "bench_list_tables_setup"),
    ("TopK JOIN with unused BLOB", "bench_topk_join_canary", "median"),
    ("Index equality (rare)", "bench_index_eq_rare", "mean"),
    ("Index equality (skewed)", "bench_index_eq_skewed", "mean"),
    ("Index range (BETWEEN)", "bench_index_range", "mean"),
    ("Index covering projection", "bench_index_covering", "mean"),
    ("Parallel batch scan (2 threads)", "bench_parallel_batch_scan_t2", "mean"),
    ("Parallel batch scan (4 threads)", "bench_parallel_batch_scan_t4", "mean"),
    ("Parallel batch scan (auto)", "bench_parallel_batch_scan_auto", "mean"),
    (
        "Numeric range LIMIT (delta overlay)",
        "bench_delta_numeric_range_limit",
        "setup",
        "setup_delta_numeric_range_limit",
    ),
)

# Full-mode par phase (R5.7 phase A): the 8-thread shape exists only in the
# dedicated 1M-row phase; the 200K canary keeps 2/4 threads.
PARALLEL_ONLY_SPECS = (
    ("Parallel batch scan (2 threads)", "bench_parallel_batch_scan_t2", "mean"),
    ("Parallel batch scan (4 threads)", "bench_parallel_batch_scan_t4", "mean"),
    ("Parallel batch scan (8 threads)", "bench_parallel_batch_scan_t8", "mean"),
    ("Parallel batch scan (auto)", "bench_parallel_batch_scan_auto", "mean"),
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

# Read-scaling curve measured by the Q/s profile. A single 4-thread point
# cannot show whether read throughput still scales, so every level is measured
# in the same run and compared base/current.
QPS_THREAD_COUNTS = (1, 2, 4, 8)


def qps_metric_labels(thread_counts=QPS_THREAD_COUNTS):
    """(metric name, Q/s result key) pairs for one scaling-curve run."""
    labels = [("Q/s (single thread)", "ApexBase_single")]
    labels.extend(
        (f"Q/s ({threads} threads)", f"ApexBase_concurrent_{threads}")
        for threads in thread_counts
        if threads > 1
    )
    return labels


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


# Recall floors per codec: recall@10 of the codec's *own* top-k against an exact
# numpy L2 ground truth over the same corpus. Latency alone cannot catch a codec
# that silently returns bad neighbours, and the gate previously measured only
# latency. The bounds sit well below the measured values (float16/bfloat16 1.0,
# int8 0.96-0.975, uint8 0.98-1.0, bit1 0.12-0.20, turboquant2 0.44-0.48,
# turboquant3 0.56-0.595, turboquant4 0.76) so ordinary fixture variation cannot
# trip them, while a collapse to random neighbours fails the run.
QUANTIZED_RECALL_FLOORS = {
    "float16": 0.95,
    "bfloat16": 0.95,
    "int8": 0.85,
    "uint8": 0.85,
    "bit1": 0.05,
    "turboquant2": 0.30,
    "turboquant3": 0.42,
    "turboquant4": 0.62,
}

# Shortlist re-ranked exactly for the ground truth; wide enough that a float32
# distance computation cannot push the true top-10 out of it.
_RECALL_REFINE = 200


def exact_topk_ids(vectors, queries, k):
    """Exact L2 top-k row indices (0-based) for each query.

    Distances are computed in float32 (the corpus is 1M x 128 at gate scale, so a
    float64 copy would double the memory) and only the shortlist is re-ranked in
    float64, which removes float32 cancellation from the final order.
    """
    x = np.asarray(vectors, dtype=np.float32)
    q = np.asarray(queries, dtype=np.float32)
    distances = (x * x).sum(1)[None, :] - 2.0 * (q @ x.T) + (q * q).sum(1)[:, None]
    take = min(max(_RECALL_REFINE, k), x.shape[0])
    candidates = np.argpartition(distances, take - 1, axis=1)[:, :take]
    rows = np.arange(candidates.shape[0])[:, None]
    refined = (
        (x[candidates].astype(np.float64) - q[:, None, :].astype(np.float64)) ** 2
    ).sum(axis=2)
    order = np.lexsort((candidates, refined), axis=1)[:, :k]
    return candidates[rows, order]


def recall_at_k(exact_ids, returned_ids, k):
    """Mean recall@k; ids are 1-based on the wire, `exact_ids` are 0-based."""
    hits = 0
    for query_index in range(exact_ids.shape[0]):
        truth = set((exact_ids[query_index] + 1).tolist())
        got = set(returned_ids[query_index, :k].tolist())
        hits += len(truth & got) / float(k)
    return hits / exact_ids.shape[0]


def run_quantized_canary(rows, warmup, iterations):
    """Measure compressed-domain L2 TopK without including column construction.

    Returns the latency metric rows plus a recall detail block. Every codec's
    recall@10 is asserted against `QUANTIZED_RECALL_FLOORS`, so a codec that gets
    faster by returning wrong neighbours fails the benchmark.
    """
    quant_rows = rows
    dim = 128
    rng = np.random.default_rng(20260822)
    vectors = rng.normal(size=(quant_rows, dim)).astype(np.float32)
    queries = vectors[:5] + rng.normal(scale=0.02, size=(5, dim)).astype(np.float32)
    exact_ids = exact_topk_ids(vectors, queries, 10)
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
            recall = {}
            for codec, target in targets.items():
                def scan():
                    client.batch_topk_distance(target, queries, k=10)

                elapsed_ms = full_bench.run_bench_nogc_median(
                    scan, warmup, iterations
                ) / len(queries)
                found = client.batch_topk_distance(target, queries, k=10)
                measured = recall_at_k(exact_ids, found[:, :, 0].astype(np.int64), 10)
                floor = QUANTIZED_RECALL_FLOORS.get(codec)
                assert floor is None or measured >= floor, (
                    f"{codec} recall@10 {measured:.3f} fell below its floor {floor}: "
                    "the codec is returning different neighbours"
                )
                recall[codec] = round(measured, 6)
                results.append({
                    "category": "ApexBase quantized vector",
                    "query": f"Batch quantized TopK L2 ({codec})",
                    "ApexBase": round(elapsed_ms, 6),
                })
                print(
                    f"Batch quantized TopK L2 ({codec})".ljust(40)
                    + f" {elapsed_ms:>12.6f} ms/query"
                    + f"  recall@10={measured:.3f}"
                )
            return results, {
                "recall_at_10": recall,
                "floors": QUANTIZED_RECALL_FLOORS,
                "corpus_rows": quant_rows,
                "queries": len(queries),
            }
        finally:
            client.close()


def run_commit_canary(rows, warmup, iterations):
    """Exercise the Rust Safe commit protocol, bypassing Python fast transactions.

    Seed the same row count as the parent suite. Each sample inserts ten rows
    through WAL, data application, publication and the applied watermark.
    Setup and integrity checks are outside the timed region.
    """
    full_bench.ensure_optional_imports()
    with tempfile.TemporaryDirectory(prefix="apexbase_commit_canary_") as tmpdir:
        client = full_bench.open_apex_benchmark_client(
            tmpdir, _auto_manage=False, durability="safe"
        )
        try:
            client.create_table("commit_t", {"value": "int"})
            client.use_table("commit_t")
            client.store({"value": np.arange(rows, dtype=np.int64)})
            client.flush()
            values = ",".join(f"({i})" for i in range(10))

            def commit():
                client._storage.execute("BEGIN")
                client._storage.execute(f"INSERT INTO commit_t (value) VALUES {values}")
                return client._storage.execute("COMMIT")

            elapsed = full_bench.run_bench(commit, warmup, iterations)
            expected = rows + 10 * (warmup + iterations)
            assert client.execute("SELECT COUNT(*) FROM commit_t").scalar() == expected
            client.close()
            client = full_bench.open_apex_benchmark_client(
                tmpdir, _auto_manage=False, durability="safe"
            )
            client.use_table("commit_t")
            assert client.execute("SELECT COUNT(*) FROM commit_t").scalar() == expected
            name = "Rust Safe TXN INSERT 10 + COMMIT"
            print(f"{name:<34} {elapsed:>12.6f} ms")
            return [{"category": "ApexBase commit", "query": name,
                     "ApexBase": round(elapsed, 6)}]
        finally:
            client.close()


def run_canary(rows, warmup, iterations, qps_only=False, index_only=False, parallel_only=False):
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
        if parallel_only:
            specs = PARALLEL_ONLY_SPECS
            # Standalone profile: the batch pipeline setup copies the
            # persisted default table, which only exists once the data
            # is loaded. The full canary loads it via the Bulk Insert
            # spec, which this profile skips - load it unmeasured here.
            bench.bench_insert()
        elif index_only:
            specs = tuple(
                spec
                for spec in CANARY_SPECS
                if spec[1].startswith("bench_index_")
            )
        elif qps_only:
            specs = ()
        else:
            specs = CANARY_SPECS
        if specs:
            for spec in specs:
                name, method_name, mode = spec[0], spec[1], spec[2]
                setup_method = spec[3] if len(spec) > 3 else None
                if method_name == "bench_topk_join_canary":
                    bench.setup_topk_join_canary()
                elif method_name == "bench_dimension_join_aggregation":
                    bench.setup_dimension_join_aggregation()
                elif method_name in {
                    "bench_uncached_delta_scan_group_having_topk",
                    "bench_uncached_delta_boolean_scan_group_having_topk",
                }:
                    bench.setup_uncached_delta_scan_pipeline()
                elif method_name in {
                    "bench_batch_scan_group_having_topk",
                    "bench_parallel_batch_scan_t2",
                    "bench_parallel_batch_scan_t4",
                    "bench_parallel_batch_scan_t8",
                    "bench_parallel_batch_scan_auto",
                }:
                    bench.setup_batch_scan_pipeline()
                elif method_name in {
                    "bench_index_eq_rare",
                    "bench_index_eq_skewed",
                    "bench_index_range",
                    "bench_index_covering",
                }:
                    bench.setup_index_canary()
                elapsed_ms = _run_metric(
                    bench, method_name, mode, warmup, iterations, setup_method
                )
                results.append({
                    "category": "ApexBase canary",
                    "query": name,
                    "ApexBase": round(elapsed_ms, 6),
                })
                print(f"{name:<34} {elapsed_ms:>12.6f} ms")
        if not index_only and not parallel_only:
            quant_results, _quant_recall = run_quantized_canary(rows, warmup, iterations)
            results.extend(quant_results)

        if not index_only and not parallel_only:
            # OLAP Q/s read profile (ApexBase-only). The harness recreates the
            # engine on a clean loaded copy, so the measurement is independent
            # of the delta-heavy state left by the DML canary metrics. Every
            # level of QPS_THREAD_COUNTS is measured in the same run, so the
            # gate compares a read-scaling curve instead of a single point.
            qps = full_bench.run_qps_benchmark(
                tmpdir,
                data,
                n_threads=QPS_THREAD_COUNTS[-1],
                min_duration=1.0,
                min_iterations=50,
                existing_engines={"ApexBase": bench},
                thread_counts=QPS_THREAD_COUNTS,
            )
            qps_labels = qps_metric_labels(QPS_THREAD_COUNTS)
            for label, key in qps_labels:
                results.append({
                    "category": "ApexBase Q/s",
                    "query": label,
                    "ApexBase": round(qps.get(key, 0.0), 3),
                })
                print(f"{label:<34} {qps.get(key, 0.0):>12.3f} Q/s")
        if not qps_only and not index_only and not parallel_only:
            results.extend(run_commit_canary(rows, warmup, iterations))
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
    parser.add_argument(
        "--index-only",
        action="store_true",
        help="Run only the index-accelerated canary metrics (R5.6 guard phase)",
    )
    parser.add_argument(
        "--parallel-only",
        action="store_true",
        help="Run only the parallel batch-scan metrics (R5.7 guard phase)",
    )
    args = parser.parse_args(argv)
    if args.rows <= 0 or args.warmup < 0 or args.iterations <= 0:
        parser.error("rows and iterations must be positive; warmup must be non-negative")

    if args.qps_only and (
        args.quant_only or args.index_only or args.parallel_only
    ):
        parser.error(
            "--qps-only is mutually exclusive with the other profile flags"
        )
    if args.quant_only and (args.index_only or args.parallel_only):
        parser.error(
            "--quant-only is mutually exclusive with the other profile flags"
        )
    if args.index_only and args.parallel_only:
        parser.error("--index-only and --parallel-only are mutually exclusive")
    quantized_recall = None
    if args.quant_only:
        results, quantized_recall = run_quantized_canary(
            args.rows, args.warmup, args.iterations
        )
    elif args.index_only:
        results = run_canary(
            args.rows, args.warmup, args.iterations, index_only=True
        )
    elif args.parallel_only:
        results = run_canary(
            args.rows, args.warmup, args.iterations, parallel_only=True
        )
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
    if quantized_recall is not None:
        report["quantized_recall"] = quantized_recall
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
