"""W1-5b: single-query TopK — flat (`raw=True`) vs ResultView return.

Not part of the public 72+6 scoreboard or the base/current gate: the base
commit has no `raw=True`, so this measures the current build only. The flat
return skips the Arrow RecordBatch/Table/ResultView fixed cost; the ranking is
byte-for-byte the same, which `test/test_topk_distance_raw.py` pins.

With ``--update`` it also measures the ranking after a SQL ``UPDATE`` of a
non-vector column: the direct mmap lane must keep serving the same ranking
instead of standing down for the rest of the session (the 2.0.0 defect), so a
fast-path regression shows up as a latency jump here even though every other
benchmark only measures a pristine table. This case cannot join the base/current
gate: the base revision has no ``raw=True`` at all, so its metric set differs.

Usage: python benchmarks/bench_topk_raw.py [--rows 1000] [--dim 8] [--k 10] [--update]
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time

import numpy as np

from apexbase.client import ApexClient


def measure(fn, warmup: int, iterations: int) -> dict:
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(iterations):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000)
    return {
        "median_ms": round(statistics.median(samples), 6),
        "p99_ms": round(sorted(samples)[max(0, int(len(samples) * 0.99) - 1)], 6),
        "samples": len(samples),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000)
    parser.add_argument("--dim", type=int, default=8)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument(
        "--update",
        action="store_true",
        help="also measure raw TopK after a SQL UPDATE of a non-vector column",
    )
    parser.add_argument("--output")
    args = parser.parse_args()

    rng = np.random.default_rng(11)
    matrix = rng.standard_normal((args.rows, args.dim), dtype=np.float32)
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table(
            "t", schema={"label": "int64", "vec": "float32_vector"}
        )
        client.use_table("t")
        client.store(
            {
                "label": list(range(args.rows)),
                "vec": [matrix[i] for i in range(args.rows)],
            }
        )
        client.flush()
        try:
            query = matrix[0].copy()
            default = measure(
                lambda: client.topk_distance("vec", query, k=args.k),
                args.warmup,
                args.iterations,
            )
            raw = measure(
                lambda: client.topk_distance("vec", query, k=args.k, raw=True),
                args.warmup,
                args.iterations,
            )
            results = [
                {"category": "ApexBase TopK return shape", "query": f"TopK default ResultView (rows={args.rows}, k={args.k})", "ApexBase": default["median_ms"]},
                {"category": "ApexBase TopK return shape", "query": f"TopK raw flat (rows={args.rows}, k={args.k})", "ApexBase": raw["median_ms"]},
            ]
            raw_after_update = None
            if args.update:
                before = client.topk_distance("vec", query, k=args.k, raw=True)
                updated = client.execute(
                    "UPDATE t SET label = -1 WHERE label < ?",
                    params=[max(1, args.rows // 10)],
                ).to_dict()
                if not updated or updated[0].get("result", 0) < 1:
                    raise SystemExit("benchmark UPDATE matched no rows")
                raw_after_update = measure(
                    lambda: client.topk_distance("vec", query, k=args.k, raw=True),
                    args.warmup,
                    args.iterations,
                )
                after = client.topk_distance("vec", query, k=args.k, raw=True)
                if after != before:
                    raise SystemExit(
                        "raw TopK ranking changed after an unrelated SQL UPDATE: "
                        f"{after} != {before}"
                    )
                results.append(
                    {
                        "category": "ApexBase TopK after UPDATE",
                        "query": f"TopK raw flat after SQL UPDATE (rows={args.rows}, k={args.k})",
                        "ApexBase": raw_after_update["median_ms"],
                    }
                )
            print(f"rows={args.rows} dim={args.dim} k={args.k}")
            print(f"default ResultView  {default['median_ms']:.4f} ms  (p99 {default['p99_ms']:.4f})")
            print(f"raw flat            {raw['median_ms']:.4f} ms  (p99 {raw['p99_ms']:.4f})")
            print(f"speedup             {default['median_ms'] / raw['median_ms']:.2f}x")
            if raw_after_update is not None:
                print(
                    f"raw after UPDATE    {raw_after_update['median_ms']:.4f} ms  "
                    f"(p99 {raw_after_update['p99_ms']:.4f}, "
                    f"{raw_after_update['median_ms'] / raw['median_ms']:.2f}x vs raw)"
                )
            if args.output:
                with open(args.output, "w") as handle:
                    json.dump({"results": results}, handle, indent=2)
                print(f"saved to {args.output}")
        finally:
            client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
