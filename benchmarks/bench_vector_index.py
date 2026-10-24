"""Approximate TopK through the coarse IVF index (W5-1 stage 2).

Not part of the base/current gate: the base commit has no vector index. The
brute-force default is measured on the same build so the report shows the
candidate-rescoring win and the recall of a small nprobe.

Usage: python benchmarks/bench_vector_index.py [--rows 200000] [--dim 64]
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


def _median_ms(fn, warmup=2, iterations=5):
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(iterations):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000)
    return statistics.median(samples)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--dim", type=int, default=64)
    parser.add_argument("--clusters", type=int, default=64)
    parser.add_argument("--nlist", type=int, default=64)
    parser.add_argument("--nprobe", type=int, default=8)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--metric", choices=("l2", "cosine"), default="l2")
    parser.add_argument("--output")
    args = parser.parse_args()

    rng = np.random.default_rng(23)
    centers = rng.standard_normal((args.clusters, args.dim)).astype(np.float32) * 6.0
    labels = rng.integers(0, args.clusters, args.rows)
    matrix = (centers[labels] + rng.standard_normal((args.rows, args.dim))).astype(np.float32)
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", {"vec": "float32_vector"})
        client.use_table("t")
        client.store({"vec": [matrix[i] for i in range(args.rows)]})
        client.flush()
        try:
            query = matrix[args.rows // 2].copy()
            exact = client.topk_distance(
                "vec", query, k=args.k, metric=args.metric, raw=True
            )
            build_start = time.perf_counter()
            client.create_vector_index(
                "vec", nlist=args.nlist, iterations=6, metric=args.metric
            )
            build_ms = (time.perf_counter() - build_start) * 1000
            probe = client.topk_distance(
                "vec", query, k=args.k, metric=args.metric, nprobe=args.nprobe, raw=True
            )
            recall = len(set(probe["ids"]) & set(exact["ids"])) / max(1, len(exact["ids"]))

            brute_ms = _median_ms(
                lambda: client.topk_distance(
                    "vec", query, k=args.k, metric=args.metric, raw=True
                )
            )
            probe_ms = _median_ms(
                lambda: client.topk_distance(
                    "vec",
                    query,
                    k=args.k,
                    metric=args.metric,
                    nprobe=args.nprobe,
                    raw=True,
                )
            )
            rows = [
                {"category": "ApexBase vector index", "query": f"TopK brute force (rows={args.rows}, dim={args.dim})", "ApexBase": round(brute_ms, 3)},
                {"category": "ApexBase vector index", "query": f"TopK IVF nprobe={args.nprobe}/{args.nlist} (rows={args.rows}, dim={args.dim})", "ApexBase": round(probe_ms, 3)},
            ]
            print(
                f"rows={args.rows} dim={args.dim} nlist={args.nlist} "
                f"nprobe={args.nprobe} metric={args.metric}"
            )
            print(f"index build          {build_ms:.1f} ms")
            print(f"brute force          {brute_ms:.4f} ms")
            print(f"IVF nprobe={args.nprobe}       {probe_ms:.4f} ms  (speedup {brute_ms / probe_ms:.2f}x)")
            print(f"recall@{args.k}              {recall:.3f}")
            if args.output:
                with open(args.output, "w") as handle:
                    json.dump({"results": rows, "recall": recall}, handle, indent=2)
                print(f"saved to {args.output}")
        finally:
            client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
