"""Measure stored-vector quantization size, latency, and recall.

This benchmark is intentionally ApexBase-only: it exercises the new derived
column lifecycle and exact-rescore path without changing the public cross-engine
benchmark's established metric contract.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

import numpy as np

from apexbase.client import ApexClient


CODECS = ("float16", "bfloat16", "int8", "uint8", "bit1", "turboquant2", "turboquant3", "turboquant4")


def elapsed_ms(callable_):
    started = time.perf_counter()
    value = callable_()
    return (time.perf_counter() - started) * 1000.0, value


def benchmark(rows: int, dim: int, queries: int, k: int, candidate_k: int, seed: int):
    rng = np.random.default_rng(seed)
    vectors = rng.normal(size=(rows, dim)).astype(np.float32)
    query_vectors = vectors[:queries] + rng.normal(scale=0.02, size=(queries, dim)).astype(np.float32)
    results = []

    with tempfile.TemporaryDirectory(prefix="apexbase_quant_bench_") as directory:
        for codec in CODECS:
            db_dir = Path(directory) / codec
            client = ApexClient(str(db_dir), drop_if_exists=True)
            client.create_table("vectors", {"embedding": "float32_vector"})
            client.store([{"embedding": vector} for vector in vectors])
            build_ms, target = elapsed_ms(
                lambda: client.create_quantized_column("embedding", codec=codec)
            )

            exact_ids = []
            approximate_ids = []
            exact_ms, _ = elapsed_ms(lambda: [
                exact_ids.append({int(row["_id"]) for row in client.topk_distance(
                    "embedding", query, k=k
                ).to_dict()})
                for query in query_vectors
            ])
            approximate_ms, _ = elapsed_ms(lambda: [
                approximate_ids.append({int(row["_id"]) for row in client.topk_distance(
                    target, query, k=k
                ).to_dict()})
                for query in query_vectors
            ])
            rescore_ms, rescored = elapsed_ms(lambda: [
                client.topk_distance(
                    "embedding", query, k=k, accelerator=target,
                    candidate_k=candidate_k,
                ).to_dict()
                for query in query_vectors
            ])
            recall = float(np.mean([
                len(exact_ids[index] & approximate_ids[index]) / k
                for index in range(queries)
            ]))
            rescore_recall = float(np.mean([
                len(exact_ids[index] & {int(row["_id"]) for row in rescored[index]}) / k
                for index in range(queries)
            ]))
            client.close()
            results.append({
                "codec": codec,
                "build_ms": round(build_ms, 3),
                "approximate_query_ms": round(approximate_ms, 3),
                "exact_query_ms": round(exact_ms, 3),
                "rescore_query_ms": round(rescore_ms, 3),
                "approximate_recall_at_k": round(recall, 6),
                "rescore_recall_at_k": round(rescore_recall, 6),
                "file_bytes": (db_dir / "vectors.apex").stat().st_size,
            })
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--queries", type=int, default=20)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--candidate-k", type=int, default=100)
    parser.add_argument("--seed", type=int, default=20260821)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if min(args.rows, args.dim, args.queries, args.k, args.candidate_k) <= 0:
        parser.error("all numeric arguments must be positive")
    if args.queries > args.rows or args.k > args.candidate_k:
        parser.error("queries must be <= rows and k must be <= candidate-k")
    payload = {
        "config": vars(args) | {"output": str(args.output) if args.output else None},
        "results": benchmark(args.rows, args.dim, args.queries, args.k, args.candidate_k, args.seed),
    }
    rendered = json.dumps(payload, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
