"""Benchmark ApexBase and sqlite-vector quantized distance scans.

The two engines receive identical Float32 vectors and query vectors. Each
reported query latency is the median per-query time across repeated batches;
recall is measured against the same engine's exact Float32 top-k result.

sqlite-vector comparisons use its supported ``vector_quantize_scan`` modes:
UINT8, INT8, 1BIT, and TurboQuant 2/3/4-bit. ApexBase additionally reports its
Float16 and BFloat16 derived-column codecs, which have no sqlite-vector
``vector_quantize_scan`` equivalent.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.resources
import json
import platform
import sqlite3
import statistics
import tempfile
import time
from pathlib import Path

import numpy as np

from apexbase.client import ApexClient


APEX_CODECS = (
    "float16", "bfloat16", "int8", "uint8", "bit1",
    "turboquant2", "turboquant3", "turboquant4",
)
SQLITE_QUANTIZERS = (
    ("int8", "INT8"),
    ("uint8", "UINT8"),
    ("bit1", "1BIT"),
    ("turboquant2", "TURBO2"),
    ("turboquant3", "TURBO3"),
    ("turboquant4", "TURBO4"),
)
SQLITE_VECTOR_BINARY_NAMES = ("vector", "vector.dylib", "vector.so", "vector.dll")


def elapsed_ms(callable_):
    started = time.perf_counter()
    value = callable_()
    return (time.perf_counter() - started) * 1000.0, value


def median_query_ms(callable_, query_count: int, warmup: int, iterations: int):
    for _ in range(warmup):
        callable_()
    samples = []
    value = None
    for _ in range(iterations):
        elapsed, value = elapsed_ms(callable_)
        samples.append(elapsed / query_count)
    return statistics.median(samples), value


def recall_at_k(exact_ids, approximate_ids, k: int) -> float:
    return float(np.mean([
        len(exact_ids[index] & approximate_ids[index]) / k
        for index in range(len(exact_ids))
    ]))


def distribution_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "unavailable"


def locate_sqlite_vector_binary() -> str | None:
    try:
        package_dir = importlib.resources.files("sqlite_vector.binaries")
    except (ImportError, ModuleNotFoundError):
        return None
    for name in SQLITE_VECTOR_BINARY_NAMES:
        candidate = package_dir / name
        if candidate.is_file():
            return str(candidate)
    return None


def load_sqlite_vector(connection: sqlite3.Connection) -> dict:
    binary = locate_sqlite_vector_binary()
    if binary is None:
        raise RuntimeError("sqliteai-vector is not installed")
    connection.enable_load_extension(True)
    try:
        connection.load_extension(binary)
    finally:
        connection.enable_load_extension(False)
    version, backend, turbo_backend = connection.execute(
        "SELECT vector_version(), vector_backend(), vector_turboquant_backend()"
    ).fetchone()
    return {
        "version": version,
        "backend": backend,
        "turboquant_backend": turbo_backend,
    }


def apex_ids(client, column: str, query_vectors, k: int, **kwargs):
    return [
        {int(row["_id"]) for row in client.topk_distance(
            column, query, k=k, **kwargs
        ).to_dict()}
        for query in query_vectors
    ]


def benchmark_apexbase(vectors, query_vectors, k, candidate_k, warmup, iterations, directory):
    results = []
    for codec in APEX_CODECS:
        db_dir = directory / f"apexbase-{codec}"
        client = ApexClient(str(db_dir), drop_if_exists=True)
        try:
            client.create_table("vectors", {"embedding": "float32_vector"})
            client.store([{"embedding": vector} for vector in vectors])
            build_ms, target = elapsed_ms(
                lambda: client.create_quantized_column("embedding", codec=codec)
            )
            exact_ms, exact = median_query_ms(
                lambda: apex_ids(client, "embedding", query_vectors, k),
                len(query_vectors), warmup, iterations,
            )
            approximate_ms, approximate = median_query_ms(
                lambda: apex_ids(client, target, query_vectors, k),
                len(query_vectors), warmup, iterations,
            )
            rescore_ms, rescored = median_query_ms(
                lambda: apex_ids(
                    client, "embedding", query_vectors, k,
                    accelerator=target, candidate_k=candidate_k,
                ),
                len(query_vectors), warmup, iterations,
            )
            results.append({
                "codec": codec,
                "build_ms": round(build_ms, 3),
                "exact_ms_per_query": round(exact_ms, 3),
                "quantized_ms_per_query": round(approximate_ms, 3),
                "rescore_ms_per_query": round(rescore_ms, 3),
                "recall_at_k": round(recall_at_k(exact, approximate, k), 6),
                "rescore_recall_at_k": round(recall_at_k(exact, rescored, k), 6),
                "database_bytes": (db_dir / "vectors.apex").stat().st_size,
            })
        finally:
            client.close()
    return results


def sqlite_ids(connection, scan: str, query_vectors, k: int):
    sql = f"SELECT rowid FROM {scan}('vectors', 'embedding', ?, {k})"
    return [
        {int(row[0]) for row in connection.execute(sql, (query.tobytes(),)).fetchall()}
        for query in query_vectors
    ]


def benchmark_sqlite_vector(vectors, query_vectors, k, warmup, iterations, directory):
    results = []
    extension = None
    for codec, qtype in SQLITE_QUANTIZERS:
        db_path = directory / f"sqlite-vector-{codec}.sqlite"
        connection = sqlite3.connect(db_path)
        try:
            extension = load_sqlite_vector(connection)
            connection.execute("PRAGMA journal_mode=OFF")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, embedding BLOB)")
            connection.executemany(
                "INSERT INTO vectors VALUES (?, ?)",
                ((index, vector.tobytes()) for index, vector in enumerate(vectors)),
            )
            connection.execute(
                "SELECT vector_init(?, ?, ?)",
                ("vectors", "embedding", f"type=FLOAT32,dimension={vectors.shape[1]},distance=L2"),
            )
            exact_ms, exact = median_query_ms(
                lambda: sqlite_ids(connection, "vector_full_scan", query_vectors, k),
                len(query_vectors), warmup, iterations,
            )
            build_ms, quantized_rows = elapsed_ms(
                lambda: connection.execute(
                    "SELECT vector_quantize(?, ?, ?)",
                    ("vectors", "embedding", f"qtype={qtype}"),
                ).fetchone()[0]
            )
            quantized_bytes = connection.execute(
                "SELECT vector_quantize_memory(?, ?)", ("vectors", "embedding")
            ).fetchone()[0]
            connection.execute(
                "SELECT vector_quantize_preload(?, ?)", ("vectors", "embedding")
            )
            approximate_ms, approximate = median_query_ms(
                lambda: sqlite_ids(connection, "vector_quantize_scan", query_vectors, k),
                len(query_vectors), warmup, iterations,
            )
            connection.commit()
            results.append({
                "codec": codec,
                "qtype": qtype,
                "build_ms": round(build_ms, 3),
                "exact_ms_per_query": round(exact_ms, 3),
                "quantized_ms_per_query": round(approximate_ms, 3),
                "recall_at_k": round(recall_at_k(exact, approximate, k), 6),
                "quantized_rows": int(quantized_rows),
                "quantized_memory_bytes": int(quantized_bytes),
                "database_bytes": db_path.stat().st_size,
            })
        finally:
            connection.close()
    return extension, results


def benchmark(rows, dim, queries, k, candidate_k, seed, warmup, iterations):
    rng = np.random.default_rng(seed)
    vectors = rng.normal(size=(rows, dim)).astype(np.float32)
    query_vectors = vectors[:queries] + rng.normal(
        scale=0.02, size=(queries, dim)
    ).astype(np.float32)
    with tempfile.TemporaryDirectory(prefix="apexbase_quant_bench_") as tmp:
        directory = Path(tmp)
        apexbase = benchmark_apexbase(
            vectors, query_vectors, k, candidate_k, warmup, iterations, directory
        )
        sqlite_extension, sqlite_vector = benchmark_sqlite_vector(
            vectors, query_vectors, k, warmup, iterations, directory
        )
    return {
        "system": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "apexbase": distribution_version("apexbase"),
            "sqliteai_vector": distribution_version("sqliteai-vector"),
            "sqlite_vector_extension": sqlite_extension,
        },
        "apexbase": apexbase,
        "sqlite_vector": sqlite_vector,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--queries", type=int, default=20)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--candidate-k", type=int, default=100)
    parser.add_argument("--seed", type=int, default=20260821)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    numeric = (
        args.rows, args.dim, args.queries, args.k, args.candidate_k,
        args.warmup, args.iterations,
    )
    if min(numeric) <= 0:
        parser.error("all numeric arguments must be positive")
    if args.queries > args.rows or args.k > args.candidate_k:
        parser.error("queries must be <= rows and k must be <= candidate-k")
    payload = {
        "config": vars(args) | {"output": str(args.output) if args.output else None},
        **benchmark(
            args.rows, args.dim, args.queries, args.k, args.candidate_k,
            args.seed, args.warmup, args.iterations,
        ),
    }
    rendered = json.dumps(payload, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
