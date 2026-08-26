#!/usr/bin/env python3
"""Benchmark the zero-initialization ApexBase and DuckDB default connections."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from pathlib import Path

import apexbase
import duckdb


WORKLOADS = (
    ("constant expression", "SELECT 40 + 2", "SELECT 40 + 2"),
    ("in-memory COUNT(*)", "SELECT COUNT(*) FROM bench", "SELECT COUNT(*) FROM bench"),
)


def _median_ms(call, warmup: int, iterations: int) -> float:
    for _ in range(warmup):
        call()
    samples = []
    for _ in range(iterations):
        started = time.perf_counter_ns()
        call()
        samples.append((time.perf_counter_ns() - started) / 1_000_000.0)
    return statistics.median(samples)


def _prepare(rows: int) -> None:
    apexbase._close_default_connection()
    apexbase.execute("CREATE TABLE bench (value BIGINT)")
    batch = 2_000
    for start in range(0, rows, batch):
        values = ",".join(f"({value})" for value in range(start, min(rows, start + batch)))
        apexbase.execute(f"INSERT INTO bench VALUES {values}")

    duckdb.execute("DROP TABLE IF EXISTS bench")
    duckdb.execute(f"CREATE TABLE bench AS SELECT range AS value FROM range({rows})")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--iterations", type=int, default=51)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--assert-faster", action="store_true")
    args = parser.parse_args()
    if args.rows <= 0 or args.warmup < 0 or args.iterations <= 0:
        parser.error("rows and iterations must be positive; warmup must be non-negative")

    _prepare(args.rows)
    results = []
    try:
        for name, apex_sql, duck_sql in WORKLOADS:
            apex_ms = _median_ms(
                lambda: apexbase.execute(apex_sql).scalar(), args.warmup, args.iterations
            )
            duck_ms = _median_ms(
                lambda: duckdb.execute(duck_sql).fetchone()[0], args.warmup, args.iterations
            )
            results.append(
                {
                    "workload": name,
                    "apexbase_ms": apex_ms,
                    "duckdb_ms": duck_ms,
                    "speedup": duck_ms / apex_ms,
                }
            )
    finally:
        apexbase._close_default_connection()
        duckdb.execute("DROP TABLE IF EXISTS bench")

    apex_geomean = math.prod(row["apexbase_ms"] for row in results) ** (1 / len(results))
    duck_geomean = math.prod(row["duckdb_ms"] for row in results) ** (1 / len(results))
    report = {
        "rows": args.rows,
        "warmup": args.warmup,
        "iterations": args.iterations,
        "results": results,
        "geomean": {
            "apexbase_ms": apex_geomean,
            "duckdb_ms": duck_geomean,
            "speedup": duck_geomean / apex_geomean,
        },
    }
    for row in results:
        print(
            f"{row['workload']:<24} ApexBase {row['apexbase_ms']:.6f} ms  "
            f"DuckDB {row['duckdb_ms']:.6f} ms  {row['speedup']:.2f}x"
        )
    print(f"geomean speedup: {report['geomean']['speedup']:.2f}x")
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if args.assert_faster and apex_geomean >= duck_geomean:
        print("FAIL: ApexBase default execute geomean did not beat DuckDB")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
