"""Quantify the V5 string zone-map gain on the shapes it exists for.

A string zone map stores three ranges per row group: the byte-length range
(every build), the order-preserving first-eight-byte value range and the
second-eight-byte tail range (V5). A row group is skipped when any range proves
the candidate absent, so the shapes below separate a build that only has the
length heuristic from one that also has the value and tail ranges:

* equal-width, shared-prefix values (timestamps, UUIDs, prefixed ids) — the
  length range is constant, so only the tail range can prune
* equal-width bucket keys — only the first-eight value range can prune
* a probe that shares its first sixteen bytes with a stored value — stays
  answerable (the zone can never prove it absent) but still skips most groups
* varied-length values (``user_{i}``) — the shared fast lane both builds use, so
  a build that loses its length pruning regresses here (this is the shape the
  first string-zone revision broke) and is measured as a gate metric too

Shapes whose lane does not consult string zones (``IN`` lists, low-cardinality
dictionary columns, ordering ranges) stay in the informational block: they are
diagnostics, and their unpruned full scan is a large, noisy number that must not
become a regression flag.

Every shape's row count is asserted, so a build that silently skips a matching
row group fails the benchmark instead of reporting a fast wrong answer.

Metric rows are emitted in the guard's report format (``query`` + ``ApexBase``),
so ``benchmarks/compare_perf_baseline.py`` and
``benchmarks/run_local_perf_guard.py --mode full`` consume them directly.
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench_vs_sqlite_duckdb as full_bench
from apexbase.client import ApexClient

# (metric name, SQL, expected row count). `{table}` is the fixture table.
GATE_SHAPES = (
    (
        "equal-width equality (first group)",
        "SELECT * FROM {table} WHERE code = '{first}'",
        1,
    ),
    (
        "equal-width equality (last group)",
        "SELECT * FROM {table} WHERE code = '{last}'",
        1,
    ),
    (
        "shared sixteen bytes equality",
        "SELECT * FROM {table} WHERE code = '{twin}'",
        0,
    ),
    (
        # The lane both builds use: V4 prunes it from the byte-length range and
        # V5 from all three ranges, so a build that loses that pruning (as the
        # first value-only revision did) shows up as a regression here.
        "varied-length equality",
        "SELECT * FROM {table} WHERE name = 'user_5000'",
        1,
    ),
)

# Diagnostics whose lane does not read string zones (an `IN` list, a
# low-cardinality dictionary column, an ordering range). They stay out of the
# gate metric set: an unpruned full scan is a large, noisy number, and pinning
# it would turn sampling noise into a regression flag.
CONTROL_SHAPES = (
    (
        "equal-width IN (3 values)",
        "SELECT * FROM {table} WHERE code IN ('{first}', '{middle}', '{last}')",
        3,
    ),
    (
        "equal-width bucket equality",
        "SELECT * FROM {table} WHERE bucket = '{bucket}'",
        1,
    ),
    (
        "ordering range",
        "SELECT * FROM {table} WHERE code >= '{first}' AND code < '{middle}'",
        None,
    ),
)


def generate_rows(rows):
    """One deterministic table covering every string-zone shape."""
    bucket_size = max(rows // 8, 1)
    return {
        # Equal width, first eight bytes shared by every row: only the tail
        # range can tell these apart.
        "code": [f"2026-09-26T{i:06d}" for i in range(rows)],
        # Equal width, first eight bytes differ per bucket: only the first-eight
        # value range can prune.
        "bucket": [f"k{i // bucket_size:07d}" for i in range(rows)],
        # Varied length: the legacy length range already prunes this shape.
        "name": [f"user_{i}" for i in range(rows)],
        "pad": list(range(rows)),
    }


def measure(client, sql, warmup, iterations):
    client.execute(sql)
    for _ in range(warmup):
        client.execute(sql)
    samples = []
    for _ in range(iterations):
        start = time.perf_counter()
        client.execute(sql)
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def run_shapes(rows, warmup, iterations):
    full_bench.ensure_optional_imports()
    if not full_bench.HAS_APEXBASE:
        raise RuntimeError("ApexBase is not importable; run maturin develop --release first")

    data = generate_rows(rows)
    tmpdir = Path(tempfile.mkdtemp(prefix="apexbase_zones_"))
    db_dir = tmpdir / "db"
    detail = {"rows": rows, "shapes": {}, "controls": {}}
    results = []
    try:
        db_dir.mkdir(parents=True, exist_ok=True)
        client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)
        client.create_table(
            "zs",
            {
                "code": "string",
                "bucket": "string",
                "name": "string",
                "pad": "int64",
            },
        )
        client.use_table("zs")
        client.store(data)
        client.flush()

        # Probes: first/last stored values, the bucket of the third group's key
        # and a value that shares its first sixteen bytes with a stored one but
        # cannot exist (the last byte is outside the stored window).
        middle = data["code"][rows // 2]
        twin = f"{middle[:16]}x"
        if twin in set(data["code"]) or twin == middle:
            raise RuntimeError("the shared-sixteen-bytes probe must not exist")
        bucket = data["bucket"][rows // 4]
        expected_middle = sum(1 for value in data["code"] if value == middle)
        expected_bucket = sum(1 for value in data["bucket"] if value == bucket)
        if expected_middle != 1 or expected_bucket == 0:
            raise RuntimeError("fixture probes must be unique and present")
        substitutions = {
            "table": "zs",
            "first": data["code"][0],
            "last": data["code"][-1],
            "middle": middle,
            "twin": twin,
            "bucket": bucket,
        }

        def expected_rows(name, expected):
            # The bucket probe matches a whole bucket, not a single row.
            if name == "equal-width bucket equality":
                return expected_bucket
            return expected

        for name, sql, expected in GATE_SHAPES:
            query = sql.format(**substitutions)
            rows_returned = len(client.execute(query).to_dict())
            want = expected_rows(name, expected)
            if rows_returned != want:
                raise RuntimeError(f"{name}: expected {want} rows, got {rows_returned}")
            warm_ms = measure(client, query, warmup, iterations)
            results.append(
                {
                    "category": "ApexBase string zone shapes",
                    "query": f"Zone: {name}",
                    "ApexBase": round(warm_ms, 6),
                }
            )
            detail["shapes"][name] = {"warm_ms": round(warm_ms, 6), "rows": rows_returned}

        for name, sql, expected in CONTROL_SHAPES:
            query = sql.format(**substitutions)
            rows_returned = len(client.execute(query).to_dict())
            want = expected_rows(name, expected)
            if want is not None and rows_returned != want:
                raise RuntimeError(f"{name}: expected {want} rows, got {rows_returned}")
            detail["controls"][name] = {
                "warm_ms": round(measure(client, query, warmup, iterations), 6),
                "rows": rows_returned,
            }
        client.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    return results, detail


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=7)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.rows <= 0 or args.warmup < 0 or args.iterations <= 0:
        parser.error("rows and iterations must be positive; warmup must be non-negative")

    results, detail = run_shapes(args.rows, args.warmup, args.iterations)
    report = {
        **full_bench.get_report_metadata("apexbase-canary"),
        "config": {
            "rows": args.rows,
            "warmup": args.warmup,
            "iterations": args.iterations,
        },
        "results": results,
        "string_zone_shapes": detail,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    raise SystemExit(main())
