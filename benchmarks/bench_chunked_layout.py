"""Chunked (per-column compressed) row-group layout: read/write guard phase.

The V5 storage work added an opt-in layout that stores every column of a
compressed row group in its own zstd/LZ4 frame, so a statement that touches one
column decodes one column instead of the whole group. The wide-table phase
cannot sample that layout: its fixture derives most columns from ``k``, and the
8% size gate correctly refuses per-column frames when dropping the cross-column
correlation costs more than that. This phase therefore builds a *independent*
16-column zstd fixture, where the gate accepts the layout, and reports one
latency row per shape so the same-machine guard samples base/current like any
other metric.

Every shape is asserted against an independent computation over the generated
rows. The flat twin is built and measured too, but only as an informational
block: it is equal work on both sides of the guard, and keeping it out of the
gated rows lets the phase measure the chunked layout without doubling every
comparison.

``APEX_CHUNK_INDEX`` is forced around the two writes so the phase keeps working
after a future default flip; the process value is restored before the reads.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench_vs_sqlite_duckdb as full_bench
from apexbase.client import ApexClient

COLUMN_COUNT = 16
SCHEMA = {
    "k": "int64",
    "i1": "int64",
    "i2": "int64",
    "i3": "int64",
    "f1": "float64",
    "f2": "float64",
    "f3": "float32",
    "s1": "string",
    "s2": "string",
    "s3": "string",
    "s4": "string",
    "b1": "bool",
    "n1": "int64",
    "u1": "int64",
    "t1": "int64",
    "c2": "int64",
}

# (metric suffix, SQL). `{lo}` changes per timed iteration so a stale result
# cache could never help, `{mid}` is the fixed point-lookup id.
SHAPES = (
    ("filter count (single column)", "SELECT COUNT(*) AS n FROM {table} WHERE k >= {lo}"),
    ("sum (single column)", "SELECT SUM(f1) AS s FROM {table} WHERE k >= {lo}"),
    ("projection (3 cols) limit 100", "SELECT k, i1, f1 FROM {table} LIMIT 100"),
    ("single column limit 100", "SELECT k FROM {table} LIMIT 100"),
    ("select * limit 100", "SELECT * FROM {table} LIMIT 100"),
    ("limit 10 offset 1", "SELECT * FROM {table} LIMIT 10 OFFSET 1"),
    ("string equality count", "SELECT COUNT(*) AS n FROM {table} WHERE s1 = 'g007'"),
    ("point lookup by _id", "SELECT * FROM {table} WHERE _id = {mid}"),
    ("group by string", "SELECT s1, COUNT(*) AS n FROM {table} GROUP BY s1"),
    ("full scan", "SELECT * FROM {table}"),
)

# "Touch almost nothing" shapes measured with a fresh client per iteration: the
# in-process decompression cache cannot hide what the first touch costs.
COLD_SHAPES = (
    ("single column limit 100 (cold reopen)", "SELECT k FROM {table} LIMIT 100"),
)


def generate_rows(rows):
    """A 16-column dataset whose columns do not predict each other.

    Independence is the point: correlated columns make the per-column frames
    larger than one flat frame, so the 8% gate refuses the layout and the phase
    would silently stop measuring it.
    """
    rng = np.random.default_rng(20261028)
    data = {
        "k": rng.integers(0, 1_000_000, rows).tolist(),
        "i1": rng.integers(0, 10_000, rows).tolist(),
        "i2": rng.integers(0, 1_000, rows).tolist(),
        "i3": rng.integers(0, 1_000_000, rows).tolist(),
        "f1": rng.random(rows).tolist(),
        "f2": (rng.random(rows) * 10).tolist(),
        "f3": (rng.random(rows) * 2).tolist(),
        "s1": [f"g{int(v) % 50:03d}" for v in rng.integers(0, 1_000_000, rows)],
        "s2": [f"k{int(v) % 1000:04d}" for v in rng.integers(0, 1_000_000, rows)],
        "s3": [f"payload-{int(v) % 37:03d}-xxxxxxxx" for v in rng.integers(0, 1_000_000, rows)],
        "s4": [f"c{int(v) % 10}" for v in rng.integers(0, 1_000_000, rows)],
        "b1": [bool(v) for v in rng.integers(0, 2, rows)],
        "n1": [None if v % 7 == 0 else int(v) for v in rng.integers(0, rows, rows)],
        "u1": rng.integers(0, 65536, rows).tolist(),
        "t1": (1_600_000_000 + rng.integers(0, 1_000_000, rows)).tolist(),
        "c2": rng.integers(0, 3, rows).tolist(),
    }
    expected = {
        "rows": rows,
        "sum_f1": float(np.sum(data["f1"])),
        # The predicate is on the `k` values, not on the row position.
        "sum_f1_k_ge_1000": float(
            np.sum([value for value, key in zip(data["f1"], data["k"]) if key >= 1000])
        ),
        "eq_g007": sum(1 for value in data["s1"] if value == "g007"),
        "groups_s1": len(set(data["s1"])),
        "k_at": lambda index: data["k"][index],
        "k_ge": lambda lo: sum(1 for value in data["k"] if value >= lo),
    }
    return data, expected


def table_path(db_dir, table):
    return db_dir / f"{table}.apex"


def row_group_count(path):
    header = path.read_bytes()[:256]
    return int.from_bytes(header[84:88], "little")


def first_rg_header(path):
    """Return (file_version, compression_flag, encoding_version, id_encoding)."""
    with path.open("rb") as handle:
        head = handle.read(256 + 32)
    return (
        int.from_bytes(head[8:12], "little"),
        head[256 + 28],
        head[256 + 29],
        head[256 + 30],
    )


def table_bytes(db_dir, table):
    return sum(
        item.stat().st_size
        for item in db_dir.glob(f"{table}*")
        if item.is_file()
    )


def measure(client, table, sql_template, warmup, iterations, mid):
    samples = []
    template = sql_template.replace("{table}", table)
    for index in range(warmup + iterations):
        sql = template.format(lo=1000 + index, mid=mid)
        if index < warmup:
            client.execute(sql)
            continue
        start = time.perf_counter()
        client.execute(sql)
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def measure_cold(db_dir, table, sql_template, iterations, mid):
    samples = []
    for index in range(iterations):
        client = ApexClient(str(db_dir), drop_if_exists=False, enable_cache=False)
        try:
            client.use_table(table)
            sql = sql_template.replace("{table}", table).format(lo=1000 + index, mid=mid)
            start = time.perf_counter()
            client.execute(sql)
            samples.append((time.perf_counter() - start) * 1000.0)
        finally:
            client.close()
    return statistics.median(samples)


def check(client, table, sql_template, expected, mid):
    """Assert each shape against the independent computation."""
    sql = sql_template.replace("{table}", table).format(lo=1000, mid=mid)
    result = client.execute(sql).to_dict()
    if sql_template.endswith("COUNT(*) AS n FROM {table} WHERE k >= {lo}"):
        assert result == [{"n": expected["k_ge"](1000)}], (sql, result)
    elif sql_template.startswith("SELECT SUM(f1)"):
        assert abs(result[0]["s"] - expected["sum_f1_k_ge_1000"]) <= (
            abs(expected["sum_f1_k_ge_1000"]) * 1e-9 + 1e-6
        ), (sql, result[0])
    elif sql_template.startswith("SELECT COUNT(*) AS n FROM {table} WHERE s1"):
        assert result == [{"n": expected["eq_g007"]}], (sql, result)
    elif sql_template.startswith("SELECT k, i1, f1"):
        assert len(result) == 100, (sql, len(result))
        assert [row["k"] for row in result] == [
            expected["k_at"](index) for index in range(100)
        ], sql
    elif sql_template.startswith("SELECT k FROM"):
        assert len(result) == 100, (sql, len(result))
        assert [row["k"] for row in result] == [
            expected["k_at"](index) for index in range(100)
        ], sql
    elif sql_template.startswith("SELECT * FROM {table} WHERE _id"):
        assert len(result) == 1, (sql, result)
        assert result[0]["k"] == expected["k_at"](mid - 1), (sql, result[0])
    elif sql_template.endswith("LIMIT 10 OFFSET 1"):
        assert len(result) == 10, (sql, len(result))
        assert result[0]["k"] == expected["k_at"](1), (sql, result[0])
    elif sql_template.startswith("SELECT s1, COUNT(*)"):
        assert len(result) == expected["groups_s1"], (sql, len(result))
        assert sum(row["n"] for row in result) == expected["rows"], sql
    elif sql_template.endswith("LIMIT 100"):
        assert len(result) == 100, (sql, len(result))
    elif sql_template.startswith("SELECT * FROM"):
        assert len(result) == expected["rows"], (sql, len(result))
    else:  # pragma: no cover - a new shape must extend this dispatcher
        raise AssertionError(f"unchecked shape: {sql_template}")


def _build(client, table, data, compression, force_chunked):
    client.create_table(table, SCHEMA)
    client.use_table(table)
    if compression:
        assert client.set_compression(compression) is not False, compression
    previous = os.environ.get("APEX_CHUNK_INDEX")
    # Force the layout on both writes (not just the chunked one): the flat twin
    # must be flat even if this process inherited the opt-in.
    os.environ["APEX_CHUNK_INDEX"] = "1" if force_chunked else "0"
    try:
        start = time.perf_counter()
        client.store(data)
        client.flush()
        return (time.perf_counter() - start) * 1000.0
    finally:
        if previous is None:
            os.environ.pop("APEX_CHUNK_INDEX", None)
        else:
            os.environ["APEX_CHUNK_INDEX"] = previous


def run_chunked_layout(rows, warmup, iterations, only=None):
    wanted = set(only) if only else None

    def want(metric):
        return wanted is None or metric in wanted

    full_bench.ensure_optional_imports()
    if not full_bench.HAS_APEXBASE:
        raise RuntimeError("ApexBase is not importable; run maturin develop --release first")

    data, expected = generate_rows(rows)
    mid = rows // 2 + 1
    tmpdir = Path(tempfile.mkdtemp(prefix="apexbase_chunked_"))
    db_dir = tmpdir / "db"
    results = []
    detail = {"rows": rows, "columns": COLUMN_COUNT, "variants": {}, "shapes": {}}
    try:
        db_dir.mkdir(parents=True, exist_ok=True)
        client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)

        # Informational flat twin: shows what the per-column frames save. It is
        # equal work on both guard sides, so it stays out of the gated rows.
        build_flat_ms = _build(client, "chunk_flat", data, "zstd", force_chunked=False)
        flat_path = table_path(db_dir, "chunk_flat")
        flat_version, flat_compress, flat_encoding, _ = first_rg_header(flat_path)
        detail["variants"]["flat"] = {
            "table": "chunk_flat",
            "compression": "zstd",
            "file_version": flat_version,
            "row_group_encoding": flat_encoding,
            "row_groups": row_group_count(flat_path),
            "bytes": table_bytes(db_dir, "chunk_flat"),
            "build_ms": round(build_flat_ms, 6),
        }
        assert flat_encoding == 1, (
            "the informational twin must stay flat; a chunked file would make the "
            "comparison meaningless"
        )

        build_ms = _build(client, "chunk_table", data, "zstd", force_chunked=True)
        path = table_path(db_dir, "chunk_table")
        version, compress_flag, encoding, id_encoding = first_rg_header(path)
        detail["variants"]["chunked"] = {
            "table": "chunk_table",
            "compression": "zstd",
            "file_version": version,
            "row_group_encoding": encoding,
            "row_groups": row_group_count(path),
            "bytes": table_bytes(db_dir, "chunk_table"),
            "build_ms": round(build_ms, 6),
        }
        assert compress_flag != 0, "the chunked fixture must be compressed"
        assert encoding == 2, (
            "the 8% size gate refused the chunked layout; the fixture's columns "
            "must stay independent of each other"
        )
        assert version in (4, 5), version  # 4 = pre-flip build, 5 = declares V5

        # The build row is the only write metric; it compares the two sides'
        # compression work on identical input.
        if want("Chunked: build (zstd fixture)"):
            results.append({
                "category": "ApexBase chunked layout",
                "query": "Chunked: build (zstd fixture)",
                "ApexBase": round(build_ms, 6),
            })
            detail["shapes"]["Chunked: build (zstd fixture)"] = {
                "warm_ms": round(build_ms, 6)
            }

        for table, label in (("chunk_table", "chunked"), ("chunk_flat", "flat")):
            prefix = "Chunked" if label == "chunked" else "Chunked/Flat"
            for name, sql in SHAPES:
                metric = f"{prefix}: {name}"
                check(client, table, sql, expected, mid)
                warm_ms = measure(client, table, sql, warmup, iterations, mid)
                detail["shapes"][metric] = {"warm_ms": round(warm_ms, 6)}
                if label == "chunked" and want(metric):
                    results.append({
                        "category": "ApexBase chunked layout",
                        "query": metric,
                        "ApexBase": round(warm_ms, 6),
                    })
            for name, sql in COLD_SHAPES:
                metric = f"{prefix}: {name}"
                check(client, table, sql, expected, mid)
                cold_ms = measure_cold(db_dir, table, sql, iterations, mid)
                detail["shapes"][metric] = {"cold_ms": round(cold_ms, 6)}
                if label == "chunked" and want(metric):
                    results.append({
                        "category": "ApexBase chunked layout",
                        "query": metric,
                        "ApexBase": round(cold_ms, 6),
                    })

        client.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    return results, detail


def chunked_metric_names():
    """Every gated metric name, in report order."""
    names = ["Chunked: build (zstd fixture)"]
    names.extend(f"Chunked: {name}" for name, _ in SHAPES)
    names.extend(f"Chunked: {name}" for name, _ in COLD_SHAPES)
    return names


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=7)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        metavar="METRIC",
        help="Measure only the named metric(s); used by the guard to recheck a "
        "marked metric in a fresh process",
    )
    args = parser.parse_args(argv)
    if args.rows <= 0 or args.warmup < 0 or args.iterations <= 0:
        parser.error("rows and iterations must be positive; warmup must be non-negative")
    known = set(chunked_metric_names())
    unknown = [name for name in args.only if name not in known]
    if unknown:
        parser.error("--only metric(s) not reported by this script: " + ", ".join(unknown))

    results, detail = run_chunked_layout(
        args.rows, args.warmup, args.iterations, only=args.only
    )
    report = {
        **full_bench.get_report_metadata("apexbase-canary"),
        "config": {
            "rows": args.rows,
            "warmup": args.warmup,
            "iterations": args.iterations,
        },
        "results": results,
        "chunked_layout": detail,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    raise SystemExit(main())
