"""Flat vs chunked (per-column compressed) row-group layout comparison.

The chunked layout stores every column of a compressed row group in its own
frame, which makes a column-selective scan decode one column instead of the
whole group. It is opt-in today (`APEX_CHUNK_INDEX=1`); this harness builds the
same data in both layouts and reports, per shape and per table width, how much
the layout costs or saves. Every measurement interleaves the two layouts and
takes a median of round medians, so machine drift cannot favour one side.

The output is decision material: each row carries the relative and absolute
delta and whether it would trip the local gate's 15% / 5 us regression rule if
the layout change were shipped as a base/current pair.

`--widths` selects the number of leading columns of :data:`SCHEMA` (4, 8 and 16
are the interesting points: the per-frame overhead and the whole-group decode
cost scale with the column count). `--correlated` builds columns derived from
`k` instead: the writer's 8% size gate must then refuse the chunked layout, and
the report records that the two "layouts" are byte-identical — that is the
protection which keeps a flip from hurting correlated tables.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

import numpy as np

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

# (name, SQL, kind). `{lo}` is a fixed literal here: the harness compares two
# layouts on identical data, so a changing literal is not needed (the result
# cache is disabled on every client).
SHAPES = (
    ("agg_full_count", "SELECT COUNT(*) AS n FROM {table} WHERE k >= 0", "warm"),
    ("agg_partial_count", "SELECT COUNT(*) AS n FROM {table} WHERE k >= 500000", "warm"),
    ("agg_full_sum", "SELECT SUM(f1) AS s FROM {table} WHERE k >= 0", "warm"),
    ("agg_partial_sum", "SELECT SUM(f1) AS s FROM {table} WHERE k >= 500000", "warm"),
    ("count_col_partial", "SELECT COUNT(f1) AS n FROM {table} WHERE k >= 500000", "warm"),
    ("window_proj3", "SELECT k, i1, f1 FROM {table} LIMIT 100", "warm"),
    ("window_k", "SELECT k FROM {table} LIMIT 100", "warm"),
    ("window_star", "SELECT * FROM {table} LIMIT 100", "warm"),
    ("window_offset", "SELECT * FROM {table} LIMIT 10 OFFSET 1", "warm"),
    ("scan_single_col", "SELECT k FROM {table}", "warm"),
    ("str_eq_low_card", "SELECT COUNT(*) AS n FROM {table} WHERE s1 = 'g007'", "warm"),
    ("str_eq_high_card", "SELECT COUNT(*) AS n FROM {table} WHERE s3 = 'payload-007-xxxxxxxx'", "warm"),
    ("group_by_string", "SELECT s1, COUNT(*) AS n FROM {table} GROUP BY s1", "warm"),
    ("point_lookup", "SELECT * FROM {table} WHERE _id = {mid}", "warm"),
    ("full_scan", "SELECT * FROM {table}", "warm"),
    ("cold_k_limit", "SELECT k FROM {table} LIMIT 100", "cold"),
    ("cold_star_limit", "SELECT * FROM {table} LIMIT 100", "cold"),
    ("cold_point_first_rg", "SELECT * FROM {table} WHERE _id = 1", "cold"),
)

# Sparse-delete aggregate runs last because it mutates the table.
DELETED_ROWS = 3
SPARSE_DELETE_SHAPE = ("sparse_delete_sum", "SELECT SUM(k) AS s FROM {table}")

GATE_RELATIVE = 0.15
GATE_ABSOLUTE_MS = 0.005


def generate_rows(rows, correlation, seed=20261030):
    """Canonical data plus the values the sparse-delete shape expects.

    ``correlation`` derives most columns from ``k``: the per-column frames then
    lose the cross-column redundancy a single frame exploits, so the writer's
    8% size gate refuses the chunked layout.
    """
    rng = np.random.default_rng(seed)
    if correlation:
        k = np.arange(rows, dtype=np.int64)
        data = {
            "k": k.tolist(),
            "i1": (k * 17 % 977).tolist(),
            "i2": (k * 31 % 101).tolist(),
            "i3": (k * 7919 % 100_003).tolist(),
            "f1": (k * 0.5).tolist(),
            "f2": (k % 97 * 0.25).tolist(),
            "f3": (k % 53 * 0.125).tolist(),
            "s1": [f"g{int(value) % 50:03d}" for value in k],
            "s2": [f"k{int(value) % 1000:04d}" for value in k],
            "s3": [f"payload-{int(value) % 37:03d}-xxxxxxxx" for value in k],
            "s4": [f"c{int(value) % 10}" for value in k],
            "b1": [bool(value % 2 == 0) for value in k],
            "n1": [None if value % 7 == 0 else int(value) for value in k],
            "u1": (k % 65536).tolist(),
            "t1": (1_600_000_000 + k).tolist(),
            "c2": (k % 3).tolist(),
        }
    else:
        data = {
            "k": rng.integers(0, 1_000_000, rows).tolist(),
            "i1": rng.integers(0, 10_000, rows).tolist(),
            "i2": rng.integers(0, 1_000, rows).tolist(),
            "i3": rng.integers(0, 1_000_000, rows).tolist(),
            "f1": rng.random(rows).tolist(),
            "f2": (rng.random(rows) * 10).tolist(),
            "f3": (rng.random(rows) * 2).tolist(),
            "s1": [f"g{int(value) % 50:03d}" for value in rng.integers(0, 1_000_000, rows)],
            "s2": [f"k{int(value) % 1000:04d}" for value in rng.integers(0, 1_000_000, rows)],
            "s3": [f"payload-{int(value) % 37:03d}-xxxxxxxx" for value in rng.integers(0, 1_000_000, rows)],
            "s4": [f"c{int(value) % 10}" for value in rng.integers(0, 1_000_000, rows)],
            "b1": [bool(value) for value in rng.integers(0, 2, rows)],
            "n1": [None if value % 7 == 0 else int(value) for value in rng.integers(0, rows, rows)],
            "u1": rng.integers(0, 65536, rows).tolist(),
            "t1": (1_600_000_000 + rng.integers(0, 1_000_000, rows)).tolist(),
            "c2": rng.integers(0, 3, rows).tolist(),
        }
    return data


def schema_for(width):
    names = list(SCHEMA)[:width]
    return {name: SCHEMA[name] for name in names}


def table_header(path):
    with open(path, "rb") as handle:
        head = handle.read(256 + 32)
    return {
        "version": int.from_bytes(head[8:12], "little"),
        "compression": head[256 + 28],
        "row_group_encoding": head[256 + 29],
        "id_encoding": head[256 + 30],
        "row_groups": int.from_bytes(head[84:88], "little"),
    }


def table_bytes(db_dir, table):
    return sum(
        item.stat().st_size
        for item in db_dir.glob(f"{table}*")
        if item.is_file()
    )


def _build(db_dir, table, schema, data, chunked):
    from apexbase.client import ApexClient

    client = ApexClient(db_dir, drop_if_exists=False, enable_cache=False)
    try:
        client.create_table(table, schema)
        client.use_table(table)
        assert client.set_compression("zstd") is not False
        previous = os.environ.get("APEX_CHUNK_INDEX")
        os.environ["APEX_CHUNK_INDEX"] = "1" if chunked else "0"
        try:
            start = time.perf_counter()
            client.store(data)
            client.flush()
            build_ms = (time.perf_counter() - start) * 1000.0
        finally:
            if previous is None:
                os.environ.pop("APEX_CHUNK_INDEX", None)
            else:
                os.environ["APEX_CHUNK_INDEX"] = previous
    finally:
        client.close()
    header = table_header(Path(db_dir) / f"{table}.apex")
    return {
        "table": table,
        "layout_requested": "chunked" if chunked else "flat",
        "build_ms": round(build_ms, 6),
        "bytes": table_bytes(Path(db_dir), table),
        **header,
    }


def _measure(client, sql):
    warmup, iterations = 2, 7
    samples = []
    for index in range(warmup + iterations):
        if index < warmup:
            client.execute(sql)
            continue
        start = time.perf_counter()
        client.execute(sql)
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def _measure_cold(db_dir, table, sql, iterations=5):
    from apexbase.client import ApexClient

    samples = []
    for _ in range(iterations):
        client = ApexClient(str(db_dir), drop_if_exists=False, enable_cache=False)
        try:
            client.use_table(table)
            start = time.perf_counter()
            client.execute(sql)
            samples.append((time.perf_counter() - start) * 1000.0)
        finally:
            client.close()
    return statistics.median(samples)


def _round_floats(value):
    """Collapse float-reduction-order noise so both layouts compare equal."""
    if isinstance(value, float):
        return float(f"{value:.9g}")
    if isinstance(value, dict):
        return {key: _round_floats(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return tuple(_round_floats(item) for item in value)
    return value


def _result_signature(client, sql):
    """A cheap, layout-independent signature of one shape's result."""
    rows = client.execute(sql).to_dict()
    if not isinstance(rows, list):
        return repr(_round_floats(rows))
    first = _round_floats(rows[0]) if rows else None
    last = _round_floats(rows[-1]) if rows else None
    return (len(rows), repr(first), repr(last))


def run_width(rows, width, rounds, correlation, base_tmp):
    from apexbase.client import ApexClient

    schema = schema_for(width)
    data = generate_rows(rows, correlation)
    db_dir = Path(tempfile.mkdtemp(prefix=f"layoutcmp_{width}_", dir=str(base_tmp)))
    mid = rows // 2 + 1
    result = {
        "width": width,
        "rows": rows,
        "correlated": correlation,
        "variants": {},
        "shapes": {},
    }

    flat = _build(db_dir, "flat", schema, data, chunked=False)
    chunk = _build(db_dir, "chunk", schema, data, chunked=True)
    result["variants"] = {"flat": flat, "chunk": chunk}
    result["layout_taken"] = {
        "flat_encoding": flat["row_group_encoding"],
        "chunk_encoding": chunk["row_group_encoding"],
        "chunk_bytes_delta_pct": round(
            (chunk["bytes"] / flat["bytes"] - 1.0) * 100.0, 3
        ),
    }

    clients = {
        "flat": ApexClient(str(db_dir), drop_if_exists=False, enable_cache=False),
        "chunk": ApexClient(str(db_dir), drop_if_exists=False, enable_cache=False),
    }
    clients["flat"].use_table("flat")
    clients["chunk"].use_table("chunk")

    # Correctness: both layouts must answer every shape identically.
    correctness = {}
    for name, template, kind in SHAPES:
        flat_sql = template.format(table="flat", mid=mid)
        chunk_sql = template.format(table="chunk", mid=mid)
        flat_sig = _result_signature(clients["flat"], flat_sql)
        chunk_sig = _result_signature(clients["chunk"], chunk_sql)
        correctness[name] = flat_sig == chunk_sig
        if flat_sig != chunk_sig:
            raise AssertionError(
                f"width {width} shape {name}: layouts disagree ({flat_sig} vs {chunk_sig})"
            )
    result["correctness"] = correctness

    for name, template, kind in SHAPES:
        per_layout = {"flat": [], "chunk": []}
        for round_index in range(rounds):
            order = ("flat", "chunk") if round_index % 2 == 0 else ("chunk", "flat")
            for layout in order:
                sql = template.format(table=layout, mid=mid)
                if kind == "cold":
                    median_ms = _measure_cold(db_dir, layout, sql)
                else:
                    median_ms = _measure(clients[layout], sql)
                per_layout[layout].append(median_ms)
        aggregate = {}
        for layout, samples in per_layout.items():
            aggregate[layout] = {
                "median_ms": round(statistics.median(samples), 6),
                "min_ms": round(min(samples), 6),
                "max_ms": round(max(samples), 6),
                "rounds": [round(value, 6) for value in samples],
            }
        flat_ms = aggregate["flat"]["median_ms"]
        chunk_ms = aggregate["chunk"]["median_ms"]
        delta_pct = (chunk_ms / flat_ms - 1.0) * 100.0 if flat_ms > 0 else 0.0
        delta_ms = chunk_ms - flat_ms
        would_flag = delta_pct > GATE_RELATIVE * 100.0 and delta_ms > GATE_ABSOLUTE_MS
        result["shapes"][name] = {
            "kind": kind,
            "flat_ms": flat_ms,
            "chunk_ms": chunk_ms,
            "delta_pct": round(delta_pct, 2),
            "delta_ms": round(delta_ms, 6),
            "would_flag_as_regression": would_flag,
            **aggregate,
        }

    # Sparse-delete aggregate last: it mutates both tables.
    delete_sql = (
        f"DELETE FROM {{table}} WHERE k >= 5000 AND k < {5000 + DELETED_ROWS}"
    )
    clients["flat"].execute(delete_sql.format(table="flat"))
    clients["chunk"].execute(delete_sql.format(table="chunk"))
    name, template = SPARSE_DELETE_SHAPE
    flat_sig = _result_signature(
        clients["flat"], template.format(table="flat", mid=mid)
    )
    chunk_sig = _result_signature(
        clients["chunk"], template.format(table="chunk", mid=mid)
    )
    if flat_sig != chunk_sig:
        raise AssertionError(f"sparse delete aggregate disagrees: {flat_sig} vs {chunk_sig}")
    per_layout = {"flat": [], "chunk": []}
    for round_index in range(rounds):
        order = ("flat", "chunk") if round_index % 2 == 0 else ("chunk", "flat")
        for layout in order:
            per_layout[layout].append(
                _measure(clients[layout], template.format(table=layout, mid=mid))
            )
    aggregate = {
        layout: {
            "median_ms": round(statistics.median(samples), 6),
            "rounds": [round(value, 6) for value in samples],
        }
        for layout, samples in per_layout.items()
    }
    flat_ms = aggregate["flat"]["median_ms"]
    chunk_ms = aggregate["chunk"]["median_ms"]
    delta_pct = (chunk_ms / flat_ms - 1.0) * 100.0 if flat_ms > 0 else 0.0
    result["shapes"][name] = {
        "kind": "warm",
        "flat_ms": flat_ms,
        "chunk_ms": chunk_ms,
        "delta_pct": round(delta_pct, 2),
        "delta_ms": round(chunk_ms - flat_ms, 6),
        "would_flag_as_regression": (
            delta_pct > GATE_RELATIVE * 100.0
            and (chunk_ms - flat_ms) > GATE_ABSOLUTE_MS
        ),
        **aggregate,
    }

    for client in clients.values():
        client.close()
    return result


def run_comparison(rows, widths, rounds, correlation=False, workdir=None):
    base_tmp = Path(tempfile.mkdtemp(prefix="layoutcmp_", dir=workdir))
    try:
        return {
            "rows": rows,
            "widths": widths,
            "rounds": rounds,
            "correlated": correlation,
            "results": [
                run_width(rows, width, rounds, correlation, base_tmp)
                for width in widths
            ],
        }
    finally:
        import shutil

        shutil.rmtree(base_tmp, ignore_errors=True)


def _print_table(report):
    for width_result in report["results"]:
        width = width_result["width"]
        layout = width_result["layout_taken"]
        print(
            f"\n== {width} columns, {width_result['rows']} rows"
            f" (correlated={width_result['correlated']}) =="
        )
        print(
            f"flat encoding={layout['flat_encoding']} "
            f"chunk encoding={layout['chunk_encoding']} "
            f"bytes delta={layout['chunk_bytes_delta_pct']:+.2f}% "
            f"build flat/chunk={width_result['variants']['flat']['build_ms']:.0f}/"
            f"{width_result['variants']['chunk']['build_ms']:.0f} ms"
        )
        print(f"{'shape':24s} {'kind':5s} {'flat_ms':>10s} {'chunk_ms':>10s} {'delta':>9s} {'abs_ms':>9s}  flag")
        for name, shape in width_result["shapes"].items():
            print(
                f"{name:24s} {shape['kind']:5s} {shape['flat_ms']:10.4f} "
                f"{shape['chunk_ms']:10.4f} {shape['delta_pct']:+8.2f}% "
                f"{shape['delta_ms']:+9.4f}"
                f"  {'REGRESSION' if shape['would_flag_as_regression'] else ''}"
            )


def _summary(report):
    wins = losses = neutral = 0
    flagged = []
    for width_result in report["results"]:
        for name, shape in width_result["shapes"].items():
            if shape["delta_pct"] <= -5.0:
                wins += 1
            elif shape["would_flag_as_regression"]:
                losses += 1
                flagged.append(f"{width_result['width']}c:{name}")
            else:
                neutral += 1
    return {"wins_over_5pct": wins, "gate_regressions": losses, "neutral": neutral, "flagged": flagged}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--widths", type=str, default="4,8,16")
    parser.add_argument("--rounds", type=int, default=7)
    parser.add_argument(
        "--correlated",
        action="store_true",
        help="derive columns from k so the writer's 8%% gate refuses the chunked layout",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.rows <= 0 or args.rounds <= 0:
        parser.error("rows and rounds must be positive")
    try:
        widths = [int(value) for value in args.widths.split(",")]
    except ValueError:
        parser.error("--widths must be comma-separated integers")
    if any(width <= 0 or width > len(SCHEMA) for width in widths):
        parser.error(f"--widths entries must be in 1..{len(SCHEMA)}")

    report = run_comparison(args.rows, widths, args.rounds, correlation=args.correlated)
    report["summary"] = _summary(report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    _print_table(report)
    print(f"\nsummary: {json.dumps(report['summary'])}")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    raise SystemExit(main())
