"""Quantify the write-then-read (HTAP) overlay gain for fused aggregations.

Any single-row `store()` lands in an overlay (buffered rows, or the append delta
after a flush) instead of a persisted row group, and the fused read lanes used to
stand down entirely while one exists: a filtered aggregation over a 200k-row base
with 1% appended rows fell back to a full merged materialization (~6.7x the clean
latency, the amplification recorded in the V5 research report).

The fused filtered-aggregation lane now folds the buffered appended rows itself,
so the same query keeps its fast path and the amplification collapses to ~1.1x.
This phase makes that gain visible base/current, and asserts every result against
an independent computation over the generated rows so a lane that silently skips
the appended rows (or drops their NULLs) fails the benchmark instead of reporting
a fast wrong answer.

Metric rows are emitted in the guard's report format (`query` + `ApexBase`), so
`benchmarks/compare_perf_baseline.py` and
`benchmarks/run_local_perf_guard.py --mode full` consume them directly. Clean
latencies and the amplification ratios stay in the informational block: they are
equal work on both sides, and pinning an unpruned full scan would turn sampling
noise into a regression flag.
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

# Fraction of the base row count appended before the measured queries run.
APPEND_FRACTION = 0.01

# (metric name, SQL). `{table}` is the fixture table, `{lo}` a literal that
# changes per timed iteration (the client result cache is off, but a changing
# predicate keeps a stale cache from ever helping) and `{hi}` a fixed bound.
# Every predicate matches appended rows, so the overlay has to be folded.
GATE_SHAPES = (
    (
        "filtered aggregation (count+sum)",
        "SELECT count(*) AS n, sum(b) AS sb FROM {table} WHERE a >= {lo}",
    ),
    (
        "filtered aggregation (min+max)",
        "SELECT min(b) AS mn, max(b) AS mx FROM {table} WHERE a >= {lo}",
    ),
)

# Measured while a delete and a cell update are both *pending* (not flushed).
# The merged read path keeps the window over the active view in that state, so it
# forced `(0, None)` into the physical reader for every request: a zero-row schema
# probe materialized the whole table (58.6 ms against 0.018 ms on a clean
# 200k x 16 plain table) and the point lane, which proves an absent id through
# exactly that probe, turned `_id = <missing>`/`_id = <deleted>` into full scans.
PENDING_SHAPES = (
    (
        "absent id point lookup (pending delete+update)",
        "SELECT a, b FROM {table} WHERE _id = 999999999",
        0,
    ),
    (
        "bounded read limit 0 (pending delete+update)",
        "SELECT a, b FROM {table} LIMIT 0",
        0,
    ),
    (
        # The window must be pushed into the reader while cells are pending: this
        # shape read the whole table and sliced (57.5 ms against 0.15 ms on a
        # 200k x 16 plain table) until pending cells stopped disabling it.
        "bounded read limit 100 (pending delete+update)",
        "SELECT a, b FROM {table} LIMIT 100",
        100,
    ),
)

# Measured after a *persisted* DELETE. The active row space no longer starts at
# the first physical row, and these are pure prefix reads: they must stay bounded
# instead of materializing the table. Before the window was pushed into the
# physical reader they cost 15.28 ms (`LIMIT 0`) and 15.39 ms (`LIMIT 100`) on a
# 200k-row plain table, against 0.017 ms and 0.13 ms with no deletion vector --
# a 117x-888x cliff that no other gate shape could see, because no other gate
# fixture carries a deletion vector.
DELETION_SHAPES = (
    ("bounded read limit 0 (after DELETE)", "SELECT a, b FROM {table} LIMIT 0", 0),
    ("bounded read limit 100 (after DELETE)", "SELECT a, b FROM {table} LIMIT 100", 100),
    # The sidecar statistics describe every physical row, so a sparse deletion
    # vector only needs the deleted rows subtracted. Without that the mere
    # existence of one deleted row dropped the aggregate from the statistics read
    # to a whole-table scan (200k rows: 0.0297 ms -> 1.4545 ms).
    (
        "aggregate after a sparse delete",
        "SELECT sum(b) AS sb FROM {table}",
        None,
    ),
    # A deletion vector must not move a grouped aggregation off its cached lane:
    # the lane's dictionary covers the file's physical rows while the row counters
    # already exclude the deleted ones, so comparing with them rejected the lane and
    # the statement fell to the generic route (200k rows: 0.21 ms -> 8.4 ms).
    (
        "group by after a sparse delete",
        "SELECT s, count(*) AS n FROM {table} GROUP BY s ORDER BY n DESC LIMIT 10",
        None,
    ),
)

# Informational: the same shapes on the clean base (equal work on both sides,
# so pinning them would invite sampling noise) plus a two-predicate shape whose
# fused lane only has a cached path, which an overlay cannot use.
CONTROL_SHAPES = (
    ("clean filtered aggregation (count+sum)", GATE_SHAPES[0][1]),
    ("clean filtered aggregation (min+max)", GATE_SHAPES[1][1]),
    (
        "clean filtered count (two predicates)",
        "SELECT count(*) AS n FROM {table} WHERE a >= {lo} AND b < {hi}",
    ),
    (
        "filtered count (two predicates, informational)",
        "SELECT count(*) AS n FROM {table} WHERE a >= {lo} AND b < {hi}",
    ),
)


def oracle_for(sql_template):
    """(row predicate, value predicate) for one gate/control shape."""
    if sql_template.startswith("SELECT count(*) AS n, sum(b)"):
        return lambda a, b: a >= 1000, lambda a, b: a >= 1000 and b is not None
    if sql_template.startswith("SELECT min(b) AS mn, max(b) AS mx"):
        return (
            lambda a, b: a >= 1000 and b is not None,
            lambda a, b: a >= 1000 and b is not None,
        )
    return (
        lambda a, b: a >= 1000 and b is not None and b < 1900,
        None,
    )

def generate_rows(rows):
    """Same shape as the canary dataset: two numeric columns and one string."""
    return {
        "a": list(range(rows)),
        "b": [i % 977 for i in range(rows)],
        "s": [f"v{i % 50}" for i in range(rows)],
    }


def expected(row_predicate, value_predicate, appended):
    """(matching rows, non-null sum, min, max) for one shape.

    `row_predicate` counts rows (`COUNT(*)`), `value_predicate` is the same
    predicate restricted to rows whose value column is not NULL.
    """
    values = (
        [b for a, b in appended if value_predicate(a, b)]
        if value_predicate is not None
        else []
    )
    return (
        len([1 for a, b in appended if row_predicate(a, b)]),
        sum(values),
        min(values) if values else None,
        max(values) if values else None,
    )


def measure(client, sql_template, warmup, iterations):
    """Median of `iterations` runs with a fresh predicate literal each time."""
    samples = []
    template = sql_template.replace("{table}", "t")
    for index in range(warmup + iterations):
        sql = template.format(lo=1000 + index, hi=1900)
        if index < warmup:
            client.execute(sql)
            continue
        start = time.perf_counter()
        client.execute(sql)
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def check(client, sql_template, rows, appended):
    """Assert the query agrees with an independent computation over the rows."""
    sql = sql_template.replace("{table}", "t").format(lo=1000, hi=1900)
    got = client.execute(sql).to_dict()[0]
    row_predicate, value_predicate = oracle_for(sql_template)
    want_rows, want_sum, want_min, want_max = expected(row_predicate, value_predicate, appended)
    if "n" in got and "sb" in got:
        assert got["n"] == want_rows, (sql, got, want_rows)
        assert abs(got["sb"] - want_sum) < 1e-6, (sql, got, want_sum)
    elif "mn" in got:
        assert got["mn"] == want_min, (sql, got, want_min)
        assert got["mx"] == want_max, (sql, got, want_max)
    else:
        assert got["n"] == want_rows, (sql, got, want_rows)


def run_htap(rows, warmup, iterations):
    full_bench.ensure_optional_imports()
    if not full_bench.HAS_APEXBASE:
        raise RuntimeError("ApexBase is not importable; run maturin develop --release first")

    data = generate_rows(rows)
    appended = [(a, b) for a, b in zip(data["a"], data["b"])]
    tmpdir = Path(tempfile.mkdtemp(prefix="apexbase_htap_"))
    db_dir = tmpdir / "db"
    detail = {"rows": rows, "appended_rows": 0, "shapes": {}, "controls": {}}
    results = []
    try:
        db_dir.mkdir(parents=True, exist_ok=True)
        client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)
        client.create_table("t", schema={"a": "int64", "b": "int64", "s": "string"})
        client.use_table("t")
        client.store(data)
        client.flush()

        # Clean controls first, on the persisted base alone.
        for name, sql in CONTROL_SHAPES[:3]:
            check(client, sql, rows, appended)
            detail["controls"][name] = {
                "warm_ms": round(measure(client, sql, warmup, iterations), 6)
            }

        # Append 1% buffered rows: some match the predicates, some carry NULLs.
        extra = max(rows // int(1 / APPEND_FRACTION), 1)
        for i in range(extra):
            b = None if i % 3 == 0 else i
            client.store({"a": rows + i, "b": b, "s": "x"})
            appended.append((rows + i, b))
        detail["appended_rows"] = extra

        for name, sql in GATE_SHAPES:
            check(client, sql, rows, appended)
            warm_ms = measure(client, sql, warmup, iterations)
            results.append(
                {
                    "category": "ApexBase HTAP overlay",
                    "query": f"HTAP: {name} (+1% appended)",
                    "ApexBase": round(warm_ms, 6),
                }
            )
            detail["shapes"][name] = {"warm_ms": round(warm_ms, 6), "appended": True}

        # A persisted deletion vector must not disable the bounded-read lanes.
        client.execute("DELETE FROM t WHERE _id = 1")
        client.flush()
        sparse_oracle = client.execute("SELECT b, s FROM t").to_dict()
        for name, sql, expected_rows in DELETION_SHAPES:
            got = client.execute(sql.replace("{table}", "t")).to_dict()
            if expected_rows is None and sql.strip().startswith("SELECT s,"):
                # GROUP BY: the surviving rows per label must match the scan. Ties in
                # `ORDER BY n DESC` are unordered, so compare labels and counts as
                # sets rather than as a sequence.
                counts: dict = {}
                for row in sparse_oracle:
                    counts[row["s"]] = counts.get(row["s"], 0) + 1
                # `ORDER BY n DESC` leaves ties unordered, so check the counts taken
                # as a multiset and every returned label's own count.
                top_counts = sorted(counts.values(), reverse=True)[:10]
                assert len(got) == 10, (sql, len(got))
                assert sorted(row["n"] for row in got) == top_counts, (sql, got, top_counts)
                for row in got:
                    assert counts.get(row["s"]) == row["n"], (sql, row, counts.get(row["s"]))
            elif expected_rows is None:
                # Aggregate: check the value against an independent scan of `b`.
                want = sum(row["b"] for row in sparse_oracle if row["b"] is not None)
                assert got and abs(got[0]["sb"] - want) < 1e-6, (sql, got[:1], want)
            else:
                assert len(got) == expected_rows, (sql, len(got), expected_rows)
                if expected_rows:
                    assert [row["a"] for row in got] == list(range(1, 1 + expected_rows)), sql
            warm_ms = measure(client, sql, warmup, iterations)
            results.append(
                {
                    "category": "ApexBase HTAP overlay",
                    "query": f"HTAP: {name}",
                    "ApexBase": round(warm_ms, 6),
                }
            )
            detail["shapes"][name] = {"warm_ms": round(warm_ms, 6), "deleted_rows": 1}

        # A pending delete plus a pending cell update must keep the zero-row probe
        # and the absent-id point lookup on the bounded lanes (see PENDING_SHAPES).
        # Ids chosen to sit outside every later check's window: past the first
        # 100 rows (the persisted-delete shapes below describe those) and below
        # `a >= 1000` (the informational two-predicate control counts those).
        client.execute("DELETE FROM t WHERE _id = 500")
        client.execute("UPDATE t SET b = 1 WHERE _id = 600")
        # Oracle for the windowed shapes: the surviving prefix of `a` as the full
        # scan reports it (earlier sections already deleted rows from the front).
        widest = max(expected for _, _, expected in PENDING_SHAPES)
        prefix_oracle = [
            row["a"] for row in client.execute("SELECT a FROM t").to_dict()
        ][:widest]
        for name, sql, expected_rows in PENDING_SHAPES:
            got = client.execute(sql.replace("{table}", "t")).to_dict()
            assert len(got) == expected_rows, (sql, len(got), expected_rows)
            if expected_rows:
                assert [row["a"] for row in got] == prefix_oracle[:expected_rows], sql
            warm_ms = measure(client, sql, warmup, iterations)
            results.append(
                {
                    "category": "ApexBase HTAP overlay",
                    "query": f"HTAP: {name}",
                    "ApexBase": round(warm_ms, 6),
                }
            )
            detail["shapes"][name] = {"warm_ms": round(warm_ms, 6), "pending_cells": True}
        client.flush()

        # The two-predicate shape keeps its own informational row: its fused lane
        # needs the cached path, which an overlay cannot use yet.
        for name, sql in CONTROL_SHAPES[2:]:
            if "informational" not in name:
                continue
            check(client, sql, rows, appended)
            detail["controls"][name] = {
                "warm_ms": round(measure(client, sql, warmup, iterations), 6),
                "appended": True,
            }

        # Informational: how much the overlay costs relative to the clean read.
        for index, (name, _) in enumerate(GATE_SHAPES):
            control = CONTROL_SHAPES[index][0]
            clean_ms = detail["controls"][control]["warm_ms"]
            overlay_ms = detail["shapes"][name]["warm_ms"]
            detail["shapes"][name]["amplification"] = (
                round(overlay_ms / clean_ms, 4) if clean_ms > 0 else None
            )

        # A range delete that covers whole row groups. The aggregate lane walks
        # row groups and checks the deletion bitmap per row, so a fully deleted
        # group used to cost a full scalar pass (200k rows: deleting 199999 took
        # `sum` 0.34 ms against 0.13 ms once those groups are skipped). Measured
        # last so it cannot change the fixture the shapes above describe.
        client.use_table("t")
        client.execute("DELETE FROM t WHERE _id <= 150000")
        client.flush()
        surviving = client.execute("SELECT b FROM t").to_dict()
        want_sum = sum(row["b"] for row in surviving if row["b"] is not None)
        got = client.execute("SELECT count(*) AS n, sum(b) AS sb FROM t").to_dict()[0]
        assert got["n"] == len(surviving), (got["n"], len(surviving))
        assert abs(got["sb"] - want_sum) < 1e-6, (got["sb"], want_sum)
        for label, sql in (
            ("aggregate after a range delete", "SELECT sum(b) AS sb FROM t"),
        ):
            warm_ms = measure(client, sql, warmup, iterations)
            results.append(
                {
                    "category": "ApexBase HTAP overlay",
                    "query": f"HTAP: {label}",
                    "ApexBase": round(warm_ms, 6),
                }
            )
            detail["shapes"][label] = {"warm_ms": round(warm_ms, 6), "range_deleted": True}

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

    results, detail = run_htap(args.rows, args.warmup, args.iterations)
    report = {
        **full_bench.get_report_metadata("apexbase-canary"),
        "config": {
            "rows": args.rows,
            "warmup": args.warmup,
            "iterations": args.iterations,
        },
        "results": results,
        "htap_overlay": detail,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    raise SystemExit(main())
