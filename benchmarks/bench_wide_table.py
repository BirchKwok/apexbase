"""Wide-table x multi-row-group x compression coverage.

The canary and the public benchmark both use narrow three-to-five column tables,
while the V5 storage work targets wide rows: projection cost scales with the
number of materialized columns, row-group count multiplies the per-group header
and zone-map work, and compression changes both the I/O volume and the decode
cost. This phase builds a 16-column table (plus a zstd-compressed twin) over
several row groups and reports a latency row per shape, so the guard samples
them base/current like any other metric.

Every shape is asserted against an independent computation over the generated
rows: a wide projection that silently drops a column, a row-group boundary that
loses its last rows, or a compressed scan that returns zeros fails the benchmark
instead of reporting a fast wrong answer. Layout facts (row-group count, bytes on
disk, bytes per row) stay in the informational block, since they are equal work
on both sides and pinning them would invite sampling noise.

Warm shapes cannot see how much a compressed row group decodes per touch: after
the first call the engine's decompression cache serves the body and only the
requested rows are decoded. The `(cold reopen)` shapes therefore reopen the
client before every timed call, which drops that cache and charges each call the
full decode of the row group it touches. That is the metric a compressed row
group's granularity shows up in.
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

# 16 declared columns; the two string columns with 50 and 1000 distinct values
# also cover the low-cardinality dictionary path.
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
COLUMN_COUNT = len(SCHEMA)

# A 100-id primary-key batch. The batch lane has its own storage path (one
# directory lookup per row group, then per-column extraction) and its own set of
# declines, so it needs its own shape rather than riding on the single-row point
# lookup: a compressed row group used to be declined one line before the lane
# decompressed it, which sent the whole statement to the SQL executor (measured
# 350 us against 48 us for the plain twin at 100 ids, and 330 us against 9.5 us
# for a three-column projection of the same ids). The ids are fixed so the gate
# compares the same rows on both sides.
BATCH_ID_VALUES = tuple(1000 + index * 7 for index in range(100))
BATCH_IDS = ", ".join(str(value) for value in BATCH_ID_VALUES)
BATCH_SHAPE = (
    "batch lookup by _id IN (100 ids)",
    "SELECT * FROM {table} WHERE _id IN (" + BATCH_IDS + ")",
)
# The same batch with one id that was never written. `_id IN (...)` is a filter,
# so the missing id only removes its own row -- but the binding's batch branch
# required `row_count == ids.len()` and declined the whole statement, which sent
# it back through the SQL executor to re-parse, re-plan and scan: on a 200k x 16
# plain table 1000 ids with one missing cost 3.71 ms against 0.43 ms when every
# id exists (8.7x), and 1.23 against 0.43 ms (2.9x) on the zstd twin.
BATCH_MISSING_ID_VALUES = BATCH_ID_VALUES[:-1] + (BATCH_ID_VALUES[-1] + 5_000_000,)
BATCH_MISSING_SHAPE = (
    "batch lookup by _id IN (100 ids, 1 missing)",
    "SELECT * FROM {table} WHERE _id IN ("
    + ", ".join(str(value) for value in BATCH_MISSING_ID_VALUES)
    + ")",
)

# (metric suffix, SQL). `{table}` is the fixture table, `{lo}` a literal that
# changes per timed iteration (the result cache is off, but a changing predicate
# keeps a stale cache from ever helping) and `{mid}` a fixed point-lookup key.
PLAIN_SHAPES = (
    ("full scan (16 cols)", "SELECT * FROM {table}"),
    ("projection (3 cols)", "SELECT k, i1, f1 FROM {table}"),
    (
        "projection (8 cols)",
        "SELECT k, i1, i2, i3, f1, f2, s1, b1 FROM {table}",
    ),
    (
        "order by int limit 100",
        "SELECT k FROM {table} ORDER BY i3 DESC, k ASC LIMIT 100",
    ),
    ("point lookup by _id", "SELECT * FROM {table} WHERE _id = {mid}"),
    ("scan limit 100", "SELECT * FROM {table} LIMIT 100"),
    # A page that is not the first one. Handing the offset straight to the reader
    # used to cost a full table read (200k rows, 16 columns: 9.6 ms against
    # 0.018 ms once the covered prefix is read and sliced).
    ("scan limit 10 offset 1", "SELECT * FROM {table} LIMIT 10 OFFSET 1"),
    # An ON clause that is not a plain column equality used to fall back to a
    # nested loop, so this shape cost the product of both row counts (100k x 1k
    # rows: 404 ms against 1.3 ms once the expression is evaluated into a key
    # column and hash joined). It matches nothing: the worst case.
    (
        "join count (expression ON, no match)",
        "SELECT COUNT(*) AS c FROM {table} b LEFT JOIN wide_dim d "
        "ON b.k + 100000000 = d.k",
    ),
    BATCH_SHAPE,
    BATCH_MISSING_SHAPE,
)

# Sub-millisecond fused/group-by shapes on the *uncompressed* fixture swing by up
# to 29% between identical runs (the same family the scoreboard's GROUP BY
# metrics live in), so they are measured, asserted and reported but not gated.
# Their compressed twin is 20x slower and stable, which is the version worth
# pinning: a compressed scan that loses its fused lane is a real regression.
INFORMATIONAL_SHAPES = (
    (
        "filtered aggregation",
        "SELECT count(*) AS n, sum(i1) AS s, avg(f1) AS a FROM {table} WHERE k >= {lo}",
    ),
    ("group by string", "SELECT s1, count(*) AS n FROM {table} GROUP BY s1"),
)

# Compression changes decode and I/O volume, so the compressed twin repeats the
# scan-centric shapes and the point path rather than the whole list.
COMPRESSED_SHAPES = (
    PLAIN_SHAPES[0],
    PLAIN_SHAPES[1],
    PLAIN_SHAPES[2],
    INFORMATIONAL_SHAPES[0],
    PLAIN_SHAPES[4],
    # A compressed row group is decoded whole, so the two "touch almost
    # nothing" shapes are the ones that expose it. `Wide: scan limit 100`
    # already covered the plain twin; the compressed twin had no LIMIT metric
    # at all, which is how the decode-cache gap in the projection path went
    # unnoticed (see the round-61 record).
    PLAIN_SHAPES[5],
    PLAIN_SHAPES[6],
    PLAIN_SHAPES[7],
    BATCH_SHAPE,
    BATCH_MISSING_SHAPE,
)

# "Touch almost nothing" shapes measured with a fresh client per iteration, so
# the engine's in-process decompression cache cannot hide the cost of decoding a
# row group. A compressed row group is decoded whole, so this is the metric that
# sees the group granularity itself; the warm variants above only see the decode
# of already-decompressed bytes. Measured on both variants: the plain twin is
# the control (its RCIX lanes do not decode whole groups).
# Measured *last* in each variant, because it deletes rows. A sparse deletion
# vector must not push an aggregate back to a full scan -- the sidecar statistics
# describe every physical row, so the deleted rows are subtracted instead
# (200k rows: `sum` 1.68 ms with a deletion vector against 0.12 ms with the
# correction, and 0.030 ms with no deletions at all). The deleted values sit
# strictly inside the column's range, so min/max stay recoverable too.
SPARSE_DELETED_ROWS = 3
SPARSE_DELETE_SHAPES = (
    ("aggregate after a sparse delete", "SELECT sum(k) AS s FROM {table}"),
)

COLD_SHAPES = (
    ("scan limit 100 (cold reopen)", PLAIN_SHAPES[5][1]),
    ("point lookup by _id (cold reopen)", PLAIN_SHAPES[4][1]),
)

# Point reads driven straight through the Rust SQL executor (the embedded and
# server lane) rather than the Python client's point-lookup fast path. The two
# lanes are not interchangeable: the Python route has always answered a
# compressed `_id = X` cheaply, while the executor lane used to treat the lean
# lane's "compressed row group" decline as "row absent" and fall back to a full
# scan. Measured on both variants so the compressed one is a gated metric.
EXECUTOR_SHAPES = (
    ("point lookup by _id (executor)", "SELECT * FROM {table} WHERE _id = {mid}"),
    (
        "projection point lookup (3 cols, executor)",
        "SELECT k, i1, f1 FROM {table} WHERE _id = {mid}",
    ),
)

# The row-group size is adaptive (32 KiB rows for wide tables), so 200k rows
# always span multiple groups; the phase asserts it instead of assuming it.
MIN_ROW_GROUPS = 2


def generate_rows(rows):
    """A 16-column dataset plus the aggregates its shapes are checked against."""
    data = {
        "k": list(range(rows)),
        "i1": [i % 977 for i in range(rows)],
        "i2": [i % 101 for i in range(rows)],
        "i3": [i % 7919 for i in range(rows)],
        "f1": [i * 0.5 for i in range(rows)],
        "f2": [(i % 97) * 0.25 for i in range(rows)],
        "f3": [(i % 53) * 0.125 for i in range(rows)],
        "s1": [f"g{i % 50:03d}" for i in range(rows)],
        "s2": [f"k{i % 1000:04d}" for i in range(rows)],
        "s3": [f"payload-{i % 37:03d}-{'x' * 8}" for i in range(rows)],
        "s4": [f"c{i % 10}" for i in range(rows)],
        "b1": [i % 2 == 0 for i in range(rows)],
        "n1": [None if i % 7 == 0 else i for i in range(rows)],
        "u1": [i % 65536 for i in range(rows)],
        "t1": [1_600_000_000 + i for i in range(rows)],
        "c2": [i % 3 for i in range(rows)],
    }
    expected = {
        "rows": rows,
        # Every shape below is checked against these independently of the engine.
        "sum_k": rows * (rows - 1) // 2,
        "sum_i1": sum(data["i1"]),
        "sum_i1_from_1000": sum(data["i1"][1000:]),
        "sum_f1": sum(data["f1"]),
        "max_i3": max(data["i3"]),
        "groups_s1": len(set(data["s1"])),
    }
    return data, expected


def table_bytes(db_dir: Path, table: str) -> int:
    return sum(
        path.stat().st_size
        for path in db_dir.glob(f"{table}*")
        if path.is_file()
    )


def row_group_count(db_dir: Path, table: str) -> int:
    """Row-group count from the header (`row_group_count` at byte 84)."""
    header = (db_dir / f"{table}.apex").read_bytes()[:256]
    return int.from_bytes(header[84:88], "little")


def measure(client, table, sql_template, warmup, iterations, mid):
    """Median of `iterations` runs with a fresh predicate literal each time."""
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
    """Median of `iterations` runs that each reopen the client first.

    Reopening drops every in-process cache (mmap, decompressed row groups, page
    cache), so the timed call decodes the row group it touches from scratch --
    the cost a compressed row group pays on its first touch.
    """
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


def measure_executor(client, table, sql_template, warmup, iterations, mid):
    """As :func:`measure`, but through the Rust SQL executor binding."""
    samples = []
    template = sql_template.replace("{table}", table)
    for index in range(warmup + iterations):
        sql = template.format(lo=1000 + index, mid=mid)
        if index < warmup:
            client._storage.execute(sql)
            continue
        start = time.perf_counter()
        client._storage.execute(sql)
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def check_executor(client, table, sql_template, mid):
    """Assert an executor-lane point read returns exactly the requested row."""
    sql = sql_template.replace("{table}", table).format(lo=1000, mid=mid)
    result = client._storage.execute(sql)
    columns = result.get("columns_dict") if isinstance(result, dict) else None
    if columns is None:  # pragma: no cover - the lane returns columnar lists
        rows = client.execute(sql).to_dict()
        assert len(rows) == 1 and rows[0]["k"] == mid - 1, (sql, rows)
        return
    row_count = len(next(iter(columns.values()), []))
    assert row_count == 1, (sql, row_count)
    if "_id" in columns:
        assert columns["_id"][0] == mid, (sql, columns["_id"])
    assert columns["k"][0] == mid - 1, (sql, columns.get("k"))
    if sql_template.startswith("SELECT k, i1, f1"):
        assert set(columns) == {"k", "i1", "f1"}, sorted(columns)
        assert columns["i1"][0] == (mid - 1) % 977, sql
        assert columns["f1"][0] == (mid - 1) * 0.5, sql


def check(client, table, sql_template, expected, mid):
    """Assert each shape against the independent computation."""
    sql = sql_template.replace("{table}", table).format(lo=1000, mid=mid)
    result = client.execute(sql).to_dict()
    name = sql_template
    if name.startswith("SELECT * FROM") and "_id IN (" in name:
        # One shape carries an id that was never written: it must only drop that
        # row, not the whole statement (and the surviving rows keep their values).
        wanted = (
            BATCH_MISSING_ID_VALUES[:-1]
            if str(BATCH_MISSING_ID_VALUES[-1]) in name
            else BATCH_ID_VALUES
        )
        assert len(result) == len(wanted), (sql, len(result))
        assert sorted(row["k"] for row in result) == sorted(
            value - 1 for value in wanted
        ), sql
    elif name.startswith("SELECT * FROM") and "_id" in name:
        assert len(result) == 1, (sql, result)
        assert result[0]["k"] == mid - 1, (sql, result[0]["k"], mid)
    elif name.startswith("SELECT * FROM") and "LIMIT" not in name:
        assert len(result) == expected["rows"], (sql, len(result))
    elif name.startswith("SELECT k, i1, f1"):
        assert len(result) == expected["rows"], (sql, len(result))
        assert sum(row["k"] for row in result) == expected["sum_k"], sql
    elif name.startswith("SELECT k, i1, i2, i3"):
        assert len(result) == expected["rows"], (sql, len(result))
        assert sum(row["k"] for row in result) == expected["sum_k"], sql
    elif name.startswith("SELECT sum(k) AS s FROM"):
        # The sparse-delete aggregate: rows k in [5000, 5000 + SPARSE_DELETED_ROWS).
        want = expected["sum_k"] - sum(
            range(5000, 5000 + SPARSE_DELETED_ROWS)
        )
        assert abs(result[0]["s"] - want) < 1e-6, (sql, result[0], want)
    elif name.startswith("SELECT count(*) AS n, sum(i1)"):
        row = result[0]
        # k >= 1000 keeps rows 1000..rows-1.
        kept = max(expected["rows"] - 1000, 0)
        assert row["n"] == kept, (sql, row, kept)
        assert row["s"] == expected["sum_i1_from_1000"], (sql, row)
        assert abs(row["a"] - statistics.fmean(
            range(1000, expected["rows"])
        ) * 0.5) < 1e-6, (sql, row)
    elif name.startswith("SELECT s1, count(*)"):
        assert len(result) == expected["groups_s1"], (sql, len(result))
        assert sum(row["n"] for row in result) == expected["rows"], sql
        counts = sorted(row["n"] for row in result)
        assert counts[-1] - counts[0] <= 1, (sql, counts[:3], counts[-3:])
    elif name.startswith("SELECT k FROM"):
        assert len(result) == 100, (sql, len(result))
    elif name.endswith("LIMIT 100"):
        assert len(result) == 100, (sql, len(result))
    elif name.startswith("SELECT COUNT(*) AS c FROM"):
        # The expression join matches nothing, so the count is the left row count.
        assert result == [{"c": expected["rows"]}], (sql, result, expected["rows"])
    elif name.endswith("LIMIT 10 OFFSET 1"):
        assert len(result) == 10, (sql, len(result))
        assert result[0]["k"] == 1, (sql, result[0])
    else:  # pragma: no cover - a new shape must extend this dispatcher
        raise AssertionError(f"unchecked shape: {name}")


def run_wide_table(rows, warmup, iterations, min_row_groups=MIN_ROW_GROUPS):
    full_bench.ensure_optional_imports()
    if not full_bench.HAS_APEXBASE:
        raise RuntimeError("ApexBase is not importable; run maturin develop --release first")

    data, expected = generate_rows(rows)
    mid = rows // 2 + 1
    tmpdir = Path(tempfile.mkdtemp(prefix="apexbase_wide_"))
    db_dir = tmpdir / "db"
    results = []
    detail = {
        "rows": rows,
        "columns": COLUMN_COUNT,
        "min_row_groups": MIN_ROW_GROUPS,
        "variants": {},
        "shapes": {},
    }
    try:
        db_dir.mkdir(parents=True, exist_ok=True)
        client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)
        # Small dimension for the expression-ON join shape: its keys sit below the
        # wide table's, so the join matches nothing.
        client.create_table("wide_dim", schema={"k": "int64"})
        client.use_table("wide_dim")
        client.store({"k": list(range(64))})
        client.flush()
        for label, compression in (("plain", None), ("zstd", "zstd")):
            table = f"wide_{label}"
            client.create_table(table, schema=SCHEMA)
            client.use_table(table)
            if compression:
                assert client.set_compression(compression) is not False, compression
            client.store(data)
            client.flush()

            groups = row_group_count(db_dir, table)
            assert groups >= min_row_groups, (
                f"{label}: a wide fixture must span several row groups, got {groups}"
            )
            size = table_bytes(db_dir, table)
            detail["variants"][label] = {
                "table": table,
                "compression": compression or "none",
                "row_groups": groups,
                "bytes": size,
                "bytes_per_row": round(size / rows, 4),
            }

            shapes = PLAIN_SHAPES if label == "plain" else COMPRESSED_SHAPES
            prefix = "Wide" if label == "plain" else "Wide/Zstd"
            for name, sql in shapes:
                check(client, table, sql, expected, mid)
                warm_ms = measure(client, table, sql, warmup, iterations, mid)
                metric = f"{prefix}: {name}"
                results.append(
                    {
                        "category": "ApexBase wide table",
                        "query": metric,
                        "ApexBase": round(warm_ms, 6),
                    }
                )
                detail["shapes"][metric] = {"warm_ms": round(warm_ms, 6)}
            if label == "plain":
                for name, sql in INFORMATIONAL_SHAPES:
                    check(client, table, sql, expected, mid)
                    warm_ms = measure(client, table, sql, warmup, iterations, mid)
                    detail["shapes"][f"Wide: {name} (informational)"] = {
                        "warm_ms": round(warm_ms, 6)
                    }
            for name, sql in EXECUTOR_SHAPES:
                check_executor(client, table, sql, mid)
                warm_ms = measure_executor(client, table, sql, warmup, iterations, mid)
                metric = f"{prefix}: {name}"
                results.append(
                    {
                        "category": "ApexBase wide table",
                        "query": metric,
                        "ApexBase": round(warm_ms, 6),
                    }
                )
                detail["shapes"][metric] = {"warm_ms": round(warm_ms, 6)}
            for name, sql in COLD_SHAPES:
                check(client, table, sql, expected, mid)
                cold_ms = measure_cold(db_dir, table, sql, iterations, mid)
                metric = f"{prefix}: {name}"
                results.append(
                    {
                        "category": "ApexBase wide table",
                        "query": metric,
                        "ApexBase": round(cold_ms, 6),
                    }
                )
                detail["shapes"][metric] = {"cold_ms": round(cold_ms, 6)}
            for name, sql in SPARSE_DELETE_SHAPES:
                client.execute(
                    f"DELETE FROM {table} WHERE k >= 5000 AND k < "
                    f"{5000 + SPARSE_DELETED_ROWS}"
                )
                client.flush()
                check(client, table, sql, expected, mid)
                warm_ms = measure(client, table, sql, warmup, iterations, mid)
                metric = f"{prefix}: {name}"
                results.append(
                    {
                        "category": "ApexBase wide table",
                        "query": metric,
                        "ApexBase": round(warm_ms, 6),
                    }
                )
                detail["shapes"][metric] = {
                    "warm_ms": round(warm_ms, 6),
                    "sparse_deleted_rows": SPARSE_DELETED_ROWS,
                }
            print(
                f"{label:<6} rows {rows} cols {COLUMN_COUNT} groups {groups} "
                f"bytes {size} ({size / rows:.2f} B/row)"
            )
        client.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    return results, detail


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=7)
    # The multi-row-group requirement is what the phase is for; small fixtures in
    # tests can lower it, the guard always runs the default.
    parser.add_argument("--min-row-groups", type=int, default=MIN_ROW_GROUPS)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.rows <= 0 or args.warmup < 0 or args.iterations <= 0:
        parser.error("rows and iterations must be positive; warmup must be non-negative")
    if args.min_row_groups <= 0:
        parser.error("min-row-groups must be positive")

    results, detail = run_wide_table(
        args.rows, args.warmup, args.iterations, args.min_row_groups
    )
    report = {
        **full_bench.get_report_metadata("apexbase-canary"),
        "config": {
            "rows": args.rows,
            "warmup": args.warmup,
            "iterations": args.iterations,
            "min_row_groups": args.min_row_groups,
        },
        "results": results,
        "wide_table": detail,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    raise SystemExit(main())
