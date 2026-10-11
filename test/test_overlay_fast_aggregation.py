"""The fused filtered-aggregation lane must fold buffered appended rows.

A single-row `store()` keeps the row in the write buffer, and the fused
aggregation lanes used to stand down as soon as any overlay existed: the query
then materialized and merged the whole base (a 7-8x amplification). The lane now
folds those buffered rows itself, which is only acceptable while the base row
groups are still exactly the base rows and the buffered rows can be read with
their NULL bitmaps. These tests pin the results against an independent oracle.

Two boundaries are deliberately *not* exercised here, because they are
pre-existing engine defects unrelated to this lane (recorded in
`V5_HYBRID_STORAGE_RESEARCH.md` §9 with repro commands):

* a base `DELETE`/`UPDATE` plus buffered appends leaves the buffers in a state
  whose NULL bitmaps are lost on later reads;
* the persisted append delta loses its NULL bitmaps in aggregations, so
  `count(b)`/`min(b)` count NULL cells as zeros.
"""

import shutil
import tempfile

from apexbase import ApexClient

ROWS = 40_000
APPENDED = 400


def _fixture():
    root = tempfile.mkdtemp(prefix="apexbase_overlay_agg_")
    client = ApexClient(root, drop_if_exists=True, enable_cache=False)
    client.create_table("t", schema={"a": "int64", "b": "int64", "s": "string"})
    client.use_table("t")
    base = {
        "a": list(range(ROWS)),
        "b": [i % 977 for i in range(ROWS)],
        "s": [f"v{i % 50}" for i in range(ROWS)],
    }
    client.store(base)
    client.flush()
    rows = [(base["a"][i], base["b"][i]) for i in range(ROWS)]
    return root, client, rows


def _append(client, rows, count=APPENDED, base=ROWS):
    for i in range(count):
        b = None if i % 3 == 0 else i
        client.store({"a": base + i, "b": b, "s": "x"})
        rows.append((base + i, b))


def _oracle(rows, row_predicate, value_predicate):
    """(matching rows, non-null sum, min, max) for one predicate pair."""
    values = [b for a, b in rows if value_predicate(a, b) and b is not None]
    return {
        "n": len([1 for a, b in rows if row_predicate(a, b)]),
        "sb": sum(values),
        "mn": min(values) if values else None,
        "mx": max(values) if values else None,
    }


SHAPES = (
    (
        "SELECT count(*) AS n, sum(b) AS sb, min(b) AS mn, max(b) AS mx "
        "FROM t WHERE a > 500",
        lambda a, b: a > 500,
    ),
    (
        "SELECT count(*) AS n, sum(b) AS sb FROM t WHERE a >= 40000",
        lambda a, b: a >= 40000,
    ),
    (
        "SELECT count(*) AS n, min(b) AS mn, max(b) AS mx FROM t WHERE a > 40040",
        lambda a, b: a > 40040,
    ),
    (
        "SELECT sum(b) AS sb, avg(b) AS av FROM t WHERE a BETWEEN 39000 AND 40300",
        lambda a, b: 39000 <= a <= 40300,
    ),
)


def _assert_agrees(client, rows):
    for sql, predicate in SHAPES:
        got = client.execute(sql).to_dict()[0]
        want = _oracle(rows, predicate, predicate)
        for key, value in want.items():
            if key in got and value is not None:
                assert abs(got[key] - value) < 1e-6, (sql, key, got[key], value)


def test_buffered_appends_are_folded_by_the_fused_lane():
    root, client, rows = _fixture()
    try:
        _append(client, rows)
        # The lane must stay available *and* answer with the appended rows
        # included; without the fold it fell back to a full merged read.
        plan = client.execute(
            "EXPLAIN ANALYZE SELECT count(*) AS n, sum(b) AS sb FROM t WHERE a > 500"
        ).to_dict()[0]["plan"]
        path = [line.strip() for line in plan.splitlines() if "Actual Path" in line]
        assert "fast_filtered_numeric_agg" in path[0], path
        _assert_agrees(client, rows)
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_buffered_appends_keep_null_cells_out_of_aggregates():
    root, client, rows = _fixture()
    try:
        _append(client, rows)
        # A third of the appended rows carry NULL in `b`: `count(*)` must include
        # them while `count(b)`/`min`/`max` must skip them.
        got = client.execute(
            "SELECT count(*) AS n, count(b) AS nb, min(b) AS mn, max(b) AS mx "
            "FROM t WHERE a >= 40000"
        ).to_dict()[0]
        non_null = [b for a, b in rows if a >= 40000 and b is not None]
        assert got["n"] == APPENDED
        assert got["nb"] == len(non_null)
        assert got["mn"] == min(non_null)
        assert got["mx"] == max(non_null)
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_folding_survives_many_appended_batches():
    root, client, rows = _fixture()
    try:
        # Appends arrive one at a time in the write-then-read workload; several
        # rounds keep the lane correct across repeated buffer growth.
        for _ in range(3):
            _append(client, rows, count=100, base=rows[-1][0] + 1)
            _assert_agrees(client, rows)
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_persisted_delta_rows_stay_visible_to_count():
    root, client, rows = _fixture()
    try:
        _append(client, rows)
        client.flush()
        # After a flush the rows live in the base or the append delta; either way
        # `count(*)` must see them.
        got = client.execute("SELECT count(*) AS n FROM t WHERE a >= 40000").to_dict()[0]
        assert got["n"] == APPENDED
        got = client.execute("SELECT count(*) AS n FROM t WHERE a > 500").to_dict()[0]
        assert got["n"] == len([1 for a, _ in rows if a > 500])
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)
