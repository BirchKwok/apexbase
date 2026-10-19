"""`COUNT(col)` must not read the value column.

The fused filtered-aggregation lane used to decode every value of the aggregate
column to accumulate sum/min/max (and, for bit-packed columns, to bulk-decode the
packed payload), even when the statement only asked for `COUNT(col)`. On a covering
range that made `COUNT(v)` cost ~30x `COUNT(*)` (1M rows: 2.19 ms against 0.07 ms):
the value payload was read for no reason. The caller now sends a per-column
"value aggregate needed" plan, so a count-only column is answered from its
validity bitmap; the value payload is never decoded.

These tests pin both sides: the counts stay exact on every lane (covering,
partial, NULLs, deletes, string columns, multi-predicate), and the covering /
partial count stays in the same cost band as `COUNT(*)` instead of the value
aggregate's band.
"""
from __future__ import annotations

import statistics
import tempfile
import time

from apexbase.client import ApexClient

ROWS = 300_000


def _median_ms(client, sql, iterations=5):
    """Best of three per-run medians.

    These guards compare a count against a value aggregate; a single median can
    land on a busy slice of the host (the suite runs on shared machines) and
    invert the order. Taking the best of three medians still catches a structural
    decode (the two shapes then cost the same in every group) while ignoring a
    transient scheduler stall.
    """
    run = lambda: client.execute(sql)  # noqa: E731
    for _ in range(2):
        run()
    best = None
    for _ in range(3):
        samples = []
        for _ in range(iterations):
            start = time.perf_counter()
            run()
            samples.append((time.perf_counter() - start) * 1000.0)
        median = statistics.median(samples)
        best = median if best is None else min(best, median)
    return best


def _value(client, sql, key="n"):
    return client.execute(sql).to_dict()[0][key]


def test_a_covering_count_column_costs_what_count_star_costs():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "v": "int64"})
        client.use_table("t")
        client.store({"k": list(range(ROWS)), "v": list(range(ROWS))})
        client.flush()
        try:
            assert _value(client, "SELECT COUNT(v) AS n FROM t WHERE k >= 0") == ROWS
            covering_star = _median_ms(client, "SELECT COUNT(*) AS n FROM t WHERE k >= 0")
            covering_value = _median_ms(client, "SELECT COUNT(v) AS n FROM t WHERE k >= 0")
            covering_sum = _median_ms(client, "SELECT SUM(v) AS n FROM t WHERE k >= 0")
            # The value payload must not be touched: the count belongs to COUNT(*)'s
            # band, not to SUM(v)'s. The wide absolute term keeps this from failing on
            # scheduling noise while still catching the ~20-30x payload decode.
            assert covering_value < covering_star * 6 + 0.3, (
                covering_value, covering_star)
            assert covering_value < covering_sum / 4 + 0.2, (
                covering_value, covering_sum)

            # The plan is aligned like the requested columns: a leading COUNT(*)
            # must not turn the SUM into a count.
            row = client.execute(
                "SELECT COUNT(*) AS c, SUM(v) AS s FROM t WHERE k >= 0").to_dict()[0]
            assert row["c"] == ROWS, row
            assert row["s"] == ROWS * (ROWS - 1) // 2, row

            half = ROWS // 2
            assert _value(
                client, f"SELECT COUNT(v) AS n FROM t WHERE k >= {half}") == ROWS - half
            partial_star = _median_ms(
                client, f"SELECT COUNT(*) AS n FROM t WHERE k >= {half}")
            partial_value = _median_ms(
                client, f"SELECT COUNT(v) AS n FROM t WHERE k >= {half}")
            assert partial_value < partial_star * 6 + 0.3, (partial_value, partial_star)
        finally:
            client.close()


def test_count_column_stays_exact_with_nulls_deletes_and_mixed_aggregates():
    rows = 60_000
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "v": "int64"})
        client.use_table("t")
        client.store({"k": list(range(rows)),
                      "v": [None if i % 3 == 0 else i for i in range(rows)]})
        client.flush()
        non_null = lambda start: len([i for i in range(start, rows) if i % 3 != 0])  # noqa: E731
        try:
            # Covering: the validity bitmap is subtracted from the live row count.
            assert _value(client, "SELECT COUNT(v) AS n FROM t WHERE k >= 0") == non_null(0)
            # Partially covering: the per-row loop still skips the value payload.
            assert _value(client, "SELECT COUNT(v) AS n FROM t WHERE k >= 30000") == non_null(30000)
            # Multi-predicate takes the cached lane's count-only plan.
            assert _value(
                client,
                "SELECT COUNT(v) AS n FROM t WHERE k >= 0 AND k <= 60000") == non_null(0)
            # A value aggregate beside a count-only one must keep both exact.
            row = client.execute(
                "SELECT COUNT(v) AS c, SUM(v) AS s, AVG(v) AS a, MIN(v) AS mn, MAX(v) AS mx "
                "FROM t WHERE k >= 30000").to_dict()[0]
            values = [i for i in range(30000, rows) if i % 3 != 0]
            assert row["c"] == len(values), row
            assert row["s"] == sum(values), row
            assert abs(row["a"] - sum(values) / len(values)) < 1e-9, row
            assert (row["mn"], row["mx"]) == (min(values), max(values)), row

            # Deleted rows leave both the count and the value aggregate.
            client.execute("DELETE FROM t WHERE k < 1000")
            client.flush()
            expected = [i for i in range(1000, rows) if i % 3 != 0]
            row = client.execute(
                "SELECT COUNT(v) AS c, SUM(v) AS s FROM t WHERE k >= 0").to_dict()[0]
            assert row["c"] == len(expected), row
            assert row["s"] == sum(expected), row
        finally:
            client.close()


def test_a_string_value_column_can_be_counted_under_a_numeric_filter():
    """Count-only needs no numeric payload, so a string column qualifies too."""
    rows = 50_000
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "s": "string"})
        client.use_table("t")
        client.store({"k": list(range(rows)),
                      "s": [None if i % 7 == 0 else f"v{i % 31}" for i in range(rows)]})
        client.flush()
        live = lambda start: len([i for i in range(start, rows) if i % 7 != 0])  # noqa: E731
        try:
            assert _value(client, "SELECT COUNT(s) AS n FROM t WHERE k >= 0") == live(0)
            assert _value(client, "SELECT COUNT(s) AS n FROM t WHERE k >= 25000") == live(25000)
            # `COUNT(*)` and `COUNT(s)` differ exactly by the NULL rows.
            row = client.execute(
                "SELECT COUNT(*) AS all_rows, COUNT(s) AS n_values FROM t WHERE k >= 0"
            ).to_dict()[0]
            assert row["all_rows"] == rows, row
            assert row["n_values"] == live(0), row
        finally:
            client.close()


def test_a_multi_predicate_count_does_not_decode_the_value_column():
    rows = 200_000
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "v": "int64"})
        client.use_table("t")
        client.store({"k": list(range(rows)), "v": list(range(rows))})
        client.flush()
        try:
            sql_count = ("SELECT COUNT(v) AS n FROM t "
                         f"WHERE k >= 0 AND k <= {rows}")
            sql_sum = ("SELECT SUM(v) AS n FROM t "
                       f"WHERE k >= 0 AND k <= {rows}")
            assert _value(client, sql_count) == rows
            row = client.execute(
                "SELECT COUNT(*) AS c, SUM(v) AS s FROM t "
                f"WHERE k >= 0 AND k <= {rows}").to_dict()[0]
            assert row["c"] == rows, row
            assert row["s"] == rows * (rows - 1) // 2, row
            counted = _median_ms(client, sql_count)
            summed = _median_ms(client, sql_sum)
            # The cached lane materializes the predicate column either way; a count
            # must still stay far below the value aggregate's band.
            assert counted < summed / 2 + 0.5, (counted, summed)
        finally:
            client.close()


def test_a_string_filter_count_column_skips_the_value_payload():
    """The string-equality lane has the same count-only plan as the numeric one."""
    rows = 200_000
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "v": "int64", "s": "string"})
        client.use_table("t")
        client.store({"k": list(range(rows)), "v": list(range(rows)),
                      "s": [f"c{i % 10}" for i in range(rows)]})
        client.flush()
        try:
            matching = len([i for i in range(rows) if i % 10 == 3])
            assert _value(client, "SELECT COUNT(v) AS n FROM t WHERE s = 'c3'") == matching
            # The Python client's pre-parse string-filter path.
            star = _median_ms(client, "SELECT COUNT(*) AS n FROM t WHERE s = 'c3'")
            counted = _median_ms(client, "SELECT COUNT(v) AS n FROM t WHERE s = 'c3'")
            summed = _median_ms(client, "SELECT SUM(v) AS n FROM t WHERE s = 'c3'")
            assert counted < star * 4 + 0.2, (counted, star)
            assert counted < summed / 2 + 0.2, (counted, summed)
            # The executor lane (a quoted identifier bypasses the Python
            # classifier) keeps its `*` rewrite for a NULL-free column.
            assert _value(
                client, "SELECT COUNT(v) AS n FROM t WHERE \"s\" = 'c3'") == matching
        finally:
            client.close()


def test_a_string_filter_count_column_stays_exact_with_nulls_and_deletes():
    rows = 120_000
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "v": "int64", "s": "string"})
        client.use_table("t")
        client.store({"k": list(range(rows)),
                      "v": [None if i % 3 == 0 else i for i in range(rows)],
                      "s": [f"c{i % 10}" for i in range(rows)]})
        client.flush()
        live = lambda start: len([i for i in range(start, rows)  # noqa: E731
                                  if i % 10 == 3 and i % 3 != 0])
        try:
            # The plain client path is served by the storage string lane.
            assert _value(client, "SELECT COUNT(v) AS n FROM t WHERE s = 'c3'") == live(0)
            # The executor lane also has to exclude the NULLs (nullable column).
            assert _value(
                client, "SELECT COUNT(v) AS n FROM t WHERE \"s\" = 'c3'") == live(0)
            # Mixed aggregates: the plan stays aligned like the columns.
            row = client.execute(
                "SELECT COUNT(*) AS c, COUNT(v) AS cv FROM t WHERE s = 'c3'"
            ).to_dict()[0]
            assert row["c"] == len([i for i in range(rows) if i % 10 == 3]), row
            assert row["cv"] == live(0), row
            # Value aggregates beside count-only ones stay exact.
            row = client.execute(
                "SELECT COUNT(v) AS c, SUM(v) AS s, MIN(v) AS mn, MAX(v) AS mx "
                "FROM t WHERE s = 'c3'").to_dict()[0]
            values = [i for i in range(rows) if i % 10 == 3 and i % 3 != 0]
            assert row["c"] == len(values), row
            assert row["s"] == sum(values), row
            assert (row["mn"], row["mx"]) == (min(values), max(values)), row

            client.execute("DELETE FROM t WHERE k < 100")
            client.flush()
            expected = [i for i in range(100, rows) if i % 10 == 3 and i % 3 != 0]
            row = client.execute(
                "SELECT COUNT(v) AS c, SUM(v) AS s FROM t WHERE s = 'c3'").to_dict()[0]
            assert row["c"] == len(expected), row
            assert row["s"] == sum(expected), row
        finally:
            client.close()
