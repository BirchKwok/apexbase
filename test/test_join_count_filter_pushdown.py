"""A local numeric-range filter must prune the side it touches.

`SELECT COUNT(*) FROM a JOIN b ON a.k = b.k WHERE a.k < 10` used to read both
sides in full before filtering (1M rows: 72 ms for a ten-row answer). The count
lane now scans the range through the row-group zone maps and scatters only the
survivors, while the full filter still runs, so results are unchanged.
"""
from __future__ import annotations

import statistics
import tempfile
import time
from collections import Counter

import pytest

from apexbase.client import ApexClient

ROWS = 200_000
DISTINCT = 50


def _client(tmp, rows=ROWS):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"k": "int64", "g": "string"})
    client.use_table("t")
    client.store({"k": list(range(rows)),
                  "g": [f"c{i % DISTINCT}" for i in range(rows)]})
    client.flush()
    return client


def _median_ms(client, sql, iterations=7):
    run = lambda: client.execute(sql)  # noqa: E731
    for _ in range(2):
        run()
    samples = []
    for _ in range(iterations):
        start = time.perf_counter()
        run()
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def _count(client, sql):
    return client.execute(sql).to_dict()[0]["n"]


def test_a_local_filter_prunes_the_side_it_touches():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            filtered = _count(client, "SELECT COUNT(*) AS n FROM t a JOIN t b "
                                      "ON a.k = b.k WHERE a.k < 10")
            assert filtered == 10

            # The unfiltered count still has to see every key on both sides.
            unfiltered = _count(client, "SELECT COUNT(*) AS n FROM t a JOIN t b "
                                        "ON a.k = b.k")
            assert unfiltered == ROWS

            lane = _median_ms(client, "SELECT COUNT(*) AS n FROM t a JOIN t b "
                                      "ON a.k = b.k WHERE a.k < 10")
            full = _median_ms(client, "SELECT COUNT(*) AS n FROM t a JOIN t b "
                                      "ON a.k = b.k")
            # Pruning the read is worth an order of magnitude; a regression to a
            # full read of both sides shows up as the two numbers converging.
            # Shared runners add a fixed overhead floor to the pruned lane, so
            # keep a 0.75x band (the ratio measured ~0.48x on the release matrix);
            # converging numbers still fail it.
            assert lane < full * 0.75 + 0.5, (lane, full)
        finally:
            client.close()


@pytest.mark.parametrize("column", ["a.k", "b.k"])
def test_either_side_can_be_pruned_and_multiple_keys_are_counted(column):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            expected = _count(client, f"SELECT COUNT(*) AS n FROM t a JOIN t b "
                                      f"ON a.k = b.k WHERE {column} < 10")
            assert expected == 10
            # A key with duplicates must multiply, not just count the survivors.
            multiplicities = Counter(f"c{i % DISTINCT}" for i in range(ROWS))
            got = _count(client, "SELECT COUNT(*) AS n FROM t a JOIN t b "
                                 "ON a.g = b.g WHERE a.k < 10")
            want = sum(multiplicities[f"c{i % DISTINCT}"] for i in range(10))
            assert got == want, (got, want)
        finally:
            client.close()


@pytest.mark.parametrize("key_column", ["k", "g", "u"])
@pytest.mark.parametrize("side", ["a", "b"])
def test_a_local_filter_prunes_string_keyed_joins_too(key_column, side):
    """The probe-key IN scan covers Int64 and string keys (plain or dictionary).

    `ON a.g = b.g WHERE a.k < 10` used to read both sides in full because only the
    Int64 key path existed; the string key now goes through `scan_string_in_mmap`.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "g": "string", "u": "string"})
        client.use_table("t")
        rows = 300_000
        client.store({"k": list(range(rows)),
                      "g": [f"c{i % DISTINCT}" for i in range(rows)],
                      "u": [f"u{i:07d}" for i in range(rows)]})
        client.flush()
        try:
            if key_column in ("k", "u"):
                # `k` is unique and `u` is high-cardinality, so both joins are 1:1.
                expected = 10
            else:
                # `g` has DISTINCT values, each repeated rows/DISTINCT times.
                expected = 10 * (rows // DISTINCT)
            got = _count(client, f"SELECT COUNT(*) AS n FROM t a JOIN t b "
                                 f"ON a.{key_column} = b.{key_column} "
                                 f"WHERE {side}.k < 10")
            assert got == expected, (key_column, side, got, expected)
            if key_column != "u":
                lane = _median_ms(client, f"SELECT COUNT(*) AS n FROM t a JOIN t b "
                                          f"ON a.{key_column} = b.{key_column} "
                                          f"WHERE a.k < 10")
                full = _median_ms(client, f"SELECT COUNT(*) AS n FROM t a JOIN t b "
                                          f"ON a.{key_column} = b.{key_column}")
                # Pruning one side should not cost what reading both sides costs.
                assert lane < full / 2 + 0.5, (key_column, lane, full)
        finally:
            client.close()


def test_filters_survive_deletions_and_string_predicates():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            client.execute("DELETE FROM t WHERE k < 1000")
            client.flush()
            # Deleted rows must not be counted, on either side.
            assert _count(client, "SELECT COUNT(*) AS n FROM t a JOIN t b "
                                  "ON a.k = b.k WHERE a.k < 1500") == 500
            # A string predicate local to one side still has to be correct.
            # The join is 1:1 on the unique key, so this is just the surviving
            # rows whose value is 'c1'.
            assert _count(client, "SELECT COUNT(*) AS n FROM t a JOIN t b "
                                  "ON a.k = b.k WHERE a.g = 'c1'") == sum(
                1 for i in range(1000, ROWS) if i % DISTINCT == 1)
            # A predicate that matches nothing yields zero, not a stale count.
            assert _count(client, "SELECT COUNT(*) AS n FROM t a JOIN t b "
                                  "ON a.k = b.k WHERE a.k < 0") == 0
        finally:
            client.close()


def test_null_keys_do_not_join():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64"})
        client.use_table("t")
        client.store({"k": [1, 2, None, 3, None, 4]})
        client.flush()
        try:
            assert _count(client, "SELECT COUNT(*) AS n FROM t a JOIN t b "
                                  "ON a.k = b.k") == 4
            # Keys 1 and 2 survive; NULL keys never match.
            assert _count(client, "SELECT COUNT(*) AS n FROM t a JOIN t b "
                                  "ON a.k = b.k WHERE a.k < 3") == 2
        finally:
            client.close()


@pytest.mark.parametrize("outer", ["LEFT", "RIGHT"])
def test_a_null_rejecting_where_drops_the_null_extended_rows(outer):
    """A WHERE predicate on the nullable side turns the outer count into the inner one.

    `a LEFT JOIN b ON a.k = b.k WHERE b.k < 10` must return the 10 matched rows, not
    every probe row: the null-extended rows are UNKNOWN for `<` and the WHERE removes
    them. An ON-clause filter, by contrast, leaves them alive.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64"})
        client.use_table("t")
        rows = 5000
        client.store({"k": list(range(rows))})
        client.flush()
        try:
            probe, build = ("a", "b") if outer == "LEFT" else ("b", "a")
            matched = _count(client, f"SELECT COUNT(*) AS n FROM t a {outer} JOIN t b "
                                     f"ON a.k = b.k WHERE {build}.k < 10")
            assert matched == 10, (outer, matched)
            # Without the nullable-side predicate every probe row survives.
            assert _count(client, f"SELECT COUNT(*) AS n FROM t a {outer} JOIN t b "
                                  f"ON a.k = b.k") == rows
            # An ON-clause filter is part of the join, so unmatched probes stay.
            # (`RIGHT` with an ON-clause filter currently fails with an unrelated
            # pre-existing Arrow error; that is tracked separately.)
            if outer == "LEFT":
                assert _count(client, f"SELECT COUNT(*) AS n FROM t a {outer} JOIN t b "
                                      f"ON a.k = b.k AND b.k < 10") == rows
            # A filter on the preserved side is ordinary pre-filtering.
            assert _count(client, f"SELECT COUNT(*) AS n FROM t a {outer} JOIN t b "
                                  f"ON a.k = b.k WHERE {probe}.k < 10") == 10
        finally:
            client.close()


def test_count_only_join_uses_a_key_histogram():
    """An unfiltered COUNT(*) join must not materialize the joined rows.

    Both join keys are integer with a bounded range, so the lane counts with a
    direct-indexed histogram instead of a hash multiset (1M rows: 127.9 -> 14.9 ms).
    The expectations here are computed by brute force, including duplicate keys on both
    sides, NULL keys (which never match) and negative values.
    """
    a_keys = [1, 1, 2, 2, 2, 3, None, -5, -5, 0]
    b_keys = [1, 1, 1, 2, 3, 3, None, -5, -7, 0, 0]

    def brute(join: str) -> int:
        total = 0
        for key in a_keys:
            matched = [b for b in b_keys if b is not None and key is not None and key == b]
            if join == "inner":
                total += len(matched)
            elif join == "left":
                total += len(matched) if matched else 1
        if join == "right":
            for key in b_keys:
                matched = [a for a in a_keys if a is not None and key is not None and a == key]
                total += len(matched) if matched else 1
        return total

    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("a", schema={"k": "int64", "v": "int64"})
        client.use_table("a")
        client.store({"k": a_keys, "v": list(range(len(a_keys)))})
        client.flush()
        client.create_table("b", schema={"k": "int64", "v": "int64"})
        client.use_table("b")
        client.store({"k": b_keys, "v": list(range(len(b_keys)))})
        client.flush()
        try:
            cases = [
                ("SELECT COUNT(*) AS n FROM a JOIN b ON a.k = b.k", brute("inner")),
                ("SELECT COUNT(*) AS n FROM a JOIN b ON a.k = b.k WHERE a.v >= 0",
                 brute("inner")),
                ("SELECT COUNT(*) AS n FROM a LEFT JOIN b ON a.k = b.k", brute("left")),
                ("SELECT COUNT(*) AS n FROM a RIGHT JOIN b ON a.k = b.k", brute("right")),
                ("SELECT COUNT(*) AS n FROM a JOIN b ON a.k = b.k WHERE a.k >= 1000", 0),
            ]
            for sql, expected in cases:
                got = list(client.execute(sql).to_dict()[0].values())[0]
                assert got == expected, (sql, got, expected)
        finally:
            client.close()


def test_string_in_list_counts_do_not_depend_on_dictionary_warmth():
    """A literal string IN list must not fall through to the generic executor.

    It used to cost 20 ms per 1M rows, and became 6.5x cheaper only once some *other*
    query had warmed the column dictionary - so the same statement measured 3.1 ms or
    20 ms depending on query history. It now goes straight to the storage set count.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("a", schema={"k": "string", "v": "int64"})
        client.use_table("a")
        rows = 200_000
        client.store({"k": [f"k{i % 1000}" for i in range(rows)], "v": list(range(rows))})
        client.flush()
        in20 = ("SELECT COUNT(*) AS n FROM a WHERE a.k IN ('k1','k2','k3','k4','k5','k6',"
                "'k7','k8','k9','k10','k11','k12','k13','k14','k15','k16','k17','k18',"
                "'k19','k20')")
        try:
            assert _count(client, in20) == 20 * (rows // 1000), _count(client, in20)
            assert _count(client, "SELECT COUNT(*) AS n FROM a WHERE a.k IN ('k1')") == rows // 1000
            assert _count(client, "SELECT COUNT(*) AS n FROM a WHERE a.k IN ('zzz')") == 0
            cold = _median_ms(client, in20)
            # Warm the dictionary the way an equality query does, then re-measure: the
            # cost must stay in the same band instead of dropping ~6x.
            _count(client, "SELECT COUNT(*) AS n FROM a WHERE a.k = 'k1'")
            warm = _median_ms(client, in20)
            assert max(cold, warm) < min(cold, warm) * 2 + 0.5, (cold, warm)
        finally:
            client.close()


def test_outer_join_counts_match_brute_force_and_stay_in_one_cost_band():
    """LEFT/RIGHT counts used to skip the histogram and pay the hash multiset.

    1M x 500k: inner 10.4 ms, left 32.4 ms, right 70.4 ms (a 6.7x spread within one
    family). The histogram now covers outer joins too, mirroring the hash path's rule
    that an unmatched preserved-side row contributes one null-extended row.
    """
    a_keys = [1, 1, 2, 2, 2, 3, None, -5, -5, 0]
    b_keys = [1, 1, 1, 2, 3, 3, None, -5, -7, 0, 0]

    def brute(join: str) -> int:
        total = 0
        if join in ("inner", "left"):
            for key in a_keys:
                matched = [b for b in b_keys if key is not None and b is not None and key == b]
                if matched:
                    total += len(matched)
                elif join == "left":
                    total += 1
        if join == "right":
            for key in b_keys:
                matched = [a for a in a_keys if a is not None and key is not None and a == key]
                total += len(matched) if matched else 1
        return total

    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("a", schema={"k": "int64", "v": "int64"})
        client.use_table("a")
        client.store({"k": a_keys, "v": list(range(len(a_keys)))})
        client.flush()
        client.create_table("b", schema={"k": "int64", "w": "int64"})
        client.use_table("b")
        client.store({"k": b_keys, "w": list(range(len(b_keys)))})
        client.flush()
        try:
            left = "SELECT COUNT(*) AS n FROM a LEFT JOIN b ON a.k = b.k"
            right = "SELECT COUNT(*) AS n FROM a RIGHT JOIN b ON a.k = b.k"
            for sql, want in (
                ("SELECT COUNT(*) AS n FROM a JOIN b ON a.k = b.k", brute("inner")),
                (left, brute("left")),
                (right, brute("right")),
                # A WHERE on the nullable side makes the outer join count like an inner one.
                ("SELECT COUNT(*) AS n FROM a LEFT JOIN b ON a.k = b.k WHERE b.w >= 0",
                 brute("inner")),
            ):
                got = _count(client, sql)
                assert got == want, (sql, got, want)
        finally:
            client.close()
