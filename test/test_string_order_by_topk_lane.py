"""`ORDER BY <string column> LIMIT k` needs no full sort.

A dictionary-encoded string column has no numeric value for the mmap top-k lane to
rank, so ordering one fell back to materialising and sorting every row: on 200k rows
`ORDER BY city DESC LIMIT 5` cost 10.3 ms against 0.34 ms for the numeric shape -- 30x.

The dictionary is small, so the lane ranks each row group's dictionary once, takes that
group's top-k by rank, and merges the already-ordered per-group lists. No row sort and
no value materialisation. A group whose sort column holds NULLs declines the query
(a rank cannot express NULLS FIRST/LAST), as does a non-dictionary column -- the
general path answers those correctly.

The cost guard below compares against the numeric spelling of the same shape, since an
absolute budget would depend on the machine. The general path is ~30x the lane, so the
bound has real teeth.
"""

import statistics
import tempfile
import time

import pytest

from apexbase.client import ApexClient

ROWS = 30_000
DISTINCT = 7  # deliberately tie-heavy


def _client(tmp, *, compressed, groups):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"k": "int64", "city": "string"})
    client.use_table("t")
    if compressed:
        assert client.set_compression(compressed) is not False
    chunk = max(1, ROWS // groups)
    for start in range(0, ROWS, chunk):
        end = min(ROWS, start + chunk)
        client.store({"k": list(range(start, end)),
                      "city": [f"c{i % DISTINCT}" for i in range(start, end)]})
        client.flush()
    return client


def _key(value):
    return (value is None, "" if value is None else value)


def _median_ms(client, sql, iterations=7):
    fn = (lambda s: (lambda: client.execute(s)))(sql)
    for _ in range(2):
        fn()
    samples = []
    for _ in range(iterations):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


@pytest.mark.parametrize("compressed", [None, "zstd"])
@pytest.mark.parametrize("groups", [1, 5])
def test_string_order_by_limit_matches_a_full_sort(compressed, groups):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, compressed=compressed, groups=groups)
        try:
            expected = sorted((f"c{i % DISTINCT}" for i in range(ROWS)), key=_key)
            for descending in (False, True):
                direction = "DESC" if descending else "ASC"
                ordered = sorted(expected, key=_key, reverse=descending)
                for limit, offset in ((1, 0), (5, 0), (3, 4), (DISTINCT + 3, 0)):
                    sql = (f"SELECT city FROM t ORDER BY city {direction} "
                           f"LIMIT {limit} OFFSET {offset}")
                    got = [row["city"] for row in client.execute(sql).to_dict()]
                    want = ordered[offset:offset + limit]
                    # Ties may come back in any order, so compare as multisets.
                    assert len(got) == len(want), (sql, len(got), len(want))
                    assert sorted(got, key=_key) == sorted(want, key=_key), sql
        finally:
            client.close()


def test_deleted_rows_do_not_enter_the_ranking():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, compressed=None, groups=1)
        try:
            # Drop the lexicographically largest value entirely.
            client.execute(f"DELETE FROM t WHERE city = 'c{DISTINCT - 1}'")
            client.flush()
            rows = [row["city"] for row in client.execute(
                "SELECT city FROM t ORDER BY city DESC LIMIT 100"
            ).to_dict()]
            assert rows, "expected rows"
            assert all(value != f"c{DISTINCT - 1}" for value in rows)
            assert max(rows, key=_key) == f"c{DISTINCT - 2}"
        finally:
            client.close()


def test_a_null_sort_value_falls_back_and_stays_correct():
    """The lane cannot rank a NULL, so it declines and the general path answers.

    The engine orders NULLs first for DESC and last for ASC, matching the default the
    general path implements.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "city": "string"})
        client.use_table("t")
        values = [None if i % 11 == 0 else f"c{i % 5}" for i in range(1000)]
        client.store({"k": list(range(1000)), "city": values})
        client.flush()
        try:
            for descending in (False, True):
                ordered = sorted(values, key=_key, reverse=descending)
                want = ordered[:10]
                got = [row["city"] for row in client.execute(
                    "SELECT * FROM t ORDER BY city "
                    f"{'DESC' if descending else 'ASC'} LIMIT 10"
                ).to_dict()]
                assert len(got) == len(want)
                assert got == want, (descending, got[:3], want[:3])
                assert (got[0] is None) == descending
        finally:
            client.close()


def test_the_string_lane_beats_the_fallback_for_a_non_dictionary_column():
    """A dictionary column uses the lane; a unique (plain) string column cannot.

    Comparing the two spellings on the *same* table keeps the guard machine-independent.
    The fallback materialises and sorts every row (~30x the lane), so the bound has teeth.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "city": "string", "uniq": "string"})
        client.use_table("t")
        rows = 200_000
        client.store({"k": list(range(rows)),
                      "city": [f"c{i % 7}" for i in range(rows)],
                      "uniq": [f"u{i:07d}" for i in range(rows)]})
        client.flush()
        try:
            lane = _median_ms(client, "SELECT * FROM t ORDER BY city DESC LIMIT 5")
            fallback = _median_ms(client, "SELECT * FROM t ORDER BY uniq DESC LIMIT 5")
            # Sanity: both agree on the shape (5 rows), then the cost bound.
            assert len(client.execute(
                "SELECT * FROM t ORDER BY city DESC LIMIT 5").to_dict()) == 5
            assert lane < fallback / 4 + 0.5, (lane, fallback)
        finally:
            client.close()


def test_a_projected_order_by_uses_the_fast_lane():
    """An explicit projection must not disqualify the lane.

    It used to: only `SELECT *` reached the fast path, so `SELECT city ... ORDER BY
    city DESC LIMIT 5` cost 15.7 ms against 0.30 ms for the star form. The lane reads
    every column of the k chosen rows and the SELECT list is applied afterwards, which
    this pins by checking the projected schema too.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "city": "string", "uniq": "string"})
        client.use_table("t")
        rows = 200_000
        client.store({"k": list(range(rows)),
                      "city": [f"c{i % 7}" for i in range(rows)],
                      "uniq": [f"u{i:07d}" for i in range(rows)]})
        client.flush()
        try:
            lane = _median_ms(client, "SELECT city FROM t ORDER BY city DESC LIMIT 5")
            fallback = _median_ms(client, "SELECT uniq FROM t ORDER BY uniq DESC LIMIT 5")
            assert lane < fallback / 4 + 0.5, (lane, fallback)
            projected = client.execute(
                "SELECT city FROM t ORDER BY city DESC LIMIT 5").to_dict()
            assert len(projected) == 5
            assert list(projected[0].keys()) == ["city"]
            # Still the arithmetic mean of the requested arithmetic mean shape: the star
            # form keeps the full row.
            assert len(client.execute(
                "SELECT * FROM t ORDER BY city DESC LIMIT 5").to_dict()[0]) == 3
        finally:
            client.close()


def test_limit_offset_is_applied_exactly_once():
    """The fast path used to skip the offset *and* let the caller apply it.

    `ORDER BY k LIMIT 4 OFFSET 3` therefore returned one row starting at the seventh
    element. The lane now returns `limit + offset` rows and leaves the slicing to the
    caller, for the numeric and the string lane alike.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "city": "string"})
        client.use_table("t")
        rows = 20_000
        client.store({"k": list(range(rows)), "city": [f"c{i % 7}" for i in range(rows)]})
        client.flush()
        try:
            for direction, want in (
                ("ASC", list(range(3, 7))),
                ("DESC", list(range(rows - 4, rows - 8, -1))),
            ):
                got = [row["k"] for row in client.execute(
                    f"SELECT k FROM t ORDER BY k {direction} LIMIT 4 OFFSET 3"
                ).to_dict()]
                assert got == want, (direction, got, want)
                starred = [row["k"] for row in client.execute(
                    f"SELECT * FROM t ORDER BY k {direction} LIMIT 4 OFFSET 3"
                ).to_dict()]
                assert starred == want, (direction, starred)
            # The string lane returns a page of the requested size too.
            for direction in ("ASC", "DESC"):
                page = [row["city"] for row in client.execute(
                    f"SELECT city FROM t ORDER BY city {direction} LIMIT 4 OFFSET 3"
                ).to_dict()]
                assert len(page) == 4, (direction, page)
                assert page == sorted(page, reverse=(direction == "DESC"))
        finally:
            client.close()


# --- plain (high-cardinality) string columns -------------------------------------

PLAIN_ROWS = 300_000  # spans many 256-value storage blocks


def _plain_client(tmp, *, compressed, groups):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"k": "int64", "uniq": "string"})
    client.use_table("t")
    if compressed:
        assert client.set_compression(compressed) is not False
    chunk = max(1, PLAIN_ROWS // groups)
    for start in range(0, PLAIN_ROWS, chunk):
        end = min(PLAIN_ROWS, start + chunk)
        client.store({"k": list(range(start, end)),
                      "uniq": [f"u{i:07d}" for i in range(start, end)]})
        client.flush()
    return client


@pytest.mark.parametrize("compressed", [None, "zstd"])
@pytest.mark.parametrize("groups", [1, 5])
def test_a_high_cardinality_string_orders_correctly_in_both_directions(compressed, groups):
    """A unique string column is not dictionary-encoded; its lane is ascending-only.

    The values are stored in a chain of 256-value blocks, so this also covers the block
    walk. Ascending uses the lane, descending deliberately stays on the previous path
    (the bounded heap degenerates to O(n*k) when the values arrive in the opposite
    order).
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = _plain_client(tmp, compressed=compressed, groups=groups)
        try:
            expected = sorted((f"u{i:07d}" for i in range(PLAIN_ROWS)))
            for limit, offset in ((1, 0), (5, 0), (10, 7), (100, 0)):
                got = [row["uniq"] for row in client.execute(
                    f"SELECT uniq FROM t ORDER BY uniq ASC LIMIT {limit} OFFSET {offset}"
                ).to_dict()]
                assert got == expected[offset:offset + limit], (limit, offset)
            descending = [row["uniq"] for row in client.execute(
                "SELECT uniq FROM t ORDER BY uniq DESC LIMIT 5"
            ).to_dict()]
            assert descending == expected[::-1][:5]

            # Deleted rows must disappear from both directions.
            client.execute("DELETE FROM t WHERE k < 1000")
            client.flush()
            live = expected[1000:]
            assert [row["uniq"] for row in client.execute(
                "SELECT uniq FROM t ORDER BY uniq ASC LIMIT 5").to_dict()] == live[:5]
            assert [row["uniq"] for row in client.execute(
                "SELECT uniq FROM t ORDER BY uniq DESC LIMIT 5").to_dict()] == live[::-1][:5]
        finally:
            client.close()


def test_a_null_in_a_plain_string_column_falls_back_safely():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "s": "string"})
        client.use_table("t")
        client.store({"k": list(range(5000)),
                      "s": [None if i % 7 == 0 else f"v{i:05d}" for i in range(5000)]})
        client.flush()
        try:
            got = [row["s"] for row in client.execute(
                "SELECT s FROM t ORDER BY s ASC LIMIT 5").to_dict()]
            assert got == sorted(f"v{i:05d}" for i in range(5000) if i % 7 != 0)[:5]
        finally:
            client.close()


def test_a_monotone_plain_column_takes_the_order_preserving_fast_path():
    """Values already in ascending order need no selection at all.

    The lane verifies monotonicity with an early exit on the first inversion, so
    unsorted columns keep the bounded-heap path and pay nothing for the check. Both
    spellings must agree with the oracle.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "sorted": "string", "mixed": "string"})
        client.use_table("t")
        rows = 200_000
        ordered = [f"u{i:07d}" for i in range(rows)]
        # `mixed` is the same data with a single inversion, which must defeat the check.
        mixed = list(ordered)
        mixed[rows // 2], mixed[rows // 2 + 1] = mixed[rows // 2 + 1], mixed[rows // 2]
        client.store({"k": list(range(rows)), "sorted": ordered, "mixed": mixed})
        client.flush()
        try:
            for limit, offset in ((5, 0), (17, 3)):
                want = sorted(ordered)[offset:offset + limit]
                for column in ("sorted", "mixed"):
                    got = [row[column] for row in client.execute(
                        f"SELECT {column} FROM t ORDER BY {column} ASC "
                        f"LIMIT {limit} OFFSET {offset}"
                    ).to_dict()]
                    assert got == want, (column, limit, offset)
            # The monotone column returns in storage order, so the page is exact.
            assert [row["sorted"] for row in client.execute(
                "SELECT sorted FROM t ORDER BY sorted ASC LIMIT 3").to_dict()] == ordered[:3]
        finally:
            client.close()
