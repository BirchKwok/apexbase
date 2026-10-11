"""INTERSECT / EXCEPT accept an equality predicate, not just BETWEEN.

`SELECT dict_col FROM t WHERE num_col BETWEEN lo AND hi` had a fast lane that
answers both sides as per-value counts in one storage pass. The same operation
written with `num_col = v` -- the degenerate range [v, v] -- was not recognised, so
it fell to the fully materialising generic path: on 200k rows EXCEPT took 5.0 ms and
INTERSECT 6.8 ms against 0.42 ms for the BETWEEN spelling. The UNION lane had always
accepted equality, so the two spellings now agree here as well.

A side without WHERE contributes every distinct value, which the same per-value count
answers as the *full* range on the other side's numeric column. Rows whose numeric
column is NULL would drop out of that count, and the count walks dictionary entries
only, so rows whose key is NULL would too: a bare side therefore requires both columns
to be null-free, and otherwise falls back to the generic path (correct, just slower).
"""

import statistics
import tempfile
import time

import pytest

from apexbase.client import ApexClient

ROWS = 200_000
CITIES = 30


def _client(tmp, *, compressed, indexed):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"k": "int64", "city": "string"})
    client.use_table("t")
    if compressed:
        assert client.set_compression(compressed) is not False
    client.store({"k": list(range(ROWS)),
                  "city": [f"city{i % CITIES}" for i in range(ROWS)]})
    client.flush()
    if indexed:
        client.execute("CREATE INDEX ix_k ON t (k)")
    return client


def _cities(client, sql):
    return sorted(row["city"] for row in client.execute(sql).to_dict())


def _range_set(lo, hi):
    return {f"city{i % CITIES}" for i in range(ROWS) if lo <= i <= hi}


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
@pytest.mark.parametrize("indexed", [False, True])
def test_equality_set_operations_match_between(compressed, indexed):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, compressed=compressed, indexed=indexed)
        try:
            left = _range_set(30, 59)      # BETWEEN 30 AND 59
            right = _range_set(25, 25)     # = 25
            cases = {
                "SELECT city FROM t WHERE k BETWEEN 30 AND 59 "
                "EXCEPT SELECT city FROM t WHERE k = 25":
                    sorted(left - right),
                "SELECT city FROM t WHERE k BETWEEN 30 AND 59 "
                "INTERSECT SELECT city FROM t WHERE k = 25":
                    sorted(left & right),
                "SELECT city FROM t WHERE k = 25 "
                "EXCEPT SELECT city FROM t WHERE k BETWEEN 30 AND 59":
                    sorted(right - left),
                "SELECT city FROM t WHERE k = 25 "
                "INTERSECT SELECT city FROM t WHERE k BETWEEN 30 AND 59":
                    sorted(right & left),
                "SELECT city FROM t WHERE k BETWEEN 30 AND 59 "
                "EXCEPT SELECT city FROM t WHERE k = 25 ORDER BY city":
                    sorted(left - right),
                "SELECT city FROM t WHERE 25 = k "
                "INTERSECT SELECT city FROM t WHERE k = 25":
                    sorted(right),
                # Both sides equal: EXCEPT is empty, INTERSECT keeps everything.
                "SELECT city FROM t WHERE k = 25 EXCEPT SELECT city FROM t WHERE k = 25":
                    [],
                "SELECT city FROM t WHERE k = 25 INTERSECT SELECT city FROM t WHERE k = 25":
                    sorted(right),
            }
            for sql, want in cases.items():
                assert _cities(client, sql) == want, sql
        finally:
            client.close()


def test_the_equality_spelling_stays_on_the_fast_lane():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, compressed=None, indexed=False)
        try:
            between = _median_ms(
                client,
                "SELECT city FROM t WHERE k BETWEEN 30 AND 59 "
                "EXCEPT SELECT city FROM t WHERE k BETWEEN 0 AND 24",
            )
            equality = _median_ms(
                client,
                "SELECT city FROM t WHERE k BETWEEN 30 AND 59 "
                "EXCEPT SELECT city FROM t WHERE k = 25",
            )
            # The generic path costs ~13x the lane, so this bound has real teeth.
            assert equality < between * 4 + 0.5, (between, equality)
        finally:
            client.close()


def _nullable_client(tmp, *, null_numeric, null_key):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"k": "int64", "city": "string"})
    client.use_table("t")
    client.store({
        "k": [None if (null_numeric and i % 10 == 0) else i for i in range(ROWS)],
        "city": [None if (null_key and i % 7 == 0) else f"city{i % CITIES}"
                 for i in range(ROWS)],
    })
    client.flush()
    return client


def _sorted(values):
    return sorted(values, key=lambda v: (v is None, v))


@pytest.mark.parametrize("null_numeric", [False, True])
@pytest.mark.parametrize("null_key", [False, True])
def test_a_side_without_where_matches_the_row_sets(null_numeric, null_key):
    with tempfile.TemporaryDirectory() as tmp:
        client = _nullable_client(tmp, null_numeric=null_numeric, null_key=null_key)
        try:
            rows = client.execute("SELECT k, city FROM t").to_dict()
            everything = {row["city"] for row in rows}
            at_25 = {row["city"] for row in rows if row["k"] == 25}
            cases = {
                "SELECT city FROM t EXCEPT SELECT city FROM t WHERE k = 25":
                    _sorted(everything - at_25),
                "SELECT city FROM t INTERSECT SELECT city FROM t WHERE k = 25":
                    _sorted(everything & at_25),
                "SELECT city FROM t WHERE k = 25 EXCEPT SELECT city FROM t":
                    _sorted(at_25 - everything),
                "SELECT city FROM t WHERE k = 25 INTERSECT SELECT city FROM t":
                    _sorted(at_25 & everything),
                "SELECT city FROM t EXCEPT SELECT city FROM t": [],
                "SELECT city FROM t INTERSECT SELECT city FROM t":
                    _sorted(everything),
                "SELECT city FROM t EXCEPT SELECT city FROM t WHERE k = 25 ORDER BY city":
                    _sorted(everything - at_25),
            }
            for sql, want in cases.items():
                got = _sorted(row["city"] for row in client.execute(sql).to_dict())
                assert got == want, (sql, null_numeric, null_key)
        finally:
            client.close()


def test_a_bare_side_uses_the_fast_lane_when_the_columns_are_null_free():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, compressed=None, indexed=False)
        try:
            filtered = _median_ms(
                client,
                "SELECT city FROM t WHERE k BETWEEN 30 AND 59 "
                "EXCEPT SELECT city FROM t WHERE k = 25",
            )
            bare = _median_ms(
                client,
                "SELECT city FROM t EXCEPT SELECT city FROM t WHERE k = 25",
            )
            # The generic path costs ~12x the lane, so this bound has real teeth.
            assert bare < filtered * 4 + 0.5, (filtered, bare)
        finally:
            client.close()
