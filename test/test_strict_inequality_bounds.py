"""Strict inequalities must not swallow the boundary value at zero.

`col < N` is executed as the inclusive range `(-inf, prev(N)]`, where `prev(N)` is
the largest representable f64 strictly below N. That epsilon step is derived from the
*bit pattern*, and zero is the one value where the naive `from_bits(0) + 1` lands on
the wrong side: it yields +4.9e-324, the smallest **positive** number, so `k < 0`
matched `k = 0` -- one silently wrong row (and `COUNT(*)` = 1) for every such query.

The conversion was duplicated in three places and only one copy handled zero
correctly, so `COUNT(*) ... WHERE k < 0` and `GROUP BY ... WHERE k < 0` were both
wrong. All three now share one implementation.
"""

import tempfile

import pytest

from apexbase.client import ApexClient

# Spans negative, zero and positive values so every boundary is exercised.
VALUES = list(range(-50, 50))


def _client(tmp, *, compressed, indexed):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"k": "int64", "f": "float64", "g": "string"})
    client.use_table("t")
    if compressed:
        assert client.set_compression(compressed) is not False
    client.store({"k": VALUES,
                  "f": [float(v) + 0.5 for v in VALUES],
                  "g": [f"cat{v % 7}" for v in VALUES]})
    client.flush()
    if indexed:
        client.execute("CREATE INDEX ix_k ON t (k)")
    return client


def _expected(predicate):
    column, _, rest = predicate.partition(" ")
    if column == "k":
        values = [float(v) for v in VALUES]
    else:
        values = [float(v) + 0.5 for v in VALUES]
    for op, literal in [(">=", rest[2:]), ("<=", rest[2:]), (">", rest[1:]), ("<", rest[1:])]:
        if rest.startswith(op):
            bound = float(literal)
            return {
                ">=": sum(v >= bound for v in values),
                "<=": sum(v <= bound for v in values),
                ">": sum(v > bound for v in values),
                "<": sum(v < bound for v in values),
            }[op]
    raise AssertionError(predicate)


@pytest.mark.parametrize("compressed", [None, "zstd"])
@pytest.mark.parametrize("indexed", [False, True])
def test_strict_inequalities_exclude_the_boundary(compressed, indexed):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, compressed=compressed, indexed=indexed)
        try:
            for predicate in ("k < 0", "k < -1", "k < 1", "k <= 0",
                              "k > 0", "k > -1", "k >= 0",
                              "f < 0", "f < 0.5", "f > -0.5"):
                want = _expected(predicate)
                counted = client.execute(
                    f"SELECT COUNT(*) AS n FROM t WHERE {predicate}"
                ).to_dict()[0]["n"]
                assert counted == want, (predicate, counted, want)
                # The projection lane must answer the same way.
                rows = client.execute(f"SELECT k FROM t WHERE {predicate}").to_dict()
                assert len(rows) == want, (predicate, len(rows), want)
        finally:
            client.close()


def test_the_fused_group_by_lane_agrees():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, compressed=None, indexed=False)
        try:
            groups = client.execute(
                "SELECT g, COUNT(*) AS n FROM t WHERE k < 0 GROUP BY g"
            ).to_dict()
            assert len(groups) == len({f"cat{v % 7}" for v in VALUES if v < 0})
            assert sum(row["n"] for row in groups) == _expected("k < 0")
            # A conjunction over the same boundary: rows with -10 < k < 0.
            assert client.execute(
                "SELECT COUNT(*) AS n FROM t WHERE k < 0 AND k > -10"
            ).to_dict()[0]["n"] == sum(1 for v in VALUES if -10 < v < 0)
        finally:
            client.close()
