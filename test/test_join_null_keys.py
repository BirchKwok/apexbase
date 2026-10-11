"""Join behaviour for NULL keys, and the cost of an expression ON clause.

Two defects met here. SQL equality with NULL is *unknown*, so a NULL key must
never match -- not even another NULL -- but a NULL key was read as 0 in the Int64
probe paths (joining a real zero key) and `arrays_equal_at` reported NULL = NULL as
equal. Separately, an ON clause that is not a plain column equality fell back to a
nested loop, so a LEFT JOIN that matched nothing cost O(left x right): 404 ms at
100k x 1k rows against 1.3 ms once the expression is evaluated into a key column
and hash joined.
"""

import statistics
import tempfile
import time

from apexbase.client import ApexClient

LEFT_ROWS = [None, 0, 2]
RIGHT_ROWS = [(0, "zero"), (2, "two"), (None, "nulltag")]


def _client(tmp):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("ln", schema={"k": "int64"})
    client.use_table("ln")
    client.store({"k": LEFT_ROWS})
    client.flush()
    client.create_table("rn", schema={"k": "int64", "tag": "string"})
    client.use_table("rn")
    client.store({"k": [row[0] for row in RIGHT_ROWS],
                  "tag": [row[1] for row in RIGHT_ROWS]})
    client.flush()
    client.use_table("ln")
    return client


def test_a_null_key_never_matches():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            inner = client.execute(
                "SELECT ln.k AS lk, rn.tag FROM ln JOIN rn ON ln.k = rn.k"
            ).to_dict()
            assert {row["lk"] for row in inner} == {0, 2}, inner

            left = client.execute(
                "SELECT ln.k AS lk, rn.tag FROM ln LEFT JOIN rn ON ln.k = rn.k"
            ).to_dict()
            assert len(left) == 3, left
            assert [row["tag"] for row in left if row["lk"] is None] == [None], left

            right = client.execute(
                "SELECT ln.k AS lk, rn.tag FROM ln RIGHT JOIN rn ON ln.k = rn.k"
            ).to_dict()
            assert len(right) == 3, right
            assert "nulltag" in {row["tag"] for row in right}, right

            full = client.execute(
                "SELECT ln.k AS lk, rn.tag FROM ln FULL JOIN rn ON ln.k = rn.k"
            ).to_dict()
            # Both NULL rows survive as unmatched rows, and they stay separate.
            assert len(full) == 4, full
            assert [row["tag"] for row in full].count("nulltag") == 1, full
            assert [row["tag"] for row in full].count(None) == 1, full
        finally:
            client.close()


def test_an_expression_on_clause_keeps_null_semantics():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            # `ln.k + 1000000` is NULL for the NULL key, so nothing matches and all
            # three left rows are null-extended.
            left = client.execute(
                "SELECT ln.k AS lk, rn.tag FROM ln LEFT JOIN rn "
                "ON ln.k + 1000000 = rn.k"
            ).to_dict()
            assert len(left) == 3, left
            assert {row["tag"] for row in left} == {None}, left

            inner = client.execute(
                "SELECT ln.k AS lk, rn.tag FROM ln JOIN rn ON ln.k + 2 = rn.k"
            ).to_dict()
            assert [row["lk"] for row in inner] == [0], inner
        finally:
            client.close()


def test_an_expression_left_join_is_not_a_nested_loop():
    """A no-match expression LEFT JOIN must not cost the product of both sides.

    Before the expression was evaluated into a key column, this shape measured
    50k x 500 rows as ~100 ms against ~0.5 ms for a plain-key join of the same
    size that matched nothing, because it fell through to the nested loop.
    """
    rows, dimension = 50_000, 500
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        try:
            client.create_table("big", schema={"k": "int64"})
            client.use_table("big")
            client.store({"k": list(range(rows))})
            client.flush()
            # Keys far above the left side: a plain-key join that matches nothing.
            client.create_table("plain_dim", schema={"k": "int64"})
            client.use_table("plain_dim")
            client.store({"k": [1_000_000 + i for i in range(dimension)]})
            client.flush()
            # Keys below the left side: the expression join also matches nothing.
            client.create_table("expr_dim", schema={"k": "int64"})
            client.use_table("expr_dim")
            client.store({"k": list(range(dimension))})
            client.flush()
            client.use_table("big")

            def median_ms(sql, iterations=5):
                for _ in range(2):
                    client.execute(sql)
                samples = []
                for _ in range(iterations):
                    start = time.perf_counter()
                    client.execute(sql)
                    samples.append((time.perf_counter() - start) * 1000.0)
                return statistics.median(samples)

            plain = median_ms(
                "SELECT COUNT(*) AS c FROM big b LEFT JOIN plain_dim d ON b.k = d.k"
            )
            expression = median_ms(
                "SELECT COUNT(*) AS c FROM big b LEFT JOIN expr_dim d "
                "ON b.k + 1000000 = d.k"
            )
            assert expression < plain * 20 + 5, (plain, expression)
        finally:
            client.close()
