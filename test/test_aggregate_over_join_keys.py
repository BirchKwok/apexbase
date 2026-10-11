"""Aggregates over a join key must resolve the side they name.

The RIGHT-JOIN swap keeps the key it consumed only under its qualified name, so
`SELECT MIN(a.k) FROM t a RIGHT JOIN t b ON a.k = b.k` failed with "Column: k". The
aggregate resolver now tries the reference as written before the bare column, which
also stops a self-join from collapsing `a.k` and `b.k` onto one column.

Two neighbouring gaps are *not* covered here because they predate this change and are
tracked separately: a same-named column on both sides resolves to the left one
(`SELECT u.id` returns the left table's `id`), and the right table's key is dropped from
INNER/LEFT join output, so referencing it fails with "Column: <key>".
"""
from __future__ import annotations

import tempfile

import pytest

from apexbase.client import ApexClient

# users: 1,2,3 plus an unmatched 9. orders: five rows over users 1..3, so a RIGHT JOIN
# has one null-extended row whose left key is NULL while the right key is 9. Column
# names differ per table to stay clear of the same-name defect noted above.
ORDERS = {"oid": [1, 2, 3, 4, 5], "user_id": [1, 2, 1, 3, 2]}


def _client(tmp):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("users", schema={"uid": "int64"})
    client.use_table("users")
    client.store({"uid": [1, 2, 3, 9]})
    client.flush()
    client.create_table("orders", schema={"oid": "int64", "user_id": "int64"})
    client.use_table("orders")
    client.store(ORDERS)
    client.flush()
    return client


def test_a_right_join_aggregate_resolves_both_keys():
    """Both keys of a RIGHT JOIN are addressable, including the null-extended row."""
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            row = client.execute(
                "SELECT COUNT(o.user_id) AS ca, COUNT(u.uid) AS cb, "
                "SUM(o.user_id) AS sa, SUM(u.uid) AS sb, MIN(o.user_id) AS ma, "
                "MIN(u.uid) AS mb, MAX(o.user_id) AS xa, MAX(u.uid) AS xb "
                "FROM orders o RIGHT JOIN users u ON o.user_id = u.uid"
            ).to_dict()[0]
            # The null-extended user 9 has a NULL left key but a right key of 9.
            assert (row["ca"], row["cb"]) == (5, 6), row
            assert (row["sa"], row["sb"]) == (9, 18), row
            assert (row["ma"], row["mb"]) == (1, 1), row
            assert (row["xa"], row["xb"]) == (3, 9), row
        finally:
            client.close()


@pytest.mark.parametrize("join_type", ["LEFT JOIN", "RIGHT JOIN"])
def test_an_aggregate_over_the_left_key_works_in_both_outer_directions(join_type):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            row = client.execute(
                f"SELECT COUNT(o.user_id) AS ca, SUM(o.user_id) AS sa, "
                f"MIN(o.user_id) AS ma FROM orders o {join_type} users u "
                f"ON o.user_id = u.uid"
            ).to_dict()[0]
            assert (row["ca"], row["sa"], row["ma"]) == (5, 9, 1), row
        finally:
            client.close()


def test_an_aggregate_over_one_key_of_a_self_join_works():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "v": "int64"})
        client.use_table("t")
        client.store({"k": [1, 2, 3], "v": [10, 20, 30]})
        client.flush()
        try:
            row = client.execute(
                "SELECT MIN(a.k) AS ak, MIN(b.k) AS bk, MAX(a.k) AS ax, SUM(b.k) AS sb "
                "FROM t a RIGHT JOIN t b ON a.k = b.k"
            ).to_dict()[0]
            assert (row["ak"], row["bk"], row["ax"], row["sb"]) == (1, 1, 3, 6), row
        finally:
            client.close()


def test_bare_and_qualified_aggregate_references_still_work():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "v": "int64"})
        client.use_table("t")
        client.store({"k": [1, 2, 3], "v": [10, 20, 30]})
        client.flush()
        try:
            row = client.execute(
                "SELECT MIN(k) AS m, SUM(v) AS s, AVG(v) AS a, COUNT(v) AS n, "
                "MIN(t.k) AS q FROM t"
            ).to_dict()[0]
            assert row["m"] == 1 and row["s"] == 60 and row["n"] == 3, row
            assert abs(row["a"] - 20.0) < 1e-9 and row["q"] == 1, row
        finally:
            client.close()
