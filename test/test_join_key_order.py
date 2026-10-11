"""The `ON` clause may name the right table's key first.

`FROM users u LEFT JOIN orders o ON o.user_id = u.id` used to fail with
"Left join key 'user_id' not found" for INNER, LEFT and RIGHT joins alike: the ON
operands were read positionally and the first one was assumed to belong to the left
table. Both spellings must produce identical results.
"""
from __future__ import annotations

import tempfile

import pytest

from apexbase.client import ApexClient

# users: 1,2,3 matched and 9 unmatched; orders: five rows over users 1,2,3.
ORDERS = {"id": [1, 2, 3, 4, 5], "user_id": [1, 2, 1, 3, 2],
          "amount": [10, 20, 30, 40, 50]}


def _client(tmp):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("users", schema={"id": "int64", "city": "string"})
    client.use_table("users")
    client.store({"id": [1, 2, 3, 9], "city": ["a", "b", "c", "x"]})
    client.flush()
    client.create_table("orders", schema={"id": "int64", "user_id": "int64",
                                          "amount": "int64"})
    client.use_table("orders")
    client.store(ORDERS)
    client.flush()
    return client


@pytest.mark.parametrize("join_type", ["JOIN", "LEFT JOIN", "RIGHT JOIN"])
@pytest.mark.parametrize("reversed_on", [False, True])
def test_both_on_orders_count_identically(join_type, reversed_on):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            on = ("o.user_id = u.id" if reversed_on else "u.id = o.user_id")
            sql = (f"SELECT COUNT(*) AS n FROM users u {join_type} orders o ON {on}")
            expected = {"JOIN": 5, "LEFT JOIN": 6, "RIGHT JOIN": 5}[join_type]
            assert client.execute(sql).to_dict()[0]["n"] == expected, sql
        finally:
            client.close()


@pytest.mark.parametrize("reversed_on", [False, True])
def test_both_on_orders_project_the_same_rows(reversed_on):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            on = ("o.user_id = u.id" if reversed_on else "u.id = o.user_id")
            def rows_of(extra: str):
                got = client.execute(
                    f"SELECT u.id AS uid, o.amount AS amount FROM users u "
                    f"LEFT JOIN orders o ON {on}{extra}"
                ).to_dict()
                # Compare as a multiset: multi-key ORDER BY is a separate concern.
                return sorted(((r["uid"], r["amount"]) for r in got),
                              key=lambda pair: (pair[0], pair[1] is None, pair[1] or 0))

            assert rows_of("") == [(1, 10), (1, 30), (2, 20), (2, 50), (3, 40), (9, None)]
            # An extra ON predicate must survive the reordering too.
            assert rows_of(" AND o.amount > 20") == [
                (1, 30), (2, 50), (3, 40), (9, None)]
        finally:
            client.close()


def test_reversed_on_does_not_disturb_the_join_count_lane():
    """The count lane prunes by key, so it must orient the keys the same way."""
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64"})
        client.use_table("t")
        rows = 50_000
        client.store({"k": list(range(rows))})
        client.flush()
        try:
            forward = "SELECT COUNT(*) AS n FROM t a JOIN t b ON a.k = b.k WHERE a.k < 10"
            reverse = "SELECT COUNT(*) AS n FROM t a JOIN t b ON b.k = a.k WHERE a.k < 10"
            assert client.execute(forward).to_dict()[0]["n"] == 10
            assert client.execute(reverse).to_dict()[0]["n"] == 10
        finally:
            client.close()
