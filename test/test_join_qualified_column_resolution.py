"""A qualified reference must pick its own table's column when names collide.

`SELECT u.id FROM orders o JOIN users u ON o.user_id = u.id` returned the *left*
table's `id` (10,20,30) because an INNER join never kept a `u.id` column: the
projection resolves a reference as written and only then falls back to the bare
column. LEFT/RIGHT joins already kept the qualified key, so only INNER was wrong.

`GROUP BY` is fixed for a single key and for two keys written string-first; the
int-first orderings (`GROUP BY u.id, o.id`) still emit the wrong first-key values and are
tracked separately.
"""
from __future__ import annotations

import tempfile

import pytest

from apexbase.client import ApexClient

# users.id: 1,2,3 plus an unmatched 9. orders.id: 10..50, user_id: 1,2,1,3,2.
ORDERS = {"id": [10, 20, 30, 40, 50], "user_id": [1, 2, 1, 3, 2]}


def _client(tmp):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("users", schema={"id": "int64", "city": "string"})
    client.use_table("users")
    client.store({"id": [1, 2, 3, 9], "city": ["a", "b", "c", "x"]})
    client.flush()
    client.create_table("orders", schema={"id": "int64", "user_id": "int64"})
    client.use_table("orders")
    client.store(ORDERS)
    client.flush()
    return client


@pytest.mark.parametrize("join_type", ["JOIN", "LEFT JOIN", "RIGHT JOIN"])
def test_a_qualified_reference_picks_its_own_table(join_type):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            base = (f"FROM orders o {join_type} users u ON o.user_id = u.id")
            users = sorted(r["x"] for r in client.execute(
                f"SELECT u.id AS x {base}").to_dict())
            if join_type == "RIGHT JOIN":
                # Every user survives; user 9 has no order, so its left key is NULL.
                # The left table's own columns are named `id_right` after the swap (a
                # separate pre-existing naming gap), so only the right key is checked.
                # Five matched orders (users 1,2,1,3,2) plus the preserved user 9.
                assert users == [1, 1, 2, 2, 3, 9], users
            else:
                orders = sorted(r["x"] for r in client.execute(
                    f"SELECT o.id AS x {base}").to_dict())
                assert users == [1, 1, 2, 2, 3], users
                assert orders == [10, 20, 30, 40, 50], orders
        finally:
            client.close()


@pytest.mark.parametrize("join_type,users_sum", [("JOIN", 9), ("LEFT JOIN", 9),
                                                ("RIGHT JOIN", 18)])
def test_aggregates_over_colliding_names_use_the_right_side(join_type, users_sum):
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            row = client.execute(
                f"SELECT SUM(u.id) AS su, MIN(u.id) AS mu FROM orders o "
                f"{join_type} users u ON o.user_id = u.id"
            ).to_dict()[0]
            assert row["su"] == users_sum, row
            assert row["mu"] == 1, row
            if join_type != "RIGHT JOIN":
                # The left key is addressable for INNER/LEFT; after the RIGHT-JOIN swap
                # the left's columns are named `id_right` (pre-existing, tracked).
                row = client.execute(
                    f"SELECT SUM(o.id) AS so, MAX(o.id) AS xo FROM orders o "
                    f"{join_type} users u ON o.user_id = u.id"
                ).to_dict()[0]
                assert row["so"] == 150 and row["xo"] == 50, row
        finally:
            client.close()


def test_an_unqualified_reference_still_uses_the_bare_column():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "v": "int64"})
        client.use_table("t")
        client.store({"k": [1, 2, 3], "v": [10, 20, 30]})
        client.flush()
        try:
            row = client.execute("SELECT SUM(v) AS s, MIN(k) AS m, v AS v FROM t GROUP BY v HAVING SUM(v) > 0").to_dict()
            assert [r["s"] for r in row] == [10, 20, 30], row
        finally:
            client.close()


@pytest.mark.parametrize("join_type", ["JOIN", "LEFT JOIN"])
def test_group_by_on_a_colliding_qualified_name_groups_the_right_column(join_type):
    """`GROUP BY u.id` used to strip the qualifier and group by the left table's `id`.

    With both tables carrying `id`, the groups came out as orders' ids (10,20,30...) and
    the SELECT alias was lost. The group key is now resolved as written and the alias is
    still honoured. (A multi-key GROUP BY containing a colliding name still goes through
    a lane that strips the qualifier; that is tracked separately.)
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            rows = client.execute(
                f"SELECT u.id AS x, COUNT(*) AS n FROM orders o {join_type} users u "
                f"ON o.user_id = u.id GROUP BY u.id"
            ).to_dict()
            got = sorted((r["x"], r["n"]) for r in rows)
            assert got == [(1, 2), (2, 2), (3, 1)], got
            # The left table's ids still group on their own side.
            rows = client.execute(
                f"SELECT o.id AS x, COUNT(*) AS n FROM orders o {join_type} users u "
                f"ON o.user_id = u.id GROUP BY o.id"
            ).to_dict()
            assert sorted(r["x"] for r in rows) == [10, 20, 30, 40, 50], rows
            # A multi-key GROUP BY that includes a colliding name still routes its
            # first key through a separate lane that strips the qualifier; that is
            # tracked on its own, so only the single-key shapes are asserted here.
        finally:
            client.close()


def test_group_by_keeps_an_alias_and_a_having_filter():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            rows = client.execute(
                "SELECT u.id AS x, COUNT(*) AS n FROM orders o JOIN users u "
                "ON o.user_id = u.id GROUP BY u.id HAVING COUNT(*) > 1 ORDER BY x DESC"
            ).to_dict()
            assert [(r["x"], r["n"]) for r in rows] == [(2, 2), (1, 2)], rows
        finally:
            client.close()


def test_a_two_key_group_by_with_an_int_first_keeps_both_keys():
    """`GROUP BY u.id, u.city` must group and label both keys like the reverse order.

    The two-key kernel is written for (string, int): it dictionary-encodes the first key
    and range-encodes the second. Written the other way round the query fell through to a
    path that emitted the left table's `id`, so the kernel now rebinds its columns and the
    emitter restores the written order (types included).
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            rows = client.execute(
                "SELECT u.id AS x, u.city AS y, COUNT(*) AS n FROM orders o "
                "JOIN users u ON o.user_id = u.id GROUP BY u.id, u.city"
            ).to_dict()
            assert sorted((r["x"], r["y"], r["n"]) for r in rows) == [
                (1, "a", 2), (2, "b", 2), (3, "c", 1)], rows
            # Both orders must agree.
            reverse = client.execute(
                "SELECT u.city AS x, u.id AS y, COUNT(*) AS n FROM orders o "
                "JOIN users u ON o.user_id = u.id GROUP BY u.city, u.id"
            ).to_dict()
            assert sorted((r["y"], r["x"], r["n"]) for r in reverse) == [
                (1, "a", 2), (2, "b", 2), (3, "c", 1)], reverse
            # Aggregates that match the SELECT list keep their own columns.
            rows = client.execute(
                "SELECT u.id AS x, u.city AS y, SUM(o.id) AS s FROM orders o "
                "JOIN users u ON o.user_id = u.id GROUP BY u.id, u.city"
            ).to_dict()
            assert sorted((r["x"], r["y"], r["s"]) for r in rows) == [
                (1, "a", 40), (2, "b", 70), (3, "c", 40)], rows
        finally:
            client.close()


def test_a_two_key_group_by_with_a_string_first_takes_the_fast_lane():
    """`GROUP BY u.city, u.id` must group and label both keys correctly.

    The two-key lane compares the SELECT names against the group keys; once the keys
    carry a qualifier (`u.id`) the bare-name comparison failed and the query fell into a
    slower path that emitted the wrong first-key values. Comparing bare names on both
    sides keeps the lane (and gets `(city, id)` pairs right).
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            rows = client.execute(
                "SELECT u.city AS x, u.id AS y, COUNT(*) AS n FROM orders o "
                "JOIN users u ON o.user_id = u.id GROUP BY u.city, u.id"
            ).to_dict()
            assert sorted((r["x"], r["y"], r["n"]) for r in rows) == [
                ("a", 1, 2), ("b", 2, 2), ("c", 3, 1)], rows
            # A plain two-key GROUP BY (no join, no collision) is unaffected.
            rows = client.execute(
                "SELECT user_id AS x, id AS y, COUNT(*) AS n FROM orders "
                "GROUP BY user_id, id"
            ).to_dict()
            assert sorted((r["x"], r["y"], r["n"]) for r in rows) == [
                (1, 10, 1), (1, 30, 1), (2, 20, 1), (2, 50, 1), (3, 40, 1)], rows
        finally:
            client.close()


def test_two_key_group_by_keeps_both_ids_when_bare_names_collide():
    """Both tables expose `id`; the emitter used to strip the qualifier on lookup.

    That resolved `o.id` to the left table's `id`, so the second group column silently
    repeated the first (`(1,1,2) …` instead of `(1,10,2), (1,30,2), (2,20,2), (3,40,1)`).
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("u", schema={"id": "int64", "city": "string"})
        client.use_table("u")
        client.store({"id": [1, 2, 3, 1, 2], "city": ["a", "b", "c", "a", "b"]})
        client.flush()
        client.create_table("o", schema={"id": "int64", "uid": "int64"})
        client.use_table("o")
        client.store({"id": [10, 20, 30, 40], "uid": [1, 2, 1, 3]})
        client.flush()
        try:
            rows = client.execute(
                "SELECT u.id AS a, o.id AS b, COUNT(*) AS n FROM u JOIN o ON u.id = o.uid "
                "GROUP BY u.id, o.id ORDER BY a, b"
            ).to_dict()
            assert [(r["a"], r["b"], r["n"]) for r in rows] == [
                (1, 10, 2), (1, 30, 2), (2, 20, 2), (3, 40, 1)], rows
            # The written order must not matter.
            rows = client.execute(
                "SELECT o.id AS a, u.id AS b, COUNT(*) AS n FROM u JOIN o ON u.id = o.uid "
                "GROUP BY o.id, u.id ORDER BY a, b"
            ).to_dict()
            assert [(r["a"], r["b"], r["n"]) for r in rows] == [
                (10, 1, 2), (20, 2, 2), (30, 1, 2), (40, 3, 1)], rows
            # A single qualified key must still pick the right table's column.
            rows = client.execute(
                "SELECT o.id AS b, COUNT(*) AS n FROM u JOIN o ON u.id = o.uid "
                "GROUP BY o.id ORDER BY b"
            ).to_dict()
            assert [(r["b"], r["n"]) for r in rows] == [
                (10, 2), (20, 2), (30, 2), (40, 1)], rows
        finally:
            client.close()


def test_right_join_exposes_the_left_tables_qualified_columns():
    """A RIGHT JOIN runs as a swapped LEFT JOIN, so the left table's colliding columns
    used to come out as `<name>_right` only and `SELECT o.id` failed with
    "Projected column 'o.id' does not exist". They are now also exposed as `o.id`, and
    the values must come from the left table (not mirrored from the right)."""
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("o", schema={"id": "int64", "uid": "int64", "amt": "int64"})
        client.use_table("o")
        client.store({"id": [10, 20, 30, 40], "uid": [1, 2, 1, 9], "amt": [5, 6, 7, 8]})
        client.flush()
        client.create_table("u", schema={"id": "int64", "city": "string"})
        client.use_table("u")
        client.store({"id": [1, 2, 3], "city": ["a", "b", "c"]})
        client.flush()
        try:
            rows = client.execute(
                "SELECT o.id AS oid, o.amt FROM o RIGHT JOIN u ON o.uid = u.id"
            ).to_dict()
            assert sorted((r["oid"] is None, r["oid"] or 0, r["amt"] or 0) for r in rows) == [
                (False, 10, 5), (False, 20, 6), (False, 30, 7), (True, 0, 0)], rows
            # Both qualified keys must resolve to their own side.
            rows = client.execute(
                "SELECT o.id AS oid, u.id AS uid FROM o RIGHT JOIN u ON o.uid = u.id"
            ).to_dict()
            assert sorted(
                ((r["oid"] is None, r["oid"] or 0), r["uid"]) for r in rows
            ) == [((False, 10), 1), ((False, 20), 2), ((False, 30), 1), ((True, 0), 3)], rows
        finally:
            client.close()


def test_right_join_with_an_on_predicate_keeps_qualified_names():
    """A RIGHT JOIN with an extra ON predicate takes the swapped outer path, which used
    to drop the original right table's join key entirely: `SELECT u.id` failed with
    "Projected column 'u.id' does not exist". It is now carried under its qualified name
    (from the swapped batch, so the rows line up), and the left table's `_right` columns
    also get their own qualified copies. Values must come from the correct side."""
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("o", schema={"id": "int64", "uid": "int64", "amt": "int64"})
        client.use_table("o")
        client.store({"id": [10, 20, 30, 40], "uid": [1, 2, 1, 9], "amt": [5, 6, 7, 8]})
        client.flush()
        client.create_table("u", schema={"id": "int64", "city": "string"})
        client.use_table("u")
        client.store({"id": [1, 2, 3], "city": ["a", "b", "c"]})
        client.flush()
        try:
            # u is the preserved side: every u row survives, matched rows repeat.
            rows = client.execute(
                "SELECT u.id AS uid FROM o RIGHT JOIN u ON o.uid = u.id AND u.city = 'a'"
            ).to_dict()
            assert sorted(r["uid"] for r in rows) == [1, 1, 2, 3], rows
            rows = client.execute(
                "SELECT u.id AS uid, o.id AS oid FROM o RIGHT JOIN u "
                "ON o.uid = u.id AND u.city = 'a'"
            ).to_dict()
            assert sorted((r["uid"], r["oid"] is None, r["oid"] or 0) for r in rows) == [
                (1, False, 10), (1, False, 30), (2, True, 0), (3, True, 0)], rows
            rows = client.execute(
                "SELECT u.id AS uid, o.amt AS amt FROM o RIGHT JOIN u "
                "ON o.uid = u.id AND o.amt > 5"
            ).to_dict()
            assert sorted((r["uid"], r["amt"] is None, r["amt"] or 0) for r in rows) == [
                (1, False, 7), (2, False, 6), (3, True, 0)], rows
            # The original left table's columns stay reachable too.
            rows = client.execute(
                "SELECT o.id AS oid FROM o RIGHT JOIN u ON o.uid = u.id AND u.city = 'a'"
            ).to_dict()
            assert sorted((r["oid"] is None, r["oid"] or 0) for r in rows) == [
                (False, 10), (False, 30), (True, 0), (True, 0)], rows
        finally:
            client.close()
