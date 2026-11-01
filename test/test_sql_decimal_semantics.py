"""SQL `DECIMAL(p,s)` numeric query semantics (V5 S1).

`DECIMAL(p,s)` columns are stored as the fixed 16-byte ``Decimal128`` payload and
the public surface still exposes that payload (D4). These tests pin the *query*
semantics that V5 S1 makes default-on:

* WHERE comparisons, ``IN`` and ``BETWEEN`` compare scaled i128 values;
* ORDER BY sorts numerically (the little-endian payload byte order is not
  numeric);
* ``SUM``/``MIN``/``MAX``/``AVG`` follow the scaled semantics on the memory,
  flushed/mmap and reopened paths, with and without a WHERE clause;
* invalid literals fail loudly instead of silently comparing bytes.
"""

from __future__ import annotations

import pytest

from apexbase.client import ApexClient

SCALE = 2


def _unscaled(payload):
    return int.from_bytes(payload, "little", signed=True)


def _write_rows(client):
    client.execute("CREATE TABLE t (id INT64, d DECIMAL(10,2))")
    client.use_table("t")
    client.execute(
        "INSERT INTO t (id, d) VALUES "
        "(1, '1.23'), (2, '45.60'), (3, '-7.00'), (4, '0.00'), "
        "(5, '999.00'), (6, '2.00'), (7, NULL)"
    )


def _ids(client, sql):
    return [row["id"] for row in client.execute(sql).to_dict()]


def test_decimal_is_the_default_representation(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        _write_rows(client)
        assert client._storage.table_types("t")["d"] == "decimal(10,2)"
        version = int.from_bytes(open(str(tmp_path / "t.apex"), "rb").read(16)[8:12], "little")
        assert version == 5
    finally:
        client.close()


def test_where_comparisons_use_numeric_semantics(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        _write_rows(client)
        assert _ids(client, "SELECT id FROM t WHERE d > '1.2' ORDER BY id") == [1, 2, 5, 6]
        assert _ids(client, "SELECT id FROM t WHERE d >= '2' ORDER BY id") == [2, 5, 6]
        assert _ids(client, "SELECT id FROM t WHERE d < '0' ORDER BY id") == [3]
        assert _ids(client, "SELECT id FROM t WHERE d <= '0' ORDER BY id") == [3, 4]
        assert _ids(client, "SELECT id FROM t WHERE d = '1.23' ORDER BY id") == [1]
        assert _ids(client, "SELECT id FROM t WHERE d != '1.23' ORDER BY id") == [2, 3, 4, 5, 6]
        # A float literal is compared as its decimal form, not its binary image.
        assert _ids(client, "SELECT id FROM t WHERE d > 1.2 ORDER BY id") == [1, 2, 5, 6]
        # A literal on the left flips the operator instead of comparing bytes.
        assert _ids(client, "SELECT id FROM t WHERE '1.2' < d ORDER BY id") == [1, 2, 5, 6]
        assert _ids(client, "SELECT id FROM t WHERE 0 >= d ORDER BY id") == [3, 4]
        # NULL rows are unknown, never matches.
        assert _ids(client, "SELECT id FROM t WHERE d IS NULL") == [7]
        assert _ids(client, "SELECT id FROM t WHERE d IS NOT NULL ORDER BY id") == [
            1,
            2,
            3,
            4,
            5,
            6,
        ]
    finally:
        client.close()


def test_where_in_and_between_use_numeric_semantics(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        _write_rows(client)
        assert _ids(client, "SELECT id FROM t WHERE d IN ('1.23', '999.00') ORDER BY id") == [1, 5]
        assert _ids(client, "SELECT id FROM t WHERE d NOT IN ('1.23', '999.00') ORDER BY id") == [
            2,
            3,
            4,
            6,
        ]
        assert _ids(
            client, "SELECT id FROM t WHERE d BETWEEN '0' AND '46' ORDER BY id"
        ) == [1, 2, 4, 6]
        assert _ids(
            client, "SELECT id FROM t WHERE d NOT BETWEEN '0' AND '46' ORDER BY id"
        ) == [3, 5]
        assert _ids(client, "SELECT id FROM t WHERE d IN (0, 2) ORDER BY id") == [4, 6]
    finally:
        client.close()


def test_order_by_sorts_numerically(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        _write_rows(client)
        # Ascending default puts NULLs last; the 16-byte payload byte order would
        # place 999.00 before 2.00.
        assert _ids(client, "SELECT id FROM t ORDER BY d") == [3, 4, 1, 6, 2, 5, 7]
        # Descending default puts NULLs first.
        assert _ids(client, "SELECT id FROM t ORDER BY d DESC") == [7, 5, 2, 6, 1, 4, 3]
        assert _ids(client, "SELECT id FROM t ORDER BY d ASC LIMIT 3") == [3, 4, 1]
        assert _ids(client, "SELECT id FROM t ORDER BY d ASC LIMIT 2 OFFSET 3") == [6, 2]
        # Multiple sort keys compare the DECIMAL key numerically first.
        assert _ids(
            client,
            "SELECT id FROM t ORDER BY d DESC, id ASC",
        ) == [7, 5, 2, 6, 1, 4, 3]
        # Projecting the value keeps the fixed payload (D4).
        rows = client.execute("SELECT d FROM t ORDER BY d LIMIT 1").to_dict()
        assert _unscaled(rows[0]["d"]) == -700
    finally:
        client.close()


def test_invalid_decimal_literal_is_rejected(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        _write_rows(client)
        with pytest.raises(RuntimeError, match="numeric literal"):
            client.execute("SELECT id FROM t WHERE d > 'abc'")
        with pytest.raises(RuntimeError, match="numeric literal"):
            client.execute("SELECT id FROM t WHERE d IN ('1.23', 'nope')")
        with pytest.raises(RuntimeError, match="numeric literal"):
            client.execute("SELECT id FROM t WHERE d BETWEEN 'x' AND '2'")
    finally:
        client.close()


def test_aggregates_with_where_use_scaled_semantics(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        _write_rows(client)
        row = client.execute(
            "SELECT SUM(d) AS s, MIN(d) AS m, MAX(d) AS x, AVG(d) AS a, COUNT(d) AS n "
            "FROM t WHERE id <= 2"
        ).to_dict()[0]
        assert _unscaled(row["s"]) == 123 + 4560
        assert _unscaled(row["m"]) == 123
        assert _unscaled(row["x"]) == 4560
        assert row["a"] == pytest.approx((1.23 + 45.60) / 2)
        assert row["n"] == 2

        # Unfiltered aggregates keep the same scaled semantics.
        row = client.execute(
            "SELECT SUM(d) AS s, MIN(d) AS m, MAX(d) AS x, AVG(d) AS a FROM t"
        ).to_dict()[0]
        assert _unscaled(row["s"]) == 123 + 4560 - 700 + 0 + 99900 + 200
        assert _unscaled(row["m"]) == -700
        assert _unscaled(row["x"]) == 99900
        assert row["a"] == pytest.approx(
            (1.23 + 45.60 - 7.00 + 0.00 + 999.00 + 2.00) / 6
        )
    finally:
        client.close()


def test_group_by_decimal_key_orders_numerically(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        _write_rows(client)
        rows = client.execute(
            "SELECT d, COUNT(*) AS n FROM t GROUP BY d ORDER BY d"
        ).to_dict()
        assert [
            None if row["d"] is None else _unscaled(row["d"]) for row in rows
        ] == [-700, 0, 123, 200, 4560, 99900, None]
        assert [row["n"] for row in rows] == [1, 1, 1, 1, 1, 1, 1]
    finally:
        client.close()


def test_group_by_decimal_value_aggregates(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        client.execute("CREATE TABLE g (cat INT64, d DECIMAL(10,2))")
        client.use_table("g")
        client.execute(
            "INSERT INTO g (cat, d) VALUES "
            "(1, '1.23'), (1, '2.00'), (2, '-7.00'), (2, NULL), (3, '999.00')"
        )
        rows = client.execute(
            "SELECT cat, SUM(d) AS s, MIN(d) AS m, MAX(d) AS x, AVG(d) AS a, COUNT(d) AS n "
            "FROM g GROUP BY cat ORDER BY cat"
        ).to_dict()
        assert [row["cat"] for row in rows] == [1, 2, 3]
        assert [_unscaled(rows[0]["s"]), _unscaled(rows[1]["s"]), _unscaled(rows[2]["s"])] == [
            323,
            -700,
            99900,
        ]
        assert [_unscaled(row["m"]) for row in rows] == [123, -700, 99900]
        assert [_unscaled(row["x"]) for row in rows] == [200, -700, 99900]
        assert rows[0]["a"] == pytest.approx((1.23 + 2.00) / 2)
        assert rows[1]["a"] == pytest.approx(-7.0)
        assert rows[2]["a"] == pytest.approx(999.0)
        assert [row["n"] for row in rows] == [2, 1, 1]
    finally:
        client.close()


def test_flushed_reopened_table_keeps_numeric_semantics(tmp_path):
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        _write_rows(client)
        client.flush()
        # INSERT after flush must keep using the catalog scale, not scale 0.
        client.execute("INSERT INTO t (id, d) VALUES (8, '2.50')")
        client.flush()
    finally:
        client.close()

    reopened = ApexClient(str(tmp_path), drop_if_exists=False, enable_cache=False)
    try:
        reopened.use_table("t")
        assert _ids(reopened, "SELECT id FROM t WHERE d > '2.00' ORDER BY id") == [2, 5, 8]
        assert _ids(reopened, "SELECT id FROM t ORDER BY d LIMIT 3") == [3, 4, 1]
        row = reopened.execute(
            "SELECT SUM(d) AS s, MIN(d) AS m, MAX(d) AS x FROM t"
        ).to_dict()[0]
        assert _unscaled(row["s"]) == 123 + 4560 - 700 + 0 + 99900 + 200 + 250
        assert _unscaled(row["m"]) == -700
        assert _unscaled(row["x"]) == 99900
    finally:
        reopened.close()


def test_order_by_decimal_on_a_large_flushed_table(tmp_path):
    """The >50K-row ORDER BY lane must also sort DECIMAL numerically."""
    client = ApexClient(str(tmp_path), drop_if_exists=True, enable_cache=False)
    try:
        client.execute("CREATE TABLE big (id INT64, d DECIMAL(10,2))")
        client.use_table("big")
        ids = list(range(60_000))
        # Values wrap so byte order and numeric order disagree.
        values = [(index % 1000) * 100 for index in ids]
        payloads = [value.to_bytes(16, "little", signed=True) for value in values]
        client.store({"id": ids, "d": payloads})
        client.flush()
        rows = client.execute("SELECT d FROM big ORDER BY d DESC LIMIT 3").to_dict()
        assert [_unscaled(row["d"]) for row in rows] == [99900, 99900, 99900]
        rows = client.execute("SELECT d FROM big ORDER BY d ASC LIMIT 1").to_dict()
        assert _unscaled(rows[0]["d"]) == 0
        expected = sum(1 for index in ids if (index % 1000) * 100 >= 9900)
        assert client.execute(
            "SELECT COUNT(*) AS n FROM big WHERE d >= '99.00'"
        ).to_dict() == [{"n": expected}]
    finally:
        client.close()
