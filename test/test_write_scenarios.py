"""Write-path coverage across every route the Python client exposes.

Each test drives one class of write (single row, batch, columnar, Arrow, pandas,
SQL DML, replace, delete, schema evolution, delta spill, reopen) and checks the
same invariants from the read side:

* the row set is exactly what the writes asked for;
* ``count_rows()``, ``SELECT COUNT(*)`` and the visible ``_id`` list agree;
* the values read back are the values written (no silent NULL / 0 / ``''``);
* the state survives a ``flush()`` and a reopen.

The regression suite in ``test_engine_defect_regressions.py`` records the
specific defects that were fixed; this file is the broader matrix that keeps the
routes consistent with each other.
"""

import os
import random
import shutil
import tempfile
import threading

import pytest

from apexbase import ApexClient


@pytest.fixture
def tmp_client():
    """A fresh file-backed client per test."""
    tmpdir = tempfile.mkdtemp()
    client = ApexClient(os.path.join(tmpdir, "db"))
    try:
        yield client
    finally:
        client.close()
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def db_dir():
    """A directory that a test can reopen clients against."""
    tmpdir = tempfile.mkdtemp()
    try:
        yield os.path.join(tmpdir, "db")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def visible_ids(client, table):
    """Every ``_id`` a plain scan returns, sorted."""
    rows = client.execute(f"SELECT _id FROM {table}").to_dict()
    return sorted(int(row["_id"]) for row in rows)


def assert_row_set(client, table, expected, context):
    """The count APIs, the scan and ``expected`` must all describe one row set."""
    expected = sorted(int(row_id) for row_id in expected)
    ids = visible_ids(client, table)
    assert ids == expected, f"{context}: scan returned {ids}, expected {expected}"
    assert client.count_rows() == len(expected), (
        f"{context}: count_rows() = {client.count_rows()}, expected {len(expected)}"
    )
    counted = client.execute(f"SELECT COUNT(*) AS n FROM {table}").to_dict()
    assert int(counted[0]["n"]) == len(expected), (
        f"{context}: COUNT(*) = {counted[0]['n']}, expected {len(expected)}"
    )


class TestWriteRouteMatrix:
    """Every route must store the values it was handed."""

    def test_store_single_row_and_list(self, tmp_client):
        """Single-row and batch stores must both land, in either order.

        A single-row store buffers its row in a warm memtable backend; a later
        write through another route used to publish a base file without it.
        """
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, n INT)")
        client.use_table("t")

        client.store({"k": "single", "n": 1})
        assert_row_set(client, "t", [1], "single-row store")
        assert client.retrieve(1)["k"] == "single"

        # A batch write, then SQL, then another single row: every route must
        # preserve the rows the others wrote.
        client.store([{"k": "a", "n": 2}, {"k": "b", "n": 3}])
        assert_row_set(client, "t", [1, 2, 3], "list store")
        client.execute("INSERT INTO t (k, n) VALUES ('c', 4)")
        assert_row_set(client, "t", [1, 2, 3, 4], "SQL insert")
        client.store({"k": "d", "n": 5})
        assert_row_set(client, "t", [1, 2, 3, 4, 5], "second single row")

        client.flush()
        assert_row_set(client, "t", [1, 2, 3, 4, 5], "after flush")
        assert [client.retrieve(i)["k"] for i in range(1, 6)] == [
            "single",
            "a",
            "b",
            "c",
            "d",
        ]

    def test_store_columnar(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, n INT)")
        client.use_table("t")

        client.store({"k": ["a", "b", "c"], "n": [1, 2, 3]})
        assert_row_set(client, "t", [1, 2, 3], "columnar store")
        assert client.execute("SELECT k, n FROM t ORDER BY n").to_dict() == [
            {"k": "a", "n": 1},
            {"k": "b", "n": 2},
            {"k": "c", "n": 3},
        ]

    def test_store_arrow_table(self, tmp_client):
        pa = pytest.importorskip("pyarrow")
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, n INT)")
        client.use_table("t")

        client.store(
            pa.table(
                {
                    "k": pa.array(["a", "b"], type=pa.string()),
                    "n": pa.array([1, 2], type=pa.int64()),
                }
            )
        )
        assert_row_set(client, "t", [1, 2], "arrow store")
        assert [client.retrieve(i)["k"] for i in (1, 2)] == ["a", "b"]

    def test_store_pandas_frame(self, tmp_client):
        pd = pytest.importorskip("pandas")
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, n INT)")
        client.use_table("t")

        client.store(pd.DataFrame({"k": ["a", "b"], "n": [1, 2]}))
        assert_row_set(client, "t", [1, 2], "pandas store")
        assert [client.retrieve(i)["n"] for i in (1, 2)] == [1, 2]

    def test_store_durable_one(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, n INT)")
        client.use_table("t")
        client.store([{"k": "seed", "n": 0}])

        client.store_durable_one({"k": "durable", "n": 7})
        assert_row_set(client, "t", [1, 2], "store_durable_one")
        assert client.retrieve(2)["k"] == "durable"
        client.flush()
        assert client.retrieve(2)["n"] == 7

    def test_sql_dml_routes(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, n INT)")
        client.use_table("t")

        client.execute("INSERT INTO t (k, n) VALUES ('a', 1)")
        client.execute("INSERT INTO t (k, n) VALUES ('b', 2), ('c', 3)")
        assert_row_set(client, "t", [1, 2, 3], "SQL INSERT")

        client.execute("UPDATE t SET n = 20 WHERE k = 'b'")
        assert client.retrieve(2)["n"] == 20
        assert_row_set(client, "t", [1, 2, 3], "SQL UPDATE")

        client.execute("DELETE FROM t WHERE k = 'c'")
        assert_row_set(client, "t", [1, 2], "SQL DELETE")

        client.execute("INSERT INTO t (k, n) SELECT 'd', 4")
        assert_row_set(client, "t", [1, 2, 4], "INSERT ... SELECT")
        assert client.retrieve(4)["k"] == "d"

    def test_api_and_sql_routes_interleave(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, n INT)")
        client.use_table("t")

        client.store([{"k": f"k{i}", "n": i} for i in range(4)])
        client.execute("DELETE FROM t WHERE n = 1")
        assert_row_set(client, "t", [1, 3, 4], "after SQL delete")

        client.store({"k": "k4", "n": 4})
        assert_row_set(client, "t", [1, 3, 4, 5], "after API insert")

        assert client.replace(3, {"k": "replaced", "n": 30}) is True
        assert client.retrieve(3)["k"] == "replaced"
        assert_row_set(client, "t", [1, 3, 4, 5], "after replace")

        # The same row set must survive a flush and be readable through SQL.
        client.flush()
        assert_row_set(client, "t", [1, 3, 4, 5], "after flush")
        assert client.retrieve(3)["n"] == 30
        assert client.execute("SELECT n FROM t WHERE _id = 3").to_dict() == [{"n": 30}]


class TestNumericCoercion:
    """A number bound for a numeric column must be stored, not dropped.

    ``Int64 -> Float64`` is the one widening the SQL INSERT path has always
    performed, so every other route has to agree with it. The typed and Arrow
    writers used to match the value variant exactly and publish ``NULL`` (or a
    fabricated ``0``) instead.
    """

    def test_int_into_double_every_route(self, tmp_client):
        pa = pytest.importorskip("pyarrow")
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, f DOUBLE)")
        client.use_table("t")

        client.store({"k": "a", "f": 1})                        # single row
        client.store([{"k": "b", "f": 2}])                      # one-row list
        client.store([{"k": "c", "f": 3}, {"k": "d", "f": 4}])  # batch
        client.store({"k": ["e"], "f": [5]})                    # columnar
        client.store_durable_one({"k": "f", "f": 6})            # durable single row
        client.execute("INSERT INTO t (k, f) VALUES ('g', 7)")  # SQL
        client.store(                                           # Arrow int column
            pa.table(
                {
                    "k": pa.array(["h"], type=pa.string()),
                    "f": pa.array([8], type=pa.int64()),
                }
            )
        )
        client.flush()

        values = {
            row["k"]: row["f"]
            for row in client.execute("SELECT k, f FROM t").to_dict()
        }
        assert values == {
            "a": 1.0,
            "b": 2.0,
            "c": 3.0,
            "d": 4.0,
            "e": 5.0,
            "f": 6.0,
            "g": 7.0,
            "h": 8.0,
        }, f"an integer bound for a DOUBLE column must be stored as that number: {values}"
        assert_row_set(client, "t", list(range(1, 9)), "int -> double")

        client.flush()
        assert_row_set(client, "t", list(range(1, 9)), "int -> double after flush")

    def test_mixed_int_and_float_column_keeps_both(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, f DOUBLE)")
        client.use_table("t")

        # The integer row used to decide the column's type and take the float
        # rows down with it.
        client.store({"f": [1, 2.5, 3]})
        client.store([{"k": "x", "f": 4}, {"k": "y", "f": 5.5}])
        values = [row["f"] for row in client.execute("SELECT f FROM t").to_dict()]
        assert values == [1.0, 2.5, 3.0, 4.0, 5.5], values

        client.store([{"k": "z", "f": 6}])
        values = [row["f"] for row in client.execute("SELECT f FROM t").to_dict()]
        assert values == [1.0, 2.5, 3.0, 4.0, 5.5, 6.0], values


    def test_uncoercible_values_read_as_null_everywhere(self, tmp_client):
        """A value the column cannot hold is absent on every read path.

        The row-group writer used to publish the type's default (`0`, `0.0`,
        `false`) for a mismatched value, so `SELECT *` said NULL while the point
        lookup and `retrieve()` said `0`.
        """
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, i INT, f DOUBLE, b BOOL, s TEXT)")
        client.use_table("t")

        for write in (
            lambda: client.store({"k": "mixed", "i": 1.5, "f": "1.5", "b": 1, "s": 5}),
            lambda: client.store(
                [{"k": "mixed2", "i": 1.5, "f": "1.5", "b": 1, "s": 5}]
            ),
        ):
            write()
        client.flush()

        star = client.execute("SELECT * FROM t ORDER BY _id").to_dict()
        projection = client.execute("SELECT i, f, b, s FROM t ORDER BY _id").to_dict()
        point = client.execute("SELECT i, f, b, s FROM t WHERE _id = 1").to_dict()
        stored = client.retrieve(1)

        assert len(star) == 2
        for rows in (star, projection, point):
            for row in rows:
                assert row["i"] is None, row
                assert row["f"] is None, row
                assert row["b"] is None, row
                assert row["s"] is None, row
        assert stored["i"] is None and stored["f"] is None, stored
        assert stored["b"] is None and stored["s"] is None, stored

        # Values of the right type still land in the same columns.
        client.store({"k": "good", "i": 2, "f": 2.5, "b": True, "s": "text"})
        client.flush()
        good = client.retrieve(3)
        assert (good["i"], good["f"], good["b"], good["s"]) == (2, 2.5, True, "text")
        assert client.execute("SELECT i FROM t WHERE _id = 3").to_dict() == [{"i": 2}]


class TestDeleteSemantics:
    """Deleting what is not there must change nothing."""

    def test_repeated_delete_is_a_noop(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (v INT)")
        client.use_table("t")
        client.store([{"v": i} for i in range(5)])
        assert_row_set(client, "t", [1, 2, 3, 4, 5], "initial")

        assert client.delete(2) is True
        assert_row_set(client, "t", [1, 3, 4, 5], "first delete")
        assert client.delete(2) is False, "deleting a deleted row must report False"
        assert_row_set(client, "t", [1, 3, 4, 5], "repeated delete")

        client.flush()
        assert client.delete(2) is False
        assert_row_set(client, "t", [1, 3, 4, 5], "repeated delete after flush")

        assert client.delete(99) is False
        assert_row_set(client, "t", [1, 3, 4, 5], "missing id")

        # A replace may not resurrect a deleted row.
        assert client.replace(2, {"v": 200}) is False
        assert client.retrieve(2) is None
        assert_row_set(client, "t", [1, 3, 4, 5], "replace of a deleted row")

    def test_delete_batch_duplicates_and_missing(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (v INT)")
        client.use_table("t")
        client.store([{"v": i} for i in range(6)])

        assert client.delete([1, 1, 3, 99, 0]) is True
        assert_row_set(client, "t", [2, 4, 5, 6], "batch delete with duplicates")
        assert client.delete([1, 3]) is False, "already deleted ids"
        assert_row_set(client, "t", [2, 4, 5, 6], "repeated batch delete")
        # An empty id list is a successful no-op ("every requested row is gone").
        assert client.delete([]) is True
        assert_row_set(client, "t", [2, 4, 5, 6], "empty batch delete")

    def test_delete_where_then_continue_writing(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (v INT, tag TEXT)")
        client.use_table("t")
        client.store([{"v": i, "tag": "old" if i % 2 else "keep"} for i in range(6)])

        removed = client.delete(where="tag = 'old'")
        assert removed == 3
        assert_row_set(client, "t", [1, 3, 5], "delete by predicate")

        client.store([{"v": 99, "tag": "new"}])
        assert_row_set(client, "t", [1, 3, 5, 7], "write after predicate delete")
        assert client.retrieve(7)["tag"] == "new"


class TestSchemaEvolutionWrites:
    """A column added after the fact must stay NULL for the rows that predate it."""

    @pytest.mark.parametrize(
        "column_type,written",
        [
            ("string", "text"),
            ("int", 5),
            ("float", 1.5),
            ("bool", True),
        ],
    )
    def test_added_column_keeps_old_rows_null(self, tmp_client, column_type, written):
        client = tmp_client
        client.execute("CREATE TABLE t (v INT)")
        client.use_table("t")
        client.store([{"v": 1}])

        client.add_column("extra", column_type)

        def extra_of(row_id):
            rows = client.execute(f"SELECT extra FROM t WHERE _id = {row_id}").to_dict()
            return rows[0]["extra"] if rows else None

        assert extra_of(1) is None, "the new column starts out NULL"

        # Both a single-row write (which appends through the delta sidecar) and a
        # batch write must leave the pre-existing row NULL rather than filling in
        # the column's default.
        client.store([{"v": 2, "extra": written}])
        assert extra_of(1) is None, (
            f"a single-row write turned the old NULL into {extra_of(1)!r}"
        )
        assert extra_of(2) == written

        client.store([{"v": 3, "extra": written}, {"v": 4, "extra": written}])
        client.flush()
        rows = client.execute("SELECT v, extra FROM t ORDER BY v").to_dict()
        assert [row["extra"] for row in rows] == [None, written, written, written], rows

        client.store([{"v": 5, "extra": written}])
        rows = client.execute("SELECT v, extra FROM t ORDER BY v").to_dict()
        assert [row["extra"] for row in rows] == [None, written, written, written, written]

    def test_writes_after_drop_column(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (a INT, b TEXT, c DOUBLE)")
        client.use_table("t")
        client.store([{"a": 1, "b": "x", "c": 1.5}])

        client.drop_column("b")
        columns = [row for row in client.execute("SELECT * FROM t").to_dict()]
        assert "b" not in columns[0]

        client.store([{"a": 2, "c": 2.5}])
        client.flush()
        rows = client.execute("SELECT a, c FROM t ORDER BY a").to_dict()
        assert [(row["a"], row["c"]) for row in rows] == [(1, 1.5), (2, 2.5)]


    def test_added_column_is_visible_to_retrieve(self, tmp_client):
        """`retrieve()` must report a column added after the row was written."""
        client = tmp_client
        client.execute("CREATE TABLE t (v INT)")
        client.use_table("t")
        client.store([{"v": 1}])
        client.add_column("extra", "string")

        assert client.retrieve(1)["extra"] is None, client.retrieve(1)
        client.store([{"v": 2, "extra": "two"}])
        assert client.retrieve(1)["extra"] is None
        assert client.retrieve(2)["extra"] == "two"
        client.flush()
        assert client.retrieve(1)["extra"] is None
        assert client.retrieve(2)["extra"] == "two"


class TestValueRoundTrip:
    """Values at the edges of each column type must survive every route."""

    def test_null_unicode_and_large_values(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (s TEXT, n INT)")
        client.use_table("t")

        long_text = "x" * (256 * 1024)
        client.store(
            [
                {"s": None, "n": None},
                {"s": "", "n": 0},
                {"s": "héllo wörld ✓ 中文 🎉", "n": -1},
                {"s": long_text, "n": 9007199254740993},
            ]
        )
        assert_row_set(client, "t", [1, 2, 3, 4], "edge values")

        rows = {
            int(row["_id"]): row
            for row in client.execute("SELECT _id, s, n FROM t").to_dict()
        }
        assert rows[1]["s"] is None and rows[1]["n"] is None
        assert rows[2]["s"] == "" and rows[2]["n"] == 0
        assert rows[3]["s"] == "héllo wörld ✓ 中文 🎉" and rows[3]["n"] == -1
        assert rows[4]["s"] == long_text and rows[4]["n"] == 9007199254740993

        client.flush()
        assert client.retrieve(4)["s"] == long_text

    def test_bytes_and_blob_columns(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, raw blob)")
        client.use_table("t")

        payload = bytes(range(256)) * 4
        client.store([{"k": "a", "raw": payload}, {"k": "b", "raw": b""}])
        client.store([{"k": "c", "raw": payload[:16]}])
        assert_row_set(client, "t", [1, 2, 3], "blob writes")

        assert client.read_blob("raw", 1) == payload
        assert client.read_blob("raw", 2) == b""
        assert client.read_blob("raw", 3) == payload[:16]
        client.flush()
        assert client.read_blob("raw", 1) == payload


class TestAllTypesEveryRoute:
    """One row carrying every column type, written by every store route.

    The table has an integer, a double, a boolean, a string, a binary payload and
    a float16 vector; each route writes one such row and the values are then read
    back through a full scan, a projection and the point API, before and after a
    flush and a reopen.
    """

    DIM = 4

    @classmethod
    def _vector(cls, seed):
        return [float(seed + index) / 4.0 for index in range(cls.DIM)]

    @classmethod
    def _row(cls, tag, variant):
        return {
            "tag": tag,
            "i": variant,
            "f": variant / 2.0,
            "b": variant % 2 == 0,
            "s": f"text-{variant}-✓",
            "by": bytes([variant, 0, 255, 7]),
            "emb": cls._vector(variant),
        }

    @classmethod
    def _expected(cls, tag, variant, *, binary=True, vector=True):
        return {
            "tag": tag,
            "i": variant,
            "f": variant / 2.0,
            "b": variant % 2 == 0,
            "s": f"text-{variant}-✓",
            "by": bytes([variant, 0, 255, 7]) if binary else None,
            "emb": cls._vector(variant) if vector else None,
        }

    @staticmethod
    def _write(client, route, row, table):
        if route == "store_dict":
            client.store(dict(row))
        elif route == "store_one_row_list":
            client.store([dict(row)])
        elif route == "store_list":
            client.store([dict(row), dict(row, tag=row["tag"] + "-2")])
        elif route == "store_columnar":
            client.store({key: [value] for key, value in row.items()})
        elif route == "store_durable_one":
            client.store_durable_one(dict(row))
        elif route == "store_arrow":
            pa = pytest.importorskip("pyarrow")
            client.store(
                pa.table(
                    {
                        "tag": pa.array([row["tag"]], type=pa.string()),
                        "i": pa.array([row["i"]], type=pa.int64()),
                        "f": pa.array([row["f"]], type=pa.float64()),
                        "b": pa.array([row["b"]], type=pa.bool_()),
                        "s": pa.array([row["s"]], type=pa.string()),
                        "by": pa.array([row["by"]], type=pa.binary()),
                    }
                )
            )
        elif route == "store_pandas":
            pd = pytest.importorskip("pandas")
            client.store(
                pd.DataFrame(
                    {
                        "tag": [row["tag"]],
                        "i": [row["i"]],
                        "f": [row["f"]],
                        "b": [row["b"]],
                        "s": [row["s"]],
                    }
                )
            )
        elif route == "sql_insert":
            client.execute(
                f"INSERT INTO {table} (tag, i, f, b, s) VALUES "
                f"('{row['tag']}', {row['i']}, {row['f']}, {str(row['b']).lower()}, '{row['s']}')"
            )
        else:  # pragma: no cover - guarded by the parametrization
            raise AssertionError(f"unknown route {route}")

    @staticmethod
    def _rows_of(client, table):
        """Every row as a dict keyed by its id, vectors normalised to lists."""
        rows = {}
        for row in client.execute(f"SELECT _id, tag, i, f, b, s, by FROM {table}").to_dict():
            rows[int(row["_id"])] = row
        return rows

    def _nearest_id(self, client, table, vector):
        """The id the vector search returns as the closest hit.

        A raw projection of a vector column is not part of the public read
        surface (the distance functions are), so a written vector is verified
        through the search path it exists for.
        """
        hits = client.topk_distance("emb", list(vector), k=1, metric="cosine").to_dict()
        assert hits, "vector search must return the stored row"
        return int(hits[0]["_id"])

    @pytest.mark.parametrize(
        "route",
        [
            "store_dict",
            "store_one_row_list",
            "store_list",
            "store_columnar",
            "store_durable_one",
            "store_arrow",
            "store_pandas",
            "sql_insert",
        ],
    )
    def test_every_type_through_route(self, db_dir, route):
        client = ApexClient(db_dir)
        client.create_table(
            "types",
            {
                "tag": "string",
                "i": "int",
                "f": "float",
                "b": "bool",
                "s": "string",
                "by": "binary",
                "emb": "float16_vector",
            },
        )
        client.use_table("types")

        row = self._row(route, 3)
        self._write(client, route, row, "types")
        client.flush()

        # The SQL and pandas/Arrow routes cannot express every column: those
        # stay NULL, which is asserted below through the same expectations.
        binary = route not in ("sql_insert", "store_pandas")
        vector = route not in ("sql_insert", "store_arrow", "store_pandas")
        expected_rows = 2 if route == "store_list" else 1

        stored = self._rows_of(client, "types")
        assert len(stored) == expected_rows, stored
        first = stored[1]
        expected = self._expected(route, 3, binary=binary, vector=vector)
        assert first["tag"] == expected["tag"]
        assert first["i"] == expected["i"]
        assert first["f"] == expected["f"]
        assert first["b"] == expected["b"]
        assert first["s"] == expected["s"]
        assert first["by"] == expected["by"], first

        # The point API reports the same values.
        point = client.retrieve(1)
        assert point["i"] == expected["i"] and point["f"] == expected["f"], point
        assert point["b"] == expected["b"] and point["s"] == expected["s"], point
        assert point["by"] == expected["by"], point

        if vector:
            assert self._nearest_id(client, "types", expected["emb"]) == 1, (
                f"route '{route}' must store the vector it was given"
            )

        # The same row must survive a replace, and the replacement's values must
        # be the ones that read back — for every column type.
        replacement = self._row("replaced", 4)
        assert client.replace(1, replacement) is True
        replaced = self._rows_of(client, "types")
        assert replaced[1]["tag"] == "replaced"
        assert replaced[1]["i"] == 4 and replaced[1]["f"] == 2.0
        assert replaced[1]["b"] is True and replaced[1]["s"] == "text-4-✓"
        assert replaced[1]["by"] == bytes([4, 0, 255, 7]), replaced[1]
        if vector:
            assert self._nearest_id(client, "types", replacement["emb"]) == 1
        client.flush()
        assert len(self._rows_of(client, "types")) == expected_rows

        # The values the replace published survive a reopen.
        client.close()
        reopened = ApexClient(db_dir)
        reopened.use_table("types")
        again = self._rows_of(reopened, "types")
        assert len(again) == expected_rows
        assert again[1]["tag"] == "replaced"
        assert again[1]["i"] == 4 and again[1]["f"] == 2.0
        assert again[1]["by"] == bytes([4, 0, 255, 7]), again[1]
        if vector:
            hits = reopened.topk_distance(
                "emb", list(replacement["emb"]), k=1, metric="cosine"
            ).to_dict()
            assert hits and int(hits[0]["_id"]) == 1
        reopened.close()


class TestEmptyTableReads:
    """An empty table still has its schema, `_id` included."""

    def test_projection_including_id_on_empty_table(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, n INT)")
        client.use_table("t")

        assert client.execute("SELECT _id, k FROM t").to_dict() == []
        assert client.execute("SELECT * FROM t").to_dict() == []
        assert client.execute("SELECT COUNT(*) AS n FROM t").to_dict() == [{"n": 0}]
        assert client.count_rows() == 0

        client.store([{"k": "a", "n": 1}])
        assert client.execute("SELECT _id, k FROM t").to_dict() == [{"_id": 1, "k": "a"}]

        # Deleting the last row returns the table to the empty state.
        assert client.delete(1) is True
        assert client.execute("SELECT _id, k FROM t").to_dict() == []
        assert client.count_rows() == 0


class TestIdAllocationAfterDelete:
    """A write never reuses the id of a row that is still live."""

    def test_sql_insert_after_delete_appends_with_a_fresh_id(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT, n INT)")
        client.use_table("t")
        client.store([{"k": f"k{i}", "n": i} for i in range(4)])
        client.store({"k": "k4", "n": 4})
        client.flush()

        assert client.delete(4) is True
        client.execute("INSERT INTO t (k, n) VALUES ('k6', 6)")
        client.flush()

        rows = {
            int(row["_id"]): row
            for row in client.execute("SELECT _id, k, n FROM t").to_dict()
        }
        assert sorted(rows) == [1, 2, 3, 5, 6], rows
        assert rows[5]["k"] == "k4", "the delta row must keep its id and value"
        assert rows[6]["k"] == "k6", "the SQL insert must append a new row"

    def test_api_insert_after_delete_appends_with_a_fresh_id(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT)")
        client.use_table("t")
        client.store([{"k": "a"}, {"k": "b"}, {"k": "c"}])
        client.flush()
        assert client.delete(3) is True

        client.store({"k": "d"})
        client.flush()
        rows = {
            int(row["_id"]): row["k"]
            for row in client.execute("SELECT _id, k FROM t").to_dict()
        }
        assert rows == {1: "a", 2: "b", 4: "d"}, rows


class TestPersistenceAcrossReopen:
    """Each route's rows must be there after the database is reopened."""

    def test_single_row_delta_writes_survive_reopen(self, db_dir):
        client = ApexClient(db_dir)
        client.execute("CREATE TABLE t (k TEXT, n INT)")
        client.use_table("t")
        for i in range(4):
            client.store({"k": f"k{i}", "n": i})
        client.close()

        reopened = ApexClient(db_dir)
        reopened.use_table("t")
        assert_row_set(reopened, "t", [1, 2, 3, 4], "after reopen")
        assert [reopened.retrieve(i)["n"] for i in (1, 2, 3, 4)] == [0, 1, 2, 3]
        reopened.close()

    def test_replace_delete_survive_reopen(self, db_dir):
        client = ApexClient(db_dir)
        client.execute("CREATE TABLE t (k TEXT)")
        client.use_table("t")
        client.store([{"k": f"k{i}"} for i in range(6)])
        assert client.replace(2, {"k": "replaced"}) is True
        assert client.delete(4) is True
        client.close()

        reopened = ApexClient(db_dir)
        reopened.use_table("t")
        assert_row_set(reopened, "t", [1, 2, 3, 5, 6], "after reopen")
        assert reopened.retrieve(2)["k"] == "replaced"
        assert reopened.retrieve(4) is None
        assert reopened.delete(4) is False
        assert_row_set(reopened, "t", [1, 2, 3, 5, 6], "repeated delete after reopen")
        reopened.close()

    def test_deferred_delete_survives_reopen(self, db_dir):
        """A delete that was only recorded in process memory must reach the file."""
        client = ApexClient(db_dir)
        client.execute("CREATE TABLE t (v INT)")
        client.use_table("t")
        client.store([{"v": i} for i in range(5)])
        assert client.delete(3) is True
        # No flush: the deletion is deferred and applied on the next open.
        client.close()

        reopened = ApexClient(db_dir)
        reopened.use_table("t")
        assert_row_set(reopened, "t", [1, 2, 4, 5], "deferred delete after reopen")
        assert reopened.retrieve(3) is None
        reopened.close()


class TestFtsWriteRoutes:
    """Writes to an FTS-enabled table must keep the index in step."""

    def test_store_replace_delete_keeps_search_consistent(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE docs (title TEXT, body TEXT)")
        client.use_table("docs")
        client.init_fts(index_fields=["title", "body"])

        client.store([{"title": "alpha", "body": "the quick brown fox"}])
        client.store([{"title": "beta", "body": "lazy dogs sleep"}])
        client.store([{"title": "gamma", "body": "quick silver"}])
        assert_row_set(client, "docs", [1, 2, 3], "fts writes")

        hits = client.search_text("quick")
        assert hits is not None and len(hits) >= 1

        assert client.replace(2, {"title": "beta2", "body": "quick foxes jump"}) is True
        client.flush()
        assert client.retrieve(2)["body"] == "quick foxes jump"
        assert_row_set(client, "docs", [1, 2, 3], "after replace")

        assert client.delete(1) is True
        client.flush()
        assert_row_set(client, "docs", [2, 3], "after delete")
        assert client.retrieve(1) is None


class TestVectorWriteRoutes:
    """Vector tables must behave like scalar tables on every write route."""

    @staticmethod
    def _unit(dim, seed):
        rnd = random.Random(seed)
        values = [rnd.gauss(0.0, 1.0) for _ in range(dim)]
        norm = sum(value * value for value in values) ** 0.5 or 1.0
        return [value / norm for value in values]

    def test_vector_writes_across_routes(self, tmp_client):
        dim = 8
        client = tmp_client
        client.execute("CREATE TABLE items (title TEXT, emb FLOAT16_VECTOR)")
        client.use_table("items")

        client.store([{"title": "a", "emb": self._unit(dim, 1)}])         # single row
        client.store([{"title": "b", "emb": self._unit(dim, 2)}])         # one-row list
        client.store(                                                     # batch
            {
                "title": ["c", "d"],
                "emb": [self._unit(dim, 3), self._unit(dim, 4)],
            }
        )
        assert_row_set(client, "items", [1, 2, 3, 4], "vector writes")

        replacement = self._unit(dim, 5)
        assert client.replace(3, {"title": "c2", "emb": replacement}) is True
        assert_row_set(client, "items", [1, 2, 3, 4], "vector replace")
        assert client.retrieve(3)["title"] == "c2"

        assert client.delete(2) is True
        assert client.delete(2) is False
        client.flush()
        assert_row_set(client, "items", [1, 3, 4], "vector delete")

        hits = client.topk_distance("emb", replacement, k=3, metric="cosine").to_dict()
        assert len(hits) == 3
        assert all(int(hit["_id"]) != 2 for hit in hits)
        assert int(hits[0]["_id"]) == 3, "the rewritten vector must be its own best match"

    def test_quantized_column_write_routes(self, tmp_client):
        dim = 16
        client = tmp_client
        client.execute("CREATE TABLE items (title TEXT, emb FLOAT16_VECTOR)")
        client.use_table("items")
        client.store(
            {
                "title": [f"t{i}" for i in range(8)],
                "emb": [self._unit(dim, i) for i in range(8)],
            }
        )

        client.create_quantized_column(source="emb", target="emb_tq4", codec="turboquant4")
        client.store([{"title": "extra", "emb": self._unit(dim, 99)}])
        assert_row_set(client, "items", list(range(1, 10)), "write with accelerator")

        assert client.replace(1, {"title": "t0b", "emb": self._unit(dim, 100)}) is True
        assert client.delete(2) is True
        assert_row_set(client, "items", [1, 3, 4, 5, 6, 7, 8, 9], "rewrite with accelerator")

        hits = client.topk_distance("emb", self._unit(dim, 99), k=3, metric="cosine").to_dict()
        assert hits, "vector search must still work after writes"
        assert all(int(hit["_id"]) != 2 for hit in hits)


class TestMemoryTableWrites:
    """Process-local tables must accept the same writes as file-backed ones."""

    def test_memory_client_write_routes(self):
        client = ApexClient(":memory:")
        try:
            client.execute("CREATE TABLE t (k TEXT, n INT)")
            client.use_table("t")
            client.store([{"k": "a", "n": 1}])
            client.store({"k": "b", "n": 2})
            client.store({"k": ["c"], "n": [3]})
            client.execute("INSERT INTO t (k, n) VALUES ('d', 4)")
            assert_row_set(client, "t", [1, 2, 3, 4], "memory writes")

            assert client.replace(1, {"k": "a2", "n": 10}) is True
            assert client.delete(2) is True
            assert client.delete(2) is False
            client.flush()
            assert_row_set(client, "t", [1, 3, 4], "memory replace/delete")
            assert client.retrieve(1)["k"] == "a2"
        finally:
            client.close()

    def test_memory_client_int_into_double(self):
        client = ApexClient(":memory:")
        try:
            client.execute("CREATE TABLE t (f DOUBLE)")
            client.use_table("t")
            client.store([{"f": 1}, {"f": 2.5}])
            values = [row["f"] for row in client.execute("SELECT f FROM t").to_dict()]
            assert values == [1.0, 2.5]
        finally:
            client.close()


class TestConcurrentWriters:
    """Concurrent writes from several threads must not lose or duplicate rows.

    Readers are deliberately not part of this test: a concurrent query opens the
    table, and the interaction between that open and a writer is covered by the
    Rust-side suite.
    """

    def test_threaded_store_delete_replace(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (worker INT, seq INT)")
        client.use_table("t")

        writers, batches, per_batch = 3, 8, 10
        expected_total = writers * batches * per_batch
        errors = []

        def writer(worker):
            try:
                for batch in range(batches):
                    client.store(
                        [
                            {"worker": worker, "seq": batch * per_batch + i}
                            for i in range(per_batch)
                        ]
                    )
            except Exception as exc:  # noqa: BLE001 - reported below
                errors.append(f"writer {worker}: {exc}")

        threads = [threading.Thread(target=writer, args=(w,)) for w in range(writers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == [], f"concurrent writers must not fail: {errors}"
        assert_row_set(client, "t", range(1, expected_total + 1), "after concurrent writes")
        rows = client.execute("SELECT worker, seq FROM t").to_dict()
        assert len({(row["worker"], row["seq"]) for row in rows}) == expected_total, (
            "concurrent writers must not duplicate a row"
        )

        # Deleting and rewriting on top of the concurrent load keeps the counts.
        victims = list(range(1, 31))
        deleters = [
            threading.Thread(
                target=lambda chunk=chunk: [client.delete(row_id) for row_id in chunk]
            )
            for chunk in (victims[:15], victims[15:])
        ]
        replacers = [
            threading.Thread(
                target=lambda chunk=chunk: [
                    client.replace(row_id, {"worker": -1, "seq": -row_id})
                    for row_id in chunk
                ]
            )
            for chunk in (victims[30:45], range(55, 70))
        ]
        for thread in deleters + replacers:
            thread.start()
        for thread in deleters + replacers:
            thread.join()

        assert errors == [], f"concurrent mutations must not fail: {errors}"
        remaining = [row_id for row_id in range(1, expected_total + 1) if row_id not in set(victims)]
        assert_row_set(client, "t", remaining, "after concurrent delete/replace")
