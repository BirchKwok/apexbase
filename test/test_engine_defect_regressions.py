"""Regression tests for engine defects found while writing the usage examples.

Each test here pins down a concrete, previously-reproduced wrong behaviour. The
comments record the failure mode so a future regression is easy to recognise.

Covers (Python-visible side):
  * aggregate-free ``GROUP BY``                (was: one row per input row)
  * outer-join ``IS NULL`` anti-join pushdown  (was: matched rows returned too)
  * ``COUNT(*)`` through the batch/join paths  (scalar reads)
  * vector-table ``replace`` + ``delete``      (was: delete silently ignored)
  * ``replace`` + ``delete`` on plain tables   (regression guard: must keep working)
  * concurrent readers sharing a writer's table (contract guard for the Rust-side
    reader/writer race fixed in ``embedded::Table::execute``)
"""

import os
import shutil
import tempfile
import threading

import pytest

from apexbase import ApexClient


@pytest.fixture
def tmp_client():
    """A fresh file-backed client per test (some defects are file-path specific)."""
    tmpdir = tempfile.mkdtemp()
    client = ApexClient(os.path.join(tmpdir, "db"))
    try:
        yield client
    finally:
        client.close()
        shutil.rmtree(tmpdir, ignore_errors=True)


def _store_group_table(client):
    """``t2`` holding exactly two distinct ``cat`` values with two rows each."""
    client.execute("CREATE TABLE t2 (cat TEXT, v INT)")
    client.use_table("t2")
    client.store(
        [
            {"cat": "x", "v": 1},
            {"cat": "x", "v": 2},
            {"cat": "y", "v": 3},
            {"cat": "y", "v": 4},
        ]
    )


class TestAggregateFreeGroupBy:
    """A GROUP BY with no aggregate in the projection must still group."""

    def test_single_group_key_only(self, tmp_client):
        _store_group_table(tmp_client)
        rows = tmp_client.execute("SELECT cat FROM t2 GROUP BY cat").to_dict()
        assert sorted(r["cat"] for r in rows) == ["x", "y"]

    def test_single_group_key_with_order_by(self, tmp_client):
        _store_group_table(tmp_client)
        rows = tmp_client.execute("SELECT cat FROM t2 GROUP BY cat ORDER BY cat").to_dict()
        assert [r["cat"] for r in rows] == ["x", "y"]

    def test_group_key_alias(self, tmp_client):
        _store_group_table(tmp_client)
        rows = tmp_client.execute("SELECT cat AS c FROM t2 GROUP BY cat").to_dict()
        assert sorted(r["c"] for r in rows) == ["x", "y"]

    def test_numeric_group_key(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t4 (g INT)")
        client.use_table("t4")
        client.store([{"g": 1}, {"g": 1}, {"g": 2}, {"g": 1}, {"g": 2}])
        rows = client.execute("SELECT g FROM t4 GROUP BY g").to_dict()
        assert sorted(r["g"] for r in rows) == [1, 2]

    def test_multi_column_group_key(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t5 (g INT, h TEXT)")
        client.use_table("t5")
        client.store(
            [
                {"g": 1, "h": "a"},
                {"g": 2, "h": "b"},
                {"g": 2, "h": "c"},
                {"g": 2, "h": "b"},
            ]
        )
        rows = client.execute("SELECT g, h FROM t5 GROUP BY g, h").to_dict()
        assert sorted((r["g"], r["h"]) for r in rows) == [(1, "a"), (2, "b"), (2, "c")]

    def test_group_by_with_aggregate_still_works(self, tmp_client):
        # Regression guard: the aggregate path must be unaffected.
        _store_group_table(tmp_client)
        rows = tmp_client.execute(
            "SELECT cat, COUNT(*) AS n FROM t2 GROUP BY cat ORDER BY cat"
        ).to_dict()
        assert [(r["cat"], r["n"]) for r in rows] == [("x", 2), ("y", 2)]

    def test_order_by_non_projected_column(self, tmp_client):
        # ORDER BY a column that is not projected must still order the output.
        client = tmp_client
        client.execute("CREATE TABLE t6 (cat TEXT, v INT)")
        client.use_table("t6")
        client.store(
            [
                {"cat": "y", "v": 4},
                {"cat": "x", "v": 1},
                {"cat": "y", "v": 3},
                {"cat": "x", "v": 2},
            ]
        )
        asc = [r["cat"] for r in client.execute("SELECT cat FROM t6 ORDER BY v").to_dict()]
        desc = [
            r["cat"] for r in client.execute("SELECT cat FROM t6 ORDER BY v DESC").to_dict()
        ]
        assert asc == ["x", "x", "y", "y"]
        assert desc == ["y", "y", "x", "x"]


class TestOuterJoinIsNullAntiJoin:
    """``IS NULL`` on an outer join's nullable side must survive the join.

    The predicate is TRUE exactly on the null-extended rows, so pushing it below
    the join removes the rows the anti-join is supposed to return.
    """

    @pytest.fixture
    def users_orders(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE users (uid INT, name TEXT)")
        client.use_table("users")
        client.store(
            [
                {"uid": 1, "name": "a"},
                {"uid": 2, "name": "b"},
                {"uid": 3, "name": "c"},
                {"uid": 4, "name": "d"},
            ]
        )
        client.execute("CREATE TABLE orders (oid INT, uid INT, tag TEXT)")
        client.use_table("orders")
        client.store(
            [
                {"oid": 10, "uid": 1, "tag": "p"},
                {"oid": 11, "uid": 2, "tag": "r"},
            ]
        )
        return client

    def test_left_join_anti_join(self, users_orders):
        rows = users_orders.execute(
            "SELECT u.uid FROM users u LEFT JOIN orders o ON u.uid = o.uid "
            "WHERE o.uid IS NULL ORDER BY u.uid"
        ).to_dict()
        assert [r["uid"] for r in rows] == [3, 4]

    def test_left_join_row_multiplication_is_preserved(self, tmp_client):
        # Two orders for uid=1 must produce two rows, not one.
        client = tmp_client
        client.execute("CREATE TABLE users (uid INT)")
        client.use_table("users")
        client.store([{"uid": 1}, {"uid": 2}, {"uid": 3}])
        client.execute("CREATE TABLE orders (oid INT, uid INT)")
        client.use_table("orders")
        client.store([{"oid": 10, "uid": 1}, {"oid": 11, "uid": 1}, {"oid": 12, "uid": 2}])
        rows = client.execute(
            "SELECT u.uid, o.oid FROM users u LEFT JOIN orders o ON u.uid = o.uid "
            "ORDER BY u.uid, o.oid"
        ).to_dict()
        assert [r["uid"] for r in rows] == [1, 1, 2, 3]

    def test_right_join_anti_join(self, users_orders):
        rows = users_orders.execute(
            "SELECT o.oid FROM users u RIGHT JOIN orders o ON u.uid = o.uid "
            "WHERE u.uid IS NULL ORDER BY o.oid"
        ).to_dict()
        # Every order matched a user, so there are no orphans.
        assert rows == []

    def test_full_join_anti_join(self, users_orders):
        rows = users_orders.execute(
            "SELECT u.uid, o.oid FROM users u FULL JOIN orders o ON u.uid = o.uid "
            "WHERE o.oid IS NULL ORDER BY u.uid"
        ).to_dict()
        assert [r["uid"] for r in rows] == [3, 4]

    def test_left_join_count_anti_join(self, users_orders):
        count = users_orders.execute(
            "SELECT COUNT(*) AS n FROM users u LEFT JOIN orders o ON u.uid = o.uid "
            "WHERE o.uid IS NULL"
        ).to_dict()
        assert count[0]["n"] == 2

    def test_null_rejecting_pushdown_still_correct(self, users_orders):
        # `IS NOT NULL` and equality are TRUE only for real matches, so they are
        # pushed below the join; make sure that optimisation stays correct.
        not_null = users_orders.execute(
            "SELECT u.uid FROM users u LEFT JOIN orders o ON u.uid = o.uid "
            "WHERE o.uid IS NOT NULL ORDER BY u.uid"
        ).to_dict()
        assert [r["uid"] for r in not_null] == [1, 2]

        equal = users_orders.execute(
            "SELECT u.uid FROM users u LEFT JOIN orders o ON u.uid = o.uid "
            "WHERE o.tag = 'p' ORDER BY u.uid"
        ).to_dict()
        assert [r["uid"] for r in equal] == [1]


class TestScalarCounts:
    """``COUNT(*)`` must be readable regardless of the execution path."""

    def test_count_on_plain_table(self, tmp_client):
        _store_group_table(tmp_client)
        assert tmp_client.count_rows() == 4

    def test_count_via_execute(self, tmp_client):
        _store_group_table(tmp_client)
        rows = tmp_client.execute("SELECT COUNT(*) AS n FROM t2").to_dict()
        assert rows[0]["n"] == 4

    def test_count_distinct(self, tmp_client):
        _store_group_table(tmp_client)
        rows = tmp_client.execute("SELECT COUNT(DISTINCT cat) AS n FROM t2").to_dict()
        assert rows[0]["n"] == 2

    def test_count_on_temp_table(self, tmp_client):
        # Temp tables take the batch path, which historically returned no scalar.
        tmpdir = tempfile.mkdtemp()
        csv_path = os.path.join(tmpdir, "src.csv")
        with open(csv_path, "w", encoding="utf-8") as handle:
            handle.write("a,b\n1,x\n2,y\n3,z\n")
        try:
            tmp_client.register_temp_table("src", csv_path)
            tmp_client.use_table("src")
            rows = tmp_client.execute("SELECT COUNT(*) AS n FROM src").to_dict()
            assert rows[0]["n"] == 3
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestReplaceDeleteInteraction:
    """``replace`` followed by ``delete`` must behave consistently."""

    def test_plain_table_replace_then_delete(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (k TEXT)")
        client.use_table("t")
        client.store([{"k": f"k{i}"} for i in range(5)])

        assert client.replace(3, {"k": "replaced"}) is True
        assert client.count_rows() == 5
        # Deleting the row that was just replaced must remove it.
        assert client.delete(3) is True
        client.flush()
        visible = {int(r["_id"]) for r in client.execute("SELECT _id FROM t").to_dict()}
        assert 3 not in visible
        assert client.count_rows() == 4

    def test_repeated_replace_delete_cycles(self, tmp_client):
        """Each replace/delete cycle must keep counts and visibility exact.

        The Rust embedded path used to duplicate rows here (`5 -> replace ->
        delete` counted 9) and then leave the rewritten row unreachable through
        the point-read APIs. The Python client goes through its own bindings, so
        this pins the shared contract down on the Python side.
        """
        client = tmp_client
        client.execute("CREATE TABLE cycles (k TEXT)")
        client.use_table("cycles")
        client.store([{"k": f"k{i}"} for i in range(6)])

        live = list(range(1, 7))
        for row_id in (1, 3, 5):
            assert client.replace(row_id, {"k": f"replaced-{row_id}"}) is True
            assert client.count_rows() == len(live), "replace must not change the row count"
            assert client.retrieve(row_id)["k"] == f"replaced-{row_id}"
            # A flush must not invalidate the row that was just rewritten.
            client.flush()
            assert client.retrieve(row_id)["k"] == f"replaced-{row_id}"

            assert client.delete(row_id) is True
            live.remove(row_id)
            assert client.count_rows() == len(live), "delete must remove exactly one row"
            assert client.retrieve(row_id) is None, "the deleted row must be gone"

        # Every remaining row is present exactly once, through both APIs.
        rows = client.execute("SELECT _id, k FROM cycles").to_dict()
        assert sorted(int(r["_id"]) for r in rows) == live
        assert client.count_rows() == len(live)

    def test_vector_table_replace_then_delete(self, tmp_client):
        """Vector tables must behave the same as plain tables here.

        This used to fail: ``delete`` returned False, the row count stayed the
        same and the row remained visible in ``SELECT`` and ``topk_distance``.
        """
        import math
        import random

        rnd = random.Random(11)

        def unit(dim):
            values = [rnd.gauss(0.0, 1.0) for _ in range(dim)]
            norm = math.sqrt(sum(v * v for v in values)) or 1.0
            return [v / norm for v in values]

        dim = 16
        count = 20
        client = tmp_client
        client.execute("CREATE TABLE items (title TEXT, emb FLOAT16_VECTOR)")
        client.use_table("items")
        client.store(
            {
                "title": [f"t{i}" for i in range(count)],
                "emb": [unit(dim) for _ in range(count)],
            }
        )
        client.create_quantized_column(source="emb", target="emb_tq4", codec="turboquant4")

        victim = 10
        replacement = unit(dim)
        assert client.replace(victim, {"title": "replaced", "emb": replacement}) is True
        client.flush()

        before = client.count_rows()
        assert client.delete(victim) is True
        client.flush()

        visible = {int(r["_id"]) for r in client.execute("SELECT _id FROM items").to_dict()}
        assert victim not in visible, "delete after replace must remove the row"
        assert client.count_rows() == before - 1

        # It must also disappear from vector search results.
        hits = client.topk_distance("emb", replacement, k=3, metric="cosine").to_dict()
        assert all(int(hit["_id"]) != victim for hit in hits)


class TestConcurrentReadersWithWriter:
    """Readers querying a table *while* it is being written must not disturb it.

    The Rust embedded path had a real defect here: ``Table::execute`` flushed a
    table that still had rows buffered in memory, concurrently with the writer's
    own flush, and both rewrote the base file through one shared scratch path
    (``<table>.apex.tmp``). The loser failed with ``No such file or directory``
    and the row count disagreed with what was written.

    The Python client was never reachable that way — its writes take a
    cross-process file lock and its reads never flush — so this test guards the
    contract itself on the Python side: interleaved readers must see a consistent
    table, report no error, and leave every written row present exactly once.
    """

    def test_readers_run_while_writers_append(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE rw (worker INT, seq INT)")
        client.use_table("rw")

        writers_n, batches, per_batch, readers_n = 3, 10, 10, 2
        expected = writers_n * batches * per_batch

        errors = []
        stop = threading.Event()
        read_counts = []

        def reader():
            while not stop.is_set():
                try:
                    rows = client.execute("SELECT COUNT(*) AS n FROM rw").to_dict()
                    read_counts.append(rows[0]["n"])
                except Exception as exc:  # noqa: BLE001 - reported below
                    errors.append(f"reader: {exc}")

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

        readers = [threading.Thread(target=reader) for _ in range(readers_n)]
        for thread in readers:
            thread.start()
        writers = [
            threading.Thread(target=writer, args=(worker,))
            for worker in range(writers_n)
        ]
        for thread in writers:
            thread.start()
        for thread in writers:
            thread.join()
        stop.set()
        for thread in readers:
            thread.join()

        assert errors == [], f"concurrent access must not raise: {errors}"
        assert read_counts, "the reader threads must actually run queries"

        assert client.count_rows() == expected
        counted = client.execute("SELECT COUNT(*) AS n FROM rw").to_dict()
        assert counted[0]["n"] == expected

        rows = client.execute("SELECT worker, seq FROM rw").to_dict()
        assert len({(r["worker"], r["seq"]) for r in rows}) == expected, (
            "concurrent writers must not duplicate a row"
        )


class TestWriteRoutesKeepValues:
    """Every write route must keep NULLs and non-scalar columns.

    ``StorageEngine::classify_write`` sends a V4 table down the V4 write path and a
    legacy table to the append-only delta file; the delta encoding carries no null
    bitmap and only scalar columns. Backend materialization used to record
    ``is_v4: false`` regardless of the actual file, so the next single-row write to
    a V4 table was misrouted and silently replaced NULLs with column defaults (and
    dropped vector columns).
    """

    def test_nulls_survive_every_write_route(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE n (s TEXT, v INT)")
        client.use_table("n")

        # 1. first write creates the file, 2. a single-row write into the
        # materialized table (the route that used to be misclassified),
        # 3. a batch write, 4. another single-row write.
        client.store([{"s": "a", "v": 1}])
        client.store([{"s": None, "v": None}])
        client.store([{"s": "c", "v": 3}, {"s": None, "v": None}])
        client.store([{"s": None, "v": None}])

        rows = client.execute("SELECT _id, s, v FROM n ORDER BY _id").to_dict()
        assert [r["s"] for r in rows] == ["a", None, "c", None, None]
        assert [r["v"] for r in rows] == [1, None, 3, None, None]
        assert client.retrieve(2)["s"] is None, "a single-row write must keep its NULL"

    def test_vector_rows_stay_retrievable(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE vec (name TEXT, emb FLOAT16_VECTOR)")
        client.use_table("vec")
        client.store(
            {
                "name": ["a", "b"],
                "emb": [[1.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0]],
            }
        )
        # A single-row write into the materialized table: the misrouted delta
        # encoding dropped non-scalar columns entirely.
        client.store({"name": ["c"], "emb": [[0.0, 0.0, 1.0, 0.0]]})

        assert client.count_rows() == 3
        for row_id in (1, 2, 3):
            stored = client.retrieve(row_id)
            assert stored is not None, f"row {row_id} must be retrievable"
            assert stored["name"] == ["a", "b", "c"][row_id - 1]

        # The vectors themselves are readable through the search path.
        hits = client.topk_distance(
            "emb", [1.0, 0.0, 0.0, 0.0], k=3, metric="cosine"
        ).to_dict()
        assert len(hits) == 3


class TestWindowExpressionExpansion:
    """Window functions must compose like ordinary expressions.

    Previously `ROUND(SUM(x) OVER (...), 2)` failed with a parse error and
    `SUM(x) OVER (...) / 2` silently returned the *un-divided* window value,
    because the surrounding expression was dropped.
    """

    @pytest.fixture
    def window_client(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE s (g TEXT, amt DOUBLE)")
        client.use_table("s")
        client.store(
            [
                {"g": "a", "amt": 10.0},
                {"g": "a", "amt": 20.0},
                {"g": "b", "amt": 30.0},
            ]
        )
        return client

    def test_plain_window_aggregate(self, window_client):
        rows = window_client.execute(
            "SELECT g, SUM(amt) OVER (PARTITION BY g) AS t FROM s ORDER BY g, amt"
        ).to_dict()
        assert [r["t"] for r in rows] == [30.0, 30.0, 30.0]

    def test_window_nested_in_scalar_function(self, window_client):
        # The wrapper (ROUND) must actually be applied.
        rows = window_client.execute(
            "SELECT g, ROUND(SUM(amt) OVER (PARTITION BY g), 2) AS t FROM s "
            "ORDER BY g, amt"
        ).to_dict()
        assert [r["t"] for r in rows] == [30.0, 30.0, 30.0]

    def test_window_in_arithmetic(self, window_client):
        # Regression: this used to return 30.0 (the raw sum) instead of 15.0.
        rows = window_client.execute(
            "SELECT g, SUM(amt) OVER (PARTITION BY g) / 2 AS t FROM s ORDER BY g, amt"
        ).to_dict()
        assert [r["t"] for r in rows] == [15.0, 15.0, 15.0]

    def test_rank_window_functions(self, window_client):
        rows = window_client.execute(
            "SELECT g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY amt) AS rn FROM s "
            "ORDER BY g, amt"
        ).to_dict()
        assert [r["rn"] for r in rows] == [1, 2, 1]

    def test_rows_frame_running_total(self, window_client):
        # ROWS frames are evaluated over physical row positions.
        rows = window_client.execute(
            "SELECT amt, SUM(amt) OVER (ORDER BY amt "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run FROM s "
            "ORDER BY amt"
        ).to_dict()
        assert [r["run"] for r in rows] == [10.0, 30.0, 60.0]

    def test_window_aggregate_partitions_independently(self, window_client):
        rows = window_client.execute(
            "SELECT g, AVG(amt) OVER (PARTITION BY g) AS a FROM s ORDER BY g, amt"
        ).to_dict()
        assert [r["a"] for r in rows] == [15.0, 15.0, 30.0]


class TestWindowFramesAndLimits:
    """Frames and the ROW_NUMBER limit pushdown.

    Two problems used to hide here:
      * the executor ignored frames entirely, so a *bounded* frame silently
        returned the whole-partition value (a plausible-looking wrong answer);
      * the `ROW_NUMBER() <= k` pushdown gathered retained rows with positions
        that could reference rows outside the evaluated partition batch, which
        panicked inside arrow's `take`.
    """

    @pytest.fixture
    def ordered_client(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE t (k DOUBLE, amt DOUBLE)")
        client.use_table("t")
        client.store([{"k": float(i), "amt": float(i * 10)} for i in range(1, 6)])
        return client

    def test_cumulative_frame_running_total(self, ordered_client):
        rows = ordered_client.execute(
            "SELECT k, SUM(amt) OVER (ORDER BY k "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run FROM t ORDER BY k"
        ).to_dict()
        assert [r["run"] for r in rows] == [10.0, 30.0, 60.0, 100.0, 150.0]

    def test_default_frame_matches_cumulative(self, ordered_client):
        rows = ordered_client.execute(
            "SELECT k, SUM(amt) OVER (ORDER BY k) AS run FROM t ORDER BY k"
        ).to_dict()
        assert [r["run"] for r in rows] == [10.0, 30.0, 60.0, 100.0, 150.0]

    def test_bounded_frame_is_rejected_not_silently_ignored(self, ordered_client):
        # Returning the whole-partition average here would be wrong but
        # plausible; an explicit error is the honest behaviour.
        with pytest.raises(Exception) as excinfo:
            ordered_client.execute(
                "SELECT k, AVG(amt) OVER (ORDER BY k "
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS m FROM t ORDER BY k"
            ).to_dict()
        assert "frame" in str(excinfo.value).lower()

    @pytest.fixture
    def partitioned_client(self, tmp_client):
        client = tmp_client
        client.execute("CREATE TABLE p (region TEXT, rep TEXT, revenue DOUBLE)")
        client.use_table("p")
        client.store(
            [{"region": "e", "rep": f"r{i}", "revenue": float(100 - i * 7)} for i in range(6)]
            + [{"region": "w", "rep": f"s{i}", "revenue": float(90 - i * 5)} for i in range(6)]
        )
        return client

    @staticmethod
    def _top3_sql(cmp_op):
        return (
            "SELECT region, rep, rn FROM ("
            "  SELECT region, rep, "
            "         ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS rn "
            "  FROM p"
            f") x WHERE rn {cmp_op}"
        )

    def test_row_number_limit_filter_does_not_panic(self, partitioned_client):
        # `rn <= 3` used to panic with `index out of bounds` inside take.
        for cmp_op in ["<= 3", "< 4", "BETWEEN 1 AND 3"]:
            rows = partitioned_client.execute(self._top3_sql(cmp_op)).to_dict()
            assert len(rows) == 6, f"{cmp_op} should return 3 rows per region"
            assert sorted(r["rn"] for r in rows) == sorted([1, 2, 3] * 2)
