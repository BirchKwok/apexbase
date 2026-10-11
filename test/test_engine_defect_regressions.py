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


class TestOrderByTopKNullOrdering:
    """The mmap top-K lane must not rank NULLs as a placeholder value.

    Defect: ``scan_top_k_indices_mmap`` decided whether to skip NULL rows from
    the persisted zone-map ``has_nulls`` flag, which every zone-map writer
    stores as ``false``. The lane therefore ranked each NULL as its stored
    placeholder (0). ``SELECT * FROM t ORDER BY v LIMIT 3`` on
    ``[NULL, NULL, NULL, 5, 6, 7, 8, 9, 10, 11]`` returned the three NULL rows
    (ascending) and dropped them entirely (descending), instead of following the
    documented ``NULLS FIRST``/``NULLS LAST`` default — NULLs sort last when
    ascending and first when descending.

    The lane now reads the row group's real null bitmap and declines the scan
    (falling back to the general path) when the sort column holds any NULL.
    """

    @staticmethod
    def _table(client, values, dtype="int64"):
        """A file-backed table whose base row groups carry zone maps."""
        client.execute(f"CREATE TABLE t (v {dtype}, tag TEXT)")
        client.use_table("t")
        client.store({"v": values, "tag": [f"r{i}" for i in range(len(values))]})
        client.flush()
        return client

    @staticmethod
    def _ordered(client, sql):
        return [row["v"] for row in client.execute(sql).to_dict()]

    VALUES = [None, None, None, 5, 6, 7, 8, 9, 10, 11]

    def test_ascending_returns_smallest_real_values(self, tmp_client):
        # Was: [None, None, None] because each NULL ranked as 0.
        self._table(tmp_client, self.VALUES)
        assert self._ordered(tmp_client, "SELECT * FROM t ORDER BY v LIMIT 3") == [5, 6, 7]

    def test_descending_returns_nulls_first(self, tmp_client):
        # Was: [11, 10, 9] because each NULL ranked as the smallest value.
        self._table(tmp_client, self.VALUES)
        rows = self._ordered(tmp_client, "SELECT * FROM t ORDER BY v DESC LIMIT 3")
        assert rows == [None, None, None]

    def test_full_ordering_matches_ascending_then_nulls(self, tmp_client):
        self._table(tmp_client, self.VALUES)
        rows = self._ordered(tmp_client, "SELECT * FROM t ORDER BY v LIMIT 20")
        assert rows == [5, 6, 7, 8, 9, 10, 11, None, None, None]

    def test_explicit_nulls_first_and_last(self, tmp_client):
        self._table(tmp_client, self.VALUES)
        assert self._ordered(
            tmp_client, "SELECT * FROM t ORDER BY v ASC NULLS FIRST LIMIT 3"
        ) == [None, None, None]
        assert self._ordered(
            tmp_client, "SELECT * FROM t ORDER BY v DESC NULLS LAST LIMIT 3"
        ) == [11, 10, 9]

    def test_float_column_takes_the_same_guard(self, tmp_client):
        self._table(tmp_client, [None, None, 1.5, 2.5, 3.5], dtype="float64")
        assert self._ordered(
            tmp_client, "SELECT * FROM t ORDER BY v LIMIT 2"
        ) == [1.5, 2.5]

    def test_multi_row_group_table_with_nulls_in_later_group(self, tmp_client):
        # 70k rows crosses the 65,536-row row-group boundary, so the NULLs land
        # in a later group than the first candidates: the lane has to decline the
        # whole request rather than return a partially wrong heap.
        values = [float(i) for i in range(70_000)] + [None, None]
        self._table(tmp_client, values, dtype="float64")
        rows = self._ordered(tmp_client, "SELECT * FROM t ORDER BY v LIMIT 3")
        assert rows == [0.0, 1.0, 2.0]

    def test_null_free_column_keeps_the_fast_path_result(self, tmp_client):
        # The guard must not disturb null-free columns.
        self._table(tmp_client, [9, 3, 7, 1, 5])
        assert self._ordered(tmp_client, "SELECT * FROM t ORDER BY v LIMIT 3") == [1, 3, 5]
        assert self._ordered(tmp_client, "SELECT * FROM t ORDER BY v DESC LIMIT 2") == [9, 7]


class TestGroupByOrderByNullOrdering:
    """GROUP BY ... ORDER BY ... LIMIT must place NULL groups like a plain sort.

    Defect: the grouped path's small-k single-key fast paths scored a NULL group
    key as the extreme value (``i64::MIN``/``f64::NEG_INFINITY``), and the
    grouped comparator derived NULL placement from the sort direction instead of
    the requested ``NULLS FIRST``/``NULLS LAST``. A descending sort therefore
    returned the NULL group last, and an explicit ``NULLS FIRST`` on an ascending
    sort was ignored.
    """

    @staticmethod
    def _grouped(client):
        client.execute("CREATE TABLE t (g INT, v INT)")
        client.use_table("t")
        rows = [{"g": 1, "v": None}, {"g": 1, "v": None}, {"g": 1, "v": 5},
                {"g": 1, "v": 6}, {"g": 1, "v": 7}, {"g": 1, "v": 8},
                {"g": 2, "v": 9}, {"g": 2, "v": 10}]
        for row in rows:
            client.store(row)
        client.flush()

    @staticmethod
    def _keys(client, order_clause):
        rows = client.execute(
            f"SELECT v, COUNT(*) AS c FROM t GROUP BY v ORDER BY {order_clause} LIMIT 3"
        ).to_dict()
        return [row["v"] for row in rows]

    def test_descending_puts_the_null_group_first(self, tmp_client):
        self._grouped(tmp_client)
        assert self._keys(tmp_client, "v DESC") == [None, 10, 9]

    def test_ascending_puts_the_null_group_last(self, tmp_client):
        self._grouped(tmp_client)
        assert self._keys(tmp_client, "v") == [5, 6, 7]

    def test_explicit_nulls_first_is_honoured(self, tmp_client):
        self._grouped(tmp_client)
        assert self._keys(tmp_client, "v ASC NULLS FIRST") == [None, 5, 6]

    def test_explicit_nulls_last_on_descending_is_honoured(self, tmp_client):
        self._grouped(tmp_client)
        assert self._keys(tmp_client, "v DESC NULLS LAST") == [10, 9, 8]


class TestNarrowColumnIndexedExtraction:
    """A Float32 column stored at V5 width must survive every read lane.

    The indexed extraction lane (candidate rows -> projected columns) decodes
    each column at index. Float32 values are four bytes wide there, so a column
    read through an 8-byte stride produced an all-NULL result: any filter over
    that column then matched nothing and the query returned zero rows. The
    shapes below need two projected numeric columns and more than 500 candidate
    rows, which is what selects that lane.
    """

    # More than 500 candidate rows plus two projected numeric columns is what
    # selects the indexed extraction lane this class guards.
    ROWS = 2400

    @staticmethod
    def _f32(value):
        import struct

        return struct.unpack("f", struct.pack("f", value))[0]

    def _store(self, client):
        cities = ["North", "South", "East", "West"]
        rows = [
            {
                "city": cities[i % 4],
                "age": i % 13,
                "score": self._f32((i % 11) * 0.5),
            }
            for i in range(self.ROWS)
        ]
        client.create_table("narrow", {"city": "string", "age": "int", "score": "float"})
        client.use_table("narrow")
        client.store(rows)
        client.flush()
        return rows

    def _reference(self, rows, predicate):
        groups = {}
        for row in rows:
            if not predicate(row):
                continue
            total, count, minimum = groups.setdefault(row["city"], [0.0, 0, row["age"]])
            groups[row["city"]] = [total + row["score"], count + 1, min(minimum, row["age"])]
        return groups

    def test_two_numeric_aggregates_over_a_filtered_narrow_column(self, tmp_client):
        rows = self._store(tmp_client)

        def predicate(row):
            return (
                row["city"] in ("North", "South") or row["age"] in (3, 7, 11)
            ) and row["score"] >= 1.0

        groups = self._reference(rows, predicate)
        assert len(groups) == 4

        query = (
            "SELECT city, SUM(score) AS total, MIN(age) AS youngest FROM narrow "
            "WHERE (city IN ('North', 'South') OR age IN (3, 7, 11)) AND score >= 1.0 "
            "GROUP BY city ORDER BY city"
        )
        actual = {
            row["city"]: (round(row["total"], 5), row["youngest"])
            for row in tmp_client.execute(query).to_dict()
        }
        expected = {
            city: (round(total, 5), youngest)
            for city, (total, _count, youngest) in groups.items()
        }
        assert actual == expected

    def test_second_aggregate_column_is_never_dropped(self, tmp_client):
        # The same lane with SUM over the second numeric column: both projected
        # columns must carry real values, not nulls.
        rows = self._store(tmp_client)
        query = (
            "SELECT city, SUM(score) AS total, SUM(age) AS age_total FROM narrow "
            "WHERE (city IN ('North', 'South') OR age IN (3, 7, 11)) AND score >= 1.0 "
            "GROUP BY city ORDER BY city"
        )
        actual = {
            row["city"]: (round(row["total"], 5), row["age_total"])
            for row in tmp_client.execute(query).to_dict()
        }

        def predicate(row):
            return (
                row["city"] in ("North", "South") or row["age"] in (3, 7, 11)
            ) and row["score"] >= 1.0

        expected = {}
        for row in rows:
            if not predicate(row):
                continue
            total, age_total = expected.setdefault(row["city"], [0.0, 0])
            expected[row["city"]] = [total + row["score"], age_total + row["age"]]
        expected = {city: (round(total, 5), age) for city, (total, age) in expected.items()}
        assert actual == expected

    def test_having_over_a_second_aggregate_matches_the_reference(self, tmp_client):
        rows = self._store(tmp_client)
        query = (
            "SELECT city, COUNT(*) AS n, SUM(score) AS total FROM narrow "
            "WHERE (city IN ('North', 'South') OR age IN (3, 7, 11)) AND score >= 1.0 "
            "GROUP BY city HAVING COUNT(*) > 5 AND MIN(age) >= 0 ORDER BY city"
        )
        actual = [
            (row["city"], row["n"], round(row["total"], 5))
            for row in tmp_client.execute(query).to_dict()
        ]

        def predicate(row):
            return (
                row["city"] in ("North", "South") or row["age"] in (3, 7, 11)
            ) and row["score"] >= 1.0

        expected = []
        for city, (total, count, youngest) in sorted(
            self._reference(rows, predicate).items()
        ):
            if count > 5 and youngest >= 0:
                expected.append((city, count, round(total, 5)))
        assert actual == expected
        assert expected


class TestHavingAggregateWithoutAValueLane:
    """A HAVING-only value aggregate must never reach the fused kernel bare.

    The fused group-by resolves at most one *value lane* (the column a
    SUM/AVG/MIN/MAX reads). A HAVING clause can inject an aggregate the SELECT
    list never asked for, leaving that lane unresolved while the aggregate mask
    still requested a value. The lookup-table scatter loop then copied the
    aggregate value from a null pointer: the three shapes below crashed the
    interpreter (SIGSEGV) instead of returning rows. They must now match a
    plain-Python reference.
    """

    ROWS = 1200

    def _store(self, client):
        cities = ["North", "South", "East", "West", "Center"]
        rows = [
            {"city": cities[i % 5], "age": i % 13, "score": (i % 9) * 0.5}
            for i in range(self.ROWS)
        ]
        client.create_table("lane", {"city": "string", "age": "int", "score": "float"})
        client.use_table("lane")
        client.store(rows)
        client.flush()
        return rows

    @staticmethod
    def _matches(row):
        return (
            row["city"] in ("North", "South") or row["age"] in (3, 7, 11)
        ) and row["score"] >= 1.0

    def _reference(self, rows):
        groups = {}
        for row in rows:
            if not self._matches(row):
                continue
            count, youngest, oldest = groups.setdefault(
                row["city"], [0, row["age"], row["age"]]
            )
            groups[row["city"]] = [
                count + 1,
                min(youngest, row["age"]),
                max(oldest, row["age"]),
            ]
        return groups

    def _assert_shape(self, client, rows, having, keep):
        query = (
            "SELECT city, COUNT(*) AS n FROM lane "
            "WHERE (city IN ('North', 'South') OR age IN (3, 7, 11)) AND score >= 1.0 "
            f"GROUP BY city HAVING {having} ORDER BY city"
        )
        actual = [(row["city"], row["n"]) for row in client.execute(query).to_dict()]
        expected = sorted(
            (city, count)
            for city, (count, youngest, oldest) in self._reference(rows).items()
            if keep(count, youngest, oldest)
        )
        assert expected, "the fixture must keep at least one group"
        assert actual == expected

    def test_having_min_over_an_unprojected_column(self, tmp_client):
        rows = self._store(tmp_client)
        self._assert_shape(
            tmp_client,
            rows,
            "COUNT(*) > 5 AND MIN(age) >= 0",
            lambda count, youngest, oldest: count > 5 and youngest >= 0,
        )

    def test_having_max_over_an_unprojected_column(self, tmp_client):
        rows = self._store(tmp_client)
        self._assert_shape(
            tmp_client,
            rows,
            "MAX(age) >= 0",
            lambda count, youngest, oldest: oldest >= 0,
        )

    def test_having_min_only(self, tmp_client):
        rows = self._store(tmp_client)
        self._assert_shape(
            tmp_client,
            rows,
            "MIN(age) >= 0",
            lambda count, youngest, oldest: youngest >= 0,
        )


class TestColumnarMaterializationKeepsRichTypes:
    """The Python columnar fast path must not silently drop rich column types.

    `to_dict()` on a projected scan used to report `None` for every column whose
    Arrow type had no branch in the pylist converter: timestamps, dates, and
    vector columns. The converter now refuses such types so the caller takes the
    Arrow path, which decodes them.
    """

    def test_timestamp_and_date_projection_returns_values(self, tmp_client):
        import datetime

        tmp_client.create_table(
            "events", {"ts": "timestamp", "d": "date", "v": "int64"}
        )
        tmp_client.use_table("events")
        tmp_client.store(
            {
                "ts": [1_700_000_000_000_000, 1_700_000_001_000_000],
                "d": [19_000, 19_001],
                "v": [1, 2],
            }
        )
        tmp_client.flush()

        rows = tmp_client.execute("SELECT ts, d, v FROM events").to_dict()
        assert isinstance(rows[0]["ts"], datetime.datetime)
        assert isinstance(rows[0]["d"], datetime.date)
        assert rows[0]["v"] == 1
        # A projection of just the rich column keeps it too.
        projected = tmp_client.execute("SELECT ts FROM events").to_dict()
        assert all(isinstance(row["ts"], datetime.datetime) for row in projected)
