"""Merged reads must compose `[persisted base][append delta][buffered rows]`.

An in-place `DELETE` on a V4 file spills the buffered append rows to the delta
sidecar and truncates the write buffer, which then holds *only* the rows appended
after the delete. The merged reader used to slice that tail-only buffer as if it
were the persisted base: every persisted row disappeared from scans, filters and
`_id` reads while `count(*)` (answered from metadata) stayed correct — silent row
loss rather than an error. These tests pin the composed view against an
independent oracle in both the pre-flush and the flushed state.

The second defect fixed alongside it: the buffered NULL bitmap starts at the first
*appended* row in that state, so base NULL cells were read from the pending bitmap
and the appended NULL cells landed on the wrong rows.
"""

import shutil
import statistics
import tempfile
import time

from apexbase import ApexClient

ROWS = 30_000
DELETED = ROWS // 60  # delete `a < DELETED`, a prefix of the first row group
APPENDED = 200
APPEND_BASE = 500_000


def _fixture():
    """A flushed base with NULL cells in `b`, plus an oracle row list."""
    root = tempfile.mkdtemp(prefix="apexbase_merged_read_")
    client = ApexClient(root, drop_if_exists=True, enable_cache=False)
    client.create_table("t", schema={"a": "int64", "b": "int64", "s": "string"})
    client.use_table("t")
    rows = [
        {
            "a": i,
            "b": None if i % 11 == 0 else i % 977,
            "s": f"v{i % 50}",
        }
        for i in range(ROWS)
    ]
    client.store(
        {
            "a": [row["a"] for row in rows],
            "b": [row["b"] for row in rows],
            "s": [row["s"] for row in rows],
        }
    )
    client.flush()
    return root, client, rows


def _append(client, rows, count=APPENDED):
    base = max(row["a"] for row in rows) + 1 if rows else APPEND_BASE
    for i in range(count):
        row = {
            "a": base + i,
            "b": None if i % 3 == 0 else i,
            "s": f"n{i % 7}",
        }
        client.store(row)
        rows.append(row)


def _scan(client):
    """The full scan as `{a: (b, s)}`, exercising the merged column reader."""
    return {row["a"]: (row["b"], row["s"]) for row in client.execute("SELECT a, b, s FROM t").to_dict()}


def _oracle(rows):
    return {row["a"]: (row["b"], row["s"]) for row in rows}


def _assert_view(client, rows, tag):
    assert _scan(client) == _oracle(rows), f"{tag}: full scan diverged from the oracle"
    count = client.execute("SELECT count(*) AS n FROM t").to_dict()[0]["n"]
    assert count == len(rows), f"{tag}: count(*)={count} expected {len(rows)}"
    nulls = client.execute("SELECT count(*) AS n FROM t WHERE b IS NULL").to_dict()[0]["n"]
    expected_nulls = len([1 for row in rows if row["b"] is None])
    assert nulls == expected_nulls, f"{tag}: NULL count {nulls} expected {expected_nulls}"
    got = client.execute("SELECT count(*) AS n, sum(b) AS sb FROM t WHERE a > 100").to_dict()[0]
    values = [row["b"] for row in rows if row["a"] > 100 and row["b"] is not None]
    assert got["n"] == len([1 for row in rows if row["a"] > 100]), f"{tag}: filtered count"
    assert got["sb"] == sum(values), f"{tag}: filtered sum"
    # `_id` projections take their own path and must stay aligned with the values.
    by_id = {
        row["_id"]: row["a"]
        for row in client.execute("SELECT _id, a FROM t").to_dict()
    }
    assert sorted(by_id.values()) == sorted(row["a"] for row in rows), f"{tag}: _id sets"
    return by_id


def _assert_stable_across_flush(client, rows, tag):
    pre = _assert_view(client, rows, f"{tag} (buffered)")
    ids = [row["a"] for row in client.execute("SELECT a FROM t ORDER BY a LIMIT 5").to_dict()]
    client.flush()
    post = _assert_view(client, rows, f"{tag} (flushed)")
    assert pre == post, f"{tag}: buffered and flushed `_id` views differ"
    # Ordering and windows must slice the same composed view.
    assert ids == [row["a"] for row in client.execute("SELECT a FROM t ORDER BY a LIMIT 5").to_dict()]
    window = client.execute("SELECT a FROM t ORDER BY a LIMIT 7 OFFSET 5").to_dict()
    expected = sorted(row["a"] for row in rows)[5:12]
    assert [row["a"] for row in window] == expected, f"{tag}: active window"


def test_persisted_delete_then_buffered_appends_keep_the_base_visible():
    root, client, rows = _fixture()
    try:
        del rows[:DELETED]
        client.execute(f"DELETE FROM t WHERE a < {DELETED}")
        _append(client, rows)
        _assert_stable_across_flush(client, rows, "delete-then-append")
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_buffered_appends_then_persisted_delete_keep_the_base_visible():
    root, client, rows = _fixture()
    try:
        _append(client, rows)
        del rows[:DELETED]
        client.execute(f"DELETE FROM t WHERE a < {DELETED}")
        # Here the delete spills the whole append buffer first.
        _append(client, rows)
        _assert_stable_across_flush(client, rows, "append-delete-append")
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_repeated_delete_append_rounds_keep_the_base_visible():
    root, client, rows = _fixture()
    try:
        for _ in range(3):
            client.execute("DELETE FROM t WHERE a > 1000 AND a < 1100")
            rows[:] = [row for row in rows if not 1000 < row["a"] < 1100]
            _append(client, rows)
        _assert_stable_across_flush(client, rows, "repeated-rounds")
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_bounded_reads_do_not_materialize_the_table_after_a_delete():
    """A persisted deletion vector must not turn every bounded read into a scan.

    `read_columns_to_arrow` used to force `(0, None)` into the physical reader and
    slice afterwards whenever the table carried a persisted deletion vector, so
    *every* bounded read materialized the whole table. On a 200k x 16 plain table
    that made `LIMIT 0` cost 15.28 ms against 0.017 ms without a deletion vector,
    `LIMIT 100` 15.39 vs 0.13 ms, and a three-column `LIMIT 100` 2.16 vs
    0.015 ms. It is also why `_id = <missing or deleted>` became a full scan: the
    point lane resolves an absent row with a zero-row schema probe.
    """
    root = tempfile.mkdtemp()
    plain = None
    try:
        for layout in ("t_nodel", "t_del"):
            client = ApexClient(root, drop_if_exists=False, enable_cache=False)
            client.create_table(layout)
            client.use_table(layout)
            client.store(
                {
                    "v": list(range(1, ROWS + 1)),
                    "s": [f"v{index % 97}" for index in range(1, ROWS + 1)],
                    "f": [index * 0.5 for index in range(1, ROWS + 1)],
                    "b": [index % 2 == 0 for index in range(1, ROWS + 1)],
                }
            )
            client.flush()
            client.close()

        # Delete the very first row (so an off-by-one window cannot pass), one
        # inside the window (so the partial decode has to reach past it), and one
        # row-group boundary's worth in.
        plain = ApexClient(root, drop_if_exists=False, enable_cache=False)
        plain.use_table("t_del")
        for removed in (1, 3, 17, ROWS // 2):
            plain.execute(f"DELETE FROM t_del WHERE _id = {removed}")
        plain.flush()

        empty = plain._storage.execute("SELECT * FROM t_del LIMIT 0")
        columns = empty.get("columns_dict") if isinstance(empty, dict) else None
        assert columns is not None
        assert {"v", "s"} <= set(columns), sorted(columns)
        assert all(len(values) == 0 for values in columns.values())

        # The window is taken over the surviving rows, not the physical ones, and
        # every column decodes exactly the values the full scan reports for the
        # same prefix -- this is the guard for the narrowed partial decode (a
        # wrong physical cover would shift or truncate the surviving values).
        full = plain.execute("SELECT v, s, f, b FROM t_del").to_dict()
        assert [row["v"] for row in full[:6]] == [2, 4, 5, 6, 7, 8]
        for limit in (1, 2, 5, 100):
            window = plain.execute(
                f"SELECT v, s, f, b FROM t_del LIMIT {limit}"
            ).to_dict()
            assert window == full[:limit], (limit, window[:2], full[:2])
            projected = plain.execute(
                f"SELECT f, b FROM t_del LIMIT {limit}"
            ).to_dict()
            assert projected == [
                {"f": row["f"], "b": row["b"]} for row in full[:limit]
            ], (limit, projected[:2])

        # A non-zero start keeps the merged read-and-slice: the window lanes are
        # only active-space for a pure prefix, so this pins that the offset still
        # counts surviving rows rather than physical ones.
        offset = plain.execute("SELECT v FROM t_del LIMIT 3 OFFSET 2").to_dict()
        assert [row["v"] for row in offset] == [row["v"] for row in full[2:5]]

        # A deleted id answers empty, and quickly.
        deleted_result = plain._storage.execute("SELECT v FROM t_del WHERE _id = 1")
        assert deleted_result.get("columns_dict") == {"v": []}, deleted_result
        deleted = time.perf_counter()
        for _ in range(20):
            plain._storage.execute("SELECT v FROM t_del WHERE _id = 1")
        deleted_ms = (time.perf_counter() - deleted) * 1000.0 / 20.0
        assert deleted_ms < 1.0, f"deleted-id lookup took {deleted_ms:.3f} ms"

        # Cost guard: the deleted twin must stay in the same order of magnitude as
        # the table without a deletion vector (it used to be ~117x).
        def median_ms(table, sql, iterations=15):
            plain.use_table(table)
            for _ in range(3):
                plain._storage.execute(sql)
            samples = []
            for _ in range(iterations):
                start = time.perf_counter()
                plain._storage.execute(sql)
                samples.append((time.perf_counter() - start) * 1000.0)
            return statistics.median(samples)

        clean = median_ms("t_nodel", "SELECT * FROM t_nodel LIMIT 100")
        removed = median_ms("t_del", "SELECT * FROM t_del LIMIT 100")
        assert removed <= max(clean * 20.0, 1.5), (
            f"LIMIT 100 cost {removed:.3f} ms on the deleted twin against "
            f"{clean:.3f} ms without a deletion vector"
        )
    finally:
        if plain is not None:
            plain.close()
        shutil.rmtree(root, ignore_errors=True)


def test_prefix_windows_apply_pending_cells_and_deletes():
    """A prefix window must stay windowed while cells and a delete are pending.

    Pending cells patch values, not row identity, so the window can be pushed into
    the reader exactly as it is on a clean table -- the reader merges the
    DeltaStore onto the windowed rows (both a delete and a patched cell inside the
    window have to be honoured). Requiring a clean cell overlay made `LIMIT n`
    read the whole table and slice: 57.5 ms against 0.15 ms on a 200k x 16 plain
    table. Appended (unflushed or spooled) rows still keep the merged read, since
    they extend the visible view past the base.
    """
    for flush_delete, flush_update in ((True, False), (False, False), (True, True), (False, True)):
        root = tempfile.mkdtemp()
        try:
            client = ApexClient(root, drop_if_exists=True, enable_cache=False)
            client.create_table("t", schema={"v": "int64", "s": "string"})
            client.use_table("t")
            client.store(
                {
                    "v": list(range(1, ROWS + 1)),
                    "s": [f"v{index % 97}" for index in range(1, ROWS + 1)],
                }
            )
            client.flush()
            # A delete and two patched cells, all inside the probed windows: the
            # first row, the fifth row, and a row only reached by the wider limits.
            client.execute("DELETE FROM t WHERE _id = 1")
            if flush_delete:
                client.flush()
            client.execute("UPDATE t SET v = 999 WHERE _id = 5")
            client.execute("UPDATE t SET s = 'patched' WHERE _id = 300")
            if flush_update:
                client.flush()

            full = client.execute("SELECT v, s FROM t").to_dict()
            for limit in (1, 2, 3, 4, 5, 6, 7, 8, 10, 100, 299, 300, 301, 1000):
                got = client.execute(f"SELECT v, s FROM t LIMIT {limit}").to_dict()
                assert got == full[:limit], (flush_delete, flush_update, limit)
            projected = client.execute("SELECT s FROM t LIMIT 10").to_dict()
            assert projected == [{"s": row["s"]} for row in full[:10]]
        finally:
            shutil.rmtree(root, ignore_errors=True)


def test_zero_row_probe_stays_bounded_while_cells_are_pending():
    """A schema probe must not materialize the table while an overlay is pending.

    `read_columns_to_arrow_inner` keeps the merged read-and-slice whenever cells
    are pending (the active view cannot be windowed physically), and that branch
    forces `(0, None)` into the physical reader. A zero-row window has nothing to
    re-map onto the active view, but it took that branch too: on a 200k x 16 plain
    table with one unflushed DELETE plus one unflushed UPDATE the probe cost
    58.6 ms against 0.018 ms on the clean table. The point lane proves an absent id
    through exactly this probe, so `_id = <missing>` and `_id = <deleted>` became
    full scans (55-57 ms against 0.03 ms).
    """
    root = tempfile.mkdtemp()
    try:
        client = ApexClient(root, drop_if_exists=True, enable_cache=False)
        client.create_table("t", schema={"v": "int64", "s": "string"})
        client.use_table("t")
        client.store(
            {
                "v": list(range(1, ROWS + 1)),
                "s": [f"v{index % 97}" for index in range(1, ROWS + 1)],
            }
        )
        client.flush()
        # One pending delete plus one pending cell update: the state that used to
        # push bounded reads and absent-id lookups back onto the full scan.
        client.execute("DELETE FROM t WHERE _id = 1")
        client.execute("UPDATE t SET v = 0 WHERE _id = 2")

        def median_ms(sql, iterations=9):
            for _ in range(3):
                client._storage.execute(sql)
            samples = []
            for _ in range(iterations):
                start = time.perf_counter()
                client._storage.execute(sql)
                samples.append((time.perf_counter() - start) * 1000.0)
            return statistics.median(samples)

        empty = client._storage.execute("SELECT * FROM t LIMIT 0")
        columns = empty.get("columns_dict") if isinstance(empty, dict) else None
        assert columns is not None, empty
        assert len(columns["_id"]) == 0
        assert len(columns["v"]) == 0
        assert len(columns["s"]) == 0
        assert median_ms("SELECT * FROM t LIMIT 0") < 1.0

        # `1` is deleted and `ROWS + 500` was never present; both must be answered
        # from the point lane instead of a scan.
        for ident in (1, ROWS + 500):
            got = client.execute(f"SELECT v FROM t WHERE _id = {ident}").to_dict()
            assert got == [], (ident, got)
            assert median_ms(f"SELECT v FROM t WHERE _id = {ident}") < 1.0

        # The pending update is still visible, and the probe did not disturb it.
        assert client.execute("SELECT v FROM t WHERE _id = 2").to_dict() == [{"v": 0}]
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_aggregates_skip_fully_deleted_row_groups():
    """A row group whose every row is deleted must not be walked row by row.

    The aggregate lane scans row groups and checks the deletion bitmap per row,
    so a fully deleted group still cost a full scalar pass. A range delete used
    to slow a whole-table aggregate by the size of the deleted range: on 200k
    rows, deleting 199999 took `sum` 0.34 ms against 0.13 ms once those groups
    are skipped. The values must be unchanged.
    """
    root = tempfile.mkdtemp()
    try:
        for compression in (None, "zstd"):
            client = ApexClient(root, drop_if_exists=True, enable_cache=False)
            client.create_table("t", schema={"v": "int64", "s": "string"})
            client.use_table("t")
            if compression:
                assert client.set_compression(compression) is not False
            client.store(
                {
                    "v": list(range(ROWS)),
                    "s": [f"v{index % 97}" for index in range(ROWS)],
                }
            )
            client.flush()
            # A contiguous range: the deleted rows cover whole row groups.
            client.execute(f"DELETE FROM t WHERE v < {ROWS - 5}")
            client.flush()

            surviving = list(range(ROWS - 5, ROWS))
            got = client.execute(
                "SELECT count(v) AS n, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t"
            ).to_dict()[0]
            assert got["n"] == len(surviving), (compression, got)
            assert abs(got["s"] - sum(surviving)) < 1e-6, (compression, got)
            assert got["mn"] == float(min(surviving))
            assert got["mx"] == float(max(surviving))
            # And the surviving rows still read back by id.
            rows = client.execute("SELECT v FROM t LIMIT 5").to_dict()
            assert [row["v"] for row in rows] == surviving, (compression, rows)
            client.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_aggregates_correct_a_sparse_deletion_vector():
    """A sparse deletion vector must not push an aggregate back to a full scan.

    The sidecar statistics describe every *physical* row, so a delete-only edit is
    one subtraction away from the active answer; without that correction the mere
    existence of one deleted row moved `sum` from the statistics read to a
    whole-table scan (200k rows: 0.0297 ms -> 1.4545 ms). The values must stay
    exact -- including min/max, which subtraction cannot recover when a deleted row
    held the extreme, and therefore fall back to the scan.
    """
    root = tempfile.mkdtemp()
    try:
        for compression in (None, "zstd"):
            client = ApexClient(root, drop_if_exists=True, enable_cache=False)
            client.create_table("t", schema={"c0": "int64", "c1": "int64", "c2": "float64"})
            client.use_table("t")
            if compression:
                assert client.set_compression(compression) is not False
            client.store(
                {
                    "c0": list(range(ROWS)),
                    "c1": list(range(ROWS)),
                    "c2": [None if index % 5 == 0 else index * 1.5 for index in range(ROWS)],
                }
            )
            client.flush()

            def oracle():
                rows = client.execute("SELECT c1, c2 FROM t").to_dict()
                c1 = [row["c1"] for row in rows]
                c2 = [row["c2"] for row in rows if row["c2"] is not None]
                return c1, c2

            def assert_matches(tag):
                c1, c2 = oracle()
                got = client.execute(
                    "SELECT count(c1) AS n, sum(c1) AS s, min(c1) AS mn, max(c1) AS mx, "
                    "avg(c1) AS a, min(c2) AS m2, max(c2) AS x2 FROM t"
                ).to_dict()[0]
                assert got["n"] == len(c1), (tag, got, len(c1))
                assert abs(got["s"] - sum(c1)) < 1e-6, (tag, got["s"], sum(c1))
                assert got["mn"] == float(min(c1)), (tag, got["mn"], min(c1))
                assert got["mx"] == float(max(c1)), (tag, got["mx"], max(c1))
                assert abs(got["a"] - sum(c1) / len(c1)) < 1e-6, (tag, got["a"])
                assert got["m2"] == min(c2), (tag, got["m2"], min(c2))
                assert got["x2"] == max(c2), (tag, got["x2"], max(c2))

            # A sparse deletion in the middle: no deleted value touches an extreme,
            # so count/sum/avg *and* min/max all come from the corrected statistics.
            client.execute("DELETE FROM t WHERE c0 >= 5000 AND c0 < 5003")
            client.flush()
            assert_matches("middle")

            # Deleting a value that *is* the extreme cannot be corrected by
            # subtraction; min/max must still be right (via the scan fallback).
            client.execute("DELETE FROM t WHERE c0 = 0")
            client.execute(f"DELETE FROM t WHERE c0 = {ROWS - 1}")
            client.flush()
            assert_matches("extremes")
            client.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_pagination_reads_only_the_page():
    """`LIMIT n OFFSET m` must not materialize the table for the page.

    A non-zero `start_row` was declined by the reader's bounded lanes, so the page
    offset meant a full table read: on 200k x 16 the second page cost 9.2 ms
    against 0.018 ms once the covered prefix is read and sliced. The values must
    be identical either way, including when the page runs past the end.
    """
    columns = 16
    rows = 30_000
    root = tempfile.mkdtemp()
    try:
        for compression in (None, "zstd"):
            client = ApexClient(root, drop_if_exists=True, enable_cache=False)
            schema = {f"c{index}": "int64" for index in range(columns)}
            client.create_table("t", schema=schema)
            client.use_table("t")
            if compression:
                assert client.set_compression(compression) is not False
            client.store({name: list(range(rows)) for name in schema})
            client.flush()

            full = client.execute("SELECT * FROM t").to_dict()
            for limit, offset in ((1, 0), (5, 1), (10, 100), (100, rows - 5), (5, rows + 3)):
                got = client.execute(
                    f"SELECT * FROM t LIMIT {limit} OFFSET {offset}"
                ).to_dict()
                assert got == full[offset : offset + limit], (compression, limit, offset)

            def median_ms(sql, iterations=7):
                for _ in range(3):
                    client._storage.execute(sql)
                samples = []
                for _ in range(iterations):
                    start = time.perf_counter()
                    client._storage.execute(sql)
                    samples.append((time.perf_counter() - start) * 1000.0)
                return statistics.median(samples)

            whole = median_ms("SELECT c0, c1, c2, c3 FROM t")
            page = median_ms("SELECT c0, c1, c2, c3 FROM t LIMIT 10 OFFSET 1")
            # The page is a 10-row prefix plus a slice; it must stay far below the
            # cost of materializing every row.
            assert page < whole * 0.5 + 0.5, (compression, page, whole)
            client.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)
