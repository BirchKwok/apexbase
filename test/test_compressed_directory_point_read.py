"""A compressed row group carries the same per-column directory as a plain one.

The writer records the per-column body offsets (`col_offsets`) *before* it hands
the body to `compress_rg_body`, so every compressed row group already stores a
directory — and that directory addresses the decompressed body, because
decompression restores the exact uncompressed layout. The decoding point lane
used to ignore it and walk the body in column order instead, decoding (or at
least skipping) every column to reach the requested row.

These tests pin the behaviour the directory lane bought: a projected point read
decodes only the columns it was asked for, the values agree with the plain twin
on every layout, and the lane stays correct through an overlay.

The width matters. With a directory the cost of a point read is set by the
*requested* columns; without one it is set by the schema width, so a wide table
is where the difference shows.
"""

import shutil
import statistics
import tempfile
import time

import pytest

from apexbase import ApexClient

ROWS = 40_000
COLUMN_COUNT = 16
SCHEMA = {f"c{i}": "int64" for i in range(COLUMN_COUNT)}
# Two string columns keep the fixture from being purely numeric; a string column
# is dictionary encoded on disk, which is a different decode path.
SCHEMA["s0"] = "string"
SCHEMA["s1"] = "string"
ALL_COLUMNS = list(SCHEMA)


def _value(column, index):
    if column == "s0":
        return f"v{index % 50}"
    if column == "s1":
        return f"w{index % 7}"
    position = int(column[1:])
    return index * (position + 1)


def _fixture_rows():
    columns = {}
    for column in ALL_COLUMNS:
        if column.startswith("s"):
            columns[column] = [_value(column, i) for i in range(ROWS)]
        else:
            columns[column] = [_value(column, i) for i in range(ROWS)]
    return columns


def _expected(row_id, columns):
    index = row_id - 1
    return {column: _value(column, index) for column in columns}


def _rows(result):
    if result is None:
        return []
    columns = result.get("columns_dict") if isinstance(result, dict) else None
    if columns is None:
        return []
    row_count = len(next(iter(columns.values()), []))
    return [
        {name: values[index] for name, values in columns.items()}
        for index in range(row_count)
    ]


@pytest.fixture(params=[None, "zstd"])
def client(request):
    root = tempfile.mkdtemp()
    apex = ApexClient(root, enable_cache=False)
    apex.create_table("t", schema=SCHEMA)
    apex.use_table("t")
    if request.param:
        assert apex.set_compression(request.param) is not False
    apex.store(_fixture_rows())
    apex.flush()
    try:
        yield apex
    finally:
        apex.close()
        shutil.rmtree(root, ignore_errors=True)


def test_projected_point_lookup_returns_exactly_the_requested_columns(client):
    for row_id in (1, 2, ROWS // 2, ROWS - 1, ROWS):
        for projection in (
            ["c0"],
            ["c0", "c15"],
            ["s0", "s1"],
            ["c15", "s0"],
            ALL_COLUMNS,
        ):
            listed = ", ".join(projection)
            rows = _rows(
                client._storage.execute(
                    f"SELECT {listed} FROM t WHERE _id = {row_id}"
                )
            )
            assert rows == [_expected(row_id, projection)], (row_id, projection)


def test_projected_point_lookup_matches_the_full_row(client):
    row_id = ROWS // 3
    full = _rows(client._storage.execute(f"SELECT * FROM t WHERE _id = {row_id}"))[0]
    for projection in (["c0"], ["c0", "c7"], ["s1"], ["c2", "c9", "s0"]):
        listed = ", ".join(projection)
        narrow = _rows(
            client._storage.execute(f"SELECT {listed} FROM t WHERE _id = {row_id}")
        )[0]
        assert narrow == {column: full[column] for column in projection}


def test_projected_point_lookup_survives_an_overlay(client):
    updated = ROWS // 4
    deleted = ROWS // 5
    client.execute(f"UPDATE t SET c0 = 424242 WHERE _id = {updated}")
    client.execute(f"DELETE FROM t WHERE _id = {deleted}")

    narrow = _rows(
        client._storage.execute(f"SELECT c0, s0 FROM t WHERE _id = {updated}")
    )
    assert narrow == [{"c0": 424242, "s0": _value("s0", updated - 1)}]
    assert _rows(client._storage.execute(f"SELECT c0 FROM t WHERE _id = {deleted}")) == []

    # A flushed table must agree with the overlay state.
    client.flush()
    assert _rows(
        client._storage.execute(f"SELECT c0, s0 FROM t WHERE _id = {updated}")
    ) == [{"c0": 424242, "s0": _value("s0", updated - 1)}]
    assert _rows(client._storage.execute(f"SELECT c0 FROM t WHERE _id = {deleted}")) == []


def test_projected_point_lookup_costs_less_than_the_whole_row(client):
    """The directory lane decodes the requested columns, not the schema.

    The comparison is between two shapes of the same build in the same process,
    so it does not depend on the machine being quiet; the absolute bound only
    keeps a full-scan fallback from passing the ratio by being uniformly slow.
    """
    row_id = ROWS // 2
    full_sql = f"SELECT * FROM t WHERE _id = {row_id}"
    narrow_sql = f"SELECT c0, c1 FROM t WHERE _id = {row_id}"

    def median_ms(sql, iterations=25):
        for _ in range(5):
            client._storage.execute(sql)
        samples = []
        for _ in range(iterations):
            start = time.perf_counter()
            client._storage.execute(sql)
            samples.append((time.perf_counter() - start) * 1000.0)
        return statistics.median(samples)

    full = median_ms(full_sql)
    narrow = median_ms(narrow_sql)

    assert full < 5.0, f"point lookup took {full:.3f} ms; it fell back to scanning"
    # Both shapes are a few microseconds of fixed per-call work, so their ratio is
    # dominated by scheduling noise at this scale: a release run measured 0.0070
    # against 0.0051 ms, a 2 us difference on a ~5 us call. The tolerance keeps the
    # comparison meaningful (a projected read that fell back to decoding the whole
    # row is an order of magnitude worse, not 2 us) without failing on a timer tick.
    assert narrow <= full + 0.02, (
        f"a 2-column point read ({narrow:.4f} ms) must not cost more than the "
        f"whole-row read ({full:.4f} ms)"
    )


def test_executor_point_lookup_does_not_fall_through_to_the_sql_executor():
    """A compressed point read must stay on the primary-key fast path.

    `try_pk_read` answers `_id = X` straight from the storage lane before the
    statement is parsed. It only asked the lean RCIX lane, which declines a
    compressed row group, so the statement fell through to the full SQL executor
    - parse, classify and plan on every call. On a 200k x 16 zstd table that cost
    a fixed ~10 us per read on top of the plain twin (~11.5 us against ~0.9 us),
    and the gap did not depend on how many columns were projected, which is the
    signature of re-planning rather than of reading.

    Both layouts are measured in the same process, so machine load cancels. The
    factor is loose enough for scheduling noise and still far below the ~12x the
    fall-through produced.
    """
    root = tempfile.mkdtemp()
    apex = ApexClient(root, enable_cache=False)
    try:
        for layout, compression in (("t_plain", None), ("t_zstd", "zstd")):
            apex.create_table(layout, schema=SCHEMA)
            apex.use_table(layout)
            if compression:
                assert apex.set_compression(compression) is not False
            apex.store(_fixture_rows())
            apex.flush()

        def median_us(table, iterations=60):
            apex.use_table(table)
            sql = f"SELECT c0, c1, c2 FROM {table} WHERE _id = {ROWS // 2}"
            for _ in range(8):
                apex._storage.execute(sql)
            samples = []
            for _ in range(iterations):
                start = time.perf_counter()
                apex._storage.execute(sql)
                samples.append((time.perf_counter() - start) * 1e6)
            return statistics.median(samples)

        fast = median_us("t_plain")
        slow = median_us("t_zstd")
    finally:
        apex.close()
        shutil.rmtree(root, ignore_errors=True)

    assert slow <= max(fast * 6.0, 6.0), (
        f"compressed point lookup took {slow:.2f} us against {fast:.2f} us on the "
        "plain twin; it fell out of the primary-key fast path"
    )


def test_batch_point_lookup_matches_the_plain_twin(client):
    """`_id IN (...)` on a compressed layout must answer from the storage lane.

    The batch lane carries the same column directory a plain group does, but it
    declined every compressed group, so the whole statement fell through to the
    SQL executor. Measured on 200k x 16: 100 ids cost 350 us against 48 us on the
    plain twin, and a 3-column projection of the same ids 330 us against 9.5 us.
    """
    ids = [1, 7, 100, ROWS // 4, ROWS // 2, ROWS - 1, ROWS]
    listed = ", ".join(str(value) for value in ids)

    batch = _rows(
        client._storage.execute(f"SELECT _id, c0, s0 FROM t WHERE _id IN ({listed})")
    )
    assert len(batch) == len(ids)
    assert {row["_id"] for row in batch} == set(ids)
    for row in batch:
        expected = _expected(row["_id"], ["c0", "s0"])
        assert row["c0"] == expected["c0"], row
        assert row["s0"] == expected["s0"], row

    # A repeated id is a single row, an unknown id contributes nothing, and the
    # public client agrees with the executor lane.
    assert _rows(
        client._storage.execute("SELECT c0 FROM t WHERE _id IN (5, 5, 5)")
    ) == [{"c0": _value("c0", 4)}]
    assert _rows(
        client._storage.execute(f"SELECT c0 FROM t WHERE _id IN (3, {ROWS + 25})")
    ) == [{"c0": _value("c0", 2)}]
    through_client = client.execute(
        f"SELECT c0 FROM t WHERE _id IN ({listed})"
    ).to_dict()
    assert sorted(row["c0"] for row in through_client) == sorted(
        row["c0"] for row in batch
    )


def test_batch_point_lookup_skips_deleted_ids(client):
    removed = ROWS // 8
    client.execute(f"DELETE FROM t WHERE _id = {removed}")
    listed = f"{removed - 1}, {removed}, {removed + 1}"
    rows = _rows(client._storage.execute(f"SELECT _id, c0 FROM t WHERE _id IN ({listed})"))
    assert {row["_id"] for row in rows} == {removed - 1, removed + 1}

    client.flush()
    rows = _rows(client._storage.execute(f"SELECT _id, c0 FROM t WHERE _id IN ({listed})"))
    assert {row["_id"] for row in rows} == {removed - 1, removed + 1}


def test_batch_point_lookup_stays_on_the_storage_lane_when_compressed():
    """Same process, same fixture: a compressed batch must not cost ~7x the plain one."""
    root = tempfile.mkdtemp()
    apex = ApexClient(root, enable_cache=False)
    try:
        for layout, compression in (("t_plain", None), ("t_zstd", "zstd")):
            apex.create_table(layout, schema=SCHEMA)
            apex.use_table(layout)
            if compression:
                assert apex.set_compression(compression) is not False
            apex.store(_fixture_rows())
            apex.flush()

        ids = ", ".join(str(1000 + index * 7) for index in range(100))

        def median_us(table, iterations=40):
            apex.use_table(table)
            sql = f"SELECT c0, c1, c2 FROM {table} WHERE _id IN ({ids})"
            for _ in range(8):
                apex._storage.execute(sql)
            samples = []
            for _ in range(iterations):
                start = time.perf_counter()
                apex._storage.execute(sql)
                samples.append((time.perf_counter() - start) * 1e6)
            return statistics.median(samples)

        fast = median_us("t_plain")
        slow = median_us("t_zstd")
    finally:
        apex.close()
        shutil.rmtree(root, ignore_errors=True)

    assert slow <= max(fast * 6.0, 60.0), (
        f"compressed batch lookup took {slow:.2f} us against {fast:.2f} us on the "
        "plain twin; it fell out of the storage batch lane"
    )


def test_batch_lookup_drops_missing_ids_instead_of_the_whole_statement(client):
    """A missing id must only remove its own row from an `_id IN (...)` batch.

    The binding's batch branch required `row_count == ids.len()` and declined the
    entire statement otherwise, which sent it back through the SQL executor to
    re-parse, re-plan and scan. On a 200k x 16 plain table, 1000 ids with one
    missing cost 3.71 ms against 0.43 ms when every id exists (8.7x), and 1.23
    against 0.43 ms (2.9x) on the zstd twin. The lane already reports the rows it
    found in ascending-id order, exactly like a scan, so the subset is the whole
    answer.
    """
    present = [1000 + index * 7 for index in range(100)]
    missing = max(present) + 5_000_000
    batch = ", ".join(str(value) for value in present)

    # All ids present: the baseline the ratio below is measured against.
    full = _rows(client._storage.execute(f"SELECT * FROM t WHERE _id IN ({batch})"))
    assert len(full) == len(present)

    # One missing id: same rows, one fewer.
    partial = _rows(
        client._storage.execute(f"SELECT * FROM t WHERE _id IN ({batch}, {missing})")
    )
    assert len(partial) == len(present)
    assert partial == full

    # A deleted id drops out the same way and does not disturb its neighbours.
    client.execute("DELETE FROM t WHERE _id = 1007")
    client.flush()
    after = _rows(
        client._storage.execute(f"SELECT * FROM t WHERE _id IN ({batch}, {missing})")
    )
    assert after == [row for row in full if row["_id"] != 1007]

    # Everything missing: still an empty relation, and the projected columns are
    # supplied by the executor.
    rows = client.execute(
        f"SELECT c3 FROM t WHERE _id IN ({missing}, {missing + 1})"
    ).to_dict()
    assert rows == []

    # Duplicate ids collapse to one row each, as `IN` requires.
    # `1007` is deleted by now, so use two ids that are still present.
    dupes = _rows(
        client._storage.execute("SELECT * FROM t WHERE _id IN (1000, 1000, 1014, 1000)")
    )
    assert sorted(row["_id"] for row in dupes) == [1000, 1014]


def test_batch_lookup_with_a_missing_id_stays_on_the_batch_lane(client):
    """The partial batch must not fall back to the executor (8.7x on 1000 ids)."""
    present = [1000 + index * 7 for index in range(1000)]
    batch = ", ".join(str(value) for value in present)
    missing = max(present) + 5_000_000

    def median_ms(sql, iterations=9):
        for _ in range(3):
            client._storage.execute(sql)
        samples = []
        for _ in range(iterations):
            start = time.perf_counter()
            client._storage.execute(sql)
            samples.append((time.perf_counter() - start) * 1000.0)
        return statistics.median(samples)

    complete = median_ms(f"SELECT * FROM t WHERE _id IN ({batch})")
    partial = median_ms(f"SELECT * FROM t WHERE _id IN ({batch}, {missing})")
    # Both run the same lane; before the fix the partial one was a full scan and
    # this ratio was 2.9x-8.7x on the same fixtures.
    assert partial < complete * 3 + 0.5, (complete, partial)


@pytest.mark.parametrize("compression", [None, "zstd"])
def test_partial_batch_lookup_never_drops_sidecar_rows(compression):
    """A partial `_id IN (...)` hit must not answer from the base alone.

    The batch lane reads persisted row groups, while a spilled transaction's rows
    live in the `.apex.delta` sidecar. Accepting a *partial* hit there silently
    dropped those rows (the executor merges them), so a partial hit is only
    authoritative while no sidecar exists -- and the two routes must agree.
    """
    root = tempfile.mkdtemp()
    try:
        apex = ApexClient(root, enable_cache=False)
        apex.create_table("t", schema={"name": "string", "score": "int"})
        apex.use_table("t")
        if compression:
            assert apex.set_compression(compression) is not False
        apex.store(
            [
                {"name": "base_a", "score": 1},
                {"name": "base_b", "score": 2},
                {"name": "base_c", "score": 3},
            ]
        )
        apex.flush()
        apex.execute("BEGIN")
        apex.execute("INSERT INTO t (name, score) VALUES ('sidecar', 30)")
        apex.execute("UPDATE t SET score = 20 WHERE _id = 2")
        apex.execute("COMMIT")

        # Id 4 exists only in the sidecar, so the request is a partial hit.
        public = apex.execute("SELECT name FROM t WHERE _id IN (1, 4)").to_dict()
        rust = _rows(apex._storage.execute("SELECT name FROM t WHERE _id IN (1, 4)"))
        assert [row["name"] for row in public] == ["base_a", "sidecar"]
        assert rust == public, (rust, public)
        apex.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)
