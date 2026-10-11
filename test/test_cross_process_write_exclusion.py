"""Cross-process write exclusion between every public write path.

`RESOURCE_OWNERSHIP.md` promises that writes to one table are serialized across
processes by an `fs2` flock. The executor's DML path and the Python bindings'
`store`/`store_batch` entry points take that lock on `<table>.apex.lock`, but the
single-row (`store_one`) and columnar (`store_columnar`) fast paths used to
publish through `Database::write_typed*`, which only takes the *in-process*
`table_save_lock`. Two processes writing one table through different paths (or
even through the same unlocked path) could therefore read the same base, append
their own rows and let the last publish win.

These tests pin the exclusion: several processes write disjoint id ranges through
a mix of the single-row, columnar and SQL paths, and every row has to survive.
"""

import multiprocessing
import os
import time

from apexbase import ApexClient

ROWS_PER_WRITER = 120
WRITERS = 4


def _open(root, table="t"):
    client = ApexClient(dirpath=root)
    client.use_table(table)
    return client


def _writer_store_one(root, start, count, gate):
    client = _open(root)
    try:
        gate.wait()
        for offset in range(count):
            client.store({"a": start + offset})
    finally:
        client.close()


def _writer_columnar(root, start, count, gate):
    client = _open(root)
    try:
        gate.wait()
        client.store({"a": list(range(start, start + count))})
        client.flush()
    finally:
        client.close()


def _writer_sql(root, start, count, gate):
    client = _open(root)
    try:
        gate.wait()
        for offset in range(count):
            client.execute(f"INSERT INTO t (a) VALUES ({start + offset})")
    finally:
        client.close()


def _v4_writer_store_one(root, start, count, gate):
    """Append into an existing V4 file from a second process.

    The V4 compat append path assigns ids itself instead of going through the
    regular allocator, so it is the one write route that needed its own
    reservation.
    """
    os.environ["APEX_WIDTH_FIDELITY"] = "0"
    client = _open(root)
    try:
        gate.wait()
        for offset in range(count):
            client.store({"a": start + offset})
        client.flush()
    finally:
        client.close()


def _run_mixed_writers(root, rounds=1):
    """Start one process per write path and return the observed row count."""
    ctx = multiprocessing.get_context("spawn")
    expected = 0
    for round_index in range(rounds):
        base = round_index * WRITERS * ROWS_PER_WRITER
        gate = ctx.Barrier(WRITERS)
        targets = (
            _writer_store_one,
            _writer_columnar,
            _writer_sql,
            _writer_store_one,
        )
        workers = [
            ctx.Process(
                target=target,
                args=(root, base + index * ROWS_PER_WRITER, ROWS_PER_WRITER, gate),
            )
            for index, target in enumerate(targets)
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(60)
            assert worker.exitcode == 0, f"writer exited with {worker.exitcode}"
        expected += WRITERS * ROWS_PER_WRITER
    return expected


def test_every_write_path_survives_cross_process_writers(tmp_path):
    root = tmp_path / "cross_process_writes"
    client = ApexClient(dirpath=str(root), drop_if_exists=True)
    try:
        client.execute("CREATE TABLE t (a INT)")
        client.use_table("t")
        client.flush()
    finally:
        client.close()

    expected = _run_mixed_writers(str(root), rounds=2)

    reader = _open(str(root))
    try:
        assert reader.count_rows() == expected
        # Every writer's rows are present, not just the count.
        values = sorted(row["a"] for row in reader.execute("SELECT a FROM t").to_dict())
        assert values == list(range(expected)), values[:5]
    finally:
        reader.close()


def test_single_row_stores_survive_two_processes(tmp_path):
    """The single-row fast path against itself: the narrowest writer pair."""
    root = tmp_path / "cross_process_store_one"
    client = ApexClient(dirpath=str(root), drop_if_exists=True)
    try:
        client.execute("CREATE TABLE t (a INT)")
        client.use_table("t")
        client.flush()
    finally:
        client.close()

    ctx = multiprocessing.get_context("spawn")
    gate = ctx.Barrier(2)
    workers = [
        ctx.Process(
            target=_writer_store_one,
            args=(str(root), start, 200, gate),
        )
        for start in (0, 10_000)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(60)
        assert worker.exitcode == 0

    reader = _open(str(root))
    try:
        assert reader.count_rows() == 400
        values = sorted(row["a"] for row in reader.execute("SELECT a FROM t").to_dict())
        assert values == list(range(0, 200)) + list(range(10_000, 10_200))
    finally:
        reader.close()


def test_sequential_ids_stay_contiguous_across_write_paths(tmp_path):
    """One process writing one table in sequence sees `1, 2, 3, ...`.

    The shared high-water mark is the authority for cross-process uniqueness,
    but the common single-process case must not pay for it with skipped ids:
    every write path reads its next id from the base file, so feeding that value
    back as the reservation minimum keeps assignments dense. Regression guard for
    a build where the table *generation* leaked into the row-id minimum and every
    second row lost its predecessor's id.
    """
    client = ApexClient(dirpath=str(tmp_path / "sequential_ids"), drop_if_exists=True)
    try:
        client.execute("CREATE TABLE t (k TEXT, f DOUBLE)")
        client.use_table("t")
        client.store({"k": "a", "f": 1})
        client.store([{"k": "b", "f": 2}])
        client.store([{"k": "c", "f": 3}, {"k": "d", "f": 4}])
        client.store({"k": ["e"], "f": [5]})
        client.store_durable_one({"k": "f", "f": 6})
        client.execute("INSERT INTO t (k, f) VALUES ('g', 7)")
        client.flush()
        ids = sorted(int(row["_id"]) for row in client.execute("SELECT _id FROM t").to_dict())
        assert ids == [1, 2, 3, 4, 5, 6, 7], ids
    finally:
        client.close()


def test_concurrent_writers_hand_out_unique_ids(tmp_path):
    """Every row keeps a distinct `_id`.

    Each writer derives its next id from the base file it read, so two processes
    used to assign the same ids: the rows survived, but identity was shared and
    `retrieve`/`DELETE WHERE _id = x` hit only one of them. The shared high-water
    mark in the table's sidecar keeps the ranges disjoint.
    """
    root = tmp_path / "cross_process_ids"
    client = ApexClient(dirpath=str(root), drop_if_exists=True)
    try:
        client.execute("CREATE TABLE t (a INT)")
        client.use_table("t")
        client.flush()
    finally:
        client.close()

    ctx = multiprocessing.get_context("spawn")
    gate = ctx.Barrier(3)
    writers = (
        (_writer_store_one, 0),
        (_writer_columnar, 10_000),
        (_writer_sql, 20_000),
    )
    workers = [
        ctx.Process(target=target, args=(str(root), start, 100, gate))
        for target, start in writers
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(60)
        assert worker.exitcode == 0, f"writer exited with {worker.exitcode}"

    reader = _open(str(root))
    try:
        rows = reader.execute("SELECT _id, a FROM t").to_dict()
        assert len(rows) == 300
        ids = [row["_id"] for row in rows]
        assert len(set(ids)) == 300, "row ids must be unique across writers"
        # Id-addressed reads resolve to the row that actually owns the id.
        by_id = {row["_id"]: row["a"] for row in rows}
        for row_id in (min(ids), max(ids), sorted(ids)[150]):
            assert reader.retrieve(row_id)["a"] == by_id[row_id], row_id
    finally:
        reader.close()


def test_v4_files_append_unique_ids_from_two_processes(tmp_path, monkeypatch):
    """A V4 file stays writable *and* keeps ids unique across processes.

    V5 is the default layout, but V4 data must remain readable — and appendable.
    The legacy compat append branch inside the engine (files with a footer, see
    the Rust test `v4_compat_append_reserves_ids_against_the_shared_mark`) is not
    reachable from files this build writes, so this test covers the public routes
    on a V4-layout file.
    """
    root = tmp_path / "v4_ids"
    monkeypatch.setenv("APEX_WIDTH_FIDELITY", "0")
    client = ApexClient(dirpath=str(root), drop_if_exists=True)
    try:
        client.execute("CREATE TABLE t (a INT, f FLOAT)")
        client.use_table("t")
        client.store({"a": list(range(20)), "f": [float(i) for i in range(20)]})
        client.flush()
    finally:
        client.close()

    # The fixture only tests the compat path if the file really is a V4 layout.
    with open(root / "t.apex", "rb") as handle:
        assert handle.read(16)[8] == 4

    ctx = multiprocessing.get_context("spawn")
    gate = ctx.Barrier(2)
    starts = (1_000, 5_000)
    workers = [
        ctx.Process(target=_v4_writer_store_one, args=(str(root), start, 50, gate))
        for start in starts
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(120)
        assert worker.exitcode == 0, f"writer exited with {worker.exitcode}"

    reader = _open(str(root))
    try:
        rows = reader.execute("SELECT _id, a FROM t").to_dict()
        ids = [row["_id"] for row in rows]
        assert len(rows) == 120
        assert len(set(ids)) == 120, "row ids must be unique across V4 appends"
        assert sorted(row["a"] for row in rows) == (
            list(range(20))
            + list(range(1_000, 1_050))
            + list(range(5_000, 5_050))
        )
        by_id = {row["_id"]: row["a"] for row in rows}
        assert reader.retrieve(min(ids))["a"] == by_id[min(ids)]
    finally:
        reader.close()
