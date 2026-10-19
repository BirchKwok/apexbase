"""A persisted cell overlay must not turn a bounded range read into a full scan.

`SELECT * FROM t WHERE k >= X LIMIT n` is served by a storage-level numeric range
scan with an early limit. It used to stand down as soon as *any* DeltaStore cell
existed (even an update to an unrelated projected column), so the generic merged
executor materialized the whole table: 1M rows went from 0.12 ms to ~1.6 s after
a single `UPDATE ... WHERE _id = ...`.

The lane now stays available while the overlay touches no filter column, hides no
rows and appends none; the selected window is read through the index reader,
which merges the updated cells. Updates to the filter column and deletes keep the
merged executor route, which must still produce exact rows.
"""
from __future__ import annotations

import statistics
import tempfile
import time

from apexbase.client import ApexClient

ROWS = 200_000


def _median_ms(client, sql, iterations=5):
    for _ in range(2):
        client.execute(sql)
    samples = []
    for _ in range(iterations):
        start = time.perf_counter()
        client.execute(sql)
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def _client(tmp):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"k": "int64", "v": "int64"})
    client.use_table("t")
    client.store({"k": list(range(ROWS)), "v": list(range(ROWS))})
    client.flush()
    return client


def _rows(client, sql):
    return client.execute(sql, show_internal_id=True).to_dict()


def test_a_cell_update_on_a_projected_column_keeps_the_bounded_lane_fast():
    sql = "SELECT * FROM t WHERE k >= 100000 LIMIT 100"
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            clean = _median_ms(client, sql)
            client.execute("UPDATE t SET v = 5 WHERE _id = 100001")
            client.flush()
            rows = _rows(client, sql)
            assert len(rows) == 100, len(rows)
            # The window is read in row order, so the updated row is first and
            # its neighbours are untouched.
            assert rows[0]["_id"] == 100001 and rows[0]["v"] == 5, rows[:2]
            assert rows[1]["_id"] == 100002 and rows[1]["v"] == 100001, rows[:2]
            overlay = _median_ms(client, sql)
            # Before the fix this was the generic merged read: ~60 ms at 200k
            # rows (~1.6 s at 1M) against a sub-millisecond clean read.
            assert overlay < clean * 6 + 0.5, (overlay, clean)

            offset = _rows(client, "SELECT * FROM t WHERE k >= 100000 LIMIT 20 OFFSET 50")
            assert [row["_id"] for row in offset[:2]] == [100051, 100052], offset[:2]
            assert len(offset) == 20, len(offset)
        finally:
            client.close()


def test_a_pending_delete_keeps_the_bounded_lane_fast():
    sql = "SELECT * FROM t WHERE k >= 100000 LIMIT 100"
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            clean = _median_ms(client, sql)
            # A delete inside the window must remove the row and still fill the
            # page from the following rows (one extra candidate is scanned).
            client.execute("DELETE FROM t WHERE _id = 100003")
            client.flush()
            page = _rows(client, sql)
            assert all(row["_id"] != 100003 for row in page)
            assert len(page) == 100, len(page)
            assert (page[0]["_id"], page[1]["_id"], page[2]["_id"]) == (
                100001, 100002, 100004), page[:3]
            overlay = _median_ms(client, sql)
            assert overlay < clean * 6 + 0.5, (overlay, clean)
        finally:
            client.close()


def test_a_filter_column_update_stays_exact():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp)
        try:
            # Moving the filter column needs a re-evaluation of the predicate,
            # so this shape keeps the merged executor route; it must still see
            # the new value in both directions.
            client.execute("UPDATE t SET k = 1 WHERE _id = 100002")
            client.execute("UPDATE t SET k = 999999999 WHERE _id = 100004")
            client.flush()
            rows = _rows(client, "SELECT * FROM t WHERE k >= 100000 LIMIT 1000000")
            ids = {row["_id"]: row["k"] for row in rows}
            assert ids.get(100002) is None, "row moved below the range leaked"
            assert ids.get(100004) == 999999999, "row moved above the range missing"
            assert len(rows) == ROWS - 100_001, len(rows)
            below = _rows(client, "SELECT * FROM t WHERE k < 100000 LIMIT 1000000")
            assert any(row["_id"] == 100002 for row in below)
        finally:
            client.close()
