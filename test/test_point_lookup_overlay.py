"""Point lookups must stay O(1) while a DeltaStore overlay is pending.

An `UPDATE`/`DELETE` records rows in the DeltaStore; `SELECT ... WHERE _id = X`
used to fall back to a full scan for *any* pending overlay, which turned a 3 µs
lookup into a ~60 ms scan on a 1M-row table. The overlay only changes the rows it
touches, so the base row is read by id and overlaid on its own.

The last test pins the cost directly: it is a regression guard for the scan
fallback, with a bound loose enough to survive machine noise (a full scan of the
fixture takes tens of milliseconds) but far tighter than one.
"""

import statistics
import tempfile
import time

import pytest

from apexbase import ApexClient

ROWS = 500_000


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    c = ApexClient(root, enable_cache=False)
    c.create_table("t", schema={"a": "int64", "b": "string", "score": "float64"})
    c.use_table("t")
    c.store({
        "a": list(range(ROWS)),
        "b": [f"v{i % 17}" for i in range(ROWS)],
        "score": [float(i) for i in range(ROWS)],
    })
    c.flush()
    c.close()

    # Reopen so the table is a persisted, mmap-backed read view.
    reopened = ApexClient(root, enable_cache=False)
    reopened.use_table("t")
    try:
        yield reopened
    finally:
        reopened.close()
        import shutil

        shutil.rmtree(root, ignore_errors=True)


def test_updated_row_reads_back_the_new_value(client):
    client.execute("UPDATE t SET score = 999.5 WHERE _id = 777")
    assert client.execute(
        "SELECT * FROM t WHERE _id = 777", show_internal_id=True
    ).to_dict() == [{"_id": 777, "a": 776, "b": "v11", "score": 999.5}]
    # A projection that does not mention the updated column still sees the rest.
    assert client.execute("SELECT a, b FROM t WHERE _id = 777").to_dict() == [
        {"a": 776, "b": "v11"}
    ]
    # The direct point API agrees with the SQL path.
    assert client.retrieve(777) == {"_id": 777, "a": 776, "b": "v11", "score": 999.5}


def test_untouched_rows_keep_their_base_values(client):
    client.execute("UPDATE t SET b = 'patched' WHERE _id = 5")
    assert client.execute(
        "SELECT * FROM t WHERE _id = 500", show_internal_id=True
    ).to_dict() == [{"_id": 500, "a": 499, "b": "v6", "score": 499.0}]
    assert client.execute(
        "SELECT * FROM t WHERE _id = 5", show_internal_id=True
    ).to_dict() == [{"_id": 5, "a": 4, "b": "patched", "score": 4.0}]


def test_deleted_row_is_gone_from_the_point_lookup(client):
    client.execute("DELETE FROM t WHERE _id = 12")
    assert client.execute(
        "SELECT * FROM t WHERE _id = 12", show_internal_id=True
    ).to_dict() == []
    assert client.retrieve(12) is None
    # The rest of the table is intact.
    assert client.execute(
        "SELECT * FROM t WHERE _id = 13", show_internal_id=True
    ).to_dict() == [{"_id": 13, "a": 12, "b": "v12", "score": 12.0}]


def test_appended_rows_are_found_after_an_update(client):
    # Appended delta rows are not in the base file, so they must still come from
    # the merged path even though a DeltaStore update exists.
    client.execute("UPDATE t SET score = 1.5 WHERE _id = 3")
    client.store({"a": [999_999], "b": ["appended"], "score": [7.5]})
    client.flush()
    assert client.execute(
        f"SELECT * FROM t WHERE _id = {ROWS + 1}", show_internal_id=True
    ).to_dict() == [{"_id": ROWS + 1, "a": 999_999, "b": "appended", "score": 7.5}]
    assert client.execute(
        "SELECT * FROM t WHERE _id = 3", show_internal_id=True
    ).to_dict() == [{"_id": 3, "a": 2, "b": "v2", "score": 1.5}]


def test_point_lookup_with_a_pending_overlay_is_not_a_scan(client):
    def median_lookup(sql, iterations=15):
        for _ in range(3):
            client.execute(sql).to_dict()
        samples = []
        for _ in range(iterations):
            start = time.perf_counter()
            client.execute(sql).to_dict()
            samples.append((time.perf_counter() - start) * 1000.0)
        return statistics.median(samples)

    clean = median_lookup("SELECT * FROM t WHERE _id = 4000")
    client.execute("UPDATE t SET score = 42.0 WHERE _id = 777")
    after_update = median_lookup("SELECT * FROM t WHERE _id = 4000")
    updated_row = median_lookup("SELECT * FROM t WHERE _id = 777")

    # A full scan of this fixture takes tens of milliseconds; an O(1) lookup
    # stays in the microsecond range. 10 ms separates the two by a wide margin
    # without being flaky.
    assert after_update < 10.0, (
        f"point lookup fell back to a scan with a pending overlay: "
        f"clean {clean:.3f} ms, after update {after_update:.3f} ms"
    )
    assert updated_row < 10.0, f"updated row lookup took {updated_row:.3f} ms"
