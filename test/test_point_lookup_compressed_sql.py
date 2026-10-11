"""SQL point lookups must stay O(1) on compressed tables and read only the projection.

`retrieve_rcix` only serves uncompressed row groups and reports "lane
unavailable" and "row absent" as the same `None`. The Arrow point readers used
to treat both as absence, so `SELECT ... WHERE _id = X` on a compressed table
fell into the general scan path — measured at 23.6 ms for one row on a
200k-row / 16-column zstd table, against 0.007 ms once the targeted decoding
lane answers. These tests pin both the values and the cost, through the Rust
SQL executor (``client._storage.execute``) rather than the Python point-lookup
fast path, which was already fast.
"""

import statistics
import tempfile
import time

import pytest

from apexbase import ApexClient

ROWS = 70_000  # more than one 65,536-row row group, so the id directory is walked
COLUMNS = {"a": "int64", "b": "string", "c": "float64", "d": "string"}


def _expected(row_id):
    index = row_id - 1
    return {
        "_id": row_id,
        "a": index,
        "b": f"v{index % 17}",
        "c": float(index) * 0.5,
        "d": f"w{index % 7}",
    }


def _rows(result):
    columns = result.get("columns_dict") if result is not None else None
    if columns is None:
        return []
    row_count = len(next(iter(columns.values()), []))
    return [
        {name: values[index] for name, values in columns.items()}
        for index in range(row_count)
    ]


@pytest.fixture(params=[None, "zstd", "lz4"])
def client(request):
    root = tempfile.mkdtemp()
    apex = ApexClient(root, enable_cache=False)
    apex.create_table("t", schema=COLUMNS)
    apex.use_table("t")
    if request.param:
        assert apex.set_compression(request.param) is not False
    apex.store(
        {
            "a": list(range(ROWS)),
            "b": [f"v{i % 17}" for i in range(ROWS)],
            "c": [float(i) * 0.5 for i in range(ROWS)],
            "d": [f"w{i % 7}" for i in range(ROWS)],
        }
    )
    apex.flush()
    try:
        yield apex
    finally:
        apex.close()
        import shutil

        shutil.rmtree(root, ignore_errors=True)


def test_sql_point_lookup_reads_the_row_on_every_layout(client):
    for row_id in (1, 2, 60_000, ROWS - 1, ROWS):
        rows = _rows(client._storage.execute(f"SELECT * FROM t WHERE _id = {row_id}"))
        assert rows == [_expected(row_id)], row_id
        # The public client agrees with the executor.
        assert client.execute(
            f"SELECT * FROM t WHERE _id = {row_id}", show_internal_id=True
        ).to_dict() == [_expected(row_id)]


def test_sql_point_lookup_of_a_missing_id_is_empty(client):
    rows = _rows(client._storage.execute(f"SELECT * FROM t WHERE _id = {ROWS + 25}"))
    assert rows == []
    assert client.execute(
        f"SELECT * FROM t WHERE _id = {ROWS + 25}", show_internal_id=True
    ).to_dict() == []


def test_sql_projected_point_lookup_reads_only_the_requested_columns(client):
    row_id = 1_234
    expected = _expected(row_id)

    result = client._storage.execute(f"SELECT a, d FROM t WHERE _id = {row_id}")
    assert _rows(result) == [{"a": expected["a"], "d": expected["d"]}]

    # `_id` remains reachable through the same lane.
    assert _rows(client._storage.execute(f"SELECT _id FROM t WHERE _id = {row_id}")) == [
        {"_id": row_id}
    ]

    # A repeated projection must not produce duplicate columns.
    assert _rows(client._storage.execute(f"SELECT a, a FROM t WHERE _id = {row_id}")) == [
        {"a": expected["a"]}
    ]

    # An aliased column keeps the alias in the public result.
    assert client.execute(f"SELECT a AS renamed FROM t WHERE _id = {row_id}").to_dict() == [
        {"renamed": expected["a"]}
    ]


def test_sql_point_lookup_answers_through_an_overlay(client):
    # A pending DeltaStore update and a pending delete both change what the same
    # `_id = X` shape must return, and both keep the base file on disk.
    client.execute("UPDATE t SET c = 4242.5 WHERE _id = 777")
    client.execute("DELETE FROM t WHERE _id = 12")

    updated = client._storage.execute("SELECT * FROM t WHERE _id = 777")
    assert _rows(updated)[0]["c"] == 4242.5
    assert _rows(client._storage.execute("SELECT * FROM t WHERE _id = 12")) == []

    # Untouched rows are unaffected by the overlay.
    assert _rows(client._storage.execute("SELECT * FROM t WHERE _id = 778")) == [
        _expected(778)
    ]

    # The filtered scan and the point lookup agree after a flush as well.
    client.flush()
    assert _rows(client._storage.execute("SELECT * FROM t WHERE _id = 777"))[0][
        "c"
    ] == 4242.5
    assert _rows(client._storage.execute("SELECT * FROM t WHERE _id = 12")) == []


def test_sql_point_lookup_is_not_a_scan_on_every_layout(client):
    # The regression this guards (the lean lane reporting "row absent" for a
    # compressed row group, turning one lookup into a full scan) only ever hit
    # compressed layouts, but the bound holds on every layout, so all three run
    # it rather than skipping the uncompressed one.
    def median_ms(sql, iterations=15):
        for _ in range(3):
            client._storage.execute(sql)
        samples = []
        for _ in range(iterations):
            start = time.perf_counter()
            client._storage.execute(sql)
            samples.append((time.perf_counter() - start) * 1000.0)
        return statistics.median(samples)

    lookup = median_ms(f"SELECT * FROM t WHERE _id = {ROWS // 2}")
    scan = median_ms("SELECT * FROM t")

    # A full scan of this fixture takes milliseconds; an O(1) lookup stays in the
    # tens of microseconds. Before the decoding-lane fallback the lookup paid for
    # the whole scan, so both bounds separate the two shapes by a wide margin.
    assert lookup < 5.0, (
        f"{client.get_compression()} point lookup took {lookup:.3f} ms"
    )
    assert lookup * 5 < scan, (
        f"{client.get_compression()} point lookup ({lookup:.3f} ms) is not clearly "
        f"cheaper than a full scan ({scan:.3f} ms); it fell back to scanning"
    )
