"""Every read lane must agree with the scan of the same table.

The round-33 defect (a three-byte BITPACK window in the page-cached point lane)
was invisible because scans and point reads disagreed and nothing compared them:
`SELECT _id, a` returned `4_000_001` while `retrieve` returned `67_841` for the
same row. This module pins the oracle: for one table state, the full scan is the
truth and *every* id-addressed lane has to reproduce it -- `retrieve`, the
projected point read behind `WHERE _id = x`, multi-id lookups, and id-filtered
aggregations -- across column types, encodings, NULLs and overlay states.

Encodings are reached by value shape, not by a knob:

* `bitpack` needs >= 16 values with a range that saves space (a narrow integer
  column), and its width grows with the range (`0..49_999` plus a wide append
  is the 22-bit case that used to truncate).
* `rle` needs long runs, `plain` anything the other two refuse (floats, wide
  ranges, few rows), `compact dictionary` low-cardinality strings,
  `float dictionary` a float column with few distinct values, and
  `plain_f32` an f32-typed column on a V5 file.
"""

import random

import pytest

from apexbase.client import ApexClient


def _scan(client, columns=("a", "b", "s")):
    """(id -> row dict) from the authoritative scan, plus the ORDER BY view."""
    projected = ", ".join(columns)
    rows = client.execute(f"SELECT _id, {projected} FROM t").to_dict()
    by_id = {int(row["_id"]): row for row in rows}
    return by_id, rows


def _assert_lanes(client, columns=("a", "b", "s"), sample=None, sum_column="a"):
    by_id, rows = _scan(client, columns)
    assert by_id, "fixture must not be empty"
    ids = sorted(by_id)
    if sample is not None:
        ids = ids[:sample]
    rng = random.Random(20260928)
    probes = ids if sample is None else rng.sample(ids, min(sample, len(ids)))

    for row_id in probes:
        want = by_id[row_id]
        got = client.retrieve(row_id)
        assert got is not None, ("retrieve returned nothing", row_id)
        for column in columns:
            assert got[column] == want[column], ("retrieve", row_id, column, got, want)

        # The projected point lane behind `WHERE _id = x`.
        projected = client.execute(
            f"SELECT {', '.join(columns)} FROM t WHERE _id = {row_id}"
        ).to_dict()
        assert len(projected) == 1, ("id filter", row_id, projected)
        for column in columns:
            assert projected[0][column] == want[column], (
                "id filter",
                row_id,
                column,
                projected[0],
                want,
            )

    # A missing id must stay missing in every lane. The projection names a real
    # column of the fixture: a `SELECT` of a column this table does not have is
    # an error on every lane (including the hit lane), and the miss lane used to
    # hide that by returning an unprojected empty batch.
    missing = max(ids) + 1_000_000
    assert client.retrieve(missing) is None, missing
    for column in columns[:2]:
        assert client.execute(
            f"SELECT {column} FROM t WHERE _id = {missing}"
        ).to_dict() == []
    assert client.execute(f"SELECT * FROM t WHERE _id = {missing}").to_dict() == []

    # Id-filtered aggregation agrees with the scanned rows.
    total = client.execute("SELECT COUNT(*) AS n FROM t").to_dict()[0]
    assert int(total["n"]) == len(rows), (total, len(rows))
    if sum_column in columns:
        summed = client.execute(
            f"SELECT SUM({sum_column}) AS s FROM t"
        ).to_dict()[0]
        expected_sum = sum(int(row[sum_column]) for row in rows)
        assert int(summed["s"] or 0) == expected_sum, (summed, expected_sum)

    # A range on `_id` must return exactly the scanned rows in that range.
    if len(ids) > 4:
        low, high = ids[1], ids[-2]
        ranged = client.execute(
            f"SELECT _id FROM t WHERE _id BETWEEN {low} AND {high}"
        ).to_dict()
        assert {int(row["_id"]) for row in ranged} == {
            row_id for row_id in ids if low <= row_id <= high
        }
    return by_id


def _new_client(tmp_path, name, schema):
    client = ApexClient(str(tmp_path / name), drop_if_exists=True, enable_cache=False)
    client.create_table("t", schema=schema)
    client.use_table("t")
    return client


def test_bitpack_small_range_matches_every_lane(tmp_path):
    client = _new_client(tmp_path, "bitpack_small", {"a": "int64", "b": "int64", "s": "string"})
    try:
        client.store(
            {
                "a": list(range(20_000)),
                "b": [i % 977 for i in range(20_000)],
                "s": [f"v{i % 50}" for i in range(20_000)],
            }
        )
        client.flush()
        _assert_lanes(client, sample=400)
    finally:
        client.close()


def test_bitpack_wide_range_after_appends(tmp_path):
    """The 22-bit case: a narrow base plus wide appended values."""
    client = _new_client(tmp_path, "bitpack_wide", {"a": "int64", "b": "int64", "s": "string"})
    try:
        client.store(
            {
                "a": list(range(50_000)),
                "b": [i % 977 for i in range(50_000)],
                "s": [f"v{i % 50}" for i in range(50_000)],
            }
        )
        client.flush()
        writer = ApexClient(str(tmp_path / "bitpack_wide"), enable_cache=False)
        writer.use_table("t")
        for offset in range(6):
            writer.execute(
                f"INSERT INTO t (a, b, s) VALUES ({4_000_000 + offset}, {offset}, 'x')"
            )
        writer.flush()
        writer.close()
        by_id = _assert_lanes(client, sample=400)
        appended = [value for value in by_id.values() if int(value["a"]) >= 4_000_000]
        assert len(appended) == 6, appended
    finally:
        client.close()


def test_rle_and_plain_columns_match_every_lane(tmp_path):
    client = _new_client(tmp_path, "mixed_encodings", {"a": "int64", "b": "int64", "s": "string"})
    try:
        client.store(
            {
                # Long runs reach RLE, a wide spread reaches PLAIN.
                "a": [7] * 5_000 + [9] * 5_000,
                "b": list(range(10_000)),
                "s": ["same"] * 9_999 + ["other"],
            }
        )
        client.flush()
        _assert_lanes(client, sample=300)
    finally:
        client.close()


def test_float_and_bool_columns_match_every_lane(tmp_path):
    client = _new_client(
        tmp_path,
        "floats_bools",
        {"a": "int64", "f": "float64", "g": "float32", "flag": "bool"},
    )
    try:
        client.store(
            {
                "a": list(range(20_000)),
                "f": [i / 7.0 for i in range(20_000)],
                "g": [i * 0.5 for i in range(20_000)],
                "flag": [i % 3 == 0 for i in range(20_000)],
            }
        )
        client.flush()
        _assert_lanes(client, columns=("a", "f", "g", "flag"), sample=300)
    finally:
        client.close()


def test_nulls_and_deletes_match_every_lane(tmp_path):
    client = _new_client(tmp_path, "nulls_deletes", {"a": "int64", "b": "int64", "s": "string"})
    try:
        rows = 20_000
        client.store(
            {
                "a": list(range(rows)),
                "b": [None if i % 5 == 0 else i % 977 for i in range(rows)],
                "s": [None if i % 7 == 0 else f"v{i % 50}" for i in range(rows)],
            }
        )
        client.flush()
        # A prefix delete leaves a contiguous hole, a scattered IN list leaves
        # gaps in the id space (the deletion bitmap must follow both).
        client.execute("DELETE FROM t WHERE a < 200")
        client.execute(f"DELETE FROM t WHERE a IN ({', '.join(str(v) for v in range(500, 4000, 250))})")
        client.flush()
        _assert_lanes(client, sample=300)
    finally:
        client.close()


def test_overlay_states_match_every_lane(tmp_path):
    """Buffered appends and flushed deltas must read the same through all lanes."""
    client = _new_client(tmp_path, "overlay", {"a": "int64", "b": "int64", "s": "string"})
    try:
        client.store(
            {
                "a": list(range(10_000)),
                "b": [i % 977 for i in range(10_000)],
                "s": [f"v{i % 50}" for i in range(10_000)],
            }
        )
        client.flush()
        _assert_lanes(client, sample=200)

        # Buffered (unflushed) appends.
        client.store(
            {
                "a": list(range(10_000, 10_050)),
                "b": [1] * 50,
                "s": ["buffered"] * 50,
            }
        )
        _assert_lanes(client, sample=200)

        # Persisted delta after a flush.
        client.flush()
        _assert_lanes(client, sample=200)
    finally:
        client.close()


def test_low_cardinality_and_high_cardinality_strings_match_every_lane(tmp_path):
    client = _new_client(tmp_path, "strings", {"a": "int64", "s": "string"})
    try:
        client.store(
            {
                "a": list(range(20_000)),
                "s": [f"unique-{i}" for i in range(20_000)],
            }
        )
        client.flush()
        _assert_lanes(client, columns=("a", "s"), sample=200)

        second = _new_client(tmp_path, "strings_low", {"a": "int64", "s": "string"})
        try:
            second.store({"a": list(range(20_000)), "s": [f"k{i % 4}" for i in range(20_000)]})
            second.flush()
            _assert_lanes(second, columns=("a", "s"), sample=200)
        finally:
            second.close()
    finally:
        client.close()


def test_updates_clear_every_lane(tmp_path):
    """`UPDATE` (pending DeltaStore cells) must be visible through point reads."""
    client = _new_client(tmp_path, "updates", {"a": "int64", "b": "int64", "s": "string"})
    try:
        client.store(
            {
                "a": list(range(10_000)),
                "b": [i % 977 for i in range(10_000)],
                "s": [f"v{i % 50}" for i in range(10_000)],
            }
        )
        client.flush()
        _assert_lanes(client, sample=100)

        client.execute("UPDATE t SET b = 424242 WHERE a < 500")
        _assert_lanes(client, sample=100)
        client.flush()
        _assert_lanes(client, sample=100)

        # Updating the pointed column itself, plus the string column.
        client.execute("UPDATE t SET a = 10000000 WHERE a BETWEEN 9000 AND 9100")
        client.execute("UPDATE t SET s = 'updated' WHERE a BETWEEN 9200 AND 9300")
        by_id = _assert_lanes(client, sample=100)
        assert any(row["s"] == "updated" for row in by_id.values())
    finally:
        client.close()


def test_added_columns_read_as_null_in_old_row_groups(tmp_path):
    """A column added after the row group was written has no value in it."""
    client = _new_client(tmp_path, "schema_evolution", {"a": "int64"})
    try:
        client.store({"a": list(range(5_000))})
        client.flush()
        client.execute("ALTER TABLE t ADD COLUMN b INT64")
        client.store({"a": list(range(5_000, 5_100)), "b": list(range(5_000, 5_100))})
        client.flush()

        rows = client.execute("SELECT _id, a, b FROM t").to_dict()
        assert rows, "fixture must not be empty"
        for row in rows[:50]:
            row_id = int(row["_id"])
            point = client.retrieve(row_id)
            assert point is not None, row_id
            assert point["a"] == row["a"], (row_id, point, row)
            if int(row["a"]) < 5_000:
                assert point["b"] is None, (row_id, point)
                assert row["b"] is None, (row_id, row)
            else:
                assert point["b"] == row["b"], (row_id, point, row)

        projected = client.execute("SELECT a, b FROM t WHERE _id = 1").to_dict()
        assert projected and projected[0]["b"] is None, projected
    finally:
        client.close()


def test_extreme_bitpack_widths_and_signs_match_every_lane(tmp_path):
    """Widths from 1 to ~48 bits, negative ranges and mixed signs."""
    cases = {
        "negative_only": [-3, -2, -1, 0, 1],
        "crossing_zero": [-5_000_000, -1, 0, 1, 5_000_000],
        "thirty_two_bits": [0, 1, 2, 3, 2_147_483_647],
        "forty_bits": [-(1 << 39), 0, (1 << 39)],
        "one_bit": [0, 1, 0, 1, 0, 1],
    }
    for name, values in cases.items():
        client = _new_client(tmp_path, f"width_{name}", {"a": "int64", "b": "int64"})
        try:
            rows = 20_000
            column = [values[i % len(values)] for i in range(rows)]
            client.store({"a": column, "b": list(range(rows))})
            client.flush()
            # Sample the few distinct values through every lane.
            probes = sorted({int(value) for value in values})
            for probe in probes[:4]:
                ids = [
                    int(row["_id"])
                    for row in client.execute(
                        f"SELECT _id FROM t WHERE a = {probe} LIMIT 3"
                    ).to_dict()
                ]
                for row_id in ids:
                    point = client.retrieve(row_id)
                    assert point is not None and point["a"] == probe, (
                        name,
                        probe,
                        row_id,
                        point,
                    )
                    sql = client.execute(
                        f"SELECT a FROM t WHERE _id = {row_id}"
                    ).to_dict()
                    assert sql and sql[0]["a"] == probe, (name, probe, row_id, sql)
        finally:
            client.close()


def test_integer_widths_and_timestamps_match_every_lane(tmp_path):
    """Every declared integer type shares the point lanes."""
    client = _new_client(
        tmp_path,
        "int_widths",
        {
            "i8": "int8",
            "i16": "int16",
            "i32": "int32",
            "i64": "int64",
            "u32": "uint32",
            "ts": "timestamp",
        },
    )
    try:
        rows = 20_000
        client.store(
            {
                "i8": [i % 127 for i in range(rows)],
                "i16": [i % 30_000 for i in range(rows)],
                "i32": [i * 1000 for i in range(rows)],
                "i64": [i * 1_000_000_000 for i in range(rows)],
                "u32": [i * 100 for i in range(rows)],
                "ts": [1_700_000_000_000 + i for i in range(rows)],
            }
        )
        client.flush()
        # The integer columns agree exactly.
        _assert_lanes(
            client,
            columns=("i8", "i16", "i32", "i64", "u32"),
            sample=200,
            sum_column="i64",
        )

        # `ts` keeps a dual representation like vector columns do: Arrow-shaped
        # lanes decode to `datetime`, while point/limit lanes carry the raw
        # stored microseconds. Only the value is guaranteed, so compare it after
        # normalising whichever form the lane returned.
        def _micros(value):
            return int(value.timestamp() * 1_000_000) if hasattr(value, "timestamp") else int(value)

        for row in client.execute("SELECT _id, ts FROM t LIMIT 50").to_dict():
            row_id = int(row["_id"])
            point = client.retrieve(row_id)
            assert point is not None, row_id
            assert _micros(point["ts"]) == _micros(row["ts"]), (row_id, point, row)
            projected = client.execute(f"SELECT ts FROM t WHERE _id = {row_id}").to_dict()
            assert projected and _micros(projected[0]["ts"]) == _micros(row["ts"]), (
                row_id,
                projected,
                row,
            )
    finally:
        client.close()


@pytest.mark.parametrize("compression", ["zstd", "lz4"])
def test_compressed_tables_keep_every_lane_consistent(tmp_path, compression):
    """Compressed row groups carry no RCIX, so point reads decode the group.

    The decompressed body is cached now; the values must stay identical to the
    scan through every lane, both on the first (cold) and later (warm) reads.
    """
    client = ApexClient(
        str(tmp_path / f"compressed_{compression}"), drop_if_exists=True, enable_cache=False
    )
    try:
        client.create_table("t", schema={"a": "int64", "b": "int64", "s": "string"})
        client.use_table("t")
        assert client.set_compression(compression) is not False, compression
        rows = 40_000
        client.store(
            {
                "a": list(range(rows)),
                "b": [i * 3 for i in range(rows)],
                "s": [f"v{i % 50}" for i in range(rows)],
            }
        )
        client.flush()
        _assert_lanes(client, sample=200)
        # Repeat ids so the second pass exercises the cached decompressed body.
        _assert_lanes(client, sample=200)
    finally:
        client.close()
