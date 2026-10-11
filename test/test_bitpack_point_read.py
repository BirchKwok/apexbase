"""The point-read lane must not truncate wide BITPACK columns.

`retrieve_rcix` (and its projected twin) read a fixed three-byte window out of a
bit-packed column, so a value whose bits straddle more than 24 bits silently lost
its high bits: a 50k-row table of `0..49_999` with three appended `4_000_000`
values read back `67_841` / `854_274` through `retrieve` while the scan was
correct. These tests pin every read lane (`retrieve`, the projected point read,
the values lane, and a SQL point lookup) against the value the scan reports.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))

from apexbase.client import ApexClient  # noqa: E402


def _fixture(tmp_path, name, base_rows=50_000):
    client = ApexClient(str(tmp_path / name), drop_if_exists=True, enable_cache=False)
    client.create_table("t", schema={"a": "int64", "b": "int64", "s": "string"})
    client.use_table("t")
    client.store(
        {
            "a": list(range(base_rows)),
            "b": [i % 977 for i in range(base_rows)],
            "s": [f"v{i % 50}" for i in range(base_rows)],
        }
    )
    client.flush()
    return client


def _assert_lanes_agree(client, ids, expected):
    """Every lane that resolves an id must return the written value."""
    scan = {
        int(row["_id"]): int(row["a"])
        for row in client.execute("SELECT _id, a FROM t WHERE a >= 4000000").to_dict()
    }
    assert scan == expected, scan
    for row_id, want in expected.items():
        point = client.retrieve(row_id)
        assert point is not None and int(point["a"]) == want, (row_id, point, want)
        # The projected lane used by `WHERE _id = x` with a column list.
        projected = client.execute(f"SELECT a FROM t WHERE _id = {row_id}").to_dict()
        assert projected and int(projected[0]["a"]) == want, (row_id, projected)


def test_sql_appends_of_wide_values_read_back_exactly(tmp_path):
    """The DML path writes the row group; the point lanes must decode it fully."""
    client = _fixture(tmp_path, "wide_sql_appends")
    try:
        writer = ApexClient(str(tmp_path / "wide_sql_appends"), enable_cache=False)
        writer.use_table("t")
        values = [4_000_000, 4_000_001, 4_000_002]
        for offset, value in enumerate(values):
            writer.execute(
                f"INSERT INTO t (a, b, s) VALUES ({value}, {offset}, 'x')"
            )
        writer.flush()
        writer.close()

        rows = client.execute("SELECT _id, a FROM t WHERE a >= 4000000").to_dict()
        expected = {int(row["_id"]): int(row["a"]) for row in rows}
        assert sorted(expected.values()) == values
        _assert_lanes_agree(client, list(expected), expected)
    finally:
        client.close()


def test_wide_bitpack_values_survive_repeated_and_negative_forms(tmp_path):
    """Several widths and signs must all decode through the point lane."""
    cases = {
        "repeated": [4_000_000, 4_000_000, 4_000_000],
        "single": [4_000_000],
        "negative": [-4_000_000, -4_000_001, -4_000_002],
        "wide": [9_000_000_000, 9_000_000_001, 9_000_000_002],
    }
    for name, values in cases.items():
        client = _fixture(tmp_path, f"wide_{name}")
        try:
            writer = ApexClient(str(tmp_path / f"wide_{name}"), enable_cache=False)
            writer.use_table("t")
            for offset, value in enumerate(values):
                writer.execute(
                    f"INSERT INTO t (a, b, s) VALUES ({value}, {offset}, 'x')"
                )
            writer.flush()
            writer.close()

            rows = client.execute(
                "SELECT _id, a FROM t WHERE s = 'x' ORDER BY _id"
            ).to_dict()
            assert [int(row["a"]) for row in rows] == values, (name, rows)
            expected = {int(row["_id"]): int(row["a"]) for row in rows}
            for row_id, want in expected.items():
                point = client.retrieve(row_id)
                assert point is not None and int(point["a"]) == want, (name, row_id, point)
        finally:
            client.close()


def test_narrow_values_still_read_back(tmp_path):
    """The fix must not disturb columns whose values fit the old window."""
    client = _fixture(tmp_path, "narrow_controls", base_rows=20_000)
    try:
        writer = ApexClient(str(tmp_path / "narrow_controls"), enable_cache=False)
        writer.use_table("t")
        for offset in range(3):
            writer.execute(
                f"INSERT INTO t (a, b, s) VALUES ({100_000 + offset}, {offset}, 'x')"
            )
        writer.flush()
        writer.close()

        rows = client.execute("SELECT _id, a FROM t WHERE s = 'x' ORDER BY _id").to_dict()
        assert [int(row["a"]) for row in rows] == [100_000, 100_001, 100_002]
        for row in rows:
            point = client.retrieve(int(row["_id"]))
            assert point is not None and int(point["a"]) == int(row["a"])
    finally:
        client.close()
