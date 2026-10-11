from pathlib import Path

from apexbase import ApexClient


def _read_row_group_size(table_path: Path) -> int:
    header = table_path.read_bytes()[:32]
    assert len(header) >= 32
    return int.from_bytes(header[28:32], "little")


def test_adaptive_row_group_size_for_narrow_table(tmp_path):
    db_dir = tmp_path / "adaptive_rg"
    client = ApexClient(str(db_dir), drop_if_exists=True)
    client.create_table("narrow")
    client.use_table("narrow")

    row_count = 140_000
    client.store(
        {
            "a": list(range(row_count)),
            "b": list(range(row_count)),
        }
    )
    client.close()

    table_path = db_dir / "narrow.apex"
    assert table_path.exists()
    assert _read_row_group_size(table_path) == 131_072


def test_adaptive_row_group_size_for_wide_table(tmp_path):
    db_dir = tmp_path / "wide_rg"
    client = ApexClient(str(db_dir), drop_if_exists=True)
    client.create_table("wide")
    client.use_table("wide")

    row_count = 80_000
    wide_value = "x" * 256
    client.store(
        {
            "payload": [wide_value for _ in range(row_count)],
            "payload_copy": [wide_value for _ in range(row_count)],
        }
    )
    client.close()

    table_path = db_dir / "wide.apex"
    assert table_path.exists()
    assert _read_row_group_size(table_path) == 32_768


def test_compressed_tables_use_finer_row_groups(tmp_path):
    """A compressed row group carries no RCIX, so its size is capped.

    A reader that touches one column of one row still decodes the whole
    compressed group, so the group is kept small; the plain layout keeps the
    large adaptive group, where the RCIX fast lanes give O(1) column access.
    """
    db_dir = tmp_path / "compressed_rg"
    client = ApexClient(str(db_dir), drop_if_exists=True)
    rows = 20_000
    for table, compression in (("plain_rg", None), ("zstd_rg", "zstd")):
        client.create_table(table, schema={"k": "int64", "s": "string"})
        client.use_table(table)
        if compression:
            assert client.set_compression(compression) is not False
        client.store({"k": list(range(rows)), "s": [f"v{i % 50}" for i in range(rows)]})
        client.flush()
    client.close()

    plain_size = _read_row_group_size(db_dir / "plain_rg.apex")
    zstd_size = _read_row_group_size(db_dir / "zstd_rg.apex")
    assert plain_size == 65_536
    assert zstd_size == 4_096
    assert zstd_size < plain_size

    # The cap is a layout choice: both files must read back whole.
    reopened = ApexClient(str(db_dir))
    for table in ("plain_rg", "zstd_rg"):
        reopened.use_table(table)
        assert reopened.execute(f"SELECT count(*) FROM {table}").scalar() == rows
        assert (
            reopened.execute(f"SELECT sum(k) FROM {table}").scalar()
            == rows * (rows - 1) // 2
        )
        row = reopened.execute(f"SELECT k, s FROM {table} WHERE _id = {rows - 3}").to_dict()[0]
        assert row["k"] == rows - 4
        assert row["s"] == f"v{(rows - 4) % 50}"
    reopened.close()


def test_string_encoding_stays_consistent_across_row_groups(tmp_path):
    db_dir = tmp_path / "mixed_string_encoding"
    client = ApexClient(str(db_dir), drop_if_exists=True)
    client.create_table("events")
    client.use_table("events")

    row_count = 70_000
    categories = ["common"] * 32_768 + [f"unique_{i}" for i in range(32_768, row_count)]
    client.store({
        "category": categories,
        "payload": ["x" * 256] * row_count,
    })
    client.close()

    reopened = ApexClient(str(db_dir))
    reopened.use_table("events")
    result = reopened.execute(
        "SELECT category FROM events WHERE category = 'unique_69999'"
    ).to_dict()
    assert result == [{"category": "unique_69999"}]
    reopened.close()
