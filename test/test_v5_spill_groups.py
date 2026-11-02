"""S11: GROUP BY / DISTINCT spill fallbacks (W3-3).

Over-budget single-batch aggregation shapes must hash-partition their state to
disk, produce exactly the in-memory result (after the global ORDER BY / LIMIT),
and remove their per-query spill area. Ordering without ORDER BY is unspecified,
so comparisons sort the rows.
"""

import pytest

from apexbase import ApexClient

LIMIT_ENV = "APEX_QUERY_MEMORY_MB"
ROWS = 60_000


@pytest.fixture
def client(tmp_path):
    instance = ApexClient(str(tmp_path / "db"), drop_if_exists=True, enable_cache=False)
    instance.create_table("g", {"k": "string", "v": "int64", "cat": "int64"})
    instance.use_table("g")
    instance.store(
        {
            "k": [f"key{i:06d}" for i in range(ROWS)],
            "v": list(range(ROWS)),
            "cat": [i % 7 for i in range(ROWS)],
        }
    )
    instance.flush()
    yield instance
    instance.close()


def _sorted_dicts(rows):
    return sorted((tuple(sorted(row.items())) for row in rows))


def test_group_by_spills_and_matches_unlimited(client, tmp_path, monkeypatch):
    unlimited = client.execute("SELECT k, COUNT(*) AS n FROM g GROUP BY k").to_dict()
    assert len(unlimited) == ROWS

    spill_dir = tmp_path / "spill"
    monkeypatch.setenv("APEX_SPILL_DIR", str(spill_dir))
    monkeypatch.setenv(LIMIT_ENV, "1")
    spilled = client.execute("SELECT k, COUNT(*) AS n FROM g GROUP BY k").to_dict()
    assert _sorted_dicts(spilled) == _sorted_dicts(unlimited)
    assert not any(spill_dir.iterdir()), "spill area must be cleaned up"


def test_group_by_spill_applies_global_order_and_limit(client, tmp_path, monkeypatch):
    unlimited = client.execute(
        "SELECT cat, COUNT(*) AS n FROM g GROUP BY cat ORDER BY n DESC LIMIT 3"
    ).to_dict()
    assert [row["cat"] for row in unlimited] == [0, 1, 2]

    monkeypatch.setenv("APEX_SPILL_DIR", str(tmp_path / "spill"))
    monkeypatch.setenv(LIMIT_ENV, "1")
    spilled = client.execute(
        "SELECT cat, COUNT(*) AS n FROM g GROUP BY cat ORDER BY n DESC LIMIT 3"
    ).to_dict()
    assert spilled == unlimited


def test_two_key_group_by_and_having_spill(client, tmp_path, monkeypatch):
    sql = (
        "SELECT cat, k, COUNT(*) AS n FROM g GROUP BY cat, k "
        "HAVING COUNT(*) >= 1 ORDER BY cat ASC, k ASC LIMIT 20"
    )
    unlimited = client.execute(sql).to_dict()
    assert len(unlimited) == 20

    monkeypatch.setenv("APEX_SPILL_DIR", str(tmp_path / "spill"))
    monkeypatch.setenv(LIMIT_ENV, "1")
    spilled = client.execute(sql).to_dict()
    assert spilled == unlimited


def test_select_distinct_spills_and_matches_unlimited(client, tmp_path, monkeypatch):
    unlimited = client.execute("SELECT DISTINCT cat FROM g").to_dict()
    assert sorted(row["cat"] for row in unlimited) == list(range(7))

    spill_dir = tmp_path / "spill"
    monkeypatch.setenv("APEX_SPILL_DIR", str(spill_dir))
    monkeypatch.setenv(LIMIT_ENV, "1")
    spilled = client.execute("SELECT DISTINCT cat FROM g").to_dict()
    assert _sorted_dicts(spilled) == _sorted_dicts(unlimited)

    # High-cardinality DISTINCT exercises the partition path for real.
    spilled_keys = client.execute("SELECT DISTINCT k FROM g").to_dict()
    assert len(spilled_keys) == ROWS
    assert spill_dir.exists()
    assert not any(spill_dir.iterdir())


def test_select_distinct_spill_honours_order_and_limit(client, tmp_path, monkeypatch):
    sql = "SELECT DISTINCT cat FROM g ORDER BY cat DESC LIMIT 2"
    unlimited = client.execute(sql).to_dict()
    assert [row["cat"] for row in unlimited] == [6, 5]

    monkeypatch.setenv("APEX_SPILL_DIR", str(tmp_path / "spill"))
    monkeypatch.setenv(LIMIT_ENV, "1")
    assert client.execute(sql).to_dict() == unlimited


def test_in_memory_paths_do_not_create_spill_areas(client, tmp_path, monkeypatch):
    spill_dir = tmp_path / "spill"
    monkeypatch.setenv("APEX_SPILL_DIR", str(spill_dir))
    monkeypatch.delenv(LIMIT_ENV, raising=False)
    assert len(client.execute("SELECT k, COUNT(*) AS n FROM g GROUP BY k").to_dict()) == ROWS
    assert len(client.execute("SELECT DISTINCT cat FROM g").to_dict()) == 7
    assert not spill_dir.exists() or not any(spill_dir.iterdir())
