"""S11: grace-hash spill JOIN fallback (W3-3).

Over-budget equi-joins must hash-partition both sides, join partition pairs and
concatenate, producing exactly the in-memory result for INNER/LEFT/RIGHT and
removing the per-query spill area.
"""

import pytest

from apexbase import ApexClient

LIMIT_ENV = "APEX_QUERY_MEMORY_MB"
ROWS = 40_000


@pytest.fixture
def client(tmp_path):
    instance = ApexClient(str(tmp_path / "db"), drop_if_exists=True, enable_cache=False)
    for name, offset in (("a", 0), ("b", 1)):
        instance.create_table(name, {"k": "int64", "v": "int64"})
        instance.use_table(name)
        instance.store(
            {
                "k": [i + offset for i in range(ROWS)],
                "v": [i * 2 for i in range(ROWS)],
            }
        )
        instance.flush()
    yield instance
    instance.close()


def _sorted_rows(rows):
    return sorted(tuple(sorted(row.items())) for row in rows)


def _spill_setup(tmp_path, monkeypatch):
    spill_dir = tmp_path / "spill"
    monkeypatch.setenv("APEX_SPILL_DIR", str(spill_dir))
    return spill_dir


def test_inner_join_spills_and_matches_unlimited(client, tmp_path, monkeypatch):
    unlimited = client.execute(
        "SELECT a.k, a.v, b.v AS w FROM a JOIN b ON a.k = b.k"
    ).to_dict()
    assert len(unlimited) == ROWS - 1

    spill_dir = _spill_setup(tmp_path, monkeypatch)
    monkeypatch.setenv(LIMIT_ENV, "1")
    spilled = client.execute(
        "SELECT a.k, a.v, b.v AS w FROM a JOIN b ON a.k = b.k"
    ).to_dict()
    assert _sorted_rows(spilled) == _sorted_rows(unlimited)
    assert spill_dir.exists()
    assert not any(spill_dir.iterdir()), "spill area must be cleaned up"


def test_left_join_spill_keeps_unmatched_rows(client, tmp_path, monkeypatch):
    sql = "SELECT a.k, b.v AS w FROM a LEFT JOIN b ON a.k = b.k"
    unlimited = client.execute(sql).to_dict()
    assert len(unlimited) == ROWS

    _spill_setup(tmp_path, monkeypatch)
    monkeypatch.setenv(LIMIT_ENV, "1")
    spilled = client.execute(sql).to_dict()
    assert len(spilled) == ROWS
    assert sum(1 for row in spilled if row["w"] is None) == 1

    # Disjoint right side: every left row is unmatched, nothing is dropped.
    client.create_table("c", {"k": "int64", "v": "int64"})
    client.use_table("c")
    client.store({"k": [i + 1_000_000 for i in range(ROWS)], "v": [1] * ROWS})
    client.flush()
    all_unmatched = client.execute(
        "SELECT a.k, c.v AS w FROM a LEFT JOIN c ON a.k = c.k"
    ).to_dict()
    assert len(all_unmatched) == ROWS
    assert all(row["w"] is None for row in all_unmatched)
    unlimited_all = client.execute(
        "SELECT a.k, c.v AS w FROM a LEFT JOIN c ON a.k = c.k"
    ).to_dict()
    assert len(unlimited_all) == ROWS


def test_join_spill_honours_global_order_and_limit(client, tmp_path, monkeypatch):
    sql = (
        "SELECT a.k FROM a JOIN b ON a.k = b.k "
        "ORDER BY a.k DESC LIMIT 5"
    )
    unlimited = client.execute(sql).to_dict()
    assert [row["k"] for row in unlimited] == [ROWS - 1, ROWS - 2, ROWS - 3, ROWS - 4, ROWS - 5]

    _spill_setup(tmp_path, monkeypatch)
    monkeypatch.setenv(LIMIT_ENV, "1")
    assert client.execute(sql).to_dict() == unlimited


def test_unavailable_spill_area_fails_explicitly(client, tmp_path, monkeypatch):
    blocker = tmp_path / "blocked"
    blocker.write_text("x")
    monkeypatch.setenv("APEX_SPILL_DIR", str(blocker))
    monkeypatch.setenv(LIMIT_ENV, "1")
    with pytest.raises(Exception) as error:
        client.execute("SELECT a.k FROM a JOIN b ON a.k = b.k").to_dict()
    message = str(error.value)
    assert "query memory budget exceeded" in message
    assert "spill" in message.lower()
    assert blocker.read_text() == "x"


def test_in_memory_join_does_not_create_spill_areas(client, tmp_path, monkeypatch):
    spill_dir = _spill_setup(tmp_path, monkeypatch)
    monkeypatch.delenv(LIMIT_ENV, raising=False)
    assert len(client.execute("SELECT a.k FROM a JOIN b ON a.k = b.k").to_dict()) == ROWS - 1
    assert not spill_dir.exists() or not any(spill_dir.iterdir())
