"""Module-level default memory connection behavior and feature parity."""

from pathlib import Path

import apexbase


def setup_function():
    apexbase._close_default_connection()


def teardown_function():
    apexbase._close_default_connection()


def test_execute_reuses_default_memory_connection_without_initialization(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    apexbase.execute("CREATE TABLE items (value BIGINT, label TEXT)")
    apexbase.execute(
        "INSERT INTO items (value, label) VALUES (?, ?)",
        [7, "seven"],
    )

    assert apexbase.execute("SELECT value, label FROM items").to_dict() == [
        {"value": 7, "label": "seven"}
    ]
    assert list(tmp_path.iterdir()) == []


def test_execute_supports_named_parameters_and_result_options():
    apexbase.execute("CREATE TABLE items (value BIGINT)")
    apexbase.execute("INSERT INTO items (value) VALUES (:value)", {"value": 9})

    result = apexbase.execute(
        "SELECT _id, value FROM items WHERE value = :value",
        {"value": 9},
        show_internal_id=True,
    )

    assert result.to_dict() == [{"_id": 1, "value": 9}]


def test_default_connection_is_ephemeral_after_close():
    apexbase.execute("CREATE TABLE transient (value BIGINT)")
    apexbase.execute("INSERT INTO transient VALUES (1)")
    old_root = Path(apexbase._get_default_connection()._dirpath)

    apexbase._close_default_connection()

    assert not old_root.exists()
    apexbase.execute("CREATE TABLE transient (value BIGINT)")
    assert apexbase.execute("SELECT COUNT(*) FROM transient").scalar() == 0


def test_explicit_memory_clients_are_isolated_and_do_not_create_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    first = apexbase.ApexClient(":memory:")
    second = apexbase.ApexClient(":memory:")
    try:
        first.execute("CREATE TABLE isolated (value BIGINT)")
        first.execute("INSERT INTO isolated VALUES (11)")
        assert first.execute("SELECT value FROM isolated").scalar() == 11
        assert second.list_tables() == []
        assert list(tmp_path.iterdir()) == []
    finally:
        first.close()
        second.close()
