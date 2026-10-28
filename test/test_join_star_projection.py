"""A star projection combined with a JOIN must keep the joined tables' columns.

At HEAD (518ea32) the general hash-join route collapses a star projection's
``required_columns()`` (``None`` == "read every column") into an empty column
list and then only adds the JOIN keys, so every non-key column is absent from
the joined batch:

    SELECT t.*, p.name AS project_name FROM tasks t LEFT JOIN projects p ...
        -> RuntimeError: Projected column 'p.name' does not exist
    SELECT t.* FROM tasks t LEFT JOIN projects p ...
        -> silently returns only {'id', 'p.id'}; name/status are dropped
    SELECT t.*, p.* ... / SELECT p.*, t.name AS tn ... / the unqualified
    `t.*, name` form / INNER and RIGHT variants
        -> the same class of failure or silent column loss

The fix must read the full column set for a star projection.  The parser expands
``alias.*`` as a plain star, so a star shape may also carry the other side's
columns; a star result is therefore checked for *containing* the base table's
columns with values taken from the correct side (the reported contract), while
shapes with an unambiguous column list are checked exactly.  The unaffected
shapes - no JOIN, explicit column lists, comma joins, COUNT(*) - pin their old
answers so the fix cannot regress them.
"""

from __future__ import annotations

import tempfile

import pytest

from apexbase import ApexClient

# tasks.id: 1,2 ; projects.id: 10,2 -> only task 2 matches project 2.
TASKS = {"id": [1, 2], "name": ["task_a", "task_b"], "status": ["todo", "doing"]}
PROJECTS = {"id": [10, 2], "name": ["proj_X", "proj_Y"], "owner": ["alice", "bob"]}
# Distinctive string values from the projects side, used to prove a star shape
# returned the right table's values (not only the left table's columns).
PROJECT_STRINGS = {"proj_X", "proj_Y", "alice", "bob"}

LEFT = "tasks t LEFT JOIN projects p ON p.id = t.id"
INNER = "tasks t JOIN projects p ON p.id = t.id"
RIGHT = "tasks t RIGHT JOIN projects p ON p.id = t.id"


def _client(tmp):
    client = ApexClient(tmp, enable_cache=False, durability="fast")
    client.create_table("tasks", schema={"id": "int64", "name": "string", "status": "string"})
    client.use_table("tasks")
    client.store(TASKS)
    client.flush()
    client.create_table("projects", schema={"id": "int64", "name": "string", "owner": "string"})
    client.use_table("projects")
    client.store(PROJECTS)
    client.flush()
    return client


def _rows(client, sql):
    return client.execute(sql).to_dict()


def _columns(rows):
    assert rows, "query returned no rows"
    return set(rows[0])


def _by(rows, key):
    return {row[key]: row for row in rows}


def _project_strings(row):
    """Project-side values visible anywhere in the row (name-agnostic)."""
    return {value for value in row.values() if isinstance(value, str) and value in PROJECT_STRINGS}


def _assert_task_columns(rows):
    """``t.*`` must keep id/name/status, all carrying tasks-side values."""
    columns = _columns(rows)
    missing = {"id", "name", "status"} - columns
    assert not missing, f"star projection dropped task columns {sorted(missing)}; got {sorted(columns)}"
    by_id = _by(rows, "id")
    assert sorted(by_id) == [1, 2], f"expected tasks ids 1,2; got {sorted(by_id)}"
    assert by_id[1]["name"] == "task_a", by_id[1]
    assert by_id[1]["status"] == "todo", by_id[1]
    assert by_id[2]["name"] == "task_b", by_id[2]
    assert by_id[2]["status"] == "doing", by_id[2]


def test_left_join_qualified_star_plus_qualified_column():
    """The reported repro: `SELECT t.*, p.name AS project_name ... LEFT JOIN`."""
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(
                client,
                f"SELECT t.*, p.name AS project_name FROM {LEFT}",
            )
            assert len(rows) == 2, rows
            _assert_task_columns(rows)
            assert "project_name" in _columns(rows), rows
            by_id = _by(rows, "id")
            assert by_id[1]["project_name"] is None, by_id[1]
            assert by_id[2]["project_name"] == "proj_Y", by_id[2]
        finally:
            client.close()


def test_left_join_qualified_star_reads_the_base_table():
    """`SELECT t.* ... LEFT JOIN` used to silently return only {'id', 'p.id'}."""
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(client, f"SELECT t.* FROM {LEFT}")
            assert len(rows) == 2, rows
            _assert_task_columns(rows)
        finally:
            client.close()


def test_left_join_star_and_star_keeps_both_sides():
    """`SELECT t.*, p.*` used to silently drop every non-key column."""
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(client, f"SELECT t.*, p.* FROM {LEFT}")
            assert len(rows) == 2, rows
            _assert_task_columns(rows)
            by_id = _by(rows, "id")
            assert _project_strings(by_id[1]) == set(), by_id[1]
            assert _project_strings(by_id[2]) == {"proj_Y", "bob"}, by_id[2]
        finally:
            client.close()


def test_left_join_star_of_right_table_plus_qualified_name():
    """`SELECT p.*, t.name AS tn ...` reported "Projected column 't.name' does not exist"."""
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(client, f"SELECT p.*, t.name AS tn FROM {LEFT}")
            assert len(rows) == 2, rows
            columns = _columns(rows)
            assert "tn" in columns, columns
            # Both task rows survive; only task 2 has a matching project.
            assert sorted(row["tn"] for row in rows) == ["task_a", "task_b"], rows
            with_project = [row for row in rows if _project_strings(row)]
            without_project = [row for row in rows if not _project_strings(row)]
            assert len(with_project) == 1 and len(without_project) == 1, rows
            assert with_project[0]["tn"] == "task_b", with_project[0]
            assert _project_strings(with_project[0]) == {"proj_Y", "bob"}, with_project[0]
            assert without_project[0]["tn"] == "task_a", without_project[0]
        finally:
            client.close()


def test_left_join_star_plus_unqualified_column():
    """`SELECT t.*, name ...` reported "Projected column 'name' does not exist"."""
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(client, f"SELECT t.*, name FROM {LEFT}")
            assert len(rows) == 2, rows
            _assert_task_columns(rows)
            assert [row["name"] for row in sorted(rows, key=lambda row: row["id"])] == [
                "task_a",
                "task_b",
            ], rows
        finally:
            client.close()


def test_left_join_star_plus_literal():
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(client, f"SELECT t.*, 'x' AS lit FROM {LEFT}")
            assert len(rows) == 2, rows
            _assert_task_columns(rows)
            assert all(row["lit"] == "x" for row in rows), rows
        finally:
            client.close()


def test_left_join_star_plus_expression_on_the_right_table():
    """An expression over the right side needs the right table read in full."""
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(client, f"SELECT t.*, upper(p.name) AS pn FROM {LEFT}")
            assert len(rows) == 2, rows
            _assert_task_columns(rows)
            by_id = _by(rows, "id")
            assert by_id[1]["pn"] is None, by_id[1]
            assert by_id[2]["pn"] == "PROJ_Y", by_id[2]
        finally:
            client.close()


def test_inner_join_qualified_star_plus_qualified_column():
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(client, f"SELECT t.*, p.name AS project_name FROM {INNER}")
            assert len(rows) == 1, rows
            row = rows[0]
            assert {"id", "name", "status", "project_name"} <= set(row), row
            assert (
                row["id"],
                row["name"],
                row["status"],
                row["project_name"],
            ) == (2, "task_b", "doing", "proj_Y"), row
        finally:
            client.close()


def test_right_join_qualified_star_plus_qualified_column():
    """RIGHT JOIN runs as a swapped LEFT JOIN; the task columns must survive."""
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(
                client,
                f"SELECT t.*, p.name AS project_name, p.id AS pid FROM {RIGHT}",
            )
            assert len(rows) == 2, rows
            columns = _columns(rows)
            by_pid = _by(rows, "pid")
            assert sorted(by_pid) == [2, 10], sorted(by_pid)
            assert by_pid[10]["project_name"] == "proj_X", by_pid[10]
            assert by_pid[2]["project_name"] == "proj_Y", by_pid[2]
            # The preserved side is projects: task columns are NULL-extended for
            # project 10 and carry task 2's values for the matched row.  A
            # qualified copy (`t.name`/`t.status`) is accepted as well.
            name_col = next((col for col in ("name", "t.name") if col in columns), None)
            status_col = next((col for col in ("status", "t.status") if col in columns), None)
            assert name_col is not None, f"task name column missing; got {sorted(columns)}"
            assert status_col is not None, f"task status column missing; got {sorted(columns)}"
            assert by_pid[2][name_col] == "task_b", by_pid[2]
            assert by_pid[2][status_col] == "doing", by_pid[2]
            assert by_pid[10][name_col] is None, by_pid[10]
            assert by_pid[10][status_col] is None, by_pid[10]
        finally:
            client.close()


def test_right_join_star_of_right_table_plus_qualified_name():
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(client, f"SELECT p.*, t.name AS tn FROM {RIGHT}")
            assert len(rows) == 2, rows
            seen = set()
            for row in rows:
                values = _project_strings(row)
                seen |= values
                if "proj_X" in values:
                    assert row["tn"] is None, row
                elif "proj_Y" in values:
                    assert row["tn"] == "task_b", row
                else:
                    pytest.fail(f"row does not expose the projects side: {row}")
            assert seen == PROJECT_STRINGS, seen
        finally:
            client.close()


def test_plain_star_without_join_is_unchanged():
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(client, "SELECT t.* FROM tasks t")
            assert _columns(rows) == {"id", "name", "status"}
            assert sorted(rows, key=lambda row: row["id"]) == [
                {"id": 1, "name": "task_a", "status": "todo"},
                {"id": 2, "name": "task_b", "status": "doing"},
            ], rows
        finally:
            client.close()


def test_explicit_columns_in_a_join_are_unchanged():
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(
                client,
                f"SELECT t.id, t.name, p.name AS project_name FROM {LEFT}",
            )
            assert _columns(rows) == {"id", "name", "project_name"}
            assert sorted(rows, key=lambda row: row["id"]) == [
                {"id": 1, "name": "task_a", "project_name": None},
                {"id": 2, "name": "task_b", "project_name": "proj_Y"},
            ], rows
        finally:
            client.close()


def test_comma_join_is_unchanged():
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            rows = _rows(
                client,
                "SELECT t.id AS tid, t.name AS tname, p.name AS pname, p.owner AS powner "
                "FROM tasks t, projects p WHERE p.id = t.id",
            )
            assert _columns(rows) == {"tid", "tname", "pname", "powner"}
            assert rows == [{"tid": 2, "tname": "task_b", "pname": "proj_Y", "powner": "bob"}], rows
        finally:
            client.close()


@pytest.mark.parametrize(
    "sql, expected",
    [
        (f"SELECT COUNT(*) AS n FROM {LEFT}", 2),
        (f"SELECT COUNT(*) AS n FROM {INNER}", 1),
        (f"SELECT COUNT(*) AS n FROM {RIGHT}", 2),
        ("SELECT COUNT(*) AS n FROM tasks t, projects p WHERE p.id = t.id", 1),
    ],
)
def test_count_star_over_a_join_is_unchanged(sql, expected):
    with tempfile.TemporaryDirectory(prefix="apexbase_join_star_") as tmp:
        client = _client(tmp)
        try:
            assert _rows(client, sql) == [{"n": expected}]
        finally:
            client.close()
