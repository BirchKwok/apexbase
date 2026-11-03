"""V5 S6: unified statement routing D9/D11.

D9 -- transaction batches run in one transaction in both transports (embedded
      and PG wire), ROLLBACK is effective, implicit multi-statement batches are
      atomic, and an error inside a transaction rolls it back and clears the
      connection state explicitly.
D11 -- `COPY <new table> FROM <file>` without a selected table answers through
      the scalar entry (no first-import pyarrow cliff) and publishes the new
      table for following statements.
"""

import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

from apexbase import ApexClient  # noqa: E402


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    sys.modules[name] = module
    return module


@pytest.fixture(scope="module")
def pg(tmp_path_factory):
    module = _load_module(
        "bench_pg_wire_concurrency", ROOT / "benchmarks" / "bench_pg_wire_concurrency.py"
    )
    db_dir = tmp_path_factory.mktemp("stmt_routing_db")
    client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)
    for name in ("t_batch", "t_msgs", "t_atomic", "t_error", "t_use"):
        client.create_table(name, {"id": "int64", "v": "int64"})
        client.store({"id": [1], "v": [10]})
    client.flush()
    client.close()
    port = module.free_port()
    process = module.start_server(str(db_dir), port)
    clients = []

    def connect():
        pg_client = module.MinimalPgClient("127.0.0.1", port)
        clients.append(pg_client)
        return pg_client

    try:
        yield module, connect
    finally:
        for pg_client in clients:
            try:
                pg_client.close()
            except Exception:
                pass
        module.stop_server(process)


def _ids(pg_client, table):
    rows = pg_client.query(f"SELECT id FROM {table} ORDER BY id")
    return [int(row[0]) for row in rows]


def _expect_error(pg_client, sql):
    with pytest.raises(Exception):
        pg_client.query(sql)


# ── D9: PG wire transactions ────────────────────────────────────────────────


def test_pg_batch_begin_insert_rollback_is_one_transaction(pg):
    _, connect = pg
    client = connect()
    client.query("BEGIN; INSERT INTO t_batch VALUES (2, 20); ROLLBACK;")
    assert _ids(client, "t_batch") == [1]
    client.query("BEGIN; INSERT INTO t_batch VALUES (3, 30); COMMIT;")
    assert _ids(client, "t_batch") == [1, 3]


def test_pg_transaction_across_messages(pg):
    _, connect = pg
    client = connect()
    client.query("BEGIN")
    client.query("INSERT INTO t_msgs VALUES (2, 20)")
    # The open transaction is visible to the same connection before commit.
    assert _ids(client, "t_msgs") == [1, 2]
    client.query("ROLLBACK")
    assert _ids(client, "t_msgs") == [1]

    client.query("BEGIN")
    client.query("INSERT INTO t_msgs VALUES (4, 40)")
    client.query("COMMIT")
    assert _ids(client, "t_msgs") == [1, 4]


def test_pg_implicit_multi_statement_batch_is_atomic(pg):
    _, connect = pg
    client = connect()
    client.query("INSERT INTO t_atomic VALUES (2, 20); INSERT INTO t_atomic VALUES (3, 30);")
    assert _ids(client, "t_atomic") == [1, 2, 3]
    _expect_error(
        client,
        "INSERT INTO t_atomic VALUES (4, 40); INSERT INTO missing_table VALUES (5, 50);",
    )
    # The successful first statement of the failed implicit batch is rolled back.
    assert _ids(client, "t_atomic") == [1, 2, 3]


def test_pg_transaction_error_rolls_back_and_clears_state(pg):
    _, connect = pg
    client = connect()
    client.query("BEGIN")
    client.query("INSERT INTO t_error VALUES (2, 20)")
    _expect_error(client, "INSERT INTO missing_table VALUES (9, 90)")
    # The transaction was rolled back and the connection is back in auto-commit.
    assert _ids(client, "t_error") == [1]
    client.query("INSERT INTO t_error VALUES (5, 50)")
    assert _ids(client, "t_error") == [1, 5]


def test_pg_use_inside_batch_is_an_explicit_error(pg):
    _, connect = pg
    client = connect()
    _expect_error(client, "BEGIN; USE other_db; ROLLBACK;")
    # The failed batch leaves the connection in auto-commit.
    client.query("INSERT INTO t_use VALUES (2, 20)")
    assert _ids(client, "t_use") == [1, 2]


# ── D11: COPY routes through the scalar entry ───────────────────────────────


def test_copy_without_selected_table_avoids_pyarrow_import(tmp_path):
    db_dir = tmp_path / "copy_scalar"
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id,v\n1,10\n2,20\n")
    script = f"""
import json, sys
from apexbase import ApexClient
client = ApexClient({db_dir.as_posix()!r}, drop_if_exists=True, enable_cache=False)
before = 'pyarrow' in sys.modules
result = client.execute("COPY newtab FROM '{csv_path.as_posix()}'").to_dict()
after = 'pyarrow' in sys.modules
rows = client.execute("SELECT * FROM newtab ORDER BY id").to_dict()
print(json.dumps({{
    "result": result,
    "pyarrow_before": before,
    "pyarrow_after": after,
    "current_table": client._current_table,
    "rows": rows,
}}))
client.close()
"""
    output = subprocess.check_output(
        [sys.executable, "-c", script], cwd=str(ROOT), text=True, timeout=180
    )
    payload = json.loads(output.strip().splitlines()[-1])
    assert payload["pyarrow_before"] is False
    assert payload["pyarrow_after"] is False, "COPY must not import pyarrow"
    assert payload["result"] == [{"result": 2}]
    assert payload["current_table"] == "newtab"
    assert payload["rows"] == [{"id": 1, "v": 10}, {"id": 2, "v": 20}]


def test_embedded_multistatement_transaction_and_batch(tmp_path):
    client = ApexClient(str(tmp_path / "embedded"), drop_if_exists=True, enable_cache=False)
    try:
        client.create_table("t", {"id": "int64", "v": "int64"})
        client.store({"id": [1], "v": [10]})
        client.flush()

        # One execute() call: BEGIN / INSERT / ROLLBACK is one transaction.
        client.execute("BEGIN; INSERT INTO t VALUES (2, 20); ROLLBACK;")
        assert client.execute("SELECT id FROM t ORDER BY id").to_dict() == [{"id": 1}]

        # Client-side transaction spanning a multi-statement batch.
        client.execute("BEGIN")
        client.execute("INSERT INTO t VALUES (3, 30); INSERT INTO t VALUES (4, 40);")
        client.execute("ROLLBACK")
        assert client.execute("SELECT id FROM t ORDER BY id").to_dict() == [{"id": 1}]

        # Ordered batch: read-after-write visibility.
        client.execute_batch(
            [
                "INSERT INTO t VALUES (5, 50)",
                "SELECT id FROM t ORDER BY id",
            ]
        )
        rows = client.execute("SELECT id FROM t ORDER BY id").to_dict()
        assert [row["id"] for row in rows] == [1, 5]
    finally:
        client.close()
