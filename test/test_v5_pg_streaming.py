"""S12: PG wire per-batch streaming for simple reads.

The server streams row-group batches through a bounded channel (the first batch
feeds the row description, the rest are pulled lazily). These tests drive a real
server with the minimal wire client to prove large results round-trip correctly
and that commands / transactions / errors keep their existing behaviour.
"""

import importlib.util
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ROWS = 200_000


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    sys.modules[name] = module
    return module


@pytest.fixture(scope="module")
def wire_bench():
    return _load(
        "bench_pg_wire_concurrency",
        ROOT / "benchmarks" / "bench_pg_wire_concurrency.py",
    )


@pytest.fixture(scope="module")
def served(wire_bench):
    import shutil

    from apexbase.client import ApexClient

    tmpdir = Path(tempfile.mkdtemp(prefix="apexbase_pg_stream_test_"))
    db_dir = tmpdir / "db"
    db_dir.mkdir(parents=True)
    client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)
    client.create_table("t", schema={"a": "int64", "s": "string"})
    client.use_table("t")
    client.store(
        {"a": list(range(ROWS)), "s": [f"x{i % 10}" for i in range(ROWS)]}
    )
    client.flush()
    client.close()
    port = wire_bench.free_port()
    process = wire_bench.start_server(str(db_dir), port)
    try:
        yield {"port": port, "rows": ROWS}
    finally:
        wire_bench.stop_server(process)
        shutil.rmtree(tmpdir, ignore_errors=True)


def _client(wire_bench, served):
    return wire_bench.MinimalPgClient("127.0.0.1", served["port"])


def test_large_simple_select_streams_all_rows(wire_bench, served):
    client = _client(wire_bench, served)
    try:
        rows = client.query("SELECT a, s FROM t")
        assert len(rows) == ROWS
        assert rows[0] == ["0", "x0"]
        assert rows[ROWS - 1] == [str(ROWS - 1), f"x{(ROWS - 1) % 10}"]
        # Values are in scan order and every row is present exactly once.
        assert rows[12345] == ["12345", f"x{12345 % 10}"]
    finally:
        client.close()


def test_streamed_filter_and_count_agree(wire_bench, served):
    client = _client(wire_bench, served)
    try:
        filtered = client.query("SELECT a FROM t WHERE a >= 199000")
        assert [row[0] for row in filtered] == [str(v) for v in range(199000, ROWS)]
        assert client.query("SELECT COUNT(*) FROM t") == [[str(ROWS)]]
        assert client.query("SELECT COUNT(*) FROM t WHERE a < 10") == [["10"]]
    finally:
        client.close()


def test_command_and_error_paths_still_work(wire_bench, served):
    client = _client(wire_bench, served)
    try:
        assert client.query("SELECT version()")
        with pytest.raises(wire_bench.PgWireError):
            client.query("SELECT * FROM does_not_exist")
    finally:
        client.close()


def test_transaction_statements_keep_the_stateful_path(wire_bench, served):
    client = _client(wire_bench, served)
    try:
        client.query("BEGIN")
        assert client.query("SELECT COUNT(*) FROM t") == [[str(ROWS)]]
        client.query("ROLLBACK")
        assert client.query("SELECT COUNT(*) FROM t") == [[str(ROWS)]]
    finally:
        client.close()


def test_two_connections_stream_independent_answers(wire_bench, served):
    first = _client(wire_bench, served)
    second = _client(wire_bench, served)
    try:
        rows_first = first.query("SELECT a FROM t WHERE a < 5")
        rows_second = second.query("SELECT a FROM t WHERE a >= 199995")
        assert [row[0] for row in rows_first] == [str(v) for v in range(5)]
        assert [row[0] for row in rows_second] == [
            str(v) for v in range(199995, ROWS)
        ]
    finally:
        first.close()
        second.close()
