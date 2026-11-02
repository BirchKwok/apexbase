"""S12: opt-in streaming results (WORK IN PROGRESS: Python iterator + Flight)."""

import json
import os
import subprocess
import sys
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 200_000


@pytest.fixture
def client(tmp_path):
    instance = ApexClient(str(tmp_path / "db"), drop_if_exists=True, enable_cache=False)
    instance.create_table("t", {"v": "int64", "s": "string"})
    instance.use_table("t")
    instance.store({"v": list(range(ROWS)), "s": [f"x{i % 10}" for i in range(ROWS)]})
    instance.flush()
    yield instance
    instance.close()


def test_execute_stream_is_batched_and_complete(client):
    batches = 0
    total = 0
    max_rows = 0
    for batch in client.execute_stream("SELECT v, s FROM t"):
        import pyarrow as pa

        assert isinstance(batch, pa.RecordBatch)
        batches += 1
        total += batch.num_rows
        max_rows = max(max_rows, batch.num_rows)
    assert total == ROWS
    assert batches >= 2, "a 200k-row result must stream in several row-group batches"
    assert max_rows < ROWS

    # The default execute() path is unchanged and returns the same result.
    rows = client.execute("SELECT v, s FROM t").to_dict()
    assert len(rows) == ROWS
    assert rows[0] == {"v": 0, "s": "x0"}


def test_execute_stream_where_and_projection(client):
    rows = []
    for batch in client.execute_stream(
        "SELECT v FROM t WHERE v >= 199000 AND v < 199010"
    ):
        rows.extend(batch.to_pydict()["v"])
    assert rows == list(range(199000, 199010))


def test_execute_stream_materialised_shapes_arrive_as_one_batch(client):
    rows = []
    for batch in client.execute_stream(
        "SELECT s, COUNT(*) AS n FROM t GROUP BY s ORDER BY s"
    ):
        rows.extend(batch.to_pydict()["n"])
    assert len(rows) == 10
    assert sum(rows) == ROWS

    ordered = []
    for batch in client.execute_stream("SELECT v FROM t ORDER BY v DESC LIMIT 3"):
        ordered.extend(batch.to_pydict()["v"])
    assert ordered == [ROWS - 1, ROWS - 2, ROWS - 3]


def test_execute_stream_close_stops_early_and_keeps_connection_usable(client):
    iterator = client.execute_stream("SELECT v FROM t")
    first = next(iterator)
    assert first.num_rows > 0
    iterator.close()
    with pytest.raises(StopIteration):
        next(iterator)
    assert client.execute("SELECT COUNT(*) FROM t").scalar() == ROWS


def test_execute_stream_errors_are_explicit(client):
    iterator = client.execute_stream("SELECT * FROM missing_stream_table")
    with pytest.raises(RuntimeError):
        next(iterator)
    # The connection stays usable after a failed stream.
    assert client.execute("SELECT COUNT(*) FROM t").scalar() == ROWS


def _peak_mb():
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/self/status", encoding="utf-8") as status:
                for line in status:
                    if line.startswith("VmHWM:"):
                        return int(line.split()[1]) / 1024
        except OSError:
            pass
    import resource

    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage / (1024 * 1024) if sys.platform == "darwin" else usage / 1024


_CHILD = r'''
import json, sys
from apexbase import ApexClient

directory, mode = sys.argv[1], sys.argv[2]
client = ApexClient(directory)
client.use_table("t")
if mode == "stream":
    total = 0
    for batch in client.execute_stream("SELECT v, s FROM t"):
        total += batch.num_rows
    outcome = total
else:
    outcome = len(client.execute("SELECT v, s FROM t").to_dict())
try:
    from test_v5_streaming import _peak_mb as peak
except Exception:
    peak = None

def peak_mb():
    import resource
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage / (1024 * 1024) if sys.platform == "darwin" else usage / 1024

print(json.dumps({"rows": outcome, "peak_mb": peak_mb()}))
client.close()
'''


def test_streaming_peak_memory_is_lower_than_materialized():
    with tempfile.TemporaryDirectory() as tmp:
        seed = ApexClient(os.path.join(tmp, "db"), drop_if_exists=True, enable_cache=False)
        seed.create_table("t", {"v": "int64", "s": "string"})
        seed.use_table("t")
        rows = 400_000
        chunk = 100_000
        for start in range(0, rows, chunk):
            end = min(start + chunk, rows)
            seed.store(
                {
                    "v": list(range(start, end)),
                    "s": [f"x{i % 10}" for i in range(start, end)],
                }
            )
        seed.flush()
        seed.close()

        results = {}
        for mode in ("stream", "materialized"):
            completed = subprocess.run(
                [sys.executable, "-c", _CHILD, os.path.join(tmp, "db"), mode],
                capture_output=True,
                text=True,
                timeout=300,
            )
            assert completed.returncode == 0, completed.stderr
            results[mode] = json.loads(completed.stdout.strip().splitlines()[-1])

    assert results["stream"]["rows"] == rows
    assert results["materialized"]["rows"] == rows
    assert results["stream"]["peak_mb"] < results["materialized"]["peak_mb"], results
