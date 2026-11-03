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
import json, os, subprocess, sys
import threading, time
from apexbase import ApexClient


def current_rss_mb():
    if sys.platform.startswith("linux"):
        with open("/proc/self/status", encoding="utf-8") as status:
            for line in status:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    output = subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())])
    return int(output.strip()) / 1024


directory, mode = sys.argv[1], sys.argv[2]
client = ApexClient(directory)
client.use_table("t")
before = current_rss_mb()
peak = [before]
stop = threading.Event()


def sample():
    while not stop.is_set():
        peak[0] = max(peak[0], current_rss_mb())
        time.sleep(0.005)


sampler = threading.Thread(target=sample, daemon=True)
sampler.start()
try:
    if mode == "stream":
        total = 0
        for batch in client.execute_stream("SELECT v, s FROM t"):
            total += batch.num_rows
        outcome = total
    else:
        outcome = len(client.execute("SELECT v, s FROM t").to_dict())
finally:
    stop.set()
    sampler.join(timeout=1.0)
print(json.dumps({"rows": outcome, "growth_mb": max(0.0, peak[0] - before)}))
client.close()
'''


def test_streaming_peak_memory_is_lower_than_materialized():
    with tempfile.TemporaryDirectory() as tmp:
        seed = ApexClient(os.path.join(tmp, "db"), drop_if_exists=True, enable_cache=False)
        seed.create_table("t", {"v": "int64", "s": "string"})
        seed.use_table("t")
        rows = 300_000
        chunk = 100_000
        for start in range(0, rows, chunk):
            end = min(start + chunk, rows)
            seed.store(
                {
                    "v": list(range(start, end)),
                    "s": [f"payload-{i:08d}-" + "x" * 400 for i in range(start, end)],
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
    # Streaming must keep the result out of memory: its query-time RSS growth
    # stays well below materializing every returned dict row. Process peak RSS
    # is not comparable across platforms because fixture construction dominates.
    assert results["stream"]["growth_mb"] < results["materialized"]["growth_mb"] * 0.8, results
    assert results["materialized"]["growth_mb"] > 20.0, results
