"""S10: process-level memory admission and bounded query queueing.

The environment is configured per subprocess so tests never mutate the pytest
process state. Every upper entry point (Python embedded, PG wire, Flight) goes
through ``Session::execute``, where admission is installed, so exercising the
Python API covers the same gate.
"""

import json
import os
import subprocess
import sys
import textwrap


def _run(script: str, env: dict) -> dict:
    merged = os.environ.copy()
    merged.pop("APEX_PROCESS_BUDGET_MB", None)
    merged.pop("APEX_QUERY_ADMISSION_MB", None)
    merged.pop("APEX_QUERY_QUEUE_MAX", None)
    merged.pop("APEX_QUERY_QUEUE_WAIT_MS", None)
    import tempfile
    directory = tempfile.mkdtemp(prefix="apex_admission_")
    # The path is embedded inside a Python string literal in the child script;
    # escape backslashes so Windows paths survive -c compilation.
    script = script.replace("__DIR__", directory.replace("\\", "\\\\"))
    env = {key: value.replace("__DIR__", directory) if isinstance(value, str) else value
           for key, value in env.items()}
    merged.update(env)
    completed = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        env=merged,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_admission_disabled_by_default():
    result = _run(
        """
        import json
        from apexbase import ApexClient
        client = ApexClient(dirpath="__DIR__")
        client.execute("CREATE TABLE t (v BIGINT)")
        client.execute("INSERT INTO t VALUES (1)")
        rows = client.execute("SELECT COUNT(*) FROM t").scalar()
        stats = client._storage._admission_stats()
        print(json.dumps({
            "rows": rows,
            "budget": stats[5],
            "admitted": stats[2],
            "used": stats[0],
        }))
        """,
        {},
    )
    assert result == {"rows": 1, "budget": 0, "admitted": 0, "used": 0}


def test_light_statements_are_admitted_and_released():
    result = _run(
        """
        import json
        from apexbase import ApexClient
        client = ApexClient(dirpath="__DIR__")
        client.execute("CREATE TABLE t (v BIGINT)")
        client.execute("INSERT INTO t VALUES (1), (2), (3)")
        for _ in range(5):
            assert client.execute("SELECT v FROM t WHERE v = 2").scalar() == 2
        stats = client._storage._admission_stats()
        print(json.dumps({
            "budget": stats[5],
            "admitted": stats[2],
            "used": stats[0],
            "waiting": stats[1],
        }))
        """,
        {"APEX_PROCESS_BUDGET_MB": "64", "APEX_QUERY_ADMISSION_MB": "8"},
    )
    assert result["budget"] == 64 * 1024 * 1024
    assert result["admitted"] >= 5
    assert result["used"] == 0
    assert result["waiting"] == 0


def test_bounded_queue_rejects_when_full_or_timed_out():
    """One heavy statement holds the budget; a second is rejected explicitly."""
    script = """
        import json, os, threading, time
        from apexbase import ApexClient

        directory = os.environ["ADMISSION_DIR"]
        client = ApexClient(dirpath=directory)
        total = 2_000_000
        client.create_table("t", {"v": "int64", "g": "int64"})
        client.store({"v": list(range(total)), "g": [i % 1000 for i in range(total)]})
        client.flush()
        client.close()

        stop = threading.Event()
        holder = ApexClient(dirpath=directory)
        holder.use_table("t")
        def hog():
            while not stop.is_set():
                holder.execute(
                    "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY COUNT(*) DESC LIMIT 10"
                )
        thread = threading.Thread(target=hog, daemon=True)
        thread.start()
        client = ApexClient(dirpath=directory)
        client.use_table("t")
        # Wait until the hog actually holds the reservation; otherwise a fast
        # platform could finish the heavy query before the probe runs.
        hold_deadline = time.time() + 10
        while time.time() < hold_deadline and client._storage._admission_stats()[0] == 0:
            time.sleep(0.001)
        deadline = time.time() + 20
        rejection = None
        while time.time() < deadline and rejection is None:
            try:
                client.execute(
                    "SELECT g, SUM(v) FROM t GROUP BY g ORDER BY g LIMIT 10"
                )
            except RuntimeError as error:
                if "APEX_ADMISSION_" in str(error):
                    rejection = str(error)
        stop.set()
        thread.join(timeout=10)
        holder.close()
        stats = client._storage._admission_stats()
        client.close()
        print(json.dumps({
            "rejection": rejection,
            "timed_out": stats[4],
            "rejected": stats[3],
            "used": stats[0],
        }))
        """
    result = _run(
        script,
        {
            "APEX_PROCESS_BUDGET_MB": "1",
            "APEX_QUERY_QUEUE_WAIT_MS": "0",
            "APEX_QUERY_QUEUE_MAX": "0",
            "ADMISSION_DIR": "__DIR__",
        },
    )
    assert result["rejection"] is not None, result
    assert "APEX_ADMISSION_" in result["rejection"], result
    assert result["used"] == 0, result
