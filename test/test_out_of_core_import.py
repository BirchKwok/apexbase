"""Out-of-core import behaviour.

Measured on a 20M-row / 643 MB CSV under a 256 MB process cap: the import path is already
memory-bounded (it added only 32.8 MB RSS, with or without an explicit mid-import flush), so
it persists incrementally rather than pinning the whole table until close. These tests pin
both halves of that: every row arrives and is queryable, and the table file grows *while* the
import is still running.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from apexbase import ApexClient

IMPORT_SNIPPET = """
import sys
from apexbase import ApexClient

database, source, rows = sys.argv[1], sys.argv[2], int(sys.argv[3])
client = ApexClient(database, drop_if_exists=True)
try:
    # A table must be selected before any query; `imported` is then named explicitly.
    client.create_table("_dummy")
    client.use_table("_dummy")
    client.register_temp_table("imported", source)
    count = client.execute("SELECT COUNT(*) AS n FROM imported").scalar()
    if count != rows:
        raise SystemExit(f"row count mismatch: {count} != {rows}")
finally:
    client.close()
"""


def _write_csv(path: Path, rows: int) -> None:
    with path.open("w") as handle:
        handle.write("k,score,category\n")
        for i in range(rows):
            handle.write(f"{i},{i % 1000 / 10.0},group_{i % 100}\n")


def test_large_import_keeps_every_row_queryable(tmp_path):
    """A large import must not lose or duplicate rows."""
    rows = 300_000
    source = tmp_path / "source.csv"
    _write_csv(source, rows)

    client = ApexClient(str(tmp_path / "db"), drop_if_exists=True)
    try:
        client.create_table("_dummy")
        client.use_table("_dummy")
        client.register_temp_table("imported", str(source))
        assert client.execute("SELECT COUNT(*) AS n FROM imported").scalar() == rows
        filtered = client.execute(
            "SELECT COUNT(*) AS n FROM imported WHERE score >= 50"
        ).scalar()
        assert filtered == sum(1 for i in range(rows) if (i % 1000) / 10.0 >= 50)
        groups = client.execute(
            "SELECT category, COUNT(*) AS n FROM imported GROUP BY category"
        ).to_dict()
        assert len(groups) == 100
        assert sum(int(row["n"]) for row in groups) == rows
    finally:
        client.close()


def test_import_persists_while_it_is_still_running(tmp_path):
    """The out-of-core property: the table grows on disk before the import finishes.

    If the import buffered everything and wrote once at the end, the file would stay empty
    until the process exits and the whole table would have to fit in memory. Polling the file
    from the parent proves the engine persists incrementally.
    """
    rows = 600_000
    source = tmp_path / "source.csv"
    _write_csv(source, rows)
    database = tmp_path / "db"
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [p for p in [env.get("PYTHONPATH", ""), str(Path(__file__).resolve().parents[1] / "apexbase" / "python")] if p]
    )

    process = subprocess.Popen(
        [sys.executable, "-c", IMPORT_SNIPPET, str(database), str(source), str(rows)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    grew_midway = False
    earliest = 0
    try:
        deadline = time.time() + 180
        while process.poll() is None and time.time() < deadline:
            tables = list(Path(database).glob("**/*.apex"))
            if tables:
                size = max(table.stat().st_size for table in tables)
                if earliest == 0:
                    earliest = size
                elif size > earliest:
                    grew_midway = True
                    break
            time.sleep(0.05)
    finally:
        stdout, stderr = process.communicate(timeout=180)
    assert process.returncode == 0, (process.returncode, stdout, stderr)
    assert grew_midway, "the import must persist before it finishes"


CSV_SCAN_SNIPPET = """
import os
import resource
import sys

from apexbase import ApexClient

database, source = sys.argv[1], sys.argv[2]


def rss_mb():
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage / (1024 * 1024) if sys.platform == "darwin" else usage / 1024


client = ApexClient(database, drop_if_exists=True)
try:
    client.create_table("_dummy")
    client.use_table("_dummy")
    before = rss_mb()
    count = client.execute(
        f"SELECT COUNT(*) FROM read_csv('{source}') WHERE score >= 50"
    ).scalar()
    peak = rss_mb()
finally:
    client.close()
print(f"{count} {peak - before:.1f}")
"""


def test_csv_scan_window_returns_identical_aggregates(tmp_path, monkeypatch):
    """Streaming a CSV in bounded windows must not change any aggregate."""
    rows = 60_000
    source = tmp_path / "scan.csv"
    _write_csv(source, rows)
    client = ApexClient(str(tmp_path / "db"), drop_if_exists=True)
    client.create_table("_dummy")
    client.use_table("_dummy")
    queries = (
        f"SELECT COUNT(*) AS n FROM read_csv('{source}')",
        f"SELECT COUNT(*) AS n FROM read_csv('{source}') WHERE score >= 5",
        f"SELECT COUNT(*) AS n, SUM(score) AS s, AVG(score) AS a, MIN(score) AS mn, "
        f"MAX(score) AS mx FROM read_csv('{source}') WHERE score >= 5",
    )
    try:
        for query in queries:
            monkeypatch.setenv("APEX_CSV_WINDOW_MB", "0")
            whole = client.execute(query).to_dict()
            monkeypatch.setenv("APEX_CSV_WINDOW_MB", "1")
            windowed = client.execute(query).to_dict()
            assert whole == windowed, (query, whole, windowed)
    finally:
        client.close()


def test_large_csv_scan_stays_memory_bounded(tmp_path):
    """The out-of-core guard: scanning a CSV far larger than the cap must not pin it.

    Reading the whole file into one batch added +1397 MB RSS on a 643 MB CSV; the windowed
    scan adds tens of megabytes. The bound below is deliberately loose (the file is ~200 MB)
    so it fails only if the scan goes back to materialising the file.
    """
    rows = 3_000_000  # ~90 MB of CSV
    source = tmp_path / "big.csv"
    _write_csv(source, rows)
    assert source.stat().st_size > 60 * 1024 * 1024
    database = tmp_path / "bigdb"
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [p for p in [env.get("PYTHONPATH", ""), str(Path(__file__).resolve().parents[1] / "apexbase" / "python")] if p]
    )
    completed = subprocess.run(
        [sys.executable, "-c", CSV_SCAN_SNIPPET, str(database), str(source)],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    count, delta = completed.stdout.split()
    assert int(count) == sum(1 for i in range(rows) if (i % 1000) / 10.0 >= 50)
    assert float(delta) < 100.0, f"scan pinned the file: +{delta} MB RSS"
