"""S13: the 10M scale harness is repeatable and reports the required fields."""

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "benchmarks" / "bench_scale_10m.py"


def test_scale_harness_runs_and_reports_contract(tmp_path):
    output = tmp_path / "scale.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--rows",
            "50000",
            "--budget-mb",
            "256",
            "--skip-vector",
            "--skip-competitors",
            "--db-dir",
            str(tmp_path / "db"),
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert completed.returncode == 0, completed.stderr
    report = json.loads(output.read_text())
    assert report["suite"] == "scale_10m"
    assert report["rows"] == 50_000
    assert report["budget_mb"] == 256
    assert report["fact_build_seconds"] > 0
    apex = report["apex"]
    for name in (
        "count",
        "group_by_k_1000",
        "group_by_id_high_cardinality",
        "order_by_v_limit_100",
        "join_dim_count",
    ):
        assert name in apex, name
        assert apex[name]["ms"] >= 0
        assert "spill_files" in apex[name] and "peak_rss_mb" in apex[name]
    assert report["peak_rss_mb"] > 0
    assert report["duration_seconds"] > 0


def test_scale_harness_rejects_invalid_sizes(tmp_path):
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--rows",
            "0",
            "--skip-vector",
            "--skip-competitors",
            "--output",
            str(tmp_path / "unused.json"),
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert completed.returncode != 0
