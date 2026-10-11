"""Coverage for the width-fidelity guard phase (V4 layout vs V5 narrow).

The phase is what quantifies the V5 storage gain inside the same-machine
guard: it writes one dataset twice — once in the V4 full-width layout, once in
the V5 narrow layout — and reports bytes on disk, warm latency, and first-touch
page faults for both. These tests pin the report contract the guard depends on
(metric names, envelope keys, comparator readability) and the gain itself.
"""

import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def width_bench():
    return _load("bench_width_fidelity", ROOT / "benchmarks" / "bench_width_fidelity.py")


def _metric_names(results):
    return [row["query"] for row in results]


def test_width_fidelity_manifest_is_well_formed(width_bench):
    names = [name for name, _sql in width_bench.SHAPES]
    assert len(names) == len(set(names))
    labels = [label for label, _env, _version in width_bench.LAYOUTS]
    assert labels == ["V4 layout", "V5 narrow"]
    assert width_bench.f32(0.1) == width_bench.struct.unpack(
        "f", width_bench.struct.pack("f", 0.1)
    )[0]


def test_width_fidelity_rejects_invalid_sizes(width_bench, tmp_path):
    for arguments in (("--rows", "0"), ("--warmup", "-1"), ("--iterations", "0")):
        with pytest.raises(SystemExit):
            width_bench.main([*arguments, "--output", str(tmp_path / "out.json")])


def test_width_fidelity_phase_measures_both_layouts(width_bench, tmp_path):
    results, detail = width_bench.run_width_fidelity(rows=300, warmup=1, iterations=2)

    # Both layouts must declare their version and the narrow encoding must be
    # real: this build stores the Float32 column at four bytes per value.
    assert detail["version"]["V4 layout"] == 4
    assert detail["version"]["V5 narrow"] == 5
    assert detail["narrow_supported"] is True
    assert detail["table_bytes"]["V5 narrow"] < detail["table_bytes"]["V4 layout"]

    names = _metric_names(results)
    for shape, _sql in width_bench.SHAPES:
        for label in ("V4 layout", "V5 narrow"):
            assert f"Width: {shape} ({label})" in names
    for label in ("V4 layout", "V5 narrow"):
        assert f"Width: bytes on disk ({label})" in names
    assert len(names) == len(set(names))

    # Page-fault counts are reported for humans but do not gate: they vary by
    # process (the same table reported 0-142 pages across guard samples).
    assert set(detail["pages_metric"]) == {
        f"{shape} ({label})"
        for shape, _sql in width_bench.SHAPES
        for label in ("V4 layout", "V5 narrow")
    }
    assert not any(name.endswith("pages (V4 layout)") for name in names)

    # The gain summary covers every shape and is a plain ratio.
    assert set(detail["gain"]) == {shape for shape, _sql in width_bench.SHAPES}
    for values in detail["gain"].values():
        assert values["warm_ratio"] > 0
        assert values["first_touch_ratio"] > 0
        # At tiny scale a layout can touch zero pages (the ratio is undefined
        # when the wide side is zero, and a genuine zero when the narrow side
        # already had every page resident). Neither may ever be negative.
        assert values["pages_ratio"] is None or values["pages_ratio"] >= 0
    assert detail["bytes_ratio"] < 1.0

    report = {
        **width_bench.full_bench.get_report_metadata("apexbase-canary"),
        "config": {"rows": 300, "warmup": 1, "iterations": 2},
        "results": results,
        "width_fidelity": detail,
    }
    json.dumps(report)
    assert report["suite"] == "apexbase-canary"
    assert report["config"]["rows"] == 300


def test_width_fidelity_metrics_are_enforced_by_the_comparator(width_bench, tmp_path):
    guard = _load("compare_perf_baseline", ROOT / "benchmarks" / "compare_perf_baseline.py")

    def report(value):
        return {
            "suite": "apexbase-canary",
            "config": {"rows": 300, "warmup": 1, "iterations": 2},
            "results": [
                {
                    "category": "ApexBase width fidelity",
                    "query": "Width: Float32 projection (V5 narrow)",
                    "ApexBase": value,
                },
                {
                    "category": "ApexBase width fidelity pages",
                    "query": "Width: Float32 projection pages (V5 narrow)",
                    "ApexBase": 12,
                },
            ],
        }

    metrics = guard.extract_apex_metrics(report(2.0))
    assert metrics["Width: Float32 projection (V5 narrow)"] == 2.0

    comparisons = guard.compare_reports(report(2.0), report(2.4))
    regressed = {row["query"] for row in comparisons if row["regressed"]}
    assert regressed == {"Width: Float32 projection (V5 narrow)"}

    comparisons = guard.compare_reports(report(2.0), report(2.1))
    assert not any(row["regressed"] for row in comparisons)


def test_width_fidelity_script_is_wired_into_the_full_guard(width_bench):
    guard_source = (ROOT / "benchmarks" / "run_local_perf_guard.py").read_text(
        encoding="utf-8"
    )
    assert "bench_width_fidelity.py" in guard_source
    assert '"wf-comparison"' in guard_source
