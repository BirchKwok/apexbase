"""Coverage for the string-zone guard phase.

The phase quantifies the V5 string zone-map gain on the shapes the value and
tail ranges exist for: equal-width values that share their first eight bytes
(timestamps, UUIDs, prefixed ids), equal-width bucket keys, and a probe that
shares its first sixteen bytes with a stored value and therefore must stay
answerable. A build with only the legacy byte-length heuristic has to scan every
row group on those shapes, so the phase separates V4 from V5 inside the same
gate run.

These tests pin the report contract the guard depends on, the fixture shape, and
the correctness guard that refuses to report a fast wrong answer.
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
def zone_bench():
    return _load(
        "bench_string_zone_shapes", ROOT / "benchmarks" / "bench_string_zone_shapes.py"
    )


def _metric_names(results):
    return [row["query"] for row in results]


def test_zone_shapes_manifest_is_well_formed(zone_bench):
    names = [name for name, _sql, _rows in zone_bench.GATE_SHAPES]
    assert len(names) == len(set(names))
    control_names = [name for name, _sql, _rows in zone_bench.CONTROL_SHAPES]
    # A control must never be a gate metric: an unpruned full scan is a large,
    # noisy number and pinning it would turn sampling noise into a flag.
    assert not set(names) & set(control_names)
    # The gate set covers the tail range, the first-eight value range and the
    # shared lane that catches a loss of length pruning.
    assert "equal-width equality (first group)" in names
    assert "equal-width equality (last group)" in names
    assert "shared sixteen bytes equality" in names
    assert "varied-length equality" in names


def test_zone_shapes_fixture_covers_every_zone_shape(zone_bench):
    rows = 4_000
    data = zone_bench.generate_rows(rows)
    codes = data["code"]
    assert len(codes) == rows
    # Equal width and a shared first eight bytes: the length and prefix zones
    # are constant, so only the tail zone can prune.
    assert len({len(code) for code in codes}) == 1
    assert len({code[:8] for code in codes}) == 1
    # Bucket keys are equal width with a distinct prefix per bucket.
    buckets = data["bucket"]
    assert len({len(bucket) for bucket in buckets}) == 1
    assert len(set(buckets)) > 1
    assert len({bucket[:8] for bucket in buckets}) > 1
    # Varied length: the legacy length heuristic already prunes this control.
    assert len({len(name) for name in data["name"]}) > 1


def test_zone_shapes_reject_invalid_sizes(zone_bench, tmp_path):
    for arguments in (("--rows", "0"), ("--warmup", "-1"), ("--iterations", "0")):
        with pytest.raises(SystemExit):
            zone_bench.main([*arguments, "--output", str(tmp_path / "out.json")])


def test_zone_shapes_phase_emits_the_gate_metrics(zone_bench, tmp_path):
    results, detail = zone_bench.run_shapes(rows=8_000, warmup=1, iterations=2)

    names = _metric_names(results)
    for shape, _sql, _rows in zone_bench.GATE_SHAPES:
        assert f"Zone: {shape}" in names
    assert len(names) == len(set(names))
    # Controls stay in the informational block the comparator ignores.
    for shape, _sql, _rows in zone_bench.CONTROL_SHAPES:
        assert f"Zone: {shape}" not in names
        assert shape in detail["controls"]
    assert detail["rows"] == 8_000

    # Every gate metric is a millisecond latency and every shape records the row
    # count it returned, which is the correctness half of the contract.
    for row in results:
        assert row["ApexBase"] > 0
        shape = row["query"].removeprefix("Zone: ")
        assert detail["shapes"][shape]["rows"] >= 0

    report = {
        **zone_bench.full_bench.get_report_metadata("apexbase-canary"),
        "config": {"rows": 8_000, "warmup": 1, "iterations": 2},
        "results": results,
        "string_zone_shapes": detail,
    }
    json.dumps(report)
    assert report["suite"] == "apexbase-canary"


def test_zone_shapes_fail_on_a_wrong_row_count(zone_bench, monkeypatch):
    # A build that silently skips a matching row group must fail the benchmark
    # instead of reporting a fast wrong answer, so the guard is exercised with
    # an expectation the fixture cannot satisfy.
    monkeypatch.setattr(
        zone_bench,
        "GATE_SHAPES",
        (
            (
                "equal-width equality (first group)",
                "SELECT * FROM {table} WHERE code = '{first}'",
                99,
            ),
        ),
    )
    with pytest.raises(RuntimeError, match="expected 99 rows, got 1"):
        zone_bench.run_shapes(rows=8_000, warmup=1, iterations=1)


def test_zone_shapes_script_is_wired_into_the_full_guard(zone_bench):
    guard_source = (ROOT / "benchmarks" / "run_local_perf_guard.py").read_text(
        encoding="utf-8"
    )
    assert "bench_string_zone_shapes.py" in guard_source
    assert '"zs-comparison"' in guard_source
