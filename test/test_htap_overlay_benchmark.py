"""Coverage for the HTAP overlay guard phase.

The phase measures filtered fused aggregations while 1% of buffered appended rows
sit on top of the persisted base. Before the append-aware fused lane, any overlay
made the lane stand down and the query fell back to a full merged
materialization (a 7-8x amplification); now the lane folds the appended rows
itself. These tests pin the report contract the guard depends on and the
correctness assertion that refuses to report a fast wrong answer.
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
def htap_bench():
    return _load("bench_htap_overlay", ROOT / "benchmarks" / "bench_htap_overlay.py")


def test_htap_manifest_is_well_formed(htap_bench):
    names = [name for name, _sql in htap_bench.GATE_SHAPES]
    assert len(names) == len(set(names))
    control_names = [name for name, _sql in htap_bench.CONTROL_SHAPES]
    assert not set(names) & set(control_names)
    # Every gate predicate must match appended rows, otherwise the overlay work
    # the phase exists to measure would not happen.
    assert len(htap_bench.GATE_SHAPES) == 2
    assert htap_bench.APPEND_FRACTION == 0.01


def test_htap_rejects_invalid_sizes(htap_bench, tmp_path):
    for arguments in (("--rows", "0"), ("--warmup", "-1"), ("--iterations", "0")):
        with pytest.raises(SystemExit):
            htap_bench.main([*arguments, "--output", str(tmp_path / "out.json")])


def test_htap_shapes_measure_the_overlay_lane(htap_bench):
    results, detail = htap_bench.run_htap(rows=8_000, warmup=1, iterations=2)

    names = [row["query"] for row in results]
    for shape, _sql in htap_bench.GATE_SHAPES:
        assert f"HTAP: {shape} (+1% appended)" in names
    assert len(names) == len(set(names))
    # Informational rows never gate.
    for shape, _sql in htap_bench.CONTROL_SHAPES:
        assert shape not in names
    assert detail["appended_rows"] == 80
    assert detail["rows"] == 8_000

    for row in results:
        assert row["ApexBase"] > 0
    # The appended-overlay gate shapes report their amplification against the
    # clean control; the deletion-vector shapes are a prefix read and carry the
    # row count they must return instead.
    for shape, _sql in htap_bench.GATE_SHAPES:
        assert detail["shapes"][shape]["amplification"] >= 0
    for shape, _sql, expected_rows in htap_bench.DELETION_SHAPES:
        assert f"HTAP: {shape}" in names
        assert detail["shapes"][shape]["warm_ms"] > 0
        # `None` marks the aggregate shape, which has no row count to pin.
        assert expected_rows in (0, 100, None)
    # The pending-overlay shapes are zero-row by construction: they pin that a
    # schema probe and an absent-id lookup stay on the bounded lanes.
    # The range-delete aggregate measures the last fixture state.
    assert "HTAP: aggregate after a range delete" in names
    assert detail["shapes"]["aggregate after a range delete"]["warm_ms"] > 0
    for shape, _sql, expected_rows in htap_bench.PENDING_SHAPES:
        assert f"HTAP: {shape}" in names
        assert detail["shapes"][shape]["warm_ms"] > 0
        assert expected_rows in (0, 100)

    report = {
        **htap_bench.full_bench.get_report_metadata("apexbase-canary"),
        "config": {"rows": 8_000, "warmup": 1, "iterations": 2},
        "results": results,
        "htap_overlay": detail,
    }
    json.dumps(report)
    assert report["suite"] == "apexbase-canary"


def test_htap_fails_when_the_appended_rows_are_skipped(htap_bench, monkeypatch):
    # The correctness guard is what stops a lane that silently ignores the
    # overlay from reporting a fast wrong answer: with an expectation the fixture
    # cannot satisfy, `run_htap` must fail instead of emitting a metric.
    original = htap_bench.oracle_for

    def wrong_oracle(sql_template):
        row_predicate, value_predicate = original(sql_template)
        return (lambda a, b: False, value_predicate)

    monkeypatch.setattr(htap_bench, "oracle_for", wrong_oracle)
    with pytest.raises(AssertionError):
        htap_bench.run_htap(rows=8_000, warmup=1, iterations=1)


def test_htap_script_is_wired_into_the_full_guard(htap_bench):
    guard_source = (ROOT / "benchmarks" / "run_local_perf_guard.py").read_text(
        encoding="utf-8"
    )
    assert "bench_htap_overlay.py" in guard_source
    assert '"htap-comparison"' in guard_source
