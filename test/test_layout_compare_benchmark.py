"""Coverage for the flat-vs-chunked layout comparison harness.

The harness produces the decision material for the chunked-layout default
question, so its contract matters: it must build both layouts from identical
data, assert they answer every shape identically, interleave the measurements,
and refuse the chunked layout on a correlated fixture (the writer's 8% size
gate). These tests pin that contract on a small fixture.
"""

import importlib.util
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
def layout_bench():
    return _load("bench_layout_compare", ROOT / "benchmarks" / "bench_layout_compare.py")


def test_metric_names_are_well_formed(layout_bench):
    names = [name for name, _sql, _kind in layout_bench.SHAPES]
    assert len(names) == len(set(names))
    assert names[-1] != layout_bench.SPARSE_DELETE_SHAPE[0]
    for _name, sql, kind in layout_bench.SHAPES:
        assert "{table}" in sql
        assert kind in ("warm", "cold")


def test_rejects_invalid_arguments(layout_bench, tmp_path):
    for arguments in (
        ("--rows", "0"),
        ("--rounds", "0"),
        ("--widths", "0"),
        ("--widths", "17"),
        ("--widths", "4,x"),
    ):
        with pytest.raises(SystemExit):
            layout_bench.main([*arguments, "--output", str(tmp_path / "out.json")])


def test_independent_fixture_takes_the_chunked_layout(layout_bench):
    report = layout_bench.run_comparison(rows=20_000, widths=[4], rounds=1)
    width_result = report["results"][0]
    assert width_result["layout_taken"]["flat_encoding"] == 1
    assert width_result["layout_taken"]["chunk_encoding"] == 2, (
        "independent columns must pass the 8% size gate and take per-column frames"
    )
    assert all(width_result["correctness"].values())
    assert width_result["variants"]["chunk"]["bytes"] < (
        width_result["variants"]["flat"]["bytes"] * 1.08
    )
    # Every gated shape reports both layouts plus the gate verdict.
    expected = {name for name, _sql, _kind in layout_bench.SHAPES}
    expected.add(layout_bench.SPARSE_DELETE_SHAPE[0])
    assert set(width_result["shapes"]) == expected
    for shape in width_result["shapes"].values():
        assert shape["flat_ms"] > 0
        assert shape["chunk_ms"] > 0
        assert "would_flag_as_regression" in shape
        assert len(shape["flat"]["rounds"]) == 1
        assert len(shape["chunk"]["rounds"]) == 1
    summary = report["summary"] if "summary" in report else layout_bench._summary(report)
    assert summary["gate_regressions"] == len(summary["flagged"])


def test_correlated_fixture_is_refused_by_the_size_gate(layout_bench):
    report = layout_bench.run_comparison(
        rows=20_000, widths=[4], rounds=1, correlation=True
    )
    width_result = report["results"][0]
    # The writer must keep the correlated table flat even when the chunked
    # layout is requested: the per-column frames give up the cross-column
    # redundancy a single frame exploits.
    assert width_result["layout_taken"]["chunk_encoding"] == 1
    assert width_result["layout_taken"]["chunk_bytes_delta_pct"] == 0.0
    assert width_result["variants"]["flat"]["bytes"] == (
        width_result["variants"]["chunk"]["bytes"]
    )
    assert all(width_result["correctness"].values())


def test_width_slices_the_canonical_schema(layout_bench):
    schema = layout_bench.schema_for(4)
    assert len(schema) == 4
    assert list(schema) == list(layout_bench.SCHEMA)[:4]
