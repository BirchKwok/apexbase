"""Coverage for the chunked-layout guard phase.

The phase builds an independent-column zstd fixture (the wide-table phase's
correlated twin stays flat under the 8% size gate), asserts the per-column
frames were actually written, and reports one latency row per window shape so
the same-machine guard samples base/current. These tests pin the report
contract, the layout assertion that keeps the fixture honest, and the
correctness checks that refuse to report a fast wrong answer.
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
def chunked_bench():
    return _load(
        "bench_chunked_layout", ROOT / "benchmarks" / "bench_chunked_layout.py"
    )


def test_chunked_metric_names_are_well_formed(chunked_bench):
    names = chunked_bench.chunked_metric_names()
    assert len(names) == len(set(names))
    assert "Chunked: build (zstd fixture)" in names
    assert "Chunked/Default: build (zstd fixture)" in names
    for name, _sql in chunked_bench.SHAPES:
        assert f"Chunked: {name}" in names
        assert f"Chunked/Default: {name}" in names
    for name, _sql in chunked_bench.COLD_SHAPES:
        assert f"Chunked: {name}" in names
        assert f"Chunked/Default: {name}" in names
    # The flat twin is informational: it must never be gated.
    assert not any(name.startswith("Chunked/Flat:") for name in names)


def test_chunked_rejects_invalid_sizes(chunked_bench, tmp_path):
    for arguments in (("--rows", "0"), ("--warmup", "-1"), ("--iterations", "0")):
        with pytest.raises(SystemExit):
            chunked_bench.main([*arguments, "--output", str(tmp_path / "out.json")])


def test_chunked_rejects_unknown_only_metric(chunked_bench, tmp_path):
    with pytest.raises(SystemExit):
        chunked_bench.main(
            [
                "--rows", "100",
                "--output", str(tmp_path / "out.json"),
                "--only", "Chunked: does not exist",
            ]
        )


def test_chunked_fixture_is_asserted_and_reports_every_shape(chunked_bench):
    results, detail = chunked_bench.run_chunked_layout(rows=8_000, warmup=1, iterations=2)

    names = [row["query"] for row in results]
    assert names == chunked_bench.chunked_metric_names()
    assert len(names) == len(set(names))
    for row in results:
        assert row["category"] == "ApexBase chunked layout"
        assert row["ApexBase"] > 0

    chunked = detail["variants"]["chunked"]
    flat = detail["variants"]["flat"]
    default = detail["variants"]["default"]
    # The whole phase exists because this fixture keeps the layout: if the 8%
    # gate ever refuses it, the run must fail instead of silently sampling a
    # flat table under a chunked metric name.
    assert chunked["row_group_encoding"] == 2
    assert flat["row_group_encoding"] == 1
    assert chunked["compression"] == "zstd"
    assert chunked["bytes"] < flat["bytes"] * 1.10, "the 8% gate bounds the growth"
    # The default fixture follows the build's default: flat before the
    # round-273 flip, per-column frames afterwards; both must be compressed.
    assert default["row_group_encoding"] in (1, 2)
    assert default["compression"] == "zstd"
    assert "Chunked: build (zstd fixture)" in names
    assert "Chunked/Default: build (zstd fixture)" in names
    # The flat twin is recorded for both warm and cold shapes but is not part
    # of the gated rows; the default twin is gated and recorded too.
    for name, _sql in chunked_bench.SHAPES:
        assert f"Chunked/Flat: {name}" in detail["shapes"]
        assert f"Chunked: {name}" in detail["shapes"]
        assert f"Chunked/Default: {name}" in detail["shapes"]
    for name, _sql in chunked_bench.COLD_SHAPES:
        assert "cold_ms" in detail["shapes"][f"Chunked: {name}"]
        assert "cold_ms" in detail["shapes"][f"Chunked/Flat: {name}"]
        assert "cold_ms" in detail["shapes"][f"Chunked/Default: {name}"]
