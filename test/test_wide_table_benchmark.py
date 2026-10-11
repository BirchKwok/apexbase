"""Coverage for the wide-table guard phase.

The phase builds a 16-column table over several row groups plus a zstd-compressed
twin and reports one latency row per shape. These tests pin the report contract
the guard depends on, the multi-row-group fixture requirement, and the
correctness assertions that refuse to report a fast wrong answer (a projection
that drops a column, a compressed scan that returns zeros).
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
def wide_bench():
    return _load("bench_wide_table", ROOT / "benchmarks" / "bench_wide_table.py")


def test_wide_manifest_is_well_formed(wide_bench):
    assert wide_bench.COLUMN_COUNT == len(wide_bench.SCHEMA) == 16
    plain = [name for name, _sql in wide_bench.PLAIN_SHAPES]
    compressed = [name for name, _sql in wide_bench.COMPRESSED_SHAPES]
    assert len(plain) == len(set(plain))
    assert len(compressed) == len(set(compressed))
    informational = [name for name, _sql in wide_bench.INFORMATIONAL_SHAPES]
    assert set(compressed) <= set(plain) | set(informational), (
        "compressed shapes must reuse an existing shape"
    )
    assert not set(plain) & set(informational)
    # The executor-lane point reads are a separate shape list (they run on both
    # variants) and must not collide with the client-lane names.
    executor = [name for name, _sql in wide_bench.EXECUTOR_SHAPES]
    assert len(executor) == len(set(executor))
    assert not set(executor) & (set(plain) | set(informational) | set(compressed))
    # Cold-reopen shapes run on both variants and must not collide with the
    # warm names, which would make the report ambiguous about the measurement.
    cold = [name for name, _sql in wide_bench.COLD_SHAPES]
    assert len(cold) == len(set(cold))
    assert not set(cold) & (set(plain) | set(informational) | set(compressed) | set(executor))
    assert wide_bench.MIN_ROW_GROUPS >= 2
    # Wide rows are the point of the fixture: narrow tables are the canary's job.
    assert wide_bench.COLUMN_COUNT > 8


def test_wide_rejects_invalid_sizes(wide_bench, tmp_path):
    for arguments in (("--rows", "0"), ("--warmup", "-1"), ("--iterations", "0")):
        with pytest.raises(SystemExit):
            wide_bench.main([*arguments, "--output", str(tmp_path / "out.json")])


def test_wide_shapes_span_row_groups_and_gate(wide_bench):
    # Two variants times their own shape list, plus the executor-lane point
    # reads, which both variants run.
    results, detail = wide_bench.run_wide_table(rows=200_000, warmup=1, iterations=2)

    names = [row["query"] for row in results]
    assert len(names) == len(set(names))
    assert len(names) == (
        len(wide_bench.PLAIN_SHAPES)
        + len(wide_bench.COMPRESSED_SHAPES)
        + 2 * len(wide_bench.EXECUTOR_SHAPES)
        + 2 * len(wide_bench.COLD_SHAPES)
        + 2 * len(wide_bench.SPARSE_DELETE_SHAPES)
    )
    for name, _sql in wide_bench.INFORMATIONAL_SHAPES:
        assert f"Wide: {name}" not in names
        assert f"Wide: {name} (informational)" in detail["shapes"]
    for name, _sql in wide_bench.PLAIN_SHAPES:
        assert f"Wide: {name}" in names
    for name, _sql in wide_bench.COMPRESSED_SHAPES:
        assert f"Wide/Zstd: {name}" in names
    for name, _sql in wide_bench.EXECUTOR_SHAPES:
        assert f"Wide: {name}" in names
        assert f"Wide/Zstd: {name}" in names
    for name, _sql in wide_bench.COLD_SHAPES:
        assert f"Wide: {name}" in names
        assert f"Wide/Zstd: {name}" in names
        assert "cold_ms" in detail["shapes"][f"Wide/Zstd: {name}"]
    for row in results:
        assert row["category"] == "ApexBase wide table"
        assert row["ApexBase"] > 0

    # The fixture must actually span several row groups and compress.
    plain = detail["variants"]["plain"]
    zstd = detail["variants"]["zstd"]
    assert plain["row_groups"] >= wide_bench.MIN_ROW_GROUPS
    assert zstd["row_groups"] >= wide_bench.MIN_ROW_GROUPS
    assert zstd["bytes"] < plain["bytes"], "zstd must shrink the file"
    # Compressed groups are capped (a compressed group has no RCIX, so a touch
    # decodes the whole group); the plain twin keeps the larger adaptive groups.
    assert zstd["row_groups"] > plain["row_groups"], (
        "compressed row groups must be finer than the plain adaptive groups"
    )
    assert detail["columns"] == wide_bench.COLUMN_COUNT
    assert detail["rows"] == 200_000
    assert len(detail["shapes"]) == len(names) + len(wide_bench.INFORMATIONAL_SHAPES)


def test_wide_fixture_accepts_a_single_row_group_override(wide_bench):
    # The correctness assertions are independent of the row-group count, so a
    # small fixture can exercise them without paying for a multi-group build.
    results, detail = wide_bench.run_wide_table(
        rows=8_000, warmup=1, iterations=1, min_row_groups=1
    )
    assert len(results) == (
        len(wide_bench.PLAIN_SHAPES)
        + len(wide_bench.COMPRESSED_SHAPES)
        + 2 * len(wide_bench.EXECUTOR_SHAPES)
        + 2 * len(wide_bench.COLD_SHAPES)
        + 2 * len(wide_bench.SPARSE_DELETE_SHAPES)
    )
    assert detail["variants"]["plain"]["row_groups"] >= 1


def test_wide_report_round_trips_through_main(wide_bench, tmp_path):
    output = tmp_path / "wide.json"
    wide_bench.main(["--rows", "8000", "--warmup", "1", "--iterations", "1",
                     "--min-row-groups", "1", "--output", str(output)])
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["results"]
    assert report["wide_table"]["columns"] == 16
    assert report["config"]["rows"] == 8000
