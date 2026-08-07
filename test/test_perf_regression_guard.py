import importlib.util
import json
from pathlib import Path
import re

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _report(metrics, **config):
    return {
        "suite": "apexbase-canary",
        "config": {"rows": 1000, "warmup": 1, "iterations": 3, **config},
        "results": [
            {"category": "test", "query": name, "ApexBase": value}
            for name, value in metrics.items()
        ],
    }


@pytest.fixture(scope="module")
def guard():
    return _load(
        "compare_perf_baseline",
        ROOT / "benchmarks" / "compare_perf_baseline.py",
    )


@pytest.fixture(scope="module")
def benchmark():
    return _load(
        "bench_report_metadata",
        ROOT / "benchmarks" / "bench_vs_sqlite_duckdb.py",
    )


def test_comparison_passes_within_relative_threshold(guard):
    rows = guard.compare_reports(
        _report({"scan": 10.0}),
        _report({"scan": 10.9}),
        relative_threshold=0.10,
        absolute_threshold_ms=0.0,
    )

    assert rows == [{
        "query": "scan",
        "baseline_ms": 10.0,
        "current_ms": 10.9,
        "delta_ms": pytest.approx(0.9),
        "limit_ms": 11.0,
        "regressed": False,
    }]


def test_comparison_reports_regression(guard):
    rows = guard.compare_reports(
        _report({"scan": 10.0}),
        _report({"scan": 11.1}),
        relative_threshold=0.10,
        absolute_threshold_ms=0.0,
    )

    assert rows[0]["regressed"] is True


def test_absolute_tolerance_protects_microbenchmarks_from_noise(guard):
    rows = guard.compare_reports(
        _report({"point lookup": 0.002}),
        _report({"point lookup": 0.006}),
        relative_threshold=0.10,
        absolute_threshold_ms=0.005,
    )

    assert rows[0]["regressed"] is False


def test_qps_metrics_use_higher_is_better_semantics(guard):
    base = _report({"Q/s (single thread)": 10000.0, "scan": 10.0})

    dropped = guard.compare_reports(
        base,
        _report({"Q/s (single thread)": 8000.0, "scan": 10.0}),
        relative_threshold=0.15,
    )
    by_name = {row["query"]: row for row in dropped}
    assert by_name["Q/s (single thread)"]["regressed"] is True
    assert by_name["scan"]["regressed"] is False

    improved = guard.compare_reports(
        base,
        _report({"Q/s (single thread)": 12000.0, "scan": 10.0}),
        relative_threshold=0.15,
    )
    by_name = {row["query"]: row for row in improved}
    assert by_name["Q/s (single thread)"]["regressed"] is False

    within_noise = guard.compare_reports(
        base,
        _report({"Q/s (single thread)": 9000.0, "scan": 10.0}),
        relative_threshold=0.15,
    )
    by_name = {row["query"]: row for row in within_noise}
    assert by_name["Q/s (single thread)"]["regressed"] is False


def test_missing_metric_is_an_error(guard):
    with pytest.raises(guard.ReportError, match="missing metrics"):
        guard.compare_reports(
            _report({"scan": 10.0, "insert": 5.0}),
            _report({"scan": 10.0}),
        )


def test_symmetric_samples_are_aggregated_by_median(guard):
    baseline = guard.aggregate_report_metrics([
        _report({"scan": 10.0}),
        _report({"scan": 12.0}),
        _report({"scan": 100.0}),
    ])
    current = guard.aggregate_report_metrics([
        _report({"scan": 11.5}),
        _report({"scan": 12.0}),
        _report({"scan": 50.0}),
    ])

    rows = guard.compare_metric_sets(
        baseline,
        current,
        relative_threshold=0.0,
        absolute_threshold_ms=0.0,
    )

    assert baseline == {"scan": 12.0}
    assert current == {"scan": 12.0}
    assert rows[0]["regressed"] is False


def test_sample_metric_mismatch_is_an_error(guard):
    with pytest.raises(guard.ReportError, match="sample 2 metric set differs: missing insert"):
        guard.aggregate_report_metrics([
            _report({"scan": 10.0, "insert": 5.0}),
            _report({"scan": 10.0}),
        ])


def test_incompatible_config_is_reported(guard):
    errors = guard.compatibility_errors(
        _report({"scan": 10.0}),
        _report({"scan": 10.0}, rows=2000),
    )

    assert errors == ["config.rows differs"]


def test_system_match_rejects_dependency_or_build_drift(guard):
    baseline = _report({"scan": 10.0})
    baseline.update({
        "system": {"machine": "arm64"},
        "dependencies": {"numpy": "2.1.3"},
        "build": {"rustc": "1.88.0"},
    })
    current = _report({"scan": 10.0})
    current.update({
        "system": {"machine": "arm64"},
        "dependencies": {"numpy": "2.4.3"},
        "build": {"rustc": "1.89.0"},
    })

    errors = guard.compatibility_errors(
        baseline,
        current,
        require_system_match=True,
    )

    assert errors == ["dependencies differs", "build differs"]


def test_vector_metrics_are_included(guard):
    report = _report({"scan": 10.0})
    report["vector_similarity"] = {
        "head_to_head": [{"query": "TopK L2", "ApexBase": 7.25}],
        "batch": [{"query": "Batch TopK L2", "ApexBase": 42.5}],
    }

    assert guard.extract_apex_metrics(report) == {
        "scan": 10.0,
        "TopK L2": 7.25,
        "Batch TopK L2": 42.5,
    }


def test_canary_manifest_references_real_benchmark_methods():
    canary = _load(
        "bench_perf_canary",
        ROOT / "benchmarks" / "bench_perf_canary.py",
    )

    methods = {spec[1] for spec in canary.CANARY_SPECS}
    available = set(dir(canary.full_bench.ApexBaseBench))
    assert methods <= available
    assert len(canary.CANARY_SPECS) == len({spec[0] for spec in canary.CANARY_SPECS})


def test_full_benchmark_json_keeps_microsecond_precision(capsys):
    canary = _load(
        "bench_perf_canary_precision",
        ROOT / "benchmarks" / "bench_perf_canary.py",
    )
    spec = [("micro", "unused", False, False, False, None)]

    rows = canary.full_bench.print_benchmark_section(
        "test",
        "test",
        spec,
        {"micro": {"ApexBase": 0.0004214}},
        ["ApexBase"],
        16,
    )

    capsys.readouterr()
    assert rows[0]["ApexBase"] == 0.000421


def test_benchmark_report_metadata_is_complete_and_serializable(benchmark):
    metadata = benchmark.get_report_metadata("test-suite")

    assert metadata["format_version"] == 1
    assert metadata["suite"] == "test-suite"
    assert re.fullmatch(r"[0-9a-f]{40}", metadata["git"]["commit"])
    assert set(metadata["git"]) == {"commit", "branch", "dirty"}
    assert {
        "platform", "machine", "processor", "cpu_count", "memory_gb", "python"
    } <= metadata["system"].keys()
    assert set(metadata["dependencies"]) == {
        "apexbase", "sqlite", "duckdb", "pyarrow", "numpy", "pandas", "polars"
    }
    assert set(metadata["build"]) == {"maturin", "rustc", "cargo"}
    assert all(isinstance(value, str) and value for value in metadata["dependencies"].values())
    assert all(isinstance(value, str) and value for value in metadata["build"].values())
    json.dumps(metadata)


def test_benchmark_git_metadata_honors_ci_source_override(benchmark, monkeypatch):
    commit = "a" * 40
    monkeypatch.setenv("APEXBASE_BENCHMARK_COMMIT", commit)
    monkeypatch.setenv("APEXBASE_BENCHMARK_BRANCH", "pull-request-base")

    git = benchmark.get_git_info()

    assert git["commit"] == commit
    assert git["branch"] == "pull-request-base"


def test_performance_workflow_is_not_used_as_an_acceptance_gate():
    assert not (ROOT / ".github" / "workflows" / "performance.yml").exists()


def test_github_workflows_do_not_run_local_performance_benchmarks():
    workflows = ROOT / ".github" / "workflows"
    sources = "\n".join(path.read_text() for path in workflows.glob("*.yml"))

    assert "bench_perf_canary.py" not in sources
    assert "run_local_perf_guard.py" not in sources


def test_submillisecond_sql_metrics_use_calibrated_median_timing(benchmark):
    specs = {name: spec for name, *spec in benchmark.BENCHMARKS}

    for name, method in (
        ("COUNT WHERE category", "bench_count_where_category"),
        ("Point Lookup (SQL by ID)", "bench_point_lookup"),
    ):
        assert specs[name][1:4] == [False, False, True]
        assert specs[name][4] is None
        assert method in benchmark.MICRO_MEDIAN_BENCHMARK_METHODS


def test_setup_benchmark_uses_median_to_reject_outlier(benchmark, monkeypatch):
    timestamps = iter((0.0, 0.001, 1.0, 1.002, 2.0, 2.100))
    monkeypatch.setattr(benchmark.time, "perf_counter", lambda: next(timestamps))

    elapsed_ms = benchmark.run_bench_with_setup(
        lambda: None,
        lambda: None,
        warmup=0,
        iterations=3,
    )

    assert elapsed_ms == pytest.approx(2.0)


def test_cold_microbenchmark_calibrates_repeats_without_timing_setup(
    benchmark, monkeypatch
):
    timestamps = iter(range(18))
    setup_calls = []
    bench_calls = []
    monkeypatch.setattr(benchmark, "MICROBENCH_CALIBRATION_TRIALS", 1)
    monkeypatch.setattr(benchmark, "MICROBENCH_TARGET_SAMPLE_NS", 4)
    monkeypatch.setattr(benchmark, "MICROBENCH_MAX_REPEATS", 4)
    monkeypatch.setattr(benchmark.time, "perf_counter_ns", lambda: next(timestamps))

    elapsed_ms = benchmark.run_bench_cold_nogc(
        lambda: setup_calls.append(None),
        lambda: bench_calls.append(None),
        warmup=0,
        iterations=2,
    )

    assert elapsed_ms == pytest.approx(0.000001)
    assert len(setup_calls) == 9
    assert len(bench_calls) == 9


def test_table_ops_metrics_leave_qps_dataset_table_selected(benchmark, tmp_path):
    """Table-operation metrics must restore the dataset table afterwards.

    The extended benchmark runs the OLAP Q/s harness after the table-ops
    metrics. Its ``SELECT ... FROM default`` queries resolve to the currently
    selected table, so when the last table-ops metric left ``bench_table_ops``
    selected, the Q/s GROUP BY query failed with "Projected column 'city' does
    not exist". Each table-ops metric must leave ``default`` selected again.
    """
    pytest.importorskip("apexbase")

    data = benchmark.generate_data(2000)
    bench = benchmark.ApexBaseBench(str(tmp_path), data)
    bench.setup()
    bench.bench_insert()
    assert bench.client.current_table == "default"

    table_ops = [
        ("bench_table_create", "bench_table_create_setup"),
        ("bench_table_drop", "bench_table_drop_setup"),
        ("bench_table_create_drop_cycle", "bench_table_create_setup"),
        ("bench_list_tables", "bench_list_tables_setup"),
        ("bench_alter_table_add_column", "bench_alter_table_add_column_setup"),
    ]
    for method_name, setup_name in table_ops:
        benchmark.run_bench_with_setup(
            getattr(bench, setup_name),
            getattr(bench, method_name),
            warmup=1,
            iterations=1,
        )
        assert bench.client.current_table == "default", method_name

    # Regression for the Q/s failure: `FROM default` must target the dataset
    # table after the full table-ops sequence.
    result = bench.execute_materialized_query(
        "SELECT city, COUNT(*) FROM default GROUP BY city"
    )
    assert len(result) > 0
    assert {"city", "COUNT(*)"} <= set(result[0].keys())
    bench.client.close()


def test_qps_read_profile_measures_clean_loaded_table(benchmark, tmp_path):
    """The Q/s harness must not measure the post-DML delta-heavy state.

    The calibrated single-row DML microbenchmarks leave the Apex dataset table
    with thousands of pending writes and update tombstones, slowing the
    read-only scan queries used by the Q/s profile by orders of magnitude.
    ``reload_loaded_state`` (which the harness calls before pre-warming) must
    restore the steady-state read path.
    """
    pytest.importorskip("apexbase")
    import time

    data = benchmark.generate_data(2000)
    bench = benchmark.ApexBaseBench(str(tmp_path), data)
    bench.setup()
    bench.bench_insert()

    query = "SELECT * FROM default WHERE age > 30 LIMIT 100"

    def median_ms():
        bench.execute_materialized_query(query)
        samples = []
        for _ in range(20):
            start = time.perf_counter()
            bench.execute_materialized_query(query)
            samples.append((time.perf_counter() - start) * 1000)
        samples.sort()
        return samples[len(samples) // 2]

    baseline_ms = median_ms()

    # Reproduce the DML contamination from the OLTP microbenchmark section.
    benchmark.run_bench_nogc_median(bench.bench_oltp_insert_one, warmup=1, iterations=1)
    benchmark.run_bench_nogc_median(bench.bench_oltp_insert_read_own_row, warmup=1, iterations=1)
    benchmark.run_bench_nogc_median(bench.bench_oltp_insert_count_visible, warmup=1, iterations=1)
    benchmark.run_bench_nogc_median(bench.bench_oltp_update_by_id, warmup=1, iterations=1)
    poisoned_ms = median_ms()

    # The contaminated scan is orders of magnitude slower than the baseline.
    assert poisoned_ms > max(10 * baseline_ms, 0.5)

    # Recreating the engine on a fresh loaded copy restores the fast read path.
    benchmark.reload_loaded_state([("ApexBase", bench)])
    recovered_ms = median_ms()
    assert recovered_ms < max(5 * baseline_ms, 0.5)
    bench.client.close()


def test_canary_with_qps_does_not_corrupt_recreated_table(benchmark):
    """The canary's Q/s reset must not corrupt a table recreated in-place.

    After the canary's DELETE microbenchmark, the Q/s section recreates the
    engine on a fresh loaded copy at the same path. Deferred delete state from
    the old file used to be applied to the new file, failing with "Corrupt Apex
    file: header/footer row counts differ" and aborting the canary.
    """
    pytest.importorskip("apexbase")

    canary = _load(
        "bench_perf_canary",
        ROOT / "benchmarks" / "bench_perf_canary.py",
    )
    results = canary.run_canary(rows=2000, warmup=1, iterations=1)
    qps = [row for row in results if row["category"] == "ApexBase Q/s"]
    assert len(qps) == 2
    assert all(row["ApexBase"] > 0 for row in qps)
    assert {"Q/s (single thread)", "Q/s (4 threads)"} == {
        row["query"] for row in qps
    }
