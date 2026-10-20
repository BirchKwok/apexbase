import importlib.util
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _load_guard():
    path = ROOT / "benchmarks" / "run_local_perf_guard.py"
    spec = importlib.util.spec_from_file_location("run_local_perf_guard", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def local_guard():
    return _load_guard()


def test_local_guard_uses_balanced_three_sample_order(local_guard):
    assert local_guard.SAMPLE_ORDER == (
        "base", "current", "current", "base", "base", "current"
    )
    assert local_guard.SAMPLE_ORDER.count("base") == 3
    assert local_guard.SAMPLE_ORDER.count("current") == 3


def test_local_guard_confirmation_extends_to_five_samples_per_side(local_guard):
    complete_order = local_guard.SAMPLE_ORDER + local_guard.CONFIRMATION_SAMPLE_ORDER

    assert local_guard.CONFIRMATION_SAMPLE_ORDER == (
        "current", "base", "base", "current"
    )
    assert complete_order.count("base") == 5
    assert complete_order.count("current") == 5


def test_local_guard_comparison_uses_every_sample(local_guard):
    class Args:
        relative_threshold = 0.15
        absolute_threshold_ms = 0.005

    reports = {
        "base": [Path("base-1.json"), Path("base-2.json"), Path("base-3.json")],
        "current": [
            Path("current-1.json"), Path("current-2.json"), Path("current-3.json")
        ],
    }

    command = local_guard.comparison_arguments(Path("python"), reports, Args())

    assert command.count("--baseline-sample") == 2
    assert command.count("--current-sample") == 2
    assert command[-1] == "--require-system-match"


@pytest.mark.parametrize(
    ("mode", "expected_script", "expected_values"),
    (
        ("canary", "bench_perf_canary.py", ("200000", "2", "7")),
        ("full", "bench_vs_sqlite_duckdb.py", ("1000000", "2", "5")),
    ),
)
def test_local_guard_benchmark_defaults(
    local_guard, mode, expected_script, expected_values
):
    command = local_guard.benchmark_arguments(mode, None, None, None, "report.json")

    assert Path(command[0]).name == expected_script
    assert tuple(command[command.index(flag) + 1] for flag in (
        "--rows", "--warmup", "--iterations"
    )) == expected_values
    assert command[-2:] == ("--output", "report.json")


def test_local_guard_accepts_explicit_benchmark_sizes(local_guard):
    command = local_guard.benchmark_arguments("canary", 1234, 4, 9, "report.json")

    assert (command[2], command[4], command[6]) == ("1234", "4", "9")


def test_local_guard_qps_only_uses_canary_script(local_guard):
    command = local_guard.benchmark_arguments(
        "full", None, None, None, "report.json", qps_only=True
    )

    assert Path(command[0]).name == "bench_perf_canary.py"
    assert "--qps-only" in command
    assert command[-2:] == ("--output", "report.json")


def test_local_guard_quant_only_uses_canary_script(local_guard):
    command = local_guard.benchmark_arguments(
        "full", None, None, None, "report.json", quant_only=True
    )

    assert Path(command[0]).name == "bench_perf_canary.py"
    assert "--quant-only" in command
    assert command[-2:] == ("--output", "report.json")
    assert local_guard.FULL_QUANT_ROWS == 1_000_000


def test_local_guard_full_mode_keeps_public_benchmark(local_guard):
    command = local_guard.benchmark_arguments("full", None, None, None, "report.json")

    assert Path(command[0]).name == "bench_vs_sqlite_duckdb.py"
    assert "--qps-only" not in command
    assert "--quant-only" not in command
    assert "--skip-quantized-vector" in command


@pytest.mark.parametrize(
    "arguments",
    (
        ("--rows", "0"),
        ("--warmup", "-1"),
        ("--iterations", "0"),
        ("--relative-threshold", "-0.1"),
        ("--absolute-threshold-ms", "-0.1"),
        ("--settle-seconds", "-1"),
    ),
)
def test_local_guard_rejects_invalid_numeric_options(local_guard, arguments):
    with pytest.raises(SystemExit):
        local_guard.parse_args(arguments)


def test_local_guard_requires_exactly_one_built_wheel(local_guard, tmp_path):
    with pytest.raises(RuntimeError, match="expected one wheel"):
        local_guard.one_wheel(tmp_path)

    wheel = tmp_path / "apexbase.whl"
    wheel.touch()
    assert local_guard.one_wheel(tmp_path) == wheel


def test_local_guard_copies_untracked_workspace_files(
    local_guard, tmp_path, monkeypatch
):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "new.py").write_text("value = 1\n", encoding="utf-8")
    outputs = iter(("", "new.py\0"))

    class Result:
        def __init__(self, stdout):
            self.stdout = stdout

    monkeypatch.setattr(local_guard, "ROOT", source)
    monkeypatch.setattr(
        local_guard, "run", lambda *args, **kwargs: Result(next(outputs))
    )

    local_guard.copy_workspace_changes(destination)

    assert (destination / "new.py").read_text(encoding="utf-8") == "value = 1\n"


def test_local_guard_keeps_release_build_and_system_match(local_guard):
    source = Path(local_guard.__file__).read_text(encoding="utf-8")

    assert '"maturin", "build", "--release"' in source
    assert '"--require-system-match"' in source
    assert 'target_dirs[side]' in source
    assert 'shutil.copy2(base_tree / "Cargo.lock", current_tree / "Cargo.lock")' in source
    assert '"APEXBASE_BENCHMARK_DIRTY": "0"' in source


def test_benchmark_git_metadata_accepts_dirty_override(monkeypatch):
    path = ROOT / "benchmarks" / "bench_vs_sqlite_duckdb.py"
    spec = importlib.util.spec_from_file_location("bench_dirty_override", path)
    benchmark = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(benchmark)
    monkeypatch.setenv("APEXBASE_BENCHMARK_DIRTY", "0")

    assert benchmark.get_git_info()["dirty"] is False

    monkeypatch.setenv("APEXBASE_BENCHMARK_DIRTY", "1")
    assert benchmark.get_git_info()["dirty"] is True


def test_commit_canary_runs_real_wal_commit_and_reopen():
    path = ROOT / "benchmarks" / "bench_perf_canary.py"
    spec = importlib.util.spec_from_file_location("commit_canary", path)
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)
    results = benchmark.run_commit_canary(50, 1, 2)
    assert len(results) == 1
    assert results[0]["query"] == "Rust Safe TXN INSERT 10 + COMMIT"
    assert results[0]["ApexBase"] > 0


def test_guard_evidence_records_real_dirty_tree(local_guard, tmp_path):
    tree = tmp_path / "tree"
    tree.mkdir()
    def git(*args):
        return subprocess.run(
            ("git", *args), cwd=tree, check=True, capture_output=True,
        )

    git("init")
    (tree / "tracked.py").write_bytes(b"before\n")
    (tree / "deleted.py").write_bytes(b"removed\n")
    (tree / ".gitignore").write_text("Cargo.lock\nignored\n")
    git("add", ".")
    git("-c", "user.name=Evidence Test", "-c", "user.email=test@example.invalid",
        "commit", "-m", "base")
    # Bytes, not text: text mode translates "\n" to "\r\n" on Windows and the
    # inventory must hash the same content on every platform.
    (tree / "tracked.py").write_bytes(b"after\n")
    (tree / "deleted.py").unlink()
    (tree / "new.bin").write_bytes(b"\x00\xff\x01")
    (tree / "ignored").write_text("not an input\n")
    (tree / "Cargo.lock").write_text("lock input\n")
    (tree / "link").symlink_to("tracked.py")
    (tree / "tracked.py").chmod(0o755)
    reports = tmp_path / "reports"
    reports.mkdir()
    evidence = local_guard.GateEvidence(reports, {"base_commit": "test"})
    evidence.source("current", tree)
    evidence.lockfile("current", tree)
    inventory = json.loads((reports / "source-current.json").read_text())
    assert inventory["tracked.py"] == {
        "sha256": hashlib.sha256(b"after\n").hexdigest(),
        # NTFS has no execute bit for chmod to set.
        "executable": sys.platform != "win32",
    }
    assert inventory["new.bin"]["sha256"] == hashlib.sha256(b"\x00\xff\x01").hexdigest()
    assert inventory["link"] == {"symlink": "tracked.py"}
    assert "deleted.py" not in inventory
    assert "ignored" not in inventory
    assert (reports / "Cargo-current.lock").read_bytes() == (tree / "Cargo.lock").read_bytes()
    patch = (reports / "source-current.patch").read_text()
    assert "-before" in patch and "+after" in patch and "deleted.py" in patch
    first = evidence.data["sources"]["current"]["inventory_sha256"]
    evidence.source("current", tree)
    assert evidence.data["sources"]["current"]["inventory_sha256"] == first
    (tree / "new.bin").write_bytes(b"different")
    evidence.source("current", tree)
    assert evidence.data["sources"]["current"]["inventory_sha256"] != first


@pytest.mark.parametrize("outcome,status", [
    (0, "passed"), (1, "regressed"), (2, "error"), (130, "interrupted"),
])
def test_guard_main_persists_outcomes(local_guard, tmp_path, monkeypatch, outcome, status):
    # Exercise CLI state/error handling without running an expensive build;
    # source provenance above uses real Git and the real filesystem.
    monkeypatch.setattr(local_guard.shutil, "which", lambda _: sys.executable)
    def execute(args, directory, evidence):
        initial = json.loads((directory / "run-manifest.json").read_text())
        assert initial["status"] == "running" and initial["exit_code"] is None
        evidence.comparison("comparison-initial", 1)
        if outcome == 130:
            raise KeyboardInterrupt
        if outcome == 2:
            raise subprocess.CalledProcessError(7, ["failed-build"])
        evidence.comparison("comparison", outcome)
        return outcome
    monkeypatch.setattr(local_guard, "execute_gate", execute)
    directory = tmp_path / "reports"
    assert local_guard.main(["--base-ref", "HEAD", "--output-dir", str(directory)]) == outcome
    report = json.loads((directory / "run-manifest.json").read_text())
    assert report["status"] == status and report["exit_code"] == outcome
    assert report["comparisons"]["comparison-initial"] == 1
    assert report["finished_at"] >= report["started_at"]
    assert report["base_commit"] == report["current_commit"]
    assert report["arguments"]["settle_seconds"] == 30
    if outcome == 2:
        assert report["failed_command_exit_code"] == 7
        assert "failed-build" in report["error"]
    assert not (directory / "run-manifest.json.tmp").exists()


def test_guard_evidence_wheel_and_incomplete_state(local_guard, tmp_path):
    evidence = local_guard.GateEvidence(tmp_path, {})
    wheel = tmp_path / "example.whl"
    wheel.write_bytes(b"wheel artifact")
    evidence.wheel("current", wheel)
    report = json.loads((tmp_path / "run-manifest.json").read_text())
    assert report["status"] == "running" and report["exit_code"] is None
    assert report["wheels"]["current"] == {
        "name": wheel.name, "sha256": hashlib.sha256(b"wheel artifact").hexdigest(),
    }


def test_guard_evidence_keeps_incompatible_report_status(local_guard, tmp_path):
    evidence = local_guard.GateEvidence(tmp_path, {})
    evidence.comparison("comparison", 2)
    evidence.finish(2)
    report = json.loads((tmp_path / "run-manifest.json").read_text())
    assert report["comparisons"] == {"comparison": 2}
    assert report["status"] == "error" and report["exit_code"] == 2


def test_local_guard_width_fidelity_uses_the_width_script(local_guard):
    command = local_guard.benchmark_arguments(
        "full", None, None, None, "report.json", width_only=True
    )

    assert Path(command[0]).name == "bench_width_fidelity.py"
    assert command[1:7] == (
        "--rows", "200000", "--warmup", "2", "--iterations", "7",
    )
    assert command[-2:] == ("--output", "report.json")
    # The phase is its own script: no canary profile flag may leak into it.
    assert "--qps-only" not in command
    assert "--index-only" not in command
    assert "--skip-quantized-vector" not in command


def test_local_guard_full_mode_runs_every_dedicated_phase(local_guard):
    source = Path(local_guard.__file__).read_text(encoding="utf-8")

    # Every dedicated base/current phase of --mode full must stay wired: the
    # guard's coverage is the union of these interleaved comparisons.
    for phase in ("qps", "quant", "idx", "par", "wf", "zs", "htap", "wide", "hybrid", "pgwire", "hot"):
        assert f"collect_{phase} = make_collect(" in source
        assert f'"{phase}-comparison"' in source
    assert "width_only=True" in source
    assert "string_zones_only=True" in source
    assert "htap_only=True" in source
    assert "wide_only=True" in source
    assert "hybrid_only=True" in source
    assert "pgwire_only=True" in source
    # M-21: the full perf phase is the frozen 103-metric public scoreboard, so
    # the DELETE/UPDATE/count-only/overlay shapes run as their own phase.
    assert "hot_shapes_only=True" in source


def test_local_guard_only_metric_recheck_arguments(local_guard):
    command = local_guard.benchmark_arguments(
        "full", None, None, None, "report.json",
        only_metric="Window ROW_NUMBER PARTITION BY city",
    )

    assert "--only" in command
    assert command[command.index("--only") + 1] == "Window ROW_NUMBER PARTITION BY city"
    # The recheck needs only the wall-clock; RSS sampling adds noise.
    assert "--memory" not in command


def test_local_guard_parses_marked_metrics(local_guard):
    text = """Metric                                       Baseline        Current     Change     Status
----------------------------------------------------------------------------------------
Window ROW_NUMBER PARTITION BY city           0.430 ms         0.599 ms     +39.37%  REGRESSED
COUNT WHERE category                          0.244 ms         0.291 ms     +19.37%         ok
"""
    names = ["COUNT WHERE category", "Window ROW_NUMBER PARTITION BY city"]

    assert local_guard.marked_metrics(text, names) == [
        "Window ROW_NUMBER PARTITION BY city"
    ]


def test_pgwire_metric_names_cover_the_script():
    path = ROOT / "benchmarks" / "bench_pg_wire_concurrency.py"
    spec = importlib.util.spec_from_file_location("pgwire_script", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    names = module.pgwire_metric_names(200_000)
    assert names[0] == "PGwire: Q/s (1 client)"
    assert "PGwire: Q/s (8 clients)" in names
    assert "PGwire: time to first row (200000-row result)" in names
    assert "PGwire: full transfer (200000-row result)" in names
    assert "PGwire: metadata probe p99 (24 clients busy)" in names


def test_pgwire_only_runs_one_metric(tmp_path):
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "benchmarks" / "bench_pg_wire_concurrency.py"),
            "--rows", "3000",
            "--warmup", "1",
            "--iterations", "1",
            "--only", "PGwire: metadata probe p99 (24 clients busy)",
            "--output", str(tmp_path / "pgwire.json"),
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    report = json.loads((tmp_path / "pgwire.json").read_text(encoding="utf-8"))
    assert [row["query"] for row in report["results"]] == [
        "PGwire: metadata probe p99 (24 clients busy)"
    ]


def test_wide_metric_names_cover_the_zstd_filtered_aggregation():
    path = ROOT / "benchmarks" / "bench_wide_table.py"
    spec = importlib.util.spec_from_file_location("wide_script", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    names = module.wide_metric_names()
    assert "Wide/Zstd: filtered aggregation" in names
    assert any(name.startswith("Wide: ") for name in names)
    assert len(names) == len(set(names))


def test_wide_only_runs_one_metric(tmp_path):
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "benchmarks" / "bench_wide_table.py"),
            "--rows", "20000",
            "--warmup", "1",
            "--iterations", "1",
            "--min-row-groups", "1",
            "--only", "Wide/Zstd: filtered aggregation",
            "--output", str(tmp_path / "wide.json"),
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    report = json.loads((tmp_path / "wide.json").read_text(encoding="utf-8"))
    assert [row["query"] for row in report["results"]] == [
        "Wide/Zstd: filtered aggregation"
    ]


def test_hybrid_metric_names_cover_the_sql_match_shape():
    path = ROOT / "benchmarks" / "bench_hybrid_retrieval.py"
    spec = importlib.util.spec_from_file_location("hybrid_script", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    names = module.hybrid_metric_names(50_000)
    assert "Hybrid: SQL MATCH row selection (needle)" in names
    assert "Hybrid: BM25 top-50 (needle)" in names
    assert len(names) == len(set(names))


def test_hybrid_only_runs_one_metric(tmp_path):
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "benchmarks" / "bench_hybrid_retrieval.py"),
            "--rows", "3000",
            "--warmup", "1",
            "--iterations", "1",
            "--only", "Hybrid: SQL MATCH row selection (needle)",
            "--output", str(tmp_path / "hybrid.json"),
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    report = json.loads((tmp_path / "hybrid.json").read_text(encoding="utf-8"))
    assert [row["query"] for row in report["results"]] == [
        "Hybrid: SQL MATCH row selection (needle)"
    ]


def test_htap_metric_names_cover_the_range_delete_shape():
    path = ROOT / "benchmarks" / "bench_htap_overlay.py"
    spec = importlib.util.spec_from_file_location("htap_script", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    names = module.htap_metric_names()
    assert "HTAP: aggregate after a range delete" in names
    assert any(name.endswith("(+1% appended)") for name in names)
    assert len(names) == len(set(names))


def test_htap_only_runs_one_metric(tmp_path):
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "benchmarks" / "bench_htap_overlay.py"),
            "--rows", "5000",
            "--warmup", "1",
            "--iterations", "1",
            "--only", "HTAP: aggregate after a range delete",
            "--output", str(tmp_path / "htap.json"),
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    report = json.loads((tmp_path / "htap.json").read_text(encoding="utf-8"))
    assert [row["query"] for row in report["results"]] == [
        "HTAP: aggregate after a range delete"
    ]


def test_zone_metric_names_and_single_metric_run(tmp_path):
    path = ROOT / "benchmarks" / "bench_string_zone_shapes.py"
    spec = importlib.util.spec_from_file_location("zone_script", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    names = module.zone_metric_names()
    assert "Zone: varied-length equality" in names
    assert len(names) == len(set(names))

    completed = subprocess.run(
        [
            sys.executable,
            str(path),
            "--rows", "20000",
            "--warmup", "1",
            "--iterations", "1",
            "--only", "Zone: varied-length equality",
            "--output", str(tmp_path / "zone.json"),
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    report = json.loads((tmp_path / "zone.json").read_text(encoding="utf-8"))
    assert [row["query"] for row in report["results"]] == [
        "Zone: varied-length equality"
    ]


def test_local_guard_htap_and_zone_rechecks_use_only_metric(local_guard):
    source = Path(local_guard.__file__).read_text(encoding="utf-8")

    assert "htap-recheck.txt" in source
    assert "htap_only=True" in source
    assert "zs-recheck.txt" in source
    assert "string_zones_only=True" in source


def test_local_guard_hybrid_recheck_uses_only_metric(local_guard):
    source = Path(local_guard.__file__).read_text(encoding="utf-8")

    # M-23: a marked hybrid metric is re-measured alone.
    assert "hybrid-recheck.txt" in source
    assert "hybrid_only=True" in source


def test_local_guard_wide_recheck_uses_only_metric(local_guard):
    source = Path(local_guard.__file__).read_text(encoding="utf-8")

    # M-23: a marked wide-table metric is re-measured alone.
    assert "wide-recheck.txt" in source
    assert "wide_only=True" in source


def test_local_guard_pgwire_recheck_uses_only_metric(local_guard):
    source = Path(local_guard.__file__).read_text(encoding="utf-8")

    # M-23: a marked pgwire metric is re-measured alone, in a fresh process.
    assert "pgwire-recheck.txt" in source
    assert "recheck_marked(" in source
    assert "pgwire_only=True" in source


def test_local_guard_full_mode_rechecks_marked_metrics(local_guard):
    source = Path(local_guard.__file__).read_text(encoding="utf-8")

    # M-22: a full-sequence marker is re-measured alone before it fails the gate.
    assert 'args.mode == "full"' in source
    assert "perf-recheck.txt" in source
    assert "only_metric=metric" in source


def test_benchmark_only_filter_keeps_the_requested_metric():
    path = ROOT / "benchmarks" / "bench_vs_sqlite_duckdb.py"
    spec = importlib.util.spec_from_file_location("bench_only", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    specs = [
        ("A", "bench_a", False, False, False, None),
        ("B", "bench_b", False, False, False, None),
    ]
    assert module.filter_benchmark_specs(specs, ["B"]) == [specs[1]]
    assert module.filter_benchmark_specs(specs, []) == specs


def test_benchmark_only_runs_one_metric(tmp_path):
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "benchmarks" / "bench_vs_sqlite_duckdb.py"),
            "--rows", "2000",
            "--warmup", "1",
            "--iterations", "1",
            "--only", "COUNT(*)",
            "--output", str(tmp_path / "only.json"),
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    report = json.loads((tmp_path / "only.json").read_text(encoding="utf-8"))
    assert [row["query"] for row in report["results"]] == ["COUNT(*)"]
    assert report["results"][0]["ApexBase"] is not None


def test_local_guard_hot_phase_uses_the_canary_hot_profile(local_guard):
    command = local_guard.benchmark_arguments(
        "canary", None, None, None, "report.json", hot_shapes_only=True
    )

    assert Path(command[0]).name == "bench_perf_canary.py"
    assert "--hot-shapes-only" in command
    assert tuple(command[command.index(flag) + 1] for flag in (
        "--rows", "--warmup", "--iterations"
    )) == ("200000", "2", "7")


def test_hot_shape_specs_cover_the_scoreboard_gaps():
    path = ROOT / "benchmarks" / "bench_perf_canary.py"
    spec = importlib.util.spec_from_file_location("bench_perf_canary", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    names = [item[0] for item in module.HOT_SHAPE_SPECS]
    assert names == list(module.HOT_SHAPE_NAMES)
    for expected in (
        "Bitpack partial range COUNT",
        "COUNT(col) covered",
        "COUNT(col) partial",
        "COUNT(col) string filter",
        "Prefix LIKE COUNT",
        "Numeric range LIMIT (delta overlay)",
        "Numeric range LIMIT (filter-column update)",
        "String cell UPDATE (single row)",
        "DELETE by _id",
        "DELETE bounded range",
    ):
        assert expected in names
    # M-13: the shapes that mutate `default` run after the reads.
    assert names[-3:] == [
        "String cell UPDATE (single row)",
        "DELETE by _id",
        "DELETE bounded range",
    ]


def test_canary_hot_profile_rejects_other_profiles(tmp_path):
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "benchmarks" / "bench_perf_canary.py"),
            "--hot-shapes-only",
            "--index-only",
            "--output",
            str(tmp_path / "report.json"),
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "mutually exclusive" in completed.stderr


def test_local_guard_pgwire_phase_uses_the_concurrency_script(local_guard):
    command = local_guard.benchmark_arguments(
        "full", None, None, None, "report.json", pgwire_only=True
    )

    assert Path(command[0]).name == "bench_pg_wire_concurrency.py"
    assert command[1:7] == (
        "--rows", "200000", "--warmup", "2", "--iterations", "7",
    )
    assert command[-2:] == ("--output", "report.json")
    # The phase starts its own server and asserts its own counts: no canary
    # profile flag may leak into it.
    assert "--qps-only" not in command
    assert "--quant-only" not in command
    assert "--index-only" not in command
    assert "--skip-quantized-vector" not in command


def test_local_guard_htap_phase_uses_the_overlay_script(local_guard):
    command = local_guard.benchmark_arguments(
        "full", None, None, None, "report.json", htap_only=True
    )

    assert Path(command[0]).name == "bench_htap_overlay.py"
    assert command[1:7] == (
        "--rows", "200000", "--warmup", "2", "--iterations", "7",
    )
    assert command[-2:] == ("--output", "report.json")
    assert "--qps-only" not in command
    assert "--index-only" not in command
    assert "--skip-quantized-vector" not in command


def test_local_guard_hybrid_phase_uses_the_retrieval_script(local_guard):
    command = local_guard.benchmark_arguments(
        "full", None, None, None, "report.json", hybrid_only=True
    )

    assert Path(command[0]).name == "bench_hybrid_retrieval.py"
    assert command[1:7] == (
        "--rows", "50000", "--warmup", "2", "--iterations", "7",
    )
    assert command[-2:] == ("--output", "report.json")
    assert "--skip-quantized-vector" not in command
    assert "--qps-only" not in command


def test_local_guard_wide_phase_uses_the_wide_table_script(local_guard):
    command = local_guard.benchmark_arguments(
        "full", None, None, None, "report.json", wide_only=True
    )

    assert Path(command[0]).name == "bench_wide_table.py"
    assert command[1:7] == (
        "--rows", "200000", "--warmup", "2", "--iterations", "7",
    )
    assert command[-2:] == ("--output", "report.json")
    # The phase builds and asserts its own fixtures: no canary profile flag may
    # leak into it.
    assert "--qps-only" not in command
    assert "--index-only" not in command
    assert "--skip-quantized-vector" not in command


def test_local_guard_string_zone_phase_uses_the_zone_script(local_guard):
    command = local_guard.benchmark_arguments(
        "full", None, None, None, "report.json", string_zones_only=True
    )

    assert Path(command[0]).name == "bench_string_zone_shapes.py"
    assert command[1:7] == (
        "--rows", "200000", "--warmup", "2", "--iterations", "7",
    )
    assert command[-2:] == ("--output", "report.json")
    # The phase is its own script: no canary/profile flag may leak into it.
    assert "--qps-only" not in command
    assert "--index-only" not in command
    assert "--skip-quantized-vector" not in command

def test_local_guard_tracks_rss_on_the_public_benchmark(local_guard):
    """The perf stage must pass --memory so RSS deltas land in the report.

    A timing-only gate cannot see an out-of-core regression: scanning a 20M-row CSV once
    added +1397 MB RSS while staying inside every timing threshold.
    """
    public = local_guard.benchmark_arguments("full", None, None, None, "report.json")
    assert "--memory" in public
    assert public[-2:] == ("--output", "report.json")

    canary = local_guard.benchmark_arguments("canary", None, None, None, "report.json")
    assert Path(canary[0]).name == "bench_perf_canary.py"
    assert "--memory" not in canary


def test_public_benchmark_persists_the_rss_deltas():
    """`--memory` results must be written into the report, not only printed."""
    source = (ROOT / "benchmarks" / "bench_vs_sqlite_duckdb.py").read_text()
    assert '"apexbase_rss_delta_mb": mem_results' in source
