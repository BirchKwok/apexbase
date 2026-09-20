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
    (tree / "tracked.py").write_text("before\n")
    (tree / "deleted.py").write_text("removed\n")
    (tree / ".gitignore").write_text("Cargo.lock\nignored\n")
    git("add", ".")
    git("-c", "user.name=Evidence Test", "-c", "user.email=test@example.invalid",
        "commit", "-m", "base")
    (tree / "tracked.py").write_text("after\n")
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
        "sha256": hashlib.sha256(b"after\n").hexdigest(), "executable": True,
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
