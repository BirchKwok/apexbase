import importlib.util
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
