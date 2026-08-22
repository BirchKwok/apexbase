"""Build and compare a base revision with the current workspace on one machine."""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_ORDER = ("base", "current", "current", "base", "base", "current")
CONFIRMATION_SAMPLE_ORDER = ("current", "base", "base", "current")
FULL_QUANT_ROWS = 1_000_000


def run(command, *, cwd=ROOT, env=None, capture=False, check=True):
    printable = " ".join(shlex.quote(str(part)) for part in command)
    print(f"\n+ {printable}", flush=True)
    return subprocess.run(
        [str(part) for part in command],
        cwd=cwd,
        env=env,
        check=check,
        text=True,
        capture_output=capture,
    )


def git_output(*args):
    return run(("git", *args), capture=True).stdout.strip()


def copy_workspace_changes(destination):
    patch = run(("git", "diff", "--binary", "HEAD"), capture=True).stdout
    if patch:
        print("\nApplying tracked workspace changes to temporary current tree...", flush=True)
        subprocess.run(
            ("git", "apply", "--whitespace=nowarn"),
            cwd=destination,
            input=patch,
            check=True,
            text=True,
        )
    untracked = run(
        ("git", "ls-files", "--others", "--exclude-standard", "-z"), capture=True
    ).stdout.split("\0")
    for relative in filter(None, untracked):
        source = ROOT / relative
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target, follow_symlinks=False)


def one_wheel(directory):
    wheels = list(directory.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"expected one wheel in {directory}, found {len(wheels)}")
    return wheels[0]


def benchmark_arguments(
    mode, rows, warmup, iterations, output, qps_only=False, quant_only=False
):
    if mode == "canary" or qps_only or quant_only:
        script = ROOT / "benchmarks" / "bench_perf_canary.py"
        defaults = (200_000, 2, 7)
    else:
        script = ROOT / "benchmarks" / "bench_vs_sqlite_duckdb.py"
        defaults = (1_000_000, 2, 5)
    resolved = (
        defaults[0] if rows is None else rows,
        defaults[1] if warmup is None else warmup,
        defaults[2] if iterations is None else iterations,
    )
    command = [
        script,
        "--rows", str(resolved[0]),
        "--warmup", str(resolved[1]),
        "--iterations", str(resolved[2]),
        "--output", output,
    ]
    if mode == "full" and not qps_only and not quant_only:
        # Quantized base/current metrics run in the dedicated interleaved phase.
        command.insert(1, "--skip-quantized-vector")
    if qps_only:
        command.insert(1, "--qps-only")
    if quant_only:
        command.insert(1, "--quant-only")
    return tuple(command)


def comparison_arguments(python, reports, args):
    command = [
        python,
        ROOT / "benchmarks" / "compare_perf_baseline.py",
        reports["base"][0],
        reports["current"][0],
    ]
    for report in reports["base"][1:]:
        command.extend(("--baseline-sample", report))
    for report in reports["current"][1:]:
        command.extend(("--current-sample", report))
    command.extend((
        "--relative-threshold", args.relative_threshold,
        "--absolute-threshold-ms", args.absolute_threshold_ms,
        "--require-system-match",
    ))
    return tuple(command)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", default="main", help="Git revision used as baseline")
    parser.add_argument("--mode", choices=("canary", "full"), default="canary")
    parser.add_argument("--rows", type=int)
    parser.add_argument("--warmup", type=int)
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--relative-threshold", type=float, default=0.15)
    parser.add_argument("--absolute-threshold-ms", type=float, default=0.005)
    parser.add_argument("--settle-seconds", type=float, default=30.0)
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Report directory (default: ./local-perf-results/<timestamp>)",
    )
    args = parser.parse_args(argv)
    if args.rows is not None and args.rows <= 0:
        parser.error("--rows must be positive")
    if args.warmup is not None and args.warmup < 0:
        parser.error("--warmup must be non-negative")
    if args.iterations is not None and args.iterations <= 0:
        parser.error("--iterations must be positive")
    if args.relative_threshold < 0 or args.absolute_threshold_ms < 0:
        parser.error("thresholds must be non-negative")
    if args.settle_seconds < 0:
        parser.error("--settle-seconds must be non-negative")
    return args


def main(argv=None):
    args = parse_args(argv)
    if shutil.which("maturin") is None:
        raise SystemExit("maturin is required; install it in the active environment")

    try:
        base_commit = git_output("rev-parse", "--verify", f"{args.base_ref}^{{commit}}")
        current_commit = git_output("rev-parse", "HEAD")
        current_branch = git_output("rev-parse", "--abbrev-ref", "HEAD")
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"cannot resolve Git revisions: {exc}") from exc

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = (args.output_dir or ROOT / "local-perf-results" / timestamp).resolve()
    output_dir.mkdir(parents=True, exist_ok=False)
    dirty = bool(git_output("status", "--porcelain"))
    if dirty:
        print("Current workspace is dirty; current wheel includes local source changes.")

    print(f"Base:    {args.base_ref} ({base_commit})")
    print(f"Current: {current_branch} ({current_commit}){' + local changes' if dirty else ''}")
    print(f"Reports: {output_dir}")

    with tempfile.TemporaryDirectory(prefix="apexbase-local-perf-") as temp_name:
        temp = Path(temp_name)
        base_tree = temp / "base"
        current_tree = temp / "current"
        wheel_dirs = {side: temp / "wheels" / side for side in ("base", "current")}
        target_dirs = {
            "base": ROOT / "target" / "local-performance-base",
            "current": ROOT / "target",
        }
        venv = temp / "venv"
        for directory in wheel_dirs.values():
            directory.mkdir(parents=True)

        worktrees = []
        try:
            run(("git", "worktree", "add", "--detach", base_tree, base_commit))
            worktrees.append(base_tree)
            run(("git", "worktree", "add", "--detach", current_tree, current_commit))
            worktrees.append(current_tree)
            copy_workspace_changes(current_tree)
            run((sys.executable, "-m", "venv", "--system-site-packages", venv))
            python = venv / "bin" / "python"
            build_env = os.environ.copy()
            build_env.setdefault("RUSTFLAGS", "-C target-cpu=native")

            for side, source in (("base", base_tree), ("current", current_tree)):
                if side == "current":
                    shutil.copy2(base_tree / "Cargo.lock", current_tree / "Cargo.lock")
                env = {**build_env, "CARGO_TARGET_DIR": str(target_dirs[side])}
                run(
                    (
                        "maturin", "build", "--release", "--interpreter", python,
                        "--out", wheel_dirs[side],
                    ),
                    cwd=source,
                    env=env,
                )
            wheels = {side: one_wheel(directory) for side, directory in wheel_dirs.items()}

            if args.settle_seconds:
                print(f"\nWaiting {args.settle_seconds:g}s for build load to settle...", flush=True)
                time.sleep(args.settle_seconds)

            def make_collect(prefix, counts, reports, command_builder):
                def collect(sample_order):
                    for side in sample_order:
                        counts[side] += 1
                        report = output_dir / f"{prefix}-{side}-{counts[side]}.json"
                        reports[side].append(report)
                        run((python, "-m", "pip", "install", "--force-reinstall", "--no-deps", wheels[side]))
                        env = os.environ.copy()
                        if side == "base":
                            env.update({
                                "APEXBASE_BENCHMARK_COMMIT": base_commit,
                                "APEXBASE_BENCHMARK_BRANCH": args.base_ref,
                                "APEXBASE_BENCHMARK_DIRTY": "0",
                            })
                        else:
                            env.update({
                                "APEXBASE_BENCHMARK_COMMIT": current_commit,
                                "APEXBASE_BENCHMARK_BRANCH": current_branch,
                                "APEXBASE_BENCHMARK_DIRTY": "1" if dirty else "0",
                            })
                        command = (python, *command_builder(side, str(report)))
                        run(command, env=env)
                return collect

            def run_comparison(collect, reports, stem):
                completed = run(
                    comparison_arguments(python, reports, args),
                    capture=True,
                    check=False,
                )
                if completed.returncode == 1:
                    initial_output = completed.stdout + completed.stderr
                    (output_dir / f"{stem}-initial.txt").write_text(
                        initial_output, encoding="utf-8"
                    )
                    print(f"\n{initial_output}", end="")
                    print(
                        "Initial three-sample comparison failed; collecting two "
                        "additional samples per side for confirmation.",
                        flush=True,
                    )
                    collect(CONFIRMATION_SAMPLE_ORDER)
                    completed = run(
                        comparison_arguments(python, reports, args),
                        capture=True,
                        check=False,
                    )
                comparison = output_dir / f"{stem}.txt"
                comparison_output = completed.stdout + completed.stderr
                comparison.write_text(comparison_output, encoding="utf-8")
                print(f"\n{comparison_output}", end="")
                return completed.returncode

            counts = {"base": 0, "current": 0}
            reports = {"base": [], "current": []}
            collect = make_collect(
                "perf",
                counts,
                reports,
                lambda side, report: benchmark_arguments(
                    args.mode, args.rows, args.warmup, args.iterations, report
                ),
            )
            collect(SAMPLE_ORDER)
            comparison_status = run_comparison(collect, reports, "comparison")

            # The full gate also covers the OLAP Q/s read profile
            # (ApexBase-only, canary scale) as a separate base/current
            # comparison, so a Q/s regression fails the gate even when the
            # public tabular/vector metrics pass.
            qps_status = 0
            if args.mode == "full":
                qps_counts = {"base": 0, "current": 0}
                qps_reports = {"base": [], "current": []}
                collect_qps = make_collect(
                    "qps",
                    qps_counts,
                    qps_reports,
                    lambda side, report: benchmark_arguments(
                        "canary", None, None, None, report, qps_only=True
                    ),
                )
                collect_qps(SAMPLE_ORDER)
                qps_status = run_comparison(
                    collect_qps, qps_reports, "qps-comparison"
                )
                comparison_status = max(comparison_status, qps_status)

                # Quantized distance scans are a core vector hot path but are
                # intentionally ApexBase-only here. Compare them base/current
                # at a bounded scale after the public cross-engine benchmark.
                quant_counts = {"base": 0, "current": 0}
                quant_reports = {"base": [], "current": []}
                collect_quant = make_collect(
                    "quant",
                    quant_counts,
                    quant_reports,
                    lambda side, report: benchmark_arguments(
                        "canary", FULL_QUANT_ROWS, None, None, report, quant_only=True
                    ),
                )
                collect_quant(SAMPLE_ORDER)
                quant_status = run_comparison(
                    collect_quant, quant_reports, "quant-comparison"
                )
                comparison_status = max(comparison_status, quant_status)

            print(f"Reports and comparison saved in {output_dir}")
        finally:
            for worktree in reversed(worktrees):
                run(("git", "worktree", "remove", "--force", worktree), check=False)

    return comparison_status


if __name__ == "__main__":
    raise SystemExit(main())
