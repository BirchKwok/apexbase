"""Build and compare a base revision with the current workspace on one machine."""

from __future__ import annotations

import argparse
import hashlib
import json
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


def file_digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class GateEvidence:
    """Persist provenance and raw outcomes outside benchmark timing regions."""

    def __init__(self, directory, metadata):
        self.directory = directory
        self.data = {
            "format_version": 1,
            **metadata,
            "status": "running",
            "exit_code": None,
            "started_at": datetime.now().astimezone().isoformat(),
            "comparisons": {},
            "sources": {},
            "wheels": {},
        }
        self.save()

    def save(self):
        temporary = self.directory / "run-manifest.json.tmp"
        temporary.write_text(json.dumps(self.data, indent=2) + "\n", encoding="utf-8")
        temporary.replace(self.directory / "run-manifest.json")

    def source(self, side, tree):
        # Record the actual temporary tree, including copied untracked sources.
        paths = run(
            ("git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"),
            cwd=tree, capture=True,
        ).stdout.split("\0")
        files = {}
        for relative in sorted(set(filter(None, paths))):
            path = tree / relative
            if path.is_symlink():
                files[relative] = {"symlink": os.readlink(path)}
            elif path.is_file():
                files[relative] = {
                    "sha256": file_digest(path),
                    "executable": bool(path.stat().st_mode & 0o111),
                }
        inventory = self.directory / f"source-{side}.json"
        inventory.write_text(json.dumps(files, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        patch = run(("git", "diff", "--binary", "HEAD"), cwd=tree, capture=True).stdout
        (self.directory / f"source-{side}.patch").write_text(patch, encoding="utf-8")
        self.data["sources"][side] = {
            "inventory": inventory.name,
            "inventory_sha256": file_digest(inventory),
            "patch_sha256": file_digest(self.directory / f"source-{side}.patch"),
        }
        self.save()

    def lockfile(self, side, tree):
        # Cargo.lock may be ignored by Git but is part of the actual build input.
        lock = tree / "Cargo.lock"
        shutil.copy2(lock, self.directory / f"Cargo-{side}.lock")
        self.data["sources"][side]["cargo_lock_sha256"] = file_digest(lock)
        self.save()

    def wheel(self, side, path):
        self.data["wheels"][side] = {"name": path.name, "sha256": file_digest(path)}
        self.save()

    def comparison(self, name, returncode):
        self.data["comparisons"][name] = returncode
        self.save()

    def finish(self, returncode, error=None):
        self.data.update(
            exit_code=returncode,
            status={0: "passed", 1: "regressed", 130: "interrupted"}.get(returncode, "error"),
            finished_at=datetime.now().astimezone().isoformat(),
        )
        if error is not None:
            self.data["error"] = str(error)
        self.save()


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
    mode, rows, warmup, iterations, output, qps_only=False, quant_only=False,
    index_only=False, parallel_only=False,
):
    if mode == "canary" or qps_only or quant_only or index_only or parallel_only:
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
    if index_only:
        command.insert(1, "--index-only")
    if parallel_only:
        command.insert(1, "--parallel-only")
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

    evidence = GateEvidence(output_dir, {
        "base_commit": base_commit,
        "current_commit": current_commit,
        "current_branch": current_branch,
        "dirty": dirty,
        "arguments": {key: str(value) if isinstance(value, Path) else value
                      for key, value in vars(args).items()},
        "python": sys.version,
        "load_average_at_start": list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
    })
    try:
        result = execute_gate(args, output_dir, evidence)
    except KeyboardInterrupt:
        evidence.finish(130, "interrupted by user")
        return 130
    except Exception as exc:
        if isinstance(exc, subprocess.CalledProcessError):
            evidence.data["failed_command_exit_code"] = exc.returncode
        evidence.finish(2, exc)
        print(f"Performance gate could not complete: {exc}", file=sys.stderr)
        return 2
    evidence.finish(result)
    return result


def execute_gate(args, output_dir, evidence):
    base_commit = evidence.data["base_commit"]
    current_commit = evidence.data["current_commit"]
    current_branch = evidence.data["current_branch"]
    dirty = evidence.data["dirty"]
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
            evidence.source("base", base_tree)
            evidence.source("current", current_tree)
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
                evidence.lockfile(side, source)
            wheels = {side: one_wheel(directory) for side, directory in wheel_dirs.items()}
            for side, wheel in wheels.items():
                evidence.wheel(side, wheel)

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
                    evidence.comparison(f"{stem}-initial", completed.returncode)
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
                evidence.comparison(stem, completed.returncode)
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

                # Index-accelerated reads are a core OLTP hot path
                # (architecture review R5.6). Compare the four index canary
                # metrics base/current at full scale in a dedicated
                # interleaved phase.
                idx_counts = {"base": 0, "current": 0}
                idx_reports = {"base": [], "current": []}
                collect_idx = make_collect(
                    "idx",
                    idx_counts,
                    idx_reports,
                    lambda side, report: benchmark_arguments(
                        "canary",
                        args.rows if args.rows is not None else 1_000_000,
                        args.warmup,
                        args.iterations,
                        report,
                        index_only=True,
                    ),
                )
                collect_idx(SAMPLE_ORDER)
                idx_status = run_comparison(
                    collect_idx, idx_reports, "idx-comparison"
                )
                comparison_status = max(comparison_status, idx_status)

                # The opt-in parallel fold (architecture review R5.7 phase A)
                # is a core scan shape when enabled; compare its behavior
                # base/current at full scale in a dedicated interleaved
                # phase (2/4/8 threads).
                par_counts = {"base": 0, "current": 0}
                par_reports = {"base": [], "current": []}
                collect_par = make_collect(
                    "par",
                    par_counts,
                    par_reports,
                    lambda side, report: benchmark_arguments(
                        "canary",
                        args.rows if args.rows is not None else 1_000_000,
                        args.warmup,
                        args.iterations,
                        report,
                        parallel_only=True,
                    ),
                )
                collect_par(SAMPLE_ORDER)
                par_status = run_comparison(
                    collect_par, par_reports, "par-comparison"
                )
                comparison_status = max(comparison_status, par_status)

            print(f"Reports and comparison saved in {output_dir}")
        finally:
            for worktree in reversed(worktrees):
                run(("git", "worktree", "remove", "--force", worktree), check=False)

    return comparison_status


if __name__ == "__main__":
    raise SystemExit(main())
