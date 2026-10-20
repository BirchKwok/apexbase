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
    index_only=False, parallel_only=False, width_only=False, string_zones_only=False,
    htap_only=False, wide_only=False, hybrid_only=False, pgwire_only=False,
    hot_shapes_only=False, only_metric=None,
):
    if width_only:
        # The width-fidelity phase builds its own tables (one V4 full-width
        # file, one V5 narrow file) and emits its own metric rows, so it is its
        # own script rather than a profile of the canary.
        script = ROOT / "benchmarks" / "bench_width_fidelity.py"
        defaults = (200_000, 2, 7)
    elif string_zones_only:
        # The string-zone phase builds its own table covering the zone shapes
        # (equal-width shared-prefix values, bucket keys, unprovable probes) and
        # asserts every row count, so it is its own script too.
        script = ROOT / "benchmarks" / "bench_string_zone_shapes.py"
        defaults = (200_000, 2, 7)
    elif htap_only:
        # The HTAP overlay phase writes its own table, appends 1% buffered rows
        # and asserts every aggregate against an independent computation, so it
        # is its own script as well.
        script = ROOT / "benchmarks" / "bench_htap_overlay.py"
        defaults = (200_000, 2, 7)
    elif wide_only:
        # The wide-table phase builds its own 16-column table (plus a zstd twin)
        # over several row groups and asserts every shape, so it is its own
        # script too.
        script = ROOT / "benchmarks" / "bench_wide_table.py"
        defaults = (200_000, 2, 7)
    elif hybrid_only:
        # The hybrid-retrieval phase builds its own text + vector table, asserts
        # BM25 hit membership and exact vector recall, then times the fused
        # pipeline; it is its own script as well.
        script = ROOT / "benchmarks" / "bench_hybrid_retrieval.py"
        defaults = (50_000, 2, 7)
    elif pgwire_only:
        # The PG wire phase starts a real server, drives it with concurrent
        # simple-query connections and asserts every timed count, so it is its
        # own script as well.
        script = ROOT / "benchmarks" / "bench_pg_wire_concurrency.py"
        defaults = (200_000, 2, 7)
    elif mode == "canary" or qps_only or quant_only or index_only or parallel_only:
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
    ]
    if script.name == "bench_vs_sqlite_duckdb.py" and only_metric is None:
        # Track per-case RSS as well. Out-of-core work is invisible to a wall-clock-only gate:
        # scanning a 20M-row CSV used to add +1397 MB RSS while staying fast enough not to
        # trip a timing threshold. The extra fields are informational (the comparison tool
        # ignores them), so this cannot mask a timing regression. The single-metric
        # recheck skips it: it only needs the wall-clock, and the RSS sampling adds noise.
        command.append("--memory")
    command += ["--output", output]
    if mode == "full" and not qps_only and not quant_only and not width_only \
            and not string_zones_only and not htap_only and not wide_only \
            and not hybrid_only and not pgwire_only and not hot_shapes_only:
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
    if hot_shapes_only:
        command.insert(1, "--hot-shapes-only")
    if only_metric is not None:
        command.extend(("--only", only_metric))
    return tuple(command)


def report_metric_names(path):
    """Metric names measured by one benchmark report, in report order."""
    try:
        report = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    return [
        row.get("query")
        for row in report.get("results", [])
        if isinstance(row, dict) and row.get("query")
    ]


def marked_metrics(comparison_text, metric_names):
    """Metric names a comparison marked REGRESSED, in report order."""
    marked = set()
    for line in comparison_text.splitlines():
        if not line.rstrip().endswith("REGRESSED"):
            continue
        for name in metric_names:
            if line.startswith(name):
                marked.add(name)
                break
    return [name for name in metric_names if name in marked]


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

            def recheck_marked(stem, marked_reports, command_builder, summary_name, file_prefix):
                """Re-measure marked metrics alone, each in a fresh process.

                A metric can be marked only because it runs late in a shared
                process whose earlier metrics left a different cache/memory state
                (M-22), or because a concurrency probe's p99 is bimodal. A real
                slowdown reproduces in the isolated run; a coupling marker does not.
                """
                comparison_text = (output_dir / f"{stem}.txt").read_text(
                    encoding="utf-8"
                )
                metric_names = report_metric_names(marked_reports["base"][0])
                marked = marked_metrics(comparison_text, metric_names)
                remaining = []
                lines = []
                for index, metric in enumerate(marked):
                    counts = {"base": 0, "current": 0}
                    reports = {"base": [], "current": []}
                    collect = make_collect(
                        f"{file_prefix}-{index}",
                        counts,
                        reports,
                        lambda side, report, metric=metric: command_builder(
                            metric, report
                        ),
                    )
                    collect(SAMPLE_ORDER)
                    status = run_comparison(
                        collect, reports, f"{file_prefix}-{index}"
                    )
                    lines.append(
                        f"{metric}: "
                        + (
                            "still regressed in a fresh process"
                            if status != 0
                            else "clean in a fresh process"
                        )
                    )
                    if status != 0:
                        remaining.append(metric)
                (output_dir / summary_name).write_text(
                    "\n".join(lines) + "\n", encoding="utf-8"
                )
                print("\nMarked-metric recheck (fresh process per metric):")
                print("\n".join(lines))
                if marked and not remaining:
                    print(
                        "All marked metrics pass when measured in a fresh process; "
                        "the full-sequence marker is cross-metric coupling, not a "
                        "query regression."
                    )
                    return 0
                return 1

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

            if comparison_status == 1 and args.mode == "full":
                comparison_status = recheck_marked(
                    "comparison",
                    reports,
                    lambda metric, report: benchmark_arguments(
                        "full", None, None, None, report, only_metric=metric
                    ),
                    "perf-recheck.txt",
                    "recheck",
                )

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

                # V5 width fidelity: the same dataset written once in the V4
                # full-width layout and once in the V5 narrow layout. The phase
                # measures bytes on disk, warm latency, and first-touch page
                # faults per shape, so the storage and read gain of a narrower
                # physical encoding is quantified in every full-mode run instead
                # of only being asserted. Latency rows are milliseconds; the
                # bytes/pages rows are counts (larger is worse, which is the
                # comparator's default direction).
                wf_counts = {"base": 0, "current": 0}
                wf_reports = {"base": [], "current": []}
                collect_wf = make_collect(
                    "wf",
                    wf_counts,
                    wf_reports,
                    lambda side, report: benchmark_arguments(
                        "canary", None, None, None, report, width_only=True
                    ),
                )
                collect_wf(SAMPLE_ORDER)
                wf_status = run_comparison(collect_wf, wf_reports, "wf-comparison")
                comparison_status = max(comparison_status, wf_status)

                # V5 string zone maps: the shapes the value and tail ranges
                # exist for. A build with only the length heuristic scans every
                # row group on an equal-width shared-prefix column, so this
                # phase quantifies the pruning gain directly instead of leaving
                # it to the noisy scoreboard family.
                zs_counts = {"base": 0, "current": 0}
                zs_reports = {"base": [], "current": []}
                collect_zs = make_collect(
                    "zs",
                    zs_counts,
                    zs_reports,
                    lambda side, report: benchmark_arguments(
                        "canary", None, None, None, report, string_zones_only=True
                    ),
                )
                collect_zs(SAMPLE_ORDER)
                zs_status = run_comparison(collect_zs, zs_reports, "zs-comparison")
                comparison_status = max(comparison_status, zs_status)

                # HTAP overlay: the same fused aggregations with 1% appended rows
                # sitting in the write buffer. A read lane that stands down while
                # an overlay exists pays a full merged materialization, so this
                # phase quantifies the write-then-read amplification directly.
                htap_counts = {"base": 0, "current": 0}
                htap_reports = {"base": [], "current": []}
                collect_htap = make_collect(
                    "htap",
                    htap_counts,
                    htap_reports,
                    lambda side, report: benchmark_arguments(
                        "canary", None, None, None, report, htap_only=True
                    ),
                )
                collect_htap(SAMPLE_ORDER)
                htap_status = run_comparison(
                    collect_htap, htap_reports, "htap-comparison"
                )
                comparison_status = max(comparison_status, htap_status)

                # Wide tables: 16 columns over seven row groups, plus a
                # zstd-compressed twin. Projection bandwidth, per-row-group
                # header work and decode cost all scale differently than in the
                # narrow canary tables, so these shapes are sampled base/current
                # like any other metric instead of being assumed.
                wide_counts = {"base": 0, "current": 0}
                wide_reports = {"base": [], "current": []}
                collect_wide = make_collect(
                    "wide",
                    wide_counts,
                    wide_reports,
                    lambda side, report: benchmark_arguments(
                        "canary", None, None, None, report, wide_only=True
                    ),
                )
                collect_wide(SAMPLE_ORDER)
                wide_status = run_comparison(
                    collect_wide, wide_reports, "wide-comparison"
                )
                if wide_status == 1:
                    wide_status = recheck_marked(
                        "wide-comparison",
                        wide_reports,
                        lambda metric, report: benchmark_arguments(
                            "canary",
                            None,
                            None,
                            None,
                            report,
                            wide_only=True,
                            only_metric=metric,
                        ),
                        "wide-recheck.txt",
                        "wide-recheck",
                    )
                comparison_status = max(comparison_status, wide_status)

                # Mixed retrieval: one table carries both a text column and a
                # 64-dimension vector column, and the phase times the BM25
                # branch, the exact vector branch and their RRF fusion. It is the
                # only coverage of V5's "mixed search" goal, and it asserts the
                # branch results (membership, exhaustive recall) before timing.
                hybrid_counts = {"base": 0, "current": 0}
                hybrid_reports = {"base": [], "current": []}
                collect_hybrid = make_collect(
                    "hybrid",
                    hybrid_counts,
                    hybrid_reports,
                    lambda side, report: benchmark_arguments(
                        "canary", None, None, None, report, hybrid_only=True
                    ),
                )
                collect_hybrid(SAMPLE_ORDER)
                hybrid_status = run_comparison(
                    collect_hybrid, hybrid_reports, "hybrid-comparison"
                )
                comparison_status = max(comparison_status, hybrid_status)

                # PG wire concurrency: a real server driven by concurrent
                # simple-query connections. This is the only coverage of the
                # server path, where a statement used to run on a tokio worker
                # (capping execution at the worker count and stalling the
                # connection loop). The phase times the 1/2/4/8-client curve and
                # a metadata probe sent while 24 clients are busy, and asserts
                # every timed count against the generated rows.
                pgwire_counts = {"base": 0, "current": 0}
                pgwire_reports = {"base": [], "current": []}
                collect_pgwire = make_collect(
                    "pgwire",
                    pgwire_counts,
                    pgwire_reports,
                    lambda side, report: benchmark_arguments(
                        "canary", None, None, None, report, pgwire_only=True
                    ),
                )
                collect_pgwire(SAMPLE_ORDER)
                pgwire_status = run_comparison(
                    collect_pgwire, pgwire_reports, "pgwire-comparison"
                )
                if pgwire_status == 1:
                    pgwire_status = recheck_marked(
                        "pgwire-comparison",
                        pgwire_reports,
                        lambda metric, report: benchmark_arguments(
                            "canary",
                            None,
                            None,
                            None,
                            report,
                            pgwire_only=True,
                            only_metric=metric,
                        ),
                        "pgwire-recheck.txt",
                        "pgwire-recheck",
                    )
                comparison_status = max(comparison_status, pgwire_status)

                # Hot shapes the public scoreboard cannot carry (M-21): DELETE,
                # single-cell UPDATE, count-only and persisted-overlay reads are
                # only in CANARY_SPECS, so the full `perf` phase (the 103-metric
                # public scoreboard, D12-frozen) never samples them. Run that
                # subset as its own interleaved base/current phase here instead
                # of widening the scoreboard and its fixed invariants.
                hot_counts = {"base": 0, "current": 0}
                hot_reports = {"base": [], "current": []}
                collect_hot = make_collect(
                    "hot",
                    hot_counts,
                    hot_reports,
                    lambda side, report: benchmark_arguments(
                        "canary", None, None, None, report, hot_shapes_only=True
                    ),
                )
                collect_hot(SAMPLE_ORDER)
                hot_status = run_comparison(
                    collect_hot, hot_reports, "hot-comparison"
                )
                comparison_status = max(comparison_status, hot_status)

            print(f"Reports and comparison saved in {output_dir}")
        finally:
            for worktree in reversed(worktrees):
                run(("git", "worktree", "remove", "--force", worktree), check=False)

    return comparison_status


if __name__ == "__main__":
    raise SystemExit(main())
