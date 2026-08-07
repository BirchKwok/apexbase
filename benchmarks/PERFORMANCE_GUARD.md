# ApexBase Performance Guard

The performance guard compares ApexBase with an earlier ApexBase commit. It is
separate from the public SQLite/DuckDB scoreboard: competitor wins do not prove
that a new ApexBase change avoided a regression.

## Fast canary

Build the revision in release mode, then write a canary report:

```bash
maturin develop --release
python benchmarks/bench_perf_canary.py \
  --rows 200000 \
  --warmup 2 \
  --iterations 7 \
  --output /tmp/apexbase-perf.json
```

The canary covers bulk insert, point reads, limited and full scans, filtering,
grouping, ordering, aggregation, small writes, updates, physical deletion, and
the OLAP Q/s read profile (single-thread and 4-thread, measured on a clean
loaded copy of the dataset table). It uses the same benchmark implementations
as the public scoreboard.

Run only the Q/s read profile with `--qps-only`:

```bash
python benchmarks/bench_perf_canary.py \
  --rows 200000 \
  --warmup 2 \
  --iterations 7 \
  --qps-only \
  --output /tmp/apexbase-qps.json
```

## Compare two reports

Run the base and current revisions on the same machine with identical arguments,
then compare their JSON reports:

```bash
python benchmarks/compare_perf_baseline.py \
  /tmp/apexbase-perf-base.json \
  /tmp/apexbase-perf-current.json \
  --relative-threshold 0.15 \
  --absolute-threshold-ms 0.005 \
  --require-system-match
```

A metric fails when its slowdown exceeds both the relative threshold and the
absolute tolerance. The absolute tolerance prevents ordinary timer noise from
failing microsecond-scale metrics. Missing metrics and incompatible benchmark
configurations are errors rather than silently skipped comparisons.

On variable shared runners, pass additional samples with `--baseline-sample`
and `--current-sample`. Each metric is compared using the median for its sample
set; every sample must contain the same metrics and compatible configuration.

## Local same-machine base/current guard

GitHub is not required to detect regressions. The local runner builds the base
revision and the current workspace as separate release wheels, installs both in
one temporary Python environment, and samples them in balanced
base/current/current/base/base/current order. The existing report comparator
then compares the median of three samples per side. If that comparison reports
a performance regression, the runner automatically collects two more samples
per side in current/base/base/current order and makes the final decision from
the five-sample medians. Configuration or metadata incompatibility still fails
immediately instead of being retried.

Both revisions are built from temporary Git worktrees. Tracked and untracked
current workspace files are copied into the current worktree, and the current
build starts from the lockfile generated for the base build. This prevents an
ignored, machine-local `Cargo.lock` from creating accidental dependency drift.
Cargo artifacts remain isolated between base and current while their target
directories are reused across invocations to keep later release builds
incremental.

Before running it, activate the Python environment normally used to develop
ApexBase. It must provide `maturin` and the benchmark dependencies from
`pyproject.toml`; the temporary benchmark environment inherits those exact
packages. Use a quiet machine, connect AC power, close CPU-heavy applications,
and keep the same power and thermal settings for both revisions.

For the development canary (which includes the two Q/s metrics):

```bash
python benchmarks/run_local_perf_guard.py --base-ref origin/main
```

The current side is built from the current workspace, including local source
changes. `--base-ref` may be any local Git commit, branch, or tag. Fetch the
remote first if `origin/main` must reflect its latest state.

For the complete suite — the public tabular/vector scoreboard **plus** a
separate base/current Q/s comparison (ApexBase-only, canary scale):

```bash
python benchmarks/run_local_perf_guard.py \
  --base-ref origin/main \
  --mode full
```

The full mode is intentionally expensive: it performs six complete benchmark
runs of the public scoreboard and six Q/s runs in addition to two release
builds. The canary is the normal edit-time check; use full mode for final
performance acceptance. A Q/s regression fails the full gate even when every
public tabular/vector metric passes.

By default, reports are written under
`local-perf-results/<timestamp>/`. A custom directory must not already exist:

```bash
python benchmarks/run_local_perf_guard.py \
  --base-ref HEAD^ \
  --output-dir /tmp/apexbase-perf-check
```

Useful options are:

- `--relative-threshold 0.15`: allow at most 15% relative slowdown.
- `--absolute-threshold-ms 0.005`: tolerate up to 0.005 ms of timer noise.
- `--settle-seconds 30`: wait after compilation before sampling.
- `--rows`, `--warmup`, and `--iterations`: override the selected mode's
  defaults. Use identical values for every baseline comparison series.

A metric fails only when it exceeds both the relative and absolute tolerance.
Exit status 0 means the gate passed; status 1 means at least one metric
regressed; status 2 means the reports were incompatible or incomplete. The
output directory normally keeps six JSON reports and `comparison.txt`. A
confirmation run keeps ten JSON reports, the first decision in
`comparison-initial.txt`, and the final five-sample decision in
`comparison.txt`.

This local guard is the required same-machine performance evidence. The
repository intentionally does not run performance comparisons in GitHub
Actions: hosted runners are not stable acceptance machines, while routing the
workflow back to the development Mac would only duplicate this local guard.
Keep the generated reports under `local-perf-results/` as the performance
record for each accepted change.

## Repeatable pytest runtime gate

The local acceptance process also measures the complete, serial pytest suite
five times:

```bash
python benchmarks/check_pytest_runtime.py \
  --runs 5 \
  --limit-seconds 9.0 \
  --allowed-over-limit 1 \
  --max-spread-seconds 1.0 \
  --output /tmp/pytest-runtime.json
```

All five runs must pass functionally. The runtime gate requires a median no
greater than 9 seconds, permits at most one noisy sample above 9 seconds, and
rejects a sample spread above 1 second. This keeps 9 seconds as the central
limit without allowing one unusually fast run to hide a consistently slow
suite. Tests are never parallelized, skipped, or reduced for this check.

Local investigation on 2026-07-19 measured five adjacent full-suite runs at
9.55, 9.02, 9.12, 9.00, and 8.90 seconds (median 9.02, spread 0.65). JUnit
timing attributed about 8.18 seconds to the 1461 test cases themselves, with
the remaining time spread across collection, pytest overhead, process startup,
and filesystem cleanup. The slowest repeatable cases were the memory-pressure
case, two million-row SQL cases, and the cross-process visibility case; there
was no single disposable test responsible for the boundary.

After adding the guard's five unit tests, an end-to-end guard run measured
8.82, 8.84, 8.87, 8.79, and 8.68 seconds (median 8.82, spread 0.19), with all
1466 tests passing in every sample.

The repository's required local acceptance sequence in `AGENTS.md` remains the
source of truth. The canary supplements the full benchmark; it does not replace
it.
