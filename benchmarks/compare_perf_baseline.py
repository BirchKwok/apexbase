"""Compare ApexBase benchmark JSON reports and fail on performance regressions."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path


CONFIG_KEYS = (
    "profile",
    "rows",
    "warmup",
    "iterations",
    "vector_rows",
    "vector_dim",
    "vector_k",
    "skip_vector",
)
SYSTEM_KEYS = ("platform", "machine", "processor", "cpu_count", "python")
THROUGHPUT_METRIC_HINTS = ("Q/s",)


def is_throughput_metric(name):
    """Return True when a larger value is better (e.g. queries per second)."""
    return any(hint in name for hint in THROUGHPUT_METRIC_HINTS)


class ReportError(ValueError):
    pass


def load_report(path):
    try:
        report = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReportError(f"cannot read {path}: {exc}") from exc
    if not isinstance(report, dict):
        raise ReportError(f"{path} must contain a JSON object")
    return report


def extract_apex_metrics(report):
    rows = list(report.get("results") or [])
    vector = report.get("vector_similarity") or {}
    for section in ("head_to_head", "batch", "apex_only"):
        rows.extend(vector.get(section) or [])

    metrics = {}
    for row in rows:
        if not isinstance(row, dict) or "query" not in row or "ApexBase" not in row:
            continue
        name = str(row["query"])
        try:
            value = float(row["ApexBase"])
        except (TypeError, ValueError) as exc:
            raise ReportError(f"metric {name!r} has a non-numeric ApexBase value") from exc
        if value < 0:
            raise ReportError(f"metric {name!r} has a negative ApexBase value")
        if name in metrics:
            raise ReportError(f"duplicate ApexBase metric {name!r}")
        metrics[name] = value
    if not metrics:
        raise ReportError("report contains no ApexBase metrics")
    return metrics


def compatibility_errors(baseline, current, require_system_match=False):
    errors = []
    if baseline.get("suite") != current.get("suite"):
        errors.append("benchmark suites differ")

    base_config = baseline.get("config") or {}
    current_config = current.get("config") or {}
    for key in CONFIG_KEYS:
        if key in base_config or key in current_config:
            if base_config.get(key) != current_config.get(key):
                errors.append(f"config.{key} differs")

    if require_system_match:
        base_system = baseline.get("system") or {}
        current_system = current.get("system") or {}
        for key in SYSTEM_KEYS:
            if base_system.get(key) != current_system.get(key):
                errors.append(f"system.{key} differs")
        for section in ("dependencies", "build"):
            if (baseline.get(section) or {}) != (current.get(section) or {}):
                errors.append(f"{section} differs")
    return errors


def aggregate_report_metrics(reports):
    reports = list(reports)
    if not reports:
        raise ReportError("at least one benchmark report is required")

    samples = [extract_apex_metrics(report) for report in reports]
    expected = set(samples[0])
    for index, sample in enumerate(samples[1:], start=2):
        actual = set(sample)
        if actual != expected:
            missing = sorted(expected - actual)
            extra = sorted(actual - expected)
            details = []
            if missing:
                details.append("missing " + ", ".join(missing))
            if extra:
                details.append("extra " + ", ".join(extra))
            raise ReportError(f"sample {index} metric set differs: " + "; ".join(details))

    return {
        name: statistics.median(sample[name] for sample in samples)
        for name in sorted(expected)
    }


def compare_metric_sets(
    baseline_metrics,
    current_metrics,
    relative_threshold=0.15,
    absolute_threshold_ms=0.005,
    metrics=None,
):
    if relative_threshold < 0 or absolute_threshold_ms < 0:
        raise ValueError("thresholds must be non-negative")

    selected = list(metrics) if metrics else sorted(baseline_metrics)
    missing = [name for name in selected if name not in baseline_metrics or name not in current_metrics]
    if missing:
        raise ReportError("missing metrics: " + ", ".join(sorted(missing)))

    comparisons = []
    for name in selected:
        before = baseline_metrics[name]
        after = current_metrics[name]
        if is_throughput_metric(name):
            # Queries per second: a drop beyond the relative tolerance is a
            # regression. The millisecond absolute tolerance is a timer-noise
            # guard for microsecond latencies and does not apply to Q/s.
            tolerance = before * relative_threshold
            delta = after - before
            regressed = delta < -tolerance
        else:
            tolerance = max(before * relative_threshold, absolute_threshold_ms)
            delta = after - before
            regressed = delta > tolerance
        comparisons.append({
            "query": name,
            "baseline_ms": before,
            "current_ms": after,
            "delta_ms": delta,
            "limit_ms": before + tolerance,
            "regressed": regressed,
        })
    return comparisons


def compare_reports(
    baseline,
    current,
    relative_threshold=0.15,
    absolute_threshold_ms=0.005,
    metrics=None,
):
    return compare_metric_sets(
        extract_apex_metrics(baseline),
        extract_apex_metrics(current),
        relative_threshold,
        absolute_threshold_ms,
        metrics,
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("current", type=Path)
    parser.add_argument("--relative-threshold", type=float, default=0.15)
    parser.add_argument("--absolute-threshold-ms", type=float, default=0.005)
    parser.add_argument("--baseline-sample", action="append", type=Path, default=[])
    parser.add_argument("--current-sample", action="append", type=Path, default=[])
    parser.add_argument("--metric", action="append", dest="metrics")
    parser.add_argument("--require-system-match", action="store_true")
    args = parser.parse_args(argv)

    try:
        baseline_reports = [load_report(args.baseline)]
        baseline_reports.extend(load_report(path) for path in args.baseline_sample)
        current_reports = [load_report(args.current)]
        current_reports.extend(load_report(path) for path in args.current_sample)
        reference = baseline_reports[0]
        for report in baseline_reports[1:] + current_reports:
            errors = compatibility_errors(reference, report, args.require_system_match)
            if errors:
                raise ReportError("incompatible reports: " + "; ".join(errors))
        rows = compare_metric_sets(
            aggregate_report_metrics(baseline_reports),
            aggregate_report_metrics(current_reports),
            args.relative_threshold,
            args.absolute_threshold_ms,
            args.metrics,
        )
    except (ReportError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(f"{'Metric':<38} {'Baseline':>14} {'Current':>14} {'Change':>10} {'Status':>10}")
    print("-" * 88)
    regressions = []
    for row in rows:
        before = row["baseline_ms"]
        after = row["current_ms"]
        change = ((after / before) - 1.0) * 100.0 if before > 0 else 0.0
        status = "REGRESSED" if row["regressed"] else "ok"
        unit = "Q/s" if is_throughput_metric(row["query"]) else "ms"
        print(
            f"{row['query']:<38} {before:>12.3f} {unit:<3} {after:>12.3f} {unit:<3} "
            f"{change:>+9.2f}% {status:>10}"
        )
        if row["regressed"]:
            regressions.append(row)

    if regressions:
        print(f"\nPerformance gate failed: {len(regressions)} metric(s) regressed.")
        return 1
    print(f"\nPerformance gate passed: {len(rows)} metric(s) checked.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
