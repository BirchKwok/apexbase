#!/usr/bin/env python3
"""Rebuild ``benchmarks/latest_public_baseline.json`` from N benchmark runs.

Design rule D10 / S14: the public baseline is the **median of at least three
same-machine runs** so a single noisy run cannot move it. Only ApexBase metric
values are aggregated; every other section (config/system/build/scoreboard and
the competitor columns) is copied from the first sample. The first report's git
metadata stays; the aggregate provenance is recorded under ``baseline_meta``.

Usage::

    python benchmarks/rebuild_public_baseline.py \
        --input run1.json run2.json run3.json \
        --output benchmarks/latest_public_baseline.json
"""

from __future__ import annotations

import argparse
import copy
import json
import statistics
from pathlib import Path


VECTOR_SECTIONS = ("head_to_head", "batch", "apex_only")


def _metric_rows(report):
    rows = list(report.get("results") or [])
    vector = report.get("vector_similarity") or {}
    for section in VECTOR_SECTIONS:
        rows.extend(vector.get(section) or [])
    return rows


def aggregate_public_baseline(reports, minimum=3):
    """Median the ApexBase values of every comparable metric across reports."""
    reports = list(reports)
    if len(reports) < minimum:
        raise ValueError(f"need at least {minimum} benchmark samples, got {len(reports)}")
    suite = reports[0].get("suite")
    config = reports[0].get("config")
    for index, report in enumerate(reports, start=1):
        if report.get("suite") != suite:
            raise ValueError(f"sample {index} has a different suite")
        if report.get("config") != config:
            raise ValueError(f"sample {index} has a different config")

    names = None
    samples = []
    for index, report in enumerate(reports, start=1):
        values = {}
        for row in _metric_rows(report):
            name = row.get("query")
            if name is None or "ApexBase" not in row:
                continue
            if name in values:
                raise ValueError(f"sample {index} has duplicate metric {name!r}")
            values[name] = float(row["ApexBase"])
        if not values:
            raise ValueError(f"sample {index} has no metrics")
        if names is None:
            names = set(values)
        elif set(values) != names:
            missing = sorted(names - set(values))
            extra = sorted(set(values) - names)
            raise ValueError(
                f"sample {index} metric set differs (missing {missing}, extra {extra})"
            )
        samples.append(values)

    merged = copy.deepcopy(reports[0])
    for row in _metric_rows(merged):
        name = row.get("query")
        if name in names:
            row["ApexBase"] = round(statistics.median(values[name] for values in samples), 6)
    merged["baseline_meta"] = {
        "samples": len(reports),
        "protocol": "median of at least three same-machine runs",
        "metric_count": len(names),
        "sources": [
            {
                "git": (report.get("git") or {}).get("commit"),
                "started_at": report.get("started_at"),
                "dirty": (report.get("git") or {}).get("dirty"),
            }
            for report in reports
        ],
    }
    return merged


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", nargs="+", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minimum", type=int, default=3)
    args = parser.parse_args(argv)

    reports = [json.loads(path.read_text(encoding="utf-8")) for path in args.input]
    merged = aggregate_public_baseline(reports, minimum=args.minimum)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(merged, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        f"wrote {args.output} from {len(reports)} samples "
        f"({merged['baseline_meta']['metric_count']} metrics)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
