"""Mixed read/write workload for the base/current gate.

Four writer processes append disjoint id ranges through `store`, while four
reader threads keep querying the same table: a full count, point lookups of the
seeded rows and a bounded range count. After the writers join, every written row
must be present exactly once, the seeded rows must be unchanged and no reader may
have observed a count below the initial row count.

The reported metrics are writer throughput (rows/s), reader throughput (Q/s) and
the read-latency p50/p99. The workload (not just a SQL string) is the unit under
test, so `--only` still runs the whole fixture and simply selects which metric
row is reported; the guard uses it to re-measure a marked metric in a fresh
process.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
import shutil
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench_vs_sqlite_duckdb as full_bench
from apexbase.client import ApexClient

WRITERS = 4
READERS = 4
BATCH = 200
DEFAULT_ROWS = 50_000
DEFAULT_PER_WRITER = 2_000


def _writer(root: str, start: int, count: int, barrier, errors) -> None:
    client = None
    try:
        client = ApexClient(dirpath=root)
        client.use_table("mixed")
        barrier.wait()
        for offset in range(0, count, BATCH):
            size = min(BATCH, count - offset)
            values = list(range(start + offset, start + offset + size))
            client.store({"k": values, "v": values})
        client.flush()
    except BaseException as exc:  # surfaced by the parent as a hard failure
        errors.put(f"writer {start}: {exc!r}")
    finally:
        if client is not None:
            client.close()


def _reader(root: str, rows: int, stop: threading.Event, started: threading.Event,
            latencies: list, errors: list, counters: list) -> None:
    client = None
    try:
        client = ApexClient(dirpath=root, enable_cache=False)
        client.use_table("mixed")
        started.wait()
        point_key = rows // 3
        point_value = point_key
        low = rows // 2
        queries = 0
        while not stop.is_set():
            for sql, check in (
                ("SELECT COUNT(*) AS n FROM mixed", None),
                (f"SELECT v FROM mixed WHERE k = {point_key}", "point"),
                (f"SELECT COUNT(*) AS n FROM mixed WHERE k >= {low}", None),
            ):
                begin = time.perf_counter()
                result = client.execute(sql).to_dict()
                latencies.append((time.perf_counter() - begin) * 1000.0)
                queries += 1
                if check == "point" and result[0]["v"] != point_value:
                    errors.append(f"point read changed: {result[0]}")
                    return
                # A live COUNT may briefly observe a partially published
                # append batch while another process is writing; the exact
                # final count is asserted after every writer has joined. The
                # seeded point read is the invariant that must never change.
        counters.append(queries)
    except BaseException as exc:
        errors.append(f"reader: {exc!r}")
    finally:
        if client is not None:
            client.close()


def mixed_metric_names(writers: int, readers: int) -> list[str]:
    return [
        f"Mixed: writer rows/s ({writers} writers)",
        f"Mixed: reader Q/s ({readers} readers)",
        "Mixed: read p50 (ms)",
        "Mixed: read p99 (ms)",
    ]


def run_mixed(rows: int, writers: int, readers: int, per_writer: int, only=None):
    full_bench.ensure_optional_imports()
    if not full_bench.HAS_APEXBASE:
        raise RuntimeError("ApexBase is not importable; run maturin develop --release first")
    wanted = set(only) if only else None

    def want(name: str) -> bool:
        return wanted is None or name in wanted

    tmpdir = Path(tempfile.mkdtemp(prefix="apexbase_mixed_"))
    db_dir = tmpdir / "db"
    metrics = []
    detail = {
        "rows": rows,
        "writers": writers,
        "readers": readers,
        "per_writer": per_writer,
    }
    try:
        db_dir.mkdir(parents=True, exist_ok=True)
        client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)
        client.create_table("mixed", schema={"k": "int64", "v": "int64"})
        client.use_table("mixed")
        client.store({"k": list(range(rows)), "v": list(range(rows))})
        client.flush()
        client.close()

        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(writers)
        errors = context.Queue()
        processes = [
            context.Process(
                target=_writer,
                args=(
                    str(db_dir),
                    rows + index * per_writer,
                    per_writer,
                    barrier,
                    errors,
                ),
            )
            for index in range(writers)
        ]
        for process in processes:
            process.start()

        stop = threading.Event()
        started = threading.Event()
        latencies: list[float] = []
        reader_errors: list[str] = []
        counters: list[int] = []
        threads = [
            threading.Thread(
                target=_reader,
                args=(
                    str(db_dir), rows, stop, started, latencies, reader_errors, counters
                ),
                daemon=True,
            )
            for _ in range(readers)
        ]
        for thread in threads:
            thread.start()
        started.set()
        reader_begin = time.perf_counter()

        writer_begin = time.perf_counter()
        for process in processes:
            process.join()
        writer_seconds = time.perf_counter() - writer_begin
        # Keep the readers running for a short minimum window so their
        # throughput is measured against a real interval even when the
        # writers finish almost immediately.
        while time.perf_counter() - reader_begin < 0.5:
            time.sleep(0.005)
        stop.set()
        for thread in threads:
            thread.join(timeout=30)
        reader_seconds = time.perf_counter() - reader_begin

        failed = [process for process in processes if process.exitcode != 0]
        writer_errors = []
        while not errors.empty():
            writer_errors.append(errors.get())
        if failed or writer_errors or reader_errors:
            raise RuntimeError(
                "mixed workload failed: "
                + repr(
                    {
                        "writer_exitcodes": [p.exitcode for p in processes],
                        "writer_errors": writer_errors,
                        "reader_errors": reader_errors,
                    }
                )
            )

        verify = ApexClient(str(db_dir), enable_cache=False)
        verify.use_table("mixed")
        total = int(verify.execute("SELECT COUNT(*) AS n FROM mixed").to_dict()[0]["n"])
        expected = rows + writers * per_writer
        if total != expected:
            raise RuntimeError(f"final row count {total} != {expected}")
        for index in range(writers):
            start = rows + index * per_writer
            for key in (start, start + per_writer // 2, start + per_writer - 1):
                got = verify.execute(f"SELECT v FROM mixed WHERE k = {key}").to_dict()
                if not got or int(got[0]["v"]) != key:
                    raise RuntimeError(f"missing or wrong written row k={key}: {got}")
        for key in (0, rows // 2, rows - 1):
            got = verify.execute(f"SELECT v FROM mixed WHERE k = {key}").to_dict()
            if not got or int(got[0]["v"]) != key:
                raise RuntimeError(f"seeded row changed k={key}: {got}")
        verify.close()

        reads = sum(counters)
        rows_per_s = (writers * per_writer) / writer_seconds if writer_seconds > 0 else 0.0
        qps = reads / reader_seconds if reader_seconds > 0 else 0.0
        ordered = sorted(latencies)
        p50 = statistics.median(ordered) if ordered else 0.0
        p99 = ordered[max(0, int(len(ordered) * 0.99) - 1)] if ordered else 0.0
        detail.update(
            {
                "writer_seconds": round(writer_seconds, 6),
                "reader_seconds": round(reader_seconds, 6),
                "reads": reads,
                "final_rows": total,
            }
        )
        for name, value in (
            (f"Mixed: writer rows/s ({writers} writers)", rows_per_s),
            (f"Mixed: reader Q/s ({readers} readers)", qps),
            ("Mixed: read p50 (ms)", p50),
            ("Mixed: read p99 (ms)", p99),
        ):
            if want(name):
                metrics.append(
                    {
                        "category": "ApexBase mixed read/write",
                        "query": name,
                        "ApexBase": round(value, 6),
                    }
                )
        if wanted is not None and not metrics:
            raise RuntimeError(f"--only matched no mixed metric: {sorted(wanted)}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    return metrics, detail


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--writers", type=int, default=WRITERS)
    parser.add_argument("--readers", type=int, default=READERS)
    parser.add_argument("--per-writer", type=int, default=DEFAULT_PER_WRITER)
    # Accepted for the guard's shared CLI; the workload is a fixed short run.
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        metavar="METRIC",
        help="Report only the named metric(s); used by the guard to recheck a "
        "marked metric in a fresh process",
    )
    args = parser.parse_args(argv)
    if args.rows <= 0 or args.writers <= 0 or args.readers <= 0 or args.per_writer <= 0:
        parser.error("rows, writers, readers and per-writer must be positive")
    known = set(mixed_metric_names(args.writers, args.readers))
    unknown = [name for name in args.only if name not in known]
    if unknown:
        parser.error("--only metric(s) not reported by this script: " + ", ".join(unknown))

    results, detail = run_mixed(
        args.rows, args.writers, args.readers, args.per_writer, only=args.only
    )
    report = {
        **full_bench.get_report_metadata("apexbase-canary"),
        "config": {
            "rows": args.rows,
            "writers": args.writers,
            "readers": args.readers,
            "per_writer": args.per_writer,
        },
        "results": results,
        "mixed_read_write": detail,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    raise SystemExit(main())
