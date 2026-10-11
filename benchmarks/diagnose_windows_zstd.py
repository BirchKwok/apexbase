#!/usr/bin/env python3
"""Locate the native abort in the compressed high-cardinality string path.

`test_string_order_by_topk_lane.py::test_a_high_cardinality_string_orders_correctly_in_both_directions[1-zstd]`
aborts the process on Windows while the uncompressed twin passes and the same
300k-row zstd table is fine on Linux and macOS. A native abort leaves no Python
traceback, so this script walks the same phases one at a time and prints a marker
before each one: the last marker in the output names the operation that faulted.
It also walks the row count downwards, which shows whether the fault has a size
threshold.

Run it from the repository root with the release extension installed:

    python -X faulthandler benchmarks/diagnose_windows_zstd.py
    python -X faulthandler benchmarks/diagnose_windows_zstd.py --rows 300000

On Windows, also set `RUST_BACKTRACE=full` (a Rust panic prints a backtrace; an
access violation or stack overflow does not) and, if the process dies silently,
read the faulting module and exception code from the event log:

    Get-WinEvent -FilterHashtable @{LogName='Application'} -MaxEvents 40 |
      Where-Object { $_.Message -match 'python|_core|apexbase' } |
      Select-Object -First 5 -ExpandProperty Message

An exception code of 0xC0000005 is an access violation and 0xC00000FD a stack
overflow; the two need different fixes.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
import traceback

from apexbase import ApexClient

DEFAULT_SIZES = (30_000, 100_000, 200_000, 300_000)
COMPRESSIONS = ("none", "zstd")


def mark(message: str) -> None:
    print(f"[phase] {message}", flush=True)


def exercise(rows: int, compression: str) -> bool:
    """Run the failing test's phases in order; return False if one faulted."""
    tmp = tempfile.mkdtemp(prefix=f"apexbase_zstd_{compression}_")
    client = None
    try:
        mark(f"rows={rows} compression={compression}: open client")
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "uniq": "string"})
        client.use_table("t")

        if compression != "none":
            mark(f"rows={rows} {compression}: set_compression")
            assert client.set_compression(compression) is not False

        mark(f"rows={rows} {compression}: store")
        client.store({
            "k": list(range(rows)),
            "uniq": [f"u{i:07d}" for i in range(rows)],
        })

        mark(f"rows={rows} {compression}: flush (compress + write)")
        client.flush()

        for limit, offset in ((1, 0), (5, 0), (10, 7), (100, 0)):
            mark(f"rows={rows} {compression}: ASC LIMIT {limit} OFFSET {offset}")
            client.execute(
                f"SELECT uniq FROM t ORDER BY uniq ASC LIMIT {limit} OFFSET {offset}"
            ).to_dict()

        mark(f"rows={rows} {compression}: DESC LIMIT 5")
        client.execute("SELECT uniq FROM t ORDER BY uniq DESC LIMIT 5").to_dict()

        mark(f"rows={rows} {compression}: DELETE k < 1000 + flush")
        client.execute("DELETE FROM t WHERE k < 1000")
        client.flush()

        for direction in ("ASC", "DESC"):
            mark(f"rows={rows} {compression}: {direction} LIMIT 5 after the delete")
            values = [row["uniq"] for row in client.execute(
                f"SELECT uniq FROM t ORDER BY uniq {direction} LIMIT 5"
            ).to_dict()]
            if values and values[0] != f"u{1000 if direction == 'ASC' else rows - 1:07d}":
                print(f"    unexpected first value: {values[:3]}", flush=True)

        mark(f"rows={rows} {compression}: ALL PHASES OK")
        return True
    except BaseException:
        # A Python-level failure is information too: print it and continue.
        traceback.print_exc()
        return False
    finally:
        if client is not None:
            try:
                client.close()
            except BaseException:
                traceback.print_exc()
        shutil.rmtree(tmp, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows",
        type=int,
        nargs="*",
        default=list(DEFAULT_SIZES),
        help="row counts to walk (default: 30000 100000 200000 300000)",
    )
    parser.add_argument(
        "--compression",
        choices=COMPRESSIONS,
        nargs="*",
        default=list(COMPRESSIONS),
        help="compressions to walk (default: none zstd); 'none' is the control",
    )
    args = parser.parse_args(argv)

    print(f"python={sys.version.split()[0]} platform={sys.platform} pid={os.getpid()}", flush=True)
    for rows in args.rows:
        for compression in args.compression:
            if not exercise(rows, compression):
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
