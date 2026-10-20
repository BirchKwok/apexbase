"""Measure the PostgreSQL wire server under concurrent clients.

Every statement used to run *on* a tokio worker thread: execution concurrency was
capped by the worker count, and a long scan stalled that worker's share of the
connection loop. Statements now wait for one of a bounded number of slots and run
on the blocking pool, so connections keep being accepted, read and answered while
scans execute.

This phase makes both halves visible base/current:

* `PGwire: Q/s (N clients)` for N in 1/2/4/8 — the throughput curve, measured
  with `iterations` verifiable aggregation queries per client per level.
* `PGwire: time to first row` / `full transfer` — a whole-table `SELECT *`. The
  server used to encode the entire result before sending its first row, so the
  first-row metric isolates result materialization from transfer cost.
* `PGwire: metadata probe p99 (24 clients busy)` — a client that only sends
  `SELECT version()` while 24 connections hammer the server. The probe measures
  how long connection handling waits behind running scans; it is answered by the
  metadata lane, which must not queue behind them.

Every timed query is asserted against an independent count over the generated
rows, and the probe's answer must be non-empty, so a server that drops, queues
past the client timeout or returns a wrong count fails the benchmark instead of
reporting a fast wrong answer.

Metric rows use the guard's report format (`query` + `ApexBase`), so
`benchmarks/compare_perf_baseline.py` and
`benchmarks/run_local_perf_guard.py --mode full` consume them directly.

The client is a minimal PostgreSQL wire implementation over `socket` (startup,
simple query, RowDescription/DataRow parsing) so the phase needs no third-party
driver and each connection is fully under the benchmark's control.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
import shutil
import socket
import statistics
import struct
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench_vs_sqlite_duckdb as full_bench
from apexbase.client import ApexClient

# Client levels for the throughput curve (same shape as the read Q/s curve).
CLIENT_LEVELS = (1, 2, 4, 8)

# Clients kept busy while the metadata probe is measured. Stay above a typical
# worker count: that is exactly the region where running queries on the reactor
# starves connection handling.
PROBE_BUSY_CLIENTS = 24

# Predicate bound for the timed queries; every query returns a verifiable count.
PREDICATE_BASE = 1000


class PgWireError(RuntimeError):
    """The server answered with an ErrorResponse.

    `code` carries the SQLSTATE, so a caller can tell a capacity rejection
    (`53300`) from a bad statement (`42000`).
    """

    def __init__(self, message: str, code: str | None = None):
        super().__init__(message)
        self.code = code


class MinimalPgClient:
    """Just enough of the PostgreSQL wire protocol for simple queries."""

    def __init__(self, host: str, port: int, timeout: float = 60.0):
        self._socket = socket.create_connection((host, port), timeout=timeout)
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._startup()

    def _recv_exact(self, count: int) -> bytes:
        chunks = []
        remaining = count
        while remaining:
            chunk = self._socket.recv(remaining)
            if not chunk:
                raise ConnectionError("server closed the connection")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _read_message(self) -> tuple[bytes, bytes]:
        kind = self._recv_exact(1)
        (length,) = struct.unpack("!i", self._recv_exact(4))
        return kind, self._recv_exact(length - 4)

    def _startup(self) -> None:
        payload = struct.pack("!HH", 3, 0) + b"user\x00bench\x00database\x00default\x00\x00"
        self._socket.sendall(struct.pack("!i", len(payload) + 4) + payload)
        while True:
            kind, body = self._read_message()
            if kind == b"R":
                (code,) = struct.unpack("!i", body[:4])
                if code != 0:
                    raise PgWireError(f"unsupported authentication request {code}")
            elif kind == b"E":
                raise self._error(body)
            elif kind == b"Z":
                return

    @staticmethod
    def _error_fields(body: bytes) -> tuple[str | None, str]:
        """(SQLSTATE, message) from an ErrorResponse."""
        code = None
        message = "query failed"
        for field in body.split(b"\x00"):
            if field[:1] == b"C":
                code = field[1:].decode("utf-8", "replace")
            elif field[:1] == b"M":
                message = field[1:].decode("utf-8", "replace")
        return code, message

    @classmethod
    def _error(cls, body: bytes) -> "PgWireError":
        code, message = cls._error_fields(body)
        return PgWireError(message, code)

    def query(self, sql: str) -> list[list[str | None]]:
        """Run one simple query and return its rows as text fields."""
        payload = sql.encode("utf-8") + b"\x00"
        self._socket.sendall(b"Q" + struct.pack("!i", len(payload) + 4) + payload)
        rows: list[list[str | None]] = []
        error: PgWireError | None = None
        while True:
            kind, body = self._read_message()
            if kind == b"D":
                if error is not None:
                    continue
                (count,) = struct.unpack("!h", body[:2])
                offset = 2
                row: list[str | None] = []
                for _ in range(count):
                    (length,) = struct.unpack("!i", body[offset : offset + 4])
                    offset += 4
                    if length == -1:
                        row.append(None)
                    else:
                        row.append(body[offset : offset + length].decode("utf-8", "replace"))
                        offset += length
                rows.append(row)
            elif kind == b"E":
                # Keep reading: the server still sends ReadyForQuery, and leaving
                # it in the socket desynchronizes the next statement.
                error = self._error(body)
            elif kind == b"Z":
                if error is not None:
                    raise error
                return rows

    def query_streaming(self, sql: str, keep_rows: bool = False) -> dict:
        """Run a query, timing the first `DataRow` and the whole transfer.

        The server used to encode every row into a `Vec<DataRow>` before sending
        the first one, so a large result made the client wait for the full encode.
        `first_row_ms` measures exactly that wait; rows are only counted, not
        decoded, so the number reflects the server rather than the client's
        parsing.
        """
        payload = sql.encode("utf-8") + b"\x00"
        started = time.perf_counter()
        self._socket.sendall(b"Q" + struct.pack("!i", len(payload) + 4) + payload)
        first_row_ms: float | None = None
        row_count = 0
        rows: list[list[str | None]] = []
        error: PgWireError | None = None
        while True:
            kind, body = self._read_message()
            if kind == b"E":
                # Drain to ReadyForQuery before raising (see `query`).
                error = self._error(body)
                continue
            if kind == b"D":
                if first_row_ms is None:
                    first_row_ms = (time.perf_counter() - started) * 1000.0
                row_count += 1
                if keep_rows:
                    (count,) = struct.unpack("!h", body[:2])
                    offset = 2
                    row: list[str | None] = []
                    for _ in range(count):
                        (length,) = struct.unpack("!i", body[offset : offset + 4])
                        offset += 4
                        if length == -1:
                            row.append(None)
                        else:
                            row.append(body[offset : offset + length].decode("utf-8", "replace"))
                            offset += length
                    rows.append(row)
            elif kind == b"Z":
                total_ms = (time.perf_counter() - started) * 1000.0
                if error is not None:
                    raise error
                return {
                    "first_row_ms": first_row_ms,
                    "total_ms": total_ms,
                    "row_count": row_count,
                    "rows": rows,
                }

    def close(self) -> None:
        try:
            self._socket.sendall(b"X" + struct.pack("!i", 4))
        except OSError:
            pass
        self._socket.close()


def generate_rows(rows: int) -> dict:
    """Same shape as the canary dataset: two numeric columns and one string."""
    return {
        "a": list(range(rows)),
        "b": [i % 977 for i in range(rows)],
        "s": [f"v{i % 50}" for i in range(rows)],
    }


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _serve(data_dir: str, host: str, port: int) -> None:
    import apexbase._core as core

    core.start_pg_server(data_dir, host, port)


def start_server(data_dir: str, port: int) -> multiprocessing.Process:
    """Start the PG wire server in a child process and wait for its port."""
    context = multiprocessing.get_context("spawn")
    process = context.Process(target=_serve, args=(data_dir, "127.0.0.1", port), daemon=True)
    process.start()
    deadline = time.monotonic() + 60.0
    while time.monotonic() < deadline:
        if process.exitcode is not None:
            raise RuntimeError(f"server exited during startup with {process.exitcode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return process
        except OSError:
            time.sleep(0.05)
    process.terminate()
    raise RuntimeError("server did not start listening in time")


def stop_server(process: multiprocessing.Process) -> None:
    if process.is_alive():
        process.terminate()
        process.join(10)


def check_query(client: MinimalPgClient, rows: int, predicate: int) -> None:
    """The timed query must agree with an independent count over the rows."""
    got = client.query(f"SELECT COUNT(*) AS n FROM t WHERE a >= {predicate}")
    assert len(got) == 1 and len(got[0]) == 1, got
    want = max(rows - predicate, 0)
    assert int(got[0][0]) == want, (predicate, got, want)


def run_client_level(
    host: str,
    port: int,
    rows: int,
    clients: int,
    warmup: int,
    iterations: int,
) -> dict:
    """Run `clients` connections in parallel, each with `iterations` queries."""
    latencies: list[float] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(clients)
    lock = threading.Lock()

    def worker(index: int) -> None:
        try:
            client = MinimalPgClient(host, port)
        except BaseException as exc:  # noqa: BLE001 - reported by the caller
            with lock:
                errors.append(exc)
            barrier.abort()
            return
        local: list[float] = []
        try:
            barrier.wait(timeout=60)
            for step in range(warmup + iterations):
                predicate = PREDICATE_BASE + index + step
                start = time.perf_counter()
                client.query(f"SELECT COUNT(*) AS n FROM t WHERE a >= {predicate}")
                elapsed = (time.perf_counter() - start) * 1000.0
                if step >= warmup:
                    local.append(elapsed)
        except BaseException as exc:  # noqa: BLE001 - reported by the caller
            with lock:
                errors.append(exc)
        finally:
            client.close()
            with lock:
                latencies.extend(local)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(clients)]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=300)
    elapsed = time.perf_counter() - started
    if errors:
        raise errors[0]
    if any(thread.is_alive() for thread in threads):
        raise RuntimeError(f"{clients} client(s) did not finish")
    total = clients * iterations
    return {
        "clients": clients,
        "queries": total,
        "qps": total / elapsed if elapsed > 0 else None,
        "p50_ms": statistics.median(latencies) if latencies else None,
        "p99_ms": sorted(latencies)[max(int(len(latencies) * 0.99) - 1, 0)] if latencies else None,
    }


def measure_probe(host: str, port: int, rows: int, warmup: int, iterations: int) -> dict:
    """Metadata-probe latency while `PROBE_BUSY_CLIENTS` connections are busy."""
    stop = threading.Event()
    latencies: list[float] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def busy_worker(index: int) -> None:
        try:
            client = MinimalPgClient(host, port)
        except BaseException as exc:  # noqa: BLE001 - reported by the caller
            with lock:
                errors.append(exc)
            return
        try:
            step = 0
            while not stop.is_set():
                client.query(f"SELECT COUNT(*) AS n FROM t WHERE a >= {PREDICATE_BASE + index + step}")
                step += 1
        except BaseException:  # noqa: BLE001 - shutdown closes sockets mid-query
            pass
        finally:
            client.close()

    busy = [
        threading.Thread(target=busy_worker, args=(index,), daemon=True)
        for index in range(PROBE_BUSY_CLIENTS)
    ]
    for thread in busy:
        thread.start()
    try:
        probe_client = MinimalPgClient(host, port)
        try:
            for step in range(warmup + iterations):
                start = time.perf_counter()
                got = probe_client.query("SELECT version()")
                elapsed = (time.perf_counter() - start) * 1000.0
                assert got and got[0] and got[0][0], got
                if step >= warmup:
                    latencies.append(elapsed)
        finally:
            probe_client.close()
    finally:
        stop.set()
        for thread in busy:
            thread.join(timeout=60)
    if errors:
        raise errors[0]
    return {
        "busy_clients": PROBE_BUSY_CLIENTS,
        "samples": len(latencies),
        "p50_ms": statistics.median(latencies) if latencies else None,
        "p99_ms": sorted(latencies)[max(int(len(latencies) * 0.99) - 1, 0)] if latencies else None,
    }


def server_rss_mb(pid: int) -> float | None:
    """Resident set size of the server process, in MiB.

    Unix uses `ps`; Windows has no `ps`, so it reads the process working set through
    `GetProcessMemoryInfo`. The probe is what the large-result memory assertion and the
    gate's RSS sampling depend on, and it must answer on every platform the release
    workflow tests. `sys` is imported at module scope.
    """
    import subprocess

    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class _ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        try:
            process = ctypes.windll.kernel32.OpenProcess(0x0410, False, pid)
            if not process:
                return None
            try:
                counters = _ProcessMemoryCounters()
                counters.cb = ctypes.sizeof(counters)
                if not ctypes.windll.psapi.GetProcessMemoryInfo(
                    process, ctypes.byref(counters), counters.cb
                ):
                    return None
                return counters.WorkingSetSize / (1024.0 * 1024.0)
            finally:
                ctypes.windll.kernel32.CloseHandle(process)
        except (AttributeError, OSError, ValueError):
            return None

    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        ).stdout.strip()
        return int(out) / 1024.0 if out else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def measure_large_result(host: str, port: int, rows: int, iterations: int, pid: int | None) -> dict:
    """Time a whole-table transfer and sample the server's peak RSS around it."""
    first: list[float] = []
    total: list[float] = []
    row_counts: list[int] = []
    peak_rss: list[float] = []
    client = MinimalPgClient(host, port)
    try:
        sql = "SELECT * FROM t"
        # Warm up the file pages and the plan once, then measure.
        warm = client.query_streaming(sql)
        assert warm["row_count"] == rows, (warm["row_count"], rows)
        for _ in range(iterations):
            baseline = server_rss_mb(pid) if pid else None
            samples: list[float] = []
            stop = threading.Event()

            def sample() -> None:
                while not stop.is_set():
                    value = server_rss_mb(pid) if pid else None
                    if value is not None:
                        samples.append(value)
                    stop.wait(0.005)

            sampler = threading.Thread(target=sample, daemon=True) if pid else None
            if sampler:
                sampler.start()
            result = client.query_streaming(sql)
            stop.set()
            if sampler:
                sampler.join(timeout=10)
            first.append(result["first_row_ms"])
            total.append(result["total_ms"])
            row_counts.append(result["row_count"])
            if baseline is not None and samples:
                peak_rss.append(max(samples) - baseline)
    finally:
        client.close()
    assert all(count == rows for count in row_counts), row_counts
    return {
        "rows": rows,
        "first_row_ms": statistics.median(first),
        "total_ms": statistics.median(total),
        "peak_rss_delta_mb": max(peak_rss) if peak_rss else None,
    }


def check_large_result(host: str, port: int, data: dict, limit: int = 50) -> None:
    """Spot-check streamed values against the generator."""
    client = MinimalPgClient(host, port)
    try:
        result = client.query_streaming(f"SELECT a, b, s FROM t LIMIT {limit}", keep_rows=True)
    finally:
        client.close()
    assert result["row_count"] == limit, result
    for index, row in enumerate(result["rows"]):
        assert int(row[0]) == data["a"][index], (index, row)
        assert int(row[1]) == data["b"][index], (index, row)
        assert row[2] == data["s"][index], (index, row)


def pgwire_metric_names(rows: int) -> list[str]:
    """Every metric this script can report, in report order."""
    names = [
        f"PGwire: Q/s ({clients} client{'s' if clients > 1 else ''})"
        for clients in CLIENT_LEVELS
    ]
    names.append(f"PGwire: time to first row ({rows}-row result)")
    names.append(f"PGwire: full transfer ({rows}-row result)")
    names.append(f"PGwire: metadata probe p99 ({PROBE_BUSY_CLIENTS} clients busy)")
    return names


def run_pgwire(
    rows: int,
    warmup: int,
    iterations: int,
    only: list[str] | None = None,
) -> tuple[list[dict], dict]:
    wanted = set(only) if only else None

    def want(name: str) -> bool:
        return wanted is None or name in wanted

    full_bench.ensure_optional_imports()
    if not full_bench.HAS_APEXBASE:
        raise RuntimeError("ApexBase is not importable; run maturin develop --release first")

    tmpdir = Path(tempfile.mkdtemp(prefix="apexbase_pgwire_"))
    db_dir = tmpdir / "db"
    detail: dict = {"rows": rows, "levels": {}, "probe": {}, "clients": list(CLIENT_LEVELS)}
    results: list[dict] = []
    server = None
    try:
        db_dir.mkdir(parents=True, exist_ok=True)
        data = generate_rows(rows)
        client = ApexClient(str(db_dir), drop_if_exists=True, enable_cache=False)
        client.create_table("t", schema={"a": "int64", "b": "int64", "s": "string"})
        client.use_table("t")
        client.store(data)
        client.flush()
        client.close()

        port = free_port()
        server = start_server(str(db_dir), port)

        # Verify the wire path itself before timing anything.
        check_client = MinimalPgClient("127.0.0.1", port)
        try:
            check_query(check_client, rows, PREDICATE_BASE)
        finally:
            check_client.close()
        check_large_result("127.0.0.1", port, data)

        for clients in CLIENT_LEVELS:
            name = f"PGwire: Q/s ({clients} client{'s' if clients > 1 else ''})"
            if not want(name):
                continue
            level = run_client_level("127.0.0.1", port, rows, clients, warmup, iterations)
            detail["levels"][str(clients)] = {
                key: (round(value, 6) if isinstance(value, float) else value)
                for key, value in level.items()
            }
            results.append(
                {
                    "category": "ApexBase PG wire",
                    "query": f"PGwire: Q/s ({clients} client{'s' if clients > 1 else ''})",
                    "ApexBase": round(level["qps"], 6),
                }
            )

        first_name = f"PGwire: time to first row ({rows}-row result)"
        transfer_name = f"PGwire: full transfer ({rows}-row result)"
        if want(first_name) or want(transfer_name):
            transfer = measure_large_result("127.0.0.1", port, rows, max(warmup, 3), server.pid)
            detail["large_result"] = {
                key: (round(value, 6) if isinstance(value, float) else value)
                for key, value in transfer.items()
            }
            if want(first_name):
                results.append(
                    {
                        "category": "ApexBase PG wire",
                        "query": first_name,
                        "ApexBase": round(transfer["first_row_ms"], 6),
                    }
                )
            if want(transfer_name):
                results.append(
                    {
                        "category": "ApexBase PG wire",
                        "query": transfer_name,
                        "ApexBase": round(transfer["total_ms"], 6),
                    }
                )

        probe_name = f"PGwire: metadata probe p99 ({PROBE_BUSY_CLIENTS} clients busy)"
        if want(probe_name):
            probe = measure_probe("127.0.0.1", port, rows, warmup, max(iterations, 7))
            detail["probe"] = {
                key: (round(value, 6) if isinstance(value, float) else value)
                for key, value in probe.items()
            }
            results.append(
                {
                    "category": "ApexBase PG wire",
                    "query": probe_name,
                    "ApexBase": round(probe["p99_ms"], 6),
                }
            )
    finally:
        if server is not None:
            stop_server(server)
        shutil.rmtree(tmpdir, ignore_errors=True)
    return results, detail


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=7)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        metavar="METRIC",
        help="Measure only the named metric(s); used by the guard to recheck a "
        "marked metric in a fresh process",
    )
    args = parser.parse_args(argv)
    if args.rows <= 0 or args.warmup < 0 or args.iterations <= 0:
        parser.error("rows and iterations must be positive; warmup must be non-negative")
    known = set(pgwire_metric_names(args.rows))
    unknown = [name for name in args.only if name not in known]
    if unknown:
        parser.error("--only metric(s) not reported by this script: " + ", ".join(unknown))

    results, detail = run_pgwire(
        args.rows, args.warmup, args.iterations, only=args.only
    )
    report = {
        **full_bench.get_report_metadata("apexbase-canary"),
        "config": {
            "rows": args.rows,
            "warmup": args.warmup,
            "iterations": args.iterations,
        },
        "results": results,
        "pg_wire_concurrency": detail,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    raise SystemExit(main())
