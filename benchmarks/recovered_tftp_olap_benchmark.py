"""Temporary, uncommitted TFTP.csv OLAP comparison against DuckDB.

This file is intentionally disposable.  It builds identical typed native tables
from the same Arrow record batches, validates result parity, and times a broad
set of data-science/data-mining/modeling SQL shapes with balanced engine order.
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import os
import shutil
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

os.environ.setdefault("RAYON_NUM_THREADS", os.environ.get("TFTP_BENCH_THREADS", "10"))

import duckdb
import pyarrow as pa
import pyarrow.csv as arrow_csv

from apexbase import ApexClient


DEFAULT_CSV = Path("/Users/guobingming/Downloads/archive/01-12/TFTP.csv")
DEFAULT_WORKDIR = Path("/tmp/apexbase-tftp-olap")
THREADS = int(os.environ.get("TFTP_BENCH_THREADS", "10"))
FULL_CSV_ROWS = 20_107_827

SOURCE_COLUMNS = (
    "Unnamed: 0",
    "Flow ID",
    " Source IP",
    " Source Port",
    " Destination IP",
    " Destination Port",
    " Protocol",
    " Timestamp",
    " Flow Duration",
    " Total Fwd Packets",
    " Total Backward Packets",
    "Total Length of Fwd Packets",
    " Total Length of Bwd Packets",
    "Flow Bytes/s",
    " Flow Packets/s",
    " Packet Length Mean",
    " Packet Length Std",
    " SYN Flag Count",
    " ACK Flag Count",
    " Inbound",
    " Label",
)
TARGET_COLUMNS = (
    "row_id",
    "flow_id",
    "src_ip",
    "src_port",
    "dst_ip",
    "dst_port",
    "protocol",
    "event_time",
    "flow_duration",
    "fwd_packets",
    "bwd_packets",
    "fwd_bytes",
    "bwd_bytes",
    "flow_bytes_s",
    "flow_packets_s",
    "packet_len_mean",
    "packet_len_std",
    "syn_count",
    "ack_count",
    "inbound",
    "label",
)
STRING_SOURCE_COLUMNS = {
    "Flow ID",
    " Source IP",
    " Destination IP",
    " Timestamp",
    " Label",
}
FLOAT_SOURCE_COLUMNS = {
    "Total Length of Fwd Packets",
    " Total Length of Bwd Packets",
    "Flow Bytes/s",
    " Flow Packets/s",
    " Packet Length Mean",
    " Packet Length Std",
}


@dataclass(frozen=True)
class Query:
    category: str
    name: str
    apex_sql: str
    duck_sql: str | None = None
    approximate: bool = False

    @property
    def duck(self) -> str:
        return self.duck_sql or self.apex_sql


NATIVE_QUERIES = (
    Query("scan", "row_count", "SELECT COUNT(*) AS n FROM flows"),
    Query(
        "descriptive",
        "numeric_profile",
        "SELECT MIN(flow_duration) AS min_duration, MAX(flow_duration) AS max_duration, "
        "AVG(flow_duration) AS avg_duration, SUM(fwd_packets) AS total_fwd, "
        "AVG(packet_len_mean) AS avg_packet FROM flows",
    ),
    Query(
        "data_quality",
        "null_profile",
        "SELECT SUM(CASE WHEN flow_bytes_s IS NULL THEN 1 ELSE 0 END) AS null_flow_bytes, "
        "SUM(CASE WHEN packet_len_std IS NULL THEN 1 ELSE 0 END) AS null_packet_std, "
        "SUM(CASE WHEN src_ip IS NULL THEN 1 ELSE 0 END) AS null_src_ip FROM flows",
    ),
    Query(
        "filter",
        "categorical_filter",
        "SELECT COUNT(*) AS n, AVG(flow_duration) AS avg_duration FROM flows "
        "WHERE protocol=17",
    ),
    Query(
        "filter",
        "compound_range_filter",
        "SELECT COUNT(*) AS n, AVG(flow_packets_s) AS avg_pps, SUM(fwd_bytes) AS bytes "
        "FROM flows WHERE inbound=1 AND flow_duration BETWEEN 1000 AND 1000000",
    ),
    Query(
        "string_mining",
        "ip_prefix_filter",
        "SELECT COUNT(*) AS n, AVG(packet_len_mean) AS avg_packet FROM flows "
        "WHERE src_ip LIKE '172.16.%'",
    ),
    Query(
        "grouping",
        "low_cardinality_group",
        "SELECT protocol, COUNT(*) AS n, AVG(flow_duration) AS avg_duration, "
        "MAX(flow_packets_s) AS peak_pps FROM flows GROUP BY protocol ORDER BY protocol",
    ),
    Query(
        "grouping",
        "two_key_group",
        "SELECT label, protocol, COUNT(*) AS n, AVG(packet_len_mean) AS avg_packet "
        "FROM flows GROUP BY label, protocol ORDER BY label, protocol",
    ),
    Query(
        "grouping",
        "high_cardinality_having",
        "SELECT dst_port, COUNT(*) AS n, AVG(flow_duration) AS avg_duration FROM flows "
        "GROUP BY dst_port HAVING COUNT(*) > 100 ORDER BY n DESC, dst_port LIMIT 100",
    ),
    Query(
        "string_feature",
        "source_prefix_group",
        "SELECT SUBSTR(src_ip,1,7) AS src_prefix, COUNT(*) AS n FROM flows "
        "GROUP BY SUBSTR(src_ip,1,7) ORDER BY n DESC, src_prefix LIMIT 100",
    ),
    Query(
        "cardinality",
        "multi_count_distinct",
        "SELECT COUNT(DISTINCT src_ip) AS src_ips, COUNT(DISTINCT dst_ip) AS dst_ips, "
        "COUNT(DISTINCT dst_port) AS dst_ports FROM flows",
    ),
    Query(
        "feature_engineering",
        "conditional_aggregates",
        "SELECT SUM(CASE WHEN protocol=17 THEN 1 ELSE 0 END) AS udp_rows, "
        "SUM(CASE WHEN protocol=6 THEN 1 ELSE 0 END) AS tcp_rows, "
        "SUM(CASE WHEN inbound=1 THEN 1 ELSE 0 END) AS inbound_rows FROM flows",
    ),
    Query(
        "feature_engineering",
        "duration_binning",
        "SELECT duration_bin, COUNT(*) AS n, AVG(packet_len_mean) AS avg_packet FROM "
        "(SELECT CASE WHEN flow_duration<1000 THEN 'tiny' "
        "WHEN flow_duration<1000000 THEN 'short' ELSE 'long' END AS duration_bin, "
        "packet_len_mean FROM flows) s GROUP BY duration_bin ORDER BY duration_bin",
    ),
    Query(
        "feature_engineering",
        "ratio_projection",
        "SELECT row_id, fwd_bytes/(fwd_packets+1.0) AS bytes_per_fwd, "
        "bwd_bytes/(bwd_packets+1.0) AS bytes_per_bwd FROM flows "
        "WHERE fwd_packets>0 ORDER BY row_id, fwd_bytes DESC, fwd_packets LIMIT 1000",
    ),
    Query(
        "feature_engineering",
        "derived_feature_by_label",
        "SELECT label, AVG(fwd_bytes/(fwd_packets+1.0)) AS avg_fwd_size, "
        "AVG(bwd_bytes/(bwd_packets+1.0)) AS avg_bwd_size FROM flows "
        "GROUP BY label ORDER BY label",
    ),
    Query(
        "outlier_detection",
        "topk_flow_rate",
        "SELECT row_id, src_ip, dst_ip, flow_bytes_s FROM flows "
        "WHERE flow_bytes_s IS NOT NULL ORDER BY flow_bytes_s DESC, row_id LIMIT 100",
    ),
    Query(
        "categorical",
        "distinct_combinations",
        "SELECT DISTINCT label, protocol, inbound FROM flows "
        "ORDER BY label, protocol, inbound",
    ),
    Query(
        "cte",
        "cte_group_filter",
        "WITH stats AS (SELECT label, protocol, COUNT(*) AS n, AVG(flow_duration) AS av "
        "FROM flows GROUP BY label, protocol) SELECT label, protocol, n, av FROM stats "
        "WHERE n>100 ORDER BY av DESC, label, protocol LIMIT 100",
    ),
    Query(
        "subquery",
        "derived_group_filter",
        "SELECT dst_port, n, av FROM (SELECT dst_port, COUNT(*) AS n, "
        "AVG(flow_duration) AS av FROM flows GROUP BY dst_port) s "
        "WHERE n>100 ORDER BY av DESC, dst_port LIMIT 100",
    ),
    Query(
        "statistics",
        "percentiles",
        "SELECT PERCENTILE_APPROX(flow_duration,0.1) AS p10, "
        "PERCENTILE_APPROX(flow_duration,0.5) AS p50, "
        "PERCENTILE_APPROX(flow_duration,0.9) AS p90 FROM flows",
        "SELECT quantile_cont(flow_duration,0.1) AS p10, "
        "quantile_cont(flow_duration,0.5) AS p50, "
        "quantile_cont(flow_duration,0.9) AS p90 FROM flows",
    ),
    Query(
        "data_quality",
        "duplicate_flow_ids",
        "SELECT COUNT(*) AS duplicate_keys FROM (SELECT flow_id FROM flows "
        "GROUP BY flow_id HAVING COUNT(*)>1) d",
    ),
    Query(
        "model_validation",
        "deterministic_split",
        "SELECT MOD(row_id,5) AS fold, COUNT(*) AS n, AVG(flow_duration) AS avg_duration "
        "FROM flows GROUP BY MOD(row_id,5) ORDER BY fold",
    ),
    Query(
        "window",
        "row_number_by_protocol",
        "SELECT row_id, protocol, ROW_NUMBER() OVER "
        "(PARTITION BY protocol ORDER BY flow_duration DESC, row_id) AS rn "
        "FROM flows WHERE row_id<500000 ORDER BY protocol, rn, row_id LIMIT 1000",
    ),
    Query(
        "window",
        "lag_feature",
        "SELECT row_id, protocol, flow_duration, LAG(flow_duration) OVER "
        "(PARTITION BY protocol ORDER BY row_id, flow_duration) AS previous_duration "
        "FROM flows WHERE row_id<500000 ORDER BY protocol, row_id, flow_duration LIMIT 1000",
    ),
    Query(
        "join",
        "dimension_join",
        "SELECT p.protocol_name, COUNT(*) AS n, AVG(f.flow_duration) AS avg_duration "
        "FROM flows f JOIN protocol_dim p ON f.protocol=p.protocol "
        "GROUP BY p.protocol_name ORDER BY p.protocol_name",
    ),
    Query(
        "classification",
        "label_feature_matrix",
        "SELECT label, SUM(CASE WHEN syn_count>0 THEN 1 ELSE 0 END) AS syn_rows, "
        "SUM(CASE WHEN ack_count>0 THEN 1 ELSE 0 END) AS ack_rows, "
        "AVG(flow_packets_s) AS avg_pps FROM flows GROUP BY label ORDER BY label",
    ),
    Query(
        "temporal",
        "hourly_profile",
        "SELECT SUBSTR(event_time,12,2) AS hour, COUNT(*) AS n, "
        "AVG(flow_duration) AS avg_duration FROM flows "
        "GROUP BY SUBSTR(event_time,12,2) ORDER BY hour",
    ),
    Query(
        "frequent_pattern",
        "frequent_source_ips",
        "SELECT src_ip, COUNT(*) AS n, COUNT(DISTINCT dst_port) AS ports FROM flows "
        "GROUP BY src_ip ORDER BY n DESC, src_ip LIMIT 100",
    ),
    Query(
        "selective_topk",
        "filtered_topk",
        "SELECT row_id, dst_port, flow_packets_s FROM flows WHERE protocol=17 AND inbound=1 "
        "ORDER BY flow_packets_s DESC, row_id, dst_port LIMIT 100",
    ),
    Query(
        "wide_aggregation",
        "multi_metric_group",
        "SELECT protocol, inbound, COUNT(*) AS n, SUM(fwd_packets) AS fwd, "
        "SUM(bwd_packets) AS bwd, AVG(flow_duration) AS avg_duration, "
        "MIN(packet_len_mean) AS min_packet, MAX(packet_len_mean) AS max_packet "
        "FROM flows GROUP BY protocol, inbound ORDER BY protocol, inbound",
    ),
)


def raw_source(engine: str, csv_path: Path) -> str:
    if engine == "apex":
        return f"'{csv_path}'"
    return f"read_csv_auto('{csv_path}', header=true)"


def raw_queries(csv_path: Path) -> tuple[Query, ...]:
    a = raw_source("apex", csv_path)
    d = raw_source("duck", csv_path)
    return (
        Query("raw_csv", "raw_count", f"SELECT COUNT(*) AS n FROM {a}", f"SELECT COUNT(*) AS n FROM {d}"),
        Query(
            "raw_csv",
            "raw_numeric_profile",
            f'SELECT COUNT(*) AS n, MIN(Protocol) AS min_protocol, MAX(Protocol) AS max_protocol, '
            f'AVG("Flow Duration") AS avg_duration, MAX("Total Fwd Packets") AS max_fwd FROM {a}',
            f'SELECT COUNT(*) AS n, MIN(Protocol) AS min_protocol, MAX(Protocol) AS max_protocol, '
            f'AVG("Flow Duration") AS avg_duration, MAX("Total Fwd Packets") AS max_fwd FROM {d}',
        ),
        Query(
            "raw_csv",
            "raw_projection_limit",
            f'SELECT "Unnamed: 0" AS row_id, Protocol AS protocol, "Flow Duration" AS duration '
            f'FROM {a} LIMIT 1000',
            f'SELECT "Unnamed: 0" AS row_id, Protocol AS protocol, "Flow Duration" AS duration '
            f'FROM {d} LIMIT 1000',
        ),
        Query(
            "raw_csv",
            "raw_filter_aggregate",
            f'SELECT COUNT(*) AS n, AVG("Flow Duration") AS avg_duration FROM {a} WHERE Protocol=17',
            f'SELECT COUNT(*) AS n, AVG("Flow Duration") AS avg_duration FROM {d} WHERE Protocol=17',
        ),
        Query(
            "raw_csv",
            "raw_group_label",
            f'SELECT Label AS label, COUNT(*) AS n FROM {a} GROUP BY Label ORDER BY label',
            f'SELECT Label AS label, COUNT(*) AS n FROM {d} GROUP BY Label ORDER BY label',
        ),
    )


def arrow_reader(csv_path: Path):
    column_types = {}
    for name in SOURCE_COLUMNS:
        if name in STRING_SOURCE_COLUMNS:
            column_types[name] = pa.string()
        elif name in FLOAT_SOURCE_COLUMNS:
            column_types[name] = pa.float64()
        else:
            column_types[name] = pa.int64()
    return arrow_csv.open_csv(
        csv_path,
        read_options=arrow_csv.ReadOptions(block_size=64 << 20, use_threads=True),
        convert_options=arrow_csv.ConvertOptions(
            include_columns=list(SOURCE_COLUMNS),
            column_types=column_types,
            strings_can_be_null=True,
        ),
    )


def setup_tables(csv_path: Path, workdir: Path, max_rows: int, reset: bool) -> dict:
    manifest_path = workdir / "manifest.json"
    if reset and workdir.exists():
        shutil.rmtree(workdir)
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        expected = max_rows or FULL_CSV_ROWS
        if setup_matches(manifest, csv_path, max_rows):
            print(f"reuse setup: {expected:,} rows in {workdir}", flush=True)
            return manifest
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    apex = ApexClient(str(workdir / "apex"), drop_if_exists=True)
    duck = duckdb.connect(str(workdir / "duckdb.db"))
    duck.execute(f"SET threads TO {THREADS}")
    duck.execute("SET memory_limit='8GB'")
    reader = arrow_reader(csv_path)
    rows = 0
    batch_no = 0
    started = time.perf_counter()
    try:
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break
            if max_rows and rows + batch.num_rows > max_rows:
                batch = batch.slice(0, max_rows - rows)
            if batch.num_rows == 0:
                break
            table = pa.Table.from_batches([batch.rename_columns(TARGET_COLUMNS)])
            row_id_index = table.schema.get_field_index("row_id")
            table = table.set_column(
                row_id_index,
                "row_id",
                pa.array(range(rows, rows + table.num_rows), type=pa.int64()),
            )
            if batch_no == 0:
                apex.from_pyarrow(table, table_name="flows")
                duck.register("_incoming", table)
                duck.execute("CREATE TABLE flows AS SELECT * FROM _incoming")
            else:
                apex.store(table)
                duck.register("_incoming", table)
                duck.execute("INSERT INTO flows SELECT * FROM _incoming")
            apex.flush()
            rows += table.num_rows
            batch_no += 1
            if batch_no % 10 == 0 or (max_rows and rows >= max_rows):
                elapsed = time.perf_counter() - started
                print(f"setup {rows:,} rows ({rows/max(elapsed, 1):,.0f} rows/s)", flush=True)
            if max_rows and rows >= max_rows:
                break

        protocol_dim = pa.table(
            {"protocol": pa.array([0, 6, 17], type=pa.int64()), "protocol_name": ["OTHER", "TCP", "UDP"]}
        )
        apex.from_pyarrow(protocol_dim, table_name="protocol_dim")
        apex.flush()
        duck.register("_protocol_dim", protocol_dim)
        duck.execute("CREATE TABLE protocol_dim AS SELECT * FROM _protocol_dim")
        duck.execute("CHECKPOINT")
        apex_count = apex.execute("SELECT COUNT(*) AS n FROM flows").scalar()
        duck_count = duck.execute("SELECT COUNT(*) FROM flows").fetchone()[0]
        if apex_count != rows or duck_count != rows:
            raise RuntimeError(f"setup count mismatch: rows={rows}, apex={apex_count}, duck={duck_count}")
    finally:
        duck.close()
        apex.close()

    manifest = {
        "rows": rows,
        "source": str(csv_path),
        "source_size": csv_path.stat().st_size,
        "source_mtime_ns": csv_path.stat().st_mtime_ns,
        "threads": THREADS,
        "setup_seconds": time.perf_counter() - started,
        "columns": list(TARGET_COLUMNS),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"setup complete: {rows:,} rows in {manifest['setup_seconds']:.2f}s", flush=True)
    return manifest


def setup_matches(manifest: dict, csv_path: Path, max_rows: int) -> bool:
    """Return whether a persisted setup covers the requested source and row count."""
    stat = csv_path.stat()
    return (
        manifest.get("rows") == (max_rows or FULL_CSV_ROWS)
        and manifest.get("source_size") == stat.st_size
        and manifest.get("source_mtime_ns") == stat.st_mtime_ns
    )


def comparable_value(value):
    if isinstance(value, float):
        if math.isnan(value):
            return ("nan",)
        if math.isinf(value):
            return ("inf", 1 if value > 0 else -1)
    return value


def tables_match(left: pa.Table, right: pa.Table, approximate: bool) -> tuple[bool, str]:
    if left.num_rows != right.num_rows or left.num_columns != right.num_columns:
        return False, f"shape apex={left.shape} duck={right.shape}"
    left_rows = left.to_pylist()
    right_rows = right.to_pylist()
    if left.column_names != right.column_names:
        return False, f"columns apex={left.column_names} duck={right.column_names}"
    for row_index, (a_row, d_row) in enumerate(zip(left_rows, right_rows)):
        for name in left.column_names:
            a = comparable_value(a_row[name])
            d = comparable_value(d_row[name])
            if isinstance(a, (int, float)) and isinstance(d, (int, float)):
                tolerance = 0.02 if approximate else 1e-7
                if not math.isclose(float(a), float(d), rel_tol=tolerance, abs_tol=1e-6):
                    return False, f"row {row_index} {name}: apex={a!r} duck={d!r}"
            elif a != d:
                return False, f"row {row_index} {name}: apex={a!r} duck={d!r}"
    return True, "ok"


def balanced_order(iterations: int) -> list[str]:
    if iterations == 3:
        return ["apex", "duck", "duck", "apex", "apex", "duck"]
    if iterations == 5:
        return ["apex", "duck", "duck", "apex", "apex", "duck", "duck", "apex", "apex", "duck"]
    return [engine for _ in range(iterations) for engine in ("apex", "duck")]


def run_suite(
    queries: tuple[Query, ...],
    apex_run: Callable[[str], pa.Table],
    duck_run: Callable[[str], pa.Table],
    warmups: int,
    iterations: int,
) -> list[dict]:
    results = []
    print("\n" + f"{'query':34s} {'apex ms':>11s} {'duck ms':>11s} {'speedup':>9s} status", flush=True)
    print("-" * 88, flush=True)
    for query in queries:
        try:
            a_result = apex_run(query.apex_sql)
            d_result = duck_run(query.duck)
            parity, detail = tables_match(a_result, d_result, query.approximate)
            if not parity:
                print(f"{query.name:34s} {'-':>11s} {'-':>11s} {'-':>9s} MISMATCH {detail}", flush=True)
                results.append({"query": query.name, "category": query.category, "status": "mismatch", "detail": detail})
                continue
            for _ in range(warmups):
                apex_run(query.apex_sql)
                duck_run(query.duck)
            samples = {"apex": [], "duck": []}
            for engine in balanced_order(iterations):
                started = time.perf_counter_ns()
                if engine == "apex":
                    apex_run(query.apex_sql)
                else:
                    duck_run(query.duck)
                samples[engine].append((time.perf_counter_ns() - started) / 1_000_000)
            apex_ms = statistics.median(samples["apex"])
            duck_ms = statistics.median(samples["duck"])
            speedup = duck_ms / apex_ms
            status = "APEX" if speedup > 1.0 else "DUCK"
            print(f"{query.name:34s} {apex_ms:11.3f} {duck_ms:11.3f} {speedup:8.2f}x {status}", flush=True)
            results.append(
                {
                    "query": query.name,
                    "category": query.category,
                    "status": "ok",
                    "winner": status.lower(),
                    "apex_ms": apex_ms,
                    "duck_ms": duck_ms,
                    "speedup": speedup,
                    "apex_samples_ms": samples["apex"],
                    "duck_samples_ms": samples["duck"],
                }
            )
        except Exception as exc:
            print(f"{query.name:34s} {'-':>11s} {'-':>11s} {'-':>9s} ERROR {exc!r}", flush=True)
            results.append({"query": query.name, "category": query.category, "status": "error", "detail": repr(exc)})
        gc.collect()
    wins = sum(r.get("winner") == "apex" for r in results)
    losses = sum(r.get("winner") == "duck" for r in results)
    invalid = len(results) - wins - losses
    print(f"\nscore: ApexBase {wins}, DuckDB {losses}, invalid {invalid}", flush=True)
    return results


def native_benchmark(workdir: Path, warmups: int, iterations: int) -> list[dict]:
    apex = ApexClient(str(workdir / "apex"))
    apex.use_table("flows")
    duck = duckdb.connect(str(workdir / "duckdb.db"), read_only=True)
    duck.execute(f"SET threads TO {THREADS}")
    duck.execute("SET memory_limit='8GB'")
    try:
        return run_suite(
            NATIVE_QUERIES,
            lambda sql: apex.execute(sql).to_arrow(),
            lambda sql: duck.execute(sql).fetch_arrow_table(),
            warmups,
            iterations,
        )
    finally:
        duck.close()
        apex.close()


def memory_benchmark(workdir: Path, warmups: int, iterations: int) -> tuple[list[dict], float]:
    """Load the identical DuckDB-native Arrow batches into ApexBase ``:memory:``."""
    duck = duckdb.connect(str(workdir / "duckdb.db"), read_only=True)
    duck.execute(f"SET threads TO {THREADS}")
    duck.execute("SET memory_limit='8GB'")
    apex = ApexClient(":memory:")
    started = time.perf_counter()
    first = True
    try:
        reader = duck.execute("SELECT * FROM flows").fetch_record_batch(250_000)
        for batch in reader:
            table = pa.Table.from_batches([batch])
            if first:
                apex.from_pyarrow(table, table_name="flows")
                first = False
            else:
                apex.store(table)
            apex.flush()
        protocol_dim = duck.execute("SELECT * FROM protocol_dim ORDER BY protocol").fetch_arrow_table()
        apex.from_pyarrow(protocol_dim, table_name="protocol_dim")
        apex.flush()
        apex.use_table("flows")
        load_seconds = time.perf_counter() - started
        print(f"ApexBase :memory: load complete in {load_seconds:.2f}s", flush=True)
        results = run_suite(
            NATIVE_QUERIES,
            lambda sql: apex.execute(sql).to_arrow(),
            lambda sql: duck.execute(sql).fetch_arrow_table(),
            warmups,
            iterations,
        )
        return results, load_seconds
    finally:
        duck.close()
        apex.close()


def raw_benchmark(csv_path: Path, workdir: Path, warmups: int, iterations: int) -> list[dict]:
    apex = ApexClient(str(workdir / "apex_raw"), drop_if_exists=True)
    duck = duckdb.connect(":memory:")
    duck.execute(f"SET threads TO {THREADS}")
    duck.execute("SET memory_limit='8GB'")
    try:
        return run_suite(
            raw_queries(csv_path),
            lambda sql: apex.execute(sql).to_arrow(),
            lambda sql: duck.execute(sql).fetch_arrow_table(),
            warmups,
            iterations,
        )
    finally:
        duck.close()
        apex.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("setup", "native", "memory", "raw", "all", "cleanup"))
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--workdir", type=Path, default=DEFAULT_WORKDIR)
    parser.add_argument("--max-rows", type=int, default=0, help="0 means the full CSV")
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    if args.phase == "cleanup":
        if args.workdir.exists():
            shutil.rmtree(args.workdir)
        print(f"removed {args.workdir}")
        return
    if not args.csv.is_file():
        raise FileNotFoundError(args.csv)
    if args.warmups < 0 or args.iterations < 1:
        raise ValueError("warmups must be >=0 and iterations must be >=1")

    manifest = None
    if args.phase in ("setup", "native", "memory", "all"):
        manifest_path = args.workdir / "manifest.json"
        if args.phase != "setup" and not args.reset and args.max_rows == 0 and manifest_path.exists():
            candidate = json.loads(manifest_path.read_text())
            if setup_matches(candidate, args.csv, args.max_rows):
                manifest = candidate
                print(f"reuse setup: {manifest['rows']:,} rows in {args.workdir}", flush=True)
        if manifest is None:
            manifest = setup_tables(args.csv, args.workdir, args.max_rows, args.reset)
    results = {}
    if args.phase in ("native", "all"):
        results["native"] = native_benchmark(args.workdir, args.warmups, args.iterations)
    if args.phase in ("memory", "all"):
        results["memory"], memory_load_seconds = memory_benchmark(
            args.workdir, args.warmups, args.iterations
        )
        results["memory_load_seconds"] = memory_load_seconds
    if args.phase in ("raw", "all"):
        args.workdir.mkdir(parents=True, exist_ok=True)
        results["raw"] = raw_benchmark(args.csv, args.workdir, args.warmups, args.iterations)
    if results:
        payload = {"manifest": manifest, "threads": THREADS, "results": results}
        result_path = args.workdir / "latest-results.json"
        result_path.write_text(json.dumps(payload, indent=2, allow_nan=True) + "\n")
        print(f"results: {result_path}")


if __name__ == "__main__":
    main()
