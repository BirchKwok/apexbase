"""
ApexBase Performance Benchmark: ApexBase vs SQLite vs DuckDB

Measures OLAP and OLTP operations across all three engines on the same
dataset. Default fair rankings use normal engine APIs and comparable
materialization semantics; tunable or opt-in paths are shown in metric labels.
Results are printed as formatted tables and optionally saved to JSON.

Usage:
    python benchmarks/bench_vs_sqlite_duckdb.py [--rows N] [--warmup N] [--iterations N] [--output FILE]
"""

import argparse
import csv as csv_mod
import gc
import importlib.metadata as importlib_metadata
import json
import math
import os
import platform
import random
import shutil
import sqlite3
import statistics
import string
import subprocess
import sys
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import numpy as np

# ---------------------------------------------------------------------------
# Optional imports
# ---------------------------------------------------------------------------
duckdb = None
ApexClient = None
pd = None
pa = None
HAS_DUCKDB = False
HAS_APEXBASE = False
HAS_PANDAS = False
HAS_PYARROW = False
_OPTIONAL_IMPORTS_READY = False


def ensure_optional_imports():
    """Load benchmark-only dependencies lazily so profile tests stay cheap."""
    global duckdb, ApexClient, pd, pa
    global HAS_DUCKDB, HAS_APEXBASE, HAS_PANDAS, HAS_PYARROW
    global _OPTIONAL_IMPORTS_READY

    if _OPTIONAL_IMPORTS_READY:
        return

    try:
        import duckdb as _duckdb
        duckdb = _duckdb
        HAS_DUCKDB = True
    except ImportError:
        duckdb = None
        HAS_DUCKDB = False

    try:
        from apexbase import ApexClient as _ApexClient
        ApexClient = _ApexClient
        HAS_APEXBASE = True
    except ImportError:
        ApexClient = None
        HAS_APEXBASE = False

    try:
        import pandas as _pd
        pd = _pd
        HAS_PANDAS = True
    except ImportError:
        pd = None
        HAS_PANDAS = False

    try:
        import pyarrow as _pa
        pa = _pa
        HAS_PYARROW = True
    except ImportError:
        pa = None
        HAS_PYARROW = False

    _OPTIONAL_IMPORTS_READY = True

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CITIES = ["Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou",
          "Nanjing", "Chengdu", "Wuhan", "Xian", "Qingdao"]
CATEGORIES = ["Electronics", "Clothing", "Food", "Sports", "Books",
              "Home", "Auto", "Health", "Travel", "Gaming"]
META_CITY_POP = [
    ("Beijing", 21_540_000), ("Shanghai", 24_870_000),
    ("Guangzhou", 18_680_000), ("Shenzhen", 17_680_000),
    ("Hangzhou", 10_360_000), ("Nanjing", 9_490_000),
    ("Chengdu", 20_940_000), ("Wuhan", 13_690_000),
    ("Xian", 13_150_000), ("Qingdao", 9_480_000),
]
TXN_BACKLOG_ROWS = 1500
TXN_BACKLOG_MISSING_NAME = "__txn_backlog_missing__"
MICROBENCH_TARGET_SAMPLE_NS = 2_000_000
MICROBENCH_MAX_REPEATS = 4096
MICROBENCH_CALIBRATION_TRIALS = 5
VECTOR_DIM_DEFAULT = 128
VECTOR_K_DEFAULT = 10
VECTOR_BATCH_QUERY_COUNT = 10
VECTOR_ROWS_DEFAULT = 1_000_000
VECTOR_HEAD_TO_HEAD_METRICS = [
    ("TopK L2", "l2"),
    ("TopK Cosine", "cosine"),
    ("TopK Dot", "dot"),
]
VECTOR_BATCH_METRICS = [
    ("Batch TopK L2 (10 queries)", "l2"),
    ("Batch TopK Cosine (10 queries)", "cosine"),
    ("Batch TopK Dot (10 queries)", "dot"),
]
VECTOR_APEX_ONLY_METRICS = [
    ("TopK L2 squared", "l2_squared"),
    ("TopK L1", "l1"),
    ("TopK Linf", "linf"),
]
VECTOR_APEX_METRIC_MAP = {
    "l2": "l2",
    "cosine": "cosine_distance",
    "dot": "dot",
    "l2_squared": "l2_squared",
    "l1": "l1",
    "linf": "linf",
}
VECTOR_DUCKDB_FUNCTIONS = {
    "l2": "array_distance",
    "cosine": "array_cosine_distance",
    "dot": "array_negative_inner_product",
}
VECTOR_SQLITE_NOTE = (
    "Stock SQLite in this harness has no native vector distance/top-k, "
    "so ranked vector comparisons are ApexBase vs DuckDB only."
)
PROFILE_PUBLIC = "public"
PROFILE_EXTENDED = "extended"
PROFILE_CHOICES = (PROFILE_PUBLIC, PROFILE_EXTENDED)
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PUBLIC_VECTOR_HEAD_TO_HEAD_METRICS = VECTOR_HEAD_TO_HEAD_METRICS
PUBLIC_VECTOR_BATCH_METRICS = VECTOR_BATCH_METRICS
PUBLIC_VECTOR_APEX_ONLY_METRICS = []
OLTP_ONE_ROW_DICT = {
    "name": "oltp_one",
    "age": 31,
    "score": 77.0,
    "city": "Beijing",
    "category": "Books",
}
OLTP_ONE_ROW_TUPLE = ("oltp_one", 31, 77.0, "Beijing", "Books")


def generate_data(n: int):
    """Generate test data as columnar dict."""
    rng = random.Random(42)
    names = [f"user_{i}" for i in range(n)]
    ages = [rng.randint(18, 80) for _ in range(n)]
    scores = [round(rng.uniform(0, 100), 2) for _ in range(n)]
    cities = [rng.choice(CITIES) for _ in range(n)]
    categories = [rng.choice(CATEGORIES) for _ in range(n)]
    return {
        "name": names,
        "age": ages,
        "score": scores,
        "city": cities,
        "category": categories,
    }


def generate_benchmark_files(tmpdir, data):
    """Generate CSV, Parquet, and NDJSON test files from benchmark data."""
    ensure_optional_imports()
    csv_path = os.path.join(tmpdir, "bench_data.csv")
    parquet_path = os.path.join(tmpdir, "bench_data.parquet")
    json_path = os.path.join(tmpdir, "bench_data.jsonl")
    n = len(data["name"])
    columns = ["name", "age", "score", "city", "category"]

    with open(csv_path, "w", newline="") as f:
        writer = csv_mod.writer(f)
        writer.writerow(columns)
        for i in range(n):
            writer.writerow([data[col][i] for col in columns])

    if HAS_PANDAS:
        df = pd.DataFrame(data)
        df.to_parquet(parquet_path, index=False)
    elif HAS_PYARROW:
        import pyarrow.parquet as pq
        table = pa.table(data)
        pq.write_table(table, parquet_path)

    with open(json_path, "w") as f:
        for i in range(n):
            row = {col: data[col][i] for col in columns}
            json.dump(row, f)
            f.write("\n")

    return csv_path, parquet_path, json_path


def build_shared_inputs(n: int):
    """Build deterministic shared benchmark inputs used by all engines."""
    if n <= 0:
        return {"point_lookup_id": 0, "retrieve_many_ids": []}

    rng = random.Random(20260416)
    point_lookup_id = min(5000, n)
    sample_size = min(100, n)
    retrieve_many_ids = rng.sample(range(1, n + 1), sample_size)
    return {
        "point_lookup_id": point_lookup_id,
        "retrieve_many_ids": retrieve_many_ids,
    }


def generate_vector_data(n: int, dim: int, seed: int = 42):
    """Generate deterministic vector benchmark data."""
    rng = np.random.default_rng(seed)
    vecs = rng.random((n, dim), dtype=np.float32)
    query = rng.random(dim, dtype=np.float32)
    batch_queries = rng.random((VECTOR_BATCH_QUERY_COUNT, dim), dtype=np.float32)
    return vecs, query, batch_queries


def rows_to_dicts(columns, rows):
    """Materialize rows into a consistent Python representation."""
    if not rows:
        return []
    if not columns:
        return list(rows)
    return [dict(zip(columns, row)) for row in rows]


def default_vector_rows(base_rows: int) -> int:
    """Default vector benchmark rows: run the separate vector module at 1M rows."""
    return max(max(1, base_rows), VECTOR_ROWS_DEFAULT)


def normalize_profile(profile: str) -> str:
    return profile if profile in PROFILE_CHOICES else PROFILE_PUBLIC


def vector_metric_sets(profile: str = PROFILE_EXTENDED):
    if normalize_profile(profile) == PROFILE_PUBLIC:
        return (
            PUBLIC_VECTOR_HEAD_TO_HEAD_METRICS,
            PUBLIC_VECTOR_BATCH_METRICS,
            PUBLIC_VECTOR_APEX_ONLY_METRICS,
        )
    return (
        VECTOR_HEAD_TO_HEAD_METRICS,
        VECTOR_BATCH_METRICS,
        VECTOR_APEX_ONLY_METRICS,
    )


def vector_metric_count(profile: str = PROFILE_EXTENDED):
    head_metrics, batch_metrics, apex_only_metrics = vector_metric_sets(profile)
    return (
        len(head_metrics)
        + len(batch_metrics)
        + len(apex_only_metrics)
    )


def display_only_specs(labels):
    """Build print_benchmark_section-compatible specs for precomputed rows."""
    return [(label, "", False, False, False, None) for label in labels]


def vector_query_sql_literal(query: np.ndarray) -> str:
    values = np.asarray(query, dtype=np.float32).reshape(-1)
    return ",".join(f"{float(v):.6f}" for v in values)


def build_duckdb_vector_sql(query: np.ndarray, k: int, metric: str) -> str:
    func = VECTOR_DUCKDB_FUNCTIONS.get(metric)
    if func is None:
        raise ValueError(f"DuckDB vector SQL does not support metric '{metric}'")

    dim = int(np.asarray(query).size)
    q_cast = f"[{vector_query_sql_literal(query)}]::FLOAT[{dim}]"
    return f"SELECT id, {func}(vec, {q_cast}) AS dist FROM vecs ORDER BY dist LIMIT {k}"


def materialize_apex_vector_result(result_view):
    ensure_optional_imports()
    if HAS_PYARROW:
        return result_view.to_arrow()
    return result_view.to_dict()


def materialize_duckdb_vector_result(cursor):
    ensure_optional_imports()
    if HAS_PYARROW:
        return cursor.fetch_arrow_table()
    return cursor.fetchall()


def setup_apex_vector_bench(base_tmpdir: str, vecs: np.ndarray):
    ensure_optional_imports()
    vector_dir = os.path.join(base_tmpdir, "vector_apex")
    client = ApexClient(vector_dir, drop_if_exists=True)
    client.create_table("vecs")
    client.use_table("vecs")

    batch_size = 10_000
    for start in range(0, len(vecs), batch_size):
        end = min(start + batch_size, len(vecs))
        rows = [{"id": i, "vec": vecs[i]} for i in range(start, end)]
        client.store(rows)
    client.flush()
    return client


def setup_duckdb_vector_bench(vecs: np.ndarray):
    ensure_optional_imports()
    con = duckdb.connect(":memory:")
    dim = vecs.shape[1]
    ids = np.arange(len(vecs), dtype=np.int32)
    df = pd.DataFrame({"id": ids, "vec": list(vecs)})
    con.register("_vecs_src", df)
    con.execute(f"CREATE TABLE vecs AS SELECT id, vec::FLOAT[{dim}] AS vec FROM _vecs_src")
    try:
        con.unregister("_vecs_src")
    except Exception:
        con.execute("DROP VIEW IF EXISTS _vecs_src")
    return con


def bench_apex_vector_query(client, query: np.ndarray, k: int, metric: str):
    return materialize_apex_vector_result(
        client.topk_distance("vec", query, k=k, metric=VECTOR_APEX_METRIC_MAP[metric])
    )


def bench_duckdb_vector_query(connection, query: np.ndarray, k: int, metric: str):
    return materialize_duckdb_vector_result(
        connection.execute(build_duckdb_vector_sql(query, k, metric))
    )


def bench_apex_batch_vector_query(client, queries: np.ndarray, k: int, metric: str):
    return client.batch_topk_distance("vec", queries, k=k, metric=VECTOR_APEX_METRIC_MAP[metric])


def bench_duckdb_batch_vector_query(connection, queries: np.ndarray, k: int, metric: str):
    last = None
    for query in queries:
        last = materialize_duckdb_vector_result(
            connection.execute(build_duckdb_vector_sql(query, k, metric))
        )
    return last


@contextmanager
def timer():
    """Context manager that yields a dict; sets 'elapsed_ms' on exit."""
    result = {}
    gc.collect()
    t0 = time.perf_counter()
    yield result
    result["elapsed_ms"] = (time.perf_counter() - t0) * 1000


def fmt_ms(ms):
    if ms < 0.01:
        return f"{ms * 1000:.2f}us"
    if ms < 1:
        return f"{ms:.3f}ms"
    if ms < 1000:
        return f"{ms:.2f}ms"
    return f"{ms / 1000:.2f}s"


def run_bench(fn, warmup=2, iterations=5):
    """Run fn() with warmup, return average ms."""
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(iterations):
        with timer() as t:
            fn()
        times.append(t["elapsed_ms"])
    return sum(times) / len(times)


def run_bench_cold(setup_fn, bench_fn, warmup=2, iterations=5):
    """Cold-start benchmark: re-run setup_fn() before EVERY iteration.
    Measures true from-scratch query latency (no warm caches)."""
    for _ in range(warmup):
        setup_fn()
        bench_fn()
    times = []
    for _ in range(iterations):
        setup_fn()   # cold restart — invalidates all in-process caches
        gc.collect()
        with timer() as t:
            bench_fn()
        times.append(t["elapsed_ms"])
    return sum(times) / len(times)


def run_bench_cold_nogc(setup_fn, bench_fn, warmup=2, iterations=5):
    """Cold-start benchmark WITHOUT gc.collect() before timing.
    Measures file-open + query latency without CPU-cache eviction noise.

    Sub-millisecond cold queries are timed in calibrated batches so one
    scheduler interruption cannot dominate a complete benchmark sample.  The
    setup still runs before every logical call and remains outside the timed
    region, preserving cold-query semantics.
    """
    for _ in range(warmup):
        setup_fn()
        bench_fn()

    repeats = 1
    best_single_ns = None
    for _ in range(MICROBENCH_CALIBRATION_TRIALS):
        setup_fn()
        t0 = time.perf_counter_ns()
        bench_fn()
        elapsed_ns = time.perf_counter_ns() - t0
        if best_single_ns is None or elapsed_ns < best_single_ns:
            best_single_ns = elapsed_ns

    if best_single_ns is not None and best_single_ns > 0:
        while (best_single_ns * repeats < MICROBENCH_TARGET_SAMPLE_NS
               and repeats < MICROBENCH_MAX_REPEATS):
            repeats *= 2

    times = []
    for _ in range(iterations):
        elapsed_ns = 0
        for _ in range(repeats):
            setup_fn()
            t0 = time.perf_counter_ns()
            bench_fn()
            elapsed_ns += time.perf_counter_ns() - t0
        times.append(elapsed_ns / (1_000_000 * repeats))
    return sum(times) / len(times)


def run_bench_nogc(fn, warmup=2, iterations=5):
    """Warm benchmark WITHOUT gc.collect() before timing."""
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return sum(times) / len(times)


def run_bench_nogc_median(fn, warmup=2, iterations=5):
    """Warm microbenchmark WITHOUT gc.collect(); returns median per-call latency.

    Ultra-fast Python call paths are vulnerable to timer granularity and
    scheduler jitter when measuring a single invocation per sample. Calibrate a
    symmetric repeat count for all engines, time a short batch, then divide by
    the repeat count so the reported number still reflects one logical call.
    """
    call = fn
    repeats = 1

    # Prime/cache the path and pick a batch size that lifts each timed sample
    # well above timer noise while preserving per-call semantics.
    best_single_ns = None
    for _ in range(MICROBENCH_CALIBRATION_TRIALS):
        t0 = time.perf_counter_ns()
        call()
        elapsed_ns = time.perf_counter_ns() - t0
        if best_single_ns is None or elapsed_ns < best_single_ns:
            best_single_ns = elapsed_ns

    if best_single_ns is not None and best_single_ns > 0:
        while (best_single_ns * repeats < MICROBENCH_TARGET_SAMPLE_NS
               and repeats < MICROBENCH_MAX_REPEATS):
            repeats *= 2

    for _ in range(warmup):
        for _ in range(repeats):
            call()
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        for _ in range(repeats):
            call()
        times.append((time.perf_counter_ns() - t0) / (1_000_000 * repeats))
    return statistics.median(times)


def run_bench_gc_median(fn, warmup=2, iterations=5):
    """Run fn() with explicit gc before each timed iteration and return median ms."""
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(iterations):
        gc.collect()
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return statistics.median(times)


def run_bench_with_setup(setup_fn, bench_fn, warmup=2, iterations=5):
    """Per-iteration setup without DB reopen; return median bench_fn latency."""
    for _ in range(warmup):
        setup_fn()
        bench_fn()
    times = []
    for _ in range(iterations):
        setup_fn()
        t0 = time.perf_counter()
        bench_fn()
        times.append((time.perf_counter() - t0) * 1000)
    return statistics.median(times)


def measure_rss_mb():
    """Return current process RSS in MB (requires psutil)."""
    try:
        import psutil
        return psutil.Process().memory_info().rss / (1024 * 1024)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# SQLite benchmark
# ---------------------------------------------------------------------------

class SQLiteBench:
    def __init__(self, tmpdir, data):
        self.db_path = os.path.join(tmpdir, "bench.db")
        self.data = data
        self.n = len(data["name"])
        self.conn = None
        self.shared_inputs = build_shared_inputs(self.n)
        self._txn_counter = 0
        self._txn_backlog_ready = False

    def _connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=OFF")

    def _query_all(self, sql, params=()):
        cur = self.conn.execute(sql, params)
        return rows_to_dicts([d[0] for d in (cur.description or [])], cur.fetchall())

    def _scalar(self, sql, params=()):
        return self.conn.execute(sql, params).fetchone()[0]

    def _query_pandas(self, sql):
        ensure_optional_imports()
        if HAS_PANDAS:
            return pd.read_sql(sql, self.conn)
        return self._query_all(sql)

    def execute_materialized_query(self, sql):
        return self._query_all(sql)

    def setup(self):
        if self.conn:
            self.conn.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self._connect()
        self.conn.execute("""
            CREATE TABLE bench (
                _id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                age INTEGER,
                score REAL,
                city TEXT,
                category TEXT
            )
        """)
        self.conn.execute("CREATE TABLE meta (city TEXT, pop INTEGER)")
        self.conn.executemany("INSERT INTO meta VALUES (?,?)", META_CITY_POP)
        self._txn_counter = 0
        self._txn_backlog_ready = False

    def cold_start_setup(self):
        if self.conn:
            self.conn.close()
        self._connect()

    def bench_insert(self):
        rows = list(zip(
            self.data["name"], self.data["age"], self.data["score"],
            self.data["city"], self.data["category"]
        ))
        self.conn.executemany(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            rows,
        )
        self.conn.commit()

    def bench_count(self):
        return self._scalar("SELECT COUNT(*) FROM bench")

    def bench_select_limit(self, limit=100):
        return self._query_all(f"SELECT * FROM bench LIMIT {limit}")

    def bench_select_limit_10k(self):
        return self._query_all("SELECT * FROM bench LIMIT 10000")

    def bench_projected_full_scan(self):
        return self._query_all("SELECT name, age, city FROM bench")

    def bench_filtered_limit_100(self):
        return self._query_all("SELECT * FROM bench WHERE age > 30 LIMIT 100")

    def bench_limit_offset_100(self):
        return self._query_all("SELECT * FROM bench LIMIT 100 OFFSET 10000")

    def bench_filter_string(self):
        return self._query_all(
            "SELECT * FROM bench WHERE name = 'user_5000'"
        )

    def bench_filter_range(self):
        return self._query_all(
            "SELECT * FROM bench WHERE age BETWEEN 25 AND 35"
        )

    def bench_group_by(self):
        return self._query_all(
            "SELECT city, COUNT(*), AVG(score) FROM bench GROUP BY city"
        )

    def bench_group_by_category(self):
        return self._query_all(
            "SELECT category, COUNT(*), AVG(score) FROM bench GROUP BY category"
        )

    def bench_group_by_order_count(self):
        return self._query_all(
            "SELECT city, COUNT(*) AS cnt FROM bench GROUP BY city ORDER BY cnt DESC LIMIT 5"
        )

    def bench_group_by_category_order_count(self):
        return self._query_all(
            "SELECT category, COUNT(*) AS cnt FROM bench GROUP BY category ORDER BY cnt DESC LIMIT 5"
        )

    def bench_group_by_having(self):
        return self._query_all(
            "SELECT city, COUNT(*) as cnt, AVG(score) FROM bench GROUP BY city HAVING cnt > 1000"
        )

    def bench_group_by_category_having(self):
        return self._query_all(
            "SELECT category, COUNT(*) as cnt, AVG(score) FROM bench GROUP BY category HAVING cnt > 1000"
        )

    def setup_view_bench(self):
        self.conn.execute("DROP VIEW IF EXISTS bench_view")
        self.conn.execute(
            "CREATE VIEW bench_view AS "
            "SELECT city, COUNT(*) AS cnt, AVG(score) AS avg_score "
            "FROM bench GROUP BY city"
        )
        self.conn.commit()

    def bench_view_select(self):
        return self._query_all(
            "SELECT city, cnt FROM bench_view WHERE cnt > 0 ORDER BY cnt DESC LIMIT 5"
        )

    def bench_order_limit(self):
        return self._query_all(
            "SELECT * FROM bench ORDER BY score DESC LIMIT 100"
        )

    def bench_order_limit_asc(self):
        return self._query_all(
            "SELECT * FROM bench ORDER BY score ASC LIMIT 100"
        )

    def bench_aggregation(self):
        return self._query_all(
            "SELECT COUNT(*), AVG(age), SUM(score), MIN(age), MAX(age) FROM bench"
        )

    def bench_filtered_aggregation(self):
        return self._query_all(
            "SELECT COUNT(*), AVG(score), MAX(score) FROM bench WHERE category = 'Electronics'"
        )

    def bench_filtered_aggregation_city(self):
        return self._query_all(
            "SELECT COUNT(*), AVG(score), MAX(score) FROM bench WHERE city = 'Beijing'"
        )

    def bench_count_where_category(self):
        return self._scalar(
            "SELECT COUNT(*) FROM bench WHERE category = 'Electronics'"
        )

    def bench_complex(self):
        return self._query_all(
            "SELECT city, AVG(score) as avg_s FROM bench WHERE age BETWEEN 25 AND 50 GROUP BY city ORDER BY avg_s DESC LIMIT 5"
        )

    def bench_point_lookup(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        return self._query_all(
            "SELECT * FROM bench WHERE _id = ?",
            (point_lookup_id,),
        )

    def bench_oltp_projected_point_lookup(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        return self._query_all(
            "SELECT name, age, score FROM bench WHERE _id = ?",
            (point_lookup_id,),
        )

    def bench_oltp_count_rows(self):
        return self._scalar("SELECT COUNT(*) FROM bench")

    def bench_oltp_direct_point_lookup(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        return self._query_all("SELECT * FROM bench WHERE _id = ?", (point_lookup_id,))

    def bench_oltp_missing_point_lookup(self):
        missing_id = self.n + 100_000_000
        return self._query_all("SELECT * FROM bench WHERE _id = ?", (missing_id,))

    def bench_retrieve_many(self):
        ids = self.shared_inputs["retrieve_many_ids"]
        placeholders = ",".join(["?"] * len(ids))
        return self._query_all(
            f"SELECT * FROM bench WHERE _id IN ({placeholders})",
            ids,
        )

    def bench_oltp_projected_retrieve_10(self):
        ids = self.shared_inputs["retrieve_many_ids"][:10]
        placeholders = ",".join(["?"] * len(ids))
        return self._query_all(
            f"SELECT name, age, score FROM bench WHERE _id IN ({placeholders})",
            ids,
        )

    def bench_oltp_projected_retrieve_100(self):
        ids = self.shared_inputs["retrieve_many_ids"]
        placeholders = ",".join(["?"] * len(ids))
        return self._query_all(
            f"SELECT name, age, score FROM bench WHERE _id IN ({placeholders})",
            ids,
        )

    def bench_oltp_projected_limit_100(self):
        return self._query_all("SELECT name, age, city FROM bench LIMIT 100")

    def bench_oltp_projected_string_eq(self):
        return self._query_all("SELECT age, score, city FROM bench WHERE name = 'user_5000'")

    def bench_oltp_city_limit_10(self):
        return self._query_all(
            "SELECT name, age FROM bench WHERE city = 'Beijing' LIMIT 100"
        )

    def bench_oltp_insert_one(self):
        self.conn.execute(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            OLTP_ONE_ROW_TUPLE,
        )
        self.conn.commit()

    def bench_oltp_insert_10_rows(self):
        rows = [
            (f"oltp_10_{self._txn_counter}_{i}", 30 + i, 70.0 + i, CITIES[i % len(CITIES)], CATEGORIES[i % len(CATEGORIES)])
            for i in range(10)
        ]
        self._txn_counter += 10
        self.conn.executemany(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            rows,
        )
        self.conn.commit()

    def bench_oltp_insert_read_own_row(self):
        cur = self.conn.execute(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            ("oltp_insert_read", 32, 78.0, "Shanghai", "Books"),
        )
        self.conn.commit()
        return self._query_all("SELECT * FROM bench WHERE _id = ?", (cur.lastrowid,))

    def bench_oltp_insert_count_visible(self):
        self.conn.execute(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            ("oltp_insert_count", 33, 79.0, "Guangzhou", "Books"),
        )
        self.conn.commit()
        return self._scalar("SELECT COUNT(*) FROM bench")

    def bench_oltp_update_by_id(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        self.conn.execute("UPDATE bench SET score = 77.0 WHERE _id = ?", (point_lookup_id,))
        self.conn.commit()

    def bench_oltp_update_missing_id(self):
        missing_id = self.n + 100_000_000
        self.conn.execute("UPDATE bench SET score = 77.0 WHERE _id = ?", (missing_id,))
        self.conn.commit()

    def bench_oltp_update_read_by_id(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        self.conn.execute("UPDATE bench SET score = 77.0 WHERE _id = ?", (point_lookup_id,))
        self.conn.commit()
        return self._query_all("SELECT score FROM bench WHERE _id = ?", (point_lookup_id,))

    def bench_oltp_replace_by_id(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        self.conn.execute(
            "UPDATE bench SET name = ?, age = ?, score = ?, city = ?, category = ? WHERE _id = ?",
            ("user_5000", 31, 77.0, "Beijing", "Books", point_lookup_id),
        )
        self.conn.commit()

    def bench_oltp_insert_delete_by_id(self):
        cur = self.conn.execute(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            ("oltp_insert_delete", 34, 80.0, "Shenzhen", "Books"),
        )
        self.conn.commit()
        self.conn.execute("DELETE FROM bench WHERE _id = ?", (cur.lastrowid,))
        self.conn.commit()

    def bench_oltp_delete_missing_id(self):
        missing_id = self.n + 100_000_000
        self.conn.execute("DELETE FROM bench WHERE _id = ?", (missing_id,))
        self.conn.commit()

    def _next_txn_prefix(self, count=1):
        start = self._txn_counter
        self._txn_counter += count
        return f"txn_sqlite_{start}"

    def _txn_rows(self, prefix, count):
        return [
            (f"{prefix}_{i}", 30 + (i % 25), 70.0 + (i % 17), CITIES[i % len(CITIES)], CATEGORIES[i % len(CATEGORIES)])
            for i in range(count)
        ]

    def bench_txn_empty_commit(self):
        self.conn.execute("BEGIN")
        self.conn.execute("COMMIT")

    def bench_txn_empty_rollback(self):
        self.conn.execute("BEGIN")
        self.conn.execute("ROLLBACK")

    def bench_txn_read_count_commit(self):
        self.conn.execute("BEGIN")
        result = self._scalar("SELECT COUNT(*) FROM bench")
        self.conn.execute("COMMIT")
        return result

    def bench_txn_insert_one_commit(self):
        row = self._txn_rows(self._next_txn_prefix(), 1)[0]
        self.conn.execute("BEGIN")
        self.conn.execute(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            row,
        )
        self.conn.execute("COMMIT")

    def bench_txn_insert_10_commit(self):
        rows = self._txn_rows(self._next_txn_prefix(10), 10)
        self.conn.execute("BEGIN")
        for row in rows:
            self.conn.execute(
                "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
                row,
            )
        self.conn.execute("COMMIT")

    def bench_txn_multi_insert_10_commit(self):
        rows = self._txn_rows(self._next_txn_prefix(10), 10)
        placeholders = ",".join(["(?,?,?,?,?)"] * len(rows))
        params = [value for row in rows for value in row]
        self.conn.execute("BEGIN")
        self.conn.execute(
            f"INSERT INTO bench (name, age, score, city, category) VALUES {placeholders}",
            params,
        )
        self.conn.execute("COMMIT")

    def bench_txn_insert_100_commit(self):
        rows = self._txn_rows(self._next_txn_prefix(100), 100)
        self.conn.execute("BEGIN")
        for row in rows:
            self.conn.execute(
                "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
                row,
            )
        self.conn.execute("COMMIT")

    def bench_txn_multi_insert_100_commit(self):
        rows = self._txn_rows(self._next_txn_prefix(100), 100)
        placeholders = ",".join(["(?,?,?,?,?)"] * len(rows))
        params = [value for row in rows for value in row]
        self.conn.execute("BEGIN")
        self.conn.execute(
            f"INSERT INTO bench (name, age, score, city, category) VALUES {placeholders}",
            params,
        )
        self.conn.execute("COMMIT")

    def bench_txn_insert_10_rollback(self):
        rows = self._txn_rows(self._next_txn_prefix(10), 10)
        self.conn.execute("BEGIN")
        for row in rows:
            self.conn.execute(
                "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
                row,
            )
        self.conn.execute("ROLLBACK")

    def bench_txn_update_by_id_commit(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        self.conn.execute("BEGIN")
        self.conn.execute("UPDATE bench SET score = 77.0 WHERE _id = ?", (point_lookup_id,))
        self.conn.execute("COMMIT")

    def bench_txn_insert_read_own_commit(self):
        row = self._txn_rows(self._next_txn_prefix(), 1)[0]
        self.conn.execute("BEGIN")
        cur = self.conn.execute(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            row,
        )
        result = self._query_all("SELECT * FROM bench WHERE _id = ?", (cur.lastrowid,))
        self.conn.execute("COMMIT")
        return result

    def setup_txn_backlog_1500(self):
        if self._txn_backlog_ready:
            return
        for _ in range(TXN_BACKLOG_ROWS):
            row = self._txn_rows(self._next_txn_prefix(), 1)[0]
            self.conn.execute("BEGIN")
            self.conn.execute(
                "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
                row,
            )
            self.conn.execute("COMMIT")
        self._txn_backlog_ready = True

    def bench_txn_backlog_string_miss_commit(self):
        self.conn.execute("BEGIN")
        result = self._query_all(
            "SELECT * FROM bench WHERE name = ?",
            (TXN_BACKLOG_MISSING_NAME,),
        )
        self.conn.execute("COMMIT")
        return result

    def bench_txn_backlog_count_commit(self):
        self.conn.execute("BEGIN")
        result = self._scalar("SELECT COUNT(*) FROM bench")
        self.conn.execute("COMMIT")
        return result

    def bench_txn_backlog_insert_read_own_by_name_commit(self):
        row = self._txn_rows(self._next_txn_prefix(), 1)[0]
        self.conn.execute("BEGIN")
        self.conn.execute(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            row,
        )
        result = self._query_all("SELECT * FROM bench WHERE name = ?", (row[0],))
        self.conn.execute("COMMIT")
        return result

    def bench_insert_1k(self):
        rows = [(f"new_{i}", 25, 50.0, "Beijing", "Books") for i in range(1000)]
        self.conn.executemany(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            rows,
        )
        self.conn.commit()

    def bench_full_scan_pandas(self):
        return self._query_pandas("SELECT * FROM bench")

    def bench_group_by_2cols(self):
        return self._query_all(
            "SELECT city, category, COUNT(*), AVG(score) FROM bench GROUP BY city, category"
        )

    def bench_filter_like(self):
        return self._query_all(
            "SELECT * FROM bench WHERE name LIKE 'user_1%'"
        )

    def bench_filter_multi_cond(self):
        return self._query_all(
            "SELECT * FROM bench WHERE age > 30 AND score > 50.0"
        )

    def bench_order_by_multi(self):
        return self._query_all(
            "SELECT * FROM bench ORDER BY city ASC, score DESC LIMIT 100"
        )

    def bench_count_distinct(self):
        return self._scalar(
            "SELECT COUNT(DISTINCT city) FROM bench"
        )

    def bench_count_distinct_category(self):
        return self._scalar(
            "SELECT COUNT(DISTINCT category) FROM bench"
        )

    def bench_filter_in(self):
        return self._query_all(
            "SELECT * FROM bench WHERE city IN ('Beijing', 'Shanghai', 'Guangzhou')"
        )

    def bench_filter_numeric_in(self):
        return self._query_all(
            "SELECT * FROM bench WHERE age IN (20, 25, 30, 35, 40, 45, 50, 55, 60)"
        )

    def bench_filter_or_cross_col(self):
        return self._query_all(
            "SELECT * FROM bench WHERE age = 25 OR city = 'Beijing'"
        )

    def bench_filter_numeric_or(self):
        return self._query_all(
            "SELECT * FROM bench WHERE age = 20 OR age = 30 OR age = 40 OR age = 50"
        )

    def bench_update_1k(self):
        self.conn.execute("UPDATE bench SET score = 50.0 WHERE age = 25")
        self.conn.commit()

    def bench_delete_1k(self):
        rows = [(f"del_{i}", 99, 99.0, "Beijing", "Books") for i in range(1000)]
        self.conn.executemany(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            rows,
        )
        self.conn.commit()
        self.conn.execute("DELETE FROM bench WHERE age = 99")
        self.conn.commit()

    def bench_delete_1k_setup(self):
        rows = [(f"del_{i}", 99, 99.0, "Beijing", "Books") for i in range(1000)]
        self.conn.executemany(
            "INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)",
            rows,
        )
        self.conn.commit()

    def bench_delete_1k_only(self):
        self.conn.execute("DELETE FROM bench WHERE age = 99")
        self.conn.commit()

    TABLE_OPS_NAME = "bench_table_ops"

    def bench_table_create_setup(self):
        self.conn.execute("DROP TABLE IF EXISTS bench_table_ops")
        self.conn.commit()

    def bench_table_create(self):
        self.conn.execute("CREATE TABLE bench_table_ops (k INTEGER)")
        self.conn.commit()

    def bench_table_drop_setup(self):
        self.conn.execute("DROP TABLE IF EXISTS bench_table_ops")
        self.conn.execute("CREATE TABLE bench_table_ops (k INTEGER)")
        self.conn.commit()

    def bench_table_drop(self):
        self.conn.execute("DROP TABLE bench_table_ops")
        self.conn.commit()

    def bench_table_create_drop_cycle(self):
        self.conn.execute("DROP TABLE IF EXISTS bench_table_ops")
        self.conn.execute("CREATE TABLE bench_table_ops (k INTEGER)")
        self.conn.execute("DROP TABLE bench_table_ops")
        self.conn.commit()

    def bench_list_tables_setup(self):
        self.conn.execute("DROP TABLE IF EXISTS bench_table_ops")
        for i in range(10):
            self.conn.execute(f"DROP TABLE IF EXISTS bench_table_ops_{i}")
        for i in range(10):
            self.conn.execute(f"CREATE TABLE bench_table_ops_{i} (k INTEGER)")
        self.conn.commit()

    def bench_list_tables(self):
        return self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name LIKE 'bench_table_ops%' ORDER BY name"
        ).fetchall()

    def bench_alter_table_add_column_setup(self):
        self.conn.execute("DROP TABLE IF EXISTS bench_table_ops")
        self.conn.execute("CREATE TABLE bench_table_ops (k INTEGER)")
        self.conn.commit()

    def bench_alter_table_add_column(self):
        self.conn.execute("ALTER TABLE bench_table_ops ADD COLUMN c INTEGER")
        self.conn.commit()

    def bench_window_row_number(self):
        return self._query_all(
            "SELECT name, city, score, "
            "ROW_NUMBER() OVER (PARTITION BY city ORDER BY score DESC) as rn "
            "FROM bench LIMIT 1000"
        )

    # --- Advanced SQL: joins, set operations, subqueries, expressions ---

    def bench_join_group_order_limit(self):
        return self._query_all(
            "SELECT b.city, COUNT(*) AS c FROM bench b JOIN meta m ON b.city = m.city "
            "GROUP BY b.city ORDER BY c DESC LIMIT 5"
        )

    def bench_left_join_count(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench b LEFT JOIN meta m ON b.city = m.city"
        )

    def bench_left_join_extra_on(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench b LEFT JOIN meta m "
            "ON b.city = m.city AND m.pop > 15000000"
        )

    def bench_full_outer_join_bounded(self):
        return self._query_all(
            "SELECT b.city, m.pop FROM bench b FULL OUTER JOIN meta m ON b.city = m.city "
            "WHERE b.age < 25 LIMIT 1000"
        )

    def bench_comma_cross_join(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench b CROSS JOIN meta m "
            "WHERE b.age < 20 AND m.pop > 0"
        )

    def bench_union_all(self):
        return self._query_all(
            "SELECT city FROM bench WHERE age = 25 "
            "UNION ALL SELECT city FROM bench WHERE age = 26 ORDER BY city LIMIT 100"
        )

    def bench_union_distinct(self):
        return self._query_all(
            "SELECT city FROM bench WHERE age = 25 "
            "UNION SELECT city FROM bench WHERE age = 26 ORDER BY city"
        )

    def bench_intersect(self):
        return self._query_all(
            "SELECT city FROM bench WHERE age BETWEEN 20 AND 30 "
            "INTERSECT SELECT city FROM bench WHERE age BETWEEN 25 AND 35 ORDER BY city"
        )

    def bench_except(self):
        return self._query_all(
            "SELECT city FROM bench WHERE age BETWEEN 20 AND 30 "
            "EXCEPT SELECT city FROM bench WHERE age BETWEEN 25 AND 35 ORDER BY city"
        )

    def bench_in_subquery(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench "
            "WHERE city IN (SELECT city FROM meta WHERE pop > 15000000)"
        )

    def bench_exists_subquery(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench b WHERE EXISTS "
            "(SELECT 1 FROM meta m WHERE m.city = b.city AND m.pop > 15000000)"
        )

    def bench_derived_table(self):
        return self._query_all(
            "SELECT city, cnt FROM "
            "(SELECT city, COUNT(*) AS cnt FROM bench GROUP BY city) d "
            "ORDER BY cnt DESC LIMIT 5"
        )

    def bench_cte(self):
        return self._query_all(
            "WITH c AS (SELECT city, AVG(score) AS av FROM bench GROUP BY city) "
            "SELECT city FROM c WHERE av > 50 ORDER BY av DESC LIMIT 5"
        )

    def bench_case_aggregate(self):
        return self._query_all(
            "SELECT city, COUNT(CASE WHEN age > 40 THEN 1 END) AS c "
            "FROM bench GROUP BY city ORDER BY city"
        )

    def bench_string_functions(self):
        return self._query_all(
            "SELECT UPPER(city) AS u, LENGTH(name) AS ln, SUBSTR(name, 1, 5) AS sub, "
            "CONCAT(city, '-', category) AS cc, TRIM(category) AS tr "
            "FROM bench WHERE age BETWEEN 30 AND 40 LIMIT 1000"
        )

    def bench_numeric_functions(self):
        return self._query_all(
            "SELECT ROUND(score, 0) AS r, ABS(age - 40) AS a, FLOOR(score) AS f, "
            "CEIL(score) AS c, MOD(age, 7) AS m "
            "FROM bench WHERE age BETWEEN 30 AND 40 LIMIT 1000"
        )

    def bench_coalesce_nullif(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench "
            "WHERE COALESCE(NULLIF(category, 'Books'), 'none') = 'Books'"
        )

    def bench_not_filter(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench "
            "WHERE age NOT BETWEEN 20 AND 40 AND name NOT LIKE 'user_5%'"
        )

    def bench_deep_offset(self):
        return self._query_all(
            "SELECT name, age FROM bench ORDER BY age, name LIMIT 100 OFFSET 100000"
        )

    def bench_order_by_expression(self):
        return self._query_all(
            "SELECT name FROM bench ORDER BY LENGTH(name) DESC, age LIMIT 100"
        )

    def bench_window_sum_over(self):
        return self._query_all(
            "SELECT city, score, SUM(score) OVER (PARTITION BY city ORDER BY age) AS run "
            "FROM bench LIMIT 1000"
        )

    def bench_window_rank(self):
        return self._query_all(
            "SELECT city, RANK() OVER (PARTITION BY city ORDER BY score DESC) AS rk "
            "FROM bench LIMIT 1000"
        )

    def bench_window_lag(self):
        return self._query_all(
            "SELECT city, LAG(score) OVER (PARTITION BY city ORDER BY age) AS prev "
            "FROM bench LIMIT 1000"
        )

    def bench_distinct_rows(self):
        return self._query_all(
            "SELECT DISTINCT city, category FROM bench ORDER BY city, category"
        )

    def bench_multi_col_group_having(self):
        return self._query_all(
            "SELECT city, category, COUNT(*) AS c FROM bench "
            "GROUP BY city, category HAVING c > 2000 ORDER BY city, category LIMIT 20"
        )

    def bench_fts_build(self):
        try:
            self.conn.execute("DROP TABLE IF EXISTS bench_fts")
            self.conn.execute("""
                CREATE VIRTUAL TABLE bench_fts
                USING fts5(name, city, category, content='bench', content_rowid='_id')
            """)
            self.conn.execute("INSERT INTO bench_fts(bench_fts) VALUES('rebuild')")
            self.conn.commit()
            self._fts_ready = True
        except Exception:
            self._fts_ready = False

    def bench_fts_search(self):
        if not getattr(self, '_fts_ready', False):
            return None
        return [
            row[0] for row in self.conn.execute(
            "SELECT rowid FROM bench_fts WHERE bench_fts MATCH 'Electronics'"
        ).fetchall()
        ]

    def close(self):
        if self.conn:
            self.conn.close()


# ---------------------------------------------------------------------------
# DuckDB benchmark
# ---------------------------------------------------------------------------

class DuckDBBench:
    def __init__(self, tmpdir, data, csv_path=None, parquet_path=None, json_path=None):
        self.db_path = os.path.join(tmpdir, "bench.duckdb")
        self.data = data
        self.n = len(data["name"])
        self.conn = None
        self.csv_path = csv_path
        self.parquet_path = parquet_path
        self.json_path = json_path
        self.shared_inputs = build_shared_inputs(self.n)
        self._next_rowid = 0
        self._txn_counter = 0
        self._txn_backlog_ready = False

    def _connect(self):
        ensure_optional_imports()
        self.conn = duckdb.connect(self.db_path)

    def _query_all(self, sql, params=()):
        cur = self.conn.execute(sql, params)
        return rows_to_dicts([d[0] for d in (cur.description or [])], cur.fetchall())

    def _scalar(self, sql, params=()):
        return self.conn.execute(sql, params).fetchone()[0]

    def _query_pandas(self, sql):
        ensure_optional_imports()
        if HAS_PANDAS:
            return self.conn.execute(sql).df()
        return self._query_all(sql)

    def execute_materialized_query(self, sql):
        return self._query_all(sql)

    def setup(self):
        if self.conn:
            self.conn.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self._connect()
        self.conn.execute("""
            CREATE TABLE bench (
                name VARCHAR,
                age INTEGER,
                score DOUBLE,
                city VARCHAR,
                category VARCHAR
            )
        """)
        self.conn.execute("CREATE TABLE meta (city VARCHAR, pop INTEGER)")
        self.conn.executemany("INSERT INTO meta VALUES (?,?)", META_CITY_POP)
        self._txn_counter = 0
        self._txn_backlog_ready = False
        self._temp_csv_name = None
        self._temp_json_name = None

    def cold_start_setup(self):
        if self.conn:
            self.conn.close()
        self._connect()

    def bench_insert(self):
        # Use executemany for reliable cross-version compatibility
        rows = list(zip(
            self.data["name"], self.data["age"], self.data["score"],
            self.data["city"], self.data["category"]
        ))
        self.conn.executemany(
            "INSERT INTO bench VALUES (?,?,?,?,?)", rows
        )
        self._next_rowid = self.n

    def bench_count(self):
        return self._scalar("SELECT COUNT(*) FROM bench")

    def bench_select_limit(self, limit=100):
        return self._query_all(f"SELECT * FROM bench LIMIT {limit}")

    def bench_select_limit_10k(self):
        return self._query_all("SELECT * FROM bench LIMIT 10000")

    def bench_projected_full_scan(self):
        return self._query_all("SELECT name, age, city FROM bench")

    def bench_filtered_limit_100(self):
        return self._query_all("SELECT * FROM bench WHERE age > 30 LIMIT 100")

    def bench_limit_offset_100(self):
        return self._query_all("SELECT * FROM bench LIMIT 100 OFFSET 10000")

    def bench_filter_string(self):
        return self._query_all(
            "SELECT * FROM bench WHERE name = 'user_5000'"
        )

    def bench_filter_range(self):
        return self._query_all(
            "SELECT * FROM bench WHERE age BETWEEN 25 AND 35"
        )

    def bench_group_by(self):
        return self._query_all(
            "SELECT city, COUNT(*), AVG(score) FROM bench GROUP BY city"
        )

    def bench_group_by_category(self):
        return self._query_all(
            "SELECT category, COUNT(*), AVG(score) FROM bench GROUP BY category"
        )

    def bench_group_by_order_count(self):
        return self._query_all(
            "SELECT city, COUNT(*) AS cnt FROM bench GROUP BY city ORDER BY cnt DESC LIMIT 5"
        )

    def bench_group_by_category_order_count(self):
        return self._query_all(
            "SELECT category, COUNT(*) AS cnt FROM bench GROUP BY category ORDER BY cnt DESC LIMIT 5"
        )

    def bench_group_by_having(self):
        return self._query_all(
            "SELECT city, COUNT(*) as cnt, AVG(score) FROM bench GROUP BY city HAVING cnt > 1000"
        )

    def bench_group_by_category_having(self):
        return self._query_all(
            "SELECT category, COUNT(*) as cnt, AVG(score) FROM bench GROUP BY category HAVING cnt > 1000"
        )

    def setup_view_bench(self):
        self.conn.execute("DROP VIEW IF EXISTS bench_view")
        self.conn.execute(
            "CREATE VIEW bench_view AS "
            "SELECT city, COUNT(*) AS cnt, AVG(score) AS avg_score "
            "FROM bench GROUP BY city"
        )

    def bench_view_select(self):
        return self._query_all(
            "SELECT city, cnt FROM bench_view WHERE cnt > 0 ORDER BY cnt DESC LIMIT 5"
        )

    def bench_order_limit(self):
        return self._query_all(
            "SELECT * FROM bench ORDER BY score DESC LIMIT 100"
        )

    def bench_order_limit_asc(self):
        return self._query_all(
            "SELECT * FROM bench ORDER BY score ASC LIMIT 100"
        )

    def bench_aggregation(self):
        return self._query_all(
            "SELECT COUNT(*), AVG(age), SUM(score), MIN(age), MAX(age) FROM bench"
        )

    def bench_filtered_aggregation(self):
        return self._query_all(
            "SELECT COUNT(*), AVG(score), MAX(score) FROM bench WHERE category = 'Electronics'"
        )

    def bench_filtered_aggregation_city(self):
        return self._query_all(
            "SELECT COUNT(*), AVG(score), MAX(score) FROM bench WHERE city = 'Beijing'"
        )

    def bench_count_where_category(self):
        return self._scalar(
            "SELECT COUNT(*) FROM bench WHERE category = 'Electronics'"
        )

    def bench_complex(self):
        return self._query_all(
            "SELECT city, AVG(score) as avg_s FROM bench WHERE age BETWEEN 25 AND 50 GROUP BY city ORDER BY avg_s DESC LIMIT 5"
        )

    def bench_point_lookup(self):
        rowid = self.shared_inputs["point_lookup_id"] - 1
        return self._query_all(
            "SELECT rowid + 1 AS _id, * FROM bench WHERE rowid = ?",
            (rowid,),
        )

    def bench_oltp_projected_point_lookup(self):
        rowid = self.shared_inputs["point_lookup_id"] - 1
        return self._query_all(
            "SELECT name, age, score FROM bench WHERE rowid = ?",
            (rowid,),
        )

    def bench_oltp_count_rows(self):
        return self._scalar("SELECT COUNT(*) FROM bench")

    def bench_oltp_direct_point_lookup(self):
        rowid = self.shared_inputs["point_lookup_id"] - 1
        return self._query_all(
            "SELECT rowid + 1 AS _id, * FROM bench WHERE rowid = ?",
            (rowid,),
        )

    def bench_oltp_missing_point_lookup(self):
        missing_rowid = self.n + 100_000_000
        return self._query_all(
            "SELECT rowid + 1 AS _id, * FROM bench WHERE rowid = ?",
            (missing_rowid,),
        )

    def bench_retrieve_many(self):
        ids = self.shared_inputs["retrieve_many_ids"]
        rowids = [id_ - 1 for id_ in ids]
        placeholders = ",".join(["?"] * len(rowids))
        return self._query_all(
            f"SELECT rowid + 1 AS _id, * FROM bench WHERE rowid IN ({placeholders})",
            rowids,
        )

    def bench_oltp_projected_retrieve_10(self):
        ids = self.shared_inputs["retrieve_many_ids"][:10]
        rowids = [id_ - 1 for id_ in ids]
        placeholders = ",".join(["?"] * len(rowids))
        return self._query_all(
            f"SELECT name, age, score FROM bench WHERE rowid IN ({placeholders})",
            rowids,
        )

    def bench_oltp_projected_retrieve_100(self):
        ids = self.shared_inputs["retrieve_many_ids"]
        rowids = [id_ - 1 for id_ in ids]
        placeholders = ",".join(["?"] * len(rowids))
        return self._query_all(
            f"SELECT name, age, score FROM bench WHERE rowid IN ({placeholders})",
            rowids,
        )

    def bench_oltp_projected_limit_100(self):
        return self._query_all("SELECT name, age, city FROM bench LIMIT 100")

    def bench_oltp_projected_string_eq(self):
        return self._query_all("SELECT age, score, city FROM bench WHERE name = 'user_5000'")

    def bench_oltp_city_limit_10(self):
        return self._query_all(
            "SELECT name, age FROM bench WHERE city = 'Beijing' LIMIT 100"
        )

    def bench_oltp_insert_one(self):
        self.conn.execute(
            "INSERT INTO bench VALUES (?,?,?,?,?)",
            OLTP_ONE_ROW_TUPLE,
        )
        self._next_rowid += 1

    def bench_oltp_insert_10_rows(self):
        rows = [
            (f"oltp_10_{self._txn_counter}_{i}", 30 + i, 70.0 + i, CITIES[i % len(CITIES)], CATEGORIES[i % len(CATEGORIES)])
            for i in range(10)
        ]
        self._txn_counter += 10
        self.conn.executemany("INSERT INTO bench VALUES (?,?,?,?,?)", rows)
        self._next_rowid += 10

    def bench_oltp_insert_read_own_row(self):
        rowid = self._next_rowid
        self.conn.execute(
            "INSERT INTO bench VALUES (?,?,?,?,?)",
            ("oltp_insert_read", 32, 78.0, "Shanghai", "Books"),
        )
        self._next_rowid += 1
        return self._query_all(
            "SELECT rowid + 1 AS _id, * FROM bench WHERE rowid = ?",
            (rowid,),
        )

    def bench_oltp_insert_count_visible(self):
        self.conn.execute(
            "INSERT INTO bench VALUES (?,?,?,?,?)",
            ("oltp_insert_count", 33, 79.0, "Guangzhou", "Books"),
        )
        self._next_rowid += 1
        return self._scalar("SELECT COUNT(*) FROM bench")

    def bench_oltp_update_by_id(self):
        rowid = self.shared_inputs["point_lookup_id"] - 1
        self.conn.execute("UPDATE bench SET score = 77.0 WHERE rowid = ?", (rowid,))

    def bench_oltp_update_missing_id(self):
        missing_rowid = self.n + 100_000_000
        self.conn.execute("UPDATE bench SET score = 77.0 WHERE rowid = ?", (missing_rowid,))

    def bench_oltp_update_read_by_id(self):
        rowid = self.shared_inputs["point_lookup_id"] - 1
        self.conn.execute("UPDATE bench SET score = 77.0 WHERE rowid = ?", (rowid,))
        return self._query_all("SELECT score FROM bench WHERE rowid = ?", (rowid,))

    def bench_oltp_replace_by_id(self):
        rowid = self.shared_inputs["point_lookup_id"] - 1
        self.conn.execute(
            "UPDATE bench SET name = ?, age = ?, score = ?, city = ?, category = ? WHERE rowid = ?",
            ("user_5000", 31, 77.0, "Beijing", "Books", rowid),
        )

    def bench_oltp_insert_delete_by_id(self):
        rowid = self._next_rowid
        self.conn.execute(
            "INSERT INTO bench VALUES (?,?,?,?,?)",
            ("oltp_insert_delete", 34, 80.0, "Shenzhen", "Books"),
        )
        self._next_rowid += 1
        self.conn.execute("DELETE FROM bench WHERE rowid = ?", (rowid,))

    def bench_oltp_delete_missing_id(self):
        missing_rowid = self.n + 100_000_000
        self.conn.execute("DELETE FROM bench WHERE rowid = ?", (missing_rowid,))

    def _next_txn_prefix(self, count=1):
        start = self._txn_counter
        self._txn_counter += count
        return f"txn_duckdb_{start}"

    def _txn_rows(self, prefix, count):
        return [
            (f"{prefix}_{i}", 30 + (i % 25), 70.0 + (i % 17), CITIES[i % len(CITIES)], CATEGORIES[i % len(CATEGORIES)])
            for i in range(count)
        ]

    def bench_txn_empty_commit(self):
        self.conn.execute("BEGIN TRANSACTION")
        self.conn.execute("COMMIT")

    def bench_txn_empty_rollback(self):
        self.conn.execute("BEGIN TRANSACTION")
        self.conn.execute("ROLLBACK")

    def bench_txn_read_count_commit(self):
        self.conn.execute("BEGIN TRANSACTION")
        result = self._scalar("SELECT COUNT(*) FROM bench")
        self.conn.execute("COMMIT")
        return result

    def bench_txn_insert_one_commit(self):
        row = self._txn_rows(self._next_txn_prefix(), 1)[0]
        self.conn.execute("BEGIN TRANSACTION")
        self.conn.execute("INSERT INTO bench VALUES (?,?,?,?,?)", row)
        self.conn.execute("COMMIT")
        self._next_rowid += 1

    def bench_txn_insert_10_commit(self):
        rows = self._txn_rows(self._next_txn_prefix(10), 10)
        self.conn.execute("BEGIN TRANSACTION")
        for row in rows:
            self.conn.execute("INSERT INTO bench VALUES (?,?,?,?,?)", row)
        self.conn.execute("COMMIT")
        self._next_rowid += 10

    def bench_txn_multi_insert_10_commit(self):
        rows = self._txn_rows(self._next_txn_prefix(10), 10)
        placeholders = ",".join(["(?,?,?,?,?)"] * len(rows))
        params = [value for row in rows for value in row]
        self.conn.execute("BEGIN TRANSACTION")
        self.conn.execute(f"INSERT INTO bench VALUES {placeholders}", params)
        self.conn.execute("COMMIT")
        self._next_rowid += 10

    def bench_txn_insert_100_commit(self):
        rows = self._txn_rows(self._next_txn_prefix(100), 100)
        self.conn.execute("BEGIN TRANSACTION")
        for row in rows:
            self.conn.execute("INSERT INTO bench VALUES (?,?,?,?,?)", row)
        self.conn.execute("COMMIT")
        self._next_rowid += 100

    def bench_txn_multi_insert_100_commit(self):
        rows = self._txn_rows(self._next_txn_prefix(100), 100)
        placeholders = ",".join(["(?,?,?,?,?)"] * len(rows))
        params = [value for row in rows for value in row]
        self.conn.execute("BEGIN TRANSACTION")
        self.conn.execute(f"INSERT INTO bench VALUES {placeholders}", params)
        self.conn.execute("COMMIT")
        self._next_rowid += 100

    def bench_txn_insert_10_rollback(self):
        rows = self._txn_rows(self._next_txn_prefix(10), 10)
        self.conn.execute("BEGIN TRANSACTION")
        for row in rows:
            self.conn.execute("INSERT INTO bench VALUES (?,?,?,?,?)", row)
        self.conn.execute("ROLLBACK")

    def bench_txn_update_by_id_commit(self):
        rowid = self.shared_inputs["point_lookup_id"] - 1
        self.conn.execute("BEGIN TRANSACTION")
        self.conn.execute("UPDATE bench SET score = 77.0 WHERE rowid = ?", (rowid,))
        self.conn.execute("COMMIT")

    def bench_txn_insert_read_own_commit(self):
        row = self._txn_rows(self._next_txn_prefix(), 1)[0]
        rowid = self._next_rowid
        self.conn.execute("BEGIN TRANSACTION")
        self.conn.execute("INSERT INTO bench VALUES (?,?,?,?,?)", row)
        result = self._query_all("SELECT rowid + 1 AS _id, * FROM bench WHERE rowid = ?", (rowid,))
        self.conn.execute("COMMIT")
        self._next_rowid += 1
        return result

    def setup_txn_backlog_1500(self):
        if self._txn_backlog_ready:
            return
        for _ in range(TXN_BACKLOG_ROWS):
            row = self._txn_rows(self._next_txn_prefix(), 1)[0]
            self.conn.execute("BEGIN TRANSACTION")
            self.conn.execute("INSERT INTO bench VALUES (?,?,?,?,?)", row)
            self.conn.execute("COMMIT")
            self._next_rowid += 1
        self._txn_backlog_ready = True

    def bench_txn_backlog_string_miss_commit(self):
        self.conn.execute("BEGIN TRANSACTION")
        result = self._query_all(
            "SELECT rowid + 1 AS _id, * FROM bench WHERE name = ?",
            (TXN_BACKLOG_MISSING_NAME,),
        )
        self.conn.execute("COMMIT")
        return result

    def bench_txn_backlog_count_commit(self):
        self.conn.execute("BEGIN TRANSACTION")
        result = self._scalar("SELECT COUNT(*) FROM bench")
        self.conn.execute("COMMIT")
        return result

    def bench_txn_backlog_insert_read_own_by_name_commit(self):
        row = self._txn_rows(self._next_txn_prefix(), 1)[0]
        self.conn.execute("BEGIN TRANSACTION")
        self.conn.execute("INSERT INTO bench VALUES (?,?,?,?,?)", row)
        result = self._query_all(
            "SELECT rowid + 1 AS _id, * FROM bench WHERE name = ?",
            (row[0],),
        )
        self.conn.execute("COMMIT")
        self._next_rowid += 1
        return result

    def bench_insert_1k(self):
        # Use executemany for reliable cross-version compatibility
        rows = [(f"new_{i}", 25, 50.0, "Beijing", "Books") for i in range(1000)]
        self.conn.executemany("INSERT INTO bench VALUES (?,?,?,?,?)", rows)
        self._next_rowid += 1000

    def bench_full_scan_pandas(self):
        return self._query_pandas("SELECT rowid + 1 AS _id, * FROM bench")

    def bench_group_by_2cols(self):
        return self._query_all(
            "SELECT city, category, COUNT(*), AVG(score) FROM bench GROUP BY city, category"
        )

    def bench_filter_like(self):
        return self._query_all(
            "SELECT * FROM bench WHERE name LIKE 'user_1%'"
        )

    def bench_filter_multi_cond(self):
        return self._query_all(
            "SELECT * FROM bench WHERE age > 30 AND score > 50.0"
        )

    def bench_order_by_multi(self):
        return self._query_all(
            "SELECT * FROM bench ORDER BY city ASC, score DESC LIMIT 100"
        )

    def bench_count_distinct(self):
        return self._scalar(
            "SELECT COUNT(DISTINCT city) FROM bench"
        )

    def bench_count_distinct_category(self):
        return self._scalar(
            "SELECT COUNT(DISTINCT category) FROM bench"
        )

    def bench_filter_in(self):
        return self._query_all(
            "SELECT * FROM bench WHERE city IN ('Beijing', 'Shanghai', 'Guangzhou')"
        )

    def bench_filter_numeric_in(self):
        return self._query_all(
            "SELECT * FROM bench WHERE age IN (20, 25, 30, 35, 40, 45, 50, 55, 60)"
        )

    def bench_filter_or_cross_col(self):
        return self._query_all(
            "SELECT * FROM bench WHERE age = 25 OR city = 'Beijing'"
        )

    def bench_filter_numeric_or(self):
        return self._query_all(
            "SELECT * FROM bench WHERE age = 20 OR age = 30 OR age = 40 OR age = 50"
        )

    def bench_update_1k(self):
        self.conn.execute("UPDATE bench SET score = 50.0 WHERE age = 25")

    def bench_delete_1k(self):
        rows = [(f"del_{i}", 99, 99.0, "Beijing", "Books") for i in range(1000)]
        self.conn.executemany("INSERT INTO bench VALUES (?,?,?,?,?)", rows)
        self._next_rowid += 1000
        self.conn.execute("DELETE FROM bench WHERE age = 99")

    def bench_delete_1k_setup(self):
        rows = [(f"del_{i}", 99, 99.0, "Beijing", "Books") for i in range(1000)]
        self.conn.executemany("INSERT INTO bench VALUES (?,?,?,?,?)", rows)
        self._next_rowid += 1000

    def bench_delete_1k_only(self):
        self.conn.execute("DELETE FROM bench WHERE age = 99")

    TABLE_OPS_NAME = "bench_table_ops"

    def bench_table_create_setup(self):
        self.conn.execute("DROP TABLE IF EXISTS bench_table_ops")

    def bench_table_create(self):
        self.conn.execute("CREATE TABLE bench_table_ops (k BIGINT)")

    def bench_table_drop_setup(self):
        self.conn.execute("DROP TABLE IF EXISTS bench_table_ops")
        self.conn.execute("CREATE TABLE bench_table_ops (k BIGINT)")

    def bench_table_drop(self):
        self.conn.execute("DROP TABLE bench_table_ops")

    def bench_table_create_drop_cycle(self):
        self.conn.execute("DROP TABLE IF EXISTS bench_table_ops")
        self.conn.execute("CREATE TABLE bench_table_ops (k BIGINT)")
        self.conn.execute("DROP TABLE bench_table_ops")

    def bench_list_tables_setup(self):
        self.conn.execute("DROP TABLE IF EXISTS bench_table_ops")
        for i in range(10):
            self.conn.execute(f"DROP TABLE IF EXISTS bench_table_ops_{i}")
        for i in range(10):
            self.conn.execute(f"CREATE TABLE bench_table_ops_{i} (k BIGINT)")

    def bench_list_tables(self):
        return self.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' "
            "AND table_name LIKE 'bench_table_ops%' ORDER BY table_name"
        ).fetchall()

    def bench_alter_table_add_column_setup(self):
        self.conn.execute("DROP TABLE IF EXISTS bench_table_ops")
        self.conn.execute("CREATE TABLE bench_table_ops (k BIGINT)")

    def bench_alter_table_add_column(self):
        self.conn.execute("ALTER TABLE bench_table_ops ADD COLUMN c BIGINT")

    def bench_window_row_number(self):
        return self._query_all(
            "SELECT name, city, score, "
            "ROW_NUMBER() OVER (PARTITION BY city ORDER BY score DESC) as rn "
            "FROM bench LIMIT 1000"
        )

    # --- Advanced SQL: joins, set operations, subqueries, expressions ---

    def bench_join_group_order_limit(self):
        return self._query_all(
            "SELECT b.city, COUNT(*) AS c FROM bench b JOIN meta m ON b.city = m.city "
            "GROUP BY b.city ORDER BY c DESC LIMIT 5"
        )

    def bench_left_join_count(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench b LEFT JOIN meta m ON b.city = m.city"
        )

    def bench_left_join_extra_on(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench b LEFT JOIN meta m "
            "ON b.city = m.city AND m.pop > 15000000"
        )

    def bench_full_outer_join_bounded(self):
        return self._query_all(
            "SELECT b.city, m.pop FROM bench b FULL OUTER JOIN meta m ON b.city = m.city "
            "WHERE b.age < 25 LIMIT 1000"
        )

    def bench_comma_cross_join(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench b CROSS JOIN meta m "
            "WHERE b.age < 20 AND m.pop > 0"
        )

    def bench_union_all(self):
        return self._query_all(
            "SELECT city FROM bench WHERE age = 25 "
            "UNION ALL SELECT city FROM bench WHERE age = 26 ORDER BY city LIMIT 100"
        )

    def bench_union_distinct(self):
        return self._query_all(
            "SELECT city FROM bench WHERE age = 25 "
            "UNION SELECT city FROM bench WHERE age = 26 ORDER BY city"
        )

    def bench_intersect(self):
        return self._query_all(
            "SELECT city FROM bench WHERE age BETWEEN 20 AND 30 "
            "INTERSECT SELECT city FROM bench WHERE age BETWEEN 25 AND 35 ORDER BY city"
        )

    def bench_except(self):
        return self._query_all(
            "SELECT city FROM bench WHERE age BETWEEN 20 AND 30 "
            "EXCEPT SELECT city FROM bench WHERE age BETWEEN 25 AND 35 ORDER BY city"
        )

    def bench_in_subquery(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench "
            "WHERE city IN (SELECT city FROM meta WHERE pop > 15000000)"
        )

    def bench_exists_subquery(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench b WHERE EXISTS "
            "(SELECT 1 FROM meta m WHERE m.city = b.city AND m.pop > 15000000)"
        )

    def bench_derived_table(self):
        return self._query_all(
            "SELECT city, cnt FROM "
            "(SELECT city, COUNT(*) AS cnt FROM bench GROUP BY city) d "
            "ORDER BY cnt DESC LIMIT 5"
        )

    def bench_cte(self):
        return self._query_all(
            "WITH c AS (SELECT city, AVG(score) AS av FROM bench GROUP BY city) "
            "SELECT city FROM c WHERE av > 50 ORDER BY av DESC LIMIT 5"
        )

    def bench_case_aggregate(self):
        return self._query_all(
            "SELECT city, COUNT(CASE WHEN age > 40 THEN 1 END) AS c "
            "FROM bench GROUP BY city ORDER BY city"
        )

    def bench_string_functions(self):
        return self._query_all(
            "SELECT UPPER(city) AS u, LENGTH(name) AS ln, SUBSTR(name, 1, 5) AS sub, "
            "CONCAT(city, '-', category) AS cc, TRIM(category) AS tr "
            "FROM bench WHERE age BETWEEN 30 AND 40 LIMIT 1000"
        )

    def bench_numeric_functions(self):
        return self._query_all(
            "SELECT ROUND(score, 0) AS r, ABS(age - 40) AS a, FLOOR(score) AS f, "
            "CEIL(score) AS c, MOD(age, 7) AS m "
            "FROM bench WHERE age BETWEEN 30 AND 40 LIMIT 1000"
        )

    def bench_coalesce_nullif(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench "
            "WHERE COALESCE(NULLIF(category, 'Books'), 'none') = 'Books'"
        )

    def bench_not_filter(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM bench "
            "WHERE age NOT BETWEEN 20 AND 40 AND name NOT LIKE 'user_5%'"
        )

    def bench_deep_offset(self):
        return self._query_all(
            "SELECT name, age FROM bench ORDER BY age, name LIMIT 100 OFFSET 100000"
        )

    def bench_order_by_expression(self):
        return self._query_all(
            "SELECT name FROM bench ORDER BY LENGTH(name) DESC, age LIMIT 100"
        )

    def bench_window_sum_over(self):
        return self._query_all(
            "SELECT city, score, SUM(score) OVER (PARTITION BY city ORDER BY age) AS run "
            "FROM bench LIMIT 1000"
        )

    def bench_window_rank(self):
        return self._query_all(
            "SELECT city, RANK() OVER (PARTITION BY city ORDER BY score DESC) AS rk "
            "FROM bench LIMIT 1000"
        )

    def bench_window_lag(self):
        return self._query_all(
            "SELECT city, LAG(score) OVER (PARTITION BY city ORDER BY age) AS prev "
            "FROM bench LIMIT 1000"
        )

    def bench_distinct_rows(self):
        return self._query_all(
            "SELECT DISTINCT city, category FROM bench ORDER BY city, category"
        )

    def bench_multi_col_group_having(self):
        return self._query_all(
            "SELECT city, category, COUNT(*) AS c FROM bench "
            "GROUP BY city, category HAVING c > 2000 ORDER BY city, category LIMIT 20"
        )

    def bench_fts_build(self):
        try:
            self.conn.execute("INSTALL fts")
            self.conn.execute("LOAD fts")
            self.conn.execute(
                "PRAGMA create_fts_index('bench', 'rowid', 'name', 'city', 'category')"
            )
            self._fts_ready = True
        except Exception:
            self._fts_ready = False

    def bench_fts_search(self):
        if not getattr(self, '_fts_ready', False):
            return None
        try:
            return [
                row["_id"] for row in self._query_all(
                "SELECT rowid + 1 AS _id FROM bench "
                "WHERE fts_main_bench.match_bm25(rowid, 'Electronics') IS NOT NULL"
            )
            ]
        except Exception:
            return None

    def bench_csv_read_count(self):
        return self._scalar(f"SELECT COUNT(*) FROM read_csv_auto('{self.csv_path}')")

    def bench_csv_read_filter_group(self):
        return self._query_all(
            f"SELECT city, COUNT(*) FROM read_csv_auto('{self.csv_path}') WHERE age > 30 GROUP BY city"
        )

    def bench_csv_read_limit_1k(self):
        return self._query_all(f"SELECT * FROM read_csv_auto('{self.csv_path}') LIMIT 1000")

    def bench_json_read_count(self):
        return self._scalar(f"SELECT COUNT(*) FROM read_json_auto('{self.json_path}')")

    def bench_temp_csv_create_query(self):
        if self._temp_csv_name is None:
            self._temp_csv_name = f"bench_csv_temp_{uuid.uuid4().hex[:8]}"
            self.conn.execute(f"CREATE TABLE {self._temp_csv_name} AS SELECT * FROM read_csv_auto('{self.csv_path}')")
        return self._scalar(f"SELECT AVG(score) FROM {self._temp_csv_name} WHERE age > 30")

    def bench_csv_read_order_limit(self):
        return self._query_all(
            f"SELECT * FROM read_csv_auto('{self.csv_path}') ORDER BY score DESC LIMIT 100"
        )

    def bench_json_read_filter(self):
        return self._scalar(
            f"SELECT COUNT(*) FROM read_json_auto('{self.json_path}') WHERE age > 30"
        )

    def bench_json_read_order_limit(self):
        return self._query_all(
            f"SELECT * FROM read_json_auto('{self.json_path}') ORDER BY score DESC LIMIT 100"
        )

    def bench_json_read_group_category(self):
        return self._query_all(
            f"SELECT category, COUNT(*) FROM read_json_auto('{self.json_path}') GROUP BY category"
        )

    def close(self):
        if self.conn:
            self.conn.close()


# ---------------------------------------------------------------------------
# ApexBase benchmark
# ---------------------------------------------------------------------------

class ApexBaseBench:
    def __init__(self, tmpdir, data, low_memory=False, csv_path=None, parquet_path=None, json_path=None):
        self.db_dir = os.path.join(tmpdir, "apex_bench")
        self.data = data
        self.n = len(data["name"])
        self.client = None
        self.low_memory = low_memory
        self.csv_path = csv_path
        self.parquet_path = parquet_path
        self.json_path = json_path
        self.shared_inputs = build_shared_inputs(self.n)
        self._next_id = 1
        self._txn_counter = 0
        self._txn_backlog_ready = False
        self._batch_update_queries = None

    def _query_all(self, sql):
        return self.client.execute(sql, show_internal_id=True).to_dict()

    def _scalar(self, sql):
        return self.client.execute(sql).scalar()

    def _query_pandas(self, sql):
        ensure_optional_imports()
        result = self.client.execute(sql, show_internal_id=True)
        if HAS_PANDAS:
            return result.to_pandas()
        return result.to_dict()

    def execute_materialized_query(self, sql):
        return self.client.execute(sql, show_internal_id=True).to_dict()

    def setup(self):
        ensure_optional_imports()
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None
        if os.path.exists(self.db_dir):
            shutil.rmtree(self.db_dir)
        self.client = ApexClient(self.db_dir, drop_if_exists=True)
        self.client.create_table('default')
        self.client.create_table('meta')
        self.client.use_table('meta')
        self.client.store({
            "city": [row[0] for row in META_CITY_POP],
            "pop": [row[1] for row in META_CITY_POP],
        })
        self.client.flush()
        self.client.use_table('default')
        self._next_id = 1
        self._txn_counter = 0
        self._txn_backlog_ready = False
        self._temp_csv_name = None
        self._temp_json_name = None

    def cold_start_setup(self):
        """Close and reopen client — clears all Python/Rust-side caches (arrow_batch_cache etc.)."""
        ensure_optional_imports()
        if self.client:
            self.client.close()
        self.client = ApexClient(self.db_dir)
        self.client.use_table('default')

    def bench_insert(self):
        self.client.store(self.data)
        self._next_id = self.n + 1

    def bench_count(self):
        return self._scalar("SELECT COUNT(*) FROM default")

    def bench_select_limit(self, limit=100):
        return self._query_all(f"SELECT * FROM default LIMIT {limit}")

    def bench_select_limit_10k(self):
        return self._query_all("SELECT * FROM default LIMIT 10000")

    def bench_projected_full_scan(self):
        return self._query_all("SELECT name, age, city FROM default")

    def bench_filtered_limit_100(self):
        return self._query_all("SELECT * FROM default WHERE age > 30 LIMIT 100")

    def bench_limit_offset_100(self):
        return self._query_all("SELECT * FROM default LIMIT 100 OFFSET 10000")

    def bench_filter_string(self):
        return self._query_all(
            "SELECT * FROM default WHERE name = 'user_5000'"
        )

    def bench_filter_range(self):
        return self._query_all(
            "SELECT * FROM default WHERE age BETWEEN 25 AND 35"
        )

    def bench_group_by(self):
        return self._query_all(
            "SELECT city, COUNT(*), AVG(score) FROM default GROUP BY city"
        )

    def bench_group_by_category(self):
        return self._query_all(
            "SELECT category, COUNT(*), AVG(score) FROM default GROUP BY category"
        )

    def bench_group_by_order_count(self):
        return self._query_all(
            "SELECT city, COUNT(*) AS cnt FROM default GROUP BY city ORDER BY cnt DESC LIMIT 5"
        )

    def bench_group_by_category_order_count(self):
        return self._query_all(
            "SELECT category, COUNT(*) AS cnt FROM default GROUP BY category ORDER BY cnt DESC LIMIT 5"
        )

    def bench_group_by_having(self):
        return self._query_all(
            "SELECT city, COUNT(*) as cnt, AVG(score) FROM default GROUP BY city HAVING cnt > 1000"
        )

    def bench_group_by_category_having(self):
        return self._query_all(
            "SELECT category, COUNT(*) as cnt, AVG(score) FROM default GROUP BY category HAVING cnt > 1000"
        )

    def setup_view_bench(self):
        try:
            self.client.execute("DROP VIEW bench_view")
        except Exception:
            pass
        self.client.execute(
            "CREATE VIEW bench_view AS "
            "SELECT city, COUNT(*) AS cnt, AVG(score) AS avg_score "
            "FROM default GROUP BY city"
        )

    def bench_view_select(self):
        return self._query_all(
            "SELECT city, cnt FROM bench_view WHERE cnt > 0 ORDER BY cnt DESC LIMIT 5"
        )

    def bench_order_limit(self):
        return self._query_all(
            "SELECT * FROM default ORDER BY score DESC LIMIT 100"
        )

    def bench_order_limit_asc(self):
        return self._query_all(
            "SELECT * FROM default ORDER BY score ASC LIMIT 100"
        )

    def bench_aggregation(self):
        return self._query_all(
            "SELECT COUNT(*), AVG(age), SUM(score), MIN(age), MAX(age) FROM default"
        )

    def setup_topk_join_canary(self):
        """Build the narrow-result/wide-source TopK JOIN regression fixture."""
        rng = np.random.default_rng(20260728)
        row_count = min(self.n, 20_000)
        dim = 16
        vectors = rng.standard_normal((row_count, dim), dtype=np.float32)
        vectors /= np.linalg.norm(vectors, axis=1, keepdims=True) + 1e-12
        self.client.execute(
            "CREATE TABLE __perf_topk "
            "(name TEXT, payload BLOB, vec FLOAT16_VECTOR)"
        )
        self.client.use_table("__perf_topk")
        for start in range(0, row_count, 2_000):
            end = min(start + 2_000, row_count)
            self.client.store(
                {
                    "name": [f"vec_{idx}" for idx in range(start, end)],
                    "payload": [b"x" * 2048] * (end - start),
                    "vec": [vectors[idx] for idx in range(start, end)],
                }
            )
        self.client.flush()
        query = ", ".join(f"{float(value):.8g}" for value in vectors[0])
        self._topk_join_canary_sql = (
            "SELECT p.name, k.dist FROM __perf_topk p "
            "JOIN (SELECT explode_rename("
            f"topk_distance(vec, [{query}], 8, 'cosine_distance'), "
            "'_id', 'dist') FROM __perf_topk) k ON p._id = k._id"
        )

    def bench_topk_join_canary(self):
        """TopK must materialize k IDs before reading projected source columns."""
        return self._query_all(self._topk_join_canary_sql)

    def bench_filtered_aggregation(self):
        return self._query_all(
            "SELECT COUNT(*), AVG(score), MAX(score) FROM default WHERE category = 'Electronics'"
        )

    def bench_filtered_aggregation_city(self):
        return self._query_all(
            "SELECT COUNT(*), AVG(score), MAX(score) FROM default WHERE city = 'Beijing'"
        )

    def bench_count_where_category(self):
        return self._scalar(
            "SELECT COUNT(*) FROM default WHERE category = 'Electronics'"
        )

    def bench_complex(self):
        return self._query_all(
            "SELECT city, AVG(score) as avg_s FROM default WHERE age BETWEEN 25 AND 50 GROUP BY city ORDER BY avg_s DESC LIMIT 5"
        )

    def bench_point_lookup(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        return self._query_all(
            f"SELECT * FROM default WHERE _id = {point_lookup_id}"
        )

    def bench_oltp_projected_point_lookup(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        return self.client.execute(
            f"SELECT name, age, score FROM default WHERE _id = {point_lookup_id}"
        ).to_dict()

    def bench_oltp_count_rows(self):
        return self.client.count_rows()

    def bench_oltp_direct_point_lookup(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        return self.client.retrieve(point_lookup_id)

    def bench_oltp_missing_point_lookup(self):
        missing_id = self.n + 100_000_000
        return self.client.retrieve(missing_id)

    def bench_retrieve_many(self):
        ids = self.shared_inputs["retrieve_many_ids"]
        id_list = ",".join(str(id_) for id_ in ids)
        return self._query_all(
            f"SELECT * FROM default WHERE _id IN ({id_list})"
        )

    def bench_oltp_projected_retrieve_10(self):
        ids = self.shared_inputs["retrieve_many_ids"][:10]
        id_list = ",".join(str(id_) for id_ in ids)
        return self.client.execute(
            f"SELECT name, age, score FROM default WHERE _id IN ({id_list})"
        ).to_dict()

    def bench_oltp_projected_retrieve_100(self):
        ids = self.shared_inputs["retrieve_many_ids"]
        id_list = ",".join(str(id_) for id_ in ids)
        return self.client.execute(
            f"SELECT name, age, score FROM default WHERE _id IN ({id_list})"
        ).to_dict()

    def bench_oltp_projected_limit_100(self):
        return self.client.execute("SELECT name, age, city FROM default LIMIT 100").to_dict()

    def bench_oltp_projected_string_eq(self):
        return self.client.execute("SELECT age, score, city FROM default WHERE name = 'user_5000'").to_dict()

    def bench_oltp_city_limit_10(self):
        return self.client.execute(
            "SELECT name, age FROM default WHERE city = 'Beijing' LIMIT 100"
        ).to_dict()

    def bench_oltp_insert_one(self):
        self.client.store(OLTP_ONE_ROW_DICT)
        self._next_id += 1

    def bench_oltp_insert_10_rows(self):
        data = {
            "name": [f"oltp_10_{self._txn_counter}_{i}" for i in range(10)],
            "age": [30 + i for i in range(10)],
            "score": [70.0 + i for i in range(10)],
            "city": [CITIES[i % len(CITIES)] for i in range(10)],
            "category": [CATEGORIES[i % len(CATEGORIES)] for i in range(10)],
        }
        self._txn_counter += 10
        self.client.store(data)
        self._next_id += 10

    def bench_oltp_insert_one_durable(self):
        self.client.store_durable_one(OLTP_ONE_ROW_DICT)
        self._next_id += 1

    def bench_oltp_insert_read_own_row(self):
        row_id = self._next_id
        self.client.store({
            "name": "oltp_insert_read",
            "age": 32,
            "score": 78.0,
            "city": "Shanghai",
            "category": "Books",
        })
        self._next_id += 1
        return self.client.retrieve(row_id)

    def bench_oltp_insert_count_visible(self):
        self.client.store({
            "name": "oltp_insert_count",
            "age": 33,
            "score": 79.0,
            "city": "Guangzhou",
            "category": "Books",
        })
        self._next_id += 1
        return self.client.count_rows()

    def bench_oltp_update_by_id(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        return self.client.execute(
            f"UPDATE default SET score = 77.0 WHERE _id = {point_lookup_id}"
        )

    def bench_oltp_batch_update_by_id(self):
        if self._batch_update_queries is None:
            self._batch_update_queries = [
                f"UPDATE default SET score = {70.0 + (i % 17):.1f} WHERE _id = {i + 1}"
                for i in range(10_000)
            ]
        return self.client.execute_batch(self._batch_update_queries)

    def bench_oltp_update_missing_id(self):
        missing_id = self.n + 100_000_000
        return self.client.execute(
            f"UPDATE default SET score = 77.0 WHERE _id = {missing_id}"
        )

    def bench_oltp_update_read_by_id(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        self.client.execute(
            f"UPDATE default SET score = 77.0 WHERE _id = {point_lookup_id}"
        )
        return self.client.execute(
            f"SELECT score FROM default WHERE _id = {point_lookup_id}"
        ).to_dict()

    def bench_oltp_replace_by_id(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        return self.client.replace(point_lookup_id, {
            "name": "user_5000",
            "age": 31,
            "score": 77.0,
            "city": "Beijing",
            "category": "Books",
        })

    def bench_oltp_insert_delete_by_id(self):
        row_id = self._next_id
        self.client.store({
            "name": "oltp_insert_delete",
            "age": 34,
            "score": 80.0,
            "city": "Shenzhen",
            "category": "Books",
        })
        self._next_id += 1
        return self.client.delete(id=row_id)

    def bench_oltp_delete_missing_id(self):
        missing_id = self.n + 100_000_000
        return self.client.delete(id=missing_id)

    def _next_txn_prefix(self, count=1):
        start = self._txn_counter
        self._txn_counter += count
        return f"txn_apex_{start}"

    def _txn_rows_sql(self, prefix, count):
        return [
            f"('{prefix}_{i}', {30 + (i % 25)}, {70.0 + (i % 17):.1f}, "
            f"'{CITIES[i % len(CITIES)]}', '{CATEGORIES[i % len(CATEGORIES)]}')"
            for i in range(count)
        ]

    def bench_txn_empty_commit(self):
        self.client.execute("BEGIN")
        self.client.execute("COMMIT")

    def bench_txn_empty_rollback(self):
        self.client.execute("BEGIN")
        self.client.execute("ROLLBACK")

    def bench_txn_read_count_commit(self):
        self.client.execute("BEGIN")
        result = self.client.execute("SELECT COUNT(*) FROM default").scalar()
        self.client.execute("COMMIT")
        return result

    def bench_txn_insert_one_commit(self):
        values = self._txn_rows_sql(self._next_txn_prefix(), 1)[0]
        self.client.execute("BEGIN")
        self.client.execute(f"INSERT INTO default (name, age, score, city, category) VALUES {values}")
        self.client.execute("COMMIT")
        self._next_id += 1

    def bench_txn_insert_10_commit(self):
        rows = self._txn_rows_sql(self._next_txn_prefix(10), 10)
        self.client.execute("BEGIN")
        for values in rows:
            self.client.execute(f"INSERT INTO default (name, age, score, city, category) VALUES {values}")
        self.client.execute("COMMIT")
        self._next_id += 10

    def bench_txn_multi_insert_10_commit(self):
        rows = self._txn_rows_sql(self._next_txn_prefix(10), 10)
        self.client.execute("BEGIN")
        self.client.execute(
            "INSERT INTO default (name, age, score, city, category) VALUES "
            + ", ".join(rows)
        )
        self.client.execute("COMMIT")
        self._next_id += 10

    def bench_txn_insert_100_commit(self):
        rows = self._txn_rows_sql(self._next_txn_prefix(100), 100)
        self.client.execute("BEGIN")
        for values in rows:
            self.client.execute(f"INSERT INTO default (name, age, score, city, category) VALUES {values}")
        self.client.execute("COMMIT")
        self._next_id += 100

    def bench_txn_multi_insert_100_commit(self):
        rows = self._txn_rows_sql(self._next_txn_prefix(100), 100)
        self.client.execute("BEGIN")
        self.client.execute(
            "INSERT INTO default (name, age, score, city, category) VALUES "
            + ", ".join(rows)
        )
        self.client.execute("COMMIT")
        self._next_id += 100

    def bench_txn_insert_10_rollback(self):
        rows = self._txn_rows_sql(self._next_txn_prefix(10), 10)
        self.client.execute("BEGIN")
        for values in rows:
            self.client.execute(f"INSERT INTO default (name, age, score, city, category) VALUES {values}")
        self.client.execute("ROLLBACK")

    def bench_txn_update_by_id_commit(self):
        point_lookup_id = self.shared_inputs["point_lookup_id"]
        self.client.execute("BEGIN")
        self.client.execute(f"UPDATE default SET score = 77.0 WHERE _id = {point_lookup_id}")
        self.client.execute("COMMIT")

    def bench_txn_insert_read_own_commit(self):
        prefix = self._next_txn_prefix()
        values = self._txn_rows_sql(prefix, 1)[0]
        self.client.execute("BEGIN")
        self.client.execute(f"INSERT INTO default (name, age, score, city, category) VALUES {values}")
        result = self.client.execute(
            f"SELECT * FROM default WHERE name = '{prefix}_0'",
            show_internal_id=True,
        ).to_dict()
        self.client.execute("COMMIT")
        self._next_id += 1
        return result

    def setup_txn_backlog_1500(self):
        if self._txn_backlog_ready:
            return
        for _ in range(TXN_BACKLOG_ROWS):
            values = self._txn_rows_sql(self._next_txn_prefix(), 1)[0]
            self.client.execute("BEGIN")
            self.client.execute(f"INSERT INTO default (name, age, score, city, category) VALUES {values}")
            self.client.execute("COMMIT")
            self._next_id += 1
        self._txn_backlog_ready = True

    def bench_txn_backlog_string_miss_commit(self):
        self.client.execute("BEGIN")
        result = self.client.execute(
            f"SELECT * FROM default WHERE name = '{TXN_BACKLOG_MISSING_NAME}'",
            show_internal_id=True,
        ).to_dict()
        self.client.execute("COMMIT")
        return result

    def bench_txn_backlog_count_commit(self):
        self.client.execute("BEGIN")
        result = self.client.execute("SELECT COUNT(*) FROM default").scalar()
        self.client.execute("COMMIT")
        return result

    def bench_txn_backlog_insert_read_own_by_name_commit(self):
        prefix = self._next_txn_prefix()
        values = self._txn_rows_sql(prefix, 1)[0]
        self.client.execute("BEGIN")
        self.client.execute(f"INSERT INTO default (name, age, score, city, category) VALUES {values}")
        result = self.client.execute(
            f"SELECT * FROM default WHERE name = '{prefix}_0'",
            show_internal_id=True,
        ).to_dict()
        self.client.execute("COMMIT")
        self._next_id += 1
        return result

    def bench_insert_1k(self):
        data_1k = {
            "name": [f"new_{i}" for i in range(1000)],
            "age": [25] * 1000,
            "score": [50.0] * 1000,
            "city": ["Beijing"] * 1000,
            "category": ["Books"] * 1000,
        }
        self.client.store(data_1k)
        self._next_id += 1000

    def bench_full_scan_pandas(self):
        return self._query_pandas("SELECT * FROM default")

    def bench_group_by_2cols(self):
        return self._query_all(
            "SELECT city, category, COUNT(*), AVG(score) FROM default GROUP BY city, category"
        )

    def bench_filter_like(self):
        return self._query_all(
            "SELECT * FROM default WHERE name LIKE 'user_1%'"
        )

    def bench_filter_multi_cond(self):
        return self._query_all(
            "SELECT * FROM default WHERE age > 30 AND score > 50.0"
        )

    def bench_order_by_multi(self):
        return self._query_all(
            "SELECT * FROM default ORDER BY city ASC, score DESC LIMIT 100"
        )

    def bench_count_distinct(self):
        return self._scalar(
            "SELECT COUNT(DISTINCT city) FROM default"
        )

    def bench_count_distinct_category(self):
        return self._scalar(
            "SELECT COUNT(DISTINCT category) FROM default"
        )

    def bench_filter_in(self):
        return self._query_all(
            "SELECT * FROM default WHERE city IN ('Beijing', 'Shanghai', 'Guangzhou')"
        )

    def bench_filter_numeric_in(self):
        return self._query_all(
            "SELECT * FROM default WHERE age IN (20, 25, 30, 35, 40, 45, 50, 55, 60)"
        )

    def bench_filter_or_cross_col(self):
        return self._query_all(
            "SELECT * FROM default WHERE age = 25 OR city = 'Beijing'"
        )

    def bench_filter_numeric_or(self):
        return self._query_all(
            "SELECT * FROM default WHERE age = 20 OR age = 30 OR age = 40 OR age = 50"
        )

    def bench_update_1k(self):
        return self.client.execute(
            "UPDATE default SET score = 50.0 WHERE age = 25"
        )

    def bench_delete_1k(self):
        data = {
            "name": [f"del_{i}" for i in range(1000)],
            "age": [99] * 1000,
            "score": [99.0] * 1000,
            "city": ["Beijing"] * 1000,
            "category": ["Books"] * 1000,
        }
        self.client.store(data)
        self._next_id += 1000
        self.client.execute("DELETE FROM default WHERE age = 99")

    def bench_delete_1k_setup(self):
        data = {
            "name": [f"del_{i}" for i in range(1000)],
            "age": [99] * 1000,
            "score": [99.0] * 1000,
            "city": ["Beijing"] * 1000,
            "category": ["Books"] * 1000,
        }
        self.client.store(data)
        self._next_id += 1000

    def bench_delete_1k_only(self):
        self.client.execute("DELETE FROM default WHERE age = 99")

    TABLE_OPS_NAME = "bench_table_ops"

    def _drop_table_ops(self, name=None):
        name = name or self.TABLE_OPS_NAME
        try:
            self.client.drop_table(name)
        except Exception:
            pass

    def _restore_default_table(self):
        try:
            self.client.use_table("default")
        except Exception:
            pass

    def bench_table_create_setup(self):
        self._restore_default_table()
        self._drop_table_ops()

    def bench_table_create(self):
        self.client.create_table(self.TABLE_OPS_NAME, {"k": "int64"})
        # DDL changes the current table; restore the dataset table so later
        # read-only sections (e.g. the OLAP Q/s harness) keep querying it.
        self._restore_default_table()

    def bench_table_drop_setup(self):
        self._restore_default_table()
        self._drop_table_ops()
        self.client.create_table(self.TABLE_OPS_NAME, {"k": "int64"})

    def bench_table_drop(self):
        self.client.drop_table(self.TABLE_OPS_NAME)
        self._restore_default_table()

    def bench_table_create_drop_cycle(self):
        self._drop_table_ops()
        self.client.create_table(self.TABLE_OPS_NAME, {"k": "int64"})
        self.client.drop_table(self.TABLE_OPS_NAME)
        self._restore_default_table()

    def bench_list_tables_setup(self):
        self._restore_default_table()
        self._drop_table_ops()
        for i in range(10):
            self._drop_table_ops(f"{self.TABLE_OPS_NAME}_{i}")
        for i in range(10):
            self.client.create_table(f"{self.TABLE_OPS_NAME}_{i}", {"k": "int64"})

    def bench_list_tables(self):
        result = self.client.list_tables()
        self._restore_default_table()
        return result

    def bench_alter_table_add_column_setup(self):
        self._restore_default_table()
        self._drop_table_ops()
        self.client.create_table(self.TABLE_OPS_NAME, {"k": "int64"})

    def bench_alter_table_add_column(self):
        self.client.execute(
            f"ALTER TABLE {self.TABLE_OPS_NAME} ADD COLUMN c INT64"
        )
        self._restore_default_table()

    def bench_window_row_number(self):
        return self._query_all(
            "SELECT name, city, score, "
            "ROW_NUMBER() OVER (PARTITION BY city ORDER BY score DESC) as rn "
            "FROM default LIMIT 1000"
        )

    # --- Advanced SQL: joins, set operations, subqueries, expressions ---

    def bench_join_group_order_limit(self):
        return self._query_all(
            "SELECT b.city, COUNT(*) AS c FROM default b JOIN meta m ON b.city = m.city "
            "GROUP BY b.city ORDER BY c DESC LIMIT 5"
        )

    def bench_left_join_count(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM default b LEFT JOIN meta m ON b.city = m.city"
        )

    def bench_left_join_extra_on(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM default b LEFT JOIN meta m "
            "ON b.city = m.city AND m.pop > 15000000"
        )

    def bench_full_outer_join_bounded(self):
        return self._query_all(
            "SELECT b.city, m.pop FROM default b FULL OUTER JOIN meta m ON b.city = m.city "
            "WHERE b.age < 25 LIMIT 1000"
        )

    def bench_comma_cross_join(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM default b CROSS JOIN meta m "
            "WHERE b.age < 20 AND m.pop > 0"
        )

    def bench_union_all(self):
        return self._query_all(
            "SELECT city FROM default WHERE age = 25 "
            "UNION ALL SELECT city FROM default WHERE age = 26 ORDER BY city LIMIT 100"
        )

    def bench_union_distinct(self):
        return self._query_all(
            "SELECT city FROM default WHERE age = 25 "
            "UNION SELECT city FROM default WHERE age = 26 ORDER BY city"
        )

    def bench_intersect(self):
        return self._query_all(
            "SELECT city FROM default WHERE age BETWEEN 20 AND 30 "
            "INTERSECT SELECT city FROM default WHERE age BETWEEN 25 AND 35 ORDER BY city"
        )

    def bench_except(self):
        return self._query_all(
            "SELECT city FROM default WHERE age BETWEEN 20 AND 30 "
            "EXCEPT SELECT city FROM default WHERE age BETWEEN 25 AND 35 ORDER BY city"
        )

    def bench_in_subquery(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM default "
            "WHERE city IN (SELECT city FROM meta WHERE pop > 15000000)"
        )

    def bench_exists_subquery(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM default b WHERE EXISTS "
            "(SELECT 1 FROM meta m WHERE m.city = b.city AND m.pop > 15000000)"
        )

    def bench_derived_table(self):
        return self._query_all(
            "SELECT city, cnt FROM "
            "(SELECT city, COUNT(*) AS cnt FROM default GROUP BY city) d "
            "ORDER BY cnt DESC LIMIT 5"
        )

    def bench_cte(self):
        return self._query_all(
            "WITH c AS (SELECT city, AVG(score) AS av FROM default GROUP BY city) "
            "SELECT city FROM c WHERE av > 50 ORDER BY av DESC LIMIT 5"
        )

    def bench_case_aggregate(self):
        return self._query_all(
            "SELECT city, COUNT(CASE WHEN age > 40 THEN 1 END) AS c "
            "FROM default GROUP BY city ORDER BY city"
        )

    def bench_string_functions(self):
        return self._query_all(
            "SELECT UPPER(city) AS u, LENGTH(name) AS ln, SUBSTR(name, 1, 5) AS sub, "
            "CONCAT(city, '-', category) AS cc, TRIM(category) AS tr "
            "FROM default WHERE age BETWEEN 30 AND 40 LIMIT 1000"
        )

    def bench_numeric_functions(self):
        return self._query_all(
            "SELECT ROUND(score, 0) AS r, ABS(age - 40) AS a, FLOOR(score) AS f, "
            "CEIL(score) AS c, MOD(age, 7) AS m "
            "FROM default WHERE age BETWEEN 30 AND 40 LIMIT 1000"
        )

    def bench_coalesce_nullif(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM default "
            "WHERE COALESCE(NULLIF(category, 'Books'), 'none') = 'Books'"
        )

    def bench_not_filter(self):
        return self._scalar(
            "SELECT COUNT(*) AS c FROM default "
            "WHERE age NOT BETWEEN 20 AND 40 AND name NOT LIKE 'user_5%'"
        )

    def bench_deep_offset(self):
        return self._query_all(
            "SELECT name, age FROM default ORDER BY age, name LIMIT 100 OFFSET 100000"
        )

    def bench_order_by_expression(self):
        return self._query_all(
            "SELECT name FROM default ORDER BY LENGTH(name) DESC, age LIMIT 100"
        )

    def bench_window_sum_over(self):
        return self._query_all(
            "SELECT city, score, SUM(score) OVER (PARTITION BY city ORDER BY age) AS run "
            "FROM default LIMIT 1000"
        )

    def bench_window_rank(self):
        return self._query_all(
            "SELECT city, RANK() OVER (PARTITION BY city ORDER BY score DESC) AS rk "
            "FROM default LIMIT 1000"
        )

    def bench_window_lag(self):
        return self._query_all(
            "SELECT city, LAG(score) OVER (PARTITION BY city ORDER BY age) AS prev "
            "FROM default LIMIT 1000"
        )

    def bench_distinct_rows(self):
        return self._query_all(
            "SELECT DISTINCT city, category FROM default ORDER BY city, category"
        )

    def bench_multi_col_group_having(self):
        return self._query_all(
            "SELECT city, category, COUNT(*) AS c FROM default "
            "GROUP BY city, category HAVING c > 2000 ORDER BY city, category LIMIT 20"
        )

    def bench_fts_build(self):
        try:
            # Close + reopen to clear the executor STORAGE_CACHE before backfill,
            # ensuring CREATE FTS INDEX reads fresh row data from disk.
            self.client.close()
            self.client = ApexClient(self.db_dir)
            self.client.use_table('default')
            self.client.execute(
                "CREATE FTS INDEX ON default (name, city, category)"
            )
            self.client._fts_tables['default'] = {
                'enabled': True,
                'index_fields': ['name', 'city', 'category'],
                'config': {'lazy_load': False, 'cache_size': 10000},
            }
            self._fts_ready = True
        except Exception:
            self._fts_ready = False

    def bench_fts_search(self):
        if not getattr(self, '_fts_ready', False):
            return None
        return self.client.search_text('Electronics').tolist()

    def bench_copy_to_csv(self):
        path = os.path.join(self.db_dir, "bench_export.csv")
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        return self.client.execute(f"COPY default TO '{path}'").scalar()

    def bench_copy_to_json(self):
        path = os.path.join(self.db_dir, "bench_export.jsonl")
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        return self.client.execute(f"COPY default TO '{path}'").scalar()

    def bench_csv_read_count(self):
        return self._scalar(f"SELECT COUNT(*) FROM read_csv('{self.csv_path}')")

    def bench_csv_read_filter_group(self):
        return self._query_all(
            f"SELECT city, COUNT(*) FROM read_csv('{self.csv_path}') WHERE age > 30 GROUP BY city"
        )

    def bench_csv_read_limit_1k(self):
        return self._query_all(f"SELECT * FROM read_csv('{self.csv_path}') LIMIT 1000")

    def bench_json_read_count(self):
        return self._scalar(f"SELECT COUNT(*) FROM read_json('{self.json_path}')")

    def bench_temp_csv_create_query(self):
        if self._temp_csv_name is None:
            self._temp_csv_name = f"bench_csv_temp_{uuid.uuid4().hex[:8]}"
            self.client.register_temp_table(self._temp_csv_name, self.csv_path)
        return self._scalar(f"SELECT AVG(score) FROM {self._temp_csv_name} WHERE age > 30")

    def bench_csv_read_order_limit(self):
        return self._query_all(
            f"SELECT * FROM read_csv('{self.csv_path}') ORDER BY score DESC LIMIT 100"
        )

    def bench_json_read_filter(self):
        return self._scalar(
            f"SELECT COUNT(*) FROM read_json('{self.json_path}') WHERE age > 30"
        )

    def bench_json_read_order_limit(self):
        return self._query_all(
            f"SELECT * FROM read_json('{self.json_path}') ORDER BY score DESC LIMIT 100"
        )

    def bench_json_read_group_category(self):
        return self._query_all(
            f"SELECT category, COUNT(*) FROM read_json('{self.json_path}') GROUP BY category"
        )

    def close(self):
        if self.client:
            self.client.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# (display_name, method_name, is_write, is_cold, is_warm_nogc, setup_method)
# is_cold=True      -> cold_start_setup() per iter (reopen connection/client), no gc
# is_warm_nogc=True -> warm cached backend, no gc
# setup_method      -> if not None, call bench.{setup_method}() before each timed iter
BENCHMARKS = [
    ("Bulk Insert (N rows; default fair)", "bench_insert",         True,  False, False, None),
    ("COUNT(*)",                         "bench_count",            False, False, False, None),
    ("SELECT * LIMIT 100 (cold reopen)", "bench_select_limit",     False, True,  False, None),
    ("SELECT * LIMIT 100 (warm cache)",  "bench_select_limit",     False, False, True,  None),
    ("SELECT * LIMIT 10K (cold reopen)", "bench_select_limit_10k", False, True,  False, None),
    ("SELECT * LIMIT 10K (warm cache)",  "bench_select_limit_10k", False, False, True,  None),
    ("Projection full scan (3 cols)",    "bench_projected_full_scan", False, False, False, None),
    ("Filtered LIMIT 100 (age>30)",      "bench_filtered_limit_100", False, False, True,  None),
    ("LIMIT 100 OFFSET 10K",             "bench_limit_offset_100", False, False, True,  None),
    ("Filter (name = 'user_5000')",      "bench_filter_string",    False, False, False, None),
    ("Filter (age BETWEEN 25 AND 35)",   "bench_filter_range",     False, False, False, None),
    ("GROUP BY city (10 groups)",        "bench_group_by",         False, False, False, None),
    ("GROUP BY category (10 groups)",    "bench_group_by_category", False, False, False, None),
    ("GROUP BY city ORDER BY count",     "bench_group_by_order_count", False, False, False, None),
    ("GROUP BY category ORDER BY count", "bench_group_by_category_order_count", False, False, False, None),
    ("GROUP BY + HAVING",                "bench_group_by_having",  False, False, False, None),
    ("GROUP BY category + HAVING",       "bench_group_by_category_having", False, False, False, None),
    ("Persistent VIEW select",           "bench_view_select",      False, False, False, "setup_view_bench"),
    ("ORDER BY score LIMIT 100",         "bench_order_limit",      False, False, False, None),
    ("ORDER BY score ASC LIMIT 100",     "bench_order_limit_asc",  False, False, False, None),
    ("Aggregation (5 funcs)",            "bench_aggregation",      False, False, False, None),
    ("Filtered aggregation (category)",  "bench_filtered_aggregation", False, False, False, None),
    ("Filtered aggregation (city)",      "bench_filtered_aggregation_city", False, False, False, None),
    ("COUNT WHERE category",             "bench_count_where_category", False, False, True,  None),
    ("Complex (Filter+Group+Order)",     "bench_complex",          False, False, False, None),
    ("Point Lookup (SQL by ID)",         "bench_point_lookup",     False, False, True,  None),
    ("Retrieve Many (SQL, 100 IDs)",     "bench_retrieve_many",    False, False, False, None),
    # --- New cases ---
    ("SELECT * -> pandas (full scan)",   "bench_full_scan_pandas", False, False, False, None),
    ("GROUP BY city,category (100 grp)","bench_group_by_2cols",   False, False, False, None),
    ("LIKE filter (name LIKE user_1%)",  "bench_filter_like",      False, False, False, None),
    ("Multi-cond (age>30 AND score>50)", "bench_filter_multi_cond",False, False, False, None),
    ("ORDER BY city,score DESC LIMIT100","bench_order_by_multi",   False, False, False, None),
    ("COUNT(DISTINCT city)",             "bench_count_distinct",   False, False, False, None),
    ("COUNT(DISTINCT category)",         "bench_count_distinct_category", False, False, False, None),
    ("IN filter (city IN 3 cities)",     "bench_filter_in",        False, False, False, None),
    ("Numeric IN (age IN 9 values)",      "bench_filter_numeric_in",False, False, False, None),
    ("OR cross-col (age=25 OR city=BJ)",  "bench_filter_or_cross_col",False,False,False, None),
    ("Numeric OR (age=20|30|40|50)",      "bench_filter_numeric_or",False, False, False, None),
    # --- Window ---
    ("Window ROW_NUMBER PARTITION BY city",  "bench_window_row_number", False, False, False, None),
    # --- Advanced SQL: joins, set operations, subqueries, expressions ---
    ("JOIN GROUP BY ORDER LIMIT",        "bench_join_group_order_limit",    False, False, False, None),
    ("LEFT JOIN COUNT",                  "bench_left_join_count",           False, False, False, None),
    ("LEFT JOIN extra ON predicate",     "bench_left_join_extra_on",        False, False, False, None),
    ("FULL OUTER JOIN (bounded)",        "bench_full_outer_join_bounded",   False, False, False, None),
    ("CROSS JOIN COUNT",                 "bench_comma_cross_join",          False, False, False, None),
    ("UNION ALL (ordered)",              "bench_union_all",                 False, False, False, None),
    ("UNION DISTINCT (ordered)",         "bench_union_distinct",            False, False, False, None),
    ("INTERSECT (ordered)",              "bench_intersect",                 False, False, False, None),
    ("EXCEPT (ordered)",                 "bench_except",                    False, False, False, None),
    ("IN subquery COUNT",                "bench_in_subquery",               False, False, False, None),
    ("EXISTS subquery COUNT",            "bench_exists_subquery",           False, False, False, None),
    ("Derived table GROUP BY",           "bench_derived_table",             False, False, False, None),
    ("CTE with AVG filter",              "bench_cte",                       False, False, False, None),
    ("CASE aggregate GROUP BY",          "bench_case_aggregate",            False, False, False, None),
    ("String functions (UPPER/LENGTH/SUBSTR/CONCAT/TRIM)", "bench_string_functions", False, False, False, None),
    ("Numeric functions (ROUND/ABS/FLOOR/CEIL/MOD)", "bench_numeric_functions", False, False, False, None),
    ("COALESCE/NULLIF filter",           "bench_coalesce_nullif",           False, False, False, None),
    ("NOT filter (age NOT BETWEEN, name NOT LIKE)", "bench_not_filter",     False, False, False, None),
    ("Deep offset (LIMIT 100 OFFSET 100K)", "bench_deep_offset",            False, False, False, None),
    ("ORDER BY expression (LENGTH)",     "bench_order_by_expression",       False, False, False, None),
    ("Window SUM OVER (running)",        "bench_window_sum_over",           False, False, False, None),
    ("Window RANK (partitioned)",        "bench_window_rank",               False, False, False, None),
    ("Window LAG (partitioned)",         "bench_window_lag",                False, False, False, None),
    ("DISTINCT (city, category)",        "bench_distinct_rows",             False, False, False, None),
    ("GROUP BY 2 cols + HAVING",         "bench_multi_col_group_having",    False, False, False, None),
    # --- File reading / temp table benchmarks ---
    ("CSV Read + COUNT(*)",               "bench_csv_read_count",        False, False, False, None),
    ("CSV Read + Filter + GROUP BY",      "bench_csv_read_filter_group", False, False, False, None),
    ("CSV Read + Full Scan LIMIT 1000",   "bench_csv_read_limit_1k",     False, False, False, None),
    ("JSON Read + COUNT(*)",              "bench_json_read_count",       False, False, False, None),
    ("JSON Read + Filter",                "bench_json_read_filter",      False, False, False, None),
    ("JSON Read + GROUP BY category",     "bench_json_read_group_category", False, False, False, None),
    ("Temp Table (CSV) Query (filter+agg)","bench_temp_csv_create_query", False, False, True,  None),
    ("JSON Read + ORDER BY LIMIT 100",    "bench_json_read_order_limit", False, False, False, None),
    ("CSV Read + ORDER BY LIMIT 100",     "bench_csv_read_order_limit",  False, False, False, None),
    # --- OLTP read microbenchmarks run after analytical/file scans but before
    # mutation-heavy DML so latency is measured on a clean loaded table.
    ("COUNT(*) (direct API)",            "bench_oltp_count_rows",  False, False, True,  None),
    ("Point lookup (projected SQL)",     "bench_oltp_projected_point_lookup", False, False, True, None),
    ("Point lookup (direct full row)",   "bench_oltp_direct_point_lookup", False, False, True, None),
    ("Missing ID lookup",                "bench_oltp_missing_point_lookup", False, False, True, None),
    ("Retrieve 10 IDs (projected SQL)",  "bench_oltp_projected_retrieve_10", False, False, True, None),
    ("Retrieve 100 IDs (projected SQL)", "bench_oltp_projected_retrieve_100", False, False, True, None),
    ("SELECT 3 cols LIMIT 100",          "bench_oltp_projected_limit_100", False, False, True, None),
    ("String equality (projected)",      "bench_oltp_projected_string_eq", False, False, True, None),
    ("City filter LIMIT 100",            "bench_oltp_city_limit_10", False, False, True, None),
    # --- Bulk DML and FTS build run last so mutation-heavy paths do not
    # perturb read-only OLAP/OLTP latency metrics.
    ("Insert 1K rows (default fair)",    "bench_insert_1k",        False, False, False, None),
    ("UPDATE rows (age=25; idempotent)", "bench_update_1k",        False, False, False, None),
    ("Store+DELETE 1K (combined)",       "bench_delete_1k",        False, False, False, None),
    ("DELETE 1K (pure delete; setup rows)", "bench_delete_1k_only", False, False, False, "bench_delete_1k_setup"),
    ("Insert 1 row (default fair)",      "bench_oltp_insert_one", False, False, True, None),
    ("Insert+Read own row",              "bench_oltp_insert_read_own_row", False, False, True, None),
    ("Insert+COUNT visible",             "bench_oltp_insert_count_visible", False, False, True, None),
    ("UPDATE by ID",                     "bench_oltp_update_by_id", False, False, True, None),
    ("Batch UPDATE by ID (10K)",         "bench_oltp_batch_update_by_id", True, False, False, None),
    ("UPDATE missing ID",                "bench_oltp_update_missing_id", False, False, True, None),
    ("UPDATE+Read by ID",                "bench_oltp_update_read_by_id", False, False, True, None),
    ("Replace row by ID",                "bench_oltp_replace_by_id", False, False, True, None),
    ("Insert+DELETE by ID",              "bench_oltp_insert_delete_by_id", False, False, True, None),
    ("DELETE missing ID",                "bench_oltp_delete_missing_id", False, False, True, None),
    ("FTS Index Build (name,city,category)", "bench_fts_build",         True,  False, False, None),
    ("FTS Search ('Electronics')",           "bench_fts_search",        False, False, False, None),
    # --- Table operations (DDL-level) run last: they mutate the catalog and
    # must not perturb read-only OLAP/OLTP metrics.
    ("Table CREATE (1 col)",                 "bench_table_create",          False, False, False, "bench_table_create_setup"),
    ("Table DROP",                           "bench_table_drop",            False, False, False, "bench_table_drop_setup"),
    ("Table CREATE+DROP cycle",              "bench_table_create_drop_cycle", False, False, False, "bench_table_create_setup"),
    ("List tables (10)",                     "bench_list_tables",           False, False, False, "bench_list_tables_setup"),
    ("ALTER TABLE ADD COLUMN",               "bench_alter_table_add_column", False, False, False, "bench_alter_table_add_column_setup"),
]

OLAP_BENCHMARK_NAMES = [
    "COUNT(*)",
    "SELECT * LIMIT 100 (cold reopen)",
    "SELECT * LIMIT 100 (warm cache)",
    "SELECT * LIMIT 10K (cold reopen)",
    "SELECT * LIMIT 10K (warm cache)",
    "Projection full scan (3 cols)",
    "Filtered LIMIT 100 (age>30)",
    "LIMIT 100 OFFSET 10K",
    "Filter (name = 'user_5000')",
    "Filter (age BETWEEN 25 AND 35)",
    "GROUP BY city (10 groups)",
    "GROUP BY category (10 groups)",
    "GROUP BY city ORDER BY count",
    "GROUP BY category ORDER BY count",
    "GROUP BY + HAVING",
    "GROUP BY category + HAVING",
    "Persistent VIEW select",
    "ORDER BY score LIMIT 100",
    "ORDER BY score ASC LIMIT 100",
    "Aggregation (5 funcs)",
    "Filtered aggregation (category)",
    "Filtered aggregation (city)",
    "COUNT WHERE category",
    "Complex (Filter+Group+Order)",
    "SELECT * -> pandas (full scan)",
    "GROUP BY city,category (100 grp)",
    "LIKE filter (name LIKE user_1%)",
    "Multi-cond (age>30 AND score>50)",
    "ORDER BY city,score DESC LIMIT100",
    "COUNT(DISTINCT city)",
    "COUNT(DISTINCT category)",
    "IN filter (city IN 3 cities)",
    "Numeric IN (age IN 9 values)",
    "OR cross-col (age=25 OR city=BJ)",
    "Numeric OR (age=20|30|40|50)",
    "Window ROW_NUMBER PARTITION BY city",
    "JOIN GROUP BY ORDER LIMIT",
    "LEFT JOIN COUNT",
    "LEFT JOIN extra ON predicate",
    "FULL OUTER JOIN (bounded)",
    "CROSS JOIN COUNT",
    "UNION ALL (ordered)",
    "UNION DISTINCT (ordered)",
    "INTERSECT (ordered)",
    "EXCEPT (ordered)",
    "IN subquery COUNT",
    "EXISTS subquery COUNT",
    "Derived table GROUP BY",
    "CTE with AVG filter",
    "CASE aggregate GROUP BY",
    "String functions (UPPER/LENGTH/SUBSTR/CONCAT/TRIM)",
    "Numeric functions (ROUND/ABS/FLOOR/CEIL/MOD)",
    "COALESCE/NULLIF filter",
    "NOT filter (age NOT BETWEEN, name NOT LIKE)",
    "Deep offset (LIMIT 100 OFFSET 100K)",
    "ORDER BY expression (LENGTH)",
    "Window SUM OVER (running)",
    "Window RANK (partitioned)",
    "Window LAG (partitioned)",
    "DISTINCT (city, category)",
    "GROUP BY 2 cols + HAVING",
    "CSV Read + COUNT(*)",
    "CSV Read + Filter + GROUP BY",
    "CSV Read + Full Scan LIMIT 1000",
    "JSON Read + COUNT(*)",
    "JSON Read + Filter",
    "JSON Read + GROUP BY category",
    "Temp Table (CSV) Query (filter+agg)",
    "JSON Read + ORDER BY LIMIT 100",
    "CSV Read + ORDER BY LIMIT 100",
]

# The public scoreboard now includes every cross-engine OLAP metric that this
# harness can run with comparable setup and materialized result semantics.
PUBLIC_OLAP_BENCHMARK_NAMES = list(OLAP_BENCHMARK_NAMES)

OLTP_FAIR_BENCHMARK_NAMES = [
    "Bulk Insert (N rows; default fair)",
    "Point Lookup (SQL by ID)",
    "Retrieve Many (SQL, 100 IDs)",
    "COUNT(*) (direct API)",
    "Point lookup (projected SQL)",
    "Point lookup (direct full row)",
    "Missing ID lookup",
    "Retrieve 10 IDs (projected SQL)",
    "Retrieve 100 IDs (projected SQL)",
    "SELECT 3 cols LIMIT 100",
    "String equality (projected)",
    "City filter LIMIT 100",
    "Insert 1 row (default fair)",
    "Insert+Read own row",
    "Insert+COUNT visible",
    "UPDATE by ID",
    "UPDATE missing ID",
    "UPDATE+Read by ID",
    "Replace row by ID",
    "Insert+DELETE by ID",
    "DELETE missing ID",
    "Insert 1K rows (default fair)",
    "UPDATE rows (age=25; idempotent)",
    "Store+DELETE 1K (combined)",
    "DELETE 1K (pure delete; setup rows)",
    "FTS Index Build (name,city,category)",
    "FTS Search ('Electronics')",
    "Table CREATE (1 col)",
    "Table DROP",
    "Table CREATE+DROP cycle",
    "List tables (10)",
    "ALTER TABLE ADD COLUMN",
]

FILE_BACKED_METHODS = {
    "bench_csv_read_count",
    "bench_csv_read_filter_group",
    "bench_csv_read_limit_1k",
    "bench_json_read_count",
    "bench_json_read_filter",
    "bench_json_read_group_category",
    "bench_temp_csv_create_query",
    "bench_json_read_order_limit",
    "bench_csv_read_order_limit",
}

FAIR_WORKLOAD_GROUPS = [
    (
        "Load & Index",
        [
            "Bulk Insert (N rows; default fair)",
            "FTS Index Build (name,city,category)",
        ],
    ),
    (
        "Point & Limited Reads",
        [
            "COUNT(*)",
            "SELECT * LIMIT 100 (cold reopen)",
            "SELECT * LIMIT 100 (warm cache)",
            "SELECT * LIMIT 10K (cold reopen)",
            "SELECT * LIMIT 10K (warm cache)",
            "Filtered LIMIT 100 (age>30)",
            "LIMIT 100 OFFSET 10K",
            "Point Lookup (SQL by ID)",
            "Retrieve Many (SQL, 100 IDs)",
            "COUNT(*) (direct API)",
            "Point lookup (projected SQL)",
            "Point lookup (direct full row)",
            "Missing ID lookup",
            "Retrieve 10 IDs (projected SQL)",
            "Retrieve 100 IDs (projected SQL)",
            "SELECT 3 cols LIMIT 100",
            "City filter LIMIT 100",
        ],
    ),
    (
        "Filtering",
        [
            "Filter (name = 'user_5000')",
            "Filter (age BETWEEN 25 AND 35)",
            "LIKE filter (name LIKE user_1%)",
            "Multi-cond (age>30 AND score>50)",
            "IN filter (city IN 3 cities)",
            "Numeric IN (age IN 9 values)",
            "OR cross-col (age=25 OR city=BJ)",
            "Numeric OR (age=20|30|40|50)",
            "String equality (projected)",
            "COUNT WHERE category",
            "NOT filter (age NOT BETWEEN, name NOT LIKE)",
        ],
    ),
    (
        "Aggregation",
        [
            "GROUP BY city (10 groups)",
            "GROUP BY category (10 groups)",
            "GROUP BY city ORDER BY count",
            "GROUP BY category ORDER BY count",
            "GROUP BY + HAVING",
            "GROUP BY category + HAVING",
            "Aggregation (5 funcs)",
            "Filtered aggregation (category)",
            "Filtered aggregation (city)",
            "Complex (Filter+Group+Order)",
            "GROUP BY city,category (100 grp)",
            "COUNT(DISTINCT city)",
            "COUNT(DISTINCT category)",
            "GROUP BY 2 cols + HAVING",
        ],
    ),
    (
        "Ordering, Window, View",
        [
            "Persistent VIEW select",
            "ORDER BY score LIMIT 100",
            "ORDER BY score ASC LIMIT 100",
            "ORDER BY city,score DESC LIMIT100",
            "Window ROW_NUMBER PARTITION BY city",
            "Window SUM OVER (running)",
            "Window RANK (partitioned)",
            "Window LAG (partitioned)",
            "Deep offset (LIMIT 100 OFFSET 100K)",
        ],
    ),
    (
        "Full Materialization",
        [
            "Projection full scan (3 cols)",
            "SELECT * -> pandas (full scan)",
            "DISTINCT (city, category)",
        ],
    ),
    (
        "Joins",
        [
            "JOIN GROUP BY ORDER LIMIT",
            "LEFT JOIN COUNT",
            "LEFT JOIN extra ON predicate",
            "FULL OUTER JOIN (bounded)",
            "CROSS JOIN COUNT",
        ],
    ),
    (
        "Set Operations",
        [
            "UNION ALL (ordered)",
            "UNION DISTINCT (ordered)",
            "INTERSECT (ordered)",
            "EXCEPT (ordered)",
        ],
    ),
    (
        "Subqueries & CTE",
        [
            "IN subquery COUNT",
            "EXISTS subquery COUNT",
            "Derived table GROUP BY",
            "CTE with AVG filter",
        ],
    ),
    (
        "Expression Evaluation",
        [
            "CASE aggregate GROUP BY",
            "String functions (UPPER/LENGTH/SUBSTR/CONCAT/TRIM)",
            "Numeric functions (ROUND/ABS/FLOOR/CEIL/MOD)",
            "COALESCE/NULLIF filter",
            "ORDER BY expression (LENGTH)",
        ],
    ),
    (
        "File Scan",
        [
            "CSV Read + COUNT(*)",
            "CSV Read + Filter + GROUP BY",
            "CSV Read + Full Scan LIMIT 1000",
            "JSON Read + COUNT(*)",
            "JSON Read + Filter",
            "JSON Read + GROUP BY category",
            "Temp Table (CSV) Query (filter+agg)",
            "JSON Read + ORDER BY LIMIT 100",
            "CSV Read + ORDER BY LIMIT 100",
        ],
    ),
    (
        "DML",
        [
            "Insert 1K rows (default fair)",
            "Insert 1 row (default fair)",
            "Insert+Read own row",
            "Insert+COUNT visible",
            "UPDATE by ID",
            "UPDATE missing ID",
            "UPDATE+Read by ID",
            "Replace row by ID",
            "Insert+DELETE by ID",
            "DELETE missing ID",
            "UPDATE rows (age=25; idempotent)",
            "Store+DELETE 1K (combined)",
            "DELETE 1K (pure delete; setup rows)",
        ],
    ),
    (
        "Search",
        [
            "FTS Search ('Electronics')",
        ],
    ),
    (
        "Table Ops",
        [
            "Table CREATE (1 col)",
            "Table DROP",
            "Table CREATE+DROP cycle",
            "List tables (10)",
            "ALTER TABLE ADD COLUMN",
        ],
    ),
]

_BENCHMARK_BY_NAME = {spec[0]: spec for spec in BENCHMARKS}
OLAP_BENCHMARK_SECTIONS = [
    (
        "OLAP Fair Metrics",
        "Analytical scans, filters, grouping, ordering, windows, and full-result materialization.",
        [_BENCHMARK_BY_NAME[name] for name in OLAP_BENCHMARK_NAMES],
    ),
]

OLTP_BENCHMARK_SECTIONS = [
    (
        "OLTP Fair Metrics",
        "Load, indexing/search, point reads, small writes, updates, and deletes on the loaded table.",
        [_BENCHMARK_BY_NAME[name] for name in OLTP_FAIR_BENCHMARK_NAMES],
    ),
]


def olap_benchmark_names_for_profile(profile: str):
    if normalize_profile(profile) == PROFILE_PUBLIC:
        return PUBLIC_OLAP_BENCHMARK_NAMES
    return OLAP_BENCHMARK_NAMES


def benchmark_sections_for_profile(profile: str):
    olap_names = olap_benchmark_names_for_profile(profile)
    olap_sections = [
        (
            "OLAP Fair Metrics",
            "Analytical scans, filters, grouping, ordering, windows, and full-result materialization.",
            [_BENCHMARK_BY_NAME[name] for name in olap_names],
        ),
    ]
    oltp_sections = list(OLTP_BENCHMARK_SECTIONS)
    return olap_sections, oltp_sections


def benchmark_specs_for_profile(profile: str):
    olap_sections, oltp_sections = benchmark_sections_for_profile(profile)
    selected = {spec[0] for _, _, specs in (olap_sections + oltp_sections) for spec in specs}
    return [spec for spec in BENCHMARKS if spec[0] in selected]


def selected_benchmarks_need_files(benchmark_specs):
    return any(spec[1] in FILE_BACKED_METHODS for spec in benchmark_specs)


def profile_runs_extended_sections(profile: str) -> bool:
    return normalize_profile(profile) == PROFILE_EXTENDED


OLTP_DEFAULT_BENCHMARKS = [
    ("COUNT(*) (direct API)", "bench_oltp_count_rows"),
    ("Point lookup (projected SQL)", "bench_oltp_projected_point_lookup"),
    ("Point lookup (direct full row)", "bench_oltp_direct_point_lookup"),
    ("Missing ID lookup", "bench_oltp_missing_point_lookup"),
    ("Retrieve 10 IDs (projected SQL)", "bench_oltp_projected_retrieve_10"),
    ("Retrieve 100 IDs (projected SQL)", "bench_oltp_projected_retrieve_100"),
    ("SELECT 3 cols LIMIT 100", "bench_oltp_projected_limit_100"),
    ("String equality (projected)", "bench_oltp_projected_string_eq"),
    ("City filter LIMIT 100", "bench_oltp_city_limit_10"),
    ("Insert 1 row (default fair)", "bench_oltp_insert_one"),
    ("Insert+Read own row", "bench_oltp_insert_read_own_row"),
    ("Insert+COUNT visible", "bench_oltp_insert_count_visible"),
    ("UPDATE by ID", "bench_oltp_update_by_id"),
    ("UPDATE missing ID", "bench_oltp_update_missing_id"),
    ("UPDATE+Read by ID", "bench_oltp_update_read_by_id"),
    ("Replace row by ID", "bench_oltp_replace_by_id"),
    ("Insert+DELETE by ID", "bench_oltp_insert_delete_by_id"),
    ("DELETE missing ID", "bench_oltp_delete_missing_id"),
]

MICRO_MEDIAN_BENCHMARK_METHODS = {
    method_name for _, method_name in OLTP_DEFAULT_BENCHMARKS
}
MICRO_MEDIAN_BENCHMARK_METHODS.update({
    "bench_count_where_category",
    "bench_point_lookup",
    "bench_temp_csv_create_query",
})

OLTP_APEX_DIAGNOSTIC_BENCHMARKS = [
    ("Insert 10 rows (small-batch API diagnostic)", "bench_oltp_insert_10_rows"),
    ("COPY TO CSV (Apex-only export)", "bench_copy_to_csv"),
    ("COPY TO JSONL (Apex-only export)", "bench_copy_to_json"),
]

OLTP_DURABLE_WRITE_BENCHMARKS = [
    ("Insert 1 row (durable fair)", "bench_oltp_insert_one_durable"),
    ("UPDATE by ID (durable fair)", "bench_oltp_update_by_id"),
]

TXN_FAIR_BENCHMARKS = [
    ("TXN empty (BEGIN+COMMIT; durable sync)", "bench_txn_empty_commit"),
    ("TXN read COUNT(*) (COMMIT; durable sync)", "bench_txn_read_count_commit"),
    (
        "TXN backlog string miss (COMMIT; 1500 preseed; durable sync)",
        "bench_txn_backlog_string_miss_commit",
        "setup_txn_backlog_1500",
    ),
    (
        "TXN backlog COUNT(*) (COMMIT; 1500 preseed; durable sync)",
        "bench_txn_backlog_count_commit",
        "setup_txn_backlog_1500",
    ),
    (
        "TXN backlog INSERT+read-own-name (COMMIT; 1500 preseed; durable sync)",
        "bench_txn_backlog_insert_read_own_by_name_commit",
        "setup_txn_backlog_1500",
    ),
]

TXN_APEX_DIAGNOSTIC_BENCHMARKS = [
    ("TXN empty (BEGIN+ROLLBACK)", "bench_txn_empty_rollback"),
    ("TXN INSERT 1 (COMMIT; .delta diagnostic)", "bench_txn_insert_one_commit"),
    ("TXN INSERT 10 stmts (COMMIT; .delta diagnostic)", "bench_txn_insert_10_commit"),
    ("TXN multi-row INSERT 10 (COMMIT; .delta diagnostic)", "bench_txn_multi_insert_10_commit"),
    ("TXN INSERT 100 stmts (COMMIT; .delta diagnostic)", "bench_txn_insert_100_commit"),
    ("TXN multi-row INSERT 100 (COMMIT; .delta diagnostic)", "bench_txn_multi_insert_100_commit"),
    ("TXN INSERT 10 (ROLLBACK; diagnostic)", "bench_txn_insert_10_rollback"),
    ("TXN UPDATE by ID (COMMIT; .delta diagnostic)", "bench_txn_update_by_id_commit"),
    ("TXN INSERT+read-own-row (COMMIT; .delta diagnostic)", "bench_txn_insert_read_own_commit"),
]


def result_shape(value):
    """Return a compact shape string for materialized benchmark results."""
    if value is None:
        return "0x0"
    if hasattr(value, "num_rows") and hasattr(value, "num_columns"):
        return f"{value.num_rows}x{value.num_columns}"
    if hasattr(value, "shape"):
        shape = tuple(value.shape)
        if len(shape) >= 2:
            return f"{shape[0]}x{shape[1]}"
        if len(shape) == 1:
            return f"{shape[0]}x1"
    if isinstance(value, list):
        return f"{len(value)}x{len(value[0]) if value else 0}"
    return type(value).__name__


def run_bench_with_result(fn, warmup=2, iterations=5):
    """Run fn() and return (average_ms, last_result)."""
    last = None
    for _ in range(warmup):
        last = fn()
    times = []
    for _ in range(iterations):
        with timer() as t:
            last = fn()
        times.append(t["elapsed_ms"])
    return sum(times) / len(times), last


def materialization_speedup_label(dict_ms, arrow_ms):
    if dict_ms is None or arrow_ms is None or arrow_ms <= 0:
        return "N/A"
    ratio = dict_ms / arrow_ms
    if ratio >= 1:
        return f"{ratio:.1f}x faster"
    return f"{1 / ratio:.1f}x slower"


def apex_ratio_label(apex_ms, others):
    if apex_ms is None or not others:
        return None
    best_other = min(others)
    ratio = apex_ms / best_other if best_other > 0 else float("inf")
    if abs(ratio - 1.0) < 1e-9:
        return "~1.0x (tied)"
    if ratio < 1:
        return f"{ratio:.2f}x (faster)"
    return f"{ratio:.1f}x (slower)"


def summarize_apex_section(benchmark_specs, results):
    wins = 0
    ties = 0
    total = 0
    for bench_name, _, _, _, _, _ in benchmark_specs:
        vals = results.get(bench_name, {})
        apex_ms = vals.get("ApexBase")
        if apex_ms is None:
            continue
        others = [v for k, v in vals.items() if k != "ApexBase" and v is not None]
        if not others:
            continue
        total += 1
        best_other = min(others)
        ratio = apex_ms / best_other if best_other > 0 else float("inf")
        if abs(ratio - 1.0) < 1e-9:
            ties += 1
        elif ratio < 1:
            wins += 1
    return {
        "wins": wins,
        "ties": ties,
        "slower": total - wins - ties,
        "total": total,
    }


def geometric_mean_ms(values):
    positives = [v for v in values if v is not None and v > 0]
    if not positives:
        return None
    return math.exp(sum(math.log(v) for v in positives) / len(positives))


def summarize_workload_group(group_name, metric_names, results, eng_names):
    rows = []
    for metric_name in metric_names:
        metric_values = results.get(metric_name, {})
        apex_ms = metric_values.get("ApexBase")
        other_values = [
            metric_values.get(eng_name)
            for eng_name in eng_names
            if eng_name != "ApexBase"
        ]
        if apex_ms is not None and any(value is not None for value in other_values):
            rows.append((metric_name, {
                eng_name: metric_values.get(eng_name)
                for eng_name in eng_names
            }))

    totals = {}
    geomeans = {}
    for eng_name in eng_names:
        values = [values.get(eng_name) for _, values in rows]
        available = [value for value in values if value is not None]
        totals[eng_name] = sum(available) if available and len(available) == len(rows) else None
        geomeans[eng_name] = geometric_mean_ms(values)

    apex_total = totals.get("ApexBase")
    other_totals = [v for k, v in totals.items() if k != "ApexBase" and v is not None]
    ratio_label = apex_ratio_label(apex_total, other_totals) if apex_total is not None else None

    wins = ties = 0
    for _, values in rows:
        apex_ms = values.get("ApexBase")
        others = [v for k, v in values.items() if k != "ApexBase" and v is not None]
        if apex_ms is None or not others:
            continue
        best_other = min(others)
        ratio = apex_ms / best_other if best_other > 0 else float("inf")
        if abs(ratio - 1.0) < 1e-9:
            ties += 1
        elif ratio < 1:
            wins += 1

    return {
        "workload": group_name,
        "metric_count": len(rows),
        "metrics": [metric_name for metric_name, _ in rows],
        "total_ms": totals,
        "geomean_ms": geomeans,
        "apex_vs_best_total": ratio_label,
        "apex_wins": wins,
        "apex_ties": ties,
        "apex_slower": len(rows) - wins - ties,
    }


def build_fair_workload_scoreboard(results, eng_names, selected_benchmarks):
    selected_names = {spec[0] for spec in selected_benchmarks}
    grouped_names = set()
    scoreboard = []
    for group_name, metric_names in FAIR_WORKLOAD_GROUPS:
        names = [name for name in metric_names if name in selected_names]
        if not names:
            continue
        grouped_names.update(names)
        summary = summarize_workload_group(group_name, names, results, eng_names)
        if summary["metric_count"] > 0:
            scoreboard.append(summary)

    uncategorized = [
        spec[0] for spec in selected_benchmarks
        if spec[0] not in grouped_names
    ]
    if uncategorized:
        summary = summarize_workload_group("Other", uncategorized, results, eng_names)
        if summary["metric_count"] > 0:
            scoreboard.append(summary)
    return scoreboard


def print_fair_workload_scoreboard(results, eng_names, selected_benchmarks, col_width):
    scoreboard = build_fair_workload_scoreboard(results, eng_names, selected_benchmarks)
    if not scoreboard:
        return []

    print("\n--- Fair Workload Scoreboard (Aggregated) ---")
    print("  Lower total is better. A row can compare ApexBase with any available peer; missing peers show N/A.")
    name_width = max(24, *(len(row["workload"]) for row in scoreboard))
    header = f"{'Workload':<{name_width}} | {'Metrics':>7}"
    for eng_name in eng_names:
        header += f" | {eng_name + ' total':>{col_width}}"
    if "ApexBase" in eng_names and len(eng_names) >= 2:
        header += f" | {'Apex/Best':>{col_width}} | {'Apex wins':>{10}}"
    print(header)
    print("-" * len(header))

    json_rows = []
    for summary in scoreboard:
        row = f"{summary['workload']:<{name_width}} | {summary['metric_count']:>7}"
        for eng_name in eng_names:
            total_ms = summary["total_ms"].get(eng_name)
            row += f" | {fmt_ms(total_ms) if total_ms is not None else 'N/A':>{col_width}}"
        if "ApexBase" in eng_names and len(eng_names) >= 2:
            ratio = summary.get("apex_vs_best_total") or "N/A"
            row += f" | {ratio:>{col_width}} | {summary['apex_wins']:>3}/{summary['metric_count']:<6}"
        print(row)
        json_rows.append({
            "workload": summary["workload"],
            "metric_count": summary["metric_count"],
            "metrics": summary["metrics"],
            "total_ms": {
                k: round(v, 3) if v is not None else None
                for k, v in summary["total_ms"].items()
            },
            "geomean_ms": {
                k: round(v, 6) if v is not None else None
                for k, v in summary["geomean_ms"].items()
            },
            "apex_vs_best_total": summary["apex_vs_best_total"],
            "apex_wins": summary["apex_wins"],
            "apex_ties": summary["apex_ties"],
            "apex_slower": summary["apex_slower"],
        })
    return json_rows


def print_benchmark_section(title, description, benchmark_specs, results, eng_names, col_width):
    print(f"\n--- {title} ---")
    print(f"  {description}")
    name_width = max(42, *(len(spec[0]) for spec in benchmark_specs))
    header = f"{'Metric':<{name_width}}"
    for name in eng_names:
        header += f" | {name:>{col_width}}"
    if len(eng_names) >= 2:
        header += f" | {'Ratio (Apex/Best)':>{col_width}}"
    print(header)
    print("-" * len(header))

    json_rows = []
    for bench_name, method_name, is_insert, is_cold, is_warm_nogc, setup_method in benchmark_specs:
        row = f"{bench_name:<{name_width}}"
        values = {}
        for eng_name in eng_names:
            ms = results.get(bench_name, {}).get(eng_name)
            if ms is not None:
                row += f" | {fmt_ms(ms):>{col_width}}"
                values[eng_name] = ms
            else:
                row += f" | {'N/A':>{col_width}}"

        if len(eng_names) >= 2 and "ApexBase" in values:
            others = [v for k, v in values.items() if k != "ApexBase"]
            label = apex_ratio_label(values["ApexBase"], others)
            if label:
                row += f" | {label:>{col_width}}"

        print(row)
        json_rows.append({
            "category": title,
            "query": bench_name,
            # Preserve microsecond-scale OLTP timings for regression checks.
            **{k: round(v, 6) for k, v in values.items()},
        })

    if "ApexBase" in eng_names:
        stats = summarize_apex_section(benchmark_specs, results)
        print(
            f"Section Summary: ApexBase wins {stats['wins']}/{stats['total']}, "
            f"ties {stats['ties']}/{stats['total']}, slower {stats['slower']}/{stats['total']}"
        )

    return json_rows


def module_metric_counts(profile: str = PROFILE_PUBLIC):
    olap_sections, oltp_sections = benchmark_sections_for_profile(profile)
    olap_count = sum(len(specs) for _, _, specs in olap_sections)
    oltp_count = sum(len(specs) for _, _, specs in oltp_sections)
    if profile_runs_extended_sections(profile):
        olap_count += len(apex_materialization_queries({"point_lookup_id": 1})) + 2
        oltp_count += (
            len(OLTP_APEX_DIAGNOSTIC_BENCHMARKS)
            + len(OLTP_DURABLE_WRITE_BENCHMARKS)
            + len(TXN_FAIR_BENCHMARKS)
            + len(TXN_APEX_DIAGNOSTIC_BENCHMARKS)
            + 2
        )
    vector_count = vector_metric_count(profile)
    return olap_count, oltp_count, vector_count


def print_module_header(title, metric_count):
    print("\n" + "=" * 80)
    print(f" {title} Module ({metric_count} named metrics)")
    print("=" * 80)


def reload_loaded_state(engines):
    """Recreate each engine on a fresh loaded table before a new microbench section."""
    ensure_optional_imports()
    for _, bench in engines:
        bench.setup()
        bench.bench_insert()
        if HAS_APEXBASE and isinstance(bench, ApexBaseBench):
            try:
                bench.client.flush()
            except Exception:
                pass


def wrap_durable_bench_fn(eng_name, bench, fn):
    """Apply comparable post-write persistence for commit-timed microbenchmarks."""
    if eng_name == "ApexBase":
        def durable_fn(fn=fn, bench=bench):
            result = fn()
            bench.client.flush()
            return result
        return durable_fn
    if eng_name == "SQLite":
        def durable_fn(fn=fn, bench=bench):
            result = fn()
            bench.conn.execute("PRAGMA wal_checkpoint(FULL)")
            return result
        return durable_fn
    if eng_name == "DuckDB":
        def durable_fn(fn=fn, bench=bench):
            result = fn()
            bench.conn.execute("CHECKPOINT")
            return result
        return durable_fn
    return fn


def apex_materialization_queries(shared_inputs):
    point_id = shared_inputs.get("point_lookup_id", 1)
    return [
        ("Point Lookup", f"SELECT * FROM default WHERE _id = {point_id}"),
        ("SELECT * LIMIT 10K", "SELECT * FROM default LIMIT 10000"),
        ("IN filter (city IN 3)", "SELECT * FROM default WHERE city IN ('Beijing', 'Shanghai', 'Guangzhou')"),
        ("Multi-cond filter", "SELECT * FROM default WHERE age > 30 AND score > 50.0"),
        ("GROUP BY city", "SELECT city, COUNT(*), AVG(score) FROM default GROUP BY city"),
        ("Full scan", "SELECT * FROM default"),
    ]


def run_apex_materialization_benchmarks(tmpdir, data, shared_inputs, warmup, iterations, low_memory=False):
    """Compare ApexBase result APIs without mixing them into cross-engine rankings."""
    ensure_optional_imports()
    if not HAS_APEXBASE:
        return []

    methods = [
        ("to_dict", lambda rv: rv.to_dict()),
        ("to_arrow", lambda rv: rv.to_arrow() if HAS_PYARROW else None),
        ("to_pandas", lambda rv: rv.to_pandas() if HAS_PANDAS else None),
    ]
    rows = []
    mat_tmpdir = tempfile.mkdtemp(prefix="apexbase_materialize_", dir=tmpdir)
    apex_bench = ApexBaseBench(mat_tmpdir, data, low_memory=low_memory)

    print("\n--- OLAP ApexBase Materialization APIs (Apex-only; not ranked) ---")
    try:
        apex_bench.setup()
        apex_bench.shared_inputs = shared_inputs
        apex_bench.bench_insert()

        print("  Uses a fresh ApexBase copy of the same generated data; previous DML benchmarks do not affect row counts.")
        print("  This isolates Python result conversion cost and is not compared against SQLite/DuckDB rows.")
        header = (
            f"  {'Metric':<24} | {'to_dict':>12} | {'to_arrow':>12} | "
            f"{'to_pandas':>12} | {'Arrow vs dict':>14} | {'Shape':>12}"
        )
        print(header)
        print("  " + "-" * (len(header) - 2))

        for label, sql in apex_materialization_queries(shared_inputs):
            method_ms = {}
            method_shapes = {}
            for method_name, materialize in methods:
                if method_name == "to_arrow" and not HAS_PYARROW:
                    method_ms[method_name] = None
                    continue
                if method_name == "to_pandas" and not HAS_PANDAS:
                    method_ms[method_name] = None
                    continue

                def fn(sql=sql, materialize=materialize):
                    rv = apex_bench.client.execute(sql, show_internal_id=True)
                    return materialize(rv)

                ms, last = run_bench_with_result(fn, warmup=warmup, iterations=iterations)
                method_ms[method_name] = ms
                method_shapes[method_name] = result_shape(last)

            shape = (
                method_shapes.get("to_arrow")
                or method_shapes.get("to_pandas")
                or method_shapes.get("to_dict")
                or "N/A"
            )
            speedup = materialization_speedup_label(method_ms.get("to_dict"), method_ms.get("to_arrow"))
            row = (
                f"  {label:<24} | "
                f"{fmt_ms(method_ms['to_dict']) if method_ms.get('to_dict') is not None else 'N/A':>12} | "
                f"{fmt_ms(method_ms['to_arrow']) if method_ms.get('to_arrow') is not None else 'N/A':>12} | "
                f"{fmt_ms(method_ms['to_pandas']) if method_ms.get('to_pandas') is not None else 'N/A':>12} | "
                f"{speedup:>14} | {shape:>12}"
            )
            print(row)
            rows.append({
                "query": label,
                "sql": sql,
                "shape": shape,
                "to_dict_ms": round(method_ms["to_dict"], 3) if method_ms.get("to_dict") is not None else None,
                "to_arrow_ms": round(method_ms["to_arrow"], 3) if method_ms.get("to_arrow") is not None else None,
                "to_pandas_ms": round(method_ms["to_pandas"], 3) if method_ms.get("to_pandas") is not None else None,
                "arrow_vs_dict": speedup,
            })
    finally:
        try:
            apex_bench.close()
        except Exception:
            pass
        try:
            shutil.rmtree(mat_tmpdir)
        except Exception:
            pass

    return rows


def run_oltp_benchmarks(engines, warmup, iterations):
    """Run short OLTP-style microbenchmarks on a fresh loaded copy of each engine."""
    ensure_optional_imports()
    if not engines:
        return []

    reload_loaded_state(engines)

    print("\n--- OLTP Default Fair Microbenchmarks ---")
    print("  Rehydrates a fresh loaded table for this section; metric labels include native/default visibility and setup options.")
    print("  Reports median hot-path latency to reduce scheduler noise in microsecond-scale timings.")

    for _, bench in engines:
        if isinstance(bench, ApexBaseBench) and getattr(bench, "_fts_ready", False):
            try:
                bench.client.disable_fts("default")
            except Exception:
                pass

    eng_names = [name for name, _ in engines]
    col_width = 16
    name_width = max(34, *(len(name) for name, _ in OLTP_DEFAULT_BENCHMARKS))
    header = f"  {'Metric':<{name_width}}"
    for name in eng_names:
        header += f" | {name:>{col_width}}"
    if len(eng_names) >= 2:
        header += f" | {'Ratio (Apex/Best)':>{col_width}}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    rows = []
    for bench_name, method_name in OLTP_DEFAULT_BENCHMARKS:
        values = {}
        row = f"  {bench_name:<{name_width}}"
        for eng_name, bench in engines:
            fn = getattr(bench, method_name, None)
            if fn is None:
                row += f" | {'N/A':>{col_width}}"
                continue
            try:
                ms = run_bench_nogc_median(fn, warmup=warmup, iterations=iterations)
                values[eng_name] = ms
                row += f" | {fmt_ms(ms):>{col_width}}"
            except Exception:
                row += f" | {'N/A':>{col_width}}"

        if len(eng_names) >= 2 and "ApexBase" in values:
            others = {k: v for k, v in values.items() if k != "ApexBase"}
            if others:
                label = apex_ratio_label(values["ApexBase"], others.values())
                row += f" | {label:>{col_width}}"
        print(row)
        rows.append({
            "operation": bench_name,
            **{k: round(v, 3) for k, v in values.items()},
        })
    return rows


def run_oltp_durable_benchmarks(engines, warmup, iterations):
    """Run OLTP write microbenchmarks with explicit durable persistence per operation."""
    ensure_optional_imports()
    if not engines:
        return []

    reload_loaded_state(engines)

    print("\n--- OLTP Durable Fair Microbenchmarks ---")
    print("  Rehydrates a fresh loaded table, then forces comparable persistence per timed write.")
    print("  Reports median hot-path latency to reduce scheduler noise in microsecond-scale timings.")

    # Keep SQLite in durable mode only for this section.
    for eng_name, bench in engines:
        if eng_name == "SQLite":
            try:
                bench.conn.execute("PRAGMA synchronous=FULL")
            except Exception:
                pass

    eng_names = [name for name, _ in engines]
    col_width = 16
    name_width = max(34, *(len(name) for name, _ in OLTP_DURABLE_WRITE_BENCHMARKS))
    header = f"  {'Metric':<{name_width}}"
    for name in eng_names:
        header += f" | {name:>{col_width}}"
    if len(eng_names) >= 2:
        header += f" | {'Ratio (Apex/Best)':>{col_width}}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    rows = []
    try:
        for bench_name, method_name in OLTP_DURABLE_WRITE_BENCHMARKS:
            values = {}
            row = f"  {bench_name:<{name_width}}"
            for eng_name, bench in engines:
                fn = getattr(bench, method_name, None)
                if fn is None and method_name == "bench_oltp_insert_one_durable":
                    fn = getattr(bench, "bench_oltp_insert_one", None)
                if fn is None:
                    row += f" | {'N/A':>{col_width}}"
                    continue

                try:
                    if eng_name == "ApexBase" and method_name == "bench_oltp_insert_one_durable":
                        durable_fn = fn
                    else:
                        durable_fn = wrap_durable_bench_fn(eng_name, bench, fn)
                    ms = run_bench_nogc_median(durable_fn, warmup=warmup, iterations=iterations)
                    values[eng_name] = ms
                    row += f" | {fmt_ms(ms):>{col_width}}"
                except Exception:
                    row += f" | {'N/A':>{col_width}}"

            if len(eng_names) >= 2 and "ApexBase" in values:
                others = {k: v for k, v in values.items() if k != "ApexBase"}
                if others:
                    label = apex_ratio_label(values["ApexBase"], others.values())
                    row += f" | {label:>{col_width}}"
            print(row)
            rows.append({
                "operation": bench_name,
                **{k: round(v, 3) for k, v in values.items()},
            })
    finally:
        for eng_name, bench in engines:
            if eng_name == "SQLite":
                try:
                    bench.conn.execute("PRAGMA synchronous=OFF")
                except Exception:
                    pass

    return rows


def run_txn_benchmarks(engines, warmup, iterations):
    """Run fair cross-engine transaction microbenchmarks with durable sync on COMMIT."""
    ensure_optional_imports()
    if not engines:
        return []

    reload_loaded_state(engines)

    print("\n--- OLTP Transaction Fair Microbenchmarks ---")
    print("  Rehydrates a fresh loaded table, then applies comparable durable sync after each committed transaction.")
    print("  Reports median hot-path latency to reduce scheduler noise in microsecond-scale timings.")

    for eng_name, bench in engines:
        if eng_name == "SQLite":
            try:
                bench.conn.execute("PRAGMA synchronous=FULL")
            except Exception:
                pass

    eng_names = [name for name, _ in engines]
    col_width = 16
    name_width = max(38, *(len(spec[0]) for spec in TXN_FAIR_BENCHMARKS))
    header = f"  {'Metric':<{name_width}}"
    for name in eng_names:
        header += f" | {name:>{col_width}}"
    if len(eng_names) >= 2:
        header += f" | {'Ratio (Apex/Best)':>{col_width}}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    rows = []
    try:
        for spec in TXN_FAIR_BENCHMARKS:
            bench_name, method_name = spec[:2]
            setup_method = spec[2] if len(spec) > 2 else None
            values = {}
            row = f"  {bench_name:<{name_width}}"
            for eng_name, bench in engines:
                if setup_method is not None:
                    setup_fn = getattr(bench, setup_method, None)
                    if setup_fn is not None:
                        setup_fn()
                fn = getattr(bench, method_name, None)
                if fn is None:
                    row += f" | {'N/A':>{col_width}}"
                    continue
                try:
                    ms = run_bench_nogc_median(
                        wrap_durable_bench_fn(eng_name, bench, fn),
                        warmup=warmup,
                        iterations=iterations,
                    )
                    values[eng_name] = ms
                    row += f" | {fmt_ms(ms):>{col_width}}"
                except Exception:
                    row += f" | {'N/A':>{col_width}}"

            if len(eng_names) >= 2 and "ApexBase" in values:
                others = {k: v for k, v in values.items() if k != "ApexBase"}
                if others:
                    label = apex_ratio_label(values["ApexBase"], others.values())
                    row += f" | {label:>{col_width}}"
            print(row)
            rows.append({
                "operation": bench_name,
                **{k: round(v, 3) for k, v in values.items()},
            })
    finally:
        for eng_name, bench in engines:
            if eng_name == "SQLite":
                try:
                    bench.conn.execute("PRAGMA synchronous=OFF")
                except Exception:
                    pass
    return rows


def run_apex_oltp_diagnostics(tmpdir, data, warmup, iterations):
    """Show Apex-only tiny-write diagnostics outside cross-engine fair rankings."""
    ensure_optional_imports()
    if not HAS_APEXBASE or not OLTP_APEX_DIAGNOSTIC_BENCHMARKS:
        return []

    print("\n--- OLTP ApexBase Small-Batch Diagnostics (Apex-only; not ranked) ---")
    print("  Uses a fresh loaded ApexBase copy; tiny Python API batching effects are tracked separately from fair cross-engine rankings.")

    col_width = 16
    name_width = max(42, *(len(name) for name, _ in OLTP_APEX_DIAGNOSTIC_BENCHMARKS))
    header = f"  {'Metric':<{name_width}} | {'ApexBase':>{col_width}}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    rows = []
    for bench_name, method_name in OLTP_APEX_DIAGNOSTIC_BENCHMARKS:
        diag_tmpdir = tempfile.mkdtemp(prefix="apexbase_oltp_diag_", dir=tmpdir)
        bench = ApexBaseBench(diag_tmpdir, data)
        try:
            bench.setup()
            bench.bench_insert()
            fn = getattr(bench, method_name)
            ms = run_bench_nogc_median(fn, warmup=warmup, iterations=iterations)
            print(f"  {bench_name:<{name_width}} | {fmt_ms(ms):>{col_width}}")
            rows.append({
                "operation": bench_name,
                "ApexBase": round(ms, 3),
            })
        finally:
            try:
                bench.close()
            except Exception:
                pass
            try:
                shutil.rmtree(diag_tmpdir)
            except Exception:
                pass

    return rows


def run_apex_txn_diagnostics(tmpdir, data, warmup, iterations):
    """Show Apex-only .delta transaction-path diagnostics outside fair rankings."""
    ensure_optional_imports()
    if not HAS_APEXBASE or not TXN_APEX_DIAGNOSTIC_BENCHMARKS:
        return []

    print("\n--- OLTP ApexBase Transaction Diagnostics (Apex-only; not ranked) ---")
    print("  Preserves ApexBase .delta commit semantics and reports transaction-control diagnostics without cross-engine ranking.")

    col_width = 16
    name_width = max(52, *(len(spec[0]) for spec in TXN_APEX_DIAGNOSTIC_BENCHMARKS))
    header = f"  {'Metric':<{name_width}} | {'ApexBase':>{col_width}}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    rows = []
    diag_tmpdir = tempfile.mkdtemp(prefix="apexbase_txn_diag_", dir=tmpdir)
    bench = ApexBaseBench(diag_tmpdir, data)
    try:
        for spec in TXN_APEX_DIAGNOSTIC_BENCHMARKS:
            bench_name, method_name = spec[:2]
            setup_method = spec[2] if len(spec) > 2 else None
            bench.setup()
            bench.bench_insert()
            if setup_method is not None:
                getattr(bench, setup_method)()
            fn = getattr(bench, method_name)
            ms = run_bench_nogc_median(fn, warmup=warmup, iterations=iterations)
            print(f"  {bench_name:<{name_width}} | {fmt_ms(ms):>{col_width}}")
            rows.append({
                "operation": bench_name,
                "ApexBase": round(ms, 3),
            })
    finally:
        try:
            bench.close()
        except Exception:
            pass
        try:
            shutil.rmtree(diag_tmpdir)
        except Exception:
            pass

    return rows


def run_apex_buffered_oltp_benchmarks(tmpdir, oltp_results, warmup, iterations):
    """Show ApexBase's explicit client-local buffered write mode.

    This is intentionally separate from the default cross-engine OLTP table:
    buffered rows are accumulated in the ApexClient process and become visible
    after flush/end/close, so the durability/visibility contract is different
    from per-call SQLite INSERT+commit.
    """
    ensure_optional_imports()
    if not HAS_APEXBASE or not hasattr(ApexClient, "begin_buffered_writes"):
        return []

    baselines = {}
    for row in oltp_results or []:
        if row.get("operation") == "Insert 1 row (default fair)":
            baselines = row
            break

    print("\n--- OLTP ApexBase Buffered Writes (Apex-only; not ranked) ---")
    print("  Opt-in client-local write buffer; visibility/durability option is included in the metric label.")

    col_width = 16
    header = f"  {'Metric':<42} | {'ApexBase Buffered':>{col_width}}"
    for name in ("SQLite", "DuckDB"):
        if name in baselines:
            header += f" | {name:>{col_width}}"
    if baselines:
        header += f" | {'Ratio (Apex/Best)':>{col_width}}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    rows = []
    buf_tmpdir = tempfile.mkdtemp(prefix="apexbase_buffered_oltp_", dir=tmpdir)
    client = ApexClient(buf_tmpdir, drop_if_exists=True)
    try:
        client.create_table("default")
        client.store({
            "name": ["seed"],
            "age": [1],
            "score": [1.0],
            "city": ["Beijing"],
            "category": ["Books"],
        })
        client.begin_buffered_writes()

        def buffered_insert_one():
            client.store({
                "name": "oltp_buffered_one",
                "age": 31,
                "score": 77.0,
                "city": "Beijing",
                "category": "Books",
            })

        ms = run_bench_nogc_median(buffered_insert_one, warmup=warmup, iterations=iterations)
        flushed = client.flush_buffered_writes()

        row_text = f"  {'Buffered Insert 1 row (flush after timing)':<42} | {fmt_ms(ms):>{col_width}}"
        others = []
        for name in ("SQLite", "DuckDB"):
            other_ms = baselines.get(name)
            if other_ms is not None:
                row_text += f" | {fmt_ms(other_ms):>{col_width}}"
                others.append(other_ms)
        if others:
            label = apex_ratio_label(ms, others)
            row_text += f" | {label:>{col_width}}"
        print(row_text)

        rows.append({
            "operation": "Buffered Insert 1 row (flush after timing)",
            "ApexBase Buffered": round(ms, 6),
            "flushed_rows_after_timing": flushed,
        })
    finally:
        try:
            client.end_buffered_writes(flush=True)
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass
        try:
            shutil.rmtree(buf_tmpdir)
        except Exception:
            pass

    return rows


def run_apex_memtable_oltp_benchmarks(tmpdir, oltp_results, warmup, iterations):
    """Show ApexBase's default fast storage-level memtable write path in isolation.

    This path keeps writes inside the storage engine and makes them immediately
    readable by the same storage instance, then persists them on flush/close or
    auto-flush. Separate processes see the rows only after persistence, so this
    stays outside committed-write OLTP rankings unless the benchmark flushes
    each timed write.
    """
    ensure_optional_imports()
    if not HAS_APEXBASE:
        return []

    baselines = {}
    for row in oltp_results or []:
        if row.get("operation") == "Insert 1 row (default fair)":
            baselines = row
            break

    print("\n--- OLTP ApexBase Memtable Writes (Apex-only; not ranked) ---")
    print("  Same-storage reads see rows immediately; cross-process visibility follows flush/close/auto-flush.")

    col_width = 16
    header = f"  {'Metric':<42} | {'ApexBase Memtable':>{col_width}}"
    for name in ("SQLite", "DuckDB"):
        if name in baselines:
            header += f" | {name:>{col_width}}"
    if baselines:
        header += f" | {'Ratio (Apex/Best)':>{col_width}}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    rows = []
    mem_tmpdir = tempfile.mkdtemp(prefix="apexbase_memtable_oltp_", dir=tmpdir)
    old_disable_env = os.environ.get("APEXBASE_DISABLE_MEMTABLE_SINGLE_WRITE")
    os.environ.pop("APEXBASE_DISABLE_MEMTABLE_SINGLE_WRITE", None)
    client = ApexClient(mem_tmpdir, drop_if_exists=True)
    try:
        client.create_table("default")
        client.store({
            "name": ["seed"],
            "age": [1],
            "score": [1.0],
            "city": ["Beijing"],
            "category": ["Books"],
        })

        def memtable_insert_one():
            client.store({
                "name": "oltp_memtable_one",
                "age": 31,
                "score": 77.0,
                "city": "Beijing",
                "category": "Books",
            })

        ms = run_bench_nogc_median(memtable_insert_one, warmup=warmup, iterations=iterations)
        client.flush()

        row_text = f"  {'Memtable Insert 1 row (same-client visible)':<42} | {fmt_ms(ms):>{col_width}}"
        others = []
        for name in ("SQLite", "DuckDB"):
            other_ms = baselines.get(name)
            if other_ms is not None:
                row_text += f" | {fmt_ms(other_ms):>{col_width}}"
                others.append(other_ms)
        if others:
            label = apex_ratio_label(ms, others)
            row_text += f" | {label:>{col_width}}"
        print(row_text)

        rows.append({
            "operation": "Memtable Insert 1 row (same-client visible)",
            "ApexBase Memtable": round(ms, 6),
        })
    finally:
        try:
            client.close()
        except Exception:
            pass
        if old_disable_env is None:
            os.environ.pop("APEXBASE_DISABLE_MEMTABLE_SINGLE_WRITE", None)
        else:
            os.environ["APEXBASE_DISABLE_MEMTABLE_SINGLE_WRITE"] = old_disable_env
        try:
            shutil.rmtree(mem_tmpdir)
        except Exception:
            pass

    return rows


def run_vector_similarity_benchmarks(tmpdir, rows, dim, k, warmup, iterations, profile=PROFILE_PUBLIC):
    """Benchmark vector similarity against DuckDB on a separate vector dataset."""
    ensure_optional_imports()
    head_metrics, batch_metrics, apex_only_metrics = vector_metric_sets(profile)
    config = {
        "rows": rows,
        "dim": dim,
        "k": k,
        "batch_queries": VECTOR_BATCH_QUERY_COUNT,
        "sqlite_note": VECTOR_SQLITE_NOTE,
        "profile": normalize_profile(profile),
    }

    reason = None
    if not HAS_APEXBASE:
        reason = "ApexBase is unavailable in this environment."
    elif not HAS_DUCKDB:
        reason = "DuckDB is unavailable in this environment."
    elif not HAS_PANDAS:
        reason = "pandas is required to bulk-load the DuckDB vector table."
    elif rows <= 0 or dim <= 0 or k <= 0:
        reason = "rows, dim, and k must all be positive."

    print_module_header("Vector Similarity", vector_metric_count(profile))
    print(f"  Dedicated vector dataset: {rows:,} rows x {dim} dims; TopK k={k}; batch queries={VECTOR_BATCH_QUERY_COUNT}.")
    print(f"  {VECTOR_SQLITE_NOTE}")

    if reason is not None:
        print(f"  Skipped: {reason}")
        return {
            "config": config,
            "skipped": True,
            "skip_reason": reason,
            "head_to_head": [],
            "batch": [],
            "apex_only": [],
            "summary": {"wins": 0, "ties": 0, "slower": 0, "total": 0},
        }

    print("Generating vector dataset...", end=" ", flush=True)
    vecs, query, batch_queries = generate_vector_data(rows, dim)
    print("done.")

    vector_tmpdir = tempfile.mkdtemp(prefix="apexbase_vector_", dir=tmpdir)
    apex_client = None
    duck_con = None
    try:
        print("Setting up vector engines...", end=" ", flush=True)
        apex_client = setup_apex_vector_bench(vector_tmpdir, vecs)
        duck_con = setup_duckdb_vector_bench(vecs)
        print("done.")

        head_results = {}
        head_rows = []
        for label, metric in head_metrics:
            head_results[label] = {
                "ApexBase": run_bench_gc_median(
                    lambda client=apex_client, q=query, metric=metric: bench_apex_vector_query(client, q, k, metric),
                    warmup=warmup,
                    iterations=iterations,
                ),
                "DuckDB": run_bench_gc_median(
                    lambda con=duck_con, q=query, metric=metric: bench_duckdb_vector_query(con, q, k, metric),
                    warmup=warmup,
                    iterations=iterations,
                ),
            }

        if head_metrics:
            head_rows = print_benchmark_section(
                "Vector Head-to-Head (single query)",
                "Single-query TopK similarity on the same vector table; results materialized via Arrow when available.",
                display_only_specs([label for label, _ in head_metrics]),
                head_results,
                ["ApexBase", "DuckDB"],
                16,
            )

        batch_results = {}
        batch_rows = []
        for label, metric in batch_metrics:
            batch_results[label] = {
                "ApexBase": run_bench_gc_median(
                    lambda client=apex_client, queries=batch_queries, metric=metric: bench_apex_batch_vector_query(client, queries, k, metric),
                    warmup=warmup,
                    iterations=iterations,
                ),
                "DuckDB": run_bench_gc_median(
                    lambda con=duck_con, queries=batch_queries, metric=metric: bench_duckdb_batch_vector_query(con, queries, k, metric),
                    warmup=warmup,
                    iterations=iterations,
                ),
            }

        if batch_metrics:
            batch_rows = print_benchmark_section(
                "Vector Head-to-Head (batch queries)",
                "Ten-query TopK workload: ApexBase batch_topk_distance() vs repeated DuckDB single-query SQL on the same batch.",
                display_only_specs([label for label, _ in batch_metrics]),
                batch_results,
                ["ApexBase", "DuckDB"],
                16,
            )

        apex_only_rows = []
        if apex_only_metrics:
            print("\n--- Vector Apex-only Metrics ---")
            print("  Metrics without a native DuckDB equivalent in this harness.")
            col_width = 16
            name_width = max(32, *(len(label) for label, _ in apex_only_metrics))
            header = f"{'Metric':<{name_width}} | {'ApexBase':>{col_width}}"
            print(header)
            print("-" * len(header))

            for label, metric in apex_only_metrics:
                ms = run_bench_gc_median(
                    lambda client=apex_client, q=query, metric=metric: bench_apex_vector_query(client, q, k, metric),
                    warmup=warmup,
                    iterations=iterations,
                )
                print(f"{label:<{name_width}} | {fmt_ms(ms):>{col_width}}")
                apex_only_rows.append({
                    "category": "Vector Apex-only Metrics",
                    "query": label,
                    "ApexBase": round(ms, 3),
                })

        combined_results = {}
        combined_results.update(head_results)
        combined_results.update(batch_results)
        combined_specs = display_only_specs(list(head_results) + list(batch_results))
        summary = summarize_apex_section(combined_specs, combined_results)
        print(
            f"Vector Competitive Summary: ApexBase wins {summary['wins']}/{summary['total']}, "
            f"ties {summary['ties']}/{summary['total']}, slower {summary['slower']}/{summary['total']}"
        )

        return {
            "config": config,
            "skipped": False,
            "skip_reason": None,
            "head_to_head": head_rows,
            "batch": batch_rows,
            "apex_only": apex_only_rows,
            "summary": summary,
        }
    finally:
        if apex_client is not None:
            try:
                apex_client.close()
            except Exception:
                pass
        if duck_con is not None:
            try:
                duck_con.close()
            except Exception:
                pass
        try:
            shutil.rmtree(vector_tmpdir)
        except Exception:
            pass


def get_system_info():
    info = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python": platform.python_version(),
    }
    try:
        import psutil
        info["cpu_count"] = psutil.cpu_count(logical=True)
        info["memory_gb"] = round(psutil.virtual_memory().total / (1024**3), 1)
    except ImportError:
        info["cpu_count"] = os.cpu_count()
        info["memory_gb"] = "N/A"

    if HAS_APEXBASE:
        try:
            from apexbase._core import __version__
            info["apexbase"] = __version__
        except Exception:
            info["apexbase"] = "unknown"
    if HAS_DUCKDB:
        info["duckdb"] = duckdb.__version__
    info["sqlite"] = sqlite3.sqlite_version
    if HAS_PYARROW:
        info["pyarrow"] = pa.__version__
    return info


def _command_output(*command, allow_empty=False):
    """Return one tool-version command's output without making reports fragile."""
    try:
        result = subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unavailable"
    if result.returncode != 0:
        return "unavailable"
    output = result.stdout.strip()
    return output if output or allow_empty else "unavailable"


def _distribution_version(name):
    try:
        return importlib_metadata.version(name)
    except importlib_metadata.PackageNotFoundError:
        return "not-installed"


def get_git_info():
    commit = os.environ.get("APEXBASE_BENCHMARK_COMMIT")
    branch = os.environ.get("APEXBASE_BENCHMARK_BRANCH")
    dirty_override = os.environ.get("APEXBASE_BENCHMARK_DIRTY")

    status = ""
    if not commit or not branch or dirty_override is None:
        status = _command_output(
            "git", "status", "--porcelain=v2", "--branch", allow_empty=True
        )

    if not commit:
        prefix = "# branch.oid "
        commit = next(
            (line[len(prefix):] for line in status.splitlines() if line.startswith(prefix)),
            "unavailable",
        )
    if commit in ("unavailable", "(initial)"):
        commit = os.environ.get("GITHUB_SHA", "unknown")

    if not branch:
        prefix = "# branch.head "
        branch = next(
            (line[len(prefix):] for line in status.splitlines() if line.startswith(prefix)),
            "unavailable",
        )
        if branch == "(detached)":
            branch = "HEAD"

    dirty = (
        dirty_override == "1"
        if dirty_override is not None
        else any(line and not line.startswith("# ") for line in status.splitlines())
    )
    return {
        "commit": commit,
        "branch": branch,
        "dirty": dirty,
    }


def get_dependency_versions():
    """Record every runtime dependency relevant to benchmark reproducibility."""
    return {
        "apexbase": _distribution_version("apexbase"),
        "sqlite": sqlite3.sqlite_version,
        "duckdb": _distribution_version("duckdb"),
        "pyarrow": _distribution_version("pyarrow"),
        "numpy": np.__version__,
        "pandas": _distribution_version("pandas"),
        "polars": _distribution_version("polars"),
    }


def get_build_versions():
    with ThreadPoolExecutor(max_workers=2) as executor:
        rustc = executor.submit(_command_output, "rustc", "--version", "--verbose")
        cargo = executor.submit(_command_output, "cargo", "--version")
        return {
            "maturin": _distribution_version("maturin"),
            "rustc": rustc.result(),
            "cargo": cargo.result(),
        }


def get_report_metadata(suite):
    """Build the shared, JSON-safe provenance envelope for benchmark reports."""
    collectors = {
        "git": get_git_info,
        "system": get_system_info,
        "dependencies": get_dependency_versions,
        "build": get_build_versions,
    }
    with ThreadPoolExecutor(max_workers=len(collectors)) as executor:
        pending = {name: executor.submit(collector) for name, collector in collectors.items()}
        return {
            "format_version": 1,
            "suite": suite,
            **{name: future.result() for name, future in pending.items()},
        }


# ========================================================================
# OLAP Throughput Tests (Single & Concurrent)
# ========================================================================
from concurrent.futures import ThreadPoolExecutor
import threading

# Q/s test queries - mixed short + analytical reads on the same loaded table
QPS_QUERIES_APEX = [
    "SELECT COUNT(*) FROM default",
    "SELECT city, COUNT(*) FROM default GROUP BY city",
    "SELECT category, AVG(score) FROM default GROUP BY category",
    "SELECT * FROM default WHERE age > 30 LIMIT 100",
]

QPS_QUERIES_SQLITE = [
    "SELECT COUNT(*) FROM bench",
    "SELECT city, COUNT(*) FROM bench GROUP BY city",
    "SELECT category, AVG(score) FROM bench GROUP BY category",
    "SELECT * FROM bench WHERE age > 30 LIMIT 100",
]

QPS_QUERIES_DUCKDB = [
    "SELECT COUNT(*) FROM bench",
    "SELECT city, COUNT(*) FROM bench GROUP BY city",
    "SELECT category, AVG(score) FROM bench GROUP BY category",
    "SELECT rowid + 1 AS _id, * FROM bench WHERE age > 30 LIMIT 100",
]

def run_qps_benchmark(tmpdir, data, n_threads=4, min_duration=1.0, min_iterations=50,
                       existing_engines=None):
    """Measure OLAP/read Q/s for single-threaded and concurrent scenarios.
    
    Args:
        min_duration: Minimum test duration in seconds for accurate timing
        min_iterations: Minimum number of query batches to run
        existing_engines: dict of {name: bench} to reuse, avoids re-inserting data
    """
    results = {}
    if existing_engines is not None:
        # The main benchmark run leaves the Apex dataset table in a
        # delta-heavy state: the calibrated single-row DML microbenchmarks
        # accumulate thousands of pending writes and update tombstones,
        # which makes the read-only scan paths 100-1000x slower than a
        # steady-state table. Recreate every engine on a fresh loaded copy
        # (same mechanism as the OLTP microbenchmark sections) so the Q/s
        # profile measures steady-state read throughput.
        reload_loaded_state([
            (name, bench) for name, bench in existing_engines.items()
        ])
    print("\n--- OLAP Throughput (mixed read profile) ---")
    print("  Metric options: COUNT + two GROUP BY scans + filtered LIMIT 100; materialized Python rows.")

    # Reuse existing engines (data already inserted) to avoid re-inserting 1M rows
    qps_engines = []
    if existing_engines is not None:
        if HAS_APEXBASE and "ApexBase" in existing_engines:
            apex_bench = existing_engines["ApexBase"]
            # The Q/s harness queries `FROM default`, which resolves to the
            # currently selected table. Earlier sections (table ops) may
            # leave another table selected, so pin the dataset table before
            # pre-warming.
            if getattr(apex_bench, "client", None) is not None:
                try:
                    apex_bench.client.use_table("default")
                except Exception:
                    pass
            qps_engines.append(("ApexBase", apex_bench, QPS_QUERIES_APEX))
        if "SQLite" in existing_engines:
            qps_engines.append(("SQLite", existing_engines["SQLite"], QPS_QUERIES_SQLITE))
        if HAS_DUCKDB and "DuckDB" in existing_engines:
            qps_engines.append(("DuckDB", existing_engines["DuckDB"], QPS_QUERIES_DUCKDB))
    else:
        # Fallback: create fresh engines (slow path)
        if HAS_APEXBASE:
            qps_engines.append(("ApexBase", ApexBaseBench(tmpdir, data), QPS_QUERIES_APEX))
        qps_engines.append(("SQLite", SQLiteBench(tmpdir, data), QPS_QUERIES_SQLITE))
        if HAS_DUCKDB:
            qps_engines.append(("DuckDB", DuckDBBench(tmpdir, data), QPS_QUERIES_DUCKDB))
        for name, bench, _ in qps_engines:
            bench.setup()
            if hasattr(bench, 'bench_insert'):
                bench.bench_insert()

    # Pre-warm all engines - run each query once to ensure caching is comparable
    for name, bench, queries in qps_engines:
        try:
            for q in queries:
                _ = bench.execute_materialized_query(q)
        except Exception:
            pass

    # 1. Single-threaded Q/s
    print("\n--- OLAP Q/s (single thread) ---")
    iterations = min_iterations  # fallback default
    single_iterations = {}
    for name, bench, queries in qps_engines:
        try:
            exec_fn = lambda q, b=bench: b.execute_materialized_query(q)

            # Run sequential queries with proper timing
            gc.collect()
            
            # First, determine appropriate number of iterations based on time
            # Run a small batch first to estimate time per query
            batch_size = len(queries)
            trial_iterations = 5
            
            t0 = time.perf_counter()
            for _ in range(trial_iterations):
                for q in queries:
                    exec_fn(q)
            trial_time = time.perf_counter() - t0
            
            # Calculate iterations needed for min_duration seconds
            # At least min_iterations batches, but also enough to run for min_duration
            time_per_batch = trial_time / trial_iterations
            iterations = max(min_iterations, int(min_duration / time_per_batch))
            single_iterations[name] = iterations
            
            # Run the actual benchmark
            t0 = time.perf_counter()
            for _ in range(iterations):
                for q in queries:
                    exec_fn(q)
            elapsed = time.perf_counter() - t0

            total_queries = iterations * len(queries)
            qps = total_queries / elapsed if elapsed > 0 else 0
            results[f'{name}_single'] = qps
            print(f"  {name}: {qps:.1f} Q/s ({total_queries} queries in {elapsed:.3f}s)")
        except Exception as e:
            print(f"  {name}: Error - {e}")

    # 2. Concurrent Q/s (multiple threads) — reuse qps_engines, no re-insert
    print(f"\n--- OLAP Q/s (threads={n_threads}) ---")

    for name, bench, queries in qps_engines:
        try:
            # For each thread, we need a separate connection for SQLite/DuckDB
            # ApexBase handles concurrency internally
            if hasattr(bench, 'client'):
                # ApexBase: shared client
                # Reuse the per-engine calibration from the single-threaded test.
                conc_iterations = max(min_iterations, single_iterations.get(name, min_iterations))
                def concurrent_worker_apex(its=conc_iterations):
                    for _ in range(its):
                        for q in queries:
                            bench.execute_materialized_query(q)
                
                gc.collect()
                t0 = time.perf_counter()
                with ThreadPoolExecutor(max_workers=n_threads) as executor:
                    list(executor.map(lambda _: concurrent_worker_apex(), range(n_threads)))
                elapsed = time.perf_counter() - t0
                total_queries = n_threads * conc_iterations * len(queries)
                qps = total_queries / elapsed if elapsed > 0.001 else 0
                
            elif hasattr(bench, 'conn'):
                # SQLite/DuckDB: create connection per thread
                db_path = bench.db_path
                is_duckdb = 'duckdb' in str(type(bench.conn)).lower()

                conc_iterations = max(min_iterations, single_iterations.get(name, min_iterations))
                def concurrent_worker_sql(db_path, is_duckdb, queries, its=conc_iterations):
                    # Each thread creates its own connection
                    try:
                        if is_duckdb:
                            import duckdb
                            conn = duckdb.connect(db_path)
                        else:
                            import sqlite3
                            conn = sqlite3.connect(db_path, timeout=30)
                        for _ in range(its):
                            for q in queries:
                                cur = conn.execute(q)
                                _ = rows_to_dicts([d[0] for d in (cur.description or [])], cur.fetchall())
                        conn.close()
                    except Exception:
                        pass
                
                gc.collect()
                t0 = time.perf_counter()
                with ThreadPoolExecutor(max_workers=n_threads) as executor:
                    list(executor.map(
                        lambda _: concurrent_worker_sql(db_path, is_duckdb, queries),
                        range(n_threads)
                    ))
                elapsed = time.perf_counter() - t0
                total_queries = n_threads * conc_iterations * len(queries)
                qps = total_queries / elapsed if elapsed > 0.001 else 0
            else:
                qps = 0
                total_queries = 0
                elapsed = 0

            results[f'{name}_concurrent_{n_threads}'] = qps
            if elapsed > 0.001:
                print(f"  {name}: {qps:.1f} Q/s ({total_queries} queries in {elapsed:.3f}s)")
            else:
                print(f"  {name}: Error - test time too short ({elapsed:.6f}s)")
        except Exception as e:
            print(f"  {name}: Error - {e}")

    # Print Q/s Summary
    print("\n--- OLAP Q/s Summary ---")
    apex_single = results.get("ApexBase_single", 0)
    sqlite_single = results.get("SQLite_single", 0)
    duckdb_single = results.get("DuckDB_single", 0)
    apex_concurrent = results.get("ApexBase_concurrent_4", 0)
    sqlite_concurrent = results.get("SQLite_concurrent_4", 0)
    duckdb_concurrent = results.get("DuckDB_concurrent_4", 0)
    print(f"  ApexBase (single-threaded): {apex_single:.1f} Q/s")
    print(f"  SQLite (single-threaded): {sqlite_single:.1f} Q/s")
    print(f"  DuckDB (single-threaded): {duckdb_single:.1f} Q/s")
    print(f"  ApexBase (4-thread concurrent): {apex_concurrent:.1f} Q/s")
    print(f"  SQLite (4-thread concurrent): {sqlite_concurrent:.1f} Q/s")
    print(f"  DuckDB (4-thread concurrent): {duckdb_concurrent:.1f} Q/s")

    return results

def main(argv=None, default_profile=PROFILE_PUBLIC):
    parser = argparse.ArgumentParser(description="ApexBase vs SQLite vs DuckDB benchmark")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows (default: 1M)")
    parser.add_argument("--warmup", type=int, default=2, help="Warmup iterations (default: 2)")
    parser.add_argument("--iterations", type=int, default=5, help="Timed iterations (default: 5)")
    parser.add_argument("--output", type=str, default=None, help="JSON output file")
    parser.add_argument("--profile", choices=PROFILE_CHOICES, default=default_profile,
                        help="Benchmark profile: public README scoreboard or extended diagnostics")
    parser.add_argument("--full", action="store_true",
                        help="Alias for --profile extended")
    parser.add_argument("--memory", action="store_true", help="Track RSS memory delta per query")
    parser.add_argument("--low-memory", action="store_true",
                        help="Disable ApexBase arrow_batch_cache (simulate low-memory mode like SQLite/DuckDB)")
    parser.add_argument("--skip-vector", action="store_true", help="Skip the vector similarity benchmark module")
    parser.add_argument("--vector-rows", type=int, default=None,
                        help="Rows for vector similarity module (default: min(rows, 200000))")
    parser.add_argument("--vector-dim", type=int, default=VECTOR_DIM_DEFAULT,
                        help=f"Vector dimensions for similarity module (default: {VECTOR_DIM_DEFAULT})")
    parser.add_argument("--vector-k", type=int, default=VECTOR_K_DEFAULT,
                        help=f"TopK value for vector similarity module (default: {VECTOR_K_DEFAULT})")
    args = parser.parse_args(argv)
    profile = PROFILE_EXTENDED if args.full else normalize_profile(args.profile)
    ensure_optional_imports()

    N = args.rows
    WARMUP = args.warmup
    ITERS = args.iterations
    VECTOR_ROWS = args.vector_rows if args.vector_rows is not None else default_vector_rows(N)
    VECTOR_DIM = args.vector_dim
    VECTOR_K = args.vector_k
    selected_benchmarks = benchmark_specs_for_profile(profile)

    print("=" * 80)
    print(f" ApexBase vs SQLite vs DuckDB — Performance Benchmark")
    print("=" * 80)

    sys_info = get_system_info()
    print(f"\nSystem: {sys_info['platform']} ({sys_info['machine']})")
    print(f"CPU: {sys_info.get('processor', 'N/A')} ({sys_info['cpu_count']} cores)")
    print(f"Memory: {sys_info['memory_gb']} GB")
    print(f"Python: {sys_info['python']}")
    if "apexbase" in sys_info:
        print(f"ApexBase: v{sys_info['apexbase']}")
    print(f"SQLite: v{sys_info['sqlite']}")
    if HAS_DUCKDB:
        print(f"DuckDB: v{sys_info['duckdb']}")
    if HAS_PYARROW:
        print(f"PyArrow: v{sys_info['pyarrow']}")
    olap_metric_count, oltp_metric_count, vector_metric_total = module_metric_counts(profile)
    print(f"\nDataset: {N:,} rows × 5 columns (name, age, score, city, category)")
    print(f"Warmup: {WARMUP} iterations, Timed: {ITERS} iterations (average)")
    print(f"Profile: {profile} ({'fair workload scoreboard' if profile == PROFILE_PUBLIC else 'full diagnostics'})")
    print("Fairness mode: default rankings use normal engine APIs, shared input data, and comparable materialized results.")
    print(
        f"Layout: OLAP, OLTP, and Vector Similarity modules; "
        f"{olap_metric_count + oltp_metric_count + vector_metric_total} named metrics configured "
        f"({olap_metric_count} OLAP, {oltp_metric_count} OLTP, {vector_metric_total} vector)."
    )
    print("Tunable/fairness options are shown in metric names using parentheses.")
    if args.skip_vector:
        print("Vector module: skipped by --skip-vector")
    else:
        print(
            f"Vector dataset: {VECTOR_ROWS:,} rows x {VECTOR_DIM} dims (separate module), "
            f"TopK={VECTOR_K}, batch queries={VECTOR_BATCH_QUERY_COUNT}"
        )
        print(
            "Vector results are reported separately and do not change the "
            f"{len(selected_benchmarks)}-metric tabular fair scoreboard."
        )
    if args.low_memory:
        print("Mode: LOW-MEMORY (ApexBase-only cache stress mode; not a cross-engine apples-to-apples setting)")
    print()

    # Generate data
    print("Generating test data...", end=" ", flush=True)
    data = generate_data(N)
    print("done.")

    tmpdir = tempfile.mkdtemp(prefix="apexbase_bench_")

    csv_path = parquet_path = json_path = None
    if selected_benchmarks_need_files(selected_benchmarks) or profile_runs_extended_sections(profile):
        print("Generating benchmark files (CSV/Parquet/JSON)...", end=" ", flush=True)
        csv_path, parquet_path, json_path = generate_benchmark_files(tmpdir, data)
        print("done.")
    else:
        print("Benchmark files: skipped; no selected fair metric needs external files.")
    results = {}
    shared_inputs = build_shared_inputs(N)

    engines = []
    if HAS_APEXBASE:
        engines.append(("ApexBase", ApexBaseBench(tmpdir, data, low_memory=args.low_memory, csv_path=csv_path, parquet_path=parquet_path, json_path=json_path)))
    engines.append(("SQLite", SQLiteBench(tmpdir, data)))
    if HAS_DUCKDB:
        engines.append(("DuckDB", DuckDBBench(tmpdir, data, csv_path=csv_path, parquet_path=parquet_path, json_path=json_path)))

    if not engines:
        print("ERROR: No database engines available!")
        return

    # Setup all engines
    for name, bench in engines:
        bench.shared_inputs = shared_inputs
        bench.setup()

    mem_results = {}  # bench_name -> {eng_name: rss_delta_mb}

    # Run benchmarks
    for bench_name, method_name, is_insert, is_cold, is_warm_nogc, setup_method in selected_benchmarks:
        results[bench_name] = {}
        mem_results.setdefault(bench_name, {})
        for eng_name, bench in engines:
            fn = getattr(bench, method_name, None)
            if fn is None:
                results[bench_name][eng_name] = None
                continue

            if is_insert:
                rss_before = measure_rss_mb()
                with timer() as t:
                    fn()
                ms = t["elapsed_ms"]
                rss_after = measure_rss_mb()
                results[bench_name][eng_name] = ms
                if rss_before and rss_after:
                    mem_results[bench_name][eng_name] = rss_after - rss_before
            elif is_cold:
                # Cold-start (no gc): reopen DB before each iteration
                cold_setup_fn = getattr(bench, "cold_start_setup", None)
                if cold_setup_fn is None:
                    # Engine has no cold_start_setup — run warm no-gc
                    ms = run_bench_nogc(fn, warmup=WARMUP, iterations=ITERS)
                else:
                    rss_before = measure_rss_mb()
                    ms = run_bench_cold_nogc(cold_setup_fn, fn, warmup=WARMUP, iterations=ITERS)
                    rss_after = measure_rss_mb()
                    if rss_before and rss_after:
                        mem_results[bench_name][eng_name] = rss_after - rss_before
                results[bench_name][eng_name] = ms
            elif is_warm_nogc:
                # Warm cached backend, no gc
                rss_before = measure_rss_mb()
                if method_name in MICRO_MEDIAN_BENCHMARK_METHODS:
                    ms = run_bench_nogc_median(fn, warmup=WARMUP, iterations=ITERS)
                else:
                    ms = run_bench_nogc(fn, warmup=WARMUP, iterations=ITERS)
                rss_after = measure_rss_mb()
                results[bench_name][eng_name] = ms
                if rss_before and rss_after:
                    mem_results[bench_name][eng_name] = rss_after - rss_before
            elif setup_method is not None:
                # Per-iteration setup (e.g. pre-insert rows before timing delete)
                per_setup_fn = getattr(bench, setup_method, None)
                if per_setup_fn is None:
                    results[bench_name][eng_name] = None
                    continue
                rss_before = measure_rss_mb()
                ms = run_bench_with_setup(per_setup_fn, fn, warmup=WARMUP, iterations=ITERS)
                rss_after = measure_rss_mb()
                results[bench_name][eng_name] = ms
                if rss_before and rss_after:
                    mem_results[bench_name][eng_name] = rss_after - rss_before
            else:
                rss_before = measure_rss_mb()
                # low_memory mode: disable arrow_batch_cache for ApexBase by reopening each iter
                cold_setup_fn = getattr(bench, "cold_start_setup", None)
                use_cold = (HAS_APEXBASE and isinstance(bench, ApexBaseBench)
                            and bench.low_memory and cold_setup_fn is not None)
                if use_cold:
                    ms = run_bench_cold_nogc(cold_setup_fn, fn, warmup=WARMUP, iterations=ITERS)
                else:
                    ms = run_bench(fn, warmup=WARMUP, iterations=ITERS)
                rss_after = measure_rss_mb()
                results[bench_name][eng_name] = ms
                if rss_before and rss_after:
                    mem_results[bench_name][eng_name] = rss_after - rss_before

            if HAS_APEXBASE and isinstance(bench, ApexBaseBench):
                try:
                    bench.client.flush()
                except Exception:
                    pass

    # Print results tables by the two top-level modules requested by the benchmark.
    eng_names = [name for name, _ in engines]
    col_width = 16

    json_results = []
    olap_sections, oltp_sections = benchmark_sections_for_profile(profile)
    benchmark_sections = olap_sections + oltp_sections
    grouped_names = {spec[0] for _, _, specs in benchmark_sections for spec in specs}
    ungrouped_specs = [spec for spec in selected_benchmarks if spec[0] not in grouped_names]
    if ungrouped_specs:
        oltp_sections.append((
            "OLTP Other Fair Metrics",
            "Metrics not yet classified into a narrower OLTP table.",
            ungrouped_specs,
        ))
        benchmark_sections = olap_sections + oltp_sections

    print_module_header("OLAP", olap_metric_count)
    for section_title, section_description, benchmark_specs in olap_sections:
        json_results.extend(print_benchmark_section(
            section_title,
            section_description,
            benchmark_specs,
            results,
            eng_names,
            col_width,
        ))

    print()

    if profile_runs_extended_sections(profile):
        materialization_results = run_apex_materialization_benchmarks(
            tmpdir,
            data,
            shared_inputs,
            warmup=WARMUP,
            iterations=ITERS,
            low_memory=args.low_memory,
        )
    else:
        materialization_results = []


    if profile_runs_extended_sections(profile):
        existing_engines = {name: bench for name, bench in engines}
        qps_results = run_qps_benchmark(
            tmpdir,
            data,
            n_threads=4,
            min_duration=2.0,
            min_iterations=50,
            existing_engines=existing_engines,
        )
    else:
        qps_results = {}

    print_module_header("OLTP", oltp_metric_count)
    for section_title, section_description, benchmark_specs in oltp_sections:
        json_results.extend(print_benchmark_section(
            section_title,
            section_description,
            benchmark_specs,
            results,
            eng_names,
            col_width,
        ))

    if args.memory:
        print()
        print("Memory delta per fair metric by module (RSS change, MB):")
        name_width = max(42, *(len(spec[0]) for _, _, specs in benchmark_sections for spec in specs))
        mem_header = f"    {'Metric':<{name_width}}"
        for name in eng_names:
            mem_header += f" | {name:>{col_width}}"
        print(mem_header)
        print("    " + "-" * (len(mem_header) - 4))
        for section_title, _, benchmark_specs in benchmark_sections:
            print(f"  {section_title}:")
            for bench_name, _, _, _, _, _ in benchmark_specs:
                row = f"    {bench_name:<{name_width}}"
                for eng_name in eng_names:
                    delta = mem_results.get(bench_name, {}).get(eng_name)
                    if delta is not None:
                        row += f" | {delta:>+{col_width}.1f}"
                    else:
                        row += f" | {'N/A':>{col_width}}"
                print(row)

    fair_workload_scoreboard = print_fair_workload_scoreboard(
        results,
        eng_names,
        selected_benchmarks,
        col_width,
    )

    if "ApexBase" in [n for n, _ in engines]:
        stats = summarize_apex_section(selected_benchmarks, results)
        print(
            f"\nTabular Fair Detail Summary: ApexBase wins {stats['wins']}/{stats['total']}, "
            f"ties {stats['ties']}/{stats['total']}, slower {stats['slower']}/{stats['total']}"
        )

    if args.skip_vector:
        vector_results = {
            "config": {
                "profile": profile,
                "rows": VECTOR_ROWS,
                "dim": VECTOR_DIM,
                "k": VECTOR_K,
                "batch_queries": VECTOR_BATCH_QUERY_COUNT,
                "sqlite_note": VECTOR_SQLITE_NOTE,
            },
            "skipped": True,
            "skip_reason": "Skipped by --skip-vector",
            "head_to_head": [],
            "batch": [],
            "apex_only": [],
            "summary": {"wins": 0, "ties": 0, "slower": 0, "total": 0},
        }
    else:
        vector_results = run_vector_similarity_benchmarks(
            tmpdir,
            rows=VECTOR_ROWS,
            dim=VECTOR_DIM,
            k=VECTOR_K,
            warmup=WARMUP,
            iterations=ITERS,
            profile=profile,
        )

    if profile_runs_extended_sections(profile):
        oltp_results = run_oltp_benchmarks(
            engines,
            warmup=WARMUP,
            iterations=ITERS,
        )
        apex_oltp_diagnostics = run_apex_oltp_diagnostics(
            tmpdir,
            data,
            warmup=WARMUP,
            iterations=ITERS,
        )
        durable_oltp_results = run_oltp_durable_benchmarks(
            engines,
            warmup=WARMUP,
            iterations=ITERS,
        )
        txn_results = run_txn_benchmarks(
            engines,
            warmup=WARMUP,
            iterations=ITERS,
        )
        apex_txn_diagnostics = run_apex_txn_diagnostics(
            tmpdir,
            data,
            warmup=WARMUP,
            iterations=ITERS,
        )
        buffered_oltp_results = run_apex_buffered_oltp_benchmarks(
            tmpdir,
            oltp_results,
            warmup=WARMUP,
            iterations=ITERS,
        )
        memtable_oltp_results = run_apex_memtable_oltp_benchmarks(
            tmpdir,
            oltp_results,
            warmup=WARMUP,
            iterations=ITERS,
        )
    else:
        oltp_results = []
        apex_oltp_diagnostics = []
        durable_oltp_results = []
        txn_results = []
        apex_txn_diagnostics = []
        buffered_oltp_results = []
        memtable_oltp_results = []

    # Cleanup (after Q/s tests, engines are still open)
    for name, bench in engines:
        bench.close()

    # Save JSON if requested
    if args.output:
        output = {
            **get_report_metadata("apexbase-vs-sqlite-duckdb"),
            "config": {
                "profile": profile,
                "rows": N,
                "warmup": WARMUP,
                "iterations": ITERS,
                "vector_rows": VECTOR_ROWS,
                "vector_dim": VECTOR_DIM,
                "vector_k": VECTOR_K,
                "skip_vector": args.skip_vector,
            },
            "results": json_results,
            "fair_workload_scoreboard": fair_workload_scoreboard,
            "vector_similarity": vector_results,
            "apexbase_materialization": materialization_results,
            "qps": qps_results,
            "oltp_microbenchmarks": oltp_results,
            "apexbase_oltp_diagnostics": apex_oltp_diagnostics,
            "oltp_durable_microbenchmarks": durable_oltp_results,
            "transaction_microbenchmarks": txn_results,
            "apexbase_transaction_diagnostics": apex_txn_diagnostics,
            "apexbase_buffered_oltp": buffered_oltp_results,
            "apexbase_memtable_oltp": memtable_oltp_results,
        }
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"Results saved to {args.output}")

    # Cleanup tmpdir
    try:
        shutil.rmtree(tmpdir)
    except Exception:
        pass


if __name__ == "__main__":
    main()
