"""Isolated re-test of 'DISTINCT (city, category)' on the fresh release build.

Replicates exactly what bench_vs_sqlite_duckdb.py does for this metric:
- identical dataset generation (seed 42, same CITIES/CATEGORIES)
- identical per-engine setup (same schema, same insert path)
- warm benchmark semantics: run_bench(fn, warmup=2, iterations=5), averaged

Runs the three engines in separate processes and interleaves R independent
rounds so we can measure run-to-run variance vs competitor gap.
"""
import os
import random
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time

N = 1_000_000
SQL = "SELECT DISTINCT city, category FROM bench ORDER BY city, category"


def generate_data(n):
    rng = random.Random(42)
    return {
        "name": [f"user_{i}" for i in range(n)],
        "age": [rng.randint(18, 80) for _ in range(n)],
        "score": [round(rng.uniform(0, 100), 2) for _ in range(n)],
        "city": [rng.choice(CITIES) for _ in range(n)],
        "category": [rng.choice(CATEGORIES) for _ in range(n)],
    }


CITIES = ["Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou",
          "Nanjing", "Chengdu", "Wuhan", "Xian", "Qingdao"]
CATEGORIES = ["Electronics", "Clothing", "Food", "Sports", "Books",
              "Home", "Auto", "Health", "Travel", "Gaming"]


def run_one(engine, data):
    """Setup + warm benchmark inside this process. Returns avg ms."""
    if engine == "sqlite":
        import sqlite3 as s3
        tmp = tempfile.mkdtemp(prefix="apex_iso_sq_")
        db = os.path.join(tmp, "bench.db")
        conn = s3.connect(db)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("CREATE TABLE bench (_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER, score REAL, city TEXT, category TEXT)")
        rows = list(zip(data["name"], data["age"], data["score"], data["city"], data["category"]))
        t0 = time.perf_counter()
        conn.executemany("INSERT INTO bench (name, age, score, city, category) VALUES (?,?,?,?,?)", rows)
        conn.commit()
        insert_ms = (time.perf_counter() - t0) * 1000
        fn = lambda: (lambda cur: list(cur.fetchall()))(conn.execute(SQL))
    elif engine == "duckdb":
        import duckdb
        tmp = tempfile.mkdtemp(prefix="apex_iso_duck_")
        db = os.path.join(tmp, "bench.duckdb")
        conn = duckdb.connect(db)
        conn.execute("CREATE TABLE bench (name VARCHAR, age INTEGER, score DOUBLE, city VARCHAR, category VARCHAR)")
        rows = list(zip(data["name"], data["age"], data["score"], data["city"], data["category"]))
        t0 = time.perf_counter()
        conn.executemany("INSERT INTO bench VALUES (?,?,?,?,?)", rows)
        insert_ms = (time.perf_counter() - t0) * 1000
        fn = lambda: list(conn.execute(SQL).fetchall())
    else:
        from apexbase import ApexClient
        tmp = tempfile.mkdtemp(prefix="apex_iso_apx_")
        client = ApexClient(tmp, drop_if_exists=True)
        client.create_table("default")
        client.use_table("default")
        t0 = time.perf_counter()
        client.store(data)
        insert_ms = (time.perf_counter() - t0) * 1000
        fn = lambda: client.execute("SELECT DISTINCT city, category FROM default ORDER BY city, category",
                                    show_internal_id=True).to_dict()

    # warm benchmark: 2 warmup + 5 timed, average
    for _ in range(2):
        fn()
    times = []
    for _ in range(5):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return sum(times) / len(times), insert_ms, times


def main():
    engine = sys.argv[1]
    rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    data = generate_data(N)
    results = []
    for r in range(rounds):
        avg, insert_ms, per = run_one(engine, data)
        results.append(avg)
        print(f"[{engine}] round {r}: avg={avg:.3f} ms  min={min(per):.3f} max={max(per):.3f} "
              f"insert={insert_ms:.0f}ms", flush=True)
    print(f"[{engine}] SUMMARY rounds={[f'{x:.3f}' for x in results]} "
          f"mean={statistics.mean(results):.3f} median={statistics.median(results):.3f} "
          f"stdev={statistics.stdev(results) if len(results) > 1 else 0:.3f}", flush=True)


if __name__ == "__main__":
    main()
