"""R3: serial batched Filter -> GROUP BY -> HAVING -> TopK pipeline.

Covers, against the installed release wheel:
1. Multi-batch vs single-batch result parity with the in-process
   APEX_BATCH_SCAN toggle (the Rust executor reads the variable per query).
2. Delta state (appended rows, DeltaStore deletes and cell updates) is
   served by the batched path through the overlay batch stream and keeps
   parity with the single-batch path.
3. Bounded scan memory: with a fixed number of groups, the batched path's
   peak RSS must stay well below the single-batch materialization on a
   multi-row-group table (measured in separate child processes).
"""

import json
import os
import subprocess
import sys
import tempfile

from apexbase import ApexClient

FIXTURE_ROWS = 200_000  # > 2 adaptive row groups (131072 rows for narrow data)

QUERIES = (
    # Int key, int source, HAVING over a SELECT aggregate.
    (
        "SELECT code, COUNT(*) AS n, SUM(amount) AS s, MIN(amount) AS mn, MAX(amount) AS mx "
        "FROM perf_scan WHERE amount >= -100 AND code >= 1 "
        "GROUP BY code HAVING SUM(amount) > -5000 ORDER BY n DESC, code LIMIT 10"
    ),
    # Two keys (string + int), float source, range + IN.
    (
        "SELECT city, code, COUNT(*) AS n, AVG(score) AS av, MIN(score) AS mn, "
        "MAX(score) AS mx, SUM(score) AS s "
        "FROM perf_scan WHERE score >= 20 AND score <= 40 AND code IN (1, 3, 5) "
        "GROUP BY city, code HAVING COUNT(*) > 2 ORDER BY av DESC, city, code LIMIT 20"
    ),
    # Bool group key.
    (
        "SELECT flag, COUNT(*) AS n FROM perf_scan WHERE amount > 0 "
        "GROUP BY flag HAVING COUNT(*) > 100 ORDER BY n DESC, flag LIMIT 5"
    ),
    # No HAVING, COUNT only.
    (
        "SELECT city, COUNT(*) AS n FROM perf_scan WHERE code <= 2 "
        "GROUP BY city ORDER BY n DESC, city LIMIT 5"
    ),
    # OR tree with BETWEEN + IN.
    (
        "SELECT city, COUNT(*) AS n, AVG(amount) AS av FROM perf_scan "
        "WHERE amount BETWEEN -100 AND 100 OR city IN ('city1', 'city7') "
        "GROUP BY city HAVING COUNT(*) > 10 ORDER BY av DESC, city LIMIT 7 OFFSET 1"
    ),
    # Predicate matching no rows: empty results must match too.
    (
        "SELECT city, COUNT(*) AS n FROM perf_scan WHERE code = 99 "
        "GROUP BY city ORDER BY n DESC, city LIMIT 3"
    ),
    # Three-key GROUP BY is outside the batch gate: both env states must
    # agree through the fallback wiring.
    (
        "SELECT city, code, flag, COUNT(*) AS n FROM perf_scan "
        "WHERE amount > 0 AND code <= 4 GROUP BY city, code, flag "
        "ORDER BY n DESC, city, code, flag LIMIT 5"
    ),
)


def _make_client(dirpath):
    return ApexClient(dirpath, drop_if_exists=True, enable_cache=False)


def _seed(client, rows=FIXTURE_ROWS):
    client.create_table(
        "perf_scan",
        {"code": "int", "amount": "int", "score": "float", "city": "string", "flag": "bool"},
    )
    client.use_table("perf_scan")
    chunk = 50_000
    for start in range(0, rows, chunk):
        end = min(start + chunk, rows)
        client.store(
            {
                "code": [i % 7 for i in range(start, end)],
                "amount": [i % 500 - 250 for i in range(start, end)],
                "score": [(i % 97) * 0.5 for i in range(start, end)],
                "city": [f"city{i % 13}" for i in range(start, end)],
                "flag": [i % 2 == 0 for i in range(start, end)],
            }
        )
    client.flush()


def _run(client, sql):
    return client.execute(sql).to_dict()


def _ab(client, sql):
    os.environ["APEX_BATCH_SCAN"] = "0"
    try:
        off = _run(client, sql)
        os.environ["APEX_BATCH_SCAN"] = "1"
        try:
            on = _run(client, sql)
        finally:
            os.environ.pop("APEX_BATCH_SCAN", None)
    finally:
        os.environ.pop("APEX_BATCH_SCAN", None)
    return off, on


def test_batch_scan_pipeline_matches_single_batch_pipeline():
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        _seed(client)
        try:
            for sql in QUERIES:
                off, on = _ab(client, sql)
                assert on == off, (
                    f"batched/single-batch results diverge:\n{sql}\n"
                    f"off={off[:3]}\non ={on[:3]}"
                )
        finally:
            client.close()


def test_batch_scan_streams_delta_state_with_parity():
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        _seed(client)
        try:
            client.execute("BEGIN")
            client.execute(
                "INSERT INTO perf_scan (city, code, amount, score, flag) "
                "VALUES ('city3', 3, 10, 25.5, true)"
            )
            client.execute("COMMIT")
            # DeltaStore cell updates must be patched per row group by _id.
            client.execute("UPDATE perf_scan SET amount = 777 WHERE code = 2")
            sql = QUERIES[1]
            off, on = _ab(client, sql)
            assert on == off, "delta state must stream and keep parity"
            os.environ["APEX_BATCH_SCAN"] = "1"
            try:
                plan = _run(client, "EXPLAIN ANALYZE " + sql)[0]["plan"]
            finally:
                os.environ.pop("APEX_BATCH_SCAN", None)
            assert "batched_scan_pipeline" in plan, plan

            # A persisted row-group deletion still keeps parity, whichever
            # lane serves it.
            client.execute("DELETE FROM perf_scan WHERE _id = 200")
            off, on = _ab(client, sql)
            assert on == off, "deleted overlay row must keep parity"
        finally:
            client.close()


def test_negative_bound_queries_match_known_values():
    # Regression: negative literals parse as UnaryOp(Minus, literal) and used
    # to defeat the typed scan protocol, silently demoting range queries with
    # negative bounds. The folded predicate must keep exact selection
    # semantics in both env states.
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        client.create_table("perf_scan", {"code": "int", "amount": "int"})
        client.use_table("perf_scan")
        client.store({"code": [1, 1, 2, 2, 3], "amount": [-150, -50, 10, 250, -10]})
        client.flush()
        try:
            for env in ("0", "1"):
                os.environ["APEX_BATCH_SCAN"] = env
                rows = client.execute(
                    "SELECT code, COUNT(*) AS n FROM perf_scan WHERE amount >= -100 "
                    "GROUP BY code ORDER BY code"
                ).to_dict()
                assert [(r["code"], r["n"]) for r in rows] == [(1, 1), (2, 2), (3, 1)]
            os.environ.pop("APEX_BATCH_SCAN", None)
        finally:
            os.environ.pop("APEX_BATCH_SCAN", None)


_CHILD_MEASURE = r"""
import json
import os
import resource
import sys

from apexbase import ApexClient


def peak_rss_mb():
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return usage / (1024 * 1024)
    return usage / 1024


def main():
    dirpath = sys.argv[1]
    client = ApexClient(dirpath, enable_cache=False)
    client.use_table("perf_scan")
    # Two group keys: the fused single-key kernel rejects this shape, so the
    # query reaches the scan-group pipeline and the batched row-group stream.
    sql = (
        "SELECT city, code, COUNT(*) AS n, SUM(value) AS s "
        "FROM perf_scan WHERE value >= 0 "
        "GROUP BY city, code HAVING COUNT(*) > 1000 "
        "ORDER BY n DESC, city, code LIMIT 5"
    )
    for _ in range(3):
        client.execute(sql).to_dict()
    client.close()
    print(json.dumps({"peak_rss_mb": peak_rss_mb()}))


main()
"""


def _seed_memory_table(dirpath, rows):
    client = _make_client(dirpath)
    try:
        client.create_table(
            "perf_scan",
            {"city": "string", "code": "int", "value": "int"},
        )
        client.use_table("perf_scan")
        chunk = 100_000
        for start in range(0, rows, chunk):
            end = min(start + chunk, rows)
            client.store(
                {
                    "city": [f"city{i % 8}" for i in range(start, end)],
                    "code": [i % 5 for i in range(start, end)],
                    "value": [i % 997 for i in range(start, end)],
                }
            )
        client.flush()
    finally:
        client.close()


def _child_peak_rss_mb(dirpath, batch_enabled):
    env = dict(os.environ)
    env["APEX_BATCH_SCAN"] = "1" if batch_enabled else "0"
    completed = subprocess.run(
        [sys.executable, "-c", _CHILD_MEASURE, str(dirpath)],
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    return payload["peak_rss_mb"]


def test_batch_scan_keeps_peak_rss_bounded():
    # 1.2M rows over 40 fixed groups: the single-batch side materializes the
    # whole table while the batched side only ever holds one row group. The
    # query uses two group keys because the fused single-key kernel intercepts
    # single-key shapes before the scan-group pipeline (and the batched
    # pipeline) is reached.
    with tempfile.TemporaryDirectory() as tmp:
        _seed_memory_table(tmp, 1_200_000)
        single_peak = _child_peak_rss_mb(tmp, False)
        batch_peak = _child_peak_rss_mb(tmp, True)
    assert single_peak > 0 and batch_peak > 0
    assert batch_peak < single_peak * 0.85, (
        f"batched peak {batch_peak:.1f} MB is not below 85% of the "
        f"single-batch peak {single_peak:.1f} MB"
    )


# ---------------------------------------------------------------------------
# R5.7: parallel (morsel) fold of the batch pipeline, opt-in via
# APEX_PARALLEL_SCAN=N (read per query, like APEX_BATCH_SCAN).
# ---------------------------------------------------------------------------


def _run_parallel(client, sql, threads):
    os.environ["APEX_PARALLEL_SCAN"] = str(threads)
    try:
        return _run(client, sql)
    finally:
        os.environ.pop("APEX_PARALLEL_SCAN", None)


def test_parallel_batch_scan_matches_serial_pipeline():
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        _seed(client)
        try:
            for sql in QUERIES:
                os.environ["APEX_BATCH_SCAN"] = "1"
                try:
                    serial = _run(client, sql)
                finally:
                    os.environ.pop("APEX_BATCH_SCAN", None)
                for threads in (2, 4, 8):
                    parallel = _run_parallel(client, sql, threads)
                    assert parallel == serial, (
                        f"parallel({threads})/serial results diverge:\n{sql}\n"
                        f"serial   ={str(serial[:3])}\nparallel={str(parallel[:3])}"
                    )
        finally:
            client.close()


def test_parallel_batch_scan_reports_fused_path_detail():
    # B-phase fused scan+fold: the granted worker count is reported in
    # the EXPLAIN ANALYZE path detail, and results match serial.
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        _seed(client)
        try:
            sql = QUERIES[0]
            serial = _run(client, sql)
            os.environ["APEX_PARALLEL_SCAN"] = "2"
            try:
                parallel = _run(client, sql)
                plan = _run(client, "EXPLAIN ANALYZE " + sql)
            finally:
                os.environ.pop("APEX_PARALLEL_SCAN", None)
            assert parallel == serial, (
                f"fused/serial results diverge:\n{sql}\n"
                f"serial   ={str(serial[:3])}\nparallel={str(parallel[:3])}"
            )
            plan_text = plan[0]["plan"]
            assert "batched_scan_pipeline(batches=" in plan_text, plan_text
            assert ", parallel=2)" in plan_text, plan_text
        finally:
            client.close()


# ---------------------------------------------------------------------------
# R5.12: cost-based auto-enable (APEX_PARALLEL_SCAN unset)
# ---------------------------------------------------------------------------


def test_parallel_batch_scan_auto_enables_after_calibration():
    # With APEX_PARALLEL_SCAN unset, the first EXPLAIN ANALYZE of a shape
    # runs serial and records the calibrated serial class; the next run of
    # the same shape whose calibrated serial time reaches the 2 ms
    # threshold auto-enables the fused parallel scan, and results must
    # match the forced-serial run.
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        _seed(client)
        try:
            sql = QUERIES[0]
            os.environ.pop("APEX_PARALLEL_SCAN", None)
            first_plan = _run(client, "EXPLAIN ANALYZE " + sql)[0]["plan"]
            assert "batched_scan_pipeline(batches=" in first_plan, first_plan
            assert "parallel=" not in first_plan, first_plan
            assert "Feedback Recorded: yes" in first_plan, first_plan

            second_plan = _run(client, "EXPLAIN ANALYZE " + sql)[0]["plan"]
            assert "batched_scan_pipeline(batches=" in second_plan, second_plan
            assert ", parallel=" in second_plan, second_plan

            os.environ["APEX_PARALLEL_SCAN"] = "0"
            try:
                serial = _run(client, sql)
            finally:
                os.environ.pop("APEX_PARALLEL_SCAN", None)
            auto = _run(client, sql)
            assert auto == serial, (
                f"auto/serial results diverge:\n{sql}\n"
                f"serial={str(serial[:3])}\nauto  ={str(auto[:3])}"
            )
        finally:
            client.close()


def test_parallel_batch_scan_auto_stays_serial_below_threshold():
    # Shapes whose calibrated serial time stays below the 2 ms threshold
    # keep the serial default with APEX_PARALLEL_SCAN unset (1K rows:
    # single row group, sub-millisecond serial scan).
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        _seed(client, rows=1_000)
        try:
            os.environ.pop("APEX_PARALLEL_SCAN", None)
            for _ in range(2):
                plan = _run(client, "EXPLAIN ANALYZE " + QUERIES[0])[0]["plan"]
                assert "batched_scan_pipeline(batches=" in plan, plan
                assert "parallel=" not in plan, plan
        finally:
            client.close()


def test_parallel_batch_scan_streams_delta_state_with_parity():
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        _seed(client)
        try:
            client.execute("BEGIN")
            client.execute(
                "INSERT INTO perf_scan (city, code, amount, score, flag) "
                "VALUES ('city3', 3, 10, 25.5, true)"
            )
            client.execute("COMMIT")
            client.execute("UPDATE perf_scan SET amount = 777 WHERE code = 2")
            sql = QUERIES[1]
            os.environ["APEX_BATCH_SCAN"] = "1"
            try:
                serial = _run(client, sql)
            finally:
                os.environ.pop("APEX_BATCH_SCAN", None)
            parallel = _run_parallel(client, sql, 4)
            assert parallel == serial, "delta state must stream and keep parity"
        finally:
            client.close()


_CHILD_PARALLEL_MEASURE = r"""
import json
import os
import resource
import sys

from apexbase import ApexClient


def peak_rss_mb():
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return usage / (1024 * 1024)
    return usage / 1024


def main():
    dirpath = sys.argv[1]
    threads = int(sys.argv[2])
    client = ApexClient(dirpath, enable_cache=False)
    client.use_table("perf_scan")
    sql = (
        "SELECT city, code, COUNT(*) AS n, SUM(value) AS s "
        "FROM perf_scan WHERE value >= 0 "
        "GROUP BY city, code HAVING COUNT(*) > 1000 "
        "ORDER BY n DESC, city, code LIMIT 5"
    )
    if threads > 1:
        os.environ["APEX_PARALLEL_SCAN"] = str(threads)
    for _ in range(3):
        client.execute(sql).to_dict()
    client.close()
    print(json.dumps({"peak_rss_mb": peak_rss_mb()}))


main()
"""


def _child_parallel_peak_rss_mb(dirpath, threads):
    env = dict(os.environ)
    env["APEX_BATCH_SCAN"] = "1"
    completed = subprocess.run(
        [sys.executable, "-c", _CHILD_PARALLEL_MEASURE, str(dirpath), str(threads)],
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    return payload["peak_rss_mb"]


def test_parallel_batch_scan_keeps_peak_rss_bounded_by_threads():
    # The parallel fold buffers the narrow projected columns plus one
    # partial group map per thread; the peak must stay within the
    # thread-scaled bound relative to the single-batch materialization
    # (architecture review §14.8.4).
    with tempfile.TemporaryDirectory() as tmp:
        _seed_memory_table(tmp, 1_200_000)
        single_peak = _child_peak_rss_mb(tmp, False)
        for threads in (2, 4):
            parallel_peak = _child_parallel_peak_rss_mb(tmp, threads)
            assert parallel_peak < single_peak * 0.85 * threads, (
                f"parallel peak {parallel_peak:.1f} MB exceeds the "
                f"{threads}-thread bound of the single-batch peak "
                f"{single_peak:.1f} MB"
            )


def test_canary_parallel_only_profile_loads_base_table():
    # The standalone --parallel-only profile (used by the full-mode par
    # phase of the local perf guard) skips the Bulk Insert spec, so the
    # profile must load the persisted base table by itself before the
    # batch pipeline setup copies it (architecture review R5.7).
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "par-only.json")
        script = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "benchmarks",
            "bench_perf_canary.py",
        )
        completed = subprocess.run(
            [
                sys.executable,
                script,
                "--parallel-only",
                "--rows",
                "200000",
                "--warmup",
                "1",
                "--iterations",
                "2",
                "--output",
                out,
            ],
            capture_output=True,
            text=True,
            timeout=600,
        )
        assert completed.returncode == 0, completed.stderr
        results = json.loads(open(out, encoding="utf-8").read())["results"]
        names = [r["query"] for r in results]
        assert names == [
            "Parallel batch scan (2 threads)",
            "Parallel batch scan (4 threads)",
            "Parallel batch scan (8 threads)",
            "Parallel batch scan (auto)",
        ]
