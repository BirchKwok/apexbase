"""R5.1: EXPLAIN ANALYZE physical path trace (architecture review R5).

Covers, against the installed release wheel:
1. ``Actual Path:`` appears in EXPLAIN ANALYZE output and in no other plan.
2. Metadata COUNT(*), generic scan, and O(1) ``_id`` point-lookup routes.
3. The batched scan pipeline route reports the batch count on a
   multi-row-group table (200k narrow rows span two adaptive row groups).
4. Shapes outside every fast path report the generic executor route.
"""

import re
import subprocess
import sys
import os
import tempfile

from apexbase import ApexClient

FIXTURE_ROWS = 200_000  # > 1 adaptive row group (131072 rows for narrow data)


def _make_client(dirpath):
    return ApexClient(dirpath, drop_if_exists=True, enable_cache=False)


def _seed(client, rows=FIXTURE_ROWS):
    client.create_table(
        "users",
        {"code": "int", "score": "float", "city": "string"},
    )
    client.use_table("users")
    chunk = 50_000
    for start in range(0, rows, chunk):
        end = min(start + chunk, rows)
        client.store(
            {
                "code": [i % 7 for i in range(start, end)],
                "score": [(i % 97) * 0.5 for i in range(start, end)],
                "city": [f"city{i % 13}" for i in range(start, end)],
            }
        )
    client.flush()


def _plan_text(client, sql):
    """EXPLAIN returns one row with a single ``plan`` column."""
    return client.execute(sql).to_dict()[0]["plan"]


def _actual_path(plan):
    line = plan.split("Actual Path:", 1)[1].splitlines()[0]
    return line.strip()


def _estimated_cost(plan):
    match = re.search(r"Chosen Plan: .*estimated_cost=([\d.]+)", plan)
    assert match, f"missing estimated_cost in plan:\n{plan}"
    return float(match.group(1))


def test_explain_analyze_reports_simple_read_paths():
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        _seed(client)
        try:
            plan = _plan_text(client, "EXPLAIN ANALYZE SELECT COUNT(*) FROM users")
            assert "Actual Path: count_star_metadata" in plan

            # A projected page read is bounded now; it used to materialize the
            # table and slice, which made any non-zero OFFSET a full table read.
            plan = _plan_text(client, "EXPLAIN ANALYZE SELECT city FROM users LIMIT 5")
            assert "Actual Path: bounded_projection" in plan

            plan = _plan_text(
                client, "EXPLAIN ANALYZE SELECT city, code FROM users WHERE _id = 3"
            )
            assert "Actual Path: id_point_lookup" in plan
        finally:
            client.close()


def test_explain_analyze_reports_batched_scan_pipeline_with_batch_count():
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        _seed(client)
        try:
            # Two group keys: the fused single-key kernels reject this shape,
            # so the query reaches the scan-group pipeline and the batched
            # row-group stream (same shape as the R3 parity tests).
            plan = _plan_text(
                client,
                "EXPLAIN ANALYZE SELECT city, code, COUNT(*) AS n FROM users "
                "WHERE score >= 20 GROUP BY city, code",
            )
            path = _actual_path(plan)
            assert path.startswith("batched_scan_pipeline(batches="), path
            batches = int(path[len("batched_scan_pipeline(batches="):-1])
            assert batches >= 2, "two-row-group fixture must consume >= 2 batches"
        finally:
            client.close()


def test_explain_analyze_reports_generic_executor_path():
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        _seed(client)
        try:
            plan = _plan_text(
                client,
                "EXPLAIN ANALYZE SELECT city, code FROM users "
                "WHERE score > 25 AND code <= 4 ORDER BY code",
            )
            assert "Actual Path: generic_executor" in plan
        finally:
            client.close()


def test_plain_explain_does_not_report_actual_path():
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        _seed(client)
        try:
            plan = _plan_text(client, "EXPLAIN SELECT COUNT(*) FROM users")
            assert "Actual Path" not in plan

            plan = _plan_text(client, "EXPLAIN SELECT city FROM users LIMIT 5")
            assert "Actual Path" not in plan
        finally:
            client.close()


def test_explain_analyze_reports_plan_divergence_for_skewed_index():
    """R5.2: the planner's index choice is plan-driven; when the index route
    turns out to be unexecutable at runtime, EXPLAIN ANALYZE reports the
    plan/execution divergence."""
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        client.create_table("users", {"score": "float", "city": "string"})
        client.use_table("users")
        rows = 10_000
        chunk = 5_000
        for start in range(0, rows, chunk):
            end = min(start + chunk, rows)
            client.store(
                {
                    "score": [(i % 97) * 0.5 for i in range(start, end)],
                    # 50% of the rows carry one skewed value, the rest spread
                    # over 7 tail values (city NDV = 8).
                    "city": [
                        "heavy" if i % 2 == 0 else f"t{i % 7}"
                        for i in range(start, end)
                    ],
                }
            )
        client.flush()
        try:
            client.execute("CREATE INDEX idx_city ON users(city)")
            client.execute("ANALYZE users")

            # Skewed value: the planner prices it at 1/NDV and chooses the
            # index, but the MCV-based execution selectivity (0.5) makes the
            # full scan cheaper, so the index route is not usable.
            plan = _plan_text(
                client,
                "EXPLAIN ANALYZE SELECT * FROM users WHERE city = 'heavy'",
            )
            assert "Chosen Plan: OltpIndexLookup" in plan
            assert (
                "Plan Divergence: plan chose index access; index route "
                "unavailable at execution; fell back to scan" in plan
            )
            assert _actual_path(plan) != "index_accelerated_read"

            # Rare value: both cost models agree on the index route and it is
            # actually used, so no divergence is reported.
            plan = _plan_text(
                client,
                "EXPLAIN ANALYZE SELECT * FROM users WHERE city = 't1'",
            )
            assert "Chosen Plan: OltpIndexLookup" in plan
            assert "Plan Divergence" not in plan
            assert _actual_path(plan) == "index_accelerated_read"
        finally:
            client.close()


def test_explain_analyze_time_calibration_updates_plan_cost():
    """R5.3: EXPLAIN ANALYZE records the measured time of the cost class
    that actually executed; the next EXPLAIN ANALYZE of the same shape
    reports calibrated costs and the applied feedback."""
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        client.create_table("users", {"score": "float", "city": "string"})
        client.use_table("users")
        rows = 10_000
        chunk = 5_000
        for start in range(0, rows, chunk):
            end = min(start + chunk, rows)
            client.store(
                {
                    "score": [(i % 97) * 0.5 for i in range(start, end)],
                    # 50% of the rows carry one skewed value, the rest spread
                    # over 7 tail values (city NDV = 8).
                    "city": [
                        "heavy" if i % 2 == 0 else f"t{i % 7}"
                        for i in range(start, end)
                    ],
                }
            )
        client.flush()
        try:
            client.execute("CREATE INDEX idx_city ON users(city)")
            client.execute("ANALYZE users")

            sql = "EXPLAIN ANALYZE SELECT * FROM users WHERE city = 'heavy'"
            first = _plan_text(client, sql)

            second = _plan_text(client, sql)
            assert "Feedback: applied" in second
            # The calibrated second plan must price the chosen candidate
            # differently from the uncalibrated first plan (measured time
            # enters the cost comparison; the winning candidate may stay
            # the same, but the cost scale changes).
            assert _estimated_cost(second) != _estimated_cost(first)
            assert "Feedback Recorded: yes" in second
        finally:
            client.close()


def test_plan_feedback_persists_across_sessions():
    """R5.8: plan feedback recorded by EXPLAIN ANALYZE is persisted to a
    per-table sidecar and reloaded by a fresh process; the first EXPLAIN
    ANALYZE of a shape that has feedback from a previous session reports
    the applied feedback (before R5.8 the state was lost on exit)."""
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        client.create_table("users", {"score": "float", "city": "string"})
        client.use_table("users")
        rows = 10_000
        chunk = 5_000
        for start in range(0, rows, chunk):
            end = min(start + chunk, rows)
            client.store(
                {
                    "score": [(i % 97) * 0.5 for i in range(start, end)],
                    "city": [
                        "heavy" if i % 2 == 0 else f"t{i % 7}"
                        for i in range(start, end)
                    ],
                }
            )
        client.flush()
        sidecar = os.path.join(tmp, "users.apex.plan_feedback")
        try:
            client.execute("CREATE INDEX idx_city ON users(city)")
            client.execute("ANALYZE users")

            sql = "EXPLAIN ANALYZE SELECT * FROM users WHERE city = 'heavy'"
            first = _plan_text(client, sql)
            assert "Feedback: applied" not in first
            assert "Feedback Recorded: yes" in first
            assert os.path.exists(sidecar)
        finally:
            client.close()

        # Fresh process: the in-memory feedback is empty, so the applied
        # feedback on the first EXPLAIN ANALYZE can only come from the
        # persisted sidecar.
        child = (
            "from apexbase import ApexClient\n"
            f"c = ApexClient({tmp!r})\n"
            'c.use_table("users")\n'
            f"plan = c.execute({sql!r}).to_dict()[0]['plan']\n"
            "c.close()\n"
            "assert 'Feedback: applied' in plan, plan\n"
            "print('CHILD_OK')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", child], capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr
        assert "CHILD_OK" in result.stdout


def test_plan_feedback_sidecar_reaped_on_same_name_recreate():
    """R5.8: dropping a table and recreating the same name reaps the
    feedback sidecar with the table files (no stale calibration for the
    recreated table)."""
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        client.create_table("users", {"score": "float"})
        client.use_table("users")
        client.store({"score": [float(i) for i in range(1000)]})
        client.flush()
        sidecar = os.path.join(tmp, "users.apex.plan_feedback")
        try:
            client.execute(
                "EXPLAIN ANALYZE SELECT COUNT(*) FROM users WHERE score > 10"
            )
            assert os.path.exists(sidecar)
            client.execute("DROP TABLE users")
            client.create_table("users", {"score": "float"})
        finally:
            client.close()
        assert not os.path.exists(sidecar)


def test_explain_analyze_reports_join_and_cte_paths():
    """R5.5: JOIN and CTE routes report their physical path in
    EXPLAIN ANALYZE (these routes previously had no Actual Path line)."""
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        client.create_table(
            "orders", {"id": "int", "user_id": "int", "amount": "int"}
        )
        client.use_table("orders")
        client.store(
            {
                "id": [1, 2, 3, 4, 5],
                "user_id": [1, 2, 1, 3, 2],
                "amount": [10, 20, 30, 40, 50],
            }
        )
        client.flush()
        client.create_table("users", {"id": "int", "city": "string"})
        client.use_table("users")
        client.store({"id": [1, 2, 3, 9], "city": ["a", "b", "c", "x"]})
        client.flush()
        try:
            # General hash-join route.
            plan = _plan_text(
                client,
                "EXPLAIN ANALYZE SELECT orders.amount, users.city "
                "FROM orders JOIN users ON orders.user_id = users.id",
            )
            assert _actual_path(plan) == "hash_join"

            # Single-use CTE is inlined (no materialization).
            plan = _plan_text(
                client,
                "EXPLAIN ANALYZE WITH top AS "
                "(SELECT amount FROM orders WHERE amount > 25) SELECT * FROM top",
            )
            assert _actual_path(plan) == "cte_inline"

            # Multi-reference CTE is materialized into the shared batch cache.
            plan = _plan_text(
                client,
                "EXPLAIN ANALYZE WITH top AS "
                "(SELECT amount FROM orders WHERE amount > 25) "
                "SELECT (SELECT COUNT(*) FROM top) AS n, "
                "(SELECT MAX(amount) FROM top) AS m",
            )
            assert _actual_path(plan) == "cte_materialize"

            # Recursive CTE runs the iterative fixpoint loop.
            plan = _plan_text(
                client,
                "EXPLAIN ANALYZE WITH RECURSIVE fact(n) AS "
                "(SELECT 1 UNION ALL SELECT n + 1 FROM fact WHERE n < 5) "
                "SELECT n FROM fact",
            )
            assert _actual_path(plan) == "cte_recursive"
        finally:
            client.close()


def test_explain_analyze_reports_index_spec():
    """R5.6: index candidates carry a directly executable spec; EXPLAIN
    ANALYZE displays the chosen candidate's spec (extracted predicates,
    covering-scan attempt and residual-skip decisions)."""
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        client.create_table("users", {"score": "float", "city": "string"})
        client.use_table("users")
        rows = 10_000
        chunk = 5_000
        for start in range(0, rows, chunk):
            end = min(start + chunk, rows)
            client.store(
                {
                    "score": [(i % 97) * 0.5 for i in range(start, end)],
                    # 50% of the rows carry one skewed value, the rest spread
                    # over 7 tail values (city NDV = 8).
                    "city": [
                        "heavy" if i % 2 == 0 else f"t{i % 7}"
                        for i in range(start, end)
                    ],
                }
            )
        client.flush()
        try:
            client.execute("CREATE INDEX idx_city ON users(city)")
            client.execute("ANALYZE users")

            # Skewed value: the planner chooses the index candidate and its
            # spec is displayed (no composite index: covering attempt is
            # allowed, residual filter stays).
            plan = _plan_text(
                client,
                "EXPLAIN ANALYZE SELECT * FROM users WHERE city = 'heavy'",
            )
            assert "Chosen Plan: OltpIndexLookup" in plan
            spec_lines = [
                line for line in plan.splitlines() if "Index Spec:" in line
            ]
            assert len(spec_lines) == 1, (
                f"exactly one Index Spec line expected:\n{plan}"
            )
            assert "city=" in spec_lines[0]
            assert "Eq" in spec_lines[0]
            assert "covering_scan=true" in spec_lines[0]
            assert "skip_residual_filter=false" in spec_lines[0]
            assert "Plan Divergence" in plan

            # Rare value: the same spec shape, and the index route executes.
            plan = _plan_text(
                client,
                "EXPLAIN ANALYZE SELECT * FROM users WHERE city = 't1'",
            )
            spec_lines = [
                line for line in plan.splitlines() if "Index Spec:" in line
            ]
            assert len(spec_lines) == 1
            assert "city=" in spec_lines[0]
            assert "covering_scan=true" in spec_lines[0]
            assert _actual_path(plan) == "index_accelerated_read"
        finally:
            client.close()
