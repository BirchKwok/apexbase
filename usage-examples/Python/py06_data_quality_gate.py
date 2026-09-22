"""Scenario 06: pre-publish data quality gate (fail when the data is not good enough)

Business context
----------------
The data team produces a ``fact_orders`` detail table every day for downstream
reports and models. There was once an incident: a failed upstream re-run wrote
duplicate order ids and the customer dimension had not synced completely, so a
downstream dashboard counted GMV twice. The post-mortem found that the bad data
was **detectable** -- nobody was detecting it.

So a "pre-publish data quality gate" was introduced: before a table may be marked
"publishable", a set of checks runs and **any failing check fails the flow**
(raises ``AssertionError``), while printing a complete check report so the on-call
engineer can see at a glance which rule failed and by how much.

Five checks (covering the most common data incidents)
-----------------------------------------------------
1. **NOT NULL check**: key columns must not be empty.
2. **Uniqueness check**: the business primary key must not repeat
   (``GROUP BY ... HAVING COUNT(*) > 1``).
3. **Value-range check**: amounts and quantities must fall in business-meaningful ranges.
4. **Referential-integrity check**: every fact customer must exist in the dimension
   table (a ``LEFT JOIN ... WHERE dim.key IS NULL`` anti-join).
5. **Row-count drift check**: this run's row count must not deviate from the
   baseline beyond a threshold (guards against a missed or repeated run).

ApexBase features demonstrated
------------------------------
1. Expressing the "check rules" as parameterised SQL templates, with the rules
   themselves forming a readable data structure.
2. A ``LEFT JOIN ... WHERE dim.key IS NULL`` anti-join for referential integrity.
3. ``CASE WHEN`` plus subqueries to build a "check report" result set.
4. Persisting the row-count baseline in a ``dq_baseline`` control table -- the
   gate's state travels with the database instead of depending on an external
   config file.
5. Wrapping the "gate" in a function that raises, with ``try/except`` demonstrating
   both paths: good data passes, bad data is blocked.

Anti-join forms
---------------
``LEFT JOIN dim ... WHERE dim.key IS NULL`` returns correct results directly, so
the referential-integrity check uses that form; ``NOT EXISTS`` / ``NOT IN`` are
equivalent alternatives.

Capability boundaries (this example follows the supported forms)
----------------------------------------------------------------
1. ``IN`` requires a column on the left-hand side; an expression such as
   ``UPPER(x)`` is not allowed.
2. Time comparison uses string ranges (``YYYY-MM-DD``) rather than ``DATE()`` /
   ``strftime``.

How to run
----------
    python py06_data_quality_gate.py

Output: the console prints two check reports (one passing, one blocked); the
database files live in ``_out/py_06/good_db`` and ``_out/py_06/bad_db``.
"""

from __future__ import annotations

import os
from typing import Callable

from apexbase import ApexClient
from _demo_env import assert_close, rng, section, show, work_dir

SLUG = "06"

GOOD_ROWS = 800          # Row count of the clean batch
BAD_ROWS = 500           # Row count of the problematic batch (37.5% below baseline, triggering the drift alarm)
ROW_DRIFT_TOLERANCE = 0.20   # Row-count drift tolerance: +/-20%

CUSTOMERS = [f"C{i:04d}" for i in range(1, 121)]   # 120 customers
CHANNELS = ["web", "mobile", "partner", "retail"]

# Columns that must not be null. These are chosen because they feed every
# downstream aggregate: losing any one of them breaks GMV / average order value /
# channel reports.
REQUIRED_NOT_NULL = ["order_id", "customer_id", "amount", "quantity", "channel", "day"]

FACT_SCHEMA = {
    "order_id": "int64",
    "customer_id": "string",
    "amount": "float64",
    "quantity": "int64",
    "channel": "string",
    "day": "string",
}


def build_fact_rows(row_count: int, *, corrupt: bool) -> list[dict]:
    """Generate a batch of order fact rows; ``corrupt=True`` injects five known defects.

    Each injected defect corresponds one-to-one with a check, so when the "bad
    batch" is blocked the operator can verify line by line which rule caught what,
    instead of only seeing a generic "the data has a problem".
    """
    rnd = rng(20240601)
    rows: list[dict] = []
    for i in range(row_count):
        amount = round(rnd.uniform(20.0, 5_000.0), 2)
        rows.append(
            {
                "order_id": i + 1,
                "customer_id": CUSTOMERS[i % len(CUSTOMERS)],
                "amount": amount,
                "quantity": 1 + rnd.randrange(9),
                "channel": CHANNELS[i % len(CHANNELS)],
                "day": f"2024-06-{1 + (i % 28):02d}",
            }
        )

    if not corrupt:
        return rows

    # Defect 1: NOT NULL -- blank out customer_id on two rows (a typical symptom of
    # an unsynced upstream dimension).
    rows[3]["customer_id"] = None
    rows[77]["customer_id"] = None

    # Defect 2: uniqueness -- make 5 rows reuse earlier order_ids (primary-key
    # collisions caused by a re-run).
    for offset in range(5):
        rows[100 + offset]["order_id"] = rows[10 + offset]["order_id"]

    # Defect 3: value range -- one negative amount (a refund leaking into the sales
    # measure) and one absurdly large amount.
    rows[7]["amount"] = -123.45
    rows[8]["amount"] = 9_999_999.99

    # Defect 4: referential integrity -- a customer that does not exist in the
    # dimension table at all.
    rows[12]["customer_id"] = "C9999"
    rows[13]["customer_id"] = "C9999"

    # Note: the row-count drift is not injected here -- it is triggered naturally by
    # the difference between GOOD_ROWS(800) and BAD_ROWS(500), which neatly shows
    # the same rule set reacting to a row-count change.
    return rows


def build_dim_rows() -> list[dict]:
    """Customer dimension: every fact.customer_id must be found here."""
    return [
        {
            "customer_id": cust,
            "name": f"Customer {cust}",
            "signup_day": f"2024-0{1 + (i % 5)}-{(i % 28) + 1:02d}",
        }
        for i, cust in enumerate(CUSTOMERS)
    ]


# ============================================================================
# Gate rule implementations
# ============================================================================
# Each check function takes the client and returns (rule name, passed, human-readable detail).
# The shared signature makes the checks easy to drive in a loop and easy to render
# as a report table.
CheckResult = tuple[str, bool, str]


def check_not_null(client: ApexClient) -> CheckResult:
    """NOT NULL check: count NULL rows for each key column.

    One SQL statement uses ``SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END)`` to
    compute all columns at once, scanning the table once instead of N times for N
    columns.
    """
    projections = ",\n                ".join(
        f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls"
        for col in REQUIRED_NOT_NULL
    )
    row = client.execute(
        f"""
        SELECT
            {projections}
        FROM fact_orders
        """
    ).to_dict()[0]
    offenders = {col: int(row[f"{col}_nulls"]) for col in REQUIRED_NOT_NULL if row[f"{col}_nulls"]}
    detail = "no nulls" if not offenders else f"columns containing nulls: {offenders}"
    return ("NOT NULL check", not offenders, detail)


def check_unique(client: ApexClient) -> CheckResult:
    """Uniqueness check: the business primary key ``order_id`` must not repeat.

    ``GROUP BY + HAVING COUNT(*) > 1`` filters duplicate keys directly, which is
    more informative than ``COUNT(*) == COUNT(DISTINCT order_id)`` -- it also tells
    you exactly which keys repeat and how many times each.
    """
    dups = client.execute(
        """
        SELECT order_id, COUNT(*) AS c
        FROM fact_orders
        GROUP BY order_id
        HAVING COUNT(*) > 1
        ORDER BY c DESC, order_id
        """
    ).to_dict()
    detail = (
        "order_id is globally unique"
        if not dups
        else f"{len(dups)} duplicate order_ids, e.g. {[(d['order_id'], d['c']) for d in dups[:3]]}"
    )
    return ("Uniqueness check", not dups, detail)


def check_value_range(client: ApexClient) -> CheckResult:
    """Value-range check: amounts must be positive and under the business cap; quantities must be 1..99."""
    bad_amount = client.execute(
        """
        SELECT COUNT(*) AS n
        FROM fact_orders
        WHERE amount <= 0 OR amount > 100000
        """
    ).scalar()
    bad_quantity = client.execute(
        """
        SELECT COUNT(*) AS n
        FROM fact_orders
        WHERE quantity < 1 OR quantity > 99
        """
    ).scalar()
    total_bad = int(bad_amount) + int(bad_quantity)
    detail = (
        "amount in (0, 100000] and quantity in [1, 99]"
        if total_bad == 0
        else f"{bad_amount} rows out of range for amount, {bad_quantity} for quantity"
    )
    return ("Value-range check", total_bad == 0, detail)


def check_referential_integrity(client: ApexClient) -> CheckResult:
    """Referential-integrity check: every fact.customer_id must exist in the dimension table.

    This uses the direct anti-join ``LEFT JOIN ... WHERE dim.customer_id IS NULL``:
    unmatched fact rows keep NULLs on the dimension side, which is exactly the set
    of orphans. ``NOT EXISTS`` / ``NOT IN`` express the same thing and are equally
    valid.
    """
    orphans = client.execute(
        """
        SELECT f.customer_id, COUNT(*) AS n
        FROM fact_orders f
        LEFT JOIN dim_customer d ON d.customer_id = f.customer_id
        WHERE d.customer_id IS NULL
        GROUP BY f.customer_id
        ORDER BY n DESC, f.customer_id
        """
    ).to_dict()
    detail = (
        "every customer_id matches the dimension table"
        if not orphans
        else f"{len(orphans)} orphan customer_ids: {[(o['customer_id'], o['n']) for o in orphans[:3]]}"
    )
    return ("Referential-integrity check", not orphans, detail)


def check_row_count_drift(client: ApexClient) -> CheckResult:
    """Row-count drift check: the relative deviation from the persisted baseline must be <= the threshold.

    The baseline lives in the ``dq_baseline`` table rather than in a script
    constant, so the gate's "what the last run looked like" travels with the
    database and is reproducible across processes and machines.

    Why this check is needed: NOT NULL / uniqueness-style checks are blind to "half
    the batch went missing" -- the surviving rows are still clean. Only row-count
    drift catches a truncated or skipped run.
    """
    baseline = client.execute(
        "SELECT value FROM dq_baseline WHERE rule = 'fact_orders_row_count'"
    ).scalar()
    if baseline is None:
        return ("Row-count drift check", False, "dq_baseline baseline missing, cannot judge (treated as failed)")

    current = client.count_rows()
    drift = abs(current - float(baseline)) / float(baseline)
    ok = drift <= ROW_DRIFT_TOLERANCE
    detail = (
        f"current {current} rows vs baseline {int(baseline)} rows, drift {drift:.2%}"
        f" (threshold +/-{ROW_DRIFT_TOLERANCE:.0%})"
    )
    return ("Row-count drift check", ok, detail)


# Gate rule list: the order here is the execution order and the report order.
CHECKS: list[Callable[[ApexClient], CheckResult]] = [
    check_not_null,
    check_unique,
    check_value_range,
    check_referential_integrity,
    check_row_count_drift,
]


def run_gate(client: ApexClient, batch_name: str) -> list[CheckResult]:
    """Run the whole gate: print the report first, then decide whether to let it through.

    "Fail when the data is not good enough" is implemented by
    ``raise AssertionError``: if the caller gets a return value, the data was
    published; if it gets an exception, the flow must be blocked. That is exactly
    the shape CI / schedulers integrate with most easily -- a non-zero exit code
    means "blocked".
    """
    print(f"\n--- Data quality gate report: {batch_name} ---")
    print(f"{'rule':<24}{'result':<8}detail")
    print("-" * 88)
    results: list[CheckResult] = []
    for check in CHECKS:
        name, ok, detail = check(client)
        results.append((name, ok, detail))
        print(f"{name:<24}{('PASS' if ok else 'FAIL'):<8}{detail}")

    failures = [name for name, ok, _ in results if not ok]
    print("-" * 88)
    if failures:
        print(f"gate verdict: FAILED ({len(failures)}/{len(results)} rules failed)")
        raise AssertionError(
            f"[{batch_name}] data quality gate failed, {len(failures)} rules failed: "
            + "; ".join(failures)
        )
    print(f"gate verdict: PASSED ({len(results)}/{len(results)} rules met)")
    return results


def seed_baseline(client: ApexClient, row_count: int) -> None:
    """Write/update the row-count baseline. In a real system the last successful publish refreshes it."""
    client.create_table("dq_baseline", {"rule": "string", "value": "float64"})
    client.store([{"rule": "fact_orders_row_count", "value": float(row_count)}])
    client.use_table("fact_orders")


def prepare_batch(db_path: str, rows: list[dict], baseline_rows: int) -> None:
    """Create the database, write the fact and dimension tables, and seed the baseline -- one full batch setup."""
    with ApexClient(db_path) as client:
        client.create_table("fact_orders", FACT_SCHEMA)
        client.store(rows)

        client.create_table(
            "dim_customer",
            {"customer_id": "string", "name": "string", "signup_day": "string"},
        )
        client.store(build_dim_rows())

        seed_baseline(client, baseline_rows)
        show("batch row count", client.count_rows("fact_orders"))


def main() -> None:
    base = work_dir(SLUG)
    good_db = os.path.join(base, "good_db")
    bad_db = os.path.join(base, "bad_db")

    section("Step 1: prepare the 'clean batch' -- baseline 800 rows + 800 compliant rows")
    prepare_batch(good_db, build_fact_rows(GOOD_ROWS, corrupt=False), GOOD_ROWS)

    with ApexClient(good_db) as client:
        client.use_table("fact_orders")
        show("fact table schema sample", client.execute("SELECT * FROM fact_orders ORDER BY order_id LIMIT 2").to_dict())
        show("dimension row count", client.count_rows("dim_customer"))

        section("Step 2: run the gate on the clean batch -- everything must pass")
        good_results = run_gate(client, "2024-06-01 regular batch")
        assert len(good_results) == len(CHECKS), "the report should have one line per rule"
        assert all(ok for _, ok, _ in good_results), "the clean batch must not produce any FAIL"
        print("[OK] the clean batch passed every gate rule and may be published")

        section("Step 3: verify the rules themselves behave (so no rule is a permanent pass)")
        # The biggest risk for a gate is not a missed detection but a **rule written
        # wrongly that always returns pass**. So the clean data gets a reverse check
        # here: set the baseline to an impossible value and the rule must really
        # report FAIL, otherwise the check is just decoration.
        try:
            # A baseline of 1 row puts the drift from the current 800 rows at ~799x,
            # which must trigger.
            client.execute(
                "UPDATE dq_baseline SET value = ? WHERE rule = 'fact_orders_row_count'",
                params=[1.0],
            )
            name, ok, detail = check_row_count_drift(client)
            show("row-count drift check with the baseline set to 1 row", (name, ok, detail))
            assert not ok, "with a shrunken baseline the row-count drift check must report FAIL"
            print("[OK] reverse verification passed: the row-count drift check really fails, it is not a permanent pass")
        finally:
            # Restore the real baseline so later steps are unaffected.
            client.execute(
                "UPDATE dq_baseline SET value = ? WHERE rule = 'fact_orders_row_count'",
                params=[float(GOOD_ROWS)],
            )
            show(
                "baseline after restore",
                client.execute("SELECT value FROM dq_baseline").to_dict(),
            )

        # One more verification: on clean data the referential-integrity rule really
        # does match every customer (confirming the anti-join semantics point the
        # right way: it is not "vacuously empty").
        matched = client.execute(
            """
            SELECT COUNT(*) AS n
            FROM fact_orders f
            WHERE EXISTS (
                SELECT 1 FROM dim_customer d WHERE d.customer_id = f.customer_id
            )
            """
        ).scalar()
        show("fact rows that match the dimension (should be all of them)", matched)
        assert matched == GOOD_ROWS, f"all rows should match, got {matched}"
        print("[OK] NOT EXISTS/EXISTS semantics point the right way: matched rows == fact table rows")

    section("Step 4: prepare the 'problem batch' -- inject five defects")
    prepare_batch(bad_db, build_fact_rows(BAD_ROWS, corrupt=True), GOOD_ROWS)

    with ApexClient(bad_db) as client:
        client.use_table("fact_orders")
        show("problem batch row count vs baseline", (client.count_rows(), GOOD_ROWS))

        section("Step 5: run the gate on the problem batch -- it must be blocked")
        # This is the heart of the example: when the gate does not pass, it must
        # raise. Run each check individually first, so "which rule caught what" can
        # be shown one by one.
        for check in CHECKS:
            name, ok, detail = check(client)
            show(f"{name} (individually)", {"ok": ok, "detail": detail})

        gate_error: AssertionError | None = None
        try:
            run_gate(client, "2024-06-02 upstream re-run batch")
        except AssertionError as exc:
            gate_error = exc
            show("gate blocking exception", str(exc))
        else:
            raise AssertionError("the problem batch should have been blocked by the gate, but it passed")

        assert gate_error is not None, "an AssertionError must be caught"
        message = str(gate_error)
        # All five defects were injected, so all five rules should appear in the failure list.
        for expected in (
            "NOT NULL check",
            "Uniqueness check",
            "Value-range check",
            "Referential-integrity check",
            "Row-count drift check",
        ):
            assert expected in message, f"the failure list should contain '{expected}', got: {message}"
        print("[OK] the problem batch was blocked and all five defects were caught by their rules")

        section("Step 6: convert the gate report to Pandas (ready for alerting/ticketing)")
        report_rows = []
        for check in CHECKS:
            name, ok, detail = check(client)
            report_rows.append(
                {"rule": name, "status": "PASS" if ok else "FAIL", "detail": detail}
            )
        import pandas as pd

        report_df = pd.DataFrame(report_rows)
        print(report_df.to_string(index=False))
        show("report shape", report_df.shape)
        assert (report_df["status"] == "FAIL").all(), "all five checks on the problem batch should be FAIL"

        # An "evidence query" per defect class: pinpoint the gate finding to specific
        # rows -- which is what the on-call engineer actually needs.
        section("Step 7: defect localisation (turning FAIL into specific rows)")
        offenders = client.execute(
            """
            SELECT order_id, customer_id, amount, quantity
            FROM fact_orders
            WHERE customer_id IS NULL
               OR amount <= 0
               OR amount > 100000
            ORDER BY order_id
            LIMIT 5
            """
        ).to_dict()
        for row in offenders:
            show("problem row", row)
        assert len(offenders) > 0, "the problem batch should localise to specific problem rows"

        # GROUP BY is used rather than SELECT DISTINCT because it returns both the
        # distinct orphan list and each orphan's row count in one pass.
        orphan_sample = client.execute(
            """
            SELECT f.customer_id AS customer_id, COUNT(*) AS n
            FROM fact_orders f
            LEFT JOIN dim_customer d ON d.customer_id = f.customer_id
            WHERE d.customer_id IS NULL
            GROUP BY f.customer_id
            ORDER BY f.customer_id
            """
        ).to_dict()
        show("orphan customer_ids (aggregated per customer)", orphan_sample)
        # Two kinds of orphans: the explicitly injected C9999, and rows whose
        # customer_id is NULL (NULL never equals any value in the dimension table, so
        # those rows are orphans by construction -- which is why the referential
        # integrity check and the NOT NULL check alarm at the same time).
        orphan_ids = {r["customer_id"] for r in orphan_sample if r["customer_id"] is not None}
        assert orphan_ids == {"C9999"}, f"the only non-null orphan should be C9999, got {orphan_ids}"
        orphan_total = sum(r["n"] for r in orphan_sample)
        assert orphan_total == 4, f"orphan rows should be 2 NULL + 2 C9999 = 4, got {orphan_total}"

        dup_sample = client.execute(
            """
            SELECT order_id, COUNT(*) AS c
            FROM fact_orders
            GROUP BY order_id
            HAVING COUNT(*) > 1
            ORDER BY order_id
            """
        ).to_dict()
        show("duplicate order_ids", dup_sample)
        assert len(dup_sample) == 5, f"expected 5 duplicate order_ids, got {len(dup_sample)}"

        assert_close(
            float(len(dup_sample)), 5.0, 1e-9, "duplicate primary key count matches the injected defect count"
        )

        print(f"\n=== Scenario 06 pre-publish data quality gate done ===")
        print(f"clean batch: {good_db}\nproblem batch: {bad_db}")


if __name__ == "__main__":
    main()
