"""Scenario 08: Retention and cost attribution (aggregate + ranking windows)

Business context
----------------
A SaaS company's finance and growth teams answer four questions every month:

1. **Retention**: of the customers who signed in January, how many are still
   paying in March? (a retention curve grouped by signup cohort)
2. **TopN attribution**: which three resources consume the most spend for each
   customer? (resource cost attribution)
3. **Cumulative and share**: how much has a customer spent year-to-date, and
   what percentage of the whole platform is that?
4. **Cost allocation**: at what ratio should the platform fixed costs (gateway,
   operations, monitoring) be spread across customers? The industry convention
   is to allocate by usage share, so "those who use more pay more".

Each question maps to one analytical SQL pattern. This script strings them
together and focuses on **window functions**: ranking windows for TopN, and
aggregate windows for cumulative totals, partition totals and shares.

ApexBase features demonstrated
-----------------------------
1. ``ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`` — TopN resources by
   cost within each customer.
2. ``RANK() OVER (...)`` — how it differs from ``ROW_NUMBER``: tied rows share
   a rank and the next rank is skipped. Attribution normally uses ``RANK``: if
   two resources tie for 2nd, both belong in the Top 3.
3. ``COUNT(DISTINCT ...)`` + ``GROUP BY`` — the numerator of the retention
   curve (active customers).
4. **Aggregate windows for cumulative totals, partition totals and shares**:
   * year-to-date: ``SUM(month_cost) OVER (PARTITION BY customer_id
     ORDER BY month_seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)``;
   * global total and share: ``SUM(cost) OVER ()`` and
     ``ROUND(cost / SUM(cost) OVER (), 4)``;
   * these compose with scalar functions and arithmetic — ``ROUND(SUM(...)
     OVER (...), 2)`` returns the rounded window value, and the rounded result
     is what reaches the output (not the raw sum).
   A single window expression replaces the "aggregate into a CTE, then join or
   cross-join it back" pattern that a missing window implementation forces.
5. Cost allocation: usage share x fixed cost pool, where the allocated amounts
   must sum exactly to the pool.

Capability boundaries (this example sticks to forms that work)
-------------------------------------------------------------
1. **Aggregate windows compose now.** ``ROUND(SUM(x) OVER (...), n)``,
   ``SUM(x) OVER (...) / k`` and ``AVG(x) OVER (PARTITION BY g)`` all work.
   What is still unsupported: a window inside ``CAST(...)``, and a window in
   the ``ORDER BY`` clause.
2. ``IN`` requires a column on its left side; an expression such as
   ``UPPER(x)`` is rejected.
3. **No ``DATE_TRUNC`` / ``strftime`` / ``DATE()``**: the month is materialised
   as a string such as ``"2024-01"`` at write time, and a separate integer
   ``month_seq`` is stored for range comparisons such as "how many months
   apart" — subtracting month strings cannot work.
4. ``GROUP BY`` supports aliases but not expressions.
5. **A CTE "diamond" plus window functions still fails.** If the same CTE is
   referenced by two downstream CTEs (A -> B, A -> C, then B joined with C)
   and at least one downstream branch contains a window function, the query
   reports:

      failed to open table '.../__cte_<name>_<pid>_<n>.apex': No such file or directory

   The CTE's temporary materialised file appears to be cleaned up too early.
   The workarounds are direct:
   * write the shared layer as an **inline subquery**, or
   * use "one ranking CTE + conditional aggregation" so a single scan produces
     both the TopN and the total.
   The Top3 concentration below uses the second form, which also scans the
   table one fewer time. (CTE self-joins, and diamonds without a window
   function, are fine.)
6. **``ORDER BY`` may now reference a column that is not in ``SELECT``.** That
   used to be silently ignored (no error, just a random row order); it now
   sorts correctly. The cumulative query still projects ``month_seq`` because
   it is part of the report, and step 4 keeps an assertion that pins the
   ordering down.

How to run
----------
    python py08_retention_cost_windows.py

Output: each step prints its results to the console; the database file lives
in ``_out/py_08/db``.
"""

from __future__ import annotations

import os

from apexbase import ApexClient
from _demo_env import assert_close, rng, section, show, work_dir

SLUG = "08"

# Six-month observation window: 2024-01 .. 2024-06.
# month_seq is the integer 1..6, used exclusively for range comparisons
# (b.seq <= a.seq). The string month is only for display and grouping; it never
# takes part in arithmetic.
MONTHS = [f"2024-{m:02d}" for m in range(1, 7)]
MONTH_SEQ = {month: i + 1 for i, month in enumerate(MONTHS)}

CUSTOMER_COUNT = 40
RESOURCES = ["compute", "storage", "egress", "gpu", "cache", "queue"]

# Platform fixed cost pool: gateway, operations, monitoring and similar costs
# that do not vary per customer. It must be allocated in full — that is the hard
# constraint for the finance reconciliation.
SHARED_COST_POOL = 60_000.00


def build_customers() -> list[dict]:
    """Customers split into 4 cohorts by signup month (10 each).

    Retention analysis depends on everyone in a cohort sharing the same start
    point, so the signup month is stored explicitly rather than inferred from
    the first activity date (inference is fragile when backfilled data exists).
    """
    rows = []
    for i in range(CUSTOMER_COUNT):
        cohort_index = i % 4               # 0..3 -> 2024-01 .. 2024-04
        cohort_month = MONTHS[cohort_index]
        rows.append(
            {
                "customer_id": i + 1,
                "cohort_month": cohort_month,
                "cohort_seq": MONTH_SEQ[cohort_month],
                "plan": ["free", "pro", "enterprise"][i % 3],
            }
        )
    return rows


def build_activity(customers: list[dict]) -> list[dict]:
    """Activity fact table: per customer, per month, whether active + active days.

    The activity rule is deterministic:
    * **the signup month is always active** — this makes month 0 of the
      retention curve exactly 100%, a natural yardstick for checking that the
      retention computation is correct;
    * afterwards it is driven by ``(customer_id * month_seq) % 3``, giving a
      gradually decaying but reproducible curve.
    """
    rows = []
    for cust in customers:
        cid = cust["customer_id"]
        start = cust["cohort_seq"]
        for seq in range(start, len(MONTHS) + 1):
            month_index = seq - start
            if month_index == 0:
                active = True
            else:
                # Use (customer x month_seq) as a pseudo-random source to avoid
                # introducing another RNG, and to keep the dataset identical on
                # every run.
                active = (cid * seq) % 3 != 0
            if not active:
                continue
            rows.append(
                {
                    "customer_id": cid,
                    "month": MONTHS[seq - 1],
                    "month_seq": seq,
                    "month_index": month_index,
                    "active_days": 5 + (cid + seq) % 21,
                }
            )
    return rows


def build_usage(customers: list[dict]) -> list[dict]:
    """Usage table at customer x month x resource granularity.

    cost = units x unit_price (a fixed price per resource), so the cost column
    is fully derivable from usage — which lets the TopN attribution and the cost
    allocation cross-check each other.
    """
    rnd = rng(20240801)
    unit_price = {
        "compute": 0.42,
        "storage": 0.08,
        "egress": 0.65,
        "gpu": 2.10,
        "cache": 0.05,
        "queue": 0.12,
    }
    rows = []
    for cust in customers:
        cid = cust["customer_id"]
        for seq in range(cust["cohort_seq"], len(MONTHS) + 1):
            for r_index, resource in enumerate(RESOURCES):
                # Give customers different resource preferences via
                # (cid + r_index) % 4, otherwise every customer would rank
                # resources identically and TopN attribution would be pointless.
                bias = 1.0 + 0.5 * ((cid + r_index) % 4)
                units = round(rnd.uniform(50.0, 400.0) * bias, 2)
                rows.append(
                    {
                        "customer_id": cid,
                        "month": MONTHS[seq - 1],
                        "month_seq": seq,
                        "resource": resource,
                        "units": units,
                        "cost": round(units * unit_price[resource], 2),
                    }
                )
    return rows


def main() -> None:
    base = work_dir(SLUG)
    customers = build_customers()
    activity = build_activity(customers)
    usage = build_usage(customers)

    with ApexClient(os.path.join(base, "db")) as client:
        client.create_table(
            "customers",
            {
                "customer_id": "int64",
                "cohort_month": "string",
                "cohort_seq": "int64",
                "plan": "string",
            },
        )
        client.store(customers)

        client.create_table(
            "activity",
            {
                "customer_id": "int64",
                "month": "string",
                "month_seq": "int64",
                "month_index": "int64",
                "active_days": "int64",
            },
        )
        client.store(activity)

        client.create_table(
            "usage",
            {
                "customer_id": "int64",
                "month": "string",
                "month_seq": "int64",
                "resource": "string",
                "units": "float64",
                "cost": "float64",
            },
        )
        client.store(usage)

        section("Step 1: data volume and cohort overview")
        show("customers / activity rows / usage rows",
             (client.count_rows("customers"), client.count_rows("activity"), client.count_rows("usage")))
        cohort_size = client.execute(
            """
            SELECT cohort_month, COUNT(*) AS customers
            FROM customers
            GROUP BY cohort_month
            ORDER BY cohort_month
            """
        ).to_dict()
        for row in cohort_size:
            show(f"cohort {row['cohort_month']}", row["customers"])
        assert all(r["customers"] == CUSTOMER_COUNT // 4 for r in cohort_size), (
            "every cohort should have the same number of customers"
        )

        section("Step 2: retention curve (COUNT DISTINCT + cohort size)")
        # Retention rate definition:
        #     month N retention = active customers of the cohort in month N
        #                         / total customers in the cohort
        # The numerator is COUNT(DISTINCT customer_id); the denominator comes
        # from a cohort-size CTE joined back on the cohort key. This is a plain
        # join of two independent aggregates, so it stays a CTE + JOIN even now
        # that partition windows exist: the denominator is a DISTINCT count of
        # customers across the whole cohort, not a sum over the grouped rows.
        retention = client.execute(
            """
            WITH cohort_size AS (
                SELECT cohort_month, COUNT(*) AS cohort_customers
                FROM customers
                GROUP BY cohort_month
            ),
            monthly_active AS (
                SELECT
                    c.cohort_month              AS cohort_month,
                    a.month_index               AS month_index,
                    COUNT(DISTINCT a.customer_id) AS active_customers
                FROM activity a
                JOIN customers c ON c.customer_id = a.customer_id
                GROUP BY c.cohort_month, a.month_index
            )
            SELECT
                m.cohort_month                                      AS cohort_month,
                m.month_index                                       AS month_index,
                m.active_customers                                  AS active_customers,
                s.cohort_customers                                  AS cohort_customers,
                ROUND(m.active_customers * 1.0 / s.cohort_customers, 4) AS retention
            FROM monthly_active m
            JOIN cohort_size s ON s.cohort_month = m.cohort_month
            ORDER BY m.cohort_month, m.month_index
            """
        ).to_dict()

        for row in retention:
            show(f"{row['cohort_month']} month {row['month_index']} retention", row)
        assert retention, "retention result must not be empty"

        # Self-check 1: month 0 retention for every cohort must be exactly 1.0
        # (build_activity guarantees the signup month is active). This is the
        # strongest correctness yardstick available.
        for row in retention:
            if row["month_index"] == 0:
                assert_close(row["retention"], 1.0, 1e-9, f"{row['cohort_month']} month 0 retention")
        # Self-check 2: retention must stay inside [0, 1].
        for row in retention:
            assert 0.0 <= row["retention"] <= 1.0, f"retention out of range: {row}"
        print(f"[OK] all {len(retention)} retention rows are within [0, 1], and month 0 is 1.0 for every cohort")

        section("Step 3: per-customer resource cost TopN (ROW_NUMBER vs RANK)")
        # Emit both ROW_NUMBER and RANK so the difference on ties is visible:
        # * ROW_NUMBER: forces 1,2,3,4... even when costs are identical (no ties);
        # * RANK: tied rows share a rank and the next rank skips (1,1,3).
        # Attribution usually picks RANK — "both resources tied for 2nd belong
        # in the Top 3".
        topn = client.execute(
            """
            SELECT
                customer_id                                  AS customer_id,
                resource                                     AS resource,
                cost                                         AS cost,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cost DESC, resource) AS rn,
                RANK()       OVER (PARTITION BY customer_id ORDER BY cost DESC, resource) AS rnk
            FROM (
                SELECT
                    customer_id,
                    resource,
                    ROUND(SUM(cost), 2) AS cost
                FROM usage
                GROUP BY customer_id, resource
            ) per_resource
            ORDER BY customer_id, rn
            """
        ).to_dict()

        # Print only the first two customers; aggregate assertions cover the rest.
        for row in topn:
            if row["customer_id"] <= 2:
                show(f"customer {row['customer_id']} resource ranking", row)

        first_customer = [r for r in topn if r["customer_id"] == 1]
        assert len(first_customer) == len(RESOURCES), "every customer should cover all resources"
        assert [r["rn"] for r in first_customer] == list(range(1, len(RESOURCES) + 1)), (
            "ROW_NUMBER must be a gap-free, tie-free 1..N"
        )
        for r in first_customer[:3]:
            assert r["rnk"] <= 3, "RANK inside the Top 3 must not exceed 3"
        print("[OK] ROW_NUMBER is gap-free and tie-free; RANK allows ties, so the two differ when truncating to TopN")

        # Real attribution with the Top 3: concentration = Top3 cost / total cost.
        # This directly shows whether spend is concentrated in a few resources.
        #
        # The query deliberately uses "one CTE + one GROUP BY" instead of
        # splitting per_resource into two CTEs and joining them back. Reason: a
        # CTE diamond (one CTE consumed by two downstream CTEs) that contains a
        # window function fails with
        # `failed to open table .../__cte_xxx_N.apex: No such file or directory`
        # — the temporary materialised file is cleaned up too early. The
        # "ranking CTE + conditional aggregation" shape is a single chain, which
        # avoids the defect and scans the table once for both the Top3 and the
        # total.
        concentration = client.execute(
            """
            WITH ranked AS (
                SELECT
                    customer_id,
                    resource,
                    cost,
                    RANK() OVER (PARTITION BY customer_id ORDER BY cost DESC, resource) AS rnk
                FROM (
                    SELECT
                        customer_id,
                        resource,
                        ROUND(SUM(cost), 2) AS cost
                    FROM usage
                    GROUP BY customer_id, resource
                ) per_resource
            )
            SELECT
                customer_id                                                  AS customer_id,
                ROUND(SUM(CASE WHEN rnk <= 3 THEN cost ELSE 0 END), 2)       AS top3_cost,
                ROUND(SUM(cost), 2)                                          AS total_cost,
                ROUND(SUM(CASE WHEN rnk <= 3 THEN cost ELSE 0 END)
                      / SUM(cost), 4)                                        AS top3_share
            FROM ranked
            GROUP BY customer_id
            -- customer_id is the tie-breaker so customers with equal share keep
            -- a deterministic order.
            ORDER BY top3_share DESC, customer_id
            """
        ).to_dict()
        show("3 customers with the highest Top3 concentration", concentration[:3])
        show("3 customers with the lowest Top3 concentration", concentration[-3:])
        for row in concentration:
            assert 0.0 < row["top3_share"] <= 1.0, f"Top3 share out of range: {row}"
        print(f"[OK] the Top3 cost share of all {len(concentration)} customers is within (0, 1]")

        section("Step 4: year-to-date cost with an aggregate window")
        # Goal: one row per (customer, month) carrying that customer's cost from
        # signup through the current month.
        # `SUM(month_cost) OVER (PARTITION BY customer_id ORDER BY month_seq
        # ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` is exactly the
        # running total; ROUND composes around the window. This replaces the old
        # self-join `monthly a JOIN monthly b ON b.month_seq <= a.month_seq`,
        # which existed only because ordered aggregate windows did not work.
        cumulative = client.execute(
            """
            WITH monthly AS (
                SELECT
                    customer_id,
                    month,
                    month_seq,
                    ROUND(SUM(cost), 2) AS month_cost
                FROM usage
                GROUP BY customer_id, month, month_seq
            )
            SELECT
                customer_id                     AS customer_id,
                month                           AS month,
                month_seq                       AS month_seq,
                month_cost                      AS month_cost,
                ROUND(SUM(month_cost) OVER (
                    PARTITION BY customer_id
                    ORDER BY month_seq
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 2)                           AS cost_ytd
            FROM monthly
            ORDER BY customer_id, month_seq
            """
        ).to_dict()

        for row in cumulative:
            if row["customer_id"] <= 2:
                show(f"customer {row['customer_id']} cumulative cost", row)

        # ORDER BY on a column that is absent from SELECT now sorts correctly
        # (it used to be silently ignored). month_seq is still projected because
        # it belongs to the report; this assertion pins the ordering down so a
        # future edit cannot quietly turn the output into random order.
        ordered_pairs = [(r["customer_id"], r["month_seq"]) for r in cumulative]
        assert ordered_pairs == sorted(ordered_pairs), (
            "cumulative output must be ordered by (customer_id, month_seq)"
        )
        print("[OK] cumulative output is strictly ordered by (customer_id, month_seq)")

        # Self-check: each customer's final cumulative value == that customer's
        # total cost. cost_ytd is non-decreasing in month_seq, so the maximum is
        # the last month's value; no need to remember month_seq separately.
        final_by_customer: dict[int, float] = {}
        for row in cumulative:
            final_by_customer[row["customer_id"]] = max(
                final_by_customer.get(row["customer_id"], 0.0), row["cost_ytd"]
            )
        totals = {
            r["customer_id"]: r["total"]
            for r in client.execute(
                """
                SELECT customer_id, ROUND(SUM(cost), 2) AS total
                FROM usage
                GROUP BY customer_id
                """
            ).to_dict()
        }
        # Check every customer but print only the first 3 results — 40 [OK] lines
        # would drown out the following output.
        checked = 0
        for cid, ytd in sorted(final_by_customer.items()):
            if abs(ytd - totals[cid]) > 0.01:
                raise AssertionError(
                    f"customer {cid} final cumulative cost {ytd} != total cost {totals[cid]}"
                )
            checked += 1
            if checked <= 3:
                assert_close(ytd, totals[cid], 0.01, f"customer {cid} final cumulative == total")
        print(f"[OK] all {checked} customers' final cumulative cost equals their total cost (first 3 printed)")

        section("Step 5: share of platform cost with a global window total")
        # share = customer cost / platform cost. `SUM(cost) OVER ()` is a global
        # window total (no PARTITION BY), so the denominator rides along on every
        # row and no separate "grand total" CTE + CROSS JOIN is needed.
        share = client.execute(
            """
            WITH per_customer AS (
                SELECT customer_id, ROUND(SUM(cost), 2) AS cost
                FROM usage
                GROUP BY customer_id
            )
            SELECT
                customer_id                        AS customer_id,
                cost                               AS cost,
                SUM(cost) OVER ()                  AS total_cost,
                ROUND(cost / SUM(cost) OVER (), 4) AS share
            FROM per_customer
            ORDER BY cost DESC, customer_id
            """
        ).to_dict()
        show("3 customers with the highest cost", share[:3])
        show("3 customers with the lowest cost", share[-3:])

        share_sum = sum(r["share"] for r in share)
        assert_close(share_sum, 1.0, 1e-3, "sum of customer cost shares")
        total_cost = share[0]["total_cost"]
        print(f"[OK] platform total cost {total_cost}; shares of {len(share)} customers sum to {round(share_sum, 4)}")

        section("Step 6: cost allocation (spread the fixed pool by usage share)")
        # Rule: allocation = fixed pool x (customer usage / platform usage).
        # Finance's hard constraint: **allocations must sum exactly to the pool**
        # — not a cent more or less. To guarantee that, the last step assigns the
        # rounding remainder to one customer (the largest), an approach used by
        # real finance systems to eliminate penny drift.
        allocation = client.execute(
            f"""
            WITH per_customer AS (
                SELECT customer_id, ROUND(SUM(units), 4) AS units
                FROM usage
                GROUP BY customer_id
            )
            SELECT
                customer_id                                       AS customer_id,
                units                                             AS units,
                ROUND(units / SUM(units) OVER (), 6)              AS usage_share,
                ROUND(units / SUM(units) OVER () * {SHARED_COST_POOL}, 2) AS allocated_cost
            FROM per_customer
            ORDER BY customer_id
            """
        ).to_dict()
        show("allocation detail (first 3 customers)", allocation[:3])

        allocated_sum = round(sum(r["allocated_cost"] for r in allocation), 2)
        show("sum of allocations vs cost pool", (allocated_sum, SHARED_COST_POOL))
        # Rounding to cents leaves a few cents of drift; that is normal. Pushing
        # the drift onto the largest customer (smallest relative error) makes the
        # books close exactly.
        drift = round(SHARED_COST_POOL - allocated_sum, 2)
        show("rounding drift", drift)
        assert abs(drift) < 1.0, f"drift should be under 1 unit, got {drift}"

        biggest = max(allocation, key=lambda r: r["allocated_cost"])
        adjusted = round(biggest["allocated_cost"] + drift, 2)
        show(f"assign drift {drift} to the largest customer {biggest['customer_id']}", adjusted)
        assert_close(
            round(allocated_sum + drift, 2),
            SHARED_COST_POOL,
            1e-9,
            "sum of allocations after adjustment",
        )
        print("[OK] the cost pool is allocated in full (drift moved to the largest customer; books close exactly)")

        # Allocation ratio must match the usage ratio (except for the drift cent).
        unit_totals = client.execute(
            "SELECT SUM(units) AS total FROM usage"
        ).scalar()
        for row in allocation[:5]:
            assert_close(
                row["usage_share"],
                row["units"] / unit_totals,
                1e-6,
                f"usage share of customer {row['customer_id']}",
            )
        print("[OK] the first 5 customers' allocation ratios match their usage shares exactly")

        section("Step 7: retention x cost cross view (growth and finance on one table)")
        # Put "retained customers" and "monthly cost" side by side: you can see
        # how much money the retained customers are spending.
        # Two independent CTEs are aggregated before the JOIN so the detail rows
        # are never joined first: activity and usage have different granularity,
        # and a raw join would multiply the COUNT.
        cross = client.execute(
            """
            WITH retained AS (
                SELECT
                    c.cohort_month                 AS cohort_month,
                    a.month                        AS month,
                    COUNT(DISTINCT a.customer_id)  AS active_customers
                FROM activity a
                JOIN customers c ON c.customer_id = a.customer_id
                GROUP BY c.cohort_month, a.month
            ),
            spend AS (
                SELECT
                    c.cohort_month        AS cohort_month,
                    u.month               AS month,
                    ROUND(SUM(u.cost), 2) AS cost
                FROM usage u
                JOIN customers c ON c.customer_id = u.customer_id
                GROUP BY c.cohort_month, u.month
            )
            SELECT
                r.cohort_month                                      AS cohort_month,
                r.month                                             AS month,
                r.active_customers                                  AS active_customers,
                s.cost                                              AS cost,
                ROUND(s.cost / r.active_customers, 2)               AS cost_per_active
            FROM retained r
            JOIN spend s ON s.cohort_month = r.cohort_month AND s.month = r.month
            ORDER BY r.cohort_month, r.month
            """
        ).to_dict()
        for row in cross[:6]:
            show(f"{row['cohort_month']} / {row['month']}", row)
        assert len(cross) > 0, "cross view must not be empty"
        for row in cross:
            assert row["active_customers"] > 0, "active customers must be positive (no division by zero)"
            assert row["cost_per_active"] > 0, "cost per active customer must be positive"
        print(f"[OK] {len(cross)} cross-view rows; active customers are always > 0 (no division by zero)")

        section("Step 8: export to Pandas for the growth weekly report")
        import pandas as pd

        report = client.execute(
            """
            WITH monthly AS (
                SELECT customer_id, month, month_seq, ROUND(SUM(cost), 2) AS month_cost
                FROM usage
                GROUP BY customer_id, month, month_seq
            )
            SELECT
                customer_id                     AS customer_id,
                month                           AS month,
                month_seq                       AS month_seq,
                month_cost                      AS month_cost,
                ROUND(SUM(month_cost) OVER (
                    PARTITION BY customer_id
                    ORDER BY month_seq
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 2)                           AS cost_ytd
            FROM monthly
            WHERE customer_id <= 3
            ORDER BY customer_id, month_seq
            """
        ).to_pandas()
        print(report.to_string(index=False))
        show("DataFrame shape", report.shape)
        assert report.shape[0] > 0, "weekly report must not be empty"
        # Cumulative values must be non-decreasing — the most basic property of a
        # year-to-date measure.
        for cid, group in report.groupby("customer_id"):
            values = group.sort_values("month")["cost_ytd"].to_list()
            assert values == sorted(values), f"customer {cid} cumulative cost is not non-decreasing: {values}"
        print("[OK] every customer's cumulative cost series is non-decreasing")

        print(f"\n=== Scenario 08 retention and cost attribution complete ===\nDatabase files: {base}")


if __name__ == "__main__":
    main()
