"""Scenario 09: window functions tour (ranking / offset / bucketing / aggregate windows)

Business context
----------------
A sales operations team builds a monthly "regional leaderboard", and a single
SQL query has to answer:

* Where does each salesperson rank **within their own region**? (ranking window
  + PARTITION BY)
* How are ties handled, and who belongs on the "Top 3 incentive list"?
* What is the month-over-month growth? (``LAG`` / ``LEAD`` fetch the previous /
  next period)
* What is the region champion's revenue? (``FIRST_VALUE``)
* Split the salespeople into revenue quartiles — who is in the top 25%?
  (``NTILE``)
* What share of the regional total does this salesperson contribute, and what
  is the region's running total through this month? (**aggregate windows**)

The first five questions are ranking / offset windows. The last one is now a
direct ``SUM(revenue) OVER (PARTITION BY region)`` (plus an ordered running
total) instead of the self-join that a missing aggregate-window implementation
used to force.

ApexBase features demonstrated
-----------------------------
1. **The ranking trio**: ``ROW_NUMBER()`` / ``RANK()`` / ``DENSE_RANK()`` side
   by side on data that contains ties, so the difference is obvious.
2. **Offset**: ``LAG()`` / ``LEAD()`` fetch the previous / next row of the same
   partition (month-over-month movement).
3. **First / last value**: ``FIRST_VALUE()`` / ``LAST_VALUE()``.
   Note that ``LAST_VALUE()`` under the default frame returns the **whole
   partition's** last value, not the last value up to the current row — the most
   common misconception about this function.
4. **Bucketing**: ``NTILE(n)`` splits a partition into n roughly equal buckets
   (the leading buckets take the extra row).
5. **Aggregate windows**: partition totals, running totals and shares as direct
   ``SUM(...) OVER (...)`` expressions:
   * ``ROUND(SUM(x) OVER (PARTITION BY g, m), 2)`` — the scalar function wraps
     the window and is actually applied;
   * ``x / SUM(x) OVER (PARTITION BY g, m)`` — a window inside arithmetic;
   * ``SUM(x) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING
     AND CURRENT ROW)`` — the physical-row running total, and
     ``SUM(x) OVER (PARTITION BY g ORDER BY k)`` — the same running total from
     the default frame (no explicit frame needed).

Capability boundaries and notes
-------------------------------
* **No** ``DATE_TRUNC`` / ``strftime`` / ``DATE()`` / ``EXTRACT``: the month is
  stored as a string such as ``"2024-01"``, and "how many periods apart"
  comparisons use the integer ``seq`` column, because month strings cannot be
  subtracted.
* **Aggregate windows compose**: ``ROUND(SUM(...) OVER (...), n)``, arithmetic
  on a window, and ``AVG(...) OVER (PARTITION BY g)`` all work. What is still
  unsupported: a window inside ``CAST(...)``, and a window in ``ORDER BY``.
* **Bounded ``ROWS n PRECEDING`` frames are not reliable**: ``AVG(...) OVER
  (... ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)`` is rejected with an error rather than evaluated:
  the whole-partition average, and ``COUNT(...)`` with such a frame can panic.
  Only the cumulative ``ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`` is available, so
  the 3-period moving average in step 6 keeps its self-join, which emulates the
  bounded frame with a range condition.
* ``LAST_VALUE`` also ignores an explicit frame in this engine: it still returns
  the whole partition's last value even with ``ROWS BETWEEN UNBOUNDED PRECEDING
  AND CURRENT ROW``. Read it as "the partition's last value", never as a
  row-by-row advancing value.
* Filtering a ``ROW_NUMBER`` alias with a bare ``<=`` still hits an engine
  pushdown panic (``index out of bounds`` in the Arrow take kernel), so step 2
  truncates with the equivalent ``BETWEEN 1 AND 3`` instead (``ROW_NUMBER`` is
  always >= 1). ``RANK`` does not need that workaround.
* ``IN`` requires a column on its left side; an expression such as ``UPPER(x)``
  is rejected.
* ``ORDER BY`` may now reference a column that is not in ``SELECT`` (it used to
  be silently ignored). ``seq`` is still projected where it belongs to the
  output, but that is no longer required for sorting to take effect.

How to run
----------
    python py09_window_functions_tour.py

Output: each step prints its results and the semantic differences to the
console; the database file lives in ``_out/py_09/db``.
"""

from __future__ import annotations

import os

from apexbase import ApexClient
from _demo_env import assert_close, section, show, work_dir

SLUG = "09"

REGIONS = ["east", "west"]
MONTHS = [f"2024-{m:02d}" for m in range(1, 7)]
REPS_PER_REGION = 4          # four salespeople per region


def build_sales_rows() -> list[dict]:
    """Build deterministic sales data and **deliberately create ties** in a partition.

    Why ties matter: the difference between ``ROW_NUMBER`` / ``RANK`` /
    ``DENSE_RANK`` only shows up when values repeat. With no duplicates the three
    produce identical output and the example demonstrates nothing.

    Construction: revenue depends only on ``(local index, month index, region
    index)`` with **no random noise** — any noise would break the ties. Formula:

        revenue = 100 + 20 * ((local_index * 2 + month_index) % 4)
                  + 10 * region_index + 3 * month_index

    Because ``(local_index * 2) % 4`` only ever takes 0 or 2, each region forms
    exactly **two tied pairs** every month: local indices 0/2 share one level and
    1/3 share the other. So ``RANK`` yields ``1,1,3,3`` while ``DENSE_RANK``
    yields ``1,1,2,2`` — a very visible difference.
    """
    reps = [
        {"rep": f"{region[0].upper()}{i + 1}", "region": region, "local": i}
        for region in REGIONS
        for i in range(REPS_PER_REGION)
    ]
    rows = []
    for rep in reps:
        region_index = REGIONS.index(rep["region"])
        for month_index, month in enumerate(MONTHS, start=1):
            revenue = float(
                100
                + 20 * ((rep["local"] * 2 + month_index) % 4)
                + 10 * region_index
                + 3 * month_index
            )
            rows.append(
                {
                    "rep": rep["rep"],
                    "region": rep["region"],
                    "month": month,
                    "seq": month_index,
                    "revenue": revenue,
                }
            )
    return rows


def main() -> None:
    base = work_dir(SLUG)
    rows = build_sales_rows()
    # Every salesperson's revenue in 2024-06, used for the within-partition
    # ranking demonstrations.
    focus_month = "2024-06"

    with ApexClient(os.path.join(base, "db")) as client:
        client.create_table(
            "sales",
            {
                "rep": "string",
                "region": "string",
                "month": "string",
                "seq": "int64",
                "revenue": "float64",
            },
        )
        client.store(rows)

        section("Step 0: data overview (ties included on purpose)")
        show("total rows", client.count_rows())
        focus = client.execute(
            "SELECT rep, region, revenue FROM sales WHERE month = ? ORDER BY region, revenue DESC",
            params=[focus_month],
        ).to_dict()
        for row in focus:
            show(f"{row['region']} / {row['rep']}", row["revenue"])

        # Prove with SQL itself that ties exist — every later assertion depends
        # on this being true.
        ties = client.execute(
            """
            SELECT region, revenue, COUNT(*) AS c
            FROM sales
            WHERE month = ?
            GROUP BY region, revenue
            HAVING COUNT(*) > 1
            ORDER BY region, revenue DESC
            """,
            params=[focus_month],
        ).to_dict()
        show(f"tied revenue values in {focus_month}", ties)
        assert ties, "the example data must contain ties, otherwise the three ranking functions cannot differ"
        print(f"[OK] the data contains {len(ties)} tied groups, enough to show the ranking semantics")

        section("Step 1: ROW_NUMBER vs RANK vs DENSE_RANK (same ORDER BY, three semantics)")
        # All three order by revenue DESC; they differ only in how ties are
        # handled:
        #   ROW_NUMBER: forces distinct 1,2,3,4... (ties still get an order)
        #   RANK      : tied rows share a rank, the next rank **skips** (1,1,3)
        #   DENSE_RANK: tied rows share a rank, the next rank **does not skip** (1,1,2)
        #
        # Note the difference in the ORDER BY of the three OVER clauses — a
        # practical rule of thumb:
        #   * ROW_NUMBER's ORDER BY adds `, rep` (a unique column). Which of two
        #     tied rows comes first is **undefined** in SQL; without a
        #     tie-breaker ROW_NUMBER can assign them differently on every run.
        #     Adding a unique column pins down a stable order, which is the
        #     standard way to make ROW_NUMBER reproducible.
        #   * RANK / DENSE_RANK deliberately do **not** add it — expressing the
        #     tie is exactly the point; a unique tie-breaker would split the tie
        #     and destroy the demonstration.
        ranks = client.execute(
            """
            SELECT
                region                                                  AS region,
                rep                                                     AS rep,
                revenue                                                 AS revenue,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS rn,
                RANK()       OVER (PARTITION BY region ORDER BY revenue DESC) AS rnk,
                DENSE_RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS dense_rnk
            FROM sales
            WHERE month = ?
            ORDER BY region, rn
            """,
            params=[focus_month],
        ).to_dict()
        print(f"{'region':<8}{'rep':<8}{'revenue':<10}{'ROW_NUMBER':<12}{'RANK':<8}{'DENSE_RANK'}")
        print("-" * 56)
        for row in ranks:
            print(
                f"{row['region']:<8}{row['rep']:<8}{row['revenue']:<10.0f}"
                f"{row['rn']:<12}{row['rnk']:<8}{row['dense_rnk']}"
            )

        # Semantic assertion 1: ROW_NUMBER must be 1..n and distinct per partition.
        for region in REGIONS:
            rns = sorted(r["rn"] for r in ranks if r["region"] == region)
            assert rns == list(range(1, REPS_PER_REGION + 1)), (
                f"{region} ROW_NUMBER should be 1..{REPS_PER_REGION}, got {rns}"
            )
        # Semantic assertion 2: on any row, RANK >= DENSE_RANK >= 1.
        for row in ranks:
            assert row["rnk"] >= row["dense_rnk"] >= 1, f"bad rank relation: {row}"
        # Semantic assertion 3: with ties present, some row must have RANK > DENSE_RANK.
        divergence = [r for r in ranks if r["rnk"] != r["dense_rnk"]]
        show("rows where RANK and DENSE_RANK diverge", divergence)
        assert divergence, "with ties present RANK > DENSE_RANK must occur, otherwise ranking is broken"
        # Semantic assertion 4: max DENSE_RANK <= number of distinct values in the partition.
        for region in REGIONS:
            distinct_values = len({r["revenue"] for r in ranks if r["region"] == region})
            max_dense = max(r["dense_rnk"] for r in ranks if r["region"] == region)
            assert max_dense <= distinct_values, (
                f"{region} max DENSE_RANK {max_dense} must not exceed distinct values {distinct_values}"
            )
        print("[OK] ROW_NUMBER is strictly increasing with no ties; RANK skips; DENSE_RANK does not — all three verified")

        section("Step 2: who gets truncated by TopN? (the business impact of ties)")
        # This is where the ranking choice actually changes the business result.
        # With 4 people per region forming two tied pairs, the ranks are
        # RANK: 1,1,3,3 and DENSE_RANK: 1,1,2,2.
        #   ROW_NUMBER Top3 -> exactly 3 people;
        #   RANK Top3       -> 4 people (rank <= 3 pulls in the whole tied group).
        # Incentive / attribution scenarios should use RANK so ties are treated
        # equally.
        # NOTE: the filter is written as `rn BETWEEN 1 AND 3` rather than
        # `rn <= 3`. ROW_NUMBER is always >= 1, so the two are equivalent, but a
        # bare `<=` on a ROW_NUMBER alias still trips an engine pushdown panic
        # (`index out of bounds` in arrow-select take), while BETWEEN returns the
        # correct rows. RANK has no such problem, so the query below keeps `<= 3`.
        by_rownumber = client.execute(
            """
            SELECT rep, region, revenue, rn FROM (
                SELECT
                    rep, region, revenue,
                    ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS rn
                FROM sales WHERE month = ?
            ) x
            WHERE rn BETWEEN 1 AND 3
            ORDER BY region, rn
            """,
            params=[focus_month],
        ).to_dict()
        by_rank = client.execute(
            """
            SELECT rep, region, revenue, rnk FROM (
                SELECT
                    rep, region, revenue,
                    RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS rnk
                FROM sales WHERE month = ?
            ) x
            WHERE rnk <= 3
            ORDER BY region, rnk, rep
            """,
            params=[focus_month],
        ).to_dict()
        show("ROW_NUMBER Top3 picks", [(r["region"], r["rep"], r["revenue"]) for r in by_rownumber])
        show("RANK Top3 picks", [(r["region"], r["rep"], r["revenue"]) for r in by_rank])
        # The RANK set can only be >= the ROW_NUMBER set (ties widen the cutoff).
        assert len(by_rank) > len(by_rownumber), (
            f"two tied pairs should make RANK's Top3 strictly larger than ROW_NUMBER's: "
            f"{len(by_rank)} vs {len(by_rownumber)}"
        )
        assert len(by_rownumber) == 3 * len(REGIONS), "ROW_NUMBER Top3 should be exactly 3 people per region"
        assert len(by_rank) == 4 * len(REGIONS), "RANK Top3 should include the whole tied group (4 per region)"
        print("[OK] RANK's TopN widens the cutoff because of ties (3 -> 4 per region); use RANK for attribution / incentives")

        section("Step 3: LAG / LEAD (month-over-month movement and trend)")
        # LAG(x) reads the **previous** row of the same partition in ORDER BY
        # order; LEAD reads the next one. At a partition boundary it returns NULL
        # — the correct representation of "no previous period", not 0.
        trend = client.execute(
            """
            SELECT
                rep                                         AS rep,
                seq                                         AS seq,
                month                                       AS month,
                revenue                                     AS revenue,
                LAG(revenue)  OVER (PARTITION BY rep ORDER BY seq) AS prev_revenue,
                LEAD(revenue) OVER (PARTITION BY rep ORDER BY seq) AS next_revenue
            FROM sales
            WHERE rep = ?
            ORDER BY seq
            """,
            params=["E1"],
        ).to_dict()
        for row in trend:
            show(f"E1 / {row['month']}", row)

        # Semantic assertion: no LAG in the first period, no LEAD in the last,
        # and neighbouring values line up exactly.
        assert trend[0]["prev_revenue"] is None, "the first period must have no previous value (LAG is NULL)"
        assert trend[-1]["next_revenue"] is None, "the last period must have no next value (LEAD is NULL)"
        for i in range(1, len(trend)):
            assert trend[i]["prev_revenue"] == trend[i - 1]["revenue"], (
                f"period {i} LAG should equal period {i - 1} revenue"
            )
        for i in range(len(trend) - 1):
            assert trend[i]["next_revenue"] == trend[i + 1]["revenue"], (
                f"period {i} LEAD should equal period {i + 1} revenue"
            )
        print("[OK] LAG/LEAD return NULL at partition boundaries and match adjacent rows inside the partition")

        # Month-over-month change: computed directly from LAG. The arithmetic is
        # done in the outer query over the projected column, which keeps the
        # window expression itself at the top level of its SELECT.
        mom = client.execute(
            """
            SELECT
                rep                                     AS rep,
                month                                   AS month,
                revenue                                 AS revenue,
                prev_revenue                            AS prev_revenue,
                ROUND(revenue - prev_revenue, 2)        AS mom_change
            FROM (
                SELECT
                    rep, month, seq, revenue,
                    LAG(revenue) OVER (PARTITION BY rep ORDER BY seq) AS prev_revenue
                FROM sales
            ) x
            WHERE rep = ? AND prev_revenue IS NOT NULL
            ORDER BY seq
            """,
            params=["E1"],
        ).to_dict()
        for row in mom:
            show(f"E1 month-over-month {row['month']}", row)
        assert all(r["mom_change"] == r["revenue"] - r["prev_revenue"] for r in mom), (
            "month-over-month change should be current minus previous"
        )
        print("[OK] month-over-month change matches the difference of adjacent periods exactly")

        section("Step 4: FIRST_VALUE / LAST_VALUE (first and last value)")
        # The most commonly misunderstood pair:
        #   FIRST_VALUE(x) OVER (PARTITION BY ... ORDER BY ...)
        #     -> the **first** value of the ordered partition;
        #   LAST_VALUE(x)  OVER (PARTITION BY ... ORDER BY ...)
        #     -> under the default frame (which covers the whole partition)
        #        returns the **partition's last value**, NOT "the last value up to
        #        the current row".
        #   So LAST_VALUE is the same number on every row. Other aggregates do
        #   advance row by row with an explicit `ROWS BETWEEN UNBOUNDED PRECEDING
        #   AND CURRENT ROW` frame (see step 6), but LAST_VALUE still returns the
        #   partition's last value even with that frame, so read region_bottom as
        #   "the region's lowest revenue" and nothing more.
        fv = client.execute(
            """
            SELECT
                rep                                                     AS rep,
                seq                                                     AS seq,
                revenue                                                 AS revenue,
                FIRST_VALUE(revenue) OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS region_top,
                LAST_VALUE(revenue)  OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS region_bottom
            FROM sales
            WHERE month = ?
            ORDER BY region, revenue DESC, rep
            """,
            params=[focus_month],
        ).to_dict()
        for row in fv:
            show(f"{row['rep']} first/last value", row)

        # Check region by region: FIRST_VALUE / LAST_VALUE are identical on every
        # row of a partition and equal the ordered partition's first / last value.
        for region in REGIONS:
            region_rows = client.execute(
                """
                SELECT
                    rep, revenue,
                    FIRST_VALUE(revenue) OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS region_top,
                    LAST_VALUE(revenue)  OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS region_bottom
                FROM sales WHERE month = ? AND region = ?
                ORDER BY revenue DESC
                """,
                params=[focus_month, region],
            ).to_dict()
            tops = {r["region_top"] for r in region_rows}
            bottoms = {r["region_bottom"] for r in region_rows}
            assert len(tops) == 1, f"{region} FIRST_VALUE should be identical on every row, got {tops}"
            assert len(bottoms) == 1, f"{region} LAST_VALUE should be identical on every row, got {bottoms}"
            assert max(r["revenue"] for r in region_rows) == tops.pop()
            assert min(r["revenue"] for r in region_rows) == bottoms.pop()
        print("[OK] FIRST_VALUE = partition maximum (first ordered row); LAST_VALUE = partition minimum (last row of the partition)")
        print("     Note: the default LAST_VALUE frame covers the whole partition, so every row returns the same value")

        section("Step 5: NTILE(4) (split each region into four buckets)")
        # NTILE(n) divides a partition as evenly as possible into n buckets,
        # numbered from 1. When it does not divide evenly, the **leading**
        # buckets take the extra row (here 4 rows / 4 buckets divides exactly).
        ntile_rows = client.execute(
            """
            SELECT
                rep                                             AS rep,
                region                                          AS region,
                revenue                                         AS revenue,
                NTILE(4) OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS quartile
            FROM sales
            WHERE month = ?
            ORDER BY region, quartile, revenue DESC
            """,
            params=[focus_month],
        ).to_dict()
        for row in ntile_rows:
            show(f"{row['region']} Q{row['quartile']}", (row["rep"], row["revenue"]))

        bucket_count = client.execute(
            """
            SELECT bucket, COUNT(*) AS n FROM (
                SELECT NTILE(4) OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS bucket
                FROM sales WHERE month = ?
            ) x
            GROUP BY bucket
            ORDER BY bucket
            """,
            params=[focus_month],
        ).to_dict()
        show("rows per bucket", bucket_count)
        # Semantic assertion: bucket numbers must be exactly 1..4 and their sizes
        # must sum to the partition row count.
        assert [b["bucket"] for b in bucket_count] == [1, 2, 3, 4], (
            f"NTILE(4) should produce exactly buckets 1..4, got {[b['bucket'] for b in bucket_count]}"
        )
        total_in_buckets = sum(b["n"] for b in bucket_count)
        assert total_in_buckets == REPS_PER_REGION * len(REGIONS), (
            f"bucket sizes {total_in_buckets} should sum to the total rows {REPS_PER_REGION * len(REGIONS)}"
        )
        # Buckets must be as even as possible: the largest and smallest differ by
        # at most 1.
        sizes = [b["n"] for b in bucket_count]
        assert max(sizes) - min(sizes) <= 1, f"NTILE bucket sizes should differ by at most 1, got {sizes}"
        print("[OK] NTILE(4) covers buckets 1..4, bucket sizes differ by at most 1, and the total is conserved")

        section("Step 6: aggregate windows (partition total / running total / share / moving average)")
        # These metrics used to be written as "self-join + aggregate + range
        # condition" because aggregate windows did not compose. They are now
        # direct window expressions; only the bounded 3-period moving average
        # still needs a self-join (see 6.4). The partition and ordering keys are
        # (region, seq).

        # --- 6.1 Partition total: SUM() OVER (PARTITION BY region, month) ---
        # Each row carries its region-month total; the window replaces the
        # self-join that summed every row of the same region and month.
        region_total = client.execute(
            """
            SELECT
                rep                                                     AS rep,
                region                                                  AS region,
                month                                                   AS month,
                revenue                                                 AS revenue,
                ROUND(SUM(revenue) OVER (PARTITION BY region, month), 2) AS region_month_total
            FROM sales
            ORDER BY region, rep
            """
        ).to_dict()
        # Print only the focus month to avoid flooding the console.
        for row in region_total:
            if row["month"] == focus_month:
                show(f"{row['region']} / {row['rep']} partition total", row["region_month_total"])

        cross_check = {
            (r["region"], r["month"]): r["total"]
            for r in client.execute(
                """
                SELECT region, month, ROUND(SUM(revenue), 2) AS total
                FROM sales
                GROUP BY region, month
                """
            ).to_dict()
        }
        # Check all 32 rows, but print only one focus-month detail to avoid flooding.
        for row in region_total:
            expected = cross_check[(row["region"], row["month"])]
            if abs(row["region_month_total"] - expected) >= 1e-9:
                raise AssertionError(
                    f"{row['region']} {row['month']} partition total {row['region_month_total']} "
                    f"!= independent GROUP BY result {expected}"
                )
        sample = next(
            r for r in region_total if r["month"] == focus_month and r["region"] == "east"
        )
        assert_close(
            sample["region_month_total"],
            cross_check[("east", focus_month)],
            1e-9,
            "east focus-month partition total (sample detail)",
        )
        print(f"[OK] the window partition total matches the independent GROUP BY on every row ({len(region_total)} rows)")

        # --- 6.2 Running total: explicit ROWS frame vs the default frame ---
        # `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is the physical-row
        # frame; `ORDER BY seq` with no frame uses the default frame, which for a
        # running aggregate is equivalent here. The two columns must agree.
        running = client.execute(
            """
            SELECT
                rep                                     AS rep,
                seq                                     AS seq,
                month                                   AS month,
                revenue                                 AS revenue,
                ROUND(SUM(revenue) OVER (
                    PARTITION BY rep
                    ORDER BY seq
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 2)                                   AS revenue_running,
                ROUND(SUM(revenue) OVER (
                    PARTITION BY rep
                    ORDER BY seq
                ), 2)                                   AS revenue_running_default
            FROM sales
            WHERE rep = ?
            ORDER BY seq
            """,
            params=["E1"],
        ).to_dict()
        for row in running:
            show(f"E1 running total through {row['month']}", row)

        # Semantic assertion 1: the explicit ROWS frame and the default frame
        # produce the same running total.
        assert [r["revenue_running"] for r in running] == [
            r["revenue_running_default"] for r in running
        ], "the ROWS frame and the default frame must give the same running total"
        # Semantic assertion 2: the running total is non-decreasing.
        running_values = [r["revenue_running"] for r in running]
        assert running_values == sorted(running_values), f"running total should be non-decreasing: {running_values}"
        # Semantic assertion 3: the last running value == this salesperson's total.
        total_e1 = client.execute(
            "SELECT ROUND(SUM(revenue), 2) AS t FROM sales WHERE rep = ?", params=["E1"]
        ).scalar()
        assert_close(running_values[-1], total_e1, 1e-9, "E1 running total final value == total revenue")
        # Semantic assertion 4: the first running value == first-period revenue.
        assert_close(running_values[0], running[0]["revenue"], 1e-9, "E1 running total first value == first-period revenue")

        # --- 6.3 Share: revenue / region-month total, both computed as windows ---
        # The denominator is a windowed partition total, so the division is a
        # window inside arithmetic (now supported) with no CTE join.
        share = client.execute(
            """
            SELECT
                rep                                                          AS rep,
                region                                                       AS region,
                month                                                        AS month,
                revenue                                                      AS revenue,
                ROUND(SUM(revenue) OVER (PARTITION BY region, month), 2)     AS region_total,
                ROUND(revenue / SUM(revenue) OVER (PARTITION BY region, month), 4) AS share
            FROM sales
            WHERE month = ?
            ORDER BY region, share DESC
            """,
            params=[focus_month],
        ).to_dict()
        for row in share:
            show(f"{row['region']} / {row['rep']} share", row["share"])

        # Semantic assertion: each region's shares sum to 1 for the month.
        for region in REGIONS:
            region_share = sum(r["share"] for r in share if r["region"] == region)
            assert_close(region_share, 1.0, 1e-3, f"{region} share sum for the month")

        # --- 6.4 3-period moving average: still a self-join ---
        # The natural window form is `AVG(revenue) OVER (PARTITION BY rep
        # ORDER BY seq ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)`, but bounded
        # `n PRECEDING` frames are not reliable in this engine (AVG ignores the
        # frame and returns the whole-partition average; COUNT with such a frame
        # can panic). Only the cumulative `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is
        # dependable, so this metric keeps the self-join that emulates the
        # bounded frame with `b.seq > a.seq - 3` — semantically equivalent.
        moving = client.execute(
            """
            SELECT
                a.rep                              AS rep,
                a.seq                              AS seq,
                a.month                            AS month,
                a.revenue                          AS revenue,
                COUNT(b.seq)                       AS window_size,
                ROUND(AVG(b.revenue), 2)           AS revenue_ma_window
            FROM sales a
            JOIN sales b
              ON b.rep = a.rep
             AND b.seq <= a.seq
             AND b.seq >  a.seq - 3
            WHERE a.rep = ?
            GROUP BY a.rep, a.seq, a.month, a.revenue
            ORDER BY a.seq
            """,
            params=["E1"],
        ).to_dict()
        for row in moving:
            show(f"E1 {row['month']} moving average", row)

        # Semantic assertion 1: the first two windows have 1 and 2 rows (not
        # enough preceding data yet).
        assert [r["window_size"] for r in moving][:3] == [1, 2, 3], (
            f"a 3-period sliding window should have sizes 1,2,3,..., got {[r['window_size'] for r in moving][:3]}"
        )
        # Semantic assertion 2: from the 3rd period on, the window average equals
        # the hand-computed 3-period mean.
        for i in range(2, len(moving)):
            manual = round(
                (moving[i]["revenue"] + moving[i - 1]["revenue"] + moving[i - 2]["revenue"]) / 3.0,
                2,
            )
            assert_close(
                moving[i]["revenue_ma_window"],
                manual,
                0.01,
                f"E1 period {i + 1} 3-period moving average",
            )
        print("[OK] the self-joined 3-period moving average matches hand computation period by period, and the window shrinks at boundaries")

        section("Step 7: four ranking measures side by side in one table")
        # Put every window function from this scenario into a single SELECT so
        # their relationships are visible at a glance.
        combined = client.execute(
            """
            SELECT
                rep                                                          AS rep,
                region                                                       AS region,
                revenue                                                      AS revenue,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS rn,
                RANK()       OVER (PARTITION BY region ORDER BY revenue DESC) AS rnk,
                DENSE_RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS dense_rnk,
                NTILE(4)     OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS quartile,
                FIRST_VALUE(revenue) OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS region_top,
                LAST_VALUE(revenue)  OVER (PARTITION BY region ORDER BY revenue DESC, rep) AS region_bottom
            FROM sales
            WHERE month = ?
            ORDER BY region, rn
            """,
            params=[focus_month],
        ).to_dict()
        for row in combined:
            show(f"{row['region']} / {row['rep']}", row)
        assert len(combined) == REPS_PER_REGION * len(REGIONS), "every salesperson should have one row"
        # Assertion: with the same ordering, NTILE bucket numbers are monotone in
        # the ROW_NUMBER order.
        for region in REGIONS:
            region_rows = [r for r in combined if r["region"] == region]
            assert [r["quartile"] for r in region_rows] == sorted(
                r["quartile"] for r in region_rows
            ), f"{region} NTILE buckets should be non-decreasing with rank"
        print("[OK] nine window expressions coexist in a single SELECT and the results are self-consistent")

        section("Step 8: export to Pandas for the leaderboard")
        import pandas as pd

        board = client.execute(
            """
            SELECT
                rep                                                          AS rep,
                region                                                       AS region,
                revenue                                                      AS revenue,
                ROUND(SUM(revenue) OVER (PARTITION BY region, month), 2)     AS region_total,
                ROUND(revenue / SUM(revenue) OVER (PARTITION BY region, month), 4) AS share,
                RANK() OVER (PARTITION BY region ORDER BY revenue DESC)      AS rnk
            FROM sales
            WHERE month = ?
            ORDER BY region, rnk, rep
            """,
            params=[focus_month],
        ).to_pandas()
        print(board.to_string(index=False))
        show("DataFrame shape", board.shape)
        assert board.shape[0] == REPS_PER_REGION * len(REGIONS)
        assert (board["share"] > 0).all() and (board["share"] <= 1).all()
        for region, group in board.groupby("region"):
            assert_close(
                float(group["share"].sum()),
                1.0,
                1e-3,
                f"{region} leaderboard share sum (Pandas re-check)",
            )

        print(f"\n=== Scenario 09 window functions tour complete ===\nDatabase files: {base}")


if __name__ == "__main__":
    main()
