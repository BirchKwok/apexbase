"""Scenario 16: multi-source federated query on the lake (Parquet + CSV + JSON in one SQL: JOIN + aggregation + window ranking)

Business context
----------------
A real data lake is never "single-format":

* **Fact table**: Parquet landed daily (order line items; columnar, compressed, large);
* **Dimension table**: a CSV maintained by the business team (SKU dimension; small, frequently hand-edited);
* **Event stream**: JSON / NDJSON written by tracking and logs (semi-structured, fields may be added later).

The traditional approach requires ETL into a warehouse before analysis. This example uses ApexBase's
**file table functions** to federate all three formats at query time: one SQL statement performs the
JOIN (fact x dimension x events) + aggregation + window ranking, **with no ETL and no intermediate table**.

ApexBase features demonstrated
------------------------------
1. ``read_parquet()`` / ``read_csv()`` / ``read_json()`` can be mixed inside one ``FROM``/``JOIN``
   graph and placed in CTEs for multi-stage processing.
2. ``UNION ALL`` across formats (measuring the size of all three sources in a single SQL statement).
3. Staged CTE processing: ``sales`` (fact x dimension) -> ``enriched`` (x event stream)
   -> ``agg`` (aggregation) -> ``rank_day`` (ranking window) -> ``ranked`` (bucketing window).
4. Two **independent** federated SQL statements: query A does JOIN + aggregation + ranking/bucketing
   windows; query B does JOIN + aggregation + a ``LAG`` day-over-day comparison. They corroborate
   each other at day x region granularity.
5. Ranking, day-over-day comparison and bucketing with the window functions
   ``ROW_NUMBER`` / ``RANK`` / ``LAG`` / ``NTILE``.
6. Cumulative revenue with an aggregate window,
   ``SUM(revenue) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)``,
   instead of a self-join.

Capability boundaries (this example uses forms known to work)
-------------------------------------------------------------
* Aggregate windows ``SUM()/AVG()/COUNT() OVER (...)`` are supported and compose directly (partition
  totals, percentages, running totals), so the cumulative revenue in step 7 is a single window expression.
* **No** ``DATE_TRUNC`` / ``strftime`` / ``DATE()`` / ``EXTRACT`` support: time buckets are materialised
  as ``YYYY-MM-DD`` strings at write time, then grouped and filtered as strings.
* **No** ``||`` string concatenation; do any concatenation on the Python side.
* Combining certain window functions still hits an implementation limit: ``ROW_NUMBER`` together with
  ``LAG`` / ``NTILE`` in the **same SELECT** reports "Projected column ... does not exist", so this
  example splits them across two CTEs (``rank_day`` / ``ranked``) -- a form verified to work.
* CTEs should be organised as a **linear chain** (each CTE referenced by exactly one downstream).
  When the same CTE is referenced by two branches at once (a DAG shape) on top of file table functions,
  the engine has been observed to intermittently fail with ``failed to open table __cte_xxx.apex``;
  splitting into two independent SQL statements or a linear chain runs reliably.
* Derived tables must have an alias.

How to run
----------
    python py16_lake_multisource_join.py

Output: the size of each of the three sources, the federated query results, the window ranking and the
consistency checks; data lives under ``_out/py_16/``.
"""

from __future__ import annotations

import json
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from apexbase import ApexClient
from _demo_env import rng, section, show, work_dir

SLUG = "16"
N_ORDERS = 4000
DAYS = ["2024-03-01", "2024-03-02", "2024-03-03", "2024-03-04", "2024-03-05"]
REGIONS = ["east", "west", "north", "south"]
CHANNELS = ["web", "app", "store"]
CATEGORIES = ["cpu", "gpu", "ssd", "ram", "nic"]
BRANDS = ["acme", "globex", "initech"]
EVENT_TYPES = ["impression", "click", "cart", "purchase"]


def build_fact_parquet(path: str) -> dict:
    """Fact table: order line items in Parquet. Returns summary values used for cross-validation."""
    rnd = rng(1601)
    skus, regions, days, channels, amounts, quantities = [], [], [], [], [], []
    for i in range(N_ORDERS):
        skus.append(f"SKU{i % 25:03d}")
        regions.append(REGIONS[i % len(REGIONS)])
        days.append(DAYS[(i // 7) % len(DAYS)])
        channels.append(CHANNELS[i % len(CHANNELS)])
        amounts.append(round(rnd.uniform(20.0, 900.0), 2))
        quantities.append(1 + (i % 6))
    table = pa.table(
        {
            "order_id": pa.array(list(range(1, N_ORDERS + 1)), pa.int64()),
            "sku": pa.array(skus, pa.string()),
            "region": pa.array(regions, pa.string()),
            "day": pa.array(days, pa.string()),
            "channel": pa.array(channels, pa.string()),
            "qty": pa.array(quantities, pa.int64()),
            "amount": pa.array(amounts, pa.float64()),
        }
    )
    pq.write_table(table, path)
    return {"rows": N_ORDERS, "amount": round(sum(amounts), 2), "qty": sum(quantities)}


def build_dim_csv(path: str) -> None:
    """Dimension table: SKU -> category / brand / list price, carried as CSV (the business team maintains it by hand)."""
    rows = {
        "sku": [f"SKU{i:03d}" for i in range(25)],
        "category": [CATEGORIES[i % len(CATEGORIES)] for i in range(25)],
        "brand": [BRANDS[i % len(BRANDS)] for i in range(25)],
        "list_price": [round(50.0 + i * 12.5, 2) for i in range(25)],
    }
    pd.DataFrame(rows).to_csv(path, index=False)


def build_events_ndjson(path: str) -> dict:
    """Event stream: one tracking event per order, NDJSON (one JSON object per line)."""
    rnd = rng(1611)
    counts: dict[str, int] = {}
    with open(path, "w", encoding="utf-8") as handle:
        for order_id in range(1, N_ORDERS + 1):
            event_type = EVENT_TYPES[order_id % len(EVENT_TYPES)]
            counts[event_type] = counts.get(event_type, 0) + 1
            handle.write(
                json.dumps(
                    {
                        "order_id": order_id,
                        "event_type": event_type,
                        "device": ["ios", "android", "web"][order_id % 3],
                        "lag_seconds": (order_id * 17) % 900,
                    }
                )
                + "\n"
            )
    return counts


def main() -> None:
    base = work_dir(SLUG)
    fact_path = os.path.join(base, "fact_orders.parquet")
    dim_path = os.path.join(base, "dim_sku.csv")
    event_path = os.path.join(base, "events.ndjson")

    fact_stats = build_fact_parquet(fact_path)
    build_dim_csv(dim_path)
    event_counts = build_events_ndjson(event_path)

    def q(path: str) -> str:
        """Escape a file path for use as a SQL single-quoted literal (an embedded quote must be doubled)."""
        return path.replace("'", "''")


    section("Step 1: three data sources in three formats (queried by path, no import needed)")
    show("fact table Parquet", f"{os.path.basename(fact_path)} ({fact_stats['rows']} rows, columnar)")
    show("dimension table CSV", f"{os.path.basename(dim_path)} (25 rows, hand-maintained)")
    show("event stream NDJSON", f"{os.path.basename(event_path)} ({sum(event_counts.values())} rows, semi-structured)")
    show("event type distribution (pre-generated on the Python side)", event_counts)

    with ApexClient(os.path.join(base, "db")) as client:
        section("Step 2: federate the three formats in one SQL statement (cross-format UNION ALL)")
        # 2.1 Source sizes: count each source separately (a table function may appear on its own in
        # any FROM position).
        inventory = {
            "parquet:fact_orders": client.execute(
                f"SELECT COUNT(*) AS n FROM read_parquet('{q(fact_path)}')"
            ).scalar(),
            "csv:dim_sku": client.execute(f"SELECT COUNT(*) AS n FROM read_csv('{q(dim_path)}')").scalar(),
            "json:events": client.execute(f"SELECT COUNT(*) AS n FROM read_json('{q(event_path)}')").scalar(),
        }
        for name, count in inventory.items():
            show(f"  {name}", count)
        assert inventory["parquet:fact_orders"] == N_ORDERS, "Parquet fact table row count mismatch"
        assert inventory["csv:dim_sku"] == 25, "CSV dimension table row count mismatch"
        assert inventory["json:events"] == N_ORDERS, "JSON event stream row count mismatch"

        # 2.2 A real cross-format UNION ALL: merge the **same-semantics numeric column** of all three
        # formats into one result set. Two observed constraints:
        #   1) the column type must match across branches (the JSON integer must be CAST to DOUBLE
        #      first), otherwise "not possible to concatenate arrays of different data types";
        #   2) a bare string literal column in the SELECT list (e.g. 'parquet' AS src) is dropped by
        #      the projection layer, so the source label is maintained on the Python side rather than
        #      fabricated as a SQL constant column.
        merged_sql = f"""
            SELECT COUNT(*) AS n, ROUND(SUM(v), 2) AS total FROM (
                SELECT amount AS v FROM read_parquet('{q(fact_path)}')
                UNION ALL
                SELECT list_price AS v FROM read_csv('{q(dim_path)}')
                UNION ALL
                SELECT CAST(lag_seconds AS DOUBLE) AS v FROM read_json('{q(event_path)}')
            ) AS merged
        """
        merged = client.execute(merged_sql).to_dict()[0]
        expected_n = inventory["parquet:fact_orders"] + inventory["csv:dim_sku"] + inventory["json:events"]
        show("cross-format UNION ALL: merged rows / numeric total", f"{merged['n']} / {merged['total']}")
        assert merged["n"] == expected_n, f"cross-format merged row count {merged['n']} should be {expected_n}"
        print(f"[OK] Parquet + CSV + NDJSON UNION ALL succeeded inside one SQL statement (merged {merged['n']} rows)")

        section("Step 3: federated query A -- JOIN (fact x dimension x events) + aggregation + ranking windows")
        # Staged CTEs (**each CTE consumed by exactly one downstream**, forming a linear chain):
        #   sales    fact table JOIN dimension table (adds category/brand/list price)
        #   enriched then JOINs the event stream (adds tracking info)
        #   agg      aggregates by day/region/category
        #   rank_day first window group: ranking (ROW_NUMBER / RANK)
        #   ranked   second window group: bucketing (NTILE)
        # Note: writing ROW_NUMBER together with LAG/NTILE in the same SELECT reports
        # "Projected column ... does not exist", hence the split across two CTEs.
        # Keeping the CTEs as a chain (instead of letting one CTE be referenced by two branches)
        # also avoids the engine's "failed to open table __cte_xxx.apex" while materialising temp tables.
        federated = client.execute(
            f"""
            WITH sales AS (
                SELECT
                    f.order_id, f.sku, f.region, f.day, f.channel,
                    f.qty, f.amount, d.category, d.brand, d.list_price
                FROM read_parquet('{q(fact_path)}') f
                JOIN read_csv('{q(dim_path)}') d ON f.sku = d.sku
            ),
            enriched AS (
                SELECT
                    s.order_id, s.region, s.day, s.category, s.brand,
                    s.qty, s.amount, e.event_type, e.device, e.lag_seconds
                FROM sales s
                JOIN read_json('{q(event_path)}') e ON s.order_id = e.order_id
            ),
            agg AS (
                SELECT
                    day, region, category,
                    COUNT(*)         AS orders,
                    SUM(amount)      AS revenue,
                    SUM(qty)         AS units,
                    AVG(lag_seconds) AS avg_lag
                FROM enriched
                GROUP BY day, region, category
            ),
            rank_day AS (
                SELECT
                    day, region, category, orders, revenue, units, avg_lag,
                    ROW_NUMBER() OVER (PARTITION BY day ORDER BY revenue DESC)         AS rn_day,
                    RANK()       OVER (PARTITION BY day, region ORDER BY revenue DESC) AS rn_region
                FROM agg
            ),
            ranked AS (
                SELECT
                    day, region, category, orders, revenue, units, avg_lag, rn_day, rn_region,
                    NTILE(4) OVER (ORDER BY revenue DESC) AS revenue_quartile
                FROM rank_day
            )
            SELECT
                day, region, category,
                orders,
                ROUND(revenue, 2) AS revenue,
                units,
                ROUND(avg_lag, 2) AS avg_lag,
                rn_day,
                rn_region,
                revenue_quartile
            FROM ranked
            ORDER BY day, rn_day
            """
        ).to_dict()
        show("federated query result rows (day x region x category)", len(federated))
        for row in federated[:5]:
            show("  ", row)

        section("Step 4: federated query B -- regional daily revenue and day-over-day change (LAG window)")
        # Second federated SQL: the same three-source JOIN, applying LAG at (day, region) granularity.
        # Still a single SQL statement, and still a linear CTE chain.
        trend = client.execute(
            f"""
            WITH sales AS (
                SELECT f.order_id, f.region, f.day, f.amount
                FROM read_parquet('{q(fact_path)}') f
                JOIN read_csv('{q(dim_path)}') d ON f.sku = d.sku
            ),
            enriched AS (
                SELECT s.region, s.day, s.amount, e.event_type
                FROM sales s JOIN read_json('{q(event_path)}') e ON s.order_id = e.order_id
            ),
            daily AS (
                SELECT day, region, SUM(amount) AS region_revenue
                FROM enriched
                GROUP BY day, region
            ),
            with_prev AS (
                SELECT
                    day, region, region_revenue,
                    LAG(region_revenue) OVER (PARTITION BY region ORDER BY day) AS prev_region_revenue
                FROM daily
            )
            SELECT
                day, region,
                ROUND(region_revenue, 2)                              AS region_revenue,
                ROUND(prev_region_revenue, 2)                         AS prev_region_revenue,
                ROUND(region_revenue - prev_region_revenue, 2)        AS region_day_over_day
            FROM with_prev
            ORDER BY region, day
            """
        ).to_dict()
        show("regional daily series rows (region x day)", len(trend))
        for row in trend[:6]:
            show("  ", row)

        section("Step 5: result consistency checks (federated results vs an independent pandas recomputation)")
        # Recompute independently with pandas: the dimension JOIN is 1:1 (sku is unique) and the event
        # JOIN is 1:1 as well (order_id is unique), so the federated query cannot inflate rows via JOINs.
        fact_df = pq.read_table(fact_path).to_pandas()
        dim_df = pd.read_csv(dim_path)
        events_df = pd.read_json(event_path, lines=True)
        merged = fact_df.merge(dim_df, on="sku", how="inner").merge(events_df, on="order_id", how="inner")
        expected_rows = len(merged)
        expected_revenue = round(float(merged["amount"].sum()), 2)
        expected_units = int(merged["qty"].sum())
        show("pandas recomputation: rows / amount / units", f"{expected_rows} / {expected_revenue} / {expected_units}")
        assert expected_rows == N_ORDERS, "the JOIN inflated the row count (duplicate keys in the dimension table or the event stream)"

        totals = client.execute(
            f"""
            WITH sales AS (
                SELECT f.order_id, f.qty, f.amount, d.category
                FROM read_parquet('{q(fact_path)}') f
                JOIN read_csv('{q(dim_path)}') d ON f.sku = d.sku
            )
            SELECT COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue, SUM(qty) AS units
            FROM sales s JOIN read_json('{q(event_path)}') e ON s.order_id = e.order_id
            """
        ).to_dict()[0]
        show("federated chain recomputation: rows / amount / units", f"{totals['orders']} / {totals['revenue']} / {totals['units']}")
        assert totals["orders"] == expected_rows == fact_stats["rows"], "federated chain row count does not match the fact table"
        assert totals["revenue"] == expected_revenue == fact_stats["amount"], "federated chain amount does not match the pandas recomputation"
        assert totals["units"] == expected_units == fact_stats["qty"], "federated chain unit count does not match the pandas recomputation"
        print("[OK] the three-source federated JOIN agrees with both the independent pandas recomputation and the raw Parquet summary on rows/amount/units")

        # The grouped summary of the ranking query must agree with pandas too (grouped by day).
        sql_by_day = {
            row["day"]: row
            for row in client.execute(
                f"""
                WITH sales AS (
                    SELECT f.order_id, f.day, f.amount FROM read_parquet('{q(fact_path)}') f
                    JOIN read_csv('{q(dim_path)}') d ON f.sku = d.sku
                )
                SELECT day, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
                FROM sales GROUP BY day ORDER BY day
                """
            ).to_dict()
        }
        pandas_by_day = (
            merged.groupby("day", as_index=False)
            .agg(orders=("order_id", "count"), revenue=("amount", lambda s: round(float(s.sum()), 2)))
            .to_dict("records")
        )
        for row in pandas_by_day:
            got = sql_by_day[row["day"]]
            assert got["orders"] == row["orders"], f"{row['day']} order count mismatch"
            assert abs(got["revenue"] - row["revenue"]) < 0.01, f"{row['day']} amount mismatch"
        show("days compared", len(pandas_by_day))
        print("[OK] the per-day aggregation matches the pandas groupby day by day")

        # The per-day summary of the ranking query must equal the sum over all (region, category) rows of that day.
        for day, expected in sql_by_day.items():
            rows = [r for r in federated if r["day"] == day]
            assert sum(r["orders"] for r in rows) == expected["orders"], f"{day} order count differs between the two granularities"
            assert abs(sum(r["revenue"] for r in rows) - expected["revenue"]) < 0.01, f"{day} amount mismatch"
        print("[OK] rolling (day x region x category) results up to day granularity matches the (day) granularity query exactly")

        section("Step 6: property checks on the window ranking")
        by_day: dict[str, list[dict]] = {}
        for row in federated:
            by_day.setdefault(row["day"], []).append(row)

        # Every day must have exactly one rn_day == 1 (the global top rank).
        for day, rows in by_day.items():
            top = [r for r in rows if r["rn_day"] == 1]
            assert len(top) == 1, f"{day} should have exactly 1 row with rn_day==1, got {len(top)}"
            rns = sorted(r["rn_day"] for r in rows)
            assert rns == list(range(1, len(rows) + 1)), f"{day} rn_day is not contiguous: {rns}"
        print(f"[OK] every day ({len(by_day)} days) has contiguous rn_day starting at 1, and the global top rank is unique")

        # rn_region is contiguous within each (day, region); NTILE stays inside 1..4.
        for day, rows in by_day.items():
            by_region: dict[str, list[dict]] = {}
            for row in rows:
                by_region.setdefault(row["region"], []).append(row)
            for region, sub in by_region.items():
                rrs = sorted(r["rn_region"] for r in sub)
                assert rrs == list(range(1, len(sub) + 1)), f"{day}/{region} rn_region is not contiguous: {rrs}"
        all_quartiles = {row["revenue_quartile"] for row in federated}
        assert all_quartiles <= {1, 2, 3, 4}, f"NTILE bucket out of range: {all_quartiles}"
        print(f"[OK] partition rank rn_region is contiguous within each (day, region); NTILE bucket set = {sorted(all_quartiles)}")

        # LAG: the first day of each region has a NULL day-over-day value, and every later day must have one.
        first_days = 0
        for region in REGIONS:
            region_rows = [r for r in trend if r["region"] == region]
            assert len(region_rows) >= 2, f"{region} has too few days to verify LAG"
            for idx, row in enumerate(region_rows):
                if idx == 0:
                    assert row["region_day_over_day"] is None, (
                        f"{region}/{row['day']} first-day day-over-day value should be NULL"
                    )
                    first_days += 1
                else:
                    assert row["region_day_over_day"] is not None, (
                        f"{region}/{row['day']} day-over-day value should not be NULL"
                    )
                    # Day-over-day = today - yesterday, and it must reconcile.
                    prev = region_rows[idx - 1]["region_revenue"]
                    assert abs((row["region_revenue"] - prev) - row["region_day_over_day"]) < 0.01, (
                        f"{region}/{row['day']} day-over-day value does not reconcile"
                    )
        print(f"[OK] LAG works: the first day of each of the {len(REGIONS)} regions has a NULL day-over-day value ({first_days} in total), and every other day reconciles")

        # Regional daily revenue must equal the sum of revenue over categories for that (day, region).
        checked = 0
        trend_map = {(r["day"], r["region"]): r for r in trend}
        for (day, region), row in trend_map.items():
            rows = [r for r in federated if r["day"] == day and r["region"] == region]
            assert rows, f"{day}/{region} is missing from the ranking result"
            total = round(sum(r["revenue"] for r in rows), 2)
            assert abs(total - row["region_revenue"]) < 0.01, (
                f"{day}/{region} regional revenue {row['region_revenue']} != sum over categories {total}"
            )
            checked += 1
        print(f"[OK] the two federated queries corroborate each other at day x region granularity ({checked} combinations)")

        section("Step 7: cumulative revenue with an aggregate window (SUM() OVER (...))")
        # Aggregate windows are supported, so a running total is one SUM() OVER (...) expression:
        # order the days and accumulate from the first row up to the current row. This replaces the
        # self-join ("connect each day to every day <= it, then aggregate") that used to be required.
        daily = client.execute(
            f"""
            WITH sales AS (
                SELECT f.day, f.amount FROM read_parquet('{q(fact_path)}') f
                JOIN read_csv('{q(dim_path)}') d ON f.sku = d.sku
            ),
            d AS (SELECT day, ROUND(SUM(amount), 2) AS revenue FROM sales GROUP BY day)
            SELECT
                day,
                revenue,
                ROUND(SUM(revenue) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS revenue_ytd
            FROM d
            ORDER BY day
            """
        ).to_dict()
        for row in daily:
            show("  ", row)
        # The last cumulative row must equal the grand total, and the cumulative series must be monotonically non-decreasing.
        assert abs(daily[-1]["revenue_ytd"] - expected_revenue) < 0.01, (
            f"last cumulative revenue row {daily[-1]['revenue_ytd']} should equal the total amount {expected_revenue}"
        )
        ytd = [row["revenue_ytd"] for row in daily]
        assert ytd == sorted(ytd), "cumulative revenue must be monotonically non-decreasing"
        print(f"[OK] cumulative-window last row {daily[-1]['revenue_ytd']} == total amount {expected_revenue}, and it is monotonically non-decreasing")

        section("Step 8: the federated queries created no intermediate table (evidence of being ETL-free)")
        show("persistent table list", client.list_tables())
        assert client.list_tables() == [], "federated queries should not create any persistent table"
        print("[OK] only files (Parquet / CSV / JSON) were read throughout; nothing was landed by ETL")

    print(
        f"\n=== Scenario 16: multi-source federated query on the lake complete ===\n"
        f"Key takeaway: one SQL statement performs the JOIN + aggregation + window ranking over a Parquet fact table,\n"
        f"         a CSV dimension table and an NDJSON event stream in a single pass -- lake data in any shape needs no ETL.\n"
        f"Data lives in: {base}"
    )


if __name__ == "__main__":
    main()
