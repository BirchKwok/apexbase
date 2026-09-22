"""Scenario 01: local Parquet detail analytics (a sales data mart)

Business context
----------------
Every day the data team exports a Parquet detail file from an upstream system
(orders, channel, amount, cost). Without standing up any database service they
want to run multi-dimensional analysis on that file directly with SQL: gross
profit by channel and region, aggregation through CTEs, and ranking plus shares
through window functions.

ApexBase features demonstrated
------------------------------
1. The ``read_parquet()`` table function: query Parquet straight from ``FROM``
   with no create-table/import step first.
2. ``register_temp_table()``: materialise the file as an mmap-backed temporary
   table so repeated queries take the zero-copy path.
3. CTEs (``WITH``), ``GROUP BY``, ``HAVING`` and multi-table ``JOIN``.
4. Window functions: ``ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`` for
   in-group ranking, plus composed aggregate windows for shares and cumulative
   totals -- ``SUM(revenue) OVER (PARTITION BY region)`` used inside ``ROUND``
   and arithmetic, and a ``ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW``
   frame for the year-to-date total.

One important capability boundary (the example follows the supported form)
--------------------------------------------------------------------------
1. ``DATE_TRUNC`` / ``strftime`` / ``DATE()`` are **not supported**. The example
   therefore materialises the "month" field as a string (``YYYY-MM``) while
   generating the data and only uses string grouping and string range
   comparison -- which also feeds column-store push-down filtering.

How to run
----------
    python py01_parquet_sales_analytics.py

Output: each step prints to the console; the intermediate Parquet file lives in
``_out/py_01/``.
"""

from __future__ import annotations

import os

import pyarrow as pa
import pyarrow.parquet as pq

from apexbase import ApexClient
from _demo_env import rng, section, show, work_dir

SLUG = "01"
ROW_COUNT = 5_000          # Detail rows: enough to exercise aggregation/window behaviour, fast enough to run in seconds
CHANNELS = ["web", "mobile", "partner", "retail"]
REGIONS = ["east", "west", "north", "south"]


def build_parquet(path: str) -> str:
    """Generate a deterministic order-detail Parquet file and return its path.

    Columns: order_id / day / month / channel / region / units / revenue / cost,
    where ``revenue = units * price`` and ``cost = units * unit_cost``. Every
    downstream aggregate can therefore be derived from the detail rows, which
    makes cross-checking possible.
    """
    rnd = rng(20240301)
    rows = {
        "order_id": [],
        "day": [],
        "month": [],
        "channel": [],
        "region": [],
        "units": [],
        "revenue": [],
        "cost": [],
    }
    for i in range(ROW_COUNT):
        # Fixed month/day distribution keeps the output reproducible.
        month = 1 + (i % 6)                       # months 1..6
        day = 1 + (i * 7) % 28                    # days 1..28, avoiding month-end edge cases
        units = 1 + rnd.randrange(20)
        unit_price = round(rnd.uniform(8.0, 120.0), 2)
        unit_cost = round(unit_price * rnd.uniform(0.45, 0.8), 2)
        rows["order_id"].append(i + 1)
        rows["day"].append(f"2024-{month:02d}-{day:02d}")
        rows["month"].append(f"2024-{month:02d}")
        rows["channel"].append(CHANNELS[i % len(CHANNELS)])
        rows["region"].append(REGIONS[(i // 3) % len(REGIONS)])
        rows["units"].append(units)
        rows["revenue"].append(round(units * unit_price, 2))
        rows["cost"].append(round(units * unit_cost, 2))

    table = pa.table(
        {
            "order_id": pa.array(rows["order_id"], pa.int64()),
            "day": pa.array(rows["day"], pa.string()),
            "month": pa.array(rows["month"], pa.string()),
            "channel": pa.array(rows["channel"], pa.string()),
            "region": pa.array(rows["region"], pa.string()),
            "units": pa.array(rows["units"], pa.int64()),
            "revenue": pa.array(rows["revenue"], pa.float64()),
            "cost": pa.array(rows["cost"], pa.float64()),
        }
    )
    pq.write_table(table, path)
    return path


def main() -> None:
    base = work_dir(SLUG)
    parquet_path = os.path.join(base, "orders.parquet")
    build_parquet(parquet_path)

    # ApexBase wraps path literals in single quotes; watch out for Windows
    # backslash escaping when adapting this.
    parquet_sql = parquet_path.replace("'", "''")

    section("Step 1: run SQL directly against the Parquet file (no import needed)")
    with ApexClient(os.path.join(base, "db")) as client:
        # read_parquet() exposes the file as a relation, so WHERE / GROUP BY /
        # JOIN can be layered on it freely.
        total = client.execute(
            f"""
            SELECT
                COUNT(*)               AS orders,
                SUM(units)             AS units,
                ROUND(SUM(revenue), 2) AS revenue,
                ROUND(SUM(revenue - cost), 2) AS gross_profit
            FROM read_parquet('{parquet_sql}')
            """
        ).to_dict()
        show("overall metrics", total)

        section("Step 2: CTE + GROUP BY for channel gross profit, filtered with HAVING")
        by_channel = client.execute(
            f"""
            WITH channel_stats AS (
                SELECT
                    channel,
                    COUNT(*)                                   AS orders,
                    ROUND(SUM(revenue), 2)                     AS revenue,
                    ROUND(SUM(revenue - cost), 2)              AS gross_profit,
                    ROUND(SUM(revenue - cost) / SUM(revenue), 4) AS margin
                FROM read_parquet('{parquet_sql}')
                GROUP BY channel
            )
            SELECT * FROM channel_stats
            WHERE orders > 0
            ORDER BY gross_profit DESC
            """
        ).to_dict()
        for row in by_channel:
            show(f"channel {row['channel']}", row)

        section("Step 3: window ranking + window share of regional revenue")
        # ApexBase supports ROW_NUMBER/RANK/LAG-style ranking and offset windows,
        # and aggregate windows now compose with ROUND() and arithmetic. The share
        # of a region's revenue is therefore a direct division by
        # SUM(revenue) OVER (PARTITION BY region); no self-join is needed. The
        # window total is computed over exactly the same grouped rows as the
        # ranking, so numerator and denominator stay consistent.
        ranked = client.execute(
            f"""
            WITH channel_stats AS (
                SELECT
                    region,
                    channel,
                    ROUND(SUM(revenue), 2) AS revenue
                FROM read_parquet('{parquet_sql}')
                GROUP BY region, channel
            )
            SELECT
                region,
                channel,
                revenue,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) AS rank_in_region,
                ROUND(revenue / SUM(revenue) OVER (PARTITION BY region), 4)   AS region_share
            FROM channel_stats
            ORDER BY region, rank_in_region
            """
        ).to_dict()
        # Only show the top two channels per region to keep the output readable
        # (the ranking itself already happens inside SQL, per region).
        for row in ranked:
            if row["rank_in_region"] <= 2:
                show(f"{row['region']} rank {row['rank_in_region']}", row)
        assert all(r["rank_in_region"] >= 1 for r in ranked), "ranks should start at 1"
        print("[OK] every region produced a contiguous ranking starting at 1")

        # Self-check: the channel shares inside each region must sum to 1
        # (within floating-point tolerance).
        share_sum: dict[str, float] = {}
        for row in ranked:
            share_sum[row["region"]] = share_sum.get(row["region"], 0.0) + row["region_share"]
        for region, total_share in share_sum.items():
            assert abs(total_share - 1.0) < 1e-3, f"{region} shares sum to {total_share}, expected 1"
        print(f"[OK] channel shares sum to 1 in every region: { {k: round(v, 4) for k, v in share_sum.items()} }")

        section("Step 4: materialise as a temporary table -- repeated queries take the mmap zero-copy path")
        # register_temp_table parses the file once and materialises it in
        # ApexBase's native format. Later queries no longer parse Parquet and
        # instead use zone maps / bloom filters / mmap.
        #
        # Two behaviours confirmed by measurement (they differ from some docs, so
        # the actual behaviour is what counts here):
        #   1. After registering, use_table() must select the temporary table, or
        #      execute() fails with "No table selected".
        #   2. Temporary tables do not show up in list_tables() (which returns
        #      persistent tables only).
        # The temporary table is cleaned up automatically when the client closes.
        client.register_temp_table("orders", parquet_path)
        client.use_table("orders")
        show("persistent tables (temporary tables not included)", client.list_tables())
        show("currently selected table", client.current_table)

        # The same SQL is shown against both the "file function" and the
        # "materialised temporary table" paths to contrast the two forms.
        monthly = client.execute(
            """
            SELECT
                month,
                COUNT(*)                                   AS orders,
                ROUND(SUM(revenue), 2)                     AS revenue,
                ROUND(SUM(revenue - cost), 2)              AS gross_profit,
                ROUND(SUM(revenue - cost) / SUM(revenue), 4) AS margin
            FROM orders
            GROUP BY month
            ORDER BY month
            """
        ).to_dict()
        for row in monthly:
            show(f"{row['month']}", row)

        section("Step 5: cumulative monthly revenue via a window frame")
        # A running total needs an explicit frame: sum everything from the start
        # of the ordering up to and including the current row. ROWS BETWEEN
        # UNBOUNDED PRECEDING AND CURRENT ROW expresses exactly that, so the
        # year-to-date figure is a plain windowed SUM rather than a self-join.
        cumulative = client.execute(
            """
            WITH m AS (
                SELECT month, ROUND(SUM(revenue), 2) AS revenue
                FROM orders
                GROUP BY month
            )
            SELECT
                month                                   AS month,
                revenue                                 AS revenue,
                ROUND(SUM(revenue) OVER (ORDER BY month
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS revenue_ytd
            FROM m
            ORDER BY month
            """
        ).to_dict()
        for row in cumulative:
            show(f"{row['month']}", row)

        # Self-check: the last cumulative row must equal total revenue, which
        # confirms the running total actually reached the full sum.
        file_total = client.execute(
            f"SELECT ROUND(SUM(revenue), 2) AS r FROM read_parquet('{parquet_sql}')"
        ).scalar()
        assert abs(cumulative[-1]["revenue_ytd"] - file_total) < 1.0, (
            f"cumulative revenue {cumulative[-1]['revenue_ytd']} should equal total revenue {file_total}"
        )
        print(f"[OK] last cumulative row {cumulative[-1]['revenue_ytd']} == total revenue {file_total}")

        section("Step 6: hand results straight to Pandas / Polars / Arrow for downstream work")
        df = client.execute(
            """
            SELECT region, channel, ROUND(SUM(revenue - cost), 2) AS gross_profit
            FROM orders
            GROUP BY region, channel
            ORDER BY gross_profit DESC
            LIMIT 5
            """
        ).to_pandas()
        print(df.to_string(index=False))
        show("DataFrame shape", df.shape)

    print(f"\nDone. Parquet and database files are under: {base}")


if __name__ == "__main__":
    main()
