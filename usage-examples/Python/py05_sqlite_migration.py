"""Scenario 05: SQLite -> ApexBase migration reconciliation

Business context
----------------
An internal reporting system has been running for three years on a single-file
SQLite database. As the detail volume grew, SQLite became strained by "large-table
aggregation + columnar scans + eventually vector search", so the team decided to
move the analytics workload to ApexBase. The scary part of a migration is not
moving the data, though -- it is that **the numbers no longer add up afterwards**.

So the point of this example is not "how to read rows out", it is
**migration reconciliation**:

1. build a deterministic source dataset with the standard-library ``sqlite3``
   (no network, no downloads);
2. import the same data into ApexBase through **two independent paths**:
   - path A: ``client.store(<columnar dict>)`` -- assemble by column, no intermediate objects;
   - path B: ``client.from_pandas(df)`` -- go through the DataFrame channel;
3. run the same set of analytics queries on both SQLite and ApexBase and **compare
   row by row**;
4. if ``duckdb`` is available in the environment, bring it in for third-party
   cross-validation (skip gracefully if not).

All three paths have to produce the same numbers before the migration counts as
successful -- that is what "reconciliation" means here.

ApexBase features demonstrated
------------------------------
1. **Columnar writes**: ``client.store({"col": [...], ...})`` takes a columnar
   dict directly, avoiding the cost of building a dict per row -- the preferred
   shape for bulk migration.
2. ``client.from_pandas(df)``: the DataFrame import channel.
3. ``client.count_rows()`` and SQL aggregation as two row-count sources that
   corroborate each other.
4. Cross-engine result comparison: normalise ApexBase's ``.to_dict()`` and the
   ``sqlite3`` cursor rows into the same structure and compare field by field
   (tolerance for numbers, equality for strings).

Capability boundaries (this example follows the supported forms)
----------------------------------------------------------------
1. ApexBase does **not support** ``DATE_TRUNC`` / ``strftime`` / ``DATE()``: the
   source table's ``month`` is already stored as a ``YYYY-MM`` string on the
   SQLite side, so it can be grouped directly after migration.
2. ``IN`` requires a column on the left-hand side; a function expression is not allowed.
3. Floating-point summation order may differ between SQLite and ApexBase, so all
   numeric comparisons carry a tolerance and both sides apply ``ROUND(x, 2)`` in
   SQL first, pushing the difference well below the tolerance.
4. DuckDB is an **optional** dependency: if ``import duckdb`` fails, the
   third-party check is skipped with an explanatory message and the ApexBase
   reconciliation result is unaffected.

How to run
----------
    python py05_sqlite_migration.py

Output: each step prints to the console; the source database and ApexBase
database live under ``_out/py_05/``.
"""

from __future__ import annotations

import os
import sqlite3

from apexbase import ApexClient
from _demo_env import assert_close, rng, section, show, work_dir

SLUG = "05"

ROW_COUNT = 3_000
CHANNELS = ["web", "mobile", "partner", "retail"]
REGIONS = ["east", "west", "north", "south"]

# The analytics query set to reconcile. Each one uses the SQL subset that both
# engines can run -- only SUM / COUNT / ROUND / GROUP BY / ORDER BY / LIMIT.
# That is the precondition for automating reconciliation: the queries must be
# engine-agnostic.
ANALYTICS_QUERIES: list[tuple[str, str]] = [
    (
        "Q1 overall metrics",
        """
        SELECT
            COUNT(*)                      AS orders,
            SUM(units)                    AS units,
            ROUND(SUM(revenue), 2)        AS revenue,
            ROUND(SUM(cost), 2)           AS cost,
            ROUND(SUM(revenue - cost), 2) AS gross_profit
        FROM orders
        """,
    ),
    (
        "Q2 roll-up by channel",
        """
        SELECT
            channel                       AS channel,
            COUNT(*)                      AS orders,
            ROUND(SUM(revenue), 2)        AS revenue,
            ROUND(SUM(revenue - cost), 2) AS gross_profit
        FROM orders
        GROUP BY channel
        ORDER BY channel
        """,
    ),
    (
        "Q3 roll-up by month",
        """
        SELECT
            month                            AS month,
            COUNT(*)                         AS orders,
            ROUND(SUM(revenue), 2)           AS revenue,
            ROUND(SUM(revenue) / COUNT(*), 2) AS revenue_per_order
        FROM orders
        GROUP BY month
        ORDER BY month
        """,
    ),
    (
        "Q4 region x channel gross profit Top 5",
        """
        SELECT
            region                        AS region,
            channel                       AS channel,
            ROUND(SUM(revenue - cost), 2) AS gross_profit
        FROM orders
        GROUP BY region, channel
        -- The tie-breaker is required: three-way comparison needs identical row
        -- order, and when sorting only by gross profit the order of tied rows is
        -- up to each engine, which would make reconciliation fail at random.
        ORDER BY gross_profit DESC, region, channel
        LIMIT 5
        """,
    ),
    (
        "Q5 large-order distribution (HAVING filter)",
        """
        SELECT
            channel                 AS channel,
            COUNT(*)                AS big_orders,
            ROUND(MAX(revenue), 2)  AS max_revenue
        FROM orders
        WHERE revenue >= 500
        GROUP BY channel
        HAVING COUNT(*) >= 1
        ORDER BY channel
        """,
    ),
]


def build_sqlite_source(path: str) -> None:
    """Build a deterministic source dataset with the standard-library sqlite3.

    The source schema matches the ApexBase side: ``month`` is already materialised
    as a ``YYYY-MM`` string so no date function appears in the queries (ApexBase
    does not support them, SQLite does -- using one would break the shared,
    engine-agnostic query set).
    """
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                day      TEXT    NOT NULL,
                month    TEXT    NOT NULL,
                channel  TEXT    NOT NULL,
                region   TEXT    NOT NULL,
                units    INTEGER NOT NULL,
                revenue  REAL    NOT NULL,
                cost     REAL    NOT NULL
            )
            """
        )
        rnd = rng(20240501)
        rows = []
        for i in range(ROW_COUNT):
            month = 1 + (i % 6)
            day = 1 + (i * 7) % 28
            units = 1 + rnd.randrange(20)
            unit_price = round(rnd.uniform(8.0, 120.0), 2)
            unit_cost = round(unit_price * rnd.uniform(0.45, 0.8), 2)
            rows.append(
                (
                    i + 1,
                    f"2024-{month:02d}-{day:02d}",
                    f"2024-{month:02d}",
                    CHANNELS[i % len(CHANNELS)],
                    REGIONS[(i // 3) % len(REGIONS)],
                    units,
                    round(units * unit_price, 2),
                    round(units * unit_cost, 2),
                )
            )
        conn.executemany(
            "INSERT INTO orders (order_id, day, month, channel, region, units, revenue, cost)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def sqlite_query(path: str, sql: str) -> list[dict]:
    """Run a query on SQLite and normalise the result to ``List[dict]``.

    Why normalise: ApexBase returns ``List[dict]`` while SQLite returns a sequence
    of tuples. Only by turning both into the same structure can a single generic
    field-by-field comparator be written.
    """
    conn = sqlite3.connect(path)
    try:
        cur = conn.execute(sql)
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def rows_match(
    label: str,
    left: list[dict],
    right: list[dict],
    tol: float = 1e-6,
) -> dict:
    """Compare two result sets row by row and field by field, returning a printable summary.

    The comparison deliberately distinguishes types:
    * numbers use ``abs(a - b) <= tol`` (different summation orders cause last-digit drift);
    * strings/None use strict equality;
    * row counts and the column-name sets must match first, otherwise the result
      is immediately judged inconsistent.
    """
    if len(left) != len(right):
        return {
            "query": label,
            "ok": False,
            "reason": f"row count mismatch: {len(left)} vs {len(right)}",
            "max_diff": None,
        }
    if set(left[0].keys()) != set(right[0].keys()) if left else False:
        return {
            "query": label,
            "ok": False,
            "reason": f"column names mismatch: {sorted(left[0])} vs {sorted(right[0])}",
            "max_diff": None,
        }

    max_diff = 0.0
    for i, (lrow, rrow) in enumerate(zip(left, right)):
        for col in lrow:
            a, b = lrow[col], rrow[col]
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                diff = abs(float(a) - float(b))
                max_diff = max(max_diff, diff)
                if diff > tol:
                    return {
                        "query": label,
                        "ok": False,
                        "reason": f"row {i} field {col} differs by {diff} > {tol} ({a} vs {b})",
                        "max_diff": max_diff,
                    }
            elif a != b:
                return {
                    "query": label,
                    "ok": False,
                    "reason": f"row {i} field {col} not equal: {a!r} vs {b!r}",
                    "max_diff": max_diff,
                }
    return {"query": label, "ok": True, "reason": "consistent", "max_diff": max_diff}


def main() -> None:
    base = work_dir(SLUG)
    sqlite_path = os.path.join(base, "source.db")

    section("Step 1: build source data with the standard-library sqlite3")
    build_sqlite_source(sqlite_path)
    show("source database file", sqlite_path)
    show("source row count", sqlite_query(sqlite_path, "SELECT COUNT(*) AS n FROM orders")[0]["n"])
    show("sample source rows", sqlite_query(sqlite_path, "SELECT * FROM orders ORDER BY order_id LIMIT 2"))

    with ApexClient(os.path.join(base, "db")) as client:
        section("Step 2A: migration path A -- columnar store (the recommended bulk channel)")
        # A columnar write gathers "all values of one column" into a single list,
        # avoiding 3000 dict constructions. It is the import path that uses the
        # least memory and the fewest allocations on a columnar engine.
        conn = sqlite3.connect(sqlite_path)
        try:
            cur = conn.execute(
                "SELECT order_id, day, month, channel, region, units, revenue, cost"
                " FROM orders ORDER BY order_id"
            )
            columns = [d[0] for d in cur.description]
            columnar: dict[str, list] = {name: [] for name in columns}
            for row in cur:
                for name, value in zip(columns, row):
                    columnar[name].append(value)
        finally:
            conn.close()

        client.create_table(
            "orders",
            {
                "order_id": "int64",
                "day": "string",
                "month": "string",
                "channel": "string",
                "region": "string",
                "units": "int64",
                "revenue": "float64",
                "cost": "float64",
            },
        )
        client.store(columnar)
        show("columnar import row count", client.count_rows())
        assert client.count_rows() == ROW_COUNT, "the columnar import should write every source row"

        section("Step 2B: migration path B -- from_pandas")
        # The second path deliberately goes through the DataFrame channel: different
        # implementations may treat type inference differently, so verifying both
        # confirms that "the migration result is independent of the channel".
        import pandas as pd  # local import: only this section needs pandas

        df = pd.read_sql_query("SELECT * FROM orders ORDER BY order_id", sqlite3.connect(sqlite_path))
        show("pandas DataFrame shape", df.shape)
        show("pandas dtypes", {k: str(v) for k, v in df.dtypes.items()})
        client.from_pandas(df, table_name="orders_pandas")
        show("DataFrame import row count", client.count_rows("orders_pandas"))

        section("Step 3: compare the two ApexBase channels (migration self-consistency)")
        # First confirm the columnar and DataFrame channels landed identical content,
        # then compare against SQLite. The order matters: locating the problem inside
        # ApexBase versus across engines first cuts debugging time substantially.
        checksum_sql = """
            SELECT
                COUNT(*)               AS n,
                SUM(units)             AS units,
                ROUND(SUM(revenue), 2) AS revenue,
                ROUND(SUM(cost), 2)    AS cost,
                ROUND(SUM(revenue * order_id), 2) AS weighted
            FROM orders
        """
        client.use_table("orders")
        checksum_store = client.execute(checksum_sql).to_dict()
        client.use_table("orders_pandas")
        checksum_pandas = client.execute(checksum_sql).to_dict()
        show("columnar channel checksum", checksum_store)
        show("DataFrame channel checksum", checksum_pandas)
        assert rows_match("columnar vs from_pandas", checksum_store, checksum_pandas)["ok"], (
            "the two import channels must produce the same result"
        )
        print("[OK] the columnar store and from_pandas channels agree field by field")

        section("Step 4: reconcile SQLite vs ApexBase query by query")
        client.use_table("orders")
        reports: list[dict] = []
        for label, sql in ANALYTICS_QUERIES:
            sqlite_rows = sqlite_query(sqlite_path, sql)
            apex_rows = client.execute(sql).to_dict()
            report = rows_match(label, sqlite_rows, apex_rows)
            reports.append(report)
            show(f"{label} row count", len(apex_rows))
            show(f"{label} comparison", report)
        for rep in reports:
            assert rep["ok"], f"{rep['query']} reconciliation failed: {rep['reason']}"
        print(f"[OK] {len(reports)} analytics queries produce identical results on SQLite and ApexBase")

        section("Step 5: third-party cross-validation (DuckDB, optional dependency)")
        # DuckDB is not a required dependency. This uses try/except to degrade
        # gracefully: run it when present for third-party corroboration, and state
        # clearly when it is skipped rather than faking a pass.
        try:
            import duckdb  # type: ignore
        except ImportError:
            print("[SKIP] duckdb is not installed, skipping third-party cross-validation (the ApexBase reconciliation result is unaffected)")
            duckdb_reports = []
        else:
            show("duckdb version", duckdb.__version__)
            con = duckdb.connect()
            # A real interoperability trap: pandas 3.x defaults string columns to
            # the new ``str`` extension type, which DuckDB 1.1.x does not recognise
            # (it fails with Data type 'str' not recognized). Converting string
            # columns back to numpy's object dtype registers fine -- and this is
            # purely the adapter layer feeding DuckDB; the ApexBase side needs no
            # such transcoding at all.
            df_for_duckdb = df.copy()
            for col in df_for_duckdb.columns:
                if str(df_for_duckdb[col].dtype) in ("str", "string", "string[pyarrow]"):
                    df_for_duckdb[col] = df_for_duckdb[col].astype(object)
            # Register the already-loaded DataFrame as a DuckDB view to avoid
            # re-reading from disk.
            con.register("orders", df_for_duckdb)
            duckdb_reports = []
            for label, sql in ANALYTICS_QUERIES:
                apex_rows = client.execute(sql).to_dict()
                cur = con.execute(sql)
                columns = [d[0] for d in cur.description]
                duck_rows = [dict(zip(columns, row)) for row in cur.fetchall()]
                report = rows_match(f"{label} (DuckDB)", duck_rows, apex_rows, tol=1e-4)
                duckdb_reports.append(report)
                show(f"{label} DuckDB comparison", report)
            for rep in duckdb_reports:
                assert rep["ok"], f"{rep['query']} cross-validation failed: {rep['reason']}"
            print(f"[OK] DuckDB cross-validation passed: {len(duckdb_reports)} queries agree three ways")
            con.close()

        section("Step 6: cross-checking the row-count definitions")
        # Three row-count sources: count_rows() (storage metadata), SQL COUNT(*),
        # and SQLite COUNT(*). Only when all three agree is it proven that "not one
        # row too many, not one too few" -- the most basic migration correctness.
        n_meta = client.count_rows()
        n_sql = client.execute("SELECT COUNT(*) AS n FROM orders").scalar()
        n_sqlite = sqlite_query(sqlite_path, "SELECT COUNT(*) AS n FROM orders")[0]["n"]
        show("row-count sources", {"count_rows()": n_meta, "SQL COUNT(*)": n_sql, "SQLite": n_sqlite})
        assert n_meta == n_sql == n_sqlite == ROW_COUNT, "all three row-count sources must agree"
        print("[OK] count_rows() / SQL COUNT(*) / SQLite COUNT(*) agree")

        section("Step 7: reconciliation report summary")
        # Turn the comparison results into a readable reconciliation table -- this
        # is what gets handed to the business side.
        print(f"{'query':<34}{'result':<8}{'max numeric diff':<16}{'detail'}")
        print("-" * 88)
        for rep in reports + duckdb_reports:
            status = "PASS" if rep["ok"] else "FAIL"
            diff = "-" if rep["max_diff"] is None else f"{rep['max_diff']:.3e}"
            print(f"{rep['query']:<34}{status:<8}{diff:<16}{rep['reason']}")

        all_reports = reports + duckdb_reports
        assert all(r["ok"] for r in all_reports), "there should be no FAIL in the reconciliation report"
        assert_close(
            float(len([r for r in all_reports if r["ok"]])),
            float(len(all_reports)),
            1e-9,
            "number of reconciliation items that passed",
        )

        section("Step 8: incremental queries after migration (usable directly on the HTAP side)")
        # The migrated database is not dead data: CTEs / JOINs can be layered on top
        # for share analysis. An empty OVER() makes the aggregated channel rows a
        # single partition, so SUM(revenue) OVER () is the global revenue total and
        # each channel's share is one direct division.
        share = client.execute(
            """
            WITH per_channel AS (
                SELECT channel, ROUND(SUM(revenue), 2) AS revenue
                FROM orders
                GROUP BY channel
            )
            SELECT
                channel                              AS channel,
                revenue                              AS revenue,
                SUM(revenue) OVER ()                 AS total_revenue,
                ROUND(revenue / SUM(revenue) OVER (), 4) AS share
            FROM per_channel
            ORDER BY revenue DESC, channel
            """
        ).to_dict()
        for row in share:
            show(f"channel {row['channel']} revenue share", row)
        assert_close(
            sum(r["share"] for r in share), 1.0, 1e-3, "sum of channel revenue shares"
        )

        print(f"\n=== Scenario 05 SQLite -> ApexBase migration reconciliation done ===")
        print(f"source database: {sqlite_path}\ntarget database: {os.path.join(base, 'db')}")


if __name__ == "__main__":
    main()
