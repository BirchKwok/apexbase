"""Scenario 15: hybrid-storage data lake foundation (partitioned Parquet cold data + ApexBase hot replica)

Business context
----------------
The most typical shape of a data lake is "a collection of Parquet files partitioned by directory":

    lake/day=2024-03-01/region=east/part-000.parquet
    lake/day=2024-03-01/region=west/part-001.parquet
    ...

This layout is cheap and open, but every query has to reopen and parse a pile of small files.
The standard engineering practice is to maintain a "**hot replica**" on top of the lake:

* **Cold path**: query Parquet directly (newly landed data, infrequent access, cost sensitive);
* **Hot path**: materialise frequently used slices into ApexBase (mmap zero-copy, zone map / bloom filter,
  columnar pushdown) so that repeated queries are faster.

This example uses ``read_parquet()`` and ``register_temp_table()`` to query the whole partitioned
lake, then materialises a hot replica with ``CREATE TABLE ... AS``, and finally times the
**cold and hot paths** on the same machine for comparison.

ApexBase features demonstrated
------------------------------
1. ``read_parquet('path')`` table function: use Parquet directly as a relation.
   It supports WHERE / GROUP BY / JOIN / UNION ALL; multiple files are assembled with a hand-written
   ``UNION ALL`` (the example shows how to assemble the whole partitioned lake).
2. ``register_temp_table(name, file_path)``: materialise a file into an mmap-backed temp table.
   **After registering you must call ``use_table(name)``**, otherwise ``execute()`` reports
   "No table selected"; the temp table also does **not** appear in ``list_tables()``.
3. ``CREATE TABLE hot AS SELECT ...``: materialise the lake query result into a persistent table (hot replica).
4. Consistency and timing comparison between the cold and hot paths (median over several rounds).
5. String date buckets plus range predicates: ``day`` is materialised as a ``YYYY-MM-DD`` string at write
   time, so both grouping and range filtering use string comparison (ApexBase does not support
   ``DATE_TRUNC`` / ``strftime``).

Capability boundaries
---------------------
* ``register_temp_table`` accepts a **single file** path; to register the whole lake, merge the small
  files into one Parquet snapshot first (this example performs that merge, which is also the usual
  compaction step in a lakehouse).
* Derived tables must have an alias (``FROM (...) AS t``), otherwise parsing fails.
* Temp tables are cleaned up automatically when the client closes, and their files live in a private
  temporary directory.

How to run
----------
    python py15_data_lake_foundation.py

Output: single-partition / whole-lake queries, hot-replica materialisation, and the cold-vs-hot
comparison; data lives under ``_out/py_15/``.
"""

from __future__ import annotations

import glob
import os
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from apexbase import ApexClient
from _demo_env import rng, section, show, work_dir

SLUG = "15"
DAYS = ["2024-03-01", "2024-03-02", "2024-03-03", "2024-03-04"]
REGIONS = ["east", "west", "north"]
CHANNELS = ["web", "app", "store"]
ROWS_PER_PART = 2000        # 2000 rows per partition -> 12 partitions, 24000 rows in total
REPEATS = 7                 # number of timing rounds (the median is taken)


def build_lake(lake_root: str) -> list[str]:
    """Write partition files under Hive-style ``day=.../region=.../part.parquet`` directories.

    Returns the paths of all Parquet files (sorted, so the run is reproducible).
    """
    rnd = rng(1501)
    for day in DAYS:
        for region in REGIONS:
            part_dir = os.path.join(lake_root, f"day={day}", f"region={region}")
            os.makedirs(part_dir, exist_ok=True)
            order_id, amount, channel, units = [], [], [], []
            for i in range(ROWS_PER_PART):
                order_id.append(i + 1)
                amount.append(round(rnd.uniform(10.0, 500.0), 2))
                channel.append(CHANNELS[i % len(CHANNELS)])
                units.append(1 + (i % 7))
            table = pa.table(
                {
                    # The partition columns are also written into the file (Hive style), so that a
                    # single-file query can still see day/region without relying on path inference.
                    "order_id": pa.array(order_id, pa.int64()),
                    "day": pa.array([day] * ROWS_PER_PART, pa.string()),
                    "region": pa.array([region] * ROWS_PER_PART, pa.string()),
                    "channel": pa.array(channel, pa.string()),
                    "units": pa.array(units, pa.int64()),
                    "amount": pa.array(amount, pa.float64()),
                }
            )
            pq.write_table(table, os.path.join(part_dir, "part-0000.parquet"))
    return sorted(glob.glob(os.path.join(lake_root, "**", "*.parquet"), recursive=True))


def union_sql(files: list[str]) -> str:
    """Join the partition file list into one ``UNION ALL`` subquery text (the simplest way to query the lake)."""
    parts = [f"SELECT * FROM read_parquet('{p.replace(chr(39), chr(39) * 2)}')" for p in files]
    return " UNION ALL ".join(parts)


def timed(sql_runner, repeats: int = REPEATS) -> float:
    """Run several rounds and return the median elapsed time in seconds."""
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        sql_runner()
        samples.append(time.perf_counter() - start)
    return float(np.median(samples))


def main() -> None:
    base = work_dir(SLUG)
    lake_root = os.path.join(base, "lake")
    files = build_lake(lake_root)

    section("Step 1: partitioning layout of the lake")
    show("Parquet partition file count", len(files))
    for path in files[:4]:
        show("  sample path", os.path.relpath(path, base))
    print(f"    ...({len(files)} files in total, organised as two levels of day=.../region=... directories)")

    all_sql = union_sql(files)

    with ApexClient(os.path.join(base, "db")) as client:
        section("Step 2: cold path -- query the whole Parquet lake directly (read_parquet + UNION ALL)")
        # Single-partition query: only one file is parsed, so partition pruning is done purely by file name.
        single = files[0].replace("'", "''")
        one = client.execute(
            f"""
            SELECT COUNT(*) AS rows, ROUND(SUM(amount), 2) AS amount
            FROM read_parquet('{single}')
            """
        ).to_dict()[0]
        show("single partition (day=2024-03-01 / region=east)", one)

        # Whole-lake aggregation: UNION ALL the 12 files, then group by day / region.
        cold_agg = client.execute(
            f"""
            SELECT day, region, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS amount
            FROM ({all_sql}) AS lake
            GROUP BY day, region
            ORDER BY day, region
            """
        ).to_dict()
        show("whole-lake group count (4 days x 3 regions)", len(cold_agg))
        for row in cold_agg[:3]:
            show("  ", row)
        # Self-check: the whole-lake row count must equal partitions x rows per partition.
        total_rows = client.execute(f"SELECT COUNT(*) AS n FROM ({all_sql}) AS lake").scalar()
        assert total_rows == len(files) * ROWS_PER_PART, (
            f"whole-lake row count {total_rows} should be {len(files) * ROWS_PER_PART}"
        )
        print(f"[OK] whole-lake rows {total_rows} == {len(files)} partitions x {ROWS_PER_PART} rows/partition")

        # Every partition must have the same row count (string grouping verifies partition pruning is correct).
        dist = client.execute(
            f"""
            SELECT day, region, COUNT(*) AS n
            FROM ({all_sql}) AS lake
            GROUP BY day, region
            HAVING n <> {ROWS_PER_PART}
            """
        ).to_dict()
        assert dist == [], f"partitions with an unexpected row count: {dist}"
        print("[OK] all 12 partitions have exactly 2000 rows (no duplicated files, nothing missed)")

        section("Step 3: compact the lake into a single file and register_temp_table (mmap materialisation)")
        # The usual lakehouse compaction: merge the small files into one snapshot file, then register it
        # as an mmap temp table so that repeated queries avoid re-parsing 12 Parquet files.
        snapshot = os.path.join(base, "lake_snapshot.parquet")
        # Use pyarrow to read the 12 partitions into one table and write a single snapshot file.
        # Use ParquetFile.read() rather than pq.read_table(): the latter performs Hive partition
        # inference on the ``day=.../region=...`` directories, parses the day column as dictionary
        # type, and then conflicts with the file's own string column (ArrowTypeError: Unable to merge).
        tables = [pq.ParquetFile(p).read() for p in files]
        pq.write_table(pa.concat_tables(tables), snapshot)
        show("snapshot file size (KB)", round(os.path.getsize(snapshot) / 1024, 1))

        client.register_temp_table("lake_snapshot", snapshot)
        # Key point: after registering you must call use_table(), otherwise execute() reports "No table selected".
        client.use_table("lake_snapshot")
        show("current table", client.current_table)
        show("persistent table list (temp tables excluded)", client.list_tables())
        snap_rows = client.count_rows()
        assert snap_rows == total_rows, f"snapshot rows {snap_rows} do not match whole-lake rows {total_rows}"
        print(f"[OK] temp table rows {snap_rows} == whole-lake rows (registration materialises everything, it is not sampling)")

        section("Step 4: materialise the hot replica (CREATE TABLE AS)")
        # The hot replica becomes a persistent table: later queries go through mmap + zone map and never
        # parse Parquet again.
        client.execute(
            f"""
            CREATE TABLE sales_hot AS
            SELECT order_id, day, region, channel, units, amount
            FROM lake_snapshot
            """
        )
        client.use_table("sales_hot")
        hot_rows = client.count_rows()
        show("hot replica rows", hot_rows)
        show("persistent table list (hot replica is persisted)", client.list_tables())
        assert hot_rows == snap_rows, "hot replica row count does not match the source"

        section("Step 5: same-machine timing comparison of the cold and hot paths")
        # Query: aggregate amount and order count by day/region/channel, with one range filter.
        agg_cold = f"""
            SELECT day, region, channel,
                   COUNT(*)                AS orders,
                   ROUND(SUM(amount), 2)   AS amount,
                   SUM(units)              AS units
            FROM ({all_sql}) AS lake
            WHERE day >= '2024-03-02'
            GROUP BY day, region, channel
            ORDER BY day, region, channel
        """
        agg_hot = """
            SELECT day, region, channel,
                   COUNT(*)                AS orders,
                   ROUND(SUM(amount), 2)   AS amount,
                   SUM(units)              AS units
            FROM sales_hot
            WHERE day >= '2024-03-02'
            GROUP BY day, region, channel
            ORDER BY day, region, channel
        """
        cold_result = client.execute(agg_cold).to_dict()
        # Switch to the hot table and run the identical aggregation (note the use_table switch).
        client.use_table("sales_hot")
        hot_result = client.execute(agg_hot).to_dict()

        # ---- Self-check 1: the cold and hot paths must agree row for row ----
        assert cold_result == hot_result, "the cold and hot paths produced different aggregation results"
        print(f"[OK] cold / hot aggregation results are identical row for row ({len(cold_result)} groups)")

        # ---- Self-check 2: full cross-validation of two paths whose **physical formats differ completely** ----
        # The cold path re-parses the Parquet text format every time; the hot path reads the mmap
        # columnar binary format. Only when the totals match do we know that materialising the hot
        # replica dropped no rows, truncated nothing and lost no precision.
        total_cold = client.execute(f"SELECT ROUND(SUM(amount), 2) AS s FROM ({all_sql}) AS lake").scalar()
        client.use_table("sales_hot")
        total_hot = client.execute("SELECT ROUND(SUM(amount), 2) AS s FROM sales_hot").scalar()
        show("cold total amount / hot total amount", f"{total_cold} / {total_hot}")
        assert total_cold == total_hot, "cold and hot total amounts differ"
        print("[OK] the two physical paths -- cold (Parquet parsing) and hot (mmap columnar) -- agree exactly on the total amount")

        # Timing: the cold path opens and parses 12 Parquet files every time; the hot path only mmaps
        # local columnar data. ApexBase caches file metadata, so repeated Parquet queries are not
        # absurdly slow; but the fixed cost of "parse + column pruning + predicate pushdown" remains,
        # and the hot replica exists to amortise exactly that cost.
        t_cold = timed(lambda: client.execute(agg_cold))
        client.use_table("sales_hot")
        t_hot = timed(lambda: client.execute(agg_hot))
        show(f"cold path (read_parquet x {len(files)}) median time", f"{t_cold * 1e3:.3f} ms")
        show("hot path (mmap columnar) median time", f"{t_hot * 1e3:.3f} ms")
        show("speedup (cold / hot)", f"{t_cold / t_hot:.2f}x")
        # The timing assertion is deliberately loose (the sample data is small, so absolute differences are milliseconds).
        assert t_hot <= t_cold * 3.0 + 0.05, "the hot path took unexpectedly long"
        print(
            "[OK] the hot replica amortises repeated-query parsing: the cold path parses 12 Parquet files, "
            "the hot path mmaps local columnar data directly"
        )

        section("Step 6: mixed query over lake + hot replica (federated: cold data JOIN hot data)")
        # Real-world case: a newly landed partition is still in the lake (cold) while history already
        # lives in the hot table (hot); one SQL statement queries both.
        hybrid = client.execute(
            f"""
            WITH lake_new AS (
                SELECT day, region, channel, amount FROM read_parquet('{files[-1].replace(chr(39), chr(39) * 2)}')
            ),
            hot AS (
                SELECT day, region, channel, amount FROM sales_hot WHERE day < '{DAYS[-1]}'
            ),
            all_rows AS (
                SELECT * FROM lake_new
                UNION ALL
                SELECT * FROM hot
            )
            SELECT day, region, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS amount
            FROM all_rows
            GROUP BY day, region
            ORDER BY day, region
            """
        ).to_dict()
        show("cold+hot mixed query group count", len(hybrid))
        for row in hybrid[:3]:
            show("  ", row)
        # The row count of the mixed cold+hot query must equal the expected total.
        # Note: the columns of both sides must be aligned explicitly (same count, same order);
        # ``read_parquet`` may surface extra partition columns for Hive-style paths, so an explicit
        # SELECT list is the safest form.
        last_file = files[-1].replace("'", "''")
        mixed_rows = client.execute(
            f"""
            WITH lake_new AS (
                SELECT order_id, day, region, channel, units, amount
                FROM read_parquet('{last_file}')
            ),
            hot AS (
                SELECT order_id, day, region, channel, units, amount
                FROM sales_hot WHERE day < '{DAYS[-1]}'
            )
            SELECT COUNT(*) AS n
            FROM (SELECT * FROM lake_new UNION ALL SELECT * FROM hot) AS t
            """
        ).scalar()
        # Expected row count: the hot side contributes every partition before the last day
        # = (4-1) days x 3 regions x 2000 rows; the lake side takes only the last day's single
        # partition (2000 rows).
        client.use_table("sales_hot")
        hot_before_last = client.execute(
            f"SELECT COUNT(*) AS n FROM sales_hot WHERE day < '{DAYS[-1]}'"
        ).scalar()
        hot_expected = (len(DAYS) - 1) * len(REGIONS) * ROWS_PER_PART
        assert hot_before_last == hot_expected, f"hot-side history rows {hot_before_last} should be {hot_expected}"
        assert mixed_rows == ROWS_PER_PART + hot_expected, f"unexpected cold+hot mixed row count: {mixed_rows}"
        print(
            f"[OK] the mixed cold+hot query covers {mixed_rows} rows == lake-side latest partition {ROWS_PER_PART} rows"
            f" + hot-table history {hot_before_last} rows"
        )

    print(
        f"\n=== Scenario 15: hybrid-storage data lake foundation complete ===\n"
        f"Key takeaway: the partitioned Parquet lake provides low-cost storage and open access,\n"
        f"         register_temp_table provides mmap materialisation and CREATE TABLE AS builds a persistent hot replica,\n"
        f"         and both paths return identical results at different cost, so data can be tiered by access frequency.\n"
        f"Data lives in: {base}"
    )


if __name__ == "__main__":
    main()
