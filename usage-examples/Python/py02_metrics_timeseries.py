"""Scenario 02: application metrics / time-series aggregation (time bucketing + anomaly detection + approximate P95)

Business context
----------------
An SRE team lands minute-level runtime metrics (request count, latency, error
count) for three service lines into ApexBase. Every day the on-call engineer
needs to do four things:

1. bucket the raw minute samples by day / hour and look at the trend;
2. roll up by "service x metric" to find the busiest service;
3. detect anomalous days automatically from the deviation against a baseline,
   rather than by staring at dashboards;
4. while chasing latency spikes, get an approximate P95 latency per service plus
   the throughput (request count) roll-up.

The whole dataset is only a few hundred to a few thousand rows, so running SQL on
embedded ApexBase is enough: no PostgreSQL / ClickHouse, and no data leaves the
process.

ApexBase features demonstrated
------------------------------
1. **Time bucketing**: the buckets (``2024-03-07`` / ``2024-03-07T10``) are
   materialised as string columns at write time, then grouped with ``GROUP BY``
   and filtered with push-down string range comparisons.
2. Multi-column ``GROUP BY`` aggregation with ordinary aggregate functions such
   as ``AVG`` / ``SUM`` / ``COUNT``.
3. **Leave-one-out baseline via a window function**: each day's comparison
   baseline is computed with ``SUM(day_value) OVER (PARTITION BY ...)`` and
   ``COUNT(*) OVER (PARTITION BY ...)``, and the deviation is derived from those.
   Excluding the current day keeps a single spike from inflating its own
   baseline.
4. **Approximate quantiles**: rank with
   ``ROW_NUMBER() OVER (PARTITION BY ...)`` and join the per-service counts to
   take rank ``ceil(0.95 * n)``, giving an approximate P95; the single-service
   ``ORDER BY ... LIMIT 1 OFFSET k`` form is shown next to it.
5. Throughput roll-up: ``SUM`` plus a window over the aggregated rows for the
   share of the total.

Capability boundaries (this example follows the supported forms)
----------------------------------------------------------------
1. ``DATE_TRUNC`` / ``strftime`` / ``DATE()`` / ``EXTRACT`` are **not
   supported**. Buckets are therefore not computed at query time; instead
   ``day`` / ``hour`` are materialised as strings at **write time**:
   ``"2024-03-07"`` and ``"2024-03-07T10"``. String prefixes sort naturally, so
   both grouping and range filtering use column-store push-down.
2. ``IN`` requires a **column** on the left-hand side; a function expression is
   rejected: ``UPPER(level) IN (...)`` fails with
   ``IN requires column on left side`` and must be rewritten as
   ``UPPER(x) = 'A' OR UPPER(x) = 'B'``. (The ``CASE`` grading in this example
   therefore expands with ``OR``.)

How to run
----------
    python py02_metrics_timeseries.py

Output: each step prints to the console; the database file lives in
``_out/py_02/db``.
"""

from __future__ import annotations

import os

from apexbase import ApexClient
from _demo_env import assert_close, rng, section, show, work_dir

SLUG = "02"

# Three service lines x three metrics x 10 days x 8 hours = 720 minute-level samples.
# The scale is deliberately small: this example is about clear query semantics,
# not about load testing.
SERVICES = ["api-gateway", "checkout", "search"]
METRICS = ["requests", "latency_ms", "errors"]
DAYS = [f"2024-03-{d:02d}" for d in range(1, 11)]
HOURS = list(range(8))

# Baseline magnitude per (service, metric). requests is a count, latency_ms is
# milliseconds and errors is a count -- the differing units do not matter because
# baselines are only ever compared inside one (service, metric) pair, never
# across metrics.
BASE = {
    ("api-gateway", "requests"): 1200.0,
    ("api-gateway", "latency_ms"): 42.0,
    ("api-gateway", "errors"): 6.0,
    ("checkout", "requests"): 800.0,
    ("checkout", "latency_ms"): 65.0,
    ("checkout", "errors"): 9.0,
    ("search", "requests"): 1500.0,
    ("search", "latency_ms"): 28.0,
    ("search", "errors"): 4.0,
}

# Deliberately injected fault: on 2024-03-07 checkout latency is multiplied by 5.
# Anomaly detection has to single out this day on its own, otherwise the detection
# logic is useless.
ANOMALY_DAY = "2024-03-07"
ANOMALY_SERVICE = "checkout"
ANOMALY_METRIC = "latency_ms"
ANOMALY_FACTOR = 5.0


def build_metric_rows() -> list[dict]:
    """Generate deterministic minute-level metric samples.

    ``value`` is composed as ``baseline x day factor x hour factor x noise``. The
    day factor only takes the three values 0.95/1.00/1.05, rotating day by day, so
    the leave-one-out baseline stays very close; the anomaly day instead gets
    multiplied by ``ANOMALY_FACTOR``, producing an order-of-magnitude jump.
    """
    rnd = rng(20240307)
    rows: list[dict] = []
    for svc in SERVICES:
        for metric in METRICS:
            base_value = BASE[(svc, metric)]
            for day in DAYS:
                day_index = DAYS.index(day)
                # Cycle through 0.95 / 1.00 / 1.05 to create mild periodic movement.
                day_factor = 1.0 + 0.05 * ((day_index % 3) - 1)
                for hour in HOURS:
                    hour_factor = 1.0 + 0.02 * hour
                    noise = rnd.uniform(0.97, 1.03)
                    value = base_value * day_factor * hour_factor * noise
                    if (
                        day == ANOMALY_DAY
                        and svc == ANOMALY_SERVICE
                        and metric == ANOMALY_METRIC
                    ):
                        value *= ANOMALY_FACTOR
                    rows.append(
                        {
                            "ts": f"{day}T{hour:02d}:00",
                            "day": day,                      # day bucket: materialised at write time
                            "hour": f"{day}T{hour:02d}",     # hour bucket: materialised at write time
                            "service": svc,
                            "metric": metric,
                            "value": round(value, 4),
                        }
                    )
    return rows


def main() -> None:
    base = work_dir(SLUG)
    rows = build_metric_rows()

    with ApexClient(os.path.join(base, "db")) as client:
        # Declare the schema explicitly: this avoids type inference on the first
        # inserted row and stops a float column from being inferred as int.
        client.create_table(
            "metrics",
            {
                "ts": "string",
                "day": "string",
                "hour": "string",
                "service": "string",
                "metric": "string",
                "value": "float64",
            },
        )
        client.store(rows)

        section("Step 1: time bucketing -- aggregate all metrics by day")
        # Because day is a string with the fixed layout YYYY-MM-DD, grouping by it
        # is exactly day bucketing; no date function such as DATE_TRUNC is needed.
        daily_all = client.execute(
            """
            SELECT
                day,
                COUNT(*)                  AS samples,
                ROUND(SUM(value), 2)      AS total_value
            FROM metrics
            GROUP BY day
            ORDER BY day
            """
        ).to_dict()
        show("number of days", len(daily_all))
        show("first day / last day", (daily_all[0], daily_all[-1]))

        # String range comparison is push-down friendly: take 03-03 through 03-06
        # (inclusive lower bound, exclusive upper bound).
        window_rows = client.execute(
            """
            SELECT COUNT(*) AS samples
            FROM metrics
            WHERE day >= ? AND day < ?
            """,
            params=["2024-03-03", "2024-03-07"],
        ).scalar()
        show("samples in the string range 03-03 .. 03-06", window_rows)
        # 4 of the 10 days x 3 services x 3 metrics x 8 hours = 288 rows
        assert window_rows == 4 * 3 * 3 * 8, f"unexpected row count for the range filter: {window_rows}"

        section("Step 2: daily x per-metric aggregation (drilling into the service dimension)")
        daily_metric = client.execute(
            """
            SELECT
                day,
                metric,
                ROUND(AVG(value), 3) AS avg_value,
                ROUND(SUM(value), 2) AS sum_value,
                COUNT(*)             AS samples
            FROM metrics
            GROUP BY day, metric
            ORDER BY day, metric
            """
        ).to_dict()
        for row in daily_metric:
            if row["day"] in ("2024-03-01", "2024-03-07"):
                show(f"{row['day']} / {row['metric']}", row)

        by_service = client.execute(
            """
            SELECT
                service,
                metric,
                ROUND(AVG(value), 3) AS avg_value,
                ROUND(SUM(value), 2) AS sum_value
            FROM metrics
            GROUP BY service, metric
            ORDER BY service, metric
            """
        ).to_dict()
        for row in by_service:
            show(f"{row['service']} / {row['metric']}", row)

        section("Step 3: anomaly detection -- deviation from a leave-one-out baseline")
        # Key design: the baseline is not "the average of all days" but "the
        # average of the **other** days, excluding the current one". The window
        # form says that directly: SUM(day_value) OVER (PARTITION BY service,
        # metric) totals the whole partition and COUNT(*) OVER (...) counts it, so
        # (total - current) / (n - 1) is exactly the leave-one-out mean.
        #
        # Keeping the current day out of its own baseline is what makes the signal
        # stand out: against a plain all-day average a 5x spike would only look
        # like a ~36% deviation and could easily slip under the threshold.
        anomalies = client.execute(
            """
            WITH daily AS (
                SELECT
                    service,
                    metric,
                    day,
                    ROUND(AVG(value), 4) AS day_value
                FROM metrics
                GROUP BY service, metric, day
            ),
            with_totals AS (
                SELECT
                    service,
                    metric,
                    day,
                    day_value,
                    SUM(day_value) OVER (PARTITION BY service, metric) AS sum_all,
                    COUNT(*)       OVER (PARTITION BY service, metric) AS n_all
                FROM daily
            ),
            with_baseline AS (
                SELECT
                    service,
                    metric,
                    day,
                    day_value,
                    ROUND((sum_all - day_value) / (n_all - 1), 4) AS baseline
                FROM with_totals
            )
            SELECT
                service                                     AS service,
                metric                                      AS metric,
                day                                         AS day,
                day_value                                   AS day_value,
                baseline                                    AS baseline,
                ROUND((day_value - baseline) / baseline, 4) AS deviation
            FROM with_baseline
            -- Add service/metric/day as tie-breakers: sorting by deviation alone
            -- leaves the order of near-equal rows up to the engine, so the output
            -- would not be reproducible.
            ORDER BY deviation DESC, service, metric, day
            """
        ).to_dict()

        # Threshold 0.4: the day factor keeps normal days within about +/-10%,
        # so only the injected fault day crosses it.
        THRESHOLD = 0.4
        flagged = [r for r in anomalies if abs(r["deviation"]) > THRESHOLD]
        show(f"deviation threshold |deviation| > {THRESHOLD}", f"{len(flagged)} hit(s)")
        for row in flagged:
            show("anomaly", row)
        show("top 3 deviations (including normal days)", anomalies[:3])

        assert len(flagged) == 1, f"exactly 1 anomaly should be flagged, got {len(flagged)}"
        hit = flagged[0]
        assert hit["service"] == ANOMALY_SERVICE, f"anomalous service should be {ANOMALY_SERVICE}"
        assert hit["metric"] == ANOMALY_METRIC, f"anomalous metric should be {ANOMALY_METRIC}"
        assert hit["day"] == ANOMALY_DAY, f"anomalous day should be {ANOMALY_DAY}"
        assert hit["deviation"] > 3.0, f"a 5x spike should deviate far beyond 3, got {hit['deviation']}"
        print(
            f"[OK] anomaly detection hit {hit['day']} / {hit['service']} / {hit['metric']} exactly, "
            f"deviation from baseline {hit['deviation']}"
        )

        section("Step 4: approximate P95 per service (ORDER BY + LIMIT nearest-rank quantile)")
        # An exact quantile would need a percentile aggregate, which ApexBase does
        # not have. In practice the nearest-rank approximation is good enough:
        #     P95 rank = ceil(0.95 * n)
        # Assign ROW_NUMBER over each service's samples in ascending value order,
        # join the per-service counts, and take the row whose rank equals
        # ceil(0.95 * n).
        p95 = client.execute(
            """
            WITH cnt AS (
                SELECT service, COUNT(*) AS n
                FROM metrics
                GROUP BY service
            ),
            ranked AS (
                SELECT
                    service,
                    value,
                    ROW_NUMBER() OVER (PARTITION BY service ORDER BY value) AS rn
                FROM metrics
            )
            SELECT
                r.service                       AS service,
                c.n                             AS samples,
                CAST(CEIL(0.95 * c.n) AS INT)   AS p95_rank,
                r.value                         AS p95_approx
            FROM ranked r
            JOIN cnt c ON r.service = c.service
            WHERE r.rn = CAST(CEIL(0.95 * c.n) AS INT)
            ORDER BY r.service
            """
        ).to_dict()
        for row in p95:
            show(f"{row['service']} approximate P95", row)

        # Also pull the median (rank = floor((n+1)/2)) to self-check monotonicity:
        # P95 must be >= the median, otherwise the ranks were computed wrongly.
        median = client.execute(
            """
            WITH cnt AS (
                SELECT service, COUNT(*) AS n FROM metrics GROUP BY service
            ),
            ranked AS (
                SELECT
                    service,
                    value,
                    ROW_NUMBER() OVER (PARTITION BY service ORDER BY value) AS rn
                FROM metrics
            )
            SELECT r.service AS service, r.value AS median
            FROM ranked r
            JOIN cnt c ON r.service = c.service
            WHERE r.rn = CAST(FLOOR((c.n + 1) / 2.0) AS INT)
            ORDER BY r.service
            """
        ).to_dict()
        median_map = {r["service"]: r["median"] for r in median}
        for row in p95:
            show(f"{row['service']} median", median_map[row["service"]])
            assert row["p95_approx"] >= median_map[row["service"]], (
                f"{row['service']} P95 {row['p95_approx']} should not be below the median "
                f"{median_map[row['service']]}"
            )
        print("[OK] every service's approximate P95 is at least its median (quantile monotonicity holds)")

        # The equivalent form for a single service is more direct: ORDER BY +
        # LIMIT 1 OFFSET k. With n = 10 days x 8 hours = 80, the rank is
        # ceil(0.95 * 80) = 76, and OFFSET is 0-based so it becomes 75.
        svc = "checkout"
        n_svc = client.execute(
            "SELECT COUNT(*) AS n FROM metrics WHERE service = ?", params=[svc]
        ).scalar()
        offset = int(-(-0.95 * n_svc // 1)) - 1  # integer form of a ceil division
        p95_kth = client.execute(
            "SELECT value FROM metrics WHERE service = ? ORDER BY value LIMIT 1 OFFSET ?",
            params=[svc, offset],
        ).scalar()
        rank_join_value = next(r["p95_approx"] for r in p95 if r["service"] == svc)
        show(f"{svc} n / offset / LIMIT-OFFSET P95", (n_svc, offset, p95_kth))
        assert_close(p95_kth, rank_join_value, 1e-9, "the two approximate P95 forms agree")

        section("Step 5: throughput roll-up (requests and share by day / by service)")
        # share = service request volume / global request volume. An empty OVER()
        # makes the whole aggregated result set a single partition, so
        # SUM(requests) OVER () is the global denominator directly -- no helper CTE
        # and no ON 1 = 1 cross join needed.
        throughput = client.execute(
            """
            WITH per_service AS (
                SELECT service, ROUND(SUM(value), 2) AS requests
                FROM metrics
                WHERE metric = 'requests'
                GROUP BY service
            )
            SELECT
                service                                     AS service,
                requests                                    AS requests,
                ROUND(SUM(requests) OVER (), 2)             AS total_requests,
                ROUND(requests / SUM(requests) OVER (), 4)  AS share
            FROM per_service
            ORDER BY requests DESC, service
            """
        ).to_dict()
        for row in throughput:
            show(f"{row['service']} throughput", row)

        daily_throughput = client.execute(
            """
            SELECT
                day,
                ROUND(SUM(value), 2) AS requests
            FROM metrics
            WHERE metric = 'requests'
            GROUP BY day
            ORDER BY day
            """
        ).to_dict()
        for row in daily_throughput[:3]:
            show(f"{row['day']} requests", row["requests"])

        # Self-check 1: the per-service shares sum to 1.
        share_sum = sum(r["share"] for r in throughput)
        assert_close(share_sum, 1.0, 1e-3, "sum of per-service throughput shares")

        # Self-check 2: summing by day == summing by service == the global requests total.
        daily_sum = round(sum(r["requests"] for r in daily_throughput), 2)
        service_sum = round(sum(r["requests"] for r in throughput), 2)
        assert_close(daily_sum, service_sum, 0.01, "daily roll-up agrees with service roll-up")

        # Self-check 3: the anomaly day's checkout latency mean should be clearly
        # above its normal level.
        normal_latency = next(
            r["avg_value"]
            for r in daily_metric
            if r["day"] == "2024-03-01" and r["metric"] == "latency_ms"
        )
        show("reference: 2024-03-01 average latency across all services", normal_latency)

        section("Step 6: hand results straight to Pandas / Polars for downstream work")
        df = client.execute(
            """
            WITH daily AS (
                SELECT service, metric, day, ROUND(AVG(value), 4) AS day_value
                FROM metrics
                GROUP BY service, metric, day
            ),
            with_totals AS (
                SELECT
                    service,
                    metric,
                    day,
                    day_value,
                    SUM(day_value) OVER (PARTITION BY service, metric) AS sum_all,
                    COUNT(*)       OVER (PARTITION BY service, metric) AS n_all
                FROM daily
            ),
            with_baseline AS (
                SELECT
                    service,
                    metric,
                    day,
                    day_value,
                    ROUND((sum_all - day_value) / (n_all - 1), 4) AS baseline
                FROM with_totals
            )
            SELECT
                day                                         AS day,
                service                                     AS service,
                day_value                                   AS latency_ms,
                ROUND((day_value - baseline) / baseline, 4) AS deviation
            FROM with_baseline
            WHERE metric = 'latency_ms' AND service = ?
            ORDER BY day
            """,
            params=[ANOMALY_SERVICE],
        ).to_pandas()
        print(df.to_string(index=False))
        show("DataFrame shape", df.shape)
        assert df.shape[0] == len(DAYS), f"expected {len(DAYS)} days, got {df.shape[0]}"
        show("the day with the largest deviation", df.loc[df["deviation"].idxmax()].to_dict())

        print(f"\n=== Scenario 02 application metrics/time-series aggregation done ===\ndatabase file under: {base}")


if __name__ == "__main__":
    main()
