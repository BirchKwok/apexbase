"""Scenario 04: log ingestion + deduplication + level roll-up

Business context
----------------
An observability platform collects application logs from each service's sidecar.
The upstream collector only guarantees "at least once" delivery, so the same log
line frequently lands in storage twice; on top of that, services spell their log
levels in every possible way (``ERROR`` / ``error`` / ``ERR``), so grouping by the
raw value produces a pile of fragmented buckets.

The on-call engineer needs one script that:

1. ingests the raw logs (duplicates included);
2. normalises levels into ``ERROR / WARN / INFO / DEBUG`` with ``CASE``;
3. counts the "real log lines" with ``DISTINCT`` and physically deletes duplicates;
4. rolls up by service x level and computes each service's error rate;
5. searches by pattern with ``LIKE`` / ``REGEXP_LIKE`` and by keyword with a full-text index.

ApexBase features demonstrated
------------------------------
1. ``CASE WHEN ... THEN ... END`` for level normalisation, with ``GROUP BY <alias>`` support.
2. Two duplicate-counting forms: ``COUNT(DISTINCT col)`` and
   ``SELECT COUNT(*) FROM (SELECT DISTINCT ...)``.
3. ``ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`` to number rows inside
   each duplicate group, then feeding the business keys of the ``rn > 1`` rows into
   ``client.delete(where=...)`` for a single physical delete.
4. **Error share via a window function**: ``SUM(total) OVER ()`` totals the
   per-service row counts in the aggregated result set (an empty ``OVER()`` is a
   single partition), so each service's share is one division -- no helper CTE and
   no cross join.
5. ``LIKE`` and ``REGEXP_LIKE`` pattern matching, plus an **FTS full-text index**:
   ``CREATE FTS INDEX ON logs(service, message)`` followed by ``WHERE MATCH('timeout')``.

Capability boundaries (this example follows the supported forms)
----------------------------------------------------------------
1. **FTS does not work on ``:memory:``** (it fails with File not open). This
   example uses a file database under ``work_dir()``, which satisfies that
   requirement by construction. FTS also needs "real words": only tokens of
   length 2 or more match reliably, while single characters often return nothing.
2. **``IN`` requires a column on the left-hand side**, not a function:
   ``UPPER(level) IN ('ERROR','ERR')`` fails with
   ``IN requires column on left side``. The level-normalisation ``CASE`` therefore
   expands the equality tests with ``OR``.
3. ``GROUP BY`` supports an **alias** but **not an expression**:
   ``GROUP BY <CASE expression>`` fails to parse and must be written as
   ``GROUP BY <alias>``.
4. No date-function arithmetic on time fields: ``day`` / ``hour`` are materialised
   as strings at write time.

How to run
----------
    python py04_log_ingestion_dedup.py

Output: each step prints to the console; the database file lives in
``_out/py_04/db``.
"""

from __future__ import annotations

import os

from apexbase import ApexClient
from _demo_env import assert_close, rng, section, show, work_dir

SLUG = "04"

SERVICES = ["api", "web", "db", "cache"]

# Upstream collectors do not agree on level spelling -- exactly why CASE
# normalisation is needed.
RAW_LEVELS = ["ERROR", "error", "ERR", "WARN", "warn", "INFO", "info", "DEBUG"]

# Log templates. Every token is at least 2 characters so FTS can index and match
# them reliably.
MESSAGES = [
    "database connection timeout while acquiring pool",
    "upstream payment gateway timeout on checkout",
    "request completed successfully with status ok",
    "slow query detected on orders table scan",
    "authentication failed for user session token",
    "cache miss for product catalog lookup",
    "disk usage above threshold on node storage",
    "connection refused by downstream inventory service",
]

DUPLICATE_ROUNDS = 3   # Each log line is delivered by upstream at most 3 times


def build_log_rows() -> list[dict]:
    """Generate deterministic raw logs (including duplicate deliveries).

    Duplicates are produced by treating ``(request_id, message, ts)`` as the
    identity of "one log line" and letting ``_demo_env.rng`` decide whether it
    appears 1..3 times. Every physical row has its own ``log_id``, so the
    "physical row count" exceeds the "logical log count" -- exactly the problem
    deduplication solves.
    """
    rnd = rng(20240401)
    rows: list[dict] = []
    log_id = 0
    for i in range(400):
        # Service / level / message are independent, so every service gets a mix
        # of levels and the error rates actually differ (otherwise each service
        # would only ever show fixed buckets and the report would carry no signal).
        svc = SERVICES[rnd.randrange(len(SERVICES))]
        level = RAW_LEVELS[rnd.randrange(len(RAW_LEVELS))]
        message = MESSAGES[rnd.randrange(len(MESSAGES))]
        day = f"2024-04-{1 + (i % 7):02d}"
        hour = f"{day}T{8 + (i % 10):02d}"
        ts = f"{hour}:00"
        request_id = f"req-{i:05d}"
        repeats = 1 + rnd.randrange(DUPLICATE_ROUNDS)
        for _ in range(repeats):
            log_id += 1
            rows.append(
                {
                    "log_id": log_id,
                    "ts": ts,
                    "day": day,
                    "hour": hour,
                    "service": svc,
                    "level": level,
                    "message": message,
                    "request_id": request_id,
                }
            )
    return rows


def main() -> None:
    base = work_dir(SLUG)
    rows = build_log_rows()

    with ApexClient(os.path.join(base, "db")) as client:
        # A file database (not :memory:) is a precondition for FTS to work.
        client.create_table(
            "logs",
            {
                "log_id": "int64",
                "ts": "string",
                "day": "string",
                "hour": "string",
                "service": "string",
                "level": "string",
                "message": "string",
                "request_id": "string",
            },
        )
        client.store(rows)

        section("Step 1: ingest logs (including upstream duplicate deliveries)")
        raw_count = client.count_rows()
        show("physical rows (duplicates included)", raw_count)
        show("sample logs", client.execute("SELECT * FROM logs ORDER BY log_id LIMIT 2").to_dict())
        assert raw_count == len(rows), f"ingested row count mismatch: {raw_count} vs {len(rows)}"

        section("Step 2: CASE level normalisation + roll-up by service/level")
        # Normalisation rules live in one CASE, so a business rule change touches a
        # single place. Note the OR expansion instead of IN -- a function
        # expression cannot sit on the left of IN.
        LEVEL_CASE = """
            CASE
                WHEN UPPER(level) = 'ERROR' OR UPPER(level) = 'ERR'
                     OR UPPER(level) = 'FATAL' THEN 'ERROR'
                WHEN UPPER(level) = 'WARN' OR UPPER(level) = 'WARNING' THEN 'WARN'
                WHEN UPPER(level) = 'INFO' THEN 'INFO'
                ELSE 'DEBUG'
            END
        """
        raw_levels = client.execute(
            """
            SELECT level, COUNT(*) AS n
            FROM logs
            GROUP BY level
            ORDER BY level
            """
        ).to_dict()
        show("raw levels before normalisation (fragmented buckets)", raw_levels)
        assert len(raw_levels) == len(RAW_LEVELS), (
            f"expected {len(RAW_LEVELS)} raw levels, got {len(raw_levels)}"
        )

        # GROUP BY uses the alias (the expression itself is not accepted).
        normalized = client.execute(
            f"""
            SELECT {LEVEL_CASE} AS level_norm, COUNT(*) AS n
            FROM logs
            GROUP BY level_norm
            ORDER BY n DESC, level_norm
            """
        ).to_dict()
        show("level distribution after normalisation", normalized)
        assert len(normalized) == 4, f"expected only 4 levels after normalisation, got {len(normalized)}"
        assert_close(
            sum(r["n"] for r in normalized),
            float(raw_count),
            1e-9,
            "normalised buckets sum to the physical row count",
        )

        by_service_level = client.execute(
            f"""
            SELECT
                service      AS service,
                {LEVEL_CASE} AS level_norm,
                COUNT(*)     AS n
            FROM logs
            GROUP BY service, level_norm
            ORDER BY service, level_norm
            """
        ).to_dict()
        for row in by_service_level:
            show(f"{row['service']} / {row['level_norm']}", row["n"])

        section("Step 3: DISTINCT duplicate counting")
        # The identity of "one log line" is (request_id, message, ts). Using it in
        # DISTINCT gives the logical log count; physical rows minus logical logs is
        # the number of duplicate deliveries.
        distinct_count = client.execute(
            """
            SELECT COUNT(*) AS n
            FROM (SELECT DISTINCT request_id, message, ts FROM logs) x
            """
        ).scalar()
        # The alternative form is COUNT(DISTINCT ...). Here request_id maps 1:1 to
        # a message, so a single column can be de-duplicated directly and must
        # agree with the count above.
        distinct_request_ids = client.execute(
            "SELECT COUNT(DISTINCT request_id) AS n FROM logs"
        ).scalar()
        show("logical log count (DISTINCT subquery)", distinct_count)
        show("logical log count (COUNT DISTINCT request_id)", distinct_request_ids)
        show("duplicate deliveries", raw_count - distinct_count)
        assert distinct_count == distinct_request_ids, "the two duplicate-counting forms must agree"
        assert raw_count - distinct_count > 0, "this example should indeed contain duplicate data"

        section("Step 4: locate duplicate rows with ROW_NUMBER and de-duplicate physically")
        # Number rows inside each group by ascending log_id and keep rn = 1 (the
        # earliest ingested row); everything else is a duplicate. Deletion goes
        # through client.delete(where=...): it accepts an IN list and returns the
        # deleted row count, saving a round trip versus deleting id by id.
        dup_rows = client.execute(
            """
            SELECT log_id, request_id, rn FROM (
                SELECT
                    log_id,
                    request_id,
                    ROW_NUMBER() OVER (PARTITION BY request_id, message, ts
                                       ORDER BY log_id) AS rn
                FROM logs
            ) x
            WHERE rn > 1
            ORDER BY log_id
            """
        ).to_dict()
        show("duplicate row count", len(dup_rows))
        show("first 5 log_ids to delete", [r["log_id"] for r in dup_rows[:5]])
        assert len(dup_rows) == raw_count - distinct_count, (
            "the number of rows with rn > 1 should equal the duplicate delivery count"
        )

        dup_ids = [r["log_id"] for r in dup_rows]
        deleted = client.delete(where=f"log_id IN ({','.join(str(i) for i in dup_ids)})")
        show("rows deleted by delete(where=...)", deleted)
        assert deleted == len(dup_ids), f"expected to delete {len(dup_ids)} rows, got {deleted}"

        after_count = client.count_rows()
        show("physical row count after deduplication", after_count)
        assert after_count == distinct_count, (
            f"row count after dedup {after_count} should equal the logical log count {distinct_count}"
        )
        print(f"[OK] physical dedup done: {raw_count} -> {after_count} rows, matching the DISTINCT count")

        section("Step 5: error-rate computation (window function over the aggregated rows)")
        # Goal: for each service, the ERROR rate within the service and its share
        # of all log lines that are errors. SUM(total) OVER () totals the
        # per-service row counts in the aggregated result set, so the global
        # denominator needs no extra CTE and no ON 1 = 1 cross join.
        error_rate = client.execute(
            f"""
            WITH per_service AS (
                SELECT
                    service                                AS service,
                    COUNT(*)                               AS total,
                    SUM(CASE WHEN {LEVEL_CASE} = 'ERROR' THEN 1 ELSE 0 END) AS errors,
                    SUM(CASE WHEN {LEVEL_CASE} = 'WARN'  THEN 1 ELSE 0 END) AS warns
                FROM logs
                GROUP BY service
            )
            SELECT
                service                                  AS service,
                total                                    AS total,
                errors                                   AS errors,
                warns                                    AS warns,
                ROUND(errors * 1.0 / total, 4)           AS error_rate,
                ROUND(errors * 1.0 / SUM(total) OVER (), 4) AS share_of_all_errors
            FROM per_service
            ORDER BY service
            """
        ).to_dict()
        for row in error_rate:
            show(f"{row['service']} error rate", row)

        for row in error_rate:
            assert 0.0 <= row["error_rate"] <= 1.0, f"{row['service']} error rate out of range"
        # Total errors == sum of per-service errors; the summed shares should equal
        # the global error rate.
        total_errors = sum(r["errors"] for r in error_rate)
        total_rows = sum(r["total"] for r in error_rate)
        assert total_rows == after_count, "the sum of per-service row counts should equal the total row count"
        assert_close(
            sum(r["share_of_all_errors"] for r in error_rate),
            total_errors / total_rows,
            1e-3,
            "sum of per-service shares of all errors",
        )

        # Service-level normalised roll-up (a report-shaped query over the
        # normalised result).
        service_summary = client.execute(
            f"""
            SELECT
                service AS service,
                COUNT(*) AS total,
                SUM(CASE WHEN {LEVEL_CASE} = 'ERROR' THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN {LEVEL_CASE} = 'WARN'  THEN 1 ELSE 0 END) AS warns,
                SUM(CASE WHEN {LEVEL_CASE} = 'INFO'  THEN 1 ELSE 0 END) AS infos,
                SUM(CASE WHEN {LEVEL_CASE} = 'DEBUG' THEN 1 ELSE 0 END) AS debugs
            FROM logs
            GROUP BY service
            ORDER BY errors DESC, service
            """
        ).to_dict()
        for row in service_summary:
            show(f"{row['service']} summary", row)

        section("Step 6: LIKE / REGEXP_LIKE pattern search")
        # LIKE suits fixed substrings/prefixes; REGEXP_LIKE suits real patterns.
        hits_timeout = client.execute(
            "SELECT COUNT(*) AS n FROM logs WHERE message LIKE '%timeout%'"
        ).scalar()
        show("message LIKE '%timeout%'", hits_timeout)
        assert hits_timeout > 0, "there should be logs containing timeout"

        prefix_hits = client.execute(
            "SELECT COUNT(*) AS n FROM logs WHERE message LIKE 'database%'"
        ).scalar()
        show("message LIKE 'database%' (prefix match)", prefix_hits)

        regex_hits = client.execute(
            """
            SELECT service, COUNT(*) AS n
            FROM logs
            WHERE REGEXP_LIKE(message, 'fail|refused')
            GROUP BY service
            ORDER BY n DESC, service
            """
        ).to_dict()
        show("REGEXP_LIKE(message, 'fail|refused')", regex_hits)

        # Pattern matching + level normalisation + grouping together form a real
        # triage query.
        triage = client.execute(
            f"""
            SELECT
                service      AS service,
                {LEVEL_CASE} AS level_norm,
                COUNT(*)     AS n
            FROM logs
            WHERE REGEXP_LIKE(message, 'timeout|refused|failed')
            GROUP BY service, level_norm
            -- ORDER BY needs enough tie-breakers: with only n DESC, the order of
            -- rows with equal counts is decided by the engine and the output would
            -- not be reproducible.
            ORDER BY n DESC, service, level_norm
            """
        ).to_dict()
        for row in triage:
            show(f"triage view {row['service']} / {row['level_norm']}", row["n"])

        section("Step 7: full-text index FTS + MATCH keyword search")
        # FTS must be built on a file database, and the index is best created after
        # the data is written. It is deliberately placed after the physical dedup
        # here: clean the data first, then index it, which keeps the semantics clear.
        status = client.execute("CREATE FTS INDEX ON logs(service, message)").to_dict()
        show("index creation result", status)

        match_timeout = client.execute(
            "SELECT service, COUNT(*) AS n FROM logs WHERE MATCH('timeout') GROUP BY service ORDER BY service"
        ).to_dict()
        show("MATCH('timeout') by service", match_timeout)
        assert sum(r["n"] for r in match_timeout) == hits_timeout, (
            "MATCH('timeout') should hit the same number of rows as LIKE '%timeout%'"
        )

        match_multi = client.execute(
            "SELECT COUNT(*) AS n FROM logs WHERE MATCH('payment gateway')"
        ).scalar()
        show("MATCH('payment gateway') (multiple words)", match_multi)

        # FTS composes with ordinary predicates: use the inverted index to converge
        # on candidates quickly, then refine with LIKE / level.
        combined = client.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM logs
            WHERE MATCH('timeout') AND {LEVEL_CASE} = 'ERROR'
            """
        ).scalar()
        show("MATCH('timeout') and level normalised to ERROR", combined)

        # After deduplication each request_id should appear exactly once -- let SQL
        # prove that the dedup worked.
        residual = client.execute(
            """
            SELECT request_id, COUNT(*) AS c
            FROM logs
            GROUP BY request_id
            HAVING COUNT(*) > 1
            """
        ).to_dict()
        show("request_ids that are still duplicated", residual)
        assert residual == [], "no duplicate request_id should remain after deduplication"
        print("[OK] deduplication is complete: no duplicate request_id remains")

        section("Step 8: convert results to Polars for downstream work")
        pl_df = client.execute(
            f"""
            SELECT
                hour         AS hour,
                {LEVEL_CASE} AS level_norm,
                COUNT(*)     AS n
            FROM logs
            GROUP BY hour, level_norm
            ORDER BY hour, level_norm
            """
        ).to_polars()
        print(pl_df.head(8))
        show("Polars shape", pl_df.shape)
        assert pl_df.height > 0

        print(f"\n=== Scenario 04 log ingestion/dedup/level roll-up done ===\ndatabase file under: {base}")


if __name__ == "__main__":
    main()
