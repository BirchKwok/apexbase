"""Cached `GROUP BY` while a pending cell update exists.

A pending DeltaStore cell update used to refuse the cached group-by lane outright.
One `UPDATE` therefore pushed the whole `GROUP BY ... ORDER BY count` family onto the
generic route for good -- 7.3 ms against 0.21 ms on 200k rows, a 35x cliff that
survived reopens -- and nothing in the query said so.

The lane is safe to keep when the update cannot change the answer:
* no row was added or removed (`pending_delta_delete_count() == 0` and the active
  row count still matches the base), and
* neither the grouped column nor any aggregated column was touched. `COUNT(*)` has
  no aggregated column, so an update to any other column leaves its counts alone.

When the aggregated column *is* touched, `SUM`/`AVG` are repaired instead of being
refused: they are sum-based, so folding `(new - old)` for each updated row into its
group gives the right answer. `MIN`/`MAX` cannot be repaired that way and a change of
nullness would move a `COUNT(col)`, so those still take the generic route.
"""

import statistics
import tempfile
import time

from apexbase.client import ApexClient

ROWS = 50_000
COUNT_SQL = "SELECT category, COUNT(*) AS n FROM t GROUP BY category ORDER BY n DESC"
SUM_SQL = "SELECT category, SUM(k) AS s FROM t GROUP BY category ORDER BY s DESC"


def _fixture(tmp):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"category": "string", "k": "int64"})
    client.use_table("t")
    client.store({"category": [f"cat{i % 20}" for i in range(ROWS)],
                  "k": list(range(ROWS))})
    client.flush()
    return client


def _path(client, sql):
    plan = str(client.execute("EXPLAIN ANALYZE " + sql).to_dict()[0])
    for line in plan.splitlines():
        if "Actual Path" in line:
            # The path label itself carries an embedded, literal "\n" plus the row
            # count, so split on that rather than on a real newline.
            return line.split("Actual Path:")[1].strip().split("\\n")[0].strip()
    return "?"


def _python_counts(client):
    counts = {}
    for row in client.execute("SELECT category FROM t").to_dict():
        counts[row["category"]] = counts.get(row["category"], 0) + 1
    return counts


def _scanned_counts(client):
    return {row["category"]: row["n"]
            for row in client.execute(
                "SELECT category, COUNT(*) AS n FROM t GROUP BY category"
            ).to_dict()}


def test_an_update_outside_the_columns_keeps_the_cached_lane():
    with tempfile.TemporaryDirectory() as tmp:
        client = _fixture(tmp)
        try:
            assert _path(client, COUNT_SQL) == "fast_cached_group_by"
            client.execute("UPDATE t SET k = 5 WHERE k = 10")
            client.flush()
            # `k` is neither grouped nor aggregated by COUNT(*), so the counts are
            # untouched and the lane must stay.
            assert _path(client, COUNT_SQL) == "fast_cached_group_by"
            assert _scanned_counts(client) == _python_counts(client)
        finally:
            client.close()


def test_an_update_to_the_grouped_column_declines_and_stays_correct():
    with tempfile.TemporaryDirectory() as tmp:
        client = _fixture(tmp)
        try:
            client.execute("UPDATE t SET category = 'catX' WHERE k = 20")
            client.flush()
            # The group labels changed, so the cached dictionary no longer describes
            # the answer: the lane must decline -- and the answer must still be right.
            assert _path(client, COUNT_SQL) != "fast_cached_group_by"
            assert _scanned_counts(client) == _python_counts(client)
            assert "catX" in _scanned_counts(client)
        finally:
            client.close()


def test_an_update_to_an_aggregated_column_is_repaired_not_refused():
    """The lane used to refuse an update to `SUM(k)`'s own column.

    It now folds `(new - old)` into the aggregate instead, so the sums must be the
    updated ones while the cost stays on the cached lane.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = _fixture(tmp)
        try:
            assert _path(client, SUM_SQL) == "fast_cached_group_by"
            client.execute("UPDATE t SET k = 7 WHERE k = 30")
            client.flush()
            assert _path(client, SUM_SQL) == "fast_cached_group_by"
            scanned = {row["category"]: row["s"]
                       for row in client.execute(
                           "SELECT category, SUM(k) AS s FROM t GROUP BY category"
                       ).to_dict()}
            assert scanned == _python_sums(client)
        finally:
            client.close()


def test_the_lane_survives_a_reopen_after_a_harmless_update():
    with tempfile.TemporaryDirectory() as tmp:
        client = _fixture(tmp)
        try:
            clean = _median_ms(client, COUNT_SQL)
            client.execute("UPDATE t SET k = 5 WHERE k = 10")
            client.flush()
            client.close()

            client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
            client.use_table("t")
            assert _path(client, COUNT_SQL) == "fast_cached_group_by"
            after = _median_ms(client, COUNT_SQL)
            # The generic route this used to take costs ~35x the cached lane.
            assert after < clean * 4 + 0.5, (clean, after)
            assert _scanned_counts(client) == _python_counts(client)
        finally:
            client.close()


def _median_ms(client, sql, iterations=7):
    for _ in range(3):
        client.execute(sql)
    samples = []
    for _ in range(iterations):
        start = time.perf_counter()
        client.execute(sql)
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def _python_sums(client):
    sums = {}
    for row in client.execute("SELECT category, k FROM t").to_dict():
        if row["k"] is None:
            continue
        sums[row["category"]] = sums.get(row["category"], 0) + row["k"]
    return sums


def test_sum_and_avg_are_repaired_for_a_pending_cell_update():
    with tempfile.TemporaryDirectory() as tmp:
        client = _fixture(tmp)
        try:
            assert _path(client, SUM_SQL) == "fast_cached_group_by"
            client.execute("UPDATE t SET k = 7 WHERE k = 30")
            client.flush()
            # The aggregate is sum-based, so the lane stays *and* the sums must be
            # the updated ones -- 0.14 ms against the generic route's 2.7 ms.
            assert _path(client, SUM_SQL) == "fast_cached_group_by"
            scanned = {row["category"]: row["s"]
                       for row in client.execute(
                           "SELECT category, SUM(k) AS s FROM t GROUP BY category"
                       ).to_dict()}
            assert scanned == _python_sums(client)

            averaged = {row["category"]: row["a"]
                        for row in client.execute(
                            "SELECT category, AVG(k) AS a FROM t GROUP BY category"
                        ).to_dict()}
            expected = {}
            counts = {}
            for row in client.execute("SELECT category, k FROM t").to_dict():
                if row["k"] is None:
                    continue
                expected[row["category"]] = expected.get(row["category"], 0) + row["k"]
                counts[row["category"]] = counts.get(row["category"], 0) + 1
            for label, total in expected.items():
                assert abs(averaged[label] - total / counts[label]) < 1e-6, label
        finally:
            client.close()


def test_min_max_and_count_of_a_column_still_decline():
    with tempfile.TemporaryDirectory() as tmp:
        client = _fixture(tmp)
        try:
            client.execute("UPDATE t SET k = 7 WHERE k = 30")
            client.flush()
            # Neither can be repaired from a sum, so both must refuse the lane -- and
            # still answer correctly.
            for function in ("MIN", "MAX", "COUNT"):
                sql = (f"SELECT category, {function}(k) AS v FROM t GROUP BY category "
                       "ORDER BY v DESC")
                assert _path(client, sql) != "fast_cached_group_by", function
            values = [row["k"] for row in client.execute("SELECT k FROM t").to_dict()
                      if row["k"] is not None]
            assert min(values) == 0 and max(values) == ROWS - 1
        finally:
            client.close()


def test_a_null_transition_declines():
    with tempfile.TemporaryDirectory() as tmp:
        client = _fixture(tmp)
        try:
            client.execute("UPDATE t SET k = NULL WHERE k = 40")
            client.flush()
            # Nullness moves COUNT(col) and the repair refuses NULLs, so the lane must
            # decline; the sum has to drop that row.
            assert _path(client, SUM_SQL) != "fast_cached_group_by"
            scanned = {row["category"]: row["s"]
                       for row in client.execute(
                           "SELECT category, SUM(k) AS s FROM t GROUP BY category"
                       ).to_dict()}
            assert scanned == _python_sums(client)
        finally:
            client.close()
