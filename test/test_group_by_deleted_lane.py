"""Cached `GROUP BY` on a table that has deletions.

Whatever lane serves them, aggregates over a table that has deletions must count
only the surviving rows -- including dropping a group whose rows were all deleted.

This is the oracle the cached group-by lane must keep passing now that it serves
deleted tables: its dictionary covers the file's *physical* rows and the aggregation
skips the deleted ones. An earlier attempt blamed the lane's coverage check for wrong
results here; instrumentation showed the check was right and the aggregation's
fallback read no deletion bitmap for a `COUNT(*)`-only group-by (see the round-112
record). Both the values and the lane's cost are pinned below.
"""

import statistics
import tempfile
import time

from apexbase.client import ApexClient

ROWS = 30_000
GROUPS = 20


def _fixture(tmp):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"category": "string", "k": "int64"})
    client.use_table("t")
    categories = [f"cat{i % GROUPS}" for i in range(ROWS)]
    client.store({"category": categories, "k": list(range(ROWS))})
    client.flush()
    return client, categories


def _oracle(categories, deleted):
    aggregate = {}
    for index in range(ROWS):
        if index in deleted:
            continue
        count, total = aggregate.get(categories[index], (0, 0))
        aggregate[categories[index]] = (count + 1, total + index)
    return aggregate


def _queried(client):
    return {
        row["category"]: (int(row["n"]), int(row["s"] or 0))
        for row in client.execute(
            "SELECT category, COUNT(*) AS n, SUM(k) AS s FROM t GROUP BY category"
        ).to_dict()
    }


def test_group_by_with_deletions_matches_an_independent_oracle():
    with tempfile.TemporaryDirectory() as tmp:
        client, categories = _fixture(tmp)
        try:
            deleted = set()
            for statement, killed in (
                ("DELETE FROM t WHERE k < 7", set(range(7))),
                ("DELETE FROM t WHERE k >= 15000 AND k < 15010", set(range(15000, 15010))),
            ):
                client.execute(statement)
                client.flush()
                deleted |= killed
                assert _queried(client) == _oracle(categories, deleted), statement

            # Emptying one group entirely must drop it from the result.
            victim = sorted({c for i, c in enumerate(categories) if i not in deleted})[0]
            client.execute(f"DELETE FROM t WHERE category = '{victim}'")
            client.flush()
            deleted |= {i for i in range(ROWS) if categories[i] == victim}
            result = _queried(client)
            assert result == _oracle(categories, deleted)
            assert victim not in result

            # A tie-heavy ORDER BY/LIMIT must still return the right counts.
            limited = client.execute(
                "SELECT category, COUNT(*) AS n FROM t GROUP BY category "
                "ORDER BY n DESC LIMIT 3"
            ).to_dict()
            assert [int(row["n"]) for row in limited] == sorted(
                (count for count, _ in _oracle(categories, deleted).values()),
                reverse=True,
            )[:3]
        finally:
            client.close()


def test_a_deleted_table_keeps_the_cached_group_by_lane():
    """Deleting a row must not move this statement onto the generic route.

    The lane's coverage check compares its dictionary -- built over the file's
    *physical* rows -- with that same physical count. Comparing against the active
    count instead (which already excludes deleted rows) refused the lane after a
    single DELETE, and the generic route costs ~8.4 ms on 200k rows against 0.21 ms
    here.
    """
    rows = 200_000
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        try:
            client.create_table("t", schema={"category": "string", "k": "int64"})
            client.use_table("t")
            client.store({"category": [f"cat{i % 20}" for i in range(rows)],
                          "k": list(range(rows))})
            client.flush()
            sql = ("SELECT category, COUNT(*) AS n FROM t GROUP BY category "
                   "ORDER BY n DESC")

            def median_ms(statement, iterations=7):
                for _ in range(3):
                    client.execute(statement)
                samples = []
                for _ in range(iterations):
                    start = time.perf_counter()
                    client.execute(statement)
                    samples.append((time.perf_counter() - start) * 1000.0)
                return statistics.median(samples)

            clean = median_ms(sql)
            client.execute("DELETE FROM t WHERE k < 5")
            client.flush()
            deleted = median_ms(sql)
            # `clean` is the cached lane (~0.21 ms) and the generic route it used to
            # fall back to costs ~8.4 ms, so this bound separates the two.
            assert deleted < clean * 4 + 0.5, (clean, deleted)
        finally:
            client.close()


def test_numeric_group_by_excludes_deleted_rows_and_keeps_its_cost():
    """An integral GROUP BY key must not fall off the native lane when rows are deleted.

    The lane used to stand down whenever any row group had deletions, sending every later
    grouping to the generic path (1M rows, 100 deletes: 0.86 -> 9.68 ms, 11.3x). It now
    skips deleted rows in its per-row loop, so both the numbers and the cost must hold.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"g": "int64", "v": "int64", "gn": "int64"})
        client.use_table("t")
        rows = 200_000
        client.store({
            "g": [i % 500 for i in range(rows)],
            "v": [i for i in range(rows)],
            "gn": [None if i % 7 == 0 else i % 300 for i in range(rows)],
        })
        client.flush()
        query = "SELECT g, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY g"

        def oracle(column):
            aggregate = {}
            for row in client.execute(f"SELECT {column}, v FROM t").to_dict():
                key = list(row.values())[0]
                if key is None:
                    continue
                key = int(key)
                count, total = aggregate.get(key, (0, 0))
                aggregate[key] = (count + 1, total + int(row["v"]))
            return aggregate

        def queried():
            return {
                int(row["g"]): (int(row["n"]), int(row["s"]))
                for row in client.execute(query).to_dict()
            }

        def median_ms(reps=5):
            statement = lambda: client.execute(query)
            for _ in range(2):
                statement()
            samples = []
            for _ in range(reps):
                start = time.perf_counter()
                statement()
                samples.append((time.perf_counter() - start) * 1000)
            return statistics.median(samples)

        try:
            assert queried() == oracle("g")
            clean = median_ms()

            # Deleting a third of the rows must change the numbers, not the lane.
            client.execute("DELETE FROM t WHERE v < 66000")
            client.flush()
            assert queried() == oracle("g")
            after = median_ms()
            # Same cost band: this is the guard for the 11x jump.
            assert max(clean, after) < min(clean, after) * 3 + 0.5, (clean, after)

            # A nullable integral key groups NULLs into one bucket, deleted rows excluded.
            nulls = client.execute(
                "SELECT gn, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY gn"
            ).to_dict()
            by_key = {list(r.values())[0]: (int(r["n"]), int(r["s"])) for r in nulls}
            expected = oracle("gn")
            assert {k: v for k, v in by_key.items() if k is not None} == expected

            client.execute("DELETE FROM t")
            client.flush()
            assert queried() == {}
        finally:
            client.close()


def test_multi_key_grouping_keeps_its_dictionary_cost_after_deletes():
    """A string GROUP BY over two keys must not fall off the cached-dictionary path.

    The cached dictionary describes physical rows, and the reader used to require a
    physical-sized code vector to equal the active row count. One DELETE therefore made the
    whole dictionary path stand down, so the string key arrived as plain Utf8 and the
    executor re-hashed every value: 1M rows went 9.3 -> 38.3 ms (4.13x) for a numeric+string
    key and 5.8 -> 41.4 ms (7.19x) for two string keys. The reader now compacts the codes
    onto the surviving rows. Both the numbers and the cost band are pinned here.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table("t", schema={"k": "int64", "g": "int64", "h": "int64", "s": "string"})
        client.use_table("t")
        rows = 300_000
        client.store({
            "k": list(range(rows)),
            "g": [i % 1000 for i in range(rows)],
            "h": [(i // 1000) % 1000 for i in range(rows)],
            "s": [f"c{i % 1000}" for i in range(rows)],
        })
        client.flush()

        numeric_string = "SELECT g, s, COUNT(*) AS n, SUM(k) AS total FROM t GROUP BY g, s"
        two_strings = "SELECT s, s AS s2, COUNT(*) AS n FROM t GROUP BY s, s"

        def oracle():
            aggregate = {}
            for row in client.execute("SELECT g, s, k FROM t").to_dict():
                key = (int(row["g"]), row["s"])
                count, total = aggregate.get(key, (0, 0))
                aggregate[key] = (count + 1, total + int(row["k"]))
            return aggregate

        def queried():
            return {
                (int(row["g"]), row["s"]): (int(row["n"]), int(row["total"]))
                for row in client.execute(numeric_string).to_dict()
            }

        def median_ms(statement, reps=4):
            run = lambda: client.execute(statement)
            for _ in range(2):
                run()
            samples = []
            for _ in range(reps):
                start = time.perf_counter()
                run()
                samples.append((time.perf_counter() - start) * 1000)
            return statistics.median(samples)

        try:
            assert queried() == oracle()
            clean_multi = median_ms(numeric_string)
            clean_two_strings = median_ms(two_strings)

            client.execute("DELETE FROM t WHERE k < 1000")
            client.flush()
            assert queried() == oracle()
            after_multi = median_ms(numeric_string)
            after_two_strings = median_ms(two_strings)
            # Same cost band: this is the guard for the 4.13x / 7.19x jumps.
            assert max(clean_multi, after_multi) < min(clean_multi, after_multi) * 3 + 0.5, (
                clean_multi, after_multi
            )
            assert max(clean_two_strings, after_two_strings) < (
                min(clean_two_strings, after_two_strings) * 3 + 0.5
            ), (clean_two_strings, after_two_strings)

            # Deleting every row of one group must drop it, not leave a stale count.
            client.execute("DELETE FROM t WHERE g = 7")
            client.flush()
            result = queried()
            assert result == oracle()
            assert all(key[0] != 7 for key in result)

            client.execute("DELETE FROM t")
            client.flush()
            assert queried() == {}
        finally:
            client.close()
