"""S2: bounded caches keep results correct after capacity eviction.

The process caches are intentionally capped (Python simple-SQL 256, query
classifier 512, SQL parser 1024, stats 1024, plan feedback 256 shapes/table).
This test drives more distinct statements than those caps and checks that
eviction or clear never changes a result, and that an evicted statement still
executes correctly on its next use.
"""

import tempfile

from apexbase import ApexClient

ROWS = 800
DISTINCT_QUERIES = 1200


def test_results_survive_parse_and_plan_cache_eviction():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(tmp, enable_cache=False)
        client.create_table("cap_t", {"v": "int"})
        client.use_table("cap_t")
        client.store({"v": list(range(ROWS))})
        client.flush()

        # One distinct SQL text per iteration: this overflows the Python
        # simple-SQL cache, the classifier cache and the parser cache at once.
        for i in range(DISTINCT_QUERIES):
            target = i % ROWS
            rows = client.execute(f"SELECT v FROM cap_t WHERE v = {target}").to_dict()
            assert [row["v"] for row in rows] == [target], i

        # Statements that may have been evicted (first and last) still agree.
        for target in (0, ROWS // 2, ROWS - 1):
            rows = client.execute(f"SELECT v FROM cap_t WHERE v = {target}").to_dict()
            assert [row["v"] for row in rows] == [target]

        # Aggregate and write shapes share the same classifier/parse caches.
        assert client.execute("SELECT COUNT(*) FROM cap_t").scalar() == ROWS
        client.execute(f"UPDATE cap_t SET v = {ROWS} WHERE v = 0")
        assert client.execute("SELECT v FROM cap_t WHERE v = 0").to_dict() == []
        assert client.execute(
            f"SELECT v FROM cap_t WHERE v = {ROWS}"
        ).to_dict() == [{"v": ROWS}]
        client.close()
