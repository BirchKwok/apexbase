"""FTS ``MATCH()`` row-selection pushdown (V5 §6.4 item 15).

A ``MATCH()`` resolves to a row-id bitmap. The SQL executor now uses it to
restrict the scan to the documents it selects instead of materializing every row
and testing ``_id`` membership per row. These tests pin the parity with the
scan-only shape, the shapes that must stay on the scan (``OR``/``NOT``, an absent
term, a majority hit set, an overlay), the vector tables that now push down
through the position reader, and the physical-path trace.
"""

import shutil
import statistics
import tempfile
import time

import pytest

from apexbase import ApexClient

ROWS = 20_000
NEEDLE = "selectorterm"
NEEDLE_EVERY = 97


def _needle_docs():
    return {i for i in range(ROWS) if i % NEEDLE_EVERY == 0}


def _path(client, sql):
    plan = client.execute("EXPLAIN ANALYZE " + sql).to_dict()[0]["plan"]
    assert "Actual Path:" in plan, plan
    return plan.split("Actual Path:", 1)[1].splitlines()[0].strip()


def _schema(client, sql):
    """Column names the statement reports, independent of how many rows match."""
    result = client._storage.execute(sql)
    columns = result.get("columns_dict") if isinstance(result, dict) else None
    assert columns is not None, result
    return sorted(columns)


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    apex = ApexClient(root, drop_if_exists=True, enable_cache=False)
    apex.create_table("t", {"doc": "int64", "body": "string", "k": "int64"})
    apex.use_table("t")
    for start in range(0, ROWS, 5_000):
        end = min(start + 5_000, ROWS)
        apex.store(
            {
                "doc": list(range(start, end)),
                "body": [
                    f"alpha beta {NEEDLE} gamma doc{i}" if i % NEEDLE_EVERY == 0
                    else f"alpha beta delta epsilon doc{i}"
                    for i in range(start, end)
                ],
                "k": [i % 1000 for i in range(start, end)],
            }
        )
    apex.flush()
    apex.init_fts(index_fields=["body"])
    try:
        yield apex
    finally:
        apex.close()
        shutil.rmtree(root, ignore_errors=True)


def test_selective_match_restricts_the_scan_and_matches_it(client):
    push = f"SELECT doc, k FROM t WHERE MATCH('{NEEDLE}')"
    # `OR 1 = 0` keeps the result identical while making the predicate unusable
    # as a row selection, so it is the scan baseline for every parity check.
    scan = push + " OR 1 = 0"
    selected = {int(row["doc"]) for row in client.execute(push).to_dict()}
    scanned = {int(row["doc"]) for row in client.execute(scan).to_dict()}
    assert selected == _needle_docs()
    assert scanned == selected, "the row selection changed the result set"
    assert _path(client, push) == "fts_row_selection"
    assert _path(client, scan) == "generic_executor"


def test_star_count_ordering_and_combined_predicates_stay_correct(client):
    push = f"SELECT * FROM t WHERE MATCH('{NEEDLE}')"
    assert {int(row["doc"]) for row in client.execute(push).to_dict()} == _needle_docs()

    count = f"SELECT count(*) AS n FROM t WHERE MATCH('{NEEDLE}')"
    assert int(client.execute(count).to_dict()[0]["n"]) == len(_needle_docs())
    assert int(
        client.execute(count + " OR 1 = 0").to_dict()[0]["n"]
    ) == len(_needle_docs())

    combined = f"SELECT doc FROM t WHERE MATCH('{NEEDLE}') AND k < 500"
    combined_scan = combined + " OR 1 = 0"
    a = {int(row["doc"]) for row in client.execute(combined).to_dict()}
    b = {int(row["doc"]) for row in client.execute(combined_scan).to_dict()}
    assert a == b == {doc for doc in _needle_docs() if doc % 1000 < 500}

    ordered = f"SELECT doc FROM t WHERE MATCH('{NEEDLE}') ORDER BY doc DESC LIMIT 5"
    ordered_scan = (
        f"SELECT doc FROM t WHERE MATCH('{NEEDLE}') OR 1 = 0 ORDER BY doc DESC LIMIT 5"
    )
    assert client.execute(ordered).to_dict() == client.execute(ordered_scan).to_dict()

    grouped = f"SELECT k, count(*) AS n FROM t WHERE MATCH('{NEEDLE}') GROUP BY k"
    grouped_scan = (
        f"SELECT k, count(*) AS n FROM t WHERE MATCH('{NEEDLE}') OR 1 = 0 GROUP BY k"
    )
    assert client.execute(grouped).to_dict() == client.execute(grouped_scan).to_dict()


def test_absent_and_majority_hit_sets_keep_the_scan(client):
    # Every document carries "alpha", so the bitmap covers the whole table and
    # reading it row by row would be strictly worse than the scan.
    majority = f"SELECT count(*) AS n FROM t WHERE MATCH('alpha')"
    assert int(client.execute(majority).to_dict()[0]["n"]) == ROWS
    assert (
        _path(client, "SELECT doc FROM t WHERE MATCH('alpha')") == "generic_executor"
    )

    # An empty hit set is a definite answer, but it must still carry the scan's
    # schema rather than the per-id reader's empty one. It used to decline to the
    # scan to get that schema, which made an absent term the *slow* case: measured
    # on 200k documents, `MATCH('zzzz')` cost 1.24 ms against 0.48 ms when the term
    # matched 25k rows. The row-selection lane now answers with the same columns and
    # no rows, so the invariant to pin is schema equality with the scan, not the
    # path it took.
    absent = "SELECT doc FROM t WHERE MATCH('nosuchtermxyz')"
    assert client.execute(absent).to_dict() == []
    assert _path(client, absent) == "fts_row_selection"
    # The schema comes back with the projected columns, not the per-id reader's
    # empty one (which is what this case had to avoid by declining to the scan).
    assert "doc" in _schema(client, absent)
    assert "body" in _schema(client, "SELECT body FROM t WHERE MATCH('nosuchtermxyz')")
    assert client.execute(
        "SELECT count(*) AS n FROM t WHERE MATCH('nosuchtermxyz')"
    ).to_dict() == [{"n": 0}]


def test_or_and_not_shapes_keep_the_scan(client):
    assert (
        _path(client, f"SELECT doc FROM t WHERE MATCH('{NEEDLE}') OR k = -1")
        == "generic_executor"
    )
    assert (
        _path(client, f"SELECT doc FROM t WHERE NOT MATCH('{NEEDLE}')")
        == "generic_executor"
    )


def test_overlay_falls_back_and_observes_the_update(client):
    # A pending DeltaStore cell makes the persisted file an incomplete view.
    client.execute("UPDATE t SET k = 424242 WHERE _id = 1")
    push = f"SELECT doc, k FROM t WHERE MATCH('{NEEDLE}')"
    rows = client.execute(push).to_dict()
    assert {int(row["doc"]) for row in rows} == _needle_docs()
    assert next(int(row["k"]) for row in rows if int(row["doc"]) == 0) == 424242
    assert _path(client, push) == "generic_executor"

    # A delete must stay visible to both shapes (the bitmap may still carry the
    # id, so parity with the scan is the invariant).
    client.execute("DELETE FROM t WHERE _id = 98")
    scan = push + " OR 1 = 0"
    assert client.execute(push).to_dict() == client.execute(scan).to_dict()


def test_vector_tables_use_the_position_reader(client):
    # The scalar per-id reader cannot stage a vector column, which used to keep
    # every vector-bearing table on the scan. Row positions plus the indexed
    # reader decode only the requested columns, so `MATCH()` pushes down on them
    # too and the vector values must come back untouched.
    rows = 2_000
    vectors = [
        [float(i), float(i) + 1.0, float(i) + 2.0, float(i) + 3.0] for i in range(rows)
    ]
    client.create_table("v", {"doc": "int64", "body": "string", "emb": "float32_vector"})
    client.use_table("v")
    for start in range(0, rows, 500):
        end = min(start + 500, rows)
        client.store(
            {
                "doc": list(range(start, end)),
                "body": [
                    f"alpha {NEEDLE} v{i}" if i % NEEDLE_EVERY == 0 else f"alpha v{i}"
                    for i in range(start, end)
                ],
                "emb": vectors[start:end],
            }
        )
    client.flush()
    client.init_fts(index_fields=["body"])

    expected = [i for i in range(rows) if i % NEEDLE_EVERY == 0]
    push = f"SELECT doc, emb FROM v WHERE MATCH('{NEEDLE}')"
    scan = push + " OR 1 = 0"
    got = client.execute(push).to_dict()
    assert [int(row["doc"]) for row in got] == expected
    for row in got:
        assert row["emb"] == pytest.approx(vectors[int(row["doc"])]), row
    assert _path(client, push) == "fts_row_selection"
    assert client.execute(scan).to_dict() == got

    # `SELECT *` must keep the whole schema, vector column included.
    star = client.execute(f"SELECT * FROM v WHERE MATCH('{NEEDLE}')").to_dict()
    assert [int(row["doc"]) for row in star] == expected
    for row in star:
        assert row["emb"] == pytest.approx(vectors[int(row["doc"])]), row


def test_a_two_row_table_keeps_the_scan(client):
    # The hit ratio gate (hits * 3 <= rows) keeps the scan when the bitmap is not
    # a minority; the schema alone no longer decides.
    client.create_table("v", {"doc": "int64", "body": "string", "emb": "float32_vector"})
    client.use_table("v")
    client.store(
        {
            "doc": [0, 1],
            "body": [f"x {NEEDLE}", "y"],
            "emb": [[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]],
        }
    )
    client.flush()
    client.init_fts(index_fields=["body"])
    sql = f"SELECT * FROM v WHERE MATCH('{NEEDLE}')"
    rows = client.execute(sql).to_dict()
    assert len(rows) == 1
    assert rows[0]["emb"] == [1.0, 2.0, 3.0, 4.0], rows[0]
    assert _path(client, sql) == "generic_executor"


def test_an_absent_term_is_not_the_slow_case(client):
    """Not finding a term must not cost more than finding hundreds of documents.

    The empty bitmap used to decline to the scan only to borrow its schema, so the
    *absent* term was the expensive one (200k documents: 1.24 ms against 0.48 ms
    for a 25k-hit term). The row-selection lane answers an empty bitmap directly
    now, so the ordering has to be "fewer hits, less work".
    """

    def median_ms(sql, iterations=9):
        for _ in range(3):
            client._storage.execute(sql)
        samples = []
        for _ in range(iterations):
            start = time.perf_counter()
            client._storage.execute(sql)
            samples.append((time.perf_counter() - start) * 1000.0)
        return statistics.median(samples)

    absent = median_ms("SELECT count(*) AS n FROM t WHERE MATCH('nosuchtermxyz')")
    hits = median_ms(f"SELECT count(*) AS n FROM t WHERE MATCH('{NEEDLE}')")
    assert absent <= hits * 2 + 0.05, (absent, hits)
    # And the empty answer still carries the table's columns.
    assert "doc" in _schema(client, "SELECT doc FROM t WHERE MATCH('nosuchtermxyz')")


def test_a_paginated_match_stages_only_the_page(client):
    """`MATCH()` + `LIMIT` must stage the page, not the term's posting list.

    The row-selection lane staged every matching row and left the LIMIT to the
    shared tail, so a ten-row page cost the term's document frequency: on 200k
    documents a 25k-hit term cost 0.88 ms and a 50k-hit term 1.77 ms, against
    0.04 ms once the selection is bounded by the page. Every document in this
    fixture carries "alpha", which makes it the dense case.
    """
    dense = "SELECT doc FROM t WHERE MATCH('alpha')"
    assert [int(row["doc"]) for row in client.execute(dense + " LIMIT 10").to_dict()] == list(
        range(10)
    )
    # OFFSET keeps counting in scan order, and pages past the end are empty.
    assert [
        int(row["doc"])
        for row in client.execute(dense + " LIMIT 5 OFFSET 5").to_dict()
    ] == list(range(5, 10))
    assert client.execute(dense + f" LIMIT 5 OFFSET {ROWS + 3}").to_dict() == []

    # Shapes where truncating the selection would be wrong must be untouched.
    assert client.execute(
        "SELECT count(*) AS n FROM t WHERE MATCH('alpha')"
    ).to_dict() == [{"n": ROWS}]
    assert client.execute(
        "SELECT count(*) AS n FROM t WHERE MATCH('alpha') LIMIT 3"
    ).to_dict() == [{"n": ROWS}]
    assert [
        int(row["doc"])
        for row in client.execute(
            "SELECT doc FROM t WHERE MATCH('alpha') AND doc < 3 LIMIT 5"
        ).to_dict()
    ] == [0, 1, 2]
    assert [
        int(row["doc"])
        for row in client.execute(
            "SELECT doc FROM t WHERE MATCH('alpha') ORDER BY doc DESC LIMIT 5"
        ).to_dict()
    ] == [ROWS - 1, ROWS - 2, ROWS - 3, ROWS - 4, ROWS - 5]

    def median_ms(sql, iterations=9):
        for _ in range(3):
            client._storage.execute(sql)
        samples = []
        for _ in range(iterations):
            start = time.perf_counter()
            client._storage.execute(sql)
            samples.append((time.perf_counter() - start) * 1000.0)
        return statistics.median(samples)

    page = median_ms(dense + " LIMIT 10")
    scan = median_ms("SELECT doc FROM t")
    assert page < scan * 0.5 + 0.5, (page, scan)
