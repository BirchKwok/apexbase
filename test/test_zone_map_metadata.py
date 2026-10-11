"""
Zone-map metadata written by the append path.

Row groups appended to an existing table used to be published with RCIX offsets
but *no* zone maps, so every incrementally written row group was unprunable.
They now carry zone maps built from the same rows and the same null bitmaps as
their column bodies, which also makes the persisted ``has_nulls`` flag real
readers treat a present zone-map entry as authoritative for NULL presence.

These tests are differential: the same data is written once in bulk and once as
many small appends, and every query must return the same rows. That pins the
pruning metadata to the data without depending on internal file layout.
"""

import os
import shutil
import tempfile

import pytest

from apexbase import ApexClient

COLUMNS = {"num": "int64", "score": "float64", "label": "string"}

# Deliberately spans a wide numeric range, a narrow one, and repeated strings so
# zone maps have something to prune on either end.
ROWS = [
    (i, float(i) * 1.5, f"label_{i % 7}")
    for i in range(300)
]


def _open(root, table):
    client = ApexClient(root)
    client.create_table(table, schema=COLUMNS)
    client.use_table(table)
    return client


def _columns(rows):
    return {
        "num": [r[0] for r in rows],
        "score": [r[1] for r in rows],
        "label": [r[2] for r in rows],
    }


@pytest.fixture
def bulk_client():
    """The whole dataset written in one columnar store()."""
    root = tempfile.mkdtemp()
    client = _open(root, "bulk")
    client.store(_columns(ROWS))
    client.flush()
    try:
        yield client
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def appended_client():
    """The same dataset written as many small appends."""
    root = tempfile.mkdtemp()
    client = _open(root, "appended")
    for start in range(0, len(ROWS), 7):
        chunk = ROWS[start:start + 7]
        client.store(_columns(chunk))
        client.flush()
    try:
        yield client
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


QUERIES = [
    "SELECT num, score, label FROM {t} WHERE num = {n} ORDER BY num",
    "SELECT num FROM {t} WHERE num BETWEEN {lo} AND {hi} ORDER BY num",
    "SELECT num FROM {t} WHERE num > {lo} ORDER BY num DESC LIMIT 9",
    "SELECT num FROM {t} WHERE num < {hi} ORDER BY num LIMIT 9",
    "SELECT score FROM {t} WHERE score BETWEEN {lo_f} AND {hi_f} ORDER BY score",
    "SELECT label FROM {t} WHERE label = '{label}' ORDER BY num",
    "SELECT COUNT(*) AS c, SUM(num) AS s FROM {t} WHERE num BETWEEN {lo} AND {hi}",
    "SELECT num FROM {t} ORDER BY num DESC LIMIT 5",
]


def _run(client, table, params):
    out = []
    for sql in QUERIES:
        rows = client.execute(sql.format(t=table, **params)).to_dict()
        out.append(rows)
    return out


PARAMS = {
    "n": 123,
    "lo": 100,
    "hi": 200,
    "lo_f": 150.0,
    "hi_f": 300.0,
    "label": "label_3",
}


class TestAppendedRowGroupZoneMaps:
    def test_queries_agree_with_bulk_written_table(self, bulk_client, appended_client):
        bulk = _run(bulk_client, "bulk", PARAMS)
        appended = _run(appended_client, "appended", PARAMS)
        for sql, b, a in zip(QUERIES, bulk, appended):
            assert a == b, f"append/bulk mismatch for {sql}"

    def test_reopen_after_appends_keeps_answers_stable(self):
        # A reopen re-reads the footer written by the append path, so the
        # appended row groups' zone maps have to survive a fresh open.
        root = tempfile.mkdtemp()
        client = _open(root, "appended")
        try:
            for start in range(0, len(ROWS), 7):
                client.store(_columns(ROWS[start:start + 7]))
                client.flush()
            before = _run(client, "appended", PARAMS)
        finally:
            client.close()

        reopened = ApexClient(root)
        reopened.use_table("appended")
        try:
            assert _run(reopened, "appended", PARAMS) == before
        finally:
            reopened.close()
            shutil.rmtree(root, ignore_errors=True)

    def test_nulls_in_appended_row_groups_are_ordered_correctly(self):
        root = tempfile.mkdtemp()
        client = _open(root, "null_rows")
        try:
            # Two appends: the first is null-free, the second carries the NULLs,
            # so the second row group's zone maps are the interesting ones.
            client.store({"num": [1, 2, 3], "score": [1.0, 2.0, 3.0],
                          "label": ["a", "b", "c"]})
            client.flush()
            client.store({"num": [None, None, 9], "score": [None, None, 9.0],
                          "label": [None, None, "z"]})
            client.flush()

            asc = [r["num"] for r in client.execute(
                "SELECT * FROM null_rows ORDER BY num LIMIT 5").to_dict()]
            assert asc == [1, 2, 3, 9, None]

            desc = [r["num"] for r in client.execute(
                "SELECT * FROM null_rows ORDER BY num DESC LIMIT 2").to_dict()]
            assert desc == [None, None]

            nulls_first = [r["num"] for r in client.execute(
                "SELECT * FROM null_rows ORDER BY num ASC NULLS FIRST LIMIT 2").to_dict()]
            assert nulls_first == [None, None]

            # Filtering on the null-carrying appended group must use the values
            # actually present there.
            assert client.execute(
                "SELECT num FROM null_rows WHERE num > 5 ORDER BY num").to_dict() == [{"num": 9}]
        finally:
            client.close()
            shutil.rmtree(root, ignore_errors=True)

    def test_appends_crossing_a_row_group_boundary_stay_correct(self):
        root = tempfile.mkdtemp()
        client = _open(root, "wide")
        try:
            # 70k rows in one append crosses the 65,536-row row-group boundary,
            # so the same call publishes more than one row group's metadata.
            total = 70_000
            client.store({"num": list(range(total)),
                          "score": [float(i) for i in range(total)],
                          "label": [f"l{i % 11}" for i in range(total)]})
            client.flush()
            assert client.execute(
                "SELECT COUNT(*) AS c FROM wide WHERE num BETWEEN 65000 AND 65010"
            ).to_dict() == [{"c": 11}]
            assert client.execute(
                "SELECT COUNT(*) AS c FROM wide WHERE label = 'l3'"
            ).to_dict() == [{"c": (total + 8) // 11}]
        finally:
            client.close()
            shutil.rmtree(root, ignore_errors=True)
