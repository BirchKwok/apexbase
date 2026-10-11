"""Projection read paths and DeltaStore overlay visibility.

These pin the SQL-visible behaviour of the two batch builders: a projection
without `_id`, a projection that requests `_id`, and the visibility of a cell
update through both the projection path and the batch (group-by) path. The
overlay locates rows by their physical ID, so any scan that starts with a
pending delta must apply it no matter which columns were projected.
"""

import os
import shutil
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 20_000


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    c = ApexClient(root, enable_cache=False)
    c.create_table("t", schema={"a": "int64", "b": "string"})
    c.use_table("t")
    c.store({"a": list(range(ROWS)), "b": [f"v{i % 17}" for i in range(ROWS)]})
    c.flush()
    try:
        yield c
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)


def test_projection_without_id_still_returns_every_row(client):
    assert client.execute("SELECT COUNT(a) AS n FROM t").to_dict() == [{"n": ROWS}]
    rows = client.execute("SELECT a, b FROM t ORDER BY a LIMIT 3").to_dict()
    assert rows == [
        {"a": 0, "b": "v0"},
        {"a": 1, "b": "v1"},
        {"a": 2, "b": "v2"},
    ]
    assert client.execute("SELECT SUM(a) AS s FROM t").to_dict() == [
        {"s": ROWS * (ROWS - 1) // 2}
    ]


def test_id_projection_still_returns_reconstructed_ids(client):
    rows = client.execute("SELECT _id, a FROM t ORDER BY _id LIMIT 3").to_dict()
    assert rows == [
        {"_id": 1, "a": 0},
        {"_id": 2, "a": 1},
        {"_id": 3, "a": 2},
    ]
    assert client.execute(
        "SELECT _id, a FROM t ORDER BY _id DESC LIMIT 1"
    ).to_dict() == [{"_id": ROWS, "a": ROWS - 1}]


def test_projection_sees_a_preexisting_update(client):
    client.execute("UPDATE t SET a = 999999 WHERE _id = 7")
    # Projection without `_id`.
    assert client.execute("SELECT a FROM t WHERE _id = 7").to_dict() == [{"a": 999999}]
    assert client.execute("SELECT a, b FROM t ORDER BY a DESC LIMIT 1").to_dict() == [
        {"a": 999999, "b": "v6"}
    ]
    # Projection with `_id`.
    assert client.execute("SELECT _id, a FROM t WHERE _id = 7").to_dict() == [
        {"_id": 7, "a": 999999}
    ]


def test_group_by_sees_a_preexisting_update(client):
    client.execute("UPDATE t SET b = 'zzz' WHERE _id = 8")
    grouped = {
        row["b"]: row["n"]
        for row in client.execute(
            "SELECT b, COUNT(*) AS n FROM t GROUP BY b"
        ).to_dict()
    }
    assert grouped["zzz"] == 1
    # `b` is `v{i % 17}`; row id 8 has i = 7, so one `v7` moved to `zzz`.
    assert grouped["v7"] == sum(1 for i in range(ROWS) if i % 17 == 7) - 1
    assert client.execute(
        "SELECT SUM(a) AS s FROM t WHERE b = 'zzz'"
    ).to_dict() == [{"s": 7}]
