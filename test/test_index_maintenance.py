"""Secondary indexes must stay in step with every committed write path.

The typed and columnar Python fast paths (`store` of one row, a list of rows or a
columnar dict) published their rows without touching the secondary indexes, so an
indexed equality returned `[]` while a scan returned the row — silent row loss
with a *trusted* index, because no stale marker was written either.

`REINDEX` hid a second defect behind the first: it cleared only the index
instances that happened to be loaded, so an unloaded index re-read its stale
postings *during* the rebuild and posted every row twice, and an indexed lookup
returned duplicate rows until the next `REINDEX`.
"""

import shutil
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 1_000
APPENDED = 500_001


def _fixture(indexed=True):
    root = tempfile.mkdtemp(prefix="apexbase_index_maintenance_")
    client = ApexClient(root, drop_if_exists=True, enable_cache=False)
    client.create_table("t", schema={"a": "int64", "s": "string"})
    client.use_table("t")
    client.store({"a": list(range(ROWS)), "s": [f"v{i % 10}" for i in range(ROWS)]})
    client.flush()
    if indexed:
        client.execute("CREATE INDEX idx_a ON t(a)")
    return root, client


def _values(client, sql):
    return [row["a"] for row in client.execute(sql).to_dict()]


def _appends(shape):
    """One appended row through each public write shape."""
    return {
        "single row": lambda client: client.store({"a": APPENDED, "s": "n"}),
        "list of rows": lambda client: client.store(
            [{"a": APPENDED, "s": "n"}, {"a": APPENDED + 1, "s": "n"}]
        ),
        "columnar dict": lambda client: client.store(
            {"a": [APPENDED, APPENDED + 1], "s": ["n", "n"]}
        ),
        "store_batch": lambda client: client._storage.store_batch(
            [{"a": APPENDED, "s": "n"}, {"a": APPENDED + 1, "s": "n"}]
        ),
        "SQL INSERT": lambda client: client.execute(
            f"INSERT INTO t (a, s) VALUES ({APPENDED}, 'n')"
        ),
    }[shape]


@pytest.mark.parametrize(
    "shape", ["single row", "list of rows", "columnar dict", "store_batch", "SQL INSERT"]
)
def test_every_write_shape_keeps_the_index_in_step(shape):
    root, client = _fixture()
    try:
        _appends(shape)(client)
        # Indexed equality must see every appended row, and the base rows stay put.
        assert APPENDED in _values(client, f"SELECT a FROM t WHERE a >= {APPENDED}")
        assert _values(client, f"SELECT a FROM t WHERE a = {APPENDED}") == [APPENDED]
        assert _values(client, "SELECT a FROM t WHERE a = 500") == [500]
        if shape not in ("single row", "SQL INSERT"):
            # Two rows went in through the batch shapes.
            assert _values(client, f"SELECT a FROM t WHERE a = {APPENDED + 1}") == [
                APPENDED + 1
            ]
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_reindex_does_not_duplicate_postings():
    root, client = _fixture()
    try:
        _appends("SQL INSERT")(client)
        # No index read before REINDEX: the unloaded index used to be left with
        # its stale postings and the rebuild posted every row a second time.
        client.execute("REINDEX t")
        assert _values(client, f"SELECT a FROM t WHERE a = {APPENDED}") == [APPENDED]
        assert _values(client, "SELECT a FROM t WHERE a = 500") == [500]
        # ... and the same after the index has been read into memory.
        client.execute("REINDEX t")
        assert _values(client, f"SELECT a FROM t WHERE a = {APPENDED}") == [APPENDED]
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)


def test_reindex_after_every_write_shape_stays_exact():
    for shape in ["single row", "list of rows", "columnar dict", "store_batch"]:
        root, client = _fixture()
        try:
            _appends(shape)(client)
            client.execute("REINDEX t")
            got = _values(client, f"SELECT a FROM t WHERE a >= {APPENDED}")
            expected = (
                [APPENDED] if shape == "single row" else [APPENDED, APPENDED + 1]
            )
            assert sorted(got) == sorted(expected), shape
        finally:
            client.close()
            shutil.rmtree(root, ignore_errors=True)


def test_writes_without_indexes_are_unaffected():
    root, client = _fixture(indexed=False)
    try:
        _appends("single row")(client)
        assert _values(client, f"SELECT a FROM t WHERE a = {APPENDED}") == [APPENDED]
        client.execute(f"INSERT INTO t (a, s) VALUES ({APPENDED + 1}, 'n')")
        assert _values(client, f"SELECT a FROM t WHERE a = {APPENDED + 1}") == [APPENDED + 1]
    finally:
        client.close()
        shutil.rmtree(root, ignore_errors=True)
