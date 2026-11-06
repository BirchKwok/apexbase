"""Regression: DROP TABLE must also clear the table's registry ownership.

ApexBase 2.0.0 defect
---------------------
When every table in a database was dropped while its data file was still
awaiting the deferred unlink, the empty registry made the next ``CREATE TABLE``
perform the legacy "adopt ``*.apex`` files" backfill. That resurrected the other
dropped table's file as a registered table, so recreating the second name
failed::

    CREATE TABLE a (again)   -- ok; reaps a.apex and adopts b.apex during insert
    CREATE TABLE b (again)   -- RuntimeError: Table 'b' already exists

The regression pins the recreate-all sequence, the per-table data isolation, and
the close/reopen path (which already worked before the fix and must keep
working).
"""

from __future__ import annotations

import json
import os
import tempfile

from apexbase import ApexClient

SCHEMA = "(id BIGINT, name STRING, payload STRING)"


def _open(tmp):
    return ApexClient(
        dirpath=tmp,
        durability="fast",
        enable_cache=False,
        _auto_manage=False,
    )


def _insert(client, table):
    client.use_table(table)
    client.store(
        {"id": [1, 2], "name": ["n1", "n2"], "payload": ["p1", "p2"]}
    )
    client.flush()


def _names(client, table):
    return [row["name"] for row in client.execute(f"SELECT name FROM {table}").to_dict()]


def test_drop_all_tables_then_recreate_each_name():
    with tempfile.TemporaryDirectory() as tmp:
        client = _open(tmp)
        try:
            client.execute(f"CREATE TABLE a {SCHEMA}")
            client.execute(f"CREATE TABLE b {SCHEMA}")
            _insert(client, "a")
            _insert(client, "b")

            client.execute("DROP TABLE a")
            client.execute("DROP TABLE b")
            assert client.list_tables() == []

            # Recreating the first name used to adopt the other dropped table's
            # still-present file, making this second statement fail.
            client.execute(f"CREATE TABLE a {SCHEMA}")
            client.execute(f"CREATE TABLE b {SCHEMA}")
            assert sorted(client.list_tables()) == ["a", "b"]

            _insert(client, "a")
            _insert(client, "b")
            assert _names(client, "a") == ["n1", "n2"]
            assert _names(client, "b") == ["n1", "n2"]
        finally:
            client.close()


def test_drop_and_recreate_every_name_in_a_three_table_cycle():
    with tempfile.TemporaryDirectory() as tmp:
        client = _open(tmp)
        names = ["a", "b", "c"]
        try:
            for name in names:
                client.execute(f"CREATE TABLE {name} {SCHEMA}")
            for name in names:
                _insert(client, name)
            for name in names:
                client.execute(f"DROP TABLE {name}")
            assert client.list_tables() == []

            # Recreate in reverse order: every statement after the first one
            # used to see a previously dropped file adopted into the registry.
            for name in reversed(names):
                client.execute(f"CREATE TABLE {name} {SCHEMA}")
            assert sorted(client.list_tables()) == names

            for name in names:
                _insert(client, name)
                assert _names(client, name) == ["n1", "n2"]
                assert len(client.execute(f"SELECT * FROM {name}").to_dict()) == 2
        finally:
            client.close()


def test_dropped_tables_stay_gone_after_close_and_reopen():
    with tempfile.TemporaryDirectory() as tmp:
        client = _open(tmp)
        try:
            client.execute(f"CREATE TABLE a {SCHEMA}")
            client.execute(f"CREATE TABLE b {SCHEMA}")
            _insert(client, "a")
            _insert(client, "b")
            client.execute("DROP TABLE a")
            client.execute("DROP TABLE b")
        finally:
            client.close()

        reopened = _open(tmp)
        try:
            assert reopened.list_tables() == []
            reopened.execute(f"CREATE TABLE a {SCHEMA}")
            reopened.execute(f"CREATE TABLE b {SCHEMA}")
            assert sorted(reopened.list_tables()) == ["a", "b"]
            _insert(reopened, "b")
            assert _names(reopened, "b") == ["n1", "n2"]
        finally:
            reopened.close()


def _fts_index_file(tmp):
    return os.path.join(tmp, "fts_indexes", "docs.afts")


def _read_fts_config(tmp):
    path = os.path.join(tmp, "fts_config.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _create_docs_table(client):
    client.create_table(
        "docs", {"content": "string", "label": "int64", "vec": "float32_vector"}
    )
    client.use_table("docs")
    client.init_fts(index_fields=["content"])


def test_drop_table_clears_fts_postings_before_same_name_recreate():
    """A dropped table must not leave FTS postings for the next same-name table.

    DROP TABLE removed the catalog entry but left `fts_config.json` and the
    `.afts` files alone, so a same-name CREATE inherited the old documents and
    `hybrid_search()` returned ids that no longer existed (ghost rows).
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = _open(tmp)
        try:
            _create_docs_table(client)
            client.store(
                {
                    "content": ["alpha old one", "beta old two"],
                    "label": [1, 2],
                    "vec": [[1.0] * 8, [2.0] * 8],
                }
            )
            client.flush()
            assert client.search_text_with_scores("alpha", limit=10)

            client.execute("DROP TABLE docs")
            assert not os.path.exists(_fts_index_file(tmp)), (
                "DROP TABLE must delete the FTS index file"
            )
            assert "docs" not in _read_fts_config(tmp), (
                "DROP TABLE must remove the FTS config entry"
            )

            _create_docs_table(client)
            client.store(
                {
                    "content": ["alpha new three"],
                    "label": [7],
                    "vec": [[3.0] * 8],
                }
            )
            client.flush()
            rows = client.execute("SELECT _id FROM docs").to_dict()
            assert len(rows) == 1
            new_id = rows[0]["_id"]
            hits = [
                doc_id for doc_id, _ in client.search_text_with_scores("alpha", limit=10)
            ]
            assert hits == [new_id], (hits, new_id)
            fused = client.hybrid_search([3.0] * 8, "alpha", k=5).to_dict()
            assert [row["_id"] for row in fused] == [new_id], fused
        finally:
            client.close()


def test_drop_table_clears_fts_files_when_no_manager_is_open():
    """A fresh process has no cached FtsManager; DROP must still unlink the files.

    The first client enables FTS and closes (which releases the manager). The
    second client drops the table before ever calling init_fts(), so cleanup has
    to remove the on-disk index without a live engine.
    """
    with tempfile.TemporaryDirectory() as tmp:
        first = _open(tmp)
        try:
            _create_docs_table(first)
            first.store(
                {
                    "content": ["alpha old one"],
                    "label": [1],
                    "vec": [[1.0] * 8],
                }
            )
            first.flush()
        finally:
            first.close()
        assert os.path.exists(_fts_index_file(tmp)), "fixture must leave an index file"

        second = _open(tmp)
        try:
            second.use_table("docs")
            second.execute("DROP TABLE docs")
            assert not os.path.exists(_fts_index_file(tmp)), (
                "DROP TABLE must unlink FTS files even without an open manager"
            )
            assert "docs" not in _read_fts_config(tmp)
        finally:
            second.close()


def test_drop_table_reaps_persisted_vector_index_before_same_name_recreate():
    """The IVF sidecar (`t.apex.ivf`) belongs to the table and must be reaped.

    A same-name CREATE after DROP reaped the table file but previously kept the
    old IVF centroids/lists, so the recreated table reported a vector index it
    never built.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = _open(tmp)
        try:
            client.create_table(
                "t", {"label": "int64", "vec": "float32_vector"}
            )
            client.use_table("t")
            client.store(
                {
                    "label": list(range(64)),
                    "vec": [[float(i), float(i + 1), float(i + 2)] for i in range(64)],
                }
            )
            client.flush()
            client.create_vector_index("vec", nlist=4, iterations=4)
            index_path = os.path.join(tmp, "t.apex.ivf")
            assert os.path.isfile(index_path)
            assert client.vector_index_info() is not None

            client.execute("DROP TABLE t")
            client.execute("CREATE TABLE t (label INT, vec FLOAT32_VECTOR)")
            client.use_table("t")
            assert not os.path.exists(index_path), (
                "same-name CREATE must reap the dropped table's IVF file"
            )
            assert client.vector_index_info() is None
            try:
                client.topk_distance("vec", [0.0, 0.0, 0.0], k=1, nprobe=1, raw=True)
            except RuntimeError as exc:
                assert "coarse vector index" in str(exc), exc
            else:
                raise AssertionError("a dropped IVF index must not be readable")
        finally:
            client.close()

