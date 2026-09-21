"""
In-memory (``:memory:``) parity for FTS back-fill and ANALYZE / REINDEX.

These regressions share one root cause: a code path that only worked for
filesystem tables silently did nothing (or reported "disabled") for
process-local databases.

- the FTS mmap lane answered "zero rows" instead of "lane unavailable", so a
  back-fill never reached the generic Arrow read;
- a process-local FTS engine reported "nothing to rebuild", so
  ``init_fts()`` never back-filled rows that already existed;
- the FTS config for ``:memory:`` lives in a Rust registry, so SQL DDL and the
  Python search API disagreed about which tables were enabled;
- ``ANALYZE`` / ``REINDEX`` opened the table path as a file.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

try:
    from apexbase import ApexClient
except ImportError as e:  # pragma: no cover - environment guard
    pytest.skip(f"ApexBase not available: {e}", allow_module_level=True)


DOCS = [
    {"title": "Python Guide", "content": "Learn Python programming"},
    {"title": "Rust Tutorial", "content": "Systems programming with Rust"},
]


def test_init_fts_after_store_backfills_existing_rows():
    with ApexClient(":memory:") as client:
        client.create_table("docs")
        client.store(DOCS)

        client.init_fts(index_fields=["title", "content"])

        assert list(client.search_text("Python")) == [1]
        assert client.get_fts_stats()["doc_count"] == 2
        assert [row["title"] for row in client.search_and_retrieve("Python")] == [
            "Python Guide"
        ]


def test_init_fts_before_store_still_indexes_new_rows():
    with ApexClient(":memory:") as client:
        client.create_table("docs")
        client.init_fts(index_fields=["title", "content"])
        client.store(DOCS)

        assert list(client.search_text("Python")) == [1]


def test_match_sql_works_after_python_init_fts_on_memory_table():
    with ApexClient(":memory:") as client:
        client.create_table("docs")
        client.store(DOCS)
        client.init_fts(index_fields=["title", "content"])

        rows = client.execute("SELECT title FROM docs WHERE MATCH('Python')").to_dict()
        assert rows == [{"title": "Python Guide"}]


def test_sql_create_fts_index_backfills_and_enables_python_search():
    with ApexClient(":memory:") as client:
        client.create_table("docs")
        client.store(DOCS)

        status = client.execute("CREATE FTS INDEX ON docs(title, content)").to_dict()[0][
            "status"
        ]
        assert "2 rows indexed" in status

        # SQL DDL must be visible to the Python search API as well.
        assert list(client.search_text("Rust")) == [2]
        rows = client.execute("SELECT title FROM docs WHERE MATCH('Rust')").to_dict()
        assert rows == [{"title": "Rust Tutorial"}]


def test_analyze_then_reindex_on_memory_table():
    with ApexClient(":memory:") as client:
        client.execute("CREATE TABLE t (a INT, s TEXT)")
        client.use_table("t")
        client.store([{"a": 1, "s": "x"}, {"a": 2, "s": "y"}])
        client.execute("CREATE INDEX idx_a ON t (a)")

        analyzed = client.execute("ANALYZE t").to_dict()
        assert {"a", "s"}.issubset({row["column_name"] for row in analyzed})
        assert {row["row_count"] for row in analyzed if row["column_name"] != "_id"} == {2}

        assert client.execute("REINDEX t").scalar() == 1
        assert client.execute("SELECT a FROM t WHERE a = 2").to_dict() == [{"a": 2}]


def test_memory_workflow_writes_no_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    with ApexClient(":memory:") as client:
        client.execute("CREATE TABLE t (a INT, s TEXT)")
        client.use_table("t")
        client.store([{"a": 1, "s": "x"}])
        client.execute("CREATE INDEX idx_a ON t (a)")
        client.init_fts(index_fields=["s"])
        client.execute("ANALYZE t")
        client.execute("REINDEX t")

    assert list(tmp_path.iterdir()) == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
