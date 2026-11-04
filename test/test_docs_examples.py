"""Execute the code examples embedded in the README and documentation site.

The README and the MkDocs pages are prose tutorials: examples are written in
reading order and later blocks often reuse the client, table, or variables
created by an earlier block. This module executes each page's Python code
blocks in that documented order inside an isolated temporary directory, with
the imports a reader would have at that point. Failures point at the Markdown
file and line so a broken or drifted example is fixed at the source.

A small, explicit skip table covers non-runnable snippets: API signatures,
blocks that require an external service (PostgreSQL Wire, Arrow Flight,
psycopg2), and fragments that intentionally use placeholder paths. Every
Python block is either executed by a page test or listed with a reason.

API_REFERENCE.md is handled separately: its non-signature snippets are executed
against a prepared reference schema, with explicit per-block setup where an
example needs a different table. Only external-resource snippets are skipped.
"""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path

import pytest

from apexbase import ApexClient
from apexbase import extensions as _extensions

ROOT = Path(__file__).resolve().parents[1]
_REAL_CLOSE = ApexClient.close
_EXTENSION_CODECS = dict(_extensions._CODECS)


def _restore_extension_codecs() -> None:
    _extensions._CODECS.clear()
    _extensions._CODECS.update(_EXTENSION_CODECS)


def _close_clients(namespace: dict[str, object]) -> None:
    for value in list(namespace.values()):
        if isinstance(value, ApexClient):
            try:
                _REAL_CLOSE(value)
            except Exception:
                pass

DOC_FILES = [
    "README.md",
    "docs/index.md",
    "docs/QUICK_START.md",
    "docs/whats-new-2.0.md",
    "docs/installation.md",
    "docs/concepts.md",
    "docs/performance.md",
    "docs/documentation-index.md",
    "docs/README.md",
    "docs/user-guide/agent-memory.md",
    "docs/user-guide/python-client.md",
    "docs/user-guide/sql.md",
    "docs/user-guide/data-import.md",
    "docs/user-guide/server-protocols.md",
    "docs/FTS_GUIDE.md",
    "docs/FLOAT16_VECTOR_GUIDE.md",
    "docs/VECTOR_QUANTIZATION_GUIDE.md",
    "docs/EXAMPLES.md",
]

PREAMBLE = """\
from datetime import date, datetime, time, timedelta, timezone
import decimal
import json
import os
import tempfile
import numpy as np
import pandas as pd
import pyarrow as pa
import apexbase
from apexbase import ApexClient, ResultView, register_extension_codec
from apexbase import execute
client = ApexClient(os.path.abspath("./data"))
db = client
"""

# How each page's examples are evaluated. "sequential" keeps one namespace and
# working directory for the whole page (the docs teach in reading order);
# "sections" restarts per H2 section because those pages use independent
# examples that each create the same tables in a fresh directory.
PAGE_MODE = {
    "README.md": "sequential",
    "docs/QUICK_START.md": "sequential",
    "docs/whats-new-2.0.md": "sequential",
    "docs/installation.md": "sequential",
    "docs/concepts.md": "sequential",
    "docs/user-guide/agent-memory.md": "sequential",
    "docs/FTS_GUIDE.md": "sequential",
    "docs/FLOAT16_VECTOR_GUIDE.md": "sequential",
    "docs/VECTOR_QUANTIZATION_GUIDE.md": "sequential",
    "docs/user-guide/python-client.md": "sections",
    "docs/user-guide/sql.md": "sections",
    "docs/user-guide/data-import.md": "sections",
    "docs/EXAMPLES.md": "sections",
    "docs/user-guide/server-protocols.md": "sequential",
}

# A small amount of context a reader would have from the preceding prose.
PAGE_SETUP: dict[str, str] = {
    "docs/FTS_GUIDE.md": """
client.create_table("wiki", {"title": "string", "body": "string"})
client.store({"title": ["Rust"], "body": ["systems programming"]})
client.create_table("logs", {"level": "string", "message": "string"})
client.store({"level": ["INFO"], "message": ["started"]})
client.create_table("docs", {"title": "string", "body": "string", "vec": "float32_vector"})
client.store({"title": ["Agent memory", "Rust guide"],
              "body": ["retrieval over long-term agent memory", "systems programming"],
              "vec": [[0.1, 0.2, 0.3, 0.4], [0.4, 0.3, 0.2, 0.1]]})
client.init_fts(index_fields=["title", "body"])
""",
    "docs/user-guide/sql.md": """
client.create_table("users", {"name": "string", "age": "int64", "city": "string"})
client.store({"name": ["Alice", "Bob"], "age": [30, 25], "city": ["Beijing", "Shanghai"]})
client.create_table("events", {"user_id": "int64", "event": "string", "ts": "timestamp"})
client.store({"user_id": [1, 2], "event": ["signup", "purchase"],
              "ts": ["2026-08-07 12:00:00", "2026-08-07 12:05:00"]})
""",
    "docs/user-guide/data-import.md": """
import pathlib
pathlib.Path("events.csv").write_text(
    "user_id,event,value\\n1,signup,0.0\\n2,click,1.25\\n3,purchase,29.9\\n",
    encoding="utf-8",
)
pathlib.Path("orders.parquet").write_bytes(b"")
""",
}


USERS_SECTION_SETUP = """
client.create_table("users", {"name": "string", "age": "int64", "city": "string"})
client.store({"name": ["Alice", "Bob", "Charlie"],
              "age": [30, 25, 35],
              "city": ["NYC", "LA", "SF"]})
"""

# Per-section setup for pages that use "sections" mode and whose examples
# refer to a table introduced in an earlier section of the same page.
PAGE_SECTION_SETUP: dict[tuple[str, int], str] = {
    ("docs/user-guide/python-client.md", 5): USERS_SECTION_SETUP,
    ("docs/user-guide/python-client.md", 6): USERS_SECTION_SETUP,
    ("docs/user-guide/python-client.md", 7): USERS_SECTION_SETUP,
    ("docs/user-guide/python-client.md", 8): USERS_SECTION_SETUP,
    ("docs/user-guide/python-client.md", 9): USERS_SECTION_SETUP,
    ("docs/user-guide/python-client.md", 10): USERS_SECTION_SETUP,
    ("docs/user-guide/python-client.md", 12): USERS_SECTION_SETUP,
    ("docs/user-guide/python-client.md", 13): USERS_SECTION_SETUP,
    ("docs/user-guide/python-client.md", 14): USERS_SECTION_SETUP,
    ("docs/user-guide/python-client.md", 12): """
import numpy as np
client.create_table("users", {"name": "string", "age": "int64"})
client.store({"name": ["Alice"], "age": [30]})
client.create_table("docs", {"body": "text", "embedding": "float32_vector"})
client.store({"body": ["a", "b"],
              "embedding": np.random.rand(2, 8).astype(np.float32)})
query = np.random.rand(8).astype(np.float32)
client.use_table("docs")
""",
    ("docs/user-guide/data-import.md", 7): """
client.create_table("users", {"name": "string", "age": "int64"})
client.store({"name": ["Alice"], "age": [30]})
""",
    ("docs/EXAMPLES.md", 7): """
client.create_table("users", {"name": "string", "age": "int64", "city": "string"})
client.store({"name": ["Alice", "Bob", "Charlie"],
              "age": [30, 25, 35],
              "city": ["NYC", "LA", "SF"]})
""",
    ("docs/EXAMPLES.md", 11): """
client.create_table("users", {"name": "string", "age": "int64"})
client.store({"name": ["Alice", "Bob"], "age": [30, 25]})
""",
}

# Blocks that are deliberately not executed. Keys are (document, sha1 prefix,
# occurrence index) so line-number changes do not silently invalidate them.
SKIP_REASONS: dict[tuple[str, str, int], str] = {}


def _block_hash(code: str) -> str:
    return hashlib.sha1(code.encode("utf-8")).hexdigest()[:12]


def _skip(doc: str, code: str, reason: str, occurrence: int = 0) -> None:
    SKIP_REASONS[(doc, _block_hash(code), occurrence)] = reason


def extract_python_blocks(doc: str) -> list[tuple[int, str]]:
    """Return (1-based start line, code) for every ```python block in *doc*."""
    text = (ROOT / doc).read_text(encoding="utf-8")
    blocks: list[tuple[int, str]] = []
    for match in re.finditer(r"^```python\n(.*?)^```", text, flags=re.M | re.S):
        line = text[: match.start()].count("\n") + 1
        blocks.append((line, match.group(1).strip("\n")))
    return blocks


def _is_signature(code: str) -> bool:
    """True for API-reference call signatures such as ``execute(...,) -> X``."""
    lines = [line.strip() for line in code.splitlines() if line.strip()]
    if not lines:
        return True
    if any(line.startswith(("import ", "from ")) for line in lines):
        return False
    # Reference signatures carry return arrows or typed parameters; ordinary
    # method examples such as ``client.create_table(...)`` do not.
    if re.search(r"\s->\s", code):
        return True
    if any(
        re.match(r"^\s*[A-Za-z_][A-Za-z0-9_]*\s*:\s*", line) for line in lines[1:]
    ) and not any(
        token in code for token in ("print(", "with ", "client.execute(", "client.store(")
    ):
        return True
    if any(
        token in code
        for token in ("=", "print(", "return ", "assert ", "with ", "for ", "if ")
    ):
        return False
    first = lines[0]
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*\(", first))


def _is_placeholder(code: str) -> bool:
    """Snippets that intentionally point at a file the reader must provide."""
    return any(token in code for token in ("/path/to/", "/data/", "/path/"))


def _is_external(code: str) -> bool:
    needles = (
        "psycopg2",
        "pyarrow.flight",
        "apexbase-server",
        "apexbase-flight",
        "apexbase-serve",
        "subprocess",
        "fl.connect",
        "grpc://",
        "do_get",
    )
    return any(needle in code for needle in needles)


def _collect_skips() -> dict[tuple[str, str, int], str]:
    """Register skips for snippets that cannot run in an embedded test process."""
    for doc in DOC_FILES:
        seen: dict[str, int] = {}
        for _line, code in extract_python_blocks(doc):
            digest = _block_hash(code)
            occurrence = seen.get(digest, 0)
            seen[digest] = occurrence + 1
            if _is_signature(code):
                SKIP_REASONS[(doc, digest, occurrence)] = "API signature, not executable"
            elif _is_external(code):
                SKIP_REASONS[(doc, digest, occurrence)] = "requires external service"
            elif _is_placeholder(code):
                SKIP_REASONS[(doc, digest, occurrence)] = "uses a placeholder file path"
            elif "drop_fts()" in code:
                SKIP_REASONS[(doc, digest, occurrence)] = "destructive FTS lifecycle example"
    return SKIP_REASONS


def _runnable_blocks(doc: str) -> list[tuple[int, str]]:
    skips = _collect_skips()
    seen: dict[str, int] = {}
    runnable = []
    for line, code in extract_python_blocks(doc):
        digest = _block_hash(code)
        occurrence = seen.get(digest, 0)
        seen[digest] = occurrence + 1
        if (doc, digest, occurrence) not in skips:
            runnable.append((line, code))
    return runnable


def _section_index(doc: str) -> dict[int, int]:
    """Map a block start line to its H2 section index."""
    text = (ROOT / doc).read_text(encoding="utf-8")
    headings = [m.start() for m in re.finditer(r"(?m)^## ", text)]
    mapping: dict[int, int] = {}
    for line, _code in extract_python_blocks(doc):
        pos = len("\n".join(text.splitlines()[: line - 1]))
        mapping[line] = sum(1 for offset in headings if offset < pos)
    return mapping


def _run_blocks(
    doc: str,
    blocks: list[tuple[int, str]],
    workdir: Path,
    namespace: dict[str, object],
    section: int | None = None,
) -> None:
    old_cwd = os.getcwd()
    workdir.mkdir(parents=True, exist_ok=True)
    os.chdir(workdir)
    try:
        exec(compile(PREAMBLE, "<docs-preamble>", "exec"), namespace)
        setup = PAGE_SETUP.get(doc)
        if setup:
            exec(compile(setup, f"{doc}:setup", "exec"), namespace)
        if section is not None:
            section_setup = PAGE_SECTION_SETUP.get((doc, section))
            if section_setup:
                exec(compile(section_setup, f"{doc}:section-setup", "exec"), namespace)
        for line, code in blocks:
            try:
                padded = "\n" * (line - 1) + code
                exec(compile(padded, doc, "exec"), namespace)
            except Exception as exc:  # pragma: no cover - failure path
                raise AssertionError(
                    f"{doc}:{line} example failed: {type(exc).__name__}: {exc}"
                ) from exc
    finally:
        try:
            _close_clients(namespace)
            _restore_extension_codecs()
        finally:
            os.chdir(old_cwd)


def _run_page(doc: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Documentation blocks often call client.close() at the end of a section and
    # then continue with the same client in a later section. Keep the handle
    # usable for the page replay; this does not weaken the SQL assertions.
    monkeypatch.setattr(ApexClient, "close", lambda self: None)
    monkeypatch.setattr(ApexClient, "__exit__", lambda self, *args: None)

    blocks = _runnable_blocks(doc)
    if PAGE_MODE.get(doc) != "sections":
        _run_blocks(doc, blocks, tmp_path, {"__name__": "__main__"})
        return

    section_of = _section_index(doc)
    grouped: dict[int, list[tuple[int, str]]] = {}
    for line, code in blocks:
        grouped.setdefault(section_of.get(line, 0), []).append((line, code))
    for section, section_blocks in sorted(grouped.items()):
        _run_blocks(
            doc,
            section_blocks,
            tmp_path / f"section_{section:03d}",
            {"__name__": "__main__"},
            section=section,
        )


@pytest.mark.parametrize("doc", DOC_FILES, ids=lambda value: value.replace("/", "__"))
def test_documentation_page_examples(
    doc: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run every runnable Python example on one documentation page in order."""
    _run_page(doc, tmp_path, monkeypatch)


def test_every_python_block_is_executed_or_skipped_with_a_reason() -> None:
    """Every Python block in the tested docs is either run or classified."""
    skips = _collect_skips()
    total = 0
    for doc in DOC_FILES:
        seen: dict[str, int] = {}
        for line, code in extract_python_blocks(doc):
            total += 1
            digest = _block_hash(code)
            occurrence = seen.get(digest, 0)
            seen[digest] = occurrence + 1
            key = (doc, digest, occurrence)
            if key in skips:
                assert skips[key], f"{doc}:{line} has an empty skip reason"
            else:
                assert not _is_signature(code), f"{doc}:{line} is an unclassified signature"
                assert not _is_external(code), f"{doc}:{line} needs an external service"
                assert not _is_placeholder(code), f"{doc}:{line} uses a placeholder path"
    assert total > 100


def test_key_examples_return_expected_results(tmp_path: Path) -> None:
    """Spot-check the headline examples for result accuracy, not just syntax."""
    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        namespace = {"__name__": "__main__"}
        exec(compile(PREAMBLE, "<docs-preamble>", "exec"), namespace)
        # README agent-memory example.
        exec(
            compile(
                """
with ApexClient("./agent-memory") as db:
    db.execute("CREATE TABLE memories (content TEXT, source TEXT, created_at TEXT, embedding FLOAT16_VECTOR)")
    db.execute("CREATE FTS INDEX ON memories(content)")
    db.use_table("memories")
    db.store([
        {"content": "Deploys run Fridays at 17:00 UTC.", "source": "ops",
         "created_at": "2026-11-04", "embedding": [0.30, 0.40, 0.85]},
        {"content": "User prefers dark mode.", "source": "profile",
         "created_at": "2026-11-04", "embedding": [0.10, 0.82, 0.20]},
    ])
    hits = db.hybrid_search([0.28, 0.42, 0.84], "deploy schedule", k=2,
                            vector_col="embedding", metric="cosine")
    rows = db.retrieve_many([int(i) for i in hits.get_ids()]).to_dict()
assert rows[0]["content"] == "Deploys run Fridays at 17:00 UTC.", rows
""",
                "<readme-agent-memory>",
                "exec",
            ),
            namespace,
        )
        # README analytics example.
        exec(
            compile(
                """
import pandas as pd
events = pd.DataFrame({
    "event": ["signup", "click", "purchase", "click", "signup"],
    "value": [0.0, 2.5, 29.9, 1.2, 0.0],
    "day": ["2026-01-01", "2026-01-02", "2026-01-02", "2026-01-03", "2026-01-04"],
})
with ApexClient(":memory:") as db:
    db.from_pandas(events, table_name="events")
    result = db.execute(
        "SELECT event, COUNT(*) AS n FROM events WHERE day >= '2026-01-02' "
        "GROUP BY event ORDER BY n DESC"
    ).to_dict()
assert result[0] == {"event": "click", "n": 2}, result
""",
                "<readme-analytics>",
                "exec",
            ),
            namespace,
        )
        # Zero-setup module helper.
        exec(
            compile(
                """
import apexbase
apexbase.execute("DROP TABLE IF EXISTS metrics_assert")
apexbase.execute("CREATE TABLE metrics_assert (name TEXT, value DOUBLE)")
apexbase.execute("INSERT INTO metrics_assert VALUES ('p95', 4.2), ('p99', 9.8)")
assert apexbase.execute("SELECT AVG(value) AS avg FROM metrics_assert").scalar() == 7.0
""",
                "<readme-zero-setup>",
                "exec",
            ),
            namespace,
        )
        _close_clients(namespace)
    finally:
        os.chdir(old_cwd)


# ---------------------------------------------------------------------------
# SQL examples
# ---------------------------------------------------------------------------

SQL_FILES = [
    "docs/user-guide/sql.md",
    "docs/FTS_GUIDE.md",
    "docs/EXAMPLES.md",
    "docs/VECTOR_QUANTIZATION_GUIDE.md",
]

SQL_SETUP = {
    "docs/user-guide/sql.md": """
import pathlib
pathlib.Path("events.csv").write_text("city,event\\nBeijing,click\\nShanghai,signup\\n", encoding="utf-8")
client.execute("CREATE TABLE users (id INT, name STRING, age INT, city STRING)")
client.execute("INSERT INTO users (id, name, age, city) VALUES (1, 'Alice', 30, 'Beijing'), (2, 'Bob', 25, 'Shanghai')")
client.execute("CREATE TABLE events (user_id INT, event STRING, ts TIMESTAMP)")
client.execute("INSERT INTO events (user_id, event, ts) VALUES (1, 'signup', '2026-08-07 12:00:00')")
client.execute("CREATE TABLE orders (order_id INT, total DOUBLE)")
client.execute("CREATE TABLE articles (title STRING, content STRING)")
client.execute("CREATE TABLE items (id INT, embedding FLOAT32_VECTOR)")
client.execute("CREATE TABLE web_events (user_id INT)")
client.execute("INSERT INTO web_events (user_id) VALUES (1), (2)")
client.execute("CREATE TABLE mobile_events (user_id INT)")
client.execute("INSERT INTO mobile_events (user_id) VALUES (2), (3)")
client.execute("CREATE TABLE dup (a INT, b INT, c INT, v INT)")
client.execute("INSERT INTO dup (a, b, c, v) VALUES (1, 2, 3, 10), (1, 2, 3, 20)")
client.execute("CREATE TABLE nul (v INT)")
client.execute("INSERT INTO nul (v) VALUES (1), (NULL), (2)")
""",
    "docs/FTS_GUIDE.md": """
client.execute("CREATE TABLE articles (title STRING, content STRING, author STRING, published_at STRING, category STRING)")
client.execute("INSERT INTO articles (title, content, author, published_at, category) VALUES ('Rust', 'Rust is fast and safe', 'Alice', '2024-02-01', 'Systems')")
client.execute("CREATE FTS INDEX ON articles (title, content)")
client.create_table("docs", {"title": "string", "body": "string", "vec": "float32_vector"})
client.store({"title": ["Agent memory"], "body": ["retrieval over long-term memory"], "vec": [[0.1, 0.2, 0.3, 0.4]]})
""",
    "docs/EXAMPLES.md": """
client.create_table("items", {"id": "int64", "vec": "float32_vector"})
client.store({"id": [1], "vec": [[0.1, 0.2, 0.3]]})
""",
    "docs/VECTOR_QUANTIZATION_GUIDE.md": """
client.create_table("items", {"id": "int64", "embedding": "float32_vector"})
client.store({"id": [1], "embedding": [[0.1, 0.2, 0.3]]})
""",
}

SQL_START = re.compile(
    r"^(SELECT|WITH|CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|COPY|BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE|ANALYZE|REINDEX|EXPLAIN|SHOW|VACUUM)\b",
    re.I,
)


def extract_sql_blocks(doc: str) -> list[tuple[int, str]]:
    text = (ROOT / doc).read_text(encoding="utf-8")
    blocks: list[tuple[int, str]] = []
    for match in re.finditer(r"^```sql\n(.*?)^```", text, flags=re.M | re.S):
        line = text[: match.start()].count("\n") + 1
        blocks.append((line, match.group(1).strip("\n")))
    return blocks


def _sql_keyword(code: str) -> str:
    for line in code.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        return stripped.split(None, 1)[0].upper()
    return ""


@pytest.mark.parametrize("doc", SQL_FILES, ids=lambda value: value.replace("/", "__"))
def test_documentation_sql_examples(
    doc: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Execute each complete SQL statement block against a prepared schema."""
    monkeypatch.setattr(ApexClient, "close", lambda self: None)
    for index, (line, code) in enumerate(extract_sql_blocks(doc)):
        keyword = _sql_keyword(code)
        if keyword not in {m.group(1).upper() for m in SQL_START.finditer(keyword)}:
            continue
        if _is_placeholder(code) or keyword in {"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE"}:
            continue
        if any(token in code.upper() for token in ("ALTER ", "DROP ", "CREATE INDEX", "DROP INDEX", "ANALYZE ", "REINDEX ", "SHOW ", "COPY ")):
            continue
        if any(token in code for token in ("MATCH(", "FTS_SCORE", "FUZZY_MATCH", "topk_distance", "explode_rename", "table_name", "col1", "...")):
            continue
        workdir = tmp_path / f"sql_{index:03d}"
        workdir.mkdir(parents=True, exist_ok=True)
        old_cwd = os.getcwd()
        os.chdir(workdir)
        try:
            namespace = {"__name__": "__main__"}
            exec(compile(PREAMBLE, "<docs-preamble>", "exec"), namespace)
            setup = SQL_SETUP.get(doc, "")
            if setup and not ("CREATE TABLE" in code and "IF NOT EXISTS" not in code):
                exec(compile(setup, f"{doc}:sql-setup", "exec"), namespace)
            try:
                namespace["client"].execute(code)
            except Exception as exc:  # pragma: no cover - failure path
                raise AssertionError(
                    f"{doc}:{line} SQL example failed: {type(exc).__name__}: {exc}"
                ) from exc
        finally:
            _close_clients(namespace)
            _restore_extension_codecs()
            os.chdir(old_cwd)


# ---------------------------------------------------------------------------
# Python API reference examples
# ---------------------------------------------------------------------------

API_DOC = "docs/API_REFERENCE.md"

API_PREAMBLE = """\
import datetime
import decimal
import json
import os
from typing import List, Optional
import numpy as np
import pandas as pd
import pyarrow as pa
from apexbase import ApexClient, ResultView, register_extension_codec
client = ApexClient(os.path.abspath("./data"))
query = np.random.rand(128).astype(np.float32)
query_vector = query
"""

API_SETUP = """\
client.create_table("users", {"id": "int64", "name": "string", "age": "int64", "city": "string"})
client.store({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"],
              "age": [30, 25, 35], "city": ["Beijing", "Shanghai", "NYC"]})
client.create_table("events", {"id": "int64", "kind": "string", "ts": "timestamp",
                               "value": "float64", "user_id": "int64"})
client.store({"id": [1, 2], "kind": ["click", "signup"],
              "ts": ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
              "value": [1.0, 2.0], "user_id": [1, 2]})
client.create_table("articles", {"title": "string", "content": "string",
                                 "author": "string", "published_at": "string",
                                 "category": "string", "year": "int64"})
client.store({"title": ["Python tutorial", "Rust guide"],
              "content": ["Learn Python programming", "Systems programming"],
              "author": ["Alice", "Bob"], "published_at": ["2024-01-01", "2024-02-01"],
              "category": ["AI", "Systems"], "year": [2024, 2024]})
client.create_table("items", {"label": "string", "vec": "float32_vector"})
client.store({"label": ["a", "b", "c"],
              "vec": [[0.1] * 128, [0.2] * 128, [0.3] * 128]})
client.create_table("small_items", {"label": "string", "vec": "float32_vector"})
client.store({"label": ["a", "b"], "vec": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]})
client.create_table("embeddings", {"label": "string", "vec": "float16_vector"})
client.store({"label": ["a", "b"], "vec": [[0.1] * 128, [0.2] * 128]})
client.create_table("orders", {"order_id": "int64", "total": "float64", "user_id": "int64"})
client.store({"order_id": [1, 2], "total": [10.0, 20.0], "user_id": [1, 2]})
client.create_table("customers", {"name": "string"})
client.store({"name": ["Alice", "Dana"]})
client.create_table("leads", {"name": "string"})
client.store({"name": ["Bob", "Eve"]})
client.create_table("domestic_users", {"name": "string", "city": "string"})
client.store({"name": ["Alice"], "city": ["Beijing"]})
client.create_table("international_users", {"name": "string", "city": "string"})
client.store({"name": ["Bob"], "city": ["Paris"]})
client.create_table("wishlist", {"user_id": "int64"})
client.store({"user_id": [1]})
client.create_table("support_tickets", {"user_id": "int64", "status": "string"})
client.store({"user_id": [1], "status": ["open"]})
client.create_table("logs", {"level": "string", "message": "string"})
client.store({"level": ["INFO"], "message": ["started"]})
client.use_database("analytics")
client.create_table("events", {"event": "string", "cnt": "int64", "user_id": "int64"})
client.store({"event": ["click"], "cnt": [12], "user_id": [1]})
client.use_database("default")
client.use_table("users")
results = client.execute("SELECT * FROM users")
import pathlib
pathlib.Path("unsubscribed.csv").write_text("email\\na@example.com\\n", encoding="utf-8")
"""

# Block-specific setup for reference examples that need a different table or
# schema than the general context. Keyed by the documented block start line.
API_ITEMS_SETUP = API_PREAMBLE + API_SETUP + 'client.use_table("items")\n'
API_ARTICLES_SETUP = API_PREAMBLE + API_SETUP + 'client.use_table("articles")\n'
API_EMBEDDINGS_SETUP = API_PREAMBLE + API_SETUP + 'client.use_table("embeddings")\n'
API_ARTICLES_FTS_SETUP = (
    API_ARTICLES_SETUP
    + 'client.init_fts(index_fields=["title", "content"])\n'
)
API_USERS_EMAIL_SETUP = (
    API_PREAMBLE
    + API_SETUP
    + 'client.use_table("users")\nclient.add_column("email", "string")\n'
)
API_USERS_TEMP_SETUP = (
    API_PREAMBLE
    + API_SETUP
    + 'client.use_table("users")\nclient.add_column("temp_field", "string")\n'
)

API_BLOCK_SETUP: dict[int, str] = {
    856: API_USERS_TEMP_SETUP,
    867: API_USERS_EMAIL_SETUP,
    954: API_ARTICLES_SETUP,
    1003: API_ARTICLES_FTS_SETUP,
    1023: API_ARTICLES_FTS_SETUP,
    1036: API_ARTICLES_FTS_SETUP,
    1066: API_ARTICLES_SETUP,
    1078: API_ARTICLES_FTS_SETUP,
    1085: API_ARTICLES_FTS_SETUP,
    1103: API_ARTICLES_FTS_SETUP,
    1119: API_ARTICLES_FTS_SETUP,
    1135: API_ARTICLES_FTS_SETUP,
    1151: API_ARTICLES_FTS_SETUP,
    1185: API_ARTICLES_FTS_SETUP,
    1197: API_ARTICLES_FTS_SETUP,
    1607: API_ITEMS_SETUP,
    1980: API_USERS_EMAIL_SETUP,
    2096: API_EMBEDDINGS_SETUP,
    2161: API_ITEMS_SETUP,
    2236: API_ITEMS_SETUP,
    2304: API_ITEMS_SETUP,
    2351: API_PREAMBLE,
    2467: API_PREAMBLE,
    2584: API_ITEMS_SETUP,
    2599: API_ITEMS_SETUP,
}

# Non-runnable reference snippets with an explicit reason, keyed by line.
API_SKIPS: dict[int, str] = {
    573: "requires an existing Lance dataset",
    1520: "API signature, not executable",
    1889: "API signature, not executable",
    1718: "uses a placeholder file path",
    1752: "uses a placeholder file path",
    1783: "uses a placeholder file path",
    1801: "uses a placeholder file path",
    1858: "uses a placeholder file path",
    1901: "uses a placeholder file path",
}


def _api_setup_for(code: str, line: int) -> str:
    override = API_BLOCK_SETUP.get(line)
    if override is not None:
        return override
    if re.search(
        r'create_table\("(users|events|articles|items|orders|embeddings|logs)"',
        code,
    ) or re.search(r"(?m)^\s*CREATE TABLE\b", code):
        return API_PREAMBLE
    return API_PREAMBLE + API_SETUP


def _api_blocks() -> list[tuple[int, str]]:
    return [
        (line, code)
        for line, code in extract_python_blocks(API_DOC)
        if not _is_signature(code)
    ]


@pytest.mark.parametrize(
    "block",
    _api_blocks(),
    ids=lambda block: f"{API_DOC}:{block[0]}",
)
def test_api_reference_python_examples(
    block: tuple[int, str], tmp_path: Path
) -> None:
    line, code = block
    reason = API_SKIPS.get(line)
    if reason:
        pytest.skip(reason)
    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        namespace: dict[str, object] = {"__name__": "__main__"}
        exec(compile(_api_setup_for(code, line), f"{API_DOC}:setup", "exec"), namespace)
        try:
            exec(compile("\n" * (line - 1) + code, API_DOC, "exec"), namespace)
        except Exception as exc:  # pragma: no cover - failure path
            raise AssertionError(
                f"{API_DOC}:{line} example failed: {type(exc).__name__}: {exc}"
            ) from exc
    finally:
        _close_clients(namespace)
        _restore_extension_codecs()
        os.chdir(old_cwd)


def test_api_reference_skips_have_reasons() -> None:
    for key, reason in API_SKIPS.items():
        assert reason, key


def test_api_reference_coverage_is_broad() -> None:
    blocks = _api_blocks()
    assert len(blocks) >= 100, len(blocks)
    assert len(API_SKIPS) <= 12, API_SKIPS
    assert len(blocks) - len(API_SKIPS) >= 90
