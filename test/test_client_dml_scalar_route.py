"""A single-statement DML must not need Arrow to report its count.

Decoding the one-row `result` through the Arrow C Data Interface cost the first
`import pyarrow` of the process -- 46 ms measured, 33 ms of that loading the C
extensions -- which put a one-time cliff on the first write of a session, while
the second write cost 0.4 ms. A subprocess makes the position of the write
deterministic: it is that interpreter's first client-lane statement.
"""

import subprocess
import sys
import textwrap
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]

_PREAMBLE = """
    import shutil
    import sys
    import tempfile

    from apexbase.client import ApexClient

    root = tempfile.mkdtemp()
    client = ApexClient(root, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"k": "int64", "s": "string"})
    client.use_table("t")
    client.store({"k": list(range(2_000)), "s": [f"v{i}" for i in range(2_000)]})
    client.flush()
"""


def _run(body: str) -> str:
    script = textwrap.dedent(_PREAMBLE) + textwrap.dedent(body)
    process = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=str(WORKSPACE),
    )
    assert process.returncode == 0, process.stderr
    return process.stdout


def test_a_single_delete_reports_its_count_without_importing_arrow():
    output = _run(
        """
        print("delete", client.execute("DELETE FROM t WHERE k = 7").to_dict())
        print("missing", client.execute("DELETE FROM t WHERE k = 999_999").to_dict())
        print("update", client.execute("UPDATE t SET k = 5 WHERE k = 5").to_dict())
        print("arrow_imported", "pyarrow" in sys.modules)
        client.close()
        shutil.rmtree(root, ignore_errors=True)
        """
    )
    assert "delete [{'result': 1}]" in output, output
    assert "missing [{'result': 0}]" in output, output
    assert "update [{'result': 1}]" in output, output
    assert "arrow_imported False" in output, output


def test_the_scalar_route_leaves_reads_and_transactions_working():
    output = _run(
        """
        client.execute("DELETE FROM t WHERE k < 10")
        print("remaining", client.execute("SELECT count(*) AS n FROM t").to_dict())
        client.execute("BEGIN")
        print("in_txn", client.execute("DELETE FROM t WHERE k = 500").to_dict())
        client.execute("COMMIT")
        print("after", client.execute("SELECT count(*) AS n FROM t").to_dict())
        print("multi", client.execute("DELETE FROM t WHERE k = 11; DELETE FROM t WHERE k = 12").to_dict())
        print("remaining2", client.execute("SELECT count(*) AS n FROM t").to_dict())
        client.close()
        shutil.rmtree(root, ignore_errors=True)
        """
    )
    assert "remaining [{'n': 1990}]" in output, output
    # Inside a transaction the client answers through its transaction branch,
    # which has always reported no count row; the write itself must still land.
    assert "in_txn []" in output, output
    assert "after [{'n': 1989}]" in output, output
    assert "remaining2 [{'n': 1987}]" in output, output


def test_every_scalar_write_statement_stays_off_arrow():
    """INSERT/DDL/COPY/batches report their counts without Arrow too.

    Each of these answers with the same single `rows_affected` row, so routing
    them through the scalar path keeps the value and the column name while
    removing the first `import pyarrow` of the process (43-51 ms measured). The
    flag is read right after the writes: the effect checks that follow use reads,
    and a filtered read is free to take the Arrow path.

    `CREATE TABLE` and `COPY ... TO` are covered too; `COPY <new table> FROM`
    only skips this route when no table is selected, because the scalar entry
    refuses to run without one.
    """
    output = _run(
        """
        import os

        print("insert", client.execute("INSERT INTO t (k) VALUES (4242)").to_dict())
        print("multi", client.execute("DELETE FROM t WHERE k = 11; DELETE FROM t WHERE k = 12").to_dict())
        print("index", client.execute("CREATE INDEX ix ON t (k)").to_dict()[0]["result"] > 0)
        print("alter", client.execute("ALTER TABLE t ADD COLUMN extra int64").to_dict())
        csv_path = os.path.join(root, "copy.csv")
        print("copy", client.execute(f"COPY t TO '{csv_path}' (FORMAT CSV)").to_dict())
        print("create", client.execute("CREATE TABLE fresh (k int64)").to_dict())
        print("fresh_insert", client.execute("INSERT INTO fresh (k) VALUES (9)").to_dict())
        print("truncate", client.execute("TRUNCATE TABLE fresh").to_dict())
        print("drop", client.execute("DROP TABLE fresh").to_dict())
        print("arrow_imported", "pyarrow" in sys.modules)
        print("rows", client.execute("SELECT count(*) AS n FROM t").to_dict())
        print("extra", client.execute("SELECT extra FROM t LIMIT 1").to_dict())
        client.close()
        shutil.rmtree(root, ignore_errors=True)
        """
    )
    assert "insert [{'result': 1}]" in output, output
    assert "multi [{'result': 1}]" in output, output
    assert "index True" in output, output
    assert "alter [{'result': 0}]" in output, output
    # 2000 fixture rows + the inserted one - the two deleted above.
    assert "copy [{'result': 1999}]" in output, output
    assert "create [{'result': 0}]" in output, output
    assert "fresh_insert [{'result': 1}]" in output, output
    assert "truncate [{'result': 0}]" in output, output
    assert "drop [{'result': 0}]" in output, output
    assert "arrow_imported False" in output, output
    # The writes landed, and the column added by ALTER reads back as NULL.
    assert "rows [{'n': 1999}]" in output, output
    assert "extra [{'extra': None}]" in output, output
