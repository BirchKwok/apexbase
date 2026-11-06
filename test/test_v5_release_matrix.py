"""S14: V5 release regression matrix (16 coverage items) and baseline protocol.

This file is the auditable matrix for the release review: it asserts that every
design item has a test/gate/record behind it and adds the few cross-cutting
checks that were only implicit before (typed logical types across reopen /
catalog / ALTER, chunked layout for the new types, and the three layout
switches falling back to the same answers).
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from apexbase import ApexClient, register_extension_codec, unregister_extension_codec

ROOT = Path(__file__).resolve().parents[1]

# 16 design items -> evidence paths (tests/gates/records) that must exist.
MATRIX = {
    "format_version": ("test/test_format_version_gate.py", "test/test_v5_list_type.py"),
    "logical_types": ("test/test_time_family.py", "test/test_v5_list_type.py", "test/test_v5_extension_type.py"),
    "typed_arrow_polars": ("test/test_v5_typed_read_probe.py", "test/test_v5_list_type.py", "test/test_v5_extension_type.py"),
    "chunked_new_types": ("test/test_v5_release_matrix.py",),
    "overlay_dml": ("test/test_v5_stmt_routing.py", "test/test_transactions.py"),
    "layout_switches": ("test/test_v5_release_matrix.py",),
    "ivf_recall": ("test/test_quantized_recall_gate.py", "test/test_vector_index.py", "benchmarks/bench_scale_10m.py"),
    "hybrid_search": ("test/test_hybrid_search.py", "test/test_hybrid_retrieval_benchmark.py"),
    "multiprocess_writes": ("test/test_cross_process_write_exclusion.py",),
    "error_semantics": ("test/test_v5_sql_semantics.py", "test/test_v5_extension_type.py", "test/test_sql_decimal_*"),
    "d6_d7_d8": ("test/test_v5_sql_semantics.py",),
    "d9_d11": ("test/test_v5_stmt_routing.py",),
    "admission_queue": ("test/test_v5_admission_queue.py",),
    "spill_streaming": (
        "test/test_v5_spill.py",
        "test/test_v5_spill_groups.py",
        "test/test_v5_spill_join.py",
        "test/test_v5_streaming.py",
    ),
    "scoreboard_105": ("test/test_benchmark_vector_module.py", "benchmarks/latest_public_baseline.json"),
    "public_baseline": ("benchmarks/rebuild_public_baseline.py", "benchmarks/latest_public_baseline.json"),
}


def test_release_matrix_has_sixteen_documented_items():
    assert len(MATRIX) == 16
    for item, paths in MATRIX.items():
        assert paths, item
        for path in paths:
            matches = sorted(ROOT.glob(path)) if "*" in path else [ROOT / path]
            assert matches and matches[0].exists(), (item, path)


def test_public_baseline_is_a_three_sample_median_with_105_metrics():
    baseline = json.loads((ROOT / "benchmarks/latest_public_baseline.json").read_text())
    assert baseline["suite"] == "apexbase-vs-sqlite-duckdb"
    assert len(baseline["results"]) == 105, len(baseline["results"])
    meta = baseline.get("baseline_meta") or {}
    assert meta.get("samples", 0) >= 3, meta
    assert meta.get("protocol") == "median of at least three same-machine runs"
    assert baseline["config"]["profile"] == "public"


def test_typed_logical_types_across_reopen_catalog_and_alter(tmp_path):
    import datetime as dt
    from decimal import Decimal

    register_extension_codec("matrix", lambda value: value.encode(), lambda raw: raw.decode())
    try:
        path = str(tmp_path / "typed")
        client = ApexClient(path, drop_if_exists=True, enable_cache=False)
        client.create_table(
            "t",
            {
                "id": "int64",
                "d": "date",
                "ts": "timestamp",
                "vals": "list<int64>",
                "dec": "decimal(10,2)",
                "payload": "extension<matrix>",
            },
        )
        client.store(
            {
                "id": [1],
                "d": [dt.date(2024, 2, 3)],
                "ts": [dt.datetime(2024, 2, 3, 4, 5, 6)],
                "vals": [[1, 2, 3]],
                "dec": [Decimal("12.34")],
                "payload": ["hello"],
            }
        )
        client.flush()
        declared = client._storage.table_types("t")
        assert declared["d"] == "date"
        assert declared["ts"] == "timestamp"
        assert declared["vals"].startswith("list<")
        assert declared["dec"] == "decimal(10,2)"
        assert declared["payload"].startswith("extension<matrix")
        client.close()

        reopened = ApexClient(path, enable_cache=False)
        try:
            reopened.use_table("t")
            row = reopened.execute("SELECT * FROM t").to_dict()[0]
            assert row["d"] == dt.date(2024, 2, 3)
            assert row["ts"] == dt.datetime(2024, 2, 3, 4, 5, 6)
            assert row["vals"] == [1, 2, 3]
            # Python writes keep the D4 fixed 16-byte payload; the SQL path is
            # the typed Decimal surface.
            assert isinstance(row["dec"], (bytes, bytearray)) and len(row["dec"]) == 16
            assert Decimal("12.34") == Decimal(
                int.from_bytes(row["dec"], "little", signed=True)
            ).scaleb(-2)
            assert row["payload"] == "hello"

            table = reopened.execute("SELECT d, ts, vals, dec, payload FROM t").to_arrow()
            assert table.schema.field("d").type == __import__("pyarrow").date32()
            assert str(table.schema.field("vals").type).startswith("list")
            assert __import__("pyarrow").types.is_timestamp(table.schema.field("ts").type)

            polars_frame = reopened.execute("SELECT * FROM t").to_polars()
            assert polars_frame.height == 1

            reopened.execute("CREATE TABLE typed_dec (d DECIMAL(10,2))")
            reopened.execute("INSERT INTO typed_dec VALUES ('12.34')")
            decimal_row = reopened.execute("SELECT d FROM typed_dec").to_dict()[0]
            # D4 keeps the public Decimal surface as the fixed 16-byte payload.
            assert decimal_row["d"] == (1234).to_bytes(16, "little", signed=True)

            reopened.execute("ALTER TABLE t ADD COLUMN extra int64")
            reopened.execute("UPDATE t SET extra = 42")
            reopened.execute("ALTER TABLE t DROP COLUMN extra")
            declared_after = reopened._storage.table_types("t")
            assert "extra" not in declared_after
            assert reopened.execute("SELECT vals FROM t").to_dict()[0]["vals"] == [1, 2, 3]
        finally:
            reopened.close()
    finally:
        unregister_extension_codec("matrix")


def test_chunked_layout_carries_all_new_types(tmp_path):
    import datetime as dt
    from decimal import Decimal

    register_extension_codec("chunked", lambda value: value.encode(), lambda raw: raw.decode())
    try:
        path = str(tmp_path / "chunked")
        env = dict(os.environ)
        env["APEX_CHUNK_INDEX"] = "1"
        script = (
            "import datetime as dt, json, sys\n"
            "from decimal import Decimal\n"
            "from apexbase import ApexClient, register_extension_codec\n"
            "register_extension_codec('chunked', lambda v: v.encode(), lambda b: b.decode())\n"
            f"c=ApexClient({path!r}, drop_if_exists=True, enable_cache=False)\n"
            "c.create_table('t', {'id':'int64','d':'date','ts':'timestamp','vals':'list<int64>','dec':'decimal(10,2)','payload':'extension<chunked>'})\n"
            "assert c.set_compression('zstd') is not False\n"
            "c.store({'id':[1,2],'d':[dt.date(2024,2,3),dt.date(2024,2,4)],'ts':[dt.datetime(2024,2,3,4,5,6),dt.datetime(2024,2,4,5,6,7)],'vals':[[1,2],[3,4]],'dec':[Decimal('1.25'),Decimal('2.50')],'payload':['a','b']})\n"
            "c.flush()\n"
            "c.store({'id':[3],'d':[dt.date(2024,2,5)],'ts':[dt.datetime(2024,2,5,6,7,8)],'vals':[[5,6]],'dec':[Decimal('3.75')],'payload':['c']})\n"
            "c.flush()\n"
            "c.close()\n"
            "c=ApexClient(" + f"{path!r}" + ", enable_cache=False)\n"
            "c.use_table('t')\n"
            "rows=c.execute('SELECT * FROM t ORDER BY id').to_dict()\n"
            "arrow=c.execute('SELECT d, ts, vals, dec FROM t ORDER BY id').to_arrow()\n"
            "print(json.dumps({'ids':[row['id'] for row in rows],'vals0':rows[0]['vals'],'dec2_len':len(rows[2]['dec']),'dtype':str(arrow.schema.field('d').type)}))\n"
            "c.close()\n"
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            env=env,
            timeout=180,
        )
        assert completed.returncode == 0, completed.stderr
        payload = json.loads(completed.stdout.strip().splitlines()[-1])
        assert payload["ids"] == [1, 2, 3]
        assert payload["vals0"] == [1, 2]
        assert payload["dec2_len"] == 16
        assert payload["dtype"].startswith("date32")
    finally:
        unregister_extension_codec("chunked")


def test_layout_switches_fall_back_to_the_same_answers(tmp_path):
    script = (
        "import datetime as dt, json, os, sys\n"
        "from decimal import Decimal\n"
        "from apexbase import ApexClient, register_extension_codec\n"
        "register_extension_codec('layout', lambda v: v.encode(), lambda b: b.decode())\n"
        "c=ApexClient(sys.argv[1], drop_if_exists=True, enable_cache=False)\n"
        "c.create_table('t', {'id':'int64','ts':'timestamp','vals':'list<int64>','payload':'extension<layout>'})\n"
        "c.store({'id':[1,2],'ts':[dt.datetime(2024,1,1,1,1,1),dt.datetime(2024,1,2,2,2,2)],'vals':[[1],[2,3]],'payload':['x','y']})\n"
        "c.flush()\n"
        "rows=c.execute('SELECT * FROM t ORDER BY id').to_dict()\n"
        "print(json.dumps({'ids':[row['id'] for row in rows],'vals':[row['vals'] for row in rows]}))\n"
        "c.close()\n"
    )
    answers = {}
    for label, extra in (
        ("default", {}),
        ("width_v4", {"APEX_WIDTH_FIDELITY": "0"}),
        ("flat", {"APEX_CHUNK_INDEX": "0"}),
    ):
        env = dict(os.environ)
        env.update(extra)
        completed = subprocess.run(
            [sys.executable, "-c", script, str(tmp_path / label)],
            capture_output=True,
            text=True,
            env=env,
            timeout=180,
        )
        assert completed.returncode == 0, completed.stderr
        answers[label] = json.loads(completed.stdout.strip().splitlines()[-1])
    baseline = answers["default"]
    assert baseline["ids"] == [1, 2]
    for label, payload in answers.items():
        assert payload["ids"] == baseline["ids"], label
        assert payload["vals"] == baseline["vals"], label


def _load_rebuild_module():
    import importlib.util

    path = ROOT / "benchmarks" / "rebuild_public_baseline.py"
    spec = importlib.util.spec_from_file_location("rebuild_public_baseline", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_baseline_rebuild_uses_median_of_three_samples():
    module = _load_rebuild_module()

    def report(values):
        return {
            "suite": "apexbase-vs-sqlite-duckdb",
            "config": {"profile": "public"},
            "git": {"commit": "abc"},
            "results": [
                {"query": name, "ApexBase": value, "SQLite": 1.0}
                for name, value in values.items()
            ],
            "vector_similarity": {
                "head_to_head": [
                    {"query": "TopKVector", "ApexBase": values["TopK"]}
                ]
            },
        }

    samples = [
        report({"a": 3.0, "b": 1.0, "TopK": 5.0}),
        report({"a": 1.0, "b": 1.0, "TopK": 5.0}),
        report({"a": 2.0, "b": 1.0, "TopK": 9.0}),
    ]
    merged = module.aggregate_public_baseline(samples)
    values = {row["query"]: row["ApexBase"] for row in module._metric_rows(merged)}
    assert values == {"a": 2.0, "b": 1.0, "TopK": 5.0, "TopKVector": 5.0}
    assert merged["baseline_meta"]["samples"] == 3
    assert merged["baseline_meta"]["metric_count"] == 4
    # Competitor columns are copied from the first sample.
    assert merged["results"][0]["SQLite"] == 1.0

    import pytest as _pytest

    with _pytest.raises(ValueError):
        module.aggregate_public_baseline(samples[:2])
    with _pytest.raises(ValueError):
        module.aggregate_public_baseline(
            [samples[0], samples[1], report({"a": 1.0, "TopK": 1.0})]
        )


def test_docs_and_version_are_consistent():
    read = lambda path: (ROOT / path).read_text(encoding="utf-8")
    readme = read("README.md")
    release_notes = read("docs/release-notes.md")
    storage = read("docs/STORAGE_ARCHITECTURE.md")
    api = read("docs/API_REFERENCE.md")

    # Current-state docs describe V5 as GA / stable, never as a pending preview.
    assert "V5 GA" in readme
    assert "105 tabular metrics" in readme
    assert "execute_stream" in readme
    for text, label in ((readme, "README"), (api, "API"), (storage, "STORAGE")):
        assert "architecture preview" not in text.lower(), label
        assert "newest preview format" not in text.lower(), label
    assert "V5 GA" in release_notes
    assert "[!v2.0.0]" not in release_notes
    assert "## [v2.0.0]" in release_notes
    assert "V5 is GA as of v2.0.0" in release_notes
    assert "stable format" in storage.lower()

    # The new documented surfaces exist in the API reference.
    for section in (
        "Process Memory Admission",
        "Out-of-Core Spill",
        "Streaming Results",
        "Vector Index (IVF)",
        "Hybrid Search",
        "APEX_PROCESS_BUDGET_MB",
        "APEX_SPILL_DIR",
        "execute_stream",
        "create_vector_index",
        "hybrid_search",
    ):
        assert section in api, section

    # Version strings agree across the crate, package and lock files. The
    # version is derived from pyproject.toml (the release workflow already
    # enforces that Cargo.toml matches it) so bumping a release only edits the
    # version sources, not this test. Regex instead of tomllib: `tomllib` only
    # exists in Python 3.11+ and the release matrix still tests 3.9/3.10.
    import re

    project_version = re.search(
        r'(?m)^version\s*=\s*"([^"]+)"', read("pyproject.toml")
    )
    assert project_version, "pyproject.toml has no project version"
    expected = project_version.group(1)
    assert expected.startswith("2."), expected
    for path in ("Cargo.toml", "Cargo.lock"):
        assert f'version = "{expected}"' in read(path), path
        assert 'version = "1.39.0"' not in read(path), path
    assert f'__version__ = "{expected}"' in read("apexbase/python/apexbase/__init__.py")
    assert f"releases/tag/v{expected}" in readme
    assert f"releases/tag/v{expected}" in read("docs/index.md")
    assert f"## [v{expected}]" in release_notes

    # No format-version constant above the stable V5 value exists.

    for path in (ROOT / "apexbase" / "src").rglob("*.rs"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in re.finditer(r"FORMAT_VERSION_(\w+)\s*:\s*u32\s*=\s*(\d+)", text):
            name, value = match.group(1), int(match.group(2))
            if name not in {"V4", "V5"}:
                assert value <= 5, (path, name, value)
