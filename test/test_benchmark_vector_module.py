import importlib.util
from functools import lru_cache
from pathlib import Path

import numpy as np
import pytest


@lru_cache(maxsize=1)
def load_benchmark_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "benchmarks" / "bench_vs_sqlite_duckdb.py"
    spec = importlib.util.spec_from_file_location("bench_vs_sqlite_duckdb", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_default_vector_rows_defaults_to_1m_floor():
    module = load_benchmark_module()

    assert module.default_vector_rows(50_000) == 1_000_000
    assert module.default_vector_rows(200_000) == 1_000_000
    assert module.default_vector_rows(800_000) == 1_000_000
    assert module.default_vector_rows(1_500_000) == 1_500_000
    assert module.QUANTIZED_VECTOR_ROWS_DEFAULT == 1_000_000


def test_generate_vector_data_is_deterministic_and_shaped():
    module = load_benchmark_module()

    vecs1, query1, batch1 = module.generate_vector_data(8, 4, seed=7)
    vecs2, query2, batch2 = module.generate_vector_data(8, 4, seed=7)

    assert vecs1.shape == (8, 4)
    assert query1.shape == (4,)
    assert batch1.shape == (module.VECTOR_BATCH_QUERY_COUNT, 4)
    assert np.array_equal(vecs1, vecs2)
    assert np.array_equal(query1, query2)
    assert np.array_equal(batch1, batch2)


def test_build_duckdb_vector_sql_uses_expected_functions():
    module = load_benchmark_module()
    query = np.array([0.1, 0.2, 0.3], dtype=np.float32)

    sql_l2 = module.build_duckdb_vector_sql(query, 10, "l2")
    sql_cos = module.build_duckdb_vector_sql(query, 5, "cosine")
    sql_dot = module.build_duckdb_vector_sql(query, 3, "dot")

    assert "array_distance" in sql_l2
    assert "::FLOAT[3]" in sql_l2
    assert "LIMIT 10" in sql_l2
    assert "array_cosine_distance" in sql_cos
    assert "LIMIT 5" in sql_cos
    assert "array_negative_inner_product" in sql_dot
    assert "LIMIT 3" in sql_dot

    with pytest.raises(ValueError):
        module.build_duckdb_vector_sql(query, 10, "l1")


def test_public_profile_matches_readme_scoreboard_shape():
    module = load_benchmark_module()

    assert len(module.PUBLIC_OLAP_BENCHMARK_NAMES) == 70
    assert len(module.OLTP_FAIR_BENCHMARK_NAMES) == 32
    assert len(module.benchmark_specs_for_profile(module.PROFILE_PUBLIC)) == 102
    assert module.module_metric_counts(module.PROFILE_PUBLIC) == (70, 32, 14)
    assert module.vector_metric_sets(module.PROFILE_PUBLIC) == (
        [
            ("TopK L2", "l2"),
            ("TopK Cosine", "cosine"),
            ("TopK Dot", "dot"),
        ],
        [
            ("Batch TopK L2 (10 queries)", "l2"),
            ("Batch TopK Cosine (10 queries)", "cosine"),
            ("Batch TopK Dot (10 queries)", "dot"),
        ],
        [],
    )
    assert module.vector_metric_count(module.PROFILE_PUBLIC) == 6
    assert module.public_vector_metric_count(module.PROFILE_PUBLIC) == 14


def test_extended_profile_keeps_diagnostics_available():
    module = load_benchmark_module()

    assert len(module.benchmark_specs_for_profile(module.PROFILE_EXTENDED)) == 102
    assert module.module_metric_counts(module.PROFILE_EXTENDED) == (78, 53, 17)
    assert module.vector_metric_count(module.PROFILE_EXTENDED) == 9


def test_public_entrypoint_renders_embedded_quantized_results(monkeypatch, capsys):
    module = load_benchmark_module()

    class QuantizationStub:
        @staticmethod
        def benchmark(*args):
            assert args == (100, 8, 2, 3, 10, 7, 1, 2, "all")
            return {
                "apexbase": [
                    {"codec": "int8", "quantized_ms_per_query": 0.25,
                     "recall_at_k": 1.0},
                    {"codec": "float16", "quantized_ms_per_query": 0.5,
                     "recall_at_k": 1.0},
                ],
                "sqlite_vector": [
                    {"codec": "int8", "quantized_ms_per_query": 1.0,
                     "recall_at_k": 0.9},
                ],
            }

    monkeypatch.setattr(module, "_quantization_benchmark_module", lambda: QuantizationStub)
    result = module.run_quantized_vector_benchmarks(100, 8, 2, 3, 10, 7, 1, 2)

    assert result["summary"] == {"wins": 1, "ties": 0, "slower": 0, "total": 1}
    rendered = capsys.readouterr().out
    assert "Quantized Vector L2 Module" in rendered
    assert "int8" in rendered
    assert "4.00x" in rendered


def test_sqliteai_vector_metric_options_cover_head_to_head_metrics():
    module = load_benchmark_module()

    head_metrics, batch_metrics, _ = module.vector_metric_sets(module.PROFILE_PUBLIC)
    for _, metric in list(head_metrics) + list(batch_metrics):
        assert metric in module.VECTOR_SQLITE_DISTANCE_OPTIONS


def test_build_sqliteai_vector_sql_uses_per_metric_full_scan_tables():
    module = load_benchmark_module()

    sql_l2 = module.build_sqliteai_vector_sql("l2", 10)
    sql_cos = module.build_sqliteai_vector_sql("cosine", 5)
    sql_dot = module.build_sqliteai_vector_sql("dot", 3)

    assert "vector_full_scan('vec_l2'" in sql_l2
    assert "?," in sql_l2 and ", 10)" in sql_l2
    assert "t.id = v.rowid" in sql_l2
    assert "vec_cosine" in sql_cos and ", 5)" in sql_cos
    assert "vec_dot" in sql_dot and ", 3)" in sql_dot


def _require_sqliteai_vector(module):
    if module.locate_sqliteai_vector_binary() is None:
        pytest.skip("sqliteai-vector is not installed in this environment")


def test_sqliteai_connection_falls_back_to_apsw_without_stdlib_extension_support(
    monkeypatch,
):
    import sqlite3
    import sys
    import types

    module = load_benchmark_module()
    closed = []

    class LimitedConnection:
        def close(self):
            closed.append(True)

    fallback = object()
    monkeypatch.setattr(sqlite3, "connect", lambda _: LimitedConnection())
    monkeypatch.setitem(
        sys.modules,
        "apsw",
        types.SimpleNamespace(Connection=lambda _: fallback),
    )

    assert module.connect_extension_capable_sqlite() is fallback
    assert closed == [True]


def test_sqliteai_binary_lookup_supports_namespace_packages(monkeypatch, tmp_path):
    import types

    module = load_benchmark_module()
    binary = tmp_path / "vector.dylib"
    binary.write_bytes(b"test")
    namespace = types.SimpleNamespace(__path__=[str(tmp_path)], __spec__=None)
    monkeypatch.setattr(module.importlib, "import_module", lambda _: namespace)

    assert module.locate_sqliteai_vector_binary() == str(binary)


def test_sqliteai_vector_topk_matches_bruteforce():
    module = load_benchmark_module()
    _require_sqliteai_vector(module)

    vecs, query, _ = module.generate_vector_data(200, 8, seed=11)
    con = module.setup_sqliteai_vector_bench(vecs)
    try:
        for metric in ("l2", "cosine", "dot"):
            rows = module.bench_sqliteai_vector_query(con, query, 5, metric)
            got_ids = list(rows.column("id").to_pylist()) if hasattr(rows, "column") else [r[0] for r in rows]

            d = vecs - query.reshape(1, -1)
            if metric == "l2":
                dist = np.linalg.norm(d, axis=1)
            elif metric == "cosine":
                dist = 1 - (vecs @ query) / (np.linalg.norm(vecs, axis=1) * np.linalg.norm(query))
            else:
                dist = -(vecs @ query)
            expected_ids = [int(i) for i in np.argsort(dist)[:5]]

            assert got_ids == expected_ids, f"{metric} top-k mismatch"
    finally:
        con.close()


def test_sqliteai_vector_batch_query_runs_all_queries():
    module = load_benchmark_module()
    _require_sqliteai_vector(module)

    vecs, _, batch_queries = module.generate_vector_data(64, 8, seed=12)
    con = module.setup_sqliteai_vector_bench(vecs)
    try:
        results = [
            module.bench_sqliteai_vector_query(con, q, 3, "cosine")
            for q in batch_queries[:2]
        ]
        assert len(results) == 2
        module.bench_sqliteai_batch_vector_query(con, batch_queries[:2], 3, "cosine")
    finally:
        con.close()
