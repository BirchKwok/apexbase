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
    assert module.module_metric_counts(module.PROFILE_PUBLIC) == (70, 32, 6)
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


def test_extended_profile_keeps_diagnostics_available():
    module = load_benchmark_module()

    assert len(module.benchmark_specs_for_profile(module.PROFILE_EXTENDED)) == 102
    assert module.module_metric_counts(module.PROFILE_EXTENDED) == (78, 53, 9)
    assert module.vector_metric_count(module.PROFILE_EXTENDED) == 9
