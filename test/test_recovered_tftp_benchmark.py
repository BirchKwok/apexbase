import importlib.util
from pathlib import Path
import sys


BENCHMARK = Path(__file__).parents[1] / "benchmarks" / "recovered_tftp_olap_benchmark.py"


def _load_benchmark_module():
    spec = importlib.util.spec_from_file_location("recovered_tftp_olap_benchmark", BENCHMARK)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_full_setup_reuse_requires_full_row_manifest(tmp_path):
    benchmark = _load_benchmark_module()
    source = tmp_path / "TFTP.csv"
    source.write_text("header\n")
    stat = source.stat()
    manifest = {
        "rows": 1_000_000,
        "source_size": stat.st_size,
        "source_mtime_ns": stat.st_mtime_ns,
    }

    assert benchmark.setup_matches(manifest, source, 1_000_000)
    assert not benchmark.setup_matches(manifest, source, 0)

    manifest["rows"] = benchmark.FULL_CSV_ROWS
    assert benchmark.setup_matches(manifest, source, 0)


def test_percentile_parity_uses_exact_reference_quantiles():
    benchmark = _load_benchmark_module()
    percentile = next(query for query in benchmark.NATIVE_QUERIES if query.name == "percentiles")
    assert "PERCENTILE_APPROX" in percentile.apex_sql
    assert "quantile_cont" in percentile.duck
    assert not percentile.approximate
