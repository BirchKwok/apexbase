"""Static contracts for the phase-two and phase-three architecture boundaries."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1] / "apexbase" / "src"


def _rust_files(relative_dir):
    return sorted((ROOT / relative_dir).rglob("*.rs"))


def test_storage_layer_has_no_query_runtime_dependency():
    for path in _rust_files("storage"):
        source = path.read_text(encoding="utf-8")
        assert "crate::query" not in source, path
        assert "ApexExecutor" not in source, path


def test_upper_entry_points_use_database_session_facade():
    for relative_dir in ("python", "embedded", "server", "flight"):
        for path in _rust_files(relative_dir):
            source = path.read_text(encoding="utf-8")
            assert "storage::engine::engine()" not in source, path
            assert "TableStorageBackend::" not in source, path
            assert "query::executor::" not in source, path


def test_read_backend_never_compacts_delta():
    source = (ROOT / "storage" / "engine.rs").read_text(encoding="utf-8")
    read_backend = source.split("pub fn get_read_backend", 1)[1].split(
        "fn get_insert_backend", 1
    )[0]
    assert ".compact()" not in read_backend
    assert "TableStorageBackend::open(table_path)" in read_backend


def test_phase_two_parent_files_only_assemble_domain_modules():
    parents = {
        ROOT / "query" / "executor" / "aggregation.rs": "pipeline",
        ROOT / "query" / "executor" / "dml.rs": "coordination",
        ROOT / "storage" / "on_demand" / "mmap_scan.rs": "statistics",
        ROOT / "python" / "bindings.rs": "wrapper",
    }
    for path, expected_module in parents.items():
        source = path.read_text(encoding="utf-8")
        assert expected_module in source
        assert "impl " not in source
        assert "fn " not in source
