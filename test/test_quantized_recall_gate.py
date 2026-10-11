"""Coverage for the quantized-recall assertions in the canary/quant gate phase.

The quant phase used to measure latency only: a codec that got faster by
returning wrong neighbours passed the gate. The phase now computes an exact numpy
L2 ground truth over the same corpus and asserts recall@10 against a per-codec
floor. These tests pin the ground truth itself, the recall arithmetic, the floor
table, and the failure path (a tightened floor must fail the run).
"""

import importlib.util
import json
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def canary_bench():
    return _load("bench_perf_canary", ROOT / "benchmarks" / "bench_perf_canary.py")


def test_exact_topk_ids_match_a_brute_force_ranking(canary_bench):
    rng = np.random.default_rng(7)
    vectors = rng.normal(size=(500, 16)).astype(np.float32)
    queries = rng.normal(size=(3, 16)).astype(np.float32)
    ids = canary_bench.exact_topk_ids(vectors, queries, 5)

    assert ids.shape == (3, 5)
    for query_index in range(3):
        distances = ((vectors - queries[query_index]) ** 2).sum(axis=1)
        expected = sorted(
            range(len(vectors)), key=lambda row: (distances[row], row)
        )[:5]
        assert ids[query_index].tolist() == expected
        assert len(set(ids[query_index].tolist())) == 5


def test_recall_at_k_counts_shared_one_based_ids(canary_bench):
    exact = np.array([[0, 1, 2], [3, 4, 5]])
    perfect = (exact + 1).astype(np.int64)
    assert canary_bench.recall_at_k(exact, perfect, 3) == 1.0

    disjoint = np.array([[90, 91, 92], [93, 94, 95]])
    assert canary_bench.recall_at_k(exact, disjoint, 3) == 0.0

    half = np.array([[1, 2, 90], [4, 5, 91]])
    assert canary_bench.recall_at_k(exact, half, 3) == pytest.approx(2 / 3)


def test_every_codec_has_a_conservative_floor(canary_bench):
    floors = canary_bench.QUANTIZED_RECALL_FLOORS
    assert set(floors) == set(canary_bench.QUANTIZED_CODECS)
    for codec, floor in floors.items():
        # A floor at or above the measured values would fail on ordinary fixture
        # variation; one at zero would never catch a regression.
        assert 0.0 < floor < 1.0, (codec, floor)
    # Compression must lose information in this order, so the floors must too.
    assert (
        floors["float16"]
        >= floors["int8"]
        >= floors["turboquant3"]
        >= floors["turboquant2"]
        > floors["bit1"]
    ), floors


def test_quant_phase_reports_recall_and_passes_its_floors(canary_bench, tmp_path, monkeypatch):
    output = tmp_path / "quant.json"
    monkeypatch.setattr(canary_bench, "QUANTIZED_CODECS", ("float16", "int8", "bit1"))
    results, detail = canary_bench.run_quantized_canary(20_000, 1, 2)

    assert len(results) == 3
    assert all(row["category"] == "ApexBase quantized vector" for row in results)
    assert set(detail["recall_at_10"]) == {"float16", "int8", "bit1"}
    assert detail["corpus_rows"] == 20_000
    for codec, measured in detail["recall_at_10"].items():
        assert measured >= canary_bench.QUANTIZED_RECALL_FLOORS[codec], (codec, measured)

    canary_bench.main(
        ["--quant-only", "--rows", "20000", "--warmup", "1", "--iterations", "2",
         "--output", str(output)]
    )
    report = json.loads(output.read_text(encoding="utf-8"))
    # The full codec set is measured by the real run, and the recall block rides
    # along without becoming a latency metric row.
    assert set(report["quantized_recall"]["recall_at_10"]) == set(
        canary_bench.QUANTIZED_CODECS
    )
    assert all("recall" not in row["query"] for row in report["results"])


def test_a_recall_regression_fails_the_phase(canary_bench, monkeypatch):
    monkeypatch.setattr(canary_bench, "QUANTIZED_CODECS", ("bit1",))
    tightened = dict(canary_bench.QUANTIZED_RECALL_FLOORS)
    tightened["bit1"] = 1.01
    monkeypatch.setattr(canary_bench, "QUANTIZED_RECALL_FLOORS", tightened)
    with pytest.raises(AssertionError, match="recall@10"):
        canary_bench.run_quantized_canary(20_000, 1, 1)
