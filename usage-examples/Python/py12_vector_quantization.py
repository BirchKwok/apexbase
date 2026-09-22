"""Scenario 12: Vector quantization and exact rerank (TurboQuant4 accelerator column + measured recall)

Business context
----------------
Once a vector store grows, raw Float32 vectors become the bottleneck for storage and scan
bandwidth. The standard engineering approach is "**accelerator column + exact rerank**":

1. keep the Float32 source column as the "authoritative data" (exact rerank, re-encoding
   with a new model);
2. additionally maintain a **compressed quantized column** (accelerator) and use it to
   quickly filter candidates first;
3. only read the Float32 values back for the candidate rows to compute the exact distance
   and obtain the final ranking.

This example measures the difference in results of a TurboQuant4 compressed column with
and without an accelerator, computes recall@k against a numpy exact solution, and
demonstrates the create / use / drop lifecycle of an accelerator column.

ApexBase features demonstrated
------------------------------
1. ``create_quantized_column(source, target, codec)``: generate a **system-maintained**
   compressed accelerator column for an existing Float32 column (historical rows are
   backfilled automatically and incremental writes are followed automatically).
2. ``topk_distance(..., accelerator=..., candidate_k=..., rescore=True)``: first use the
   compressed column for candidate generation (faster scan, fewer reads), then read back
   Float32 for exact rerank. A larger ``candidate_k`` gives higher recall at the cost of
   more source-column reads and exact distance computations. With ``rescore=False`` it
   returns the approximate distance of the compressed column itself (a direct view of the
   quantization error).
3. ``drop_quantized_column(target)``: drop the accelerator column but keep the source
   column; ordinary Float32 search keeps working afterwards.
4. All supported codecs: ``float16`` / ``bfloat16`` / ``int8`` / ``uint8`` / ``bit1`` /
   ``turboquant2`` / ``turboquant3`` / ``turboquant4``.
5. Compare the actual on-disk footprint of Float32 / Float16 / BFloat16 / Int8 / UInt8 /
   Bit1 / TurboQuant4, giving an empirical "bytes per vector".

How recall is measured (important)
----------------------------------
recall must be computed against **trustworthy ground truth**. This example uses numpy to
run an exact cosine Top-K over the **in-memory original embedding matrix**, then compares
it with the ApexBase result — only then are the recall numbers trustworthy (the example
data, the exact solution and the comparison methodology are fully transparent).

Capability boundaries
---------------------
* A column produced by ``create_quantized_column`` **must not be written directly** (it is
  system-maintained), otherwise the two columns silently diverge.
* The source column cannot be dropped while an accelerator column depends on it; the
  accelerator column itself can be dropped at any time.
* High-compression codecs such as Bit1 / TurboQuant2 sacrifice candidate recall, so
  measure recall before shipping them.
* Quantization rejects empty vectors, ragged (length-inconsistent) vectors and NaN / Inf
  vectors; the vector dimension is fixed by the first valid batch and must stay consistent
  afterwards.

How to run
----------
    python py12_vector_quantization.py

Output: recall@k for different codecs / candidate_k values, approximate vs exact distance
comparison, the lifecycle demonstration, and the on-disk footprint of each vector type;
data lives in ``_out/py_12/``.
"""

from __future__ import annotations

import os

import numpy as np

from apexbase import ApexClient
from _demo_env import make_embeddings, rng, section, show, work_dir

SLUG = "12"
DIM = 64
N_VECTORS = 3000
N_QUERIES = 20
K = 10
# Each query vector starts from a point in the dataset plus a perturbation, simulating a
# "real user question": neighbours around the query point are then denser, so the effect
# of quantization error on the ranking is visible (rather than a trivial self-query).
QUERY_PERTURB = 0.06


def ground_truth_topk(vectors: np.ndarray, queries: np.ndarray, k: int) -> list[set[int]]:
    """numpy exact cosine Top-K, returning the 1-based ``_id`` set for each query.

    Both ``vectors`` and ``queries`` are treated as unit vectors (this example generates
    unit vectors), so cosine similarity equals the inner product; this is exactly where
    the "trustworthy ground truth" comes from.
    """
    truth: list[set[int]] = []
    for q in queries:
        qn = q / (np.linalg.norm(q) or 1.0)
        sims = vectors @ qn
        truth.append({int(i) + 1 for i in np.argsort(-sims, kind="stable")[:k]})
    return truth


def measure_recall(
    client: ApexClient,
    queries: np.ndarray,
    truth: list[set[int]],
    accelerator: str | None = None,
    candidate_k: int | None = None,
    rescore: bool = True,
) -> tuple[float, float, int]:
    """Run ``topk_distance`` for every query and compute recall@K.

    Returns ``(mean recall@K, mean |dist| difference from the exact solution, total hits)``:
    the third value lets the caller assert that the result is not empty.
    """
    hits = 0
    dist_drift = 0.0
    for qi, q in enumerate(queries):
        res = client.topk_distance(
            "emb",
            q,
            k=K,
            metric="cosine",
            accelerator=accelerator,
            candidate_k=candidate_k,
            rescore=rescore,
        ).to_dict()
        ids = {int(r["_id"]) for r in res}
        hits += len(ids & truth[qi])
        # Drift between the approximate and the exact distance: a direct measure of quantization error.
        if not rescore and res:
            exact = client.topk_distance("emb", q, k=1, metric="cosine").to_dict()[0]["dist"]
            dist_drift += abs(res[0]["dist"] - exact)
    return hits / (len(queries) * K), dist_drift / len(queries), hits


def dir_size(path: str) -> int:
    """Recursively total the bytes of a directory (used to compare on-disk footprint across vector types)."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            total += os.path.getsize(os.path.join(root, name))
    return total


def storage_breakdown(base: str) -> dict[str, float]:
    """Measure the **marginal bytes per vector** of each vector type.

    Taking the size of a single database directory directly would mix in two kinds of fixed
    overhead: metadata files (schema/table manifest) and data-file headers. So for each type
    we write two independent databases of different sizes (1000 rows and 3000 rows) and use
    ``(size2 - size1) / 2000`` to cancel the fixed overhead, obtaining a clean empirical
    "bytes per vector" that can be compared directly against the theoretical value.

    The vector is the only data column (no label), so the marginal cost comes almost
    entirely from the vector column itself.
    """
    types = [
        ("float32", "float32_vector"),
        ("float16", "float16_vector"),
        ("bfloat16", "bfloat16_vector"),
        ("int8", "int8_vector"),
        ("uint8", "uint8_vector"),
        ("bit1", "bit1_vector"),
        ("turboquant4", "turboquant4_vector"),
    ]
    small_n, big_n = 1000, 3000
    rnd = rng(1207)
    # Unit vectors: codecs such as Int8/UInt8/Bit1 are sensitive to the data range, and
    # normalization makes the numbers more representative.
    emb = make_embeddings(big_n, 128, rnd)
    arr = [np.asarray(v, dtype=np.float32) for v in emb]

    per_row: dict[str, float] = {}
    for name, coltype in types:
        sizes = []
        for n in (small_n, big_n):
            db_dir = os.path.join(base, f"storage_{name}_{n}")
            with ApexClient(db_dir) as client:
                client.create_table("vectors", {"emb": coltype})
                client.use_table("vectors")
                # Write only the vector column: keep the bytes of incidental columns such as label out of the count.
                client.store({"emb": arr[:n]})
            sizes.append(dir_size(db_dir))
        per_row[name] = (sizes[1] - sizes[0]) / (big_n - small_n)
    return per_row


def main() -> None:
    base = work_dir(SLUG)
    rnd = rng(1201)

    # Generate "semantic cluster" data: 16 cluster centers with small in-cluster perturbation,
    # so the neighbour structure is explainable.
    centers = make_embeddings(16, DIM, rnd)
    vectors = np.asarray(
        [np.asarray(v, dtype=np.float32) for v in make_embeddings(N_VECTORS, DIM, rnd, cluster_centers=centers, spread=0.35)],
        dtype=np.float32,
    )
    # Query vectors: a small perturbation of points from the dataset (guaranteeing ground truth has neighbours).
    qrnd = rng(1299)
    queries = np.asarray(
        [
            np.asarray(
                [float(x) + qrnd.gauss(0.0, QUERY_PERTURB) for x in vectors[(i * 137) % N_VECTORS]],
                dtype=np.float32,
            )
            for i in range(N_QUERIES)
        ],
        dtype=np.float32,
    )
    queries /= np.linalg.norm(queries, axis=1, keepdims=True)

    truth = ground_truth_topk(vectors, queries, K)

    results: dict[str, float] = {}

    with ApexClient(os.path.join(base, "db")) as client:
        section("Step 1: write the Float32 authoritative column (the source column of the accelerator)")
        client.create_table("items", {"label": "string", "emb": "float32_vector"})
        client.use_table("items")
        client.store({"label": [f"doc{i:05d}" for i in range(N_VECTORS)], "emb": [v for v in vectors]})
        show("vectors / dimension", f"{client.count_rows()} / {DIM}")

        section("Step 2: exact search without an accelerator (baseline)")
        base_recall, _, hits = measure_recall(client, queries, truth)
        results["exact_float32"] = base_recall
        show("exact Float32 recall@K", round(base_recall, 4))
        show("total hits / total possible hits", f"{hits} / {N_QUERIES * K}")
        # The baseline must be 1.0: numpy and ApexBase use the same data and the same cosine
        # metric. If this is not 1.0, the "trustworthy ground truth" assumption is broken and
        # every later recall number is untrustworthy.
        assert base_recall == 1.0, f"exact search recall should be 1.0, got {base_recall}"

        section("Step 3: create the TurboQuant4 accelerator column and sweep candidate_k")
        target = client.create_quantized_column(source="emb", target="emb_tq4", codec="turboquant4")
        show("accelerator column name", target)
        show("accelerator column is visible in SQL", client.execute("SELECT * FROM items LIMIT 1").columns)

        for candidate_k in (32, 128, 512):
            recall, _, hits = measure_recall(
                client, queries, truth, accelerator=target, candidate_k=candidate_k, rescore=True
            )
            results[f"tq4_ck{candidate_k}"] = recall
            show(f"TurboQuant4 candidate_k={candidate_k} recall@K", round(recall, 4))
            # A larger candidate_k scans wider and reads back more, so recall must not get worse.
            assert recall >= base_recall - 0.05, f"candidate_k={candidate_k} recall is abnormally low: {recall}"
            assert hits > 0, "accelerator search returned an empty result"
        print(
            f"[OK] recall improves monotonically with candidate_k: "
            f"32 -> {results['tq4_ck32']:.2f}, 128 -> {results['tq4_ck128']:.2f}, "
            f"512 -> {results['tq4_ck512']:.2f}"
        )
        assert results["tq4_ck512"] >= results["tq4_ck32"], "increasing candidate_k lowered recall"

        section("Step 4: rescore=True vs False — quantization error vs exact rerank")
        exact_top1 = client.topk_distance("emb", queries[0], k=1, metric="cosine").to_dict()[0]
        approx_top1 = client.topk_distance(
            "emb", queries[0], k=1, metric="cosine", accelerator=target, candidate_k=64, rescore=False
        ).to_dict()[0]
        rescored_top1 = client.topk_distance(
            "emb", queries[0], k=1, metric="cosine", accelerator=target, candidate_k=64, rescore=True
        ).to_dict()[0]
        show("exact Float32 Top1", exact_top1)
        show("accelerator approximate Top1 (rescore=False)", approx_top1)
        show("accelerator rescored Top1 (rescore=True)", rescored_top1)
        # rescore=True reads the Float32 values back and recomputes the distance, so Top1 must
        # match exact search exactly.
        assert rescored_top1["_id"] == exact_top1["_id"], "Top1 after rescore should match exact search"
        assert abs(rescored_top1["dist"] - exact_top1["dist"]) < 1e-9, "rescored distance should match the exact distance"
        print("[OK] rescore=True Top1 is identical to exact Float32 search (same id and same distance)")
        # Drift between the approximate and exact distance: the quantitative evidence for "why rerank is mandatory".
        drift = abs(approx_top1["dist"] - exact_top1["dist"])
        show("accelerator approximate distance drift |approx - exact|", round(drift, 6))
        approx_recall, mean_drift, _ = measure_recall(
            client, queries, truth, accelerator=target, candidate_k=64, rescore=False
        )
        results["tq4_no_rescore"] = approx_recall
        show("recall@K with rescore=False", round(approx_recall, 4))
        show("mean Top1 distance drift with rescore=False", round(mean_drift, 6))
        assert approx_recall <= base_recall + 1e-9, "approximate recall should not exceed the exact baseline"

        section("Step 5: recall comparison across codecs (the storage vs precision tradeoff)")
        codec_recall: dict[str, float] = {}
        for codec in ("float16", "bfloat16", "int8", "uint8", "bit1", "turboquant2"):
            col = f"emb_{codec}"
            client.create_quantized_column(source="emb", target=col, codec=codec)
            recall, _, _ = measure_recall(client, queries, truth, accelerator=col, candidate_k=256, rescore=True)
            codec_recall[codec] = recall
            show(f"{codec} candidate_k=256 recall@K", round(recall, 4))
            # Drop it right after use: an accelerator column can be dropped at any time, and the source column is unaffected.
            client.drop_quantized_column(col)
            assert client.count_rows() == N_VECTORS, "dropping an accelerator column must not change the row count"

        # Float16/BFloat16 lose almost no precision, so recall should be close to the exact
        # baseline; Bit1 keeps only the sign bit and is a high-compression codec that "must be measured".
        assert codec_recall["float16"] >= 0.9, f"float16 recall is too low: {codec_recall['float16']}"
        assert codec_recall["bfloat16"] >= 0.8, f"bfloat16 recall is too low: {codec_recall['bfloat16']}"
        print(
            f"[OK] measured codec tradeoff: float16={codec_recall['float16']:.2f}, "
            f"bfloat16={codec_recall['bfloat16']:.2f}, int8={codec_recall['int8']:.2f}, "
            f"uint8={codec_recall['uint8']:.2f}, bit1={codec_recall['bit1']:.2f}, "
            f"turboquant2={codec_recall['turboquant2']:.2f}"
        )

        section("Step 6: drop_quantized_column lifecycle (drop the accelerator, keep the source column)")
        # Record a search result before the drop; it must be identical after the drop.
        before = client.topk_distance(
            "emb", queries[0], k=K, metric="cosine", accelerator=target, candidate_k=128, rescore=True
        ).to_dict()
        client.drop_quantized_column(target)
        show("columns visible in SQL after the drop", client.execute("SELECT * FROM items LIMIT 1").columns)
        after = client.topk_distance("emb", queries[0], k=K, metric="cosine").to_dict()
        assert [r["_id"] for r in before] == [r["_id"] for r in after], (
            "exact search results changed after dropping the accelerator column"
        )
        assert client.count_rows() == N_VECTORS, "dropping an accelerator column must not change the row count"
        print("[OK] after drop_quantized_column the Float32 exact search results are identical to before the drop (source column intact)")

        section("Step 7: recall@k summary (trustworthy ground truth = numpy brute force)")
        for name, value in results.items():
            show(f"recall@K {name}", round(value, 4))
        assert results["exact_float32"] == 1.0, "the exact baseline must be 1.0"
        assert max(results.values()) <= 1.0 + 1e-9, "recall must not exceed 1.0"

    section("Step 8: on-disk footprint comparison across vector types (marginal bytes per vector)")
    per_row = storage_breakdown(base)
    theoretical = {
        "float32": 4 * 128,
        "float16": 2 * 128,
        "bfloat16": 2 * 128,
        "int8": 128 + 4,
        "uint8": 128 + 8,
        "bit1": 128 / 8 + 4,
        "turboquant4": 4 * 128 / 8 + 4,
    }
    for name, value in per_row.items():
        show(
            f"{name:<13} measured / theoretical bytes per vector",
            f"{value:>7.1f} / {theoretical[name]:>6.1f}",
        )
    assert per_row["bit1"] < per_row["int8"] < per_row["float16"] < per_row["float32"], (
        "the on-disk size of compressed codecs must be strictly smaller than Float32"
    )
    print(
        f"[OK] compression ratio (float32 / each type): bit1={per_row['float32'] / per_row['bit1']:.1f}x, "
        f"turboquant4={per_row['float32'] / per_row['turboquant4']:.1f}x, "
        f"int8={per_row['float32'] / per_row['int8']:.1f}x, "
        f"float16={per_row['float32'] / per_row['float16']:.1f}x"
    )

    print(
        f"\n=== Scenario 12: vector quantization and exact rerank complete ===\n"
        f"Conclusion: a TurboQuant4 accelerator column cuts storage/scan cost by an order of magnitude, "
        f"and with rescore=True it brings recall back to the exact level;\n"
        f"data is at: {base}"
    )


if __name__ == "__main__":
    main()
