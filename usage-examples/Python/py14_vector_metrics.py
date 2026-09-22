"""Scenario 14: comparing vector distance metrics (l2 / l2_squared / cosine / dot / l1 / linf)

Business context
----------------
"Which distance metric should I pick?" is one of the most common — and most
error-prone — decisions in vector retrieval:

* embedding models are usually trained with **cosine**, and L2 then produces a similar
  ranking (provided the vectors are normalized);
* if the vectors are **not normalized**, L2 is dominated by vector length and the
  ranking can differ completely from cosine;
* the inner product (dot) is often used in recommender systems, but it is a
  "similarity" rather than a "distance", and it is biased towards long vectors;
* L1 / Linf are more common for sparse, interpretable features or for adversarial
  robustness.

This example runs TopK with all 6 metrics over the same data set, verifies their
**ordering relationships and mathematical properties**, and states the empirical rule
for "when they can be swapped and when they cannot".

ApexBase features demonstrated
-----------------------------
1. ``topk_distance(..., metric=...)`` supports
   ``l2`` / ``l2_squared`` / ``cosine`` / ``dot`` / ``l1`` / ``linf``
   (plus aliases such as ``euclidean`` / ``manhattan`` / ``chebyshev`` / ``inner_product``).
2. Two tables over the same data: a **unit-vector table** and a **non-normalized table**,
   showing the effect of normalization directly.
3. Recomputing all six distances term by term in numpy from their definitions and
   comparing them one by one against the values returned by ApexBase — this validates the
   metric semantics and also shows how to compute your own ground truth.
4. SQL-side distance functions (``array_distance`` / ``l1_distance`` / ``linf_distance`` /
   ``cosine_distance`` / ``dot_product`` / ``l2_squared_distance``) cross-checked against
   the ``topk_distance`` metrics.

Mathematical properties (asserted one by one in this example)
-------------------------------------------------------------
For vectors a and b (let ``d = a - b``):

* ``l2_squared == l2 ** 2``;
* ``linf <= l2 <= l1`` (the element-wise norm inequality chain);
* for unit vectors ``l2^2 = 2 - 2*cos_sim``, so **l2 and cosine produce exactly the same
  TopK ordering**;
* for unit vectors ``dot_sim = cos_sim``, so ApexBase's ``dot`` distance
  (which returns ``-a.b``) orders identically to ``cosine``;
* without normalization, the ``dot`` and ``cosine`` orderings are **no longer equivalent**
  (the length term enters the ordering).

Capability boundaries
---------------------
* Metric names must be supported strings; a typo raises an error instead of silently
  falling back.
* ``dot`` returns ``-a.b`` (smaller is nearer) so that the "smaller distance is better"
  ordering semantics stay uniform, but that is **not** the inner product itself; use the
  SQL ``dot_product(...)`` when you need the true inner product.
* Vector columns read back as ``None`` over SQL (opaque binary columns), so per-vector
  comparison must go through distance functions or a numpy copy.

How to run
----------
    python py14_vector_metrics.py

Output: the TopK and distances for the six metrics, the comparison against the numpy
definitions, and the ordering-equivalence conclusions for the unit-vector and
non-normalized cases; data lives in ``_out/py_14/``.
"""

from __future__ import annotations

import os

import numpy as np

from apexbase import ApexClient
from _demo_env import make_embeddings, rng, section, show, vector_literal, work_dir

SLUG = "14"
DIM = 64
N_VECTORS = 1000
K = 10
METRICS = ["l2", "l2_squared", "cosine", "dot", "l1", "linf"]


# ---------------------------------------------------------------------------
# numpy side: recompute the six metrics term by term from their definitions
# (this is the ground truth)
# ---------------------------------------------------------------------------
def np_distances(kind: str, a: np.ndarray, b: np.ndarray) -> float:
    """Compute the metric between a and b from its mathematical definition, with the same
    semantics as ApexBase's metric.

    * ``l2``         Euclidean distance ``||a-b||_2``
    * ``l2_squared`` squared Euclidean distance ``||a-b||_2^2``
    * ``cosine``     cosine distance ``1 - a.b/(||a||*||b||)``
    * ``dot``        ApexBase convention: returns ``-a.b`` (smaller is nearer)
    * ``l1``         Manhattan distance ``||a-b||_1``
    * ``linf``       Chebyshev distance ``max|a-b|``
    """
    d = a.astype(np.float64) - b.astype(np.float64)
    if kind == "l2":
        return float(np.sqrt(np.dot(d, d)))
    if kind == "l2_squared":
        return float(np.dot(d, d))
    if kind == "cosine":
        na = float(np.linalg.norm(a))
        nb = float(np.linalg.norm(b))
        if na == 0.0 or nb == 0.0:
            return 1.0
        return float(1.0 - float(a.astype(np.float64) @ b.astype(np.float64)) / (na * nb))
    if kind == "dot":
        return float(-(a.astype(np.float64) @ b.astype(np.float64)))
    if kind == "l1":
        return float(np.abs(d).sum())
    if kind == "linf":
        return float(np.abs(d).max())
    raise ValueError(f"unknown metric: {kind}")


def np_topk(kind: str, vectors: np.ndarray, query: np.ndarray, k: int) -> tuple[list[int], list[float]]:
    """Brute-force numpy TopK: returns (1-based _id list, distance list), ascending by distance."""
    scored = [(np_distances(kind, query, v), i + 1) for i, v in enumerate(vectors)]
    # Break distance ties by ascending _id so the result is comparable with ApexBase's
    # stable sort.
    scored.sort(key=lambda x: (x[0], x[1]))
    top = scored[:k]
    return [i for _, i in top], [d for d, _ in top]


def unit_rows(x: np.ndarray) -> np.ndarray:
    """L2-normalize row-wise; zero vectors are returned unchanged to avoid division by zero."""
    norms = np.linalg.norm(x, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    return x / norms


def main() -> None:
    base = work_dir(SLUG)
    rnd = rng(1401)

    # Unit-vector table: "direction" is all that matters (the usual convention in embedding retrieval)
    units = np.asarray([np.asarray(v, dtype=np.float32) for v in make_embeddings(N_VECTORS, DIM, rnd)], dtype=np.float32)
    # Non-normalized table: same directions as units but row-scaled lengths (mimicking raw,
    # unnormalized embeddings)
    scales = np.asarray([0.25 + (i % 8) * 0.5 for i in range(N_VECTORS)], dtype=np.float32)
    raws = units * scales[:, None]

    # Queries: 4 unit vectors + 4 non-normalized vectors (to exercise both cases separately)
    qrnd = rng(1499)
    all_q = np.asarray(
        [
            np.asarray([float(x) + qrnd.gauss(0.0, 0.1) for x in units[(i * 89) % N_VECTORS]], dtype=np.float32)
            for i in range(8)
        ],
        dtype=np.float32,
    )
    unit_queries = np.asarray(unit_rows(all_q[:4]), dtype=np.float32)   # normalized queries
    raw_queries = np.asarray(all_q[4:] * np.asarray([[1.5], [3.0], [0.5], [2.0]], dtype=np.float32), dtype=np.float32)

    with ApexClient(os.path.join(base, "db")) as client:
        section("Step 1: create two tables (unit vectors / non-normalized) over the same directions")
        for table, data in (("unit_items", units), ("raw_items", raws)):
            client.create_table(table, {"label": "string", "emb": "float32_vector"})
            client.use_table(table)
            client.store({"label": [f"{table}_{i:05d}" for i in range(N_VECTORS)], "emb": [v for v in data]})
            show(f"{table} row count", client.count_rows())
        show("unit-vector norms (should all be 1)", f"min={np.linalg.norm(units, axis=1).min():.6f}, max={np.linalg.norm(units, axis=1).max():.6f}")
        show("non-normalized vector norm range", f"{np.linalg.norm(raws, axis=1).min():.3f} ~ {np.linalg.norm(raws, axis=1).max():.3f}")

        # ================= Step 2: ordering relationships of the six metrics on unit vectors =================
        section("Step 2: unit vectors — TopK and distances for the six metrics (compared with the numpy definitions)")
        client.use_table("unit_items")
        q = unit_queries[0]
        per_metric_unit: dict[str, tuple[list[int], list[float]]] = {}
        for metric in METRICS:
            rows = client.topk_distance("emb", q, k=K, metric=metric).to_dict()
            ids = [int(r["_id"]) for r in rows]
            dists = [float(r["dist"]) for r in rows]
            per_metric_unit[metric] = (ids, dists)
            # ---- Self-check: ApexBase distances == numpy values recomputed from the definitions ----
            expect_ids, expect_dists = np_topk(metric, units, q, K)
            for got_id, got_d, exp_d in zip(ids, dists, expect_dists):
                assert abs(got_d - exp_d) < 1e-4, (
                    f"metric={metric} distance mismatch: _id={got_id} apex={got_d} numpy={exp_d}"
                )
            # Distances must be ascending (nearest neighbor first)
            assert dists == sorted(dists), f"metric={metric} results are not sorted by ascending distance"
            show(f"{metric:<11} TopK (_id)", ids)
            show(f"{metric:<11} distances", [round(d, 6) for d in dists])

        # ---- Self-check 1: on unit vectors, cosine / l2 / l2_squared / dot orderings are fully equivalent ----
        ref_ids = per_metric_unit["cosine"][0]
        for metric in ("l2", "l2_squared", "dot"):
            assert per_metric_unit[metric][0] == ref_ids, (
                f"on unit vectors the {metric} TopK ordering should equal cosine: "
                f"{per_metric_unit[metric][0]} vs {ref_ids}"
            )
        print("[OK] on unit vectors cosine == l2 == l2_squared == dot in TopK ordering (all four are strictly equivalent)")

        # ---- Self-check 2: analytic relationships between the distance values ----
        cos_d = np.asarray(per_metric_unit["cosine"][1])
        l2_d = np.asarray(per_metric_unit["l2"][1])
        l2s_d = np.asarray(per_metric_unit["l2_squared"][1])
        dot_d = np.asarray(per_metric_unit["dot"][1])
        l1_d = np.asarray(per_metric_unit["l1"][1])
        linf_d = np.asarray(per_metric_unit["linf"][1])
        # Unit vectors: l2^2 = 2 - 2*cos_sim = 2*cosine_distance
        assert np.allclose(l2s_d, 2.0 * cos_d, atol=1e-4), "on unit vectors l2_squared should equal 2 * cosine_distance"
        assert np.allclose(l2_d, np.sqrt(l2s_d), atol=1e-4), "l2 should equal sqrt(l2_squared)"
        # Unit vectors: cos_sim = 1 - cos_d, so dot distance = -cos_sim = cos_d - 1
        assert np.allclose(dot_d, cos_d - 1.0, atol=1e-4), "on unit vectors dot_distance should equal cosine_distance - 1"
        # Norm inequality chain: linf <= l2 <= l1
        assert np.all(linf_d <= l2_d + 1e-6), "linf <= l2 should hold"
        assert np.all(l2_d <= l1_d + 1e-6), "l2 <= l1 should hold"
        print(
            "[OK] analytic relationships hold: l2_squared == 2*cosine, l2 == sqrt(l2_squared), "
            "dot == cosine-1, linf <= l2 <= l1"
        )

        # ---- Self-check 3: SQL-side distance functions cross-check the metrics ----
        # Vector distance functions require an inline array literal; a `?` placeholder would be
        # expanded into one scalar parameter per element and fail, hence vector_literal().
        lit = vector_literal(q, precision=6)
        sql_row = client.execute(
            f"""
            SELECT _id,
                   array_distance(emb, {lit})       AS l2,
                   l2_squared_distance(emb, {lit})  AS l2_squared,
                   cosine_distance(emb, {lit})      AS cosine,
                   dot_product(emb, {lit})          AS dot_raw,
                   l1_distance(emb, {lit})          AS l1,
                   linf_distance(emb, {lit})        AS linf
            FROM unit_items
            WHERE _id = {ref_ids[0]}
            """
        ).to_dict()[0]
        show("SQL distance functions (Top1, _id=%d)" % ref_ids[0], {k: (round(v, 6) if isinstance(v, float) else v) for k, v in sql_row.items()})
        assert abs(sql_row["l2"] - per_metric_unit["l2"][1][0]) < 1e-4, "array_distance disagrees with metric='l2'"
        assert abs(sql_row["l2_squared"] - per_metric_unit["l2_squared"][1][0]) < 1e-4, "l2_squared_distance disagrees with the metric"
        assert abs(sql_row["cosine"] - per_metric_unit["cosine"][1][0]) < 1e-4, "cosine_distance disagrees with the metric"
        assert abs(sql_row["l1"] - per_metric_unit["l1"][1][0]) < 1e-4, "l1_distance disagrees with the metric"
        assert abs(sql_row["linf"] - per_metric_unit["linf"][1][0]) < 1e-4, "linf_distance disagrees with the metric"
        # dot_product is the **true inner product** (a similarity: larger means more similar), the
        # opposite of the negated convention used by metric='dot'.
        assert abs(sql_row["dot_raw"] - (-per_metric_unit["dot"][1][0])) < 1e-4, (
            "dot_product should be the negation of metric='dot'"
        )
        print("[OK] SQL distance functions agree exactly with the topk_distance metrics; dot_product is the true inner product (sign opposite to metric='dot')")

        # ================= Step 3: non-normalized data — orderings are no longer equivalent =================
        section("Step 3: non-normalized vectors — dot / l2 diverge from cosine")
        client.use_table("raw_items")
        qr = raw_queries[0]
        raw_result: dict[str, list[int]] = {}
        for metric in METRICS:
            ids = [int(r["_id"]) for r in client.topk_distance("emb", qr, k=K, metric=metric).to_dict()]
            raw_result[metric] = ids
            # The exact numpy solution must also be computed on the non-normalized data
            exp_ids, _ = np_topk(metric, raws, qr, K)
            assert ids == exp_ids, f"on non-normalized data metric={metric} disagrees with numpy: {ids} vs {exp_ids}"
            show(f"{metric:<11} TopK (_id)", ids)
        print("[OK] all six metrics match the exact numpy solution on non-normalized data too (including dot / l1 / linf)")

        cos_ids = set(raw_result["cosine"])
        dot_overlap = len(cos_ids & set(raw_result["dot"])) / K
        l2_overlap = len(cos_ids & set(raw_result["l2"])) / K
        l1_overlap = len(cos_ids & set(raw_result["l1"])) / K
        show("TopK overlap with cosine on non-normalized data: dot", f"{dot_overlap:.2f}")
        show("TopK overlap with cosine on non-normalized data: l2", f"{l2_overlap:.2f}")
        show("TopK overlap with cosine on non-normalized data: l1", f"{l1_overlap:.2f}")
        # The length term dominates the L2 and inner-product orderings, so the overlap here
        # must be clearly below 1.0.
        assert dot_overlap < 1.0, "without normalization dot and cosine orderings must not be fully equivalent"
        assert l2_overlap < 1.0, "without normalization l2 and cosine orderings must not be fully equivalent"
        print(
            f"[OK] divergence quantified once normalization is removed: dot overlaps cosine only {dot_overlap:.0%}, "
            f"l2 only {l2_overlap:.0%} (the length term enters the ordering)"
        )

        # Normalizing only the **query** is not enough: the |b|^2 term in
        # |a-b|^2 = |a|^2 + |b|^2 - 2a.b is still present, and dot is still missing the
        # division by |b|. This measures that "half normalization" cannot restore equivalence.
        normed_query = (qr / np.linalg.norm(qr)).astype(np.float32)
        normed_ids = {
            metric: [int(r["_id"]) for r in client.topk_distance("emb", normed_query, k=K, metric=metric).to_dict()]
            for metric in ("cosine", "l2", "dot")
        }
        normed_overlap = {
            metric: len(set(normed_ids["cosine"]) & set(normed_ids[metric])) / K
            for metric in ("l2", "dot")
        }
        show("overlap with cosine when only the query is normalized (l2 / dot)", f"{normed_overlap['l2']:.2f} / {normed_overlap['dot']:.2f}")
        assert normed_overlap["l2"] < 1.0 and normed_overlap["dot"] < 1.0, (
            "normalizing only the query must not restore l2/dot equivalence with cosine"
        )
        print(
            "[OK] normalizing only the query vector is **not enough** to restore equivalence: the length "
            "differences on the data side still pollute the l2 and dot orderings; the data must be "
            "normalized too (see the unit_items table) or cosine used directly"
        )

        # ================= Step 4: statistics over multiple queries =================
        section("Step 4: average behavior over 8 queries (unit vectors / non-normalized)")
        unit_agree = 0
        raw_agree = 0
        total = 0
        for qu, qraw in zip(unit_queries, raw_queries):
            client.use_table("unit_items")
            u_cos = {int(r["_id"]) for r in client.topk_distance("emb", qu, k=K, metric="cosine").to_dict()}
            u_l2 = {int(r["_id"]) for r in client.topk_distance("emb", qu, k=K, metric="l2").to_dict()}
            client.use_table("raw_items")
            r_cos = {int(r["_id"]) for r in client.topk_distance("emb", qraw, k=K, metric="cosine").to_dict()}
            r_dot = {int(r["_id"]) for r in client.topk_distance("emb", qraw, k=K, metric="dot").to_dict()}
            unit_agree += len(u_cos & u_l2)
            raw_agree += len(r_cos & r_dot)
            total += K
        show("unit vectors: average TopK overlap between cosine and l2", f"{unit_agree / total:.3f}")
        show("non-normalized: average TopK overlap between cosine and dot", f"{raw_agree / total:.3f}")
        assert unit_agree == total, "on unit vectors cosine and l2 must produce exactly the same TopK"
        assert raw_agree < total, "without normalization cosine and dot must not agree completely"

        section("Step 5: empirical rules for choosing a metric (measured by this example)")
        show("metric", "when to use / caveats")
        show("cosine", "default first choice for embedding retrieval; looks only at direction, unaffected by length")
        show("l2 / l2_squared", "equivalent to cosine when the vectors are normalized; l2_squared saves a square root and is faster for pure ranking")
        show("dot", "common in recommender systems (the inner product is the score); sensitive to vector length, so always normalize")
        show("l1", "sparse / interpretable features; more robust than l2 but costlier and orders differently")
        show("linf", "looks only at the largest single-dimension deviation (anomaly detection / adversarial checks); almost never used to rank semantic similarity")

    print(
        f"\n=== Scenario 14: vector distance metric comparison complete ===\n"
        f"Key takeaway: on unit vectors cosine / l2 / l2_squared / dot are strictly equivalent in TopK;\n"
        f"without normalization dot and l2 are polluted by the length term and diverge visibly from cosine;\n"
        f"Data lives in: {base}"
    )


if __name__ == "__main__":
    main()
