"""Scenario 13: Batch vector search (batch_topk_distance — N queries in one call)

Business context
----------------
An online retrieval service rarely issues "one query at a time". Instead:

* a recommender system fetches the similar neighbors of N candidate items for one user;
* a dedup / clustering service throws thousands of query vectors in at once to find
  nearest neighbors;
* offline evaluation / recall regression runs tens of thousands of queries per pass.

Calling ``topk_distance`` one query at a time means reloading the mmap vector buffer,
re-reading the ``_id`` column, and scanning serially for every single query.
``batch_topk_distance`` packs the N queries into **one Rust call**:

1. the mmap float buffer is loaded **once** (independent of N);
2. all queries are scanned in **parallel** on the Rust side with Rayon (outer-level
   parallelism);
3. the ``_id`` column is read once.

ApexBase features demonstrated
-----------------------------
1. ``batch_topk_distance(col, queries, k, metric)``:
   takes an ``(N, D)`` query matrix and returns an ``(N, K, 2)`` numpy result,
   where ``[:, :, 0]`` is ``_id`` and ``[:, :, 1]`` is the distance (ascending).
2. **Element-wise equivalence** with running ``topk_distance`` query by query
   (batching is not an approximation);
3. comparison against a brute-force numpy solution, confirming the batch API does not
   trade away correctness;
4. a same-machine comparison using Python's ``time.perf_counter`` (the numbers are
   order-of-magnitude references only — the demo data set is small and the clock
   resolution is limited — so no hard performance assertion is made).

Capability boundaries
---------------------
* The batch API does **not** support the ``accelerator`` / ``rescore`` parameters; when
  you need a quantized accelerator column, keep using single-query
  ``topk_distance(..., accelerator=...)``.
* When a query has fewer than k neighbors, the result is padded with ``(-1, inf)``.
* Like the single-query path, the batch API only accepts the native scan path with the
  vector column inside the table, so ``use_table()`` must be called first.

How to run
----------
    python py13_batch_vector_search.py

Output: agreement between the batch and per-query results, latency comparison and
throughput; data lives in ``_out/py_13/``.
"""

from __future__ import annotations

import os
import time

import numpy as np

from apexbase import ApexClient
from _demo_env import make_embeddings, rng, section, show, work_dir

SLUG = "13"
DIM = 128
N_VECTORS = 20_000       # Large enough that the batching amortization is not drowned out by clock resolution
BATCH_QUERIES = 64       # Main batch size (mimics multiple queries inside one retrieval request)
LARGE_BATCH = 256        # Larger batch, to observe how parallel-scan throughput changes
K = 10
REPEATS = 5              # Timing rounds; the median resists jitter


def numpy_topk(vectors: np.ndarray, queries: np.ndarray, k: int) -> np.ndarray:
    """Brute-force exact numpy TopK, returning ``(N, K, 2)`` to match the batch API shape.

    Each row is ``[_id, distance]``; ``_id`` is 1-based and the distance is the cosine
    distance (1 - cos).
    """
    out = np.zeros((queries.shape[0], k, 2), dtype=np.float64)
    for i, q in enumerate(queries):
        qn = q / (np.linalg.norm(q) or 1.0)
        sims = vectors @ qn
        order = np.argsort(-sims, kind="stable")[:k]
        out[i, :, 0] = order + 1
        out[i, :, 1] = 1.0 - sims[order]
    return out


def timed(fn, repeats: int = REPEATS) -> float:
    """Run fn for several rounds and return the **median** elapsed time in seconds.

    Why the median: a single timing round is easily disturbed by OS scheduling and page
    cache state; the median makes the comparison more stable and avoids "picking a
    flattering sample".
    """
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return float(np.median(samples))


def main() -> None:
    base = work_dir(SLUG)
    rnd = rng(1301)

    section(f"Step 1: prepare data — {N_VECTORS} vectors + {LARGE_BATCH} query vectors")
    centers = make_embeddings(24, DIM, rnd)
    vectors = np.asarray(
        [np.asarray(v, dtype=np.float32) for v in make_embeddings(N_VECTORS, DIM, rnd, cluster_centers=centers, spread=0.4)],
        dtype=np.float32,
    )
    # Query vectors: sample the data set at a fixed stride, then add noise (mimics the
    # queries arriving in real requests).
    qrnd = rng(1399)
    queries = np.asarray(
        [
            np.asarray([float(x) + qrnd.gauss(0.0, 0.08) for x in vectors[(i * 47) % N_VECTORS]], dtype=np.float32)
            for i in range(LARGE_BATCH)
        ],
        dtype=np.float32,
    )
    show("vector / query matrix shapes", f"{vectors.shape} / {queries.shape}")

    with ApexClient(os.path.join(base, "db")) as client:
        client.create_table("items", {"label": "string", "emb": "float32_vector"})
        client.use_table("items")
        client.store({"label": [f"item{i:05d}" for i in range(N_VECTORS)], "emb": [v for v in vectors]})
        show("rows stored", client.count_rows())

        batch_q = queries[:BATCH_QUERIES]

        section(f"Step 2: retrieve {BATCH_QUERIES} queries with one batch_topk_distance call")
        start = time.perf_counter()
        batch = client.batch_topk_distance("emb", batch_q, k=K, metric="cosine")
        batch_once = time.perf_counter() - start
        show("return type / shape / dtype", f"{type(batch).__name__} / {batch.shape} / {batch.dtype}")
        show("TopK of the first query (_id, dist)", [(int(i), round(d, 6)) for i, d in batch[0]])
        # Shape contract: (N, K, 2). This is the easiest thing to get wrong when writing a
        # batch service, so assert it explicitly.
        assert batch.shape == (BATCH_QUERIES, K, 2), f"unexpected batch return shape: {batch.shape}"
        assert not np.isinf(batch[:, :, 1]).any(), "inf in the result means some query did not find enough neighbors"

        section(f"Step 3: per-query topk_distance ({BATCH_QUERIES} calls) as the control")
        loop_ids = np.zeros((BATCH_QUERIES, K), dtype=np.int64)
        loop_dists = np.zeros((BATCH_QUERIES, K), dtype=np.float64)
        start = time.perf_counter()
        for i, q in enumerate(batch_q):
            rows = client.topk_distance("emb", q, k=K, metric="cosine").to_dict()
            loop_ids[i] = [int(r["_id"]) for r in rows]
            loop_dists[i] = [float(r["dist"]) for r in rows]
        loop_once = time.perf_counter() - start

        # ---- Self-check 1: batch results are **element-wise equal** to per-query calls
        # (batching is not an approximation) ----
        assert np.array_equal(batch[:, :, 0].astype(np.int64), loop_ids), "batch _ids disagree with per-query calls"
        assert np.allclose(batch[:, :, 1], loop_dists, atol=1e-9), "batch distances disagree with per-query calls"
        print(f"[OK] {BATCH_QUERIES} queries x Top{K}: _ids and distances match per-query topk_distance exactly")

        # ---- Self-check 2: agreement with the brute-force numpy solution (confirms the
        # batch API does not trade away correctness) ----
        truth = numpy_topk(vectors, batch_q, K)
        assert np.array_equal(batch[:, :, 0].astype(np.int64), truth[:, :, 0].astype(np.int64)), (
            "batch results disagree with the brute-force numpy solution"
        )
        assert np.allclose(batch[:, :, 1], truth[:, :, 1], atol=1e-5), (
            "batch distances disagree with the brute-force numpy solution"
        )
        print("[OK] batch results == brute-force numpy cosine TopK (identical _ids, distance error < 1e-5)")

        section("Step 4: multi-round timing comparison (median, same machine, same data)")
        # Note: the latency comparison is only an order-of-magnitude reference. Absolute
        # numbers in a real system depend on data size, memory bandwidth and concurrent
        # load; the median resists jitter here and the assertions stay loose.
        t_batch = timed(lambda: client.batch_topk_distance("emb", batch_q, k=K, metric="cosine"))
        t_loop = timed(
            lambda: [client.topk_distance("emb", q, k=K, metric="cosine") for q in batch_q]
        )
        show(f"first-round latency batch / loop ({BATCH_QUERIES} queries)", f"{batch_once * 1e3:.3f} ms / {loop_once * 1e3:.3f} ms")
        show(f"{REPEATS}-round median batch / loop", f"{t_batch * 1e3:.3f} ms / {t_loop * 1e3:.3f} ms")
        show("speedup (loop / batch)", f"{t_loop / t_batch:.2f}x")
        show("throughput (batch)", f"{BATCH_QUERIES / t_batch:.0f} queries/s")
        show("throughput (per-query calls)", f"{BATCH_QUERIES / t_loop:.0f} queries/s")
        # Only assert "same order of magnitude, no significant regression" so that noise
        # is never turned into a performance conclusion.
        assert t_batch <= t_loop * 1.5, "batch API latency is abnormal (more than 1.5x the per-query calls)"

        section(f"Step 5: batch-size sweep (16 / {BATCH_QUERIES} / {LARGE_BATCH}) — observing amortization")
        # One batch call loads the mmap buffer once, reads the _id column once, and scans
        # the N queries in parallel on the Rust side; therefore the **amortized per-query
        # cost** should fall as the batch grows.
        scaling: list[tuple[int, float]] = []
        for nq in (16, BATCH_QUERIES, LARGE_BATCH):
            t = timed(lambda nq=nq: client.batch_topk_distance("emb", queries[:nq], k=K, metric="cosine"))
            scaling.append((nq, t))
            show(f"batch={nq:<4} median latency / throughput", f"{t * 1e3:>8.3f} ms / {nq / t:>9.0f} q/s")
        per_query_small = scaling[0][1] / scaling[0][0]
        per_query_big = scaling[-1][1] / scaling[-1][0]
        show(f"amortized per-query cost (batch=16 vs batch={LARGE_BATCH})", f"{per_query_small * 1e3:.3f} ms / {per_query_big * 1e3:.3f} ms")
        show("amortization benefit", f"{per_query_small / per_query_big:.2f}x")
        # The assertion is relaxed to 1.5x so machine noise cannot cause a false alarm; the
        # trend is still reported honestly in the printed output.
        assert per_query_big <= per_query_small * 1.5, "per-query cost at the large batch is significantly worse, contradicting amortization"

        # ---- Self-check 3: the small batch is a strict prefix of the large batch (the same
        # queries give reproducible results) ----
        small_batch = client.batch_topk_distance("emb", batch_q, k=K, metric="cosine")
        big_batch = client.batch_topk_distance("emb", queries[:LARGE_BATCH], k=K, metric="cosine")
        assert np.array_equal(small_batch, big_batch[:BATCH_QUERIES]), "the same query gives different results at different batch sizes"
        print("[OK] results for a given query vector do not depend on batch size (reproducible, shardable)")

        section("Step 6: fetch business fields for the batch results (mimicking response assembly in a batch retrieval service)")
        # The batch API only returns _id and distance; business fields are fetched with a
        # single IN query, so "vector search" and "business reads" each take their most
        # suitable path.
        top1_ids = [int(x) for x in big_batch[:, 0, 0]]
        id_list = ",".join(str(i) for i in top1_ids)
        rows = client.execute(
            f"SELECT _id, label FROM items WHERE _id IN ({id_list})"
        ).to_dict()
        show("rows fetched for business fields", len(rows))
        show("sample (first 3)", rows[:3])
        assert len(rows) == LARGE_BATCH, "fetched row count does not match the number of queries"
        assert len({r["_id"] for r in rows}) == LARGE_BATCH, "duplicate _id in the fetched rows"
        print(f"[OK] Top1 of all {LARGE_BATCH} queries was fetched back, with no duplicates")

        # ---- Self-check 4: the batch API's metric parameter takes effect (on unit vectors,
        # l2 and cosine recall the same set) ----
        # Math: for a unit vector b and a fixed query a, |a-b|^2 = |a|^2 + 1 - 2a.b, and
        # |a|^2 is a constant, so the l2 and cosine orderings are monotonically equivalent
        # (py14 validates the metrics more systematically). Neighbors with nearly equal
        # distances may still swap places inside the TopK, so we compare TopK **sets**
        # rather than position-by-position order.
        batch_l2 = client.batch_topk_distance("emb", batch_q, k=K, metric="l2")
        l2_sets = [set(batch_l2[i, :, 0].astype(np.int64).tolist()) for i in range(BATCH_QUERIES)]
        cos_sets = [set(batch[i, :, 0].astype(np.int64).tolist()) for i in range(BATCH_QUERIES)]
        same_sets = sum(1 for a, b in zip(l2_sets, cos_sets) if a == b)
        top1_same = int(np.sum(batch_l2[:, 0, 0] == batch[:, 0, 0]))
        show("queries with identical l2 / cosine TopK sets", f"{same_sets} / {BATCH_QUERIES}")
        show("queries with identical l2 / cosine Top1", f"{top1_same} / {BATCH_QUERIES}")
        assert same_sets == BATCH_QUERIES, "on unit vectors l2 and cosine must recall exactly the same TopK set"
        assert top1_same == BATCH_QUERIES, "on unit vectors l2 and cosine must agree on Top1 for every query"
        print("[OK] metric='l2' and metric='cosine' recall exactly the same TopK sets on unit vectors (the metrics are interchangeable)")

    print(
        f"\n=== Scenario 13: batch vector search (batch_topk_distance) complete ===\n"
        f"Key takeaway: the batch API is element-wise equivalent to per-query calls while skipping\n"
        f"N-1 buffer loads and _id reads, and it lets Rust scan the N queries in parallel; the measured\n"
        f"amortized per-query cost falls as the batch grows, so a batch retrieval service should\n"
        f"accumulate concurrent queries (batching) before calling.\n"
        f"Data lives in: {base}"
    )


if __name__ == "__main__":
    main()
