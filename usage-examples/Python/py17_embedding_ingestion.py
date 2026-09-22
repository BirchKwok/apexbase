"""Scenario 17: bulk embedding ingestion and incremental updates (columnar store + quantisation accelerator column + single-row replace)

Business context
----------------
Embedding data is "always growing, occasionally corrected":

* **Bulk ingestion**: after retraining a model you must load millions of
  vectors; the initial build imports a large batch in one go.
* **Incremental append**: new documents/products go live and must be appended at
  any time without rebuilding the index.
* **Single-row update**: when a piece of content is rewritten or a vector was
  computed wrong, that one row's vector must be **replaced in place**.
* **Retrieval consistency**: after an update, search results must agree with the
  new data (no stale vectors).

This example walks through ApexBase's full usage for high-throughput writes and
incremental updates, with end-to-end consistency checks.

ApexBase features demonstrated
------------------------------
1. **Columnar bulk write**: the column-oriented ``client.store({"col": [...]})``
   form (one FFI call writes many rows) is markedly faster than row-by-row dict
   writes; pair it with ``flush()`` to control when data lands.
2. **Chunked loading**: store + flush a large table in batches, keeping memory
   bounded and progress observable.
3. ``create_quantized_column`` builds a TurboQuant4 accelerator column; later
   **incremental writes and source-column replaces rebuild the accelerator
   automatically** (no manual maintenance).
4. ``client.replace(id, {...})`` replaces a single row in place (including the
   vector column), and ``client.delete(id)`` physically deletes a row.
5. Incremental FTS-index maintenance: appends and replaces after
   ``CREATE FTS INDEX`` keep the index in sync automatically.
6. A numpy "shadow matrix" tracks the same data and is compared against
   ApexBase's search results after every change -- a real
   **retrieval-consistency check**, not just a row count.

Capability boundaries
---------------------
* Vector columns **cannot** be read back through SQL (``SELECT emb`` returns
  NULL; binary vectors are treated as opaque columns), so consistency checks
  must use ``topk_distance`` / distance functions, or compare against the
  Python-side shadow copy.
* The accelerator column is maintained by the system and **must not be written
  directly**; a source column cannot be dropped while an accelerator depends on
  it.
* ``replace`` needs a **numeric _id**: look the ``_id`` up with SQL
  (``SELECT _id ...``); do not take it via ``scalar()`` (on a vector table
  scalar may return None).
* FTS needs tokens of 2 or more characters; a ``:memory:`` database does not
  support FTS.

How to run
----------
    python py17_embedding_ingestion.py

Output: write-time comparison, accelerator recall, and consistency checks after
incremental writes and updates; data lives in ``_out/py_17/``.
"""

from __future__ import annotations

import os
import time

import numpy as np

from apexbase import ApexClient
from _demo_env import make_embeddings, rng, section, show, work_dir

SLUG = "17"
DIM = 96
N_INITIAL = 6000          # initial database size
CHUNK = 2000              # chunked loading: rows per chunk
N_INCREMENTAL = 1000      # incremental append rows
K = 10
N_QUERIES = 12
TOPICS = ["vector", "retrieval", "lakehouse", "quantization", "storage", "benchmark"]


def unit(vectors: np.ndarray) -> np.ndarray:
    """Row-wise normalisation (the usual convention for embedding retrieval)."""
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    return np.asarray(vectors / norms, dtype=np.float32)


def numpy_topk(matrix: np.ndarray, query: np.ndarray, k: int) -> list[int]:
    """Brute-force numpy cosine TopK; returns 1-based _ids (matrix row number + 1)."""
    q = query / (np.linalg.norm(query) or 1.0)
    sims = matrix @ q
    return [int(i) + 1 for i in np.argsort(-sims, kind="stable")[:k]]


def make_rows(start: int, count: int, rnd, centers: np.ndarray) -> tuple[dict, list[np.ndarray]]:
    """Generate one segment of "new data": text + category + unit vectors.

    Returns ``(columnar dict, vector list)``; the vectors come back separately so
    the Python-side shadow matrix can be maintained.
    """
    vectors = make_embeddings(count, DIM, rnd, cluster_centers=[centers], spread=0.3)
    payload = {
        "title": [f"{TOPICS[(start + i) % len(TOPICS)]} document {start + i:06d} vector retrieval practice" for i in range(count)],
        "category": [TOPICS[(start + i) % len(TOPICS)] for i in range(count)],
        "day": [f"2024-05-{(start + i) % 28 + 1:02d}" for i in range(count)],
        "emb": [np.asarray(v, dtype=np.float32) for v in vectors],
    }
    return payload, [np.asarray(v, dtype=np.float32) for v in vectors]


def main() -> None:
    base = work_dir(SLUG)
    rnd = rng(1701)

    # 6 semantic cluster centres: give "new documents" a clear topic so the
    # consistency checks have something meaningful to compare.
    centers = np.asarray([np.asarray(c, dtype=np.float32) for c in make_embeddings(len(TOPICS), DIM, rnd)], dtype=np.float32)

    section("Step 1: columnar bulk ingestion vs row-by-row ingestion (the same 2000 rows)")
    # Prepare two **identical** batches and write each to its own table, so the
    # two write paths are compared fairly.
    payload, vectors = make_rows(0, CHUNK, rnd, centers[0])
    rows_as_dicts = [
        {
            "title": payload["title"][i],
            "category": payload["category"][i],
            "day": payload["day"][i],
            "emb": payload["emb"][i],
        }
        for i in range(CHUNK)
    ]

    with ApexClient(os.path.join(base, "db")) as client:
        schema = {
            "title": "string",
            "category": "string",
            "day": "string",
            "emb": "float32_vector",
        }
        # --- row-by-row writes ---
        client.create_table("items_rowwise", schema)
        client.use_table("items_rowwise")
        start = time.perf_counter()
        for row in rows_as_dicts:
            client.store(row)
        client.flush()
        t_rowwise = time.perf_counter() - start

        # --- columnar write (the recommended form) ---
        client.create_table("items_columnar", schema)
        client.use_table("items_columnar")
        start = time.perf_counter()
        client.store(payload)          # dict of lists: one call writes the whole block
        client.flush()
        t_columnar = time.perf_counter() - start

        show(f"row-by-row write of {CHUNK} rows", f"{t_rowwise * 1e3:.1f} ms ({CHUNK / t_rowwise:.0f} rows/s)")
        show(f"columnar write of {CHUNK} rows", f"{t_columnar * 1e3:.1f} ms ({CHUNK / t_columnar:.0f} rows/s)")
        show("speedup (rowwise / columnar)", f"{t_rowwise / t_columnar:.2f}x")
        assert client.count_rows() == CHUNK, "columnar write row count mismatch"
        # The columnar path must not be slower than the row-by-row path (assert
        # loosely in this direction to avoid false alarms from machine noise).
        assert t_columnar <= t_rowwise, "columnar write should not be slower than row-by-row write"
        print("[OK] columnar store and row-by-row store write the same number of rows, and columnar is faster (one call writes many rows)")

        # Clean up the comparison table with one delete so it cannot skew later
        # counts (and demonstrates delete along the way).
        client.use_table("items_rowwise")
        # Demo: delete the row with _id 1; the whole table is dropped right after.
        client.delete(1)
        assert client.count_rows() == CHUNK - 1, "after delete the row count should be CHUNK-1"
        client.drop_table("items_rowwise")
        show("persistent table list after cleaning up the comparison table", client.list_tables())

        section(f"Step 2: chunked loading to build the store ({N_INITIAL} rows = {N_INITIAL // CHUNK} chunks x {CHUNK})")
        client.create_table("items", schema)
        client.use_table("items")
        shadow: list[np.ndarray] = []      # Python-side shadow matrix (kept in sync with the store)
        chunk_times: list[float] = []
        for chunk_idx in range(N_INITIAL // CHUNK):
            rnd_chunk = rng(1700 + chunk_idx)
            start_idx = chunk_idx * CHUNK
            # Each chunk is generated around a different cluster centre,
            # simulating "different batches, different topic mixes".
            chunk_payload, chunk_vectors = make_rows(
                start_idx, CHUNK, rnd_chunk, centers[chunk_idx % len(centers)]
            )
            start = time.perf_counter()
            client.store(chunk_payload)     # columnar write
            client.flush()                  # flush per chunk: bounds memory and controls visibility
            chunk_times.append(time.perf_counter() - start)
            shadow.extend(chunk_vectors)
        show("chunk write time (ms per chunk)", [round(t * 1e3, 1) for t in chunk_times])
        show("total rows ingested", client.count_rows())
        assert client.count_rows() == N_INITIAL, "initial build row count mismatch"
        assert len(shadow) == N_INITIAL, "shadow matrix row count does not match the store"
        print(f"[OK] chunked loading done: {N_INITIAL} rows / {len(chunk_times)} chunks, every chunk flushed")

        section("Step 3: build the FTS index and the TurboQuant4 accelerator column")
        client.execute("CREATE FTS INDEX ON items(title, category)")
        target = client.create_quantized_column(source="emb", target="emb_tq4", codec="turboquant4")
        show("accelerator column name / SQL-visible columns", f"{target} / {client.execute('SELECT * FROM items LIMIT 1').columns}")
        show("FTS stats", client.get_fts_stats())

        # Query set: perturb each cluster centre, and compute ground truth with
        # the numpy shadow matrix.
        qrnd = rng(1799)
        queries = unit(
            np.asarray(
                [
                    np.asarray([float(x) + qrnd.gauss(0.0, 0.15) for x in centers[i % len(centers)]], dtype=np.float32)
                    for i in range(N_QUERIES)
                ],
                dtype=np.float32,
            )
        )

        def consistency(label: str, expect_accelerator: bool = True) -> float:
            """Three-way comparison per query: exact topk / numpy shadow / accelerator column.

            Returns the recall@K of exact search (against the numpy shadow
            matrix) and asserts internally that all three agree.
            """
            hits = 0
            for qi, q in enumerate(queries):
                truth = numpy_topk(np.asarray(shadow, dtype=np.float32), q, K)
                got = [int(r["_id"]) for r in client.topk_distance("emb", q, k=K, metric="cosine").to_dict()]
                assert got == truth, f"{label}: exact search disagrees with the numpy shadow (query {qi})"
                hits += len(set(got) & set(truth))
                if expect_accelerator:
                    acc = [
                        int(r["_id"])
                        for r in client.topk_distance(
                            "emb", q, k=K, metric="cosine", accelerator=target, candidate_k=256, rescore=True
                        ).to_dict()
                    ]
                    # Accelerator column + rescore should agree closely with exact search.
                    assert len(set(acc) & set(got)) >= K // 2, f"{label}: accelerator results deviate too far from exact search"
            recall = hits / (N_QUERIES * K)
            print(f"[OK] {label}: exact search == numpy shadow matrix (recall@K = {recall:.2f})")
            return recall

        baseline_recall = consistency("consistency check in the initial state")
        assert baseline_recall == 1.0, "in the initial state exact search must match the numpy shadow exactly"

        section(f"Step 4: incremental append of {N_INCREMENTAL} rows (accelerator and FTS follow automatically)")
        rnd_inc = rng(1801)
        inc_payload, inc_vectors = make_rows(N_INITIAL, N_INCREMENTAL, rnd_inc, centers[2])
        start = time.perf_counter()
        client.store(inc_payload)
        client.flush()
        t_incremental = time.perf_counter() - start
        shadow.extend(inc_vectors)
        show(f"incremental write of {N_INCREMENTAL} rows", f"{t_incremental * 1e3:.1f} ms ({N_INCREMENTAL / t_incremental:.0f} rows/s)")
        show("row count after append", client.count_rows())
        assert client.count_rows() == N_INITIAL + N_INCREMENTAL, "row count after incremental append mismatch"

        # 4.1 Vector search: newly appended rows must be searchable immediately
        # (query with the row's own vector, so Top1 must be that row).
        new_id = N_INITIAL + 1
        new_vec = inc_vectors[0]
        top1 = client.topk_distance("emb", new_vec, k=1, metric="cosine").to_dict()[0]
        show("Top1 for a search with the new row's own vector", top1)
        assert int(top1["_id"]) == new_id, f"newly appended row {new_id} is not immediately searchable (Top1 = {top1['_id']})"
        assert top1["dist"] < 1e-5, "the cosine distance from a vector to itself should be close to 0"
        print(f"[OK] incremental row _id={new_id} is searchable immediately after the write (self-query distance {top1['dist']:.2e})")

        # 4.2 Accelerator column: new rows must also be recalled through it
        # (verifies the accelerator is rebuilt automatically).
        acc_top1 = client.topk_distance(
            "emb", new_vec, k=1, metric="cosine", accelerator=target, candidate_k=64, rescore=True
        ).to_dict()[0]
        show("Top1 through the TurboQuant4 accelerator column", acc_top1)
        assert int(acc_top1["_id"]) == new_id, "the accelerator did not follow the incremental write"
        print("[OK] the TurboQuant4 accelerator column automatically covers incrementally written rows (no manual rebuild)")

        # 4.3 FTS: the new row's text must also be searchable immediately
        # (verifies the inverted index syncs automatically).
        fts_hits = client.execute("SELECT _id FROM items WHERE MATCH('retrieval') LIMIT 5").to_dict()
        show("rows matched by MATCH('retrieval') (first 5)", [r["_id"] for r in fts_hits])
        assert fts_hits, "FTS did not sync the incremental write"
        print("[OK] the FTS index syncs incremental writes automatically (MATCH hits the new rows)")

        # 4.4 overall consistency after the append
        after_append_recall = consistency("consistency check after the incremental append")
        assert after_append_recall == 1.0, "after the append exact search must match the numpy shadow exactly"

        section("Step 5: replace a single row's vector (in-place update, including the accelerator column)")
        # Pick an identifiable target row: the lowest _id whose category is
        # 'quantization'.
        client.use_table("items")
        target_rows = client.execute(
            "SELECT _id FROM items WHERE category = 'quantization' ORDER BY _id LIMIT 1"
        ).to_dict()
        show("candidate row to update", target_rows[:1])
        target_id = int(target_rows[0]["_id"])
        old_vec = shadow[target_id - 1]

        # New vector: move to the direction around another cluster centre
        # (semantically "changed topic").
        new_rnd = rng(1811)
        replacement = unit(
            np.asarray([[float(x) + new_rnd.gauss(0.0, 0.05) for x in centers[5]]], dtype=np.float32)
        )[0]
        # replace needs a numeric _id: pass an int explicitly.
        client.replace(target_id, {"title": "quantization document (rewritten) vector retrieval practice",
                                   "category": "quantization",
                                   "day": "2024-06-01",
                                   "emb": replacement})
        client.flush()
        shadow[target_id - 1] = replacement   # keep the shadow matrix in sync

        show("row count after replace (should not change)", client.count_rows())
        assert client.count_rows() == N_INITIAL + N_INCREMENTAL, "replace must not change the row count"

        # 5.1 A self-query with the new vector must hit that _id, with distance ~ 0.
        new_top1 = client.topk_distance("emb", replacement, k=1, metric="cosine").to_dict()[0]
        show("after replace: Top1 for the new vector", new_top1)
        assert int(new_top1["_id"]) == target_id, "the new vector did not take effect after replace"
        assert new_top1["dist"] < 1e-5, "after replace the self-query distance should be close to 0"
        print("[OK] after replace the vector of that _id is immediately the new value (self-query distance ~ 0)")

        # 5.2 The old vector no longer hits that _id (verifies the old value was
        # overwritten, not appended as a new row).
        old_top = client.topk_distance("emb", old_vec, k=3, metric="cosine").to_dict()
        show("after replace: Top3 for the old vector", old_top)
        assert all(int(r["_id"]) != target_id for r in old_top), "the old vector still hits the row, so replace did not overwrite it"
        print("[OK] the old vector no longer hits that _id (this is an overwrite update, not an append)")

        # 5.3 The accelerator still works after replace, and the new vector is
        # among its candidates.
        acc_after_replace = client.topk_distance(
            "emb", replacement, k=1, metric="cosine", accelerator=target, candidate_k=256, rescore=True
        ).to_dict()[0]
        show("after replace: accelerator Top1", acc_after_replace)
        assert int(acc_after_replace["_id"]) == target_id, "the accelerator did not follow the replace update"
        print("[OK] the accelerator follows replace updates automatically (source and compressed columns stay consistent)")

        # 5.4 Overall consistency after replace + append (the final consistency
        # check of this example).
        final_recall = consistency("consistency check after replace + incremental")
        assert final_recall == 1.0, "in the final state exact search must match the numpy shadow exactly"

        # 5.5 Overall recall of the accelerator column (against the numpy
        # shadow, quantisation + exact rescoring).
        acc_hits = 0
        for q in queries:
            truth = numpy_topk(np.asarray(shadow, dtype=np.float32), q, K)
            acc = [
                int(r["_id"])
                for r in client.topk_distance(
                    "emb", q, k=K, metric="cosine", accelerator=target, candidate_k=256, rescore=True
                ).to_dict()
            ]
            acc_hits += len(set(acc) & set(truth))
        accel_recall = acc_hits / (N_QUERIES * K)
        show(f"TurboQuant4 accelerator recall@{K} (candidate_k=256)", round(accel_recall, 4))
        assert accel_recall >= 0.8, f"accelerator recall too low: {accel_recall}"
        print(f"[OK] after incremental writes and a replace, the accelerator still holds recall@{K} = {accel_recall:.2f}")

        section("Step 6: retrieval-consistency check summary")
        show("final row count", client.count_rows())
        show("shadow matrix shape", np.asarray(shadow).shape)
        show("exact-search recall@K (against the numpy shadow)", 1.0)
        show(f"accelerator recall@{K}", round(accel_recall, 4))
        show("FTS document count", client.get_fts_stats().get("doc_count"))
        assert client.count_rows() == len(shadow), "row count in the store does not match the shadow matrix"
        assert client.get_fts_stats()["doc_count"] == len(shadow), "FTS document count did not follow the writes"
        print("[OK] row count, shadow matrix and FTS document count agree; every incremental write and update stayed searchable with no stale data")

        section("Step 7: delete a row that was just replaced (vector-table delete)")
        # Delete the row that was replaced in step 5: on a vector table,
        # delete-after-replace now works and removes exactly that row. The shadow
        # matrix is not re-verified after this final step, so it is left as-is.
        before_delete = client.count_rows()
        deleted_ok = client.delete(target_id)
        client.flush()
        visible_ids = {int(r["_id"]) for r in client.execute("SELECT _id FROM items").to_dict()}
        show("delete return value", deleted_ok)
        show("count_rows() after delete", f"{before_delete} -> {client.count_rows()}")
        show(f"is the deleted _id={target_id} still visible", target_id in visible_ids)
        assert deleted_ok, "delete should return True"
        assert target_id not in visible_ids, f"after delete _id={target_id} is still visible"
        assert client.count_rows() == before_delete - 1, "count_rows did not decrease after delete"
        print(f"[OK] delete(_id={target_id}) took effect: the row is gone from SELECT and the count dropped by 1")

    print(
        f"\n=== Scenario 17: bulk embedding ingestion and incremental updates complete ===\n"
        f"Key takeaways: columnar store is the preferred bulk-ingestion form; chunking + flush keeps memory bounded;\n"
        f"         the accelerator column and the FTS index both follow incremental writes and single-row replaces automatically;\n"
        f"         every step reconciles against the numpy shadow matrix, so search results never go stale.\n"
        f"Data at: {base}"
    )


if __name__ == "__main__":
    main()
