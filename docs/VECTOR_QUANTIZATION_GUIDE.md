# Vector Quantization

ApexBase supports vector columns stored as `FLOAT32_VECTOR`, `FLOAT16_VECTOR`,
`BFLOAT16_VECTOR`, `INT8_VECTOR`, `UINT8_VECTOR`, `BIT1_VECTOR`, and
`TURBOQUANT2_VECTOR` / `TURBOQUANT3_VECTOR` / `TURBOQUANT4_VECTOR`.

For agent memory and RAG, retrieval cost has two independent axes: bytes
scanned and rows compared. ApexBase gives you a dial for each inside the same
engine — compress the stored vectors (this page) and prune the scan with an IVF
index — then rerank the surviving candidates against the exact float32 source.
Cost and recall stay explicit tuning parameters instead of a separate service to
keep in sync. The same table can feed `hybrid_search()` when you also need BM25;
see [Hybrid Search](API_REFERENCE.md#hybrid-search) and
[Agent memory](user-guide/agent-memory.md).

For retrieval systems that need exact reranking, keep the Float32 vector as the
authoritative column and add a separate stored accelerator:

```python
import numpy as np
from apexbase import ApexClient

client = ApexClient("./items")
rows = [
    {"title": f"item {i}", "embedding": np.random.rand(128).astype(np.float32)}
    for i in range(100)
]
query = np.random.rand(128).astype(np.float32)

client.create_table("items", {
    "title": "string",
    "embedding": "float32_vector",
})
client.use_table("items")
client.store(rows)

client.create_quantized_column(
    source="embedding",
    target="embedding_tq4",
    codec="turboquant4",
)

hits = client.topk_distance(
    "embedding",
    query,
    k=20,
    accelerator="embedding_tq4",
    candidate_k=160,
    rescore=True,
)
```

The accelerator scans the compressed column to produce `candidate_k` rows.
ApexBase then reads only those rows from `embedding`, recomputes the requested
distance in Float32, and returns the exact ranking within the candidate set.
Increasing `candidate_k` improves recall at the cost of more source reads and
exact distance calculations.

Compressed L2 scans operate directly on the stored representation. Float16
and BFloat16 decode inside the scan; Int8 and UInt8 reuse a query
quantization and integer dot/norm terms for dimensions of 64 or more; 1Bit
uses a Hamming candidate pass followed by a small magnitude-aware rerank; and
TurboQuant reuses one rotated query plus byte lookup tables. Batch search
shares the encoded query state across rows and parallelizes across queries.

The exact Int8/UInt8 kernels use runtime-selected SIMD on both major desktop
architectures: AVX2/FMA on x86_64 and NEON on AArch64. Unsupported CPUs retain
the scalar implementation, so stored formats and query results do not depend
on the instruction set available at write time.

## Stored formats

| Type | Approximate bytes per vector of dimension D | Notes |
|---|---:|---|
| Float32 | `4D` | Exact source and exact scan |
| Float16 | `2D` | IEEE binary16 |
| BFloat16 | `2D` | Round-to-nearest-even |
| Int8 | `D + 4` | Per-vector symmetric scale |
| UInt8 | `D + 8` | Per-vector affine min/scale |
| 1Bit | `ceil(D/8) + 4` | Sign bits and per-vector magnitude |
| TurboQuant 2/3/4 | `ceil(bits × next_power_of_two(D) / 8) + 4` | Random sign rotation, normalized Hadamard transform, and fixed Gaussian codebook |

TurboQuant columns use a deterministic, versioned TurboQuant-MSE-style codec.
They do not currently implement the paper's product quantization or QJL paths.
See the [TurboQuant paper](https://arxiv.org/abs/2504.19874) for the underlying
rotation and scalar-quantization method.

## Lifecycle and consistency

Creating an accelerator backfills existing rows row-group by row-group and
publishes the result with an atomic file replacement. Later inserts and source
vector replacements regenerate the accelerator automatically. Direct writes to
the accelerator are rejected so the two columns cannot silently diverge.

The source column cannot be dropped while an accelerator depends on it. The
accelerator itself can be removed at any time:

```python
client.drop_quantized_column("embedding_tq4")
```

This removes only the stored compressed column and its dependency metadata.
`embedding` remains available, so ordinary Float32 `topk_distance` and exact
reranking continue to work.

## Standalone quantized columns

All supported types can also be declared directly in SQL:

```sql
CREATE TABLE vectors (
    name TEXT,
    embedding TURBOQUANT3_VECTOR
);
```

Standalone quantized columns save space and can be searched directly, but the
original Float32 values are not recoverable. Arrow/SQL reads decode them to
Float32 approximations. Use a separate derived column whenever exact rescore,
future re-quantization, or model migration matters.

## Choosing a codec

- Use Float16 or BFloat16 when recall is the priority and a 2x size reduction is enough.
- Use Int8 or UInt8 for a conservative compact accelerator.
- Use TurboQuant4 as the general high-compression starting point.
- Use TurboQuant2/3 or 1Bit only after measuring recall on representative queries.

Quantization rejects empty, ragged, NaN, and infinite vectors. Vector dimension
is fixed by the first valid batch and must remain consistent.

Int8/UInt8 compressed scans at dimensions of 64 or more use an approximate
query-once integer score; exact source reranking is the appropriate path when
the final ordering must match Float32. Likewise, very compact 1Bit and
TurboQuant codecs trade candidate recall for storage and scan speed. Always
measure recall and tune `candidate_k` on representative embeddings.

## IVF candidate pruning

For larger tables you can also prune the scan with a persisted IVF index. It
stores k-means centroids plus per-list row positions in `<table>.apex.ivf`, and
`nprobe` controls how many lists a query scans:

```python
# continuing the items table above (authoritative float32 `embedding` column)
client.create_vector_index("embedding", nlist=64, iterations=10, metric="l2")

info = client.vector_index_info()
print(info["nlist"], info["rows"], info["stale"])   # 64 5000 False

hits = client.topk_distance("embedding", query, k=20, nprobe=16)
client.drop_vector_index()
```

- `create_vector_index()` requires the authoritative float32 vector column; it
  rejects `FLOAT16_VECTOR` and quantized columns (`not a supported f32 vector
  column`).
- Query with the same `metric` you built with (`l2` or `cosine`).
- After any write, `vector_index_info()["stale"]` is `True`. An `nprobe` query
  then fails explicitly instead of silently degrading: `vector index is stale:
  built over N rows but the table now has M; rebuild it with
  create_vector_index`. Rebuild after bulk loads.
- `nprobe` and `accelerator` are separate query paths in 2.0.0; a single call
  rejects the combination (`nprobe does not support accelerator yet`). Use one
  per call, or stage the two passes yourself.
- Reference point: the 2.0.0 release harness records 10M-row IVF recall@10 =
  0.97 at ~11 ms median with `nprobe=16`. Measure on your own embeddings and
  tune `nlist`/`nprobe`.

Combined, the two dials bound both sides of retrieval cost: IVF reduces the rows
compared, and a quantized accelerator reduces the bytes read for the surviving
candidates before exact float32 reranking.
