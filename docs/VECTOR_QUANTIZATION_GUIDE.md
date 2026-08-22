# Vector Quantization

ApexBase supports vector columns stored as `FLOAT32_VECTOR`, `FLOAT16_VECTOR`,
`BFLOAT16_VECTOR`, `INT8_VECTOR`, `UINT8_VECTOR`, `BIT1_VECTOR`, and
`TURBOQUANT2_VECTOR` / `TURBOQUANT3_VECTOR` / `TURBOQUANT4_VECTOR`.

For retrieval systems that need exact reranking, keep the Float32 vector as the
authoritative column and add a separate stored accelerator:

```python
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
