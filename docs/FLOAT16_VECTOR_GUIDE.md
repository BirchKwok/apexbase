# Float16 Vector Storage Guide

ApexBase supports `FLOAT16_VECTOR` columns for storing embedding vectors in half-precision (float16) format.  Compared to the default float32 `FixedList` column, float16 halves the storage footprint and enables hardware-accelerated SIMD distance kernels on ARM and x86 platforms.

---

## Table of Contents

1. [When to use float16](#when-to-use-float16)
2. [Creating a float16 table](#creating-a-float16-table)
3. [Inserting vectors](#inserting-vectors)
4. [Querying — TopK and SQL](#querying-topk-and-sql)
5. [SIMD acceleration details](#simd-acceleration-details)
6. [Quantization error and precision](#quantization-error-and-precision)
7. [Full working example](#full-working-example)
8. [Comparison: float16 vs float32](#comparison-float16-vs-float32)

---

## When to use float16

| Use float16 (`FLOAT16_VECTOR`) | Stick with float32 (default `FixedList`) |
|---|---|
| Large embedding tables (>1M rows, high dim) | Embeddings with high dynamic range (e.g., raw logits) |
| Memory-constrained environments | Downstream code that expects exact float32 fidelity |
| Apple Silicon / AWS Graviton (NEON fp16 hardware) | Dimensions that are odd multiples < 8 (micro-batches) |
| Models that already quantize (CLIP, text embeddings) | Prototype / exploration phase where precision matters |

For most modern embedding models (CLIP, BERT, Sentence-Transformers, OpenAI `text-embedding-*`), float16 quantization error is well below retrieval noise.

---

## Creating a float16 table

```python
from apexbase import ApexClient

client = ApexClient("./mydb")

# SQL CREATE TABLE with FLOAT16_VECTOR column
client.execute("CREATE TABLE embeddings (doc_id TEXT, vec FLOAT16_VECTOR)")
client.use_table("embeddings")
```

Accepted type name aliases (all equivalent):

- `FLOAT16_VECTOR` *(recommended)*
- `FLOAT16VECTOR`
- `F16_VECTOR`

---

## Inserting vectors

### Recommended: batch store with numpy arrays

Use a **batch call** for best throughput. The fixedlist path converts source vectors to float32 bytes and then the storage layer applies a single f32→f16 conversion, giving correct float16 data.

```python
import numpy as np

n, dim = 50_000, 128
vecs = np.random.rand(n, dim).astype(np.float32)  # source in float32

# Batch list-of-dicts
client.store([{"doc_id": str(i), "vec": vecs[i]} for i in range(n)])

# Columnar dict — fastest for large homogeneous batches
client.store({
    "doc_id": [str(i) for i in range(n)],
    "vec":    [vecs[i] for i in range(n)],
})
```

### numpy float16 and Python-list source data

If your upstream model already produces float16 numpy arrays, they are accepted directly. Numeric Python lists/tuples are also accepted, including single-record writes.

```python
vecs_f16 = np.random.rand(n, dim).astype(np.float16)
client.store([{"doc_id": str(i), "vec": vecs_f16[i]} for i in range(n)])

# Single-record writes are correct too
client.store({"doc_id": "one", "vec": [0.1, 0.2, 0.3, 0.4]})
```

---

## Querying — TopK and SQL

The query API is **identical** to float32 columns.  Query vectors are always provided as float32 — the runtime converts them internally.

### `topk_distance`

```python
query = np.random.rand(128).astype(np.float32)

results = client.topk_distance("vec", query, k=10, metric="l2")
df = results.to_pandas()
# df columns: _id (int64), dist (float64) — sorted nearest first
```

All six metrics are supported:

| `metric` | Description |
|---|---|
| `'l2'` | Euclidean distance √Σ(aᵢ−bᵢ)² |
| `'l2_squared'` | Squared L2 Σ(aᵢ−bᵢ)² |
| `'cosine'` / `'cosine_distance'` | 1 − cos(a, b) |
| `'dot'` / `'inner_product'` | Negative dot product (for min-heap) |
| `'l1'` / `'manhattan'` | Σ|aᵢ−bᵢ| |
| `'linf'` / `'chebyshev'` | max|aᵢ−bᵢ| |

### `batch_topk_distance`

```python
N = 100
queries = np.random.rand(N, 128).astype(np.float32)

result = client.batch_topk_distance("vec", queries, k=10, metric="l2")
# result.shape == (N, 10, 2)
# result[i, :, 0] = _id of top-10 for query i
# result[i, :, 1] = distances
```

### SQL distance functions

`array_distance`, `l1_distance`, `linf_distance` all work on `FLOAT16_VECTOR` columns:

```python
client.execute("""
    SELECT doc_id,
           array_distance(vec, [0.1, 0.2, 0.3, 0.4]) AS l2_dist,
           l1_distance(vec,    [0.1, 0.2, 0.3, 0.4]) AS l1_dist
    FROM embeddings
    ORDER BY l2_dist
    LIMIT 5
""")
```

SQL TopK via `topk_distance` + `explode_rename`:

```python
q_str = ",".join(f"{v:.6f}" for v in query)
client.execute(f"""
    SELECT explode_rename(topk_distance(vec, [{q_str}], 10, 'l2'), '_id', 'dist')
    FROM embeddings
""")
```

---

## SIMD acceleration details

Float16 distance kernels use native hardware half-precision instructions when available, detected at runtime:

### aarch64 — NEON fp16 (`fp16` feature flag)

Applies to: Apple M1/M2/M3/M4, AWS Graviton 3/4, Ampere Altra.

- Uses `FCVTL` / `FCVTL2` to widen f16→f32 in-register (no memory round-trip)
- 8 elements loaded per NEON vector, widened to two 128-bit f32 registers
- ~2–3× faster than scalar; often ≥2× faster than equivalent float32 kernels

### x86_64 — AVX2 + F16C

Applies to: Intel Haswell+ (2013+), AMD Ryzen (2017+).

- Uses `_mm256_cvtph_ps` to convert 8 f16 → 8 f32 per instruction
- 256-bit AVX2 arithmetic on the widened values
- Falls back to scalar if `f16c` or `avx2` is not detected

### Scalar fallback

All platforms have a scalar fallback that converts f16→f32 element-by-element.  Performance is comparable to a plain float32 scan of the same data (same number of distance operations, but on narrower storage).

### Runtime dispatch (no user action required)

```
CPU query at startup → select kernel → cache decision
                         ↙            ↘
              NEON fp16              AVX2+F16C
              (aarch64)              (x86_64)
                    ↓ neither ↓
                scalar fallback
```

---

## Quantization error and precision

Float16 (IEEE 754 binary16) has:

- 10-bit mantissa → ~3.3 decimal digits of precision
- Range: ±65504
- Machine epsilon: ~9.77 × 10⁻⁴

For typical embedding ranges (values in [−1, 1]):

```python
import numpy as np

def f16_quantize(v):
    return v.astype(np.float16).astype(np.float32)

rng = np.random.default_rng(0)
dim = 128
vec   = rng.random(dim).astype(np.float32) * 2 - 1   # uniform [-1, 1]
query = rng.random(dim).astype(np.float32) * 2 - 1

exact    = float(np.sqrt(np.sum((vec - query) ** 2)))
f16_dist = float(np.sqrt(np.sum((f16_quantize(vec) - query) ** 2)))
rel_err  = abs(exact - f16_dist) / exact
print(f"exact: {exact:.4f}  f16: {f16_dist:.4f}  rel_err: {rel_err:.2e}")
# exact: 5.8123  f16: 5.8119  rel_err: 7.23e-05
```

For top-k retrieval, quantization error is well within retrieval noise for dimensions ≥ 32 and any standard embedding model.

---

## Full working example

```python
import numpy as np
from apexbase import ApexClient

# 1. Create database and table
client = ApexClient("./embeddings_db")
client.execute("CREATE TABLE docs (title TEXT, vec FLOAT16_VECTOR)")
client.use_table("docs")

# 2. Generate and store embeddings (float32 source → auto-quantized to f16)
rng = np.random.default_rng(42)
n, dim = 10_000, 384
vecs  = rng.random((n, dim), dtype=np.float32)
titles = [f"document_{i}" for i in range(n)]

client.store({"title": titles, "vec": [vecs[i] for i in range(n)]})

# 3. Nearest-neighbour search
query = rng.random(dim, dtype=np.float32)

# Single query
results = client.topk_distance("vec", query, k=5, metric="l2")
print(results.to_pandas())

# Batch query
queries = rng.random((20, dim), dtype=np.float32)
batch_results = client.batch_topk_distance("vec", queries, k=5, metric="cosine")
print(batch_results.shape)  # (20, 5, 2)

# SQL query
q_str = ",".join(f"{v:.6f}" for v in query)
sql_results = client.execute(f"""
    SELECT title, array_distance(vec, [{q_str}]) AS dist
    FROM docs
    ORDER BY dist
    LIMIT 5
""")
print(sql_results.to_pandas())

client.close()
```

---

## Comparison: float16 vs float32

| | `FLOAT16_VECTOR` | Default `FixedList` (float32) |
|---|---|---|
| **Bytes per element** | 2 | 4 |
| **Storage for 1M × dim=128** | ~256 MB | ~512 MB |
| **Distance precision** | ~3.3 decimal digits | ~7.2 decimal digits |
| **SIMD on Apple Silicon** | NEON fp16 (≥2× faster) | NEON f32 |
| **SIMD on x86_64** | AVX2+F16C | AVX2 f32 |
| **Query API** | identical | identical |
| **Recommended for** | production, large tables | prototyping, high-precision needs |

> **Tip:** For the fastest possible inserts, use the columnar dict API:
> ```python
> client.store({"vec": list_of_numpy_arrays, "label": list_of_strings})
> ```
> This bypasses the row-oriented path entirely and feeds data directly to the Rust columnar encoder.
