# ApexBase Engineering Guidelines

> For coding agents and contributors working on the ApexBase codebase.

---

## 1. Query Signature Classifier — Single Source of Truth

All SQL classification MUST go through the centralized `QuerySignature` system.

### Rust: `query_signature::classify(sql)`

- **Location**: `apexbase/src/query/query_signature.rs`
- **Enum**: `QuerySignature` — classifies SQL into `CountStar`, `PointLookup`, `SimpleScanLimit`, `StringEqualityFilter`, `LikeFilter`, `DmlWrite`, `Ddl`, `Transaction`, `MultiStatement`, `SessionCommand`, `Explain`, `Cte`, `TableFunction`, `Complex`
- **Usage**: Call `classify()` ONCE per query entry point, then `match` on the result.

```rust
use crate::query::query_signature::{self, QuerySignature};
let sig = query_signature::classify(sql);
match &sig {
    QuerySignature::CountStar { table } => { /* fast path */ }
    QuerySignature::PointLookup { id } => { /* fast path */ }
    _ => { /* full parse pipeline */ }
}
```

### Python: Mirror classifier in `_execute_impl()`

- **Location**: `apexbase/python/apexbase/client.py` → `_execute_impl()`
- **Pattern**: Single `sql_upper = sql.strip().upper()` + if-elif chain → `_sig` string
- **Rule**: `sql.strip().upper()` MUST happen exactly ONCE per `_execute_impl()` call

### What is FORBIDDEN

- **No inline SQL pattern matching** in `bindings.rs`, `executor/mod.rs`, or `client.py` outside of the classifier
- **No duplicate `sql.to_uppercase()`** — each entry point does ONE classify/uppercase pass
- **No new fast paths** added directly in bindings or client — add a new `QuerySignature` variant first, then wire it through the 3 layers

---

## 2. Three-Layer Architecture

```
┌─────────────────────────────────────────────────┐
│  Python Client  (client.py)                     │  Thin dispatcher
│  - Classifies once → _sig                       │  - Routes to correct Rust method
│  - Manages Python-side state (_in_txn, _lock)   │  - NO query execution logic
├─────────────────────────────────────────────────┤
│  PyO3 Bindings  (python/bindings/)              │  Bridge layer
│  - classify() once per method entry             │  - execute(): writes, txn, point lookups
│  - _execute_arrow_ffi(): all reads (zero-copy)  │  - _execute_arrow_ipc(): multi-stmt, fallback
│  - _execute_like_ffi(): LIKE mmap scan          │  - State: current_txn_id, cached_backends
├─────────────────────────────────────────────────┤
│  Rust Executor  (executor/mod.rs, select.rs)    │  Core engine
│  - classify() once in execute_with_base_dir()   │  - Pre-parse fast paths: COUNT*, _id lookup
│  - Full SQL parse + CBO for Complex queries     │  - Post-parse fast paths in select.rs (mmap)
└─────────────────────────────────────────────────┘
```

### Dispatch rules by query type

| Signature | Python calls | Bindings method | Why |
|-----------|-------------|-----------------|-----|
| `count_star` | `fast_row_count()` | atomic read | No Arrow overhead |
| `point_lookup` | `execute()` | `cached_backends` DashMap | Zero PathBuf hash |
| `scan_limit` (≤500) | `execute()` | `pread_rcix` columnar | `columns_dict` format |
| `like` | `_execute_like_ffi()` | mmap scan → FFI | Zero-copy |
| `transaction` | `execute()` | state management | txn_id tracking |
| `multi` | `_execute_arrow_ipc()` | `execute_multi_with_txn` | Transaction context |
| `write` | `_execute_arrow_ffi()` | executor | Cache invalidation |
| `complex` / other | `_execute_arrow_ffi()` | executor → FFI | Zero-copy Arrow |

---

## 3. Transaction Handling

- **BEGIN** → sets `_in_txn = True` (Python) + `current_txn_id` (Rust)
- **COMMIT/ROLLBACK** → clears both
- **Multi-statement within txn** → MUST go through `_execute_arrow_ipc()` which calls `execute_multi_with_txn(stmts, ..., current_txn)`. NEVER route multi-statement through `execute()` when inside a transaction — `execute()` does NOT propagate txn context for multi-statement.
- **Single DML within txn** → routes through `execute()` → `execute_in_txn()` which buffers writes

---

## 4. Performance Rules

### Fast Path Preservation

- **cached_backends DashMap** (bindings.rs): Per-instance cache keyed by table name string. Avoids global `STORAGE_CACHE` PathBuf hashing. Populated on first point lookup, reused on subsequent calls.
- **pread_rcix** (bindings.rs): Direct columnar read for `SELECT * LIMIT N`. Returns `columns_dict` format consumed by `ResultView(lazy_pydict=...)`.
- **Arrow FFI** (zero-copy): Primary read path for all queries ≥500 rows. No serialization overhead.
- **Arrow IPC** (fallback): Used for multi-statement and when FFI fails.

### What NOT to do

- **NEVER add query result caching** (e.g., caching PyObject results by SQL string). All optimizations must be genuine algorithmic improvements.
- **NEVER add `sql.to_uppercase()` calls** outside the classifier. Each layer does ONE uppercase pass.
- **NEVER add new `if sql_upper.starts_with(...)` checks** in bindings.rs or client.py. Add a QuerySignature variant instead.

---

## 5. Adding a New Fast Path

1. **Add variant** to `QuerySignature` enum in `query_signature.rs`
2. **Add classification logic** in `classify()` function
3. **Add unit test** in the `tests` module of `query_signature.rs`
4. **Wire executor** dispatch in `execute_with_base_dir()` (if pre-parse) or `select.rs` (if post-parse)
5. **Wire bindings** dispatch in the appropriate method (`execute()`, `_execute_arrow_ffi()`, etc.)
6. **Wire Python** dispatch in `_execute_impl()` — add new `_sig` value + handler
7. **Run full test suite**: `cargo test --lib query_signature` + `maturin develop --release` + `pytest test/ -x -q`

---

## 6. Testing Requirements

- **Rust unit tests**: `cargo test --lib query_signature` — all classifier tests must pass
- **Python integration tests**: `maturin develop --release && python -m pytest -x -q`
- **No test deletion**: Never delete or weaken existing tests without explicit direction
- **Regression tests**: When fixing a bug, add a test that reproduces the original failure

---

## 7. Code Style

- **Rust**: Follow existing patterns — `use` imports inside function scope for module-specific types, `Arc<TableStorageBackend>` for shared backends
- **Python**: Pre-compiled regexes at module level (`_RE_*`), single `sql_upper` computation, `ResultView` wrapping for all results
- **Comments**: Preserve existing comments. Add `// QuerySignature: <variant>` when adding new fast path dispatch.
- **No public API changes** without explicit permission (applies to both Rust `pub fn` and Python method signatures)

---

## 8. File Reference

| File | Role |
|------|------|
| `apexbase/src/query/query_signature.rs` | QuerySignature enum + classify() |
| `apexbase/src/query/mod.rs` | Module registration + re-exports |
| `apexbase/src/query/executor/mod.rs` | Pre-parse dispatch, execute_with_base_dir |
| `apexbase/src/query/executor/select.rs` | Post-parse SELECT fast paths (mmap) |
| `apexbase/src/query/executor/joins.rs` | resolve_table_path, resolve_point_lookup_table_path |
| `apexbase/src/python/bindings.rs` | PyO3 bridge entry: execute(), _execute_arrow_ffi/ipc/like |
| `apexbase/src/python/bindings/` | Split bindings: arrow, blob, read, sql, wrapper, write |
| `apexbase/python/apexbase/client.py` | Python client: _execute_impl() classifier + dispatch |
