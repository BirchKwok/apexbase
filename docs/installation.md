# Installation

ApexBase ships as a Python package with a Rust core. Install the wheel, open a
database, and run SQL in your own process; the embedded API needs no server.

## Requirements

| Component | Requirement |
| --- | --- |
| Python | CPython 3.9 through 3.13 |
| Operating systems | Linux, macOS, Windows |
| Architectures | x86_64 and ARM64 |
| Wheel install | No Rust toolchain or C compiler required |
| Source build | Rust stable, `maturin`, and `protoc` for the `server` and `flight` features |
| Runtime dependencies | `numpy`, `pandas`, `polars`, and `pyarrow` are installed with the package |

Prebuilt wheels are published for Linux, macOS, and Windows on supported Python
versions; source builds cover other architectures.

## Install From PyPI

```bash
python -m pip install apexbase
```

If you already use `uv`:

```bash
uv pip install apexbase
```

## Verify The Install

This round trip uses an in-memory database, creates no files, and closes the
client cleanly. It prints the version and the single row written.

```python
import apexbase
from apexbase import ApexClient

print("apexbase", apexbase.__version__)

with ApexClient(":memory:") as client:
    client.execute("CREATE TABLE hits (path TEXT, score DOUBLE)")
    client.execute(
        "INSERT INTO hits VALUES (?, ?)", params=["docs/concepts.md", 0.97]
    )
    print(client.execute("SELECT * FROM hits").to_dict())
```

`ApexClient(":memory:")` is a true process-local backend, not a temporary
directory. Pass a directory path instead when data must survive the process.
For the storage model, see [Core Concepts](concepts.md).

## Upgrade From v1

```bash
python -m pip install --upgrade apexbase
```

v2.1.0 is the current release on the V5 GA line (a drop-in upgrade from
2.0.0: the on-disk format and SQL surface are unchanged):

- V4 files open unchanged. No migration step or table rewrite is required; a
  table starts declaring format version 5 on its first V5-only write.
- Format-5 files and files with the logical-type section need a v2.0.0-or-newer
  reader; pre-V5 builds do not understand the new encodings.
- To keep a file readable by pre-V5 builds, set `APEX_WIDTH_FIDELITY=0` (V4
  layout) and `APEX_CHUNK_INDEX=0` (flat compressed frames) before writing.
- Opt-outs: `APEX_LOGICAL_TYPES=0` refuses new typed columns and
  `APEX_SQL_DECIMAL=0` restores legacy text decimals.
- SQL results changed for `SELECT DISTINCT *`, duplicate aliases, and RIGHT JOIN
  ordering; check queries that depended on the old behavior.

See [What's New in 2.0](whats-new-2.0.md) and
[Release Notes](release-notes.md) for details.

## Build From Source

```bash
git clone https://github.com/BirchKwok/ApexBase.git
cd ApexBase
python -m pip install maturin
maturin develop --release
```

`maturin develop --release` builds the Python extension with the `server` and
`flight` features declared in `pyproject.toml`. Install `protoc` first
(`protobuf-compiler`, `brew install protobuf`, or `choco install protoc`) or
the build stops at the gRPC parts.

The release profile no longer strips the extension dylib. rustc 1.92.x
stripped macOS 26/27 dylibs into an invalid Mach-O (`mis-aligned LINKEDIT string
pool`; [rust-lang/rust#157750](https://github.com/rust-lang/rust/issues/157750)),
so `maturin develop --release` now leaves symbols for the packaging step and
works on every toolchain the crate builds with. If you strip wheels yourself,
use rustc 1.98.1 or newer (the upstream fix).

For a development environment with test tools:

```bash
python -m pip install -e ".[dev]"
maturin develop --release
python -m pytest test/ -q
```

Build the standalone server binaries without Python bindings, and run the Rust
suites:

```bash
cargo build --release --no-default-features --features server,flight
cargo test --no-default-features --release
cargo test --no-default-features --features server --release
```

## Documentation Site

```bash
python -m pip install -r docs/requirements.txt
python -m mkdocs serve            # live preview at http://127.0.0.1:8000
python -m mkdocs build --strict   # fails on warnings and broken links
```

## Troubleshooting

!!! warning "Platform notes"
    The package pins `numpy>=1.26,<2.4.4`; unpinned newer releases have caused
    a macOS ARM64 SIGSEGV when loaded before ApexBase's storage engine is
    constructed. A source build with `server` or `flight` needs `protoc` on
    `PATH` and fails with a `protoc` error otherwise.

## Next Steps

- [Quick Start](QUICK_START.md): first database, table, and query.
- [Core Concepts](concepts.md): storage, logical types, and execution.
- [What's New in 2.0](whats-new-2.0.md): V5 features and migration notes.
- [Python API Reference](API_REFERENCE.md): exact signatures and options.
