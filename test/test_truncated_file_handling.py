"""Truncated / half-published files must produce errors, never panics.

The storage layer indexes fixed 32-byte row-group headers and length-prefixed
payloads, and a reader can observe a file that another writer has not finished
(append in flight, footer from an older snapshot, external truncation). Several
defects in this class were seen as Rust panics inside reader threads
(`range end index N out of range for slice of length M`), which surface in
Python as `pyo3_runtime.PanicException` - a `BaseException` that escapes normal
`except Exception` handling. These tests truncate the base file at every byte
length around the interesting boundaries and require a clean `Exception` (or a
clean empty result) instead.
"""

import os
import shutil
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 500


def _build(root):
    client = ApexClient(root, drop_if_exists=True, enable_cache=False)
    client.create_table("t", {"doc": "int64", "k": "int64", "s": "string"})
    client.use_table("t")
    client.store(
        {
            "doc": list(range(ROWS)),
            "k": [index % 97 for index in range(ROWS)],
            "s": [f"v{index % 13}" for index in range(ROWS)],
        }
    )
    client.flush()
    client.close()


def _panic_free(fn):
    """Return the raised exception, failing the test on a Rust panic."""
    try:
        fn()
    except BaseException as error:  # noqa: BLE001 - we must catch panics too
        assert not type(error).__name__.endswith("PanicException"), (
            f"a Rust panic escaped: {error!r}"
        )
        return error
    return None


@pytest.mark.parametrize(
    "length",
    [0, 1, 4, 8, 16, 28, 29, 31, 32, 33, 36, 40, 64, 100, 256, 1024],
)
def test_truncated_table_file_never_panics(length):
    root = tempfile.mkdtemp()
    try:
        _build(root)
        path = os.path.join(root, "t.apex")
        assert os.path.exists(path)
        with open(path, "rb") as handle:
            original = handle.read()
        if length >= len(original):
            pytest.skip("fixture too small for this truncation length")
        with open(path, "wb") as handle:
            handle.write(original[:length])

        client = ApexClient(root, enable_cache=False)
        client.use_table("t")
        for statement in (
            "SELECT COUNT(*) AS n FROM t",
            "SELECT doc FROM t",
            "SELECT doc, k FROM t WHERE k < 10",
            "SELECT city",
        ):
            if statement == "SELECT city":
                continue
            _panic_free(lambda sql=statement: client.execute(sql).to_dict())
        _panic_free(lambda: client.retrieve(1))
        _panic_free(lambda: client.count_rows())
        client.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_garbage_row_group_header_never_panics():
    """A header whose declared counts are absurd must not slice out of bounds."""
    root = tempfile.mkdtemp()
    try:
        _build(root)
        path = os.path.join(root, "t.apex")
        with open(path, "rb") as handle:
            original = bytearray(handle.read())
        # Flip the row-group header bytes near the end of the file (compression
        # flag, encoding version, id encoding) and the payload count prefixes.
        for offset in range(max(0, len(original) - 600), len(original) - 8):
            original[offset] = 0xFF
        with open(path, "wb") as handle:
            handle.write(bytes(original))

        client = ApexClient(root, enable_cache=False)
        client.use_table("t")
        _panic_free(lambda: client.execute("SELECT COUNT(*) AS n FROM t").to_dict())
        _panic_free(lambda: client.execute("SELECT doc FROM t").to_dict())
        _panic_free(lambda: client.retrieve(1))
        client.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)
