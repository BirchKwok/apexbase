"""Chunked row groups: partial-coverage filtered aggregation must match flat.

The chunked layout decodes only the filter/aggregate columns for a filtered
aggregate, so its scan loop is its own implementation. That loop originally
fell back to a per-row ``NumericView::at`` dispatch, which re-parsed a BITPACK
payload header per value and made partially covering aggregations slower than
the flat layout. These tests pin the values of that lane across the encodings a
filter/aggregate column can take: wide and narrow BITPACK integers, PLAIN
floats, NULL-bearing value columns, equality and range predicates, a projection
count, and the delete-rewritten file.
"""

from __future__ import annotations

import os
import shutil
import tempfile

import numpy as np
import pytest

from apexbase.client import ApexClient

ROWS = 120_000


def _build(tmp, chunked):
    rng = np.random.default_rng(20261031)
    data = {
        # Wide BITPACK integer (the encoding that exposed the regression).
        "k": rng.integers(0, 1_000_000, ROWS).tolist(),
        # Narrow BITPACK integer.
        "i1": rng.integers(0, 10_000, ROWS).tolist(),
        # PLAIN float / float.
        "f1": rng.random(ROWS).tolist(),
        "f2": (rng.random(ROWS) * 10).tolist(),
        # NULL-bearing integer value column.
        "n1": [None if value % 7 == 0 else int(value) for value in rng.integers(0, ROWS, ROWS)],
        "s1": [f"g{int(value) % 50:03d}" for value in rng.integers(0, 1_000_000, ROWS)],
    }
    table = "chunk" if chunked else "flat"
    previous = os.environ.get("APEX_CHUNK_INDEX")
    os.environ["APEX_CHUNK_INDEX"] = "1" if chunked else "0"
    try:
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.create_table(
            table,
            {"k": "int64", "i1": "int64", "f1": "float64", "f2": "float64",
             "n1": "int64", "s1": "string"},
        )
        client.use_table(table)
        assert client.set_compression("zstd") is not False
        client.store(data)
        client.flush()
        client.close()
    finally:
        if previous is None:
            os.environ.pop("APEX_CHUNK_INDEX", None)
        else:
            os.environ["APEX_CHUNK_INDEX"] = previous
    return data


def _values(result):
    return result.to_dict()


def _round(value):
    if isinstance(value, float):
        return float(f"{value:.9g}")
    if isinstance(value, dict):
        return {key: _round(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_round(item) for item in value]
    return value


SHAPES = (
    # (name, SQL); the predicate ranges are fixed so both layouts and the
    # independent computation agree bit-for-bit.
    ("count_star_wide_bitpack", "SELECT COUNT(*) AS n FROM {table} WHERE k >= 500000"),
    ("sum_float_wide_filter", "SELECT SUM(f1) AS s FROM {table} WHERE k >= 500000"),
    ("count_float_wide_filter", "SELECT COUNT(f1) AS n FROM {table} WHERE k >= 500000"),
    ("count_nullable_narrow", "SELECT COUNT(n1) AS n FROM {table} WHERE i1 >= 5000"),
    ("sum_nullable_narrow", "SELECT SUM(n1) AS s FROM {table} WHERE i1 >= 5000"),
    ("avg_nullable_narrow", "SELECT AVG(n1) AS a FROM {table} WHERE k >= 250000"),
    ("sum_int_of_float_filter", "SELECT SUM(i1) AS s FROM {table} WHERE f1 >= 0.25"),
    ("sum_float_of_float_filter", "SELECT SUM(f2) AS s FROM {table} WHERE f1 >= 0.25"),
    ("count_star_float_filter", "SELECT COUNT(*) AS n FROM {table} WHERE f1 >= 0.25"),
    ("sum_nullable_float_filter", "SELECT SUM(n1) AS s FROM {table} WHERE f1 >= 0.25"),
    ("count_star_equality", "SELECT COUNT(*) AS n FROM {table} WHERE k = 500000"),
    ("count_star_string", "SELECT COUNT(*) AS n FROM {table} WHERE s1 = 'g007'"),
    ("range_both_bounds", "SELECT COUNT(*) AS n FROM {table} WHERE k >= 250000 AND k < 750000"),
)


@pytest.fixture(scope="module")
def compared_tables():
    tmp = tempfile.mkdtemp(prefix="apex_chunked_partial_agg_")
    try:
        data = _build(tmp, chunked=False)
        _build(tmp, chunked=True)
        with open(os.path.join(tmp, "chunk.apex"), "rb") as handle:
            head = handle.read(256 + 32)
        assert head[256 + 29] == 2, "the chunk fixture must use per-column frames"
        flat = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        chunk = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        flat.use_table("flat")
        chunk.use_table("chunk")
        yield tmp, data, flat, chunk
        flat.close()
        chunk.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_partial_aggregation_matches_flat_across_encodings(compared_tables):
    _tmp, data, flat, chunk = compared_tables
    for name, template in SHAPES:
        flat_sql = template.format(table="flat")
        chunk_sql = template.format(table="chunk")
        flat_result = _round(_values(flat.execute(flat_sql)))
        chunk_result = _round(_values(chunk.execute(chunk_sql)))
        assert chunk_result == flat_result, (name, flat_result, chunk_result)

    # Two independent checks so the equality above cannot hide a shared bug.
    k = data["k"]
    f1 = data["f1"]
    n1 = data["n1"]
    assert flat.execute(
        "SELECT COUNT(*) AS n FROM flat WHERE k >= 500000"
    ).to_dict()[0]["n"] == sum(1 for value in k if value >= 500000)
    expected_sum = float(np.sum([value for value, key in zip(f1, k) if key >= 500000]))
    got = chunk.execute(
        "SELECT SUM(f1) AS s FROM chunk WHERE k >= 500000"
    ).to_dict()[0]["s"]
    assert abs(got - expected_sum) <= abs(expected_sum) * 1e-9 + 1e-6, (got, expected_sum)
    i1 = data["i1"]
    expected_count = sum(
        1 for value, key in zip(n1, i1) if key >= 5000 and value is not None
    )
    assert chunk.execute(
        "SELECT COUNT(n1) AS n FROM chunk WHERE i1 >= 5000"
    ).to_dict()[0]["n"] == expected_count


def test_partial_aggregation_after_delete_rewrite(compared_tables):
    tmp, data, flat, chunk = compared_tables
    delete = "DELETE FROM {table} WHERE k >= 1000 AND k < 1010"
    flat.execute(delete.format(table="flat"))
    chunk.execute(delete.format(table="chunk"))
    removed = sum(1 for value in data["k"] if 1000 <= value < 1010)
    for name, template in (
        ("count_star", "SELECT COUNT(*) AS n FROM {table} WHERE k >= 500000"),
        ("sum_float", "SELECT SUM(f1) AS s FROM {table} WHERE k >= 500000"),
        ("sum_nullable", "SELECT SUM(n1) AS s FROM {table} WHERE f1 >= 0.25"),
    ):
        flat_result = _round(_values(flat.execute(template.format(table="flat"))))
        chunk_result = _round(_values(chunk.execute(template.format(table="chunk"))))
        assert chunk_result == flat_result, (name, flat_result, chunk_result)
    assert flat.execute(
        "SELECT COUNT(*) AS n FROM flat"
    ).to_dict()[0]["n"] == ROWS - removed
