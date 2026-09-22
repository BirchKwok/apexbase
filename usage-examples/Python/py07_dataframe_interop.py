"""Scenario 07: Pandas / Polars / PyArrow interoperability round-trips

Business context
----------------
A data team's day is half Python and half SQL:

* Feature engineers build a batch of data in **Pandas** and want to push it
  straight into ApexBase for SQL validation.
* Reporting folks prefer **Polars** for its lazy execution and memory efficiency.
* Platform folks use **PyArrow** for zero-copy columnar transfer and
  cross-process exchange.

When the same batch of data is shuttled back and forth between these three
in-memory formats, the usual failure is not "it cannot be read" but that it
**quietly changes shape**: int64 becomes float64, string columns sprout NaN,
boolean columns get treated as strings. None of this raises an error, yet it
silently skews every downstream model.

This example pins the behaviour down with round-trip verification: after
writing the data through each format, read it back through all three formats
and compare schema and values item by item.

ApexBase features demonstrated
------------------------------
1. Three write entry points: ``client.from_pandas(df)`` /
   ``client.from_polars(df)`` / ``client.from_pyarrow(table)``.
   They all accept ``table_name=``, so "select/create table + import" is a
   single step.
2. Three read exit points: ``ResultView.to_pandas()`` / ``.to_polars()`` /
   ``.to_arrow()``.
3. **Type fidelity**: ``int64`` / ``float64`` / ``bool`` / ``timestamp[us]`` /
   ``date32`` survive the round trip bit for bit (asserted per column here).
4. **Value fidelity**: large integers (above ``2**32``), tiny floats (``1e-9``
   magnitude) and negatives come back identical, with no precision loss or
   double rounding.
5. **Null fidelity**: ``None / NA / NaN`` stay null after the round trip and
   are never filled with 0 or an empty string.

Three measured behaviours you must know about (or the assertions will be wrong)
------------------------------------------------------------------------------
1. **Column order is not guaranteed to be preserved.** Columns written by
   ``from_pandas`` in the order ``a, b, c`` may be read back as ``a, c, b``,
   because ApexBase stores them in its own internal column layout. This does
   not affect correctness (SQL always addresses columns by name), but **never
   read columns by positional index**. Every assertion here goes by column name
   and explicitly checks that the column-name sets match.
2. **Low-cardinality string columns are dictionary-encoded.** A written
   ``string`` may read back as the Arrow type
   ``dictionary<values=string, indices=uint32>``, and as ``Categorical`` on the
   Polars side. This is lossless compression and the logical type is still a
   string, but it makes a naive "Arrow types must be strictly equal" assertion
   fail. This example restores dictionary types to ``string`` through
   ``canonical_arrow_type()`` before comparing, and prints the differences
   explicitly.
3. ``to_pandas()`` returns **PyArrow-backed** dtypes (``int64[pyarrow]`` /
   ``string[pyarrow]``), not numpy's ``int64`` / ``object``. The data has not
   changed either, only pandas' storage backend has; comparisons must use
   ``reset_index(drop=True)`` + ``check_dtype=False``. Note that a string
   column passing through ``to_pandas()`` is decoded from the dictionary type
   back to ``string[pyarrow]``, so the pandas exit point never exposes the
   dictionary encoding.

Capability boundaries (this example is written against what works today)
-----------------------------------------------------------------------
1. Time-type fidelity is only verified over the **PyArrow channel**
   (``timestamp[us]`` / ``date32``). In SQL, still do not count on
   ``DATE_TRUNC`` / ``strftime`` / ``DATE()`` -- they are unsupported; when you
   need time buckets, materialise them as a string column at write time.
2. pandas 3.x's ``str`` extension dtype is accepted correctly by ApexBase; but
   handing such a DataFrame to some third-party engines (for example DuckDB
   1.1.x) may require ``.astype(object)`` first. That is their compatibility
   problem, not ApexBase's.
3. **Do not use SQL reserved words as table names.** This example originally
   named the table holding the nulls ``nulls``, and ``SELECT * FROM nulls``
   failed with ``Expected table name after FROM``: the parser read it as the
   ``NULLS`` keyword from ``ORDER BY ... NULLS FIRST/LAST``. Renaming it to
   ``null_data`` fixes it. Other words to avoid include ``values`` / ``rows``.

How to run
----------
    python py07_dataframe_interop.py

Output: the console prints the round-trip verdict for every step; the database
file lives in ``_out/py_07/db``.
"""

from __future__ import annotations

import datetime as dt
import os

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa

from apexbase import ApexClient
from _demo_env import assert_close, rng, section, show, work_dir

SLUG = "07"

ROW_COUNT = 200


# ============================================================================
# Data construction: build one equivalent batch in each of the three formats
# ============================================================================
def build_records() -> list[dict]:
    """Build a deterministic batch of raw records.

    Deliberately includes several value classes that format conversion tends to
    corrupt:
    * negatives and large integers (``2**40``, beyond int32) -- checks they are
      not downcast;
    * tiny floats (``1e-9`` magnitude) and negatives -- checks precision is kept;
    * booleans -- checks they are not treated as 0/1 or as strings;
    * strings with an internal space -- checks encoding and length.
    """
    rnd = rng(20240701)
    rows = []
    for i in range(ROW_COUNT):
        rows.append(
            {
                "row_id": i + 1,
                "int_val": int(rnd.randrange(-10_000, 10_000)),
                "big_int": 2**40 + i,
                "float_val": round(rnd.uniform(-1e6, 1e6), 6),
                # Format as a string first, then parse to float, so these really
                # are tiny values rather than values rounded down to 0.
                "tiny_float": float(f"{rnd.uniform(1e-9, 1e-6):.12f}"),
                "flag": (i % 2 == 0),
                "label": ["alpha", "beta", "gamma", "data group"][i % 4],
            }
        )
    return rows


def build_pandas_frame(records: list[dict]) -> pd.DataFrame:
    """Build the Pandas DataFrame with explicit numpy dtypes.

    The explicit dtypes are deliberate: they force ``from_pandas`` down a
    deterministic type-inference path, so implicit behaviour such as "this
    column is all integers, therefore it becomes int" cannot pollute the
    assertions.
    """
    return pd.DataFrame(
        {
            "row_id": np.array([r["row_id"] for r in records], dtype=np.int64),
            "int_val": np.array([r["int_val"] for r in records], dtype=np.int64),
            "big_int": np.array([r["big_int"] for r in records], dtype=np.int64),
            "float_val": np.array([r["float_val"] for r in records], dtype=np.float64),
            "tiny_float": np.array([r["tiny_float"] for r in records], dtype=np.float64),
            "flag": np.array([r["flag"] for r in records], dtype=bool),
            "label": [r["label"] for r in records],
        }
    )


def build_polars_frame(records: list[dict]) -> pl.DataFrame:
    """Build the Polars DataFrame with explicit dtypes, for the same reason."""
    return pl.DataFrame(
        {
            "row_id": pl.Series([r["row_id"] for r in records], dtype=pl.Int64),
            "int_val": pl.Series([r["int_val"] for r in records], dtype=pl.Int64),
            "big_int": pl.Series([r["big_int"] for r in records], dtype=pl.Int64),
            "float_val": pl.Series([r["float_val"] for r in records], dtype=pl.Float64),
            "tiny_float": pl.Series(
                [r["tiny_float"] for r in records], dtype=pl.Float64
            ),
            "flag": pl.Series([r["flag"] for r in records], dtype=pl.Boolean),
            "label": pl.Series([r["label"] for r in records], dtype=pl.String),
        }
    )


def build_arrow_table(records: list[dict]) -> pa.Table:
    """Build a PyArrow Table, with two extra columns: ``timestamp[us]`` / ``date32``.

    Time types are only demonstrated over the Arrow channel: ``from_pyarrow``
    maps them to ApexBase's ``timestamp`` / ``date`` column types, and the schema
    is identical after the round trip.
    """
    base_day = dt.datetime(2024, 7, 1, 12, 30, 0)
    return pa.table(
        {
            "row_id": pa.array([r["row_id"] for r in records], pa.int64()),
            "big_int": pa.array([r["big_int"] for r in records], pa.int64()),
            "float_val": pa.array([r["float_val"] for r in records], pa.float64()),
            "flag": pa.array([r["flag"] for r in records], pa.bool_()),
            "label": pa.array([r["label"] for r in records], pa.string()),
            "event_ts": pa.array(
                [base_day + dt.timedelta(hours=r["row_id"]) for r in records],
                pa.timestamp("us"),
            ),
            "event_day": pa.array(
                [
                    (base_day + dt.timedelta(days=r["row_id"] % 30)).date()
                    for r in records
                ],
                pa.date32(),
            ),
        }
    )


# ============================================================================
# Canonicalisation + validators: fix "what counts as equal" in reusable asserts
# ============================================================================
def canonical_arrow_type(typ: pa.DataType) -> pa.DataType:
    """Fold "different representation, same logical type" types into one for comparison.

    Two families need folding:
    * **Dictionary encoding**: for low-cardinality string columns ApexBase uses
      ``dictionary<values=string>``, which is lossless compression and logically
      still ``string``;
    * **Width variants**: pandas 3.x's ``str`` extension dtype becomes
      ``large_string`` (64-bit offsets) through ``pa.Table.from_pandas``,
      whereas ApexBase stores ``string`` (32-bit offsets). Both are UTF-8
      strings, only the offset width differs.
    Without folding, both cases would be misreported as "type drift".
    """
    if pa.types.is_dictionary(typ):
        return canonical_arrow_type(typ.value_type)
    if pa.types.is_large_string(typ):
        return pa.string()
    if pa.types.is_large_binary(typ):
        return pa.binary()
    return typ


def canonical_arrow_table(table: pa.Table, columns: list[str]) -> pa.Table:
    """Reorder to the given columns, decode dictionaries and unify string widths.

    The result can be compared item by item.
    """
    data = {}
    for name in columns:
        col = table.column(name).combine_chunks()
        if pa.types.is_dictionary(col.type):
            col = col.dictionary_decode()
        # Collapse large_string / large_binary to the 32-bit variants so the
        # schemas can be compared for direct equality.
        if pa.types.is_large_string(col.type):
            col = col.cast(pa.string())
        elif pa.types.is_large_binary(col.type):
            col = col.cast(pa.binary())
        data[name] = col
    return pa.table(data)


def assert_arrow_equal(label: str, expected: pa.Table, actual: pa.Table) -> None:
    """Arrow-level round-trip check: logical types + data.

    Three check levels, so a failure immediately tells you whether a column went
    missing, its type changed, or its values changed:
    1. column-name sets match (order is not required -- ApexBase does not
       preserve it);
    2. each column's **canonical type** matches (dictionary encoding counts as
       string);
    3. after canonicalising to the expected column order, the data is equal item
       by item.
    """
    if set(actual.column_names) != set(expected.column_names):
        raise AssertionError(
            f"{label}: column-name sets differ\n"
            f"expected: {sorted(expected.column_names)}\n"
            f"actual:   {sorted(actual.column_names)}"
        )

    for field in expected.schema:
        want = canonical_arrow_type(field.type)
        got = canonical_arrow_type(actual.schema.field(field.name).type)
        if not want.equals(got):
            raise AssertionError(
                f"{label}: column {field.name} changed type, {field.type} -> "
                f"{actual.schema.field(field.name).type}"
            )

    expected_norm = canonical_arrow_table(expected, expected.column_names)
    actual_norm = canonical_arrow_table(actual, expected.column_names)
    if not expected_norm.schema.equals(actual_norm.schema):
        raise AssertionError(
            f"{label}: schemas still differ after canonicalisation\n"
            f"expected: {expected_norm.schema}\n"
            f"actual:   {actual_norm.schema}"
        )
    if not expected_norm.equals(actual_norm):
        raise AssertionError(f"{label}: same Arrow logical types but the data differs")
    print(f"[OK] {label}: column-name set, per-column logical types and data all match")


def canonical_polars_frame(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """Restore Polars' ``Categorical`` (= Arrow dictionary encoding) to ``String``."""
    exprs = []
    for name in columns:
        if df.schema[name] == pl.Categorical:
            exprs.append(pl.col(name).cast(pl.String))
        else:
            exprs.append(pl.col(name))
    return df.select(exprs)


def assert_polars_equal(label: str, expected: pl.DataFrame, actual: pl.DataFrame) -> None:
    """Polars-level round-trip check, likewise treating ``Categorical`` as logical ``String``."""
    if set(actual.columns) != set(expected.columns):
        raise AssertionError(
            f"{label}: column-name sets differ\n"
            f"expected: {sorted(expected.columns)}\n"
            f"actual:   {sorted(actual.columns)}"
        )
    for name in expected.columns:
        want, got = expected.schema[name], actual.schema[name]
        # Categorical is the result of dictionary encoding, logically equal to String.
        if got == pl.Categorical and want == pl.String:
            continue
        if want != got:
            raise AssertionError(f"{label}: column {name} changed type, {want} -> {got}")

    if not canonical_polars_frame(expected, expected.columns).equals(
        canonical_polars_frame(actual, expected.columns)
    ):
        raise AssertionError(f"{label}: same Polars logical types but the data differs")
    print(f"[OK] {label}: column-name set, per-column logical types and data all match")


def assert_pandas_equal(label: str, expected: pd.DataFrame, actual: pd.DataFrame) -> None:
    """Pandas-level round-trip check.

    Three things that must be relaxed (see the module docstring for why):
    * reorder by column name: ApexBase does not preserve column order, so a
      positional comparison would raise false alarms;
    * ``reset_index(drop=True)``: the index coming out of ``to_pandas()`` is a
      pyarrow-backed integer type whose dtype differs from the original numpy
      index, even though the sequence numbers agree;
    * switches such as ``check_dtype=False``: ignore pyarrow / numpy
      storage-backend differences.

    Values are still compared by **exact equality** -- every float here is moved
    directly from the same Python floats, no arithmetic is involved, so no
    ULP-level drift should appear. If drift does appear, precision was lost and
    it must be surfaced.
    """
    actual = actual[list(expected.columns)]
    pd.testing.assert_frame_equal(
        actual.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
        check_index_type=False,
        check_column_type=False,
    )
    print(f"[OK] {label}: pandas column names and values match (pyarrow backend dtypes ignored)")


def main() -> None:
    base = work_dir(SLUG)
    records = build_records()
    pdf = build_pandas_frame(records)
    pldf = build_polars_frame(records)
    arrow_table = build_arrow_table(records)

    with ApexClient(os.path.join(base, "db")) as client:
        # ------------------------------------------------------------------
        section("Step 1: Pandas -> ApexBase -> Pandas / Arrow / Polars (round trips in three directions)")
        # from_pandas(df, table_name=...) creates the table and imports in one
        # step, so no explicit create_table is needed.
        client.from_pandas(pdf, table_name="from_pd")
        show("from_pandas imported rows", client.count_rows())
        assert client.count_rows() == ROW_COUNT, "from_pandas should write every row"

        back_pd = client.execute("SELECT * FROM from_pd ORDER BY row_id").to_pandas()
        show("to_pandas dtypes (pyarrow backend, strings decoded back to string)",
             # Sort by column name before printing: the physical column order is
             # itself unstable (see step 2), and sorting is what makes the
             # example output reproducible from run to run.
             dict(sorted((k, str(v)) for k, v in back_pd.dtypes.items())))
        show("original pandas dtypes", dict(sorted((k, str(v)) for k, v in pdf.dtypes.items())))
        assert back_pd.shape == pdf.shape, f"shapes should match: {back_pd.shape} vs {pdf.shape}"
        assert_pandas_equal("Pandas round trip", pdf, back_pd)

        # Read the same data through a different exit to verify the exit point
        # does not affect the content.
        arrow_from_pd = client.execute("SELECT * FROM from_pd ORDER BY row_id").to_arrow()
        assert_arrow_equal(
            "Pandas write -> Arrow read",
            pa.Table.from_pandas(pdf, preserve_index=False),
            arrow_from_pd,
        )

        polars_from_pd = client.execute("SELECT * FROM from_pd ORDER BY row_id").to_polars()
        show("Polars schema read back (label is dictionary-encoded to Categorical)",
             dict(sorted((k, str(v)) for k, v in polars_from_pd.schema.items())))
        assert_polars_equal("Pandas write -> Polars read", pldf, polars_from_pd)

        # ------------------------------------------------------------------
        section("Step 2: column order is not guaranteed (address columns by name, never by position)")
        inserted_order = list(pdf.columns)
        read_order = client.execute("SELECT * FROM from_pd").columns
        show("column order at write time (source DataFrame order)", inserted_order)
        show("column order at read time", sorted(read_order))
        assert set(read_order) == set(inserted_order), "column-name sets must match"
        print(
            "[OK] column-name sets match"
            + (" (the order happens to match too)" if read_order == inserted_order else " (different order, which is expected)")
        )
        # Select columns explicitly by name to show that a different physical
        # order never affects the values.
        by_name = client.execute(
            "SELECT row_id, label, big_int, flag FROM from_pd ORDER BY row_id"
        ).to_arrow()
        assert by_name.column_names == ["row_id", "label", "big_int", "flag"], (
            f"projection order should follow SELECT, actual {by_name.column_names}"
        )
        print("[OK] SELECT projection order is decided by the query, independent of the table's physical column order")

        # ------------------------------------------------------------------
        section("Step 3: Polars -> ApexBase -> Polars / Pandas / Arrow")
        client.from_polars(pldf, table_name="from_pl")
        show("from_polars imported rows", client.count_rows())
        assert client.count_rows() == ROW_COUNT

        back_pl = client.execute("SELECT * FROM from_pl ORDER BY row_id").to_polars()
        show("Polars round-trip schema", dict(sorted((k, str(v)) for k, v in back_pl.schema.items())))
        assert_polars_equal("Polars round trip", pldf, back_pl)

        pd_from_pl = client.execute("SELECT * FROM from_pl ORDER BY row_id").to_pandas()
        assert_pandas_equal("Polars write -> Pandas read", pdf, pd_from_pl)

        arrow_from_pl = client.execute("SELECT * FROM from_pl ORDER BY row_id").to_arrow()
        assert_arrow_equal(
            "Polars write -> Arrow read",
            pa.Table.from_pandas(pdf, preserve_index=False),
            arrow_from_pl,
        )

        # ------------------------------------------------------------------
        section("Step 4: PyArrow -> ApexBase -> Arrow / Polars / Pandas (including time types)")
        client.from_pyarrow(arrow_table, table_name="from_arrow")
        show("from_pyarrow imported rows", client.count_rows())
        assert client.count_rows() == ROW_COUNT

        back_arrow = client.execute("SELECT * FROM from_arrow ORDER BY row_id").to_arrow()
        show("original Arrow schema", str(arrow_table.schema).replace("\n", " | "))
        show("Arrow schema after the round trip", str(back_arrow.schema).replace("\n", " | "))
        assert_arrow_equal("Arrow round trip (including timestamp/date32)", arrow_table, back_arrow)

        # The actual values of the time columns must be checked too, not just
        # their types.
        first = back_arrow.to_pylist()[0]
        show("time column sample", {"event_ts": first["event_ts"], "event_day": first["event_day"]})
        assert first["event_ts"] == dt.datetime(2024, 7, 1, 13, 30, 0), (
            f"timestamp value wrong after the round trip: {first['event_ts']}"
        )
        # The first row has row_id = 1; the date column is generated as an
        # offset of row_id % 30 days, hence 07-02.
        assert first["event_day"] == dt.date(2024, 7, 2), (
            f"date32 value wrong after the round trip: {first['event_day']}"
        )
        print("[OK] timestamp[us] and date32 keep both type and value across the round trip")

        # ------------------------------------------------------------------
        section("Step 5: the three channels cross-check each other (same data, three entry points must be equivalent)")
        # Pull the same common columns from all three tables and compare at the
        # Arrow level -- the strongest equivalence assertion. The from_pd /
        # from_pl tables have no time columns, so only the common columns are
        # compared.
        common_cols = "row_id, big_int, float_val, flag, label"
        arrow_via_pd = client.execute(
            f"SELECT {common_cols} FROM from_pd ORDER BY row_id"
        ).to_arrow()
        arrow_via_pl = client.execute(
            f"SELECT {common_cols} FROM from_pl ORDER BY row_id"
        ).to_arrow()
        arrow_via_ar = client.execute(
            f"SELECT {common_cols} FROM from_arrow ORDER BY row_id"
        ).to_arrow()
        assert_arrow_equal("Pandas channel vs Polars channel", arrow_via_pd, arrow_via_pl)
        assert_arrow_equal("Pandas channel vs Arrow channel", arrow_via_pd, arrow_via_ar)
        print("[OK] the common columns of all three import channels are exactly equivalent")

        # ------------------------------------------------------------------
        section("Step 6: null fidelity (None / NA / NaN are not filled with 0 or an empty string)")
        # Nulls are where silent filling happens most easily. This covers three
        # null carriers at once: float64 NaN, Arrow/Polars null, and pandas'
        # nullable Int64 NA.
        null_pdf = pd.DataFrame(
            {
                "row_id": np.array([1, 2, 3, 4], dtype=np.int64),
                "float_val": np.array([1.5, np.nan, 3.5, np.nan], dtype=np.float64),
                "label": pl.Series(["a", None, "c", None], dtype=pl.String).to_pandas(),
                "nullable_int": pd.array([10, None, 30, None], dtype="Int64"),
            }
        )
        show("original DataFrame with nulls", null_pdf.to_dict("records"))
        # Do not use SQL reserved words as table names: `nulls` makes the parser
        # read the ORDER BY NULLS FIRST/LAST clause and report
        # "Expected table name after FROM".
        client.from_pandas(null_pdf, table_name="null_data")
        null_back = client.execute("SELECT * FROM null_data ORDER BY row_id").to_arrow()
        show("nulls after the round trip", dict(sorted(null_back.to_pydict().items())))  # sort keys so output is reproducible
        assert_arrow_equal(
            "null round trip",
            # preserve_index=False: do not bring pandas' RangeIndex in as a column.
            pa.Table.from_pandas(null_pdf, preserve_index=False),
            null_back,
        )

        # Cross-check from the SQL side by counting nulls, rather than
        # concluding from a glance at the data.
        null_counts = client.execute(
            """
            SELECT
                SUM(CASE WHEN float_val IS NULL THEN 1 ELSE 0 END)    AS float_nulls,
                SUM(CASE WHEN label IS NULL THEN 1 ELSE 0 END)        AS label_nulls,
                SUM(CASE WHEN nullable_int IS NULL THEN 1 ELSE 0 END) AS int_nulls
            FROM null_data
            """
        ).to_dict()[0]
        show("SQL null counts", null_counts)
        assert null_counts["float_nulls"] == 2, "float_val should have 2 nulls"
        assert null_counts["label_nulls"] == 2, "label should have 2 nulls"
        assert null_counts["int_nulls"] == 2, "nullable_int should have 2 nulls"
        print("[OK] nulls are still null after the round trip, and the SQL counts match the source DataFrame")

        # ------------------------------------------------------------------
        section("Step 7: type fidelity table (write type -> read type, column by column)")
        # Lay "what type goes in, what type comes out" out as a table so it can
        # be scanned at a glance. Dictionary encoding is called out separately
        # as "equivalent but different" instead of a blanket pass/fail.
        print(f"{'column':<12}{'Arrow type written':<24}{'Arrow type read':<44}{'verdict'}")
        print("-" * 92)
        for field in arrow_table.schema:
            out_type = back_arrow.schema.field(field.name).type
            want = canonical_arrow_type(field.type)
            got = canonical_arrow_type(out_type)
            if not want.equals(got):
                verdict = "TYPE DRIFT!"
            elif field.type.equals(out_type):
                verdict = "exact match"
            else:
                verdict = "equivalent (encoding/width variant)"
            print(f"{field.name:<12}{str(field.type):<24}{str(out_type):<44}{verdict}")
            assert want.equals(got), (
                f"column {field.name} logical type changed from {field.type} to {out_type}"
            )

        # Pinpoint numeric-precision check: take the three rows with the
        # smallest floats and compare them item by item.
        extremes_pl = client.execute(
            "SELECT row_id, tiny_float FROM from_pd ORDER BY tiny_float LIMIT 3"
        ).to_polars()
        show("three rows with the smallest tiny_float", extremes_pl.to_dict(as_series=False))
        source_map = {r["row_id"]: r["tiny_float"] for r in records}
        for row_id, value in zip(extremes_pl["row_id"], extremes_pl["tiny_float"]):
            assert value == source_map[int(row_id)], (
                f"tiny_float of row_id={row_id} changed from {source_map[int(row_id)]!r} to {value!r}"
            )
        print("[OK] float extremes are item-for-item identical after the round trip (no double rounding)")

        # Large integers likewise -- this is the type most easily and silently
        # downcast to float64.
        big_pl = client.execute(
            "SELECT row_id, big_int FROM from_pd ORDER BY big_int DESC LIMIT 3"
        ).to_polars()
        show("three rows with the largest big_int", big_pl.to_dict(as_series=False))
        assert big_pl["big_int"].dtype == pl.Int64, (
            f"big_int should stay Int64, actual {big_pl['big_int'].dtype}"
        )
        source_big = {r["row_id"]: r["big_int"] for r in records}
        for row_id, value in zip(big_pl["row_id"], big_pl["big_int"]):
            assert value == source_big[int(row_id)], f"big_int of row_id={row_id} was rewritten"
        assert max(source_big.values()) > 2**32, "the test data should cover integers above 2**32"
        print("[OK] integers above 2**32 stay Int64 with unchanged values after the round trip")

        # ------------------------------------------------------------------
        section("Step 8: summarise the interoperability results as a reconciliation table (Polars output)")
        cols_pd = client.execute("SELECT * FROM from_pd").columns
        cols_pl = client.execute("SELECT * FROM from_pl").columns
        cols_ar = client.execute("SELECT * FROM from_arrow").columns
        summary = pl.DataFrame(
            {
                "channel": ["from_pandas", "from_polars", "from_pyarrow"],
                "source_format": ["pandas", "polars", "pyarrow"],
                "row_count": [
                    client.count_rows("from_pd"),
                    client.count_rows("from_pl"),
                    client.count_rows("from_arrow"),
                ],
                "column_count": [len(cols_pd), len(cols_pl), len(cols_ar)],
                "has_time_columns": [
                    any(c in cols_pd for c in ("event_ts", "event_day")),
                    any(c in cols_pl for c in ("event_ts", "event_day")),
                    any(c in cols_ar for c in ("event_ts", "event_day")),
                ],
            }
        )
        print(summary)
        assert summary["row_count"].to_list() == [ROW_COUNT] * 3, "all three channels should have the source row count"
        # The three channels happen to all have 7 columns, but their
        # **composition differs**: only the Arrow source carries time columns,
        # while from_pd / from_pl carry int_val / tiny_float. So equal column
        # counts mean nothing on their own; the column-name sets must be
        # compared.
        assert summary["column_count"].to_list() == [7, 7, 7], (
            f"all three channels should have 7 columns, actual {summary['column_count'].to_list()}"
        )
        show("from_pandas / from_polars columns", sorted(cols_pd))
        show("from_pyarrow columns", sorted(cols_ar))
        assert set(cols_pd) == set(cols_pl), "the Pandas and Polars channels must have exactly the same column-name set"
        assert {"event_ts", "event_day"} <= set(cols_ar), "the Arrow channel should keep both time columns"
        assert {"int_val", "tiny_float"}.isdisjoint(cols_ar), (
            "the Arrow source never had int_val / tiny_float, so they must not be read back"
        )
        common = set(cols_pd) & set(cols_pl) & set(cols_ar)
        show("columns common to all three channels", sorted(common))
        assert common == {"row_id", "big_int", "float_val", "flag", "label"}, (
            f"unexpected common-column set: {sorted(common)}"
        )
        print("[OK] all three channels agree on the row count; 5 common columns, and each extra column is as expected (Arrow has 2 time columns)")

        assert_close(
            float(client.count_rows("from_pd")),
            float(ROW_COUNT),
            1e-9,
            "final row-count recheck",
        )

        print(f"\n=== Scenario 07 Pandas/Polars/PyArrow interoperability round trips complete ===\nDatabase file at: {base}")


if __name__ == "__main__":
    main()
