"""A string equality that cannot match must not cost a column materialization.

`COUNT(*) WHERE s = 'absent'` used to be the *slow* case: on a 200k-row table a
missing value cost 1.16 ms (indexed) / 0.93 ms (plain) against 0.14 ms for a value
that exists, because absence fell through to a lane that read and filtered the whole
column. Two exact proofs now answer it from an empty batch:

* a value that appears in no row group's dictionary cannot be stored; and
* a value outside every row group's string zone range cannot be stored either (this
  one covers plain, non-dictionary columns).

Both refuse when any row group has deletions (the removed rows may have held the
value), and a single-column equality index that holds no such key is a third proof.
Every proof is conservative: when it cannot prove absence, the query takes the
scanning path exactly as before.
"""

import tempfile

from apexbase.client import ApexClient


def _client(tmp, *, indexed, deleted, dictionary):
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", schema={"k": "int64", "s": "string"})
    client.use_table("t")
    rows = 20_000
    values = ([f"c{i % 7}" for i in range(rows)] if dictionary
              else [f"v{i:06d}" for i in range(rows)])
    client.store({"k": list(range(rows)), "s": values})
    client.flush()
    if indexed:
        client.execute("CREATE INDEX ix_s ON t (s)")
    if deleted:
        client.execute("DELETE FROM t WHERE s = 'c0' AND k = 0"
                       if dictionary else "DELETE FROM t WHERE s = 'v000005'")
        client.flush()
    return client


def _count(client, value):
    return client.execute(
        f"SELECT COUNT(*) AS n FROM t WHERE s = '{value}'"
    ).to_dict()[0]["n"]


def test_present_extreme_and_absent_values_stay_exact():
    for indexed in (False, True):
        for deleted in (False, True):
            with tempfile.TemporaryDirectory() as tmp:
                client = _client(tmp, indexed=indexed, deleted=deleted,
                                 dictionary=False)
                try:
                    label = f"indexed={indexed} deleted={deleted}"
                    # Present values, including both ends of the range.
                    assert _count(client, "v000000") == 1, label
                    assert _count(client, "v019999") == 1, label
                    assert _count(client, "v010000") == 1, label
                    # Absent: below the range, above it, and between two values.
                    for probe in ("a", "zzzzzz", "v0000000"):
                        assert _count(client, probe) == 0, (label, probe)
                    if deleted:
                        # The only row holding this value was deleted, so it must not
                        # be reported as present.
                        assert _count(client, "v000005") == 0, label
                finally:
                    client.close()


def test_dictionary_columns_use_the_same_proof():
    for indexed in (False, True):
        with tempfile.TemporaryDirectory() as tmp:
            client = _client(tmp, indexed=indexed, deleted=False, dictionary=True)
            try:
                # 20_000 rows over 7 values: 20000/7 rounds to 2858 for the
                # first two values and 2857 for the rest.
                assert _count(client, "c0") == 2858
                assert _count(client, "c6") == 2857
                assert _count(client, "nope") == 0
            finally:
                client.close()


def test_an_empty_result_keeps_the_aggregate_convention():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, indexed=False, deleted=False, dictionary=False)
        try:
            row = client.execute(
                "SELECT COUNT(*) AS c, COUNT(s) AS cs, SUM(k) AS s, "
                "MIN(k) AS mn, MAX(k) AS mx FROM t WHERE s = 'absent'"
            ).to_dict()[0]
            # SUM over an empty set is 0 in this engine (the numeric empty filter
            # reports the same), and MIN/MAX are NULL.
            assert (row["c"], row["cs"], row["s"], row["mn"], row["mx"]) == (
                0, 0, 0, None, None)
        finally:
            client.close()


def test_expression_aggregates_still_resolve_their_columns():
    """The empty-batch answer must carry the full schema.

    Returning only `aggregate_input_columns` broke aggregates whose expression
    references a column the projection list does not name: `SUM(CASE WHEN k > 0
    ...) ... WHERE s = 'absent'` failed with "Column 'k' not found" -- a regression
    from the round that introduced this lane.
    """
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, indexed=False, deleted=False, dictionary=False)
        try:
            # A plain column SUM is 0 over an empty set in this engine; an
            # *expression* aggregate is NULL, exactly as a numeric empty filter
            # reports it. COUNT is 0 either way and MIN/MAX stay NULL.
            cases = {
                "SELECT SUM(k) AS v FROM t WHERE s = 'absent'": 0,
                "SELECT COUNT(*) AS v FROM t WHERE s = 'absent'": 0,
                "SELECT COUNT(CASE WHEN k > 0 THEN 1 END) AS v FROM t "
                "WHERE s = 'absent'": 0,
                "SELECT SUM(CASE WHEN k > 0 THEN 1 ELSE 0 END) AS v FROM t "
                "WHERE s = 'absent'": None,
                "SELECT SUM(k * 2) AS v FROM t WHERE s = 'absent'": None,
                "SELECT SUM(LENGTH(s)) AS v FROM t WHERE s = 'absent'": None,
                "SELECT MIN(k) AS v FROM t WHERE s = 'absent'": None,
            }
            for sql, want in cases.items():
                got = client.execute(sql).to_dict()[0]["v"]
                assert got == want, (sql, got, want)
            # The same convention holds on the generic (numeric) empty filter.
            for sql, want in (
                ("SELECT SUM(CASE WHEN k > 0 THEN 1 ELSE 0 END) AS v FROM t "
                 "WHERE k < 0", None),
                ("SELECT SUM(k) AS v FROM t WHERE k < 0", 0),
            ):
                got = client.execute(sql).to_dict()[0]["v"]
                assert got == want, (sql, got, want)
            # A matching filter still evaluates the expression.
            assert client.execute(
                "SELECT SUM(CASE WHEN k > 0 THEN 1 ELSE 0 END) AS v FROM t "
                "WHERE s = 'v000100'"
            ).to_dict()[0]["v"] == 1
        finally:
            client.close()


def test_a_pending_update_that_introduces_the_value_is_not_answered_as_absent():
    with tempfile.TemporaryDirectory() as tmp:
        client = _client(tmp, indexed=True, deleted=False, dictionary=True)
        try:
            client.execute("UPDATE t SET s = 'brandnew' WHERE k = 1")
            client.flush()
            # The index and the dictionaries were written before the update, so the
            # proofs must stand down while the overlay is visible.
            assert _count(client, "brandnew") == 1
        finally:
            client.close()
