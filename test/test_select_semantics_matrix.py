"""Differential SELECT-semantics matrix: fast lanes vs an independent oracle.

The executor picks a fast lane per query shape (string/range filters, late
materialization, dictionary reads, cached/fused aggregation, dedup helpers), and
several defects found in this codebase were "the lane silently skipped a
remaining operator or mis-read a column": DISTINCT dropped when a WHERE was
present, a float-literal GROUP BY collapsing into one group, a BITPACK column
failing to decode inside the fused string-filter aggregation, and a GROUP BY key
where NULL hashed equal to 0.

This module re-runs a broad matrix of shapes and compares every result with a
pandas oracle computed directly from the generated columns. A lane that
mis-handles NULLs, ties or an encoding, or that drops DISTINCT / ORDER BY /
LIMIT / OFFSET / HAVING fails loudly.
"""

import shutil
import tempfile

import numpy as np
import pandas as pd
import pytest

from apexbase import ApexClient

N = 1200
DOC = [index % 200 for index in range(N)]
AGE = [None if index % 17 == 0 else int(index % 60) for index in range(N)]
SCORE = [None if index % 23 == 0 else float(index % 100) * 0.5 for index in range(N)]
CITY = [["Beijing", "Shanghai", "Guangzhou"][index % 3] for index in range(N)]
CAT = [f"c{index % 7}" for index in range(N)]
FLAG = [index % 2 == 0 for index in range(N)]

FRAME = pd.DataFrame(
    {"doc": DOC, "age": AGE, "score": SCORE, "city": CITY, "cat": CAT, "flag": FLAG}
)


@pytest.fixture(scope="module")
def client():
    root = tempfile.mkdtemp()
    apex = ApexClient(root, drop_if_exists=True, enable_cache=False)
    apex.create_table(
        "t",
        {
            "doc": "int64",
            "age": "int64",
            "score": "float64",
            "city": "string",
            "cat": "string",
            "flag": "bool",
        },
    )
    apex.use_table("t")
    apex.store(
        {"doc": DOC, "age": AGE, "score": SCORE, "city": CITY, "cat": CAT, "flag": FLAG}
    )
    apex.flush()
    try:
        yield apex
    finally:
        apex.close()
        shutil.rmtree(root, ignore_errors=True)


def _norm_rows(rows):
    out = []
    for row in rows:
        values = []
        for key in sorted(row):
            if key == "_id":
                continue
            value = row[key]
            if isinstance(value, float):
                value = None if np.isnan(value) else round(value, 6)
            values.append(value)
        out.append(tuple(values))
    return sorted(out, key=repr)


def _norm_frame(frame):
    out = []
    for record in frame.to_dict("records"):
        values = []
        for key in sorted(record):
            value = record[key]
            if value is None or (isinstance(value, float) and np.isnan(value)):
                value = None
            elif isinstance(value, np.integer):
                value = int(value)
            elif isinstance(value, (np.floating, float)):
                value = round(float(value), 6)
            values.append(value)
        out.append(tuple(values))
    return sorted(out, key=repr)


def _check(client, sql, frame):
    got = _norm_rows(client.execute(sql).to_dict())
    want = _norm_frame(frame)
    assert got == want, f"{sql}\n  got  {got[:4]}\n  want {want[:4]}"


def _pairs(client, sql, keys):
    """`{(key...): count}` for grouped results (order-independent)."""
    out = {}
    for row in client.execute(sql).to_dict():
        key = tuple(
            None if row[name] is None else (round(float(row[name]), 6) if isinstance(row[name], float) else row[name])
            for name in keys
        )
        out[key] = int(row["n"])
    return out


def _pairs_expected(frame, keys):
    out = {}
    for record in frame.to_dict("records"):
        key = tuple(
            None
            if record[name] is None or (isinstance(record[name], float) and np.isnan(record[name]))
            else (
                round(float(record[name]), 6)
                if isinstance(record[name], (float, np.floating))
                else int(record[name])
                if isinstance(record[name], np.integer)
                else record[name]
            )
            for name in keys
        )
        out[key] = out.get(key, 0) + int(record["n"])
    return out


# --------------------------------------------------------------------------
# 1. plain projections (scan, dictionary read, expression evaluation)
# --------------------------------------------------------------------------
def test_star_and_plain_projections(client):
    _check(client, "SELECT * FROM t", FRAME)
    _check(client, "SELECT doc FROM t", FRAME[["doc"]])
    _check(client, "SELECT doc, city, score FROM t", FRAME[["doc", "city", "score"]])


def test_projection_expressions_match_the_oracle(client):
    _check(
        client,
        "SELECT doc, abs(score - 2.5) AS b FROM t",
        FRAME.assign(b=FRAME["score"].apply(lambda s: None if pd.isna(s) else abs(s - 2.5)))[["doc", "b"]],
    )
    _check(
        client,
        "SELECT doc, upper(city) AS u FROM t",
        FRAME.assign(u=FRAME["city"].str.upper())[["doc", "u"]],
    )


# --------------------------------------------------------------------------
# 2. WHERE shapes (equality, range, string, conjunction, negation, IN, NULL)
# --------------------------------------------------------------------------
def test_where_shapes_match_the_oracle(client):
    age, score, city, doc = FRAME["age"], FRAME["score"], FRAME["city"], FRAME["doc"]
    cases = [
        ("SELECT doc, age FROM t WHERE age = 25", FRAME[age == 25][["doc", "age"]]),
        (
            "SELECT doc, age FROM t WHERE age BETWEEN 20 AND 30",
            FRAME[(age >= 20) & (age <= 30)][["doc", "age"]],
        ),
        ("SELECT doc, city FROM t WHERE city = 'Beijing'", FRAME[city == "Beijing"][["doc", "city"]]),
        ("SELECT doc, city FROM t WHERE city LIKE 'B%'", FRAME[city.str.startswith("B")][["doc", "city"]]),
        (
            "SELECT doc, age, city FROM t WHERE age > 20 AND city = 'Beijing'",
            FRAME[(age > 20) & (city == "Beijing")][["doc", "age", "city"]],
        ),
        # SQL NULL semantics: a comparison with NULL is unknown, so the row is
        # excluded from both the predicate and its negation.
        (
            "SELECT doc, age FROM t WHERE age NOT BETWEEN 20 AND 30",
            FRAME[age.notna() & ~((age >= 20) & (age <= 30))][["doc", "age"]],
        ),
        ("SELECT doc, age FROM t WHERE age IN (20, 30, 40)", FRAME[age.isin([20, 30, 40])][["doc", "age"]]),
        ("SELECT doc, age FROM t WHERE age IS NULL", FRAME[age.isna()][["doc", "age"]]),
        ("SELECT doc, age FROM t WHERE age IS NOT NULL", FRAME[age.notna()][["doc", "age"]]),
        ("SELECT doc, score FROM t WHERE score IS NOT NULL", FRAME[score.notna()][["doc", "score"]]),
    ]
    for sql, frame in cases:
        _check(client, sql, frame)


# --------------------------------------------------------------------------
# 3. ORDER BY / LIMIT / OFFSET (the sorted shapes are total orders)
# --------------------------------------------------------------------------
def test_order_limit_offset_match_the_oracle(client):
    age, doc, score = FRAME["age"], FRAME["doc"], FRAME["score"]
    ordered = FRAME[age.notna()].sort_values(["age", "doc"], kind="stable")
    _check(
        client,
        "SELECT doc, age FROM t WHERE age IS NOT NULL ORDER BY age, doc LIMIT 10",
        ordered[["doc", "age"]].head(10),
    )
    _check(
        client,
        "SELECT doc, age FROM t WHERE age IS NOT NULL ORDER BY age, doc LIMIT 10 OFFSET 5",
        ordered[["doc", "age"]].iloc[5:15],
    )
    desc = FRAME[age.notna()].sort_values(["age", "doc"], ascending=[False, True], kind="stable")
    _check(
        client,
        "SELECT doc, age FROM t WHERE age IS NOT NULL ORDER BY age DESC, doc LIMIT 10",
        desc[["doc", "age"]].head(10),
    )
    # An expression in ORDER BY whose rendered name contains a float literal.
    big = FRAME.assign(b=score.apply(lambda s: None if pd.isna(s) else abs(s - 2.5)))
    big = big.sort_values(["b", "doc"], kind="stable")
    _check(
        client,
        "SELECT doc, abs(score - 2.5) AS b FROM t ORDER BY abs(score - 2.5), doc LIMIT 7",
        big[["doc", "b"]].head(7),
    )
    _check(
        client,
        "SELECT doc, abs(score - 2.5) AS b FROM t ORDER BY abs(score - 2.5), doc",
        big[["doc", "b"]],
    )


# --------------------------------------------------------------------------
# 4. DISTINCT (with and without WHERE, plain and expression projections)
# --------------------------------------------------------------------------
def test_distinct_shapes_match_the_oracle(client):
    doc = FRAME["doc"]
    _check(client, "SELECT DISTINCT city FROM t", FRAME[["city"]].drop_duplicates())
    _check(
        client,
        "SELECT DISTINCT city FROM t WHERE doc >= 100",
        FRAME[doc >= 100][["city"]].drop_duplicates(),
    )
    _check(
        client,
        "SELECT DISTINCT upper(city) AS u FROM t",
        FRAME.assign(u=FRAME["city"].str.upper())[["u"]].drop_duplicates(),
    )
    _check(
        client,
        "SELECT DISTINCT city, cat FROM t WHERE doc < 200 ORDER BY city, cat",
        FRAME[doc < 200][["city", "cat"]]
        .drop_duplicates()
        .sort_values(["city", "cat"], kind="stable"),
    )


# --------------------------------------------------------------------------
# 5. aggregates, GROUP BY, HAVING (NULL-aware)
# --------------------------------------------------------------------------
def test_global_aggregates_match_the_oracle(client):
    got = client.execute(
        "SELECT COUNT(score) AS n, SUM(score) AS s, AVG(score) AS a, "
        "MIN(score) AS lo, MAX(score) AS hi FROM t"
    ).to_dict()[0]
    score = FRAME["score"]
    assert int(got["n"]) == int(score.notna().sum())
    assert round(float(got["s"]), 6) == round(float(score.sum()), 6)
    assert round(float(got["a"]), 6) == round(float(score.mean()), 6)
    assert round(float(got["lo"]), 6) == round(float(score.min()), 6)
    assert round(float(got["hi"]), 6) == round(float(score.max()), 6)

    got = client.execute("SELECT COUNT(age) AS n, MIN(age) AS lo, MAX(age) AS hi FROM t").to_dict()[0]
    age = FRAME["age"]
    assert (int(got["n"]), int(got["lo"]), int(got["hi"])) == (
        int(age.notna().sum()),
        int(age.min()),
        int(age.max()),
    )


def test_filtered_aggregates_match_the_oracle(client):
    age, score, city = FRAME["age"], FRAME["score"], FRAME["city"]
    got = client.execute(
        "SELECT COUNT(*) AS n, SUM(score) AS s, AVG(age) AS a FROM t WHERE city = 'Beijing'"
    ).to_dict()[0]
    beijing = FRAME[city == "Beijing"]
    assert int(got["n"]) == len(beijing)
    assert round(float(got["s"]), 6) == round(float(beijing["score"].sum()), 6)
    assert round(float(got["a"]), 6) == round(float(beijing["age"].mean()), 6)

    got = client.execute(
        "SELECT COUNT(age) AS n, SUM(age) AS s, MIN(age) AS lo, MAX(age) AS hi "
        "FROM t WHERE city <> 'Beijing'"
    ).to_dict()[0]
    other = FRAME[(city != "Beijing") & age.notna()]
    assert (int(got["n"]), int(got["s"]), int(got["lo"]), int(got["hi"])) == (
        len(other),
        int(other["age"].sum()),
        int(other["age"].min()),
        int(other["age"].max()),
    )

    got = client.execute("SELECT SUM(score * 2) AS s, AVG(abs(score - 2.5)) AS a FROM t").to_dict()[0]
    assert round(float(got["s"]), 6) == round(float((score * 2).sum()), 6)
    adjusted = score.apply(lambda value: None if pd.isna(value) else abs(value - 2.5))
    assert round(float(got["a"]), 6) == round(float(adjusted.mean()), 6)


def test_group_by_shapes_match_the_oracle(client):
    grouped = FRAME.groupby("city").agg(n=("doc", "size")).reset_index()
    assert _pairs(client, "SELECT city, COUNT(*) AS n FROM t GROUP BY city", ["city"]) == _pairs_expected(
        grouped, ["city"]
    )

    # NULL is its own group, and 0 must not be merged into it.
    by_age = FRAME.groupby("age", dropna=False).size().reset_index(name="n")
    assert _pairs(client, "SELECT age, COUNT(*) AS n FROM t GROUP BY age", ["age"]) == _pairs_expected(
        by_age, ["age"]
    )

    # A float-literal expression key: 0.0 and NULL stay distinct groups.
    expression = FRAME.assign(
        b=FRAME["score"].apply(lambda value: None if pd.isna(value) else abs(value - 2.5))
    )
    expected = expression.groupby("b", dropna=False).size().reset_index(name="n")
    got = _pairs(
        client,
        "SELECT abs(score - 2.5) AS b, COUNT(*) AS n FROM t GROUP BY abs(score - 2.5)",
        ["b"],
    )
    assert got == _pairs_expected(expected, ["b"])
    assert (0.0,) in got and (None,) in got


def test_having_and_ordered_group_aggregates_match_the_oracle(client):
    score, age, city, cat = FRAME["score"], FRAME["age"], FRAME["city"], FRAME["cat"]
    got = client.execute(
        "SELECT city FROM t GROUP BY city HAVING SUM(score) > 20000 ORDER BY city"
    ).to_dict()
    sums = FRAME.groupby("city")["score"].sum()
    assert [row["city"] for row in got] == sorted(name for name, value in sums.items() if value > 20000)

    got = client.execute(
        "SELECT cat, COUNT(*) AS n, SUM(score) AS s FROM t WHERE age IS NOT NULL "
        "GROUP BY cat HAVING COUNT(*) > 40 ORDER BY s DESC, cat"
    ).to_dict()
    filtered = FRAME[age.notna()]
    expected = filtered.groupby("cat").agg(n=("doc", "size"), s=("score", "sum")).reset_index()
    expected = expected[expected["n"] > 40].sort_values(
        ["s", "cat"], ascending=[False, True], kind="stable"
    )
    assert [(row["cat"], int(row["n"]), round(float(row["s"]), 6)) for row in got] == [
        (row["cat"], int(row["n"]), round(float(row["s"]), 6))
        for row in expected.to_dict("records")
    ]


# --------------------------------------------------------------------------
# 6. LIMIT / OFFSET on the filter lanes (any matching rows are valid, but the
#    count and membership must be right), and aggregates must ignore LIMIT.
# --------------------------------------------------------------------------
def test_filter_lane_limit_and_offset_membership(client):
    age, city, cat, doc = FRAME["age"], FRAME["city"], FRAME["cat"], FRAME["doc"]
    cases = [
        ("SELECT doc, city FROM t WHERE city = 'Beijing' LIMIT 5", FRAME[city == "Beijing"], ["doc", "city"], 5),
        (
            "SELECT doc, city FROM t WHERE city = 'Beijing' LIMIT 5 OFFSET 3",
            FRAME[city == "Beijing"],
            ["doc", "city"],
            5,
        ),
        (
            "SELECT doc, age FROM t WHERE age BETWEEN 20 AND 30 LIMIT 7 OFFSET 2",
            FRAME[(age >= 20) & (age <= 30)],
            ["doc", "age"],
            7,
        ),
        (
            "SELECT doc, city FROM t WHERE city LIKE 'B%' LIMIT 3",
            FRAME[city.str.startswith("B")],
            ["doc", "city"],
            3,
        ),
        ("SELECT doc, cat FROM t WHERE cat <> 'c3' LIMIT 4", FRAME[cat != "c3"], ["doc", "cat"], 4),
    ]
    for sql, frame, columns, expected_rows in cases:
        got = client.execute(sql).to_dict()
        assert len(got) == expected_rows, sql
        allowed = set(_norm_frame(frame[columns]))
        assert all(row in allowed for row in _norm_rows(got)), sql

    got = client.execute("SELECT COUNT(*) AS n FROM t LIMIT 5").to_dict()
    assert len(got) == 1 and int(got[0]["n"]) == len(FRAME)


# --------------------------------------------------------------------------
# 7. DISTINCT ON
# --------------------------------------------------------------------------
def test_distinct_on_matches_the_oracle(client):
    doc = FRAME["doc"]
    first = FRAME.sort_values(["city", "doc"], kind="stable").drop_duplicates(subset=["city"])
    _check(
        client,
        "SELECT DISTINCT ON (city) city, doc, score FROM t ORDER BY city, doc",
        first[["city", "doc", "score"]],
    )
    second = (
        FRAME[doc < 400]
        .sort_values(["city", "doc"], ascending=[True, False], kind="stable")
        .drop_duplicates(subset=["city"])
    )
    _check(
        client,
        "SELECT DISTINCT ON (city) city, doc FROM t WHERE doc < 400 ORDER BY city, doc DESC",
        second[["city", "doc"]],
    )


# --------------------------------------------------------------------------
# 8. window functions (order-free shapes only: their per-row value is unique)
# --------------------------------------------------------------------------
def test_window_functions_match_the_oracle(client):
    counted = FRAME.assign(c=FRAME.groupby("city")["doc"].transform("size"))
    _check(client, "SELECT doc, COUNT(*) OVER (PARTITION BY city) AS c FROM t", counted[["doc", "c"]])

    summed = FRAME.assign(s=FRAME.groupby("city")["score"].transform("sum"))
    _check(client, "SELECT doc, SUM(score) OVER (PARTITION BY city) AS s FROM t", summed[["doc", "s"]])

    scored = FRAME[FRAME["score"].notna()].copy()
    scored["rk"] = scored["score"].rank(method="min").astype("int64")
    _check(
        client,
        "SELECT doc, RANK() OVER (ORDER BY score) AS rk FROM t WHERE score IS NOT NULL",
        scored[["doc", "rk"]],
    )
