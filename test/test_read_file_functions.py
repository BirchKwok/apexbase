"""Tests for SQL table functions: read_csv, read_parquet, read_json."""
import csv
import json
import os
import shutil
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

try:
    from apexbase import ApexClient
except ImportError as e:
    pytest.skip(f"ApexBase not available: {e}", allow_module_level=True)


# ---- helpers ---------------------------------------------------------------

ROWS = [
    {'name': 'Alice', 'age': 30, 'score': 88.5, 'city': 'Beijing'},
    {'name': 'Bob',   'age': 25, 'score': 72.0, 'city': 'Shanghai'},
    {'name': 'Carol', 'age': 35, 'score': 95.0, 'city': 'Beijing'},
    {'name': 'Dave',  'age': 28, 'score': 60.0, 'city': 'Guangzhou'},
    {'name': 'Eve',   'age': 22, 'score': 83.0, 'city': 'Shanghai'},
]


def _write_csv(path, rows, header=True, delimiter=','):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f, delimiter=delimiter)
        if header and rows:
            w.writerow(rows[0].keys())
        for r in rows:
            w.writerow(r.values())


def _write_ndjson(path, rows):
    with open(path, 'w', encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r) + '\n')


def _write_parquet(path, rows):
    pa = pytest.importorskip('pyarrow')
    pq = pytest.importorskip('pyarrow.parquet')
    tbl = pa.table({
        'name':  [r['name']  for r in rows],
        'age':   pa.array([r['age']   for r in rows], type=pa.int64()),
        'score': pa.array([r['score'] for r in rows], type=pa.float64()),
        'city':  [r['city']  for r in rows],
    })
    pq.write_table(tbl, path)


# ============================================================
# read_csv
# ============================================================

class TestReadCsv:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.csv   = os.path.join(self.d, 'data.csv')
        self.tsv   = os.path.join(self.d, 'data.tsv')
        self.nohead = os.path.join(self.d, 'nohead.csv')
        _write_csv(self.csv,    ROWS)
        _write_csv(self.tsv,    ROWS, delimiter='\t')
        _write_csv(self.nohead, ROWS, header=False)

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_row_count(self):
        rv = self.c.execute(f"SELECT * FROM read_csv('{self.csv}')")
        assert len(rv) == 5

    def test_columns_present(self):
        rv = self.c.execute(f"SELECT * FROM read_csv('{self.csv}')")
        assert {'name', 'age', 'score', 'city'}.issubset(set(rv.columns))

    def test_tsv_delimiter_option(self):
        rv = self.c.execute(f"SELECT * FROM read_csv('{self.tsv}', delimiter='\\t')")
        assert len(rv) == 5

    def test_delim_alias(self):
        rv = self.c.execute(f"SELECT * FROM read_csv('{self.tsv}', delim='\\t')")
        assert len(rv) == 5

    def test_sep_alias(self):
        rv = self.c.execute(f"SELECT * FROM read_csv('{self.tsv}', sep='\\t')")
        assert len(rv) == 5

    def test_no_header(self):
        rv = self.c.execute(f"SELECT * FROM read_csv('{self.nohead}', header=false)")
        assert len(rv) == 5

    def test_where_filter(self):
        rv = self.c.execute(f"SELECT name FROM read_csv('{self.csv}') WHERE city='Beijing' ORDER BY name")
        assert [r['name'] for r in rv] == ['Alice', 'Carol']

    def test_count_star(self):
        rv = self.c.execute(f"SELECT COUNT(*) AS cnt FROM read_csv('{self.csv}')")
        assert rv.first()['cnt'] == 5

    def test_count_star_direct_file(self):
        # DuckDB-style direct file path as the FROM source.
        rv = self.c.execute(f"SELECT COUNT(*) AS cnt FROM '{self.csv}'")
        assert rv.first()['cnt'] == 5

    def test_count_star_direct_file_with_limit_noop(self):
        # LIMIT (>=1) with no OFFSET is a no-op on the single-row COUNT result.
        rv = self.c.execute(f"SELECT COUNT(1) AS cnt FROM '{self.csv}' LIMIT 100")
        assert rv.first()['cnt'] == 5

    def test_scalar_numeric_aggregate_direct_file_with_spaced_header(self):
        # Network-flow CSV exports commonly put a space after delimiters.  The
        # direct-file aggregate path must normalize the header and avoid
        # materializing the unrelated wide columns.
        path = os.path.join(self.d, 'flow.csv')
        with open(path, 'w', encoding='utf-8') as f:
            f.write('id, Protocol , score\n1,6,10.5\n2,17,20.25\n3,,5.0\n')

        result = self.c.execute(
            f"SELECT MAX(Protocol) AS cnt FROM '{path}' LIMIT 100"
        )
        assert result.first()['cnt'] == 17

        aggregates = self.c.execute(
            f"SELECT COUNT(*), SUM(Protocol), AVG(score), MIN(Protocol), MAX(score) "
            f"FROM read_csv('{path}')"
        ).first()
        assert aggregates['COUNT(*)'] == 3
        assert aggregates['SUM(Protocol)'] == 23
        assert abs(aggregates['AVG(score)'] - (10.5 + 20.25 + 5.0) / 3) < 1e-6
        assert aggregates['MIN(Protocol)'] == 6
        assert abs(aggregates['MAX(score)'] - 20.25) < 1e-6

    def test_filtered_numeric_aggregate_direct_file(self):
        path = os.path.join(self.d, 'filtered_flow.csv')
        with open(path, 'w', encoding='utf-8') as f:
            f.write(
                'id, Protocol , duration, bytes\n'
                '1,6,10,100\n'
                '2,17,20,200\n'
                '3,6,30,300\n'
                '4,6,,400\n'
            )

        result = self.c.execute(
            f"SELECT COUNT(*) AS n, SUM(bytes) AS total, AVG(duration) AS av "
            f"FROM '{path}' WHERE Protocol = 6 AND duration >= 15"
        ).first()
        assert result == {'n': 1, 'total': 300, 'av': 30.0}

    def test_count_star_direct_file_tsv(self):
        rv = self.c.execute(f"SELECT COUNT(*) AS cnt FROM '{self.tsv}'")
        assert rv.first()['cnt'] == 5

    def test_count_star_direct_file_skips_when_where(self):
        # A WHERE clause disables the fast count; full parse handles it.
        rv = self.c.execute(f"SELECT COUNT(*) AS cnt FROM '{self.csv}' WHERE city='Beijing'")
        assert rv.first()['cnt'] == 2

    def test_group_by(self):
        rv = self.c.execute(
            f"SELECT city, COUNT(*) AS cnt FROM read_csv('{self.csv}') GROUP BY city ORDER BY city"
        )
        m = {r['city']: r['cnt'] for r in rv}
        assert m == {'Beijing': 2, 'Shanghai': 2, 'Guangzhou': 1}

    def test_group_by_string_with_numeric_aggregates_direct_file(self):
        rows = self.c.execute(
            f"SELECT city, COUNT(*) AS n, SUM(age) AS total, AVG(score) AS av, "
            f"MIN(age) AS lo, MAX(score) AS hi FROM '{self.csv}' "
            f"GROUP BY city ORDER BY n DESC, city"
        ).to_dict()
        expected = {
            'Beijing': (2, 65, 91.75, 30, 95.0),
            'Shanghai': (2, 47, 77.5, 22, 83.0),
            'Guangzhou': (1, 28, 60.0, 28, 60.0),
        }
        for row in rows:
            assert (
                row['n'], row['total'], row['av'], row['lo'], row['hi']
            ) == expected[row['city']]

    def test_filtered_group_by_string_with_numeric_aggregates_direct_file(self):
        rows = self.c.execute(
            f"SELECT city, COUNT(*) AS n, AVG(score) AS av, MAX(age) AS hi "
            f"FROM '{self.csv}' WHERE age >= 25 AND score < 90 "
            f"GROUP BY city HAVING COUNT(*) > 0 ORDER BY n DESC, city"
        ).to_dict()
        assert rows == [
            {'city': 'Beijing', 'n': 1, 'av': 88.5, 'hi': 30},
            {'city': 'Guangzhou', 'n': 1, 'av': 60.0, 'hi': 28},
            {'city': 'Shanghai', 'n': 1, 'av': 72.0, 'hi': 25},
        ]
        implicit = self.c.execute(
            f"SELECT city, AVG(score) AS av FROM '{self.csv}' "
            f"GROUP BY city HAVING COUNT(*) > 1 ORDER BY city"
        ).to_dict()
        assert implicit == [
            {'city': 'Beijing', 'av': 91.75},
            {'city': 'Shanghai', 'av': 77.5},
        ]

    def test_integer_group_by_with_numeric_aggregate_direct_file(self):
        rows = self.c.execute(
            f"SELECT age, COUNT(*) AS n, AVG(score) AS av FROM '{self.csv}' "
            f"GROUP BY age ORDER BY age"
        ).to_dict()
        assert rows == [
            {'age': 22, 'n': 1, 'av': 83.0},
            {'age': 25, 'n': 1, 'av': 72.0},
            {'age': 28, 'n': 1, 'av': 60.0},
            {'age': 30, 'n': 1, 'av': 88.5},
            {'age': 35, 'n': 1, 'av': 95.0},
        ]

    def test_multi_count_distinct_direct_file(self):
        row = self.c.execute(
            f"SELECT COUNT(DISTINCT age) AS ages, "
            f"COUNT(DISTINCT city) AS cities FROM '{self.csv}'"
        ).first()
        assert row == {'ages': 5, 'cities': 3}

    def test_order_by_limit(self):
        rv = self.c.execute(f"SELECT age FROM read_csv('{self.csv}') ORDER BY age DESC LIMIT 3")
        ages = [r['age'] for r in rv]
        assert ages == sorted(ages, reverse=True)
        assert len(ages) == 3

    def test_projection(self):
        rv = self.c.execute(f"SELECT name, city FROM read_csv('{self.csv}')")
        assert rv.columns == ['name', 'city']

    def test_type_inference_int(self):
        rv = self.c.execute(f"SELECT age FROM read_csv('{self.csv}') WHERE name='Alice'")
        assert rv.first()['age'] == 30

    def test_type_inference_float(self):
        rv = self.c.execute(f"SELECT score FROM read_csv('{self.csv}') WHERE name='Alice'")
        assert abs(rv.first()['score'] - 88.5) < 1e-6

    def test_avg_aggregation(self):
        rv = self.c.execute(f"SELECT AVG(score) AS avg_score FROM read_csv('{self.csv}')")
        expected = sum(r['score'] for r in ROWS) / len(ROWS)
        assert abs(rv.scalar() - expected) < 1e-4

    def test_having(self):
        rv = self.c.execute(
            f"SELECT city FROM read_csv('{self.csv}') GROUP BY city HAVING COUNT(*) > 1 ORDER BY city"
        )
        cities = [r['city'] for r in rv]
        assert cities == ['Beijing', 'Shanghai']

    def test_to_pandas(self):
        pytest.importorskip('pandas')
        df = self.c.execute(f"SELECT * FROM read_csv('{self.csv}')").to_pandas()
        assert len(df) == 5

    def test_to_arrow(self):
        pytest.importorskip('pyarrow')
        tbl = self.c.execute(f"SELECT * FROM read_csv('{self.csv}')").to_arrow()
        assert tbl.num_rows == 5

    def test_nonexistent_file_raises(self):
        with pytest.raises(Exception):
            self.c.execute(f"SELECT * FROM read_csv('{self.d}/no_such_file.csv')")

    def test_bad_lines_default_error_and_skip(self):
        dirty = os.path.join(self.d, 'dirty.csv')
        with open(dirty, 'w', encoding='utf-8') as f:
            f.write("name,age\nAlice,30\nbroken,40,extra\nBob,25\n")

        with pytest.raises(Exception, match="fields; expected"):
            self.c.execute(f"SELECT * FROM read_csv('{dirty}')")

        rows = self.c.execute(
            f"SELECT name, age FROM read_csv('{dirty}', on_bad_lines='skip') ORDER BY name"
        ).to_dict()
        assert rows == [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]

        warned = self.c.execute(
            f"SELECT name FROM read_csv('{dirty}', on_bad_lines='warn') ORDER BY name"
        ).to_dict()
        assert [row['name'] for row in warned] == ['Alice', 'Bob']

    def test_expression_key_join_uses_replace(self):
        styles = os.path.join(self.d, 'styles.csv')
        images = os.path.join(self.d, 'images.csv')
        _write_csv(styles, [
            {'id': 1001, 'product': 'shirt'},
            {'id': 1002, 'product': 'shoes'},
            {'id': 9999, 'product': 'missing'},
        ])
        _write_csv(images, [
            {'filename': '1002.jpg'},
            {'filename': '1001.jpg'},
        ])
        rows = self.c.execute(f"""
            SELECT CAST(s.id AS INT) AS product_id, i.filename
            FROM read_csv('{styles}') s
            JOIN read_csv('{images}') i
              ON CAST(s.id AS TEXT) = REPLACE(i.filename, '.jpg', '')
            ORDER BY product_id
        """).to_dict()
        assert rows == [
            {'product_id': 1001, 'filename': '1001.jpg'},
            {'product_id': 1002, 'filename': '1002.jpg'},
        ]


# ============================================================
# read_parquet
# ============================================================

class TestReadParquet:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.pq = os.path.join(self.d, 'data.parquet')
        _write_parquet(self.pq, ROWS)

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_row_count(self):
        rv = self.c.execute(f"SELECT * FROM read_parquet('{self.pq}')")
        assert len(rv) == 5

    def test_columns_present(self):
        rv = self.c.execute(f"SELECT * FROM read_parquet('{self.pq}')")
        assert {'name', 'age', 'score', 'city'}.issubset(set(rv.columns))

    def test_where_filter(self):
        rv = self.c.execute(
            f"SELECT name FROM read_parquet('{self.pq}') WHERE city='Beijing' ORDER BY name"
        )
        assert [r['name'] for r in rv] == ['Alice', 'Carol']

    def test_count_star(self):
        rv = self.c.execute(f"SELECT COUNT(*) AS cnt FROM read_parquet('{self.pq}')")
        assert rv.first()['cnt'] == 5

    def test_group_by(self):
        rv = self.c.execute(
            f"SELECT city, COUNT(*) AS cnt FROM read_parquet('{self.pq}') GROUP BY city ORDER BY city"
        )
        m = {r['city']: r['cnt'] for r in rv}
        assert m == {'Beijing': 2, 'Shanghai': 2, 'Guangzhou': 1}

    def test_order_by_limit(self):
        rv = self.c.execute(f"SELECT name FROM read_parquet('{self.pq}') ORDER BY age ASC LIMIT 2")
        assert len(rv) == 2

    def test_projection(self):
        rv = self.c.execute(f"SELECT name, city FROM read_parquet('{self.pq}')")
        assert rv.columns == ['name', 'city']

    def test_avg_aggregation(self):
        rv = self.c.execute(f"SELECT AVG(score) AS avg_score FROM read_parquet('{self.pq}')")
        expected = sum(r['score'] for r in ROWS) / len(ROWS)
        assert abs(rv.scalar() - expected) < 1e-4

    def test_to_pandas(self):
        pytest.importorskip('pandas')
        df = self.c.execute(f"SELECT * FROM read_parquet('{self.pq}')").to_pandas()
        assert len(df) == 5

    def test_to_arrow(self):
        pytest.importorskip('pyarrow')
        tbl = self.c.execute(f"SELECT * FROM read_parquet('{self.pq}')").to_arrow()
        assert tbl.num_rows == 5

    def test_nonexistent_file_raises(self):
        with pytest.raises(Exception):
            self.c.execute(f"SELECT * FROM read_parquet('{self.d}/no_such.parquet')")

    def test_numeric_filter_projection_pushdown_crosses_batches(self):
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        rows = 70_000
        values = list(range(rows))
        scores = [
            None if i in (0, 65_536) else (i % 1_000) / 10 for i in range(rows)
        ]
        path = os.path.join(self.d, "pushdown.parquet")
        pq.write_table(
            pa.table(
                {
                    "value": pa.array(values, type=pa.int64()),
                    "score": pa.array(scores, type=pa.float64()),
                    "category": pa.array([f"group_{i % 8}" for i in values]),
                    "unused_payload": pa.array([f"payload_{i:08d}" for i in values]),
                }
            ),
            path,
            row_group_size=10_000,
        )

        expected = sum(score is not None and score >= 50 for score in scores)
        count = self.c.execute(
            f"SELECT COUNT(*) AS n FROM read_parquet('{path}') WHERE score >= 50"
        ).scalar()
        assert count == expected

        grouped = self.c.execute(
            f"SELECT category, COUNT(*) AS n, AVG(score) AS avg_score "
            f"FROM read_parquet('{path}') WHERE score >= 50 "
            "GROUP BY category ORDER BY category"
        ).to_dict()
        assert len(grouped) == 8
        assert sum(row["n"] for row in grouped) == expected

        projected = self.c.execute(
            f"SELECT value FROM read_parquet('{path}') "
            "WHERE score = 99.9 ORDER BY value LIMIT 5"
        )
        assert projected.columns == ["value"]
        assert [row["value"] for row in projected] == [999, 1999, 2999, 3999, 4999]

        value_base = 9_007_199_254_740_992
        precision_path = os.path.join(self.d, "int64_precision.parquet")
        pq.write_table(
            pa.table(
                {
                    "value": pa.array(
                        [value_base, value_base + 1, value_base + 2], type=pa.int64()
                    )
                }
            ),
            precision_path,
        )
        assert (
            self.c.execute(
                f"SELECT COUNT(*) FROM read_parquet('{precision_path}') "
                f"WHERE value = {value_base + 1}"
            ).scalar()
            == 1
        )

    def test_parquet_count_avoids_python_arrow_materialization(self, monkeypatch):
        import apexbase.client as client_module

        monkeypatch.setattr(
            client_module,
            "_ensure_pyarrow",
            lambda: (_ for _ in ()).throw(AssertionError("unexpected PyArrow import")),
        )

        assert self.c.execute(
            f"SELECT COUNT(*) FROM read_parquet('{self.pq}') WHERE score >= 85"
        ).scalar() == 2


# ============================================================
# read_json
# ============================================================

class TestReadJson:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.ndjson = os.path.join(self.d, 'data.json')
        _write_ndjson(self.ndjson, ROWS)

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_row_count(self):
        rv = self.c.execute(f"SELECT * FROM read_json('{self.ndjson}')")
        assert len(rv) == 5

    def test_columns_present(self):
        rv = self.c.execute(f"SELECT * FROM read_json('{self.ndjson}')")
        assert {'name', 'age', 'score', 'city'}.issubset(set(rv.columns))

    def test_where_filter(self):
        rv = self.c.execute(
            f"SELECT name FROM read_json('{self.ndjson}') WHERE city='Shanghai' ORDER BY name"
        )
        assert [r['name'] for r in rv] == ['Bob', 'Eve']

    def test_count_star(self):
        rv = self.c.execute(f"SELECT COUNT(*) AS cnt FROM read_json('{self.ndjson}')")
        assert rv.first()['cnt'] == 5

    def test_count_star_numeric_filter(self):
        rv = self.c.execute(f"SELECT COUNT(*) AS cnt FROM read_json('{self.ndjson}') WHERE age > 30")
        assert rv.first()['cnt'] == 1

    def test_group_by(self):
        rv = self.c.execute(
            f"SELECT city, COUNT(*) AS cnt FROM read_json('{self.ndjson}') GROUP BY city ORDER BY city"
        )
        m = {r['city']: r['cnt'] for r in rv}
        assert m == {'Beijing': 2, 'Shanghai': 2, 'Guangzhou': 1}

    def test_order_by_limit(self):
        rv = self.c.execute(
            f"SELECT name FROM read_json('{self.ndjson}') ORDER BY age ASC LIMIT 2"
        )
        assert len(rv) == 2

    def test_type_inference_int(self):
        rv = self.c.execute(f"SELECT age FROM read_json('{self.ndjson}') WHERE name='Alice'")
        assert rv.first()['age'] == 30

    def test_type_inference_float(self):
        rv = self.c.execute(f"SELECT score FROM read_json('{self.ndjson}') WHERE name='Alice'")
        assert abs(rv.first()['score'] - 88.5) < 1e-6

    def test_to_pandas(self):
        pytest.importorskip('pandas')
        df = self.c.execute(f"SELECT * FROM read_json('{self.ndjson}')").to_pandas()
        assert len(df) == 5

    def test_to_arrow(self):
        pytest.importorskip('pyarrow')
        tbl = self.c.execute(f"SELECT * FROM read_json('{self.ndjson}')").to_arrow()
        assert tbl.num_rows == 5


# ============================================================
# Combined: file reads with stored tables
# ============================================================

class TestReadFileCombined:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.c.execute("CREATE TABLE users (name STRING, age INT, city STRING)")
        self.c.execute(
            "INSERT INTO users (name, age, city) VALUES "
            "('Alice', 30, 'Beijing'), ('Bob', 25, 'Shanghai')"
        )
        self.extra_csv = os.path.join(self.d, 'extra.csv')
        _write_csv(self.extra_csv, [
            {'name': 'Frank', 'age': 31, 'city': 'Beijing'},
            {'name': 'Grace', 'age': 27, 'city': 'Shanghai'},
        ])

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_union_all_csv_with_table(self):
        rv = self.c.execute(f"""
            SELECT name FROM users
            UNION ALL
            SELECT name FROM read_csv('{self.extra_csv}')
            ORDER BY name
        """)
        assert len(rv) == 4
        assert [r['name'] for r in rv] == ['Alice', 'Bob', 'Frank', 'Grace']

    def test_union_dedup_csv_with_table(self):
        overlap = os.path.join(self.d, 'overlap.csv')
        _write_csv(overlap, [
            {'name': 'Alice'},
            {'name': 'Zara'},
        ])
        rv = self.c.execute(f"""
            SELECT name FROM users
            UNION
            SELECT name FROM read_csv('{overlap}')
            ORDER BY name
        """)
        names = [r['name'] for r in rv]
        assert names.count('Alice') == 1
        assert 'Zara' in names
        assert len(names) == 3

    def test_except_csv_as_blocklist(self):
        block = os.path.join(self.d, 'block.csv')
        _write_csv(block, [{'name': 'Bob'}])
        rv = self.c.execute(f"""
            SELECT name FROM users
            EXCEPT
            SELECT name FROM read_csv('{block}')
            ORDER BY name
        """)
        assert [r['name'] for r in rv] == ['Alice']

    def test_intersect_csv_with_table(self):
        common = os.path.join(self.d, 'common.csv')
        _write_csv(common, [{'name': 'Alice'}, {'name': 'Zara'}])
        rv = self.c.execute(f"""
            SELECT name FROM users
            INTERSECT
            SELECT name FROM read_csv('{common}')
            ORDER BY name
        """)
        assert [r['name'] for r in rv] == ['Alice']

    def test_join_csv_with_stored_table(self):
        scores = os.path.join(self.d, 'scores.csv')
        _write_csv(scores, [
            {'name': 'Alice', 'score': 99},
            {'name': 'Bob',   'score': 55},
        ])
        rv = self.c.execute(f"""
            SELECT u.name, s.score
            FROM users u
            JOIN read_csv('{scores}') s ON u.name = s.name
            ORDER BY u.name
        """)
        assert len(rv) == 2
        assert rv.first()['name'] == 'Alice'
        assert rv.first()['score'] == 99

    def test_where_on_join_result(self):
        scores = os.path.join(self.d, 'scores2.csv')
        _write_csv(scores, [
            {'name': 'Alice', 'score': 99},
            {'name': 'Bob',   'score': 55},
        ])
        rv = self.c.execute(f"""
            SELECT u.name
            FROM users u
            JOIN read_csv('{scores}') s ON u.name = s.name
            WHERE s.score >= 80
        """)
        assert len(rv) == 1
        assert rv.first()['name'] == 'Alice'

    def test_csv_parquet_union(self):
        pq = os.path.join(self.d, 'extra.parquet')
        _write_parquet(pq, [
            {'name': 'Hank', 'age': 40, 'score': 78.0, 'city': 'Chengdu'},
        ])
        csv2 = os.path.join(self.d, 'one.csv')
        _write_csv(csv2, [{'name': 'Ivy'}])
        rv = self.c.execute(f"""
            SELECT name FROM read_parquet('{pq}')
            UNION ALL
            SELECT name FROM read_csv('{csv2}')
            ORDER BY name
        """)
        assert [r['name'] for r in rv] == ['Hank', 'Ivy']


# ============================================================
# DuckDB-style direct file reading: SELECT * FROM 'path' — on-demand LIMIT
# ============================================================

class TestDirectFileRead:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.csv = os.path.join(self.d, 'data.csv')
        self.tsv = os.path.join(self.d, 'data.tsv')
        self.comma_txt = os.path.join(self.d, 'comma.txt')
        self.tab_txt = os.path.join(self.d, 'tab.txt')
        _write_csv(self.csv, ROWS)
        _write_csv(self.tsv, ROWS, delimiter='\t')
        _write_csv(self.comma_txt, ROWS, delimiter=',')
        _write_csv(self.tab_txt, ROWS, delimiter='\t')

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_csv_limit_1(self):
        rv = self.c.execute(f"SELECT * FROM '{self.csv}' LIMIT 1")
        assert len(rv) == 1
        assert rv.first()['name'] == 'Alice'

    def test_csv_limit_1_to_pandas(self):
        # Mirrors the reported use case: `select * from 'file.csv' limit 1` → pandas.
        pytest.importorskip('pandas')
        df = self.c.execute(f"SELECT * FROM '{self.csv}' LIMIT 1").to_pandas()
        assert len(df) == 1
        assert df.iloc[0]['name'] == 'Alice'

    def test_csv_limit_offset(self):
        rv = self.c.execute(f"SELECT * FROM '{self.csv}' LIMIT 2 OFFSET 1").to_dict()
        assert [r['name'] for r in rv] == ['Bob', 'Carol']

    def test_csv_limit_does_not_parse_tail(self):
        # A malformed row later in the file errors on a full read, but a LIMIT 1
        # only reads the first data row — proving the tail is never parsed.
        path = os.path.join(self.d, 'dirty_tail.csv')
        with open(path, 'w', encoding='utf-8') as f:
            f.write("name,age\nAlice,30\nbroken,40,extra\nBob,25\n")
        with pytest.raises(Exception, match="fields; expected"):
            self.c.execute(f"SELECT * FROM '{path}'")
        rows = self.c.execute(f"SELECT * FROM '{path}' LIMIT 1").to_dict()
        assert rows == [{'name': 'Alice', 'age': 30}]

    def test_tsv_limit_1(self):
        rv = self.c.execute(f"SELECT * FROM '{self.tsv}' LIMIT 1")
        assert len(rv) == 1
        assert rv.first()['name'] == 'Alice'

    def test_txt_comma_sniff_limit_1(self):
        rv = self.c.execute(f"SELECT * FROM '{self.comma_txt}' LIMIT 1")
        assert len(rv) == 1
        assert rv.first()['name'] == 'Alice'

    def test_txt_tab_sniff_limit_1(self):
        rv = self.c.execute(f"SELECT * FROM '{self.tab_txt}' LIMIT 1")
        assert len(rv) == 1
        assert rv.first()['name'] == 'Alice'

    def test_projection_limit_1(self):
        rv = self.c.execute(f"SELECT name, city FROM '{self.csv}' LIMIT 1").to_dict()
        assert rv == [{'name': 'Alice', 'city': 'Beijing'}]

    def test_limit_0(self):
        rv = self.c.execute(f"SELECT * FROM '{self.csv}' LIMIT 0")
        assert len(rv) == 0
        assert {'name', 'age', 'score', 'city'}.issubset(set(rv.columns))


class TestDirectFileReadParquet:
    def setup_method(self):
        self.d = tempfile.mkdtemp()
        self.c = ApexClient(dirpath=self.d)
        self.pq = os.path.join(self.d, 'data.parquet')
        _write_parquet(self.pq, ROWS)

    def teardown_method(self):
        self.c.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def test_parquet_limit_1(self):
        rv = self.c.execute(f"SELECT * FROM '{self.pq}' LIMIT 1")
        assert len(rv) == 1
        assert rv.first()['name'] == 'Alice'

    def test_parquet_limit_offset(self):
        rv = self.c.execute(f"SELECT * FROM '{self.pq}' LIMIT 2 OFFSET 1").to_dict()
        assert [r['name'] for r in rv] == ['Bob', 'Carol']

    def test_parquet_multi_row_group_limit_1(self):
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        rows = 20_000
        path = os.path.join(self.d, 'many_groups.parquet')
        pq.write_table(
            pa.table({
                'id': pa.array(range(rows), type=pa.int64()),
                'payload': pa.array([f'p_{i}' for i in range(rows)]),
            }),
            path,
            row_group_size=1000,
        )
        rv = self.c.execute(f"SELECT * FROM '{path}' LIMIT 3").to_dict()
        assert [r['id'] for r in rv] == [0, 1, 2]
