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

    def test_group_by(self):
        rv = self.c.execute(
            f"SELECT city, COUNT(*) AS cnt FROM read_csv('{self.csv}') GROUP BY city ORDER BY city"
        )
        m = {r['city']: r['cnt'] for r in rv}
        assert m == {'Beijing': 2, 'Shanghai': 2, 'Guangzhou': 1}

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
