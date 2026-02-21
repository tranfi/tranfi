"""
test_duckdb.py — Tests for the DuckDB engine and SQL transpiler.

Run: python -m pytest test/test_duckdb.py -v
(Requires libtranfi.so in build/ and duckdb installed)
"""

import os
import sys
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'py'))

build_dir = os.path.join(os.path.dirname(__file__), '..', 'build')
lib_path = os.path.join(build_dir, 'libtranfi.so')
if os.path.exists(lib_path):
    os.environ['TRANFI_LIB_PATH'] = lib_path

import tranfi as tf

duckdb = pytest.importorskip('duckdb')

CSV_DATA = b'name,age,score\nAlice,30,85\nBob,20,92\nCharlie,35,78\nDiana,22,95\nEve,28,88\n'


class TestCompileToSql:
    def test_basic(self):
        sql = tf.compile_to_sql('csv | head 10 | csv')
        assert 'LIMIT 10' in sql

    def test_filter(self):
        sql = tf.compile_to_sql('csv | filter "col(\'age\') > 25" | csv')
        assert 'WHERE' in sql
        assert '"age"' in sql

    def test_select(self):
        sql = tf.compile_to_sql('csv | select name,age | csv')
        assert '"name"' in sql
        assert '"age"' in sql

    def test_sort(self):
        sql = tf.compile_to_sql('csv | sort age | csv')
        assert 'ORDER BY' in sql

    def test_derive(self):
        sql = tf.compile_to_sql("csv | derive total=col('a')+col('b') | csv")
        assert '"total"' in sql

    def test_invalid_dsl(self):
        with pytest.raises(RuntimeError):
            tf.compile_to_sql('not a valid pipeline')


class TestDuckDBEngine:
    def test_passthrough(self):
        r = tf.pipeline('csv | csv', engine='duckdb').run(input=CSV_DATA)
        lines = r.output_text.strip().split('\n')
        assert lines[0] == 'name,age,score'
        assert len(lines) == 6  # header + 5 rows

    def test_filter(self):
        r = tf.pipeline('csv | filter "col(\'age\') > 25" | csv', engine='duckdb').run(input=CSV_DATA)
        text = r.output_text
        assert 'Alice' in text
        assert 'Charlie' in text
        assert 'Eve' in text
        assert 'Bob' not in text
        assert 'Diana' not in text

    def test_select(self):
        r = tf.pipeline('csv | select name,age | csv', engine='duckdb').run(input=CSV_DATA)
        lines = r.output_text.strip().split('\n')
        assert lines[0] == 'name,age'
        assert 'score' not in r.output_text

    def test_head(self):
        r = tf.pipeline('csv | head 3 | csv', engine='duckdb').run(input=CSV_DATA)
        lines = r.output_text.strip().split('\n')
        assert len(lines) == 4  # header + 3 rows

    def test_sort(self):
        r = tf.pipeline('csv | sort age | csv', engine='duckdb').run(input=CSV_DATA)
        lines = r.output_text.strip().split('\n')
        # First data row should be youngest (Bob, 20)
        assert 'Bob' in lines[1]

    def test_derive(self):
        r = tf.pipeline("csv | derive age_x2=col('age')*2 | select name,age_x2 | csv", engine='duckdb').run(input=CSV_DATA)
        text = r.output_text
        assert 'age_x2' in text
        assert '60' in text  # Alice: 30*2

    def test_unique(self):
        data = b'name,city\nAlice,NYC\nBob,LA\nCharlie,NYC\nDiana,LA\n'
        r = tf.pipeline('csv | unique city | csv', engine='duckdb').run(input=data)
        lines = r.output_text.strip().split('\n')
        assert len(lines) == 3  # header + 2 unique cities

    def test_rename(self):
        r = tf.pipeline('csv | rename age=years | csv', engine='duckdb').run(input=CSV_DATA)
        text = r.output_text
        assert 'years' in text
        assert 'age' not in text.split('\n')[0]

    def test_skip(self):
        r = tf.pipeline('csv | skip 2 | csv', engine='duckdb').run(input=CSV_DATA)
        lines = r.output_text.strip().split('\n')
        assert len(lines) == 4  # header + 3 rows

    def test_group_agg(self):
        data = b'city,sales\nNYC,100\nLA,200\nNYC,150\nLA,300\n'
        r = tf.pipeline('csv | group-agg city sales:sum:total | csv', engine='duckdb').run(input=data)
        text = r.output_text
        assert 'city' in text
        assert 'total' in text

    def test_file_input(self):
        fd, tmp = tempfile.mkstemp(suffix='.csv')
        os.write(fd, CSV_DATA)
        os.close(fd)
        try:
            r = tf.pipeline('csv | head 2 | csv', engine='duckdb').run(input_file=tmp)
            lines = r.output_text.strip().split('\n')
            assert len(lines) == 3  # header + 2 rows
        finally:
            os.unlink(tmp)

    def test_parity_filter_select(self):
        """Verify DuckDB and native engines produce same results."""
        dsl = 'csv | filter "col(\'age\') > 25" | select name,age | csv'
        native = tf.pipeline(dsl).run(input=CSV_DATA)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=CSV_DATA)

        native_rows = set(native.output_text.strip().split('\n')[1:])
        duck_rows = set(duck.output_text.strip().split('\n')[1:])
        assert native_rows == duck_rows
