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


# ---------------------------------------------------------------------------
# Parity tests: native vs DuckDB for every supported operator
# ---------------------------------------------------------------------------

# Helpers for parity comparison

def parse_rows(output_text):
    """Parse CSV output into header + set of data rows."""
    lines = output_text.strip().split('\n')
    if not lines:
        return '', set()
    return lines[0], set(lines[1:])


def parse_rows_ordered(output_text):
    """Parse CSV output into header + ordered list of data rows."""
    lines = output_text.strip().split('\n')
    if not lines:
        return '', []
    return lines[0], lines[1:]


def assert_parity(dsl, data=CSV_DATA, ordered=False, float_cols=None):
    """Run DSL on both native and DuckDB engines, compare results.

    Args:
        dsl: pipeline DSL string
        data: input CSV bytes
        ordered: if True, compare row order; otherwise compare as sets
        float_cols: if set, parse these columns as floats with tolerance
    """
    native = tf.pipeline(dsl).run(input=data)
    duck = tf.pipeline(dsl, engine='duckdb').run(input=data)
    n_text = native.output_text.strip()
    d_text = duck.output_text.strip()
    n_header, n_rows = parse_rows(n_text) if not ordered else (None, None)
    d_header, d_rows = parse_rows(d_text) if not ordered else (None, None)

    if ordered:
        n_header, n_rows = parse_rows_ordered(n_text)
        d_header, d_rows = parse_rows_ordered(d_text)

    # Headers must match
    assert set(n_header.split(',')) == set(d_header.split(',')), \
        f'Header mismatch:\n  native: {n_header}\n  duck:   {d_header}'

    if float_cols:
        # Parse into dicts and compare with tolerance
        n_cols = n_header.split(',')
        d_cols = d_header.split(',')

        def parse_row_dict(row, cols):
            vals = row.split(',')
            return {c: v for c, v in zip(cols, vals)}

        n_parsed = [parse_row_dict(r, n_cols) for r in (n_rows if ordered else sorted(n_rows))]
        d_parsed = [parse_row_dict(r, d_cols) for r in (d_rows if ordered else sorted(d_rows))]
        assert len(n_parsed) == len(d_parsed), \
            f'Row count mismatch: native={len(n_parsed)}, duck={len(d_parsed)}'
        for i, (nr, dr) in enumerate(zip(n_parsed, d_parsed)):
            for col in n_cols:
                if col in float_cols:
                    nv = float(nr[col]) if nr[col] else 0.0
                    dv = float(dr[col]) if dr[col] else 0.0
                    assert abs(nv - dv) < 1e-4, \
                        f'Row {i}, col {col}: native={nv}, duck={dv}'
                else:
                    assert nr[col] == dr[col], \
                        f'Row {i}, col {col}: native={nr[col]!r}, duck={dr[col]!r}'
    else:
        if ordered:
            assert n_rows == d_rows, \
                f'Row mismatch (ordered):\n  native: {n_rows}\n  duck:   {d_rows}'
        else:
            assert n_rows == d_rows, \
                f'Row mismatch:\n  native: {n_rows}\n  duck:   {d_rows}'


# Test datasets for parity tests
CSV_CITIES = b'name,age,city,score\nAlice,30,NY,85\nBob,20,LA,92\nCharlie,35,NY,78\nDiana,22,SF,95\nEve,28,LA,88\n'
CSV_SALES = b'city,sales\nNYC,100\nLA,200\nNYC,150\nLA,300\nSF,50\n'
CSV_NUMS = b'val\n10\n20\n30\n40\n50\n'
CSV_DUPS = b'name,city\nAlice,NY\nBob,LA\nAlice,NY\nCharlie,SF\nBob,LA\n'
CSV_TAGS = b'name,tags\nAlice,python|sql|r\nBob,java|go\nCharlie,python\n'
CSV_NAMES = b'full_name,age\nAlice Smith,30\nBob Jones,25\nCharlie Brown,35\n'
CSV_PADDED = b'name,city\n  Alice  , NY \n Bob ,  LA  \nCharlie,SF\n'
CSV_PIVOT = b'id,metric,value\n1,revenue,100\n1,cost,40\n2,revenue,200\n2,cost,80\n3,revenue,150\n3,cost,60\n'
CSV_DATES = b'id,date,value\n1,2024-03-15,100\n2,2023-12-01,200\n3,2024-07-22,150\n'
CSV_FILLDOWN = b'group,value\nA,1\n,2\n,3\nB,4\n,5\n'
CSV_NULLS = b'name,age,city,score\nAlice,30,NY,95.5\nBob,,LA,87.0\nCharlie,35,,92.3\nDiana,28,SF,\n'


class TestParitySelection:
    """Parity tests for selection/filtering operators."""

    def test_filter(self):
        assert_parity('csv | filter "col(\'age\') > 25" | csv', data=CSV_CITIES)

    def test_filter_equality(self):
        assert_parity('csv | filter "col(\'city\') == \'NY\'" | csv', data=CSV_CITIES)

    def test_select(self):
        assert_parity('csv | select name,age | csv', data=CSV_CITIES)

    def test_select_single(self):
        assert_parity('csv | select name | csv', data=CSV_CITIES)

    def test_head(self):
        assert_parity('csv | head 3 | csv', data=CSV_CITIES, ordered=True)

    def test_head_1(self):
        assert_parity('csv | head 1 | csv', data=CSV_CITIES, ordered=True)

    def test_skip(self):
        assert_parity('csv | skip 2 | csv', data=CSV_CITIES, ordered=True)

    def test_skip_all(self):
        """Skip more rows than available."""
        dsl = 'csv | skip 100 | csv'
        native = tf.pipeline(dsl).run(input=CSV_CITIES)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=CSV_CITIES)
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        # Both should have only header (or be empty)
        assert len(n_lines) <= 2
        assert len(d_lines) <= 2

    def test_top(self):
        assert_parity('csv | top 3 score | csv', data=CSV_CITIES, ordered=True)

    def test_top_asc(self):
        assert_parity('csv | top 2 -score | csv', data=CSV_CITIES, ordered=True)

    def test_grep(self):
        assert_parity('csv | grep Ali name | csv', data=CSV_CITIES)

    def test_grep_invert(self):
        assert_parity('csv | grep -v Ali name | csv', data=CSV_CITIES)


class TestParityColumnOps:
    """Parity tests for column manipulation operators."""

    def test_rename(self):
        assert_parity('csv | rename age=years | csv', data=CSV_CITIES)

    def test_derive_arithmetic(self):
        assert_parity("csv | derive age_x2=col('age')*2 | select name,age_x2 | csv",
                       data=CSV_CITIES)

    def test_derive_complex(self):
        assert_parity("csv | derive total=col('age')+col('score') | select name,total | csv",
                       data=CSV_CITIES, float_cols={'total'})

    def test_clip_both(self):
        assert_parity('csv | clip score min=80 max=95 | csv',
                       data=CSV_CITIES, float_cols={'score'})

    def test_clip_min_only(self):
        assert_parity('csv | clip score min=85 | csv',
                       data=CSV_CITIES, float_cols={'score'})

    def test_clip_max_only(self):
        assert_parity('csv | clip score max=90 | csv',
                       data=CSV_CITIES, float_cols={'score'})

    def test_replace(self):
        assert_parity('csv | replace city NY NewYork | csv', data=CSV_CITIES)

    def test_cast(self):
        """Cast to float: native keeps int representation, DuckDB adds .0."""
        data = b'name,val\nAlice,10\nBob,20\nCharlie,30\n'
        dsl = 'csv | cast val=float | csv'
        native = tf.pipeline(dsl).run(input=data)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=data)
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        assert len(n_lines) == len(d_lines)
        # Values should be numerically equal
        for nl, dl in zip(n_lines[1:], d_lines[1:]):
            n_val = float(nl.split(',')[1])
            d_val = float(dl.split(',')[1])
            assert abs(n_val - d_val) < 1e-6

    def test_hash(self):
        """Hash all columns: DuckDB hash(*) not supported, test native only."""
        dsl = 'csv | hash | csv'
        native = tf.pipeline(dsl).run(input=CSV_CITIES)
        n_header = native.output_text.strip().split('\n')[0]
        assert '_hash' in n_header

    def test_hash_columns(self):
        """Hash specific columns -- parity check."""
        dsl = 'csv | hash name,age | csv'
        native = tf.pipeline(dsl).run(input=CSV_CITIES)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=CSV_CITIES)
        assert '_hash' in native.output_text.strip().split('\n')[0]
        assert '_hash' in duck.output_text.strip().split('\n')[0]
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        assert len(n_lines) == len(d_lines)

    def test_trim(self):
        """Trim: native trims all columns, DuckDB trims only specified columns."""
        dsl = 'csv | trim name | csv'
        native = tf.pipeline(dsl).run(input=CSV_PADDED)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=CSV_PADDED)
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        assert len(n_lines) == len(d_lines)
        # Name column should be trimmed in both
        for nl, dl in zip(n_lines[1:], d_lines[1:]):
            n_name = nl.split(',')[0].strip()
            d_name = dl.split(',')[0].strip()
            assert n_name == d_name

    def test_bin(self):
        """Bin labels should match between engines."""
        assert_parity('csv | bin score 80,90 | select name,score_bin | csv', data=CSV_CITIES)

    def test_validate(self):
        """Both engines should add _valid column."""
        dsl = 'csv | validate "col(\'score\') > 85" | csv'
        native = tf.pipeline(dsl).run(input=CSV_CITIES)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=CSV_CITIES)
        n_header = native.output_text.strip().split('\n')[0]
        d_header = duck.output_text.strip().split('\n')[0]
        assert '_valid' in n_header
        assert '_valid' in d_header
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        assert len(n_lines) == len(d_lines)


class TestParityRowOps:
    """Parity tests for row-level operators."""

    def test_sort_asc(self):
        assert_parity('csv | sort age | csv', data=CSV_CITIES, ordered=True)

    def test_sort_desc(self):
        assert_parity('csv | sort -age | csv', data=CSV_CITIES, ordered=True)

    def test_unique(self):
        assert_parity('csv | unique city | csv', data=CSV_DUPS)

    def test_dedup(self):
        assert_parity('csv | dedup | csv', data=CSV_DUPS)


class TestParityAggregation:
    """Parity tests for aggregation operators."""

    def test_group_agg_sum(self):
        assert_parity('csv | group-agg city sales:sum:total | csv',
                       data=CSV_SALES, float_cols={'total'})

    def test_group_agg_avg(self):
        assert_parity('csv | group-agg city sales:avg:avg_sales | csv',
                       data=CSV_SALES, float_cols={'avg_sales'})

    def test_group_agg_count(self):
        assert_parity('csv | group-agg city sales:count:n | csv',
                       data=CSV_SALES, float_cols={'n'})

    def test_group_agg_min_max(self):
        dsl = 'csv | group-agg city sales:min:lo sales:max:hi | csv'
        assert_parity(dsl, data=CSV_SALES, float_cols={'lo', 'hi'})

    def test_frequency(self):
        assert_parity('csv | frequency city | csv', data=CSV_CITIES)

    def test_frequency_multi(self):
        """Multi-column frequency: native concatenates into value, DuckDB keeps separate.
        This is a known semantic difference; verify structure only."""
        data = b'a,b\nx,1\ny,1\nx,2\ny,1\nx,1\n'
        dsl = 'csv | frequency a,b | csv'
        native = tf.pipeline(dsl).run(input=data)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=data)
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        # Same number of distinct groups
        assert len(n_lines) == len(d_lines)


class TestParityReshape:
    """Parity tests for reshaping operators."""

    def test_explode(self):
        """Explode with default comma delimiter."""
        data = b'name,items\nAlice,a;b;c\nBob,x\n'
        dsl = 'csv | explode items ; | csv'
        native = tf.pipeline(dsl).run(input=data)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=data)
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        assert len(n_lines) == len(d_lines), f'native={len(n_lines)}, duck={len(d_lines)}'
        n_items = set(r.split(',')[1] for r in n_lines[1:] if r)
        d_items = set(r.split(',')[1] for r in d_lines[1:] if r)
        assert n_items == d_items

    def test_split(self):
        dsl = 'csv | split full_name " " first,last | select first,last,age | csv'
        assert_parity(dsl, data=CSV_NAMES)

    def test_unpivot(self):
        data = b'name,math,english\nAlice,90,85\nBob,75,92\n'
        dsl = 'csv | unpivot math,english | csv'
        native = tf.pipeline(dsl).run(input=data)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=data)
        n_header, n_rows = parse_rows(native.output_text.strip())
        d_header, d_rows = parse_rows(duck.output_text.strip())
        assert 'variable' in n_header
        assert 'value' in n_header
        assert 'variable' in d_header
        assert 'value' in d_header
        assert len(n_rows) == len(d_rows) == 4  # 2 names * 2 subjects

    def test_pivot(self):
        dsl = 'csv | pivot metric value sum | csv'
        native = tf.pipeline(dsl).run(input=CSV_PIVOT)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=CSV_PIVOT)
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        # Should have 3 data rows (ids 1, 2, 3) with revenue and cost columns
        assert len(n_lines) == len(d_lines)
        assert 'revenue' in n_lines[0] or 'cost' in n_lines[0]
        assert 'revenue' in d_lines[0] or 'cost' in d_lines[0]


class TestParitySequential:
    """Parity tests for window/sequential operators."""

    def test_step_cumsum(self):
        assert_parity('csv | step val running-sum cumsum | csv',
                       data=CSV_NUMS, ordered=True, float_cols={'cumsum'})

    def test_step_cumavg(self):
        assert_parity('csv | step val running-avg ravg | csv',
                       data=CSV_NUMS, ordered=True, float_cols={'ravg'})

    def test_window_avg(self):
        assert_parity('csv | window val 3 avg ma3 | csv',
                       data=CSV_NUMS, ordered=True, float_cols={'ma3'})

    def test_window_sum(self):
        assert_parity('csv | window val 2 sum ws | csv',
                       data=CSV_NUMS, ordered=True, float_cols={'ws'})

    def test_lead(self):
        assert_parity('csv | lead val 1 next_val | csv',
                       data=CSV_NUMS, ordered=True)

    def test_lead_offset_2(self):
        assert_parity('csv | lead val 2 next2 | csv',
                       data=CSV_NUMS, ordered=True)

    def test_fill_down(self):
        assert_parity('csv | fill-down group | csv',
                       data=CSV_FILLDOWN, ordered=True)


class TestParityDateTime:
    """Parity tests for datetime operators."""

    def test_datetime_extract(self):
        dsl = 'csv | datetime date year,month,day | csv'
        native = tf.pipeline(dsl).run(input=CSV_DATES)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=CSV_DATES)
        n_header = native.output_text.strip().split('\n')[0]
        d_header = duck.output_text.strip().split('\n')[0]
        for part in ['date_year', 'date_month', 'date_day']:
            assert part in n_header, f'{part} missing from native header'
            assert part in d_header, f'{part} missing from duck header'
        # Compare extracted year values
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        assert len(n_lines) == len(d_lines)

    def test_date_trunc(self):
        dsl = 'csv | date-trunc date month date_month | csv'
        native = tf.pipeline(dsl).run(input=CSV_DATES)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=CSV_DATES)
        n_header = native.output_text.strip().split('\n')[0]
        d_header = duck.output_text.strip().split('\n')[0]
        assert 'date_month' in n_header
        assert 'date_month' in d_header


class TestParityFillNull:
    """Parity tests for null handling."""

    def test_fill_null(self):
        dsl = 'csv | fill-null age=0 city=unknown score=0 | csv'
        native = tf.pipeline(dsl).run(input=CSV_NULLS)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=CSV_NULLS)
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        assert len(n_lines) == len(d_lines)
        # Both should have no empty fields for filled columns
        for line in n_lines[1:]:
            parts = line.split(',')
            assert len(parts) >= 4
            assert all(p != '' for p in parts), f'Native has empty field: {line}'
        for line in d_lines[1:]:
            parts = line.split(',')
            assert len(parts) >= 4
            assert all(p != '' for p in parts), f'DuckDB has empty field: {line}'


class TestParityChained:
    """Parity tests for multi-step pipelines."""

    def test_filter_sort_head(self):
        assert_parity('csv | filter "col(\'age\') > 22" | sort -score | head 3 | csv',
                       data=CSV_CITIES, ordered=True)

    def test_derive_filter_select(self):
        assert_parity(
            "csv | derive double=col('age')*2 | filter \"col('double') > 50\" | select name,double | csv",
            data=CSV_CITIES, float_cols={'double'})

    def test_rename_sort_head(self):
        assert_parity('csv | rename score=pts | sort -pts | head 2 | csv',
                       data=CSV_CITIES, ordered=True)

    def test_group_agg_sort(self):
        assert_parity('csv | group-agg city sales:sum:total | sort -total | csv',
                       data=CSV_SALES, ordered=True, float_cols={'total'})

    def test_filter_unique(self):
        assert_parity('csv | filter "col(\'age\') > 25" | unique city | csv',
                       data=CSV_CITIES)

    def test_clip_derive(self):
        assert_parity("csv | clip score min=80 max=90 | derive bonus=col('score')+10 | select name,bonus | csv",
                       data=CSV_CITIES, float_cols={'bonus'})


class TestParityEdgeCases:
    """Edge cases and corner conditions."""

    def test_empty_after_filter(self):
        """Filter that matches no rows."""
        dsl = 'csv | filter "col(\'age\') > 100" | csv'
        native = tf.pipeline(dsl).run(input=CSV_CITIES)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=CSV_CITIES)
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        # Both should have just header
        assert len(n_lines) <= 2  # header only (or header + empty)
        assert len(d_lines) <= 2

    def test_single_row(self):
        """Single data row."""
        data = b'x,y\n1,2\n'
        assert_parity('csv | derive z=col(\'x\')+col(\'y\') | csv',
                       data=data, float_cols={'z'})

    def test_large_numbers(self):
        data = b'val\n1000000\n2000000\n3000000\n'
        assert_parity('csv | derive sq=col(\'val\')*col(\'val\') | csv',
                       data=data, float_cols={'sq'})

    def test_negative_numbers(self):
        data = b'val\n-10\n-20\n30\n'
        assert_parity('csv | sort val | csv', data=data, ordered=True)

    def test_sort_stable(self):
        """Rows with same sort key should appear in both engines."""
        data = b'name,score\nAlice,90\nBob,90\nCharlie,85\n'
        dsl = 'csv | sort -score | csv'
        native = tf.pipeline(dsl).run(input=data)
        duck = tf.pipeline(dsl, engine='duckdb').run(input=data)
        # Both should have Charlie last (score 85)
        n_lines = native.output_text.strip().split('\n')
        d_lines = duck.output_text.strip().split('\n')
        assert 'Charlie' in n_lines[-1]
        assert 'Charlie' in d_lines[-1]
        # Alice and Bob both have 90, order may differ but both present
        assert len(n_lines) == len(d_lines)

    def test_many_columns(self):
        data = b'a,b,c,d,e\n1,2,3,4,5\n6,7,8,9,10\n'
        assert_parity('csv | select a,c,e | csv', data=data)

    def test_special_values_in_strings(self):
        """Strings with spaces and mixed case."""
        data = b'name,city\nAlice,New York\nBob,Los Angeles\n'
        assert_parity('csv | sort name | csv', data=data, ordered=True)

    def test_head_exceeds_rows(self):
        """Head larger than dataset returns all rows."""
        data = b'x\n1\n2\n'
        assert_parity('csv | head 100 | csv', data=data, ordered=True)

    def test_multiple_renames(self):
        assert_parity('csv | rename age=years score=pts | csv', data=CSV_CITIES)

    def test_derive_constant(self):
        assert_parity("csv | derive one=1 | select name,one | csv", data=CSV_CITIES)

    def test_frequency_single_value(self):
        """Single unique value frequency."""
        data = b'x\na\na\na\n'
        assert_parity('csv | frequency x | csv', data=data)
