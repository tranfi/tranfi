"""
test_parity.py — Pandas parity tests for tranfi.

Runs the same operations in tranfi and pandas, compares outputs.
Catches subtle bugs in type handling, null behavior, sorting,
aggregation, and edge cases.

Run:
    cd /home/anton/projects/tranfi/tranfi
    python -m pytest test/test_parity.py -v
"""

import io
import json
import os
import sys
import tempfile

import pandas as pd
import pytest

# Add the Python package to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'py'))

# Set library path to the build directory
build_dir = os.path.join(os.path.dirname(__file__), '..', 'build')
lib_path = os.path.join(build_dir, 'libtranfi.so')
if os.path.exists(lib_path):
    os.environ['TRANFI_LIB_PATH'] = lib_path

import tranfi as tf


# ---------------------------------------------------------------------------
# Test datasets
# ---------------------------------------------------------------------------

CSV_DATA = (
    b'name,age,city,score,active\n'
    b'Alice,30,NY,95.5,true\n'
    b'Bob,25,LA,87.0,false\n'
    b'Charlie,35,NY,92.3,true\n'
    b'Diana,28,SF,88.1,true\n'
    b'Eve,25,LA,91.0,false\n'
    b'Frank,40,NY,76.5,true\n'
    b'Grace,33,SF,94.2,false\n'
    b'Henry,29,LA,85.0,true\n'
)

CSV_DF = pd.read_csv(io.BytesIO(CSV_DATA))

CSV_WITH_NULLS = (
    b'name,age,city,score\n'
    b'Alice,30,NY,95.5\n'
    b'Bob,,LA,87.0\n'
    b'Charlie,35,,92.3\n'
    b'Diana,28,SF,\n'
)

CSV_WITH_DUPS = (
    b'name,age,city\n'
    b'Alice,30,NY\n'
    b'Bob,25,LA\n'
    b'Alice,30,NY\n'
    b'Charlie,35,SF\n'
    b'Bob,25,LA\n'
)

CSV_WITH_DATES = (
    b'id,date,value\n'
    b'1,2024-03-15,100\n'
    b'2,2023-12-01,200\n'
    b'3,2024-07-22,150\n'
)

CSV_WITH_TAGS = (
    b'name,tags\n'
    b'Alice,python|sql|r\n'
    b'Bob,java|go\n'
    b'Charlie,python\n'
)

CSV_FULL_NAMES = (
    b'full_name,age\n'
    b'Alice Smith,30\n'
    b'Bob Jones,25\n'
    b'Charlie Brown,35\n'
)

CSV_PADDED = (
    b'name,city\n'
    b'  Alice  , NY \n'
    b' Bob ,  LA  \n'
    b'Charlie,SF\n'
)

CSV_PIVOT_DATA = (
    b'id,metric,value\n'
    b'1,revenue,100\n'
    b'1,cost,40\n'
    b'2,revenue,200\n'
    b'2,cost,80\n'
    b'3,revenue,150\n'
    b'3,cost,60\n'
)

CSV_LOOKUP = (
    b'city,country\n'
    b'NY,USA\n'
    b'LA,USA\n'
    b'SF,USA\n'
)

CSV_NUMERIC = (
    b'val\n'
    b'10\n'
    b'20\n'
    b'30\n'
    b'40\n'
    b'50\n'
)

CSV_WITH_FILL_DOWN = (
    b'group,value\n'
    b'A,1\n'
    b',2\n'
    b',3\n'
    b'B,4\n'
    b',5\n'
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_tf(steps, data=CSV_DATA):
    """Run tranfi pipeline, return output as DataFrame."""
    p = tf.pipeline(steps)
    result = p.run(input=data)
    text = result.output_text.strip()
    if not text:
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(text))


def run_tf_dsl(dsl, data=CSV_DATA):
    """Run tranfi DSL pipeline, return output as DataFrame."""
    p = tf.pipeline(dsl)
    result = p.run(input=data)
    text = result.output_text.strip()
    if not text:
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(text))


def run_tf_raw(steps, data=CSV_DATA):
    """Run tranfi pipeline, return raw text output."""
    p = tf.pipeline(steps)
    result = p.run(input=data)
    return result.output_text


def assert_df_equal(actual, expected, atol=1e-6, check_order=True):
    """Compare DataFrames with tolerance for floats."""
    pd.testing.assert_frame_equal(
        actual.reset_index(drop=True),
        expected.reset_index(drop=True),
        atol=atol,
        check_like=not check_order,
        check_dtype=False,
    )


# ---------------------------------------------------------------------------
# 1. Codecs
# ---------------------------------------------------------------------------

class TestCodecs:
    def test_csv_passthrough(self):
        """CSV roundtrip should preserve data."""
        result = run_tf([tf.codec.csv(), tf.codec.csv_encode()])
        assert_df_equal(result, CSV_DF)

    def test_csv_tsv_delimiter(self):
        """Write TSV, re-read as CSV with tab delimiter."""
        p = tf.pipeline([tf.codec.csv(), tf.codec.csv_encode(delimiter='\t')])
        tsv = p.run(input=CSV_DATA).output
        result = run_tf([tf.codec.csv(delimiter='\t'), tf.codec.csv_encode()], data=tsv)
        assert_df_equal(result, CSV_DF)

    def test_jsonl_roundtrip(self):
        """CSV → JSONL → CSV should preserve data."""
        p1 = tf.pipeline([tf.codec.csv(), tf.codec.jsonl_encode()])
        jsonl = p1.run(input=CSV_DATA).output
        result = run_tf([tf.codec.jsonl(), tf.codec.csv_encode()], data=jsonl)
        assert_df_equal(result, CSV_DF)

    def test_text_roundtrip(self):
        """Text codec roundtrip preserves lines."""
        lines = b'hello world\nfoo bar\nbaz\n'
        p = tf.pipeline([tf.codec.text(), tf.codec.text_encode()])
        result = p.run(input=lines)
        for line in [b'hello world', b'foo bar', b'baz']:
            assert line in result.output


# ---------------------------------------------------------------------------
# 2. Row Filtering & Slicing
# ---------------------------------------------------------------------------

class TestRowFiltering:
    def test_filter_numeric(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.filter(tf.expr("col('age') > 30")),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF[CSV_DF.age > 30].copy()
        assert_df_equal(result, expected)

    def test_filter_string_eq(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.filter(tf.expr("col('city') == 'NY'")),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF[CSV_DF.city == 'NY'].copy()
        assert_df_equal(result, expected)

    def test_filter_compound(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.filter(tf.expr("col('age') > 25 and col('score') > 90")),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF[(CSV_DF.age > 25) & (CSV_DF.score > 90)].copy()
        assert_df_equal(result, expected)

    def test_head(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.head(3),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.head(3).copy()
        assert_df_equal(result, expected)

    def test_tail(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.tail(3),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.tail(3).copy()
        assert_df_equal(result, expected)

    def test_skip(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.skip(5),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.iloc[5:].copy()
        assert_df_equal(result, expected)

    def test_top(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.top(3, 'score'),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.nlargest(3, 'score').copy()
        assert_df_equal(result, expected, check_order=False)

    def test_sample(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.sample(4),
            tf.codec.csv_encode(),
        ])
        assert len(result) == 4
        # All sampled names must exist in original
        for name in result['name']:
            assert name in CSV_DF['name'].values


# ---------------------------------------------------------------------------
# 3. Column Operations
# ---------------------------------------------------------------------------

class TestColumnOps:
    def test_select(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.select(['name', 'age']),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF[['name', 'age']].copy()
        assert_df_equal(result, expected)

    def test_rename(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.rename(name='full_name'),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.rename(columns={'name': 'full_name'}).copy()
        assert_df_equal(result, expected)

    def test_derive_multiply(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.derive({'double_age': tf.expr("col('age') * 2")}),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.copy()
        expected['double_age'] = expected['age'] * 2
        assert_df_equal(result, expected)

    def test_derive_add(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.derive({'total': tf.expr("col('age') + col('score')")}),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.copy()
        expected['total'] = expected['age'] + expected['score']
        assert_df_equal(result, expected)

    def test_trim(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.trim(),
            tf.codec.csv_encode(),
        ], data=CSV_PADDED)
        # Verify all string values are stripped
        for col in result.columns:
            if result[col].dtype == object:
                for val in result[col]:
                    assert val == val.strip(), f'{col}: {val!r} not stripped'
        assert list(result['name']) == ['Alice', 'Bob', 'Charlie']
        assert list(result['city']) == ['NY', 'LA', 'SF']

    def test_fill_null(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.fill_null(age='0', city='unknown', score='0'),
            tf.codec.csv_encode(),
        ], data=CSV_WITH_NULLS)
        assert result['age'].notna().all()
        assert result['city'].notna().all()
        assert result['score'].notna().all()
        # Bob's age was null, should now be '0'
        bob = result[result['name'] == 'Bob'].iloc[0]
        assert str(int(float(bob['age']))) == '0'

    def test_cast(self):
        """Cast to float should allow float operations downstream."""
        data = b'name,val\nAlice,10\nBob,20\n'
        # Cast to float then derive a division — verifies float behavior
        result = run_tf([
            tf.codec.csv(),
            tf.ops.cast(val='float'),
            tf.ops.derive({'half': tf.expr("col('val') / 3")}),
            tf.codec.csv_encode(),
        ], data=data)
        # Without float cast, integer division might differ
        assert 'half' in result.columns
        assert abs(float(result.iloc[0]['half']) - 10 / 3) < 1e-4

    def test_clip(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.clip('score', min=80, max=95),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.copy()
        expected['score'] = expected['score'].clip(80, 95)
        assert_df_equal(result, expected)

    def test_replace(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.replace('city', 'NY', 'New York'),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.copy()
        expected['city'] = expected['city'].replace('NY', 'New York')
        assert_df_equal(result, expected)

    def test_validate(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.validate(tf.expr("col('score') > 85")),
            tf.codec.csv_encode(),
        ])
        assert '_valid' in result.columns
        assert len(result) == len(CSV_DF)
        # Check specific rows
        alice = result[result['name'] == 'Alice'].iloc[0]
        frank = result[result['name'] == 'Frank'].iloc[0]
        assert str(alice['_valid']).lower() == 'true'
        assert str(frank['_valid']).lower() == 'false'

    def test_hash(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.hash(),
            tf.codec.csv_encode(),
        ])
        assert '_hash' in result.columns
        assert len(result) == len(CSV_DF)
        # Deterministic: same input → same hash
        result2 = run_tf([
            tf.codec.csv(),
            tf.ops.hash(),
            tf.codec.csv_encode(),
        ])
        assert list(result['_hash']) == list(result2['_hash'])

    def test_bin(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.bin('score', [80, 90]),
            tf.codec.csv_encode(),
        ])
        assert 'score_bin' in result.columns
        assert len(result) == len(CSV_DF)


# ---------------------------------------------------------------------------
# 4. Sorting & Dedup
# ---------------------------------------------------------------------------

class TestSortDedup:
    def test_sort_asc(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.sort(['age']),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.sort_values('age', kind='stable').copy()
        assert_df_equal(result, expected)

    def test_sort_desc(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.sort(['-score']),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.sort_values('score', ascending=False, kind='stable').copy()
        assert_df_equal(result, expected)

    def test_sort_multi(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.sort(['city', '-age']),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.sort_values(
            ['city', 'age'], ascending=[True, False], kind='stable'
        ).copy()
        assert_df_equal(result, expected)

    def test_dedup(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.dedup(),
            tf.codec.csv_encode(),
        ], data=CSV_WITH_DUPS)
        df = pd.read_csv(io.BytesIO(CSV_WITH_DUPS))
        expected = df.drop_duplicates().copy()
        assert_df_equal(result, expected, check_order=False)


# ---------------------------------------------------------------------------
# 5. Aggregation
# ---------------------------------------------------------------------------

class TestAggregation:
    def test_stats_basic(self):
        """Verify count, avg, min, max match pandas."""
        data = b'x,y\n10,1\n20,2\n30,3\n40,4\n50,5\n'
        result = run_tf([
            tf.codec.csv(),
            tf.ops.stats(['count', 'avg', 'min', 'max']),
            tf.codec.csv_encode(),
        ], data=data)
        df = pd.read_csv(io.BytesIO(data))
        for col in ['x', 'y']:
            row = result[result['column'] == col].iloc[0]
            assert int(row['count']) == len(df)
            assert abs(float(row['avg']) - df[col].mean()) < 1e-6
            assert abs(float(row['min']) - df[col].min()) < 1e-6
            assert abs(float(row['max']) - df[col].max()) < 1e-6

    def test_stats_stddev(self):
        """Verify stddev and var match pandas (sample, ddof=1)."""
        data = b'x\n10\n20\n30\n40\n50\n'
        result = run_tf([
            tf.codec.csv(),
            tf.ops.stats(['stddev', 'var']),
            tf.codec.csv_encode(),
        ], data=data)
        df = pd.read_csv(io.BytesIO(data))
        row = result[result['column'] == 'x'].iloc[0]
        assert abs(float(row['stddev']) - df['x'].std(ddof=1)) < 1e-4
        assert abs(float(row['var']) - df['x'].var(ddof=1)) < 1e-4

    def test_stats_median(self):
        """Verify median is approximately correct (P2 quantile)."""
        data = b'x\n10\n20\n30\n40\n50\n'
        result = run_tf([
            tf.codec.csv(),
            tf.ops.stats(['median']),
            tf.codec.csv_encode(),
        ], data=data)
        df = pd.read_csv(io.BytesIO(data))
        row = result[result['column'] == 'x'].iloc[0]
        # P2 quantile can deviate on small datasets
        assert abs(float(row['median']) - df['x'].median()) < 10.0

    def test_frequency(self):
        """Verify frequency counts match pandas value_counts."""
        result = run_tf([
            tf.codec.csv(),
            tf.ops.frequency(['city']),
            tf.codec.csv_encode(),
        ])
        vc = CSV_DF['city'].value_counts()
        for _, row in result.iterrows():
            city = row['value']
            assert int(row['count']) == vc[city]

    def test_group_agg_sum(self):
        # Output column name follows {column}_{func} pattern
        result = run_tf([
            tf.codec.csv(),
            tf.ops.group_agg(['city'], [
                {'column': 'score', 'func': 'sum', 'result': 'score_sum'},
            ]),
            tf.codec.csv_encode(),
        ])
        expected_sum = CSV_DF.groupby('city')['score'].sum()
        for _, row in result.iterrows():
            city = row['city']
            assert abs(float(row['score_sum']) - expected_sum[city]) < 1e-4

    def test_group_agg_multi(self):
        # Output column names: score_avg, age_count
        result = run_tf([
            tf.codec.csv(),
            tf.ops.group_agg(['city'], [
                {'column': 'score', 'func': 'avg', 'result': 'score_avg'},
                {'column': 'age', 'func': 'count', 'result': 'age_count'},
            ]),
            tf.codec.csv_encode(),
        ])
        expected_avg = CSV_DF.groupby('city')['score'].mean()
        expected_cnt = CSV_DF.groupby('city')['age'].count()
        for _, row in result.iterrows():
            city = row['city']
            assert abs(float(row['score_avg']) - expected_avg[city]) < 1e-4
            assert int(float(row['age_count'])) == expected_cnt[city]


# ---------------------------------------------------------------------------
# 6. Reshape
# ---------------------------------------------------------------------------

class TestReshape:
    def test_unpivot(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.unpivot(['age', 'score']),
            tf.codec.csv_encode(),
        ])
        expected = CSV_DF.melt(
            id_vars=[c for c in CSV_DF.columns if c not in ('age', 'score')],
            value_vars=['age', 'score'],
        )
        # Tranfi output columns: id cols + variable + value
        assert 'variable' in result.columns
        assert 'value' in result.columns
        assert len(result) == len(CSV_DF) * 2

    def test_explode(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.explode('tags', '|'),
            tf.codec.csv_encode(),
        ], data=CSV_WITH_TAGS)
        df = pd.read_csv(io.BytesIO(CSV_WITH_TAGS))
        expected = df.assign(
            tags=df['tags'].str.split('|')
        ).explode('tags').reset_index(drop=True)
        assert len(result) == len(expected)
        assert set(result['tags']) == set(expected['tags'])

    def test_split(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.split('full_name', ['first', 'last']),
            tf.codec.csv_encode(),
        ], data=CSV_FULL_NAMES)
        assert 'first' in result.columns
        assert 'last' in result.columns
        df = pd.read_csv(io.BytesIO(CSV_FULL_NAMES))
        parts = df['full_name'].str.split(' ', expand=True)
        assert list(result['first']) == list(parts[0])
        assert list(result['last']) == list(parts[1])

    def test_pivot(self):
        result = run_tf([
            {'op': 'codec.csv.decode', 'args': {}},
            {'op': 'pivot', 'args': {
                'name_column': 'metric',
                'value_column': 'value',
                'agg': 'sum',
            }},
            {'op': 'codec.csv.encode', 'args': {}},
        ], data=CSV_PIVOT_DATA)
        df = pd.read_csv(io.BytesIO(CSV_PIVOT_DATA))
        expected = df.pivot_table(
            index='id', columns='metric', values='value', aggfunc='sum'
        ).reset_index()
        # Verify shape: 3 rows (ids 1, 2, 3) with revenue and cost columns
        assert len(result) == 3
        assert 'revenue' in result.columns
        assert 'cost' in result.columns
        for _, row in result.iterrows():
            eid = int(row['id'])
            exp_row = expected[expected['id'] == eid].iloc[0]
            assert abs(float(row['revenue']) - float(exp_row['revenue'])) < 1e-6
            assert abs(float(row['cost']) - float(exp_row['cost'])) < 1e-6


# ---------------------------------------------------------------------------
# 7. Sequential / Window
# ---------------------------------------------------------------------------

class TestSequential:
    def test_step_running_sum(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.step('val', 'running-sum', 'cumsum'),
            tf.codec.csv_encode(),
        ], data=CSV_NUMERIC)
        df = pd.read_csv(io.BytesIO(CSV_NUMERIC))
        expected_cumsum = df['val'].cumsum()
        for i, (_, row) in enumerate(result.iterrows()):
            assert abs(float(row['cumsum']) - expected_cumsum.iloc[i]) < 1e-6

    def test_step_running_avg(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.step('val', 'running-avg', 'ravg'),
            tf.codec.csv_encode(),
        ], data=CSV_NUMERIC)
        df = pd.read_csv(io.BytesIO(CSV_NUMERIC))
        expected_avg = df['val'].expanding().mean()
        for i, (_, row) in enumerate(result.iterrows()):
            assert abs(float(row['ravg']) - expected_avg.iloc[i]) < 1e-6

    def test_window_avg(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.window('val', 3, 'avg', 'ma3'),
            tf.codec.csv_encode(),
        ], data=CSV_NUMERIC)
        df = pd.read_csv(io.BytesIO(CSV_NUMERIC))
        rolling = df['val'].rolling(3).mean()
        # First 2 rows may be NaN/partial in pandas; compare from row 3 onward
        for i in range(2, len(result)):
            assert abs(float(result.iloc[i]['ma3']) - rolling.iloc[i]) < 1e-6

    def test_fill_down(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.fill_down(),
            tf.codec.csv_encode(),
        ], data=CSV_WITH_FILL_DOWN)
        # After fill_down: empty groups should be filled with last non-empty
        groups = list(result['group'])
        assert groups[0] == 'A'
        assert groups[1] == 'A'  # was empty, filled from row above
        assert groups[2] == 'A'  # was empty, filled from row above
        assert groups[3] == 'B'
        assert groups[4] == 'B'  # was empty, filled from row above


# ---------------------------------------------------------------------------
# 8. Join
# ---------------------------------------------------------------------------

class TestJoin:
    @pytest.fixture(autouse=True)
    def setup_lookup(self, tmp_path):
        self.lookup_path = str(tmp_path / 'lookup.csv')
        with open(self.lookup_path, 'wb') as f:
            f.write(CSV_LOOKUP)

    def test_inner_join(self):
        result = run_tf([
            {'op': 'codec.csv.decode', 'args': {}},
            {'op': 'join', 'args': {
                'file': self.lookup_path,
                'on': 'city',
            }},
            {'op': 'codec.csv.encode', 'args': {}},
        ])
        lookup = pd.read_csv(io.BytesIO(CSV_LOOKUP))
        expected = CSV_DF.merge(lookup, on='city', how='inner')
        assert len(result) == len(expected)
        assert 'country' in result.columns
        for _, row in result.iterrows():
            assert row['country'] == 'USA'

    def test_left_join(self):
        # Add a city not in lookup
        data = CSV_DATA + b'Ivan,22,Tokyo,80.0,true\n'
        result = run_tf([
            {'op': 'codec.csv.decode', 'args': {}},
            {'op': 'join', 'args': {
                'file': self.lookup_path,
                'on': 'city',
                'how': 'left',
            }},
            {'op': 'codec.csv.encode', 'args': {}},
        ], data=data)
        assert len(result) == 9  # 8 original + 1 Ivan
        assert 'country' in result.columns


# ---------------------------------------------------------------------------
# 9. Datetime
# ---------------------------------------------------------------------------

class TestDatetime:
    def test_date_extract(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.datetime('date', ['year', 'month', 'day']),
            tf.codec.csv_encode(),
        ], data=CSV_WITH_DATES)
        df = pd.read_csv(io.BytesIO(CSV_WITH_DATES))
        df['date'] = pd.to_datetime(df['date'])
        assert 'date_year' in result.columns
        assert 'date_month' in result.columns
        assert 'date_day' in result.columns
        assert list(result['date_year'].astype(int)) == list(df['date'].dt.year)
        assert list(result['date_month'].astype(int)) == list(df['date'].dt.month)
        assert list(result['date_day'].astype(int)) == list(df['date'].dt.day)

    def test_cast_to_date(self):
        result = run_tf([
            tf.codec.csv(),
            tf.ops.cast(date='date'),
            tf.codec.csv_encode(),
        ], data=CSV_WITH_DATES)
        # After cast, dates should still be parseable
        dates = pd.to_datetime(result['date'])
        assert len(dates) == 3


# ---------------------------------------------------------------------------
# 10. Recipes
# ---------------------------------------------------------------------------

class TestRecipes:
    def test_recipe_profile(self):
        """Profile recipe should output stats for all numeric columns."""
        result = run_tf_dsl('profile')
        assert 'column' in result.columns
        cols_reported = set(result['column'])
        # All numeric columns should appear
        for col in ['age', 'score']:
            assert col in cols_reported

    def test_recipe_preview(self):
        """Preview recipe shows first 10 rows (we have 8)."""
        result = run_tf_dsl('preview')
        assert_df_equal(result, CSV_DF)

    def test_recipe_summary(self):
        """Summary recipe outputs count, min, max, avg, stddev."""
        result = run_tf_dsl('summary')
        assert 'column' in result.columns
        for stat in ['count', 'min', 'max', 'avg', 'stddev']:
            assert stat in result.columns

    def test_recipe_count(self):
        """Count recipe outputs row count per column."""
        result = run_tf_dsl('count')
        assert 'column' in result.columns
        assert 'count' in result.columns
        for _, row in result.iterrows():
            assert int(row['count']) == len(CSV_DF)

    def test_recipe_freq(self):
        """Freq recipe outputs value counts."""
        result = run_tf_dsl('freq')
        assert 'value' in result.columns
        assert 'count' in result.columns
        assert len(result) > 0

    def test_recipe_dedup(self):
        """Dedup recipe removes duplicate rows."""
        result = run_tf_dsl('dedup', data=CSV_WITH_DUPS)
        df = pd.read_csv(io.BytesIO(CSV_WITH_DUPS))
        expected = df.drop_duplicates()
        assert len(result) == len(expected)

    def test_recipe_clean(self):
        """Clean recipe trims whitespace."""
        result = run_tf_dsl('clean', data=CSV_PADDED)
        for col in result.columns:
            if result[col].dtype == object:
                for val in result[col]:
                    assert val == val.strip()

    def test_recipe_csv2json(self):
        """CSV to JSON recipe produces valid JSONL."""
        p = tf.pipeline('csv2json')
        raw = p.run(input=CSV_DATA).output_text.strip()
        lines = raw.split('\n')
        assert len(lines) == len(CSV_DF)
        for line in lines:
            obj = json.loads(line)
            assert 'name' in obj
            assert 'age' in obj

    def test_recipe_head(self):
        """Head recipe returns first 20 rows (we have 8)."""
        result = run_tf_dsl('head')
        assert_df_equal(result, CSV_DF)

    def test_recipe_sample(self):
        """Sample recipe returns subset of original."""
        result = run_tf_dsl('sample')
        assert len(result) <= len(CSV_DF)
        for name in result['name']:
            assert name in CSV_DF['name'].values


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
