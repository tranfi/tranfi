"""
test_properties.py -- Property-based tests for tranfi using Hypothesis.

Tests invariants that must hold for any valid input data:
- CSV roundtrip stability
- Row count invariants across transforms
- Head/skip/tail bounds
- Filter/unique/sort semantic properties
- Derive column addition

Run:
    cd tranfi && source ~/tools/miniconda3/etc/profile.d/conda.sh && conda activate base && \
    TRANFI_LIB_PATH=build/libtranfi.so python -m pytest test/test_properties.py -v --tb=short
"""

import os
import sys
import csv
import io

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'py'))
build_dir = os.path.join(os.path.dirname(__file__), '..', 'build')
lib_path = os.path.join(build_dir, 'libtranfi.so')
if os.path.exists(lib_path):
    os.environ['TRANFI_LIB_PATH'] = lib_path

import tranfi as tf
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st


# --- Helpers ---

def make_csv(headers, rows):
    """Build CSV bytes from headers and list-of-lists."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for row in rows:
        w.writerow(row)
    return buf.getvalue().encode('utf-8')


def parse_csv_output(text):
    """Parse CSV output text into (headers, rows)."""
    text = text.strip()
    if not text:
        return [], []
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return [], []
    return rows[0], rows[1:]


def run_pipeline(steps, data):
    """Run a pipeline and return output text."""
    p = tf.pipeline(steps)
    result = p.run(input=data)
    return result.output_text


# --- Strategies ---

# Safe cell values: printable ASCII, no newlines/commas/quotes to keep CSV simple
safe_cell = st.text(
    alphabet=st.characters(whitelist_categories=('L', 'N'),
                           whitelist_characters=' ._-'),
    min_size=0, max_size=20
)

# Numeric values as strings
numeric_str = st.integers(min_value=-9999, max_value=9999).map(str)

# Column names: non-empty, no special chars
col_name = st.text(
    alphabet=st.characters(whitelist_categories=('L',), whitelist_characters='_'),
    min_size=1, max_size=10
).filter(lambda s: s[0].isalpha())


# --- Tests ---

@given(
    rows=st.lists(
        st.lists(safe_cell, min_size=3, max_size=3),
        min_size=1, max_size=20
    )
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow])
def test_csv_roundtrip_stability(rows):
    """decode(encode(decode(data))) == decode(data).
    A second roundtrip must not change the data."""
    headers = ['a', 'b', 'c']
    data = make_csv(headers, rows)

    # First roundtrip
    out1 = run_pipeline([tf.codec.csv(), tf.codec.csv_encode()], data)
    # Second roundtrip
    out2 = run_pipeline([tf.codec.csv(), tf.codec.csv_encode()], out1.encode('utf-8'))

    h1, r1 = parse_csv_output(out1)
    h2, r2 = parse_csv_output(out2)
    assert h1 == h2, f"Headers changed: {h1} vs {h2}"
    assert r1 == r2, f"Rows changed after second roundtrip"


@given(
    n=st.integers(min_value=1, max_value=30),
    n_rows=st.integers(min_value=0, max_value=30)
)
@settings(max_examples=50)
def test_head_row_count(n, n_rows):
    """head(n) produces min(n, total_rows) rows. n must be >= 1."""
    rows = [[str(i)] for i in range(n_rows)]
    data = make_csv(['x'], rows)
    out = run_pipeline([tf.codec.csv(), tf.ops.head(n), tf.codec.csv_encode()], data)
    _, out_rows = parse_csv_output(out)
    expected = min(n, n_rows)
    assert len(out_rows) == expected, f"head({n}) on {n_rows} rows: got {len(out_rows)}, expected {expected}"


@given(
    n=st.integers(min_value=1, max_value=30),
    n_rows=st.integers(min_value=0, max_value=30)
)
@settings(max_examples=50)
def test_skip_row_count(n, n_rows):
    """skip(n) produces max(0, total_rows - n) rows. n must be >= 1."""
    rows = [[str(i)] for i in range(n_rows)]
    data = make_csv(['x'], rows)
    out = run_pipeline([tf.codec.csv(), tf.ops.skip(n), tf.codec.csv_encode()], data)
    _, out_rows = parse_csv_output(out)
    expected = max(0, n_rows - n)
    assert len(out_rows) == expected, f"skip({n}) on {n_rows} rows: got {len(out_rows)}, expected {expected}"


@given(
    n_head=st.integers(min_value=1, max_value=20),
    n_skip=st.integers(min_value=1, max_value=20),
    n_rows=st.integers(min_value=0, max_value=30)
)
@settings(max_examples=50)
def test_head_after_skip(n_head, n_skip, n_rows):
    """skip(s) | head(h) produces min(h, max(0, total - s)) rows."""
    rows = [[str(i)] for i in range(n_rows)]
    data = make_csv(['x'], rows)
    out = run_pipeline([
        tf.codec.csv(), tf.ops.skip(n_skip), tf.ops.head(n_head), tf.codec.csv_encode()
    ], data)
    _, out_rows = parse_csv_output(out)
    expected = min(n_head, max(0, n_rows - n_skip))
    assert len(out_rows) == expected


@given(
    threshold=st.integers(min_value=-100, max_value=100),
    values=st.lists(st.integers(min_value=-100, max_value=100), min_size=1, max_size=30)
)
@settings(max_examples=50)
def test_filter_row_count(threshold, values):
    """filter(col('x') > threshold) produces exactly the rows matching the predicate."""
    rows = [[str(v)] for v in values]
    data = make_csv(['x'], rows)
    out = run_pipeline([
        tf.codec.csv(),
        tf.ops.filter(tf.expr(f"col('x') > {threshold}")),
        tf.codec.csv_encode()
    ], data)
    _, out_rows = parse_csv_output(out)
    expected_count = sum(1 for v in values if v > threshold)
    assert len(out_rows) == expected_count


@given(
    values=st.lists(st.integers(min_value=-100, max_value=100), min_size=1, max_size=30)
)
@settings(max_examples=50)
def test_filter_values_correct(values):
    """All rows passing filter actually satisfy the predicate."""
    rows = [[str(v)] for v in values]
    data = make_csv(['x'], rows)
    out = run_pipeline([
        tf.codec.csv(),
        tf.ops.filter(tf.expr("col('x') > 0")),
        tf.codec.csv_encode()
    ], data)
    _, out_rows = parse_csv_output(out)
    for row in out_rows:
        assert int(row[0]) > 0, f"Filter leaked row with x={row[0]}"


@given(
    values=st.lists(
        st.text(alphabet='abcdefgh', min_size=1, max_size=5),
        min_size=1, max_size=30
    )
)
@settings(max_examples=50)
def test_unique_no_duplicates(values):
    """unique() output has no duplicate values in the target column."""
    rows = [[v] for v in values]
    data = make_csv(['x'], rows)
    out = run_pipeline([tf.codec.csv(), tf.ops.unique(['x']), tf.codec.csv_encode()], data)
    _, out_rows = parse_csv_output(out)
    out_values = [r[0] for r in out_rows]
    assert len(out_values) == len(set(out_values)), f"Duplicates in unique output: {out_values}"


@given(
    values=st.lists(
        st.text(alphabet='abcdefgh', min_size=1, max_size=5),
        min_size=1, max_size=30
    )
)
@settings(max_examples=50)
def test_unique_preserves_all_distinct(values):
    """unique() preserves every distinct value from the input."""
    rows = [[v] for v in values]
    data = make_csv(['x'], rows)
    out = run_pipeline([tf.codec.csv(), tf.ops.unique(['x']), tf.codec.csv_encode()], data)
    _, out_rows = parse_csv_output(out)
    out_set = {r[0] for r in out_rows}
    in_set = set(values)
    assert out_set == in_set, f"Missing values: {in_set - out_set}"


@given(
    values=st.lists(st.integers(min_value=-1000, max_value=1000), min_size=1, max_size=30)
)
@settings(max_examples=50)
def test_sort_ascending(values):
    """sort(['x']) produces rows in ascending order."""
    rows = [[str(v)] for v in values]
    data = make_csv(['x'], rows)
    out = run_pipeline([tf.codec.csv(), tf.ops.sort(['x']), tf.codec.csv_encode()], data)
    _, out_rows = parse_csv_output(out)
    out_values = [float(r[0]) for r in out_rows]
    assert out_values == sorted(out_values), f"Not sorted: {out_values}"


@given(
    values=st.lists(st.integers(min_value=-1000, max_value=1000), min_size=1, max_size=30)
)
@settings(max_examples=50)
def test_sort_preserves_rows(values):
    """sort() preserves the multiset of values (no rows lost or duplicated)."""
    rows = [[str(v)] for v in values]
    data = make_csv(['x'], rows)
    out = run_pipeline([tf.codec.csv(), tf.ops.sort(['x']), tf.codec.csv_encode()], data)
    _, out_rows = parse_csv_output(out)
    out_values = sorted([int(r[0]) for r in out_rows])
    in_values = sorted(values)
    assert out_values == in_values


@given(
    values=st.lists(st.integers(min_value=-1000, max_value=1000), min_size=1, max_size=30),
    n=st.integers(min_value=1, max_value=10)
)
@settings(max_examples=50)
def test_sort_head_subset(values, n):
    """sort | head(n) produces the same rows as sorting all then taking first n."""
    rows = [[str(v)] for v in values]
    data = make_csv(['x'], rows)
    out = run_pipeline([
        tf.codec.csv(), tf.ops.sort(['x']), tf.ops.head(n), tf.codec.csv_encode()
    ], data)
    _, out_rows = parse_csv_output(out)
    out_values = [float(r[0]) for r in out_rows]
    expected = sorted(values)[:n]
    expected_floats = [float(v) for v in expected]
    assert out_values == expected_floats


@given(
    values=st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=20)
)
@settings(max_examples=50)
def test_derive_adds_column(values):
    """derive adds a new column without changing existing ones."""
    rows = [[str(v)] for v in values]
    data = make_csv(['x'], rows)
    out = run_pipeline([
        tf.codec.csv(),
        tf.ops.derive({'y': tf.expr("col('x') * 2")}),
        tf.codec.csv_encode()
    ], data)
    headers, out_rows = parse_csv_output(out)
    assert 'x' in headers, "Original column lost"
    assert 'y' in headers, "Derived column not added"
    assert len(out_rows) == len(values), "Row count changed"
    xi = headers.index('x')
    yi = headers.index('y')
    for row, orig_val in zip(out_rows, values):
        assert int(row[xi]) == orig_val
        assert int(row[yi]) == orig_val * 2


@given(
    values=st.lists(st.integers(min_value=-100, max_value=100), min_size=1, max_size=20),
    lo=st.integers(min_value=-50, max_value=0),
    hi=st.integers(min_value=0, max_value=50)
)
@settings(max_examples=50)
def test_clip_bounds(lo, hi, values):
    """clip(col, min, max) clamps all values to [min, max]."""
    assume(lo <= hi)
    rows = [[str(v)] for v in values]
    data = make_csv(['x'], rows)
    out = run_pipeline([
        tf.codec.csv(), tf.ops.clip('x', min=lo, max=hi), tf.codec.csv_encode()
    ], data)
    _, out_rows = parse_csv_output(out)
    for row in out_rows:
        v = int(row[0])
        assert lo <= v <= hi, f"clip({lo},{hi}) produced {v}"


@given(
    rows=st.lists(
        st.tuples(
            st.text(alphabet='abcdefghij', min_size=1, max_size=10),
            st.text(alphabet='abcdefghij', min_size=1, max_size=10),
        ),
        min_size=1, max_size=20
    )
)
@settings(max_examples=50)
def test_select_subset(rows):
    """select(['b']) keeps only the selected column."""
    data = make_csv(['a', 'b'], [[a, b] for a, b in rows])
    out = run_pipeline([tf.codec.csv(), tf.ops.select(['b']), tf.codec.csv_encode()], data)
    headers, out_rows = parse_csv_output(out)
    assert headers == ['b']
    assert len(out_rows) == len(rows)
    for out_row, (_, b_val) in zip(out_rows, rows):
        assert out_row[0] == b_val


@given(
    values=st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=20)
)
@settings(max_examples=50)
def test_passthrough_preserves_row_count(values):
    """CSV decode -> encode preserves row count."""
    rows = [[str(v)] for v in values]
    data = make_csv(['x'], rows)
    out = run_pipeline([tf.codec.csv(), tf.codec.csv_encode()], data)
    _, out_rows = parse_csv_output(out)
    assert len(out_rows) == len(values)


@given(
    values=st.lists(st.integers(min_value=-100, max_value=100), min_size=2, max_size=20)
)
@settings(max_examples=50)
def test_filter_complement(values):
    """filter(x > 0) + filter(x <= 0) covers all rows."""
    rows = [[str(v)] for v in values]
    data = make_csv(['x'], rows)

    out_pos = run_pipeline([
        tf.codec.csv(), tf.ops.filter(tf.expr("col('x') > 0")), tf.codec.csv_encode()
    ], data)
    out_neg = run_pipeline([
        tf.codec.csv(), tf.ops.filter(tf.expr("col('x') <= 0")), tf.codec.csv_encode()
    ], data)

    _, rows_pos = parse_csv_output(out_pos)
    _, rows_neg = parse_csv_output(out_neg)
    assert len(rows_pos) + len(rows_neg) == len(values), \
        f"Complement mismatch: {len(rows_pos)} + {len(rows_neg)} != {len(values)}"


@given(
    n=st.integers(min_value=1, max_value=20),
    n_rows=st.integers(min_value=0, max_value=30)
)
@settings(max_examples=50)
def test_tail_row_count(n, n_rows):
    """tail(n) produces min(n, total_rows) rows."""
    rows = [[str(i)] for i in range(n_rows)]
    data = make_csv(['x'], rows)
    out = run_pipeline([tf.codec.csv(), tf.ops.tail(n), tf.codec.csv_encode()], data)
    _, out_rows = parse_csv_output(out)
    expected = min(n, n_rows)
    assert len(out_rows) == expected


@given(
    n=st.integers(min_value=1, max_value=20),
    values=st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=30)
)
@settings(max_examples=50)
def test_tail_values(n, values):
    """tail(n) returns the last n values."""
    rows = [[str(v)] for v in values]
    data = make_csv(['x'], rows)
    out = run_pipeline([tf.codec.csv(), tf.ops.tail(n), tf.codec.csv_encode()], data)
    _, out_rows = parse_csv_output(out)
    expected = values[-n:]
    actual = [int(r[0]) for r in out_rows]
    assert actual == expected
