"""
test_properties.py -- Property-based tests for tranfi using Hypothesis.

Tests invariants that must hold for any valid input data:
- CSV roundtrip stability
- Row count invariants across transforms
- Head/skip/tail bounds
- Filter/unique/sort semantic properties
- Derive column addition
- Chunk-boundary parity for representative streaming pipelines
- CSV codec fuzz cases for quotes, invalid UTF-8 bytes, and record-size caps
- JSONL/text codec fuzz cases for valid objects, malformed diagnostics, fail-fast errors, and record-size caps
- DSL parser fuzz cases for valid pipelines, alias normalization, and clean failures
- Expression parser fuzz cases for row-local filters/derives and clean failures
- Schema validator fuzz cases for annotate/filter/quarantine row routing

Run:
    cd tranfi && source ~/tools/miniconda3/etc/profile.d/conda.sh && conda activate base && \
    TRANFI_LIB_PATH=build/libtranfi.so python -m pytest test/test_properties.py -v --tb=short
"""

import os
import sys
import csv
import io
import json
import struct
import tempfile
import pytest

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
    w = csv.writer(buf, lineterminator='\n')
    w.writerow(headers)
    for row in rows:
        w.writerow(row)
    return buf.getvalue().encode('utf-8')


def parse_csv_output(text):
    """Parse CSV output text into (headers, rows) without trimming field data."""
    if not text:
        return [], []
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return [], []
    return rows[0], rows[1:]


def make_jsonl(rows):
    """Build JSONL bytes from a sequence of JSON objects."""
    return ''.join(
        json.dumps(row, separators=(',', ':')) + '\n'
        for row in rows
    ).encode('utf-8')


def parse_jsonl_output(text):
    """Parse JSONL output text into JSON objects."""
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def float_bits(value):
    return struct.unpack('>Q', struct.pack('>d', float(value)))[0]


def compile_dsl_plan(dsl):
    """Compile DSL and parse the returned recipe JSON."""
    return json.loads(tf.compile_dsl(dsl))


def run_pipeline(steps, data, allow_blocking=False, chunk_size=None):
    """Run a pipeline and return output text."""
    p = tf.pipeline(steps)
    kwargs = {'input': data, 'allow_blocking': allow_blocking}
    if chunk_size is not None:
        kwargs['chunk_size'] = chunk_size
    result = p.run(**kwargs)
    return result.output_text


def write_temp_csv(headers, rows):
    """Write a temporary CSV lookup file and return its path."""
    f = tempfile.NamedTemporaryFile('w', suffix='.csv', delete=False, newline='')
    try:
        w = csv.writer(f, lineterminator='\n')
        w.writerow(headers)
        for row in rows:
            w.writerow(row)
        return f.name
    finally:
        f.close()


def stats_objects(stats_text):
    return [json.loads(line) for line in stats_text.splitlines() if line.strip()]


def transform_stats(stats_text, op):
    for obj in stats_objects(stats_text):
        for step in obj.get('steps', []):
            if step.get('op') == op:
                keys = [
                    'op', 'execution_target', 'memory_class', 'emit_class', 'schema_class',
                    'rows_in', 'rows_out', 'tracked_categories', 'spill_output_rows',
                    'spill_distinct_groups', 'spill_distinct_rows', 'spill_kept_rows'
                ]
                return {k: step[k] for k in keys if k in step}
    raise AssertionError(f"missing step stats for {op}: {stats_text}")


def full_transform_stats(stats_text, op):
    for obj in stats_objects(stats_text):
        for step in obj.get('steps', []):
            if step.get('op') == op:
                return step
    raise AssertionError(f"missing step stats for {op}: {stats_text}")


def run_spill_for_chunk_sizes(dsl, data, op, chunk_sizes):
    """Run one spill pipeline with several input chunk sizes and normalize volatile stats."""
    runs = []
    for chunk_size in chunk_sizes:
        with tempfile.TemporaryDirectory() as spill:
            result = tf.pipeline(dsl).run(
                input=data,
                spill_dir=spill,
                memory='1KB',
                chunk_size=chunk_size,
            )
            assert os.listdir(spill) == []
        runs.append((result.output_text, transform_stats(result.stats_text, op)))
    return runs


def assert_chunk_invariant(dsl, data, op, expected_text, expected_stats):
    chunk_sizes = [1, 2, 3, 7, 64, max(1, len(data) + 17)]
    runs = run_spill_for_chunk_sizes(dsl, data, op, chunk_sizes)
    base_output, base_stats = runs[0]
    assert base_output == expected_text
    for output, stats in runs:
        assert output == base_output
        assert stats == base_stats
    for key, value in expected_stats.items():
        assert base_stats.get(key) == value, f"{op} stat {key}: {base_stats}"




CHUNK_PARITY_SIZES = [1, 2, 3, 7, 64, 1024]


def chunk_parity_sizes(data):
    """Canonical byte chunk cuts used by the recurring streaming parity tests."""
    return CHUNK_PARITY_SIZES + [max(1, len(data))]


def normalized_step_stats(stats_text):
    """Keep deterministic contract and row-count fields from step_stats records."""
    stable_keys = [
        'index', 'op', 'execution_target',
        'memory_class', 'emit_class', 'schema_class', 'state_estimate',
        'state_bytes_estimate', 'state_bytes_reason', 'warnings',
        'rows_in', 'rows_out', 'batches_in', 'batches_out',
        'tracked_keys', 'tracked_key_bytes', 'retained_state_bytes',
        'max_state_bytes', 'tracked_values', 'overflow_count',
        'tracked_categories', 'category_value_bytes', 'category_output_name_bytes',
        'lookup_rows', 'lookup_keys', 'lookup_key_bytes', 'lookup_row_refs',
        'emitted_keys', 'emitted_key_bytes',
        'checked_rows', 'passed_rows', 'failed_rows', 'failure_rate',
        'audit_emitted', 'rule_count', 'violation_count',
        'required_failures', 'type_failures', 'nullable_failures',
        'values_failures', 'min_failures', 'max_failures', 'regex_failures',
        'schema_failures',
        'quarantined_rows', 'kept_rows', 'sampled_rows', 'total_rows',
    ]
    normalized = []
    for obj in stats_objects(stats_text):
        if obj.get('type') != 'step_stats':
            continue
        for step in obj.get('steps', []):
            normalized.append({k: step[k] for k in stable_keys if k in step})
    return normalized


def assert_pipeline_chunk_parity(pipeline_spec, data, *, allow_blocking=False,
                                 compare_raw_stats=False, expected_output=None):
    """Run a pipeline at standard byte cuts and compare output plus side channels."""
    runs = []
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(pipeline_spec).run(
            input=data,
            chunk_size=chunk_size,
            allow_blocking=allow_blocking,
        )
        stats = result.stats_text if compare_raw_stats else normalized_step_stats(result.stats_text)
        runs.append({
            'chunk_size': chunk_size,
            'output': result.output_text,
            'errors': result.errors,
            'samples': result.samples,
            'stats': stats,
        })

    base = runs[0]
    if expected_output is not None:
        assert base['output'] == expected_output
    for run in runs[1:]:
        assert run['output'] == base['output'], (base['chunk_size'], run['chunk_size'])
        assert run['errors'] == base['errors'], (base['chunk_size'], run['chunk_size'])
        assert run['samples'] == base['samples'], (base['chunk_size'], run['chunk_size'])
        assert run['stats'] == base['stats'], (base['chunk_size'], run['chunk_size'])
    return base

def first_seen_order(values):
    seen = set()
    out = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


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

csv_edge_cell = st.text(
    alphabet=list('abcxyz ABCXYZ_-,"#\n'),
    min_size=0,
    max_size=12,
)


@st.composite
def csv_edge_table(draw):
    n_cols = draw(st.integers(min_value=1, max_value=4))
    n_rows = draw(st.integers(min_value=1, max_value=8))
    row_strategy = st.lists(csv_edge_cell, min_size=n_cols, max_size=n_cols).filter(
        lambda row: any(cell.strip(' 	') != '' for cell in row)
    )
    rows = draw(st.lists(row_strategy, min_size=n_rows, max_size=n_rows))
    headers = [f'c{i}' for i in range(n_cols)]
    return headers, rows


jsonl_text = st.text(
    alphabet=list('abcxyz ABCXYZ_-. ,"\\/:\n\t'),
    min_size=0,
    max_size=16,
)


@st.composite
def jsonl_flat_rows(draw):
    rows = []
    for _ in range(draw(st.integers(min_value=1, max_value=10))):
        rows.append({
            'id': draw(st.integers(min_value=-1000, max_value=1000)),
            'name': draw(jsonl_text),
            'active': draw(st.booleans()),
            'note': draw(st.one_of(st.none(), jsonl_text)),
            'score': draw(st.integers(min_value=-1000, max_value=1000)),
        })
    return rows


dsl_base_valid_fragments = [
    ['head 0'],
    ['head 5'],
    ['skip 0'],
    ['tail 3'],
    ['slice_head n=4'],
    ['slice-tail 2'],
    ['filter "col(score) >= 0"'],
    ["filter \"contains(col(city),'N')\""],
    ['mutate doubled=col(score)*2'],
    ["derive band=case_when(col(score)<10,'low','high')"],
    ['select id,city,score'],
    ['rename city=town'],
    ['distinct city max_keys=16'],
    ['frequency city max_values=16'],
    ['rowid city result=city_row max_keys=16'],
    ['arrange -score', 'head 5'],
    ['sort score', 'head 4'],
    ['top-k 5 score'],
    ['bottom-k 5 score'],
    ['summarise city count:*:rows'],
]


def _dsl_schema_after_fragment(schema, fragment):
    """Approximate compile-time schema effects for valid-DSL generation."""
    first = fragment[0]
    if first.startswith('select id,city,score'):
        return ['id', 'city', 'score'] if schema is not None else None
    if first.startswith('select starts_with(score)'):
        return [name for name in schema if name.startswith('score')] if schema is not None else None
    if first.startswith('rename city=town'):
        return ['town' if name == 'city' else name for name in schema] if schema is not None else None
    if first.startswith('frequency '):
        return ['value', 'count']
    if first.startswith('summarise '):
        return ['city', 'rows']
    if first.startswith('mutate doubled='):
        return schema + ['doubled'] if schema is not None else None
    if first.startswith('derive band='):
        return schema + ['band'] if schema is not None else None
    if first.startswith('rowid ') and schema is not None and 'city_row' not in schema:
        return schema + ['city_row']
    return schema


def _dsl_valid_fragment_options(schema):
    options = list(dsl_base_valid_fragments)
    if schema is None or any(name.startswith('score') for name in schema):
        options.append(['select starts_with(score)'])
    if schema is None or ('score' in schema and 'city' in schema):
        options.append(['relocate score before=city'])
    return options


@st.composite
def dsl_valid_pipeline(draw):
    first = draw(st.sampled_from([
        'csv',
        'csv batch_size=1',
        'csv batch_size=3 trim_ws=false',
        'csv nulls=NA,NULL quoted_nulls=false',
    ]))
    n_fragments = draw(st.integers(min_value=1, max_value=6))
    stages = [first]
    schema = None
    for _ in range(n_fragments):
        fragment = draw(st.sampled_from(_dsl_valid_fragment_options(schema)))
        stages.extend(fragment)
        schema = _dsl_schema_after_fragment(schema, fragment)
    stages.append('csv')
    return ' | '.join(stages)


@st.composite
def dsl_alias_pipeline(draw):
    derive_op = draw(st.sampled_from(['derive', 'mutate']))
    sort_op = draw(st.sampled_from(['sort', 'arrange']))
    unique_op = draw(st.sampled_from(['unique', 'distinct']))
    group_op = draw(st.sampled_from(['group-agg', 'summarise', 'summarize']))
    slice_head = draw(st.sampled_from(['slice-head', 'slice_head']))
    slice_tail = draw(st.sampled_from(['slice-tail', 'slice_tail']))
    return (
        f'csv | {derive_op} total=col(score)*2 | '
        f'{sort_op} -total | head 3 | '
        f'{unique_op} city max_keys=16 | '
        f'{group_op} city count:*:rows | '
        f'{slice_head} n=2 | {slice_tail} 1 | csv'
    )


dsl_invalid_cases = [
    'csv || csv',
    '| csv',
    'csv |',
    'csv | head nope | csv',
    'csv | select | csv',
    'csv | rename bad | csv',
    'csv | unknownop x | csv',
    'csv | slice-min score n=2 with_ties=true | csv',
    'csv | join lookup.csv | csv',
    'csv | csv max_error_bytes=-1 | csv',
    'csv | frequency city overflow=other | csv',
    'csv | json-filter payload $.age gt | csv',
    'csv | filter "unterminated | csv',
]


expr_name = st.sampled_from(['Alice', 'Bob', 'Cara', 'Nina', 'Ana', 'Max', ''])
expr_city = st.sampled_from(['NY', 'LA', 'SF', 'SEA', 'DAL'])


@st.composite
def expr_table(draw):
    n_rows = draw(st.integers(min_value=1, max_value=12))
    rows = []
    for _ in range(n_rows):
        rows.append({
            'x': draw(st.integers(min_value=-20, max_value=20)),
            'y': draw(st.integers(min_value=-20, max_value=20)),
            'name': draw(expr_name),
            'city': draw(expr_city),
        })
    return rows


def expr_csv_data(rows):
    return make_csv(
        ['x', 'y', 'name', 'city'],
        [[str(row['x']), str(row['y']), row['name'], row['city']] for row in rows],
    )


def expr_csv_row(row):
    return [str(row['x']), str(row['y']), row['name'], row['city']]


schema_city = st.sampled_from(['NY', 'LA', 'SF', 'BOS', ''])
schema_code = st.sampled_from(['A1', 'B2', 'C3', 'bad', 'AA', ''])


@st.composite
def schema_case(draw):
    n_rows = draw(st.integers(min_value=1, max_value=12))
    lo = draw(st.integers(min_value=-10, max_value=20))
    hi = draw(st.integers(min_value=max(lo, 0), max_value=40))
    rows = []
    # Keep the first decoded batch schema deterministic for type checks.
    rows.append({
        'id': 1,
        'score': draw(st.integers(min_value=lo, max_value=hi)),
        'city': draw(st.sampled_from(['NY', 'LA', 'SF'])),
        'code': draw(st.sampled_from(['A1', 'B2', 'C3'])),
    })
    for i in range(2, n_rows + 1):
        rows.append({
            'id': i,
            'score': draw(st.one_of(st.none(), st.integers(min_value=-20, max_value=60))),
            'city': draw(schema_city),
            'code': draw(schema_code),
        })
    return rows, lo, hi


def schema_valid(row, lo, hi):
    score = row['score']
    return (
        score is not None and lo <= score <= hi and
        row['city'] in {'NY', 'LA', 'SF'} and
        len(row['code']) == 2 and row['code'][0] in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' and row['code'][1].isdigit()
    )


def schema_violation_count(row, lo, hi):
    violations = 0
    score = row['score']
    if score is None:
        violations += 1
    elif score < lo or score > hi:
        violations += 1
    if row['city'] == '':
        violations += 1
    elif row['city'] not in {'NY', 'LA', 'SF'}:
        violations += 1
    if row['code'] == '':
        violations += 1
    elif not (len(row['code']) == 2 and row['code'][0] in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' and row['code'][1].isdigit()):
        violations += 1
    return violations


def schema_csv_data(rows):
    return make_csv(
        ['id', 'score', 'city', 'code'],
        [[str(row['id']), '' if row['score'] is None else str(row['score']), row['city'], row['code']] for row in rows],
    )


def schema_csv_row(row):
    return [str(row['id']), '' if row['score'] is None else str(row['score']), row['city'], row['code']]


def schema_step(mode, lo, hi, **kwargs):
    return tf.ops.schema(
        columns={'id': 'int', 'score': 'int', 'city': 'string', 'code': 'string'},
        non_null=['id', 'score', 'city', 'code'],
        values={'city': ['NY', 'LA', 'SF']},
        min={'score': lo},
        max={'score': hi},
        regex={'code': '^[A-Z][0-9]$'},
        mode=mode,
        **kwargs,
    )


def schema_stats(result):
    stats = normalized_step_stats(result.stats_text)
    assert len(stats) == 1
    return stats[0]


@st.composite
def expr_filter_case(draw):
    case = draw(st.integers(min_value=0, max_value=4))
    if case == 0:
        threshold = draw(st.integers(min_value=-25, max_value=25))
        return f"col(x)>{threshold}", lambda row: row['x'] > threshold
    if case == 1:
        lo = draw(st.integers(min_value=-25, max_value=15))
        hi = draw(st.integers(min_value=lo, max_value=25))
        return f"between(col(x),{lo},{hi})", lambda row: lo <= row['x'] <= hi
    if case == 2:
        threshold = draw(st.integers(min_value=-25, max_value=25))
        return (
            f"if_any(col(x)>{threshold},contains(col(name),'a'))",
            lambda row: row['x'] > threshold or ('a' in row['name']),
        )
    if case == 3:
        lo = draw(st.integers(min_value=-25, max_value=15))
        hi = draw(st.integers(min_value=lo, max_value=25))
        return (
            f"if_all(col(x)>={lo},col(y)<={hi})",
            lambda row: row['x'] >= lo and row['y'] <= hi,
        )
    return (
        "(col(city)=='NY'or col(city)=='LA')and not contains(col(name),'z')",
        lambda row: row['city'] in {'NY', 'LA'} and ('z' not in row['name']),
    )


@st.composite
def expr_arithmetic_case(draw):
    case = draw(st.integers(min_value=0, max_value=4))
    if case == 0:
        return "col(x)+col(y)", lambda row: row['x'] + row['y']
    if case == 1:
        return "(col(x)+col(y))*2", lambda row: (row['x'] + row['y']) * 2
    if case == 2:
        return "if(col(x)>col(y),col(x),col(y))", lambda row: row['x'] if row['x'] > row['y'] else row['y']
    if case == 3:
        return "max(col(x),col(y),0)", lambda row: max(row['x'], row['y'], 0)
    return "min(abs(col(x)),abs(col(y)))", lambda row: min(abs(row['x']), abs(row['y']))


@st.composite
def expr_string_case(draw):
    case = draw(st.integers(min_value=0, max_value=3))
    if case == 0:
        return (
            "case_when(col(x)<0,'neg',col(x)==0,'zero','pos')",
            lambda row: 'neg' if row['x'] < 0 else ('zero' if row['x'] == 0 else 'pos'),
        )
    if case == 1:
        return (
            "case_match(col(city),'NY','east','LA','west','other')",
            lambda row: 'east' if row['city'] == 'NY' else ('west' if row['city'] == 'LA' else 'other'),
        )
    if case == 2:
        return "coalesce(col(name),'fallback')", lambda row: row['name'] if row['name'] != '' else 'fallback'
    return (
        "if(contains(col(name),'a'),'has-a','no-a')",
        lambda row: 'has-a' if 'a' in row['name'] else 'no-a',
    )


# --- Tests ---

@given(
    value=st.floats(width=64, allow_nan=False, allow_infinity=False),
    chunk_size=st.sampled_from([1, 2, 3, 7, 64]),
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_float_roundtrip_bits_csv_and_jsonl(value, chunk_size):
    data = f"x\n{repr(value)}\n".encode('ascii')
    expected = float_bits(value)

    csv_out = run_pipeline([tf.codec.csv(batch_size=1), tf.codec.csv_encode()], data, chunk_size=chunk_size)
    _, csv_rows = parse_csv_output(csv_out)
    assert len(csv_rows) == 1
    assert float_bits(float(csv_rows[0][0])) == expected

    jsonl_out = run_pipeline([tf.codec.csv(batch_size=1), tf.codec.jsonl_encode()], data, chunk_size=chunk_size)
    json_rows = [json.loads(line, parse_int=float) for line in jsonl_out.splitlines() if line.strip()]
    assert len(json_rows) == 1
    assert isinstance(json_rows[0]['x'], float)
    assert float_bits(json_rows[0]['x']) == expected



def test_csv_wide_columns_and_max_columns_chunk_boundaries():
    headers = [f'col{i}' for i in range(300)]
    row = [f'v{i}' for i in range(300)]
    data = make_csv(headers, [row])
    for chunk_size in [1, 2, 3, 7, 64, len(data)]:
        out = run_pipeline([tf.codec.csv(batch_size=1), tf.codec.csv_encode()], data, chunk_size=chunk_size)
        out_headers, out_rows = parse_csv_output(out)
        assert len(out_headers) == 300
        assert out_headers[255] == 'col255'
        assert out_headers[299] == 'col299'
        assert out_rows == [row]

    over_cap = b'a,b,c,d\n1,2,3,4\n'
    for chunk_size in [1, 2, 7, len(over_cap)]:
        with pytest.raises(RuntimeError, match='csv record exceeds max_columns'):
            run_pipeline([tf.codec.csv(max_columns=3), tf.codec.csv_encode()], over_cap, chunk_size=chunk_size)


def test_csv_typed_post_inference_materialization_chunk_boundaries():
    data = (
        b'id,score,d,ts,flag,name\n'
        b'1,1.5,2024-01-01,2024-01-01T01:02:03.123456Z,true,Alice\n'
        b'2,2.25,2024-01-02,2024-01-02,false,Bob\n'
        b'bad,,bad,bad,maybe,Charlie\n'
        b'4,4.5,2024-01-04,2024-01-04T05:06:07Z,true,Diana\n'
    )
    expected = [
        'id,score,d,ts,flag,name',
        '1,1.5,2024-01-01,2024-01-01T01:02:03.123456Z,true,Alice',
        '2,2.25,2024-01-02,2024-01-02T00:00:00Z,false,Bob',
        ',,,,maybe,Charlie',
        '4,4.5,2024-01-04,2024-01-04T05:06:07Z,true,Diana',
    ]
    for chunk_size in chunk_parity_sizes(data):
        out = run_pipeline('csv batch_size=2 | csv', data, chunk_size=chunk_size)
        assert out.strip().splitlines() == expected


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
    out = run_pipeline([tf.codec.csv(), tf.ops.sort(['x']), tf.codec.csv_encode()], data, allow_blocking=True)
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
    out = run_pipeline([tf.codec.csv(), tf.ops.sort(['x']), tf.codec.csv_encode()], data, allow_blocking=True)
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
    ], data, allow_blocking=True)
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



# --- CSV codec fuzz and boundary tests ---

@given(table=csv_edge_table())
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_csv_quoted_field_fuzz_chunk_boundaries(table):
    """CSV quotes, commas, comments, and embedded newlines must survive byte cuts."""
    headers, rows = table
    data = make_csv(headers, rows)
    baseline_stats = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline('csv batch_size=2 trim_ws=false | csv').run(
            input=data,
            chunk_size=chunk_size,
        )
        out_headers, out_rows = parse_csv_output(result.output_text)
        assert out_headers == headers
        assert out_rows == rows
        stats = normalized_step_stats(result.stats_text)
        if baseline_stats is None:
            baseline_stats = stats
        else:
            assert stats == baseline_stats


def test_csv_invalid_utf8_payload_chunk_boundaries():
    """Invalid UTF-8 bytes are payload bytes; chunking must not alter record state."""
    data = (
        b'a,b\n'
        b'alpha,\xff\xfe\n'
        b'"quo,ted",x\xffy\n'
        b'last,"line\n\xfe"\n'
    )
    outputs = []
    stats = []
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline('csv batch_size=1 trim_ws=false | csv').run(
            input=data,
            chunk_size=chunk_size,
        )
        outputs.append(result.output)
        stats.append(normalized_step_stats(result.stats_text))
    assert all(output == outputs[0] for output in outputs)
    assert all(stat == stats[0] for stat in stats)
    assert b'\xff\xfe' in outputs[0]
    assert b'x\xffy' in outputs[0]
    assert b'line\n\xfe' in outputs[0]


def test_csv_max_record_bytes_rejects_oversized_record_across_chunks():
    """The record-size guard must fail consistently no matter where chunks split."""
    data = b'a,b\nok,1\nhuge,' + (b'x' * 80) + b'\n'
    for chunk_size in chunk_parity_sizes(data):
        try:
            tf.pipeline('csv max_record_bytes=32 | csv').run(
                input=data,
                chunk_size=chunk_size,
            )
        except RuntimeError as exc:
            assert 'csv record exceeds max_record_bytes' in str(exc)
        else:
            raise AssertionError(f'max_record_bytes did not fail for chunk size {chunk_size}')


def test_jsonl_max_record_bytes_rejects_oversized_record_across_chunks():
    """JSONL record-size caps must fail consistently across chunk cuts."""
    data = b'{"id":1}\n{"name":"' + (b'x' * 80) + b'"}'
    for chunk_size in chunk_parity_sizes(data):
        try:
            tf.pipeline('jsonl max_record_bytes=32 | csv').run(
                input=data,
                chunk_size=chunk_size,
            )
        except RuntimeError as exc:
            assert 'jsonl record exceeds max_record_bytes' in str(exc)
        else:
            raise AssertionError(f'jsonl max_record_bytes did not fail for chunk size {chunk_size}')


def test_text_max_record_bytes_rejects_oversized_record_across_chunks():
    """Text record-size caps must bound long no-newline lines across chunk cuts."""
    data = b'ok\n' + (b'x' * 80)
    for chunk_size in chunk_parity_sizes(data):
        try:
            tf.pipeline('text max_record_bytes=32 | text').run(
                input=data,
                chunk_size=chunk_size,
            )
        except RuntimeError as exc:
            assert 'text record exceeds max_record_bytes' in str(exc)
        else:
            raise AssertionError(f'text max_record_bytes did not fail for chunk size {chunk_size}')


# --- JSONL codec fuzz and boundary tests ---

@given(rows=jsonl_flat_rows())
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_jsonl_valid_object_fuzz_chunk_boundaries(rows):
    """Valid JSONL object records must round-trip across byte cuts."""
    data = make_jsonl(rows)
    baseline_stats = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(
            'jsonl batch_size=2 | select id,name,active,note,score | jsonl'
        ).run(input=data, chunk_size=chunk_size)
        assert parse_jsonl_output(result.output_text) == rows
        assert result.errors == b''
        stats = normalized_step_stats(result.stats_text)
        if baseline_stats is None:
            baseline_stats = stats
        else:
            assert stats == baseline_stats
    assert baseline_stats[0]['op'] == 'select'
    assert baseline_stats[0]['memory_class'] == 'row_local'


def test_jsonl_malformed_warn_diagnostics_chunk_boundaries():
    """Malformed and non-object records must keep stable line diagnostics."""
    data = (
        b'{"id":1,"name":"ok"}\n'
        b'not-json-not-json\n'
        b'[1,2,3]\n'
        b'{"id":2,"name":"last"}'
    )
    baseline_output = None
    baseline_errors = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(
            'jsonl batch_size=1 on_error=warn max_error_bytes=8 | jsonl'
        ).run(input=data, chunk_size=chunk_size)
        assert parse_jsonl_output(result.output_text) == [
            {'id': 1, 'name': 'ok'},
            {'id': 2, 'name': 'last'},
        ]
        errors_text = result.errors.decode('utf-8')
        if baseline_output is None:
            baseline_output = result.output_text
            baseline_errors = errors_text
        else:
            assert result.output_text == baseline_output
            assert errors_text == baseline_errors

    diagnostics = parse_jsonl_output(baseline_errors)
    assert [diag['line'] for diag in diagnostics] == [2, 3]
    assert [diag['message'] for diag in diagnostics] == [
        'invalid JSON',
        'JSONL record is not an object',
    ]
    assert all(diag['type'] == 'jsonl_malformed' for diag in diagnostics)
    assert all(diag['action'] == 'warn' for diag in diagnostics)
    assert diagnostics[0]['raw_bytes'] > 8
    assert diagnostics[0]['raw'] == 'not-json'
    assert diagnostics[0]['truncated'] is True
    assert diagnostics[1]['raw'] == '[1,2,3]'


def test_jsonl_fail_policy_reports_stable_line_across_chunks():
    """Fail-fast JSONL decode must stop at the same logical line for every chunk cut."""
    data = b'{"id":1}\nnot-json\n{"id":2}\n'
    messages = []
    for chunk_size in chunk_parity_sizes(data):
        try:
            tf.pipeline(
                'jsonl batch_size=1 on_error=fail max_error_bytes=6 | jsonl'
            ).run(input=data, chunk_size=chunk_size)
        except RuntimeError as exc:
            messages.append(str(exc))
        else:
            raise AssertionError(f'jsonl fail policy did not fail for chunk size {chunk_size}')
    assert len(set(messages)) == 1
    assert messages[0] == 'Push failed: jsonl decode failed at line 2: invalid JSON'


# --- DSL parser fuzz tests ---

@given(dsl=dsl_valid_pipeline())
@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_dsl_parser_valid_pipeline_fuzz(dsl):
    """Generated valid DSL pipelines must compile to metadata-rich canonical IR."""
    plan = compile_dsl_plan(dsl)
    steps = plan['steps']
    assert steps[0]['op'] == 'codec.csv.decode'
    assert steps[-1]['op'] == 'codec.csv.encode'
    assert len(steps) >= 3
    for step in steps:
        assert isinstance(step.get('args'), dict)
        assert step['memory_class'] in {
            'row_local', 'bounded_state', 'key_state', 'blocking', 'external',
        }
        assert step['emit_class'] in {'per_batch', 'on_flush', 'side_only', 'mixed'}
        assert step['schema_class'] in {'stable', 'parametric', 'data_dependent'}
        assert isinstance(step['state_estimate'], str) and step['state_estimate']


@given(dsl=dsl_alias_pipeline())
@settings(max_examples=30, deadline=None)
def test_dsl_parser_alias_normalization_fuzz(dsl):
    """Dataframe-style aliases must canonicalize before validation/metadata."""
    plan = compile_dsl_plan(dsl)
    ops = [step['op'] for step in plan['steps']]
    assert 'derive' in ops
    assert 'top' in ops
    assert 'unique' in ops
    assert 'group-agg' in ops
    assert 'slice-head' in ops
    assert 'slice-tail' in ops
    assert not ({'mutate', 'arrange', 'distinct', 'summarise', 'summarize',
                 'slice_head', 'slice_tail'} & set(ops))


@given(
    prefix=st.text(alphabet=[' ', '\t', '\n'], min_size=0, max_size=3),
    dsl=st.sampled_from(dsl_invalid_cases),
    suffix=st.text(alphabet=[' ', '\t', '\n'], min_size=0, max_size=3),
)
@settings(max_examples=80, deadline=None)
def test_dsl_parser_invalid_inputs_fail_cleanly(prefix, dsl, suffix):
    """Malformed DSL should report a clean compile error, not corrupt strings or crash."""
    try:
        tf.compile_dsl(prefix + dsl + suffix)
    except RuntimeError as exc:
        msg = str(exc)
        assert msg.startswith('DSL compile failed: ')
        assert len(msg) > len('DSL compile failed: ')
        assert '\ufffd' not in msg
    else:
        raise AssertionError(f'invalid DSL compiled successfully: {dsl!r}')

# --- Expression parser fuzz tests ---

@given(rows=expr_table(), case=expr_filter_case())
@settings(max_examples=45, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_expr_parser_filter_fuzz_matches_python_oracle(rows, case):
    """Generated row-local predicates must match a Python oracle across byte cuts."""
    expr, predicate = case
    data = expr_csv_data(rows)
    expected_rows = [expr_csv_row(row) for row in rows if predicate(row)]
    baseline_stats = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(f'csv batch_size=2 | filter "{expr}" | csv').run(
            input=data,
            chunk_size=chunk_size,
        )
        headers, out_rows = parse_csv_output(result.output_text)
        if expected_rows:
            assert headers == ['x', 'y', 'name', 'city']
            assert out_rows == expected_rows
        else:
            assert headers == []
            assert out_rows == []
        stats = normalized_step_stats(result.stats_text)
        if baseline_stats is None:
            baseline_stats = stats
        else:
            assert stats == baseline_stats
    assert baseline_stats[0]['op'] == 'filter'
    assert baseline_stats[0]['memory_class'] == 'row_local'


@given(rows=expr_table(), case=expr_arithmetic_case())
@settings(max_examples=45, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_expr_parser_derive_arithmetic_fuzz_matches_python_oracle(rows, case):
    """Generated arithmetic/conditional derives must preserve parser precedence."""
    expr, evaluator = case
    data = expr_csv_data(rows)
    expected_rows = [[str(row['x']), str(row['y']), str(evaluator(row))] for row in rows]
    baseline_stats = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(f'csv batch_size=2 | derive z={expr} | select x,y,z | csv').run(
            input=data,
            chunk_size=chunk_size,
        )
        headers, out_rows = parse_csv_output(result.output_text)
        assert headers == ['x', 'y', 'z']
        assert out_rows == expected_rows
        stats = normalized_step_stats(result.stats_text)
        if baseline_stats is None:
            baseline_stats = stats
        else:
            assert stats == baseline_stats
    assert baseline_stats[0]['op'] == 'derive'
    assert baseline_stats[0]['memory_class'] == 'row_local'


@given(rows=expr_table(), case=expr_string_case())
@settings(max_examples=45, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_expr_parser_derive_string_fuzz_matches_python_oracle(rows, case):
    """Generated conditional/string derives must match row-local Python semantics."""
    expr, evaluator = case
    data = expr_csv_data(rows)
    expected_rows = [[row['name'], row['city'], str(row['x']), evaluator(row)] for row in rows]
    baseline_stats = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(f'csv batch_size=2 | derive out={expr} | select name,city,x,out | csv').run(
            input=data,
            chunk_size=chunk_size,
        )
        headers, out_rows = parse_csv_output(result.output_text)
        assert headers == ['name', 'city', 'x', 'out']
        assert out_rows == expected_rows
        stats = normalized_step_stats(result.stats_text)
        if baseline_stats is None:
            baseline_stats = stats
        else:
            assert stats == baseline_stats
    assert baseline_stats[0]['op'] == 'derive'
    assert baseline_stats[0]['memory_class'] == 'row_local'


def test_expr_parser_date_functions_chunk_boundaries_match_oracle():
    """Row-local date/time expressions must not depend on input chunk boundaries."""
    data = make_csv(
        ['d', 'ts'],
        [
            ['2024-03-15', '2024-03-15T12:34:56.123456Z'],
            ['2020-02-29', '2020-02-29T00:00:00.120000Z'],
            ['1969-12-31', '1969-12-31T23:59:59.999999Z'],
            ['bad', 'not-a-time'],
        ],
    )
    dsl = (
        "csv batch_size=2 | derive "
        "dy=year(col(d)) "
        "dm=month(col(d)) "
        "dd=day(col(d)) "
        "dw=weekday(col(d)) "
        "de=epoch(col(d)) "
        "ty=year(col(ts)) "
        "th=hour(col(ts)) "
        "ts_epoch=epoch(col(ts)) "
        "month_start=date_trunc(col(d),'month') "
        "minute_start=date_trunc(col(ts),'minute') "
        "| select d,ts,dy,dm,dd,dw,de,ty,th,ts_epoch,month_start,minute_start | csv"
    )
    expected = make_csv(
        ['d', 'ts', 'dy', 'dm', 'dd', 'dw', 'de', 'ty', 'th', 'ts_epoch', 'month_start', 'minute_start'],
        [
            ['2024-03-15', '2024-03-15T12:34:56.123456Z', '2024', '3', '15', '5', '1710460800', '2024', '12', '1710506096', '2024-03-01', '2024-03-15T12:34:00Z'],
            ['2020-02-29', '2020-02-29T00:00:00.12Z', '2020', '2', '29', '6', '1582934400', '2020', '0', '1582934400', '2020-02-01', '2020-02-29T00:00:00Z'],
            ['1969-12-31', '1969-12-31T23:59:59.999999Z', '1969', '12', '31', '3', '-86400', '1969', '23', '-1', '1969-12-01', '1969-12-31T23:59:00Z'],
            ['', '', '', '', '', '', '', '', '', '', '', ''],
        ],
    ).decode('utf-8')
    baseline_stats = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(dsl).run(input=data, chunk_size=chunk_size)
        assert result.output_text == expected
        stats = normalized_step_stats(result.stats_text)
        if baseline_stats is None:
            baseline_stats = stats
        else:
            assert stats == baseline_stats
    assert [step['op'] for step in baseline_stats] == ['derive', 'select']
    assert all(step['memory_class'] == 'row_local' for step in baseline_stats)

    filter_dsl = (
        "csv batch_size=2 | filter \"year(col(d))==2024 or epoch(col(ts))<0\" "
        "| select d,ts | csv"
    )
    filter_expected = make_csv(
        ['d', 'ts'],
        [
            ['2024-03-15', '2024-03-15T12:34:56.123456Z'],
            ['1969-12-31', '1969-12-31T23:59:59.999999Z'],
        ],
    ).decode('utf-8')
    baseline_filter_stats = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(filter_dsl).run(input=data, chunk_size=chunk_size)
        assert result.output_text == filter_expected
        stats = normalized_step_stats(result.stats_text)
        if baseline_filter_stats is None:
            baseline_filter_stats = stats
        else:
            assert stats == baseline_filter_stats
    assert [step['op'] for step in baseline_filter_stats] == ['filter', 'select']
    assert all(step['memory_class'] == 'row_local' for step in baseline_filter_stats)


@given(
    prefix=st.text(alphabet=[' ', '\t', '\n'], min_size=0, max_size=2),
    expr=st.sampled_from([
        'col(x)>',
        'between(col(x),1,)',
        'contains(col(name),)',
        '(col(x)>0',
        'col()',
    ]),
    suffix=st.text(alphabet=[' ', '\t', '\n'], min_size=0, max_size=2),
)
@settings(max_examples=30, deadline=None)
def test_expr_parser_invalid_inputs_fail_cleanly(prefix, expr, suffix):
    """Malformed expressions should set fresh, bounded, UTF-8-safe errors."""
    data = b'x,y,name,city\n1,2,Alice,NY\n'
    messages = []
    for _ in range(3):
        try:
            tf.pipeline(f'csv | filter "{prefix}{expr}{suffix}" | csv').run(input=data)
        except RuntimeError as exc:
            msg = str(exc)
            messages.append(msg)
            assert msg.startswith("Failed to create pipeline: failed to create 'filter': expression: ")
            assert len(msg) < 160
            assert '\ufffd' not in msg
        else:
            raise AssertionError(f'invalid expression compiled successfully: {expr!r}')
    assert messages == [messages[0]] * len(messages)

# --- Schema validator fuzz tests ---

@given(case=schema_case())
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_schema_validator_annotate_fuzz_matches_python_oracle(case):
    """Generated schema annotate runs must match a row-local Python oracle across byte cuts."""
    rows, lo, hi = case
    data = schema_csv_data(rows)
    expected_rows = [schema_csv_row(row) + ['true' if schema_valid(row, lo, hi) else 'false'] for row in rows]
    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline([
            tf.codec.csv(batch_size=2),
            schema_step('annotate', lo, hi, result='schema_ok'),
            tf.codec.csv_encode(),
        ]).run(input=data, chunk_size=chunk_size)
        headers, out_rows = parse_csv_output(result.output_text)
        assert headers == ['id', 'score', 'city', 'code', 'schema_ok']
        assert out_rows == expected_rows
        assert result.errors == b''
        current = (out_rows, schema_stats(result))
        if baseline is None:
            baseline = current
        else:
            assert current == baseline
    stats = baseline[1]
    assert stats['op'] == 'schema'
    assert stats['memory_class'] == 'row_local'
    assert stats['emit_class'] == 'mixed'
    assert stats['rows_out'] == len(rows)
    assert stats['checked_rows'] == len(rows)
    assert stats['passed_rows'] == sum(schema_valid(row, lo, hi) for row in rows)
    assert stats['failed_rows'] == len(rows) - stats['passed_rows']
    assert stats.get('violation_count', 0) == sum(schema_violation_count(row, lo, hi) for row in rows)


@given(case=schema_case())
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_schema_validator_filter_audit_fuzz_matches_python_oracle(case):
    """Generated schema filter runs must drop exactly invalid rows and cap audit output."""
    rows, lo, hi = case
    data = schema_csv_data(rows)
    expected_rows = [schema_csv_row(row) for row in rows if schema_valid(row, lo, hi)]
    expected_failed = len(rows) - len(expected_rows)
    expected_violations = sum(schema_violation_count(row, lo, hi) for row in rows)
    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline([
            tf.codec.csv(batch_size=2),
            schema_step('filter', lo, hi, audit=True, audit_limit=2, name='schema_check', message='schema rule'),
            tf.codec.csv_encode(),
        ]).run(input=data, chunk_size=chunk_size)
        headers, out_rows = parse_csv_output(result.output_text)
        if expected_rows:
            assert headers == ['id', 'score', 'city', 'code']
            assert out_rows == expected_rows
        else:
            assert headers == []
            assert out_rows == []
        assert result.errors == b''
        audit_rows = [obj for obj in stats_objects(result.stats_text) if obj.get('type') == 'audit']
        assert len(audit_rows) == min(2, expected_failed)
        assert all(obj.get('op') == 'schema' and obj.get('reason') == 'schema_failed' for obj in audit_rows)
        current = (out_rows, audit_rows, schema_stats(result))
        if baseline is None:
            baseline = current
        else:
            assert current == baseline
    stats = baseline[2]
    assert stats['checked_rows'] == len(rows)
    assert stats['rows_out'] == len(expected_rows)
    assert stats['failed_rows'] == expected_failed
    assert stats.get('violation_count', 0) == expected_violations
    assert stats.get('audit_emitted', 0) == min(2, expected_failed)


@given(case=schema_case())
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_schema_validator_quarantine_fuzz_matches_python_oracle(case):
    """Generated schema quarantine runs must route invalid rows to errors across byte cuts."""
    rows, lo, hi = case
    data = schema_csv_data(rows)
    expected_rows = [schema_csv_row(row) for row in rows if schema_valid(row, lo, hi)]
    expected_failed = len(rows) - len(expected_rows)
    expected_violations = sum(schema_violation_count(row, lo, hi) for row in rows)
    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline([
            tf.codec.csv(batch_size=2),
            schema_step('quarantine', lo, hi, name='schema_check', message='schema rule'),
            tf.codec.csv_encode(),
        ]).run(input=data, chunk_size=chunk_size)
        headers, out_rows = parse_csv_output(result.output_text)
        if expected_rows:
            assert headers == ['id', 'score', 'city', 'code']
            assert out_rows == expected_rows
        else:
            assert headers == []
            assert out_rows == []
        errors = parse_jsonl_output(result.errors.decode('utf-8'))
        assert len(errors) == expected_violations
        assert all(obj.get('type') == 'schema_failure' and obj.get('action') == 'quarantine' for obj in errors)
        current = (out_rows, errors, schema_stats(result))
        if baseline is None:
            baseline = current
        else:
            assert current == baseline
    stats = baseline[2]
    assert stats['checked_rows'] == len(rows)
    assert stats['rows_out'] == len(expected_rows)
    assert stats['failed_rows'] == expected_failed
    assert stats.get('violation_count', 0) == expected_violations

# --- Broad chunk-boundary parity tests for streaming paths ---

def test_chunk_boundary_parity_streaming_csv_row_local_and_audit():
    """Row-local CSV output and bounded audit stats must be byte-cut invariant."""
    data = (
        b'id,city,score,note\n'
        b'1,NY,10,"quoted\nline"\n'
        b'2,SF,3,bad\n'
        b'3,NY,7,"ok ""quoted"""\n'
        b'4,LA,1,bad\n'
        b'5,SF,8,ok\n'
    )
    expected = 'id,city,doubled\n1,NY,20\n3,NY,14\n5,SF,16\n'
    base = assert_pipeline_chunk_parity(
        'csv batch_size=2 | filter "col(score) >= 7" audit audit_limit=2 | derive doubled=col(score)*2 | select id,city,doubled | csv',
        data,
        compare_raw_stats=True,
        expected_output=expected,
    )
    assert '"event":"row_dropped"' in base['stats']
    assert '"row":2' in base['stats']
    assert '"row":4' in base['stats']


def test_chunk_boundary_parity_bounded_state_pipeline():
    """Lag, rolling windows, rleid, and tail must not depend on input byte cuts."""
    data = make_csv(
        ['city', 'score'],
        [
            ['NY', '10'], ['NY', '20'], ['SF', '5'], ['SF', '15'],
            ['SF', '25'], ['LA', '7'], ['LA', '14'], ['NY', '30'],
        ],
    )
    base = assert_pipeline_chunk_parity(
        'csv batch_size=2 | lag score 2 prev_score | rolling-sum score 3 sum3 | rolling-mean score 3 mean3 | rleid city result=city_run | tail 5 | csv',
        data,
    )
    assert base['output'].splitlines()[0] == 'city,score,prev_score,sum3,mean3,city_run'
    assert len(parse_csv_output(base['output'])[1]) == 5
    ops = [step['op'] for step in base['stats']]
    assert ops == ['lag', 'rolling-sum', 'rolling-mean', 'rleid', 'tail']
    assert all(step['memory_class'] == 'bounded_state' for step in base['stats'])


def test_chunk_boundary_parity_key_state_capped_pipeline():
    """Capped exact key-state output and measured stats must be byte-cut invariant."""
    data = make_csv(
        ['city', 'score'],
        [
            ['NY', '10'], ['SF', '3'], ['NY', '7'], ['LA', '1'],
            ['SF', '8'], ['LA', '2'], ['SEA', '5'], ['NY', '6'],
        ],
    )
    base = assert_pipeline_chunk_parity(
        'csv batch_size=2 | rowid city result=city_row max_keys=4 | frequency city max_values=4 | csv',
        data,
    )
    headers, rows = parse_csv_output(base['output'])
    assert headers == ['value', 'count']
    assert sorted(rows) == sorted([['NY', '3'], ['SF', '2'], ['LA', '2'], ['SEA', '1']])
    rowid_stats, frequency_stats = base['stats']
    assert rowid_stats['op'] == 'rowid'
    assert rowid_stats['tracked_keys'] == 4
    assert frequency_stats['op'] == 'frequency'
    assert frequency_stats['tracked_values'] == 4
    assert frequency_stats['rows_out'] == 4


def hll_allowed_error(cardinality):
    """Conservative deterministic error envelope for stats distinct HLL estimates."""
    if cardinality <= 64:
        return max(2, int(cardinality * 0.08))
    if cardinality <= 512:
        return max(8, int(cardinality * 0.12))
    return max(32, int(cardinality * 0.20))


def stats_distinct_run(values, chunk_size):
    data = make_csv(['id'], [[value] for value in values])
    result = tf.pipeline('csv batch_size=17 | stats count,distinct | csv').run(
        input=data,
        chunk_size=chunk_size,
    )
    headers, rows = parse_csv_output(result.output_text)
    assert headers == ['column', 'count', 'distinct']
    assert len(rows) == 1
    assert rows[0][0] == 'id'
    stats = full_transform_stats(result.stats_text, 'stats')
    return {
        'output': result.output_text,
        'count': int(rows[0][1]),
        'distinct': int(rows[0][2]),
        'stats': {
            'memory_class': stats.get('memory_class'),
            'emit_class': stats.get('emit_class'),
            'schema_class': stats.get('schema_class'),
            'rows_in': stats.get('rows_in'),
            'rows_out': stats.get('rows_out'),
        },
    }


def assert_stats_distinct_hll_invariant(values):
    data = make_csv(['id'], [[value] for value in values])
    expected_count = len(values)
    expected_distinct = len(set(values))
    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        current = stats_distinct_run(values, chunk_size)
        if baseline is None:
            baseline = current
        else:
            assert current == baseline
    assert baseline['count'] == expected_count
    assert abs(baseline['distinct'] - expected_distinct) <= hll_allowed_error(expected_distinct)
    assert baseline['stats'] == {
        'memory_class': 'bounded_state',
        'emit_class': 'on_flush',
        'schema_class': 'parametric',
        'rows_in': expected_count,
        'rows_out': 1,
    }


def test_stats_distinct_hll_known_cardinalities_chunk_boundaries():
    """HLL distinct estimates should stay stable and bounded for known cardinalities."""
    for cardinality in [1, 2, 3, 7, 16, 31, 64, 127, 256, 511, 1024, 2048, 4096, 8192]:
        values = [f'k{i:05d}' for i in range(cardinality)]
        values = values * 2 + values[:max(1, min(cardinality, 13))]
        assert_stats_distinct_hll_invariant(values)


@given(
    values=st.lists(
        st.text(alphabet=list('abcdef0123456789_-'), min_size=1, max_size=10),
        min_size=1,
        max_size=80,
    )
)
@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_stats_distinct_hll_generated_duplicates_chunk_boundaries(values):
    """Generated duplicate patterns should not change stats distinct across byte cuts."""
    assert_stats_distinct_hll_invariant(values)


@given(
    cities=st.lists(st.sampled_from(['NY', 'LA', 'SF', 'SEA', 'CHI', 'DAL']), min_size=1, max_size=30)
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_rowid_unsorted_chunk_boundaries_match_oracle(cities):
    """Exact grouped rowid must count per key consistently across byte cuts."""
    counts = {}
    expected_rows = []
    for idx, city in enumerate(cities, start=1):
        counts[city] = counts.get(city, 0) + 1
        expected_rows.append([str(idx), city, str(counts[city])])
    data = make_csv(['id', 'city'], [[str(i + 1), city] for i, city in enumerate(cities)])
    expected = make_csv(['id', 'city', 'city_row'], expected_rows).decode('utf-8')

    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(
            'csv batch_size=2 | rowid city result=city_row max_keys=8 | csv'
        ).run(input=data, chunk_size=chunk_size)
        stats = full_transform_stats(result.stats_text, 'rowid')
        current = (
            result.output_text,
            stats.get('tracked_keys'),
            stats.get('rows_in'),
            stats.get('rows_out'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == expected
        assert stats.get('tracked_keys') == len(set(cities))
        assert stats.get('rows_in') == len(cities)
        assert stats.get('rows_out') == len(cities)


def adjacent_run_numbers(keys):
    run = 0
    prev = object()
    out = []
    for key in keys:
        if key != prev:
            run += 1
            prev = key
        out.append(run)
    return out


def adjacent_run_lengths(keys):
    prev = object()
    count = 0
    out = []
    for key in keys:
        count = count + 1 if key == prev else 1
        prev = key
        out.append(count)
    return out


@given(n_rows=st.integers(min_value=1, max_value=30))
@settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_rowid_global_chunk_boundaries_match_oracle(n_rows):
    """Global rowid must be O(1) bounded state and stable across byte cuts."""
    data = make_csv(['id'], [[str(i + 1)] for i in range(n_rows)])
    expected = make_csv(
        ['id', 'row_n'],
        [[str(i + 1), str(i + 1)] for i in range(n_rows)],
    ).decode('utf-8')
    plan = compile_dsl_plan('csv | rowid result=row_n | csv')
    assert plan['steps'][1]['memory_class'] == 'bounded_state'
    assert plan['steps'][1]['state_estimate'] == 'O(1)'

    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline('csv batch_size=2 | rowid result=row_n | csv').run(
            input=data,
            chunk_size=chunk_size,
        )
        stats = full_transform_stats(result.stats_text, 'rowid')
        current = (
            result.output_text,
            stats.get('memory_class'),
            stats.get('state_estimate'),
            stats.get('rows_in'),
            stats.get('rows_out'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == expected
        assert stats.get('memory_class') == 'bounded_state'
        assert stats.get('state_estimate') == 'O(1)'
        assert stats.get('rows_in') == n_rows
        assert stats.get('rows_out') == n_rows


@given(
    cities=st.lists(st.sampled_from(['NY', 'LA', 'SF', 'SEA', 'CHI', 'DAL']), min_size=1, max_size=30)
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_rowid_sorted_chunk_boundaries_match_run_oracle(cities):
    """Sorted rowid must count within adjacent runs without key-state metadata."""
    run_lengths = adjacent_run_lengths(cities)
    data = make_csv(['id', 'city'], [[str(i + 1), city] for i, city in enumerate(cities)])
    expected = make_csv(
        ['id', 'city', 'run_row'],
        [[str(i + 1), city, str(run_lengths[i])] for i, city in enumerate(cities)],
    ).decode('utf-8')
    plan = compile_dsl_plan('csv | rowid city result=run_row sorted=true | csv')
    assert plan['steps'][1]['memory_class'] == 'bounded_state'
    assert plan['steps'][1]['state_estimate'] == 'O(previous_key + counter)'

    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(
            'csv batch_size=2 | rowid city result=run_row sorted=true | csv'
        ).run(input=data, chunk_size=chunk_size)
        stats = full_transform_stats(result.stats_text, 'rowid')
        current = (
            result.output_text,
            stats.get('memory_class'),
            stats.get('state_estimate'),
            stats.get('rows_in'),
            stats.get('rows_out'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == expected
        assert stats.get('memory_class') == 'bounded_state'
        assert stats.get('state_estimate') == 'O(previous_key + counter)'
        assert stats.get('rows_in') == len(cities)
        assert stats.get('rows_out') == len(cities)


@given(
    cities=st.lists(st.sampled_from(['NY', 'LA', 'SF', 'SEA', 'CHI', 'DAL']), min_size=1, max_size=30)
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_rleid_chunk_boundaries_match_run_oracle(cities):
    """Run IDs must increment only when adjacent keys change across byte cuts."""
    run_ids = adjacent_run_numbers(cities)
    data = make_csv(['id', 'city'], [[str(i + 1), city] for i, city in enumerate(cities)])
    expected = make_csv(
        ['id', 'city', 'run_id'],
        [[str(i + 1), city, str(run_ids[i])] for i, city in enumerate(cities)],
    ).decode('utf-8')

    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline('csv batch_size=2 | rleid city result=run_id | csv').run(
            input=data,
            chunk_size=chunk_size,
        )
        stats = full_transform_stats(result.stats_text, 'rleid')
        current = (
            result.output_text,
            stats.get('memory_class'),
            stats.get('state_estimate'),
            stats.get('rows_in'),
            stats.get('rows_out'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == expected
        assert stats.get('memory_class') == 'bounded_state'
        assert stats.get('state_estimate') == 'O(key_width)'
        assert stats.get('rows_in') == len(cities)
        assert stats.get('rows_out') == len(cities)


def unique_first_rows(rows):
    expected = []
    seen = set()
    for row in rows:
        key = row[1]
        if key in seen:
            continue
        seen.add(key)
        expected.append(row)
    return expected


@given(
    cities=st.lists(st.sampled_from(['NY', 'LA', 'SF', 'SEA', 'CHI', 'DAL']), min_size=1, max_size=30)
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_unique_unsorted_chunk_boundaries_match_oracle(cities):
    """Exact unique must keep the first row for each key across byte cuts."""
    rows = [[str(i + 1), city, f'name{i + 1}'] for i, city in enumerate(cities)]
    expected_rows = unique_first_rows(rows)
    data = make_csv(['id', 'city', 'name'], rows)
    expected = make_csv(['id', 'city', 'name'], expected_rows).decode('utf-8')

    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline('csv batch_size=2 | unique city max_keys=8 | csv').run(
            input=data,
            chunk_size=chunk_size,
        )
        stats = full_transform_stats(result.stats_text, 'unique')
        current = (
            result.output_text,
            stats.get('tracked_keys'),
            stats.get('rows_in'),
            stats.get('rows_out'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == expected
        assert stats.get('tracked_keys') == len(expected_rows)
        assert stats.get('rows_in') == len(rows)
        assert stats.get('rows_out') == len(expected_rows)


@given(
    cities=st.lists(st.sampled_from(['NY', 'LA', 'SF', 'SEA', 'CHI', 'DAL']), min_size=1, max_size=30)
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_dedup_unsorted_chunk_boundaries_match_oracle(cities):
    """The dedup alias must preserve first-row unique semantics and stats."""
    rows = [[str(i + 1), city, f'name{i + 1}'] for i, city in enumerate(cities)]
    expected_rows = unique_first_rows(rows)
    data = make_csv(['id', 'city', 'name'], rows)
    expected = make_csv(['id', 'city', 'name'], expected_rows).decode('utf-8')

    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline('csv batch_size=2 | dedup city max_keys=8 | csv').run(
            input=data,
            chunk_size=chunk_size,
        )
        stats = full_transform_stats(result.stats_text, 'dedup')
        current = (
            result.output_text,
            stats.get('tracked_keys'),
            stats.get('rows_in'),
            stats.get('rows_out'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == expected
        assert stats.get('tracked_keys') == len(expected_rows)
        assert stats.get('rows_in') == len(rows)
        assert stats.get('rows_out') == len(expected_rows)


def unique_adjacent_first_rows(rows):
    expected = []
    prev = object()
    for row in rows:
        key = row[1]
        if key != prev:
            expected.append(row)
            prev = key
    return expected


@given(
    cities=st.lists(st.sampled_from(['NY', 'LA', 'SF', 'SEA', 'CHI', 'DAL']), min_size=1, max_size=30)
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_unique_sorted_chunk_boundaries_match_run_oracle(cities):
    """Sorted unique must collapse only adjacent duplicate runs across byte cuts."""
    rows = [[str(i + 1), city, f'name{i + 1}'] for i, city in enumerate(cities)]
    expected_rows = unique_adjacent_first_rows(rows)
    data = make_csv(['id', 'city', 'name'], rows)
    expected = make_csv(['id', 'city', 'name'], expected_rows).decode('utf-8')

    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline('csv batch_size=2 | unique city sorted=true | csv').run(
            input=data,
            chunk_size=chunk_size,
        )
        stats = full_transform_stats(result.stats_text, 'unique')
        current = (
            result.output_text,
            stats.get('memory_class'),
            stats.get('state_estimate'),
            stats.get('rows_in'),
            stats.get('rows_out'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == expected
        assert stats.get('memory_class') == 'bounded_state'
        assert stats.get('state_estimate') == 'O(previous_key)'
        assert stats.get('rows_in') == len(rows)
        assert stats.get('rows_out') == len(expected_rows)


@given(
    rows=st.lists(
        st.tuples(
            st.sampled_from(['NY', 'LA', 'SF', 'SEA', 'CHI', 'DAL']),
            st.integers(min_value=-20, max_value=20),
        ),
        min_size=1,
        max_size=30,
    )
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_group_agg_sorted_chunk_boundaries_match_run_oracle(rows):
    """Sorted group-agg must aggregate adjacent key runs, not global distinct keys."""
    expected_rows = []
    current_key = None
    current_sum = 0
    current_count = 0
    for city, amount in rows:
        if current_key is not None and city != current_key:
            expected_rows.append([current_key, str(current_sum), str(current_count)])
            current_sum = 0
            current_count = 0
        current_key = city
        current_sum += amount
        current_count += 1
    expected_rows.append([current_key, str(current_sum), str(current_count)])

    data = make_csv(['city', 'amount'], [[city, str(amount)] for city, amount in rows])
    expected = make_csv(['city', 'total', 'rows'], expected_rows).decode('utf-8')

    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(
            'csv batch_size=2 | group-agg city sum:amount:total count:*:rows sorted=true | csv'
        ).run(input=data, chunk_size=chunk_size)
        stats = full_transform_stats(result.stats_text, 'group-agg')
        current = (
            result.output_text,
            stats.get('memory_class'),
            stats.get('emit_class'),
            stats.get('state_estimate'),
            stats.get('rows_in'),
            stats.get('rows_out'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == expected
        assert stats.get('memory_class') == 'bounded_state'
        assert stats.get('emit_class') == 'mixed'
        assert stats.get('state_estimate') == 'O(current_group + aggregate_accumulators)'
        assert stats.get('rows_in') == len(rows)
        assert stats.get('rows_out') == len(expected_rows)


@given(
    values=st.lists(st.sampled_from(['A', 'B', 'C', 'D', 'E', 'F', 'G']), min_size=1, max_size=30),
    max_values=st.integers(min_value=1, max_value=6),
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_frequency_approx_chunk_boundaries_match_counter_bounds(values, max_values):
    """Approximate frequency must stay bounded and report valid counter error bounds."""
    data = make_csv(['city'], [[value] for value in values])
    truth = {value: values.count(value) for value in set(values)}
    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(
            f'csv batch_size=2 | frequency city mode=approx max_values={max_values} | csv'
        ).run(input=data, chunk_size=chunk_size)
        headers, rows = parse_csv_output(result.output_text)
        stats = full_transform_stats(result.stats_text, 'frequency')
        current = (
            result.output_text,
            stats.get('tracked_values'),
            stats.get('approx_replacements'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert headers == ['value', 'count', 'error']
        assert len(rows) <= max_values
        assert stats.get('approximate') is True
        assert stats.get('tracked_values') == len(rows)
        assert sum(int(row[1]) for row in rows) == len(values)
        for value, count_text, error_text in rows:
            count = int(count_text)
            error = int(error_text)
            actual = truth[value]
            assert error >= 0
            assert count >= actual
            assert count - error <= actual


@given(
    n_rows=st.integers(min_value=1, max_value=30),
    n=st.integers(min_value=1, max_value=10),
    seed=st.integers(min_value=0, max_value=1000),
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_sample_seeded_chunk_boundaries_match_subset_invariant(n_rows, n, seed):
    """Seeded reservoir sampling must be deterministic across byte chunk cuts."""
    values = list(range(n_rows))
    data = make_csv(['id'], [[str(value)] for value in values])
    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(f'csv batch_size=2 | sample {n} seed={seed} | csv').run(
            input=data,
            chunk_size=chunk_size,
        )
        stats = full_transform_stats(result.stats_text, 'sample')
        current = (result.output_text, stats.get('rows_in'), stats.get('rows_out'))
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        headers, rows = parse_csv_output(result.output_text)
        sampled = [int(row[0]) for row in rows]
        assert headers == ['id']
        assert len(sampled) == min(n, n_rows)
        assert len(set(sampled)) == len(sampled)
        assert all(value in values for value in sampled)
        assert stats.get('rows_in') == n_rows
        assert stats.get('rows_out') == min(n, n_rows)


@given(
    values=st.lists(st.integers(min_value=-20, max_value=20), min_size=1, max_size=20),
    delta=st.integers(min_value=-10, max_value=10),
    tolerance=st.integers(min_value=0, max_value=10),
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_aggregate_assert_tolerance_chunk_boundaries_match_oracle(values, delta, tolerance):
    """Aggregate assert tolerance must be chunk-invariant and match a Python oracle."""
    total = sum(values)
    threshold = total + delta
    expected_pass = abs(delta) <= tolerance
    data = make_csv(['amount'], [[str(value)] for value in values])
    dsl = (
        'csv batch_size=2 | assert aggregate=sum:amount op=== '
        f'value={threshold} tolerance={tolerance} rel=false action=warn | csv'
    )
    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(dsl).run(input=data, chunk_size=chunk_size)
        stats = full_transform_stats(result.stats_text, 'assert')
        errors = [
            json.loads(line)
            for line in result.errors.decode('utf-8').splitlines()
            if line.strip()
        ]
        current = (
            result.output_text,
            result.errors,
            stats.get('aggregate_passed'),
            stats.get('aggregate_value'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == make_csv(['amount'], [[str(value)] for value in values]).decode('utf-8')
        assert stats.get('assert_mode') == 'aggregate'
        assert stats.get('aggregate') == 'sum'
        assert stats.get('aggregate_column') == 'amount'
        assert stats.get('comparison') == '=='
        assert stats.get('threshold') == threshold
        assert stats.get('tolerance') == tolerance
        assert stats.get('relative_tolerance') is False
        assert stats.get('aggregate_rows') == len(values)
        assert stats.get('aggregate_non_null') == len(values)
        assert stats.get('aggregate_value') == total
        assert stats.get('aggregate_passed') is expected_pass
        assert len(errors) == (0 if expected_pass else 1)
        if not expected_pass:
            assert errors[0].get('reason') == 'aggregate_assert_failed'


@given(
    colors=st.lists(st.sampled_from(['red', 'blue', 'green', 'yellow']), min_size=1, max_size=24)
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_onehot_declared_categories_chunk_boundaries_match_oracle(colors):
    """Declared-category onehot must keep a stable schema and zero unknown=null rows."""
    data = make_csv(['id', 'color'], [[str(i + 1), color] for i, color in enumerate(colors)])
    expected = make_csv(
        ['id', 'color', 'color_red', 'color_blue'],
        [
            [
                str(i + 1),
                color,
                '1' if color == 'red' else '0',
                '1' if color == 'blue' else '0',
            ]
            for i, color in enumerate(colors)
        ],
    ).decode('utf-8')
    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(
            'csv batch_size=2 | onehot color categories=red,blue unknown=null | csv'
        ).run(input=data, chunk_size=chunk_size)
        stats = full_transform_stats(result.stats_text, 'onehot')
        current = (
            result.output_text,
            stats.get('tracked_categories'),
            stats.get('rows_in'),
            stats.get('rows_out'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == expected
        assert stats.get('tracked_categories') == 2
        assert stats.get('rows_in') == len(colors)
        assert stats.get('rows_out') == len(colors)


@given(
    cities=st.lists(st.sampled_from(['Paris', 'London', 'Berlin', 'Rome']), min_size=1, max_size=24)
)
@settings(max_examples=35, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_label_encode_declared_categories_chunk_boundaries_match_oracle(cities):
    """Declared-category label encoding must use stable labels for unknown=other."""
    data = make_csv(['city'], [[city] for city in cities])
    expected = make_csv(
        ['city', 'city_id'],
        [
            [
                city,
                '0' if city == 'Paris' else ('1' if city == 'London' else '2'),
            ]
            for city in cities
        ],
    ).decode('utf-8')
    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(
            'csv batch_size=2 | label-encode city city_id categories=Paris,London unknown=other | csv'
        ).run(input=data, chunk_size=chunk_size)
        stats = full_transform_stats(result.stats_text, 'label-encode')
        current = (
            result.output_text,
            stats.get('tracked_categories'),
            stats.get('rows_in'),
            stats.get('rows_out'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == expected
        assert stats.get('tracked_categories') == 3
        assert stats.get('rows_in') == len(cities)
        assert stats.get('rows_out') == len(cities)


def test_chunk_boundary_parity_jsonl_and_text_record_ops():
    """JSONL/text record parsers and row-local JSON ops must handle byte cuts inside records."""
    jsonl = (
        b'{"id":1,"payload":{"city":"NY","zip":10001,"age":31}}\n'
        b'{"id":2,"payload":{"city":"SF","zip":94105,"age":25}}\n'
        b'{"id":3,"payload":{"city":"LA","zip":90001,"age":43}}\n'
    )
    jsonl_base = assert_pipeline_chunk_parity(
        'jsonl batch_size=2 | json-filter payload $.age >= 30 type=float | json-flatten payload fields=$.city:city,$.zip:zip:int | csv',
        jsonl,
        expected_output='id,payload,city,zip\n1,"{""city"":""NY"",""zip"":10001,""age"":31}",NY,10001\n3,"{""city"":""LA"",""zip"":90001,""age"":43}",LA,90001\n',
    )
    assert [step['op'] for step in jsonl_base['stats']] == ['json-filter', 'json-flatten']

    text = b'info: boot\nerror: first timeout\ndebug: noisy\nerror: second timeout'
    text_base = assert_pipeline_chunk_parity(
        'text batch_size=2 | grep -r "^error:.*timeout" | text',
        text,
        expected_output='error: first timeout\nerror: second timeout\n',
    )
    assert text_base['stats'][0]['op'] == 'grep'
    assert text_base['stats'][0]['rows_out'] == 2

# --- Chunk-boundary oracle tests for native spill paths ---

small_id = st.sampled_from(['0', '1', '2', '3', '4'])
# Keep generated string payload columns non-numeric so streaming CSV type inference
# does not coerce later mixed values to null before the operator under test runs.
small_name = st.text(alphabet='abcxyz', min_size=1, max_size=4)
set_name = st.text(alphabet='abcxyz', min_size=1, max_size=4)
pivot_metric = st.sampled_from(['a', 'b', 'c', 'd'])
small_value = st.integers(min_value=-20, max_value=20)


@st.composite
def sorted_run_counts(draw, *, max_count=3):
    counts = draw(st.lists(st.integers(min_value=0, max_value=max_count), min_size=5, max_size=5))
    assume(sum(counts) > 0)
    return counts


def rows_from_counts(counts, prefix):
    rows = []
    for key, count in zip(['1', '2', '3', '4', '5'], counts):
        for i in range(count):
            rows.append([key, f'{prefix}{key}_{i}'])
    return rows


def grouped_by_first_col(rows):
    groups = {}
    order = []
    for row in rows:
        key = row[0]
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(row)
    return order, groups


def expected_sorted_join_rows(left_rows, lookup_rows, how):
    _, lookup = grouped_by_first_col(lookup_rows)
    out = []
    for key, name in left_rows:
        matches = lookup.get(key, [])
        if how == 'semi':
            if matches:
                out.append([key, name])
        elif how == 'anti':
            if not matches:
                out.append([key, name])
        elif how == 'inner':
            for _, label in matches:
                out.append([key, name, label])
        elif how == 'left':
            if matches:
                for _, label in matches:
                    out.append([key, name, label])
            else:
                out.append([key, name, ''])
    return out


def expected_sorted_set_rows(left_rows, lookup_rows, op):
    left_order, left = grouped_by_first_col(left_rows)
    _, lookup = grouped_by_first_col(lookup_rows)
    out = []
    for key in left_order:
        left_run = left[key]
        lookup_count = len(lookup.get(key, []))
        if op == 'intersect':
            if lookup_count:
                out.append(left_run[0])
        elif op == 'setdiff':
            if not lookup_count:
                out.append(left_run[0])
        elif op == 'intersect-all':
            out.extend(left_run[:min(len(left_run), lookup_count)])
        elif op == 'setdiff-all':
            out.extend(left_run[min(len(left_run), lookup_count):])
    return [list(row) for row in out]


def expected_sorted_union_rows(left_rows, lookup_rows):
    left_order, left = grouped_by_first_col(left_rows)
    lookup_order, lookup = grouped_by_first_col(lookup_rows)
    keys = sorted(set(left_order) | set(lookup_order), key=int)
    out = []
    for key in keys:
        out.append(list(left[key][0] if key in left else lookup[key][0]))
    return out


def assert_sorted_file_chunk_invariant(dsl, data, op, expected_text, expected_stats):
    baseline = None
    for chunk_size in chunk_parity_sizes(data):
        result = tf.pipeline(dsl).run(
            input=data,
            chunk_size=chunk_size,
            allow_fs=True,
            memory='64KB',
        )
        stats = full_transform_stats(result.stats_text, op)
        current = (
            result.output_text,
            stats.get('execution_target'),
            stats.get('memory_class'),
            stats.get('emit_class'),
            stats.get('schema_class'),
            stats.get('state_estimate'),
            stats.get('rows_in'),
            stats.get('rows_out'),
        )
        if baseline is None:
            baseline = current
        else:
            assert current == baseline

        assert result.output_text == expected_text
        for key, value in expected_stats.items():
            assert stats.get(key) == value, f"{op} stat {key}: {stats}"


def expected_pivot_text(rows):
    metrics = first_seen_order(metric for _, metric, _ in rows)
    groups = first_seen_order(group for group, _, _ in rows)
    sums = {}
    for group, metric, value in rows:
        sums[(group, metric)] = sums.get((group, metric), 0) + value
    out_rows = []
    for group in groups:
        out_rows.append([group] + [str(sums[(group, metric)]) if (group, metric) in sums else '' for metric in metrics])
    return make_csv(['id'] + metrics, out_rows).decode('utf-8')


@given(
    left_counts=sorted_run_counts(),
    lookup_counts=sorted_run_counts(),
)
@settings(max_examples=15, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_sorted_join_chunk_boundaries_match_oracles(left_counts, lookup_counts):
    """Sorted joins must stay bounded/current-run and byte-cut invariant."""
    left_rows = rows_from_counts(left_counts, 'n')
    lookup_rows = rows_from_counts(lookup_counts, 'l')
    data = make_csv(['id', 'name'], left_rows)
    lookup_path = write_temp_csv(['id', 'label'], lookup_rows)
    try:
        cases = [
            ('semi', 'semi-join', 'semi-join', '', ['id', 'name'], 'stable', 'O(current_lookup_run)'),
            ('anti', 'anti-join', 'anti-join', '', ['id', 'name'], 'stable', 'O(current_lookup_run)'),
            ('inner', 'join', 'join', ' max_matches_per_row=3', ['id', 'name', 'label'],
             'data_dependent', 'O(current_lookup_run + max_matches_per_row)'),
            ('left', 'join', 'join', ' max_matches_per_row=3 --left', ['id', 'name', 'label'],
             'data_dependent', 'O(current_lookup_run + max_matches_per_row)'),
        ]
        for how, op, dsl_op, extra, headers, schema_class, state_estimate in cases:
            expected_rows = expected_sorted_join_rows(left_rows, lookup_rows, how)
            expected = '' if not expected_rows else make_csv(headers, expected_rows).decode('utf-8')
            assert_sorted_file_chunk_invariant(
                f'csv batch_size=2 | {dsl_op} {lookup_path} on=id sorted=true{extra} | csv',
                data,
                op,
                expected,
                {
                    'execution_target': 'native',
                    'memory_class': 'bounded_state',
                    'emit_class': 'per_batch',
                    'schema_class': schema_class,
                    'state_estimate': state_estimate,
                    'rows_in': len(left_rows),
                    'rows_out': len(expected_rows),
                },
            )
    finally:
        os.unlink(lookup_path)


@given(
    left_counts=sorted_run_counts(),
    lookup_counts=sorted_run_counts(),
)
@settings(max_examples=15, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_sorted_set_union_chunk_boundaries_match_oracles(left_counts, lookup_counts):
    """Sorted set/union modes must keep only adjacent/current-run state across byte cuts."""
    left_rows = rows_from_counts(left_counts, 'n')
    lookup_rows = rows_from_counts(lookup_counts, 'l')
    data = make_csv(['id', 'name'], left_rows)
    lookup_path = write_temp_csv(['id', 'name'], lookup_rows)
    try:
        for op in ['intersect', 'setdiff', 'intersect-all', 'setdiff-all']:
            expected_rows = expected_sorted_set_rows(left_rows, lookup_rows, op)
            expected = '' if not expected_rows else make_csv(['id', 'name'], expected_rows).decode('utf-8')
            assert_sorted_file_chunk_invariant(
                f'csv batch_size=2 | {op} {lookup_path} columns=id sorted=true | csv',
                data,
                op,
                expected,
                {
                    'execution_target': 'native',
                    'memory_class': 'bounded_state',
                    'emit_class': 'per_batch',
                    'schema_class': 'stable',
                    'state_estimate': (
                        'O(current_lookup_run + current_left_run)' if op.endswith('-all')
                        else 'O(previous_left_key + current_lookup_key)'
                    ),
                    'rows_in': len(left_rows),
                    'rows_out': len(expected_rows),
                },
            )

        expected_rows = expected_sorted_union_rows(left_rows, lookup_rows)
        assert_sorted_file_chunk_invariant(
            f'csv batch_size=2 | union {lookup_path} columns=id sorted=true | csv',
            data,
            'union',
            make_csv(['id', 'name'], expected_rows).decode('utf-8'),
            {
                'execution_target': 'native',
                'memory_class': 'bounded_state',
                'emit_class': 'mixed',
                'schema_class': 'stable',
                'state_estimate': 'O(previous_left_key + current_file_key)',
                'rows_in': len(left_rows),
                'rows_out': len(expected_rows),
            },
        )
    finally:
        os.unlink(lookup_path)


@given(
    rows=st.lists(st.tuples(small_id, pivot_metric, small_value), min_size=1, max_size=18)
)
@settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_spill_pivot_chunk_boundaries_match_oracle(rows):
    """Spill pivot output/stats must not depend on input byte chunking."""
    data = make_csv(['id', 'metric', 'value'], rows)
    expected = expected_pivot_text(rows)
    expected_groups = len(first_seen_order(group for group, _, _ in rows))
    expected_categories = len(first_seen_order(metric for _, metric, _ in rows))
    assert_chunk_invariant(
        'csv batch_size=3 | pivot metric value sum max_categories=4 | csv',
        data,
        'pivot',
        expected,
        {
            'execution_target': 'native_spill',
            'memory_class': 'external',
            'emit_class': 'on_flush',
            'schema_class': 'data_dependent',
            'rows_in': len(rows),
            'rows_out': expected_groups,
            'tracked_categories': expected_categories,
            'spill_output_rows': expected_groups,
            'spill_distinct_groups': expected_groups,
        },
    )


def flatten_lookup_map(lookup_map):
    rows = []
    for key in sorted(lookup_map):
        for label in lookup_map[key]:
            rows.append([key, label])
    return rows


def expected_join_text(left_rows, lookup_rows):
    lookup = {}
    for key, label in lookup_rows:
        lookup.setdefault(key, []).append(label)
    out = []
    for key, name in left_rows:
        for label in lookup.get(key, []):
            out.append([key, name, label])
    if not out:
        return ''
    return make_csv(['id', 'name', 'label'], out).decode('utf-8')


@given(
    left_rows=st.lists(st.tuples(small_id, small_name), min_size=1, max_size=18),
    lookup_map=st.dictionaries(
        keys=small_id,
        values=st.lists(small_name, min_size=0, max_size=3),
        min_size=1,
        max_size=5,
    ),
)
@settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_spill_mutating_join_chunk_boundaries_match_oracle(left_rows, lookup_map):
    """Spill mutating join must preserve left order and match order for every chunk cut."""
    lookup_rows = flatten_lookup_map(lookup_map)
    assume(lookup_rows)
    data = make_csv(['id', 'name'], left_rows)
    expected = expected_join_text(left_rows, lookup_rows)
    lookup_path = write_temp_csv(['id', 'label'], lookup_rows)
    try:
        expected_rows = len(parse_csv_output(expected)[1])
        assert_chunk_invariant(
            f'csv batch_size=2 | join {lookup_path} on=id max_matches_per_row=3 | csv',
            data,
            'join',
            expected,
            {
                'execution_target': 'native_spill',
                'memory_class': 'external',
                'emit_class': 'on_flush',
                'schema_class': 'data_dependent',
                'rows_in': len(left_rows),
                'rows_out': expected_rows,
                'spill_output_rows': expected_rows,
                'spill_kept_rows': expected_rows,
            },
        )
    finally:
        os.unlink(lookup_path)


def test_spill_temporal_keys_chunk_boundaries_match_oracles():
    """Date/timestamp spill keys must materialize and compare consistently across chunks."""
    date_left = make_csv(
        ['id', 'dt', 'name'],
        [
            ['1', '2024-03-01', 'alice'],
            ['2', '2024-01-15', 'bob'],
            ['3', '2024-03-01', 'carol'],
            ['4', '2024-04-20', 'drew'],
        ],
    )
    date_lookup_path = write_temp_csv(
        ['dt', 'label'],
        [
            ['2024-03-01', 'x'],
            ['2024-03-01', 'y'],
            ['2024-02-01', 'z'],
        ],
    )
    try:
        assert_chunk_invariant(
            f'csv batch_size=2 | join {date_lookup_path} on=dt max_matches_per_row=2 | csv',
            date_left,
            'join',
            make_csv(
                ['id', 'dt', 'name', 'label'],
                [
                    ['1', '2024-03-01', 'alice', 'x'],
                    ['1', '2024-03-01', 'alice', 'y'],
                    ['3', '2024-03-01', 'carol', 'x'],
                    ['3', '2024-03-01', 'carol', 'y'],
                ],
            ).decode('utf-8'),
            {
                'execution_target': 'native_spill',
                'memory_class': 'external',
                'emit_class': 'on_flush',
                'rows_in': 4,
                'rows_out': 4,
                'spill_output_rows': 4,
                'spill_kept_rows': 4,
            },
        )
    finally:
        os.unlink(date_lookup_path)

    date_set_left = make_csv(
        ['dt', 'name'],
        [
            ['2024-03-01', 'alice'],
            ['2024-01-15', 'bob'],
            ['2024-03-01', 'carol'],
            ['2024-04-20', 'drew'],
        ],
    )
    date_set_lookup_path = write_temp_csv(
        ['dt', 'name'],
        [
            ['2024-03-01', 'lookup-a'],
            ['2024-05-10', 'lookup-b'],
        ],
    )
    try:
        assert_chunk_invariant(
            f'csv batch_size=2 | intersect {date_set_lookup_path} columns=dt | csv',
            date_set_left,
            'intersect',
            make_csv(['dt', 'name'], [['2024-03-01', 'alice']]).decode('utf-8'),
            {
                'execution_target': 'native_spill',
                'memory_class': 'external',
                'emit_class': 'on_flush',
                'rows_in': 4,
                'rows_out': 1,
                'spill_output_rows': 1,
                'spill_distinct_rows': 1,
            },
        )
        assert_chunk_invariant(
            f'csv batch_size=2 | setdiff {date_set_lookup_path} columns=dt | csv',
            date_set_left,
            'setdiff',
            make_csv(
                ['dt', 'name'],
                [['2024-01-15', 'bob'], ['2024-04-20', 'drew']],
            ).decode('utf-8'),
            {
                'execution_target': 'native_spill',
                'memory_class': 'external',
                'emit_class': 'on_flush',
                'rows_in': 4,
                'rows_out': 2,
                'spill_output_rows': 2,
                'spill_distinct_rows': 2,
            },
        )
    finally:
        os.unlink(date_set_lookup_path)

    ts_left = make_csv(
        ['ts', 'value'],
        [
            ['2024-03-01T12:00:00', '10'],
            ['2024-03-01T12:00:01', '5'],
            ['2024-03-01T12:00:00', '7'],
            ['2024-03-02T09:30:00', '3'],
        ],
    )
    assert_chunk_invariant(
        'csv batch_size=2 | group-agg ts value:sum:total value:count:n | csv',
        ts_left,
        'group-agg',
        make_csv(
            ['ts', 'total', 'n'],
            [
                ['2024-03-01T12:00:00Z', '17', '2'],
                ['2024-03-01T12:00:01Z', '5', '1'],
                ['2024-03-02T09:30:00Z', '3', '1'],
            ],
        ).decode('utf-8'),
        {
            'execution_target': 'native_spill',
            'memory_class': 'external',
            'emit_class': 'on_flush',
            'rows_in': 4,
            'rows_out': 3,
            'spill_output_rows': 3,
            'spill_distinct_groups': 3,
        },
    )


def expected_intersect_rows(left_rows, lookup_rows):
    lookup_keys = {row[0] for row in lookup_rows}
    seen = set()
    out = []
    for row in left_rows:
        key = row[0]
        if key in lookup_keys and key not in seen:
            seen.add(key)
            out.append(list(row))
    return out


def expected_setdiff_rows(left_rows, lookup_rows):
    lookup_keys = {row[0] for row in lookup_rows}
    seen = set()
    out = []
    for row in left_rows:
        key = row[0]
        if key not in lookup_keys and key not in seen:
            seen.add(key)
            out.append(list(row))
    return out


def expected_union_rows(left_rows, lookup_rows):
    seen = set()
    out = []
    for row in list(left_rows) + list(lookup_rows):
        key = row[0]
        if key not in seen:
            seen.add(key)
            out.append(list(row))
    return out


def assert_set_like_chunk_invariant(op, dsl_op, left_rows, lookup_rows, expected_rows):
    data = make_csv(['id', 'name'], left_rows)
    expected = '' if not expected_rows else make_csv(['id', 'name'], expected_rows).decode('utf-8')
    lookup_path = write_temp_csv(['id', 'name'], lookup_rows)
    try:
        stat_name = 'spill_distinct_rows'
        assert_chunk_invariant(
            f'csv batch_size=2 | {dsl_op} {lookup_path} columns=id | csv',
            data,
            op,
            expected,
            {
                'execution_target': 'native_spill',
                'memory_class': 'external',
                'emit_class': 'on_flush',
                'rows_in': len(left_rows),
                'rows_out': len(expected_rows),
                'spill_output_rows': len(expected_rows),
                stat_name: len(expected_rows),
            },
        )
    finally:
        os.unlink(lookup_path)


@given(
    left_rows=st.lists(st.tuples(small_id, set_name), min_size=1, max_size=16),
    lookup_rows=st.lists(st.tuples(small_id, set_name), min_size=1, max_size=12),
)
@settings(max_examples=15, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_spill_set_ops_chunk_boundaries_match_oracles(left_rows, lookup_rows):
    """Spill set/union outputs and normalized stats must be chunk-boundary invariant."""
    assert_set_like_chunk_invariant('intersect', 'intersect', left_rows, lookup_rows,
                                    expected_intersect_rows(left_rows, lookup_rows))
    assert_set_like_chunk_invariant('setdiff', 'setdiff', left_rows, lookup_rows,
                                    expected_setdiff_rows(left_rows, lookup_rows))
    assert_set_like_chunk_invariant('union', 'union', left_rows, lookup_rows,
                                    expected_union_rows(left_rows, lookup_rows))
