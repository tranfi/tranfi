"""
test_python.py — Integration tests for the Python tranfi package.

Run: cd tranfi-core && python -m pytest test/test_python.py -v
(Requires libtranfi.so in build/)
"""

import os
import sys

# Add the Python package to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'py'))

# Set library path to the build directory
build_dir = os.path.join(os.path.dirname(__file__), '..', 'build')
lib_path = os.path.join(build_dir, 'libtranfi.so')
if os.path.exists(lib_path):
    os.environ['TRANFI_LIB_PATH'] = lib_path

import tranfi as tf


def test_version():
    v = tf.version()
    assert v == '0.1.0'


def test_csv_passthrough():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,age\nAlice,30\nBob,25\n')
    text = result.output_text
    assert 'name,age' in text
    assert 'Alice' in text
    assert 'Bob' in text


def test_csv_filter():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.filter(tf.expr("col('age') > 27")),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,age,score\nAlice,30,85\nBob,25,92\nCharlie,35,78\n')
    text = result.output_text
    assert 'Alice' in text
    assert 'Charlie' in text
    assert 'Bob' not in text


def test_csv_select():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.select(['name', 'score']),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,age,score\nAlice,30,85\nBob,25,92\n')
    text = result.output_text
    assert 'name,score' in text
    assert 'age' not in text


def test_csv_rename():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.rename(name='full_name'),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,age\nAlice,30\n')
    text = result.output_text
    assert 'full_name' in text


def test_csv_head():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.head(2),
        tf.codec.csv_encode(),
    ])
    csv = b'name,age\nAlice,30\nBob,25\nCharlie,35\nDiana,28\n'
    result = p.run(input=csv)
    text = result.output_text
    assert 'Alice' in text
    assert 'Bob' in text
    assert 'Charlie' not in text


def test_combined_pipeline():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.filter(tf.expr("col('age') > 25")),
        tf.ops.select(['name', 'age']),
        tf.ops.rename(name='person'),
        tf.ops.head(2),
        tf.codec.csv_encode(),
    ])
    csv = (
        b'name,age,score\n'
        b'Alice,30,85\n'
        b'Bob,25,92\n'
        b'Charlie,35,78\n'
        b'Diana,28,95\n'
        b'Eve,42,88\n'
    )
    result = p.run(input=csv)
    text = result.output_text
    assert 'person,age' in text
    assert 'Alice' in text
    assert 'Bob' not in text


def test_jsonl_passthrough():
    p = tf.pipeline([
        tf.codec.jsonl(),
        tf.codec.jsonl_encode(),
    ])
    jsonl = b'{"name":"Alice","age":30}\n{"name":"Bob","age":25}\n'
    result = p.run(input=jsonl)
    text = result.output_text
    assert 'Alice' in text
    assert 'Bob' in text


def test_jsonl_filter():
    p = tf.pipeline([
        tf.codec.jsonl(),
        tf.ops.filter(tf.expr("col('age') >= 30")),
        tf.codec.jsonl_encode(),
    ])
    jsonl = (
        b'{"name":"Alice","age":30}\n'
        b'{"name":"Bob","age":25}\n'
        b'{"name":"Charlie","age":35}\n'
    )
    result = p.run(input=jsonl)
    text = result.output_text
    assert 'Alice' in text
    assert 'Charlie' in text
    assert 'Bob' not in text


def test_file_input():
    fixtures_dir = os.path.join(os.path.dirname(__file__), 'fixtures')
    csv_path = os.path.join(fixtures_dir, 'sample.csv')

    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.filter(tf.expr("col('age') > 30")),
        tf.codec.csv_encode(),
    ])
    result = p.run(input_file=csv_path)
    text = result.output_text
    # From sample.csv: Charlie(35), Eve(42), Grace(31), Hank(45), Jack(33)
    assert 'Charlie' in text
    assert 'Eve' in text
    assert 'Alice' not in text  # age 30, not > 30
    assert 'Bob' not in text    # age 25


def test_stats_channel():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'x\n1\n2\n3\n')
    assert len(result.stats) > 0
    assert b'rows_in' in result.stats


def test_pipeline_result_repr():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'x\n1\n')
    r = repr(result)
    assert 'PipelineResult' in r


def test_error_handling():
    try:
        tf.pipeline([]).run(input=b'')
    except RuntimeError:
        pass  # expected — empty plan


# ---- New operator tests ----

def test_ops_tail():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.tail(2),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,age\nAlice,30\nBob,25\nCharlie,35\n')
    text = result.output_text
    assert 'Bob' in text
    assert 'Charlie' in text
    assert 'Alice' not in text


def test_ops_clip():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.clip('age', min=26, max=34),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,age\nAlice,30\nBob,25\nCharlie,35\n')
    text = result.output_text
    assert '30' in text     # Alice: 30 within range
    assert '26' in text     # Bob: 25 clipped to 26
    assert '34' in text     # Charlie: 35 clipped to 34


def test_ops_replace():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.replace('name', 'Alice', 'Alicia'),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,age\nAlice,30\nBob,25\n')
    text = result.output_text
    assert 'Alicia' in text
    assert 'Bob' in text


def test_ops_trim():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.trim(['name']),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,age\n  Alice  ,30\n Bob ,25\n')
    text = result.output_text
    lines = text.strip().split('\n')
    assert lines[1].startswith('Alice,')
    assert lines[2].startswith('Bob,')


def test_ops_validate():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.validate(tf.expr("col('age') > 27")),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,age\nAlice,30\nBob,25\n')
    text = result.output_text
    assert '_valid' in text
    lines = text.strip().split('\n')
    assert len(lines) == 3  # header + 2 rows (keeps all)


def test_ops_explode():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.explode('tags', '|'),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,tags\nAlice,a|b|c\nBob,x\n')
    text = result.output_text
    lines = text.strip().split('\n')
    assert len(lines) == 5  # header + 4 rows (3 for Alice + 1 for Bob)


def test_ops_step():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.step('val', 'running-sum', 'cumsum'),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'val\n10\n20\n30\n')
    text = result.output_text
    assert 'cumsum' in text
    assert '60' in text  # 10+20+30


def test_ops_frequency():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.frequency(['city']),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'city\nNY\nLA\nNY\nNY\nLA\n')
    text = result.output_text
    assert 'value' in text
    assert 'count' in text
    assert 'NY' in text


def test_ops_top():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.top(2, 'score'),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,score\nAlice,85\nBob,92\nCharlie,78\n')
    text = result.output_text
    assert 'Bob' in text
    assert 'Alice' in text
    assert 'Charlie' not in text


def test_ops_datetime():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.datetime('date', ['year', 'month']),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'date\n2024-03-15\n2023-12-01\n')
    text = result.output_text
    assert 'date_year' in text
    assert 'date_month' in text
    assert '2024' in text
    assert '3' in text


def test_ops_window():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.window('val', 2, 'sum', 'val_sum2'),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'val\n10\n20\n30\n')
    text = result.output_text
    assert 'val_sum2' in text
    assert '50' in text  # 20+30


def test_ops_hash():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.hash(['name']),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name,age\nAlice,30\nBob,25\n')
    text = result.output_text
    assert '_hash' in text


def test_ops_sample():
    p = tf.pipeline([
        tf.codec.csv(),
        tf.ops.sample(2),
        tf.codec.csv_encode(),
    ])
    result = p.run(input=b'name\nAlice\nBob\nCharlie\nDiana\nEve\n')
    text = result.output_text
    lines = text.strip().split('\n')
    assert len(lines) == 3  # header + 2 sampled rows


def test_ops_text_grep():
    p = tf.pipeline([
        tf.codec.text(),
        tf.ops.grep('error'),
        tf.codec.text_encode(),
    ])
    result = p.run(input=b'info: started\nerror: something failed\ninfo: done\nerror: another\n')
    text = result.output_text
    assert 'error: something failed' in text
    assert 'error: another' in text
    assert 'info: started' not in text
    assert 'info: done' not in text


def test_ops_text_passthrough():
    p = tf.pipeline([
        tf.codec.text(),
        tf.codec.text_encode(),
    ])
    result = p.run(input=b'hello world\nfoo bar\n')
    text = result.output_text
    assert 'hello world' in text
    assert 'foo bar' in text


def test_compile_dsl():
    recipe = tf.compile_dsl('csv | head 5 | csv')
    import json
    data = json.loads(recipe)
    assert 'steps' in data
    assert len(data['steps']) == 3
    assert data['steps'][0]['op'] == 'codec.csv.decode'
    assert data['steps'][1]['op'] == 'head'
    assert data['steps'][2]['op'] == 'codec.csv.encode'


def test_save_load_recipe():
    import tempfile
    steps = [
        tf.codec.csv(),
        tf.ops.head(1),
        tf.codec.csv_encode(),
    ]
    with tempfile.NamedTemporaryFile(suffix='.tranfi', mode='w', delete=False) as f:
        path = f.name
    try:
        tf.save_recipe(steps, path)
        p = tf.load_recipe(path)
        result = p.run(input=b'x\n1\n2\n3\n')
        assert '1' in result.output_text
        assert '3' not in result.output_text
    finally:
        os.unlink(path)


def test_load_recipe_json_string():
    recipe = '{"steps":[{"op":"codec.csv.decode","args":{}},{"op":"codec.csv.encode","args":{}}]}'
    p = tf.load_recipe(recipe)
    result = p.run(input=b'x\n1\n')
    assert '1' in result.output_text


def test_pipeline_recipe_kwarg():
    import tempfile
    import json
    with tempfile.NamedTemporaryFile(suffix='.tranfi', mode='w', delete=False) as f:
        json.dump({'steps': [
            {'op': 'codec.csv.decode', 'args': {}},
            {'op': 'codec.csv.encode', 'args': {}},
        ]}, f)
        path = f.name
    try:
        p = tf.pipeline(recipe=path)
        result = p.run(input=b'x\n1\n')
        assert '1' in result.output_text
    finally:
        os.unlink(path)


def test_recipes_list():
    r = tf.recipes()
    assert len(r) == 20
    names = [x['name'] for x in r]
    assert 'profile' in names
    assert 'preview' in names
    assert 'csv2json' in names
    for item in r:
        assert 'name' in item
        assert 'dsl' in item
        assert 'description' in item


def test_recipe_pipeline_preview():
    data = b'name,age\nAlice,30\nBob,25\n'
    result = tf.pipeline('preview').run(input=data)
    assert 'Alice' in result.output_text
    assert 'Bob' in result.output_text


def test_recipe_pipeline_dedup():
    data = b'x\n1\n2\n1\n3\n'
    result = tf.pipeline('dedup').run(input=data)
    lines = [l for l in result.output_text.strip().split('\n') if l]
    assert len(lines) == 4  # header + 3 unique values


def test_recipe_pipeline_csv2json():
    data = b'name,age\nAlice,30\n'
    result = tf.pipeline('csv2json').run(input=data)
    assert '"name"' in result.output_text
    assert '"Alice"' in result.output_text


if __name__ == '__main__':
    import pytest
    pytest.main([__file__, '-v'])
