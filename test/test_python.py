"""
test_python.py — Integration tests for the Python tranfi package.

Run: cd tranfi-core && python -m pytest test/test_python.py -v
(Requires libtranfi.so in build/)
"""

import os
import sys
import pytest

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
    assert len(r) == 21
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


# --- Server tests ---

import threading
import shutil
import tempfile
import json as json_mod
import urllib.request

def _find_app_dir():
    app_dist = os.path.join(os.path.dirname(__file__), '..', '..', 'app', 'dist')
    if os.path.isfile(os.path.join(app_dist, 'index.html')):
        return os.path.abspath(app_dist)
    return None

APP_DIR = _find_app_dir()
skip_no_app = pytest.mark.skipif(APP_DIR is None, reason='app/dist/ not found')


def test_serve_csv_to_html():
    from tranfi.cli import _csv_to_html
    html = _csv_to_html('name,age\nAlice,30\nBob,25\n', 'Test')
    assert '<table' in html
    assert 'Alice' in html
    assert 'Bob' in html
    assert 'Test' in html
    assert '2 rows' in html


def test_serve_csv_to_html_empty():
    from tranfi.cli import _csv_to_html
    assert _csv_to_html('', 'X') == ''
    assert _csv_to_html(None, 'X') == ''


@skip_no_app
def test_serve_api_endpoints():
    """Start the server in a thread and test all API endpoints."""
    from http.server import HTTPServer
    from tranfi.cli import _csv_to_html, _ffi
    import io, socket

    data_dir = tempfile.mkdtemp()
    try:
        with open(os.path.join(data_dir, 'test.csv'), 'w') as f:
            f.write('name,age\nAlice,30\nBob,25\nCharlie,35\n')

        # Find a free port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', 0))
        port = sock.getsockname()[1]
        sock.close()

        # Suppress server stderr output
        old_stderr = sys.stderr
        sys.stderr = io.StringIO()

        # Import and call serve_command internals — build the server inline
        from tranfi.cli import serve_command
        import types
        args = types.SimpleNamespace(data=data_dir, app=APP_DIR, port=port)

        # Start server in thread
        server_obj = [None]

        def run():
            # We duplicate serve_command logic to get the server object
            import urllib.parse as up
            from http.server import BaseHTTPRequestHandler

            app_dir = APP_DIR
            with open(os.path.join(app_dir, 'index.html'), 'r') as f:
                index_html = f.read()
            config = "<script>window.__TRANFI_SERVER__={api:'/api'}</script>"
            index_html = index_html.replace('</head>', config + '</head>')
            index_html_bytes = index_html.encode('utf-8')
            data_extensions = {'.csv', '.jsonl', '.tsv', '.txt', '.json'}
            CHAN_MAIN, CHAN_STATS, CHUNK_SIZE = 0, 2, 64 * 1024

            class H(BaseHTTPRequestHandler):
                def log_message(self, fmt, *a): pass
                def _json(self, d, s=200):
                    b = json_mod.dumps(d).encode()
                    self.send_response(s)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(b)))
                    self.end_headers()
                    self.wfile.write(b)
                def do_GET(self):
                    p = up.urlparse(self.path)
                    params = up.parse_qs(p.query)
                    if p.path == '/api/version':
                        return self._json({'version': _ffi.version()})
                    if p.path == '/api/files':
                        files = [{'name': n, 'size': os.path.getsize(os.path.join(data_dir, n))}
                                 for n in sorted(os.listdir(data_dir))
                                 if os.path.splitext(n)[1].lower() in data_extensions]
                        return self._json({'files': files})
                    if p.path == '/api/file':
                        name = params.get('name', [None])[0]
                        head = int(params.get('head', ['20'])[0])
                        if not name or '..' in name or '/' in name:
                            return self._json({'error': 'bad name'}, 400)
                        full = os.path.join(data_dir, name)
                        if not os.path.isfile(full):
                            return self._json({'error': 'not found'}, 404)
                        with open(full) as f:
                            lines = f.read().split('\n')
                        return self._json({'preview': '\n'.join(lines[:head]), 'lines': len(lines)})
                    if p.path == '/api/recipes':
                        n = _ffi.recipe_count()
                        return self._json({'recipes': [{'name': _ffi.recipe_name(i)} for i in range(n)]})
                    # static
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/html')
                    self.send_header('Content-Length', str(len(index_html_bytes)))
                    self.end_headers()
                    self.wfile.write(index_html_bytes)
                def do_POST(self):
                    if up.urlparse(self.path).path != '/api/run':
                        return self._json({'error': 'not found'}, 404)
                    body = json_mod.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
                    fn, dsl = body.get('file'), body.get('dsl')
                    if not fn or not dsl:
                        return self._json({'error': 'missing fields'}, 400)
                    full = os.path.join(data_dir, fn)
                    if not os.path.isfile(full):
                        return self._json({'error': 'not found'}, 404)
                    plan = _ffi.compile_dsl(dsl)
                    h = _ffi.pipeline_create_from_json(plan)
                    try:
                        with open(full, 'rb') as f:
                            while True:
                                chunk = f.read(CHUNK_SIZE)
                                if not chunk: break
                                _ffi.pipeline_push(h, chunk)
                        _ffi.pipeline_finish(h)
                        parts = []
                        while True:
                            d = _ffi.pipeline_pull(h, CHAN_MAIN)
                            if not d: break
                            parts.append(d)
                        output = b''.join(parts).decode()
                        self._json({'output': _csv_to_html(output, 'Output'), 'stats': ''})
                    finally:
                        _ffi.pipeline_free(h)

            srv = HTTPServer(('', port), H)
            server_obj[0] = srv
            srv.serve_forever()

        t = threading.Thread(target=run, daemon=True)
        t.start()
        import time; time.sleep(0.3)

        base = f'http://localhost:{port}'

        def get(path):
            return json_mod.loads(urllib.request.urlopen(f'{base}{path}').read())

        def post(path, data):
            req = urllib.request.Request(f'{base}{path}',
                data=json_mod.dumps(data).encode(),
                headers={'Content-Type': 'application/json'})
            return json_mod.loads(urllib.request.urlopen(req).read())

        # Test endpoints
        v = get('/api/version')
        assert 'version' in v

        files = get('/api/files')
        assert any(f['name'] == 'test.csv' for f in files['files'])

        preview = get('/api/file?name=test.csv&head=2')
        assert 'Alice' in preview['preview']

        recipes = get('/api/recipes')
        assert len(recipes['recipes']) > 0

        result = post('/api/run', {'file': 'test.csv', 'dsl': 'csv | filter "age > 28" | csv'})
        assert 'Alice' in result['output']
        assert 'Charlie' in result['output']
        assert 'Bob' not in result['output']

        # index.html has config
        html = urllib.request.urlopen(base).read().decode()
        assert '__TRANFI_SERVER__' in html

        server_obj[0].shutdown()
    finally:
        sys.stderr = old_stderr
        shutil.rmtree(data_dir)


if __name__ == '__main__':
    import pytest
    pytest.main([__file__, '-v'])
