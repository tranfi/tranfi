"""
cli.py — Tranfi CLI entry point.

Usage:
  tranfi 'csv | filter "age > 25" | csv'  < in.csv
  tranfi -f pipeline.tf < in.csv > out.csv
  tranfi -i input.csv -o output.csv 'csv | sort age | csv'
  tranfi profile < data.csv
  tranfi serve -d ./data/
"""

import argparse
import sys
import json
import os
from . import _ffi

CHUNK_SIZE = 64 * 1024
CHAN_MAIN = 0
CHAN_ERRORS = 1
CHAN_STATS = 2


def format_bytes(n):
    if n < 1024:
        return f'{n}B'
    elif n < 1024 * 1024:
        return f'{n / 1024:.1f}KB'
    elif n < 1024 * 1024 * 1024:
        return f'{n / (1024 * 1024):.1f}MB'
    else:
        return f'{n / (1024 * 1024 * 1024):.1f}GB'


def main():
    # Check for 'serve' subcommand before argparse (to avoid conflict with positional pipeline arg)
    if len(sys.argv) > 1 and sys.argv[1] == 'serve':
        serve_parser = argparse.ArgumentParser(prog='tranfi serve', description='Serve the tranfi app with a native backend.')
        serve_parser.add_argument('-d', '--data', default='.', help='data directory (default: .)')
        serve_parser.add_argument('-a', '--app', default=None, help='app dist directory (auto-detect)')
        serve_parser.add_argument('-p', '--port', type=int, default=3000, help='port (default: 3000)')
        args = serve_parser.parse_args(sys.argv[2:])
        return serve_command(args)

    parser = argparse.ArgumentParser(
        prog='tranfi',
        description='Streaming ETL with a pipe-style DSL.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''\
examples:
  tranfi 'csv | csv'                           # passthrough
  tranfi 'csv | filter "age > 25" | csv'       # filter rows
  tranfi 'csv | select name,age | csv'         # select columns
  tranfi 'csv | head 10 | csv'                 # first N rows
  tranfi 'csv | sort age | csv'                # sort by column
  tranfi 'csv | stats | jsonl'                 # aggregate stats
  tranfi profile                               # built-in recipe
  tranfi serve -d ./data/                      # start server
''')
    parser.add_argument('pipeline', nargs='?', help='DSL string or recipe name')
    parser.add_argument('-f', '--file', help='read pipeline from file')
    parser.add_argument('-i', '--input', help='read input from file instead of stdin')
    parser.add_argument('-o', '--output', help='write output to file instead of stdout')
    parser.add_argument('-j', '--json', action='store_true', help='compile only, output JSON plan')
    parser.add_argument('-q', '--quiet', action='store_true', help='suppress stats on stderr')
    parser.add_argument('-p', '--progress', action='store_true', help='show progress on stderr')
    parser.add_argument('-v', '--version', action='store_true', help='show version')
    parser.add_argument('-R', '--recipes', action='store_true', help='list built-in recipes')

    args = parser.parse_args()

    if args.version:
        print(f'tranfi {_ffi.version()}')
        return

    if args.recipes:
        n = _ffi.recipe_count()
        print(f'Built-in recipes ({n}):\n')
        for i in range(n):
            print(f'  {_ffi.recipe_name(i):<12s} {_ffi.recipe_description(i)}')
            print(f'  {"":12s} {_ffi.recipe_dsl(i)}')
            print()
        return

    # Get pipeline text
    if args.file:
        with open(args.file, 'r') as f:
            pipeline_text = f.read()
    elif args.pipeline:
        pipeline_text = args.pipeline
    else:
        parser.error('no pipeline specified')

    # Parse: recipe name, JSON, or DSL
    pt = pipeline_text.strip()
    if pt.startswith('{'):
        plan_json = pt
    elif '|' not in pt and ' ' not in pt:
        # Try built-in recipe
        recipe_dsl = _ffi.recipe_find_dsl(pt)
        if recipe_dsl:
            plan_json = _ffi.compile_dsl(recipe_dsl)
        else:
            plan_json = _ffi.compile_dsl(pipeline_text)
    else:
        plan_json = _ffi.compile_dsl(pipeline_text)

    # JSON mode: print and exit
    if args.json:
        print(plan_json)
        return

    # Create pipeline
    handle = _ffi.pipeline_create_from_json(plan_json)

    try:
        # Open I/O
        if args.input:
            fin = open(args.input, 'rb')
        else:
            fin = sys.stdin.buffer

        if args.output:
            fout = open(args.output, 'wb')
        else:
            fout = sys.stdout.buffer

        # Stream input
        total_bytes = 0
        while True:
            chunk = fin.read(CHUNK_SIZE)
            if not chunk:
                break
            _ffi.pipeline_push(handle, chunk)
            total_bytes += len(chunk)

            # Pull output immediately (streaming)
            while True:
                data = _ffi.pipeline_pull(handle, CHAN_MAIN)
                if not data:
                    break
                fout.write(data)

            if args.progress:
                sys.stderr.write(f'\r{format_bytes(total_bytes)} processed')

        # Finish
        _ffi.pipeline_finish(handle)

        # Pull remaining output
        while True:
            data = _ffi.pipeline_pull(handle, CHAN_MAIN)
            if not data:
                break
            fout.write(data)
        fout.flush()

        if args.progress:
            sys.stderr.write(f'\r{format_bytes(total_bytes)} processed (done)\n')

        # Errors to stderr
        while True:
            data = _ffi.pipeline_pull(handle, CHAN_ERRORS)
            if not data:
                break
            sys.stderr.buffer.write(data)

        # Stats to stderr (unless quiet)
        if not args.quiet:
            while True:
                data = _ffi.pipeline_pull(handle, CHAN_STATS)
                if not data:
                    break
                sys.stderr.buffer.write(data)

    finally:
        _ffi.pipeline_free(handle)
        if args.input and fin is not sys.stdin.buffer:
            fin.close()
        if args.output and fout is not sys.stdout.buffer:
            fout.close()


def _esc(s):
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def _csv_to_html(csv_text, label):
    if not csv_text or not csv_text.strip():
        return ''
    lines = csv_text.strip().split('\n')
    header = lines[0].split(',')
    body = lines[1:]
    max_rows = 500
    truncated = len(body) > max_rows
    display_body = body[:max_rows] if truncated else body

    h = f'<div style="font-size:12px;color:#888;margin-bottom:6px">'
    h += f'{_esc(label)} ({len(body)} rows'
    if truncated:
        h += f', showing first {max_rows}'
    h += ')</div>'
    h += '<div style="overflow-x:auto;max-height:500px;overflow-y:auto">'
    h += '<table style="width:100%;border-collapse:collapse;font-size:13px;font-family:monospace">'
    h += '<thead><tr>'
    for col in header:
        h += '<th style="padding:6px 12px;text-align:left;border-bottom:2px solid #ddd;'
        h += 'background:#f8f8f8;font-weight:600;position:sticky;top:0">'
        h += _esc(col.strip()) + '</th>'
    h += '</tr></thead><tbody>'
    for row in display_body:
        h += '<tr>'
        for cell in row.split(','):
            h += '<td style="padding:4px 12px;text-align:left;border-bottom:1px solid #eee">'
            h += _esc(cell.strip()) + '</td>'
        h += '</tr>'
    h += '</tbody></table></div>'
    return h


def serve_command(args):
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import urllib.parse
    import pathlib

    data_dir = os.path.abspath(args.data)
    app_dir = os.path.abspath(args.app) if args.app else _find_app_dir()
    port = args.port

    if not app_dir or not os.path.isfile(os.path.join(app_dir, 'index.html')):
        print('error: app directory not found. Use --app to specify it.', file=sys.stderr)
        sys.exit(1)
    if not os.path.isdir(data_dir):
        print(f'error: data directory not found: {data_dir}', file=sys.stderr)
        sys.exit(1)

    # Read and patch index.html
    with open(os.path.join(app_dir, 'index.html'), 'r') as f:
        index_html = f.read()
    config_script = "<script>window.__TRANFI_SERVER__={api:'/api'}</script>"
    index_html = index_html.replace('</head>', config_script + '</head>')
    index_html_bytes = index_html.encode('utf-8')

    data_extensions = {'.csv', '.jsonl', '.tsv', '.txt', '.json'}
    mime_types = {
        '.html': 'text/html; charset=utf-8',
        '.js': 'application/javascript; charset=utf-8',
        '.mjs': 'application/javascript; charset=utf-8',
        '.css': 'text/css; charset=utf-8',
        '.json': 'application/json; charset=utf-8',
        '.wasm': 'application/wasm',
        '.svg': 'image/svg+xml',
        '.png': 'image/png',
        '.ico': 'image/x-icon',
        '.woff': 'font/woff',
        '.woff2': 'font/woff2',
        '.ttf': 'font/ttf',
        '.eot': 'application/vnd.ms-fontobject',
    }

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass  # suppress default logging

        def _send_json(self, data, status=200):
            body = json.dumps(data).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_error(self, msg, status=400):
            self._send_json({'error': msg}, status)

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            self.end_headers()

        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            pathname = parsed.path
            params = urllib.parse.parse_qs(parsed.query)

            if pathname == '/api/files':
                files = []
                for name in sorted(os.listdir(data_dir)):
                    ext = os.path.splitext(name)[1].lower()
                    if ext not in data_extensions:
                        continue
                    full = os.path.join(data_dir, name)
                    if os.path.isfile(full):
                        files.append({'name': name, 'size': os.path.getsize(full)})
                return self._send_json({'files': files})

            if pathname == '/api/file':
                name = params.get('name', [None])[0]
                head = int(params.get('head', ['20'])[0])
                if not name or '..' in name or '/' in name or '\\' in name:
                    return self._send_error('Invalid file name')
                full = os.path.join(data_dir, name)
                if not os.path.isfile(full):
                    return self._send_error('File not found', 404)
                with open(full, 'r') as f:
                    content = f.read()
                lines = content.split('\n')
                preview = '\n'.join(lines[:head])
                return self._send_json({'preview': preview, 'lines': len(lines)})

            if pathname == '/api/recipes':
                n = _ffi.recipe_count()
                recipes = []
                for i in range(n):
                    recipes.append({
                        'name': _ffi.recipe_name(i),
                        'dsl': _ffi.recipe_dsl(i),
                        'description': _ffi.recipe_description(i),
                    })
                return self._send_json({'recipes': recipes})

            if pathname == '/api/version':
                return self._send_json({'version': _ffi.version()})

            # Static files
            self._serve_static(pathname)

        def do_POST(self):
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path != '/api/run':
                self._send_error('Not found', 404)
                return

            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            file_name = body.get('file')
            dsl = body.get('dsl')

            if not dsl:
                return self._send_error('Missing dsl field')
            if not file_name:
                return self._send_error('Missing file field')
            if '..' in file_name or '/' in file_name or '\\' in file_name:
                return self._send_error('Invalid file name')

            full = os.path.join(data_dir, file_name)
            if not os.path.isfile(full):
                return self._send_error(f'File not found: {file_name}', 404)

            try:
                # Compile DSL
                pt = dsl.strip()
                if pt.startswith('{'):
                    plan_json = pt
                elif '|' not in pt and ' ' not in pt:
                    recipe_dsl = _ffi.recipe_find_dsl(pt)
                    plan_json = _ffi.compile_dsl(recipe_dsl if recipe_dsl else dsl)
                else:
                    plan_json = _ffi.compile_dsl(dsl)

                handle = _ffi.pipeline_create_from_json(plan_json)
                try:
                    with open(full, 'rb') as f:
                        while True:
                            chunk = f.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            _ffi.pipeline_push(handle, chunk)
                    _ffi.pipeline_finish(handle)

                    output_parts = []
                    while True:
                        data = _ffi.pipeline_pull(handle, CHAN_MAIN)
                        if not data:
                            break
                        output_parts.append(data)
                    output = b''.join(output_parts).decode('utf-8', errors='replace')

                    stats_parts = []
                    while True:
                        data = _ffi.pipeline_pull(handle, CHAN_STATS)
                        if not data:
                            break
                        stats_parts.append(data)
                    stats = b''.join(stats_parts).decode('utf-8', errors='replace')

                    self._send_json({
                        'output': _csv_to_html(output, 'Output'),
                        'stats': _csv_to_html(stats, 'Stats'),
                    })
                finally:
                    _ffi.pipeline_free(handle)
            except Exception as e:
                self._send_error(str(e), 500)

        def _serve_static(self, pathname):
            if pathname == '/':
                pathname = '/index.html'

            file_path = os.path.join(app_dir, pathname.lstrip('/'))
            resolved = os.path.realpath(file_path)

            # Security check
            if not resolved.startswith(os.path.realpath(app_dir)):
                self.send_response(403)
                self.end_headers()
                return

            # Serve patched index.html
            if os.path.basename(resolved) == 'index.html' and os.path.isfile(resolved):
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(index_html_bytes)))
                self.end_headers()
                self.wfile.write(index_html_bytes)
                return

            if not os.path.isfile(resolved):
                # SPA fallback
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(index_html_bytes)))
                self.end_headers()
                self.wfile.write(index_html_bytes)
                return

            ext = os.path.splitext(resolved)[1].lower()
            content_type = mime_types.get(ext, 'application/octet-stream')
            with open(resolved, 'rb') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(content)))
            self.end_headers()
            self.wfile.write(content)

    server = HTTPServer(('', port), Handler)
    v = _ffi.version()
    print(f'tranfi {v} server', file=sys.stderr)
    print(f'  App:  {app_dir}', file=sys.stderr)
    print(f'  Data: {data_dir}', file=sys.stderr)
    print(f'  http://localhost:{port}', file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


def _find_app_dir():
    """Try to find the app dist directory."""
    cli_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(cli_dir, 'app'),                       # pip install: bundled in package
        os.path.join(cli_dir, '..', '..', 'app', 'dist'),   # dev: py/tranfi/ → ../../app/dist
    ]
    for d in candidates:
        d = os.path.abspath(d)
        if os.path.isfile(os.path.join(d, 'index.html')):
            return d
    return None


if __name__ == '__main__':
    main()
