"""Run README examples verbatim with explicit fixture/setup annotations.

Shell installation/build commands are syntax-checked, not executed. CLI examples
run through each applicable installed command surface without invoking npm/pip.
Browser integration fragments are reported as skips, never certified by Node.
"""
import gzip
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
READMES = ('README.md', 'js/README.md', 'py/README.md')
CSV = ('name,age,city,score,score1,score2,price,qty,date,color,sales,amount,metric,value\n'
       'Alice,30,NY,95,96,70,12.5,2,2026-09-01,red,25,25,a,2\n'
       'Bob,25,LA,85,60,92,10.0,3,2026-09-02,blue,30,30,b,3\n')
SETUP = {
    'run': '',
    'node-cjs': '',
    'python-api': 'import tranfi as tf\n',
    'python-data': 'import tranfi as tf\ncsv_bytes = ' + repr(CSV.encode()) + '\n',
    'python-pipeline': "import tranfi as tf\np = tf.pipeline('csv | csv')\n"
                       'def process(chunk):\n    assert isinstance(chunk, bytes)\n',
    'js-api': "import { pipeline, codec, ops, expr } from 'tranfi'\n",
    'js-data': 'const csvString = ' + json.dumps(CSV) + '\n',
    'js-pipeline': "import { pipeline } from 'tranfi'\nconst p = pipeline('csv | csv')\n",
    'js-sql': "import { compileToSql } from 'tranfi'\n",
    'c-callback': '',
    'c-run': '',
    'browser': '',
}


def blocks():
    for path in READMES:
        text = (ROOT / path).read_text(encoding='utf-8')
        for i, match in enumerate(re.finditer(r'^```([^\n]*)\n(.*?)^```', text, re.M | re.S)):
            annotation = re.search(r'<!-- readme-test: ([\w-]+) -->\s*$', text[:match.start()])
            yield path, i, match[1], match[2], annotation[1] if annotation else 'run'


BLOCKS = list(blocks())


@pytest.fixture
def work(tmp_path):
    for name in ('data.csv', 'people.csv', 'input.csv', 'students.csv', 'small.csv',
                 'delivery.csv', 'lookup.csv', 'part-a.csv', 'part-b.csv'):
        (tmp_path / name).write_text(CSV)
    for name in ('server.log', 'app.log'):
        (tmp_path / name).write_text('error: request timeout\ndebug hello\n')
    (tmp_path / 'delivery_baseline.json').write_text(json.dumps({
        'columns': {'name': 'string', 'city': 'string', 'score': 'int'},
        'values': {'city': ['NY', 'LA']},
    }))
    with gzip.open(tmp_path / 'events.jsonl.gz', 'wb') as stream:
        stream.write(b'{"name":"Alice","age":30}\n')
    (tmp_path / 'spill').mkdir(mode=0o700)
    (tmp_path / 'node_modules').mkdir()
    (tmp_path / 'node_modules/tranfi').symlink_to(ROOT / 'js', target_is_directory=True)
    return tmp_path


def execute(command, work, **kwargs):
    env = dict(os.environ, PYTHONPATH=str(ROOT / 'py'),
               TRANFI_LIB_PATH=str(ROOT / 'build/libtranfi.so'))
    return subprocess.run(command, cwd=work, env=env, capture_output=True,
                          text=True, timeout=120, **kwargs)


@pytest.mark.parametrize('path,index,lang,code,mode', BLOCKS,
                         ids=[f'{p}:{i}:{lang}' for p, i, lang, _, _ in BLOCKS])
def test_readme_block(path, index, lang, code, mode, work):
    assert mode in SETUP, f'Unknown README setup: {mode}'
    if mode == 'browser':
        pytest.skip('Browser integration fragment; native/Wasm/Worker suites do not certify this example')
    if lang in ('', 'text', 'mermaid'):
        return  # Usage output, architecture diagrams and lifecycle notation.
    if lang == 'bash':
        result = execute(['bash', '-n'], work, input=code)
    elif lang == 'python':
        assert mode in ('run', 'python-api', 'python-data', 'python-pipeline')
        result = execute([sys.executable, '-c', SETUP[mode] + code], work)
    elif lang == 'js':
        assert mode in ('run', 'node-cjs', 'js-api', 'js-data', 'js-pipeline', 'js-sql')
        example = work / ('example.cjs' if mode == 'node-cjs' else 'example.mjs')
        example.write_text(SETUP[mode] + code)
        result = execute([os.environ.get('NODE', 'node'), str(example)], work)
    elif lang == 'c':
        assert mode in ('c-callback', 'c-run')
        # The docs separate callback definitions from registration statements.
        declarations, body = code.rsplit('\n\n', 1) if mode == 'c-callback' else ('', code)
        example = work / 'example.c'
        example.write_text('#include <stdio.h>\n#include "tranfi.h"\n' + declarations +
                           '\nvoid example(tf_pipeline *p, void *user, int in_fd, int out_fd) {\n' +
                           body + '\n}\nint main(void) { return 0; }\n')
        result = execute([os.environ.get('CC', 'cc'), '-std=c11',
                          '-Werror=implicit-function-declaration', '-Werror=incompatible-pointer-types',
                          '-I', str(ROOT / 'src'),
                          str(example), str(ROOT / 'build/libtranfi.so'),
                          '-o', str(work / 'example')], work)
    else:
        pytest.fail(f'Unclassified code fence: {path}:{index} ({lang})')
    assert result.returncode == 0, result.stdout + result.stderr


def cli_examples():
    for path, index, lang, code, _ in BLOCKS:
        if lang != 'bash':
            continue
        for line_number, line in enumerate(code.splitlines()):
            if not line.startswith(('tranfi ', './build/tranfi ')) and '| npx tranfi ' not in line:
                continue
            for backend in ('C', 'npm', 'pip'):
                if line.startswith('./build/tranfi ') and backend != 'C':
                    continue
                yield path, index, line_number, backend, line


CLI = list(cli_examples())


@pytest.mark.parametrize('path,index,line_number,backend,line', CLI,
                         ids=[f'{p}:{i}:{n}:{b}' for p, i, n, b, _ in CLI])
def test_readme_cli(path, index, line_number, backend, line, work):
    command = {
        'C': [str(ROOT / 'build/tranfi')],
        'npm': [os.environ.get('NODE', 'node'), str(ROOT / 'js/src/cli.js')],
        'pip': [sys.executable, '-m', 'tranfi.cli'],
    }[backend]
    # Preserve shell quoting, redirections and printf input. Replace only the
    # command location and private spill fixture; never run npx against a registry.
    line = line.replace('/tmp/tranfi-spill', str(work / 'spill'))
    line = line.replace('./build/tranfi', shlex.quote(str(ROOT / 'build/tranfi')))
    line = line.replace('npx tranfi', 'tranfi')
    script = 'tranfi() { ' + shlex.join(command) + ' "$@"; }\n' + line
    result = execute(['bash', '-o', 'pipefail', '-c', script], work, input=CSV)
    if '# readme-test: rejects-blocking' in line:
        assert result.returncode != 0
        assert 'blocking' in result.stderr
    else:
        assert result.returncode == 0, result.stdout + result.stderr
    if 'printf ' in line:
        assert result.stdout == 'name,age\nAlice,30\n'
