#!/usr/bin/env python3
"""Install built Tranfi package artifacts in temporary projects and run smoke tests."""

from __future__ import annotations

import argparse
import glob
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def expand_one(pattern: str) -> Path:
    matches = [Path(p) for p in glob.glob(pattern)]
    if len(matches) != 1:
        raise ValueError(f'{pattern}: expected exactly one match, found {len(matches)}')
    return matches[0].resolve()


def run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, env=merged_env, check=True)


def venv_python(venv: Path) -> Path:
    if os.name == 'nt':
        return venv / 'Scripts' / 'python.exe'
    return venv / 'bin' / 'python'


def smoke_python(python: str, sdist: Path) -> None:
    with tempfile.TemporaryDirectory(prefix='tranfi-py-install-') as tmp:
        tmp_path = Path(tmp)
        venv = tmp_path / 'venv'
        run([python, '-m', 'venv', str(venv)])
        py = venv_python(venv)
        env = {
            'PIP_DISABLE_PIP_VERSION_CHECK': '1',
            'PIP_NO_WARN_SCRIPT_LOCATION': '0',
        }
        run([str(py), '-m', 'pip', 'install', '--no-deps', str(sdist)], env=env)
        code = r'''
import tranfi as tf
result = tf.pipeline('csv | filter "col(age) > 25" | csv').run(input=b'name,age\nA,20\nB,30\n')
out = result.output_text
if 'B,30' not in out or 'A,20' in out:
    raise SystemExit(f'unexpected output: {out!r}')
print('python clean install smoke OK')
'''
        run([str(py), '-c', code])


def smoke_npm(node: str, npm: str, tarball: Path) -> None:
    with tempfile.TemporaryDirectory(prefix='tranfi-npm-install-') as tmp:
        tmp_path = Path(tmp)
        env = {
            'npm_config_cache': '/tmp/npm-pack-audit',
            'npm_config_audit': 'false',
            'npm_config_fund': 'false',
        }
        run([npm, 'init', '-y'], cwd=tmp_path, env=env)
        run([npm, 'install', '--no-audit', '--no-fund', str(tarball)], cwd=tmp_path, env=env)
        code = r'''
const tf = require('tranfi')
async function main () {
  const result = await tf.pipeline('csv | filter "col(age) > 25" | csv').run({ input: 'name,age\nA,20\nB,30\n' })
  const out = result.outputText
  if (!out.includes('B,30') || out.includes('A,20')) {
    throw new Error('unexpected output: ' + JSON.stringify(out))
  }
  console.log('npm clean install smoke OK')
}
main().catch(err => { console.error(err); process.exit(1) })
'''
        run([node, '-e', code], cwd=tmp_path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--python-sdist', help='Python sdist .tar.gz path or glob')
    parser.add_argument('--npm-tarball', help='npm package .tgz path or glob')
    parser.add_argument('--python', default=sys.executable)
    parser.add_argument('--node', default='node')
    parser.add_argument('--npm', default='npm')
    args = parser.parse_args()

    if not args.python_sdist and not args.npm_tarball:
        parser.error('provide --python-sdist and/or --npm-tarball')

    if args.python_sdist:
        smoke_python(args.python, expand_one(args.python_sdist))
    if args.npm_tarball:
        smoke_npm(args.node, args.npm, expand_one(args.npm_tarball))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
