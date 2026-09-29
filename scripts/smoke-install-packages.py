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


REPO_ROOT = Path(__file__).resolve().parents[1]


def temp_root() -> Path:
    root = Path(os.environ.get('TRANFI_TEST_TMPDIR', REPO_ROOT / 'build' / 'package-smoke-tmp'))
    root.mkdir(parents=True, exist_ok=True)
    return root.resolve()


def expand_one(pattern: str) -> Path:
    matches = [Path(p) for p in glob.glob(pattern)]
    if len(matches) != 1:
        raise ValueError(f'{pattern}: expected exactly one match, found {len(matches)}')
    return matches[0].resolve()


def run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    merged_env = os.environ.copy()
    tmp = str(temp_root())
    merged_env.setdefault('TMPDIR', tmp)
    merged_env.setdefault('TEMP', tmp)
    merged_env.setdefault('TMP', tmp)
    merged_env.setdefault('PIP_CACHE_DIR', str(temp_root() / 'pip-cache'))
    if env:
        merged_env.update(env)
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, env=merged_env, check=True)


def venv_python(venv: Path) -> Path:
    if os.name == 'nt':
        return venv / 'Scripts' / 'python.exe'
    return venv / 'bin' / 'python'


def smoke_python(python: str, sdist: Path) -> None:
    with tempfile.TemporaryDirectory(prefix='tranfi-py-install-', dir=temp_root()) as tmp:
        tmp_path = Path(tmp)
        venv = tmp_path / 'venv'
        run([python, '-m', 'venv', str(venv)])
        py = venv_python(venv)
        env = {
            'PIP_DISABLE_PIP_VERSION_CHECK': '1',
            'PIP_NO_WARN_SCRIPT_LOCATION': '0',
        }
        run([str(py), '-m', 'pip', 'install', '--no-cache-dir', '--no-deps', str(sdist)], env=env)
        code = r'''
from array import array
import ctypes
import tranfi as tf
import tranfi._native as native_extension
native_symbols = ctypes.CDLL(native_extension.__file__)
if hasattr(native_symbols, 'tf_wasm_transform_recipe_from_json'):
    raise SystemExit('Python extension leaked the raw wasm32 transform shim')
limits = tf.safe_transform_limits()
if limits.max_recipe_bytes != 1048576:
    raise SystemExit(f'unexpected transform limits: {limits.max_recipe_bytes!r}')
recipe_json = '{"columns":[{"categorical":null,"kind":{"maxCategories":null,"op":"declared","rule":null,"value":"numeric"},"numeric":{"impute":{"allMissing":null,"constant":null,"op":"none"},"normalize":{"ddof":null,"op":"none"}},"sourceId":"x0"}],"format":"tranfi.transform-recipe","outputDtype":"float64","policyVersion":1,"semanticLimits":{"maxOutputColumns":65536,"maxOutputElementsPerApply":134217728},"version":1}'
schema = [{'id': 'x0', 'dtype': 'float64'}]
with tf.TransformRecipe.from_json(recipe_json) as recipe:
    with recipe.analyzer(schema) as analyzer:
        analyzer.push({'rows': 2, 'columns': [array('d', [1.0, 2.0])]})
        with analyzer.finalize() as plan:
            with tf.TransformPlan.from_bytes(plan.to_bytes()) as loaded:
                with loaded.apply(schema) as apply:
                    transformed = apply.run({'rows': 1, 'columns': [array('d', [3.0])]})
                    if list(transformed.data) != [3.0]:
                        raise SystemExit(f'unexpected prepared output: {transformed.data!r}')
result = tf.pipeline('csv | filter "col(age) > 25" | csv').run(input=b'name,age\nA,20\nB,30\n')
out = result.output_text
if 'B,30' not in out or 'A,20' in out:
    raise SystemExit(f'unexpected output: {out!r}')
print('python clean install smoke OK')
'''
        run([str(py), '-c', code], cwd=tmp_path, env={'PYTHONPATH': '', 'TRANFI_LIB_PATH': ''})
        # Resolve extras from the installed archive's metadata in this otherwise
        # clean venv. Manually installing pandas would mask a broken extra.
        run([str(py), '-m', 'pip', 'install', 'tranfi[duckdb]'], cwd=tmp_path, env=env)
        run([str(py), '-c', r'''
import subprocess
import sys
from pathlib import Path
import tranfi as tf
assert Path(tf.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()), tf.__file__
result = tf.pipeline('csv | head 1 | csv', engine='duckdb').run(input=b'x\n1\n')
assert result.output == b'x\n1\n', result.output
for flags, allowed in [([], False), (['--allow-blocking'], True)]:
    result = subprocess.run([sys.executable, '-m', 'tranfi.cli', *flags, '-q',
                             'csv | sort age | csv'],
                            input=b'age\n30\n20\n', capture_output=True)
    assert (result.returncode == 0) == allowed, result.stderr
    if allowed:
        assert result.stdout == b'age\n20\n30\n', result.stdout
    else:
        assert b'blocking' in result.stderr
print('python installed DuckDB extra and CLI policy smoke OK')
'''], cwd=tmp_path, env={'PYTHONPATH': '', 'TRANFI_LIB_PATH': ''})


def smoke_npm(node: str, npm: str, tarball: Path) -> None:
    with tempfile.TemporaryDirectory(prefix='tranfi-npm-install-', dir=temp_root()) as tmp:
        tmp_path = Path(tmp)
        env = {
            'npm_config_cache': str(temp_root() / 'npm-cache'),
            'npm_config_audit': 'false',
            'npm_config_fund': 'false',
        }
        run([npm, 'init', '-y'], cwd=tmp_path, env=env)
        run([npm, 'install', '--no-audit', '--no-fund', str(tarball)], cwd=tmp_path, env=env)
        code = r'''
const tf = require('tranfi')
async function main () {
  const recipeConfig = { columns: [{ categorical: null, kind: { maxCategories: null, op: 'declared', rule: null, value: 'numeric' }, numeric: { impute: { allMissing: null, constant: null, op: 'none' }, normalize: { ddof: null, op: 'none' } }, sourceId: 'x0' }], format: 'tranfi.transform-recipe', outputDtype: 'float64', policyVersion: 1, semanticLimits: { maxOutputColumns: 65536, maxOutputElementsPerApply: 134217728 }, version: 1 }
  const schema = [{ id: 'x0', dtype: 'float64' }]
  const recipe = tf.TransformRecipe.fromJSON(recipeConfig)
  const analyzer = recipe.analyzer(schema)
  analyzer.push({ rows: 2, columns: [new Float64Array([1, 2])] })
  const plan = analyzer.finalize()
  const loaded = tf.TransformPlan.fromBytes(plan.toBytes())
  const apply = loaded.apply(schema)
  const transformed = apply.run({ rows: 1, columns: [new Float64Array([3])] })
  if (transformed.rows !== 1 || transformed.data[0] !== 3) {
    throw new Error('unexpected prepared output')
  }
  apply.close()
  loaded.close()
  plan.close()
  analyzer.close()
  recipe.close()
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
        run([node, str(REPO_ROOT / 'test' / 'smoke_windows_wasm.js')], cwd=tmp_path)


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
