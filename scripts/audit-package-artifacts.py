#!/usr/bin/env python3
"""Audit Python and npm package artifact contents."""

from __future__ import annotations

import argparse
import glob
import json
import sys
import tarfile
from pathlib import Path

FORBIDDEN_EXACT = {
    'AGENTS.md',
    'CLAUDE.md',
    'PLAN.md',
    'MEMORY.md',
    'UPSTREAM.md',
    '.gitignore',
    '.gitmodules',
    '.npmignore',
    'package-lock.json',
}
FORBIDDEN_COMPONENTS = {
    '.git',
    '.agents',
    '.codex',
    '.claude',
    '.pytest_cache',
    '__pycache__',
    'node_modules',
    'test',
    'tests',
    'upstream',
    'reference',
    'references',
    'patches',
    'build',
    'coverage',
}
FORBIDDEN_SUFFIXES = ('.log', '.pyc', '.pyo')

NPM_REQUIRED = {
    'package.json',
    'README.md',
    'LICENSE',
    'NOTICE',
    'binding.gyp',
    'napi_api.c',
    'scripts/install-native.js',
    'scripts/prepack.js',
    'scripts/sync-csrc.js',
    'src/cli.js',
    'src/index.js',
    'src/native.js',
    'src/pipeline.js',
    'src/memory_policy.js',
    'wasm/index.js',
    'wasm/package.json',
    'wasm/tranfi_core.js',
    'wasm/worker.js',
    'csrc/tranfi.h',
    'csrc/codec_csv.c',
    'csrc/pipeline.c',
    'app/index.html',
}
NPM_REQUIRED_PREFIXES = ('app/assets/',)

PY_SDIST_REQUIRED = {
    'pyproject.toml',
    'setup.py',
    'MANIFEST.in',
    'README.md',
    'LICENSE',
    'NOTICE',
    'tranfi/__init__.py',
    'tranfi/_ffi.py',
    'tranfi/_native.c',
    'tranfi/cli.py',
    'tranfi/memory_policy.py',
    'tranfi/pipeline.py',
    'csrc/tranfi.h',
    'csrc/codec_csv.c',
    'csrc/pipeline.c',
}


def normalize(path: str) -> str:
    return path.replace('\\', '/').lstrip('./')


def strip_sdist_root(path: str) -> str:
    path = normalize(path)
    parts = path.split('/')
    if len(parts) >= 2 and parts[0].startswith('tranfi-'):
        return '/'.join(parts[1:])
    return path


def forbidden_reasons(paths: set[str]) -> list[str]:
    errors: list[str] = []
    for path in sorted(paths):
        if not path or path.endswith('/'):
            continue
        name = path.rsplit('/', 1)[-1]
        components = set(path.split('/'))
        if name in FORBIDDEN_EXACT or path in FORBIDDEN_EXACT:
            errors.append(f'forbidden file included: {path}')
        hit = components & FORBIDDEN_COMPONENTS
        if hit:
            errors.append(f'forbidden directory included: {path} ({sorted(hit)[0]})')
        if path.endswith(FORBIDDEN_SUFFIXES) or name == '.DS_Store':
            errors.append(f'forbidden generated/local file included: {path}')
    return errors


def require_paths(kind: str, paths: set[str], required: set[str], prefixes: tuple[str, ...] = ()) -> list[str]:
    errors: list[str] = []
    missing = sorted(required - paths)
    for path in missing:
        errors.append(f'{kind}: missing required artifact: {path}')
    for prefix in prefixes:
        if not any(path.startswith(prefix) for path in paths):
            errors.append(f'{kind}: missing required artifact under: {prefix}')
    return errors


def npm_paths(path: Path) -> set[str]:
    data = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data, list) or not data:
        raise ValueError(f'{path}: expected npm pack --json list')
    files = data[0].get('files')
    if not isinstance(files, list):
        raise ValueError(f'{path}: expected files list in npm pack output')
    return {normalize(item['path']) for item in files if isinstance(item, dict) and 'path' in item}


def sdist_paths(path: Path) -> set[str]:
    with tarfile.open(path, 'r:gz') as tar:
        return {strip_sdist_root(member.name) for member in tar.getmembers() if member.isfile()}


def expand_one(pattern: str) -> Path:
    matches = [Path(p) for p in glob.glob(pattern)]
    if len(matches) != 1:
        raise ValueError(f'{pattern}: expected exactly one match, found {len(matches)}')
    return matches[0]


def audit(kind: str, paths: set[str], required: set[str], prefixes: tuple[str, ...] = ()) -> list[str]:
    errors = forbidden_reasons(paths)
    errors.extend(require_paths(kind, paths, required, prefixes))
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--npm-json', action='append', default=[], help='npm pack --dry-run --json output file')
    parser.add_argument('--python-sdist', action='append', default=[], help='Python sdist .tar.gz path or glob')
    args = parser.parse_args()

    errors: list[str] = []
    summaries: list[str] = []

    for item in args.npm_json:
        path = expand_one(item)
        paths = npm_paths(path)
        errors.extend(audit('npm', paths, NPM_REQUIRED, NPM_REQUIRED_PREFIXES))
        summaries.append(f'npm {path}: {len(paths)} files')

    for item in args.python_sdist:
        path = expand_one(item)
        paths = sdist_paths(path)
        errors.extend(audit('python sdist', paths, PY_SDIST_REQUIRED))
        summaries.append(f'python sdist {path}: {len(paths)} files')

    if not args.npm_json and not args.python_sdist:
        parser.error('provide --npm-json and/or --python-sdist')

    if errors:
        for error in errors:
            print(f'audit-package-artifacts: ERROR: {error}', file=sys.stderr)
        return 1

    for summary in summaries:
        print(f'audit-package-artifacts: {summary} OK')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
