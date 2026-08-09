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
    'napi_transform.c',
    'napi_transform.h',
    'scripts/install-native.js',
    'scripts/prepack.js',
    'scripts/sync-csrc.js',
    'src/cli.js',
    'src/index.js',
    'src/native.js',
    'src/pipeline.js',
    'src/memory_policy.js',
    'src/transform.js',
    'src/transform_error.js',
    'wasm/index.js',
    'wasm/package.json',
    'wasm/tranfi_core.js',
    'wasm/transform.js',
    'wasm/worker.js',
    'csrc/tranfi.h',
    'csrc/transform.h',
    'csrc/transform_api.c',
    'csrc/transform_categorical.c',
    'csrc/transform_json.c',
    'csrc/transform_numeric.c',
    'csrc/transform_sha256.c',
    'csrc/transform_wasm.h',
    'csrc/transform_wasm_api.c',
    'csrc/codec_csv.c',
    'csrc/pipeline.c',
    'csrc/spill.c',
    'csrc/spill.h',
    'csrc/size_utils.c',
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
    'tranfi/transform.py',
    'csrc/tranfi.h',
    'csrc/transform.h',
    'csrc/transform_api.c',
    'csrc/transform_categorical.c',
    'csrc/transform_json.c',
    'csrc/transform_numeric.c',
    'csrc/transform_sha256.c',
    'csrc/codec_csv.c',
    'csrc/pipeline.c',
    'csrc/spill.c',
    'csrc/spill.h',
    'csrc/size_utils.c',
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
    text = path.read_text(encoding='utf-8')
    try:
        data = json.loads(text)
    except json.JSONDecodeError as original_error:
        decoder = json.JSONDecoder()
        data = None
        for idx, ch in enumerate(text):
            if ch != '[':
                continue
            try:
                candidate, _ = decoder.raw_decode(text[idx:])
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, list):
                data = candidate
                break
        if data is None:
            raise original_error
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


def audit_repo_hardening() -> list[str]:
    cmake = Path('CMakeLists.txt')
    if not cmake.exists():
        return ['repo: missing CMakeLists.txt for hardening audit']
    text = cmake.read_text(encoding='utf-8')
    required = [
        '_POSIX_C_SOURCE=200809L',
        '_XOPEN_SOURCE=700',
        'Werror=implicit-function-declaration',
        'Wformat',
        'Werror=format-security',
        'fno-common',
        'fstack-protector-strong',
        '_FORTIFY_SOURCE=3',
        '-fPIE',
        '-pie',
    ]
    return [f'repo: CMakeLists.txt missing hardening token: {token}' for token in required if token not in text]


def audit_repo_license() -> list[str]:
    errors: list[str] = []
    required_files = [
        Path('LICENSE'),
        Path('NOTICE'),
        Path('README.md'),
        Path('py/pyproject.toml'),
        Path('py/NOTICE'),
        Path('js/package.json'),
        Path('js/NOTICE'),
    ]
    for path in required_files:
        if not path.exists():
            errors.append(f'repo: missing license/notice file: {path}')
    if errors:
        return errors

    readme = Path('README.md').read_text(encoding='utf-8')
    if 'Apache-2.0' not in readme:
        errors.append('repo: README.md license section must mention Apache-2.0')
    if '\nMIT\n' in readme or 'License\n\nMIT' in readme:
        errors.append('repo: README.md still contains MIT license text')

    pyproject = Path('py/pyproject.toml').read_text(encoding='utf-8')
    if 'license = "Apache-2.0"' not in pyproject:
        errors.append('repo: py/pyproject.toml must use SPDX license = "Apache-2.0"')
    if 'license = { text =' in pyproject:
        errors.append('repo: py/pyproject.toml still uses deprecated license table metadata')
    if 'license-files = ["LICENSE", "NOTICE"]' not in pyproject:
        errors.append('repo: py/pyproject.toml must include LICENSE and NOTICE in license-files')

    package_json = json.loads(Path('js/package.json').read_text(encoding='utf-8'))
    if package_json.get('license') != 'Apache-2.0':
        errors.append('repo: js/package.json license must be Apache-2.0')

    for path in [Path('NOTICE'), Path('py/NOTICE'), Path('js/NOTICE')]:
        body = path.read_text(encoding='utf-8')
        if 'Apache License, Version 2.0' not in body and 'Apache-2.0' not in body:
            errors.append(f'repo: {path} must mention Apache-2.0 / Apache License, Version 2.0')
        if 'cJSON 1.7.19' not in body:
            errors.append(f'repo: {path} must include cJSON 1.7.19 attribution')
    return errors


def audit_dependency_governance() -> list[str]:
    errors: list[str] = []
    provenance_path = Path('third_party/cjson.json')
    if not provenance_path.exists():
        return ['repo: missing third_party/cjson.json provenance file']
    provenance = json.loads(provenance_path.read_text(encoding='utf-8'))
    if provenance.get('version') != '1.7.19':
        errors.append('repo: cJSON provenance version must be 1.7.19')
    header = Path('src/cJSON.h').read_text(encoding='utf-8')
    if '#define CJSON_VERSION_PATCH 19' not in header:
        errors.append('repo: vendored cJSON.h must be version 1.7.19')
    source = Path('src/cJSON.c').read_text(encoding='utf-8')
    if 'static CJSON_THREAD_LOCAL error global_error' not in source:
        errors.append('repo: cJSON thread-local parse-error patch is missing')
    if Path('src/cJSON_Utils.c').exists() or Path('src/cJSON_Utils.h').exists():
        errors.append('repo: cJSON_Utils must not be vendored/compiled')
    sbom_path = Path('build/tranfi-sbom.spdx.json')
    if not sbom_path.exists():
        errors.append('repo: missing generated SBOM build/tranfi-sbom.spdx.json')
    else:
        sbom = json.loads(sbom_path.read_text(encoding='utf-8'))
        packages = {pkg.get('name'): pkg for pkg in sbom.get('packages', [])}
        if packages.get('cJSON', {}).get('versionInfo') != '1.7.19':
            errors.append('repo: generated SBOM must include cJSON 1.7.19')
        if packages.get('tranfi', {}).get('licenseDeclared') != 'Apache-2.0':
            errors.append('repo: generated SBOM must declare tranfi Apache-2.0')
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--npm-json', action='append', default=[], help='npm pack --dry-run --json output file')
    parser.add_argument('--python-sdist', action='append', default=[], help='Python sdist .tar.gz path or glob')
    args = parser.parse_args()

    errors: list[str] = []
    summaries: list[str] = []
    errors.extend(audit_repo_hardening())
    errors.extend(audit_repo_license())
    errors.extend(audit_dependency_governance())

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
