#!/usr/bin/env python3
"""Check that copied package C source mirrors match src/."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SOURCE_SUFFIXES = ('.c', '.h')
MIRRORS = {
    'py': Path('py/csrc'),
    'js': Path('js/csrc'),
}


def c_files(directory: Path) -> dict[str, Path]:
    return {
        path.name: path
        for path in directory.iterdir()
        if path.is_file() and path.suffix in SOURCE_SUFFIXES
    }


def check_mirror(repo_root: Path, name: str) -> list[str]:
    src_dir = repo_root / 'src'
    mirror_dir = repo_root / MIRRORS[name]
    errors: list[str] = []

    if not src_dir.is_dir():
        return [f'source directory not found: {src_dir}']
    if not mirror_dir.is_dir():
        return [f'{name}: mirror directory not found: {mirror_dir}']

    src_files = c_files(src_dir)
    if not src_files:
        return [f'no C sources found in {src_dir}']
    mirror_files = c_files(mirror_dir)

    for filename in sorted(src_files.keys() - mirror_files.keys()):
        errors.append(f'{name}: missing {MIRRORS[name] / filename}')
    for filename in sorted(mirror_files.keys() - src_files.keys()):
        errors.append(f'{name}: extra {MIRRORS[name] / filename}')
    for filename in sorted(src_files.keys() & mirror_files.keys()):
        src_path = src_files[filename]
        mirror_path = mirror_files[filename]
        if src_path.read_bytes() != mirror_path.read_bytes():
            errors.append(f'{name}: stale {MIRRORS[name] / filename} differs from src/{filename}')

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--mirror',
        choices=['all', *MIRRORS.keys()],
        default='all',
        help='mirror to check; defaults to both package mirrors',
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    names = list(MIRRORS) if args.mirror == 'all' else [args.mirror]
    errors: list[str] = []
    for name in names:
        errors.extend(check_mirror(repo_root, name))

    if errors:
        for error in errors:
            print(f'check-csrc-sync: ERROR: {error}', file=sys.stderr)
        print('check-csrc-sync: run make sync-py-csrc sync-js-csrc', file=sys.stderr)
        return 1

    src_count = len(c_files(repo_root / 'src'))
    checked = ', '.join(str(MIRRORS[name]) for name in names)
    print(f'check-csrc-sync: {checked} match src/ ({src_count} files)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
