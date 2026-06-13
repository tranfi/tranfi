#!/usr/bin/env python3
"""
Copy Tranfi C sources from the repo into py/csrc/.

The Python package builds from py/csrc/ so sdists remain self-contained.
Run from either the repo root or py/:
    python py/scripts/sync-csrc.py
    python scripts/sync-csrc.py
"""

import os
import shutil
import sys


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    py_dir = os.path.dirname(script_dir)
    repo_root = os.path.dirname(py_dir)
    src_dir = os.path.join(repo_root, 'src')
    csrc_dir = os.path.join(py_dir, 'csrc')

    if not os.path.isdir(src_dir):
        print(f'ERROR: source directory not found: {src_dir}', file=sys.stderr)
        sys.exit(1)

    files = sorted(
        name for name in os.listdir(src_dir)
        if name.endswith(('.c', '.h'))
    )
    if not files:
        print(f'ERROR: no C sources found in {src_dir}', file=sys.stderr)
        sys.exit(1)

    if os.path.exists(csrc_dir):
        shutil.rmtree(csrc_dir)
    os.makedirs(csrc_dir, exist_ok=True)

    for name in files:
        shutil.copy(os.path.join(src_dir, name), os.path.join(csrc_dir, name))

    print(f'sync-csrc: copied {len(files)} files to {csrc_dir}')


if __name__ == '__main__':
    main()
