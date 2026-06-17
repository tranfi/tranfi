import glob
import os
import subprocess
import sys
from setuptools import setup, Extension

here = os.path.dirname(os.path.abspath(__file__))
csrc = os.path.join(here, 'csrc')
repo_root = os.path.dirname(here)
src_dir = os.path.join(repo_root, 'src')


def _repo_source_files():
    if not os.path.isdir(src_dir):
        return []
    return [
        name for name in sorted(os.listdir(src_dir))
        if name.endswith(('.c', '.h'))
    ]


def _needs_sync(files):
    if not files:
        return False
    if not os.path.isdir(csrc):
        return True
    for name in files:
        src = os.path.join(src_dir, name)
        dst = os.path.join(csrc, name)
        if not os.path.isfile(dst):
            return True
        if os.path.getmtime(src) > os.path.getmtime(dst):
            return True
    return False


repo_files = _repo_source_files()
sync_script = os.path.join(here, 'scripts', 'sync-csrc.py')
if _needs_sync(repo_files):
    print('setup.py: syncing csrc/ from repo sources...')
    subprocess.check_call([sys.executable, sync_script])
elif not os.path.isdir(csrc):
    raise SystemExit(
        'csrc/ directory not found and repo src/ is unavailable.\n'
        'Run from the repo: python py/scripts/sync-csrc.py'
    )

# All C sources except main.c (CLI) and wasm_api.c (Emscripten)
exclude = {'main.c', 'wasm_api.c'}
sources = [
    os.path.join('csrc', f)
    for f in sorted(os.listdir(csrc))
    if f.endswith('.c') and f not in exclude
] + ['tranfi/_native.c']

setup(
    ext_modules=[
        Extension(
            'tranfi._native',
            sources=sources,
            include_dirs=[csrc],
            extra_compile_args=[
                '-std=c11',
                '-O2',
                '-D_POSIX_C_SOURCE=200809L',
                '-D_FORTIFY_SOURCE=3',
                '-Werror=implicit-function-declaration',
                '-Werror=incompatible-pointer-types',
                '-Wformat',
                '-Werror=format-security',
                '-Werror=unused-result',
                '-fno-common',
                '-fstack-protector-strong',
            ],
            libraries=['m'],
        )
    ]
)
