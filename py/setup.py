import glob
import os
from setuptools import setup, Extension

csrc = os.path.join(os.path.dirname(__file__), 'csrc')

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
            extra_compile_args=['-std=c11', '-O2', '-D_POSIX_C_SOURCE=200809L'],
            libraries=['m'],
        )
    ]
)
