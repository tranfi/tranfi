"""
_ffi.py — ctypes wrapper around libtranfi shared library.

Loads libtranfi.so / libtranfi.dylib and exposes the C API as Python functions.
"""

import ctypes
import ctypes.util
import os
import sys
import platform

_lib = None


def _find_lib():
    """Find libtranfi shared library."""
    # 1. Check TRANFI_LIB_PATH environment variable
    env_path = os.environ.get('TRANFI_LIB_PATH')
    if env_path and os.path.isfile(env_path):
        return env_path

    # 2. Try the installed _native extension module (pip install)
    try:
        import importlib.util
        spec = importlib.util.find_spec('tranfi._native')
        if spec and spec.origin:
            return spec.origin
    except (ImportError, ValueError):
        pass

    # 3. Check relative to this file (development layout)
    this_dir = os.path.dirname(os.path.abspath(__file__))
    system = platform.system()

    if system == 'Darwin':
        lib_name = 'libtranfi.dylib'
    elif system == 'Windows':
        lib_name = 'tranfi.dll'
    else:
        lib_name = 'libtranfi.so'

    # Check common build directories relative to package
    search_paths = [
        os.path.join(this_dir, lib_name),
        os.path.join(this_dir, '..', '..', 'build', lib_name),
    ]

    for path in search_paths:
        resolved = os.path.abspath(path)
        if os.path.isfile(resolved):
            return resolved

    # 4. Try system library path
    found = ctypes.util.find_library('tranfi')
    if found:
        return found

    return None


def _load_lib():
    """Load the shared library and declare function signatures."""
    global _lib
    if _lib is not None:
        return _lib

    lib_path = _find_lib()
    if not lib_path:
        raise RuntimeError(
            "Could not find libtranfi shared library. "
            "Set TRANFI_LIB_PATH or build the C core first: "
            "mkdir build && cd build && cmake .. && make"
        )

    _lib = ctypes.CDLL(lib_path)

    # tf_pipeline_create
    _lib.tf_pipeline_create.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
    _lib.tf_pipeline_create.restype = ctypes.c_void_p

    # tf_pipeline_free
    _lib.tf_pipeline_free.argtypes = [ctypes.c_void_p]
    _lib.tf_pipeline_free.restype = None

    # tf_pipeline_push
    _lib.tf_pipeline_push.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
    _lib.tf_pipeline_push.restype = ctypes.c_int

    # tf_pipeline_finish
    _lib.tf_pipeline_finish.argtypes = [ctypes.c_void_p]
    _lib.tf_pipeline_finish.restype = ctypes.c_int

    # tf_pipeline_pull
    _lib.tf_pipeline_pull.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_size_t]
    _lib.tf_pipeline_pull.restype = ctypes.c_size_t

    # tf_pipeline_error
    _lib.tf_pipeline_error.argtypes = [ctypes.c_void_p]
    _lib.tf_pipeline_error.restype = ctypes.c_char_p

    # tf_version
    _lib.tf_version.argtypes = []
    _lib.tf_version.restype = ctypes.c_char_p

    # tf_last_error
    _lib.tf_last_error.argtypes = []
    _lib.tf_last_error.restype = ctypes.c_char_p

    # tf_compile_dsl
    _lib.tf_compile_dsl.argtypes = [ctypes.c_char_p, ctypes.c_size_t,
                                     ctypes.POINTER(ctypes.c_char_p)]
    _lib.tf_compile_dsl.restype = ctypes.c_void_p

    # tf_string_free
    _lib.tf_string_free.argtypes = [ctypes.c_void_p]
    _lib.tf_string_free.restype = None

    # tf_compile_to_sql
    _lib.tf_compile_to_sql.argtypes = [ctypes.c_char_p, ctypes.c_size_t,
                                        ctypes.POINTER(ctypes.c_char_p)]
    _lib.tf_compile_to_sql.restype = ctypes.c_void_p

    # tf_ir_plan_from_json
    _lib.tf_ir_plan_from_json.argtypes = [ctypes.c_char_p, ctypes.c_size_t,
                                           ctypes.POINTER(ctypes.c_char_p)]
    _lib.tf_ir_plan_from_json.restype = ctypes.c_void_p

    # tf_ir_plan_to_json
    _lib.tf_ir_plan_to_json.argtypes = [ctypes.c_void_p]
    _lib.tf_ir_plan_to_json.restype = ctypes.c_void_p

    # tf_ir_plan_validate
    _lib.tf_ir_plan_validate.argtypes = [ctypes.c_void_p]
    _lib.tf_ir_plan_validate.restype = ctypes.c_int

    # tf_ir_plan_destroy
    _lib.tf_ir_plan_destroy.argtypes = [ctypes.c_void_p]
    _lib.tf_ir_plan_destroy.restype = None

    # tf_pipeline_create_from_ir
    _lib.tf_pipeline_create_from_ir.argtypes = [ctypes.c_void_p]
    _lib.tf_pipeline_create_from_ir.restype = ctypes.c_void_p

    # tf_recipe_count
    _lib.tf_recipe_count.argtypes = []
    _lib.tf_recipe_count.restype = ctypes.c_size_t

    # tf_recipe_name
    _lib.tf_recipe_name.argtypes = [ctypes.c_size_t]
    _lib.tf_recipe_name.restype = ctypes.c_char_p

    # tf_recipe_dsl
    _lib.tf_recipe_dsl.argtypes = [ctypes.c_size_t]
    _lib.tf_recipe_dsl.restype = ctypes.c_char_p

    # tf_recipe_description
    _lib.tf_recipe_description.argtypes = [ctypes.c_size_t]
    _lib.tf_recipe_description.restype = ctypes.c_char_p

    # tf_recipe_find_dsl
    _lib.tf_recipe_find_dsl.argtypes = [ctypes.c_char_p]
    _lib.tf_recipe_find_dsl.restype = ctypes.c_char_p

    return _lib


# --- Thin Python wrappers ---

def pipeline_create(plan_json: str) -> int:
    """Create a pipeline from JSON plan. Returns handle (pointer as int)."""
    lib = _load_lib()
    data = plan_json.encode('utf-8')
    handle = lib.tf_pipeline_create(data, len(data))
    if not handle:
        err = lib.tf_last_error()
        msg = err.decode('utf-8') if err else 'unknown error'
        raise RuntimeError(f"Failed to create pipeline: {msg}")
    return handle


def pipeline_push(handle: int, data: bytes) -> None:
    """Push input bytes into the pipeline."""
    lib = _load_lib()
    rc = lib.tf_pipeline_push(handle, data, len(data))
    if rc != 0:
        err = lib.tf_pipeline_error(handle)
        msg = err.decode('utf-8') if err else 'unknown error'
        raise RuntimeError(f"Push failed: {msg}")


def pipeline_finish(handle: int) -> None:
    """Signal end of input and flush the pipeline."""
    lib = _load_lib()
    rc = lib.tf_pipeline_finish(handle)
    if rc != 0:
        err = lib.tf_pipeline_error(handle)
        msg = err.decode('utf-8') if err else 'unknown error'
        raise RuntimeError(f"Finish failed: {msg}")


def pipeline_pull(handle: int, channel: int, buf_size: int = 65536) -> bytes:
    """Pull all available output from a channel."""
    lib = _load_lib()
    chunks = []
    buf = ctypes.create_string_buffer(buf_size)
    while True:
        n = lib.tf_pipeline_pull(handle, channel, buf, buf_size)
        if n == 0:
            break
        chunks.append(buf.raw[:n])
    return b''.join(chunks)


def pipeline_free(handle: int) -> None:
    """Free pipeline resources."""
    lib = _load_lib()
    lib.tf_pipeline_free(handle)


def version() -> str:
    """Get library version."""
    lib = _load_lib()
    v = lib.tf_version()
    return v.decode('utf-8') if v else ''


def compile_dsl(dsl: str) -> str:
    """Compile a DSL string to a JSON recipe string."""
    lib = _load_lib()
    data = dsl.encode('utf-8')
    error = ctypes.c_char_p()
    ptr = lib.tf_compile_dsl(data, len(data), ctypes.byref(error))
    if not ptr:
        msg = error.value.decode('utf-8') if error.value else 'unknown error'
        raise RuntimeError(f"DSL compile failed: {msg}")
    result = ctypes.string_at(ptr).decode('utf-8')
    lib.tf_string_free(ptr)
    return result


def recipe_count() -> int:
    """Get number of built-in recipes."""
    lib = _load_lib()
    return lib.tf_recipe_count()


def recipe_name(index: int) -> str:
    """Get recipe name by index."""
    lib = _load_lib()
    s = lib.tf_recipe_name(index)
    return s.decode('utf-8') if s else ''


def recipe_dsl(index: int) -> str:
    """Get recipe DSL by index."""
    lib = _load_lib()
    s = lib.tf_recipe_dsl(index)
    return s.decode('utf-8') if s else ''


def recipe_description(index: int) -> str:
    """Get recipe description by index."""
    lib = _load_lib()
    s = lib.tf_recipe_description(index)
    return s.decode('utf-8') if s else ''


def recipe_find_dsl(name: str) -> str:
    """Find recipe DSL by name. Returns empty string if not found."""
    lib = _load_lib()
    s = lib.tf_recipe_find_dsl(name.encode('utf-8'))
    return s.decode('utf-8') if s else ''


def compile_to_sql(dsl: str) -> str:
    """Compile a DSL string to a SQL query string."""
    lib = _load_lib()
    data = dsl.encode('utf-8')
    error = ctypes.c_char_p()
    ptr = lib.tf_compile_to_sql(data, len(data), ctypes.byref(error))
    if not ptr:
        msg = error.value.decode('utf-8') if error.value else 'unknown error'
        raise RuntimeError(f"SQL compile failed: {msg}")
    result = ctypes.string_at(ptr).decode('utf-8')
    lib.tf_string_free(ptr)
    return result


def pipeline_create_from_json(plan_json: str) -> int:
    """Create a pipeline from a JSON recipe string. Returns handle."""
    lib = _load_lib()
    data = plan_json.encode('utf-8')
    error = ctypes.c_char_p()
    ir = lib.tf_ir_plan_from_json(data, len(data), ctypes.byref(error))
    if not ir:
        msg = error.value.decode('utf-8') if error.value else 'unknown error'
        raise RuntimeError(f"Failed to parse recipe: {msg}")
    rc = lib.tf_ir_plan_validate(ir)
    if rc != 0:
        lib.tf_ir_plan_destroy(ir)
        raise RuntimeError("Recipe validation failed")
    handle = lib.tf_pipeline_create_from_ir(ir)
    lib.tf_ir_plan_destroy(ir)
    if not handle:
        err = lib.tf_last_error()
        msg = err.decode('utf-8') if err else 'unknown error'
        raise RuntimeError(f"Failed to create pipeline from recipe: {msg}")
    return handle
