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
_SINK_CALLBACK = ctypes.CFUNCTYPE(
    ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t, ctypes.c_void_p
)
_TRANSFORM_CANCEL_CALLBACK = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p)


class _TfHostPolicy(ctypes.Structure):
    _fields_ = [
        ('allow_fs', ctypes.c_bool),
        ('allow_net', ctypes.c_bool),
        ('allow_spill', ctypes.c_bool),
        ('allow_blocking', ctypes.c_bool),
        ('allow_rules_file', ctypes.c_bool),
        ('workspace_root', ctypes.c_char_p),
        ('resolve_path', ctypes.c_void_p),
        ('user', ctypes.c_void_p),
    ]


class _TfTransformLimitsV1(ctypes.Structure):
    _fields_ = [
        ('abi_version', ctypes.c_uint32),
        ('struct_size', ctypes.c_uint32),
        ('max_recipe_bytes', ctypes.c_uint64),
        ('max_plan_bytes', ctypes.c_uint64),
        ('max_json_depth', ctypes.c_uint64),
        ('max_object_keys', ctypes.c_uint64),
        ('max_steps', ctypes.c_uint64),
        ('max_input_columns', ctypes.c_uint64),
        ('max_output_columns', ctypes.c_uint64),
        ('max_categories_per_column', ctypes.c_uint64),
        ('max_total_categories', ctypes.c_uint64),
        ('max_string_bytes', ctypes.c_uint64),
        ('max_decoded_string_bytes', ctypes.c_uint64),
        ('max_analyzer_rows', ctypes.c_uint64),
        ('max_analyzer_input_bytes', ctypes.c_uint64),
        ('max_resident_state_bytes', ctypes.c_uint64),
        ('max_spill_bytes', ctypes.c_uint64),
        ('max_apply_rows', ctypes.c_uint64),
        ('max_apply_input_bytes', ctypes.c_uint64),
        ('max_output_elements_per_call', ctypes.c_uint64),
        ('max_allocation_bytes', ctypes.c_uint64),
        ('max_allocations_per_session', ctypes.c_uint64),
        ('max_live_handles', ctypes.c_uint64),
        ('max_retired_handle_slots', ctypes.c_uint64),
    ]


class _TfTransformRuntimeV1(ctypes.Structure):
    _fields_ = [
        ('abi_version', ctypes.c_uint32),
        ('struct_size', ctypes.c_uint32),
        ('limits', ctypes.POINTER(_TfTransformLimitsV1)),
        ('host_policy', ctypes.c_void_p),
        ('spill_dir_utf8', ctypes.POINTER(ctypes.c_uint8)),
        ('spill_dir_bytes', ctypes.c_size_t),
        ('cancel', ctypes.c_void_p),
        ('cancel_user', ctypes.c_void_p),
        ('flags', ctypes.c_uint32),
        ('reserved', ctypes.c_uint32),
    ]


class _TfFieldViewV1(ctypes.Structure):
    _fields_ = [
        ('abi_version', ctypes.c_uint32),
        ('struct_size', ctypes.c_uint32),
        ('dtype', ctypes.c_uint32),
        ('flags', ctypes.c_uint32),
        ('id_utf8', ctypes.POINTER(ctypes.c_uint8)),
        ('id_bytes', ctypes.c_size_t),
        ('name_utf8', ctypes.POINTER(ctypes.c_uint8)),
        ('name_bytes', ctypes.c_size_t),
    ]


class _TfSchemaViewV1(ctypes.Structure):
    _fields_ = [
        ('abi_version', ctypes.c_uint32),
        ('struct_size', ctypes.c_uint32),
        ('column_count', ctypes.c_size_t),
        ('fields', ctypes.POINTER(_TfFieldViewV1)),
        ('fields_bytes', ctypes.c_size_t),
    ]


class _TfColumnViewV1(ctypes.Structure):
    _fields_ = [
        ('abi_version', ctypes.c_uint32),
        ('struct_size', ctypes.c_uint32),
        ('data', ctypes.c_void_p),
        ('data_bytes', ctypes.c_size_t),
        ('stride_bytes', ctypes.c_size_t),
        ('validity', ctypes.POINTER(ctypes.c_uint8)),
        ('validity_bytes', ctypes.c_size_t),
        ('validity_bit_offset', ctypes.c_size_t),
        ('validity_bit_stride', ctypes.c_size_t),
    ]


class _TfTableViewV1(ctypes.Structure):
    _fields_ = [
        ('abi_version', ctypes.c_uint32),
        ('struct_size', ctypes.c_uint32),
        ('row_count', ctypes.c_size_t),
        ('column_count', ctypes.c_size_t),
        ('columns', ctypes.POINTER(_TfColumnViewV1)),
        ('columns_bytes', ctypes.c_size_t),
    ]


class _TfOwnedDenseV1(ctypes.Structure):
    _fields_ = [
        ('abi_version', ctypes.c_uint32),
        ('struct_size', ctypes.c_uint32),
        ('dtype', ctypes.c_uint32),
        ('flags', ctypes.c_uint32),
        ('rows', ctypes.c_size_t),
        ('columns', ctypes.c_size_t),
        ('data', ctypes.c_void_p),
        ('data_bytes', ctypes.c_size_t),
    ]


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

    # tf_pipeline_create_with_host_policy
    _lib.tf_pipeline_create_with_host_policy.argtypes = [ctypes.c_char_p, ctypes.c_size_t,
                                                          ctypes.POINTER(_TfHostPolicy)]
    _lib.tf_pipeline_create_with_host_policy.restype = ctypes.c_void_p

    # tf_pipeline_free
    _lib.tf_pipeline_free.argtypes = [ctypes.c_void_p]
    _lib.tf_pipeline_free.restype = None

    # tf_pipeline_push
    _lib.tf_pipeline_push.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
    _lib.tf_pipeline_push.restype = ctypes.c_int

    # tf_pipeline_flush_input
    _lib.tf_pipeline_flush_input.argtypes = [ctypes.c_void_p]
    _lib.tf_pipeline_flush_input.restype = ctypes.c_int

    # tf_pipeline_set_source_name
    _lib.tf_pipeline_set_source_name.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    _lib.tf_pipeline_set_source_name.restype = ctypes.c_int

    # tf_pipeline_finish
    _lib.tf_pipeline_finish.argtypes = [ctypes.c_void_p]
    _lib.tf_pipeline_finish.restype = ctypes.c_int

    # tf_pipeline_finish_step
    _lib.tf_pipeline_finish_step.argtypes = [ctypes.c_void_p]
    _lib.tf_pipeline_finish_step.restype = ctypes.c_int

    # tf_pipeline_pull
    _lib.tf_pipeline_pull.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_size_t]
    _lib.tf_pipeline_pull.restype = ctypes.c_size_t

    # tf_pipeline_set_sink
    _lib.tf_pipeline_set_sink.argtypes = [ctypes.c_void_p, ctypes.c_int, _SINK_CALLBACK, ctypes.c_void_p]
    _lib.tf_pipeline_set_sink.restype = ctypes.c_int

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

    transform_code = ctypes.c_int
    handle_out = ctypes.POINTER(ctypes.c_void_p)
    error_out = ctypes.POINTER(ctypes.c_void_p)
    _lib.tf_transform_limits_init_safe_v1.argtypes = [
        ctypes.POINTER(_TfTransformLimitsV1), ctypes.c_size_t]
    _lib.tf_transform_limits_init_safe_v1.restype = transform_code
    _lib.tf_transform_recipe_from_json.argtypes = [
        ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t,
        ctypes.POINTER(_TfTransformLimitsV1), handle_out, error_out]
    _lib.tf_transform_recipe_from_json.restype = transform_code
    _lib.tf_transform_analyzer_create.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(_TfSchemaViewV1),
        ctypes.POINTER(_TfTransformRuntimeV1), handle_out, error_out]
    _lib.tf_transform_analyzer_create.restype = transform_code
    _lib.tf_transform_analyzer_push.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(_TfTableViewV1), error_out]
    _lib.tf_transform_analyzer_push.restype = transform_code
    _lib.tf_transform_analyzer_finalize.argtypes = [
        ctypes.c_void_p, handle_out, error_out]
    _lib.tf_transform_analyzer_finalize.restype = transform_code
    _lib.tf_transform_plan_export.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(_TfTransformLimitsV1),
        ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_size_t), error_out]
    _lib.tf_transform_plan_export.restype = transform_code
    _lib.tf_transform_plan_import.argtypes = [
        ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t,
        ctypes.POINTER(_TfTransformRuntimeV1), handle_out, error_out]
    _lib.tf_transform_plan_import.restype = transform_code
    _lib.tf_transform_apply_create.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(_TfSchemaViewV1),
        ctypes.POINTER(_TfTransformRuntimeV1), handle_out, error_out]
    _lib.tf_transform_apply_create.restype = transform_code
    _lib.tf_transform_apply_run.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(_TfTableViewV1),
        ctypes.POINTER(_TfOwnedDenseV1), error_out]
    _lib.tf_transform_apply_run.restype = transform_code
    for name in (
        'tf_transform_recipe_destroy', 'tf_transform_analyzer_destroy',
        'tf_transform_plan_destroy', 'tf_transform_apply_destroy',
        'tf_transform_error_destroy',
    ):
        function = getattr(_lib, name)
        function.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        function.restype = None
    _lib.tf_transform_bytes_free.argtypes = [
        ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_size_t)]
    _lib.tf_transform_bytes_free.restype = None
    _lib.tf_owned_dense_free.argtypes = [ctypes.POINTER(_TfOwnedDenseV1)]
    _lib.tf_owned_dense_free.restype = None
    _lib.tf_transform_error_get_code.argtypes = [ctypes.c_void_p]
    _lib.tf_transform_error_get_code.restype = transform_code
    _lib.tf_transform_error_message.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t)]
    _lib.tf_transform_error_message.restype = ctypes.c_void_p
    _lib.tf_transform_plan_schema_json.argtypes = [
        ctypes.c_void_p, ctypes.c_uint32,
        ctypes.POINTER(_TfTransformLimitsV1),
        ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_size_t), error_out]
    _lib.tf_transform_plan_schema_json.restype = transform_code

    return _lib


# --- Thin Python wrappers ---

def _host_policy(*, allow_fs=False, allow_net=False, allow_spill=False,
                 allow_blocking=True, allow_rules_file=False, workspace_root=None):
    workspace_bytes = None if workspace_root in (None, '') else os.fspath(workspace_root).encode('utf-8')
    policy = _TfHostPolicy(
        bool(allow_fs),
        bool(allow_net),
        bool(allow_spill),
        bool(allow_blocking),
        bool(allow_rules_file),
        workspace_bytes,
        None,
        None,
    )
    return policy, workspace_bytes


def pipeline_create(plan_json: str, *, allow_fs=False, allow_net=False,
                    allow_spill=False, allow_blocking=True,
                    allow_rules_file=False, workspace_root=None) -> int:
    """Create a pipeline from JSON plan. Returns handle (pointer as int)."""
    lib = _load_lib()
    data = plan_json.encode('utf-8')
    policy, workspace_bytes = _host_policy(
        allow_fs=allow_fs,
        allow_net=allow_net,
        allow_spill=allow_spill,
        allow_blocking=allow_blocking,
        allow_rules_file=allow_rules_file,
        workspace_root=workspace_root,
    )
    handle = lib.tf_pipeline_create_with_host_policy(data, len(data), ctypes.byref(policy))
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

def pipeline_flush_input(handle: int) -> None:
    """Flush the decoder input boundary without finishing transforms."""
    lib = _load_lib()
    rc = lib.tf_pipeline_flush_input(handle)
    if rc != 0:
        err = lib.tf_pipeline_error(handle)
        msg = err.decode('utf-8') if err else 'unknown error'
        raise RuntimeError(f"Input flush failed: {msg}")


def pipeline_set_source_name(handle: int, name) -> None:
    """Set or clear the host source name used by source-name."""
    lib = _load_lib()
    raw = None if name is None else os.fspath(name).encode('utf-8')
    rc = lib.tf_pipeline_set_source_name(handle, raw)
    if rc != 0:
        err = lib.tf_pipeline_error(handle)
        msg = err.decode('utf-8') if err else 'unknown error'
        raise RuntimeError(f"Set source name failed: {msg}")


def pipeline_finish(handle: int) -> None:
    """Signal end of input and flush the pipeline."""
    lib = _load_lib()
    rc = lib.tf_pipeline_finish(handle)
    if rc != 0:
        err = lib.tf_pipeline_error(handle)
        msg = err.decode('utf-8') if err else 'unknown error'
        raise RuntimeError(f"Finish failed: {msg}")


def pipeline_finish_step(handle: int) -> bool:
    """Advance finish by one flush boundary. Return True when complete."""
    lib = _load_lib()
    rc = lib.tf_pipeline_finish_step(handle)
    if rc < 0:
        err = lib.tf_pipeline_error(handle)
        msg = err.decode('utf-8') if err else 'unknown error'
        raise RuntimeError(f"Finish failed: {msg}")
    return rc == 1


def pipeline_pull_chunk(handle: int, channel: int, buf_size: int = 65536) -> bytes:
    """Pull at most buf_size bytes from a channel."""
    if buf_size <= 0:
        raise ValueError("buf_size must be positive")
    lib = _load_lib()
    buf = ctypes.create_string_buffer(buf_size)
    n = lib.tf_pipeline_pull(handle, channel, buf, buf_size)
    return buf.raw[:n]


def pipeline_pull(handle: int, channel: int, buf_size: int = 65536) -> bytes:
    """Pull all available output from a channel."""
    chunks = []
    while True:
        chunk = pipeline_pull_chunk(handle, channel, buf_size)
        if not chunk:
            break
        chunks.append(chunk)
    return b''.join(chunks)


def pipeline_set_sink(handle: int, channel: int, callback):
    """Register a C-level sink callback. Return the ctypes callback to keep alive."""
    lib = _load_lib()

    @_SINK_CALLBACK
    def c_callback(ch, data, length, user):
        del user
        try:
            callback(ch, ctypes.string_at(data, length))
        except BaseException:
            return -1
        return 0

    rc = lib.tf_pipeline_set_sink(handle, channel, c_callback, None)
    if rc != 0:
        err = lib.tf_pipeline_error(handle)
        msg = err.decode('utf-8') if err else 'unknown error'
        raise RuntimeError(f"Set sink failed: {msg}")
    return c_callback


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


def _normalize_sql_dialect(dialect: str) -> str:
    if dialect is None:
        return 'duckdb'
    if not isinstance(dialect, str):
        raise TypeError('dialect must be a string')
    name = dialect.lower()
    if name == 'duckdb':
        return name
    if name in ('sqlite', 'postgres'):
        raise ValueError(
            f"SQL dialect {name!r} is recognized but not implemented yet; "
            "only duckdb lowering is available"
        )
    raise ValueError(
        f"unknown SQL dialect {dialect!r} (expected duckdb, sqlite, or postgres)"
    )


def compile_to_sql(dsl: str, *, dialect: str = 'duckdb') -> str:
    """Compile a DSL string to a SQL query string."""
    _normalize_sql_dialect(dialect)
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


def pipeline_create_from_json(plan_json: str, *, allow_fs=False, allow_net=False,
                              allow_spill=False, allow_blocking=True,
                              allow_rules_file=False, workspace_root=None) -> int:
    """Create a pipeline from a JSON recipe string. Returns handle."""
    return pipeline_create(
        plan_json,
        allow_fs=allow_fs,
        allow_net=allow_net,
        allow_spill=allow_spill,
        allow_blocking=allow_blocking,
        allow_rules_file=allow_rules_file,
        workspace_root=workspace_root,
    )
